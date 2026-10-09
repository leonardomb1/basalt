//! `GROUP BY` past `--op-memory`: the serial aggregate's spilling mode, by the
//! scheme in `op/spill_parts.zig`. It is on when the aggregate has a `space`, keys,
//! and no `part_state` (per-lane partials never spill). `state` is metered from the
//! first row; the drain loops call `overBudget` after each batch, and once frozen
//! probe with `GroupTable.find` instead of inserting and hand the rows of absent
//! keys to `spillRows`. Partitions are picked from the table's own key hash
//! (`fixedHash` or `MultiKeyCtx.hash`), so equal keys, NULL keys and numeric keys
//! that group together always share a file. A fixed-width string key not yet
//! interned is absent by definition and is not interned while frozen, so spilled
//! keys cost no memory. Every aggregate in `lang/aggregates.zig`, DISTINCT ones
//! included, stays exact, floating-point sums bit for bit, because a group's rows
//! are all folded in one place in input order.

const Aggregate = @import("../aggregate.zig").Aggregate;
const Batch = @import("../../batch.zig").Batch;
const Op = @import("../../op.zig").Op;
const parts_mod = @import("../spill_parts.zig");
const std = @import("std");

pub const Spill = struct {
    meter: parts_mod.Meter,
    parts: parts_mod.Parts,
    frozen: bool = false,
    replay: ?parts_mod.Replay(Aggregate) = null,
};

/// `next` in spilling mode: the whole input folded (spilling what does not fit),
/// the groups held in memory emitted, then each spill file aggregated in turn.
pub fn spillNext(self: *Aggregate, arena: std.mem.Allocator) anyerror!?Batch {
    if (self.spill == null) {
        const sp = try self.state.create(Spill);
        sp.* = .{
            .meter = .{ .child = self.state },
            .parts = .{ .space = self.space.?, .alloc = self.state, .schema = self.in_schema, .tag = "group-by" },
        };
        self.spill = sp;
        self.state = sp.meter.allocator();
        const set = self.drainSet() catch |e| {
            sp.parts.abort();
            return e;
        };
        sp.replay = .{ .runs = try sp.parts.finish() };
        if (set.len > 0) return try self.emitSets(arena, &.{set}, null);
    }
    const sp = self.spill.?;
    return sp.replay.?.next(self, self.gpa, arena);
}

/// A fresh aggregate over one spill file, one level deeper, on `state`.
pub fn replayChild(self: *Aggregate, child: Op, state: std.mem.Allocator) Aggregate {
    return .{
        .child = child,
        .in_schema = self.in_schema,
        .by = self.by,
        .aggs = self.aggs,
        .out_schema = self.out_schema,
        .err = self.err,
        .state = state,
        .gpa = self.gpa,
        .table_gpa = self.table_gpa,
        .space = self.space,
        .spill_at = self.spill_at,
        .max_spill_depth = self.max_spill_depth,
        .spill_depth = self.spill_depth + 1,
    };
}

/// Whether the table is frozen, freezing it once what it holds passes `spill_at`.
/// A table holding no group never freezes, so every level folds some groups.
pub fn overBudget(self: *Aggregate, parts: anytype) bool {
    const sp = self.spill orelse return false;
    if (sp.frozen) return true;
    var bytes = sp.meter.bytes;
    var groups: usize = 0;
    for (parts) |*p| {
        bytes += p.table.entries.len * @sizeOf(u32);
        groups += p.table.len;
    }
    if (groups > 0 and bytes > self.spill_at) sp.frozen = true;
    return sp.frozen;
}

/// Writes the rows `dest` marks to their spill files, failing past the depth limit.
pub fn spillRows(self: *Aggregate, scratch: std.mem.Allocator, b: Batch, dest: []const u8) !void {
    const sp = self.spill.?;
    if (self.spill_depth >= self.max_spill_depth) {
        if (self.err) |ec| ec.set("GROUP BY still exceeds --op-memory after {d} levels of spilling; raise --op-memory", .{self.spill_depth});
        return error.SpillTooDeep;
    }
    sp.parts.route(scratch, b, dest) catch |e| {
        if (e == error.SpillCapExceeded) if (self.err) |ec| ec.set("GROUP BY spilled past --spill-cap; raise --spill-cap or --op-memory", .{});
        return e;
    };
}

/// A row's spill file from the key hash the fold already computed.
pub fn destOf(self: *const Aggregate, h: u64) u8 {
    return parts_mod.partOf(h, self.spill_depth);
}

const Scan = @import("../../op.zig").Scan;
const ErrCtx = @import("../../op.zig").ErrCtx;
const Space = @import("../../space.zig").Space;
const DirSpace = @import("../../space.zig").DirSpace;
const TestSource = @import("../testing_util.zig").TestSource;
const Value = @import("../../value.zig").Value;
const ast = @import("../../../lang/ast.zig");
const column = @import("../../column.zig");
const eval = @import("../../eval.zig");
const types = @import("../../../lang/types.zig");
const testing = std.testing;

const in_schema = types.Schema{ .fields = &.{
    .{ .name = "k", .ty = types.Type.init(.int).asNullable() },
    .{ .name = "s", .ty = types.Type.init(.string).asNullable() },
    .{ .name = "f", .ty = types.Type.init(.float).asNullable() },
    .{ .name = "d", .ty = types.Type.decimal(18, 2).asNullable() },
    .{ .name = "b", .ty = types.Type.init(.bool).asNullable() },
    .{ .name = "x", .ty = types.Type.init(.int).asNullable() },
} };

fn fieldExpr(comptime name: []const u8) ast.Expr {
    return .{ .field = .{ .parts = &[_][]const u8{name} } };
}

var fs = fieldExpr("s");
var ff = fieldExpr("f");
var fd = fieldExpr("d");
var fb = fieldExpr("b");
var fx = fieldExpr("x");
var two = ast.Expr{ .int_lit = 2 };
var x2 = ast.Expr{ .binary = .{ .op = .mul, .l = &fx, .r = &two } };

const int_n = types.Type.init(.int).asNullable();
const float_n = types.Type.init(.float).asNullable();
const dec_n = types.Type.decimal(18, 2).asNullable();
const str_n = types.Type.init(.string).asNullable();
const bool_n = types.Type.init(.bool).asNullable();

const every_agg = [_]Aggregate.Agg{
    .{ .func = .count, .arg = null, .ty = types.Type.init(.int) },
    .{ .func = .count, .arg = &fx, .ty = types.Type.init(.int) },
    .{ .func = .count, .arg = &fx, .ty = types.Type.init(.int), .distinct = true },
    .{ .func = .count, .arg = &fs, .ty = types.Type.init(.int), .distinct = true },
    .{ .func = .sum, .arg = &fx, .ty = int_n },
    .{ .func = .sum, .arg = &x2, .ty = int_n },
    .{ .func = .sum, .arg = &ff, .ty = float_n },
    .{ .func = .sum, .arg = &fd, .ty = dec_n },
    .{ .func = .sum, .arg = &fx, .ty = int_n, .distinct = true },
    .{ .func = .avg, .arg = &ff, .ty = float_n },
    .{ .func = .avg, .arg = &fd, .ty = float_n },
    .{ .func = .min, .arg = &fs, .ty = str_n },
    .{ .func = .max, .arg = &fs, .ty = str_n },
    .{ .func = .min, .arg = &fd, .ty = dec_n },
    .{ .func = .max, .arg = &fx, .ty = int_n },
    .{ .func = .min, .arg = &ff, .ty = float_n },
    .{ .func = .median, .arg = &ff, .ty = float_n },
    .{ .func = .median, .arg = &fx, .ty = float_n },
    .{ .func = .count_if, .arg = &fb, .ty = types.Type.init(.int) },
    .{ .func = .bool_and, .arg = &fb, .ty = bool_n },
    .{ .func = .bool_or, .arg = &fb, .ty = bool_n },
    .{ .func = .bit_and, .arg = &fx, .ty = int_n },
    .{ .func = .bit_or, .arg = &fx, .ty = int_n },
    .{ .func = .bit_xor, .arg = &fx, .ty = int_n },
    .{ .func = .var_samp, .arg = &ff, .ty = float_n },
    .{ .func = .var_pop, .arg = &fx, .ty = float_n },
    .{ .func = .stddev_samp, .arg = &ff, .ty = float_n },
    .{ .func = .stddev_pop, .arg = &fd, .ty = float_n },
};

/// Rows `lo..hi` of a deterministic input: `groups` int keys spread by a
/// multiplier, five strings, floats with -0.0, decimals of mixed scale for one
/// value, NULLs in every column.
fn makeBatch(a: std.mem.Allocator, lo: usize, hi: usize, groups: usize) !Batch {
    var bs: [in_schema.fields.len]column.Builder = undefined;
    for (&bs, in_schema.fields) |*b, f| b.* = column.Builder.init(a, f.ty);
    for (lo..hi) |i| {
        const ii: i64 = @intCast(i);
        const u: i128 = @as(i128, @intCast(i % 450)) * 30 - 3000;
        const dv: Value = if (i % 9 == 4) .null else if (i % 3 == 0 and @rem(u, 10) == 0) .{ .decimal = .{ .unscaled = @divExact(u, 10), .scale = 1 } } else .{ .decimal = .{ .unscaled = u, .scale = 2 } };
        const fv: Value = if (i % 11 == 0) .null else if (i % 23 == 0) .{ .float = -0.0 } else if (i % 29 == 0) .{ .float = 0.0 } else .{ .float = @as(f64, @floatFromInt(i % 170)) * 0.1 - 8.0 };
        const row = [_]Value{
            if (i % 97 == 5) .null else .{ .int = @intCast((i * 7919) % groups) },
            if (i % 13 == 0) .null else .{ .string = try std.fmt.allocPrint(a, "s{d}", .{i % 5}) },
            fv,
            dv,
            if (i % 19 == 0) .null else .{ .bool = i % 3 != 0 },
            if (i % 7 == 0) .null else .{ .int = @mod(ii * 31, 1000) - 500 },
        };
        for (&bs, row) |*b, v| try b.append(v);
    }
    const cols = try a.alloc(column.Column, bs.len);
    for (cols, &bs) |*c, *b| c.* = try b.finish();
    return .{ .schema = &in_schema, .columns = cols, .len = hi - lo };
}

fn makeInput(a: std.mem.Allocator, rows: usize, per: usize, groups: usize) ![]Batch {
    const n = (rows + per - 1) / per;
    const out = try a.alloc(Batch, n);
    for (out, 0..) |*b, i| b.* = try makeBatch(a, i * per, @min(rows, (i + 1) * per), groups);
    return out;
}

fn outSchema(a: std.mem.Allocator, by: []const usize, aggs: []const Aggregate.Agg) !*types.Schema {
    const fields = try a.alloc(types.Schema.Field, by.len + aggs.len);
    for (by, fields[0..by.len]) |ci, *f| f.* = in_schema.fields[ci];
    for (aggs, fields[by.len..], 0..) |ag, *f, j| f.* = .{ .name = try std.fmt.allocPrint(a, "a{d}", .{j}), .ty = ag.ty };
    const s = try a.create(types.Schema);
    s.* = .{ .fields = fields };
    return s;
}

fn fmtValue(a: std.mem.Allocator, v: Value) ![]const u8 {
    return switch (v) {
        .null => "NULL",
        .float => |x| std.fmt.allocPrint(a, "f{x}", .{@as(u64, @bitCast(x))}),
        .decimal => |d| std.fmt.allocPrint(a, "d{d}e{d}", .{ d.unscaled, d.scale }),
        .string => |s| std.fmt.allocPrint(a, "'{s}'", .{s}),
        else => eval.valueToString(a, v),
    };
}

const Opts = struct {
    space: ?Space = null,
    spill_at: usize = std.math.maxInt(usize),
    max_depth: u8 = parts_mod.default_max_depth,
    err: ?*ErrCtx = null,
};

fn lessStr(_: void, x: []const u8, y: []const u8) bool {
    return std.mem.lessThan(u8, x, y);
}

/// Every output row as one string, sorted, so runs compare as multisets.
fn runAgg(a: std.mem.Allocator, input: []const Batch, by: []const usize, aggs: []const Aggregate.Agg, o: Opts) ![]const []const u8 {
    var ts = TestSource{ .schema_ = in_schema, .batches = input };
    var scan = Scan{ .src = ts.src() };
    var state = std.heap.ArenaAllocator.init(testing.allocator);
    defer state.deinit();
    var agg = Aggregate{
        .child = .{ .scan = &scan },
        .in_schema = &in_schema,
        .by = by,
        .aggs = aggs,
        .out_schema = try outSchema(a, by, aggs),
        .err = o.err,
        .state = state.allocator(),
        .gpa = testing.allocator,
        .space = o.space,
        .spill_at = o.spill_at,
        .max_spill_depth = o.max_depth,
    };
    var rows = std.array_list.Managed([]const u8).init(a);
    while (try agg.next(a)) |b| {
        try testing.expect(b.len > 0);
        for (0..b.len) |r| {
            var line = std.array_list.Managed(u8).init(a);
            for (b.columns) |c| {
                try line.appendSlice(try fmtValue(a, c.getValue(r)));
                try line.append('|');
            }
            try rows.append(line.items);
        }
    }
    std.mem.sort([]const u8, rows.items, {}, lessStr);
    return rows.items;
}

pub const TestDir = struct {
    tmp: testing.TmpDir,
    path: []const u8,
    ds: DirSpace,

    pub fn init(cap: u64) !*TestDir {
        const self = try testing.allocator.create(TestDir);
        self.tmp = testing.tmpDir(.{ .iterate = true });
        self.path = try self.tmp.dir.realpathAlloc(testing.allocator, ".");
        self.ds = .{ .dir = self.path, .cap = cap };
        return self;
    }

    pub fn deinit(self: *TestDir) void {
        testing.allocator.free(self.path);
        self.tmp.cleanup();
        testing.allocator.destroy(self);
    }

    pub fn files(self: *TestDir) !usize {
        var it = self.tmp.dir.iterate();
        var n: usize = 0;
        while (try it.next()) |_| n += 1;
        return n;
    }
};

fn expectSameRows(want: []const []const u8, got: []const []const u8) !void {
    try testing.expectEqual(want.len, got.len);
    for (want, got) |w, g| try testing.expectEqualStrings(w, g);
}

test "aggregate spill: every aggregate matches the in-memory result over every key shape" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const input = try makeInput(a, 3000, 100, 700);
    const shapes = [_][]const usize{ &.{0}, &.{ 1, 0 }, &.{3}, &.{2}, &.{ 5, 1 }, &.{ 4, 3, 1 } };
    for (shapes) |by| {
        const want = try runAgg(a, input, by, &every_agg, .{});
        for ([_]usize{ 1, 64 << 10 }) |at| {
            var td = try TestDir.init(std.math.maxInt(u64));
            defer td.deinit();
            const got = try runAgg(a, input, by, &every_agg, .{ .space = td.ds.space(), .spill_at = at });
            try expectSameRows(want, got);
            if (at == 1) try testing.expect(td.ds.seq.load(.monotonic) > 0);
            try testing.expectEqual(@as(usize, 0), try td.files());
        }
    }
}

test "aggregate spill: count-only and plain sums take the fast folds and still match" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const input = try makeInput(a, 2000, 128, 900);
    const counts = [_]Aggregate.Agg{
        .{ .func = .count, .arg = null, .ty = types.Type.init(.int) },
        .{ .func = .count, .arg = null, .ty = types.Type.init(.int) },
    };
    const sums = [_]Aggregate.Agg{
        .{ .func = .sum, .arg = &fx, .ty = int_n },
        .{ .func = .sum, .arg = &ff, .ty = float_n },
        .{ .func = .count, .arg = &fx, .ty = types.Type.init(.int) },
    };
    for ([_][]const Aggregate.Agg{ &counts, &sums }) |aggs| for ([_][]const usize{ &.{0}, &.{ 0, 1 } }) |by| {
        const want = try runAgg(a, input, by, aggs, .{});
        var td = try TestDir.init(std.math.maxInt(u64));
        defer td.deinit();
        try expectSameRows(want, try runAgg(a, input, by, aggs, .{ .space = td.ds.space(), .spill_at = 1 }));
    };
    var td = try TestDir.init(std.math.maxInt(u64));
    defer td.deinit();
    var total: i64 = 0;
    for (try runAgg(a, input, &.{0}, &counts, .{ .space = td.ds.space(), .spill_at = 1 })) |line| {
        var it = std.mem.splitScalar(u8, line, '|');
        _ = it.next();
        total += try std.fmt.parseInt(i64, it.next().?, 10);
    }
    try testing.expectEqual(@as(i64, 2000), total);
}

test "aggregate spill: a spill file that overflows again partitions one level deeper" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const input = try makeInput(a, 4000, 100, 2500);
    const want = try runAgg(a, input, &.{ 1, 0 }, &every_agg, .{});
    var td = try TestDir.init(std.math.maxInt(u64));
    defer td.deinit();
    try expectSameRows(want, try runAgg(a, input, &.{ 1, 0 }, &every_agg, .{ .space = td.ds.space(), .spill_at = 1 }));
    try testing.expect(td.ds.seq.load(.monotonic) > parts_mod.fanout);
}

test "aggregate spill: past the depth limit fails clearly" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const input = try makeInput(a, 4000, 100, 2500);
    var td = try TestDir.init(std.math.maxInt(u64));
    defer td.deinit();
    var ec = ErrCtx{};
    try testing.expectError(error.SpillTooDeep, runAgg(a, input, &.{0}, &every_agg, .{ .space = td.ds.space(), .spill_at = 1, .max_depth = 1, .err = &ec }));
    try testing.expect(std.mem.indexOf(u8, ec.msg, "--op-memory") != null);
}

test "aggregate spill: past the disk cap fails with SpillCapExceeded" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const input = try makeInput(a, 2000, 100, 700);
    var td = try TestDir.init(4096);
    defer td.deinit();
    var ec = ErrCtx{};
    try testing.expectError(error.SpillCapExceeded, runAgg(a, input, &.{ 1, 0 }, &every_agg, .{ .space = td.ds.space(), .spill_at = 1, .err = &ec }));
    try testing.expect(std.mem.indexOf(u8, ec.msg, "--spill-cap") != null);
    try testing.expectEqual(@as(usize, 0), try td.files());
}

test "aggregate spill: under the budget nothing is written" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const input = try makeInput(a, 1500, 100, 300);
    var td = try TestDir.init(std.math.maxInt(u64));
    defer td.deinit();
    const want = try runAgg(a, input, &.{ 1, 0 }, &every_agg, .{});
    try expectSameRows(want, try runAgg(a, input, &.{ 1, 0 }, &every_agg, .{ .space = td.ds.space(), .spill_at = 1 << 30 }));
    try testing.expectEqual(@as(u64, 0), td.ds.seq.load(.monotonic));
}
