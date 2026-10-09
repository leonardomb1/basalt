//! `DISTINCT` and `DISTINCT ON`: the first row per key, in input order.
//!
//! With a `space` (and no `track_ords`, which only the lanes set) it spills past
//! `spill_at` by the scheme in `spill_parts.zig`: once frozen, a row whose key was
//! seen is dropped as before, a row whose key was not goes to the spill file of
//! its key hash, and each file is deduplicated by a fresh `Distinct` after the
//! input ends. A key's first row is then either already out or the first of its
//! file, so DISTINCT ON still keeps the first row per key; only the order of the
//! output changes.

const Batch = @import("../batch.zig").Batch;
const ErrCtx = @import("../op.zig").ErrCtx;
const Op = @import("../op.zig").Op;
const Space = @import("../space.zig").Space;
const spill_parts = @import("spill_parts.zig");
const Stats = @import("../op.zig").Stats;
const Value = @import("../value.zig").Value;
const column = @import("../column.zig");
const dupeValue = @import("aggregate.zig").dupeValue;
const keyhash = @import("../keyhash.zig");
const std = @import("std");
const types = @import("../../lang/types.zig");
const Scan = @import("../op.zig").Scan;
const TestSource = @import("testing_util.zig").TestSource;
const intBatch = @import("testing_util.zig").intBatch;
const int_schema = @import("testing_util.zig").int_schema;
const strBatch = @import("testing_util.zig").strBatch;
const testing = std.testing;

pub const Distinct = struct {
    stats: Stats = .{},
    child: Op,
    in_schema: *const types.Schema,
    keys: ?[]const usize,
    state: std.mem.Allocator,
    gpa: std.mem.Allocator,
    seen: ?Seen = null,
    track_ords: bool = false,
    ords: []const u64 = &.{},
    seen_rows: u64 = 0,
    seen_words: ?SeenWords = null,
    seen_null: bool = false,
    err: ?*ErrCtx = null,
    space: ?Space = null,
    spill_at: usize = std.math.maxInt(usize),
    max_spill_depth: u8 = spill_parts.default_max_depth,
    spill_depth: u8 = 0,
    spill: ?*Spill = null,

    const Spill = struct {
        meter: spill_parts.Meter,
        parts: spill_parts.Parts,
        frozen: bool = false,
        drained: bool = false,
        replay: ?spill_parts.Replay(Distinct) = null,
    };

    const null_hash: u64 = 0x2545F4914F6CDD1D;

    const Seen = std.HashMap([]const Value, void, keyhash.MultiKeyCtx, std.hash_map.default_max_load_percentage);
    const SeenWords = std.AutoHashMap(u64, void);

    pub fn next(self: *Distinct, arena: std.mem.Allocator) anyerror!?Batch {
        if (self.space == null or self.track_ords) return self.dedup(arena);
        if (self.spill == null) {
            const sp = try self.state.create(Spill);
            sp.* = .{
                .meter = .{ .child = self.state },
                .parts = .{ .space = self.space.?, .alloc = self.state, .schema = self.in_schema, .tag = "distinct" },
            };
            self.spill = sp;
            self.state = sp.meter.allocator();
        }
        const sp = self.spill.?;
        if (!sp.drained) {
            const got = self.dedup(arena) catch |e| {
                sp.parts.abort();
                return e;
            };
            if (got) |b| return b;
            sp.drained = true;
            sp.replay = .{ .runs = try sp.parts.finish() };
        }
        return sp.replay.?.next(self, self.gpa, arena);
    }

    /// A fresh `Distinct` over one spill file, one level deeper, on `state`.
    pub fn replayChild(self: *Distinct, child: Op, state: std.mem.Allocator) Distinct {
        return .{
            .child = child,
            .in_schema = self.in_schema,
            .keys = self.keys,
            .state = state,
            .gpa = self.gpa,
            .err = self.err,
            .space = self.space,
            .spill_at = self.spill_at,
            .max_spill_depth = self.max_spill_depth,
            .spill_depth = self.spill_depth + 1,
        };
    }

    /// Whether the seen keys are frozen, freezing them once they pass `spill_at`.
    /// Nothing freezes before a key is held, so every level keeps some.
    fn overBudget(self: *Distinct) bool {
        const sp = self.spill orelse return false;
        if (sp.frozen) return true;
        var keys: usize = @intFromBool(self.seen_null);
        if (self.seen) |m| keys += m.count();
        if (self.seen_words) |m| keys += m.count();
        if (keys > 0 and sp.meter.bytes > self.spill_at) sp.frozen = true;
        return sp.frozen;
    }

    fn spillRows(self: *Distinct, scratch: std.mem.Allocator, b: Batch, dest: []const u8) !void {
        if (self.spill_depth >= self.max_spill_depth) {
            if (self.err) |ec| ec.set("DISTINCT still exceeds --op-memory after {d} levels of spilling; raise --op-memory", .{self.spill_depth});
            return error.SpillTooDeep;
        }
        self.spill.?.parts.route(scratch, b, dest) catch |e| {
            if (e == error.SpillCapExceeded) if (self.err) |ec| ec.set("DISTINCT spilled past --spill-cap; raise --spill-cap or --op-memory", .{});
            return e;
        };
    }

    fn dedup(self: *Distinct, arena: std.mem.Allocator) anyerror!?Batch {
        if (self.seen == null) self.seen = Seen.init(self.state);
        const seen = &self.seen.?;

        var scratch = std.heap.ArenaAllocator.init(self.gpa);
        defer scratch.deinit();
        const pull = scratch.allocator();

        while (try self.child.next(pull)) |b| {
            var key_idx: []const usize = undefined;
            if (self.keys) |k| {
                key_idx = k;
            } else {
                const idxs = try pull.alloc(usize, b.columns.len);
                for (idxs, 0..) |*x, i| x.* = i;
                key_idx = idxs;
            }

            const keep = try pull.alloc(bool, b.len);
            const probe = try pull.alloc(Value, key_idx.len);
            const frozen = self.overBudget();
            const dest: []u8 = if (frozen) try pull.alloc(u8, b.len) else &.{};
            @memset(dest, spill_parts.keep);
            var nspill: usize = 0;
            var kept: usize = 0;
            var r: usize = 0;
            if (key_idx.len == 1 and fixedWord(b.columns[key_idx[0]]) != null) {
                if (self.seen_words == null) self.seen_words = SeenWords.init(self.state);
                const words = &self.seen_words.?;
                const col = b.columns[key_idx[0]];
                while (r < b.len) : (r += 1) {
                    if (frozen) {
                        const valid = col.validity.get(r);
                        const known = if (valid) words.contains(fixedWord(col).?[r]) else self.seen_null;
                        keep[r] = false;
                        if (!known) {
                            dest[r] = spill_parts.partOf(if (valid) fixedWord(col).?[r] else null_hash, self.spill_depth);
                            nspill += 1;
                        }
                        continue;
                    }
                    if (!col.validity.get(r)) {
                        keep[r] = !self.seen_null;
                        self.seen_null = true;
                    } else {
                        const gop = try words.getOrPut(fixedWord(col).?[r]);
                        keep[r] = !gop.found_existing;
                    }
                    if (keep[r]) kept += 1;
                }
            } else while (r < b.len) : (r += 1) {
                for (key_idx, 0..) |ci, j| probe[j] = b.columns[ci].getValue(r);
                if (frozen) {
                    keep[r] = false;
                    if (!seen.contains(probe)) {
                        dest[r] = spill_parts.partOf(keyhash.MultiKeyCtx.hash(.{}, probe), self.spill_depth);
                        nspill += 1;
                    }
                    continue;
                }
                const gop = try seen.getOrPut(probe);
                if (gop.found_existing) {
                    keep[r] = false;
                } else {
                    const kv = try self.state.alloc(Value, key_idx.len);
                    for (key_idx, 0..) |ci, j| kv[j] = try dupeValue(self.state, b.columns[ci].getValue(r));
                    gop.key_ptr.* = kv;
                    keep[r] = true;
                    kept += 1;
                }
            }
            if (nspill > 0) try self.spillRows(pull, b, dest);
            const base = self.seen_rows;
            self.seen_rows += b.len;
            if (kept == 0) {
                _ = scratch.reset(.retain_capacity);
                continue;
            }
            if (self.track_ords) {
                const ords = try arena.alloc(u64, kept);
                var oi: usize = 0;
                r = 0;
                while (r < b.len) : (r += 1) if (keep[r]) {
                    ords[oi] = base + r;
                    oi += 1;
                };
                self.ords = ords;
            }
            return try gatherDeep(arena, b, keep, kept);
        }
        return null;
    }
};

/// The raw 64-bit words of an int-family column, or null for a kind whose bits
/// are not its identity (floats have two zeros).
fn fixedWord(col: column.Column) ?[]const u64 {
    return switch (col.ty.kind) {
        .int, .time, .timestamp => @ptrCast(col.data.i64),
        else => null,
    };
}

/// Deep-copy the `keep`-marked rows of `b` into `arena`, duping string payloads
/// (unlike `column.gather`), for a source batch whose scratch is about to be freed.
fn gatherDeep(arena: std.mem.Allocator, b: Batch, keep: []const bool, kept: usize) anyerror!Batch {
    const outcols = try arena.alloc(column.Column, b.columns.len);
    for (b.columns, b.schema.fields, 0..) |*col, f, ci| {
        var bd = column.Builder.init(arena, f.ty);
        var r: usize = 0;
        while (r < b.len) : (r += 1) if (keep[r]) try bd.append(col.getValue(r));
        outcols[ci] = try bd.finish();
    }
    return Batch{ .schema = b.schema, .columns = outcols, .len = kept };
}

test "distinct reports the input ordinal of every surviving row" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const batches = [_]Batch{
        try intBatch(a, &int_schema, &.{ 7, 7, 4 }),
        try intBatch(a, &int_schema, &.{ 4, 7, 9 }),
    };
    var ts = TestSource{ .schema_ = int_schema, .batches = &batches };
    var scan = Scan{ .src = ts.src() };
    var dst = Distinct{ .child = .{ .scan = &scan }, .in_schema = &int_schema, .keys = null, .state = a, .gpa = testing.allocator, .track_ords = true };

    var ords = std.array_list.Managed(u64).init(a);
    const top = Op{ .distinct = &dst };
    while (try top.next(a)) |b| {
        try testing.expectEqual(b.len, dst.ords.len);
        try ords.appendSlice(dst.ords);
    }
    try testing.expectEqualSlices(u64, &.{ 0, 2, 5 }, ords.items);
}

test "distinct dedups across batches, groups nulls as one key, deep-copies strings" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const schema = types.Schema{ .fields = &.{
        .{ .name = "s", .ty = types.Type.init(.string).asNullable() },
    } };
    const batches = [_]Batch{
        try strBatch(a, &schema, &.{ "a", "b", null }),
        try strBatch(a, &schema, &.{ "b", null, "c", "a" }),
    };
    var ts = TestSource{ .schema_ = schema, .batches = &batches };
    var scan = Scan{ .src = ts.src() };
    var dst = Distinct{ .child = .{ .scan = &scan }, .in_schema = &schema, .keys = null, .state = a, .gpa = testing.allocator };

    var got = std.array_list.Managed(?[]const u8).init(a);
    const top = Op{ .distinct = &dst };
    while (try top.next(a)) |b| {
        var r: usize = 0;
        while (r < b.len) : (r += 1) {
            const v = b.columns[0].getValue(r);
            try got.append(if (v.isNull()) null else v.string);
        }
        for (batches[0..ts.idx]) |consumed| @memset(consumed.columns[0].data.bytes.values, '#');
    }
    const want = [_]?[]const u8{ "a", "b", null, "c" };
    try testing.expectEqual(want.len, got.items.len);
    for (want, got.items) |w, g| {
        if (w) |s| try testing.expectEqualStrings(s, g.?) else try testing.expect(g == null);
    }
}

const SpillDir = @import("aggregate/spill.zig").TestDir;

const spill_schema = types.Schema{ .fields = &.{
    .{ .name = "k", .ty = types.Type.init(.int).asNullable() },
    .{ .name = "s", .ty = types.Type.init(.string).asNullable() },
    .{ .name = "v", .ty = types.Type.init(.int) },
} };

/// `rows` rows in batches of `per`: `groups` int keys spread by a multiplier, seven
/// strings, NULLs in both, and the row's own index in `v`.
fn spillInput(a: std.mem.Allocator, rows: usize, per: usize, groups: usize) ![]Batch {
    const out = try a.alloc(Batch, (rows + per - 1) / per);
    for (out, 0..) |*b, bi| {
        const lo = bi * per;
        const hi = @min(rows, lo + per);
        var bs: [3]column.Builder = undefined;
        for (&bs, spill_schema.fields) |*x, f| x.* = column.Builder.init(a, f.ty);
        for (lo..hi) |i| {
            try bs[0].append(if (i % 97 == 5) .null else .{ .int = @intCast((i * 7919) % groups) });
            try bs[1].append(if (i % 13 == 0) .null else .{ .string = try std.fmt.allocPrint(a, "s{d}", .{i % 7}) });
            try bs[2].append(.{ .int = @intCast(i) });
        }
        const cols = try a.alloc(column.Column, 3);
        for (cols, &bs) |*c, *x| c.* = try x.finish();
        b.* = .{ .schema = &spill_schema, .columns = cols, .len = hi - lo };
    }
    return out;
}

fn lessStr(_: void, x: []const u8, y: []const u8) bool {
    return std.mem.lessThan(u8, x, y);
}

/// Every output row as `k|s|v`, sorted.
fn runSpillDistinct(a: std.mem.Allocator, input: []const Batch, keys: ?[]const usize, space: ?Space, spill_at: usize, err: ?*ErrCtx) ![]const []const u8 {
    var ts = TestSource{ .schema_ = spill_schema, .batches = input };
    var scan = Scan{ .src = ts.src() };
    var state = std.heap.ArenaAllocator.init(testing.allocator);
    defer state.deinit();
    var dst = Distinct{ .child = .{ .scan = &scan }, .in_schema = &spill_schema, .keys = keys, .state = state.allocator(), .gpa = testing.allocator, .space = space, .spill_at = spill_at, .err = err };
    var rows = std.array_list.Managed([]const u8).init(a);
    while (try dst.next(a)) |b| for (0..b.len) |r| {
        const k = b.columns[0].getValue(r);
        const s = b.columns[1].getValue(r);
        try rows.append(try std.fmt.allocPrint(a, "{d}|{s}|{d}", .{
            if (k.isNull()) -1 else k.int,
            if (s.isNull()) "NULL" else s.string,
            b.columns[2].getValue(r).int,
        }));
    };
    std.mem.sort([]const u8, rows.items, {}, lessStr);
    return rows.items;
}

fn expectSameRows(want: []const []const u8, got: []const []const u8) !void {
    try testing.expectEqual(want.len, got.len);
    for (want, got) |w, g| try testing.expectEqualStrings(w, g);
}

test "distinct spill: DISTINCT and DISTINCT ON keep the same rows, first per key, as in memory" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const input = try spillInput(a, 3000, 100, 900);
    const key_sets = [_]?[]const usize{ &.{0}, &.{ 1, 0 }, &.{1}, null };
    for (key_sets) |keys| {
        const want = try runSpillDistinct(a, input, keys, null, std.math.maxInt(usize), null);
        for ([_]usize{ 1, 16 << 10 }) |at| {
            var td = try SpillDir.init(std.math.maxInt(u64));
            defer td.deinit();
            try expectSameRows(want, try runSpillDistinct(a, input, keys, td.ds.space(), at, null));
            if (at == 1 and want.len > 200) try testing.expect(td.ds.seq.load(.monotonic) > 0);
            try testing.expectEqual(@as(usize, 0), try td.files());
        }
    }
    const first = try runSpillDistinct(a, input, &.{0}, null, std.math.maxInt(usize), null);
    try testing.expectEqual(@as(usize, 901), first.len);
}

test "distinct spill: a spill file that overflows again partitions one level deeper" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const input = try spillInput(a, 5000, 100, 4000);
    var td = try SpillDir.init(std.math.maxInt(u64));
    defer td.deinit();
    const want = try runSpillDistinct(a, input, &.{0}, null, std.math.maxInt(usize), null);
    try expectSameRows(want, try runSpillDistinct(a, input, &.{0}, td.ds.space(), 1, null));
    try testing.expect(td.ds.seq.load(.monotonic) > spill_parts.fanout);
}

test "distinct spill: past the disk cap fails with SpillCapExceeded" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const input = try spillInput(a, 3000, 100, 900);
    var td = try SpillDir.init(2048);
    defer td.deinit();
    var ec = ErrCtx{};
    try testing.expectError(error.SpillCapExceeded, runSpillDistinct(a, input, null, td.ds.space(), 1, &ec));
    try testing.expect(std.mem.indexOf(u8, ec.msg, "--spill-cap") != null);
    try testing.expectEqual(@as(usize, 0), try td.files());
}
