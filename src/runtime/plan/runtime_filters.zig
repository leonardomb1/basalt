//! Placing runtime filters (`exec/op/runtime_filter.zig`) in a planned operator
//! tree. A hash join that drops the probe rows matching nothing (run as inner,
//! semi or right, never `NOT IN`) is an owner. Each owner's probe key columns are
//! followed down its probe side: through filters, through projections where they
//! pass through, and through each earlier hash join, into its probe side when they
//! are all its left columns, or into its build side when they are all its right
//! columns and it outputs them (inner, left, right or full). Where every key reaches
//! one scan with at least one join in between, that scan gets a runtime filter (one
//! per scan) holding a slot for the owner. The first join over a scan gets none: its
//! own probe is that check, exact. Through a build side the filter narrows what the
//! earlier join indexes: `orders` read already without the orders of customers a
//! later customer join rejects.
//!
//! Sound because a row dropped at the scan reaches the owner, in every row made from
//! it, with the same key values: only the side holding the keys is followed, through
//! operators that keep a row's columns. A row the earlier join then leaves
//! unmatched, and an outer join emits with this side null, reaches the owner with a
//! null key, which matches nothing. Nothing that counts, orders or numbers rows
//! (limit, top-N, window, distinct, aggregate, sort) lies on the path, nor an
//! explode or a union, so the owner's output, and its order, is what it was. `WITH
//! (runtime_filter = false)` on a join keeps it from owning a filter.
//!
//! A filter through a build side helps only if the owner indexes its own side
//! before that build side is read, which pulling top-down gives, unless the owner
//! first reads its probe ahead for key pushdown (`op.Join.push_build`). For a
//! Parquet right side, which only skips row groups by it, the filter wins and the
//! read-ahead is dropped; a SQL right side keeps its read-ahead, saving a remote
//! read, and the filter is not placed.

const ast = @import("../../lang/ast.zig");
const op = @import("../../exec/op.zig");
const std = @import("std");
const textconv = @import("../../exec/textconv.zig");

/// Whether `WITH (runtime_filter = false)` turned the join's runtime filter off.
pub fn disabled(hints: []const ast.Hint) bool {
    for (hints) |h| {
        if (!std.mem.eql(u8, h.key, "runtime_filter")) continue;
        return switch (h.value) {
            .ident, .str => |s| !textconv.boolText(.runtime_filter, s).?,
            .int => |i| i == 0,
            .flag => false,
        };
    }
    return false;
}

/// Gives every owner in the tree under `root` its slot, adding runtime filters
/// above the scans they reach; plan memory in `arena`. Wiring a tree again adds
/// nothing.
pub fn wire(arena: std.mem.Allocator, root: *op.Op) !void {
    switch (root.*) {
        .scan => {},
        .join => |j| {
            try own(arena, j);
            try wire(arena, &j.probe);
            if (j.build != null) try wire(arena, &j.build.?);
        },
        .nl_join => |j| {
            try wire(arena, &j.probe);
            if (j.build != null) try wire(arena, &j.build.?);
        },
        .union_ => |u| for (@constCast(u.children)) |*c| try wire(arena, c),
        inline else => |o| try wire(arena, &o.child),
    }
}

fn owns(j: *const op.Join) bool {
    if (j.filter_off or j.null_aware or j.left_keys.len == 0) return false;
    return switch (j.kind) {
        .inner, .semi, .right => true,
        else => false,
    };
}

/// Whether a join outputs its build side's columns, after its probe side's.
fn keepsRight(j: *const op.Join) bool {
    if (j.build == null) return false;
    return switch (j.kind) {
        .inner, .left, .right, .full => true,
        else => false,
    };
}

/// The owner's probe keys followed to a scan; a slot there when they all reach it
/// past another join.
fn own(arena: std.mem.Allocator, j: *op.Join) !void {
    if (!owns(j)) return;
    const cols = try arena.dupe(usize, j.left_keys);
    var cur: *op.Op = &j.probe;
    var joins: usize = 0;
    var built = false;
    while (true) switch (cur.*) {
        .filter => |f| cur = &f.child,
        .project => |p| {
            for (cols) |*c| switch (p.cols[c.*].source) {
                .passthrough => |i| c.* = i,
                .expr => return,
            };
            cur = &p.child;
        },
        .join => |pj| {
            const nl = pj.left_schema.fields.len;
            var left: usize = 0;
            for (cols) |c| left += @intFromBool(c < nl);
            joins += 1;
            if (left == cols.len) {
                cur = &pj.probe;
            } else if (left == 0 and keepsRight(pj)) {
                for (cols) |*c| c.* -= nl;
                built = true;
                cur = &pj.build.?;
            } else return;
        },
        .runtime_filter => |rf| {
            if (rf.child != .scan) {
                cur = &rf.child;
                continue;
            }
            if (joins == 0 or !readsFirst(j, built)) return;
            return addSlot(arena, rf, j, cols);
        },
        .scan => |s| {
            if (joins == 0 or !readsFirst(j, built)) return;
            const rf = try arena.create(op.RuntimeFilter);
            rf.* = .{ .child = cur.*, .back = arena };
            rf.stats.at = s.stats.at;
            cur.* = .{ .runtime_filter = rf };
            return addSlot(arena, rf, j, cols);
        },
        else => return,
    };
}

/// Whether the owner's index is built before a filter through a build side reads:
/// a Parquet read-ahead for key pushdown is dropped for it, a SQL one is kept and
/// the filter not placed.
fn readsFirst(j: *op.Join, built: bool) bool {
    if (!built or j.push_build == null) return true;
    if (!j.push_build_yields) return false;
    j.push_build = null;
    return true;
}

/// The owner's slot in `rf`, once: a tree is wired again when a pipeline wired on
/// its own becomes part of a larger one.
fn addSlot(arena: std.mem.Allocator, rf: *op.RuntimeFilter, j: *op.Join, cols: []const usize) !void {
    for (rf.slots.items) |sl| if (sl.index == &j.index) return;
    try rf.slots.append(arena, .{ .index = &j.index, .cols = cols, .label = if (j.name.len > 0) j.name else "?" });
    var b = std.array_list.Managed(u8).init(arena);
    try b.appendSlice(if (rf.slots.items.len == 1) "for the join with " else "for the joins with ");
    for (rf.slots.items, 0..) |sl, i| {
        if (i > 0) try b.appendSlice(", ");
        try b.appendSlice(sl.label);
    }
    rf.note = b.items;
}

const testing = std.testing;
const util = @import("../../exec/op/testing_util.zig");
const types = @import("../../lang/types.zig");

const chain_schema = types.Schema{ .fields = &.{
    .{ .name = "lk", .ty = types.Type.init(.int).asNullable() },
    .{ .name = "lv", .ty = types.Type.init(.string).asNullable() },
    .{ .name = "rk", .ty = types.Type.init(.int).asNullable() },
    .{ .name = "rv", .ty = types.Type.init(.string).asNullable() },
    .{ .name = "rk2", .ty = types.Type.init(.int).asNullable() },
    .{ .name = "rv2", .ty = types.Type.init(.string).asNullable() },
} };

/// `head ⋈ b1 ⋈ b2`, the second join keyed on column `key` of the first's output,
/// run with or without wiring; what came out, and the filter placed, if any.
const Chain = struct {
    head: []const util.Batch,
    b1: []const util.Batch,
    b2: []const util.Batch,
    kind1: ast.JoinKind = .inner,
    kind2: ast.JoinKind = .inner,
    keys: []const usize = &.{0},
    off: bool = false,
    /// The owner reads its probe ahead for key pushdown: true for a Parquet right
    /// side, which yields to a filter, false for a SQL one.
    push: ?bool = null,

    const Ran = struct { rows: util.JoinRows, rf: ?*op.RuntimeFilter, rf_build: ?*op.RuntimeFilter };

    fn run(self: Chain, a: std.mem.Allocator, wired: bool) !Ran {
        var hs = util.TestSource{ .schema_ = util.join_left_schema, .batches = self.head };
        var s1 = util.TestSource{ .schema_ = util.join_right_schema, .batches = self.b1 };
        var s2 = util.TestSource{ .schema_ = util.join_right_schema, .batches = self.b2 };
        const hscan = try a.create(op.Scan);
        hscan.* = .{ .src = hs.src() };
        const scan1 = try a.create(op.Scan);
        scan1.* = .{ .src = s1.src() };
        const scan2 = try a.create(op.Scan);
        scan2.* = .{ .src = s2.src() };
        const j1 = try a.create(op.Join);
        j1.* = .{ .probe = .{ .scan = hscan }, .build = .{ .scan = scan1 }, .left_keys = &.{0}, .right_keys = &.{0}, .left_schema = &util.join_left_schema, .right_schema = &util.join_right_schema, .out_schema = &util.join_both_schema, .kind = self.kind1, .state = a, .name = "b1" };
        const keys = try a.dupe(usize, self.keys);
        const rkeys = try a.alloc(usize, keys.len);
        @memset(rkeys, 0);
        const j2 = try a.create(op.Join);
        j2.* = .{ .probe = .{ .join = j1 }, .build = .{ .scan = scan2 }, .left_keys = keys, .right_keys = rkeys, .left_schema = &util.join_both_schema, .right_schema = &util.join_right_schema, .out_schema = &chain_schema, .kind = self.kind2, .state = a, .name = "b2", .filter_off = self.off };
        if (self.push) |yields| {
            j2.push_build = .{ .ctx = j2, .apply = ignoreKeys };
            j2.push_build_yields = yields;
        }
        var root: op.Op = .{ .join = j2 };
        if (wired) {
            try wire(a, &root);
            try wire(a, &root);
        }
        const rf: ?*op.RuntimeFilter = if (j1.probe == .runtime_filter) j1.probe.runtime_filter else null;
        const rf_build: ?*op.RuntimeFilter = if (j1.build.? == .runtime_filter) j1.build.?.runtime_filter else null;
        return .{ .rows = try util.JoinRows.collect(a, root, 5), .rf = rf, .rf_build = rf_build };
    }
};

fn ignoreKeys(_: *anyopaque, _: []const op.KeyValues) anyerror!void {}

fn headRows(a: std.mem.Allocator, n: usize) ![]const util.Batch {
    const ks = try a.alloc(?i64, n + 1);
    const vs = try a.alloc(?[]const u8, n + 1);
    for (ks[0..n], vs[0..n], 0..) |*k, *v, i| {
        k.* = @intCast(i);
        v.* = try std.fmt.allocPrint(a, "v{d}", .{i});
    }
    ks[n] = null;
    vs[n] = "nul";
    const out = try a.alloc(util.Batch, 1);
    out[0] = try util.kvBatch(a, &util.join_left_schema, ks, vs);
    return out;
}

fn buildRows(a: std.mem.Allocator, keys: []const ?i64) ![]const util.Batch {
    const vs = try a.alloc(?[]const u8, keys.len);
    for (vs, keys) |*v, k| v.* = if (k) |x| try std.fmt.allocPrint(a, "b{d}", .{x}) else null;
    const out = try a.alloc(util.Batch, 1);
    out[0] = try util.kvBatch(a, &util.join_right_schema, keys, vs);
    return out;
}

fn expectSame(x: util.JoinRows, y: util.JoinRows) !void {
    try testing.expectEqualSlices(?i64, x.keys.items, y.keys.items);
    try testing.expectEqual(x.rvs.items.len, y.rvs.items.len);
    for (x.rvs.items, y.rvs.items) |p, q| {
        if (p == null or q == null) try testing.expectEqual(p == null, q == null) else try testing.expectEqualStrings(p.?, q.?);
    }
}

test "runtime filters: the later join's filter sits on the head scan, drops what it will drop, and changes nothing" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const all = try a.alloc(?i64, 200);
    for (all, 0..) |*k, i| k.* = @intCast(i);
    const c = Chain{ .head = try headRows(a, 200), .b1 = try buildRows(a, all), .b2 = try buildRows(a, &.{ 3, 50, 51, 150, null }) };
    const plain = try c.run(a, false);
    const wired = try c.run(a, true);
    try expectSame(plain.rows, wired.rows);
    try testing.expectEqual(@as(usize, 4), wired.rows.keys.items.len);
    const rf = wired.rf.?;
    try testing.expectEqual(@as(usize, 1), rf.slots.items.len);
    try testing.expectEqualStrings("for the join with b2", rf.note);
    const t = rf.tally();
    try testing.expectEqual(@as(u64, 201), t.checked);
    try testing.expect(t.dropped >= 190);
    try testing.expect(plain.rf == null);
}

test "runtime filters: a key from an earlier join's right side filters that join's build side, for every kind that outputs it" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const all = try a.alloc(?i64, 120);
    for (all, 0..) |*k, i| k.* = @intCast(i);
    for ([_]ast.JoinKind{ .inner, .left, .right, .full }) |kind1| {
        const c = Chain{ .head = try headRows(a, 100), .b1 = try buildRows(a, all), .b2 = try buildRows(a, &.{ 3, 7, 110, null }), .kind1 = kind1, .keys = &.{2} };
        const plain = try c.run(a, false);
        const wired = try c.run(a, true);
        try expectSame(plain.rows, wired.rows);
        try testing.expect(wired.rf == null);
        const t = wired.rf_build.?.tally();
        try testing.expectEqual(@as(u64, 120), t.checked);
        try testing.expect(t.dropped >= 110);
    }
}

test "runtime filters: none for keys split over both sides of an earlier join, an owner that keeps unmatched rows, or one turned off" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const all = try a.alloc(?i64, 50);
    for (all, 0..) |*k, i| k.* = @intCast(i);
    const base = Chain{ .head = try headRows(a, 50), .b1 = try buildRows(a, all), .b2 = try buildRows(a, &.{ 3, 7 }) };
    for ([_]Chain{
        blk: {
            var c = base;
            c.keys = &.{ 0, 2 };
            break :blk c;
        },
        blk: {
            var c = base;
            c.kind2 = .left;
            break :blk c;
        },
        blk: {
            var c = base;
            c.off = true;
            break :blk c;
        },
    }) |c| {
        const plain = try c.run(a, false);
        const wired = try c.run(a, true);
        try testing.expect(wired.rf == null);
        try testing.expect(wired.rf_build == null);
        try expectSame(plain.rows, wired.rows);
    }
}

test "runtime filters: a filter that drops almost nothing turns itself off, rows unchanged" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const n = @import("../../exec/op/runtime_filter.zig").min_checked + 5000;
    const all = try a.alloc(?i64, n);
    for (all, 0..) |*k, i| k.* = @intCast(i);
    const c = Chain{ .head = try headRows(a, n), .b1 = try buildRows(a, all), .b2 = try buildRows(a, all) };
    const wired = try c.run(a, true);
    try testing.expectEqual(@as(usize, n), wired.rows.keys.items.len);
    try testing.expectEqual(@as(u64, 1), wired.rf.?.tally().off);
}

test "runtime filters: through a build side only when the owner indexes first: a Parquet read-ahead yields, a SQL one stays" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const all = try a.alloc(?i64, 40);
    for (all, 0..) |*k, i| k.* = @intCast(i);
    const base = Chain{ .head = try headRows(a, 40), .b1 = try buildRows(a, all), .b2 = try buildRows(a, &.{ 3, 7 }), .keys = &.{2} };
    var pq = base;
    pq.push = true;
    const yielded = try pq.run(a, true);
    try testing.expect(yielded.rf_build != null);
    var sq = base;
    sq.push = false;
    const kept = try sq.run(a, true);
    try testing.expect(kept.rf_build == null);
    try expectSame(yielded.rows, kept.rows);
}
