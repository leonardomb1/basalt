//! Join order from size estimates (`estimate.zig`), for the serial plan: which side
//! of an inner join is held in memory, and in which order a chain of inner joins
//! runs. The result is always the written query's: same columns, same names, same
//! rows; only the order of the rows may change, so either choice is made only
//! where something after the join reorders them anyway.
//!
//! Build side. An inner join whose left side is estimated at least `flip_ratio`
//! times smaller than its right (rows against rows, else bytes against bytes) is
//! flipped: the left side is indexed and the right side streams through it. The
//! flipped `op.Join` sees the right side as its probe; a `Project` above it puts
//! the columns back as left then right, under the written join's names (`_r`
//! suffixes included). Key pushdown follows the sides: a SQL right side that
//! takes the left side's keys gets them once the left side is indexed, not from a
//! read-ahead, and a SQL left read that takes the right side's keys gets them from
//! the right side's read-ahead. A left side whose estimated bytes pass the join's
//! spill threshold is not flipped, so the side moved into memory never spills;
//! a flipped join that spills anyway is still correct, inner joins being symmetric.
//!
//! Star chains. A run of two or more consecutive inner joins, nothing between them,
//! each keyed only on the columns of what precedes the run (the head), is run
//! smallest right side first when every side's row count is estimated. Each join's
//! keys are resolved against the head alone, in the written order and in the new
//! one, and must name the same columns all three ways; else the chain runs as
//! written. A `Project` restores the written column order and names.
//!
//! Row order. A join emits rows in its probe side's order, which `queries.md`
//! promises for a pipeline that only filters, projects and joins. So neither
//! choice is made unless an aggregate or a sort follows the join, with only
//! filters, selects, explodes and joins between (`orderFree`). A float `SUM` may
//! then differ in its last digits, as it does between `-j` runs.
//!
//! Not chosen here: semi, anti, outer and cross joins, NOT IN, a join with a
//! residual condition, a join under `WITH (join_order = 'written')`, and joins the
//! parallel lanes run (`lanes/join.zig` indexes the right side and probes with the
//! head read in every lane; a right side too large for that already sends the
//! pipeline back to the serial plan, where this applies). `EXPLAIN ANALYZE` (and
//! `run --explain`) prints the choice under the join.

const Env = @import("env.zig").Env;
const PipeRes = @import("env.zig").PipeRes;
const analyze = @import("analyze.zig");
const ast = @import("../lang/ast.zig");
const estimate = @import("estimate.zig");
const op = @import("../exec/op.zig");
const plan = @import("plan.zig");
const planErr = @import("env.zig").planErr;
const schemaPtr = @import("env.zig").schemaPtr;
const std = @import("std");
const types = @import("../lang/types.zig");

pub const flip_ratio = 4;

pub const Mode = enum { auto, written, invalid };

pub fn mode(hints: []const ast.Hint) Mode {
    for (hints) |h| {
        if (!std.mem.eql(u8, h.key, "join_order")) continue;
        const v = switch (h.value) {
            .str, .ident => |s| s,
            else => return .invalid,
        };
        if (std.ascii.eqlIgnoreCase(v, "written")) return .written;
        if (std.ascii.eqlIgnoreCase(v, "auto")) return .auto;
        return .invalid;
    }
    return .auto;
}

/// Whether the stages after a join reorder its rows anyway before anything sees
/// their order: an aggregate or a sort, reached through order-keeping stages only.
pub fn orderFree(after: []const ast.Stage) bool {
    for (after) |st| switch (st.node) {
        .filter, .select, .join, .explode => {},
        .aggregate, .sort => return true,
        else => return false,
    };
    return false;
}

/// A join that may be flipped or moved in a chain, its estimates aside.
pub fn eligible(st: ast.Stage) bool {
    if (st.node != .join) return false;
    const j = st.node.join;
    return j.kind == .inner and !j.null_aware and j.residual == null and !j.keyless() and mode(st.hints) == .auto;
}

/// Past the run of consecutive chainable joins starting at `si`.
pub fn chainEnd(stages: []const ast.Stage, si: usize) usize {
    var e = si;
    while (e < stages.len and eligible(stages[e]) and stages[e].node.join.deferred.len == 0) e += 1;
    return e;
}

/// The estimates side by side when the left side should be the one indexed.
pub fn decide(left: estimate.Estimate, right: estimate.Estimate, spill_at: usize) ?estimate.Pair {
    const p = estimate.pair(left, right) orelse return null;
    if (p.left == 0 or p.left > p.right / flip_ratio) return null;
    if (left.bytes) |b| if (b > spill_at) return null;
    return p;
}

/// What static `EXPLAIN` says of a join: estimates come only at run time.
pub fn staticNote(st: ast.Stage, after: []const ast.Stage) []const u8 {
    if (st.node != .join) return "";
    return switch (mode(st.hints)) {
        .written => "as written (join_order = 'written')",
        .invalid => "",
        .auto => if (eligible(st) and orderFree(after)) "the side estimated smaller is held in memory, decided at run time" else "",
    };
}

pub const Built = struct { res: PipeRes, next: usize };

/// The join at `stages[si]`, or the chain starting there, planned over `probe`.
pub fn buildAt(env: *Env, stages: []const ast.Stage, si: usize, probe: op.Op, schema: types.Schema, take: ?plan.ProbeTake) anyerror!Built {
    errdefer env.diag.stamp(stages[si].pos);
    const st = stages[si];
    if (mode(st.hints) == .invalid)
        return planErr(env.diag, "join_order is 'auto' or 'written'");
    if (take == null) {
        const end = chainEnd(stages, si);
        if (end - si >= 2 and orderFree(stages[end..]))
            return .{ .res = try buildChain(env, stages[si..end], probe, schema), .next = end };
    }
    const j = st.node.join;
    const side = try plan.joinSide(env, j, st.hints);
    const a = try plan.assembleJoin(env, j, st.hints, side, schema, probe, take);
    if (a.nl) |r| return .{ .res = r, .next = si + 1 };
    const plain: Built = .{ .res = .{ .op = .{ .join = a.o }, .schema = a.schema }, .next = si + 1 };
    if (!eligible(st) or a.o.residual != null or !orderFree(stages[si + 1 ..])) return plain;
    const left = estimate.ofSide(env, stages[0..si]);
    const right = estimate.ofSide(env, side.rstages);
    const spill_at = try plan.joinSpillAt(env, st.hints);
    const p = decide(left, right, spill_at) orelse {
        if (estimate.pair(left, right)) |q|
            a.o.note = try std.fmt.allocPrint(env.arena, "build: right (est. {s} vs {s})", .{ try estimate.describe(env.arena, q.right, q.unit), try estimate.describe(env.arena, q.left, q.unit) });
        return plain;
    };
    const note = try std.fmt.allocPrint(env.arena, "build: left (est. {s} vs {s})", .{ try estimate.describe(env.arena, p.left, p.unit), try estimate.describe(env.arena, p.right, p.unit) });
    env.log.log(.info, "join with `{s}`: the left side is held in memory, {s}", .{ j.alias, note["build: left ".len..] });
    return .{ .res = try flipped(env.arena, a.o, a.schema, note, env.errctx), .next = si + 1 };
}

/// `o` turned around, its right side probing an index of its left side, under a
/// projection that gives back the written columns in the written order.
pub fn flipped(arena: std.mem.Allocator, o: *const op.Join, schema: types.Schema, note: []const u8, err: ?*op.ErrCtx) !PipeRes {
    const nl = o.left_schema.fields.len;
    const nr = o.right_schema.fields.len;
    const fields = try arena.alloc(types.Schema.Field, nr + nl);
    @memcpy(fields[0..nr], o.right_schema.fields);
    @memcpy(fields[nr..], o.left_schema.fields);
    const f = try arena.create(op.Join);
    f.* = o.*;
    f.probe = o.build.?;
    f.build = o.probe;
    f.left_keys = o.right_keys;
    f.right_keys = o.left_keys;
    f.left_schema = o.right_schema;
    f.right_schema = o.left_schema;
    f.out_schema = try schemaPtr(arena, .{ .fields = fields });
    f.push_build = o.push_probe;
    f.push_probe = o.push_build;
    f.note = note;
    const cols = try arena.alloc(op.Project.Col, nl + nr);
    for (cols, 0..) |*c, i| c.* = .{
        .source = .{ .passthrough = if (i < nl) nr + i else i - nl },
        .ty = schema.fields[i].ty,
    };
    const p = try arena.create(op.Project);
    p.* = .{ .child = .{ .join = f }, .cols = cols, .out_schema = o.out_schema, .err = err };
    return .{ .op = .{ .project = p }, .schema = schema };
}

/// A star chain run smallest side first, else as written; either way its output
/// is the written chain's.
fn buildChain(env: *Env, chain: []const ast.Stage, probe: op.Op, head: types.Schema) anyerror!PipeRes {
    const arena = env.arena;
    const sides = try arena.alloc(plan.JoinSide, chain.len);
    for (chain, sides) |st, *s| s.* = try plan.joinSide(env, st.node.join, st.hints);

    const order = (try starOrder(env, chain, sides, head)) orelse {
        var cur = probe;
        var sch = head;
        for (chain, sides) |st, s| {
            const a = try plan.assembleJoin(env, st.node.join, st.hints, s, sch, cur, null);
            cur = .{ .join = a.o };
            sch = a.schema;
        }
        return .{ .op = cur, .schema = sch };
    };

    var cur = probe;
    var sch = head;
    const at = try arena.alloc(usize, chain.len);
    for (order.ks, 0..) |k, n| {
        const st = chain[k];
        at[k] = sch.fields.len;
        const a = try plan.assembleJoin(env, st.node.join, st.hints, sides[k], sch, cur, null);
        a.o.note = try std.fmt.allocPrint(arena, "joined {d} of {d}, written {d} (est. {s})", .{ n + 1, chain.len, k + 1, try estimate.describe(arena, order.rows[k], .rows) });
        cur = .{ .join = a.o };
        sch = a.schema;
    }
    env.log.log(.info, "{d} inner joins run smallest side first, not as written", .{chain.len});

    const written = order.schema;
    const cols = try arena.alloc(op.Project.Col, written.fields.len);
    for (cols[0..head.fields.len], 0..) |*c, i| c.* = .{ .source = .{ .passthrough = i }, .ty = written.fields[i].ty };
    var w = head.fields.len;
    for (sides, at) |s, start| for (0..s.build.schema.fields.len) |c| {
        cols[w] = .{ .source = .{ .passthrough = start + c }, .ty = written.fields[w].ty };
        w += 1;
    };
    const p = try arena.create(op.Project);
    p.* = .{ .child = cur, .cols = cols, .out_schema = try schemaPtr(arena, written), .err = env.errctx };
    return .{ .op = .{ .project = p }, .schema = written };
}

const Order = struct { ks: []const usize, rows: []const u64, schema: types.Schema };

/// The chain's order by estimated rows, smallest first, with the written chain's
/// schema; null to keep it as written: a side not estimated in rows, the order
/// unchanged, or a key that would not name the same columns in both orders.
fn starOrder(env: *Env, chain: []const ast.Stage, sides: []const plan.JoinSide, head: types.Schema) !?Order {
    const arena = env.arena;
    var ad = analyze.Diag{};
    const rows = try arena.alloc(u64, chain.len);
    const alone = try arena.alloc(analyze.JoinPlan, chain.len);
    for (chain, sides, rows, alone) |st, s, *r, *jp| {
        r.* = estimate.ofSide(env, s.rstages).rows orelse return null;
        jp.* = analyze.joinPlan(arena, head, s.build.schema, st.node.join, &ad) catch return null;
        if (jp.lks.len == 0) return null;
        for (jp.lks) |k| if (k >= head.fields.len) return null;
    }
    const ks = try arena.alloc(usize, chain.len);
    for (ks, 0..) |*k, i| k.* = i;
    std.sort.insertion(usize, ks, rows, struct {
        fn lt(r: []const u64, a: usize, b: usize) bool {
            return r[a] < r[b];
        }
    }.lt);
    for (ks, 0..) |k, i| {
        if (k != i) break;
    } else return null;

    const written = (try sameKeys(arena, chain, sides, head, alone, null)) orelse return null;
    _ = (try sameKeys(arena, chain, sides, head, alone, ks)) orelse return null;
    return .{ .ks = ks, .rows = rows, .schema = written };
}

/// The chain's schema joined in `order` (written when null), or null when a join's
/// keys there differ from its keys against the head alone.
fn sameKeys(arena: std.mem.Allocator, chain: []const ast.Stage, sides: []const plan.JoinSide, head: types.Schema, alone: []const analyze.JoinPlan, order: ?[]const usize) !?types.Schema {
    var ad = analyze.Diag{};
    var sch = head;
    for (0..chain.len) |n| {
        const k = if (order) |o| o[n] else n;
        const jp = analyze.joinPlan(arena, sch, sides[k].build.schema, chain[k].node.join, &ad) catch return null;
        if (!std.mem.eql(usize, jp.lks, alone[k].lks) or !std.mem.eql(usize, jp.rks, alone[k].rks)) return null;
        sch = jp.schema;
    }
    return sch;
}

const testing = std.testing;

fn stage(node: ast.Stage.Node, hints: []const ast.Hint) ast.Stage {
    return .{ .node = node, .hints = hints, .pos = .{ .line = 0, .col = 0 } };
}

test "joinorder: only an aggregate or a sort after the join frees its row order" {
    const f = stage(.{ .filter = undefined }, &.{});
    const agg = stage(.{ .aggregate = .{ .aggs = &.{}, .by = &.{} } }, &.{});
    const srt = stage(.{ .sort = .{ .keys = &.{} } }, &.{});
    const lim = stage(.{ .limit = .{ .count = 1 } }, &.{});
    const dis = stage(.{ .distinct = .{ .on = null } }, &.{});
    try testing.expect(orderFree(&.{ f, agg }));
    try testing.expect(orderFree(&.{srt}));
    try testing.expect(!orderFree(&.{}));
    try testing.expect(!orderFree(&.{f}));
    try testing.expect(!orderFree(&.{ lim, srt }));
    try testing.expect(!orderFree(&.{ dis, agg }));
}

test "joinorder: join_order hint and which joins are eligible" {
    const k = [_]ast.QualName{.{ .parts = &.{"k"} }};
    const ij = ast.Join{ .kind = .inner, .binding = "b", .left_keys = &k, .right_keys = &k };
    const keyless = ast.Join{ .kind = .inner, .binding = "b", .left_keys = &.{}, .right_keys = &.{} };
    var lj = ij;
    lj.kind = .left;
    var na = ij;
    na.null_aware = true;
    const written = [_]ast.Hint{.{ .key = "join_order", .value = .{ .str = "written" }, .pos = .{ .line = 0, .col = 0 } }};
    const auto = [_]ast.Hint{.{ .key = "join_order", .value = .{ .ident = "AUTO" }, .pos = .{ .line = 0, .col = 0 } }};
    const bad = [_]ast.Hint{.{ .key = "join_order", .value = .{ .str = "best" }, .pos = .{ .line = 0, .col = 0 } }};
    try testing.expectEqual(Mode.written, mode(&written));
    try testing.expectEqual(Mode.auto, mode(&auto));
    try testing.expectEqual(Mode.invalid, mode(&bad));
    try testing.expect(eligible(stage(.{ .join = ij }, &.{})));
    try testing.expect(eligible(stage(.{ .join = ij }, &auto)));
    try testing.expect(!eligible(stage(.{ .join = ij }, &written)));
    try testing.expect(!eligible(stage(.{ .join = lj }, &.{})));
    try testing.expect(!eligible(stage(.{ .join = na }, &.{})));
    try testing.expect(!eligible(stage(.{ .join = keyless }, &.{})));

    const sel = stage(.{ .select = &.{} }, &.{});
    const chain = [_]ast.Stage{ sel, stage(.{ .join = ij }, &.{}), stage(.{ .join = ij }, &.{}), stage(.{ .join = lj }, &.{}), stage(.{ .join = ij }, &.{}) };
    try testing.expectEqual(@as(usize, 3), chainEnd(&chain, 1));
    try testing.expectEqual(@as(usize, 3), chainEnd(&chain, 2));
    try testing.expectEqual(@as(usize, 3), chainEnd(&chain, 3));
    try testing.expectEqual(@as(usize, 0), chainEnd(&chain, 0));

    const agg = stage(.{ .aggregate = .{ .aggs = &.{}, .by = &.{} } }, &.{});
    try testing.expectEqualStrings("as written (join_order = 'written')", staticNote(stage(.{ .join = ij }, &written), &.{agg}));
    try testing.expect(staticNote(stage(.{ .join = ij }, &.{}), &.{agg}).len > 0);
    try testing.expectEqualStrings("", staticNote(stage(.{ .join = ij }, &.{}), &.{}));
}

test "joinorder: the left side is indexed only when known to be flip_ratio times smaller and under the spill threshold" {
    try testing.expect(decide(.{ .rows = 10 }, .{ .rows = 40 }, 1 << 30) != null);
    try testing.expect(decide(.{ .rows = 10 }, .{ .rows = 39 }, 1 << 30) == null);
    try testing.expect(decide(.{ .rows = 0 }, .{ .rows = 39 }, 1 << 30) == null);
    try testing.expect(decide(.{ .rows = 40 }, .{ .rows = 10 }, 1 << 30) == null);
    try testing.expect(decide(.{}, .{ .rows = 1000 }, 1 << 30) == null);
    try testing.expect(decide(.{ .rows = 10 }, .{ .bytes = 1000 }, 1 << 30) == null);
    try testing.expectEqual(estimate.Unit.bytes, decide(.{ .bytes = 100 }, .{ .bytes = 1000 }, 1 << 30).?.unit);
    try testing.expect(decide(.{ .rows = 10, .bytes = 5000 }, .{ .rows = 1000 }, 4096) == null);
    try testing.expect(decide(.{ .rows = std.math.maxInt(u64) / 2 }, .{ .rows = std.math.maxInt(u64) }, 1 << 30) == null);
}

test "joinorder: a flipped join gives the written columns and rows, and each side's keys go where they did" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const tu = @import("../exec/op/testing_util.zig");
    const keyset = @import("../exec/op/keyset.zig");
    const Rec = struct {
        got: ?[]const keyset.KeyValues = null,
        fn apply(ctx: *anyopaque, keys: []const keyset.KeyValues) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.got = keys;
        }
    };
    const Rows = struct {
        fn of(al: std.mem.Allocator, top: op.Op) ![]const []const u8 {
            var out = std.array_list.Managed([]const u8).init(al);
            while (try top.next(al)) |b| {
                try testing.expectEqualStrings("lv", b.schema.fields[1].name);
                try testing.expectEqualStrings("rk", b.schema.fields[2].name);
                for (0..b.len) |r| {
                    var line = std.array_list.Managed(u8).init(al);
                    for (b.columns) |c| switch (c.getValue(r)) {
                        .int => |v| try line.writer().print("{d}|", .{v}),
                        .string => |v| try line.writer().print("{s}|", .{v}),
                        else => try line.appendSlice("null|"),
                    };
                    try out.append(line.items);
                }
            }
            std.mem.sort([]const u8, out.items, {}, struct {
                fn lt(_: void, x: []const u8, y: []const u8) bool {
                    return std.mem.order(u8, x, y) == .lt;
                }
            }.lt);
            return out.items;
        }
    };

    var got: [2][]const []const u8 = undefined;
    var to_left: [2]Rec = .{ .{}, .{} };
    var to_right: [2]Rec = .{ .{}, .{} };
    for (0..2) |flip| {
        const lb = [_]tu.Batch{try tu.kvBatch(a, &tu.join_left_schema, &.{ 1, 2, null, 1 }, &.{ "a", "b", "n", "c" })};
        const rb = [_]tu.Batch{ try tu.kvBatch(a, &tu.join_right_schema, &.{ 1, 1, 4 }, &.{ "x", "y", "z" }), try tu.kvBatch(a, &tu.join_right_schema, &.{ null, 2, 5 }, &.{ "m", "w", "q" }) };
        var lts = tu.TestSource{ .schema_ = tu.join_left_schema, .batches = &lb };
        var rts = tu.TestSource{ .schema_ = tu.join_right_schema, .batches = &rb };
        var lscan = op.Scan{ .src = lts.src() };
        var rscan = op.Scan{ .src = rts.src() };
        var jn = op.Join{
            .probe = .{ .scan = &lscan },
            .build = .{ .scan = &rscan },
            .left_keys = &.{0},
            .right_keys = &.{0},
            .left_schema = &tu.join_left_schema,
            .right_schema = &tu.join_right_schema,
            .out_schema = &tu.join_both_schema,
            .kind = .inner,
            .state = a,
            .push_build = .{ .ctx = &to_right[flip], .apply = Rec.apply },
            .push_probe = .{ .ctx = &to_left[flip], .apply = Rec.apply },
        };
        const top: op.Op = if (flip == 1) (try flipped(a, &jn, tu.join_both_schema, "build: left", null)).op else .{ .join = &jn };
        got[flip] = try Rows.of(a, top);
    }
    try testing.expectEqual(@as(usize, 5), got[0].len);
    for (got[0], got[1]) |x, y| try testing.expectEqualStrings(x, y);
    for (0..2) |flip| {
        try testing.expectEqual(@as(usize, 2), to_right[flip].got.?[0].values.len);
        try testing.expectEqual(@as(usize, 4), to_left[flip].got.?[0].values.len);
    }
}
