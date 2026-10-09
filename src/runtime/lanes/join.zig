//! The joins a lane probes: each build side indexed once and shared, every lane
//! running its own probe over it.

const Env = @import("../env.zig").Env;
const aErr = @import("../plan.zig").aErr;
const analyze = @import("../analyze.zig");
const ast = @import("../../lang/ast.zig");
const buildChainFrom = @import("../plan.zig").buildChainFrom;
const buildPipeline = @import("../plan.zig").buildPipeline;
const joinMaySpill = @import("../plan.zig").joinMaySpill;
const joinSpillAt = @import("../plan.zig").joinSpillAt;
const laneJoinCap = @import("../plan.zig").laneJoinCap;
const mapChainSchema = @import("../plan.zig").mapChainSchema;
const op = @import("../../exec/op.zig");
const planErr = @import("../env.zig").planErr;
const prepareJoinSide = @import("../plan.zig").prepareJoinSide;
const schemaPtr = @import("../env.zig").schemaPtr;
const std = @import("std");
const types = @import("../../lang/types.zig");

const LaneJoinChain = struct { joins: []const LaneJoin, out_schema: types.Schema };

/// Null when one join's build side sends the pipeline back to the serial plan; the
/// reads of the joins indexed before it are closed too.
pub fn resolveLaneJoins(env: *Env, join_span: []const ast.Stage, left_schema: types.Schema) anyerror!?LaneJoinChain {
    var list = std.array_list.Managed(LaneJoin).init(env.arena);
    var schema = left_schema;
    const src_base = env.sources.items.len;
    var i: usize = 0;
    while (i < join_span.len) {
        std.debug.assert(join_span[i].node == .join);
        var k = i + 1;
        while (k < join_span.len and join_span[k].node != .join) k += 1;
        const lp = (try resolveLaneJoin(env, join_span[i].node.join, join_span[i].hints, join_span[i + 1 .. k], schema)) orelse {
            for (env.sources.items[src_base..]) |sc| sc.close();
            env.sources.shrinkRetainingCapacity(src_base);
            return null;
        };
        try list.append(lp.lane);
        schema = lp.out_schema;
        i = k;
    }
    return .{ .joins = list.items, .out_schema = schema };
}

/// A key-index field of `analyze.JoinPlan` as a slice, accepting both the single and
/// the multi-key spelling so this builds against either snapshot.
fn planKeys(a: std.mem.Allocator, jp: analyze.JoinPlan, comptime which: []const u8) ![]const usize {
    const name: []const u8 = comptime if (@hasField(analyze.JoinPlan, which)) which else if (std.mem.eql(u8, which, "lk")) "lks" else "rks";
    const v = @field(jp, name);
    return if (@TypeOf(v) == usize) try a.dupe(usize, &[_]usize{v}) else v;
}

pub const LaneJoin = struct {
    index: *op.JoinIndex,
    left_keys: []const usize,
    right_keys: []const usize,
    /// What each lane's probe rows go through before the join (computing keys whose
    /// side the plan placed), and their schema before it.
    probe_prep: []const ast.Stage = &.{},
    probe_schema: *const types.Schema,
    /// The probe side's key columns by name, for tracing them to its read.
    left_key_names: []const ast.QualName = &.{},
    residual: ?*const ast.Expr = null,
    pair_schema: ?*const types.Schema = null,
    left_schema: *const types.Schema,
    right_schema: *const types.Schema,
    out_schema: *const types.Schema,
    kind: ast.JoinKind,
    null_aware: bool,
    suffix: []const ast.Stage,
    shared_matched: ?[]std.atomic.Value(u64) = null,
};

const LaneJoinPlan = struct { lane: LaneJoin, out_schema: types.Schema };

/// Hoist the join out of the fan-out: materialize the build side into a shared index
/// (the pulls are scratch) and prevalidate the suffix. Returns the lane recipe and the
/// sink's output schema, or null when the build side is past the spill threshold of
/// a join that could spill serially: its reads are closed and the caller falls back
/// to the serial plan, which reads that side again.
pub fn resolveLaneJoin(env: *Env, j: ast.Join, join_hints: []const ast.Hint, suffix: []const ast.Stage, left_schema: types.Schema) anyerror!?LaneJoinPlan {
    const arena = env.arena;
    const binding = env.bindings.get(j.binding) orelse
        return planErr(env.diag, try std.fmt.allocPrint(arena, "unknown binding `{s}` in join", .{j.binding}));
    const src_base = env.sources.items.len;
    const build = try buildPipeline(env, try prepareJoinSide(env, try j.rightStages(arena, binding.stages)));

    var ad = analyze.Diag{};
    const prep = analyze.orientKeys(arena, left_schema, build.schema, j, &ad) catch |e| return aErr(env, &ad, e);
    const lsch = try mapChainSchema(env, prep.left, left_schema);
    const rsch = try mapChainSchema(env, prep.right, build.schema);
    const jp = analyze.joinPlan(arena, lsch, rsch, prep.join, &ad) catch |e| return aErr(env, &ad, e);
    const out = try schemaPtr(arena, jp.schema);
    const right_schema = try schemaPtr(arena, rsch);
    const right_keys = try planKeys(arena, jp, "rk");

    const final_schema = try mapChainSchema(env, suffix, out.*);
    const rp = analyze.residualPlan(arena, lsch, rsch, prep.join, env.params_expr, &ad) catch |e| return aErr(env, &ad, e);

    var build_arena = std.heap.ArenaAllocator.init(env.gpa);
    defer build_arena.deinit();
    const build_op = try buildChainFrom(arena, env.params_expr, env.errctx, prep.right, build.op, build.schema);
    const index = op.JoinIndex.create(arena, build_arena.allocator(), build_op, right_schema, right_keys, try laneJoinCap(env, j, join_hints)) catch |e| {
        if (e != error.JoinBuildTooLarge or !joinMaySpill(env, j)) return e;
        for (env.sources.items[src_base..]) |sc| sc.close();
        env.sources.shrinkRetainingCapacity(src_base);
        env.log.log(.info, "join build side `{s}` is past {d} bytes; the join runs serially, where it can spill to disk", .{ j.binding, try joinSpillAt(env, join_hints) });
        return null;
    };
    const shared_matched: ?[]std.atomic.Value(u64) = if (j.kind == .right or j.kind == .full) blk: {
        const words = try arena.alloc(std.atomic.Value(u64), (index.rows() + 63) / 64);
        @memset(words, std.atomic.Value(u64).init(0));
        break :blk words;
    } else null;

    return .{
        .lane = .{
            .index = index,
            .left_keys = try planKeys(arena, jp, "lk"),
            .right_keys = right_keys,
            .probe_prep = prep.left,
            .left_key_names = prep.join.left_keys,
            .residual = if (rp) |r| r.pred else null,
            .pair_schema = if (rp) |r| try schemaPtr(arena, r.schema) else null,
            .probe_schema = try schemaPtr(arena, left_schema),
            .left_schema = try schemaPtr(arena, lsch),
            .right_schema = right_schema,
            .out_schema = out,
            .kind = j.kind,
            .null_aware = j.null_aware,
            .suffix = suffix,
            .shared_matched = shared_matched,
        },
        .out_schema = final_schema,
    };
}

pub fn buildLaneJoinChain(ta: std.mem.Allocator, params: *std.StringHashMap(*const ast.Expr), errctx: ?*op.ErrCtx, lj: LaneJoin, probe: op.Op) !op.Op {
    const j = try ta.create(op.Join);
    j.* = .{
        .probe = try buildChainFrom(ta, params, errctx, lj.probe_prep, probe, lj.probe_schema.*),
        .build = null,
        .index = lj.index,
        .left_keys = lj.left_keys,
        .right_keys = lj.right_keys,
        .left_schema = lj.left_schema,
        .right_schema = lj.right_schema,
        .out_schema = lj.out_schema,
        .kind = lj.kind,
        .null_aware = lj.null_aware,
        .residual = lj.residual,
        .pair_schema = lj.pair_schema,
        .shared_matched = lj.shared_matched,
        .state = ta,
    };
    return buildChainFrom(ta, params, errctx, lj.suffix, .{ .join = j }, lj.out_schema.*);
}

/// A right/full lane join's unmatched build rows, left side null, in build order
/// and through the join's suffix: a join over an empty probe whose match flags are
/// read from `shared_matched`, the one bitset (made by `resolveLaneJoin`) every
/// lane's probe marks. Null for every other kind. Call it once, after every lane
/// has finished probing.
pub fn buildLaneJoinDrain(ta: std.mem.Allocator, params: *std.StringHashMap(*const ast.Expr), errctx: ?*op.ErrCtx, lj: LaneJoin) !?op.Op {
    const words = lj.shared_matched orelse return null;
    const matched = try ta.alloc(bool, lj.index.rows());
    for (matched, 0..) |*m, r| m.* = words[r >> 6].load(.monotonic) & (@as(u64, 1) << @intCast(r & 63)) != 0;
    const none = try ta.create(op.Union);
    none.* = .{ .children = &.{} };
    const j = try ta.create(op.Join);
    j.* = .{
        .probe = .{ .union_ = none },
        .build = null,
        .index = lj.index,
        .left_keys = lj.left_keys,
        .right_keys = lj.right_keys,
        .left_schema = lj.left_schema,
        .right_schema = lj.right_schema,
        .out_schema = lj.out_schema,
        .kind = lj.kind,
        .null_aware = lj.null_aware,
        .residual = lj.residual,
        .pair_schema = lj.pair_schema,
        .matched = matched,
        .probe_done = true,
        .state = ta,
    };
    return try buildChainFrom(ta, params, errctx, lj.suffix, .{ .join = j }, lj.out_schema.*);
}

const op_testing = @import("../../exec/op/testing_util.zig");

const TestLane = struct {
    lj: LaneJoin,
    probe: []const op.Batch,
    params: *std.StringHashMap(*const ast.Expr),
    rows: usize = 0,
    err: ?anyerror = null,

    fn run(self: *TestLane) void {
        self.probeAll() catch |e| {
            self.err = e;
        };
    }

    fn probeAll(self: *TestLane) !void {
        var ar = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer ar.deinit();
        const a = ar.allocator();
        var ts = op_testing.TestSource{ .schema_ = op_testing.join_left_schema, .batches = self.probe };
        var scan = op.Scan{ .src = ts.src() };
        const chain = try buildLaneJoinChain(a, self.params, null, self.lj, .{ .scan = &scan });
        while (try chain.next(a)) |b| self.rows += b.len;
    }
};

test "lane joins: right and full lanes mark one shared bitset, and one drain emits each unmatched build row once, in build order" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var params = std.StringHashMap(*const ast.Expr).init(a);

    const rv = try a.create(ast.Expr);
    rv.* = .{ .field = .{ .parts = &.{"rv"} } };
    const x = try a.create(ast.Expr);
    x.* = .{ .str_lit = "x" };
    const not_x = try a.create(ast.Expr);
    not_x.* = .{ .binary = .{ .op = .ne, .l = rv, .r = x } };

    const build = try op_testing.kvBatch(a, &op_testing.join_right_schema, &.{ 1, 1, 2, 4, null }, &.{ "x", "y", "z", "w", "m" });
    const probes = [_][]const op.Batch{
        &.{try op_testing.kvBatch(a, &op_testing.join_left_schema, &.{1}, &.{"a"})},
        &.{try op_testing.kvBatch(a, &op_testing.join_left_schema, &.{ 2, null }, &.{ "b", "n" })},
        &.{try op_testing.kvBatch(a, &op_testing.join_left_schema, &.{ 1, 3 }, &.{ "c", "d" })},
        &.{},
    };

    const Case = struct { kind: ast.JoinKind, residual: ?*const ast.Expr, lane_rows: usize, drained: []const ?[]const u8 };
    const cases = [_]Case{
        .{ .kind = .right, .residual = null, .lane_rows = 5, .drained = &.{ "w", "m" } },
        .{ .kind = .full, .residual = null, .lane_rows = 7, .drained = &.{ "w", "m" } },
        .{ .kind = .right, .residual = not_x, .lane_rows = 3, .drained = &.{ "x", "w", "m" } },
        .{ .kind = .full, .residual = not_x, .lane_rows = 5, .drained = &.{ "x", "w", "m" } },
    };
    for (cases) |case| {
        const index = try op.JoinIndex.fromBatches(a, &.{build}, &op_testing.join_right_schema, &.{0}, 0, std.math.maxInt(usize));
        const words = try a.alloc(std.atomic.Value(u64), (index.rows() + 63) / 64);
        @memset(words, std.atomic.Value(u64).init(0));
        const lj = LaneJoin{
            .index = index,
            .left_keys = &.{0},
            .right_keys = &.{0},
            .probe_schema = &op_testing.join_left_schema,
            .residual = case.residual,
            .pair_schema = if (case.residual != null) &op_testing.join_both_schema else null,
            .left_schema = &op_testing.join_left_schema,
            .right_schema = &op_testing.join_right_schema,
            .out_schema = &op_testing.join_both_schema,
            .kind = case.kind,
            .null_aware = false,
            .suffix = &.{},
            .shared_matched = words,
        };

        var lanes: [probes.len]TestLane = undefined;
        var threads: [probes.len]std.Thread = undefined;
        for (&lanes, &threads, probes) |*l, *t, p| {
            l.* = .{ .lj = lj, .probe = p, .params = &params };
            t.* = try std.Thread.spawn(.{}, TestLane.run, .{l});
        }
        var lane_rows: usize = 0;
        for (&lanes, threads) |*l, t| {
            t.join();
            if (l.err) |e| return e;
            lane_rows += l.rows;
        }
        try std.testing.expectEqual(case.lane_rows, lane_rows);

        const drain = (try buildLaneJoinDrain(a, &params, null, lj)).?;
        const got = try op_testing.JoinRows.collect(a, drain, 3);
        const no_left = try a.alloc(?i64, case.drained.len);
        @memset(no_left, null);
        try got.expect(no_left, case.drained);
    }
}

test "lane joins: only right and full joins get a drain" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var params = std.StringHashMap(*const ast.Expr).init(a);
    const build = try op_testing.kvBatch(a, &op_testing.join_right_schema, &.{1}, &.{"x"});
    const index = try op.JoinIndex.fromBatches(a, &.{build}, &op_testing.join_right_schema, &.{0}, 0, std.math.maxInt(usize));
    const lj = LaneJoin{
        .index = index,
        .left_keys = &.{0},
        .right_keys = &.{0},
        .probe_schema = &op_testing.join_left_schema,
        .left_schema = &op_testing.join_left_schema,
        .right_schema = &op_testing.join_right_schema,
        .out_schema = &op_testing.join_both_schema,
        .kind = .left,
        .null_aware = false,
        .suffix = &.{},
    };
    try std.testing.expect((try buildLaneJoinDrain(a, &params, null, lj)) == null);
}
