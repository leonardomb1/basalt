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
        .state = ta,
    };
    return buildChainFrom(ta, params, errctx, lj.suffix, .{ .join = j }, lj.out_schema.*);
}
