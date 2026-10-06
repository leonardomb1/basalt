//! The joins a lane probes: each build side indexed once and shared, every lane
//! running its own probe over it.

const Env = @import("../env.zig").Env;
const aErr = @import("../plan.zig").aErr;
const analyze = @import("../analyze.zig");
const ast = @import("../../lang/ast.zig");
const buildChainFrom = @import("../plan.zig").buildChainFrom;
const buildPipeline = @import("../plan.zig").buildPipeline;
const joinBuildCap = @import("../plan.zig").joinBuildCap;
const mapChainSchema = @import("../plan.zig").mapChainSchema;
const op = @import("../../exec/op.zig");
const planErr = @import("../env.zig").planErr;
const prepareJoinSide = @import("../plan.zig").prepareJoinSide;
const schemaPtr = @import("../env.zig").schemaPtr;
const std = @import("std");
const types = @import("../../lang/types.zig");

const LaneJoinChain = struct { joins: []const LaneJoin, out_schema: types.Schema };

pub fn resolveLaneJoins(env: *Env, join_span: []const ast.Stage, left_schema: types.Schema) anyerror!LaneJoinChain {
    var list = std.array_list.Managed(LaneJoin).init(env.arena);
    var schema = left_schema;
    var i: usize = 0;
    while (i < join_span.len) {
        std.debug.assert(join_span[i].node == .join);
        var k = i + 1;
        while (k < join_span.len and join_span[k].node != .join) k += 1;
        const lp = try resolveLaneJoin(env, join_span[i].node.join, join_span[i].hints, join_span[i + 1 .. k], schema);
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
/// sink's output schema.
pub fn resolveLaneJoin(env: *Env, j: ast.Join, join_hints: []const ast.Hint, suffix: []const ast.Stage, left_schema: types.Schema) anyerror!LaneJoinPlan {
    const arena = env.arena;
    const binding = env.bindings.get(j.binding) orelse
        return planErr(env.diag, try std.fmt.allocPrint(arena, "unknown binding `{s}` in join", .{j.binding}));
    const build = try buildPipeline(env, try prepareJoinSide(env, try j.rightStages(arena, binding.stages)));

    var ad = analyze.Diag{};
    const jp = analyze.joinPlan(arena, left_schema, build.schema, j, &ad) catch |e| return aErr(env, &ad, e);
    const out = try schemaPtr(arena, jp.schema);
    const right_schema = try schemaPtr(arena, build.schema);
    const right_keys = try planKeys(arena, jp, "rk");

    const final_schema = try mapChainSchema(env, suffix, out.*);

    var build_arena = std.heap.ArenaAllocator.init(env.gpa);
    defer build_arena.deinit();
    const index = try op.JoinIndex.create(arena, build_arena.allocator(), build.op, right_schema, right_keys, try joinBuildCap(env, join_hints));

    return .{
        .lane = .{
            .index = index,
            .left_keys = try planKeys(arena, jp, "lk"),
            .right_keys = right_keys,
            .left_schema = try schemaPtr(arena, left_schema),
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
        .probe = probe,
        .build = null,
        .index = lj.index,
        .left_keys = lj.left_keys,
        .right_keys = lj.right_keys,
        .left_schema = lj.left_schema,
        .right_schema = lj.right_schema,
        .out_schema = lj.out_schema,
        .kind = lj.kind,
        .null_aware = lj.null_aware,
        .state = ta,
    };
    return buildChainFrom(ta, params, errctx, lj.suffix, .{ .join = j }, lj.out_schema.*);
}
