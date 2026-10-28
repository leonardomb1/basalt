//! The joins a lane probes: each build side indexed once and shared, every lane
//! running its own probe over it.

const Env = @import("../env.zig").Env;
const Profile = @import("../../exec/profile.zig").Profile;
const aErr = @import("../env.zig").aErr;
const analyze = @import("../analyze.zig");
const ast = @import("../../lang/ast.zig");
const chainAt = @import("../plan.zig").chainAt;
const chainOps = @import("../plan.zig").chainOps;
const buildPipeline = @import("../plan.zig").buildPipeline;
const joinMaySpill = @import("../plan.zig").joinMaySpill;
const joinSpillAt = @import("../plan.zig").joinSpillAt;
const stampedBinding = @import("../plan.zig").stampedBinding;
const laneJoinCap = @import("../plan.zig").laneJoinCap;
const op = @import("../../exec/op.zig");
const planErr = @import("../env.zig").planErr;
const prepareJoinSide = @import("../plan.zig").prepareJoinSide;
const profile = @import("../../exec/profile.zig");
const schemaPtr = @import("../env.zig").schemaPtr;
const std = @import("std");
const msgfmt = @import("../../format/msgfmt.zig");
const trace = @import("../../exec/trace.zig");
const types = @import("../../lang/types.zig");
const Batch = @import("../../exec/batch.zig").Batch;
const LaneMem = @import("../parallel.zig").LaneMem;
const Morsels = @import("morsels.zig").Morsels;
const ReaderSource = @import("sources.zig").ReaderSource;
const WorkQueue = @import("../parallel.zig").WorkQueue;
const HeldFile = @import("morsels.zig").HeldFile;
const openItem = @import("morsels.zig").openItem;
const obs = @import("../obs.zig");
const parallel = @import("../parallel.zig");
const parquetSplit = @import("sources.zig").parquetSplit;

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
        var lane = lp.lane;
        lane.at = profile.keyOf(join_span[i].pos);
        try list.append(lane);
        schema = lp.out_schema;
        i = k;
    }
    return .{ .joins = list.items, .out_schema = schema };
}

pub const LaneJoin = struct {
    index: *op.JoinIndex,
    left_keys: []const usize,
    right_keys: []const usize,
    /// What each lane's probe rows go through before the join (computing keys whose
    /// side the plan placed).
    probe_prep: []const analyze.Resolved = &.{},
    /// The probe side's key columns by name, for tracing them to its read.
    left_key_names: []const ast.QualName = &.{},
    residual: ?*const ast.Expr = null,
    pair_schema: ?*const types.Schema = null,
    left_schema: *const types.Schema,
    right_schema: *const types.Schema,
    out_schema: *const types.Schema,
    kind: ast.JoinKind,
    null_aware: bool,
    suffix: []const analyze.Resolved,
    shared_matched: ?[]std.atomic.Value(u64) = null,
    /// The join stage, for the profile; 0 takes the pipeline's one join.
    at: u64 = 0,
    /// The profile's node for the chain that built `index`, which the lanes' joins
    /// show as their build side.
    build_root: ?profile.Id = null,
};

const LaneJoinPlan = struct { lane: LaneJoin, out_schema: types.Schema };

/// The build side's Parquet split when it is a local Parquet read followed only by
/// filters and projections, so the lanes read it as they read a probe side; else null
/// and the build side reads serially.
fn buildSplit(env: *Env, rstages: []const ast.Stage) !?Morsels {
    if (env.pipe.threads < 2 or rstages.len == 0 or rstages[0].node != .read or rstages[0].hints.len > 0) return null;
    for (rstages[1..]) |st| if (!analyze.isMap(st.node, false)) return null;
    const w = ast.Write{ .connector = "", .form = null, .target = "", .mode = .append };
    return parquetSplit(env, rstages[0].node.read, rstages[0].hints, rstages[1..], w, .{ .threads = env.pipe.threads });
}

/// One item of a parallel build read: the rows it kept, copied out of the read's
/// per-batch scratch into the item's own arena until the index has copied them.
const BuildPart = struct {
    arena: std.heap.ArenaAllocator,
    batches: std.ArrayListUnmanaged(Batch) = .empty,
};

const BuildCtx = struct {
    split: *Morsels,
    recs: []const analyze.Resolved,
    errctx: ?*op.ErrCtx,
    queue: WorkQueue,
    parts: []BuildPart,
    bytes: std.atomic.Value(usize) = .init(0),
    cap: usize,
    rows_read: *obs.RowCounter,
    prof: ?*Profile = null,
    read_at: u64 = 0,
    root: ?profile.Id = null,
    root_mtx: std.Thread.Mutex = .{},
};

/// A lane: items taken from the queue in turn, each row group read through the lane's
/// one open reader (reopening a file per row group re-parses its footer), its kept
/// rows filed under the item's index so the index sees them in file order.
fn buildLane(ctx: *BuildCtx, lane_idx: usize) void {
    const span = trace.begin("join build lane", lane_idx);
    defer span.end();
    buildLaneRun(ctx) catch |e| ctx.queue.fail(e);
}

fn buildLaneRun(ctx: *BuildCtx) !void {
    const m = &ctx.split.parquet;
    var mem: LaneMem = .{};
    mem.init();
    defer mem.deinit();
    var keep = std.heap.ArenaAllocator.init(mem.gpa.allocator());
    defer keep.deinit();
    var held: ?HeldFile = null;
    defer if (held) |h| h.r.close();
    var item = ReaderSource{ .r = undefined, .schema_ = m.src_schema, .own = false };
    defer item.slicer.deinit();
    while (ctx.queue.take()) |i| {
        _ = mem.work.reset(.retain_capacity);
        const r = (try openItem(m, keep.allocator(), &held, i)) orelse continue;
        item.r = r;
        var cs = obs.CountingSource{ .inner = .{ .ptr = &item, .vtable = &ReaderSource.vtable }, .count = ctx.rows_read };
        var scan = op.Scan{ .src = cs.source() };
        scan.stats.at = ctx.read_at;
        const child = try chainOps(mem.work.allocator(), ctx.errctx, ctx.recs, .{ .scan = &scan });
        const part = &ctx.parts[i];
        const a = part.arena.allocator();
        while (true) {
            _ = mem.batch.reset(.retain_capacity);
            const b = (try child.next(mem.batch.allocator())) orelse break;
            if (b.len == 0) continue;
            const kept = try b.deepCopy(a);
            try part.batches.append(a, kept);
            if (ctx.bytes.fetchAdd(kept.heapBytes(), .monotonic) + kept.heapBytes() > ctx.cap) return error.JoinBuildTooLarge;
        }
        if (ctx.prof) |p| {
            const id = try p.absorb(child);
            ctx.root_mtx.lock();
            defer ctx.root_mtx.unlock();
            ctx.root = id;
        }
    }
}

/// The build side read by the lanes, its items' rows concatenated in file order (the
/// order a serial read gives) and indexed.
fn indexInParallel(env: *Env, split: *Morsels, read_at: u64, recs: []const analyze.Resolved, right_schema: *const types.Schema, right_keys: []const usize, left_kinds: []const types.TypeKind, cap: usize, root: *?profile.Id) !*op.JoinIndex {
    const arena = env.arena;
    const nitems = split.count();
    const parts = try arena.alloc(BuildPart, nitems);
    for (parts) |*p| p.* = .{ .arena = .init(env.gpa) };
    defer for (parts) |*p| p.arena.deinit();
    var ctx = BuildCtx{ .split = split, .recs = recs, .errctx = env.errctx, .queue = .{ .nitems = nitems }, .parts = parts, .cap = cap, .rows_read = env.rows_read, .prof = env.profile, .read_at = read_at };
    _ = try parallel.spawnJoin(arena, env.pipe.threads, buildLane, &ctx);
    if (ctx.queue.failure()) |e| return e;
    root.* = ctx.root;
    var chunks = std.array_list.Managed(Batch).init(arena);
    for (parts) |*p| try chunks.appendSlice(p.batches.items);
    return op.JoinIndex.fromBatchesMeeting(arena, chunks.items, right_schema, right_keys, left_kinds, ctx.bytes.load(.monotonic), cap, env.pipe.threads);
}

/// Hoist the join out of the fan-out: materialize the build side into a shared index
/// (the pulls are scratch) and resolve the suffix once. Returns the lane recipe and the
/// sink's output schema, or null when the build side is past the spill threshold of
/// a join that could spill serially, or when its keys all turned out to be filters
/// (a nested-loop join): its reads are closed and the caller falls back to the
/// serial plan, which reads that side again.
pub fn resolveLaneJoin(env: *Env, j: ast.Join, join_hints: []const ast.Hint, suffix: []const ast.Stage, left_schema: types.Schema) anyerror!?LaneJoinPlan {
    const arena = env.arena;
    const binding = (try stampedBinding(env, j.binding)) orelse
        return planErr(env.diag, try msgfmt.allocPrint(arena, "unknown binding `{s}` in join", .{j.binding}));
    const src_base = env.sources.items.len;
    const rstages = try prepareJoinSide(env, try j.rightStages(arena, binding.stages));
    var split = try buildSplit(env, rstages);
    const build: struct { op: op.Op = undefined, schema: types.Schema, pre: []const analyze.Resolved = &.{} } = if (split) |*sp| blk: {
        const pre = try chainAt(env, sp.schema().*, rstages[1..]);
        break :blk .{ .schema = pre.out.*, .pre = pre.recs };
    } else blk: {
        const pr = try buildPipeline(env, rstages);
        break :blk .{ .op = pr.op, .schema = pr.schema };
    };

    var ad = analyze.Diag{};
    const jr = analyze.resolveJoin(arena, left_schema, build.schema, j, env.params_expr, &ad) catch |e| return aErr(env, &ad, e);
    if (jr.keys.join.keyless()) {
        for (env.sources.items[src_base..]) |sc| sc.close();
        env.sources.shrinkRetainingCapacity(src_base);
        return null;
    }
    const out = try schemaPtr(arena, jr.plan.schema);
    const left_kinds = try arena.alloc(types.TypeKind, jr.plan.lks.len);
    for (left_kinds, jr.plan.lks) |*x, k| x.* = jr.left.out.fields[k].ty.kind;
    const after = try chainAt(env, out.*, suffix);

    const cap = try laneJoinCap(env, j, join_hints);
    var build_root: ?profile.Id = null;
    var build_chain: ?op.Op = null;
    const built = if (split) |*sp|
        indexInParallel(env, sp, profile.keyOf(rstages[0].pos), try std.mem.concat(arena, analyze.Resolved, &.{ build.pre, jr.right.recs }), jr.right.out, jr.plan.rks, left_kinds, cap, &build_root)
    else blk: {
        build_chain = try chainOps(arena, env.errctx, jr.right.recs, build.op);
        break :blk op.JoinIndex.createMeeting(arena, build_chain.?, jr.right.out, jr.plan.rks, left_kinds, cap, env.pipe.threads);
    };
    const index = built catch |e| {
        if (e == error.JoinBuildTooLarge and j.null_aware) op.notInTooLarge(env.errctx, cap);
        if (e != error.JoinBuildTooLarge or !joinMaySpill(env, j)) {
            if (e == error.JoinBuildTooLarge) env.errctx.set("{s}", .{op.failLabel(e)});
            return e;
        }
        for (env.sources.items[src_base..]) |sc| sc.close();
        env.sources.shrinkRetainingCapacity(src_base);
        env.log.log(.info, "join build side `{s}` is past {d} bytes; the join runs serially, where it can spill to disk", .{ j.binding, try joinSpillAt(env, join_hints) });
        return null;
    };
    if (env.profile) |p| if (build_chain) |bc| {
        build_root = try p.absorb(bc);
    };
    const shared_matched: ?[]std.atomic.Value(u64) = if (j.kind == .right or j.kind == .full) blk: {
        const words = try arena.alloc(std.atomic.Value(u64), (index.rows() + 63) / 64);
        @memset(words, std.atomic.Value(u64).init(0));
        break :blk words;
    } else null;

    return .{
        .lane = .{
            .index = index,
            .left_keys = jr.plan.lks,
            .right_keys = jr.plan.rks,
            .probe_prep = jr.left.recs,
            .left_key_names = jr.keys.join.left_keys,
            .residual = if (jr.residual) |r| r.pred else null,
            .pair_schema = if (jr.residual) |r| try schemaPtr(arena, r.schema) else null,
            .left_schema = jr.left.out,
            .right_schema = jr.right.out,
            .out_schema = out,
            .kind = j.kind,
            .null_aware = j.null_aware,
            .suffix = after.recs,
            .shared_matched = shared_matched,
            .build_root = build_root,
        },
        .out_schema = after.out.*,
    };
}

pub fn buildLaneJoinChain(ta: std.mem.Allocator, errctx: ?*op.ErrCtx, lj: LaneJoin, probe: op.Op) !op.Op {
    const j = try ta.create(op.Join);
    j.* = .{
        .probe = try chainOps(ta, errctx, lj.probe_prep, probe),
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
    j.stats.at = lj.at;
    return chainOps(ta, errctx, lj.suffix, .{ .join = j });
}

/// A right/full lane join's unmatched build rows, left side null, in build order
/// and through the join's suffix: a join over an empty probe whose match flags are
/// read from `shared_matched`, the one bitset (made by `resolveLaneJoin`) every
/// lane's probe marks. Null for every other kind. Call it once, after every lane
/// has finished probing.
pub fn buildLaneJoinDrain(ta: std.mem.Allocator, errctx: ?*op.ErrCtx, lj: LaneJoin) !?op.Op {
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
    j.stats.at = lj.at;
    return try chainOps(ta, errctx, lj.suffix, .{ .join = j });
}

const op_testing = @import("../../exec/op/testing_util.zig");

const TestLane = struct {
    lj: LaneJoin,
    probe: []const op.Batch,
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
        const chain = try buildLaneJoinChain(a, null, self.lj, .{ .scan = &scan });
        while (try chain.next(a)) |b| self.rows += b.len;
    }
};

test "lane joins: right and full lanes mark one shared bitset, and one drain emits each unmatched build row once, in build order" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

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
        const index = try op.JoinIndex.fromBatches(a, &.{build}, &op_testing.join_right_schema, &.{0}, 0, std.math.maxInt(usize), 1);
        const words = try a.alloc(std.atomic.Value(u64), (index.rows() + 63) / 64);
        @memset(words, std.atomic.Value(u64).init(0));
        const lj = LaneJoin{
            .index = index,
            .left_keys = &.{0},
            .right_keys = &.{0},
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
            l.* = .{ .lj = lj, .probe = p };
            t.* = try std.Thread.spawn(.{}, TestLane.run, .{l});
        }
        var lane_rows: usize = 0;
        for (&lanes, threads) |*l, t| {
            t.join();
            if (l.err) |e| return e;
            lane_rows += l.rows;
        }
        try std.testing.expectEqual(case.lane_rows, lane_rows);

        const drain = (try buildLaneJoinDrain(a, null, lj)).?;
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
    const build = try op_testing.kvBatch(a, &op_testing.join_right_schema, &.{1}, &.{"x"});
    const index = try op.JoinIndex.fromBatches(a, &.{build}, &op_testing.join_right_schema, &.{0}, 0, std.math.maxInt(usize), 1);
    const lj = LaneJoin{
        .index = index,
        .left_keys = &.{0},
        .right_keys = &.{0},
        .left_schema = &op_testing.join_left_schema,
        .right_schema = &op_testing.join_right_schema,
        .out_schema = &op_testing.join_both_schema,
        .kind = .left,
        .null_aware = false,
        .suffix = &.{},
    };
    try std.testing.expect((try buildLaneJoinDrain(a, null, lj)) == null);
}
