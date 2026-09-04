//! Parallel lane drivers: the shape classifiers that decide whether a pipeline
//! can fan out, and the per-shape drivers (map, aggregate, top-N, distinct,
//! SQL split, parquet/CSV morsels) plus their merge machinery.

const std = @import("std");
const ast = @import("../lang/ast.zig");
const types = @import("../lang/types.zig");
const op = @import("../exec/op.zig");
const Batch = @import("../exec/batch.zig").Batch;
const column = @import("../exec/column.zig");
const eval = @import("../exec/eval.zig");
const csv = @import("../connect/csv.zig");
const pqdecode = @import("../connect/pqdecode.zig");
const driver = @import("../connect/driver.zig");
const sql = @import("../connect/sql.zig");
const wrapProjected = @import("../connect/split.zig").wrapProjected;
const parallel = @import("parallel.zig");
const analyze = @import("analyze.zig");
const pushdown = @import("pushdown.zig");
const obs = @import("obs.zig");
const Value = @import("../exec/value.zig").Value;

const Env = @import("env.zig").Env;
const planErr = @import("env.zig").planErr;
const RunOptions = @import("env.zig").RunOptions;
const schemaPtr = @import("env.zig").schemaPtr;
const Stats = @import("env.zig").Stats;

const buildParallelSink = @import("connect.zig").buildParallelSink;
const dupeSchema = @import("connect.zig").dupeSchema;
const OneBatch = @import("connect.zig").OneBatch;
const openSink = @import("connect.zig").openSink;
const openSqlQuery = @import("connect.zig").openSqlQuery;
const planSplit = @import("connect.zig").planSplit;
const resolveUpsertKeys = @import("connect.zig").resolveUpsertKeys;
const sinkLabel = @import("connect.zig").sinkLabel;
const SplitCtx = @import("connect.zig").SplitCtx;
const sqlDescForStage = @import("connect.zig").sqlDescForStage;

const aErr = @import("plan.zig").aErr;
const buildChainFrom = @import("plan.zig").buildChainFrom;
const buildMapChain = @import("plan.zig").buildMapChain;
const buildPipeline = @import("plan.zig").buildPipeline;
const buildStage = @import("plan.zig").buildStage;
const filterBounds = @import("plan.zig").filterBounds;
const joinBuildCap = @import("plan.zig").joinBuildCap;
const mapChainSchema = @import("plan.zig").mapChainSchema;
const projectedColumns = @import("plan.zig").projectedColumns;
const tailSchema = @import("plan.zig").tailSchema;

/// The pipeline shapes a lane path can run, classified once for every source.
///
/// Each source's runner switches exhaustively over this, which is the point: a new
/// shape does not compile until both sources decide what to do with it. A source that
/// genuinely cannot fan a shape out returns false and falls back to the serial driver —
/// explicitly, rather than by omission.
const LaneShape = union(enum) {
    agg: AggShape,
    agg_join: AggJoinShape,
    distinct: DistinctShape,
    top_n: TopNShape,
    map: []const ast.Stage,
    map_join: MapJoinShape,
};

/// Order matters: `classifyAggPipeline` rejects a pipeline containing a join, so the
/// join-carrying variant is tried after it, and the map shapes last — a map+join is
/// only a map+join once nothing above it matched.
pub fn classifyLaneShape(stages: []const ast.Stage) ?LaneShape {
    if (classifyAggPipeline(stages)) |x| return .{ .agg = x };
    if (classifyAggJoinPipeline(stages)) |x| return .{ .agg_join = x };
    if (classifyDistinctPipeline(stages)) |x| return .{ .distinct = x };
    if (classifyTopNPipeline(stages)) |x| return .{ .top_n = x };
    if (classifyMapPipeline(stages)) |x| return .{ .map = x };
    if (classifyMapJoinPipeline(stages)) |x| return .{ .map_join = x };
    return null;
}

/// Preconditions every lane path shares. A hinted read is left to the paths that
/// honour the hint, and one stage plus a write is nothing to split.
pub fn laneEligible(stages: []const ast.Stage, opts: RunOptions) bool {
    return opts.threads > 1 and stages.len >= 2 and
        stages[0].node == .read and stages[0].hints.len == 0;
}

pub fn runParquetLane(env: *Env, stages: []const ast.Stage, shape: LaneShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    const rd = stages[0].node.read;
    const pipe = stages[0 .. stages.len - 1];
    return switch (shape) {
        .agg => |x| runParallelParquetAgg(env, rd, pipe, x.prefix, x.ag, x.tail, w, opts, stats, lanes_used),
        .agg_join => |x| runParallelParquetAggJoin(env, rd, pipe, x, w, opts, stats, lanes_used),
        .distinct => |x| runParallelParquetDistinct(env, rd, pipe, x.prefix, x.dist, x.tail, w, opts, stats, lanes_used),
        .top_n => |x| runParallelParquetTopN(env, rd, pipe, x.prefix, x.srt, x.lim, w, opts, stats, lanes_used),
        .map => |x| runParallelParquetMap(env, rd, pipe, x, w, opts, stats, lanes_used),
        .map_join => |x| runParallelParquetMapJoin(env, rd, pipe, x, w, opts, stats, lanes_used),
    };
}

pub fn runCsvLane(env: *Env, stages: []const ast.Stage, shape: LaneShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    const rd = stages[0].node.read;
    return switch (shape) {
        .agg => |x| runParallelCsvAgg(env, rd, x.prefix, x.ag, x.tail, w, opts, stats, lanes_used),
        .agg_join => |x| runParallelCsvAggJoin(env, rd, x, w, opts, stats, lanes_used),
        .distinct => |x| runParallelCsvDistinct(env, rd, x.prefix, x.dist, x.tail, w, opts, stats, lanes_used),
        .top_n => |x| runParallelCsvTopN(env, rd, x.prefix, x.srt, x.lim, w, opts, stats, lanes_used),
        .map => |x| runParallelCsvMap(env, rd, x, w, opts, stats, lanes_used),
        .map_join => |x| runParallelCsvMapJoin(env, rd, x, w, opts, stats, lanes_used),
    };
}

const AggShape = struct { prefix: []const ast.Stage, ag: ast.Aggregate, tail: []const ast.Stage };

/// Recognize `read … | (filter|select)* | aggregate | (sort|limit)* | write` — the
/// shape the parallel CSV-aggregate path handles: a map-only prefix (folded in
/// parallel), exactly one aggregate (the breaker), and a small post-aggregate tail
/// (run serially on the merged result). Anything else → null (serial path).
pub fn classifyAggPipeline(stages: []const ast.Stage) ?AggShape {
    const middle = stages[1 .. stages.len - 1];
    var ai: ?usize = null;
    for (middle, 0..) |st, i| switch (st.node) {
        // A `select` after the aggregate is the reordering projection the parser emits
        // for an interleaved SELECT list. Rejecting it sent every such query to the
        // serial driver — 379ms against 88ms on TPC-H q01's shape. `writeTail` applies
        // it, and the sink is opened with the schema it produces.
        .filter => if (ai != null) return null,
        .select => {},
        .aggregate => {
            if (ai != null) return null;
            ai = i;
        },
        .sort, .limit => if (ai == null) return null,
        else => return null,
    };
    const a = ai orelse return null;
    return .{ .prefix = middle[0..a], .ag = middle[a].node.aggregate, .tail = middle[a + 1 ..] };
}

/// Recognize `read … | filter* | aggregate | <anything>* | write` — the shape a whole
/// aggregate can descend into one grouped source query. Two differences from
/// `classifyAggPipeline`: the prefix is filters ONLY (a `select` renames columns, so the
/// group keys would no longer be source columns to name in a GROUP BY), and the tail is
/// unrestricted — `having`, sort, limit and anything else run engine-side over the
/// grouped result exactly as they did before. Pure: eligibility of the *shape* only;
/// `pushdown.planWholeAgg` decides whether the aggregate itself is renderable.
pub fn classifyWholeAgg(stages: []const ast.Stage) ?AggShape {
    if (stages.len < 3 or stages[0].node != .read) return null;
    // A hint on the read is about scanning it (`@[where]`, `@[split…]`, `@[buffer]`),
    // and none of those survive the rewrite into a grouped query. Leave hinted reads
    // to the paths that honour them.
    if (stages[0].hints.len != 0) return null;
    const middle = stages[1 .. stages.len - 1];
    for (middle, 0..) |st, i| switch (st.node) {
        .filter => {},
        .aggregate => |ag| return .{ .prefix = middle[0..i], .ag = ag, .tail = middle[i + 1 ..] },
        else => return null,
    };
    return null;
}

/// Try to descend the whole aggregate into the source. On success returns a rewritten
/// stage list — a QUERY-form read of the grouped SQL, then the untouched post-aggregate
/// tail, then the write — which the caller rebuilds through the ordinary serial path, so
/// the result's schema, row counting and sink all come from the existing machinery.
/// Null means "not eligible": the caller keeps the pipeline it already built.
pub fn wholeAggStages(env: *Env, stages: []const ast.Stage, shape: AggShape, src_base: usize) !?[]const ast.Stage {
    const arena = env.arena;
    const desc = env.sql_desc orelse return null;
    if (stages[0].node != .read) return null;
    const rd = stages[0].node.read;
    if (rd.form != .table and rd.form != .query) return null;
    for (shape.prefix) |st| if (st.node != .filter) return null;

    const src_schema = try dupeSchema(arena, env.sources.items[src_base].schema());

    // The engine's own output schema for this aggregate — the types every rendered
    // aggregate is CAST to, and the names the tail and sink already expect.
    var ad = analyze.Diag{};
    const apl = analyze.aggregatePlan(arena, src_schema, shape.ag, env.params_expr, &ad) catch return null;

    const wa = (try pushdown.planWholeAgg(arena, desc.dialect, desc.base_sql, src_schema, shape.prefix, shape.ag, apl.schema)) orelse return null;

    const out = try arena.alloc(ast.Stage, shape.tail.len + 2);
    // No hints: an `@[where = …]` would be re-applied over the grouped result (whose
    // columns are the aggregate's, not the source's), and `@[split = …]` has nothing
    // left to split — the read is already one small result set.
    out[0] = .{
        .node = .{ .read = .{ .connector = rd.connector, .form = .{ .query = wa.sql } } },
        .hints = &.{},
        .pos = stages[0].pos,
    };
    for (shape.tail, out[1 .. 1 + shape.tail.len]) |st, *o| o.* = st;
    out[out.len - 1] = stages[stages.len - 1];

    env.log.log(.debug, "aggregate pushdown: grouped query sent to {s}", .{@tagName(desc.kind)});
    return out;
}

/// Shared header for the parallel workers: a work-stealing item counter plus the
/// first-error latch. `failed` flags the other workers to stop (checked lock-free);
/// `first_err` is what the caller re-raises after the join.
const WorkQueue = struct {
    nitems: usize,
    next: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    err_mtx: std.Thread.Mutex = .{},
    first_err: ?anyerror = null,
    failed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    fn fail(q: *WorkQueue, e: anyerror) void {
        q.err_mtx.lock();
        if (q.first_err == null) q.first_err = e;
        q.err_mtx.unlock();
        q.failed.store(true, .seq_cst);
    }
};

/// The worker-dispatch loop shared by the parallel CSV/SQL paths: steal the next
/// item off `ctx.queue`, run `workOne(ctx, i)`, and latch the first error (which
/// also stops the other workers). `Ctx` only needs a `queue: WorkQueue` field.
fn dispatchWorker(comptime Ctx: type, comptime workOne: anytype) fn (*Ctx, usize) void {
    return struct {
        fn go(ctx: *Ctx, _: usize) void {
            while (true) {
                if (ctx.queue.failed.load(.seq_cst)) return;
                const i = ctx.queue.next.fetchAdd(1, .seq_cst);
                if (i >= ctx.queue.nitems) break;
                workOne(ctx, i) catch |e| {
                    ctx.queue.fail(e);
                    return;
                };
            }
        }
    }.go;
}

/// One work item's partial group set, kept alive past the worker that folded it
/// so the combine can run in item order.
///
/// Folding into a per-*item* slot rather than straight into shared state under a
/// lock is what makes a parallel aggregate reproducible. Which rows land in which
/// partial is fixed by the item boundaries (a CSV byte range, a key-range
/// predicate), and `combineAggSlots` walks the slots by index, so a float `SUM`
/// adds its partials in the same order on every run. Merging as lanes *finished*
/// made the order depend on thread scheduling, so the same script wrote a
/// slightly different total each run — see the note in `combineAggSlots`.
const AggSlot = struct {
    /// Per-slot and single-threaded: exactly one worker ever folds a given slot,
    /// and the main thread only touches it after `spawnJoin` has returned.
    gpa: std.heap.GeneralPurposeAllocator(.{ .thread_safe = false }) = .{},
    arena: std.heap.ArenaAllocator = undefined,
    groups: []op.Aggregate.Group = &.{},
    /// One hash per group, kept from the fold so the combine can partition by it
    /// without hashing every key again.
    hashes: []u64 = &.{},
    /// This slot's group indices split by key hash — the same shape `PqLane`
    /// carries, so the radix combine can walk slots and lanes alike. Empty for a
    /// slot no worker reached.
    buckets: []std.array_list.Managed(u32) = &.{},
    /// False for a slot no worker reached, which is what an aborted run leaves
    /// behind. Such a slot holds no groups and is skipped by the combine.
    done: bool = false,

    fn arm(self: *AggSlot) void {
        self.arena = std.heap.ArenaAllocator.init(self.gpa.allocator());
    }
};

fn allocAggSlots(gpa: std.mem.Allocator, n: usize) ![]AggSlot {
    const slots = try gpa.alloc(AggSlot, n);
    for (slots) |*s| s.* = .{};
    // Armed in a second pass: `arm` takes the address of the slot's own gpa, so
    // it has to run once the slot is at its final location.
    for (slots) |*s| s.arm();
    return slots;
}

fn freeAggSlots(gpa: std.mem.Allocator, slots: []AggSlot) void {
    for (slots) |*s| {
        s.arena.deinit();
        _ = s.gpa.deinit();
    }
    gpa.free(slots);
}

/// Above this many partial groups the combine is split across threads by key
/// hash; below it the single pass takes microseconds and the partition arenas
/// and thread hop cost more than they save.
pub const agg_combine_parallel_min: usize = 1 << 14;

/// Combine per-item partials into one group set, walking the slots by index.
///
/// The order matters for floats and only for floats: `mergeAcc` adds partial
/// sums, and float addition is not associative, so combining the same partials
/// in a different order gives a total that differs in the last few bits. Integer
/// and DECIMAL sums, counts and min/max are exact and order-insensitive. Note
/// that this pins the result for a *given* item count — `-j` changes how the
/// input is cut up, so a float total can still differ between `-j 4` and `-j 8`
/// (as it does between either and the serial path). `CAST`ing to DECIMAL is the
/// way to get a total that is identical everywhere.
///
/// Two shapes, chosen by how many partial groups there are. One pass is right for
/// the ordinary case, where a handful of groups come back per slot. But the pass
/// is O(partials), and with a key of high cardinality that is O(rows): a 2M-row
/// CSV grouped by a unique id spent longer combining 2M partials than the whole
/// serial aggregate took, so `-j 16` came in at 1.32s against 0.77s at `-j 1` —
/// parallelism made it slower. Past the threshold the combine is radix-partitioned
/// like the parquet path's, so it scales with the fold instead of undoing it.
///
/// `parts` is the caller's, because the merged groups live in its arenas and have
/// to outlive the write. Determinism survives partitioning: keys are disjoint
/// across partitions, so no group is ever summed across two of them, and each
/// partition still walks the slots in index order.
fn combineAggSlots(
    env: *Env,
    slots: []AggSlot,
    aggs: []const op.Aggregate.Agg,
    threads: usize,
    parts: []PqPart,
) ![]const op.Aggregate.Group {
    var total: usize = 0;
    for (slots) |*s| total += s.groups.len;

    if (threads < 2 or total < agg_combine_parallel_min) {
        var map = op.Aggregate.GroupMap().init(env.arena);
        var groups = std.array_list.Managed(op.Aggregate.Group).init(env.arena);
        for (slots) |*s| {
            if (!s.done) continue;
            try op.Aggregate.mergeGroups(&map, &groups, env.arena, s.groups, aggs);
        }
        return groups.items;
    }

    var mctx = SlotMergeCtx{ .slots = slots, .parts = parts, .aggs = aggs, .queue = .{ .nitems = pq_parts } };
    _ = try parallel.spawnJoin(env.arena, @min(threads, pq_parts), slotMergeWorker, &mctx);
    if (mctx.queue.first_err) |e| return e;

    env.log.log(.debug, "parallel agg combine: {d} partial groups over {d} partitions", .{ total, pq_parts });

    var merged: usize = 0;
    for (parts) |*pp| merged += pp.groups.items.len;
    const all = try env.arena.alloc(op.Aggregate.Group, merged);
    var at: usize = 0;
    for (parts) |*pp| {
        for (pp.groups.items) |g| {
            all[at] = g;
            at += 1;
        }
    }
    return all;
}

const SlotMergeCtx = struct {
    slots: []AggSlot,
    parts: []PqPart,
    aggs: []const op.Aggregate.Agg,
    queue: WorkQueue,
};

const slotMergeWorker = dispatchWorker(SlotMergeCtx, slotMergeOne);

fn slotMergeOne(ctx: *SlotMergeCtx, p: usize) !void {
    return mergeRadixPart(&ctx.parts[p], ctx.slots, ctx.aggs, p);
}

/// The radix partitions and their arenas, owned by the caller so that whatever the
/// combine merges into them outlives the sink write. Initialising one is free —
/// an arena allocates nothing until used — so the ordinary single-pass combine
/// pays nothing for these being here.
fn allocMergeParts(gpa: std.mem.Allocator) ![]PqPart {
    const parts = try gpa.alloc(PqPart, pq_parts);
    for (parts) |*pp| {
        pp.* = .{ .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator), .map = undefined, .groups = undefined };
        pp.map = op.Aggregate.GroupMap().init(pp.arena.allocator());
        pp.groups = std.array_list.Managed(op.Aggregate.Group).init(pp.arena.allocator());
    }
    return parts;
}

fn freeMergeParts(gpa: std.mem.Allocator, parts: []PqPart) void {
    for (parts) |*pp| pp.arena.deinit();
    gpa.free(parts);
}

/// Finalize merged parallel-aggregate groups into one output batch (a "merger"
/// Aggregate just for `emit`; it never pulls a child).
fn emitMergedGroups(env: *Env, agg_in: *const types.Schema, by: []const usize, aggs: []const op.Aggregate.Agg, out_schema: *const types.Schema, groups: []const op.Aggregate.Group) !Batch {
    var merger = op.Aggregate{
        .child = undefined,
        .in_schema = agg_in,
        .by = by,
        .aggs = aggs,
        .out_schema = out_schema,
        .state = env.arena,
        .gpa = env.gpa,
    };
    return merger.emit(env.arena, groups);
}

fn writeTail(env: *Env, snk: driver.Sink, batch: Batch, schema: types.Schema, tail: []const ast.Stage, stats: *Stats) !void {
    const arena = env.arena;
    if (tail.len == 0) {
        if (batch.len > 0) {
            try snk.writeBatch(arena, batch);
            stats.rows_out += batch.len;
        }
        return;
    }
    var ob = OneBatch{ .b = batch, .sch = schema };
    var scan = op.Scan{ .src = ob.source() };
    var cur: op.Op = .{ .scan = &scan };
    var sch = schema;
    for (tail) |st| {
        const r = try buildStage(env, st, cur, sch);
        cur = r.op;
        sch = r.schema;
    }
    while (try cur.next(arena)) |outb| {
        try snk.writeBatch(arena, outb);
        stats.rows_out += outb.len;
    }
}

/// Shared state for the parallel-aggregate workers. Each worker rebuilds the prefix
/// chain on its own arena, folds one or more newline-aligned file chunks into a
/// thread-local partial group set, then merges it into the combined set under `mtx`
/// (deep-copying keys/min-max into the plan arena, so its own arena can be freed).
/// The merge is small (O(groups)) vs the fold (O(rows)), so lock contention is low.
const AggCtx = struct {
    mapped: *csv.MappedCsv,
    csv_schema: *const types.Schema,
    agg_in_schema: *const types.Schema,
    out_schema: *const types.Schema,
    prefix: []const ast.Stage,
    params: *std.StringHashMap(*const ast.Expr),
    by: []const usize,
    aggs: []const op.Aggregate.Agg,
    queue: WorkQueue,
    slots: []AggSlot,
    rows_read: *std.atomic.Value(u64),
    /// The join-then-aggregate shape, exactly as `PqAggCtx.joins`.
    joins: []const LaneJoin = &.{},
};

const aggWorker = dispatchWorker(AggCtx, aggWorkOne);

fn aggWorkOne(ctx: *AggCtx, i: usize) !void {
    const slot = &ctx.slots[i];
    const wa = slot.arena.allocator();

    var reader = csv.CsvSliceReader{ .data = ctx.mapped.chunk(i, ctx.queue.nitems), .schema = ctx.csv_schema };
    var cs = obs.CountingSource{ .inner = reader.source(), .count = ctx.rows_read };
    var scan = op.Scan{ .src = cs.source() };
    var child = try buildMapChain(wa, ctx.params, ctx.prefix, &scan, ctx.csv_schema);
    for (ctx.joins) |lj| child = try buildLaneJoinChain(wa, ctx.params, lj, child);
    var agg = op.Aggregate{
        .child = child,
        .in_schema = ctx.agg_in_schema,
        .by = ctx.by,
        .aggs = ctx.aggs,
        .out_schema = ctx.out_schema,
        .err = null,
        .state = wa,
        .gpa = slot.gpa.allocator(),
    };
    // Stays in the slot's arena: the combine reads it after every lane has joined.
    const drained = try agg.drainGroupsHashed();
    slot.groups = drained.groups;
    slot.hashes = drained.hashes;
    slot.buckets = try bucketByHash(wa, drained.hashes);
    slot.done = true;
}

/// One parallel-aggregate lane: its own arena and its own group table, so the
/// fold phase never takes a lock.
const PqLane = struct {
    arena: std.heap.ArenaAllocator,
    groups: []op.Aggregate.Group = &.{},
    hashes: []u64 = &.{},
    /// The lane's groups split by key hash, computed once when the lane runs
    /// dry. The merge then reads a partition's slice directly instead of
    /// rescanning every lane for every partition.
    buckets: []std.array_list.Managed(u32) = &.{},
};

/// One radix partition of the merge: the lanes' groups are split by key hash, so
/// each partition is owned outright by one task and needs no lock either.
const PqPart = struct {
    arena: std.heap.ArenaAllocator,
    map: op.Aggregate.GroupMap(),
    groups: std.array_list.Managed(op.Aggregate.Group),
};

/// Number of radix partitions. Comfortably above the lane count so the merge
/// stays balanced when key hashes are uneven.
const pq_parts: usize = 64;

/// Fewest lanes worth splitting an aggregate across. Two suffices now that the
/// merge reuses the hashes the fold produced; while it re-hashed every key, the
/// extra pass cost more than two lanes could win back.
const pq_min_lanes: usize = 2;

/// Shared state for parallel *parquet* aggregate lanes. The morsel is a row
/// group: each lane opens its own reader over a disjoint window and folds into
/// its own table. Nothing is shared until the radix merge.
const PqAggCtx = struct {
    morsels: PqMorsels,
    agg_in_schema: *const types.Schema,
    out_schema: *const types.Schema,
    prefix: []const ast.Stage,
    params: *std.StringHashMap(*const ast.Expr),
    by: []const usize,
    aggs: []const op.Aggregate.Agg,
    lanes: []PqLane,
    rows_read: *std.atomic.Value(u64),
    /// The join-then-aggregate shape: one shared build index per join, in application
    /// order, plus the post-join stages each lane rebuilds over them. Empty is the
    /// plain aggregate.
    joins: []const LaneJoin = &.{},
};

/// Pulls row-group morsels off the shared queue and presents them as one
/// continuous stream. This is what lets a lane run a *single* aggregate over
/// everything it steals: folding per morsel and merging afterwards would walk
/// the lane's groups an extra time, which at high cardinality costs more than
/// the parallelism buys.
/// The shared row-group work list a parallel parquet path hands to its lanes.
const PqMorsels = struct {
    path: []const u8,
    project: ?[][]const u8,
    bounds: []const pqdecode.Bound,
    src_schema: *const types.Schema,
    per_item: usize,
    queue: WorkQueue,
};

const MorselSource = struct {
    m: *PqMorsels,
    scratch: std.mem.Allocator,
    cur: ?*pqdecode.Reader = null,
    /// When set, this source owns a fixed arithmetic slice of the morsels
    /// (`next`, `next + step`, …) instead of stealing whichever is free.
    ///
    /// The aggregate path needs that: a lane folds every morsel it reads into one
    /// partial group set, so the order its float sums are added in is the order
    /// morsels reached the lane. Stealing makes that order depend on thread
    /// timing, and the run's totals wobble in their last bits. Fixed slices cost
    /// balance only when row groups are uneven, whereas the map path — which
    /// combines nothing across morsels — keeps stealing and stays balanced.
    fixed: ?struct { next: usize, step: usize } = null,

    /// The next morsel index this source should read, or null when it is done.
    fn nextIndex(self: *MorselSource) ?usize {
        if (self.fixed) |*f| {
            if (f.next >= self.m.queue.nitems) return null;
            defer f.next += f.step;
            return f.next;
        }
        const i = self.m.queue.next.fetchAdd(1, .seq_cst);
        return if (i >= self.m.queue.nitems) null else i;
    }

    fn schemaFn(ptr: *anyopaque) types.Schema {
        const self: *MorselSource = @ptrCast(@alignCast(ptr));
        return self.m.src_schema.*;
    }
    fn nextFn(ptr: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
        const self: *MorselSource = @ptrCast(@alignCast(ptr));
        while (true) {
            if (self.cur) |r| {
                if (try r.next(arena)) |b| return b;
                // Exhausted morsel: release its file handle. A reader was opened
                // per row group and never closed, so a lane leaked one fd (and
                // one parsed footer in its arena) per row group it took.
                r.close();
                self.cur = null;
            }
            if (self.m.queue.failed.load(.seq_cst)) return null;
            const i = self.nextIndex() orelse return null;
            const r = try pqdecode.Reader.openProjected(self.scratch, self.m.path, self.m.project);
            r.bounds = self.m.bounds;
            r.rg = i * self.m.per_item;
            if (r.rg >= r.md.row_groups.len) {
                r.close();
                continue;
            }
            r.rg_end = @min(r.rg + self.m.per_item, r.md.row_groups.len);
            self.cur = r;
        }
    }
    fn closeFn(ptr: *anyopaque) void {
        const self: *MorselSource = @ptrCast(@alignCast(ptr));
        if (self.cur) |r| r.close();
        self.cur = null;
    }
    const vtable = driver.Source.VTable{ .schema = schemaFn, .next = nextFn, .close = closeFn };
};

fn pqAggLane(ctx: *PqAggCtx, lane_idx: usize) void {
    pqAggLaneRun(ctx, lane_idx) catch |e| ctx.morsels.queue.fail(e);
}

fn pqAggLaneRun(ctx: *PqAggCtx, lane_idx: usize) !void {
    const ls = &ctx.lanes[lane_idx];
    const la = ls.arena.allocator();

    var ms = MorselSource{
        .m = &ctx.morsels,
        .scratch = la,
        .fixed = .{ .next = lane_idx, .step = ctx.lanes.len },
    };
    var cs = obs.CountingSource{ .inner = .{ .ptr = &ms, .vtable = &MorselSource.vtable }, .count = ctx.rows_read };
    var scan = op.Scan{ .src = cs.source() };
    var child = try buildMapChain(la, ctx.params, ctx.prefix, &scan, ctx.morsels.src_schema);
    for (ctx.joins) |lj| child = try buildLaneJoinChain(la, ctx.params, lj, child);
    var agg = op.Aggregate{
        .child = child,
        .in_schema = ctx.agg_in_schema,
        .by = ctx.by,
        .aggs = ctx.aggs,
        .out_schema = ctx.out_schema,
        .err = null,
        .state = la,
        .gpa = ls.arena.child_allocator,
    };
    const drained = try agg.drainGroupsHashed();
    ls.groups = drained.groups;
    ls.hashes = drained.hashes;
    try bucketLane(ls);
}

/// Split group indices into radix buckets, reusing the hash the fold already
/// produced. Shared by the parquet lanes and the CSV/SQL slots.
fn bucketByHash(a: std.mem.Allocator, hashes: []const u64) ![]std.array_list.Managed(u32) {
    const buckets = try a.alloc(std.array_list.Managed(u32), pq_parts);
    for (buckets) |*b| b.* = std.array_list.Managed(u32).init(a);
    for (hashes, 0..) |h, i| {
        const p: usize = @intCast((h >> 32) % pq_parts);
        try buckets[p].append(@intCast(i));
    }
    return buckets;
}

fn bucketLane(ls: *PqLane) !void {
    ls.buckets = try bucketByHash(ls.arena.allocator(), ls.hashes);
}

const PqMergeCtx = struct {
    lanes: []PqLane,
    parts: []PqPart,
    aggs: []const op.Aggregate.Agg,
    queue: WorkQueue,
};

const pqMergeWorker = dispatchWorker(PqMergeCtx, pqMergeOne);

fn pqMergeOne(ctx: *PqMergeCtx, p: usize) !void {
    return mergeRadixPart(&ctx.parts[p], ctx.lanes, ctx.aggs, p);
}

/// Merge radix partition `p` of a set of partials into `dst`. `srcs` is a slice of
/// anything carrying `groups`/`hashes`/`buckets` — parquet lanes or CSV/SQL slots —
/// and is walked in index order, which is what keeps a float SUM from depending on
/// thread timing (see `AggSlot`). A partition owns its keys outright, so the tasks
/// need no lock between them, which is what stops a high-cardinality merge from
/// serialising behind one table.
fn mergeRadixPart(dst: *PqPart, srcs: anytype, aggs: []const op.Aggregate.Agg, p: usize) !void {
    const da = dst.arena.allocator();
    var tbl = try op.Aggregate.MergeTable.init(da, 256);
    var hashes = std.array_list.Managed(u64).init(da);
    var any_distinct = false;
    for (aggs) |a| {
        if (a.distinct) any_distinct = true;
    }
    for (srcs) |*ls| {
        if (ls.buckets.len == 0) continue;
        for (ls.buckets[p].items) |gi| {
            const g = ls.groups[gi];
            const h = ls.hashes[gi];
            if (try tbl.find(h, g.key_vals, dst.groups.items, hashes.items, dst.groups.items.len)) |at| {
                const cg = &dst.groups.items[at];
                for (g.accs, aggs, 0..) |src, agg, j| try op.Aggregate.mergeAcc(da, &cg.accs[j], src, agg);
            } else {
                try dst.groups.append(try op.Aggregate.adoptOne(da, g, aggs, any_distinct));
                try hashes.append(h);
            }
        }
    }
}

/// The `sort … limit` shape a parallel aggregate can push into its partitions.
const TopNTail = struct { keys: []const ast.SortKey, n: usize };

/// Recognise a tail that is only sorts and limits, so each partition can drop to
/// its own best `n` rows before anything is materialised. An `OFFSET` disables
/// it: the rows a partition discards could be the ones the offset lands on.
fn topNTail(tail: []const ast.Stage) ?TopNTail {
    var keys: []const ast.SortKey = &.{};
    var n: ?usize = null;
    for (tail) |st| switch (st.node) {
        .sort => |so| keys = so.keys,
        .limit => |l| {
            if (l.offset != 0) return null;
            n = @intCast(l.count);
        },
        else => return null,
    };
    if (keys.len == 0) return null;
    return .{ .keys = keys, .n = n orelse return null };
}

/// The output value a sort key refers to: the group keys come first in the
/// aggregate's schema, the aggregates after them.
fn groupSortValue(g: op.Aggregate.Group, col: usize, by_len: usize, aggs: []const op.Aggregate.Agg) Value {
    if (col < by_len) return g.key_vals[col];
    return op.Aggregate.finalizeAcc(g.accs[col - by_len], aggs[col - by_len]);
}

const GroupOrder = struct {
    cols: []const usize,
    desc: []const bool,
    by_len: usize,
    aggs: []const op.Aggregate.Agg,

    fn less(self: GroupOrder, a: op.Aggregate.Group, b: op.Aggregate.Group) bool {
        for (self.cols, self.desc) |c, d| {
            const av = groupSortValue(a, c, self.by_len, self.aggs);
            const bv = groupSortValue(b, c, self.by_len, self.aggs);
            const o = eval.compareValues(av, bv) orelse .eq;
            if (o != .eq) return if (d) o == .gt else o == .lt;
        }
        return false;
    }
};

const PqTopNCtx = struct {
    parts: []PqPart,
    order: GroupOrder,
    n: usize,
    queue: WorkQueue,
};

const pqTopNWorker = dispatchWorker(PqTopNCtx, pqTopNOne);

fn pqTopNOne(ctx: *PqTopNCtx, p: usize) !void {
    const g = &ctx.parts[p].groups;
    if (g.items.len <= ctx.n) return;
    std.sort.pdq(op.Aggregate.Group, g.items, ctx.order, GroupOrder.less);
    g.shrinkRetainingCapacity(ctx.n);
}

/// Map-only pipeline (scan -> filter/project/explode -> write) over a local
/// parquet file. Each lane pulls row-group morsels and writes its own output,
/// so nothing is shared except the work list and, for a shared sink, the write
/// lock.
/// Where a lane's rows come from. This is the ONLY difference between the parquet and
/// CSV copies of every lane body: a parquet lane pulls row-group morsels off a queue
/// shared with its siblings, a CSV lane parses one newline-aligned byte range. Above
/// this seam the map chain, the join probe, the aggregate fold and the sink writes are
/// identical — and are written out once per shape per source today.
///
/// The two still *distribute* work differently (morsel stealing versus a static chunk
/// per lane), which is why this unifies the reader and not yet the whole body.
const LaneRows = union(enum) {
    /// Borrowed: every lane shares one queue, which is what makes stealing work.
    parquet: *PqMorsels,
    /// One NAMED row group, rather than whichever is free. A shape whose work
    /// item index has to mean a position in the file needs this — see
    /// `LaneSplit`.
    parquet_group: struct { m: *const PqMorsels, group: usize },
    csv: struct { mapped: *csv.MappedCsv, schema: *const types.Schema, chunk: usize, of: usize },
};

/// This lane's row stream, or null when the item holds nothing (a row group past
/// the end of a file that shrank between plan and read). The backing reader is
/// allocated from `scratch` because the returned `driver.Source` borrows it and
/// has to outlive this call — the reason both copies held it in a `var` local
/// beside the source. Closing the returned source releases the reader.
fn laneRowSource(rows: LaneRows, scratch: std.mem.Allocator) !?driver.Source {
    switch (rows) {
        .parquet => |m| {
            const ms = try scratch.create(MorselSource);
            ms.* = .{ .m = m, .scratch = scratch };
            return .{ .ptr = ms, .vtable = &MorselSource.vtable };
        },
        .parquet_group => |g| {
            const rdr = try pqdecode.Reader.openProjected(scratch, g.m.path, g.m.project);
            rdr.bounds = g.m.bounds;
            rdr.rg = g.group;
            if (rdr.rg >= rdr.md.row_groups.len) {
                rdr.close();
                return null;
            }
            rdr.rg_end = rdr.rg + g.m.per_item;
            const rs = try scratch.create(ReaderSource);
            rs.* = .{ .r = rdr, .schema_ = g.m.src_schema };
            return .{ .ptr = rs, .vtable = &ReaderSource.vtable };
        },
        .csv => |c| {
            const rd = try scratch.create(csv.CsvSliceReader);
            rd.* = .{ .data = c.mapped.chunk(c.chunk, c.of), .schema = c.schema };
            return rd.source();
        },
    }
}

/// Open `rd`'s parquet file as a splittable input, or null when this pipeline
/// cannot be split across lanes. Shared by every parquet lane path, which all
/// gate on the same three things: a real path, an upsert that names its keys,
/// and enough row groups and threads to be worth dividing.
fn parquetSplit(env: *Env, rd: ast.Read, push_stages: []const ast.Stage, w: ast.Write, opts: RunOptions) anyerror!?LaneSplit {
    const arena = env.arena;
    const path = switch (rd.form) {
        .path => |p| p,
        else => return null,
    };
    if (w.mode == .upsert and w.mode.upsert.keys.len == 0) return null;
    if (opts.threads < pq_min_lanes) return null;

    // `push_stages` is the caller's: the aggregate and distinct paths hand over
    // the whole pipeline, join included, because `projectedColumns` understands a
    // `.join` stage (it contributes the left keys) and restricting it made those
    // bail to "read every column". The map path hands over the PRE-join stages
    // only, since past its join the column names are the join's, not the source's.
    const project = try projectedColumns(env, push_stages);
    const bounds = try filterBounds(env, push_stages);
    const probe = pqdecode.Reader.openProjected(arena, path, project) catch return null;
    const ngroups = probe.md.row_groups.len;
    if (ngroups < 2) return null;

    return .{ .parquet = .{
        .path = path,
        .project = project,
        .bounds = bounds,
        .src_schema = try schemaPtr(arena, probe.schema),
        .per_item = 1,
        .queue = .{ .nitems = ngroups },
    } };
}

/// Map `rd`'s CSV for splitting, or null when it cannot be split. The caller
/// owns the result and must `close()` it.
fn csvSplitFile(env: *Env, rd: ast.Read, w: ast.Write) anyerror!?*csv.MappedCsv {
    const path = switch (rd.form) {
        .path => |p| p,
        else => return null,
    };
    if (w.mode == .upsert and w.mode.upsert.keys.len == 0) return null;

    const mapped = csv.MappedCsv.open(env.arena, path, env.csv_in) catch return null;
    // A newline inside a quoted field makes chunk boundaries undecidable from a
    // byte offset, so this file is parsed serially rather than split. Closed
    // explicitly: this returns before the caller's `defer` is armed, and leaking
    // the mmap + fd once per file exhausted the fd limit in a FOR EACH.
    if (mapped.quoted_newlines) {
        mapped.close();
        return null;
    }
    return mapped;
}

/// The input side of a parallel shape whose work items are POSITIONS: item `i`
/// covers the part of the file that precedes item `i+1`. Distinct needs that (it
/// resolves ties by first appearance, so the item index has to order rows the
/// way a serial run would); the map path does not, and keeps stealing morsels.
///
/// This is the axis the parallel paths were duplicated along. Each shape had a
/// `…Csv…` and a `…Parquet…` copy differing only in how many items there are and
/// how to open one, while the operator chain, the merge and the sink above were
/// identical — so naming that difference lets a shape have one implementation,
/// the way the join variants already share one `…Impl`.
const LaneSplit = union(enum) {
    csv: struct { mapped: *csv.MappedCsv, schema: *const types.Schema },
    parquet: PqMorsels,

    /// A CSV is split by byte offset, so its item count is a free choice and one
    /// chunk per thread is the balanced one; a parquet file is split at row
    /// groups, which the file itself fixes.
    fn count(self: LaneSplit, nthreads: usize) usize {
        return switch (self) {
            .csv => nthreads,
            .parquet => |m| m.queue.nitems,
        };
    }

    fn schema(self: *const LaneSplit) *const types.Schema {
        return switch (self.*) {
            .csv => |c| c.schema,
            .parquet => |m| m.src_schema,
        };
    }

    fn label(self: LaneSplit) []const u8 {
        return switch (self) {
            .csv => "csv",
            .parquet => "parquet",
        };
    }

    /// What `laneRowSource` needs to open work item `i` of `nitems`, for a shape
    /// whose items are positions.
    fn rows(self: *const LaneSplit, i: usize, nitems: usize) LaneRows {
        return switch (self.*) {
            .csv => |c| .{ .csv = .{ .mapped = c.mapped, .schema = c.schema, .chunk = i, .of = nitems } },
            .parquet => |*m| .{ .parquet_group = .{ .m = m, .group = i } },
        };
    }

    /// The same, for a shape that does NOT need its items ordered and can take
    /// whatever rows are going — top-N re-sorts, so it only cares that every row
    /// is seen once. A parquet lane then steals row groups off the shared queue
    /// and keeps ONE heap over all of them, instead of a heap per row group.
    ///
    /// `nitems` is still the thread count, and items are still stolen rather than
    /// indexed by lane: `spawnJoin` may spawn fewer threads than asked, and a
    /// CSV chunk keyed on lane index would then be silently skipped.
    fn unorderedRows(self: *LaneSplit, i: usize, nitems: usize) LaneRows {
        return switch (self.*) {
            .csv => |c| .{ .csv = .{ .mapped = c.mapped, .schema = c.schema, .chunk = i, .of = nitems } },
            .parquet => |*m| .{ .parquet = m },
        };
    }

    /// Stop the shared morsel queue after a lane has failed, so the others do not
    /// keep stealing work whose result is about to be thrown away.
    fn abort(self: *LaneSplit) void {
        switch (self.*) {
            .parquet => |*m| m.queue.failed.store(true, .seq_cst),
            .csv => {},
        }
    }

    /// How the item count reads in the debug line: chunks are ours to choose,
    /// row groups are the file's.
    fn unitName(self: LaneSplit) []const u8 {
        return switch (self) {
            .csv => "chunks",
            .parquet => "row groups",
        };
    }
};

/// Parallel map-only pipeline (`read <local> | (filter|select)* | write`): the
/// input is fanned out across lanes, each runs the map chain on its own share and
/// writes batches to a shared sink under a mutex (or to its own, with a per-lane
/// sink). Row ORDER is not preserved — lanes interleave; use `-j 1` for a
/// deterministic order. Optionally carries one hash join, whose build side is
/// materialized once before any lane exists and probed read-only by all of them.
const MapCtx = struct {
    split: LaneSplit,
    map_stages: []const ast.Stage,
    params: *std.StringHashMap(*const ast.Expr),
    queue: WorkQueue,
    sink_mode: parallel.SinkMode,
    sink_mtx: std.Thread.Mutex = .{},
    rows_out: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    rows_read: *std.atomic.Value(u64),
    /// Set when the pipeline carries one hash join: each lane probes the shared
    /// index with its own `op.Join` and runs the post-join stages itself.
    join: ?LaneJoin = null,
};

const mapWorker = dispatchWorker(MapCtx, mapWorkOne);

fn mapWorkOne(ctx: *MapCtx, i: usize) !void {
    var wgpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = false }){};
    defer _ = wgpa.deinit();
    const wa = wgpa.allocator();
    var warena = std.heap.ArenaAllocator.init(wa);
    defer warena.deinit();
    var batch_arena = std.heap.ArenaAllocator.init(wa);
    defer batch_arena.deinit();

    const own_sink: ?driver.Sink = switch (ctx.sink_mode) {
        .shared => null,
        .per_lane => |pl| try pl.open(pl.ctx, wa, i),
    };
    var own_sink_open = own_sink != null;
    defer if (own_sink) |s| {
        if (own_sink_open) s.abort();
    };

    const src_schema = ctx.split.schema();
    const inner = (try laneRowSource(ctx.split.unorderedRows(i, ctx.queue.nitems), warena.allocator())) orelse return;
    defer inner.close();
    errdefer ctx.split.abort();
    var cs = obs.CountingSource{ .inner = inner, .count = ctx.rows_read };
    var scan = op.Scan{ .src = cs.source() };
    const mapped_chain = try buildMapChain(warena.allocator(), ctx.params, ctx.map_stages, &scan, src_schema);
    const chain = if (ctx.join) |lj|
        try buildLaneJoinChain(warena.allocator(), ctx.params, lj, mapped_chain)
    else
        mapped_chain;

    var out: u64 = 0;
    while (try chain.next(batch_arena.allocator())) |b| {
        if (ctx.queue.failed.load(.seq_cst)) return;
        if (b.len > 0) {
            try parallel.writeLaneBatch(ctx.sink_mode, &ctx.sink_mtx, own_sink, batch_arena.allocator(), b);
            out += b.len;
        }
        _ = batch_arena.reset(.retain_capacity);
    }
    // One add rather than one per batch: the lanes all hit this counter.
    _ = ctx.rows_out.fetchAdd(out, .monotonic);

    if (own_sink) |s| {
        own_sink_open = false;
        try s.close();
    }
}

/// Source-independent — `split` says how the input divides; see `LaneSplit`.
fn runParallelMapImpl(
    env: *Env,
    split: LaneSplit,
    map_stages: []const ast.Stage,
    jshape: ?MapJoinShape,
    w: ast.Write,
    opts: RunOptions,
    stats: *Stats,
    lanes_used: *usize,
) anyerror!bool {
    const arena = env.arena;
    var out_schema = try mapChainSchema(env, map_stages, split.schema().*);

    env.src_name = split.label();
    env.sink_name = sinkLabel(env, w);

    // Before any lane exists: build side materialized, suffix prevalidated.
    var lane_join: ?LaneJoin = null;
    if (jshape) |js| {
        const lp = try resolveLaneJoin(env, js.join, js.join_hints, js.suffix, out_schema);
        lane_join = lp.lane;
        out_schema = lp.out_schema;
    }

    const wr = try resolveUpsertKeys(env, w);
    const sink_mode = (try buildParallelSink(env, wr, out_schema)) orelse
        parallel.SinkMode{ .shared = try openSink(env, wr, out_schema) };
    var shared_open = sink_mode == .shared;
    errdefer if (shared_open) sink_mode.shared.abort();

    const nthreads = @max(@as(usize, 1), opts.threads);
    var ctx = MapCtx{
        .split = split,
        .map_stages = map_stages,
        .params = env.params_expr,
        .queue = .{ .nitems = nthreads },
        .sink_mode = sink_mode,
        .rows_read = env.rows_read,
        .join = lane_join,
    };

    const lanes = try parallel.spawnJoin(arena, nthreads, mapWorker, &ctx);
    lanes_used.* = @max(lanes_used.*, lanes);
    const units = split.count(nthreads);
    env.log.log(.debug, "parallel {s} map{s}: {d} {s} over {d} lanes ({s} sink)", .{
        split.label(), if (lane_join != null) "+join" else "", units, split.unitName(), lanes, @tagName(sink_mode),
    });
    // Only the parquet path has ever printed this; left as it was rather than
    // widened, since `--explain` output is compared against goldens.
    if (opts.explain and split == .parquet) {
        std.debug.print("actuals (parallel map, {d} lanes over {d} row groups): {d} rows out\n", .{
            lanes, units, ctx.rows_out.load(.monotonic),
        });
    }

    if (ctx.queue.first_err) |e| return e;
    stats.rows_out += ctx.rows_out.load(.monotonic);
    if (sink_mode == .shared) {
        shared_open = false;
        try sink_mode.shared.close();
    }
    return true;
}

fn runParallelParquetMap(env: *Env, rd: ast.Read, pipeline: []const ast.Stage, map_stages: []const ast.Stage, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    return runParallelParquetMapImpl(env, rd, pipeline, map_stages, null, w, opts, stats, lanes_used);
}

/// Row-group fan-out with one hash join hoisted out of it.
fn runParallelParquetMapJoin(env: *Env, rd: ast.Read, pipeline: []const ast.Stage, shape: MapJoinShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    if (!joinKindLaneSafe(shape.join.kind)) return false;
    return runParallelParquetMapImpl(env, rd, pipeline, shape.prefix, shape, w, opts, stats, lanes_used);
}

fn runParallelParquetMapImpl(env: *Env, rd: ast.Read, pipeline: []const ast.Stage, map_stages: []const ast.Stage, jshape: ?MapJoinShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    if (std.mem.eql(u8, w.connector, "stdout")) return false;
    const split = (try parquetSplit(env, rd, pipeline[1..][0..map_stages.len], w, opts)) orelse return false;
    return runParallelMapImpl(env, split, map_stages, jshape, w, opts, stats, lanes_used);
}

const AggJoinShape = struct {
    map_stages: []const ast.Stage,
    /// Every stage from the first join up to the aggregate. Starts with a `.join`, and
    /// each join is followed by its own `filter`/`select` suffix. Held as a span rather
    /// than a list of steps so the classifier stays allocation-free;
    /// `resolveLaneJoins` walks it.
    join_span: []const ast.Stage,
    ag: ast.Aggregate,
    tail: []const ast.Stage,
};

fn classifyAggJoinPipeline(stages: []const ast.Stage) ?AggJoinShape {
    if (stages.len < 4) return null;
    if (stages[stages.len - 1].node != .write) return null;
    const middle = stages[1 .. stages.len - 1];
    var first_join: ?usize = null;
    var ai: ?usize = null;
    for (middle, 0..) |st, i| switch (st.node) {
        // A `select` after the aggregate is the reordering projection the parser emits
        // for an interleaved SELECT list. Rejecting it sent every such query to the
        // serial driver — 379ms against 88ms on TPC-H q01's shape. `writeTail` applies
        // it, and the sink is opened with the schema it produces.
        .filter => if (ai != null) return null,
        .select => {},
        .join => |j| {
            // A join chain is fine; a join *fed by* an aggregate is a different shape.
            if (ai != null) return null;
            // Only kinds whose per-lane join carries no cross-lane state. A right or
            // full join must emit the build rows that nothing matched, and each lane
            // tracks matches against its own copy — so every lane emitted the
            // unmatched rows again. TPC-H-shaped queries never noticed; a `RIGHT JOIN`
            // under a `COUNT(*)` returned 160 instead of 10 at `-j 16`. The map+join
            // paths refuse these for the same reason.
            if (!joinKindLaneSafe(j.kind)) return null;
            if (first_join == null) first_join = i;
        },
        .aggregate => {
            if (ai != null) return null;
            ai = i;
        },
        .sort, .limit => if (ai == null) return null,
        else => return null,
    };
    const j = first_join orelse return null;
    const a = ai orelse return null;
    return .{
        .map_stages = middle[0..j],
        .join_span = middle[j..a],
        .ag = middle[a].node.aggregate,
        .tail = middle[a + 1 ..],
    };
}

/// Materialize every build side in a join chain, left to right, threading each join's
/// output schema into the next as its probe schema. Returns the lane recipes in
/// application order plus the schema the aggregate above them reads.
///
/// A single join was the original limit, and it left TPC-H q03 — which joins two
/// dimensions — on the serial driver while q12 and q14 fanned out.
const LaneJoinChain = struct { joins: []const LaneJoin, out_schema: types.Schema };

fn resolveLaneJoins(env: *Env, join_span: []const ast.Stage, left_schema: types.Schema) anyerror!LaneJoinChain {
    var list = std.array_list.Managed(LaneJoin).init(env.arena);
    var schema = left_schema;
    var i: usize = 0;
    while (i < join_span.len) {
        // The span starts on a join, and `i` only ever advances to the next one.
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

/// Parallel aggregate over a local parquet file, one morsel per row group.
/// Two phases, neither of which takes a lock: lanes fold disjoint row groups
/// into private tables, then the tables are merged by hash partition.
///
/// Returns false (serial fallback) when the file has too few row groups to
/// split, or when fewer than `pq_min_lanes` lanes are available.
fn runParallelParquetAgg(env: *Env, rd: ast.Read, pipeline: []const ast.Stage, prefix: []const ast.Stage, ag: ast.Aggregate, tail: []const ast.Stage, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    return runParallelParquetAggImpl(env, rd, pipeline, prefix, ag, tail, null, w, opts, stats, lanes_used);
}

/// The join-then-aggregate shape: the build side is materialized once into a shared
/// index (exactly as the map+join path does) and each lane probes it, then folds its
/// own morsels into its own partial group set.
fn runParallelParquetAggJoin(env: *Env, rd: ast.Read, pipeline: []const ast.Stage, js: AggJoinShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    return runParallelParquetAggImpl(env, rd, pipeline, js.map_stages, js.ag, js.tail, js, w, opts, stats, lanes_used);
}

fn runParallelParquetAggImpl(env: *Env, rd: ast.Read, pipeline: []const ast.Stage, prefix: []const ast.Stage, ag: ast.Aggregate, tail: []const ast.Stage, jshape: ?AggJoinShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    const arena = env.arena;
    const split = (try parquetSplit(env, rd, pipeline[1..], w, opts)) orelse return false;
    const morsels = split.parquet;
    const ngroups = morsels.queue.nitems;
    const src_schema = morsels.src_schema;
    var agg_in_schema = try mapChainSchema(env, prefix, src_schema.*);

    // The build side is materialized here, once, and every lane probes it read-only.
    // `resolveLaneJoin` also prevalidates the post-join stages and hands back the
    // schema they produce — which is exactly what the aggregate reads.
    var lane_joins: []const LaneJoin = &.{};
    if (jshape) |js| {
        const chain = try resolveLaneJoins(env, js.join_span, agg_in_schema);
        lane_joins = chain.joins;
        agg_in_schema = chain.out_schema;
    }
    const agg_in = try schemaPtr(arena, agg_in_schema);

    var ad = analyze.Diag{};
    const apl = analyze.aggregatePlan(arena, agg_in.*, ag, env.params_expr, &ad) catch |e| return aErr(env, &ad, e);
    const aggs = try arena.alloc(op.Aggregate.Agg, apl.aggs.len);
    for (apl.aggs, aggs) |ra, *a| a.* = .{ .func = ra.func, .arg = ra.arg, .ty = ra.ty, .distinct = ra.distinct };
    const out_schema = try schemaPtr(arena, apl.schema);

    env.src_name = split.label();
    env.sink_name = sinkLabel(env, w);

    const nthreads = @max(@as(usize, 1), opts.threads);
    const lanes = try env.gpa.alloc(PqLane, nthreads);
    defer env.gpa.free(lanes);
    for (lanes) |*l| {
        l.* = .{ .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator) };
    }
    defer for (lanes) |*l| l.arena.deinit();

    var ctx = PqAggCtx{
        .morsels = morsels,
        .agg_in_schema = agg_in,
        .out_schema = out_schema,
        .prefix = prefix,
        .params = env.params_expr,
        .by = apl.by,
        .aggs = aggs,
        .lanes = lanes,
        .rows_read = env.rows_read,
        .joins = lane_joins,
    };

    const t_fold0 = std.time.Instant.now() catch unreachable;
    const used = try parallel.spawnJoin(arena, nthreads, pqAggLane, &ctx);
    const t_fold1 = std.time.Instant.now() catch unreachable;
    lanes_used.* = @max(lanes_used.*, used);
    if (ctx.morsels.queue.first_err) |e| return e;

    const parts = try env.gpa.alloc(PqPart, pq_parts);
    defer env.gpa.free(parts);
    for (parts) |*pp| {
        pp.* = .{ .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator), .map = undefined, .groups = undefined };
        pp.map = op.Aggregate.GroupMap().init(pp.arena.allocator());
        pp.groups = std.array_list.Managed(op.Aggregate.Group).init(pp.arena.allocator());
    }
    defer for (parts) |*pp| pp.arena.deinit();

    const t_mrg0 = std.time.Instant.now() catch unreachable;
    if (apl.by.len == 0) {
        // An ungrouped aggregate (`SELECT SUM(x) FROM ...`) folds to one implicit
        // group per lane and has no key to hash, so the radix merge below had
        // nothing to partition on and dropped every lane — COUNT(*) came back 0
        // instead of the row count, silently. That is why this shape used to be
        // turned away to the serial driver, at 3x the wall clock on 16 threads.
        //
        // With no key there is exactly one group, so partitioning buys nothing:
        // fold the lanes straight into partition 0. Walking them in lane index
        // order is what keeps a parallel float SUM reproducible, the same rule
        // `combineAggSlots` follows — and it is the CSV path's behaviour, which
        // has always run this shape in parallel.
        const dst = &parts[0];
        for (lanes) |*l| {
            try op.Aggregate.mergeGroups(&dst.map, &dst.groups, dst.arena.allocator(), l.groups, aggs);
        }
    } else {
        var mctx = PqMergeCtx{ .lanes = lanes, .parts = parts, .aggs = aggs, .queue = .{ .nitems = pq_parts } };
        _ = try parallel.spawnJoin(arena, nthreads, pqMergeWorker, &mctx);
        if (mctx.queue.first_err) |e| return e;
    }
    const t_mrg1 = std.time.Instant.now() catch unreachable;
    var lane_groups: usize = 0;
    for (lanes) |*l| lane_groups += l.groups.len;
    env.log.log(.debug, "pq agg phases: fold {d}ms (incl. bucket), merge {d}ms, {d} lane groups", .{
        t_fold1.since(t_fold0) / 1_000_000,
        t_mrg1.since(t_mrg0) / 1_000_000,
        lane_groups,
    });
    if (opts.explain) {
        std.debug.print(
            \\actuals (parallel aggregate, {d} lanes over {d} row groups):
            \\  fold+bucket  {d:>8.1}ms {d:>12} groups
            \\  merge        {d:>8.1}ms {d:>12} partitions
            \\
        , .{
            used,                                                  ngroups,
            @as(f64, @floatFromInt(t_fold1.since(t_fold0))) / 1e6, lane_groups,
            @as(f64, @floatFromInt(t_mrg1.since(t_mrg0))) / 1e6,   pq_parts,
        });
    }

    env.log.log(.debug, "parallel parquet aggregate: {d} row groups in {d} morsels over {d} lanes, merged in {d} partitions", .{ ngroups, ngroups, used, pq_parts });

    if (topNTail(tail)) |tn| {
        var cols = std.array_list.Managed(usize).init(arena);
        var descs = std.array_list.Managed(bool).init(arena);
        var ok = true;
        for (tn.keys) |k| {
            const idx = out_schema.indexOf(k.field.last()) orelse {
                ok = false;
                break;
            };
            try cols.append(idx);
            try descs.append(k.desc);
        }
        if (ok) {
            var tctx = PqTopNCtx{
                .parts = parts,
                .order = .{ .cols = cols.items, .desc = descs.items, .by_len = apl.by.len, .aggs = aggs },
                .n = tn.n,
                .queue = .{ .nitems = pq_parts },
            };
            _ = try parallel.spawnJoin(arena, nthreads, pqTopNWorker, &tctx);
            if (tctx.queue.first_err) |e| return e;
        }
    }

    const t_emit0 = std.time.Instant.now() catch unreachable;
    var total: usize = 0;
    for (parts) |*pp| total += pp.groups.items.len;
    const all = try arena.alloc(op.Aggregate.Group, total);
    var at: usize = 0;
    for (parts) |*pp| {
        for (pp.groups.items) |g| {
            all[at] = g;
            at += 1;
        }
    }

    const batch = try emitMergedGroups(env, agg_in, apl.by, aggs, out_schema, all);
    const t_emit1 = std.time.Instant.now() catch unreachable;

    const wr = try resolveUpsertKeys(env, w);
    const snk = try openSink(env, wr, try tailSchema(env, tail, out_schema.*));
    var snk_open = true;
    errdefer if (snk_open) snk.abort();

    const t_tail0 = std.time.Instant.now() catch unreachable;
    try writeTail(env, snk, batch, out_schema.*, tail, stats);
    const t_tail1 = std.time.Instant.now() catch unreachable;
    env.log.log(.debug, "pq agg tail: emit {d}ms ({d} groups), sort+limit+write {d}ms", .{
        t_emit1.since(t_emit0) / 1_000_000, total, t_tail1.since(t_tail0) / 1_000_000,
    });
    if (opts.explain) {
        std.debug.print(
            \\  emit         {d:>8.1}ms {d:>12} rows
            \\  sort+write   {d:>8.1}ms
            \\
        , .{
            @as(f64, @floatFromInt(t_emit1.since(t_emit0))) / 1e6, total,
            @as(f64, @floatFromInt(t_tail1.since(t_tail0))) / 1e6,
        });
    }
    snk_open = false;
    try snk.close();
    return true;
}

/// Returns true if it handled the pipeline in parallel; false to fall back to serial.
fn runParallelCsvAgg(env: *Env, rd: ast.Read, prefix: []const ast.Stage, ag: ast.Aggregate, tail: []const ast.Stage, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    return runParallelCsvAggImpl(env, rd, prefix, ag, tail, null, w, opts, stats, lanes_used);
}

/// The join-then-aggregate shape over a CSV source. The parquet twin is
/// `runParallelParquetAggJoin`; keeping both means a shape does not silently lose its
/// parallelism by changing file format, which is how an ungrouped aggregate came to
/// run on one core for parquet and sixteen for CSV.
fn runParallelCsvAggJoin(env: *Env, rd: ast.Read, js: AggJoinShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    return runParallelCsvAggImpl(env, rd, js.map_stages, js.ag, js.tail, js, w, opts, stats, lanes_used);
}

fn runParallelCsvAggImpl(env: *Env, rd: ast.Read, prefix: []const ast.Stage, ag: ast.Aggregate, tail: []const ast.Stage, jshape: ?AggJoinShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    const arena = env.arena;
    const mapped = (try csvSplitFile(env, rd, w)) orelse return false;
    defer mapped.close();

    var agg_in_schema = try mapChainSchema(env, prefix, mapped.schema);
    var lane_joins: []const LaneJoin = &.{};
    if (jshape) |js| {
        const chain = try resolveLaneJoins(env, js.join_span, agg_in_schema);
        lane_joins = chain.joins;
        agg_in_schema = chain.out_schema;
    }
    const agg_in = try schemaPtr(arena, agg_in_schema);

    var ad = analyze.Diag{};
    const apl = analyze.aggregatePlan(arena, agg_in.*, ag, env.params_expr, &ad) catch |e| return aErr(env, &ad, e);
    const aggs = try arena.alloc(op.Aggregate.Agg, apl.aggs.len);
    for (apl.aggs, aggs) |ra, *a| a.* = .{ .func = ra.func, .arg = ra.arg, .ty = ra.ty, .distinct = ra.distinct };
    const out_schema = try schemaPtr(arena, apl.schema);

    env.src_name = "csv";
    env.sink_name = sinkLabel(env, w);

    const nthreads = @max(@as(usize, 1), opts.threads);
    const slots = try allocAggSlots(env.gpa, nthreads);
    defer freeAggSlots(env.gpa, slots);
    const parts = try allocMergeParts(env.gpa);
    defer freeMergeParts(env.gpa, parts);
    var ctx = AggCtx{
        .mapped = mapped,
        .csv_schema = &mapped.schema,
        .agg_in_schema = agg_in,
        .out_schema = out_schema,
        .prefix = prefix,
        .params = env.params_expr,
        .by = apl.by,
        .aggs = aggs,
        .queue = .{ .nitems = nthreads },
        .slots = slots,
        .rows_read = env.rows_read,
        .joins = lane_joins,
    };

    const lanes = try parallel.spawnJoin(arena, nthreads, aggWorker, &ctx);
    lanes_used.* = @max(lanes_used.*, lanes);
    env.log.log(.debug, "parallel csv aggregate: {d} chunks over {d} lanes", .{ nthreads, lanes });

    if (ctx.queue.first_err) |e| return e;

    const cgroups = try combineAggSlots(env, slots, aggs, opts.threads, parts);
    const batch = try emitMergedGroups(env, agg_in, apl.by, aggs, out_schema, cgroups);

    const wr = try resolveUpsertKeys(env, w);
    const snk = try openSink(env, wr, try tailSchema(env, tail, out_schema.*));
    var snk_open = true;
    errdefer if (snk_open) snk.abort();

    try writeTail(env, snk, batch, out_schema.*, tail, stats);
    snk_open = false;
    try snk.close();
    return true;
}

/// Shared state for parallel SQL-aggregate lanes. Mirrors `AggCtx`, but each lane opens
/// its own DB connection over one key-range predicate (`openSplitSource`) instead of
/// reading a CSV byte-range. Each lane folds its range into that range's `AggSlot`, and
/// `combineAggSlots` folds the slots together in key-range order at the raw-`Acc` level
/// (so AVG stays correct, and a float SUM adds the same way on every run). The combine is
/// O(groups) vs the O(rows) fold, so it costs little next to the scan.
const SqlAggCtx = struct {
    split: SplitCtx,
    predicates: []const []const u8,
    proj_select: ?[]const u8,
    where_extra: ?[]const u8,
    src_schema: *const types.Schema,
    agg_in_schema: *const types.Schema,
    out_schema: *const types.Schema,
    prefix: []const ast.Stage,
    params: *std.StringHashMap(*const ast.Expr),
    by: []const usize,
    aggs: []const op.Aggregate.Agg,
    queue: WorkQueue,
    slots: []AggSlot,
    rows_read: *std.atomic.Value(u64),
};

const sqlAggWorker = dispatchWorker(SqlAggCtx, sqlAggWorkOne);

fn sqlAggWorkOne(ctx: *SqlAggCtx, i: usize) !void {
    const slot = &ctx.slots[i];
    const wa = slot.arena.allocator();

    const q = try wrapProjected(wa, ctx.split.base_sql, ctx.proj_select, ctx.predicates[i], ctx.where_extra);
    const src = try openSqlQuery(&ctx.split, slot.gpa.allocator(), q);
    defer src.close();

    var cs = obs.CountingSource{ .inner = src, .count = ctx.rows_read };
    var scan = op.Scan{ .src = cs.source() };
    const child = try buildMapChain(wa, ctx.params, ctx.prefix, &scan, ctx.src_schema);
    var agg = op.Aggregate{
        .child = child,
        .in_schema = ctx.agg_in_schema,
        .by = ctx.by,
        .aggs = ctx.aggs,
        .out_schema = ctx.out_schema,
        .err = null,
        .state = wa,
        .gpa = slot.gpa.allocator(),
    };
    const drained = try agg.drainGroupsHashed();
    slot.groups = drained.groups;
    slot.hashes = drained.hashes;
    slot.buckets = try bucketByHash(wa, drained.hashes);
    slot.done = true;
}

/// Parallel SQL aggregate: `read <sqltable> | (filter|select)* | aggregate | (sort|limit)* | write`
/// over a splittable source. Fans into key-range lanes (one DB connection each), folds a
/// partial group set per lane, merges at the raw-`Acc` level, then runs the small
/// post-aggregate tail serially over the merged batch. Returns false to fall back to the
/// serial path (non-splittable source, no split plan, bare upsert). NOTE: exercised only
/// against a live DB — there is no local DB in the test suite, so this path is covered by
/// the shared CSV-aggregate machinery (`drainGroups`/`mergeGroups`/`emit`) it reuses, not
/// by a direct test.
pub fn runParallelSqlAgg(env: *Env, stages: []const ast.Stage, prefix: []const ast.Stage, ag: ast.Aggregate, tail: []const ast.Stage, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize, src_base: usize) anyerror!bool {
    const arena = env.arena;
    const desc = env.sql_desc orelse return false;
    if (stages[0].node != .read) return false;
    if (w.mode == .upsert and w.mode.upsert.keys.len == 0) return false;
    if (src_base >= env.sources.items.len) return false;

    const src_schema = try schemaPtr(arena, try dupeSchema(arena, env.sources.items[src_base].schema()));

    const pd = try pushdown.planAgg(arena, desc.dialect, src_schema.*, prefix, ag);
    const eff = if (pd.proj_schema) |ps| try schemaPtr(arena, ps) else src_schema;

    const agg_in = try schemaPtr(arena, try mapChainSchema(env, prefix, eff.*));

    var ad = analyze.Diag{};
    const apl = analyze.aggregatePlan(arena, agg_in.*, ag, env.params_expr, &ad) catch |e| return aErr(env, &ad, e);
    const aggs = try arena.alloc(op.Aggregate.Agg, apl.aggs.len);
    for (apl.aggs, aggs) |ra, *a| a.* = .{ .func = ra.func, .arg = ra.arg, .ty = ra.ty, .distinct = ra.distinct };
    const out_schema = try schemaPtr(arena, apl.schema);

    const sp = (try planSplit(env, desc, stages[0], opts.threads, w)) orelse return false;

    for (env.sources.items[src_base..]) |sc| sc.close();
    env.sources.shrinkRetainingCapacity(src_base);

    env.sink_name = sinkLabel(env, w);

    const nlanes = @min(@max(@as(usize, 1), opts.threads), sp.predicates.len);
    // One slot per key range, not per lane: the lanes steal ranges off the queue,
    // so only the range index is a stable identity to combine in.
    const slots = try allocAggSlots(env.gpa, sp.predicates.len);
    defer freeAggSlots(env.gpa, slots);
    const parts = try allocMergeParts(env.gpa);
    defer freeMergeParts(env.gpa, parts);
    var ctx = SqlAggCtx{
        .split = .{ .gpa = env.gpa, .kind = desc.kind, .cfg = desc.cfg, .base_sql = desc.base_sql },
        .predicates = sp.predicates,
        .proj_select = pd.proj_select,
        .where_extra = pd.where_extra,
        .src_schema = eff,
        .agg_in_schema = agg_in,
        .out_schema = out_schema,
        .prefix = prefix,
        .params = env.params_expr,
        .by = apl.by,
        .aggs = aggs,
        .queue = .{ .nitems = sp.predicates.len },
        .slots = slots,
        .rows_read = env.rows_read,
    };

    const lanes = try parallel.spawnJoin(arena, nlanes, sqlAggWorker, &ctx);
    lanes_used.* = @max(lanes_used.*, lanes);
    env.log.log(.debug, "parallel sql aggregate: {d} splits over {d} lanes on key range (projection: {d}/{d} cols, filter pushdown: {s})", .{
        sp.predicates.len,
        lanes,
        if (pd.proj_schema) |ps| ps.fields.len else src_schema.fields.len,
        src_schema.fields.len,
        if (pd.where_extra != null) "yes" else "no",
    });

    if (ctx.queue.first_err) |e| return e;

    const cgroups = try combineAggSlots(env, slots, aggs, opts.threads, parts);
    const batch = try emitMergedGroups(env, agg_in, apl.by, aggs, out_schema, cgroups);

    const snk = try openSink(env, w, out_schema.*);
    var snk_open = true;
    errdefer if (snk_open) snk.abort();

    try writeTail(env, snk, batch, out_schema.*, tail, stats);
    snk_open = false;
    try snk.close();
    return true;
}

/// Recognize a map-only `read … | (filter|select)* | write` (no breaker, no limit),
/// returning the middle stages. Such a pipeline parallelizes by byte-range chunks
/// with each worker writing to a shared sink. null → not eligible.
fn classifyMapPipeline(stages: []const ast.Stage) ?[]const ast.Stage {
    const middle = stages[1 .. stages.len - 1];
    for (middle) |st| switch (st.node) {
        .filter, .select => {},
        else => return null,
    };
    return middle;
}

const MapJoinShape = struct { prefix: []const ast.Stage, join: ast.Join, join_hints: []const ast.Hint, suffix: []const ast.Stage };

/// Recognize `read … | (filter|select)* | join | (filter|select)* | write` — exactly
/// one hash join, no breaker anywhere. The build side is materialized once up front
/// and the probe side then fans out over morsels like a plain map pipeline.
/// null → not eligible (any other stage, a second join, or no join at all).
pub fn classifyMapJoinPipeline(stages: []const ast.Stage) ?MapJoinShape {
    if (stages.len < 3) return null;
    if (stages[stages.len - 1].node != .write) return null;
    const middle = stages[1 .. stages.len - 1];
    var ji: ?usize = null;
    for (middle, 0..) |st, i| switch (st.node) {
        .filter, .select => {},
        .join => {
            if (ji != null) return null;
            ji = i;
        },
        else => return null,
    };
    const j = ji orelse return null;
    return .{ .prefix = middle[0..j], .join = middle[j].node.join, .join_hints = middle[j].hints, .suffix = middle[j + 1 ..] };
}

/// Join kinds whose probe is a pure lookup into a read-only index, so every lane can
/// share one index and emit independently. `right`/`full` have to remember which
/// build rows matched — that state is global to the probe, not per-lane, so they stay
/// on the serial driver.
///
/// Matched by tag name rather than by a switch: the kind set is still growing on
/// another branch, and an allowlist spelled this way stays correct either way.
pub fn joinKindLaneSafe(kind: ast.JoinKind) bool {
    for ([_][]const u8{ "inner", "left", "semi", "anti", "cross" }) |ok| {
        if (std.mem.eql(u8, @tagName(kind), ok)) return true;
    }
    return false;
}

/// Read a key-index field off `analyze.JoinPlan` as a slice. The multi-key rework
/// turns the single `lk`/`rk` index into an array; accepting both spellings and both
/// arities keeps this path building against either snapshot.
fn planKeys(a: std.mem.Allocator, jp: analyze.JoinPlan, comptime which: []const u8) ![]const usize {
    const name: []const u8 = comptime if (@hasField(analyze.JoinPlan, which)) which else if (std.mem.eql(u8, which, "lk")) "lks" else "rks";
    const v = @field(jp, name);
    return if (@TypeOf(v) == usize) try a.dupe(usize, &[_]usize{v}) else v;
}

/// What a lane needs to rebuild its own `op.Join` over the one shared build index.
/// Everything here is read-only for the lifetime of the fan-out: `index` is fully
/// populated before any lane starts, and the schemas/key slices live in the plan
/// arena. Each lane still allocates its OWN `op.Join` (it carries mutable stats and
/// per-batch scratch) — only the index is shared.
const LaneJoin = struct {
    index: *op.JoinIndex,
    left_keys: []const usize,
    right_keys: []const usize,
    left_schema: *const types.Schema,
    right_schema: *const types.Schema,
    out_schema: *const types.Schema,
    kind: ast.JoinKind,
    suffix: []const ast.Stage,
};

const LaneJoinPlan = struct { lane: LaneJoin, out_schema: types.Schema };

/// Hoist the join out of the fan-out: resolve the binding, materialize the build side
/// once into a shared read-only index, and prevalidate the post-join stages against
/// the join's output schema so a lane rebuild cannot fail where this succeeded.
/// Returns the lane recipe plus the pipeline's final output schema (what the sink
/// gets opened with).
fn resolveLaneJoin(env: *Env, j: ast.Join, join_hints: []const ast.Hint, suffix: []const ast.Stage, left_schema: types.Schema) anyerror!LaneJoinPlan {
    const arena = env.arena;
    const binding = env.bindings.get(j.binding) orelse
        return planErr(env.diag, try std.fmt.allocPrint(arena, "unknown binding `{s}` in join", .{j.binding}));
    const build = try buildPipeline(env, binding.stages);

    var ad = analyze.Diag{};
    const jp = analyze.joinPlan(arena, left_schema, build.schema, j, &ad) catch |e| return aErr(env, &ad, e);
    const out = try schemaPtr(arena, jp.schema);
    const right_schema = try schemaPtr(arena, build.schema);
    const right_keys = try planKeys(arena, jp, "rk");

    // Serial prevalidation of the suffix, exactly as `mapChainSchema` does for the
    // prefix: any analyze error surfaces here, with a diag, before any lane exists.
    const final_schema = try mapChainSchema(env, suffix, out.*);

    // The index outlives the fan-out (plan arena); the pulls that fill it are scratch
    // and go away with `build_arena` — `materializeFull` copies into the state arena.
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
            .suffix = suffix,
        },
        .out_schema = final_schema,
    };
}

/// Per-lane tail of the operator tree: this lane's own `op.Join` over the shared
/// index, then the post-join `filter`/`select` stages. `ta` is the lane arena, so
/// nothing built here is touched by another thread.
fn buildLaneJoinChain(ta: std.mem.Allocator, params: *std.StringHashMap(*const ast.Expr), lj: LaneJoin, probe: op.Op) !op.Op {
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
        .state = ta,
    };
    return buildChainFrom(ta, params, lj.suffix, .{ .join = j }, lj.out_schema.*);
}

/// One lane of a split-parallel SQL map+join. Everything here is either read-only
/// for the fan-out (the split recipe, the shared build index, the stage lists) or
/// atomic/mutex-guarded (`queue`, `rows_out`, the shared sink).
const SqlMapJoinCtx = struct {
    split: SplitCtx,
    predicates: []const []const u8,
    src_schema: *const types.Schema,
    prefix: []const ast.Stage,
    join: LaneJoin,
    params: *std.StringHashMap(*const ast.Expr),
    queue: WorkQueue,
    sink_mode: parallel.SinkMode,
    sink_mtx: std.Thread.Mutex = .{},
    rows_out: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    rows_read: *std.atomic.Value(u64),
};

/// A lane's probe source: one key-range query at a time, rolling to the next split
/// when the current one runs dry. Stealing inside the source is what lets a lane
/// build its operator chain — and open its sink — once however many splits it takes,
/// the same deal `parallel.run` gives the map-only split path.
const SplitSource = struct {
    ctx: *SqlMapJoinCtx,
    gpa: std.mem.Allocator,
    cur: ?driver.Source = null,

    fn schemaFn(ptr: *anyopaque) types.Schema {
        const self: *SplitSource = @ptrCast(@alignCast(ptr));
        return self.ctx.src_schema.*;
    }
    fn nextFn(ptr: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
        const self: *SplitSource = @ptrCast(@alignCast(ptr));
        while (true) {
            if (self.cur) |src| {
                if (try src.next(arena)) |b| return b;
                src.close();
                self.cur = null;
            }
            if (self.ctx.queue.failed.load(.seq_cst)) return null;
            const i = self.ctx.queue.next.fetchAdd(1, .seq_cst);
            if (i >= self.ctx.queue.nitems) return null;
            const q = try wrapProjected(self.gpa, self.ctx.split.base_sql, self.ctx.split.proj_select, self.ctx.predicates[i], self.ctx.split.where_extra);
            defer self.gpa.free(q);
            self.cur = try openSqlQuery(&self.ctx.split, self.gpa, q);
        }
    }
    fn closeFn(ptr: *anyopaque) void {
        const self: *SplitSource = @ptrCast(@alignCast(ptr));
        if (self.cur) |src| src.close();
        self.cur = null;
    }
    const vtable = driver.Source.VTable{ .schema = schemaFn, .next = nextFn, .close = closeFn };
};

fn sqlMapJoinLane(ctx: *SqlMapJoinCtx, lane_idx: usize) void {
    sqlMapJoinLaneRun(ctx, lane_idx) catch |e| ctx.queue.fail(e);
}

fn sqlMapJoinLaneRun(ctx: *SqlMapJoinCtx, lane_idx: usize) !void {
    var wgpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = false }){};
    defer _ = wgpa.deinit();
    const wa = wgpa.allocator();
    var warena = std.heap.ArenaAllocator.init(wa);
    defer warena.deinit();
    var batch_arena = std.heap.ArenaAllocator.init(wa);
    defer batch_arena.deinit();

    const own_sink: ?driver.Sink = switch (ctx.sink_mode) {
        .shared => null,
        .per_lane => |pl| try pl.open(pl.ctx, wa, lane_idx),
    };
    var own_sink_open = own_sink != null;
    defer if (own_sink) |sk| {
        if (own_sink_open) sk.abort();
    };

    var ss = SplitSource{ .ctx = ctx, .gpa = wa };
    defer SplitSource.closeFn(&ss);
    var cs = obs.CountingSource{ .inner = .{ .ptr = &ss, .vtable = &SplitSource.vtable }, .count = ctx.rows_read };
    var scan = op.Scan{ .src = cs.source() };
    const probe = try buildMapChain(warena.allocator(), ctx.params, ctx.prefix, &scan, ctx.src_schema);
    const chain = try buildLaneJoinChain(warena.allocator(), ctx.params, ctx.join, probe);

    var out: u64 = 0;
    while (try chain.next(batch_arena.allocator())) |b| {
        try parallel.writeLaneBatch(ctx.sink_mode, &ctx.sink_mtx, own_sink, batch_arena.allocator(), b);
        out += b.len;
        _ = batch_arena.reset(.retain_capacity);
    }
    _ = ctx.rows_out.fetchAdd(out, .monotonic);
    if (own_sink) |sk| {
        own_sink_open = false;
        try sk.close();
    }
}

/// Split-parallel SQL map+join:
/// `read <sqltable> | (filter|select)* | join | (filter|select)* | write` over a
/// splittable source. The build side is materialized once here into a shared
/// read-only index; each lane then opens its own connection per key range, runs the
/// pre-join chain, probes that index with its own `op.Join`, and runs the post-join
/// stages. Returns false to fall back to the serial driver (non-splittable source,
/// no split plan, bare upsert, join kind whose probe carries global state).
///
/// NOTE: like `runParallelSqlAgg`, only exercised against a live DB — the local test
/// suite covers the shared join machinery through the CSV/parquet fan-outs.
pub fn runParallelSqlMapJoin(env: *Env, stages: []const ast.Stage, shape: MapJoinShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize, src_base: usize) anyerror!bool {
    const arena = env.arena;
    if (stages[0].node != .read) return false;
    // Not `env.sql_desc`: the build side planned after this read may have replaced it.
    const desc = (try sqlDescForStage(env, stages[0])) orelse return false;
    if (!joinKindLaneSafe(shape.join.kind)) return false;
    if (w.mode == .upsert and w.mode.upsert.keys.len == 0) return false;
    if (src_base >= env.sources.items.len) return false;

    const src_schema = try schemaPtr(arena, try dupeSchema(arena, env.sources.items[src_base].schema()));
    const probe_schema = try mapChainSchema(env, shape.prefix, src_schema.*);

    const sp = (try planSplit(env, desc, stages[0], opts.threads, w)) orelse return false;

    // Drop the serial plan's sources (this read and the build side's) before the
    // shared index opens its own, so nothing this path needs gets closed under it.
    for (env.sources.items[src_base..]) |sc| sc.close();
    env.sources.shrinkRetainingCapacity(src_base);

    // Before any lane exists: build side materialized, suffix prevalidated.
    const lp = try resolveLaneJoin(env, shape.join, shape.join_hints, shape.suffix, probe_schema);

    env.sink_name = sinkLabel(env, w);
    const sink_mode: parallel.SinkMode = (try buildParallelSink(env, w, lp.out_schema)) orelse
        .{ .shared = try openSink(env, w, lp.out_schema) };
    var shared_open = sink_mode == .shared;
    errdefer if (shared_open) sink_mode.shared.abort();

    // ponytail: no projection narrowing under a join. `pushdown.planMap`'s liveness is
    // map-shaped; past a join the live set is the left join keys plus whatever the
    // suffix and the emitted left columns read, which it does not compute — so lanes
    // select `*`. Leading prefix filters are already folded into `base_sql` by
    // runOutput's implicit pushdown, so the WHERE half needs nothing here either.
    // Upgrade: join-aware liveness.
    var ctx = SqlMapJoinCtx{
        .split = .{ .gpa = env.gpa, .kind = desc.kind, .cfg = desc.cfg, .base_sql = desc.base_sql },
        .predicates = sp.predicates,
        .src_schema = src_schema,
        .prefix = shape.prefix,
        .join = lp.lane,
        .params = env.params_expr,
        .queue = .{ .nitems = sp.predicates.len },
        .sink_mode = sink_mode,
        .rows_read = env.rows_read,
    };

    const nlanes = @min(@max(@as(usize, 1), opts.threads), sp.predicates.len);
    const lanes = try parallel.spawnJoin(arena, nlanes, sqlMapJoinLane, &ctx);
    lanes_used.* = @max(lanes_used.*, lanes);
    env.log.log(.debug, "split-parallel map+join: {d} splits over {d} lanes", .{ sp.predicates.len, lanes });

    if (ctx.queue.first_err) |e| return e;
    stats.rows_out += ctx.rows_out.load(.monotonic);
    if (sink_mode == .shared) {
        shared_open = false;
        try sink_mode.shared.close();
    }
    return true;
}

fn runParallelCsvMap(env: *Env, rd: ast.Read, map_stages: []const ast.Stage, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    return runParallelCsvMapImpl(env, rd, map_stages, null, w, opts, stats, lanes_used);
}

/// The same fan-out with one hash join hoisted out of it.
fn runParallelCsvMapJoin(env: *Env, rd: ast.Read, shape: MapJoinShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    if (!joinKindLaneSafe(shape.join.kind)) return false;
    return runParallelCsvMapImpl(env, rd, shape.prefix, shape, w, opts, stats, lanes_used);
}

fn runParallelCsvMapImpl(env: *Env, rd: ast.Read, map_stages: []const ast.Stage, jshape: ?MapJoinShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    if (std.mem.eql(u8, w.connector, "stdout")) return false;
    const mapped = (try csvSplitFile(env, rd, w)) orelse return false;
    defer mapped.close();
    return runParallelMapImpl(env, .{ .csv = .{ .mapped = mapped, .schema = &mapped.schema } }, map_stages, jshape, w, opts, stats, lanes_used);
}

const TopNShape = struct { prefix: []const ast.Stage, srt: ast.Sort, lim: ast.Limit };

/// Recognize `read … | (filter|select)* | sort | limit | write` — a Top-N. The sort
/// must be immediately followed by the limit (the fusable form). null otherwise.
fn classifyTopNPipeline(stages: []const ast.Stage) ?TopNShape {
    const middle = stages[1 .. stages.len - 1];
    if (middle.len < 2) return null;
    if (middle[middle.len - 1].node != .limit or middle[middle.len - 2].node != .sort) return null;
    const prefix = middle[0 .. middle.len - 2];
    for (prefix) |st| switch (st.node) {
        .filter, .select => {},
        else => return null,
    };
    return .{ .prefix = prefix, .srt = middle[middle.len - 2].node.sort, .lim = middle[middle.len - 1].node.limit };
}

/// Parallel top-N: each work item keeps its own top `offset+count` rows, then a
/// global top-N over the union produces the final sorted, offset/limited output.
/// Only `cap` rows per item ever reach the combine. The output is small and
/// sorted, so (unlike map-only) it stays deterministic.
const TopNCtx = struct {
    split: LaneSplit,
    row_schema: *const types.Schema,
    prefix: []const ast.Stage,
    params: *std.StringHashMap(*const ast.Expr),
    keys: []const op.Sort.Key,
    cap: u64,
    queue: WorkQueue,
    builders: []column.Builder,
    mtx: std.Thread.Mutex = .{},
    rows_read: *std.atomic.Value(u64),
};

const topnWorker = dispatchWorker(TopNCtx, topnWorkOne);

fn topnWorkOne(ctx: *TopNCtx, i: usize) !void {
    var wgpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = false }){};
    defer _ = wgpa.deinit();
    var warena = std.heap.ArenaAllocator.init(wgpa.allocator());
    defer warena.deinit();
    var batch_arena = std.heap.ArenaAllocator.init(wgpa.allocator());
    defer batch_arena.deinit();

    const src_schema = ctx.split.schema();
    const rows = ctx.split.unorderedRows(i, ctx.queue.nitems);
    const inner = (try laneRowSource(rows, warena.allocator())) orelse return;
    defer inner.close();
    errdefer ctx.split.abort();
    var cs = obs.CountingSource{ .inner = inner, .count = ctx.rows_read };
    var scan = op.Scan{ .src = cs.source() };
    const child = try buildMapChain(warena.allocator(), ctx.params, ctx.prefix, &scan, src_schema);
    var tn = op.TopN{
        .child = child,
        .in_schema = ctx.row_schema,
        .keys = ctx.keys,
        .count = ctx.cap,
        .offset = 0,
        .state = batch_arena.allocator(),
        .gpa = wgpa.allocator(),
    };
    const local = (try tn.next(batch_arena.allocator())) orelse return;

    ctx.mtx.lock();
    defer ctx.mtx.unlock();
    var r: usize = 0;
    while (r < local.len) : (r += 1) {
        for (local.columns, ctx.builders) |*col, *bld| try bld.append(col.getValue(r));
    }
}

/// Source-independent — `split` says how the input divides; see `LaneSplit`.
fn runParallelTopN(
    env: *Env,
    split: LaneSplit,
    prefix: []const ast.Stage,
    srt: ast.Sort,
    lim: ast.Limit,
    w: ast.Write,
    opts: RunOptions,
    stats: *Stats,
    lanes_used: *usize,
) anyerror!bool {
    const arena = env.arena;
    const row_schema = try schemaPtr(arena, try mapChainSchema(env, prefix, split.schema().*));

    const qs = try arena.alloc(ast.QualName, srt.keys.len);
    for (srt.keys, qs) |sk, *q| q.* = sk.field;
    var ad = analyze.Diag{};
    const idxs = analyze.fieldIndices(arena, row_schema.*, qs, &ad) catch |e| return aErr(env, &ad, e);
    const ks = try arena.alloc(op.Sort.Key, srt.keys.len);
    for (srt.keys, idxs, ks) |sk, idx, *k| k.* = .{ .idx = idx, .desc = sk.desc };

    env.src_name = split.label();
    env.sink_name = sinkLabel(env, w);

    const nthreads = @max(@as(usize, 1), opts.threads);
    const builders = try arena.alloc(column.Builder, row_schema.fields.len);
    for (builders, row_schema.fields) |*b, f| b.* = column.Builder.init(arena, f.ty);
    var ctx = TopNCtx{
        .split = split,
        .row_schema = row_schema,
        .prefix = prefix,
        .params = env.params_expr,
        .keys = ks,
        .cap = lim.offset + lim.count,
        .queue = .{ .nitems = nthreads },
        .builders = builders,
        .rows_read = env.rows_read,
    };

    const lanes = try parallel.spawnJoin(arena, nthreads, topnWorker, &ctx);
    lanes_used.* = @max(lanes_used.*, lanes);
    env.log.log(.debug, "parallel {s} top-n: {d} {s} over {d} lanes", .{ split.label(), split.count(nthreads), split.unitName(), lanes });
    if (ctx.queue.first_err) |e| return e;

    const cols = try arena.alloc(column.Column, builders.len);
    for (builders, cols) |*b, *c| c.* = try b.finish();
    var combined = OneBatch{ .b = .{ .schema = row_schema, .columns = cols, .len = cols[0].len }, .sch = row_schema.* };
    var gscan = op.Scan{ .src = combined.source() };
    var global = op.TopN{
        .child = .{ .scan = &gscan },
        .in_schema = row_schema,
        .keys = ks,
        .count = lim.count,
        .offset = lim.offset,
        .state = arena,
        .gpa = env.gpa,
    };

    const wr = try resolveUpsertKeys(env, w);
    const snk = try openSink(env, wr, row_schema.*);
    var snk_open = true;
    errdefer if (snk_open) snk.abort();
    while (try global.next(arena)) |b| {
        try snk.writeBatch(arena, b);
        stats.rows_out += b.len;
    }
    snk_open = false;
    try snk.close();
    return true;
}

fn runParallelParquetTopN(env: *Env, rd: ast.Read, pipeline: []const ast.Stage, prefix: []const ast.Stage, srt: ast.Sort, lim: ast.Limit, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    const split = (try parquetSplit(env, rd, pipeline[1..], w, opts)) orelse return false;
    return runParallelTopN(env, split, prefix, srt, lim, w, opts, stats, lanes_used);
}

fn runParallelCsvTopN(env: *Env, rd: ast.Read, prefix: []const ast.Stage, srt: ast.Sort, lim: ast.Limit, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    const mapped = (try csvSplitFile(env, rd, w)) orelse return false;
    defer mapped.close();
    return runParallelTopN(env, .{ .csv = .{ .mapped = mapped, .schema = &mapped.schema } }, prefix, srt, lim, w, opts, stats, lanes_used);
}

const DistinctShape = struct { prefix: []const ast.Stage, dist: ast.Distinct, tail: []const ast.Stage };

/// Recognize `read … | (filter|select)* | distinct | (sort|limit)* | write`.
fn classifyDistinctPipeline(stages: []const ast.Stage) ?DistinctShape {
    const middle = stages[1 .. stages.len - 1];
    var di: ?usize = null;
    for (middle, 0..) |st, i| switch (st.node) {
        .filter, .select => if (di != null) return null,
        .distinct => {
            if (di != null) return null;
            di = i;
        },
        .sort, .limit => if (di == null) return null,
        else => return null,
    };
    const d = di orelse return null;
    return .{ .prefix = middle[0..d], .dist = middle[d].node.distinct, .tail = middle[d + 1 ..] };
}

/// A surviving distinct row plus where it sat in the input: chunk index (chunks
/// are handed out in file order) then row ordinal inside that chunk. `DISTINCT`
/// keeps the *first* row per key, so the merge has to be able to say which of
/// two candidates came first — otherwise the winner is whichever lane reached
/// the mutex first and the non-key columns change from run to run.
const DistinctRow = struct {
    chunk: usize,
    ord: u64,
    vals: []Value,

    fn before(_: void, a: DistinctRow, b: DistinctRow) bool {
        if (a.chunk != b.chunk) return a.chunk < b.chunk;
        return a.ord < b.ord;
    }
};

/// Cross-lane distinct state. Each lane dedups its own chunk and then folds the
/// survivors in here; ties are broken by input position, so the result — values
/// *and* order — is what a serial run would have produced.
const DistinctMerge = struct {
    seen: op.Aggregate.GroupMap(),
    rows: std.array_list.Managed(DistinctRow),
    key_idx: []const usize,
    arena: std.mem.Allocator,
    mtx: std.Thread.Mutex = .{},

    fn init(arena: std.mem.Allocator, key_idx: []const usize) DistinctMerge {
        return .{
            .seen = op.Aggregate.GroupMap().init(arena),
            .rows = std.array_list.Managed(DistinctRow).init(arena),
            .key_idx = key_idx,
            .arena = arena,
        };
    }

    fn dupeRow(self: *DistinctMerge, b: Batch, r: usize) ![]Value {
        const vals = try self.arena.alloc(Value, b.columns.len);
        for (b.columns, vals) |*col, *v| v.* = try op.dupeValue(self.arena, col.getValue(r));
        return vals;
    }

    /// Folds one lane's already-deduped batch in. `ords` is `op.Distinct.ords`
    /// for that batch; `chunk` is the work item the lane read.
    fn absorb(self: *DistinctMerge, chunk: usize, b: Batch, ords: []const u64, probe: []Value) !void {
        self.mtx.lock();
        defer self.mtx.unlock();
        var r: usize = 0;
        while (r < b.len) : (r += 1) {
            for (self.key_idx, 0..) |ci, j| probe[j] = b.columns[ci].getValue(r);
            const gop = try self.seen.getOrPut(probe);
            if (gop.found_existing) {
                const slot = &self.rows.items[gop.value_ptr.*];
                const cand = DistinctRow{ .chunk = chunk, .ord = ords[r], .vals = &.{} };
                if (!DistinctRow.before({}, cand, slot.*)) continue;
                slot.chunk = chunk;
                slot.ord = ords[r];
                slot.vals = try self.dupeRow(b, r);
                continue;
            }
            const kv = try self.arena.alloc(Value, self.key_idx.len);
            for (self.key_idx, 0..) |ci, j| kv[j] = try op.dupeValue(self.arena, b.columns[ci].getValue(r));
            gop.key_ptr.* = kv;
            gop.value_ptr.* = self.rows.items.len;
            try self.rows.append(.{ .chunk = chunk, .ord = ords[r], .vals = try self.dupeRow(b, r) });
        }
    }

    /// Input order, then one batch. Sorting here is what makes `-j 8` agree with
    /// `-j 1` byte for byte instead of only row-count for row-count.
    fn finish(self: *DistinctMerge, row_schema: *const types.Schema) !Batch {
        std.mem.sort(DistinctRow, self.rows.items, {}, DistinctRow.before);
        const cols = try self.arena.alloc(column.Column, row_schema.fields.len);
        for (row_schema.fields, cols, 0..) |f, *c, ci| {
            var bd = column.Builder.init(self.arena, f.ty);
            for (self.rows.items) |row| try bd.append(row.vals[ci]);
            c.* = try bd.finish();
        }
        return .{ .schema = row_schema, .columns = cols, .len = self.rows.items.len };
    }
};

const DistinctCtx = struct {
    split: LaneSplit,
    row_schema: *const types.Schema,
    prefix: []const ast.Stage,
    params: *std.StringHashMap(*const ast.Expr),
    local_keys: ?[]const usize,
    key_idx: []const usize,
    queue: WorkQueue,
    merge: *DistinctMerge,
    rows_read: *std.atomic.Value(u64),
};

const distinctWorker = dispatchWorker(DistinctCtx, distinctWorkOne);

/// One work item — a CSV chunk or a parquet row group. Deduping per item rather
/// than per lane costs a little extra traffic through the merge mutex, but it is
/// what makes the item index a usable position: item `i` precedes item `i+1` in
/// the file, so the merge keeps the same row a serial run would.
fn distinctWorkOne(ctx: *DistinctCtx, i: usize) !void {
    var wgpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = false }){};
    defer _ = wgpa.deinit();
    var warena = std.heap.ArenaAllocator.init(wgpa.allocator());
    defer warena.deinit();
    var batch_arena = std.heap.ArenaAllocator.init(wgpa.allocator());
    defer batch_arena.deinit();

    const src_schema = ctx.split.schema();
    const inner = (try laneRowSource(ctx.split.rows(i, ctx.queue.nitems), warena.allocator())) orelse return;
    defer inner.close();
    var cs = obs.CountingSource{ .inner = inner, .count = ctx.rows_read };
    var scan = op.Scan{ .src = cs.source() };
    const child = try buildMapChain(warena.allocator(), ctx.params, ctx.prefix, &scan, src_schema);
    var d = op.Distinct{ .child = child, .in_schema = ctx.row_schema, .keys = ctx.local_keys, .state = warena.allocator(), .gpa = wgpa.allocator(), .track_ords = true };

    const probe = try warena.allocator().alloc(Value, ctx.key_idx.len);
    while (try d.next(batch_arena.allocator())) |b| {
        try ctx.merge.absorb(i, b, d.ords, probe);
        _ = batch_arena.reset(.retain_capacity);
    }
}

/// A single already-positioned parquet reader as a `Source`.
const ReaderSource = struct {
    r: *pqdecode.Reader,
    schema_: *const types.Schema,

    fn schemaFn(ptr: *anyopaque) types.Schema {
        const self: *ReaderSource = @ptrCast(@alignCast(ptr));
        return self.schema_.*;
    }
    fn nextFn(ptr: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
        const self: *ReaderSource = @ptrCast(@alignCast(ptr));
        return self.r.next(arena);
    }
    /// Owns the reader: `laneRowSource` hands this out in place of a reader the
    /// caller held in a local, so closing the source is what releases it.
    fn closeFn(ptr: *anyopaque) void {
        const self: *ReaderSource = @ptrCast(@alignCast(ptr));
        self.r.close();
    }
    const vtable = driver.Source.VTable{ .schema = schemaFn, .next = nextFn, .close = closeFn };
};

/// Parallel distinct: each worker dedups its own work item locally, then folds
/// the survivors into a shared merge that keeps the row which came first in the
/// input, so the result matches `-j 1` exactly. Returns false to fall back.
///
/// Source-independent — `split` says how the input divides; see `LaneSplit`.
fn runParallelDistinct(
    env: *Env,
    split: LaneSplit,
    prefix: []const ast.Stage,
    dist: ast.Distinct,
    tail: []const ast.Stage,
    w: ast.Write,
    opts: RunOptions,
    stats: *Stats,
    lanes_used: *usize,
) anyerror!bool {
    const arena = env.arena;
    const row_schema = try schemaPtr(arena, try mapChainSchema(env, prefix, split.schema().*));

    var local_keys: ?[]const usize = null;
    var key_idx: []const usize = undefined;
    if (dist.on) |fields| {
        var ad = analyze.Diag{};
        const ks = analyze.fieldIndices(arena, row_schema.*, fields, &ad) catch |e| return aErr(env, &ad, e);
        local_keys = ks;
        key_idx = ks;
    } else {
        const all = try arena.alloc(usize, row_schema.fields.len);
        for (all, 0..) |*x, j| x.* = j;
        key_idx = all;
    }

    env.src_name = split.label();
    env.sink_name = sinkLabel(env, w);

    const nthreads = @max(@as(usize, 1), opts.threads);
    const nitems = split.count(nthreads);
    var merge = DistinctMerge.init(arena, key_idx);
    var ctx = DistinctCtx{
        .split = split,
        .row_schema = row_schema,
        .prefix = prefix,
        .params = env.params_expr,
        .local_keys = local_keys,
        .key_idx = key_idx,
        .queue = .{ .nitems = nitems },
        .merge = &merge,
        .rows_read = env.rows_read,
    };

    const lanes = try parallel.spawnJoin(arena, nthreads, distinctWorker, &ctx);
    lanes_used.* = @max(lanes_used.*, lanes);
    env.log.log(.debug, "parallel {s} distinct: {d} {s} over {d} lanes", .{ split.label(), nitems, split.unitName(), lanes });
    if (ctx.queue.first_err) |e| return e;

    const merged = try merge.finish(row_schema);

    const wr = try resolveUpsertKeys(env, w);
    const snk = try openSink(env, wr, row_schema.*);
    var snk_open = true;
    errdefer if (snk_open) snk.abort();
    try writeTail(env, snk, merged, row_schema.*, tail, stats);
    snk_open = false;
    try snk.close();
    return true;
}

fn runParallelParquetDistinct(env: *Env, rd: ast.Read, pipeline: []const ast.Stage, prefix: []const ast.Stage, dist: ast.Distinct, tail: []const ast.Stage, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    const split = (try parquetSplit(env, rd, pipeline[1..], w, opts)) orelse return false;
    return runParallelDistinct(env, split, prefix, dist, tail, w, opts, stats, lanes_used);
}

fn runParallelCsvDistinct(env: *Env, rd: ast.Read, prefix: []const ast.Stage, dist: ast.Distinct, tail: []const ast.Stage, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    if (std.mem.eql(u8, w.connector, "stdout")) return false;
    const mapped = (try csvSplitFile(env, rd, w)) orelse return false;
    defer mapped.close();
    return runParallelDistinct(env, .{ .csv = .{ .mapped = mapped, .schema = &mapped.schema } }, prefix, dist, tail, w, opts, stats, lanes_used);
}
