//! Parallel lane drivers: the shape classifiers that decide whether a pipeline
//! can fan out, and the per-shape drivers (map, aggregate, top-N, distinct,
//! SQL split, parquet/CSV morsels) plus their merge machinery.
//!
//! Shapes. `classifyLaneShape` picks one `LaneShape` per pipeline and each source's
//! runner switches exhaustively over it, so a new shape does not compile until both
//! parquet and CSV decide what to do with it; a source that cannot fan a shape out
//! returns false and falls back to the serial driver explicitly. Below the shape,
//! `LaneRows` is the only difference between the parquet and CSV copies of a lane
//! body (row-group morsels off a shared queue vs one newline-aligned byte range),
//! and `LaneSplit` names how the input divides so a shape has one implementation.
//! Only right and full joins are kept off the lanes: they must emit unmatched build
//! rows, and each lane tracking its own matches emitted them once per lane (a
//! `RIGHT JOIN` under `COUNT(*)` returned 160 instead of 10 at `-j 16`). A join's
//! build side is materialized once into a read-only index before any lane exists,
//! and its post-join stages are prevalidated so a lane rebuild cannot fail; each
//! lane still builds its own `op.Join` for its mutable stats and scratch.
//!
//! Determinism. Results do not depend on thread timing. Aggregates fold into one
//! `AggSlot` per work item (or one fixed arithmetic slice of parquet morsels per
//! lane, `MorselSource.fixed`) and are combined in item order, because float
//! addition is not associative and merging as lanes finished made a float SUM
//! differ between runs; a different `-j` still cuts the input differently, so only
//! a DECIMAL cast is identical everywhere. An ungrouped aggregate folds straight
//! into partition 0 in lane order (it has no key to hash, and the radix merge once
//! dropped every lane, so COUNT(*) returned 0). Distinct dedups per positional item
//! and breaks ties by input position, keeping the row a serial run keeps. A shared
//! sink's map output goes through `OrderedOut`: lanes format units outside any
//! lock, and whichever lane completes the next unit due writes it and the finished
//! units behind it, at most `window` units ahead, so output is in file order at any
//! `-j`. Writing under the lock had stalled every lane (parquet to CSV ran 50%
//! slower), and reusing written units' arenas avoids faulting in fresh pages.
//!
//! Merging. Above `agg_combine_parallel_min` partial groups the combine is radix
//! partitioned by key hash (`pq_parts`, the fold's own partition count), each
//! partition owned by one task with no lock: a single O(partials) pass over a
//! unique-key GROUP BY was slower at `-j 16` than serial. The largest source is
//! merged into in place and the others freed as folded, since copying all of them
//! doubled memory. Lane partition arenas use the thread-safe gpa, not per-slot or
//! page allocators (64 page-backed arenas per lane cost 12% at -j 8). Merged groups
//! under a tail that only filters and projects are written a partition at a time
//! across lanes; ORDER BY + LIMIT in the tail fuse into a per-partition top-N (a
//! full sort of the merged groups was 3x slower than serial).
//!
//! Descent. `wholeAggStages` and `topNStages` rewrite a SQL read into one grouped
//! or capped QUERY-form read, dropping hints (a hinted `@[where]` would be reapplied
//! to the wrong columns, a split has nothing left to split), and the caller rebuilds
//! the pipeline through the ordinary serial path. The SQL split paths
//! (`runParallelSqlAgg`, `runParallelSqlMapJoin`) run only against a live DB; the
//! local tests cover them through the shared CSV/parquet machinery they reuse.
//! A SQL map+join lane's `SplitSource` rolls to the next key range when one runs
//! dry, so the lane builds its chain and opens its sink once.
//!
//! This file classifies a pipeline's shape and holds the shared work queue; each
//! shape runs in lanes/: `map.zig`, `agg.zig`, `topn.zig` and `distinct.zig`, with
//! `join.zig` for the joins a lane probes and `sources.zig` for how a read divides.

const std = @import("std");
const ast = @import("../lang/ast.zig");
const types = @import("../lang/types.zig");
const op = @import("../exec/op.zig");
const Batch = @import("../exec/batch.zig").Batch;
const column = @import("../exec/column.zig");
const eval = @import("../exec/eval.zig");
const csv = @import("../format/csv.zig");
const pqdecode = @import("../format/parquet/read.zig");
const folder = @import("../connect/folder.zig");
const sftp = @import("../store/sftp.zig");
const smb = @import("../store/smb.zig");
const driver = @import("../connect/driver.zig");
const sql = @import("../db/sql.zig");
const wrapProjected = @import("../connect/split.zig").wrapProjected;
const parallel = @import("parallel.zig");
const analyze = @import("analyze.zig");
const pushdown = @import("pushdown.zig");
const obs = @import("obs.zig");
const Value = @import("../exec/value.zig").Value;

const Env = @import("env.zig").Env;
const planErr = @import("env.zig").planErr;
const planErrT = @import("env.zig").planErrT;
const RunOptions = @import("env.zig").RunOptions;
const schemaPtr = @import("env.zig").schemaPtr;
const Stats = @import("env.zig").Stats;

const connect_mod = @import("connect.zig");
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
const prepareJoinSide = @import("plan.zig").prepareJoinSide;
const buildStage = @import("plan.zig").buildStage;
const buildTopN = @import("plan.zig").buildTopN;
const filterBounds = @import("plan.zig").filterBounds;
const joinBuildCap = @import("plan.zig").joinBuildCap;
const mapChainSchema = @import("plan.zig").mapChainSchema;
const projectedColumns = @import("plan.zig").projectedColumns;
const tailSchema = @import("plan.zig").tailSchema;

const LaneShape = union(enum) {
    agg: AggShape,
    agg_join: AggJoinShape,
    distinct: DistinctShape,
    top_n: TopNShape,
    map: []const ast.Stage,
    map_join: MapJoinShape,
};

/// Order matters: `classifyAggPipeline` rejects a join, so the join-carrying variant
/// is tried after it, and the map shapes last.
pub fn classifyLaneShape(stages: []const ast.Stage) ?LaneShape {
    if (classifyAggPipeline(stages)) |x| return .{ .agg = x };
    if (classifyAggJoinPipeline(stages)) |x| return .{ .agg_join = x };
    if (classifyDistinctPipeline(stages)) |x| return .{ .distinct = x };
    if (classifyTopNPipeline(stages)) |x| return .{ .top_n = x };
    if (classifyMapPipeline(stages)) |x| return .{ .map = x };
    if (classifyMapJoinPipeline(stages)) |x| return .{ .map_join = x };
    return null;
}

/// Preconditions every lane path shares. A hinted read is left to the paths that honour
/// the hint: any hint used to keep a read serial while EXPLAIN called it morsel-parallel.
pub fn laneEligible(stages: []const ast.Stage, opts: RunOptions) bool {
    return opts.threads > 1 and stages.len >= 2 and
        stages[0].node == .read and analyze.laneHints(stages[0]);
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
    const pipe = stages[0 .. stages.len - 1];
    const push = switch (shape) {
        .map => |x| pipe[1..][0..x.len],
        .map_join => |x| pipe[1..][0..x.prefix.len],
        else => pipe[1..],
    };
    var rd = stages[0].node.read;
    if (try projectedColumns(env, push)) |cols| rd.cols = cols;
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

/// Recognize `read … | (filter|select)* | aggregate | tail | write`. A `select` or
/// `filter` after the aggregate (a reordering projection, HAVING) is applied by `writeTail`;
/// refusing them sent such queries serial (TPC-H q01's shape: 379ms vs 88ms).
pub fn classifyAggPipeline(stages: []const ast.Stage) ?AggShape {
    const middle = stages[1 .. stages.len - 1];
    var ai: ?usize = null;
    for (middle, 0..) |st, i| switch (st.node) {
        .filter, .select => {},
        .aggregate => {
            if (ai != null) return null;
            ai = i;
        },
        .sort, .limit, .distinct, .window => if (ai == null) return null,
        else => return null,
    };
    const a = ai orelse return null;
    return .{ .prefix = middle[0..a], .ag = middle[a].node.aggregate, .tail = middle[a + 1 ..] };
}

/// Recognize the shape a whole aggregate can descend into one grouped source query:
/// filters only before it (a `select` renames the keys) and no hint on the read; any
/// tail. `pushdown.planWholeAgg` decides whether the aggregate itself renders.
pub fn classifyWholeAgg(stages: []const ast.Stage) ?AggShape {
    if (stages.len < 3 or stages[0].node != .read) return null;
    if (stages[0].hints.len != 0) return null;
    const middle = stages[1 .. stages.len - 1];
    for (middle, 0..) |st, i| switch (st.node) {
        .filter => {},
        .aggregate => |ag| return .{ .prefix = middle[0..i], .ag = ag, .tail = middle[i + 1 ..] },
        else => return null,
    };
    return null;
}

/// Descend the whole aggregate into the source: a QUERY-form read of the grouped SQL
/// (cast to the engine's own output schema), the untouched tail, then the write. Null
/// means not eligible, and the caller keeps the pipeline it built.
pub fn wholeAggStages(env: *Env, stages: []const ast.Stage, shape: AggShape, src_base: usize, why: *[]const u8) !?[]const ast.Stage {
    const arena = env.arena;
    const desc = env.sql_desc orelse return null;
    if (stages[0].node != .read) return null;
    const rd = stages[0].node.read;
    if (rd.form != .table and rd.form != .query) {
        why.* = "the read is not a table or query";
        return null;
    }
    for (shape.prefix) |st| if (st.node != .filter) {
        why.* = "a stage other than WHERE sits between the read and the aggregate";
        return null;
    };

    const src_schema = try dupeSchema(arena, env.sources.items[src_base].schema());

    var ad = analyze.Diag{};
    const apl = analyze.aggregatePlan(arena, src_schema, shape.ag, env.params_expr, &ad) catch {
        why.* = ad.msg;
        return null;
    };

    const facts = try connect_mod.factsIfWanted(env, rd, desc.dialect, shape.prefix, src_schema, true, .exact);
    const wa = (try pushdown.planWholeAggWhy(arena, desc.dialect, desc.base_sql, src_schema, shape.prefix, shape.ag, apl.schema, facts, why)) orelse return null;

    const out = try arena.alloc(ast.Stage, shape.tail.len + 2);
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

/// Descend a `LIMIT` (with its `ORDER BY`) into the SQL read when `pushdown.planTopN`
/// allows; the engine still sorts and cuts what arrives. Null leaves the pipeline as
/// built, with `why` set when there was a limit to push.
pub fn topNStages(env: *Env, stages: []const ast.Stage, src_base: usize, why: *[]const u8) !?[]const ast.Stage {
    const arena = env.arena;
    const desc = env.sql_desc orelse return null;
    if (stages[0].node != .read) return null;
    const rd = stages[0].node.read;
    if (rd.form != .table and rd.form != .query) return null;
    const t = (try pushdown.classifyTopN(arena, stages[0 .. stages.len - 1], why)) orelse return null;
    const src_schema = try dupeSchema(arena, env.sources.items[src_base].schema());
    const facts = try connect_mod.factsIfWanted(env, rd, desc.dialect, stages[1 .. stages.len - 1], src_schema, true, .exact);
    const q = (try pushdown.planTopN(arena, desc.dialect, desc.base_sql, src_schema, stages[0 .. stages.len - 1], t, facts, why)) orelse return null;

    const out = try arena.dupe(ast.Stage, stages);
    out[0] = .{
        .node = .{ .read = .{ .connector = rd.connector, .form = .{ .query = q } } },
        .hints = &.{},
        .pos = stages[0].pos,
    };
    env.log.log(.debug, "top-N pushdown: {s} sent to {s}", .{ try t.describe(arena), @tagName(desc.kind) });
    return out;
}

pub const WorkQueue = struct {
    nitems: usize,
    next: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    err_mtx: std.Thread.Mutex = .{},
    first_err: ?anyerror = null,
    failed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    note_buf: [480]u8 = undefined,
    note_len: usize = 0,

    pub fn fail(q: *WorkQueue, e: anyerror) void {
        q.err_mtx.lock();
        if (q.first_err == null) {
            q.first_err = e;
            const name = @errorName(e);
            if (eval.takeFailure(e)) |why| {
                q.note_len = @min(why.len, q.note_buf.len);
                @memcpy(q.note_buf[0..q.note_len], why[0..q.note_len]);
            } else if ((std.mem.startsWith(u8, name, "Sftp") or std.mem.startsWith(u8, name, "Ssh")) and sftp.lastError().len > 0) {
                q.note_len = (std.fmt.bufPrint(&q.note_buf, "sftp: {s}: {s}", .{ name, sftp.lastError() }) catch q.note_buf[0..0]).len;
            } else if (std.mem.startsWith(u8, name, "Smb") and smb.lastError().len > 0) {
                q.note_len = (std.fmt.bufPrint(&q.note_buf, "smb: {s}: {s}", .{ name, smb.lastError() }) catch q.note_buf[0..0]).len;
            }
        }
        q.err_mtx.unlock();
        q.failed.store(true, .seq_cst);
    }

    pub fn failure(q: *WorkQueue) ?anyerror {
        const e = q.first_err orelse return null;
        return if (q.note_len > 0) eval.explain(e, q.note_buf[0..q.note_len]) else e;
    }
};

/// The worker loop shared by the parallel CSV/SQL paths: steal the next item off
/// `ctx.queue`, run `workOne(ctx, i)`, and latch the first error, which stops the others.
pub fn dispatchWorker(comptime Ctx: type, comptime workOne: anytype) fn (*Ctx, usize) void {
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

pub const agg_combine_parallel_min = @import("lanes/agg.zig").agg_combine_parallel_min;
const runParallelParquetMap = @import("lanes/map.zig").runParallelParquetMap;
const runParallelParquetMapJoin = @import("lanes/map.zig").runParallelParquetMapJoin;
const AggJoinShape = @import("lanes/agg.zig").AggJoinShape;
const classifyAggJoinPipeline = @import("lanes/agg.zig").classifyAggJoinPipeline;
const runParallelParquetAgg = @import("lanes/agg.zig").runParallelParquetAgg;
const runParallelParquetAggJoin = @import("lanes/agg.zig").runParallelParquetAggJoin;
const runParallelCsvAgg = @import("lanes/agg.zig").runParallelCsvAgg;
const runParallelCsvAggJoin = @import("lanes/agg.zig").runParallelCsvAggJoin;
pub const runParallelSqlAgg = @import("lanes/agg.zig").runParallelSqlAgg;
const classifyMapPipeline = @import("lanes/map.zig").classifyMapPipeline;
const MapJoinShape = @import("lanes/map.zig").MapJoinShape;
pub const classifyMapJoinPipeline = @import("lanes/map.zig").classifyMapJoinPipeline;
pub const joinKindLaneSafe = @import("lanes/map.zig").joinKindLaneSafe;
pub const runParallelSqlMapJoin = @import("lanes/map.zig").runParallelSqlMapJoin;
const runParallelCsvMap = @import("lanes/map.zig").runParallelCsvMap;
const runParallelCsvMapJoin = @import("lanes/map.zig").runParallelCsvMapJoin;
const TopNShape = @import("lanes/topn.zig").TopNShape;
const classifyTopNPipeline = @import("lanes/topn.zig").classifyTopNPipeline;
const runParallelParquetTopN = @import("lanes/topn.zig").runParallelParquetTopN;
const runParallelCsvTopN = @import("lanes/topn.zig").runParallelCsvTopN;
const DistinctShape = @import("lanes/distinct.zig").DistinctShape;
const classifyDistinctPipeline = @import("lanes/distinct.zig").classifyDistinctPipeline;
const runParallelParquetDistinct = @import("lanes/distinct.zig").runParallelParquetDistinct;
const runParallelCsvDistinct = @import("lanes/distinct.zig").runParallelCsvDistinct;

test "classifyWholeAgg: filters-only prefix, unrestricted tail, no hints" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const p = ast.Pos{ .line = 0, .col = 0 };
    const pred = try a.create(ast.Expr);
    pred.* = .{ .bool_lit = true };
    const items = try a.alloc(ast.SelectItem, 1);
    items[0] = .star;

    const rd = ast.Stage{ .node = .{ .read = .{ .connector = "db", .form = .{ .table = .{ .parts = &.{"t"} } } } }, .hints = &.{}, .pos = p };
    const flt = ast.Stage{ .node = .{ .filter = pred }, .hints = &.{}, .pos = p };
    const sel = ast.Stage{ .node = .{ .select = items }, .hints = &.{}, .pos = p };
    const agg = ast.Stage{ .node = .{ .aggregate = .{ .aggs = &.{}, .by = &.{} } }, .hints = &.{}, .pos = p };
    const wrt = ast.Stage{ .node = .{ .write = .{ .connector = "csv", .form = null, .target = "o.csv", .mode = .default } }, .hints = &.{}, .pos = p };

    const simple = [_]ast.Stage{ rd, flt, agg, wrt };
    const s1 = classifyWholeAgg(&simple).?;
    try std.testing.expectEqual(@as(usize, 1), s1.prefix.len);
    try std.testing.expectEqual(@as(usize, 0), s1.tail.len);

    const having = [_]ast.Stage{ rd, agg, flt, flt, wrt };
    const s2 = classifyWholeAgg(&having).?;
    try std.testing.expectEqual(@as(usize, 0), s2.prefix.len);
    try std.testing.expectEqual(@as(usize, 2), s2.tail.len);
    const lane = classifyAggPipeline(&having).?;
    try std.testing.expectEqual(@as(usize, 0), lane.prefix.len);
    try std.testing.expectEqual(@as(usize, 2), lane.tail.len);

    const selected = [_]ast.Stage{ rd, sel, agg, wrt };
    try std.testing.expect(classifyWholeAgg(&selected) == null);

    const no_agg = [_]ast.Stage{ rd, flt, wrt };
    try std.testing.expect(classifyWholeAgg(&no_agg) == null);

    const hints = try a.alloc(ast.Hint, 1);
    hints[0] = .{ .key = "split", .value = .{ .ident = "id" }, .pos = p };
    var hinted_rd = rd;
    hinted_rd.hints = hints;
    const hinted = [_]ast.Stage{ hinted_rd, flt, agg, wrt };
    try std.testing.expect(classifyWholeAgg(&hinted) == null);
}

test {
    _ = @import("lanes/agg.zig");
    _ = @import("lanes/distinct.zig");
    _ = @import("lanes/join.zig");
    _ = @import("lanes/map.zig");
    _ = @import("lanes/sources.zig");
    _ = @import("lanes/topn.zig");
    _ = @import("lanes/testing_util.zig");
}
