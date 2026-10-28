//! Parallel lane drivers: the shape classifiers that decide whether a pipeline
//! can fan out, and the per-shape drivers (map, aggregate, top-N, distinct,
//! SQL split) over a source cut into morsels (`lanes/morsels.zig`).
//!
//! Shapes. `classifyLaneShape` picks one `LaneShape` per pipeline and `runShape`
//! switches exhaustively over it, so a new shape does not compile until it decides
//! what to do with every source; a source that cannot fan a shape out returns false
//! and falls back to the serial driver explicitly. Every shape reads `Morsels`:
//! Parquet row groups or files, CSV byte ranges, SQL key ranges, one interface. An
//! aggregate is the first breaker run as a `ParallelSink` (`.sink`): map stages and
//! join probes per morsel, then the sink's combine and finalize, so its result is
//! the same at every `-j` of 2 or more; one lane runs the serial aggregate.
//! Right and full joins must emit unmatched build rows, and each lane tracking its
//! own matches emitted them once per lane (a `RIGHT JOIN` under `COUNT(*)` returned
//! 160 instead of 10 at `-j 16`). On the map+join paths every lane marks one
//! shared match bitset and the driver drains the unmatched rows once, after the
//! lanes, through the join's suffix; under an aggregate they stay serial. A join's
//! build side is materialized once into a read-only index before any lane exists,
//! and the join and the stages after it are resolved once into records; each lane builds
//! its own `op.Join` and stage operators from them, for its mutable stats and scratch.
//!
//! Determinism. Results do not depend on thread timing, and an aggregate's not on
//! a `-j` of 2 or more: it folds one partial per morsel and merges them in morsel order
//! (`lanes/agg.zig`), because float addition is not associative and merging as lanes
//! finished made a float SUM differ between runs. Distinct dedups per positional
//! morsel and breaks ties by input position, keeping the row a serial run keeps. A shared
//! sink's map output goes through `OrderedOut`: lanes format units outside any
//! lock, and whichever lane completes the next unit due writes it and the finished
//! units behind it, at most `window` units ahead, so output is in file order at any
//! `-j`. Writing under the lock had stalled every lane (parquet to CSV ran 50%
//! slower), and reusing written units' arenas avoids faulting in fresh pages.
//!
//! Merging. Partial groups are combined radix partitioned by key hash (`pq_parts`,
//! the fold's own partition count), each partition owned by one task with no lock:
//! a single O(partials) pass over a unique-key GROUP BY was slower at `-j 16` than
//! serial. Partials are freed as folded, since keeping them doubled memory. Lane
//! partition arenas use the thread-safe gpa, not per-slot or page allocators (64
//! page-backed arenas per lane cost 12% at -j 8). Merged groups under a tail that
//! only filters and projects are written a partition at a time across lanes; ORDER
//! BY + LIMIT in the tail fuse into a per-partition top-N (a full sort of the merged
//! groups was 3x slower than serial).
//!
//! Descent. `wholeAggStages` and `topNStages` rewrite a SQL read into one grouped
//! or capped QUERY-form read, dropping hints (a hinted `@[where]` would be reapplied
//! to the wrong columns, a split has nothing left to split), and the caller rebuilds
//! the pipeline through the ordinary serial path. The SQL split paths
//! (`runParallelSqlAgg`, `runParallelSqlMapJoin`) run the file paths' drivers over
//! key-range morsels, each range on its own connection.
//!
//! This file classifies a pipeline's shape; each shape runs in lanes/: `map.zig`,
//! `agg.zig`, `topn.zig` and `distinct.zig`, with `join.zig` for the joins a lane
//! probes, `sources.zig` for how a file divides and `morsels.zig` for the morsels,
//! their workers and the parallel sink they drive.

const std = @import("std");
const ast = @import("../lang/ast.zig");
const analyze = @import("analyze.zig");
const pushdown = @import("pushdown.zig");
const sql_read = @import("connect/sql_read.zig");

const Env = @import("env.zig").Env;
const RunOptions = @import("env.zig").RunOptions;
const Stats = @import("env.zig").Stats;

const connect_mod = @import("connect.zig");
const dupeSchema = @import("connect.zig").dupeSchema;

const projectedColumns = @import("plan.zig").projectedColumns;

pub const LaneShape = union(enum) {
    sink: SinkShape,
    distinct: DistinctShape,
    top_n: TopNShape,
    map: []const ast.Stage,
    map_join: MapJoinShape,
};

pub fn classifyLaneShape(stages: []const ast.Stage) ?LaneShape {
    if (classifySinkPipeline(stages)) |x| return .{ .sink = x };
    if (classifyDistinctPipeline(stages)) |x| return .{ .distinct = x };
    if (classifyTopNPipeline(stages)) |x| return .{ .top_n = x };
    if (classifyMapPipeline(stages)) |x| return .{ .map = x };
    if (classifyMapJoinPipeline(stages)) |x| return .{ .map_join = x };
    return null;
}

pub fn runParquetLane(env: *Env, stages: []const ast.Stage, shape: LaneShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    const split = (try parquetSplit(env, stages[0].node.read, stages[0].hints, pushStages(stages, shape), w, opts)) orelse return false;
    return runShape(env, split, shape, w, opts, stats, lanes_used);
}

pub fn runCsvLane(env: *Env, stages: []const ast.Stage, shape: LaneShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    var rd = stages[0].node.read;
    if (try projectedColumns(env, pushStages(stages, shape))) |cols| rd.cols = cols;
    const mapped = (try csvSplitFile(env, rd, stages[0].hints, w)) orelse return false;
    defer mapped.close();
    return runShape(env, .ofCsv(mapped, &mapped.schema, opts.morsel_bytes), shape, w, opts, stats, lanes_used);
}

/// The stages a read may push down: a map path stops before its join, whose names are the join's.
fn pushStages(stages: []const ast.Stage, shape: LaneShape) []const ast.Stage {
    const middle = stages[1 .. stages.len - 1];
    return switch (shape) {
        .map => |x| middle[0..x.len],
        .map_join => |x| middle[0..x.prefix.len],
        else => middle,
    };
}

pub fn shapeLaneSafe(shape: LaneShape) bool {
    return switch (shape) {
        .map_join => |x| mapJoinKindLaneSafe(x.join.kind),
        else => true,
    };
}

fn runShape(env: *Env, split: Morsels, shape: LaneShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    return switch (shape) {
        .sink => |x| runParallelAgg(env, &split, x, w, opts, stats, lanes_used),
        .distinct => |x| runParallelDistinct(env, &split, x.prefix, x.dist, x.tail, w, opts, stats, lanes_used),
        .top_n => |x| runParallelTopN(env, &split, x.prefix, x.srt, x.lim, w, opts, stats, lanes_used),
        .map => |x| runParallelMap(env, split, x, null, w, opts, stats, lanes_used),
        .map_join => |x| runParallelMap(env, split, x.prefix, x, w, opts, stats, lanes_used),
    };
}

/// A pipeline whose breaker is a parallel sink, the aggregate: map stages, then
/// lane-safe keyed joins, the aggregate, and a tail its finalize writes through.
pub const SinkShape = struct { prefix: []const ast.Stage, joins: []const ast.Stage, agg: ast.Stage, tail: []const ast.Stage };

/// Recognize `read … | (filter|select)* | join* | aggregate | tail | write`. A
/// `select` or `filter` after the aggregate (a reordering projection, HAVING) is
/// applied by the tail; refusing them sent such queries serial (TPC-H q01's shape:
/// 379ms vs 88ms). A distinct or window in the tail is taken only without joins.
pub fn classifySinkPipeline(stages: []const ast.Stage) ?SinkShape {
    const middle = stages[1 .. stages.len - 1];
    var ji: ?usize = null;
    var ai: ?usize = null;
    for (middle, 0..) |st, i| switch (st.node) {
        .filter, .select => {},
        .join => |j| {
            if (ai != null or !joinKindLaneSafe(j.kind) or j.keyless()) return null;
            if (ji == null) ji = i;
        },
        .aggregate => {
            if (ai != null) return null;
            ai = i;
        },
        .sort, .limit => if (ai == null) return null,
        .distinct, .window => if (ai == null or ji != null) return null,
        else => return null,
    };
    const a = ai orelse return null;
    const j = ji orelse a;
    return .{ .prefix = middle[0..j], .joins = middle[j..a], .agg = middle[a], .tail = middle[a + 1 ..] };
}

/// Recognize the shape a whole aggregate can descend into one grouped source query:
/// filters only before it (a `select` renames the keys), no hint on the read, and
/// keys and aggregates `pushdown.wholeAggShape` takes; any tail.
pub fn classifyWholeAgg(stages: []const ast.Stage) ?SinkShape {
    if (stages.len < 3 or stages[0].node != .read) return null;
    if (stages[0].hints.len != 0) return null;
    const middle = stages[1 .. stages.len - 1];
    for (middle, 0..) |st, i| switch (st.node) {
        .filter => {},
        .aggregate => {
            if (!pushdown.wholeAggShape(st.node.aggregate)) return null;
            return .{ .prefix = middle[0..i], .joins = &.{}, .agg = st, .tail = middle[i + 1 ..] };
        },
        else => return null,
    };
    return null;
}

/// Descend the whole aggregate into the source: a QUERY-form read of the grouped SQL
/// (cast to the engine's own output schema), the untouched tail, then the write. Null
/// means not eligible, and the caller keeps the pipeline it built.
pub fn wholeAggStages(env: *Env, stages: []const ast.Stage, shape: SinkShape, src_base: usize, why: *[]const u8) !?[]const ast.Stage {
    const arena = env.arena;
    const desc = env.pipe.sql_desc orelse return null;
    if (stages[0].node != .read) return null;
    const rd = stages[0].node.read;
    if (!sql_read.pushableRead(desc.dialect, rd)) {
        why.* = "the read is not a table or a query the source can take as a subquery";
        return null;
    }
    for (shape.prefix) |st| if (st.node != .filter) {
        why.* = "a stage other than WHERE sits between the read and the aggregate";
        return null;
    };

    const src_schema = try dupeSchema(arena, env.sources.items[src_base].schema());

    var ad = analyze.Diag{};
    const apl = analyze.resolve(arena, src_schema, shape.agg, env.params_expr, &ad) catch {
        why.* = ad.msg;
        return null;
    };

    const facts = try connect_mod.factsIfWanted(env, rd, desc.dialect, shape.prefix, src_schema, true, .source);
    const wa = (try pushdown.planWholeAggWhy(arena, desc.dialect, desc.base_sql, src_schema, shape.prefix, shape.agg.node.aggregate, apl.out.*, facts, why)) orelse return null;

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
    const desc = env.pipe.sql_desc orelse return null;
    if (stages[0].node != .read) return null;
    const rd = stages[0].node.read;
    if (!sql_read.pushableRead(desc.dialect, rd)) return null;
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

pub const agg_combine_parallel_min = @import("lanes/agg.zig").agg_combine_parallel_min;
const runParallelAgg = @import("lanes/agg.zig").runParallelAgg;
pub const runParallelSqlAgg = @import("lanes/agg.zig").runParallelSqlAgg;
const classifyMapPipeline = @import("lanes/map.zig").classifyMapPipeline;
pub const MapJoinShape = @import("lanes/map.zig").MapJoinShape;
pub const classifyMapJoinPipeline = @import("lanes/map.zig").classifyMapJoinPipeline;
pub const joinKindLaneSafe = @import("lanes/map.zig").joinKindLaneSafe;
const mapJoinKindLaneSafe = @import("lanes/map.zig").mapJoinKindLaneSafe;
pub const runParallelSqlMapJoin = @import("lanes/map.zig").runParallelSqlMapJoin;
const runParallelMap = @import("lanes/map.zig").runParallelMap;
const TopNShape = @import("lanes/topn.zig").TopNShape;
const classifyTopNPipeline = @import("lanes/topn.zig").classifyTopNPipeline;
const runParallelTopN = @import("lanes/topn.zig").runParallelTopN;
const DistinctShape = @import("lanes/distinct.zig").DistinctShape;
const classifyDistinctPipeline = @import("lanes/distinct.zig").classifyDistinctPipeline;
const runParallelDistinct = @import("lanes/distinct.zig").runParallelDistinct;
const Morsels = @import("lanes/morsels.zig").Morsels;
const parquetSplit = @import("lanes/sources.zig").parquetSplit;
const csvSplitFile = @import("lanes/sources.zig").csvSplitFile;

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
    const by = try a.alloc(ast.QualName, 1);
    by[0] = .{ .parts = &.{"b"} };
    const agg = ast.Stage{ .node = .{ .aggregate = .{ .aggs = &.{}, .by = by } }, .hints = &.{}, .pos = p };
    const wrt = ast.Stage{ .node = .{ .write = .{ .connector = "csv", .form = null, .target = "o.csv", .mode = .default } }, .hints = &.{}, .pos = p };

    const simple = [_]ast.Stage{ rd, flt, agg, wrt };
    const s1 = classifyWholeAgg(&simple).?;
    try std.testing.expectEqual(@as(usize, 1), s1.prefix.len);
    try std.testing.expectEqual(@as(usize, 0), s1.tail.len);

    const having = [_]ast.Stage{ rd, agg, flt, flt, wrt };
    const s2 = classifyWholeAgg(&having).?;
    try std.testing.expectEqual(@as(usize, 0), s2.prefix.len);
    try std.testing.expectEqual(@as(usize, 2), s2.tail.len);
    const lane = classifySinkPipeline(&having).?;
    try std.testing.expectEqual(@as(usize, 0), lane.prefix.len);
    try std.testing.expectEqual(@as(usize, 2), lane.tail.len);

    const selected = [_]ast.Stage{ rd, sel, agg, wrt };
    try std.testing.expect(classifyWholeAgg(&selected) == null);

    const no_agg = [_]ast.Stage{ rd, flt, wrt };
    try std.testing.expect(classifyWholeAgg(&no_agg) == null);

    const arg = try a.create(ast.Expr);
    arg.* = .{ .field = .{ .parts = &.{"c"} } };
    const med = try a.alloc(ast.AggItem, 1);
    med[0] = .{ .name = "m", .func = .median, .arg = arg };
    const med_agg = ast.Stage{ .node = .{ .aggregate = .{ .aggs = med, .by = by } }, .hints = &.{}, .pos = p };
    const median = [_]ast.Stage{ rd, med_agg, wrt };
    try std.testing.expect(classifyWholeAgg(&median) == null);

    const hints = try a.alloc(ast.Hint, 1);
    hints[0] = .{ .key = "split", .value = .{ .ident = "id" }, .pos = p };
    var hinted_rd = rd;
    hinted_rd.hints = hints;
    const hinted = [_]ast.Stage{ hinted_rd, flt, agg, wrt };
    try std.testing.expect(classifyWholeAgg(&hinted) == null);
}

test "classifySinkPipeline takes a HAVING tail over joins, and a distinct tail only without them" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const testStages = @import("lanes/testing_util.zig").testStages;
    const having = try testStages(a,
        \\LOAD INTO 'o.csv' AS WITH d AS (SELECT id AS did FROM 'x.parquet')
        \\SELECT g, COUNT(*) AS n FROM 'x.parquet' JOIN d ON id = did GROUP BY g HAVING COUNT(*) > 1;
    );
    const shape = classifySinkPipeline(having) orelse return error.TestExpectedLanes;
    try std.testing.expect(shape.joins.len == 1 and shape.tail[0].node == .filter);
    const plain = try testStages(a, "LOAD INTO 'o.csv' AS SELECT DISTINCT COUNT(*) AS n FROM 'x.parquet' GROUP BY g;");
    try std.testing.expect(classifySinkPipeline(plain).?.joins.len == 0);
    const joined = try testStages(a,
        \\LOAD INTO 'o.csv' AS WITH d AS (SELECT id AS did FROM 'x.parquet')
        \\SELECT DISTINCT COUNT(*) AS n FROM 'x.parquet' JOIN d ON id = did GROUP BY g;
    );
    try std.testing.expect(classifySinkPipeline(joined) == null);
}

test {
    _ = @import("lanes/agg.zig");
    _ = @import("lanes/distinct.zig");
    _ = @import("lanes/join.zig");
    _ = @import("lanes/map.zig");
    _ = @import("lanes/morsels.zig");
    _ = @import("lanes/sources.zig");
    _ = @import("lanes/topn.zig");
    _ = @import("lanes/testing_util.zig");
}
