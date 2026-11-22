//! Running an output pipeline: bindings materialized ahead, the parallel paths tried
//! in order, else the serial driver, with its sink opened, written and closed.

const Batch = @import("../../exec/batch.zig").Batch;
const column = @import("../../exec/column.zig");
const lane_paths = @import("../lanes.zig");
const union_plan = @import("../plan/union.zig");
const Env = @import("../env.zig").Env;
const RunOptions = @import("../env.zig").RunOptions;
const SplitCtx = @import("../connect.zig").SplitCtx;
const Stats = @import("../env.zig").Stats;
const UnitPipeline = @import("../lanes/map.zig").UnitPipeline;
const aborting = @import("../env.zig").aborting;
const analyze = @import("../analyze.zig");
const ast = @import("../../lang/ast.zig");
const buildParallelSink = @import("../connect.zig").buildParallelSink;
const buildPipeline = @import("../plan.zig").buildPipeline;
const buildProject = @import("../plan.zig").buildProject;
const buildStage = @import("../plan.zig").buildStage;
const driver = @import("../../exec/driver.zig");
const descendLeadingWhere = @import("../plan.zig").descendLeadingWhere;
const buildPipelineLateLead = @import("../plan.zig").buildPipelineLateLead;
const chunk_copy = @import("chunk_copy.zig");
const dupeSchema = @import("../connect.zig").dupeSchema;
const env_mod = @import("../env.zig");
const exceptColumns = @import("../connect.zig").exceptColumns;
const factsIfWanted = @import("../connect.zig").factsIfWanted;
const forHintIdent = @import("../env.zig").forHintIdent;
const ftp = @import("../../store/ftp.zig");
const guardFileFormat = @import("../connect.zig").guardFileFormat;
const hasAggregate = @import("../run.zig").hasAggregate;
const hasFlagHint = @import("../env.zig").hasFlagHint;
const hasSort = @import("../run.zig").hasSort;
const inlineHeadBindings = @import("../plan.zig").inlineHeadBindings;
const stampedBinding = @import("../plan.zig").stampedBinding;
const keysPreferSerial = @import("../plan.zig").keysPreferSerial;
const localRead = @import("../connect/source.zig").localRead;
const mem_connector = @import("../connect.zig").mem_connector;
const moveLabel = @import("../run.zig").moveLabel;
const noteLoad = @import("../run.zig").noteLoad;
const notePlan = @import("explain.zig").notePlan;
const op = @import("../../exec/op.zig");
const openSink = @import("../connect.zig").openSink;
const opensPerLane = @import("../connect.zig").opensPerLane;
const openSource = @import("../connect.zig").openSource;
const openSplitSource = @import("../connect.zig").openSplitSource;
const parallel = @import("../parallel.zig");
const path = @import("../path.zig");
const aErr = @import("../env.zig").aErr;
const planErr = @import("../env.zig").planErr;
const planSplit = @import("../connect.zig").planSplit;
const printProfile = @import("explain.zig").printProfile;
const trace = @import("../../base/iotrace.zig");
const Profile = @import("../../exec/profile.zig").Profile;
const profile = @import("../../exec/profile.zig");
const projectSqlRead = @import("../connect.zig").projectSqlRead;
const projectedColumns = @import("../plan.zig").projectedColumns;
const pruneKnown = @import("../plan.zig").pruneKnown;
const pushdown = @import("../pushdown.zig");
const readReport = @import("../connect.zig").readReport;
const rebuildMapStages = @import("../plan.zig").rebuildMapStages;
const resolveUpsertKeys = @import("../connect.zig").resolveUpsertKeys;
const runCsvLane = @import("../lanes.zig").runCsvLane;
const runParallelSqlAgg = @import("../lanes.zig").runParallelSqlAgg;
const runParallelSqlMapJoin = @import("../lanes.zig").runParallelSqlMapJoin;
const runParquetLane = @import("../lanes.zig").runParquetLane;
const SinkGuard = @import("../../exec/guard.zig").SinkGuard;
const schemaPtr = @import("../env.zig").schemaPtr;
const sinkLabel = @import("../connect.zig").sinkLabel;
const std = @import("std");
const windowTopK = @import("../plan/union.zig").windowTopK;
const runWindowTopK = @import("../lanes/window.zig").runWindowTopK;
const Morsels = @import("../lanes/morsels.zig").Morsels;
const Reader = @import("../lanes/morsels.zig").Reader;
const MappedCsv = @import("../../format/csv.zig").MappedCsv;
const chainAt = @import("../plan.zig").chainAt;
const chainOps = @import("../plan.zig").chainOps;
const obs = @import("../../exec/obs.zig");
const parquetSplit = @import("../lanes/sources.zig").parquetSplit;
const csvSplitFile = @import("../lanes/sources.zig").csvSplitFile;
const passingRows = @import("../lanes/sources.zig").passingRows;
const connect = @import("../connect.zig");
const SinkShape = @import("../lanes.zig").SinkShape;
const MapJoinShape = @import("../lanes.zig").MapJoinShape;
const runParallelMap = @import("../lanes/map.zig").runParallelMap;
const runParallelAgg = @import("../lanes/agg.zig").runParallelAgg;
const rightStagesFor = @import("../plan.zig").rightStagesFor;
const joinorder = @import("../plan/joinorder.zig");
const estimate = @import("../plan/estimate.zig");
const offlineSchema = @import("../analyze/targets.zig").offlineSchema;
const msgfmt = @import("../../base/msgfmt.zig");
const synthReconcile = @import("../plan.zig").synthReconcile;
const targetLabel = @import("../run.zig").targetLabel;
const topNStages = @import("../lanes.zig").topNStages;
const types = @import("../../lang/types.zig");
const sql_read = @import("../connect/sql_read.zig");
const lake = @import("../../format/lake/table.zig");
const feed = @import("feed.zig");
const changes_pg = @import("../connect/changes_pg.zig");
const delta = @import("../../format/lake/delta.zig");
const delta_write = @import("../../format/lake/delta_write.zig");
const mkLit = @import("../env.zig").mkLit;
const Value = @import("../../exec/value.zig").Value;
const unionCanon = @import("../plan.zig").unionCanon;
const unionExceptNames = @import("../plan.zig").unionExceptNames;
const unionSpecs = @import("../plan.zig").unionSpecs;
const branchWhere = @import("../plan.zig").branchWhere;
const canonOf = @import("../plan.zig").canonOf;
const wholeAggStages = @import("../lanes.zig").wholeAggStages;

/// Run one output pipeline: split it into key-range lanes or stream it serially. A
/// fanned-out union reports per branch, so this outer call then counts nothing.
pub fn runOutput(env: *Env, out: ast.Pipeline, opts_in: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!void {
    errdefer env.diag.stamp(out.pos);
    const arena = env.arena;
    var stages = out.stages;
    if (stages.len == 0) return planErr(env.diag, "empty pipeline");
    const last = stages[stages.len - 1].node;
    if (last != .write) return planErr(env.diag, "a top-level query must be a `LOAD INTO` or a terminal `SELECT`");
    if (last.write.delete != null or last.write.update != null) {
        defer env.endStatement(!env.explain and !opts_in.explain);
        const dml = @import("dml.zig");
        return if (last.write.delete != null) dml.runDelete(env, out, opts_in, stats, lanes_used, batch_arena) else dml.runUpdate(env, out, opts_in, stats, lanes_used, batch_arena);
    }
    env.sink_name = sinkLabel(env, last.write);
    if (!env.explain and !std.mem.eql(u8, last.write.connector, "stdout")) env.wrote_sink = true;
    const is_load = !env.explain and !std.mem.eql(u8, last.write.connector, "stdout");
    if (is_load) env.last_target = try targetLabel(arena, last.write);
    defer env.endStatement(is_load);
    var prof: Profile = undefined;
    const own_profile = opts_in.explain and env.profile == null;
    if (own_profile) {
        prof = Profile.init(env.gpa);
        env.profile = &prof;
        trace.current = prof.tracer();
    }
    defer if (own_profile) {
        trace.current = null;
        env.profile = null;
        prof.deinit();
    };
    const t0 = std.time.Instant.now() catch null;
    if (last.write.changes != null) stages = try feed.prepare(env, out, opts_in, lanes_used, batch_arena);
    stages = try changes_pg.materialize(env, stages);
    defer if (last.write.changes != null) {
        _ = env.params.remove(analyze.since_name);
        _ = env.params_expr.remove(analyze.since_name);
    };
    const moving = env.progress != null and (is_load or env.progress.?.mode == .hook);
    if (moving) env.progress.?.begin(try moveLabel(arena, stages[0], last.write));
    const load_t0 = std.time.milliTimestamp();
    const load_rows0 = stats.rows_out;
    const read0 = env.rows_read.load(.monotonic);
    const lanes0 = lanes_used.*;
    lanes_used.* = 1;
    var load_err: ?anyerror = null;
    const branch = env.union_branch;
    defer {
        if (moving) env.progress.?.end();
        const lanes = lanes_used.*;
        lanes_used.* = @max(lanes0, lanes);
        if (is_load and !branch) noteLoad(env, out.pos, load_err, .{
            .rows = stats.rows_out - load_rows0,
            .rows_read = env.rows_read.load(.monotonic) - read0,
            .elapsed_ms = @intCast(@max(0, std.time.milliTimestamp() - load_t0)),
            .lanes = lanes,
        });
    }
    const scan0 = scanCounts(env);
    const held0: u64 = if (own_profile) if (env.space) |sp| sp.restartPeak() else 0 else 0;
    runOutputBody(env, opts_in, stages, last, stats, lanes_used, batch_arena) catch |e| {
        load_err = e;
        return e;
    };
    if (own_profile) {
        const scan1 = scanCounts(env);
        const facts: @import("explain.zig").RunFacts = .{
            .wall_ns = if (t0) |s| if (std.time.Instant.now()) |now| now.since(s) else |_| 0 else 0,
            .rows_read = env.rows_read.load(.monotonic) - read0,
            .rows_out = stats.rows_out - load_rows0,
            .lanes = lanes_used.*,
            .row_groups = scan1[0] - scan0[0],
            .row_groups_skipped = scan1[1] - scan0[1],
            .columns_read = scan1[2] - scan0[2],
            .columns_total = scan1[3] - scan0[3],
            .spill_peak = if (env.space) |sp| sp.peak() -| held0 else 0,
            .sink = std.mem.trimRight(u8, try msgfmt.allocPrint(arena, "{s} {s}", .{ try sinkKind(env, stages[stages.len - 1]), try targetLabel(arena, last.write) }), " "),
        };
        if (env.loop_report) |lr| try lr.take(env.report_row, &prof, facts) else try printProfile(env, &prof, facts);
    }
    if (!is_load and !env.explain) if (env.max_rows) |cap| {
        stats.rows_out = load_rows0 + @min(stats.rows_out - load_rows0, cap);
    };
}

/// What write stage `st` would write as, for a report: the format it names, else the
/// one its path says, else a folder's table, as the sink would find it.
fn sinkKind(env: *Env, st: ast.Stage) ![]const u8 {
    const w = st.node.write;
    if (!std.mem.eql(u8, w.connector, "csv")) return sinkLabel(env, w);
    var d = analyze.Diag{};
    if (analyze.formatFromHints(st.hints, &d) catch null) |f| return @tagName(f);
    if (std.mem.endsWith(u8, w.target, "/")) if (lake.detect(env.arena, w.target) catch null) |found| return @tagName(found.kind);
    return sinkLabel(env, w);
}

/// The run's read counts so far: row groups read and skipped, columns read and offered.
fn scanCounts(env: *Env) [4]u64 {
    const t = env.scan orelse return .{ 0, 0, 0, 0 };
    return .{ t.row_groups.load(.monotonic), t.row_groups_skipped.load(.monotonic), t.columns_read.load(.monotonic), t.columns_total.load(.monotonic) };
}

/// A derived table or CTE that aggregates a file or table is run first, in parallel,
/// into memory, since built where it is read it ran on one thread. Returns the
/// binding's name when it was; `held` gets every binding run into memory, for the
/// caller to drop afterwards.
fn materializeBinding(env: *Env, opts: RunOptions, stages: []const ast.Stage, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator, held: *std.array_list.Managed([]const u8)) anyerror!?[]const u8 {
    if (opts.lanes() < 2 or stages[0].node != .ref) return null;
    const name = stages[0].node.ref;
    if (!try materializeNamed(env, opts, name, lanes_used, batch_arena, held)) return null;
    try notePrelude(env, name, stages[0].pos);
    return name;
}

/// Join sides that aggregate by the join's key, their input first cut to the keys the
/// probe side can hold: an inner or semi join on a smaller side among the joins and
/// filters around it, or the FROM file itself, pins a probe key to a column of that
/// side, either read from it (`p.p_partkey`, the file joined as `p`) or matched
/// against it by the same key, so a group of any other key matches nothing (TPC-H
/// Q17's average quantity per part over every lineitem row, asked for 204 parts). A
/// semi join on that side goes into the aggregating side as early as its key column
/// is there, under a name of its own that `added` gets, for the caller to drop. Not
/// `NOT IN`, whose answer a null key on its side decides, nor a right or full join,
/// which emits the side's unmatched rows.
fn reduceJoinSides(env: *Env, stages: []const ast.Stage, added: *std.array_list.Managed([]const u8)) ![]const ast.Stage {
    var out: ?[]ast.Stage = null;
    var seen: ?[]const ast.Stage = null;
    for (stages, 0..) |st, i| {
        if (st.node != .join) continue;
        const j = st.node.join;
        switch (j.kind) {
            .left, .inner, .semi, .anti => {},
            else => continue,
        }
        if (j.null_aware or j.deferred.len > 0 or j.left_keys.len != j.right_keys.len) continue;
        if (std.mem.indexOf(u8, j.binding, keys_tag) != null or env.materialized.contains(j.binding)) continue;
        const b = (try stampedBinding(env, j.binding)) orelse continue;
        const ai = groupedAt(b.stages) orelse continue;
        for (j.left_keys, j.right_keys) |lk, rk| {
            const by = groupKey(b.stages[ai].node.aggregate, b.stages[ai + 1 ..], rk) orelse continue;
            const sv = seen orelse blk: {
                seen = (try pushdown.hoistFilters(env.arena, env.arena, stages, env.bindings, env.tables)) orelse stages;
                break :blk seen.?;
            };
            const src = (try keySource(env, sv, j.binding, lk)) orelse continue;
            const b_rows = estimate.ofSide(env, b.stages[0..1]).rows orelse continue;
            if (src.rows * joinorder.flip_ratio > b_rows) continue;
            const point = try semiPoint(env, b.stages, ai, by);
            const tag = @intFromPtr(stages.ptr) + i;
            const side = src.join orelse blk: {
                const hn = try msgfmt.allocPrint(env.arena, "{s}" ++ keys_tag ++ "head_{x}", .{ j.binding, tag });
                try env.bindings.put(hn, .{ .stages = src.side, .pos = b.pos });
                try added.append(hn);
                break :blk ast.Join{ .kind = .inner, .binding = hn, .alias = hn, .left_keys = &.{}, .right_keys = &.{} };
            };
            const rks = try env.arena.alloc(ast.QualName, 1);
            rks[0] = .{ .parts = try env.arena.dupe([]const u8, &.{src.col}), .span = rk.span };
            const semi = ast.Stage{ .node = .{ .join = .{
                .kind = .semi,
                .binding = side.binding,
                .alias = if (side.alias.len > 0) side.alias else side.binding,
                .left_keys = try env.arena.dupe(ast.QualName, &.{point.key}),
                .right_keys = rks,
                .right_filter = side.right_filter,
            } }, .hints = &.{}, .pos = st.pos };
            const name = try msgfmt.allocPrint(env.arena, "{s}" ++ keys_tag ++ "{x}", .{ j.binding, tag });
            const cut = try std.mem.concat(env.arena, ast.Stage, &.{ b.stages[0..point.at], &.{semi}, b.stages[point.at..] });
            try env.bindings.put(name, .{ .stages = cut, .pos = b.pos });
            try added.append(name);
            if (out == null) out = try env.arena.dupe(ast.Stage, stages);
            out.?[i].node.join.binding = name;
            break;
        }
    }
    return out orelse stages;
}

const keys_tag = "__keys_";

const Swap = struct { name: []const u8, was: ast.Pipeline };

/// `reduceJoinSides` for the bindings `stages` join and the ones those join, each
/// reduced binding put in place of its own until the caller restores `swapped`.
fn reduceNestedSides(env: *Env, stages: []const ast.Stage, added: *std.array_list.Managed([]const u8), swapped: *std.array_list.Managed(Swap), depth: usize) !void {
    if (depth > 8) return;
    for (stages) |st| {
        if (st.node != .join) continue;
        const name = st.node.join.binding;
        if (env.materialized.contains(name)) continue;
        const was = env.bindings.get(name) orelse continue;
        const cut = try reduceJoinSides(env, was.stages, added);
        if (cut.ptr != was.stages.ptr) {
            try swapped.append(.{ .name = name, .was = was });
            env.bindings.getPtr(name).?.stages = cut;
        }
        try reduceNestedSides(env, cut, added, swapped, depth + 1);
    }
}

/// The aggregate of `stages` whose groups come out whole: a file read, filters,
/// selects and joins, the aggregate, then only filters, selects, sorts and DISTINCT,
/// no LIMIT or window choosing among the groups.
fn groupedAt(stages: []const ast.Stage) ?usize {
    if (stages.len < 2 or stages[0].node != .read) return null;
    const ai = for (stages[1..], 1..) |s, k| switch (s.node) {
        .aggregate => break k,
        .filter, .select, .join => {},
        else => return null,
    } else return null;
    const ag = stages[ai].node.aggregate;
    if (ag.by_rest != null or ag.on_empty.len > 0 or ag.by.len == 0) return null;
    for (stages[ai + 1 ..]) |s| switch (s.node) {
        .filter, .select, .sort, .distinct => {},
        else => return null,
    };
    return ai;
}

/// The group key of `ag` that output column `out` carries through the selects of
/// `after`, the stages past the aggregate.
fn groupKey(ag: ast.Aggregate, after: []const ast.Stage, out: ast.QualName) ?ast.QualName {
    var q = out;
    var k = after.len;
    while (k > 0) {
        k -= 1;
        if (after[k].node == .select) q = selectSource(after[k].node.select, q) orelse return null;
    }
    for (ag.by) |by| if (std.ascii.eqlIgnoreCase(by.last(), q.last())) return by;
    return null;
}

/// The column a select's output `q` reads: the field it names or renames, or `q`
/// itself through `*`; null when computed or not plainly there.
fn selectSource(items: []const ast.SelectItem, q: ast.QualName) ?ast.QualName {
    var star = false;
    for (items) |it| switch (it) {
        .star => star = true,
        .field => |f| if (std.ascii.eqlIgnoreCase(f.last(), q.last())) return f,
        .computed => |c| if (std.ascii.eqlIgnoreCase(c.name, q.last())) {
            return if (c.expr.* == .field and !c.expr.field.dollar) c.expr.field else null;
        },
        else => return null,
    };
    return if (star) q else null;
}

/// Where in `stages` a semi join on group key `by` of the aggregate at `ai` can go,
/// and the column it is there: walked back past filters, selects that pass or rename
/// it, and joins that bring no column of its name, to the file read at best.
fn semiPoint(env: *Env, stages: []const ast.Stage, ai: usize, by: ast.QualName) !struct { at: usize, key: ast.QualName } {
    var key = by;
    var at = ai;
    while (at > 1) : (at -= 1) {
        switch (stages[at - 1].node) {
            .filter => {},
            .select => |items| key = selectSource(items, key) orelse break,
            .join => |jj| switch (jj.kind) {
                .semi, .anti => {},
                .inner, .left => {
                    if (key.parts.len > 1) {
                        if (std.ascii.eqlIgnoreCase(key.parts[0], if (jj.alias.len > 0) jj.alias else jj.binding)) break;
                    } else {
                        const sch = (try sideSchema(env, jj, stages[at..])) orelse break;
                        if (sch.indexOf(key.last()) != null) break;
                    }
                },
                else => break,
            },
            else => break,
        }
    }
    return .{ .at = at, .key = key };
}

/// Whether a side's rows are fewer than its file's: a filter cuts them, or they are
/// held in memory, counted. An estimate reads a file's rows, not a filter's.
fn narrowed(env: *Env, side: []const ast.Stage) bool {
    if (side.len > 0 and side[0].node == .ref and env.materialized.contains(side[0].node.ref)) return true;
    for (side) |st| if (st.node == .filter) return true;
    return false;
}

const KeySrc = struct { join: ?ast.Join, side: []const ast.Stage, col: []const u8, rows: u64 };

/// The side estimated smallest among the joins and filters around the join reading
/// `binding` in `stages` (their filters pushed into the join sides) that pins probe
/// key `lk` to a column of its own, and that column: an inner join ahead of it that
/// `lk` names the side of (`p.p_partkey`, `p` its alias), an inner or semi join keyed
/// on `lk` itself, or the FROM file with its filters when only it has a column `lk`
/// names.
fn keySource(env: *Env, stages: []const ast.Stage, binding: []const u8, lk: ast.QualName) !?KeySrc {
    const at = for (stages, 0..) |st, k| {
        if (st.node == .join and std.mem.eql(u8, st.node.join.binding, binding)) break k;
    } else return null;
    var lo = at;
    while (lo > 1 and (stages[lo - 1].node == .join or stages[lo - 1].node == .filter)) lo -= 1;
    var hi = at + 1;
    while (hi < stages.len and (stages[hi].node == .join or stages[hi].node == .filter)) hi += 1;
    var best: ?KeySrc = null;
    for (stages[lo..hi], lo..) |st, k| {
        if (k == at or st.node != .join) continue;
        const j0 = st.node.join;
        if (j0.null_aware or j0.deferred.len > 0 or j0.left_keys.len == 0 or j0.left_keys.len != j0.right_keys.len) continue;
        const col = col: {
            if (j0.kind == .inner and k < at and lk.parts.len == 2) {
                const name = if (j0.alias.len > 0) j0.alias else j0.binding;
                if (std.ascii.eqlIgnoreCase(lk.parts[0], name)) break :col lk.last();
            }
            if (j0.kind != .inner and j0.kind != .semi) continue;
            for (j0.left_keys, j0.right_keys) |l0, r0| {
                if (l0.parts.len != lk.parts.len) continue;
                const same = for (l0.parts, lk.parts) |x, y| {
                    if (!std.ascii.eqlIgnoreCase(x, y)) break false;
                } else true;
                if (same) break :col r0.last();
            }
            continue;
        };
        const side = try rightStagesFor(env, j0, null);
        if (!narrowed(env, side)) continue;
        const rows = estimate.ofSide(env, side).rows orelse continue;
        if (best == null or rows < best.?.rows) best = .{ .join = j0, .side = side, .col = col, .rows = rows };
    }
    if (lo != 1 or stages[0].node != .read or lk.parts.len != 1) return best;
    const head = offlineSchema(env.arena, stages[0].node.read, stages[0].hints, env.tables) orelse return best;
    if (head.indexOf(lk.last()) == null) return best;
    for (stages[1..at], 1..) |st, k| {
        if (st.node != .join or st.node.join.kind == .semi or st.node.join.kind == .anti) continue;
        const sch = (try sideSchema(env, st.node.join, stages[k + 1 ..])) orelse return best;
        if (sch.indexOf(lk.last()) != null) return best;
    }
    var m: usize = 1;
    while (m < stages.len and stages[m].node == .filter) m += 1;
    if (m == 1) return best;
    const rows = estimate.ofSide(env, stages[0..m]).rows orelse return best;
    if (best == null or rows < best.?.rows) best = .{ .join = null, .side = stages[0..m], .col = lk.last(), .rows = rows };
    return best;
}

/// The bindings the pipeline's joins read that aggregate a file or table, each run
/// first, in parallel, into memory, as `materializeBinding` runs a head one: as a
/// join's build side it was built serially, an `IN (SELECT ... GROUP BY ...)`'s
/// aggregate over a whole table on one thread. The join then reads it from memory
/// (`plan.joinBindingStages`). A side that does not aggregate has its own sides
/// looked at the same way: an `IN` over a join with a correlated aggregate (TPC-H
/// Q20's) built that aggregate on one thread as a side of a side. `held` gets every
/// binding run into memory, for the caller to drop.
fn materializeJoinSides(env: *Env, opts: RunOptions, stages: []const ast.Stage, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator, held: *std.array_list.Managed([]const u8)) anyerror!void {
    return materializeSidesAt(env, opts, stages, lanes_used, batch_arena, held, 0);
}

fn materializeSidesAt(env: *Env, opts: RunOptions, stages: []const ast.Stage, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator, held: *std.array_list.Managed([]const u8), depth: usize) anyerror!void {
    if (opts.lanes() < 2 or depth > 8) return;
    for (stages) |st| {
        if (st.node != .join) continue;
        const name = st.node.join.binding;
        if (try materializeNamed(env, opts, name, lanes_used, batch_arena, held)) continue;
        if (env.materialized.contains(name)) continue;
        const b = (try stampedBinding(env, name)) orelse continue;
        try materializeSidesAt(env, opts, b.stages, lanes_used, batch_arena, held, depth + 1);
    }
}

/// `stages` run into memory, under `EXPLAIN ANALYZE` profiled in a scope of their
/// own named `what`.
pub fn runToMemory(env: *Env, opts: RunOptions, stages: []const ast.Stage, what: []const u8, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!env_mod.MemSink {
    return runToMemoryCapped(env, opts, stages, what, lanes_used, batch_arena, null);
}

/// `runToMemory` failing with `error.PastMemCap` past `cap` rows.
fn runToMemoryCapped(env: *Env, opts: RunOptions, stages: []const ast.Stage, what: []const u8, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator, cap: ?u64) anyerror!env_mod.MemSink {
    const inner = try env.arena.alloc(ast.Stage, stages.len + 1);
    @memcpy(inner[0..stages.len], stages);
    inner[stages.len] = .{ .node = .{ .write = .{ .connector = mem_connector, .form = null, .target = "", .mode = .overwrite } }, .hints = &.{}, .pos = stages[0].pos };
    var ms = env_mod.MemSink{ .arena = env.arena, .batches = std.array_list.Managed(Batch).init(env.arena), .cap_rows = cap };
    const outer_sink = env.mem_sink;
    const outer_name = env.sink_name;
    const outer_free = env.out_order_free;
    env.mem_sink = &ms;
    defer env.mem_sink = outer_sink;
    defer env.sink_name = outer_name;
    env.out_order_free = false;
    defer env.out_order_free = outer_free;
    var scratch: Stats = .{};
    const outer_scope = enterScope(env, what);
    defer leaveScope(env, outer_scope);
    try runOutputBody(env, opts, inner, inner[stages.len].node, &scratch, lanes_used, batch_arena);
    return ms;
}

/// Binding `name` run on lanes into memory when it aggregates a file or table and is
/// not there yet; whether it was. A binding that reads another (a CTE over a CTE)
/// first has the one it starts from run into memory, when that one aggregates, else
/// laid out as the pipeline over it would lay it out (one with a window is kept): kept
/// as a reference it was never run first, and every CTE of a report built on CTEs
/// ran on one thread. `held` gets each binding run into memory, nested ones too.
pub fn materializeNamed(env: *Env, opts: RunOptions, name: []const u8, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator, held: *std.array_list.Managed([]const u8)) anyerror!bool {
    if (env.materialized.contains(name)) return false;
    const b = (try stampedBinding(env, name)) orelse return false;
    var stages = b.stages;
    var n: usize = 0;
    while (stages.len > 0 and stages[0].node == .ref and n < 16) : (n += 1) {
        const under = stages[0].node.ref;
        if (env.materialized.contains(under)) break;
        if (try materializeNamed(env, opts, under, lanes_used, batch_arena, held)) break;
        const ub = (try stampedBinding(env, under)) orelse break;
        if (ub.stages.len == 0 or (ub.stages[0].node != .read and ub.stages[0].node != .ref)) break;
        const windowed = for (ub.stages) |st| {
            if (st.node == .window) break true;
        } else false;
        if (windowed) break;
        stages = try std.mem.concat(env.arena, ast.Stage, &.{ ub.stages, stages[1..] });
    }
    if (!path.materializes(stages, opts.lanes())) return false;

    const inner = try env.arena.alloc(ast.Stage, stages.len + 1);
    @memcpy(inner[0..stages.len], stages);
    const w = ast.Write{ .connector = mem_connector, .form = null, .target = "", .mode = .overwrite };
    inner[stages.len] = .{ .node = .{ .write = w }, .hints = &.{}, .pos = b.pos };
    var ms = env_mod.MemSink{ .arena = env.shared orelse env.arena, .batches = std.array_list.Managed(Batch).init(env.shared orelse env.arena) };
    const outer_sink = env.mem_sink;
    const outer_name = env.sink_name;
    const outer_free = env.out_order_free;
    env.mem_sink = &ms;
    defer env.mem_sink = outer_sink;
    defer env.sink_name = outer_name;
    env.out_order_free = false;
    defer env.out_order_free = outer_free;
    var scratch: Stats = .{};
    {
        const outer_scope = enterScope(env, name);
        defer leaveScope(env, outer_scope);
        try runOutputBody(env, opts, inner, inner[stages.len].node, &scratch, lanes_used, batch_arena);
    }
    try env.materialized.put(env.arena, name, .{ .schema = ms.schema, .batches = ms.batches.items });
    try held.append(name);
    return true;
}

/// The rows a union run into memory first may hold, its branches each and itself;
/// past them it runs as planned.
const union_mem_rows: u64 = 1 << 23;

/// A union at the head (or the head binding's, through bindings of bindings), run into memory first when its rows
/// are broken after it (an aggregate, DISTINCT, sort or window, a set operation's own)
/// or a branch breaks or joins its own: each branch of its own pipeline run on its
/// own path, then, rows broken after it, the branches put together, an INTERSECT's or
/// EXCEPT's side marks kept and the operation left to stages that follow
/// (`union.setOpStages`), so the lanes take them over memory (`memLanes`). Built in
/// place, every branch ran on one thread and the set operation counted on one. Null
/// leaves the stages as they are; `held` gets each binding run into memory.
fn unionToMemory(env: *Env, opts: RunOptions, stages_in: []const ast.Stage, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator, held: *std.array_list.Managed([]const u8)) anyerror!?[]const ast.Stage {
    if (opts.lanes() < 2 or stages_in.len < 2 or env.union_sink != null) return null;
    var stages = stages_in;
    var depth: usize = 0;
    while (stages[0].node == .ref and depth < 16) : (depth += 1) {
        if (env.materialized.contains(stages[0].node.ref)) return null;
        const b = (try stampedBinding(env, stages[0].node.ref)) orelse return null;
        if (b.stages.len == 0 or (b.stages[0].node != .union_ and b.stages[0].node != .ref)) return null;
        stages = try std.mem.concat(env.arena, ast.Stage, &.{ b.stages, stages[1..] });
    }
    if (stages[0].node != .union_) return null;
    const u = stages[0].node.union_;
    if (u.branches.len < 2 or u.marks_only or (u.set != .union_all and u.set_all)) return null;
    const lowered = u.set != .union_all;
    const after = stages[1 .. stages.len - 1];
    const broken = lowered or for (after) |st| switch (st.node) {
        .aggregate, .distinct, .sort, .window => break true,
        else => {},
    } else false;
    var branch_breaks = false;
    for (u.branches) |br| if (br.pipeline) |p| for (p.stages) |st| switch (st.node) {
        .aggregate, .distinct, .sort, .window, .join, .union_, .ref => branch_breaks = true,
        else => {},
    };
    if (!broken and !branch_breaks) return null;

    const tag = @intFromPtr(stages_in.ptr);
    const branches = try env.arena.dupe(ast.UnionBranch, u.branches);
    for (branches, 0..) |*br, i| {
        const p = br.pipeline orelse continue;
        const name = try msgfmt.allocPrint(env.arena, "__union_{x}_{d}", .{ tag, i });
        const ms = runToMemoryCapped(env, opts, p.stages, name, lanes_used, batch_arena, union_mem_rows) catch |e| {
            if (e == error.PastMemCap) return null;
            return e;
        };
        try env.materialized.put(env.arena, name, .{ .schema = ms.schema, .batches = ms.batches.items });
        try held.append(name);
        const ref = try env.arena.alloc(ast.Stage, 1);
        ref[0] = .{ .node = .{ .ref = name }, .hints = &.{}, .pos = p.pos };
        br.pipeline = .{ .stages = ref, .pos = p.pos };
    }
    var nu = u;
    nu.branches = branches;
    nu.marks_only = lowered;
    const head = ast.Stage{ .node = .{ .union_ = nu }, .hints = stages[0].hints, .pos = stages[0].pos };
    if (!broken) return try std.mem.concat(env.arena, ast.Stage, &.{ &.{head}, stages[1..] });

    const name = try msgfmt.allocPrint(env.arena, "__union_{x}", .{tag});
    const ms = runToMemoryCapped(env, opts, &.{head}, name, lanes_used, batch_arena, union_mem_rows) catch |e| {
        if (e == error.PastMemCap) return null;
        return e;
    };
    try env.materialized.put(env.arena, name, .{ .schema = ms.schema, .batches = ms.batches.items });
    try held.append(name);
    if (env.profile) |p| p.path = analyze.union_branches_text;
    try notePrelude(env, "the union", stages[0].pos);
    const ref = ast.Stage{ .node = .{ .ref = name }, .hints = &.{}, .pos = stages[0].pos };
    const set_stages: []const ast.Stage = if (lowered) try union_plan.setOpStages(env.arena, u.set, stages[0].pos) else &.{};
    return try std.mem.concat(env.arena, ast.Stage, &.{ &.{ref}, set_stages, stages[1..] });
}

/// A pipeline whose head is a binding held in memory of `lanes.mem_lane_rows` rows or
/// more, breaking them (an aggregate, DISTINCT, top-N, sort or window), run on lanes
/// over its batches, a large one cut into views of that many rows: over a CTE run
/// into memory first, the rest of a report ran on one thread.
fn memLanes(env: *Env, opts: RunOptions, stages: []const ast.Stage, w: ast.Write, stats: *Stats, lanes_used: *usize) anyerror!bool {
    if (opts.lanes() < 2 or stages.len < 3 or stages[0].node != .ref) return false;
    const mat = env.materialized.get(stages[0].node.ref) orelse return false;
    var rows: usize = 0;
    for (mat.batches) |b| rows += b.len;
    if (rows < lane_paths.mem_lane_rows) return false;
    const shape = lane_paths.classifyLaneShape(stages) orelse return false;
    if (!lane_paths.memLaneShape(shape) or !lane_paths.shapeLaneSafe(shape)) return false;
    var pieces = std.array_list.Managed(Batch).init(env.arena);
    for (mat.batches) |b| {
        var lo: usize = 0;
        while (lo < b.len) : (lo += lane_paths.mem_lane_rows) {
            const n = @min(lane_paths.mem_lane_rows, b.len - lo);
            if (lo == 0 and n == b.len) {
                try pieces.append(b);
                break;
            }
            const cols = try env.arena.alloc(column.Column, b.columns.len);
            for (cols, b.columns) |*o, c| o.* = try column.slice(env.arena, c, lo, n);
            try pieces.append(.{ .schema = b.schema, .columns = cols, .len = n });
        }
    }
    const sch = try env.arena.create(types.Schema);
    sch.* = mat.schema;
    const before = if (env.profile) |p| p.path else "";
    if (!try lane_paths.runMemLane(env, .{ .mem = .{ .batches = pieces.items, .schema = sch } }, shape, w, opts, stats, lanes_used)) return false;
    if (env.profile) |p| {
        if (std.mem.endsWith(u8, before, "then serial"))
            p.path = try msgfmt.allocPrint(env.arena, "{s}{s}", .{ before[0 .. before.len - "serial".len], p.path });
    }
    return true;
}

/// Under `EXPLAIN ANALYZE`, a run inside the pipeline's (an input read into memory
/// first) is profiled in its own scope, its operators apart from the pipeline's
/// built from the same stages; returns the scope to restore.
fn enterScope(env: *Env, what: []const u8) u64 {
    const p = env.profile orelse return 0;
    const outer = p.scope;
    p.scope = std.hash.Wyhash.hash(outer, what);
    return outer;
}

fn leaveScope(env: *Env, outer: u64) void {
    if (env.profile) |p| p.scope = outer;
}

/// Under `EXPLAIN ANALYZE`, the path names the input read into memory first and how,
/// until the pipeline over it names its own, and the scan at `pos` over it is named.
fn notePrelude(env: *Env, what: []const u8, pos: ast.Pos) !void {
    const p = env.profile orelse return;
    p.path = try msgfmt.allocPrint(env.arena, "{s} read into memory first ({s}), then serial", .{ what, p.path });
    try p.describe(profile.keyOf(pos), .scan, try msgfmt.allocPrint(env.arena, "{s}, in memory", .{what}));
}

const window_topk = "__window_topk";

/// A top-k window (a lone ROW_NUMBER filtered `rn <= k`) over a local file runs on
/// lanes, its output held in memory and the rest of the pipeline read from it
/// (`lanes/window.zig`): serially, every row went through one top-k core. Only map
/// stages may sit between the read and the window. The read takes the columns the
/// whole pipeline needs; row groups are skipped by the filters before the window
/// alone, as one after it filters the window's output, not its input.
fn windowTopKLanes(env: *Env, opts: RunOptions, stages_in: []const ast.Stage, lanes_used: *usize) anyerror!?[]const ast.Stage {
    if (opts.lanes() < 2 or env.materialized.contains(window_topk)) return null;
    var stages = stages_in;
    if (stages.len > 0 and stages[0].node == .ref) {
        if (env.materialized.contains(stages[0].node.ref)) return null;
        const b = (try stampedBinding(env, stages[0].node.ref)) orelse return null;
        if (b.stages.len == 0 or b.stages[0].node != .read) return null;
        stages = try std.mem.concat(env.arena, ast.Stage, &.{ b.stages, stages[1..] });
    }
    if (stages.len < 3 or stages[0].node != .read or !path.laneEligible(stages, opts.lanes())) return null;
    const rd = stages[0].node.read;
    if (!connect.isLocalCsvRead(rd) and !connect.isParquetFileRead(rd) and !connect.isFolderRead(rd)) return null;
    var wi: usize = 1;
    while (wi < stages.len and (stages[wi].node == .filter or stages[wi].node == .select)) wi += 1;
    if (wi >= stages.len or stages[wi].node != .window) return null;
    var j = wi + 1;
    while (j < stages.len and stages[j].node == .select) j += 1;
    const topped = (try windowTopK(env.arena, stages[0..j], stages[j..])) orelse return null;

    const w = ast.Write{ .connector = "", .form = null, .target = "", .mode = .append };
    var mapped: ?*MappedCsv = null;
    defer if (mapped) |mc| mc.close();
    var crd = rd;
    if (crd.cols.len == 0) if (try @import("../plan.zig").readProjection(env, crd, stages[0].hints, stages[1..])) |cols| {
        crd.cols = cols;
        crd.projected = true;
    };
    const m: Morsels = if (try parquetSplit(env, crd, stages[0].hints, stages[1..wi], w, opts)) |pm| pm else blk: {
        mapped = (try csvSplitFile(env, crd, stages[0].hints, w)) orelse return null;
        break :blk Morsels.ofCsv(mapped.?, &mapped.?.schema, opts.morsel_bytes);
    };
    if (m.count() < 2) return null;
    const mat = try runWindowTopK(env, &m, stages[1..wi], topped[wi], opts, lanes_used);
    try notePrelude(env, "the top-k window", stages[0].pos);
    try env.materialized.put(env.arena, window_topk, mat);

    const outer = try env.arena.alloc(ast.Stage, stages.len - wi);
    outer[0] = .{ .node = .{ .ref = window_topk }, .hints = &.{}, .pos = stages[0].pos };
    @memcpy(outer[1..], stages[wi + 1 ..]);
    return outer;
}

/// Whether `stages`, or a binding they read (a CTE, a joined one, a union's branch),
/// calls an http function per row (`JOIN LATERAL f(t.col)`). Such a statement runs
/// on one lane, the lookup's own requests the only ones in flight: a parallel path
/// samples, reads ahead or reruns its input, and every call is paid for.
fn callsApi(env: *Env, stages: []const ast.Stage, depth: usize) bool {
    if (depth > 16) return false;
    for (stages) |st| switch (st.node) {
        .lookup => return true,
        .ref => |name| if (env.bindings.get(name)) |b| if (callsApi(env, b.stages, depth + 1)) return true,
        .join => |j| if (env.bindings.get(j.binding)) |b| if (callsApi(env, b.stages, depth + 1)) return true,
        .union_ => |u| for (u.branches) |br| if (br.pipeline) |pl| if (callsApi(env, pl.stages, depth + 1)) return true,
        else => {},
    };
    return false;
}

/// Whether `stages`, or a binding they read (a CTE, a joined one, a union's branch),
/// reads the connection `name`. A bulk load into a SQL Server table it reads takes no
/// table lock: the read holds its own lock on the table until its last row, the load
/// would wait for it while the read waits for the load to take its rows, and the
/// server sees no deadlock in that, the cycle closing through the run.
fn readsConnection(env: *Env, stages: []const ast.Stage, name: []const u8, depth: usize) bool {
    if (depth > 16) return true;
    for (stages) |st| switch (st.node) {
        .read => |rd| if (std.mem.eql(u8, rd.connector, name)) return true,
        .ref => |n| if (env.bindings.get(n)) |b| if (readsConnection(env, b.stages, name, depth + 1)) return true,
        .join => |j| if (env.bindings.get(j.binding)) |b| if (readsConnection(env, b.stages, name, depth + 1)) return true,
        .union_ => |u| for (u.branches) |br| {
            if (std.mem.eql(u8, br.read.connector, name)) return true;
            if (br.pipeline) |pl| if (readsConnection(env, pl.stages, name, depth + 1)) return true;
        },
        else => {},
    };
    return false;
}

/// How often `stages`, and the bindings they read, read each binding, a binding's
/// own reads walked the first time it is met, so `counts` lists the ones it reads
/// before it.
fn countReads(env: *Env, stages: []const ast.Stage, counts: *std.StringArrayHashMap(usize), depth: usize) !void {
    if (depth > 16) return;
    for (stages) |st| {
        const name = switch (st.node) {
            .ref => |n| n,
            .join => |j| j.binding,
            .union_ => |u| {
                for (u.branches) |br| if (br.pipeline) |pl| try countReads(env, pl.stages, counts, depth + 1);
                continue;
            },
            else => continue,
        };
        if (counts.getPtr(name)) |n| {
            n.* += 1;
            continue;
        }
        if (env.bindings.get(name)) |b| try countReads(env, b.stages, counts, depth + 1);
        try counts.put(name, 1);
    }
}

/// Each binding that calls an API and is read more than once, run into memory once
/// ahead, the ones it reads first, so every read takes the same rows and no row is
/// sent twice. A binding read once streams as it is. Returns their names, for the
/// caller to drop.
fn materializeApiBindings(env: *Env, opts: RunOptions, stages: []const ast.Stage, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror![]const []const u8 {
    var counts = std.StringArrayHashMap(usize).init(env.arena);
    try countReads(env, stages, &counts, 0);
    var names = std.array_list.Managed([]const u8).init(env.arena);
    for (counts.keys(), counts.values()) |name, n| {
        if (n < 2 or env.materialized.contains(name)) continue;
        const b = (try stampedBinding(env, name)) orelse continue;
        if (!callsApi(env, b.stages, 0)) continue;
        const ms = try runToMemory(env, opts, b.stages, name, lanes_used, batch_arena);
        try env.materialized.put(env.arena, name, .{ .schema = ms.schema, .batches = ms.batches.items });
        try names.append(name);
    }
    return names.toOwnedSlice();
}

const flip_left = "__flip_left";
const min_flip_rows: u64 = 1 << 16;
const max_flip_depth = 8;

/// A run whose last join `flipLaterJoin` may turn: map stages, then the span from its
/// first join on (the joins and the map stages between and after them), then what
/// takes its rows: a lane aggregate (`agg`), or, for a run held in memory to be
/// indexed (`heldTurn`), the memory write, the span's last map stages ahead of it.
const Turn = struct {
    prefix: []const ast.Stage,
    joins: []const ast.Stage,
    agg: ?SinkShape = null,

    fn ofAgg(shape: SinkShape) Turn {
        return .{ .prefix = shape.prefix, .joins = shape.joins, .agg = shape };
    }

    /// The stages after the span: the aggregate and its tail, or none.
    fn consumer(self: Turn, arena: std.mem.Allocator) ![]const ast.Stage {
        const a = self.agg orelse return &.{};
        return std.mem.concat(arena, ast.Stage, &.{ &.{a.agg}, a.tail });
    }
};

/// A run held in memory to be indexed (`Env.out_order_free`, the memory write last)
/// that reads a file and joins: its last join turns as a lane aggregate's does, so the
/// joins held before a turned join turn too, at any depth (`max_flip_depth`).
fn heldTurn(env: *const Env, stages: []const ast.Stage) ?Turn {
    if (!env.out_order_free or stages.len < 3 or stages[0].node != .read) return null;
    const w = stages[stages.len - 1];
    if (w.node != .write or !std.mem.eql(u8, w.node.write.connector, mem_connector)) return null;
    const middle = stages[1 .. stages.len - 1];
    var first: ?usize = null;
    for (middle, 0..) |st, i| switch (st.node) {
        .filter, .select => {},
        .join => if (first == null) {
            first = i;
        },
        else => return null,
    };
    const f = first orelse return null;
    return .{ .prefix = middle[0..f], .joins = middle[f..] };
}

/// The name a held run is bound under at the first depth free, null past the last.
fn flipName(env: *Env) !?[]const u8 {
    if (!env.materialized.contains(flip_left)) return flip_left;
    for (1..max_flip_depth) |d| {
        const name = try msgfmt.allocPrint(env.arena, "{s}{d}", .{ flip_left, d });
        if (!env.materialized.contains(name)) return name;
    }
    return null;
}

/// A lane aggregate's join run the other way round, decided on the sizes the run
/// meets: the joins before it can leave few rows (q03's customers and orders, 147K)
/// while its right side is a large file (3.2M filtered lineitem rows, indexed to be
/// probed 147K times). The last join, its right side a local CSV or Parquet file, or
/// an earlier one of more rows whose later joins all index sides estimated under its
/// cap (q05's lineitem, then supplier, nation and region): a later join whose side is
/// unknown or large could have cut the head with its runtime filter (q18's IN), and
/// would cut the turned file instead. What comes before it, the head and the joins ahead, narrowed to the columns
/// the rest reads, is run on lanes into memory first, while it stays under that
/// side's estimated rows over `joinorder.flip_ratio`. Then the first join of what is
/// left, this one, turns around (`lanes/join.zig` `flipFirstJoin`): those rows
/// indexed, the file read on the lanes. Past the cap the rows are dropped and the run
/// goes as planned, having spent at most that many; so does one where the turn is
/// declined. A first join is taken the same way when a filter on the head can leave
/// few rows (q12: 31K of 6M lineitem rows against 1.5M orders), and its first rows'
/// pass rate does not put it past the cap (`passingRows`). The run held in memory
/// goes through here too (`heldTurn`), so its own last join turns, its rows going to
/// memory through `runParallelMap`: nation and customer held, orders read on the
/// lanes into memory, then lineitem read on the lanes into the aggregate.
fn flipLaterJoin(env: *Env, stages: []const ast.Stage, shape: Turn, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!bool {
    const held_name = (try flipName(env)) orelse return false;
    const pick = (try turnedJoin(env, shape)) orelse return false;
    const ji = pick.at;
    const st = shape.joins[ji];
    const rows = pick.rows;
    const cap = rows / joinorder.flip_ratio;
    if (cap < min_flip_rows) return false;
    var head_rows: u64 = 0;
    if (ji == 0) {
        for (shape.prefix) |x| {
            if (x.node == .filter) break;
        } else return false;
        head_rows = estimate.ofSide(env, stages[0..1]).rows orelse return false;
        if (head_rows <= cap) return false;
    }
    const side = if (st.node.join.alias.len > 0) st.node.join.alias else st.node.join.binding;

    const pre_len = 1 + shape.prefix.len + ji;
    const keep = (try flipKeeps(env, stages[0..pre_len], stages[pre_len .. stages.len - 1])) orelse return false;
    const inner = try env.arena.alloc(ast.Stage, pre_len + 2);
    @memcpy(inner[0..pre_len], stages[0..pre_len]);
    const pos = st.pos;
    inner[pre_len] = .{ .node = .{ .select = keep }, .hints = &.{}, .pos = pos };
    const mw = ast.Write{ .connector = mem_connector, .form = null, .target = "", .mode = .overwrite };
    inner[pre_len + 1] = .{ .node = .{ .write = mw }, .hints = &.{}, .pos = pos };
    if (ji == 0 and try passingRows(env, inner[0 .. pre_len + 1], mw, opts, head_rows, std.math.maxInt(u64)) > cap) return false;
    var ms = env_mod.MemSink{ .arena = env.shared orelse env.arena, .batches = std.array_list.Managed(Batch).init(env.shared orelse env.arena), .cap_rows = cap, .keep_rels = true };
    const before = if (ji == 0) "the filtered rows before" else "the joins before";
    {
        const outer_sink = env.mem_sink;
        const outer_name = env.sink_name;
        const outer_free = env.out_order_free;
        env.mem_sink = &ms;
        defer env.mem_sink = outer_sink;
        env.out_order_free = true;
        defer env.out_order_free = outer_free;
        defer env.sink_name = outer_name;
        var scratch: Stats = .{};
        const outer_scope = enterScope(env, held_name);
        defer leaveScope(env, outer_scope);
        runOutputBody(env, opts, inner, inner[pre_len + 1].node, &scratch, lanes_used, batch_arena) catch |e| {
            if (e != error.PastMemCap) return e;
            env.log.log(.debug, "{s} `{s}` leave over {d} rows; it is held in memory as written", .{ before, side, cap });
            return false;
        };
    }
    try env.materialized.put(env.arena, held_name, .{ .schema = ms.schema, .batches = ms.batches.items });
    defer _ = env.materialized.remove(held_name);
    const head = ast.Stage{ .node = .{ .ref = held_name }, .hints = &.{}, .pos = pos };
    const joins = try env.arena.dupe(ast.Stage, shape.joins[ji..]);
    joins[0].node.join.own_cols = &.{};
    if (shape.agg) |a| {
        const rest = SinkShape{ .prefix = &.{}, .joins = joins, .agg = a.agg, .tail = a.tail };
        if (!try runParallelAgg(env, null, head, rest, w, opts, stats, lanes_used)) return false;
    } else {
        const j = joins[0];
        const js = MapJoinShape{ .prefix = &.{}, .join = j.node.join, .join_hints = j.hints, .suffix = joins[1..], .stage = j };
        if (!try runParallelMap(env, null, head, &.{}, js, w, opts, stats, lanes_used)) return false;
    }
    if (env.profile) |p| {
        const what = try msgfmt.allocPrint(env.arena, "{s} `{s}` ({d} rows)", .{ before, side, ms.rows });
        p.path = try msgfmt.allocPrint(env.arena, "{s} read into memory first, then {s}, that join turned around", .{ what, p.path });
        try p.describe(profile.keyOf(pos), .scan, try msgfmt.allocPrint(env.arena, "{s}, in memory", .{what}));
    }
    return true;
}

/// A lane aggregate whose joins end in semi and anti joins over large files run with
/// what comes before them read into memory first: the head and the joins ahead, on
/// the lanes, narrowed to the columns the rest reads, while they stay under the
/// largest such file's estimated rows over `joinorder.flip_ratio`. With no join ahead
/// the head must be filtered and its first rows' pass rate put it under a quarter of
/// that (TPC-H Q4's quarter of orders against its EXISTS over lineitem). Each semi or anti
/// join's build side is then read under a filter by the keys the held rows hold
/// (`lanes/join.zig` `heldSlots`), and the held rows are probed on the lanes. Past
/// the cap the rows are dropped and the run goes as planned, having spent at most that
/// many. Not `NOT IN`, whose answer a null key on its build side decides.
fn holdBeforeSemis(env: *Env, stages: []const ast.Stage, shape: SinkShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!bool {
    const held_name = (try flipName(env)) orelse return false;
    var si = shape.joins.len;
    while (si > 0) {
        const st = shape.joins[si - 1];
        switch (st.node) {
            .filter, .select => {},
            .join => |j| if ((j.kind != .semi and j.kind != .anti) or j.null_aware) break,
            else => break,
        }
        si -= 1;
    }
    while (si < shape.joins.len and shape.joins[si].node != .join) si += 1;
    if (si == shape.joins.len) return false;
    var before = false;
    for (shape.joins[0..si]) |st| if (st.node == .join) {
        before = true;
    };
    const filtered = for (shape.prefix) |x| {
        if (x.node == .filter) break true;
    } else false;
    if (!before and !filtered) return false;

    const consumer = try std.mem.concat(env.arena, ast.Stage, &.{ &.{shape.agg}, shape.tail });
    var largest: u64 = 0;
    for (shape.joins[si..], si..) |st, i| if (st.node == .join) {
        const after = try std.mem.concat(env.arena, ast.Stage, &.{ shape.joins[i + 1 ..], consumer });
        const rst = try rightStagesFor(env, st.node.join, after);
        if (!fileSide(rst)) continue;
        largest = @max(largest, estimate.ofSide(env, rst).rows orelse continue);
    };
    const cap = if (before) largest / joinorder.flip_ratio else largest / (joinorder.flip_ratio * 4);
    if (cap < min_flip_rows) return false;

    const pre_len = 1 + shape.prefix.len + si;
    const keep = (try flipKeeps(env, stages[0..pre_len], stages[pre_len .. stages.len - 1])) orelse return false;
    const pos = shape.joins[si].pos;
    const inner = try env.arena.alloc(ast.Stage, pre_len + 2);
    @memcpy(inner[0..pre_len], stages[0..pre_len]);
    inner[pre_len] = .{ .node = .{ .select = keep }, .hints = &.{}, .pos = pos };
    const mw = ast.Write{ .connector = mem_connector, .form = null, .target = "", .mode = .overwrite };
    inner[pre_len + 1] = .{ .node = .{ .write = mw }, .hints = &.{}, .pos = pos };
    if (!before) {
        const head_rows = estimate.ofSide(env, stages[0..1]).rows orelse return false;
        if (try passingRows(env, inner[0 .. pre_len + 1], mw, opts, head_rows, std.math.maxInt(u64)) > cap) return false;
    }
    var ms = env_mod.MemSink{ .arena = env.shared orelse env.arena, .batches = std.array_list.Managed(Batch).init(env.shared orelse env.arena), .cap_rows = cap, .keep_rels = true };
    {
        const outer_sink = env.mem_sink;
        const outer_name = env.sink_name;
        const outer_free = env.out_order_free;
        env.mem_sink = &ms;
        defer env.mem_sink = outer_sink;
        env.out_order_free = true;
        defer env.out_order_free = outer_free;
        defer env.sink_name = outer_name;
        var scratch: Stats = .{};
        const outer_scope = enterScope(env, held_name);
        defer leaveScope(env, outer_scope);
        runOutputBody(env, opts, inner, inner[pre_len + 1].node, &scratch, lanes_used, batch_arena) catch |e| {
            if (e != error.PastMemCap) return e;
            env.log.log(.debug, "the joins before the semi and anti joins leave over {d} rows; they run as planned", .{cap});
            return false;
        };
    }
    if (ms.batches.items.len == 0) return false;
    const held_schema = try env.arena.create(types.Schema);
    held_schema.* = ms.schema;
    const m = Morsels{ .mem = .{ .batches = ms.batches.items, .schema = held_schema } };
    const joins = try env.arena.dupe(ast.Stage, shape.joins[si..]);
    for (joins) |*st| if (st.node == .join) {
        st.node.join.own_cols = &.{};
    };
    const rest = SinkShape{ .prefix = &.{}, .joins = joins, .agg = shape.agg, .tail = shape.tail };
    if (!try runParallelAgg(env, &m, null, rest, w, opts, stats, lanes_used)) return false;
    if (env.profile) |p| {
        const what = try msgfmt.allocPrint(env.arena, "the joins before the semi and anti joins ({d} rows)", .{ms.rows});
        p.path = try msgfmt.allocPrint(env.arena, "{s} read into memory first, then {s}, their build sides read for the held keys", .{ what, p.path });
        try p.describe(profile.keyOf(pos), .scan, try msgfmt.allocPrint(env.arena, "{s}, in memory", .{what}));
    }
    return true;
}

const Pick = struct { at: usize, rows: u64 };

/// The join of `shape` a turn takes, with its right side's estimated rows: of the joins
/// whose right side is a local CSV or Parquet file read through map stages, the one of
/// the most rows whose later joins all index sides of known rows under its cap and
/// key on the turned side or a join after it (`cutsHead`).
fn turnedJoin(env: *Env, shape: Turn) !?Pick {
    const consumer = try shape.consumer(env.arena);
    var best: ?Pick = null;
    var later_max: u64 = 0;
    var later_unknown = false;
    var i = shape.joins.len;
    while (i > 0) {
        i -= 1;
        const st = shape.joins[i];
        if (st.node != .join) continue;
        const after = try std.mem.concat(env.arena, ast.Stage, &.{ shape.joins[i + 1 ..], consumer });
        const rst = try rightStagesFor(env, st.node.join, after);
        const est = estimate.ofSide(env, rst).rows;
        if (est) |n| if (joinorder.flippable(st) and fileSide(rst) and !later_unknown and later_max <= n / joinorder.flip_ratio and !cutsHead(shape.joins[i..])) {
            if (best == null or n > best.?.rows) best = .{ .at = i, .rows = n };
        };
        if (est) |n| later_max = @max(later_max, n) else later_unknown = true;
    }
    return best;
}

/// Whether a join after `span[0]` keys only on the rows held before it: no key names
/// `span[0]`'s side or a join between, by its qualifier. As written its runtime filter
/// cut the head (q18's IN, on `o.o_orderkey`), which a turn of
/// `span[0]` would lose. A bare key could be either side's, and counts as held.
fn cutsHead(span: []const ast.Stage) bool {
    for (span[1..], 1..) |st, k| {
        if (st.node != .join) continue;
        const j = st.node.join;
        const reaches = for (j.left_keys) |q| {
            if (q.parts.len < 2) continue;
            const named = for (span[0..k]) |x| {
                if (x.node != .join) continue;
                const xj = x.node.join;
                if (std.mem.eql(u8, q.parts[0], if (xj.alias.len > 0) xj.alias else xj.binding)) break true;
            } else false;
            if (named) break true;
        } else false;
        if (!reaches) return true;
    }
    return false;
}

/// Whether `rst` is a local CSV or Parquet file read through map stages alone.
fn fileSide(rst: []const ast.Stage) bool {
    if (rst.len == 0 or rst[0].node != .read or rst[0].hints.len > 0) return false;
    const rd = rst[0].node.read;
    if (!connect.isLocalCsvRead(rd) and !connect.isParquetFileRead(rd)) return false;
    for (rst[1..]) |x| if (!analyze.isMap(x.node, false)) return false;
    return true;
}

/// The columns `rest` reads from what `pre` leaves, as select items written the way
/// `rest` names them, so `o.o_orderdate` keeps its side. A name qualified by a join in
/// `rest` is that join's; a bare one is kept when a read of `pre` has it and dropped
/// when only a read of `rest` does. Null when `rest` reads every column or a bare
/// name's side cannot be told from the files.
fn flipKeeps(env: *Env, pre: []const ast.Stage, rest: []const ast.Stage) !?[]const ast.SelectItem {
    var quals = std.array_list.Managed(ast.QualName).init(env.arena);
    var made = std.StringHashMap(void).init(env.arena);
    var later = std.array_list.Managed([]const u8).init(env.arena);
    var later_reads = std.array_list.Managed(?types.Schema).init(env.arena);
    walk: for (rest, 0..) |st, i| switch (st.node) {
        .filter => |e| try ast.fieldsOf(e, &quals),
        .join => |j| {
            try quals.appendSlice(j.left_keys);
            for (j.deferred) |d| {
                try ast.fieldsOf(d.a, &quals);
                try ast.fieldsOf(d.b, &quals);
                try made.put(d.left_name, {});
                try made.put(d.right_name, {});
            }
            if (j.residual) |e| try ast.fieldsOf(e, &quals);
            try later.append(if (j.alias.len > 0) j.alias else j.binding);
            try later_reads.append(try sideSchema(env, j, rest[i + 1 ..]));
        },
        .select => |items| {
            for (items) |it| switch (it) {
                .field => |q| try quals.append(q),
                .computed => |c| try ast.fieldsOf(c.expr, &quals),
                else => return null,
            };
            break :walk;
        },
        .aggregate => |ag| {
            if (ag.by_rest != null) return null;
            try quals.appendSlice(ag.by);
            for (ag.aggs) |a| if (a.arg) |e| try ast.fieldsOf(e, &quals);
            break :walk;
        },
        else => return null,
    };
    var own = std.array_list.Managed(?types.Schema).init(env.arena);
    if (pre[0].node == .read) try own.append(offlineSchema(env.arena, pre[0].node.read, pre[0].hints, env.tables));
    for (pre[1..], 1..) |st, i| if (st.node == .join) try own.append(try sideSchema(env, st.node.join, pre[i + 1 ..]));

    var seen = std.StringHashMap(void).init(env.arena);
    var out = std.array_list.Managed(ast.SelectItem).init(env.arena);
    next: for (quals.items) |q| {
        if (q.parts.len > 1) {
            for (later.items) |a| if (std.ascii.eqlIgnoreCase(a, q.parts[0])) continue :next;
        } else {
            if (made.contains(q.parts[0])) continue;
            switch (sideOf(own.items, q.parts[0])) {
                .yes => {},
                .unknown => return null,
                .no => switch (sideOf(later_reads.items, q.parts[0])) {
                    .yes => continue,
                    else => return null,
                },
            }
        }
        const key = try std.mem.join(env.arena, ".", q.parts);
        if ((try seen.getOrPut(key)).found_existing) continue;
        try out.append(.{ .field = q });
    }
    if (out.items.len == 0) return null;
    return try out.toOwnedSlice();
}

/// A joined side's columns when it is a file read with only filters and selects after
/// it (a subquery's renamed keys, as an EXISTS's).
fn sideSchema(env: *Env, j: ast.Join, after: []const ast.Stage) !?types.Schema {
    const rst = try rightStagesFor(env, j, after);
    if (rst.len == 0 or rst[0].node != .read) return null;
    for (rst[1..]) |x| if (x.node != .filter and x.node != .select) return null;
    const sch = offlineSchema(env.arena, rst[0].node.read, rst[0].hints, env.tables) orelse return null;
    for (rst[1..]) |x| if (x.node == .select) {
        var ad = analyze.Diag{};
        const ch = analyze.resolveChain(env.arena, sch, rst[1..], env.params_expr, &ad) catch return null;
        return ch.out.*;
    };
    return sch;
}

fn sideOf(schemas: []const ?types.Schema, name: []const u8) enum { yes, no, unknown } {
    var all_known = true;
    for (schemas) |sc| {
        const s = sc orelse {
            all_known = false;
            continue;
        };
        if (s.indexOf(name) != null) return .yes;
    }
    return if (all_known) .no else .unknown;
}

fn runOutputBody(env: *Env, opts_in: RunOptions, stages_in: []const ast.Stage, last: @FieldType(ast.Stage, "node"), stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!void {
    var opts = opts_in;
    const api = callsApi(env, stages_in, 0);
    if (api) opts.threads = 1;
    const arena = env.arena;
    const gpa = env.gpa;
    var stages = stages_in;
    const outer_pipe = env.pipe;
    env.pipe = .{ .threads = opts.lanes() };
    defer {
        if (env.pipe.pq_held) |h| h.src.close();
        env.pipe = outer_pipe;
    }
    const pipe = &env.pipe;
    const outer_stop = env.stop;
    defer env.stop = outer_stop;
    env.stop = if (env.union_sink != null) outer_stop else if (env.max_rows != null and last == .write and std.mem.eql(u8, last.write.connector, "stdout")) blk: {
        const f = try arena.create(std.atomic.Value(bool));
        f.* = .init(false);
        break :blk f;
    } else null;

    const api_held = if (api) try materializeApiBindings(env, opts, stages, lanes_used, batch_arena) else &.{};
    defer for (api_held) |name| {
        _ = env.materialized.remove(name);
    };
    var run_first = std.array_list.Managed([]const u8).init(env.arena);
    defer for (run_first.items) |name| {
        _ = env.materialized.remove(name);
    };
    var cut_sides = std.array_list.Managed([]const u8).init(env.arena);
    defer for (cut_sides.items) |name| {
        _ = env.bindings.remove(name);
    };
    var swapped = std.array_list.Managed(Swap).init(env.arena);
    defer {
        var k = swapped.items.len;
        while (k > 0) {
            k -= 1;
            if (env.bindings.getPtr(swapped.items[k].name)) |b| b.* = swapped.items[k].was;
        }
    }
    if (opts.lanes() > 1) {
        stages = try reduceJoinSides(env, stages, &cut_sides);
        try reduceNestedSides(env, stages, &cut_sides, &swapped, 0);
    }
    if (try unionToMemory(env, opts, stages, lanes_used, batch_arena, &run_first)) |s| stages = s;
    _ = try materializeBinding(env, opts, stages, lanes_used, batch_arena, &run_first);
    try materializeJoinSides(env, opts, stages, lanes_used, batch_arena, &run_first);
    stages = try inlineHeadBindings(env, stages);
    const wtk = try windowTopKLanes(env, opts, stages, lanes_used);
    defer if (wtk != null) {
        _ = env.materialized.remove(window_topk);
    };
    if (wtk) |w| stages = w;
    if (stages[0].node == .read and stages[0].node.read.form == .path and ftp.isUrl(stages[0].node.read.form.path)) {
        const copy = try env.arena.dupe(ast.Stage, stages);
        copy[0].node.read = try localRead(env, copy[0].node.read);
        stages = copy;
    }

    var ddiag = analyze.Diag{};
    analyze.checkJoinHints(env.arena, stages, &ddiag) catch |e| return aErr(env, &ddiag, e);
    env.csv_in = analyze.dialectFromHints(stages[0].hints, &ddiag) catch |e| return aErr(env, &ddiag, e);
    env.csv_out = analyze.dialectFromHints(stages[stages.len - 1].hints, &ddiag) catch |e| return aErr(env, &ddiag, e);
    env.fmt_in = analyze.formatFromHints(stages[0].hints, &ddiag) catch |e| return aErr(env, &ddiag, e);
    env.fmt_out = analyze.formatFromHints(stages[stages.len - 1].hints, &ddiag) catch |e| return aErr(env, &ddiag, e);
    env.write_opts = analyze.writeOptions(arena, last.write, stages[stages.len - 1].hints, &ddiag) catch |e| return aErr(env, &ddiag, e);
    env.cutover = analyze.cutoverProblem(last.write, stages[stages.len - 1].hints, &ddiag) catch |e| return aErr(env, &ddiag, e);
    if (stages[0].node == .read and stages[0].node.read.form == .path and
        std.mem.eql(u8, stages[0].node.read.connector, "csv"))
        try guardFileFormat(env, stages[0].node.read.form.path, env.fmt_in, "read");
    if (std.mem.eql(u8, last.write.connector, "csv") and last.write.target.len > 0)
        try guardFileFormat(env, last.write.target, env.fmt_out, "write");
    const is_file_sink = std.mem.eql(u8, last.write.connector, "csv") or std.mem.eql(u8, last.write.connector, "stdout");
    if (is_file_sink)
        analyze.checkFileSink(last.write, stages[stages.len - 1].hints, &ddiag) catch |e| return aErr(env, &ddiag, e);
    const sink_connector = if (is_file_sink) last.write.connector else if (env.connections.get(last.write.connector)) |c| c.connector else last.write.connector;
    analyze.checkSinkForm(last.write, stages[stages.len - 1].hints, sink_connector, &ddiag) catch |e| return aErr(env, &ddiag, e);
    env.table_lock = analyze.tableLockProblem(last.write, stages[stages.len - 1].hints, sink_connector, &ddiag) catch |e| return aErr(env, &ddiag, e);
    if (env.table_lock and readsConnection(env, stages, last.write.connector, 0)) env.table_lock = false;
    if (!is_file_sink) if (env.connections.get(last.write.connector)) |c|
        analyze.checkSinkConnection(arena, c, &ddiag) catch |e| return aErr(env, &ddiag, e);
    analyze.checkBareUpsert(stages, env.connections, &ddiag) catch |e| return aErr(env, &ddiag, e);
    env.sink_label_prefix = analyze.hintText(stages[stages.len - 1].hints, "label_prefix");

    stages = try analyze.substFilterParams(arena, stages, env.params_expr);

    if (try pushdown.hoistFilters(arena, env.gpa, stages, env.bindings, env.tables)) |hoisted| {
        env.log.log(.debug, "filter hoisted through a projection or join: {d} -> {d} stages", .{ stages.len, hoisted.len });
        stages = hoisted;
    }
    stages = try pruneKnown(env, stages);

    if (stages[0].node == .read or stages[0].node == .union_) {
        var extra = std.array_list.Managed(ast.Hint).init(arena);
        if (last.write.split) |sp| {
            try extra.append(.{ .key = "split", .value = .{ .ident = sp.col }, .pos = sp.pos });
            if (sp.jobs) |n| {
                opts.threads = n;
                pipe.threads = opts.lanes();
            }
        }
        if (extra.items.len > 0) {
            const with = try arena.dupe(ast.Stage, stages);
            try extra.appendSlice(stages[0].hints);
            with[0].hints = try extra.toOwnedSlice();
            stages = with;
        }
    }

    stages = try projectSqlRead(env, stages);

    stages = try descendLeadingWhere(env, stages);

    var head_stages = stages;
    var inlined: usize = 0;
    while (head_stages[0].node == .ref and inlined < 16) : (inlined += 1) {
        if (env.materialized.contains(head_stages[0].node.ref)) break;
        const b = (try stampedBinding(env, head_stages[0].node.ref)) orelse break;
        if (b.stages.len == 0) break;
        const joined = try arena.alloc(ast.Stage, b.stages.len + head_stages.len - 1);
        @memcpy(joined[0..b.stages.len], b.stages);
        @memcpy(joined[b.stages.len..], head_stages[1..]);
        head_stages = joined;
    }
    if (inlined > 0) {
        if (try pushdown.hoistFilters(arena, env.gpa, head_stages, env.bindings, env.tables)) |h| head_stages = h;
        head_stages = try pruneKnown(env, head_stages);
    }
    const path_facts: path.Facts = .{
        .lanes = opts.lanes(),
        .sink = last.write,
        .source = path.sourceOf(stages[0], env.connections),
        .head_source = path.sourceOf(head_stages[0], env.connections),
    };
    if (env.union_sink == null and path.chunkCopy(stages, path_facts))
        if (try chunk_copy.run(env, stages, last.write, stats)) return;
    const chosen = try path.choose(arena, stages, head_stages, path_facts);
    if (chosen == .serial and env.union_sink == null) if (try memLanes(env, opts, stages, last.write, stats, lanes_used)) return;
    if (chosen == .union_split) {
        return runUnionSplit(env, stages[0].node.union_, stages[0].hints, stages[1 .. stages.len - 1], stages[stages.len - 1], opts, stats, lanes_used, batch_arena);
    }
    if (env.profile) |p| {
        try notePlan(env, p, head_stages);
        try notePlan(env, p, stages);
    }
    if (chosen == .lanes and !try keysPreferSerial(env, head_stages)) {
        const ln = chosen.lanes;
        if (ln.shape == .sink) if (try flipLaterJoin(env, head_stages, .ofAgg(ln.shape.sink), last.write, opts, stats, lanes_used, batch_arena)) return;
        if (ln.shape == .sink) if (try holdBeforeSemis(env, head_stages, ln.shape.sink, last.write, opts, stats, lanes_used, batch_arena)) return;
        if (ln.shape == .map_join) if (heldTurn(env, head_stages)) |t| if (try flipLaterJoin(env, head_stages, t, last.write, opts, stats, lanes_used, batch_arena)) return;
        if (ln.source == .parquet) {
            if (try runParquetLane(env, head_stages, ln.shape, last.write, opts, stats, lanes_used)) return;
        } else if (try runCsvLane(env, head_stages, ln.shape, last.write, opts, stats, lanes_used)) return;
    }
    if (chosen == .serial) if (path.oneLaneTurn(head_stages, path_facts)) |sh| if (try flipLaterJoin(env, head_stages, .ofAgg(sh), last.write, opts, stats, lanes_used, batch_arena)) return;
    if (chosen == .serial) if (heldTurn(env, head_stages)) |t| if (try flipLaterJoin(env, head_stages, t, last.write, opts, stats, lanes_used, batch_arena)) return;
    const sql: ?path.Sql = if (chosen == .sql) chosen.sql else null;

    pipe.sql_desc = null;
    env.src_name = "";
    const src_base = env.sources.items.len;
    const t_open = std.time.Instant.now() catch null;

    var whole_agg = false;
    var top_n = false;
    const lead_desc = try sql_read.sqlDescForStage(env, stages[0]);
    if (lead_desc) |desc| {
        const agg_shape: ?SinkShape = if (sql) |s| s.whole_agg else null;
        var cwhy: []const u8 = "";
        const topn_shaped = (try pushdown.classifyTopN(arena, stages[0 .. stages.len - 1], &cwhy)) != null;
        const schema: ?types.Schema = if (agg_shape != null or topn_shaped or hasSort(stages)) blk: {
            var probe = stages[0].node.read;
            probe.where = "1 = 0";
            break :blk sql_read.probedSchema(env, probe, stages[0].hints) catch null;
        } else null;
        var why: []const u8 = "the pipeline is not read -> filter* -> aggregate";
        if (agg_shape) |shape| if (schema) |sch| if (try wholeAggStages(env, stages, shape, desc, sch, &why)) |ns| {
            stages = ns;
            whole_agg = true;
            if (env.profile) |p| p.path = "serial; the aggregate runs in the source";
        };
        if (!whole_agg and hasAggregate(stages))
            env.log.log(.warn, "aggregate over {s} runs engine-side, not in the source ({s}); every matching row is streamed here to be grouped", .{ @tagName(desc.dialect), why });
        if (!whole_agg) if (schema) |sch| {
            var twhy: []const u8 = "";
            if (try topNStages(env, stages, desc, sch, &twhy)) |ns| {
                stages = ns;
                top_n = true;
                if (env.profile) |p| p.path = "serial; ORDER BY ... LIMIT runs in the source";
            } else if (twhy.len > 0 and hasSort(stages))
                env.log.log(.warn, "ORDER BY ... LIMIT over {s} sorts engine-side, not in the source ({s}); every matching row is streamed here to be sorted", .{ @tagName(desc.dialect), twhy });
        };
    }

    const may_split = !whole_agg and !top_n and opts.lanes() > 1 and lead_desc != null and sql_read.splitMayCutInto(env, stages[0], last.write);
    var res = if (may_split) try buildPipelineLateLead(env, stages[0 .. stages.len - 1]) else try buildPipeline(env, stages[0 .. stages.len - 1]);
    defer res.op.release();
    if (env.profile) |p| if (t_open) |t| try p.phase("plan and open the sources", (std.time.Instant.now() catch t).since(t));

    const wr = try resolveUpsertKeys(env, last.write);

    if (!whole_agg and !top_n and opts.lanes() > 1 and pipe.sql_desc != null) {
        if (sql) |s| {
            const ran = switch (s.split) {
                .agg => |shape| try runParallelSqlAgg(env, pipe, stages, shape, wr, opts, stats, lanes_used, src_base),
                .map_join => |js| try runParallelSqlMapJoin(env, stages, js, wr, opts, stats, lanes_used, src_base),
                .map, .none => false,
            };
            if (ran) return;
            if (env.sources.items.len == src_base) {
                pipe.sql_desc = null;
                res.op.release();
                res = try buildPipeline(env, stages[0 .. stages.len - 1]);
            }
        }
        if (try op.linearize(arena, res.op)) |lin| {
            if (try planSplit(env, pipe.sql_desc.?, stages[0], opts.lanes(), wr)) |sp| {
                const schema = try dupeSchema(arena, res.schema);

                const middle = stages[1 .. stages.len - 1];
                var lane_stages = lin.stages;
                var proj_select: ?[]const u8 = null;
                var where_extra: ?[]const u8 = null;
                var want: ?*const types.Schema = null;
                if (stages[0].node == .read) {
                    const src_schema = try dupeSchema(arena, env.sources.items[src_base].schema());
                    want = try schemaPtr(arena, src_schema);
                    const out_cols = try arena.alloc([]const u8, schema.fields.len);
                    for (schema.fields, out_cols) |f, *o| o.* = f.name;
                    const facts = try factsIfWanted(env, stages[0].node.read, pipe.sql_desc.?.dialect, middle, src_schema, true, .superset);
                    const mp = try pushdown.planMap(arena, pipe.sql_desc.?.dialect, src_schema, middle, out_cols, facts);
                    where_extra = mp.where_extra;
                    if (mp.proj_schema) |ps| {
                        if (rebuildMapStages(env, mp.stages orelse middle, try schemaPtr(arena, ps))) |rs| {
                            lane_stages = rs;
                            proj_select = mp.proj_select;
                            want = try schemaPtr(arena, ps);
                        }
                    }
                }

                for (env.sources.items[src_base..]) |sc| sc.close();
                env.sources.shrinkRetainingCapacity(src_base);
                var ctx = SplitCtx{ .gpa = gpa, .kind = pipe.sql_desc.?.kind, .cfg = pipe.sql_desc.?.cfg, .base_sql = sp.base_sql, .proj_select = proj_select, .where_extra = where_extra, .report = try readReport(env, @tagName(pipe.sql_desc.?.kind)), .want = want, .reach = sp.reach };
                const ranges = if (sp.reach.inner) "page ranges" else "key ranges";
                lanes_used.* = @max(lanes_used.*, @min(opts.lanes(), sp.predicates.len));
                env.log.log(.debug, "split-parallel: {d} {s} over {d} lanes (projection: {s}, filter pushdown: {s})", .{ sp.predicates.len, ranges, @min(opts.lanes(), sp.predicates.len), proj_select orelse "all", if (where_extra != null) "yes" else "no" });
                const profiled: ?parallel.Profiled = if (env.profile) |p| blk: {
                    const root = if (lane_stages.len > 0) lane_stages[lane_stages.len - 1].asOp() else res.op;
                    const ids = try arena.alloc(profile.Id, lane_stages.len + 1);
                    p.path = try msgfmt.allocPrint(arena, "{s} split-parallel, {d} {s}", .{ @tagName(pipe.sql_desc.?.kind), sp.predicates.len, ranges });
                    const chain = p.chainIds(try p.shape(root), ids);
                    try p.splitRead(chain[chain.len - 1], try ctx.sentNote(arena));
                    break :blk .{ .prof = p, .ids = chain };
                } else null;
                const mode = (try buildParallelSink(env, wr, schema)) orelse parallel.SinkMode{ .shared = try openSink(env, wr, schema) };
                var guard = parallel.ModeGuard.init(mode);
                defer guard.abort();
                stats.rows_out += try parallel.run(gpa, sp.predicates, openSplitSource, &ctx, lane_stages, mode, opts.lanes(), env.rows_read, profiled);
                try guard.close();
                return;
            }
        }
    }

    if (hasFlagHint(stages[0].hints, "buffer")) {
        var held = std.heap.ArenaAllocator.init(gpa);
        defer held.deinit();
        var batches = std.array_list.Managed(Batch).init(held.allocator());
        while (true) {
            if (aborting()) return error.Aborted;
            const b = (try res.op.next(held.allocator())) orelse break;
            try batches.append(b);
        }
        for (env.sources.items[src_base..]) |sc| sc.close();
        env.sources.shrinkRetainingCapacity(src_base);

        const snk = try openSink(env, wr, res.schema);
        var guard = SinkGuard.init(snk);
        defer guard.abort();
        for (batches.items) |b| {
            if (aborting()) return error.Aborted;
            try snk.writeBatch(batch_arena.allocator(), b);
            stats.rows_out += b.len;
            _ = batch_arena.reset(.retain_capacity);
        }
        try guard.close();
        if (env.profile) |p| _ = try p.absorb(res.op);
        return;
    }

    const snk = try openSink(env, wr, res.schema);
    var guard = SinkGuard.init(snk);
    defer guard.abort();

    if (snk.unitRows() > 0 and opts.lanes() > 1) {
        var up: UnitPipeline = undefined;
        try up.start(arena, gpa, snk, opts.lanes() - 1);
        defer up.shutdown();
        while (true) {
            if (aborting()) return error.Aborted;
            const b = (try res.op.next(batch_arena.allocator())) orelse break;
            try up.submit(b);
            stats.rows_out += b.len;
            _ = batch_arena.reset(.retain_capacity);
        }
        try up.finish();
        try guard.close();
        if (env.profile) |p| _ = try p.absorb(res.op);
        return;
    }

    var ping_pong: [2]std.heap.ArenaAllocator = .{ std.heap.ArenaAllocator.init(gpa), std.heap.ArenaAllocator.init(gpa) };
    defer for (&ping_pong) |*a| a.deinit();
    var pw = parallel.PipelinedSink{ .snk = snk, .gpa = gpa };
    try pw.start();
    defer pw.shutdown();
    var cur: usize = 0;
    while (true) {
        if (aborting()) return error.Aborted;
        const b = (try res.op.next(ping_pong[cur].allocator())) orelse break;
        try pw.submit(try b.ownTypes(ping_pong[cur].allocator()));
        stats.rows_out += b.len;
        cur ^= 1;
        _ = ping_pong[cur].reset(.retain_capacity);
    }
    try pw.finish();
    try guard.close();
    if (env.profile) |p| _ = try p.absorb(res.op);
}

/// Split-parallel union: each branch runs as its own split pipeline. A target with
/// per-lane sinks takes each branch on its own, the first keeping the write mode (so
/// `overwrite` truncates once) and later ones appending; any other is opened once, on
/// the union's schema, and every branch writes to it, so the union is one result.
fn runUnionSplit(env: *Env, u: ast.Union, hints: []const ast.Hint, downstream: []const ast.Stage, write_stage: ast.Stage, opts: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!void {
    const arena = env.arena;
    const tag_col = forHintIdent(hints, "tag");
    const canon_opt = @import("../env.zig").unionSchemaFrom(hints);
    const specs = try unionSpecs(env, u, hints);
    if (specs.len == 0) return planErr(env.diag, "union has no source tables");
    const canon_types = try canonOf(env, specs, canon_opt);
    if (!u.positional) for (specs) |*s| if (s.pipeline == null) {
        s.read.where = try branchWhere(env, s.read, downstream, tag_col, canon_types);
    };

    const except = try unionExceptNames(env.arena, downstream);
    const schemas = try arena.alloc(types.Schema, specs.len);
    const probes = try arena.alloc(driver.Source, specs.len);
    var opened: usize = 0;
    defer for (probes[0..opened]) |p| p.close();
    try @import("../plan/union.zig").narrowBranches(env, u, specs, hints, downstream);
    for (specs, schemas, probes) |*s, *sch, *p| {
        if (except.len > 0 and s.read.cols.len == 0) {
            if (try exceptColumns(env, s.read, hints, except)) |cols| s.read.cols = cols;
        }
        var probe = s.read;
        probe.where = "1 = 0";
        p.* = try openSource(env, probe, hints);
        opened += 1;
        sch.* = try dupeSchema(arena, p.schema());
    }
    const canon = try unionCanon(env, specs, schemas, canon_opt, except);
    const w = write_stage.node.write;
    const outer_sink = env.union_sink;
    defer env.union_sink = outer_sink;
    var shared: SinkGuard = .empty;
    defer shared.abort();
    if (!opensPerLane(env, w)) {
        var out_fields: []types.Schema.Field = &.{};
        for (specs, schemas, probes, 0..) |s, sch, p, i| {
            const scan = try arena.create(op.Scan);
            scan.* = .{ .src = p };
            var r = try buildProject(env, try synthReconcile(arena, sch, canon, tag_col, s.tag), sch, .{ .scan = scan });
            for (downstream) |st| r = try buildStage(env, st, r.op, r.schema);
            r.op.release();
            if (i == 0) {
                out_fields = try arena.dupe(types.Schema.Field, r.schema.fields);
            } else for (out_fields, r.schema.fields) |*f, b| {
                f.ty.nullable = f.ty.nullable or b.ty.nullable;
            }
        }
        const snk = try openSink(env, w, .{ .fields = out_fields });
        shared.arm(snk);
        env.union_sink = try keptOpen(arena, snk);
    }
    for (probes[0..opened]) |p| p.close();
    opened = 0;

    var split_hints = std.array_list.Managed(ast.Hint).init(arena);
    for (hints) |h| {
        if (std.mem.eql(u8, h.key, "split") or std.mem.eql(u8, h.key, "splits") or std.mem.eql(u8, h.key, "split_kind"))
            try split_hints.append(h);
    }
    const branch_hints = try split_hints.toOwnedSlice();

    const outer_scope = if (env.profile) |p| p.scope else 0;
    defer if (env.profile) |p| {
        p.scope = outer_scope;
    };
    const outer_branch = env.union_branch;
    env.union_branch = true;
    defer env.union_branch = outer_branch;
    for (specs, schemas, 0..) |s, sch, i| {
        if (env.stop) |f| if (f.load(.monotonic)) break;
        if (env.profile) |p| p.scope = std.hash.Wyhash.hash(outer_scope, std.mem.asBytes(&i));
        const items = try synthReconcile(arena, sch, canon, tag_col, s.tag);
        var bstages = std.array_list.Managed(ast.Stage).init(arena);
        try bstages.append(.{ .node = .{ .read = s.read }, .hints = branch_hints, .pos = u.pos });
        try bstages.append(.{ .node = .{ .select = items }, .hints = &.{}, .pos = u.pos });
        try bstages.appendSlice(downstream);
        const bmode: ast.WriteMode = if (i == 0 or w.mode != .overwrite) w.mode else .append;
        var bw = w;
        bw.mode = bmode;
        bw.split = null;
        try bstages.append(.{ .node = .{ .write = bw }, .hints = write_stage.hints, .pos = write_stage.pos });
        try runOutput(env, .{ .stages = try bstages.toOwnedSlice(), .pos = u.pos }, opts, stats, lanes_used, batch_arena);
    }
    try shared.close();
    if (env.profile) |p| p.path = try msgfmt.allocPrint(arena, "union split-parallel, {d} branches", .{specs.len});
}

/// `s` with close and abort doing nothing, for the branches of a union whose
/// owner closes it once.
fn keptOpen(arena: std.mem.Allocator, s: driver.Sink) !driver.Sink {
    const vt = try arena.create(driver.Sink.VTable);
    vt.* = s.vtable.*;
    vt.close = struct {
        fn f(_: *anyopaque) anyerror!void {}
    }.f;
    vt.abort = struct {
        fn f(_: *anyopaque) void {}
    }.f;
    return .{ .ptr = s.ptr, .vtable = vt };
}
