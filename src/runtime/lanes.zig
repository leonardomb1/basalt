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

const std = @import("std");
const ast = @import("../lang/ast.zig");
const types = @import("../lang/types.zig");
const op = @import("../exec/op.zig");
const Batch = @import("../exec/batch.zig").Batch;
const column = @import("../exec/column.zig");
const eval = @import("../exec/eval.zig");
const csv = @import("../format/csv.zig");
const pqdecode = @import("../format/pqdecode.zig");
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

const WorkQueue = struct {
    nitems: usize,
    next: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    err_mtx: std.Thread.Mutex = .{},
    first_err: ?anyerror = null,
    failed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    note_buf: [480]u8 = undefined,
    note_len: usize = 0,

    fn fail(q: *WorkQueue, e: anyerror) void {
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

    fn failure(q: *WorkQueue) ?anyerror {
        const e = q.first_err orelse return null;
        return if (q.note_len > 0) eval.explain(e, q.note_buf[0..q.note_len]) else e;
    }
};

/// The worker loop shared by the parallel CSV/SQL paths: steal the next item off
/// `ctx.queue`, run `workOne(ctx, i)`, and latch the first error, which stops the others.
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

const AggSlot = struct {
    gpa: std.heap.GeneralPurposeAllocator(.{ .thread_safe = false }) = .{},
    arena: std.heap.ArenaAllocator = undefined,
    parts: LaneParts = undefined,
    sets: []const op.Aggregate.GroupSet = &.{},

    fn arm(self: *AggSlot, shared: std.mem.Allocator) void {
        self.arena = std.heap.ArenaAllocator.init(self.gpa.allocator());
        self.parts.init(shared);
    }
};

const LaneParts = struct {
    arenas: [pq_parts]std.heap.ArenaAllocator,
    allocs: [pq_parts]std.mem.Allocator,

    fn init(self: *LaneParts, child: std.mem.Allocator) void {
        for (&self.arenas, &self.allocs) |*a, *al| {
            a.* = std.heap.ArenaAllocator.init(child);
            al.* = a.allocator();
        }
    }

    fn deinit(self: *LaneParts) void {
        for (&self.arenas) |*a| a.deinit();
    }

    /// Release partition `p` once merged, leaving a fresh empty arena so `deinit` stays unconditional.
    fn free(self: *LaneParts, p: usize) void {
        self.arenas[p].deinit();
        self.arenas[p] = std.heap.ArenaAllocator.init(self.backing());
    }

    fn backing(self: *const LaneParts) std.mem.Allocator {
        return self.arenas[0].child_allocator;
    }
};

fn allocAggSlots(gpa: std.mem.Allocator, n: usize) ![]AggSlot {
    const slots = try gpa.alloc(AggSlot, n);
    for (slots) |*s| s.* = .{};
    for (slots) |*s| s.arm(gpa);
    return slots;
}

fn freeAggSlots(gpa: std.mem.Allocator, slots: []AggSlot) void {
    for (slots) |*s| {
        for (s.sets) |*st| st.freeTable();
        s.parts.deinit();
        s.arena.deinit();
        _ = s.gpa.deinit();
    }
    gpa.free(slots);
}

pub const agg_combine_parallel_min: usize = 1 << 14;

/// Combine per-item partials into one group set, walking the slots by index so a
/// float SUM adds in the same order every run; radix-partitioned past
/// `agg_combine_parallel_min`. `parts` is the caller's: the merged groups outlive the write.
fn combineAggSlots(
    env: *Env,
    slots: []AggSlot,
    aggs: []const op.Aggregate.Agg,
    threads: usize,
    parts: []PqPart,
) ![]const op.Aggregate.GroupSet {
    var total: usize = 0;
    for (slots) |*s| {
        for (s.sets) |st| total += st.len;
    }

    if (threads < 2 or total < agg_combine_parallel_min) {
        var m: ?op.Aggregate.GroupMerge = null;
        for (slots) |*s| {
            for (s.sets) |*st| {
                if (m == null) m = try op.Aggregate.GroupMerge.init(env.arena, st, aggs);
                for (0..st.len) |i| try m.?.add(st, i);
            }
        }
        const one = try env.arena.alloc(op.Aggregate.GroupSet, if (m == null) 0 else 1);
        if (m) |*mm| one[0] = mm.result();
        return one;
    }

    var mctx = SlotMergeCtx{ .slots = slots, .parts = parts, .aggs = aggs, .queue = .{ .nitems = pq_parts } };
    _ = try parallel.spawnJoin(env.arena, @min(threads, pq_parts), slotMergeWorker, &mctx);
    if (mctx.queue.failure()) |e| return e;

    env.log.log(.debug, "parallel agg combine: {d} partial groups over {d} partitions", .{ total, pq_parts });

    return partSets(env.arena, parts);
}

fn partSets(a: std.mem.Allocator, parts: []PqPart) ![]const op.Aggregate.GroupSet {
    var sets = std.array_list.Managed(op.Aggregate.GroupSet).init(a);
    for (parts) |*pp| {
        if (pp.merge) |*m| try sets.append(m.result());
    }
    return sets.items;
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

/// The radix partitions, owned by the caller so merged groups outlive the sink write.
/// Free when unused: an arena allocates nothing until used.
fn allocMergeParts(gpa: std.mem.Allocator) ![]PqPart {
    const parts = try gpa.alloc(PqPart, pq_parts);
    for (parts) |*pp| pp.* = .{ .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator) };
    return parts;
}

fn freeMergeParts(gpa: std.mem.Allocator, parts: []PqPart) void {
    for (parts) |*pp| {
        if (pp.merge) |*m| m.deinit();
        pp.arena.deinit();
    }
    gpa.free(parts);
}

fn groupEmitter(env: *Env, agg_in: *const types.Schema, by: []const usize, aggs: []const op.Aggregate.Agg, out_schema: *const types.Schema) op.Aggregate {
    return .{
        .child = undefined,
        .in_schema = agg_in,
        .by = by,
        .aggs = aggs,
        .out_schema = out_schema,
        .state = env.arena,
        .gpa = env.gpa,
    };
}

/// Emit merged groups and write them through `tail`: a partition at a time across lanes
/// when the tail only filters and projects, else as one batch (which had been a third
/// of a high-cardinality GROUP BY at -j 8).
fn writeGroups(
    env: *Env,
    snk: driver.Sink,
    emitter: op.Aggregate,
    sets: []const op.Aggregate.GroupSet,
    sel: ?[]const []const u32,
    tail: []const ast.Stage,
    threads: usize,
    stats: *Stats,
) !void {
    var total: usize = 0;
    for (sets) |st| total += st.len;
    const rowwise = for (tail) |st| switch (st.node) {
        .filter, .select => {},
        else => break false,
    } else true;
    if (sel != null or emitter.by.len == 0 or threads < 2 or sets.len < 2 or total < agg_combine_parallel_min or !rowwise) {
        var em = emitter;
        const batch = try em.emitSets(env.arena, sets, sel);
        return writeTail(env, snk, batch, emitter.out_schema.*, tail, stats);
    }

    var ord = try OrderedOut.init(env.arena, sets.len, 2 * threads, snk);
    defer ord.freeRest();
    var ctx = GroupWriteCtx{
        .emitter = emitter,
        .sets = sets,
        .tail = tail,
        .params = env.params_expr,
        .errctx = env.errctx,
        .queue = .{ .nitems = sets.len },
        .ord = &ord,
    };
    _ = try parallel.spawnJoin(env.arena, @min(threads, sets.len), groupWriteWorker, &ctx);
    if (ctx.queue.failure()) |e| return e;
    stats.rows_out += ctx.rows_out.load(.monotonic);
}

const GroupWriteCtx = struct {
    emitter: op.Aggregate,
    sets: []const op.Aggregate.GroupSet,
    tail: []const ast.Stage,
    params: *std.StringHashMap(*const ast.Expr),
    errctx: ?*op.ErrCtx = null,
    queue: WorkQueue,
    ord: *OrderedOut,
    rows_out: std.atomic.Value(u64) = .init(0),
};

fn groupWriteWorker(ctx: *GroupWriteCtx, _: usize) void {
    var wgpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = false }){};
    defer _ = wgpa.deinit();
    var warena = std.heap.ArenaAllocator.init(wgpa.allocator());
    defer warena.deinit();
    while (true) {
        if (ctx.queue.failed.load(.seq_cst)) return;
        const i = ctx.queue.next.fetchAdd(1, .seq_cst);
        if (i >= ctx.queue.nitems) break;
        groupWriteUnit(ctx, i, warena.allocator()) catch |e| {
            ctx.queue.fail(e);
            ctx.ord.wake();
            return;
        };
        _ = warena.reset(.retain_capacity);
    }
}

fn groupWriteUnit(ctx: *GroupWriteCtx, i: usize, wa: std.mem.Allocator) !void {
    const ord = ctx.ord;
    ord.waitTurn(i, &ctx.queue.failed);
    if (ctx.queue.failed.load(.seq_cst)) return;

    const unit = try ord.takeUnit();
    var handed = false;
    defer if (!handed) OrderedOut.freeUnit(unit);
    const ua = unit.arena.allocator();
    const snk = ord.snk;
    const enc: ?driver.UnitEncoder = if (snk.openUnit(ua)) |r| try r else null;
    errdefer if (enc) |e| e.discard();

    var em = ctx.emitter;
    em.state = wa;
    const out_schema = ctx.emitter.out_schema;
    var ob = OneBatch{ .b = try em.emitSets(wa, ctx.sets[i..][0..1], null), .sch = out_schema.* };
    var scan = op.Scan{ .src = ob.source() };
    const chain = try buildMapChain(wa, ctx.params, ctx.errctx, ctx.tail, &scan, out_schema);
    var out: u64 = 0;
    while (try chain.next(wa)) |b| {
        if (b.len == 0) continue;
        if (enc) |e| {
            try e.write(wa, b);
        } else try unit.parts.append(if (snk.canRender())
            .{ .bytes = try snk.renderBatch(ua, b).? }
        else
            .{ .batch = try b.deepCopy(ua) });
        out += b.len;
    }
    if (enc) |e| {
        try e.seal();
        try unit.parts.append(.{ .encoded = e });
    }
    _ = ctx.rows_out.fetchAdd(out, .monotonic);
    handed = true;
    try ord.deposit(i, unit);
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
    var i: usize = 0;
    while (i < tail.len) : (i += 1) {
        const r = if (tail[i].node == .sort and i + 1 < tail.len and tail[i + 1].node == .limit) blk: {
            i += 1;
            break :blk try buildTopN(env, tail[i - 1].node.sort, tail[i].node.limit, cur, sch);
        } else try buildStage(env, tail[i], cur, sch);
        cur = r.op;
        sch = r.schema;
    }
    while (try cur.next(arena)) |outb| {
        try snk.writeBatch(arena, outb);
        stats.rows_out += outb.len;
    }
}

const AggCtx = struct {
    strs: *op.Aggregate.StrTable,
    mapped: *csv.MappedCsv,
    csv_schema: *const types.Schema,
    agg_in_schema: *const types.Schema,
    out_schema: *const types.Schema,
    prefix: []const ast.Stage,
    params: *std.StringHashMap(*const ast.Expr),
    errctx: ?*op.ErrCtx = null,
    by: []const usize,
    aggs: []const op.Aggregate.Agg,
    queue: WorkQueue,
    slots: []AggSlot,
    rows_read: *obs.RowCounter,
    joins: []const LaneJoin = &.{},
};

const aggWorker = dispatchWorker(AggCtx, aggWorkOne);

fn aggWorkOne(ctx: *AggCtx, i: usize) !void {
    const slot = &ctx.slots[i];
    const wa = slot.arena.allocator();

    var reader = csv.CsvSliceReader{ .data = ctx.mapped.chunk(i, ctx.queue.nitems), .schema = ctx.csv_schema, .dialect = ctx.mapped.dialect, .slot = ctx.mapped.slot };
    var cs = obs.CountingSource{ .inner = reader.source(), .count = ctx.rows_read };
    var scan = op.Scan{ .src = cs.source() };
    var child = try buildMapChain(wa, ctx.params, ctx.errctx, ctx.prefix, &scan, ctx.csv_schema);
    for (ctx.joins) |lj| child = try buildLaneJoinChain(wa, ctx.params, ctx.errctx, lj, child);
    var agg = op.Aggregate{
        .strs = ctx.strs,
        .child = child,
        .in_schema = ctx.agg_in_schema,
        .by = ctx.by,
        .aggs = ctx.aggs,
        .out_schema = ctx.out_schema,
        .err = null,
        .state = wa,
        .gpa = slot.gpa.allocator(),
        .part_state = &slot.parts.allocs,
        .table_gpa = slot.parts.backing(),
    };
    slot.sets = try agg.drainParts();
}

const PqLane = struct {
    arena: std.heap.ArenaAllocator,
    parts: LaneParts = undefined,
    sets: []const op.Aggregate.GroupSet = &.{},
};

const PqPart = struct {
    arena: std.heap.ArenaAllocator,
    merge: ?op.Aggregate.GroupMerge = null,
};

const pq_parts: usize = op.Aggregate.fold_parts;

const pq_min_lanes: usize = 2;

const PqAggCtx = struct {
    strs: *op.Aggregate.StrTable,
    morsels: PqMorsels,
    agg_in_schema: *const types.Schema,
    out_schema: *const types.Schema,
    prefix: []const ast.Stage,
    params: *std.StringHashMap(*const ast.Expr),
    errctx: ?*op.ErrCtx = null,
    by: []const usize,
    aggs: []const op.Aggregate.Agg,
    lanes: []PqLane,
    rows_read: *obs.RowCounter,
    joins: []const LaneJoin = &.{},
};

const PqMorsels = struct {
    files: []const []const u8,
    root: ?[]const u8 = null,
    items: []const PqItem,
    project: ?[][]const u8,
    bounds: []const pqdecode.Bound,
    src_schema: *const types.Schema,
    queue: WorkQueue,
    tally: ?*driver.ScanTally = null,
    max_lanes: usize = std.math.maxInt(usize),
    check_in_lane: bool = false,
};

const PqItem = struct { file: u32, rg: u32, rg_end: ?u32 };

const HeldFile = struct { file: u32, r: *pqdecode.Reader };

/// The reader positioned on item `i`, reusing `held` when it is the same file (reopening
/// per row group re-parsed the footer); null when a file shrank since it was planned.
fn openItem(m: *const PqMorsels, scratch: std.mem.Allocator, held: *?HeldFile, i: usize) !?*pqdecode.Reader {
    const it = m.items[i];
    if (held.*) |h| if (h.file != it.file) {
        h.r.close();
        held.* = null;
    };
    if (held.* == null) {
        const path = m.files[it.file];
        const r = try pqdecode.Reader.openProjected(scratch, path, m.project);
        if (m.check_in_lane and it.file != 0) if (pqdecode.schemaMismatch(scratch, m.src_schema.*, r.schema)) |why| {
            r.close();
            return eval.explain(error.ParquetFolderMismatch, pqdecode.mismatchMessage(scratch, m.root orelse "", path, m.files[0], why));
        };
        held.* = .{ .file = it.file, .r = r };
    }
    const r = held.*.?.r;
    r.bounds = m.bounds;
    r.tally = m.tally;
    r.rg = it.rg;
    r.rg_end = if (it.rg_end) |e| e else null;
    if (r.rg >= r.md.row_groups.len) return null;
    return r;
}

const MorselSource = struct {
    m: *PqMorsels,
    scratch: std.mem.Allocator,
    held: ?HeldFile = null,
    cur: ?*pqdecode.Reader = null,
    cur_item: usize = 0,
    fixed: ?struct { next: usize, step: usize } = null,

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
                self.cur = null;
            }
            if (self.m.queue.failed.load(.seq_cst)) return null;
            const i = self.nextIndex() orelse return null;
            self.cur = try openItem(self.m, self.scratch, &self.held, i);
            self.cur_item = i;
        }
    }
    fn closeFn(ptr: *anyopaque) void {
        const self: *MorselSource = @ptrCast(@alignCast(ptr));
        if (self.held) |h| h.r.close();
        self.held = null;
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
    var child = try buildMapChain(la, ctx.params, ctx.errctx, ctx.prefix, &scan, ctx.morsels.src_schema);
    for (ctx.joins) |lj| child = try buildLaneJoinChain(la, ctx.params, ctx.errctx, lj, child);
    var agg = op.Aggregate{
        .strs = ctx.strs,
        .child = child,
        .in_schema = ctx.agg_in_schema,
        .by = ctx.by,
        .aggs = ctx.aggs,
        .out_schema = ctx.out_schema,
        .err = null,
        .state = la,
        .gpa = ls.arena.child_allocator,
        .part_state = &ls.parts.allocs,
        .table_gpa = ls.parts.backing(),
    };
    ls.sets = try agg.drainParts();
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

/// Merge radix partition `p` of `srcs` (parquet lanes or CSV/SQL slots) into `dst`. The
/// largest source is merged into in place, the rest walked in index order and freed.
fn mergeRadixPart(dst: *PqPart, srcs: anytype, aggs: []const op.Aggregate.Agg, p: usize) !void {
    var big: ?usize = null;
    for (srcs, 0..) |*ls, i| {
        if (ls.sets.len <= p or ls.sets[p].len == 0) continue;
        if (big == null or ls.sets[p].len > srcs[big.?].sets[p].len) big = i;
    }
    const bi = big orelse return;
    const into = &srcs[bi].sets[p];
    dst.merge = op.Aggregate.GroupMerge.adopt(into, aggs) orelse blk: {
        var m = try op.Aggregate.GroupMerge.init(dst.arena.allocator(), into, aggs);
        for (0..into.len) |gi| try m.add(into, gi);
        break :blk m;
    };
    dst.merge.?.own = true;
    for (srcs, 0..) |*ls, i| {
        if (i == bi or ls.sets.len <= p) continue;
        const st = &ls.sets[p];
        for (0..st.len) |gi| try dst.merge.?.add(st, gi);
        st.freeTable();
        ls.parts.free(p);
    }
    dst.merge.?.deinit();
}

const TopNTail = struct { keys: []const ast.SortKey, n: usize };

/// A tail of only sorts and limits, so each partition keeps its own best `n` rows. An
/// `OFFSET` disables it: the rows a partition discards could be the ones it lands on.
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

/// The output value a sort key refers to: group keys first, then the aggregates. A sum
/// out of range only orders here; it fails the query when emitted.
fn groupSortValue(set: *const op.Aggregate.GroupSet, gi: usize, col: usize, by_len: usize, aggs: []const op.Aggregate.Agg) Value {
    if (col < by_len) return set.keyValue(gi, col);
    return op.Aggregate.finalizeAcc(set.acc(gi, col - by_len), aggs[col - by_len]) catch .null;
}

const GroupOrder = struct {
    cols: []const usize,
    desc: []const bool,
    by_len: usize,
    aggs: []const op.Aggregate.Agg,
    set: *const op.Aggregate.GroupSet = undefined,

    fn less(self: GroupOrder, a: u32, b: u32) bool {
        for (self.cols, self.desc) |c, d| {
            const av = groupSortValue(self.set, a, c, self.by_len, self.aggs);
            const bv = groupSortValue(self.set, b, c, self.by_len, self.aggs);
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
    sel: [][]const u32,
};

const pqTopNWorker = dispatchWorker(PqTopNCtx, pqTopNOne);

fn pqTopNOne(ctx: *PqTopNCtx, p: usize) !void {
    const pp = &ctx.parts[p];
    const m = if (pp.merge) |*x| x else {
        ctx.sel[p] = &.{};
        return;
    };
    const set = m.result();
    const idx = try pp.arena.allocator().alloc(u32, set.len);
    for (idx, 0..) |*x, i| x.* = @intCast(i);
    if (set.len > ctx.n) {
        const set_ptr = try pp.arena.allocator().create(op.Aggregate.GroupSet);
        set_ptr.* = set;
        var order = ctx.order;
        order.set = set_ptr;
        std.sort.pdq(u32, idx, order, GroupOrder.less);
    }
    ctx.sel[p] = idx[0..@min(idx.len, ctx.n)];
}

const LaneRows = union(enum) {
    parquet: *PqMorsels,
    parquet_group: struct { m: *const PqMorsels, group: usize },
    csv: struct { mapped: *csv.MappedCsv, schema: *const types.Schema, chunk: usize, of: usize },
};

/// This lane's row stream, or null when the item holds nothing (a file that shrank
/// since planning). The reader comes from `scratch` because the source borrows it;
/// closing the source releases it.
fn laneRowSource(rows: LaneRows, scratch: std.mem.Allocator) !?driver.Source {
    switch (rows) {
        .parquet => |m| {
            const ms = try scratch.create(MorselSource);
            ms.* = .{ .m = m, .scratch = scratch };
            return .{ .ptr = ms, .vtable = &MorselSource.vtable };
        },
        .parquet_group => |g| {
            var held: ?HeldFile = null;
            const rdr = (try openItem(g.m, scratch, &held, g.group)) orelse {
                if (held) |h| h.r.close();
                return null;
            };
            const rs = try scratch.create(ReaderSource);
            rs.* = .{ .r = rdr, .schema_ = g.m.src_schema };
            return .{ .ptr = rs, .vtable = &ReaderSource.vtable };
        },
        .csv => |c| {
            const rd = try scratch.create(csv.CsvSliceReader);
            rd.* = .{ .data = c.mapped.chunk(c.chunk, c.of), .schema = c.schema, .dialect = c.mapped.dialect, .slot = c.mapped.slot };
            return rd.source();
        },
    }
}

/// Open `rd`'s parquet input as splittable, or null. Local files split at row groups,
/// each footer checked here; a remote folder splits at files and a single remote file
/// stays serial. `push_stages` stops before a map path's join, whose names are the join's.
fn parquetSplit(env: *Env, rd: ast.Read, push_stages: []const ast.Stage, w: ast.Write, opts: RunOptions) anyerror!?LaneSplit {
    const arena = env.arena;
    const path = switch (rd.form) {
        .path => |p| p,
        else => return null,
    };
    if (w.mode == .upsert and w.mode.upsert.keys.len == 0) return null;
    if (opts.threads < pq_min_lanes) return null;

    var files: []const []const u8 = &.{};
    var root: ?[]const u8 = null;
    if (folder.isFolder(path)) {
        const fr = (try connect_mod.resolveFolderFmt(env, path, env.fmt_in)) orelse return null;
        if (fr.kind != .parquet) return null;
        files = fr.files;
        root = path;
    } else {
        if (csv.CsvReader.isUrl(path)) return null;
        const one = try arena.alloc([]const u8, 1);
        one[0] = path;
        files = one;
    }
    const remote = csv.CsvReader.isUrl(path);

    const project = try projectedColumns(env, push_stages);
    const bounds = try filterBounds(env, push_stages);
    const probe = pqdecode.Reader.openProjected(arena, files[0], project) catch return null;
    defer probe.close();
    var schema = probe.schema;
    if (root != null) {
        const fields = try arena.alloc(types.Schema.Field, schema.fields.len);
        for (fields, schema.fields) |*f, x| f.* = .{ .name = x.name, .ty = x.ty.asNullable() };
        schema = .{ .fields = fields };
    }

    var items = std.array_list.Managed(PqItem).init(arena);
    if (remote) {
        if (files.len < 2) return null;
        for (0..files.len) |k| try items.append(.{ .file = @intCast(k), .rg = 0, .rg_end = null });
    } else {
        for (files, 0..) |f, k| {
            const r = if (k == 0) probe else pqdecode.Reader.openProjected(arena, f, project) catch |e|
                return planErrT(env.diag, e, try std.fmt.allocPrint(arena, "could not read parquet `{s}` ({s})", .{ f, @errorName(e) }));
            defer if (k != 0) r.close();
            if (k != 0) if (pqdecode.schemaMismatch(arena, schema, r.schema)) |why|
                return planErr(env.diag, pqdecode.mismatchMessage(arena, root.?, f, files[0], why));
            for (0..r.md.row_groups.len) |g| try items.append(.{ .file = @intCast(k), .rg = @intCast(g), .rg_end = @intCast(g + 1) });
        }
        if (items.items.len < 2) return null;
    }
    if (root != null) env.folder_memo = null;
    if (env.scan) |t| {
        driver.ScanTally.add(&t.columns_read, probe.leaves.len);
        driver.ScanTally.add(&t.columns_total, probe.md.leafCount());
    }

    return .{ .parquet = .{
        .tally = env.scan,
        .files = files,
        .root = root,
        .items = items.items,
        .project = project,
        .bounds = bounds,
        .src_schema = try schemaPtr(arena, schema),
        .queue = .{ .nitems = items.items.len },
        .max_lanes = if (!remote) std.math.maxInt(usize) else if (sftp.isUrl(path)) 4 else 8,
        .check_in_lane = remote and root != null,
    } };
}

/// Map `rd`'s CSV for splitting, or null; the caller must `close()` it. A quoted newline
/// makes byte boundaries undecidable, so that file is closed here and read serially
/// (leaking it before the caller's defer exhausted fds in a FOR EACH).
fn csvSplitFile(env: *Env, rd: ast.Read, w: ast.Write) anyerror!?*csv.MappedCsv {
    const path = switch (rd.form) {
        .path => |p| p,
        else => return null,
    };
    if (w.mode == .upsert and w.mode.upsert.keys.len == 0) return null;
    if (analyze.readFormat(path, env.fmt_in) != .csv) return null;

    const mapped = csv.MappedCsv.open(env.arena, path, env.csv_in) catch return null;
    if (mapped.quoted_newlines) {
        mapped.close();
        return null;
    }
    if (rd.cols.len > 0) try mapped.project(env.arena, rd.cols);
    return mapped;
}

const LaneSplit = union(enum) {
    csv: struct { mapped: *csv.MappedCsv, schema: *const types.Schema },
    parquet: PqMorsels,

    /// A CSV's item count is free, one chunk per thread; a parquet file's is its row groups.
    fn count(self: LaneSplit, nthreads: usize) usize {
        return switch (self) {
            .csv => nthreads,
            .parquet => |m| m.queue.nitems,
        };
    }

    fn lanes(self: *const LaneSplit, threads: usize) usize {
        const n = @max(@as(usize, 1), threads);
        return switch (self.*) {
            .csv => n,
            .parquet => |m| @min(n, m.max_lanes),
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

    fn rows(self: *const LaneSplit, i: usize, nitems: usize) LaneRows {
        return switch (self.*) {
            .csv => |c| .{ .csv = .{ .mapped = c.mapped, .schema = c.schema, .chunk = i, .of = nitems } },
            .parquet => |*m| .{ .parquet_group = .{ .m = m, .group = i } },
        };
    }

    /// Rows for a shape that needs no item order (top-N re-sorts): a parquet lane steals row
    /// groups into one heap. Items are still stolen, not indexed by lane, since `spawnJoin`
    /// may start fewer threads than asked and a lane-keyed CSV chunk would be skipped.
    fn unorderedRows(self: *LaneSplit, i: usize, nitems: usize) LaneRows {
        return switch (self.*) {
            .csv => |c| .{ .csv = .{ .mapped = c.mapped, .schema = c.schema, .chunk = i, .of = nitems } },
            .parquet => |*m| .{ .parquet = m },
        };
    }

    fn abort(self: *LaneSplit) void {
        switch (self.*) {
            .parquet => |*m| m.queue.failed.store(true, .seq_cst),
            .csv => {},
        }
    }

    fn unitName(self: LaneSplit) []const u8 {
        return switch (self) {
            .csv => "chunks",
            .parquet => |m| if (m.items.len > 0 and m.items[0].rg_end == null) "files" else "row groups",
        };
    }
};

const MapCtx = struct {
    split: LaneSplit,
    map_stages: []const ast.Stage,
    params: *std.StringHashMap(*const ast.Expr),
    errctx: ?*op.ErrCtx = null,
    queue: WorkQueue,
    sink_mode: parallel.SinkMode,
    sink_mtx: std.Thread.Mutex = .{},
    rows_out: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    rows_read: *obs.RowCounter,
    join: ?LaneJoin = null,
    ordered: ?*OrderedOut = null,
};

const OrderedOut = struct {
    mtx: std.Thread.Mutex = .{},
    cv: std.Thread.Condition = .{},
    next: usize = 0,
    window: usize,
    done: []?*Unit,
    snk: driver.Sink,
    writing: bool = false,
    free: ?*Unit = null,

    const Part = union(enum) { bytes: []const u8, batch: Batch, encoded: driver.UnitEncoder };
    const Unit = struct { arena: std.heap.ArenaAllocator, parts: std.array_list.Managed(Part), next_free: ?*Unit = null };

    fn init(arena: std.mem.Allocator, nunits: usize, window: usize, snk: driver.Sink) !OrderedOut {
        const done = try arena.alloc(?*Unit, nunits);
        @memset(done, null);
        return .{ .window = window, .done = done, .snk = snk };
    }

    /// Block until unit `i` is within the window, or the run has failed.
    fn waitTurn(self: *OrderedOut, i: usize, failed: *std.atomic.Value(bool)) void {
        self.mtx.lock();
        defer self.mtx.unlock();
        while (i >= self.next + self.window and !failed.load(.seq_cst)) self.cv.wait(&self.mtx);
    }

    fn wake(self: *OrderedOut) void {
        self.mtx.lock();
        self.cv.broadcast();
        self.mtx.unlock();
    }

    /// Hand over unit `i`. With no lane writing, this one writes every consecutive finished
    /// unit from `next` on, outside `mtx`, so handing over never waits on the sink.
    fn deposit(self: *OrderedOut, i: usize, u: *Unit) !void {
        self.mtx.lock();
        self.done[i] = u;
        if (self.writing) {
            self.mtx.unlock();
            return;
        }
        self.writing = true;
        while (self.next < self.done.len) {
            const head = self.done[self.next] orelse break;
            self.done[self.next] = null;
            self.next += 1;
            self.cv.broadcast();
            self.mtx.unlock();
            const r = writeUnit(self.snk, head);
            _ = head.arena.reset(.retain_capacity);
            head.parts = std.array_list.Managed(Part).init(head.arena.allocator());
            self.mtx.lock();
            head.next_free = self.free;
            self.free = head;
            r catch |e| {
                self.writing = false;
                self.mtx.unlock();
                return e;
            };
        }
        self.writing = false;
        self.mtx.unlock();
    }

    fn writeUnit(snk: driver.Sink, u: *Unit) !void {
        for (u.parts.items) |part| switch (part) {
            .bytes => |b| try snk.writeRendered(b),
            .batch => |b| try snk.writeBatch(u.arena.allocator(), b),
            .encoded => |e| try e.commit(),
        };
    }

    fn takeUnit(self: *OrderedOut) !*Unit {
        self.mtx.lock();
        const reused = self.free;
        if (reused) |u| self.free = u.next_free;
        self.mtx.unlock();
        if (reused) |u| {
            u.next_free = null;
            return u;
        }
        return newUnit();
    }

    fn newUnit() !*Unit {
        const u = try std.heap.page_allocator.create(Unit);
        u.* = .{ .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator), .parts = undefined };
        u.parts = std.array_list.Managed(Part).init(u.arena.allocator());
        return u;
    }

    fn freeUnit(u: *Unit) void {
        u.arena.deinit();
        std.heap.page_allocator.destroy(u);
    }

    fn freeRest(self: *OrderedOut) void {
        for (self.done) |*d| if (d.*) |u| {
            freeUnit(u);
            d.* = null;
        };
        while (self.free) |u| {
            self.free = u.next_free;
            freeUnit(u);
        }
    }
};

const mapWorker = dispatchWorker(MapCtx, mapWorkOne);

/// One set of arenas for the lane's whole run, reset between units: per-unit arenas
/// left every unit decoding and formatting into cold pages.
fn orderedMapWorker(ctx: *MapCtx, _: usize) void {
    var wgpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = false }){};
    defer _ = wgpa.deinit();
    var warena = std.heap.ArenaAllocator.init(wgpa.allocator());
    defer warena.deinit();
    var batch_arena = std.heap.ArenaAllocator.init(wgpa.allocator());
    defer batch_arena.deinit();
    const ord = ctx.ordered.?;
    while (true) {
        if (ctx.queue.failed.load(.seq_cst)) return;
        const i = ctx.queue.next.fetchAdd(1, .seq_cst);
        if (i >= ctx.queue.nitems) break;
        mapOrderedUnit(ctx, ord, i, &warena, &batch_arena) catch |e| {
            ctx.queue.fail(e);
            ord.wake();
            return;
        };
        _ = warena.reset(.retain_capacity);
        _ = batch_arena.reset(.retain_capacity);
    }
}

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
    const mapped_chain = try buildMapChain(warena.allocator(), ctx.params, ctx.errctx, ctx.map_stages, &scan, src_schema);
    const chain = if (ctx.join) |lj|
        try buildLaneJoinChain(warena.allocator(), ctx.params, ctx.errctx, lj, mapped_chain)
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
    _ = ctx.rows_out.fetchAdd(out, .monotonic);

    if (own_sink) |s| {
        own_sink_open = false;
        try s.close();
    }
}

/// One positional unit of an ordered map, formatted (or unit-encoded) on the lane. A unit
/// with no rows is still handed over: the writer waits on every position.
fn mapOrderedUnit(ctx: *MapCtx, ord: *OrderedOut, i: usize, warena: *std.heap.ArenaAllocator, batch_arena: *std.heap.ArenaAllocator) !void {
    errdefer {
        ctx.split.abort();
        ctx.queue.failed.store(true, .seq_cst);
        ord.wake();
    }
    ord.waitTurn(i, &ctx.queue.failed);
    if (ctx.queue.failed.load(.seq_cst)) return;

    const unit = try ord.takeUnit();
    var handed = false;
    defer if (!handed) OrderedOut.freeUnit(unit);
    const ua = unit.arena.allocator();
    const snk = ord.snk;
    const render = snk.canRender();
    const enc: ?driver.UnitEncoder = if (snk.openUnit(ua)) |r| try r else null;
    errdefer if (enc) |e| e.discard();

    var out: u64 = 0;
    if (try laneRowSource(ctx.split.rows(i, ctx.queue.nitems), warena.allocator())) |inner| {
        defer inner.close();
        var cs = obs.CountingSource{ .inner = inner, .count = ctx.rows_read };
        var scan = op.Scan{ .src = cs.source() };
        const mapped_chain = try buildMapChain(warena.allocator(), ctx.params, ctx.errctx, ctx.map_stages, &scan, ctx.split.schema());
        const chain = if (ctx.join) |lj|
            try buildLaneJoinChain(warena.allocator(), ctx.params, ctx.errctx, lj, mapped_chain)
        else
            mapped_chain;
        while (try chain.next(batch_arena.allocator())) |b| {
            if (ctx.queue.failed.load(.seq_cst)) {
                if (enc) |e| e.discard();
                return;
            }
            if (b.len > 0) {
                if (enc) |e| {
                    try e.write(batch_arena.allocator(), b);
                } else try unit.parts.append(if (render)
                    .{ .bytes = try snk.renderBatch(ua, b).? }
                else
                    .{ .batch = try b.deepCopy(ua) });
                out += b.len;
            }
            _ = batch_arena.reset(.retain_capacity);
        }
    }
    if (enc) |e| {
        try e.seal();
        try unit.parts.append(.{ .encoded = e });
    }
    _ = ctx.rows_out.fetchAdd(out, .monotonic);
    handed = true;
    try ord.deposit(i, unit);
}

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

    const nthreads = split.lanes(opts.threads);
    var ctx = MapCtx{
        .split = split,
        .map_stages = map_stages,
        .params = env.params_expr,
        .errctx = env.errctx,
        .queue = .{ .nitems = nthreads },
        .sink_mode = sink_mode,
        .rows_read = env.rows_read,
        .join = lane_join,
    };
    var ordered: OrderedOut = undefined;
    if (sink_mode == .shared) {
        const nunits = orderedUnits(split, nthreads);
        ordered = try OrderedOut.init(arena, nunits, 2 * nthreads, sink_mode.shared);
        ctx.queue.nitems = nunits;
        ctx.ordered = &ordered;
    }
    defer if (ctx.ordered) |o| o.freeRest();

    const lanes = if (ctx.ordered != null)
        try parallel.spawnJoin(arena, nthreads, orderedMapWorker, &ctx)
    else
        try parallel.spawnJoin(arena, nthreads, mapWorker, &ctx);
    lanes_used.* = @max(lanes_used.*, lanes);
    const units = ctx.queue.nitems;
    env.log.log(.debug, "parallel {s} map{s}: {d} {s} over {d} lanes ({s} sink)", .{
        split.label(), if (lane_join != null) "+join" else "", units, split.unitName(), lanes, @tagName(sink_mode),
    });
    if (opts.explain and split == .parquet) {
        std.debug.print("actuals (parallel map, {d} lanes over {d} row groups): {d} rows out\n", .{
            lanes, units, ctx.rows_out.load(.monotonic),
        });
    }

    if (ctx.queue.failure()) |e| return e;
    stats.rows_out += ctx.rows_out.load(.monotonic);
    if (sink_mode == .shared) {
        shared_open = false;
        try sink_mode.shared.close();
    }
    return true;
}

/// A row group per item for parquet; a CSV is cut into about `ordered_chunk_bytes`
/// ranges, at least one per lane, so the reorder window holds a few MB per lane.
fn orderedUnits(split: LaneSplit, nthreads: usize) usize {
    return switch (split) {
        .csv => |c| std.math.clamp(c.mapped.body.len / ordered_chunk_bytes + 1, nthreads, 1 << 16),
        .parquet => split.count(nthreads),
    };
}

const ordered_chunk_bytes = 4 << 20;

fn runParallelParquetMap(env: *Env, rd: ast.Read, pipeline: []const ast.Stage, map_stages: []const ast.Stage, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    return runParallelParquetMapImpl(env, rd, pipeline, map_stages, null, w, opts, stats, lanes_used);
}

fn runParallelParquetMapJoin(env: *Env, rd: ast.Read, pipeline: []const ast.Stage, shape: MapJoinShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    if (!joinKindLaneSafe(shape.join.kind)) return false;
    return runParallelParquetMapImpl(env, rd, pipeline, shape.prefix, shape, w, opts, stats, lanes_used);
}

fn runParallelParquetMapImpl(env: *Env, rd: ast.Read, pipeline: []const ast.Stage, map_stages: []const ast.Stage, jshape: ?MapJoinShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    const split = (try parquetSplit(env, rd, pipeline[1..][0..map_stages.len], w, opts)) orelse return false;
    return runParallelMapImpl(env, split, map_stages, jshape, w, opts, stats, lanes_used);
}

const AggJoinShape = struct {
    map_stages: []const ast.Stage,
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
        .filter, .select => {},
        .join => |j| {
            if (ai != null) return null;
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

const LaneJoinChain = struct { joins: []const LaneJoin, out_schema: types.Schema };

fn resolveLaneJoins(env: *Env, join_span: []const ast.Stage, left_schema: types.Schema) anyerror!LaneJoinChain {
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

/// Parallel aggregate over parquet, one morsel per row group, folded into private tables
/// and merged by hash partition. False when too few row groups or lanes.
fn runParallelParquetAgg(env: *Env, rd: ast.Read, pipeline: []const ast.Stage, prefix: []const ast.Stage, ag: ast.Aggregate, tail: []const ast.Stage, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    return runParallelParquetAggImpl(env, rd, pipeline, prefix, ag, tail, null, w, opts, stats, lanes_used);
}

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

    const nthreads = split.lanes(opts.threads);
    const lanes = try env.gpa.alloc(PqLane, nthreads);
    defer env.gpa.free(lanes);
    for (lanes) |*l| {
        l.* = .{ .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator) };
        l.parts.init(env.gpa);
    }
    defer for (lanes) |*l| {
        for (l.sets) |*st| st.freeTable();
        l.parts.deinit();
        l.arena.deinit();
    };

    var strs = op.Aggregate.StrTable.init(std.heap.page_allocator);
    defer strs.deinit();
    var ctx = PqAggCtx{
        .strs = &strs,
        .morsels = morsels,
        .agg_in_schema = agg_in,
        .out_schema = out_schema,
        .prefix = prefix,
        .params = env.params_expr,
        .errctx = env.errctx,
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
    if (ctx.morsels.queue.failure()) |e| return e;

    const parts = try env.gpa.alloc(PqPart, pq_parts);
    defer env.gpa.free(parts);
    for (parts) |*pp| pp.* = .{ .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator) };
    defer for (parts) |*pp| {
        if (pp.merge) |*m| m.deinit();
        pp.arena.deinit();
    };

    const t_mrg0 = std.time.Instant.now() catch unreachable;
    if (apl.by.len == 0) {
        const dst = &parts[0];
        for (lanes) |*l| {
            for (l.sets) |*st| {
                if (dst.merge == null) dst.merge = try op.Aggregate.GroupMerge.init(dst.arena.allocator(), st, aggs);
                for (0..st.len) |i| try dst.merge.?.add(st, i);
            }
        }
    } else {
        var mctx = PqMergeCtx{ .lanes = lanes, .parts = parts, .aggs = aggs, .queue = .{ .nitems = pq_parts } };
        _ = try parallel.spawnJoin(arena, nthreads, pqMergeWorker, &mctx);
        if (mctx.queue.failure()) |e| return e;
    }
    const t_mrg1 = std.time.Instant.now() catch unreachable;
    var lane_groups: usize = 0;
    for (lanes) |*l| {
        for (l.sets) |st| lane_groups += st.len;
    }
    env.log.log(.debug, "pq agg phases: fold {d}ms, merge {d}ms, {d} lane groups", .{
        t_fold1.since(t_fold0) / 1_000_000,
        t_mrg1.since(t_mrg0) / 1_000_000,
        lane_groups,
    });
    if (opts.explain) {
        std.debug.print(
            \\actuals (parallel aggregate, {d} lanes over {d} row groups):
            \\  fold         {d:>8.1}ms {d:>12} groups
            \\  merge        {d:>8.1}ms {d:>12} partitions
            \\
        , .{
            used,                                                  ngroups,
            @as(f64, @floatFromInt(t_fold1.since(t_fold0))) / 1e6, lane_groups,
            @as(f64, @floatFromInt(t_mrg1.since(t_mrg0))) / 1e6,   pq_parts,
        });
    }

    env.log.log(.debug, "parallel parquet aggregate: {d} row groups in {d} morsels over {d} lanes, merged in {d} partitions", .{ ngroups, ngroups, used, pq_parts });

    var sel: ?[]const []const u32 = null;
    if (topNTail(tail)) |tn| {
        var cols = std.array_list.Managed(usize).init(arena);
        var descs = std.array_list.Managed(bool).init(arena);
        var ok = true;
        for (tn.keys) |k| {
            const idx = out_schema.resolve(k.field.parts) orelse {
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
                .sel = try arena.alloc([]const u32, pq_parts),
            };
            _ = try parallel.spawnJoin(arena, nthreads, pqTopNWorker, &tctx);
            if (tctx.queue.failure()) |e| return e;
            var kept = std.array_list.Managed([]const u32).init(arena);
            for (parts, tctx.sel) |*pp, sl| {
                if (pp.merge != null) try kept.append(sl);
            }
            sel = kept.items;
        }
    }

    const sets = try partSets(arena, parts);
    var total: usize = 0;
    for (sets) |st| total += st.len;

    const wr = try resolveUpsertKeys(env, w);
    const snk = try openSink(env, wr, try tailSchema(env, tail, out_schema.*));
    var snk_open = true;
    errdefer if (snk_open) snk.abort();

    const t_emit0 = std.time.Instant.now() catch unreachable;
    try writeGroups(env, snk, groupEmitter(env, agg_in, apl.by, aggs, out_schema), sets, sel, tail, nthreads, stats);
    const t_emit1 = std.time.Instant.now() catch unreachable;
    env.log.log(.debug, "pq agg tail: emit+write {d}ms ({d} groups)", .{ t_emit1.since(t_emit0) / 1_000_000, total });
    if (opts.explain) {
        std.debug.print(
            \\  emit+write   {d:>8.1}ms {d:>12} rows
            \\
        , .{ @as(f64, @floatFromInt(t_emit1.since(t_emit0))) / 1e6, total });
    }
    snk_open = false;
    try snk.close();
    return true;
}

fn runParallelCsvAgg(env: *Env, rd: ast.Read, prefix: []const ast.Stage, ag: ast.Aggregate, tail: []const ast.Stage, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    return runParallelCsvAggImpl(env, rd, prefix, ag, tail, null, w, opts, stats, lanes_used);
}

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
    var strs = op.Aggregate.StrTable.init(std.heap.page_allocator);
    defer strs.deinit();
    var ctx = AggCtx{
        .strs = &strs,
        .mapped = mapped,
        .csv_schema = &mapped.schema,
        .agg_in_schema = agg_in,
        .out_schema = out_schema,
        .prefix = prefix,
        .params = env.params_expr,
        .errctx = env.errctx,
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

    if (ctx.queue.failure()) |e| return e;

    const csets = try combineAggSlots(env, slots, aggs, opts.threads, parts);

    const wr = try resolveUpsertKeys(env, w);
    const snk = try openSink(env, wr, try tailSchema(env, tail, out_schema.*));
    var snk_open = true;
    errdefer if (snk_open) snk.abort();

    try writeGroups(env, snk, groupEmitter(env, agg_in, apl.by, aggs, out_schema), csets, null, tail, opts.threads, stats);
    snk_open = false;
    try snk.close();
    return true;
}

const SqlAggCtx = struct {
    strs: *op.Aggregate.StrTable,
    split: SplitCtx,
    predicates: []const []const u8,
    proj_select: ?[]const u8,
    where_extra: ?[]const u8,
    src_schema: *const types.Schema,
    agg_in_schema: *const types.Schema,
    out_schema: *const types.Schema,
    prefix: []const ast.Stage,
    params: *std.StringHashMap(*const ast.Expr),
    errctx: ?*op.ErrCtx = null,
    by: []const usize,
    aggs: []const op.Aggregate.Agg,
    queue: WorkQueue,
    slots: []AggSlot,
    rows_read: *obs.RowCounter,
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
    const child = try buildMapChain(wa, ctx.params, ctx.errctx, ctx.prefix, &scan, ctx.src_schema);
    var agg = op.Aggregate{
        .strs = ctx.strs,
        .child = child,
        .in_schema = ctx.agg_in_schema,
        .by = ctx.by,
        .aggs = ctx.aggs,
        .out_schema = ctx.out_schema,
        .err = null,
        .state = wa,
        .gpa = slot.gpa.allocator(),
        .part_state = &slot.parts.allocs,
        .table_gpa = slot.parts.backing(),
    };
    slot.sets = try agg.drainParts();
}

/// Parallel SQL aggregate over key-range lanes, one connection and one `AggSlot` per
/// range (ranges are stolen, so only the range index is stable). False falls back.
pub fn runParallelSqlAgg(env: *Env, stages: []const ast.Stage, prefix: []const ast.Stage, ag: ast.Aggregate, tail: []const ast.Stage, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize, src_base: usize) anyerror!bool {
    const arena = env.arena;
    const desc = env.sql_desc orelse return false;
    if (stages[0].node != .read) return false;
    if (w.mode == .upsert and w.mode.upsert.keys.len == 0) return false;
    if (src_base >= env.sources.items.len) return false;

    const src_schema = try schemaPtr(arena, try dupeSchema(arena, env.sources.items[src_base].schema()));

    const facts = try connect_mod.factsIfWanted(env, stages[0].node.read, desc.dialect, prefix, src_schema.*, true, .superset);
    const pd = try pushdown.planAggWith(arena, desc.dialect, src_schema.*, prefix, ag, facts);
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
    const slots = try allocAggSlots(env.gpa, sp.predicates.len);
    defer freeAggSlots(env.gpa, slots);
    const parts = try allocMergeParts(env.gpa);
    defer freeMergeParts(env.gpa, parts);
    var strs = op.Aggregate.StrTable.init(std.heap.page_allocator);
    defer strs.deinit();
    var ctx = SqlAggCtx{
        .strs = &strs,
        .split = .{ .gpa = env.gpa, .kind = desc.kind, .cfg = desc.cfg, .base_sql = sp.base_sql, .report = try connect_mod.readReport(env, @tagName(desc.kind)) },
        .predicates = sp.predicates,
        .proj_select = pd.proj_select,
        .where_extra = pd.where_extra,
        .src_schema = eff,
        .agg_in_schema = agg_in,
        .out_schema = out_schema,
        .prefix = prefix,
        .params = env.params_expr,
        .errctx = env.errctx,
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

    if (ctx.queue.failure()) |e| return e;

    const csets = try combineAggSlots(env, slots, aggs, opts.threads, parts);

    const snk = try openSink(env, w, out_schema.*);
    var snk_open = true;
    errdefer if (snk_open) snk.abort();

    try writeGroups(env, snk, groupEmitter(env, agg_in, apl.by, aggs, out_schema), csets, null, tail, opts.threads, stats);
    snk_open = false;
    try snk.close();
    return true;
}

fn classifyMapPipeline(stages: []const ast.Stage) ?[]const ast.Stage {
    const middle = stages[1 .. stages.len - 1];
    for (middle) |st| switch (st.node) {
        .filter, .select => {},
        else => return null,
    };
    return middle;
}

const MapJoinShape = struct { prefix: []const ast.Stage, join: ast.Join, join_hints: []const ast.Hint, suffix: []const ast.Stage };

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

/// Join kinds whose probe is a pure lookup into a shared read-only index. Matched by tag
/// name, not a switch: the kind set is still growing on another branch.
pub fn joinKindLaneSafe(kind: ast.JoinKind) bool {
    for ([_][]const u8{ "inner", "left", "semi", "anti", "cross" }) |ok| {
        if (std.mem.eql(u8, @tagName(kind), ok)) return true;
    }
    return false;
}

/// A key-index field of `analyze.JoinPlan` as a slice, accepting both the single and
/// the multi-key spelling so this builds against either snapshot.
fn planKeys(a: std.mem.Allocator, jp: analyze.JoinPlan, comptime which: []const u8) ![]const usize {
    const name: []const u8 = comptime if (@hasField(analyze.JoinPlan, which)) which else if (std.mem.eql(u8, which, "lk")) "lks" else "rks";
    const v = @field(jp, name);
    return if (@TypeOf(v) == usize) try a.dupe(usize, &[_]usize{v}) else v;
}

const LaneJoin = struct {
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
fn resolveLaneJoin(env: *Env, j: ast.Join, join_hints: []const ast.Hint, suffix: []const ast.Stage, left_schema: types.Schema) anyerror!LaneJoinPlan {
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

fn buildLaneJoinChain(ta: std.mem.Allocator, params: *std.StringHashMap(*const ast.Expr), errctx: ?*op.ErrCtx, lj: LaneJoin, probe: op.Op) !op.Op {
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

const SqlMapJoinCtx = struct {
    split: SplitCtx,
    predicates: []const []const u8,
    src_schema: *const types.Schema,
    prefix: []const ast.Stage,
    join: LaneJoin,
    params: *std.StringHashMap(*const ast.Expr),
    errctx: ?*op.ErrCtx = null,
    queue: WorkQueue,
    sink_mode: parallel.SinkMode,
    sink_mtx: std.Thread.Mutex = .{},
    rows_out: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    rows_read: *obs.RowCounter,
};

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
    const probe = try buildMapChain(warena.allocator(), ctx.params, ctx.errctx, ctx.prefix, &scan, ctx.src_schema);
    const chain = try buildLaneJoinChain(warena.allocator(), ctx.params, ctx.errctx, ctx.join, probe);

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

/// Split-parallel SQL map+join. The serial plan's sources are closed before the shared
/// index opens its own. No projection narrowing under a join (planMap's liveness is
/// map-shaped), so lanes select `*`; join-aware liveness would fix that.
pub fn runParallelSqlMapJoin(env: *Env, stages: []const ast.Stage, shape: MapJoinShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize, src_base: usize) anyerror!bool {
    const arena = env.arena;
    if (stages[0].node != .read) return false;
    const desc = (try sqlDescForStage(env, stages[0])) orelse return false;
    if (!joinKindLaneSafe(shape.join.kind)) return false;
    if (w.mode == .upsert and w.mode.upsert.keys.len == 0) return false;
    if (src_base >= env.sources.items.len) return false;

    const src_schema = try schemaPtr(arena, try dupeSchema(arena, env.sources.items[src_base].schema()));
    const probe_schema = try mapChainSchema(env, shape.prefix, src_schema.*);

    const sp = (try planSplit(env, desc, stages[0], opts.threads, w)) orelse return false;

    for (env.sources.items[src_base..]) |sc| sc.close();
    env.sources.shrinkRetainingCapacity(src_base);

    const lp = try resolveLaneJoin(env, shape.join, shape.join_hints, shape.suffix, probe_schema);

    env.sink_name = sinkLabel(env, w);
    const sink_mode: parallel.SinkMode = (try buildParallelSink(env, w, lp.out_schema)) orelse
        .{ .shared = try openSink(env, w, lp.out_schema) };
    var shared_open = sink_mode == .shared;
    errdefer if (shared_open) sink_mode.shared.abort();

    var ctx = SqlMapJoinCtx{
        .split = .{ .gpa = env.gpa, .kind = desc.kind, .cfg = desc.cfg, .base_sql = sp.base_sql, .report = try connect_mod.readReport(env, @tagName(desc.kind)) },
        .predicates = sp.predicates,
        .src_schema = src_schema,
        .prefix = shape.prefix,
        .join = lp.lane,
        .params = env.params_expr,
        .errctx = env.errctx,
        .queue = .{ .nitems = sp.predicates.len },
        .sink_mode = sink_mode,
        .rows_read = env.rows_read,
    };

    const nlanes = @min(@max(@as(usize, 1), opts.threads), sp.predicates.len);
    const lanes = try parallel.spawnJoin(arena, nlanes, sqlMapJoinLane, &ctx);
    lanes_used.* = @max(lanes_used.*, lanes);
    env.log.log(.debug, "split-parallel map+join: {d} splits over {d} lanes", .{ sp.predicates.len, lanes });

    if (ctx.queue.failure()) |e| return e;
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

fn runParallelCsvMapJoin(env: *Env, rd: ast.Read, shape: MapJoinShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    if (!joinKindLaneSafe(shape.join.kind)) return false;
    return runParallelCsvMapImpl(env, rd, shape.prefix, shape, w, opts, stats, lanes_used);
}

fn runParallelCsvMapImpl(env: *Env, rd: ast.Read, map_stages: []const ast.Stage, jshape: ?MapJoinShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    const mapped = (try csvSplitFile(env, rd, w)) orelse return false;
    defer mapped.close();
    return runParallelMapImpl(env, .{ .csv = .{ .mapped = mapped, .schema = &mapped.schema } }, map_stages, jshape, w, opts, stats, lanes_used);
}

const TopNShape = struct { prefix: []const ast.Stage, srt: ast.Sort, lim: ast.Limit };

fn classifyTopNPipeline(stages: []const ast.Stage) ?TopNShape {
    const middle = stages[1 .. stages.len - 1];
    if (middle.len < 2) return null;
    if (middle[middle.len - 1].node != .limit or middle[middle.len - 2].node != .sort) return null;
    if (!op.TopN.fits(middle[middle.len - 1].node.limit)) return null;
    const prefix = middle[0 .. middle.len - 2];
    for (prefix) |st| switch (st.node) {
        .filter, .select => {},
        else => return null,
    };
    return .{ .prefix = prefix, .srt = middle[middle.len - 2].node.sort, .lim = middle[middle.len - 1].node.limit };
}

const TopNCtx = struct {
    split: LaneSplit,
    row_schema: *const types.Schema,
    prefix: []const ast.Stage,
    params: *std.StringHashMap(*const ast.Expr),
    errctx: ?*op.ErrCtx = null,
    keys: []const op.Sort.Key,
    cap: u64,
    queue: WorkQueue,
    kept: std.array_list.Managed(op.TopN.Entry),
    kept_arena: std.mem.Allocator,
    mtx: std.Thread.Mutex = .{},
    rows_read: *obs.RowCounter,
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
    var item_ptr: ?*const usize = null;
    const inner: driver.Source = switch (ctx.split) {
        .parquet => |*m| blk: {
            const ms = try warena.allocator().create(MorselSource);
            ms.* = .{ .m = m, .scratch = warena.allocator() };
            item_ptr = &ms.cur_item;
            break :blk .{ .ptr = ms, .vtable = &MorselSource.vtable };
        },
        .csv => (try laneRowSource(ctx.split.unorderedRows(i, ctx.queue.nitems), warena.allocator())) orelse return,
    };
    defer inner.close();
    errdefer ctx.split.abort();
    var cs = obs.CountingSource{ .inner = inner, .count = ctx.rows_read };
    var scan = op.Scan{ .src = cs.source() };
    const child = try buildMapChain(warena.allocator(), ctx.params, ctx.errctx, ctx.prefix, &scan, src_schema);
    var tn = op.TopN{
        .child = child,
        .in_schema = ctx.row_schema,
        .keys = ctx.keys,
        .count = ctx.cap,
        .offset = 0,
        .state = batch_arena.allocator(),
        .gpa = wgpa.allocator(),
        .seq_base = @as(u64, i) << 40,
        .item = item_ptr,
    };
    const local = (try tn.nextEntries(batch_arena.allocator())) orelse return;

    ctx.mtx.lock();
    defer ctx.mtx.unlock();
    for (local) |e| {
        const row = try ctx.kept_arena.alloc(Value, e.len);
        for (row, e) |*o, v| o.* = try op.dupeValue(ctx.kept_arena, v);
        try ctx.kept.append(row);
    }
}

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

    const nthreads = split.lanes(opts.threads);
    var kept_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer kept_arena.deinit();
    var ctx = TopNCtx{
        .split = split,
        .row_schema = row_schema,
        .prefix = prefix,
        .params = env.params_expr,
        .errctx = env.errctx,
        .keys = ks,
        .cap = lim.offset + lim.count,
        .queue = .{ .nitems = nthreads },
        .kept = .init(env.gpa),
        .kept_arena = kept_arena.allocator(),
        .rows_read = env.rows_read,
    };
    defer ctx.kept.deinit();

    const lanes = try parallel.spawnJoin(arena, nthreads, topnWorker, &ctx);
    lanes_used.* = @max(lanes_used.*, lanes);
    env.log.log(.debug, "parallel {s} top-n: {d} {s} over {d} lanes", .{ split.label(), split.count(nthreads), split.unitName(), lanes });
    if (ctx.queue.failure()) |e| return e;

    std.mem.sort(op.TopN.Entry, ctx.kept.items, ks, struct {
        fn lt(keys: []const op.Sort.Key, x: op.TopN.Entry, y: op.TopN.Entry) bool {
            return op.entryLess(x, y, keys);
        }
    }.lt);
    var global = op.TopN{ .child = undefined, .in_schema = row_schema, .keys = ks, .count = lim.count, .offset = lim.offset, .state = arena, .gpa = env.gpa };
    const start = @min(lim.offset, ctx.kept.items.len);
    const end = @min(lim.offset +| lim.count, ctx.kept.items.len);

    const wr = try resolveUpsertKeys(env, w);
    const snk = try openSink(env, wr, row_schema.*);
    var snk_open = true;
    errdefer if (snk_open) snk.abort();
    if (start < end) {
        const b = try global.emit(arena, ctx.kept.items[start..end]);
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

fn classifyDistinctPipeline(stages: []const ast.Stage) ?DistinctShape {
    const middle = stages[1 .. stages.len - 1];
    var di: ?usize = null;
    for (middle, 0..) |st, i| switch (st.node) {
        .filter, .select => {},
        .distinct => {
            if (di != null) return null;
            di = i;
        },
        .sort, .limit, .aggregate, .window => if (di == null) return null,
        else => return null,
    };
    const d = di orelse return null;
    return .{ .prefix = middle[0..d], .dist = middle[d].node.distinct, .tail = middle[d + 1 ..] };
}

const DistinctRow = struct {
    chunk: usize,
    ord: u64,
    vals: []Value,

    fn before(_: void, a: DistinctRow, b: DistinctRow) bool {
        if (a.chunk != b.chunk) return a.chunk < b.chunk;
        return a.ord < b.ord;
    }
};

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

    /// Input order, then one batch: sorting here makes `-j 8` agree with `-j 1` byte for byte.
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
    errctx: ?*op.ErrCtx = null,
    local_keys: ?[]const usize,
    key_idx: []const usize,
    queue: WorkQueue,
    merge: *DistinctMerge,
    rows_read: *obs.RowCounter,
};

const distinctWorker = dispatchWorker(DistinctCtx, distinctWorkOne);

/// Dedups per item rather than per lane, so the item index is a file position and the
/// merge keeps the same row a serial run would.
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
    const child = try buildMapChain(warena.allocator(), ctx.params, ctx.errctx, ctx.prefix, &scan, src_schema);
    var d = op.Distinct{ .child = child, .in_schema = ctx.row_schema, .keys = ctx.local_keys, .state = warena.allocator(), .gpa = wgpa.allocator(), .track_ords = true };

    const probe = try warena.allocator().alloc(Value, ctx.key_idx.len);
    while (try d.next(batch_arena.allocator())) |b| {
        try ctx.merge.absorb(i, b, d.ords, probe);
        _ = batch_arena.reset(.retain_capacity);
    }
}

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
    fn closeFn(ptr: *anyopaque) void {
        const self: *ReaderSource = @ptrCast(@alignCast(ptr));
        self.r.close();
    }
    const vtable = driver.Source.VTable{ .schema = schemaFn, .next = nextFn, .close = closeFn };
};

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

    const nthreads = split.lanes(opts.threads);
    const nitems = split.count(nthreads);
    var merge = DistinctMerge.init(arena, key_idx);
    var ctx = DistinctCtx{
        .split = split,
        .row_schema = row_schema,
        .prefix = prefix,
        .params = env.params_expr,
        .errctx = env.errctx,
        .local_keys = local_keys,
        .key_idx = key_idx,
        .queue = .{ .nitems = nitems },
        .merge = &merge,
        .rows_read = env.rows_read,
    };

    const lanes = try parallel.spawnJoin(arena, nthreads, distinctWorker, &ctx);
    lanes_used.* = @max(lanes_used.*, lanes);
    env.log.log(.debug, "parallel {s} distinct: {d} {s} over {d} lanes", .{ split.label(), nitems, split.unitName(), lanes });
    if (ctx.queue.failure()) |e| return e;

    const merged = try merge.finish(row_schema);

    const wr = try resolveUpsertKeys(env, w);
    const snk = try openSink(env, wr, try tailSchema(env, tail, row_schema.*));
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
    const mapped = (try csvSplitFile(env, rd, w)) orelse return false;
    defer mapped.close();
    return runParallelDistinct(env, .{ .csv = .{ .mapped = mapped, .schema = &mapped.schema } }, prefix, dist, tail, w, opts, stats, lanes_used);
}

fn testStages(arena: std.mem.Allocator, src: []const u8) ![]const ast.Stage {
    var diag: @import("../lang/sql_parser.zig").Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try @import("../lang/sql_parser.zig").parseSource(arena, src, &diag);
    for (prog.stmts) |s| if (s == .output) return s.output.stages;
    return error.NoOutput;
}

test "a join then an aggregate runs on lanes with a HAVING after it, as the plain aggregate does" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const having = try testStages(a,
        \\LOAD INTO 'o.csv' AS WITH d AS (SELECT id AS did FROM 'x.parquet')
        \\SELECT g, COUNT(*) AS n FROM 'x.parquet' JOIN d ON id = did GROUP BY g HAVING COUNT(*) > 1;
    );
    const shape = classifyAggJoinPipeline(having) orelse return error.TestExpectedLanes;
    try std.testing.expect(shape.tail[0].node == .filter);
    const plain = try testStages(a, "LOAD INTO 'o.csv' AS SELECT g, COUNT(*) AS n FROM 'x.parquet' GROUP BY g HAVING COUNT(*) > 1;");
    try std.testing.expect(classifyAggPipeline(plain) != null);
}
