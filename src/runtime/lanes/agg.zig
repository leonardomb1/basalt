//! The aggregate as the first `ParallelSink` (`morsels.zig`): every morsel folds
//! into a partial of its own, the partials merge by hash partition in morsel order,
//! and the groups are written through the tail; a file's morsels and a SQL read's
//! key ranges run the same sink.
//!
//! Each morsel folds from fresh state, so a group's records depend on that morsel's
//! rows alone (a lane's table is bounded and clears when full), and the merge adds a
//! group's records morsel by morsel: float addition is not associative, and since the
//! cut follows the input, not `-j`, a float sum is the same at every `-j` of 2 or
//! more (one lane runs the serial aggregate, whose last bits may differ). A
//! morsel's fold tables go as it is sealed; its stores and DISTINCT pairs stay until
//! each partition takes them, once every earlier morsel's are in. With a spill
//! directory, partials totalling past `--op-memory` send the aggregate to the serial
//! plan, which spills: that total follows the input too, so neither does the choice.

const Batch = @import("../../exec/batch.zig").Batch;
const Env = @import("../env.zig").Env;
const Pipe = @import("../env.zig").Pipe;
const LaneJoin = @import("join.zig").LaneJoin;
const OneBatch = @import("../connect.zig").OneBatch;
const OrderedOut = @import("map.zig").OrderedOut;
const ParallelSink = @import("morsels.zig").ParallelSink;
const Pipeline = @import("morsels.zig").Pipeline;
const Profile = @import("../../exec/profile.zig").Profile;
const PqTopNCtx = @import("topn.zig").PqTopNCtx;
const Reader = @import("morsels.zig").Reader;
const RunOptions = @import("../env.zig").RunOptions;
const SinkShape = @import("../lanes.zig").SinkShape;
const SplitCtx = @import("../connect.zig").SplitCtx;
const Stats = @import("../env.zig").Stats;
const LaneMem = @import("../parallel.zig").LaneMem;
const Morsels = @import("morsels.zig").Morsels;
const WorkQueue = @import("../parallel.zig").WorkQueue;
const orderedWorker = @import("map.zig").orderedWorker;
const analyze = @import("../analyze.zig");
const ast = @import("../../lang/ast.zig");
const buildOp = @import("../plan.zig").buildOp;
const buildTopN = @import("../plan.zig").buildTopN;
const chainAt = @import("../plan.zig").chainAt;
const chainOps = @import("../plan.zig").chainOps;
const connect_mod = @import("../connect.zig");
const dispatchWorker = @import("../parallel.zig").dispatchWorker;
const drive = @import("morsels.zig").drive;
const driver = @import("../../connect/driver.zig");
const dupeSchema = @import("../connect.zig").dupeSchema;
const op = @import("../../exec/op.zig");
const openSink = @import("../connect.zig").openSink;
const parallel = @import("../parallel.zig");
const planSplit = @import("../connect.zig").planSplit;
const pqTopNWorker = @import("topn.zig").pqTopNWorker;
const profile = @import("../../exec/profile.zig");
const pushdown = @import("../pushdown.zig");
const resolveLaneJoins = @import("join.zig").resolveLaneJoins;
const resolveAt = @import("../plan.zig").resolveAt;
const resolveUpsertKeys = @import("../connect.zig").resolveUpsertKeys;
const SinkGuard = @import("../../connect/guard.zig").SinkGuard;
const schemaPtr = @import("../env.zig").schemaPtr;
const sinkLabel = @import("../connect.zig").sinkLabel;
const std = @import("std");
const msgfmt = @import("../../format/msgfmt.zig");
const trace = @import("../../exec/trace.zig");
const topNTail = @import("topn.zig").topNTail;
const types = @import("../../lang/types.zig");

const LaneParts = struct {
    arenas: [pq_parts]std.heap.ArenaAllocator,
    allocs: [pq_parts]std.mem.Allocator,

    pub fn init(self: *LaneParts, child: std.mem.Allocator) void {
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

pub const agg_combine_parallel_min: usize = 1 << 14;

/// One radix partition of the merged groups; `next` is the first morsel not yet
/// merged into it, both under `mtx`.
pub const PqPart = struct {
    arena: std.heap.ArenaAllocator,
    merge: ?op.Aggregate.GroupMerge = null,
    mtx: std.Thread.Mutex = .{},
    next: usize = 0,
};

const pq_parts: usize = op.Aggregate.fold_parts;

/// One morsel's groups: a set per radix partition, each in that partition's arena.
const Partial = struct {
    state: std.heap.ArenaAllocator,
    parts: LaneParts = undefined,
    sets: []op.Aggregate.GroupSet = &.{},

    fn bytes(self: *const Partial) usize {
        var n = self.state.queryCapacity();
        for (self.parts.arenas) |a| n += a.queryCapacity();
        return n;
    }
};

fn partSets(a: std.mem.Allocator, parts: []PqPart) ![]const op.Aggregate.GroupSet {
    var sets = std.array_list.Managed(op.Aggregate.GroupSet).init(a);
    for (parts) |*pp| {
        if (pp.merge) |*m| try sets.append(try m.result());
    }
    return sets.items;
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

/// The aggregate every lane folds: its input and output schemas, keys and accumulators.
const LaneAgg = struct {
    strs: *op.Aggregate.StrTable,
    in_schema: *const types.Schema,
    out_schema: *const types.Schema,
    by: []const usize,
    aggs: []const op.Aggregate.Agg,
    prof: ?*Profile = null,
    err: ?*op.ErrCtx = null,

    fn of(env: *Env, r: analyze.Resolved, strs: *op.Aggregate.StrTable) LaneAgg {
        return .{ .strs = strs, .in_schema = r.in, .out_schema = r.out, .by = r.plan.aggregate.by, .aggs = r.plan.aggregate.aggs, .prof = env.profile, .err = env.errctx };
    }

    /// Fold `child` into private group tables, one per hash partition of `parts`.
    fn fold(self: LaneAgg, child: op.Op, state: std.mem.Allocator, gpa: std.mem.Allocator, parts: *LaneParts) ![]op.Aggregate.GroupSet {
        var agg = op.Aggregate{
            .strs = self.strs,
            .child = child,
            .in_schema = self.in_schema,
            .by = self.by,
            .aggs = self.aggs,
            .out_schema = self.out_schema,
            .err = self.err,
            .state = state,
            .gpa = gpa,
            .part_state = &parts.allocs,
            .table_gpa = parts.backing(),
        };
        const t0 = if (self.prof != null) std.time.Instant.now() catch null else null;
        const sets = try agg.drainParts();
        if (self.prof) |p| {
            if (t0) |s| agg.stats.ns = if (std.time.Instant.now()) |now| now.since(s) else |_| 0;
            for (sets) |st| agg.stats.rows += st.len;
            agg.stats.calls = 1;
            _ = try p.absorb(.{ .aggregate = &agg });
        }
        return sets;
    }

    fn emitter(self: LaneAgg, env: *Env) op.Aggregate {
        return .{
            .child = undefined,
            .in_schema = self.in_schema,
            .by = self.by,
            .aggs = self.aggs,
            .out_schema = self.out_schema,
            .state = env.arena,
            .gpa = env.gpa,
        };
    }
};

/// The sink for `w`, typed `schema`: what a parallel breaker's tail leaves.
pub fn openTailSink(env: *Env, w: ast.Write, schema: types.Schema) !driver.Sink {
    return openSink(env, try resolveUpsertKeys(env, w), schema);
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
    tail: []const analyze.Resolved,
    threads: usize,
    stats: *Stats,
) !void {
    var total: usize = 0;
    for (sets) |st| total += st.len;
    const rowwise = for (tail) |r| switch (r.plan) {
        .filter, .project => {},
        else => break false,
    } else true;
    if (sel != null or emitter.by.len == 0 or threads < 2 or sets.len < 2 or total < agg_combine_parallel_min or !rowwise) {
        var em = emitter;
        const batch = try em.emitSets(env.arena, sets, sel);
        return writeTail(env, snk, batch, emitter.out_schema.*, tail, stats, .aggregate);
    }

    var ord = try OrderedOut.init(env.arena, sets.len, 2 * threads, snk);
    defer ord.freeRest();
    var ctx = GroupWriteCtx{
        .emitter = emitter,
        .sets = sets,
        .tail = tail,
        .errctx = env.errctx,
        .queue = .{ .nitems = sets.len },
        .ordered = &ord,
        .prof = env.profile,
    };
    _ = try parallel.spawnJoin(env.arena, @min(threads, sets.len), groupWriteWorker, &ctx);
    if (ctx.queue.failure()) |e| return e;
    stats.rows_out += ctx.rows_out.load(.monotonic);
    if (env.profile) |p| if (ctx.top) |top| if (p.rootOf(.aggregate)) |a| try p.graftLeaf(top, a);
}

const GroupWriteCtx = struct {
    emitter: op.Aggregate,
    sets: []const op.Aggregate.GroupSet,
    tail: []const analyze.Resolved,
    errctx: ?*op.ErrCtx = null,
    queue: WorkQueue,
    ordered: ?*OrderedOut,
    rows_out: std.atomic.Value(u64) = .init(0),
    prof: ?*Profile = null,
    top: ?profile.Id = null,
    top_mtx: std.Thread.Mutex = .{},
};

const groupWriteWorker = orderedWorker(GroupWriteCtx, groupWriteUnit);

fn groupWriteUnit(ctx: *GroupWriteCtx, i: usize, mem: *LaneMem, _: *Reader) !void {
    const ord = ctx.ordered.?;
    var f = (try ord.begin(i, &ctx.queue.failed)) orelse return;
    {
        errdefer f.drop();
        const wa = mem.work.allocator();
        var em = ctx.emitter;
        em.state = wa;
        const out_schema = ctx.emitter.out_schema;
        var ob = OneBatch{ .b = try em.emitSets(wa, ctx.sets[i..][0..1], null), .sch = out_schema.* };
        var scan = op.Scan{ .src = ob.source() };
        scan.stats.at = profile.merged_at;
        const chain = try chainOps(wa, ctx.errctx, ctx.tail, .{ .scan = &scan });
        while (try chain.next(wa)) |b| try f.add(ord.snk, wa, b);
        if (ctx.prof) |p| {
            const top = try p.absorb(chain);
            ctx.top_mtx.lock();
            defer ctx.top_mtx.unlock();
            ctx.top = top;
        }
    }
    _ = ctx.rows_out.fetchAdd(f.rows, .monotonic);
    try ord.hand(i, f);
}

/// Write a parallel breaker's merged result through `tail`; under a profile the tail's
/// operators stand above the lanes' `feeds` operators, whose result they read.
pub fn writeTail(env: *Env, snk: driver.Sink, batch: Batch, schema: types.Schema, tail: []const analyze.Resolved, stats: *Stats, feeds: profile.Kind) !void {
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
    scan.stats.at = profile.merged_at;
    var cur: op.Op = .{ .scan = &scan };
    defer cur.release();
    var i: usize = 0;
    while (i < tail.len) : (i += 1) {
        const r = if (tail[i].plan == .sort and i + 1 < tail.len and tail[i + 1].plan == .limit) blk: {
            i += 1;
            break :blk try buildTopN(env, tail[i - 1], tail[i].stage.node.limit, cur);
        } else try buildOp(env, tail[i], cur);
        cur = r.op;
        cur.stampNew(profile.keyOf(tail[if (cur == .top_n) i - 1 else i].stage.pos));
    }
    while (try cur.next(arena)) |outb| {
        try snk.writeBatch(arena, outb);
        stats.rows_out += outb.len;
    }
    if (env.profile) |p| {
        const top = try p.absorb(cur);
        if (p.rootOf(feeds)) |below| try p.graftLeaf(top, below);
    }
}

/// The aggregate's Global: a partial per morsel, the merged radix partitions, and
/// what `finalize` writes them through. A Local is the worker's `LaneMem`. Each
/// partition merges the partials in morsel order as they are done, so what is held
/// is the merged groups and the partials that finished ahead of an earlier one.
const AggSink = struct {
    env: *Env,
    la: LaneAgg,
    partials: []?*Partial,
    done: []std.atomic.Value(bool),
    parts: []PqPart,
    tail: []const ast.Stage,
    w: ast.Write,
    threads: usize,
    stats: *Stats,
    /// Bytes of partials past which the sink gives up; 0 never does.
    budget: usize,
    held: std.atomic.Value(usize) = .init(0),
    t0: std.time.Instant,

    const vtable = ParallelSink.VTable{ .initLocal = initLocal, .sink = sink, .combine = combine, .finalize = finalize };

    fn init(env: *Env, la: LaneAgg, morsels: usize, tail: []const ast.Stage, w: ast.Write, threads: usize, stats: *Stats) !AggSink {
        const partials = try env.gpa.alloc(?*Partial, morsels);
        @memset(partials, null);
        errdefer env.gpa.free(partials);
        const done = try env.gpa.alloc(std.atomic.Value(bool), morsels);
        @memset(done, .init(false));
        errdefer env.gpa.free(done);
        return .{
            .env = env,
            .la = la,
            .partials = partials,
            .done = done,
            .parts = try allocMergeParts(env.gpa),
            .tail = tail,
            .w = w,
            .threads = threads,
            .stats = stats,
            .budget = if (env.space != null) env.op_memory else 0,
            .t0 = std.time.Instant.now() catch unreachable,
        };
    }

    fn deinit(self: *AggSink) void {
        const gpa = self.env.gpa;
        for (self.partials) |pt| if (pt) |ls| {
            for (ls.sets) |*st| st.freeTable();
            ls.parts.deinit();
            ls.state.deinit();
            gpa.destroy(ls);
        };
        gpa.free(self.partials);
        gpa.free(self.done);
        freeMergeParts(gpa, self.parts);
    }

    fn parallelSink(self: *AggSink) ParallelSink {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn initLocal(_: *anyopaque, mem: *LaneMem) anyerror!*anyopaque {
        return mem;
    }

    /// Fold morsel `i` into partial `i`, whose fold tables go once it is sealed, then
    /// merge what each partition not busy can take in order.
    fn sink(ptr: *anyopaque, local: *anyopaque, i: usize, chain: op.Op) anyerror!void {
        const self: *AggSink = @ptrCast(@alignCast(ptr));
        const mem: *LaneMem = @ptrCast(@alignCast(local));
        const gpa = self.env.gpa;
        const ls = try gpa.create(Partial);
        ls.* = .{ .state = .init(gpa) };
        ls.parts.init(gpa);
        self.partials[i] = ls;
        ls.sets = try self.la.fold(chain, ls.state.allocator(), mem.gpa.allocator(), &ls.parts);
        for (ls.sets) |*st| if (st.table) |t| {
            t.deinit();
            st.table = null;
        };
        if (self.budget > 0) {
            const n = ls.bytes();
            if (self.held.fetchAdd(n, .monotonic) + n > self.budget) return error.AggregatePastMemory;
        }
        self.done[i].store(true, .release);
        const n = self.mergedParts();
        for (0..n) |k| {
            const p = (i + k) % n;
            if (!self.parts[p].mtx.tryLock()) continue;
            defer self.parts[p].mtx.unlock();
            try self.drain(p);
        }
    }

    fn mergedParts(self: *const AggSink) usize {
        return if (self.la.by.len == 0) 1 else pq_parts;
    }

    /// Merge into partition `p`, its lock held, the partials from its watermark on
    /// that are done, in morsel order, freeing each one's share as merged. An
    /// ungrouped aggregate merges into partition 0 alone (it has no key to hash, and
    /// the radix merge once dropped every partial, so COUNT(*) returned 0), keeping
    /// its partials, whose DISTINCT pairs its merge reads when it settles.
    fn drain(self: *AggSink, p: usize) !void {
        const dst = &self.parts[p];
        while (dst.next < self.partials.len and self.done[dst.next].load(.acquire)) : (dst.next += 1) {
            const ls = self.partials[dst.next].?;
            if (self.la.by.len == 0) {
                for (ls.sets) |*st| {
                    if (dst.merge == null) {
                        dst.merge = try op.Aggregate.GroupMerge.init(dst.arena.allocator(), st, self.la.aggs);
                        dst.merge.?.threads = self.threads;
                    }
                    try dst.merge.?.addSet(st);
                }
                continue;
            }
            if (ls.sets.len <= p or ls.sets[p].len == 0) continue;
            const st = &ls.sets[p];
            if (dst.merge == null) {
                dst.merge = try op.Aggregate.GroupMerge.init(dst.arena.allocator(), st, self.la.aggs);
                dst.merge.?.own = true;
            }
            try dst.merge.?.addSet(st);
            st.freeTable();
            ls.parts.free(p);
        }
    }

    /// Partition `p`'s last partials, merged and settled once every morsel is done.
    fn finishPart(self: *AggSink, p: usize) !void {
        const dst = &self.parts[p];
        dst.mtx.lock();
        defer dst.mtx.unlock();
        try self.drain(p);
        if (self.la.by.len == 0) return;
        if (dst.merge) |*m| {
            try m.settle();
            m.deinit();
        }
    }

    fn combine(_: *anyopaque, _: *anyopaque) anyerror!void {}

    fn finalize(ptr: *anyopaque) anyerror!void {
        const self: *AggSink = @ptrCast(@alignCast(ptr));
        const env = self.env;
        const t_mrg0 = std.time.Instant.now() catch unreachable;
        try self.merge();
        const t_mrg1 = std.time.Instant.now() catch unreachable;
        const sel = try self.topN();
        const sets = try partSets(env.arena, self.parts);
        var groups: usize = 0;
        for (sets) |st| groups += st.len;
        env.log.log(.debug, "parallel aggregate: fold {d}ms, merge {d}ms, {d} groups in {d} partitions", .{ t_mrg0.since(self.t0) / 1_000_000, t_mrg1.since(t_mrg0) / 1_000_000, groups, sets.len });

        const after = try chainAt(env, self.la.out_schema.*, self.tail);
        const snk = try openTailSink(env, self.w, after.out.*);
        var guard = SinkGuard.init(snk);
        defer guard.abort();
        const span = trace.begin("aggregate emit and write", 0);
        try writeGroups(env, snk, self.la.emitter(env), sets, sel, after.recs, self.threads, self.stats);
        span.end();
        if (env.profile) |p| {
            try p.phase("fold on the lanes", t_mrg0.since(self.t0));
            try p.phase("merge the lanes' groups", t_mrg1.since(t_mrg0));
            try p.phase("emit and write the groups", (std.time.Instant.now() catch unreachable).since(t_mrg1));
        }
        try guard.close();
    }

    /// What the workers left unmerged, a partition per task.
    fn merge(self: *AggSink) !void {
        const span = trace.begin("aggregate merge", 0);
        defer span.end();
        var ctx = FinishCtx{ .g = self, .queue = .{ .nitems = self.mergedParts() } };
        _ = try parallel.spawnJoin(self.env.arena, @min(self.threads, ctx.queue.nitems), finishWorker, &ctx);
        if (ctx.queue.failure()) |e| return e;
    }

    const FinishCtx = struct { g: *AggSink, queue: WorkQueue };

    const finishWorker = dispatchWorker(FinishCtx, finishOne);

    fn finishOne(ctx: *FinishCtx, p: usize) !void {
        return ctx.g.finishPart(p);
    }

    /// Each partition's best groups when the tail is only ORDER BY and LIMIT, so the
    /// emit skips the rest (a full sort of the merged groups was 3x slower than serial).
    fn topN(self: *AggSink) !?[]const []const u32 {
        const arena = self.env.arena;
        const tn = topNTail(self.tail) orelse return null;
        var keys = std.array_list.Managed(op.Sort.Key).init(arena);
        for (tn.keys) |k| {
            const idx = self.la.out_schema.resolve(k.field.parts) orelse return null;
            try keys.append(.{ .idx = idx, .desc = k.desc, .nulls_first = k.nulls_first });
        }
        var tctx = PqTopNCtx{
            .parts = self.parts,
            .order = .{ .keys = keys.items, .by_len = self.la.by.len, .aggs = self.la.aggs },
            .n = tn.n,
            .queue = .{ .nitems = pq_parts },
            .sel = try arena.alloc([]const u32, pq_parts),
        };
        _ = try parallel.spawnJoin(arena, @min(self.threads, pq_parts), pqTopNWorker, &tctx);
        if (tctx.queue.failure()) |e| return e;
        var kept = std.array_list.Managed([]const u32).init(arena);
        for (self.parts, tctx.sel) |*pp, sl| {
            if (pp.merge != null) try kept.append(sl);
        }
        return kept.items;
    }
};

/// The aggregate pipeline `shape` over `m`: the morsels through its map stages and
/// joins into the aggregate sink. False sends it to the serial plan: a join's build
/// side past its lane threshold, or partials past --op-memory.
pub fn runParallelAgg(env: *Env, m: *const Morsels, shape: SinkShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    const src_base = env.sources.items.len;
    const pre = try chainAt(env, m.schema().*, shape.prefix);
    var agg_in = pre.out.*;
    var joins: []const LaneJoin = &.{};
    if (shape.joins.len > 0) {
        const chain = (try resolveLaneJoins(env, shape.joins, agg_in)) orelse return false;
        joins = chain.joins;
        agg_in = chain.out_schema;
    }
    var strs = op.Aggregate.StrTable.init(std.heap.page_allocator);
    defer strs.deinit();
    const la = LaneAgg.of(env, try resolveAt(env, agg_in, shape.agg), &strs);
    if (m.* != .sql) env.src_name = m.label();
    env.sink_name = sinkLabel(env, w);

    var g = try AggSink.init(env, la, m.count(), shape.tail, w, opts.lanes(), stats);
    defer g.deinit();
    const p: Pipeline = .{ .m = m, .recs = pre.recs, .joins = joins, .errctx = env.errctx, .rows_read = env.rows_read };
    const used = drive(env.arena, p, g.parallelSink(), opts.lanes()) catch |e| {
        if (e != error.AggregatePastMemory) return e;
        for (env.sources.items[src_base..]) |sc| sc.close();
        env.sources.shrinkRetainingCapacity(src_base);
        env.log.log(.info, "the aggregate's partials are past {d} bytes (--op-memory); it runs serially, where it can spill to disk", .{env.op_memory});
        return false;
    };
    lanes_used.* = @max(lanes_used.*, used);
    if (env.profile) |pr| {
        for (joins) |lj| if (lj.build_root) |b| try pr.graftBuild(lj.at, b);
        if (m.* == .sql) {
            pr.path = try msgfmt.allocPrint(env.arena, "{s} split-parallel aggregate, {d} key ranges", .{ m.label(), m.count() });
            try pr.noteSent("{s}, once per key range: {s}", .{ m.label(), m.sql.split.base_sql });
        } else pr.path = try msgfmt.allocPrint(env.arena, "{s} parallel aggregate{s}, {d} {s}", .{ m.label(), if (joins.len > 0) " over joins" else "", m.count(), m.unitName() });
    }
    return true;
}

/// A SQL read's aggregate over key ranges, one connection per range, each range's
/// query narrowed to the projection and filters `pushdown.planAgg` allows. False
/// falls back; with the serial plan's sources already closed when it gave up late.
pub fn runParallelSqlAgg(env: *Env, pipe: *const Pipe, stages: []const ast.Stage, shape: SinkShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize, src_base: usize) anyerror!bool {
    const arena = env.arena;
    const desc = pipe.sql_desc orelse return false;
    if (stages[0].node != .read) return false;
    if (w.mode == .upsert and w.mode.upsert.keys.len == 0) return false;
    if (src_base >= env.sources.items.len) return false;

    const src_schema = try schemaPtr(arena, try dupeSchema(arena, env.sources.items[src_base].schema()));
    const facts = try connect_mod.factsIfWanted(env, stages[0].node.read, desc.dialect, shape.prefix, src_schema.*, true, .superset);
    const pd = try pushdown.planAgg(arena, desc.dialect, src_schema.*, shape.prefix, shape.agg.node.aggregate, facts);
    const eff = if (pd.proj_schema) |ps| try schemaPtr(arena, ps) else src_schema;
    const sp = (try planSplit(env, desc, stages[0], opts.lanes(), w)) orelse return false;

    for (env.sources.items[src_base..]) |sc| sc.close();
    env.sources.shrinkRetainingCapacity(src_base);

    const split = try arena.create(SplitCtx);
    split.* = .{ .gpa = env.gpa, .kind = desc.kind, .cfg = desc.cfg, .base_sql = sp.base_sql, .proj_select = pd.proj_select, .where_extra = pd.where_extra, .report = try connect_mod.readReport(env, @tagName(desc.kind)), .want = eff };
    env.log.log(.debug, "parallel sql aggregate: {d} key ranges (projection: {d}/{d} cols, filter pushdown: {s})", .{
        sp.predicates.len,
        eff.fields.len,
        src_schema.fields.len,
        if (pd.where_extra != null) "yes" else "no",
    });
    const m = Morsels{ .sql = .{ .split = split, .predicates = sp.predicates, .schema = eff } };
    return runParallelAgg(env, &m, shape, w, opts, stats, lanes_used);
}
