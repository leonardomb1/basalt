//! Aggregate lanes: each lane folds its share into a private group table, the tables
//! merge by hash partition, and the groups are written in order; the Parquet morsel
//! path, the join-then-aggregate shape, and the CSV, Parquet and SQL entry points.

const Batch = @import("../../exec/batch.zig").Batch;
const Slicer = @import("../../exec/batch.zig").Slicer;
const Env = @import("../env.zig").Env;
const LaneJoin = @import("join.zig").LaneJoin;
const OneBatch = @import("../connect.zig").OneBatch;
const OrderedOut = @import("map.zig").OrderedOut;
const Profile = @import("../../exec/profile.zig").Profile;
const PqTopNCtx = @import("topn.zig").PqTopNCtx;
const RunOptions = @import("../env.zig").RunOptions;
const SplitCtx = @import("../connect.zig").SplitCtx;
const Stats = @import("../env.zig").Stats;
const LaneMem = @import("../parallel.zig").LaneMem;
const LaneSplit = @import("sources.zig").LaneSplit;
const WorkQueue = @import("../parallel.zig").WorkQueue;
const orderedWorker = @import("map.zig").orderedWorker;
const aErr = @import("../env.zig").aErr;
const analyze = @import("../analyze.zig");
const ast = @import("../../lang/ast.zig");
const buildLaneJoinChain = @import("join.zig").buildLaneJoinChain;
const buildMapChain = @import("../plan.zig").buildMapChain;
const buildStage = @import("../plan.zig").buildStage;
const buildTopN = @import("../plan.zig").buildTopN;
const connect_mod = @import("../connect.zig");
const csv = @import("../../format/csv.zig");
const dispatchWorker = @import("../parallel.zig").dispatchWorker;
const driver = @import("../../connect/driver.zig");
const dupeSchema = @import("../connect.zig").dupeSchema;
const eval = @import("../../exec/eval.zig");
const joinKindLaneSafe = @import("map.zig").joinKindLaneSafe;
const mapChainSchema = @import("../plan.zig").mapChainSchema;
const obs = @import("../obs.zig");
const op = @import("../../exec/op.zig");
const openSink = @import("../connect.zig").openSink;
const openSqlQuery = @import("../connect.zig").openSqlQuery;
const parallel = @import("../parallel.zig");
const planSplit = @import("../connect.zig").planSplit;
const pqTopNWorker = @import("topn.zig").pqTopNWorker;
const pqdecode = @import("../../format/parquet/read.zig");
const profile = @import("../../exec/profile.zig");
const pushdown = @import("../pushdown.zig");
const resolveLaneJoins = @import("join.zig").resolveLaneJoins;
const resolveUpsertKeys = @import("../connect.zig").resolveUpsertKeys;
const schemaPtr = @import("../env.zig").schemaPtr;
const sinkLabel = @import("../connect.zig").sinkLabel;
const std = @import("std");
const trace = @import("../../exec/trace.zig");
const tailSchema = @import("../plan.zig").tailSchema;
const topNTail = @import("topn.zig").topNTail;
const types = @import("../../lang/types.zig");
const wrapProjected = @import("../../connect/split.zig").wrapProjected;
const classifyAggPipeline = @import("../lanes.zig").classifyAggPipeline;
const testStages = @import("testing_util.zig").testStages;

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
                if (m == null) {
                    m = try op.Aggregate.GroupMerge.init(env.arena, st, aggs);
                    m.?.threads = threads;
                }
                try m.?.addSet(st);
            }
        }
        defer if (m) |*mm| mm.deinit();
        const one = try env.arena.alloc(op.Aggregate.GroupSet, if (m == null) 0 else 1);
        if (m) |*mm| one[0] = try mm.result();
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
        if (pp.merge) |*m| try sets.append(try m.result());
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

/// The aggregate every lane folds: its input and output schemas, keys and accumulators.
const LaneAgg = struct {
    strs: *op.Aggregate.StrTable,
    in_schema: *const types.Schema,
    out_schema: *const types.Schema,
    by: []const usize,
    aggs: []const op.Aggregate.Agg,
    prof: ?*Profile = null,

    fn plan(env: *Env, in_schema: types.Schema, ag: ast.Aggregate, strs: *op.Aggregate.StrTable) !LaneAgg {
        const arena = env.arena;
        const agg_in = try schemaPtr(arena, in_schema);
        var ad = analyze.Diag{};
        const apl = analyze.aggregatePlan(arena, agg_in.*, ag, env.params_expr, &ad) catch |e| return aErr(env, &ad, e);
        const aggs = try arena.alloc(op.Aggregate.Agg, apl.aggs.len);
        for (apl.aggs, aggs) |ra, *a| a.* = .{ .func = ra.func, .arg = ra.arg, .ty = ra.ty, .distinct = ra.distinct, .arg_ty = ra.arg_ty };
        return .{ .strs = strs, .in_schema = agg_in, .out_schema = try schemaPtr(arena, apl.schema), .by = apl.by, .aggs = aggs, .prof = env.profile };
    }

    /// Fold `child` into private group tables, one per hash partition of `parts`.
    fn fold(self: LaneAgg, child: op.Op, state: std.mem.Allocator, gpa: std.mem.Allocator, parts: *LaneParts) ![]const op.Aggregate.GroupSet {
        var agg = op.Aggregate{
            .strs = self.strs,
            .child = child,
            .in_schema = self.in_schema,
            .by = self.by,
            .aggs = self.aggs,
            .out_schema = self.out_schema,
            .err = null,
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

/// The sink for `w`, typed by what `tail` makes of `schema`.
pub fn openTailSink(env: *Env, w: ast.Write, tail: []const ast.Stage, schema: types.Schema) !driver.Sink {
    return openSink(env, try resolveUpsertKeys(env, w), try tailSchema(env, tail, schema));
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
        return writeTail(env, snk, batch, emitter.out_schema.*, tail, stats, .aggregate);
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
    tail: []const ast.Stage,
    params: *std.StringHashMap(*const ast.Expr),
    errctx: ?*op.ErrCtx = null,
    queue: WorkQueue,
    ordered: ?*OrderedOut,
    rows_out: std.atomic.Value(u64) = .init(0),
    prof: ?*Profile = null,
    top: ?profile.Id = null,
    top_mtx: std.Thread.Mutex = .{},
};

const groupWriteWorker = orderedWorker(GroupWriteCtx, groupWriteUnit);

fn groupWriteUnit(ctx: *GroupWriteCtx, i: usize, mem: *LaneMem) !void {
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
        const chain = try buildMapChain(wa, ctx.params, ctx.errctx, ctx.tail, &scan, out_schema);
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
pub fn writeTail(env: *Env, snk: driver.Sink, batch: Batch, schema: types.Schema, tail: []const ast.Stage, stats: *Stats, feeds: profile.Kind) !void {
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
    var sch = schema;
    var i: usize = 0;
    while (i < tail.len) : (i += 1) {
        const r = if (tail[i].node == .sort and i + 1 < tail.len and tail[i + 1].node == .limit) blk: {
            i += 1;
            break :blk try buildTopN(env, tail[i - 1].node.sort, tail[i].node.limit, cur, sch);
        } else try buildStage(env, tail[i], cur, sch);
        cur = r.op;
        cur.stampNew(profile.keyOf(tail[if (cur == .top_n) i - 1 else i].pos));
        sch = r.schema;
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

const AggCtx = struct {
    agg: LaneAgg,
    mapped: *csv.MappedCsv,
    csv_schema: *const types.Schema,
    prefix: []const ast.Stage,
    params: *std.StringHashMap(*const ast.Expr),
    errctx: ?*op.ErrCtx = null,
    queue: WorkQueue,
    slots: []AggSlot,
    rows_read: *obs.RowCounter,
    joins: []const LaneJoin = &.{},
};

const aggWorker = dispatchWorker(AggCtx, aggWorkOne);

fn aggWorkOne(ctx: *AggCtx, i: usize) !void {
    const slot = &ctx.slots[i];
    const wa = slot.arena.allocator();

    var cs = obs.CountingSource{ .inner = try ctx.mapped.lane(wa, i, ctx.queue.nitems, ctx.csv_schema), .count = ctx.rows_read };
    var scan = op.Scan{ .src = cs.source() };
    var child = try buildMapChain(wa, ctx.params, ctx.errctx, ctx.prefix, &scan, ctx.csv_schema);
    for (ctx.joins) |lj| child = try buildLaneJoinChain(wa, ctx.params, ctx.errctx, lj, child);
    slot.sets = try ctx.agg.fold(child, wa, slot.gpa.allocator(), &slot.parts);
}

const PqLane = struct {
    arena: std.heap.ArenaAllocator,
    parts: LaneParts = undefined,
    sets: []const op.Aggregate.GroupSet = &.{},
};

pub const PqPart = struct {
    arena: std.heap.ArenaAllocator,
    merge: ?op.Aggregate.GroupMerge = null,
};

const pq_parts: usize = op.Aggregate.fold_parts;

pub const pq_min_lanes: usize = 2;

const PqAggCtx = struct {
    agg: LaneAgg,
    morsels: PqMorsels,
    prefix: []const ast.Stage,
    params: *std.StringHashMap(*const ast.Expr),
    errctx: ?*op.ErrCtx = null,
    lanes: []PqLane,
    rows_read: *obs.RowCounter,
    joins: []const LaneJoin = &.{},
};

pub const PqMorsels = struct {
    files: []const []const u8,
    root: ?[]const u8 = null,
    items: []const PqItem,
    project: ?[][]const u8,
    bounds: []const pqdecode.Bound,
    keys: []const pqdecode.KeyBound = &.{},
    src_schema: *const types.Schema,
    queue: WorkQueue,
    tally: ?*driver.ScanTally = null,
    max_lanes: usize = std.math.maxInt(usize),
    check_in_lane: bool = false,
    /// A single remote file's reader as planning opened it: lanes reuse its footer
    /// and size, each on its own connection.
    like: ?*const pqdecode.Reader = null,
    /// A table's files, each opened fitted to `view`, the table's projected columns.
    layout: ?*const pqdecode.Layout = null,
    view: ?*const pqdecode.View = null,
};

pub const PqItem = struct { file: u32, rg: u32, rg_end: ?u32 };

pub const HeldFile = struct { file: u32, r: *pqdecode.Reader };

/// The reader positioned on item `i`, reusing `held` when it is the same file (reopening
/// per row group re-parsed the footer); null when a file shrank since it was planned.
pub fn openItem(m: *const PqMorsels, scratch: std.mem.Allocator, held: *?HeldFile, i: usize) !?*pqdecode.Reader {
    const it = m.items[i];
    if (held.*) |h| if (h.file != it.file) {
        h.r.close();
        held.* = null;
    };
    if (held.* == null) {
        const path = m.files[it.file];
        if (m.layout) |lay| {
            held.* = .{ .file = it.file, .r = try lay.open(scratch, m.view.?, it.file, m.bounds) orelse return null };
            return positioned(m, held.*.?.r, it);
        }
        const r = if (m.like) |l| try pqdecode.Reader.openLike(scratch, path, l) else try pqdecode.Reader.openProjected(scratch, path, m.project);
        if (m.check_in_lane and it.file != 0) if (pqdecode.schemaMismatch(scratch, m.src_schema.*, r.schema, m.project)) |why| {
            r.close();
            return eval.explain(error.ParquetFolderMismatch, pqdecode.mismatchMessage(scratch, m.root orelse "", path, m.files[0], why));
        };
        held.* = .{ .file = it.file, .r = r };
    }
    return positioned(m, held.*.?.r, it);
}

fn positioned(m: *const PqMorsels, r: *pqdecode.Reader, it: PqItem) ?*pqdecode.Reader {
    r.bounds = m.bounds;
    r.keys = m.keys;
    r.tally = m.tally;
    r.rg = it.rg;
    r.rg_end = if (it.rg_end) |e| e else null;
    if (r.rg >= r.md.row_groups.len) return null;
    return r;
}

pub const MorselSource = struct {
    m: *PqMorsels,
    scratch: std.mem.Allocator,
    held: ?HeldFile = null,
    cur: ?*pqdecode.Reader = null,
    cur_item: usize = 0,
    fixed: ?struct { next: usize, step: usize } = null,
    span: trace.Span = .{ .name = "", .arg = 0, .start = null },
    slicer: Slicer = .{},

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
                if (try self.slicer.read(arena, r)) |b| return b;
                self.cur = null;
                self.span.end();
            }
            const i = (if (self.m.queue.failed.load(.seq_cst)) null else self.nextIndex()) orelse {
                self.slicer.deinit();
                return null;
            };
            self.span = trace.begin("row group", i);
            self.cur = try openItem(self.m, self.scratch, &self.held, i);
            self.cur_item = i;
        }
    }
    fn closeFn(ptr: *anyopaque) void {
        const self: *MorselSource = @ptrCast(@alignCast(ptr));
        if (self.held) |h| h.r.close();
        self.held = null;
        self.cur = null;
        self.slicer.deinit();
    }
    pub const vtable = driver.Source.VTable{ .schema = schemaFn, .next = nextFn, .close = closeFn };
};

fn pqAggLane(ctx: *PqAggCtx, lane_idx: usize) void {
    pqAggLaneRun(ctx, lane_idx) catch |e| ctx.morsels.queue.fail(e);
}

fn pqAggLaneRun(ctx: *PqAggCtx, lane_idx: usize) !void {
    const span = trace.begin("parquet aggregate lane", lane_idx);
    defer span.end();
    const ls = &ctx.lanes[lane_idx];
    const la = ls.arena.allocator();

    var ms = MorselSource{
        .m = &ctx.morsels,
        .scratch = la,
        .fixed = .{ .next = lane_idx, .step = ctx.lanes.len },
    };
    defer ms.slicer.deinit();
    var cs = obs.CountingSource{ .inner = .{ .ptr = &ms, .vtable = &MorselSource.vtable }, .count = ctx.rows_read };
    var scan = op.Scan{ .src = cs.source() };
    var child = try buildMapChain(la, ctx.params, ctx.errctx, ctx.prefix, &scan, ctx.morsels.src_schema);
    for (ctx.joins) |lj| child = try buildLaneJoinChain(la, ctx.params, ctx.errctx, lj, child);
    ls.sets = try ctx.agg.fold(child, la, ls.arena.child_allocator, &ls.parts);
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

/// Merge radix partition `p` of `srcs` (parquet lanes or CSV/SQL slots) into `dst`:
/// one table built from every source in index order, each freed once merged. A lane
/// may hold a group in several records (its table is bounded), so no source's store
/// is adopted as the result.
fn mergeRadixPart(dst: *PqPart, srcs: anytype, aggs: []const op.Aggregate.Agg, p: usize) !void {
    var total: usize = 0;
    var ndistinct: usize = 0;
    for (srcs) |*ls| {
        if (ls.sets.len > p) ndistinct = @max(ndistinct, ls.sets[p].pairs.len);
    }
    const npairs = try dst.arena.allocator().alloc(usize, ndistinct);
    @memset(npairs, 0);
    for (srcs) |*ls| {
        if (ls.sets.len <= p) continue;
        total += ls.sets[p].len;
        for (ls.sets[p].pairs, npairs[0..ls.sets[p].pairs.len]) |*pp, *np| np.* += pp.count(p);
    }
    for (srcs) |*ls| {
        if (ls.sets.len <= p or ls.sets[p].len == 0) continue;
        const st = &ls.sets[p];
        if (dst.merge == null) {
            dst.merge = try op.Aggregate.GroupMerge.initFor(dst.arena.allocator(), st, aggs, .{ .groups = total, .pairs = npairs });
            dst.merge.?.own = true;
        }
        try dst.merge.?.addSet(st);
        st.freeTable();
        ls.parts.free(p);
    }
    if (dst.merge) |*m| {
        try m.settle();
        m.deinit();
    }
}

pub const AggJoinShape = struct {
    map_stages: []const ast.Stage,
    join_span: []const ast.Stage,
    ag: ast.Aggregate,
    tail: []const ast.Stage,
};

pub fn classifyAggJoinPipeline(stages: []const ast.Stage) ?AggJoinShape {
    if (stages.len < 4) return null;
    if (stages[stages.len - 1].node != .write) return null;
    const middle = stages[1 .. stages.len - 1];
    var first_join: ?usize = null;
    var ai: ?usize = null;
    for (middle, 0..) |st, i| switch (st.node) {
        .filter, .select => {},
        .join => |j| {
            if (ai != null) return null;
            if (!joinKindLaneSafe(j.kind) or j.keyless()) return null;
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

/// Parallel aggregate over a split read, folded into private tables and merged by hash
/// partition. False when a join in the span cannot run on lanes.
pub fn runParallelAgg(env: *Env, split: LaneSplit, prefix: []const ast.Stage, ag: ast.Aggregate, tail: []const ast.Stage, jshape: ?AggJoinShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    var agg_in_schema = try mapChainSchema(env, prefix, split.schema().*);
    var lane_joins: []const LaneJoin = &.{};
    if (jshape) |js| {
        const chain = (try resolveLaneJoins(env, js.join_span, agg_in_schema)) orelse return false;
        lane_joins = chain.joins;
        agg_in_schema = chain.out_schema;
    }
    var strs = op.Aggregate.StrTable.init(std.heap.page_allocator);
    defer strs.deinit();
    const la = try LaneAgg.plan(env, agg_in_schema, ag, &strs);

    env.src_name = split.label();
    env.sink_name = sinkLabel(env, w);

    var slots: []AggSlot = &.{};
    defer freeAggSlots(env.gpa, slots);
    var pq_lanes: []PqLane = &.{};
    defer freePqLanes(env.gpa, pq_lanes);
    const parts = try allocMergeParts(env.gpa);
    defer freeMergeParts(env.gpa, parts);

    const nthreads = split.lanes(opts);
    var sel: ?[]const []const u32 = null;
    const sets, const write_threads = switch (split) {
        .csv => |c| blk: {
            slots = try allocAggSlots(env.gpa, nthreads);
            var ctx = AggCtx{
                .agg = la,
                .mapped = c.mapped,
                .csv_schema = c.schema,
                .prefix = prefix,
                .params = env.params_expr,
                .errctx = env.errctx,
                .queue = .{ .nitems = nthreads },
                .slots = slots,
                .rows_read = env.rows_read,
                .joins = lane_joins,
            };
            const lanes = try parallel.spawnJoin(env.arena, nthreads, aggWorker, &ctx);
            lanes_used.* = @max(lanes_used.*, lanes);
            env.log.log(.debug, "parallel csv aggregate: {d} chunks over {d} lanes", .{ nthreads, lanes });
            if (ctx.queue.failure()) |e| return e;
            const t_mrg0 = std.time.Instant.now() catch unreachable;
            const combined = try combineAggSlots(env, slots, la.aggs, opts.lanes(), parts);
            if (env.profile) |p| try p.phase("merge the lanes' groups", (std.time.Instant.now() catch unreachable).since(t_mrg0));
            break :blk .{ combined, opts.lanes() };
        },
        .parquet => |m| blk: {
            pq_lanes = try allocPqLanes(env.gpa, nthreads);
            sel = try foldParquet(env, la, m, prefix, lane_joins, tail, pq_lanes, parts, lanes_used);
            break :blk .{ try partSets(env.arena, parts), nthreads };
        },
    };

    const snk = try openTailSink(env, w, tail, la.out_schema.*);
    var snk_open = true;
    errdefer if (snk_open) snk.abort();
    const t_emit0 = std.time.Instant.now() catch unreachable;
    const emit_span = trace.begin("aggregate emit and write", 0);
    try writeGroups(env, snk, la.emitter(env), sets, sel, tail, write_threads, stats);
    emit_span.end();
    const t_emit1 = std.time.Instant.now() catch unreachable;
    if (split == .parquet) {
        var total: usize = 0;
        for (sets) |st| total += st.len;
        env.log.log(.debug, "pq agg tail: emit+write {d}ms ({d} groups)", .{ t_emit1.since(t_emit0) / 1_000_000, total });
    }
    if (env.profile) |p| {
        try p.phase("emit and write the groups", t_emit1.since(t_emit0));
        for (lane_joins) |lj| if (lj.build_root) |b| try p.graftBuild(lj.at, b);
        p.path = try std.fmt.allocPrint(env.arena, "{s} parallel aggregate{s}, {d} {s}", .{ split.label(), if (lane_joins.len > 0) " over joins" else "", split.count(nthreads), split.unitName() });
    }
    snk_open = false;
    try snk.close();
    return true;
}

fn allocPqLanes(gpa: std.mem.Allocator, n: usize) ![]PqLane {
    const lanes = try gpa.alloc(PqLane, n);
    for (lanes) |*l| {
        l.* = .{ .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator) };
        l.parts.init(gpa);
    }
    return lanes;
}

fn freePqLanes(gpa: std.mem.Allocator, lanes: []PqLane) void {
    for (lanes) |*l| {
        for (l.sets) |*st| st.freeTable();
        l.parts.deinit();
        l.arena.deinit();
    }
    gpa.free(lanes);
}

/// Fold parquet morsels on `lanes`, one fixed slice of row groups each, and merge them
/// into `parts`; returns each partition's top-N selection when the tail allows one.
fn foldParquet(env: *Env, la: LaneAgg, morsels: PqMorsels, prefix: []const ast.Stage, joins: []const LaneJoin, tail: []const ast.Stage, lanes: []PqLane, parts: []PqPart, lanes_used: *usize) !?[]const []const u32 {
    const arena = env.arena;
    const ngroups = morsels.queue.nitems;
    const nthreads = lanes.len;
    var ctx = PqAggCtx{
        .agg = la,
        .morsels = morsels,
        .prefix = prefix,
        .params = env.params_expr,
        .errctx = env.errctx,
        .lanes = lanes,
        .rows_read = env.rows_read,
        .joins = joins,
    };

    const t_fold0 = std.time.Instant.now() catch unreachable;
    const used = try parallel.spawnJoin(arena, nthreads, pqAggLane, &ctx);
    const t_fold1 = std.time.Instant.now() catch unreachable;
    lanes_used.* = @max(lanes_used.*, used);
    if (ctx.morsels.queue.failure()) |e| return e;

    const t_mrg0 = std.time.Instant.now() catch unreachable;
    const merge_span = trace.begin("aggregate merge", 0);
    if (la.by.len == 0) {
        const dst = &parts[0];
        for (lanes) |*l| {
            for (l.sets) |*st| {
                if (dst.merge == null) {
                    dst.merge = try op.Aggregate.GroupMerge.init(dst.arena.allocator(), st, la.aggs);
                    dst.merge.?.threads = nthreads;
                }
                try dst.merge.?.addSet(st);
            }
        }
    } else {
        var mctx = PqMergeCtx{ .lanes = lanes, .parts = parts, .aggs = la.aggs, .queue = .{ .nitems = pq_parts } };
        _ = try parallel.spawnJoin(arena, nthreads, pqMergeWorker, &mctx);
        if (mctx.queue.failure()) |e| return e;
    }
    merge_span.end();
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
    if (env.profile) |p| {
        try p.phase("fold on the lanes", t_fold1.since(t_fold0));
        try p.phase("merge the lanes' groups", t_mrg1.since(t_mrg0));
    }

    env.log.log(.debug, "parallel parquet aggregate: {d} row groups in {d} morsels over {d} lanes, merged in {d} partitions", .{ ngroups, ngroups, used, pq_parts });

    const tn = topNTail(tail) orelse return null;
    var keys = std.array_list.Managed(op.Sort.Key).init(arena);
    for (tn.keys) |k| {
        const idx = la.out_schema.resolve(k.field.parts) orelse return null;
        try keys.append(.{ .idx = idx, .desc = k.desc, .nulls_first = k.nulls_first });
    }
    var tctx = PqTopNCtx{
        .parts = parts,
        .order = .{ .keys = keys.items, .by_len = la.by.len, .aggs = la.aggs },
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
    return kept.items;
}

const SqlAggCtx = struct {
    agg: LaneAgg,
    split: SplitCtx,
    predicates: []const []const u8,
    proj_select: ?[]const u8,
    where_extra: ?[]const u8,
    src_schema: *const types.Schema,
    prefix: []const ast.Stage,
    params: *std.StringHashMap(*const ast.Expr),
    errctx: ?*op.ErrCtx = null,
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
    slot.sets = try ctx.agg.fold(child, wa, slot.gpa.allocator(), &slot.parts);
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
    const pd = try pushdown.planAgg(arena, desc.dialect, src_schema.*, prefix, ag, facts);
    const eff = if (pd.proj_schema) |ps| try schemaPtr(arena, ps) else src_schema;

    var strs = op.Aggregate.StrTable.init(std.heap.page_allocator);
    defer strs.deinit();
    const la = try LaneAgg.plan(env, try mapChainSchema(env, prefix, eff.*), ag, &strs);

    const sp = (try planSplit(env, desc, stages[0], opts.lanes(), w)) orelse return false;

    for (env.sources.items[src_base..]) |sc| sc.close();
    env.sources.shrinkRetainingCapacity(src_base);

    env.sink_name = sinkLabel(env, w);

    const nlanes = @min(@max(@as(usize, 1), opts.lanes()), sp.predicates.len);
    const slots = try allocAggSlots(env.gpa, sp.predicates.len);
    defer freeAggSlots(env.gpa, slots);
    const parts = try allocMergeParts(env.gpa);
    defer freeMergeParts(env.gpa, parts);
    var ctx = SqlAggCtx{
        .agg = la,
        .split = .{ .gpa = env.gpa, .kind = desc.kind, .cfg = desc.cfg, .base_sql = sp.base_sql, .report = try connect_mod.readReport(env, @tagName(desc.kind)) },
        .predicates = sp.predicates,
        .proj_select = pd.proj_select,
        .where_extra = pd.where_extra,
        .src_schema = eff,
        .prefix = prefix,
        .params = env.params_expr,
        .errctx = env.errctx,
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

    const t_mrg0 = std.time.Instant.now() catch unreachable;
    const csets = try combineAggSlots(env, slots, la.aggs, opts.lanes(), parts);
    const t_emit0 = std.time.Instant.now() catch unreachable;

    const snk = try openTailSink(env, w, tail, la.out_schema.*);
    var snk_open = true;
    errdefer if (snk_open) snk.abort();
    try writeGroups(env, snk, la.emitter(env), csets, null, tail, opts.lanes(), stats);
    if (env.profile) |p| {
        try p.phase("merge the lanes' groups", t_emit0.since(t_mrg0));
        try p.phase("emit and write the groups", (std.time.Instant.now() catch unreachable).since(t_emit0));
        p.path = try std.fmt.allocPrint(env.arena, "{s} split-parallel aggregate, {d} key ranges", .{ @tagName(desc.kind), sp.predicates.len });
        try p.noteSent("{s}, once per key range: {s}", .{ @tagName(desc.kind), sp.base_sql });
    }
    snk_open = false;
    try snk.close();
    return true;
}

test "classifyAggJoinPipeline accepts a HAVING tail, as classifyAggPipeline does" {
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
