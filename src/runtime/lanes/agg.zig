//! Aggregate lanes: each lane folds its share into a private group table, the tables
//! merge by hash partition, and the groups are written in order; the Parquet morsel
//! path, the join-then-aggregate shape, and the CSV, Parquet and SQL entry points.

const Batch = @import("../../exec/batch.zig").Batch;
const Env = @import("../env.zig").Env;
const LaneJoin = @import("join.zig").LaneJoin;
const OneBatch = @import("../connect.zig").OneBatch;
const OrderedOut = @import("map.zig").OrderedOut;
const PqTopNCtx = @import("topn.zig").PqTopNCtx;
const RunOptions = @import("../env.zig").RunOptions;
const SplitCtx = @import("../connect.zig").SplitCtx;
const Stats = @import("../env.zig").Stats;
const WorkQueue = @import("../lanes.zig").WorkQueue;
const aErr = @import("../plan.zig").aErr;
const analyze = @import("../analyze.zig");
const ast = @import("../../lang/ast.zig");
const buildLaneJoinChain = @import("join.zig").buildLaneJoinChain;
const buildMapChain = @import("../plan.zig").buildMapChain;
const buildStage = @import("../plan.zig").buildStage;
const buildTopN = @import("../plan.zig").buildTopN;
const connect_mod = @import("../connect.zig");
const csv = @import("../../format/csv.zig");
const csvSplitFile = @import("sources.zig").csvSplitFile;
const dispatchWorker = @import("../lanes.zig").dispatchWorker;
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
const parquetSplit = @import("sources.zig").parquetSplit;
const planSplit = @import("../connect.zig").planSplit;
const pqTopNWorker = @import("topn.zig").pqTopNWorker;
const pqdecode = @import("../../format/parquet/read.zig");
const pushdown = @import("../pushdown.zig");
const resolveLaneJoins = @import("join.zig").resolveLaneJoins;
const resolveUpsertKeys = @import("../connect.zig").resolveUpsertKeys;
const schemaPtr = @import("../env.zig").schemaPtr;
const sinkLabel = @import("../connect.zig").sinkLabel;
const std = @import("std");
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

pub fn writeTail(env: *Env, snk: driver.Sink, batch: Batch, schema: types.Schema, tail: []const ast.Stage, stats: *Stats) !void {
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

pub const PqPart = struct {
    arena: std.heap.ArenaAllocator,
    merge: ?op.Aggregate.GroupMerge = null,
};

const pq_parts: usize = op.Aggregate.fold_parts;

pub const pq_min_lanes: usize = 2;

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
        const r = try pqdecode.Reader.openProjected(scratch, path, m.project);
        if (m.check_in_lane and it.file != 0) if (pqdecode.schemaMismatch(scratch, m.src_schema.*, r.schema)) |why| {
            r.close();
            return eval.explain(error.ParquetFolderMismatch, pqdecode.mismatchMessage(scratch, m.root orelse "", path, m.files[0], why));
        };
        held.* = .{ .file = it.file, .r = r };
    }
    const r = held.*.?.r;
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
    pub const vtable = driver.Source.VTable{ .schema = schemaFn, .next = nextFn, .close = closeFn };
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

/// Parallel aggregate over parquet, one morsel per row group, folded into private tables
/// and merged by hash partition. False when too few row groups or lanes.
pub fn runParallelParquetAgg(env: *Env, rd: ast.Read, pipeline: []const ast.Stage, prefix: []const ast.Stage, ag: ast.Aggregate, tail: []const ast.Stage, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    return runParallelParquetAggImpl(env, rd, pipeline, prefix, ag, tail, null, w, opts, stats, lanes_used);
}

pub fn runParallelParquetAggJoin(env: *Env, rd: ast.Read, pipeline: []const ast.Stage, js: AggJoinShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
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
        const chain = (try resolveLaneJoins(env, js.join_span, agg_in_schema)) orelse return false;
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

pub fn runParallelCsvAgg(env: *Env, rd: ast.Read, prefix: []const ast.Stage, ag: ast.Aggregate, tail: []const ast.Stage, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    return runParallelCsvAggImpl(env, rd, prefix, ag, tail, null, w, opts, stats, lanes_used);
}

pub fn runParallelCsvAggJoin(env: *Env, rd: ast.Read, js: AggJoinShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    return runParallelCsvAggImpl(env, rd, js.map_stages, js.ag, js.tail, js, w, opts, stats, lanes_used);
}

fn runParallelCsvAggImpl(env: *Env, rd: ast.Read, prefix: []const ast.Stage, ag: ast.Aggregate, tail: []const ast.Stage, jshape: ?AggJoinShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    const arena = env.arena;
    const mapped = (try csvSplitFile(env, rd, w)) orelse return false;
    defer mapped.close();

    var agg_in_schema = try mapChainSchema(env, prefix, mapped.schema);
    var lane_joins: []const LaneJoin = &.{};
    if (jshape) |js| {
        const chain = (try resolveLaneJoins(env, js.join_span, agg_in_schema)) orelse return false;
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
