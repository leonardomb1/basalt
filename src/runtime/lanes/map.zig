//! Map lanes (filter, project, a join probe): each lane maps its share, and the
//! output is written in file order, as a serial run writes it.

const Batch = @import("../../exec/batch.zig").Batch;
const Env = @import("../env.zig").Env;
const LaneJoin = @import("join.zig").LaneJoin;
const LaneSplit = @import("sources.zig").LaneSplit;
const RunOptions = @import("../env.zig").RunOptions;
const SplitCtx = @import("../connect.zig").SplitCtx;
const Stats = @import("../env.zig").Stats;
const WorkQueue = @import("../lanes.zig").WorkQueue;
const ast = @import("../../lang/ast.zig");
const buildLaneJoinChain = @import("join.zig").buildLaneJoinChain;
const buildMapChain = @import("../plan.zig").buildMapChain;
const buildParallelSink = @import("../connect.zig").buildParallelSink;
const connect_mod = @import("../connect.zig");
const csvSplitFile = @import("sources.zig").csvSplitFile;
const dispatchWorker = @import("../lanes.zig").dispatchWorker;
const driver = @import("../../connect/driver.zig");
const dupeSchema = @import("../connect.zig").dupeSchema;
const laneRowSource = @import("sources.zig").laneRowSource;
const mapChainSchema = @import("../plan.zig").mapChainSchema;
const obs = @import("../obs.zig");
const op = @import("../../exec/op.zig");
const openSink = @import("../connect.zig").openSink;
const openSqlQuery = @import("../connect.zig").openSqlQuery;
const parallel = @import("../parallel.zig");
const parquetSplit = @import("sources.zig").parquetSplit;
const planSplit = @import("../connect.zig").planSplit;
const resolveLaneJoin = @import("join.zig").resolveLaneJoin;
const keyExprs = @import("../plan.zig").keyExprs;
const keypush = @import("../keypush.zig");
const resolveUpsertKeys = @import("../connect.zig").resolveUpsertKeys;
const schemaPtr = @import("../env.zig").schemaPtr;
const sinkLabel = @import("../connect.zig").sinkLabel;
const sqlDescForStage = @import("../connect.zig").sqlDescForStage;
const std = @import("std");
const types = @import("../../lang/types.zig");
const wrapProjected = @import("../../connect/split.zig").wrapProjected;

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

pub const OrderedOut = struct {
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

    pub fn init(arena: std.mem.Allocator, nunits: usize, window: usize, snk: driver.Sink) !OrderedOut {
        const done = try arena.alloc(?*Unit, nunits);
        @memset(done, null);
        return .{ .window = window, .done = done, .snk = snk };
    }

    /// Block until unit `i` is within the window, or the run has failed.
    pub fn waitTurn(self: *OrderedOut, i: usize, failed: *std.atomic.Value(bool)) void {
        self.mtx.lock();
        defer self.mtx.unlock();
        while (i >= self.next + self.window and !failed.load(.seq_cst)) self.cv.wait(&self.mtx);
    }

    pub fn wake(self: *OrderedOut) void {
        self.mtx.lock();
        self.cv.broadcast();
        self.mtx.unlock();
    }

    /// Hand over unit `i`. With no lane writing, this one writes every consecutive finished
    /// unit from `next` on, outside `mtx`, so handing over never waits on the sink.
    pub fn deposit(self: *OrderedOut, i: usize, u: *Unit) !void {
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

    pub fn takeUnit(self: *OrderedOut) !*Unit {
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

    pub fn freeUnit(u: *Unit) void {
        u.arena.deinit();
        std.heap.page_allocator.destroy(u);
    }

    pub fn freeRest(self: *OrderedOut) void {
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
        const lp = (try resolveLaneJoin(env, js.join, js.join_hints, js.suffix, out_schema)) orelse return false;
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

pub fn runParallelParquetMap(env: *Env, rd: ast.Read, pipeline: []const ast.Stage, map_stages: []const ast.Stage, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    return runParallelParquetMapImpl(env, rd, pipeline, map_stages, null, w, opts, stats, lanes_used);
}

pub fn runParallelParquetMapJoin(env: *Env, rd: ast.Read, pipeline: []const ast.Stage, shape: MapJoinShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    if (!joinKindLaneSafe(shape.join.kind)) return false;
    return runParallelParquetMapImpl(env, rd, pipeline, shape.prefix, shape, w, opts, stats, lanes_used);
}

fn runParallelParquetMapImpl(env: *Env, rd: ast.Read, pipeline: []const ast.Stage, map_stages: []const ast.Stage, jshape: ?MapJoinShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    const split = (try parquetSplit(env, rd, pipeline[1..][0..map_stages.len], w, opts)) orelse return false;
    return runParallelMapImpl(env, split, map_stages, jshape, w, opts, stats, lanes_used);
}

pub fn classifyMapPipeline(stages: []const ast.Stage) ?[]const ast.Stage {
    const middle = stages[1 .. stages.len - 1];
    for (middle) |st| switch (st.node) {
        .filter, .select => {},
        else => return null,
    };
    return middle;
}

pub const MapJoinShape = struct { prefix: []const ast.Stage, join: ast.Join, join_hints: []const ast.Hint, suffix: []const ast.Stage };

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

    const lp = (try resolveLaneJoin(env, shape.join, shape.join_hints, shape.suffix, probe_schema)) orelse return false;

    var where_extra: ?[]const u8 = null;
    if (keypush.leftMayNarrow(shape.join) and !keypush.disabled(shape.join_hints)) {
        const trace = try std.mem.concat(arena, ast.Stage, &.{ shape.prefix, lp.lane.probe_prep });
        if (try keyExprs(arena, trace, lp.lane.left_key_names)) |exprs| {
            const keys = try op.collectKeys(arena, &.{lp.lane.index.build_batch}, lp.lane.right_keys, op.default_push_cap);
            where_extra = try keypush.render(arena, desc.dialect, src_schema.*, exprs, keys);
            if (where_extra) |x| env.log.log(.debug, "key pushdown into {s} splits: WHERE {s}", .{ @tagName(desc.dialect), if (x.len > 400) x[0..400] else x });
        }
    }

    env.sink_name = sinkLabel(env, w);
    const sink_mode: parallel.SinkMode = (try buildParallelSink(env, w, lp.out_schema)) orelse
        .{ .shared = try openSink(env, w, lp.out_schema) };
    var shared_open = sink_mode == .shared;
    errdefer if (shared_open) sink_mode.shared.abort();

    var ctx = SqlMapJoinCtx{
        .split = .{ .gpa = env.gpa, .kind = desc.kind, .cfg = desc.cfg, .base_sql = sp.base_sql, .where_extra = where_extra, .report = try connect_mod.readReport(env, @tagName(desc.kind)) },
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

pub fn runParallelCsvMap(env: *Env, rd: ast.Read, map_stages: []const ast.Stage, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    return runParallelCsvMapImpl(env, rd, map_stages, null, w, opts, stats, lanes_used);
}

pub fn runParallelCsvMapJoin(env: *Env, rd: ast.Read, shape: MapJoinShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    if (!joinKindLaneSafe(shape.join.kind)) return false;
    return runParallelCsvMapImpl(env, rd, shape.prefix, shape, w, opts, stats, lanes_used);
}

fn runParallelCsvMapImpl(env: *Env, rd: ast.Read, map_stages: []const ast.Stage, jshape: ?MapJoinShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    const mapped = (try csvSplitFile(env, rd, w)) orelse return false;
    defer mapped.close();
    return runParallelMapImpl(env, .{ .csv = .{ .mapped = mapped, .schema = &mapped.schema } }, map_stages, jshape, w, opts, stats, lanes_used);
}

test "join kinds allowed on the parallel probe path" {
    try std.testing.expect(joinKindLaneSafe(.inner));
    try std.testing.expect(joinKindLaneSafe(.left));
    try std.testing.expect(joinKindLaneSafe(.semi));
    try std.testing.expect(joinKindLaneSafe(.anti));
    try std.testing.expect(joinKindLaneSafe(.cross));
    try std.testing.expect(!joinKindLaneSafe(.right));
    try std.testing.expect(!joinKindLaneSafe(.full));
}
