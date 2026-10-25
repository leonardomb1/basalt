//! Map lanes (filter, project, a join probe): each lane maps its share, and the
//! output is written in file order, as a serial run writes it. A right or full
//! join's lanes mark one shared match bitset; its unmatched build rows are written
//! once by the driver after every lane is done, so they come last, as serially.

const Batch = @import("../../exec/batch.zig").Batch;
const Env = @import("../env.zig").Env;
const LaneJoin = @import("join.zig").LaneJoin;
const LaneSplit = @import("sources.zig").LaneSplit;
const Profile = @import("../../exec/profile.zig").Profile;
const RunOptions = @import("../env.zig").RunOptions;
const SplitCtx = @import("../connect.zig").SplitCtx;
const Stats = @import("../env.zig").Stats;
const LaneMem = @import("../parallel.zig").LaneMem;
const WorkQueue = @import("../parallel.zig").WorkQueue;
const ast = @import("../../lang/ast.zig");
const buildLaneJoinChain = @import("join.zig").buildLaneJoinChain;
const buildLaneJoinDrain = @import("join.zig").buildLaneJoinDrain;
const buildMapChain = @import("../plan.zig").buildMapChain;
const buildParallelSink = @import("../connect.zig").buildParallelSink;
const connect_mod = @import("../connect.zig");
const dispatchWorker = @import("../parallel.zig").dispatchWorker;
const driver = @import("../../connect/driver.zig");
const dupeSchema = @import("../connect.zig").dupeSchema;
const laneRowSource = @import("sources.zig").laneRowSource;
const mapChainSchema = @import("../plan.zig").mapChainSchema;
const obs = @import("../obs.zig");
const op = @import("../../exec/op.zig");
const openSink = @import("../connect.zig").openSink;
const openSqlQuery = @import("../connect.zig").openSqlQuery;
const parallel = @import("../parallel.zig");
const pqdecode = @import("../../format/parquet/read.zig");
const planSplit = @import("../connect.zig").planSplit;
const resolveLaneJoin = @import("join.zig").resolveLaneJoin;
const keyExprs = @import("../plan.zig").keyExprs;
const keypush = @import("../plan/keypush.zig");
const resolveUpsertKeys = @import("../connect.zig").resolveUpsertKeys;
const SinkGuard = @import("../../connect/guard.zig").SinkGuard;
const schemaPtr = @import("../env.zig").schemaPtr;
const sinkLabel = @import("../connect.zig").sinkLabel;
const sqlDescForStage = @import("../connect.zig").sqlDescForStage;
const std = @import("std");
const trace = @import("../../exec/trace.zig");
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
    prof: ?*Profile = null,
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

    /// A unit being filled on a lane: unit-encoded, rendered, or copied batches.
    pub const Fill = struct {
        unit: *Unit,
        enc: ?driver.UnitEncoder,
        rows: u64 = 0,

        pub fn add(f: *Fill, snk: driver.Sink, scratch: std.mem.Allocator, b: Batch) !void {
            if (b.len == 0) return;
            const ua = f.unit.arena.allocator();
            if (f.enc) |e| {
                try e.write(scratch, b);
            } else try f.unit.parts.append(if (snk.canRender())
                .{ .bytes = try snk.renderBatch(ua, b).? }
            else
                .{ .batch = try b.deepCopy(ua) });
            f.rows += b.len;
        }

        pub fn drop(f: Fill) void {
            if (f.enc) |e| e.discard();
            freeUnit(f.unit);
        }
    };

    /// Wait for unit `i`'s turn and take a unit to fill; null once the run has failed.
    pub fn begin(self: *OrderedOut, i: usize, failed: *std.atomic.Value(bool)) !?Fill {
        self.waitTurn(i, failed);
        if (failed.load(.seq_cst)) return null;
        const unit = try self.takeUnit();
        errdefer freeUnit(unit);
        const enc: ?driver.UnitEncoder = if (self.snk.openUnit(unit.arena.allocator())) |r| try r else null;
        return .{ .unit = unit, .enc = enc };
    }

    /// Seal unit `i` and hand it over; from here it is the writer's, even on error.
    pub fn hand(self: *OrderedOut, i: usize, f: Fill) !void {
        if (f.enc) |e| {
            errdefer f.drop();
            try e.seal();
            try f.unit.parts.append(.{ .encoded = e });
        }
        try self.deposit(i, f.unit);
    }
};

/// The worker loop of an ordered shape: one `LaneMem` for the lane's whole run, reset
/// between units (per-unit arenas left every unit decoding into cold pages), and a
/// failure wakes the lanes waiting on the window.
pub fn orderedWorker(comptime Ctx: type, comptime unitFn: anytype) fn (*Ctx, usize) void {
    return struct {
        fn go(ctx: *Ctx, _: usize) void {
            var mem: LaneMem = .{};
            mem.init();
            defer mem.deinit();
            while (ctx.queue.take()) |i| {
                const span = trace.begin(comptime trace.short(Ctx), i);
                defer span.end();
                unitFn(ctx, i, &mem) catch |e| {
                    ctx.queue.fail(e);
                    ctx.ordered.?.wake();
                    return;
                };
                _ = mem.work.reset(.retain_capacity);
                _ = mem.batch.reset(.retain_capacity);
            }
        }
    }.go;
}

const mapWorker = dispatchWorker(MapCtx, mapWorkOne);
const orderedMapWorker = orderedWorker(MapCtx, mapOrderedUnit);

fn mapWorkOne(ctx: *MapCtx, i: usize) !void {
    var mem: LaneMem = .{};
    mem.init();
    defer mem.deinit();
    const inner = (try laneRowSource(ctx.split.unorderedRows(i, ctx.queue.nitems), mem.work.allocator())) orelse return;
    defer inner.close();
    errdefer ctx.split.abort();
    var cs = obs.CountingSource{ .inner = inner, .count = ctx.rows_read };
    var scan = op.Scan{ .src = cs.source() };
    const chain = try mapLaneChain(ctx, &scan, mem.work.allocator());
    const out = try pourLane(ctx.sink_mode, &ctx.sink_mtx, mem.gpa.allocator(), i, chain, &mem.batch, &ctx.queue.failed);
    _ = ctx.rows_out.fetchAdd(out, .monotonic);
    if (ctx.prof) |p| _ = try p.absorb(chain);
}

fn mapLaneChain(ctx: *MapCtx, scan: *op.Scan, wa: std.mem.Allocator) !op.Op {
    const mapped = try buildMapChain(wa, ctx.params, ctx.errctx, ctx.map_stages, scan, ctx.split.schema());
    return if (ctx.join) |lj| buildLaneJoinChain(wa, ctx.params, ctx.errctx, lj, mapped) else mapped;
}

/// Pour `chain` into the sink: the shared one, or a lane sink numbered `lane_idx`,
/// opened at the first row and closed at the end. A set `failed` stops the pour and
/// aborts that lane sink.
fn pourLane(mode: parallel.SinkMode, mtx: *std.Thread.Mutex, gpa: std.mem.Allocator, lane_idx: usize, chain: op.Op, batch_arena: *std.heap.ArenaAllocator, failed: ?*std.atomic.Value(bool)) !u64 {
    var own = SinkGuard.empty;
    defer own.abort();
    var out: u64 = 0;
    while (try chain.next(batch_arena.allocator())) |b| {
        if (failed) |f| if (f.load(.seq_cst)) return out;
        if (b.len > 0) {
            if (!own.armed()) switch (mode) {
                .shared => {},
                .per_lane => |pl| own.arm(try pl.open(pl.ctx, gpa, lane_idx)),
            };
            try parallel.writeLaneBatch(mode, mtx, own.get(), batch_arena.allocator(), b);
            out += b.len;
        }
        _ = batch_arena.reset(.retain_capacity);
    }
    try own.close();
    return out;
}

/// One positional unit of an ordered map, formatted (or unit-encoded) on the lane. A unit
/// with no rows is still handed over: the writer waits on every position.
fn mapOrderedUnit(ctx: *MapCtx, i: usize, mem: *LaneMem) !void {
    errdefer ctx.split.abort();
    const ord = ctx.ordered.?;
    var f = (try ord.begin(i, &ctx.queue.failed)) orelse return;
    {
        errdefer f.drop();
        if (try laneRowSource(ctx.split.rows(i, ctx.queue.nitems), mem.work.allocator())) |inner| {
            defer inner.close();
            var cs = obs.CountingSource{ .inner = inner, .count = ctx.rows_read };
            var scan = op.Scan{ .src = cs.source() };
            const chain = try mapLaneChain(ctx, &scan, mem.work.allocator());
            while (try chain.next(mem.batch.allocator())) |b| {
                if (ctx.queue.failed.load(.seq_cst)) return f.drop();
                try f.add(ord.snk, mem.batch.allocator(), b);
                _ = mem.batch.reset(.retain_capacity);
            }
            if (ctx.prof) |p| _ = try p.absorb(chain);
        }
    }
    _ = ctx.rows_out.fetchAdd(f.rows, .monotonic);
    try ord.hand(i, f);
}

pub fn runParallelMap(
    env: *Env,
    split_in: LaneSplit,
    map_stages: []const ast.Stage,
    jshape: ?MapJoinShape,
    w: ast.Write,
    opts: RunOptions,
    stats: *Stats,
    lanes_used: *usize,
) anyerror!bool {
    const arena = env.arena;
    var split = split_in;
    var out_schema = try mapChainSchema(env, map_stages, split.schema().*);

    env.src_name = split.label();
    env.sink_name = sinkLabel(env, w);

    var lane_join: ?LaneJoin = null;
    if (jshape) |js| {
        const lp = (try resolveLaneJoin(env, js.join, js.join_hints, js.suffix, out_schema)) orelse return false;
        lane_join = lp.lane;
        out_schema = lp.out_schema;
        if (split == .parquet) split.parquet.keys = try laneParquetKeys(env, js, lp.lane, map_stages);
    }

    const wr = try resolveUpsertKeys(env, w);
    const sink_mode = (try buildParallelSink(env, wr, out_schema)) orelse
        parallel.SinkMode{ .shared = try openSink(env, wr, out_schema) };
    var shared: SinkGuard = if (sink_mode == .shared) .init(sink_mode.shared) else .empty;
    defer shared.abort();
    errdefer parallel.abortLanes(sink_mode);

    const nthreads = split.lanes(opts);
    var ctx = MapCtx{
        .split = split,
        .map_stages = map_stages,
        .params = env.params_expr,
        .errctx = env.errctx,
        .queue = .{ .nitems = nthreads },
        .sink_mode = sink_mode,
        .rows_read = env.rows_read,
        .join = lane_join,
        .prof = env.profile,
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
    if (lane_join) |lj| if (ctx.queue.failure() == null) {
        const drained = try writeLaneJoinDrain(env, lj, sink_mode, &ctx.sink_mtx, ctx.queue.nitems);
        _ = ctx.rows_out.fetchAdd(drained, .monotonic);
    };
    const units = ctx.queue.nitems;
    env.log.log(.debug, "parallel {s} map{s}: {d} {s} over {d} lanes ({s} sink)", .{
        split.label(), if (lane_join != null) "+join" else "", units, split.unitName(), lanes, @tagName(sink_mode),
    });
    if (env.profile) |p| {
        if (lane_join) |lj| if (lj.build_root) |b| try p.graftBuild(lj.at, b);
        p.path = try std.fmt.allocPrint(env.arena, "{s} parallel map{s}, {d} {s}", .{ split.label(), if (lane_join != null) "+join" else "", units, split.unitName() });
    }

    if (ctx.queue.failure()) |e| return e;
    stats.rows_out += ctx.rows_out.load(.monotonic);
    try shared.close();
    try parallel.finishLanes(sink_mode);
    return true;
}

/// Once every lane is done, a right/full join's unmatched build rows go through
/// the join's suffix into the sink, after all the lanes' output: the shared sink,
/// or one more lane sink numbered `lane_idx`, opened only if there is a row to write.
/// Its chain folds into the lanes' join and suffix nodes, as more of their work.
fn writeLaneJoinDrain(env: *Env, lj: LaneJoin, sink_mode: parallel.SinkMode, mtx: *std.Thread.Mutex, lane_idx: usize) !u64 {
    var state = std.heap.ArenaAllocator.init(env.gpa);
    defer state.deinit();
    var batch_arena = std.heap.ArenaAllocator.init(env.gpa);
    defer batch_arena.deinit();
    const chain = (try buildLaneJoinDrain(state.allocator(), env.params_expr, env.errctx, lj)) orelse return 0;
    const rows = try pourLane(sink_mode, mtx, env.gpa, lane_idx, chain, &batch_arena, null);
    if (env.profile) |p| _ = try p.absorb(chain);
    return rows;
}

/// The indexed right side's keys as row-group tests for the Parquet morsels of the
/// left side, on the terms of the serial plan (`keypush.ParquetKeys`).
fn laneParquetKeys(env: *Env, js: MapJoinShape, lane: LaneJoin, map_stages: []const ast.Stage) ![]const pqdecode.KeyBound {
    const arena = env.arena;
    const exprs = (try narrowingKeyExprs(arena, js, lane, map_stages)) orelse return &.{};
    const cols = try keypush.plainColumns(arena, exprs);
    for (cols) |c| {
        if (c != null) break;
    } else return &.{};
    const keys = try op.collectKeys(arena, &.{lane.index.build_batch}, lane.right_keys, op.default_push_cap);
    return keypush.keyBounds(arena, cols, keys);
}

/// The probe side's key expressions, traced back through `map_stages`, when the
/// join's build keys may narrow its left side; null otherwise.
fn narrowingKeyExprs(arena: std.mem.Allocator, js: MapJoinShape, lane: LaneJoin, map_stages: []const ast.Stage) !?[]const ?*ast.Expr {
    if (!keypush.leftMayNarrow(js.join) or keypush.disabled(js.join_hints)) return null;
    const stages = try std.mem.concat(arena, ast.Stage, &.{ map_stages, lane.probe_prep });
    return keyExprs(arena, stages, lane.left_key_names);
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
    if (middle[j].node.join.keyless()) return null;
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

/// The kinds a single map+join may probe on lanes: the lookup-only ones, plus right
/// and full, whose lanes mark one shared match bitset and whose unmatched build rows
/// `writeLaneJoinDrain` writes once, after the lanes.
pub fn mapJoinKindLaneSafe(kind: ast.JoinKind) bool {
    return joinKindLaneSafe(kind) or kind == .right or kind == .full;
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
    prof: ?*Profile = null,
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
            const i = self.ctx.queue.take() orelse return null;
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
    var mem: LaneMem = .{};
    mem.init();
    defer mem.deinit();
    var ss = SplitSource{ .ctx = ctx, .gpa = mem.gpa.allocator() };
    defer SplitSource.closeFn(&ss);
    var cs = obs.CountingSource{ .inner = .{ .ptr = &ss, .vtable = &SplitSource.vtable }, .count = ctx.rows_read };
    var scan = op.Scan{ .src = cs.source() };
    const probe = try buildMapChain(mem.work.allocator(), ctx.params, ctx.errctx, ctx.prefix, &scan, ctx.src_schema);
    const chain = try buildLaneJoinChain(mem.work.allocator(), ctx.params, ctx.errctx, ctx.join, probe);
    const out = try pourLane(ctx.sink_mode, &ctx.sink_mtx, mem.gpa.allocator(), lane_idx, chain, &mem.batch, &ctx.queue.failed);
    _ = ctx.rows_out.fetchAdd(out, .monotonic);
    if (ctx.prof) |p| _ = try p.absorb(chain);
}

/// Split-parallel SQL map+join. The serial plan's sources are closed before the shared
/// index opens its own. No projection narrowing under a join (planMap's liveness is
/// map-shaped), so lanes select `*`; join-aware liveness would fix that.
pub fn runParallelSqlMapJoin(env: *Env, stages: []const ast.Stage, shape: MapJoinShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize, src_base: usize) anyerror!bool {
    const arena = env.arena;
    if (stages[0].node != .read) return false;
    const desc = (try sqlDescForStage(env, stages[0])) orelse return false;
    if (!mapJoinKindLaneSafe(shape.join.kind)) return false;
    if (w.mode == .upsert and w.mode.upsert.keys.len == 0) return false;
    if (src_base >= env.sources.items.len) return false;

    const src_schema = try schemaPtr(arena, try dupeSchema(arena, env.sources.items[src_base].schema()));
    const probe_schema = try mapChainSchema(env, shape.prefix, src_schema.*);

    const sp = (try planSplit(env, desc, stages[0], opts.lanes(), w)) orelse return false;

    for (env.sources.items[src_base..]) |sc| sc.close();
    env.sources.shrinkRetainingCapacity(src_base);

    const lp = (try resolveLaneJoin(env, shape.join, shape.join_hints, shape.suffix, probe_schema)) orelse return false;

    var where_extra: ?[]const u8 = null;
    if (try narrowingKeyExprs(arena, shape, lp.lane, shape.prefix)) |exprs| {
        const keys = try op.collectKeys(arena, &.{lp.lane.index.build_batch}, lp.lane.right_keys, op.default_push_cap);
        where_extra = try keypush.render(arena, desc.dialect, src_schema.*, exprs, keys);
        if (where_extra) |x| env.log.log(.debug, "key pushdown into {s} splits: WHERE {s}", .{ @tagName(desc.dialect), if (x.len > 400) x[0..400] else x });
    }

    env.sink_name = sinkLabel(env, w);
    const sink_mode: parallel.SinkMode = (try buildParallelSink(env, w, lp.out_schema)) orelse
        .{ .shared = try openSink(env, w, lp.out_schema) };
    var shared: SinkGuard = if (sink_mode == .shared) .init(sink_mode.shared) else .empty;
    defer shared.abort();
    errdefer parallel.abortLanes(sink_mode);

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
        .prof = env.profile,
    };

    const nlanes = @min(@max(@as(usize, 1), opts.lanes()), sp.predicates.len);
    const lanes = try parallel.spawnJoin(arena, nlanes, sqlMapJoinLane, &ctx);
    lanes_used.* = @max(lanes_used.*, lanes);
    env.log.log(.debug, "split-parallel map+join: {d} splits over {d} lanes", .{ sp.predicates.len, lanes });
    if (env.profile) |p| {
        if (lp.lane.build_root) |b| try p.graftBuild(lp.lane.at, b);
        p.path = try std.fmt.allocPrint(env.arena, "{s} split-parallel map+join, {d} key ranges", .{ @tagName(desc.kind), sp.predicates.len });
        try p.noteSent("{s}, once per key range: {s}", .{ @tagName(desc.kind), sp.base_sql });
    }

    if (ctx.queue.failure()) |e| return e;
    stats.rows_out += ctx.rows_out.load(.monotonic);
    stats.rows_out += try writeLaneJoinDrain(env, lp.lane, sink_mode, &ctx.sink_mtx, nlanes);
    try shared.close();
    try parallel.finishLanes(sink_mode);
    return true;
}

test "join kinds allowed on the parallel probe path" {
    try std.testing.expect(joinKindLaneSafe(.inner));
    try std.testing.expect(joinKindLaneSafe(.left));
    try std.testing.expect(joinKindLaneSafe(.semi));
    try std.testing.expect(joinKindLaneSafe(.anti));
    try std.testing.expect(joinKindLaneSafe(.cross));
    try std.testing.expect(!joinKindLaneSafe(.right));
    try std.testing.expect(!joinKindLaneSafe(.full));
    try std.testing.expect(mapJoinKindLaneSafe(.right));
    try std.testing.expect(mapJoinKindLaneSafe(.full));
    try std.testing.expect(mapJoinKindLaneSafe(.inner));
}
