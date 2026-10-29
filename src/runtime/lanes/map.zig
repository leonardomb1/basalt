//! Map lanes (filter, project, explode, a join probe): workers claim morsels and map
//! them. Into a shared sink a file's output is written in file order, as a serial
//! run writes it, one unit per morsel; a SQL source's key ranges, or a sink each lane
//! opens for itself, take rows as they come. A right or full join's lanes mark one
//! shared match bitset; its unmatched build rows are written once by the driver after
//! every lane is done, so they come last, as serially.

const Batch = @import("../../exec/batch.zig").Batch;
const Env = @import("../env.zig").Env;
const LaneJoin = @import("join.zig").LaneJoin;
const Morsels = @import("morsels.zig").Morsels;
const Profile = @import("../../exec/profile.zig").Profile;
const Reader = @import("morsels.zig").Reader;
const RunOptions = @import("../env.zig").RunOptions;
const SplitCtx = @import("../connect.zig").SplitCtx;
const Stats = @import("../env.zig").Stats;
const Stream = @import("morsels.zig").Stream;
const LaneMem = @import("../parallel.zig").LaneMem;
const WorkQueue = @import("../parallel.zig").WorkQueue;
const ast = @import("../../lang/ast.zig");
const buildLaneJoinChain = @import("join.zig").buildLaneJoinChain;
const buildLaneJoinDrain = @import("join.zig").buildLaneJoinDrain;
const analyze = @import("../analyze.zig");
const chainAt = @import("../plan.zig").chainAt;
const chainOps = @import("../plan.zig").chainOps;
const buildParallelSink = @import("../connect.zig").buildParallelSink;
const connect_mod = @import("../connect.zig");
const driver = @import("../../connect/driver.zig");
const dupeSchema = @import("../connect.zig").dupeSchema;
const obs = @import("../obs.zig");
const op = @import("../../exec/op.zig");
const openSink = @import("../connect.zig").openSink;
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
const msgfmt = @import("../../format/msgfmt.zig");
const trace = @import("../../exec/trace.zig");
const types = @import("../../lang/types.zig");

const MapCtx = struct {
    m: *const Morsels,
    map: []const analyze.Resolved,
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

/// The worker loop of an ordered shape: one `LaneMem` and morsel `Reader` for the
/// lane's whole run, reset between units (per-unit arenas left every unit decoding
/// into cold pages), and a failure wakes the lanes waiting on the window.
pub fn orderedWorker(comptime Ctx: type, comptime unitFn: anytype) fn (*Ctx, usize) void {
    return struct {
        fn go(ctx: *Ctx, _: usize) void {
            var mem: LaneMem = .{};
            mem.init();
            defer mem.deinit();
            var rd = Reader.init(mem.gpa.allocator());
            defer rd.deinit();
            while (ctx.queue.take()) |i| {
                const span = trace.begin(comptime trace.short(Ctx), i);
                defer span.end();
                unitFn(ctx, i, &mem, &rd) catch |e| {
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

const orderedMapWorker = orderedWorker(MapCtx, mapOrderedUnit);

fn mapLane(ctx: *MapCtx, lane_idx: usize) void {
    mapLaneRun(ctx, lane_idx) catch |e| ctx.queue.fail(e);
}

/// An unordered lane: the morsels it claims as one stream through one chain and,
/// for a lane sink, one connection.
fn mapLaneRun(ctx: *MapCtx, lane_idx: usize) !void {
    var mem: LaneMem = .{};
    mem.init();
    defer mem.deinit();
    var rd = Reader.init(mem.gpa.allocator());
    defer rd.deinit();
    var st = Stream{ .m = ctx.m, .queue = &ctx.queue, .rd = &rd, .scratch = mem.work.allocator() };
    defer st.source().close();
    var cs = obs.CountingSource{ .inner = st.source(), .count = ctx.rows_read };
    var scan = op.Scan{ .src = cs.source() };
    const chain = try mapLaneChain(ctx, &scan, mem.work.allocator());
    const out = try pourLane(ctx.sink_mode, &ctx.sink_mtx, mem.gpa.allocator(), lane_idx, chain, &mem.batch, &ctx.queue.failed);
    _ = ctx.rows_out.fetchAdd(out, .monotonic);
    if (ctx.prof) |p| _ = try p.absorb(chain);
}

fn mapLaneChain(ctx: *MapCtx, scan: *op.Scan, wa: std.mem.Allocator) !op.Op {
    const mapped = try chainOps(wa, ctx.errctx, ctx.map, .{ .scan = scan });
    return if (ctx.join) |lj| buildLaneJoinChain(wa, ctx.errctx, lj, mapped) else mapped;
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

/// One morsel of an ordered map, formatted (or unit-encoded) on the lane. A unit
/// with no rows is still handed over: the writer waits on every position.
fn mapOrderedUnit(ctx: *MapCtx, i: usize, mem: *LaneMem, rd: *Reader) !void {
    const ord = ctx.ordered.?;
    var f = (try ord.begin(i, &ctx.queue.failed)) orelse return;
    {
        errdefer f.drop();
        if (try ctx.m.open(i, rd, mem.work.allocator())) |inner| {
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

/// A file's map, or map and one join, on lanes.
pub fn runParallelMap(env: *Env, m_in: Morsels, map_stages: []const ast.Stage, jshape: ?MapJoinShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    var m = m_in;
    const map = try chainAt(env, m.schema().*, map_stages);
    env.src_name = m.label();
    env.sink_name = sinkLabel(env, w);
    var lane_join: ?LaneJoin = null;
    var out_schema = map.out.*;
    if (jshape) |js| {
        const lp = (try resolveLaneJoin(env, js.join, js.join_hints, js.suffix, out_schema)) orelse return false;
        lane_join = lp.lane;
        out_schema = lp.out_schema;
        if (m == .parquet) m.parquet.keys = try laneParquetKeys(env, js, lp.lane, map_stages);
    }
    try mapDrive(env, &m, map.recs, lane_join, out_schema, try resolveUpsertKeys(env, w), opts, stats, lanes_used);
    return true;
}

/// Run `m` through `recs` and `lane_join` into the sink: in file order through
/// `OrderedOut` when it is shared and the morsels are a file's, else as lanes pour.
/// A shared sink that encodes each unit on its lane (Parquet) takes a file cut
/// `coarser`: a 4 MB CSV range held under the encoder's half row group, so every
/// unit went to the writer's one thread to be encoded.
fn mapDrive(env: *Env, m_in: *const Morsels, recs: []const analyze.Resolved, lane_join: ?LaneJoin, out_schema: types.Schema, wr: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) !void {
    const arena = env.arena;
    const sink_mode = (try buildParallelSink(env, wr, out_schema)) orelse
        parallel.SinkMode{ .shared = try openSink(env, wr, out_schema) };
    const cut = if (sink_mode == .shared and sink_mode.shared.encodesUnits()) m_in.coarser() else m_in.*;
    const m = &cut;
    var shared: SinkGuard = if (sink_mode == .shared) .init(sink_mode.shared) else .empty;
    defer shared.abort();
    errdefer parallel.abortLanes(sink_mode);

    const nthreads = m.workers(opts.lanes());
    var ctx = MapCtx{
        .m = m,
        .map = recs,
        .errctx = env.errctx,
        .queue = .{ .nitems = m.count() },
        .sink_mode = sink_mode,
        .rows_read = env.rows_read,
        .join = lane_join,
        .prof = env.profile,
    };
    var ordered: OrderedOut = undefined;
    if (sink_mode == .shared and m.* != .sql) {
        ordered = try OrderedOut.init(arena, m.count(), 2 * nthreads, sink_mode.shared);
        ctx.ordered = &ordered;
    }
    defer if (ctx.ordered) |o| o.freeRest();

    const lanes = if (ctx.ordered != null)
        try parallel.spawnJoin(arena, nthreads, orderedMapWorker, &ctx)
    else
        try parallel.spawnJoin(arena, nthreads, mapLane, &ctx);
    lanes_used.* = @max(lanes_used.*, lanes);
    if (lane_join) |lj| if (ctx.queue.failure() == null) {
        const drained = try writeLaneJoinDrain(env, lj, sink_mode, &ctx.sink_mtx, nthreads);
        _ = ctx.rows_out.fetchAdd(drained, .monotonic);
    };
    const how = if (m.* == .sql) "split-parallel" else "parallel";
    const what = if (lane_join != null) "map+join" else "map";
    env.log.log(.debug, "{s} {s} {s}: {d} {s} over {d} lanes ({s} sink)", .{ m.label(), how, what, m.count(), m.unitName(), lanes, @tagName(sink_mode) });
    if (env.profile) |p| {
        if (lane_join) |lj| if (lj.build_root) |b| try p.graftBuild(lj.at, b);
        p.path = try msgfmt.allocPrint(arena, "{s} {s} {s}, {d} {s}", .{ m.label(), how, what, m.count(), m.unitName() });
        if (m.* == .sql) try p.noteSent("{s}, once per key range: {s}", .{ m.label(), m.sql.split.base_sql });
    }

    if (ctx.queue.failure()) |e| return e;
    stats.rows_out += ctx.rows_out.load(.monotonic);
    try shared.close();
    try parallel.finishLanes(sink_mode);
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
    const chain = (try buildLaneJoinDrain(state.allocator(), env.errctx, lj)) orelse return 0;
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
    const stages = try arena.alloc(ast.Stage, map_stages.len + lane.probe_prep.len);
    @memcpy(stages[0..map_stages.len], map_stages);
    for (lane.probe_prep, stages[map_stages.len..]) |r, *st| st.* = r.stage;
    return keyExprs(arena, stages, lane.left_key_names);
}

/// A row group per item for parquet; a CSV is cut into about `ordered_chunk_bytes`
pub fn classifyMapPipeline(stages: []const ast.Stage) ?[]const ast.Stage {
    const middle = stages[1 .. stages.len - 1];
    for (middle) |st| if (!analyze.isMap(st.node, true)) return null;
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

/// Split-parallel SQL map+join: the read's key ranges as morsels. The serial plan's
/// sources are closed before the shared index opens its own. No projection narrowing
/// under a join (planMap's liveness is map-shaped), so lanes select `*`; join-aware
/// liveness would fix that.
pub fn runParallelSqlMapJoin(env: *Env, stages: []const ast.Stage, shape: MapJoinShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize, src_base: usize) anyerror!bool {
    const arena = env.arena;
    if (stages[0].node != .read) return false;
    const desc = (try sqlDescForStage(env, stages[0])) orelse return false;
    if (!mapJoinKindLaneSafe(shape.join.kind)) return false;
    if (w.mode == .upsert and w.mode.upsert.keys.len == 0) return false;
    if (src_base >= env.sources.items.len) return false;

    const src_schema = try schemaPtr(arena, try dupeSchema(arena, env.sources.items[src_base].schema()));
    const pre = try chainAt(env, src_schema.*, shape.prefix);

    const sp = (try planSplit(env, desc, stages[0], opts.lanes(), w)) orelse return false;

    for (env.sources.items[src_base..]) |sc| sc.close();
    env.sources.shrinkRetainingCapacity(src_base);

    const lp = (try resolveLaneJoin(env, shape.join, shape.join_hints, shape.suffix, pre.out.*)) orelse return false;

    var where_extra: ?[]const u8 = null;
    if (try narrowingKeyExprs(arena, shape, lp.lane, shape.prefix)) |exprs| {
        const keys = try op.collectKeys(arena, &.{lp.lane.index.build_batch}, lp.lane.right_keys, op.default_push_cap);
        where_extra = try keypush.render(arena, desc.dialect, src_schema.*, exprs, keys);
        if (where_extra) |x| env.log.log(.debug, "key pushdown into {s} splits: WHERE {s}", .{ @tagName(desc.dialect), if (x.len > 400) x[0..400] else x });
    }

    env.sink_name = sinkLabel(env, w);
    const split = try arena.create(SplitCtx);
    split.* = .{ .gpa = env.gpa, .kind = desc.kind, .cfg = desc.cfg, .base_sql = sp.base_sql, .where_extra = where_extra, .report = try connect_mod.readReport(env, @tagName(desc.kind)), .want = src_schema };
    const m = Morsels{ .sql = .{ .split = split, .predicates = sp.predicates, .schema = src_schema } };
    try mapDrive(env, &m, pre.recs, lp.lane, lp.out_schema, w, opts, stats, lanes_used);
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
