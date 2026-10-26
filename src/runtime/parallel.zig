//! Split-parallel source driver. Given a list of split predicates and a factory
//! that opens a `driver.Source` for one split (owned and closed by the lane), this
//! runs N lanes (threads) that work-steal splits from a shared counter. Each lane
//! owns its split's connection, reads it to exhaustion, applies the map-only stage
//! chain (the vectorized kernels) on its own arena, and writes to the sink. Because
//! the splits are disjoint key ranges, output order across lanes is not preserved
//! (union order), which is exactly what a partitioned read means.
//!
//! Sinks come in two flavors. A `shared` sink (e.g. one CSV file) is written under
//! a mutex and closed by the caller. A `per_lane` sink (StarRocks stream-load, a DB
//! connection) is opened once per lane (`lane_idx` keeps e.g. load labels apart),
//! written lock-free and closed by its lane, so the write side fans out across
//! cores too.
//!
//! `PipelinedSink` overlaps writes with reads in the serial pipeline: one writer
//! thread, one batch in flight, FIFO, so a passthrough job costs max(read, write)
//! per batch. Arena contract: `submit` returns only once the previous batch is
//! fully written, so its arena may then be reset. Always pair `start` with a
//! deferred `shutdown`, and call `finish` before the sink's own close to surface
//! the last write error.

const std = @import("std");
const msgfmt = @import("../format/msgfmt.zig");
const trace = @import("../exec/trace.zig");
const obs = @import("obs.zig");
const driver = @import("../connect/driver.zig");
const op = @import("../exec/op.zig");
const profile = @import("../exec/profile.zig");
const Profile = profile.Profile;
const Batch = @import("../exec/batch.zig").Batch;
const eval = @import("../exec/eval.zig");
const connectorNote = @import("env.zig").connectorNote;
const SinkGuard = @import("../connect/guard.zig").SinkGuard;

pub const OpenSplitFn = *const fn (ctx: *anyopaque, gpa: std.mem.Allocator, pred: []const u8) anyerror!driver.Source;

pub const OpenSinkFn = *const fn (ctx: *anyopaque, gpa: std.mem.Allocator, lane_idx: usize) anyerror!driver.Sink;

/// `finish` runs once every lane's sink has closed, `abort` once a lane failed: a
/// staged REPLACE cuts over to its target, or drops its staging table, there.
pub const SinkMode = union(enum) {
    shared: driver.Sink,
    per_lane: struct {
        open: OpenSinkFn,
        ctx: *anyopaque,
        finish: ?*const fn (*anyopaque) anyerror!void = null,
        abort: ?*const fn (*anyopaque) void = null,
    },
};

pub fn finishLanes(mode: SinkMode) !void {
    if (mode == .per_lane) if (mode.per_lane.finish) |f| try f(mode.per_lane.ctx);
}

pub fn abortLanes(mode: SinkMode) void {
    if (mode == .per_lane) if (mode.per_lane.abort) |f| f(mode.per_lane.ctx);
}

/// Formats outside the lock and holds `mtx` only for the append: locking the whole
/// write made 6M rows of parquet to CSV slower at -j 16 (835ms) than at -j 1 (752ms).
pub fn writeLaneBatch(mode: SinkMode, mtx: *std.Thread.Mutex, own: ?driver.Sink, a: std.mem.Allocator, b: Batch) !void {
    switch (mode) {
        .shared => |snk| {
            if (snk.canRender()) {
                const bytes = try snk.renderBatch(a, b).?;
                mtx.lock();
                defer mtx.unlock();
                return snk.writeRendered(bytes);
            }
            mtx.lock();
            defer mtx.unlock();
            try snk.writeBatch(a, b);
        },
        .per_lane => try own.?.writeBatch(a, b),
    }
}

pub const PipelinedSink = struct {
    snk: driver.Sink,
    gpa: std.mem.Allocator,
    mtx: std.Thread.Mutex = .{},
    cv: std.Thread.Condition = .{},
    slot: ?Batch = null,
    busy: bool = false,
    stop: bool = false,
    err: ?anyerror = null,
    note_buf: [480]u8 = undefined,
    note_len: usize = 0,
    thread: ?std.Thread = null,

    pub fn start(self: *PipelinedSink) !void {
        self.thread = try std.Thread.spawn(.{}, workerFn, .{self});
    }

    pub fn submit(self: *PipelinedSink, b: Batch) !void {
        self.mtx.lock();
        defer self.mtx.unlock();
        while ((self.slot != null or self.busy) and self.err == null) self.cv.wait(&self.mtx);
        if (self.err) |e| return self.explained(e);
        self.slot = b;
        self.cv.broadcast();
    }

    pub fn finish(self: *PipelinedSink) !void {
        self.shutdown();
        if (self.err) |e| return self.explained(e);
    }

    /// The sink's own account of its failure (`eval.explain`) is thread-local to the
    /// writer thread, so it is carried over to the caller's.
    fn explained(self: *PipelinedSink, e: anyerror) anyerror {
        return if (self.note_len > 0) eval.explain(e, self.note_buf[0..self.note_len]) else e;
    }

    pub fn shutdown(self: *PipelinedSink) void {
        const t = self.thread orelse return;
        self.mtx.lock();
        self.stop = true;
        self.cv.broadcast();
        self.mtx.unlock();
        t.join();
        self.thread = null;
    }

    fn workerFn(self: *PipelinedSink) void {
        var scratch = std.heap.ArenaAllocator.init(self.gpa);
        defer scratch.deinit();
        self.mtx.lock();
        defer self.mtx.unlock();
        while (true) {
            while (self.slot == null and !self.stop) self.cv.wait(&self.mtx);
            const b = self.slot orelse break;
            self.slot = null;
            self.busy = true;
            self.mtx.unlock();
            const r = self.snk.writeBatch(scratch.allocator(), b);
            _ = scratch.reset(.retain_capacity);
            self.mtx.lock();
            self.busy = false;
            self.cv.broadcast();
            r catch |e| {
                self.err = e;
                if (eval.takeFailure(e)) |why| {
                    self.note_len = @min(why.len, self.note_buf.len);
                    @memcpy(self.note_buf[0..self.note_len], why[0..self.note_len]);
                }
                break;
            };
        }
    }
};

/// The lane count when `-j` is not given: one per physical core the process may run
/// on, since a second hyperthread adds little to lanes that stream memory (on a 6P+8E
/// laptop, 20 lanes ran parquet aggregates and top-N 15-30% slower than 14). The
/// logical count where the topology cannot be read. Read once per process.
pub fn defaultLanes() usize {
    const known = default_lanes.load(.monotonic);
    if (known != 0) return known;
    const n = physicalCores();
    default_lanes.store(n, .monotonic);
    return n;
}

var default_lanes = std.atomic.Value(usize).init(0);

fn physicalCores() usize {
    const logical = std.Thread.getCpuCount() catch 1;
    if (@import("builtin").os.tag != .linux) return logical;
    const set = std.posix.sched_getaffinity(0) catch return logical;
    var cores: usize = 0;
    var path_buf: [96]u8 = undefined;
    var list_buf: [256]u8 = undefined;
    for (0..set.len * @bitSizeOf(usize)) |cpu| {
        if (!cpuAllowed(set, cpu)) continue;
        const path = std.fmt.bufPrint(&path_buf, "/sys/devices/system/cpu/cpu{d}/topology/thread_siblings_list", .{cpu}) catch return logical;
        const list = std.fs.cwd().readFile(path, &list_buf) catch return logical;
        const first = firstAllowedSibling(std.mem.trim(u8, list, " \n"), set) orelse return logical;
        if (first == cpu) cores += 1;
    }
    return std.math.clamp(cores, 1, logical);
}

fn cpuAllowed(set: std.posix.cpu_set_t, cpu: usize) bool {
    const bits = @bitSizeOf(usize);
    if (cpu / bits >= set.len) return false;
    return (set[cpu / bits] >> @intCast(cpu % bits)) & 1 == 1;
}

/// The lowest CPU in a sysfs CPU list (`0-1`, `0,10`, `12`) that `set` allows, which
/// stands for the whole core; null on a list that does not parse.
fn firstAllowedSibling(list: []const u8, set: std.posix.cpu_set_t) ?usize {
    var it = std.mem.tokenizeScalar(u8, list, ',');
    var best: ?usize = null;
    while (it.next()) |part| {
        const dash = std.mem.indexOfScalar(u8, part, '-');
        const lo = std.fmt.parseInt(usize, if (dash) |d| part[0..d] else part, 10) catch return null;
        const hi = if (dash) |d| std.fmt.parseInt(usize, part[d + 1 ..], 10) catch return null else lo;
        var c = lo;
        while (c <= hi) : (c += 1) if (cpuAllowed(set, c)) {
            if (best == null or c < best.?) best = c;
            break;
        };
    }
    return best;
}

/// Spawn up to `n` copies of `worker(ctx, lane_idx)`, join them, and return the
/// lane count (>= 1). When no thread can be spawned the worker runs inline as lane 0.
pub fn spawnJoin(alloc: std.mem.Allocator, n: usize, comptime worker: anytype, ctx: anytype) !usize {
    const threads = try alloc.alloc(std.Thread, n);
    defer alloc.free(threads);
    var spawned: usize = 0;
    while (spawned < n) : (spawned += 1) {
        threads[spawned] = std.Thread.spawn(.{}, worker, .{ ctx, spawned }) catch break;
    }
    if (spawned == 0) worker(ctx, 0) else for (threads[0..spawned]) |t| t.join();
    return @max(spawned, 1);
}

const Shared = struct {
    gpa: std.mem.Allocator,
    open: OpenSplitFn,
    ctx: *anyopaque,
    predicates: []const []const u8,
    stages: []const op.Stage,
    sink_mode: SinkMode,
    rows_read: *obs.RowCounter,
    queue: WorkQueue,
    profiled: ?Profiled = null,

    sink_mtx: std.Thread.Mutex = .{},
    rows: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
};

/// Where a split run's lanes report under `EXPLAIN ANALYZE`: the profile, and the
/// nodes `stages` stand for, root first and the read last, as `Profile.chainIds`
/// gives them. The lanes apply the stages without pulling them, so they time each one.
pub const Profiled = struct { prof: *Profile, ids: []const profile.Id };

/// One lane's time and rows for the read and each stage, folded into the profile
/// when the lane is done.
const LaneTally = struct {
    t: []profile.Tally,

    fn init(a: std.mem.Allocator, n: usize) !LaneTally {
        const t = try a.alloc(profile.Tally, n + 1);
        @memset(t, .{});
        return .{ .t = t };
    }

    /// Credits each node its time with its inputs', as `Op.next` counts it.
    fn credit(self: LaneTally, p: Profiled) !void {
        var below: u64 = 0;
        for (self.t, 0..) |own, k| {
            below += own.ns;
            const id = p.ids[p.ids.len - 1 - k];
            try p.prof.credit(id, .{ .ns = below, .rows = own.rows, .calls = own.calls });
        }
    }
};

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
            if (eval.takeFailure(e)) |why| {
                q.note_len = @min(why.len, q.note_buf.len);
                @memcpy(q.note_buf[0..q.note_len], why[0..q.note_len]);
            } else if (connectorNote(e, null)) |n| {
                q.note_len = (std.fmt.bufPrint(&q.note_buf, "{s}: {s}: {s}", .{ n.layer, @errorName(e), n.why }) catch q.note_buf[0..0]).len;
            }
        }
        q.err_mtx.unlock();
        q.failed.store(true, .seq_cst);
    }

    pub fn failure(q: *WorkQueue) ?anyerror {
        const e = q.first_err orelse return null;
        return if (q.note_len > 0) eval.explain(e, q.note_buf[0..q.note_len]) else e;
    }

    /// The next item for a lane, or null once every item is taken or a lane failed.
    pub fn take(q: *WorkQueue) ?usize {
        if (q.failed.load(.seq_cst)) return null;
        return claim(&q.next, q.nitems, .seq_cst);
    }
};

/// The next of `n` items off the shared counter `next`, or null once all are
/// taken: the steal under `WorkQueue.take`, for a loop that latches its own stop.
pub inline fn claim(next: *std.atomic.Value(usize), n: usize, comptime order: std.builtin.AtomicOrder) ?usize {
    const i = next.fetchAdd(1, order);
    return if (i >= n) null else i;
}

/// A lane's private allocators: a thread-unsafe gpa, a work arena for the chain it
/// builds, and a batch arena reset between batches. Init in place; it must not move.
pub const LaneMem = struct {
    gpa: std.heap.GeneralPurposeAllocator(.{ .thread_safe = false }) = .{},
    work: std.heap.ArenaAllocator = undefined,
    batch: std.heap.ArenaAllocator = undefined,

    pub fn init(self: *LaneMem) void {
        self.work = std.heap.ArenaAllocator.init(self.gpa.allocator());
        self.batch = std.heap.ArenaAllocator.init(self.gpa.allocator());
    }

    pub fn deinit(self: *LaneMem) void {
        self.batch.deinit();
        self.work.deinit();
        _ = self.gpa.deinit();
    }
};

/// The worker loop shared by the parallel CSV/SQL paths: steal the next item off
/// `ctx.queue`, run `workOne(ctx, i)`, and latch the first error, which stops the others.
pub fn dispatchWorker(comptime Ctx: type, comptime workOne: anytype) fn (*Ctx, usize) void {
    return struct {
        fn go(ctx: *Ctx, _: usize) void {
            while (ctx.queue.take()) |i| {
                const span = trace.begin(comptime trace.short(Ctx), i);
                defer span.end();
                workOne(ctx, i) catch |e| {
                    ctx.queue.fail(e);
                    return;
                };
            }
        }
    }.go;
}

fn lane(sh: *Shared, lane_idx: usize) void {
    var lane_gpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = false }){};
    defer _ = lane_gpa.deinit();
    const lane_alloc = lane_gpa.allocator();
    var arena = std.heap.ArenaAllocator.init(lane_alloc);
    defer arena.deinit();
    const a = arena.allocator();

    var own_sink = SinkGuard.empty;
    defer own_sink.settle(sh.queue.failed.load(.seq_cst)) catch |e| sh.queue.fail(e);

    const tally: ?LaneTally = if (sh.profiled) |p| (if (p.ids.len == sh.stages.len + 1) LaneTally.init(lane_alloc, sh.stages.len) catch null else null) else null;
    defer if (tally) |t| {
        t.credit(sh.profiled.?) catch {};
        lane_alloc.free(t.t);
    };

    while (sh.queue.take()) |i| {
        if (sh.sink_mode == .per_lane and !own_sink.armed()) {
            const pl = sh.sink_mode.per_lane;
            own_sink.arm(pl.open(pl.ctx, lane_alloc, lane_idx) catch |e| {
                sh.queue.fail(e);
                return;
            });
        }

        const src = sh.open(sh.ctx, lane_alloc, sh.predicates[i]) catch |e| {
            sh.queue.fail(e);
            return;
        };
        defer src.close();

        while (true) {
            if (sh.queue.failed.load(.seq_cst)) return;
            _ = arena.reset(.retain_capacity);
            var clock = if (tally != null) std.time.Instant.now() catch null else null;
            const maybe = src.next(a) catch |e| {
                sh.queue.fail(e);
                return;
            };
            var b = maybe orelse break;
            _ = sh.rows_read.fetchAdd(b.len, .monotonic);
            if (tally) |t| lap(&clock, &t.t[0], b.len);
            var ok = true;
            for (sh.stages, 1..) |st, k| {
                b = st.apply(a, b) catch |e| {
                    sh.queue.fail(e);
                    ok = false;
                    break;
                };
                if (tally) |t| lap(&clock, &t.t[k], b.len);
            }
            if (!ok) return;
            if (b.len == 0) continue;

            writeLaneBatch(sh.sink_mode, &sh.sink_mtx, own_sink.get(), a, b) catch |e| {
                sh.queue.fail(e);
                return;
            };
            _ = sh.rows.fetchAdd(b.len, .seq_cst);
        }
    }
}

/// Adds the time since `clock` and a batch of `rows` to `t`, and restarts the clock.
fn lap(clock: *?std.time.Instant, t: *profile.Tally, rows: usize) void {
    const now = std.time.Instant.now() catch return;
    if (clock.*) |c| t.ns += now.since(c);
    t.rows += rows;
    t.calls += 1;
    clock.* = now;
}

/// Run the splits across `min(nthreads, predicates.len)` lanes. Returns total rows
/// written, or the first error any lane hit.
pub fn run(
    gpa: std.mem.Allocator,
    predicates: []const []const u8,
    open: OpenSplitFn,
    ctx: *anyopaque,
    stages: []const op.Stage,
    sink_mode: SinkMode,
    nthreads: usize,
    rows_read: *obs.RowCounter,
    profiled: ?Profiled,
) !usize {
    var sh = Shared{
        .gpa = gpa,
        .open = open,
        .ctx = ctx,
        .predicates = predicates,
        .stages = stages,
        .sink_mode = sink_mode,
        .rows_read = rows_read,
        .queue = .{ .nitems = predicates.len },
        .profiled = profiled,
    };

    const nlanes = @min(@max(@as(usize, 1), nthreads), predicates.len);
    _ = try spawnJoin(gpa, nlanes, lane, &sh);

    if (sh.queue.failure()) |e| return e;
    return sh.rows.load(.seq_cst);
}

const testing = std.testing;
const types = @import("../lang/types.zig");
const column = @import("../exec/column.zig");

const test_empty_schema = types.Schema{ .fields = &.{} };
var test_no_cols: [0]column.Column = .{};

const FakeSplitSource = struct {
    gpa: std.mem.Allocator,
    remaining: usize,
    fail_read: bool = false,

    fn source(self: *FakeSplitSource) driver.Source {
        return .{ .ptr = self, .vtable = &vtable };
    }
    const vtable = driver.Source.VTable{ .schema = vtSchema, .next = vtNext, .close = vtClose };
    fn vtSchema(_: *anyopaque) types.Schema {
        return test_empty_schema;
    }
    fn vtNext(ptr: *anyopaque, _: std.mem.Allocator) anyerror!?Batch {
        const self: *FakeSplitSource = @ptrCast(@alignCast(ptr));
        if (self.fail_read) return eval.explain(error.SplitReadFailed, "row 7: bad cast");
        if (self.remaining == 0) return null;
        const n = @min(self.remaining, 2);
        self.remaining -= n;
        return Batch{ .schema = &test_empty_schema, .columns = &test_no_cols, .len = n };
    }
    fn vtClose(ptr: *anyopaque) void {
        const self: *FakeSplitSource = @ptrCast(@alignCast(ptr));
        self.gpa.destroy(self);
    }
};

/// The predicate is the split's row count, or "fail-open" / "fail-read".
fn testOpenSplit(ctx: *anyopaque, gpa: std.mem.Allocator, pred: []const u8) anyerror!driver.Source {
    _ = ctx;
    if (std.mem.eql(u8, pred, "fail-open")) return error.SplitOpenFailed;
    const src = try gpa.create(FakeSplitSource);
    src.* = .{
        .gpa = gpa,
        .remaining = std.fmt.parseInt(usize, pred, 10) catch 0,
        .fail_read = std.mem.eql(u8, pred, "fail-read"),
    };
    return src.source();
}

const CountSink = struct {
    rows: usize = 0,
    closed: bool = false,
    aborted: bool = false,

    fn sink(self: *CountSink) driver.Sink {
        return .{ .ptr = self, .vtable = &vtable };
    }
    const vtable = driver.Sink.VTable{ .writeBatch = vtWrite, .close = vtClose, .abort = vtAbort };
    fn vtWrite(ptr: *anyopaque, _: std.mem.Allocator, b: Batch) anyerror!void {
        const self: *CountSink = @ptrCast(@alignCast(ptr));
        self.rows += b.len;
    }
    fn vtClose(ptr: *anyopaque) anyerror!void {
        const self: *CountSink = @ptrCast(@alignCast(ptr));
        self.closed = true;
    }
    fn vtAbort(ptr: *anyopaque) void {
        const self: *CountSink = @ptrCast(@alignCast(ptr));
        self.aborted = true;
    }
};

const LaneSink = struct {
    gpa: std.mem.Allocator,
    totals: *Totals,
    rows: usize = 0,

    const Totals = struct {
        opened: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
        committed: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
        aborted: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    };

    fn sinkOf(self: *LaneSink) driver.Sink {
        return .{ .ptr = self, .vtable = &vtable };
    }
    const vtable = driver.Sink.VTable{ .writeBatch = vtWrite, .close = vtClose, .abort = vtAbort };
    fn vtWrite(ptr: *anyopaque, _: std.mem.Allocator, b: Batch) anyerror!void {
        const self: *LaneSink = @ptrCast(@alignCast(ptr));
        self.rows += b.len;
    }
    fn vtClose(ptr: *anyopaque) anyerror!void {
        const self: *LaneSink = @ptrCast(@alignCast(ptr));
        _ = self.totals.committed.fetchAdd(self.rows, .seq_cst);
        self.gpa.destroy(self);
    }
    fn vtAbort(ptr: *anyopaque) void {
        const self: *LaneSink = @ptrCast(@alignCast(ptr));
        _ = self.totals.aborted.fetchAdd(1, .seq_cst);
        self.gpa.destroy(self);
    }
};

fn testOpenLaneSink(ctx: *anyopaque, gpa: std.mem.Allocator, lane_idx: usize) anyerror!driver.Sink {
    _ = lane_idx;
    const totals: *LaneSink.Totals = @ptrCast(@alignCast(ctx));
    _ = totals.opened.fetchAdd(1, .seq_cst);
    const s = try gpa.create(LaneSink);
    s.* = .{ .gpa = gpa, .totals = totals };
    return s.sinkOf();
}

const SeqSink = struct {
    lens: std.array_list.Managed(usize),
    fail_on: usize = 0,
    writes: usize = 0,

    fn sinkOf(self: *SeqSink) driver.Sink {
        return .{ .ptr = self, .vtable = &vtable };
    }
    const vtable = driver.Sink.VTable{ .writeBatch = vtWrite, .close = vtClose, .abort = vtAbort };
    fn vtWrite(ptr: *anyopaque, _: std.mem.Allocator, b: Batch) anyerror!void {
        const self: *SeqSink = @ptrCast(@alignCast(ptr));
        self.writes += 1;
        if (self.fail_on != 0 and self.writes == self.fail_on) return error.SinkWriteFailed;
        try self.lens.append(b.len);
    }
    fn vtClose(_: *anyopaque) anyerror!void {}
    fn vtAbort(_: *anyopaque) void {}
};

test "PipelinedSink: batches arrive in submit order, finish drains the last one" {
    var ss = SeqSink{ .lens = std.array_list.Managed(usize).init(std.testing.allocator) };
    defer ss.lens.deinit();
    var pw = PipelinedSink{ .snk = ss.sinkOf(), .gpa = std.testing.allocator };
    try pw.start();
    defer pw.shutdown();
    for (1..6) |n| try pw.submit(.{ .schema = &test_empty_schema, .columns = &test_no_cols, .len = n });
    try pw.finish();
    try std.testing.expectEqualSlices(usize, &.{ 1, 2, 3, 4, 5 }, ss.lens.items);
}

test "PipelinedSink: a write failure surfaces on a later submit or on finish" {
    var ss = SeqSink{ .lens = std.array_list.Managed(usize).init(std.testing.allocator), .fail_on = 2 };
    defer ss.lens.deinit();
    var pw = PipelinedSink{ .snk = ss.sinkOf(), .gpa = std.testing.allocator };
    try pw.start();
    defer pw.shutdown();
    const b = Batch{ .schema = &test_empty_schema, .columns = &test_no_cols, .len = 1 };
    var got: ?anyerror = null;
    for (0..4) |_| pw.submit(b) catch |e| {
        got = e;
        break;
    };
    if (got == null) pw.finish() catch |e| {
        got = e;
    };
    try std.testing.expectEqual(@as(?anyerror, error.SinkWriteFailed), got);
    try std.testing.expectEqual(@as(usize, 1), ss.lens.items.len);
}

test "WorkQueue.take: each item once in order, then null; nothing after a failure" {
    var q = WorkQueue{ .nitems = 3 };
    try std.testing.expectEqual(@as(?usize, 0), q.take());
    try std.testing.expectEqual(@as(?usize, 1), q.take());
    try std.testing.expectEqual(@as(?usize, 2), q.take());
    try std.testing.expectEqual(@as(?usize, null), q.take());
    try std.testing.expectEqual(@as(?usize, null), q.take());
    var f = WorkQueue{ .nitems = 3 };
    try std.testing.expectEqual(@as(?usize, 0), f.take());
    f.fail(error.Boom);
    try std.testing.expectEqual(@as(?usize, null), f.take());
    try std.testing.expectEqual(@as(?anyerror, error.Boom), f.failure());
    var empty = WorkQueue{ .nitems = 0 };
    try std.testing.expectEqual(@as(?usize, null), empty.take());
}

test "spawnJoin: worker runs exactly once per effective lane (n, 1, 0 threads)" {
    const Ctx = struct {
        calls: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
        fn worker(self: *@This(), _: usize) void {
            _ = self.calls.fetchAdd(1, .seq_cst);
        }
    };
    inline for (.{ 4, 1, 0 }) |n| {
        var c = Ctx{};
        const lanes = try spawnJoin(testing.allocator, n, Ctx.worker, &c);
        try testing.expect(lanes >= 1 and lanes <= @max(n, 1));
        try testing.expectEqual(lanes, c.calls.load(.seq_cst));
    }
}

const RenderSink = struct {
    mtx: *std.Thread.Mutex,
    renders: usize = 0,
    rendered_unlocked: bool = false,
    appended: [16]u8 = undefined,
    appended_len: usize = 0,
    appended_locked: bool = false,
    plain_writes: usize = 0,

    fn sinkOf(self: *RenderSink) driver.Sink {
        return .{ .ptr = self, .vtable = &vtable };
    }
    const vtable = driver.Sink.VTable{ .writeBatch = vtWrite, .close = vtClose, .abort = vtAbort, .renderBatch = vtRender, .writeRendered = vtWriteRendered };
    fn vtWrite(ptr: *anyopaque, _: std.mem.Allocator, _: Batch) anyerror!void {
        const self: *RenderSink = @ptrCast(@alignCast(ptr));
        self.plain_writes += 1;
    }
    fn vtRender(ptr: *anyopaque, a: std.mem.Allocator, b: Batch) anyerror![]const u8 {
        const self: *RenderSink = @ptrCast(@alignCast(ptr));
        self.renders += 1;
        self.rendered_unlocked = self.mtx.tryLock();
        if (self.rendered_unlocked) self.mtx.unlock();
        return msgfmt.allocPrint(a, "rows={d}", .{b.len});
    }
    fn vtWriteRendered(ptr: *anyopaque, bytes: []const u8) anyerror!void {
        const self: *RenderSink = @ptrCast(@alignCast(ptr));
        self.appended_locked = !self.mtx.tryLock();
        if (!self.appended_locked) self.mtx.unlock();
        @memcpy(self.appended[0..bytes.len], bytes);
        self.appended_len = bytes.len;
    }
    fn vtClose(_: *anyopaque) anyerror!void {}
    fn vtAbort(_: *anyopaque) void {}
};

test "writeLaneBatch: shared routes to the shared sink, rendering outside the lock when it can; per_lane to the lane's own" {
    var shared = CountSink{};
    var own = CountSink{};
    var totals = LaneSink.Totals{};
    var mtx = std.Thread.Mutex{};
    const b = Batch{ .schema = &test_empty_schema, .columns = &test_no_cols, .len = 3 };

    try writeLaneBatch(.{ .shared = shared.sink() }, &mtx, null, testing.allocator, b);
    try testing.expectEqual(@as(usize, 3), shared.rows);

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var rs = RenderSink{ .mtx = &mtx };
    try writeLaneBatch(.{ .shared = rs.sinkOf() }, &mtx, null, arena.allocator(), b);
    try testing.expectEqual(@as(usize, 1), rs.renders);
    try testing.expect(rs.rendered_unlocked);
    try testing.expect(rs.appended_locked);
    try testing.expectEqualStrings("rows=3", rs.appended[0..rs.appended_len]);
    try testing.expectEqual(@as(usize, 0), rs.plain_writes);
    try testing.expect(mtx.tryLock());
    mtx.unlock();

    try writeLaneBatch(.{ .per_lane = .{ .open = testOpenLaneSink, .ctx = &totals } }, &mtx, own.sink(), testing.allocator, b);
    try testing.expectEqual(@as(usize, 3), own.rows);
    try testing.expectEqual(@as(usize, 3), shared.rows);
    try testing.expectEqual(@as(usize, 0), totals.opened.load(.seq_cst));
}

fn testOpenSplitCounted(ctx: *anyopaque, gpa: std.mem.Allocator, pred: []const u8) anyerror!driver.Source {
    const opens: *[8]std.atomic.Value(usize) = @ptrCast(@alignCast(ctx));
    _ = opens[try std.fmt.parseInt(usize, pred, 10)].fetchAdd(1, .seq_cst);
    return testOpenSplit(ctx, gpa, pred);
}

test "run: work-steals uneven splits across fewer lanes than splits; every split is read exactly once" {
    var snk = CountSink{};
    var rows_read = obs.RowCounter.init(0);
    var opens = [_]std.atomic.Value(usize){std.atomic.Value(usize).init(0)} ** 8;
    const preds = [_][]const u8{ "5", "3", "4", "1" };
    const n = try run(testing.allocator, &preds, testOpenSplitCounted, &opens, &.{}, .{ .shared = snk.sink() }, 2, &rows_read, null);
    for (opens, 0..) |o, i| try testing.expectEqual(@as(usize, switch (i) {
        1, 3, 4, 5 => 1,
        else => 0,
    }), o.load(.seq_cst));
    try testing.expectEqual(@as(usize, 13), n);
    try testing.expectEqual(@as(usize, 13), snk.rows);
    try testing.expectEqual(@as(u64, 13), rows_read.load(.seq_cst));
    try testing.expect(!snk.closed and !snk.aborted);
}

test "run: lane count clamps to the split count; zero threads still runs one lane" {
    var dummy: u8 = 0;
    {
        var totals = LaneSink.Totals{};
        var rows_read = obs.RowCounter.init(0);
        const preds = [_][]const u8{"2"};
        try testing.expectEqual(@as(usize, 2), try run(testing.allocator, &preds, testOpenSplit, &dummy, &.{}, .{ .per_lane = .{ .open = testOpenLaneSink, .ctx = &totals } }, 8, &rows_read, null));
        try testing.expectEqual(@as(usize, 1), totals.opened.load(.seq_cst));
        try testing.expectEqual(@as(usize, 2), totals.committed.load(.seq_cst));
    }
    {
        var totals = LaneSink.Totals{};
        var rows_read = obs.RowCounter.init(0);
        const preds = [_][]const u8{ "3", "2" };
        try testing.expectEqual(@as(usize, 5), try run(testing.allocator, &preds, testOpenSplit, &dummy, &.{}, .{ .per_lane = .{ .open = testOpenLaneSink, .ctx = &totals } }, 0, &rows_read, null));
        try testing.expectEqual(@as(usize, 1), totals.opened.load(.seq_cst));
        try testing.expectEqual(@as(usize, 5), totals.committed.load(.seq_cst));
    }
}

test "run: a lane's open or read failure is the run's error" {
    var dummy: u8 = 0;
    var rows_read = obs.RowCounter.init(0);
    {
        var snk = CountSink{};
        const preds = [_][]const u8{"fail-open"};
        try testing.expectError(error.SplitOpenFailed, run(testing.allocator, &preds, testOpenSplit, &dummy, &.{}, .{ .shared = snk.sink() }, 2, &rows_read, null));
    }
    {
        var snk = CountSink{};
        const preds = [_][]const u8{"fail-read"};
        try testing.expectError(error.SplitReadFailed, run(testing.allocator, &preds, testOpenSplit, &dummy, &.{}, .{ .shared = snk.sink() }, 2, &rows_read, null));
        try testing.expectEqualStrings("row 7: bad cast", eval.takeFailure(error.SplitReadFailed).?);
    }
}

test "run: per_lane sinks are opened by lanes and committed (closed) on success" {
    var totals = LaneSink.Totals{};
    var rows_read = obs.RowCounter.init(0);
    var dummy: u8 = 0;
    const preds = [_][]const u8{ "4", "2", "5" };
    const n = try run(testing.allocator, &preds, testOpenSplit, &dummy, &.{}, .{ .per_lane = .{ .open = testOpenLaneSink, .ctx = &totals } }, 2, &rows_read, null);
    try testing.expectEqual(@as(usize, 11), n);
    try testing.expectEqual(@as(usize, 11), totals.committed.load(.seq_cst));
    try testing.expectEqual(@as(usize, 0), totals.aborted.load(.seq_cst));
    const opened = totals.opened.load(.seq_cst);
    try testing.expect(opened >= 1 and opened <= 2);
}

test "defaultLanes: one per physical core, within the logical count" {
    const n = defaultLanes();
    try testing.expect(n >= 1);
    try testing.expect(n <= (std.Thread.getCpuCount() catch 1));

    var set = std.mem.zeroes(std.posix.cpu_set_t);
    set[0] = 0b1110;
    try testing.expectEqual(@as(?usize, 1), firstAllowedSibling("0-1", set));
    try testing.expectEqual(@as(?usize, 2), firstAllowedSibling("2,10", set));
    try testing.expectEqual(@as(?usize, null), firstAllowedSibling("4-5", set));
    try testing.expectEqual(@as(?usize, null), firstAllowedSibling("x", set));
    try testing.expectEqual(@as(?usize, 3), firstAllowedSibling("3", set));
}
