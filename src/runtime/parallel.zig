//! Split-parallel source driver. Given a list of split predicates and a factory
//! that opens a `driver.Source` for one split, this runs N lanes (threads) that
//! work-steal splits from a shared counter. Each lane owns its split's connection,
//! reads it to exhaustion, applies the map-only stage chain (the vectorized
//! kernels) on its own arena, and writes to the sink. Because the splits are
//! disjoint key ranges, output order across lanes is not preserved (union order)
//! — which is exactly what a partitioned read means.
//!
//! Sinks come in two flavors. A `shared` sink (e.g. one CSV file) is written under
//! a mutex. A `per_lane` sink (StarRocks stream-load, a DB connection) is opened
//! once per lane and written lock-free, so the *write* side fans out across cores
//! too — N concurrent stream-load streams / INSERT connections.

const std = @import("std");
const driver = @import("../connect/driver.zig");
const op = @import("../exec/op.zig");
const batchmod = @import("../exec/batch.zig");

/// Opens a fresh source for one split predicate. `ctx` carries the connection
/// config; the returned source is owned by the caller (closed by the lane).
pub const OpenSplitFn = *const fn (ctx: *anyopaque, gpa: std.mem.Allocator, pred: []const u8) anyerror!driver.Source;

/// Opens a sink for one lane (`lane_idx` disambiguates e.g. StarRocks labels).
pub const OpenSinkFn = *const fn (ctx: *anyopaque, gpa: std.mem.Allocator, lane_idx: usize) anyerror!driver.Sink;

pub const SinkMode = union(enum) {
    /// One sink object; lane writes serialize on a mutex (single-stream sinks).
    shared: driver.Sink,
    /// Each lane opens its own sink and writes it lock-free (parallel sinks).
    per_lane: struct { open: OpenSinkFn, ctx: *anyopaque },
};

/// Write one batch through a lane's sink: a `shared` sink serializes on `mtx`;
/// a `per_lane` sink (`own`, already opened by the lane) writes lock-free.
pub fn writeLaneBatch(mode: SinkMode, mtx: *std.Thread.Mutex, own: ?driver.Sink, a: std.mem.Allocator, b: batchmod.Batch) !void {
    switch (mode) {
        .shared => |snk| {
            mtx.lock();
            defer mtx.unlock();
            try snk.writeBatch(a, b);
        },
        .per_lane => try own.?.writeBatch(a, b),
    }
}

/// Overlaps sink writes with source reads in the serial pipeline: one writer
/// thread, one batch in flight, FIFO (row order is preserved). The caller builds
/// batch N+1 while the worker has batch N on the wire, so a passthrough job costs
/// max(read, write) per batch instead of read + write. Arena contract: `submit`
/// returns only when the PREVIOUS batch is fully written — the arena that batch
/// lived in is then safe to reset. Always pair `start` with `shutdown` (defer);
/// call `finish` before the sink's own close to surface the last write error.
pub const PipelinedSink = struct {
    snk: driver.Sink,
    gpa: std.mem.Allocator,
    mtx: std.Thread.Mutex = .{},
    cv: std.Thread.Condition = .{},
    slot: ?batchmod.Batch = null,
    busy: bool = false,
    stop: bool = false,
    err: ?anyerror = null,
    thread: ?std.Thread = null,

    pub fn start(self: *PipelinedSink) !void {
        self.thread = try std.Thread.spawn(.{}, workerFn, .{self});
    }

    /// Hand a batch to the writer; blocks until the worker is idle (previous
    /// batch written). Returns the worker's pending error, if any.
    pub fn submit(self: *PipelinedSink, b: batchmod.Batch) !void {
        self.mtx.lock();
        defer self.mtx.unlock();
        while ((self.slot != null or self.busy) and self.err == null) self.cv.wait(&self.mtx);
        if (self.err) |e| return e;
        self.slot = b;
        self.cv.broadcast();
    }

    /// Drain the in-flight batch, stop the worker, and surface its error.
    pub fn finish(self: *PipelinedSink) !void {
        self.shutdown();
        if (self.err) |e| return e;
    }

    /// Idempotent join (safe as a defer alongside an explicit `finish`).
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
            const b = self.slot orelse break; // stop && drained
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
                break; // stop consuming; submit/finish will surface it
            };
        }
    }
};

/// Spawn up to `n` copies of `worker(ctx, lane_idx)`, join them, and return the
/// effective lane count (>= 1). When no thread can be spawned the worker runs
/// inline on this thread as lane 0.
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
    rows_read: *std.atomic.Value(u64),

    sink_mtx: std.Thread.Mutex = .{},
    next_split: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    err_mtx: std.Thread.Mutex = .{},
    first_err: ?anyerror = null,
    failed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    rows: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
};

fn fail(sh: *Shared, e: anyerror) void {
    sh.err_mtx.lock();
    if (sh.first_err == null) sh.first_err = e;
    sh.err_mtx.unlock();
    sh.failed.store(true, .seq_cst);
}

fn lane(sh: *Shared, lane_idx: usize) void {
    // Each lane gets its own thread-unsafe allocator (no cross-thread mutex) that
    // pools pages (no per-alloc syscall). Both extremes serialize the per-row parse
    // path: a shared GPA on its mutex, a raw page allocator on the kernel mmap lock.
    var lane_gpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = false }){};
    defer _ = lane_gpa.deinit();
    const lane_alloc = lane_gpa.allocator();
    var arena = std.heap.ArenaAllocator.init(lane_alloc);
    defer arena.deinit();
    const a = arena.allocator();

    // A per-lane sink is opened on first use and committed when the lane ends —
    // unless the run failed, in which case its tail buffer is discarded (abort)
    // rather than flushed: a failed run must not commit more data on the way out.
    // (Best-effort: a lane that already closed before another lane failed has
    // committed its tail; prior flushes are always committed regardless.)
    var own_sink: ?driver.Sink = null;
    defer if (own_sink) |s| {
        if (sh.failed.load(.seq_cst)) {
            s.abort();
        } else {
            s.close() catch |e| fail(sh, e);
        }
    };

    while (true) {
        if (sh.failed.load(.seq_cst)) return;
        const i = sh.next_split.fetchAdd(1, .seq_cst);
        if (i >= sh.predicates.len) break;

        if (sh.sink_mode == .per_lane and own_sink == null) {
            const pl = sh.sink_mode.per_lane;
            own_sink = pl.open(pl.ctx, lane_alloc, lane_idx) catch |e| {
                fail(sh, e);
                return;
            };
        }

        const src = sh.open(sh.ctx, lane_alloc, sh.predicates[i]) catch |e| {
            fail(sh, e);
            return;
        };
        defer src.close();

        while (true) {
            if (sh.failed.load(.seq_cst)) return;
            _ = arena.reset(.retain_capacity);
            const maybe = src.next(a) catch |e| {
                fail(sh, e);
                return;
            };
            var b = maybe orelse break;
            _ = sh.rows_read.fetchAdd(b.len, .monotonic);
            var ok = true;
            for (sh.stages) |st| {
                b = st.apply(a, b) catch |e| {
                    fail(sh, e);
                    ok = false;
                    break;
                };
            }
            if (!ok) return;
            if (b.len == 0) continue;

            writeLaneBatch(sh.sink_mode, &sh.sink_mtx, own_sink, a, b) catch |e| {
                fail(sh, e);
                return;
            };
            _ = sh.rows.fetchAdd(b.len, .seq_cst);
        }
    }
}

/// Run `predicates.len` splits across `min(nthreads, predicates.len)` lanes.
/// Returns total rows written, or the first error any lane hit. A `shared` sink is
/// closed by the caller; `per_lane` sinks are closed by their lanes.
pub fn run(
    gpa: std.mem.Allocator,
    predicates: []const []const u8,
    open: OpenSplitFn,
    ctx: *anyopaque,
    stages: []const op.Stage,
    sink_mode: SinkMode,
    nthreads: usize,
    rows_read: *std.atomic.Value(u64),
) !usize {
    var sh = Shared{
        .gpa = gpa,
        .open = open,
        .ctx = ctx,
        .predicates = predicates,
        .stages = stages,
        .sink_mode = sink_mode,
        .rows_read = rows_read,
    };

    const nlanes = @min(@max(@as(usize, 1), nthreads), predicates.len);
    _ = try spawnJoin(gpa, nlanes, lane, &sh);

    if (sh.first_err) |e| return e;
    return sh.rows.load(.seq_cst);
}

// ---------------------------------------------------------------------------
// Tests (deterministic only: totals, effective lane counts, and error identity
// — never per-lane ordering or timing)
// ---------------------------------------------------------------------------

const testing = std.testing;
const types = @import("../lang/types.zig");
const column = @import("../exec/column.zig");

const test_empty_schema = types.Schema{ .fields = &.{} };
var test_no_cols: [0]column.Column = .{};

/// A split source yielding `remaining` rows as zero-column batches of ≤2 rows
/// (so a split spans several `next` pulls). "fail-read" splits error on read.
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
    fn vtNext(ptr: *anyopaque, _: std.mem.Allocator) anyerror!?batchmod.Batch {
        const self: *FakeSplitSource = @ptrCast(@alignCast(ptr));
        if (self.fail_read) return error.SplitReadFailed;
        if (self.remaining == 0) return null;
        const n = @min(self.remaining, 2);
        self.remaining -= n;
        return batchmod.Batch{ .schema = &test_empty_schema, .columns = &test_no_cols, .len = n };
    }
    fn vtClose(ptr: *anyopaque) void {
        const self: *FakeSplitSource = @ptrCast(@alignCast(ptr));
        self.gpa.destroy(self);
    }
};

/// `OpenSplitFn` for tests: the predicate is the split's row count, or a
/// failure directive ("fail-open" / "fail-read").
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

/// A caller-owned (shared-mode) sink counting rows and lifecycle calls.
const CountSink = struct {
    rows: usize = 0,
    closed: bool = false,
    aborted: bool = false,

    fn sink(self: *CountSink) driver.Sink {
        return .{ .ptr = self, .vtable = &vtable };
    }
    const vtable = driver.Sink.VTable{ .writeBatch = vtWrite, .close = vtClose, .abort = vtAbort };
    fn vtWrite(ptr: *anyopaque, _: std.mem.Allocator, b: batchmod.Batch) anyerror!void {
        const self: *CountSink = @ptrCast(@alignCast(ptr));
        self.rows += b.len; // shared-mode writes are serialized under run's mutex
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

/// A lane-owned sink: rows commit into the shared totals only on close, so the
/// test observes the lanes' commit-on-close/abort-on-failure discipline.
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
    fn vtWrite(ptr: *anyopaque, _: std.mem.Allocator, b: batchmod.Batch) anyerror!void {
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

/// Records the length of every batch written, in order; optionally fails on the
/// N-th write (1-based) to exercise error propagation.
const SeqSink = struct {
    lens: std.array_list.Managed(usize),
    fail_on: usize = 0,
    writes: usize = 0,

    fn sinkOf(self: *SeqSink) driver.Sink {
        return .{ .ptr = self, .vtable = &vtable };
    }
    const vtable = driver.Sink.VTable{ .writeBatch = vtWrite, .close = vtClose, .abort = vtAbort };
    fn vtWrite(ptr: *anyopaque, _: std.mem.Allocator, b: batchmod.Batch) anyerror!void {
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
    const b = batchmod.Batch{ .schema = &test_empty_schema, .columns = &test_no_cols, .len = 1 };
    // The failure lands on the 2nd write; with one batch in flight it must show
    // up by the time we have submitted a few more or drained.
    var got: ?anyerror = null;
    for (0..4) |_| pw.submit(b) catch |e| {
        got = e;
        break;
    };
    if (got == null) pw.finish() catch |e| {
        got = e;
    };
    try std.testing.expectEqual(@as(?anyerror, error.SinkWriteFailed), got);
    try std.testing.expectEqual(@as(usize, 1), ss.lens.items.len); // only the 1st landed
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
        // effective count: 1..n even when spawning fails (n=0 → inline as lane 0)
        try testing.expect(lanes >= 1 and lanes <= @max(n, 1));
        try testing.expectEqual(lanes, c.calls.load(.seq_cst));
    }
}

test "writeLaneBatch: shared routes to the shared sink, per_lane to the lane's own" {
    var shared = CountSink{};
    var own = CountSink{};
    var totals = LaneSink.Totals{};
    var mtx = std.Thread.Mutex{};
    const b = batchmod.Batch{ .schema = &test_empty_schema, .columns = &test_no_cols, .len = 3 };

    try writeLaneBatch(.{ .shared = shared.sink() }, &mtx, null, testing.allocator, b);
    try testing.expectEqual(@as(usize, 3), shared.rows);

    try writeLaneBatch(.{ .per_lane = .{ .open = testOpenLaneSink, .ctx = &totals } }, &mtx, own.sink(), testing.allocator, b);
    try testing.expectEqual(@as(usize, 3), own.rows);
    try testing.expectEqual(@as(usize, 3), shared.rows); // untouched by the per-lane write
}

test "run: work-steals uneven splits across fewer lanes than splits (shared sink)" {
    var snk = CountSink{};
    var rows_read = std.atomic.Value(u64).init(0);
    var dummy: u8 = 0;
    const preds = [_][]const u8{ "5", "3", "4", "1" };
    const n = try run(testing.allocator, &preds, testOpenSplit, &dummy, &.{}, .{ .shared = snk.sink() }, 2, &rows_read);
    try testing.expectEqual(@as(usize, 13), n);
    try testing.expectEqual(@as(usize, 13), snk.rows);
    try testing.expectEqual(@as(u64, 13), rows_read.load(.seq_cst));
    // a shared sink is caller-owned: run must neither commit nor abort it
    try testing.expect(!snk.closed and !snk.aborted);
}

test "run: lane count clamps to the split count; zero threads still runs one lane" {
    var dummy: u8 = 0;
    { // 1 split, 8 threads → single lane, all rows through
        var snk = CountSink{};
        var rows_read = std.atomic.Value(u64).init(0);
        const preds = [_][]const u8{"2"};
        try testing.expectEqual(@as(usize, 2), try run(testing.allocator, &preds, testOpenSplit, &dummy, &.{}, .{ .shared = snk.sink() }, 8, &rows_read));
        try testing.expectEqual(@as(usize, 2), snk.rows);
    }
    { // nthreads=0 clamps up to 1
        var snk = CountSink{};
        var rows_read = std.atomic.Value(u64).init(0);
        const preds = [_][]const u8{ "3", "2" };
        try testing.expectEqual(@as(usize, 5), try run(testing.allocator, &preds, testOpenSplit, &dummy, &.{}, .{ .shared = snk.sink() }, 0, &rows_read));
        try testing.expectEqual(@as(usize, 5), snk.rows);
    }
}

test "run: a lane's open or read failure is the run's error" {
    var dummy: u8 = 0;
    var rows_read = std.atomic.Value(u64).init(0);
    {
        var snk = CountSink{};
        const preds = [_][]const u8{"fail-open"};
        try testing.expectError(error.SplitOpenFailed, run(testing.allocator, &preds, testOpenSplit, &dummy, &.{}, .{ .shared = snk.sink() }, 2, &rows_read));
    }
    {
        var snk = CountSink{};
        const preds = [_][]const u8{"fail-read"};
        try testing.expectError(error.SplitReadFailed, run(testing.allocator, &preds, testOpenSplit, &dummy, &.{}, .{ .shared = snk.sink() }, 2, &rows_read));
    }
}

test "run: per_lane sinks are opened by lanes and committed (closed) on success" {
    var totals = LaneSink.Totals{};
    var rows_read = std.atomic.Value(u64).init(0);
    var dummy: u8 = 0;
    const preds = [_][]const u8{ "4", "2", "5" };
    const n = try run(testing.allocator, &preds, testOpenSplit, &dummy, &.{}, .{ .per_lane = .{ .open = testOpenLaneSink, .ctx = &totals } }, 2, &rows_read);
    try testing.expectEqual(@as(usize, 11), n);
    try testing.expectEqual(@as(usize, 11), totals.committed.load(.seq_cst));
    try testing.expectEqual(@as(usize, 0), totals.aborted.load(.seq_cst));
    const opened = totals.opened.load(.seq_cst);
    try testing.expect(opened >= 1 and opened <= 2); // ≤ one sink per lane that took work
}
