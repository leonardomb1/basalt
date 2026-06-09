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

        const sink: driver.Sink = switch (sh.sink_mode) {
            .shared => |s| s,
            .per_lane => |pl| own_sink orelse blk: {
                const s = pl.open(pl.ctx, lane_alloc, lane_idx) catch |e| {
                    fail(sh, e);
                    return;
                };
                own_sink = s;
                break :blk s;
            },
        };

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

            switch (sh.sink_mode) {
                .shared => {
                    sh.sink_mtx.lock();
                    sink.writeBatch(a, b) catch |e| {
                        sh.sink_mtx.unlock();
                        fail(sh, e);
                        return;
                    };
                    sh.sink_mtx.unlock();
                },
                .per_lane => sink.writeBatch(a, b) catch |e| {
                    fail(sh, e);
                    return;
                },
            }
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
    const threads = try gpa.alloc(std.Thread, nlanes);
    defer gpa.free(threads);

    var spawned: usize = 0;
    while (spawned < nlanes) : (spawned += 1) {
        threads[spawned] = std.Thread.spawn(.{}, lane, .{ &sh, spawned }) catch break;
    }
    if (spawned == 0) {
        lane(&sh, 0); // could not spawn: run inline on this thread
    } else {
        for (threads[0..spawned]) |t| t.join();
    }

    if (sh.first_err) |e| return e;
    return sh.rows.load(.seq_cst);
}
