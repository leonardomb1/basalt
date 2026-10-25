//! A close-or-abort guard for anything with `close() !void` and `abort() void`,
//! replacing the hand-kept `var snk_open = true; errdefer if (snk_open) snk.abort();
//! snk_open = false; try snk.close();` triple (runtime/run/output.zig,
//! runtime/lanes/{agg,topn,map}.zig). `close` disarms before it calls the target,
//! because a close releases everything even when it fails, so an `errdefer abort`
//! after a failed close must do nothing: a second release is a double free.
//!
//!     var g = SinkGuard.init(try openSink(...));
//!     errdefer g.abort();
//!     ...
//!     try g.close();
//!
//! A guard may start empty and be armed later (a lane sink opened at the first row),
//! and `abort`/`close` on an empty guard do nothing, so either is safe to repeat.

const std = @import("std");
const driver = @import("driver.zig");

pub fn Guard(comptime T: type) type {
    return struct {
        const Self = @This();

        target: ?T = null,

        pub const empty: Self = .{};

        pub fn init(t: T) Self {
            return .{ .target = t };
        }

        /// Arms `t` when `open` holds, else starts empty.
        pub fn initIf(open: bool, t: T) Self {
            return .{ .target = if (open) t else null };
        }

        pub fn arm(self: *Self, t: T) void {
            std.debug.assert(self.target == null);
            self.target = t;
        }

        pub fn get(self: Self) ?T {
            return self.target;
        }

        pub fn armed(self: Self) bool {
            return self.target != null;
        }

        /// Hands the target back unguarded, for a caller that closes it some other way.
        pub fn release(self: *Self) ?T {
            const t = self.target;
            self.target = null;
            return t;
        }

        pub fn close(self: *Self) anyerror!void {
            const t = self.release() orelse return;
            return t.close();
        }

        pub fn abort(self: *Self) void {
            const t = self.release() orelse return;
            t.abort();
        }

        /// `abort` when `failed`, else `close`: the end of a lane whose run may have failed elsewhere.
        pub fn settle(self: *Self, failed: bool) anyerror!void {
            if (failed) return self.abort();
            return self.close();
        }
    };
}

pub const SinkGuard = Guard(driver.Sink);

const testing = std.testing;
const Batch = @import("../exec/batch.zig").Batch;

const Probe = struct {
    closes: usize = 0,
    aborts: usize = 0,
    released: usize = 0,
    fail_close: bool = false,

    pub fn writeBatch(_: *Probe, _: std.mem.Allocator, _: Batch) !void {}

    pub fn close(self: *Probe) !void {
        self.closes += 1;
        self.released += 1;
        if (self.fail_close) return error.WriteFailed;
    }

    pub fn abort(self: *Probe) void {
        self.aborts += 1;
        self.released += 1;
    }

    const vtable = driver.sinkVTable(Probe);

    fn sink(self: *Probe) driver.Sink {
        return .{ .ptr = self, .vtable = &vtable };
    }
};

fn pourThenFail(p: *Probe, fail_before_close: bool) !void {
    var g = SinkGuard.init(p.sink());
    errdefer g.abort();
    if (fail_before_close) return error.Upstream;
    try g.close();
}

fn openSinkLike(open: bool, p: *Probe) !void {
    var g = SinkGuard.initIf(open, p.sink());
    errdefer g.abort();
    try g.close();
}

fn handKept(p: *Probe, fail_before_close: bool) !void {
    const snk = p.sink();
    var snk_open = true;
    errdefer if (snk_open) snk.abort();
    if (fail_before_close) return error.Upstream;
    snk_open = false;
    try snk.close();
}

test "a clean run closes once and never aborts" {
    var p = Probe{};
    try pourThenFail(&p, false);
    try testing.expectEqual(@as(usize, 1), p.closes);
    try testing.expectEqual(@as(usize, 0), p.aborts);
}

test "a failure before close aborts once and never closes" {
    var p = Probe{};
    try testing.expectError(error.Upstream, pourThenFail(&p, true));
    try testing.expectEqual(@as(usize, 0), p.closes);
    try testing.expectEqual(@as(usize, 1), p.aborts);
}

test "a failed close is not followed by an abort, so the sink is released exactly once" {
    var p = Probe{ .fail_close = true };
    try testing.expectError(error.WriteFailed, pourThenFail(&p, false));
    try testing.expectEqual(@as(usize, 1), p.closes);
    try testing.expectEqual(@as(usize, 0), p.aborts);
    try testing.expectEqual(@as(usize, 1), p.released);
}

test "the guard does what the hand-kept snk_open flag does, in every outcome" {
    for ([_]bool{ false, true }) |fail_close| {
        for ([_]bool{ false, true }) |fail_before| {
            var a = Probe{ .fail_close = fail_close };
            var b = Probe{ .fail_close = fail_close };
            const ra = pourThenFail(&a, fail_before);
            const rb = handKept(&b, fail_before);
            if (rb) |_| try ra else |e| try testing.expectError(e, ra);
            try testing.expectEqual(b.closes, a.closes);
            try testing.expectEqual(b.aborts, a.aborts);
            try testing.expectEqual(@as(usize, 1), a.released);
        }
    }
}

test "an empty guard ignores close and abort; initIf arms only when open" {
    var p = Probe{};
    try openSinkLike(false, &p);
    try testing.expectEqual(@as(usize, 0), p.released);
    try openSinkLike(true, &p);
    try testing.expectEqual(@as(usize, 1), p.closes);

    var g = SinkGuard.empty;
    try testing.expect(!g.armed());
    try g.close();
    g.abort();
    try testing.expect(g.get() == null);
}

test "arm late, then abort and close are each idempotent" {
    var p = Probe{};
    var g = SinkGuard.empty;
    g.arm(p.sink());
    try testing.expect(g.armed());
    g.abort();
    g.abort();
    try g.close();
    try testing.expectEqual(@as(usize, 1), p.aborts);
    try testing.expectEqual(@as(usize, 0), p.closes);

    var q = Probe{};
    g.arm(q.sink());
    try g.close();
    try g.close();
    g.abort();
    try testing.expectEqual(@as(usize, 1), q.closes);
    try testing.expectEqual(@as(usize, 0), q.aborts);
}

test "release hands the sink back unguarded" {
    var p = Probe{};
    var g = SinkGuard.init(p.sink());
    const s = g.release().?;
    g.abort();
    try testing.expectEqual(@as(usize, 0), p.released);
    try s.close();
    try testing.expectEqual(@as(usize, 1), p.closes);
    try testing.expect(g.release() == null);
}

test "settle aborts a failed lane and closes a good one" {
    var p = Probe{};
    var g = SinkGuard.init(p.sink());
    try g.settle(true);
    try testing.expectEqual(@as(usize, 1), p.aborts);
    try testing.expectEqual(@as(usize, 0), p.closes);

    var q = Probe{ .fail_close = true };
    var h = SinkGuard.init(q.sink());
    try testing.expectError(error.WriteFailed, h.settle(false));
    h.abort();
    try testing.expectEqual(@as(usize, 1), q.closes);
    try testing.expectEqual(@as(usize, 0), q.aborts);
}

test "Guard works over a pointer to a concrete writer, the EXPLAIN Arrow shape" {
    var p = Probe{ .fail_close = true };
    const run = struct {
        fn f(w: *Probe) !void {
            var g = Guard(*Probe).init(w);
            errdefer g.abort();
            try g.close();
        }
    }.f;
    try testing.expectError(error.WriteFailed, run(&p));
    try testing.expectEqual(@as(usize, 1), p.released);
}
