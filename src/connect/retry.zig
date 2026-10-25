//! The one retry loop every connector shares, moved from `store/objstore.zig`
//! (`retry`, `Policy`, `Verdict`, `pause`, `retriable`, `transient`), so the object
//! stores, the HTTP client, stream load and the SQL sinks back off the same way.
//!
//! An attempt reports `.done`, `.again` (it fixed the cause itself, e.g. created a
//! missing container: no backoff, but the attempt counter still advances) or
//! `.failed` with a status, or returns an error. A failure is retried when its
//! status is retriable (or its `retriable` override says so), an error when it is
//! transient; anything else ends the loop at once. Waits double from `base_ms` at
//! most `max_shift` times, are capped at `max_ms`, gain up to `jitter_pct` percent
//! jitter so parallel lanes throttled together do not retry in lockstep, and stretch
//! to any `Retry-After`. The loop stops after `max_attempts`, once `budget_ms` of
//! sleep is spent, or as soon as the run is aborting; a pause sleeps in 100 ms
//! slices so Ctrl+C is honoured within one slice. The defaults are objstore's.

const std = @import("std");
const driver = @import("driver.zig");

pub const max_attempts = 5;
pub const budget_ms: u64 = 30_000;
pub const slice_ms: u64 = 100;

pub const retriable = driver.transientStatus;

/// A request that died on the wire: the socket set every connector shares, plus a
/// server that closed the connection instead of answering.
pub fn transient(e: anyerror) bool {
    return driver.transientNet(e) or e == error.HttpConnectionClosing;
}

/// `Retry-After` in delta-seconds; the HTTP-date form is left to the backoff.
pub fn retryAfterMs(value: []const u8) ?u64 {
    const secs = std.fmt.parseInt(u64, std.mem.trim(u8, value, " \t"), 10) catch return null;
    return std.math.mul(u64, secs, std.time.ms_per_s) catch budget_ms;
}

/// Seeded from the OS, so lanes throttled in the same millisecond still draw different jitter.
pub fn jitterPrng() std.Random.DefaultPrng {
    return std.Random.DefaultPrng.init(std.crypto.random.int(u64));
}

/// `retriable` overrides the status rule, for a caller with its own list of codes.
pub const Verdict = union(enum) {
    done,
    again,
    failed: Failure,
};

pub const Failure = struct { code: u16, err: anyerror, retry_after_ms: ?u64 = null, retriable: ?bool = null };

pub const Sleep = *const fn (ns: u64) void;

pub const Policy = struct {
    rand: std.Random,
    sleep: Sleep = std.Thread.sleep,
    max_attempts: usize = max_attempts,
    base_ms: u64 = 500,
    max_shift: u6 = 4,
    max_ms: u64 = std.math.maxInt(u64),
    jitter_pct: u8 = 50,
    budget_ms: u64 = budget_ms,
    retriable: *const fn (code: u16) bool = retriable,
    transient: *const fn (e: anyerror) bool = transient,

    /// `base_ms` doubling per attempt, at most `max_shift` times, capped at `max_ms`,
    /// plus up to `jitter_pct` percent of that as jitter. Saturates instead of overflowing.
    pub fn backoffMs(self: Policy, attempt: usize) u64 {
        const shifted = self.base_ms *| (@as(u64, 1) << @intCast(@min(attempt, self.max_shift)));
        const base = @min(shifted, self.max_ms);
        if (self.jitter_pct == 0) return base;
        const spread = base / 100 * self.jitter_pct + base % 100 * self.jitter_pct / 100;
        return base +| self.rand.uintLessThan(u64, spread + 1);
    }

    /// `pause` with this policy's sleep.
    pub fn pause(self: Policy, ms: u64) bool {
        return pauseWith(self.sleep, ms);
    }
};

/// Runs `attemptFn(ctx)` until it reports `.done`, giving up on the first failure
/// that is not retriable, after the policy's attempts, once its sleep budget is
/// spent, or as soon as the run is aborting; the last failure's error is returned.
pub fn retry(policy: Policy, ctx: anytype, comptime attemptFn: anytype) anyerror!void {
    var slept: u64 = 0;
    var attempt: usize = 0;
    while (true) : (attempt += 1) {
        var wait = policy.backoffMs(attempt);
        const err = if (attemptFn(ctx)) |v| switch (v) {
            .done => return,
            .again => continue,
            .failed => |f| blk: {
                if (!(f.retriable orelse policy.retriable(f.code))) return f.err;
                wait = @max(wait, f.retry_after_ms orelse 0);
                break :blk f.err;
            },
        } else |e| blk: {
            if (!policy.transient(e)) return e;
            break :blk e;
        };
        if (attempt + 1 >= policy.max_attempts or slept >= policy.budget_ms or driver.aborting()) return err;
        wait = @min(wait, policy.budget_ms - slept);
        if (!policy.pause(wait)) return err;
        slept += wait;
    }
}

/// Sleeps `ms` in `slice_ms` slices; false as soon as the run starts aborting.
pub fn pauseWith(sleep: Sleep, ms: u64) bool {
    var left = ms;
    while (left > 0) {
        const step: u64 = @min(left, slice_ms);
        sleep(step * std.time.ns_per_ms);
        left -= step;
        if (driver.aborting()) return false;
    }
    return true;
}

const testing = std.testing;
const objstore = @import("../store/objstore.zig");

const Scripted = struct {
    codes: []const u16,
    calls: usize = 0,
    retry_after_ms: ?u64 = null,
    override: ?bool = null,

    var slept_ms: u64 = 0;
    var naps: usize = 0;
    var abort_after_naps: usize = std.math.maxInt(usize);

    fn reset() void {
        slept_ms = 0;
        naps = 0;
        abort_after_naps = std.math.maxInt(usize);
    }

    /// 0 fixes the cause itself (`.again`), 1 is a reset connection, 2 a local
    /// non-transient failure, 3 a server that closed the connection, 200 success,
    /// anything else that status.
    fn attempt(self: *Scripted) !Verdict {
        const code = self.codes[@min(self.calls, self.codes.len - 1)];
        self.calls += 1;
        return switch (code) {
            0 => .again,
            1 => error.ConnectionResetByPeer,
            2 => error.OutOfMemory,
            3 => error.HttpConnectionClosing,
            200 => .done,
            else => .{ .failed = .{ .code = code, .err = error.Scripted, .retry_after_ms = self.retry_after_ms, .retriable = self.override } },
        };
    }

    fn objAttempt(self: *Scripted) !objstore.Verdict {
        return switch (try self.attempt()) {
            .done => .done,
            .again => .again,
            .failed => |f| .{ .failed = .{ .code = f.code, .err = f.err, .retry_after_ms = f.retry_after_ms, .retriable = f.retriable } },
        };
    }

    fn noSleep(ns: u64) void {
        slept_ms += ns / std.time.ns_per_ms;
        naps += 1;
        if (naps >= abort_after_naps) driver.requestAbort();
    }
    fn abortingSleep(ns: u64) void {
        noSleep(ns);
        driver.requestAbort();
    }
};

test "status and error classification match objstore's lists" {
    for ([_]u16{ 408, 429, 500, 502, 503, 504 }) |c| try testing.expect(retriable(c));
    for ([_]u16{ 0, 100, 200, 201, 206, 301, 400, 401, 403, 404, 409, 412, 413, 501, 505, 599 }) |c| try testing.expect(!retriable(c));
    for (0..1000) |c| try testing.expectEqual(objstore.retriable(@intCast(c)), retriable(@intCast(c)));
    const errs = [_]anyerror{
        error.ConnectionRefused,  error.ConnectionTimedOut,       error.ConnectionResetByPeer,      error.NetworkUnreachable,
        error.HostUnreachable,    error.BrokenPipe,               error.EndOfStream,                error.ReadFailed,
        error.WriteFailed,        error.UnexpectedConnectFailure, error.TemporaryNameServerFailure, error.ServerClosedConnection,
        error.ConnectionIoFailed, error.HttpConnectionClosing,
    };
    for (errs) |e| {
        try testing.expect(transient(e));
        try testing.expect(objstore.transient(e));
    }
    for ([_]anyerror{ error.OutOfMemory, error.S3AuthFailed, error.Aborted, error.UnknownHostName, error.S3Throttled, error.TlsFailure }) |e| {
        try testing.expect(!transient(e));
        try testing.expectEqual(objstore.transient(e), transient(e));
    }
}

test "backoff doubles from 500 ms with up to 50% jitter, capped at 8 s" {
    for (0..5) |i| {
        const base = @as(u64, 500) << @intCast(i);
        var above_base = false;
        for (0..256) |seed| {
            var prng = std.Random.DefaultPrng.init(seed);
            const ms = (Policy{ .rand = prng.random() }).backoffMs(i);
            try testing.expect(ms >= base and ms <= base + base / 2);
            if (ms > base) above_base = true;
        }
        try testing.expect(above_base);
    }
    for ([_]usize{ 5, 6, 12, 40, std.math.maxInt(usize) }) |attempt| {
        for (0..64) |seed| {
            var prng = std.Random.DefaultPrng.init(seed);
            const ms = (Policy{ .rand = prng.random() }).backoffMs(attempt);
            try testing.expect(ms >= 8000 and ms <= 12000);
        }
    }
}

test "backoff draws the same jitter as objstore's for the same seed" {
    for (0..64) |seed| {
        for (0..10) |attempt| {
            var a = std.Random.DefaultPrng.init(seed);
            var b = std.Random.DefaultPrng.init(seed);
            const mine = (Policy{ .rand = a.random() }).backoffMs(attempt);
            const theirs = (objstore.Policy{ .rand = b.random() }).backoffMs(attempt);
            try testing.expectEqual(theirs, mine);
            const mine6 = (Policy{ .rand = a.random(), .base_ms = 37, .max_shift = 6 }).backoffMs(attempt);
            const theirs6 = (objstore.Policy{ .rand = b.random(), .base_ms = 37, .max_shift = 6 }).backoffMs(attempt);
            try testing.expectEqual(theirs6, mine6);
        }
    }
}

test "backoff saturates on a huge base, and jitter_pct and max_ms shape the wait" {
    var prng = std.Random.DefaultPrng.init(1);
    const huge = Policy{ .rand = prng.random(), .base_ms = std.math.maxInt(u64), .max_shift = 63 };
    try testing.expectEqual(@as(u64, std.math.maxInt(u64)), huge.backoffMs(63));
    try testing.expectEqual(@as(u64, std.math.maxInt(u64)), huge.backoffMs(0));

    const flat = Policy{ .rand = prng.random(), .jitter_pct = 0 };
    for (0..5) |i| try testing.expectEqual(@as(u64, 500) << @intCast(i), flat.backoffMs(i));
    try testing.expectEqual(@as(u64, 8000), flat.backoffMs(99));

    const full = Policy{ .rand = prng.random(), .base_ms = 7, .jitter_pct = 100 };
    for (0..200) |_| {
        const ms = full.backoffMs(0);
        try testing.expect(ms >= 7 and ms <= 14);
    }

    const odd = Policy{ .rand = prng.random(), .base_ms = 199, .max_shift = 0, .jitter_pct = 50 };
    for (0..500) |_| try testing.expect(odd.backoffMs(0) <= 199 + 99);

    const capped = Policy{ .rand = prng.random(), .base_ms = 1000, .max_shift = 6, .max_ms = 10_000, .jitter_pct = 0 };
    var delay: u64 = 1000;
    for (0..8) |i| {
        try testing.expectEqual(delay, capped.backoffMs(i));
        delay = @min(delay * 2, 10_000);
    }
}

test "Retry-After reads delta-seconds and nothing else" {
    try testing.expectEqual(@as(?u64, 7000), retryAfterMs("7"));
    try testing.expectEqual(@as(?u64, 0), retryAfterMs(" 0 "));
    try testing.expectEqual(@as(?u64, 120_000), retryAfterMs("\t120"));
    try testing.expectEqual(@as(?u64, null), retryAfterMs("Wed, 21 Oct 2015 07:28:00 GMT"));
    try testing.expectEqual(@as(?u64, null), retryAfterMs(""));
    try testing.expectEqual(@as(?u64, null), retryAfterMs("-1"));
    try testing.expectEqual(@as(?u64, null), retryAfterMs("1.5"));
    try testing.expectEqual(@as(?u64, budget_ms), retryAfterMs("18446744073709551615"));
    for ([_][]const u8{ "7", " 0 ", "x", "", "18446744073709551615", "99999999999999999999" }) |s|
        try testing.expectEqual(objstore.retryAfterMs(s), retryAfterMs(s));
}

test "retry gives up after max_attempts and only ever retries transient failures" {
    Scripted.reset();
    var prng = std.Random.DefaultPrng.init(0);
    const policy = Policy{ .rand = prng.random(), .sleep = Scripted.noSleep };

    var throttled = Scripted{ .codes = &.{503} };
    try testing.expectError(error.Scripted, retry(policy, &throttled, Scripted.attempt));
    try testing.expectEqual(@as(usize, max_attempts), throttled.calls);

    var flaky = Scripted{ .codes = &.{ 500, 429, 1, 502, 504, 200 } };
    try testing.expectError(error.Scripted, retry(policy, &flaky, Scripted.attempt));
    try testing.expectEqual(@as(usize, max_attempts), flaky.calls);

    var recovers = Scripted{ .codes = &.{ 1, 408, 3, 200 } };
    try retry(policy, &recovers, Scripted.attempt);
    try testing.expectEqual(@as(usize, 4), recovers.calls);

    var denied = Scripted{ .codes = &.{ 403, 200 } };
    try testing.expectError(error.Scripted, retry(policy, &denied, Scripted.attempt));
    try testing.expectEqual(@as(usize, 1), denied.calls);

    var broken = Scripted{ .codes = &.{ 2, 200 } };
    try testing.expectError(error.OutOfMemory, retry(policy, &broken, Scripted.attempt));
    try testing.expectEqual(@as(usize, 1), broken.calls);

    var created = Scripted{ .codes = &.{ 0, 200 } };
    try retry(policy, &created, Scripted.attempt);
    try testing.expectEqual(@as(usize, 2), created.calls);

    var first = Scripted{ .codes = &.{200} };
    try retry(policy, &first, Scripted.attempt);
    try testing.expectEqual(@as(usize, 1), first.calls);

    var once = Scripted{ .codes = &.{503} };
    try testing.expectError(error.Scripted, retry(.{ .rand = prng.random(), .sleep = Scripted.noSleep, .max_attempts = 1 }, &once, Scripted.attempt));
    try testing.expectEqual(@as(usize, 1), once.calls);

    var zero = Scripted{ .codes = &.{503} };
    try testing.expectError(error.Scripted, retry(.{ .rand = prng.random(), .sleep = Scripted.noSleep, .max_attempts = 0 }, &zero, Scripted.attempt));
    try testing.expectEqual(@as(usize, 1), zero.calls);
}

test "a failure's retriable override beats the status list both ways" {
    Scripted.reset();
    var prng = std.Random.DefaultPrng.init(0);
    const policy = Policy{ .rand = prng.random(), .sleep = Scripted.noSleep };

    var forced = Scripted{ .codes = &.{ 418, 418, 200 }, .override = true };
    try retry(policy, &forced, Scripted.attempt);
    try testing.expectEqual(@as(usize, 3), forced.calls);

    var refused = Scripted{ .codes = &.{ 503, 200 }, .override = false };
    try testing.expectError(error.Scripted, retry(policy, &refused, Scripted.attempt));
    try testing.expectEqual(@as(usize, 1), refused.calls);
}

fn only501(code: u16) bool {
    return code == 501;
}
fn onlyOom(e: anyerror) bool {
    return e == error.OutOfMemory;
}

test "a policy's own classifiers replace the default lists" {
    Scripted.reset();
    var prng = std.Random.DefaultPrng.init(0);
    const policy = Policy{ .rand = prng.random(), .sleep = Scripted.noSleep, .retriable = only501, .transient = onlyOom };

    var gateway = Scripted{ .codes = &.{ 501, 2, 200 } };
    try retry(policy, &gateway, Scripted.attempt);
    try testing.expectEqual(@as(usize, 3), gateway.calls);

    var busy = Scripted{ .codes = &.{ 503, 200 } };
    try testing.expectError(error.Scripted, retry(policy, &busy, Scripted.attempt));
    try testing.expectEqual(@as(usize, 1), busy.calls);

    var reset = Scripted{ .codes = &.{ 1, 200 } };
    try testing.expectError(error.ConnectionResetByPeer, retry(policy, &reset, Scripted.attempt));
    try testing.expectEqual(@as(usize, 1), reset.calls);
}

test "retry stretches the wait to Retry-After but never sleeps past the budget" {
    var prng = std.Random.DefaultPrng.init(0);
    const policy = Policy{ .rand = prng.random(), .sleep = Scripted.noSleep };

    Scripted.reset();
    var told = Scripted{ .codes = &.{ 503, 200 }, .retry_after_ms = 4000 };
    try retry(policy, &told, Scripted.attempt);
    try testing.expectEqual(@as(u64, 4000), Scripted.slept_ms);
    try testing.expectEqual(@as(usize, 40), Scripted.naps);

    Scripted.reset();
    var short = Scripted{ .codes = &.{ 503, 200 }, .retry_after_ms = 1 };
    try retry(policy, &short, Scripted.attempt);
    try testing.expect(Scripted.slept_ms >= 500 and Scripted.slept_ms <= 750);

    Scripted.reset();
    var greedy = Scripted{ .codes = &.{503}, .retry_after_ms = 3600 * 1000 };
    try testing.expectError(error.Scripted, retry(policy, &greedy, Scripted.attempt));
    try testing.expectEqual(budget_ms, Scripted.slept_ms);
    try testing.expectEqual(@as(usize, 2), greedy.calls);

    Scripted.reset();
    var plain = Scripted{ .codes = &.{503} };
    try testing.expectError(error.Scripted, retry(policy, &plain, Scripted.attempt));
    try testing.expect(Scripted.slept_ms >= 500 + 1000 + 2000 + 4000 and Scripted.slept_ms <= budget_ms);

    Scripted.reset();
    var endless = Scripted{ .codes = &.{503} };
    try testing.expectError(error.Scripted, retry(.{ .rand = prng.random(), .sleep = Scripted.noSleep, .max_attempts = 4, .budget_ms = std.math.maxInt(u64), .base_ms = 1, .max_shift = 6 }, &endless, Scripted.attempt));
    try testing.expectEqual(@as(usize, 4), endless.calls);
}

test "retry stops as soon as the run is aborting" {
    defer driver.resetAbort();
    var prng = std.Random.DefaultPrng.init(0);
    const policy = Policy{ .rand = prng.random(), .sleep = Scripted.abortingSleep };

    Scripted.reset();
    var throttled = Scripted{ .codes = &.{503} };
    try testing.expectError(error.Scripted, retry(policy, &throttled, Scripted.attempt));
    try testing.expectEqual(@as(usize, 1), throttled.calls);
    try testing.expect(Scripted.slept_ms <= slice_ms);

    var again = Scripted{ .codes = &.{503} };
    try testing.expectError(error.Scripted, retry(policy, &again, Scripted.attempt));
    try testing.expectEqual(@as(usize, 1), again.calls);

    var done = Scripted{ .codes = &.{200} };
    try retry(policy, &done, Scripted.attempt);
    try testing.expectEqual(@as(usize, 1), done.calls);
}

test "pause sleeps in 100 ms slices and stops within one slice of an abort" {
    defer driver.resetAbort();
    Scripted.reset();
    try testing.expect(pauseWith(Scripted.noSleep, 0));
    try testing.expectEqual(@as(usize, 0), Scripted.naps);
    try testing.expect(pauseWith(Scripted.noSleep, 250));
    try testing.expectEqual(@as(u64, 250), Scripted.slept_ms);
    try testing.expectEqual(@as(usize, 3), Scripted.naps);

    Scripted.reset();
    Scripted.abort_after_naps = 3;
    try testing.expect(!pauseWith(Scripted.noSleep, 10_000));
    try testing.expectEqual(@as(u64, 300), Scripted.slept_ms);
    driver.resetAbort();

    Scripted.reset();
    Scripted.abort_after_naps = 2;
    var prng = std.Random.DefaultPrng.init(0);
    var long = Scripted{ .codes = &.{503}, .retry_after_ms = 5000 };
    try testing.expectError(error.Scripted, retry(.{ .rand = prng.random(), .sleep = Scripted.noSleep }, &long, Scripted.attempt));
    try testing.expectEqual(@as(usize, 1), long.calls);
    try testing.expectEqual(@as(u64, 200), Scripted.slept_ms);
}

test "pinned: .again sleeps not at all but spends an attempt, so a later failure past max_attempts ends at once" {
    Scripted.reset();
    var prng = std.Random.DefaultPrng.init(0);
    const policy = Policy{ .rand = prng.random(), .sleep = Scripted.noSleep, .jitter_pct = 0 };
    var stubborn = Scripted{ .codes = &.{ 0, 0, 0, 0, 0, 0, 0, 503, 200 } };
    try testing.expectError(error.Scripted, retry(policy, &stubborn, Scripted.attempt));
    try testing.expectEqual(@as(usize, 8), stubborn.calls);
    try testing.expectEqual(@as(u64, 0), Scripted.slept_ms);

    Scripted.reset();
    var late = Scripted{ .codes = &.{ 0, 0, 503, 200 } };
    try retry(policy, &late, Scripted.attempt);
    try testing.expectEqual(@as(usize, 4), late.calls);
    try testing.expectEqual(@as(u64, 2000), Scripted.slept_ms);

    var prng2 = std.Random.DefaultPrng.init(0);
    Scripted.reset();
    var theirs = Scripted{ .codes = &.{ 0, 0, 0, 0, 0, 0, 0, 503, 200 } };
    try testing.expectError(error.Scripted, objstore.retry(.{ .rand = prng2.random(), .sleep = Scripted.noSleep }, &theirs, Scripted.objAttempt));
    try testing.expectEqual(@as(usize, 8), theirs.calls);
}

test "retry makes the same calls and sleeps as objstore's loop for every script" {
    defer driver.resetAbort();
    const scripts = [_][]const u16{
        &.{200},                &.{503},              &.{ 500, 429, 1, 502, 504, 200 },
        &.{ 1, 408, 3, 200 },   &.{ 403, 200 },       &.{ 2, 200 },
        &.{ 0, 200 },           &.{ 0, 0, 503, 200 }, &.{ 501, 200 },
        &.{ 3, 3, 3, 3, 3, 3 }, &.{ 408, 0, 1, 200 },
    };
    for ([_]?u64{ null, 0, 900, 20_000 }) |after| {
        for (scripts) |codes| {
            for (0..4) |seed| {
                var pa = std.Random.DefaultPrng.init(seed);
                Scripted.reset();
                var mine = Scripted{ .codes = codes, .retry_after_ms = after };
                const r1 = retry(.{ .rand = pa.random(), .sleep = Scripted.noSleep }, &mine, Scripted.attempt);
                const slept1 = Scripted.slept_ms;
                const naps1 = Scripted.naps;

                var pb = std.Random.DefaultPrng.init(seed);
                Scripted.reset();
                var theirs = Scripted{ .codes = codes, .retry_after_ms = after };
                const r2 = objstore.retry(.{ .rand = pb.random(), .sleep = Scripted.noSleep }, &theirs, Scripted.objAttempt);

                try testing.expectEqual(theirs.calls, mine.calls);
                try testing.expectEqual(Scripted.slept_ms, slept1);
                try testing.expectEqual(Scripted.naps, naps1);
                if (r2) |_| try r1 else |e| try testing.expectError(e, r1);
            }
        }
    }
}
