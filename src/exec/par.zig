//! Work split across up to `threads` threads for the breakers' big in-memory passes
//! (dealing rows to sort ranges, sorting the ranges, gathering sorted columns). Items
//! are handed out one at a time from a shared counter, so a slow core takes fewer of
//! them instead of holding up the rest, as an equal split per thread would on a
//! machine with performance and efficiency cores. Row chunks are cut at multiples of
//! 64, so a validity bitmap's bytes never straddle two chunks. The calling thread is
//! one of the workers, and a worker that cannot be spawned leaves the items to the
//! others. `f` cannot fail: allocate before, report through `ctx` after.

const std = @import("std");

pub const max_workers = 64;

/// How many chunks of about `size` rows `n` rows make; one when `n` is small.
pub fn chunks(n: usize, size: usize) usize {
    return @max(1, n / @max(size, 1));
}

/// The rows of chunk `c` of `nc` over `n` rows.
pub fn bounds(n: usize, nc: usize, c: usize) [2]usize {
    const step = std.mem.alignForward(usize, (n + nc - 1) / nc, 64);
    return .{ @min(n, c * step), @min(n, (c + 1) * step) };
}

/// Calls `f(ctx, i)` once for every `i` below `count`, on up to `threads` threads.
pub fn each(threads: usize, count: usize, ctx: anytype, comptime f: fn (@TypeOf(ctx), usize) void) void {
    const workers = @min(@min(threads, count), max_workers);
    if (workers <= 1) {
        for (0..count) |i| f(ctx, i);
        return;
    }
    var next = std.atomic.Value(usize).init(0);
    const W = struct {
        fn work(c: @TypeOf(ctx), nx: *std.atomic.Value(usize), n: usize) void {
            while (true) {
                const i = nx.fetchAdd(1, .monotonic);
                if (i >= n) return;
                f(c, i);
            }
        }
    };
    var handles: [max_workers]?std.Thread = @splat(null);
    for (handles[1..workers]) |*h| h.* = std.Thread.spawn(.{}, W.work, .{ ctx, &next, count }) catch null;
    W.work(ctx, &next, count);
    for (handles[1..workers]) |h| if (h) |t| t.join();
}

test "each hands every item out once; chunks are cut at multiples of 64" {
    var seen = [_]u8{0} ** 1000;
    const Ctx = struct {
        seen: []u8,
        nc: usize,
        fn f(c: *const @This(), i: usize) void {
            const b = bounds(c.seen.len, c.nc, i);
            if (b[0] < b[1]) std.debug.assert(b[0] % 64 == 0);
            for (c.seen[b[0]..b[1]]) |*s| s.* += 1;
        }
    };
    const ctx = Ctx{ .seen = &seen, .nc = chunks(seen.len, 100) };
    each(7, ctx.nc, &ctx, Ctx.f);
    for (seen) |s| try std.testing.expectEqual(@as(u8, 1), s);
    try std.testing.expectEqual(@as(usize, 1), chunks(100, 1000));
}
