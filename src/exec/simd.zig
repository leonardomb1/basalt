//! Explicit SIMD kernels for the columnar executor.
//!
//! Scope is deliberately narrow and *benchmark-gated* (`zig build bench`): we only
//! keep explicit `@Vector` code where it measurably beats what LLVM already does
//! on plain scalar loops. Benchmarks showed simple all-valid arithmetic and
//! comparison loops are ALREADY auto-vectorized by LLVM (explicit `@Vector` ties
//! or, for compares, regresses), so those stay as ordinary loops in `eval.zig`.
//!
//! The proven win is reductions LLVM cannot legally auto-vectorize: `f64` sum is
//! not associative, so without an explicit `@reduce` it stays serial (~4x slower).
//! Integer sums ARE associative and LLVM vectorizes them, so there is no int-sum
//! kernel here on purpose.

const std = @import("std");

/// Native vector lane count for `T` (>= 1).
pub inline fn lanes(comptime T: type) comptime_int {
    return std.simd.suggestVectorLength(T) orelse 1;
}

/// Sum of an `f64` slice using a vector accumulator + `@reduce` (reassociates,
/// which is exactly why LLVM won't do it for us). Null lanes hold 0 by builder
/// convention, so summing every lane is correct for SQL `SUM` (nulls add 0).
pub fn sumF(a: []const f64) f64 {
    const L = lanes(f64);
    var i: usize = 0;
    var total: f64 = 0;
    if (L > 1) {
        const V = @Vector(L, f64);
        var acc: V = @splat(0);
        while (i + L <= a.len) : (i += L) acc += @as(V, a[i..][0..L].*);
        total = @reduce(.Add, acc);
    }
    while (i < a.len) : (i += 1) total += a[i];
    return total;
}

/// Min of a non-empty `f64` slice. Caller must guarantee no null lanes (their 0
/// default would corrupt the result); see `eval`/aggregate callers' all-valid gate.
pub fn minF(a: []const f64) f64 {
    const L = lanes(f64);
    var i: usize = 0;
    var m: f64 = a[0];
    if (L > 1 and a.len >= L) {
        const V = @Vector(L, f64);
        var acc: V = @splat(a[0]);
        while (i + L <= a.len) : (i += L) acc = @min(acc, @as(V, a[i..][0..L].*));
        m = @reduce(.Min, acc);
    }
    while (i < a.len) : (i += 1) m = @min(m, a[i]);
    return m;
}

/// Max of a non-empty `f64` slice. Same null caveat as `minF`.
pub fn maxF(a: []const f64) f64 {
    const L = lanes(f64);
    var i: usize = 0;
    var m: f64 = a[0];
    if (L > 1 and a.len >= L) {
        const V = @Vector(L, f64);
        var acc: V = @splat(a[0]);
        while (i + L <= a.len) : (i += L) acc = @max(acc, @as(V, a[i..][0..L].*));
        m = @reduce(.Max, acc);
    }
    while (i < a.len) : (i += 1) m = @max(m, a[i]);
    return m;
}

/// Count of set (valid/non-null) bits among the first `n` bits of a validity
/// bitmap, via byte-wise `@popCount`.
pub fn popcountValid(bits: []const u8, n: usize) usize {
    var count: usize = 0;
    const full = n >> 3;
    for (bits[0..full]) |byte| count += @popCount(byte);
    const rem: u3 = @intCast(n & 7);
    if (rem != 0) {
        const mask = (@as(u8, 1) << rem) - 1;
        count += @popCount(bits[full] & mask);
    }
    return count;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

test "sumF matches scalar across vector body + tail" {
    const n = 37;
    var a: [n]f64 = undefined;
    var expect: f64 = 0;
    for (0..n) |i| {
        a[i] = @floatFromInt(i * 3 + 1);
        expect += a[i];
    }
    try testing.expectApproxEqAbs(expect, sumF(&a), 1e-9);
}

test "sumF agrees with a scalar loop at every remainder length" {
    const L = lanes(f64);
    var buf: [2 * L + 3]f64 = undefined;
    for (&buf, 0..) |*x, i| x.* = @as(f64, @floatFromInt(i)) * 1.5 - 3;
    var n: usize = 0;
    while (n <= buf.len) : (n += 1) { // n = 0 covers the empty slice
        var expect: f64 = 0;
        for (buf[0..n]) |x| expect += x;
        try testing.expectApproxEqAbs(expect, sumF(buf[0..n]), 1e-9);
    }
}

test "sumF propagates NaN from vector body and scalar tail" {
    const L = lanes(f64);
    var a: [2 * L + 1]f64 = undefined;
    @memset(&a, 1.0);
    a[0] = std.math.nan(f64); // lands in the vector body (or scalar loop if L == 1)
    try testing.expect(std.math.isNan(sumF(&a)));
    @memset(&a, 1.0);
    a[a.len - 1] = std.math.nan(f64); // odd length: always the scalar tail
    try testing.expect(std.math.isNan(sumF(&a)));
}

test "minF/maxF" {
    const a = [_]f64{ 5, 3, 9, 1, 7, 2, 8, 4, 6, 0, 11, 10, 12, 13 };
    try testing.expectEqual(@as(f64, 0), minF(&a));
    try testing.expectEqual(@as(f64, 13), maxF(&a));
    const one = [_]f64{42};
    try testing.expectEqual(@as(f64, 42), minF(&one));
    try testing.expectEqual(@as(f64, 42), maxF(&one));
}

test "minF/maxF honor extremes in the scalar tail at odd lengths" {
    const L = lanes(f64);
    var a: [2 * L + 1]f64 = undefined; // full vector body + 1-element tail
    for (&a, 0..) |*x, i| x.* = @floatFromInt(i + 10);
    a[a.len - 1] = -1; // extreme only reachable via the tail loop
    try testing.expectEqual(@as(f64, -1), minF(&a));
    a[a.len - 1] = 1e9;
    try testing.expectEqual(@as(f64, 10), minF(&a)); // body extreme
    try testing.expectEqual(@as(f64, 1e9), maxF(&a)); // tail extreme
}

test "popcountValid: zero bits and exact byte multiples read no partial byte" {
    const bits = [_]u8{ 0b1010_1010, 0xFF };
    try testing.expectEqual(@as(usize, 0), popcountValid(&bits, 0));
    try testing.expectEqual(@as(usize, 1), popcountValid(&bits, 2)); // bits 0..1 -> only bit 1
    // n == bits.len * 8: rem is 0, so bits[full] must not be touched (would be OOB)
    try testing.expectEqual(@as(usize, 12), popcountValid(&bits, 16));
}

test "popcountValid honors partial trailing byte" {
    // 20 bits: set every bit, then verify; then clear a few.
    const alloc = testing.allocator;
    const bits = try alloc.alloc(u8, 3);
    defer alloc.free(bits);
    @memset(bits, 0xFF);
    try testing.expectEqual(@as(usize, 20), popcountValid(bits, 20));
    bits[0] &= ~@as(u8, 1); // clear bit 0
    bits[2] &= ~@as(u8, 0b0000_1000); // clear bit 19
    try testing.expectEqual(@as(usize, 18), popcountValid(bits, 20));
}
