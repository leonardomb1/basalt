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
//! kernel here on purpose. Null lanes hold 0 by builder convention, so a sum
//! over every lane is a correct SQL `SUM`; min and max have no such luck, and
//! their callers must gate on an all-valid column.
//!
//! Substring search is the other win: `find` tests a vector of positions per
//! step where `std.mem.indexOfPos` compares from each one, and made a TPC-H SF1
//! `LIKE '%special%'` scan 2.5-3x faster end to end.

const std = @import("std");

pub inline fn lanes(comptime T: type) comptime_int {
    return std.simd.suggestVectorLength(T) orelse 1;
}

/// Sum with a vector accumulator and `@reduce`, which reassociates; that is
/// exactly why LLVM will not vectorize it for us.
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

/// Min of a non-empty slice with no null lanes, whose 0 would corrupt it.
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

/// Max of a non-empty slice with no null lanes, whose 0 would corrupt it. A NaN is
/// the largest value, as a sort places it, so one makes the max NaN; `@max` alone
/// would skip it, as IEEE `maxNum` does.
pub fn maxF(a: []const f64) f64 {
    const L = lanes(f64);
    var i: usize = 0;
    var m: f64 = a[0];
    var nan = false;
    if (L > 1 and a.len >= L) {
        const V = @Vector(L, f64);
        const B = @Vector(L, bool);
        var acc: V = @splat(a[0]);
        var seen: B = @splat(false);
        while (i + L <= a.len) : (i += L) {
            const v: V = a[i..][0..L].*;
            acc = @max(acc, v);
            seen = @select(bool, v != v, @as(B, @splat(true)), seen);
        }
        m = @reduce(.Max, acc);
        nan = @reduce(.Or, seen);
    }
    while (i < a.len) : (i += 1) {
        m = @max(m, a[i]);
        if (std.math.isNan(a[i])) nan = true;
    }
    return if (nan or std.math.isNan(m)) std.math.nan(f64) else m;
}

/// The first `needle` in `hay` at or after byte `from`, compared byte for byte. A
/// block of positions is tested at once against the needle's first and last bytes,
/// and only positions where both agree are compared in full; in text few do, so
/// the scan runs at about a vector of bytes per step where `std.mem.indexOfPos`
/// compares from every position.
pub fn find(hay: []const u8, needle: []const u8, from: usize) ?usize {
    const n = needle.len;
    if (from > hay.len) return null;
    if (n == 0) return from;
    if (hay.len - from < n) return null;
    if (n == 1) return std.mem.indexOfScalarPos(u8, hay, from, needle[0]);
    const L = lanes(u8);
    const V = @Vector(L, u8);
    const M = std.meta.Int(.unsigned, L);
    const first: V = @splat(needle[0]);
    const last: V = @splat(needle[n - 1]);
    const end = hay.len - n;
    var i = from;
    while (i + L - 1 <= end) : (i += L) {
        const a: V = hay[i..][0..L].*;
        const b: V = hay[i + n - 1 ..][0..L].*;
        var hits = @as(M, @bitCast(a == first)) & @as(M, @bitCast(b == last));
        while (hits != 0) : (hits &= hits - 1) {
            const k = i + @ctz(hits);
            if (std.mem.eql(u8, hay[k + 1 .. k + n - 1], needle[1 .. n - 1])) return k;
        }
    }
    while (i <= end) : (i += 1) {
        if (hay[i] == needle[0] and std.mem.eql(u8, hay[i..][0..n], needle)) return i;
    }
    return null;
}

/// Set bits among the first `n` of a validity bitmap, popcounted a 64-bit word at
/// a time (a byte loop paid eight `popcnt`s for one); the tail goes byte-wise.
pub fn popcountValid(bits: []const u8, n: usize) usize {
    var count: usize = 0;
    const full = n >> 3;
    var i: usize = 0;
    while (i + @sizeOf(u64) <= full) : (i += @sizeOf(u64)) {
        count += @popCount(std.mem.readInt(u64, bits[i..][0..@sizeOf(u64)], .little));
    }
    for (bits[i..full]) |byte| count += @popCount(byte);
    const rem: u3 = @intCast(n & 7);
    if (rem != 0) {
        const mask = (@as(u8, 1) << rem) - 1;
        count += @popCount(bits[full] & mask);
    }
    return count;
}

const testing = std.testing;

test "sumF agrees with a scalar loop at every remainder length" {
    const L = lanes(f64);
    var buf: [2 * L + 3]f64 = undefined;
    for (&buf, 0..) |*x, i| x.* = @as(f64, @floatFromInt(i)) * 1.5 - 3;
    var n: usize = 0;
    while (n <= buf.len) : (n += 1) {
        var expect: f64 = 0;
        for (buf[0..n]) |x| expect += x;
        try testing.expectApproxEqAbs(expect, sumF(buf[0..n]), 1e-9);
    }
}

test "sumF propagates NaN from vector body and scalar tail" {
    const L = lanes(f64);
    var a: [2 * L + 1]f64 = undefined;
    @memset(&a, 1.0);
    a[0] = std.math.nan(f64);
    try testing.expect(std.math.isNan(sumF(&a)));
    @memset(&a, 1.0);
    a[a.len - 1] = std.math.nan(f64);
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

test "maxF is NaN when any value is, as a sort places NaN last; minF skips NaN unless all are" {
    const L = lanes(f64);
    var a: [2 * L + 1]f64 = undefined;
    for ([_]usize{ 0, 1, L, a.len - 1 }) |at| {
        for (&a, 0..) |*x, i| x.* = @floatFromInt(i);
        a[at] = std.math.nan(f64);
        try testing.expect(std.math.isNan(maxF(&a)));
        try testing.expectEqual(@as(f64, if (at == 0) 1 else 0), minF(&a));
    }
    @memset(&a, std.math.nan(f64));
    try testing.expect(std.math.isNan(minF(&a)));
    try testing.expect(std.math.isNan(maxF(&a)));
}

test "minF/maxF honor extremes in the scalar tail at odd lengths" {
    const L = lanes(f64);
    var a: [2 * L + 1]f64 = undefined;
    for (&a, 0..) |*x, i| x.* = @floatFromInt(i + 10);
    a[a.len - 1] = -1;
    try testing.expectEqual(@as(f64, -1), minF(&a));
    a[a.len - 1] = 1e9;
    try testing.expectEqual(@as(f64, 10), minF(&a));
    try testing.expectEqual(@as(f64, 1e9), maxF(&a));
}

test "find agrees with std.mem.indexOfPos for every needle length, start and position, the edges included" {
    var hay: [3 * 64 + 7]u8 = undefined;
    for (&hay, 0..) |*b, i| b.* = "abcab"[i % 5];
    hay[150] = 'x';
    hay[hay.len - 1] = 'z';
    const needles = [_][]const u8{ "", "a", "z", "ab", "ca", "abca", "cabx", "bcabx", "abcabcabcabcabcabcabcabcabcabcabcabcabc", "xz" };
    for (needles) |nd| {
        var from: usize = 0;
        while (from <= hay.len + 1) : (from += 1) {
            const want = if (from > hay.len) null else std.mem.indexOfPos(u8, &hay, from, nd);
            try testing.expectEqual(want, find(&hay, nd, from));
        }
    }
    for (0..hay.len) |len| {
        const h = hay[0..len];
        try testing.expectEqual(std.mem.indexOf(u8, h, "abz"), find(h, "abz", 0));
        try testing.expectEqual(std.mem.indexOf(u8, h, "cabcab"), find(h, "cabcab", 0));
    }
}

test "popcountValid: word-wise count agrees with a bit-by-bit one" {
    const alloc = testing.allocator;
    const n = 200;
    const bits = try alloc.alloc(u8, (n + 7) / 8);
    defer alloc.free(bits);

    for (bits, 0..) |*b, i| b.* = @truncate(i *% 37 +% 11);

    for (0..n + 1) |k| {
        var expect: usize = 0;
        for (0..k) |i| {
            if ((bits[i >> 3] >> @intCast(i & 7)) & 1 != 0) expect += 1;
        }
        try testing.expectEqual(expect, popcountValid(bits, k));
    }

    @memset(bits, 0xFF);
    try testing.expectEqual(@as(usize, n), popcountValid(bits, n));
    try testing.expectEqual(@as(usize, 64), popcountValid(bits, 64));
    @memset(bits, 0);
    try testing.expectEqual(@as(usize, 0), popcountValid(bits, n));
}
