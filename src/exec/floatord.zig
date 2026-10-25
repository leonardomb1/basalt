//! The engine's one total order over f64, in three shapes that agree on every pair:
//! `orderF64` (a `std.math.Order`, for comparators), `ltF64`/`eqF64` (branchless
//! bools, so vector lanes stay vectorized) and `orderedBits` (a u64 whose unsigned
//! order is the float's, for radix and prefix keys). Every NaN equals every other
//! NaN and sorts above +inf; -0.0 equals 0.0. `std.math.order` on a NaN reaches
//! `unreachable`, so float comparisons go through here.

const std = @import("std");

/// Copied from `exec/eval/support.zig:176`; Wave 1 points that one here.
pub fn orderF64(x: f64, y: f64) std.math.Order {
    const xn = std.math.isNan(x);
    const yn = std.math.isNan(y);
    if (xn or yn) {
        if (xn and yn) return .eq;
        return if (xn) .gt else .lt;
    }
    return std.math.order(x, y);
}

/// `orderF64(a, d) == .lt` without branches (the `cmpLane` body, `exec/eval/vec.zig:549`).
/// The other comparisons follow: `gt(a, d)` is `ltF64(d, a)`, `le` is `!ltF64(d, a)`,
/// `ge` is `!ltF64(a, d)`, `ne` is `!eqF64(a, d)`.
pub inline fn ltF64(a: f64, d: f64) bool {
    const an = std.math.isNan(a);
    const dn = std.math.isNan(d);
    return (a < d) | (!an & dn);
}

/// `orderF64(a, d) == .eq` without branches (the `cmpLane` body, `exec/eval/vec.zig:550`).
pub inline fn eqF64(a: f64, d: f64) bool {
    const an = std.math.isNan(a);
    const dn = std.math.isNan(d);
    return (a == d) | (an & dn);
}

/// The ascending u64 key: the IEEE sign trick with -0.0 folded into 0.0 and every NaN
/// canonicalized above +inf (the float arm of `orderedWord`, `exec/op/sort.zig:1090`).
/// A descending key is its complement.
pub fn orderedBits(x: f64) u64 {
    const y = if (x == 0) 0.0 else x;
    const b: u64 = if (std.math.isNan(y)) 0x7ff8000000000000 else @bitCast(y);
    return if (b >> 63 != 0) ~b else b | (1 << 63);
}

const testing = std.testing;

fn bits(b: u64) f64 {
    return @bitCast(b);
}

const ranked = [_][]const f64{
    &.{-std.math.inf(f64)},
    &.{-std.math.floatMax(f64)},
    &.{-1e300},
    &.{-1.5},
    &.{-1.0},
    &.{-std.math.floatMin(f64)},
    &.{-std.math.floatTrueMin(f64)},
    &.{ -0.0, 0.0 },
    &.{std.math.floatTrueMin(f64)},
    &.{bits(0x000fffffffffffff)},
    &.{std.math.floatMin(f64)},
    &.{1.0},
    &.{bits(0x3ff0000000000001)},
    &.{1e300},
    &.{std.math.floatMax(f64)},
    &.{std.math.inf(f64)},
    &.{
        std.math.nan(f64),
        -std.math.nan(f64),
        std.math.snan(f64),
        bits(0x7ff0000000000001),
        bits(0xfff0000000000001),
        bits(0x7fffffffffffffff),
        bits(0xffffffffffffffff),
        bits(0x7ff8000000000123),
    },
};

const Pair = struct { x: f64, y: f64, want: std.math.Order };

fn matrix(buf: []Pair) []Pair {
    var n: usize = 0;
    for (ranked, 0..) |gx, rx| for (gx) |x| for (ranked, 0..) |gy, ry| for (gy) |y| {
        buf[n] = .{ .x = x, .y = y, .want = std.math.order(rx, ry) };
        n += 1;
    };
    return buf[0..n];
}

fn matrixLen() usize {
    var n: usize = 0;
    for (ranked) |g| n += g.len;
    return n * n;
}

fn pinnedCmpLane(comptime op: enum { eq, ne, lt, le, gt, ge }, a: f64, d: f64) bool {
    const an = std.math.isNan(a);
    const dn = std.math.isNan(d);
    const eq = (a == d) | (an & dn);
    const lt = (a < d) | (!an & dn);
    const gt = (a > d) | (an & !dn);
    return switch (op) {
        .eq => eq,
        .ne => !eq,
        .lt => lt,
        .le => !gt,
        .gt => gt,
        .ge => !lt,
    };
}

fn pinnedOrderedWord(x0: f64, desc: bool) u64 {
    const x = if (x0 == 0) 0.0 else x0;
    const b: u64 = if (std.math.isNan(x)) 0x7ff8000000000000 else @bitCast(x);
    const w = if (b >> 63 != 0) ~b else b | (1 << 63);
    return if (desc) ~w else w;
}

fn pinnedHalvesLt(ax: f64, ap: usize, bx: f64, bp: usize) bool {
    const an = std.math.isNan(ax);
    const bn = std.math.isNan(bx);
    if (an != bn) return bn;
    if (!an and ax != bx) return ax < bx;
    return ap < bp;
}

test "floatord: orderF64 ranks the whole NaN/±0/inf/subnormal matrix" {
    var buf: [matrixLen()]Pair = undefined;
    for (matrix(&buf)) |p| try testing.expectEqual(p.want, orderF64(p.x, p.y));
}

test "floatord: ltF64 and eqF64 agree with orderF64 on every pair" {
    var buf: [matrixLen()]Pair = undefined;
    for (matrix(&buf)) |p| {
        try testing.expectEqual(p.want == .lt, ltF64(p.x, p.y));
        try testing.expectEqual(p.want == .eq, eqF64(p.x, p.y));
        try testing.expectEqual(p.want == .gt, ltF64(p.y, p.x));
    }
}

test "floatord: orderedBits sorts as orderF64, NaNs and zeros sharing one key" {
    var buf: [matrixLen()]Pair = undefined;
    for (matrix(&buf)) |p| {
        try testing.expectEqual(p.want, std.math.order(orderedBits(p.x), orderedBits(p.y)));
        try testing.expectEqual(p.want, std.math.order(~orderedBits(p.y), ~orderedBits(p.x)));
    }
    try testing.expectEqual(orderedBits(0.0), orderedBits(-0.0));
    try testing.expectEqual(@as(u64, 1 << 63), orderedBits(0.0));
    try testing.expectEqual(orderedBits(std.math.nan(f64)), orderedBits(bits(0xffffffffffffffff)));
    try testing.expectEqual(@as(u64, 0xfff8000000000000), orderedBits(std.math.nan(f64)));
    try testing.expect(orderedBits(std.math.inf(f64)) < orderedBits(std.math.nan(f64)));
}

test "floatord: orderedBits round trips every non-NaN, non-negative-zero float" {
    var buf: [matrixLen()]Pair = undefined;
    for (matrix(&buf)) |p| {
        if (std.math.isNan(p.x) or (p.x == 0 and std.math.signbit(p.x))) continue;
        const k = orderedBits(p.x);
        const b: u64 = if (k >> 63 != 0) k & ~@as(u64, 1 << 63) else ~k;
        try testing.expectEqual(@as(u64, @bitCast(p.x)), b);
    }
}

test "floatord: the vec.zig cmpLane copy matches ltF64/eqF64 for all six ops" {
    var buf: [matrixLen()]Pair = undefined;
    for (matrix(&buf)) |p| {
        const a = p.x;
        const d = p.y;
        try testing.expectEqual(pinnedCmpLane(.eq, a, d), eqF64(a, d));
        try testing.expectEqual(pinnedCmpLane(.ne, a, d), !eqF64(a, d));
        try testing.expectEqual(pinnedCmpLane(.lt, a, d), ltF64(a, d));
        try testing.expectEqual(pinnedCmpLane(.le, a, d), !ltF64(d, a));
        try testing.expectEqual(pinnedCmpLane(.gt, a, d), ltF64(d, a));
        try testing.expectEqual(pinnedCmpLane(.ge, a, d), !ltF64(a, d));
    }
}

test "floatord: the sort.zig orderedWord float copy matches orderedBits both ways" {
    var buf: [matrixLen()]Pair = undefined;
    for (matrix(&buf)) |p| {
        try testing.expectEqual(pinnedOrderedWord(p.x, false), orderedBits(p.x));
        try testing.expectEqual(pinnedOrderedWord(p.x, true), ~orderedBits(p.x));
    }
}

test "floatord: the window.zig Halves.lt copy is orderF64 with ties by position" {
    var buf: [matrixLen()]Pair = undefined;
    for (matrix(&buf)) |p| for ([_][2]usize{ .{ 0, 1 }, .{ 1, 0 }, .{ 3, 3 } }) |pos| {
        const o = orderF64(p.x, p.y);
        const want = o == .lt or (o == .eq and pos[0] < pos[1]);
        try testing.expectEqual(pinnedHalvesLt(p.x, pos[0], p.y, pos[1]), want);
    };
}

test "floatord: std.math.order (the repl view.zig copy) agrees off NaN, where it has no answer" {
    var buf: [matrixLen()]Pair = undefined;
    for (matrix(&buf)) |p| {
        if (std.math.isNan(p.x) or std.math.isNan(p.y)) continue;
        try testing.expectEqual(std.math.order(p.x, p.y), orderF64(p.x, p.y));
    }
}

test "floatord: a comparator sort through orderF64 puts NaN last and is stable on ±0" {
    var xs = [_]f64{ std.math.nan(f64), 2.0, -0.0, -std.math.inf(f64), 0.0, -std.math.nan(f64), 1.0 };
    const Ctx = struct {
        fn lt(_: void, a: f64, b: f64) bool {
            return orderF64(a, b) == .lt;
        }
    };
    std.sort.insertion(f64, &xs, {}, Ctx.lt);
    try testing.expectEqual(-std.math.inf(f64), xs[0]);
    try testing.expect(std.math.signbit(xs[1]) and xs[1] == 0);
    try testing.expect(!std.math.signbit(xs[2]) and xs[2] == 0);
    try testing.expectEqual(@as(f64, 1.0), xs[3]);
    try testing.expectEqual(@as(f64, 2.0), xs[4]);
    try testing.expect(std.math.isNan(xs[5]) and std.math.isNan(xs[6]));
}
