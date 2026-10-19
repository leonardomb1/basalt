//! The aggregate functions as math only. Each is a `State` and the rules over it:
//! `add` one value of its input type `In`, `merge` another lane's state, `finish`
//! into the result, and, for the few holding memory, `own` it in the group's
//! allocator once a merge has copied the state in. `kernel.zig` builds every loop
//! around these (a batch, a batch's rows into their groups, a lane's states into
//! another's), so adding an aggregate is a struct here and a line in `kernelFor`.
//!
//! `In` is the value the state takes: `void` when only presence counts, `Value` when
//! no physical type fits. Optional hooks: `addN` (k values at once, for `void`),
//! `addAll` (a slice with no nulls, where a vector loop beats one at a time),
//! `needs_alloc` (the state allocates per value), `fold_expr` (a DECIMAL arithmetic
//! argument is added value by value from its operands, as the column would hold
//! them, without the column), `own` (rebuild in the group's
//! allocator when a merge frees its source), and `remove` (the exact inverse of
//! `add`, which a window frame slides with when a value leaves it). A DISTINCT
//! aggregate is its plain math behind `pairs.zig`'s filter.
//!
//! Sums are exact, i128 for ints and i256 for DECIMAL (whose addends reach 10^38),
//! range-checked once in `finish`, so the answer cannot depend on how rows split
//! across batches and lanes or on their order. DECIMAL addends are normalized to
//! the output scale. A DECIMAL AVG sums exactly at its argument's scale, a value of
//! wider scale as a float, and divides once. Variances keep Welford's moments and
//! merge with Chan's formula, in fixed lane order. Float MIN/MAX order NaN above
//! every number. Bitwise aggregates start from their first value, zero not being
//! AND's identity.

const Value = @import("../../value.zig").Value;
const Decimal = @import("../../value.zig").Decimal;
const ast = @import("../../../lang/ast.zig");
const eval = @import("../../eval.zig");
const simd = @import("../../simd.zig");
const std = @import("std");
const types = @import("../../../lang/types.zig");

/// What the math sees besides its state: the result type, the type values are read
/// as (`ty`, the cast target for a value not already of type `In`), the group's
/// allocator for anything a state keeps, and a scratch one for a cast.
pub const Ctx = struct {
    out: types.Type,
    ty: types.Type,
    alloc: std.mem.Allocator,
    scratch: std.mem.Allocator,
};

pub const Count = struct {
    pub const In = void;
    pub const State = struct { n: i64 = 0 };
    pub fn add(s: *State, _: void, _: Ctx) !void {
        s.n += 1;
    }
    pub fn addN(s: *State, k: usize) void {
        s.n += @intCast(k);
    }
    pub fn remove(s: *State, _: void, _: Ctx) !void {
        s.n -= 1;
    }
    pub fn merge(d: *State, s: *const State, _: Ctx) !void {
        d.n += s.n;
    }
    pub fn finish(s: *const State, _: Ctx) !Value {
        return .{ .int = s.n };
    }
};

pub const CountIf = struct {
    pub const In = bool;
    pub const State = struct { n: i64 = 0 };
    pub fn add(s: *State, x: bool, _: Ctx) !void {
        s.n += @intFromBool(x);
    }
    pub fn remove(s: *State, x: bool, _: Ctx) !void {
        s.n -= @intFromBool(x);
    }
    pub fn merge(d: *State, s: *const State, _: Ctx) !void {
        d.n += s.n;
    }
    pub fn finish(s: *const State, _: Ctx) !Value {
        return .{ .int = s.n };
    }
};

pub const SumInt = struct {
    pub const In = i64;
    pub const State = struct { n: i64 = 0, s: i128 align(8) = 0 };
    pub fn add(s: *State, x: i64, _: Ctx) !void {
        s.s += x;
        s.n += 1;
    }
    pub fn remove(s: *State, x: i64, _: Ctx) !void {
        s.s -= x;
        s.n -= 1;
    }
    pub fn addAll(s: *State, xs: []const i64, _: Ctx) !void {
        var t: i128 = 0;
        for (xs) |x| t += x;
        s.s += t;
        s.n += @intCast(xs.len);
    }
    pub fn merge(d: *State, s: *const State, _: Ctx) !void {
        d.s = std.math.add(i128, d.s, s.s) catch return error.IntOverflow;
        d.n += s.n;
    }
    pub fn finish(s: *const State, _: Ctx) !Value {
        if (s.n == 0) return .null;
        return .{ .int = std.math.cast(i64, s.s) orelse return error.IntOverflow };
    }
};

pub const SumDecimal = struct {
    pub const In = Decimal;
    pub const fold_expr = true;
    pub const State = struct { n: i64 = 0, s: i256 align(8) = 0 };
    pub fn add(s: *State, x: Decimal, cx: Ctx) !void {
        try eval.addAtScale(&s.s, x, cx.out.scale);
        s.n += 1;
    }
    pub fn remove(s: *State, x: Decimal, cx: Ctx) !void {
        const u = (eval.rescaleTo(x, cx.out.scale) orelse return eval.decimalOverflow()).unscaled;
        s.s = std.math.sub(i256, s.s, u) catch return eval.decimalOverflow();
        s.n -= 1;
    }
    pub fn merge(d: *State, s: *const State, _: Ctx) !void {
        d.s = std.math.add(i256, d.s, s.s) catch return eval.decimalOverflow();
        d.n += s.n;
    }
    pub fn finish(s: *const State, cx: Ctx) !Value {
        if (s.n == 0) return .null;
        return .{ .decimal = eval.wideSumDecimal(s.s, cx.out.scale) orelse return eval.decimalOverflow() };
    }
};

pub const SumFloat = struct {
    pub const In = f64;
    pub const State = struct { n: i64 = 0, s: f64 = 0 };
    pub fn add(s: *State, x: f64, _: Ctx) !void {
        s.s += x;
        s.n += 1;
    }
    pub fn addAll(s: *State, xs: []const f64, _: Ctx) !void {
        s.s += simd.sumF(xs);
        s.n += @intCast(xs.len);
    }
    pub fn merge(d: *State, s: *const State, _: Ctx) !void {
        d.s += s.s;
        d.n += s.n;
    }
    pub fn finish(s: *const State, _: Ctx) !Value {
        return if (s.n == 0) .null else .{ .float = s.s };
    }
};

pub const AvgInt = struct {
    pub const In = i64;
    pub const State = SumInt.State;
    pub const add = SumInt.add;
    pub const addAll = SumInt.addAll;
    pub const remove = SumInt.remove;
    pub const merge = SumInt.merge;
    pub fn finish(s: *const State, _: Ctx) !Value {
        if (s.n == 0) return .null;
        return .{ .float = @as(f64, @floatFromInt(s.s)) / @as(f64, @floatFromInt(s.n)) };
    }
};

pub const AvgFloat = struct {
    pub const In = f64;
    pub const State = SumFloat.State;
    pub const add = SumFloat.add;
    pub const addAll = SumFloat.addAll;
    pub const merge = SumFloat.merge;
    pub fn finish(s: *const State, _: Ctx) !Value {
        if (s.n == 0) return .null;
        return .{ .float = s.s / @as(f64, @floatFromInt(s.n)) };
    }
};

/// Exact at the argument's scale (`cx.ty.scale`); a value of wider scale, which a
/// per-value scale allows, is added as a float rather than rounded.
pub const AvgDecimal = struct {
    pub const In = Decimal;
    pub const State = struct { n: i64 = 0, s: i256 align(8) = 0, f: f64 = 0 };
    pub fn add(s: *State, x: Decimal, cx: Ctx) !void {
        s.n += 1;
        if (x.scale > cx.ty.scale) {
            s.f += x.toF64();
            return;
        }
        const r = eval.rescaleTo(x, cx.ty.scale) orelse {
            s.f += x.toF64();
            return;
        };
        s.s += r.unscaled;
    }
    pub fn merge(d: *State, s: *const State, _: Ctx) !void {
        d.s += s.s;
        d.f += s.f;
        d.n += s.n;
    }
    pub fn finish(s: *const State, cx: Ctx) !Value {
        if (s.n == 0) return .null;
        const whole = @as(f64, @floatFromInt(s.s)) / std.math.pow(f64, 10, @floatFromInt(cx.ty.scale));
        return .{ .float = (whole + s.f) / @as(f64, @floatFromInt(s.n)) };
    }
};

/// The order MIN and MAX keep, GROUP BY and window alike: NaN above every number,
/// decimals exactly across scales, text by bytes, and across kinds `compareValues`'s.
pub fn order(comptime T: type, a: T, b: T) std.math.Order {
    return switch (T) {
        Value => eval.compareValues(a, b) orelse .eq,
        f64 => eval.orderF64(a, b),
        Decimal => a.order(b),
        bool => std.math.order(@intFromBool(a), @intFromBool(b)),
        []const u8 => std.mem.order(u8, a, b),
        else => std.math.order(a, b),
    };
}

/// A physical value as the declared type `out` tags it: a TIMESTAMP and an INT
/// share i64, a BYTES and a STRING their bytes.
pub fn toValue(comptime T: type, x: T, out: types.Type) Value {
    return switch (T) {
        Value => x,
        i64 => switch (out.kind) {
            .time => .{ .time = x },
            .timestamp => .{ .timestamp = x },
            else => .{ .int = x },
        },
        i32 => .{ .date = x },
        f64 => .{ .float = x },
        Decimal => .{ .decimal = x },
        bool => .{ .bool = x },
        []const u8 => if (out.kind == .bytes) .{ .bytes = x } else .{ .string = x },
        else => unreachable,
    };
}

/// MIN (`want` .lt) or MAX (.gt) over one physical type; the result takes its tag
/// from the declared type, so a TIMESTAMP and an INT share the i64 state.
pub fn Extreme(comptime T: type, comptime want: std.math.Order) type {
    return struct {
        pub const In = T;
        pub const needs_alloc = T == []const u8;
        pub const State = struct { has: bool = false, v: T = undefined };
        pub fn add(s: *State, x: T, cx: Ctx) !void {
            if (s.has and order(T, x, s.v) != want) return;
            s.v = if (T == []const u8) try cx.alloc.dupe(u8, x) else x;
            s.has = true;
        }
        pub fn merge(d: *State, s: *const State, cx: Ctx) !void {
            if (s.has) try add(d, s.v, cx);
        }
        pub fn own(s: *State, cx: Ctx) !void {
            if (T == []const u8 and s.has) s.v = try cx.alloc.dupe(u8, s.v);
        }
        pub fn finish(s: *const State, cx: Ctx) !Value {
            return if (s.has) toValue(T, s.v, cx.out) else .null;
        }
        pub const addAll = if (T == f64) addAllF else {};
        fn addAllF(s: *State, xs: []const f64, cx: Ctx) !void {
            if (xs.len > 0) try add(s, if (want == .lt) simd.minF(xs) else simd.maxF(xs), cx);
        }
    };
}

/// MIN or MAX over values of no single physical type, in `compareValues` order.
pub fn ExtremeValue(comptime want: std.math.Order) type {
    return struct {
        pub const In = Value;
        pub const needs_alloc = true;
        pub const State = struct { v: Value = .null };
        pub fn add(s: *State, x: Value, cx: Ctx) !void {
            if (!s.v.isNull() and order(Value, x, s.v) != want) return;
            s.v = try dupeValue(cx.alloc, x);
        }
        pub fn merge(d: *State, s: *const State, cx: Ctx) !void {
            if (!s.v.isNull()) try add(d, s.v, cx);
        }
        pub fn own(s: *State, cx: Ctx) !void {
            s.v = try dupeValue(cx.alloc, s.v);
        }
        pub fn finish(s: *const State, _: Ctx) !Value {
            return s.v;
        }
    };
}

/// The values kept whole and selected from at the end; with an even count the mean
/// of the two middle values, as Postgres's percentile_cont(0.5) has it.
pub const Median = struct {
    pub const In = f64;
    pub const needs_alloc = true;
    const List = std.array_list.Managed(f64);
    pub const State = struct { vals: ?*List = null };
    pub fn add(s: *State, x: f64, cx: Ctx) !void {
        const l = s.vals orelse blk: {
            const p = try cx.alloc.create(List);
            p.* = List.init(cx.alloc);
            s.vals = p;
            break :blk p;
        };
        try l.append(x);
    }
    pub fn merge(d: *State, s: *const State, cx: Ctx) !void {
        if (s.vals) |l| for (l.items) |x| try add(d, x, cx);
    }
    pub fn own(s: *State, cx: Ctx) !void {
        const src = s.vals orelse return;
        const l = try cx.alloc.create(List);
        l.* = try .initCapacity(cx.alloc, src.items.len);
        l.appendSliceAssumeCapacity(src.items);
        s.vals = l;
    }
    pub fn finish(s: *const State, _: Ctx) !Value {
        const l = s.vals orelse return .null;
        const xs = l.items;
        if (xs.len == 0) return .null;
        const mid = xs.len / 2;
        const hi = selectNth(xs, mid);
        if (xs.len % 2 == 1) return .{ .float = hi };
        var lo = xs[0];
        for (xs[1..mid]) |x| lo = @max(lo, x);
        return .{ .float = (lo + hi) / 2 };
    }
};

/// BOOL_AND (`all`) or BOOL_OR; NULL over no values.
pub fn BoolFold(comptime all: bool) type {
    return struct {
        pub const In = bool;
        pub const State = struct { n: i64 = 0, t: i64 = 0 };
        pub fn add(s: *State, x: bool, _: Ctx) !void {
            s.n += 1;
            s.t += @intFromBool(x);
        }
        pub fn remove(s: *State, x: bool, _: Ctx) !void {
            s.n -= 1;
            s.t -= @intFromBool(x);
        }
        pub fn merge(d: *State, s: *const State, _: Ctx) !void {
            d.n += s.n;
            d.t += s.t;
        }
        pub fn finish(s: *const State, _: Ctx) !Value {
            if (s.n == 0) return .null;
            return .{ .bool = if (all) s.t == s.n else s.t > 0 };
        }
    };
}

pub const BitOp = enum { @"and", @"or", xor };

pub fn Bits(comptime op: BitOp) type {
    return struct {
        pub const In = i64;
        pub const State = struct { n: i64 = 0, v: i64 = 0 };
        fn fold(a: i64, b: i64) i64 {
            return switch (op) {
                .@"and" => a & b,
                .@"or" => a | b,
                .xor => a ^ b,
            };
        }
        pub fn add(s: *State, x: i64, _: Ctx) !void {
            s.v = if (s.n == 0) x else fold(s.v, x);
            s.n += 1;
        }
        pub const remove = if (op == .xor) removeXor else {};
        fn removeXor(s: *State, x: i64, _: Ctx) !void {
            s.v ^= x;
            s.n -= 1;
        }
        pub fn merge(d: *State, s: *const State, _: Ctx) !void {
            if (s.n == 0) return;
            d.v = if (d.n == 0) s.v else fold(d.v, s.v);
            d.n += s.n;
        }
        pub fn finish(s: *const State, _: Ctx) !Value {
            return if (s.n == 0) .null else .{ .int = s.v };
        }
    };
}

/// A group's count, mean and sum of squared deviations.
pub const Moments = struct {
    n: f64 = 0,
    mean: f64 = 0,
    m2: f64 = 0,

    /// Chan et al.'s pairwise merge of two groups' moments.
    pub fn merge(a: Moments, b: Moments) Moments {
        if (a.n == 0) return b;
        if (b.n == 0) return a;
        const n = a.n + b.n;
        const d = b.mean - a.mean;
        return .{ .n = n, .mean = a.mean + d * b.n / n, .m2 = a.m2 + b.m2 + d * d * a.n * b.n / n };
    }
};

/// A variance or standard deviation (`func`) of `n` values with squared deviations
/// `m2`: NULL below two values for the sample statistics and below one for the
/// population ones. A negative `m2` is rounding and reads 0; a NaN stays NaN.
pub fn spreadOf(func: ast.AggFunc, n: i64, m2: f64) Value {
    const samp = func == .var_samp or func == .stddev_samp;
    if (n < @as(i64, if (samp) 2 else 1)) return .null;
    const variance = (if (m2 < 0) 0 else m2) / @as(f64, @floatFromInt(if (samp) n - 1 else n));
    return .{ .float = switch (func) {
        .stddev_samp, .stddev_pop => @sqrt(variance),
        else => variance,
    } };
}

/// Exact sums of x and x² over INT values, so a variance over ints is exact (a run
/// of equal values reads 0 however many rows passed) until they leave i128, when
/// the float moments stand in.
pub const IntSquares = struct {
    s1: i128 align(8) = 0,
    s2: i128 align(8) = 0,
    overflow: bool = false,

    pub fn add(q: *IntSquares, v: i64, sub: bool) void {
        const x: i128 = v;
        const sq = std.math.mul(i128, x, x) catch {
            q.overflow = true;
            return;
        };
        q.s1 = std.math.add(i128, q.s1, if (sub) -x else x) catch blk: {
            q.overflow = true;
            break :blk q.s1;
        };
        q.s2 = std.math.add(i128, q.s2, if (sub) -sq else sq) catch blk: {
            q.overflow = true;
            break :blk q.s2;
        };
    }

    pub fn merge(d: *IntSquares, s: IntSquares) void {
        d.overflow = d.overflow or s.overflow;
        d.s1 = std.math.add(i128, d.s1, s.s1) catch blk: {
            d.overflow = true;
            break :blk d.s1;
        };
        d.s2 = std.math.add(i128, d.s2, s.s2) catch blk: {
            d.overflow = true;
            break :blk d.s2;
        };
    }

    /// The squared deviations of `n` values, or null once the sums overflowed.
    pub fn m2(q: IntSquares, n: i64) ?f64 {
        if (q.overflow or n <= 0) return null;
        const nn: i128 = n;
        const a = std.math.mul(i128, nn, q.s2) catch return null;
        const b = std.math.mul(i128, q.s1, q.s1) catch return null;
        return @as(f64, @floatFromInt(a - b)) / @as(f64, @floatFromInt(nn));
    }
};

/// VAR_SAMP, VAR_POP, STDDEV_SAMP or STDDEV_POP by Welford's update, and over ints
/// (`exact`) by `IntSquares` while they hold.
pub fn Spread(comptime func: ast.AggFunc, comptime exact: bool) type {
    return struct {
        pub const In = if (exact) i64 else f64;
        pub const State = struct { n: i64 = 0, mean: f64 = 0, m2: f64 = 0, sq: if (exact) IntSquares else void = if (exact) .{} else {} };
        pub fn add(s: *State, v: In, _: Ctx) !void {
            const x: f64 = if (exact) @floatFromInt(v) else v;
            if (exact) s.sq.add(v, false);
            s.n += 1;
            const delta = x - s.mean;
            s.mean += delta / @as(f64, @floatFromInt(s.n));
            s.m2 += delta * (x - s.mean);
        }
        pub fn merge(d: *State, s: *const State, _: Ctx) !void {
            if (s.n == 0) return;
            const m = Moments.merge(
                .{ .n = @floatFromInt(d.n), .mean = d.mean, .m2 = d.m2 },
                .{ .n = @floatFromInt(s.n), .mean = s.mean, .m2 = s.m2 },
            );
            d.mean = m.mean;
            d.m2 = m.m2;
            d.n += s.n;
            if (exact) d.sq.merge(s.sq);
        }
        pub fn finish(s: *const State, _: Ctx) !Value {
            const m2 = if (exact) (s.sq.m2(s.n) orelse s.m2) else s.m2;
            return spreadOf(func, s.n, m2);
        }
    };
}

pub fn dupeValue(alloc: std.mem.Allocator, v: Value) !Value {
    return switch (v) {
        .string => |s| .{ .string = try alloc.dupe(u8, s) },
        .bytes => |s| .{ .bytes = try alloc.dupe(u8, s) },
        else => v,
    };
}

/// Quickselect: `xs[k]` becomes the k-th smallest, smaller before, larger after.
/// Median-of-three pivot, Hoare partition; expected O(n). NaN orders above every
/// number, as `eval.orderF64` has it, so the NaNs are moved to the end first.
pub fn selectNth(xs_all: []f64, k: usize) f64 {
    var m: usize = 0;
    for (xs_all) |x| {
        if (std.math.isNan(x)) continue;
        xs_all[m] = x;
        m += 1;
    }
    @memset(xs_all[m..], std.math.nan(f64));
    if (k >= m) return std.math.nan(f64);
    const xs = xs_all[0..m];
    var lo: usize = 0;
    var hi: usize = xs.len - 1;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (xs[mid] < xs[lo]) std.mem.swap(f64, &xs[mid], &xs[lo]);
        if (xs[hi] < xs[lo]) std.mem.swap(f64, &xs[hi], &xs[lo]);
        if (xs[hi] < xs[mid]) std.mem.swap(f64, &xs[hi], &xs[mid]);
        const p = xs[mid];
        var i = lo;
        var j = hi;
        while (i <= j) {
            while (xs[i] < p) i += 1;
            while (p < xs[j]) j -= 1;
            if (i <= j) {
                std.mem.swap(f64, &xs[i], &xs[j]);
                i += 1;
                if (j == 0) break;
                j -= 1;
            }
        }
        if (k <= j) {
            hi = j;
        } else if (k >= i) {
            lo = i;
        } else {
            return xs[k];
        }
    }
    return xs[k];
}
