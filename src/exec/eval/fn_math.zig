//! The elementary math functions: logarithms and exponentials, trigonometry,
//! integer number theory and the float class tests. An argument outside a
//! function's domain answers NULL, as `sqrt(-1)` does; a float that overflows is
//! IEEE `inf`, as `power` gives; an integer that overflows is an error, as `abs`
//! raises.

const std = @import("std");
const Cx = @import("scalar.zig").Cx;
const EvalError = @import("../eval.zig").EvalError;

/// A real function of one number as a FLOAT, NULL where `f` answers null.
fn Real(comptime f: fn (f64) ?f64) type {
    return struct {
        pub const args = .{.num};
        pub const signature = "(x)";
        pub const may_null = true;
        pub fn apply(_: Cx, x: f64) EvalError!?f64 {
            return f(x);
        }
    };
}

/// A real function of two numbers as a FLOAT, NULL where `f` answers null.
fn Real2(comptime sig: []const u8, comptime f: fn (f64, f64) ?f64) type {
    return struct {
        pub const args = .{ .num, .num };
        pub const signature = sig;
        pub const may_null = true;
        pub fn apply(_: Cx, a: f64, b: f64) EvalError!?f64 {
            return f(a, b);
        }
    };
}

fn positive(comptime f: fn (f64) f64) fn (f64) ?f64 {
    return struct {
        fn g(x: f64) ?f64 {
            return if (x > 0) f(x) else null;
        }
    }.g;
}

fn total(comptime f: fn (f64) f64) fn (f64) ?f64 {
    return struct {
        fn g(x: f64) ?f64 {
            return f(x);
        }
    }.g;
}

fn unit(comptime f: fn (f64) f64) fn (f64) ?f64 {
    return struct {
        fn g(x: f64) ?f64 {
            return if (x >= -1 and x <= 1) f(x) else null;
        }
    }.g;
}

fn exp(x: f64) f64 {
    return @exp(x);
}
fn ln(x: f64) f64 {
    return @log(x);
}
fn log10(x: f64) f64 {
    return @log10(x);
}
fn log2(x: f64) f64 {
    return @log2(x);
}
fn cbrt(x: f64) f64 {
    return std.math.cbrt(x);
}
fn sin(x: f64) f64 {
    return @sin(x);
}
fn cos(x: f64) f64 {
    return @cos(x);
}
fn tan(x: f64) f64 {
    return @tan(x);
}
fn asin(x: f64) f64 {
    return std.math.asin(x);
}
fn acos(x: f64) f64 {
    return std.math.acos(x);
}
fn atan(x: f64) f64 {
    return std.math.atan(x);
}
fn degrees(x: f64) f64 {
    return x * (180.0 / std.math.pi);
}
fn radians(x: f64) f64 {
    return x * (std.math.pi / 180.0);
}

fn cot(x: f64) ?f64 {
    const t = @tan(x);
    return if (t == 0) null else 1 / t;
}

/// `log(b, x)`: NULL for a base that is not positive or is 1, or an `x` not positive.
fn logBase(b: f64, x: f64) ?f64 {
    if (b <= 0 or b == 1 or x <= 0) return null;
    return @log(x) / @log(b);
}

fn atan2(y: f64, x: f64) ?f64 {
    return std.math.atan2(y, x);
}

pub const Exp = Real(total(exp));
pub const Ln = Real(positive(ln));
pub const Log10 = Real(positive(log10));
pub const Log2 = Real(positive(log2));
pub const Log = Real2("(base, x)", logBase);
pub const Cbrt = Real(total(cbrt));
pub const Sin = Real(total(sin));
pub const Cos = Real(total(cos));
pub const Tan = Real(total(tan));
pub const Cot = Real(cot);
pub const Asin = Real(unit(asin));
pub const Acos = Real(unit(acos));
pub const Atan = Real(total(atan));
pub const Atan2 = Real2("(y, x)", atan2);
pub const Degrees = Real(total(degrees));
pub const Radians = Real(total(radians));

pub const Pi = struct {
    pub const args = .{};
    pub const signature = "()";
    pub fn apply(_: Cx) EvalError!f64 {
        return std.math.pi;
    }
};

/// `isnan`, `isinf` and `isfinite` over a number, by `class`.
pub fn Class(comptime class: enum { nan, inf, finite }) type {
    return struct {
        pub const args = .{.num};
        pub const signature = "(x)";
        pub fn apply(_: Cx, x: f64) EvalError!bool {
            return switch (class) {
                .nan => std.math.isNan(x),
                .inf => std.math.isInf(x),
                .finite => std.math.isFinite(x),
            };
        }
    };
}

/// The greatest common divisor, never negative; `gcd(0, 0)` is 0. One that is
/// 2^63 overflows.
pub const Gcd = struct {
    pub const args = .{ .exact_int, .exact_int };
    pub const signature = "(a, b)";
    pub fn apply(_: Cx, a: i64, b: i64) EvalError!i64 {
        return gcd(a, b);
    }
};

fn gcd(a: i64, b: i64) EvalError!i64 {
    var x: u64 = @abs(a);
    var y: u64 = @abs(b);
    while (y != 0) {
        const r = x % y;
        x = y;
        y = r;
    }
    return std.math.cast(i64, x) orelse error.IntOverflow;
}

/// The least common multiple, never negative; 0 when either is 0.
pub const Lcm = struct {
    pub const args = .{ .exact_int, .exact_int };
    pub const signature = "(a, b)";
    pub fn apply(_: Cx, a: i64, b: i64) EvalError!i64 {
        if (a == 0 or b == 0) return 0;
        const g = try gcd(a, b);
        const m = std.math.mul(i64, @divExact(a, g), b) catch return error.IntOverflow;
        return std.math.cast(i64, @abs(m)) orelse error.IntOverflow;
    }
};

/// `n!`: NULL for a negative `n`, an overflow past 20!.
pub const Factorial = struct {
    pub const args = .{.exact_int};
    pub const signature = "(n)";
    pub const may_null = true;
    pub fn apply(_: Cx, n: i64) EvalError!?i64 {
        if (n < 0) return null;
        if (n > 20) return error.IntOverflow;
        var f: i64 = 1;
        var k: i64 = 2;
        while (k <= n) : (k += 1) f *= k;
        return f;
    }
};
