//! DECIMAL in expressions: result typing of an operator over decimal operands, the
//! checked arithmetic an `ast.BinOp` names, comparison bounds, reading a `Value` as
//! a decimal, the messages a failed decimal step gives, and the comptime lane kernels
//! the vectorized evaluator runs decimal arithmetic and comparison through. The
//! number itself lives in `base/decimal.zig`.

const std = @import("std");
const ast = @import("../lang/ast.zig");
const types = @import("../lang/types.zig");
const decimal = @import("../base/decimal.zig");
const Decimal = decimal.Decimal;
const fitsPrecision = decimal.fitsPrecision;
const roundScaleDown = decimal.roundScaleDown;
const parse = decimal.parse;
const writeDecimal = decimal.writeDecimal;
const rescaleTo = decimal.rescaleTo;
const pow10_i256 = decimal.pow10_i256;
const pow10_i128 = decimal.pow10_i128;
const max_digits = decimal.max_digits;
const floatToDecimal = decimal.floatToDecimal;
const Dec64 = decimal.Dec64;
const Value = @import("value.zig").Value;
const Type = types.Type;
const failWith = @import("eval/support.zig").failWith;
const explain = @import("eval/support.zig").explain;
const msgfmt = @import("../base/msgfmt.zig");
const trim = @import("eval/support.zig").trim;
const fmt_bound = @import("eval/format.zig").fmt_bound;

pub fn decimalOverflow() error{IntOverflow} {
    return @errorCast(failWith(error.IntOverflow, "decimal overflow: more than 38 digits", .{}));
}

/// A typed writer's `DecimalDropsDigits` for column `name` of type `ty`, naming the
/// CAST that rounds its values on purpose.
pub fn dropsDigitsFailure(name: []const u8, ty: Type) anyerror {
    @branchHint(.cold);
    var buf: [480]u8 = undefined;
    const p: u8 = if (ty.precision == 0) max_digits else ty.precision;
    const msg = if (ty.kind == .decimal)
        msgfmt.clipPrint(&buf, "column `{s}` is DECIMAL({d},{d}) and holds a value with more decimal places, which writing would round away; CAST it to DECIMAL({d},{d}) to round on purpose", .{ name, p, ty.scale, p, ty.scale })
    else
        msgfmt.clipPrint(&buf, "column `{s}` holds a decimal with more places than its type's scale, which writing would round away; CAST it to that scale to round on purpose", .{name});
    return explain(error.DecimalDropsDigits, msg);
}

/// The scale `round(decimal, digits)` answers in: the digits when they are a
/// literal (as DuckDB types it; a negative one is scale 0), else the input's own.
pub fn roundOutScale(c: ast.Expr.Call, in_scale: u8) u8 {
    if (c.args.len < 2) return 0;
    const d = c.args[1];
    if (d.* == .unary and d.unary.op == .neg and d.unary.e.* == .int_lit) return 0;
    if (d.* != .int_lit) return in_scale;
    return @intCast(std.math.clamp(d.int_lit, 0, in_scale));
}

pub fn wholeType(t: Type) Type {
    return Type.decimal(@min(38, (t.precision -| t.scale) + 1), 0);
}

pub fn asDecimal(v: Value) ?Decimal {
    return switch (v) {
        .decimal => |d| d,
        .int => |x| .{ .unscaled = x, .scale = 0 },
        else => null,
    };
}

/// The decimal a value reads as before it meets a target's scale and precision.
pub fn asDecimalValue(v: Value) ?Decimal {
    return switch (v) {
        .decimal => |x| x,
        .int => |x| .{ .unscaled = x, .scale = 0 },
        .bool => |x| .{ .unscaled = if (x) 1 else 0, .scale = 0 },
        .float => |x| floatToDecimal(x),
        .string, .bytes => |str| parse(trim(str)),
        else => null,
    };
}

/// The exact DECIMAL type of `op`, or null when it stays float. `+ - %` take the
/// wider scale, `*` the summed scale; an INT operand is a DECIMAL(19,0). A
/// quotient's scale is the operands' wider one but at least 16 while its whole
/// digits leave room; each value carries the finer scale `decimalDiv` picks.
pub fn decimalArithType(op: ast.BinOp, lt: Type, rt: Type) ?Type {
    switch (op) {
        .add, .sub, .mul, .div, .mod => {},
        else => return null,
    }
    if (lt.kind == .float or rt.kind == .float) return null;
    if (lt.kind != .decimal and rt.kind != .decimal) return null;
    const lp: u16 = if (lt.kind != .decimal) 19 else if (lt.precision == 0) 38 else lt.precision;
    const rp: u16 = if (rt.kind != .decimal) 19 else if (rt.precision == 0) 38 else rt.precision;
    const ls: u16 = if (lt.kind == .decimal) lt.scale else 0;
    const rs: u16 = if (rt.kind == .decimal) rt.scale else 0;
    const whole_digits = (lp -| ls) + rs;
    const scale: u16 = switch (op) {
        .mul => ls + rs,
        .div => @max(ls, rs, @min(div_min_digits, max_digits -| whole_digits)),
        else => @max(ls, rs),
    };
    const prec: u16 = switch (op) {
        .mul => lp + rp,
        .div => whole_digits + scale,
        .mod => @max(1, @min(lp -| ls, rp -| rs) + scale),
        else => @max(lp -| ls, rp -| rs) + scale + 1,
    };
    return Type.decimal(@intCast(@min(38, prec)), @intCast(@min(38, scale)));
}

/// `*` multiplies in i256 so a product past scale 38 can round back to it before
/// the 38-digit check; any result wider than 38 digits is a decimal overflow.
pub fn decimalOp(op: ast.BinOp, a: Decimal, b: Decimal) ?error{ IntOverflow, DivByZero }!Decimal {
    const r: Decimal = switch (op) {
        .mul => blk: {
            const s = @as(u16, a.scale) + b.scale;
            if (s <= max_digits) if (std.math.mul(i128, a.unscaled, b.unscaled)) |u| break :blk .{ .unscaled = u, .scale = @intCast(s) } else |_| {};
            const p = @as(i256, a.unscaled) * b.unscaled;
            const drop = s -| max_digits;
            const q = roundScaleDown(p, drop);
            break :blk .{ .unscaled = std.math.cast(i128, q) orelse return decimalOverflow(), .scale = @intCast(s - drop) };
        },
        .add, .sub => blk: {
            const s = @max(a.scale, b.scale);
            const x = rescaleTo(a, s) orelse return decimalOverflow();
            const y = rescaleTo(b, s) orelse return decimalOverflow();
            const u = if (op == .add) std.math.add(i128, x.unscaled, y.unscaled) else std.math.sub(i128, x.unscaled, y.unscaled);
            break :blk .{ .unscaled = u catch return decimalOverflow(), .scale = s };
        },
        .div => try decimalDiv(a, b),
        .mod => blk: {
            const s = @max(a.scale, b.scale);
            const x = rescaleTo(a, s) orelse return decimalOverflow();
            const y = rescaleTo(b, s) orelse return decimalOverflow();
            if (y.unscaled == 0) return error.DivByZero;
            break :blk .{ .unscaled = @rem(x.unscaled, y.unscaled), .scale = s };
        },
        else => return null,
    };
    if (!fitsPrecision(r.unscaled, max_digits)) return decimalOverflow();
    return r;
}

pub const Bound = struct { op: ast.BinOp, bound: i128 };

/// A comparison against constant `d` at a column's scale `sc` (`x > 2.5` over INT
/// is `x > 2`): the operator and whole bound that decide the same rows, with the
/// constant on the right. `never` is a bound no element falls below. Null when
/// the bound passes i128.
pub fn cmpBound(op: ast.BinOp, flip: bool, d: Decimal, sc: u8, never: i128) ?Bound {
    var fl: i128 = undefined;
    var exact = true;
    if (d.scale <= sc) {
        fl = std.math.mul(i128, d.unscaled, pow10_i128[sc - d.scale]) catch return null;
    } else {
        if (d.scale - sc >= pow10_i128.len) return null;
        const p = pow10_i128[d.scale - sc];
        fl = @divFloor(d.unscaled, p);
        exact = @mod(d.unscaled, p) == 0;
    }
    const ce = if (exact) fl else fl + 1;
    const o: ast.BinOp = if (!flip) op else switch (op) {
        .lt => .gt,
        .le => .ge,
        .gt => .lt,
        .ge => .le,
        else => op,
    };
    return switch (o) {
        .gt => .{ .op = .gt, .bound = fl },
        .ge => .{ .op = .ge, .bound = ce },
        .lt => .{ .op = .lt, .bound = ce },
        .le => .{ .op = .le, .bound = fl },
        .eq => if (exact) .{ .op = .eq, .bound = fl } else .{ .op = .lt, .bound = never },
        .ne => if (exact) .{ .op = .ne, .bound = fl } else .{ .op = .ge, .bound = never },
        else => null,
    };
}

pub fn formatDecimal(arena: std.mem.Allocator, unscaled: i128, scale: u8) ![]const u8 {
    var buf: [fmt_bound]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    writeDecimal(&w, unscaled, scale) catch unreachable;
    return arena.dupe(u8, w.buffered());
}

/// How a lane operand is held: an INT column, a wide or narrow decimal column, or
/// one constant.
pub const Side = enum { int, dec, d64, one };

/// One operand of a lane kernel: its column's elements (`ints`, `decs` or `narrow`
/// at `nscale`), the scale it is read at, the factor `f` that brings it to the
/// result scale, and, for a constant, `c`, its unscaled value already brought there.
pub const LaneSrc = struct { ints: []const i64 = &.{}, decs: []const Decimal = &.{}, narrow: []const Dec64 = &.{}, nscale: u8 = 0, scale: u8, f: i128, c: i128 };

/// The result scale `s` of `op` over operands read at scales `ls` and `rs`, and
/// the factors that raise each to it; null when the lane cannot hold it (past 38
/// digits, or a raise of more than 18 places).
pub fn lanePlan(op: ast.BinOp, ls: u8, rs: u8) ?struct { s: u8, fl: i128, fr: i128 } {
    const s = resultScale(op, ls, rs);
    if (s > max_digits or (op != .mul and (s - ls > 18 or s - rs > 18))) return null;
    const fl: i128 = if (op == .mul) 1 else pow10_i128[@intCast(s - ls)];
    const fr: i128 = if (op == .mul) 1 else pow10_i128[@intCast(s - rs)];
    return .{ .s = @intCast(s), .fl = fl, .fr = fr };
}

/// The scale `op` answers in over operands at scales `ls` and `rs`: their sum for
/// a product, else the wider; it may pass 38.
pub inline fn resultScale(op: ast.BinOp, ls: u8, rs: u8) u16 {
    return if (op == .mul) @as(u16, ls) + rs else @max(ls, rs);
}

/// Whether a product's two sides can both fit i64 throughout (no decimal column),
/// so `narrowLane` may pick the widening-multiply loop.
pub inline fn narrowable(comptime op: ast.BinOp, comptime lk: Side, comptime rk: Side) bool {
    return op == .mul and lk != .dec and rk != .dec;
}

/// Whether a `narrowable` product's constants fit i64, so each row is one widening
/// multiply that cannot pass 38 digits.
pub inline fn narrowLane(lc: i128, rc: i128) bool {
    return std.math.cast(i64, lc) != null and std.math.cast(i64, rc) != null;
}

/// Each valid row of `op` over lane operands `ln.a` and `ln.b` handed to
/// `sink.put(row, value)` at scale `ln.s`, in error set `E`. A row an operand
/// cannot give at its lane's scale, or whose i128 result is not settled, takes
/// `ln.at(op, row)`, the full `decimalOp`. `narrow`: both sides fit i64
/// (`narrowLane`). Otherwise a product whose two elements each fit i64, as every
/// DECIMAL of 18 digits or fewer does, takes that multiply too, row by row; only a
/// wider pair pays the overflow-checked 128-bit one.
pub fn laneLoop(comptime E: type, comptime op: ast.BinOp, comptime lk: Side, comptime rk: Side, comptime narrow: bool, ln: anytype, sink: anytype, valid: anytype, any: bool) E!void {
    for (0..valid.len) |i| {
        if (any and !valid.get(i)) continue;
        if (laneRow(op, lk, rk, narrow, ln.a, ln.b, i)) |u| {
            try sink.put(i, .{ .unscaled = u, .scale = ln.s });
        } else {
            @branchHint(.cold);
            try rowPut(E, op, &ln, sink, i);
        }
    }
}

/// A row the lane cannot settle, out of line and shared by every lane of a sink.
noinline fn rowPut(comptime E: type, op: ast.BinOp, ln: anytype, sink: anytype, i: usize) E!void {
    try sink.put(i, try ln.at(op, i));
}

/// Row `i` of `op` over lane operands `la` and `lb` at the lane's scale; null when
/// an operand is off its lane's scale or the i128 result is not settled.
inline fn laneRow(comptime op: ast.BinOp, comptime lk: Side, comptime rk: Side, comptime narrow: bool, la: LaneSrc, lb: LaneSrc, i: usize) ?i128 {
    const limit: u128 = @intCast(pow10_i128[max_digits]);
    const a = laneAt(op, lk, la, i) orelse return null;
    const b = laneAt(op, rk, lb, i) orelse return null;
    var small = narrow;
    const u: i128 = @as(?i128, switch (op) {
        .add => addOrNull(a, b),
        .sub => subOrNull(a, b),
        .mul => if (narrow) @as(i128, @as(i64, @intCast(a))) * @as(i64, @intCast(b)) else blk: {
            if (std.math.cast(i64, a)) |x| if (std.math.cast(i64, b)) |y| {
                small = true;
                break :blk @as(i128, x) * y;
            };
            break :blk mulOrNull(a, b);
        },
        .mod => if (b == 0 or b == -1) null else @rem(a, b),
        else => unreachable,
    }) orelse return null;
    return if (small or @abs(u) < limit) u else null;
}

inline fn laneAt(comptime op: ast.BinOp, comptime k: Side, src: LaneSrc, i: usize) ?i128 {
    return switch (k) {
        .one => src.c,
        .int => if (op == .mul) src.ints[i] else @as(i128, src.ints[i]) * src.f,
        .dec => blk: {
            const d = src.decs[i];
            if (d.scale != src.scale) break :blk null;
            break :blk if (op == .mul or src.f == 1) d.unscaled else mulOrNull(d.unscaled, src.f);
        },
        .d64 => blk: {
            if (src.nscale != src.scale) break :blk null;
            const u: i128 = src.narrow[i].int();
            break :blk if (op == .mul) u else u * src.f;
        },
    };
}

inline fn addOrNull(a: i128, b: i128) ?i128 {
    const r = @addWithOverflow(a, b);
    return if (r[1] == 0) r[0] else null;
}

inline fn subOrNull(a: i128, b: i128) ?i128 {
    const r = @subWithOverflow(a, b);
    return if (r[1] == 0) r[0] else null;
}

inline fn mulOrNull(a: i128, b: i128) ?i128 {
    const r = @mulWithOverflow(a, b);
    return if (r[1] == 0) r[0] else null;
}

/// `x` against constant `lit` (`flip`: the constant on the left) on a lane typed
/// at `scale`: there its unscaled value meets `bound` under `fast_op`, the
/// `cmpBound` pick; at any other scale it compares in full under `op`. `result`
/// turns an operator and an order into the answer.
pub inline fn cmpAtScale(x: Decimal, fast_op: ast.BinOp, bound: i128, scale: u8, op: ast.BinOp, flip: bool, lit: Decimal, comptime result: fn (ast.BinOp, std.math.Order) bool) bool {
    if (x.scale == scale) return result(fast_op, std.math.order(x.unscaled, bound));
    return cmpFull(x, op, flip, lit, result);
}

noinline fn cmpFull(x: Decimal, op: ast.BinOp, flip: bool, lit: Decimal, comptime result: fn (ast.BinOp, std.math.Order) bool) bool {
    @branchHint(.cold);
    return result(op, if (flip) lit.order(x) else x.order(lit));
}

/// Adds `d` to a DECIMAL SUM's wide total, at the sum's `scale`. The add needs no
/// check: each addend is an i128, so fewer than 2^63 of them (a state's i64 count)
/// stay under 2^191, far inside i256.
pub inline fn addAtScale(sum: *align(8) i256, d: Decimal, scale: u8) error{IntOverflow}!void {
    const u = if (d.scale == scale) d.unscaled else (rescaleTo(d, scale) orelse return decimalOverflow()).unscaled;
    sum.* += u;
}

const div_min_digits = 16;

/// `a / b` as PostgreSQL's numeric division answers it (`select_div_scale`): at
/// least 16 significant digits, judged from the operands' leading base-10000
/// digits, and no fewer scale digits than either operand, rounded half away from
/// zero. Past 38 digits in all, the scale gives way first.
pub fn decimalDiv(a: Decimal, b: Decimal) error{ IntOverflow, DivByZero }!Decimal {
    if (b.unscaled == 0) return error.DivByZero;
    const wa = pgWeight(a);
    const wb = pgWeight(b);
    var qweight = wa.weight - wb.weight;
    if (wa.first <= wb.first) qweight -= 1;
    const rscale: i32 = @max(div_min_digits - qweight * 4, a.scale, b.scale, 0);
    var scale: u8 = @intCast(@min(rscale, max_digits));
    while (true) {
        const q = quotientAt(a, b, scale);
        const excess = excessDigits(q);
        if (excess == 0) return .{ .unscaled = @intCast(q), .scale = scale };
        if (scale < excess) return decimalOverflow();
        scale -= excess;
    }
}

/// A decimal's PostgreSQL weight (the power of 10000 of its leading base-10000
/// digit) and that digit; a zero is weight 0, digit 0.
fn pgWeight(d: Decimal) struct { weight: i32, first: u256 } {
    const u: u256 = @abs(d.unscaled);
    if (u == 0) return .{ .weight = 0, .first = 0 };
    var n: i32 = 1;
    while (n < pow10_i256.len and u >= @as(u256, @intCast(pow10_i256[@intCast(n)]))) n += 1;
    const weight = @divFloor(n - 1 - @as(i32, d.scale), 4);
    const shift = weight * 4 + d.scale;
    const first = if (shift >= 0) u / @as(u256, @intCast(pow10_i256[@intCast(shift)])) else u * @as(u256, @intCast(pow10_i256[@intCast(-shift)]));
    return .{ .weight = weight, .first = first };
}

/// `a / b` at `scale`, rounded half away from zero. i256 holds every case whose
/// dividend is raised by at most 38 places; only a wider shift pays for i512.
fn quotientAt(a: Decimal, b: Decimal, scale: u8) i512 {
    const e: i32 = @as(i32, scale) + b.scale - a.scale;
    if (e <= max_digits) return roundedQuotient(i256, a.unscaled, b.unscaled, e);
    return roundedQuotient(i512, a.unscaled, b.unscaled, e);
}

fn roundedQuotient(comptime T: type, x: i128, y: i128, e: i32) T {
    var n: T = x;
    var d: T = y;
    if (e >= 0) n *= pow10Of(T, @intCast(e)) else d *= pow10Of(T, @intCast(-e));
    const q = @divTrunc(n, d);
    const r = @rem(n, d);
    if (@abs(r) * 2 >= @abs(d)) return if ((n < 0) != (d < 0)) q - 1 else q + 1;
    return q;
}

fn pow10Of(comptime T: type, n: u16) T {
    if (n < pow10_i256.len) return pow10_i256[n];
    var r: T = pow10_i256[pow10_i256.len - 1];
    for (pow10_i256.len - 1..n) |_| r *= 10;
    return r;
}

/// How many digits `q` has past 38; 0 when it fits.
fn excessDigits(q: i512) u8 {
    var k: u8 = 0;
    const u = @abs(q);
    while (u >= @as(u512, @intCast(pow10Of(i512, max_digits + k)))) k += 1;
    return k;
}

const testing = std.testing;

test "formatDecimal pads sub-unit magnitudes, zero, and negatives" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try testing.expectEqualStrings("-0.005", try formatDecimal(a, -5, 3));
    try testing.expectEqualStrings("0", try formatDecimal(a, 0, 0));
    try testing.expectEqualStrings("0.00", try formatDecimal(a, 0, 2));
    try testing.expectEqualStrings("7", try formatDecimal(a, 7, 0));
}
