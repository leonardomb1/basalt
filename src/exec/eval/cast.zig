//! Casts between value kinds, and decimal rescaling and rounding.

const Decimal = @import("../value.zig").Decimal;
const EvalError = @import("../eval.zig").EvalError;
const Value = @import("../value.zig").Value;
const ast = @import("../../lang/ast.zig");
const floatToInt = @import("vec.zig").floatToInt;
const parseIsoDate = @import("time.zig").parseIsoDate;
const parseIsoTime = @import("time.zig").parseIsoTime;
const parseIsoTimestamp = @import("time.zig").parseIsoTimestamp;
const sql = @import("../../db/sql.zig");
const std = @import("std");
const trim = @import("support.zig").trim;
const types = @import("../../lang/types.zig");
const valueToString = @import("format.zig").valueToString;

/// Cast honouring the target's scale; `castValue` only sees a `TypeKind`, which
/// for `DECIMAL(p, s)` left the conversion undefined.
pub fn castValueTyped(arena: std.mem.Allocator, v: Value, ty: types.Type) EvalError!Value {
    if (ty.kind != .decimal) return castValue(arena, v, ty.kind);
    const d: Decimal = switch (v) {
        .decimal => |x| x,
        .int => |x| .{ .unscaled = x, .scale = 0 },
        .bool => |x| .{ .unscaled = if (x) 1 else 0, .scale = 0 },
        .float => |x| floatToDecimal(x) orelse return error.CastFailed,
        .string, .bytes => |str| switch (sql.parseDecimalText(trim(str)) orelse return error.CastFailed) {
            .decimal => |x| x,
            .int => |x| Decimal{ .unscaled = x, .scale = 0 },
            else => return error.CastFailed,
        },
        else => return error.CastFailed,
    };
    return .{ .decimal = rescaleTo(d, ty.scale) orelse return error.CastFailed };
}

fn powTen(n: u8) i128 {
    var r: i128 = 1;
    var i: u8 = 0;
    while (i < n) : (i += 1) r *= 10;
    return r;
}

/// A float as the decimal its 15 significant digits spell, as PostgreSQL casts
/// float8 to numeric. Null for a non-finite value or one past 38 digits.
pub fn floatToDecimal(x: f64) ?Decimal {
    if (!std.math.isFinite(x)) return null;
    if (x == 0) return .{ .unscaled = 0, .scale = 0 };
    var buf: [48]u8 = undefined;
    const s = std.fmt.bufPrint(&buf, "{e:.14}", .{@abs(x)}) catch return null;
    const e_at = std.mem.indexOfScalar(u8, s, 'e') orelse return null;
    var m: i128 = 0;
    for (s[0..e_at]) |c| {
        if (c == '.') continue;
        m = m * 10 + (c - '0');
    }
    const exp = std.fmt.parseInt(i32, s[e_at + 1 ..], 10) catch return null;
    if (x < 0) m = -m;
    const shift = exp - 14;
    if (shift >= 0) {
        if (shift > 23) return null;
        return .{ .unscaled = std.math.mul(i128, m, powTen(@intCast(shift))) catch return null, .scale = 0 };
    }
    const sc = -shift;
    if (sc <= 38) return .{ .unscaled = m, .scale = @intCast(sc) };
    return .{ .unscaled = roundScaleDown(m, @intCast(sc - 38)), .scale = 38 };
}

/// The scale `round(decimal, digits)` answers in: the digits when they are a
/// literal (as DuckDB types it), else the input's own.
pub fn roundOutScale(c: ast.Expr.Call, in_scale: u8) u8 {
    if (c.args.len < 2) return 0;
    if (c.args[1].* != .int_lit) return in_scale;
    return @intCast(std.math.clamp(c.args[1].int_lit, 0, in_scale));
}

pub fn roundDecimal(d: Decimal, digits: i64, out_scale: u8) ?Decimal {
    var r = d;
    if (digits < d.scale) {
        const drop = @as(i64, d.scale) - digits;
        if (drop > 38) return .{ .unscaled = 0, .scale = out_scale };
        var q = roundScaleDown(d.unscaled, @intCast(drop));
        if (digits < 0) {
            q = std.math.mul(i128, q, powTen(@intCast(@min(-digits, 38)))) catch return null;
            r = .{ .unscaled = q, .scale = 0 };
        } else r = .{ .unscaled = q, .scale = @intCast(digits) };
    }
    return rescaleTo(r, out_scale);
}

/// `u / 10^drop`, rounded half away from zero.
pub fn roundScaleDown(u: i128, drop: u32) i128 {
    if (drop == 0) return u;
    if (drop > 38) return 0;
    const p = powTen(@intCast(drop));
    const q = @divTrunc(u, p);
    const r = @rem(u, p);
    const twice = @abs(r) * 2;
    if (twice >= @abs(p)) return if (u < 0) q - 1 else q + 1;
    return q;
}

/// Shifts a decimal to `want`, rounding half away from zero; null on overflow.
/// Public because a value's scale need not match its column's (Postgres sends a
/// per-value `dscale`), so anything combining decimals across rows normalizes first.
pub fn rescaleTo(d: Decimal, want: u8) ?Decimal {
    var unscaled = d.unscaled;
    var have: i32 = d.scale;
    while (have < want) : (have += 1) {
        unscaled = std.math.mul(i128, unscaled, 10) catch return null;
    }
    if (have > want) unscaled = roundScaleDown(unscaled, @intCast(have - want));
    return .{ .unscaled = unscaled, .scale = want };
}

pub fn castValue(arena: std.mem.Allocator, v: Value, kind: types.TypeKind) EvalError!Value {
    return switch (kind) {
        .int => switch (v) {
            .int => v,
            .float => |x| .{ .int = try floatToInt(x) },
            .decimal => |d| .{ .int = std.math.cast(i64, (rescaleTo(d, 0) orelse return error.IntOverflow).unscaled) orelse return error.IntOverflow },
            .bool => |x| .{ .int = if (x) 1 else 0 },
            .string => |s| .{ .int = std.fmt.parseInt(i64, trim(s), 10) catch return error.CastFailed },
            else => error.CastFailed,
        },
        .float => switch (v) {
            .float => v,
            .int => |x| .{ .float = @floatFromInt(x) },
            .decimal => |d| .{ .float = d.toF64() },
            .string => |s| .{ .float = std.fmt.parseFloat(f64, trim(s)) catch return error.CastFailed },
            else => error.CastFailed,
        },
        .date => switch (v) {
            .date => v,
            .timestamp => |x| .{ .date = @intCast(@divFloor(x, 86_400_000_000)) },
            .string => |str| .{ .date = @intCast(parseIsoDate(str) orelse return error.CastFailed) },
            else => error.CastFailed,
        },
        .timestamp => switch (v) {
            .timestamp => v,
            .date => |x| .{ .timestamp = @as(i64, x) * 86_400_000_000 },
            .string => |str| .{ .timestamp = parseIsoTimestamp(str) orelse return error.CastFailed },
            else => error.CastFailed,
        },
        .time => switch (v) {
            .time => v,
            .timestamp => |x| .{ .time = @mod(x, 86_400_000_000) },
            .string => |str| .{ .time = parseIsoTime(str) orelse return error.CastFailed },
            else => error.CastFailed,
        },
        .string => .{ .string = try valueToString(arena, v) },
        .bool => switch (v) {
            .bool => v,
            .int => |x| .{ .bool = x != 0 },
            .string => |s| if (std.ascii.eqlIgnoreCase(trim(s), "true"))
                Value{ .bool = true }
            else if (std.ascii.eqlIgnoreCase(trim(s), "false"))
                Value{ .bool = false }
            else
                error.CastFailed,
            else => error.CastFailed,
        },
        else => error.CastFailed,
    };
}
