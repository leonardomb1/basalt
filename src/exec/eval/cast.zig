//! Casts between value kinds.

const Decimal = @import("../value.zig").Decimal;
const EvalError = @import("../eval.zig").EvalError;
const Value = @import("../value.zig").Value;
const ast = @import("../../lang/ast.zig");
const civil = @import("../civil.zig");
const textconv = @import("../textconv.zig");
const parseDateText = @import("time.zig").parseDateText;
const parseIsoTime = @import("time.zig").parseIsoTime;
const parseTimestampText = @import("time.zig").parseTimestampText;
const decimal = @import("../decimal.zig");
const rescaleTo = decimal.rescaleTo;
const fitsPrecision = decimal.fitsPrecision;
const max_digits = decimal.max_digits;
const asDecimalValue = decimal.asDecimalValue;
const std = @import("std");
const clip = @import("support.zig").clip;
const decimalOverflow = decimal.decimalOverflow;
const failWith = @import("support.zig").failWith;
const trim = @import("support.zig").trim;
const types = @import("../../lang/types.zig");
const valueToString = @import("format.zig").valueToString;

/// Cast honouring the target's scale; `castValue` only sees a `TypeKind`, which
/// for `DECIMAL(p, s)` left the conversion undefined.
pub fn castValueTyped(arena: std.mem.Allocator, v: Value, ty: types.Type) EvalError!Value {
    if (ty.kind != .decimal) return castValue(arena, v, ty.kind);
    const r = rescaleTo(asDecimalValue(v) orelse return error.CastFailed, ty.scale) orelse return error.CastFailed;
    if (!fitsPrecision(r.unscaled, ty.precision)) return error.CastFailed;
    return .{ .decimal = r };
}

/// Text as `SUM` over a text column adds it: an exact number in `ty` (an INT, or a
/// DECIMAL whose scale the column's sampled text set), never rounded. More decimal
/// places than that scale (beyond trailing zeros), exponent form, `nan` and `inf`
/// fail with the CAST that sums them.
pub fn exactSumText(text: []const u8, ty: types.Type) EvalError!Value {
    const s = trim(text);
    const want: u8 = if (ty.kind == .decimal) ty.scale else 0;
    const d: Decimal = decimal.parse(s) orelse {
        if (parseFloatStrict(s) != null)
            return failWith(error.CastFailed, "SUM: '{s}' is not an exact number; sum CAST(x AS FLOAT) to add it as a float", .{clip(s)});
        return failWith(error.CastFailed, "SUM: '{s}' is not a number", .{clip(s)});
    };
    if (decimal.dropsDigits(d, want)) {
        if (want == 0)
            return failWith(error.CastFailed, "SUM: '{s}' has decimal places, but the sum is an INT, as no value sampled at plan time had any; sum CAST(x AS DECIMAL(38, {d})) instead", .{ clip(s), d.scale });
        return failWith(error.CastFailed, "SUM: '{s}' has {d} decimal places, more than the {d} of the column's sampled values the sum is typed by; sum CAST(x AS DECIMAL(38, {d})) instead", .{ clip(s), d.scale, want, d.scale });
    }
    const r = rescaleTo(d, want) orelse return decimalOverflow();
    if (ty.kind == .decimal) {
        if (!fitsPrecision(r.unscaled, max_digits)) return decimalOverflow();
        return .{ .decimal = r };
    }
    return .{ .int = std.math.cast(i64, r.unscaled) orelse
        return failWith(error.IntOverflow, "SUM: '{s}' is past the INT range; sum CAST(x AS DECIMAL(38, 0)) instead", .{clip(s)}) };
}

/// Text as an INT: an optional sign and digits, nothing else (callers trim).
/// `std.fmt.parseInt` alone also takes `_` separators, so `'1_000'` read as 1000.
pub fn parseIntStrict(s: []const u8) ?i64 {
    const body = if (s.len > 0 and (s[0] == '-' or s[0] == '+')) s[1..] else s;
    if (body.len == 0) return null;
    for (body) |c| if (c < '0' or c > '9') return null;
    return std.fmt.parseInt(i64, s, 10) catch null;
}

/// Text as a FLOAT: an optional sign, digits with at most one `.`, and an optional
/// exponent (`1.5e-05`, as spreadsheets and pandas write small values), or the
/// words `nan`, `inf` and `infinity`. No `_` or hex, which `std.fmt.parseFloat`
/// alone accepts; callers trim.
pub fn parseFloatStrict(s: []const u8) ?f64 {
    var i: usize = 0;
    if (i < s.len and (s[i] == '-' or s[i] == '+')) i += 1;
    for ([_][]const u8{ "nan", "inf", "infinity" }) |word| {
        if (std.ascii.eqlIgnoreCase(s[i..], word)) return std.fmt.parseFloat(f64, s) catch null;
    }
    var digits: usize = 0;
    var dot = false;
    while (i < s.len) : (i += 1) {
        if (s[i] >= '0' and s[i] <= '9') {
            digits += 1;
        } else if (s[i] == '.' and !dot) {
            dot = true;
        } else break;
    }
    if (digits == 0) return null;
    if (i < s.len and (s[i] == 'e' or s[i] == 'E')) {
        i += 1;
        if (i < s.len and (s[i] == '-' or s[i] == '+')) i += 1;
        const start = i;
        while (i < s.len and s[i] >= '0' and s[i] <= '9') i += 1;
        if (i == start) return null;
    }
    if (i != s.len) return null;
    const x = std.fmt.parseFloat(f64, s) catch return null;
    return if (std.math.isFinite(x)) x else null;
}

/// How a non-decimal kind's value is held, in a `Value` and in a column.
pub fn Elem(comptime k: types.TypeKind) type {
    return switch (k) {
        .bool => bool,
        .int, .time, .timestamp => i64,
        .float => f64,
        .date => i32,
        .string => []const u8,
        else => @compileError("castElem covers the non-decimal scalar kinds"),
    };
}

/// Whether `castElem` converts `from` to `to`: the pairs between different kinds
/// other than DECIMAL ones and those to STRING.
pub fn hasElemCast(comptime from: types.TypeKind, comptime to: types.TypeKind) bool {
    return switch (to) {
        .int => from == .float or from == .bool or from == .string,
        .float => from == .int or from == .string,
        .date => from == .timestamp or from == .string,
        .timestamp => from == .date or from == .string,
        .time => from == .timestamp or from == .string,
        .bool => from == .int or from == .string,
        else => false,
    };
}

/// One value cast from `from` to `to`, the math the row cast and the vector cast
/// share. Its error set is empty for a pair that cannot fail, so a column loop over
/// one has no failure branch.
pub inline fn castElem(comptime from: types.TypeKind, comptime to: types.TypeKind, x: Elem(from)) !Elem(to) {
    if (comptime !hasElemCast(from, to)) @compileError("no element cast from " ++ @tagName(from) ++ " to " ++ @tagName(to));
    return switch (to) {
        .int => switch (from) {
            .float => textconv.floatToInt(x, .half_even) orelse error.CastFailed,
            .bool => @intFromBool(x),
            .string => parseIntStrict(trim(x)) orelse error.CastFailed,
            else => comptime unreachable,
        },
        .float => switch (from) {
            .int => @floatFromInt(x),
            .string => parseFloatStrict(trim(x)) orelse error.CastFailed,
            else => comptime unreachable,
        },
        .date => switch (from) {
            .timestamp => @intCast(civil.dayOf(x)),
            .string => @intCast(parseDateText(x) orelse return error.CastFailed),
            else => comptime unreachable,
        },
        .timestamp => switch (from) {
            .date => civil.dayMicrosChecked(x) orelse error.CastFailed,
            .string => parseTimestampText(x) orelse error.CastFailed,
            else => comptime unreachable,
        },
        .time => switch (from) {
            .timestamp => civil.timeOfDay(x),
            .string => parseIsoTime(x) orelse error.CastFailed,
            else => comptime unreachable,
        },
        .bool => switch (from) {
            .int => x != 0,
            .string => textconv.boolText(.sql, x) orelse error.CastFailed,
            else => comptime unreachable,
        },
        else => comptime unreachable,
    };
}

/// A cast by kind alone; `castValueTyped` brings a DECIMAL to its scale.
pub fn castValue(arena: std.mem.Allocator, v: Value, kind: types.TypeKind) EvalError!Value {
    switch (kind) {
        .string => return .{ .string = try valueToString(arena, v) },
        inline .bool, .int, .float, .date, .timestamp, .time => |to| switch (v) {
            .null, .bytes => return error.CastFailed,
            .decimal => |d| return switch (to) {
                .int => .{ .int = std.math.cast(i64, (rescaleTo(d, 0) orelse return error.IntOverflow).unscaled) orelse return error.IntOverflow },
                .float => .{ .float = d.toF64() },
                else => error.CastFailed,
            },
            inline else => |x, tag| {
                const from = comptime @field(types.TypeKind, @tagName(tag));
                if (comptime from == to) return v;
                if (comptime !hasElemCast(from, to)) return error.CastFailed;
                return @unionInit(Value, @tagName(to), try castElem(from, to, x));
            },
        },
        else => return error.CastFailed,
    }
}

test "decimals lose digits by rounding half away from zero, on every path" {
    const cases = [_]struct { u: i128, s: u8, to: u8, want: i128 }{
        .{ .u = 12345, .s = 3, .to = 2, .want = 1235 },
        .{ .u = -12345, .s = 3, .to = 2, .want = -1235 },
        .{ .u = 12344, .s = 3, .to = 2, .want = 1234 },
        .{ .u = 5, .s = 3, .to = 2, .want = 1 },
        .{ .u = -4, .s = 3, .to = 2, .want = 0 },
        .{ .u = 999, .s = 3, .to = 0, .want = 1 },
        .{ .u = std.math.maxInt(i128), .s = 38, .to = 0, .want = 2 },
    };
    for (cases) |c| try std.testing.expectEqual(c.want, rescaleTo(.{ .unscaled = c.u, .scale = c.s }, c.to).?.unscaled);

    const floats = [_]struct { x: f64, to: u8, want: i128 }{
        .{ .x = 12.345, .to = 2, .want = 1235 },
        .{ .x = -12.345, .to = 2, .want = -1235 },
        .{ .x = 1.005, .to = 2, .want = 101 },
        .{ .x = 2.675, .to = 2, .want = 268 },
        .{ .x = 0.1 + 0.2, .to = 17, .want = 30000000000000000 },
        .{ .x = 1e-300, .to = 2, .want = 0 },
        .{ .x = 123456789012345678.0, .to = 0, .want = 123456789012346000 },
    };
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    for (floats) |c| {
        const got = try castValueTyped(ar.allocator(), .{ .float = c.x }, types.Type.decimal(38, c.to));
        try std.testing.expectEqual(c.want, got.decimal.unscaled);
    }
    try std.testing.expectError(error.CastFailed, castValueTyped(ar.allocator(), .{ .float = 1e300 }, types.Type.decimal(10, 2)));
    try std.testing.expectError(error.CastFailed, castValueTyped(ar.allocator(), .{ .float = std.math.nan(f64) }, types.Type.decimal(10, 2)));
    try std.testing.expectEqual(@as(i128, -1235), (try castValueTyped(ar.allocator(), .{ .string = "-12.345" }, types.Type.decimal(10, 2))).decimal.unscaled);
}

test "exactSumText adds text exactly or fails, never rounding" {
    const d2 = types.Type.decimal(38, 2);
    try std.testing.expectEqual(@as(i128, 150), (try exactSumText(" 1.5 ", d2)).decimal.unscaled);
    try std.testing.expectEqual(@as(i128, 150), (try exactSumText("1.500", d2)).decimal.unscaled);
    try std.testing.expectEqual(@as(i64, 9007199254740993), (try exactSumText("9007199254740993", types.Type.init(.int))).int);
    try std.testing.expectError(error.CastFailed, exactSumText("1.255", d2));
    try std.testing.expectError(error.CastFailed, exactSumText("1.5", types.Type.init(.int)));
    try std.testing.expectError(error.CastFailed, exactSumText("1e3", d2));
    try std.testing.expectError(error.CastFailed, exactSumText("nan", d2));
    try std.testing.expectError(error.IntOverflow, exactSumText("99999999999999999999", types.Type.init(.int)));
}

test "castValue: conversions succeed and failures are CastFailed specifically" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    try std.testing.expectEqual(@as(i64, 42), (try castValue(a, .{ .string = " 42 " }, .int)).int);
    try std.testing.expectEqual(@as(i64, 1), (try castValue(a, .{ .bool = true }, .int)).int);
    try std.testing.expectEqual(@as(i64, -4), (try castValue(a, .{ .float = -3.9 }, .int)).int);
    try std.testing.expectEqual(@as(i64, 2), (try castValue(a, .{ .float = 2.5 }, .int)).int);
    try std.testing.expectEqual(@as(i64, 4), (try castValue(a, .{ .float = 3.5 }, .int)).int);
    try std.testing.expectEqual(@as(i64, -2), (try castValue(a, .{ .float = -2.5 }, .int)).int);
    try std.testing.expectEqual(@as(i64, -4), (try castValue(a, .{ .float = -3.5 }, .int)).int);
    try std.testing.expectEqual(@as(i64, 3), (try castValue(a, .{ .float = 2.7 }, .int)).int);
    try std.testing.expectEqual(@as(i64, 3), (try castValue(a, .{ .decimal = .{ .unscaled = 25, .scale = 1 } }, .int)).int);
    try std.testing.expectEqual(@as(i64, -3), (try castValue(a, .{ .decimal = .{ .unscaled = -25, .scale = 1 } }, .int)).int);
    try std.testing.expectEqual(@as(i64, 2), (try castValue(a, .{ .float = 2.4 }, .int)).int);
    try std.testing.expectError(error.CastFailed, castValue(a, .{ .float = 9223372036854775807.0 }, .int));
    try std.testing.expectEqual(@as(f64, 2.5), (try castValue(a, .{ .string = "2.5" }, .float)).float);
    try std.testing.expect((try castValue(a, .{ .string = " TRUE " }, .bool)).bool);
    try std.testing.expect(!(try castValue(a, .{ .int = 0 }, .bool)).bool);
    try std.testing.expectEqualStrings("123.45", (try castValue(a, .{ .decimal = .{ .unscaled = 12345, .scale = 2 } }, .string)).string);

    try std.testing.expectError(error.CastFailed, castValue(a, .{ .string = "abc" }, .int));
    try std.testing.expectError(error.CastFailed, castValue(a, .{ .float = std.math.nan(f64) }, .int));
    try std.testing.expectError(error.CastFailed, castValue(a, .{ .float = 1e19 }, .int));
    try std.testing.expectError(error.CastFailed, castValue(a, .{ .float = std.math.inf(f64) }, .int));
    try std.testing.expect((try castValue(a, .{ .string = "yes" }, .bool)).bool);
    try std.testing.expectError(error.CastFailed, castValue(a, .{ .string = "on" }, .bool));
    try std.testing.expectError(error.CastFailed, castValue(a, .{ .bool = true }, .float));
}

test "text to a number is strict: sign, digits, one dot; a FLOAT also takes an exponent" {
    for ([_][]const u8{ "1_000", "0x10", "1 000", "", "-", "1.5", "1e3", "inf", " 5" }) |s|
        try std.testing.expect(parseIntStrict(s) == null);
    try std.testing.expectEqual(@as(?i64, -42), parseIntStrict("-42"));
    try std.testing.expectEqual(@as(?i64, 7), parseIntStrict("+7"));
    for ([_][]const u8{ "1_000", "0x10", "0x1p3", "1 000", "", ".", "-", "1e", "1e+", "1.2.3", "1e3.5", "1e400", "nana" }) |s|
        try std.testing.expect(parseFloatStrict(s) == null);
    try std.testing.expectEqual(@as(?f64, 1000), parseFloatStrict("1e3"));
    try std.testing.expectEqual(@as(?f64, 0.000015), parseFloatStrict("1.5e-05"));
    try std.testing.expectEqual(@as(?f64, -0.5), parseFloatStrict("-.5"));
    try std.testing.expectEqual(@as(?f64, 2), parseFloatStrict("2."));
    try std.testing.expect(std.math.isNan(parseFloatStrict("NaN").?));
    try std.testing.expect(std.math.isNegativeInf(parseFloatStrict("-Infinity").?));
    try std.testing.expect(std.math.isPositiveInf(parseFloatStrict("inf").?));
}

test "a DECIMAL cast fails when the value does not fit its precision" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectError(error.CastFailed, castValueTyped(a, .{ .string = "1234.5" }, types.Type.decimal(4, 1)));
    try std.testing.expectError(error.CastFailed, castValueTyped(a, .{ .int = 10 }, types.Type.decimal(38, 37)));
    try std.testing.expectError(error.CastFailed, castValueTyped(a, .{ .string = "9.999" }, types.Type.decimal(3, 2)));
    try std.testing.expectEqual(@as(i128, 999), (try castValueTyped(a, .{ .string = "9.99" }, types.Type.decimal(3, 2))).decimal.unscaled);
    try std.testing.expectEqual(@as(i128, 12345), (try castValueTyped(a, .{ .string = "123.45" }, types.Type.decimal(0, 2))).decimal.unscaled);
    try std.testing.expect(fitsPrecision(std.math.pow(i128, 10, 38) - 1, 38));
    try std.testing.expect(!fitsPrecision(std.math.pow(i128, 10, 38), 0));
}

test "a date past the TIMESTAMP range fails the cast instead of overflowing" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectError(error.CastFailed, castValue(a, .{ .date = std.math.maxInt(i32) }, .timestamp));
    try std.testing.expectError(error.CastFailed, castValue(a, .{ .date = std.math.minInt(i32) }, .timestamp));
    try std.testing.expectError(error.CastFailed, @import("testing_util.zig").evalLit(a, "CAST(make_date(300000, 1, 1) AS TIMESTAMP)"));
    try std.testing.expectEqual(@as(i64, 106_751 * civil.us_per_day), (try castValue(a, .{ .date = 106_751 }, .timestamp)).timestamp);
    try std.testing.expectEqual(@as(i64, -1), (try castValue(a, .{ .timestamp = -civil.us_per_day - 1 }, .date)).date + 1);
    try std.testing.expectEqual(civil.us_per_day - 1, (try castValue(a, .{ .timestamp = -1 }, .time)).time);
}

test "castValue on castElem gives what the per-kind switch gave, for every kind pair and every short BOOL text, less the words D6 added" {
    const Orig = struct {
        fn oldCastValue(arena: std.mem.Allocator, v: Value, kind: types.TypeKind) EvalError!Value {
            const column = @import("../column.zig");
            return switch (kind) {
                .int => switch (v) {
                    .int => v,
                    .float => |x| .{ .int = try column.floatToInt(x) },
                    .decimal => |d| .{ .int = std.math.cast(i64, (rescaleTo(d, 0) orelse return error.IntOverflow).unscaled) orelse return error.IntOverflow },
                    .bool => |x| .{ .int = if (x) 1 else 0 },
                    .string => |s| .{ .int = parseIntStrict(trim(s)) orelse return error.CastFailed },
                    else => error.CastFailed,
                },
                .float => switch (v) {
                    .float => v,
                    .int => |x| .{ .float = @floatFromInt(x) },
                    .decimal => |d| .{ .float = d.toF64() },
                    .string => |s| .{ .float = parseFloatStrict(trim(s)) orelse return error.CastFailed },
                    else => error.CastFailed,
                },
                .date => switch (v) {
                    .date => v,
                    .timestamp => |x| .{ .date = @intCast(@divFloor(x, 24 * 3600 * 1_000_000)) },
                    .string => |str| .{ .date = @intCast(parseDateText(str) orelse return error.CastFailed) },
                    else => error.CastFailed,
                },
                .timestamp => switch (v) {
                    .timestamp => v,
                    .date => |x| .{ .timestamp = std.math.mul(i64, x, 24 * 3600 * 1_000_000) catch return error.CastFailed },
                    .string => |str| .{ .timestamp = parseTimestampText(str) orelse return error.CastFailed },
                    else => error.CastFailed,
                },
                .time => switch (v) {
                    .time => v,
                    .timestamp => |x| .{ .time = @mod(x, 24 * 3600 * 1_000_000) },
                    .string => |str| .{ .time = parseIsoTime(str) orelse return error.CastFailed },
                    else => error.CastFailed,
                },
                .string => .{ .string = try valueToString(arena, v) },
                .bool => switch (v) {
                    .bool => v,
                    .int => |x| .{ .bool = x != 0 },
                    .string => |s| sqlBool(trim(s)),
                    else => error.CastFailed,
                },
                else => error.CastFailed,
            };
        }
        /// D6: `t`/`f`, `yes`/`no`, `y`/`n` and `1`/`0` join `true`/`false`.
        fn sqlBool(s: []const u8) EvalError!Value {
            for ([_][]const u8{ "true", "t", "yes", "y", "1" }) |w| if (std.ascii.eqlIgnoreCase(s, w)) return .{ .bool = true };
            for ([_][]const u8{ "false", "f", "no", "n", "0" }) |w| if (std.ascii.eqlIgnoreCase(s, w)) return .{ .bool = false };
            return error.CastFailed;
        }
        fn expectSameCast(a: std.mem.Allocator, v: Value, kind: types.TypeKind) !void {
            const want = oldCastValue(a, v, kind);
            const got = castValue(a, v, kind);
            if (want) |w| {
                const g = try got;
                try std.testing.expectEqual(std.meta.activeTag(w), std.meta.activeTag(g));
                switch (w) {
                    .string, .bytes => |s| try std.testing.expectEqualStrings(s, if (g == .string) g.string else g.bytes),
                    .float => |x| if (std.math.isNan(x)) try std.testing.expect(std.math.isNan(g.float)) else try std.testing.expectEqual(x, g.float),
                    else => try std.testing.expectEqual(w, g),
                }
            } else |e| try std.testing.expectError(e, got);
        }
    };
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const values = [_]Value{
        .null,                                  .{ .bool = true },                      .{ .bool = false },
        .{ .int = 0 },                          .{ .int = 1 },                          .{ .int = -1 },
        .{ .int = std.math.maxInt(i64) },       .{ .int = std.math.minInt(i64) },       .{ .float = 2.5 },
        .{ .float = -3.5 },                     .{ .float = 0x1p63 },                   .{ .float = -0x1p63 },
        .{ .float = 1e19 },                     .{ .float = std.math.nan(f64) },        .{ .float = std.math.inf(f64) },
        .{ .float = -std.math.inf(f64) },       .{ .float = -0.0 },                     .{ .decimal = .{ .unscaled = 125, .scale = 2 } },
        .{ .string = " 42 " },                  .{ .string = "1_000" },                 .{ .string = "0x10" },
        .{ .string = "2.5" },                   .{ .string = "1e3" },                   .{ .string = " TRUE " },
        .{ .string = "false" },                 .{ .string = "t" },                     .{ .string = "" },
        .{ .string = "nan" },                   .{ .string = "2026-10-03" },            .{ .string = "2026-10-03 12:34:56.5" },
        .{ .string = "12:34:56" },              .{ .string = "-0043-03-15" },           .{ .bytes = "\x01\xff" },
        .{ .date = 0 },                         .{ .date = 106_751 },                   .{ .date = -106_752 },
        .{ .date = std.math.maxInt(i32) },      .{ .date = std.math.minInt(i32) },      .{ .time = 0 },
        .{ .time = 86_399_999_999 },            .{ .timestamp = 0 },                    .{ .timestamp = -1 },
        .{ .timestamp = std.math.maxInt(i64) }, .{ .timestamp = std.math.minInt(i64) },
    };
    const kinds = [_]types.TypeKind{ .bool, .int, .float, .decimal, .string, .bytes, .date, .time, .timestamp, .array, .@"struct" };
    for (values) |v| for (kinds) |k| Orig.expectSameCast(a, v, k) catch |e| {
        std.debug.print("CAST({any} AS {s})\n", .{ v, @tagName(k) });
        return e;
    };
    const alphabet = "tRuEfalsyNo01 \t";
    var buf: [6]u8 = undefined;
    for (0..6) |len| {
        var idx = [_]usize{0} ** 6;
        while (true) {
            for (0..len) |i| buf[i] = alphabet[idx[i]];
            try Orig.expectSameCast(a, .{ .string = buf[0..len] }, .bool);
            var i: usize = 0;
            while (i < len) : (i += 1) {
                idx[i] += 1;
                if (idx[i] < alphabet.len) break;
                idx[i] = 0;
            }
            if (i == len) break;
        }
    }
}

test "a column cast and the row cast agree, nulls and failures included" {
    const column = @import("../column.zig");
    const Batch = @import("../batch.zig").Batch;
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const T = types.Type;
    var fb = column.Builder.init(a, T.init(.float).asNullable());
    var bb = column.Builder.init(a, T.init(.bool).asNullable());
    var sb = column.Builder.init(a, T.init(.string).asNullable());
    var nb = column.Builder.init(a, T.init(.string).asNullable());
    const fs = [_]?f64{ 2.5, -3.5, null, 1e18, -0.0, 7 };
    const bs = [_]?bool{ true, false, null, true, false, true };
    const ss = [_]?[]const u8{ " 42 ", "-7", null, "+3", "0", "9223372036854775807" };
    const ns = [_]?[]const u8{ "1_000", "x", null, "", "1e3", "0x10" };
    for (fs, bs, ss, ns) |f, b, s, t| {
        try fb.append(if (f) |x| .{ .float = x } else .null);
        try bb.append(if (b) |x| .{ .bool = x } else .null);
        try sb.append(if (s) |x| .{ .string = x } else .null);
        try nb.append(if (t) |x| .{ .string = x } else .null);
    }
    const x = try column.intColumn(a, &.{ 0, 1, null, -1, std.math.maxInt(i64), std.math.minInt(i64) });
    const schema = types.Schema{ .fields = &.{
        .{ .name = "f", .ty = T.init(.float).asNullable() },
        .{ .name = "b", .ty = T.init(.bool).asNullable() },
        .{ .name = "s", .ty = T.init(.string).asNullable() },
        .{ .name = "n", .ty = T.init(.string).asNullable() },
        .{ .name = "x", .ty = T.init(.int).asNullable() },
    } };
    var cols = [_]column.Column{ try fb.finish(), try bb.finish(), try sb.finish(), try nb.finish(), x };
    const batch = Batch{ .schema = &schema, .columns = &cols, .len = fs.len };
    const util = @import("testing_util.zig");
    try util.expectVecMatchesRows(a, batch, &.{
        "CAST(f AS INT)", "CAST(b AS INT)", "CAST(s AS INT)", "CAST(x AS DOUBLE)", "CAST(s AS DOUBLE)", "CAST(x AS BOOL)",
    }, true);
    try util.expectVecMatchesRows(a, batch, &.{ "CAST(n AS INT)", "CAST(n AS DOUBLE)", "CAST(s AS BOOL)", "CAST(CAST(x AS DOUBLE) * 1e3 AS INT)" }, false);
}
