//! Parquet's logical and converted types onto basalt types: temporal units, decimals,
//! JSON, and the value coercions they need.

const Error = @import("read.zig").Error;
const Value = @import("../../exec/value.zig").Value;
const parquet = @import("footer.zig");
const std = @import("std");
const types = @import("../../lang/types.zig");

const conv_utf8 = 0;

const conv_decimal = 5;

const conv_date = 6;

const conv_time_millis = 7;

const conv_time_micros = 8;

const conv_timestamp_millis = 9;

const conv_timestamp_micros = 10;

const conv_json = 19;

const conv_bson = 20;
const logical = @import("testing_util.zig").logical;
const testing = std.testing;

pub const TemporalScale = struct {
    mul: i64 = 1,
    div: i64 = 1,

    pub const identity: TemporalScale = .{};

    pub fn isIdentity(self: TemporalScale) bool {
        return self.mul == 1 and self.div == 1;
    }

    pub fn apply(self: TemporalScale, x: i64) i64 {
        const m = std.math.mul(i64, x, self.mul) catch
            return if (x < 0) std.math.minInt(i64) else std.math.maxInt(i64);
        return if (self.div == 1) m else @divFloor(m, self.div);
    }

    fn of(unit: parquet.TimeUnit) TemporalScale {
        return switch (unit) {
            .millis => .{ .mul = 1000 },
            .micros => .identity,
            .nanos => .{ .div = 1000 },
        };
    }
};

pub fn temporalScale(e: parquet.SchemaElement) TemporalScale {
    if (e.logical_type) |lt| switch (lt) {
        .time, .timestamp => |tt| return TemporalScale.of(tt.unit),
        else => {},
    };
    const c = e.converted_type orelse return .identity;
    return switch (c) {
        conv_time_millis, conv_timestamp_millis => .{ .mul = 1000 },
        else => .identity,
    };
}

/// Modern writers omit the ConvertedType for naive and nanosecond timestamps, so
/// the logical type is preferred. `isAdjustedToUTC` is dropped: basalt has no
/// zoned timestamp, so a UTC instant reads as its UTC wall-clock.
pub fn basaltType(e: parquet.SchemaElement) Error!types.Type {
    const phys = e.ty orelse return Error.UnsupportedParquetSchema;
    var t = logicalBasaltType(e, phys) orelse try convertedBasaltType(e, phys);
    if (e.repetition orelse .required != .required) t = t.asNullable();
    return t;
}

/// Null when the logical type is absent, says nothing basalt acts on, or does
/// not fit its physical type, deferring to the converted type.
fn logicalBasaltType(e: parquet.SchemaElement, phys: parquet.PhysicalType) ?types.Type {
    const lt = e.logical_type orelse return null;
    const is_bytes = phys == .byte_array or phys == .fixed_len_byte_array;
    return switch (lt) {
        .string, .@"enum", .json, .bson => if (is_bytes) types.Type.init(.string) else null,
        .date => if (phys == .int32) types.Type.init(.date) else null,
        .time => |tt| switch (phys) {
            .int32 => if (tt.unit == .millis) types.Type.init(.time) else null,
            .int64 => if (tt.unit != .millis) types.Type.init(.time) else null,
            else => null,
        },
        .timestamp => if (phys == .int64) types.Type.init(.timestamp) else null,
        .decimal => |d| switch (phys) {
            .int32, .int64, .byte_array, .fixed_len_byte_array => decimalOf(.{
                .precision = d.precision,
                .scale = d.scale,
            }),
            else => null,
        },
        .integer => if (phys == .int32 or phys == .int64) types.Type.init(.int) else null,
        .uuid, .other => null,
    };
}

fn convertedBasaltType(e: parquet.SchemaElement, phys: parquet.PhysicalType) Error!types.Type {
    const conv = e.converted_type;
    return switch (phys) {
        .boolean => types.Type.init(.bool),
        .int32 => blk: {
            if (conv) |c| {
                if (c == conv_date) break :blk types.Type.init(.date);
                if (c == conv_time_millis) break :blk types.Type.init(.time);
                if (c == conv_decimal) break :blk decimalOf(e);
            }
            break :blk types.Type.init(.int);
        },
        .int64 => blk: {
            if (conv) |c| {
                if (c == conv_timestamp_millis or c == conv_timestamp_micros)
                    break :blk types.Type.init(.timestamp);
                if (c == conv_time_micros) break :blk types.Type.init(.time);
                if (c == conv_decimal) break :blk decimalOf(e);
            }
            break :blk types.Type.init(.int);
        },
        .float, .double => types.Type.init(.float),
        .int96 => types.Type.init(.timestamp),
        .byte_array, .fixed_len_byte_array => blk: {
            if (conv) |c| {
                if (c == conv_utf8 or c == conv_json or c == conv_bson)
                    break :blk types.Type.init(.string);
                if (c == conv_decimal) break :blk decimalOf(e);
            }
            break :blk types.Type.init(.bytes);
        },
        else => Error.UnsupportedParquetSchema,
    };
}

fn decimalOf(e: parquet.SchemaElement) types.Type {
    const p: u8 = if (e.precision) |x| @intCast(@max(1, @min(38, x))) else 38;
    const s: u8 = if (e.scale) |x| @intCast(@max(0, @min(38, x))) else 0;
    return types.Type.decimal(p, s);
}

fn decimalValue(t: types.Type, v: Value) Value {
    return switch (v) {
        .int => |x| .{ .decimal = .{ .unscaled = x, .scale = t.scale } },
        .bytes => |b| blk: {
            var acc: i128 = if (b.len > 0 and b[0] & 0x80 != 0) -1 else 0;
            for (b) |byte| acc = (acc << 8) | byte;
            break :blk .{ .decimal = .{ .unscaled = acc, .scale = t.scale } };
        },
        else => v,
    };
}

pub fn coerce(t: types.Type, v: Value, scale: TemporalScale) Value {
    if (v == .null) return v;
    return switch (t.kind) {
        .string => switch (v) {
            .bytes => |b| .{ .string = b },
            else => v,
        },
        .date => switch (v) {
            .int => |x| .{ .date = @intCast(x) },
            else => v,
        },
        .time => switch (v) {
            .int => |x| .{ .time = scale.apply(x) },
            else => v,
        },
        .timestamp => switch (v) {
            .int => |x| .{ .timestamp = scale.apply(x) },
            else => v,
        },
        .decimal => decimalValue(t, v),
        else => v,
    };
}

test "physical plus converted type maps onto a basalt type" {
    const opt = parquet.Repetition.optional;
    try testing.expectEqual(types.TypeKind.int, (try basaltType(.{ .ty = .int32, .repetition = opt })).kind);
    try testing.expectEqual(types.TypeKind.date, (try basaltType(.{ .ty = .int32, .converted_type = 6, .repetition = opt })).kind);
    try testing.expectEqual(types.TypeKind.timestamp, (try basaltType(.{ .ty = .int64, .converted_type = 10, .repetition = opt })).kind);
    try testing.expectEqual(types.TypeKind.timestamp, (try basaltType(.{ .ty = .int96, .repetition = opt })).kind);
    try testing.expectEqual(types.TypeKind.float, (try basaltType(.{ .ty = .double, .repetition = opt })).kind);
    try testing.expectEqual(types.TypeKind.bytes, (try basaltType(.{ .ty = .byte_array, .repetition = opt })).kind);
    try testing.expectEqual(types.TypeKind.string, (try basaltType(.{ .ty = .byte_array, .converted_type = 0, .repetition = opt })).kind);
    try testing.expectEqual(types.TypeKind.decimal, (try basaltType(.{ .ty = .int64, .converted_type = 5, .precision = 18, .scale = 4, .repetition = opt })).kind);

    try testing.expect((try basaltType(.{ .ty = .int32, .repetition = .required })).nullable == false);
    try testing.expect((try basaltType(.{ .ty = .int32, .repetition = opt })).nullable);
    try testing.expectError(Error.UnsupportedParquetSchema, basaltType(.{ .num_children = 2 }));
}

test "temporal scale converts millisecond columns and leaves micros alone" {
    try testing.expectEqual(TemporalScale{ .mul = 1000 }, temporalScale(.{ .converted_type = 9 }));
    try testing.expectEqual(TemporalScale{ .mul = 1000 }, temporalScale(.{ .converted_type = 7 }));
    try testing.expectEqual(TemporalScale.identity, temporalScale(.{ .converted_type = 10 }));
    try testing.expectEqual(TemporalScale.identity, temporalScale(.{}));

    const ts = types.Type.init(.timestamp);
    try testing.expectEqual(@as(i64, 1_583_298_367_123_000), coerce(ts, .{ .int = 1_583_298_367_123 }, .{ .mul = 1000 }).timestamp);
    try testing.expectEqual(@as(i64, 1_583_298_367_123), coerce(ts, .{ .int = 1_583_298_367_123 }, .identity).timestamp);
}

test "a LogicalType-only TIMESTAMP reads as timestamp in every unit" {
    const want: i64 = 1_767_268_800_000_000;
    const ts = types.Type.init(.timestamp);
    inline for (.{
        .{ parquet.TimeUnit.millis, want / 1000 },
        .{ parquet.TimeUnit.micros, want },
        .{ parquet.TimeUnit.nanos, want * 1000 },
    }) |c| {
        const e = logical(.int64, .{ .timestamp = .{ .unit = c[0] } });
        try testing.expectEqual(types.TypeKind.timestamp, (try basaltType(e)).kind);
        try testing.expectEqual(want, coerce(ts, .{ .int = c[1] }, temporalScale(e)).timestamp);
    }
    const utc = logical(.int64, .{ .timestamp = .{ .adjusted_to_utc = true, .unit = .nanos } });
    try testing.expectEqual(types.TypeKind.timestamp, (try basaltType(utc)).kind);
}

test "nanoseconds floor to micros, including before 1970" {
    const ns = TemporalScale{ .div = 1000 };
    try testing.expectEqual(@as(i64, 1), ns.apply(1_999));
    try testing.expectEqual(@as(i64, -1), ns.apply(-1));
    try testing.expectEqual(@as(i64, -2), ns.apply(-1_001));
    const ms = TemporalScale{ .mul = 1000 };
    try testing.expectEqual(@as(i64, std.math.maxInt(i64)), ms.apply(std.math.maxInt(i64) / 10));
}

test "a LogicalType-only TIME reads as time in millis and micros" {
    const tm = types.Type.init(.time);
    const want: i64 = 3_720_000_000;
    const ms = logical(.int32, .{ .time = .{ .unit = .millis } });
    try testing.expectEqual(types.TypeKind.time, (try basaltType(ms)).kind);
    try testing.expectEqual(want, coerce(tm, .{ .int = want / 1000 }, temporalScale(ms)).time);
    const us = logical(.int64, .{ .time = .{ .unit = .micros } });
    try testing.expectEqual(types.TypeKind.time, (try basaltType(us)).kind);
    try testing.expectEqual(want, coerce(tm, .{ .int = want }, temporalScale(us)).time);
    const ns = logical(.int64, .{ .time = .{ .unit = .nanos } });
    try testing.expectEqual(types.TypeKind.time, (try basaltType(ns)).kind);
    try testing.expectEqual(want, coerce(tm, .{ .int = want * 1000 }, temporalScale(ns)).time);
}

test "LogicalType DATE, DECIMAL, STRING and INTEGER map without a ConvertedType" {
    try testing.expectEqual(types.TypeKind.date, (try basaltType(logical(.int32, .date))).kind);
    const dec = try basaltType(logical(.fixed_len_byte_array, .{ .decimal = .{ .scale = 2, .precision = 12 } }));
    try testing.expectEqual(types.TypeKind.decimal, dec.kind);
    try testing.expectEqual(@as(u8, 12), dec.precision);
    try testing.expectEqual(@as(u8, 2), dec.scale);
    try testing.expectEqual(types.TypeKind.string, (try basaltType(logical(.byte_array, .string))).kind);
    try testing.expectEqual(types.TypeKind.string, (try basaltType(logical(.byte_array, .json))).kind);
    try testing.expectEqual(types.TypeKind.int, (try basaltType(logical(.int32, .{ .integer = .{ .bit_width = 8, .signed = false } }))).kind);
    try testing.expectEqual(types.TypeKind.bytes, (try basaltType(logical(.fixed_len_byte_array, .uuid))).kind);
    try testing.expectEqual(types.TypeKind.bytes, (try basaltType(logical(.byte_array, .{ .timestamp = .{} }))).kind);
}

test "LogicalType wins over a ConvertedType, and agreeing annotations stay put" {
    var both = logical(.int64, .{ .timestamp = .{ .adjusted_to_utc = true, .unit = .millis } });
    both.converted_type = 9;
    try testing.expectEqual(types.TypeKind.timestamp, (try basaltType(both)).kind);
    try testing.expectEqual(TemporalScale{ .mul = 1000 }, temporalScale(both));

    var dec = logical(.int64, .{ .decimal = .{ .scale = 4, .precision = 18 } });
    dec.converted_type = 5;
    dec.scale = 4;
    dec.precision = 18;
    const d = try basaltType(dec);
    try testing.expectEqual(@as(u8, 18), d.precision);
    try testing.expectEqual(@as(u8, 4), d.scale);

    var other = logical(.int32, .other);
    other.converted_type = 6;
    try testing.expectEqual(types.TypeKind.date, (try basaltType(other)).kind);
}
