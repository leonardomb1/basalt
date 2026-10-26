//! Parquet's logical and converted types onto basalt types: temporal units, decimals,
//! JSON, and the unit, sign and decimal rules the page kernel converts values by.

const Error = @import("read.zig").Error;
const parquet = @import("footer.zig");
const std = @import("std");
const types = @import("../../lang/types.zig");
const civil = @import("../../exec/civil.zig");

pub const conv_utf8 = 0;
pub const conv_decimal = 5;
pub const conv_date = 6;
const conv_time_millis = 7;
pub const conv_time_micros = 8;
const conv_timestamp_millis = 9;
pub const conv_timestamp_micros = 10;
const conv_uint8 = 11;
const conv_uint16 = 12;
const conv_uint32 = 13;
const conv_uint64 = 14;
pub const conv_int8 = 15;
pub const conv_int16 = 16;
pub const conv_int32 = 17;
const conv_json = 19;
const conv_bson = 20;
const logical = @import("testing_util.zig").logical;
const testing = std.testing;

/// The unit a TIME or TIMESTAMP column counts in, which the page kernel reads as
/// micros through `civil.Tick.toMicros(.saturate)`: the logical type's, else a
/// millisecond converted type's, else micros.
pub fn temporalTick(e: parquet.SchemaElement) civil.Tick {
    if (e.logical_type) |lt| switch (lt) {
        .time, .timestamp => |tt| return tickOf(tt.unit),
        else => {},
    };
    const c = e.converted_type orelse return .us;
    return switch (c) {
        conv_time_millis, conv_timestamp_millis => .ms,
        else => .us,
    };
}

fn tickOf(unit: parquet.TimeUnit) civil.Tick {
    return switch (unit) {
        .millis => .ms,
        .micros => .us,
        .nanos => .ns,
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
        .integer => if (phys == .int32 or phys == .int64) intType(e) else null,
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
            break :blk intType(e);
        },
        .int64 => blk: {
            if (conv) |c| {
                if (c == conv_timestamp_millis or c == conv_timestamp_micros)
                    break :blk types.Type.init(.timestamp);
                if (c == conv_time_micros) break :blk types.Type.init(.time);
                if (c == conv_decimal) break :blk decimalOf(e);
            }
            break :blk intType(e);
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

/// The width of an unsigned integer column (`INTEGER(n, false)` or `UINT_n`), whose
/// physical value is stored sign-extended; null for a signed one.
pub fn unsignedBits(e: parquet.SchemaElement) ?u8 {
    if (e.logical_type) |lt| switch (lt) {
        .integer => |it| return if (it.signed) null else @intCast(@max(8, it.bit_width)),
        else => return null,
    };
    return switch (e.converted_type orelse return null) {
        conv_uint8 => 8,
        conv_uint16 => 16,
        conv_uint32 => 32,
        conv_uint64 => 64,
        else => null,
    };
}

/// An integer column's type: `types.unsignedType` for an unsigned one, so a UINT64
/// is the exact DECIMAL(20,0).
fn intType(e: parquet.SchemaElement) types.Type {
    const bits = unsignedBits(e) orelse return types.Type.init(.int);
    return types.unsignedType(bits);
}

fn decimalOf(e: parquet.SchemaElement) types.Type {
    const p: u8 = if (e.precision) |x| @intCast(@max(1, @min(38, x))) else 38;
    const s: u8 = if (e.scale) |x| @intCast(@max(0, @min(38, x))) else 0;
    return types.Type.decimal(p, s);
}

/// A byte-array decimal's big-endian two's-complement unscaled value, unchecked
/// (`decimal.bigEndianWrapping`).
pub const unscaledOf = @import("../../exec/decimal.zig").bigEndianWrapping;

test "unscaledOf reads big-endian two's complement unchecked: past 16 bytes only the last 16 count" {
    try testing.expectEqual(@as(i128, 0), unscaledOf(&.{}));
    try testing.expectEqual(@as(i128, -255), unscaledOf(&.{ 0xFF, 0x01 }));
    try testing.expectEqual(@as(i128, 0x0102), unscaledOf(&.{ 0x01, 0x02 }));
    try testing.expectEqual(@as(i128, std.math.minInt(i128)), unscaledOf(&(.{ 0x7F, 0x80 } ++ .{0x00} ** 15)));
    try testing.expectEqual(@as(i128, 5), unscaledOf(&(.{ 0x01, 0x00 } ++ .{0x00} ** 14 ++ .{0x05})));
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

test "unsigned columns know their width; a UINT64 is DECIMAL(20,0) and a narrower one INT" {
    const u32c = logical(.int32, .{ .integer = .{ .bit_width = 32, .signed = false } });
    try testing.expectEqual(@as(?u8, 32), unsignedBits(u32c));
    try testing.expectEqual(@as(?u8, null), unsignedBits(logical(.int32, .{ .integer = .{ .bit_width = 32, .signed = true } })));
    try testing.expectEqual(@as(?u8, 16), unsignedBits(.{ .ty = .int32, .converted_type = 12 }));
    try testing.expectEqual(@as(?u8, null), unsignedBits(.{ .ty = .int32 }));
    const u64c = logical(.int64, .{ .integer = .{ .bit_width = 64, .signed = false } });
    try testing.expectEqual(@as(?u8, 64), unsignedBits(u64c));
    const dec = try basaltType(u64c);
    try testing.expectEqual(types.TypeKind.decimal, dec.kind);
    try testing.expectEqual(@as(u8, 20), dec.precision);
    try testing.expectEqual(@as(u8, 0), dec.scale);
    try testing.expectEqual(types.TypeKind.decimal, (try basaltType(.{ .ty = .int64, .converted_type = 14 })).kind);
    try testing.expectEqual(types.TypeKind.int, (try basaltType(u32c)).kind);
}

fn micros(t: civil.Tick, x: i64) i64 {
    return t.toMicros(x, .saturate);
}

test "temporal tick converts millisecond columns and leaves micros alone" {
    try testing.expectEqual(civil.Tick.ms, temporalTick(.{ .converted_type = 9 }));
    try testing.expectEqual(civil.Tick.ms, temporalTick(.{ .converted_type = 7 }));
    try testing.expectEqual(civil.Tick.us, temporalTick(.{ .converted_type = 10 }));
    try testing.expectEqual(civil.Tick.us, temporalTick(.{}));

    try testing.expectEqual(@as(i64, 1_583_298_367_123_000), micros(.ms, 1_583_298_367_123));
    try testing.expectEqual(@as(i64, 1_583_298_367_123), micros(.us, 1_583_298_367_123));
}

test "civil.Tick saturating micros equal the old TemporalScale on every unit and edge" {
    const old = struct {
        fn oldScaleApply(mul: i64, div: i64, x: i64) i64 {
            const m = std.math.mul(i64, x, mul) catch
                return if (x < 0) std.math.minInt(i64) else std.math.maxInt(i64);
            return if (div == 1) m else @divFloor(m, div);
        }
    };
    const max = std.math.maxInt(i64);
    const min = std.math.minInt(i64);
    const xs = [_]i64{ 0, 1, -1, 999, -999, 1000, -1000, 1001, -1001, 1999, -1_001, max, min, max - 1, min + 1, max / 1000, max / 1000 + 1, max / 1000 - 1, min / 1000, min / 1000 - 1, min / 1000 + 1, max / 10, 1_583_298_367_123, -1_583_298_367_123 };
    const units = [_]struct { parquet.TimeUnit, i64, i64 }{ .{ .millis, 1000, 1 }, .{ .micros, 1, 1 }, .{ .nanos, 1, 1000 } };
    for (units) |u| for (xs) |x| {
        const e = logical(.int64, .{ .timestamp = .{ .unit = u[0] } });
        try testing.expectEqual(old.oldScaleApply(u[1], u[2], x), micros(temporalTick(e), x));
    };
}

test "a LogicalType-only TIMESTAMP reads as timestamp in every unit" {
    const want: i64 = 1_767_268_800_000_000;
    inline for (.{
        .{ parquet.TimeUnit.millis, want / 1000 },
        .{ parquet.TimeUnit.micros, want },
        .{ parquet.TimeUnit.nanos, want * 1000 },
    }) |c| {
        const e = logical(.int64, .{ .timestamp = .{ .unit = c[0] } });
        try testing.expectEqual(types.TypeKind.timestamp, (try basaltType(e)).kind);
        try testing.expectEqual(want, micros(temporalTick(e), c[1]));
    }
    const utc = logical(.int64, .{ .timestamp = .{ .adjusted_to_utc = true, .unit = .nanos } });
    try testing.expectEqual(types.TypeKind.timestamp, (try basaltType(utc)).kind);
}

test "nanoseconds floor to micros, including before 1970; millis past i64 saturate (D2 pending)" {
    try testing.expectEqual(@as(i64, 1), micros(.ns, 1_999));
    try testing.expectEqual(@as(i64, -1), micros(.ns, -1));
    try testing.expectEqual(@as(i64, -2), micros(.ns, -1_001));
    try testing.expectEqual(@as(i64, std.math.maxInt(i64)), micros(.ms, std.math.maxInt(i64) / 10));
    try testing.expectEqual(@as(i64, std.math.minInt(i64)), micros(.ms, std.math.minInt(i64) / 1000 - 1));
}

test "a LogicalType-only TIME reads as time in millis and micros" {
    const want: i64 = 3_720_000_000;
    const ms = logical(.int32, .{ .time = .{ .unit = .millis } });
    try testing.expectEqual(types.TypeKind.time, (try basaltType(ms)).kind);
    try testing.expectEqual(want, micros(temporalTick(ms), want / 1000));
    const us = logical(.int64, .{ .time = .{ .unit = .micros } });
    try testing.expectEqual(types.TypeKind.time, (try basaltType(us)).kind);
    try testing.expectEqual(want, micros(temporalTick(us), want));
    const ns = logical(.int64, .{ .time = .{ .unit = .nanos } });
    try testing.expectEqual(types.TypeKind.time, (try basaltType(ns)).kind);
    try testing.expectEqual(want, micros(temporalTick(ns), want * 1000));
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
    try testing.expectEqual(civil.Tick.ms, temporalTick(both));

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
