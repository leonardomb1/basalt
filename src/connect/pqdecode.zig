//! Parquet value decoding: levels, encodings, and page-to-column assembly.
//!
//! A decompressed page is still encoded. This module turns those bytes into
//! `Value`s and builds a basalt `Column` from them.
//!
//! A struct's fields read as flat dotted columns (`addr.city`). A list of
//! scalars — any depth, so a list of lists too — reads as one `string` column
//! of JSON arrays named for the list, assembled from repetition and definition
//! levels, which `JSON_EACH` and `json_get` take apart. A list of structs, a
//! map, and any nesting of them spread over several leaves and are assembled
//! from all of them into one JSON column — objects for structs and maps,
//! arrays for lists.
//!
//! Encodings handled: PLAIN, and the RLE/bit-packed hybrid used for both
//! definition levels and dictionary indices (`PLAIN_DICTIONARY` and
//! `RLE_DICTIONARY` share a wire format). The DELTA_* family is reported as
//! unsupported rather than guessed at.

const std = @import("std");
const parquet = @import("parquet.zig");
const types = @import("../lang/types.zig");
const column = @import("../exec/column.zig");
pub const Threshold = @import("../exec/value.zig").Threshold;
const Value = @import("../exec/value.zig").Value;
const Decimal = @import("../exec/value.zig").Decimal;
const eval = @import("../exec/eval.zig");

pub const Error = error{
    CorruptParquetPage,
    /// Nested (list/map/struct) columns; basalt has no type for them.
    UnsupportedParquetSchema,
    /// A DELTA_* or BYTE_STREAM_SPLIT page.
    UnsupportedParquetEncoding,
} || std.mem.Allocator.Error;

// --- bit-level readers -------------------------------------------------------

/// LSB-first bit reader, the order Parquet's bit-packing uses.
///
/// Reads a whole 64-bit word and shifts, rather than looping one bit at a time.
/// Dictionary indices and delta miniblocks use widths up to 32, where the
/// per-bit loop costs 5-12x more; definition levels (width 1) are unaffected.
pub const BitReader = struct {
    buf: []const u8,
    bit_pos: usize = 0,

    /// `u7`, not `u6`: delta miniblocks over INT64 may legally use width 64,
    /// which a `u6` cannot even name — the old signature turned that page into
    /// an @intCast panic in the caller.
    pub fn read(self: *BitReader, width: u7) Error!u64 {
        if (width == 0) return 0;
        const end = self.bit_pos + width;
        if ((end + 7) >> 3 > self.buf.len) return Error.CorruptParquetPage;

        const byte = self.bit_pos >> 3;
        const shift: u6 = @intCast(self.bit_pos & 7);
        self.bit_pos = end;

        // Fast path: offset + width fit one u64, which every hot width does
        // (dictionary indices and levels are <= 32, so shift + width <= 39).
        if (@as(usize, shift) + width <= 64) {
            var word: u64 = 0;
            if (byte + 8 <= self.buf.len) {
                word = std.mem.readInt(u64, self.buf[byte..][0..8], .little);
            } else {
                // Tail: fewer than 8 bytes remain, so assemble what is there.
                var k: usize = 0;
                while (byte + k < self.buf.len and k < 8) : (k += 1) {
                    word |= @as(u64, self.buf[byte + k]) << @intCast(8 * k);
                }
            }
            const v = word >> shift;
            return if (width == 64) v else v & ((@as(u64, 1) << @intCast(width)) - 1);
        }

        // Wide value at a non-zero bit offset: the bits span two u64 words.
        // One word used to be masked as if it held them all, so widths above
        // 64-shift returned a value with its top bits silently zeroed.
        var word: u128 = 0;
        var k: usize = 0;
        while (byte + k < self.buf.len and k < 9) : (k += 1) {
            word |= @as(u128, self.buf[byte + k]) << @intCast(8 * k);
        }
        const v: u64 = @truncate(word >> shift);
        return if (width == 64) v else v & ((@as(u64, 1) << @intCast(width)) - 1);
    }
};

/// Bits needed to hold values up to `max` — the width Parquet uses for levels.
pub fn bitWidth(max: u32) u6 {
    if (max == 0) return 0;
    return @intCast(32 - @clz(max));
}

/// Decodes `count` values from an RLE / bit-packed hybrid stream.
///
/// The stream is a sequence of runs, each introduced by a varint header whose
/// low bit selects the kind: set means a bit-packed run of `(header >> 1) * 8`
/// values, clear means an RLE run of `header >> 1` copies of one value.
pub fn decodeRleHybrid(
    arena: std.mem.Allocator,
    src: []const u8,
    width: u6,
    count: usize,
) Error![]u32 {
    const out = try arena.alloc(u32, count);
    if (count == 0) return out;
    if (width == 0) {
        @memset(out, 0);
        return out;
    }

    var pos: usize = 0;
    var n: usize = 0;
    while (n < count) {
        const header = try readVarint(src, &pos);
        if (header & 1 == 1) {
            // bit-packed: header >> 1 groups of eight values. The count comes
            // straight off the wire, so both products can wrap before anything
            // is bounds-checked against the page.
            const groups = std.math.cast(usize, header >> 1) orelse return Error.CorruptParquetPage;
            const bytes = std.math.mul(usize, groups, width) catch return Error.CorruptParquetPage;
            if (bytes > src.len - pos) return Error.CorruptParquetPage;
            const vals = groups * 8; // groups <= src.len here, so this cannot wrap
            const run = src[pos..][0..bytes];
            const take = @min(vals, count - n); // trailing padding in the last group
            // The run's bytes are checked above, so values with a whole u64 after
            // their first byte unpack without a check apiece; reading them one at a
            // time through `BitReader` was a fifth of a 100-group GROUP BY. The
            // last few go through it.
            const mask = (@as(u64, 1) << width) - 1;
            var i: usize = 0;
            while (width <= 32 and i < take) : (i += 1) {
                const bit = i * width;
                const byte = bit >> 3;
                if (byte + 8 > run.len) break;
                out[n + i] = @intCast((std.mem.readInt(u64, run[byte..][0..8], .little) >> @intCast(bit & 7)) & mask);
            }
            var br = BitReader{ .buf = run, .bit_pos = i * width };
            while (i < take) : (i += 1) out[n + i] = @intCast(try br.read(width));
            n += take;
            pos += bytes;
        } else {
            const run: usize = @intCast(header >> 1);
            if (run == 0) return Error.CorruptParquetPage; // no progress
            // the repeated value occupies ceil(width/8) little-endian bytes
            const vb = (@as(usize, width) + 7) / 8;
            if (pos + vb > src.len) return Error.CorruptParquetPage;
            var v: u32 = 0;
            for (0..vb) |k| v |= @as(u32, src[pos + k]) << @intCast(8 * k);
            pos += vb;
            for (0..run) |_| {
                if (n >= count) break;
                out[n] = v;
                n += 1;
            }
        }
    }
    return out;
}

fn readVarint(src: []const u8, pos: *usize) Error!u64 {
    var v: u64 = 0;
    var shift: u6 = 0;
    while (true) {
        if (pos.* >= src.len) return Error.CorruptParquetPage;
        const b = src[pos.*];
        pos.* += 1;
        v |= @as(u64, b & 0x7F) << shift;
        if (b & 0x80 == 0) return v;
        shift = std.math.add(u6, shift, 7) catch return Error.CorruptParquetPage;
    }
}

// --- PLAIN values ------------------------------------------------------------

/// Walks PLAIN-encoded values of one physical type. Byte arrays borrow from the
/// page buffer rather than copying — the column builder dupes on append.
pub const PlainCursor = struct {
    src: []const u8,
    pos: usize = 0,
    ty: parquet.PhysicalType,
    type_length: usize = 0,
    /// BOOLEAN is bit-packed, one bit per value, so it needs its own cursor.
    bits: BitReader = .{ .buf = &.{} },

    pub fn init(ty: parquet.PhysicalType, type_length: i32, src: []const u8) PlainCursor {
        return .{
            .src = src,
            .ty = ty,
            .type_length = if (type_length > 0) @intCast(type_length) else 0,
            .bits = .{ .buf = src },
        };
    }

    pub fn next(self: *PlainCursor) Error!Value {
        switch (self.ty) {
            .boolean => return .{ .bool = (try self.bits.read(1)) != 0 },
            .int32 => return .{ .int = try self.readInt(i32) },
            .int64 => return .{ .int = try self.readInt(i64) },
            .float => {
                const raw = try self.takeInt(u32);
                return .{ .float = @floatCast(@as(f32, @bitCast(raw))) };
            },
            .double => {
                const raw = try self.takeInt(u64);
                return .{ .float = @bitCast(raw) };
            },
            .byte_array => {
                const n: usize = @intCast(try self.takeInt(u32));
                const b = try self.take(n);
                return .{ .bytes = b };
            },
            .fixed_len_byte_array => {
                if (self.type_length == 0) return Error.CorruptParquetPage;
                return .{ .bytes = try self.take(self.type_length) };
            },
            // 12 bytes: 8-byte nanoseconds-of-day then a 4-byte Julian day.
            .int96 => {
                const b = try self.take(12);
                const nanos = std.mem.readInt(u64, b[0..8], .little);
                const jday = std.mem.readInt(u32, b[8..12], .little);
                return .{ .timestamp = int96ToMicros(jday, nanos) };
            },
            else => return Error.UnsupportedParquetEncoding,
        }
    }

    fn take(self: *PlainCursor, n: usize) Error![]const u8 {
        if (self.pos + n > self.src.len) return Error.CorruptParquetPage;
        defer self.pos += n;
        return self.src[self.pos..][0..n];
    }

    fn takeInt(self: *PlainCursor, comptime T: type) Error!T {
        const n = @sizeOf(T);
        const b = try self.take(n);
        return std.mem.readInt(T, b[0..n], .little);
    }

    fn readInt(self: *PlainCursor, comptime T: type) Error!i64 {
        return @intCast(try self.takeInt(T));
    }
};

/// INT96 is a deprecated Spark timestamp: Julian day plus nanoseconds of day.
/// 2440588 is the Julian day of 1970-01-01.
/// The Julian day is a full u32 on the wire: 4.29e9 days of microseconds is
/// 3.7e20, far past i64. Saturating keeps a corrupt file from being undefined
/// behaviour in a release build, where the overflow is not checked.
pub fn int96ToMicros(julian_day: u32, nanos_of_day: u64) i64 {
    const days: i64 = @as(i64, julian_day) - 2_440_588;
    return (days *| 86_400_000_000) +| @as(i64, @intCast(nanos_of_day / 1000));
}

// --- DELTA encodings ---------------------------------------------------------

/// DELTA_BINARY_PACKED: a header, then blocks of miniblocks holding deltas
/// bit-packed against a per-block minimum.
///
/// Layout: `<block size> <miniblocks per block> <total count> <first value>`
/// then per block `<min delta> <bit width per miniblock> <packed miniblocks>`.
/// Values are recovered by running sums, so a single miscount desynchronises
/// everything after it — hence the explicit bounds checks throughout.
pub fn decodeDeltaBinaryPacked(
    arena: std.mem.Allocator,
    src: []const u8,
    count: usize,
) Error![]i64 {
    var pos: usize = 0;
    const block_size: usize = @intCast(try readVarint(src, &pos));
    const miniblocks: usize = @intCast(try readVarint(src, &pos));
    const total: usize = @intCast(try readVarint(src, &pos));
    var value: i64 = try readZigZagAt(src, &pos);

    if (miniblocks == 0 or block_size == 0 or block_size % miniblocks != 0) {
        return Error.CorruptParquetPage;
    }
    const per_mini = block_size / miniblocks;

    const want = @min(count, total);
    const out = try arena.alloc(i64, count);
    if (count == 0) return out;

    var n: usize = 0;
    out[n] = value;
    n += 1;

    while (n < want) {
        const min_delta = try readZigZagAt(src, &pos);
        if (pos + miniblocks > src.len) return Error.CorruptParquetPage;
        const widths = src[pos..][0..miniblocks];
        pos += miniblocks;

        for (widths) |w| {
            if (n >= want) break;
            // A raw page byte: 0..64 are meaningful widths, anything above is
            // a corrupt page — @intCast here was a crash on hostile input.
            if (w > 64) return Error.CorruptParquetPage;
            const width: u7 = @intCast(w);
            const bytes = (per_mini * @as(usize, width) + 7) / 8;
            if (pos + bytes > src.len) return Error.CorruptParquetPage;
            var br = BitReader{ .buf = src[pos..][0..bytes] };
            for (0..per_mini) |_| {
                if (n >= want) break;
                const raw: i64 = @intCast(try br.read(width));
                value +%= min_delta +% raw;
                out[n] = value;
                n += 1;
            }
            pos += bytes;
        }
    }
    // a page may declare more values than the level count asks for
    while (n < count) : (n += 1) out[n] = value;
    return out;
}

fn readZigZagAt(src: []const u8, pos: *usize) Error!i64 {
    const u = try readVarint(src, pos);
    return @as(i64, @bitCast(u >> 1)) ^ -@as(i64, @intCast(u & 1));
}

/// DELTA_LENGTH_BYTE_ARRAY: all lengths delta-packed up front, then the bytes
/// back to back.
pub fn decodeDeltaLengthByteArray(
    arena: std.mem.Allocator,
    src: []const u8,
    count: usize,
) Error![][]const u8 {
    var pos: usize = 0;
    const lens = try decodeDeltaBinaryPackedTracking(arena, src, count, &pos);
    const out = try arena.alloc([]const u8, count);
    var off = pos;
    for (out, lens) |*o, l| {
        const n: usize = @intCast(@max(0, l));
        if (off + n > src.len) return Error.CorruptParquetPage;
        o.* = src[off..][0..n];
        off += n;
    }
    return out;
}

/// DELTA_BYTE_ARRAY: each value shares a prefix with the one before it, so the
/// stream carries prefix lengths, suffix lengths, then the suffix bytes.
pub fn decodeDeltaByteArray(
    arena: std.mem.Allocator,
    src: []const u8,
    count: usize,
) Error![][]const u8 {
    var pos: usize = 0;
    const prefixes = try decodeDeltaBinaryPackedTracking(arena, src, count, &pos);
    var pos2 = pos;
    const suffixes = try decodeDeltaBinaryPackedTracking(arena, src[pos..], count, &pos2);
    var off = pos + pos2;

    const out = try arena.alloc([]const u8, count);
    var prev: []const u8 = "";
    for (out, prefixes, suffixes) |*o, p, sfx| {
        const plen: usize = @intCast(@max(0, p));
        const slen: usize = @intCast(@max(0, sfx));
        if (off + slen > src.len or plen > prev.len) return Error.CorruptParquetPage;
        const buf = try arena.alloc(u8, plen + slen);
        @memcpy(buf[0..plen], prev[0..plen]);
        @memcpy(buf[plen..], src[off..][0..slen]);
        off += slen;
        o.* = buf;
        prev = buf;
    }
    return out;
}

/// `decodeDeltaBinaryPacked` that also reports where the stream ended, so a
/// caller can find the payload that follows it.
fn decodeDeltaBinaryPackedTracking(
    arena: std.mem.Allocator,
    src: []const u8,
    count: usize,
    end: *usize,
) Error![]i64 {
    var pos: usize = 0;
    const block_size: usize = @intCast(try readVarint(src, &pos));
    const miniblocks: usize = @intCast(try readVarint(src, &pos));
    const total: usize = @intCast(try readVarint(src, &pos));
    var value: i64 = try readZigZagAt(src, &pos);
    if (miniblocks == 0 or block_size == 0 or block_size % miniblocks != 0) {
        return Error.CorruptParquetPage;
    }
    const per_mini = block_size / miniblocks;

    const out = try arena.alloc(i64, count);
    const want = @min(count, total);
    var n: usize = 0;
    if (count > 0) {
        out[n] = value;
        n += 1;
    }
    while (n < want) {
        const min_delta = try readZigZagAt(src, &pos);
        if (pos + miniblocks > src.len) return Error.CorruptParquetPage;
        const widths = src[pos..][0..miniblocks];
        pos += miniblocks;
        for (widths) |w| {
            // Same guard as `decodeDeltaBinaryPacked`: a raw page byte, valid
            // only up to 64.
            if (w > 64) return Error.CorruptParquetPage;
            const width: u7 = @intCast(w);
            const bytes = (per_mini * @as(usize, width) + 7) / 8;
            if (pos + bytes > src.len) return Error.CorruptParquetPage;
            if (n < want) {
                var br = BitReader{ .buf = src[pos..][0..bytes] };
                for (0..per_mini) |_| {
                    if (n >= want) break;
                    const raw: i64 = @intCast(try br.read(width));
                    value +%= min_delta +% raw;
                    out[n] = value;
                    n += 1;
                }
            }
            pos += bytes;
        }
    }
    while (n < count) : (n += 1) out[n] = value;
    end.* = pos;
    return out;
}

/// BYTE_STREAM_SPLIT: the bytes of fixed-width values are transposed — every
/// value's first byte, then every second byte, and so on. Regrouping them
/// restores the original little-endian values, which compress far better in
/// that order for floats.
pub fn decodeByteStreamSplit(
    arena: std.mem.Allocator,
    src: []const u8,
    width: usize,
    count: usize,
) Error![]u8 {
    if (count == 0) return &.{};
    if (src.len < width * count) return Error.CorruptParquetPage;
    const out = try arena.alloc(u8, width * count);
    for (0..width) |j| {
        for (0..count) |i| out[i * width + j] = src[j * count + i];
    }
    return out;
}

// --- schema mapping ----------------------------------------------------------

/// `ConvertedType` values that change how a physical type is interpreted.
const conv_utf8 = 0;
const conv_decimal = 5;
const conv_date = 6;
const conv_time_millis = 7;
const conv_time_micros = 8;
const conv_timestamp_millis = 9;
const conv_timestamp_micros = 10;
const conv_json = 19;
const conv_bson = 20;

/// Conversion taking a column's stored temporal unit to basalt's microseconds.
///
/// Parquet stores TIME/TIMESTAMP in milliseconds, microseconds or nanoseconds
/// depending on the annotation; basalt's `time`/`timestamp` are always micros.
/// Ignoring this reads a millisecond timestamp as if it were micros — a silent
/// 1000x error that lands values in 1970 — and a nanosecond one 1000x too far
/// out. Nanoseconds floor-divide, so sub-microsecond digits are dropped and a
/// pre-1970 value still truncates towards the earlier instant; flooring is
/// monotone, which keeps statistics-based pruning sound after conversion.
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

/// basalt type for a leaf schema element, from its physical type and its
/// annotation: the LogicalType when the writer gave one, else the legacy
/// ConvertedType. Modern writers (polars, DuckDB, Spark, pyarrow) omit the
/// converted type for naive and nanosecond timestamps, so the logical type is
/// the only thing that says the int64 is a timestamp at all.
///
/// `isAdjustedToUTC` is not carried: basalt has no zoned timestamp, and a UTC
/// instant and a naive wall-clock time both read as the same `timestamp`
/// value — the UTC one as its UTC wall-clock.
pub fn basaltType(e: parquet.SchemaElement) Error!types.Type {
    const phys = e.ty orelse return Error.UnsupportedParquetSchema;
    var t = logicalBasaltType(e, phys) orelse try convertedBasaltType(e, phys);
    if (e.repetition orelse .required != .required) t = t.asNullable();
    return t;
}

/// Null when the logical type is absent or says nothing basalt acts on, which
/// defers to the converted type. A logical type that does not fit its physical
/// type (a TIMESTAMP on a byte array) is ignored the same way rather than
/// trusted.
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
        // basalt has no uuid type; the 16 raw bytes stay `bytes`, as before
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

/// Rescales a decimal carried as an integer or big-endian byte array.
fn decimalValue(t: types.Type, v: Value) Value {
    return switch (v) {
        .int => |x| .{ .decimal = .{ .unscaled = x, .scale = t.scale } },
        .bytes => |b| blk: {
            // two's-complement big-endian, as Parquet stores DECIMAL bytes
            var acc: i128 = if (b.len > 0 and b[0] & 0x80 != 0) -1 else 0;
            for (b) |byte| acc = (acc << 8) | byte;
            break :blk .{ .decimal = .{ .unscaled = acc, .scale = t.scale } };
        },
        else => v,
    };
}

/// Adapts a decoded physical value to the column's logical type. `scale` carries
/// the temporal unit conversion from `temporalScale`.
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
            // int96 already decodes to micros and carries the identity scale
            .int => |x| .{ .timestamp = scale.apply(x) },
            else => v,
        },
        .decimal => decimalValue(t, v),
        else => v,
    };
}

// --- column chunk assembly ---------------------------------------------------

/// One leaf column of a Parquet schema, resolved against its ancestry.
pub const Leaf = struct {
    /// Index into `FileMetaData.schema`.
    schema_idx: usize,
    /// Index into a row group's `columns`; chunks appear in leaf order,
    /// *including* leaves this reader skips, so the two can diverge.
    chunk_idx: usize,
    /// Dotted path, so a struct field reads as `addr.city` rather than colliding.
    name: []const u8,
    max_def: u32,
    max_rep: u32,
    /// Set for a leaf under a repeated group: a list element, read as JSON text
    /// of the whole list, and `name` is then the list's own (`tags`, not
    /// `tags.list.element`).
    list: ?ListShape = null,
    /// For the same leaves: the schema node their column starts at, and the
    /// levels above it — what a column spanning several leaves (a list of
    /// structs, a map) is assembled from.
    root: ?RootRef = null,

    /// A leaf under a repeated group is a list element: many values per row.
    pub fn isRepeated(self: Leaf) bool {
        return self.max_rep > 0;
    }
};

/// How a list leaf's levels nest: for each repeated ancestor, outermost first,
/// the definition level at which it holds an element. One level below that,
/// the list at that depth exists but is empty; below the outermost one's, the
/// whole column is null for the row.
pub const ListShape = struct {
    rep_def: []const u32,
};

/// Where a repeated leaf's column starts in the schema, and the definition and
/// repetition levels its ancestors above that node contribute.
pub const RootRef = struct { idx: usize, base_def: u32, base_rep: u32 };

const PathNode = struct { name: []const u8, idx: usize, def: u32, repeated: bool, list_group: bool };

/// `LIST` / `MAP` annotations on a group, as the legacy converted type or the
/// logical type carries them.
fn isListGroup(e: parquet.SchemaElement) bool {
    if (e.converted_type) |c| if (c == 1 or c == 2 or c == 3) return true;
    return false;
}

/// Walks the depth-first schema list, resolving each leaf's definition and
/// repetition levels from its ancestors.
///
/// This is what makes a file with nested columns usable: a struct's fields are
/// flat leaves, and a repeated leaf learns which column — the list or map it
/// belongs to — it helps rebuild.
pub fn collectLeaves(arena: std.mem.Allocator, schema: []const parquet.SchemaElement) Error![]Leaf {
    var out = std.array_list.Managed(Leaf).init(arena);
    var path = std.array_list.Managed(PathNode).init(arena);
    if (schema.len == 0) return Error.UnsupportedParquetSchema;
    var pos: usize = 1; // element 0 is the synthetic root
    var chunk: usize = 0;
    const root_children: usize = @intCast(@max(0, schema[0].num_children));
    for (0..root_children) |_| {
        try walkNode(arena, schema, &pos, &chunk, &out, &path, 0, 0);
    }
    return out.toOwnedSlice();
}

fn walkNode(
    arena: std.mem.Allocator,
    schema: []const parquet.SchemaElement,
    pos: *usize,
    chunk: *usize,
    out: *std.array_list.Managed(Leaf),
    path: *std.array_list.Managed(PathNode),
    def: u32,
    rep: u32,
) Error!void {
    if (pos.* >= schema.len) return Error.UnsupportedParquetSchema;
    // the schema is the file's to describe; a hostile one must not recurse forever
    if (path.items.len >= 64) return Error.UnsupportedParquetSchema;
    const e = schema[pos.*];
    const idx = pos.*;
    pos.* += 1;

    const r = e.repetition orelse .required;
    const d2 = def + @as(u32, if (r == .required) 0 else 1);
    const r2 = rep + @as(u32, if (r == .repeated) 1 else 0);
    try path.append(.{ .name = e.name, .idx = idx, .def = d2, .repeated = r == .repeated, .list_group = !e.isLeaf() and isListGroup(e) });
    defer _ = path.pop();

    if (e.isLeaf()) {
        var leaf = Leaf{
            .schema_idx = idx,
            .chunk_idx = chunk.*,
            .name = try joinPath(arena, path.items),
            .max_def = d2,
            .max_rep = r2,
        };
        if (r2 > 0) {
            // The list is named where it starts: at a LIST/MAP-annotated group
            // wrapping the first repeated node (the three-level layout every
            // modern writer uses), else at that repeated node itself (the
            // legacy two-level one, `repeated int32 xs`).
            var first: usize = 0;
            while (!path.items[first].repeated) first += 1;
            const root = if (first > 0 and path.items[first - 1].list_group) first - 1 else first;
            leaf.name = try joinPath(arena, path.items[0 .. root + 1]);
            const rep_def = try arena.alloc(u32, r2);
            var k: usize = 0;
            for (path.items) |pn| if (pn.repeated) {
                rep_def[k] = pn.def;
                k += 1;
            };
            leaf.list = .{ .rep_def = rep_def };
            var base_rep: u32 = 0;
            for (path.items[0..root]) |pn| if (pn.repeated) {
                base_rep += 1;
            };
            leaf.root = .{
                .idx = path.items[root].idx,
                .base_def = if (root > 0) path.items[root - 1].def else 0,
                .base_rep = base_rep,
            };
        }
        try out.append(leaf);
        chunk.* += 1;
        return;
    }
    const n: usize = @intCast(@max(0, e.num_children));
    for (0..n) |_| try walkNode(arena, schema, pos, chunk, out, path, d2, r2);
}

/// `a.b.c` from the names along a path.
fn joinPath(arena: std.mem.Allocator, nodes: []const PathNode) ![]const u8 {
    var buf = std.array_list.Managed(u8).init(arena);
    for (nodes, 0..) |pn, i| {
        if (i > 0) try buf.append('.');
        try buf.appendSlice(pn.name);
    }
    return buf.toOwnedSlice();
}

/// Decodes one column chunk into a `Column`.
///
/// Walks the chunk's pages in order: a dictionary page, if present, populates
/// the dictionary that later data pages index into. Only data page v1 is
/// handled; v2 moves the levels outside the compressed region and is rejected
/// rather than mis-parsed.
pub fn readColumnChunk(
    arena: std.mem.Allocator,
    file_bytes: []const u8,
    meta: parquet.ColumnMetaData,
    elem: parquet.SchemaElement,
    rows: usize,
    max_def: u32,
    /// Offset of `file_bytes[0]` within the file. Zero when the caller passed a
    /// slice that already starts at the chunk, as the ranged reader does.
    base_offset: u64,
) (Error || parquet.Error || @import("codec.zig").Error)!column.Column {
    return readColumnChunkLevels(arena, file_bytes, meta, elem, rows, max_def, 0, null, base_offset);
}

/// `readColumnChunk` for any leaf, a list element included: with `list` set, the
/// chunk's entries — one per level pair, not one per row — are assembled into a
/// JSON array per row.
pub fn readColumnChunkLevels(
    arena: std.mem.Allocator,
    file_bytes: []const u8,
    meta: parquet.ColumnMetaData,
    elem: parquet.SchemaElement,
    rows: usize,
    max_def: u32,
    max_rep: u32,
    list: ?ListShape,
    base_offset: u64,
) (Error || parquet.Error || @import("codec.zig").Error)!column.Column {
    const ty = (try basaltType(elem)).asNullable();
    const tscale = temporalScale(elem);

    // A list's pages count level entries: several per row, or one for an empty
    // or null list. The chunk's total says when they are all in.
    const entries = if (list != null) std.math.cast(usize, meta.num_values) orelse return Error.CorruptParquetPage else rows;
    var levels: ?Levels = if (list != null) .{
        .reps = std.array_list.Managed(u32).init(arena),
        .defs = std.array_list.Managed(u32).init(arena),
    } else null;

    var b = try column.Builder.initCapacity(arena, ty, entries);
    var dict: ?[]Value = null;
    // offsets come from the footer: a negative one, or one before the slice,
    // is a corrupt file rather than a cast to trap on
    const at = std.math.cast(u64, meta.startOffset()) orelse return Error.CorruptParquetPage;
    if (at < base_offset) return Error.CorruptParquetPage;
    var offset: usize = std.math.cast(usize, at - base_offset) orelse return Error.CorruptParquetPage;
    var produced: usize = 0;

    while (produced < entries) {
        if (offset >= file_bytes.len) return Error.CorruptParquetPage;
        const pg = try parquet.readPage(arena, file_bytes, offset, meta.compression);
        offset = pg.next_offset;

        switch (pg.header.ty) {
            .dictionary_page => {
                // dictionary entries are always PLAIN, whatever the data pages use
                const n = std.math.cast(usize, pg.header.num_values) orelse return Error.CorruptParquetPage;
                const vals = try arena.alloc(Value, n);
                var cur = PlainCursor.init(meta.ty, elem.type_length orelse 0, pg.data);
                for (vals) |*v| v.* = try cur.next();
                dict = vals;
            },
            .data_page, .data_page_v2 => {
                produced += try appendDataPage(arena, &b, pg, meta, elem, ty, max_def, dict, tscale, max_rep, if (levels) |*l| l else null);
            },
            .index_page => {}, // not data; skip
            else => return Error.CorruptParquetPage,
        }
    }
    const col = try b.finish();
    if (list) |shape| return assembleLists(arena, col, levels.?.reps.items, levels.?.defs.items, max_def, shape, rows);
    return col;
}

/// A repeated leaf's chunk as entries: one value per level pair (null where the
/// definition level falls short), with the pair itself.
pub const Entries = struct { vals: column.Column, reps: []const u32, defs: []const u32 };

pub fn readEntries(
    arena: std.mem.Allocator,
    file_bytes: []const u8,
    meta: parquet.ColumnMetaData,
    elem: parquet.SchemaElement,
    max_def: u32,
    max_rep: u32,
    base_offset: u64,
) (Error || parquet.Error || @import("codec.zig").Error)!Entries {
    const ty = (try basaltType(elem)).asNullable();
    const tscale = temporalScale(elem);
    const entries = std.math.cast(usize, meta.num_values) orelse return Error.CorruptParquetPage;
    var levels = Levels{
        .reps = std.array_list.Managed(u32).init(arena),
        .defs = std.array_list.Managed(u32).init(arena),
    };
    var b = try column.Builder.initCapacity(arena, ty, entries);
    var dict: ?[]Value = null;
    // offsets come from the footer: a negative one, or one before the slice,
    // is a corrupt file rather than a cast to trap on
    const at = std.math.cast(u64, meta.startOffset()) orelse return Error.CorruptParquetPage;
    if (at < base_offset) return Error.CorruptParquetPage;
    var offset: usize = std.math.cast(usize, at - base_offset) orelse return Error.CorruptParquetPage;
    var produced: usize = 0;
    while (produced < entries) {
        if (offset >= file_bytes.len) return Error.CorruptParquetPage;
        const pg = try parquet.readPage(arena, file_bytes, offset, meta.compression);
        offset = pg.next_offset;
        switch (pg.header.ty) {
            .dictionary_page => {
                const n = std.math.cast(usize, pg.header.num_values) orelse return Error.CorruptParquetPage;
                const vals = try arena.alloc(Value, n);
                var cur = PlainCursor.init(meta.ty, elem.type_length orelse 0, pg.data);
                for (vals) |*v| v.* = try cur.next();
                dict = vals;
            },
            .data_page, .data_page_v2 => {
                produced += try appendDataPage(arena, &b, pg, meta, elem, ty, max_def, dict, tscale, max_rep, &levels);
            },
            .index_page => {},
            else => return Error.CorruptParquetPage,
        }
    }
    const vals = try b.finish();
    if (vals.len != levels.reps.items.len or vals.len != levels.defs.items.len) return Error.CorruptParquetPage;
    return .{ .vals = vals, .reps = levels.reps.items, .defs = levels.defs.items };
}

// --- nested columns ----------------------------------------------------------
//
// A column that spans several leaves — a list of structs, a map, a struct
// holding lists — is rebuilt row by row from all of its leaves' entries, as
// JSON. Within one row every leaf's entries are contiguous (a repetition level
// of 0 starts the next row), and the schema subtree says how to cut them
// further: a repeated node at repetition level R starts a new element at every
// entry whose level is R; a definition level below a node's own says the node
// is absent — null if it is optional, an empty list if it is the repeated one.
// Every leaf carries at least one entry for every instance of every ancestor,
// so the first entry of a node's first leaf answers "is this node here".

/// One node of a nested column's schema subtree.
pub const NNode = struct {
    name: []const u8,
    /// Definition level when this node is present.
    def: u32,
    /// Repetition level of this node's elements (for a repeated node).
    rep: u32,
    optional: bool,
    repeated: bool,
    kind: Kind,
    children: []NNode = &.{},
    /// This node's leaves are `first_leaf .. first_leaf + nleaves` of the column's.
    first_leaf: usize,
    nleaves: usize,
    /// For a leaf: the value's declared max definition level.
    max_def: u32 = 0,

    pub const Kind = enum { leaf, group, list, map };
};

/// A column assembled from several leaves.
pub const Nested = struct {
    name: []const u8,
    root: NNode,
    /// Indices into `Reader.leaves`, in the subtree's depth-first order.
    leaves: []const usize,
};

/// The subtree at `pos`, levels counted on from `def`/`rep`. Leaves are numbered
/// in depth-first order, the order their chunks appear in.
fn buildNode(arena: std.mem.Allocator, schema: []const parquet.SchemaElement, pos: *usize, def: u32, rep: u32, next_leaf: *usize, depth: usize) Error!NNode {
    if (pos.* >= schema.len or depth > 64) return Error.UnsupportedParquetSchema;
    const e = schema[pos.*];
    pos.* += 1;
    const r = e.repetition orelse .required;
    const d2 = def + @as(u32, if (r == .required) 0 else 1);
    const r2 = rep + @as(u32, if (r == .repeated) 1 else 0);
    var node = NNode{
        .name = e.name,
        .def = d2,
        .rep = r2,
        .optional = r == .optional,
        .repeated = r == .repeated,
        .kind = .leaf,
        .first_leaf = next_leaf.*,
        .nleaves = 0,
    };
    if (e.isLeaf()) {
        node.max_def = d2;
        node.nleaves = 1;
        next_leaf.* += 1;
        return node;
    }
    const n: usize = @intCast(@max(0, e.num_children));
    const kids = try arena.alloc(NNode, n);
    for (kids) |*k| k.* = try buildNode(arena, schema, pos, d2, r2, next_leaf, depth + 1);
    node.children = kids;
    node.nleaves = next_leaf.* - node.first_leaf;
    const conv = e.converted_type orelse -1;
    // LIST and MAP only as the spec lays them out — a single repeated child —
    // and otherwise as the plain group the file says they are
    node.kind = if (n == 1 and kids[0].repeated and conv == 3)
        .list
    else if (n == 1 and kids[0].repeated and (conv == 1 or conv == 2) and kids[0].children.len == 2)
        .map
    else
        .group;
    return node;
}

/// The subtree a nested column starts at.
pub fn buildNested(arena: std.mem.Allocator, schema: []const parquet.SchemaElement, root: RootRef) Error!NNode {
    var pos = root.idx;
    var next: usize = 0;
    return buildNode(arena, schema, &pos, root.base_def, root.base_rep, &next, 0);
}

/// A leaf's entries within one instance of some node: `lo .. hi`.
const Span = struct { lo: usize, hi: usize };

const Assembler = struct {
    arena: std.mem.Allocator,
    entries: []const Entries,
    out: *std.array_list.Managed(u8),

    fn firstDef(self: *const Assembler, n: *const NNode, spans: []const Span) Error!u32 {
        const s = spans[n.first_leaf];
        if (s.lo >= s.hi) return Error.CorruptParquetPage;
        return self.entries[n.first_leaf].defs[s.lo];
    }

    /// The node as it appears in its parent: a repeated one as the array of its
    /// elements, anything else as its value.
    fn field(self: *Assembler, n: *const NNode, spans: []const Span) anyerror!void {
        if (n.repeated) return self.array(n, spans, elementOf(n, true));
        return self.value(n, spans);
    }

    /// A non-repeated node's value.
    fn value(self: *Assembler, n: *const NNode, spans: []const Span) anyerror!void {
        if (n.optional and try self.firstDef(n, spans) < n.def) return self.out.appendSlice("null");
        switch (n.kind) {
            .leaf => {
                const s = spans[n.first_leaf];
                if (s.hi - s.lo != 1) return Error.CorruptParquetPage;
                const e = self.entries[n.first_leaf];
                try jsonValue(self.arena, self.out, if (e.defs[s.lo] < n.max_def) .null else e.vals.getValue(s.lo));
            },
            .group => try self.object(n, spans),
            .list => try self.array(&n.children[0], spans, elementOf(&n.children[0], false)),
            .map => try self.mapObject(&n.children[0], spans),
        }
    }

    fn object(self: *Assembler, n: *const NNode, spans: []const Span) anyerror!void {
        try self.out.append('{');
        for (n.children, 0..) |*c, i| {
            if (i > 0) try self.out.append(',');
            try appendJsonString(self.arena, self.out, c.name);
            try self.out.append(':');
            try self.field(c, spans);
        }
        try self.out.append('}');
    }

    /// What an element of repeated node `rep` is: the node itself for a bare
    /// repeated field or a list whose repeated group holds several fields (or is
    /// named `array` / `*_tuple`, the legacy two-level spellings); else, in the
    /// three-level layout, its one child.
    fn elementOf(rep: *const NNode, bare: bool) ?*const NNode {
        if (bare or rep.kind == .leaf or rep.children.len != 1) return null;
        if (std.mem.eql(u8, rep.name, "array") or std.mem.endsWith(u8, rep.name, "_tuple")) return null;
        return &rep.children[0];
    }

    /// The instances of repeated node `rep` within `spans`, as a JSON array: one
    /// element per entry of its first leaf at `rep.rep` (the first entry opens
    /// the first), none when that entry's definition stops short of `rep.def`.
    fn array(self: *Assembler, rep: *const NNode, spans: []const Span, element: ?*const NNode) anyerror!void {
        if (try self.firstDef(rep, spans) < rep.def) return self.out.appendSlice("[]");
        try self.out.append('[');
        var count: ?usize = null;
        const cut = try self.arena.alloc([]Span, rep.nleaves);
        for (cut, 0..) |*c, k| {
            const li = rep.first_leaf + k;
            c.* = try self.split(li, spans[li], rep.rep);
            if (count) |n| {
                if (n != c.len) return Error.CorruptParquetPage;
            } else count = c.len;
        }
        const sub = try self.arena.dupe(Span, spans);
        for (0..count.?) |i| {
            if (i > 0) try self.out.append(',');
            for (cut, 0..) |c, k| sub[rep.first_leaf + k] = c[i];
            if (element) |el| {
                try self.field(el, sub);
            } else {
                // the repeated node is the element: its value, present by now
                var as_value = rep.*;
                as_value.repeated = false;
                as_value.optional = false;
                try self.value(&as_value, sub);
            }
        }
        try self.out.append(']');
    }

    /// A MAP's key_value entries as an object keyed by each key's text.
    fn mapObject(self: *Assembler, kv: *const NNode, spans: []const Span) anyerror!void {
        if (try self.firstDef(kv, spans) < kv.def) return self.out.appendSlice("{}");
        try self.out.append('{');
        const cut = try self.arena.alloc([]Span, kv.nleaves);
        var count: ?usize = null;
        for (cut, 0..) |*c, k| {
            const li = kv.first_leaf + k;
            c.* = try self.split(li, spans[li], kv.rep);
            if (count) |n| {
                if (n != c.len) return Error.CorruptParquetPage;
            } else count = c.len;
        }
        const sub = try self.arena.dupe(Span, spans);
        const key = &kv.children[0];
        for (0..count.?) |i| {
            if (i > 0) try self.out.append(',');
            for (cut, 0..) |c, k| sub[kv.first_leaf + k] = c[i];
            // a key is required by the spec; a key that is a group is rendered
            // as its JSON and used as text
            var kbuf = std.array_list.Managed(u8).init(self.arena);
            var ka = Assembler{ .arena = self.arena, .entries = self.entries, .out = &kbuf };
            if (key.kind == .leaf and key.nleaves == 1) {
                const s = sub[key.first_leaf];
                if (s.hi - s.lo != 1) return Error.CorruptParquetPage;
                const e = self.entries[key.first_leaf];
                const kv_val = if (e.defs[s.lo] < key.max_def) Value.null else e.vals.getValue(s.lo);
                try kbuf.appendSlice(if (kv_val == .null) "null" else try eval.valueToString(self.arena, kv_val));
            } else try ka.field(key, sub);
            try appendJsonString(self.arena, self.out, kbuf.items);
            try self.out.append(':');
            try self.field(&kv.children[1], sub);
        }
        try self.out.append('}');
    }

    /// Cut `span` of leaf `li` where a new element at repetition level `r`
    /// begins. Every entry within an instance repeats at `r` or deeper; one
    /// that repeats shallower would belong to another instance.
    fn split(self: *Assembler, li: usize, span: Span, r: u32) Error![]Span {
        const reps = self.entries[li].reps;
        var out = std.array_list.Managed(Span).init(self.arena);
        var start = span.lo;
        var i = span.lo + 1;
        while (i < span.hi) : (i += 1) {
            if (reps[i] < r) return Error.CorruptParquetPage;
            if (reps[i] == r) {
                try out.append(.{ .lo = start, .hi = i });
                start = i;
            }
        }
        try out.append(.{ .lo = start, .hi = span.hi });
        return out.items;
    }
};

fn appendJsonString(arena: std.mem.Allocator, buf: *std.array_list.Managed(u8), s: []const u8) Error!void {
    var aw = std.Io.Writer.Allocating.init(arena);
    std.json.Stringify.encodeJsonString(s, .{}, &aw.writer) catch return error.OutOfMemory;
    try buf.appendSlice(aw.written());
}

/// A nested column's rows as JSON text, from its leaves' entries for one row
/// group.
pub fn assembleNested(arena: std.mem.Allocator, root: *const NNode, entries: []const Entries, rows: usize) anyerror!column.Column {
    if (entries.len != root.nleaves) return Error.CorruptParquetPage;
    // each leaf's row boundaries: its entries at repetition level 0
    const starts = try arena.alloc([]usize, entries.len);
    for (entries, starts) |e, *st| {
        var list = std.array_list.Managed(usize).init(arena);
        for (e.reps, 0..) |r, i| if (r == 0) try list.append(i);
        if (list.items.len != rows or (rows > 0 and list.items[0] != 0)) return Error.CorruptParquetPage;
        try list.append(e.reps.len);
        st.* = list.items;
    }
    var out = try column.Builder.initCapacity(arena, types.Type.init(.string).asNullable(), rows);
    var scratch = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer scratch.deinit();
    const spans = try arena.alloc(Span, entries.len);
    for (0..rows) |row| {
        _ = scratch.reset(.retain_capacity);
        const sa = scratch.allocator();
        for (spans, starts) |*sp, st| sp.* = .{ .lo = st[row], .hi = st[row + 1] };
        var buf = std.array_list.Managed(u8).init(sa);
        var asm_ = Assembler{ .arena = sa, .entries = entries, .out = &buf };
        try asm_.field(root, spans);
        // a whole-row null reads as a null cell, not the text `null`
        if (std.mem.eql(u8, buf.items, "null")) try out.append(.null) else try out.append(.{ .string = buf.items });
    }
    return out.finish();
}

/// Every entry's repetition and definition level, for a list chunk.
const Levels = struct {
    reps: std.array_list.Managed(u32),
    defs: std.array_list.Managed(u32),
};

/// Rows of JSON arrays from a list leaf's entries: `elems` holds one value per
/// entry (null where the entry's level is below `max_def`), `reps` and `defs`
/// its levels. A repetition level of 0 starts a row; `r > 0` is a new element
/// of the list at depth `r`. Below that depth, each level's definition
/// threshold decides whether it holds an element, is an empty list, or — the
/// outermost only, or an element that is itself a list — is null.
fn assembleLists(
    arena: std.mem.Allocator,
    elems: column.Column,
    reps: []const u32,
    defs: []const u32,
    max_def: u32,
    shape: ListShape,
    rows: usize,
) Error!column.Column {
    if (reps.len != defs.len or reps.len != elems.len) return Error.CorruptParquetPage;
    const depth = shape.rep_def.len;
    var out = try column.Builder.initCapacity(arena, types.Type.init(.string).asNullable(), rows);
    var buf = std.array_list.Managed(u8).init(arena);
    var open: usize = 0; // arrays open in the current row
    var in_row = false;
    var row_null = false;
    var done: usize = 0;

    for (reps, defs, 0..) |r, d, i| {
        if (r > depth or d > max_def) return Error.CorruptParquetPage;
        if (r == 0) {
            if (in_row) {
                try finishRow(&out, &buf, &open, row_null);
                done += 1;
            }
            in_row = true;
            row_null = false;
            // below the outermost repeated node's own level, the list is null
            if (d + 1 < shape.rep_def[0]) {
                row_null = true;
                continue;
            }
            try buf.append('[');
            open = 1;
        } else {
            // a repeated entry is an element of the list at depth `r`, so that
            // list holds one; a level saying otherwise is a corrupt page
            if (!in_row or row_null or r > open or d < shape.rep_def[r - 1]) return Error.CorruptParquetPage;
            while (open > r) : (open -= 1) try buf.append(']');
            try buf.append(',');
        }
        // descend from depth `open` as far as this entry's level reaches
        var lvl = open;
        while (true) {
            // no element at this depth: the list here is empty
            if (d < shape.rep_def[lvl - 1]) break;
            if (lvl == depth) {
                try jsonValue(arena, &buf, if (d < max_def) .null else elems.getValue(i));
                break;
            }
            // the element is itself a list: null, or opened one level down
            if (d + 1 < shape.rep_def[lvl]) {
                try buf.appendSlice("null");
                break;
            }
            try buf.append('[');
            lvl += 1;
            open = lvl;
        }
    }
    if (in_row) {
        try finishRow(&out, &buf, &open, row_null);
        done += 1;
    }
    if (done != rows) return Error.CorruptParquetPage;
    return out.finish();
}

fn finishRow(out: *column.Builder, buf: *std.array_list.Managed(u8), open: *usize, row_null: bool) Error!void {
    if (row_null) {
        try out.append(.null);
    } else {
        while (open.* > 0) : (open.* -= 1) try buf.append(']');
        try out.append(.{ .string = buf.items });
    }
    buf.clearRetainingCapacity();
}

/// One list element as JSON: numbers and booleans bare, decimals as their exact
/// digits, text and temporal values quoted as their SQL text.
fn jsonValue(arena: std.mem.Allocator, buf: *std.array_list.Managed(u8), v: Value) Error!void {
    switch (v) {
        .null => try buf.appendSlice("null"),
        .int => |x| try buf.writer().print("{d}", .{x}),
        .bool => |x| try buf.appendSlice(if (x) "true" else "false"),
        .float => |x| if (std.math.isFinite(x)) try buf.writer().print("{d}", .{x}) else try buf.appendSlice("null"),
        .decimal => try buf.appendSlice(eval.valueToString(arena, v) catch return error.OutOfMemory),
        else => {
            var aw = std.Io.Writer.Allocating.init(arena);
            std.json.Stringify.encodeJsonString(eval.valueToString(arena, v) catch return error.OutOfMemory, .{}, &aw.writer) catch return error.OutOfMemory;
            try buf.appendSlice(aw.written());
        },
    }
}

/// Splits a data page into levels and values, then emits rows.
///
/// v1 length-prefixes each RLE level section with four bytes; v2 moves those
/// lengths into the page header and leaves the sections unprefixed. Everything
/// after the levels is the same in both.
fn appendDataPage(
    arena: std.mem.Allocator,
    b: *column.Builder,
    pg: parquet.Page,
    meta: parquet.ColumnMetaData,
    elem: parquet.SchemaElement,
    ty: types.Type,
    max_def: u32,
    dict: ?[]Value,
    tscale: TemporalScale,
    max_rep: u32,
    /// A list leaf's levels, appended entry by entry; null for a flat column.
    levels: ?*Levels,
) Error!usize {
    // A negative count is not a count; @intCast on it is undefined in release.
    const n = std.math.cast(usize, pg.header.num_values) orelse return Error.CorruptParquetPage;
    var body = pg.data;

    var reps: ?[]u32 = null;
    var defs: ?[]u32 = null;
    if (pg.header.ty == .data_page_v2) {
        // v2 keeps both level sections, unprefixed, ahead of the values
        const rl = pg.header.rep_levels_len;
        if (rl > body.len) return Error.CorruptParquetPage;
        if (max_rep > 0 and rl > 0) reps = try decodeRleHybrid(arena, body[0..rl], bitWidth(max_rep), n);
        body = body[rl..];
        const dl = pg.header.def_levels_len;
        if (dl > body.len) return Error.CorruptParquetPage;
        if (max_def > 0 and dl > 0) {
            defs = try decodeRleHybrid(arena, body[0..dl], bitWidth(max_def), n);
        }
        body = body[dl..];
    } else {
        // v1: repetition levels, then definition levels, each length-prefixed
        if (max_rep > 0) {
            if (body.len < 4) return Error.CorruptParquetPage;
            const len: usize = std.mem.readInt(u32, body[0..4], .little);
            if (4 + len > body.len) return Error.CorruptParquetPage;
            reps = try decodeRleHybrid(arena, body[4..][0..len], bitWidth(max_rep), n);
            body = body[4 + len ..];
        }
        if (max_def > 0) {
            if (body.len < 4) return Error.CorruptParquetPage;
            const len: usize = std.mem.readInt(u32, body[0..4], .little);
            if (4 + len > body.len) return Error.CorruptParquetPage;
            defs = try decodeRleHybrid(arena, body[4..][0..len], bitWidth(max_def), n);
            body = body[4 + len ..];
        }
    }
    if (levels) |lv| {
        if (reps) |r| try lv.reps.appendSlice(r) else try lv.reps.appendNTimes(0, n);
        if (defs) |d| try lv.defs.appendSlice(d) else try lv.defs.appendNTimes(max_def, n);
    }

    // how many values are actually stored: nulls occupy a level but no value
    var present: usize = n;
    if (defs) |d| {
        present = 0;
        for (d) |lvl| {
            if (lvl == max_def) present += 1;
        }
    }

    switch (pg.header.encoding) {
        .plain => {
            // Fast path: a page of fixed-width values with no nulls and no unit
            // conversion is just a typed array. Skipping the per-value `Value`
            // round trip is worth a special case on the hottest loop there is.
            //
            // `present == n` rather than `defs == null`: most writers mark every
            // column OPTIONAL, so levels are present even when no row is null,
            // and keying off their absence would never fire in practice.
            if (tscale.isIdentity() and try bulkPlain(arena, b, ty, meta.ty, body, present, defs, max_def)) {
                return n;
            }
            var cur = PlainCursor.init(meta.ty, elem.type_length orelse 0, body);
            try emit(b, ty, defs, max_def, n, &cur, null, null, tscale);
        },
        .byte_stream_split => {
            const width: usize = switch (meta.ty) {
                .float => 4,
                .double => 8,
                .int32 => 4,
                .int64 => 8,
                .fixed_len_byte_array => @intCast(@max(0, elem.type_length orelse 0)),
                else => return Error.UnsupportedParquetEncoding,
            };
            const flat = try decodeByteStreamSplit(arena, body, width, present);
            var cur = PlainCursor.init(meta.ty, elem.type_length orelse 0, flat);
            try emit(b, ty, defs, max_def, n, &cur, null, null, tscale);
        },
        .delta_binary_packed => {
            const vals = try decodeDeltaBinaryPacked(arena, body, present);
            try emitInts(b, ty, defs, max_def, n, vals, tscale);
        },
        .delta_length_byte_array => {
            const vals = try decodeDeltaLengthByteArray(arena, body, present);
            try emitBytes(b, ty, defs, max_def, n, vals, tscale);
        },
        .delta_byte_array => {
            const vals = try decodeDeltaByteArray(arena, body, present);
            try emitBytes(b, ty, defs, max_def, n, vals, tscale);
        },
        // a boolean data page may be RLE rather than PLAIN bit-packing
        .rle => {
            const bits = try decodeRleHybrid(arena, body[@min(4, body.len)..], 1, present);
            const vals = try arena.alloc(i64, present);
            for (vals, bits) |*v, x| v.* = @intCast(x);
            try emitInts(b, ty, defs, max_def, n, vals, tscale);
        },
        .plain_dictionary, .rle_dictionary => {
            const d = dict orelse return Error.CorruptParquetPage;
            if (body.len < 1) return Error.CorruptParquetPage;
            // the index bit width is a single byte ahead of the hybrid stream;
            // parquet caps it at 32, and anything past 63 does not fit the shift
            if (body[0] > 32) return Error.CorruptParquetPage;
            const width: u6 = @intCast(body[0]);
            const idx = try decodeRleHybrid(arena, body[1..], width, present);
            if (try bulkDict(arena, b, ty, d, idx, defs, max_def, tscale)) return n;
            try emit(b, ty, defs, max_def, n, null, idx, d, tscale);
        },
        else => return Error.UnsupportedParquetEncoding,
    }
    return n;
}

/// Decodes a whole PLAIN page of fixed-width values directly into the column's
/// typed store. Returns false when the shape is not one of the handled cases,
/// leaving the caller to take the general path.
fn bulkPlain(
    arena: std.mem.Allocator,
    b: *column.Builder,
    ty: types.Type,
    phys: parquet.PhysicalType,
    body: []const u8,
    present: usize,
    defs: ?[]const u32,
    max_def: u32,
) Error!bool {
    // values are only stored for present rows; nulls occupy a level, not a slot
    const count = present;
    switch (phys) {
        .int64 => {
            if (body.len < count * 8) return Error.CorruptParquetPage;
            switch (ty.kind) {
                // time and timestamp share the i64 store; the caller has already
                // ruled out a unit conversion (identity tscale)
                .int, .time, .timestamp => {
                    const out = try arena.alloc(i64, count);
                    for (out, 0..) |*o, i| o.* = std.mem.readInt(i64, body[i * 8 ..][0..8], .little);
                    if (defs) |d| {
                        b.appendBulkScattered(i64, out, d, max_def) catch return false;
                    } else b.appendBulk(i64, out) catch return false;
                    return true;
                },
                .decimal => {
                    const out = try arena.alloc(Decimal, count);
                    for (out, 0..) |*o, i| o.* = .{ .unscaled = std.mem.readInt(i64, body[i * 8 ..][0..8], .little), .scale = ty.scale };
                    if (defs) |d| {
                        b.appendBulkScattered(Decimal, out, d, max_def) catch return false;
                    } else b.appendBulk(Decimal, out) catch return false;
                    return true;
                },
                else => return false,
            }
        },
        .int32 => {
            if (body.len < count * 4) return Error.CorruptParquetPage;
            switch (ty.kind) {
                .int => {
                    const out = try arena.alloc(i64, count);
                    for (out, 0..) |*o, i| o.* = std.mem.readInt(i32, body[i * 4 ..][0..4], .little);
                    if (defs) |d| {
                        b.appendBulkScattered(i64, out, d, max_def) catch return false;
                    } else b.appendBulk(i64, out) catch return false;
                    return true;
                },
                .date => {
                    const out = try arena.alloc(i32, count);
                    for (out, 0..) |*o, i| o.* = std.mem.readInt(i32, body[i * 4 ..][0..4], .little);
                    if (defs) |d| {
                        b.appendBulkScattered(i32, out, d, max_def) catch return false;
                    } else b.appendBulk(i32, out) catch return false;
                    return true;
                },
                .decimal => {
                    const out = try arena.alloc(Decimal, count);
                    for (out, 0..) |*o, i| o.* = .{ .unscaled = std.mem.readInt(i32, body[i * 4 ..][0..4], .little), .scale = ty.scale };
                    if (defs) |d| {
                        b.appendBulkScattered(Decimal, out, d, max_def) catch return false;
                    } else b.appendBulk(Decimal, out) catch return false;
                    return true;
                },
                else => return false,
            }
        },
        .byte_array => {
            // Strings are slices of the page body: length-prefixed, no copy
            // until the builder's own payload append.
            if (ty.kind != .string and ty.kind != .bytes) return false;
            const vals = try plainByteArrays(arena, body, count);
            b.appendBytesScattered(vals, defs, max_def) catch return false;
            return true;
        },
        .double => {
            if (ty.kind != .float) return false;
            if (body.len < count * 8) return Error.CorruptParquetPage;
            const out = try arena.alloc(f64, count);
            for (out, 0..) |*o, i| o.* = @bitCast(std.mem.readInt(u64, body[i * 8 ..][0..8], .little));
            if (defs) |d| {
                b.appendBulkScattered(f64, out, d, max_def) catch return false;
            } else b.appendBulk(f64, out) catch return false;
            return true;
        },
        .float => {
            if (ty.kind != .float) return false;
            if (body.len < count * 4) return Error.CorruptParquetPage;
            const out = try arena.alloc(f64, count);
            for (out, 0..) |*o, i| {
                const raw = std.mem.readInt(u32, body[i * 4 ..][0..4], .little);
                o.* = @floatCast(@as(f32, @bitCast(raw)));
            }
            if (defs) |d| {
                b.appendBulkScattered(f64, out, d, max_def) catch return false;
            } else b.appendBulk(f64, out) catch return false;
            return true;
        },
        else => return false,
    }
}

/// The `count` length-prefixed values of a PLAIN byte-array page, as slices
/// into the page body.
fn plainByteArrays(arena: std.mem.Allocator, body: []const u8, count: usize) Error![]const []const u8 {
    const out = try arena.alloc([]const u8, count);
    var pos: usize = 0;
    for (out) |*o| {
        if (pos + 4 > body.len) return Error.CorruptParquetPage;
        const len: usize = std.mem.readInt(u32, body[pos..][0..4], .little);
        pos += 4;
        if (pos + len > body.len) return Error.CorruptParquetPage;
        o.* = body[pos..][0..len];
        pos += len;
    }
    return out;
}

/// Expands a dictionary-encoded page straight into the typed store: the
/// dictionary is turned into a flat typed array once, the indices gathered
/// through it. The general `emit` boxed every row's entry into a `Value` —
/// for a low-cardinality string column, the same handful of strings a
/// million times over. Returns false for a shape it does not cover.
fn bulkDict(
    arena: std.mem.Allocator,
    b: *column.Builder,
    ty: types.Type,
    dict: []const Value,
    idx: []const u32,
    defs: ?[]const u32,
    max_def: u32,
    tscale: TemporalScale,
) Error!bool {
    for (idx) |ix| if (ix >= dict.len) return Error.CorruptParquetPage;
    switch (ty.kind) {
        .string, .bytes => {
            for (dict) |v| if (v != .bytes) return false;
            const vals = try arena.alloc([]const u8, idx.len);
            for (vals, idx) |*o, ix| o.* = dict[ix].bytes;
            b.appendBytesScattered(vals, defs, max_def) catch return false;
            return true;
        },
        .int => {
            for (dict) |v| if (v != .int) return false;
            const vals = try arena.alloc(i64, idx.len);
            for (vals, idx) |*o, ix| o.* = dict[ix].int;
            if (defs) |d| {
                b.appendBulkScattered(i64, vals, d, max_def) catch return false;
            } else b.appendBulk(i64, vals) catch return false;
            return true;
        },
        .float => {
            for (dict) |v| if (v != .float) return false;
            const vals = try arena.alloc(f64, idx.len);
            for (vals, idx) |*o, ix| o.* = dict[ix].float;
            if (defs) |d| {
                b.appendBulkScattered(f64, vals, d, max_def) catch return false;
            } else b.appendBulk(f64, vals) catch return false;
            return true;
        },
        .date => {
            if (!tscale.isIdentity()) return false;
            for (dict) |v| if (v != .int) return false;
            const vals = try arena.alloc(i32, idx.len);
            for (vals, idx) |*o, ix| o.* = std.math.cast(i32, dict[ix].int) orelse return false;
            if (defs) |d| {
                b.appendBulkScattered(i32, vals, d, max_def) catch return false;
            } else b.appendBulk(i32, vals) catch return false;
            return true;
        },
        else => return false,
    }
}

/// Emits integer-shaped decoded values, interleaving nulls by definition level.
fn emitInts(
    b: *column.Builder,
    ty: types.Type,
    defs: ?[]const u32,
    max_def: u32,
    n: usize,
    vals: []const i64,
    tscale: TemporalScale,
) Error!void {
    var j: usize = 0;
    for (0..n) |i| {
        if (if (defs) |d| d[i] != max_def else false) {
            try b.append(.null);
            continue;
        }
        if (j >= vals.len) return Error.CorruptParquetPage;
        const v: Value = if (ty.kind == .bool) .{ .bool = vals[j] != 0 } else .{ .int = vals[j] };
        j += 1;
        try b.append(coerce(ty, v, tscale));
    }
}

/// Emits byte-array-shaped decoded values, interleaving nulls.
fn emitBytes(
    b: *column.Builder,
    ty: types.Type,
    defs: ?[]const u32,
    max_def: u32,
    n: usize,
    vals: []const []const u8,
    tscale: TemporalScale,
) Error!void {
    if (ty.kind == .string or ty.kind == .bytes) {
        // A short `vals` is the only non-memory failure: the page lied.
        b.appendBytesScattered(vals, defs, max_def) catch |e| switch (e) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return Error.CorruptParquetPage,
        };
        return;
    }
    var j: usize = 0;
    for (0..n) |i| {
        if (if (defs) |d| d[i] != max_def else false) {
            try b.append(.null);
            continue;
        }
        if (j >= vals.len) return Error.CorruptParquetPage;
        const v = Value{ .bytes = vals[j] };
        j += 1;
        try b.append(coerce(ty, v, tscale));
    }
}

/// Interleaves nulls with values: a row whose definition level is below the
/// maximum has no stored value, so the cursor/index stream must not advance.
fn emit(
    b: *column.Builder,
    ty: types.Type,
    defs: ?[]const u32,
    max_def: u32,
    n: usize,
    cur: ?*PlainCursor,
    idx: ?[]const u32,
    dict: ?[]const Value,
    tscale: TemporalScale,
) Error!void {
    var j: usize = 0;
    for (0..n) |i| {
        const is_null = if (defs) |d| d[i] != max_def else false;
        if (is_null) {
            try b.append(.null);
            continue;
        }
        const v: Value = if (cur) |c|
            try c.next()
        else blk: {
            const ix = idx.?;
            const dv = dict.?;
            if (j >= ix.len or ix[j] >= dv.len) return Error.CorruptParquetPage;
            break :blk dv[ix[j]];
        };
        j += 1;
        try b.append(coerce(ty, v, tscale));
    }
}

// --- row group filtering -----------------------------------------------------

/// A simple `column <op> literal` bound, the shape a row-group filter can use.
pub const Bound = struct {
    column: []const u8,
    op: Op,
    value: Value,

    pub const Op = enum { lt, le, gt, ge, eq };
};

/// Whether a row group can possibly satisfy `bounds`, judged from its
/// statistics alone.
///
/// Conservative in one direction only: a group is skipped solely when its
/// statistics *prove* no row can match. Missing or unreadable statistics always
/// mean "keep", so this can never drop matching rows.
pub fn groupMayMatch(
    schema: []const parquet.SchemaElement,
    leaves: []const Leaf,
    g: parquet.RowGroup,
    bounds: []const Bound,
) bool {
    for (bounds) |b| {
        const lf = findLeaf(leaves, b.column) orelse continue;
        if (lf.chunk_idx >= g.columns.len) continue;
        const meta = g.columns[lf.chunk_idx].meta orelse continue;
        const elem = schema[lf.schema_idx];

        const lo = statValue(elem, meta.ty, meta.stats.min) orelse continue;
        const hi = statValue(elem, meta.ty, meta.stats.max) orelse continue;

        // An unorderable pair is "unknown", which must never exclude.
        const excluded = switch (b.op) {
            // every value is >= lo, so `col < v` is impossible when lo >= v
            .lt => (cmp(lo, b.value) orelse .lt) != .lt,
            .le => (cmp(lo, b.value) orelse .eq) == .gt,
            .gt => (cmp(hi, b.value) orelse .gt) != .gt,
            .ge => (cmp(hi, b.value) orelse .eq) == .lt,
            .eq => (cmp(b.value, lo) orelse .eq) == .lt or (cmp(b.value, hi) orelse .eq) == .gt,
        };
        if (excluded) return false;
    }
    return true;
}

/// Whether a row group could hold a row that enters the current top-N.
///
/// Conservative by construction: unknown column, missing statistics, or an
/// unfilled heap all return true. Only a proven strict miss skips.
pub fn groupBeatsThreshold(
    schema: []const parquet.SchemaElement,
    leaves: []const Leaf,
    g: parquet.RowGroup,
    t: Threshold,
) bool {
    if (!t.full or t.value == .null) return true;
    const lf = findLeaf(leaves, t.column) orelse return true;
    if (lf.chunk_idx >= g.columns.len) return true;
    const meta = g.columns[lf.chunk_idx].meta orelse return true;
    const elem = schema[lf.schema_idx];

    // A null sorts last, so a group holding any null can still matter when the
    // bound itself is null — but that case already returned above.
    if (t.desc) {
        const hi = statValue(elem, meta.ty, meta.stats.max) orelse return true;
        return cmp(hi, t.value) == .gt;
    }
    const lo = statValue(elem, meta.ty, meta.stats.min) orelse return true;
    return cmp(lo, t.value) == .lt;
}

pub const MinMax = struct { min: Value, max: Value };

/// Whole-file min/max for one column, folded from row-group statistics without
/// reading a single page. Returns null the moment any row group is missing the
/// statistic, so the caller scans rather than reporting a partial answer.
pub fn fileMinMax(rdr: *const Reader, name: []const u8) ?MinMax {
    if (rdr.md.row_groups.len == 0) return null;
    const lf = findLeaf(rdr.leaves, name) orelse return null;
    const elem = rdr.md.schema[lf.schema_idx];
    var lo: ?Value = null;
    var hi: ?Value = null;
    for (rdr.md.row_groups) |g| {
        if (lf.chunk_idx >= g.columns.len) return null;
        const meta = g.columns[lf.chunk_idx].meta orelse return null;
        switch (meta.ty) {
            .byte_array, .fixed_len_byte_array => return null,
            else => {},
        }
        const mn = statValue(elem, meta.ty, meta.stats.min) orelse return null;
        const mx = statValue(elem, meta.ty, meta.stats.max) orelse return null;
        // Bail rather than report a wrong extreme when the type cannot be
        // ordered — this silently returned row group 0's value before.
        if (lo == null) lo = mn else lo = if ((cmp(mn, lo.?) orelse return null) == .lt) mn else lo;
        if (hi == null) hi = mx else hi = if ((cmp(mx, hi.?) orelse return null) == .gt) mx else hi;
    }
    return .{ .min = lo orelse return null, .max = hi orelse return null };
}

fn wanted(want: ?[]const []const u8, name: []const u8) bool {
    const names = want orelse return true;
    for (names) |n| if (std.mem.eql(u8, n, name)) return true;
    return false;
}

/// The leaf a statistics question is about. A list column is never one: its
/// chunk statistics describe the elements, not the JSON text the column holds,
/// so a bound on it proves nothing and must keep every group.
fn findLeaf(leaves: []const Leaf, name: []const u8) ?Leaf {
    for (leaves) |lf| {
        if (std.mem.eql(u8, lf.name, name)) return if (lf.list != null) null else lf;
    }
    return null;
}

/// The column type a kept leaf reads as: its own, or JSON text for a list.
pub fn leafType(e: parquet.SchemaElement, lf: Leaf) Error!types.Type {
    if (lf.list != null) {
        // the element must still be a type basalt reads
        _ = try basaltType(e);
        return types.Type.init(.string).asNullable();
    }
    return (try basaltType(e)).asNullable();
}

/// Decodes one PLAIN-encoded statistics blob into a comparable `Value`.
fn statValue(elem: parquet.SchemaElement, phys: parquet.PhysicalType, raw: ?[]const u8) ?Value {
    const b = raw orelse return null;
    const ty = basaltType(elem) catch return null;
    var cur = PlainCursor.init(phys, elem.type_length orelse 0, b);
    const v = cur.next() catch return null;
    return coerce(ty, v, temporalScale(elem));
}

/// Order two statistic values, or null when the pair cannot be ordered.
///
/// Null means "unknown", and every caller must then KEEP the row group — the
/// pruning contract is that a missing or unusable statistic never drops rows.
/// This used to return `.eq` for anything it did not recognize, which inverted
/// that: a DECIMAL column's stats compared equal to every bound, so `.lt`/`.gt`
/// proved every group non-matching and a range predicate returned ZERO rows.
fn cmp(a: Value, b: Value) ?std.math.Order {
    // Numerics (int/float/decimal, in any mix) order through the engine's own
    // comparison, so pruning agrees with the filter that re-checks the rows —
    // including per-value decimal scale and the NaN total order.
    if (numOrder(a, b)) |o| return o;
    return switch (a) {
        .int => std.math.order(a.int, switch (b) {
            .int => |x| x,
            .float => |x| @as(i64, @intFromFloat(x)),
            else => return null,
        }),
        .float => std.math.order(a.float, switch (b) {
            .float => |x| x,
            .int => |x| @as(f64, @floatFromInt(x)),
            else => return null,
        }),
        // A date column against an ISO string literal is an ordinary comparison in
        // this dialect (§9), so parse it and prune on it. A string that is not a
        // date is unknown, not equal.
        .date => std.math.order(@as(i64, a.date), switch (b) {
            .date => |x| @as(i64, x),
            .int => |x| x,
            .string => |x| eval.parseIsoDate(x) orelse return null,
            else => return null,
        }),
        .time, .timestamp => blk: {
            const av = if (a == .time) a.time else a.timestamp;
            const bv = switch (b) {
                .time => |x| x,
                .timestamp => |x| x,
                .int => |x| x,
                .string => |x| if (a == .timestamp)
                    eval.parseIsoTimestamp(x) orelse return null
                else
                    return null,
                else => return null,
            };
            break :blk std.math.order(av, bv);
        },
        .string, .bytes => blk: {
            const sa = if (a == .string) a.string else a.bytes;
            const sb = switch (b) {
                .string => |x| x,
                .bytes => |x| x,
                else => return null,
            };
            break :blk std.mem.order(u8, sa, sb);
        },
        else => null,
    };
}

test "cmp: an unorderable pair is unknown, never a fabricated equality" {
    // This is what made a parquet date range return nothing. `cmp` answered `.eq`
    // for a date against a string, and `groupMayMatch`'s `.lt` rule excludes a group
    // whenever the order is not `.lt` — so every row group was pruned and
    // `WHERE l_shipdate < '1995-01-01'` came back empty. `<=`, `>=` and `=` survived
    // only because their own defaults happen to keep the group.
    const d: Value = .{ .date = 9131 }; // 1995-01-01
    try std.testing.expectEqual(std.math.Order.lt, cmp(.{ .date = 9130 }, .{ .string = "1995-01-01" }).?);
    try std.testing.expectEqual(std.math.Order.eq, cmp(d, .{ .string = "1995-01-01" }).?);
    try std.testing.expectEqual(std.math.Order.gt, cmp(.{ .date = 9132 }, .{ .string = "1995-01-01" }).?);
    // Not a date: unknown, so pruning must keep the group.
    try std.testing.expect(cmp(d, .{ .string = "not-a-date" }) == null);
    try std.testing.expect(cmp(d, .{ .bool = true }) == null);
    try std.testing.expect(cmp(.{ .int = 5 }, .{ .string = "5" }) == null);
    try std.testing.expect(cmp(.{ .string = "a" }, .{ .int = 1 }) == null);
    // A timestamp column against an ISO literal still orders.
    try std.testing.expectEqual(std.math.Order.lt, cmp(.{ .timestamp = 0 }, .{ .string = "1970-01-02" }).?);
}

fn isNumV(v: Value) bool {
    return v == .int or v == .float or v == .decimal;
}

/// Numeric ordering shared with the engine (`eval.compareValues`), used only
/// when BOTH sides are numeric so the temporal/text arms below still apply.
fn numOrder(a: Value, b: Value) ?std.math.Order {
    if (!isNumV(a) or !isNumV(b)) return null;
    return eval.compareValues(a, b);
}

// --- source ------------------------------------------------------------------

const driver = @import("driver.zig");
const Batch = @import("../exec/batch.zig").Batch;
const http_client = @import("http_client.zig");
const objstore = @import("objstore.zig");

/// Byte source a reader pulls from: a local file read on demand, or an already
/// resident buffer.
///
/// Parquet is random-access by design — the footer sits at the end and points at
/// chunks — so holding the whole object in memory is unnecessary. Fetching only
/// the footer and the chunks a query touches is what keeps a multi-gigabyte file
/// from becoming multi-gigabyte resident.
pub const Bytes = union(enum) {
    memory: []const u8,
    file: struct { f: std.fs.File, size: u64 },
    remote: *Remote,

    pub fn size(self: Bytes) u64 {
        return switch (self) {
            .memory => |m| m.len,
            .file => |x| x.size,
            .remote => |r| r.total,
        };
    }

    /// Reads `len` bytes at `off`. The result is owned by `arena` for the file
    /// and remote cases and borrowed for the memory case; callers treat it as
    /// read-only.
    pub fn range(self: Bytes, arena: std.mem.Allocator, off: u64, len: usize) ![]const u8 {
        switch (self) {
            .memory => |m| {
                if (off + len > m.len) return Error.CorruptParquetPage;
                return m[@intCast(off)..][0..len];
            },
            .file => |x| {
                if (off + len > x.size) return Error.CorruptParquetPage;
                const buf = try arena.alloc(u8, len);
                const n = try x.f.preadAll(buf, off);
                if (n != len) return Error.CorruptParquetPage;
                return buf;
            },
            .remote => |r| {
                if (off + len > r.total) return Error.CorruptParquetPage;
                return r.read(arena, off, len);
            },
        }
    }

    pub fn close(self: Bytes) void {
        switch (self) {
            .memory => {},
            .file => |x| x.f.close(),
            .remote => |r| r.client.deinit(),
        }
    }
};

/// An object read over HTTP by range request — a plain `http(s)://` URL, or an
/// `az://` blob, which differs only in that Shared Key signs the Range header
/// and so must be re-signed per request.
///
/// Parquet is what makes this worth the round trips: the footer names the byte
/// extent of every column chunk, so a projected query over a remote object
/// fetches the footer and those extents and nothing else. Reading the whole
/// object to decode two columns of forty is the thing this exists to avoid.
///
/// Not every server honours `Range`. One that answers a ranged GET with `200`
/// has sent the whole body anyway, so it is kept in `whole` and served from
/// there — correct on any server, fast on the ones that cooperate.
pub const Remote = struct {
    /// The reader's arena, not a batch's. `whole` outlives the call that fills
    /// it, and `range` is handed the batch arena, which is recycled per batch —
    /// so the fallback body has to be copied somewhere that survives.
    arena: std.mem.Allocator,
    client: *std.http.Client,
    url: []const u8,
    object: ?objstore.Object = null,
    total: u64,
    /// Set when the origin ignored `Range` (or could not report a size), which
    /// makes every later read a slice instead of another full transfer.
    whole: ?[]const u8 = null,
    /// A corporate TLS interceptor is repaired once per object, not per range.
    repaired: bool = false,

    pub fn open(arena: std.mem.Allocator, path: []const u8) !*Remote {
        const client = try arena.create(std.http.Client);
        client.* = http_client.initClient(arena);
        const self = try arena.create(Remote);
        self.* = .{ .arena = arena, .client = client, .url = path, .total = 0 };
        if (objstore.isUrl(path)) {
            const o = try objstore.parse(arena, path);
            self.object = o;
            self.url = o.url;
        }

        // HEAD answers "how big?" without a body. A server that refuses it, or
        // reports no length, leaves us no way to find the footer — fall back to
        // fetching the object once, which is what this used to do always.
        if (self.contentLength(arena)) |n| {
            self.total = n;
        } else |_| {
            const body = try self.fetchWhole(arena);
            self.whole = body;
            self.total = body.len;
        }
        return self;
    }

    pub fn read(self: *Remote, arena: std.mem.Allocator, off: u64, len: usize) ![]const u8 {
        if (len == 0) return "";
        // `total` came from HEAD; `whole` came from a later 200. A server that
        // disagrees between the two must not slice us past the buffer.
        if (self.whole) |w| {
            if (off + len > w.len) return Error.CorruptParquetPage;
            return w[@intCast(off)..][0..len];
        }

        const hdr = try std.fmt.allocPrint(arena, "bytes={d}-{d}", .{ off, off + len - 1 });
        const res = try self.send(arena, .GET, hdr);
        switch (res.code) {
            206 => {
                if (res.body.len != len) return Error.CorruptParquetPage;
                return res.body;
            },
            // Range ignored: this is the whole object, so keep it and stop
            // asking. It has to be copied out of the caller's arena first —
            // that one is a batch's, recycled before the next read.
            200 => {
                const kept = try self.arena.dupe(u8, res.body);
                self.whole = kept;
                if (off + len > kept.len) return Error.CorruptParquetPage;
                return kept[@intCast(off)..][0..len];
            },
            else => return self.statusError(res.code, res.body),
        }
    }

    fn fetchWhole(self: *Remote, arena: std.mem.Allocator) ![]const u8 {
        const res = try self.send(arena, .GET, "");
        if (res.code != 200) return self.statusError(res.code, res.body);
        return res.body;
    }

    const Resp = struct { code: u16, body: []const u8 };

    fn send(
        self: *Remote,
        arena: std.mem.Allocator,
        method: std.http.Method,
        range_hdr: []const u8,
    ) !Resp {
        const extra = try self.headers(arena, method, range_hdr);
        return self.once(arena, method, extra) catch |e| switch (e) {
            // Retried on its own buffer: a partially written response from the
            // failed attempt must not be prepended to the retry's body.
            error.TlsInitializationFailed => {
                if (!self.repair()) return e;
                return self.once(arena, method, extra);
            },
            else => e,
        };
    }

    fn once(
        self: *Remote,
        arena: std.mem.Allocator,
        method: std.http.Method,
        extra: []const std.http.Header,
    ) !Resp {
        var aw = std.Io.Writer.Allocating.init(arena);
        const res = try self.client.fetch(.{
            .method = method,
            .location = .{ .url = self.url },
            .extra_headers = extra,
            .decompress_buffer = http_client.decompress_direct,
            .response_writer = &aw.writer,
        });
        return .{ .code = @intFromEnum(res.status), .body = aw.writer.buffered() };
    }

    pub fn headers(
        self: *Remote,
        arena: std.mem.Allocator,
        method: std.http.Method,
        range_hdr: []const u8,
    ) ![]const std.http.Header {
        if (self.object) |o| {
            const verb = if (method == .HEAD) "HEAD" else "GET";
            return o.requestHeaders(arena, verb, range_hdr);
        }
        if (range_hdr.len == 0) return &.{};
        return arena.dupe(std.http.Header, &.{.{ .name = "Range", .value = range_hdr }});
    }

    /// The object's size, from a HEAD. Errors (405, no Content-Length, a proxy
    /// that drops it) send the caller to the whole-object path.
    fn contentLength(self: *Remote, arena: std.mem.Allocator) !u64 {
        const extra = try self.headers(arena, .HEAD, "");
        const uri = std.Uri.parse(self.url) catch return error.InvalidUrl;
        var req = try self.client.request(.HEAD, uri, .{ .extra_headers = extra });
        defer req.deinit();
        try req.sendBodiless();
        var redirect_buf: [8 * 1024]u8 = undefined;
        const resp = try req.receiveHead(&redirect_buf);
        if (@intFromEnum(resp.head.status) != 200) return error.HeadUnsupported;
        return resp.head.content_length orelse error.HeadUnsupported;
    }

    pub fn statusError(self: *Remote, code: u16, body: []const u8) anyerror {
        if (self.object) |o| return o.statusToError(code, body);
        return http_client.statusError(code);
    }

    fn repair(self: *Remote) bool {
        if (self.repaired) return false;
        self.repaired = true;
        const uri = std.Uri.parse(self.url) catch return false;
        const h = http_client.uriHost(uri) orelse return false;
        if (!http_client.repairBundle(self.client.allocator, &self.client.ca_bundle, h, uri.port orelse 443)) return false;
        self.client.next_https_rescan_certs = false;
        return true;
    }
};

/// Reads a Parquet file as a pipeline source, one batch per row group.
///
/// Only the footer and the column chunks a query needs are read; a chunk is
/// fetched, decoded and released per row group, so resident memory tracks the
/// widest row group's projected columns rather than the file.
pub const Output = union(enum) {
    leaf: usize,
    nested: *const Nested,
};

pub const Reader = struct {
    arena: std.mem.Allocator,
    src: Bytes,
    md: parquet.FileMetaData,
    schema: types.Schema,
    /// Readable leaves, in output order. Repeated (list) leaves are excluded but
    /// still occupy a chunk slot, which is why `Leaf.chunk_idx` is carried.
    leaves: []const Leaf,
    /// The output columns, in schema order: a leaf of `leaves`, or a column
    /// assembled from several of them.
    outputs: []const Output = &.{},
    /// Ascending chunk start offsets plus the footer start, used to bound each
    /// ranged read.
    boundaries: []const u64 = &.{},
    /// Bounds used to skip row groups outright; empty means read them all.
    bounds: []const Bound = &.{},
    /// Live top-N bound, when the pipeline is a `sort … limit` over this file.
    threshold: ?*const Threshold = null,
    /// Row groups skipped on statistics, for reporting.
    groups_skipped: usize = 0,
    /// The run's pushdown tally, when it keeps one: groups seen and skipped.
    tally: ?*driver.ScanTally = null,
    rg: usize = 0,
    /// Exclusive end of the row-group window this reader is confined to. Null
    /// reads to the end of the file; a parallel worker sets it so each lane owns
    /// a disjoint slice of the row groups.
    rg_end: ?usize = null,

    pub fn isPath(path: []const u8) bool {
        return std.mem.endsWith(u8, path, ".parquet");
    }

    pub fn open(arena: std.mem.Allocator, path: []const u8) !*Reader {
        return openProjected(arena, path, null);
    }

    /// `open`, decoding only the named columns.
    ///
    /// This is what makes Parquet worth its complexity: a column not asked for
    /// is never touched, so a query over two of forty columns reads two chunks.
    /// An unknown name is ignored rather than an error — the caller's set is a
    /// hint, and a stage that truly needs a missing column will fail loudly when
    /// it cannot resolve it.
    pub fn openProjected(arena: std.mem.Allocator, path: []const u8, want: ?[]const []const u8) !*Reader {
        // Local files read by pread, remote objects by HTTP range — the same
        // footer-then-chunks access pattern either way, so a projected query
        // over an object store transfers only what it decodes.
        const src: Bytes = if (isRemote(path))
            .{ .remote = try Remote.open(arena, path) }
        else blk: {
            const f = try std.fs.cwd().openFile(path, .{});
            break :blk .{ .file = .{ .f = f, .size = (try f.stat()).size } };
        };
        errdefer src.close();

        var footer_start: u64 = 0;
        const md = try parseFooterOf(arena, src, &footer_start);

        // Struct fields read as flat dotted columns; a list of scalars as one JSON
        // column from its leaf; a list of structs, a map, or anything else that
        // spans several leaves as one JSON column assembled from all of them.
        // Nothing in the file is left out.
        const all = try collectLeaves(arena, md.schema);
        var keep = std.array_list.Managed(Leaf).init(arena);
        var fields = std.array_list.Managed(types.Schema.Field).init(arena);
        var outputs = std.array_list.Managed(Output).init(arena);
        var roots_seen = std.array_list.Managed(usize).init(arena);
        for (all, 0..) |lf, li| {
            const shared = if (lf.root) |rt| for (all, 0..) |other, oi| {
                if (oi != li and other.root != null and other.root.?.idx == rt.idx) break true;
            } else false else false;
            if (shared) {
                // a column spanning several leaves: taken whole at its first leaf
                const rt = lf.root.?;
                for (roots_seen.items) |x| {
                    if (x == rt.idx) break;
                } else {
                    try roots_seen.append(rt.idx);
                    if (!wanted(want, lf.name)) continue;
                    // a column basalt cannot rebuild fails the read: leaving it
                    // out would copy a file short of a column without a word
                    const tree = try buildNested(arena, md.schema, rt);
                    var ix = std.array_list.Managed(usize).init(arena);
                    for (all) |o| if (o.root != null and o.root.?.idx == rt.idx) {
                        _ = try basaltType(md.schema[o.schema_idx]);
                        try keep.append(o);
                        try ix.append(keep.items.len - 1);
                    };
                    if (ix.items.len != tree.nleaves) return Error.UnsupportedParquetSchema;
                    const n = try arena.create(Nested);
                    n.* = .{ .name = lf.name, .root = tree, .leaves = ix.items };
                    try outputs.append(.{ .nested = n });
                    try fields.append(.{ .name = lf.name, .ty = types.Type.init(.string).asNullable() });
                }
                continue;
            }
            if (!wanted(want, lf.name)) continue;
            try keep.append(lf);
            try outputs.append(.{ .leaf = keep.items.len - 1 });
            try fields.append(.{ .name = lf.name, .ty = try leafType(md.schema[lf.schema_idx], lf) });
        }
        // An empty projection (COUNT(*)) still needs batches with a row count,
        // so the narrowest column is kept rather than none.
        if (fields.items.len == 0 and want != null) {
            for (all) |lf| {
                if (lf.isRepeated()) continue;
                try keep.append(lf);
                try outputs.append(.{ .leaf = keep.items.len - 1 });
                try fields.append(.{
                    .name = lf.name,
                    .ty = (try basaltType(md.schema[lf.schema_idx])).asNullable(),
                });
                break;
            }
        }
        if (fields.items.len == 0) return Error.UnsupportedParquetSchema;

        const self = try arena.create(Reader);
        self.* = .{
            .arena = arena,
            .src = src,
            .md = md,
            .schema = .{ .fields = try fields.toOwnedSlice() },
            .leaves = try keep.toOwnedSlice(),
            .outputs = try outputs.toOwnedSlice(),
            .boundaries = try chunkBoundaries(arena, md, footer_start),
        };
        return self;
    }

    /// One row group per call. Empty groups are skipped rather than returned as
    /// zero-row batches, which downstream operators treat as end-of-stream.
    pub fn next(self: *Reader, arena: std.mem.Allocator) !?Batch {
        const last = self.rg_end orelse self.md.row_groups.len;
        while (self.rg < last) {
            const g = self.md.row_groups[self.rg];
            self.rg += 1;
            const rows = std.math.cast(usize, g.num_rows) orelse return Error.CorruptParquetPage;
            if (rows == 0) continue;
            if (self.tally) |t| driver.ScanTally.add(&t.row_groups, 1);
            // statistics can rule a whole group out before any page is touched
            if (self.bounds.len > 0 and
                !groupMayMatch(self.md.schema, self.leaves, g, self.bounds))
            {
                self.groups_skipped += 1;
                if (self.tally) |t| driver.ScanTally.add(&t.row_groups_skipped, 1);
                continue;
            }
            if (self.threshold) |t| {
                if (!groupBeatsThreshold(self.md.schema, self.leaves, g, t.*)) {
                    self.groups_skipped += 1;
                    if (self.tally) |ty| driver.ScanTally.add(&ty.row_groups_skipped, 1);
                    continue;
                }
            }
            const cols = try arena.alloc(column.Column, self.outputs.len);
            for (self.outputs, 0..) |o, ci| switch (o) {
                .leaf => |li| {
                    const lf = self.leaves[li];
                    const chunk = try self.chunkOf(arena, g, lf);
                    cols[ci] = try readColumnChunkLevels(
                        arena,
                        chunk.bytes,
                        chunk.meta,
                        self.md.schema[lf.schema_idx],
                        rows,
                        lf.max_def,
                        lf.max_rep,
                        lf.list,
                        chunk.start,
                    );
                },
                .nested => |n| {
                    const entries = try arena.alloc(Entries, n.leaves.len);
                    for (entries, n.leaves) |*e, li| {
                        const lf = self.leaves[li];
                        const chunk = try self.chunkOf(arena, g, lf);
                        e.* = try readEntries(arena, chunk.bytes, chunk.meta, self.md.schema[lf.schema_idx], lf.max_def, lf.max_rep, chunk.start);
                    }
                    cols[ci] = try assembleNested(arena, &n.root, entries, rows);
                },
            };
            return Batch{ .schema = &self.schema, .columns = cols, .len = rows };
        }
        return null;
    }

    const Chunk = struct { bytes: []const u8, meta: parquet.ColumnMetaData, start: u64 };

    /// One leaf's chunk of a row group, fetched alone: bounded by wherever the
    /// next chunk begins.
    fn chunkOf(self: *Reader, arena: std.mem.Allocator, g: parquet.RowGroup, lf: Leaf) !Chunk {
        if (lf.chunk_idx >= g.columns.len) return Error.CorruptParquetPage;
        const meta = g.columns[lf.chunk_idx].meta orelse return Error.CorruptParquetPage;
        const start = std.math.cast(u64, meta.startOffset()) orelse return Error.CorruptParquetPage;
        const end = chunkEnd(self.boundaries, start);
        if (end <= start) return Error.CorruptParquetPage;
        return .{ .bytes = try self.src.range(arena, start, @intCast(end - start)), .meta = meta, .start = start };
    }

    pub fn close(self: *Reader) void {
        self.src.close();
    }

    pub fn source(self: *Reader) driver.Source {
        return .{ .ptr = self, .vtable = &source_vtable };
    }
};

const source_vtable = driver.Source.VTable{
    .schema = srcSchema,
    .next = srcNext,
    .close = srcClose,
};

fn srcSchema(p: *anyopaque) types.Schema {
    return @as(*Reader, @ptrCast(@alignCast(p))).schema;
}
fn srcNext(p: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
    return @as(*Reader, @ptrCast(@alignCast(p))).next(arena);
}
fn srcClose(p: *anyopaque) void {
    @as(*Reader, @ptrCast(@alignCast(p))).close();
}

/// End offset of the chunk starting at `start`.
///
/// `total_compressed_size` cannot be used for this: writers disagree about
/// whether it counts page headers and the dictionary page, so trusting it
/// truncates chunks. The next chunk's start is unambiguous, and the footer
/// bounds the last one.
fn chunkEnd(boundaries: []const u64, start: u64) u64 {
    for (boundaries) |b| {
        if (b > start) return b;
    }
    return start;
}

/// Every chunk start in the file plus the footer offset, ascending. Built once
/// so each chunk read knows exactly where it ends.
fn chunkBoundaries(arena: std.mem.Allocator, md: parquet.FileMetaData, footer_start: u64) ![]u64 {
    var out = std.array_list.Managed(u64).init(arena);
    for (md.row_groups) |g| {
        for (g.columns) |c| {
            const m = c.meta orelse continue;
            try out.append(std.math.cast(u64, m.startOffset()) orelse return Error.CorruptParquetPage);
        }
    }
    try out.append(footer_start);
    const sl = try out.toOwnedSlice();
    std.mem.sort(u64, sl, {}, comptime std.sort.asc(u64));
    return sl;
}

/// Reads the footer with two small ranged reads instead of the whole file.
fn parseFooterOf(arena: std.mem.Allocator, src: Bytes, footer_start: *u64) !parquet.FileMetaData {
    const total = src.size();
    if (total < parquet.trailer_len + parquet.magic.len) return parquet.Error.NotParquet;
    const head = try src.range(arena, 0, parquet.magic.len);
    if (!std.mem.eql(u8, head, parquet.magic)) return parquet.Error.NotParquet;

    const trailer = try src.range(arena, total - parquet.trailer_len, parquet.trailer_len);
    const r = try parquet.footerRange(total, trailer);
    footer_start.* = r.offset;
    const footer = try src.range(arena, r.offset, r.len);
    return parquet.parseFooter(arena, footer);
}

/// Paths a `Remote` serves: object storage and plain URLs alike. Everything
/// else is a local file.
pub fn isRemote(path: []const u8) bool {
    return objstore.isUrl(path) or
        std.mem.startsWith(u8, path, "http://") or
        std.mem.startsWith(u8, path, "https://");
}

// --- tests -------------------------------------------------------------------

const testing = std.testing;

test "bitWidth covers the level widths Parquet asks for" {
    try testing.expectEqual(@as(u6, 0), bitWidth(0)); // required column: no levels
    try testing.expectEqual(@as(u6, 1), bitWidth(1)); // optional flat column
    try testing.expectEqual(@as(u6, 2), bitWidth(2));
    try testing.expectEqual(@as(u6, 2), bitWidth(3));
    try testing.expectEqual(@as(u6, 3), bitWidth(4));
    try testing.expectEqual(@as(u6, 8), bitWidth(255));
}

test "RLE run repeats one value" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    // header varint 8 = (4 << 1) | 0 -> RLE, run of 4; value byte 1
    const got = try decodeRleHybrid(ar.allocator(), &[_]u8{ 0x08, 0x01 }, 1, 4);
    try testing.expectEqualSlices(u32, &.{ 1, 1, 1, 1 }, got);
}

test "bit-packed run unpacks LSB-first in groups of eight" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    // header varint 3 = (1 << 1) | 1 -> one group of 8 values, width 1.
    // 0b10110010 read LSB-first is 0,1,0,0,1,1,0,1
    const got = try decodeRleHybrid(ar.allocator(), &[_]u8{ 0x03, 0b10110010 }, 1, 8);
    try testing.expectEqualSlices(u32, &.{ 0, 1, 0, 0, 1, 1, 0, 1 }, got);
}

test "a width of zero yields all zeroes without consuming input" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const got = try decodeRleHybrid(ar.allocator(), &.{}, 0, 3);
    try testing.expectEqualSlices(u32, &.{ 0, 0, 0 }, got);
}

test "a truncated hybrid stream errors rather than reading past the page" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    // claims a bit-packed group but supplies no data
    try testing.expectError(Error.CorruptParquetPage, decodeRleHybrid(ar.allocator(), &[_]u8{0x03}, 1, 8));
    // an RLE run of zero would loop forever
    try testing.expectError(Error.CorruptParquetPage, decodeRleHybrid(ar.allocator(), &[_]u8{ 0x00, 0x01 }, 1, 4));
}

test "int96 converts Julian day plus nanoseconds to epoch micros" {
    // Julian day 2440588 is 1970-01-01
    try testing.expectEqual(@as(i64, 0), int96ToMicros(2_440_588, 0));
    try testing.expectEqual(@as(i64, 1_000_000), int96ToMicros(2_440_588, 1_000_000_000));
    try testing.expectEqual(@as(i64, 86_400_000_000), int96ToMicros(2_440_589, 0));
    try testing.expectEqual(@as(i64, -86_400_000_000), int96ToMicros(2_440_587, 0));
    // A wire Julian day is a full u32: 4.29e9 days of micros is 3.7e20, past i64.
    // Saturating keeps a corrupt file from being undefined behaviour in release.
    try testing.expectEqual(@as(i64, std.math.maxInt(i64)), int96ToMicros(std.math.maxInt(u32), 0));
    // Julian day 0 is only 2.4e6 days before the epoch, so it still fits.
    try testing.expectEqual(@as(i64, -210_866_803_200_000_000), int96ToMicros(0, 0));
}

test "a bit-packed run length from the wire cannot overflow the byte count" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    // varint 0xFFFF_FFFF_FFFF_FFFF: the bit-packed header claims 2^63 groups, so
    // `groups * width` wrapped before anything checked it against the page.
    const huge = [_]u8{ 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01 };
    try testing.expectError(Error.CorruptParquetPage, decodeRleHybrid(ar.allocator(), &huge, 32, 8));
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

    // required columns are non-nullable, optional ones nullable
    try testing.expect((try basaltType(.{ .ty = .int32, .repetition = .required })).nullable == false);
    try testing.expect((try basaltType(.{ .ty = .int32, .repetition = opt })).nullable);
    // a group node has no physical type and cannot be a column
    try testing.expectError(Error.UnsupportedParquetSchema, basaltType(.{ .num_children = 2 }));
}

const fx = @embedFile("testdata/zstd.parquet");

test "decodes real column values from a DuckDB-written file" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const md = try parquet.parseFile(a, fx);
    const g = md.row_groups[0];
    const rows: usize = @intCast(g.num_rows);

    // id INT32: 0..59
    const id = try readColumnChunk(a, fx, g.columns[0].meta.?, md.schema[1], rows, 1, 0);
    try testing.expectEqual(@as(usize, 60), id.len);
    try testing.expectEqual(@as(i64, 0), id.getValue(0).int);
    try testing.expectEqual(@as(i64, 59), id.getValue(59).int);

    // name BYTE_ARRAY/UTF8 -> string
    const name = try readColumnChunk(a, fx, g.columns[1].meta.?, md.schema[2], rows, 1, 0);
    try testing.expectEqualStrings("row-0", name.getValue(0).string);
    try testing.expectEqualStrings("row-59", name.getValue(59).string);

    // amt DOUBLE: i * 1.5
    const amt = try readColumnChunk(a, fx, g.columns[2].meta.?, md.schema[3], rows, 1, 0);
    try testing.expectEqual(@as(f64, 0.0), amt.getValue(0).float);
    try testing.expectEqual(@as(f64, 88.5), amt.getValue(59).float);

    // flag BOOLEAN: even ids true — bit-packed, one bit per value
    const flag = try readColumnChunk(a, fx, g.columns[3].meta.?, md.schema[4], rows, 1, 0);
    try testing.expectEqual(true, flag.getValue(0).bool);
    try testing.expectEqual(false, flag.getValue(1).bool);
    try testing.expectEqual(false, flag.getValue(59).bool);
}

test "every codec's fixture decodes to the same values" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const files = [_][]const u8{
        @embedFile("testdata/uncompressed.parquet"),
        @embedFile("testdata/snappy.parquet"),
        @embedFile("testdata/gzip.parquet"),
        @embedFile("testdata/lz4.parquet"),
    };
    for (files) |f| {
        const md = try parquet.parseFile(a, f);
        const g = md.row_groups[0];
        const name = try readColumnChunk(a, f, g.columns[1].meta.?, md.schema[2], @intCast(g.num_rows), 1, 0);
        try testing.expectEqualStrings("row-0", name.getValue(0).string);
        try testing.expectEqualStrings("row-42", name.getValue(42).string);
    }
}

test "schema walk resolves levels and dotted names for nested groups" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    // root { id (required int32), addr (optional group { city, zip }),
    //        tags (repeated group { element }) }
    const schema = [_]parquet.SchemaElement{
        .{ .name = "root", .num_children = 3 },
        .{ .name = "id", .ty = .int32, .repetition = .required },
        .{ .name = "addr", .repetition = .optional, .num_children = 2 },
        .{ .name = "city", .ty = .byte_array, .repetition = .optional },
        .{ .name = "zip", .ty = .byte_array, .repetition = .required },
        .{ .name = "tags", .repetition = .repeated, .num_children = 1 },
        .{ .name = "element", .ty = .byte_array, .repetition = .required },
    };
    const leaves = try collectLeaves(a, &schema);
    try testing.expectEqual(@as(usize, 4), leaves.len);

    try testing.expectEqualStrings("id", leaves[0].name);
    try testing.expectEqual(@as(u32, 0), leaves[0].max_def);
    try testing.expect(!leaves[0].isRepeated());

    // a field of an optional group is nullable at two levels
    try testing.expectEqualStrings("addr.city", leaves[1].name);
    try testing.expectEqual(@as(u32, 2), leaves[1].max_def);
    try testing.expectEqualStrings("addr.zip", leaves[2].name);
    try testing.expectEqual(@as(u32, 1), leaves[2].max_def);

    // the list element is repeated: named for the list it makes, with the
    // definition level at which the (two-level, legacy) list holds an element
    try testing.expectEqualStrings("tags", leaves[3].name);
    try testing.expect(leaves[3].isRepeated());
    try testing.expectEqualSlices(u32, &.{1}, leaves[3].list.?.rep_def);

    // chunk indices count every leaf, list elements included
    try testing.expectEqual(@as(usize, 3), leaves[3].chunk_idx);
}

test "temporal scale converts millisecond columns and leaves micros alone" {
    try testing.expectEqual(TemporalScale{ .mul = 1000 }, temporalScale(.{ .converted_type = 9 })); // TIMESTAMP_MILLIS
    try testing.expectEqual(TemporalScale{ .mul = 1000 }, temporalScale(.{ .converted_type = 7 })); // TIME_MILLIS
    try testing.expectEqual(TemporalScale.identity, temporalScale(.{ .converted_type = 10 })); // TIMESTAMP_MICROS
    try testing.expectEqual(TemporalScale.identity, temporalScale(.{})); // no annotation

    const ts = types.Type.init(.timestamp);
    // a millisecond value must be scaled up, not read as micros
    try testing.expectEqual(@as(i64, 1_583_298_367_123_000), coerce(ts, .{ .int = 1_583_298_367_123 }, .{ .mul = 1000 }).timestamp);
    try testing.expectEqual(@as(i64, 1_583_298_367_123), coerce(ts, .{ .int = 1_583_298_367_123 }, .identity).timestamp);
}

fn logical(phys: parquet.PhysicalType, lt: parquet.LogicalType) parquet.SchemaElement {
    return .{ .ty = phys, .repetition = .optional, .logical_type = lt };
}

test "a LogicalType-only TIMESTAMP reads as timestamp in every unit" {
    // 2026-01-01T12:00:00, as polars/pyarrow write it with no ConvertedType
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
    // isAdjustedToUTC reads as the same timestamp: basalt has no zoned type
    const utc = logical(.int64, .{ .timestamp = .{ .adjusted_to_utc = true, .unit = .nanos } });
    try testing.expectEqual(types.TypeKind.timestamp, (try basaltType(utc)).kind);
}

test "nanoseconds floor to micros, including before 1970" {
    const ns = TemporalScale{ .div = 1000 };
    try testing.expectEqual(@as(i64, 1), ns.apply(1_999));
    try testing.expectEqual(@as(i64, -1), ns.apply(-1)); // 1969-12-31T23:59:59.999999
    try testing.expectEqual(@as(i64, -2), ns.apply(-1_001));
    // a millisecond value too large for micros saturates rather than wrapping
    const ms = TemporalScale{ .mul = 1000 };
    try testing.expectEqual(@as(i64, std.math.maxInt(i64)), ms.apply(std.math.maxInt(i64) / 10));
}

test "a LogicalType-only TIME reads as time in millis and micros" {
    const tm = types.Type.init(.time);
    // 01:02:00 in micros
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
    // uuid has no basalt type and stays raw bytes
    try testing.expectEqual(types.TypeKind.bytes, (try basaltType(logical(.fixed_len_byte_array, .uuid))).kind);
    // a logical type that contradicts its physical type is ignored, not trusted
    try testing.expectEqual(types.TypeKind.bytes, (try basaltType(logical(.byte_array, .{ .timestamp = .{} }))).kind);
}

test "LogicalType wins over a ConvertedType, and agreeing annotations stay put" {
    // both present and agreeing, as a spec-following writer emits for millis
    var both = logical(.int64, .{ .timestamp = .{ .adjusted_to_utc = true, .unit = .millis } });
    both.converted_type = 9; // TIMESTAMP_MILLIS
    try testing.expectEqual(types.TypeKind.timestamp, (try basaltType(both)).kind);
    try testing.expectEqual(TemporalScale{ .mul = 1000 }, temporalScale(both));

    var dec = logical(.int64, .{ .decimal = .{ .scale = 4, .precision = 18 } });
    dec.converted_type = 5;
    dec.scale = 4;
    dec.precision = 18;
    const d = try basaltType(dec);
    try testing.expectEqual(@as(u8, 18), d.precision);
    try testing.expectEqual(@as(u8, 4), d.scale);

    // an unrecognised logical type falls back to the converted one
    var other = logical(.int32, .other);
    other.converted_type = 6; // DATE
    try testing.expectEqual(types.TypeKind.date, (try basaltType(other)).kind);
}

const fx_v2 = @embedFile("testdata/v2delta.parquet");

test "data page v2 with DELTA encodings decodes to the same values as v1" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    // same 60 rows as the v1 fixtures, written with V2 pages and DELTA encodings
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realpathAlloc(a, ".");
    const path = try std.fs.path.join(a, &.{ dir, "v2.parquet" });
    try tmp.dir.writeFile(.{ .sub_path = "v2.parquet", .data = fx_v2 });

    const r = try Reader.open(a, path);
    try testing.expectEqual(@as(usize, 4), r.schema.fields.len);
    const b = (try r.next(a)).?;
    try testing.expectEqual(@as(usize, 60), b.len);

    // id is DELTA_BINARY_PACKED
    try testing.expectEqual(@as(i64, 0), b.columns[0].getValue(0).int);
    try testing.expectEqual(@as(i64, 42), b.columns[0].getValue(42).int);
    try testing.expectEqual(@as(i64, 59), b.columns[0].getValue(59).int);
    // name is DELTA_LENGTH_BYTE_ARRAY
    try testing.expectEqualStrings("row-0", b.columns[1].getValue(0).string);
    try testing.expectEqualStrings("row-59", b.columns[1].getValue(59).string);
    try testing.expectEqual(@as(f64, 88.5), b.columns[2].getValue(59).float);
    try testing.expectEqual(true, b.columns[3].getValue(0).bool);
}

test "delta binary packed recovers a running sum, including negatives" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    // header: block 128, 4 miniblocks, 1 value, first value 7 (zigzag 14)
    const src = [_]u8{ 0x80, 0x01, 0x04, 0x01, 0x0e };
    const got = try decodeDeltaBinaryPacked(a, &src, 1);
    try testing.expectEqualSlices(i64, &.{7}, got);

    // a malformed header (zero miniblocks) must not divide by zero
    try testing.expectError(Error.CorruptParquetPage, decodeDeltaBinaryPacked(a, &[_]u8{ 0x80, 0x01, 0x00, 0x01, 0x00 }, 1));
}

test "byte stream split regroups transposed value bytes" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    // two 4-byte values 0x03020100 and 0x07060504, transposed by byte position
    const src = [_]u8{ 0x00, 0x04, 0x01, 0x05, 0x02, 0x06, 0x03, 0x07 };
    const got = try decodeByteStreamSplit(ar.allocator(), &src, 4, 2);
    try testing.expectEqualSlices(u8, &.{ 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07 }, got);
    try testing.expectError(Error.CorruptParquetPage, decodeByteStreamSplit(ar.allocator(), &src, 4, 3));
}

test "projection keeps only the named columns and never drops all of them" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realpathAlloc(a, ".");
    const path = try std.fs.path.join(a, &.{ dir, "p.parquet" });
    try tmp.dir.writeFile(.{ .sub_path = "p.parquet", .data = @embedFile("testdata/zstd.parquet") });

    // no projection: every column
    const all = try Reader.open(a, path);
    try testing.expectEqual(@as(usize, 4), all.schema.fields.len);

    // two of four
    const two = try Reader.openProjected(a, path, &.{ "name", "flag" });
    try testing.expectEqual(@as(usize, 2), two.schema.fields.len);
    try testing.expectEqualStrings("name", two.schema.fields[0].name);
    try testing.expectEqualStrings("flag", two.schema.fields[1].name);
    const b = (try two.next(a)).?;
    try testing.expectEqual(@as(usize, 60), b.len);
    try testing.expectEqualStrings("row-0", b.columns[0].getValue(0).string);
    try testing.expectEqual(true, b.columns[1].getValue(0).bool);

    // an unknown name is ignored rather than fatal
    const one = try Reader.openProjected(a, path, &.{ "name", "nosuch" });
    try testing.expectEqual(@as(usize, 1), one.schema.fields.len);

    // an empty projection still yields batches with a usable row count
    const none = try Reader.openProjected(a, path, &.{});
    try testing.expectEqual(@as(usize, 1), none.schema.fields.len);
    const nb = (try none.next(a)).?;
    try testing.expectEqual(@as(usize, 60), nb.len);
}

test "row groups are skipped only when statistics prove no row can match" {
    const schema = [_]parquet.SchemaElement{
        .{ .name = "root", .num_children = 1 },
        .{ .name = "id", .ty = .int64, .repetition = .optional },
    };
    const leaves = [_]Leaf{.{ .schema_idx = 1, .chunk_idx = 0, .name = "id", .max_def = 1, .max_rep = 0 }};

    // a chunk whose values run 100..200
    var lo: [8]u8 = undefined;
    var hi: [8]u8 = undefined;
    std.mem.writeInt(i64, &lo, 100, .little);
    std.mem.writeInt(i64, &hi, 200, .little);
    var chunks = [_]parquet.ColumnChunk{.{ .meta = .{
        .ty = .int64,
        .stats = .{ .min = &lo, .max = &hi },
    } }};
    const g = parquet.RowGroup{ .columns = &chunks, .num_rows = 10 };

    const keep = [_]Bound{.{ .column = "id", .op = .lt, .value = .{ .int = 500 } }};
    const drop = [_]Bound{.{ .column = "id", .op = .lt, .value = .{ .int = 50 } }};
    try testing.expect(groupMayMatch(&schema, &leaves, g, &keep));
    try testing.expect(!groupMayMatch(&schema, &leaves, g, &drop));

    // equality outside [min,max] is provably empty; inside it is not
    const eq_in = [_]Bound{.{ .column = "id", .op = .eq, .value = .{ .int = 150 } }};
    const eq_out = [_]Bound{.{ .column = "id", .op = .eq, .value = .{ .int = 999 } }};
    try testing.expect(groupMayMatch(&schema, &leaves, g, &eq_in));
    try testing.expect(!groupMayMatch(&schema, &leaves, g, &eq_out));

    const gt_keep = [_]Bound{.{ .column = "id", .op = .gt, .value = .{ .int = 150 } }};
    const gt_drop = [_]Bound{.{ .column = "id", .op = .gt, .value = .{ .int = 200 } }};
    try testing.expect(groupMayMatch(&schema, &leaves, g, &gt_keep));
    try testing.expect(!groupMayMatch(&schema, &leaves, g, &gt_drop));

    // without statistics nothing is provable, so the group is always kept
    var bare = [_]parquet.ColumnChunk{.{ .meta = .{ .ty = .int64 } }};
    const g2 = parquet.RowGroup{ .columns = &bare, .num_rows = 10 };
    try testing.expect(groupMayMatch(&schema, &leaves, g2, &drop));

    // an unknown column contributes no bound
    const other = [_]Bound{.{ .column = "nosuch", .op = .lt, .value = .{ .int = 0 } }};
    try testing.expect(groupMayMatch(&schema, &leaves, g, &other));
}

/// polars 1.44, no ConvertedType on the temporal columns: `ts` naive micros,
/// `ts_utc` UTC nanos, `t` TIME nanos. Two row groups of two rows each.
const fx_logical = @embedFile("testdata/logical_types.parquet");

test "a polars footer carries the LogicalType, and pruning uses converted units" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const md = try parquet.parseFile(a, fx_logical);
    const leaves = try collectLeaves(a, md.schema);
    const ts = md.schema[leaves[1].schema_idx];
    const utc = md.schema[leaves[2].schema_idx];
    try testing.expectEqual(@as(?i32, null), ts.converted_type);
    try testing.expectEqual(parquet.TimeUnit.micros, ts.logical_type.?.timestamp.unit);
    try testing.expect(!ts.logical_type.?.timestamp.adjusted_to_utc);
    try testing.expectEqual(parquet.TimeUnit.nanos, utc.logical_type.?.timestamp.unit);
    try testing.expect(utc.logical_type.?.timestamp.adjusted_to_utc);
    try testing.expectEqual(types.TypeKind.time, (try basaltType(md.schema[leaves[3].schema_idx])).kind);

    // group 0: ts up to 2026-01-01 12:00, ts_utc from 2025-06-01 08:30
    // group 1: ts only 1969-12-31,        ts_utc only 2026-03-01
    try testing.expectEqual(@as(usize, 2), md.row_groups.len);
    const jan1: i64 = 1_767_225_600_000_000; // 2026-01-01T00:00:00 in micros
    const ge_jan1 = [_]Bound{.{ .column = "ts", .op = .ge, .value = .{ .timestamp = jan1 } }};
    try testing.expect(groupMayMatch(md.schema, leaves, md.row_groups[0], &ge_jan1));
    try testing.expect(!groupMayMatch(md.schema, leaves, md.row_groups[1], &ge_jan1));
    // raw nanosecond stats would sit 1000x above any micros literal and keep group 0
    const utc_lt = [_]Bound{.{ .column = "ts_utc", .op = .lt, .value = .{ .timestamp = jan1 } }};
    try testing.expect(groupMayMatch(md.schema, leaves, md.row_groups[0], &utc_lt));
    try testing.expect(!groupMayMatch(md.schema, leaves, md.row_groups[1], &utc_lt));
    const utc_gt = [_]Bound{.{ .column = "ts_utc", .op = .gt, .value = .{ .timestamp = jan1 } }};
    try testing.expect(groupMayMatch(md.schema, leaves, md.row_groups[1], &utc_gt));
}

test "chunk extents come from the next chunk, never from total_compressed_size" {
    const b = [_]u64{ 4, 100, 250, 900 };
    try testing.expectEqual(@as(u64, 100), chunkEnd(&b, 4));
    try testing.expectEqual(@as(u64, 250), chunkEnd(&b, 100));
    // the last chunk ends at the footer, which is the final boundary
    try testing.expectEqual(@as(u64, 900), chunkEnd(&b, 250));
    // an offset past every boundary yields no span, which the caller rejects
    try testing.expectEqual(@as(u64, 900), chunkEnd(&b, 900));
}

test "a corrupted file errors instead of panicking" {
    // Every guard in this file is a wire value used as a length, a shift or a
    // count. Flipping bytes across a real file walks them: the only acceptable
    // outcomes are a decoded batch or an error, never a trap.
    const good = @embedFile("testdata/zstd.parquet");
    var buf: [good.len]u8 = undefined;

    var off: usize = 0;
    while (off < good.len) : (off += 7) {
        for ([_]u8{ 0xFF, 0x80, 0x01 }) |bit| {
            @memcpy(&buf, good);
            buf[off] ^= bit;

            var ar = std.heap.ArenaAllocator.init(testing.allocator);
            defer ar.deinit();
            var tmp = testing.tmpDir(.{});
            defer tmp.cleanup();
            try tmp.dir.writeFile(.{ .sub_path = "c.parquet", .data = &buf });
            const dir = try tmp.dir.realpathAlloc(ar.allocator(), ".");
            const path = try std.fs.path.join(ar.allocator(), &.{ dir, "c.parquet" });

            const r = Reader.open(ar.allocator(), path) catch continue;
            defer r.close();
            while (r.next(ar.allocator()) catch null) |b| {
                if (b.len == 0) break;
            }
        }
    }
}

test "ranged reads return the same values as an in-memory file" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "r.parquet", .data = @embedFile("testdata/zstd.parquet") });
    const dir = try tmp.dir.realpathAlloc(a, ".");
    const path = try std.fs.path.join(a, &.{ dir, "r.parquet" });

    // the reader opens by range; compare against decoding the same bytes whole
    const r = try Reader.open(a, path);
    defer r.close();
    const got = (try r.next(a)).?;
    try testing.expectEqual(@as(usize, 60), got.len);
    try testing.expectEqual(@as(i64, 0), got.columns[0].getValue(0).int);
    try testing.expectEqual(@as(i64, 59), got.columns[0].getValue(59).int);
    try testing.expectEqualStrings("row-59", got.columns[1].getValue(59).string);
    try testing.expectEqual(@as(f64, 88.5), got.columns[2].getValue(59).float);
    try testing.expect((try r.next(a)) == null);
}

test "a Bytes range refuses to read past the end" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const src = Bytes{ .memory = "0123456789" };
    try testing.expectEqualStrings("234", try src.range(ar.allocator(), 2, 3));
    try testing.expectError(Error.CorruptParquetPage, src.range(ar.allocator(), 8, 5));
    try testing.expectEqual(@as(u64, 10), src.size());
}

test "a remote whole-body read refuses to slice past the body it was given" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    // HEAD claimed 100000 bytes; the ranged GET came back 200 with 10. The
    // fast path must not trust `total` over the buffer it actually holds.
    var client = http_client.initClient(a);
    defer client.deinit();
    var r = Remote{
        .arena = a,
        .client = &client,
        .url = "http://example/x.parquet",
        .total = 100000,
        .whole = "0123456789",
    };
    try testing.expectEqualStrings("234", try r.read(a, 2, 3));
    try testing.expectError(Error.CorruptParquetPage, r.read(a, 99992, 8));
    try testing.expectError(Error.CorruptParquetPage, r.read(a, 8, 5));
}

test "top-N threshold skips only groups it can prove cannot contribute" {
    const schema = [_]parquet.SchemaElement{
        .{ .name = "root", .num_children = 1 },
        .{ .name = "v", .ty = .int64, .repetition = .optional },
    };
    const leaves = [_]Leaf{.{ .schema_idx = 1, .chunk_idx = 0, .name = "v", .max_def = 1, .max_rep = 0 }};
    var lo: [8]u8 = undefined;
    var hi: [8]u8 = undefined;
    std.mem.writeInt(i64, &lo, 100, .little);
    std.mem.writeInt(i64, &hi, 200, .little);
    var chunks = [_]parquet.ColumnChunk{.{ .meta = .{ .ty = .int64, .stats = .{ .min = &lo, .max = &hi } } }};
    const g = parquet.RowGroup{ .columns = &chunks, .num_rows = 10 };

    // DESC: the group tops out at 200, so a bound of 500 rules it out entirely
    try testing.expect(!groupBeatsThreshold(&schema, &leaves, g, .{ .column = "v", .desc = true, .full = true, .value = .{ .int = 500 } }));
    try testing.expect(groupBeatsThreshold(&schema, &leaves, g, .{ .column = "v", .desc = true, .full = true, .value = .{ .int = 150 } }));
    // equal to the bound is NOT skippable on its own, but cannot beat it either
    try testing.expect(!groupBeatsThreshold(&schema, &leaves, g, .{ .column = "v", .desc = true, .full = true, .value = .{ .int = 200 } }));

    // ASC mirrors it against the minimum
    try testing.expect(!groupBeatsThreshold(&schema, &leaves, g, .{ .column = "v", .desc = false, .full = true, .value = .{ .int = 50 } }));
    try testing.expect(groupBeatsThreshold(&schema, &leaves, g, .{ .column = "v", .desc = false, .full = true, .value = .{ .int = 150 } }));

    // every conservative case must keep the group
    try testing.expect(groupBeatsThreshold(&schema, &leaves, g, .{ .column = "v", .desc = true, .full = false, .value = .{ .int = 500 } }));
    try testing.expect(groupBeatsThreshold(&schema, &leaves, g, .{ .column = "nosuch", .desc = true, .full = true, .value = .{ .int = 500 } }));
    try testing.expect(groupBeatsThreshold(&schema, &leaves, g, .{ .column = "v", .desc = true, .full = true, .value = .null }));
    var bare = [_]parquet.ColumnChunk{.{ .meta = .{ .ty = .int64 } }};
    const g2 = parquet.RowGroup{ .columns = &bare, .num_rows = 10 };
    try testing.expect(groupBeatsThreshold(&schema, &leaves, g2, .{ .column = "v", .desc = true, .full = true, .value = .{ .int = 500 } }));
}

test "remote paths are recognised, local ones left alone" {
    try testing.expect(isRemote("https://host/a.parquet"));
    try testing.expect(isRemote("http://host/a.parquet"));
    try testing.expect(isRemote("az://acct/ctr/a.parquet"));
    try testing.expect(!isRemote("/data/a.parquet"));
    try testing.expect(!isRemote("a.parquet"));
}

// The regression this pins: `openProjected` used to send everything that was not
// `az://` to `std.fs.cwd().openFile`, so an `http(s)://` URL failed with
// FileNotFound having never opened a socket — while the docs advertised it. Port
// 1 is not listening, so a routed read fails at connect; a filesystem error here
// means the URL never reached the network at all.
test "an http parquet source routes to the network, never the local filesystem" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();

    const r = Reader.open(ar.allocator(), "http://127.0.0.1:1/nope.parquet");
    try testing.expectError(error.ConnectionRefused, r);
}

fn fuzzKernels(_: void, input: []const u8) anyerror!void {
    // Every kernel here decodes attacker-controlled page bytes. `count` and
    // `width` come from page headers in real use — also attacker-controlled —
    // so both are derived from the input; count is bounded only to keep the
    // harness fast, not because the kernels may assume a bound.
    if (input.len < 3) return;
    const width6: u6 = @truncate(input[0]);
    const count: usize = ((@as(usize, input[1]) << 4) | (input[2] & 0x0F)) & 0x1FF;
    const src = input[3..];
    // Fixed buffer, not a heap arena: it makes each iteration allocation-free
    // (the mutation loop runs thousands), and a decoder talked into a huge
    // size by hostile bytes gets error.OutOfMemory instead of the memory.
    var mem: [256 * 1024]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&mem);
    var arena = std.heap.ArenaAllocator.init(fba.allocator());
    defer arena.deinit();
    const a = arena.allocator();

    _ = decodeRleHybrid(a, src, width6 % 33, count) catch {};
    _ = decodeDeltaBinaryPacked(a, src, count) catch {};
    _ = decodeDeltaLengthByteArray(a, src, count) catch {};
    _ = decodeDeltaByteArray(a, src, count) catch {};
    _ = decodeByteStreamSplit(a, src, @max(1, @as(usize, width6 % 17)), count) catch {};

    // PLAIN decode across every physical type, including the deprecated int96.
    inline for (.{ .boolean, .int32, .int64, .int96, .float, .double, .byte_array, .fixed_len_byte_array }) |pt| {
        var cur = PlainCursor.init(pt, @intCast(width6), src);
        var n: usize = 0;
        while (n < count) : (n += 1) {
            _ = cur.next() catch break;
        }
    }
}

const fuzzKernels_corpus = [_][]const u8{
    "\x03\x01\x00" ++ "\x03\x88\x01\x02\x03", // small RLE-ish seed
    "\x02\x00\x08" ++ "\x80\x01\x04\x05\x00\x01\x02\x03\x04", // delta-ish seed
};

test "fuzz: page decode kernels survive arbitrary bytes" {
    try std.testing.fuzz({}, fuzzKernels, .{ .corpus = &fuzzKernels_corpus });
    try @import("fuzzutil.zig").pound(fuzzKernels, &fuzzKernels_corpus);
}

test "BitReader: wide values at non-zero bit offsets keep their top bits" {
    // Layout: 3 one-bits, then a 61-bit value, then a 64-bit value. Before the
    // two-word path, any read whose shift + width crossed 64 bits silently
    // zeroed the bits beyond the first word — a wrong VALUE, not an error.
    const v61: u64 = 0x1ABC_DEF0_1234_5678 & ((1 << 61) - 1);
    const v64: u64 = 0xFEDC_BA98_7654_3210;
    var bits: [17]u8 = @splat(0);
    var w = std.io.Writer.fixed(&bits);
    _ = &w;
    // Pack by hand, LSB-first: bit 0..2 = 0b111, then v61, then v64.
    var acc: u128 = 0b111;
    acc |= @as(u128, v61) << 3;
    var acc2: u128 = @as(u128, v64) << ((3 + 61) % 8); // second region starts at bit 64
    _ = &acc2;
    var all: [16]u8 = undefined;
    std.mem.writeInt(u128, &all, acc | (@as(u128, v64) << 64), .little);
    var br = BitReader{ .buf = &all };
    try std.testing.expectEqual(@as(u64, 0b111), try br.read(3));
    try std.testing.expectEqual(v61, try br.read(61));
    try std.testing.expectEqual(v64, try br.read(64));

    // Reading past the buffer is an error, not a partial value.
    var short = BitReader{ .buf = all[0..8] };
    _ = try short.read(3);
    try std.testing.expectError(Error.CorruptParquetPage, short.read(64));
}

test "decodeDeltaBinaryPacked: a miniblock width above 64 is a corrupt page" {
    var mem: [4096]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&mem);
    // header: block=128, miniblocks=1, total=2, first=0; then min_delta=0 and
    // a width byte of 255 — the exact byte the mutation harness found.
    const page = [_]u8{ 0x80, 0x01, 0x01, 0x02, 0x00, 0x00, 0xFF };
    try std.testing.expectError(
        Error.CorruptParquetPage,
        decodeDeltaBinaryPacked(fba.allocator(), &page, 2),
    );
}

fn listColumn(a: std.mem.Allocator, vals: []const Value) !column.Column {
    var b = column.Builder.init(a, types.Type.init(.int).asNullable());
    for (vals) |v| try b.append(v);
    return b.finish();
}

test "assembleLists: null, empty, a null element, and nested lists from levels" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    // optional group xs (LIST) { repeated group list { optional int64 element } }
    // defs: 0 = xs null, 1 = empty, 2 = null element, 3 = value
    const flat = ListShape{ .rep_def = &.{2} };
    const vals = [_]Value{ .{ .int = 1 }, .{ .int = 2 }, .null, .null, .null, .{ .int = 5 } };
    const reps = [_]u32{ 0, 1, 0, 0, 0, 1 };
    const defs = [_]u32{ 3, 3, 1, 0, 2, 3 };
    const got = try assembleLists(a, try listColumn(a, &vals), &reps, &defs, 3, flat, 4);
    try testing.expectEqualStrings("[1,2]", got.getValue(0).string);
    try testing.expectEqualStrings("[]", got.getValue(1).string);
    try testing.expect(got.getValue(2) == .null);
    try testing.expectEqualStrings("[null,5]", got.getValue(3).string);

    // list<list<int>>, both levels optional: rep_def = {2, 4}, max_def = 5
    const nested = ListShape{ .rep_def = &.{ 2, 4 } };
    const nvals = [_]Value{ .{ .int = 1 }, .{ .int = 2 }, .{ .int = 3 }, .null, .null, .{ .int = 4 } };
    const nreps = [_]u32{ 0, 1, 2, 0, 1, 1 };
    const ndefs = [_]u32{ 5, 5, 5, 3, 2, 5 };
    const ngot = try assembleLists(a, try listColumn(a, &nvals), &nreps, &ndefs, 5, nested, 2);
    try testing.expectEqualStrings("[[1],[2,3]]", ngot.getValue(0).string);
    try testing.expectEqualStrings("[[],null,[4]]", ngot.getValue(1).string);

    // a row count that disagrees with the levels, or a repetition deeper than
    // the list, is a corrupt page — never a wrong answer
    try testing.expectError(Error.CorruptParquetPage, assembleLists(a, try listColumn(a, &vals), &reps, &defs, 3, flat, 5));
    const bad_reps = [_]u32{ 0, 2, 0, 0, 0, 1 };
    try testing.expectError(Error.CorruptParquetPage, assembleLists(a, try listColumn(a, &vals), &bad_reps, &defs, 3, flat, 4));
    // a repeated entry below its own level's threshold: an element that is not one
    const bad_defs = [_]u32{ 3, 1, 1, 0, 2, 3 };
    try testing.expectError(Error.CorruptParquetPage, assembleLists(a, try listColumn(a, &vals), &reps, &bad_defs, 3, flat, 4));
}

fn readAllText(a: std.mem.Allocator, bytes: []const u8, name: []const u8) ![]const u8 {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = name, .data = bytes });
    const dir = try tmp.dir.realpathAlloc(a, ".");
    const r = try Reader.open(a, try std.fs.path.join(a, &.{ dir, name }));
    defer r.close();
    var out = std.Io.Writer.Allocating.init(a);
    while (try r.next(a)) |b| {
        for (0..b.len) |i| {
            for (b.columns, r.schema.fields, 0..) |c, f, k| {
                if (k > 0) try out.writer.writeByte(' ');
                const v = c.getValue(i);
                try out.writer.print("{s}={s}", .{ f.name, if (v == .null) "null" else try eval.valueToString(a, v) });
            }
            try out.writer.writeByte('\n');
        }
    }
    return out.written();
}

test "parquet LIST columns read as JSON from pyarrow (pages v1 and v2) and polars" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    // `recs` (a list of structs) and `m` (a map) span several leaves and are
    // assembled from all of them: objects in an array, and an object
    const want =
        \\id=1 xs=[1,2] nest=[[1],[2,3]] ds=["2026-01-01"] recs=[{"a":1,"b":"x"}] m={"k":1}
        \\id=2 xs=[] nest=[[]] ds=null recs=null m=null
        \\id=3 xs=null nest=null ds=[] recs=[] m={}
        \\id=4 xs=[null,5] nest=[null,[4]] ds=["1969-12-31",null] recs=[{"a":2,"b":"y"}] m={"z":2}
        \\
    ;
    // v1: two row groups of two rows, uncompressed; v2: one group, snappy
    try testing.expectEqualStrings(want, try readAllText(a, @embedFile("testdata/lists_v1.parquet"), "v1.parquet"));
    try testing.expectEqualStrings(want, try readAllText(a, @embedFile("testdata/lists_v2.parquet"), "v2.parquet"));
    try testing.expectEqualStrings(
        \\id=1 xs=[1,2] ss=["a"]
        \\id=2 xs=[] ss=["b","c"]
        \\id=3 xs=null ss=null
        \\id=4 xs=[null,5] ss=[]
        \\
    , try readAllText(a, @embedFile("testdata/lists_polars.parquet"), "p.parquet"));
}

test "a nested column is typed string, read whole, and no bound prunes on it" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "l.parquet", .data = @embedFile("testdata/lists_v1.parquet") });
    const dir = try tmp.dir.realpathAlloc(a, ".");
    const r = try Reader.open(a, try std.fs.path.join(a, &.{ dir, "l.parquet" }));
    defer r.close();
    try testing.expectEqual(types.TypeKind.string, r.schema.fields[r.schema.indexOf("xs").?].ty.kind);
    // every column of the file, nothing left out
    const names = [_][]const u8{ "id", "xs", "nest", "ds", "recs", "m" };
    try testing.expectEqual(names.len, r.schema.fields.len);
    for (names, r.schema.fields) |n, f| try testing.expectEqualStrings(n, f.name);
    try testing.expectEqual(types.TypeKind.string, r.schema.fields[4].ty.kind);
    // a bound on a nested column's name proves nothing either
    const rb = [_]Bound{.{ .column = "recs", .op = .eq, .value = .{ .string = "zzz" } }};
    for (r.md.row_groups) |g| try testing.expect(groupMayMatch(r.md.schema, r.leaves, g, &rb));
    // the element statistics say 1..5; a bound on the column must not trust them
    const b = [_]Bound{.{ .column = "xs", .op = .gt, .value = .{ .int = 100 } }};
    for (r.md.row_groups) |g| try testing.expect(groupMayMatch(r.md.schema, r.leaves, g, &b));
}

fn entriesOf(a: std.mem.Allocator, vals: []const Value, reps: []const u32, defs: []const u32) !Entries {
    return .{ .vals = try listColumn(a, vals), .reps = reps, .defs = defs };
}

test "assembleNested: leaves that disagree, or a row count that does not match, are a corrupt page" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    // optional group recs (LIST) { repeated group list { optional group element { optional int a; optional int b } } }
    const schema = [_]parquet.SchemaElement{
        .{ .name = "root", .num_children = 1 },
        .{ .name = "recs", .repetition = .optional, .num_children = 1, .converted_type = 3 },
        .{ .name = "list", .repetition = .repeated, .num_children = 1 },
        .{ .name = "element", .repetition = .optional, .num_children = 2 },
        .{ .name = "a", .ty = .int64, .repetition = .optional },
        .{ .name = "b", .ty = .int64, .repetition = .optional },
    };
    const root = try buildNested(a, &schema, .{ .idx = 1, .base_def = 0, .base_rep = 0 });
    // row 0: [{a:1,b:2},{a:null,b:3}]; row 1: null; row 2: []; row 3: [null]
    const ea = try entriesOf(a, &.{ .{ .int = 1 }, .null, .null, .null, .null }, &.{ 0, 1, 0, 0, 0 }, &.{ 4, 3, 0, 1, 2 });
    const eb = try entriesOf(a, &.{ .{ .int = 2 }, .{ .int = 3 }, .null, .null, .null }, &.{ 0, 1, 0, 0, 0 }, &.{ 4, 4, 0, 1, 2 });
    const got = try assembleNested(a, &root, &.{ ea, eb }, 4);
    try testing.expectEqualStrings("[{\"a\":1,\"b\":2},{\"a\":null,\"b\":3}]", got.getValue(0).string);
    try testing.expect(got.getValue(1) == .null);
    try testing.expectEqualStrings("[]", got.getValue(2).string);
    try testing.expectEqualStrings("[null]", got.getValue(3).string);

    // `b` claims a third element in row 0 that `a` does not have
    const eb3 = try entriesOf(a, &.{ .{ .int = 2 }, .{ .int = 3 }, .{ .int = 9 }, .null, .null, .null }, &.{ 0, 1, 1, 0, 0, 0 }, &.{ 4, 4, 4, 0, 1, 2 });
    try testing.expectError(Error.CorruptParquetPage, assembleNested(a, &root, &.{ ea, eb3 }, 4));
    // a row count the levels do not have
    try testing.expectError(Error.CorruptParquetPage, assembleNested(a, &root, &.{ ea, eb }, 5));
    // a leaf missing altogether
    try testing.expectError(Error.CorruptParquetPage, assembleNested(a, &root, &.{ea}, 4));
}

test "a nested fixture with bytes flipped anywhere errors or reads, never crashes" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    inline for (.{ "testdata/lists_v1.parquet", "testdata/lists_v2.parquet" }) |fixture| {
        const good = @embedFile(fixture);
        var buf: [good.len]u8 = undefined;
        // Every byte, one mask: this sweep found a negative row count, a
        // negative chunk offset and an empty schema reaching unchecked casts,
        // and a pre-year-0 date trapping in the formatter.
        var i: usize = 0;
        while (i < good.len) : (i += 1) {
            @memcpy(&buf, good);
            buf[i] ^= 0x5A;
            _ = readAllText(a, &buf, "f.parquet") catch continue;
        }
    }
}
