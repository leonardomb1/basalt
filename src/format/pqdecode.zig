//! Parquet value decoding and the file reader: levels, encodings, page-to-column
//! assembly, row-group pruning, and the byte sources a file is read from.
//!
//! A decompressed page is still encoded. This module turns those bytes into
//! `Value`s, or straight into a column's typed store on the hot paths, and builds
//! a basalt `Column` from them. Encodings handled: PLAIN, the RLE/bit-packed
//! hybrid used for levels and dictionary indices (`PLAIN_DICTIONARY` and
//! `RLE_DICTIONARY` share a wire format), DELTA_BINARY_PACKED,
//! DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY and BYTE_STREAM_SPLIT, in data pages
//! v1 and v2. Every length, count, width and offset comes off the wire and is
//! attacker-controlled, so each is checked before use as a length, shift or cast:
//! a hostile file yields `CorruptParquetPage`, never a trap or a wrong value.
//!
//! Types come from the LogicalType when the writer gave one, else the legacy
//! ConvertedType. TIME and TIMESTAMP are stored in millis, micros or nanos and
//! always read as micros (`TemporalScale`): ignoring the unit once read millisecond
//! timestamps as 1970. Nanos floor-divide, which is monotone, so statistics-based
//! pruning stays sound after conversion.
//!
//! A struct's fields read as flat dotted columns (`addr.city`). A list of scalars,
//! any depth, reads as one `string` column of JSON arrays named for the list,
//! assembled from repetition and definition levels (`ListShape`), which
//! `JSON_EACH` and `json_get` take apart. A list of structs, a map, and any
//! nesting of them span several leaves and are rebuilt row by row from all of
//! their entries into one JSON column (`NNode`, `Assembler`): objects for structs
//! and maps, arrays for lists. Within a row every leaf's entries are contiguous (a
//! repetition level of 0 starts the next row); a repeated node at repetition
//! level R starts an element at every entry whose level is R, and a definition
//! level below a node's own says it is absent — null if optional, an empty list if
//! repeated. Every leaf has an entry for every instance of every ancestor, so the
//! first entry of a node's first leaf answers whether the node is there. Chunks
//! appear in leaf order including leaves a read skips, hence `Leaf.chunk_idx`.
//!
//! Row-group pruning (`groupMayMatch`, `groupBeatsThreshold`, `fileMinMax`) is
//! conservative in one direction only: a group is skipped solely when statistics
//! prove no row can match; anything missing, unknown or unorderable keeps it.
//!
//! `Reader` reads one batch per row group, fetching only the footer and the
//! column chunks a query projects, so resident memory tracks the widest row
//! group's projected columns rather than the file. `Bytes` is where those bytes
//! come from: a local file by pread, a resident buffer, an SFTP or SMB file by
//! offset, or a `Remote` over HTTP range requests (`http(s)://`, or an `az://`
//! blob whose Shared Key signature is redone per range). A server that answers a
//! ranged GET with 200 has sent the whole body; `Remote` keeps it in `whole`,
//! copied into the reader's arena because the batch arena passed to `read` is
//! recycled per batch, and serves later reads from it. `Folder` reads a folder
//! of files as one table; every file must match the first's projected columns in
//! name, order and type, or the read fails naming it rather than misplace values.

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
    UnsupportedParquetSchema,
    UnsupportedParquetEncoding,
} || std.mem.Allocator.Error;

pub const BitReader = struct {
    buf: []const u8,
    bit_pos: usize = 0,

    /// LSB-first, the order Parquet's bit-packing uses, a whole u64 word at a time.
    /// Width is `u7` because INT64 delta miniblocks may use 64; a value spanning two
    /// words is assembled from both (one word once silently zeroed its top bits).
    pub fn read(self: *BitReader, width: u7) Error!u64 {
        if (width == 0) return 0;
        const end = self.bit_pos + width;
        if ((end + 7) >> 3 > self.buf.len) return Error.CorruptParquetPage;

        const byte = self.bit_pos >> 3;
        const shift: u6 = @intCast(self.bit_pos & 7);
        self.bit_pos = end;

        if (@as(usize, shift) + width <= 64) {
            var word: u64 = 0;
            if (byte + 8 <= self.buf.len) {
                word = std.mem.readInt(u64, self.buf[byte..][0..8], .little);
            } else {
                var k: usize = 0;
                while (byte + k < self.buf.len and k < 8) : (k += 1) {
                    word |= @as(u64, self.buf[byte + k]) << @intCast(8 * k);
                }
            }
            const v = word >> shift;
            return if (width == 64) v else v & ((@as(u64, 1) << @intCast(width)) - 1);
        }

        var word: u128 = 0;
        var k: usize = 0;
        while (byte + k < self.buf.len and k < 9) : (k += 1) {
            word |= @as(u128, self.buf[byte + k]) << @intCast(8 * k);
        }
        const v: u64 = @truncate(word >> shift);
        return if (width == 64) v else v & ((@as(u64, 1) << @intCast(width)) - 1);
    }
};

pub fn bitWidth(max: u32) u6 {
    if (max == 0) return 0;
    return @intCast(32 - @clz(max));
}

/// RLE/bit-packed hybrid: each run's varint header has its low bit set for
/// `(header >> 1) * 8` bit-packed values, clear for an RLE run of `header >> 1`
/// copies. Run counts are checked before they are multiplied, as they can wrap.
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
            const groups = std.math.cast(usize, header >> 1) orelse return Error.CorruptParquetPage;
            const bytes = std.math.mul(usize, groups, width) catch return Error.CorruptParquetPage;
            if (bytes > src.len - pos) return Error.CorruptParquetPage;
            const vals = groups * 8;
            const run = src[pos..][0..bytes];
            const take = @min(vals, count - n);
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
            if (run == 0) return Error.CorruptParquetPage;
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

pub const PlainCursor = struct {
    src: []const u8,
    pos: usize = 0,
    ty: parquet.PhysicalType,
    type_length: usize = 0,
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

/// INT96: Julian day (2440588 is 1970-01-01) plus nanoseconds of day. Saturates,
/// since a u32 Julian day of micros overflows i64 on a corrupt file.
pub fn int96ToMicros(julian_day: u32, nanos_of_day: u64) i64 {
    const days: i64 = @as(i64, julian_day) - 2_440_588;
    return (days *| 86_400_000_000) +| @as(i64, @intCast(nanos_of_day / 1000));
}

/// DELTA_BINARY_PACKED: `<block size> <miniblocks> <count> <first>`, then per block
/// `<min delta> <widths> <miniblocks>`. Running sums desync on any miscount, so
/// every bound is checked; a width byte over 64 is corrupt (it was once a panic).
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
    while (n < count) : (n += 1) out[n] = value;
    return out;
}

fn readZigZagAt(src: []const u8, pos: *usize) Error!i64 {
    const u = try readVarint(src, pos);
    return @as(i64, @bitCast(u >> 1)) ^ -@as(i64, @intCast(u & 1));
}

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

/// DELTA_BYTE_ARRAY: prefix lengths shared with the previous value, suffix
/// lengths, then the suffix bytes.
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

/// BYTE_STREAM_SPLIT: every value's first byte, then every second byte, and so
/// on; regrouping restores the little-endian values.
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

const conv_utf8 = 0;
const conv_decimal = 5;
const conv_date = 6;
const conv_time_millis = 7;
const conv_time_micros = 8;
const conv_timestamp_millis = 9;
const conv_timestamp_micros = 10;
const conv_json = 19;
const conv_bson = 20;

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

pub const Leaf = struct {
    schema_idx: usize,
    chunk_idx: usize,
    name: []const u8,
    max_def: u32,
    max_rep: u32,
    list: ?ListShape = null,
    root: ?RootRef = null,

    pub fn isRepeated(self: Leaf) bool {
        return self.max_rep > 0;
    }
};

pub const ListShape = struct {
    rep_def: []const u32,
};

pub const RootRef = struct { idx: usize, base_def: u32, base_rep: u32 };

const PathNode = struct { name: []const u8, idx: usize, def: u32, repeated: bool, list_group: bool };

fn isListGroup(e: parquet.SchemaElement) bool {
    if (e.converted_type) |c| if (c == 1 or c == 2 or c == 3) return true;
    return false;
}

/// Resolves each leaf's levels and dotted name from its ancestors. A list is
/// named at a LIST/MAP group wrapping its first repeated node (three-level), else
/// at that node (legacy two-level). Depth is capped so a hostile schema cannot recurse forever.
pub fn collectLeaves(arena: std.mem.Allocator, schema: []const parquet.SchemaElement) Error![]Leaf {
    var out = std.array_list.Managed(Leaf).init(arena);
    var path = std.array_list.Managed(PathNode).init(arena);
    if (schema.len == 0) return Error.UnsupportedParquetSchema;
    var pos: usize = 1;
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

fn joinPath(arena: std.mem.Allocator, nodes: []const PathNode) ![]const u8 {
    var buf = std.array_list.Managed(u8).init(arena);
    for (nodes, 0..) |pn, i| {
        if (i > 0) try buf.append('.');
        try buf.appendSlice(pn.name);
    }
    return buf.toOwnedSlice();
}

/// A dictionary page, if present, fills the dictionary later data pages index
/// into. `base_offset` is where `file_bytes[0]` sits in the file.
pub fn readColumnChunk(
    arena: std.mem.Allocator,
    file_bytes: []const u8,
    meta: parquet.ColumnMetaData,
    elem: parquet.SchemaElement,
    rows: usize,
    max_def: u32,
    base_offset: u64,
) (Error || parquet.Error || @import("codec.zig").Error)!column.Column {
    return readColumnChunkLevels(arena, file_bytes, meta, elem, rows, max_def, 0, null, base_offset);
}

/// With `list` set, the chunk's entries (one per level pair, not per row) are
/// assembled into a JSON array per row.
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

    const entries = if (list != null) std.math.cast(usize, meta.num_values) orelse return Error.CorruptParquetPage else rows;
    var levels: ?Levels = if (list != null) .{
        .reps = std.array_list.Managed(u32).init(arena),
        .defs = std.array_list.Managed(u32).init(arena),
    } else null;

    var b = try column.Builder.initCapacity(arena, ty, entries);
    var dict: ?[]Value = null;
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
                produced += try appendDataPage(arena, &b, pg, meta, elem, ty, max_def, dict, tscale, max_rep, if (levels) |*l| l else null);
            },
            .index_page => {},
            else => return Error.CorruptParquetPage,
        }
    }
    const col = try b.finish();
    if (list) |shape| return assembleLists(arena, col, levels.?.reps.items, levels.?.defs.items, max_def, shape, rows);
    return col;
}

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

pub const NNode = struct {
    name: []const u8,
    def: u32,
    rep: u32,
    optional: bool,
    repeated: bool,
    kind: Kind,
    children: []NNode = &.{},
    first_leaf: usize,
    nleaves: usize,
    max_def: u32 = 0,

    pub const Kind = enum { leaf, group, list, map };
};

pub const Nested = struct {
    name: []const u8,
    root: NNode,
    leaves: []const usize,
};

/// Leaves are numbered depth-first, the order their chunks appear in. LIST and MAP
/// are honoured only in the spec's single-repeated-child shape.
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
    node.kind = if (n == 1 and kids[0].repeated and conv == 3)
        .list
    else if (n == 1 and kids[0].repeated and (conv == 1 or conv == 2) and kids[0].children.len == 2)
        .map
    else
        .group;
    return node;
}

pub fn buildNested(arena: std.mem.Allocator, schema: []const parquet.SchemaElement, root: RootRef) Error!NNode {
    var pos = root.idx;
    var next: usize = 0;
    return buildNode(arena, schema, &pos, root.base_def, root.base_rep, &next, 0);
}

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

    fn field(self: *Assembler, n: *const NNode, spans: []const Span) anyerror!void {
        if (n.repeated) return self.array(n, spans, elementOf(n, true));
        return self.value(n, spans);
    }

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

    /// The node itself for a bare repeated field, or a repeated group with several
    /// fields or a legacy name (`array`, `*_tuple`); else, three-level, its one child.
    fn elementOf(rep: *const NNode, bare: bool) ?*const NNode {
        if (bare or rep.kind == .leaf or rep.children.len != 1) return null;
        if (std.mem.eql(u8, rep.name, "array") or std.mem.endsWith(u8, rep.name, "_tuple")) return null;
        return &rep.children[0];
    }

    /// One element per entry of the first leaf at `rep.rep`; none when the first
    /// entry's definition stops short of `rep.def`.
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
                var as_value = rep.*;
                as_value.repeated = false;
                as_value.optional = false;
                try self.value(&as_value, sub);
            }
        }
        try self.out.append(']');
    }

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

    /// Cuts where a new element at repetition level `r` begins. Every entry within an
    /// instance repeats at `r` or deeper.
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

pub fn assembleNested(arena: std.mem.Allocator, root: *const NNode, entries: []const Entries, rows: usize) anyerror!column.Column {
    if (entries.len != root.nleaves) return Error.CorruptParquetPage;
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
        if (std.mem.eql(u8, buf.items, "null")) try out.append(.null) else try out.append(.{ .string = buf.items });
    }
    return out.finish();
}

const Levels = struct {
    reps: std.array_list.Managed(u32),
    defs: std.array_list.Managed(u32),
};

/// Repetition 0 starts a row and `r > 0` a new element at depth `r`; below it,
/// each level's definition threshold says element, empty list, or null.
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
    var open: usize = 0;
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
            if (d + 1 < shape.rep_def[0]) {
                row_null = true;
                continue;
            }
            try buf.append('[');
            open = 1;
        } else {
            if (!in_row or row_null or r > open or d < shape.rep_def[r - 1]) return Error.CorruptParquetPage;
            while (open > r) : (open -= 1) try buf.append(']');
            try buf.append(',');
        }
        var lvl = open;
        while (true) {
            if (d < shape.rep_def[lvl - 1]) break;
            if (lvl == depth) {
                try jsonValue(arena, &buf, if (d < max_def) .null else elems.getValue(i));
                break;
            }
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

/// v1 length-prefixes each level section; v2 keeps them unprefixed with lengths
/// in the header. The bulk path keys on `present == n`, not on absent levels:
/// most writers mark every column OPTIONAL even when nothing is null.
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
    levels: ?*Levels,
) Error!usize {
    const n = std.math.cast(usize, pg.header.num_values) orelse return Error.CorruptParquetPage;
    var body = pg.data;

    var reps: ?[]u32 = null;
    var defs: ?[]u32 = null;
    if (pg.header.ty == .data_page_v2) {
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

    var present: usize = n;
    if (defs) |d| {
        present = 0;
        for (d) |lvl| {
            if (lvl == max_def) present += 1;
        }
    }

    switch (pg.header.encoding) {
        .plain => {
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
        .rle => {
            const bits = try decodeRleHybrid(arena, body[@min(4, body.len)..], 1, present);
            const vals = try arena.alloc(i64, present);
            for (vals, bits) |*v, x| v.* = @intCast(x);
            try emitInts(b, ty, defs, max_def, n, vals, tscale);
        },
        .plain_dictionary, .rle_dictionary => {
            const d = dict orelse return Error.CorruptParquetPage;
            if (body.len < 1) return Error.CorruptParquetPage;
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

/// Decodes a PLAIN page straight into the typed store, or returns false for a
/// shape it does not cover. Byte arrays are slices of the page body.
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
    const count = present;
    switch (phys) {
        .int64 => {
            if (body.len < count * 8) return Error.CorruptParquetPage;
            switch (ty.kind) {
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

/// Turns the dictionary into a typed array once and gathers indices through it,
/// rather than boxing every row into a `Value`. False for a shape not covered.
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
            const entries = try arena.alloc([]const u8, dict.len);
            for (entries, dict) |*o, v| o.* = v.bytes;
            try b.noteDict(@intFromPtr(dict.ptr), entries, idx, defs, max_def);
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

/// A row whose definition level is below the maximum has no stored value, so the
/// value stream does not advance for it.
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

pub const Bound = struct {
    column: []const u8,
    op: Op,
    value: Value,

    pub const Op = enum { lt, le, gt, ge, eq };
};

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

        const excluded = switch (b.op) {
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

/// Whether a row group could hold a row entering the current top-N; only a
/// proven strict miss returns false.
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

    if (t.desc) {
        const hi = statValue(elem, meta.ty, meta.stats.max) orelse return true;
        return cmp(hi, t.value) == .gt;
    }
    const lo = statValue(elem, meta.ty, meta.stats.min) orelse return true;
    return cmp(lo, t.value) == .lt;
}

pub const MinMax = struct { min: Value, max: Value };

/// Folded from row-group statistics; null as soon as one group lacks them or the
/// type cannot be ordered (it once returned row group 0's value instead).
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

/// A list column never matches: its chunk statistics describe the elements, not
/// the JSON text, so a bound on it proves nothing.
fn findLeaf(leaves: []const Leaf, name: []const u8) ?Leaf {
    for (leaves) |lf| {
        if (std.mem.eql(u8, lf.name, name)) return if (lf.list != null) null else lf;
    }
    return null;
}

pub fn leafType(e: parquet.SchemaElement, lf: Leaf) Error!types.Type {
    if (lf.list != null) {
        _ = try basaltType(e);
        return types.Type.init(.string).asNullable();
    }
    return (try basaltType(e)).asNullable();
}

fn statValue(elem: parquet.SchemaElement, phys: parquet.PhysicalType, raw: ?[]const u8) ?Value {
    const b = raw orelse return null;
    const ty = basaltType(elem) catch return null;
    var cur = PlainCursor.init(phys, elem.type_length orelse 0, b);
    const v = cur.next() catch return null;
    return coerce(ty, v, temporalScale(elem));
}

/// Null means unknown and the caller must keep the group. It once returned `.eq`
/// for unknown pairs, which pruned every group of a DECIMAL or date-vs-string range.
/// A date against an ISO string literal orders by parsing it (dialect §9).
fn cmp(a: Value, b: Value) ?std.math.Order {
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
    const d: Value = .{ .date = 9131 };
    try std.testing.expectEqual(std.math.Order.lt, cmp(.{ .date = 9130 }, .{ .string = "1995-01-01" }).?);
    try std.testing.expectEqual(std.math.Order.eq, cmp(d, .{ .string = "1995-01-01" }).?);
    try std.testing.expectEqual(std.math.Order.gt, cmp(.{ .date = 9132 }, .{ .string = "1995-01-01" }).?);
    try std.testing.expect(cmp(d, .{ .string = "not-a-date" }) == null);
    try std.testing.expect(cmp(d, .{ .bool = true }) == null);
    try std.testing.expect(cmp(.{ .int = 5 }, .{ .string = "5" }) == null);
    try std.testing.expect(cmp(.{ .string = "a" }, .{ .int = 1 }) == null);
    try std.testing.expectEqual(std.math.Order.lt, cmp(.{ .timestamp = 0 }, .{ .string = "1970-01-02" }).?);
}

fn isNumV(v: Value) bool {
    return v == .int or v == .float or v == .decimal;
}

/// Numeric ordering via `eval.compareValues`, used only when both sides are
/// numeric, so pruning agrees with the filter that re-checks rows.
fn numOrder(a: Value, b: Value) ?std.math.Order {
    if (!isNumV(a) or !isNumV(b)) return null;
    return eval.compareValues(a, b);
}

const driver = @import("../connect/driver.zig");
const Batch = @import("../exec/batch.zig").Batch;
const http_client = @import("../net/http_client.zig");
const objstore = @import("../store/objstore.zig");
const sftp = @import("../store/sftp.zig");
const smb = @import("../store/smb.zig");

pub const Bytes = union(enum) {
    memory: []const u8,
    file: struct { f: std.fs.File, size: u64 },
    remote: *Remote,
    sftp: *sftp.File,
    smb: *smb.File,

    pub fn open(arena: std.mem.Allocator, path: []const u8) !Bytes {
        if (sftp.isUrl(path)) return .{ .sftp = try sftp.File.open(arena, path) };
        if (smb.isUrl(path)) return .{ .smb = try smb.File.open(arena, path) };
        if (isRemote(path)) return .{ .remote = try Remote.open(arena, path) };
        const f = try std.fs.cwd().openFile(path, .{});
        errdefer f.close();
        return .{ .file = .{ .f = f, .size = (try f.stat()).size } };
    }

    pub fn size(self: Bytes) u64 {
        return switch (self) {
            .memory => |m| m.len,
            .file => |x| x.size,
            .remote => |r| r.total,
            .sftp => |f| f.size,
            .smb => |f| f.size,
        };
    }

    /// Owned by `arena` for file and remote sources, borrowed for memory; read-only.
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
            .sftp => |f| {
                if (off + len > f.size) return Error.CorruptParquetPage;
                return f.read(arena, off, len) catch |e| switch (e) {
                    error.EndOfStream => Error.CorruptParquetPage,
                    else => e,
                };
            },
            .smb => |f| {
                if (off + len > f.size) return Error.CorruptParquetPage;
                return f.read(arena, off, len) catch |e| switch (e) {
                    error.EndOfStream => Error.CorruptParquetPage,
                    else => e,
                };
            },
        }
    }

    pub fn close(self: Bytes) void {
        switch (self) {
            .memory => {},
            .file => |x| x.f.close(),
            .remote => |r| r.client.deinit(),
            .sftp => |f| f.close(),
            .smb => |f| f.close(),
        }
    }
};

pub const Remote = struct {
    arena: std.mem.Allocator,
    client: *std.http.Client,
    url: []const u8,
    object: ?objstore.Object = null,
    total: u64,
    whole: ?[]const u8 = null,
    repaired: bool = false,

    /// Sizes the object by HEAD; without a length it fetches the whole object once.
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

        if (self.contentLength(arena)) |n| {
            self.total = n;
        } else |_| {
            const body = try self.fetchWhole(arena);
            self.whole = body;
            self.total = body.len;
        }
        return self;
    }

    /// Slices `whole` once a 200 supplied it, never past its real length, which may
    /// disagree with HEAD's.
    pub fn read(self: *Remote, arena: std.mem.Allocator, off: u64, len: usize) ![]const u8 {
        if (len == 0) return "";
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

    /// A TLS retry gets its own buffer, so a failed attempt's partial response is
    /// not prepended to the retry's body.
    fn send(
        self: *Remote,
        arena: std.mem.Allocator,
        method: std.http.Method,
        range_hdr: []const u8,
    ) !Resp {
        const extra = try self.headers(arena, method, range_hdr);
        return self.once(arena, method, extra) catch |e| switch (e) {
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

pub const Output = union(enum) {
    leaf: usize,
    nested: *const Nested,
};

pub const Reader = struct {
    arena: std.mem.Allocator,
    src: Bytes,
    md: parquet.FileMetaData,
    schema: types.Schema,
    leaves: []const Leaf,
    outputs: []const Output = &.{},
    boundaries: []const u64 = &.{},
    bounds: []const Bound = &.{},
    threshold: ?*const Threshold = null,
    groups_skipped: usize = 0,
    tally: ?*driver.ScanTally = null,
    rg: usize = 0,
    rg_end: ?usize = null,

    pub fn isPath(path: []const u8) bool {
        return std.mem.endsWith(u8, path, ".parquet");
    }

    pub fn open(arena: std.mem.Allocator, path: []const u8) !*Reader {
        return openProjected(arena, path, null);
    }

    /// `open`, decoding only the named columns. An unknown name is ignored; an empty
    /// projection (COUNT(*)) keeps the narrowest column so batches carry a row count.
    pub fn openProjected(arena: std.mem.Allocator, path: []const u8, want: ?[]const []const u8) !*Reader {
        const src = try Bytes.open(arena, path);
        errdefer src.close();

        var footer_start: u64 = 0;
        const md = try parseFooterOf(arena, src, &footer_start);

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
                const rt = lf.root.?;
                for (roots_seen.items) |x| {
                    if (x == rt.idx) break;
                } else {
                    try roots_seen.append(rt.idx);
                    if (!wanted(want, lf.name)) continue;
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

    /// One row group per call. Empty groups are skipped, since a zero-row batch reads
    /// as end-of-stream downstream.
    pub fn next(self: *Reader, arena: std.mem.Allocator) !?Batch {
        const last = self.rg_end orelse self.md.row_groups.len;
        while (self.rg < last) {
            const g = self.md.row_groups[self.rg];
            self.rg += 1;
            const rows = std.math.cast(usize, g.num_rows) orelse return Error.CorruptParquetPage;
            if (rows == 0) continue;
            if (self.tally) |t| driver.ScanTally.add(&t.row_groups, 1);
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

/// How a folder file's columns differ from the first file's, in words; null when
/// they do not. Nullability is not compared.
pub fn schemaMismatch(arena: std.mem.Allocator, want: types.Schema, got: types.Schema) ?[]const u8 {
    for (want.fields, 0..) |w, k| {
        if (k >= got.fields.len) return std.fmt.allocPrint(arena, "it has no column `{s}`", .{w.name}) catch "a column is missing";
        const g = got.fields[k];
        if (!std.mem.eql(u8, w.name, g.name)) {
            for (got.fields) |o| if (std.mem.eql(u8, o.name, w.name))
                return std.fmt.allocPrint(arena, "its columns are in another order (`{s}` where `{s}` is expected)", .{ g.name, w.name }) catch "columns in another order";
            return std.fmt.allocPrint(arena, "it has no column `{s}`", .{w.name}) catch "a column is missing";
        }
        if (w.ty.kind != g.ty.kind or w.ty.precision != g.ty.precision or w.ty.scale != g.ty.scale)
            return std.fmt.allocPrint(arena, "column `{s}` is {s} there, not {s}", .{ w.name, @tagName(g.ty.kind), @tagName(w.ty.kind) }) catch "a column has another type";
    }
    if (got.fields.len > want.fields.len)
        return std.fmt.allocPrint(arena, "it has a column `{s}` the first file lacks", .{got.fields[want.fields.len].name}) catch "an extra column";
    return null;
}

pub fn mismatchMessage(arena: std.mem.Allocator, root: []const u8, path: []const u8, first: []const u8, why: []const u8) []const u8 {
    const rel = struct {
        fn f(r: []const u8, x: []const u8) []const u8 {
            return if (std.mem.startsWith(u8, x, r)) x[r.len..] else x;
        }
    }.f;
    return std.fmt.allocPrint(arena, "`{s}` in folder `{s}` does not match `{s}`, its first file: {s}", .{ rel(root, path), root, rel(root, first), why }) catch why;
}

pub const Folder = struct {
    arena: std.mem.Allocator,
    root: []const u8,
    files: []const []const u8,
    project: ?[]const []const u8,
    schema: types.Schema,
    bounds: []const Bound = &.{},
    threshold: ?*const Threshold = null,
    tally: ?*driver.ScanTally = null,
    cur: ?*Reader = null,
    i: usize = 0,

    pub fn open(arena: std.mem.Allocator, root: []const u8, files: []const []const u8, project: ?[]const []const u8) !*Folder {
        if (files.len == 0) return error.EmptyFolder;
        const first = try Reader.openProjected(arena, files[0], project);
        const fields = try arena.alloc(types.Schema.Field, first.schema.fields.len);
        for (fields, first.schema.fields) |*f, src| f.* = .{ .name = src.name, .ty = src.ty.asNullable() };
        const self = try arena.create(Folder);
        self.* = .{ .arena = arena, .root = root, .files = files, .project = project, .schema = .{ .fields = fields }, .cur = first, .i = 1 };
        return self;
    }

    pub fn firstReader(self: *const Folder) ?*Reader {
        return if (self.i == 1) self.cur else null;
    }

    pub fn next(self: *Folder, arena: std.mem.Allocator) !?Batch {
        while (true) {
            if (self.cur) |r| {
                r.bounds = self.bounds;
                r.threshold = self.threshold;
                r.tally = self.tally;
                if (try r.next(arena)) |b| {
                    var out = b;
                    out.schema = &self.schema;
                    return out;
                }
                r.close();
                self.cur = null;
            }
            if (self.i >= self.files.len) return null;
            const path = self.files[self.i];
            self.i += 1;
            const r = try Reader.openProjected(self.arena, path, self.project);
            if (self.mismatch(r.schema)) |why| {
                r.close();
                return eval.explain(error.ParquetFolderMismatch, mismatchMessage(self.arena, self.root, path, self.files[0], why));
            }
            self.cur = r;
        }
    }

    fn mismatch(self: *Folder, got: types.Schema) ?[]const u8 {
        return schemaMismatch(self.arena, self.schema, got);
    }

    pub fn close(self: *Folder) void {
        if (self.cur) |r| r.close();
        self.cur = null;
    }

    pub fn source(self: *Folder) driver.Source {
        return .{ .ptr = self, .vtable = &folder_vtable };
    }

    const folder_vtable = driver.Source.VTable{
        .schema = struct {
            fn f(p: *anyopaque) types.Schema {
                return @as(*Folder, @ptrCast(@alignCast(p))).schema;
            }
        }.f,
        .next = struct {
            fn f(p: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
                return @as(*Folder, @ptrCast(@alignCast(p))).next(arena);
            }
        }.f,
        .close = struct {
            fn f(p: *anyopaque) void {
                @as(*Folder, @ptrCast(@alignCast(p))).close();
            }
        }.f,
    };
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

/// Ends at the next chunk's start, or the footer. `total_compressed_size` is not
/// used: writers disagree on whether it counts headers, which truncated chunks.
fn chunkEnd(boundaries: []const u64, start: u64) u64 {
    for (boundaries) |b| {
        if (b > start) return b;
    }
    return start;
}

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

pub fn isRemote(path: []const u8) bool {
    return objstore.isUrl(path) or
        std.mem.startsWith(u8, path, "http://") or
        std.mem.startsWith(u8, path, "https://");
}

const testing = std.testing;

test "bitWidth covers the level widths Parquet asks for" {
    try testing.expectEqual(@as(u6, 0), bitWidth(0));
    try testing.expectEqual(@as(u6, 1), bitWidth(1));
    try testing.expectEqual(@as(u6, 2), bitWidth(2));
    try testing.expectEqual(@as(u6, 2), bitWidth(3));
    try testing.expectEqual(@as(u6, 3), bitWidth(4));
    try testing.expectEqual(@as(u6, 8), bitWidth(255));
}

test "RLE run repeats one value" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const got = try decodeRleHybrid(ar.allocator(), &[_]u8{ 0x08, 0x01 }, 1, 4);
    try testing.expectEqualSlices(u32, &.{ 1, 1, 1, 1 }, got);
}

test "bit-packed run unpacks LSB-first in groups of eight" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
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
    try testing.expectError(Error.CorruptParquetPage, decodeRleHybrid(ar.allocator(), &[_]u8{0x03}, 1, 8));
    try testing.expectError(Error.CorruptParquetPage, decodeRleHybrid(ar.allocator(), &[_]u8{ 0x00, 0x01 }, 1, 4));
}

test "int96 converts Julian day plus nanoseconds to epoch micros" {
    try testing.expectEqual(@as(i64, 0), int96ToMicros(2_440_588, 0));
    try testing.expectEqual(@as(i64, 1_000_000), int96ToMicros(2_440_588, 1_000_000_000));
    try testing.expectEqual(@as(i64, 86_400_000_000), int96ToMicros(2_440_589, 0));
    try testing.expectEqual(@as(i64, -86_400_000_000), int96ToMicros(2_440_587, 0));
    try testing.expectEqual(@as(i64, std.math.maxInt(i64)), int96ToMicros(std.math.maxInt(u32), 0));
    try testing.expectEqual(@as(i64, -210_866_803_200_000_000), int96ToMicros(0, 0));
}

test "a bit-packed run length from the wire cannot overflow the byte count" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
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

    try testing.expect((try basaltType(.{ .ty = .int32, .repetition = .required })).nullable == false);
    try testing.expect((try basaltType(.{ .ty = .int32, .repetition = opt })).nullable);
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

    const id = try readColumnChunk(a, fx, g.columns[0].meta.?, md.schema[1], rows, 1, 0);
    try testing.expectEqual(@as(usize, 60), id.len);
    try testing.expectEqual(@as(i64, 0), id.getValue(0).int);
    try testing.expectEqual(@as(i64, 59), id.getValue(59).int);

    const name = try readColumnChunk(a, fx, g.columns[1].meta.?, md.schema[2], rows, 1, 0);
    try testing.expectEqualStrings("row-0", name.getValue(0).string);
    try testing.expectEqualStrings("row-59", name.getValue(59).string);

    const amt = try readColumnChunk(a, fx, g.columns[2].meta.?, md.schema[3], rows, 1, 0);
    try testing.expectEqual(@as(f64, 0.0), amt.getValue(0).float);
    try testing.expectEqual(@as(f64, 88.5), amt.getValue(59).float);

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

    try testing.expectEqualStrings("addr.city", leaves[1].name);
    try testing.expectEqual(@as(u32, 2), leaves[1].max_def);
    try testing.expectEqualStrings("addr.zip", leaves[2].name);
    try testing.expectEqual(@as(u32, 1), leaves[2].max_def);

    try testing.expectEqualStrings("tags", leaves[3].name);
    try testing.expect(leaves[3].isRepeated());
    try testing.expectEqualSlices(u32, &.{1}, leaves[3].list.?.rep_def);

    try testing.expectEqual(@as(usize, 3), leaves[3].chunk_idx);
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

fn logical(phys: parquet.PhysicalType, lt: parquet.LogicalType) parquet.SchemaElement {
    return .{ .ty = phys, .repetition = .optional, .logical_type = lt };
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

const fx_v2 = @embedFile("testdata/v2delta.parquet");

test "data page v2 with DELTA encodings decodes to the same values as v1" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realpathAlloc(a, ".");
    const path = try std.fs.path.join(a, &.{ dir, "v2.parquet" });
    try tmp.dir.writeFile(.{ .sub_path = "v2.parquet", .data = fx_v2 });

    const r = try Reader.open(a, path);
    try testing.expectEqual(@as(usize, 4), r.schema.fields.len);
    const b = (try r.next(a)).?;
    try testing.expectEqual(@as(usize, 60), b.len);

    try testing.expectEqual(@as(i64, 0), b.columns[0].getValue(0).int);
    try testing.expectEqual(@as(i64, 42), b.columns[0].getValue(42).int);
    try testing.expectEqual(@as(i64, 59), b.columns[0].getValue(59).int);
    try testing.expectEqualStrings("row-0", b.columns[1].getValue(0).string);
    try testing.expectEqualStrings("row-59", b.columns[1].getValue(59).string);
    try testing.expectEqual(@as(f64, 88.5), b.columns[2].getValue(59).float);
    try testing.expectEqual(true, b.columns[3].getValue(0).bool);
}

test "delta binary packed recovers a running sum, including negatives" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const src = [_]u8{ 0x80, 0x01, 0x04, 0x01, 0x0e };
    const got = try decodeDeltaBinaryPacked(a, &src, 1);
    try testing.expectEqualSlices(i64, &.{7}, got);

    try testing.expectError(Error.CorruptParquetPage, decodeDeltaBinaryPacked(a, &[_]u8{ 0x80, 0x01, 0x00, 0x01, 0x00 }, 1));
}

test "byte stream split regroups transposed value bytes" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
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

    const all = try Reader.open(a, path);
    try testing.expectEqual(@as(usize, 4), all.schema.fields.len);

    const two = try Reader.openProjected(a, path, &.{ "name", "flag" });
    try testing.expectEqual(@as(usize, 2), two.schema.fields.len);
    try testing.expectEqualStrings("name", two.schema.fields[0].name);
    try testing.expectEqualStrings("flag", two.schema.fields[1].name);
    const b = (try two.next(a)).?;
    try testing.expectEqual(@as(usize, 60), b.len);
    try testing.expectEqualStrings("row-0", b.columns[0].getValue(0).string);
    try testing.expectEqual(true, b.columns[1].getValue(0).bool);

    const one = try Reader.openProjected(a, path, &.{ "name", "nosuch" });
    try testing.expectEqual(@as(usize, 1), one.schema.fields.len);

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

    const eq_in = [_]Bound{.{ .column = "id", .op = .eq, .value = .{ .int = 150 } }};
    const eq_out = [_]Bound{.{ .column = "id", .op = .eq, .value = .{ .int = 999 } }};
    try testing.expect(groupMayMatch(&schema, &leaves, g, &eq_in));
    try testing.expect(!groupMayMatch(&schema, &leaves, g, &eq_out));

    const gt_keep = [_]Bound{.{ .column = "id", .op = .gt, .value = .{ .int = 150 } }};
    const gt_drop = [_]Bound{.{ .column = "id", .op = .gt, .value = .{ .int = 200 } }};
    try testing.expect(groupMayMatch(&schema, &leaves, g, &gt_keep));
    try testing.expect(!groupMayMatch(&schema, &leaves, g, &gt_drop));

    var bare = [_]parquet.ColumnChunk{.{ .meta = .{ .ty = .int64 } }};
    const g2 = parquet.RowGroup{ .columns = &bare, .num_rows = 10 };
    try testing.expect(groupMayMatch(&schema, &leaves, g2, &drop));

    const other = [_]Bound{.{ .column = "nosuch", .op = .lt, .value = .{ .int = 0 } }};
    try testing.expect(groupMayMatch(&schema, &leaves, g, &other));
}

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

    try testing.expectEqual(@as(usize, 2), md.row_groups.len);
    const jan1: i64 = 1_767_225_600_000_000;
    const ge_jan1 = [_]Bound{.{ .column = "ts", .op = .ge, .value = .{ .timestamp = jan1 } }};
    try testing.expect(groupMayMatch(md.schema, leaves, md.row_groups[0], &ge_jan1));
    try testing.expect(!groupMayMatch(md.schema, leaves, md.row_groups[1], &ge_jan1));
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
    try testing.expectEqual(@as(u64, 900), chunkEnd(&b, 250));
    try testing.expectEqual(@as(u64, 900), chunkEnd(&b, 900));
}

test "a corrupted file errors instead of panicking" {
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

    try testing.expect(!groupBeatsThreshold(&schema, &leaves, g, .{ .column = "v", .desc = true, .full = true, .value = .{ .int = 500 } }));
    try testing.expect(groupBeatsThreshold(&schema, &leaves, g, .{ .column = "v", .desc = true, .full = true, .value = .{ .int = 150 } }));
    try testing.expect(!groupBeatsThreshold(&schema, &leaves, g, .{ .column = "v", .desc = true, .full = true, .value = .{ .int = 200 } }));

    try testing.expect(!groupBeatsThreshold(&schema, &leaves, g, .{ .column = "v", .desc = false, .full = true, .value = .{ .int = 50 } }));
    try testing.expect(groupBeatsThreshold(&schema, &leaves, g, .{ .column = "v", .desc = false, .full = true, .value = .{ .int = 150 } }));

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

test "an http parquet source routes to the network, never the local filesystem" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();

    _ = Reader.open(ar.allocator(), "http://127.0.0.1:1/nope.parquet") catch |e| {
        try testing.expect(e != error.FileNotFound and e != error.NotDir and e != error.AccessDenied);
        return;
    };
    return error.TestExpectedConnectionError;
}

fn fuzzKernels(_: void, input: []const u8) anyerror!void {
    if (input.len < 3) return;
    const width6: u6 = @truncate(input[0]);
    const count: usize = ((@as(usize, input[1]) << 4) | (input[2] & 0x0F)) & 0x1FF;
    const src = input[3..];
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

    inline for (.{ .boolean, .int32, .int64, .int96, .float, .double, .byte_array, .fixed_len_byte_array }) |pt| {
        var cur = PlainCursor.init(pt, @intCast(width6), src);
        var n: usize = 0;
        while (n < count) : (n += 1) {
            _ = cur.next() catch break;
        }
    }
}

const fuzzKernels_corpus = [_][]const u8{
    "\x03\x01\x00" ++ "\x03\x88\x01\x02\x03",
    "\x02\x00\x08" ++ "\x80\x01\x04\x05\x00\x01\x02\x03\x04",
};

test "fuzz: page decode kernels survive arbitrary bytes" {
    try std.testing.fuzz({}, fuzzKernels, .{ .corpus = &fuzzKernels_corpus });
    try @import("../net/fuzzutil.zig").pound(fuzzKernels, &fuzzKernels_corpus);
}

test "BitReader: wide values at non-zero bit offsets keep their top bits" {
    const v61: u64 = 0x1ABC_DEF0_1234_5678 & ((1 << 61) - 1);
    const v64: u64 = 0xFEDC_BA98_7654_3210;
    var bits: [17]u8 = @splat(0);
    var w = std.io.Writer.fixed(&bits);
    _ = &w;
    var acc: u128 = 0b111;
    acc |= @as(u128, v61) << 3;
    var acc2: u128 = @as(u128, v64) << ((3 + 61) % 8);
    _ = &acc2;
    var all: [16]u8 = undefined;
    std.mem.writeInt(u128, &all, acc | (@as(u128, v64) << 64), .little);
    var br = BitReader{ .buf = &all };
    try std.testing.expectEqual(@as(u64, 0b111), try br.read(3));
    try std.testing.expectEqual(v61, try br.read(61));
    try std.testing.expectEqual(v64, try br.read(64));

    var short = BitReader{ .buf = all[0..8] };
    _ = try short.read(3);
    try std.testing.expectError(Error.CorruptParquetPage, short.read(64));
}

test "decodeDeltaBinaryPacked: a miniblock width above 64 is a corrupt page" {
    var mem: [4096]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&mem);
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

    const flat = ListShape{ .rep_def = &.{2} };
    const vals = [_]Value{ .{ .int = 1 }, .{ .int = 2 }, .null, .null, .null, .{ .int = 5 } };
    const reps = [_]u32{ 0, 1, 0, 0, 0, 1 };
    const defs = [_]u32{ 3, 3, 1, 0, 2, 3 };
    const got = try assembleLists(a, try listColumn(a, &vals), &reps, &defs, 3, flat, 4);
    try testing.expectEqualStrings("[1,2]", got.getValue(0).string);
    try testing.expectEqualStrings("[]", got.getValue(1).string);
    try testing.expect(got.getValue(2) == .null);
    try testing.expectEqualStrings("[null,5]", got.getValue(3).string);

    const nested = ListShape{ .rep_def = &.{ 2, 4 } };
    const nvals = [_]Value{ .{ .int = 1 }, .{ .int = 2 }, .{ .int = 3 }, .null, .null, .{ .int = 4 } };
    const nreps = [_]u32{ 0, 1, 2, 0, 1, 1 };
    const ndefs = [_]u32{ 5, 5, 5, 3, 2, 5 };
    const ngot = try assembleLists(a, try listColumn(a, &nvals), &nreps, &ndefs, 5, nested, 2);
    try testing.expectEqualStrings("[[1],[2,3]]", ngot.getValue(0).string);
    try testing.expectEqualStrings("[[],null,[4]]", ngot.getValue(1).string);

    try testing.expectError(Error.CorruptParquetPage, assembleLists(a, try listColumn(a, &vals), &reps, &defs, 3, flat, 5));
    const bad_reps = [_]u32{ 0, 2, 0, 0, 0, 1 };
    try testing.expectError(Error.CorruptParquetPage, assembleLists(a, try listColumn(a, &vals), &bad_reps, &defs, 3, flat, 4));
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
    const want =
        \\id=1 xs=[1,2] nest=[[1],[2,3]] ds=["2026-01-01"] recs=[{"a":1,"b":"x"}] m={"k":1}
        \\id=2 xs=[] nest=[[]] ds=null recs=null m=null
        \\id=3 xs=null nest=null ds=[] recs=[] m={}
        \\id=4 xs=[null,5] nest=[null,[4]] ds=["1969-12-31",null] recs=[{"a":2,"b":"y"}] m={"z":2}
        \\
    ;
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
    const names = [_][]const u8{ "id", "xs", "nest", "ds", "recs", "m" };
    try testing.expectEqual(names.len, r.schema.fields.len);
    for (names, r.schema.fields) |n, f| try testing.expectEqualStrings(n, f.name);
    try testing.expectEqual(types.TypeKind.string, r.schema.fields[4].ty.kind);
    const rb = [_]Bound{.{ .column = "recs", .op = .eq, .value = .{ .string = "zzz" } }};
    for (r.md.row_groups) |g| try testing.expect(groupMayMatch(r.md.schema, r.leaves, g, &rb));
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
    const schema = [_]parquet.SchemaElement{
        .{ .name = "root", .num_children = 1 },
        .{ .name = "recs", .repetition = .optional, .num_children = 1, .converted_type = 3 },
        .{ .name = "list", .repetition = .repeated, .num_children = 1 },
        .{ .name = "element", .repetition = .optional, .num_children = 2 },
        .{ .name = "a", .ty = .int64, .repetition = .optional },
        .{ .name = "b", .ty = .int64, .repetition = .optional },
    };
    const root = try buildNested(a, &schema, .{ .idx = 1, .base_def = 0, .base_rep = 0 });
    const ea = try entriesOf(a, &.{ .{ .int = 1 }, .null, .null, .null, .null }, &.{ 0, 1, 0, 0, 0 }, &.{ 4, 3, 0, 1, 2 });
    const eb = try entriesOf(a, &.{ .{ .int = 2 }, .{ .int = 3 }, .null, .null, .null }, &.{ 0, 1, 0, 0, 0 }, &.{ 4, 4, 0, 1, 2 });
    const got = try assembleNested(a, &root, &.{ ea, eb }, 4);
    try testing.expectEqualStrings("[{\"a\":1,\"b\":2},{\"a\":null,\"b\":3}]", got.getValue(0).string);
    try testing.expect(got.getValue(1) == .null);
    try testing.expectEqualStrings("[]", got.getValue(2).string);
    try testing.expectEqualStrings("[null]", got.getValue(3).string);

    const eb3 = try entriesOf(a, &.{ .{ .int = 2 }, .{ .int = 3 }, .{ .int = 9 }, .null, .null, .null }, &.{ 0, 1, 1, 0, 0, 0 }, &.{ 4, 4, 4, 0, 1, 2 });
    try testing.expectError(Error.CorruptParquetPage, assembleNested(a, &root, &.{ ea, eb3 }, 4));
    try testing.expectError(Error.CorruptParquetPage, assembleNested(a, &root, &.{ ea, eb }, 5));
    try testing.expectError(Error.CorruptParquetPage, assembleNested(a, &root, &.{ea}, 4));
}

test "a nested fixture with bytes flipped anywhere errors or reads, never crashes" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    inline for (.{ "testdata/lists_v1.parquet", "testdata/lists_v2.parquet" }) |fixture| {
        const good = @embedFile(fixture);
        var buf: [good.len]u8 = undefined;
        var i: usize = 0;
        while (i < good.len) : (i += 1) {
            @memcpy(&buf, good);
            buf[i] ^= 0x5A;
            _ = readAllText(a, &buf, "f.parquet") catch continue;
        }
    }
}
