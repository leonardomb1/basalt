//! Parquet value decoding: levels, encodings, and page-to-column assembly.
//!
//! A decompressed page is still encoded. This module turns those bytes into
//! `Value`s and builds a basalt `Column` from them.
//!
//! Scope is flat schemas — every column a leaf, `max_rep_level == 0`. Nested
//! lists and structs are rejected by name rather than silently flattened, since
//! basalt's type system cannot represent them anyway.
//!
//! Encodings handled: PLAIN, and the RLE/bit-packed hybrid used for both
//! definition levels and dictionary indices (`PLAIN_DICTIONARY` and
//! `RLE_DICTIONARY` share a wire format). The DELTA_* family is reported as
//! unsupported rather than guessed at.

const std = @import("std");
const pq = @import("parquet.zig");
const types = @import("../lang/types.zig");
const column = @import("../exec/column.zig");
const valuemod = @import("../exec/value.zig");
const eval = @import("../exec/eval.zig");

const Value = valuemod.Value;

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

    pub fn read(self: *BitReader, width: u6) Error!u64 {
        if (width == 0) return 0;
        const end = self.bit_pos + width;
        if ((end + 7) >> 3 > self.buf.len) return Error.CorruptParquetPage;

        const byte = self.bit_pos >> 3;
        const shift: u6 = @intCast(self.bit_pos & 7);
        self.bit_pos = end;

        // Fast path: the value plus its bit offset fit in one unaligned u64.
        if (byte + 8 <= self.buf.len) {
            const word = std.mem.readInt(u64, self.buf[byte..][0..8], .little);
            const v = word >> shift;
            return if (width == 64) v else v & ((@as(u64, 1) << width) - 1);
        }

        // Tail: fewer than 8 bytes remain, so assemble what is there.
        var word: u64 = 0;
        var k: usize = 0;
        while (byte + k < self.buf.len and k < 8) : (k += 1) {
            word |= @as(u64, self.buf[byte + k]) << @intCast(8 * k);
        }
        const v = word >> shift;
        return if (width == 64) v else v & ((@as(u64, 1) << width) - 1);
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
            var br = BitReader{ .buf = src[pos..][0..bytes] };
            for (0..vals) |_| {
                if (n >= count) break; // trailing padding in the last group
                out[n] = @intCast(try br.read(width));
                n += 1;
            }
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
    ty: pq.PhysicalType,
    type_length: usize = 0,
    /// BOOLEAN is bit-packed, one bit per value, so it needs its own cursor.
    bits: BitReader = .{ .buf = &.{} },

    pub fn init(ty: pq.PhysicalType, type_length: i32, src: []const u8) PlainCursor {
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
            const width: u6 = @intCast(w);
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
            const width: u6 = @intCast(w);
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

/// Multiplier taking a column's stored temporal unit to basalt's microseconds.
///
/// Parquet stores TIME/TIMESTAMP in milliseconds, microseconds or nanoseconds
/// depending on the annotation; basalt's `time`/`timestamp` are always micros.
/// Ignoring this reads a millisecond timestamp as if it were micros — a silent
/// 1000x error that lands values in 1970.
pub fn temporalScale(e: pq.SchemaElement) i64 {
    const c = e.converted_type orelse return 1;
    return switch (c) {
        conv_time_millis, conv_timestamp_millis => 1000,
        else => 1,
    };
}

/// basalt type for a leaf schema element, from its physical and converted type.
pub fn basaltType(e: pq.SchemaElement) Error!types.Type {
    const phys = e.ty orelse return Error.UnsupportedParquetSchema;
    const conv = e.converted_type;

    var t: types.Type = switch (phys) {
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
        else => return Error.UnsupportedParquetSchema,
    };
    if (e.repetition orelse .required != .required) t = t.asNullable();
    return t;
}

fn decimalOf(e: pq.SchemaElement) types.Type {
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
pub fn coerce(t: types.Type, v: Value, scale: i64) Value {
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
            .int => |x| .{ .time = x * scale },
            else => v,
        },
        .timestamp => switch (v) {
            // int96 already decodes to micros and carries scale 1
            .int => |x| .{ .timestamp = x * scale },
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

    /// A leaf under a repeated group is a list element: many values per row,
    /// which basalt has no column type for.
    pub fn isRepeated(self: Leaf) bool {
        return self.max_rep > 0;
    }
};

/// Walks the depth-first schema list, resolving each leaf's definition and
/// repetition levels from its ancestors.
///
/// This is what makes a file with nested columns usable: the flat columns are
/// still readable, and only the repeated ones are skipped. Rejecting the whole
/// file because one column is a list would make most real lake files unopenable.
pub fn collectLeaves(arena: std.mem.Allocator, schema: []const pq.SchemaElement) Error![]Leaf {
    var out = std.array_list.Managed(Leaf).init(arena);
    var pos: usize = 1; // element 0 is the synthetic root
    var chunk: usize = 0;
    const root_children: usize = @intCast(@max(0, schema[0].num_children));
    for (0..root_children) |_| {
        try walkNode(arena, schema, &pos, &chunk, &out, "", 0, 0);
    }
    return out.toOwnedSlice();
}

fn walkNode(
    arena: std.mem.Allocator,
    schema: []const pq.SchemaElement,
    pos: *usize,
    chunk: *usize,
    out: *std.array_list.Managed(Leaf),
    prefix: []const u8,
    def: u32,
    rep: u32,
) Error!void {
    if (pos.* >= schema.len) return Error.UnsupportedParquetSchema;
    const e = schema[pos.*];
    const idx = pos.*;
    pos.* += 1;

    const r = e.repetition orelse .required;
    const d2 = def + @as(u32, if (r == .required) 0 else 1);
    const r2 = rep + @as(u32, if (r == .repeated) 1 else 0);
    const name = if (prefix.len == 0)
        try arena.dupe(u8, e.name)
    else
        try std.fmt.allocPrint(arena, "{s}.{s}", .{ prefix, e.name });

    if (e.isLeaf()) {
        try out.append(.{
            .schema_idx = idx,
            .chunk_idx = chunk.*,
            .name = name,
            .max_def = d2,
            .max_rep = r2,
        });
        chunk.* += 1;
        return;
    }
    const n: usize = @intCast(@max(0, e.num_children));
    for (0..n) |_| try walkNode(arena, schema, pos, chunk, out, name, d2, r2);
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
    meta: pq.ColumnMetaData,
    elem: pq.SchemaElement,
    rows: usize,
    max_def: u32,
    /// Offset of `file_bytes[0]` within the file. Zero when the caller passed a
    /// slice that already starts at the chunk, as the ranged reader does.
    base_offset: u64,
) (Error || pq.Error || @import("codec.zig").Error)!column.Column {
    const ty = (try basaltType(elem)).asNullable();
    const tscale = temporalScale(elem);

    var b = try column.Builder.initCapacity(arena, ty, rows);
    var dict: ?[]Value = null;
    var offset: usize = @intCast(@as(u64, @intCast(meta.startOffset())) - base_offset);
    var produced: usize = 0;

    while (produced < rows) {
        if (offset >= file_bytes.len) return Error.CorruptParquetPage;
        const pg = try pq.readPage(arena, file_bytes, offset, meta.compression);
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
                produced += try appendDataPage(arena, &b, pg, meta, elem, ty, max_def, dict, tscale);
            },
            .index_page => {}, // not data; skip
            else => return Error.CorruptParquetPage,
        }
    }
    return b.finish();
}

/// Splits a data page into levels and values, then emits rows.
///
/// v1 length-prefixes each RLE level section with four bytes; v2 moves those
/// lengths into the page header and leaves the sections unprefixed. Everything
/// after the levels is the same in both.
fn appendDataPage(
    arena: std.mem.Allocator,
    b: *column.Builder,
    pg: pq.Page,
    meta: pq.ColumnMetaData,
    elem: pq.SchemaElement,
    ty: types.Type,
    max_def: u32,
    dict: ?[]Value,
    tscale: i64,
) Error!usize {
    // A negative count is not a count; @intCast on it is undefined in release.
    const n = std.math.cast(usize, pg.header.num_values) orelse return Error.CorruptParquetPage;
    var body = pg.data;

    var defs: ?[]u32 = null;
    if (pg.header.ty == .data_page_v2) {
        // repetition levels come first and are ignored: repeated columns are
        // filtered out before a chunk is ever read
        if (pg.header.rep_levels_len > body.len) return Error.CorruptParquetPage;
        body = body[pg.header.rep_levels_len..];
        const dl = pg.header.def_levels_len;
        if (dl > body.len) return Error.CorruptParquetPage;
        if (max_def > 0 and dl > 0) {
            defs = try decodeRleHybrid(arena, body[0..dl], bitWidth(max_def), n);
        }
        body = body[dl..];
    } else if (max_def > 0) {
        if (body.len < 4) return Error.CorruptParquetPage;
        const len: usize = std.mem.readInt(u32, body[0..4], .little);
        if (4 + len > body.len) return Error.CorruptParquetPage;
        defs = try decodeRleHybrid(arena, body[4..][0..len], bitWidth(max_def), n);
        body = body[4 + len ..];
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
            if (tscale == 1 and try bulkPlain(arena, b, ty, meta.ty, body, present, defs, max_def)) {
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
    phys: pq.PhysicalType,
    body: []const u8,
    present: usize,
    defs: ?[]const u32,
    max_def: u32,
) Error!bool {
    // values are only stored for present rows; nulls occupy a level, not a slot
    const count = present;
    switch (phys) {
        .int64 => {
            if (ty.kind != .int) return false;
            if (body.len < count * 8) return Error.CorruptParquetPage;
            const out = try arena.alloc(i64, count);
            for (out, 0..) |*o, i| o.* = std.mem.readInt(i64, body[i * 8 ..][0..8], .little);
            if (defs) |d| {
                b.appendBulkScattered(i64, out, d, max_def) catch return false;
            } else b.appendBulk(i64, out) catch return false;
            return true;
        },
        .int32 => {
            if (ty.kind != .int) return false;
            if (body.len < count * 4) return Error.CorruptParquetPage;
            const out = try arena.alloc(i64, count);
            for (out, 0..) |*o, i| o.* = std.mem.readInt(i32, body[i * 4 ..][0..4], .little);
            if (defs) |d| {
                b.appendBulkScattered(i64, out, d, max_def) catch return false;
            } else b.appendBulk(i64, out) catch return false;
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

/// Emits integer-shaped decoded values, interleaving nulls by definition level.
fn emitInts(
    b: *column.Builder,
    ty: types.Type,
    defs: ?[]const u32,
    max_def: u32,
    n: usize,
    vals: []const i64,
    tscale: i64,
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
    tscale: i64,
) Error!void {
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
    tscale: i64,
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
pub const Threshold = valuemod.Threshold;

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
    schema: []const pq.SchemaElement,
    leaves: []const Leaf,
    g: pq.RowGroup,
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
    schema: []const pq.SchemaElement,
    leaves: []const Leaf,
    g: pq.RowGroup,
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

fn findLeaf(leaves: []const Leaf, name: []const u8) ?Leaf {
    for (leaves) |lf| {
        if (std.mem.eql(u8, lf.name, name)) return lf;
    }
    return null;
}

/// Decodes one PLAIN-encoded statistics blob into a comparable `Value`.
fn statValue(elem: pq.SchemaElement, phys: pq.PhysicalType, raw: ?[]const u8) ?Value {
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
            else => a.int,
        }),
        .float => std.math.order(a.float, switch (b) {
            .float => |x| x,
            .int => |x| @as(f64, @floatFromInt(x)),
            else => a.float,
        }),
        .date => std.math.order(a.date, switch (b) {
            .date => |x| x,
            .int => |x| @as(i32, @intCast(x)),
            else => a.date,
        }),
        .time, .timestamp => blk: {
            const av = if (a == .time) a.time else a.timestamp;
            const bv = switch (b) {
                .time => |x| x,
                .timestamp => |x| x,
                .int => |x| x,
                else => av,
            };
            break :blk std.math.order(av, bv);
        },
        .string, .bytes => blk: {
            const sa = if (a == .string) a.string else a.bytes;
            const sb = switch (b) {
                .string => |x| x,
                .bytes => |x| x,
                else => sa,
            };
            break :blk std.mem.order(u8, sa, sb);
        },
        else => null,
    };
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
const batchmod = @import("../exec/batch.zig");
const azure = @import("azure.zig");
const httpx = @import("http.zig");
const Batch = batchmod.Batch;

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
    blob: ?azure.Blob,
    total: u64,
    /// Set when the origin ignored `Range` (or could not report a size), which
    /// makes every later read a slice instead of another full transfer.
    whole: ?[]const u8 = null,
    /// A corporate TLS interceptor is repaired once per object, not per range.
    repaired: bool = false,

    fn open(arena: std.mem.Allocator, path: []const u8) !*Remote {
        const client = try arena.create(std.http.Client);
        client.* = httpx.initClient(arena);
        const self = try arena.create(Remote);
        self.* = .{ .arena = arena, .client = client, .url = path, .blob = null, .total = 0 };
        if (azure.isUrl(path)) {
            const b = try azure.parseUrl(arena, path, azure.endpointFromEnv(arena));
            self.blob = b;
            self.url = b.url;
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

    fn read(self: *Remote, arena: std.mem.Allocator, off: u64, len: usize) ![]const u8 {
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
            .decompress_buffer = httpx.decompress_direct,
            .response_writer = &aw.writer,
        });
        return .{ .code = @intFromEnum(res.status), .body = aw.writer.buffered() };
    }

    fn headers(
        self: *Remote,
        arena: std.mem.Allocator,
        method: std.http.Method,
        range_hdr: []const u8,
    ) ![]const std.http.Header {
        if (self.blob) |b| {
            const verb = if (method == .HEAD) "HEAD" else "GET";
            return azure.requestHeaders(arena, b, verb, range_hdr);
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

    fn statusError(self: *Remote, code: u16, body: []const u8) anyerror {
        if (self.blob != null) return azure.statusToError(code, body);
        return httpx.statusError(code);
    }

    fn repair(self: *Remote) bool {
        if (self.repaired) return false;
        self.repaired = true;
        const uri = std.Uri.parse(self.url) catch return false;
        const h = httpx.uriHost(uri) orelse return false;
        if (!httpx.repairBundle(self.client.allocator, &self.client.ca_bundle, h, uri.port orelse 443)) return false;
        self.client.next_https_rescan_certs = false;
        return true;
    }
};

/// Reads a Parquet file as a pipeline source, one batch per row group.
///
/// Only the footer and the column chunks a query needs are read; a chunk is
/// fetched, decoded and released per row group, so resident memory tracks the
/// widest row group's projected columns rather than the file.
pub const Reader = struct {
    arena: std.mem.Allocator,
    src: Bytes,
    md: pq.FileMetaData,
    schema: types.Schema,
    /// Readable leaves, in output order. Repeated (list) leaves are excluded but
    /// still occupy a chunk slot, which is why `Leaf.chunk_idx` is carried.
    leaves: []const Leaf,
    /// Names of columns skipped because they are list elements, for reporting.
    skipped: []const []const u8,
    /// Ascending chunk start offsets plus the footer start, used to bound each
    /// ranged read.
    boundaries: []const u64 = &.{},
    /// Bounds used to skip row groups outright; empty means read them all.
    bounds: []const Bound = &.{},
    /// Live top-N bound, when the pipeline is a `sort … limit` over this file.
    threshold: ?*const Threshold = null,
    /// Row groups skipped on statistics, for reporting.
    groups_skipped: usize = 0,
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

        // Struct fields read fine as flat dotted columns; only list elements
        // (repeated) have no representation, so those alone are skipped.
        const all = try collectLeaves(arena, md.schema);
        var keep = std.array_list.Managed(Leaf).init(arena);
        var skipped = std.array_list.Managed([]const u8).init(arena);
        var fields = std.array_list.Managed(types.Schema.Field).init(arena);
        for (all) |lf| {
            if (lf.isRepeated()) {
                try skipped.append(lf.name);
                continue;
            }
            if (want) |names| {
                var hit = false;
                for (names) |n| {
                    if (std.mem.eql(u8, n, lf.name)) {
                        hit = true;
                        break;
                    }
                }
                if (!hit) continue;
            }
            try keep.append(lf);
            try fields.append(.{
                .name = lf.name,
                .ty = (try basaltType(md.schema[lf.schema_idx])).asNullable(),
            });
        }
        // An empty projection (COUNT(*)) still needs batches with a row count,
        // so the narrowest column is kept rather than none.
        if (fields.items.len == 0 and want != null) {
            for (all) |lf| {
                if (lf.isRepeated()) continue;
                try keep.append(lf);
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
            .skipped = try skipped.toOwnedSlice(),
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
            const rows: usize = @intCast(g.num_rows);
            if (rows == 0) continue;
            // statistics can rule a whole group out before any page is touched
            if (self.bounds.len > 0 and
                !groupMayMatch(self.md.schema, self.leaves, g, self.bounds))
            {
                self.groups_skipped += 1;
                continue;
            }
            if (self.threshold) |t| {
                if (!groupBeatsThreshold(self.md.schema, self.leaves, g, t.*)) {
                    self.groups_skipped += 1;
                    continue;
                }
            }
            const cols = try arena.alloc(column.Column, self.leaves.len);
            for (self.leaves, 0..) |lf, ci| {
                if (lf.chunk_idx >= g.columns.len) return Error.CorruptParquetPage;
                const meta = g.columns[lf.chunk_idx].meta orelse return Error.CorruptParquetPage;
                // fetch just this chunk, bounded by wherever the next one begins
                const start: u64 = @intCast(meta.startOffset());
                const end = chunkEnd(self.boundaries, start);
                if (end <= start) return Error.CorruptParquetPage;
                const chunk = try self.src.range(arena, start, @intCast(end - start));
                cols[ci] = try readColumnChunk(
                    arena,
                    chunk,
                    meta,
                    self.md.schema[lf.schema_idx],
                    rows,
                    lf.max_def,
                    start,
                );
            }
            return Batch{ .schema = &self.schema, .columns = cols, .len = rows };
        }
        return null;
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
fn chunkBoundaries(arena: std.mem.Allocator, md: pq.FileMetaData, footer_start: u64) ![]u64 {
    var out = std.array_list.Managed(u64).init(arena);
    for (md.row_groups) |g| {
        for (g.columns) |c| {
            const m = c.meta orelse continue;
            try out.append(@intCast(m.startOffset()));
        }
    }
    try out.append(footer_start);
    const sl = try out.toOwnedSlice();
    std.mem.sort(u64, sl, {}, comptime std.sort.asc(u64));
    return sl;
}

/// Reads the footer with two small ranged reads instead of the whole file.
fn parseFooterOf(arena: std.mem.Allocator, src: Bytes, footer_start: *u64) !pq.FileMetaData {
    const total = src.size();
    if (total < pq.trailer_len + pq.magic.len) return pq.Error.NotParquet;
    const head = try src.range(arena, 0, pq.magic.len);
    if (!std.mem.eql(u8, head, pq.magic)) return pq.Error.NotParquet;

    const trailer = try src.range(arena, total - pq.trailer_len, pq.trailer_len);
    const r = try pq.footerRange(total, trailer);
    footer_start.* = r.offset;
    const footer = try src.range(arena, r.offset, r.len);
    return pq.parseFooter(arena, footer);
}

/// Paths a `Remote` serves: object storage and plain URLs alike. Everything
/// else is a local file.
pub fn isRemote(path: []const u8) bool {
    return azure.isUrl(path) or
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
    const opt = pq.Repetition.optional;
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

    const md = try pq.parseFile(a, fx);
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
        const md = try pq.parseFile(a, f);
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
    const schema = [_]pq.SchemaElement{
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

    // the list element is repeated, so it is reported and then skipped
    try testing.expectEqualStrings("tags.element", leaves[3].name);
    try testing.expect(leaves[3].isRepeated());

    // chunk indices count every leaf, including the skipped one
    try testing.expectEqual(@as(usize, 3), leaves[3].chunk_idx);
}

test "temporal scale converts millisecond columns and leaves micros alone" {
    try testing.expectEqual(@as(i64, 1000), temporalScale(.{ .converted_type = 9 })); // TIMESTAMP_MILLIS
    try testing.expectEqual(@as(i64, 1000), temporalScale(.{ .converted_type = 7 })); // TIME_MILLIS
    try testing.expectEqual(@as(i64, 1), temporalScale(.{ .converted_type = 10 })); // TIMESTAMP_MICROS
    try testing.expectEqual(@as(i64, 1), temporalScale(.{})); // no annotation

    const ts = types.Type.init(.timestamp);
    // a millisecond value must be scaled up, not read as micros
    try testing.expectEqual(@as(i64, 1_583_298_367_123_000), coerce(ts, .{ .int = 1_583_298_367_123 }, 1000).timestamp);
    try testing.expectEqual(@as(i64, 1_583_298_367_123), coerce(ts, .{ .int = 1_583_298_367_123 }, 1).timestamp);
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
    const schema = [_]pq.SchemaElement{
        .{ .name = "root", .num_children = 1 },
        .{ .name = "id", .ty = .int64, .repetition = .optional },
    };
    const leaves = [_]Leaf{.{ .schema_idx = 1, .chunk_idx = 0, .name = "id", .max_def = 1, .max_rep = 0 }};

    // a chunk whose values run 100..200
    var lo: [8]u8 = undefined;
    var hi: [8]u8 = undefined;
    std.mem.writeInt(i64, &lo, 100, .little);
    std.mem.writeInt(i64, &hi, 200, .little);
    var chunks = [_]pq.ColumnChunk{.{ .meta = .{
        .ty = .int64,
        .stats = .{ .min = &lo, .max = &hi },
    } }};
    const g = pq.RowGroup{ .columns = &chunks, .num_rows = 10 };

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
    var bare = [_]pq.ColumnChunk{.{ .meta = .{ .ty = .int64 } }};
    const g2 = pq.RowGroup{ .columns = &bare, .num_rows = 10 };
    try testing.expect(groupMayMatch(&schema, &leaves, g2, &drop));

    // an unknown column contributes no bound
    const other = [_]Bound{.{ .column = "nosuch", .op = .lt, .value = .{ .int = 0 } }};
    try testing.expect(groupMayMatch(&schema, &leaves, g, &other));
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
    var client = httpx.initClient(a);
    defer client.deinit();
    var r = Remote{
        .arena = a,
        .client = &client,
        .url = "http://example/x.parquet",
        .blob = null,
        .total = 100000,
        .whole = "0123456789",
    };
    try testing.expectEqualStrings("234", try r.read(a, 2, 3));
    try testing.expectError(Error.CorruptParquetPage, r.read(a, 99992, 8));
    try testing.expectError(Error.CorruptParquetPage, r.read(a, 8, 5));
}

test "top-N threshold skips only groups it can prove cannot contribute" {
    const schema = [_]pq.SchemaElement{
        .{ .name = "root", .num_children = 1 },
        .{ .name = "v", .ty = .int64, .repetition = .optional },
    };
    const leaves = [_]Leaf{.{ .schema_idx = 1, .chunk_idx = 0, .name = "v", .max_def = 1, .max_rep = 0 }};
    var lo: [8]u8 = undefined;
    var hi: [8]u8 = undefined;
    std.mem.writeInt(i64, &lo, 100, .little);
    std.mem.writeInt(i64, &hi, 200, .little);
    var chunks = [_]pq.ColumnChunk{.{ .meta = .{ .ty = .int64, .stats = .{ .min = &lo, .max = &hi } } }};
    const g = pq.RowGroup{ .columns = &chunks, .num_rows = 10 };

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
    var bare = [_]pq.ColumnChunk{.{ .meta = .{ .ty = .int64 } }};
    const g2 = pq.RowGroup{ .columns = &bare, .num_rows = 10 };
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
