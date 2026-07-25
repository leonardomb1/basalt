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
pub const BitReader = struct {
    buf: []const u8,
    bit_pos: usize = 0,

    pub fn read(self: *BitReader, width: u6) Error!u64 {
        if (width == 0) return 0;
        var out: u64 = 0;
        for (0..width) |i| {
            const byte = self.bit_pos >> 3;
            if (byte >= self.buf.len) return Error.CorruptParquetPage;
            const b: u1 = @truncate(self.buf[byte] >> @intCast(self.bit_pos & 7));
            out |= @as(u64, b) << @intCast(i);
            self.bit_pos += 1;
        }
        return out;
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
            // bit-packed: header >> 1 groups of eight values
            const groups: usize = @intCast(header >> 1);
            const vals = groups * 8;
            const bytes = groups * @as(usize, width);
            if (pos + bytes > src.len) return Error.CorruptParquetPage;
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
pub fn int96ToMicros(julian_day: u32, nanos_of_day: u64) i64 {
    const days: i64 = @as(i64, julian_day) - 2_440_588;
    return days * 86_400_000_000 + @as(i64, @intCast(nanos_of_day / 1000));
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
) (Error || pq.Error || @import("codec.zig").Error)!column.Column {
    const ty = (try basaltType(elem)).asNullable();
    const tscale = temporalScale(elem);

    var b = try column.Builder.initCapacity(arena, ty, rows);
    var dict: ?[]Value = null;
    var offset: usize = @intCast(meta.startOffset());
    var produced: usize = 0;

    while (produced < rows) {
        if (offset >= file_bytes.len) return Error.CorruptParquetPage;
        const pg = try pq.readPage(arena, file_bytes, offset, meta.compression);
        offset = pg.next_offset;

        switch (pg.header.ty) {
            .dictionary_page => {
                // dictionary entries are always PLAIN, whatever the data pages use
                const n: usize = @intCast(pg.header.num_values);
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
    const n: usize = @intCast(pg.header.num_values);
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
            // the index bit width is a single byte ahead of the hybrid stream
            const width: u6 = @intCast(body[0]);
            const idx = try decodeRleHybrid(arena, body[1..], width, present);
            try emit(b, ty, defs, max_def, n, null, idx, d, tscale);
        },
        else => return Error.UnsupportedParquetEncoding,
    }
    return n;
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

        const excluded = switch (b.op) {
            // every value is >= lo, so `col < v` is impossible when lo >= v
            .lt => cmp(lo, b.value) != .lt,
            .le => cmp(lo, b.value) == .gt,
            .gt => cmp(hi, b.value) != .gt,
            .ge => cmp(hi, b.value) == .lt,
            .eq => cmp(b.value, lo) == .lt or cmp(b.value, hi) == .gt,
        };
        if (excluded) return false;
    }
    return true;
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

fn cmp(a: Value, b: Value) std.math.Order {
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
        else => .eq,
    };
}

// --- source ------------------------------------------------------------------

const driver = @import("driver.zig");
const batchmod = @import("../exec/batch.zig");
const azure = @import("azure.zig");
const httpx = @import("http.zig");
const Batch = batchmod.Batch;

/// Reads a Parquet file as a pipeline source, one batch per row group.
///
/// The whole object is held in memory: Parquet metadata lives at the end, and a
/// column chunk is only meaningful in full, so streaming a byte at a time is not
/// possible the way it is for CSV. Row-group-sized batches are the natural unit.
pub const Reader = struct {
    arena: std.mem.Allocator,
    bytes: []const u8,
    md: pq.FileMetaData,
    schema: types.Schema,
    /// Readable leaves, in output order. Repeated (list) leaves are excluded but
    /// still occupy a chunk slot, which is why `Leaf.chunk_idx` is carried.
    leaves: []const Leaf,
    /// Names of columns skipped because they are list elements, for reporting.
    skipped: []const []const u8,
    /// Bounds used to skip row groups outright; empty means read them all.
    bounds: []const Bound = &.{},
    /// Row groups skipped on statistics, for reporting.
    groups_skipped: usize = 0,
    rg: usize = 0,

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
        const bytes = if (azure.isUrl(path))
            try fetchBlob(arena, path)
        else
            try std.fs.cwd().readFileAlloc(arena, path, 1 << 31);

        const md = try pq.parseFile(arena, bytes);

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
            .bytes = bytes,
            .md = md,
            .schema = .{ .fields = try fields.toOwnedSlice() },
            .leaves = try keep.toOwnedSlice(),
            .skipped = try skipped.toOwnedSlice(),
        };
        return self;
    }

    /// One row group per call. Empty groups are skipped rather than returned as
    /// zero-row batches, which downstream operators treat as end-of-stream.
    pub fn next(self: *Reader, arena: std.mem.Allocator) !?Batch {
        while (self.rg < self.md.row_groups.len) {
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
            const cols = try arena.alloc(column.Column, self.leaves.len);
            for (self.leaves, 0..) |lf, ci| {
                if (lf.chunk_idx >= g.columns.len) return Error.CorruptParquetPage;
                const meta = g.columns[lf.chunk_idx].meta orelse return Error.CorruptParquetPage;
                cols[ci] = try readColumnChunk(
                    arena,
                    self.bytes,
                    meta,
                    self.md.schema[lf.schema_idx],
                    rows,
                    lf.max_def,
                );
            }
            return Batch{ .schema = &self.schema, .columns = cols, .len = rows };
        }
        return null;
    }

    pub fn close(self: *Reader) void {
        _ = self; // everything is arena-owned
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

/// Whole-blob GET for `az://` paths. Parquet needs the footer before anything
/// else, so a ranged fetch of just the metadata would still be followed by
/// reads of every referenced chunk — worth doing later, not needed for correctness.
fn fetchBlob(arena: std.mem.Allocator, path: []const u8) ![]const u8 {
    var client = httpx.initClient(arena);
    defer client.deinit();
    const blob = try azure.parseUrl(arena, path, azure.endpointFromEnv(arena));
    const hdrs = try azure.getHeaders(arena, blob, "");

    var aw = std.Io.Writer.Allocating.init(arena);
    const res = try client.fetch(.{
        .method = .GET,
        .location = .{ .url = blob.url },
        .extra_headers = hdrs,
        .response_writer = &aw.writer,
    });
    const code = @intFromEnum(res.status);
    if (code != 200) return azure.statusToError(code, aw.writer.buffered());
    return aw.writer.buffered();
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
    const id = try readColumnChunk(a, fx, g.columns[0].meta.?, md.schema[1], rows, 1);
    try testing.expectEqual(@as(usize, 60), id.len);
    try testing.expectEqual(@as(i64, 0), id.getValue(0).int);
    try testing.expectEqual(@as(i64, 59), id.getValue(59).int);

    // name BYTE_ARRAY/UTF8 -> string
    const name = try readColumnChunk(a, fx, g.columns[1].meta.?, md.schema[2], rows, 1);
    try testing.expectEqualStrings("row-0", name.getValue(0).string);
    try testing.expectEqualStrings("row-59", name.getValue(59).string);

    // amt DOUBLE: i * 1.5
    const amt = try readColumnChunk(a, fx, g.columns[2].meta.?, md.schema[3], rows, 1);
    try testing.expectEqual(@as(f64, 0.0), amt.getValue(0).float);
    try testing.expectEqual(@as(f64, 88.5), amt.getValue(59).float);

    // flag BOOLEAN: even ids true — bit-packed, one bit per value
    const flag = try readColumnChunk(a, fx, g.columns[3].meta.?, md.schema[4], rows, 1);
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
        const name = try readColumnChunk(a, f, g.columns[1].meta.?, md.schema[2], @intCast(g.num_rows), 1);
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
