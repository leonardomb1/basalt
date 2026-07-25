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

/// Adapts a decoded physical value to the column's logical type.
pub fn coerce(t: types.Type, v: Value) Value {
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
            .int => |x| .{ .time = x },
            else => v,
        },
        .timestamp => switch (v) {
            .int => |x| .{ .timestamp = x },
            else => v,
        },
        .decimal => decimalValue(t, v),
        else => v,
    };
}

// --- column chunk assembly ---------------------------------------------------

/// Maximum definition level of a flat leaf: 1 when the column is optional, 0
/// when required. Nested columns would add a level per enclosing group, which is
/// why they are rejected instead of decoded.
pub fn maxDefLevel(e: pq.SchemaElement) u32 {
    return if ((e.repetition orelse .required) == .required) 0 else 1;
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
) (Error || pq.Error || @import("codec.zig").Error)!column.Column {
    const ty = try basaltType(elem);
    const max_def = maxDefLevel(elem);

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
            .data_page => {
                produced += try appendDataPageV1(arena, &b, pg, meta, elem, ty, max_def, dict);
            },
            .data_page_v2 => return Error.UnsupportedParquetEncoding,
            .index_page => {}, // not data; skip
            else => return Error.CorruptParquetPage,
        }
    }
    return b.finish();
}

/// Data page v1 body: `[rep levels][def levels][values]`, each level section
/// length-prefixed when RLE-encoded. Flat columns have no repetition levels.
fn appendDataPageV1(
    arena: std.mem.Allocator,
    b: *column.Builder,
    pg: pq.Page,
    meta: pq.ColumnMetaData,
    elem: pq.SchemaElement,
    ty: types.Type,
    max_def: u32,
    dict: ?[]Value,
) Error!usize {
    const n: usize = @intCast(pg.header.num_values);
    var body = pg.data;

    var defs: ?[]u32 = null;
    if (max_def > 0) {
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
            try emit(b, ty, defs, max_def, n, &cur, null, null);
        },
        .plain_dictionary, .rle_dictionary => {
            const d = dict orelse return Error.CorruptParquetPage;
            if (body.len < 1) return Error.CorruptParquetPage;
            // the index bit width is a single byte ahead of the hybrid stream
            const width: u6 = @intCast(body[0]);
            const idx = try decodeRleHybrid(arena, body[1..], width, present);
            try emit(b, ty, defs, max_def, n, null, idx, d);
        },
        else => return Error.UnsupportedParquetEncoding,
    }
    return n;
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
        try b.append(coerce(ty, v));
    }
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
    /// Indices into `md.schema` of the leaf columns, in chunk order.
    leaves: []const usize,
    rg: usize = 0,

    pub fn isPath(path: []const u8) bool {
        return std.mem.endsWith(u8, path, ".parquet");
    }

    pub fn open(arena: std.mem.Allocator, path: []const u8) !*Reader {
        const bytes = if (azure.isUrl(path))
            try fetchBlob(arena, path)
        else
            try std.fs.cwd().readFileAlloc(arena, path, 1 << 31);

        const md = try pq.parseFile(arena, bytes);

        // Flat schemas only: a group below the root means a list/map/struct,
        // which basalt has no column type for. Name it rather than drop it.
        var leaves = std.array_list.Managed(usize).init(arena);
        var fields = std.array_list.Managed(types.Schema.Field).init(arena);
        for (md.schema[1..], 1..) |e, i| {
            if (!e.isLeaf()) return Error.UnsupportedParquetSchema;
            try leaves.append(i);
            try fields.append(.{ .name = try arena.dupe(u8, e.name), .ty = try basaltType(e) });
        }

        const self = try arena.create(Reader);
        self.* = .{
            .arena = arena,
            .bytes = bytes,
            .md = md,
            .schema = .{ .fields = try fields.toOwnedSlice() },
            .leaves = try leaves.toOwnedSlice(),
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
            if (g.columns.len < self.leaves.len) return Error.CorruptParquetPage;

            const cols = try arena.alloc(column.Column, self.leaves.len);
            for (self.leaves, 0..) |si, ci| {
                const meta = g.columns[ci].meta orelse return Error.CorruptParquetPage;
                cols[ci] = try readColumnChunk(arena, self.bytes, meta, self.md.schema[si], rows);
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
    const id = try readColumnChunk(a, fx, g.columns[0].meta.?, md.schema[1], rows);
    try testing.expectEqual(@as(usize, 60), id.len);
    try testing.expectEqual(@as(i64, 0), id.getValue(0).int);
    try testing.expectEqual(@as(i64, 59), id.getValue(59).int);

    // name BYTE_ARRAY/UTF8 -> string
    const name = try readColumnChunk(a, fx, g.columns[1].meta.?, md.schema[2], rows);
    try testing.expectEqualStrings("row-0", name.getValue(0).string);
    try testing.expectEqualStrings("row-59", name.getValue(59).string);

    // amt DOUBLE: i * 1.5
    const amt = try readColumnChunk(a, fx, g.columns[2].meta.?, md.schema[3], rows);
    try testing.expectEqual(@as(f64, 0.0), amt.getValue(0).float);
    try testing.expectEqual(@as(f64, 88.5), amt.getValue(59).float);

    // flag BOOLEAN: even ids true — bit-packed, one bit per value
    const flag = try readColumnChunk(a, fx, g.columns[3].meta.?, md.schema[4], rows);
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
        const name = try readColumnChunk(a, f, g.columns[1].meta.?, md.schema[2], @intCast(g.num_rows));
        try testing.expectEqualStrings("row-0", name.getValue(0).string);
        try testing.expectEqualStrings("row-42", name.getValue(42).string);
    }
}
