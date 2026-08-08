//! Parquet file metadata: the footer, and the structs needed to locate pages.
//!
//! Layout is `PAR1 <data> <thrift FileMetaData> <u32 LE footer length> PAR1`.
//! Metadata lives at the *end* precisely so a reader can fetch it with two small
//! ranged reads instead of downloading the object — the last 8 bytes give the
//! footer length, then the footer itself.
//!
//! Only the fields basalt needs are decoded; everything else is skipped, which
//! is what lets this open files written by newer Parquet versions.

const std = @import("std");
const thrift = @import("thrift.zig");
const codec = @import("codec.zig");

pub const Error = error{
    /// Missing or misplaced `PAR1` magic, or a footer length that does not fit.
    NotParquet,
    /// A wire value that no enum in the format defines.
    CorruptParquet,
} || thrift.Error || std.mem.Allocator.Error;

/// Convert a wire i32 into `E`, or fail. `@enumFromInt` on a value the enum does
/// not define is illegal behaviour — a corrupt or hostile file must produce an
/// error, not a trap.
fn enumFrom(comptime E: type, v: i32) Error!E {
    inline for (@typeInfo(E).@"enum".fields) |f| {
        if (f.value == v) return @field(E, f.name);
    }
    return Error.CorruptParquet;
}

pub const magic = "PAR1";
/// Trailing `u32` footer length plus the magic.
pub const trailer_len = 8;

/// Physical storage type of a column (`parquet.thrift` `Type`).
pub const PhysicalType = enum(i32) {
    boolean = 0,
    int32 = 1,
    int64 = 2,
    /// Deprecated 12-byte timestamp, still emitted by older Spark writers.
    int96 = 3,
    float = 4,
    double = 5,
    byte_array = 6,
    fixed_len_byte_array = 7,
    _,
};

pub const Repetition = enum(i32) { required = 0, optional = 1, repeated = 2, _ };

pub const Encoding = enum(i32) {
    plain = 0,
    plain_dictionary = 2,
    rle = 3,
    bit_packed = 4,
    delta_binary_packed = 5,
    delta_length_byte_array = 6,
    delta_byte_array = 7,
    rle_dictionary = 8,
    byte_stream_split = 9,
    _,
};

/// One node of the schema tree, flattened depth-first as Parquet stores it.
/// `num_children > 0` marks a group (the root, or a nested list/struct).
pub const SchemaElement = struct {
    ty: ?PhysicalType = null,
    type_length: ?i32 = null,
    repetition: ?Repetition = null,
    name: []const u8 = "",
    num_children: i32 = 0,
    converted_type: ?i32 = null,
    scale: ?i32 = null,
    precision: ?i32 = null,

    pub fn isLeaf(self: SchemaElement) bool {
        return self.num_children == 0;
    }
};

/// Per-chunk statistics. `min`/`max` are PLAIN-encoded bytes of the column's
/// physical type, which is enough to decide whether a row group can contain a
/// value without decoding any of it.
pub const Statistics = struct {
    null_count: ?i64 = null,
    min: ?[]const u8 = null,
    max: ?[]const u8 = null,
};

pub const ColumnMetaData = struct {
    ty: PhysicalType = .boolean,
    encodings: []Encoding = &.{},
    path_in_schema: [][]const u8 = &.{},
    compression: codec.Codec = .uncompressed,
    num_values: i64 = 0,
    total_uncompressed_size: i64 = 0,
    total_compressed_size: i64 = 0,
    data_page_offset: i64 = 0,
    dictionary_page_offset: ?i64 = null,
    stats: Statistics = .{},

    /// Where this chunk's pages start. A dictionary page, when present, precedes
    /// the data pages, so it — not `data_page_offset` — is the true beginning.
    pub fn startOffset(self: ColumnMetaData) i64 {
        if (self.dictionary_page_offset) |d| if (d > 0 and d < self.data_page_offset) return d;
        return self.data_page_offset;
    }
};

pub const ColumnChunk = struct {
    file_path: []const u8 = "",
    file_offset: i64 = 0,
    meta: ?ColumnMetaData = null,
};

pub const RowGroup = struct {
    columns: []ColumnChunk = &.{},
    total_byte_size: i64 = 0,
    num_rows: i64 = 0,
};

pub const FileMetaData = struct {
    version: i32 = 0,
    schema: []SchemaElement = &.{},
    num_rows: i64 = 0,
    row_groups: []RowGroup = &.{},
    created_by: []const u8 = "",

    /// Leaf columns, in the order their chunks appear. The first schema element
    /// is the synthetic root and is never a column.
    pub fn leafCount(self: FileMetaData) usize {
        var n: usize = 0;
        for (self.schema[@min(1, self.schema.len)..]) |e| {
            if (e.isLeaf()) n += 1;
        }
        return n;
    }
};

/// Byte range of the footer, given the file size and its last 8 bytes. Lets a
/// caller issue exactly two ranged reads rather than fetching the whole object.
pub fn footerRange(file_size: u64, trailer: []const u8) Error!struct { offset: u64, len: u32 } {
    if (trailer.len < trailer_len) return Error.NotParquet;
    const tail = trailer[trailer.len - trailer_len ..];
    if (!std.mem.eql(u8, tail[4..8], magic)) return Error.NotParquet;
    const len = std.mem.readInt(u32, tail[0..4], .little);
    if (@as(u64, len) + trailer_len > file_size) return Error.NotParquet;
    return .{ .offset = file_size - trailer_len - len, .len = len };
}

/// Parses a whole in-memory Parquet file, validating both magics.
pub fn parseFile(arena: std.mem.Allocator, bytes: []const u8) Error!FileMetaData {
    if (bytes.len < trailer_len + magic.len) return Error.NotParquet;
    if (!std.mem.eql(u8, bytes[0..4], magic)) return Error.NotParquet;
    const r = try footerRange(bytes.len, bytes);
    return parseFooter(arena, bytes[r.offset..][0..r.len]);
}

/// Parses a `FileMetaData` from the footer bytes alone.
pub fn parseFooter(arena: std.mem.Allocator, footer: []const u8) Error!FileMetaData {
    var r = thrift.Reader.init(footer);
    var md = FileMetaData{};

    try r.structBegin();
    while (true) {
        const f = try r.readField();
        if (f.ty == .stop) break;
        switch (f.id) {
            1 => md.version = try r.readI32(),
            2 => md.schema = try readList(SchemaElement, arena, &r, readSchemaElement),
            3 => md.num_rows = try r.readZigZag(),
            4 => md.row_groups = try readList(RowGroup, arena, &r, readRowGroup),
            6 => md.created_by = try r.readBinary(),
            else => try r.skip(f.ty),
        }
    }
    try r.structEnd();
    return md;
}

/// Reads a `list<struct>` into a slice using `readOne` per element.
fn readList(
    comptime T: type,
    arena: std.mem.Allocator,
    r: *thrift.Reader,
    comptime readOne: fn (std.mem.Allocator, *thrift.Reader) Error!T,
) Error![]T {
    const h = try r.readListHeader();
    const out = try arena.alloc(T, h.size);
    for (out) |*e| e.* = try readOne(arena, r);
    return out;
}

fn readSchemaElement(arena: std.mem.Allocator, r: *thrift.Reader) Error!SchemaElement {
    _ = arena;
    var e = SchemaElement{};
    try r.structBegin();
    while (true) {
        const f = try r.readField();
        if (f.ty == .stop) break;
        switch (f.id) {
            1 => e.ty = @enumFromInt(try r.readI32()),
            2 => e.type_length = try r.readI32(),
            3 => e.repetition = @enumFromInt(try r.readI32()),
            4 => e.name = try r.readBinary(),
            5 => e.num_children = try r.readI32(),
            6 => e.converted_type = try r.readI32(),
            7 => e.scale = try r.readI32(),
            8 => e.precision = try r.readI32(),
            else => try r.skip(f.ty),
        }
    }
    try r.structEnd();
    return e;
}

fn readRowGroup(arena: std.mem.Allocator, r: *thrift.Reader) Error!RowGroup {
    var g = RowGroup{};
    try r.structBegin();
    while (true) {
        const f = try r.readField();
        if (f.ty == .stop) break;
        switch (f.id) {
            1 => g.columns = try readList(ColumnChunk, arena, r, readColumnChunk),
            2 => g.total_byte_size = try r.readZigZag(),
            3 => g.num_rows = try r.readZigZag(),
            else => try r.skip(f.ty),
        }
    }
    try r.structEnd();
    return g;
}

fn readColumnChunk(arena: std.mem.Allocator, r: *thrift.Reader) Error!ColumnChunk {
    var c = ColumnChunk{};
    try r.structBegin();
    while (true) {
        const f = try r.readField();
        if (f.ty == .stop) break;
        switch (f.id) {
            1 => c.file_path = try r.readBinary(),
            2 => c.file_offset = try r.readZigZag(),
            3 => c.meta = try readColumnMetaData(arena, r),
            else => try r.skip(f.ty),
        }
    }
    try r.structEnd();
    return c;
}

/// Reads `Statistics`, preferring `min_value`/`max_value` (fields 5/6) over the
/// deprecated `min`/`max` (2/1) whose byte-array ordering was never consistent.
fn readStatistics(r: *thrift.Reader) Error!Statistics {
    var st = Statistics{};
    var legacy_min: ?[]const u8 = null;
    var legacy_max: ?[]const u8 = null;
    try r.structBegin();
    while (true) {
        const f = try r.readField();
        if (f.ty == .stop) break;
        switch (f.id) {
            1 => legacy_max = try r.readBinary(),
            2 => legacy_min = try r.readBinary(),
            3 => st.null_count = try r.readZigZag(),
            5 => st.max = try r.readBinary(),
            6 => st.min = try r.readBinary(),
            else => try r.skip(f.ty),
        }
    }
    try r.structEnd();
    if (st.min == null) st.min = legacy_min;
    if (st.max == null) st.max = legacy_max;
    return st;
}

fn readColumnMetaData(arena: std.mem.Allocator, r: *thrift.Reader) Error!ColumnMetaData {
    var m = ColumnMetaData{};
    try r.structBegin();
    while (true) {
        const f = try r.readField();
        if (f.ty == .stop) break;
        switch (f.id) {
            1 => m.ty = @enumFromInt(try r.readI32()),
            2 => {
                const h = try r.readListHeader();
                const out = try arena.alloc(Encoding, h.size);
                for (out) |*e| e.* = @enumFromInt(try r.readI32());
                m.encodings = out;
            },
            3 => {
                const h = try r.readListHeader();
                const out = try arena.alloc([]const u8, h.size);
                for (out) |*e| e.* = try r.readBinary();
                m.path_in_schema = out;
            },
            4 => m.compression = try enumFrom(codec.Codec, try r.readI32()),
            5 => m.num_values = try r.readZigZag(),
            6 => m.total_uncompressed_size = try r.readZigZag(),
            7 => m.total_compressed_size = try r.readZigZag(),
            9 => m.data_page_offset = try r.readZigZag(),
            11 => m.dictionary_page_offset = try r.readZigZag(),
            12 => m.stats = try readStatistics(r),
            else => try r.skip(f.ty),
        }
    }
    try r.structEnd();
    return m;
}

pub const PageType = enum(i32) {
    data_page = 0,
    index_page = 1,
    dictionary_page = 2,
    data_page_v2 = 3,
    _,
};

/// Header preceding every page in a column chunk. Also thrift-encoded, so pages
/// cannot be walked without the same decoder the footer needs.
pub const PageHeader = struct {
    ty: PageType = .data_page,
    uncompressed_page_size: i32 = 0,
    compressed_page_size: i32 = 0,
    /// Values in this page (data pages and dictionary pages alike).
    num_values: i32 = 0,
    encoding: Encoding = .plain,
    /// Bytes consumed by the header itself; the page body follows it.
    header_len: usize = 0,
    /// Data page v2 only: level sections sit *outside* the compressed region and
    /// their lengths come from the header rather than an inline prefix.
    def_levels_len: usize = 0,
    rep_levels_len: usize = 0,
    /// Data page v2 may declare its values uncompressed even when the chunk has
    /// a codec.
    is_compressed: bool = true,
};

/// Decodes a page header from the start of `bytes`, reporting how many bytes it
/// occupied so the caller can find the body.
pub fn parsePageHeader(bytes: []const u8) Error!PageHeader {
    var r = thrift.Reader.init(bytes);
    var h = PageHeader{};
    try r.structBegin();
    while (true) {
        const f = try r.readField();
        if (f.ty == .stop) break;
        switch (f.id) {
            1 => h.ty = @enumFromInt(try r.readI32()),
            2 => h.uncompressed_page_size = try r.readI32(),
            3 => h.compressed_page_size = try r.readI32(),
            // v1 data page and dictionary page
            5, 7 => try readPageDetail(&r, &h),
            8 => try readPageV2Detail(&r, &h),
            else => try r.skip(f.ty),
        }
    }
    try r.structEnd();
    h.header_len = r.pos;
    return h;
}

/// DataPageHeaderV2 numbers its fields differently from v1: the encoding is
/// field 4, and the level byte lengths are fields 5 and 6.
fn readPageV2Detail(r: *thrift.Reader, h: *PageHeader) Error!void {
    try r.structBegin();
    while (true) {
        const f = try r.readField();
        if (f.ty == .stop) break;
        switch (f.id) {
            1 => h.num_values = try r.readI32(),
            4 => h.encoding = @enumFromInt(try r.readI32()),
            5 => h.def_levels_len = @intCast(@max(0, try r.readI32())),
            6 => h.rep_levels_len = @intCast(@max(0, try r.readI32())),
            7 => h.is_compressed = f.ty == .bool_true,
            else => try r.skip(f.ty),
        }
    }
    try r.structEnd();
}

fn readPageDetail(r: *thrift.Reader, h: *PageHeader) Error!void {
    try r.structBegin();
    while (true) {
        const f = try r.readField();
        if (f.ty == .stop) break;
        switch (f.id) {
            1 => h.num_values = try r.readI32(),
            2 => h.encoding = @enumFromInt(try r.readI32()),
            else => try r.skip(f.ty),
        }
    }
    try r.structEnd();
}

/// Re-exported so callers can name the codec of a chunk without importing the
/// codec module separately.
pub const Codec = codec.Codec;

pub const Page = struct {
    header: PageHeader,
    /// Decompressed page body.
    data: []const u8,
    /// Offset of the next page in the chunk.
    next_offset: usize,
};

/// Reads one page at `offset`: thrift header, then the body decompressed with
/// the chunk's codec. This is where the three layers meet — thrift for framing,
/// codec for the body — so callers walk pages without touching either directly.
pub fn readPage(
    arena: std.mem.Allocator,
    bytes: []const u8,
    offset: usize,
    compression: Codec,
) (Error || codec.Error)!Page {
    if (offset >= bytes.len) return Error.NotParquet;
    const h = try parsePageHeader(bytes[offset..]);
    if (h.compressed_page_size < 0 or h.uncompressed_page_size < 0) return Error.NotParquet;

    const body_start = offset + h.header_len;
    const clen: usize = @intCast(h.compressed_page_size);
    if (body_start + clen > bytes.len) return Error.NotParquet;

    const body = bytes[body_start..][0..clen];
    const ulen: usize = @intCast(h.uncompressed_page_size);

    if (h.ty == .data_page_v2) {
        // v2 layout: [rep levels][def levels][values]. Only the values are
        // compressed, so the levels must be carved off before decompressing —
        // running the codec over the whole body would fail or produce garbage.
        const lvl = h.rep_levels_len + h.def_levels_len;
        if (lvl > body.len or lvl > ulen) return Error.NotParquet;
        const codec_used: Codec = if (h.is_compressed) compression else .uncompressed;
        const values = try codec.decompress(arena, codec_used, body[lvl..], ulen - lvl);

        const out = try arena.alloc(u8, ulen);
        @memcpy(out[0..lvl], body[0..lvl]);
        @memcpy(out[lvl..], values);
        return .{ .header = h, .data = out, .next_offset = body_start + clen };
    }

    const raw = try codec.decompress(arena, compression, body, ulen);
    return .{ .header = h, .data = raw, .next_offset = body_start + clen };
}

// --- tests ------------------------------------------------------------------

const t = std.testing;

/// Footer of a 3-row file written by DuckDB v1.5.1 (id INT32, name BYTE_ARRAY,
/// amt DOUBLE, uncompressed). A real writer's output, not a hand-built vector.
const tiny_footer =
    "\x15\x02\x19\x4c\x35\x00\x18\x0d\x64\x75\x63\x6b\x64\x62\x5f\x73\x63\x68\x65\x6d\x61\x15\x06\x00" ++
    "\x15\x02\x25\x02\x18\x02\x69\x64\x25\x22\x00\x15\x0c\x25\x02\x18\x04\x6e\x61\x6d\x65\x25\x00\x00";

test "footerRange locates the footer from the trailer and rejects bad magic" {
    // 468-byte file, 322-byte footer: offset 468-8-322 = 138
    var trailer: [8]u8 = undefined;
    std.mem.writeInt(u32, trailer[0..4], 322, .little);
    @memcpy(trailer[4..8], magic);
    const r = try footerRange(468, &trailer);
    try t.expectEqual(@as(u64, 138), r.offset);
    try t.expectEqual(@as(u32, 322), r.len);

    var bad = trailer;
    bad[4] = 'X';
    try t.expectError(Error.NotParquet, footerRange(468, &bad));

    // a footer longer than the file is corruption, not a huge read
    var big: [8]u8 = undefined;
    std.mem.writeInt(u32, big[0..4], 999_999, .little);
    @memcpy(big[4..8], magic);
    try t.expectError(Error.NotParquet, footerRange(468, &big));
}

test "parseFile rejects anything without both magics" {
    var ar = std.heap.ArenaAllocator.init(t.allocator);
    defer ar.deinit();
    try t.expectError(Error.NotParquet, parseFile(ar.allocator(), "not a parquet file at all"));
    try t.expectError(Error.NotParquet, parseFile(ar.allocator(), "PAR1"));
}

test "schema elements decode from a real DuckDB footer prefix" {
    var ar = std.heap.ArenaAllocator.init(t.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    // The prefix above covers version + the first schema entries; decoding it
    // exercises the delta-id and nested-struct handling against real bytes.
    var r = thrift.Reader.init(tiny_footer);
    try r.structBegin();
    const f1 = try r.readField();
    try t.expectEqual(@as(i16, 1), f1.id);
    try t.expectEqual(@as(i32, 1), try r.readI32()); // version 1

    const f2 = try r.readField();
    try t.expectEqual(@as(i16, 2), f2.id); // schema list
    const h = try r.readListHeader();
    try t.expectEqual(thrift.Type.@"struct", h.elem);

    const root = try readSchemaElement(a, &r);
    try t.expectEqualStrings("duckdb_schema", root.name);
    try t.expectEqual(@as(i32, 3), root.num_children); // three columns
    try t.expect(root.isLeaf() == false);

    const id_col = try readSchemaElement(a, &r);
    try t.expectEqualStrings("id", id_col.name);
    try t.expectEqual(PhysicalType.int32, id_col.ty.?);
    try t.expect(id_col.isLeaf());

    const name_col = try readSchemaElement(a, &r);
    try t.expectEqualStrings("name", name_col.name);
    try t.expectEqual(PhysicalType.byte_array, name_col.ty.?);
}

test "startOffset prefers the dictionary page when one precedes the data pages" {
    const with_dict = ColumnMetaData{ .data_page_offset = 500, .dictionary_page_offset = 200 };
    try t.expectEqual(@as(i64, 200), with_dict.startOffset());

    const no_dict = ColumnMetaData{ .data_page_offset = 500 };
    try t.expectEqual(@as(i64, 500), no_dict.startOffset());

    // some writers emit 0 for "absent" rather than omitting the field
    const zero = ColumnMetaData{ .data_page_offset = 500, .dictionary_page_offset = 0 };
    try t.expectEqual(@as(i64, 500), zero.startOffset());
}

// --- real-file tests --------------------------------------------------------
//
// Fixtures are written by DuckDB v1.5.1: the same 60 rows in five codecs. They
// are ~6 KB total and exist to prove interoperability with a real writer, which
// hand-built vectors cannot do — the Snappy and LZ4 decoders in `codec.zig` are
// our own, and this is what actually holds them honest.

const fx_uncompressed = @embedFile("testdata/uncompressed.parquet");
const fx_snappy = @embedFile("testdata/snappy.parquet");
const fx_gzip = @embedFile("testdata/gzip.parquet");
const fx_zstd = @embedFile("testdata/zstd.parquet");
const fx_lz4 = @embedFile("testdata/lz4.parquet");

test "footer of a real DuckDB file decodes to the expected schema and layout" {
    var ar = std.heap.ArenaAllocator.init(t.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const md = try parseFile(a, fx_uncompressed);
    try t.expectEqual(@as(i64, 60), md.num_rows);
    try t.expectEqual(@as(usize, 1), md.row_groups.len);
    try t.expectEqual(@as(usize, 4), md.leafCount());
    try t.expect(std.mem.startsWith(u8, md.created_by, "DuckDB"));

    // schema is root + 4 leaves, in column order
    try t.expectEqual(@as(usize, 5), md.schema.len);
    try t.expectEqualStrings("id", md.schema[1].name);
    try t.expectEqual(PhysicalType.int32, md.schema[1].ty.?);
    try t.expectEqualStrings("name", md.schema[2].name);
    try t.expectEqual(PhysicalType.byte_array, md.schema[2].ty.?);
    try t.expectEqualStrings("amt", md.schema[3].name);
    try t.expectEqual(PhysicalType.double, md.schema[3].ty.?);
    try t.expectEqualStrings("flag", md.schema[4].name);
    try t.expectEqual(PhysicalType.boolean, md.schema[4].ty.?);

    const g = md.row_groups[0];
    try t.expectEqual(@as(i64, 60), g.num_rows);
    try t.expectEqual(@as(usize, 4), g.columns.len);
    for (g.columns) |c| {
        const m = c.meta.?;
        try t.expectEqual(@as(i64, 60), m.num_values);
        try t.expectEqual(Codec.uncompressed, m.compression);
        try t.expect(m.startOffset() > 0);
    }
}

// The decisive codec test: identical rows written five ways must decompress to
// byte-identical PLAIN page bodies. Matching *lengths* would prove nothing —
// matching *bytes* is what catches a wrong copy offset in Snappy or LZ4.
test "every codec decompresses a real page to byte-identical output" {
    var ar = std.heap.ArenaAllocator.init(t.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const files = [_][]const u8{ fx_snappy, fx_gzip, fx_zstd, fx_lz4 };
    for (0..4) |col| {
        const want = try firstPage(a, fx_uncompressed, col);
        for (files) |f| {
            const got = try firstPage(a, f, col);
            try t.expectEqualSlices(u8, want, got);
        }
    }
}

test "each fixture reports the codec it was written with" {
    var ar = std.heap.ArenaAllocator.init(t.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const cases = [_]struct { bytes: []const u8, want: Codec }{
        .{ .bytes = fx_uncompressed, .want = .uncompressed },
        .{ .bytes = fx_snappy, .want = .snappy },
        .{ .bytes = fx_gzip, .want = .gzip },
        .{ .bytes = fx_zstd, .want = .zstd },
        // DuckDB writes LZ4_RAW (codec 7) when asked for LZ4, not the
        // deprecated Hadoop-framed codec 5.
        .{ .bytes = fx_lz4, .want = .lz4_raw },
    };
    for (cases) |c| {
        const md = try parseFile(a, c.bytes);
        try t.expectEqual(c.want, md.row_groups[0].columns[0].meta.?.compression);
    }
}

test "page headers of a real file report plain encoding and exact sizes" {
    var ar = std.heap.ArenaAllocator.init(t.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const md = try parseFile(a, fx_zstd);
    const m = md.row_groups[0].columns[0].meta.?;
    const pg = try readPage(a, fx_zstd, @intCast(m.startOffset()), m.compression);
    try t.expectEqual(PageType.data_page, pg.header.ty);
    try t.expectEqual(Encoding.plain, pg.header.encoding);
    try t.expectEqual(@as(i32, 60), pg.header.num_values);
    // readPage validates this, but assert it explicitly: the decompressed body
    // must be exactly the size the header promised.
    try t.expectEqual(@as(usize, @intCast(pg.header.uncompressed_page_size)), pg.data.len);
}

fn firstPage(a: std.mem.Allocator, file: []const u8, col: usize) ![]const u8 {
    const md = try parseFile(a, file);
    const m = md.row_groups[0].columns[col].meta.?;
    const pg = try readPage(a, file, @intCast(m.startOffset()), m.compression);
    return pg.data;
}
