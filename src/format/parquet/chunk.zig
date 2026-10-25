//! A column chunk's pages decoded into a column: dictionary and data pages (v1 and v2),
//! levels, and one typed kernel every encoding feeds. An encoding decodes a page's
//! present values into a source, a `Cast` function turns each physical value into
//! the column's store type, and `Rows` lays them out around the null rows; a
//! statistic's value goes through the same kernel into `One`.

const Decimal = @import("../../exec/value.zig").Decimal;
const Value = @import("../../exec/value.zig").Value;
const Error = @import("read.zig").Error;
const Levels = @import("nested.zig").Levels;
const ListShape = @import("schema.zig").ListShape;
const assembleLists = @import("nested.zig").assembleLists;
const basaltType = @import("logical.zig").basaltType;
const bitWidth = @import("encoding.zig").bitWidth;
const capacity_hint = @import("encoding.zig").capacity_hint;
const column = @import("../../exec/column.zig");
const decodeByteStreamSplit = @import("encoding.zig").decodeByteStreamSplit;
const decodeDeltaBinaryPacked = @import("encoding.zig").decodeDeltaBinaryPacked;
const decodeDeltaByteArray = @import("encoding.zig").decodeDeltaByteArray;
const decodeDeltaLengthByteArray = @import("encoding.zig").decodeDeltaLengthByteArray;
const decodeRleHybrid = @import("encoding.zig").decodeRleHybrid;
const int96ToMicros = @import("encoding.zig").int96ToMicros;
const civil = @import("../../exec/civil.zig");
const parquet = @import("footer.zig");
const std = @import("std");
const temporalTick = @import("logical.zig").temporalTick;
const types = @import("../../lang/types.zig");
const unscaledOf = @import("logical.zig").unscaledOf;
const unsignedBits = @import("logical.zig").unsignedBits;
const eval = @import("../../exec/eval.zig");
const Reader = @import("read.zig").Reader;
const fx = @import("testing_util.zig").fx;
const testing = std.testing;

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
) ReadError!column.Column {
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
) ReadError!column.Column {
    const d = try readPages(arena, file_bytes, meta, elem, if (list == null) rows else null, max_def, max_rep, base_offset);
    const shape = list orelse return d.vals;
    return assembleLists(arena, d.vals, d.levels.?.reps.items, d.levels.?.defs.items, max_def, shape, rows);
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
) ReadError!Entries {
    const d = try readPages(arena, file_bytes, meta, elem, null, max_def, max_rep, base_offset);
    const lv = d.levels.?;
    if (d.vals.len != lv.reps.items.len or d.vals.len != lv.defs.items.len) return Error.CorruptParquetPage;
    return .{ .vals = d.vals, .reps = lv.reps.items, .defs = lv.defs.items };
}

const ReadError = Error || parquet.Error || @import("../compress/codecs.zig").Error;

/// Every read of a chunk walks its pages here: `rows` values, or with `rows` null
/// the chunk's `num_values` entries along with their levels. Pages holding more
/// values than the row group's rows are corrupt, never cut to fit.
fn readPages(
    arena: std.mem.Allocator,
    file_bytes: []const u8,
    meta: parquet.ColumnMetaData,
    elem: parquet.SchemaElement,
    rows: ?usize,
    max_def: u32,
    max_rep: u32,
    base_offset: u64,
) ReadError!struct { vals: column.Column, levels: ?Levels } {
    const cast: Cast = .{ .ty = (try basaltType(elem)).asNullable(), .tick = temporalTick(elem) };
    const entries = rows orelse std.math.cast(usize, meta.num_values) orelse return Error.CorruptParquetPage;
    var levels: ?Levels = if (rows == null) .{
        .reps = std.array_list.Managed(u32).init(arena),
        .defs = std.array_list.Managed(u32).init(arena),
    } else null;

    var b = try column.Builder.initCapacity(arena, cast.ty, @min(entries, capacity_hint));
    var dict: ?Dict = null;
    const at = std.math.cast(u64, meta.startOffset()) orelse return Error.CorruptParquetPage;
    if (at < base_offset) return Error.CorruptParquetPage;
    var offset: usize = std.math.cast(usize, at - base_offset) orelse return Error.CorruptParquetPage;
    var produced: usize = 0;

    while (produced < entries) {
        if (offset >= file_bytes.len) return Error.CorruptParquetPage;
        const pg = try readPageOf(arena, file_bytes, offset, meta, elem);
        offset = pg.next_offset;

        switch (pg.header.ty) {
            .dictionary_page => dict = try readDict(arena, pg, meta, elem, cast),
            .data_page, .data_page_v2 => {
                produced += try appendDataPage(arena, &b, pg, meta, elem, cast, max_def, dict, max_rep, if (levels) |*l| l else null);
            },
            .index_page => {},
            else => return Error.CorruptParquetPage,
        }
    }
    if (rows != null and produced != entries) return Error.CorruptParquetPage;
    const vals = try b.finish();
    try readUnsigned(elem, vals);
    return .{ .vals = vals, .levels = levels };
}

fn readPageOf(arena: std.mem.Allocator, file_bytes: []const u8, offset: usize, meta: parquet.ColumnMetaData, elem: parquet.SchemaElement) !parquet.Page {
    return parquet.readPage(arena, file_bytes, offset, meta.compression) catch |e| {
        if (e != error.UnsupportedCodec) return e;
        var buf: [256]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "parquet column `{s}` is compressed with {s}, which basalt does not read (it reads snappy, gzip, zstd and lz4)", .{ elem.name, @tagName(meta.compression) }) catch "a parquet codec basalt does not read";
        return @errorCast(eval.explain(e, msg));
    };
}

/// Every decode path sign-extends a physical int, so an unsigned column is mended
/// once here; a UINT64 past 2^63-1 is refused rather than read negative.
fn readUnsigned(elem: parquet.SchemaElement, col: column.Column) Error!void {
    if (col.ty.kind != .int) return;
    const bits = unsignedBits(elem) orelse return;
    for (col.data.i64, 0..) |*x, i| {
        if (!col.validity.get(i)) continue;
        x.* = types.unsignedOf(bits, x.*) orelse {
            var buf: [256]u8 = undefined;
            const msg = std.fmt.bufPrint(&buf, "parquet column `{s}` holds the UINT64 {d}, past 2^63-1, the largest basalt int", .{ elem.name, @as(u64, @bitCast(x.*)) }) catch "a parquet UINT64 past 2^63-1";
            return @errorCast(eval.explain(Error.ParquetIntOverflow, msg));
        };
    }
}

/// A dictionary decoded once into a typed column; a byte dictionary also keeps its
/// values as slices, which pages gather from and the column notes as its codes,
/// with an `id` no other dictionary in the process shares (an address can be reused).
const Dict = struct {
    col: column.Column,
    entries: []const []const u8,
    id: u64,
    /// A byte array dictionary's values end to end with 16 bytes to spare, and
    /// where each starts, for `Builder.appendDictBytes`.
    flat: []const u8 = &.{},
    offs: []const i32 = &.{},
};

var dict_ids = std.atomic.Value(u64).init(1);

/// The dictionary page is PLAIN, so it goes through the same kernel as a data page.
fn readDict(arena: std.mem.Allocator, pg: parquet.Page, meta: parquet.ColumnMetaData, elem: parquet.SchemaElement, cast: Cast) Error!Dict {
    const n = std.math.cast(usize, pg.header.num_values) orelse return Error.CorruptParquetPage;
    if (!plainFits(meta.ty, elem.type_length orelse 0, pg.data.len, n)) return Error.CorruptParquetPage;
    var db = try column.Builder.initCapacity(arena, cast.ty, n);
    try pushPlain(arena, Rows{ .arena = arena, .b = &db, .defs = null, .max_def = 0 }, cast, meta.ty, elem.type_length orelse 0, pg.data, n);
    const col = try db.finish();
    var d = Dict{ .col = col, .entries = &.{}, .id = dict_ids.fetchAdd(1, .monotonic) };
    if (col.data == .bytes) {
        const bs = col.data.bytes;
        const out = try arena.alloc([]const u8, n);
        for (out, 0..) |*o, i| o.* = bs.at(i);
        d.entries = out;
        const used: usize = @intCast(bs.offsets[n]);
        const flat = try arena.alloc(u8, used + 16);
        @memcpy(flat[0..used], bs.values[0..used]);
        @memset(flat[used..], 0);
        d.flat = flat;
        d.offs = bs.offsets[0 .. n + 1];
    }
    return d;
}

/// v1 length-prefixes each level section; v2 keeps them unprefixed with lengths
/// in the header. Every encoding hands the kernel only the present values; the
/// definition levels place them and mark the rest null.
fn appendDataPage(
    arena: std.mem.Allocator,
    b: *column.Builder,
    pg: parquet.Page,
    meta: parquet.ColumnMetaData,
    elem: parquet.SchemaElement,
    cast: Cast,
    max_def: u32,
    dict: ?Dict,
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
    var present: usize = n;
    if (defs) |d| {
        present = 0;
        for (d) |lvl| {
            if (lvl == max_def) present += 1;
        }
    }

    const rows: Rows = .{ .arena = arena, .b = b, .defs = defs, .max_def = max_def };
    const type_length = elem.type_length orelse 0;
    switch (pg.header.encoding) {
        .plain => try pushPlain(arena, rows, cast, meta.ty, type_length, body, present),
        .byte_stream_split => {
            const width: usize = switch (meta.ty) {
                .float => 4,
                .double => 8,
                .int32 => 4,
                .int64 => 8,
                .fixed_len_byte_array => @intCast(@max(0, type_length)),
                else => return Error.UnsupportedParquetEncoding,
            };
            const flat = try decodeByteStreamSplit(arena, body, width, present);
            try pushPlain(arena, rows, cast, meta.ty, type_length, flat, present);
        },
        .delta_binary_packed => {
            const vals = try decodeDeltaBinaryPacked(arena, body, present);
            if (meta.ty == .int32) {
                for (vals) |*v| v.* = @as(i32, @truncate(v.*));
            }
            try pushInts(rows, cast, Items(i64){ .items = vals }, present);
        },
        .delta_length_byte_array => try pushBytes(rows, cast, try decodeDeltaLengthByteArray(arena, body, present)),
        .delta_byte_array => try pushBytes(rows, cast, try decodeDeltaByteArray(arena, body, present)),
        .rle => {
            const bits = try decodeRleHybrid(arena, body[@min(4, body.len)..], 1, present);
            try pushInts(rows, cast, Items(u32){ .items = bits }, present);
        },
        .plain_dictionary, .rle_dictionary => {
            const d = dict orelse return Error.CorruptParquetPage;
            if (body.len < 1) return Error.CorruptParquetPage;
            if (body[0] > 32) return Error.CorruptParquetPage;
            const width: u6 = @intCast(body[0]);
            const idx = try decodeRleHybrid(arena, body[1..], width, present);
            try pushDict(rows, cast, d, idx);
        },
        else => return Error.UnsupportedParquetEncoding,
    }
    if (levels) |lv| {
        if (reps) |r| try lv.reps.appendSlice(r) else try lv.reps.appendNTimes(0, n);
        if (defs) |d| try lv.defs.appendSlice(d) else try lv.defs.appendNTimes(max_def, n);
    }
    return n;
}

/// How a physical value becomes the column's store type: the only per-type code
/// the kernel runs. Ints widen to i64 before they arrive.
const Cast = struct {
    ty: types.Type,
    tick: civil.Tick,

    fn same(_: Cast, x: anytype) @TypeOf(x) {
        return x;
    }

    /// A count of `t` as micros, one function per unit so the page loop has no
    /// branch on it.
    fn micros(comptime t: civil.Tick) fn (Cast, i64) i64 {
        return struct {
            fn f(_: Cast, x: i64) i64 {
                return t.toMicros(x, .saturate);
            }
        }.f;
    }

    fn date(_: Cast, x: i64) i32 {
        return @intCast(x);
    }

    fn truthy(_: Cast, x: i64) bool {
        return x != 0;
    }

    fn decimal(c: Cast, x: i64) Decimal {
        return .{ .unscaled = x, .scale = c.ty.scale };
    }

    fn bytesDecimal(c: Cast, s: []const u8) Decimal {
        return .{ .unscaled = unscaledOf(s), .scale = c.ty.scale };
    }
};

/// The kernel's destination for a page: the `n` present values of `src`, each
/// through `conv`, land at the rows whose definition level is `max_def` and the
/// rest read null. A page with no nulls is written straight into the store.
const Rows = struct {
    arena: std.mem.Allocator,
    b: *column.Builder,
    defs: ?[]const u32,
    max_def: u32,

    fn put(s: Rows, comptime T: type, cast: Cast, comptime conv: anytype, src: anytype, n: usize) Error!void {
        const dense = if (s.defs) |d| d.len == n else true;
        const out = if (dense) s.b.reserveSlots(T, n) catch |e| return landed(e) else try s.arena.alloc(T, n);
        for (out, 0..) |*o, i| o.* = conv(cast, src.get(i));
        if (dense) {
            s.b.commitSlots(T, n) catch |e| return landed(e);
        } else {
            s.b.appendBulkScattered(T, out, s.defs.?, s.max_def) catch |e| return landed(e);
        }
    }

    fn bytes(s: Rows, _: Cast, vals: []const []const u8) Error!void {
        s.b.appendBytesScattered(vals, s.defs, s.max_def) catch |e| return landed(e);
    }
};

/// The kernel's destination for a statistic: its one value, read back as a
/// one-row column of the same store would give it.
const One = struct {
    out: *Value,

    fn put(s: One, comptime T: type, cast: Cast, comptime conv: anytype, src: anytype, _: usize) Error!void {
        var x = [1]T{conv(cast, src.get(0))};
        var valid = [1]u8{1};
        const row: column.Column = .{ .ty = cast.ty, .len = 1, .validity = .{ .bits = &valid, .len = 1 }, .data = storeOf(T, &x) };
        s.out.* = row.getValue(0);
    }

    fn bytes(s: One, cast: Cast, vals: []const []const u8) Error!void {
        s.out.* = if (cast.ty.kind == .string) .{ .string = vals[0] } else .{ .bytes = vals[0] };
    }

    fn storeOf(comptime T: type, x: []T) column.Column.Data {
        inline for (comptime std.enums.values(column.Tag)) |t| {
            if (t != .bytes and column.Elem(t) == T) return column.dataOf(t, x);
        }
        @compileError("no fixed-width storage holds " ++ @typeName(T));
    }
};

/// A statistic's raw min or max as the column's pages would read it, through the
/// page kernel so pruning and scans agree: the first PLAIN value of `raw`, except a
/// BYTE_ARRAY statistic, which is the value's bytes with no length prefix.
pub fn plainValue(elem: parquet.SchemaElement, phys: parquet.PhysicalType, raw: []const u8) Error!Value {
    const cast: Cast = .{ .ty = try basaltType(elem), .tick = temporalTick(elem) };
    var slot: [1][]const u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(std.mem.sliceAsBytes(&slot));
    var v: Value = .null;
    if (phys == .byte_array) {
        try pushBytes(One{ .out = &v }, cast, &.{raw});
        return v;
    }
    try pushPlain(fba.allocator(), One{ .out = &v }, cast, phys, elem.type_length orelse 0, raw, 1);
    return v;
}

fn landed(e: anytype) Error {
    return switch (e) {
        error.OutOfMemory => error.OutOfMemory,
        else => Error.CorruptParquetPage,
    };
}

/// Physical ints, and booleans as 0 or 1, into whichever store the column's kind has.
fn pushInts(sink: anytype, cast: Cast, src: anytype, n: usize) Error!void {
    return switch (cast.ty.kind) {
        .int => sink.put(i64, cast, Cast.same, src, n),
        .time, .timestamp => switch (cast.tick) {
            .us => sink.put(i64, cast, Cast.same, src, n),
            inline else => |t| sink.put(i64, cast, Cast.micros(t), src, n),
        },
        .date => sink.put(i32, cast, Cast.date, src, n),
        .decimal => sink.put(Decimal, cast, Cast.decimal, src, n),
        .bool => sink.put(bool, cast, Cast.truthy, src, n),
        else => Error.UnsupportedParquetEncoding,
    };
}

fn pushFloats(sink: anytype, cast: Cast, src: anytype, n: usize) Error!void {
    if (cast.ty.kind != .float) return Error.UnsupportedParquetEncoding;
    return sink.put(f64, cast, Cast.same, src, n);
}

/// Text and binary columns take the slices as they are; a decimal reads each as
/// a big-endian unscaled value.
fn pushBytes(sink: anytype, cast: Cast, vals: []const []const u8) Error!void {
    switch (cast.ty.kind) {
        .string, .bytes => try sink.bytes(cast, vals),
        .decimal => try sink.put(Decimal, cast, Cast.bytesDecimal, Items([]const u8){ .items = vals }, vals.len),
        else => return Error.UnsupportedParquetEncoding,
    }
}

/// PLAIN values (a data or dictionary page's, byte-stream-split ones once
/// interleaved back, or a statistic's), fixed-width ones read in place.
fn pushPlain(
    arena: std.mem.Allocator,
    sink: anytype,
    cast: Cast,
    phys: parquet.PhysicalType,
    type_length: i32,
    body: []const u8,
    n: usize,
) Error!void {
    if (!plainFits(phys, type_length, body.len, n)) return Error.CorruptParquetPage;
    switch (phys) {
        .boolean => {
            if (body.len < (n + 7) / 8) return Error.CorruptParquetPage;
            try pushInts(sink, cast, Bits{ .body = body }, n);
        },
        .int32 => try pushInts(sink, cast, try plain(i32, body, n), n),
        .int64 => try pushInts(sink, cast, try plain(i64, body, n), n),
        .float => try pushFloats(sink, cast, try plain(f32, body, n), n),
        .double => try pushFloats(sink, cast, try plain(f64, body, n), n),
        .int96 => {
            if (body.len < n * 12) return Error.CorruptParquetPage;
            try sink.put(i64, cast, Cast.same, Int96{ .body = body }, n);
        },
        .byte_array => try pushBytes(sink, cast, try plainByteArrays(arena, body, n)),
        .fixed_len_byte_array => try pushBytes(sink, cast, try fixedByteArrays(arena, body, type_length, n)),
        else => return Error.UnsupportedParquetEncoding,
    }
}

/// Whether `len` bytes can hold `n` PLAIN values: each takes at least a bit
/// (BOOLEAN), its width, or a BYTE_ARRAY's 4-byte length.
fn plainFits(phys: parquet.PhysicalType, type_length: i32, len: usize, n: usize) bool {
    const bits: usize = switch (phys) {
        .boolean => 1,
        .int32, .float, .byte_array => 32,
        .int64, .double => 64,
        .int96 => 96,
        .fixed_len_byte_array => if (type_length > 0) 8 * @as(usize, @intCast(type_length)) else return true,
        else => return true,
    };
    return n <= (len *| 8) / bits;
}

fn plain(comptime P: type, body: []const u8, n: usize) Error!Plain(P) {
    if (body.len < n * @sizeOf(P)) return Error.CorruptParquetPage;
    return .{ .body = body };
}

/// Fixed-width little-endian values read in place.
fn Plain(comptime P: type) type {
    return struct {
        body: []const u8,

        inline fn get(s: @This(), i: usize) P {
            const U = std.meta.Int(.unsigned, @bitSizeOf(P));
            return @bitCast(std.mem.readInt(U, s.body[i * @sizeOf(P) ..][0..@sizeOf(P)], .little));
        }
    };
}

/// PLAIN booleans: bit-packed, LSB first.
const Bits = struct {
    body: []const u8,

    inline fn get(s: Bits, i: usize) u1 {
        return @truncate(s.body[i >> 3] >> @intCast(i & 7));
    }
};

/// INT96: nanoseconds of day then Julian day, read as micros.
const Int96 = struct {
    body: []const u8,

    inline fn get(s: Int96, i: usize) i64 {
        const w = s.body[i * 12 ..][0..12];
        return int96ToMicros(std.mem.readInt(u32, w[8..12], .little), std.mem.readInt(u64, w[0..8], .little));
    }
};

/// Values an encoding already decoded into a slice.
fn Items(comptime S: type) type {
    return struct {
        items: []const S,

        inline fn get(s: @This(), i: usize) S {
            return s.items[i];
        }
    };
}

/// Dictionary entries looked up by a page's indices.
fn Gather(comptime T: type) type {
    return struct {
        vals: []const T,
        idx: []const u32,

        inline fn get(s: @This(), i: usize) T {
            return s.vals[s.idx[i]];
        }
    };
}

/// Byte arrays are slices of the page body.
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

fn fixedByteArrays(arena: std.mem.Allocator, body: []const u8, type_length: i32, count: usize) Error![]const []const u8 {
    const w: usize = if (type_length > 0) @intCast(type_length) else 0;
    if (count > 0 and w == 0) return Error.CorruptParquetPage;
    if (body.len < w * count) return Error.CorruptParquetPage;
    const out = try arena.alloc([]const u8, count);
    for (out, 0..) |*o, i| o.* = body[i * w ..][0..w];
    return out;
}

/// Indices are bounds-checked once, then gathered through the typed dictionary.
fn pushDict(rows: Rows, cast: Cast, d: Dict, idx: []const u32) Error!void {
    var top: u32 = 0;
    for (idx) |ix| top = @max(top, ix);
    if (idx.len > 0 and top >= d.col.len) return Error.CorruptParquetPage;
    switch (d.col.data) {
        .bytes => {
            rows.b.appendDictBytes(d.flat, d.offs, idx, rows.defs, rows.max_def) catch |e| return landed(e);
            try rows.b.noteDict(d.id, d.entries, idx, rows.defs, rows.max_def);
        },
        inline else => |_, tag| {
            const vals = column.fixedOf(tag, d.col.data);
            const T = column.Elem(tag);
            try rows.put(T, cast, Cast.same, Gather(T){ .vals = vals, .idx = idx }, idx.len);
        },
    }
}

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

test "ranged reads return the same values as an in-memory file" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "r.parquet", .data = fx });
    const dir = try tmp.dir.realpathAlloc(a, ".");
    const path = try std.fs.path.join(a, &.{ dir, "r.parquet" });

    const r = try Reader.open(a, path);
    defer r.close();
    try testing.expect(r.src == .file);
    const got = (try r.next(a)).?;
    try testing.expectEqual(@as(usize, 60), got.len);
    try testing.expect((try r.next(a)) == null);

    const md = try parquet.parseFile(a, fx);
    const g = md.row_groups[0];
    try testing.expectEqual(g.columns.len, got.columns.len);
    for (g.columns, 0..) |c, ci| {
        const want = try readColumnChunk(a, fx, c.meta.?, md.schema[ci + 1], @intCast(g.num_rows), 1, 0);
        try testing.expectEqual(want.len, got.columns[ci].len);
        for (0..want.len) |i| try testing.expectEqualDeep(want.getValue(i), got.columns[ci].getValue(i));
    }
}

test "a PLAIN count the page body cannot hold is refused before it is allocated" {
    var buf: [4096]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    var v: Value = .null;
    const cast: Cast = .{ .ty = types.Type.init(.string), .tick = .us };
    const body = [_]u8{ 1, 0, 0, 0, 'a', 1, 0, 0, 0, 'b' };
    try testing.expectError(Error.CorruptParquetPage, pushPlain(fba.allocator(), One{ .out = &v }, cast, .byte_array, 0, &body, 1 << 30));
    try testing.expectError(Error.CorruptParquetPage, pushPlain(fba.allocator(), One{ .out = &v }, cast, .fixed_len_byte_array, 2, &body, 1 << 30));
    try testing.expect(plainFits(.byte_array, 0, body.len, 2));
    try testing.expect(!plainFits(.byte_array, 0, body.len, 3));
    try testing.expect(plainFits(.boolean, 0, 1, 8) and !plainFits(.boolean, 0, 1, 9));
    try testing.expect(!plainFits(.int96, 0, 23, 2));
}

test "a dictionary page's count is bounded by its body before the dictionary is sized" {
    var buf: [4096]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    const body = [_]u8{ 1, 0, 0, 0, 'a', 1, 0, 0, 0, 'b' };
    const pg: parquet.Page = .{ .header = .{ .ty = .dictionary_page, .num_values = 1 << 30 }, .data = &body, .next_offset = 0 };
    const cast: Cast = .{ .ty = types.Type.init(.string).asNullable(), .tick = .us };
    try testing.expectError(Error.CorruptParquetPage, readDict(fba.allocator(), pg, .{ .ty = .byte_array }, .{ .ty = .byte_array }, cast));
}

test "a footer's row count only hints the column's size, so a lying one fails on the pages, not the allocation" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const md = try parquet.parseFile(a, fx);
    const g = md.row_groups[0];
    if (readColumnChunk(a, fx, g.columns[0].meta.?, md.schema[1], 1 << 50, 1, 0)) |_| {
        return error.TestUnexpectedResult;
    } else |e| try testing.expect(e != error.OutOfMemory);
}

test "a nested leaf's page with no level streams pads its levels only after its values prove the count" {
    var buf: [64 * 1024]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    const a = fba.allocator();
    var levels: Levels = .{ .reps = std.array_list.Managed(u32).init(a), .defs = std.array_list.Managed(u32).init(a) };
    const cast: Cast = .{ .ty = types.Type.init(.int).asNullable(), .tick = .us };
    var b = column.Builder.init(a, cast.ty);
    const body = [_]u8{ 1, 0, 0, 0, 2, 0, 0, 0 };
    const pg: parquet.Page = .{ .header = .{ .ty = .data_page_v2, .num_values = 1 << 30, .encoding = .plain }, .data = &body, .next_offset = 0 };
    try testing.expectError(Error.CorruptParquetPage, appendDataPage(a, &b, pg, .{ .ty = .int32 }, .{ .ty = .int32 }, cast, 0, null, 0, &levels));
    try testing.expectEqual(@as(usize, 0), levels.reps.items.len);
}
