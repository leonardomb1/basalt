//! Parquet writer: PLAIN-encoded pages, one data page per column chunk, one
//! chunk per row group.
//!
//! Every column is written OPTIONAL, so every page carries definition levels —
//! uniform and always correct, at the cost of one bit per row for columns that
//! happen to have no nulls.
//!
//! Rows accumulate until `row_group_rows`, then a row group is flushed and the
//! buffers are reset. That bounds memory to roughly one row group rather than
//! the whole file, which is the closest a Parquet writer can get to streaming:
//! the format needs a chunk's full size and offset before its metadata can be
//! written, and the footer needs every row group.
//!
//! Output is strictly forward — every byte goes through `emit`, which tracks the
//! running offset itself and never seeks — so the destination only has to be a
//! `*std.Io.Writer`. That is what lets the same writer target a local file or an
//! Azure block blob, and what a future S3/GCS sink would plug into.

const std = @import("std");
const pq = @import("parquet.zig");
const thrift = @import("thrift.zig");
const codec = @import("codec.zig");
const eval = @import("../exec/eval.zig");
const driver = @import("driver.zig");
const azure = @import("azure.zig");
const httpx = @import("http.zig");
const types = @import("../lang/types.zig");
const batchmod = @import("../exec/batch.zig");
const valuemod = @import("../exec/value.zig");

const Batch = batchmod.Batch;
const Value = valuemod.Value;
const List = std.array_list.Managed;

pub const Error = error{
    UnsupportedParquetWrite,
    /// A parquet file cannot be extended: the footer indexes every row group and
    /// is written last, so adding rows means rewriting the file.
    AppendNotSupported,
    /// A DECIMAL column this writer cannot store: scale above precision (which no
    /// reader accepts), or a value needing more than the 18 digits INT64 holds.
    /// Wider decimals need the FIXED_LEN_BYTE_ARRAY physical type, unimplemented.
    UnsupportedParquetDecimal,
} || std.mem.Allocator.Error || codec.Error;

/// Rows buffered before a row group is flushed.
pub const row_group_rows = 100_000;

/// Target uncompressed size of one data page. Splitting a chunk into pages of
/// roughly this size lets a reader skip and buffer page-by-page instead of
/// materialising a whole column chunk.
pub const page_target_bytes = 1 << 20;

/// How a basalt column is stored in Parquet.
const Mapping = struct {
    phys: pq.PhysicalType,
    converted: ?i32 = null,
    precision: ?i32 = null,
    scale: ?i32 = null,
    /// REQUIRED columns carry no definition levels at all, which is both smaller
    /// and truer to the source schema than marking everything OPTIONAL.
    optional: bool = true,
};

const conv_utf8 = 0;
const conv_decimal = 5;
const conv_date = 6;
const conv_time_micros = 8;
const conv_timestamp_micros = 10;

fn mapType(t: types.Type) Error!Mapping {
    return switch (t.kind) {
        .bool => .{ .phys = .boolean },
        .int => .{ .phys = .int64 },
        .float => .{ .phys = .double },
        .string => .{ .phys = .byte_array, .converted = conv_utf8 },
        .bytes => .{ .phys = .byte_array },
        .date => .{ .phys = .int32, .converted = conv_date },
        .time => .{ .phys = .int64, .converted = conv_time_micros },
        .timestamp => .{ .phys = .int64, .converted = conv_timestamp_micros },
        // DECIMAL as INT64 covers precision up to 18, which is what basalt's
        // own decimal handling produces in practice.
        .decimal => blk: {
            const p: i32 = @min(18, @as(i32, t.precision));
            // scale > precision is not a representable DECIMAL — Spark and Arrow
            // both reject the schema — and clamping precision to 18 is what
            // produces it from e.g. `numeric(30,20)`.
            if (@as(i32, t.scale) > p) return Error.UnsupportedParquetDecimal;
            break :blk .{ .phys = .int64, .converted = conv_decimal, .precision = p, .scale = t.scale };
        },
        .array, .@"struct" => Error.UnsupportedParquetWrite,
    };
}

/// Distinct values a column may hold before dictionary encoding is abandoned.
/// Past this the dictionary stops paying for itself and the indices get wide.
pub const dict_max_entries = 1 << 15;

/// A page whose rows are complete, waiting to be written at row-group flush.
/// Pages of one chunk must land contiguously in the file, so they cannot be
/// emitted as they fill — they are held until the group is flushed.
const PendingPage = struct {
    defs: []const u8,
    values: []const u8,
    rows: usize,
};

/// Per-column accumulation for the row group being built.
const ColBuf = struct {
    values: List(u8),
    pages: List(PendingPage),
    /// One entry per row: 1 present, 0 null. Packed at flush time.
    defs: List(u8),
    /// BOOLEAN values are bit-packed, so they need their own accumulator.
    bit_buf: u8 = 0,
    bit_n: u3 = 0,
    /// Column statistics for this row group. Readers use these to skip whole
    /// row groups without decoding them, so emitting them is what makes a
    /// basalt-written file efficient for *other* engines.
    nulls: i64 = 0,
    min: ?Value = null,
    max: ?Value = null,
    /// Dictionary state for BYTE_ARRAY columns. Strings repeat far more often
    /// than they do not, and PLAIN stores every copy in full; a dictionary page
    /// plus RLE indices is what closes the size gap with other writers.
    dict: std.StringHashMap(u32),
    dict_order: List([]const u8),
    /// One index per non-null row, or abandoned once the column proves to have
    /// too many distinct values.
    dict_idx: List(u32),
    dict_ok: bool = false,

    fn init(a: std.mem.Allocator) ColBuf {
        return .{
            .values = List(u8).init(a),
            .pages = List(PendingPage).init(a),
            .defs = List(u8).init(a),
            .dict = std.StringHashMap(u32).init(a),
            .dict_order = List([]const u8).init(a),
            .dict_idx = List(u32).init(a),
        };
    }

    /// Seals the in-progress page. Definition levels are packed now, while the
    /// row set is known.
    fn sealPage(self: *ColBuf, arena: std.mem.Allocator, optional: bool) !void {
        if (self.defs.items.len == 0) return;
        try self.flushBits();
        const defs: []const u8 = if (optional)
            try packLevels(arena, self.defs.items)
        else
            &.{};
        try self.pages.append(.{
            .defs = defs,
            .values = try arena.dupe(u8, self.values.items),
            .rows = self.defs.items.len,
        });
        self.values.clearRetainingCapacity();
        self.defs.clearRetainingCapacity();
    }

    fn reset(self: *ColBuf) void {
        self.values.clearRetainingCapacity();
        self.pages.clearRetainingCapacity();
        self.defs.clearRetainingCapacity();
        self.bit_buf = 0;
        self.bit_n = 0;
        self.nulls = 0;
        self.min = null;
        self.max = null;
        self.dict.clearRetainingCapacity();
        self.dict_order.clearRetainingCapacity();
        self.dict_idx.clearRetainingCapacity();
        self.dict_ok = false;
    }

    /// Records a byte-array value against the dictionary. Returns false once the
    /// column has too many distinct values to be worth encoding this way.
    fn dictPut(self: *ColBuf, arena: std.mem.Allocator, v: []const u8) !bool {
        if (!self.dict_ok) return false;
        if (self.dict.get(v)) |ix| {
            try self.dict_idx.append(ix);
            return true;
        }
        if (self.dict.count() >= dict_max_entries) {
            self.dict_ok = false;
            return false;
        }
        const owned = try arena.dupe(u8, v);
        const ix: u32 = @intCast(self.dict_order.items.len);
        try self.dict.put(owned, ix);
        try self.dict_order.append(owned);
        try self.dict_idx.append(ix);
        return true;
    }

    /// Statistics are per row group, so they track only values since the last
    /// flush.
    ///
    /// String and byte values must be copied: the incoming `Value` borrows from
    /// the *batch* arena, which is recycled long before the row group flushes.
    /// Keeping the borrowed slice reads freed memory at flush time.
    fn observe(self: *ColBuf, arena: std.mem.Allocator, v: Value) !void {
        if (self.min == null or order(v, self.min.?) == .lt) self.min = try own(arena, v);
        if (self.max == null or order(v, self.max.?) == .gt) self.max = try own(arena, v);
    }

    fn pushBit(self: *ColBuf, b: bool) !void {
        if (b) self.bit_buf |= @as(u8, 1) << self.bit_n;
        if (self.bit_n == 7) {
            try self.values.append(self.bit_buf);
            self.bit_buf = 0;
            self.bit_n = 0;
        } else self.bit_n += 1;
    }

    fn flushBits(self: *ColBuf) !void {
        if (self.bit_n != 0) {
            try self.values.append(self.bit_buf);
            self.bit_buf = 0;
            self.bit_n = 0;
        }
    }
};

/// Copies any value that borrows memory, so it can outlive the batch it came from.
fn own(arena: std.mem.Allocator, v: Value) !Value {
    return switch (v) {
        .string => |x| .{ .string = try arena.dupe(u8, x) },
        .bytes => |x| .{ .bytes = try arena.dupe(u8, x) },
        else => v,
    };
}

const ChunkMeta = struct {
    offset: i64,
    /// Offset of the dictionary page, when the chunk is dictionary-encoded.
    dict_offset: i64 = 0,
    dict: bool = false,
    num_values: i64,
    uncompressed: i64,
    compressed: i64,
    nulls: i64 = 0,
    min: ?Value = null,
    max: ?Value = null,
};

/// Ordering used for min/max statistics. Only the shapes basalt writes need to
/// be comparable; mixed kinds cannot occur within one column.
fn order(a: Value, b: Value) std.math.Order {
    return switch (a) {
        .int => std.math.order(a.int, switch (b) {
            .int => |x| x,
            else => a.int,
        }),
        .float => std.math.order(a.float, switch (b) {
            .float => |x| x,
            else => a.float,
        }),
        .date => std.math.order(a.date, switch (b) {
            .date => |x| x,
            else => a.date,
        }),
        .time => std.math.order(a.time, switch (b) {
            .time => |x| x,
            else => a.time,
        }),
        .timestamp => std.math.order(a.timestamp, switch (b) {
            .timestamp => |x| x,
            else => a.timestamp,
        }),
        // Compare the NUMBER, not the mantissa: a source sends a per-value scale
        // (postgres NUMERIC dscale), so `0.5` (unscaled 5) and `0.10` (unscaled
        // 10) ranked backwards and this wrote min > max into the row group's
        // statistics. Readers then pruned the group and dropped real rows.
        .decimal => eval.compareValues(a, b) orelse .eq,
        .bool => std.math.order(@intFromBool(a.bool), switch (b) {
            .bool => |x| @intFromBool(x),
            else => @intFromBool(a.bool),
        }),
        .string, .bytes => blk: {
            const sa = if (a == .string) a.string else a.bytes;
            const sb = switch (b) {
                .string => |x| x,
                .bytes => |x| x,
                else => sa,
            };
            break :blk std.mem.order(u8, sa, sb);
        },
        .null => .eq,
    };
}

const RowGroupMeta = struct {
    chunks: []ChunkMeta,
    num_rows: i64,
    total_byte_size: i64,
};

/// Bytes buffered before the local-file destination drains. Pages arrive as
/// single large slices; this mostly amortizes the small footer/length/magic
/// emits at the end of a file.
const WRITE_BUF = 64 * 1024;

/// The IANA-registered media type for Parquet.
const parquet_content_type = "application/vnd.apache.parquet";

pub const Writer = struct {
    arena: std.mem.Allocator,
    backend: Backend,
    write_buf: [WRITE_BUF]u8 = undefined,
    fw: std.fs.File.Writer = undefined,
    schema: types.Schema,
    maps: []Mapping,
    compression: codec.Codec,
    cols: []ColBuf,
    rows: usize = 0,
    total_rows: i64 = 0,
    offset: i64 = 0,
    groups: List(RowGroupMeta),

    /// A local file, or a block blob staged over HTTP. Both expose a plain
    /// `*std.Io.Writer`, so page and footer emission below is identical either
    /// way — mirrors `csv.CsvWriter.Backend`.
    const Backend = union(enum) {
        file: std.fs.File,
        blob: struct { client: *std.http.Client, w: *azure.BlockBlobWriter },
    };

    fn dest(self: *Writer) *std.Io.Writer {
        return switch (self.backend) {
            .file => &self.fw.interface,
            .blob => |b| &b.w.interface,
        };
    }

    pub fn isPath(path: []const u8) bool {
        return std.mem.endsWith(u8, path, ".parquet");
    }

    /// `mode` only ever arrives as `.truncate`: the plan layer turns an `APPEND`
    /// onto a parquet target into an error before opening anything. The check is
    /// repeated here so a future caller that forgets it fails loudly instead of
    /// truncating the file it meant to extend.
    pub fn open(
        arena: std.mem.Allocator,
        path: []const u8,
        schema: types.Schema,
        compression: codec.Codec,
        mode: driver.FileMode,
    ) !*Writer {
        if (mode == .append) return Error.AppendNotSupported;
        if (!codec.canCompress(compression)) return codec.Error.UnsupportedCodec;
        const maps = try arena.alloc(Mapping, schema.fields.len);
        for (schema.fields, maps) |f, *m| {
        m.* = try mapType(f.ty);
        m.optional = f.ty.nullable;
    }

        const cols = try arena.alloc(ColBuf, schema.fields.len);
        for (cols, maps) |*c, m| {
            c.* = ColBuf.init(arena);
            // only variable-length values gain from a dictionary; fixed-width
            // types are already as small as the index would be
            c.dict_ok = m.phys == .byte_array;
        }

        const self = try arena.create(Writer);
        self.* = .{
            .arena = arena,
            .backend = undefined,
            .schema = schema,
            .maps = maps,
            .compression = compression,
            .cols = cols,
            .groups = List(RowGroupMeta).init(arena),
        };
        if (azure.isUrl(path)) {
            const client = try arena.create(std.http.Client);
            client.* = httpx.initClient(arena);
            const blob = try azure.parseUrl(arena, path, azure.endpointFromEnv(arena));
            self.backend = .{ .blob = .{
                .client = client,
                .w = try azure.BlockBlobWriter.init(arena, client, blob, parquet_content_type),
            } };
        } else {
            self.backend = .{ .file = try std.fs.cwd().createFile(path, .{}) };
            self.fw = self.backend.file.writer(&self.write_buf);
        }
        try self.emit(pq.magic);
        return self;
    }

    fn emit(self: *Writer, bytes: []const u8) !void {
        self.dest().writeAll(bytes) catch |e| return self.specific(e);
        self.offset += @intCast(bytes.len);
    }

    /// Recovers the error a blob destination actually hit. Staging a block runs
    /// under `std.Io.Writer`, whose error set is just `WriteFailed`, so the Azure
    /// code recorded at the point of failure is put back here — otherwise a 403
    /// and a missing container are the same word to the caller.
    fn specific(self: *Writer, e: anyerror) anyerror {
        if (e != error.WriteFailed) return e;
        return switch (self.backend) {
            .file => e,
            .blob => |b| b.w.last_status orelse e,
        };
    }

    pub fn writeBatch(self: *Writer, arena: std.mem.Allocator, batch: Batch) !void {
        _ = arena;
        for (0..batch.len) |r| {
            for (batch.columns, self.cols, self.maps) |col, *cb, m| {
                const v = col.getValue(r);
                try cb.defs.append(if (v == .null) 0 else 1);
                if (v == .null) {
                    cb.nulls += 1;
                    continue;
                }
                try cb.observe(self.arena, v);
                if (m.phys == .byte_array and cb.dict_ok) {
                    const sv: []const u8 = switch (v) {
                        .string => |x| x,
                        .bytes => |x| x,
                        else => "",
                    };
                    _ = try cb.dictPut(self.arena, sv);
                }
                try encodePlain(cb, m, v);
            }
            for (self.cols, self.maps) |*cb, m| {
                // a dictionary is per chunk, so its indices are emitted as one
                // page at flush; splitting only applies to PLAIN columns
                if (cb.dict_ok) continue;
                if (cb.values.items.len >= page_target_bytes) try cb.sealPage(self.arena, m.optional);
            }
            self.rows += 1;
            self.total_rows += 1;
            if (self.rows >= row_group_rows) try self.flushRowGroup();
        }
    }

    /// Writes one page per column, then records the row group's metadata.
    fn flushRowGroup(self: *Writer) !void {
        if (self.rows == 0) return;
        const chunks = try self.arena.alloc(ChunkMeta, self.cols.len);
        var group_bytes: i64 = 0;

        for (self.cols, chunks, self.maps) |*cb, *cm, m| {
            // A dictionary-encoded chunk is emitted whole: a dictionary page
            // followed by one data page of RLE indices.
            const use_dict = cb.dict_ok and cb.dict_order.items.len > 0 and
                cb.dict_order.items.len * 2 < cb.dict_idx.items.len;
            if (use_dict) {
                cm.* = try self.writeDictChunk(cb, m);
                group_bytes += cm.uncompressed;
                cb.reset();
                continue;
            }
            try cb.sealPage(self.arena, m.optional);

            const start = self.offset;
            var values: i64 = 0;
            var uncompressed: i64 = 0;
            var compressed: i64 = 0;

            for (cb.pages.items) |pgm| {
                // page body: [4-byte level length][RLE def levels][values],
                // with the level section omitted for REQUIRED columns
                var body = List(u8).init(self.arena);
                if (m.optional) {
                    var len4: [4]u8 = undefined;
                    std.mem.writeInt(u32, &len4, @intCast(pgm.defs.len), .little);
                    try body.appendSlice(&len4);
                    try body.appendSlice(pgm.defs);
                }
                try body.appendSlice(pgm.values);

                const raw = body.items;
                const packed_body = try codec.compress(self.arena, self.compression, raw);
                var hdr = List(u8).init(self.arena);
                try writePageHeader(&hdr, raw.len, packed_body.len, pgm.rows, std.hash.Crc32.hash(packed_body));
                try self.emit(hdr.items);
                try self.emit(packed_body);

                values += @intCast(pgm.rows);
                // Both totals count the page headers, per ColumnMetaData's
                // "including the headers". A reader that bounds the chunk by
                // total_compressed_size stops one header short of the last page
                // otherwise — a truncated-page error in Arrow-based readers, and
                // silently fine in ones that just walk headers to the row count.
                uncompressed += @intCast(hdr.items.len + raw.len);
                compressed += @intCast(hdr.items.len + packed_body.len);
            }

            cm.* = .{
                .offset = start,
                .num_values = values,
                .uncompressed = uncompressed,
                .compressed = compressed,
                .nulls = cb.nulls,
                .min = cb.min,
                .max = cb.max,
            };
            group_bytes += uncompressed;
            cb.reset();
        }

        try self.groups.append(.{
            .chunks = chunks,
            .num_rows = @intCast(self.rows),
            .total_byte_size = group_bytes,
        });
        self.rows = 0;
    }

    /// Writes a dictionary page followed by an RLE_DICTIONARY data page.
    fn writeDictChunk(self: *Writer, cb: *ColBuf, m: Mapping) !ChunkMeta {
        const start = self.offset;
        var uncompressed: i64 = 0;
        var compressed: i64 = 0;

        // dictionary page: the distinct values, PLAIN-encoded in index order
        var dict_body = List(u8).init(self.arena);
        for (cb.dict_order.items) |v| {
            var len4: [4]u8 = undefined;
            std.mem.writeInt(u32, &len4, @intCast(v.len), .little);
            try dict_body.appendSlice(&len4);
            try dict_body.appendSlice(v);
        }
        const dict_packed = try codec.compress(self.arena, self.compression, dict_body.items);
        var dhdr = List(u8).init(self.arena);
        try writeDictPageHeader(&dhdr, dict_body.items.len, dict_packed.len, cb.dict_order.items.len, std.hash.Crc32.hash(dict_packed));
        try self.emit(dhdr.items);
        try self.emit(dict_packed);
        // Headers included — see the note in flushRowGroup. A dictionary chunk
        // carries two of them, so it is short by twice as much when they are not.
        uncompressed += @intCast(dhdr.items.len + dict_body.items.len);
        compressed += @intCast(dhdr.items.len + dict_packed.len);

        const data_start = self.offset;

        // data page: [levels][bit width][RLE indices]
        var body = List(u8).init(self.arena);
        if (m.optional) {
            const levels = try packLevels(self.arena, cb.defs.items);
            var len4: [4]u8 = undefined;
            std.mem.writeInt(u32, &len4, @intCast(levels.len), .little);
            try body.appendSlice(&len4);
            try body.appendSlice(levels);
        }
        const width = indexWidth(cb.dict_order.items.len);
        try body.append(width);
        try packRleIndices(&body, cb.dict_idx.items, width);

        const packed_body = try codec.compress(self.arena, self.compression, body.items);
        var hdr = List(u8).init(self.arena);
        try writeDictDataPageHeader(&hdr, body.items.len, packed_body.len, cb.defs.items.len, std.hash.Crc32.hash(packed_body));
        try self.emit(hdr.items);
        try self.emit(packed_body);
        uncompressed += @intCast(hdr.items.len + body.items.len);
        compressed += @intCast(hdr.items.len + packed_body.len);

        return .{
            .offset = data_start,
            .dict_offset = start,
            .num_values = @intCast(cb.defs.items.len),
            .uncompressed = uncompressed,
            .compressed = compressed,
            .nulls = cb.nulls,
            .min = cb.min,
            .max = cb.max,
            .dict = true,
        };
    }

    pub fn close(self: *Writer) !void {
        try self.flushRowGroup();

        var footer = List(u8).init(self.arena);
        try self.writeFileMetaData(&footer);
        try self.emit(footer.items);

        var len4: [4]u8 = undefined;
        std.mem.writeInt(u32, &len4, @intCast(footer.items.len), .little);
        try self.emit(&len4);
        try self.emit(pq.magic);

        switch (self.backend) {
            .file => |f| {
                self.fw.interface.flush() catch |e| {
                    f.close();
                    return e;
                };
                f.close();
            },
            // Committing the block list is what publishes the blob, and it happens
            // only once the footer is written — so a reader never observes a
            // Parquet object without one. Atomic publication, for free.
            .blob => |b| b.w.finish() catch |e| return self.specific(e),
        }
    }

    /// A partial Parquet file has no footer and is unreadable, which is the
    /// correct outcome for an aborted run — there is nothing to roll back. A blob
    /// is stronger: skipping the block-list commit means the object never appears
    /// at all, and Azure discards the staged blocks after a week.
    pub fn abort(self: *Writer) void {
        switch (self.backend) {
            .file => |f| f.close(),
            .blob => {},
        }
    }

    fn writeFileMetaData(self: *Writer, out: *List(u8)) !void {
        var w = thrift.Writer.init(out);
        try w.structBegin();
        try w.writeI32(1, 1); // version
        // schema: a root group followed by one leaf per column, depth-first
        try w.listBegin(2, .@"struct", self.schema.fields.len + 1);
        try writeSchemaRoot(&w, self.schema.fields.len);
        for (self.schema.fields, self.maps) |f, m| try writeSchemaLeaf(&w, f.name, m);

        try w.writeI64(3, self.total_rows);
        try w.listBegin(4, .@"struct", self.groups.items.len);
        for (self.groups.items) |g| try self.writeRowGroup(&w, g);
        try w.writeBinary(6, "basalt");
        try w.structEnd();
    }

    fn writeRowGroup(self: *Writer, w: *thrift.Writer, g: RowGroupMeta) !void {
        try w.structBegin();
        try w.listBegin(1, .@"struct", g.chunks.len);
        for (g.chunks, self.schema.fields, self.maps) |c, f, m| {
            try w.structBegin();
            try w.writeI64(2, c.offset); // file_offset
            try w.fieldBegin(.@"struct", 3); // meta_data
            try w.structBegin();
            try w.writeI32(1, @intFromEnum(m.phys));
            try w.listBegin(2, .i32, 1);
            try w.writeZigZag(@intFromEnum(if (c.dict) pq.Encoding.rle_dictionary else pq.Encoding.plain));
            try w.listBegin(3, .binary, 1);
            try w.writeVarint(f.name.len);
            try w.out.appendSlice(f.name);
            try w.writeI32(4, @intFromEnum(self.compression));
            try w.writeI64(5, c.num_values);
            try w.writeI64(6, c.uncompressed);
            try w.writeI64(7, c.compressed);
            try w.writeI64(9, c.offset); // data_page_offset
            if (c.dict) try w.writeI64(11, c.dict_offset); // dictionary_page_offset
            try writeStatistics(w, self.arena, m, c);
            try w.structEnd();
            try w.structEnd();
        }
        try w.writeI64(2, g.total_byte_size);
        try w.writeI64(3, g.num_rows);
        try w.structEnd();
    }

    pub fn sink(self: *Writer) driver.Sink {
        return .{ .ptr = self, .vtable = &sink_vtable };
    }
};

/// `Statistics` (ColumnMetaData field 12). Writes the modern `min_value` and
/// `max_value` fields plus `null_count`; the legacy `min`/`max` are deliberately
/// omitted, since their signedness rules for byte arrays were never consistent
/// and readers prefer the newer pair.
fn writeStatistics(w: *thrift.Writer, arena: std.mem.Allocator, m: Mapping, c: ChunkMeta) !void {
    try w.fieldBegin(.@"struct", 12);
    try w.structBegin();
    try w.writeI64(3, c.nulls); // null_count
    // parquet.thrift orders Statistics as max(1), min(2), ..., max_value(5),
    // min_value(6) — max comes *before* min in both pairs. Writing them the
    // intuitive way round silently inverts every reader's row-group filter.
    if (c.max) |mx| {
        if (try statBytes(arena, m, mx)) |b| try w.writeBinary(5, b); // max_value
    }
    if (c.min) |mn| {
        if (try statBytes(arena, m, mn)) |b| try w.writeBinary(6, b); // min_value
    }
    try w.structEnd();
}

/// Statistics values are PLAIN-encoded, but *without* the length prefix that a
/// byte-array page would carry — the thrift binary field already carries it.
fn statBytes(arena: std.mem.Allocator, m: Mapping, v: Value) !?[]const u8 {
    switch (m.phys) {
        .boolean => {
            const b = try arena.alloc(u8, 1);
            b[0] = if (v == .bool and v.bool) 1 else 0;
            return b;
        },
        .int32 => {
            const b = try arena.alloc(u8, 4);
            const x: i32 = switch (v) {
                .date => |d| d,
                .int => |i| @intCast(i),
                else => 0,
            };
            std.mem.writeInt(i32, b[0..4], x, .little);
            return b;
        },
        .int64 => {
            const b = try arena.alloc(u8, 8);
            const x: i64 = switch (v) {
                .int => |i| i,
                .time => |i| i,
                .timestamp => |i| i,
                .decimal => |d| try rescale(d, m.scale orelse 0),
                else => 0,
            };
            std.mem.writeInt(i64, b[0..8], x, .little);
            return b;
        },
        .double => {
            const b = try arena.alloc(u8, 8);
            const x: f64 = switch (v) {
                .float => |f| f,
                .int => |i| @floatFromInt(i),
                else => 0,
            };
            std.mem.writeInt(u64, b[0..8], @bitCast(x), .little);
            return b;
        },
        .byte_array => return switch (v) {
            .string => |x| x,
            .bytes => |x| x,
            else => null,
        },
        else => return null,
    }
}

fn writeSchemaRoot(w: *thrift.Writer, children: usize) !void {
    try w.structBegin();
    try w.writeBinary(4, "basalt_schema");
    try w.writeI32(5, @intCast(children));
    try w.structEnd();
}

fn writeSchemaLeaf(w: *thrift.Writer, name: []const u8, m: Mapping) !void {
    try w.structBegin();
    try w.writeI32(1, @intFromEnum(m.phys));
    try w.writeI32(3, @intFromEnum(if (m.optional) pq.Repetition.optional else pq.Repetition.required));
    try w.writeBinary(4, name);
    if (m.converted) |c| try w.writeI32(6, c);
    if (m.scale) |s| try w.writeI32(7, s);
    if (m.precision) |p| try w.writeI32(8, p);
    try writeLogicalType(w, m);
    try w.structEnd();
}

/// `LogicalType` (SchemaElement field 10), the modern annotation that
/// supersedes ConvertedType. It carries what ConvertedType cannot — a
/// timestamp's unit and whether it is UTC-adjusted — and newer readers prefer
/// it. Both are emitted so old and new readers agree.
fn writeLogicalType(w: *thrift.Writer, m: Mapping) !void {
    const c = m.converted orelse return;
    try w.fieldBegin(.@"struct", 10);
    try w.structBegin();
    switch (c) {
        conv_utf8 => try emptyVariant(w, 1), // STRING
        conv_decimal => {
            try w.fieldBegin(.@"struct", 5); // DECIMAL
            try w.structBegin();
            try w.writeI32(1, m.scale orelse 0);
            try w.writeI32(2, m.precision orelse 18);
            try w.structEnd();
        },
        conv_date => try emptyVariant(w, 6), // DATE
        conv_time_micros => try timeVariant(w, 7), // TIME
        conv_timestamp_micros => try timeVariant(w, 8), // TIMESTAMP
        else => {},
    }
    try w.structEnd();
}

fn emptyVariant(w: *thrift.Writer, id: i16) !void {
    try w.fieldBegin(.@"struct", id);
    try w.structBegin();
    try w.structEnd();
}

/// TIME and TIMESTAMP both carry `isAdjustedToUTC` and a unit.
///
/// `isAdjustedToUTC` is false: basalt has no timezone concept, so its
/// timestamps are wall-clock values. Claiming UTC would make readers treat them
/// as instants and re-render them in the viewer's zone, shifting every
/// displayed time.
fn timeVariant(w: *thrift.Writer, id: i16) !void {
    try w.fieldBegin(.@"struct", id);
    try w.structBegin();
    try w.writeBool(1, false); // isAdjustedToUTC
    try w.fieldBegin(.@"struct", 2); // TimeUnit
    try w.structBegin();
    try emptyVariant(w, 2); // MICROS
    try w.structEnd();
    try w.structEnd();
}

fn writePageHeader(
    out: *List(u8),
    uncompressed: usize,
    compressed: usize,
    values: usize,
    crc: u32,
) !void {
    var w = thrift.Writer.init(out);
    try w.structBegin();
    try w.writeI32(1, @intFromEnum(pq.PageType.data_page));
    try w.writeI32(2, @intCast(uncompressed));
    try w.writeI32(3, @intCast(compressed));
    // CRC32 of the compressed page data, so a reader can detect corruption
    try w.writeI32(4, @bitCast(crc));
    try w.fieldBegin(.@"struct", 5); // data_page_header
    try w.structBegin();
    try w.writeI32(1, @intCast(values));
    try w.writeI32(2, @intFromEnum(pq.Encoding.plain));
    try w.writeI32(3, @intFromEnum(pq.Encoding.rle)); // definition_level_encoding
    try w.writeI32(4, @intFromEnum(pq.Encoding.rle)); // repetition_level_encoding
    try w.structEnd();
    try w.structEnd();
}

/// Definition levels as an RLE/bit-packed hybrid. Always bit-packed groups of
/// eight at width 1: simple, and never worse than a byte per eight rows.
fn packLevels(arena: std.mem.Allocator, defs: []const u8) ![]u8 {
    var out = List(u8).init(arena);
    const groups = (defs.len + 7) / 8;
    // header varint: (groups << 1) | 1 selects the bit-packed form
    var h: u64 = (@as(u64, groups) << 1) | 1;
    while (true) {
        const b: u8 = @intCast(h & 0x7F);
        h >>= 7;
        try out.append(if (h != 0) b | 0x80 else b);
        if (h == 0) break;
    }
    var i: usize = 0;
    while (i < groups) : (i += 1) {
        var byte: u8 = 0;
        for (0..8) |k| {
            const idx = i * 8 + k;
            if (idx < defs.len and defs[idx] != 0) byte |= @as(u8, 1) << @intCast(k);
        }
        try out.append(byte);
    }
    return out.toOwnedSlice();
}

fn encodePlain(cb: *ColBuf, m: Mapping, v: Value) !void {
    switch (m.phys) {
        .boolean => try cb.pushBit(v == .bool and v.bool),
        .int32 => {
            const x: i32 = switch (v) {
                .date => |d| d,
                .int => |i| @intCast(i),
                else => 0,
            };
            var b: [4]u8 = undefined;
            std.mem.writeInt(i32, &b, x, .little);
            try cb.values.appendSlice(&b);
        },
        .int64 => {
            const x: i64 = switch (v) {
                .int => |i| i,
                .time => |i| i,
                .timestamp => |i| i,
                .decimal => |d| try rescale(d, m.scale orelse 0),
                else => 0,
            };
            var b: [8]u8 = undefined;
            std.mem.writeInt(i64, &b, x, .little);
            try cb.values.appendSlice(&b);
        },
        .double => {
            const x: f64 = switch (v) {
                .float => |f| f,
                .int => |i| @floatFromInt(i),
                else => 0,
            };
            var b: [8]u8 = undefined;
            std.mem.writeInt(u64, &b, @bitCast(x), .little);
            try cb.values.appendSlice(&b);
        },
        .byte_array => {
            const s: []const u8 = switch (v) {
                .string => |x| x,
                .bytes => |x| x,
                else => "",
            };
            var b: [4]u8 = undefined;
            std.mem.writeInt(u32, &b, @intCast(s.len), .little);
            try cb.values.appendSlice(&b);
            try cb.values.appendSlice(s);
        },
        else => return Error.UnsupportedParquetWrite,
    }
}

/// Decimal values may carry a scale different from the column's; Parquet stores
/// the unscaled integer against the schema's scale, so it has to be restated.
/// A value too wide for INT64 is an error, not a saturated one: clamping turned
/// `12.5` in a `numeric(38,18)` column into `9.223372036854775807`.
fn rescale(d: valuemod.Decimal, want: i32) Error!i64 {
    var unscaled: i128 = d.unscaled;
    var have: i32 = d.scale;
    while (have < want) : (have += 1)
        unscaled = std.math.mul(i128, unscaled, 10) catch return Error.UnsupportedParquetDecimal;
    while (have > want) : (have -= 1) unscaled = @divTrunc(unscaled, 10);
    return std.math.cast(i64, unscaled) orelse Error.UnsupportedParquetDecimal;
}

const sink_vtable = driver.Sink.VTable{
    .writeBatch = sinkWrite,
    .close = sinkClose,
    .abort = sinkAbort,
};
fn sinkWrite(p: *anyopaque, arena: std.mem.Allocator, b: Batch) anyerror!void {
    return @as(*Writer, @ptrCast(@alignCast(p))).writeBatch(arena, b);
}
fn sinkClose(p: *anyopaque) anyerror!void {
    return @as(*Writer, @ptrCast(@alignCast(p))).close();
}
fn sinkAbort(p: *anyopaque) void {
    @as(*Writer, @ptrCast(@alignCast(p))).abort();
}

// --- tests -------------------------------------------------------------------

const testing = std.testing;
const pqdecode = @import("pqdecode.zig");
const column = @import("../exec/column.zig");

test "basalt types map onto Parquet physical and converted types" {
    try testing.expectEqual(pq.PhysicalType.boolean, (try mapType(types.Type.init(.bool))).phys);
    try testing.expectEqual(pq.PhysicalType.int64, (try mapType(types.Type.init(.int))).phys);
    try testing.expectEqual(pq.PhysicalType.double, (try mapType(types.Type.init(.float))).phys);
    try testing.expectEqual(pq.PhysicalType.byte_array, (try mapType(types.Type.init(.string))).phys);
    try testing.expectEqual(@as(?i32, conv_utf8), (try mapType(types.Type.init(.string))).converted);
    try testing.expectEqual(pq.PhysicalType.int32, (try mapType(types.Type.init(.date))).phys);
    try testing.expectEqual(@as(?i32, conv_date), (try mapType(types.Type.init(.date))).converted);
    try testing.expectEqual(@as(?i32, conv_timestamp_micros), (try mapType(types.Type.init(.timestamp))).converted);

    const dec = try mapType(types.Type.decimal(10, 2));
    try testing.expectEqual(pq.PhysicalType.int64, dec.phys);
    try testing.expectEqual(@as(?i32, 2), dec.scale);

    // nested types have no representation and must be refused, not guessed
    try testing.expectError(Error.UnsupportedParquetWrite, mapType(types.Type.init(.array)));
}

test "definition levels pack LSB-first into bit-packed groups of eight" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    // 8 rows, present except rows 1 and 6 -> 0b10111101
    const packed_levels = try packLevels(a, &[_]u8{ 1, 0, 1, 1, 1, 1, 0, 1 });
    try testing.expectEqual(@as(usize, 2), packed_levels.len); // header + one group
    try testing.expectEqual(@as(u8, 0x03), packed_levels[0]); // (1 groups << 1) | 1
    try testing.expectEqual(@as(u8, 0b10111101), packed_levels[1]);

    // the decoder must agree with what we wrote
    const got = try pqdecode.decodeRleHybrid(a, packed_levels, 1, 8);
    try testing.expectEqualSlices(u32, &.{ 1, 0, 1, 1, 1, 1, 0, 1 }, got);
}

test "decimal rescaling restates the unscaled value against the column scale" {
    try testing.expectEqual(@as(i64, 1550), try rescale(.{ .unscaled = 155, .scale = 1 }, 2));
    try testing.expectEqual(@as(i64, 155), try rescale(.{ .unscaled = 155, .scale = 2 }, 2));
    // reducing scale truncates, matching the direction Parquet writers take
    try testing.expectEqual(@as(i64, 15), try rescale(.{ .unscaled = 155, .scale = 2 }, 1));
}

test "decimal rescaling fails loudly instead of saturating to i64" {
    // `12.5` in a numeric(38,18) column: 125 at scale 1 restated to scale 18 is
    // 1.25e19, past i64. Clamping wrote 9.223372036854775807 into the file.
    try testing.expectError(Error.UnsupportedParquetDecimal, rescale(.{ .unscaled = 125, .scale = 1 }, 18));
    try testing.expectError(Error.UnsupportedParquetDecimal, rescale(.{ .unscaled = -125, .scale = 1 }, 18));
    // The intermediate must not overflow i128 either.
    try testing.expectError(Error.UnsupportedParquetDecimal, rescale(.{ .unscaled = std.math.maxInt(i64), .scale = 0 }, 30));
    // The widest value INT64 does hold still goes through.
    try testing.expectEqual(@as(i64, std.math.maxInt(i64)), try rescale(.{ .unscaled = std.math.maxInt(i64), .scale = 0 }, 0));
}

test "mapType rejects a decimal whose scale exceeds the stored precision" {
    // numeric(30,20) clamps to precision 18, and DECIMAL(18,20) is not a schema
    // any reader accepts.
    try testing.expectError(Error.UnsupportedParquetDecimal, mapType(types.Type.decimal(30, 20)));
    try testing.expectError(Error.UnsupportedParquetDecimal, mapType(types.Type.decimal(10, 12)));
    const ok = try mapType(types.Type.decimal(38, 6));
    try testing.expectEqual(@as(?i32, 18), ok.precision);
    try testing.expectEqual(@as(?i32, 6), ok.scale);
}

// Writes a file, reads it back with our own reader, and compares. Exercises
// the writer and reader against each other end to end.
test "written files read back with the values and nulls intact" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realpathAlloc(a, ".");
    const path = try std.fs.path.join(a, &.{ dir, "out.parquet" });

    const schema = types.Schema{ .fields = &.{
        .{ .name = "id", .ty = types.Type.init(.int).asNullable() },
        .{ .name = "name", .ty = types.Type.init(.string).asNullable() },
        .{ .name = "amt", .ty = types.Type.init(.float).asNullable() },
        .{ .name = "ok", .ty = types.Type.init(.bool).asNullable() },
    } };

    var w = try Writer.open(a, path, schema, .snappy, .truncate);
    // three rows, with a null in every column position at least once
    var ids = try column.Builder.initCapacity(a, schema.fields[0].ty, 3);
    var names = try column.Builder.initCapacity(a, schema.fields[1].ty, 3);
    var amts = try column.Builder.initCapacity(a, schema.fields[2].ty, 3);
    var oks = try column.Builder.initCapacity(a, schema.fields[3].ty, 3);
    try ids.append(.{ .int = 1 });
    try ids.append(.null);
    try ids.append(.{ .int = 3 });
    try names.append(.{ .string = "alpha" });
    try names.append(.{ .string = "beta" });
    try names.append(.null);
    try amts.append(.{ .float = 1.5 });
    try amts.append(.{ .float = -2.25 });
    try amts.append(.{ .float = 0 });
    try oks.append(.{ .bool = true });
    try oks.append(.{ .bool = false });
    try oks.append(.{ .bool = true });

    var cols = [_]column.Column{ try ids.finish(), try names.finish(), try amts.finish(), try oks.finish() };
    try w.writeBatch(a, .{ .schema = &schema, .columns = &cols, .len = 3 });
    try w.close();

    const r = try pqdecode.Reader.open(a, path);
    try testing.expectEqual(@as(usize, 4), r.schema.fields.len);
    try testing.expectEqualStrings("id", r.schema.fields[0].name);
    try testing.expectEqualStrings("ok", r.schema.fields[3].name);

    const b = (try r.next(a)).?;
    try testing.expectEqual(@as(usize, 3), b.len);
    try testing.expectEqual(@as(i64, 1), b.columns[0].getValue(0).int);
    try testing.expect(b.columns[0].getValue(1).isNull());
    try testing.expectEqual(@as(i64, 3), b.columns[0].getValue(2).int);
    try testing.expectEqualStrings("alpha", b.columns[1].getValue(0).string);
    try testing.expect(b.columns[1].getValue(2).isNull());
    try testing.expectEqual(@as(f64, -2.25), b.columns[2].getValue(1).float);
    try testing.expectEqual(true, b.columns[3].getValue(0).bool);
    try testing.expectEqual(false, b.columns[3].getValue(1).bool);
    try testing.expect((try r.next(a)) == null); // one row group only
}

test "a codec without an encoder is refused at open, before any bytes are written" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realpathAlloc(a, ".");
    const path = try std.fs.path.join(a, &.{ dir, "x.parquet" });
    const schema = types.Schema{ .fields = &.{.{ .name = "a", .ty = types.Type.init(.int) }} };
    try testing.expectError(codec.Error.UnsupportedCodec, Writer.open(a, path, schema, .zstd, .truncate));
}

test "statistics record min, max and null count per row group" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realpathAlloc(a, ".");
    const path = try std.fs.path.join(a, &.{ dir, "s.parquet" });

    const schema = types.Schema{ .fields = &.{
        .{ .name = "n", .ty = types.Type.init(.int).asNullable() },
        .{ .name = "s", .ty = types.Type.init(.string).asNullable() },
    } };
    var w = try Writer.open(a, path, schema, .snappy, .truncate);
    var ns = try column.Builder.initCapacity(a, schema.fields[0].ty, 4);
    var ss = try column.Builder.initCapacity(a, schema.fields[1].ty, 4);
    try ns.append(.{ .int = 5 });
    try ns.append(.null);
    try ns.append(.{ .int = -3 });
    try ns.append(.{ .int = 11 });
    try ss.append(.{ .string = "pear" });
    try ss.append(.{ .string = "apple" });
    try ss.append(.null);
    try ss.append(.{ .string = "zebra" });
    var cols = [_]column.Column{ try ns.finish(), try ss.finish() };
    try w.writeBatch(a, .{ .schema = &schema, .columns = &cols, .len = 4 });
    try w.close();

    // read the footer back and check the statistics landed in the right fields
    const bytes = try std.fs.cwd().readFileAlloc(a, path, 1 << 20);
    const md = try pq.parseFile(a, bytes);
    const stats = try readStats(a, bytes, md);
    try testing.expectEqual(@as(i64, 1), stats[0].nulls);
    try testing.expectEqual(@as(i64, -3), std.mem.readInt(i64, stats[0].min[0..8], .little));
    try testing.expectEqual(@as(i64, 11), std.mem.readInt(i64, stats[0].max[0..8], .little));
    try testing.expectEqual(@as(i64, 1), stats[1].nulls);
    try testing.expectEqualStrings("apple", stats[1].min);
    try testing.expectEqualStrings("zebra", stats[1].max);
}

const Stat = struct { nulls: i64, min: []const u8, max: []const u8 };

/// Minimal Statistics reader, used by the test to prove the writer put max_value
/// and min_value in the fields readers actually look at.
fn readStats(arena: std.mem.Allocator, bytes: []const u8, md: pq.FileMetaData) ![]Stat {
    const r = try pq.footerRange(bytes.len, bytes);
    var th = thrift.Reader.init(bytes[r.offset..][0..r.len]);
    var out = std.array_list.Managed(Stat).init(arena);
    _ = md;

    try th.structBegin();
    while (true) {
        const f = try th.readField();
        if (f.ty == .stop) break;
        if (f.id != 4) {
            try th.skip(f.ty);
            continue;
        }
        const groups = try th.readListHeader();
        for (0..groups.size) |_| {
            try th.structBegin();
            while (true) {
                const gf = try th.readField();
                if (gf.ty == .stop) break;
                if (gf.id != 1) {
                    try th.skip(gf.ty);
                    continue;
                }
                const chunks = try th.readListHeader();
                for (0..chunks.size) |_| {
                    try th.structBegin();
                    while (true) {
                        const cf = try th.readField();
                        if (cf.ty == .stop) break;
                        if (cf.id != 3) {
                            try th.skip(cf.ty);
                            continue;
                        }
                        try th.structBegin();
                        var st = Stat{ .nulls = 0, .min = "", .max = "" };
                        while (true) {
                            const mf = try th.readField();
                            if (mf.ty == .stop) break;
                            if (mf.id != 12) {
                                try th.skip(mf.ty);
                                continue;
                            }
                            try th.structBegin();
                            while (true) {
                                const sf = try th.readField();
                                if (sf.ty == .stop) break;
                                switch (sf.id) {
                                    3 => st.nulls = try th.readZigZag(),
                                    5 => st.max = try th.readBinary(),
                                    6 => st.min = try th.readBinary(),
                                    else => try th.skip(sf.ty),
                                }
                            }
                            try th.structEnd();
                        }
                        try th.structEnd();
                        try out.append(st);
                    }
                    try th.structEnd();
                }
            }
            try th.structEnd();
        }
        break;
    }
    return out.toOwnedSlice();
}

/// Bits needed to index a dictionary of `n` entries.
fn indexWidth(n: usize) u8 {
    if (n <= 1) return 0;
    return @intCast(32 - @clz(@as(u32, @intCast(n - 1))));
}

/// Dictionary indices as an RLE/bit-packed hybrid, always bit-packed in groups
/// of eight — the same shape the reader's `decodeRleHybrid` expects.
fn packRleIndices(out: *List(u8), idx: []const u32, width: u8) !void {
    if (width == 0 or idx.len == 0) return;
    const groups = (idx.len + 7) / 8;
    var h: u64 = (@as(u64, groups) << 1) | 1;
    while (true) {
        const b: u8 = @intCast(h & 0x7F);
        h >>= 7;
        try out.append(if (h != 0) b | 0x80 else b);
        if (h == 0) break;
    }
    var bit_buf: u32 = 0;
    var bit_n: u6 = 0;
    for (0..groups * 8) |i| {
        const v: u32 = if (i < idx.len) idx[i] else 0;
        bit_buf |= v << @intCast(bit_n);
        bit_n += @intCast(width);
        while (bit_n >= 8) {
            try out.append(@intCast(bit_buf & 0xFF));
            bit_buf >>= 8;
            bit_n -= 8;
        }
    }
    if (bit_n > 0) try out.append(@intCast(bit_buf & 0xFF));
}

fn writeDictPageHeader(out: *List(u8), uncompressed: usize, compressed: usize, values: usize, crc: u32) !void {
    var w = thrift.Writer.init(out);
    try w.structBegin();
    try w.writeI32(1, @intFromEnum(pq.PageType.dictionary_page));
    try w.writeI32(2, @intCast(uncompressed));
    try w.writeI32(3, @intCast(compressed));
    try w.writeI32(4, @bitCast(crc));
    try w.fieldBegin(.@"struct", 7); // dictionary_page_header
    try w.structBegin();
    try w.writeI32(1, @intCast(values));
    try w.writeI32(2, @intFromEnum(pq.Encoding.plain));
    try w.structEnd();
    try w.structEnd();
}

fn writeDictDataPageHeader(out: *List(u8), uncompressed: usize, compressed: usize, values: usize, crc: u32) !void {
    var w = thrift.Writer.init(out);
    try w.structBegin();
    try w.writeI32(1, @intFromEnum(pq.PageType.data_page));
    try w.writeI32(2, @intCast(uncompressed));
    try w.writeI32(3, @intCast(compressed));
    try w.writeI32(4, @bitCast(crc));
    try w.fieldBegin(.@"struct", 5);
    try w.structBegin();
    try w.writeI32(1, @intCast(values));
    try w.writeI32(2, @intFromEnum(pq.Encoding.rle_dictionary));
    try w.writeI32(3, @intFromEnum(pq.Encoding.rle));
    try w.writeI32(4, @intFromEnum(pq.Encoding.rle));
    try w.structEnd();
    try w.structEnd();
}

test "dictionary encoding round-trips low-cardinality strings" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realpathAlloc(a, ".");
    const path = try std.fs.path.join(a, &.{ dir, "d.parquet" });

    const schema = types.Schema{ .fields = &.{
        .{ .name = "cat", .ty = types.Type.init(.string).asNullable() },
    } };
    var w = try Writer.open(a, path, schema, .snappy, .truncate);

    // 300 rows over three distinct values: comfortably dictionary-worthy
    var b = try column.Builder.initCapacity(a, schema.fields[0].ty, 300);
    const vals = [_][]const u8{ "alpha", "beta", "gamma" };
    for (0..300) |i| {
        if (i % 37 == 0) try b.append(.null) else try b.append(.{ .string = vals[i % 3] });
    }
    var cols = [_]column.Column{try b.finish()};
    try w.writeBatch(a, .{ .schema = &schema, .columns = &cols, .len = 300 });
    try w.close();

    const r = try pqdecode.Reader.open(a, path);
    const back = (try r.next(a)).?;
    try testing.expectEqual(@as(usize, 300), back.len);
    for (0..300) |i| {
        if (i % 37 == 0) {
            try testing.expect(back.columns[0].getValue(i).isNull());
        } else {
            try testing.expectEqualStrings(vals[i % 3], back.columns[0].getValue(i).string);
        }
    }
}

test "index width covers the dictionary size" {
    try testing.expectEqual(@as(u8, 0), indexWidth(1));
    try testing.expectEqual(@as(u8, 1), indexWidth(2));
    try testing.expectEqual(@as(u8, 2), indexWidth(3));
    try testing.expectEqual(@as(u8, 2), indexWidth(4));
    try testing.expectEqual(@as(u8, 3), indexWidth(5));
    try testing.expectEqual(@as(u8, 8), indexWidth(256));
}

// The regression this pins: `open` used to call `std.fs.cwd().createFile` for
// every target, so an `az://` path became an attempt to create a file under a
// local directory named `az:` and failed with FileNotFound — the error looked
// like a storage problem and hid the real one for a whole debugging session.
// Whatever the outcome here without credentials in the environment, it must not
// come from the filesystem.
test "an az:// target routes to the blob writer, never the local filesystem" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const schema = types.Schema{ .fields = &.{
        .{ .name = "x", .ty = types.Type.init(.int).asNullable() },
    } };

    const w = Writer.open(a, "az://acct/ctr/bronze/t.parquet", schema, .snappy, .truncate) catch |e| {
        // No AZURE_STORAGE_KEY set is the expected outcome on a bare test box.
        try testing.expect(e != error.FileNotFound and e != error.NotDir);
        return;
    };
    try testing.expect(w.backend == .blob);
}

// A reader that bounds a column chunk by `total_compressed_size` — Arrow does,
// and so StarRocks does — must find the chunk's last page whole. It did not:
// both size totals omitted the page headers, leaving every chunk short by 29
// bytes per page (58 for a dictionary chunk, which has two), surfacing as
// "Page was smaller than expected". A lenient reader that just walks page
// headers to the row count never noticed, which is why a round trip through
// basalt's own reader stayed green.
test "chunk size totals account for page headers" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realpathAlloc(a, ".");
    const path = try std.fs.path.join(a, &.{ dir, "sizes.parquet" });

    // `name` repeats, so it takes the dictionary path (two headers per chunk);
    // `id` stays PLAIN (one). Both accounting sites are covered.
    const schema = types.Schema{ .fields = &.{
        .{ .name = "id", .ty = types.Type.init(.int).asNullable() },
        .{ .name = "name", .ty = types.Type.init(.string).asNullable() },
    } };

    var w = try Writer.open(a, path, schema, .snappy, .truncate);
    const rows = 500;
    var ids = try column.Builder.initCapacity(a, schema.fields[0].ty, rows);
    var names = try column.Builder.initCapacity(a, schema.fields[1].ty, rows);
    const vals = [_][]const u8{ "alpha", "beta", "gamma" };
    for (0..rows) |i| {
        try ids.append(.{ .int = @intCast(i) });
        try names.append(.{ .string = vals[i % vals.len] });
    }
    var cols = [_]column.Column{ try ids.finish(), try names.finish() };
    try w.writeBatch(a, .{ .schema = &schema, .columns = &cols, .len = rows });
    try w.close();

    const bytes = try std.fs.cwd().readFileAlloc(a, path, 1 << 24);
    const trailer = bytes[bytes.len - pq.trailer_len ..];
    const flen = std.mem.readInt(u32, trailer[0..4], .little);
    const footer_start = bytes.len - pq.trailer_len - flen;
    const md = try pq.parseFooter(a, bytes[footer_start..][0..flen]);

    // Chunks are written back to back, so the next one's start — or the footer,
    // for the last — is where this one truly ends.
    var starts = std.array_list.Managed(i64).init(a);
    for (md.row_groups) |g| for (g.columns) |c| try starts.append((c.meta.?).startOffset());
    std.mem.sort(i64, starts.items, {}, std.sort.asc(i64));

    for (md.row_groups) |g| for (g.columns) |c| {
        const meta = c.meta.?;
        const start = meta.startOffset();
        var end: i64 = @intCast(footer_start);
        for (starts.items) |s| if (s > start) {
            end = s;
            break;
        };
        try testing.expectEqual(end - start, meta.total_compressed_size);
    };
}
