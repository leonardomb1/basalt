//! Parquet writer: PLAIN-encoded pages (or a dictionary page plus RLE indices for
//! byte-array columns), one column chunk per column per row group.
//!
//! Nullable columns are written OPTIONAL with definition levels; REQUIRED columns
//! carry none. Rows accumulate until `group_rows`, then a row group is flushed.
//! That bounds memory to roughly one row group, the closest a Parquet writer gets
//! to streaming: a chunk's size and offset must be known before its metadata, and
//! the footer indexes every row group, so a file cannot be appended to.
//! Everything that matters only until the group is written (sealed pages,
//! compressed bodies, dictionary strings, statistics) lives in `scratch` and is
//! reset after each flush; on the plan arena a 10M-row write held 1.4 GB.
//! Statistics values are copied there because an incoming `Value` borrows from
//! the batch arena, which is recycled long before the group flushes.
//!
//! Pages of one chunk must land contiguously, so they are held until the flush.
//! A dictionary is per chunk and emitted whole; a column gives it up once it
//! exceeds `dict_max_entries`. Both chunk size totals include page headers, per
//! ColumnMetaData's "including the headers": without them a reader that bounds a
//! chunk by `total_compressed_size` (Arrow-based ones do) found the last page short.
//!
//! Output is strictly forward: every byte goes through `emit`, which tracks the
//! running offset and never seeks, so the destination is any `*std.Io.Writer`, a
//! local file or a staged object upload. Committing the upload happens only after
//! the footer, so a reader never sees a Parquet object without one; an aborted
//! run leaves either a footerless (unreadable) file or no object at all.
//!
//! Decimals take the physical type their precision calls for, as the spec
//! prescribes: INT32 to 9 digits, INT64 to 18, FIXED_LEN_BYTE_ARRAY to the 38-digit
//! i128 ceiling. Everything used to be INT64, so a `numeric(38,18)` could not hold
//! its own values. Scale above precision is refused, as Spark and Arrow do.
//! Timestamps are written with `isAdjustedToUTC` false: basalt has no timezone
//! concept, and claiming UTC would make readers shift every displayed time.
//!
//! In a parallel run each ordered unit is encoded to row groups on the lane that
//! read it (`UnitEnc`, `openEncoder`), and `appendEncoded` places them in order.

const std = @import("std");
const parquet = @import("parquet.zig");
const thrift = @import("thrift.zig");
const codec = @import("codec.zig");
const eval = @import("../exec/eval.zig");
const driver = @import("../connect/driver.zig");
const http_client = @import("../net/http_client.zig");
const objstore = @import("../store/objstore.zig");
const sftp = @import("../store/sftp.zig");
const smb = @import("../store/smb.zig");
const types = @import("../lang/types.zig");
const Batch = @import("../exec/batch.zig").Batch;
const column = @import("../exec/column.zig");
const Decimal = @import("../exec/value.zig").Decimal;
const Value = @import("../exec/value.zig").Value;

const List = std.array_list.Managed;

pub const Error = error{
    UnsupportedParquetWrite,
    AppendNotSupported,
    UnsupportedParquetDecimal,
} || std.mem.Allocator.Error || codec.Error;

pub const row_group_rows = 100_000;

pub const page_target_bytes = 1 << 20;

const Mapping = struct {
    phys: parquet.PhysicalType,
    converted: ?i32 = null,
    precision: ?i32 = null,
    scale: ?i32 = null,
    type_length: ?i32 = null,
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
        .decimal => blk: {
            const p: i32 = @max(1, @min(38, @as(i32, t.precision)));
            const s: i32 = @as(i32, t.scale);
            if (s > p) return Error.UnsupportedParquetDecimal;
            if (p <= 9) break :blk .{ .phys = .int32, .converted = conv_decimal, .precision = p, .scale = s };
            if (p <= 18) break :blk .{ .phys = .int64, .converted = conv_decimal, .precision = p, .scale = s };
            break :blk .{ .phys = .fixed_len_byte_array, .converted = conv_decimal, .precision = p, .scale = s, .type_length = flbaLen(p) };
        },
        .array, .@"struct" => Error.UnsupportedParquetWrite,
    };
}

/// Narrowest two's-complement byte width that holds every `p`-digit decimal;
/// 16 at p = 38.
fn flbaLen(p: i32) i32 {
    var limit: i128 = 1;
    var k: i32 = 0;
    while (k < p) : (k += 1) limit *= 10;
    var n: i32 = 1;
    while (n < 16) : (n += 1) {
        const span: i128 = @as(i128, 1) << @intCast(8 * n - 1);
        if (span >= limit) return n;
    }
    return 16;
}

pub const dict_max_entries = 1 << 15;

const PendingPage = struct {
    defs: []const u8,
    values: []const u8,
    rows: usize,
};

const ColBuf = struct {
    values: List(u8),
    pages: List(PendingPage),
    defs: List(u8),
    bit_buf: u8 = 0,
    bit_n: u3 = 0,
    nulls: i64 = 0,
    min: ?Value = null,
    max: ?Value = null,
    dict: std.StringHashMap(u32),
    dict_order: List([]const u8),
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

fn ownOpt(arena: std.mem.Allocator, v: ?Value) !?Value {
    return if (v) |x| try own(arena, x) else null;
}

fn own(arena: std.mem.Allocator, v: Value) !Value {
    return switch (v) {
        .string => |x| .{ .string = try arena.dupe(u8, x) },
        .bytes => |x| .{ .bytes = try arena.dupe(u8, x) },
        else => v,
    };
}

const ChunkMeta = struct {
    offset: i64,
    dict_offset: i64 = 0,
    dict: bool = false,
    num_values: i64,
    uncompressed: i64,
    compressed: i64,
    nulls: i64 = 0,
    min: ?Value = null,
    max: ?Value = null,
};

/// Ordering for min/max statistics. Decimals compare as numbers, not mantissas:
/// per-value scales once ranked `0.5` above `0.10`, wrote min > max, and readers pruned real rows.
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

const WRITE_BUF = 64 * 1024;

const parquet_content_type = "application/vnd.apache.parquet";

pub const Writer = struct {
    arena: std.mem.Allocator,
    scratch: std.heap.ArenaAllocator,
    scratch_live: bool = true,
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
    group_rows: usize = row_group_rows,

    const Backend = union(enum) {
        file: std.fs.File,
        object: objstore.Writer,
        memory: *std.Io.Writer.Allocating,
    };

    fn dest(self: *Writer) *std.Io.Writer {
        return switch (self.backend) {
            .file => &self.fw.interface,
            .object => |o| o.io,
            .memory => |m| &m.writer,
        };
    }

    pub fn isPath(path: []const u8) bool {
        return std.mem.endsWith(u8, path, ".parquet");
    }

    /// `.append` is refused here as well as in the plan layer, so a caller that
    /// forgets the check fails loudly instead of truncating the file. An object
    /// URL must never reach the filesystem: `az://` once became a local `az:` dir.
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
            c.dict_ok = m.phys == .byte_array;
        }

        const self = try arena.create(Writer);
        self.* = .{
            .arena = arena,
            .scratch = std.heap.ArenaAllocator.init(std.heap.page_allocator),
            .backend = undefined,
            .schema = schema,
            .maps = maps,
            .compression = compression,
            .cols = cols,
            .groups = List(RowGroupMeta).init(arena),
        };
        if (sftp.isUrl(path)) {
            self.backend = .{ .object = objstore.writer(try sftp.Upload.open(arena, path)) };
        } else if (smb.isUrl(path)) {
            self.backend = .{ .object = objstore.writer(try smb.Upload.open(arena, path)) };
        } else if (objstore.isUrl(path)) {
            const client = try arena.create(std.http.Client);
            client.* = http_client.initClient(arena);
            const obj = try objstore.parse(arena, path);
            self.backend = .{ .object = try obj.openWriter(arena, client, parquet_content_type) };
        } else {
            self.backend = .{ .file = try std.fs.cwd().createFile(path, .{}) };
            self.fw = self.backend.file.writer(&self.write_buf);
        }
        try self.emit(parquet.magic);
        return self;
    }

    fn emit(self: *Writer, bytes: []const u8) !void {
        self.dest().writeAll(bytes) catch |e| return self.specific(e);
        self.offset += @intCast(bytes.len);
    }

    /// Recovers the error an object destination actually hit (see
    /// `objstore.Writer.specific`); a file's error is already the real one.
    fn specific(self: *Writer, e: anyerror) anyerror {
        return switch (self.backend) {
            .file, .memory => e,
            .object => |o| o.specific(e),
        };
    }

    pub fn writeBatch(self: *Writer, arena: std.mem.Allocator, batch: Batch) !void {
        _ = arena;
        const sa = self.scratch.allocator();
        for (0..batch.len) |r| {
            for (batch.columns, self.cols, self.maps) |col, *cb, m| {
                const v = col.getValue(r);
                try cb.defs.append(if (v == .null) 0 else 1);
                if (v == .null) {
                    cb.nulls += 1;
                    continue;
                }
                try cb.observe(sa, v);
                if (m.phys == .byte_array and cb.dict_ok) {
                    const sv: []const u8 = switch (v) {
                        .string => |x| x,
                        .bytes => |x| x,
                        else => "",
                    };
                    _ = try cb.dictPut(sa, sv);
                }
                try encodePlain(cb, m, v);
            }
            for (self.cols, self.maps) |*cb, m| {
                if (cb.dict_ok) continue;
                if (cb.values.items.len >= page_target_bytes) try cb.sealPage(sa, m.optional);
            }
            self.rows += 1;
            self.total_rows += 1;
            if (self.rows >= self.group_rows) try self.flushRowGroup();
        }
    }

    fn flushRowGroup(self: *Writer) !void {
        if (self.rows == 0) return;
        const chunks = try self.arena.alloc(ChunkMeta, self.cols.len);
        const sa = self.scratch.allocator();
        var group_bytes: i64 = 0;

        for (self.cols, chunks, self.maps) |*cb, *cm, m| {
            const use_dict = cb.dict_ok and cb.dict_order.items.len > 0 and
                cb.dict_order.items.len * 2 < cb.dict_idx.items.len;
            if (use_dict) {
                cm.* = try self.writeDictChunk(cb, m);
                group_bytes += cm.uncompressed;
                cb.reset();
                continue;
            }
            try cb.sealPage(sa, m.optional);

            const start = self.offset;
            var values: i64 = 0;
            var uncompressed: i64 = 0;
            var compressed: i64 = 0;

            for (cb.pages.items) |pgm| {
                var body = List(u8).init(sa);
                if (m.optional) {
                    var len4: [4]u8 = undefined;
                    std.mem.writeInt(u32, &len4, @intCast(pgm.defs.len), .little);
                    try body.appendSlice(&len4);
                    try body.appendSlice(pgm.defs);
                }
                try body.appendSlice(pgm.values);

                const raw = body.items;
                const packed_body = try codec.compress(sa, self.compression, raw);
                var hdr = List(u8).init(sa);
                try writePageHeader(&hdr, raw.len, packed_body.len, pgm.rows, std.hash.Crc32.hash(packed_body));
                try self.emit(hdr.items);
                try self.emit(packed_body);

                values += @intCast(pgm.rows);
                uncompressed += @intCast(hdr.items.len + raw.len);
                compressed += @intCast(hdr.items.len + packed_body.len);
            }

            cm.* = .{
                .offset = start,
                .num_values = values,
                .uncompressed = uncompressed,
                .compressed = compressed,
                .nulls = cb.nulls,
                .min = try ownOpt(self.arena, cb.min),
                .max = try ownOpt(self.arena, cb.max),
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
        _ = self.scratch.reset(.retain_capacity);
    }

    fn writeDictChunk(self: *Writer, cb: *ColBuf, m: Mapping) !ChunkMeta {
        const sa = self.scratch.allocator();
        const start = self.offset;
        var uncompressed: i64 = 0;
        var compressed: i64 = 0;

        var dict_body = List(u8).init(sa);
        for (cb.dict_order.items) |v| {
            var len4: [4]u8 = undefined;
            std.mem.writeInt(u32, &len4, @intCast(v.len), .little);
            try dict_body.appendSlice(&len4);
            try dict_body.appendSlice(v);
        }
        const dict_packed = try codec.compress(sa, self.compression, dict_body.items);
        var dhdr = List(u8).init(sa);
        try writeDictPageHeader(&dhdr, dict_body.items.len, dict_packed.len, cb.dict_order.items.len, std.hash.Crc32.hash(dict_packed));
        try self.emit(dhdr.items);
        try self.emit(dict_packed);
        uncompressed += @intCast(dhdr.items.len + dict_body.items.len);
        compressed += @intCast(dhdr.items.len + dict_packed.len);

        const data_start = self.offset;

        var body = List(u8).init(sa);
        if (m.optional) {
            const levels = try packLevels(sa, cb.defs.items);
            var len4: [4]u8 = undefined;
            std.mem.writeInt(u32, &len4, @intCast(levels.len), .little);
            try body.appendSlice(&len4);
            try body.appendSlice(levels);
        }
        const width = indexWidth(cb.dict_order.items.len);
        try body.append(width);
        try packRleIndices(&body, cb.dict_idx.items, width);

        const packed_body = try codec.compress(sa, self.compression, body.items);
        var hdr = List(u8).init(sa);
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
            .min = try ownOpt(self.arena, cb.min),
            .max = try ownOpt(self.arena, cb.max),
            .dict = true,
        };
    }

    pub fn close(self: *Writer) !void {
        try self.flushRowGroup();
        self.freeScratch();

        var footer = List(u8).init(self.arena);
        try self.writeFileMetaData(&footer);
        try self.emit(footer.items);

        var len4: [4]u8 = undefined;
        std.mem.writeInt(u32, &len4, @intCast(footer.items.len), .little);
        try self.emit(&len4);
        try self.emit(parquet.magic);

        switch (self.backend) {
            .file => |f| {
                self.fw.interface.flush() catch |e| {
                    f.close();
                    return e;
                };
                f.close();
            },
            .object => |o| o.finish() catch |e| return self.specific(e),
            .memory => {},
        }
    }

    /// Once only: `abort` may follow a `close` that failed after freeing it.
    fn freeScratch(self: *Writer) void {
        if (!self.scratch_live) return;
        self.scratch.deinit();
        self.scratch_live = false;
    }

    pub fn abort(self: *Writer) void {
        self.freeScratch();
        switch (self.backend) {
            .file => |f| f.close(),
            .object => |o| o.abort(),
            .memory => {},
        }
    }

    fn writeFileMetaData(self: *Writer, out: *List(u8)) !void {
        var w = thrift.Writer.init(out);
        try w.structBegin();
        try w.writeI32(1, 1);
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
            try w.writeI64(2, c.offset);
            try w.fieldBegin(.@"struct", 3);
            try w.structBegin();
            try w.writeI32(1, @intFromEnum(m.phys));
            try w.listBegin(2, .i32, 1);
            try w.writeZigZag(@intFromEnum(if (c.dict) parquet.Encoding.rle_dictionary else parquet.Encoding.plain));
            try w.listBegin(3, .binary, 1);
            try w.writeVarint(f.name.len);
            try w.out.appendSlice(f.name);
            try w.writeI32(4, @intFromEnum(self.compression));
            try w.writeI64(5, c.num_values);
            try w.writeI64(6, c.uncompressed);
            try w.writeI64(7, c.compressed);
            try w.writeI64(9, c.offset);
            if (c.dict) try w.writeI64(11, c.dict_offset);
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

    fn openEncoder(self: *Writer, arena: std.mem.Allocator) !*Writer {
        const cols = try arena.alloc(ColBuf, self.cols.len);
        for (cols, self.maps) |*c, m| {
            c.* = ColBuf.init(arena);
            c.dict_ok = m.phys == .byte_array;
        }
        const mem = try arena.create(std.Io.Writer.Allocating);
        mem.* = std.Io.Writer.Allocating.init(arena);
        const e = try arena.create(Writer);
        e.* = .{
            .arena = arena,
            .scratch = std.heap.ArenaAllocator.init(std.heap.page_allocator),
            .backend = .{ .memory = mem },
            .schema = self.schema,
            .maps = self.maps,
            .compression = self.compression,
            .cols = cols,
            .groups = List(RowGroupMeta).init(arena),
            .group_rows = unit_group_rows,
        };
        return e;
    }

    /// Place an encoder's row groups after everything written so far. Rows this
    /// writer still buffers came first, so they are flushed as a group of their own.
    fn appendEncoded(self: *Writer, enc: *Writer) !void {
        try self.flushRowGroup();
        const base = self.offset;
        try self.emit(enc.backend.memory.written());
        for (enc.groups.items) |g| {
            const chunks = try self.arena.alloc(ChunkMeta, g.chunks.len);
            for (g.chunks, chunks) |c, *o| {
                o.* = c;
                o.offset += base;
                if (c.dict) o.dict_offset += base;
                o.min = try ownOpt(self.arena, c.min);
                o.max = try ownOpt(self.arena, c.max);
            }
            try self.groups.append(.{ .chunks = chunks, .num_rows = g.num_rows, .total_byte_size = g.total_byte_size });
            self.total_rows += g.num_rows;
        }
    }
};

const unit_group_rows = 4 * row_group_rows;

const unit_min_rows = row_group_rows / 2;

const UnitEnc = struct {
    parent: *Writer,
    arena: std.mem.Allocator,
    held: List(Batch),
    held_rows: usize = 0,
    enc: ?*Writer = null,

    fn write(self: *UnitEnc, b: Batch) !void {
        if (self.enc) |e| return e.writeBatch(self.arena, b);
        try self.held.append(try b.deepCopy(self.arena));
        self.held_rows += b.len;
        if (self.held_rows < unit_min_rows) return;
        const e = try self.parent.openEncoder(self.arena);
        self.enc = e;
        for (self.held.items) |hb| try e.writeBatch(self.arena, hb);
        self.held.clearRetainingCapacity();
    }

    fn seal(self: *UnitEnc) !void {
        const e = self.enc orelse return;
        defer e.freeScratch();
        try e.flushRowGroup();
    }

    fn commit(self: *UnitEnc) !void {
        if (self.enc) |e| return self.parent.appendEncoded(e);
        for (self.held.items) |b| try self.parent.writeBatch(self.arena, b);
    }

    fn discard(self: *UnitEnc) void {
        if (self.enc) |e| e.freeScratch();
    }

    const vtable = driver.UnitEncoder.VTable{
        .write = struct {
            fn f(p: *anyopaque, _: std.mem.Allocator, b: Batch) anyerror!void {
                return @as(*UnitEnc, @ptrCast(@alignCast(p))).write(b);
            }
        }.f,
        .seal = struct {
            fn f(p: *anyopaque) anyerror!void {
                return @as(*UnitEnc, @ptrCast(@alignCast(p))).seal();
            }
        }.f,
        .commit = struct {
            fn f(p: *anyopaque) anyerror!void {
                return @as(*UnitEnc, @ptrCast(@alignCast(p))).commit();
            }
        }.f,
        .discard = struct {
            fn f(p: *anyopaque) void {
                @as(*UnitEnc, @ptrCast(@alignCast(p))).discard();
            }
        }.f,
    };
};

/// `Statistics` (ColumnMetaData field 12): `min_value`/`max_value` and `null_count`;
/// legacy `min`/`max` are omitted. parquet.thrift puts max before min in both
/// pairs (fields 5 and 6); swapping them inverts every reader's row-group filter.
fn writeStatistics(w: *thrift.Writer, arena: std.mem.Allocator, m: Mapping, c: ChunkMeta) !void {
    try w.fieldBegin(.@"struct", 12);
    try w.structBegin();
    try w.writeI64(3, c.nulls);
    if (c.max) |mx| {
        if (try statBytes(arena, m, mx)) |b| try w.writeBinary(5, b);
    }
    if (c.min) |mn| {
        if (try statBytes(arena, m, mn)) |b| try w.writeBinary(6, b);
    }
    try w.structEnd();
}

/// PLAIN-encoded, but without the length prefix a byte-array page carries,
/// since the thrift binary field already has one.
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
                .decimal => |d| try rescaleTo(i32, d, m.scale orelse 0),
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
                .decimal => |d| try rescaleTo(i64, d, m.scale orelse 0),
                else => 0,
            };
            std.mem.writeInt(i64, b[0..8], x, .little);
            return b;
        },
        .fixed_len_byte_array => {
            const len: usize = @intCast(m.type_length orelse return null);
            const x: i128 = switch (v) {
                .decimal => |d| try rescale(d, m.scale orelse 0),
                .int => |i| i,
                else => return null,
            };
            const b = try arena.alloc(u8, len);
            try decimalBytes(b, x);
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
    if (m.type_length) |n| try w.writeI32(2, n);
    try w.writeI32(3, @intFromEnum(if (m.optional) parquet.Repetition.optional else parquet.Repetition.required));
    try w.writeBinary(4, name);
    if (m.converted) |c| try w.writeI32(6, c);
    if (m.scale) |s| try w.writeI32(7, s);
    if (m.precision) |p| try w.writeI32(8, p);
    try writeLogicalType(w, m);
    try w.structEnd();
}

/// `LogicalType` (SchemaElement field 10), emitted beside ConvertedType so old and
/// new readers agree; it alone carries a timestamp's unit and UTC flag.
fn writeLogicalType(w: *thrift.Writer, m: Mapping) !void {
    const c = m.converted orelse return;
    try w.fieldBegin(.@"struct", 10);
    try w.structBegin();
    switch (c) {
        conv_utf8 => try emptyVariant(w, 1),
        conv_decimal => {
            try w.fieldBegin(.@"struct", 5);
            try w.structBegin();
            try w.writeI32(1, m.scale orelse 0);
            try w.writeI32(2, m.precision orelse 18);
            try w.structEnd();
        },
        conv_date => try emptyVariant(w, 6),
        conv_time_micros => try timeVariant(w, 7),
        conv_timestamp_micros => try timeVariant(w, 8),
        else => {},
    }
    try w.structEnd();
}

fn emptyVariant(w: *thrift.Writer, id: i16) !void {
    try w.fieldBegin(.@"struct", id);
    try w.structBegin();
    try w.structEnd();
}

fn timeVariant(w: *thrift.Writer, id: i16) !void {
    try w.fieldBegin(.@"struct", id);
    try w.structBegin();
    try w.writeBool(1, false);
    try w.fieldBegin(.@"struct", 2);
    try w.structBegin();
    try emptyVariant(w, 2);
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
    try w.writeI32(1, @intFromEnum(parquet.PageType.data_page));
    try w.writeI32(2, @intCast(uncompressed));
    try w.writeI32(3, @intCast(compressed));
    try w.writeI32(4, @bitCast(crc));
    try w.fieldBegin(.@"struct", 5);
    try w.structBegin();
    try w.writeI32(1, @intCast(values));
    try w.writeI32(2, @intFromEnum(parquet.Encoding.plain));
    try w.writeI32(3, @intFromEnum(parquet.Encoding.rle));
    try w.writeI32(4, @intFromEnum(parquet.Encoding.rle));
    try w.structEnd();
    try w.structEnd();
}

/// Definition levels as an RLE/bit-packed hybrid, always bit-packed groups of
/// eight at width 1.
fn packLevels(arena: std.mem.Allocator, defs: []const u8) ![]u8 {
    var out = List(u8).init(arena);
    const groups = (defs.len + 7) / 8;
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
                .decimal => |d| try rescaleTo(i32, d, m.scale orelse 0),
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
                .decimal => |d| try rescaleTo(i64, d, m.scale orelse 0),
                else => 0,
            };
            var b: [8]u8 = undefined;
            std.mem.writeInt(i64, &b, x, .little);
            try cb.values.appendSlice(&b);
        },
        .fixed_len_byte_array => {
            const len: usize = @intCast(m.type_length orelse return Error.UnsupportedParquetDecimal);
            const x: i128 = switch (v) {
                .decimal => |d| try rescale(d, m.scale orelse 0),
                .int => |i| i,
                else => 0,
            };
            var b: [16]u8 = undefined;
            if (len > b.len) return Error.UnsupportedParquetDecimal;
            try decimalBytes(b[0..len], x);
            try cb.values.appendSlice(b[0..len]);
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

/// The unscaled integer restated to the schema's scale. Overflowing i128 is an
/// error: clamping once turned `12.5` in a `numeric(38,18)` into `9.223372036854775807`.
fn rescale(d: Decimal, want: i32) Error!i128 {
    var unscaled: i128 = d.unscaled;
    var have: i32 = d.scale;
    while (have < want) : (have += 1)
        unscaled = std.math.mul(i128, unscaled, 10) catch return Error.UnsupportedParquetDecimal;
    if (have > want) unscaled = eval.roundScaleDown(unscaled, @intCast(have - want));
    return unscaled;
}

fn rescaleTo(comptime T: type, d: Decimal, want: i32) Error!T {
    return std.math.cast(T, try rescale(d, want)) orelse Error.UnsupportedParquetDecimal;
}

/// Big-endian two's complement in exactly `len` bytes (a FIXED_LEN_BYTE_ARRAY
/// decimal); errors rather than truncating a value the precision cannot hold.
fn decimalBytes(b: []u8, x: i128) Error!void {
    if (b.len == 0 or b.len > 16) return Error.UnsupportedParquetDecimal;
    var v = x;
    var i = b.len;
    while (i > 0) : (i -= 1) {
        b[i - 1] = @truncate(@as(u128, @bitCast(v)));
        v >>= 8;
    }
    if (v != (if (x < 0) @as(i128, -1) else 0)) return Error.UnsupportedParquetDecimal;
    if ((b[0] & 0x80 != 0) != (x < 0)) return Error.UnsupportedParquetDecimal;
}

const sink_vtable = driver.Sink.VTable{
    .writeBatch = sinkWrite,
    .close = sinkClose,
    .abort = sinkAbort,
    .openUnit = sinkOpenUnit,
};
fn sinkOpenUnit(p: *anyopaque, arena: std.mem.Allocator) anyerror!driver.UnitEncoder {
    const u = try arena.create(UnitEnc);
    u.* = .{ .parent = @ptrCast(@alignCast(p)), .arena = arena, .held = List(Batch).init(arena) };
    return .{ .ptr = u, .vtable = &UnitEnc.vtable };
}
fn sinkWrite(p: *anyopaque, arena: std.mem.Allocator, b: Batch) anyerror!void {
    return @as(*Writer, @ptrCast(@alignCast(p))).writeBatch(arena, b);
}
fn sinkClose(p: *anyopaque) anyerror!void {
    return @as(*Writer, @ptrCast(@alignCast(p))).close();
}
fn sinkAbort(p: *anyopaque) void {
    @as(*Writer, @ptrCast(@alignCast(p))).abort();
}

const testing = std.testing;
const pqdecode = @import("pqdecode.zig");

test "basalt types map onto Parquet physical and converted types" {
    try testing.expectEqual(parquet.PhysicalType.boolean, (try mapType(types.Type.init(.bool))).phys);
    try testing.expectEqual(parquet.PhysicalType.int64, (try mapType(types.Type.init(.int))).phys);
    try testing.expectEqual(parquet.PhysicalType.double, (try mapType(types.Type.init(.float))).phys);
    try testing.expectEqual(parquet.PhysicalType.byte_array, (try mapType(types.Type.init(.string))).phys);
    try testing.expectEqual(@as(?i32, conv_utf8), (try mapType(types.Type.init(.string))).converted);
    try testing.expectEqual(parquet.PhysicalType.int32, (try mapType(types.Type.init(.date))).phys);
    try testing.expectEqual(@as(?i32, conv_date), (try mapType(types.Type.init(.date))).converted);
    try testing.expectEqual(@as(?i32, conv_timestamp_micros), (try mapType(types.Type.init(.timestamp))).converted);

    try testing.expectError(Error.UnsupportedParquetWrite, mapType(types.Type.init(.array)));
}

test "definition levels pack LSB-first into bit-packed groups of eight" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const packed_levels = try packLevels(a, &[_]u8{ 1, 0, 1, 1, 1, 1, 0, 1 });
    try testing.expectEqual(@as(usize, 2), packed_levels.len);
    try testing.expectEqual(@as(u8, 0x03), packed_levels[0]);
    try testing.expectEqual(@as(u8, 0b10111101), packed_levels[1]);

    const got = try pqdecode.decodeRleHybrid(a, packed_levels, 1, 8);
    try testing.expectEqualSlices(u32, &.{ 1, 0, 1, 1, 1, 1, 0, 1 }, got);
}

test "decimal rescaling restates the unscaled value against the column scale" {
    try testing.expectEqual(@as(i128, 1550), try rescale(.{ .unscaled = 155, .scale = 1 }, 2));
    try testing.expectEqual(@as(i128, 155), try rescale(.{ .unscaled = 155, .scale = 2 }, 2));
    try testing.expectEqual(@as(i128, 16), try rescale(.{ .unscaled = 155, .scale = 2 }, 1));
    try testing.expectEqual(@as(i128, -16), try rescale(.{ .unscaled = -155, .scale = 2 }, 1));
    try testing.expectEqual(@as(i128, 15), try rescale(.{ .unscaled = 154, .scale = 2 }, 1));
    try testing.expectEqual(@as(i128, -1235), try rescale(.{ .unscaled = -12345, .scale = 3 }, 2));
    try testing.expectEqual(@as(i128, 12_500_000_000_000_000_000), try rescale(.{ .unscaled = 125, .scale = 1 }, 18));
    try testing.expectError(Error.UnsupportedParquetDecimal, rescale(.{ .unscaled = std.math.maxInt(i64), .scale = 0 }, 30));
    try testing.expectError(Error.UnsupportedParquetDecimal, rescaleTo(i64, .{ .unscaled = 125, .scale = 1 }, 18));
    try testing.expectEqual(@as(i64, std.math.maxInt(i64)), try rescaleTo(i64, .{ .unscaled = std.math.maxInt(i64), .scale = 0 }, 0));
}

test "the physical type follows the decimal's real precision" {
    const small = try mapType(types.Type.decimal(9, 2));
    try testing.expectEqual(parquet.PhysicalType.int32, small.phys);
    try testing.expectEqual(@as(?i32, 9), small.precision);

    const ten = try mapType(types.Type.decimal(10, 2));
    try testing.expectEqual(parquet.PhysicalType.int64, ten.phys);
    try testing.expectEqual(@as(?i32, 2), ten.scale);
    try testing.expectEqual(@as(?i32, 10), ten.precision);

    const mid = try mapType(types.Type.decimal(18, 4));
    try testing.expectEqual(parquet.PhysicalType.int64, mid.phys);
    try testing.expectEqual(@as(?i32, 18), mid.precision);

    const wide = try mapType(types.Type.decimal(38, 18));
    try testing.expectEqual(parquet.PhysicalType.fixed_len_byte_array, wide.phys);
    try testing.expectEqual(@as(?i32, 38), wide.precision);
    try testing.expectEqual(@as(?i32, 18), wide.scale);
    try testing.expectEqual(@as(?i32, 16), wide.type_length);

    const nc = try mapType(types.Type.decimal(30, 20));
    try testing.expectEqual(@as(?i32, 30), nc.precision);
    try testing.expectEqual(@as(?i32, 20), nc.scale);

    try testing.expectError(Error.UnsupportedParquetDecimal, mapType(types.Type.decimal(10, 12)));
}

test "flba widths and big-endian two's complement match what readers expect" {
    try testing.expectEqual(@as(i32, 16), flbaLen(38));
    try testing.expectEqual(@as(i32, 13), flbaLen(30));
    try testing.expectEqual(@as(i32, 9), flbaLen(20));
    try testing.expectEqual(@as(i32, 1), flbaLen(2));

    var b: [4]u8 = undefined;
    try decimalBytes(&b, 1);
    try testing.expectEqualSlices(u8, &.{ 0, 0, 0, 1 }, &b);
    try decimalBytes(&b, -1);
    try testing.expectEqualSlices(u8, &.{ 0xFF, 0xFF, 0xFF, 0xFF }, &b);
    try decimalBytes(&b, -2);
    try testing.expectEqualSlices(u8, &.{ 0xFF, 0xFF, 0xFF, 0xFE }, &b);

    var wide: [16]u8 = undefined;
    const x: i128 = 12_500_000_000_000_000_000;
    try decimalBytes(&wide, x);
    var acc: i128 = if (wide[0] & 0x80 != 0) -1 else 0;
    for (wide) |byte| acc = (acc << 8) | byte;
    try testing.expectEqual(x, acc);

    var one: [1]u8 = undefined;
    try testing.expectError(Error.UnsupportedParquetDecimal, decimalBytes(&one, 128));
    try testing.expectError(Error.UnsupportedParquetDecimal, decimalBytes(&one, -129));
    try decimalBytes(&one, -128);
    try testing.expectEqual(@as(u8, 0x80), one[0]);
}

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
    try testing.expect((try r.next(a)) == null);
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
    try testing.expectError(error.FileNotFound, tmp.dir.statFile("x.parquet"));
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

    const bytes = try std.fs.cwd().readFileAlloc(a, path, 1 << 20);
    const md = try parquet.parseFile(a, bytes);
    const stats = try readStats(a, bytes, md);
    try testing.expectEqual(@as(i64, 1), stats[0].nulls);
    try testing.expectEqual(@as(i64, -3), std.mem.readInt(i64, stats[0].min[0..8], .little));
    try testing.expectEqual(@as(i64, 11), std.mem.readInt(i64, stats[0].max[0..8], .little));
    try testing.expectEqual(@as(i64, 1), stats[1].nulls);
    try testing.expectEqualStrings("apple", stats[1].min);
    try testing.expectEqualStrings("zebra", stats[1].max);
}

const Stat = struct { nulls: i64, min: []const u8, max: []const u8 };

fn readStats(arena: std.mem.Allocator, bytes: []const u8, md: parquet.FileMetaData) ![]Stat {
    const r = try parquet.footerRange(bytes.len, bytes);
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

fn indexWidth(n: usize) u8 {
    if (n <= 1) return 0;
    return @intCast(32 - @clz(@as(u32, @intCast(n - 1))));
}

/// Dictionary indices as an RLE/bit-packed hybrid, always bit-packed in groups
/// of eight, the shape the reader's `decodeRleHybrid` expects.
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
    try w.writeI32(1, @intFromEnum(parquet.PageType.dictionary_page));
    try w.writeI32(2, @intCast(uncompressed));
    try w.writeI32(3, @intCast(compressed));
    try w.writeI32(4, @bitCast(crc));
    try w.fieldBegin(.@"struct", 7);
    try w.structBegin();
    try w.writeI32(1, @intCast(values));
    try w.writeI32(2, @intFromEnum(parquet.Encoding.plain));
    try w.structEnd();
    try w.structEnd();
}

fn writeDictDataPageHeader(out: *List(u8), uncompressed: usize, compressed: usize, values: usize, crc: u32) !void {
    var w = thrift.Writer.init(out);
    try w.structBegin();
    try w.writeI32(1, @intFromEnum(parquet.PageType.data_page));
    try w.writeI32(2, @intCast(uncompressed));
    try w.writeI32(3, @intCast(compressed));
    try w.writeI32(4, @bitCast(crc));
    try w.fieldBegin(.@"struct", 5);
    try w.structBegin();
    try w.writeI32(1, @intCast(values));
    try w.writeI32(2, @intFromEnum(parquet.Encoding.rle_dictionary));
    try w.writeI32(3, @intFromEnum(parquet.Encoding.rle));
    try w.writeI32(4, @intFromEnum(parquet.Encoding.rle));
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

    var b = try column.Builder.initCapacity(a, schema.fields[0].ty, 300);
    const vals = [_][]const u8{ "alpha", "beta", "gamma" };
    for (0..300) |i| {
        if (i % 37 == 0) try b.append(.null) else try b.append(.{ .string = vals[i % 3] });
    }
    var cols = [_]column.Column{try b.finish()};
    try w.writeBatch(a, .{ .schema = &schema, .columns = &cols, .len = 300 });
    try w.close();

    const bytes = try tmp.dir.readFileAlloc(a, "d.parquet", 1 << 20);
    const md = try parquet.parseFile(a, bytes);
    const meta = md.row_groups[0].columns[0].meta.?;
    try testing.expect(std.mem.indexOfScalar(parquet.Encoding, meta.encodings, .rle_dictionary) != null);
    const dict_at: usize = @intCast(meta.dictionary_page_offset.?);
    try testing.expect(dict_at > 0 and dict_at < meta.data_page_offset);
    const dict = try parquet.parsePageHeader(bytes[dict_at..]);
    try testing.expectEqual(parquet.PageType.dictionary_page, dict.ty);
    try testing.expectEqual(@as(i32, 3), dict.num_values);
    const data = try parquet.parsePageHeader(bytes[@intCast(meta.data_page_offset)..]);
    try testing.expectEqual(parquet.Encoding.rle_dictionary, data.encoding);

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

test "an az:// target routes to the blob writer, never the local filesystem" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const schema = types.Schema{ .fields = &.{
        .{ .name = "x", .ty = types.Type.init(.int).asNullable() },
    } };

    if (std.posix.getenv("AZURE_STORAGE_KEY") != null) return error.SkipZigTest;
    try testing.expectError(error.AzureMissingKey, Writer.open(a, "az://acct/ctr/bronze/t.parquet", schema, .snappy, .truncate));
}

test "chunk size totals account for page headers" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realpathAlloc(a, ".");
    const path = try std.fs.path.join(a, &.{ dir, "sizes.parquet" });

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
    const trailer = bytes[bytes.len - parquet.trailer_len ..];
    const flen = std.mem.readInt(u32, trailer[0..4], .little);
    const footer_start = bytes.len - parquet.trailer_len - flen;
    const md = try parquet.parseFooter(a, bytes[footer_start..][0..flen]);

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
