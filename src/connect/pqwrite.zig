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

const std = @import("std");
const pq = @import("parquet.zig");
const thrift = @import("thrift.zig");
const codec = @import("codec.zig");
const driver = @import("driver.zig");
const types = @import("../lang/types.zig");
const batchmod = @import("../exec/batch.zig");
const valuemod = @import("../exec/value.zig");

const Batch = batchmod.Batch;
const Value = valuemod.Value;
const List = std.array_list.Managed;

pub const Error = error{
    UnsupportedParquetWrite,
} || std.mem.Allocator.Error || codec.Error;

/// Rows buffered before a row group is flushed.
pub const row_group_rows = 100_000;

/// How a basalt column is stored in Parquet.
const Mapping = struct {
    phys: pq.PhysicalType,
    converted: ?i32 = null,
    precision: ?i32 = null,
    scale: ?i32 = null,
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
        .decimal => .{
            .phys = .int64,
            .converted = conv_decimal,
            .precision = @min(18, @as(i32, t.precision)),
            .scale = t.scale,
        },
        .array, .@"struct" => Error.UnsupportedParquetWrite,
    };
}

/// Per-column accumulation for the row group being built.
const ColBuf = struct {
    values: List(u8),
    /// One entry per row: 1 present, 0 null. Packed at flush time.
    defs: List(u8),
    /// BOOLEAN values are bit-packed, so they need their own accumulator.
    bit_buf: u8 = 0,
    bit_n: u3 = 0,

    fn init(a: std.mem.Allocator) ColBuf {
        return .{ .values = List(u8).init(a), .defs = List(u8).init(a) };
    }

    fn reset(self: *ColBuf) void {
        self.values.clearRetainingCapacity();
        self.defs.clearRetainingCapacity();
        self.bit_buf = 0;
        self.bit_n = 0;
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

const ChunkMeta = struct {
    offset: i64,
    num_values: i64,
    uncompressed: i64,
    compressed: i64,
};

const RowGroupMeta = struct {
    chunks: []ChunkMeta,
    num_rows: i64,
    total_byte_size: i64,
};

pub const Writer = struct {
    arena: std.mem.Allocator,
    file: std.fs.File,
    schema: types.Schema,
    maps: []Mapping,
    compression: codec.Codec,
    cols: []ColBuf,
    rows: usize = 0,
    total_rows: i64 = 0,
    offset: i64 = 0,
    groups: List(RowGroupMeta),

    pub fn isPath(path: []const u8) bool {
        return std.mem.endsWith(u8, path, ".parquet");
    }

    pub fn open(
        arena: std.mem.Allocator,
        path: []const u8,
        schema: types.Schema,
        compression: codec.Codec,
    ) !*Writer {
        if (!codec.canCompress(compression)) return codec.Error.UnsupportedCodec;
        const maps = try arena.alloc(Mapping, schema.fields.len);
        for (schema.fields, maps) |f, *m| m.* = try mapType(f.ty);

        const cols = try arena.alloc(ColBuf, schema.fields.len);
        for (cols) |*c| c.* = ColBuf.init(arena);

        const self = try arena.create(Writer);
        self.* = .{
            .arena = arena,
            .file = try std.fs.cwd().createFile(path, .{}),
            .schema = schema,
            .maps = maps,
            .compression = compression,
            .cols = cols,
            .groups = List(RowGroupMeta).init(arena),
        };
        try self.emit(pq.magic);
        return self;
    }

    fn emit(self: *Writer, bytes: []const u8) !void {
        try self.file.writeAll(bytes);
        self.offset += @intCast(bytes.len);
    }

    pub fn writeBatch(self: *Writer, arena: std.mem.Allocator, batch: Batch) !void {
        _ = arena;
        for (0..batch.len) |r| {
            for (batch.columns, self.cols, self.maps) |col, *cb, m| {
                const v = col.getValue(r);
                try cb.defs.append(if (v == .null) 0 else 1);
                if (v == .null) continue;
                try encodePlain(cb, m, v);
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

        for (self.cols, chunks) |*cb, *cm| {
            try cb.flushBits();

            // page body: [4-byte level length][RLE def levels][PLAIN values]
            var body = List(u8).init(self.arena);
            const levels = try packLevels(self.arena, cb.defs.items);
            var len4: [4]u8 = undefined;
            std.mem.writeInt(u32, &len4, @intCast(levels.len), .little);
            try body.appendSlice(&len4);
            try body.appendSlice(levels);
            try body.appendSlice(cb.values.items);

            const raw = body.items;
            const packed_body = try codec.compress(self.arena, self.compression, raw);

            const start = self.offset;
            var hdr = List(u8).init(self.arena);
            try writePageHeader(&hdr, raw.len, packed_body.len, cb.defs.items.len);
            try self.emit(hdr.items);
            try self.emit(packed_body);

            cm.* = .{
                .offset = start,
                .num_values = @intCast(cb.defs.items.len),
                .uncompressed = @intCast(raw.len),
                .compressed = @intCast(packed_body.len),
            };
            group_bytes += @intCast(raw.len);
            cb.reset();
        }

        try self.groups.append(.{
            .chunks = chunks,
            .num_rows = @intCast(self.rows),
            .total_byte_size = group_bytes,
        });
        self.rows = 0;
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
        self.file.close();
    }

    /// A partial Parquet file has no footer and is unreadable, which is the
    /// correct outcome for an aborted run — there is nothing to roll back.
    pub fn abort(self: *Writer) void {
        self.file.close();
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
            try w.writeZigZag(@intFromEnum(pq.Encoding.plain));
            try w.listBegin(3, .binary, 1);
            try w.writeVarint(f.name.len);
            try w.out.appendSlice(f.name);
            try w.writeI32(4, @intFromEnum(self.compression));
            try w.writeI64(5, c.num_values);
            try w.writeI64(6, c.uncompressed);
            try w.writeI64(7, c.compressed);
            try w.writeI64(9, c.offset); // data_page_offset
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

fn writeSchemaRoot(w: *thrift.Writer, children: usize) !void {
    try w.structBegin();
    try w.writeBinary(4, "basalt_schema");
    try w.writeI32(5, @intCast(children));
    try w.structEnd();
}

fn writeSchemaLeaf(w: *thrift.Writer, name: []const u8, m: Mapping) !void {
    try w.structBegin();
    try w.writeI32(1, @intFromEnum(m.phys));
    try w.writeI32(3, @intFromEnum(pq.Repetition.optional));
    try w.writeBinary(4, name);
    if (m.converted) |c| try w.writeI32(6, c);
    if (m.scale) |s| try w.writeI32(7, s);
    if (m.precision) |p| try w.writeI32(8, p);
    try w.structEnd();
}

fn writePageHeader(out: *List(u8), uncompressed: usize, compressed: usize, values: usize) !void {
    var w = thrift.Writer.init(out);
    try w.structBegin();
    try w.writeI32(1, @intFromEnum(pq.PageType.data_page));
    try w.writeI32(2, @intCast(uncompressed));
    try w.writeI32(3, @intCast(compressed));
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
                .decimal => |d| rescale(d, m.scale orelse 0),
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
fn rescale(d: valuemod.Decimal, want: i32) i64 {
    const target: i32 = want;
    var unscaled: i128 = d.unscaled;
    var have: i32 = d.scale;
    while (have < target) : (have += 1) unscaled *= 10;
    while (have > target) : (have -= 1) unscaled = @divTrunc(unscaled, 10);
    return @intCast(std.math.clamp(unscaled, std.math.minInt(i64), std.math.maxInt(i64)));
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
    try testing.expectEqual(@as(i64, 1550), rescale(.{ .unscaled = 155, .scale = 1 }, 2));
    try testing.expectEqual(@as(i64, 155), rescale(.{ .unscaled = 155, .scale = 2 }, 2));
    // reducing scale truncates, matching the direction Parquet writers take
    try testing.expectEqual(@as(i64, 15), rescale(.{ .unscaled = 155, .scale = 2 }, 1));
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

    var w = try Writer.open(a, path, schema, .snappy);
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
    try testing.expectError(codec.Error.UnsupportedCodec, Writer.open(a, path, schema, .zstd));
}
