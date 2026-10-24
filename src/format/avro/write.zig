//! `LOAD INTO 'x.avro'`: rows as an Avro object container file, through `Output`, so
//! it may go to a local file, an object store, SFTP or SMB.
//!
//! The schema is one record, `row`, a field per column, each a union of `null` and
//! its type, so any column may hold NULL: BOOL as `boolean`, INT as `long`, FLOAT as
//! `double`, STRING and BYTES as themselves, DECIMAL(p,s) as `bytes` with the
//! `decimal` logical type (precision 38 when the column's is unknown), DATE as `date`,
//! TIME as `time-micros` and TIMESTAMP as `local-timestamp-micros`, since basalt's
//! timestamps carry no timezone (as its Parquet files say with `isAdjustedToUTC`
//! false). An array or struct column, and a column name Avro does not allow
//! (`[A-Za-z_][A-Za-z0-9_]*`, so not a flattened `addr.city`), are refused up front
//! naming the column, rather than written under another name.
//!
//! Each batch is one block, encoded and compressed on its own (`renderBatch`), so
//! lanes encode in parallel and only the finished bytes are written in turn; every
//! block ends in the one sync marker the header chose at random. Blocks are
//! `deflate` (the codec every reader must support) unless `compression` names
//! `snappy` or `none`; zstandard is read but not written, as std ships no zstd
//! compressor.

const std = @import("std");
const types = @import("../../lang/types.zig");
const Batch = @import("../../exec/batch.zig").Batch;
const Value = @import("../../exec/value.zig").Value;
const codecs = @import("../compress/codecs.zig");
const deflate = @import("../compress/deflate.zig");
const driver = @import("../../connect/driver.zig");
const jsonwrite = @import("../json/write.zig");
const read = @import("read.zig");
const schema_mod = @import("schema.zig");
const stream = @import("../stream.zig");
const varint = @import("../varint.zig");
const eval = @import("../../exec/eval.zig");
const Decimal = @import("../../exec/value.zig").Decimal;
const pow10_i128 = @import("../../exec/value.zig").pow10_i128;
const Output = @import("../output.zig").Output;

pub const Codec = read.Codec;

pub const Error = error{ AvroUnsupportedType, AvroBadName, AvroCodecUnsupported, AvroDecimalOverflow };

/// Why a column cannot be written, null when every one can.
pub fn schemaProblem(schema: types.Schema) ?anyerror {
    for (schema.fields) |f| {
        if (f.ty.kind == .array or f.ty.kind == .@"struct")
            return stream.explainf(error.AvroUnsupportedType, "column `{s}` is an array or struct, which basalt does not write to Avro", .{f.name});
        if (!schema_mod.validName(f.name))
            return stream.explainf(error.AvroBadName, "column `{s}` is not a name Avro allows (letters, digits and `_`, not starting with a digit); rename it with AS", .{f.name});
    }
    return null;
}

/// The schema's JSON for `schema`'s columns, each a union of null and its type.
pub fn schemaJson(arena: std.mem.Allocator, schema: types.Schema) ![]const u8 {
    var aw = std.Io.Writer.Allocating.init(arena);
    const w = &aw.writer;
    try w.writeAll("{\"type\":\"record\",\"name\":\"row\",\"fields\":[");
    for (schema.fields, 0..) |f, i| {
        if (i > 0) try w.writeByte(',');
        try w.writeAll("{\"name\":\"");
        try jsonwrite.writeEscaped(w, f.name);
        try w.writeAll("\",\"type\":[\"null\",");
        switch (f.ty.kind) {
            .bool => try w.writeAll("\"boolean\""),
            .int => try w.writeAll("\"long\""),
            .float => try w.writeAll("\"double\""),
            .string => try w.writeAll("\"string\""),
            .bytes => try w.writeAll("\"bytes\""),
            .decimal => try w.print("{{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":{d},\"scale\":{d}}}", .{ precisionOf(f.ty), f.ty.scale }),
            .date => try w.writeAll("{\"type\":\"int\",\"logicalType\":\"date\"}"),
            .time => try w.writeAll("{\"type\":\"long\",\"logicalType\":\"time-micros\"}"),
            .timestamp => try w.writeAll("{\"type\":\"long\",\"logicalType\":\"local-timestamp-micros\"}"),
            .array, .@"struct" => unreachable,
        }
        try w.writeAll("],\"default\":null}");
    }
    try w.writeAll("]}");
    return aw.written();
}

fn precisionOf(ty: types.Type) u8 {
    return if (ty.precision == 0) 38 else @max(ty.precision, ty.scale);
}

pub const Writer = struct {
    out: *Output,
    schema: types.Schema,
    codec: Codec,
    sync: [16]u8,

    pub fn open(arena: std.mem.Allocator, path: []const u8, schema: types.Schema, codec: Codec) !*Writer {
        if (schemaProblem(schema)) |e| return e;
        if (codec == .zstandard) return stream.explainf(error.AvroCodecUnsupported, "basalt reads zstandard Avro blocks but does not write them; use deflate, snappy or none", .{});
        const out = try Output.open(arena, path, "avro/binary", .truncate);
        errdefer out.abort();
        const self = try arena.create(Writer);
        self.* = .{ .out = out, .schema = schema, .codec = codec, .sync = undefined };
        std.crypto.random.bytes(&self.sync);
        const head = try header(arena, try schemaJson(arena, schema), codec, self.sync);
        out.writer().writeAll(head) catch |e| return out.specific(e);
        return self;
    }

    /// One block: its object count and byte size, the rows encoded and compressed,
    /// and the sync marker. Reads only what `open` fixed, so lanes call it at once.
    pub fn renderBatch(self: *Writer, arena: std.mem.Allocator, batch: Batch) ![]const u8 {
        if (batch.len == 0) return "";
        var body = std.array_list.Managed(u8).init(arena);
        try body.ensureTotalCapacity(batch.len * (batch.columns.len * 9 + 1));
        for (0..batch.len) |r| for (batch.columns, self.schema.fields) |*col, f| try encode(&body, col.getValue(r), f);
        const data = try compress(arena, self.codec, body.items);
        var out = std.array_list.Managed(u8).init(arena);
        try out.ensureTotalCapacity(data.len + 36);
        try long(&out, @intCast(batch.len));
        try long(&out, @intCast(data.len));
        try out.appendSlice(data);
        try out.appendSlice(&self.sync);
        return out.items;
    }

    pub fn writeRendered(self: *Writer, bytes: []const u8) !void {
        if (bytes.len == 0) return;
        self.out.writer().writeAll(bytes) catch |e| return self.out.specific(e);
    }

    pub fn writeBatch(self: *Writer, arena: std.mem.Allocator, batch: Batch) !void {
        return self.writeRendered(try self.renderBatch(arena, batch));
    }

    pub fn close(self: *Writer) !void {
        return self.out.finish();
    }

    pub fn abort(self: *Writer) void {
        self.out.abort();
    }

    pub fn sink(self: *Writer) driver.Sink {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = driver.sinkVTable(Writer);
};

/// The container header: magic, the schema and codec as metadata, the sync marker.
pub fn header(arena: std.mem.Allocator, schema_json: []const u8, codec: Codec, sync: [16]u8) ![]const u8 {
    var out = std.array_list.Managed(u8).init(arena);
    try out.appendSlice(read.magic);
    try long(&out, 2);
    try bytesOf(&out, "avro.schema");
    try bytesOf(&out, schema_json);
    try bytesOf(&out, "avro.codec");
    try bytesOf(&out, @tagName(codec));
    try long(&out, 0);
    try out.appendSlice(&sync);
    return out.items;
}

fn long(out: *std.array_list.Managed(u8), v: i64) !void {
    try varint.write(out, varint.zigzag(v));
}

fn bytesOf(out: *std.array_list.Managed(u8), s: []const u8) !void {
    try long(out, @intCast(s.len));
    try out.appendSlice(s);
}

/// One cell as its union: branch 0 for NULL, else branch 1 and the value.
fn encode(out: *std.array_list.Managed(u8), v: Value, f: types.Schema.Field) !void {
    if (v == .null) return out.append(0);
    try out.append(2);
    switch (v) {
        .null => unreachable,
        .bool => |b| try out.append(@intFromBool(b)),
        .int => |x| try long(out, x),
        .float => |x| try out.appendSlice(&std.mem.toBytes(std.mem.nativeToLittle(u64, @bitCast(x)))),
        .string, .bytes => |s| try bytesOf(out, s),
        .date => |d| try long(out, d),
        .time, .timestamp => |x| try long(out, x),
        .decimal => |d| try decimalBytes(out, d, f),
    }
}

/// A decimal restated at the column's scale, as Parquet's writer restates it, in the
/// fewest big-endian two's complement bytes that hold it. One with more digits than
/// the schema's precision fails naming the column, as a reader would refuse it.
fn decimalBytes(out: *std.array_list.Managed(u8), d: Decimal, f: types.Schema.Field) !void {
    const p = precisionOf(f.ty);
    const r = eval.rescaleTo(d, f.ty.scale) orelse return decimalTooWide(f, p);
    if (r.unscaled >= pow10_i128[p] or r.unscaled <= -pow10_i128[p]) return decimalTooWide(f, p);
    const be = std.mem.toBytes(std.mem.nativeToBig(i128, r.unscaled));
    var start: usize = 0;
    while (start < 15) : (start += 1) {
        const lead = be[start];
        const next_high = be[start + 1] & 0x80 != 0;
        if (!((lead == 0 and !next_high) or (lead == 0xFF and next_high))) break;
    }
    try bytesOf(out, be[start..]);
}

fn decimalTooWide(f: types.Schema.Field, p: u8) anyerror {
    return stream.explainf(error.AvroDecimalOverflow, "a value of column `{s}` does not fit its DECIMAL({d},{d})", .{ f.name, p, f.ty.scale });
}

fn compress(arena: std.mem.Allocator, codec: Codec, body: []const u8) ![]const u8 {
    switch (codec) {
        .null => return body,
        .deflate => {
            var aw: std.Io.Writer.Allocating = .init(arena);
            const z = try deflate.Deflate.init(std.heap.page_allocator, &aw.writer, .{ .level = .fast, .container = .raw });
            defer z.deinit(std.heap.page_allocator);
            try z.interface.writeAll(body);
            try z.finish();
            return aw.written();
        },
        .snappy => {
            const packed_ = try codecs.compress(arena, .snappy, body);
            const out = try arena.alloc(u8, packed_.len + 4);
            @memcpy(out[0..packed_.len], packed_);
            std.mem.writeInt(u32, out[packed_.len..][0..4], std.hash.Crc32.hash(body), .big);
            return out;
        },
        .zstandard => unreachable,
    }
}

const testing = std.testing;
const celltext = @import("../../exec/celltext.zig");

fn same(want: Value, got: Value, ty: types.Type) bool {
    if (want == .null or got == .null) return want == .null and got == .null;
    return switch (want) {
        .float => |x| std.math.isNan(x) and std.math.isNan(got.float) or @as(u64, @bitCast(x)) == @as(u64, @bitCast(got.float)),
        .decimal => |d| eval.rescaleTo(d, ty.scale).?.order(got.decimal) == .eq,
        .string => |s| std.mem.eql(u8, s, got.string),
        .bytes => |s| std.mem.eql(u8, s, got.bytes),
        else => std.meta.eql(want, got),
    };
}

fn roundTrip(a: std.mem.Allocator, dir: std.fs.Dir, batch: Batch, codec: Codec) !void {
    const path = try std.fs.path.join(a, &.{ try dir.realpathAlloc(a, "."), "t.avro" });
    const w = try Writer.open(a, path, batch.schema.*, codec);
    {
        errdefer w.abort();
        try w.writeBatch(a, batch);
        try w.writeRendered(try w.renderBatch(a, batch));
    }
    try w.close();
    const f = try read.Files.open(a, testing.allocator, &.{path}, .{ .batch_rows = 3 });
    defer f.close();
    try testing.expectEqual(batch.schema.fields.len, f.schema.fields.len);
    for (batch.schema.fields, f.schema.fields) |x, y| {
        try testing.expectEqualStrings(x.name, y.name);
        try testing.expect(types.Type.eql(x.ty, y.ty));
    }
    var at: usize = 0;
    while (try f.next(a)) |b| {
        for (0..b.len) |r| {
            for (batch.columns, b.columns, batch.schema.fields) |want, got, fld| {
                const wv = want.getValue((at + r) % batch.len);
                if (!same(wv, got.getValue(r), fld.ty)) {
                    std.debug.print("{s} row {d}: wrote {any}, read {any}\n", .{ fld.name, at + r, wv, got.getValue(r) });
                    return error.TestUnexpectedResult;
                }
            }
        }
        at += b.len;
    }
    try testing.expectEqual(2 * batch.len, at);
}

test "every golden column round-trips through each codec, two blocks each" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_]Codec{ .null, .deflate, .snappy }) |codec| {
        for (try celltext.goldenBatches(a)) |batch| {
            if (schemaProblem(batch.schema.*) != null) continue;
            if (batch.schema.fields[0].ty.kind == .decimal and batch.schema.fields.len == 1) {
                try testing.expectError(error.AvroDecimalOverflow, roundTrip(a, tmp.dir, batch, codec));
                try testing.expectEqualStrings("a value of column `c` does not fit its DECIMAL(14,2)", eval.takeFailure(error.AvroDecimalOverflow).?);
                continue;
            }
            try roundTrip(a, tmp.dir, batch, codec);
        }
    }
}

test "an empty table is a header and no blocks, read back as no rows" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const schema = types.Schema{ .fields = &.{.{ .name = "x", .ty = types.Type.init(.int).asNullable() }} };
    const path = try std.fs.path.join(a, &.{ try tmp.dir.realpathAlloc(a, "."), "e.avro" });
    const w = try Writer.open(a, path, schema, .deflate);
    try w.close();
    const f = try read.Files.open(a, testing.allocator, &.{path}, .{});
    defer f.close();
    try testing.expectEqualStrings("x", f.schema.fields[0].name);
    try testing.expect((try f.next(a)) == null);
}

test "an array column, a name Avro forbids and a zstandard write are refused naming why" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const nested = types.Schema{ .fields = &.{.{ .name = "x", .ty = types.Type.init(.array) }} };
    try testing.expectEqual(error.AvroUnsupportedType, schemaProblem(nested).?);
    try testing.expect(std.mem.indexOf(u8, eval.takeFailure(error.AvroUnsupportedType).?, "`x`") != null);
    const dotted = types.Schema{ .fields = &.{.{ .name = "addr.city", .ty = types.Type.init(.string) }} };
    try testing.expectEqual(error.AvroBadName, schemaProblem(dotted).?);
    try testing.expectError(error.AvroCodecUnsupported, Writer.open(a, "unused.avro", .{ .fields = &.{} }, .zstandard));
}

test "a decimal is written in the fewest bytes, restated at its column's scale" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const Case = struct { Decimal, u8, []const u8 };
    const cases = [_]Case{
        .{ .{ .unscaled = 0, .scale = 2 }, 2, &.{0} },
        .{ .{ .unscaled = 127, .scale = 0 }, 0, &.{0x7F} },
        .{ .{ .unscaled = 128, .scale = 0 }, 0, &.{ 0x00, 0x80 } },
        .{ .{ .unscaled = -128, .scale = 0 }, 0, &.{0x80} },
        .{ .{ .unscaled = -129, .scale = 0 }, 0, &.{ 0xFF, 0x7F } },
        .{ .{ .unscaled = 155, .scale = 1 }, 2, &.{ 0x06, 0x0E } },
    };
    for (cases) |c| {
        var out = std.array_list.Managed(u8).init(a);
        try decimalBytes(&out, c[0], .{ .name = "d", .ty = types.Type.decimal(38, c[1]) });
        try testing.expectEqual(@as(u8, @intCast(2 * c[2].len)), out.items[0]);
        try testing.expectEqualSlices(u8, c[2], out.items[1..]);
        try testing.expect((try read.decimalOf(out.items[1..], 38, c[1])).decimal.order(eval.rescaleTo(c[0], c[1]).?) == .eq);
    }
    var out = std.array_list.Managed(u8).init(a);
    try testing.expectError(error.AvroDecimalOverflow, decimalBytes(&out, .{ .unscaled = 1000, .scale = 0 }, .{ .name = "d", .ty = types.Type.decimal(3, 0) }));
    try decimalBytes(&out, .{ .unscaled = 999, .scale = 0 }, .{ .name = "d", .ty = types.Type.decimal(3, 0) });
}
