//! `LOAD INTO 'x.avro'`: rows as an Avro object container file, through `Output`, so
//! it may go to a local file, an object store, SFTP or SMB.
//!
//! The schema is one record, `row`, a field per column, each a union of `null` and
//! its type, so any column may hold NULL: BOOL as `boolean`, INT as `long`, FLOAT as
//! `double`, STRING and BYTES as themselves, DECIMAL(p,s) as `bytes` with the
//! `decimal` logical type (precision 38 when the column's is unknown), DATE as `date`,
//! TIME as `time-micros` and TIMESTAMP as `local-timestamp-micros`, since basalt's
//! timestamps carry no timezone (as its Parquet files say with `isAdjustedToUTC`
//! false). A struct is a record (named `r1`, `r2`… in the order the schema
//! meets them, as Avro names every record), a list an array, a map a map, every
//! part of them a union of `null` and its type as a column is, a map's keys aside,
//! which Avro holds as strings: a map whose keys are not STRING is refused. A
//! column or field name Avro does not allow (`[A-Za-z_][A-Za-z0-9_]*`) is refused
//! up front naming it, rather than written under another name.
//!
//! Each batch is one block, encoded and compressed on its own (`renderBatch`), so
//! lanes encode in parallel and only the finished bytes are written in turn; every
//! block ends in the one sync marker the header chose at random. Blocks are
//! `deflate` (the codec every reader must support) unless `compression` names
//! `snappy` or `none`; zstandard is read but not written, as std ships no zstd
//! compressor.

const std = @import("std");
const pagecache = @import("../../exec/pagecache.zig");
const msgfmt = @import("../msgfmt.zig");
const types = @import("../../lang/types.zig");
const Batch = @import("../../exec/batch.zig").Batch;
const Value = @import("../../exec/value.zig").Value;
const Column = @import("../../exec/column.zig").Column;
const decimals = @import("../../exec/column.zig").decimals;
const codecs = @import("../compress/codecs.zig");
const deflate = @import("../compress/deflate.zig");
const driver = @import("../../connect/driver.zig");
const jsontext = @import("../../exec/jsontext.zig");
const column_mod = @import("../../exec/column.zig");
const civil = @import("../../exec/civil.zig");
const read = @import("read.zig");
const schema_mod = @import("schema.zig");
const stream = @import("../stream.zig");
const varint = @import("../varint.zig");
const eval = @import("../../exec/eval.zig");
const decimal = @import("../../exec/decimal.zig");
const Decimal = @import("../../exec/value.zig").Decimal;
const pow10_i128 = decimal.pow10_i128;
const Output = @import("../output.zig").Output;

pub const Codec = read.Codec;

pub const Error = error{ AvroUnsupportedType, AvroBadName, AvroCodecUnsupported, AvroDecimalOverflow };

/// Why a column cannot be written, null when every one can.
pub fn schemaProblem(schema: types.Schema) ?anyerror {
    for (schema.fields) |f| {
        if (!schema_mod.validName(f.name))
            return stream.explainf(error.AvroBadName, "column `{s}` is not a name Avro allows (letters, digits and `_`, not starting with a digit); rename it with AS", .{f.name});
        if (typeProblem(f.name, f.ty)) |e| return e;
    }
    return null;
}

fn typeProblem(col: []const u8, ty: types.Type) ?anyerror {
    switch (ty.kind) {
        .@"struct" => for (ty.fields orelse &.{}) |f| {
            if (!schema_mod.validName(f.name))
                return stream.explainf(error.AvroBadName, "column `{s}` has a field `{s}`, which is not a name Avro allows (letters, digits and `_`, not starting with a digit)", .{ col, f.name });
            if (typeProblem(col, f.ty)) |e| return e;
        },
        .array => return typeProblem(col, column_mod.childType(ty)),
        .map => {
            const kv = ty.fields orelse return null;
            if (kv[0].ty.kind != .string) return stream.explainf(error.AvroUnsupportedType, "column `{s}` is a MAP with {s} keys; Avro's map keys are strings, so cast the keys to STRING first", .{ col, @tagName(kv[0].ty.kind) });
            return typeProblem(col, kv[1].ty);
        },
        else => {},
    }
    return null;
}

/// The schema's JSON for `schema`'s columns, each a union of null and its type.
pub fn schemaJson(arena: std.mem.Allocator, schema: types.Schema) ![]const u8 {
    var aw = std.Io.Writer.Allocating.init(arena);
    const w = &aw.writer;
    try w.writeAll("{\"type\":\"record\",\"name\":\"row\",\"fields\":[");
    var records: usize = 0;
    try fieldsJson(w, schema.fields, &records);
    try w.writeAll("]}");
    return aw.written();
}

/// A record's fields, each a union of null and its type.
fn fieldsJson(w: *std.Io.Writer, fields: []const types.Schema.Field, records: *usize) !void {
    for (fields, 0..) |f, i| {
        if (i > 0) try w.writeByte(',');
        try w.writeAll("{\"name\":\"");
        try jsontext.chars(w, f.name);
        try w.writeAll("\",\"type\":");
        try unionJson(w, f.ty, records);
        try w.writeAll(",\"default\":null}");
    }
}

fn unionJson(w: *std.Io.Writer, ty: types.Type, records: *usize) anyerror!void {
    try w.writeAll("[\"null\",");
    try typeJson(w, ty, records);
    try w.writeByte(']');
}

fn typeJson(w: *std.Io.Writer, ty: types.Type, records: *usize) anyerror!void {
    switch (ty.kind) {
        .bool => try w.writeAll("\"boolean\""),
        .int => try w.writeAll("\"long\""),
        .float => try w.writeAll("\"double\""),
        .string => try w.writeAll("\"string\""),
        .bytes => try w.writeAll("\"bytes\""),
        .decimal => try w.print("{{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":{d},\"scale\":{d}}}", .{ precisionOf(ty), ty.scale }),
        .date => try w.writeAll("{\"type\":\"int\",\"logicalType\":\"date\"}"),
        .time => try w.writeAll("{\"type\":\"long\",\"logicalType\":\"time-micros\"}"),
        .timestamp => try w.writeAll("{\"type\":\"long\",\"logicalType\":\"local-timestamp-micros\"}"),
        .@"struct" => {
            records.* += 1;
            try w.print("{{\"type\":\"record\",\"name\":\"r{d}\",\"fields\":[", .{records.*});
            const fs = ty.fields orelse &.{};
            const sf = try std.heap.smp_allocator.alloc(types.Schema.Field, fs.len);
            defer std.heap.smp_allocator.free(sf);
            for (fs, sf) |f, *o| o.* = .{ .name = f.name, .ty = f.ty };
            try fieldsJson(w, sf, records);
            try w.writeAll("]}");
        },
        .array => {
            try w.writeAll("{\"type\":\"array\",\"items\":");
            try unionJson(w, column_mod.childType(ty), records);
            try w.writeByte('}');
        },
        .map => {
            try w.writeAll("{\"type\":\"map\",\"values\":");
            try unionJson(w, (ty.fields orelse &.{})[1].ty, records);
            try w.writeByte('}');
        },
    }
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
        const encoders = try arena.alloc(Encoder, batch.columns.len);
        for (encoders, batch.columns) |*e, c| e.* = encoderFor(c.data);
        for (0..batch.len) |r| for (batch.columns, self.schema.fields, encoders) |*col, f, enc| try enc(&body, col, r, f);
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

/// One cell of a column as its union: branch 0 for NULL, else branch 1 and the value.
const Encoder = *const fn (out: *std.array_list.Managed(u8), col: *const Column, row: usize, f: types.Schema.Field) anyerror!void;

/// The encoder of a column's storage, one instantiated per storage at compile time,
/// reading the typed slice itself rather than a `Value` per cell.
fn encoderFor(data: Column.Data) Encoder {
    return switch (data) {
        inline else => |_, t| &struct {
            fn f(out: *std.array_list.Managed(u8), col: *const Column, row: usize, fld: types.Schema.Field) anyerror!void {
                if (!col.validity.get(row)) return out.append(0);
                try out.append(2);
                switch (t) {
                    .b => try out.append(@intFromBool(col.data.b[row])),
                    .i32 => try long(out, col.data.i32[row]),
                    .i64 => try long(out, col.data.i64[row]),
                    .f64 => try out.appendSlice(&std.mem.toBytes(std.mem.nativeToLittle(u64, @bitCast(col.data.f64[row])))),
                    .bytes => try bytesOf(out, col.data.bytes.at(row)),
                    .dec, .d64 => try decimalBytes(out, decimals(col.*).at(row), fld),
                    .nested => try nestedBody(out, col, row),
                }
            }
        }.f,
    };
}

/// A nested value's body: a struct's fields in order, an array's items or a map's
/// entries in one block, each part as a column's cell is (a union of null and it),
/// a map's key as its string.
fn nestedBody(out: *std.array_list.Managed(u8), col: *const Column, row: usize) anyerror!void {
    const nd = col.data.nested;
    switch (nd.body) {
        .fields => |fs| for (fs, nd.ty.fields orelse &.{}) |*c, f| try encoderFor(c.data)(out, c, row, .{ .name = f.name, .ty = f.ty }),
        .list => |l| {
            const lo: usize = @intCast(l.offsets[row]);
            const hi: usize = @intCast(l.offsets[row + 1]);
            if (hi > lo) {
                try long(out, @intCast(hi - lo));
                if (nd.ty.kind == .map) {
                    const kv = l.child.data.nested.body.fields;
                    const vf = types.Schema.Field{ .name = "value", .ty = kv[1].ty };
                    for (lo..hi) |x| {
                        try bytesOf(out, kv[0].data.bytes.at(x));
                        try encoderFor(kv[1].data)(out, &kv[1], x, vf);
                    }
                } else {
                    const ef = types.Schema.Field{ .name = "item", .ty = l.child.ty };
                    for (lo..hi) |x| try encoderFor(l.child.data)(out, &l.child, x, ef);
                }
            }
            try long(out, 0);
        },
    }
}

/// A decimal restated at the column's scale, as Parquet's writer restates it, in the
/// fewest big-endian two's complement bytes that hold it. One with more digits than
/// the schema's precision fails naming the column, as a reader would refuse it.
fn decimalBytes(out: *std.array_list.Managed(u8), d: Decimal, f: types.Schema.Field) !void {
    const p = precisionOf(f.ty);
    const r = decimal.rescaleTo(d, f.ty.scale) orelse return decimalTooWide(f, p);
    if (!decimal.withinPrecision(r.unscaled, p)) return decimalTooWide(f, p);
    const be = decimal.minimalBigEndian(r.unscaled);
    try bytesOf(out, be.bytes[be.start..]);
}

fn decimalTooWide(f: types.Schema.Field, p: u8) anyerror {
    return stream.explainf(error.AvroDecimalOverflow, "a value of column `{s}` does not fit its DECIMAL({d},{d})", .{ f.name, p, f.ty.scale });
}

pub fn compress(arena: std.mem.Allocator, codec: Codec, body: []const u8) ![]const u8 {
    switch (codec) {
        .null => return body,
        .deflate => {
            var aw: std.Io.Writer.Allocating = .init(arena);
            const z = try deflate.Deflate.init(pagecache.process, &aw.writer, .{ .level = .fast, .container = .raw });
            defer z.deinit(pagecache.process);
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
        .decimal => |d| decimal.rescaleTo(d, ty.scale).?.order(got.decimal) == .eq,
        .string => |s| std.mem.eql(u8, s, got.string),
        .bytes => |s| std.mem.eql(u8, s, got.bytes),
        .nested => |r| blk: {
            const nested = @import("../../exec/nested.zig");
            var ar = std.heap.ArenaAllocator.init(testing.allocator);
            defer ar.deinit();
            const x = nested.jsonText(ar.allocator(), r) catch break :blk false;
            const y = nested.jsonText(ar.allocator(), got.nested) catch break :blk false;
            break :blk std.mem.eql(u8, x, y);
        },
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

test "a map keyed by other than STRING, a name Avro forbids and a zstandard write are refused naming why" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const int_keys = types.Type{ .kind = .map, .fields = &.{ .{ .name = "key", .ty = types.Type.init(.int) }, .{ .name = "value", .ty = types.Type.init(.int) } } };
    const nested = types.Schema{ .fields = &.{.{ .name = "x", .ty = int_keys }} };
    try testing.expectEqual(error.AvroUnsupportedType, schemaProblem(nested).?);
    try testing.expect(std.mem.indexOf(u8, eval.takeFailure(error.AvroUnsupportedType).?, "`x`") != null);
    const bad_field = types.Type{ .kind = .@"struct", .fields = &.{.{ .name = "a b", .ty = types.Type.init(.int) }} };
    try testing.expectEqual(error.AvroBadName, schemaProblem(.{ .fields = &.{.{ .name = "s", .ty = bad_field }} }).?);
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
        try testing.expect((try read.decimalOf(out.items[1..], 38, c[1])).decimal.order(decimal.rescaleTo(c[0], c[1]).?) == .eq);
    }
    var out = std.array_list.Managed(u8).init(a);
    try testing.expectError(error.AvroDecimalOverflow, decimalBytes(&out, .{ .unscaled = 1000, .scale = 0 }, .{ .name = "d", .ty = types.Type.decimal(3, 0) }));
    try decimalBytes(&out, .{ .unscaled = 999, .scale = 0 }, .{ .name = "d", .ty = types.Type.decimal(3, 0) });
}

fn randomValue(r: std.Random, a: std.mem.Allocator, ty: types.Type) !Value {
    if (r.uintLessThan(u8, 6) == 0) return .null;
    const edge = r.uintLessThan(u8, 4) == 0;
    return switch (ty.kind) {
        .bool => .{ .bool = r.boolean() },
        .int => .{ .int = if (edge) (if (r.boolean()) std.math.minInt(i64) else std.math.maxInt(i64)) else r.int(i64) >> r.int(u6) },
        .float => .{ .float = if (edge) ([_]f64{ std.math.nan(f64), std.math.inf(f64), -std.math.inf(f64), -0.0, 5e-324 })[r.uintLessThan(usize, 5)] else @bitCast(r.int(u64)) },
        .decimal => blk: {
            const bound = pow10_i128[ty.precision];
            const v: i128 = if (edge) (if (r.boolean()) bound - 1 else 1 - bound) else @rem(@as(i128, @bitCast(r.int(u128))), bound);
            break :blk .{ .decimal = .{ .unscaled = v, .scale = ty.scale } };
        },
        .string, .bytes => blk: {
            const s = try a.alloc(u8, r.uintLessThan(usize, 40));
            r.bytes(s);
            break :blk if (ty.kind == .string) Value{ .string = s } else Value{ .bytes = s };
        },
        .date => .{ .date = if (edge) std.math.minInt(i32) + 1 else r.int(i32) },
        .time => .{ .time = r.intRangeLessThan(i64, 0, civil.us_per_day) },
        .timestamp => .{ .timestamp = if (edge) (if (r.boolean()) std.math.minInt(i64) else std.math.maxInt(i64)) else r.int(i64) },
        .array, .@"struct", .map => unreachable,
    };
}

test "fuzz: random tables of every type round-trip through each codec, any batch split" {
    const kinds = [_]types.TypeKind{ .bool, .int, .float, .decimal, .string, .bytes, .date, .time, .timestamp };
    for (0..@import("testing_util.zig").iters(150)) |seed| {
        var ar = std.heap.ArenaAllocator.init(testing.allocator);
        defer ar.deinit();
        const a = ar.allocator();
        var prng = std.Random.DefaultPrng.init(0xa5_e000 + seed);
        const r = prng.random();
        var tmp = testing.tmpDir(.{});
        defer tmp.cleanup();
        const fields = try a.alloc(types.Schema.Field, 1 + r.uintLessThan(usize, 6));
        for (fields, 0..) |*f, i| {
            const k = kinds[r.uintLessThan(usize, kinds.len)];
            const p = 1 + r.uintLessThan(u8, 38);
            f.* = .{ .name = try msgfmt.allocPrint(a, "c_{d}", .{i}), .ty = (if (k == .decimal) types.Type.decimal(p, r.uintAtMost(u8, p)) else types.Type.init(k)).asNullable() };
        }
        const sch = try a.create(types.Schema);
        sch.* = .{ .fields = fields };
        const nbatches = r.uintLessThan(usize, 4);
        const batches = try a.alloc(Batch, nbatches);
        var total: usize = 0;
        for (batches) |*b| {
            const n = 1 + r.uintLessThan(usize, 50);
            const cols = try a.alloc(@import("../../exec/column.zig").Column, fields.len);
            for (cols, fields) |*c, f| {
                var bld = @import("../../exec/column.zig").Builder.init(a, f.ty);
                for (0..n) |_| try bld.append(try randomValue(r, a, f.ty));
                c.* = try bld.finish();
            }
            b.* = .{ .schema = sch, .columns = cols, .len = n };
            total += n;
        }
        const codec = ([_]Codec{ .null, .deflate, .snappy })[r.uintLessThan(usize, 3)];
        const path = try std.fs.path.join(a, &.{ try tmp.dir.realpathAlloc(a, "."), "p.avro" });
        const w = try Writer.open(a, path, sch.*, codec);
        for (batches) |b| try w.writeBatch(a, b);
        try w.close();
        const f = try read.Files.open(a, testing.allocator, &.{path}, .{ .batch_rows = 1 + r.uintLessThan(usize, 64) });
        defer f.close();
        for (sch.fields, f.schema.fields) |x, y| try testing.expect(types.Type.eql(x.ty, y.ty));
        var bi: usize = 0;
        var at: usize = 0;
        var seen: usize = 0;
        while (try f.next(a)) |got| {
            for (0..got.len) |row| {
                while (at == batches[bi].len) {
                    bi += 1;
                    at = 0;
                }
                for (batches[bi].columns, got.columns, sch.fields) |want, back, fld| {
                    if (!same(want.getValue(at), back.getValue(row), fld.ty)) {
                        std.debug.print("seed {d} {s}: wrote {any}, read {any}\n", .{ seed, fld.name, want.getValue(at), back.getValue(row) });
                        return error.TestUnexpectedResult;
                    }
                }
                at += 1;
                seen += 1;
            }
        }
        try testing.expectEqual(total, seen);
    }
}
