//! Arrow IPC stream writer — the `--format arrow` stdout sink.
//!
//! The columnar store is already Arrow's layout for most of what it holds:
//! the validity bitmap is LSB-first with 1 = valid, strings and bytes are an
//! i32 offsets buffer over one values buffer, and the fixed-width stores are
//! contiguous host-order slices. Those are written as they are. Two kinds are
//! repacked per batch: bools (one byte per row here, one bit in Arrow) and
//! decimals (an i128 with a per-value scale here, sixteen bytes at the column's
//! declared scale in Arrow). Dates go out as Date32, times and timestamps in
//! microseconds, which is what the engine keeps.
//!
//! One record batch per engine batch, framed as the streaming format: a
//! continuation marker, the FlatBuffer message length padded to eight, the
//! message, then the body with every buffer padded to eight. A run that
//! produced no rows still yields a valid stream — the schema and the
//! end-of-stream marker — which is what lets a consumer distinguish an empty
//! result from a failed one. Every column is declared nullable, since the
//! engine's nullability flag is a plan-time promise and the bitmap is the
//! truth. Arrays and structs are refused; nothing produces them.

const std = @import("std");
const builtin = @import("builtin");
const types = @import("../lang/types.zig");
const column = @import("../exec/column.zig");
const Column = column.Column;
const Batch = @import("../exec/batch.zig").Batch;
const driver = @import("driver.zig");
const flatbuf = @import("flatbuf.zig");
const rescaleTo = @import("../exec/eval.zig").rescaleTo;

comptime {
    if (builtin.cpu.arch.endian() != .little)
        @compileError("the arrow writer emits host-order buffers and declares them little-endian");
}

const continuation: u32 = 0xFFFFFFFF;
const version_v5: i16 = 4;

const Header = enum(u8) { schema = 1, record_batch = 3 };

/// The Arrow `Type` union tags this writer emits.
const TypeTag = enum(u8) { int = 2, floating_point = 3, binary = 4, utf8 = 5, bool = 6, decimal = 7, date = 8, time = 9, timestamp = 10 };

pub const StreamWriter = struct {
    out: *std.Io.Writer,
    fb: flatbuf.Builder,
    gpa: std.mem.Allocator,

    /// Writes the schema message at once, so the stream is well-formed from
    /// the first byte even if no batch ever follows.
    pub fn init(gpa: std.mem.Allocator, out: *std.Io.Writer, schema: types.Schema) !StreamWriter {
        var self = StreamWriter{ .out = out, .fb = try flatbuf.Builder.init(gpa, 1024), .gpa = gpa };
        errdefer self.fb.deinit();
        try self.writeSchema(schema);
        return self;
    }

    pub fn deinit(self: *StreamWriter) void {
        self.fb.deinit();
    }

    /// End-of-stream marker, then flush.
    pub fn finish(self: *StreamWriter) !void {
        try self.out.writeInt(u32, continuation, .little);
        try self.out.writeInt(u32, 0, .little);
        try self.out.flush();
    }

    fn writeSchema(self: *StreamWriter, schema: types.Schema) !void {
        const fb = &self.fb;
        fb.reset();
        const offs = try self.gpa.alloc(u32, schema.fields.len);
        defer self.gpa.free(offs);
        for (schema.fields, offs) |f, *o| {
            const name = try fb.createString(f.name);
            const ty = try typeTable(fb, f.ty);
            const children = try fb.createOffsetVector(&.{});
            try fb.startTable(7);
            try fb.addOffset(0, name);
            try fb.addBool(1, true);
            try fb.addUnion(2, @intFromEnum(ty.tag), ty.off);
            try fb.addOffset(5, children);
            o.* = try fb.endTable();
        }
        const fields = try fb.createOffsetVector(offs);
        try fb.startTable(4);
        try fb.addInt(i16, 0, 0);
        try fb.addOffset(1, fields);
        const sch = try fb.endTable();
        const msg = try message(fb, .schema, sch, 0);
        try self.frame(try fb.finish(msg));
    }

    pub fn writeBatch(self: *StreamWriter, arena: std.mem.Allocator, batch: Batch) !void {
        const n = batch.len;
        if (n == 0) return;
        var body = Body{ .bufs = std.array_list.Managed([]const u8).init(arena), .meta = std.array_list.Managed(i64).init(arena) };
        var nodes = std.array_list.Managed(i64).init(arena);
        for (batch.columns) |*col| {
            const nb = (n + 7) / 8;
            if (col.validity.bits.len < nb) return error.ArrowValidityTooShort;
            const validity = col.validity.bits[0..nb];
            try nodes.appendSlice(&.{ @intCast(n), @intCast(n - validCount(validity, n)) });
            try body.push(validity);
            try pushData(arena, &body, col, n);
        }
        const fb = &self.fb;
        fb.reset();
        const nodes_vec = try fb.createI64PairVector(nodes.items);
        const bufs_vec = try fb.createI64PairVector(body.meta.items);
        try fb.startTable(5);
        try fb.addInt(i64, 0, @intCast(n));
        try fb.addOffset(1, nodes_vec);
        try fb.addOffset(2, bufs_vec);
        const rb = try fb.endTable();
        const msg = try message(fb, .record_batch, rb, body.len);
        try self.frame(try fb.finish(msg));
        for (body.bufs.items) |b| {
            try self.out.writeAll(b);
            try self.out.splatByteAll(0, pad8(b.len) - b.len);
        }
    }

    fn frame(self: *StreamWriter, meta: []const u8) !void {
        const padded = pad8(meta.len);
        try self.out.writeInt(u32, continuation, .little);
        try self.out.writeInt(u32, @intCast(padded), .little);
        try self.out.writeAll(meta);
        try self.out.splatByteAll(0, padded - meta.len);
    }
};

const Body = struct {
    bufs: std.array_list.Managed([]const u8),
    meta: std.array_list.Managed(i64),
    len: i64 = 0,

    fn push(self: *Body, bytes: []const u8) !void {
        try self.meta.appendSlice(&.{ self.len, @intCast(bytes.len) });
        try self.bufs.append(bytes);
        self.len += @intCast(pad8(bytes.len));
    }
};

fn pad8(n: usize) usize {
    return (n + 7) & ~@as(usize, 7);
}

fn validCount(bits: []const u8, n: usize) usize {
    var c: usize = 0;
    const full = n / 8;
    for (bits[0..full]) |b| c += @popCount(b);
    const rem: u3 = @intCast(n % 8);
    if (rem != 0) c += @popCount(bits[full] & ((@as(u8, 1) << rem) - 1));
    return c;
}

fn message(fb: *flatbuf.Builder, header: Header, off: u32, body_len: i64) !u32 {
    try fb.startTable(5);
    try fb.addInt(i16, 0, version_v5);
    try fb.addUnion(1, @intFromEnum(header), off);
    try fb.addInt(i64, 3, body_len);
    return fb.endTable();
}

fn typeTable(fb: *flatbuf.Builder, ty: types.Type) !struct { tag: TypeTag, off: u32 } {
    switch (ty.kind) {
        .bool => {
            try fb.startTable(0);
            return .{ .tag = .bool, .off = try fb.endTable() };
        },
        .int => {
            try fb.startTable(2);
            try fb.addInt(i32, 0, 64);
            try fb.addBool(1, true);
            return .{ .tag = .int, .off = try fb.endTable() };
        },
        .float => {
            try fb.startTable(1);
            try fb.addInt(i16, 0, 2);
            return .{ .tag = .floating_point, .off = try fb.endTable() };
        },
        .decimal => {
            try fb.startTable(3);
            try fb.addInt(i32, 0, if (ty.precision == 0) 38 else ty.precision);
            try fb.addInt(i32, 1, ty.scale);
            try fb.addInt(i32, 2, 128);
            return .{ .tag = .decimal, .off = try fb.endTable() };
        },
        .string => {
            try fb.startTable(0);
            return .{ .tag = .utf8, .off = try fb.endTable() };
        },
        .bytes => {
            try fb.startTable(0);
            return .{ .tag = .binary, .off = try fb.endTable() };
        },
        .date => {
            try fb.startTable(1);
            try fb.addInt(i16, 0, 0);
            return .{ .tag = .date, .off = try fb.endTable() };
        },
        .time => {
            try fb.startTable(2);
            try fb.addInt(i16, 0, 2);
            try fb.addInt(i32, 1, 64);
            return .{ .tag = .time, .off = try fb.endTable() };
        },
        .timestamp => {
            try fb.startTable(2);
            try fb.addInt(i16, 0, 2);
            return .{ .tag = .timestamp, .off = try fb.endTable() };
        },
        .array, .@"struct" => return error.ArrowUnsupportedType,
    }
}

fn store(data: Column.Data, comptime tag: std.meta.Tag(Column.Data)) !@FieldType(Column.Data, @tagName(tag)) {
    if (data != tag) return error.ArrowColumnStoreMismatch;
    return @field(data, @tagName(tag));
}

fn pushData(arena: std.mem.Allocator, body: *Body, col: *const Column, n: usize) !void {
    switch (col.ty.kind) {
        .bool => {
            const src = try store(col.data, .b);
            const bits = try arena.alloc(u8, (n + 7) / 8);
            @memset(bits, 0);
            for (src[0..n], 0..) |v, i| {
                if (v) bits[i >> 3] |= @as(u8, 1) << @intCast(i & 7);
            }
            try body.push(bits);
        },
        .int, .time, .timestamp => try body.push(std.mem.sliceAsBytes((try store(col.data, .i64))[0..n])),
        .float => try body.push(std.mem.sliceAsBytes((try store(col.data, .f64))[0..n])),
        .date => try body.push(std.mem.sliceAsBytes((try store(col.data, .i32))[0..n])),
        .decimal => {
            const src = try store(col.data, .dec);
            const out = try arena.alloc(u8, n * 16);
            @memset(out, 0);
            for (src[0..n], 0..) |d, i| {
                if (!col.validity.get(i)) continue;
                const r = rescaleTo(d, col.ty.scale) orelse return error.DecimalOverflow;
                std.mem.writeInt(i128, out[i * 16 ..][0..16], r.unscaled, .little);
            }
            try body.push(out);
        },
        .string, .bytes => {
            const b = try store(col.data, .bytes);
            if (b.offsets.len < n + 1) return error.ArrowOffsetsTooShort;
            const offs = b.offsets[0 .. n + 1];
            const base: usize = @intCast(offs[0]);
            const end: usize = @intCast(offs[n]);
            if (base == 0) {
                try body.push(std.mem.sliceAsBytes(offs));
            } else {
                const rebased = try arena.alloc(i32, n + 1);
                for (offs, rebased) |o, *r| r.* = o - offs[0];
                try body.push(std.mem.sliceAsBytes(rebased));
            }
            try body.push(b.values[base..end]);
        },
        .array, .@"struct" => return error.ArrowUnsupportedType,
    }
}

/// The stdout sink: an Arrow stream over a buffered stdout writer.
pub const ArrowWriter = struct {
    gpa: std.mem.Allocator,
    buf: [1 << 16]u8 = undefined,
    fw: std.fs.File.Writer = undefined,
    sw: StreamWriter = undefined,

    pub fn open(gpa: std.mem.Allocator, schema: types.Schema) !*ArrowWriter {
        const self = try gpa.create(ArrowWriter);
        errdefer gpa.destroy(self);
        self.* = .{ .gpa = gpa };
        self.fw = std.fs.File.stdout().writerStreaming(&self.buf);
        self.sw = try StreamWriter.init(gpa, &self.fw.interface, schema);
        return self;
    }

    pub fn writeBatch(self: *ArrowWriter, arena: std.mem.Allocator, batch: Batch) !void {
        try self.sw.writeBatch(arena, batch);
    }

    pub fn close(self: *ArrowWriter) !void {
        defer self.deinit();
        try self.sw.finish();
    }

    /// Failure path: release without flushing the tail of the stream.
    pub fn abort(self: *ArrowWriter) void {
        self.deinit();
    }

    fn deinit(self: *ArrowWriter) void {
        self.sw.deinit();
        self.gpa.destroy(self);
    }

    pub fn sink(self: *ArrowWriter) driver.Sink {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = driver.sinkVTable(ArrowWriter);
};

// --- tests -----------------------------------------------------------------

const Frame = struct { meta: []const u8, body: []const u8, next: usize };

fn readFrame(bytes: []const u8, at: usize) Frame {
    std.debug.assert(std.mem.readInt(u32, bytes[at..][0..4], .little) == continuation);
    const ml: usize = @intCast(std.mem.readInt(u32, bytes[at + 4 ..][0..4], .little));
    const meta = bytes[at + 8 ..][0..ml];
    const msg = flatbuf.Table.root(meta);
    const body_len: usize = @intCast(msg.int(i64, 3, 0));
    const body_at = at + 8 + ml;
    return .{ .meta = meta, .body = bytes[body_at..][0..body_len], .next = body_at + body_len };
}

const test_fields = [_]types.Schema.Field{
    .{ .name = "id", .ty = .{ .kind = .int, .nullable = true } },
    .{ .name = "nome", .ty = .{ .kind = .string, .nullable = true } },
    .{ .name = "ok", .ty = .{ .kind = .bool } },
    .{ .name = "valor", .ty = .{ .kind = .decimal, .precision = 18, .scale = 2 } },
    .{ .name = "dia", .ty = .{ .kind = .date } },
    .{ .name = "ts", .ty = .{ .kind = .timestamp } },
    .{ .name = "x", .ty = .{ .kind = .float } },
    .{ .name = "hora", .ty = .{ .kind = .time } },
};

fn testSchema() types.Schema {
    return .{ .fields = &test_fields };
}

fn testBatch(arena: std.mem.Allocator, schema: *const types.Schema) !Batch {
    const V = @import("../exec/value.zig").Value;
    const cols = try arena.alloc(Column, schema.fields.len);
    const rows = [_][8]V{
        .{ .{ .int = 1 }, .{ .string = "ana" }, .{ .bool = true }, .{ .decimal = .{ .unscaled = 1050, .scale = 2 } }, .{ .date = 20000 }, .{ .timestamp = 1_700_000_000_000_000 }, .{ .float = 1.5 }, .{ .time = 3_600_000_000 } },
        .{ .null, .null, .{ .bool = false }, .{ .decimal = .{ .unscaled = 3, .scale = 0 } }, .{ .date = 20001 }, .{ .timestamp = 1_700_000_000_000_001 }, .{ .float = -2.25 }, .{ .time = 0 } },
        .{ .{ .int = 3 }, .{ .string = "bob" }, .{ .bool = true }, .{ .decimal = .{ .unscaled = -12345, .scale = 3 } }, .{ .date = 20002 }, .{ .timestamp = 0 }, .{ .float = 0 }, .{ .time = 86_399_000_000 } },
    };
    for (schema.fields, cols) |f, *c| {
        var b = column.Builder.init(arena, f.ty);
        for (rows) |r| try b.append(r[schema.indexOf(f.name).?]);
        c.* = try b.finish();
    }
    return .{ .schema = schema, .columns = cols, .len = rows.len };
}

test "stream: schema message declares every column's arrow type" {
    var aw = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer aw.deinit();
    var sw = try StreamWriter.init(std.testing.allocator, &aw.writer, testSchema());
    defer sw.deinit();
    try sw.finish();

    const bytes = aw.written();
    const f0 = readFrame(bytes, 0);
    try std.testing.expectEqual(@as(usize, 0), f0.meta.len % 8);
    const msg = flatbuf.Table.root(f0.meta);
    try std.testing.expectEqual(@as(i16, version_v5), msg.int(i16, 0, 0));
    try std.testing.expectEqual(@intFromEnum(Header.schema), msg.int(u8, 1, 0));
    const schema = msg.table(2).?;
    const fields = schema.vector(1).?;
    try std.testing.expectEqual(@as(usize, 8), fields.len);

    const want = [_]struct { name: []const u8, tag: TypeTag }{
        .{ .name = "id", .tag = .int },           .{ .name = "nome", .tag = .utf8 },
        .{ .name = "ok", .tag = .bool },          .{ .name = "valor", .tag = .decimal },
        .{ .name = "dia", .tag = .date },         .{ .name = "ts", .tag = .timestamp },
        .{ .name = "x", .tag = .floating_point }, .{ .name = "hora", .tag = .time },
    };
    for (want, 0..) |w, i| {
        const fld = schema.tableAt(fields.at, i);
        try std.testing.expectEqualStrings(w.name, fld.string(0).?);
        try std.testing.expect(fld.int(u8, 1, 0) == 1);
        try std.testing.expectEqual(@intFromEnum(w.tag), fld.int(u8, 2, 0));
        try std.testing.expectEqual(@as(usize, 0), fld.vector(5).?.len);
    }
    const int_t = schema.tableAt(fields.at, 0).table(3).?;
    try std.testing.expectEqual(@as(i32, 64), int_t.int(i32, 0, 0));
    try std.testing.expectEqual(@as(u8, 1), int_t.int(u8, 1, 0));
    const dec_t = schema.tableAt(fields.at, 3).table(3).?;
    try std.testing.expectEqual(@as(i32, 18), dec_t.int(i32, 0, 0));
    try std.testing.expectEqual(@as(i32, 2), dec_t.int(i32, 1, 0));
    const time_t = schema.tableAt(fields.at, 7).table(3).?;
    try std.testing.expectEqual(@as(i16, 2), time_t.int(i16, 0, 0));
    try std.testing.expectEqual(@as(i32, 64), time_t.int(i32, 1, 32));

    // An empty result is schema + end-of-stream, nothing else.
    try std.testing.expectEqual(bytes.len, f0.next + 8);
    try std.testing.expectEqual(continuation, std.mem.readInt(u32, bytes[f0.next..][0..4], .little));
    try std.testing.expectEqual(@as(u32, 0), std.mem.readInt(u32, bytes[f0.next + 4 ..][0..4], .little));
}

test "stream: a record batch carries validity, repacked bools and decimals, raw fixed and bytes buffers" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const schema = testSchema();
    const batch = try testBatch(a, &schema);

    var aw = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer aw.deinit();
    var sw = try StreamWriter.init(std.testing.allocator, &aw.writer, schema);
    defer sw.deinit();
    try sw.writeBatch(a, batch);
    try sw.writeBatch(a, .{ .schema = &schema, .columns = batch.columns, .len = 0 });
    try sw.finish();

    const bytes = aw.written();
    const f0 = readFrame(bytes, 0);
    const f1 = readFrame(bytes, f0.next);
    try std.testing.expectEqual(bytes.len, f1.next + 8);
    try std.testing.expectEqual(@as(usize, 0), f1.body.len % 8);

    const msg = flatbuf.Table.root(f1.meta);
    try std.testing.expectEqual(@intFromEnum(Header.record_batch), msg.int(u8, 1, 0));
    const rb = msg.table(2).?;
    try std.testing.expectEqual(@as(i64, 3), rb.int(i64, 0, 0));
    const nodes = rb.vector(1).?;
    try std.testing.expectEqual(@as(usize, 8), nodes.len);
    try std.testing.expectEqual(@as(i64, 3), rb.i64At(nodes.at));
    try std.testing.expectEqual(@as(i64, 1), rb.i64At(nodes.at + 8));
    try std.testing.expectEqual(@as(i64, 0), rb.i64At(nodes.at + 16 * 2 + 8));
    const bufs = rb.vector(2).?;
    // 6 fixed-width columns × 2 buffers + 1 bytes column × 3 + bool × 2.
    try std.testing.expectEqual(@as(usize, 6 * 2 + 3 + 2), bufs.len);

    const Buf = struct { off: usize, len: usize };
    const bufAt = struct {
        fn f(t: flatbuf.Table, at: usize, i: usize) Buf {
            return .{ .off = @intCast(t.i64At(at + i * 16)), .len = @intCast(t.i64At(at + i * 16 + 8)) };
        }
    }.f;
    const body = f1.body;

    const id_valid = bufAt(rb, bufs.at, 0);
    try std.testing.expectEqual(@as(u8, 0b101), body[id_valid.off] & 0b111);
    const id_data = bufAt(rb, bufs.at, 1);
    try std.testing.expectEqual(@as(usize, 24), id_data.len);
    try std.testing.expectEqual(@as(i64, 3), std.mem.readInt(i64, body[id_data.off + 16 ..][0..8], .little));

    const nome_offs = bufAt(rb, bufs.at, 3);
    const nome_data = bufAt(rb, bufs.at, 4);
    try std.testing.expectEqual(@as(usize, 16), nome_offs.len);
    try std.testing.expectEqual(@as(i32, 6), std.mem.readInt(i32, body[nome_offs.off + 12 ..][0..4], .little));
    try std.testing.expectEqualStrings("anabob", body[nome_data.off..][0..nome_data.len]);

    const ok_data = bufAt(rb, bufs.at, 6);
    try std.testing.expectEqual(@as(u8, 0b101), body[ok_data.off]);

    const valor_data = bufAt(rb, bufs.at, 8);
    try std.testing.expectEqual(@as(usize, 48), valor_data.len);
    try std.testing.expectEqual(@as(i128, 1050), std.mem.readInt(i128, body[valor_data.off..][0..16], .little));
    try std.testing.expectEqual(@as(i128, 300), std.mem.readInt(i128, body[valor_data.off + 16 ..][0..16], .little));
    try std.testing.expectEqual(@as(i128, -1234), std.mem.readInt(i128, body[valor_data.off + 32 ..][0..16], .little));

    const dia_data = bufAt(rb, bufs.at, 10);
    try std.testing.expectEqual(@as(usize, 12), dia_data.len);
    try std.testing.expectEqual(@as(i32, 20002), std.mem.readInt(i32, body[dia_data.off + 8 ..][0..4], .little));

    const hora_data = bufAt(rb, bufs.at, 16);
    try std.testing.expectEqual(@as(i64, 86_399_000_000), std.mem.readInt(i64, body[hora_data.off + 16 ..][0..8], .little));
}
