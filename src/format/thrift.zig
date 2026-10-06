//! Thrift compact protocol. Parquet serialises its footer and every page header
//! with it, so nothing in a Parquet file is reachable without it. `Reader` decodes;
//! `Writer` mirrors it for the metadata basalt writes.
//!
//! Two details are easy to get wrong and are handled explicitly:
//!   * field ids are deltas from the previous field within the same struct, so
//!     both sides keep a per-struct id stack rather than one running id; a delta
//!     of 1..15 packs into the header byte, anything else (including a lower id)
//!     takes the explicit zigzag form;
//!   * booleans carry their value in the field type (`bool_true`/`bool_false`)
//!     and occupy no bytes of their own.
//! Doubles are little-endian on the wire, unlike the binary protocol.
//!
//! `skip` is what makes this forward-compatible: Parquet gains metadata fields
//! over time, and a reader that cannot skip an unknown field cannot open a file
//! written by anything newer. The input is hostile, so the reader only ever
//! returns a value or `CorruptThrift`: nesting is capped at `max_depth` for
//! structs and containers alike (only structs were once capped, and a few hundred
//! KB of "list of list" bytes overflowed the stack), and a list header claiming
//! more elements than the input holds is refused before any loop runs.
//! `readBinary` slices borrow the input buffer.

const std = @import("std");

pub const Error = error{
    CorruptThrift,
};

pub const Type = enum(u8) {
    stop = 0,
    bool_true = 1,
    bool_false = 2,
    byte = 3,
    i16 = 4,
    i32 = 5,
    i64 = 6,
    double = 7,
    binary = 8,
    list = 9,
    set = 10,
    map = 11,
    @"struct" = 12,
    _,
};

pub const Field = struct { ty: Type, id: i16 };
pub const ListHeader = struct { elem: Type, size: usize };

pub const max_depth = 32;

pub const Reader = struct {
    buf: []const u8,
    pos: usize = 0,
    last_id: i16 = 0,
    id_stack: [max_depth]i16 = undefined,
    depth: usize = 0,

    pub fn init(buf: []const u8) Reader {
        return .{ .buf = buf };
    }

    fn take(self: *Reader, n: usize) Error![]const u8 {
        if (self.pos + n > self.buf.len) return Error.CorruptThrift;
        defer self.pos += n;
        return self.buf[self.pos..][0..n];
    }

    pub fn readByte(self: *Reader) Error!u8 {
        return (try self.take(1))[0];
    }

    pub fn readVarint(self: *Reader) Error!u64 {
        var v: u64 = 0;
        var shift: u6 = 0;
        while (true) {
            const b = try self.readByte();
            v |= @as(u64, b & 0x7F) << shift;
            if (b & 0x80 == 0) return v;
            shift = std.math.add(u6, shift, 7) catch return Error.CorruptThrift;
        }
    }

    pub fn readZigZag(self: *Reader) Error!i64 {
        const u = try self.readVarint();
        return @as(i64, @bitCast(u >> 1)) ^ -@as(i64, @intCast(u & 1));
    }

    pub fn readI32(self: *Reader) Error!i32 {
        const v = try self.readZigZag();
        if (v < std.math.minInt(i32) or v > std.math.maxInt(i32)) return Error.CorruptThrift;
        return @intCast(v);
    }

    pub fn readDouble(self: *Reader) Error!f64 {
        const b = try self.take(8);
        return @bitCast(std.mem.readInt(u64, b[0..8], .little));
    }

    pub fn readBinary(self: *Reader) Error![]const u8 {
        const n = try self.readVarint();
        if (n > self.buf.len) return Error.CorruptThrift;
        return self.take(@intCast(n));
    }

    pub fn readField(self: *Reader) Error!Field {
        const b = try self.readByte();
        if (b == 0) return .{ .ty = .stop, .id = 0 };
        const ty: Type = @enumFromInt(b & 0x0F);
        const delta: i16 = @intCast((b >> 4) & 0x0F);
        const id = if (delta == 0) blk: {
            const v = try self.readZigZag();
            if (v < std.math.minInt(i16) or v > std.math.maxInt(i16)) return Error.CorruptThrift;
            break :blk @as(i16, @intCast(v));
        } else self.last_id + delta;
        self.last_id = id;
        return .{ .ty = ty, .id = id };
    }

    pub fn structBegin(self: *Reader) Error!void {
        if (self.depth >= max_depth) return Error.CorruptThrift;
        self.id_stack[self.depth] = self.last_id;
        self.depth += 1;
        self.last_id = 0;
    }

    pub fn structEnd(self: *Reader) Error!void {
        if (self.depth == 0) return Error.CorruptThrift;
        self.depth -= 1;
        self.last_id = self.id_stack[self.depth];
    }

    pub fn readListHeader(self: *Reader) Error!ListHeader {
        const b = try self.readByte();
        const elem: Type = @enumFromInt(b & 0x0F);
        var size: usize = (b >> 4) & 0x0F;
        if (size == 15) {
            const n = try self.readVarint();
            if (n > self.buf.len) return Error.CorruptThrift;
            size = @intCast(n);
        }
        return .{ .elem = elem, .size = size };
    }

    pub fn skip(self: *Reader, ty: Type) Error!void {
        switch (ty) {
            .bool_true, .bool_false, .stop => {},
            .byte => _ = try self.readByte(),
            .i16, .i32, .i64 => _ = try self.readZigZag(),
            .double => _ = try self.take(8),
            .binary => _ = try self.readBinary(),
            .list, .set => {
                const h = try self.readListHeader();
                if (h.size == 0) return;
                if (self.depth >= max_depth) return Error.CorruptThrift;
                self.depth += 1;
                defer self.depth -= 1;
                for (0..h.size) |_| try self.skip(h.elem);
            },
            .map => {
                const n = try self.readVarint();
                if (n > 0) {
                    const kv = try self.readByte();
                    const k: Type = @enumFromInt((kv >> 4) & 0x0F);
                    const v: Type = @enumFromInt(kv & 0x0F);
                    if (self.depth >= max_depth) return Error.CorruptThrift;
                    self.depth += 1;
                    defer self.depth -= 1;
                    for (0..@as(usize, @intCast(n))) |_| {
                        try self.skip(k);
                        try self.skip(v);
                    }
                }
            },
            .@"struct" => try self.skipStruct(),
            else => return Error.CorruptThrift,
        }
    }

    pub fn skipStruct(self: *Reader) Error!void {
        try self.structBegin();
        while (true) {
            const f = try self.readField();
            if (f.ty == .stop) break;
            try self.skip(f.ty);
        }
        try self.structEnd();
    }
};

const t = std.testing;

test "varint and zigzag round-trip the boundary values" {
    var r = Reader.init(&[_]u8{ 0x00, 0x01, 0x7f, 0x80, 0x01, 0xff, 0xff, 0x03 });
    try t.expectEqual(@as(u64, 0), try r.readVarint());
    try t.expectEqual(@as(u64, 1), try r.readVarint());
    try t.expectEqual(@as(u64, 127), try r.readVarint());
    try t.expectEqual(@as(u64, 128), try r.readVarint());
    try t.expectEqual(@as(u64, 65535), try r.readVarint());

    var z = Reader.init(&[_]u8{ 0x00, 0x01, 0x02, 0x03, 0x04 });
    try t.expectEqual(@as(i64, 0), try z.readZigZag());
    try t.expectEqual(@as(i64, -1), try z.readZigZag());
    try t.expectEqual(@as(i64, 1), try z.readZigZag());
    try t.expectEqual(@as(i64, -2), try z.readZigZag());
    try t.expectEqual(@as(i64, 2), try z.readZigZag());
}

test "field ids accumulate as deltas, and a zero delta means an explicit id" {
    var r = Reader.init(&[_]u8{ 0x15, 0x02, 0x25, 0x04, 0x05, 0x28, 0x06, 0x00 });
    try r.structBegin();

    const f1 = try r.readField();
    try t.expectEqual(Type.i32, f1.ty);
    try t.expectEqual(@as(i16, 1), f1.id);
    try t.expectEqual(@as(i32, 1), try r.readI32());

    const f2 = try r.readField();
    try t.expectEqual(@as(i16, 3), f2.id);
    try t.expectEqual(@as(i32, 2), try r.readI32());

    const f3 = try r.readField();
    try t.expectEqual(@as(i16, 20), f3.id);
    try t.expectEqual(@as(i32, 3), try r.readI32());

    try t.expectEqual(Type.stop, (try r.readField()).ty);
    try r.structEnd();
}

test "booleans carry their value in the type and consume no bytes" {
    var r = Reader.init(&[_]u8{ 0x11, 0x12, 0x00 });
    try r.structBegin();
    const a = try r.readField();
    try t.expectEqual(Type.bool_true, a.ty);
    try t.expectEqual(@as(i16, 1), a.id);
    const b = try r.readField();
    try t.expectEqual(Type.bool_false, b.ty);
    try t.expectEqual(@as(i16, 2), b.id);
    try t.expectEqual(Type.stop, (try r.readField()).ty);
    try r.structEnd();
}

test "nested structs restart field-id deltas and restore the outer id" {
    var r = Reader.init(&[_]u8{ 0x1c, 0x15, 0x02, 0x00, 0x15, 0x04, 0x00 });
    try r.structBegin();
    const outer1 = try r.readField();
    try t.expectEqual(Type.@"struct", outer1.ty);
    try t.expectEqual(@as(i16, 1), outer1.id);

    try r.structBegin();
    const inner = try r.readField();
    try t.expectEqual(@as(i16, 1), inner.id);
    try t.expectEqual(@as(i32, 1), try r.readI32());
    try t.expectEqual(Type.stop, (try r.readField()).ty);
    try r.structEnd();

    const outer2 = try r.readField();
    try t.expectEqual(@as(i16, 2), outer2.id);
    try t.expectEqual(@as(i32, 2), try r.readI32());
    try r.structEnd();
}

test "short list header inlines the size; 15 escapes to a varint" {
    var r = Reader.init(&[_]u8{0x38});
    const h = try r.readListHeader();
    try t.expectEqual(Type.binary, h.elem);
    try t.expectEqual(@as(usize, 3), h.size);

    var backing: [34]u8 = undefined;
    @memset(&backing, 0x02);
    backing[0] = 0xf5;
    backing[1] = 0x20;
    var big = Reader.init(&backing);
    const h2 = try big.readListHeader();
    try t.expectEqual(Type.i32, h2.elem);
    try t.expectEqual(@as(usize, 32), h2.size);

    var lying = Reader.init(&[_]u8{ 0xf5, 0x80, 0x80, 0x04 });
    try t.expectError(Error.CorruptThrift, lying.readListHeader());
}

test "binary borrows from the buffer without copying" {
    const buf: []const u8 = "\x05hello";
    var r = Reader.init(buf);
    const got = try r.readBinary();
    try t.expectEqualStrings("hello", got);
    try t.expectEqual(buf.ptr + 1, got.ptr);
}

test "double is little-endian, unlike the binary protocol" {
    var r = Reader.init(&[_]u8{ 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f });
    try t.expectEqual(@as(f64, 1.0), try r.readDouble());
}

test "skip walks past every type, including nested lists and structs" {
    const bytes = [_]u8{ 0x19, 0x1c, 0x15, 0x02, 0x00 } ++
        [_]u8{ 0x18, 0x03, 'a', 'b', 'c' } ++
        [_]u8{ 0x17, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f } ++
        [_]u8{ 0x16, 0xff, 0x01 } ++
        [_]u8{ 0x1b, 0x02, 0x85, 0x01, 'k', 0x02, 0x01, 'j', 0x04 } ++
        [_]u8{ 0x1a, 0x25, 0x02, 0x04 } ++
        [_]u8{ 0x11, 0x12 } ++
        [_]u8{ 0x13, 0x7f } ++
        [_]u8{ 0x14, 0x06 } ++
        [_]u8{ 0x1b, 0x00 } ++
        [_]u8{0x00};
    var r = Reader.init(&bytes);
    try r.structBegin();
    var seen: i16 = 0;
    while (true) {
        const f = try r.readField();
        if (f.ty == .stop) break;
        seen += 1;
        try t.expectEqual(seen, f.id);
        try r.skip(f.ty);
    }
    try r.structEnd();
    try t.expectEqual(@as(i16, 11), seen);
    try t.expectEqual(bytes.len, r.pos);
}

test "truncated input errors instead of reading past the buffer" {
    var r = Reader.init(&[_]u8{0x18});
    try t.expectError(Error.CorruptThrift, r.skip(.binary));

    var v = Reader.init(&[_]u8{0x80});
    try t.expectError(Error.CorruptThrift, v.readVarint());

    var b = Reader.init(&[_]u8{ 0x05, 'h' });
    try t.expectError(Error.CorruptThrift, b.readBinary());
}

test "nesting deeper than max_depth is an error, not a stack overflow" {
    var buf: [max_depth + 8]u8 = undefined;
    @memset(&buf, 0x1c);
    var r = Reader.init(&buf);
    try t.expectError(Error.CorruptThrift, r.skipStruct());
}

test "nested lists are capped too, not just structs" {
    var deep: [max_depth + 8]u8 = undefined;
    @memset(&deep, 0x19);
    deep[deep.len - 1] = 0x09;
    var r = Reader.init(&deep);
    try t.expectError(Error.CorruptThrift, r.skip(.list));

    var ok = [_]u8{ 0x19, 0x19, 0x09 };
    var r2 = Reader.init(&ok);
    try r2.skip(.list);
}

pub const Writer = struct {
    out: *std.array_list.Managed(u8),
    last_id: i16 = 0,
    id_stack: [max_depth]i16 = undefined,
    depth: usize = 0,

    pub fn init(out: *std.array_list.Managed(u8)) Writer {
        return .{ .out = out };
    }

    pub fn writeVarint(self: *Writer, v_in: u64) !void {
        var v = v_in;
        while (true) {
            const b: u8 = @intCast(v & 0x7F);
            v >>= 7;
            try self.out.append(if (v != 0) b | 0x80 else b);
            if (v == 0) return;
        }
    }

    pub fn writeZigZag(self: *Writer, v: i64) !void {
        try self.writeVarint(@bitCast((v >> 63) ^ (v << 1)));
    }

    pub fn structBegin(self: *Writer) !void {
        if (self.depth >= max_depth) return error.CorruptThrift;
        self.id_stack[self.depth] = self.last_id;
        self.depth += 1;
        self.last_id = 0;
    }

    pub fn structEnd(self: *Writer) !void {
        try self.out.append(0);
        if (self.depth == 0) return error.CorruptThrift;
        self.depth -= 1;
        self.last_id = self.id_stack[self.depth];
    }

    pub fn fieldBegin(self: *Writer, ty: Type, id: i16) !void {
        const delta = id - self.last_id;
        if (delta > 0 and delta <= 15) {
            try self.out.append(@intCast((@as(u8, @intCast(delta)) << 4) | @intFromEnum(ty)));
        } else {
            try self.out.append(@intFromEnum(ty));
            try self.writeZigZag(id);
        }
        self.last_id = id;
    }

    pub fn writeI32(self: *Writer, id: i16, v: i32) !void {
        try self.fieldBegin(.i32, id);
        try self.writeZigZag(v);
    }

    pub fn writeI64(self: *Writer, id: i16, v: i64) !void {
        try self.fieldBegin(.i64, id);
        try self.writeZigZag(v);
    }

    pub fn writeBool(self: *Writer, id: i16, v: bool) !void {
        try self.fieldBegin(if (v) .bool_true else .bool_false, id);
    }

    pub fn writeBinary(self: *Writer, id: i16, v: []const u8) !void {
        try self.fieldBegin(.binary, id);
        try self.writeVarint(v.len);
        try self.out.appendSlice(v);
    }

    pub fn listBegin(self: *Writer, id: i16, elem: Type, size: usize) !void {
        try self.fieldBegin(.list, id);
        if (size < 15) {
            try self.out.append(@intCast((@as(u8, @intCast(size)) << 4) | @intFromEnum(elem)));
        } else {
            try self.out.append(0xF0 | @intFromEnum(elem));
            try self.writeVarint(size);
        }
    }
};

test "writer output round-trips through the reader" {
    var buf = std.array_list.Managed(u8).init(t.allocator);
    defer buf.deinit();
    var w = Writer.init(&buf);

    try w.structBegin();
    try w.writeI32(1, 7);
    try w.writeI64(3, -12345);
    try w.writeBool(4, true);
    try w.writeBool(5, false);
    try w.writeBinary(20, "hello");
    try w.writeI32(21, std.math.minInt(i32));
    try w.writeI64(40, std.math.maxInt(i64));
    try w.writeBinary(41, "");
    try w.structEnd();

    var r = Reader.init(buf.items);
    try r.structBegin();
    const f1 = try r.readField();
    try t.expectEqual(Type.i32, f1.ty);
    try t.expectEqual(@as(i16, 1), f1.id);
    try t.expectEqual(@as(i32, 7), try r.readI32());
    const f2 = try r.readField();
    try t.expectEqual(Type.i64, f2.ty);
    try t.expectEqual(@as(i16, 3), f2.id);
    try t.expectEqual(@as(i64, -12345), try r.readZigZag());
    const f3 = try r.readField();
    try t.expectEqual(Type.bool_true, f3.ty);
    try t.expectEqual(@as(i16, 4), f3.id);
    const f4 = try r.readField();
    try t.expectEqual(Type.bool_false, f4.ty);
    try t.expectEqual(@as(i16, 5), f4.id);
    const f5 = try r.readField();
    try t.expectEqual(Type.binary, f5.ty);
    try t.expectEqual(@as(i16, 20), f5.id);
    try t.expectEqualStrings("hello", try r.readBinary());
    const f6 = try r.readField();
    try t.expectEqual(Type.i32, f6.ty);
    try t.expectEqual(@as(i16, 21), f6.id);
    try t.expectEqual(@as(i32, std.math.minInt(i32)), try r.readI32());
    const f7 = try r.readField();
    try t.expectEqual(Type.i64, f7.ty);
    try t.expectEqual(@as(i16, 40), f7.id);
    try t.expectEqual(@as(i64, std.math.maxInt(i64)), try r.readZigZag());
    const f8 = try r.readField();
    try t.expectEqual(Type.binary, f8.ty);
    try t.expectEqual(@as(i16, 41), f8.id);
    try t.expectEqualStrings("", try r.readBinary());
    try t.expectEqual(Type.stop, (try r.readField()).ty);
    try r.structEnd();
    try t.expectEqual(buf.items.len, r.pos);
}

test "lists round-trip in both the short and escaped size forms" {
    for ([_]usize{ 3, 40 }) |n| {
        var buf = std.array_list.Managed(u8).init(t.allocator);
        defer buf.deinit();
        var w = Writer.init(&buf);
        try w.structBegin();
        try w.listBegin(2, .i32, n);
        for (0..n) |i| try w.writeZigZag(@intCast(i));
        try w.structEnd();

        var r = Reader.init(buf.items);
        try r.structBegin();
        _ = try r.readField();
        const h = try r.readListHeader();
        try t.expectEqual(Type.i32, h.elem);
        try t.expectEqual(n, h.size);
        for (0..n) |i| try t.expectEqual(@as(i64, @intCast(i)), try r.readZigZag());
    }
}

test "nested struct writing restores the outer field id" {
    var buf = std.array_list.Managed(u8).init(t.allocator);
    defer buf.deinit();
    var w = Writer.init(&buf);
    try w.structBegin();
    try w.fieldBegin(.@"struct", 1);
    try w.structBegin();
    try w.writeI32(1, 42);
    try w.structEnd();
    try w.writeI32(2, 99);
    try w.structEnd();

    var r = Reader.init(buf.items);
    try r.structBegin();
    try t.expectEqual(@as(i16, 1), (try r.readField()).id);
    try r.structBegin();
    try t.expectEqual(@as(i16, 1), (try r.readField()).id);
    try t.expectEqual(@as(i32, 42), try r.readI32());
    try t.expectEqual(Type.stop, (try r.readField()).ty);
    try r.structEnd();
    try t.expectEqual(@as(i16, 2), (try r.readField()).id);
    try t.expectEqual(@as(i32, 99), try r.readI32());
}

fn fuzzOne(_: void, input: []const u8) anyerror!void {
    var r = Reader.init(input);
    r.skipStruct() catch return;
}

const fuzzOne_corpus = [_][]const u8{
    @embedFile("testdata/uncompressed.parquet"),
};

test "fuzz: compact-protocol reader survives arbitrary bytes" {
    try std.testing.fuzz({}, fuzzOne, .{ .corpus = &fuzzOne_corpus });
    try @import("../net/fuzzutil.zig").pound(fuzzOne, &fuzzOne_corpus);
}
