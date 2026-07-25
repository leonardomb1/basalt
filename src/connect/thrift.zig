//! Thrift compact protocol, read side.
//!
//! Parquet serialises its footer and every page header with this, so nothing in
//! a Parquet file is reachable without it. Only decoding is implemented — basalt
//! reads Parquet, it does not write it.
//!
//! Two details are easy to get wrong and are handled explicitly here:
//!   * field ids are *deltas* from the previous field within the same struct, so
//!     the decoder keeps a per-struct stack rather than one running id;
//!   * booleans carry their value in the field *type* (`bool_true`/`bool_false`)
//!     and occupy no bytes of their own.
//!
//! `skip` is what makes this forward-compatible: Parquet gains metadata fields
//! over time, and a reader that cannot skip an unknown field cannot open a file
//! written by anything newer than itself.

const std = @import("std");

pub const Error = error{
    /// Truncated, malformed, or nested past `max_depth`.
    CorruptThrift,
};

/// Compact-protocol type ids, as they appear in field and list headers.
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

/// Nesting limit. Parquet metadata is only a few levels deep; the cap turns a
/// malicious or corrupt file into an error instead of a stack overflow.
pub const max_depth = 32;

pub const Reader = struct {
    buf: []const u8,
    pos: usize = 0,
    /// Field id the next delta is relative to, for the struct being read.
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

    /// Unsigned LEB128.
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

    /// Signed integers travel zigzag-encoded so small negatives stay short.
    pub fn readZigZag(self: *Reader) Error!i64 {
        const u = try self.readVarint();
        return @as(i64, @bitCast(u >> 1)) ^ -@as(i64, @intCast(u & 1));
    }

    pub fn readI32(self: *Reader) Error!i32 {
        const v = try self.readZigZag();
        if (v < std.math.minInt(i32) or v > std.math.maxInt(i32)) return Error.CorruptThrift;
        return @intCast(v);
    }

    /// Compact protocol stores doubles little-endian (unlike binary protocol,
    /// which is big-endian) — a classic source of garbage values.
    pub fn readDouble(self: *Reader) Error!f64 {
        const b = try self.take(8);
        return @bitCast(std.mem.readInt(u64, b[0..8], .little));
    }

    /// Borrowed from the input buffer; valid as long as the footer bytes are.
    pub fn readBinary(self: *Reader) Error![]const u8 {
        const n = try self.readVarint();
        if (n > self.buf.len) return Error.CorruptThrift;
        return self.take(@intCast(n));
    }

    /// Reads the next field header. `.stop` marks the end of the current struct.
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

    /// Enter a nested struct: field-id deltas restart from zero inside it.
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

    /// Skips one value of `ty`. Unknown fields must be skippable or a file
    /// written by a newer Parquet cannot be opened at all.
    pub fn skip(self: *Reader, ty: Type) Error!void {
        switch (ty) {
            // booleans are carried entirely by the type id
            .bool_true, .bool_false, .stop => {},
            .byte => _ = try self.readByte(),
            .i16, .i32, .i64 => _ = try self.readZigZag(),
            .double => _ = try self.take(8),
            .binary => _ = try self.readBinary(),
            .list, .set => {
                const h = try self.readListHeader();
                for (0..h.size) |_| try self.skip(h.elem);
            },
            .map => {
                const n = try self.readVarint();
                if (n > 0) {
                    const kv = try self.readByte();
                    const k: Type = @enumFromInt((kv >> 4) & 0x0F);
                    const v: Type = @enumFromInt(kv & 0x0F);
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

// --- tests ------------------------------------------------------------------

const t = std.testing;

test "varint and zigzag round-trip the boundary values" {
    var r = Reader.init(&[_]u8{ 0x00, 0x01, 0x7f, 0x80, 0x01, 0xff, 0xff, 0x03 });
    try t.expectEqual(@as(u64, 0), try r.readVarint());
    try t.expectEqual(@as(u64, 1), try r.readVarint());
    try t.expectEqual(@as(u64, 127), try r.readVarint());
    try t.expectEqual(@as(u64, 128), try r.readVarint());
    try t.expectEqual(@as(u64, 65535), try r.readVarint());

    // zigzag: 0,-1,1,-2,2 encode as 0,1,2,3,4
    var z = Reader.init(&[_]u8{ 0x00, 0x01, 0x02, 0x03, 0x04 });
    try t.expectEqual(@as(i64, 0), try z.readZigZag());
    try t.expectEqual(@as(i64, -1), try z.readZigZag());
    try t.expectEqual(@as(i64, 1), try z.readZigZag());
    try t.expectEqual(@as(i64, -2), try z.readZigZag());
    try t.expectEqual(@as(i64, 2), try z.readZigZag());
}

test "field ids accumulate as deltas, and a zero delta means an explicit id" {
    // 0x15 = delta 1, type i32(5); 0x25 = delta 2 -> id 3; then 0x05 = delta 0,
    // explicit zigzag id 20 (=40); then stop
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
    // field 1 bool_true (delta 1), field 2 bool_false (delta 1), stop
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
    // outer: field 1 = struct { field 1 = i32 }, then outer field 2
    var r = Reader.init(&[_]u8{ 0x1c, 0x15, 0x02, 0x00, 0x15, 0x04, 0x00 });
    try r.structBegin();
    const outer1 = try r.readField();
    try t.expectEqual(Type.@"struct", outer1.ty);
    try t.expectEqual(@as(i16, 1), outer1.id);

    try r.structBegin();
    const inner = try r.readField();
    try t.expectEqual(@as(i16, 1), inner.id); // restarted, not 2
    try t.expectEqual(@as(i32, 1), try r.readI32());
    try t.expectEqual(Type.stop, (try r.readField()).ty);
    try r.structEnd();

    // delta 1 applies to the outer struct's last id (1), giving 2
    const outer2 = try r.readField();
    try t.expectEqual(@as(i16, 2), outer2.id);
    try t.expectEqual(@as(i32, 2), try r.readI32());
    try r.structEnd();
}

test "short list header inlines the size; 15 escapes to a varint" {
    var r = Reader.init(&[_]u8{0x38}); // size 3, elem binary(8)
    const h = try r.readListHeader();
    try t.expectEqual(Type.binary, h.elem);
    try t.expectEqual(@as(usize, 3), h.size);

    // size nibble 15 escapes to a varint. The buffer must be able to hold the
    // elements it claims: readListHeader rejects a size larger than the input,
    // which is what stops a corrupt header from driving a huge loop.
    var backing: [34]u8 = undefined;
    @memset(&backing, 0x02);
    backing[0] = 0xf5;
    backing[1] = 0x20; // varint 32, elem i32
    var big = Reader.init(&backing);
    const h2 = try big.readListHeader();
    try t.expectEqual(Type.i32, h2.elem);
    try t.expectEqual(@as(usize, 32), h2.size);

    var lying = Reader.init(&[_]u8{ 0xf5, 0x80, 0x80, 0x04 }); // claims 65536 elems
    try t.expectError(Error.CorruptThrift, lying.readListHeader());
}

test "binary borrows from the buffer without copying" {
    var r = Reader.init("\x05hello");
    try t.expectEqualStrings("hello", try r.readBinary());
}

test "double is little-endian, unlike the binary protocol" {
    // 1.0 = 0x3FF0000000000000, little-endian on the wire
    var r = Reader.init(&[_]u8{ 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f });
    try t.expectEqual(@as(f64, 1.0), try r.readDouble());
}

test "skip walks past every type, including nested lists and structs" {
    // struct { 1: list<struct{1:i32}> [1 elem], 2: binary, 3: double } stop
    const bytes = [_]u8{
        0x19, 0x1c, // field 1, list; header: size 1, elem struct
        0x15, 0x02, 0x00, //   the element struct: field 1 i32 = 1, stop
        0x18, 0x03, 'a', 'b', 'c', // field 2, binary "abc"
        0x17, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, // field 3, double 1.0
        0x00, // stop
    };
    var r = Reader.init(&bytes);
    try r.structBegin();
    var seen: usize = 0;
    while (true) {
        const f = try r.readField();
        if (f.ty == .stop) break;
        try r.skip(f.ty);
        seen += 1;
    }
    try r.structEnd();
    try t.expectEqual(@as(usize, 3), seen);
    try t.expectEqual(bytes.len, r.pos);
}

test "truncated input errors instead of reading past the buffer" {
    var r = Reader.init(&[_]u8{0x18}); // binary field with no length byte
    try t.expectError(Error.CorruptThrift, r.skip(.binary));

    var v = Reader.init(&[_]u8{0x80}); // varint continuation with nothing after
    try t.expectError(Error.CorruptThrift, v.readVarint());

    var b = Reader.init(&[_]u8{ 0x05, 'h' }); // claims 5 bytes, supplies 1
    try t.expectError(Error.CorruptThrift, b.readBinary());
}

test "nesting deeper than max_depth is an error, not a stack overflow" {
    var buf: [max_depth + 8]u8 = undefined;
    @memset(&buf, 0x1c); // endless "field 1, struct"
    var r = Reader.init(&buf);
    try t.expectError(Error.CorruptThrift, r.skipStruct());
}
