//! FlatBuffers, write side, plus the few reads the tests need. Arrow IPC frames
//! every message's metadata as a FlatBuffer — the Schema, and one RecordBatch
//! header per batch — so nothing Arrow can be written without it. Only what
//! those two messages use is implemented: tables of scalars and offsets,
//! strings, vectors of offsets and of inline structs, and unions (a type byte
//! plus a table offset). No vtable deduplication, no file identifier, no shared
//! strings — a message here is a few hundred bytes.
//!
//! Layout rules that are easy to get wrong and are handled explicitly:
//!   * the buffer is built back to front, so a child is created before the
//!     table that references it (an offset only ever points forward);
//!   * every scalar is aligned to its own size relative to the END of the
//!     buffer, which is why `finish` pads to the largest alignment seen;
//!   * a table starts with a signed offset back to its vtable, and the vtable
//!     lists each field's offset from the table start (0 = absent).

const std = @import("std");

pub const Builder = struct {
    alloc: std.mem.Allocator,
    buf: []u8,
    head: usize,
    minalign: usize = 1,
    fields_buf: []u32 = &.{},
    nfields: usize = 0,
    object_end: u32 = 0,

    pub fn init(alloc: std.mem.Allocator, capacity: usize) !Builder {
        const buf = try alloc.alloc(u8, @max(capacity, 16));
        return .{ .alloc = alloc, .buf = buf, .head = buf.len };
    }

    pub fn deinit(self: *Builder) void {
        self.alloc.free(self.buf);
        self.alloc.free(self.fields_buf);
    }

    /// Forget everything written; the allocation is kept for the next message.
    pub fn reset(self: *Builder) void {
        self.head = self.buf.len;
        self.minalign = 1;
    }

    /// Bytes written so far, which is also the position of the last thing
    /// written measured from the end — the coordinate every offset speaks.
    pub fn offset(self: *const Builder) u32 {
        return @intCast(self.buf.len - self.head);
    }

    fn ensure(self: *Builder, need: usize) !void {
        if (self.head >= need) return;
        const used = self.buf.len - self.head;
        var cap = self.buf.len;
        while (cap - used < need) cap *= 2;
        const nb = try self.alloc.alloc(u8, cap);
        @memcpy(nb[cap - used ..], self.buf[self.head..]);
        self.alloc.free(self.buf);
        self.buf = nb;
        self.head = cap - used;
    }

    fn prep(self: *Builder, size: usize, additional: usize) !void {
        if (size > self.minalign) self.minalign = size;
        const used = self.buf.len - self.head + additional;
        const pad = (0 -% used) & (size - 1);
        try self.ensure(pad + size + additional);
        self.head -= pad;
        @memset(self.buf[self.head..][0..pad], 0);
    }

    fn place(self: *Builder, comptime T: type, x: T) void {
        self.head -= @sizeOf(T);
        std.mem.writeInt(T, self.buf[self.head..][0..@sizeOf(T)], x, .little);
    }

    fn prependInt(self: *Builder, comptime T: type, x: T) !void {
        try self.prep(@sizeOf(T), 0);
        self.place(T, x);
    }

    fn prependUOffset(self: *Builder, off: u32) !void {
        try self.prep(4, 0);
        self.place(u32, self.offset() - off + 4);
    }

    pub fn startTable(self: *Builder, nfields: usize) !void {
        if (self.fields_buf.len < nfields) {
            self.alloc.free(self.fields_buf);
            self.fields_buf = try self.alloc.alloc(u32, nfields);
        }
        self.nfields = nfields;
        @memset(self.fields_buf[0..nfields], 0);
        self.object_end = self.offset();
    }

    pub fn addInt(self: *Builder, comptime T: type, slot: usize, x: T) !void {
        try self.prependInt(T, x);
        self.fields_buf[slot] = self.offset();
    }

    pub fn addBool(self: *Builder, slot: usize, x: bool) !void {
        try self.addInt(u8, slot, @intFromBool(x));
    }

    pub fn addOffset(self: *Builder, slot: usize, off: u32) !void {
        try self.prependUOffset(off);
        self.fields_buf[slot] = self.offset();
    }

    /// A union member: its type tag in `slot` and the value's offset in `slot + 1`.
    pub fn addUnion(self: *Builder, slot: usize, tag: u8, off: u32) !void {
        try self.addInt(u8, slot, tag);
        try self.addOffset(slot + 1, off);
    }

    pub fn endTable(self: *Builder) !u32 {
        try self.prependInt(i32, 0);
        const object_offset = self.offset();
        var i = self.nfields;
        while (i > 0) {
            i -= 1;
            const f = self.fields_buf[i];
            try self.prependInt(u16, if (f != 0) @intCast(object_offset - f) else 0);
        }
        try self.prependInt(u16, @intCast(object_offset - self.object_end));
        try self.prependInt(u16, @intCast((self.nfields + 2) * 2));
        const vt = self.offset();
        const pos = self.buf.len - object_offset;
        std.mem.writeInt(i32, self.buf[pos..][0..4], @intCast(vt - object_offset), .little);
        return object_offset;
    }

    pub fn createString(self: *Builder, s: []const u8) !u32 {
        try self.prep(4, s.len + 1);
        self.place(u8, 0);
        self.head -= s.len;
        @memcpy(self.buf[self.head..][0..s.len], s);
        self.place(u32, @intCast(s.len));
        return self.offset();
    }

    pub fn createOffsetVector(self: *Builder, offs: []const u32) !u32 {
        try self.prep(4, offs.len * 4);
        var i = offs.len;
        while (i > 0) {
            i -= 1;
            try self.prependUOffset(offs[i]);
        }
        self.place(u32, @intCast(offs.len));
        return self.offset();
    }

    /// A vector of two-i64 structs (Arrow's FieldNode and Buffer are both that
    /// shape), given as `[a0, b0, a1, b1, …]`.
    pub fn createI64PairVector(self: *Builder, pairs: []const i64) !u32 {
        const n = pairs.len / 2;
        try self.prep(4, n * 16);
        try self.prep(8, n * 16);
        var i = pairs.len;
        while (i > 0) {
            i -= 2;
            self.place(i64, pairs[i + 1]);
            self.place(i64, pairs[i]);
        }
        self.place(u32, @intCast(n));
        return self.offset();
    }

    /// Close the buffer with the root offset; the returned slice aliases the
    /// builder and is valid until the next `reset`.
    pub fn finish(self: *Builder, root: u32) ![]const u8 {
        try self.prep(self.minalign, 4);
        try self.prependUOffset(root);
        return self.buf[self.head..];
    }
};

/// Just enough decoding to check what the builder produced.
pub const Table = struct {
    bytes: []const u8,
    pos: usize,

    pub fn root(bytes: []const u8) Table {
        return .{ .bytes = bytes, .pos = rd(u32, bytes, 0) };
    }

    fn rd(comptime T: type, bytes: []const u8, at: usize) T {
        return std.mem.readInt(T, bytes[at..][0..@sizeOf(T)], .little);
    }

    fn fieldPos(self: Table, slot: usize) ?usize {
        const vt = self.pos - @as(usize, @intCast(rd(i32, self.bytes, self.pos)));
        const vsize = rd(u16, self.bytes, vt);
        const fo = 4 + slot * 2;
        if (fo >= vsize) return null;
        const o = rd(u16, self.bytes, vt + fo);
        if (o == 0) return null;
        return self.pos + o;
    }

    pub fn int(self: Table, comptime T: type, slot: usize, default: T) T {
        const p = self.fieldPos(slot) orelse return default;
        return rd(T, self.bytes, p);
    }

    fn target(self: Table, slot: usize) ?usize {
        const p = self.fieldPos(slot) orelse return null;
        return p + rd(u32, self.bytes, p);
    }

    pub fn string(self: Table, slot: usize) ?[]const u8 {
        const t = self.target(slot) orelse return null;
        const len = rd(u32, self.bytes, t);
        return self.bytes[t + 4 ..][0..len];
    }

    pub fn table(self: Table, slot: usize) ?Table {
        const t = self.target(slot) orelse return null;
        return .{ .bytes = self.bytes, .pos = t };
    }

    /// Element count and position of the first element.
    pub fn vector(self: Table, slot: usize) ?struct { len: usize, at: usize } {
        const t = self.target(slot) orelse return null;
        return .{ .len = rd(u32, self.bytes, t), .at = t + 4 };
    }

    pub fn tableAt(self: Table, vec_at: usize, i: usize) Table {
        const p = vec_at + i * 4;
        return .{ .bytes = self.bytes, .pos = p + rd(u32, self.bytes, p) };
    }

    pub fn stringAt(self: Table, vec_at: usize, i: usize) []const u8 {
        const p = vec_at + i * 4;
        const t = p + rd(u32, self.bytes, p);
        return self.bytes[t + 4 ..][0..rd(u32, self.bytes, t)];
    }

    pub fn i64At(self: Table, at: usize) i64 {
        return rd(i64, self.bytes, at);
    }
};

test "table of scalars: absent fields read as defaults, present ones as written" {
    var b = try Builder.init(std.testing.allocator, 32);
    defer b.deinit();
    try b.startTable(3);
    try b.addInt(i32, 0, 7);
    try b.addInt(i64, 2, -5);
    const t = try b.endTable();
    const bytes = try b.finish(t);
    try std.testing.expectEqual(@as(usize, 0), bytes.len % 8);

    const r = Table.root(bytes);
    try std.testing.expectEqual(@as(i32, 7), r.int(i32, 0, 0));
    try std.testing.expectEqual(@as(i16, 99), r.int(i16, 1, 99));
    try std.testing.expectEqual(@as(i64, -5), r.int(i64, 2, 0));
}

test "strings, offset vectors, nested tables and struct vectors" {
    var b = try Builder.init(std.testing.allocator, 16);
    defer b.deinit();
    const s0 = try b.createString("id");
    const s1 = try b.createString("valor");
    try b.startTable(1);
    try b.addOffset(0, s1);
    const inner = try b.endTable();
    const names = try b.createOffsetVector(&.{ s0, s1 });
    const pairs = try b.createI64PairVector(&.{ 3, 0, 4, 1 });
    try b.startTable(4);
    try b.addOffset(0, names);
    try b.addUnion(1, 6, inner);
    try b.addOffset(3, pairs);
    const t = try b.endTable();
    const bytes = try b.finish(t);

    const r = Table.root(bytes);
    const v = r.vector(0).?;
    try std.testing.expectEqual(@as(usize, 2), v.len);
    try std.testing.expectEqualStrings("id", r.stringAt(v.at, 0));
    try std.testing.expectEqualStrings("valor", r.stringAt(v.at, 1));
    try std.testing.expectEqual(@as(u8, 6), r.int(u8, 1, 0));
    try std.testing.expectEqualStrings("valor", r.table(2).?.string(0).?);
    const pv = r.vector(3).?;
    try std.testing.expectEqual(@as(usize, 2), pv.len);
    try std.testing.expectEqual(@as(usize, 0), pv.at % 8);
    try std.testing.expectEqual(@as(i64, 4), r.i64At(pv.at + 16));
    try std.testing.expectEqual(@as(i64, 1), r.i64At(pv.at + 24));
}

test "growth keeps earlier bytes and offsets valid" {
    var b = try Builder.init(std.testing.allocator, 16);
    defer b.deinit();
    const long = "x" ** 200;
    const s = try b.createString(long);
    try b.startTable(1);
    try b.addOffset(0, s);
    const t = try b.endTable();
    const bytes = try b.finish(t);
    try std.testing.expectEqualStrings(long, Table.root(bytes).string(0).?);
}
