//! A test-only Avro writer: an encoder of the binary primitives, and a container
//! around records already encoded, so tests can build the manifests a reader is
//! fed. Zstandard blocks are stored as raw zstd blocks, as std has no compressor.

const std = @import("std");
const codecs = @import("../compress/codecs.zig");
const deflate = @import("../compress/deflate.zig");

pub const Codec = enum { null, deflate, snappy, zstandard };

pub const Encoder = struct {
    out: std.array_list.Managed(u8),

    pub fn init(arena: std.mem.Allocator) Encoder {
        return .{ .out = std.array_list.Managed(u8).init(arena) };
    }

    pub fn long(self: *Encoder, v: i64) !void {
        var z: u64 = @bitCast((v << 1) ^ (v >> 63));
        while (z >= 0x80) : (z >>= 7) try self.out.append(@as(u8, @truncate(z)) | 0x80);
        try self.out.append(@intCast(z));
    }

    pub fn string(self: *Encoder, s: []const u8) !void {
        try self.long(@intCast(s.len));
        try self.out.appendSlice(s);
    }

    pub fn fixed(self: *Encoder, s: []const u8) !void {
        try self.out.appendSlice(s);
    }

    pub fn boolean(self: *Encoder, b: bool) !void {
        try self.out.append(@intFromBool(b));
    }

    pub fn double(self: *Encoder, f: f64) !void {
        var b: [8]u8 = undefined;
        std.mem.writeInt(u64, &b, @bitCast(f), .little);
        try self.out.appendSlice(&b);
    }

    pub fn float(self: *Encoder, f: f32) !void {
        var b: [4]u8 = undefined;
        std.mem.writeInt(u32, &b, @bitCast(f), .little);
        try self.out.appendSlice(&b);
    }

    /// One array or map block of `n` items already encoded; `sized` writes the
    /// count negated and the byte size after it.
    pub fn block(self: *Encoder, n: i64, items: []const u8, sized: bool) !void {
        if (sized) {
            try self.long(-n);
            try self.long(@intCast(items.len));
        } else try self.long(n);
        try self.out.appendSlice(items);
    }
};

/// A container of `records` (each encoded under `schema`), `per_block` to a block;
/// `meta` pairs join the header, and an `avro.codec` among them names the codec.
pub fn container(arena: std.mem.Allocator, schema: []const u8, codec: Codec, records: []const []const u8, per_block: usize, meta: []const [2][]const u8) ![]const u8 {
    var e = Encoder.init(arena);
    try e.out.appendSlice("Obj\x01");
    var named = false;
    for (meta) |m| named = named or std.mem.eql(u8, m[0], "avro.codec");
    try e.long(@intCast(meta.len + @as(usize, if (named) 1 else 2)));
    try e.string("avro.schema");
    try e.string(schema);
    if (!named) {
        try e.string("avro.codec");
        try e.string(@tagName(codec));
    }
    for (meta) |m| {
        try e.string(m[0]);
        try e.string(m[1]);
    }
    try e.long(0);
    const sync = "0123456789abcdef";
    try e.out.appendSlice(sync);
    var i: usize = 0;
    while (i < records.len) {
        const end = @min(records.len, i + per_block);
        const raw = try std.mem.concat(arena, u8, records[i..end]);
        const data = try compress(arena, codec, raw);
        try e.long(@intCast(end - i));
        try e.long(@intCast(data.len));
        try e.out.appendSlice(data);
        try e.out.appendSlice(sync);
        i = end;
    }
    return e.out.items;
}

fn compress(arena: std.mem.Allocator, codec: Codec, raw: []const u8) ![]const u8 {
    switch (codec) {
        .null => return raw,
        .snappy => {
            const s = try codecs.compress(arena, .snappy, raw);
            var crc: [4]u8 = undefined;
            std.mem.writeInt(u32, &crc, std.hash.Crc32.hash(raw), .big);
            return std.mem.concat(arena, u8, &.{ s, &crc });
        },
        .deflate => {
            var out: std.Io.Writer.Allocating = .init(arena);
            const z = try deflate.Deflate.init(std.heap.page_allocator, &out.writer, .{ .container = .raw });
            defer z.deinit(std.heap.page_allocator);
            try z.interface.writeAll(raw);
            try z.finish();
            return out.written();
        },
        .zstandard => {
            var out = std.array_list.Managed(u8).init(arena);
            try out.appendSlice("\x28\xb5\x2f\xfd\xa0");
            var n: [4]u8 = undefined;
            std.mem.writeInt(u32, &n, @intCast(raw.len), .little);
            try out.appendSlice(&n);
            var i: usize = 0;
            while (true) {
                const len = @min(raw.len - i, 1 << 16);
                const last = i + len == raw.len;
                const hdr: u32 = @intCast((len << 3) | @intFromBool(last));
                try out.appendSlice(std.mem.asBytes(&std.mem.nativeToLittle(u32, hdr))[0..3]);
                try out.appendSlice(raw[i..][0..len]);
                i += len;
                if (last) break;
            }
            return out.items;
        },
    }
}
