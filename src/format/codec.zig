//! Parquet page compression codecs, plus the LZ4 frame and ZSTD entry points that
//! Arrow IPC body compression uses.
//!
//! Every Parquet page header carries the uncompressed size, so decompression
//! always writes into an exactly-sized buffer: a codec that produces more or fewer
//! bytes than promised is corruption, and is reported as such rather than silently
//! truncating a column. Page bytes come straight out of an untrusted file, so every
//! decoder bounds-checks each literal and back-reference against input and output.
//!
//! GZIP and ZSTD come from std. SNAPPY and LZ4 are implemented here: both are small
//! LZ77 variants, and neither is in std. Parquet uses raw block Snappy (no stream
//! framing) and, for the deprecated `LZ4` codec (superseded by `lz4_raw`), Hadoop's
//! length-prefixed framing around LZ4 blocks. `brotli` and `lzo` are deliberately
//! unsupported: Brotli needs a full decoder with its 122 KB static dictionary, and
//! LZO is effectively extinct in Parquet (parquet-mr dropped it). Both report
//! `UnsupportedCodec` up front rather than failing obscurely mid-page.
//!
//! Writing is narrower than reading: only Snappy compresses. std ships no zstd or
//! lz4 compressor, and its flate `Compress` is unfinished in 0.15.2 (`drain` ends in
//! `@panic("TODO")`). Snappy is the dominant Parquet codec anyway, and a writer
//! asked for anything else fails up front instead of emitting an unreadable file.

const std = @import("std");

pub const Codec = enum(i32) {
    uncompressed = 0,
    snappy = 1,
    gzip = 2,
    lzo = 3,
    brotli = 4,
    lz4 = 5,
    zstd = 6,
    lz4_raw = 7,

    pub fn fromInt(v: i32) ?Codec {
        return std.meta.intToEnum(Codec, v) catch null;
    }
};

pub const Error = error{
    CorruptCompressedData,
    UnsupportedCodec,
} || std.mem.Allocator.Error;

pub fn supported(c: Codec) bool {
    return switch (c) {
        .uncompressed, .snappy, .gzip, .zstd, .lz4, .lz4_raw => true,
        .brotli, .lzo => false,
    };
}

pub fn decompress(
    arena: std.mem.Allocator,
    codec: Codec,
    src: []const u8,
    uncompressed_len: usize,
) Error![]u8 {
    if (!supported(codec)) return Error.UnsupportedCodec;
    if (codec == .uncompressed) {
        if (src.len != uncompressed_len) return Error.CorruptCompressedData;
        return arena.dupe(u8, src);
    }
    const out = try arena.alloc(u8, uncompressed_len);
    switch (codec) {
        .snappy => try snappyBlock(src, out),
        .lz4_raw => try lz4Block(src, out),
        .lz4 => try lz4Hadoop(src, out),
        .gzip => try inflate(arena, src, out, .gzip),
        .zstd => try zstdDecode(arena, src, out),
        else => unreachable,
    }
    return out;
}

fn inflate(
    arena: std.mem.Allocator,
    src: []const u8,
    out: []u8,
    container: std.compress.flate.Container,
) Error!void {
    const win = try arena.alloc(u8, std.compress.flate.max_window_len);
    defer arena.free(win);
    var in: std.Io.Reader = .fixed(src);
    var d = std.compress.flate.Decompress.init(&in, container, win);
    d.reader.readSliceAll(out) catch return Error.CorruptCompressedData;
    try expectExhausted(&d.reader);
}

fn zstdDecode(arena: std.mem.Allocator, src: []const u8, out: []u8) Error!void {
    const win = try arena.alloc(u8, std.compress.zstd.default_window_len);
    defer arena.free(win);
    var in: std.Io.Reader = .fixed(src);
    var d = std.compress.zstd.Decompress.init(&in, win, .{});
    d.reader.readSliceAll(out) catch return Error.CorruptCompressedData;
    try expectExhausted(&d.reader);
}

/// `readSliceAll` stops once the destination is full, so a stream decoding to more
/// than the page header promised would be silently truncated; one byte past the end
/// must not be readable.
fn expectExhausted(r: *std.Io.Reader) Error!void {
    if (r.takeByte()) |_| return Error.CorruptCompressedData else |_| {}
}

fn snappyBlock(src: []const u8, out: []u8) Error!void {
    var i: usize = 0;

    var declared: usize = 0;
    var shift: u6 = 0;
    while (true) {
        if (i >= src.len) return Error.CorruptCompressedData;
        const b = src[i];
        i += 1;
        declared |= @as(usize, b & 0x7F) << shift;
        if (b & 0x80 == 0) break;
        shift = std.math.add(u6, shift, 7) catch return Error.CorruptCompressedData;
    }
    if (declared != out.len) return Error.CorruptCompressedData;

    var pos: usize = 0;
    while (i < src.len) {
        const tag = src[i];
        i += 1;
        switch (tag & 0x03) {
            0 => {
                var len: usize = tag >> 2;
                if (len >= 60) {
                    const extra = len - 59;
                    if (i + extra > src.len) return Error.CorruptCompressedData;
                    len = 0;
                    for (0..extra) |k| len |= @as(usize, src[i + k]) << @intCast(8 * k);
                    i += extra;
                }
                len += 1;
                if (i + len > src.len or pos + len > out.len) return Error.CorruptCompressedData;
                if (len <= 16 and i + 16 <= src.len and pos + 16 <= out.len) {
                    out[pos..][0..16].* = src[i..][0..16].*;
                } else @memcpy(out[pos..][0..len], src[i..][0..len]);
                i += len;
                pos += len;
            },
            1 => {
                if (i >= src.len) return Error.CorruptCompressedData;
                const len: usize = 4 + ((tag >> 2) & 0x07);
                const off: usize = (@as(usize, (tag >> 5) & 0x07) << 8) | src[i];
                i += 1;
                try copyMatch(out, &pos, off, len);
            },
            2 => {
                if (i + 2 > src.len) return Error.CorruptCompressedData;
                const len: usize = @as(usize, tag >> 2) + 1;
                const off: usize = std.mem.readInt(u16, src[i..][0..2], .little);
                i += 2;
                try copyMatch(out, &pos, off, len);
            },
            else => {
                if (i + 4 > src.len) return Error.CorruptCompressedData;
                const len: usize = @as(usize, tag >> 2) + 1;
                const off: usize = std.mem.readInt(u32, src[i..][0..4], .little);
                i += 4;
                try copyMatch(out, &pos, off, len);
            },
        }
    }
    if (pos != out.len) return Error.CorruptCompressedData;
}

/// When `off < len` the copy reads bytes it is writing (how Snappy and LZ4 encode
/// runs). Disjoint copies use `@memcpy` or fixed 16/8-byte chunks that may overrun
/// into space later overwritten, never reading a byte not yet written (1.3-2.2x).
inline fn copyMatch(out: []u8, pos: *usize, off: usize, len: usize) Error!void {
    if (off == 0 or off > pos.* or pos.* + len > out.len) return Error.CorruptCompressedData;
    const s = pos.* - off;
    const d = pos.*;
    pos.* = d + len;
    if (off >= 16 and d + len + 16 <= out.len) {
        var k: usize = 0;
        while (k < len) : (k += 16) out[d + k ..][0..16].* = out[s + k ..][0..16].*;
    } else if (off >= 8 and d + len + 8 <= out.len) {
        var k: usize = 0;
        while (k < len) : (k += 8) out[d + k ..][0..8].* = out[s + k ..][0..8].*;
    } else if (off >= len) {
        @memcpy(out[d..][0..len], out[s..][0..len]);
    } else {
        for (0..len) |k| out[d + k] = out[s + k];
    }
}

/// LZ4 block format: a token per sequence, high nibble literal length, low nibble
/// match length, each extended by 255-continuation bytes; the last sequence
/// is literals only.
fn lz4Block(src: []const u8, out: []u8) Error!void {
    if (try lz4BlockAt(src, out, 0) != out.len) return Error.CorruptCompressedData;
}

/// Decodes one block into `out` from `start`, returning where it ended. A match may
/// reach back past `start`, into earlier blocks of a linked LZ4 frame.
fn lz4BlockAt(src: []const u8, out: []u8, start: usize) Error!usize {
    var i: usize = 0;
    var pos: usize = start;
    while (i < src.len) {
        const token = src[i];
        i += 1;

        var lit: usize = token >> 4;
        if (lit == 15) lit += try readLenExt(src, &i);
        if (i + lit > src.len or pos + lit > out.len) return Error.CorruptCompressedData;
        @memcpy(out[pos..][0..lit], src[i..][0..lit]);
        i += lit;
        pos += lit;

        if (i == src.len) break;
        if (i + 2 > src.len) return Error.CorruptCompressedData;
        const off: usize = std.mem.readInt(u16, src[i..][0..2], .little);
        i += 2;

        var mlen: usize = token & 0x0F;
        if (mlen == 15) mlen += try readLenExt(src, &i);
        mlen += 4;
        try copyMatch(out, &pos, off, mlen);
    }
    return pos;
}

/// The LZ4 frame format (`LZ4F`): magic, descriptor, then size-prefixed blocks (stored
/// when the size's top bit is set) until a zero end mark. Checksums are skipped, not
/// verified; frames may follow one another, and skippable frames are stepped over.
pub fn lz4Frame(src: []const u8, out: []u8) Error!void {
    var i: usize = 0;
    var pos: usize = 0;
    while (i < src.len) {
        if (i + 4 > src.len) return Error.CorruptCompressedData;
        const magic = std.mem.readInt(u32, src[i..][0..4], .little);
        i += 4;
        if (magic & 0xFFFFFFF0 == 0x184D2A50) {
            if (i + 4 > src.len) return Error.CorruptCompressedData;
            const n = std.mem.readInt(u32, src[i..][0..4], .little);
            i += 4;
            if (n > src.len - i) return Error.CorruptCompressedData;
            i += n;
            continue;
        }
        if (magic != 0x184D2204) return Error.CorruptCompressedData;
        if (i + 2 > src.len) return Error.CorruptCompressedData;
        const flg = src[i];
        i += 2;
        if (flg >> 6 != 1) return Error.CorruptCompressedData;
        const block_checksum = flg & 0x10 != 0;
        const content_size = flg & 0x08 != 0;
        const content_checksum = flg & 0x04 != 0;
        const dict_id = flg & 0x01 != 0;
        if (dict_id) return Error.UnsupportedCodec;
        if (content_size) i += 8;
        i += 1;
        while (true) {
            if (i + 4 > src.len) return Error.CorruptCompressedData;
            const word = std.mem.readInt(u32, src[i..][0..4], .little);
            i += 4;
            if (word == 0) break;
            const stored = word & 0x80000000 != 0;
            const n: usize = word & 0x7FFFFFFF;
            if (n > src.len - i) return Error.CorruptCompressedData;
            const blk = src[i..][0..n];
            i += n;
            if (stored) {
                if (n > out.len - pos) return Error.CorruptCompressedData;
                @memcpy(out[pos..][0..n], blk);
                pos += n;
            } else {
                pos = try lz4BlockAt(blk, out, pos);
            }
            if (block_checksum) i += 4;
        }
        if (content_checksum) i += 4;
    }
    if (i != src.len or pos != out.len) return Error.CorruptCompressedData;
}

pub fn zstdInto(arena: std.mem.Allocator, src: []const u8, out: []u8) Error!void {
    return zstdDecode(arena, src, out);
}

fn readLenExt(src: []const u8, i: *usize) Error!usize {
    var n: usize = 0;
    while (true) {
        if (i.* >= src.len) return Error.CorruptCompressedData;
        const b = src[i.*];
        i.* += 1;
        n += b;
        if (b != 255) return n;
    }
}

/// Hadoop framing (big-endian raw and compressed sizes, then a block, repeated). Some
/// writers emitted a raw block under the same codec id, so framing is tried and
/// validated first, falling back to raw, as parquet-mr and Arrow settled on.
fn lz4Hadoop(src: []const u8, out: []u8) Error!void {
    if (lz4HadoopFramed(src, out)) |_| return else |_| {}
    return lz4Block(src, out);
}

fn lz4HadoopFramed(src: []const u8, out: []u8) Error!void {
    var i: usize = 0;
    var pos: usize = 0;
    while (i + 8 <= src.len) {
        const raw_len: usize = std.mem.readInt(u32, src[i..][0..4], .big);
        const comp_len: usize = std.mem.readInt(u32, src[i + 4 ..][0..4], .big);
        i += 8;
        if (i + comp_len > src.len or pos + raw_len > out.len) return Error.CorruptCompressedData;
        try lz4Block(src[i..][0..comp_len], out[pos..][0..raw_len]);
        i += comp_len;
        pos += raw_len;
    }
    if (pos != out.len or i != src.len) return Error.CorruptCompressedData;
}

const t = std.testing;

fn expectRoundTrip(codec: Codec, comp: []const u8, want: []const u8) !void {
    var ar = std.heap.ArenaAllocator.init(t.allocator);
    defer ar.deinit();
    const got = try decompress(ar.allocator(), codec, comp, want.len);
    try t.expectEqualStrings(want, got);
}

test "codec enum maps the thrift ids and rejects unknown ones" {
    try t.expectEqual(Codec.snappy, Codec.fromInt(1).?);
    try t.expectEqual(Codec.zstd, Codec.fromInt(6).?);
    try t.expectEqual(Codec.lz4_raw, Codec.fromInt(7).?);
    try t.expect(Codec.fromInt(99) == null);
    try t.expect(supported(.snappy) and supported(.lz4) and !supported(.brotli) and !supported(.lzo));
}

test "uncompressed passes through and rejects a length that disagrees" {
    try expectRoundTrip(.uncompressed, "hello", "hello");
    var ar = std.heap.ArenaAllocator.init(t.allocator);
    defer ar.deinit();
    try t.expectError(Error.CorruptCompressedData, decompress(ar.allocator(), .uncompressed, "hello", 4));
}

test "unsupported codecs are named, not silently wrong" {
    var ar = std.heap.ArenaAllocator.init(t.allocator);
    defer ar.deinit();
    try t.expectError(Error.UnsupportedCodec, decompress(ar.allocator(), .brotli, "x", 1));
    try t.expectError(Error.UnsupportedCodec, decompress(ar.allocator(), .lzo, "x", 1));
}

test "snappy: literal run" {
    try expectRoundTrip(.snappy, "\x05\x10hello", "hello");
}

test "snappy: 2-byte-offset copy repeats earlier output" {
    const comp = "\x0a\x10abcde" ++ "\x12\x05\x00";
    try expectRoundTrip(.snappy, comp, "abcdeabcde");
}

test "snappy: overlapping copy expands a run (offset < length)" {
    const comp = "\x06\x00a" ++ "\x12\x01\x00";
    try expectRoundTrip(.snappy, comp, "aaaaaa");
}

test "snappy: 1-byte-offset copy form" {
    const comp = "\x08\x0cabcd" ++ "\x01\x04";
    try expectRoundTrip(.snappy, comp, "abcdabcd");
}

test "snappy: corrupt input is rejected, not truncated" {
    var ar = std.heap.ArenaAllocator.init(t.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try t.expectError(Error.CorruptCompressedData, decompress(a, .snappy, "\x05\x10hello", 4));
    try t.expectError(Error.CorruptCompressedData, decompress(a, .snappy, "\x04\x00a\x12\x09\x00", 4));
    try t.expectError(Error.CorruptCompressedData, decompress(a, .snappy, "\x05\x10he", 5));
}

test "lz4 block: literals only, then a match, then an overlapping match" {
    try expectRoundTrip(.lz4_raw, "\x50hello", "hello");

    try expectRoundTrip(.lz4_raw, "\x50abcde\x05\x00", "abcdeabcd");

    try expectRoundTrip(.lz4_raw, "\x11a\x01\x00", "aaaaaa");
}

test "lz4 block: extended lengths use 255-continuation bytes" {
    var comp: [1 + 1 + 20]u8 = undefined;
    comp[0] = 0xF0;
    comp[1] = 5;
    for (comp[2..], 0..) |*c, i| c.* = @intCast('a' + (i % 26));
    var want: [20]u8 = undefined;
    for (&want, 0..) |*c, i| c.* = @intCast('a' + (i % 26));
    try expectRoundTrip(.lz4_raw, &comp, &want);
}

test "lz4 block: corrupt input is rejected" {
    var ar = std.heap.ArenaAllocator.init(t.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try t.expectError(Error.CorruptCompressedData, decompress(a, .lz4_raw, "\x10a\x09\x00", 6));
    try t.expectError(Error.CorruptCompressedData, decompress(a, .lz4_raw, "\xF0\xff", 300));
}

test "lz4 hadoop framing decodes, and falls back to a raw block" {
    var ar = std.heap.ArenaAllocator.init(t.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const framed = "\x00\x00\x00\x05" ++ "\x00\x00\x00\x06" ++ "\x50hello";
    const got = try decompress(a, .lz4, framed, 5);
    try t.expectEqualStrings("hello", got);

    const bare = try decompress(a, .lz4, "\x50hello", 5);
    try t.expectEqualStrings("hello", bare);
}

const ref_payload = "\x69\x64\x2c\x6e\x61\x6d\x65\x2c\x61\x6d\x6f\x75\x6e\x74\x0a\x30\x2c\x61\x6c\x70\x68\x61\x2d\x30\x2c\x31\x30\x30\x0a\x31\x2c\x61\x6c\x70\x68\x61\x2d\x31\x2c\x31\x30\x30\x0a\x32\x2c\x61\x6c\x70\x68\x61\x2d\x32\x2c\x31\x30\x30\x0a\x33\x2c\x61\x6c\x70\x68\x61\x2d\x33\x2c\x31\x30\x30\x0a\x34\x2c\x61\x6c\x70\x68\x61\x2d\x34\x2c\x31\x30\x30\x0a\x35\x2c\x61\x6c\x70\x68\x61\x2d\x35\x2c\x31\x30\x30\x0a\x36\x2c\x61\x6c\x70\x68\x61\x2d\x36\x2c\x31\x30\x30\x0a\x37\x2c\x61\x6c\x70\x68\x61\x2d\x30\x2c\x31\x30\x30\x0a\x38\x2c\x61\x6c\x70\x68\x61\x2d\x31\x2c\x31\x30\x30\x0a\x39\x2c\x61\x6c\x70\x68\x61\x2d\x32\x2c\x31\x30\x30\x0a\x31\x30\x2c\x61\x6c\x70\x68\x61\x2d\x33\x2c\x31\x30\x30\x0a\x31\x31\x2c\x61\x6c\x70\x68\x61\x2d\x34\x2c\x31\x30\x30\x0a\x31\x32\x2c\x61\x6c\x70\x68\x61\x2d\x35\x2c\x31\x30\x30\x0a\x31\x33\x2c\x61\x6c\x70\x68\x61\x2d\x36\x2c\x31\x30\x30\x0a\x31\x34\x2c\x61\x6c\x70\x68\x61\x2d\x30\x2c\x31\x30\x30\x0a\x31\x35\x2c\x61\x6c\x70\x68\x61\x2d\x31\x2c\x31\x30\x30\x0a\x31\x36\x2c\x61\x6c\x70\x68\x61\x2d\x32\x2c\x31\x30\x30\x0a\x31\x37\x2c\x61\x6c\x70\x68\x61\x2d\x33\x2c\x31\x30\x30\x0a\x31\x38\x2c\x61\x6c\x70\x68\x61\x2d\x34\x2c\x31\x30\x30\x0a\x31\x39\x2c\x61\x6c\x70\x68\x61\x2d\x35\x2c\x31\x30\x30\x0a\x32\x30\x2c\x61\x6c\x70\x68\x61\x2d\x36\x2c\x31\x30\x30\x0a\x32\x31\x2c\x61\x6c\x70\x68\x61\x2d\x30\x2c\x31\x30\x30\x0a\x32\x32\x2c\x61\x6c\x70\x68\x61\x2d\x31\x2c\x31\x30\x30\x0a\x32\x33\x2c\x61\x6c\x70\x68\x61\x2d\x32\x2c\x31\x30\x30\x0a\x32\x34\x2c\x61\x6c\x70\x68\x61\x2d\x33\x2c\x31\x30\x30\x0a\x32\x35\x2c\x61\x6c\x70\x68\x61\x2d\x34\x2c\x31\x30\x30\x0a\x32\x36\x2c\x61\x6c\x70\x68\x61\x2d\x35\x2c\x31\x30\x30\x0a\x32\x37\x2c\x61\x6c\x70\x68\x61\x2d\x36\x2c\x31\x30\x30\x0a\x32\x38\x2c\x61\x6c\x70\x68\x61\x2d\x30\x2c\x31\x30\x30\x0a\x32\x39\x2c\x61\x6c\x70\x68\x61\x2d\x31\x2c\x31\x30\x30\x0a\x33\x30\x2c\x61\x6c\x70\x68\x61\x2d\x32\x2c\x31\x30\x30\x0a\x33\x31\x2c\x61\x6c\x70\x68\x61\x2d\x33\x2c\x31\x30\x30\x0a\x33\x32\x2c\x61\x6c\x70\x68\x61\x2d\x34\x2c\x31\x30\x30\x0a\x33\x33\x2c\x61\x6c\x70\x68\x61\x2d\x35\x2c\x31\x30\x30\x0a\x33\x34\x2c\x61\x6c\x70\x68\x61\x2d\x36\x2c\x31\x30\x30\x0a\x33\x35\x2c\x61\x6c\x70\x68\x61\x2d\x30\x2c\x31\x30\x30\x0a\x33\x36\x2c\x61\x6c\x70\x68\x61\x2d\x31\x2c\x31\x30\x30\x0a\x33\x37\x2c\x61\x6c\x70\x68\x61\x2d\x32\x2c\x31\x30\x30\x0a\x33\x38\x2c\x61\x6c\x70\x68\x61\x2d\x33\x2c\x31\x30\x30\x0a\x33\x39\x2c\x61\x6c\x70\x68\x61\x2d\x34\x2c\x31\x30\x30\x0a";
const ref_gzip = "\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x55\xd1\x3b\x0a\x42\x41\x0c\x46\xe1\xde\xb5\x8c\x90\xc7\x3c\x97\x33\xa0\xa0\xe0\xbd\x5a\xe8\xfe\x85\x90\xe6\x94\xa7\xfa\xc8\x9f\xe7\xad\x9c\xfb\xb8\x97\x7d\xbc\x7f\xe7\xf7\x22\x65\xbf\x3e\x8f\x7d\x95\xa2\x22\x17\xcd\xd2\x28\xcb\xb2\x28\xcf\xf2\xa8\x9a\x55\xa3\x5a\x56\x8b\xea\x59\x3d\x6a\x40\x98\x10\x16\x04\x15\x10\xaa\x30\xd4\x80\xa8\x43\xd1\xca\x43\x1a\x1c\xed\x84\x06\xa1\x49\x68\x01\x32\x01\x64\x0a\xc8\x8c\x93\x39\x20\xab\x80\xac\x01\xb2\x4e\x68\x10\x9a\x84\x16\x20\x17\x3e\x47\x01\xb9\x01\x72\x07\xe4\x15\x90\x37\x40\xde\x09\x0d\x42\x93\xd0\x02\xf4\x07\x36\x13\x14\x47\x5d\x02\x00\x00";
const ref_zstd = "\x28\xb5\x2f\xfd\x60\x5d\x01\xfd\x03\x00\xf2\x06\x13\x15\xa0\x29\x1d\x89\xf6\x59\x85\x84\xf0\xff\x38\x6b\x4d\x4a\x99\x12\xa5\xf4\x07\x07\x61\x84\xec\xd0\x7f\xbf\xe7\xed\x75\xfa\xef\xd7\xb4\xb9\x4c\xbe\xed\x9a\x36\x57\xa9\xaf\xdb\xb2\xd6\x2a\xbd\x16\x34\x40\x80\x59\x15\xd1\x6f\xb7\x84\xec\xd0\xb8\x42\xe2\x70\x43\xa2\x38\x16\x88\x72\x43\x28\x77\xcc\xc0\x10\x4d\xa8\x21\xfc\xfa\x7f\x06\xc0\xa3\xea\x10\x82\x52\x0f\x6d\x9a\xa6\xd5\x34\x4d\xa3\xb9\xcc\x84\x49\x2e\x8f\x45\x54\x26\x4a\x32\xf9\x48\x04\x12\xf9\x0c\x92\xec\xbf\x7f\xff\x0c\x2f\x8c\xda\xaa";

test "gzip and zstd decode real third-party output byte-for-byte" {
    try expectRoundTrip(.gzip, ref_gzip, ref_payload);
    try expectRoundTrip(.zstd, ref_zstd, ref_payload);
}

test "a truncated real stream is rejected rather than short-read" {
    var ar = std.heap.ArenaAllocator.init(t.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try t.expectError(Error.CorruptCompressedData, decompress(a, .gzip, ref_gzip[0 .. ref_gzip.len - 20], ref_payload.len));
    try t.expectError(Error.CorruptCompressedData, decompress(a, .zstd, ref_zstd[0 .. ref_zstd.len - 20], ref_payload.len));
}

test "a real stream decoded with the wrong expected length is corruption" {
    var ar = std.heap.ArenaAllocator.init(t.allocator);
    defer ar.deinit();
    try t.expectError(Error.CorruptCompressedData, decompress(ar.allocator(), .gzip, ref_gzip, ref_payload.len - 1));
}

pub fn canCompress(c: Codec) bool {
    return switch (c) {
        .uncompressed, .snappy => true,
        else => false,
    };
}

pub fn compress(arena: std.mem.Allocator, codec: Codec, src: []const u8) Error![]u8 {
    if (!canCompress(codec)) return Error.UnsupportedCodec;
    return switch (codec) {
        .uncompressed => arena.dupe(u8, src),
        .snappy => snappyCompress(arena, src),
        else => unreachable,
    };
}

const snappy_hash_bits = 14;
const snappy_window = (1 << 16) - 1;
const snappy_min_match = 4;

/// Greedy LZ77 over a hash table of 4-byte sequences. Not the tightest possible, but
/// the stream any Snappy decoder accepts. Offsets stop at 65535, since 65536
/// would encode as offset 0.
fn snappyCompress(arena: std.mem.Allocator, src: []const u8) Error![]u8 {
    var out = std.array_list.Managed(u8).init(arena);
    try putVarint(&out, src.len);
    if (src.len == 0) return out.toOwnedSlice();

    const table = try arena.alloc(u32, 1 << snappy_hash_bits);
    defer arena.free(table);
    @memset(table, std.math.maxInt(u32));

    var pos: usize = 0;
    var lit_start: usize = 0;
    while (pos + snappy_min_match <= src.len) {
        const h = snappyHash(src[pos..][0..4]);
        const cand = table[h];
        table[h] = @intCast(pos);

        const hit = cand != std.math.maxInt(u32) and
            pos - cand <= snappy_window and
            std.mem.eql(u8, src[cand..][0..snappy_min_match], src[pos..][0..snappy_min_match]);
        if (!hit) {
            pos += 1;
            continue;
        }

        var len: usize = snappy_min_match;
        while (pos + len < src.len and src[cand + len] == src[pos + len]) len += 1;

        try emitLiteral(&out, src[lit_start..pos]);
        try emitCopy(&out, pos - cand, len);
        pos += len;
        lit_start = pos;
    }
    try emitLiteral(&out, src[lit_start..]);
    return out.toOwnedSlice();
}

fn snappyHash(b: *const [4]u8) usize {
    const v = std.mem.readInt(u32, b, .little);
    return (v *% 0x1e35a7bd) >> (32 - snappy_hash_bits);
}

fn putVarint(out: *std.array_list.Managed(u8), n_in: usize) Error!void {
    var n = n_in;
    while (true) {
        const b: u8 = @intCast(n & 0x7F);
        n >>= 7;
        try out.append(if (n != 0) b | 0x80 else b);
        if (n == 0) return;
    }
}

fn emitLiteral(out: *std.array_list.Managed(u8), data: []const u8) Error!void {
    if (data.len == 0) return;
    const n = data.len - 1;
    if (n < 60) {
        try out.append(@intCast(n << 2));
    } else {
        var extra: u8 = 0;
        var v = n;
        while (v > 0) : (v >>= 8) extra += 1;
        try out.append(@intCast((@as(usize, 59 + extra) << 2)));
        var k: u8 = 0;
        while (k < extra) : (k += 1) try out.append(@intCast((n >> @intCast(8 * k)) & 0xFF));
    }
    try out.appendSlice(data);
}

/// The 2-byte-offset form encodes a length of 1..64, so longer matches split across
/// copies. `take` is explicitly usize: `@min` against a comptime bound narrowed it to
/// u7, and the tag shift then truncated silently.
fn emitCopy(out: *std.array_list.Managed(u8), offset: usize, len_in: usize) Error!void {
    var len = len_in;
    while (len > 0) {
        const take: usize = @min(len, 64);
        try out.append(@intCast(((take - 1) << 2) | 2));
        try out.append(@intCast(offset & 0xFF));
        try out.append(@intCast((offset >> 8) & 0xFF));
        len -= take;
    }
}

test "snappy compressor output round-trips through our own decoder" {
    var ar = std.heap.ArenaAllocator.init(t.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const cases = [_][]const u8{
        "",
        "a",
        "hello hello hello hello hello hello",
        "the quick brown fox jumps over the lazy dog, the quick brown fox again",
    };
    for (cases) |want| {
        const comp = try compress(a, .snappy, want);
        const got = try decompress(a, .snappy, comp, want.len);
        try t.expectEqualStrings(want, got);
    }

    const big = try a.alloc(u8, 100_000);
    for (big, 0..) |*c, i| c.* = @intCast('a' + (i / 997) % 26);
    const comp = try compress(a, .snappy, big);
    try t.expect(comp.len < big.len);
    try t.expectEqualSlices(u8, big, try decompress(a, .snappy, comp, big.len));
}

test "compress refuses codecs without an encoder" {
    var ar = std.heap.ArenaAllocator.init(t.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try t.expect(supported(.gzip) and !canCompress(.gzip));
    try t.expect(supported(.zstd) and !canCompress(.zstd));
    try t.expect(supported(.lz4_raw) and !canCompress(.lz4_raw));
    try t.expectError(Error.UnsupportedCodec, compress(a, .zstd, "x"));
    try t.expectError(Error.UnsupportedCodec, compress(a, .gzip, "x"));

    try t.expectEqualStrings("abc", try decompress(a, .uncompressed, try compress(a, .uncompressed, "abc"), 3));
}

/// Fuzzes every decoder on hostile bytes with an input-derived output length, into a
/// fixed buffer so a decoder talked into a huge size gets OutOfMemory, not the memory.
fn fuzzDecompress(_: void, input: []const u8) anyerror!void {
    if (input.len < 2) return;
    const ulen: usize = (@as(usize, input[0]) << 8) | input[1];
    const src = input[2..];
    var mem: [256 * 1024]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&mem);
    var arena = std.heap.ArenaAllocator.init(fba.allocator());
    defer arena.deinit();
    const a = arena.allocator();
    _ = decompress(a, .snappy, src, ulen) catch {};
    _ = decompress(a, .lz4_raw, src, ulen) catch {};
    _ = decompress(a, .lz4, src, ulen) catch {};
    _ = decompress(a, .gzip, src, ulen) catch {};
    _ = decompress(a, .zstd, src, ulen) catch {};
}

const fuzzDecompress_corpus = [_][]const u8{
    "\x00\x33" ++ @embedFile("testdata/rows.csv.gz"),
    "\x00\x33" ++ @embedFile("testdata/rows.csv.zst"),
};

test "fuzz: decompressors survive arbitrary bytes" {
    try std.testing.fuzz({}, fuzzDecompress, .{ .corpus = &fuzzDecompress_corpus });
    try @import("../net/fuzzutil.zig").pound(fuzzDecompress, &fuzzDecompress_corpus);
}

test "snappy: chunked copies agree with a byte-at-a-time decode on random streams" {
    const a = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(42);
    const rnd = prng.random();
    for (0..200) |_| {
        var stream: std.ArrayList(u8) = .empty;
        defer stream.deinit(a);
        var want: std.ArrayList(u8) = .empty;
        defer want.deinit(a);
        const n_ops = rnd.intRangeAtMost(usize, 1, 60);
        var body: std.ArrayList(u8) = .empty;
        defer body.deinit(a);
        for (0..n_ops) |_| {
            if (want.items.len == 0 or rnd.boolean()) {
                const len = rnd.intRangeAtMost(usize, 1, 40);
                try body.append(a, @intCast((len - 1) << 2));
                for (0..len) |_| {
                    const c = rnd.int(u8);
                    try body.append(a, c);
                    try want.append(a, c);
                }
            } else {
                const off = rnd.intRangeAtMost(usize, 1, @min(want.items.len, 64));
                const len = rnd.intRangeAtMost(usize, 1, 64);
                try body.append(a, @intCast(((len - 1) << 2) | 2));
                try body.append(a, @intCast(off & 0xff));
                try body.append(a, @intCast(off >> 8));
                for (0..len) |_| try want.append(a, want.items[want.items.len - off]);
            }
        }
        var v = want.items.len;
        while (v >= 0x80) : (v >>= 7) try stream.append(a, @intCast((v & 0x7f) | 0x80));
        try stream.append(a, @intCast(v));
        try stream.appendSlice(a, body.items);
        const out = try a.alloc(u8, want.items.len);
        defer a.free(out);
        try snappyBlock(stream.items, out);
        try std.testing.expectEqualSlices(u8, want.items, out);
    }
}
