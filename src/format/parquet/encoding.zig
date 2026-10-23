//! Parquet's page value encodings: the RLE/bit-packed hybrid, the three DELTA
//! forms and BYTE_STREAM_SPLIT. PLAIN values are read in place by chunk.zig's kernel.

const Error = @import("read.zig").Error;
const std = @import("std");
const testing = std.testing;
const varint = @import("../varint.zig");

pub const BitReader = struct {
    buf: []const u8,
    bit_pos: usize = 0,

    /// LSB-first, the order Parquet's bit-packing uses, a whole u64 word at a time.
    /// Width is `u7` because INT64 delta miniblocks may use 64; a value spanning two
    /// words is assembled from both (one word once silently zeroed its top bits).
    pub fn read(self: *BitReader, width: u7) Error!u64 {
        if (width == 0) return 0;
        const end = self.bit_pos + width;
        if ((end + 7) >> 3 > self.buf.len) return Error.CorruptParquetPage;

        const byte = self.bit_pos >> 3;
        const shift: u6 = @intCast(self.bit_pos & 7);
        self.bit_pos = end;

        if (@as(usize, shift) + width <= 64) {
            var word: u64 = 0;
            if (byte + 8 <= self.buf.len) {
                word = std.mem.readInt(u64, self.buf[byte..][0..8], .little);
            } else {
                var k: usize = 0;
                while (byte + k < self.buf.len and k < 8) : (k += 1) {
                    word |= @as(u64, self.buf[byte + k]) << @intCast(8 * k);
                }
            }
            const v = word >> shift;
            return if (width == 64) v else v & ((@as(u64, 1) << @intCast(width)) - 1);
        }

        var word: u128 = 0;
        var k: usize = 0;
        while (byte + k < self.buf.len and k < 9) : (k += 1) {
            word |= @as(u128, self.buf[byte + k]) << @intCast(8 * k);
        }
        const v: u64 = @truncate(word >> shift);
        return if (width == 64) v else v & ((@as(u64, 1) << @intCast(width)) - 1);
    }
};

pub fn bitWidth(max: u32) u6 {
    if (max == 0) return 0;
    return @intCast(32 - @clz(max));
}

/// RLE/bit-packed hybrid: each run's varint header has its low bit set for
/// `(header >> 1) * 8` bit-packed values, clear for an RLE run of `header >> 1`
/// copies. Run counts are checked before they are multiplied, as they can wrap.
pub fn decodeRleHybrid(
    arena: std.mem.Allocator,
    src: []const u8,
    width: u6,
    count: usize,
) Error![]u32 {
    const out = try arena.alloc(u32, count);
    if (count == 0) return out;
    if (width == 0) {
        @memset(out, 0);
        return out;
    }

    var pos: usize = 0;
    var n: usize = 0;
    while (n < count) {
        const header = try readVarint(src, &pos);
        if (header & 1 == 1) {
            const groups = std.math.cast(usize, header >> 1) orelse return Error.CorruptParquetPage;
            const bytes = std.math.mul(usize, groups, width) catch return Error.CorruptParquetPage;
            if (bytes > src.len - pos) return Error.CorruptParquetPage;
            const vals = groups * 8;
            const run = src[pos..][0..bytes];
            const take = @min(vals, count - n);
            const mask = (@as(u64, 1) << width) - 1;
            var i: usize = switch (width) {
                inline 1...32 => |w| unpackGroups(w, run, out[n..][0..take]),
                else => 0,
            };
            while (width <= 32 and i < take) : (i += 1) {
                const bit = i * width;
                const byte = bit >> 3;
                if (byte + 8 > run.len) break;
                out[n + i] = @intCast((std.mem.readInt(u64, run[byte..][0..8], .little) >> @intCast(bit & 7)) & mask);
            }
            var br = BitReader{ .buf = run, .bit_pos = i * width };
            while (i < take) : (i += 1) out[n + i] = @intCast(try br.read(width));
            n += take;
            pos += bytes;
        } else {
            const run: usize = @intCast(header >> 1);
            if (run == 0) return Error.CorruptParquetPage;
            const vb = (@as(usize, width) + 7) / 8;
            if (pos + vb > src.len) return Error.CorruptParquetPage;
            var v: u32 = 0;
            for (0..vb) |k| v |= @as(u32, src[pos + k]) << @intCast(8 * k);
            pos += vb;
            const take = @min(run, count - n);
            @memset(out[n..][0..take], v);
            n += take;
        }
    }
    return out;
}

/// The whole groups of eight `w`-bit values at the start of a bit-packed run, each
/// group's shifts fixed at compile time; returns how many values it wrote. Stops at
/// the first group whose last word would read past `run`, which the caller finishes.
fn unpackGroups(comptime w: u6, run: []const u8, out: []u32) usize {
    const mask: u64 = (@as(u64, 1) << w) - 1;
    const groups = out.len / 8;
    var g: usize = 0;
    while (g < groups) : (g += 1) {
        const base = g * @as(usize, w);
        if (base + w + 8 > run.len) break;
        const p = run[base..];
        inline for (0..8) |k| {
            const bit = k * @as(usize, w);
            out[g * 8 + k] = @intCast((std.mem.readInt(u64, p[bit / 8 ..][0..8], .little) >> @intCast(bit % 8)) & mask);
        }
    }
    return g * 8;
}

fn readVarint(src: []const u8, pos: *usize) Error!u64 {
    return varint.read(src, pos) catch Error.CorruptParquetPage;
}

/// INT96: Julian day (2440588 is 1970-01-01) plus nanoseconds of day. Saturates,
/// since a u32 Julian day of micros overflows i64 on a corrupt file.
pub fn int96ToMicros(julian_day: u32, nanos_of_day: u64) i64 {
    const days: i64 = @as(i64, julian_day) - 2_440_588;
    return (days *| 86_400_000_000) +| @as(i64, @intCast(nanos_of_day / 1000));
}

const DeltaHeader = struct { miniblocks: usize, per_mini: usize, total: usize, first: i64 };

/// A DELTA_BINARY_PACKED header held to the spec: a block a multiple of 128
/// values, each miniblock a multiple of 32, capped, so the sizes computed from
/// them (values times a width of up to 64 bits) cannot overflow.
fn deltaHeader(src: []const u8, pos: *usize) Error!DeltaHeader {
    const block_size = try readVarint(src, pos);
    const miniblocks = try readVarint(src, pos);
    const total = try readVarint(src, pos);
    const first = try readZigZagAt(src, pos);
    if (block_size == 0 or block_size % 128 != 0 or block_size > 1 << 20) return Error.CorruptParquetPage;
    if (miniblocks == 0 or block_size % miniblocks != 0 or (block_size / miniblocks) % 32 != 0) return Error.CorruptParquetPage;
    if (total > std.math.maxInt(usize)) return Error.CorruptParquetPage;
    return .{ .miniblocks = @intCast(miniblocks), .per_mini = @intCast(block_size / miniblocks), .total = @intCast(total), .first = first };
}

/// DELTA_BINARY_PACKED: `<block size> <miniblocks> <count> <first>`, then per block
/// `<min delta> <widths> <miniblocks>`. Running sums desync on any miscount, so
/// every bound is checked; a width byte over 64 is corrupt (it was once a panic).
pub fn decodeDeltaBinaryPacked(
    arena: std.mem.Allocator,
    src: []const u8,
    count: usize,
) Error![]i64 {
    var pos: usize = 0;
    const h = try deltaHeader(src, &pos);
    const miniblocks = h.miniblocks;
    const per_mini = h.per_mini;
    const total = h.total;
    var value: i64 = h.first;

    const want = @min(count, total);
    const out = try arena.alloc(i64, count);
    if (count == 0) return out;

    var n: usize = 0;
    out[n] = value;
    n += 1;

    while (n < want) {
        const min_delta = try readZigZagAt(src, &pos);
        if (miniblocks > src.len - pos) return Error.CorruptParquetPage;
        const widths = src[pos..][0..miniblocks];
        pos += miniblocks;

        for (widths) |w| {
            if (n >= want) break;
            if (w > 64) return Error.CorruptParquetPage;
            const width: u7 = @intCast(w);
            const bytes = (per_mini * @as(usize, width) + 7) / 8;
            if (pos + bytes > src.len) return Error.CorruptParquetPage;
            var br = BitReader{ .buf = src[pos..][0..bytes] };
            for (0..per_mini) |_| {
                if (n >= want) break;
                const raw: i64 = @bitCast(try br.read(width));
                value +%= min_delta +% raw;
                out[n] = value;
                n += 1;
            }
            pos += bytes;
        }
    }
    while (n < count) : (n += 1) out[n] = value;
    return out;
}

fn readZigZagAt(src: []const u8, pos: *usize) Error!i64 {
    return varint.unzigzag(try readVarint(src, pos));
}

pub fn decodeDeltaLengthByteArray(
    arena: std.mem.Allocator,
    src: []const u8,
    count: usize,
) Error![][]const u8 {
    var pos: usize = 0;
    const lens = try decodeDeltaBinaryPackedTracking(arena, src, count, &pos);
    const out = try arena.alloc([]const u8, count);
    var off = pos;
    for (out, lens) |*o, l| {
        const n: usize = @intCast(@max(0, l));
        if (off + n > src.len) return Error.CorruptParquetPage;
        o.* = src[off..][0..n];
        off += n;
    }
    return out;
}

/// DELTA_BYTE_ARRAY: prefix lengths shared with the previous value, suffix
/// lengths, then the suffix bytes. A first pass sizes the output so every value
/// lands in one buffer, refused past `deltaBytesCap` of the page's length.
pub fn decodeDeltaByteArray(
    arena: std.mem.Allocator,
    src: []const u8,
    count: usize,
) Error![][]const u8 {
    var pos: usize = 0;
    const prefixes = try decodeDeltaBinaryPackedTracking(arena, src, count, &pos);
    var pos2: usize = 0;
    const suffixes = try decodeDeltaBinaryPackedTracking(arena, src[pos..], count, &pos2);
    const start = pos + pos2;

    const cap = deltaBytesCap(src.len);
    var total: usize = 0;
    var prev: usize = 0;
    var off = start;
    for (prefixes, suffixes) |p, sfx| {
        const plen: usize = @intCast(@max(0, p));
        const slen: usize = @intCast(@max(0, sfx));
        if (plen > prev or slen > src.len - off) return Error.CorruptParquetPage;
        prev = plen + slen;
        if (prev > cap - total) return Error.CorruptParquetPage;
        total += prev;
        off += slen;
    }

    const buf = try arena.alloc(u8, total);
    const out = try arena.alloc([]const u8, count);
    var w: usize = 0;
    var last: usize = 0;
    off = start;
    for (out, prefixes, suffixes) |*o, p, sfx| {
        const plen: usize = @intCast(@max(0, p));
        const slen: usize = @intCast(@max(0, sfx));
        @memcpy(buf[w..][0..plen], buf[last..][0..plen]);
        @memcpy(buf[w + plen ..][0..slen], src[off..][0..slen]);
        off += slen;
        o.* = buf[w..][0 .. plen + slen];
        last = w;
        w += plen + slen;
    }
    return out;
}

/// The most bytes a DELTA_BYTE_ARRAY page of `page_len` bytes may decode to:
/// shared prefixes legitimately repeat bytes, but a few KB claiming gigabytes is
/// a crafted page. Never past i32, the column offsets' range.
fn deltaBytesCap(page_len: usize) usize {
    return @min(std.math.maxInt(i32), @max(64 << 20, page_len *| 256));
}

/// `decodeDeltaBinaryPacked` that also reports where the stream ended, so a
/// caller can find the payload that follows it.
fn decodeDeltaBinaryPackedTracking(
    arena: std.mem.Allocator,
    src: []const u8,
    count: usize,
    end: *usize,
) Error![]i64 {
    var pos: usize = 0;
    const h = try deltaHeader(src, &pos);
    const miniblocks = h.miniblocks;
    const per_mini = h.per_mini;
    const total = h.total;
    var value: i64 = h.first;

    const out = try arena.alloc(i64, count);
    const want = @min(count, total);
    var n: usize = 0;
    if (count > 0) {
        out[n] = value;
        n += 1;
    }
    while (n < want) {
        const min_delta = try readZigZagAt(src, &pos);
        if (miniblocks > src.len - pos) return Error.CorruptParquetPage;
        const widths = src[pos..][0..miniblocks];
        pos += miniblocks;
        for (widths) |w| {
            if (w > 64) return Error.CorruptParquetPage;
            const width: u7 = @intCast(w);
            const bytes = (per_mini * @as(usize, width) + 7) / 8;
            if (pos + bytes > src.len) return Error.CorruptParquetPage;
            if (n < want) {
                var br = BitReader{ .buf = src[pos..][0..bytes] };
                for (0..per_mini) |_| {
                    if (n >= want) break;
                    const raw: i64 = @bitCast(try br.read(width));
                    value +%= min_delta +% raw;
                    out[n] = value;
                    n += 1;
                }
            }
            pos += bytes;
        }
    }
    while (n < count) : (n += 1) out[n] = value;
    end.* = pos;
    return out;
}

/// BYTE_STREAM_SPLIT: every value's first byte, then every second byte, and so
/// on; regrouping restores the little-endian values.
pub fn decodeByteStreamSplit(
    arena: std.mem.Allocator,
    src: []const u8,
    width: usize,
    count: usize,
) Error![]u8 {
    if (count == 0) return &.{};
    if (src.len < width * count) return Error.CorruptParquetPage;
    const out = try arena.alloc(u8, width * count);
    for (0..width) |j| {
        for (0..count) |i| out[i * width + j] = src[j * count + i];
    }
    return out;
}

test "bitWidth covers the level widths Parquet asks for" {
    try testing.expectEqual(@as(u6, 0), bitWidth(0));
    try testing.expectEqual(@as(u6, 1), bitWidth(1));
    try testing.expectEqual(@as(u6, 2), bitWidth(2));
    try testing.expectEqual(@as(u6, 2), bitWidth(3));
    try testing.expectEqual(@as(u6, 3), bitWidth(4));
    try testing.expectEqual(@as(u6, 8), bitWidth(255));
}

test "RLE run repeats one value" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const got = try decodeRleHybrid(ar.allocator(), &[_]u8{ 0x08, 0x01 }, 1, 4);
    try testing.expectEqualSlices(u32, &.{ 1, 1, 1, 1 }, got);
}

test "bit-packed run unpacks LSB-first in groups of eight" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const got = try decodeRleHybrid(ar.allocator(), &[_]u8{ 0x03, 0b10110010 }, 1, 8);
    try testing.expectEqualSlices(u32, &.{ 0, 1, 0, 0, 1, 1, 0, 1 }, got);
}

test "a width of zero yields all zeroes without consuming input" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const got = try decodeRleHybrid(ar.allocator(), &.{}, 0, 3);
    try testing.expectEqualSlices(u32, &.{ 0, 0, 0 }, got);
}

test "a truncated hybrid stream errors rather than reading past the page" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    try testing.expectError(Error.CorruptParquetPage, decodeRleHybrid(ar.allocator(), &[_]u8{0x03}, 1, 8));
    try testing.expectError(Error.CorruptParquetPage, decodeRleHybrid(ar.allocator(), &[_]u8{ 0x00, 0x01 }, 1, 4));
}

test "int96 converts Julian day plus nanoseconds to epoch micros" {
    try testing.expectEqual(@as(i64, 0), int96ToMicros(2_440_588, 0));
    try testing.expectEqual(@as(i64, 1_000_000), int96ToMicros(2_440_588, 1_000_000_000));
    try testing.expectEqual(@as(i64, 86_400_000_000), int96ToMicros(2_440_589, 0));
    try testing.expectEqual(@as(i64, -86_400_000_000), int96ToMicros(2_440_587, 0));
    try testing.expectEqual(@as(i64, std.math.maxInt(i64)), int96ToMicros(std.math.maxInt(u32), 0));
    try testing.expectEqual(@as(i64, -210_866_803_200_000_000), int96ToMicros(0, 0));
}

test "a bit-packed run length from the wire cannot overflow the byte count" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const huge = [_]u8{ 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01 };
    try testing.expectError(Error.CorruptParquetPage, decodeRleHybrid(ar.allocator(), &huge, 32, 8));
}

test "delta binary packed recovers a running sum, including negatives" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const src = [_]u8{ 0x80, 0x01, 0x04, 0x01, 0x0e };
    const got = try decodeDeltaBinaryPacked(a, &src, 1);
    try testing.expectEqualSlices(i64, &.{7}, got);

    const block = [_]u8{ 0x80, 0x01, 0x04, 0x05, 0x05, 0x0f, 0x04, 0x00, 0x00, 0x00, 0x96, 0x0c } ++ [_]u8{0} ** 14;
    try testing.expectEqualSlices(i64, &.{ -3, -5, -4, 0, -8 }, try decodeDeltaBinaryPacked(a, &block, 5));

    try testing.expectError(Error.CorruptParquetPage, decodeDeltaBinaryPacked(a, &[_]u8{ 0x80, 0x01, 0x00, 0x01, 0x00 }, 1));
}

test "byte stream split regroups transposed value bytes" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const src = [_]u8{ 0x00, 0x04, 0x01, 0x05, 0x02, 0x06, 0x03, 0x07 };
    const got = try decodeByteStreamSplit(ar.allocator(), &src, 4, 2);
    try testing.expectEqualSlices(u8, &.{ 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07 }, got);
    try testing.expectError(Error.CorruptParquetPage, decodeByteStreamSplit(ar.allocator(), &src, 4, 3));
}

test "BitReader: wide values at non-zero bit offsets keep their top bits" {
    const v61: u64 = 0x1ABC_DEF0_1234_5678 & ((1 << 61) - 1);
    const v64: u64 = 0xFEDC_BA98_7654_3210;
    var all: [16]u8 = undefined;
    std.mem.writeInt(u128, &all, 0b111 | (@as(u128, v61) << 3) | (@as(u128, v64) << 64), .little);
    var br = BitReader{ .buf = &all };
    try std.testing.expectEqual(@as(u64, 0b111), try br.read(3));
    try std.testing.expectEqual(v61, try br.read(61));
    try std.testing.expectEqual(v64, try br.read(64));

    var nine: [9]u8 = undefined;
    std.mem.writeInt(u72, &nine, 0b101 | (@as(u72, v64) << 3), .little);
    var odd = BitReader{ .buf = &nine };
    try std.testing.expectEqual(@as(u64, 0b101), try odd.read(3));
    try std.testing.expectEqual(v64, try odd.read(64));

    var short = BitReader{ .buf = all[0..8] };
    _ = try short.read(3);
    try std.testing.expectError(Error.CorruptParquetPage, short.read(64));
}

test "decodeDeltaBinaryPacked: a 64-bit miniblock delta wraps instead of trapping" {
    var page: [5 + 1 + 1 + 128 * 8]u8 = @splat(0);
    page[0..5].* = .{ 0x80, 0x01, 0x01, 0x02, 0x00 };
    page[6] = 64;
    @memset(page[7..15], 0xFF);
    const got = try decodeDeltaBinaryPacked(std.testing.allocator, &page, 2);
    defer std.testing.allocator.free(got);
    try std.testing.expectEqualSlices(i64, &.{ 0, -1 }, got);
}

test "decodeDeltaBinaryPacked: a miniblock width above 64 is a corrupt page" {
    var mem: [4096]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&mem);
    const page = [_]u8{ 0x80, 0x01, 0x01, 0x02, 0x00, 0x00, 0xFF };
    try std.testing.expectError(
        Error.CorruptParquetPage,
        decodeDeltaBinaryPacked(fba.allocator(), &page, 2),
    );
}

test "decodeDeltaBinaryPacked: a header off the spec, or one whose sizes would overflow, is a corrupt page" {
    var mem: [4096]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&mem);
    const huge = [_]u8{0xFF} ** 9 ++ [_]u8{0x01};
    const pages = [_][]const u8{
        &(huge ++ huge ++ [_]u8{ 0x02, 0x00, 0x00 }),
        &[_]u8{ 0x64, 0x01, 0x02, 0x00 },
        &[_]u8{ 0x80, 0x01, 0x08, 0x02, 0x00 },
        &([_]u8{ 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x10 } ++ [_]u8{ 0x01, 0x02, 0x00 }),
    };
    for (pages) |page| {
        fba.reset();
        try std.testing.expectError(Error.CorruptParquetPage, decodeDeltaBinaryPacked(fba.allocator(), page, 2));
    }
}

test "decodeDeltaByteArray: values share one buffer, and a page claiming far more bytes than it holds is corrupt" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const prefixes = [_]u8{ 0x80, 0x01, 0x04, 0x02, 0x00, 0x80, 0x40, 0, 0, 0, 0 };
    const suffixes = [_]u8{ 0x80, 0x01, 0x04, 0x02, 0x80, 0x40, 0xFF, 0x3F, 0, 0, 0, 0 };
    const page = prefixes ++ suffixes ++ [_]u8{'x'} ** 4096;

    const vals = try decodeDeltaByteArray(a, &page, 100);
    try testing.expectEqual(@as(usize, 100), vals.len);
    for (vals, 0..) |v, i| {
        try testing.expectEqual(@as(usize, 4096), v.len);
        try testing.expectEqual('x', v[4095]);
        try testing.expectEqual(vals[0].ptr + i * 4096, v.ptr);
    }

    try testing.expectError(Error.CorruptParquetPage, decodeDeltaByteArray(a, &page, 20_000));
}
