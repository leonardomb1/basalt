//! The portable 64-bit RoaringBitmap a Delta deletion vector holds, after its
//! magic: a u64 bucket count, then per bucket a u32 key (a row's high 32 bits) and
//! a standard 32-bit RoaringBitmap of the low 32 bits, all little-endian. A 32-bit
//! bitmap opens with a cookie (12346: a container count follows and no run
//! containers; else `12347 | (n-1) << 16`, then a run-flag bitset), a key and
//! cardinality-1 per container, an offset per container where the format writes
//! one (cookie 12346, or four containers or more), then the containers: runs,
//! arrays of up to 4096 sorted values, or bitsets of 1024 words.
//!
//! The bytes are checked once and indexed by container, never expanded: a scan
//! clears the rows of one row group at a time (`clear`), visiting only the
//! containers that overlap it. Every count and length comes off the wire, so each
//! is bounds-checked; a malformed bitmap is `DeletionVectorCorrupt`, never a trap.

const std = @import("std");
const testing = std.testing;

pub const Error = error{DeletionVectorCorrupt} || std.mem.Allocator.Error;

const no_runs = 12346;
const with_runs = 12347;
const no_offset_threshold = 4;
const max_array = 4096;
const bitset_len = 8192;

pub const Container = struct {
    /// The row's bits above the low 16: the bucket key and the container key.
    high: u64,
    kind: Kind,
    data: []const u8,

    pub const Kind = enum { array, bitset, run };
};

pub const Bitmap = struct {
    containers: []const Container = &.{},
    cardinality: u64 = 0,
    /// One past the largest row held; 0 when none is.
    end: u64 = 0,

    /// Marks false each row of `[base, base + keep.len)` the bitmap holds, returning
    /// how many it marked.
    pub fn clear(self: *const Bitmap, base: u64, keep: []bool) usize {
        return self.walk(base, keep.len, keep);
    }

    /// How many rows of `[base, base + len)` the bitmap holds, marking nothing, so
    /// a caller can learn a range is wholly deleted before it sizes a mask for it.
    pub fn count(self: *const Bitmap, base: u64, len: usize) usize {
        return self.walk(base, len, null);
    }

    fn walk(self: *const Bitmap, base: u64, len: usize, keep: ?[]bool) usize {
        if (len == 0 or base >= self.end) return 0;
        const last = base +| (len - 1);
        var lo: usize = 0;
        var hi: usize = self.containers.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.containers[mid].high < base >> 16) lo = mid + 1 else hi = mid;
        }
        var n: usize = 0;
        for (self.containers[lo..]) |c| {
            if (c.high > last >> 16) break;
            const at = c.high << 16;
            const from: u32 = if (base > at) @intCast(base - at) else 0;
            const to: u32 = @intCast(@min(last - at, 0xFFFF));
            switch (c.kind) {
                .array => {
                    var i: usize = 0;
                    while (i < c.data.len) : (i += 2) {
                        const v = std.mem.readInt(u16, c.data[i..][0..2], .little);
                        if (v < from or v > to) continue;
                        if (keep) |k| k[@intCast(at + v - base)] = false;
                        n += 1;
                    }
                },
                .run => {
                    var i: usize = 2;
                    while (i < c.data.len) : (i += 4) {
                        const s: u32 = std.mem.readInt(u16, c.data[i..][0..2], .little);
                        const e = s + std.mem.readInt(u16, c.data[i + 2 ..][0..2], .little);
                        const a = @max(s, from);
                        const b = @min(e, to);
                        if (a > b) continue;
                        if (keep) |k| @memset(k[@intCast(at + a - base)..][0 .. b - a + 1], false);
                        n += b - a + 1;
                    }
                },
                .bitset => {
                    var w = from / 64;
                    while (w <= to / 64) : (w += 1) {
                        var bits = std.mem.readInt(u64, c.data[w * 8 ..][0..8], .little);
                        while (bits != 0) : (bits &= bits - 1) {
                            const v = w * 64 + @ctz(bits);
                            if (v < from or v > to) continue;
                            if (keep) |k| k[@intCast(at + v - base)] = false;
                            n += 1;
                        }
                    }
                },
            }
        }
        return n;
    }
};

const Cursor = @import("../cursor.zig").Reader(.little, error.DeletionVectorCorrupt);

/// The bitmap `bytes` (the portable format, its magic already taken off) holds,
/// checked whole and indexed by container into `arena`; `bytes` must outlive it.
pub fn parse(arena: std.mem.Allocator, bytes: []const u8) Error!Bitmap {
    var cur = Cursor{ .p = bytes };
    const buckets = try cur.int(u64);
    if (buckets > bytes.len / 12) return error.DeletionVectorCorrupt;
    var out = std.ArrayListUnmanaged(Container).empty;
    var card: u64 = 0;
    var end: u64 = 0;
    var prev: ?u64 = null;
    for (0..@intCast(buckets)) |_| {
        const key = try cur.int(u32);
        if (key & 0x8000_0000 != 0) return error.DeletionVectorCorrupt;
        const cookie = try cur.int(u32);
        var n: usize = undefined;
        var flags: []const u8 = &.{};
        var offsets = false;
        if (cookie == no_runs) {
            n = try cur.int(u32);
            offsets = true;
        } else if (cookie & 0xFFFF == with_runs) {
            n = (cookie >> 16) + 1;
            flags = try cur.bytes((n + 7) / 8);
            offsets = n >= no_offset_threshold;
        } else return error.DeletionVectorCorrupt;
        if (n > 1 << 16) return error.DeletionVectorCorrupt;
        const header = try cur.bytes(n * 4);
        if (offsets) _ = try cur.bytes(n * 4);
        try out.ensureUnusedCapacity(arena, n);
        for (0..n) |k| {
            const low = std.mem.readInt(u16, header[k * 4 ..][0..2], .little);
            const count = @as(u32, std.mem.readInt(u16, header[k * 4 + 2 ..][0..2], .little)) + 1;
            const high = (@as(u64, key) << 16) | low;
            if (prev) |p| if (high <= p) return error.DeletionVectorCorrupt;
            prev = high;
            const is_run = flags.len > 0 and (flags[k / 8] >> @intCast(k % 8)) & 1 != 0;
            var c = Container{ .high = high, .kind = undefined, .data = undefined };
            var top: u32 = 0;
            if (is_run) {
                const head = cur.i;
                const runs = try cur.int(u16);
                _ = try cur.bytes(@as(usize, runs) * 4);
                c = .{ .high = high, .kind = .run, .data = bytes[head..cur.i] };
                var sum: u32 = 0;
                var after: u32 = 0;
                var i: usize = 2;
                while (i < c.data.len) : (i += 4) {
                    const s: u32 = std.mem.readInt(u16, c.data[i..][0..2], .little);
                    const len: u32 = std.mem.readInt(u16, c.data[i + 2 ..][0..2], .little);
                    if (s < after or s + len > 0xFFFF) return error.DeletionVectorCorrupt;
                    after = s + len + 1;
                    sum += len + 1;
                    top = s + len;
                }
                if (runs == 0 or sum != count) return error.DeletionVectorCorrupt;
            } else if (count <= max_array) {
                c = .{ .high = high, .kind = .array, .data = try cur.bytes(count * 2) };
                var i: usize = 0;
                while (i < c.data.len) : (i += 2) {
                    const v = std.mem.readInt(u16, c.data[i..][0..2], .little);
                    if (i > 0 and v <= top) return error.DeletionVectorCorrupt;
                    top = v;
                }
            } else {
                c = .{ .high = high, .kind = .bitset, .data = try cur.bytes(bitset_len) };
                var sum: u32 = 0;
                var w: usize = 0;
                while (w < bitset_len) : (w += 8) {
                    const bits = std.mem.readInt(u64, c.data[w..][0..8], .little);
                    sum += @popCount(bits);
                    if (bits != 0) top = @intCast(w * 8 + 63 - @clz(bits));
                }
                if (sum != count) return error.DeletionVectorCorrupt;
            }
            card += count;
            end = (high << 16) + top + 1;
            out.appendAssumeCapacity(c);
        }
    }
    return .{ .containers = out.items, .cardinality = card, .end = end };
}

/// The rows `b` holds, in order: what a test compares.
fn rowsOf(a: std.mem.Allocator, b: Bitmap) ![]u64 {
    const keep = try a.alloc(bool, @intCast(b.end));
    @memset(keep, true);
    _ = b.clear(0, keep);
    var out = std.ArrayListUnmanaged(u64).empty;
    for (keep, 0..) |k, i| if (!k) try out.append(a, i);
    return out.items;
}

/// Little-endian bytes built up by a test.
const Buf = struct {
    a: std.mem.Allocator,
    b: std.ArrayListUnmanaged(u8) = .empty,

    fn int(self: *Buf, comptime T: type, v: T) !void {
        var x: [@sizeOf(T)]u8 = undefined;
        std.mem.writeInt(T, &x, v, .little);
        try self.b.appendSlice(self.a, &x);
    }
};

test "the kernel-rs inline vector holds rows 3, 4, 7, 11, 18 and 29" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const bytes = [_]u8{
        0x01, 0, 0, 0, 0, 0, 0,  0, 0,  0, 0,  0, 0x3a, 0x30, 0, 0, 0x01, 0, 0, 0, 0, 0, 0x05, 0, 0x10, 0, 0, 0,
        3,    0, 4, 0, 7, 0, 11, 0, 18, 0, 29, 0,
    };
    const b = try parse(a, &bytes);
    try testing.expectEqualSlices(u64, &.{ 3, 4, 7, 11, 18, 29 }, try rowsOf(a, b));
    try testing.expectEqual(@as(u64, 6), b.cardinality);
    try testing.expectEqual(@as(u64, 30), b.end);
}

test "array, bitset and run containers across buckets, cleared one row group at a time" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var w = Buf{ .a = a };
    try w.int(u64, 2);
    try w.int(u32, 0);
    try w.int(u32, with_runs | (3 - 1) << 16);
    try w.b.append(a, 0b100);
    for ([_]u16{ 0, 1, 1, 4096, 2, 9, 0, 65535 }) |x| try w.int(u16, x);
    for (0..1024) |i| try w.int(u64, if (i == 0) 0b1 else if (i <= 64) ~@as(u64, 0) else 0);
    for ([_]u16{ 2, 0, 4, 65530, 4 }) |x| try w.int(u16, x);
    try w.int(u32, 1);
    try w.int(u32, no_runs);
    try w.int(u32, 1);
    for ([_]u16{ 7, 0 }) |x| try w.int(u16, x);
    try w.int(u32, 0);
    try w.int(u16, 9);
    const b = try parse(a, w.b.items);
    const far = (@as(u64, 1) << 32) | (7 << 16) | 9;
    var want = std.ArrayListUnmanaged(u64).empty;
    try want.appendSlice(a, &.{ 0, 65535, 65536 });
    for (0..4096) |k| try want.append(a, 65536 + 64 + k);
    for (0..5) |k| try want.append(a, (2 << 16) + k);
    for (0..5) |k| try want.append(a, (2 << 16) + 65530 + k);
    try testing.expectEqual(@as(usize, 4), b.containers.len);
    try testing.expectEqual(@as(u64, want.items.len + 1), b.cardinality);
    try testing.expectEqual(far + 1, b.end);
    try testing.expectEqualSlices(u64, want.items, try rowsOf(a, .{ .containers = b.containers[0..3], .end = 3 << 16 }));

    try want.append(a, far);
    var keep: [100]bool = undefined;
    for ([_]u64{ 0, 65500, 65586, 65536 + 4100, (2 << 16) + 65500, 3 << 16, far - 50, far - 99, far + 1, 1 << 40 }) |base| {
        @memset(&keep, true);
        const n = b.clear(base, &keep);
        var held: usize = 0;
        for (keep, 0..) |k, i| {
            const in = std.mem.indexOfScalar(u64, want.items, base + i) != null;
            try testing.expectEqual(!in, k);
            held += @intFromBool(in);
        }
        try testing.expectEqual(held, n);
    }
}

test "an offset header follows a run cookie from four containers on, and an empty bitmap holds nothing" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    for ([_]bool{ false, true }) |runs| {
        var w = Buf{ .a = a };
        try w.int(u64, 1);
        try w.int(u32, 0);
        try w.int(u32, with_runs | (4 - 1) << 16);
        try w.b.append(a, if (runs) 0b1000 else 0);
        for (0..4) |k| {
            try w.int(u16, @intCast(k));
            try w.int(u16, 0);
        }
        for (0..4) |k| try w.int(u32, @intCast(k));
        for (0..3) |k| try w.int(u16, @intCast(k + 1));
        if (runs) {
            try w.int(u16, 1);
            try w.int(u16, 100);
            try w.int(u16, 0);
        } else try w.int(u16, 100);
        const b = try parse(a, w.b.items);
        try testing.expectEqualSlices(u64, &.{ 1, 65538, 131075, 196708 }, try rowsOf(a, b));
    }
    var e = Buf{ .a = a };
    try e.int(u64, 1);
    try e.int(u32, 0);
    try e.int(u32, no_runs);
    try e.int(u32, 0);
    const empty = try parse(a, e.b.items);
    try testing.expectEqual(@as(u64, 0), empty.cardinality);
    try testing.expectEqual(@as(u64, 0), empty.end);
    const none = try parse(a, &(.{0} ** 8));
    try testing.expectEqual(@as(usize, 0), none.containers.len);
}

test "a malformed bitmap is refused, never read past its end" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const good = [_]u8{
        0x01, 0, 0, 0, 0, 0, 0,  0, 0,  0, 0,  0, 0x3a, 0x30, 0, 0, 0x01, 0, 0, 0, 0, 0, 0x05, 0, 0x10, 0, 0, 0,
        3,    0, 4, 0, 7, 0, 11, 0, 18, 0, 29, 0,
    };
    for (0..good.len) |n| try testing.expectError(error.DeletionVectorCorrupt, parse(a, good[0..n]));
    const cases = [_]struct { at: usize, v: u8 }{
        .{ .at = 11, .v = 0x80 },
        .{ .at = 12, .v = 0x3b },
        .{ .at = 32, .v = 2 },
        .{ .at = 0, .v = 0xFF },
    };
    for (cases) |c| {
        var bad = good;
        bad[c.at] = c.v;
        try testing.expectError(error.DeletionVectorCorrupt, parse(a, &bad));
    }

    var run = Buf{ .a = a };
    try run.int(u64, 1);
    try run.int(u32, 0);
    try run.int(u32, with_runs);
    try run.b.append(a, 1);
    try run.int(u16, 0);
    try run.int(u16, 9);
    try run.int(u16, 1);
    try run.int(u16, 65530);
    try run.int(u16, 9);
    try testing.expectError(error.DeletionVectorCorrupt, parse(a, run.b.items));

    var unsorted = Buf{ .a = a };
    try unsorted.int(u64, 1);
    try unsorted.int(u32, 0);
    try unsorted.int(u32, no_runs);
    try unsorted.int(u32, 2);
    for ([_]u16{ 5, 0, 3, 0 }) |x| try unsorted.int(u16, x);
    for (0..2) |_| try unsorted.int(u32, 0);
    for ([_]u16{ 1, 2 }) |x| try unsorted.int(u16, x);
    try testing.expectError(error.DeletionVectorCorrupt, parse(a, unsorted.b.items));

    var fuzz = std.Random.DefaultPrng.init(7);
    var buf: [64]u8 = undefined;
    for (0..20_000) |_| {
        fuzz.random().bytes(&buf);
        @memcpy(buf[0..8], &[_]u8{ 1, 0, 0, 0, 0, 0, 0, 0 });
        @memset(buf[8..12], 0);
        if (parse(a, buf[0 .. fuzz.random().uintLessThan(usize, buf.len) + 1])) |b| {
            var keep: [256]bool = undefined;
            @memset(&keep, true);
            _ = b.clear(0, &keep);
        } else |_| {}
    }
}
