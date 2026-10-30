//! A register-blocked Bloom filter over 64-bit key hashes, for runtime filters: a
//! key sets four bits of one u64 word, so a lookup reads one word. The word is the
//! top bits of the key's `fmix64`, the four bits its low 24 bits, six apiece, so a
//! hash whose bits a hash table already spent still spreads. At 16 bits per key
//! about 2% of absent keys pass, and a filter is never under `min_bytes`, so one of a
//! few keys (an `IN` list's) lets almost none pass. It never answers absent for a
//! key it holds.

const keyhash = @import("../keyhash.zig");
const std = @import("std");

pub const bits_per_key = 16;
pub const max_bytes: usize = 64 << 20;
pub const min_bytes: usize = 8 << 10;

pub const Bloom = struct {
    words: []u64,
    log: u6,

    /// Room for `n` keys at `bits_per_key`, a power of two of words from `min_bytes`
    /// to `max_bytes`; one word for no key.
    pub fn init(a: std.mem.Allocator, n: usize) !Bloom {
        const want = if (n == 0) 1 else @max(min_bytes / 8, (n * bits_per_key + 63) / 64);
        var words: usize = 1;
        while (words < want and words * 8 < max_bytes) words *= 2;
        const w = try a.alloc(u64, words);
        @memset(w, 0);
        return .{ .words = w, .log = @intCast(std.math.log2_int(usize, words)) };
    }

    fn word(self: Bloom, m: u64) usize {
        if (self.log == 0) return 0;
        return @intCast(m >> @intCast(64 - @as(u7, self.log)));
    }

    fn mask(m: u64) u64 {
        const one: u64 = 1;
        return one << @intCast(m & 63) | one << @intCast(m >> 6 & 63) | one << @intCast(m >> 12 & 63) | one << @intCast(m >> 18 & 63);
    }

    pub fn insert(self: Bloom, h: u64) void {
        const m = keyhash.fmix64(h);
        self.words[self.word(m)] |= mask(m);
    }

    pub fn mayContain(self: Bloom, h: u64) bool {
        const m = keyhash.fmix64(h);
        const k = mask(m);
        return self.words[self.word(m)] & k == k;
    }
};

const testing = std.testing;

test "bloom: every inserted hash is found, and few absent ones pass" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const n = 50_000;
    const b = try Bloom.init(a, n);
    var rng = std.Random.DefaultPrng.init(42);
    const r = rng.random();
    const keys = try a.alloc(u64, n);
    for (keys) |*k| {
        k.* = r.int(u64);
        b.insert(k.*);
    }
    for (keys) |k| try testing.expect(b.mayContain(k));
    var passed: usize = 0;
    const trials = 200_000;
    for (0..trials) |_| passed += @intFromBool(b.mayContain(r.int(u64)));
    try testing.expect(passed * 100 < trials * 3);
}

test "bloom: one word for no key, at least min_bytes for a few, sized by keys past that" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const few = try Bloom.init(a, 57);
    try testing.expectEqual(min_bytes / 8, few.words.len);
    for (0..57) |i| few.insert(i *% 0x9E3779B97F4A7C15);
    var passed: usize = 0;
    for (0..100_000) |i| passed += @intFromBool(few.mayContain((i + 1000) *% 0x2545F4914F6CDD1D));
    try testing.expect(passed < 10);
    const empty = try Bloom.init(a, 0);
    try testing.expectEqual(@as(usize, 1), empty.words.len);
    try testing.expect(!empty.mayContain(7));
    try testing.expectEqual(@as(usize, 8192), (try Bloom.init(a, 30_000)).words.len);
}
