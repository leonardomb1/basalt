//! A DISTINCT aggregate's values as bits, for an INT, TIME, TIMESTAMP or DATE
//! argument of a global aggregate whose result does not depend on the order its
//! values arrive in (`Pairs.orderFree`): COUNT(DISTINCT id) is a bit set per row,
//! where a pair store hashes, files and dedups every row, and two lanes' sets
//! union by OR. Bit `v` stands for value `v` (offset by 2^63, so negatives sort
//! first); the set holds whole 64-bit words from `base` on, growing at either end,
//! and refuses values that would take it past `max_words`, which the pairs then
//! take over (`Pairs.toPairs`).
//!
//! Its partitions are the merge's: words go in chunks of `chunk` dealt round the
//! `keytable.parts` partitions, so each partition unions apart, as a pair store's
//! value-hash partitions do, and its values come out ascending.

const std = @import("std");
const keytable = @import("../../keytable.zig");

pub const max_words: usize = 1 << 19;
const chunk: u64 = 64;
const bias: u64 = 1 << 63;

pub const Bits = struct {
    base: u64 = 0,
    words: std.ArrayListUnmanaged(u64) = .empty,

    pub fn deinit(self: *Bits, gpa: std.mem.Allocator) void {
        self.words.deinit(gpa);
    }

    pub fn bytes(self: *const Bits) usize {
        return self.words.items.len * @sizeOf(u64);
    }

    inline fn biased(v: i64) u64 {
        return @as(u64, @bitCast(v)) +% bias;
    }

    inline fn valueOf(w: u64, bit: u6) i64 {
        return @bitCast(((w << 6) | bit) -% bias);
    }

    /// Whether words `lo..hi` fit, the set grown to hold them; false leaves it as it was.
    fn reach(self: *Bits, gpa: std.mem.Allocator, lo: u64, hi: u64) !bool {
        if (self.words.items.len == 0) {
            if (hi - lo >= max_words) return false;
            self.base = lo;
            try self.words.appendNTimes(gpa, 0, @intCast(hi - lo + 1));
            return true;
        }
        const end = self.base + self.words.items.len;
        const new_lo = @min(lo, self.base);
        const new_end = @max(hi + 1, end);
        if (new_end - new_lo > max_words) return false;
        if (new_end > end) try self.words.appendNTimes(gpa, 0, @intCast(new_end - end));
        if (new_lo < self.base) {
            const front: usize = @intCast(self.base - new_lo);
            const old = self.words.items.len;
            try self.words.ensureUnusedCapacity(gpa, front);
            self.words.items.len += front;
            std.mem.copyBackwards(u64, self.words.items[front..], self.words.items[0..old]);
            @memset(self.words.items[0..front], 0);
            self.base = new_lo;
        }
        return true;
    }

    /// Records `keys`; false, nothing recorded, when they would not fit.
    pub fn addAll(self: *Bits, gpa: std.mem.Allocator, keys: []const i64) !bool {
        if (keys.len == 0) return true;
        var lo: u64 = std.math.maxInt(u64);
        var hi: u64 = 0;
        for (keys) |k| {
            const w = biased(k) >> 6;
            lo = @min(lo, w);
            hi = @max(hi, w);
        }
        if (!try self.reach(gpa, lo, hi)) return false;
        const ws = self.words.items;
        for (keys) |k| {
            const u = biased(k);
            ws[@intCast((u >> 6) - self.base)] |= @as(u64, 1) << @truncate(u);
        }
        return true;
    }

    /// The word indexes of partition `p`'s chunks, a range at a time, to `f(ctx, lo, hi)`.
    fn spans(self: *const Bits, p: usize, ctx: anytype, comptime f: anytype) !void {
        const n = self.words.items.len;
        if (n == 0) return;
        const end = self.base + n;
        const parts: u64 = keytable.parts;
        const c0 = self.base / chunk;
        var c = c0 + (p + parts - c0 % parts) % parts;
        while (c * chunk < end) : (c += parts) {
            const lo = @max(c * chunk, self.base);
            const hi = @min((c + 1) * chunk, end);
            try f(ctx, @as(usize, @intCast(lo - self.base)), @as(usize, @intCast(hi - self.base)));
        }
    }

    /// How many values partition `p` holds.
    pub fn count(self: *const Bits, p: usize) usize {
        const S = struct {
            b: *const Bits,
            n: usize = 0,
            fn add(s: *@This(), lo: usize, hi: usize) !void {
                for (s.b.words.items[lo..hi]) |x| s.n += @popCount(x);
            }
        };
        var s = S{ .b = self };
        self.spans(p, &s, S.add) catch unreachable;
        return s.n;
    }

    /// Partition `p` of `src` added to this set; false when it would not fit.
    pub fn unionPart(self: *Bits, gpa: std.mem.Allocator, src: *const Bits, p: usize) !bool {
        const R = struct {
            b: *const Bits,
            lo: ?usize = null,
            hi: usize = 0,
            fn see(r: *@This(), lo: usize, hi: usize) !void {
                for (r.b.words.items[lo..hi], lo..) |x, i| if (x != 0) {
                    if (r.lo == null) r.lo = i;
                    r.hi = i;
                };
            }
        };
        var r = R{ .b = src };
        try src.spans(p, &r, R.see);
        const first = r.lo orelse return true;
        if (!try self.reach(gpa, src.base + first, src.base + r.hi)) return false;
        const O = struct {
            dst: *Bits,
            src: *const Bits,
            fn put(o: *@This(), lo: usize, hi: usize) !void {
                for (o.src.words.items[lo..hi], lo..) |x, i| {
                    if (x != 0) o.dst.words.items[@intCast(o.src.base + i - o.dst.base)] |= x;
                }
            }
        };
        var o = O{ .dst = self, .src = src };
        try src.spans(p, &o, O.put);
        return true;
    }

    /// Partition `p`'s values, ascending, to `f(ctx, value)`.
    pub fn each(self: *const Bits, p: usize, ctx: anytype, comptime f: anytype) !void {
        const E = struct {
            b: *const Bits,
            ctx: @TypeOf(ctx),
            fn run(e: *@This(), lo: usize, hi: usize) !void {
                for (e.b.words.items[lo..hi], lo..) |x0, i| {
                    var x = x0;
                    while (x != 0) : (x &= x - 1) try f(e.ctx, valueOf(e.b.base + i, @intCast(@ctz(x))));
                }
            }
        };
        var e = E{ .b = self, .ctx = ctx };
        try self.spans(p, &e, E.run);
    }
};

test "bits: values at both ends, negative, grown either way, partitioned and unioned, refusing what would not fit" {
    const gpa = std.testing.allocator;
    var a = Bits{};
    defer a.deinit(gpa);
    try std.testing.expect(try a.addAll(gpa, &.{ 5, 70, 5, -3, 100_000 }));
    try std.testing.expect(try a.addAll(gpa, &.{-200_000}));
    var c = Bits{};
    defer c.deinit(gpa);
    try std.testing.expect(try c.addAll(gpa, &.{ 71, 4_000_000 }));

    var u = Bits{};
    defer u.deinit(gpa);
    var total: usize = 0;
    for (0..keytable.parts) |p| {
        total += a.count(p);
        try std.testing.expect(try u.unionPart(gpa, &c, p));
        try std.testing.expect(try u.unionPart(gpa, &a, p));
    }
    try std.testing.expectEqual(@as(usize, 5), total);

    const Collect = struct {
        fn put(list: *std.ArrayList(i64), v: i64) !void {
            try list.append(std.testing.allocator, v);
        }
    };
    var got: std.ArrayList(i64) = .empty;
    defer got.deinit(gpa);
    for (0..keytable.parts) |p| try u.each(p, &got, Collect.put);
    std.mem.sort(i64, got.items, {}, std.sort.asc(i64));
    try std.testing.expectEqualSlices(i64, &.{ -200_000, -3, 5, 70, 71, 100_000, 4_000_000 }, got.items);

    var far = Bits{};
    defer far.deinit(gpa);
    try std.testing.expect(!try far.addAll(gpa, &.{ 0, @as(i64, max_words) * 64 }));
    try std.testing.expectEqual(@as(usize, 0), far.words.items.len);
    try std.testing.expect(try far.addAll(gpa, &.{0}));
    try std.testing.expect(!try far.addAll(gpa, &.{@as(i64, max_words) * 64}));
    try std.testing.expect(try far.addAll(gpa, &.{ std.math.minInt(i64), std.math.minInt(i64) + 63 }) == false);
}
