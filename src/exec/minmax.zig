//! A running MIN and MAX that owns its values: a value folded in that beats a kept
//! bound is copied out of its batch with `Value.own`. Nulls are skipped, a tie keeps
//! the value seen first, and a value the order cannot place moves neither bound.
//! The order and what a NaN does are parameters, as the users differ in both.

const std = @import("std");
const Value = @import("value.zig").Value;
const eval = @import("eval.zig");

/// What a float NaN does: `ordered` leaves it to the order; `apart` keeps the first
/// one seen beside the bounds, which then bound the other values, while `lo`/`hi`
/// give the total order's answer, a NaN above everything; `poison` drops both bounds
/// for good, as Parquet statistics leave NaN out of min/max.
pub const Nan = enum { ordered, apart, poison };

/// `compareValues` order, a pair it cannot place counted a tie.
pub const ValueOrder = struct {
    pub fn below(v: Value, kept: Value) bool {
        return (eval.compareValues(v, kept) orelse .eq) == .lt;
    }

    pub fn above(v: Value, kept: Value) bool {
        return (eval.compareValues(v, kept) orelse .eq) == .gt;
    }
};

/// The tracker under `Ord`, whose `below(v, kept)` and `above(v, kept)` tell whether
/// `v` beats the kept MIN and the kept MAX, with NaN handled as `rule` says.
pub fn MinMax(comptime Ord: type, comptime rule: Nan) type {
    return struct {
        min: ?Value = null,
        max: ?Value = null,
        nan: NanState = switch (rule) {
            .ordered => {},
            .apart => null,
            .poison => false,
        },

        const Self = @This();

        pub const NanState = switch (rule) {
            .ordered => void,
            .apart => ?f64,
            .poison => bool,
        };

        pub inline fn fold(self: *Self, a: std.mem.Allocator, v: Value) !void {
            if (v == .null) return;
            switch (rule) {
                .ordered => {},
                .apart => if (v == .float and std.math.isNan(v.float)) {
                    if (self.nan == null) self.nan = v.float;
                    return;
                },
                .poison => {
                    if (self.nan) return;
                    if (v == .float and std.math.isNan(v.float)) {
                        self.nan = true;
                        self.min = null;
                        self.max = null;
                        return;
                    }
                },
            }
            try self.widen(a, v, v);
        }

        /// The bounds widened by another span's `low` and `high`, already folded
        /// (neither null nor NaN is looked for).
        pub inline fn widen(self: *Self, a: std.mem.Allocator, low: Value, high: Value) !void {
            if (self.min == null or Ord.below(low, self.min.?)) self.min = try low.own(a);
            if (self.max == null or Ord.above(high, self.max.?)) self.max = try high.own(a);
        }

        pub fn lo(self: Self) ?Value {
            return if (rule == .apart) (self.min orelse if (self.nan) |x| Value{ .float = x } else null) else self.min;
        }

        pub fn hi(self: Self) ?Value {
            return if (rule == .apart) (if (self.nan) |x| Value{ .float = x } else self.max) else self.max;
        }
    };
}

const testing = std.testing;
const valueEq = @import("keyhash.zig").valueEq;

fn sameBound(want: ?Value, got: ?Value) !void {
    try testing.expectEqual(want == null, got == null);
    const w = want orelse return;
    const g = got.?;
    try testing.expectEqual(std.meta.activeTag(w), std.meta.activeTag(g));
    if (w == .float) return testing.expectEqual(@as(u64, @bitCast(w.float)), @as(u64, @bitCast(g.float)));
    try testing.expect(valueEq(w, g));
}

test "MinMax apart: lo and hi are the old keyset fold's bounds, NaN payload included" {
    const Orig = struct {
        fn pinnedKeysetBounds(vals: []const Value) [2]?Value {
            var lo: ?Value = null;
            var hi: ?Value = null;
            for (vals) |v| {
                if (v == .null) continue;
                if (lo == null or (eval.compareValues(v, lo.?) orelse .eq) == .lt) lo = v;
                if (hi == null or (eval.compareValues(v, hi.?) orelse .eq) == .gt) hi = v;
            }
            return .{ lo, hi };
        }
    };
    const Apart = MinMax(ValueOrder, .apart);
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const nan = std.math.nan(f64);
    const odd: f64 = @bitCast(@as(u64, 0xfff8000000000123));
    const pool = [_]Value{ .null, .{ .float = nan }, .{ .float = odd }, .{ .float = -0.0 }, .{ .float = 0.0 }, .{ .float = 1.5 }, .{ .float = -std.math.inf(f64) }, .{ .float = std.math.inf(f64) } };
    var prng = std.Random.DefaultPrng.init(7);
    const rand = prng.random();
    for (0..4000) |_| {
        var buf: [6]Value = undefined;
        const vals = buf[0..rand.uintAtMost(usize, buf.len)];
        for (vals) |*v| v.* = pool[rand.uintLessThan(usize, pool.len)];
        var mm = Apart{};
        for (vals) |v| try mm.fold(a, v);
        const want = Orig.pinnedKeysetBounds(vals);
        try sameBound(want[0], mm.lo());
        try sameBound(want[1], mm.hi());
    }
    var t = Apart{};
    for ([_][]const u8{ "m", "b", "z", "b" }) |s| try t.fold(a, .{ .string = s });
    try testing.expectEqualStrings("b", t.lo().?.string);
    try testing.expectEqualStrings("z", t.hi().?.string);
    try testing.expect(t.nan == null);
}

test "MinMax poison: a NaN drops both bounds for good, nulls are skipped" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var s = MinMax(ValueOrder, .poison){};
    try s.fold(a, .null);
    try s.fold(a, .{ .float = 2 });
    try s.fold(a, .{ .float = -1 });
    try testing.expectEqual(@as(f64, -1), s.min.?.float);
    try testing.expectEqual(@as(f64, 2), s.max.?.float);
    try s.fold(a, .{ .float = std.math.nan(f64) });
    try s.fold(a, .{ .float = 7 });
    try testing.expect(s.min == null and s.max == null and s.nan);
}

test "MinMax: a tie keeps the first value, an unplaceable one moves neither bound" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var s = MinMax(ValueOrder, .ordered){};
    try s.fold(a, .{ .decimal = .{ .unscaled = 15, .scale = 1 } });
    try s.fold(a, .{ .decimal = .{ .unscaled = 150, .scale = 2 } });
    try s.fold(a, .{ .bool = true });
    try testing.expectEqual(@as(u8, 1), s.min.?.decimal.scale);
    try testing.expectEqual(@as(u8, 1), s.max.?.decimal.scale);
}
