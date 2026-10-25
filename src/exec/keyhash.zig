//! Value-keyed hashing for group-by, distinct and join. Key `Value`s are hashed and
//! compared directly: a stored key is `[]const Value` deep-copied into plan state,
//! and the caller builds a transient probe key (aliasing batch memory) in scratch
//! and looks it up with `getOrPut`. Backed by `std.HashMap`, already a
//! Swiss/F14-style open-addressing table.
//!
//! Do not use `getOrPutAdapted`: in Zig 0.15.2 the adapted-probe path is ~30x
//! slower than `getOrPut` with a prebuilt key, so callers materialize a small key
//! slice per row in the scratch arena instead.
//!
//! Invariant: values that compare equal always hash equal. The numeric kinds need
//! care because they compare numerically while their representations differ
//! (`1.5` and `1.50`, `0.0` and `-0.0`, every NaN, `1 == 1.0 == 1.00`), so int,
//! float and decimal share one tag and are canonicalized to an f64 before hashing.
//! Past bugs from getting this wrong: a join between an int key and a float key
//! matched nothing, a join dropped `-0.0` rows that `WHERE f = 0` matched, and
//! GROUP BY and DISTINCT disagreed on NaN and decimal-scale groups. Decimal equality
//! is exact while the f64 is not, so distinct 18-digit decimals may share a hash.
//!
//! Nulls group together under `valueEq`, but never match in a join: callers skip
//! null keys before inserting into or probing a `SingleKeyCtx` map.

const std = @import("std");
const Value = @import("value.zig").Value;
const Column = @import("column.zig").Column;
const eval = @import("eval.zig");
const types = @import("../lang/types.zig");

/// The canonical bit pattern for a float key: -0.0 becomes 0.0 and every NaN one NaN,
/// matching `valueEq`, which compares floats numerically.
pub fn canonF64(x: f64) f64 {
    if (std.math.isNan(x)) return std.math.nan(f64);
    return if (x == 0) 0 else x;
}

/// The value a group or distinct row keeps for its key: a float canonical, so which
/// of `0.0` and `-0.0` (or which NaN) a key prints never depends on row order,
/// thread count or which key path ran.
pub fn canonValue(v: Value) Value {
    return if (v == .float) .{ .float = canonF64(v.float) } else v;
}

/// How a typed key path stores one column's key as a 64-bit word whose equality is
/// `valueEq`'s within that kind; null for a kind it boxes.
pub const KeyKind = enum { i64k, i32k, boolk, f64k, strk };

/// The most keys a fixed-width encoding takes: each row's NULL keys are one bit
/// each in a `u64` mask, so wider keys are boxed.
pub const max_fixed_keys = 64;

pub fn keyKindOf(kind: types.TypeKind) ?KeyKind {
    return switch (kind) {
        .int, .time, .timestamp => .i64k,
        .date => .i32k,
        .bool => .boolk,
        .float => .f64k,
        .string => .strk,
        else => null,
    };
}

pub const Meet = struct { want: types.TypeKind, kind: ?KeyKind };

/// The words a build key column of `build` stands for against a probe column of
/// `other` (null: unknown, the build's own). An INT meeting a FLOAT keys as the float
/// it compares as under `valueEq`, so the result does not depend on which side is
/// built.
pub fn meetKind(build: types.TypeKind, other: ?types.TypeKind) Meet {
    const p = other orelse build;
    if ((build == .int and p == .float) or (build == .float and p == .int)) return .{ .want = .float, .kind = .f64k };
    return .{ .want = build, .kind = keyKindOf(build) };
}

/// A float key's word: its canonical bits, so equal floats share one word.
pub inline fn floatWord(x: f64) i64 {
    return @bitCast(canonF64(x));
}

/// murmur3's 64-bit finalizer.
pub fn fmix64(x0: u64) u64 {
    var x = x0;
    x ^= x >> 33;
    x *%= 0xff51afd7ed558ccd;
    x ^= x >> 33;
    x *%= 0xc4ceb9fe1a85ec53;
    x ^= x >> 33;
    return x;
}

const num_tag: u8 = 0xFF;

pub fn hashNum(h: *std.hash.Wyhash, x: f64) void {
    h.update(&[_]u8{num_tag});
    const c = canonF64(x);
    h.update(std.mem.asBytes(&c));
}

/// An int hashes as the f64 it compares as, since `2^53 + 1` equals `9007199254740992.0`
/// under `compareValues`. Ints past 2^53 therefore collide in runs; hashing them
/// exactly silently emptied that join. Change int-versus-float equality first.
pub fn hashInt(h: *std.hash.Wyhash, x: i64) void {
    hashNum(h, @floatFromInt(x));
}

/// A non-numeric value's tag then payload, so a column-typed hasher can fold a cell
/// without boxing it and still agree with `hashValue` bit for bit.
pub fn hashTagged(h: *std.hash.Wyhash, tag: std.meta.Tag(Value), payload: []const u8) void {
    h.update(&[_]u8{@intFromEnum(tag)});
    h.update(payload);
}

pub fn hashValue(h: *std.hash.Wyhash, v: Value) void {
    switch (v) {
        .int => |x| hashInt(h, x),
        .float => |x| hashNum(h, x),
        .decimal => |d| hashNum(h, d.keyF64()),
        .null => hashTagged(h, .null, &.{}),
        .bool => |x| hashTagged(h, .bool, &[_]u8{@intFromBool(x)}),
        .string => |s| hashTagged(h, .string, s),
        .bytes => |s| hashTagged(h, .bytes, s),
        .date => |x| hashTagged(h, .date, std.mem.asBytes(&x)),
        .time => |x| hashTagged(h, .time, std.mem.asBytes(&x)),
        .timestamp => |x| hashTagged(h, .timestamp, std.mem.asBytes(&x)),
    }
}

/// `hashValue` of valid row `row` of `col`, read from its storage without boxing. A
/// nested value, which boxes to null, hashes by its encoded bytes under the bytes tag.
pub inline fn hashCell(h: *std.hash.Wyhash, col: *const Column, row: usize) void {
    switch (col.ty.kind) {
        .int => hashInt(h, col.data.i64[row]),
        .float => hashNum(h, col.data.f64[row]),
        .time => hashTagged(h, .time, std.mem.asBytes(&col.data.i64[row])),
        .timestamp => hashTagged(h, .timestamp, std.mem.asBytes(&col.data.i64[row])),
        .string => hashTagged(h, .string, col.data.bytes.at(row)),
        .bytes, .array, .@"struct" => hashTagged(h, .bytes, col.data.bytes.at(row)),
        .bool, .date, .decimal => hashValue(h, col.getValue(row)),
    }
}

pub fn hashOne(v: Value) u64 {
    var h = std.hash.Wyhash.init(0);
    hashValue(&h, v);
    return h.final();
}

/// Grouping equality: two nulls are equal; otherwise string and bytes compare by
/// bytes and the rest via `compareValues`. Text never equals another kind, in
/// either order, since the two hash apart.
pub fn valueEq(a: Value, b: Value) bool {
    const an = a.isNull();
    const bn = b.isNull();
    if (an or bn) return an and bn;
    return switch (a) {
        .string, .bytes => |s| switch (b) {
            .string, .bytes => |t| std.mem.eql(u8, s, t),
            else => false,
        },
        else => switch (b) {
            .string, .bytes => false,
            else => (eval.compareValues(a, b) orelse return false) == .eq,
        },
    };
}

/// A text or bytes value copied into `a`, out of a batch that may be freed.
pub fn ownValue(a: std.mem.Allocator, v: Value) !Value {
    return switch (v) {
        .string => |s| .{ .string = try a.dupe(u8, s) },
        .bytes => |s| .{ .bytes = try a.dupe(u8, s) },
        else => v,
    };
}

/// The running MIN and MAX of the non-null values folded, in `compareValues` order,
/// owned in the fold's allocator. A NaN is kept apart (`nan`, the first one seen), so
/// `min`/`max` bound the other values; `lo`/`hi` are the total order's own answer,
/// where a NaN sits above everything. A value that does not compare moves neither.
pub const MinMax = struct {
    min: ?Value = null,
    max: ?Value = null,
    nan: ?f64 = null,

    pub fn fold(self: *MinMax, a: std.mem.Allocator, v: Value) !void {
        if (v == .null) return;
        if (v == .float and std.math.isNan(v.float)) {
            if (self.nan == null) self.nan = v.float;
            return;
        }
        if (self.min == null or (eval.compareValues(v, self.min.?) orelse .eq) == .lt) self.min = try ownValue(a, v);
        if (self.max == null or (eval.compareValues(v, self.max.?) orelse .eq) == .gt) self.max = try ownValue(a, v);
    }

    pub fn lo(self: MinMax) ?Value {
        return self.min orelse if (self.nan) |x| Value{ .float = x } else null;
    }

    pub fn hi(self: MinMax) ?Value {
        return if (self.nan) |x| .{ .float = x } else self.max;
    }
};

pub const MultiKeyCtx = struct {
    pub fn hash(_: MultiKeyCtx, key: []const Value) u64 {
        var h = std.hash.Wyhash.init(0);
        for (key) |v| hashValue(&h, v);
        return h.final();
    }
    pub fn eql(_: MultiKeyCtx, a: []const Value, b: []const Value) bool {
        if (a.len != b.len) return false;
        for (a, b) |x, y| if (!valueEq(x, y)) return false;
        return true;
    }
};

pub const SingleKeyCtx = struct {
    pub fn hash(_: SingleKeyCtx, key: Value) u64 {
        return hashOne(key);
    }
    pub fn eql(_: SingleKeyCtx, a: Value, b: Value) bool {
        return valueEq(a, b);
    }
};

const testing = std.testing;

test "equal values hash equal; payload and type tag discriminate" {
    try testing.expectEqual(hashOne(.{ .int = 42 }), hashOne(.{ .int = 42 }));
    try testing.expectEqual(hashOne(.{ .string = "abc" }), hashOne(.{ .string = "abc" }));
    try testing.expectEqual(hashOne(.null), hashOne(.null));
    try testing.expect(hashOne(.{ .int = 42 }) != hashOne(.{ .int = 43 }));
    try testing.expect(hashOne(.{ .int = 1 }) != hashOne(.{ .timestamp = 1 }));
    try testing.expect(hashOne(.null) != hashOne(.{ .int = 0 }));
    try testing.expect(hashOne(.{ .string = "" }) != hashOne(.null));
}

test "distinct int keys produce no 64-bit collisions over a dense domain" {
    var hashes: [512]u64 = undefined;
    for (&hashes, 0..) |*h, i| h.* = hashOne(.{ .int = @intCast(i) });
    std.mem.sort(u64, &hashes, {}, std.sort.asc(u64));
    for (hashes[0 .. hashes.len - 1], hashes[1..]) |a, b| try testing.expect(a != b);
}

test "valueEq: nulls group together, null never equals a value, non-numeric mixed types unequal" {
    try testing.expect(valueEq(.null, .null));
    try testing.expect(!valueEq(.null, .{ .int = 0 }));
    try testing.expect(!valueEq(.{ .string = "" }, .null));
    try testing.expect(valueEq(.{ .string = "a" }, .{ .string = "a" }));
    try testing.expect(!valueEq(.{ .string = "a" }, .{ .string = "b" }));
    try testing.expect(valueEq(.{ .float = 2.5 }, .{ .float = 2.5 }));
    try testing.expect(valueEq(.{ .bool = true }, .{ .bool = true }));
    try testing.expect(!valueEq(.{ .string = "1" }, .{ .int = 1 }));
    try testing.expect(!valueEq(.{ .bool = true }, .{ .int = 1 }));
}

test "MultiKeyCtx: composite equality and order-sensitive hashing" {
    const ctx = MultiKeyCtx{};
    const k1 = [_]Value{ .{ .int = 1 }, .{ .string = "x" } };
    const k2 = [_]Value{ .{ .int = 1 }, .{ .string = "x" } };
    const k3 = [_]Value{ .{ .string = "x" }, .{ .int = 1 } };
    try testing.expect(ctx.eql(&k1, &k2));
    try testing.expectEqual(ctx.hash(&k1), ctx.hash(&k2));
    try testing.expect(!ctx.eql(&k1, &k3));
    try testing.expect(ctx.hash(&k1) != ctx.hash(&k3));
    try testing.expect(!ctx.eql(k1[0..1], &k2));

    const n1 = [_]Value{.null};
    const n2 = [_]Value{.null};
    const z0 = [_]Value{.{ .int = 0 }};
    try testing.expect(ctx.eql(&n1, &n2));
    try testing.expectEqual(ctx.hash(&n1), ctx.hash(&n2));
    try testing.expect(!ctx.eql(&n1, &z0));
}

test "hashValue: numerically equal decimals hash alike, so DISTINCT counts them once" {
    const a = Value{ .decimal = .{ .unscaled = 15, .scale = 1 } };
    const b = Value{ .decimal = .{ .unscaled = 150, .scale = 2 } };
    try testing.expect(valueEq(a, b));
    try testing.expectEqual(hashOne(a), hashOne(b));

    try testing.expectEqual(
        hashOne(.{ .decimal = .{ .unscaled = 0, .scale = 0 } }),
        hashOne(.{ .decimal = .{ .unscaled = 0, .scale = 4 } }),
    );
    try testing.expectEqual(
        hashOne(.{ .decimal = .{ .unscaled = -15, .scale = 1 } }),
        hashOne(.{ .decimal = .{ .unscaled = -1500, .scale = 3 } }),
    );
    try testing.expect(!valueEq(a, .{ .decimal = .{ .unscaled = 151, .scale = 2 } }));
}

test "hashValue: -0.0 and NaN hash by value, matching valueEq" {
    const zero = Value{ .float = 0.0 };
    const neg_zero = Value{ .float = -0.0 };
    try testing.expect(valueEq(zero, neg_zero));
    try testing.expectEqual(hashOne(zero), hashOne(neg_zero));

    const nan_a = Value{ .float = std.math.nan(f64) };
    const nan_b = Value{ .float = -std.math.nan(f64) };
    try testing.expect(valueEq(nan_a, nan_b));
    try testing.expectEqual(hashOne(nan_a), hashOne(nan_b));

    try testing.expect(!valueEq(nan_a, zero));
    try testing.expect(hashOne(nan_a) != hashOne(zero));
}

test "canonValue and floatWord: equal floats keep one value and one word" {
    try testing.expectEqual(@as(u64, 0), @as(u64, @bitCast(canonValue(.{ .float = -0.0 }).float)));
    try testing.expectEqual(floatWord(0.0), floatWord(-0.0));
    try testing.expectEqual(floatWord(std.math.nan(f64)), floatWord(-std.math.nan(f64)));
    try testing.expectEqual(@as(i64, -3), canonValue(.{ .int = -3 }).int);
}

test "hashValue: numerically equal values hash alike ACROSS int/float/decimal" {
    const one_i = Value{ .int = 1 };
    const one_f = Value{ .float = 1.0 };
    const one_d = Value{ .decimal = .{ .unscaled = 100, .scale = 2 } };
    try testing.expect(valueEq(one_i, one_f));
    try testing.expect(valueEq(one_i, one_d));
    try testing.expectEqual(hashOne(one_i), hashOne(one_f));
    try testing.expectEqual(hashOne(one_i), hashOne(one_d));

    try testing.expect(hashOne(one_i) != hashOne(.{ .int = 2 }));
    try testing.expect(!valueEq(one_i, .{ .string = "1" }));
    try testing.expect(hashOne(one_i) != hashOne(.{ .string = "1" }));
    try testing.expect(hashOne(one_i) != hashOne(.{ .timestamp = 1 }));
}

test "decimals compare exactly, and equal ones hash alike at any scale and size" {
    const a = Value{ .decimal = .{ .unscaled = 123456789012345678, .scale = 8 } };
    const b = Value{ .decimal = .{ .unscaled = 123456789012345679, .scale = 8 } };
    try testing.expect(!valueEq(a, b));
    const a_wide = Value{ .decimal = .{ .unscaled = 12345678901234567800, .scale = 10 } };
    try testing.expect(valueEq(a, a_wide));
    try testing.expectEqual(hashOne(a), hashOne(a_wide));
    const big = Value{ .decimal = .{ .unscaled = 10000000000000001, .scale = 0 } };
    try testing.expect(!valueEq(big, .{ .int = 10000000000000000 }));
    try testing.expect(valueEq(big, .{ .int = 10000000000000001 }));
    try testing.expectEqual(hashOne(big), hashOne(.{ .int = 10000000000000001 }));
    const huge = Value{ .decimal = .{ .unscaled = 99999999999999999999999999999999999990, .scale = 30 } };
    const huge_short = Value{ .decimal = .{ .unscaled = 9999999999999999999999999999999999999, .scale = 29 } };
    try testing.expect(valueEq(huge, huge_short));
    try testing.expectEqual(hashOne(huge), hashOne(huge_short));
}

test "valueEq is symmetric: text never equals a date, whichever side it is on" {
    const d = Value{ .date = 19000 };
    const t = Value{ .string = "2022-01-08" };
    try std.testing.expect(!valueEq(d, t));
    try std.testing.expect(!valueEq(t, d));
    try std.testing.expect(valueEq(.{ .int = 1 }, .{ .float = 1.0 }) == valueEq(.{ .float = 1.0 }, .{ .int = 1 }));
}

test "meetKind: INT against FLOAT keys as FLOAT from either side; otherwise the build's own" {
    try testing.expectEqual(Meet{ .want = .float, .kind = .f64k }, meetKind(.int, .float));
    try testing.expectEqual(Meet{ .want = .float, .kind = .f64k }, meetKind(.float, .int));
    try testing.expectEqual(Meet{ .want = .int, .kind = .i64k }, meetKind(.int, null));
    try testing.expectEqual(Meet{ .want = .int, .kind = .i64k }, meetKind(.int, .decimal));
    try testing.expectEqual(Meet{ .want = .decimal, .kind = null }, meetKind(.decimal, .int));
    try testing.expectEqual(Meet{ .want = .string, .kind = .strk }, meetKind(.string, .bytes));
}

test "hashCell: every scalar kind hashes as its boxed value and as the old join row hash" {
    const Orig = struct {
        fn pinnedJoinHash(c: *const Column, row: usize) u64 {
            var h = std.hash.Wyhash.init(0);
            switch (std.meta.activeTag(c.data)) {
                .i64 => switch (c.ty.kind) {
                    .int => hashInt(&h, c.data.i64[row]),
                    .time => hashTagged(&h, .time, std.mem.asBytes(&c.data.i64[row])),
                    .timestamp => hashTagged(&h, .timestamp, std.mem.asBytes(&c.data.i64[row])),
                    else => hashValue(&h, c.getValue(row)),
                },
                .bytes => hashTagged(&h, if (c.ty.kind == .string) .string else .bytes, c.data.bytes.at(row)),
                else => hashValue(&h, c.getValue(row)),
            }
            return h.final();
        }
    };
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const D = @import("value.zig").Decimal;
    const nan = std.math.nan(f64);
    const cases = [_]struct { ty: types.Type, vals: []const Value }{
        .{ .ty = types.Type.init(.int), .vals = &.{ .{ .int = 0 }, .{ .int = -1 }, .{ .int = std.math.maxInt(i64) }, .{ .int = 9007199254740993 } } },
        .{ .ty = types.Type.init(.float), .vals = &.{ .{ .float = 0.0 }, .{ .float = -0.0 }, .{ .float = nan }, .{ .float = -nan }, .{ .float = 1.5 }, .{ .float = std.math.inf(f64) } } },
        .{ .ty = types.Type.init(.time), .vals = &.{ .{ .time = 0 }, .{ .time = 86_399_999_999 } } },
        .{ .ty = types.Type.init(.timestamp), .vals = &.{ .{ .timestamp = -1 }, .{ .timestamp = 1 } } },
        .{ .ty = types.Type.init(.date), .vals = &.{ .{ .date = 0 }, .{ .date = -719162 } } },
        .{ .ty = types.Type.init(.bool), .vals = &.{ .{ .bool = true }, .{ .bool = false } } },
        .{ .ty = types.Type.init(.string), .vals = &.{ .{ .string = "" }, .{ .string = "a\x00b" }, .{ .string = "\xff" } } },
        .{ .ty = types.Type.init(.bytes), .vals = &.{ .{ .bytes = "" }, .{ .bytes = "\x01\x02" } } },
        .{ .ty = types.Type.decimal(10, 2), .vals = &.{ .{ .decimal = D{ .unscaled = 150, .scale = 2 } }, .{ .decimal = D{ .unscaled = -1, .scale = 2 } } } },
    };
    for (cases) |cs| {
        var b = @import("column.zig").Builder.init(a, cs.ty);
        for (cs.vals) |v| try b.append(v);
        const col = try b.finish();
        for (0..cs.vals.len) |r| {
            var h = std.hash.Wyhash.init(0);
            hashCell(&h, &col, r);
            const got = h.final();
            try testing.expectEqual(hashOne(col.getValue(r)), got);
            try testing.expectEqual(Orig.pinnedJoinHash(&col, r), got);
        }
    }
}

fn sameBound(want: ?Value, got: ?Value) !void {
    try testing.expectEqual(want == null, got == null);
    const w = want orelse return;
    const g = got.?;
    try testing.expectEqual(std.meta.activeTag(w), std.meta.activeTag(g));
    if (w == .float) return testing.expectEqual(@as(u64, @bitCast(w.float)), @as(u64, @bitCast(g.float)));
    try testing.expect(valueEq(w, g));
}

test "MinMax: lo and hi are the old keyset fold's bounds, NaN payload included" {
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
        var mm = MinMax{};
        for (vals) |v| try mm.fold(a, v);
        const want = Orig.pinnedKeysetBounds(vals);
        try sameBound(want[0], mm.lo());
        try sameBound(want[1], mm.hi());
    }
    var t = MinMax{};
    for ([_][]const u8{ "m", "b", "z", "b" }) |s| try t.fold(a, .{ .string = s });
    try testing.expectEqualStrings("b", t.lo().?.string);
    try testing.expectEqualStrings("z", t.hi().?.string);
    try testing.expect(t.nan == null);
}
