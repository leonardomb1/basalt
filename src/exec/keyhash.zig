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
//! GROUP BY and DISTINCT disagreed on NaN and decimal-scale groups.
//!
//! Nulls group together under `valueEq`, but never match in a join: callers skip
//! null keys before inserting into or probing a `SingleKeyCtx` map.

const std = @import("std");
const Value = @import("value.zig").Value;
const eval = @import("eval.zig");

/// The canonical bit pattern for a float key: -0.0 becomes 0.0 and every NaN one NaN,
/// matching `valueEq`, which compares floats numerically.
pub fn canonF64(x: f64) f64 {
    if (std.math.isNan(x)) return std.math.nan(f64);
    return if (x == 0) 0 else x;
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
        .float, .decimal => hashNum(h, eval.toF64(v)),
        .null => hashTagged(h, .null, &.{}),
        .bool => |x| hashTagged(h, .bool, &[_]u8{@intFromBool(x)}),
        .string => |s| hashTagged(h, .string, s),
        .bytes => |s| hashTagged(h, .bytes, s),
        .date => |x| hashTagged(h, .date, std.mem.asBytes(&x)),
        .time => |x| hashTagged(h, .time, std.mem.asBytes(&x)),
        .timestamp => |x| hashTagged(h, .timestamp, std.mem.asBytes(&x)),
    }
}

pub fn hashOne(v: Value) u64 {
    var h = std.hash.Wyhash.init(0);
    hashValue(&h, v);
    return h.final();
}

/// Grouping equality: two nulls are equal; otherwise string and bytes compare by
/// bytes and the rest via `compareValues`.
pub fn valueEq(a: Value, b: Value) bool {
    const an = a.isNull();
    const bn = b.isNull();
    if (an or bn) return an and bn;
    return switch (a) {
        .string, .bytes => |s| switch (b) {
            .string, .bytes => |t| std.mem.eql(u8, s, t),
            else => false,
        },
        else => (eval.compareValues(a, b) orelse return false) == .eq,
    };
}

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
