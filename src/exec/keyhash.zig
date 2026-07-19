//! Value-keyed hashing for group-by / distinct / join. The old path serialized each
//! row's key columns into a byte string (allocating per row, formatting ints to
//! decimal) and hashed that. Here we hash and compare the key `Value`s directly: a
//! stored key is `[]const Value` (the key columns, deep-copied into plan state); the
//! caller builds a transient probe key (aliasing batch memory) in scratch and looks
//! it up with the standard `getOrPut`. Backed by `std.HashMap`, whose flat
//! metadata-byte probing is already a Swiss/F14-style open-addressing table.
//!
//! NOTE: do not use `getOrPutAdapted` here — in Zig 0.15.2 the adapted-probe path is
//! ~30x slower than `getOrPut` with a prebuilt key, so callers materialize a small
//! key slice per row in the scratch arena instead.
//!
//! Invariant: within one key column the value type is fixed, so two values that
//! compare equal here always hash equal (the hash includes the type tag).

const std = @import("std");
const valuemod = @import("value.zig");
const eval = @import("eval.zig");

const Value = valuemod.Value;

/// Fold one value (type tag + payload bytes) into a running hash.
pub fn hashValue(h: *std.hash.Wyhash, v: Value) void {
    const tag: u8 = @intFromEnum(std.meta.activeTag(v));
    h.update(&[_]u8{tag});
    switch (v) {
        .null => {},
        .bool => |x| h.update(&[_]u8{@intFromBool(x)}),
        .int => |x| h.update(std.mem.asBytes(&x)),
        .float => |x| h.update(std.mem.asBytes(&x)),
        .decimal => |d| {
            h.update(std.mem.asBytes(&d.unscaled));
            h.update(std.mem.asBytes(&d.scale));
        },
        .string, .bytes => |s| h.update(s),
        .date => |x| h.update(std.mem.asBytes(&x)),
        .time => |x| h.update(std.mem.asBytes(&x)),
        .timestamp => |x| h.update(std.mem.asBytes(&x)),
    }
}

pub fn hashOne(v: Value) u64 {
    var h = std.hash.Wyhash.init(0);
    hashValue(&h, v);
    return h.final();
}

/// Grouping equality: two nulls are equal (they group together); otherwise compare
/// by value (string/bytes by bytes, the rest via `compareValues`).
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

/// Context for a stored composite key (`[]const Value`). Zero-sized, so a managed
/// `std.HashMap` can default-construct it.
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

/// Context for a single stored `Value` key (join). Null keys never match (SQL),
/// so callers skip them before insert/probe.
pub const SingleKeyCtx = struct {
    pub fn hash(_: SingleKeyCtx, key: Value) u64 {
        return hashOne(key);
    }
    pub fn eql(_: SingleKeyCtx, a: Value, b: Value) bool {
        return valueEq(a, b);
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

test "equal values hash equal; payload and type tag discriminate" {
    try testing.expectEqual(hashOne(.{ .int = 42 }), hashOne(.{ .int = 42 }));
    try testing.expectEqual(hashOne(.{ .string = "abc" }), hashOne(.{ .string = "abc" }));
    try testing.expectEqual(hashOne(.null), hashOne(.null));
    try testing.expect(hashOne(.{ .int = 42 }) != hashOne(.{ .int = 43 }));
    // Same payload bytes under a different tag must not collide by construction:
    // int 1 vs timestamp 1 (identical i64), null vs int 0.
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

test "valueEq: nulls group together, null never equals a value, mixed types unequal" {
    try testing.expect(valueEq(.null, .null));
    try testing.expect(!valueEq(.null, .{ .int = 0 }));
    try testing.expect(!valueEq(.{ .string = "" }, .null));
    try testing.expect(valueEq(.{ .string = "a" }, .{ .string = "a" }));
    try testing.expect(!valueEq(.{ .string = "a" }, .{ .string = "b" }));
    try testing.expect(valueEq(.{ .float = 2.5 }, .{ .float = 2.5 }));
    try testing.expect(valueEq(.{ .bool = true }, .{ .bool = true }));
    // Incomparable types compare unequal (no error): compareValues yields null.
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
    try testing.expect(!ctx.eql(&k1, &k3)); // column order matters
    try testing.expect(ctx.hash(&k1) != ctx.hash(&k3));
    try testing.expect(!ctx.eql(k1[0..1], &k2)); // prefix != full key

    // Null slots participate in grouping: (null) == (null), (null) != (0).
    const n1 = [_]Value{.null};
    const n2 = [_]Value{.null};
    const z0 = [_]Value{.{ .int = 0 }};
    try testing.expect(ctx.eql(&n1, &n2));
    try testing.expectEqual(ctx.hash(&n1), ctx.hash(&n2));
    try testing.expect(!ctx.eql(&n1, &z0));
}

test "SingleKeyCtx delegates to hashOne/valueEq" {
    const ctx = SingleKeyCtx{};
    try testing.expectEqual(hashOne(.{ .int = 9 }), ctx.hash(.{ .int = 9 }));
    try testing.expect(ctx.eql(.{ .string = "k" }, .{ .string = "k" }));
    try testing.expect(!ctx.eql(.{ .string = "k" }, .null));
}
