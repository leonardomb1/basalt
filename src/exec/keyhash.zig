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
