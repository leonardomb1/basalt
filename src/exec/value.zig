//! A single scalar value. Used for literals, parameters, and per-cell access out
//! of columns. The columnar store (see `column.zig`) is the hot path; `Value` is
//! the boxed, one-at-a-time view used at cold edges and in tests.

const std = @import("std");

/// Exact decimal: `unscaled * 10^-scale`.
pub const Decimal = struct { unscaled: i128, scale: u8 };

pub const Value = union(enum) {
    null,
    bool: bool,
    int: i64,
    float: f64,
    decimal: Decimal,
    string: []const u8,
    bytes: []const u8,
    date: i32, // days since 1970-01-01
    time: i64, // microseconds since midnight
    timestamp: i64, // microseconds since epoch, UTC

    pub fn isNull(self: Value) bool {
        return self == .null;
    }
};

test "value tag and null" {
    const v: Value = .{ .int = 7 };
    try std.testing.expect(!v.isNull());
    try std.testing.expectEqual(@as(i64, 7), v.int);

    const n: Value = .null;
    try std.testing.expect(n.isNull());
}
