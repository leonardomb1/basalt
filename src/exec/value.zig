//! A single scalar value. Used for literals, parameters, and per-cell access out
//! of columns. The columnar store (see `column.zig`) is the hot path; `Value` is
//! the boxed, one-at-a-time view used at cold edges and in tests.

const std = @import("std");

/// Exact decimal: `unscaled * 10^-scale`.
/// `align(8)` on the 128-bit field is deliberate: i128 otherwise forces 16-byte
/// alignment on `Decimal`, which in turn sizes the whole `Value` union at 48
/// bytes — every bool and every i64 in the engine pays for a decimal it does not
/// hold. Relaxing it drops `Decimal` to 24 bytes and `Value` to 32, and x86
/// handles the unaligned 128-bit access without complaint on a path that is not
/// hot.
pub const Decimal = struct { unscaled: i128 align(8), scale: u8 };

/// Live top-N bound shared between a TopN operator and a source that can skip
/// blocks of rows (currently the parquet reader).
///
/// The operator publishes the value at the K-th place; a source uses it to skip
/// regions whose statistics prove they hold nothing better. `full` matters: until
/// the heap holds K entries every region can still contribute. Comparisons must
/// be strict, since a region whose extreme *equals* the bound may hold ties.
pub const Threshold = struct {
    column: []const u8 = "",
    desc: bool = false,
    full: bool = false,
    value: Value = .null,
};

pub const Value = union(enum) {
    null,
    bool: bool,
    int: i64,
    float: f64,
    decimal: Decimal,
    string: []const u8,
    bytes: []const u8,
    date: i32,
    time: i64,
    timestamp: i64,

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
