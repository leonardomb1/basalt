//! A single scalar value, used for literals, parameters, and per-cell access out
//! of columns. The columnar store (see `column.zig`) is the hot path; `Value` is
//! the boxed, one-at-a-time view used at cold edges and in tests. `Decimal` and
//! its logic live in `decimal.zig`.
//!
//! A nested value (a list, map or struct) is a `NestedRef` into its column, not a
//! copy, so a `Value` stays as small as its scalars need.
//!
//! `Threshold` is the live top-N bound shared between a TopN operator and a source
//! that can skip blocks (currently parquet). The operator publishes the K-th value;
//! a source skips regions whose statistics prove they hold nothing better. Until
//! `full` (the heap holds K entries) every region can contribute, and comparisons
//! must be strict, since a region whose extreme equals the bound may hold ties.

pub const Decimal = @import("decimal.zig").Decimal;
const std = @import("std");

pub const Threshold = struct {
    column: []const u8 = "",
    desc: bool = false,
    nulls_first: bool = false,
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
    nested: NestedRef,

    /// `self` with its text, bytes or nested storage copied into `a`, so it outlives
    /// the batch it was read from.
    pub fn own(self: Value, a: std.mem.Allocator) error{OutOfMemory}!Value {
        return switch (self) {
            .string => |s| .{ .string = try a.dupe(u8, s) },
            .bytes => |s| .{ .bytes = try a.dupe(u8, s) },
            .nested => |r| .{ .nested = try @import("nested.zig").own(a, r) },
            else => self,
        };
    }

    pub fn isNull(self: Value) bool {
        return self == .null;
    }
};

/// Row `row` of a nested column's storage, borrowed: valid while the batch that
/// holds `node` is. Whatever keeps a value past its batch copies it
/// (`Builder.append`, `Value.own`).
pub const NestedRef = struct { node: *const @import("column.zig").Nested, row: usize };
