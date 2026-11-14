//! The table builtins' signatures, read as `FROM name(args)`: each name, the kinds
//! and count of its constant arguments, and the message a wrong one gets. The parser
//! needs no more than these to read a call; what an entry reads and how it opens
//! lives with the run (`runtime/connect/table_builtins.zig`), one entry per signature.

const std = @import("std");

pub const ArgKind = enum { int, size, path, text, bool };

pub const Signature = struct {
    name: []const u8,
    args: []const ArgKind,
    min_args: usize,
    arg_msg: []const u8,
};

pub const signatures = [_]Signature{
    .{ .name = "range", .args = &.{ .int, .int }, .min_args = 1, .arg_msg = "RANGE bounds must be integer literals, params or expressions of them" },
    .{ .name = "lines", .args = &.{.path}, .min_args = 1, .arg_msg = "LINES takes a file path: text, not a `dir/` folder" },
    .{ .name = "fixed", .args = &.{ .path, .size, .bool }, .min_args = 2, .arg_msg = "FIXED takes a file path (text, not a `dir/` folder), a positive integer width of at most 64 MiB and an optional TRUE/FALSE skip_newlines" },
    .{ .name = "split", .args = &.{ .path, .text }, .min_args = 2, .arg_msg = "SPLIT takes a file path (text, not a `dir/` folder) and a non-empty text terminator" },
    .{ .name = "chunks", .args = &.{ .path, .size }, .min_args = 2, .arg_msg = "CHUNKS takes a file path (text, not a `dir/` folder) and a positive integer size of at most 64 MiB" },
};

/// The signature named `name`, in any case.
pub fn lookup(name: []const u8) ?*const Signature {
    for (&signatures) |*b| if (std.ascii.eqlIgnoreCase(b.name, name)) return b;
    return null;
}

/// The signature named `name`, which must exist: a run entry is tied to its own at
/// compile time.
pub fn named(comptime name: []const u8) *const Signature {
    return comptime lookup(name) orelse @compileError("no table builtin signature `" ++ name ++ "`");
}

test "table builtin signatures: unique names, each found through lookup in any case" {
    for (&signatures, 0..) |b, i| {
        for (signatures[0..i]) |prev| try std.testing.expect(!std.ascii.eqlIgnoreCase(prev.name, b.name));
        try std.testing.expectEqual(&signatures[i], lookup(b.name).?);
        try std.testing.expect(b.min_args <= b.args.len);
    }
    try std.testing.expectEqual(lookup("range").?, lookup("RANGE").?);
    try std.testing.expect(lookup("nope") == null);
    try std.testing.expect(lookup("split_part") == null);
}
