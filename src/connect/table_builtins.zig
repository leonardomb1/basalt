//! The table-builtin registry, the table-valued counterpart of the scalar one
//! (`exec/builtins.zig`): one `TableBuiltin` per name read as `FROM name(args)`.
//! The parser, `check`, EXPLAIN and the run each `lookup` the read's entry, so a
//! builtin missing from the table is unknown everywhere at once. An entry owns its
//! name, its constant arguments (kinds and count), the columns `check` sees, its
//! EXPLAIN label, whether it runs on lanes, and how it opens a `driver.Source`.

const std = @import("std");
const ast = @import("../lang/ast.zig");
const types = @import("../lang/types.zig");
const Value = @import("../exec/value.zig").Value;
const driver = @import("driver.zig");
const gen = @import("gen.zig");

pub const ArgKind = enum { int };

pub const TableBuiltin = struct {
    name: []const u8,
    args: []const ArgKind,
    min_args: usize,
    arg_msg: []const u8,
    schema: types.Schema,
    label: []const u8,
    lanes: bool,
    open: *const fn (gpa: std.mem.Allocator, args: []const Value) anyerror!driver.Source,
};

pub const table = [_]TableBuiltin{
    .{
        .name = "range",
        .args = &.{ .int, .int },
        .min_args = 1,
        .arg_msg = "RANGE bounds must be integer literals or integer params",
        .schema = .{ .fields = &gen.RangeSource.fields },
        .label = "range",
        .lanes = false,
        .open = openRange,
    },
};

/// The entry named `name`, in any case.
pub fn lookup(name: []const u8) ?*const TableBuiltin {
    for (&table) |*b| if (std.ascii.eqlIgnoreCase(b.name, name)) return b;
    return null;
}

/// The entry a parsed `FROM name(args)` read names.
pub fn of(call: ast.BuiltinRead) *const TableBuiltin {
    return lookup(call.name).?;
}

/// An argument once its params are literals, or null when it is not one of `kind`:
/// an int is an integer literal or a negated one.
pub fn argValue(kind: ArgKind, e: *const ast.Expr) ?Value {
    return switch (kind) {
        .int => switch (e.*) {
            .int_lit => |v| .{ .int = v },
            .unary => |u| if (u.op == .neg and u.e.* == .int_lit) .{ .int = -u.e.int_lit } else null,
            else => null,
        },
    };
}

/// `range(hi)` is `range(0, hi)`.
fn openRange(gpa: std.mem.Allocator, args: []const Value) anyerror!driver.Source {
    const lo = if (args.len == 2) args[0].int else 0;
    const s = try gen.RangeSource.open(gpa, lo, args[args.len - 1].int);
    return s.source();
}

test "table builtins: unique names, each found through lookup in any case" {
    for (&table, 0..) |b, i| {
        for (table[0..i]) |prev| try std.testing.expect(!std.ascii.eqlIgnoreCase(prev.name, b.name));
        try std.testing.expectEqual(&table[i], lookup(b.name).?);
        try std.testing.expect(b.min_args <= b.args.len);
    }
    try std.testing.expectEqual(lookup("range").?, lookup("RANGE").?);
    try std.testing.expect(lookup("nope") == null);
}
