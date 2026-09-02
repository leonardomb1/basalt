//! The scalar-builtin registry: one `Builtin` per function name, each carrying
//! its plan-time typing, its row-wise evaluator and (when one exists) its
//! vectorized kernel. `eval.zig`'s `typeOfCall`, `evalCall` and `callVec` are
//! each a `lookup` on the call's name, so a builtin missing from the table is
//! unknown everywhere at once rather than in one of three if-chains.
//!
//! The table itself lives at the bottom of `eval.zig`, next to the handlers it
//! points at: those handlers lean on eval-private helpers (`evalRow`, `strArg`,
//! `mkCol`, …), and putting the table here would make `builtins.zig` import
//! `eval.zig` while `eval.zig` imports `builtins.zig` for dispatch. Keeping the
//! import one-way costs only this re-export. This file is the public face of
//! the registry (what `runtime/pushdown.zig` checks its own table against) and
//! home of the tests that pin its shape.

const std = @import("std");
const eval = @import("eval.zig");

/// One entry of the registry; see `eval.Builtin`.
pub const Builtin = eval.Builtin;

/// Every scalar builtin, one entry per name.
pub const table = &eval.builtins;

/// The builtin called `name`, or null for unknown names and aggregates.
pub const lookup = eval.lookupBuiltin;

test "builtins: every entry has a unique name and resolves through lookup" {
    for (table, 0..) |b, i| {
        for (table[0..i]) |prev| try std.testing.expect(!std.mem.eql(u8, prev.name, b.name));
        const found = lookup(b.name) orelse return error.TestUnexpectedResult;
        try std.testing.expectEqualStrings(b.name, found.name);
    }
}

test "builtins: aggregates and unknown names are not scalar builtins" {
    for ([_][]const u8{ "count", "sum", "avg", "min", "max", "no_such_fn", "" }) |n|
        try std.testing.expect(lookup(n) == null);
}

test "builtins: exactly these lack a vectorized kernel" {
    const rowwise_only = [_][]const u8{
        "regexp_replace", "date_trunc", "extract",   "bit_count",  "to_hex", "from_hex",
        "abs",            "floor",      "ceil",      "round",      "mod",    "power",
        "sqrt",           "sign",       "nullif",    "greatest",   "least",  "lpad",
        "rpad",           "left",       "right",     "split_part", "strpos", "repeat",
        "reverse",        "date_add",   "date_diff", "make_date",  "epoch",  "to_timestamp",
        "strftime",
    };
    for (rowwise_only) |n| try std.testing.expect(lookup(n) != null);
    for (table) |b| {
        var listed = false;
        for (rowwise_only) |n| listed = listed or std.mem.eql(u8, n, b.name);
        try std.testing.expectEqual(listed, b.vec_fn == null);
    }
}
