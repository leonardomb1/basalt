//! The aggregate registry: one `Spec` per aggregate function, carrying the names
//! it answers to, what it accepts and how its result is typed. The parser
//! recognizes an aggregate through `lookup`, `runtime/analyze.zig` checks and
//! types it through `spec`, and Tab completion is held to the same names by a
//! test in `cli/cli.zig` — so adding an aggregate is one `ast.AggFunc` value and
//! one row here, after which the compiler points at every `switch` in
//! `exec/op.zig` that must learn to fold it.
//!
//! Every name here is reserved: aggregates are recognized while parsing, before
//! `CREATE FUNCTION` macros expand, so a user function cannot take one.

const std = @import("std");
const ast = @import("ast.zig");
const types = @import("types.zig");

pub const AggFunc = ast.AggFunc;

/// What an aggregate's argument may be, checked at plan time. Text always passes:
/// the parallel CSV lanes carry raw text and coerce it per row, so a numeric CSV
/// column may still be typed STRING here. So does a type `check` could not see
/// (a SQL table it has not connected to).
pub const Arg = enum {
    /// `COUNT(*)` or `COUNT(x)` of anything.
    star_or_any,
    /// Any one value: MIN and MAX compare whatever they are given.
    any,
    /// INT, FLOAT or DECIMAL.
    numeric,
    /// INT: the bitwise aggregates.
    int,
    /// BOOL: a condition, as `count_if(amount > 0)` or `bool_and(paid)`.
    bool,

    pub fn accepts(self: Arg, t: types.Type) bool {
        if (t.unknown or t.kind == .string) return true;
        return switch (self) {
            .star_or_any, .any => true,
            .numeric => t.kind.isNumeric(),
            .int => t.kind == .int,
            .bool => t.kind == .bool,
        };
    }

    /// The argument's kind in a plan-time error: "`sum` needs a {s} argument".
    pub fn word(self: Arg) []const u8 {
        return switch (self) {
            .star_or_any, .any => "",
            .numeric => "numeric",
            .int => "INT",
            .bool => "BOOL",
        };
    }
};

/// How an aggregate's result type follows from its argument's.
pub const Result = enum {
    /// A row count: INT, never null (COUNT, `count_if`).
    count,
    /// SUM: FLOAT over floats, DECIMAL over decimals (same scale), else INT; nullable.
    sum,
    /// Always a nullable FLOAT (AVG, MEDIAN, the variances).
    float,
    /// The argument's own type, nullable (MIN, MAX).
    same,
    /// A nullable BOOL (`bool_and`, `bool_or`).
    bool,
    /// A nullable INT (the bitwise aggregates).
    int,
};

pub const Spec = struct {
    func: AggFunc,
    /// The first name is canonical; the rest are aliases.
    names: []const []const u8,
    arg: Arg,
    result: Result,
    /// How many arguments the call may pass; the parser refuses more.
    max_args: u8 = 1,
};

/// In `AggFunc` order — `spec` indexes it by the enum's value.
pub const specs = [_]Spec{
    .{ .func = .count, .names = &.{"count"}, .arg = .star_or_any, .result = .count },
    .{ .func = .sum, .names = &.{"sum"}, .arg = .numeric, .result = .sum },
    .{ .func = .avg, .names = &.{"avg"}, .arg = .numeric, .result = .float },
    .{ .func = .min, .names = &.{"min"}, .arg = .any, .result = .same },
    .{ .func = .max, .names = &.{"max"}, .arg = .any, .result = .same },
    .{ .func = .median, .names = &.{"median"}, .arg = .numeric, .result = .float },
    .{ .func = .count_if, .names = &.{"count_if"}, .arg = .bool, .result = .count },
    .{ .func = .bool_and, .names = &.{"bool_and"}, .arg = .bool, .result = .bool },
    .{ .func = .bool_or, .names = &.{"bool_or"}, .arg = .bool, .result = .bool },
    .{ .func = .bit_and, .names = &.{"bit_and"}, .arg = .int, .result = .int },
    .{ .func = .bit_or, .names = &.{"bit_or"}, .arg = .int, .result = .int },
    .{ .func = .bit_xor, .names = &.{"bit_xor"}, .arg = .int, .result = .int },
    // The unqualified names are the sample statistics, as in Postgres, DuckDB,
    // Trino and SQL Server. MySQL and StarRocks read them as the population ones.
    .{ .func = .var_samp, .names = &.{ "var_samp", "variance" }, .arg = .numeric, .result = .float },
    .{ .func = .var_pop, .names = &.{"var_pop"}, .arg = .numeric, .result = .float },
    .{ .func = .stddev_samp, .names = &.{ "stddev_samp", "stddev" }, .arg = .numeric, .result = .float },
    .{ .func = .stddev_pop, .names = &.{"stddev_pop"}, .arg = .numeric, .result = .float },
};

comptime {
    const fields = @typeInfo(AggFunc).@"enum".fields;
    if (specs.len != fields.len) @compileError("aggregates.specs needs exactly one row per ast.AggFunc");
    for (specs, 0..) |s, i| {
        if (@intFromEnum(s.func) != i) @compileError("aggregates.specs is out of ast.AggFunc order at `" ++ @tagName(s.func) ++ "`");
        if (s.names.len == 0) @compileError("aggregate `" ++ @tagName(s.func) ++ "` has no name");
    }
}

pub fn spec(func: AggFunc) Spec {
    return specs[@intFromEnum(func)];
}

/// The aggregate called `name` (any case), or null.
pub fn lookup(name: []const u8) ?AggFunc {
    for (specs) |s| for (s.names) |n| {
        if (std.ascii.eqlIgnoreCase(n, name)) return s.func;
    };
    return null;
}

test "aggregates: every name is unique and resolves to its own aggregate" {
    for (specs, 0..) |s, i| for (s.names) |n| {
        try std.testing.expectEqual(s.func, lookup(n).?);
        for (specs[0..i]) |prev| for (prev.names) |p| try std.testing.expect(!std.ascii.eqlIgnoreCase(p, n));
    };
}

test "aggregates: a numeric argument takes numbers, text and the unseen, nothing else" {
    const num = Arg.numeric;
    for ([_]types.TypeKind{ .int, .float, .decimal, .string }) |k| try std.testing.expect(num.accepts(types.Type.init(k)));
    for ([_]types.TypeKind{ .bool, .bytes, .date, .time, .timestamp }) |k| try std.testing.expect(!num.accepts(types.Type.init(k)));
    try std.testing.expect(num.accepts(types.Type.unknownNull()));
    try std.testing.expect(Arg.any.accepts(types.Type.init(.date)));
}

test "aggregates: lookup ignores case and rejects unknown names" {
    try std.testing.expectEqual(AggFunc.median, lookup("MEDIAN").?);
    try std.testing.expect(lookup("no_such_agg") == null);
    try std.testing.expect(lookup("") == null);
}
