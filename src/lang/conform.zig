//! Checking a schema (a file's, a union branch's) against a reference one under a
//! `Policy`: how columns are matched (by name, by position, by position with equal
//! names), which type differences pass, and in which order columns are walked, so
//! the first problem reported is the one each reader reports today. A pass gives,
//! per reference column, the candidate column that fills it and how; a failure gives
//! one `Problem` for the caller to phrase. Nullability never matters.
//!
//! The presets reproduce the checks they are named for: a Parquet folder's files
//! against its first (`parquet_folder`), a Delta table's files against the table
//! (`lake_fit`), an Avro file's columns against the first file's (`avro_route`), a
//! union's branches (`union_by_name`, `union_positional`) and an APPEND's columns
//! against a CSV file's header (`csv_append`).

const std = @import("std");
const types = @import("types.zig");
const Type = types.Type;
const Schema = types.Schema;

pub const Policy = struct {
    match: Match,
    types: Types,
    walk: Walk = .reference,

    pub const Match = enum { by_name, by_position, by_position_named };

    /// `exact`: kind, precision and scale equal. `same_scale`: kind equal and a
    /// decimal's scale equal, a precision difference relabelled. `widen`: equal, or
    /// `Type.canWiden` from the candidate to the reference, cast. `unify`:
    /// `Type.unify` finds a common type. `ignore`: any.
    pub const Types = enum { exact, same_scale, widen, unify, ignore };

    /// The order a `by_name` match walks columns in, which decides the problem
    /// reported when several columns have one.
    pub const Walk = enum { reference, candidate };

    pub const parquet_folder: Policy = .{ .match = .by_position_named, .types = .exact };
    pub const lake_fit: Policy = .{ .match = .by_name, .types = .same_scale };
    pub const avro_route: Policy = .{ .match = .by_name, .types = .widen, .walk = .candidate };
    pub const union_by_name: Policy = .{ .match = .by_name, .types = .unify };
    pub const union_positional: Policy = .{ .match = .by_position, .types = .unify };
    pub const csv_append: Policy = .{ .match = .by_position_named, .types = .ignore };
};

/// How a reference column is filled: from its candidate column as it is, relabelled
/// with the reference type, cast to it, or with NULLs (`absent`, no column of the name).
pub const How = enum { same, relabel, cast, absent };

/// `ty` is the reference column's type, or under `unify` the common type.
pub const Col = struct { from: ?usize, ty: Type, how: How };

pub const Problem = union(enum) {
    missing: []const u8,
    extra: []const u8,
    order: struct { got: []const u8, want: []const u8 },
    count: struct { got: usize, want: usize },
    type: struct { name: []const u8, got: Type, want: Type },
};

pub const Outcome = union(enum) {
    cols: []const Col,
    problem: Problem,
};

/// `got` checked against `want`. Under `by_position_named`, `asked` is the columns
/// a projection requested (null: all): a candidate column the reference lacks counts
/// as extra only when asked for, else the reference's column counts as missing.
pub fn conform(arena: std.mem.Allocator, want: Schema, got: Schema, policy: Policy, asked: ?[]const []const u8) std.mem.Allocator.Error!Outcome {
    const cols = try arena.alloc(Col, want.fields.len);
    switch (policy.match) {
        .by_position_named => {
            for (want.fields, cols, 0..) |w, *c, k| {
                if (k >= got.fields.len) return .{ .problem = .{ .missing = w.name } };
                const g = got.fields[k];
                if (!std.mem.eql(u8, w.name, g.name)) return .{ .problem = positionProblem(want, got, w.name, g.name, asked) };
                c.* = check(policy.types, w, g.ty, k) orelse return .{ .problem = typeProblem(w.name, g.ty, w.ty) };
            }
            if (got.fields.len > want.fields.len) return .{ .problem = .{ .extra = got.fields[want.fields.len].name } };
        },
        .by_position => {
            if (got.fields.len != want.fields.len) return .{ .problem = .{ .count = .{ .got = got.fields.len, .want = want.fields.len } } };
            for (want.fields, got.fields, cols, 0..) |w, g, *c, k|
                c.* = check(policy.types, w, g.ty, k) orelse return .{ .problem = typeProblem(w.name, g.ty, w.ty) };
        },
        .by_name => switch (policy.walk) {
            .reference => for (want.fields, cols) |w, *c| {
                const k = got.indexOf(w.name) orelse {
                    c.* = .{ .from = null, .ty = w.ty, .how = .absent };
                    continue;
                };
                const g = got.fields[k];
                c.* = check(policy.types, w, g.ty, k) orelse return .{ .problem = typeProblem(w.name, g.ty, w.ty) };
            },
            .candidate => {
                for (want.fields, cols) |w, *c| c.* = .{ .from = null, .ty = w.ty, .how = .absent };
                for (got.fields, 0..) |g, k| {
                    const to = want.indexOf(g.name) orelse continue;
                    const w = want.fields[to];
                    cols[to] = check(policy.types, w, g.ty, k) orelse return .{ .problem = typeProblem(g.name, g.ty, w.ty) };
                }
            },
        },
    }
    return .{ .cols = cols };
}

fn typeProblem(name: []const u8, got: Type, want: Type) Problem {
    return .{ .type = .{ .name = name, .got = got, .want = want } };
}

/// The problem when the candidate's `g` sits where the reference's `w` should
/// (parquet/read.zig `schemaMismatch`).
fn positionProblem(want: Schema, got: Schema, w: []const u8, g: []const u8, asked: ?[]const []const u8) Problem {
    if (got.indexOf(w) != null) return .{ .order = .{ .got = g, .want = w } };
    const is_asked = if (asked) |r| for (r) |n| {
        if (std.mem.eql(u8, n, g)) break true;
    } else false else true;
    if (is_asked and want.indexOf(g) == null) return .{ .extra = g };
    return .{ .missing = w };
}

fn check(rule: Policy.Types, w: Schema.Field, g: Type, k: usize) ?Col {
    switch (rule) {
        .ignore => return .{ .from = k, .ty = w.ty, .how = .same },
        .exact => {
            if (g.kind != w.ty.kind or g.precision != w.ty.precision or g.scale != w.ty.scale) return null;
            return .{ .from = k, .ty = w.ty, .how = .same };
        },
        .same_scale => {
            if (g.kind != w.ty.kind or (g.kind == .decimal and g.scale != w.ty.scale)) return null;
            const relabel = g.kind == .decimal and g.precision != w.ty.precision;
            return .{ .from = k, .ty = w.ty, .how = if (relabel) .relabel else .same };
        },
        .widen => {
            if (Type.eql(g, w.ty)) return .{ .from = k, .ty = w.ty, .how = .same };
            if (!Type.canWiden(g.kind, w.ty.kind)) return null;
            return .{ .from = k, .ty = w.ty, .how = .cast };
        },
        .unify => {
            const u = Type.unify(w.ty, g) orelse return null;
            return .{ .from = k, .ty = u, .how = if (Type.eql(g, u)) .same else .cast };
        },
    }
}

/// A problem as a Parquet folder names it, after "does not match its first file: ".
pub fn describe(arena: std.mem.Allocator, p: Problem) []const u8 {
    return switch (p) {
        .missing => |n| std.fmt.allocPrint(arena, "it has no column `{s}`", .{n}) catch "a column is missing",
        .extra => |n| std.fmt.allocPrint(arena, "it has a column `{s}` the first file lacks", .{n}) catch "an extra column",
        .order => |o| std.fmt.allocPrint(arena, "its columns are in another order (`{s}` where `{s}` is expected)", .{ o.got, o.want }) catch "columns in another order",
        .count => |c| std.fmt.allocPrint(arena, "it has {d} columns, not {d}", .{ c.got, c.want }) catch "another number of columns",
        .type => |t| std.fmt.allocPrint(arena, "column `{s}` is {s} there, not {s}", .{ t.name, t.got.name(arena) catch "decimal", t.want.name(arena) catch "decimal" }) catch "a column has another type",
    };
}

const testing = std.testing;

const sample_types = [_]Type{
    Type.init(.bool),
    Type.init(.int),
    Type.init(.int).asNullable(),
    Type.init(.float),
    Type.init(.string),
    .{ .kind = .string, .nullable = true, .scale = 2 },
    Type.init(.bytes),
    Type.init(.date),
    Type.init(.time),
    Type.init(.timestamp),
    Type.decimal(10, 2),
    Type.decimal(12, 2),
    Type.decimal(12, 2).asNullable(),
    Type.decimal(10, 4),
    Type.decimal(38, 0),
    Type.decimal(0, 0),
    Type.unknownNull(),
    Type.nullColumn(true),
};

fn single(arena: std.mem.Allocator, name: []const u8, t: Type) !Schema {
    const f = try arena.alloc(Schema.Field, 1);
    f[0] = .{ .name = name, .ty = t };
    return .{ .fields = f };
}

test "parquet_folder reproduces schemaMismatch's verdict and message on every pair" {
    const pq = @import("../format/parquet/read.zig");
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const i = Type.init(.int);
    const s = Type.init(.string);
    const F = Schema.Field;
    const schemas = [_]Schema{
        .{ .fields = &.{} },
        .{ .fields = &[_]F{.{ .name = "id", .ty = i }} },
        .{ .fields = &[_]F{.{ .name = "id", .ty = i.asNullable() }} },
        .{ .fields = &[_]F{.{ .name = "id", .ty = s }} },
        .{ .fields = &[_]F{.{ .name = "x", .ty = i }} },
        .{ .fields = &[_]F{ .{ .name = "id", .ty = i }, .{ .name = "x", .ty = i } } },
        .{ .fields = &[_]F{ .{ .name = "x", .ty = i }, .{ .name = "id", .ty = i } } },
        .{ .fields = &[_]F{ .{ .name = "id", .ty = i }, .{ .name = "y", .ty = i } } },
        .{ .fields = &[_]F{ .{ .name = "id", .ty = i }, .{ .name = "x", .ty = s } } },
        .{ .fields = &[_]F{ .{ .name = "id", .ty = i }, .{ .name = "x", .ty = i }, .{ .name = "y", .ty = i } } },
        .{ .fields = &[_]F{ .{ .name = "y", .ty = i }, .{ .name = "x", .ty = i } } },
        .{ .fields = &[_]F{.{ .name = "d", .ty = Type.decimal(10, 2) }} },
        .{ .fields = &[_]F{.{ .name = "d", .ty = Type.decimal(12, 2) }} },
        .{ .fields = &[_]F{.{ .name = "d", .ty = Type.decimal(10, 3) }} },
        .{ .fields = &[_]F{.{ .name = "d", .ty = Type.decimal(10, 2).asNullable() }} },
        .{ .fields = &[_]F{.{ .name = "d", .ty = .{ .kind = .string, .scale = 2 } }} },
        .{ .fields = &[_]F{.{ .name = "d", .ty = s }} },
        .{ .fields = &[_]F{.{ .name = "d", .ty = Type.nullColumn(true) }} },
    };
    const askeds = [_]?[]const []const u8{ null, &.{}, &.{"id"}, &.{"x"}, &.{"y"}, &.{ "id", "x" }, &.{ "x", "y" }, &.{"d"} };
    for (schemas) |want| for (schemas) |got| for (askeds) |asked| {
        const today = pq.schemaMismatch(a, want, got, asked);
        const home = try conform(a, want, got, Policy.parquet_folder, asked);
        switch (home) {
            .cols => |cols| {
                try testing.expect(today == null);
                try testing.expectEqual(want.fields.len, cols.len);
                for (cols, 0..) |c, k| {
                    try testing.expectEqual(@as(?usize, k), c.from);
                    try testing.expectEqual(How.same, c.how);
                }
            },
            .problem => |p| try testing.expectEqualStrings(today.?, describe(a, p)),
        }
    };
}

test "lake_fit reproduces the table fit (parquet/fit.zig:126-135, transcribed): kind and scale must agree, precision is relabelled" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    for (sample_types) |w| for (sample_types) |g| {
        const today = g.kind == w.kind and (g.kind != .decimal or g.scale == w.scale);
        const home = try conform(a, try single(a, "c", w), try single(a, "c", g), Policy.lake_fit, null);
        try testing.expectEqual(today, home == .cols);
        if (home == .cols) {
            const c = home.cols[0];
            try testing.expectEqual(@as(?usize, 0), c.from);
            try expectSame(w, c.ty);
            try testing.expectEqual(w.kind == .decimal and w.precision != g.precision, c.how == .relabel);
        } else {
            try testing.expectEqualStrings("c", home.problem.type.name);
        }
    };
    const table = Schema{ .fields = &.{ .{ .name = "a", .ty = Type.decimal(10, 2) }, .{ .name = "b", .ty = Type.init(.int) }, .{ .name = "c", .ty = Type.init(.string) } } };
    const file = Schema{ .fields = &.{ .{ .name = "z", .ty = Type.init(.bool) }, .{ .name = "c", .ty = Type.init(.string) }, .{ .name = "a", .ty = Type.decimal(12, 2) } } };
    const out = (try conform(a, table, file, Policy.lake_fit, null)).cols;
    try testing.expectEqual(@as(?usize, 2), out[0].from);
    try testing.expectEqual(How.relabel, out[0].how);
    try testing.expectEqual(@as(u8, 10), out[0].ty.precision);
    try testing.expectEqual(@as(?usize, null), out[1].from);
    try testing.expectEqual(How.absent, out[1].how);
    try testing.expectEqual(@as(?usize, 1), out[2].from);
    try testing.expectEqual(How.same, out[2].how);
    const narrower = Schema{ .fields = &.{.{ .name = "a", .ty = Type.decimal(8, 2) }} };
    try testing.expectEqual(How.relabel, (try conform(a, table, narrower, Policy.lake_fit, null)).cols[0].how);
    const two_bad = Schema{ .fields = &.{ .{ .name = "c", .ty = Type.init(.int) }, .{ .name = "b", .ty = Type.init(.string) } } };
    try testing.expectEqualStrings("b", (try conform(a, table, two_bad, Policy.lake_fit, null)).problem.type.name);
}

fn expectSame(want: Type, got: Type) !void {
    try testing.expectEqual(want.kind, got.kind);
    try testing.expectEqual(want.nullable, got.nullable);
    try testing.expectEqual(want.unknown, got.unknown);
    try testing.expectEqual(want.precision, got.precision);
    try testing.expectEqual(want.scale, got.scale);
}

test "avro_route reproduces Avro's route (avro/read.zig:174-190, transcribed): equal or widening, cast unless equal, walked in the file's order" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    for (sample_types) |w| for (sample_types) |g| {
        const today_ok = Type.eql(g, w) or Type.canWiden(g.kind, w.kind);
        const today_cast = !Type.eql(g, w);
        const home = try conform(a, try single(a, "c", w), try single(a, "c", g), Policy.avro_route, null);
        try testing.expectEqual(today_ok, home == .cols);
        if (home == .cols) try testing.expectEqual(today_cast, home.cols[0].how == .cast);
    };
    const d = (try conform(a, try single(a, "d", Type.decimal(12, 4)), try single(a, "d", Type.decimal(10, 2)), Policy.avro_route, null)).cols[0];
    try testing.expectEqual(How.cast, d.how);
    const first = Schema{ .fields = &.{ .{ .name = "a", .ty = Type.init(.int) }, .{ .name = "b", .ty = Type.init(.int) }, .{ .name = "c", .ty = Type.init(.float) } } };
    const file = Schema{ .fields = &.{ .{ .name = "x", .ty = Type.init(.bool) }, .{ .name = "b", .ty = Type.init(.string) }, .{ .name = "a", .ty = Type.init(.string) } } };
    const p = (try conform(a, first, file, Policy.avro_route, null)).problem.type;
    try testing.expectEqualStrings("b", p.name);
    try testing.expectEqual(types.TypeKind.string, p.got.kind);
    try testing.expectEqual(types.TypeKind.int, p.want.kind);
    const file2 = Schema{ .fields = &.{ .{ .name = "c", .ty = Type.init(.int) }, .{ .name = "x", .ty = Type.init(.bool) } } };
    const r = (try conform(a, first, file2, Policy.avro_route, null)).cols;
    try testing.expectEqual(How.absent, r[0].how);
    try testing.expectEqual(How.absent, r[1].how);
    try testing.expectEqual(@as(?usize, 0), r[2].from);
    try testing.expectEqual(How.cast, r[2].how);
}

test "union_by_name reproduces unionCanon (plan/union.zig:143-154, transcribed): Type.unify per shared name, absent names left" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    for (sample_types) |w| for (sample_types) |g| {
        const today = Type.unify(w, g);
        const home = try conform(a, try single(a, "c", w), try single(a, "c", g), Policy.union_by_name, null);
        try testing.expectEqual(today != null, home == .cols);
        if (today) |t| try expectSame(t, home.cols[0].ty) else {
            try expectSame(g, home.problem.type.got);
            try expectSame(w, home.problem.type.want);
        }
    };
    const canon = Schema{ .fields = &.{ .{ .name = "a", .ty = Type.init(.int) }, .{ .name = "b", .ty = Type.init(.int) } } };
    const branch = Schema{ .fields = &.{ .{ .name = "z", .ty = Type.init(.bool) }, .{ .name = "a", .ty = Type.init(.float) } } };
    const out = (try conform(a, canon, branch, Policy.union_by_name, null)).cols;
    try testing.expectEqual(types.TypeKind.float, out[0].ty.kind);
    try testing.expectEqual(How.absent, out[1].how);
    try expectSame(Type.init(.int), out[1].ty);
}

test "union_positional reproduces positionalCanon (plan/union.zig:160-180, transcribed): equal counts first, then Type.unify by position" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const two = Schema{ .fields = &.{ .{ .name = "a", .ty = Type.init(.int) }, .{ .name = "b", .ty = Type.init(.int) } } };
    const three_bad = Schema{ .fields = &.{ .{ .name = "x", .ty = Type.init(.string) }, .{ .name = "y", .ty = Type.init(.int) }, .{ .name = "z", .ty = Type.init(.int) } } };
    const c = (try conform(a, two, three_bad, Policy.union_positional, null)).problem.count;
    try testing.expectEqual(@as(usize, 3), c.got);
    try testing.expectEqual(@as(usize, 2), c.want);
    const renamed = Schema{ .fields = &.{ .{ .name = "x", .ty = Type.decimal(10, 2) }, .{ .name = "y", .ty = Type.unknownNull() } } };
    const out = (try conform(a, two, renamed, Policy.union_positional, null)).cols;
    try testing.expectEqual(types.TypeKind.decimal, out[0].ty.kind);
    try testing.expectEqual(@as(u8, 21), out[0].ty.precision);
    try testing.expect(out[1].ty.nullable);
    try testing.expectEqual(types.TypeKind.int, out[1].ty.kind);
    const bad = Schema{ .fields = &.{ .{ .name = "a", .ty = Type.init(.int) }, .{ .name = "b", .ty = Type.init(.string) } } };
    try testing.expectEqualStrings("b", (try conform(a, two, bad, Policy.union_positional, null)).problem.type.name);
}

test "csv_append reproduces checkAppendHeader's test (csv.zig:1036-1038, transcribed): equal counts and names in order, types ignored" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const names = [_][]const []const u8{ &.{}, &.{"a"}, &.{"b"}, &.{ "a", "b" }, &.{ "b", "a" }, &.{ "a", "b", "c" }, &.{ "a", "a" }, &.{ "a", "c" } };
    for (names) |hn| for (names) |sn| {
        const have = try a.alloc(Schema.Field, hn.len);
        for (have, hn, 0..) |*f, n, k| f.* = .{ .name = n, .ty = sample_types[k % sample_types.len] };
        const writes = try a.alloc(Schema.Field, sn.len);
        for (writes, sn, 0..) |*f, n, k| f.* = .{ .name = n, .ty = sample_types[(k + 5) % sample_types.len] };
        const today = hn.len == sn.len and for (hn, sn) |h, s| {
            if (!std.mem.eql(u8, h, s)) break false;
        } else true;
        const home = try conform(a, .{ .fields = writes }, .{ .fields = have }, Policy.csv_append, null);
        try testing.expectEqual(today, home == .cols);
    };
}

test "conform ignores nullability under every policy" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const policies = [_]Policy{ Policy.parquet_folder, Policy.lake_fit, Policy.avro_route, Policy.union_by_name, Policy.union_positional, Policy.csv_append };
    for (policies) |p| for (sample_types) |t| {
        if (t.unknown) continue;
        const home = try conform(a, try single(a, "c", t), try single(a, "c", t.withNull(!t.nullable)), p, null);
        try testing.expect(home == .cols);
    };
}

test "describe phrases every problem" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try testing.expectEqualStrings("it has 3 columns, not 2", describe(a, .{ .count = .{ .got = 3, .want = 2 } }));
    try testing.expectEqualStrings("column `d` is decimal(12,2) there, not decimal(10,2)", describe(a, .{ .type = .{ .name = "d", .got = Type.decimal(12, 2), .want = Type.decimal(10, 2) } }));
}
