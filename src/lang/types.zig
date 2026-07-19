//! The engine's canonical type system: the type lattice every column and value
//! speaks, the nullability flag, and the implicit-widening / unification rules.
//!
//! Coercion policy (see plan): implicit *widening only* (`int -> decimal`,
//! `int -> float`); everything else needs an explicit `cast`. Nulls follow SQL
//! three-valued logic, tracked here as a per-type `nullable` flag.

const std = @import("std");

pub const TypeKind = enum {
    bool,
    int, // i64
    float, // f64
    decimal, // (precision, scale), exact
    string, // utf-8
    bytes,
    date, // days since 1970-01-01
    time, // microseconds since midnight
    timestamp, // microseconds since epoch, UTC
    array, // element type in `elem`
    @"struct", // named fields in `fields`

    pub fn isNumeric(self: TypeKind) bool {
        return switch (self) {
            .int, .float, .decimal => true,
            else => false,
        };
    }
};

pub const Type = struct {
    kind: TypeKind,
    nullable: bool = false,
    // `unknown` marks the type of a bare `null` literal: it unifies with any
    // concrete type (yielding that type, nullable). Never appears on a column.
    unknown: bool = false,
    // decimal parameters (kind == .decimal)
    precision: u8 = 0,
    scale: u8 = 0,
    // array element (kind == .array)
    elem: ?*const Type = null,
    // struct fields (kind == .@"struct")
    fields: ?[]const Field = null,

    pub const Field = struct { name: []const u8, ty: Type };

    pub fn init(kind: TypeKind) Type {
        return .{ .kind = kind };
    }

    /// The type of a bare `null` literal — unifies with anything.
    pub fn unknownNull() Type {
        return .{ .kind = .bool, .nullable = true, .unknown = true };
    }

    pub fn decimal(precision: u8, scale: u8) Type {
        return .{ .kind = .decimal, .precision = precision, .scale = scale };
    }

    pub fn asNullable(self: Type) Type {
        var t = self;
        t.nullable = true;
        return t;
    }

    pub fn withNull(self: Type, n: bool) Type {
        var t = self;
        t.nullable = n;
        return t;
    }

    pub fn eql(a: Type, b: Type) bool {
        if (a.kind != b.kind) return false;
        if (a.kind == .decimal and (a.precision != b.precision or a.scale != b.scale)) return false;
        // array/struct deep comparison is deferred past M0.
        return true;
    }

    /// Can a value of kind `from` be implicitly widened to kind `to`?
    /// Only `int -> decimal` and `int -> float` (plus identity).
    pub fn canWiden(from: TypeKind, to: TypeKind) bool {
        if (from == to) return true;
        return from == .int and (to == .decimal or to == .float);
    }

    /// The common type of two `if`/`match` arms, or null if they don't unify.
    /// Reconciles nullability and widens one side toward the other.
    pub fn unify(a: Type, b: Type) ?Type {
        if (a.unknown) return b.asNullable();
        if (b.unknown) return a.asNullable();
        const nn = a.nullable or b.nullable;
        if (a.kind == b.kind) {
            var t = a;
            t.nullable = nn;
            if (a.kind == .decimal) {
                t.precision = @max(a.precision, b.precision);
                t.scale = @max(a.scale, b.scale);
            }
            return t;
        }
        if (canWiden(a.kind, b.kind)) {
            var t = b;
            t.nullable = nn;
            return t;
        }
        if (canWiden(b.kind, a.kind)) {
            var t = a;
            t.nullable = nn;
            return t;
        }
        return null;
    }
};

/// An ordered set of named, typed columns — the schema flowing between operators.
pub const Schema = struct {
    fields: []const Field,

    pub const Field = struct { name: []const u8, ty: Type };

    pub fn indexOf(self: Schema, name: []const u8) ?usize {
        for (self.fields, 0..) |f, i| {
            if (std.mem.eql(u8, f.name, name)) return i;
        }
        return null;
    }
};

test "canWiden follows int-only widening" {
    try std.testing.expect(Type.canWiden(.int, .float));
    try std.testing.expect(Type.canWiden(.int, .decimal));
    try std.testing.expect(Type.canWiden(.int, .int));
    try std.testing.expect(!Type.canWiden(.decimal, .float));
    try std.testing.expect(!Type.canWiden(.float, .int));
    try std.testing.expect(!Type.canWiden(.string, .bytes));
}

test "unify widens and reconciles nullability" {
    const i = Type.init(.int);
    const f = Type.init(.float);
    const u = Type.unify(i, f).?;
    try std.testing.expectEqual(TypeKind.float, u.kind);

    const ni = Type.init(.int).asNullable();
    const unified = Type.unify(ni, Type.init(.int)).?;
    try std.testing.expect(unified.nullable);

    try std.testing.expect(Type.unify(Type.init(.string), i) == null);
}

test "unify: unknown-null unifies with any type, yielding it nullable" {
    const s = Type.init(.string);
    const ua = Type.unify(Type.unknownNull(), s).?;
    try std.testing.expectEqual(TypeKind.string, ua.kind);
    try std.testing.expect(ua.nullable);
    try std.testing.expect(!ua.unknown); // the result is a concrete type
    const ub = Type.unify(s, Type.unknownNull()).?;
    try std.testing.expectEqual(TypeKind.string, ub.kind);
    try std.testing.expect(ub.nullable);
}

test "unify decimals takes max precision/scale; int widens into decimal" {
    const u = Type.unify(Type.decimal(18, 2), Type.decimal(10, 4)).?;
    try std.testing.expectEqual(TypeKind.decimal, u.kind);
    try std.testing.expectEqual(@as(u8, 18), u.precision);
    try std.testing.expectEqual(@as(u8, 4), u.scale);

    const w = Type.unify(Type.init(.int), Type.decimal(12, 3)).?;
    try std.testing.expectEqual(TypeKind.decimal, w.kind);
    try std.testing.expectEqual(@as(u8, 12), w.precision);
    try std.testing.expectEqual(@as(u8, 3), w.scale);
}

test "eql compares decimal parameters but ignores nullability" {
    try std.testing.expect(Type.eql(Type.decimal(10, 2), Type.decimal(10, 2)));
    try std.testing.expect(!Type.eql(Type.decimal(10, 2), Type.decimal(10, 3)));
    try std.testing.expect(!Type.eql(Type.decimal(11, 2), Type.decimal(10, 2)));
    try std.testing.expect(!Type.eql(Type.init(.int), Type.init(.float)));
    try std.testing.expect(Type.eql(Type.init(.int).asNullable(), Type.init(.int)));
}

test "schema lookup" {
    const s = Schema{ .fields = &.{
        .{ .name = "id", .ty = Type.init(.int) },
        .{ .name = "name", .ty = Type.init(.string) },
    } };
    try std.testing.expectEqual(@as(?usize, 0), s.indexOf("id"));
    try std.testing.expectEqual(@as(?usize, 1), s.indexOf("name"));
    try std.testing.expectEqual(@as(?usize, null), s.indexOf("missing"));
}
