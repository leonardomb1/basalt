//! The engine's canonical type system: the type lattice every column and value
//! speaks, the nullability flag, and the implicit-widening / unification rules.
//!
//! Coercion policy: implicit widening only (`int -> decimal`, `int -> float`);
//! everything else needs an explicit `cast`. Nulls follow SQL three-valued logic,
//! tracked as a per-type `nullable` flag; a bare `null` literal is `unknown` and
//! unifies with anything.
//!
//! `BodyCol` (a declared `FROM BODY (...)` column) lives here so the connect layer
//! does not need the AST. A `Field`'s `rel`/`base` are set on columns a join's
//! right side contributed: the side's alias and the column's original name, which
//! `name` no longer is once a collision renamed it `x_r`.

const std = @import("std");

pub const TypeKind = enum {
    bool,
    int,
    float,
    decimal,
    string,
    bytes,
    date,
    time,
    timestamp,
    array,
    @"struct",

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
    unknown: bool = false,
    precision: u8 = 0,
    scale: u8 = 0,
    elem: ?*const Type = null,
    fields: ?[]const Field = null,

    pub const Field = struct { name: []const u8, ty: Type };

    pub fn init(kind: TypeKind) Type {
        return .{ .kind = kind };
    }

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

    /// The type as `DESCRIBE` and Tab name it: the kind, and a decimal's
    /// precision and scale. Nullability is not part of the name.
    pub fn name(self: Type, arena: std.mem.Allocator) ![]const u8 {
        if (self.kind == .decimal) return std.fmt.allocPrint(arena, "decimal({d},{d})", .{ self.precision, self.scale });
        return @tagName(self.kind);
    }

    pub fn withNull(self: Type, n: bool) Type {
        var t = self;
        t.nullable = n;
        return t;
    }

    pub fn eql(a: Type, b: Type) bool {
        if (a.kind != b.kind) return false;
        if (a.kind == .decimal and (a.precision != b.precision or a.scale != b.scale)) return false;
        return true;
    }

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

pub const BodyCol = struct { name: []const u8, ty: Type, not_null: bool = false };

pub const Schema = struct {
    fields: []const Field,

    pub const Field = struct { name: []const u8, ty: Type, rel: []const u8 = "", base: []const u8 = "" };

    pub fn indexOf(self: Schema, name: []const u8) ?usize {
        for (self.fields, 0..) |f, i| {
            if (std.mem.eql(u8, f.name, name)) return i;
        }
        return null;
    }

    /// The column a possibly qualified name refers to. `b.x`, where `b` is a join's
    /// right side, is that side's `x` whatever it was renamed to; any other
    /// qualifier is decoration and the last part alone decides.
    pub fn resolve(self: Schema, parts: []const []const u8) ?usize {
        const name = parts[parts.len - 1];
        if (parts.len > 1) {
            const rel = parts[parts.len - 2];
            for (self.fields, 0..) |f, i| {
                if (f.rel.len != 0 and std.mem.eql(u8, f.rel, rel) and std.mem.eql(u8, f.base, name)) return i;
            }
        }
        return self.indexOf(name);
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
    try std.testing.expect(!ua.unknown);
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

test "Schema.resolve finds a join's renamed right-side column by its qualified name" {
    const s = Schema{ .fields = &.{
        .{ .name = "id", .ty = Type.init(.int) },
        .{ .name = "x", .ty = Type.init(.string) },
        .{ .name = "x_r", .ty = Type.init(.int), .rel = "b", .base = "x" },
    } };
    try std.testing.expectEqual(@as(?usize, 2), s.resolve(&.{ "b", "x" }));
    try std.testing.expectEqual(@as(?usize, 1), s.resolve(&.{ "a", "x" }));
    try std.testing.expectEqual(@as(?usize, 1), s.resolve(&.{"x"}));
    try std.testing.expectEqual(@as(?usize, 2), s.resolve(&.{"x_r"}));
    try std.testing.expectEqual(@as(?usize, 0), s.resolve(&.{ "b", "id" }));
    try std.testing.expectEqual(@as(?usize, null), s.resolve(&.{ "b", "missing" }));
    try std.testing.expectEqual(@as(?usize, null), s.indexOf("missing"));
}
