//! The engine's canonical type system: the type lattice every column and value
//! speaks, the nullability flag, and the implicit-widening / unification rules.
//!
//! Coercion policy: implicit widening only (`int -> decimal -> float`, as
//! PostgreSQL resolves numeric beside float8); everything else needs an explicit
//! `cast`. Nulls follow SQL three-valued logic, tracked as a per-type `nullable`
//! flag; a bare `null` literal is `unknown` and unifies with anything.
//!
//! A `Field`'s `rel`/`base` are set on columns a join's right side contributed:
//! the side's alias and the column's original name, which `name` no longer is
//! once a collision renamed it `x_r`.

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

    /// The type of a column that held no value: of unknown type when the sample read
    /// the whole input (`whole`), so it unifies with any type and is text where shown
    /// or written, else a nullable string, since unread rows may hold text. Unlike
    /// `unknownNull` (a NULL literal) its kind is `.string`.
    pub fn nullColumn(whole: bool) Type {
        if (whole) return .{ .kind = .string, .nullable = true, .unknown = true };
        return Type.init(.string).asNullable();
    }

    /// The type a sink writes a column as: a column of unknown type is a nullable
    /// string, every other type is itself.
    pub fn sinkType(self: Type) Type {
        if (self.unknown) return Type.init(.string).asNullable();
        return self;
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
        return (from == .int and (to == .decimal or to == .float)) or (from == .decimal and to == .float);
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
                const whole = @max(a.precision -| a.scale, b.precision -| b.scale);
                t.precision = if (a.precision == 0 or b.precision == 0) 0 else @min(38, whole + @max(a.scale, b.scale));
                t.scale = @max(a.scale, b.scale);
            }
            return t;
        }
        if (a.kind == .int and b.kind == .decimal) return unify(decimal(19, 0).withNull(a.nullable), b);
        if (b.kind == .int and a.kind == .decimal) return unify(a, decimal(19, 0).withNull(b.nullable));
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

/// How a 64-bit unsigned integer is typed: as an INT whose values past 2^63-1 are
/// refused (`int_checked`, Parquet and Arrow), or as the exact DECIMAL(20,0)
/// (`decimal`, MySQL `bigint unsigned`).
pub const UnsignedPolicy = enum { int_checked, decimal };

/// The type of an unsigned integer `bits` wide; one narrower than 64 bits is always
/// an INT.
pub fn unsignedType(bits: u8, comptime policy: UnsignedPolicy) Type {
    if (bits < 64) return Type.init(.int);
    return switch (policy) {
        .int_checked => Type.init(.int),
        .decimal => Type.decimal(20, 0),
    };
}

/// A u64 as the engine's int, or null past 2^63-1.
pub fn u64ToEngine(u: u64) ?i64 {
    return std.math.cast(i64, u);
}

/// The unsigned value of a `bits`-wide integer stored sign-extended in `x`, or null
/// for a 64-bit one past 2^63-1.
pub fn unsignedOf(bits: u8, x: i64) ?i64 {
    const u: u64 = @bitCast(x);
    return switch (bits) {
        8 => @as(u8, @truncate(u)),
        16 => @as(u16, @truncate(u)),
        32 => @as(u32, @truncate(u)),
        else => u64ToEngine(u),
    };
}

/// The running type of a column's sampled values: nothing seen yet, one kind, or a
/// mix with no common kind (read as text).
pub const Sample = union(enum) {
    none,
    one: TypeKind,
    mixed,

    /// The sampled kind, a mix being a string; null when nothing was seen.
    pub fn kind(self: Sample) ?TypeKind {
        return switch (self) {
            .none => null,
            .one => |k| k,
            .mixed => .string,
        };
    }
};

/// The sample after one more value of kind `k`. Equal kinds stay, numbers widen as
/// `Type.canWiden` does (int -> decimal -> float), a mix of DATE, TIME and TIMESTAMP is
/// a TIMESTAMP, and any other mix has no common kind.
pub fn widenSample(s: Sample, k: TypeKind) Sample {
    const a = switch (s) {
        .none => return .{ .one = k },
        .mixed => return .mixed,
        .one => |a| a,
    };
    if (Type.canWiden(a, k)) return .{ .one = k };
    if (Type.canWiden(k, a)) return .{ .one = a };
    if (temporal(a) and temporal(k)) return .{ .one = .timestamp };
    return .mixed;
}

fn temporal(k: TypeKind) bool {
    return k == .date or k == .time or k == .timestamp;
}

pub const Schema = struct {
    fields: []const Field,

    pub const Field = struct { name: []const u8, ty: Type, rel: []const u8 = "", base: []const u8 = "", clash: bool = false };

    pub fn indexOf(self: Schema, name: []const u8) ?usize {
        for (self.fields, 0..) |f, i| {
            if (std.mem.eql(u8, f.name, name)) return i;
        }
        return null;
    }

    /// Whether two columns share the unqualified `name`, so that a reference to it
    /// is ambiguous, as SQL reads it. A hidden name the parser gave (`__…`) is one
    /// expression wherever it repeats, so never.
    pub fn ambiguous(self: Schema, name: []const u8) bool {
        if (std.mem.startsWith(u8, name, "__")) return false;
        var n: usize = 0;
        for (self.fields) |f| {
            if (std.mem.eql(u8, f.name, name)) n += 1;
        }
        return n > 1;
    }

    /// Whether one of the join sides `sides` brought a `name` the join renamed
    /// `name_r` for a clash, so that `name` written bare beside those joins could
    /// mean either table, as SQL reads it.
    pub fn clashes(self: Schema, name: []const u8, sides: []const []const u8) bool {
        for (self.fields) |f| {
            if (!f.clash or !std.mem.eql(u8, f.base, name)) continue;
            for (sides) |r| if (std.mem.eql(u8, f.rel, r)) return true;
        }
        return false;
    }

    /// The column a possibly qualified name refers to. `b.x`, where `b` is a join's
    /// right side, is that side's `x` whatever it was renamed to, and none when that
    /// side has no `x` or is not joined yet: a qualified name never falls back to
    /// another table's column of that name. (The parser drops the FROM table's own
    /// alias, which its columns do not carry.)
    pub fn resolve(self: Schema, parts: []const []const u8) ?usize {
        const name = parts[parts.len - 1];
        if (parts.len == 1) return self.indexOf(name);
        const rel = parts[parts.len - 2];
        for (self.fields, 0..) |f, i| {
            if (std.mem.eql(u8, f.rel, rel) and std.mem.eql(u8, f.base, name)) return i;
        }
        return null;
    }
};

test "canWiden follows int -> decimal -> float" {
    try std.testing.expect(Type.canWiden(.int, .float));
    try std.testing.expect(Type.canWiden(.int, .decimal));
    try std.testing.expect(Type.canWiden(.int, .int));
    try std.testing.expect(Type.canWiden(.decimal, .float));
    try std.testing.expect(!Type.canWiden(.float, .decimal));
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

test "unify decimals keeps both sides' whole digits and the larger scale; an int widens as DECIMAL(19,0)" {
    const u = Type.unify(Type.decimal(18, 2), Type.decimal(10, 4)).?;
    try std.testing.expectEqual(TypeKind.decimal, u.kind);
    try std.testing.expectEqual(@as(u8, 20), u.precision);
    try std.testing.expectEqual(@as(u8, 4), u.scale);

    const w = Type.unify(Type.init(.int), Type.decimal(2, 1)).?;
    try std.testing.expectEqual(TypeKind.decimal, w.kind);
    try std.testing.expectEqual(@as(u8, 20), w.precision);
    try std.testing.expectEqual(@as(u8, 1), w.scale);

    try std.testing.expectEqual(TypeKind.float, Type.unify(Type.decimal(2, 1), Type.init(.float)).?.kind);
}

test "eql compares decimal parameters but ignores nullability" {
    try std.testing.expect(Type.eql(Type.decimal(10, 2), Type.decimal(10, 2)));
    try std.testing.expect(!Type.eql(Type.decimal(10, 2), Type.decimal(10, 3)));
    try std.testing.expect(!Type.eql(Type.decimal(11, 2), Type.decimal(10, 2)));
    try std.testing.expect(!Type.eql(Type.init(.int), Type.init(.float)));
    try std.testing.expect(Type.eql(Type.init(.int).asNullable(), Type.init(.int)));
}

test "Schema.resolve finds a join's renamed right-side column by its qualified name, and never another table's" {
    const s = Schema{ .fields = &.{
        .{ .name = "id", .ty = Type.init(.int) },
        .{ .name = "x", .ty = Type.init(.string) },
        .{ .name = "x_r", .ty = Type.init(.int), .rel = "b", .base = "x" },
    } };
    try std.testing.expectEqual(@as(?usize, 2), s.resolve(&.{ "b", "x" }));
    try std.testing.expectEqual(@as(?usize, null), s.resolve(&.{ "a", "x" }));
    try std.testing.expectEqual(@as(?usize, null), s.resolve(&.{ "c", "x" }));
    try std.testing.expectEqual(@as(?usize, 1), s.resolve(&.{"x"}));
    try std.testing.expectEqual(@as(?usize, 2), s.resolve(&.{"x_r"}));
    try std.testing.expectEqual(@as(?usize, null), s.resolve(&.{ "b", "id" }));
    try std.testing.expectEqual(@as(?usize, null), s.resolve(&.{ "b", "missing" }));
    try std.testing.expectEqual(@as(?usize, null), s.indexOf("missing"));
    try std.testing.expect(!s.ambiguous("x"));
    const dup: Schema = .{ .fields = &.{ .{ .name = "a", .ty = Type.init(.int) }, .{ .name = "a", .ty = Type.init(.string) } } };
    try std.testing.expect(dup.ambiguous("a"));
    try std.testing.expect(!dup.ambiguous("b"));
}

fn expectSameType(want: Type, got: Type) !void {
    try std.testing.expectEqual(want.kind, got.kind);
    try std.testing.expectEqual(want.nullable, got.nullable);
    try std.testing.expectEqual(want.unknown, got.unknown);
    try std.testing.expectEqual(want.precision, got.precision);
    try std.testing.expectEqual(want.scale, got.scale);
}

test "nullColumn is texttype's untyped column: unknown string when the whole input was read, else nullable string" {
    const texttype = @import("../format/texttype.zig");
    const empty: texttype.Column = .{};
    try expectSameType(empty.resolve(true), Type.nullColumn(true));
    try expectSameType(empty.resolve(false), Type.nullColumn(false));
    try expectSameType(.{ .kind = .string, .nullable = true, .unknown = true }, Type.nullColumn(true));
    try expectSameType(.{ .kind = .string, .nullable = true }, Type.nullColumn(false));
}

test "nullColumn differs from unknownNull only in kind; both unify with any type" {
    try std.testing.expectEqual(TypeKind.string, Type.nullColumn(true).kind);
    try std.testing.expectEqual(TypeKind.bool, Type.unknownNull().kind);
    for ([_]Type{ Type.nullColumn(true), Type.unknownNull() }) |n| {
        try expectSameType(Type.init(.int).asNullable(), Type.unify(n, Type.init(.int)).?);
        try expectSameType(Type.decimal(10, 2).asNullable(), Type.unify(Type.decimal(10, 2), n).?);
    }
    try std.testing.expect(Type.unify(Type.nullColumn(false), Type.init(.int)) == null);
}

test "sinkType writes an unknown column as a nullable string and every other type as itself" {
    try expectSameType(Type.init(.string).asNullable(), Type.sinkType(Type.unknownNull()));
    try expectSameType(Type.init(.string).asNullable(), Type.sinkType(Type.nullColumn(true)));
    var odd = Type.unknownNull();
    odd.precision = 7;
    odd.scale = 3;
    try expectSameType(Type.init(.string).asNullable(), Type.sinkType(odd));
    const kept = [_]Type{
        Type.init(.int),
        Type.init(.bool).asNullable(),
        Type.decimal(10, 2),
        Type.decimal(38, 4).asNullable(),
        Type.nullColumn(false),
        .{ .kind = .string, .nullable = true, .scale = 2 },
        Type.init(.timestamp),
    };
    for (kept) |t| try expectSameType(t, Type.sinkType(t));
}

test "unsignedType: narrower than 64 bits is INT; 64 bits follows the policy" {
    for ([_]u8{ 8, 16, 24, 32 }) |bits| {
        try expectSameType(Type.init(.int), unsignedType(bits, .int_checked));
        try expectSameType(Type.init(.int), unsignedType(bits, .decimal));
    }
    try expectSameType(Type.init(.int), unsignedType(64, .int_checked));
    try expectSameType(Type.decimal(20, 0), unsignedType(64, .decimal));
}

test "u64ToEngine refuses past 2^63-1, as Arrow's ToInt does" {
    try std.testing.expectEqual(@as(?i64, 0), u64ToEngine(0));
    try std.testing.expectEqual(@as(?i64, std.math.maxInt(i64)), u64ToEngine(std.math.maxInt(i64)));
    try std.testing.expectEqual(@as(?i64, null), u64ToEngine(@as(u64, std.math.maxInt(i64)) + 1));
    try std.testing.expectEqual(@as(?i64, null), u64ToEngine(std.math.maxInt(u64)));
}

test "unsignedOf equals Parquet's asUnsigned on every width" {
    const logical = @import("../format/parquet/logical.zig");
    const xs = [_]i64{ 0, 1, -1, 127, -128, 255, 256, -129, 32767, -32768, 65535, 65536, std.math.maxInt(i32), std.math.minInt(i32), 4294967295, 4294967296, std.math.maxInt(i64), std.math.minInt(i64), -4294967296 };
    for ([_]u8{ 8, 16, 32, 64 }) |bits| for (xs) |x| {
        try std.testing.expectEqual(logical.asUnsigned(bits, x), unsignedOf(bits, x));
    };
    try std.testing.expectEqual(@as(?i64, 255), unsignedOf(8, -1));
    try std.testing.expectEqual(@as(?i64, 4294967295), unsignedOf(32, -1));
    try std.testing.expectEqual(@as(?i64, null), unsignedOf(64, -1));
}

test "unsignedType reproduces MySQL's engineTypeFor on its integer types (mysql.zig:627-641, transcribed)" {
    const Case = struct { mtype: u8, unsigned: bool, bits: u8 };
    const cases = [_]Case{
        .{ .mtype = 0x01, .unsigned = true, .bits = 8 },
        .{ .mtype = 0x02, .unsigned = true, .bits = 16 },
        .{ .mtype = 0x09, .unsigned = true, .bits = 24 },
        .{ .mtype = 0x03, .unsigned = true, .bits = 32 },
        .{ .mtype = 0x08, .unsigned = true, .bits = 64 },
        .{ .mtype = 0x08, .unsigned = false, .bits = 64 },
    };
    for (cases) |c| {
        const today: Type = if (c.mtype == 0x08 and c.unsigned) Type.decimal(20, 0) else Type.init(.int);
        const home: Type = if (c.unsigned) unsignedType(c.bits, .decimal) else Type.init(.int);
        try expectSameType(today, home);
    }
}

fn foldSample(kinds: []const TypeKind) Sample {
    var s: Sample = .none;
    for (kinds) |k| s = widenSample(s, k);
    return s;
}

test "widenSample: equal kinds stay, numbers widen, temporals meet at TIMESTAMP, any other mix is mixed" {
    try std.testing.expectEqual(Sample.none, foldSample(&.{}));
    try std.testing.expectEqual(@as(?TypeKind, null), foldSample(&.{}).kind());
    const Case = struct { []const TypeKind, ?TypeKind, bool };
    const cases = [_]Case{
        .{ &.{.int}, .int, false },
        .{ &.{ .int, .int }, .int, false },
        .{ &.{ .int, .float }, .float, false },
        .{ &.{ .float, .int }, .float, false },
        .{ &.{ .int, .decimal }, .decimal, false },
        .{ &.{ .decimal, .float }, .float, false },
        .{ &.{ .float, .decimal, .int }, .float, false },
        .{ &.{ .bool, .bool }, .bool, false },
        .{ &.{ .bool, .int }, .string, true },
        .{ &.{ .int, .bool }, .string, true },
        .{ &.{ .string, .string }, .string, false },
        .{ &.{ .string, .int }, .string, true },
        .{ &.{ .int, .string, .int }, .string, true },
        .{ &.{ .date, .date }, .date, false },
        .{ &.{ .time, .time }, .time, false },
        .{ &.{ .date, .time }, .timestamp, false },
        .{ &.{ .time, .date }, .timestamp, false },
        .{ &.{ .date, .timestamp }, .timestamp, false },
        .{ &.{ .timestamp, .time }, .timestamp, false },
        .{ &.{ .date, .int }, .string, true },
        .{ &.{ .int, .date }, .string, true },
        .{ &.{ .timestamp, .float }, .string, true },
        .{ &.{ .bytes, .string }, .string, true },
        .{ &.{ .bytes, .bytes }, .bytes, false },
    };
    for (cases) |c| {
        const s = foldSample(c[0]);
        try std.testing.expectEqual(c[1], s.kind());
        try std.testing.expectEqual(c[2], s == .mixed);
    }
    try std.testing.expectEqual(Sample.mixed, widenSample(.mixed, .int));
    try std.testing.expectEqual(Sample.mixed, widenSample(.mixed, .string));
}

fn textCellKind(raw: []const u8, quoted: bool) ?TypeKind {
    const cell = @import("../exec/cell.zig");
    const eval = @import("../exec/eval.zig");
    if (quoted) return .string;
    if (raw.len == 0) return null;
    if (eval.parseIsoDate(raw) != null) return .date;
    if (cell.codeLooking(raw)) return .string;
    if (eval.parseIntStrict(raw) != null) return .int;
    if (eval.parseFloatStrict(raw) != null) return .float;
    return .string;
}

test "widenSample over per-cell kinds reproduces texttype's Column.resolve" {
    const texttype = @import("../format/texttype.zig");
    const Cell = struct { []const u8, bool };
    const columns = [_][]const Cell{
        &.{},
        &.{.{ "", false }},
        &.{ .{ "", false }, .{ "", false } },
        &.{.{ "", true }},
        &.{ .{ "1", false }, .{ "2", false } },
        &.{ .{ "1", false }, .{ "2.5", false } },
        &.{ .{ "1.5", false }, .{ "", false }, .{ "7", false } },
        &.{.{ "1", true }},
        &.{ .{ "1", false }, .{ "1", true } },
        &.{.{ "007", false }},
        &.{ .{ "1", false }, .{ "007", false } },
        &.{.{ "+1", false }},
        &.{.{ "-01", false }},
        &.{.{ "0.5", false }},
        &.{.{ "0", false }},
        &.{.{ "1e400", false }},
        &.{.{ "1e5", false }},
        &.{.{ "nan", false }},
        &.{ .{ "nan", false }, .{ "1", false } },
        &.{.{ "inf", false }},
        &.{.{ "-", false }},
        &.{.{ "2020-01-01", false }},
        &.{ .{ "2020-01-01", false }, .{ "2021-12-31", false } },
        &.{ .{ "2020-01-01", false }, .{ "1", false } },
        &.{ .{ "1", false }, .{ "2020-01-01", false } },
        &.{ .{ "2020-01-01", false }, .{ "1.5", false } },
        &.{.{ "0001-01-01", false }},
        &.{ .{ "0001-01-01", false }, .{ "2020-02-29", false } },
        &.{.{ "2020-02-30", false }},
        &.{ .{ "2020-01-01", true }, .{ "2020-01-02", false } },
        &.{.{ "abc", false }},
        &.{ .{ "abc", false }, .{ "1", false } },
        &.{.{ " 1", false }},
        &.{.{ "1 ", false }},
        &.{.{ "true", false }},
        &.{.{ "12:00:00", false }},
        &.{.{ "2020-01-01 10:00:00", false }},
    };
    for (columns) |cells| for ([_]bool{ true, false }) |whole| {
        var c: texttype.Column = .{};
        var s: Sample = .none;
        for (cells) |x| {
            c.note(x[0], x[1]);
            if (textCellKind(x[0], x[1])) |k| s = widenSample(s, k);
        }
        const today = c.resolve(whole);
        const home = if (s.kind()) |k| Type.init(k).asNullable() else Type.nullColumn(whole);
        try std.testing.expectEqual(today.kind, home.kind);
        try std.testing.expectEqual(today.nullable, home.nullable);
        try std.testing.expectEqual(today.unknown, home.unknown);
    };
}

test "widenSample reproduces JSON Infer's kinds; an all-NULL key is a known STRING there, not nullColumn" {
    const json_read = @import("../format/json/read.zig");
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const Case = struct { []const []const u8, []const TypeKind };
    const cases = [_]Case{
        .{ &.{"1"}, &.{.int} },
        .{ &.{ "1", "2" }, &.{ .int, .int } },
        .{ &.{ "1", "2.5" }, &.{ .int, .float } },
        .{ &.{ "1e3", "4" }, &.{ .float, .int } },
        .{ &.{ "true", "false" }, &.{ .bool, .bool } },
        .{ &.{ "true", "1" }, &.{ .bool, .int } },
        .{ &.{ "\"a\"", "1" }, &.{ .string, .int } },
        .{ &.{ "{\"x\":1}", "[1]" }, &.{ .string, .string } },
        .{ &.{ "null", "1", "null" }, &.{.int} },
        .{ &.{ "null", "null" }, &.{} },
        .{ &.{ "1", "null", "\"x\"", "2" }, &.{ .int, .string, .int } },
        .{ &.{ "18446744073709551616", "1" }, &.{ .float, .int } },
    };
    for (cases) |c| {
        var inf: json_read.Infer = .{};
        for (c[0]) |v| try inf.note(a, try std.fmt.allocPrint(a, "{{\"k\":{s}}}", .{v}));
        const today = (try inf.schema(a)).fields[0].ty;
        const s = foldSample(c[1]);
        const home = if (s.kind()) |k| Type.init(k).asNullable() else Type.init(.string).asNullable();
        try expectSameType(home, today);
    }
}

fn avroBranchSample(s: Sample, b: anytype) Sample {
    if (b.kind == .null) return s;
    const plain = b.logical == .none;
    const k: ?TypeKind = switch (b.kind) {
        .int, .long => if (plain) .int else null,
        .float, .double => if (plain) .float else null,
        .@"enum" => .string,
        .string => if (plain or b.logical == .uuid) .string else null,
        else => null,
    };
    return if (k) |kk| widenSample(s, kk) else .mixed;
}

test "widenSample reproduces Avro's commonType once a logical or non-plain branch counts as mixed" {
    const avro = @import("../format/avro/schema.zig");
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const unions = [_][]const u8{
        \\["int","long"]
        ,
        \\["int","double","null"]
        ,
        \\["float","double"]
        ,
        \\["long","float"]
        ,
        \\["string",{"type":"enum","name":"E","symbols":["A"]}]
        ,
        \\[{"type":"string","logicalType":"uuid"},{"type":"enum","name":"E","symbols":["A"]}]
        ,
        \\["string","int"]
        ,
        \\["boolean","int"]
        ,
        \\["boolean","string"]
        ,
        \\["bytes","string"]
        ,
        \\["null","bytes","int"]
        ,
        \\[{"type":"int","logicalType":"date"},"long"]
        ,
        \\[{"type":"int","logicalType":"date"},{"type":"long","logicalType":"timestamp-millis"}]
        ,
        \\[{"type":"int","logicalType":"time-millis"},{"type":"long","logicalType":"timestamp-micros"}]
        ,
        \\[{"type":"bytes","logicalType":"decimal","precision":10,"scale":2},"int"]
        ,
        \\[{"type":"array","items":"int"},"int"]
        ,
    };
    for (unions) |text| {
        const root = try avro.parse(a, text);
        const p = try avro.plan(a, root);
        var s: Sample = .none;
        for (root.branches) |b| s = avroBranchSample(s, b);
        var text_step = false;
        for (p.step.choice.branches) |st| if (st == .text) {
            text_step = true;
        };
        try std.testing.expectEqual(@as(usize, 1), p.schema.fields.len);
        try std.testing.expectEqual(s == .mixed, text_step);
        try std.testing.expectEqual(s.kind().?, p.schema.fields[0].ty.kind);
    }
}

test "widenSample reproduces xlsx Stats.kind over per-cell kinds (xlsx/read.zig:200-244, transcribed)" {
    const Stats = struct {
        text: bool = false,
        boolean: bool = false,
        number: bool = false,
        integral: bool = true,
        dated: bool = false,
        timed: bool = false,
        fraction: bool = false,
        any: bool = false,

        fn kind(self: @This()) TypeKind {
            if (!self.any or self.text) return .string;
            const nkinds = @as(u8, @intFromBool(self.boolean)) + @intFromBool(self.number) + @intFromBool(self.dated or self.timed);
            if (nkinds > 1) return .string;
            if (self.boolean) return .bool;
            if (self.number) return if (self.integral) .int else .float;
            if (self.timed and !self.dated and !self.fraction) return .time;
            if (self.dated and !self.timed and !self.fraction) return .date;
            return .timestamp;
        }
    };
    const X = enum { text, boolean, num_int, num_frac, date_whole, date_frac, datetime, time_short, time_long };
    const all = std.enums.values(X);
    var buf: [3]X = undefined;
    for (0..all.len + 1) |n0| for (0..all.len + 1) |n1| for (0..all.len + 1) |n2| {
        var len: usize = 0;
        for ([_]usize{ n0, n1, n2 }) |n| if (n < all.len) {
            buf[len] = all[n];
            len += 1;
        };
        var st: Stats = .{};
        var s: Sample = .none;
        for (buf[0..len]) |c| {
            switch (c) {
                .text => st.text = true,
                .boolean => st.boolean = true,
                .num_int => st.number = true,
                .num_frac => {
                    st.number = true;
                    st.integral = false;
                },
                .date_whole => st.dated = true,
                .date_frac, .datetime => {
                    st.dated = true;
                    st.fraction = true;
                },
                .time_short => st.timed = true,
                .time_long => {
                    st.timed = true;
                    st.fraction = true;
                },
            }
            st.any = true;
            s = widenSample(s, switch (c) {
                .text => .string,
                .boolean => .bool,
                .num_int => .int,
                .num_frac => .float,
                .date_whole => .date,
                .date_frac, .datetime, .time_long => .timestamp,
                .time_short => .time,
            });
        }
        try std.testing.expectEqual(st.kind(), s.kind() orelse .string);
    };
}
