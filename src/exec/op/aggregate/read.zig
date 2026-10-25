//! How an aggregate reads its argument, for GROUP BY and window functions alike, so
//! the two cannot disagree on a value: a text argument cast once per column
//! (`castText`), each storage's physical type (`Phys`, keyed by `Key`, the column's
//! storage or `nested`), the conversions taken without a cast (an INT or DECIMAL
//! read as FLOAT, an INT as DECIMAL, a `dec64` DECIMAL at its column's scale), and a
//! cast to the type the aggregate reads for anything else (`fromValue`).

const Dec64 = @import("../../value.zig").Dec64;
const Decimal = @import("../../value.zig").Decimal;
const Value = @import("../../value.zig").Value;
const aggregates = @import("../../../lang/aggregates.zig");
const ast = @import("../../../lang/ast.zig");
const column = @import("../../column.zig");
const eval = @import("../../eval.zig");
const funcs = @import("funcs.zig");
const std = @import("std");
const types = @import("../../../lang/types.zig");

const Ctx = funcs.Ctx;

/// The type a text argument of `func` is cast to before it reaches the state, or
/// null for COUNT, MIN and MAX, which take it as text. Parallel CSV lanes carry
/// raw text, so the cast happens here.
pub fn castTo(func: ast.AggFunc, out: types.Type) ?types.Type {
    return switch (aggregates.spec(func).arg) {
        .star_or_any, .any => null,
        .numeric => out.asNullable(),
        .int => types.Type.init(.int).asNullable(),
        .bool => types.Type.init(.bool).asNullable(),
    };
}

/// A text column cast per `castTo`; any other column as it is. A text SUM into an
/// INT or DECIMAL adds exactly or fails; it never rounds.
pub fn castText(arena: std.mem.Allocator, func: ast.AggFunc, out: types.Type, c: column.Column) !column.Column {
    if (c.ty.kind != .string) return c;
    const to = castTo(func, out) orelse return c;
    if (to.kind == .string) return c;
    var bd = try column.Builder.initCapacity(arena, to, c.len);
    for (0..c.len) |r| try bd.append(try castValue(arena, func, to, c.getValue(r)));
    return bd.finish();
}

pub fn castValue(arena: std.mem.Allocator, func: ast.AggFunc, to: types.Type, v: Value) !Value {
    if (v != .string) return v;
    const exact = func == .sum and to.kind != .float;
    return (if (exact) eval.exactSumText(v.string, to) else eval.castValueTyped(arena, v, to)) catch |err| {
        return if (err == error.CastFailed and !exact) eval.castFailure(arena, v, to) else err;
    };
}

/// How an aggregate reads a column: through its storage, or not at all for an ARRAY
/// or STRUCT (`nested`), whose values it sees only as NULL. Kinds sharing a storage
/// read alike, so loops are generated per key, not per kind.
pub const Key = enum { nested, b, i32, i64, f64, dec, dec64, bytes };

pub fn keyOf(c: column.Column) Key {
    return keyOfStorage(c.ty, std.meta.activeTag(c.data));
}

/// The key a column of type `ty` is read through, from its declared type.
pub fn keyOfType(ty: types.Type) Key {
    return keyOfStorage(ty, column.storageOf(ty));
}

fn keyOfStorage(ty: types.Type, t: column.Tag) Key {
    if (ty.kind == .array or ty.kind == .@"struct") return .nested;
    return switch (t) {
        inline else => |x| @field(Key, @tagName(x)),
    };
}

/// The physical type a column read through `key` holds, or null for one read only as values.
pub fn Phys(comptime key: Key) ?type {
    return switch (key) {
        .nested => null,
        .bytes => []const u8,
        inline else => |k| column.Elem(@field(column.Tag, @tagName(k))),
    };
}

pub inline fn get(comptime S: type, c: column.Column, i: usize) S {
    return switch (S) {
        i64 => c.data.i64[i],
        i32 => c.data.i32[i],
        f64 => c.data.f64[i],
        Decimal => c.data.dec[i],
        Dec64 => c.data.dec64[i],
        bool => c.data.b[i],
        []const u8 => c.data.bytes.at(i),
        else => unreachable,
    };
}

pub fn slice(comptime S: type, c: column.Column, n: usize) []const S {
    return switch (S) {
        i64 => c.data.i64[0..n],
        f64 => c.data.f64[0..n],
        Decimal => c.data.dec[0..n],
        Dec64 => c.data.dec64[0..n],
        else => unreachable,
    };
}

/// Whether a value of physical type `S` converts to `In` without a cast.
/// A `dec64` value reads as a DECIMAL or FLOAT at `scale`, its column's.
pub fn converts(comptime S: type, comptime In: type) bool {
    if (S == In) return true;
    return (In == f64 and (S == i64 or S == Decimal or S == Dec64)) or (In == Decimal and (S == i64 or S == Dec64));
}

pub inline fn conv(comptime S: type, comptime In: type, x: S, scale: u8) In {
    if (S == In) return x;
    return switch (In) {
        f64 => switch (S) {
            i64 => @floatFromInt(x),
            Dec64 => x.at(scale).toF64(),
            else => x.toF64(),
        },
        Decimal => if (S == Dec64) x.at(scale) else .{ .unscaled = x, .scale = 0 },
        else => unreachable,
    };
}

/// `v` as an `In`, through a cast to the type the aggregate reads (`cx.ty`) when it
/// is not one already.
pub fn fromValue(comptime In: type, v: Value, cx: Ctx) !In {
    if (In == Value) return v;
    if (extract(In, v, cx.ty.scale)) |x| return x;
    const c = try eval.castValueTyped(cx.scratch, v, cx.ty);
    return extract(In, c, cx.ty.scale) orelse error.TypeMismatch;
}

fn extract(comptime In: type, v: Value, scale: u8) ?In {
    return switch (In) {
        i64 => switch (v) {
            .int, .time, .timestamp => |x| x,
            else => null,
        },
        i32 => if (v == .date) v.date else null,
        f64 => switch (v) {
            .float, .int, .decimal => eval.toF64(v),
            else => null,
        },
        Decimal => eval.asDecimal(v),
        Dec64 => Dec64.fit(eval.asDecimal(v) orelse return null, scale),
        bool => if (v == .bool) v.bool else null,
        []const u8 => switch (v) {
            .string, .bytes => |s| s,
            else => null,
        },
        else => unreachable,
    };
}

/// The type an aggregate reading `In` casts a stray value to; `out` for one that
/// reads values whole, or a DECIMAL that keeps the result's scale.
pub fn inType(comptime In: type, out: types.Type) types.Type {
    return switch (In) {
        i64 => types.Type.init(.int),
        i32 => types.Type.init(.date),
        f64 => types.Type.init(.float),
        Decimal => if (out.kind == .decimal) out else types.Type.decimal(38, 0),
        Dec64 => out,
        bool => types.Type.init(.bool),
        []const u8 => types.Type.init(.string),
        else => out,
    };
}

/// The code `atValid` runs reading `In` through a key: keys of one lane read alike,
/// so an aggregate loop is generated once per lane, not per key.
pub const Lane = enum { none, nested, value, slow, b, i32, i64, f64, dec, dec64, bytes };

pub fn lane(comptime In: type, comptime key: Key) Lane {
    if (In == void) return .none;
    const S = Phys(key) orelse return .nested;
    if (In == Value) return .value;
    if (!converts(S, In)) return .slow;
    return @field(Lane, @tagName(key));
}

/// Row `i` of a column read through `key` as `In`, or null when it is NULL; a
/// nested value counts as present but reads as NULL otherwise. The one per-row read
/// every aggregate loop makes.
pub inline fn at(comptime In: type, comptime key: Key, c: column.Column, i: usize, cx: Ctx) !?In {
    if (!c.validity.get(i)) return null;
    return atValid(In, key, c, i, cx);
}

/// `at` for a row known not NULL.
pub inline fn atValid(comptime In: type, comptime key: Key, c: column.Column, i: usize, cx: Ctx) !?In {
    if (In == void) return {};
    const S = comptime Phys(key) orelse return null;
    if (comptime In != Value and converts(S, In)) {
        if (comptime S == Dec64 and In == Dec64) if (c.ty.scale != cx.ty.scale) return try fromValue(In, c.getValue(i), cx);
        return conv(S, In, get(S, c, i), c.ty.scale);
    }
    return try fromValue(In, c.getValue(i), cx);
}
