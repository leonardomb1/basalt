//! Nested values and JSON text. A list, map or struct column holds `column.Nested`
//! storage. JSON text is how one is read in (`appendJson`, behind `CAST` and
//! `Builder.append`) and how it is shown and written as text (`writeJson`).
//! `CAST` to a nested type reads the text as it: a struct's fields in the type's
//! order, a field the text lacks as null and one it has beyond the type dropped,
//! a list's elements and a map's entries in the order written, and every scalar
//! read as its type, so equal values write back (`writeJson`) as equal text. A
//! container of the wrong JSON kind, a value that does not read as its type, or a
//! null where the type is NOT NULL fails the cast. A map's key is read as the key
//! type from its text.
//!
//! `scalarOf` is the one rule for a JSON scalar as a typed value, which the
//! Parquet writer reads nested leaves by too: text as itself, BYTES from the base64
//! JSON spells them with, everything else cast from its text.

const std = @import("std");
const json = @import("json.zig");
const jsontext = @import("jsontext.zig");
const types = @import("../lang/types.zig");
const Value = @import("value.zig").Value;
const castValueTyped = @import("eval/cast.zig").castValueTyped;
const EvalError = @import("eval.zig").EvalError;
const column = @import("column.zig");
const compareValues = @import("eval/support.zig").compareValues;
const Builder = column.Builder;

const Writer = std.Io.Writer;

/// Scalar text `text` (a JSON value's content, decoded) as a value of `ty`.
pub fn scalarOf(arena: std.mem.Allocator, ty: types.Type, text: []const u8) EvalError!Value {
    return switch (ty.kind) {
        .string => .{ .string = text },
        .bytes => .{ .bytes = try jsontext.unbase64(arena, text) },
        else => castValueTyped(arena, .{ .string = text }, ty),
    };
}

/// JSON text `text` read as nested type `ty`, by `appendJson`'s rules: a nested
/// value in a one-row column of its own, or null when the text is JSON `null`.
pub fn parse(arena: std.mem.Allocator, text: []const u8, ty: types.Type) EvalError!Value {
    var b = Builder.init(arena, ty.asNullable());
    try appendJson(&b, text);
    const col = try b.finish();
    if (!col.validity.get(0)) return .null;
    return .{ .nested = .{ .node = col.data.nested, .row = 0 } };
}

/// JSON text `text` appended to nested builder `b` as one row, read by `conform`'s
/// rules; JSON `null` is a null row.
pub fn appendJson(b: *Builder, text: []const u8) Builder.AppendError!void {
    json.validate(b.arena, text) catch |e| return failed(e);
    const t = std.mem.trim(u8, text, " \t\r\n");
    if (t[0] == 'n') return b.append(.null);
    return into(b, t);
}

fn failed(e: anyerror) Builder.AppendError {
    return if (e == error.OutOfMemory) error.OutOfMemory else error.CastFailed;
}

fn into(b: *Builder, t: []const u8) Builder.AppendError!void {
    if (t[0] == 'n') {
        if (!b.ty.nullable) return error.CastFailed;
        return b.append(.null);
    }
    switch (b.ty.kind) {
        .@"struct" => {
            if (t[0] != '{') return error.CastFailed;
            const fields = b.ty.fields orelse return error.CastFailed;
            var buf: [16]?[]const u8 = undefined;
            const got = if (fields.len <= buf.len) buf[0..fields.len] else try b.arena.alloc(?[]const u8, fields.len);
            json.pick(b.arena, t, fields, got) catch |e| return failed(e);
            const nb = try b.openNested();
            for (nb.fields, got) |*k, v| try into(k, v orelse "null");
        },
        .array => {
            if (t[0] != '[') return error.CastFailed;
            const nb = try b.openNested();
            var it = json.Elements.root(t);
            while (it.next()) |e| try into(&nb.list.child, e);
            try Builder.endEntries(nb);
        },
        .map => {
            if (t[0] != '{') return error.CastFailed;
            const nb = try b.openNested();
            var it = json.Members.root(t);
            while (it.next()) |m| {
                const kv = try nb.list.child.openNested();
                const k = &kv.fields[0];
                const key = json.decodeString(b.arena, m.key) catch |e| return failed(e);
                try k.append(scalarOf(b.arena, k.ty, key) catch |e| return failed(e));
                try into(&kv.fields[1], m.value);
            }
            try Builder.endEntries(nb);
        },
        else => {
            const cell = json.cell(b.arena, t) catch |e| return failed(e);
            try b.append(scalarOf(b.arena, b.ty, cell.text) catch |e| return failed(e));
        },
    }
}

/// Row `row` of nested storage `n` as basalt writes it as JSON: a struct an object
/// in its type's field order, a list an array, a map an object keyed by each key's
/// text, every scalar as `jsontext.scalar` writes one nested.
pub fn writeJson(w: *Writer, n: *const column.Nested, row: usize) Writer.Error!void {
    switch (n.body) {
        .fields => |fs| {
            try w.writeByte('{');
            for (fs, n.ty.fields orelse &.{}, 0..) |c, f, i| {
                if (i > 0) try w.writeByte(',');
                try jsontext.key(w, f.name);
                try jsontext.scalar(w, c.getValue(row), jsontext.nested);
            }
            try w.writeByte('}');
        },
        .list => |l| {
            const map = n.ty.kind == .map;
            try w.writeByte(if (map) '{' else '[');
            for (@as(usize, @intCast(l.offsets[row]))..@as(usize, @intCast(l.offsets[row + 1])), 0..) |x, i| {
                if (i > 0) try w.writeByte(',');
                if (!map) {
                    try jsontext.scalar(w, l.child.getValue(x), jsontext.nested);
                    continue;
                }
                const kv = l.child.data.nested.body.fields;
                try writeKey(w, kv[0].getValue(x));
                try w.writeByte(':');
                try jsontext.scalar(w, kv[1].getValue(x), jsontext.nested);
            }
            try w.writeByte(if (map) '}' else ']');
        },
    }
}

/// A map key as an object key: text and BYTES (base64) as the strings they are,
/// temporal values in their quoted ISO text, a number or bool quoted, and a nested
/// key (a file's map may have one) its JSON text as a string.
fn writeKey(w: *Writer, k: Value) Writer.Error!void {
    switch (k) {
        .int, .float, .decimal, .bool => {
            try w.writeByte('"');
            try jsontext.scalar(w, k, jsontext.nested);
            try w.writeByte('"');
        },
        .nested => |r| {
            var inner = Writer.Allocating.init(std.heap.smp_allocator);
            defer inner.deinit();
            try writeJson(&inner.writer, r.node, r.row);
            try jsontext.string(w, inner.written());
        },
        else => try jsontext.scalar(w, k, jsontext.nested),
    }
}

/// `r[key]`: a list's element `key` counted from 1, a map's value under `key` (the
/// first entry with that key) or a struct's field named `key`; null past the end
/// or when no entry has it.
pub fn get(r: @import("value.zig").NestedRef, key: Value) Value {
    const l = switch (r.node.body) {
        .list => |x| x,
        .fields => |fs| {
            if (key != .string) return .null;
            for (r.node.ty.fields orelse &.{}, fs) |f, c| if (std.mem.eql(u8, f.name, key.string)) return c.getValue(r.row);
            return .null;
        },
    };
    const lo: usize = @intCast(l.offsets[r.row]);
    const hi: usize = @intCast(l.offsets[r.row + 1]);
    if (r.node.ty.kind != .map) {
        if (key != .int or key.int < 1 or key.int > hi - lo) return .null;
        return l.child.getValue(lo + @as(usize, @intCast(key.int)) - 1);
    }
    const kv = l.child.data.nested.body.fields;
    for (lo..hi) |x| if (compareValues(kv[0].getValue(x), key) == .eq) return kv[1].getValue(x);
    return .null;
}

/// The order nested values sort, group and join by: a struct field by field in its
/// type's order, a list element by element and then the shorter first, a map entry
/// by entry (key, then value) in its stored order; a NULL part after every value.
/// Null for two values of different shapes, as for scalars of unlike kinds.
/// `keyhash.hashValue` walks the same parts, so values equal here hash alike.
pub fn order(a: @import("value.zig").NestedRef, b: @import("value.zig").NestedRef) ?std.math.Order {
    switch (a.node.body) {
        .fields => |fa| {
            const fb = switch (b.node.body) {
                .fields => |x| x,
                .list => return null,
            };
            if (fa.len != fb.len) return null;
            for (fa, fb) |ca, cb| {
                const o = partOrder(ca.getValue(a.row), cb.getValue(b.row)) orelse return null;
                if (o != .eq) return o;
            }
            return .eq;
        },
        .list => |la| {
            const lb = switch (b.node.body) {
                .list => |x| x,
                .fields => return null,
            };
            if ((a.node.ty.kind == .map) != (b.node.ty.kind == .map)) return null;
            const alo: usize = @intCast(la.offsets[a.row]);
            const na: usize = @as(usize, @intCast(la.offsets[a.row + 1])) - alo;
            const blo: usize = @intCast(lb.offsets[b.row]);
            const nb: usize = @as(usize, @intCast(lb.offsets[b.row + 1])) - blo;
            for (0..@min(na, nb)) |i| {
                const o = partOrder(la.child.getValue(alo + i), lb.child.getValue(blo + i)) orelse return null;
                if (o != .eq) return o;
            }
            return std.math.order(na, nb);
        },
    }
}

fn partOrder(x: Value, y: Value) ?std.math.Order {
    const xn = x.isNull();
    const yn = y.isNull();
    if (xn or yn) return if (xn and yn) .eq else if (xn) .gt else .lt;
    return compareValues(x, y);
}

/// Row `r` copied out of its batch into `arena`, its own one-row storage, its floats
/// canonical (`keyhash.canonF64`), so which of two equal keys a group keeps (`0.0`
/// or `-0.0` inside) never depends on row order or lanes.
pub fn own(arena: std.mem.Allocator, r: @import("value.zig").NestedRef) error{OutOfMemory}!@import("value.zig").NestedRef {
    const node = try column.permuteNested(arena, r.node, &.{r.row});
    canonFloats(node);
    return .{ .node = node, .row = 0 };
}

/// Every float under `n` made canonical in place; `n` must be storage of its own.
pub fn canonFloats(n: *const column.Nested) void {
    const canonF64 = @import("keyhash.zig").canonF64;
    const kids: []const column.Column = switch (n.body) {
        .fields => |fs| fs,
        .list => |*l| (&l.child)[0..1],
    };
    for (kids) |c| switch (c.data) {
        .f64 => |xs| for (xs) |*x| {
            x.* = canonF64(x.*);
        },
        .nested => |nd| canonFloats(nd),
        else => {},
    };
}

/// A nested value's JSON text in `arena`.
pub fn jsonText(arena: std.mem.Allocator, r: @import("value.zig").NestedRef) error{OutOfMemory}![]const u8 {
    var out = Writer.Allocating.init(arena);
    writeJson(&out.writer, r.node, r.row) catch return error.OutOfMemory;
    return out.written();
}

const testing = std.testing;

fn scalar(kind: types.TypeKind, nullable: bool) types.Type {
    return types.Type.init(kind).withNull(nullable);
}

fn expectConform(a: std.mem.Allocator, ty: types.Type, in: []const u8, want: ?[]const u8) !void {
    const got = try parse(a, in, ty);
    if (want) |x| try testing.expectEqualStrings(x, try jsonText(a, got.nested)) else try testing.expect(got == .null);
}

test "nested: a struct takes its type's field order, nulls what is missing, drops the rest and reads each scalar as its type" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const tags = types.Type{ .kind = .array, .nullable = true, .elem = &scalar(.int, true) };
    const rec = types.Type{ .kind = .@"struct", .fields = &.{
        .{ .name = "id", .ty = scalar(.int, false) },
        .{ .name = "amt", .ty = types.Type.decimal(10, 2).asNullable() },
        .{ .name = "d", .ty = scalar(.date, true) },
        .{ .name = "tags", .ty = tags },
    } };
    try expectConform(a, rec, " {\"tags\":[\"1\",2,null],\"x\":true,\"amt\":\"1.5\",\"id\":\"7\"} ", "{\"id\":7,\"amt\":1.50,\"d\":null,\"tags\":[1,2,null]}");
    try expectConform(a, rec, "{\"id\":1,\"d\":\"2024-02-29\",\"tags\":[]}", "{\"id\":1,\"amt\":null,\"d\":\"2024-02-29\",\"tags\":[]}");
    try expectConform(a, rec, "null", null);
    try testing.expectError(error.CastFailed, parse(a, "{\"amt\":1}", rec));
    try testing.expectError(error.CastFailed, parse(a, "{\"id\":\"x\"}", rec));
    try testing.expectError(error.CastFailed, parse(a, "[1]", rec));
    try testing.expectError(error.CastFailed, parse(a, "{\"id\":1,\"tags\":{}}", rec));
    try testing.expectError(error.CastFailed, parse(a, "{\"id\":", rec));
}

test "nested: a map reads its keys as the key type and keeps the order written; a NOT NULL element refuses a null" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const m = types.Type{ .kind = .map, .fields = &.{ .{ .name = "key", .ty = scalar(.int, false) }, .{ .name = "value", .ty = scalar(.bool, true) } } };
    try expectConform(a, m, "{\"10\":true,\"2\":\"false\",\"03\":null}", "{\"10\":true,\"2\":false,\"3\":null}");
    try testing.expectError(error.CastFailed, parse(a, "{\"k\":true}", m));
    const strict = types.Type{ .kind = .array, .elem = &scalar(.string, false) };
    try expectConform(a, strict, "[\"a\",\"b\\n\"]", "[\"a\",\"b\\n\"]");
    try testing.expectError(error.CastFailed, parse(a, "[\"a\",null]", strict));
}
