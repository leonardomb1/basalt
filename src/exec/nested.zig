//! Nested values against their type. A list, map or struct is held as JSON text,
//! and `CAST` to one conforms the text to it: a struct's fields in the type's
//! order, a field the text lacks as null and one it has beyond the type dropped,
//! a list's elements and a map's entries in the order written, and every scalar
//! read as its type and written back as basalt writes JSON (`jsontext.scalar`), so
//! equal values conform to equal text. A container of the wrong JSON kind, a value
//! that does not read as its type, or a null where the type is NOT NULL fails the
//! cast. A map's key is its text as an object key (`jsontext.keyText`).
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

const Writer = std.Io.Writer;

/// Scalar text `text` (a JSON value's content, decoded) as a value of `ty`.
pub fn scalarOf(arena: std.mem.Allocator, ty: types.Type, text: []const u8) EvalError!Value {
    return switch (ty.kind) {
        .string => .{ .string = text },
        .bytes => .{ .bytes = try jsontext.unbase64(arena, text) },
        else => castValueTyped(arena, .{ .string = text }, ty),
    };
}

/// JSON text `text` conformed to nested type `ty`, or null when it is JSON `null`.
pub fn conform(arena: std.mem.Allocator, text: []const u8, ty: types.Type) EvalError!?[]const u8 {
    json.validate(arena, text) catch |e| return if (e == error.OutOfMemory) e else error.CastFailed;
    const t = std.mem.trim(u8, text, " \t\r\n");
    if (t[0] == 'n') return null;
    var out = try Writer.Allocating.initCapacity(arena, t.len + t.len / 4);
    value(arena, &out.writer, t, ty) catch |e| return switch (e) {
        error.WriteFailed => error.OutOfMemory,
        else => |x| x,
    };
    return out.written();
}

const Error = EvalError || Writer.Error;

fn value(arena: std.mem.Allocator, w: *Writer, t: []const u8, ty: types.Type) Error!void {
    if (t[0] == 'n') {
        if (!ty.nullable) return error.CastFailed;
        return w.writeAll("null");
    }
    switch (ty.kind) {
        .@"struct" => {
            if (t[0] != '{') return error.CastFailed;
            const fields = ty.fields orelse return error.CastFailed;
            var buf: [16]?[]const u8 = undefined;
            const got = if (fields.len <= buf.len) buf[0..fields.len] else try arena.alloc(?[]const u8, fields.len);
            try json.pick(arena, t, fields, got);
            try w.writeByte('{');
            for (fields, got, 0..) |f, v, i| {
                if (i > 0) try w.writeByte(',');
                try jsontext.key(w, f.name);
                try value(arena, w, v orelse "null", f.ty);
            }
            try w.writeByte('}');
        },
        .array => {
            if (t[0] != '[') return error.CastFailed;
            const elem = ty.elem orelse return error.CastFailed;
            try w.writeByte('[');
            var it = json.Elements.root(t);
            var first = true;
            while (it.next()) |e| : (first = false) {
                if (!first) try w.writeByte(',');
                try value(arena, w, e, elem.*);
            }
            try w.writeByte(']');
        },
        .map => {
            if (t[0] != '{') return error.CastFailed;
            const kv = ty.fields orelse return error.CastFailed;
            if (kv.len != 2) return error.CastFailed;
            try w.writeByte('{');
            var it = json.Members.root(t);
            var first = true;
            while (it.next()) |m| : (first = false) {
                if (!first) try w.writeByte(',');
                const k = try scalarOf(arena, kv[0].ty, try json.decodeString(arena, m.key));
                try jsontext.key(w, try jsontext.keyText(arena, k));
                try value(arena, w, m.value, kv[1].ty);
            }
            try w.writeByte('}');
        },
        else => {
            const text = switch (try json.cell(arena, t)) {
                .null => unreachable,
                .text => |x| x,
            };
            try jsontext.scalar(w, try scalarOf(arena, ty, text), jsontext.nested);
        },
    }
}

const testing = std.testing;

fn scalar(kind: types.TypeKind, nullable: bool) types.Type {
    return types.Type.init(kind).withNull(nullable);
}

fn expectConform(a: std.mem.Allocator, ty: types.Type, in: []const u8, want: ?[]const u8) !void {
    const got = try conform(a, in, ty);
    if (want) |x| try testing.expectEqualStrings(x, got.?) else try testing.expect(got == null);
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
    try testing.expectError(error.CastFailed, conform(a, "{\"amt\":1}", rec));
    try testing.expectError(error.CastFailed, conform(a, "{\"id\":\"x\"}", rec));
    try testing.expectError(error.CastFailed, conform(a, "[1]", rec));
    try testing.expectError(error.CastFailed, conform(a, "{\"id\":1,\"tags\":{}}", rec));
    try testing.expectError(error.CastFailed, conform(a, "{\"id\":", rec));
}

test "nested: a map reads its keys as the key type and keeps the order written; a NOT NULL element refuses a null" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const m = types.Type{ .kind = .map, .fields = &.{ .{ .name = "key", .ty = scalar(.int, false) }, .{ .name = "value", .ty = scalar(.bool, true) } } };
    try expectConform(a, m, "{\"10\":true,\"2\":\"false\",\"03\":null}", "{\"10\":true,\"2\":false,\"3\":null}");
    try testing.expectError(error.CastFailed, conform(a, "{\"k\":true}", m));
    const strict = types.Type{ .kind = .array, .elem = &scalar(.string, false) };
    try expectConform(a, strict, "[\"a\",\"b\\n\"]", "[\"a\",\"b\\n\"]");
    try testing.expectError(error.CastFailed, conform(a, "[\"a\",null]", strict));
}
