//! JSON objects to rows, for `read http`: the schema is inferred from the first
//! object, and later objects coerce to it or read as null.

const std = @import("std");
const types = @import("../lang/types.zig");
const column = @import("../exec/column.zig");
const Batch = @import("../exec/batch.zig").Batch;
const Value = @import("../exec/value.zig").Value;
const eval = @import("../exec/eval.zig");

const json = std.json;

/// Schema from the first object's keys and value types, all nullable.
pub fn inferSchema(arena: std.mem.Allocator, items: []const json.Value) !*types.Schema {
    var fields = std.array_list.Managed(types.Schema.Field).init(arena);
    if (items.len > 0 and items[0] == .object) {
        const obj = items[0].object;
        for (obj.keys()) |k| {
            try fields.append(.{ .name = try arena.dupe(u8, k), .ty = inferType(obj.get(k).?) });
        }
    }
    const schema = try arena.create(types.Schema);
    schema.* = .{ .fields = try fields.toOwnedSlice() };
    return schema;
}

/// Fields missing from an object become null; fields not in the schema are dropped,
/// so later REST pages with drifting keys do not break the run.
pub fn batchFromJson(arena: std.mem.Allocator, schema: *types.Schema, items: []const json.Value) !Batch {
    const builders = try arena.alloc(column.Builder, schema.fields.len);
    for (builders, schema.fields) |*b, f| b.* = column.Builder.init(arena, f.ty);
    for (items) |row| {
        const obj: ?json.ObjectMap = if (row == .object) row.object else null;
        for (schema.fields, 0..) |f, ci| {
            const jv: ?json.Value = if (obj) |o| o.get(f.name) else null;
            try builders[ci].append(try coerce(arena, jv, f.ty));
        }
    }
    const cols = try arena.alloc(column.Column, schema.fields.len);
    for (builders, 0..) |*b, k| cols[k] = try b.finish();
    return .{ .schema = schema, .columns = cols, .len = items.len };
}

fn inferType(v: json.Value) types.Type {
    return (switch (v) {
        .integer => types.Type.init(.int),
        .float, .number_string => types.Type.init(.float),
        .bool => types.Type.init(.bool),
        else => types.Type.init(.string),
    }).asNullable();
}

/// A value the column's type cannot read becomes null, so an inferred schema survives
/// later rows that drift.
fn coerce(arena: std.mem.Allocator, jv: ?json.Value, ty: types.Type) !Value {
    const v = jv orelse return .null;
    return try readJson(arena, v, ty) orelse .null;
}

/// A JSON value as `ty`, read as CAST reads it (strings as text, numbers and booleans
/// as themselves); null when the type cannot read it. Strings keep any value as text.
fn readJson(arena: std.mem.Allocator, v: json.Value, ty: types.Type) !?Value {
    if (v == .null) return .null;
    if (ty.kind == .string) return .{ .string = try jsonToString(arena, v) };
    const src: Value = switch (v) {
        .integer => |x| .{ .int = x },
        .float => |x| .{ .float = x },
        .bool => |x| .{ .bool = x },
        .string, .number_string => |s| .{ .string = s },
        else => return null,
    };
    return eval.castValueTyped(arena, src, ty) catch |e| switch (e) {
        error.OutOfMemory => e,
        else => null,
    };
}

fn jsonToString(arena: std.mem.Allocator, v: json.Value) ![]const u8 {
    return switch (v) {
        .string => |s| try arena.dupe(u8, s),
        .number_string => |s| try arena.dupe(u8, s),
        .integer => |x| try std.fmt.allocPrint(arena, "{d}", .{x}),
        .float => |x| try std.fmt.allocPrint(arena, "{d}", .{x}),
        .bool => |x| if (x) "true" else "false",
        .object, .array => try std.json.Stringify.valueAlloc(arena, v, .{}),
        .null => "",
    };
}

fn testRows(arena: std.mem.Allocator, text: []const u8) !Batch {
    const root = try json.parseFromSliceLeaky(json.Value, arena, text, .{});
    return batchFromJson(arena, try inferSchema(arena, root.array.items), root.array.items);
}

test "schema comes from row 1; later rows coerce or null" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const b = try testRows(ar.allocator(),
        \\[{"id":1,"name":"a","score":1.5},{"id":"42","extra":true}]
    );
    try std.testing.expectEqual(@as(usize, 3), b.schema.fields.len);
    try std.testing.expectEqual(types.TypeKind.float, b.schema.fields[2].ty.kind);
    try std.testing.expectEqual(@as(i64, 42), b.columns[0].getValue(1).int);
    try std.testing.expect(b.columns[1].getValue(1).isNull());
    try std.testing.expect(b.columns[2].getValue(1).isNull());
    const b2 = try testRows(ar.allocator(),
        \\[{"n":1},{"n":"not-a-number"}]
    );
    try std.testing.expect(b2.columns[0].getValue(1).isNull());
}

test "a JSON array of objects reads as typed rows" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const b = try testRows(ar.allocator(),
        \\[{"id":1,"name":"alice","ok":true},{"id":2,"name":"bob","ok":false}]
    );
    try std.testing.expectEqual(@as(usize, 2), b.len);
    try std.testing.expectEqualStrings("id", b.schema.fields[0].name);
    try std.testing.expectEqual(types.TypeKind.int, b.schema.fields[0].ty.kind);
    try std.testing.expectEqual(types.TypeKind.bool, b.schema.fields[2].ty.kind);
    try std.testing.expectEqual(@as(i64, 1), b.columns[0].getValue(0).int);
    try std.testing.expectEqualStrings("bob", b.columns[1].getValue(1).string);
    try std.testing.expect(b.columns[2].getValue(0).bool);
}

test "inferred schema: object payloads type as string and keep JSON text" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const b = try testRows(ar.allocator(),
        \\[{"id":1,"payload":{"a":[1,2]}}]
    );
    try std.testing.expectEqual(types.TypeKind.string, b.schema.fields[1].ty.kind);
    try std.testing.expectEqualStrings(
        \\{"a":[1,2]}
    , b.columns[1].getValue(0).string);
}

test "inferred schema: a later value its column cannot read is null, not a guess" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const b = try testRows(ar.allocator(),
        \\[{"ok":true,"n":1},{"ok":"yes","n":1e300},{"ok":"str","n":"9"}]
    );
    try std.testing.expect(b.columns[0].getValue(1).isNull());
    try std.testing.expect(b.columns[0].getValue(2).isNull());
    try std.testing.expect(b.columns[1].getValue(1).isNull());
    try std.testing.expectEqual(@as(i64, 9), b.columns[1].getValue(2).int);
}
