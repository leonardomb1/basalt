//! `read request` / `FROM BODY` — turns an HTTP request body (JSON) into rows.
//! Accepts a JSON array of objects (or a single object) and materializes one
//! batch. With a DECLARED schema (`FROM BODY (col TYPE [NOT NULL], ...)`) the
//! body is validated row by row — a violation is a permanent error naming the
//! offending row/column (the server surfaces it as 422). Without one, the
//! schema is inferred from the first object (BSL `read request`).

const std = @import("std");
const types = @import("../lang/types.zig");
const column = @import("../exec/column.zig");
const batchmod = @import("../exec/batch.zig");
const valuemod = @import("../exec/value.zig");
const driver = @import("driver.zig");

const Value = valuemod.Value;
const Batch = batchmod.Batch;
const json = std.json;

pub const RequestSource = struct {
    gpa: std.mem.Allocator,
    arena_inst: std.heap.ArenaAllocator,
    schema: *types.Schema,
    batch: Batch,
    yielded: bool = false,

    /// `declared` is the `FROM BODY (...)` schema (null = infer). On
    /// `error.BodySchemaViolation`, a human-readable reason naming the row and
    /// column is allocated in `msg_arena` and stored in `msg_out`.
    pub fn open(
        gpa: std.mem.Allocator,
        body: []const u8,
        declared: ?[]const types.BodyCol,
        msg_arena: std.mem.Allocator,
        msg_out: *[]const u8,
    ) !*RequestSource {
        const self = try gpa.create(RequestSource);
        self.* = .{ .gpa = gpa, .arena_inst = std.heap.ArenaAllocator.init(gpa), .schema = undefined, .batch = undefined };
        errdefer {
            self.arena_inst.deinit();
            gpa.destroy(self);
        }
        try self.build(self.arena_inst.allocator(), body, declared, msg_arena, msg_out);
        return self;
    }

    fn build(
        self: *RequestSource,
        arena: std.mem.Allocator,
        body: []const u8,
        declared: ?[]const types.BodyCol,
        msg_arena: std.mem.Allocator,
        msg_out: *[]const u8,
    ) !void {
        const root = try json.parseFromSliceLeaky(json.Value, arena, body, .{});
        const items: []const json.Value = switch (root) {
            .array => |arr| arr.items,
            .object => blk: {
                const one = try arena.alloc(json.Value, 1);
                one[0] = root;
                break :blk one;
            },
            else => return error.ExpectedJsonArrayOrObject,
        };

        if (declared) |cols| {
            try validateBody(items, cols, msg_arena, msg_out);
            self.schema = try schemaFromBodyCols(arena, cols);
        } else {
            self.schema = try inferSchema(arena, items);
        }
        self.batch = try batchFromJson(arena, self.schema, items);
    }

    pub fn source(self: *RequestSource) driver.Source {
        return .{ .ptr = self, .vtable = &source_vtable };
    }
};

/// A Schema from declared `FROM BODY` / `ACCEPT BODY` columns: field order is
/// declaration order; NOT NULL keeps the type non-nullable. Shared by the
/// request source and the WAL buffer source.
pub fn schemaFromBodyCols(arena: std.mem.Allocator, cols: []const types.BodyCol) !*types.Schema {
    const fields = try arena.alloc(types.Schema.Field, cols.len);
    for (cols, fields) |c, *f| f.* = .{
        .name = try arena.dupe(u8, c.name),
        .ty = if (c.not_null) c.ty else c.ty.asNullable(),
    };
    const schema = try arena.create(types.Schema);
    schema.* = .{ .fields = fields };
    return schema;
}

/// Row-by-row check of a body against a declared schema: a required column
/// that is missing/null, or a value the declared type can't read, fails the
/// whole request with a message naming the first offending row. Shared with
/// the serve buffer accept path (`ACCEPT BODY ... INTO BUFFER`).
pub fn validateBody(
    items: []const json.Value,
    cols: []const types.BodyCol,
    msg_arena: std.mem.Allocator,
    msg_out: *[]const u8,
) !void {
    for (items, 0..) |row, i| {
        if (row != .object) {
            msg_out.* = try std.fmt.allocPrint(msg_arena, "body row {d} is not a JSON object", .{i});
            return error.BodySchemaViolation;
        }
        const obj = row.object;
        for (cols) |c| {
            const jv = obj.get(c.name);
            if (jv == null or jv.? == .null) {
                if (c.not_null) {
                    msg_out.* = try std.fmt.allocPrint(msg_arena, "body row {d}: required column `{s}` is missing or null", .{ i, c.name });
                    return error.BodySchemaViolation;
                }
                continue;
            }
            if (!coercible(jv.?, c.ty.kind)) {
                msg_out.* = try std.fmt.allocPrint(msg_arena, "body row {d}: column `{s}` cannot be read as {s}", .{ i, c.name, @tagName(c.ty.kind) });
                return error.BodySchemaViolation;
            }
        }
    }
}

/// Can this JSON value be read as the declared kind? (Mirrors `coerce`, which
/// is lenient — validation is the strict pass that runs first.)
fn coercible(v: json.Value, kind: types.TypeKind) bool {
    return switch (kind) {
        .int => switch (v) {
            .integer, .bool => true,
            .float => true,
            .string => |s| blk: {
                _ = std.fmt.parseInt(i64, s, 10) catch break :blk false;
                break :blk true;
            },
            else => false,
        },
        .float => switch (v) {
            .float, .integer => true,
            .number_string, .string => |s| blk: {
                _ = std.fmt.parseFloat(f64, s) catch break :blk false;
                break :blk true;
            },
            else => false,
        },
        .bool => switch (v) {
            .bool, .integer => true,
            .string => true,
            else => false,
        },
        // string / bytes / temporal / decimal / json: stored as text downstream
        // (a CAST in the query does the strict conversion) — anything passes.
        else => true,
    };
}

/// Schema from the first object's keys and value types (int/float/bool/string,
/// all nullable). Shared by `read request` and `read http`.
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

/// One batch from an array of JSON objects, coerced to `schema`. Fields missing
/// from an object become null; fields not in the schema are dropped (keeps later
/// REST pages with drifting keys from breaking the run).
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

fn coerce(arena: std.mem.Allocator, jv: ?json.Value, ty: types.Type) !Value {
    const v = jv orelse return .null;
    if (v == .null) return .null;
    return switch (ty.kind) {
        .int => switch (v) {
            .integer => |x| .{ .int = x },
            .float => |x| .{ .int = @intFromFloat(x) },
            .bool => |x| .{ .int = if (x) 1 else 0 },
            .string => |s| .{ .int = std.fmt.parseInt(i64, s, 10) catch return .null },
            else => .null,
        },
        .float => switch (v) {
            .float => |x| .{ .float = x },
            .integer => |x| .{ .float = @floatFromInt(x) },
            .number_string, .string => |s| .{ .float = std.fmt.parseFloat(f64, s) catch return .null },
            else => .null,
        },
        .bool => switch (v) {
            .bool => |x| .{ .bool = x },
            .integer => |x| .{ .bool = x != 0 },
            .string => |s| .{ .bool = std.mem.eql(u8, s, "true") },
            else => .null,
        },
        .string => .{ .string = try jsonToString(arena, v) },
        else => .null,
    };
}

fn jsonToString(arena: std.mem.Allocator, v: json.Value) ![]const u8 {
    return switch (v) {
        .string => |s| try arena.dupe(u8, s),
        .number_string => |s| try arena.dupe(u8, s),
        .integer => |x| try std.fmt.allocPrint(arena, "{d}", .{x}),
        .float => |x| try std.fmt.allocPrint(arena, "{d}", .{x}),
        .bool => |x| if (x) "true" else "false",
        // Objects/arrays ride as canonical JSON text (a declared `payload JSON`
        // column must not flatten to "").
        .object, .array => try std.json.Stringify.valueAlloc(arena, v, .{}),
        .null => "",
    };
}

const source_vtable = driver.Source.VTable{ .schema = srcSchema, .next = srcNext, .close = srcClose };

fn srcSchema(ptr: *anyopaque) types.Schema {
    const self: *RequestSource = @ptrCast(@alignCast(ptr));
    return self.schema.*;
}
fn srcNext(ptr: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
    _ = arena;
    const self: *RequestSource = @ptrCast(@alignCast(ptr));
    if (self.yielded) return null;
    self.yielded = true;
    return self.batch;
}
fn srcClose(ptr: *anyopaque) void {
    const self: *RequestSource = @ptrCast(@alignCast(ptr));
    self.arena_inst.deinit();
    self.gpa.destroy(self);
}

test "request source parses a JSON array of objects" {
    const gpa = std.testing.allocator;
    var msg: []const u8 = "";
    var s = try RequestSource.open(gpa,
        \\[{"id":1,"name":"alice","ok":true},{"id":2,"name":"bob","ok":false}]
    , null, std.testing.allocator, &msg);
    defer srcClose(s);
    try std.testing.expectEqual(@as(usize, 2), s.batch.len);
    try std.testing.expectEqualStrings("id", s.schema.fields[0].name);
    try std.testing.expectEqual(types.TypeKind.int, s.schema.fields[0].ty.kind);
    try std.testing.expectEqual(types.TypeKind.bool, s.schema.fields[2].ty.kind);
    try std.testing.expectEqual(@as(i64, 1), s.batch.columns[0].getValue(0).int);
    try std.testing.expectEqualStrings("bob", s.batch.columns[1].getValue(1).string);
    try std.testing.expect(s.batch.columns[2].getValue(0).bool);
}

test "declared body schema: column order, types, and enforcement" {
    const gpa = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();

    const decl = [_]types.BodyCol{
        .{ .name = "device_id", .ty = types.Type.init(.string), .not_null = true },
        .{ .name = "value", .ty = types.Type.init(.int) },
    };
    var msg: []const u8 = "";

    // Valid body: schema follows the declaration (order + types), extra keys drop.
    var s = try RequestSource.open(gpa,
        \\[{"value":7,"device_id":"a","extra":true},{"device_id":"b"}]
    , &decl, a, &msg);
    defer srcClose(s);
    try std.testing.expectEqual(@as(usize, 2), s.schema.fields.len);
    try std.testing.expectEqualStrings("device_id", s.schema.fields[0].name);
    try std.testing.expectEqual(types.TypeKind.int, s.schema.fields[1].ty.kind);
    try std.testing.expectEqual(@as(i64, 7), s.batch.columns[1].getValue(0).int);

    // NOT NULL violation names the row and column.
    try std.testing.expectError(error.BodySchemaViolation, RequestSource.open(gpa,
        \\[{"device_id":"a"},{"value":3}]
    , &decl, a, &msg));
    try std.testing.expect(std.mem.indexOf(u8, msg, "row 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, msg, "device_id") != null);

    // Type violation: "x" is not readable as int.
    try std.testing.expectError(error.BodySchemaViolation, RequestSource.open(gpa,
        \\[{"device_id":"a","value":"x"}]
    , &decl, a, &msg));
    try std.testing.expect(std.mem.indexOf(u8, msg, "value") != null);
}

test "JSON-typed columns keep object payloads as JSON text" {
    const gpa = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();

    const decl = [_]types.BodyCol{
        .{ .name = "payload", .ty = types.Type.init(.string) }, // JSON rides as text
    };
    var msg: []const u8 = "";
    var s = try RequestSource.open(gpa,
        \\[{"payload":{"a":1,"b":[true,null]}}]
    , &decl, arena.allocator(), &msg);
    defer srcClose(s);
    try std.testing.expectEqualStrings(
        \\{"a":1,"b":[true,null]}
    , s.batch.columns[0].getValue(0).string);
}

test "inferred schema: object payloads type as string and keep JSON text" {
    const gpa = std.testing.allocator;
    var msg: []const u8 = "";
    var s = try RequestSource.open(gpa,
        \\[{"id":1,"payload":{"a":[1,2]}}]
    , null, std.testing.allocator, &msg);
    defer srcClose(s);
    try std.testing.expectEqual(types.TypeKind.string, s.schema.fields[1].ty.kind);
    try std.testing.expectEqualStrings(
        \\{"a":[1,2]}
    , s.batch.columns[1].getValue(0).string);
}
