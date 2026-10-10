//! `FROM BODY`: turns an HTTP request body (JSON) into rows.
//! Accepts a JSON array of objects (or a single object) and materializes one
//! batch. With a declared schema (`FROM BODY (col TYPE [NOT NULL], ...)`) the
//! body is validated row by row, and a violation is a permanent error naming the
//! offending row and column (the server surfaces it as 422). Without one, the
//! schema is inferred from the first object.

const std = @import("std");
const types = @import("../lang/types.zig");
const column = @import("../exec/column.zig");
const Batch = @import("../exec/batch.zig").Batch;
const Value = @import("../exec/value.zig").Value;
const driver = @import("driver.zig");
const eval = @import("../exec/eval.zig");

const json = std.json;

pub const RequestSource = struct {
    gpa: std.mem.Allocator,
    arena_inst: std.heap.ArenaAllocator,
    schema_: *types.Schema,
    batch: Batch,
    yielded: bool = false,

    /// `declared` null infers the schema. On `error.BodySchemaViolation` a reason naming
    /// the row and column is allocated in `msg_arena` and stored in `msg_out`.
    pub fn open(
        gpa: std.mem.Allocator,
        body: []const u8,
        declared: ?[]const types.BodyCol,
        msg_arena: std.mem.Allocator,
        msg_out: *[]const u8,
    ) !*RequestSource {
        const self = try gpa.create(RequestSource);
        self.* = .{ .gpa = gpa, .arena_inst = std.heap.ArenaAllocator.init(gpa), .schema_ = undefined, .batch = undefined };
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
            self.schema_ = try schemaFromBodyCols(arena, cols);
        } else {
            self.schema_ = try inferSchema(arena, items);
        }
        self.batch = try batchFromJson(arena, self.schema_, items);
    }

    pub fn source(self: *RequestSource) driver.Source {
        return .{ .ptr = self, .vtable = &source_vtable };
    }

    pub fn schema(self: *RequestSource) types.Schema {
        return self.schema_.*;
    }
    pub fn next(self: *RequestSource, arena: std.mem.Allocator) anyerror!?Batch {
        _ = arena;
        if (self.yielded) return null;
        self.yielded = true;
        return self.batch;
    }
    pub fn close(self: *RequestSource) void {
        self.arena_inst.deinit();
        self.gpa.destroy(self);
    }
};

/// Field order is declaration order; NOT NULL keeps the type non-nullable. Shared by
/// the request source and the WAL buffer source.
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

/// Strict pass before the lenient `coerce`: a missing or null required column, or a
/// value the declared type cannot read (by `readJson`), fails the whole request at the
/// first bad row.
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
            if (c.ty.kind != .string and try readJson(msg_arena, jv.?, c.ty) == null) {
                msg_out.* = try std.fmt.allocPrint(msg_arena, "body row {d}: column `{s}` cannot be read as {s}", .{ i, c.name, @tagName(c.ty.kind) });
                return error.BodySchemaViolation;
            }
        }
    }
}

/// Schema from the first object's keys and value types, all nullable. Shared by
/// `read request` and `read http`.
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
/// later rows that drift; a declared schema rejected such rows in `validateBody`.
fn coerce(arena: std.mem.Allocator, jv: ?json.Value, ty: types.Type) !Value {
    const v = jv orelse return .null;
    return try readJson(arena, v, ty) orelse .null;
}

/// A JSON value as `ty`, read as CAST reads it (strings as text, numbers and booleans
/// as themselves); null when the type cannot read it. Strings keep any value as text,
/// BYTES takes a string's raw bytes.
fn readJson(arena: std.mem.Allocator, v: json.Value, ty: types.Type) !?Value {
    if (v == .null) return .null;
    switch (ty.kind) {
        .string => return .{ .string = try jsonToString(arena, v) },
        .bytes => return if (v == .string) .{ .bytes = try arena.dupe(u8, v.string) } else null,
        else => {},
    }
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

const source_vtable = driver.sourceVTable(RequestSource);

test "request source: a single object becomes one row" {
    const gpa = std.testing.allocator;
    var msg: []const u8 = "";
    var s = try RequestSource.open(gpa,
        \\{"id": 7, "name": "solo"}
    , null, gpa, &msg);
    defer s.close();
    try std.testing.expectEqual(@as(usize, 1), s.batch.len);
    try std.testing.expectEqual(@as(i64, 7), s.batch.columns[0].getValue(0).int);
    try std.testing.expectEqualStrings("solo", s.batch.columns[1].getValue(0).string);
}

test "request source: schema comes from row 1; later rows coerce or null" {
    const gpa = std.testing.allocator;
    var msg: []const u8 = "";
    var s = try RequestSource.open(gpa,
        \\[{"id":1,"name":"a","score":1.5},{"id":"42","extra":true}]
    , null, gpa, &msg);
    defer s.close();
    try std.testing.expectEqual(@as(usize, 3), s.schema().fields.len);
    try std.testing.expectEqual(types.TypeKind.float, s.schema().fields[2].ty.kind);
    try std.testing.expectEqual(@as(i64, 42), s.batch.columns[0].getValue(1).int);
    try std.testing.expect(s.batch.columns[1].getValue(1).isNull());
    try std.testing.expect(s.batch.columns[2].getValue(1).isNull());
    var s2 = try RequestSource.open(gpa,
        \\[{"n":1},{"n":"not-a-number"}]
    , null, gpa, &msg);
    defer s2.close();
    try std.testing.expect(s2.batch.columns[0].getValue(1).isNull());
}

test "request source: non-array/object bodies are rejected by name" {
    const gpa = std.testing.allocator;
    var msg: []const u8 = "";
    try std.testing.expectError(error.ExpectedJsonArrayOrObject, RequestSource.open(gpa, "42", null, gpa, &msg));
    try std.testing.expectError(error.ExpectedJsonArrayOrObject, RequestSource.open(gpa,
        \\"just a string"
    , null, gpa, &msg));
    try std.testing.expectError(error.SyntaxError, RequestSource.open(gpa, "{nope", null, gpa, &msg));
}

test "request source parses a JSON array of objects" {
    const gpa = std.testing.allocator;
    var msg: []const u8 = "";
    var s = try RequestSource.open(gpa,
        \\[{"id":1,"name":"alice","ok":true},{"id":2,"name":"bob","ok":false}]
    , null, std.testing.allocator, &msg);
    defer s.close();
    try std.testing.expectEqual(@as(usize, 2), s.batch.len);
    try std.testing.expectEqualStrings("id", s.schema().fields[0].name);
    try std.testing.expectEqual(types.TypeKind.int, s.schema().fields[0].ty.kind);
    try std.testing.expectEqual(types.TypeKind.bool, s.schema().fields[2].ty.kind);
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

    var s = try RequestSource.open(gpa,
        \\[{"value":7,"device_id":"a","extra":true},{"device_id":"b"}]
    , &decl, a, &msg);
    defer s.close();
    try std.testing.expectEqual(@as(usize, 2), s.schema().fields.len);
    try std.testing.expectEqualStrings("device_id", s.schema().fields[0].name);
    try std.testing.expectEqual(types.TypeKind.int, s.schema().fields[1].ty.kind);
    try std.testing.expectEqual(@as(i64, 7), s.batch.columns[1].getValue(0).int);

    try std.testing.expectError(error.BodySchemaViolation, RequestSource.open(gpa,
        \\[{"device_id":"a"},{"value":3}]
    , &decl, a, &msg));
    try std.testing.expect(std.mem.indexOf(u8, msg, "row 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, msg, "device_id") != null);

    try std.testing.expectError(error.BodySchemaViolation, RequestSource.open(gpa,
        \\[{"device_id":"a","value":"x"}]
    , &decl, a, &msg));
    try std.testing.expect(std.mem.indexOf(u8, msg, "value") != null);
}

test "a declared string column keeps an object payload as JSON text" {
    const gpa = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();

    const decl = [_]types.BodyCol{
        .{ .name = "payload", .ty = types.Type.init(.string) },
    };
    var msg: []const u8 = "";
    var s = try RequestSource.open(gpa,
        \\[{"payload":{"a":1,"b":[true,null]}}]
    , &decl, arena.allocator(), &msg);
    defer s.close();
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
    defer s.close();
    try std.testing.expectEqual(types.TypeKind.string, s.schema().fields[1].ty.kind);
    try std.testing.expectEqualStrings(
        \\{"a":[1,2]}
    , s.batch.columns[1].getValue(0).string);
}

test "declared body columns read as CAST reads them; unreadable values are rejected" {
    const gpa = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const decl = [_]types.BodyCol{
        .{ .name = "d", .ty = types.Type.init(.date), .not_null = true },
        .{ .name = "m", .ty = types.Type.decimal(10, 2), .not_null = true },
        .{ .name = "ts", .ty = types.Type.init(.timestamp) },
        .{ .name = "t", .ty = types.Type.init(.time) },
        .{ .name = "b", .ty = types.Type.init(.bytes) },
        .{ .name = "ok", .ty = types.Type.init(.bool) },
        .{ .name = "n", .ty = types.Type.init(.int) },
    };
    var msg: []const u8 = "";
    var s = try RequestSource.open(gpa,
        \\[{"d":"2026-02-01","m":"1.25","ts":"2026-02-01 10:00:00","t":"10:30","b":"xy","ok":"TRUE","n":"7"},
        \\ {"d":"2026-02-02","m":3.5,"ok":true,"n":2.0}]
    , &decl, a, &msg);
    defer s.close();
    try std.testing.expectEqual(@as(i32, 20485), s.batch.columns[0].getValue(0).date);
    try std.testing.expectEqual(@as(i128, 125), s.batch.columns[1].getValue(0).decimal.unscaled);
    try std.testing.expectEqual(@as(i128, 350), s.batch.columns[1].getValue(1).decimal.unscaled);
    try std.testing.expect(!s.batch.columns[2].getValue(0).isNull());
    try std.testing.expectEqual(@as(i64, 37_800_000_000), s.batch.columns[3].getValue(0).time);
    try std.testing.expectEqualStrings("xy", s.batch.columns[4].getValue(0).bytes);
    try std.testing.expect(s.batch.columns[5].getValue(0).bool);
    try std.testing.expectEqual(@as(i64, 7), s.batch.columns[6].getValue(0).int);

    const bad = [_][]const u8{
        \\{"d":"garbage","m":"1"}
        ,
        \\{"d":"2026-02-01","m":"1,5"}
        ,
        \\{"d":"2026-02-01","m":"1","ok":"yes"}
        ,
        \\{"d":"2026-02-01","m":"1","ok":"maybe"}
        ,
        \\{"d":"2026-02-01","m":"1","n":1e300}
        ,
        \\{"d":"2026-02-01","m":"1","b":5}
        ,
    };
    for (bad) |body| {
        try std.testing.expectError(error.BodySchemaViolation, RequestSource.open(gpa, body, &decl, a, &msg));
        try std.testing.expect(std.mem.indexOf(u8, msg, "row 0") != null);
    }
}

test "inferred schema: a later value its column cannot read is null, not a guess" {
    const gpa = std.testing.allocator;
    var msg: []const u8 = "";
    var s = try RequestSource.open(gpa,
        \\[{"ok":true,"n":1},{"ok":"yes","n":1e300},{"ok":"str","n":"9"}]
    , null, gpa, &msg);
    defer s.close();
    try std.testing.expect(s.batch.columns[0].getValue(1).isNull());
    try std.testing.expect(s.batch.columns[0].getValue(2).isNull());
    try std.testing.expect(s.batch.columns[1].getValue(1).isNull());
    try std.testing.expectEqual(@as(i64, 9), s.batch.columns[1].getValue(2).int);
}
