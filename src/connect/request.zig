//! `read request` — turns an HTTP request body (JSON) into rows. Accepts a JSON
//! array of objects (or a single object), infers the schema from the first
//! object, and materializes one batch. A `driver.Source`, like the others.

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

    pub fn open(gpa: std.mem.Allocator, body: []const u8) !*RequestSource {
        const self = try gpa.create(RequestSource);
        self.* = .{ .gpa = gpa, .arena_inst = std.heap.ArenaAllocator.init(gpa), .schema = undefined, .batch = undefined };
        errdefer {
            self.arena_inst.deinit();
            gpa.destroy(self);
        }
        try self.build(self.arena_inst.allocator(), body);
        return self;
    }

    fn build(self: *RequestSource, arena: std.mem.Allocator, body: []const u8) !void {
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

        self.schema = try inferSchema(arena, items);
        self.batch = try batchFromJson(arena, self.schema, items);
    }

    pub fn source(self: *RequestSource) driver.Source {
        return .{ .ptr = self, .vtable = &source_vtable };
    }
};

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
        else => "",
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
    var s = try RequestSource.open(gpa,
        \\[{"id":1,"name":"alice","ok":true},{"id":2,"name":"bob","ok":false}]
    );
    defer srcClose(s);
    try std.testing.expectEqual(@as(usize, 2), s.batch.len);
    try std.testing.expectEqualStrings("id", s.schema.fields[0].name);
    try std.testing.expectEqual(types.TypeKind.int, s.schema.fields[0].ty.kind);
    try std.testing.expectEqual(types.TypeKind.bool, s.schema.fields[2].ty.kind);
    try std.testing.expectEqual(@as(i64, 1), s.batch.columns[0].getValue(0).int);
    try std.testing.expectEqualStrings("bob", s.batch.columns[1].getValue(1).string);
    try std.testing.expect(s.batch.columns[2].getValue(0).bool);
}
