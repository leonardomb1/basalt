//! Test helpers shared by the tests of op.zig's parts.

pub const Batch = @import("../op.zig").Batch;
const Op = @import("../op.zig").Op;
pub const Value = @import("../op.zig").Value;
const column = @import("../column.zig");
const driver = @import("../../connect/driver.zig");
const std = @import("std");
const testing = std.testing;
const types = @import("../../lang/types.zig");
pub const TestSource = struct {
    schema_: types.Schema,
    batches: []const Batch,
    idx: usize = 0,

    const vtable = driver.Source.VTable{ .schema = schemaFn, .next = nextFn, .close = closeFn };

    fn schemaFn(p: *anyopaque) types.Schema {
        return @as(*TestSource, @ptrCast(@alignCast(p))).schema_;
    }
    fn nextFn(p: *anyopaque, _: std.mem.Allocator) anyerror!?Batch {
        const self: *TestSource = @ptrCast(@alignCast(p));
        if (self.idx >= self.batches.len) return null;
        defer self.idx += 1;
        return self.batches[self.idx];
    }
    fn closeFn(_: *anyopaque) void {}

    pub fn src(self: *TestSource) driver.Source {
        return .{ .ptr = self, .vtable = &vtable };
    }
};

pub fn intBatch(a: std.mem.Allocator, schema: *const types.Schema, vals: []const ?i64) !Batch {
    const cols = try a.alloc(column.Column, 1);
    cols[0] = try column.intColumn(a, vals);
    return Batch{ .schema = schema, .columns = cols, .len = vals.len };
}

pub fn strBatch(a: std.mem.Allocator, schema: *const types.Schema, vals: []const ?[]const u8) !Batch {
    var bd = column.Builder.init(a, types.Type.init(.string).asNullable());
    for (vals) |v| try bd.append(if (v) |s| Value{ .string = s } else .null);
    const cols = try a.alloc(column.Column, 1);
    cols[0] = try bd.finish();
    return Batch{ .schema = schema, .columns = cols, .len = vals.len };
}

pub fn kvBatch(a: std.mem.Allocator, schema: *const types.Schema, ints: []const ?i64, strs: []const ?[]const u8) !Batch {
    const cols = try a.alloc(column.Column, 2);
    cols[0] = try column.intColumn(a, ints);
    var bd = column.Builder.init(a, types.Type.init(.string).asNullable());
    for (strs) |v| try bd.append(if (v) |s| Value{ .string = s } else .null);
    cols[1] = try bd.finish();
    return Batch{ .schema = schema, .columns = cols, .len = ints.len };
}

pub fn drainInts(a: std.mem.Allocator, top: Op) ![]const ?i64 {
    var got = std.array_list.Managed(?i64).init(a);
    while (try top.next(a)) |b| {
        var r: usize = 0;
        while (r < b.len) : (r += 1) {
            const v = b.columns[0].getValue(r);
            try got.append(if (v.isNull()) null else v.int);
        }
    }
    return got.toOwnedSlice();
}

pub const int_schema = types.Schema{ .fields = &.{
    .{ .name = "x", .ty = types.Type.init(.int).asNullable() },
} };

pub const join_left_schema = types.Schema{ .fields = &.{
    .{ .name = "lk", .ty = types.Type.init(.int).asNullable() },
    .{ .name = "lv", .ty = types.Type.init(.string).asNullable() },
} };

pub const join_right_schema = types.Schema{ .fields = &.{
    .{ .name = "rk", .ty = types.Type.init(.int).asNullable() },
    .{ .name = "rv", .ty = types.Type.init(.string).asNullable() },
} };

pub const join_both_schema = types.Schema{ .fields = &.{
    .{ .name = "lk", .ty = types.Type.init(.int).asNullable() },
    .{ .name = "lv", .ty = types.Type.init(.string).asNullable() },
    .{ .name = "rk", .ty = types.Type.init(.int).asNullable() },
    .{ .name = "rv", .ty = types.Type.init(.string).asNullable() },
} };

pub const JoinRows = struct {
    keys: std.array_list.Managed(?i64),
    rvs: std.array_list.Managed(?[]const u8),

    pub fn collect(a: std.mem.Allocator, top: Op, rvc: ?usize) !JoinRows {
        var out = JoinRows{
            .keys = std.array_list.Managed(?i64).init(a),
            .rvs = std.array_list.Managed(?[]const u8).init(a),
        };
        while (try top.next(a)) |b| {
            var r: usize = 0;
            while (r < b.len) : (r += 1) {
                const kv = b.columns[0].getValue(r);
                try out.keys.append(if (kv.isNull()) null else kv.int);
                if (rvc) |c| {
                    const rv = b.columns[c].getValue(r);
                    try out.rvs.append(if (rv.isNull()) null else rv.string);
                }
            }
        }
        return out;
    }

    pub fn expect(self: JoinRows, keys: []const ?i64, rvs: []const ?[]const u8) !void {
        try testing.expectEqualDeep(keys, @as([]const ?i64, self.keys.items));
        try testing.expectEqual(rvs.len, self.rvs.items.len);
        for (rvs, self.rvs.items) |w, g| {
            if (w) |s| try testing.expectEqualStrings(s, g.?) else try testing.expect(g == null);
        }
    }
};
