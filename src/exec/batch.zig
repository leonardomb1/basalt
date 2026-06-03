//! A `Batch` is the universal currency between operators: a slice of columns
//! (struct-of-arrays) sharing one schema and row count, ~4096 rows at a time.
//! Column buffers are owned by the producing operator's per-batch arena.

const std = @import("std");
const types = @import("../lang/types.zig");
const col = @import("column.zig");

pub const Batch = struct {
    schema: *const types.Schema,
    columns: []col.Column,
    len: usize, // rows

    pub fn column(self: Batch, name: []const u8) ?*col.Column {
        const idx = self.schema.indexOf(name) orelse return null;
        return &self.columns[idx];
    }
};

test "batch column lookup by name" {
    const alloc = std.testing.allocator;

    const id_col = try col.intColumn(alloc, &.{ 10, 20 });
    defer {
        alloc.free(id_col.validity.bits);
        alloc.free(id_col.data.i64);
    }

    const schema = types.Schema{ .fields = &.{
        .{ .name = "id", .ty = types.Type.init(.int) },
    } };
    var cols = [_]col.Column{id_col};
    const b = Batch{ .schema = &schema, .columns = &cols, .len = 2 };

    const got = b.column("id").?;
    try std.testing.expectEqual(@as(i64, 10), got.getValue(0).int);
    try std.testing.expect(b.column("missing") == null);
}
