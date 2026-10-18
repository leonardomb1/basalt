//! A `Batch` is the universal currency between operators: a slice of columns
//! (struct-of-arrays) sharing one schema and row count, ~4096 rows at a time.
//! Column buffers are owned by the producing operator's per-batch arena.

const std = @import("std");
const types = @import("../lang/types.zig");
const Column = @import("column.zig").Column;
const column_mod = @import("column.zig");
const intColumn = @import("column.zig").intColumn;
const permute = @import("column.zig").permute;

pub const Batch = struct {
    schema: *const types.Schema,
    columns: []Column,
    len: usize,

    pub fn column(self: Batch, name: []const u8) ?*Column {
        const idx = self.schema.indexOf(name) orelse return null;
        return &self.columns[idx];
    }

    /// Every column copied into `a`, for a batch that must outlive the per-batch
    /// arena it was produced in.
    pub fn deepCopy(self: Batch, a: std.mem.Allocator) !Batch {
        const cols = try a.alloc(Column, self.columns.len);
        for (cols, self.columns) |*o, c| o.* = try column_mod.copy(a, c);
        return .{ .schema = self.schema, .columns = cols, .len = self.len };
    }

    /// The batch's heap, typed stores plus validity bitmaps: what holding it costs.
    pub fn heapBytes(self: Batch) usize {
        var n: usize = 0;
        for (self.columns) |c| {
            n += c.validity.bits.len + switch (c.data) {
                .bytes => |b| b.values.len + b.offsets.len * @sizeOf(i32),
                inline else => |s| std.mem.sliceAsBytes(s).len,
            };
        }
        return n;
    }
};

test "batch column lookup by name" {
    const alloc = std.testing.allocator;

    const id_col = try intColumn(alloc, &.{ 10, 20 });
    defer {
        alloc.free(id_col.validity.bits);
        alloc.free(id_col.data.i64);
    }

    const schema = types.Schema{ .fields = &.{
        .{ .name = "id", .ty = types.Type.init(.int) },
    } };
    var cols = [_]Column{id_col};
    const b = Batch{ .schema = &schema, .columns = &cols, .len = 2 };

    const got = b.column("id").?;
    try std.testing.expectEqual(@as(i64, 10), got.getValue(0).int);
    try std.testing.expect(b.column("missing") == null);
}
