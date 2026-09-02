//! Generated sources — rows made from nothing: `SELECT <exprs>;` with no
//! FROM (one empty row the projection fills with literals) and
//! `FROM RANGE(lo, hi)` (integers lo..hi-1 as one `range` column).

const std = @import("std");
const types = @import("../lang/types.zig");
const Bitmap = @import("../exec/column.zig").Bitmap;
const Column = @import("../exec/column.zig").Column;
const Batch = @import("../exec/batch.zig").Batch;
const driver = @import("driver.zig");

/// One row, zero columns. The select stage above computes every output.
pub const UnitSource = struct {
    gpa: std.mem.Allocator,
    schema_: types.Schema = .{ .fields = &.{} },
    columns: [0]Column = .{},
    done: bool = false,

    pub fn open(gpa: std.mem.Allocator) !*UnitSource {
        const self = try gpa.create(UnitSource);
        self.* = .{ .gpa = gpa };
        return self;
    }

    pub fn source(self: *UnitSource) driver.Source {
        return .{ .ptr = self, .vtable = &unit_vtable };
    }

    pub fn schema(self: *UnitSource) types.Schema {
        return self.schema_;
    }

    pub fn next(self: *UnitSource, arena: std.mem.Allocator) anyerror!?Batch {
        _ = arena;
        if (self.done) return null;
        self.done = true;
        return .{ .schema = &self.schema_, .columns = self.columns[0..], .len = 1 };
    }

    pub fn close(self: *UnitSource) void {
        self.gpa.destroy(self);
    }

    const unit_vtable = driver.sourceVTable(UnitSource);
};

/// `RANGE(lo, hi)` — streams lo..hi-1 in batches; an empty or inverted range
/// yields zero rows.
pub const RangeSource = struct {
    gpa: std.mem.Allocator,
    schema_: types.Schema,
    next_val: i64,
    hi: i64,

    const batch_rows: i64 = 8192;

    pub fn open(gpa: std.mem.Allocator, lo: i64, hi: i64) !*RangeSource {
        const self = try gpa.create(RangeSource);
        errdefer gpa.destroy(self);
        const fields = try gpa.alloc(types.Schema.Field, 1);
        fields[0] = .{ .name = "range", .ty = .{ .kind = .int } };
        self.* = .{ .gpa = gpa, .schema_ = .{ .fields = fields }, .next_val = lo, .hi = hi };
        return self;
    }

    pub fn source(self: *RangeSource) driver.Source {
        return .{ .ptr = self, .vtable = &range_vtable };
    }

    pub fn schema(self: *RangeSource) types.Schema {
        return self.schema_;
    }

    pub fn next(self: *RangeSource, arena: std.mem.Allocator) anyerror!?Batch {
        if (self.next_val >= self.hi) return null;
        const n: usize = @intCast(@min(self.hi - self.next_val, batch_rows));
        const vals = try arena.alloc(i64, n);
        for (vals, 0..) |*v, i| v.* = self.next_val + @as(i64, @intCast(i));
        self.next_val += @as(i64, @intCast(n));
        const cols = try arena.alloc(Column, 1);
        cols[0] = .{
            .ty = self.schema_.fields[0].ty,
            .len = n,
            .validity = try Bitmap.initFull(arena, n),
            .data = .{ .i64 = vals },
        };
        return .{ .schema = &self.schema_, .columns = cols, .len = n };
    }

    pub fn close(self: *RangeSource) void {
        self.gpa.free(self.schema_.fields);
        self.gpa.destroy(self);
    }

    const range_vtable = driver.sourceVTable(RangeSource);
};

test "range source streams lo..hi-1 and unit source yields one empty row" {
    const gpa = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();

    const r = try RangeSource.open(gpa, 2, 5);
    const rs = r.source();
    defer rs.close();
    const b = (try rs.next(arena.allocator())).?;
    try std.testing.expectEqual(@as(usize, 3), b.len);
    try std.testing.expectEqual(@as(i64, 2), b.columns[0].data.i64[0]);
    try std.testing.expectEqual(@as(i64, 4), b.columns[0].data.i64[2]);
    try std.testing.expectEqual(@as(?Batch, null), try rs.next(arena.allocator()));

    const u = try UnitSource.open(gpa);
    const us = u.source();
    defer us.close();
    const ub = (try us.next(arena.allocator())).?;
    try std.testing.expectEqual(@as(usize, 1), ub.len);
    try std.testing.expectEqual(@as(usize, 0), ub.columns.len);
    try std.testing.expectEqual(@as(?Batch, null), try us.next(arena.allocator()));
}
