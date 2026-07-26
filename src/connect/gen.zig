//! Generated sources — rows made from nothing: `SELECT <exprs>;` with no
//! FROM (one empty row the projection fills with literals) and
//! `FROM RANGE(lo, hi)` (integers lo..hi-1 as one `range` column).

const std = @import("std");
const types = @import("../lang/types.zig");
const col = @import("../exec/column.zig");
const batchmod = @import("../exec/batch.zig");
const driver = @import("driver.zig");

/// One row, zero columns. The select stage above computes every output.
pub const UnitSource = struct {
    gpa: std.mem.Allocator,
    schema_: types.Schema = .{ .fields = &.{} },
    columns: [0]col.Column = .{},
    done: bool = false,

    pub fn open(gpa: std.mem.Allocator) !*UnitSource {
        const self = try gpa.create(UnitSource);
        self.* = .{ .gpa = gpa };
        return self;
    }

    pub fn source(self: *UnitSource) driver.Source {
        return .{ .ptr = self, .vtable = &unit_vtable };
    }

    fn vtSchema(p: *anyopaque) types.Schema {
        const self: *UnitSource = @ptrCast(@alignCast(p));
        return self.schema_;
    }

    fn vtNext(p: *anyopaque, arena: std.mem.Allocator) anyerror!?batchmod.Batch {
        _ = arena;
        const self: *UnitSource = @ptrCast(@alignCast(p));
        if (self.done) return null;
        self.done = true;
        return .{ .schema = &self.schema_, .columns = self.columns[0..], .len = 1 };
    }

    fn vtClose(p: *anyopaque) void {
        const self: *UnitSource = @ptrCast(@alignCast(p));
        self.gpa.destroy(self);
    }

    const unit_vtable = driver.Source.VTable{ .schema = vtSchema, .next = vtNext, .close = vtClose };
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

    fn vtSchema(p: *anyopaque) types.Schema {
        const self: *RangeSource = @ptrCast(@alignCast(p));
        return self.schema_;
    }

    fn vtNext(p: *anyopaque, arena: std.mem.Allocator) anyerror!?batchmod.Batch {
        const self: *RangeSource = @ptrCast(@alignCast(p));
        if (self.next_val >= self.hi) return null;
        const n: usize = @intCast(@min(self.hi - self.next_val, batch_rows));
        const vals = try arena.alloc(i64, n);
        for (vals, 0..) |*v, i| v.* = self.next_val + @as(i64, @intCast(i));
        self.next_val += @as(i64, @intCast(n));
        const cols = try arena.alloc(col.Column, 1);
        cols[0] = .{
            .ty = self.schema_.fields[0].ty,
            .len = n,
            .validity = try col.Bitmap.initFull(arena, n),
            .data = .{ .i64 = vals },
        };
        return .{ .schema = &self.schema_, .columns = cols, .len = n };
    }

    fn vtClose(p: *anyopaque) void {
        const self: *RangeSource = @ptrCast(@alignCast(p));
        self.gpa.free(self.schema_.fields);
        self.gpa.destroy(self);
    }

    const range_vtable = driver.Source.VTable{ .schema = vtSchema, .next = vtNext, .close = vtClose };
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
    try std.testing.expectEqual(@as(?batchmod.Batch, null), try rs.next(arena.allocator()));

    const u = try UnitSource.open(gpa);
    const us = u.source();
    defer us.close();
    const ub = (try us.next(arena.allocator())).?;
    try std.testing.expectEqual(@as(usize, 1), ub.len);
    try std.testing.expectEqual(@as(usize, 0), ub.columns.len);
    try std.testing.expectEqual(@as(?batchmod.Batch, null), try us.next(arena.allocator()));
}
