//! The key values one side of a join holds, gathered so the other side's read can
//! ask its database for only those: distinct non-null values up to a cap, and the
//! smallest and largest, which still bound a key past the cap. The runtime turns
//! them into `IN (…)` or a range (`runtime/plan/keypush.zig`); a `KeyPush` is how a join hands
//! them over without knowing what reads them.

const Batch = @import("../batch.zig").Batch;
const Value = @import("../value.zig").Value;
const keyhash = @import("../keyhash.zig");
const std = @import("std");

/// One key column's values. `overflow` means `values` stopped at the cap and only
/// `min`/`max` speak for the rest; no values and no overflow means the side holds
/// no non-null key at all, so nothing on the other side can match.
pub const KeyValues = struct {
    values: []const Value,
    overflow: bool = false,
    min: ?Value = null,
    max: ?Value = null,
};

/// Where a join sends the keys of one side: called once, before the other side's
/// first row is read.
pub const KeyPush = struct {
    ctx: *anyopaque,
    apply: *const fn (ctx: *anyopaque, keys: []const KeyValues) anyerror!void,
};

/// The distinct non-null values of each column in `cols` across `batches`, distinct
/// as `keyhash.valueEq` tells them.
pub fn collect(arena: std.mem.Allocator, batches: []const Batch, cols: []const usize, cap: usize) ![]KeyValues {
    const out = try arena.alloc(KeyValues, cols.len);
    for (cols, out) |ci, *kv| {
        var seen = std.HashMap(Value, void, keyhash.SingleKeyCtx, std.hash_map.default_max_load_percentage).init(arena);
        var vals = std.array_list.Managed(Value).init(arena);
        var overflow = false;
        var bounds = keyhash.MinMax{};
        for (batches) |b| {
            const col = b.columns[ci];
            for (0..b.len) |i| {
                const v = col.getValue(i);
                if (v == .null) continue;
                try bounds.fold(arena, v);
                if (overflow) continue;
                if (seen.contains(v)) continue;
                if (vals.items.len == cap) {
                    overflow = true;
                    continue;
                }
                const o = try keyhash.ownValue(arena, v);
                try seen.put(o, {});
                try vals.append(o);
            }
        }
        kv.* = .{ .values = try vals.toOwnedSlice(), .overflow = overflow, .min = bounds.lo(), .max = bounds.hi() };
    }
    return out;
}

test "collect: distinct non-null values per key, min and max past the cap" {
    const testing = std.testing;
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const util = @import("testing_util.zig");
    const b1 = try util.kvBatch(a, &util.join_left_schema, &.{ 3, 1, null, 3 }, &.{ "x", "y", null, "x" });
    const b2 = try util.kvBatch(a, &util.join_left_schema, &.{ 2, 7 }, &.{ "y", "z" });
    const keys = try collect(a, &.{ b1, b2 }, &.{ 0, 1 }, 3);
    try testing.expectEqual(@as(usize, 3), keys[0].values.len);
    try testing.expect(keys[0].overflow);
    try testing.expectEqual(@as(i64, 1), keys[0].min.?.int);
    try testing.expectEqual(@as(i64, 7), keys[0].max.?.int);
    try testing.expectEqual(@as(usize, 3), keys[1].values.len);
    try testing.expect(!keys[1].overflow);
    const none = try collect(a, &.{}, &.{0}, 3);
    try testing.expectEqual(@as(usize, 0), none[0].values.len);
    try testing.expect(!none[0].overflow);
}

test "collect: values equal under keyhash count once, whatever their scale or zero sign" {
    const testing = std.testing;
    const types = @import("../../lang/types.zig");
    const column = @import("../column.zig");
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const schema = types.Schema{ .fields = &.{
        .{ .name = "d", .ty = types.Type.decimal(10, 2) },
        .{ .name = "f", .ty = types.Type.init(.float) },
    } };
    var dec = column.Builder.init(a, schema.fields[0].ty);
    try dec.append(.{ .decimal = .{ .unscaled = 10, .scale = 1 } });
    try dec.append(.{ .decimal = .{ .unscaled = 100, .scale = 2 } });
    var flt = column.Builder.init(a, schema.fields[1].ty);
    try flt.append(.{ .float = -0.0 });
    try flt.append(.{ .float = 0.0 });
    const cols = try a.alloc(column.Column, 2);
    cols[0] = try dec.finish();
    cols[1] = try flt.finish();
    const keys = try collect(a, &.{.{ .schema = &schema, .columns = cols, .len = 2 }}, &.{ 0, 1 }, 1);
    for (keys) |k| {
        try testing.expectEqual(@as(usize, 1), k.values.len);
        try testing.expect(!k.overflow);
    }
}
