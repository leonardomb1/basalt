//! The key values one side of a join holds, gathered so the other side's read can
//! ask its database for only those: distinct non-null values up to a cap, and the
//! smallest and largest, which still bound a key past the cap. The runtime turns
//! them into `IN (…)` or a range (`keypush.zig`); a `KeyPush` is how a join hands
//! them over without knowing what reads them.

const Batch = @import("../batch.zig").Batch;
const Value = @import("../value.zig").Value;
const eval = @import("../eval.zig");
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

/// The distinct non-null values of each column in `cols` across `batches`.
pub fn collect(arena: std.mem.Allocator, batches: []const Batch, cols: []const usize, cap: usize) ![]KeyValues {
    const out = try arena.alloc(KeyValues, cols.len);
    for (cols, out) |ci, *kv| {
        var seen = std.StringHashMap(void).init(arena);
        var vals = std.array_list.Managed(Value).init(arena);
        var overflow = false;
        var lo: ?Value = null;
        var hi: ?Value = null;
        for (batches) |b| {
            const col = b.columns[ci];
            for (0..b.len) |i| {
                const v = col.getValue(i);
                if (v == .null) continue;
                if (lo == null or (eval.compareValues(v, lo.?) orelse .eq) == .lt) lo = try own(arena, v);
                if (hi == null or (eval.compareValues(v, hi.?) orelse .eq) == .gt) hi = try own(arena, v);
                if (overflow) continue;
                const k = try identity(arena, v);
                const gop = try seen.getOrPut(k);
                if (gop.found_existing) continue;
                if (vals.items.len == cap) {
                    overflow = true;
                    continue;
                }
                try vals.append(try own(arena, v));
            }
        }
        kv.* = .{ .values = try vals.toOwnedSlice(), .overflow = overflow, .min = lo, .max = hi };
    }
    return out;
}

/// A text or bytes value copied out of the batch it came from, which may be freed.
fn own(arena: std.mem.Allocator, v: Value) !Value {
    return switch (v) {
        .string => |s| .{ .string = try arena.dupe(u8, s) },
        .bytes => |s| .{ .bytes = try arena.dupe(u8, s) },
        else => v,
    };
}

/// Equal values have equal identities and values of different kinds never share
/// one, so `1` and `'1'` stay apart.
fn identity(arena: std.mem.Allocator, v: Value) ![]const u8 {
    return std.fmt.allocPrint(arena, "{s}:{s}", .{ @tagName(v), try eval.valueToString(arena, v) });
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
