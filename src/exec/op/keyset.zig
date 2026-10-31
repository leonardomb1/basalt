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
    /// Past the cap, an integer key's values as a few closed ranges (`ranged`), when
    /// they cover well under its min..max; empty otherwise.
    ranges: []const [2]i64 = &.{},
};

/// At most this many ranges stand for an integer key past the cap.
pub const max_ranges = 32;
/// Past the cap, integer keys are gathered for `ranged` up to this many.
pub const range_values = 1 << 22;

/// `vals`, sorted and with repeats dropped, cut at their `max_ranges - 1` widest gaps
/// into closed ranges; empty when those cover over half of min..max, where the
/// min/max bound already says as much.
pub fn ranged(arena: std.mem.Allocator, vals: []i64) ![]const [2]i64 {
    if (vals.len < 2) return &.{};
    std.mem.sortUnstable(i64, vals, {}, std.sort.asc(i64));
    var n: usize = 1;
    for (vals[1..]) |v| if (v != vals[n - 1]) {
        vals[n] = v;
        n += 1;
    };
    const u = vals[0..n];
    if (n < 2) return &.{};
    const Gap = struct { at: usize, width: u64 };
    const gaps = try arena.alloc(Gap, n - 1);
    for (gaps, 0..) |*g, i| g.* = .{ .at = i, .width = @intCast(@as(i128, u[i + 1]) - u[i]) };
    const cuts: usize = @min(max_ranges - 1, gaps.len);
    std.mem.sortUnstable(Gap, gaps, {}, struct {
        fn wider(_: void, x: Gap, y: Gap) bool {
            return x.width > y.width or (x.width == y.width and x.at < y.at);
        }
    }.wider);
    const at = try arena.alloc(usize, cuts);
    for (at, gaps[0..cuts]) |*x, g| x.* = g.at;
    std.mem.sortUnstable(usize, at, {}, std.sort.asc(usize));
    const out = try arena.alloc([2]i64, cuts + 1);
    var lo = u[0];
    var covered: u128 = 0;
    for (at, 0..) |i, k| {
        out[k] = .{ lo, u[i] };
        covered += @intCast(@as(i128, u[i]) - lo);
        lo = u[i + 1];
    }
    out[cuts] = .{ lo, u[n - 1] };
    covered += @intCast(@as(i128, u[n - 1]) - lo);
    const span: u128 = @intCast(@as(i128, u[n - 1]) - u[0]);
    if (covered * 2 > span) return &.{};
    return out;
}

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
        var past = std.array_list.Managed(i64).init(arena);
        var ints = true;
        for (batches) |b| {
            const col = b.columns[ci];
            for (0..b.len) |i| {
                const v = col.getValue(i);
                if (v == .null) continue;
                try bounds.fold(arena, v);
                if (v != .int) ints = false;
                if (overflow) {
                    if (ints and past.items.len < range_values) try past.append(v.int);
                    continue;
                }
                if (seen.contains(v)) continue;
                if (vals.items.len == cap) {
                    overflow = true;
                    if (ints) {
                        for (vals.items) |x| try past.append(x.int);
                        try past.append(v.int);
                    }
                    continue;
                }
                const o = try keyhash.ownValue(arena, v);
                try seen.put(o, {});
                try vals.append(o);
            }
        }
        const rs = if (overflow and ints and past.items.len < range_values) try ranged(arena, past.items) else &.{};
        kv.* = .{ .values = try vals.toOwnedSlice(), .overflow = overflow, .min = bounds.lo(), .max = bounds.hi(), .ranges = rs };
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

test "ranged: clustered integer keys become a few ranges holding every key; spread ones none" {
    const testing = std.testing;
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var vals = std.array_list.Managed(i64).init(a);
    for (0..3000) |i| try vals.append(@intCast(if (i % 3 == 0) i else 5_000_000 + i % 997));
    try vals.append(9_000_000);
    const all = try a.dupe(i64, vals.items);
    const rs = try ranged(a, vals.items);
    try testing.expect(rs.len > 1 and rs.len <= max_ranges);
    for (all) |v| {
        const hit = for (rs) |r| {
            if (v >= r[0] and v <= r[1]) break true;
        } else false;
        try testing.expect(hit);
    }
    var spread = std.array_list.Managed(i64).init(a);
    for (0..3000) |i| try spread.append(@intCast(i * 7));
    try testing.expectEqual(@as(usize, 0), (try ranged(a, spread.items)).len);
}
