//! An aggregate's several arguments as one value: `string_agg(x, ',' ORDER BY y)` and
//! its like fold one column, as every aggregate does, its rows packed by the hidden
//! scalar `__agg_args(flags, values..., keys...)`. So the planner, column pruning,
//! FILTER, spilling and the lanes see one ordinary expression and one BYTES column.
//!
//! `flags` is a constant string: a digit, how many of the values come before the sort
//! keys, then one letter per key (`keyFlag`). A row is the flags, length first, then
//! each value as a tag and its bytes. A nested value travels as its JSON text, read
//! back at its declared type (`firstAs`).

const std = @import("std");
const Decimal = @import("value.zig").Decimal;
const Value = @import("value.zig").Value;
const nested = @import("nested.zig");
const types = @import("../lang/types.zig");

pub const fn_name = "__agg_args";

const Tag = enum(u8) { null, bool, int, float, decimal, string, bytes, date, time, timestamp, nested };

/// A sort key's direction and NULL placement as a flags letter.
pub fn keyFlag(desc: bool, nulls_first: bool) u8 {
    return 'a' + @as(u8, @intFromBool(desc)) + 2 * @as(u8, @intFromBool(nulls_first));
}

pub fn encode(arena: std.mem.Allocator, flags: []const u8, vals: []const Value) ![]const u8 {
    var out = std.array_list.Managed(u8).init(arena);
    try out.append(@intCast(flags.len));
    try out.appendSlice(flags);
    for (vals) |v| switch (v) {
        .null => try out.append(@intFromEnum(Tag.null)),
        .bool => |x| {
            try out.append(@intFromEnum(Tag.bool));
            try out.append(@intFromBool(x));
        },
        .int => |x| try pod(&out, .int, i64, x),
        .float => |x| try pod(&out, .float, f64, x),
        .decimal => |x| try pod(&out, .decimal, Decimal, x),
        .date => |x| try pod(&out, .date, i32, x),
        .time => |x| try pod(&out, .time, i64, x),
        .timestamp => |x| try pod(&out, .timestamp, i64, x),
        .string => |s| try text(&out, .string, s),
        .bytes => |s| try text(&out, .bytes, s),
        .nested => |r| try text(&out, .nested, try nested.jsonText(arena, r)),
    };
    return out.items;
}

fn pod(out: *std.array_list.Managed(u8), tag: Tag, comptime T: type, x: T) !void {
    try out.append(@intFromEnum(tag));
    try out.appendSlice(std.mem.asBytes(&x));
}

fn text(out: *std.array_list.Managed(u8), tag: Tag, s: []const u8) !void {
    try out.append(@intFromEnum(tag));
    const n: u32 = @intCast(s.len);
    try out.appendSlice(std.mem.asBytes(&n));
    try out.appendSlice(s);
}

/// A packed row read back; `vals` borrow its bytes, a nested value as its text.
pub const Row = struct {
    leading: usize,
    keys: []const u8,
    vals: []const Value,
    first_nested: bool,

    /// The first value at its declared type `ty`.
    pub fn firstAs(self: Row, arena: std.mem.Allocator, ty: types.Type) !Value {
        if (!self.first_nested) return self.vals[0];
        return nested.parse(arena, self.vals[0].string, ty);
    }
};

/// How many values row `row` holds, for the `buf` of `decode`.
pub fn valueCount(row: []const u8) usize {
    const flags = row[1..][0..row[0]];
    return (flags[0] - '0') + flags.len - 1;
}

/// Row `row` with its values in `buf`, which `valueCount` sizes.
pub fn decode(row: []const u8, buf: []Value) Row {
    const flags = row[1..][0..row[0]];
    var at: usize = 1 + flags.len;
    var first_nested = false;
    for (buf, 0..) |*v, i| {
        const tag: Tag = @enumFromInt(row[at]);
        at += 1;
        v.* = switch (tag) {
            .null => .null,
            .bool => blk: {
                at += 1;
                break :blk .{ .bool = row[at - 1] != 0 };
            },
            .int => .{ .int = read(i64, row, &at) },
            .float => .{ .float = read(f64, row, &at) },
            .decimal => .{ .decimal = read(Decimal, row, &at) },
            .date => .{ .date = read(i32, row, &at) },
            .time => .{ .time = read(i64, row, &at) },
            .timestamp => .{ .timestamp = read(i64, row, &at) },
            .string => .{ .string = slice(row, &at) },
            .bytes => .{ .bytes = slice(row, &at) },
            .nested => blk: {
                if (i == 0) first_nested = true;
                break :blk .{ .string = slice(row, &at) };
            },
        };
    }
    return .{ .leading = flags[0] - '0', .keys = flags[1..], .vals = buf, .first_nested = first_nested };
}

fn read(comptime T: type, row: []const u8, at: *usize) T {
    var x: T = undefined;
    @memcpy(std.mem.asBytes(&x), row[at.*..][0..@sizeOf(T)]);
    at.* += @sizeOf(T);
    return x;
}

fn slice(row: []const u8, at: *usize) []const u8 {
    const n = read(u32, row, at);
    defer at.* += n;
    return row[at.*..][0..n];
}

test "agg_args: a row reads back as it was packed, every kind and the key flags" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const flags = [_]u8{ '2', keyFlag(true, false), keyFlag(false, true) };
    const vals = [_]Value{ .{ .string = "abc" }, .null, .{ .int = -7 }, .{ .float = 1.5 } };
    const row = try encode(a, &flags, &vals);
    try std.testing.expectEqual(@as(usize, 4), valueCount(row));
    var buf: [4]Value = undefined;
    const r = decode(row, &buf);
    try std.testing.expectEqual(@as(usize, 2), r.leading);
    try std.testing.expectEqualStrings("abc", r.vals[0].string);
    try std.testing.expect(r.vals[1] == .null);
    try std.testing.expectEqual(@as(i64, -7), r.vals[2].int);
    try std.testing.expectEqual(@as(f64, 1.5), r.vals[3].float);
    try std.testing.expectEqual(@as(u8, 'b'), r.keys[0]);
    try std.testing.expectEqual(@as(u8, 'c'), r.keys[1]);
}
