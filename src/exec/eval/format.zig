//! Values as text: how dates, times, timestamps and decimals print.

const Value = @import("../value.zig").Value;
const civilFromDays = @import("time.zig").civilFromDays;
const formatDecimal = @import("support.zig").formatDecimal;
const std = @import("std");
const evalLit = @import("testing_util.zig").evalLit;

pub fn writeValue(w: anytype, v: Value) !void {
    switch (v) {
        .null => {},
        .string, .bytes => |x| try w.writeAll(x),
        .bool => |b| try w.writeAll(if (b) "true" else "false"),
        .int => |x| try w.print("{d}", .{x}),
        .float => |x| try w.print("{d}", .{x}),
        .decimal => |d| try writeDecimal(w, d.unscaled, d.scale),
        .date => |x| try writeDate(w, x),
        .time => |x| try writeTime(w, x),
        .timestamp => |x| try writeTimestamp(w, x),
    }
}

pub fn writeDate(w: anytype, days: i64) !void {
    const c = civilFromDays(days);
    try writeYear(w, c.y);
    try w.print("-{d:0>2}-{d:0>2}", .{ c.m, c.d });
}

/// Four digits, zero-padded; a year before 0 carries a leading `-`, as ISO
/// 8601's expanded form does (printing one used to trap on the cast).
fn writeYear(w: anytype, y: i64) !void {
    if (y < 0) try w.writeByte('-');
    try w.print("{d:0>4}", .{@abs(y)});
}

pub fn writeTime(w: anytype, t: i64) !void {
    const us: u64 = @intCast(@mod(t, 86_400_000_000));
    const secs = us / 1_000_000;
    const frac = us % 1_000_000;
    if (frac != 0) {
        try w.print("{d:0>2}:{d:0>2}:{d:0>2}.{d:0>6}", .{ secs / 3600, (secs % 3600) / 60, secs % 60, frac });
    } else {
        try w.print("{d:0>2}:{d:0>2}:{d:0>2}", .{ secs / 3600, (secs % 3600) / 60, secs % 60 });
    }
}

pub fn writeTimestamp(w: anytype, micros: i64) !void {
    const days = @divFloor(micros, 86_400_000_000);
    const us: u64 = @intCast(micros - days * 86_400_000_000);
    const secs = us / 1_000_000;
    const frac = us % 1_000_000;
    const c = civilFromDays(days);
    try writeYear(w, c.y);
    if (frac != 0) {
        try w.print("-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}.{d:0>6}", .{
            c.m, c.d, secs / 3600, (secs % 3600) / 60, secs % 60, frac,
        });
    } else {
        try w.print("-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}", .{
            c.m, c.d, secs / 3600, (secs % 3600) / 60, secs % 60,
        });
    }
}

pub fn writeDecimal(w: anytype, unscaled: i128, scale: u8) !void {
    const neg = unscaled < 0;
    var mag: u128 = if (neg) @intCast(-unscaled) else @intCast(unscaled);

    var digits: [48]u8 = undefined;
    var n: usize = 0;
    if (mag == 0) {
        digits[0] = '0';
        n = 1;
    }
    while (mag > 0) : (mag /= 10) {
        digits[n] = @intCast('0' + mag % 10);
        n += 1;
    }
    while (n <= scale) : (n += 1) digits[n] = '0';

    if (neg) try w.writeByte('-');
    var k: usize = n;
    while (k > 0) {
        k -= 1;
        try w.writeByte(digits[k]);
        if (scale > 0 and k == scale) try w.writeByte('.');
    }
}

pub fn valueToString(arena: std.mem.Allocator, v: Value) ![]const u8 {
    return switch (v) {
        .null => "",
        .string => |s| s,
        .bytes => |s| s,
        .bool => |b| if (b) "true" else "false",
        .int => |x| try std.fmt.allocPrint(arena, "{d}", .{x}),
        .float => |x| try std.fmt.allocPrint(arena, "{d}", .{x}),
        .decimal => |d| try formatDecimal(arena, d.unscaled, d.scale),
        .date => |x| try formatDate(arena, x),
        .time => |x| try formatTime(arena, x),
        .timestamp => |x| try formatTimestamp(arena, x),
    };
}

pub const fmt_bound = 128;

/// Renders through `writeDate` into a fixed `fmt_bound` buffer, which cannot
/// overflow (the widest output is 17 bytes), so the catch is `unreachable`.
pub fn formatDate(arena: std.mem.Allocator, days: i64) ![]const u8 {
    var buf: [fmt_bound]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    writeDate(&w, days) catch unreachable;
    return arena.dupe(u8, w.buffered());
}

pub fn formatTime(arena: std.mem.Allocator, t: i64) ![]const u8 {
    var buf: [fmt_bound]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    writeTime(&w, t) catch unreachable;
    return arena.dupe(u8, w.buffered());
}

pub fn formatTimestamp(arena: std.mem.Allocator, micros: i64) ![]const u8 {
    var buf: [fmt_bound]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    writeTimestamp(&w, micros) catch unreachable;
    return arena.dupe(u8, w.buffered());
}

test "dates and timestamps before year 0 print with a sign instead of trapping" {
    var buf: [128]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try writeDate(&w, -1_000_000);
    try w.writeByte(' ');
    try writeTimestamp(&w, -1_000_000 * 86_400_000_000 + 1);
    try w.writeByte('|');
    try writeDate(&w, -719_529);
    try w.writeByte('|');
    try writeDate(&w, -719_530);
    try std.testing.expectEqualStrings("-0768-02-05 -0768-02-05 00:00:00.000001|0000-01-01|-0001-12-31", w.buffered());
}

test "format temporal values for text sinks" {
    const alloc = std.testing.allocator;
    const cases = .{
        .{ try formatDate(alloc, 0), "1970-01-01" },
        .{ try formatDate(alloc, -1), "1969-12-31" },
        .{ try formatTimestamp(alloc, 0), "1970-01-01 00:00:00" },
        .{ try formatTimestamp(alloc, 86_400_000_000 + (1 * 3600 + 2 * 60 + 3) * 1_000_000), "1970-01-02 01:02:03" },
    };
    inline for (cases) |c| {
        defer alloc.free(c[0]);
        try std.testing.expectEqualStrings(c[1], c[0]);
    }
}

test "date builtins: month clamp, boundary diffs, epoch round trip, strftime padding" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    try std.testing.expectEqualStrings("2024-02-29", try formatDate(a, (try evalLit(a, "date_add('month', 1, cast('2024-01-31' as date))")).date));
    try std.testing.expectEqualStrings("2023-02-28", try formatDate(a, (try evalLit(a, "date_add('month', 1, cast('2023-01-31' as date))")).date));
    try std.testing.expectEqualStrings("2023-12-31", try formatDate(a, (try evalLit(a, "date_add('day', -1, cast('2024-01-01' as date))")).date));
    try std.testing.expectEqualStrings("2025-03-15", try formatDate(a, (try evalLit(a, "date_add('year', 1, cast('2024-03-15' as date))")).date));
    try std.testing.expectEqualStrings("2024-02-29 06:30:00", try formatTimestamp(a, (try evalLit(a, "date_add('month', 1, cast('2024-01-31 06:30:00' as timestamp))")).timestamp));

    try std.testing.expectEqual(@as(i64, 1), (try evalLit(a, "date_diff('year', cast('2023-12-31' as date), cast('2024-01-01' as date))")).int);
    try std.testing.expectEqual(@as(i64, 1), (try evalLit(a, "date_diff('month', cast('2023-12-31' as date), cast('2024-01-01' as date))")).int);
    try std.testing.expectEqual(@as(i64, 60), (try evalLit(a, "date_diff('day', cast('2024-01-01' as date), cast('2024-03-01' as date))")).int);
    try std.testing.expectEqual(@as(i64, -1), (try evalLit(a, "date_diff('day', cast('2024-01-02' as date), cast('2024-01-01' as date))")).int);

    try std.testing.expectEqualStrings("2024-02-29", try formatDate(a, (try evalLit(a, "make_date(2024, 2, 29)")).date));
    try std.testing.expectError(error.CastFailed, evalLit(a, "make_date(2023, 2, 29)"));
    try std.testing.expectError(error.CastFailed, evalLit(a, "make_date(2023, 13, 1)"));

    try std.testing.expectEqual(@as(i64, 1700000000), (try evalLit(a, "epoch(to_timestamp(1700000000))")).int);
    try std.testing.expectEqual(@as(i64, 0), (try evalLit(a, "epoch(cast('1970-01-01' as date))")).int);

    try std.testing.expectEqualStrings("1970-01-01 00:00:00", (try evalLit(a, "strftime(to_timestamp(0), '%Y-%m-%d %H:%M:%S')")).string);
    try std.testing.expectEqualStrings("70 01:01:01 %", (try evalLit(a, "strftime(to_timestamp(3661), '%y %H:%M:%S %%')")).string);
}

test "json_reduce: folds, typed accumulators, positions, shadowing" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    try std.testing.expectEqual(@as(i64, 6), (try evalLit(a, "json_reduce('[1,2,3]', 0, (acc, x) -> acc + x)")).int);
    try std.testing.expectEqual(@as(i64, 0), (try evalLit(a, "json_reduce('[]', 0, (acc, x) -> acc + x)")).int);
    try std.testing.expect((try evalLit(a, "json_reduce(NULL, 0, (acc, x) -> acc + x)")) == .null);
    try std.testing.expectEqual(@as(f64, 3.5), (try evalLit(a, "json_reduce('[1.5,2]', 0.0, (acc, x) -> acc + x)")).float);
    try std.testing.expectError(error.CastFailed, evalLit(a, "json_reduce('[1.5,2]', 0, (acc, x) -> acc + x)"));

    const d = try evalLit(a, "json_reduce('[\"0.10\",\"0.25\"]', CAST(0 AS DECIMAL(10,2)), (acc, x) -> acc + CAST(x AS DECIMAL(10,2)))");
    try std.testing.expectEqualStrings("0.35", try valueToString(a, d));
    try std.testing.expectEqualStrings("2026-01-04", try formatDate(a, (try evalLit(a, "json_reduce('[1,2]', CAST('2026-01-01' AS DATE), (dt, x) -> date_add('day', x, dt))")).date));

    try std.testing.expectEqual(@as(i64, 22), (try evalLit(a, "json_reduce('[1,2,3]', 0, (acc, x, i) -> acc + x * CAST(json_get('[5,4,3]', CAST(i AS STRING)) AS INT))")).int);
    try std.testing.expectEqualStrings("[10,21]", (try evalLit(a, "json_transform('[10,20]', (x, i) -> x + i)")).string);

    try std.testing.expect((try evalLit(a, "json_any('[[1,2],[3]]', x -> json_reduce(x, 0, (acc, x) -> acc + x) = 3)")).bool);
}

test "writeValue renders exactly what valueToString does, for every kind" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const cases = [_]Value{
        .null,
        .{ .bool = true },
        .{ .bool = false },
        .{ .int = 0 },
        .{ .int = -1 },
        .{ .int = std.math.maxInt(i64) },
        .{ .int = std.math.minInt(i64) },
        .{ .float = 0 },
        .{ .float = -0.0 },
        .{ .float = 0.1 },
        .{ .float = 1.0 / 3.0 },
        .{ .float = -2.5e-8 },
        .{ .float = 1.7976931348623157e308 },
        .{ .float = 5e-324 },
        .{ .float = std.math.inf(f64) },
        .{ .float = -std.math.inf(f64) },
        .{ .float = std.math.nan(f64) },
        .{ .decimal = .{ .unscaled = 0, .scale = 0 } },
        .{ .decimal = .{ .unscaled = 1700, .scale = 2 } },
        .{ .decimal = .{ .unscaled = -1700, .scale = 2 } },
        .{ .decimal = .{ .unscaled = 5, .scale = 6 } },
        .{ .decimal = .{ .unscaled = std.math.maxInt(i128), .scale = 0 } },
        .{ .decimal = .{ .unscaled = std.math.minInt(i128) + 1, .scale = 10 } },
        .{ .string = "" },
        .{ .string = "plain" },
        .{ .bytes = "raw" },
        .{ .date = 0 },
        .{ .date = 20000 },
        .{ .date = 2932896 },
        .{ .time = 0 },
        .{ .time = 1 },
        .{ .time = 86_400_000_000 - 1 },
        .{ .timestamp = 0 },
        .{ .timestamp = -1 },
        .{ .timestamp = 1_754_000_000_000_000 },
        .{ .timestamp = 1_754_000_000_123_456 },
    };

    for (cases) |v| {
        var buf: [512]u8 = undefined;
        var w = std.Io.Writer.fixed(&buf);
        try writeValue(&w, v);
        const want = try valueToString(a, v);
        std.testing.expectEqualStrings(want, w.buffered()) catch |e| {
            std.debug.print("mismatch on {s}\n", .{@tagName(v)});
            return e;
        };
    }
}

test "formatTime suppresses an all-zero fraction, like formatTimestamp" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings("12:00:00", try formatTime(a, 12 * 3600 * 1_000_000));
    try std.testing.expectEqualStrings("00:00:00", try formatTime(a, 0));
    try std.testing.expectEqualStrings("23:59:59.999999", try formatTime(a, 86_400_000_000 - 1));
    try std.testing.expectEqualStrings("00:00:00.000001", try formatTime(a, 1));
}
