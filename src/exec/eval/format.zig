//! Values as text: how dates, times, timestamps and decimals print.

const Value = @import("../value.zig").Value;
const civil = @import("../civil.zig");
const formatDecimal = @import("../decimal.zig").formatDecimal;
const std = @import("std");
const evalLit = @import("testing_util.zig").evalLit;

/// `YYYY-MM-DD`; a year past 9999 takes more digits and one before 0 a leading
/// `-`, as ISO 8601's expanded form does.
pub fn writeDate(w: anytype, days: i64) !void {
    return civil.writeLayout(w, days, .date);
}

/// `v` as `{d}` prints it, its digits set down in one write.
pub fn writeInt(w: anytype, v: i64) !void {
    var buf: [20]u8 = undefined;
    var i: usize = buf.len;
    var x: u64 = @abs(v);
    while (true) {
        i -= 1;
        buf[i] = '0' + @as(u8, @intCast(x % 10));
        x /= 10;
        if (x == 0) break;
    }
    if (v < 0) try w.writeByte('-');
    return w.writeAll(buf[i..]);
}

/// `HH:MM:SS`, then `.ffffff` when the fraction is not zero.
pub fn writeTime(w: anytype, t: i64) !void {
    return civil.writeLayout(w, t, .time);
}

/// `writeDate`, a space and `writeTime`.
pub fn writeTimestamp(w: anytype, micros: i64) !void {
    return civil.writeLayout(w, micros, .timestamp);
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

/// The date, time and timestamp writers as they were before `civil.writeLayout`,
/// kept as the oracle the differential test checks it against.
const Old = struct {
    fn writeDate(w: anytype, days: i64) !void {
        const c = civil.civilFromDays(days);
        if (c.y < 0 or c.y > 9999) {
            try writeYear(w, c.y);
            return w.print("-{d:0>2}-{d:0>2}", .{ c.m, c.d });
        }
        var buf: [10]u8 = undefined;
        putDigits(buf[0..4], @intCast(c.y));
        buf[4] = '-';
        putDigits(buf[5..7], @intCast(c.m));
        buf[7] = '-';
        putDigits(buf[8..10], @intCast(c.d));
        return w.writeAll(&buf);
    }

    inline fn putDigits(out: []u8, v: u64) void {
        var x = v;
        var i = out.len;
        while (i > 0) {
            i -= 1;
            out[i] = '0' + @as(u8, @intCast(x % 10));
            x /= 10;
        }
    }

    fn writeYear(w: anytype, y: i64) !void {
        if (y < 0) try w.writeByte('-');
        try w.print("{d:0>4}", .{@abs(y)});
    }

    fn writeTime(w: anytype, t: i64) !void {
        const us: u64 = @intCast(@mod(t, 86_400_000_000));
        const secs = us / 1_000_000;
        const frac = us % 1_000_000;
        if (frac != 0) {
            try w.print("{d:0>2}:{d:0>2}:{d:0>2}.{d:0>6}", .{ secs / 3600, (secs % 3600) / 60, secs % 60, frac });
        } else {
            try w.print("{d:0>2}:{d:0>2}:{d:0>2}", .{ secs / 3600, (secs % 3600) / 60, secs % 60 });
        }
    }

    fn writeTimestamp(w: anytype, micros: i64) !void {
        const days = @divFloor(micros, 86_400_000_000);
        const us: u64 = @intCast(@mod(micros, 86_400_000_000));
        const secs = us / 1_000_000;
        const frac = us % 1_000_000;
        const c = civil.civilFromDays(days);
        if (c.y >= 0 and c.y <= 9999) {
            var buf: [26]u8 = undefined;
            putDigits(buf[0..4], @intCast(c.y));
            buf[4] = '-';
            putDigits(buf[5..7], @intCast(c.m));
            buf[7] = '-';
            putDigits(buf[8..10], @intCast(c.d));
            buf[10] = ' ';
            putDigits(buf[11..13], secs / 3600);
            buf[13] = ':';
            putDigits(buf[14..16], (secs % 3600) / 60);
            buf[16] = ':';
            putDigits(buf[17..19], secs % 60);
            if (frac == 0) return w.writeAll(buf[0..19]);
            buf[19] = '.';
            putDigits(buf[20..26], frac);
            return w.writeAll(&buf);
        }
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
};

test "writeDate, writeTime and writeTimestamp print what the bodies they replaced did" {
    var rnd = std.Random.DefaultPrng.init(0xf0a7);
    const r = rnd.random();
    var a: [fmt_bound]u8 = undefined;
    var b: [fmt_bound]u8 = undefined;
    const edges = [_]i64{ std.math.minInt(i64), std.math.maxInt(i64), 0, -1, 1, std.math.minInt(i32), std.math.maxInt(i32), -719_528, -719_529, 2_932_896, 2_932_897 };
    for (0..300_000) |i| {
        const x: i64 = if (i < edges.len) edges[i] else switch (i % 4) {
            0 => r.int(i64),
            1 => r.int(i32),
            2 => r.intRangeAtMost(i64, -800_000, 3_000_000) * civil.us_per_day + r.intRangeAtMost(i64, 0, 3) * r.intRangeAtMost(i64, 0, civil.us_per_day - 1),
            else => r.intRangeAtMost(i64, -800_000, 3_000_000),
        };
        inline for (.{ .{ writeDate, Old.writeDate }, .{ writeTime, Old.writeTime }, .{ writeTimestamp, Old.writeTimestamp } }, 0..) |f, k| {
            const v = if (k == 0) std.math.clamp(x, std.math.minInt(i32), std.math.maxInt(i32)) else x;
            var fa = std.Io.Writer.fixed(&a);
            var fb = std.Io.Writer.fixed(&b);
            try f[0](&fa, v);
            try f[1](&fb, v);
            std.testing.expectEqualStrings(fb.buffered(), fa.buffered()) catch |e| {
                std.debug.print("writer {d} of {d}\n", .{ k, v });
                return e;
            };
        }
    }
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
    try std.testing.expectEqualStrings("-0768-02-04 -0768-02-04 00:00:00.000001|-0001-12-31|-0001-12-30", w.buffered());
}

test "the earliest and latest timestamps an INT holds print instead of trapping" {
    var buf: [128]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try writeTimestamp(&w, std.math.minInt(i64));
    try w.writeByte('|');
    try writeTimestamp(&w, std.math.maxInt(i64));
    try std.testing.expectEqualStrings("-290308-12-21 19:59:05.224192|294247-01-10 04:00:54.775807", w.buffered());
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
    try std.testing.expectError(error.CastFailed, evalLit(a, "make_date(9223372036854775807, 1, 1)"));
    try std.testing.expectError(error.CastFailed, evalLit(a, "make_date(-9223372036854775807 - 1, 1, 1)"));
    try std.testing.expectError(error.CastFailed, evalLit(a, "make_date(6000000, 1, 1)"));
    _ = try evalLit(a, "epoch(make_date(5000000, 1, 1))");
    _ = try evalLit(a, "date_trunc('day', make_date(-5000000, 1, 1))");
    _ = try evalLit(a, "date_diff('second', make_date(-5000000, 1, 1), make_date(5000000, 1, 1))");
    _ = try evalLit(a, "extract('year', make_date(-5000000, 1, 1))");

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
    try std.testing.expectEqual(@as(f64, 3.5), (try evalLit(a, "json_reduce('[1.5,2]', 0e0, (acc, x) -> acc + x)")).float);
    try std.testing.expectError(error.CastFailed, evalLit(a, "json_reduce('[1.5,2]', 0, (acc, x) -> acc + x)"));

    const d = try evalLit(a, "json_reduce('[\"0.10\",\"0.25\"]', CAST(0 AS DECIMAL(10,2)), (acc, x) -> acc + CAST(x AS DECIMAL(10,2)))");
    try std.testing.expectEqualStrings("0.35", try valueToString(a, d));
    try std.testing.expectEqualStrings("2026-01-04", try formatDate(a, (try evalLit(a, "json_reduce('[1,2]', CAST('2026-01-01' AS DATE), (dt, x) -> date_add('day', x, dt))")).date));

    try std.testing.expectEqual(@as(i64, 22), (try evalLit(a, "json_reduce('[1,2,3]', 0, (acc, x, i) -> acc + x * CAST(json_get('[5,4,3]', CAST(i AS STRING)) AS INT))")).int);
    try std.testing.expectEqualStrings("[10,21]", (try evalLit(a, "json_transform('[10,20]', (x, i) -> x + i)")).string);

    try std.testing.expect((try evalLit(a, "json_any('[[1,2],[3]]', x -> json_reduce(x, 0, (acc, x) -> acc + x) = 3)")).bool);
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
