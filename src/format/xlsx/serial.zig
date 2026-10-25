//! Excel serial numbers, read and written: days since the workbook's epoch
//! (1899-12-30, or 1904-01-01 in a 1904 workbook), the time of day the fraction,
//! kept to the millisecond. Excel's 1900 calendar counts a 29 February 1900 that
//! never was, so serial 1 is 1900-01-01 and 59 is 28 February, as Excel shows them;
//! 60, the missing day, reads as 28 February too, as openpyxl reads it, and only
//! dates from 1 March 1900 are written as serials. Also the ISO text a date-styled
//! cell shows when its column is text.

const std = @import("std");
const civil = @import("../../exec/civil.zig");

pub const day_ms: i64 = 86_400_000;

const epoch1900: i64 = -civil.epoch.excel1900;
const epoch1904: i64 = -civil.epoch.excel1904;

/// The first and last day written as a serial: 1900-03-01, past Excel's phantom
/// leap day, and 9999-12-31, Excel's last date.
pub const first_day: i64 = civil.daysFromCivil(1900, 3, 1);
pub const last_day: i64 = civil.daysFromCivil(9999, 12, 31);

/// Past Excel's last date a date-formatted number is read as a number; NaN and the
/// infinities fail the range test too.
pub const max_serial: f64 = @floatFromInt(last_day + epoch1900 + 1);

/// Days from the workbook's epoch to 1970-01-01, less the phantom 29 February 1900
/// for a serial before it.
fn epochOffset(serial: f64, date1904: bool) i64 {
    if (date1904) return epoch1904;
    return if (serial >= 1 and serial < 60) epoch1900 - 1 else epoch1900;
}

/// The date of `serial`, in `[0, max_serial)`, as days since 1970-01-01.
pub fn days(serial: f64, date1904: bool) i64 {
    return @as(i64, @intFromFloat(@floor(serial))) - epochOffset(serial, date1904);
}

/// `serial`, in `[0, max_serial)`, as a timestamp, to the millisecond: a day's
/// fraction in a double cannot say 23:59:59.5 exactly, and Excel itself keeps times
/// to the millisecond.
pub fn micros(serial: f64, date1904: bool) i64 {
    const ms: i64 = @intFromFloat(@round(serial * @as(f64, @floatFromInt(day_ms))));
    return (ms - epochOffset(serial, date1904) * day_ms) * 1000;
}

/// The time of day of `serial` to the millisecond, a fraction that rounds up to
/// midnight held at 23:59:59.999 so it stays inside its day.
pub fn timeMicros(serial: f64) i64 {
    const frac = serial - @floor(serial);
    const ms: i64 = @intFromFloat(@round(frac * @as(f64, @floatFromInt(day_ms))));
    return @min(ms, day_ms - 1) * 1000;
}

/// The serial a 1900 workbook stores date `d` as, null outside `first_day..last_day`.
pub fn ofDays(d: i64) ?i64 {
    if (d < first_day or d > last_day) return null;
    return d + epoch1900;
}

/// The serial of timestamp `us`, cut to the millisecond, null when its day is
/// outside `first_day..last_day`.
pub fn ofMicros(us: i64) ?f64 {
    const ms = @divFloor(us, 1000);
    const d = @divFloor(ms, day_ms);
    if (d < first_day or d > last_day) return null;
    return @as(f64, @floatFromInt(ms + epoch1900 * day_ms)) / @as(f64, @floatFromInt(day_ms));
}

/// The serial of time `us`, a fraction of a day, cut to the millisecond.
pub fn ofTime(us: i64) f64 {
    const ms = @mod(@divFloor(us, 1000), day_ms);
    return @as(f64, @floatFromInt(ms)) / @as(f64, @floatFromInt(day_ms));
}

pub const date_layout = civil.Layout.xlsx_date;
pub const time_layout = civil.Layout.xlsx_time;
pub const timestamp_layout: civil.Layout = .{ .parts = .timestamp, .year = .clamp_zero, .frac = .ms_nonzero };

/// `x` (days for a date, microseconds otherwise) as the text a cell shows: ISO, a
/// year before 0 as 0000, milliseconds only when not zero.
pub fn text(arena: std.mem.Allocator, x: i64, comptime layout: civil.Layout) ![]const u8 {
    var buf: [80]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    civil.writeLayout(&w, x, layout) catch unreachable;
    return arena.dupe(u8, w.buffered());
}

const testing = std.testing;

test "serial: the epochs and bounds equal the literals the reader and writer held" {
    try testing.expectEqual(@as(i64, 25569), epoch1900);
    try testing.expectEqual(@as(i64, 24107), epoch1904);
    try testing.expectEqual(@as(i64, 61 - 25569), first_day);
    try testing.expectEqual(@as(i64, 2958465 - 25569), last_day);
    try testing.expectEqual(@as(f64, 2958466.0), max_serial);
}

test "serial: 1900 serials before March read as the dates Excel shows" {
    const day = 86_400_000_000;
    try testing.expectEqual(@as(i64, -25567), days(1, false));
    try testing.expectEqual(@as(i64, -25509), days(59, false));
    try testing.expectEqual(@as(i64, -25509), days(60, false));
    try testing.expectEqual(@as(i64, -25508), days(61, false));
    try testing.expectEqual(@as(i64, -25509 * day + day / 2), micros(59.5, false));
    try testing.expectEqual(@as(i64, -24107), days(0, true));
    try testing.expectEqual(@as(i64, -24106), days(1, true));
    try testing.expectEqual(@as(i64, 86_399_999_000), timeMicros(0.9999999999));
    try testing.expectEqual(@as(i64, 86_399_999_000), timeMicros(45000.9999999999));
    try testing.expectEqual(@as(i64, 43_200_000_000), timeMicros(0.5));
}

test "serial: reading equals the reader's old arithmetic over the whole serial range" {
    const Orig = struct {
        fn oracleOffset(serial: f64, date1904: bool) i64 {
            if (date1904) return 24107;
            return if (serial >= 1 and serial < 60) 25568 else 25569;
        }
        fn oracleDays(serial: f64, date1904: bool) i64 {
            return @as(i64, @intFromFloat(@floor(serial))) - oracleOffset(serial, date1904);
        }
        fn oracleMicros(serial: f64, date1904: bool) i64 {
            const ms: i64 = @intFromFloat(@round(serial * 86_400_000.0));
            return (ms - oracleOffset(serial, date1904) * 86_400_000) * 1000;
        }
        fn oracleTimeMicros(serial: f64) i64 {
            const frac = serial - @floor(serial);
            const ms: i64 = @intFromFloat(@round(frac * 86_400_000.0));
            return @min(ms, 86_399_999) * 1000;
        }
    };
    var prng = std.Random.DefaultPrng.init(0x5e_71a1);
    const r = prng.random();
    const edges = [_]f64{ 0, 0.5, 1, 1.5, 59, 59.999999, 60, 60.5, 61, 25569, 25569.25, max_serial - 1e-6, max_serial - 1 };
    for (0..200_000 + edges.len) |i| {
        const s = if (i < edges.len) edges[i] else if (i % 3 == 0) @as(f64, @floatFromInt(r.uintLessThan(u32, 2958466))) else r.float(f64) * max_serial;
        for ([_]bool{ false, true }) |d1904| {
            try testing.expectEqual(Orig.oracleDays(s, d1904), days(s, d1904));
            try testing.expectEqual(Orig.oracleMicros(s, d1904), micros(s, d1904));
        }
        try testing.expectEqual(Orig.oracleTimeMicros(s), timeMicros(s));
    }
}

test "serial: writing equals the writer's old arithmetic, and what it writes reads back" {
    const Orig = struct {
        const oracle_epoch_days = 25569;
        const oracle_first_day = 61 - oracle_epoch_days;
        const oracle_last_day = 2958465 - oracle_epoch_days;
    };
    var prng = std.Random.DefaultPrng.init(0x5e_71a2);
    const r = prng.random();
    const edges = [_]i64{ first_day - 1, first_day, 0, -1, last_day, last_day + 1, std.math.minInt(i32), std.math.maxInt(i32) };
    for (0..200_000 + edges.len) |i| {
        const d: i64 = if (i < edges.len) edges[i] else r.intRangeAtMost(i64, first_day - 400, last_day + 400);
        const want: ?i64 = if (d >= Orig.oracle_first_day and d <= Orig.oracle_last_day) d + Orig.oracle_epoch_days else null;
        try testing.expectEqual(want, ofDays(d));
        if (ofDays(d)) |s| try testing.expectEqual(d, days(@floatFromInt(s), false));

        const us = if (i < edges.len) d *| 86_400_000_000 else r.intRangeAtMost(i64, (first_day - 2) * 86_400_000_000, (last_day + 2) * 86_400_000_000);
        const ms = @divFloor(us, 1000);
        const day_of = @divFloor(ms, 86_400_000);
        const want_ts: ?f64 = if (day_of >= Orig.oracle_first_day and day_of <= Orig.oracle_last_day) @as(f64, @floatFromInt(ms + Orig.oracle_epoch_days * 86_400_000)) / 86_400_000 else null;
        try testing.expectEqual(want_ts, ofMicros(us));
        if (ofMicros(us)) |s| try testing.expectEqual(ms * 1000, micros(s, false));

        const tms = @mod(@divFloor(us, 1000), 86_400_000);
        try testing.expectEqual(@as(f64, @floatFromInt(tms)) / 86_400_000, ofTime(us));
        try testing.expectEqual(tms * 1000, timeMicros(ofTime(us)));
    }
}

test "serial: cell text equals the reader's old formatters" {
    const Orig = struct {
        fn oracleDate(arena: std.mem.Allocator, d: i64) ![]const u8 {
            const c = civil.civilFromDays(d);
            return std.fmt.allocPrint(arena, "{d:0>4}-{d:0>2}-{d:0>2}", .{ @as(u64, @intCast(@max(c.y, 0))), c.m, c.d });
        }
        fn oracleTime(arena: std.mem.Allocator, us: i64) ![]const u8 {
            const t: u64 = @intCast(@max(us, 0));
            const s = t / 1_000_000;
            const ms = (t % 1_000_000) / 1000;
            if (ms == 0) return std.fmt.allocPrint(arena, "{d:0>2}:{d:0>2}:{d:0>2}", .{ s / 3600, s / 60 % 60, s % 60 });
            return std.fmt.allocPrint(arena, "{d:0>2}:{d:0>2}:{d:0>2}.{d:0>3}", .{ s / 3600, s / 60 % 60, s % 60, ms });
        }
        fn oracleTimestamp(arena: std.mem.Allocator, us: i64) ![]const u8 {
            const d = @divFloor(us, 86_400_000_000);
            return std.fmt.allocPrint(arena, "{s} {s}", .{ try oracleDate(arena, d), try oracleTime(arena, us - d * 86_400_000_000) });
        }
    };
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var prng = std.Random.DefaultPrng.init(0x5e_71a3);
    const r = prng.random();
    const day_edges = [_]i64{ 0, -1, -719528, -719529, -800000, last_day, last_day + 1, 3_000_000 };
    for (day_edges) |d| try testing.expectEqualStrings(try Orig.oracleDate(a, d), try text(a, d, date_layout));
    for (0..50_000) |_| {
        _ = ar.reset(.retain_capacity);
        const d = r.intRangeAtMost(i64, -1_000_000, 3_500_000);
        try testing.expectEqualStrings(try Orig.oracleDate(a, d), try text(a, d, date_layout));
        const t = r.intRangeLessThan(i64, 0, 86_400_000_000);
        const tm = if (r.boolean()) @divFloor(t, 1000) * 1000 else t;
        try testing.expectEqualStrings(try Orig.oracleTime(a, tm), try text(a, tm, time_layout));
        const us = r.intRangeAtMost(i64, -800_000 * 86_400_000_000, 3_500_000 * 86_400_000_000);
        try testing.expectEqualStrings(try Orig.oracleTimestamp(a, us), try text(a, us, timestamp_layout));
    }
}
