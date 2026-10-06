//! Date and time arithmetic: truncation, extraction, adding units, differences,
//! strftime/strptime and ISO 8601 parsing, on microseconds since the epoch.

const EvalError = @import("../eval.zig").EvalError;
const Value = @import("../value.zig").Value;
const std = @import("std");
const trim = @import("support.zig").trim;

pub const TimeUnit = enum { year, month, week, day, hour, minute, second };
const formatTimestamp = @import("format.zig").formatTimestamp;

fn isoWeekStart(day: i64) i64 {
    return day - @mod(day + 3, 7);
}

pub fn timeUnit(name: []const u8) ?TimeUnit {
    var buf: [16]u8 = undefined;
    if (name.len == 0 or name.len >= buf.len) return null;
    return std.meta.stringToEnum(TimeUnit, std.ascii.lowerString(buf[0..name.len], name));
}

pub fn temporalMicros(v: Value) ?i64 {
    return switch (v) {
        .timestamp => |x| x,
        .date => |x| @as(i64, x) * 86_400_000_000,
        else => null,
    };
}

pub fn truncMicros(us: i64, u: TimeUnit) i64 {
    const day = @divFloor(us, 86_400_000_000);
    const rem = us - day * 86_400_000_000;
    return switch (u) {
        .second => us - @mod(rem, 1_000_000),
        .minute => us - @mod(rem, 60_000_000),
        .hour => us - @mod(rem, 3_600_000_000),
        .day => day * 86_400_000_000,
        .week => isoWeekStart(day) * 86_400_000_000,
        .month => blk: {
            const c = civilFromDays(day);
            break :blk daysFromCivil(c.y, c.m, 1) * 86_400_000_000;
        },
        .year => blk: {
            const c = civilFromDays(day);
            break :blk daysFromCivil(c.y, 1, 1) * 86_400_000_000;
        },
    };
}

pub fn extractField(us: i64, u: TimeUnit) i64 {
    const day = @divFloor(us, 86_400_000_000);
    const rem = us - day * 86_400_000_000;
    const c = civilFromDays(day);
    return switch (u) {
        .year => c.y,
        .month => @intCast(c.m),
        .week => blk: {
            const thu = isoWeekStart(day) + 3;
            const ty = civilFromDays(thu).y;
            break :blk @divFloor(thu - daysFromCivil(ty, 1, 1), 7) + 1;
        },
        .day => @intCast(c.d),
        .hour => @divFloor(rem, 3_600_000_000),
        .minute => @mod(@divFloor(rem, 60_000_000), 60),
        .second => @mod(@divFloor(rem, 1_000_000), 60),
    };
}

pub fn mulI64(a: i64, b: i64) EvalError!i64 {
    return std.math.mul(i64, a, b) catch return error.CastFailed;
}

fn addI64(a: i64, b: i64) EvalError!i64 {
    return std.math.add(i64, a, b) catch return error.CastFailed;
}

fn isLeapYear(y: i64) bool {
    return @mod(y, 4) == 0 and (@mod(y, 100) != 0 or @mod(y, 400) == 0);
}

pub fn daysInMonth(y: i64, m: u32) u32 {
    const lens = [_]u32{ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    if (m == 2 and isLeapYear(y)) return 29;
    return lens[m - 1];
}

/// Adds `n` calendar months, clamping the day to the target month's length;
/// the clamp is not remembered (Jan 31 +1 is Feb 29, +2 is Mar 29), as Postgres does.
fn addMonthsToDays(days: i64, n: i64) i64 {
    const c = civilFromDays(days);
    const total = c.y * 12 + @as(i64, c.m) - 1 + n;
    const y = @divFloor(total, 12);
    const m: u32 = @intCast(@mod(total, 12) + 1);
    return daysFromCivil(y, m, @min(c.d, daysInMonth(y, m)));
}

pub fn addUnits(v: Value, u: TimeUnit, n: i64) EvalError!Value {
    switch (v) {
        .date => |d0| {
            const days: i64 = d0;
            const nd = switch (u) {
                .year => addMonthsToDays(days, try mulI64(n, 12)),
                .month => addMonthsToDays(days, n),
                .week => try addI64(days, try mulI64(n, 7)),
                .day => try addI64(days, n),
                .hour, .minute, .second => return error.TypeMismatch,
            };
            return .{ .date = std.math.cast(i32, nd) orelse return error.CastFailed };
        },
        .timestamp => |us| {
            const out: i64 = switch (u) {
                .year, .month => blk: {
                    const day = @divFloor(us, 86_400_000_000);
                    const rem = us - day * 86_400_000_000;
                    const months = if (u == .year) try mulI64(n, 12) else n;
                    const shifted = try mulI64(addMonthsToDays(day, months), 86_400_000_000);
                    break :blk try addI64(shifted, rem);
                },
                .week => try addI64(us, try mulI64(n, 7 * 86_400_000_000)),
                .day => try addI64(us, try mulI64(n, 86_400_000_000)),
                .hour => try addI64(us, try mulI64(n, 3_600_000_000)),
                .minute => try addI64(us, try mulI64(n, 60_000_000)),
                .second => try addI64(us, try mulI64(n, 1_000_000)),
            };
            return .{ .timestamp = out };
        },
        else => return error.TypeMismatch,
    }
}

/// DuckDB semantics: `year`/`month`/`week` count unit boundaries crossed, so
/// 2023-12-31 to 2024-01-01 is one year; `day` and finer divide the elapsed time and truncate.
pub fn dateDiff(a_us: i64, b_us: i64, u: TimeUnit) i64 {
    switch (u) {
        .year, .month => {
            const ca = civilFromDays(@divFloor(a_us, 86_400_000_000));
            const cb = civilFromDays(@divFloor(b_us, 86_400_000_000));
            if (u == .year) return cb.y - ca.y;
            return (cb.y * 12 + @as(i64, cb.m)) - (ca.y * 12 + @as(i64, ca.m));
        },
        .week => {
            const wa = isoWeekStart(@divFloor(a_us, 86_400_000_000));
            const wb = isoWeekStart(@divFloor(b_us, 86_400_000_000));
            return @divExact(wb - wa, 7);
        },
        .day => return @divTrunc(b_us - a_us, 86_400_000_000),
        .hour => return @divTrunc(b_us - a_us, 3_600_000_000),
        .minute => return @divTrunc(b_us - a_us, 60_000_000),
        .second => return @divTrunc(b_us - a_us, 1_000_000),
    }
}

pub fn badStrftime(fmt: []const u8) ?[]const u8 {
    var i: usize = 0;
    while (i < fmt.len) : (i += 1) {
        if (fmt[i] != '%') continue;
        i += 1;
        if (i >= fmt.len) return fmt[fmt.len - 1 ..];
        switch (fmt[i]) {
            'Y', 'y', 'm', 'd', 'H', 'M', 'S', '%' => {},
            else => return fmt[i .. i + 1],
        }
    }
    return null;
}

/// `strftime` over exactly `%Y %m %d %H %M %S %y %%`; any other directive is an
/// error, never a silent passthrough.
pub fn strftimeFmt(arena: std.mem.Allocator, us: i64, fmt: []const u8) EvalError![]const u8 {
    const day = @divFloor(us, 86_400_000_000);
    const rem: u64 = @intCast(us - day * 86_400_000_000);
    const secs = rem / 1_000_000;
    const c = civilFromDays(day);
    const year: u32 = if (c.y < 0) 0 else @intCast(c.y);
    var out = std.array_list.Managed(u8).init(arena);
    const w = out.writer();
    var i: usize = 0;
    while (i < fmt.len) : (i += 1) {
        if (fmt[i] != '%') {
            try out.append(fmt[i]);
            continue;
        }
        i += 1;
        if (i >= fmt.len) return error.CastFailed;
        switch (fmt[i]) {
            'Y' => try w.print("{d:0>4}", .{year}),
            'y' => try w.print("{d:0>2}", .{year % 100}),
            'm' => try w.print("{d:0>2}", .{c.m}),
            'd' => try w.print("{d:0>2}", .{c.d}),
            'H' => try w.print("{d:0>2}", .{secs / 3600}),
            'M' => try w.print("{d:0>2}", .{(secs % 3600) / 60}),
            'S' => try w.print("{d:0>2}", .{secs % 60}),
            '%' => try out.append('%'),
            else => return error.CastFailed,
        }
    }
    return try out.toOwnedSlice();
}

/// Numbers take up to their width in digits, `%y` pivots as POSIX does (69-99 are
/// the 1900s), and the whole text must be consumed. Null for a date that does not exist.
pub fn strptimeFmt(text: []const u8, fmt: []const u8) ?i64 {
    var y: i64 = 1970;
    var mo: u32 = 1;
    var d: u32 = 1;
    var h: i64 = 0;
    var mi: i64 = 0;
    var sec: i64 = 0;
    var t: usize = 0;
    var i: usize = 0;
    while (i < fmt.len) : (i += 1) {
        if (fmt[i] != '%' or (i + 1 < fmt.len and fmt[i + 1] == '%')) {
            if (fmt[i] == '%') i += 1;
            if (t >= text.len or text[t] != fmt[i]) return null;
            t += 1;
            continue;
        }
        i += 1;
        if (i >= fmt.len) return null;
        const width: usize = if (fmt[i] == 'Y') 4 else 2;
        const start = t;
        var n: i64 = 0;
        while (t < text.len and t - start < width and std.ascii.isDigit(text[t])) : (t += 1) n = n * 10 + (text[t] - '0');
        if (t == start) return null;
        switch (fmt[i]) {
            'Y' => y = n,
            'y' => y = if (n >= 69) 1900 + n else 2000 + n,
            'm' => mo = std.math.cast(u32, n) orelse return null,
            'd' => d = std.math.cast(u32, n) orelse return null,
            'H' => h = n,
            'M' => mi = n,
            'S' => sec = n,
            else => return null,
        }
    }
    if (t != text.len) return null;
    if (mo < 1 or mo > 12 or d < 1 or d > daysInMonth(y, mo) or h > 23 or mi > 59 or sec > 59) return null;
    return (daysFromCivil(y, mo, d) * 86_400 + h * 3600 + mi * 60 + sec) * 1_000_000;
}

pub fn daysFromCivil(y0: i64, m: u32, d: u32) i64 {
    const y = if (m <= 2) y0 - 1 else y0;
    const era = @divFloor(if (y >= 0) y else y - 399, 400);
    const yoe = y - era * 400;
    const mp: i64 = @intCast((m + 9) % 12);
    const doy = @divFloor(153 * mp + 2, 5) + @as(i64, @intCast(d)) - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146097 + doe - 719468;
}

fn isoNum(s: []const u8) ?i64 {
    var v: i64 = 0;
    for (s) |c| {
        if (c < '0' or c > '9') return null;
        v = v * 10 + @as(i64, c - '0');
    }
    return v;
}

/// Strict on purpose: a literal that is not an ISO date must fail, so
/// `date_col = '01/07/2013'` is an error, not a silently wrong comparison.
pub fn parseIsoDate(s0: []const u8) ?i64 {
    const s = trim(s0);
    if (s.len != 10 or s[4] != '-' or s[7] != '-') return null;
    const y = isoNum(s[0..4]) orelse return null;
    const m = isoNum(s[5..7]) orelse return null;
    const d = isoNum(s[8..10]) orelse return null;
    if (m < 1 or m > 12 or d < 1 or d > 31) return null;
    return daysFromCivil(y, @intCast(m), @intCast(d));
}

/// `HH:MM:SS[.ffffff]` or `HH:MM` as microseconds since midnight, the text a
/// `time` prints as, using the timestamp parser's rules on a fixed date.
pub fn parseIsoTime(s0: []const u8) ?i64 {
    const s = trim(s0);
    if (s.len == 5 and s[2] == ':') {
        const hh = isoNum(s[0..2]) orelse return null;
        const mm = isoNum(s[3..5]) orelse return null;
        if (hh > 23 or mm > 59) return null;
        return (hh * 3600 + mm * 60) * 1_000_000;
    }
    if (s.len < 8 or s.len > 8 + 7) return null;
    var buf: [32]u8 = undefined;
    const ts = std.fmt.bufPrint(&buf, "1970-01-01 {s}", .{s}) catch return null;
    return parseIsoTimestamp(ts);
}

/// `YYYY-MM-DD[ HH:MM:SS[.ffffff]]` as microseconds since the epoch. The fraction
/// is kept: dropping it once made sub-second rows identical under DISTINCT.
pub fn parseIsoTimestamp(s0: []const u8) ?i64 {
    const s = trim(s0);
    if (s.len == 10) return (parseIsoDate(s) orelse return null) * 86_400_000_000;
    if (s.len < 19 or s[13] != ':' or s[16] != ':') return null;
    const days = parseIsoDate(s[0..10]) orelse return null;
    const hh = isoNum(s[11..13]) orelse return null;
    const mm = isoNum(s[14..16]) orelse return null;
    const ss = isoNum(s[17..19]) orelse return null;
    if (hh > 23 or mm > 59 or ss > 59) return null;
    var frac: i64 = 0;
    if (s.len > 20 and s[19] == '.') {
        var i: usize = 20;
        var scale: i64 = 100_000;
        while (i < s.len and scale > 0 and s[i] >= '0' and s[i] <= '9') : (i += 1) {
            frac += @as(i64, s[i] - '0') * scale;
            scale = @divTrunc(scale, 10);
        }
        if (i != s.len) return null;
    } else if (s.len != 19) return null;
    return days * 86_400_000_000 + (hh * 3600 + mm * 60 + ss) * 1_000_000 + frac;
}

pub fn civilFromDays(z0: i64) struct { y: i64, m: u32, d: u32 } {
    const z = z0 + 719468;
    const era = @divFloor(if (z >= 0) z else z - 146096, 146097);
    const doe = z - era * 146097;
    const yoe = @divFloor(doe - @divFloor(doe, 1460) + @divFloor(doe, 36524) - @divFloor(doe, 146096), 365);
    const y = yoe + era * 400;
    const doy = doe - (365 * yoe + @divFloor(yoe, 4) - @divFloor(yoe, 100));
    const mp = @divFloor(5 * doy + 2, 153);
    const d: u32 = @intCast(doy - @divFloor(153 * mp + 2, 5) + 1);
    const m: u32 = @intCast(if (mp < 10) mp + 3 else mp - 9);
    return .{ .y = y + (if (m <= 2) @as(i64, 1) else 0), .m = m, .d = d };
}

test "parseIsoTime: the text a time prints as, and nothing out of range" {
    try std.testing.expectEqual(@as(?i64, 3_723_000_000), parseIsoTime("01:02:03"));
    try std.testing.expectEqual(@as(?i64, 86_399_999_999), parseIsoTime("23:59:59.999999"));
    try std.testing.expectEqual(@as(?i64, 45_000_000_000), parseIsoTime(" 12:30 "));
    try std.testing.expectEqual(@as(?i64, null), parseIsoTime("24:00:00"));
    try std.testing.expectEqual(@as(?i64, null), parseIsoTime("1:02:03"));
    try std.testing.expectEqual(@as(?i64, null), parseIsoTime("01:02:03 extra"));
}

test "timestamps keep sub-second precision through parse and format" {
    const us = parseIsoTimestamp("2026-08-08 12:34:56.123456").?;
    try std.testing.expectEqual(@as(i64, 123456), @mod(us, 1_000_000));
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    try std.testing.expectEqualStrings(
        "2026-08-08 12:34:56.123456",
        try formatTimestamp(ar.allocator(), us),
    );
    try std.testing.expectEqual(@as(i64, 100000), @mod(parseIsoTimestamp("2026-08-08 12:34:56.1").?, 1_000_000));
    const w = parseIsoTimestamp("2026-08-08 12:34:56").?;
    try std.testing.expectEqualStrings("2026-08-08 12:34:56", try formatTimestamp(ar.allocator(), w));
    try std.testing.expect(parseIsoTimestamp("2026-08-08 12:34:56.12x") == null);
}
