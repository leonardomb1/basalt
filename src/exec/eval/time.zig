//! Date and time arithmetic: truncation, extraction, adding units, differences,
//! strftime/strptime and ISO 8601 parsing, on microseconds since the epoch. The
//! calendar and the text parsers are `civil.zig`'s; this file names them for SQL.

const EvalError = @import("../eval.zig").EvalError;
const Value = @import("../value.zig").Value;
const std = @import("std");
const civil = @import("../civil.zig");

pub const TimeUnit = civil.Part;
const formatTimestamp = @import("format.zig").formatTimestamp;

pub const daysFromCivil = civil.daysFromCivil;
pub const civilFromDays = civil.civilFromDays;
pub const daysInMonth = civil.daysInMonth;

const us_per_day = civil.us_per_day;

fn isoWeekStart(day: i64) i64 {
    return day - @mod(day + 3, 7);
}

/// The unit `name` spells, singular or plural, ignoring case.
pub fn timeUnit(name: []const u8) ?TimeUnit {
    return TimeUnit.parse(name);
}

/// A DATE as its midnight's microseconds, saturating for one past about ±290,000
/// years that no timestamp holds; a TIMESTAMP as itself.
pub fn temporalMicros(v: Value) ?i64 {
    return switch (v) {
        .timestamp => |x| x,
        .date => |x| civil.dayMicros(x),
        else => null,
    };
}

pub fn truncMicros(us: i64, u: TimeUnit) i64 {
    const day = civil.dayOf(us);
    const rem = civil.timeOfDay(us);
    return switch (u) {
        inline .second, .minute, .hour => |p| us - @mod(rem, comptime p.fixedMicros().?),
        .day => civil.dayMicros(day),
        .week => civil.dayMicros(isoWeekStart(day)),
        .month => blk: {
            const c = civilFromDays(day);
            break :blk civil.dayMicros(daysFromCivil(c.y, c.m, 1));
        },
        .year => blk: {
            const c = civilFromDays(day);
            break :blk civil.dayMicros(daysFromCivil(c.y, 1, 1));
        },
    };
}

pub fn extractField(us: i64, u: TimeUnit) i64 {
    const day = civil.dayOf(us);
    const f = civil.fieldsOfMicros(us);
    return switch (u) {
        .year => f.y,
        .month => f.m,
        .week => blk: {
            const thu = isoWeekStart(day) + 3;
            const ty = civilFromDays(thu).y;
            break :blk @divFloor(thu - daysFromCivil(ty, 1, 1), 7) + 1;
        },
        .day => f.d,
        .hour => f.h,
        .minute => f.mi,
        .second => f.s,
    };
}

pub fn mulI64(a: i64, b: i64) EvalError!i64 {
    return std.math.mul(i64, a, b) catch return error.CastFailed;
}

fn addI64(a: i64, b: i64) EvalError!i64 {
    return std.math.add(i64, a, b) catch return error.CastFailed;
}

/// Adds `n` calendar months, clamping the day to the target month's length;
/// the clamp is not remembered (Jan 31 +1 is Feb 29, +2 is Mar 29), as Postgres does.
/// A year past ±6,000,000, which neither a DATE nor a TIMESTAMP holds, is a CastFailed.
fn addMonthsToDays(days: i64, n: i64) EvalError!i64 {
    const c = civilFromDays(days);
    const total = try addI64(c.y * 12 + @as(i64, c.m) - 1, n);
    const y = @divFloor(total, 12);
    if (y < -6_000_000 or y > 6_000_000) return error.CastFailed;
    const m: u32 = @intCast(@mod(total, 12) + 1);
    return daysFromCivil(y, m, @min(c.d, daysInMonth(y, m)));
}

/// `n` units after the date `d0`; a unit finer than a day is a TypeMismatch.
pub fn addDays(d0: i32, u: TimeUnit, n: i64) EvalError!i32 {
    const days: i64 = d0;
    const nd = switch (u) {
        .year => try addMonthsToDays(days, try mulI64(n, 12)),
        .month => try addMonthsToDays(days, n),
        .week => try addI64(days, try mulI64(n, 7)),
        .day => try addI64(days, n),
        .hour, .minute, .second => return error.TypeMismatch,
    };
    return std.math.cast(i32, nd) orelse error.CastFailed;
}

/// `n` units after the timestamp `us`.
pub fn addMicros(us: i64, u: TimeUnit, n: i64) EvalError!i64 {
    return switch (u) {
        .year, .month => blk: {
            const months = if (u == .year) try mulI64(n, 12) else n;
            const shifted = try mulI64(try addMonthsToDays(civil.dayOf(us), months), us_per_day);
            break :blk try addI64(shifted, civil.timeOfDay(us));
        },
        inline .week, .day, .hour, .minute, .second => |p| try addI64(us, try mulI64(n, comptime p.fixedMicros().?)),
    };
}

/// DuckDB semantics: `year`/`month`/`week` count unit boundaries crossed, so
/// 2023-12-31 to 2024-01-01 is one year; `day` and finer divide the elapsed time and truncate.
pub fn dateDiff(a_us: i64, b_us: i64, u: TimeUnit) i64 {
    switch (u) {
        .year, .month => {
            const ca = civilFromDays(civil.dayOf(a_us));
            const cb = civilFromDays(civil.dayOf(b_us));
            if (u == .year) return cb.y - ca.y;
            return (cb.y * 12 + @as(i64, cb.m)) - (ca.y * 12 + @as(i64, ca.m));
        },
        .week => {
            const wa = isoWeekStart(civil.dayOf(a_us));
            const wb = isoWeekStart(civil.dayOf(b_us));
            return @divExact(wb - wa, 7);
        },
        inline .day, .hour, .minute, .second => |p| return @intCast(@divTrunc(@as(i128, b_us) - a_us, comptime p.fixedMicros().?)),
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

/// `strftime` over exactly `%Y %m %d %H %M %S %y %%`, appended to `out`; any other
/// directive is an error, never a silent passthrough. A year before 0 prints as 0.
pub fn strftimeInto(out: *std.array_list.Managed(u8), us: i64, fmt: []const u8) EvalError!void {
    const f = civil.fieldsOfMicros(us);
    const year: u32 = if (f.y < 0) 0 else @intCast(f.y);
    var i: usize = 0;
    while (i < fmt.len) : (i += 1) {
        if (fmt[i] != '%') {
            try out.append(fmt[i]);
            continue;
        }
        i += 1;
        if (i >= fmt.len) return error.CastFailed;
        switch (fmt[i]) {
            'Y' => try padded(out, year, 4),
            'y' => try padded(out, year % 100, 2),
            'm' => try padded(out, f.m, 2),
            'd' => try padded(out, f.d, 2),
            'H' => try padded(out, f.h, 2),
            'M' => try padded(out, f.mi, 2),
            'S' => try padded(out, f.s, 2),
            '%' => try out.append('%'),
            else => return error.CastFailed,
        }
    }
}

/// `x` in decimal, zero-padded to at least `width` digits.
fn padded(out: *std.array_list.Managed(u8), x: u64, width: usize) !void {
    var b: [20]u8 = undefined;
    var i: usize = b.len;
    var v = x;
    while (true) {
        i -= 1;
        b[i] = @intCast('0' + v % 10);
        v /= 10;
        if (v == 0) break;
    }
    while (b.len - i < width) {
        i -= 1;
        b[i] = '0';
    }
    try out.appendSlice(b[i..]);
}

/// Numbers take up to their width in digits, `%y` pivots as POSIX does (69-99 are
/// the 1900s), and the whole text must be consumed. Null for a date that does not exist.
pub fn strptimeFmt(text: []const u8, fmt: []const u8) ?i64 {
    var y: i64 = 1970;
    var mo: i64 = 1;
    var d: i64 = 1;
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
            'y' => y = civil.pivotYY(n),
            'm' => mo = n,
            'd' => d = n,
            'H' => h = n,
            'M' => mi = n,
            'S' => sec = n,
            else => return null,
        }
    }
    if (t != text.len) return null;
    const days = civil.civilDays(y, mo, d) orelse return null;
    return days * us_per_day + (civil.clockMicros(h, mi, sec, 0) orelse return null);
}

const Iso = civil.Text(.iso);
const PgText = civil.Text(.pg_text);

/// Strict on purpose: a literal that is not an ISO date must fail, so
/// `date_col = '01/07/2013'` is an error, not a silently wrong comparison.
pub const parseIsoDate = Iso.date;

/// `HH:MM:SS[.ffffff]` or `HH:MM` as microseconds since midnight, the text a
/// `time` prints as.
pub const parseIsoTime = Iso.time;

/// `YYYY-MM-DD[ HH:MM:SS[.ffffff]]` as microseconds since the epoch. The fraction
/// is kept: dropping it once made sub-second rows identical under DISTINCT.
pub const parseIsoTimestamp = Iso.timestamp;

/// A quoted literal or CAST read as a date, as PostgreSQL reads one: one- or
/// two-digit month and day, and a time part (`T` or space) that is dropped.
pub const parseDateText = PgText.date;

/// A quoted literal or CAST read as a timestamp, as PostgreSQL reads one: the
/// date as `parseDateText`, `T` or space, `H:MM[:SS[.f]]`, and a zone (`Z`,
/// `±HH`, `±HH:MM`, `±HHMM`) folded into UTC, the zone basalt's timestamps keep.
pub const parseTimestampText = PgText.timestamp;

test "parseDateText and parseTimestampText read text as PostgreSQL does" {
    const day = daysFromCivil(2024, 1, 5);
    try std.testing.expectEqual(day, parseDateText("2024-01-05").?);
    try std.testing.expectEqual(day, parseDateText("2024-1-5").?);
    try std.testing.expectEqual(day, parseDateText("2024-01-05 12:00:00").?);
    try std.testing.expectEqual(day, parseDateText("2024-01-05T00:00:00Z").?);
    try std.testing.expect(parseDateText("01/05/2024") == null);
    try std.testing.expect(parseDateText("2024-02-30") == null);
    try std.testing.expect(parseDateText("2024-01-05 25:00") == null);

    const noon = day * 86_400_000_000 + 12 * 3_600_000_000;
    try std.testing.expectEqual(noon, parseTimestampText("2024-01-05 12:00:00").?);
    try std.testing.expectEqual(noon, parseTimestampText("2024-1-5T12:00").?);
    try std.testing.expectEqual(noon, parseTimestampText("2024-01-05 12:00:00Z").?);
    try std.testing.expectEqual(noon, parseTimestampText("2024-01-05 17:00:00+05:00").?);
    try std.testing.expectEqual(noon, parseTimestampText("2024-01-05 09:30-0230").?);
    try std.testing.expectEqual(noon, parseTimestampText("2024-01-05 14:00+02").?);
    try std.testing.expectEqual(noon + 250_000, parseTimestampText("2024-01-05 12:00:00.25").?);
    try std.testing.expectEqual(day * 86_400_000_000, parseTimestampText("2024-01-05").?);
    try std.testing.expect(parseTimestampText("2024-01-05 12") == null);
    try std.testing.expect(parseTimestampText("2024-01-05 12:00:00.") == null);
    try std.testing.expect(parseTimestampText("2024-01-05 12:00 UTC") == null);
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

test "an impossible day of the month is rejected, not rolled into the next month" {
    try std.testing.expect(parseIsoDate("2023-02-29") == null);
    try std.testing.expect(parseIsoDate("2024-04-31") == null);
    try std.testing.expect(parseIsoTimestamp("2024-02-30 10:00:00") == null);
    try std.testing.expect(parseIsoDate("2024-02-29") != null);
    try std.testing.expect(parseIsoDate("2000-02-29") != null);
    try std.testing.expect(parseIsoDate("1900-02-29") == null);
}

test "kept until decided: text readers refuse 5-digit and signed years (D3)" {
    try std.testing.expectEqual(@as(?i64, null), parseDateText("11000-01-01"));
    try std.testing.expectEqual(@as(?i64, null), parseDateText("-0001-01-01"));
    try std.testing.expectEqual(@as(?i64, null), parseTimestampText("11000-01-01 00:00:00"));
    try std.testing.expectEqual(@as(?i64, null), parseIsoDate("-0001-01-01"));
}

test "a TIME's digits past the microsecond are dropped, as a TIMESTAMP's are (D4)" {
    try std.testing.expectEqual(@as(?i64, 43_200_123_456), parseIsoTime("12:00:00.1234567"));
    try std.testing.expectEqual(@as(?i64, 43_200_123_456), parseTimestampText("1970-01-01 12:00:00.1234567"));
    try std.testing.expectEqual(@as(?i64, 43_200_123_456), parseIsoTimestamp("1970-01-01 12:00:00.123456789"));
    try std.testing.expectEqual(@as(?i64, 86_399_999_999), parseIsoTimestamp("1970-01-01 23:59:59.9999999"));
    try std.testing.expectEqual(@as(?i64, null), parseIsoTimestamp("1970-01-01 12:00:00.1234567x"));
    try std.testing.expectEqual(@as(?i64, 86_399_999_999), parseIsoTime("23:59:59.999999999"));
    try std.testing.expectEqual(@as(?i64, null), parseIsoTime("12:00:00.1234567x"));
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const evalLit = @import("testing_util.zig").evalLit;
    try std.testing.expectEqual(@as(i64, 43_200_123_456), (try evalLit(a, "CAST('12:00:00.123456789' AS TIME)")).time);
    try std.testing.expectEqual(@as(i64, 43_200_123_456), (try evalLit(a, "CAST(CAST('1970-01-01 12:00:00.123456789' AS TIMESTAMP) AS TIME)")).time);
}

test "a unit reads singular or plural in date_trunc, extract, date_add and date_diff (D11)" {
    try std.testing.expectEqual(@as(?TimeUnit, .hour), timeUnit("HOUR"));
    try std.testing.expectEqual(@as(?TimeUnit, .hour), timeUnit("hours"));
    try std.testing.expectEqual(@as(?TimeUnit, .day), timeUnit("Days"));
    try std.testing.expectEqual(@as(?TimeUnit, null), timeUnit("hourss"));
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const evalLit = @import("testing_util.zig").evalLit;
    const ts = "CAST('2024-03-15 10:20:30' AS TIMESTAMP)";
    try std.testing.expectEqual(try evalLit(a, "date_trunc('hour', " ++ ts ++ ")"), try evalLit(a, "date_trunc('HOURS', " ++ ts ++ ")"));
    try std.testing.expectEqual(@as(i64, 20), (try evalLit(a, "extract(minutes FROM " ++ ts ++ ")")).int);
    try std.testing.expectEqual(try evalLit(a, "date_add('day', 3, " ++ ts ++ ")"), try evalLit(a, "date_add('days', 3, " ++ ts ++ ")"));
    try std.testing.expectEqual(@as(i64, 2), (try evalLit(a, "date_diff('weeks', CAST('2024-03-04' AS DATE), CAST('2024-03-18' AS DATE))")).int);
    try std.testing.expectEqual(@as(i32, 19828), (try evalLit(a, "date_add('months', 1, CAST('2024-03-15' AS DATE))")).date);
}

test "kept until decided (D14): strftime prints a year before 0 as 0000" {
    var out = std.array_list.Managed(u8).init(std.testing.allocator);
    defer out.deinit();
    try strftimeInto(&out, civil.dayMicros(daysFromCivil(-1, 6, 1)), "%Y-%m-%d|%y");
    try std.testing.expectEqualStrings("0000-06-01|00", out.items);
}

test "civil.Text agrees with the parsers it replaced on 300k spellings; past i64 the wire now refuses, and a TIME or ISO timestamp cuts past the microsecond (D4)" {
    const Old = struct {
        fn trim(s: []const u8) []const u8 {
            return std.mem.trim(u8, s, " \t\r\n");
        }

        fn isoNum(s: []const u8) ?i64 {
            var v: i64 = 0;
            for (s) |c| {
                if (c < '0' or c > '9') return null;
                v = v * 10 + @as(i64, c - '0');
            }
            return v;
        }

        fn isoDate(s0: []const u8) ?i64 {
            const s = trim(s0);
            if (s.len != 10 or s[4] != '-' or s[7] != '-') return null;
            const y = isoNum(s[0..4]) orelse return null;
            const m = isoNum(s[5..7]) orelse return null;
            const d = isoNum(s[8..10]) orelse return null;
            if (m < 1 or m > 12 or d < 1 or d > daysInMonth(y, @intCast(m))) return null;
            return daysFromCivil(y, @intCast(m), @intCast(d));
        }

        fn isoTime(s0: []const u8) ?i64 {
            const s = trim(s0);
            if (s.len == 5 and s[2] == ':') {
                const hh = isoNum(s[0..2]) orelse return null;
                const mm = isoNum(s[3..5]) orelse return null;
                if (hh > 23 or mm > 59) return null;
                return (hh * 3600 + mm * 60) * 1_000_000;
            }
            if (s.len < 8) return null;
            var buf: [64]u8 = undefined;
            const ts = std.fmt.bufPrint(&buf, "1970-01-01 {s}", .{s}) catch return null;
            return isoTimestamp(ts);
        }

        fn isoTimestamp(s0: []const u8) ?i64 {
            const s = trim(s0);
            if (s.len == 10) return (isoDate(s) orelse return null) * 86_400_000_000;
            if (s.len < 19 or s[13] != ':' or s[16] != ':') return null;
            const days = isoDate(s[0..10]) orelse return null;
            const hh = isoNum(s[11..13]) orelse return null;
            const mm = isoNum(s[14..16]) orelse return null;
            const ss = isoNum(s[17..19]) orelse return null;
            if (hh > 23 or mm > 59 or ss > 59) return null;
            var frac: i64 = 0;
            if (s.len > 20 and s[19] == '.') {
                var i: usize = 20;
                var scale: i64 = 100_000;
                while (i < s.len and s[i] >= '0' and s[i] <= '9') : (i += 1) {
                    frac += @as(i64, s[i] - '0') * scale;
                    scale = @divTrunc(scale, 10);
                }
                if (i != s.len) return null;
            } else if (s.len != 19) return null;
            return days * 86_400_000_000 + (hh * 3600 + mm * 60 + ss) * 1_000_000 + frac;
        }

        fn pgDate(s0: []const u8) ?i64 {
            const s = trim(s0);
            var p = TextParser{ .s = s };
            const days = p.date() orelse return null;
            if (p.i == s.len) return days;
            if (s[p.i] != ' ' and s[p.i] != 'T') return null;
            _ = pgTimestamp(s) orelse return null;
            return days;
        }

        fn pgTimestamp(s0: []const u8) ?i64 {
            const s = trim(s0);
            var p = TextParser{ .s = s };
            const days = p.date() orelse return null;
            if (p.i == s.len) return days * 86_400_000_000;
            if (s[p.i] != ' ' and s[p.i] != 'T') return null;
            p.i += 1;
            const hh = p.num(1, 2) orelse return null;
            if (!p.eat(':')) return null;
            const mm = p.num(2, 2) orelse return null;
            var ss: i64 = 0;
            var frac: i64 = 0;
            if (p.eat(':')) {
                ss = p.num(2, 2) orelse return null;
                if (p.eat('.')) {
                    var scale: i64 = 100_000;
                    const start = p.i;
                    while (p.i < s.len and std.ascii.isDigit(s[p.i])) : (p.i += 1) {
                        frac += @as(i64, s[p.i] - '0') * scale;
                        scale = @divTrunc(scale, 10);
                    }
                    if (p.i == start) return null;
                }
            }
            if (hh > 23 or mm > 59 or ss > 59) return null;
            var off_s: i64 = 0;
            if (p.i < s.len) {
                const c = s[p.i];
                if (c == 'Z' or c == 'z') {
                    p.i += 1;
                } else if (c == '+' or c == '-') {
                    p.i += 1;
                    const oh = p.num(2, 2) orelse return null;
                    _ = p.eat(':');
                    const om = if (p.i < s.len) p.num(2, 2) orelse return null else 0;
                    if (oh > 15 or om > 59) return null;
                    off_s = (oh * 3600 + om * 60) * @as(i64, if (c == '-') -1 else 1);
                }
            }
            if (p.i != s.len) return null;
            return days * 86_400_000_000 + (hh * 3600 + mm * 60 + ss - off_s) * 1_000_000 + frac;
        }

        const TextParser = struct {
            s: []const u8,
            i: usize = 0,

            fn date(p: *TextParser) ?i64 {
                const y = p.num(4, 4) orelse return null;
                if (!p.eat('-')) return null;
                const m = p.num(1, 2) orelse return null;
                if (!p.eat('-')) return null;
                const d = p.num(1, 2) orelse return null;
                if (m < 1 or m > 12 or d < 1 or d > daysInMonth(y, @intCast(m))) return null;
                return daysFromCivil(y, @intCast(m), @intCast(d));
            }

            fn num(p: *TextParser, min: usize, max: usize) ?i64 {
                const start = p.i;
                while (p.i < p.s.len and p.i - start < max and std.ascii.isDigit(p.s[p.i])) p.i += 1;
                if (p.i - start < min) return null;
                return isoNum(p.s[start..p.i]);
            }

            fn eat(p: *TextParser, c: u8) bool {
                if (p.i < p.s.len and p.s[p.i] == c) {
                    p.i += 1;
                    return true;
                }
                return false;
            }
        };

        const DateText = struct { days: i64, rest: []const u8 };

        fn dateText(t: []const u8) ?DateText {
            const bc = std.mem.endsWith(u8, t, " BC");
            const body = if (bc) t[0 .. t.len - 3] else t;
            var i: usize = 0;
            while (i < body.len and i < 7 and std.ascii.isDigit(body[i])) i += 1;
            if (i < 4 or body.len < i + 6 or body[i] != '-' or body[i + 3] != '-') return null;
            for ([_]usize{ i + 1, i + 2, i + 4, i + 5 }) |k| if (!std.ascii.isDigit(body[k])) return null;
            var y = atoiN(body[0..i]);
            const m: u32 = @intCast(atoiN(body[i + 1 .. i + 3]));
            const d: u32 = @intCast(atoiN(body[i + 4 .. i + 6]));
            if (m < 1 or m > 12 or d < 1 or d > daysInMonth(y, m)) return null;
            if (bc) y = 1 - y;
            return .{ .days = daysFromCivil(y, m, d), .rest = body[i + 6 ..] };
        }

        fn wireDate(t: []const u8) ?i64 {
            const dt = dateText(t) orelse return null;
            if (dt.rest.len > 0) return null;
            return dt.days;
        }

        /// cell.zig's `wireDatetime`, its arithmetic widened to i128 so that a value past
        /// i64, where the old body overflowed, shows as such.
        fn wireDatetime(t: []const u8) ?i128 {
            const dt = dateText(t) orelse return null;
            const r = dt.rest;
            if (r.len == 0) return @as(i128, dt.days) * 86_400_000_000;
            if (r.len < 9 or (r[0] != ' ' and r[0] != 'T') or r[3] != ':' or r[6] != ':') return null;
            for ([_]usize{ 1, 2, 4, 5, 7, 8 }) |k| if (!std.ascii.isDigit(r[k])) return null;
            var secs: i64 = atoiN(r[1..3]) * 3600 + atoiN(r[4..6]) * 60 + atoiN(r[7..9]);
            var frac: i64 = 0;
            var i: usize = 9;
            if (r.len > 10 and r[9] == '.') {
                i = 10;
                var scale: i64 = 100_000;
                while (i < r.len and r[i] >= '0' and r[i] <= '9') : (i += 1) {
                    if (scale > 0) frac += @as(i64, r[i] - '0') * scale;
                    scale = @divTrunc(scale, 10);
                }
            }
            if (i < r.len and r[i] == ' ') i += 1;
            if (i + 3 <= r.len and (r[i] == '+' or r[i] == '-')) {
                const sign: i64 = if (r[i] == '-') -1 else 1;
                var off = atoiN(r[i + 1 .. i + 3]) * 3600;
                var j = i + 3;
                if (j < r.len and r[j] == ':') j += 1;
                if (j + 2 <= r.len) off += atoiN(r[j .. j + 2]) * 60;
                secs -= sign * off;
            }
            return @as(i128, dt.days) * 86_400_000_000 + secs * 1_000_000 + frac;
        }

        fn atoiN(s: []const u8) i64 {
            var v: i64 = 0;
            for (s) |c| {
                if (c >= '0' and c <= '9') v = v * 10 + (c - '0');
            }
            return v;
        }
    };
    const fuzzTemporal = struct {
        fn f(r: std.Random, buf: []u8) []const u8 {
            var w = std.Io.Writer.fixed(buf);
            const pick = struct {
                fn f(rr: std.Random, comptime opts: []const []const u8) []const u8 {
                    return opts[rr.uintLessThan(usize, opts.len)];
                }
            }.f;
            const num = struct {
                fn f(rr: std.Random, ww: *std.Io.Writer, usual: usize, max: i64) void {
                    const digits = if (rr.uintLessThan(u8, 8) == 0) rr.intRangeAtMost(usize, 1, usual + 1) else usual;
                    const v = rr.intRangeAtMost(i64, 0, max);
                    var tmp: [24]u8 = undefined;
                    const s = std.fmt.bufPrint(&tmp, "{d}", .{v}) catch unreachable;
                    if (s.len < digits) ww.splatByteAll('0', digits - s.len) catch {};
                    ww.writeAll(s[s.len -| @max(digits, 1)..]) catch {};
                }
            }.f;
            if (r.uintLessThan(u8, 6) == 0) w.writeAll(pick(r, &.{ " ", "\t", "\r\n", "\x0B", "\x0C", "  " })) catch {};
            const time_only = r.uintLessThan(u8, 4) == 0;
            if (!time_only) {
                switch (r.uintLessThan(u8, 10)) {
                    0 => w.writeAll(pick(r, &.{ "294247", "294248", "300000", "10000", "11000", "0000", "0001", "-0001", "+2024", "9999999", "12345678", "0044" })) catch {},
                    1 => num(r, &w, r.intRangeAtMost(usize, 1, 8), 99_999_999),
                    else => num(r, &w, 4, 9999),
                }
                w.writeAll(if (r.uintLessThan(u8, 20) == 0) pick(r, &.{ "/", " ", "", ":" }) else "-") catch {};
                num(r, &w, 2, if (r.uintLessThan(u8, 10) == 0) 99 else 13);
                w.writeAll(if (r.uintLessThan(u8, 20) == 0) pick(r, &.{ "/", " ", "" }) else "-") catch {};
                num(r, &w, 2, if (r.uintLessThan(u8, 10) == 0) 99 else 31);
            }
            if (time_only or r.uintLessThan(u8, 10) < 6) {
                if (!time_only) w.writeAll(pick(r, &.{ " ", " ", "T", "T", "x", "-", "  " })) catch {};
                num(r, &w, 2, if (r.uintLessThan(u8, 10) == 0) 99 else 24);
                w.writeAll(if (r.uintLessThan(u8, 30) == 0) "." else ":") catch {};
                num(r, &w, 2, 60);
                if (r.uintLessThan(u8, 4) != 0) {
                    w.writeByte(':') catch {};
                    num(r, &w, 2, 60);
                    if (r.boolean()) {
                        w.writeByte('.') catch {};
                        const n = r.uintAtMost(usize, 9);
                        for (0..n) |_| w.writeByte('0' + r.uintLessThan(u8, 10)) catch {};
                    }
                }
                if (r.uintLessThan(u8, 3) == 0) w.writeAll(pick(r, &.{ "Z", "z", "+5", "-03", "+05:45", "-0230", "+05:", " +00:00", " UTC", "+16", "+05:60", "+a5", "Zx", "+0", "-1:3" })) catch {};
            }
            if (r.uintLessThan(u8, 12) == 0) w.writeAll(pick(r, &.{ " BC", " AD", "BC", "x" })) catch {};
            if (r.uintLessThan(u8, 6) == 0) w.writeAll(pick(r, &.{ " ", "\t", "\n", "\x0B", " \r" })) catch {};
            return w.buffered();
        }
    }.f;
    const Wire = civil.Text(.wire);
    var rnd = std.Random.DefaultPrng.init(0x7e47);
    const r = rnd.random();
    var buf: [96]u8 = undefined;
    const n = 300_000;
    var accepted = [_]usize{0} ** 8;
    var overflowed: usize = 0;
    for (0..n) |_| {
        const s = fuzzTemporal(r, &buf);
        errdefer std.debug.print("input `{s}`\n", .{s});
        const pairs = [_]struct { ?i64, ?i64 }{
            .{ Old.isoDate(s), parseIsoDate(s) },
            .{ Old.isoTime(s), parseIsoTime(s) },
            .{ Old.isoTimestamp(s), parseIsoTimestamp(s) },
            .{ Old.pgDate(s), parseDateText(s) },
            .{ Old.pgTimestamp(s), parseTimestampText(s) },
            .{ Old.wireDate(s), Wire.date(s) },
        };
        for (pairs, 0..) |p, k| {
            try std.testing.expectEqual(p[0], p[1]);
            if (p[0] != null) accepted[k] += 1;
        }
        const new = Wire.timestamp(s);
        if (Old.wireDatetime(s)) |old| {
            if (std.math.cast(i64, old)) |fits| {
                try std.testing.expectEqual(@as(?i64, fits), new);
                accepted[6] += 1;
            } else {
                try std.testing.expectEqual(@as(?i64, null), new);
                overflowed += 1;
            }
        } else try std.testing.expectEqual(@as(?i64, null), new);
    }
    for (accepted[0..7]) |a| try std.testing.expect(a >= n / 50);
    try std.testing.expect(overflowed > 0);
}

test "civil dates round-trip through year 0 and negative multiples of 400" {
    try std.testing.expectEqual(@as(i64, -719528), daysFromCivil(0, 1, 1));
    try std.testing.expectEqual(@as(i64, -719529), daysFromCivil(-1, 12, 31));
    const years = [_]i64{ -800, -401, -400, -399, -1, 0, 1, 399, 400, 1970, 2000, 2024 };
    for (years) |y| for ([_]u32{ 1, 2, 3, 12 }) |m| {
        const days = daysFromCivil(y, m, 1);
        const c = civilFromDays(days);
        try std.testing.expectEqual(y, c.y);
        try std.testing.expectEqual(m, c.m);
        try std.testing.expectEqual(@as(u32, 1), c.d);
        const prev = civilFromDays(days - 1);
        try std.testing.expectEqual(daysInMonth(prev.y, prev.m), prev.d);
    };
}

test "date_trunc, extract, date_add, date_diff and strptime agree with the bodies they replaced" {
    const Old = struct {
        fn trunc(us: i64, u: TimeUnit) i64 {
            const day = @divFloor(us, 86_400_000_000);
            const rem = @mod(us, 86_400_000_000);
            return switch (u) {
                .second => us - @mod(rem, 1_000_000),
                .minute => us - @mod(rem, 60_000_000),
                .hour => us - @mod(rem, 3_600_000_000),
                .day => day *| 86_400_000_000,
                .week => isoWeekStart(day) *| 86_400_000_000,
                .month => blk: {
                    const c = civilFromDays(day);
                    break :blk daysFromCivil(c.y, c.m, 1) *| 86_400_000_000;
                },
                .year => blk: {
                    const c = civilFromDays(day);
                    break :blk daysFromCivil(c.y, 1, 1) *| 86_400_000_000;
                },
            };
        }

        fn extract(us: i64, u: TimeUnit) i64 {
            const day = @divFloor(us, 86_400_000_000);
            const rem = @mod(us, 86_400_000_000);
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

        fn add(us: i64, u: TimeUnit, n: i64) EvalError!i64 {
            return switch (u) {
                .year, .month => blk: {
                    const day = @divFloor(us, 86_400_000_000);
                    const rem = @mod(us, 86_400_000_000);
                    const months = if (u == .year) try mulI64(n, 12) else n;
                    const shifted = try mulI64(try addMonthsToDays(day, months), 86_400_000_000);
                    break :blk try addI64(shifted, rem);
                },
                .week => try addI64(us, try mulI64(n, 7 * 86_400_000_000)),
                .day => try addI64(us, try mulI64(n, 86_400_000_000)),
                .hour => try addI64(us, try mulI64(n, 3_600_000_000)),
                .minute => try addI64(us, try mulI64(n, 60_000_000)),
                .second => try addI64(us, try mulI64(n, 1_000_000)),
            };
        }

        fn diff(a_us: i64, b_us: i64, u: TimeUnit) i64 {
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
                .day => return @intCast(@divTrunc(@as(i128, b_us) - a_us, 86_400_000_000)),
                .hour => return @intCast(@divTrunc(@as(i128, b_us) - a_us, 3_600_000_000)),
                .minute => return @intCast(@divTrunc(@as(i128, b_us) - a_us, 60_000_000)),
                .second => return @intCast(@divTrunc(@as(i128, b_us) - a_us, 1_000_000)),
            }
        }

        fn strptime(text: []const u8, fmt: []const u8) ?i64 {
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
    };
    var rnd = std.Random.DefaultPrng.init(0xa7d1);
    const r = rnd.random();
    const units = std.enums.values(TimeUnit);
    const span: i64 = 180_000 * 365 * us_per_day;
    for (0..200_000) |i| {
        const a = if (i % 3 == 0) r.int(i64) else r.intRangeAtMost(i64, -span, span);
        const b = r.intRangeAtMost(i64, -span, span);
        const u = units[i % units.len];
        errdefer std.debug.print("{d} {d} {t}\n", .{ a, b, u });
        try std.testing.expectEqual(Old.trunc(a, u), truncMicros(a, u));
        try std.testing.expectEqual(Old.extract(a, u), extractField(a, u));
        if (i % 3 != 0) try std.testing.expectEqual(Old.diff(a, b, u), dateDiff(a, b, u));
        const n: i64 = switch (i % 4) {
            0 => r.intRangeAtMost(i64, -1000, 1000),
            1 => r.int(i64),
            2 => r.int(i32),
            else => @divTrunc(std.math.maxInt(i64), u.fixedMicros() orelse us_per_day) + r.intRangeAtMost(i64, -2, 2),
        };
        const old = Old.add(a, u, n);
        const new = addMicros(a, u, n);
        if (old) |v| try std.testing.expectEqual(v, try new) else |e| try std.testing.expectError(e, new);
    }
    const fmts = [_][]const u8{ "%Y-%m-%d", "%d/%m/%y %H:%M:%S", "%Y%m%d%H%M", "%H:%M", "%%%Y" };
    var buf: [32]u8 = undefined;
    for (0..100_000) |i| {
        const f = fmts[i % fmts.len];
        var n: usize = 0;
        var k: usize = 0;
        while (k < f.len) : (k += 1) {
            if (f[k] == '%' and k + 1 < f.len and f[k + 1] != '%') {
                const w: usize = if (f[k + 1] == 'Y') 4 else 2;
                const digits = if (r.uintLessThan(u8, 10) == 0) r.uintAtMost(usize, w) else w;
                for (0..digits) |_| {
                    buf[n] = '0' + r.uintLessThan(u8, if (r.boolean()) 3 else 10);
                    n += 1;
                }
                k += 1;
            } else {
                buf[n] = if (f[k] == '%') blk: {
                    k += 1;
                    break :blk '%';
                } else f[k];
                n += 1;
            }
        }
        if (r.uintLessThan(u8, 30) == 0) buf[r.uintLessThan(usize, n + 1) -| 1] = 'x';
        const text = buf[0..n];
        errdefer std.debug.print("`{s}` in `{s}`\n", .{ text, f });
        try std.testing.expectEqual(Old.strptime(text, f), strptimeFmt(text, f));
    }
}
