//! The proleptic Gregorian calendar on days and microseconds since 1970-01-01:
//! civil conversions, the epochs other systems count from, temporal units and
//! ticks, and the one text parser (`Text`) and writer (`writeLayout`) factories.
//! std only, so store/, net/ and format/ can import it.

const std = @import("std");

pub const us_per_day: i64 = 86_400_000_000;
pub const us_per_sec: i64 = 1_000_000;

/// The widest year `civilDays` takes, as `make_date` bounds it.
pub const max_year: i64 = 1_000_000_000;

pub const Date = struct { y: i64, m: u32, d: u32 };

pub fn isLeap(y: i64) bool {
    return @mod(y, 4) == 0 and (@mod(y, 100) != 0 or @mod(y, 400) == 0);
}

pub fn daysInMonth(y: i64, m: u32) u32 {
    const lens = [_]u32{ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    if (m == 2 and isLeap(y)) return 29;
    return lens[m - 1];
}

/// Days since 1970-01-01 of a date whose month and day are in range.
pub fn daysFromCivil(y0: i64, m: u32, d: u32) i64 {
    const y = if (m <= 2) y0 - 1 else y0;
    const era = @divFloor(y, 400);
    const yoe = y - era * 400;
    const mp: i64 = @intCast((m + 9) % 12);
    const doy = @divFloor(153 * mp + 2, 5) + @as(i64, @intCast(d)) - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146097 + doe - 719468;
}

pub fn civilFromDays(z0: i64) Date {
    const z = z0 + 719468;
    const era = @divFloor(z, 146097);
    const doe = z - era * 146097;
    const yoe = @divFloor(doe - @divFloor(doe, 1460) + @divFloor(doe, 36524) - @divFloor(doe, 146096), 365);
    const y = yoe + era * 400;
    const doy = doe - (365 * yoe + @divFloor(yoe, 4) - @divFloor(yoe, 100));
    const mp = @divFloor(5 * doy + 2, 153);
    const d: u32 = @intCast(doy - @divFloor(153 * mp + 2, 5) + 1);
    const m: u32 = @intCast(if (mp < 10) mp + 3 else mp - 9);
    return .{ .y = y + (if (m <= 2) @as(i64, 1) else 0), .m = m, .d = d };
}

/// 0 is Sunday, 6 is Saturday.
pub fn weekday(days: i64) u3 {
    return @intCast(@mod(days + 4, 7));
}

/// Each epoch as the day number since 1970-01-01 of its own day zero, so a raw
/// day count `n` from it is the date `n + epoch.x`; `julian_unix` alone is the
/// other way round, the Julian Day Number of 1970-01-01 (`n - julian_unix`).
pub const epoch = struct {
    pub const ce1: i64 = daysFromCivil(1, 1, 1);
    pub const sql1900: i64 = daysFromCivil(1900, 1, 1);
    pub const excel1900: i64 = daysFromCivil(1899, 12, 30);
    pub const excel1904: i64 = daysFromCivil(1904, 1, 1);
    pub const julian_unix: i64 = -daysFromCivil(-4713, 11, 24);
};

/// A day's midnight in microseconds, saturating past the ±292,277 years a
/// timestamp holds.
pub fn dayMicros(days: i64) i64 {
    return days *| us_per_day;
}

pub fn dayMicrosChecked(days: i64) ?i64 {
    return std.math.mul(i64, days, us_per_day) catch null;
}

pub fn dayOf(us: i64) i64 {
    return @divFloor(us, us_per_day);
}

/// Microseconds since the midnight before `us`, in `[0, us_per_day)`.
pub fn timeOfDay(us: i64) i64 {
    return @mod(us, us_per_day);
}

pub const Fields = struct {
    y: i64,
    m: u32,
    d: u32,
    h: u32 = 0,
    mi: u32 = 0,
    s: u32 = 0,
    us: u32 = 0,
};

pub fn fieldsOfDays(days: i64) Fields {
    const c = civilFromDays(days);
    return .{ .y = c.y, .m = c.m, .d = c.d };
}

pub fn fieldsOfMicros(us: i64) Fields {
    var f = fieldsOfDays(dayOf(us));
    const t: u64 = @intCast(timeOfDay(us));
    const secs: u32 = @intCast(t / 1_000_000);
    f.h = secs / 3600;
    f.mi = secs % 3600 / 60;
    f.s = secs % 60;
    f.us = @intCast(t % 1_000_000);
    return f;
}

pub fn fieldsOfSecs(secs: i64) Fields {
    var f = fieldsOfDays(@divFloor(secs, 86_400));
    const t: u32 = @intCast(@mod(secs, 86_400));
    f.h = t / 3600;
    f.mi = t % 3600 / 60;
    f.s = t % 60;
    return f;
}

/// Days since 1970-01-01 of a date that exists, null for a month or day out of
/// range or a year past ±`max_year`.
pub fn civilDays(y: i64, m: i64, d: i64) ?i64 {
    if (y < -max_year or y > max_year or m < 1 or m > 12 or d < 1) return null;
    if (d > daysInMonth(y, @intCast(m))) return null;
    return daysFromCivil(y, @intCast(m), @intCast(d));
}

/// Microseconds since midnight of a clock reading that exists.
pub fn clockMicros(h: i64, mi: i64, s: i64, us: i64) ?i64 {
    if (h < 0 or h > 23 or mi < 0 or mi > 59 or s < 0 or s > 59 or us < 0 or us >= us_per_sec) return null;
    return (h * 3600 + mi * 60 + s) * us_per_sec + us;
}

/// A two-digit year as POSIX `%y` reads it: 69-99 are the 1900s, 00-68 the 2000s.
pub fn pivotYY(v: i64) i64 {
    return if (v >= 69) 1900 + v else 2000 + v;
}

/// A count of one time unit, the order Arrow's TimeUnit codes 0..3 take.
pub const Tick = enum {
    s,
    ms,
    us,
    ns,

    /// `x` ticks as microseconds; nanoseconds round toward minus infinity. A count
    /// past i64 microseconds is an Overflow, never clamped to a wrong instant.
    pub fn toMicros(t: Tick, x: i64) error{Overflow}!i64 {
        return switch (t) {
            .s => std.math.mul(i64, x, us_per_sec),
            .ms => std.math.mul(i64, x, 1000),
            .us => x,
            .ns => @divFloor(x, 1000),
        };
    }

    /// The largest magnitude of ticks `toMicros` takes, for a kernel that checks a
    /// whole run of values at once.
    pub fn limit(comptime t: Tick) i64 {
        return switch (t) {
            .s => @divTrunc(std.math.maxInt(i64), us_per_sec),
            .ms => @divTrunc(std.math.maxInt(i64), 1000),
            .us, .ns => std.math.maxInt(i64),
        };
    }
};

/// A calendar or clock unit, in the order `date_trunc`'s units take.
pub const Part = enum {
    year,
    month,
    week,
    day,
    hour,
    minute,
    second,

    /// The unit `name` spells, singular or plural, ignoring case, as PostgreSQL's
    /// and DuckDB's unit tables read it.
    pub fn parse(name: []const u8) ?Part {
        var buf: [16]u8 = undefined;
        if (name.len == 0 or name.len >= buf.len) return null;
        const low = std.ascii.lowerString(buf[0..name.len], name);
        if (std.meta.stringToEnum(Part, low)) |p| return p;
        if (low[low.len - 1] == 's') return std.meta.stringToEnum(Part, low[0 .. low.len - 1]);
        return null;
    }

    /// Microseconds in one unit, null for a month or year, whose length varies.
    pub fn fixedMicros(p: Part) ?i64 {
        return switch (p) {
            .year, .month => null,
            .week => 7 * us_per_day,
            .day => us_per_day,
            .hour => 3_600_000_000,
            .minute => 60_000_000,
            .second => us_per_sec,
        };
    }
};

/// How a source spells dates and times as text; `Text` turns one into a parser.
pub const Spelling = struct {
    trim: bool,
    year_digits: [2]u8,
    month_day_min: u8,
    hour_min: u8,
    bc_suffix: bool = false,
    year_sign: bool = false,
    /// A year spelled only as `writeLayout` spells one: past four digits no leading
    /// `0`, and no `-` before year 0, so `'01234-05-06'`, a code, is no date.
    canonical_year: bool = false,
    any_separator: bool = false,
    date_tail: bool = false,
    stamp_seconds: bool,
    time_seconds: bool,
    frac_min: u8,
    check_clock: bool = true,
    zone: enum { none, pg, wire } = .none,
    strict_end: bool = true,

    /// `[-]YYYY-MM-DD[?HH:MM:SS[.f+]]` with a four- to seven-digit year, as
    /// `writeLayout` spells one, any byte between date and time, and a time
    /// `HH:MM[:SS[.f+]]`, each fraction cut to the microsecond; strict, so
    /// `'01/07/2013'` is no date.
    pub const iso: Spelling = .{
        .trim = true,
        .year_digits = .{ 4, 7 },
        .year_sign = true,
        .canonical_year = true,
        .month_day_min = 2,
        .hour_min = 2,
        .any_separator = true,
        .stamp_seconds = true,
        .time_seconds = false,
        .frac_min = 1,
    };

    /// As PostgreSQL reads a literal: one- or two-digit month, day and hour, `T` or
    /// space, optional seconds, any fraction (cut to microseconds), and a zone
    /// `Z`/`±HH[[:]MM]` folded into UTC; a date may carry a valid time it drops.
    /// The year is `iso`'s, signed and four to seven digits.
    pub const pg_text: Spelling = .{
        .trim = true,
        .year_digits = .{ 4, 7 },
        .year_sign = true,
        .month_day_min = 1,
        .hour_min = 1,
        .date_tail = true,
        .stamp_seconds = false,
        .time_seconds = false,
        .frac_min = 1,
        .zone = .pg,
    };

    /// A database's own text: four- to seven-digit and ` BC` years, `HH:MM:SS`
    /// unchecked, any fraction, an offset folded into UTC, and anything after it
    /// ignored.
    pub const wire: Spelling = .{
        .trim = false,
        .year_digits = .{ 4, 7 },
        .month_day_min = 2,
        .hour_min = 2,
        .bc_suffix = true,
        .stamp_seconds = true,
        .time_seconds = true,
        .frac_min = 0,
        .check_clock = false,
        .zone = .wire,
        .strict_end = false,
    };
};

/// The parser for one spelling: `date` as days, `timestamp` and `time` as
/// microseconds, null for text that spelling does not read or that overflows: a
/// date past the i32 days a DATE holds, or a timestamp past i64 microseconds.
pub fn Text(comptime sp: Spelling) type {
    return struct {
        const Clock = struct { secs: i64, frac: i64 };

        const Scan = struct {
            s: []const u8,
            i: usize = 0,

            fn end(c: *const Scan) bool {
                return c.i == c.s.len;
            }

            fn eat(c: *Scan, ch: u8) bool {
                if (c.i < c.s.len and c.s[c.i] == ch) {
                    c.i += 1;
                    return true;
                }
                return false;
            }

            /// `min` to `max` digits; both comptime, so the loop unrolls.
            fn num(c: *Scan, comptime min: usize, comptime max: usize) ?i64 {
                const start = c.i;
                var v: i64 = 0;
                var more = true;
                inline for (0..max) |_| {
                    if (more and c.i < c.s.len and std.ascii.isDigit(c.s[c.i])) {
                        v = v * 10 + (c.s[c.i] - '0');
                        c.i += 1;
                    } else more = false;
                }
                if (c.i - start < min) return null;
                return v;
            }

            fn date(c: *Scan, bc: bool) ?i64 {
                const neg = sp.year_sign and c.eat('-');
                const start = c.i;
                const n = c.num(sp.year_digits[0], sp.year_digits[1]) orelse return null;
                if (sp.canonical_year and ((c.i - start > 4 and c.s[start] == '0') or (neg and n == 0))) return null;
                const y = if (bc) 1 - n else if (neg) -n else n;
                if (!c.eat('-')) return null;
                const m = c.num(sp.month_day_min, 2) orelse return null;
                if (!c.eat('-')) return null;
                const d = c.num(sp.month_day_min, 2) orelse return null;
                if (m < 1 or m > 12 or d < 1 or d > daysInMonth(y, @intCast(m))) return null;
                const days = daysFromCivil(y, @intCast(m), @intCast(d));
                if (days < std.math.minInt(i32) or days > std.math.maxInt(i32)) return null;
                return days;
            }

            fn fraction(c: *Scan) ?i64 {
                const start = c.i;
                var frac: i64 = 0;
                var scale: i64 = 100_000;
                while (c.i < c.s.len and std.ascii.isDigit(c.s[c.i])) : (c.i += 1) {
                    frac += @as(i64, c.s[c.i] - '0') * scale;
                    scale = @divTrunc(scale, 10);
                }
                if (c.i - start < sp.frac_min) return null;
                return frac;
            }

            fn clock(c: *Scan, comptime seconds: bool) ?Clock {
                const hh = c.num(sp.hour_min, 2) orelse return null;
                if (!c.eat(':')) return null;
                const mm = c.num(2, 2) orelse return null;
                var ss: i64 = 0;
                var frac: i64 = 0;
                if (c.eat(':')) {
                    ss = c.num(2, 2) orelse return null;
                    if (c.eat('.')) frac = c.fraction() orelse return null;
                } else if (seconds) return null;
                if (sp.check_clock and (hh > 23 or mm > 59 or ss > 59)) return null;
                return .{ .secs = hh * 3600 + mm * 60 + ss, .frac = frac };
            }

            fn digitSum(s: []const u8) i64 {
                var v: i64 = 0;
                for (s) |ch| if (ch >= '0' and ch <= '9') {
                    v = v * 10 + (ch - '0');
                };
                return v;
            }

            fn zone(c: *Scan) ?i64 {
                switch (sp.zone) {
                    .none => return 0,
                    .pg => {
                        if (c.end()) return 0;
                        const ch = c.s[c.i];
                        if (ch == 'Z' or ch == 'z') {
                            c.i += 1;
                            return 0;
                        }
                        if (ch != '+' and ch != '-') return 0;
                        c.i += 1;
                        const oh = c.num(2, 2) orelse return null;
                        _ = c.eat(':');
                        const om = if (!c.end()) c.num(2, 2) orelse return null else 0;
                        if (oh > 15 or om > 59) return null;
                        return (oh * 3600 + om * 60) * @as(i64, if (ch == '-') -1 else 1);
                    },
                    .wire => {
                        const r = c.s;
                        _ = c.eat(' ');
                        const i = c.i;
                        if (i + 3 > r.len or (r[i] != '+' and r[i] != '-')) return 0;
                        var off = digitSum(r[i + 1 .. i + 3]) * 3600;
                        var j = i + 3;
                        if (j < r.len and r[j] == ':') j += 1;
                        if (j + 2 <= r.len) off += digitSum(r[j .. j + 2]) * 60;
                        c.i = r.len;
                        return if (r[i] == '-') -off else off;
                    },
                }
            }
        };

        const Body = struct { s: []const u8, bc: bool };

        fn body(s0: []const u8) Body {
            const s = if (sp.trim) std.mem.trim(u8, s0, " \t\r\n") else s0;
            if (sp.bc_suffix and std.mem.endsWith(u8, s, " BC")) return .{ .s = s[0 .. s.len - 3], .bc = true };
            return .{ .s = s, .bc = false };
        }

        fn separator(ch: u8) bool {
            return sp.any_separator or ch == ' ' or ch == 'T';
        }

        fn combine(days: i64, secs: i64, frac: i64) ?i64 {
            return std.math.cast(i64, @as(i128, days) * us_per_day + secs * us_per_sec + frac);
        }

        pub fn date(s0: []const u8) ?i64 {
            const b = body(s0);
            var c = Scan{ .s = b.s };
            const days = c.date(b.bc) orelse return null;
            if (c.end()) return days;
            if (!sp.date_tail or !separator(c.s[c.i])) return null;
            _ = timestamp(s0) orelse return null;
            return days;
        }

        pub fn timestamp(s0: []const u8) ?i64 {
            const b = body(s0);
            var c = Scan{ .s = b.s };
            const days = c.date(b.bc) orelse return null;
            if (c.end()) return dayMicrosChecked(days);
            if (!separator(c.s[c.i])) return null;
            c.i += 1;
            const k = c.clock(sp.stamp_seconds) orelse return null;
            const off = c.zone() orelse return null;
            if (sp.strict_end and !c.end()) return null;
            return combine(days, k.secs - off, k.frac);
        }

        /// A time of day with no zone.
        pub fn time(s0: []const u8) ?i64 {
            var c = Scan{ .s = if (sp.trim) std.mem.trim(u8, s0, " \t\r\n") else s0 };
            const k = c.clock(sp.time_seconds) orelse return null;
            if (sp.strict_end and !c.end()) return null;
            return k.secs * us_per_sec + k.frac;
        }
    };
}

/// How `writeLayout` spells a date, time or timestamp.
pub const Layout = struct {
    parts: enum { date, time, timestamp },
    year: enum { expanded, four_digit, clamp_zero, bc } = .expanded,
    date_sep: []const u8 = "-",
    between: []const u8 = " ",
    time_sep: []const u8 = ":",
    frac: enum { none, us_nonzero, ms_nonzero, us, ms } = .us_nonzero,
    suffix: []const u8 = "",

    pub const date: Layout = .{ .parts = .date };
    pub const time: Layout = .{ .parts = .time };
    pub const timestamp: Layout = .{ .parts = .timestamp };
    pub const delta_date: Layout = .{ .parts = .date, .year = .four_digit };
    pub const delta_timestamp: Layout = .{ .parts = .timestamp, .year = .four_digit, .frac = .us };
    pub const delta_timestamp_ms: Layout = .{ .parts = .timestamp, .year = .four_digit, .frac = .ms };
    pub const delta_timestamp_utc: Layout = .{ .parts = .timestamp, .year = .four_digit, .between = "T", .frac = .us, .suffix = "Z" };
    pub const delta_timestamp_utc_ms: Layout = .{ .parts = .timestamp, .year = .four_digit, .between = "T", .frac = .ms, .suffix = "Z" };
    pub const amz: Layout = .{ .parts = .timestamp, .date_sep = "", .between = "T", .time_sep = "", .frac = .none, .suffix = "Z" };
    pub const kerberos: Layout = .{ .parts = .timestamp, .date_sep = "", .between = "", .time_sep = "", .frac = .none, .suffix = "Z" };
    pub const aad: Layout = .{ .parts = .timestamp, .between = "T", .frac = .none, .suffix = ".000Z" };
    pub const xlsx_date: Layout = .{ .parts = .date, .year = .clamp_zero };
    pub const pg_date: Layout = .{ .parts = .date, .year = .bc };
    pub const pg_timestamp: Layout = .{ .parts = .timestamp, .year = .bc };
    pub const xlsx_time: Layout = .{ .parts = .time, .frac = .ms_nonzero };
};

/// `v` as exactly `out.len` digits, zero-padded on the left.
inline fn putDigits(out: []u8, v: u64) void {
    var x = v;
    var i = out.len;
    while (i > 0) {
        i -= 1;
        out[i] = '0' + @as(u8, @intCast(x % 10));
        x /= 10;
    }
}

/// At least four digits, more as the year needs.
fn putYear(out: []u8, y: u64) usize {
    if (y < 10_000) {
        putDigits(out[0..4], y);
        return 4;
    }
    var n: usize = 4;
    var x = y / 10_000;
    while (x > 0) : (x /= 10) n += 1;
    putDigits(out[0..n], y);
    return n;
}

/// `x` (days for a date, microseconds otherwise) in layout `l`, set down in one
/// write. `expanded` years are at least four digits, `-` before year 0;
/// `four_digit` years outside 1..9999 are YearOutOfRange; `clamp_zero` prints a
/// year before 0 as 0000; `bc` prints a year before 1 as PostgreSQL does, `1 - y`
/// and ` BC` at the end (year 0 is `0001 ... BC`).
pub fn writeLayout(w: anytype, x: i64, comptime l: Layout) !void {
    var buf: [80]u8 = undefined;
    var n: usize = 0;
    var bc = false;
    if (l.parts != .time) {
        const c = civilFromDays(if (l.parts == .date) x else dayOf(x));
        switch (l.year) {
            .expanded => {
                if (c.y < 0) {
                    buf[n] = '-';
                    n += 1;
                }
                n += putYear(buf[n..], @abs(c.y));
            },
            .four_digit => {
                if (c.y < 1 or c.y > 9999) return error.YearOutOfRange;
                n += putYear(buf[n..], @intCast(c.y));
            },
            .clamp_zero => n += putYear(buf[n..], @intCast(@max(c.y, 0))),
            .bc => {
                bc = c.y < 1;
                n += putYear(buf[n..], @intCast(if (bc) 1 - c.y else c.y));
            },
        }
        @memcpy(buf[n..][0..l.date_sep.len], l.date_sep);
        n += l.date_sep.len;
        putDigits(buf[n..][0..2], c.m);
        n += 2;
        @memcpy(buf[n..][0..l.date_sep.len], l.date_sep);
        n += l.date_sep.len;
        putDigits(buf[n..][0..2], c.d);
        n += 2;
    }
    if (l.parts == .timestamp) {
        @memcpy(buf[n..][0..l.between.len], l.between);
        n += l.between.len;
    }
    if (l.parts != .date) {
        const t: u64 = @intCast(timeOfDay(x));
        const secs = t / 1_000_000;
        const frac = t % 1_000_000;
        putDigits(buf[n..][0..2], secs / 3600);
        n += 2;
        @memcpy(buf[n..][0..l.time_sep.len], l.time_sep);
        n += l.time_sep.len;
        putDigits(buf[n..][0..2], secs % 3600 / 60);
        n += 2;
        @memcpy(buf[n..][0..l.time_sep.len], l.time_sep);
        n += l.time_sep.len;
        putDigits(buf[n..][0..2], secs % 60);
        n += 2;
        const digits: usize, const v: u64 = switch (l.frac) {
            .none => .{ 0, 0 },
            .us_nonzero => if (frac != 0) .{ 6, frac } else .{ 0, 0 },
            .ms_nonzero => if (frac / 1000 != 0) .{ 3, frac / 1000 } else .{ 0, 0 },
            .us => .{ 6, frac },
            .ms => .{ 3, frac / 1000 },
        };
        if (digits > 0) {
            buf[n] = '.';
            putDigits(buf[n + 1 ..][0..digits], v);
            n += 1 + digits;
        }
    }
    if (bc) {
        @memcpy(buf[n..][0..3], " BC");
        n += 3;
    }
    @memcpy(buf[n..][0..l.suffix.len], l.suffix);
    n += l.suffix.len;
    return w.writeAll(buf[0..n]);
}

/// `Sun, 06 Nov 1994 08:49:37 GMT`, the HTTP date (RFC 1123) of `secs` since the epoch.
pub fn writeRfc1123(w: anytype, secs: i64) !void {
    const day_names = [_][]const u8{ "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat" };
    const mon_names = [_][]const u8{ "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };
    const f = fieldsOfSecs(secs);
    var yb: [24]u8 = undefined;
    var yn: usize = 0;
    if (f.y < 0) {
        yb[0] = '-';
        yn = 1;
    }
    yn += putYear(yb[yn..], @abs(f.y));
    try w.print("{s}, {d:0>2} {s} {s} {d:0>2}:{d:0>2}:{d:0>2} GMT", .{
        day_names[weekday(@divFloor(secs, 86_400))], f.d, mon_names[f.m - 1], yb[0..yn], f.h, f.mi, f.s,
    });
}

const testing = std.testing;

test "civil: days and dates round-trip through year 0, negative eras and the i32 day range" {
    try testing.expectEqual(@as(i64, -719528), daysFromCivil(0, 1, 1));
    try testing.expectEqual(@as(i64, -719529), daysFromCivil(-1, 12, 31));
    try testing.expectEqual(@as(i64, 0), daysFromCivil(1970, 1, 1));
    try testing.expectEqual(@as(i64, 19727), daysFromCivil(2024, 1, 5));
    const years = [_]i64{ -800, -401, -400, -399, -1, 0, 1, 399, 400, 1970, 2000, 2024 };
    for (years) |y| for ([_]u32{ 1, 2, 3, 12 }) |m| {
        const days = daysFromCivil(y, m, 1);
        const c = civilFromDays(days);
        try testing.expectEqual(Date{ .y = y, .m = m, .d = 1 }, c);
        const prev = civilFromDays(days - 1);
        try testing.expectEqual(daysInMonth(prev.y, prev.m), prev.d);
    };
    var prev = civilFromDays(-1_000_000);
    var z: i64 = -999_999;
    while (z <= 3_000_000) : (z += 1) {
        const c = civilFromDays(z);
        try testing.expectEqual(z, daysFromCivil(c.y, c.m, c.d));
        if (c.d == 1) {
            try testing.expectEqual(daysInMonth(prev.y, prev.m), prev.d);
        } else try testing.expectEqual(prev.d + 1, c.d);
        prev = c;
    }
    var rnd = std.Random.DefaultPrng.init(7);
    const r = rnd.random();
    for ([_]i64{ std.math.minInt(i32), std.math.maxInt(i32) }) |e| {
        const c = civilFromDays(e);
        try testing.expectEqual(e, daysFromCivil(c.y, c.m, c.d));
    }
    for (0..100_000) |_| {
        const e: i64 = r.int(i32);
        const c = civilFromDays(e);
        try testing.expectEqual(e, daysFromCivil(c.y, c.m, c.d));
        try testing.expect(c.m >= 1 and c.m <= 12 and c.d >= 1 and c.d <= daysInMonth(c.y, c.m));
    }
}

test "civil: leap years, month lengths and weekdays" {
    for ([_]i64{ 2000, 2024, 1600, 0, -4, -400 }) |y| try testing.expect(isLeap(y));
    for ([_]i64{ 1900, 2023, 2100, -1, -100 }) |y| try testing.expect(!isLeap(y));
    try testing.expectEqual(@as(u32, 29), daysInMonth(2024, 2));
    try testing.expectEqual(@as(u32, 28), daysInMonth(1900, 2));
    try testing.expectEqual(@as(u32, 30), daysInMonth(2024, 4));
    try testing.expectEqual(@as(u32, 31), daysInMonth(2024, 12));
    try testing.expectEqual(@as(u3, 4), weekday(0));
    try testing.expectEqual(@as(u3, 3), weekday(-1));
    try testing.expectEqual(@as(u3, 0), weekday(daysFromCivil(1994, 11, 6)));
    try testing.expectEqual(@as(u3, 1), weekday(daysFromCivil(2024, 1, 1)));
    try testing.expectEqual(@as(u3, 1), weekday(epoch.ce1));
    try testing.expectEqual(@as(u3, 6), weekday(daysFromCivil(2000, 1, 1)));
}

test "civil: epochs agree with the literals the readers count from" {
    try testing.expectEqual(@as(i64, -719162), epoch.ce1);
    try testing.expectEqual(@as(i64, -25567), epoch.sql1900);
    try testing.expectEqual(@as(i64, -25569), epoch.excel1900);
    try testing.expectEqual(@as(i64, -24107), epoch.excel1904);
    try testing.expectEqual(@as(i64, 2_440_588), epoch.julian_unix);
    try testing.expectEqual(@as(i64, -719468), daysFromCivil(0, 3, 1));
    try testing.expectEqual(Date{ .y = 1, .m = 1, .d = 1 }, civilFromDays(epoch.ce1));
    try testing.expectEqual(Date{ .y = 1899, .m = 12, .d = 30 }, civilFromDays(epoch.excel1900));
}

test "civil: midnights, days and times of day at the edges" {
    try testing.expectEqual(@as(i64, 0), dayMicros(0));
    try testing.expectEqual(@as(i64, -us_per_day), dayMicros(-1));
    try testing.expectEqual(@as(i64, std.math.maxInt(i64)), dayMicros(106_751_992));
    try testing.expectEqual(@as(i64, std.math.minInt(i64)), dayMicros(-106_751_992));
    try testing.expectEqual(@as(i64, 106_751_991 * us_per_day), dayMicros(106_751_991));
    try testing.expectEqual(@as(?i64, 106_751_991 * us_per_day), dayMicrosChecked(106_751_991));
    try testing.expectEqual(@as(?i64, null), dayMicrosChecked(106_751_992));
    try testing.expectEqual(@as(?i64, null), dayMicrosChecked(-106_751_992));
    try testing.expectEqual(@as(?i64, null), dayMicrosChecked(daysFromCivil(300000, 1, 1)));
    try testing.expectEqual(@as(i64, -1), dayOf(-1));
    try testing.expectEqual(@as(i64, us_per_day - 1), timeOfDay(-1));
    try testing.expectEqual(@as(i64, 0), dayOf(us_per_day - 1));
    try testing.expectEqual(@as(i64, 1), dayOf(us_per_day));
    try testing.expectEqual(@as(i64, 0), timeOfDay(us_per_day));
    for ([_]i64{ std.math.minInt(i64), std.math.maxInt(i64), -1, 0, 1 }) |us| {
        try testing.expectEqual(us, dayOf(us) *% us_per_day +% timeOfDay(us));
    }
}

test "civil: fields of days, seconds and microseconds" {
    try testing.expectEqual(Fields{ .y = 1970, .m = 1, .d = 1 }, fieldsOfDays(0));
    const us = (daysFromCivil(2024, 2, 29) * 86_400 + 13 * 3600 + 4 * 60 + 5) * us_per_sec + 678;
    try testing.expectEqual(Fields{ .y = 2024, .m = 2, .d = 29, .h = 13, .mi = 4, .s = 5, .us = 678 }, fieldsOfMicros(us));
    try testing.expectEqual(Fields{ .y = 1969, .m = 12, .d = 31, .h = 23, .mi = 59, .s = 59, .us = 999_999 }, fieldsOfMicros(-1));
    try testing.expectEqual(Fields{ .y = 1969, .m = 12, .d = 31, .h = 23, .mi = 59, .s = 59 }, fieldsOfSecs(-1));
    try testing.expectEqual(Fields{ .y = 1994, .m = 11, .d = 6, .h = 8, .mi = 49, .s = 37 }, fieldsOfSecs(784111777));
    try testing.expectEqual(Fields{ .y = -290308, .m = 12, .d = 21, .h = 19, .mi = 59, .s = 5, .us = 224192 }, fieldsOfMicros(std.math.minInt(i64)));
    try testing.expectEqual(Fields{ .y = 294247, .m = 1, .d = 10, .h = 4, .mi = 0, .s = 54, .us = 775807 }, fieldsOfMicros(std.math.maxInt(i64)));
    var rnd = std.Random.DefaultPrng.init(11);
    for (0..20_000) |_| {
        const x = rnd.random().int(i64);
        const f = fieldsOfMicros(x);
        const back = (daysFromCivil(f.y, f.m, f.d) *% 86_400 +% f.h * 3600 + f.mi * 60 + f.s) *% us_per_sec +% f.us;
        try testing.expectEqual(x, back);
        const fs = fieldsOfSecs(@divFloor(x, us_per_sec));
        try testing.expectEqual(Fields{ .y = f.y, .m = f.m, .d = f.d, .h = f.h, .mi = f.mi, .s = f.s }, fs);
    }
}

test "civil: civilDays, clockMicros and pivotYY refuse what does not exist" {
    try testing.expectEqual(@as(?i64, 19727), civilDays(2024, 1, 5));
    try testing.expectEqual(@as(?i64, daysFromCivil(-43, 3, 15)), civilDays(-43, 3, 15));
    try testing.expectEqual(@as(?i64, daysFromCivil(2024, 2, 29)), civilDays(2024, 2, 29));
    for ([_][3]i64{ .{ 2023, 2, 29 }, .{ 1900, 2, 29 }, .{ 2024, 4, 31 }, .{ 2024, 0, 1 }, .{ 2024, 13, 1 }, .{ 2024, 1, 0 }, .{ 2024, -1, 1 }, .{ 2024, 1, -5 }, .{ max_year + 1, 1, 1 }, .{ -max_year - 1, 1, 1 }, .{ std.math.maxInt(i64), 1, 1 } }) |c| {
        try testing.expectEqual(@as(?i64, null), civilDays(c[0], c[1], c[2]));
    }
    try testing.expect(civilDays(max_year, 12, 31) != null and civilDays(-max_year, 1, 1) != null);
    try testing.expectEqual(@as(?i64, 3_723_000_004), clockMicros(1, 2, 3, 4));
    try testing.expectEqual(@as(?i64, us_per_day - 1), clockMicros(23, 59, 59, 999_999));
    try testing.expectEqual(@as(?i64, 0), clockMicros(0, 0, 0, 0));
    for ([_][4]i64{ .{ 24, 0, 0, 0 }, .{ 0, 60, 0, 0 }, .{ 0, 0, 60, 0 }, .{ 0, 0, 0, 1_000_000 }, .{ -1, 0, 0, 0 }, .{ 0, 0, 0, -1 } }) |c| {
        try testing.expectEqual(@as(?i64, null), clockMicros(c[0], c[1], c[2], c[3]));
    }
    try testing.expectEqual(@as(i64, 2000), pivotYY(0));
    try testing.expectEqual(@as(i64, 2068), pivotYY(68));
    try testing.expectEqual(@as(i64, 1969), pivotYY(69));
    try testing.expectEqual(@as(i64, 1999), pivotYY(99));
}

test "civil: ticks past i64 microseconds overflow in every unit, never clamp (D2)" {
    const max: i64 = std.math.maxInt(i64);
    const min: i64 = std.math.minInt(i64);
    const Case = struct { Tick, i64, ?i64 };
    const cases = [_]Case{
        .{ .s, 1, 1_000_000 },
        .{ .s, -1, -1_000_000 },
        .{ .s, max / 1_000_000, max / 1_000_000 * 1_000_000 },
        .{ .s, max / 1_000_000 + 1, null },
        .{ .s, min / 1_000_000, min / 1_000_000 * 1_000_000 },
        .{ .s, min / 1_000_000 - 1, null },
        .{ .ms, 1_583_298_367_123, 1_583_298_367_123_000 },
        .{ .ms, max / 1000, max / 1000 * 1000 },
        .{ .ms, max / 1000 + 1, null },
        .{ .ms, min / 1000 - 1, null },
        .{ .ms, min, null },
        .{ .us, max, max },
        .{ .us, min, min },
        .{ .ns, 1_999, 1 },
        .{ .ns, -1, -1 },
        .{ .ns, -1000, -1 },
        .{ .ns, -1001, -2 },
        .{ .ns, max, @divFloor(max, 1000) },
        .{ .ns, min, @divFloor(min, 1000) },
    };
    for (cases) |c| {
        if (c[2]) |want| {
            try testing.expectEqual(want, try c[0].toMicros(c[1]));
        } else try testing.expectError(error.Overflow, c[0].toMicros(c[1]));
    }
    var rnd = std.Random.DefaultPrng.init(3);
    for (0..50_000) |i| {
        const x = rnd.random().int(i64) >> @intCast(i % 64);
        inline for (.{ .s, .ms, .us, .ns }) |t| {
            const fits = x >= -Tick.limit(t) and x <= Tick.limit(t);
            if (Tick.toMicros(t, x)) |_| try testing.expect(fits) else |_| try testing.expect(!fits);
        }
        try testing.expectEqual(@divFloor(x, 1000), try Tick.ns.toMicros(x));
    }
    try testing.expectEqual(@as(u2, 0), @intFromEnum(Tick.s));
    try testing.expectEqual(@as(u2, 3), @intFromEnum(Tick.ns));
}

test "civil: Part.parse reads every unit singular or plural, in any case" {
    for ([_]struct { []const u8, Part }{ .{ "year", .year }, .{ "MONTH", .month }, .{ "Week", .week }, .{ "day", .day }, .{ "hour", .hour }, .{ "minute", .minute }, .{ "SeCoNd", .second } }) |c| {
        try testing.expectEqual(@as(?Part, c[1]), Part.parse(c[0]));
    }
    for ([_]struct { []const u8, Part }{ .{ "years", .year }, .{ "Days", .day }, .{ "HOURS", .hour }, .{ "minutes", .minute }, .{ "seconds", .second }, .{ "weeks", .week }, .{ "months", .month } }) |c| {
        try testing.expectEqual(@as(?Part, c[1]), Part.parse(c[0]));
    }
    for ([_][]const u8{ "", "s", "dayss", "secondss", "millisecond", "d", " day", "day ", "aaaaaaaaaaaaaaaa", "dayssssssssssssss" }) |bad| {
        try testing.expectEqual(@as(?Part, null), Part.parse(bad));
    }
    try testing.expectEqual(@as(?i64, null), Part.year.fixedMicros());
    try testing.expectEqual(@as(?i64, null), Part.month.fixedMicros());
    try testing.expectEqual(@as(?i64, 604_800_000_000), Part.week.fixedMicros());
    try testing.expectEqual(@as(?i64, 86_400_000_000), Part.day.fixedMicros());
    try testing.expectEqual(@as(?i64, 3_600_000_000), Part.hour.fixedMicros());
    try testing.expectEqual(@as(?i64, 60_000_000), Part.minute.fixedMicros());
    try testing.expectEqual(@as(?i64, 1_000_000), Part.second.fixedMicros());
}

fn testDay(y: i64, m: u32, d: u32) i64 {
    return daysFromCivil(y, m, d);
}

fn testStamp(y: i64, m: u32, d: u32, h: i64, mi: i64, s: i64, us: i64) i64 {
    return daysFromCivil(y, m, d) * us_per_day + (h * 3600 + mi * 60 + s) * us_per_sec + us;
}

const Golden = struct { []const u8, ?i64 };

fn expectGolden(comptime f: fn ([]const u8) ?i64, cases: []const Golden) !void {
    for (cases) |c| {
        testing.expectEqual(c[1], f(c[0])) catch |e| {
            std.debug.print("input `{s}`\n", .{c[0]});
            return e;
        };
    }
}

test "civil: Text(.iso) pins parseIsoDate, parseIsoTimestamp and parseIsoTime" {
    const T = Text(.iso);
    try expectGolden(T.date, &.{
        .{ "2024-01-05", testDay(2024, 1, 5) },       .{ " 2024-01-05\t", testDay(2024, 1, 5) },
        .{ "2000-02-29", testDay(2000, 2, 29) },      .{ "0000-01-01", testDay(0, 1, 1) },
        .{ "9999-12-31", testDay(9999, 12, 31) },     .{ "1900-02-29", null },
        .{ "2023-02-29", null },                      .{ "2024-04-31", null },
        .{ "2024-1-05", null },                       .{ "2024-01-5", null },
        .{ "10000-01-01", testDay(10000, 1, 1) },     .{ "+024-01-05", null },
        .{ "-0001-01-01", testDay(-1, 1, 1) },        .{ "-12345-06-07", testDay(-12345, 6, 7) },
        .{ "5000000-01-01", testDay(5000000, 1, 1) }, .{ "9999999-12-31", null },
        .{ "-9999999-01-01", null },                  .{ "12345678-01-01", null },
        .{ "--0001-01-01", null },                    .{ "0044-03-15 BC", null },
        .{ "01234-05-06", null },                     .{ "-0000-01-01", null },
        .{ "0044-03-15", testDay(44, 3, 15) },        .{ "-01234-05-06", null },
        .{ "2024-01-05 12:00:00", null },             .{ "01/07/2013", null },
        .{ "2024-13-01", null },                      .{ "", null },
    });
    try expectGolden(T.timestamp, &.{
        .{ "2024-01-05", testStamp(2024, 1, 5, 0, 0, 0, 0) },
        .{ "2024-01-05 12:34:56", testStamp(2024, 1, 5, 12, 34, 56, 0) },
        .{ "2024-01-05T12:34:56", testStamp(2024, 1, 5, 12, 34, 56, 0) },
        .{ "2024-01-05X12:34:56", testStamp(2024, 1, 5, 12, 34, 56, 0) },
        .{ "2024-01-05 12:34:56.123456", testStamp(2024, 1, 5, 12, 34, 56, 123456) },
        .{ "2024-01-05 12:34:56.1", testStamp(2024, 1, 5, 12, 34, 56, 100000) },
        .{ " 2024-01-05 23:59:59.999999 ", testStamp(2024, 1, 5, 23, 59, 59, 999999) },
        .{ "2024-01-05 12:34:56.1234567", testStamp(2024, 1, 5, 12, 34, 56, 123456) },
        .{ "2024-01-05 12:34:56.", null },
        .{ "2024-01-05 12:34:56.12x", null },
        .{ "2024-01-05 12:34", null },
        .{ "2024-01-05 1:02:03", null },
        .{ "2024-01-05 24:00:00", null },
        .{ "2024-01-05 12:60:00", null },
        .{ "2024-01-05 12:34:56Z", null },
        .{ "2024-01-05 12:34:56+05", null },
        .{ "2024-02-30 10:00:00", null },
    });
    try expectGolden(T.time, &.{
        .{ "01:02:03", 3_723_000_000 },          .{ "12:30", 45_000_000_000 },
        .{ " 12:30 ", 45_000_000_000 },          .{ "23:59:59.999999", 86_399_999_999 },
        .{ "00:00:00.5", 500_000 },              .{ "12:30:00.1234567", 45_000_123_456 },
        .{ "23:59:59.9999999", 86_399_999_999 }, .{ "1:02:03", null },
        .{ "24:00:00", null },                   .{ "01:02:03.", null },
        .{ "01:02:03 extra", null },             .{ "12:30.5", null },
        .{ "12:3", null },                       .{ "01:02:03+05", null },
        .{ "", null },
    });
}

test "civil: Text(.pg_text) pins parseDateText and parseTimestampText" {
    const T = Text(.pg_text);
    const day = testDay(2024, 1, 5);
    const noon = testStamp(2024, 1, 5, 12, 0, 0, 0);
    try expectGolden(T.date, &.{
        .{ "2024-01-05", day },              .{ "2024-1-5", day },
        .{ "2024-01-05 12:00:00", day },     .{ "2024-01-05T00:00:00Z", day },
        .{ "2024-01-05 23:00-05", day },     .{ " 2024-01-05 ", day },
        .{ "01/05/2024", null },             .{ "2024-02-30", null },
        .{ "2024-01-05 25:00", null },       .{ "2024-01-05x12:00", null },
        .{ "24-01-05", null },               .{ "02024-01-05", day },
        .{ "-0001-1-5", testDay(-1, 1, 5) }, .{ "11000-01-01", testDay(11000, 1, 1) },
        .{ "2024-01-05 12:00 UTC", null },   .{ "2024-001-05", null },
    });
    try expectGolden(T.timestamp, &.{
        .{ "2024-01-05 12:00:00", noon },
        .{ "2024-1-5T12:00", noon },
        .{ "2024-01-05 12:00:00Z", noon },
        .{ "2024-01-05 12:00:00z", noon },
        .{ "2024-01-05 17:00:00+05:00", noon },
        .{ "2024-01-05 09:30-0230", noon },
        .{ "2024-01-05 14:00+02", noon },
        .{ "2024-01-05 17:00:00+05:", noon },
        .{ "2024-01-05 12:00:00.25", noon + 250_000 },
        .{ "2024-01-05 12:00:00.1234567", noon + 123_456 },
        .{ "2024-01-05 1:02", testStamp(2024, 1, 5, 1, 2, 0, 0) },
        .{ "2024-01-05", day * us_per_day },
        .{ "2024-01-05 12", null },
        .{ "2024-01-05 12:00:00.", null },
        .{ "2024-01-05 12:00.5", null },
        .{ "2024-01-05 12:00 UTC", null },
        .{ "2024-01-05 12:00+16", null },
        .{ "2024-01-05 12:00+05:60", null },
        .{ "2024-01-05 12:00+5", null },
        .{ "2024-01-05 12:00Zx", null },
        .{ "2024-01-05 24:00", null },
    });
    try expectGolden(T.time, &.{
        .{ "1:02", 3_720_000_000 }, .{ "01:02:03.1234567", 3_723_123_456 },
        .{ "24:00", null },         .{ "01:02:03Z", null },
    });
}

test "civil: Text(.wire) pins cell's wireDate and wireDatetime" {
    const T = Text(.wire);
    const base = testStamp(2024, 1, 5, 5, 0, 0, 0);
    const noon = testStamp(2024, 1, 5, 12, 0, 0, 0);
    try expectGolden(T.date, &.{
        .{ "1970-01-01", 0 },
        .{ "2024-01-05", 19727 },
        .{ "0044-03-15 BC", testDay(-43, 3, 15) },
        .{ "10000-01-01", testDay(10000, 1, 1) },
        .{ "1234567-01-01", testDay(1234567, 1, 1) },
        .{ "0000-01-01", testDay(0, 1, 1) },
        .{ "0004-02-29 BC", null },
        .{ "0001-02-29 BC", testDay(0, 2, 29) },
        .{ "-0001-01-01", null },
        .{ "9999999-01-01", null },
        .{ "12345678-01-01", null },
        .{ "2024-01-05 12:00:00", null },
        .{ " 2024-01-05", null },
        .{ "2024-1-05", null },
        .{ "24-01-05", null },
        .{ "2024/01/05", null },
        .{ "infinity", null },
        .{ "2024-13-01", null },
        .{ "", null },
    });
    try expectGolden(T.timestamp, &.{
        .{ "2024-01-05", testStamp(2024, 1, 5, 0, 0, 0, 0) },
        .{ "2024-01-05 05:00:00", base },
        .{ "2024-01-05T05:00:00", base },
        .{ "2024-01-05 02:00:00-03", base },
        .{ "2024-01-05 08:00:00+03:00", base },
        .{ "2024-01-05 10:45:00 +05:45", base },
        .{ "2024-01-05 10:45:00+0545", base },
        .{ "2024-01-05 10:00:00+a5", base },
        .{ "2024-01-05 12:00:00.1234567", noon + 123_456 },
        .{ "2024-01-05 12:00:00.", noon },
        .{ "2024-01-05 12:00:00xyz", noon },
        .{ "2024-01-05 12:00:00Z", noon },
        .{ "2024-01-05 25:00:00", testStamp(2024, 1, 6, 1, 0, 0, 0) },
        .{ "2024-01-05 12:00:00 BC", testStamp(-2023, 1, 5, 12, 0, 0, 0) },
        .{ "294247-01-10 04:00:54.775807", std.math.maxInt(i64) },
        .{ "294247-01-10 04:00:54.775808", null },
        .{ "300000-01-01", null },
        .{ "2024-01-05 5:00", null },
        .{ "2024-01-05 12:00", null },
        .{ "2024-01-05x12:00:00", null },
        .{ "infinity", null },
    });
}

test "civil: Text parses what writeLayout writes, over the i32 day range; iso and pg_text read back every year (D3)" {
    const Iso = Text(.iso);
    const Pg = Text(.pg_text);
    const Wire = Text(.wire);
    var rnd = std.Random.DefaultPrng.init(17);
    const r = rnd.random();
    var buf: [64]u8 = undefined;
    for (0..200_000) |i| {
        const days: i64 = switch (i) {
            0 => std.math.minInt(i32),
            1 => std.math.maxInt(i32),
            2 => epoch.ce1,
            3 => daysFromCivil(9999, 12, 31),
            else => if (i % 2 == 0) r.int(i32) else r.intRangeAtMost(i64, daysFromCivil(0, 1, 1), daysFromCivil(9999, 12, 31)),
        };
        var w = std.Io.Writer.fixed(&buf);
        try writeLayout(&w, days, .date);
        const text = w.buffered();
        const y = civilFromDays(days).y;
        try testing.expectEqual(@as(?i64, days), Iso.date(text));
        try testing.expectEqual(@as(?i64, days), Pg.date(text));
        try testing.expectEqual(if (y >= 0) @as(?i64, days) else null, Wire.date(text));
    }
    for (0..200_000) |i| {
        const us: i64 = if (i == 0) std.math.maxInt(i64) else if (i % 2 == 0) r.int(i64) else r.intRangeAtMost(i64, testStamp(0, 1, 1, 0, 0, 0, 0), testStamp(9999, 12, 31, 23, 59, 59, 999_999));
        var w = std.Io.Writer.fixed(&buf);
        try writeLayout(&w, us, .timestamp);
        const text = w.buffered();
        const y = civilFromDays(dayOf(us)).y;
        try testing.expectEqual(@as(?i64, us), Iso.timestamp(text));
        try testing.expectEqual(@as(?i64, us), Pg.timestamp(text));
        try testing.expectEqual(if (y >= 0) @as(?i64, us) else null, Wire.timestamp(text));
        const tod = timeOfDay(us);
        w = std.Io.Writer.fixed(&buf);
        try writeLayout(&w, tod, .time);
        try testing.expectEqual(@as(?i64, tod), Iso.time(w.buffered()));
        try testing.expectEqual(@as(?i64, tod), Pg.time(w.buffered()));
        try testing.expectEqual(@as(?i64, tod), Wire.time(w.buffered()));
    }
}

fn expectLayout(comptime l: Layout, x: i64, want: []const u8) !void {
    var buf: [80]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try writeLayout(&w, x, l);
    try testing.expectEqualStrings(want, w.buffered());
}

test "civil: writeLayout's default layouts match format.zig byte for byte" {
    try expectLayout(.date, 0, "1970-01-01");
    try expectLayout(.date, -1, "1969-12-31");
    try expectLayout(.date, -1_000_000, "-0768-02-04");
    try expectLayout(.date, -719_529, "-0001-12-31");
    try expectLayout(.date, -719_530, "-0001-12-30");
    try expectLayout(.date, -719_528, "0000-01-01");
    try expectLayout(.date, testDay(10000, 1, 1), "10000-01-01");
    try expectLayout(.date, std.math.maxInt(i32), "5881580-07-11");
    try expectLayout(.date, std.math.minInt(i32), "-5877641-06-23");
    try expectLayout(.timestamp, 0, "1970-01-01 00:00:00");
    try expectLayout(.timestamp, us_per_day + (1 * 3600 + 2 * 60 + 3) * us_per_sec, "1970-01-02 01:02:03");
    try expectLayout(.timestamp, -1_000_000 * us_per_day + 1, "-0768-02-04 00:00:00.000001");
    try expectLayout(.timestamp, std.math.minInt(i64), "-290308-12-21 19:59:05.224192");
    try expectLayout(.timestamp, std.math.maxInt(i64), "294247-01-10 04:00:54.775807");
    try expectLayout(.time, 0, "00:00:00");
    try expectLayout(.time, us_per_day - 1, "23:59:59.999999");
    try expectLayout(.time, -1, "23:59:59.999999");
    try expectLayout(.time, us_per_day + 1_500_000, "00:00:01.500000");
    var rnd = std.Random.DefaultPrng.init(5);
    const r = rnd.random();
    var a: [80]u8 = undefined;
    var b: [80]u8 = undefined;
    for (0..200_000) |i| {
        var fa = std.Io.Writer.fixed(&a);
        var fb = std.Io.Writer.fixed(&b);
        switch (i % 3) {
            0 => {
                const days = r.intRangeAtMost(i64, -1_000_000, 3_500_000);
                try writeLayout(&fa, days, .date);
                const c = civilFromDays(days);
                if (c.y < 0) try fb.writeByte('-');
                try fb.print("{d:0>4}-{d:0>2}-{d:0>2}", .{ @abs(c.y), c.m, c.d });
            },
            1 => {
                const us = r.intRangeAtMost(i64, -50_000_000_000_000_000, 250_000_000_000_000_000);
                const t = if (i % 2 == 0) @divFloor(us, 1_000_000) * 1_000_000 else us;
                try writeLayout(&fa, t, .timestamp);
                const days = @divFloor(t, us_per_day);
                const rest: u64 = @intCast(t - days * us_per_day);
                const secs = rest / 1_000_000;
                const frac = rest % 1_000_000;
                const c = civilFromDays(days);
                if (c.y < 0) try fb.writeByte('-');
                try fb.print("{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}", .{ @abs(c.y), c.m, c.d, secs / 3600, (secs % 3600) / 60, secs % 60 });
                if (frac != 0) try fb.print(".{d:0>6}", .{frac});
            },
            else => {
                const t = r.int(i64);
                try writeLayout(&fa, t, .time);
                const us: u64 = @intCast(@mod(t, us_per_day));
                const secs = us / 1_000_000;
                const frac = us % 1_000_000;
                try fb.print("{d:0>2}:{d:0>2}:{d:0>2}", .{ secs / 3600, (secs % 3600) / 60, secs % 60 });
                if (frac != 0) try fb.print(".{d:0>6}", .{frac});
            },
        }
        try testing.expectEqualStrings(fb.buffered(), fa.buffered());
    }
}

test "civil: the PostgreSQL layouts spell a year before 1 with BC, and the wire reads it back (D3)" {
    try expectLayout(.pg_date, testDay(2024, 1, 5), "2024-01-05");
    try expectLayout(.pg_date, testDay(1, 1, 1), "0001-01-01");
    try expectLayout(.pg_date, testDay(0, 2, 29), "0001-02-29 BC");
    try expectLayout(.pg_date, testDay(-43, 3, 15), "0044-03-15 BC");
    try expectLayout(.pg_date, testDay(12345, 6, 7), "12345-06-07");
    try expectLayout(.pg_timestamp, testStamp(-43, 3, 15, 12, 0, 0, 5), "0044-03-15 12:00:00.000005 BC");
    try expectLayout(.pg_timestamp, testStamp(2024, 1, 5, 12, 0, 0, 0), "2024-01-05 12:00:00");
    const W = Text(.wire);
    var buf: [80]u8 = undefined;
    for ([_]i64{ testDay(0, 2, 29), testDay(-4712, 1, 1), testDay(-3, 3, 1), testDay(1, 1, 1), testDay(2024, 2, 29) }) |d| {
        var w = std.Io.Writer.fixed(&buf);
        try writeLayout(&w, d, .pg_date);
        try testing.expectEqual(@as(?i64, d), W.date(w.buffered()));
        w = std.Io.Writer.fixed(&buf);
        try writeLayout(&w, d * us_per_day + 1, .pg_timestamp);
        try testing.expectEqual(@as(?i64, d * us_per_day + 1), W.timestamp(w.buffered()));
    }
}

test "civil: writeLayout's Delta, S3, Kerberos, AAD, xlsx and HTTP layouts match their writers" {
    const noon_ms = testStamp(2024, 1, 5, 12, 34, 56, 789_012);
    try expectLayout(.delta_timestamp, noon_ms, "2024-01-05 12:34:56.789012");
    try expectLayout(.delta_timestamp_ms, noon_ms, "2024-01-05 12:34:56.789");
    try expectLayout(.delta_timestamp_utc, noon_ms, "2024-01-05T12:34:56.789012Z");
    try expectLayout(.delta_timestamp_utc_ms, noon_ms, "2024-01-05T12:34:56.789Z");
    try expectLayout(.delta_timestamp, 0, "1970-01-01 00:00:00.000000");
    try expectLayout(.delta_date, -719162, "0001-01-01");
    var buf: [80]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try testing.expectError(error.YearOutOfRange, writeLayout(&w, -719163, .delta_date));
    try testing.expectError(error.YearOutOfRange, writeLayout(&w, testDay(10000, 1, 1), .delta_date));
    try testing.expectError(error.YearOutOfRange, writeLayout(&w, testStamp(0, 12, 31, 23, 59, 59, 0), .delta_timestamp_utc));
    try expectLayout(.amz, testStamp(2013, 5, 24, 0, 0, 0, 0), "20130524T000000Z");
    try expectLayout(.kerberos, testStamp(2013, 5, 24, 1, 2, 3, 0), "20130524010203Z");
    try expectLayout(.aad, testStamp(2026, 10, 25, 12, 0, 1, 0), "2026-10-25T12:00:01.000Z");
    try expectLayout(.xlsx_date, testDay(1904, 1, 1), "1904-01-01");
    try expectLayout(.xlsx_date, testDay(-5, 3, 1), "0000-03-01");
    try expectLayout(.xlsx_time, 45_296_000_000, "12:34:56");
    try expectLayout(.xlsx_time, 45_296_789_999, "12:34:56.789");
    try expectLayout(.xlsx_time, 45_296_000_999, "12:34:56");
    w = std.Io.Writer.fixed(&buf);
    try writeRfc1123(&w, 784111777);
    try testing.expectEqualStrings("Sun, 06 Nov 1994 08:49:37 GMT", w.buffered());
    var rnd = std.Random.DefaultPrng.init(9);
    const r = rnd.random();
    var a: [80]u8 = undefined;
    var b: [80]u8 = undefined;
    for (0..50_000) |i| {
        var fa = std.Io.Writer.fixed(&a);
        var fb = std.Io.Writer.fixed(&b);
        const us = r.intRangeAtMost(i64, testStamp(1, 1, 1, 0, 0, 0, 0), testStamp(9999, 12, 31, 23, 59, 59, 999_999));
        const secs = @divFloor(us, us_per_sec);
        const day = @divFloor(us, us_per_day);
        const rest: u64 = @intCast(@mod(us, us_per_day));
        const c = civilFromDays(day);
        const y: u64 = @intCast(c.y);
        const sday: u64 = @intCast(@mod(secs, 86_400));
        switch (i % 7) {
            0 => {
                const utc = r.boolean();
                const ms = r.boolean();
                switch (@as(u2, @intFromBool(utc)) * 2 + @intFromBool(ms)) {
                    0 => try writeLayout(&fa, us, .delta_timestamp),
                    1 => try writeLayout(&fa, us, .delta_timestamp_ms),
                    2 => try writeLayout(&fa, us, .delta_timestamp_utc),
                    3 => try writeLayout(&fa, us, .delta_timestamp_utc_ms),
                }
                try fb.print("{d:0>4}-{d:0>2}-{d:0>2}{c}{d:0>2}:{d:0>2}:{d:0>2}", .{ y, c.m, c.d, @as(u8, if (utc) 'T' else ' '), rest / 3_600_000_000, rest / 60_000_000 % 60, rest / 1_000_000 % 60 });
                if (ms) try fb.print(".{d:0>3}", .{rest / 1000 % 1000}) else try fb.print(".{d:0>6}", .{rest % 1_000_000});
                if (utc) try fb.writeByte('Z');
            },
            1 => {
                try writeLayout(&fa, day, .delta_date);
                try fb.print("{d:0>4}-{d:0>2}-{d:0>2}", .{ y, c.m, c.d });
            },
            2 => {
                try writeLayout(&fa, secs * us_per_sec, .amz);
                try fb.print("{d:0>4}{d:0>2}{d:0>2}T{d:0>2}{d:0>2}{d:0>2}Z", .{ y, c.m, c.d, sday / 3600, (sday % 3600) / 60, sday % 60 });
            },
            3 => {
                try writeLayout(&fa, secs * us_per_sec, .kerberos);
                try fb.print("{d:0>4}{d:0>2}{d:0>2}{d:0>2}{d:0>2}{d:0>2}Z", .{ y, c.m, c.d, sday / 3600, (sday / 60) % 60, sday % 60 });
            },
            4 => {
                const s2 = @max(secs, 0);
                try writeLayout(&fa, s2 * us_per_sec, .aad);
                const es = std.time.epoch.EpochSeconds{ .secs = @intCast(s2) };
                const yd = es.getEpochDay().calculateYearDay();
                const md = yd.calculateMonthDay();
                const ds = es.getDaySeconds();
                try fb.print("{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.000Z", .{
                    yd.year,              md.month.numeric(),      md.day_index + 1,
                    ds.getHoursIntoDay(), ds.getMinutesIntoHour(), ds.getSecondsIntoMinute(),
                });
            },
            5 => {
                const t: u64 = rest;
                try writeLayout(&fa, @intCast(t), .xlsx_time);
                const s = t / 1_000_000;
                const ms = (t % 1_000_000) / 1000;
                if (ms == 0) try fb.print("{d:0>2}:{d:0>2}:{d:0>2}", .{ s / 3600, s / 60 % 60, s % 60 }) else try fb.print("{d:0>2}:{d:0>2}:{d:0>2}.{d:0>3}", .{ s / 3600, s / 60 % 60, s % 60, ms });
            },
            else => {
                try writeRfc1123(&fa, secs);
                const day_names = [_][]const u8{ "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat" };
                const mon_names = [_][]const u8{ "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };
                const days = @divFloor(secs, 86400);
                const dow: usize = @intCast(@mod(days + 4, 7));
                try fb.print("{s}, {d:0>2} {s} {d:0>4} {d:0>2}:{d:0>2}:{d:0>2} GMT", .{ day_names[dow], c.d, mon_names[c.m - 1], @as(u32, @intCast(c.y)), sday / 3600, (sday % 3600) / 60, sday % 60 });
            },
        }
        try testing.expectEqualStrings(fb.buffered(), fa.buffered());
    }
    for (0..20_000) |_| {
        const days = r.intRangeAtMost(i64, -1_000_000, 3_500_000);
        var fa = std.Io.Writer.fixed(&a);
        var fb = std.Io.Writer.fixed(&b);
        try writeLayout(&fa, days, .xlsx_date);
        const c = civilFromDays(days);
        try fb.print("{d:0>4}-{d:0>2}-{d:0>2}", .{ @as(u64, @intCast(@max(c.y, 0))), c.m, c.d });
        try testing.expectEqualStrings(fb.buffered(), fa.buffered());
    }
}

fn krb5Days(y: i64, mo: i64, d: i64) i64 {
    const yy = if (mo <= 2) y - 1 else y;
    const era = @divFloor(if (yy >= 0) yy else yy - 399, 400);
    const yoe = yy - era * 400;
    const mp = if (mo > 2) mo - 3 else mo + 9;
    const doy = @divFloor(153 * mp + 2, 5) + d - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146097 + doe - 719468;
}

test "civil: krb5's own days-from-civil agrees for years 0 and up and drifts before" {
    var y: i64 = 0;
    while (y <= 3000) : (y += 1) for (1..13) |m| {
        try testing.expectEqual(daysFromCivil(y, @intCast(m), 28), krb5Days(y, @intCast(m), 28));
    };
    try testing.expectEqual(daysFromCivil(-1, 3, 1), krb5Days(-1, 3, 1));
    try testing.expect(krb5Days(-2, 3, 1) != daysFromCivil(-2, 3, 1));
    try testing.expect(krb5Days(-400, 3, 1) != daysFromCivil(-400, 3, 1));
}
