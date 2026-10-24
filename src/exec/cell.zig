//! Text cells into columns, the step every text reader shares: CSV and XML cells
//! and the values of PostgreSQL's and MySQL's text protocol. A number, decimal or
//! date reads through one parser wherever it comes from; each reader keeps only its
//! framing and the `Rules` of its source, which differ on purpose — a sniffed CSV
//! column takes a leading zero for a code, while a database has already typed its
//! value and may spell a date `0044-03-15 BC`.
//!
//! `converter` picks, once per column, the function that parses a cell of that
//! column's type and appends it straight into the builder's storage, one
//! instantiated per type and rules at compile time, so the per-cell work is the
//! parse and a typed append with no `Value` between them.
//!
//! Binary cells share less, since each wire lays its numbers out its own way, but
//! a decimal's unscaled integer comes in two layouts that several readers decode:
//! big-endian two's complement (Avro, Parquet) and a sign beside a little-endian
//! magnitude (SQL Server's TDS).

const std = @import("std");
const types = @import("../lang/types.zig");
const column = @import("column.zig");
const Decimal = @import("value.zig").Decimal;
const cast = @import("eval/cast.zig");
const time = @import("eval/time.zig");

pub const Error = error{ TextTypeMismatch, UnparseableNumber, OutOfMemory };

/// How a source spells its values. `typed` lists the kinds parsed; a column of any
/// other kind takes the cell as text.
pub const Rules = struct {
    typed: []const types.TypeKind,
    /// A `+` or a leading zero (`007`, `-01`) marks a code, not a number, as a
    /// sniffer types it: `01310100` is never read silently as 1310100.
    codes: bool = false,
    /// Numbers past the fast paths: `strict` takes no `_` or hex, as CAST reads
    /// text; `std` takes what `std.fmt` does, for a database's own output.
    numbers: enum { strict, std } = .strict,
    /// Spaces around an integer are dropped, as a CHAR column pads one.
    trim_ints: bool = false,
    /// `iso`: `YYYY-MM-DD`, as a sniffer accepts one. `wire`: a database's text,
    /// with five-digit and BC years, a time, a fraction and an offset folded into
    /// UTC, and MySQL's zero date read as NULL.
    dates: enum { iso, wire } = .iso,
    /// What a cell its column cannot read fails with.
    mismatch: Error = error.TextTypeMismatch,
};

/// A sniffed text source's cells: CSV and XML.
pub const sniffed = Rules{ .typed = &.{ .int, .float, .date }, .codes = true };

/// A database's text protocol: PostgreSQL and MySQL.
pub const wire = Rules{
    .typed = &.{ .int, .float, .decimal, .bool, .date, .timestamp },
    .numbers = .std,
    .trim_ints = true,
    .dates = .wire,
    .mismatch = error.UnparseableNumber,
};

/// Appends one non-NULL cell to a builder of the column's type.
pub const Converter = *const fn (b: *column.Builder, raw: []const u8) Error!void;

/// The converter of a column of `kind` under `rules`.
pub fn converter(kind: types.TypeKind, comptime rules: Rules) Converter {
    return switch (kind) {
        inline else => |k| &struct {
            fn f(b: *column.Builder, raw: []const u8) Error!void {
                const as = comptime readAs(k, rules);
                const v = try parse(as, rules, raw);
                switch (comptime as) {
                    .int => try b.appendInt(v),
                    .float => try b.appendFloat(v),
                    .decimal => try b.appendAs(.dec, v),
                    .bool => try b.appendAs(.b, v),
                    .date => if (v) |x| try b.appendAs(.i32, x) else try b.appendNull(),
                    .timestamp => if (v) |x| try b.appendInt(x) else try b.appendNull(),
                    else => try b.appendStr(v),
                }
            }
        }.f,
    };
}

/// The kind a column of `k` is parsed as under `rules`: itself, or text.
fn readAs(comptime k: types.TypeKind, comptime rules: Rules) types.TypeKind {
    for (rules.typed) |t| if (t == k) return k;
    return .string;
}

fn Parsed(comptime k: types.TypeKind) type {
    return switch (k) {
        .int => i64,
        .float => f64,
        .decimal => Decimal,
        .bool => bool,
        .date => ?i32,
        .timestamp => ?i64,
        else => []const u8,
    };
}

/// One non-NULL cell as a column of `k` under `rules` holds it; a NULL date or
/// timestamp is MySQL's zero date. A timestamp is read in the wire's spelling,
/// the only source that types one.
pub fn parse(comptime k: types.TypeKind, comptime rules: Rules, raw: []const u8) Error!Parsed(k) {
    const bad = rules.mismatch;
    return switch (k) {
        .int => int(rules, raw) orelse bad,
        .float => float(rules, raw) orelse bad,
        .decimal => decimal(raw) orelse bad,
        .bool => raw.len > 0 and (raw[0] == '1' or raw[0] == 't' or raw[0] == 'T' or raw[0] == 'y' or raw[0] == 'Y'),
        .date => switch (rules.dates) {
            .iso => @as(?i32, @intCast(time.parseIsoDate(raw) orelse return bad)),
            .wire => if (zeroDate(raw)) null else std.math.cast(i32, wireDate(raw) orelse return bad) orelse bad,
        },
        .timestamp => if (zeroDate(raw)) null else wireDatetime(raw) orelse bad,
        else => raw,
    };
}

/// Whether a non-empty cell reads as a column of `kind` under `rules`.
pub fn parses(kind: types.TypeKind, comptime rules: Rules, raw: []const u8) bool {
    return switch (kind) {
        inline else => |k| if (parse(comptime readAs(k, rules), rules, raw)) |_| true else |_| false,
    };
}

/// A `+` or a leading zero (`007`, `-01`) marks a code, not a number.
pub fn codeLooking(raw: []const u8) bool {
    return raw.len > 0 and (raw[0] == '+' or (raw.len > 1 and (raw[0] == '0' or (raw[0] == '-' and raw[1] == '0')) and std.mem.indexOfScalar(u8, raw, '.') == null));
}

pub fn int(comptime rules: Rules, raw: []const u8) ?i64 {
    if (rules.codes and codeLooking(raw)) return null;
    const s = if (rules.trim_ints) std.mem.trim(u8, raw, " ") else raw;
    if (intFast(s)) |v| return v;
    return switch (rules.numbers) {
        .strict => cast.parseIntStrict(s),
        .std => std.fmt.parseInt(i64, s, 10) catch null,
    };
}

pub fn float(comptime rules: Rules, raw: []const u8) ?f64 {
    if (rules.codes and codeLooking(raw)) return null;
    if (floatFast(raw)) |v| return v;
    return switch (rules.numbers) {
        .strict => cast.parseFloatStrict(raw),
        .std => std.fmt.parseFloat(f64, raw) catch null,
    };
}

/// `[-]digits`, at most 18 of them so no i64 overflow: what a cell needs of
/// `std.fmt.parseInt`, without its per-call base, underscore and sign handling.
pub fn intFast(s: []const u8) ?i64 {
    if (s.len == 0 or s.len > 19) return null;
    var i: usize = 0;
    const neg = s[0] == '-';
    if (neg) i = 1;
    if (s.len - i == 0 or s.len - i > 18) return null;
    var v: i64 = 0;
    while (i < s.len) : (i += 1) {
        const c = s[i];
        if (c < '0' or c > '9') return null;
        v = v * 10 + (c - '0');
    }
    return if (neg) -v else v;
}

/// `[-]digits[.digits]` with at most 15 significant digits, as an integer mantissa
/// over a power of ten: both exact doubles, so one division rounds correctly
/// (Clinger's fast path) and matches `std.fmt.parseFloat`. Anything else is null.
pub fn floatFast(s: []const u8) ?f64 {
    if (s.len == 0 or s.len > 18) return null;
    var i: usize = 0;
    const neg = s[0] == '-';
    if (neg) i = 1;
    var mant: u64 = 0;
    var digits: usize = 0;
    var frac: usize = 0;
    var seen_dot = false;
    while (i < s.len) : (i += 1) {
        const c = s[i];
        if (c == '.') {
            if (seen_dot) return null;
            seen_dot = true;
            continue;
        }
        if (c < '0' or c > '9') return null;
        mant = mant * 10 + (c - '0');
        digits += 1;
        if (seen_dot) frac += 1;
    }
    if (digits == 0 or digits > 15) return null;
    const pow10 = [_]f64{ 1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22 };
    if (frac >= pow10.len) return null;
    const v = @as(f64, @floatFromInt(mant)) / pow10[frac];
    return if (neg) -v else v;
}

/// A decimal literal: optional sign, digits, at most one `.`, at least one digit, else
/// null. It once skipped unknown characters, so `'1000,00'` read as 100000.00 and
/// `'abc'` as 0; comma-decimal data must be rewritten with `replace(x, ',', '.')`.
pub fn decimal(t: []const u8) ?Decimal {
    var s = t;
    var neg = false;
    if (s.len > 0 and (s[0] == '-' or s[0] == '+')) {
        neg = s[0] == '-';
        s = s[1..];
    }
    if (s.len == 0) return null;
    var unscaled: i128 = 0;
    var scale: u8 = 0;
    var digits: usize = 0;
    var after_dot = false;
    for (s) |c| {
        if (c == '.') {
            if (after_dot) return null;
            after_dot = true;
            continue;
        }
        if (c < '0' or c > '9') return null;
        unscaled = std.math.mul(i128, unscaled, 10) catch return null;
        unscaled = std.math.add(i128, unscaled, c - '0') catch return null;
        digits += 1;
        if (after_dot) scale += 1;
    }
    if (digits == 0) return null;
    return .{ .unscaled = if (neg) -unscaled else unscaled, .scale = scale };
}

/// A big-endian two's complement integer as an unscaled decimal; null when it is
/// wider than an i128 holds. Eight bytes or fewer take a 64-bit path.
pub fn bigEndianDecimal(b: []const u8) ?i128 {
    if (b.len == 0) return 0;
    const neg = b[0] & 0x80 != 0;
    if (b.len <= 8) {
        var acc: u64 = if (neg) std.math.maxInt(u64) else 0;
        for (b) |x| acc = (acc << 8) | x;
        return @as(i64, @bitCast(acc));
    }
    var start: usize = 0;
    if (b.len > 16) {
        start = b.len - 16;
        for (b[0..start]) |x| if (x != (if (neg) @as(u8, 0xFF) else 0)) return null;
        if ((b[start] & 0x80 != 0) != neg) return null;
    }
    var acc: u128 = if (neg) std.math.maxInt(u128) else 0;
    for (b[start..]) |x| acc = (acc << 8) | x;
    return @bitCast(acc);
}

/// A sign beside a little-endian magnitude as an unscaled decimal; null when the
/// magnitude is wider than an i128 holds.
pub fn signMagnitudeDecimal(positive: bool, mag: []const u8) ?i128 {
    if (mag.len > 16) return null;
    var u: u128 = 0;
    for (mag, 0..) |b, k| u |= @as(u128, b) << @intCast(k * 8);
    const m = std.math.cast(i128, u) orelse return null;
    return if (positive) m else -m;
}

/// Whether an unscaled decimal has at most `precision` digits.
pub fn withinPrecision(v: i128, precision: u8) bool {
    const bound = @import("value.zig").pow10_i128[precision];
    return v < bound and v > -bound;
}

const DateText = struct { days: i64, rest: []const u8 };

/// `YYYY-MM-DD` at the start of `t`, the year four digits or more and a ` BC` at the
/// end of `t` taken as its era; the month and day must exist. Null for anything
/// else, `infinity` included, so a value no date can hold fails rather than reads
/// as 1970-01-01.
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
    if (m < 1 or m > 12 or d < 1 or d > time.daysInMonth(y, m)) return null;
    if (bc) y = 1 - y;
    return .{ .days = time.daysFromCivil(y, m, d), .rest = body[i + 6 ..] };
}

/// MySQL's zero date (`0000-00-00`, with or without a time), which stands for no
/// date: it reads as NULL, as Connector/J's `zeroDateTimeBehavior=convertToNull` does.
fn zeroDate(t: []const u8) bool {
    return std.mem.startsWith(u8, t, "0000-00-00");
}

fn wireDate(t: []const u8) ?i64 {
    const dt = dateText(t) orelse return null;
    if (dt.rest.len > 0) return null;
    return dt.days;
}

/// Keeps the `.ffffff` fraction: dropping it once made `datetime(3)` values within a
/// second identical, collapsing distinct rows and unsettling ORDER BY. A trailing
/// offset (`-03`, `+05:45`, ` +00:00`), as a `timestamptz` prints in the session's
/// zone, is folded into UTC, so a value reads the same whatever that zone is. A
/// date alone is its midnight; a malformed time is null, never zero.
fn wireDatetime(t: []const u8) ?i64 {
    const dt = dateText(t) orelse return null;
    const r = dt.rest;
    if (r.len == 0) return dt.days * 86_400_000_000;
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
    return dt.days * 86_400_000_000 + secs * 1_000_000 + frac;
}

fn atoiN(s: []const u8) i64 {
    var v: i64 = 0;
    for (s) |c| {
        if (c >= '0' and c <= '9') v = v * 10 + (c - '0');
    }
    return v;
}

const testing = std.testing;

test "binary decimals: both layouts at every width, too wide refused" {
    const Case = struct { []const u8, ?i128 };
    const be = [_]Case{
        .{ &.{}, 0 },                                            .{ &.{0x7F}, 127 },
        .{ &.{0x80}, -128 },                                     .{ &.{ 0x00, 0x80 }, 128 },
        .{ &.{ 0xFF, 0x7F }, -129 },                             .{ &(.{0xFF} ** 8), -1 },
        .{ &(.{0x00} ++ .{0xFF} ** 8), 0xFFFF_FFFF_FFFF_FFFF },  .{ &(.{0xFF} ** 20), -1 },
        .{ &(.{0x7F} ++ .{0xFF} ** 15), std.math.maxInt(i128) }, .{ &(.{ 0x01, 0x00 } ++ .{0x00} ** 15), null },
    };
    for (be) |c| try testing.expectEqual(c[1], bigEndianDecimal(c[0]));
    try testing.expectEqual(@as(?i128, 12345), signMagnitudeDecimal(true, &.{ 0x39, 0x30, 0, 0 }));
    try testing.expectEqual(@as(?i128, -12345), signMagnitudeDecimal(false, &.{ 0x39, 0x30, 0, 0 }));
    try testing.expectEqual(@as(?i128, null), signMagnitudeDecimal(true, &(.{0xFF} ** 16)));
    try testing.expectEqual(@as(?i128, null), signMagnitudeDecimal(true, &(.{0x00} ** 17)));
    try testing.expect(withinPrecision(999, 3) and !withinPrecision(1000, 3) and !withinPrecision(-1000, 3));
}

test "intFast agrees with std and declines what it cannot prove" {
    for ([_][]const u8{ "0", "7", "-7", "123456789012345678", "-123456789012345678", "007" }) |s| {
        try testing.expectEqual(try std.fmt.parseInt(i64, s, 10), intFast(s).?);
    }
    for ([_][]const u8{ "", "-", "+1", "1_000", "0x10", "1234567890123456789", "1.0", " 1" }) |s| {
        try testing.expect(intFast(s) == null);
    }
}

test "floatFast agrees with std on plain decimals and declines the rest" {
    for ([_][]const u8{ "0", "1", "-1", "3.25", "-0.001", "123456.789", "99.90", "0.1", "1234567890.12345" }) |s| {
        try testing.expectEqual(try std.fmt.parseFloat(f64, s), floatFast(s).?);
    }
    for ([_][]const u8{ "", "-", ".", "1e5", "+1", "1.2.3", "abc", "1234567890123456", "nan" }) |s| {
        try testing.expect(floatFast(s) == null);
    }
}

test "decimal rejects what it cannot read instead of inventing a number" {
    try testing.expectEqual(@as(i128, 100000), decimal("1000.00").?.unscaled);
    try testing.expectEqual(@as(u8, 2), decimal("1000.00").?.scale);
    try testing.expectEqual(@as(i128, -5), decimal("-5").?.unscaled);
    try testing.expectEqual(@as(i128, 7), decimal("+7").?.unscaled);
    try testing.expectEqual(@as(i128, 25), decimal(".25").?.unscaled);
    for ([_][]const u8{ "1000,00", "1.234,56", "1,2,3", "abc", "1.2.3", "12 34", "", "-", "R$ 10" }) |s| {
        try testing.expect(decimal(s) == null);
    }
}

test "wire dates: the real ones read, BC and five-digit years included; no date reads as a made-up one" {
    try testing.expectEqual(@as(?i32, 0), try parse(.date, wire, "1970-01-01"));
    try testing.expectEqual(@as(?i32, 19727), try parse(.date, wire, "2024-01-05"));
    try testing.expectEqual(@as(?i32, @intCast(time.daysFromCivil(-43, 3, 15))), try parse(.date, wire, "0044-03-15 BC"));
    try testing.expectEqual(@as(?i32, @intCast(time.daysFromCivil(10000, 1, 1))), try parse(.date, wire, "10000-01-01"));
    try testing.expectEqual(@as(?i32, null), try parse(.date, wire, "0000-00-00"));
    try testing.expectEqual(@as(?i64, null), try parse(.timestamp, wire, "0000-00-00 00:00:00"));
    for ([_][]const u8{ "infinity", "-infinity", "2024-13-01", "2024-02-30", "24-01-05", "2024/01/05", "" }) |bad| {
        try testing.expectError(error.UnparseableNumber, parse(.date, wire, bad));
    }
    try testing.expectError(error.UnparseableNumber, parse(.timestamp, wire, "infinity"));
    try testing.expectError(error.UnparseableNumber, parse(.timestamp, wire, "2024-01-05 5:00"));
    try testing.expectEqual(wireDatetime("2024-01-05 00:00:00").?, wireDatetime("2024-01-05").?);
}

test "wire text: bad numbers fail instead of silently zeroing; bools, dates, timestamps and decimals read" {
    try testing.expectEqual(@as(i64, 42), try parse(.int, wire, "42"));
    try testing.expectEqual(@as(i64, 7), try parse(.int, wire, " 7 "));
    try testing.expectError(error.UnparseableNumber, parse(.int, wire, "abc"));
    try testing.expectError(error.UnparseableNumber, parse(.int, wire, ""));
    try testing.expectError(error.UnparseableNumber, parse(.float, wire, "1.2.3"));
    for ([_][]const u8{ "t", "1", "Yes" }) |s| try testing.expect(try parse(.bool, wire, s));
    for ([_][]const u8{ "false", "0" }) |s| try testing.expect(!try parse(.bool, wire, s));
    try testing.expectEqual(@as(?i32, 19723), try parse(.date, wire, "2024-01-01"));
    try testing.expectEqual(@as(?i32, 11016), try parse(.date, wire, "2000-02-29"));
    try testing.expectEqual(@as(?i64, 19723 * 86_400_000_000 + (1 * 3600 + 2 * 60 + 3) * 1_000_000), try parse(.timestamp, wire, "2024-01-01 01:02:03"));
    try testing.expectEqual(@as(?i64, 19723 * 86_400_000_000), try parse(.timestamp, wire, "2024-01-01"));
    const d = try parse(.decimal, wire, "-12.345");
    try testing.expectEqual(@as(i128, -12345), d.unscaled);
    try testing.expectEqual(@as(u8, 3), d.scale);
}

test "wire timestamps fold a timestamptz offset into UTC" {
    const base = wireDatetime("2024-01-05 05:00:00").?;
    try testing.expectEqual(base, wireDatetime("2024-01-05 02:00:00-03").?);
    try testing.expectEqual(base, wireDatetime("2024-01-05 10:45:00+05:45").?);
    try testing.expectEqual(base, wireDatetime("2024-01-05 05:00:00 +00:00").?);
    try testing.expectEqual(base + 250_000, wireDatetime("2024-01-05 02:00:00.25-03").?);
    try testing.expectEqual(base + 123_456, wireDatetime("2024-01-05 05:00:00.1234567").?);
}

test "the rules differ where the sources do: a code stays text when sniffed, a padded int reads on the wire" {
    try testing.expectError(error.TextTypeMismatch, parse(.int, sniffed, "007"));
    try testing.expectEqual(@as(i64, 7), try parse(.int, wire, "007"));
    try testing.expectEqual(@as(i64, 42), try parse(.int, wire, " 42 "));
    try testing.expectError(error.TextTypeMismatch, parse(.int, sniffed, " 42 "));
    try testing.expectError(error.TextTypeMismatch, parse(.int, sniffed, "1_000"));
    try testing.expectEqual(@as(i64, 1000), try parse(.int, wire, "1_000"));
    try testing.expectEqual(@as(f64, 1.5e-5), try parse(.float, sniffed, "1.5e-05"));
    try testing.expect(std.math.isNan(try parse(.float, sniffed, "nan")));
    try testing.expectError(error.TextTypeMismatch, parse(.date, sniffed, "0044-03-15 BC"));
    try testing.expect(parses(.int, sniffed, "12") and !parses(.int, sniffed, "+12") and parses(.decimal, sniffed, "x"));
}

test "a converter appends each kind into its storage, a zero date as NULL" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const Case = struct { types.TypeKind, []const []const u8, []const u8 };
    const cases = [_]Case{
        .{ .int, &.{ "1", " -2", "9223372036854775807" }, "1|-2|9223372036854775807" },
        .{ .float, &.{ "1.5", "-0.25", "2.5e3" }, "1.5|-0.25|2500" },
        .{ .decimal, &.{ "1.50", "-3" }, "1.50|-3" },
        .{ .bool, &.{ "t", "f", "1" }, "true|false|true" },
        .{ .date, &.{ "2024-01-05", "0000-00-00" }, "2024-01-05|NULL" },
        .{ .timestamp, &.{ "2024-01-05 01:02:03.5", "0000-00-00 00:00:00" }, "2024-01-05 01:02:03.500000|NULL" },
        .{ .string, &.{ "a", "" }, "a|" },
    };
    for (cases) |c| {
        const ty = if (c[0] == .decimal) types.Type.decimal(10, 2) else types.Type.init(c[0]);
        var b = column.Builder.init(a, ty.asNullable());
        const conv = converter(c[0], wire);
        for (c[1]) |raw| try conv(&b, raw);
        const col = try b.finish();
        var out = std.Io.Writer.Allocating.init(a);
        for (0..col.len) |i| {
            if (i > 0) try out.writer.writeByte('|');
            const v = col.getValue(i);
            try out.writer.writeAll(if (v.isNull()) "NULL" else try @import("eval.zig").valueToString(a, v));
        }
        try testing.expectEqualStrings(c[2], out.written());
    }
}
