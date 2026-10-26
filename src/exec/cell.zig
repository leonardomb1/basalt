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
//! parse and a typed append with no `Value` between them. A decimal's text, and
//! the binary layouts its unscaled integer comes in, are read in `decimal.zig`.
//!
//! A PostgreSQL `time` reads as a TIME, a time of day in `[00:00:00, 24:00:00)`:
//! PostgreSQL's `24:00:00` has no basalt TIME and fails, naming the column, rather
//! than wrap to midnight. A MySQL TIME stays text, as it may hold an elapsed time
//! (`-838:59:59` to `838:59:59`) that no time of day can.

const std = @import("std");
const types = @import("../lang/types.zig");
const column = @import("column.zig");
const Decimal = @import("value.zig").Decimal;
const decimal = @import("decimal.zig");
const textconv = @import("textconv.zig");
const cast = @import("eval/cast.zig");
const civil = @import("civil.zig");
const hex = @import("../format/hex.zig");

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
    /// A BYTES cell: `raw` as sent (MySQL's binary columns), or `pg_bytea`,
    /// PostgreSQL's `bytea` text in its hex or escape format.
    bytes: enum { raw, pg_bytea } = .raw,
};

/// A sniffed text source's cells: CSV and XML.
pub const sniffed = Rules{ .typed = &.{ .int, .float, .date }, .codes = true };

/// MySQL's text protocol, and the MySQL-protocol StarRocks and Doris.
pub const wire = Rules{
    .typed = &.{ .int, .float, .decimal, .bool, .date, .timestamp },
    .numbers = .std,
    .trim_ints = true,
    .dates = .wire,
    .mismatch = error.UnparseableNumber,
};

/// PostgreSQL's text protocol: `wire`, its `time` read as a time of day and its
/// `bytea` decoded.
pub const pg_wire = Rules{
    .typed = wire.typed ++ &[_]types.TypeKind{ .time, .bytes },
    .numbers = wire.numbers,
    .trim_ints = wire.trim_ints,
    .dates = wire.dates,
    .mismatch = wire.mismatch,
    .bytes = .pg_bytea,
};

/// Appends one non-NULL cell to a builder of the column's type.
pub const Converter = *const fn (b: *column.Builder, raw: []const u8) Error!void;

/// The converter of a column of `kind` under `rules`.
pub fn converter(kind: types.TypeKind, comptime rules: Rules) Converter {
    return switch (kind) {
        inline else => |k| &struct {
            fn f(b: *column.Builder, raw: []const u8) Error!void {
                const as = comptime readAs(k, rules);
                if (comptime as == .bytes and rules.bytes == .pg_bytea) return appendPgBytea(b, raw, rules.mismatch);
                const v = try parse(as, rules, raw);
                switch (comptime as) {
                    .int, .time => try b.appendInt(v),
                    .float => try b.appendFloat(v),
                    .decimal => try b.appendDecimal(v),
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
        .time => i64,
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
        .decimal => decimal.parse(raw) orelse bad,
        .bool => textconv.boolText(.wire, raw).?,
        .date => switch (rules.dates) {
            .iso => @as(?i32, @intCast(Iso.date(raw) orelse return bad)),
            .wire => if (zeroDate(raw)) null else std.math.cast(i32, Wire.date(raw) orelse return bad) orelse bad,
        },
        .timestamp => if (zeroDate(raw)) null else Wire.timestamp(raw) orelse bad,
        .time => Iso.time(raw) orelse bad,
        else => raw,
    };
}

/// PostgreSQL's `bytea` output decoded straight into the builder, in either format
/// a server's `bytea_output` gives: hex, `\x` then two digits a byte (the default),
/// or escape, a byte as itself, `\\` a backslash and `\ooo` any byte in octal.
/// Text neither format spells fails with `bad`.
fn appendPgBytea(b: *column.Builder, raw: []const u8, bad: Error) Error!void {
    if (raw.len >= 2 and raw[0] == '\\' and raw[1] == 'x') {
        if (raw.len % 2 != 0) return bad;
        const digits = raw[2..];
        const out = try b.reserveBytes(digits.len / 2);
        _ = hex.decode(out, digits) catch return bad;
        return b.commitBytes(digits.len / 2);
    }
    const out = try b.reserveBytes(raw.len);
    var n: usize = 0;
    var i: usize = 0;
    while (i < raw.len) : (n += 1) {
        if (raw[i] != '\\') {
            out[n] = raw[i];
            i += 1;
        } else if (i + 1 < raw.len and raw[i + 1] == '\\') {
            out[n] = '\\';
            i += 2;
        } else {
            if (i + 4 > raw.len) return bad;
            var v: u16 = 0;
            for (raw[i + 1 .. i + 4]) |c| {
                if (c < '0' or c > '7') return bad;
                v = v * 8 + (c - '0');
            }
            out[n] = std.math.cast(u8, v) orelse return bad;
            i += 4;
        }
    }
    try b.commitBytes(n);
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
    if (frac > decimal.exact_pow10f) return null;
    const v = @as(f64, @floatFromInt(mant)) / decimal.pow10f_table[frac];
    return if (neg) -v else v;
}

/// MySQL's zero date (`0000-00-00`, with or without a time), which stands for no
/// date: it reads as NULL, as Connector/J's `zeroDateTimeBehavior=convertToNull` does.
fn zeroDate(t: []const u8) bool {
    return std.mem.startsWith(u8, t, "0000-00-00");
}

/// A database's date and timestamp text (`civil.Spelling.wire`): `YYYY-MM-DD`, the
/// year four to seven digits and a trailing ` BC` its era, `infinity` refused rather
/// than read as 1970-01-01. A timestamp keeps its `.ffffff` fraction, since dropping
/// it once collapsed distinct `datetime(3)` rows, and folds a trailing offset into
/// UTC, so a `timestamptz` reads the same whatever the session's zone; one past what
/// an i64 of microseconds holds is null.
const Wire = civil.Text(.wire);
/// ISO text (`civil.Spelling.iso`), its clock checked: a time is `HH:MM[:SS[.f{1,6}]]`
/// below 24:00:00.
const Iso = civil.Text(.iso);

const testing = std.testing;

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

test "wire dates: the real ones read, BC and five-digit years included; no date reads as a made-up one" {
    try testing.expectEqual(@as(?i32, 0), try parse(.date, wire, "1970-01-01"));
    try testing.expectEqual(@as(?i32, 19727), try parse(.date, wire, "2024-01-05"));
    try testing.expectEqual(@as(?i32, @intCast(civil.daysFromCivil(-43, 3, 15))), try parse(.date, wire, "0044-03-15 BC"));
    try testing.expectEqual(@as(?i32, @intCast(civil.daysFromCivil(10000, 1, 1))), try parse(.date, wire, "10000-01-01"));
    try testing.expectEqual(@as(?i32, null), try parse(.date, wire, "0000-00-00"));
    try testing.expectEqual(@as(?i64, null), try parse(.timestamp, wire, "0000-00-00 00:00:00"));
    for ([_][]const u8{ "infinity", "-infinity", "2024-13-01", "2024-02-30", "24-01-05", "2024/01/05", "" }) |bad| {
        try testing.expectError(error.UnparseableNumber, parse(.date, wire, bad));
    }
    try testing.expectError(error.UnparseableNumber, parse(.timestamp, wire, "infinity"));
    try testing.expectError(error.UnparseableNumber, parse(.timestamp, wire, "2024-01-05 5:00"));
    try testing.expectEqual(Wire.timestamp("2024-01-05 00:00:00").?, Wire.timestamp("2024-01-05").?);
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

test "a wire timestamp past what an i64 of microseconds holds is unparseable, not an overflow" {
    for ([_][]const u8{ "300000-01-01", "294247-01-10 04:00:54.775808", "9999999-12-31 23:59:59", "0300000-01-01 00:00:00 BC" }) |s| {
        errdefer std.debug.print("input `{s}`\n", .{s});
        try testing.expectError(error.UnparseableNumber, parse(.timestamp, wire, s));
    }
    try testing.expectEqual(@as(?i64, std.math.maxInt(i64)), try parse(.timestamp, wire, "294247-01-10 04:00:54.775807"));
}

test "wire timestamps fold a timestamptz offset into UTC" {
    const base = Wire.timestamp("2024-01-05 05:00:00").?;
    try testing.expectEqual(base, Wire.timestamp("2024-01-05 02:00:00-03").?);
    try testing.expectEqual(base, Wire.timestamp("2024-01-05 10:45:00+05:45").?);
    try testing.expectEqual(base, Wire.timestamp("2024-01-05 05:00:00 +00:00").?);
    try testing.expectEqual(base + 250_000, Wire.timestamp("2024-01-05 02:00:00.25-03").?);
    try testing.expectEqual(base + 123_456, Wire.timestamp("2024-01-05 05:00:00.1234567").?);
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
        .{ .time, &.{ "01:02:03", "23:59:59.999999", "00:00:00.5" }, "01:02:03|23:59:59.999999|00:00:00.500000" },
        .{ .string, &.{ "a", "" }, "a|" },
    };
    for (cases) |c| {
        const ty = if (c[0] == .decimal) types.Type.decimal(10, 2) else types.Type.init(c[0]);
        var b = column.Builder.init(a, ty.asNullable());
        const conv = if (c[0] == .time) converter(c[0], pg_wire) else converter(c[0], wire);
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

test "a PostgreSQL time is a time of day, its 24:00:00 failing rather than wrapping; a MySQL TIME stays text" {
    try testing.expectEqual(@as(i64, 3_723_000_000), try parse(.time, pg_wire, "01:02:03"));
    try testing.expectEqual(@as(i64, 86_399_999_999), try parse(.time, pg_wire, "23:59:59.999999"));
    for ([_][]const u8{ "24:00:00", "25:00:00", "-01:00:00", "12:60:00", "1:02:03", "" }) |bad| {
        try testing.expectError(error.UnparseableNumber, parse(.time, pg_wire, bad));
    }
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    var b = column.Builder.init(ar.allocator(), types.Type.init(.time).asNullable());
    try testing.expectError(error.UnparseableNumber, converter(.time, pg_wire)(&b, "24:00:00"));
    try testing.expectEqualStrings("838:59:59", try parse(comptime readAs(.time, wire), wire, "838:59:59"));
    try testing.expectEqualStrings("-01:00:00", try parse(comptime readAs(.time, wire), wire, "-01:00:00"));
}

test "a BYTES cell is MySQL's raw bytes, and PostgreSQL's bytea decoded from hex or escape; other text fails" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var my = column.Builder.init(a, types.Type.init(.bytes).asNullable());
    try converter(.bytes, wire)(&my, "\\x01");
    var pg = column.Builder.init(a, types.Type.init(.bytes).asNullable());
    for ([_][]const u8{ "\\x01ff5C", "\\x", "\\001a\\\\b\\377", "" }) |raw| try converter(.bytes, pg_wire)(&pg, raw);
    for ([_][]const u8{ "\\x1", "\\xzz", "\\01", "\\", "a\\9zz", "\\400", "\\1_2", "\\+12", "\\-00", "\\08a" }) |raw| {
        try testing.expectError(error.UnparseableNumber, converter(.bytes, pg_wire)(&pg, raw));
    }
    const mc = try my.finish();
    try testing.expectEqualStrings("\\x01", mc.getValue(0).bytes);
    const pc = try pg.finish();
    try testing.expectEqual(@as(usize, 4), pc.len);
    try testing.expectEqualStrings("\x01\xff\x5c", pc.getValue(0).bytes);
    try testing.expectEqualStrings("", pc.getValue(1).bytes);
    try testing.expectEqualStrings("\x01a\\b\xff", pc.getValue(2).bytes);
    try testing.expectEqualStrings("", pc.getValue(3).bytes);
}
