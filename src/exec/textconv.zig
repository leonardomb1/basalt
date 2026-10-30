//! Text to BOOL and float/decimal to INT, written once. Each reader of a boolean
//! spelling picks a `Words` preset (the set of spellings it accepts) and one
//! kernel, `boolText`, does the matching; each number-to-int site picks a `Round`
//! and `floatToInt`/`floatToIntScaled`/`decimalToInt` guard the i64 range, NaN and
//! infinities, which `@intFromFloat` treats as illegal behaviour.
//!
//! SQL reads a boolean one way (`Words.sql`) in a CAST, a literal met by a BOOL and
//! a PARAM; the other presets are config and file-format flags, each as its source
//! spells them.

const std = @import("std");
const column = @import("column.zig");
const value = @import("value.zig");

const Decimal = value.Decimal;
const decimal = @import("decimal.zig");
const pow10_i128 = decimal.pow10_i128;

/// A spelling that reads as one truth value. Input matches when it is a prefix of
/// `text` at least `min` bytes long, so `min == text.len` is a whole-word match.
pub const Word = struct {
    text: []const u8,
    min: usize,

    pub fn whole(comptime text: []const u8) Word {
        return .{ .text = text, .min = text.len };
    }

    pub fn prefix(comptime text: []const u8, comptime min: usize) Word {
        return .{ .text = text, .min = min };
    }
};

/// What one boolean reader accepts. `fold` ignores ASCII case, `trim` strips
/// `" \t\r\n"` first, `lead` lets only the first byte decide (it is compared with
/// each word's first byte), and `otherwise` is the answer for anything unmatched.
pub const Words = struct {
    yes: []const Word,
    no: []const Word = &.{},
    fold: bool = false,
    trim: bool = false,
    lead: bool = false,
    otherwise: ?bool = null,

    /// Text as SQL reads a BOOL, in `CAST(s AS BOOL)`, a literal met by a BOOL and a
    /// PARAM: `true`/`t`/`yes`/`y`/`1` and `false`/`f`/`no`/`n`/`0`, any case,
    /// trimmed, the words DuckDB's cast and PostgreSQL's input both read; no
    /// prefix and no `on`/`off`, which PostgreSQL alone reads.
    pub const sql: Words = .{
        .yes = &.{ .whole("true"), .whole("t"), .whole("yes"), .whole("y"), .whole("1") },
        .no = &.{ .whole("false"), .whole("f"), .whole("no"), .whole("n"), .whole("0") },
        .fold = true,
        .trim = true,
    };

    /// A WITH hint (analyze/targets.zig) or a bool option given as text
    /// (analyze/options.zig): any case, untrimmed, `true`/`false`.
    pub const hint: Words = .{ .yes = &.{.whole("true")}, .no = &.{.whole("false")}, .fold = true };

    /// The `key_pushdown` hint (plan/keypush.zig): `false` or `off` in any case
    /// turn it off; anything else leaves it on.
    pub const key_pushdown: Words = .{ .yes = &.{}, .no = &.{ .whole("false"), .whole("off") }, .fold = true, .otherwise = true };

    /// The `runtime_filter` hint (plan/runtime_filters.zig), read as `key_pushdown` is.
    pub const runtime_filter: Words = .{ .yes = &.{}, .no = &.{ .whole("false"), .whole("off") }, .fold = true, .otherwise = true };

    /// A BOOL cell as the engine printed it, read back by the terminal grid
    /// (connect/table.zig, cli/repl/view.zig): exactly `true`, else false.
    pub const printed: Words = .{ .yes = &.{.whole("true")}, .otherwise = false };

    /// An `xsi:nil` attribute (xml/read.zig:518): exactly `true` or `1`, else false.
    pub const xsd_nil: Words = .{ .yes = &.{ .whole("true"), .whole("1") }, .otherwise = false };

    /// An OOXML `date1904` attribute (xlsx/read.zig:369): `1` or `true` in any case,
    /// else false.
    pub const ooxml_flag: Words = .{ .yes = &.{ .whole("true"), .whole("1") }, .fold = true, .otherwise = false };

    /// A Delta table property (lake/delta_write.zig:439): `true` in any case, else false.
    pub const delta_property: Words = .{ .yes = &.{.whole("true")}, .fold = true, .otherwise = false };

    /// A driver's wire cell (cell.zig:110): a first byte of `1`, `t` or `y` in any
    /// case is true, anything else (the empty cell too) is false.
    pub const wire: Words = .{ .yes = &.{ .whole("1"), .whole("t"), .whole("y") }, .fold = true, .lead = true, .otherwise = false };
};

/// `s` as a truth value under `w`, or null when `w` has no `otherwise` and `s`
/// spells neither.
pub fn boolText(comptime w: Words, s: []const u8) ?bool {
    const t = if (w.trim) std.mem.trim(u8, s, " \t\r\n") else s;
    if (spells(w, w.yes, t)) return true;
    if (spells(w, w.no, t)) return false;
    return w.otherwise;
}

fn spells(comptime w: Words, comptime list: []const Word, t: []const u8) bool {
    inline for (list) |word| {
        if (w.lead) {
            if (t.len > 0 and sameByte(w.fold, t[0], word.text[0])) return true;
        } else if (t.len >= word.min and t.len <= word.text.len) {
            const head = word.text[0..t.len];
            if (if (w.fold) std.ascii.eqlIgnoreCase(t, head) else std.mem.eql(u8, t, head)) return true;
        }
    }
    return false;
}

inline fn sameByte(comptime fold: bool, a: u8, b: u8) bool {
    return if (fold) std.ascii.toLower(a) == std.ascii.toLower(b) else a == b;
}

/// How a number with a fraction becomes an int. `half_even` is PostgreSQL's float8
/// cast (column.floatToInt), `half_away` is `@round` and the decimal cast
/// (eval/cast.zig rescaleTo), and `exact` refuses any fraction (keytable intOf).
pub const Round = decimal.Round;

/// `x` rounded by `r`, or null when it is NaN, infinite or outside i64.
pub fn floatToInt(x: f64, comptime r: Round) ?i64 {
    const v = switch (r) {
        .half_even => halfEven(x),
        .half_away => @round(x),
        .exact => if (@trunc(x) == x) x else return null,
    };
    if (!(v >= -0x1p63 and v < 0x1p63)) return null;
    return @intFromFloat(v);
}

/// `x * factor` rounded by `r`, as a count of a smaller unit (micros of a day
/// offset, milliseconds of an Excel serial, a decimal's unscaled digits); the
/// product is taken in f64, so an integer factor past 2^53 is not exact.
pub fn floatToIntScaled(x: f64, factor: f64, comptime r: Round) ?i64 {
    return floatToInt(x * factor, r);
}

fn halfEven(x: f64) f64 {
    const f = @floor(x);
    const d = x - f;
    return if (d < 0.5) f else if (d > 0.5) f + 1 else if (@mod(f, 2) == 0) f else f + 1;
}

/// A decimal value, at its own scale, rounded by `r` to an int in exact i128
/// arithmetic; null when the result is outside i64 or, for `exact`, has a fraction.
pub fn decimalToInt(d: Decimal, comptime r: Round) ?i64 {
    return std.math.cast(i64, decimal.roundedWhole(d, r) orelse return null);
}

const testing = std.testing;

const bool_corpus = [_][]const u8{
    "",         "t",     "T",  "tr",  "tru", "true",  "TRUE",  "tRuE",   " true",     "true ",
    "\ttrue\n", "truee", "y",  "Y",   "ye",  "yes",   "YES",   "yes!",   "on",        "ON",
    "o",        "O",     "of", "OF",  "off", "OFF",   "offf",  "1",      "0",         "10",
    "01",       "2",     "f",  "F",   "fa",  "false", "FALSE", " false", "false\r\n", "n",
    "N",        "no",    "NO", "nil", "x",   "-1",    "\xff",  "t\x00",  "1 ",        " 1",
};

fn oracleSql(s: []const u8) ?bool {
    const t = std.mem.trim(u8, s, " \t\r\n");
    for ([_][]const u8{ "true", "t", "yes", "y", "1" }) |w| if (std.ascii.eqlIgnoreCase(t, w)) return true;
    for ([_][]const u8{ "false", "f", "no", "n", "0" }) |w| if (std.ascii.eqlIgnoreCase(t, w)) return false;
    return null;
}

fn oracleHint(s: []const u8) ?bool {
    return if (std.ascii.eqlIgnoreCase(s, "true")) true else if (std.ascii.eqlIgnoreCase(s, "false")) false else null;
}

fn oracleXsdNil(s: []const u8) ?bool {
    return std.mem.eql(u8, s, "true") or std.mem.eql(u8, s, "1");
}

fn oracleOoxmlFlag(s: []const u8) ?bool {
    return std.mem.eql(u8, s, "1") or std.ascii.eqlIgnoreCase(s, "true");
}

fn oracleDeltaProperty(s: []const u8) ?bool {
    return std.ascii.eqlIgnoreCase(s, "true");
}

fn oracleWire(raw: []const u8) ?bool {
    return raw.len > 0 and (raw[0] == '1' or raw[0] == 't' or raw[0] == 'T' or raw[0] == 'y' or raw[0] == 'Y');
}

fn expectPreset(comptime w: Words, comptime oracle: fn ([]const u8) ?bool) !void {
    for (bool_corpus) |s| {
        testing.expectEqual(oracle(s), boolText(w, s)) catch |e| {
            std.debug.print("preset disagrees with its source on '{f}'\n", .{std.zig.fmtString(s)});
            return e;
        };
    }
    var b: [3]u8 = undefined;
    for (0..256) |i| for (0..256) |j| {
        b = .{ @intCast(i), @intCast(j), 'e' };
        for ([_][]const u8{ b[0..1], b[0..2], b[0..3] }) |s| try testing.expectEqual(oracle(s), boolText(w, s));
    };
}

test "each bool preset reads exactly what the reader it replaces reads" {
    try expectPreset(Words.sql, oracleSql);
    try expectPreset(Words.hint, oracleHint);
    try expectPreset(Words.xsd_nil, oracleXsdNil);
    try expectPreset(Words.ooxml_flag, oracleOoxmlFlag);
    try expectPreset(Words.delta_property, oracleDeltaProperty);
    try expectPreset(Words.wire, oracleWire);
}

test "SQL reads a BOOL in one word set: DuckDB's and PostgreSQL's shared words, trimmed, any case (D6)" {
    for ([_][]const u8{ "t", "T", "true", "TRUE", "yes", " Yes ", "y", "1", "\ttrue\n" }) |s|
        try testing.expectEqual(@as(?bool, true), boolText(Words.sql, s));
    for ([_][]const u8{ "f", "F", "false", "no", "NO", "n", "0", " false " }) |s|
        try testing.expectEqual(@as(?bool, false), boolText(Words.sql, s));
    for ([_][]const u8{ "", "tr", "tru", "ye", "fa", "on", "off", "of", "o", "2", "-1", "10", "truee", "yess", "nil" }) |s|
        try testing.expectEqual(@as(?bool, null), boolText(Words.sql, s));
    try testing.expectEqual(@as(?bool, null), boolText(Words.hint, " true "));
}

test "a custom Words set: lead without fold is case-sensitive" {
    const w: Words = comptime .{ .yes = &.{.whole("t")}, .no = &.{.whole("f")}, .lead = true };
    try testing.expectEqual(@as(?bool, true), boolText(w, "tango"));
    try testing.expectEqual(@as(?bool, false), boolText(w, "foxtrot"));
    try testing.expectEqual(@as(?bool, null), boolText(w, "Tango"));
    try testing.expectEqual(@as(?bool, null), boolText(w, ""));
}

const float_corpus = [_]f64{
    0,                 -0.0,               0.5,        -0.5,                1.5,
    -1.5,              2.5,                -2.5,       0.49999999999999994, -0.49999999999999994,
    1e15 + 0.5,        -(1e15 + 0.5),      0x1p52 + 1, 0x1p53,              0x1p53 + 2,
    0x1p62,            0x1p63 - 1024,      0x1p63,     -0x1p63,             -0x1p63 - 2048,
    0x1p64,            1e300,              -1e300,     std.math.inf(f64),   -std.math.inf(f64),
    std.math.nan(f64), -std.math.nan(f64), 5e-324,     -5e-324,             std.math.floatMax(f64),
    123.456,           -123.456,           9.5,        10.5,                -9.5,
};

test "half_even is column.floatToInt" {
    for (float_corpus) |x| {
        const want: ?i64 = column.floatToInt(x) catch null;
        try testing.expectEqual(want, floatToInt(x, .half_even));
    }
    var prng = std.Random.DefaultPrng.init(0x7e47c0);
    const rnd = prng.random();
    for (0..20_000) |_| {
        const x: f64 = @bitCast(rnd.int(u64));
        const want: ?i64 = column.floatToInt(x) catch null;
        try testing.expectEqual(want, floatToInt(x, .half_even));
        const y = @as(f64, @floatFromInt(rnd.intRangeAtMost(i32, -1000, 1000))) / 2;
        try testing.expectEqual(column.floatToInt(y) catch null, floatToInt(y, .half_even));
    }
}

test "half_even matches the LIMIT float rounding" {
    for (float_corpus) |f| {
        if (!std.math.isFinite(f) or @abs(f) >= 0x1p63) {
            if (f != -0x1p63) try testing.expectEqual(@as(?i64, null), floatToInt(f, .half_even));
            continue;
        }
        const r = if (@abs(f - @trunc(f)) == 0.5) 2 * @round(f / 2) else @round(f);
        try testing.expectEqual(@as(?i64, @intFromFloat(r)), floatToInt(f, .half_even));
    }
}

test "half_away is @round where @intFromFloat is defined, null elsewhere" {
    for (float_corpus) |x| {
        const r = @round(x);
        const want: ?i64 = if (r >= -0x1p63 and r < 0x1p63) @intFromFloat(r) else null;
        try testing.expectEqual(want, floatToInt(x, .half_away));
    }
    try testing.expectEqual(@as(?i64, 3), floatToInt(2.5, .half_away));
    try testing.expectEqual(@as(?i64, -3), floatToInt(-2.5, .half_away));
    try testing.expectEqual(@as(?i64, 2), floatToInt(2.5, .half_even));
    try testing.expectEqual(@as(?i64, -2), floatToInt(-2.5, .half_even));
    try testing.expectEqual(@as(?i64, std.math.minInt(i64)), floatToInt(-0x1p63, .half_away));
    try testing.expectEqual(@as(?i64, null), floatToInt(0x1p63, .half_away));
}

test "exact refuses a fraction and is keytable's intOf" {
    for (float_corpus) |x| {
        const want: ?i64 = if (!(x >= -9223372036854775808.0 and x < 9223372036854775808.0) or @trunc(x) != x) null else @intFromFloat(x);
        try testing.expectEqual(want, floatToInt(x, .exact));
    }
    try testing.expectEqual(@as(?i64, null), floatToInt(0.5, .exact));
    try testing.expectEqual(@as(?i64, 0), floatToInt(-0.0, .exact));
}

test "floatToIntScaled is the guarded @round(x * factor) of opOff and the xlsx reader" {
    const factors = [_]f64{ 1, 1000, 86_400_000, 86_400_000_000, 3_600_000_000, 1e6, 100 };
    for (float_corpus) |x| for (factors) |k| {
        const p = @round(x * k);
        const want: ?i64 = if (p >= -0x1p63 and p < 0x1p63) @intFromFloat(p) else null;
        try testing.expectEqual(want, floatToIntScaled(x, k, .half_away));
    };
    try testing.expectEqual(@as(?i64, null), floatToIntScaled(1e40, 86_400_000_000, .half_away));
    try testing.expectEqual(@as(?i64, 129_600_000_000), floatToIntScaled(1.5, 86_400_000_000, .half_away));
    try testing.expectEqual(@as(?i64, 43_200_000), floatToIntScaled(0.5, 86_400_000, .half_away));
    try testing.expectEqual(@as(?i64, 86_400_000), floatToIntScaled(0.99999999999, 86_400_000, .half_away));
    try testing.expectEqual(@as(?i64, 1235), floatToIntScaled(12.345, 100, .half_away));
}

fn dec(u: i128, s: u8) Decimal {
    return .{ .unscaled = u, .scale = s };
}

const dec_corpus = [_]Decimal{
    dec(0, 0),                                        dec(0, 5),                                        dec(12345, 0),
    dec(-12345, 0),                                   dec(25, 1),                                       dec(-25, 1),
    dec(35, 1),                                       dec(-35, 1),                                      dec(15, 1),
    dec(-15, 1),                                      dec(24999, 4),                                    dec(-24999, 4),
    dec(250001, 5),                                   dec(1, 38),                                       dec(5, 1),
    dec(-5, 1),                                       dec(std.math.maxInt(i64), 0),                     dec(std.math.minInt(i64), 0),
    dec(@as(i128, std.math.maxInt(i64)) + 1, 0),      dec(@as(i128, std.math.minInt(i64)) - 1, 0),      dec(@as(i128, std.math.maxInt(i64)) * 10 + 4, 1),
    dec(@as(i128, std.math.maxInt(i64)) * 10 + 5, 1), dec(@as(i128, std.math.minInt(i64)) * 10 - 5, 1), dec(@as(i128, std.math.minInt(i64)) * 10 - 4, 1),
    dec(std.math.maxInt(i128), 38),                   dec(std.math.minInt(i128) + 1, 38),               dec(std.math.maxInt(i128), 20),
    dec(std.math.maxInt(i128), 0),                    dec(7, 39),                                       dec(-7, 200),
    dec(0, 255),                                      dec(5 * pow10_i128[37], 38),                      dec(-5 * pow10_i128[37], 38),
};

test "decimalToInt half_away is the CAST(decimal AS INT) path" {
    const rescaleTo = @import("decimal.zig").rescaleTo;
    for (dec_corpus) |d| {
        const want: ?i64 = if (rescaleTo(d, 0)) |z| std.math.cast(i64, z.unscaled) else null;
        try testing.expectEqual(want, decimalToInt(d, .half_away));
    }
    try testing.expectEqual(@as(?i64, 3), decimalToInt(dec(25, 1), .half_away));
    try testing.expectEqual(@as(?i64, -3), decimalToInt(dec(-25, 1), .half_away));
    try testing.expectEqual(@as(?i64, 2), decimalToInt(dec(24999, 4), .half_away));
    try testing.expectEqual(@as(?i64, 1), decimalToInt(dec(5 * std.math.pow(i128, 10, 37), 38), .half_away));
    try testing.expectEqual(@as(?i64, null), decimalToInt(dec(@as(i128, std.math.maxInt(i64)) * 10 + 5, 1), .half_away));
}

test "decimalToInt half_away matches the LIMIT decimal rounding" {
    for (dec_corpus) |d| {
        const p = std.math.powi(i128, 10, d.scale) catch {
            try testing.expectEqual(@as(?i64, 0), decimalToInt(d, .half_away));
            continue;
        };
        var q = @divTrunc(d.unscaled, p);
        if (@abs(@rem(d.unscaled, p)) * 2 >= p) q += if (d.unscaled < 0) -1 else 1;
        try testing.expectEqual(std.math.cast(i64, q), decimalToInt(d, .half_away));
    }
}

test "decimalToInt exact is keytable's intOfDec" {
    for (dec_corpus) |d| {
        const want: ?i64 = blk: {
            if (d.scale >= pow10_i128.len) break :blk if (d.unscaled == 0) 0 else null;
            const p = pow10_i128[d.scale];
            if (@rem(d.unscaled, p) != 0) break :blk null;
            break :blk std.math.cast(i64, @divExact(d.unscaled, p));
        };
        try testing.expectEqual(want, decimalToInt(d, .exact));
    }
    try testing.expectEqual(@as(?i64, 12), decimalToInt(dec(1200, 2), .exact));
    try testing.expectEqual(@as(?i64, null), decimalToInt(dec(1201, 2), .exact));
}

test "decimalToInt half_even rounds ties to even and agrees with the float path" {
    try testing.expectEqual(@as(?i64, 2), decimalToInt(dec(25, 1), .half_even));
    try testing.expectEqual(@as(?i64, 4), decimalToInt(dec(35, 1), .half_even));
    try testing.expectEqual(@as(?i64, -2), decimalToInt(dec(-25, 1), .half_even));
    try testing.expectEqual(@as(?i64, -4), decimalToInt(dec(-35, 1), .half_even));
    try testing.expectEqual(@as(?i64, 0), decimalToInt(dec(5, 1), .half_even));
    try testing.expectEqual(@as(?i64, 3), decimalToInt(dec(250001, 5), .half_even));
    for (0..2001) |i| {
        const u = @as(i128, @intCast(i)) - 1000;
        const d = dec(u, 1);
        const x = @as(f64, @floatFromInt(u)) / 10;
        if (@abs(@rem(u, 10)) == 5) {
            try testing.expectEqual(floatToInt(x, .half_even), decimalToInt(d, .half_even));
            try testing.expectEqual(floatToInt(x, .half_away), decimalToInt(d, .half_away));
        }
        try testing.expectEqual(floatToInt(x, .exact), decimalToInt(d, .exact));
    }
}

test "a decimal value's own scale decides, not a column's" {
    try testing.expectEqual(decimalToInt(dec(25, 1), .half_away), decimalToInt(dec(2500, 3), .half_away));
    try testing.expectEqual(decimalToInt(dec(25, 1), .half_even), decimalToInt(dec(25000000, 7), .half_even));
    try testing.expectEqual(@as(?i64, 7), decimalToInt(dec(7000, 3), .exact));
}
