//! How a text source types its cells, CSV and XML alike: a column is sniffed from
//! the sample's cells (`Column`), INT if every value is an integer, FLOAT if a
//! number, DATE if an ISO date, else STRING, and every later cell is read as that
//! type by the same rules (`appendCell`), a cell that no longer reads being an error
//! with its location rather than a NULL or a guess. A `+` or a leading zero marks a
//! code, not a number, and a quoted cell is text.

const std = @import("std");
const types = @import("../lang/types.zig");
const column = @import("../exec/column.zig");
const eval = @import("../exec/eval.zig");
const Value = @import("../exec/value.zig").Value;

pub const Column = struct {
    seen: bool = false,
    all_int: bool = true,
    all_float: bool = true,
    all_date: bool = true,
    scale: u8 = 0,

    /// Notes one sampled cell; an empty unquoted one is a NULL and says nothing.
    pub fn note(c: *Column, raw: []const u8, quoted: bool) void {
        if (quoted) {
            c.seen = true;
            c.all_int = false;
            c.all_float = false;
            c.all_date = false;
            noteScale(c, raw);
        } else if (raw.len > 0) {
            c.seen = true;
            noteScale(c, raw);
            if (c.all_date and eval.parseIsoDate(raw) == null) c.all_date = false;
            if (textLooking(raw)) {
                c.all_int = false;
                c.all_float = false;
            } else {
                if (c.all_int and eval.parseIntStrict(raw) == null) c.all_int = false;
                if (c.all_float and eval.parseFloatStrict(raw) == null) c.all_float = false;
            }
        }
    }

    /// The most decimal places of a plain number in the cell (`1.25`, `"-3.5"`), which
    /// a text column carries in its type's `scale` for `SUM` to type its exact result.
    fn noteScale(c: *Column, cell: []const u8) void {
        const t = std.mem.trim(u8, cell, " ");
        var i: usize = if (t.len > 0 and (t[0] == '-' or t[0] == '+')) 1 else 0;
        var digits: usize = 0;
        var places: ?usize = null;
        while (i < t.len) : (i += 1) {
            if (t[i] >= '0' and t[i] <= '9') {
                digits += 1;
                if (places) |*p| p.* += 1;
            } else if (t[i] == '.' and places == null) {
                places = 0;
            } else return;
        }
        if (digits == 0 or digits > 38) return;
        c.scale = @max(c.scale, @as(u8, @intCast(places orelse 0)));
    }

    /// The column's type from its sampled cells. One with no value at all in a sample
    /// that read the whole input (`whole`) holds only NULLs, so it is of unknown type,
    /// as SQL types a NULL: it compares and unifies with any type and is text where
    /// shown or written. Past the sample it may hold text, so there it stays a string.
    pub fn resolve(c: Column, whole: bool) types.Type {
        if (!c.seen and whole) return .{ .kind = .string, .nullable = true, .unknown = true };
        const k: types.TypeKind = if (!c.seen) .string else if (c.all_int) .int else if (c.all_float) .float else if (c.all_date) .date else .string;
        var t = types.Type.init(k).asNullable();
        if (k == .string) t.scale = c.scale;
        return t;
    }
};

/// Each sniffed field's type, a field the query reads only through `CAST` (named in
/// `text`) left as its text for the cast to convert.
pub fn settle(fields: []types.Schema.Field, cols: []const Column, whole: bool, text: []const []const u8) void {
    for (fields, cols) |*f, c| {
        f.ty = c.resolve(whole);
        for (text) |t| if (std.mem.eql(u8, t, f.name)) {
            f.ty = types.Type.init(.string).asNullable();
        };
    }
}

/// Appends a text cell to `b` as its sniffed type: an empty cell is NULL, or the
/// empty string when it was quoted into a string column; a cell its numeric or
/// date column cannot read is `error.TextTypeMismatch`, never a guess.
pub fn appendCell(b: *column.Builder, raw: []const u8, quoted: bool) !void {
    if (raw.len == 0) return b.append(if (quoted and b.ty.kind == .string) Value{ .string = raw } else .null);
    switch (b.ty.kind) {
        .int => try b.appendInt(intCell(raw) orelse return error.TextTypeMismatch),
        .float => try b.appendFloat(floatCell(raw) orelse return error.TextTypeMismatch),
        .date => try b.append(.{ .date = @intCast(eval.parseIsoDate(raw) orelse return error.TextTypeMismatch) }),
        else => try b.appendStr(raw),
    }
}

/// A `+` or a leading zero (`007`, `-01`) marks a code, not a number: the sniffer
/// types such a column text, and a later row holding one in a numeric column is a
/// mismatch rather than `01310100` read silently as 1310100.
fn textLooking(raw: []const u8) bool {
    return raw.len > 0 and (raw[0] == '+' or (raw.len > 1 and (raw[0] == '0' or (raw[0] == '-' and raw[1] == '0')) and std.mem.indexOfScalar(u8, raw, '.') == null));
}

fn intCell(raw: []const u8) ?i64 {
    if (textLooking(raw)) return null;
    return parseIntFast(raw) orelse eval.parseIntStrict(raw);
}

fn floatCell(raw: []const u8) ?f64 {
    if (textLooking(raw)) return null;
    return parseFloatFast(raw) orelse eval.parseFloatStrict(raw);
}

/// `[-]digits`, at most 18 of them so no i64 overflow: what a CSV cell needs of
/// `std.fmt.parseInt`, without its per-call base, underscore and sign handling.
fn parseIntFast(s: []const u8) ?i64 {
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

test "parseIntFast agrees with std and declines what it cannot prove" {
    for ([_][]const u8{ "0", "7", "-7", "123456789012345678", "-123456789012345678", "007" }) |s| {
        try std.testing.expectEqual(try std.fmt.parseInt(i64, s, 10), parseIntFast(s).?);
    }
    for ([_][]const u8{ "", "-", "+1", "1_000", "0x10", "1234567890123456789", "1.0", " 1" }) |s| {
        try std.testing.expect(parseIntFast(s) == null);
    }
}

/// `[-]digits[.digits]` with at most 15 significant digits, as an integer mantissa
/// over a power of ten: both exact doubles, so one division rounds correctly
/// (Clinger's fast path) and matches `std.fmt.parseFloat`. Anything else is null.
fn parseFloatFast(s: []const u8) ?f64 {
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

test "parseFloatFast agrees with std on plain decimals and declines the rest" {
    for ([_][]const u8{ "0", "1", "-1", "3.25", "-0.001", "123456.789", "99.90", "0.1", "1234567890.12345" }) |s| {
        try std.testing.expectEqual(try std.fmt.parseFloat(f64, s), parseFloatFast(s).?);
    }
    for ([_][]const u8{ "", "-", ".", "1e5", "+1", "1.2.3", "abc", "1234567890123456", "nan" }) |s| {
        try std.testing.expect(parseFloatFast(s) == null);
    }
}

/// Whether `raw` reads as `kind`, as `appendCell` would read it.
pub fn cellParses(kind: types.TypeKind, raw: []const u8) bool {
    if (raw.len == 0) return true;
    return switch (kind) {
        .int => intCell(raw) != null,
        .float => floatCell(raw) != null,
        .date => eval.parseIsoDate(raw) != null,
        else => true,
    };
}
