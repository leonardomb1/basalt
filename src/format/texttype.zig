//! How a text source types its cells, CSV and XML alike: a column is sniffed from
//! the sample's cells (`Column`), INT if every value is an integer, FLOAT if a
//! number, DATE if an ISO date, else STRING, and every later cell is read as that
//! type by the same rules (`appendCell`), a cell that no longer reads being an error
//! with its location rather than a NULL or a guess. A `+` or a leading zero marks a
//! code, not a number, and a quoted cell is text.

const std = @import("std");
const types = @import("../lang/types.zig");
const column = @import("../exec/column.zig");
const cell = @import("../exec/cell.zig");
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
            if (c.all_date and !cell.parses(.date, cell.sniffed, raw)) c.all_date = false;
            if (c.all_int and !cell.parses(.int, cell.sniffed, raw)) c.all_int = false;
            if (c.all_float and !cell.parses(.float, cell.sniffed, raw)) c.all_float = false;
        }
    }

    /// The most decimal places of a plain number in the cell (`1.25`, `"-3.5"`), which
    /// a text column carries in its type's `scale` for `SUM` to type its exact result.
    fn noteScale(c: *Column, text: []const u8) void {
        const t = std.mem.trim(u8, text, " ");
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

/// Appends a text cell to `b` as its sniffed type (`cell.sniffed`): an empty cell is
/// NULL, or the empty string when it was quoted into a string column; a cell its
/// numeric or date column cannot read is `error.TextTypeMismatch`, never a guess.
pub fn appendCell(b: *column.Builder, raw: []const u8, quoted: bool) !void {
    if (raw.len == 0) return b.append(if (quoted and b.ty.kind == .string) Value{ .string = raw } else .null);
    return cell.converter(b.ty.kind, cell.sniffed)(b, raw);
}

/// Whether `raw` reads as `kind`, as `appendCell` would read it.
pub fn cellParses(kind: types.TypeKind, raw: []const u8) bool {
    return raw.len == 0 or cell.parses(kind, cell.sniffed, raw);
}

/// `Column.note` as it was before it read cells through `cell.parses`, kept as the
/// oracle the differential test checks against.
fn oldNote(c: *Column, raw: []const u8) void {
    const eval = @import("../exec/eval.zig");
    c.seen = true;
    if (c.all_date and eval.parseIsoDate(raw) == null) c.all_date = false;
    if (cell.codeLooking(raw)) {
        c.all_int = false;
        c.all_float = false;
    } else {
        if (c.all_int and eval.parseIntStrict(raw) == null) c.all_int = false;
        if (c.all_float and eval.parseFloatStrict(raw) == null) c.all_float = false;
    }
}

test "a sniffed cell types as it did before the sniffer read it through cell.parses" {
    const corpus = [_][]const u8{
        "0",                    "7",                   "-7",                  "-0",                  "007",                  "-01",                 "+1",
        "+",                    "-",                   ".",                   ".5",                  "5.",                   "-.5",                 "-5.",
        "1e5",                  "1E-5",                "1.5e-05",             "1e",                  "1e+",                  "nan",                 "NaN",
        "inf",                  "-inf",                "Infinity",            "+inf",                "1_000",                "0x10",                " 1",
        "1 ",                   " 2024-01-05",         "2024-01-05",          "2024-01-05 ",         "2024-1-5",             "2024-02-30",          "0044-03-15 BC",
        "123456789012345678",   "1234567890123456789", "9223372036854775807", "9223372036854775808", "-9223372036854775808", "-123456789012345678", "1234567890.12345",
        "1234567890123456.5",   "0.1",                 "00.5",                "0.",                  "1.2.3",                "abc",                 "1,5",
        "12345678901234567890", "1e400",               "-1e400",              "1e-400",              "\t3",                  "3\r",                 "2024-01-05T00:00",
    };
    for (corpus) |raw| {
        var old = Column{};
        var new = Column{};
        oldNote(&old, raw);
        new.note(raw, false);
        errdefer std.debug.print("cell `{s}`\n", .{raw});
        try std.testing.expectEqual(old.all_int, new.all_int);
        try std.testing.expectEqual(old.all_float, new.all_float);
        try std.testing.expectEqual(old.all_date, new.all_date);
    }
    var rnd = std.Random.DefaultPrng.init(0x5aff);
    const r = rnd.random();
    const alphabet = "0123456789-+.eE_x n";
    var buf: [24]u8 = undefined;
    for (0..200_000) |_| {
        const n = r.intRangeAtMost(usize, 1, buf.len);
        for (buf[0..n]) |*ch| ch.* = alphabet[if (r.boolean()) r.uintLessThan(usize, 10) else r.uintLessThan(usize, alphabet.len)];
        const raw = buf[0..n];
        var old = Column{};
        var new = Column{};
        oldNote(&old, raw);
        new.note(raw, false);
        errdefer std.debug.print("cell `{s}`\n", .{raw});
        try std.testing.expectEqual(old.all_int, new.all_int);
        try std.testing.expectEqual(old.all_float, new.all_float);
        try std.testing.expectEqual(old.all_date, new.all_date);
    }
}
