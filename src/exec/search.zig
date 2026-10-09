//! Row search: the query language of `search(cols, 'query')` and of `\view`'s
//! find. Words are AND-ed and each may be in any searched column; `-word` keeps
//! the rows without it; `col:word` looks in that column only (a name that is not
//! a searched column leaves the whole token as text); `"two words"` is one
//! phrase. Matching is a substring in any case over a value's text as basalt
//! prints it (`eval.valueToString`: ISO dates, decimals at their scale), and a
//! NULL holds nothing. "Any case" is `upper()`/`lower()`'s: accented Latin,
//! Greek and Cyrillic letters fold too, so `crédito` finds `CRÉDITO`. With
//! `Fold.accents` the accents go as well, as `unaccent()` drops them, so
//! `credito` finds `CRÉDITO` and `strasse` finds `Straße`; a character that
//! unaccents to two (`ß`, `Æ`) matches both of them in turn.
//!
//! A query is parsed once (`Query.parse`) and bound to the names of the columns
//! it searches (`Query.bind`); `matches` then tests a row through a callback for
//! each column's text, so the engine (batch columns) and the REPL (rendered
//! cells) share every rule.

const caseMap = @import("eval/strings.zig").caseMap;
const charWidth = @import("eval/strings.zig").charWidth;
const unaccentCp = @import("eval/strings.zig").unaccentCp;
const std = @import("std");

pub const Term = struct { name: ?[]const u8, text: []const u8, whole: []const u8, not: bool };

/// How two characters are compared: by case alone, or by case and accent.
pub const Fold = enum { case, accents };

pub const Bound = struct { col: ?usize, text: []const u8, not: bool, fold: Fold = .case };

pub const Query = struct {
    terms: []const Term = &.{},
    fold: Fold = .case,

    /// Splits `src` into terms; their text points into `src`. gpa-owned.
    pub fn parse(gpa: std.mem.Allocator, src: []const u8) !Query {
        var out = std.array_list.Managed(Term).init(gpa);
        errdefer out.deinit();
        var i: usize = 0;
        while (i < src.len) {
            while (i < src.len and src[i] == ' ') i += 1;
            if (i == src.len) break;
            const not = src[i] == '-' and i + 1 < src.len and src[i + 1] != ' ';
            if (not) i += 1;
            const start = i;
            var quoted = false;
            while (i < src.len and (quoted or src[i] != ' ')) : (i += 1) {
                if (src[i] == '"') quoted = !quoted;
            }
            const tok = src[start..i];
            var name: ?[]const u8 = null;
            var text = tok;
            if (std.mem.indexOfScalar(u8, tok, ':')) |c| if (c > 0 and tok[0] != '"') {
                name = tok[0..c];
                text = tok[c + 1 ..];
            };
            const whole = std.mem.trim(u8, tok, "\"");
            text = std.mem.trim(u8, text, "\"");
            if (whole.len > 0) try out.append(.{ .name = name, .text = text, .whole = whole, .not = not });
        }
        return .{ .terms = try out.toOwnedSlice() };
    }

    pub fn deinit(self: *Query, gpa: std.mem.Allocator) void {
        gpa.free(self.terms);
        self.terms = &.{};
    }

    /// The terms with `col:` resolved against `names` (any case); a name that is
    /// none of them makes the whole token text. gpa-owned.
    pub fn bind(self: Query, gpa: std.mem.Allocator, names: []const []const u8) ![]Bound {
        var out = std.array_list.Managed(Bound).init(gpa);
        errdefer out.deinit();
        for (self.terms) |t| {
            var col: ?usize = null;
            if (t.name) |n| for (names, 0..) |cn, k| if (std.ascii.eqlIgnoreCase(cn, n)) {
                col = k;
                break;
            };
            const text = if (t.name != null and col == null) t.whole else t.text;
            if (text.len == 0) continue;
            try out.append(.{ .col = col, .text = text, .not = t.not, .fold = self.fold });
        }
        return out.toOwnedSlice();
    }
};

/// Whether a row passes every term: `cell(ctx, c)` is column `c`'s text, null
/// for NULL, over `ncols` columns.
pub fn matches(terms: []const Bound, ncols: usize, ctx: anytype, comptime cell: fn (@TypeOf(ctx), usize) ?[]const u8) bool {
    for (terms) |t| {
        const hit = if (t.col) |c| holds(cell(ctx, c), t.text, t.fold) else blk: {
            for (0..ncols) |c| if (holds(cell(ctx, c), t.text, t.fold)) break :blk true;
            break :blk false;
        };
        if (hit == t.not) return false;
    }
    return true;
}

/// The texts to highlight in column `col`: the terms that look there and are not
/// exclusions, at most `buf.len`.
pub fn needles(terms: []const Bound, col: usize, buf: [][]const u8) [][]const u8 {
    var n: usize = 0;
    for (terms) |t| {
        if (t.not or n == buf.len) continue;
        if (t.col != null and t.col.? != col) continue;
        buf[n] = t.text;
        n += 1;
    }
    return buf[0..n];
}

fn holds(cell: ?[]const u8, text: []const u8, fold: Fold) bool {
    const c = cell orelse return false;
    return findFold(c, text, 0, fold) != null;
}

pub const Span = struct { start: usize, end: usize };

/// `findFold` by case alone.
pub fn find(hay: []const u8, needle: []const u8, from: usize) ?Span {
    return findFold(hay, needle, from, .case);
}

/// The first occurrence of `needle` in `hay` at or after byte `from`, comparing
/// characters folded as `fold` says; its bytes in `hay`, which may differ in
/// length from `needle`'s.
pub fn findFold(hay: []const u8, needle: []const u8, from: usize, fold: Fold) ?Span {
    if (needle.len == 0) return .{ .start = from, .end = from };
    var i = from;
    while (i < hay.len) : (i += charWidth(hay, i)) {
        if (matchAt(hay, i, needle, fold)) |end| return .{ .start = i, .end = end };
    }
    return null;
}

fn matchAt(hay: []const u8, at: usize, needle: []const u8, fold: Fold) ?usize {
    var h = Folded{ .s = hay, .i = at, .fold = fold };
    var n = Folded{ .s = needle, .i = 0, .fold = fold };
    while (n.next()) |nc| {
        const hc = h.next() orelse return null;
        if (hc != nc) return null;
    }
    return h.end;
}

/// A text's characters folded one at a time: lower case, and with `.accents`
/// unaccented, a character becoming none (a combining accent) or several (`ß`).
/// `end` is the byte after the character the last one came from.
const Folded = struct {
    s: []const u8,
    i: usize,
    fold: Fold,
    end: usize = 0,
    queue: []const u8 = "",

    fn next(self: *Folded) ?u21 {
        while (true) {
            if (self.queue.len > 0) {
                const c = self.queue[0];
                self.queue = self.queue[1..];
                return std.ascii.toLower(c);
            }
            if (self.i >= self.s.len) return null;
            const w = charWidth(self.s, self.i);
            const c = self.s[self.i..][0..w];
            self.i += w;
            self.end = self.i;
            if (w == 1) return std.ascii.toLower(c[0]);
            const cp = std.unicode.utf8Decode(c) catch return c[0];
            if (self.fold == .accents) if (unaccentCp(cp)) |plain| {
                self.queue = plain;
                continue;
            };
            return caseMap(cp, false);
        }
    }
};

test "search: words, exclusions, a column, a phrase, an unknown column as text" {
    const gpa = std.testing.allocator;
    var q = try Query.parse(gpa, "sp -2026-03 UF:rj \"two words\" nope:x");
    defer q.deinit(gpa);
    const b = try q.bind(gpa, &.{ "uf", "valor" });
    defer gpa.free(b);
    try std.testing.expectEqual(@as(usize, 5), b.len);
    try std.testing.expectEqualStrings("sp", b[0].text);
    try std.testing.expect(b[0].col == null and !b[0].not);
    try std.testing.expect(b[1].not);
    try std.testing.expectEqualStrings("2026-03", b[1].text);
    try std.testing.expectEqual(@as(?usize, 0), b[2].col);
    try std.testing.expectEqualStrings("rj", b[2].text);
    try std.testing.expectEqualStrings("two words", b[3].text);
    try std.testing.expect(b[4].col == null);
    try std.testing.expectEqualStrings("nope:x", b[4].text);
}

test "search: a row matches when every term holds, in any case; NULL holds nothing" {
    const gpa = std.testing.allocator;
    const Row = struct {
        cells: []const ?[]const u8,
        fn at(self: @This(), c: usize) ?[]const u8 {
            return self.cells[c];
        }
    };
    var q = try Query.parse(gpa, "SP -rio valor:10");
    defer q.deinit(gpa);
    const b = try q.bind(gpa, &.{ "uf", "valor", "city" });
    defer gpa.free(b);
    try std.testing.expect(matches(b, 3, Row{ .cells = &.{ "sp", "10.50", "Campinas" } }, Row.at));
    try std.testing.expect(!matches(b, 3, Row{ .cells = &.{ "sp", "10.50", "Rio Claro" } }, Row.at));
    try std.testing.expect(!matches(b, 3, Row{ .cells = &.{ "sp", "9.50", "Campinas" } }, Row.at));
    try std.testing.expect(!matches(b, 3, Row{ .cells = &.{ null, "10.00", null } }, Row.at));
    try std.testing.expect(matches(b, 3, Row{ .cells = &.{ "SP", "10", "CAMPINAS" } }, Row.at));
    var nb: [4][]const u8 = undefined;
    try std.testing.expectEqual(@as(usize, 1), needles(b, 0, &nb).len);
    try std.testing.expectEqual(@as(usize, 2), needles(b, 1, &nb).len);
}

test "search: accented and non-Latin letters match in any case, as upper()/lower() fold them" {
    try std.testing.expectEqual(Span{ .start = 6, .end = 14 }, find("TIPO: CRÉDITO X", "crédito", 0).?);
    try std.testing.expect(find("São Paulo", "SÃO", 0) != null);
    try std.testing.expect(find("ÇÃO", "ção", 0) != null);
    try std.testing.expect(find("Москва", "МОСКВА", 0) != null);
    try std.testing.expect(find("ΑΘΗΝΑ", "αθηνα", 0) != null);
    try std.testing.expect(find("credito", "crédito", 0) == null);
    try std.testing.expectEqual(Span{ .start = 8, .end = 11 }, find("abc abc abc", "ABC", 5).?);
    try std.testing.expect(find("abc", "abcd", 0) == null);
    try std.testing.expect(find("", "a", 0) == null);
}

test "search: with accents folded too, unaccented text finds accented and the reverse" {
    try std.testing.expect(find("CRÉDITO", "credito", 0) == null);
    try std.testing.expectEqual(Span{ .start = 5, .end = 13 }, findFold("TIPO CRÉDITO", "credito", 0, .accents).?);
    try std.testing.expect(findFold("credito", "CRÉDITO", 0, .accents) != null);
    try std.testing.expect(findFold("Straße 5", "STRASSE", 0, .accents) != null);
    try std.testing.expect(findFold("strasse", "straße", 0, .accents) != null);
    try std.testing.expect(findFold("Ærø", "aero", 0, .accents) != null);
    try std.testing.expect(findFold("cafe\u{301}", "café", 0, .accents) != null);
    try std.testing.expect(findFold("São Paulo", "sao paulo", 0, .accents) != null);
    try std.testing.expect(findFold("abc", "abd", 0, .accents) == null);

    const gpa = std.testing.allocator;
    var q = try Query.parse(gpa, "credito -debito");
    defer q.deinit(gpa);
    q.fold = .accents;
    const b = try q.bind(gpa, &.{"n"});
    defer gpa.free(b);
    const Row = struct {
        cells: []const ?[]const u8,
        fn at(self: @This(), c: usize) ?[]const u8 {
            return self.cells[c];
        }
    };
    try std.testing.expect(matches(b, 1, Row{ .cells = &.{"CRÉDITO EM CONTA"} }, Row.at));
    try std.testing.expect(!matches(b, 1, Row{ .cells = &.{"CRÉDITO X DÉBITO"} }, Row.at));
}
