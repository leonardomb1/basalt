//! Row search: the query language of `search(cols, 'query')` and of `\view`'s
//! find. Words are AND-ed and each may be in any searched column; `-word` keeps
//! the rows without it; `col:word` looks in that column only (a name that is not
//! a searched column leaves the whole token as text); `"two words"` is one
//! phrase. Matching is a substring in any case over a value's text as basalt
//! prints it (`eval.valueToString`: ISO dates, decimals at their scale), and a
//! NULL holds nothing.
//!
//! A query is parsed once (`Query.parse`) and bound to the names of the columns
//! it searches (`Query.bind`); `matches` then tests a row through a callback for
//! each column's text, so the engine (batch columns) and the REPL (rendered
//! cells) share every rule.

const std = @import("std");

pub const Term = struct { name: ?[]const u8, text: []const u8, whole: []const u8, not: bool };

pub const Bound = struct { col: ?usize, text: []const u8, not: bool };

pub const Query = struct {
    terms: []const Term = &.{},

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
            try out.append(.{ .col = col, .text = text, .not = t.not });
        }
        return out.toOwnedSlice();
    }
};

/// Whether a row passes every term: `cell(ctx, c)` is column `c`'s text, null
/// for NULL, over `ncols` columns.
pub fn matches(terms: []const Bound, ncols: usize, ctx: anytype, comptime cell: fn (@TypeOf(ctx), usize) ?[]const u8) bool {
    for (terms) |t| {
        const hit = if (t.col) |c| holds(cell(ctx, c), t.text) else blk: {
            for (0..ncols) |c| if (holds(cell(ctx, c), t.text)) break :blk true;
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

fn holds(cell: ?[]const u8, text: []const u8) bool {
    const c = cell orelse return false;
    return std.ascii.indexOfIgnoreCase(c, text) != null;
}

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
    var nb: [4][]const u8 = undefined;
    try std.testing.expectEqual(@as(usize, 1), needles(b, 0, &nb).len);
    try std.testing.expectEqual(@as(usize, 2), needles(b, 1, &nb).len);
}
