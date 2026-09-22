//! Tab completion for the REPL, as a library: given the text, the cursor and
//! the names a session knows, what could the word under the cursor become? No
//! terminal, no I/O — the REPL supplies the names (and lists directories for a
//! path), the editor draws the choices — so the same matcher can serve an LSP.
//!
//! What is offered, by where the cursor is:
//!   - `\…` at the start of the entry: the meta commands;
//!   - inside an unclosed `'…'`: a file path, asked of the caller;
//!   - `$…`: params and LETs;
//!   - `conn.…`: that connection's tables, `schema.table`; `x.…` otherwise: columns;
//!   - a bare word: CTEs, connections, functions, the columns of the tables and
//!     files the entry names, then keywords — spelled in the case of the prefix.

const std = @import("std");
const hilite = @import("hilite.zig");

pub const Kind = enum { meta, param, cte, connection, function, table, column, keyword, path };

pub const Candidate = struct { text: []const u8, kind: Kind };

/// One connection's tables, as `schema.table`.
pub const ConnTables = struct { conn: []const u8, tables: []const []const u8 };

pub const Names = struct {
    connections: []const []const u8 = &.{},
    ctes: []const []const u8 = &.{},
    functions: []const []const u8 = &.{},
    params: []const []const u8 = &.{},
    tables: []const ConnTables = &.{},
    /// The columns of every table and file the entry names, pooled.
    columns: []const []const u8 = &.{},
};

/// Every meta command the REPL answers to; `cli.zig`'s test checks it stays so.
pub const meta_commands = [_][]const u8{ "\\connections", "\\c", "\\connect", "\\reset", "\\clear", "\\cls", "\\format", "\\f", "\\view", "\\v", "\\d", "\\dt", "\\i", "\\source", "\\save", "\\edit", "\\e", "\\help", "\\h", "\\q", "\\quit" };

pub const Result = union(enum) {
    /// Replace `text[start..cursor]` with a pick from `items`.
    candidates: struct { start: usize, items: []const Candidate },
    /// The cursor is inside a string: the caller lists paths for `partial` and
    /// replaces from `start` (just after the opening quote).
    path: struct { start: usize, partial: []const u8 },
    none,
};

fn isWordByte(c: u8) bool {
    return std.ascii.isAlphanumeric(c) or c == '_' or c >= 0x80;
}

/// Is `at` inside a single-quoted string on its line? Quotes are counted from
/// the line's start, a doubled quote being one literal character.
fn stringStart(text: []const u8, at: usize) ?usize {
    const ls = if (std.mem.lastIndexOfScalar(u8, text[0..at], '\n')) |p| p + 1 else 0;
    var open: ?usize = null;
    var i = ls;
    while (i < at) : (i += 1) {
        if (text[i] != '\'') continue;
        if (open == null) {
            open = i;
        } else if (i + 1 < at and text[i + 1] == '\'') {
            i += 1;
        } else open = null;
    }
    return open;
}

fn startsWithFold(s: []const u8, prefix: []const u8) bool {
    return s.len >= prefix.len and std.ascii.eqlIgnoreCase(s[0..prefix.len], prefix);
}

fn hasUpper(s: []const u8) bool {
    for (s) |c| if (std.ascii.isUpper(c)) return true;
    return false;
}

/// A keyword in the case the person is typing: `sel` → `select`, `SEL` → `SELECT`.
fn cased(arena: std.mem.Allocator, word: []const u8, like: []const u8) ![]const u8 {
    const out = try arena.dupe(u8, word);
    if (hasUpper(like)) {
        for (out) |*c| c.* = std.ascii.toUpper(c.*);
    }
    return out;
}

pub fn complete(arena: std.mem.Allocator, names: Names, text: []const u8, cursor: usize) !Result {
    if (stringStart(text, cursor)) |q| return .{ .path = .{ .start = q + 1, .partial = text[q + 1 .. cursor] } };

    var start = cursor;
    while (start > 0 and (isWordByte(text[start - 1]) or text[start - 1] == '.' or text[start - 1] == '$' or text[start - 1] == '\\')) start -= 1;
    const word = text[start..cursor];
    if (word.len == 0) return .none;
    var out = std.array_list.Managed(Candidate).init(arena);

    if (word.len > 0 and word[0] == '\\') {
        if (std.mem.trim(u8, text[0..start], " \t").len != 0) return .none;
        for (meta_commands) |m| if (startsWithFold(m, word)) try out.append(.{ .text = m, .kind = .meta });
        return finish(&out, start);
    }
    if (word.len > 0 and word[0] == '$') {
        for (names.params) |p| {
            if (startsWithFold(p, word[1..])) try out.append(.{ .text = try std.fmt.allocPrint(arena, "${s}", .{p}), .kind = .param });
        }
        return finish(&out, start);
    }
    if (std.mem.indexOfScalar(u8, word, '.')) |dot| {
        const head = word[0..dot];
        const rest = word[dot + 1 ..];
        for (names.tables) |ct| {
            if (!std.ascii.eqlIgnoreCase(ct.conn, head)) continue;
            for (ct.tables) |t| {
                if (startsWithFold(t, rest)) try out.append(.{ .text = try std.fmt.allocPrint(arena, "{s}.{s}", .{ head, t }), .kind = .table });
            }
            return finish(&out, start);
        }
        for (names.columns) |c| {
            if (startsWithFold(c, rest)) try out.append(.{ .text = try std.fmt.allocPrint(arena, "{s}.{s}", .{ head, c }), .kind = .column });
        }
        return finish(&out, start);
    }

    for (names.ctes) |n| if (startsWithFold(n, word)) try out.append(.{ .text = n, .kind = .cte });
    for (names.connections) |n| if (startsWithFold(n, word)) try out.append(.{ .text = n, .kind = .connection });
    for (names.functions) |n| if (startsWithFold(n, word)) try out.append(.{ .text = n, .kind = .function });
    for (names.columns) |n| if (startsWithFold(n, word)) try out.append(.{ .text = n, .kind = .column });
    if (word.len > 0) {
        for (hilite.keywords) |k| if (startsWithFold(k, word)) try out.append(.{ .text = try cased(arena, k, word), .kind = .keyword });
    }
    return finish(&out, start);
}

fn finish(out: *std.array_list.Managed(Candidate), start: usize) !Result {
    if (out.items.len == 0) return .none;
    return .{ .candidates = .{ .start = start, .items = try out.toOwnedSlice() } };
}

/// The longest prefix every candidate shares, compared without case — what a
/// first Tab fills in before the choices are shown.
pub fn commonPrefix(items: []const Candidate) []const u8 {
    if (items.len == 0) return "";
    var n = items[0].text.len;
    for (items[1..]) |c| {
        var i: usize = 0;
        while (i < n and i < c.text.len and std.ascii.toLower(c.text[i]) == std.ascii.toLower(items[0].text[i])) i += 1;
        n = i;
    }
    return items[0].text[0..n];
}

test "complete: keywords in the typer's case, names first, and nothing for an empty word" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const names = Names{ .ctes = &.{"sales"}, .connections = &.{"sr"}, .columns = &.{ "customer", "cents" } };
    const r = try complete(a, names, "SELECT c", 8);
    const items = r.candidates.items;
    try std.testing.expectEqual(@as(usize, 7), r.candidates.start);
    try std.testing.expectEqualStrings("customer", items[0].text);
    try std.testing.expectEqual(Kind.column, items[0].kind);
    try std.testing.expectEqualStrings("cents", items[1].text);
    try std.testing.expectEqual(Kind.keyword, items[2].kind);
    try std.testing.expectEqualStrings("call", items[2].text);
    try std.testing.expectEqualStrings("SELECT", (try complete(a, names, "SEL", 3)).candidates.items[0].text);
    // `co` could be a column or several keywords: what they share is `co` itself.
    const co = (try complete(a, .{ .columns = &.{"color"} }, "WHERE co", 8)).candidates.items;
    try std.testing.expectEqualStrings("color", co[0].text);
    try std.testing.expect(co.len > 1);
    try std.testing.expectEqualStrings("co", commonPrefix(co));
    try std.testing.expect((try complete(a, names, "SELECT ", 7)) == .none);
}

test "complete: conn.table from the catalog, alias.column from the pool, $param, \\meta, and a path" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const names = Names{
        .connections = &.{"sr"},
        .params = &.{ "since", "tag" },
        .tables = &.{.{ .conn = "sr", .tables = &.{ "bronze.kimai_tags", "bronze.kimai_users" } }},
        .columns = &.{ "id", "name" },
    };
    const t = (try complete(a, names, "FROM sr.bronze.kimai_t", 22)).candidates;
    try std.testing.expectEqual(@as(usize, 5), t.start);
    try std.testing.expectEqual(@as(usize, 1), t.items.len);
    try std.testing.expectEqualStrings("sr.bronze.kimai_tags", t.items[0].text);
    const c = (try complete(a, names, "SELECT t.n", 10)).candidates;
    try std.testing.expectEqualStrings("t.name", c.items[0].text);
    const p = (try complete(a, names, "WHERE x > $s", 12)).candidates;
    try std.testing.expectEqualStrings("$since", p.items[0].text);
    const m = (try complete(a, names, "\\d", 2)).candidates;
    try std.testing.expectEqualStrings("\\d", m.items[0].text);
    try std.testing.expectEqualStrings("\\dt", m.items[1].text);
    const path = (try complete(a, names, "FROM 'data/or", 13)).path;
    try std.testing.expectEqualStrings("data/or", path.partial);
    try std.testing.expectEqual(@as(usize, 6), path.start);
    // A closed string is not a path.
    try std.testing.expect((try complete(a, names, "FROM 'a.csv' WHERE ", 19)) == .none);
}
