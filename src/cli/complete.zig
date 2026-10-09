//! Tab completion for the REPL, as a library: given the text, the cursor and
//! the names a session knows, what could the word under the cursor become? No
//! terminal, no I/O — the REPL supplies the names (and lists directories for a
//! path), the editor draws the choices — so the same matcher can serve an LSP.
//!
//! What is offered, by where the cursor is:
//!   - `\…` at the start of the entry: the meta commands;
//!   - inside an unclosed `'…'`: a file path, asked of the caller, which replaces
//!     from just after the opening quote;
//!   - `$…`: params and LETs;
//!   - `conn.…`: that connection's tables, `schema.table`; `x.…` otherwise: columns;
//!   - a bare word: CTEs, connections, declared functions, the columns of the
//!     tables and files the entry names, the built-in functions, then keywords —
//!     spelled in the case of the prefix.
//!
//! A word is offered once: the first kind to claim it wins, so a column called
//! `name` is not offered again as the keyword `name`. A candidate's `detail` is
//! what an editor shows beside it: a function's signature, or a column's type as
//! its source names it (empty when unknown).
//!
//! `builtin_functions` and `meta_commands` list everything the engine and REPL
//! answer to by name; `cli.zig`'s tests hold them to the engine's registries both
//! ways, so a builtin added there cannot go missing from Tab.

const std = @import("std");
const hilite = @import("hilite.zig");

pub const Kind = enum { meta, param, cte, connection, function, table, column, keyword, path };

pub const Candidate = struct { text: []const u8, kind: Kind, detail: []const u8 = "" };

pub const ConnTables = struct { conn: []const u8, tables: []const []const u8 };

pub const Column = struct { name: []const u8, type: []const u8 = "" };

pub const Names = struct {
    connections: []const []const u8 = &.{},
    ctes: []const []const u8 = &.{},
    functions: []const []const u8 = &.{},
    params: []const []const u8 = &.{},
    tables: []const ConnTables = &.{},
    columns: []const Column = &.{},
};

pub const Builtin = struct { name: []const u8, sig: []const u8 };

pub const builtin_functions = [_]Builtin{
    .{ .name = "now", .sig = "now()" },
    .{ .name = "today", .sig = "today()" },
    .{ .name = "regexp_replace", .sig = "regexp_replace(s, pattern, replacement[, flags])" },
    .{ .name = "search", .sig = "search(* | t.* | * EXCEPT (c) | (a, b), 'words -not col:word \"a phrase\"'[, unaccent bool])" },
    .{ .name = "regexp_matches", .sig = "regexp_matches(s, pattern)" },
    .{ .name = "regexp_extract", .sig = "regexp_extract(s, pattern[, group])" },
    .{ .name = "md5", .sig = "md5(s)" },
    .{ .name = "sha256", .sig = "sha256(s)" },
    .{ .name = "xxhash64", .sig = "xxhash64(s)" },
    .{ .name = "date_trunc", .sig = "date_trunc(unit, ts)" },
    .{ .name = "extract", .sig = "extract(unit FROM ts)" },
    .{ .name = "upper", .sig = "upper(s)" },
    .{ .name = "lower", .sig = "lower(s)" },
    .{ .name = "length", .sig = "length(s)" },
    .{ .name = "strlen", .sig = "strlen(s)" },
    .{ .name = "bit_count", .sig = "bit_count(n)" },
    .{ .name = "to_hex", .sig = "to_hex(n)" },
    .{ .name = "from_hex", .sig = "from_hex(s)" },
    .{ .name = "concat", .sig = "concat(a, …)" },
    .{ .name = "concat_ws", .sig = "concat_ws(sep, a, …)" },
    .{ .name = "chars", .sig = "chars(s)" },
    .{ .name = "coalesce", .sig = "coalesce(a, …)" },
    .{ .name = "starts_with", .sig = "starts_with(s, prefix)" },
    .{ .name = "ends_with", .sig = "ends_with(s, suffix)" },
    .{ .name = "contains", .sig = "contains(s, sub)" },
    .{ .name = "like", .sig = "like(s, pattern)" },
    .{ .name = "trim", .sig = "trim(s)" },
    .{ .name = "substr", .sig = "substr(s, start[, length])" },
    .{ .name = "replace", .sig = "replace(s, from, to)" },
    .{ .name = "abs", .sig = "abs(x)" },
    .{ .name = "floor", .sig = "floor(x)" },
    .{ .name = "ceil", .sig = "ceil(x)" },
    .{ .name = "round", .sig = "round(x[, digits])" },
    .{ .name = "mod", .sig = "mod(a, b)" },
    .{ .name = "power", .sig = "power(base, exponent)" },
    .{ .name = "sqrt", .sig = "sqrt(x)" },
    .{ .name = "sign", .sig = "sign(x)" },
    .{ .name = "nullif", .sig = "nullif(a, b)" },
    .{ .name = "greatest", .sig = "greatest(a, b, …)" },
    .{ .name = "least", .sig = "least(a, b, …)" },
    .{ .name = "lpad", .sig = "lpad(s, length[, fill])" },
    .{ .name = "rpad", .sig = "rpad(s, length[, fill])" },
    .{ .name = "left", .sig = "left(s, n)" },
    .{ .name = "right", .sig = "right(s, n)" },
    .{ .name = "split_part", .sig = "split_part(s, delimiter, n)" },
    .{ .name = "strpos", .sig = "strpos(s, sub)" },
    .{ .name = "repeat", .sig = "repeat(s, n)" },
    .{ .name = "reverse", .sig = "reverse(s)" },
    .{ .name = "date_add", .sig = "date_add(unit, n, ts)" },
    .{ .name = "date_diff", .sig = "date_diff(unit, start, end)" },
    .{ .name = "make_date", .sig = "make_date(year, month, day)" },
    .{ .name = "epoch", .sig = "epoch(ts)" },
    .{ .name = "to_timestamp", .sig = "to_timestamp(seconds)" },
    .{ .name = "strftime", .sig = "strftime(ts, format)" },
    .{ .name = "json_reduce", .sig = "json_reduce(arr, initial, (acc, x) -> value)" },
    .{ .name = "json_range", .sig = "json_range([start,] stop)" },
    .{ .name = "json_length", .sig = "json_length(arr)" },
    .{ .name = "json_slice", .sig = "json_slice(arr, start[, stop])" },
    .{ .name = "json_concat", .sig = "json_concat(arr, arr, …)" },
    .{ .name = "json_object", .sig = "json_object(key, value, …)" },
    .{ .name = "json_array", .sig = "json_array(value, …)" },
    .{ .name = "to_base64", .sig = "to_base64(s)" },
    .{ .name = "from_base64", .sig = "from_base64(s)" },
    .{ .name = "url_encode", .sig = "url_encode(s)" },
    .{ .name = "url_decode", .sig = "url_decode(s)" },
    .{ .name = "strptime", .sig = "strptime(text, format)" },
    .{ .name = "try_strptime", .sig = "try_strptime(text, format)" },
    .{ .name = "unaccent", .sig = "unaccent(s)" },
    .{ .name = "strip_accents", .sig = "strip_accents(s)" },
    .{ .name = "translate", .sig = "translate(s, from, to)" },
    .{ .name = "initcap", .sig = "initcap(s)" },
    .{ .name = "ascii", .sig = "ascii(s)" },
    .{ .name = "chr", .sig = "chr(n)" },
    .{ .name = "json_get", .sig = "json_get(json, path)" },
    .{ .name = "json_filter", .sig = "json_filter(array, x -> condition)" },
    .{ .name = "json_transform", .sig = "json_transform(array, x -> value)" },
    .{ .name = "json_any", .sig = "json_any(array, x -> condition)" },
    .{ .name = "json_all", .sig = "json_all(array, x -> condition)" },
    .{ .name = "count", .sig = "count(* | x) [OVER (…)]" },
    .{ .name = "sum", .sig = "sum(x) [OVER (…)]" },
    .{ .name = "avg", .sig = "avg(x) [OVER (…)]" },
    .{ .name = "min", .sig = "min(x) [OVER (…)]" },
    .{ .name = "max", .sig = "max(x) [OVER (…)]" },
    .{ .name = "median", .sig = "median(x)" },
    .{ .name = "count_if", .sig = "count_if(condition)" },
    .{ .name = "bool_and", .sig = "bool_and(condition)" },
    .{ .name = "bool_or", .sig = "bool_or(condition)" },
    .{ .name = "bit_and", .sig = "bit_and(x)" },
    .{ .name = "bit_or", .sig = "bit_or(x)" },
    .{ .name = "bit_xor", .sig = "bit_xor(x)" },
    .{ .name = "var_samp", .sig = "var_samp(x)" },
    .{ .name = "variance", .sig = "variance(x)" },
    .{ .name = "var_pop", .sig = "var_pop(x)" },
    .{ .name = "stddev_samp", .sig = "stddev_samp(x)" },
    .{ .name = "stddev", .sig = "stddev(x)" },
    .{ .name = "stddev_pop", .sig = "stddev_pop(x)" },
    .{ .name = "row_number", .sig = "row_number() OVER (…)" },
    .{ .name = "rank", .sig = "rank() OVER (…)" },
    .{ .name = "dense_rank", .sig = "dense_rank() OVER (…)" },
    .{ .name = "percent_rank", .sig = "percent_rank() OVER (…)" },
    .{ .name = "cume_dist", .sig = "cume_dist() OVER (…)" },
    .{ .name = "ntile", .sig = "ntile(n) OVER (…)" },
    .{ .name = "lag", .sig = "lag(x[, n[, default]]) [IGNORE NULLS] OVER (…)" },
    .{ .name = "lead", .sig = "lead(x[, n[, default]]) [IGNORE NULLS] OVER (…)" },
    .{ .name = "first_value", .sig = "first_value(x) [IGNORE NULLS] OVER (…)" },
    .{ .name = "last_value", .sig = "last_value(x) [IGNORE NULLS] OVER (…)" },
    .{ .name = "nth_value", .sig = "nth_value(x, n) [IGNORE NULLS] OVER (…)" },
    .{ .name = "cast", .sig = "cast(x AS type)" },
    .{ .name = "try_cast", .sig = "try_cast(x AS type)" },
    .{ .name = "if", .sig = "if(cond, a, b)" },
};

pub const meta_commands = [_][]const u8{ "\\connections", "\\c", "\\connect", "\\reset", "\\clear", "\\cls", "\\format", "\\f", "\\view", "\\v", "\\d", "\\dt", "\\i", "\\source", "\\save", "\\edit", "\\e", "\\help", "\\h", "\\q", "\\quit" };

pub const Result = union(enum) {
    candidates: struct { start: usize, items: []const Candidate },
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

/// Where the word at `cursor` begins (`cursor` itself when none is under way).
/// An empty offer still reports it, so an editor never replaces from the top of the cell.
pub fn wordStart(text: []const u8, cursor: usize) usize {
    var start = cursor;
    while (start > 0 and (isWordByte(text[start - 1]) or text[start - 1] == '.' or text[start - 1] == '$' or text[start - 1] == '\\')) start -= 1;
    return start;
}

pub fn complete(arena: std.mem.Allocator, names: Names, text: []const u8, cursor: usize) !Result {
    if (stringStart(text, cursor)) |q| return .{ .path = .{ .start = q + 1, .partial = text[q + 1 .. cursor] } };

    const start = wordStart(text, cursor);
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
            if (startsWithFold(c.name, rest)) try add(&out, .{ .text = try std.fmt.allocPrint(arena, "{s}.{s}", .{ head, c.name }), .kind = .column, .detail = c.type });
        }
        return finish(&out, start);
    }

    for (names.ctes) |n| if (startsWithFold(n, word)) try add(&out, .{ .text = n, .kind = .cte });
    for (names.connections) |n| if (startsWithFold(n, word)) try add(&out, .{ .text = n, .kind = .connection });
    for (names.functions) |n| if (startsWithFold(n, word)) try add(&out, .{ .text = n, .kind = .function });
    for (names.columns) |c| if (startsWithFold(c.name, word)) try add(&out, .{ .text = c.name, .kind = .column, .detail = c.type });
    for (builtin_functions) |f| if (startsWithFold(f.name, word)) try add(&out, .{ .text = try cased(arena, f.name, word), .kind = .function, .detail = f.sig });
    for (hilite.keywords) |k| if (startsWithFold(k, word)) try add(&out, .{ .text = try cased(arena, k, word), .kind = .keyword });
    return finish(&out, start);
}

/// Append `c` unless a candidate with the same text (ignoring case) is already
/// there: the earlier, more specific kind keeps the word.
fn add(out: *std.array_list.Managed(Candidate), c: Candidate) !void {
    for (out.items) |have| if (std.ascii.eqlIgnoreCase(have.text, c.text)) return;
    try out.append(c);
}

fn finish(out: *std.array_list.Managed(Candidate), start: usize) !Result {
    if (out.items.len == 0) return .none;
    return .{ .candidates = .{ .start = start, .items = try out.toOwnedSlice() } };
}

/// The longest prefix every candidate shares, compared without case: what a
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

fn offered(items: []const Candidate, text: []const u8) ?Candidate {
    for (items) |c| if (std.mem.eql(u8, c.text, text)) return c;
    return null;
}

fn timesOffered(items: []const Candidate, text: []const u8) usize {
    var n: usize = 0;
    for (items) |c| if (std.mem.eql(u8, c.text, text)) {
        n += 1;
    };
    return n;
}

test "complete: keywords in the typer's case, names first, and nothing for an empty word" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const names = Names{ .ctes = &.{"sales"}, .connections = &.{"sr"}, .columns = &.{ .{ .name = "customer" }, .{ .name = "cents" } } };
    const r = try complete(a, names, "SELECT c", 8);
    const items = r.candidates.items;
    try std.testing.expectEqual(@as(usize, 7), r.candidates.start);
    try std.testing.expectEqualStrings("customer", items[0].text);
    try std.testing.expectEqual(Kind.column, items[0].kind);
    try std.testing.expectEqualStrings("cents", items[1].text);
    try std.testing.expectEqual(Kind.column, items[1].kind);
    try std.testing.expectEqual(Kind.function, offered(items, "concat").?.kind);
    try std.testing.expectEqual(Kind.keyword, items[items.len - 1].kind);
    try std.testing.expectEqualStrings("SELECT", (try complete(a, names, "SEL", 3)).candidates.items[0].text);
    const co = (try complete(a, .{ .columns = &.{.{ .name = "color" }} }, "WHERE co", 8)).candidates.items;
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
        .columns = &.{ .{ .name = "id" }, .{ .name = "name" } },
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
    try std.testing.expect((try complete(a, names, "FROM 'a.csv' WHERE ", 19)) == .none);
}

test "complete: built-in functions by prefix with their signature, columns with their type, each word once" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const text = "SELECT * FROM RANGE(3) WHERE date_";
    const d = (try complete(a, .{}, text, text.len)).candidates;
    try std.testing.expectEqual(@as(usize, text.len - "date_".len), d.start);
    for (d.items) |c| {
        try std.testing.expectEqual(Kind.function, c.kind);
        try std.testing.expect(std.mem.startsWith(u8, c.text, "date_"));
    }
    try std.testing.expect(offered(d.items, "date_trunc") != null);
    try std.testing.expect(offered(d.items, "date_diff") != null);
    try std.testing.expectEqualStrings("date_add(unit, n, ts)", offered(d.items, "date_add").?.detail);
    try std.testing.expectEqualStrings("DATE_ADD", (try complete(a, .{}, "DATE_A", 6)).candidates.items[0].text);
    try std.testing.expectEqualStrings("row_number() OVER (…)", (try complete(a, .{}, "row_n", 5)).candidates.items[0].detail);

    const names = Names{ .columns = &.{ .{ .name = "name", .type = "string" }, .{ .name = "amount", .type = "decimal(10,2)" }, .{ .name = "NAME", .type = "int" } } };
    const n = (try complete(a, names, "SELECT na", 9)).candidates.items;
    try std.testing.expectEqual(@as(usize, 1), n.len);
    try std.testing.expectEqual(Kind.column, n[0].kind);
    try std.testing.expectEqualStrings("string", n[0].detail);
    const q = (try complete(a, names, "SELECT t.am", 11)).candidates.items;
    try std.testing.expectEqualStrings("t.amount", q[0].text);
    try std.testing.expectEqualStrings("decimal(10,2)", q[0].detail);
    const cnt = (try complete(a, .{ .columns = &.{.{ .name = "count", .type = "int" }} }, "cou", 3)).candidates.items;
    try std.testing.expectEqual(Kind.column, cnt[0].kind);
    try std.testing.expectEqualStrings("count", cnt[0].text);
    try std.testing.expectEqual(@as(usize, 1), timesOffered(cnt, "count"));
    try std.testing.expectEqual(Kind.function, offered(cnt, "count_if").?.kind);
    const fnc = (try complete(a, .{}, "cou", 3)).candidates.items;
    for (fnc) |c| try std.testing.expectEqual(Kind.function, c.kind);
    try std.testing.expect(offered(fnc, "count") != null);
    try std.testing.expect(offered(fnc, "count_if") != null);
}
