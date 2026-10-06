//! Syntax colouring for the REPL's entry: a style per byte, from a scanner that
//! never fails on half-typed text — an unclosed string is coloured to the end and
//! the rest is left plain. Deliberately not the parser's lexer: that one skips
//! comments and stops at the first bad byte, both wrong for text being typed.
//!
//! `keywords` is the parser's keyword set (`sql_parser.zig`'s `isKw`) plus the
//! aggregate, window and type names that read as keywords to a person. Only a bare
//! word can be a keyword (`x.count` is a column); a word followed by `(` is coloured
//! as a call (`trim(`, `erp.QUERY(`) unless it is a keyword (`IN (`, `AS (`).

const std = @import("std");

pub const Style = enum(u8) { plain, keyword, string, number, comment, param, punct, func };

pub const keywords = [_][]const u8{
    "accept",   "all",      "analyze",    "anchor",    "and",       "anti",      "append",     "as",         "asc",
    "at",       "between",  "body",       "buffer",    "by",        "call",      "case",       "cols",       "connection",
    "continue", "costs",    "create",     "cross",     "current",   "default",   "desc",       "distinct",   "doc",
    "each",     "else",     "empty",      "end",       "endpoint",  "error",     "every",      "except",     "exclude",
    "explain",  "flush",    "for",        "from",      "full",      "function",  "group",      "having",     "header",
    "hours",    "http",     "identifier", "in",        "inner",     "into",      "is",         "jobs",       "join",
    "json",     "left",     "let",        "like",      "limit",     "load",      "loaded",     "max",        "name",
    "not",      "null",     "of",         "offset",    "on",        "options",   "or",         "order",      "outer",
    "over",     "paginate", "parallel",   "param",     "partial",   "partition", "preceding",  "precision",  "print",
    "pushdown", "query",    "range",      "rename",    "replace",   "retain",    "retry",      "right",      "row",
    "rows",     "schema",   "seconds",    "segment",   "select",    "semi",      "sequential", "split",      "stop",
    "table",    "then",     "throw",      "type",      "unbounded", "union",     "unnest",     "until",      "upsert",
    "using",    "when",     "where",      "with",      "true",      "false",     "count",      "sum",        "avg",
    "min",      "median",   "cast",       "try_cast",  "if",        "rank",      "dense_rank", "row_number", "lag",
    "lead",     "string",   "int",        "float",     "decimal",   "bool",      "date",       "time",       "timestamp",
    "resource", "get",      "post",       "json_each",
    // type names
    "varchar",   "char",      "nvarchar",   "text",       "bigint",
    "smallint", "tinyint",  "integer",    "double",    "real",      "numeric",   "boolean",    "datetime",
};

fn isKeyword(word: []const u8) bool {
    for (keywords) |k| if (std.ascii.eqlIgnoreCase(word, k)) return true;
    return false;
}

fn isIdent(c: u8) bool {
    return std.ascii.isAlphanumeric(c) or c == '_' or c >= 0x80;
}

pub fn scan(text: []const u8, out: []Style) void {
    @memset(out, .plain);
    var i: usize = 0;
    while (i < text.len) {
        const c = text[i];
        if (c == '-' and i + 1 < text.len and text[i + 1] == '-') {
            const end = std.mem.indexOfScalarPos(u8, text, i, '\n') orelse text.len;
            @memset(out[i..end], .comment);
            i = end;
        } else if (c == '/' and i + 1 < text.len and text[i + 1] == '*') {
            const end = if (std.mem.indexOfPos(u8, text, i + 2, "*/")) |p| p + 2 else text.len;
            @memset(out[i..end], .comment);
            i = end;
        } else if (c == '\'' or c == '"') {
            var j = i + 1;
            while (j < text.len) : (j += 1) {
                if (text[j] == c) {
                    if (j + 1 < text.len and text[j + 1] == c) {
                        j += 1;
                        continue;
                    }
                    j += 1;
                    break;
                }
            }
            @memset(out[i..j], if (c == '\'') .string else .plain);
            i = j;
        } else if (c == '$' and i + 1 < text.len and text[i + 1] == '$') {
            const end = if (std.mem.indexOfPos(u8, text, i + 2, "$$")) |p| p + 2 else text.len;
            @memset(out[i..end], .string);
            i = end;
        } else if (c == '$' and i + 1 < text.len and (text[i + 1] == '{' or isIdent(text[i + 1]))) {
            var j = i + 1;
            if (text[j] == '{') {
                j = if (std.mem.indexOfScalarPos(u8, text, j, '}')) |p| p + 1 else text.len;
            } else {
                while (j < text.len and (isIdent(text[j]) or text[j] == '.')) j += 1;
            }
            @memset(out[i..j], .param);
            i = j;
        } else if (std.ascii.isDigit(c) and (i == 0 or !isIdent(text[i - 1]))) {
            var j = i;
            while (j < text.len and (std.ascii.isDigit(text[j]) or text[j] == '.')) j += 1;
            @memset(out[i..j], .number);
            i = j;
        } else if (isIdent(c)) {
            var j = i;
            while (j < text.len and isIdent(text[j])) j += 1;
            const qualified = i > 0 and text[i - 1] == '.';
            var k = j;
            while (k < text.len and text[k] == ' ') k += 1;
            const call = k < text.len and text[k] == '(';
            if (!qualified and isKeyword(text[i..j])) {
                @memset(out[i..j], .keyword);
            } else if (call) @memset(out[i..j], .func);
            i = j;
        } else {
            if (std.mem.indexOfScalar(u8, "(),;|=<>+-*/%", c) != null) out[i] = .punct;
            i += 1;
        }
    }
}

pub fn sgr(s: Style) []const u8 {
    return switch (s) {
        .plain => "\x1b[39m",
        .keyword => "\x1b[1;34m",
        .string => "\x1b[32m",
        .number => "\x1b[36m",
        .comment => "\x1b[2;39m",
        .param => "\x1b[35m",
        .punct => "\x1b[39m",
        .func => "\x1b[33m",
    };
}

pub const sgr_reset = "\x1b[22;39m";

test "scan: keywords, a string with a doubled quote, a comment, a param, a number, an unclosed string" {
    const text = "SELECT 'it''s' AS x, $p, 12.5 -- note\nFROM t WHERE y = 'open";
    var out: [64]Style = undefined;
    scan(text, out[0..text.len]);
    try std.testing.expectEqual(Style.keyword, out[0]);
    try std.testing.expectEqual(Style.string, out[7]);
    try std.testing.expectEqual(Style.string, out[12]);
    try std.testing.expectEqual(Style.keyword, out[15]);
    try std.testing.expectEqual(Style.plain, out[18]);
    try std.testing.expectEqual(Style.param, out[21]);
    try std.testing.expectEqual(Style.number, out[25]);
    try std.testing.expectEqual(Style.comment, out[30]);
    try std.testing.expectEqual(Style.keyword, out[38]);
    try std.testing.expectEqual(Style.string, out[text.len - 1]);
}

test "scan: a call is a function whatever its name; a keyword before ( stays one; types are keywords" {
    const text = "SELECT trim(a), erp.QUERY($$x$$), x IN (1) FROM t WHERE CAST(b AS varchar) = lower (c)";
    var out: [96]Style = undefined;
    scan(text, out[0..text.len]);
    try std.testing.expectEqual(Style.func, out[7]);
    try std.testing.expectEqual(Style.func, out[20]);
    try std.testing.expectEqual(Style.plain, out[16]);
    try std.testing.expectEqual(Style.keyword, out[36]);
    try std.testing.expectEqual(Style.keyword, out[56]);
    try std.testing.expectEqual(Style.keyword, out[66]);
    try std.testing.expectEqual(Style.func, out[77]);
}

test "scan: a qualified name is not a keyword, a dollar-quoted body is a string" {
    const text = "SELECT t.count FROM erp.QUERY($$SELECT 1$$)";
    var out: [64]Style = undefined;
    scan(text, out[0..text.len]);
    try std.testing.expectEqual(Style.plain, out[9]);
    try std.testing.expectEqual(Style.string, out[32]);
}
