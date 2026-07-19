//! Hand-written lexer. Skips whitespace (incl. newlines) and `#` line comments,
//! then emits one token at a time. Numbers split a trailing unit suffix into a
//! separate ident (so `100mb` lexes as int(100) + ident(mb)).

const std = @import("std");
const tok = @import("token.zig");

const Token = tok.Token;
const Tag = tok.Tag;

pub const Lexer = struct {
    src: []const u8,
    pos: usize = 0,
    line: u32 = 1,
    col: u32 = 1,

    pub fn init(src: []const u8) Lexer {
        return .{ .src = src };
    }

    fn peek(self: *Lexer) ?u8 {
        if (self.pos >= self.src.len) return null;
        return self.src[self.pos];
    }

    fn peek1(self: *Lexer) ?u8 {
        if (self.pos + 1 >= self.src.len) return null;
        return self.src[self.pos + 1];
    }

    fn bump(self: *Lexer) void {
        const c = self.src[self.pos];
        self.pos += 1;
        if (c == '\n') {
            self.line += 1;
            self.col = 1;
        } else {
            self.col += 1;
        }
    }

    fn skipTrivia(self: *Lexer) void {
        while (self.peek()) |c| {
            switch (c) {
                ' ', '\t', '\r', '\n' => self.bump(),
                '#' => while (self.peek()) |d| {
                    if (d == '\n') break;
                    self.bump();
                },
                else => break,
            }
        }
    }

    pub fn next(self: *Lexer) Token {
        self.skipTrivia();
        const start = self.pos;
        const sline = self.line;
        const scol = self.col;

        const c = self.peek() orelse return .{ .tag = .eof, .text = "", .line = sline, .col = scol };

        if (isIdentStart(c)) {
            while (self.peek()) |d| {
                if (!isIdentCont(d)) break;
                self.bump();
            }
            return self.make(.ident, start, sline, scol);
        }
        if (std.ascii.isDigit(c)) return self.lexNumber(start, sline, scol);
        if (c == '"') return self.lexString(sline, scol);
        if (c == '$' and self.peek1() == '{') return self.lexInterp(start, sline, scol);
        return self.lexPunct(start, sline, scol);
    }

    fn lexNumber(self: *Lexer, start: usize, sline: u32, scol: u32) Token {
        while (self.peek()) |d| {
            if (!std.ascii.isDigit(d)) break;
            self.bump();
        }
        // fractional part only if a digit follows the dot (so `a.b` stays separate)
        if (self.peek() == '.') {
            if (self.peek1()) |d2| {
                if (std.ascii.isDigit(d2)) {
                    self.bump(); // '.'
                    while (self.peek()) |d| {
                        if (!std.ascii.isDigit(d)) break;
                        self.bump();
                    }
                    return self.make(.float, start, sline, scol);
                }
            }
        }
        return self.make(.int, start, sline, scol);
    }

    /// `${ <body> }` in a bare-name position — a template placeholder. The body is any
    /// expression text (`name`, `lower(name)`, `if(pk == "", concat(name, "id"), pk)`,
    /// …); it is captured verbatim and parsed/evaluated at render time. The token text
    /// is the whole lexeme (including `${` and `}`).
    fn lexInterp(self: *Lexer, start: usize, sline: u32, scol: u32) Token {
        const body_start = self.pos + 2; // after `${`
        const closed = self.skipInterpHole();
        if (!closed) return self.make(.invalid, start, sline, scol); // unterminated
        if (self.pos - 1 == body_start) return self.make(.invalid, start, sline, scol); // empty `${}`
        return self.make(.interp, start, sline, scol);
    }

    // Scan a `"..."` body, honoring `\` escapes so an escaped `\"` does not end the
    // string. A `${ ... }` interpolation hole is scanned through brace-balanced and
    // string-aware (C#-style): nested `"..."` inside a hole do NOT end the outer
    // string, so `"${if(pk == "", a, b)}"` needs no escaping. The token text is the
    // raw inner slice; `tokenize` resolves any `\` escapes (see `unescape`) after.
    fn lexString(self: *Lexer, sline: u32, scol: u32) Token {
        self.bump(); // opening quote
        const inner_start = self.pos;
        while (self.peek()) |c| {
            if (c == '\\') {
                self.bump(); // backslash
                if (self.peek() != null) self.bump(); // escaped char
                continue;
            }
            if (c == '$' and self.peek1() == '{') {
                _ = self.skipInterpHole();
                continue;
            }
            if (c == '"') {
                const inner = self.src[inner_start..self.pos];
                self.bump(); // closing quote
                return .{ .tag = .string, .text = inner, .line = sline, .col = scol };
            }
            self.bump();
        }
        // unterminated string
        return .{ .tag = .invalid, .text = self.src[inner_start..self.pos], .line = sline, .col = scol };
    }

    /// Consume a `${ ... }` interpolation hole (the cursor is on the `$`), scanning
    /// brace-balanced and string-aware so a `"` or `}` inside a nested string does
    /// not end the hole. Returns true if it consumed the matching `}`, false on
    /// end-of-input (unbalanced). Shared by `lexString` and `lexInterp`.
    fn skipInterpHole(self: *Lexer) bool {
        self.bump(); // $
        self.bump(); // {
        var depth: usize = 1;
        while (self.peek()) |c| {
            switch (c) {
                '"' => {
                    self.bump();
                    while (self.peek()) |d| {
                        if (d == '\\') {
                            self.bump();
                            if (self.peek() != null) self.bump();
                            continue;
                        }
                        if (d == '"') {
                            self.bump();
                            break;
                        }
                        self.bump();
                    }
                },
                '{' => {
                    depth += 1;
                    self.bump();
                },
                '}' => {
                    depth -= 1;
                    self.bump();
                    if (depth == 0) return true;
                },
                else => self.bump(),
            }
        }
        return false;
    }

    fn lexPunct(self: *Lexer, start: usize, sline: u32, scol: u32) Token {
        const c = self.peek().?;
        switch (c) {
            '|' => {
                self.bump();
                return self.make(.pipe, start, sline, scol);
            },
            '@' => {
                self.bump();
                return self.make(.at, start, sline, scol);
            },
            '(' => return self.single(.lparen, start, sline, scol),
            ')' => return self.single(.rparen, start, sline, scol),
            '[' => return self.single(.lbracket, start, sline, scol),
            ']' => return self.single(.rbracket, start, sline, scol),
            '{' => return self.single(.lbrace, start, sline, scol),
            '}' => return self.single(.rbrace, start, sline, scol),
            ',' => return self.single(.comma, start, sline, scol),
            '.' => return self.single(.dot, start, sline, scol),
            ':' => return self.single(.colon, start, sline, scol),
            '*' => return self.single(.star, start, sline, scol),
            '+' => return self.single(.plus, start, sline, scol),
            '-' => return self.single(.minus, start, sline, scol),
            '/' => return self.single(.slash, start, sline, scol),
            '%' => return self.single(.percent, start, sline, scol),
            '?' => {
                self.bump();
                if (self.peek() == '?') {
                    self.bump();
                    return self.make(.qq, start, sline, scol);
                }
                if (self.peek() == '.') {
                    self.bump();
                    return self.make(.qdot, start, sline, scol);
                }
                return self.make(.invalid, start, sline, scol);
            },
            '=' => {
                self.bump();
                if (self.peek() == '=') {
                    self.bump();
                    return self.make(.eq, start, sline, scol);
                }
                if (self.peek() == '>') {
                    self.bump();
                    return self.make(.fat_arrow, start, sline, scol);
                }
                return self.make(.assign, start, sline, scol);
            },
            '!' => {
                self.bump();
                if (self.peek() == '=') {
                    self.bump();
                    return self.make(.ne, start, sline, scol);
                }
                return self.make(.invalid, start, sline, scol);
            },
            '<' => {
                self.bump();
                if (self.peek() == '=') {
                    self.bump();
                    return self.make(.le, start, sline, scol);
                }
                return self.make(.lt, start, sline, scol);
            },
            '>' => {
                self.bump();
                if (self.peek() == '=') {
                    self.bump();
                    return self.make(.ge, start, sline, scol);
                }
                return self.make(.gt, start, sline, scol);
            },
            else => return self.single(.invalid, start, sline, scol),
        }
    }

    fn single(self: *Lexer, tag: Tag, start: usize, sline: u32, scol: u32) Token {
        self.bump();
        return self.make(tag, start, sline, scol);
    }

    fn make(self: *Lexer, tag: Tag, start: usize, sline: u32, scol: u32) Token {
        return .{ .tag = tag, .text = self.src[start..self.pos], .line = sline, .col = scol };
    }
};

fn isIdentStart(c: u8) bool {
    return std.ascii.isAlphabetic(c) or c == '_';
}

fn isIdentCont(c: u8) bool {
    return std.ascii.isAlphanumeric(c) or c == '_';
}

/// Translate backslash escapes in a string-literal body into their byte values.
/// Recognized: `\"` `\\` `\n` `\t` `\r` `\'`. An unrecognized escape (`\x`) is
/// kept verbatim (backslash and all) so data like Windows paths survives. The
/// result is allocated in `alloc`; only called when the body contains a `\`.
fn unescape(alloc: std.mem.Allocator, s: []const u8) ![]const u8 {
    var out = std.array_list.Managed(u8).init(alloc);
    errdefer out.deinit();
    var i: usize = 0;
    while (i < s.len) : (i += 1) {
        if (s[i] != '\\' or i + 1 >= s.len) {
            try out.append(s[i]);
            continue;
        }
        i += 1;
        switch (s[i]) {
            '"' => try out.append('"'),
            '\\' => try out.append('\\'),
            'n' => try out.append('\n'),
            't' => try out.append('\t'),
            'r' => try out.append('\r'),
            '\'' => try out.append('\''),
            else => |e| {
                try out.append('\\'); // unknown escape: keep both bytes
                try out.append(e);
            },
        }
    }
    return out.toOwnedSlice();
}

/// Lex the whole source into a slice ending with an `eof` token. Caller frees.
pub fn tokenize(alloc: std.mem.Allocator, src: []const u8) ![]Token {
    var lx = Lexer.init(src);
    var list = std.array_list.Managed(Token).init(alloc);
    errdefer list.deinit();
    while (true) {
        var t = lx.next();
        // String bodies are lexed raw (slice into src); resolve their escapes
        // here, the single point every `.string` token flows through.
        if (t.tag == .string and std.mem.indexOfScalar(u8, t.text, '\\') != null) {
            t.text = try unescape(alloc, t.text);
        }
        try list.append(t);
        if (t.tag == .eof) break;
    }
    return list.toOwnedSlice();
}

test "lex a pipeline into tags" {
    const alloc = std.testing.allocator;
    const toks = try tokenize(alloc, "read mssql table public.orders | filter total > 0");
    defer alloc.free(toks);

    const expect = [_]Tag{ .ident, .ident, .ident, .ident, .dot, .ident, .pipe, .ident, .ident, .gt, .int, .eof };
    try std.testing.expectEqual(expect.len, toks.len);
    for (expect, toks) |e, t| try std.testing.expectEqual(e, t.tag);
}

test "lex string content and number kinds" {
    const alloc = std.testing.allocator;
    const toks = try tokenize(alloc, "query \"select 1\" limit 100 1.5");
    defer alloc.free(toks);

    try std.testing.expectEqual(Tag.ident, toks[0].tag);
    try std.testing.expectEqual(Tag.string, toks[1].tag);
    try std.testing.expectEqualStrings("select 1", toks[1].text);
    try std.testing.expectEqual(Tag.ident, toks[2].tag);
    try std.testing.expectEqual(Tag.int, toks[3].tag);
    try std.testing.expectEqualStrings("100", toks[3].text);
    try std.testing.expectEqual(Tag.float, toks[4].tag);
    try std.testing.expectEqualStrings("1.5", toks[4].text);
}

test "string escapes are resolved" {
    const alloc = std.testing.allocator;
    // \" -> " (embedded SQL identifier quoting), \\ -> \, and an unknown escape kept verbatim.
    const toks = try tokenize(alloc, "query \"where \\\"updated_at\\\" > 0\" \"a\\\\b\" \"c\\d\"");
    defer {
        // toks[1..3] contained escapes, so their text is heap-allocated by unescape.
        alloc.free(toks[1].text);
        alloc.free(toks[2].text);
        alloc.free(toks[3].text);
        alloc.free(toks);
    }
    try std.testing.expectEqualStrings("where \"updated_at\" > 0", toks[1].text);
    try std.testing.expectEqualStrings("a\\b", toks[2].text);
    try std.testing.expectEqualStrings("c\\d", toks[3].text);
}

test "lex operators, annotations, and unit suffix split" {
    const alloc = std.testing.allocator;
    const toks = try tokenize(alloc, "a == b != c <= d >= @[batch_bytes = 100mb]");
    defer alloc.free(toks);

    const expect = [_]Tag{ .ident, .eq, .ident, .ne, .ident, .le, .ident, .ge, .at, .lbracket, .ident, .assign, .int, .ident, .rbracket, .eof };
    try std.testing.expectEqual(expect.len, toks.len);
    for (expect, toks) |e, t| try std.testing.expectEqual(e, t.tag);
}

test "lex null-coalesce operator" {
    const alloc = std.testing.allocator;
    const toks = try tokenize(alloc, "a ?? b");
    defer alloc.free(toks);
    const expect = [_]Tag{ .ident, .qq, .ident, .eof };
    try std.testing.expectEqual(expect.len, toks.len);
    for (expect, toks) |e, t| try std.testing.expectEqual(e, t.tag);
}

test "lex interpolation placeholders" {
    const alloc = std.testing.allocator;
    const toks = try tokenize(alloc, "read mssql table dbo.${tbl} | write sr ${tbl}");
    defer alloc.free(toks);
    const expect = [_]Tag{ .ident, .ident, .ident, .ident, .dot, .interp, .pipe, .ident, .ident, .interp, .eof };
    try std.testing.expectEqual(expect.len, toks.len);
    for (expect, toks) |e, t| try std.testing.expectEqual(e, t.tag);
    try std.testing.expectEqualStrings("${tbl}", toks[5].text);
}

test "string with an interpolation hole keeps nested quotes (C#-style, no escaping)" {
    const alloc = std.testing.allocator;
    // The inner `"" ` and `"id"` must NOT terminate the outer string.
    const toks = try tokenize(alloc, "select k = \"${if(pk == \"\", concat(name, \"id\"), pk)}\"");
    defer alloc.free(toks);
    // select, k, =, <string>, eof
    try std.testing.expectEqual(Tag.string, toks[3].tag);
    try std.testing.expectEqualStrings("${if(pk == \"\", concat(name, \"id\"), pk)}", toks[3].text);
    try std.testing.expectEqual(Tag.eof, toks[4].tag);
}

test "interpolation hole with a literal prefix is one string token" {
    const alloc = std.testing.allocator;
    const toks = try tokenize(alloc, "write csv \"crm_${lower(name)}\"");
    defer alloc.free(toks);
    try std.testing.expectEqual(Tag.string, toks[2].tag);
    try std.testing.expectEqualStrings("crm_${lower(name)}", toks[2].text);
}

test "lex safe-nav and fat-arrow" {
    const alloc = std.testing.allocator;
    const toks = try tokenize(alloc, "a?.b => c");
    defer alloc.free(toks);
    const expect = [_]Tag{ .ident, .qdot, .ident, .fat_arrow, .ident, .eof };
    try std.testing.expectEqual(expect.len, toks.len);
    for (expect, toks) |e, t| try std.testing.expectEqual(e, t.tag);
}

test "lone `?` and `!` are invalid tokens" {
    const alloc = std.testing.allocator;
    const toks = try tokenize(alloc, "a ? b");
    defer alloc.free(toks);
    try std.testing.expectEqual(Tag.invalid, toks[1].tag);

    const toks2 = try tokenize(alloc, "a ! b");
    defer alloc.free(toks2);
    try std.testing.expectEqual(Tag.invalid, toks2[1].tag);
}

test "a dot with no following digit ends the number" {
    const alloc = std.testing.allocator;
    // `1.x` must stay int(1) . ident(x) (so `a.b` field paths work); a trailing
    // `2.` is int(2) followed by a bare dot, not a float.
    const toks = try tokenize(alloc, "1.x 2.");
    defer alloc.free(toks);
    const expect = [_]Tag{ .int, .dot, .ident, .int, .dot, .eof };
    try std.testing.expectEqual(expect.len, toks.len);
    for (expect, toks) |e, t| try std.testing.expectEqual(e, t.tag);
    try std.testing.expectEqualStrings("1", toks[0].text);
    try std.testing.expectEqualStrings("2", toks[3].text);
}

test "unterminated and empty interpolation are invalid" {
    const alloc = std.testing.allocator;
    const toks = try tokenize(alloc, "${tbl");
    defer alloc.free(toks);
    try std.testing.expectEqual(Tag.invalid, toks[0].tag);

    const toks2 = try tokenize(alloc, "${}");
    defer alloc.free(toks2);
    try std.testing.expectEqual(Tag.invalid, toks2[0].tag);
}

test "comments are trivia; line/col positions survive newlines" {
    const alloc = std.testing.allocator;
    const toks = try tokenize(alloc, "a # note\n  b");
    defer alloc.free(toks);
    // the comment vanishes: just a, b, eof
    try std.testing.expectEqual(@as(usize, 3), toks.len);
    try std.testing.expectEqual(@as(u32, 1), toks[0].line);
    try std.testing.expectEqual(@as(u32, 1), toks[0].col);
    try std.testing.expectEqual(Tag.ident, toks[1].tag);
    try std.testing.expectEqualStrings("b", toks[1].text);
    try std.testing.expectEqual(@as(u32, 2), toks[1].line);
    try std.testing.expectEqual(@as(u32, 3), toks[1].col);
}

test "unterminated string is invalid" {
    const alloc = std.testing.allocator;
    const toks = try tokenize(alloc, "x \"oops");
    defer alloc.free(toks);
    try std.testing.expectEqual(Tag.ident, toks[0].tag);
    try std.testing.expectEqual(Tag.invalid, toks[1].tag);
}

/// Tokenizing arbitrary bytes must never crash (OOB read, integer-overflow panic,
/// unreachable) — only return tokens or an allocator error. The string-escape and
/// brace-balanced `${...}` hole scanners are the riskiest spots. Run the real fuzzer
/// with `zig build test --fuzz`; without it this just exercises the seed corpus.
fn fuzzTokenize(_: void, input: []const u8) anyerror!void {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    _ = tokenize(arena.allocator(), input) catch {};
}

test "fuzz: tokenize never crashes on arbitrary input" {
    try std.testing.fuzz({}, fuzzTokenize, .{
        .corpus = &.{ "read x table a.b | filter c > 0", "\"${if(p == \"\", a, b)}\"", "a?.b.c", "1.5 100mb \\d", "\"unterminated", "${" },
    });
}
