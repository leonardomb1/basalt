//! Lexer for the basalt SQL dialect (docs/language/scripts.md). Keywords are plain
//! `ident`s, matched case-insensitively by the parser. The token forms:
//!   - comments: `--` to end of line and `/* ... */` blocks
//!   - strings: '...' with `''` doubling (unescaped here)
//!   - "..." is an ANSI quoted identifier, the only way to name a column with a
//!     space or keyword in it; read as a string it would make
//!     `SELECT "Exchange rate"` a constant instead of the column.
//!   - raw SQL literals: $$...$$ / $tag$...$tag$ (dollar-quoting, verbatim body)
//!   - `$name` lexes as .dollar_ident (a PARAM reference)
//!   - numbers: `42` is .int; `2.5`, `.5` and `1e3` are .float, the parser telling
//!     an exact decimal (a point) from a float (an exponent)
//!   - `;` statement terminator, `<>` as not-equal, `||` concat over bitwise `|`

const std = @import("std");
const token = @import("token.zig");

const Token = token.Token;
const Tag = token.Tag;

pub const Lexer = struct {
    src: []const u8,
    i: usize = 0,
    line: u32 = 1,
    col: u32 = 1,
    alloc: std.mem.Allocator,

    pub fn init(alloc: std.mem.Allocator, src: []const u8) Lexer {
        return .{ .src = src, .alloc = alloc };
    }

    fn peek(self: *Lexer, off: usize) u8 {
        const j = self.i + off;
        return if (j < self.src.len) self.src[j] else 0;
    }

    fn bump(self: *Lexer) u8 {
        const c = self.src[self.i];
        self.i += 1;
        if (c == '\n') {
            self.line += 1;
            self.col = 1;
        } else {
            self.col += 1;
        }
        return c;
    }

    /// Skips blanks and comments; a `/*` never closed is returned as an invalid
    /// token, so the rest of the script is refused rather than silently dropped.
    fn skipTrivia(self: *Lexer) ?Token {
        while (self.i < self.src.len) {
            const c = self.src[self.i];
            if (c == ' ' or c == '\t' or c == '\r' or c == '\n') {
                _ = self.bump();
            } else if (c == '-' or c == '/') {
                const sp = spanAt(self.src, self.i) orelse break;
                const start = self.i;
                const line = self.line;
                const col = self.col;
                self.bumpTo(sp.end);
                if (!sp.closed) return self.make(.invalid, self.src[start..], line, col);
            } else break;
        }
        return null;
    }

    fn bumpTo(self: *Lexer, end: usize) void {
        while (self.i < end) _ = self.bump();
    }

    /// Called once the token is consumed, so the lexer's position is its end.
    fn make(self: *Lexer, tag: Tag, text: []const u8, line: u32, col: u32) Token {
        return .{ .tag = tag, .text = text, .line = line, .col = col, .end_line = self.line, .end_col = self.col };
    }

    fn isIdentStart(c: u8) bool {
        return (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z') or c == '_';
    }
    fn isIdentChar(c: u8) bool {
        return isIdentStart(c) or isDigit(c);
    }

    fn isDigit(c: u8) bool {
        return c >= '0' and c <= '9';
    }

    /// A byte after which `.` qualifies a name or path (`t.col`, `$job.0`, `f().x`)
    /// rather than starting a number like `.5`.
    fn continuesName(c: u8) bool {
        return isIdentChar(c) or c >= 0x80 or c == ')' or c == ']' or c == '"' or c == '$' or c == '`';
    }

    fn digits(self: *Lexer) void {
        while (self.i < self.src.len and isDigit(self.src[self.i])) _ = self.bump();
    }

    /// Unescape a single-quoted body: `''` -> `'`. Allocates only when needed.
    fn unquoteSingle(self: *Lexer, body: []const u8) ![]const u8 {
        if (std.mem.indexOf(u8, body, "''") == null) return body;
        var out = try std.array_list.Managed(u8).initCapacity(self.alloc, body.len);
        var k: usize = 0;
        while (k < body.len) : (k += 1) {
            out.appendAssumeCapacity(body[k]);
            if (body[k] == '\'' and k + 1 < body.len and body[k + 1] == '\'') k += 1;
        }
        return try out.toOwnedSlice();
    }

    /// Unescape a quoted identifier: ANSI doubles the delimiter, so `"a""b"` names the
    /// column `a"b`. A backslash is an ordinary character in a name.
    fn unquoteIdent(self: *Lexer, body: []const u8) ![]const u8 {
        if (std.mem.indexOfScalar(u8, body, '"') == null) return body;
        var out = try std.array_list.Managed(u8).initCapacity(self.alloc, body.len);
        var k: usize = 0;
        while (k < body.len) : (k += 1) {
            out.appendAssumeCapacity(body[k]);
            if (body[k] == '"' and k + 1 < body.len and body[k + 1] == '"') k += 1;
        }
        return try out.toOwnedSlice();
    }

    pub fn next(self: *Lexer) !Token {
        if (self.skipTrivia()) |open| return open;
        const line = self.line;
        const col = self.col;
        if (self.i >= self.src.len) return self.make(.eof, "", line, col);

        const c = self.src[self.i];

        if (isIdentStart(c)) {
            const start = self.i;
            while (self.i < self.src.len and isIdentChar(self.src[self.i])) _ = self.bump();
            return self.make(.ident, self.src[start..self.i], line, col);
        }

        if (isDigit(c) or (c == '.' and isDigit(self.peek(1)) and (self.i == 0 or !continuesName(self.src[self.i - 1])))) {
            const start = self.i;
            var is_float = false;
            self.digits();
            if (self.i < self.src.len and self.src[self.i] == '.' and isDigit(self.peek(1))) {
                is_float = true;
                _ = self.bump();
                self.digits();
            }
            const sign: usize = if (self.peek(1) == '+' or self.peek(1) == '-') 1 else 0;
            if ((self.peek(0) == 'e' or self.peek(0) == 'E') and isDigit(self.peek(1 + sign))) {
                is_float = true;
                for (0..1 + sign) |_| _ = self.bump();
                self.digits();
            }
            return self.make(if (is_float) .float else .int, self.src[start..self.i], line, col);
        }

        if (c == '\'' or c == '"') {
            const sp = spanAt(self.src, self.i).?;
            const start = self.i;
            self.bumpTo(sp.end);
            if (!sp.closed) return self.make(.invalid, self.src[start..], line, col);
            const body = self.src[start + 1 .. sp.end - 1];
            if (c == '\'') return self.make(.string, try self.unquoteSingle(body), line, col);
            if (body.len == 0) return self.make(.invalid, self.src[start..], line, col);
            return self.make(.qident, try self.unquoteIdent(body), line, col);
        }

        if (c == '$') {
            if (spanAt(self.src, self.i)) |sp| {
                const start = self.i;
                if (!sp.closed) {
                    self.i = self.src.len;
                    return self.make(.invalid, self.src[start..], line, col);
                }
                self.bumpTo(sp.end);
                return self.make(.string, self.src[start + sp.open .. sp.end - sp.open], line, col);
            }
            if (isIdentStart(self.peek(1))) {
                _ = self.bump();
                const nstart = self.i;
                while (self.i < self.src.len and isIdentChar(self.src[self.i])) _ = self.bump();
                return self.make(.dollar_ident, self.src[nstart..self.i], line, col);
            }
            _ = self.bump();
            return self.make(.invalid, self.src[self.i - 1 .. self.i], line, col);
        }

        const start = self.i;
        _ = self.bump();
        switch (c) {
            '(' => return self.make(.lparen, self.src[start..self.i], line, col),
            ')' => return self.make(.rparen, self.src[start..self.i], line, col),
            '[' => return self.make(.lbracket, self.src[start..self.i], line, col),
            ']' => return self.make(.rbracket, self.src[start..self.i], line, col),
            '{' => return self.make(.lbrace, self.src[start..self.i], line, col),
            '}' => return self.make(.rbrace, self.src[start..self.i], line, col),
            ',' => return self.make(.comma, self.src[start..self.i], line, col),
            '.' => return self.make(.dot, self.src[start..self.i], line, col),
            ':' => return self.make(.colon, self.src[start..self.i], line, col),
            ';' => return self.make(.semi, self.src[start..self.i], line, col),
            '*' => return self.make(.star, self.src[start..self.i], line, col),
            '+' => return self.make(.plus, self.src[start..self.i], line, col),
            '-' => {
                if (self.i < self.src.len and self.src[self.i] == '>') {
                    _ = self.bump();
                    return self.make(.arrow, self.src[start..self.i], line, col);
                }
                return self.make(.minus, self.src[start..self.i], line, col);
            },
            '/' => return self.make(.slash, self.src[start..self.i], line, col),
            '%' => return self.make(.percent, self.src[start..self.i], line, col),
            '&' => return self.make(.amp, self.src[start..self.i], line, col),
            '^' => return self.make(.caret, self.src[start..self.i], line, col),
            '~' => return self.make(.tilde, self.src[start..self.i], line, col),
            '|' => {
                if (self.i < self.src.len and self.src[self.i] == '|') {
                    _ = self.bump();
                    return self.make(.pipe, self.src[start..self.i], line, col);
                }
                return self.make(.bar, self.src[start..self.i], line, col);
            },
            '=' => {
                if (self.i < self.src.len and self.src[self.i] == '=') {
                    _ = self.bump();
                    return self.make(.eq, self.src[start..self.i], line, col);
                }
                return self.make(.assign, self.src[start..self.i], line, col);
            },
            '<' => {
                if (self.i < self.src.len and self.src[self.i] == '=') {
                    _ = self.bump();
                    return self.make(.le, self.src[start..self.i], line, col);
                }
                if (self.i < self.src.len and self.src[self.i] == '>') {
                    _ = self.bump();
                    return self.make(.ne, self.src[start..self.i], line, col);
                }
                if (self.i < self.src.len and self.src[self.i] == '<') {
                    _ = self.bump();
                    return self.make(.shl, self.src[start..self.i], line, col);
                }
                return self.make(.lt, self.src[start..self.i], line, col);
            },
            '>' => {
                if (self.i < self.src.len and self.src[self.i] == '=') {
                    _ = self.bump();
                    return self.make(.ge, self.src[start..self.i], line, col);
                }
                if (self.i < self.src.len and self.src[self.i] == '>') {
                    _ = self.bump();
                    return self.make(.shr, self.src[start..self.i], line, col);
                }
                return self.make(.gt, self.src[start..self.i], line, col);
            },
            '!' => {
                if (self.i < self.src.len and self.src[self.i] == '=') {
                    _ = self.bump();
                    return self.make(.ne, self.src[start..self.i], line, col);
                }
                return self.make(.invalid, self.src[start..self.i], line, col);
            },
            '?' => {
                if (self.i < self.src.len and self.src[self.i] == '?') {
                    _ = self.bump();
                    return self.make(.qq, self.src[start..self.i], line, col);
                }
                if (self.i < self.src.len and self.src[self.i] == '.') {
                    _ = self.bump();
                    return self.make(.qdot, self.src[start..self.i], line, col);
                }
                return self.make(.invalid, self.src[start..self.i], line, col);
            },
            else => {
                const n = std.unicode.utf8ByteSequenceLength(c) catch 1;
                if (n > 1 and start + n <= self.src.len and std.unicode.utf8ValidateSlice(self.src[start .. start + n]))
                    while (self.i < start + n) {
                        _ = self.bump();
                    };
                return self.make(.invalid, self.src[start..self.i], line, col);
            },
        }
    }
};

pub const SpanKind = enum { string, quoted_ident, line_comment, block_comment, dollar_quote };

/// A span the lexer reads whole: `end` is just past it (`src.len` when it never
/// closes), `open` its opener's length (`'` 1, `--` 2, `$tag$` 5); a line comment
/// is always closed, ending before its newline.
pub const Span = struct { kind: SpanKind, end: usize, open: usize, closed: bool };

/// The string, quoted name, comment or dollar quote opening at `src[at]`, by the
/// lexer's rules (a doubled quote stays inside, a backslash is ordinary), or null
/// when none opens there. Never fails on unfinished text, so editors share it.
pub fn spanAt(src: []const u8, at: usize) ?Span {
    const c = src[at];
    const next: u8 = if (at + 1 < src.len) src[at + 1] else 0;
    switch (c) {
        '\'', '"' => {
            const kind: SpanKind = if (c == '\'') .string else .quoted_ident;
            var i = at + 1;
            while (i < src.len) : (i += 1) {
                if (src[i] != c) continue;
                if (i + 1 < src.len and src[i + 1] == c) {
                    i += 1;
                    continue;
                }
                return .{ .kind = kind, .end = i + 1, .open = 1, .closed = true };
            }
            return .{ .kind = kind, .end = src.len, .open = 1, .closed = false };
        },
        '-' => if (next == '-') {
            const end = std.mem.indexOfScalarPos(u8, src, at, '\n') orelse src.len;
            return .{ .kind = .line_comment, .end = end, .open = 2, .closed = true };
        },
        '/' => if (next == '*') {
            const close = std.mem.indexOfPos(u8, src, at + 2, "*/");
            return .{ .kind = .block_comment, .end = if (close) |p| p + 2 else src.len, .open = 2, .closed = close != null };
        },
        '$' => if (dollarOpener(src, at)) |n| {
            const close = std.mem.indexOfPos(u8, src, at + n, src[at .. at + n]);
            return .{ .kind = .dollar_quote, .end = if (close) |p| p + n else src.len, .open = n, .closed = close != null };
        },
        else => {},
    }
    return null;
}

/// Length of the dollar-quote opener at `src[at]` (`$$`, `$tag$`), or null when
/// this `$` starts a `$name` reference or nothing.
fn dollarOpener(src: []const u8, at: usize) ?usize {
    var j = at + 1;
    if (j < src.len and src[j] == '$') return 2;
    if (j >= src.len or !Lexer.isIdentStart(src[j])) return null;
    while (j < src.len and Lexer.isIdentChar(src[j])) j += 1;
    if (j >= src.len or src[j] != '$') return null;
    return j + 1 - at;
}

pub fn tokenize(alloc: std.mem.Allocator, src: []const u8) ![]Token {
    var lx = Lexer.init(alloc, src);
    var list = std.array_list.Managed(Token).init(alloc);
    while (true) {
        const t = try lx.next();
        try list.append(t);
        if (t.tag == .eof) break;
    }
    return try list.toOwnedSlice();
}

const testing = std.testing;

test "sql lexer: keywords, strings, dollar quoting, params" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const toks = try tokenize(a, "SELECT 'it''s' FROM $job.tables WHERE x <> 1; -- c\n$$D <> '*'$$ $sql$a$$b$sql$");
    try testing.expectEqual(token.Tag.ident, toks[0].tag);
    try testing.expectEqual(token.Tag.string, toks[1].tag);
    try testing.expectEqualStrings("it's", toks[1].text);
    try testing.expectEqual(token.Tag.ident, toks[2].tag);
    try testing.expectEqual(token.Tag.dollar_ident, toks[3].tag);
    try testing.expectEqualStrings("job", toks[3].text);
    try testing.expectEqual(token.Tag.dot, toks[4].tag);
    try testing.expectEqualStrings("tables", toks[5].text);
    try testing.expectEqual(token.Tag.ident, toks[6].tag);
    try testing.expectEqual(token.Tag.ne, toks[8].tag);
    try testing.expectEqual(token.Tag.semi, toks[10].tag);
    try testing.expectEqual(token.Tag.string, toks[11].tag);
    try testing.expectEqualStrings("D <> '*'", toks[11].text);
    try testing.expectEqual(token.Tag.string, toks[12].tag);
    try testing.expectEqualStrings("a$$b", toks[12].text);
    try testing.expectEqual(token.Tag.eof, toks[13].tag);
}

test "sql lexer: numbers with a leading point or an exponent; a point after a name qualifies it" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const toks = try tokenize(a, ".5 1e3 2.5E-1 7e+2 1e t.5 f().5 3");
    const want = [_]struct { tag: token.Tag, text: []const u8 }{
        .{ .tag = .float, .text = ".5" },     .{ .tag = .float, .text = "1e3" },
        .{ .tag = .float, .text = "2.5E-1" }, .{ .tag = .float, .text = "7e+2" },
        .{ .tag = .int, .text = "1" },        .{ .tag = .ident, .text = "e" },
        .{ .tag = .ident, .text = "t" },      .{ .tag = .dot, .text = "." },
        .{ .tag = .int, .text = "5" },        .{ .tag = .ident, .text = "f" },
        .{ .tag = .lparen, .text = "(" },     .{ .tag = .rparen, .text = ")" },
        .{ .tag = .dot, .text = "." },        .{ .tag = .int, .text = "5" },
        .{ .tag = .int, .text = "3" },
    };
    for (want, toks[0..want.len]) |w, t| {
        try testing.expectEqual(w.tag, t.tag);
        try testing.expectEqualStrings(w.text, t.text);
    }
}

test "sql lexer: block comments and operators" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const toks = try tokenize(a, "a /* x */ = b != c ?? d ?. e");
    try testing.expectEqual(token.Tag.assign, toks[1].tag);
    try testing.expectEqual(token.Tag.ne, toks[3].tag);
    try testing.expectEqual(token.Tag.qq, toks[5].tag);
    try testing.expectEqual(token.Tag.qdot, toks[7].tag);
}

test "sql lexer: an arrow spans two columns, so the tokens after it keep their columns" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const toks = try tokenize(a, "x -> x + )");
    try testing.expectEqual(token.Tag.arrow, toks[1].tag);
    try testing.expectEqual(@as(u32, 3), toks[1].col);
    try testing.expectEqual(@as(u32, 5), toks[1].end_col);
    try testing.expectEqual(@as(u32, 6), toks[2].col);
    try testing.expectEqual(@as(u32, 10), toks[4].col);
}

test "sql lexer: bitwise operators stay distinct from concat and comparisons" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const toks = try tokenize(a, "a & b | c || d ^ ~e << 2 >> 3 < 4 > 5 <= 6 >= 7 <> 8");
    const want = [_]token.Tag{
        .ident, .amp, .ident, .bar, .ident, .pipe, .ident, .caret, .tilde, .ident,
        .shl,   .int, .shr,   .int, .lt,    .int,  .gt,    .int,   .le,    .int,
        .ge,    .int, .ne,    .int, .eof,
    };
    try testing.expectEqual(want.len, toks.len);
    for (want, toks) |w, t| try testing.expectEqual(w, t.tag);
}
