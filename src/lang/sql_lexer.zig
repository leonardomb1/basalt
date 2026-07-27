//! Lexer for the Basalt SQL dialect (language.md). Shares `token.Token` with
//! the BSL lexer; keywords are plain `ident`s matched case-insensitively by the
//! SQL parser. Differences from the BSL lexer:
//!   - comments: `--` to end of line and `/* ... */` blocks (not `#`)
//!   - strings: '...' with `''` doubling (unescaped here), plus "..." kept with
//!     the BSL backslash rules for interp-friendly quoting
//!   - raw SQL literals: $$...$$ / $tag$...$tag$ (dollar-quoting, verbatim body)
//!   - `$name` lexes as .dollar_ident (a PARAM reference)
//!   - `;` statement terminator, `<>` as not-equal

const std = @import("std");
const tok = @import("token.zig");

const Token = tok.Token;
const Tag = tok.Tag;

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

    fn skipTrivia(self: *Lexer) void {
        while (self.i < self.src.len) {
            const c = self.src[self.i];
            if (c == ' ' or c == '\t' or c == '\r' or c == '\n') {
                _ = self.bump();
            } else if (c == '-' and self.peek(1) == '-') {
                while (self.i < self.src.len and self.src[self.i] != '\n') _ = self.bump();
            } else if (c == '/' and self.peek(1) == '*') {
                _ = self.bump();
                _ = self.bump();
                while (self.i < self.src.len) {
                    if (self.src[self.i] == '*' and self.peek(1) == '/') {
                        _ = self.bump();
                        _ = self.bump();
                        break;
                    }
                    _ = self.bump();
                }
            } else break;
        }
    }

    fn make(self: *Lexer, tag: Tag, text: []const u8, line: u32, col: u32) Token {
        _ = self;
        return .{ .tag = tag, .text = text, .line = line, .col = col };
    }

    fn isIdentStart(c: u8) bool {
        return (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z') or c == '_';
    }
    fn isIdentChar(c: u8) bool {
        return isIdentStart(c) or (c >= '0' and c <= '9');
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

    /// Unescape a quoted identifier: ANSI doubles the delimiter, so `"a""b"`
    /// names the column `a"b`. A backslash is an ordinary character in a name.
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
        self.skipTrivia();
        const line = self.line;
        const col = self.col;
        if (self.i >= self.src.len) return self.make(.eof, "", line, col);

        const c = self.src[self.i];

        if (isIdentStart(c)) {
            const start = self.i;
            while (self.i < self.src.len and isIdentChar(self.src[self.i])) _ = self.bump();
            return self.make(.ident, self.src[start..self.i], line, col);
        }

        if (c >= '0' and c <= '9') {
            const start = self.i;
            var is_float = false;
            while (self.i < self.src.len and self.src[self.i] >= '0' and self.src[self.i] <= '9') _ = self.bump();
            if (self.i + 1 < self.src.len and self.src[self.i] == '.' and
                self.src[self.i + 1] >= '0' and self.src[self.i + 1] <= '9')
            {
                is_float = true;
                _ = self.bump();
                while (self.i < self.src.len and self.src[self.i] >= '0' and self.src[self.i] <= '9') _ = self.bump();
            }
            return self.make(if (is_float) .float else .int, self.src[start..self.i], line, col);
        }

        if (c == '\'') {
            _ = self.bump();
            const start = self.i;
            while (self.i < self.src.len) {
                if (self.src[self.i] == '\'') {
                    if (self.peek(1) == '\'') {
                        _ = self.bump();
                        _ = self.bump();
                        continue;
                    }
                    break;
                }
                _ = self.bump();
            }
            if (self.i >= self.src.len) return self.make(.invalid, self.src[start - 1 ..], line, col);
            const body = self.src[start..self.i];
            _ = self.bump();
            return self.make(.string, try self.unquoteSingle(body), line, col);
        }

        // ANSI quoted identifier. `"x"` names a column — the only way to reach
        // one whose name has a space or a keyword in it. It was a second string
        // syntax before v0.4.6, which made `SELECT "Exchange rate"` a constant
        // repeated down the column instead of the column itself, silently.
        if (c == '"') {
            _ = self.bump();
            const start = self.i;
            while (self.i < self.src.len) {
                if (self.src[self.i] == '"') {
                    // A doubled quote is an escaped one, not the end.
                    if (self.i + 1 < self.src.len and self.src[self.i + 1] == '"') {
                        _ = self.bump();
                        _ = self.bump();
                        continue;
                    }
                    break;
                }
                _ = self.bump();
            }
            if (self.i >= self.src.len) return self.make(.invalid, self.src[start - 1 ..], line, col);
            const body = self.src[start..self.i];
            _ = self.bump();
            if (body.len == 0) return self.make(.invalid, self.src[start - 1 ..], line, col);
            return self.make(.qident, try self.unquoteIdent(body), line, col);
        }

        if (c == '$') {
            if (self.peek(1) == '$') {
                _ = self.bump();
                _ = self.bump();
                const start = self.i;
                const end = std.mem.indexOfPos(u8, self.src, self.i, "$$") orelse {
                    const rest = self.src[start - 2 ..];
                    self.i = self.src.len;
                    return self.make(.invalid, rest, line, col);
                };
                const body = self.src[start..end];
                while (self.i < end + 2) _ = self.bump();
                return self.make(.string, body, line, col);
            }
            if (isIdentStart(self.peek(1))) {
                _ = self.bump();
                const nstart = self.i;
                while (self.i < self.src.len and isIdentChar(self.src[self.i])) _ = self.bump();
                const name = self.src[nstart..self.i];
                if (self.i < self.src.len and self.src[self.i] == '$') {
                    _ = self.bump();
                    const closer = std.fmt.allocPrint(self.alloc, "${s}$", .{name}) catch return error.OutOfMemory;
                    const start = self.i;
                    const end = std.mem.indexOfPos(u8, self.src, self.i, closer) orelse {
                        const rest = self.src[nstart - 1 ..];
                        self.i = self.src.len;
                        return self.make(.invalid, rest, line, col);
                    };
                    const body = self.src[start..end];
                    while (self.i < end + closer.len) _ = self.bump();
                    return self.make(.string, body, line, col);
                }
                return self.make(.dollar_ident, name, line, col);
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
            '-' => return self.make(.minus, self.src[start..self.i], line, col),
            '/' => return self.make(.slash, self.src[start..self.i], line, col),
            '%' => return self.make(.percent, self.src[start..self.i], line, col),
            '&' => return self.make(.amp, self.src[start..self.i], line, col),
            '^' => return self.make(.caret, self.src[start..self.i], line, col),
            '~' => return self.make(.tilde, self.src[start..self.i], line, col),
            '|' => {
                // `||` (concat) must win over the bitwise `|`.
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
            else => return self.make(.invalid, self.src[start..self.i], line, col),
        }
    }
};

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
    try testing.expectEqual(tok.Tag.ident, toks[0].tag);
    try testing.expectEqual(tok.Tag.string, toks[1].tag);
    try testing.expectEqualStrings("it's", toks[1].text);
    try testing.expectEqual(tok.Tag.ident, toks[2].tag);
    try testing.expectEqual(tok.Tag.dollar_ident, toks[3].tag);
    try testing.expectEqualStrings("job", toks[3].text);
    try testing.expectEqual(tok.Tag.dot, toks[4].tag);
    try testing.expectEqualStrings("tables", toks[5].text);
    try testing.expectEqual(tok.Tag.ident, toks[6].tag);
    try testing.expectEqual(tok.Tag.ne, toks[8].tag);
    try testing.expectEqual(tok.Tag.semi, toks[10].tag);
    try testing.expectEqual(tok.Tag.string, toks[11].tag);
    try testing.expectEqualStrings("D <> '*'", toks[11].text);
    try testing.expectEqual(tok.Tag.string, toks[12].tag);
    try testing.expectEqualStrings("a$$b", toks[12].text);
    try testing.expectEqual(tok.Tag.eof, toks[13].tag);
}

test "sql lexer: block comments and operators" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const toks = try tokenize(a, "a /* x */ = b != c ?? d ?. e");
    try testing.expectEqual(tok.Tag.assign, toks[1].tag);
    try testing.expectEqual(tok.Tag.ne, toks[3].tag);
    try testing.expectEqual(tok.Tag.qq, toks[5].tag);
    try testing.expectEqual(tok.Tag.qdot, toks[7].tag);
}

test "sql lexer: bitwise operators stay distinct from concat and comparisons" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const toks = try tokenize(a, "a & b | c || d ^ ~e << 2 >> 3 < 4 > 5 <= 6 >= 7 <> 8");
    const want = [_]tok.Tag{
        .ident, .amp,   .ident, .bar, .ident, .pipe,  .ident, .caret, .tilde, .ident,
        .shl,   .int,   .shr,   .int, .lt,    .int,   .gt,    .int,   .le,    .int,
        .ge,    .int,   .ne,    .int, .eof,
    };
    try testing.expectEqual(want.len, toks.len);
    for (want, toks) |w, t| try testing.expectEqual(w, t.tag);
}
