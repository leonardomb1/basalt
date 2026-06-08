//! Hand-written lexer. Skips whitespace (incl. newlines) and `#` line comments,
//! then emits one token at a time. Numbers split a trailing unit suffix into a
//! separate ident (so `100mb` lexes as int(100) + ident(mb) for the hint parser).

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

    /// `${ident}` — a template placeholder. The token text is the whole lexeme
    /// (including `${` and `}`) so the planner can render it by literal replacement.
    fn lexInterp(self: *Lexer, start: usize, sline: u32, scol: u32) Token {
        self.bump(); // $
        self.bump(); // {
        const name_start = self.pos;
        while (self.peek()) |d| {
            if (!isIdentCont(d)) break;
            self.bump();
        }
        if (self.pos == name_start or self.peek() != '}') return self.make(.invalid, start, sline, scol);
        self.bump(); // }
        return self.make(.interp, start, sline, scol);
    }

    fn lexString(self: *Lexer, sline: u32, scol: u32) Token {
        self.bump(); // opening quote
        const inner_start = self.pos;
        while (self.peek()) |c| {
            if (c == '\\') {
                self.bump(); // backslash
                if (self.peek() != null) self.bump(); // escaped char
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
            '*' => return self.single(.star, start, sline, scol),
            '+' => return self.single(.plus, start, sline, scol),
            '-' => return self.single(.minus, start, sline, scol),
            '/' => return self.single(.slash, start, sline, scol),
            '%' => return self.single(.percent, start, sline, scol),
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

/// Lex the whole source into a slice ending with an `eof` token. Caller frees.
pub fn tokenize(alloc: std.mem.Allocator, src: []const u8) ![]Token {
    var lx = Lexer.init(src);
    var list = std.array_list.Managed(Token).init(alloc);
    errdefer list.deinit();
    while (true) {
        const t = lx.next();
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

test "lex operators, annotations, and unit suffix split" {
    const alloc = std.testing.allocator;
    const toks = try tokenize(alloc, "a == b != c <= d >= @[batch_bytes = 100mb]");
    defer alloc.free(toks);

    const expect = [_]Tag{ .ident, .eq, .ident, .ne, .ident, .le, .ident, .ge, .at, .lbracket, .ident, .assign, .int, .ident, .rbracket, .eof };
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

test "unterminated string is invalid" {
    const alloc = std.testing.allocator;
    const toks = try tokenize(alloc, "x \"oops");
    defer alloc.free(toks);
    try std.testing.expectEqual(Tag.ident, toks[0].tag);
    try std.testing.expectEqual(Tag.invalid, toks[1].tag);
}
