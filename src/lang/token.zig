//! Token definitions for the DSL. The lexer is whitespace-insensitive (newlines
//! are trivia); structure comes from `|`, commas, and leading keywords. Keywords
//! are not distinguished here — they are plain `ident`s recognized contextually
//! by the parser (so `filter` can be an operator or, in another position, a field).

const std = @import("std");

pub const Tag = enum {
    ident,
    string, // text excludes the surrounding quotes (escapes not yet unescaped)
    int,
    float,

    pipe, // |
    at, // @

    lparen,
    rparen,
    lbracket,
    rbracket,

    comma,
    dot,
    star, // * (also multiply)

    assign, // =
    fat_arrow, // =>
    eq, // ==
    ne, // !=
    lt, // <
    le, // <=
    gt, // >
    ge, // >=
    plus,
    minus,
    slash,
    percent,

    eof,
    invalid,

    pub fn describe(self: Tag) []const u8 {
        return switch (self) {
            .ident => "identifier",
            .string => "string",
            .int => "integer",
            .float => "float",
            .pipe => "'|'",
            .at => "'@'",
            .lparen => "'('",
            .rparen => "')'",
            .lbracket => "'['",
            .rbracket => "']'",
            .comma => "','",
            .dot => "'.'",
            .star => "'*'",
            .assign => "'='",
            .fat_arrow => "'=>'",
            .eq => "'=='",
            .ne => "'!='",
            .lt => "'<'",
            .le => "'<='",
            .gt => "'>'",
            .ge => "'>='",
            .plus => "'+'",
            .minus => "'-'",
            .slash => "'/'",
            .percent => "'%'",
            .eof => "end of input",
            .invalid => "invalid token",
        };
    }
};

pub const Token = struct {
    tag: Tag,
    text: []const u8, // slice into the source
    line: u32,
    col: u32,
};
