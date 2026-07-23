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
    interp, // ${name} — template placeholder; text is the full lexeme incl. `${}`

    pipe, // |
    at, // @

    lparen,
    rparen,
    lbracket,
    rbracket,
    lbrace,
    rbrace,

    comma,
    dot,
    colon, // : (loop-variable type annotation)
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
    qq, // ?? (null-coalesce)
    qdot, // ?. (safe navigation)

    // SQL-dialect tokens (unused by the BSL lexer).
    semi, // ;
    dollar_ident, // $name — PARAM reference; text excludes the `$`

    eof,
    invalid,

    pub fn describe(self: Tag) []const u8 {
        return switch (self) {
            .ident => "identifier",
            .string => "string",
            .int => "integer",
            .float => "float",
            .interp => "interpolation `${...}`",
            .pipe => "'|'",
            .at => "'@'",
            .lparen => "'('",
            .rparen => "')'",
            .lbracket => "'['",
            .rbracket => "']'",
            .lbrace => "'{'",
            .rbrace => "'}'",
            .comma => "','",
            .dot => "'.'",
            .colon => "':'",
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
            .qq => "'??'",
            .qdot => "'?.'",
            .semi => "';'",
            .dollar_ident => "parameter reference `$name`",
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
