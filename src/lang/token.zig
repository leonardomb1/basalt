//! Token definitions for the DSL. The lexer is whitespace-insensitive (newlines
//! are trivia); structure comes from `|`, commas, and leading keywords. Keywords
//! are not distinguished here: they are plain `ident`s recognized contextually by
//! the parser, so `filter` can be an operator or, in another position, a field.
//!
//! `qident` is a double-quoted name, an ANSI SQL quoted identifier: `"Exchange
//! rate"` names that column, never the string, and is never read as a keyword.
//! `end_line`/`end_col` mark just past the token's last source byte, where an
//! editor ends its underline; they are zero on a token made rather than lexed.

const std = @import("std");

pub const Tag = enum {
    ident,
    qident,
    string,
    int,
    float,
    interp,

    pipe,
    at,

    lparen,
    rparen,
    lbracket,
    rbracket,
    lbrace,
    rbrace,

    comma,
    dot,
    colon,
    star,

    assign,
    fat_arrow,
    arrow,
    eq,
    ne,
    lt,
    le,
    gt,
    ge,
    plus,
    minus,
    slash,
    percent,
    amp,
    bar,
    caret,
    tilde,
    shl,
    shr,
    qq,
    qdot,

    semi,
    dollar_ident,

    eof,
    invalid,

    pub fn describe(self: Tag) []const u8 {
        return switch (self) {
            .ident => "identifier",
            .qident => "quoted identifier",
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
            .arrow => "'->'",
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
            .amp => "'&'",
            .bar => "'|'",
            .caret => "'^'",
            .tilde => "'~'",
            .shl => "'<<'",
            .shr => "'>>'",
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
    text: []const u8,
    line: u32,
    col: u32,
    end_line: u32 = 0,
    end_col: u32 = 0,
};
