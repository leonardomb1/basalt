//! A file's names as modules see them. Every file of a program is a module: the
//! functions, connections, PARAMs and LETs it declares are its own, and only those
//! it declares `EXPORT` reach a file that includes it — directly, never through a
//! file in between. Two files may each declare a private `pg`, so a name declared in
//! an included file takes an internal name of its file's own (`__m<file>_<name>`)
//! while the root script's keep theirs; the parser resolves every reference through
//! the scope of the file its token came from (a table function's body, parsed again
//! at a call in another file, still sees its own file's names). An internal name is
//! an identifier, so `${...}` holes rewritten with it still parse at run time, and
//! whatever is shown to a person carries the name as written (`shown`, `unmangle`).

const std = @import("std");
const Token = @import("token.zig").Token;
const msgfmt = @import("../base/msgfmt.zig");

pub const Kind = enum {
    func,
    conn,
    variable,

    pub fn word(k: Kind) []const u8 {
        return switch (k) {
            .func => "FUNCTION",
            .conn => "CONNECTION",
            .variable => "LET",
        };
    }
};

/// A name a file declares: as written, as the program knows it, and whether the
/// file shares it. A variable is a PARAM's or a LET's (they share one namespace).
pub const Name = struct {
    kind: Kind,
    name: []const u8,
    internal: []const u8,
    exported: bool = false,
    param: bool = false,
    query: bool = false,
    file: u16 = 0,
};

/// A file included `AS alias`: every name it declares, its exports reachable as
/// `alias.name`, the rest named in the refusal of a call to them.
pub const Lib = struct {
    alias: []const u8,
    path: []const u8,
    names: []const Name,
};

/// Where a name a file may see comes from, for the refusal of a private one: the
/// file that declares it, and, for an export of a file this one includes only
/// through another (`via`), that other.
pub const Hidden = struct { name: Name, path: []const u8, via: []const u8 = "" };

/// One file's view: its own names, the exports of the files it includes without
/// `AS` (each with the path that exports it), its libraries, and the private names
/// of its direct includes, kept to say why one is out of reach.
pub const Scope = struct {
    path: []const u8 = "",
    own: []const Name = &.{},
    imports: []const Hidden = &.{},
    libs: []const Lib = &.{},
    hidden: []const Hidden = &.{},
    /// Names local to a block somewhere in the file (FOR EACH variables, function
    /// parameters), which a `${...}` hole may mean instead of a module variable.
    locals: []const []const u8 = &.{},
};

const prefix = "__m";

/// The program's name for `name`, of kind `kind`, declared in file `file`: the
/// root's (file 0) as written, a function's lower-cased as calls are.
pub fn internalName(arena: std.mem.Allocator, file: u16, kind: Kind, name: []const u8) error{OutOfMemory}![]const u8 {
    const base = if (kind == .func) try std.ascii.allocLowerString(arena, name) else name;
    if (file == 0) return base;
    return msgfmt.allocPrint(arena, prefix ++ "{d}_{s}", .{ file, base });
}

/// `name` as its file wrote it.
pub fn shown(name: []const u8) []const u8 {
    const n = mangledLen(name, 0) orelse return name;
    return name[n..];
}

/// The length of an internal-name prefix at `text[at..]`, or null.
fn mangledLen(text: []const u8, at: usize) ?usize {
    if (!std.mem.startsWith(u8, text[at..], prefix)) return null;
    var i = at + prefix.len;
    const digits = i;
    while (i < text.len and std.ascii.isDigit(text[i])) i += 1;
    if (i == digits or i >= text.len or text[i] != '_') return null;
    return i + 1 - at;
}

/// The length of an internal-name prefix starting a name at `text[i]` (not inside
/// a word: after a letter or digit it is a user's own text), or null.
pub fn prefixAt(text: []const u8, i: usize) ?usize {
    if (i > 0 and std.ascii.isAlphanumeric(text[i - 1])) return null;
    return mangledLen(text, i);
}

/// `text` (a message, a log line, a plan) with every internal name shown as written.
pub fn unmangle(arena: std.mem.Allocator, text: []const u8) error{OutOfMemory}![]const u8 {
    if (std.mem.indexOf(u8, text, prefix) == null) return text;
    var aw = std.Io.Writer.Allocating.init(arena);
    writeShown(&aw.writer, text) catch return error.OutOfMemory;
    return aw.written();
}

/// Writes `text` to `w` with every internal name shown as written.
pub fn writeShown(w: *std.Io.Writer, text: []const u8) std.Io.Writer.Error!void {
    var start: usize = 0;
    var i: usize = 0;
    while (i < text.len) {
        if (prefixAt(text, i)) |n| {
            try w.writeAll(text[start..i]);
            i += n;
            start = i;
            continue;
        }
        i += 1;
    }
    try w.writeAll(text[start..]);
}

/// `unmangle` into `buf`, for a message held in a fixed buffer; the text is never
/// longer than `text`.
pub fn unmangleInto(buf: []u8, text: []const u8) []const u8 {
    var n: usize = 0;
    var i: usize = 0;
    while (i < text.len and n < buf.len) {
        if (prefixAt(text, i)) |k| {
            i += k;
            continue;
        }
        buf[n] = text[i];
        n += 1;
        i += 1;
    }
    return buf[0..n];
}

fn kw(t: Token, w: []const u8) bool {
    return t.tag == .ident and std.ascii.eqlIgnoreCase(t.text, w);
}

fn named(t: Token) bool {
    return t.tag == .ident or t.tag == .qident;
}

/// The names file `file`'s tokens declare at the top level: `CREATE [OR REPLACE]
/// CONNECTION | FUNCTION n`, `EXPORT [OR REPLACE] CONNECTION | FUNCTION | PARAM | LET n`,
/// and `PARAM n` / `LET n = ...` opening a statement; and, apart, the names its blocks
/// keep local (FOR EACH ... AS (a, b), a function's parameters).
pub fn prescan(arena: std.mem.Allocator, toks: []const Token, file: u16) error{OutOfMemory}!struct { names: []const Name, locals: []const []const u8 } {
    var out = std.array_list.Managed(Name).init(arena);
    var locals = std.array_list.Managed([]const u8).init(arena);
    var i: usize = 0;
    while (i < toks.len) : (i += 1) {
        const t = toks[i];
        const starts = i == 0 or toks[i - 1].tag == .semi;
        if (kw(t, "create") or kw(t, "export")) {
            const exported = kw(t, "export");
            var j = i + 1;
            if (j + 1 < toks.len and kw(toks[j], "or") and kw(toks[j + 1], "replace")) j += 2;
            if (j + 1 >= toks.len or !named(toks[j + 1])) continue;
            const what = toks[j];
            const kind: ?Kind = if (kw(what, "function")) .func else if (kw(what, "connection")) .conn else if (exported and (kw(what, "param") or kw(what, "let"))) .variable else null;
            const k = kind orelse continue;
            const name = toks[j + 1].text;
            try out.append(.{ .kind = k, .name = name, .internal = try internalName(arena, file, k, name), .exported = exported, .param = kw(what, "param"), .query = k == .variable and kw(what, "let") and opensQuery(toks, j + 2), .file = file });
            if (k == .func) try fnParams(toks, j + 2, &locals);
            continue;
        }
        if (starts and (kw(t, "param") or kw(t, "let")) and i + 1 < toks.len and named(toks[i + 1])) {
            if (kw(t, "let") and !(i + 2 < toks.len and toks[i + 2].tag == .assign)) continue;
            const name = toks[i + 1].text;
            try out.append(.{ .kind = .variable, .name = name, .internal = try internalName(arena, file, .variable, name), .param = kw(t, "param"), .query = kw(t, "let") and opensQuery(toks, i + 2), .file = file });
            continue;
        }
        if (kw(t, "for") and i + 1 < toks.len and kw(toks[i + 1], "each")) {
            var j = i + 2;
            while (j < toks.len and toks[j].tag != .semi) : (j += 1) {
                if (kw(toks[j], "as") and j + 1 < toks.len and toks[j + 1].tag == .lparen) {
                    j += 2;
                    while (j < toks.len and toks[j].tag != .rparen) : (j += 1) if (named(toks[j])) try locals.append(toks[j].text);
                    break;
                }
            }
        }
    }
    return .{ .names = out.items, .locals = locals.items };
}

/// Whether the LET whose `=` is at `toks[at]` takes a query: `= (SELECT` or `= (WITH`.
fn opensQuery(toks: []const Token, at: usize) bool {
    var j = at;
    if (j < toks.len and toks[j].tag == .assign) j += 1;
    return j + 1 < toks.len and toks[j].tag == .lparen and (kw(toks[j + 1], "select") or kw(toks[j + 1], "with"));
}

/// A function declaration's parameter names, from the `(` at `toks[at]`.
fn fnParams(toks: []const Token, at: usize, out: *std.array_list.Managed([]const u8)) error{OutOfMemory}!void {
    if (at >= toks.len or toks[at].tag != .lparen) return;
    var depth: usize = 0;
    var j = at;
    var expect_name = true;
    while (j < toks.len) : (j += 1) {
        const t = toks[j];
        switch (t.tag) {
            .lparen => {
                depth += 1;
                if (depth == 1) expect_name = true;
                continue;
            },
            .rparen => {
                depth -= 1;
                if (depth == 0) return;
                continue;
            },
            .comma => if (depth == 1) {
                expect_name = true;
                continue;
            },
            else => {},
        }
        if (depth == 1 and expect_name and named(t)) {
            try out.append(t.text);
            expect_name = false;
        }
    }
}

test "internal names: the root keeps its own, an included file's carry its file, and every shown text drops them" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings("pg", try internalName(a, 0, .conn, "pg"));
    try std.testing.expectEqualStrings("__m3_Pg", try internalName(a, 3, .conn, "Pg"));
    try std.testing.expectEqualStrings("__m3_median", try internalName(a, 3, .func, "Median"));
    try std.testing.expectEqualStrings("pg", shown("__m12_pg"));
    try std.testing.expectEqualStrings("__mx_pg", shown("__mx_pg"));
    try std.testing.expectEqualStrings("pg sink failed for `x` in pg.t (__m_y)", try unmangle(a, "__m2_pg sink failed for `__m3_x` in __m2_pg.t (__m_y)"));
    try std.testing.expectEqualStrings("a__m2_b", try unmangle(a, "a__m2_b"));
    try std.testing.expectEqualStrings("binding __tvf1_remoto, smb://pg/x", try unmangle(a, "binding __tvf1___m1_remoto, smb://__m12_pg/x"));
}
