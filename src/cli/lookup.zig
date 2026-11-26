//! What the name under the cursor is, for an editor's hover and go-to-definition
//! (the kernel's `hover` and `definition`). A name resolves against the declarations
//! in scope — the session's, those the script's `@include`s export to it (a
//! library's as `alias.name`), then the script's own, each shadowing the one before
//! — then the built-in functions, then the columns of the files and tables the
//! script names, as Tab finds them. All of it is read from the text, so a script
//! still being typed resolves too. Nothing runs, and nothing connects unless Tab
//! would: a connection's tables and columns are asked under `connect` alone.
//!
//! Every declaration keeps where its name stands (`entry.Origin`): that is what
//! `definition` answers, and why a built-in or a column has none. A local include
//! is read from the working directory, as the kernel resolves it; a remote one only
//! from the cache that pinned it, never fetched. A connection's hover shows its
//! TYPE and the OPTIONS whose value is a literal under a name that holds no secret,
//! a URL's user and password cut out: never a password, a token or an `env()` or
//! `secret()` value.

const std = @import("std");
const entry = @import("entry.zig");
const DeclStore = entry.DeclStore;
const DeclKind = entry.DeclKind;
const Origin = entry.Origin;
const declOf = entry.declOf;
const splitStatements = entry.splitStatements;
const declareFrom = @import("cmd_complete.zig").declareFrom;
const repl_catalog = @import("repl/catalog.zig");
const Completer = repl_catalog.Completer;
const cmd_catalog = @import("cmd_catalog.zig");
const complete = @import("repl/complete.zig");
const include = @import("../lang/include.zig");
const remote = @import("../lang/remote.zig");
const sql_lexer = @import("../lang/sql_lexer.zig");
const parser = @import("../lang/sql_parser.zig");
const ast = @import("../lang/ast.zig");
const print = @import("../lang/print.zig");
const jsontext = @import("../exec/jsontext.zig");
const Value = @import("../exec/value.zig").Value;
const utf16 = @import("../base/utf16.zig");

/// The names in scope for `script`: the session's, those its `@include`s export to
/// it, then its own.
pub fn scopeFor(gpa: std.mem.Allocator, arena: std.mem.Allocator, session: *const DeclStore, script: []const u8) !DeclStore {
    var scope = DeclStore.init(gpa);
    errdefer scope.deinit();
    for (session.items.items) |e| try scope.putAt(.{ .kind = e.kind, .name = e.name }, e.text, e.origin);
    try declareIncludes(&scope, arena, script);
    try declareFrom(&scope, arena, script);
    return scope;
}

fn declareIncludes(scope: *DeclStore, arena: std.mem.Allocator, script: []const u8) !void {
    var resolver: ?remote.Resolver = null;
    for (try include.writtenIncludes(arena, script)) |inc| {
        const file = (try includedFile(arena, &resolver, inc.path)) orelse continue;
        for (try splitStatements(arena, file.text)) |st| {
            const id = declOf(st) orelse continue;
            if (!id.exported) continue;
            const origin = entry.originIn(file.path, "", file.text, id.name);
            if (inc.alias) |al| {
                const qualified = try std.fmt.allocPrint(arena, "{s}.{s}", .{ al, id.name });
                try scope.putAt(.{ .kind = id.kind, .name = qualified }, st, origin);
            } else try scope.putAt(id, st, origin);
        }
    }
}

const Included = struct { path: []const u8, text: []const u8 };

/// An include's file and text: a local path from the working directory, a remote one
/// from the cache; null when it does not read.
fn includedFile(arena: std.mem.Allocator, resolver: *?remote.Resolver, path: []const u8) !?Included {
    if (remote.parse(path)) |spec| {
        if (resolver.* == null) resolver.* = remote.Resolver.init(arena, ".", null, false) catch return null;
        const at = (try resolver.*.?.cachedPath(spec)) orelse return null;
        const text = std.fs.cwd().readFileAlloc(arena, at, 8 << 20) catch return null;
        return .{ .path = at, .text = text };
    }
    const text = std.fs.cwd().readFileAlloc(arena, path, 8 << 20) catch return null;
    return .{ .path = path, .text = text };
}

/// The name under the cursor: its bytes, `$` included, each dot-separated part, the
/// one the cursor is on, and whether a `(` follows it.
pub const Word = struct {
    start: usize,
    end: usize,
    dollar: bool,
    parts: []const []const u8,
    part: usize,
    call: bool,
};

fn isWordByte(c: u8) bool {
    return std.ascii.isAlphanumeric(c) or c == '_' or c >= 0x80;
}

/// The word at byte `at`, or just before it; null inside a string, comment or quoted
/// name, and between two words.
pub fn wordAt(arena: std.mem.Allocator, text: []const u8, at: usize) !?Word {
    var i: usize = 0;
    while (i < @min(at, text.len)) {
        if (sql_lexer.spanAt(text, i)) |sp| {
            if (at > i and at < sp.end) return null;
            i = sp.end;
        } else i += 1;
    }
    var s = at;
    while (s > 0 and (isWordByte(text[s - 1]) or text[s - 1] == '.')) s -= 1;
    var e = at;
    while (e < text.len and (isWordByte(text[e]) or text[e] == '.')) e += 1;
    while (s < e and text[s] == '.') s += 1;
    while (e > s and text[e - 1] == '.') e -= 1;
    if (s == e or at < s or at > e or std.ascii.isDigit(text[s])) return null;
    const dollar = s > 0 and text[s - 1] == '$';
    var parts = std.array_list.Managed([]const u8).init(arena);
    var part: usize = 0;
    var from = s;
    var it = std.mem.splitScalar(u8, text[s..e], '.');
    while (it.next()) |p| {
        if (at >= from and at <= from + p.len) part = parts.items.len;
        try parts.append(p);
        from += p.len + 1;
    }
    var k = e;
    while (k < text.len and (text[k] == ' ' or text[k] == '\t')) k += 1;
    return .{ .start = if (dollar) s - 1 else s, .end = e, .dollar = dollar, .parts = parts.items, .part = part, .call = k < text.len and text[k] == '(' };
}

pub const Hover = struct {
    start: usize,
    end: usize,
    kind: []const u8,
    name: []const u8,
    signature: []const u8,
    type: ?[]const u8 = null,
    doc: ?[]const u8 = null,
    /// A built-in's kind in the catalog: `scalar`, `aggregate`, `window` or `table`.
    builtin: ?[]const u8 = null,
    /// An expression function's body, as written.
    body: ?[]const u8 = null,
    /// A PARAM's DEFAULT, as written.
    default: ?[]const u8 = null,
    /// A PARAM's bound value or a LET's held one, as JSON.
    value: ?[]const u8 = null,
    columns: ?[]const complete.Column = null,
    /// Where it is declared, for `definition`.
    origin: ?Origin = null,
};

/// What the name at `at` is, against `cx`'s scope (`scopeFor`); `session` holds the
/// values LETs keep, `params` a request's bound PARAMs.
pub fn resolve(arena: std.mem.Allocator, cx: *const Completer, session: *const DeclStore, text: []const u8, at: usize, params: ?std.json.Value) !?Hover {
    const w = (try wordAt(arena, text, at)) orelse return null;
    const scope = cx.decls;
    if (w.dollar) {
        const whole = try std.mem.join(arena, ".", w.parts);
        for ([_][]const u8{ whole, w.parts[0] }) |n| for ([_]DeclKind{ .param, .let }) |kind| {
            const e = scope.find(kind, n) orelse continue;
            return try declHover(arena, e, n, w.start, w.start + 1 + n.len, session, params);
        };
        return null;
    }
    if (w.parts.len >= 2) {
        const lib = try std.fmt.allocPrint(arena, "{s}.{s}", .{ w.parts[0], w.parts[1] });
        if (w.part <= 1) if (scope.find(.function, lib)) |e| return try declHover(arena, e, lib, w.start, w.start + lib.len, session, params);
        if (scope.find(.connection, w.parts[0])) |e| {
            if (w.part == 0) return try declHover(arena, e, w.parts[0], w.start, w.start + w.parts[0].len, session, params);
            const qualified = text[w.start..w.end];
            return .{
                .start = w.start,
                .end = w.end,
                .kind = "table",
                .name = qualified,
                .signature = qualified,
                .columns = if (w.parts.len == 3) nonEmpty(repl_catalog.tableColumns(cx, qualified)) else null,
            };
        }
        if (w.part == w.parts.len - 1) return try columnHover(arena, cx, text, at, w, w.parts[w.part]);
        return null;
    }
    const n = w.parts[0];
    if (!w.call) if (scope.find(.connection, n)) |e| return try declHover(arena, e, n, w.start, w.end, session, params);
    if (scope.find(.function, n)) |e| return try declHover(arena, e, n, w.start, w.end, session, params);
    if (w.call) {
        const f = cmd_catalog.lookup(try cmd_catalog.get(), n) orelse return null;
        return .{ .start = w.start, .end = w.end, .kind = "function", .name = f.name, .signature = f.signature, .type = f.returns, .doc = f.doc, .builtin = @tagName(f.kind) };
    }
    return try columnHover(arena, cx, text, at, w, n);
}

/// Null for no columns: a table not asked, as `connect` is off, is not an empty one.
fn nonEmpty(cols: []const complete.Column) ?[]const complete.Column {
    return if (cols.len == 0) null else cols;
}

fn columnHover(arena: std.mem.Allocator, cx: *const Completer, text: []const u8, at: usize, w: Word, name: []const u8) !?Hover {
    for (try repl_catalog.columnsIn(arena, cx, text, at)) |c| {
        if (!std.ascii.eqlIgnoreCase(c.name, name)) continue;
        const start = w.end - name.len;
        return .{
            .start = start,
            .end = w.end,
            .kind = "column",
            .name = c.name,
            .signature = if (c.type.len > 0) try std.fmt.allocPrint(arena, "{s} {s}", .{ c.name, c.type }) else c.name,
            .type = if (c.type.len > 0) c.type else null,
        };
    }
    return null;
}

/// A declaration's hover: what its text says of it, and its leading comment as its doc.
fn declHover(arena: std.mem.Allocator, e: *const DeclStore.Entry, shown: []const u8, start: usize, end: usize, session: *const DeclStore, params: ?std.json.Value) !Hover {
    const stmt = pastComments(e.text);
    var h = Hover{
        .start = start,
        .end = end,
        .kind = @tagName(e.kind),
        .name = shown,
        .signature = try oneLine(arena, stmt),
        .doc = try commentText(arena, e.text[0 .. e.text.len - stmt.len]),
        .origin = e.origin,
    };
    const id = declOf(stmt) orelse return h;
    const after = stmt[entry.sliceAt(stmt, id.name).? + id.name.len ..];
    switch (e.kind) {
        .function => try functionParts(arena, &h, shown, after),
        .param => {
            const d = topWord(after, "default");
            const ty = std.mem.trim(u8, after[0 .. d orelse after.len], " \t\r\n");
            h.type = if (parser.parseTypeStr(arena, ty)) |t| try t.name(arena) else try std.ascii.allocLowerString(arena, ty);
            if (d) |k| h.default = try oneLine(arena, after[k + "default".len ..]);
            if (params) |pv| if (pv == .object) if (pv.object.get(shown)) |v| {
                h.value = try std.json.Stringify.valueAlloc(arena, v, .{});
            };
        },
        .let => if (try heldFor(arena, session, e)) |v| {
            var aw = std.Io.Writer.Allocating.init(arena);
            try jsontext.scalar(&aw.writer, v, .{ .decimal = .string });
            h.value = aw.written();
            h.type = valueType(v);
        },
        .connection => try connectionParts(arena, &h, stmt),
    }
    return h;
}

/// The value the session holds for LET `e`: the session's own declaration, or the
/// script's when it is the statement the session ran (its words, comments aside), as
/// a file run in the kernel and then hovered is. A LET written otherwise since has
/// no value yet.
fn heldFor(arena: std.mem.Allocator, session: *const DeclStore, e: *const DeclStore.Entry) !?Value {
    const v = session.heldValue(e.name) orelse return null;
    if (std.mem.eql(u8, e.origin.file, "session")) return v;
    const ran = session.find(.let, e.name) orelse return null;
    const same = std.mem.eql(u8, try oneLine(arena, pastComments(ran.text)), try oneLine(arena, pastComments(e.text)));
    return if (same) v else null;
}

/// The type of a value a LET holds, or null for a nested one.
fn valueType(v: Value) ?[]const u8 {
    return switch (v) {
        .null, .nested => null,
        else => @tagName(v),
    };
}

/// A function's signature (`name(params)`, `RETURNS TABLE` after a table function's)
/// and an expression function's body, from its text past the name.
fn functionParts(arena: std.mem.Allocator, h: *Hover, shown: []const u8, after: []const u8) !void {
    const open = std.mem.indexOfScalar(u8, after, '(') orelse return;
    const close = matching(after, open) orelse return;
    const rest = std.mem.trim(u8, after[close + 1 ..], " \t\r\n");
    var sig = try std.fmt.allocPrint(arena, "{s}{s}", .{ shown, after[open .. close + 1] });
    var i: usize = 0;
    const w1 = entry.nextWord(rest, &i) orelse "";
    if (std.ascii.eqlIgnoreCase(w1, "returns")) {
        sig = try std.fmt.allocPrint(arena, "{s} RETURNS TABLE", .{sig});
        h.type = "table";
    } else if (std.ascii.eqlIgnoreCase(w1, "as")) {
        const body = std.mem.trim(u8, rest[i..], " \t\r\n");
        if (!(body.len >= 5 and std.ascii.eqlIgnoreCase(body[0..5], "begin") and (body.len == 5 or !isWordByte(body[5]))))
            h.body = try oneLine(arena, body);
    }
    h.signature = try oneLine(arena, sig);
}

/// The `)` matching the `(` at `open`, past strings and comments.
fn matching(s: []const u8, open: usize) ?usize {
    var depth: usize = 0;
    var i = open;
    while (i < s.len) {
        if (sql_lexer.spanAt(s, i)) |sp| {
            i = sp.end;
            continue;
        }
        switch (s[i]) {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if (depth == 0) return i;
            },
            else => {},
        }
        i += 1;
    }
    return null;
}

/// Where the word `kw` first stands in `s` outside strings and comments, in any case.
fn topWord(s: []const u8, kw: []const u8) ?usize {
    var i: usize = 0;
    while (i < s.len) {
        if (sql_lexer.spanAt(s, i)) |sp| {
            i = sp.end;
            continue;
        }
        if (i + kw.len <= s.len and std.ascii.eqlIgnoreCase(s[i .. i + kw.len], kw) and
            (i == 0 or !isWordByte(s[i - 1])) and (i + kw.len == s.len or !isWordByte(s[i + kw.len]))) return i;
        i += 1;
    }
    return null;
}

/// A connection's TYPE and the options safe to show, as one line.
fn connectionParts(arena: std.mem.Allocator, h: *Hover, stmt: []const u8) !void {
    h.type = repl_catalog.connTypeOf(stmt);
    var pd: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = parser.parseSource(arena, stmt, &pd) catch {
        h.signature = try std.fmt.allocPrint(arena, "CONNECTION {s} TYPE {s}", .{ h.name, h.type orelse "?" });
        return;
    };
    for (prog.stmts) |st| if (st == .connection) {
        const c = st.connection;
        h.type = c.connector;
        var aw = std.Io.Writer.Allocating.init(arena);
        try aw.writer.print("CONNECTION {s} TYPE {s}", .{ h.name, c.connector });
        var n: usize = 0;
        for (c.config) |at| {
            const lit = switch (at.value.*) {
                .str_lit, .int_lit, .float_lit, .dec_lit, .bool_lit => true,
                else => false,
            };
            if (!lit or secretKey(at.key)) continue;
            try aw.writer.writeAll(if (n == 0) " OPTIONS (" else ", ");
            try aw.writer.print("{s} = ", .{at.key});
            if (at.value.* == .str_lit) {
                try print.expr(&aw.writer, try ast.mk(arena, .{ .str_lit = try withoutUserinfo(arena, at.value.str_lit) }));
            } else try print.expr(&aw.writer, at.value);
            n += 1;
        }
        if (n > 0) try aw.writer.writeByte(')');
        h.signature = aw.written();
    };
}

/// Whether an option's name says it holds a secret, or names where one is kept.
fn secretKey(key: []const u8) bool {
    for ([_][]const u8{ "pass", "pwd", "secret", "token", "key", "auth", "credential", "bearer", "sas", "cookie", "signature", "private" }) |w| {
        if (std.ascii.indexOfIgnoreCase(key, w) != null) return true;
    }
    return false;
}

/// A URL with its `user:password@` cut out; any other text as it is.
fn withoutUserinfo(arena: std.mem.Allocator, s: []const u8) ![]const u8 {
    const scheme = std.mem.indexOf(u8, s, "://") orelse return s;
    const from = scheme + 3;
    const to = std.mem.indexOfAnyPos(u8, s, from, "/?#") orelse s.len;
    const at = std.mem.lastIndexOfScalar(u8, s[from..to], '@') orelse return s;
    return std.fmt.allocPrint(arena, "{s}{s}", .{ s[0..from], s[from + at + 1 ..] });
}

/// `stmt` past its leading blanks and comments.
fn pastComments(stmt: []const u8) []const u8 {
    var i: usize = 0;
    while (i < stmt.len) {
        if (std.ascii.isWhitespace(stmt[i])) {
            i += 1;
            continue;
        }
        const sp = sql_lexer.spanAt(stmt, i) orelse break;
        if (sp.kind != .line_comment and sp.kind != .block_comment) break;
        i = sp.end;
    }
    return stmt[i..];
}

/// The words of leading comments, their markers dropped; null when there are none.
fn commentText(arena: std.mem.Allocator, lead: []const u8) !?[]const u8 {
    var out = std.array_list.Managed(u8).init(arena);
    var i: usize = 0;
    while (i < lead.len) {
        const sp = sql_lexer.spanAt(lead, i) orelse {
            i += 1;
            continue;
        };
        var body = lead[i..sp.end];
        body = if (sp.kind == .line_comment) body[2..] else if (body.len >= 4) body[2 .. body.len - 2] else "";
        var words = std.mem.tokenizeAny(u8, body, " \t\r\n*");
        while (words.next()) |wd| {
            if (out.items.len > 0) try out.append(' ');
            try out.appendSlice(wd);
        }
        i = sp.end;
    }
    return if (out.items.len == 0) null else out.items;
}

/// `s` with each run of blanks one space, trimmed.
fn oneLine(arena: std.mem.Allocator, s: []const u8) ![]const u8 {
    var out = std.array_list.Managed(u8).init(arena);
    var words = std.mem.tokenizeAny(u8, s, " \t\r\n");
    while (words.next()) |wd| {
        if (out.items.len > 0) try out.append(' ');
        try out.appendSlice(wd);
    }
    return out.items;
}

/// A hover as JSON, its offsets in bytes or, with `units`, UTF-16 units of `text`;
/// `null` for none.
pub fn writeHover(w: *std.Io.Writer, h: ?Hover, text: []const u8, units: bool) !void {
    const x = h orelse return w.writeAll("null");
    const start = if (units) utf16.byteToUnit(text, x.start) else x.start;
    const end = if (units) utf16.byteToUnit(text, x.end) else x.end;
    try w.print("{{\"start\":{d},\"end\":{d},\"kind\":", .{ start, end });
    try jsontext.string(w, x.kind);
    try w.writeAll(",\"name\":");
    try jsontext.string(w, x.name);
    try w.writeAll(",\"signature\":");
    try jsontext.string(w, x.signature);
    try w.writeAll(",\"type\":");
    try optString(w, x.type);
    try w.writeAll(",\"doc\":");
    try optString(w, x.doc);
    inline for (.{ "builtin", "body", "default" }) |f| if (@field(x, f)) |v| {
        try w.writeAll(",\"" ++ f ++ "\":");
        try jsontext.string(w, v);
    };
    if (x.value) |v| try w.print(",\"value\":{s}", .{v});
    if (x.columns) |cols| {
        try w.writeAll(",\"columns\":[");
        for (cols, 0..) |c, i| {
            if (i > 0) try w.writeByte(',');
            try w.writeAll("{\"name\":");
            try jsontext.string(w, c.name);
            try w.writeAll(",\"type\":");
            try optString(w, if (c.type.len > 0) c.type else null);
            try w.writeByte('}');
        }
        try w.writeByte(']');
    }
    try w.writeByte('}');
}

/// Where a hover's name is declared, as JSON: its file, line and column, and where
/// the name ends, columns in bytes or, with `units`, UTF-16 units; a session
/// declaration's carries the `id` of the request that made it. `null` for a name
/// declared nowhere, a built-in or a column.
pub fn writeDefinition(w: *std.Io.Writer, h: ?Hover, units: bool) !void {
    const x = h orelse return w.writeAll("null");
    const o = x.origin orelse return w.writeAll("null");
    if (o.line == 0) return w.writeAll("null");
    const col = if (units) o.col16 else o.col;
    try w.writeAll("{\"file\":");
    try jsontext.string(w, o.file);
    try w.print(",\"line\":{d},\"col\":{d},\"end_line\":{d},\"end_col\":{d}", .{ o.line, col, o.line, col + o.len });
    if (o.id.len > 0) {
        try w.writeAll(",\"id\":");
        try jsontext.string(w, o.id);
    }
    try w.writeByte('}');
}

fn optString(w: *std.Io.Writer, s: ?[]const u8) !void {
    if (s) |x| try jsontext.string(w, x) else try w.writeAll("null");
}

const testing = std.testing;

fn testHover(a: std.mem.Allocator, session: *const DeclStore, text: []const u8, marker: []const u8, params: ?std.json.Value) !?Hover {
    var scope = try scopeFor(testing.allocator, a, session, text);
    defer scope.deinit();
    var catalog = repl_catalog.Catalog.init(testing.allocator);
    defer catalog.deinit();
    const cx = Completer{ .gpa = testing.allocator, .decls = &scope, .catalog = &catalog, .connect = false };
    const at = (std.mem.indexOf(u8, text, marker) orelse return error.TestUnexpectedResult) + 1;
    var h = (try resolve(a, &cx, session, text, at, params)) orelse return null;
    if (h.origin) |o| h.origin = .{ .file = try a.dupe(u8, o.file), .id = try a.dupe(u8, o.id), .line = o.line, .col = o.col, .col16 = o.col16, .len = o.len };
    return h;
}

test "wordAt: a dotted name and the part under the cursor, a $name, a call; nothing in a string or between words" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const text = "SELECT fin.days(x), $since, 'a b' FROM t WHERE  y";
    const w = (try wordAt(a, text, 12)).?;
    try testing.expectEqual(@as(usize, 2), w.parts.len);
    try testing.expectEqual(@as(usize, 1), w.part);
    try testing.expect(w.call);
    try testing.expectEqualStrings("fin.days", text[w.start..w.end]);
    const d = (try wordAt(a, text, 22)).?;
    try testing.expect(d.dollar);
    try testing.expectEqualStrings("$since", text[d.start..d.end]);
    try testing.expect((try wordAt(a, text, 30)) == null);
    try testing.expect((try wordAt(a, text, 47)) == null);
    try testing.expectEqualStrings("y", (try wordAt(a, text, text.len)).?.parts[0]);
}

test "resolve: a script's function with its comment, body and place; a built-in with its catalog entry" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var session = DeclStore.init(testing.allocator);
    defer session.deinit();
    const text = "-- twice the tax\nCREATE FUNCTION tax(x INT) AS x * 2;\nSELECT tax(1) AS t, upper('a') AS u";
    const h = (try testHover(a, &session, text, " tax(1", null)).?;
    try testing.expectEqualStrings("function", h.kind);
    try testing.expectEqualStrings("tax(x INT)", h.signature);
    try testing.expectEqualStrings("x * 2", h.body.?);
    try testing.expectEqualStrings("twice the tax", h.doc.?);
    try testing.expectEqualStrings("script", h.origin.?.file);
    try testing.expectEqual(@as(u32, 2), h.origin.?.line);
    try testing.expectEqual(@as(u32, 17), h.origin.?.col);
    const u = (try testHover(a, &session, text, " upper", null)).?;
    try testing.expectEqualStrings("scalar", u.builtin.?);
    try testing.expectEqualStrings("upper(s)", u.signature);
    try testing.expectEqualStrings("string", u.type.?);
    try testing.expect(u.origin == null);
    try testing.expect((try testHover(a, &session, text, " AS t", null)) == null);
}

test "resolve: a session PARAM's type, default and bound value, a LET's held value, and the session's place" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var session = DeclStore.init(testing.allocator);
    defer session.deinit();
    const cell = "PARAM since DATE DEFAULT '2020-01-01';\nLET n = 41 + 1;";
    for (try splitStatements(a, cell)) |st| {
        const id = declOf(st).?;
        try session.putAt(id, st, entry.originIn("session", "c1", cell, id.name));
    }
    try session.hold("n", .{ .int = 42 });
    const params = try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"since\":\"2024-05-01\"}", .{});
    const p = (try testHover(a, &session, "SELECT $since AS s, $n AS n", "$since", params)).?;
    try testing.expectEqualStrings("param", p.kind);
    try testing.expectEqualStrings("date", p.type.?);
    try testing.expectEqualStrings("'2020-01-01'", p.default.?);
    try testing.expectEqualStrings("\"2024-05-01\"", p.value.?);
    try testing.expectEqualStrings("session", p.origin.?.file);
    try testing.expectEqualStrings("c1", p.origin.?.id);
    try testing.expectEqual(@as(u32, 7), p.origin.?.col);
    const n = (try testHover(a, &session, "SELECT $since AS s, $n AS n", "$n", null)).?;
    try testing.expectEqualStrings("let", n.kind);
    try testing.expectEqualStrings("42", n.value.?);
    try testing.expectEqualStrings("int", n.type.?);
    try testing.expectEqual(@as(u32, 2), n.origin.?.line);
    const own = (try testHover(a, &session, "LET n = 7;\nSELECT $n AS n", "$n AS", null)).?;
    try testing.expectEqualStrings("script", own.origin.?.file);
    try testing.expect(own.value == null);
    const ran = (try testHover(a, &session, "-- the answer\nLET n =  41 + 1;\nSELECT $n AS n", "$n AS", null)).?;
    try testing.expectEqualStrings("script", ran.origin.?.file);
    try testing.expectEqualStrings("42", ran.value.?);
}

test "resolve: a connection shows its TYPE and literal options, never a secret, an env() value or a URL's password" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var session = DeclStore.init(testing.allocator);
    defer session.deinit();
    const text =
        \\CREATE CONNECTION pg TYPE postgres OPTIONS (host = 'db.local', port = 5432, user = env('PGUSER'), password = 's3cret');
        \\CREATE CONNECTION api TYPE http OPTIONS (base_url = 'https://bob:hunter2@api.example.com/v1', bearer = 'tok');
        \\SELECT * FROM pg.public.orders
    ;
    const h = (try testHover(a, &session, text, " pg.public", null)).?;
    try testing.expectEqualStrings("connection", h.kind);
    try testing.expectEqualStrings("postgres", h.type.?);
    try testing.expectEqualStrings("CONNECTION pg TYPE postgres OPTIONS (host = 'db.local', port = 5432)", h.signature);
    const t = (try testHover(a, &session, text, ".orders", null)).?;
    try testing.expectEqualStrings("table", t.kind);
    try testing.expectEqualStrings("pg.public.orders", t.name);
    const api = (try testHover(a, &session, text, " api T", null)).?;
    try testing.expect(std.mem.indexOf(u8, api.signature, "hunter2") == null);
    try testing.expect(std.mem.indexOf(u8, api.signature, "tok") == null);
    try testing.expect(std.mem.indexOf(u8, api.signature, "https://api.example.com/v1") != null);
}

test "resolve: a file's column with its type; a library's export through @include, at its place in that file" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "t.csv", .data = "id,amount\n1,2.5\n" });
    try tmp.dir.writeFile(.{ .sub_path = "fin.sql", .data = "-- library\nEXPORT FUNCTION days(d) AS d + 1;\nEXPORT PARAM rate INT DEFAULT 3;\nCREATE FUNCTION hidden(x) AS x;\n" });
    const dir = try tmp.dir.realpathAlloc(a, ".");
    var session = DeclStore.init(testing.allocator);
    defer session.deinit();
    const text = try std.fmt.allocPrint(a, "@include '{s}/fin.sql' AS fin;\nSELECT amount, fin.days(id), $fin.rate, hidden(1) FROM '{s}/t.csv'", .{ dir, dir });
    const c = (try testHover(a, &session, text, " amount", null)).?;
    try testing.expectEqualStrings("column", c.kind);
    try testing.expectEqualStrings("float", c.type.?);
    const f = (try testHover(a, &session, text, ".days", null)).?;
    try testing.expectEqualStrings("fin.days(d)", f.signature);
    try testing.expect(std.mem.endsWith(u8, f.origin.?.file, "/fin.sql"));
    try testing.expectEqual(@as(u32, 2), f.origin.?.line);
    try testing.expectEqual(@as(u32, 17), f.origin.?.col);
    const r = (try testHover(a, &session, text, "$fin.rate", null)).?;
    try testing.expectEqualStrings("param", r.kind);
    try testing.expectEqualStrings("3", r.default.?);
    try testing.expect((try testHover(a, &session, text, " hidden", null)) == null);
}

test "writeHover and writeDefinition: offsets and columns in UTF-16 units past text that is not ASCII, null for none" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var session = DeclStore.init(testing.allocator);
    defer session.deinit();
    const t2 = "SELECT 'é😀' AS a; CREATE FUNCTION f(x) AS x;\nSELECT 'é😀', f(1) AS v";
    const h = try testHover(a, &session, t2, " f(1", null);
    var aw = std.Io.Writer.Allocating.init(a);
    try writeHover(&aw.writer, h, t2, true);
    const at = std.mem.indexOf(u8, t2, " f(1").? + 1;
    const want = try std.fmt.allocPrint(a, "{{\"start\":{d},\"end\":{d}", .{ utf16.byteToUnit(t2, at), utf16.byteToUnit(t2, at) + 1 });
    try testing.expect(std.mem.startsWith(u8, aw.written(), want));
    aw = std.Io.Writer.Allocating.init(a);
    try writeDefinition(&aw.writer, h, true);
    try testing.expectEqualStrings("{\"file\":\"script\",\"line\":1,\"col\":36,\"end_line\":1,\"end_col\":37}", aw.written());
    aw = std.Io.Writer.Allocating.init(a);
    try writeDefinition(&aw.writer, h, false);
    try testing.expectEqualStrings("{\"file\":\"script\",\"line\":1,\"col\":39,\"end_line\":1,\"end_col\":40}", aw.written());
    aw = std.Io.Writer.Allocating.init(a);
    try writeHover(&aw.writer, null, t2, true);
    try testing.expectEqualStrings("null", aw.written());
}
