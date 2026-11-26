//! A REPL or kernel entry as statements: where each ends, which are declarations,
//! and the session's store of declarations that later entries build on.

const Session = @import("repl/repl.zig").Session;
const isClear = @import("repl/repl.zig").isClear;
const isHelp = @import("repl/repl.zig").isHelp;
const isQuit = @import("repl/repl.zig").isQuit;
const parser = @import("../lang/sql_parser.zig");
const sql_lexer = @import("../lang/sql_lexer.zig");
const HeldLet = @import("../runtime/env.zig").HeldLet;
const Value = @import("../exec/value.zig").Value;
const nested = @import("../exec/nested.zig");
const std = @import("std");
const Catalog = @import("repl/catalog.zig").Catalog;
const utf16 = @import("../base/utf16.zig");

/// Next statement-level `;`, skipping strings, dollar quotes and comments by the
/// lexer's own spans, so the REPL agrees with the parser on where a statement ends.
pub fn nextTopSemi(s: []const u8, from: usize) ?usize {
    var i = from;
    while (i < s.len) {
        if (s[i] == ';') return i;
        i = if (sql_lexer.spanAt(s, i)) |sp| sp.end else i + 1;
    }
    return null;
}

/// Run or wait for more? SQL is whole at a top-level `;` unless the parser, given
/// the session, runs out of input: an open `CREATE FUNCTION` body holds `;`s before `END;`.
pub fn entryComplete(ctx: *anyopaque, s: []const u8) bool {
    const t = std.mem.trim(u8, s, " \t\r\n");
    if (t.len == 0 or t[0] == '\\' or isQuit(t) or isHelp(t) or isClear(t)) return true;
    if (!endsComplete(s)) return false;
    const sess: *Session = @ptrCast(@alignCast(ctx));
    var arena = std.heap.ArenaAllocator.init(sess.decls.gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var text = std.array_list.Managed(u8).init(a);
    for (sess.decls.items.items) |e| {
        text.appendSlice(e.text) catch return true;
        text.appendSlice(";\n") catch return true;
    }
    text.appendSlice(s) catch return true;
    var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    _ = parser.parseSource(a, text.items, &diag) catch {
        return std.mem.indexOf(u8, diag.msg, "found end of input") == null;
    };
    return true;
}

pub fn endsComplete(s: []const u8) bool {
    const t = std.mem.trim(u8, s, " \t\r\n");
    if (t.len == 0 or t[t.len - 1] != ';') return false;
    var i: usize = 0;
    while (nextTopSemi(t, i)) |p| : (i = p + 1) {
        if (p == t.len - 1) return true;
    }
    return false;
}

pub fn splitStatements(arena: std.mem.Allocator, s: []const u8) ![]const []const u8 {
    var out = std.array_list.Managed([]const u8).init(arena);
    var start: usize = 0;
    var i: usize = 0;
    while (nextTopSemi(s, i)) |p| : (i = p + 1) {
        const seg = std.mem.trim(u8, s[start..p], " \t\r\n");
        if (seg.len > 0) try out.append(seg);
        start = p + 1;
    }
    const tail = std.mem.trim(u8, s[start..], " \t\r\n");
    if (tail.len > 0) try out.append(tail);
    return out.toOwnedSlice();
}

pub const DeclKind = enum { connection, function, param, let };

/// A declaration's kind and name, and whether it is written `EXPORT`.
pub const DeclId = struct { kind: DeclKind, name: []const u8, exported: bool = false };

pub fn nextWord(s: []const u8, i: *usize) ?[]const u8 {
    while (i.* < s.len and std.ascii.isWhitespace(s[i.*])) i.* += 1;
    if (i.* >= s.len) return null;
    const start = i.*;
    while (i.* < s.len and !std.ascii.isWhitespace(s[i.*])) i.* += 1;
    return s[start..i.*];
}

/// Bare identifiers only: the dialect has no quoted declaration names.
fn identPrefix(w: []const u8) []const u8 {
    var n: usize = 0;
    while (n < w.len and (std.ascii.isAlphanumeric(w[n]) or w[n] == '_')) n += 1;
    return w[0..n];
}

/// The session declaration a statement makes, by its first words past any leading
/// comment, or null: a `-- helper` line above a declaration is the script's text,
/// not another statement.
pub fn declOf(stmt_in: []const u8) ?DeclId {
    const stmt = pastComments(stmt_in);
    var i: usize = 0;
    var w = nextWord(stmt, &i) orelse return null;
    const exported = std.ascii.eqlIgnoreCase(w, "export");
    if (exported) {
        const at = i;
        const next = nextWord(stmt, &i) orelse return null;
        if (std.ascii.eqlIgnoreCase(next, "param") or std.ascii.eqlIgnoreCase(next, "let")) w = next else i = at;
    }
    if (std.ascii.eqlIgnoreCase(w, "param")) {
        const n = identPrefix(nextWord(stmt, &i) orelse return null);
        return if (n.len == 0) null else .{ .kind = .param, .name = n, .exported = exported };
    }
    if (std.ascii.eqlIgnoreCase(w, "let")) {
        const n = identPrefix(nextWord(stmt, &i) orelse return null);
        return if (n.len == 0) null else .{ .kind = .let, .name = n, .exported = exported };
    }
    if (!std.ascii.eqlIgnoreCase(w, "create") and !std.ascii.eqlIgnoreCase(w, "export")) return null;
    w = nextWord(stmt, &i) orelse return null;
    if (std.ascii.eqlIgnoreCase(w, "or")) {
        w = nextWord(stmt, &i) orelse return null;
        if (!std.ascii.eqlIgnoreCase(w, "replace")) return null;
        w = nextWord(stmt, &i) orelse return null;
    }
    const kind: DeclKind = if (std.ascii.eqlIgnoreCase(w, "connection"))
        .connection
    else if (std.ascii.eqlIgnoreCase(w, "function"))
        .function
    else
        return null;
    const n = identPrefix(nextWord(stmt, &i) orelse return null);
    return if (n.len == 0) null else .{ .kind = kind, .name = n, .exported = exported };
}

/// Where a declaration's name stands: `script`, `session` or an included file's path,
/// the request that declared a session's (`id`), and the name's line, its column in
/// bytes and in UTF-16 units, and its length. A zero `line` is unknown.
pub const Origin = struct {
    file: []const u8 = "",
    id: []const u8 = "",
    line: u32 = 0,
    col: u32 = 0,
    col16: u32 = 0,
    len: u32 = 0,
};

/// The origin of `name`, a slice of `text` (the file `file` holds), or an unknown
/// one when it is not.
pub fn originIn(file: []const u8, id: []const u8, text: []const u8, name: []const u8) Origin {
    const off = sliceAt(text, name) orelse return .{ .file = file, .id = id };
    const line_start = if (std.mem.lastIndexOfScalar(u8, text[0..off], '\n')) |nl| nl + 1 else 0;
    return .{
        .file = file,
        .id = id,
        .line = @intCast(1 + std.mem.count(u8, text[0..off], "\n")),
        .col = @intCast(off - line_start + 1),
        .col16 = @intCast(utf16.byteToUnit(text[line_start..], off - line_start) + 1),
        .len = @intCast(name.len),
    };
}

/// Where `part` starts in `whole`, when it is a slice of it.
pub fn sliceAt(whole: []const u8, part: []const u8) ?usize {
    const w = @intFromPtr(whole.ptr);
    const p = @intFromPtr(part.ptr);
    if (p < w or p + part.len > w + whole.len) return null;
    return p - w;
}

/// `stmt` past its leading blanks and comments, by the lexer's own spans.
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

/// The session's declarations as text, replayed ahead of each entry, and the value
/// each LET was given when its entry ran, which it holds until declared anew.
pub const DeclStore = struct {
    pub const Entry = struct { kind: DeclKind, name: []u8, text: []u8, origin: Origin = .{} };
    const Held = struct { name: []u8, arena: *std.heap.ArenaAllocator, value: Value };

    gpa: std.mem.Allocator,
    items: std.array_list.Managed(Entry),
    held: std.array_list.Managed(Held),

    pub fn init(gpa: std.mem.Allocator) DeclStore {
        return .{ .gpa = gpa, .items = std.array_list.Managed(Entry).init(gpa), .held = std.array_list.Managed(Held).init(gpa) };
    }
    pub fn deinit(self: *DeclStore) void {
        self.clear();
        self.items.deinit();
        self.held.deinit();
    }
    pub fn clear(self: *DeclStore) void {
        for (self.items.items) |e| {
            self.gpa.free(e.name);
            self.gpa.free(e.text);
            self.freeOrigin(e.origin);
        }
        self.items.clearRetainingCapacity();
        for (self.held.items) |h| self.freeHeld(h);
        self.held.clearRetainingCapacity();
    }

    /// Keeps `v` as LET `name`'s value, a copy of its own.
    pub fn hold(self: *DeclStore, name: []const u8, v: Value) !void {
        const arena = try self.gpa.create(std.heap.ArenaAllocator);
        arena.* = std.heap.ArenaAllocator.init(self.gpa);
        errdefer {
            arena.deinit();
            self.gpa.destroy(arena);
        }
        const h = Held{ .name = try arena.allocator().dupe(u8, name), .arena = arena, .value = try nested.detach(arena.allocator(), v) };
        self.drop(name);
        try self.held.append(h);
    }

    /// The values LETs hold, as an entry's run takes them.
    pub fn heldLets(self: *const DeclStore, a: std.mem.Allocator) ![]const HeldLet {
        const out = try a.alloc(HeldLet, self.held.items.len);
        for (self.held.items, out) |h, *o| o.* = .{ .name = h.name, .value = h.value };
        return out;
    }

    fn drop(self: *DeclStore, name: []const u8) void {
        for (self.held.items, 0..) |h, i| if (std.ascii.eqlIgnoreCase(h.name, name)) {
            self.freeHeld(h);
            _ = self.held.orderedRemove(i);
            return;
        };
    }

    fn freeHeld(self: *DeclStore, h: Held) void {
        h.arena.deinit();
        self.gpa.destroy(h.arena);
    }

    /// A LET declared anew drops the value the old one held: it holds what it is given
    /// when it runs.
    pub fn put(self: *DeclStore, id: DeclId, text: []const u8) !void {
        return self.putAt(id, text, .{});
    }

    /// `put`, keeping where the name was declared.
    pub fn putAt(self: *DeclStore, id: DeclId, text: []const u8, origin: Origin) !void {
        if (id.kind == .let) self.drop(id.name);
        const dup_text = try self.gpa.dupe(u8, text);
        errdefer self.gpa.free(dup_text);
        const own = try self.ownOrigin(origin);
        errdefer self.freeOrigin(own);
        for (self.items.items) |*e| {
            if (e.kind != id.kind or !std.ascii.eqlIgnoreCase(e.name, id.name)) continue;
            self.gpa.free(e.text);
            self.freeOrigin(e.origin);
            e.text = dup_text;
            e.origin = own;
            return;
        }
        const dup_name = try self.gpa.dupe(u8, id.name);
        errdefer self.gpa.free(dup_name);
        try self.items.append(.{ .kind = id.kind, .name = dup_name, .text = dup_text, .origin = own });
    }

    /// A copy of the store of its own: each declaration, where it stands, and the
    /// value each LET holds.
    pub fn clone(self: *const DeclStore, gpa: std.mem.Allocator) !DeclStore {
        var c = DeclStore.init(gpa);
        errdefer c.deinit();
        for (self.items.items) |e| try c.putAt(.{ .kind = e.kind, .name = e.name }, e.text, e.origin);
        for (self.held.items) |h| try c.hold(h.name, h.value);
        return c;
    }

    /// The declaration of kind `kind` named `name`, in any case.
    pub fn find(self: *const DeclStore, kind: DeclKind, name: []const u8) ?*const Entry {
        for (self.items.items) |*e| if (e.kind == kind and std.ascii.eqlIgnoreCase(e.name, name)) return e;
        return null;
    }

    /// The value LET `name` holds, if it holds one.
    pub fn heldValue(self: *const DeclStore, name: []const u8) ?Value {
        for (self.held.items) |h| if (std.ascii.eqlIgnoreCase(h.name, name)) return h.value;
        return null;
    }

    fn ownOrigin(self: *DeclStore, o: Origin) !Origin {
        var own = o;
        own.file = try self.gpa.dupe(u8, o.file);
        errdefer self.gpa.free(own.file);
        own.id = try self.gpa.dupe(u8, o.id);
        return own;
    }

    fn freeOrigin(self: *DeclStore, o: Origin) void {
        self.gpa.free(o.file);
        self.gpa.free(o.id);
    }
};

test "endsComplete sees only statement-level semicolons" {
    try std.testing.expect(endsComplete("SELECT 1;"));
    try std.testing.expect(endsComplete("  SELECT 1;\n\n"));
    try std.testing.expect(!endsComplete(""));
    try std.testing.expect(!endsComplete("SELECT 1"));

    try std.testing.expect(!endsComplete("SELECT ';' AS x"));
    try std.testing.expect(endsComplete("SELECT ';' AS x;"));
    try std.testing.expect(!endsComplete("SELECT 'it''s;"));
    try std.testing.expect(endsComplete("SELECT 'it''s;' AS x;"));
    try std.testing.expect(endsComplete("SELECT \"path\\\" FROM t;"));
    try std.testing.expect(!endsComplete("SELECT \"a\"\";"));
    try std.testing.expect(endsComplete("SELECT \"a\"\"b;\" AS x;"));
    try std.testing.expect(endsComplete("SELECT \"a;b\" AS x;"));
    try std.testing.expect(!endsComplete("SELECT 1 -- ;"));
    try std.testing.expect(!endsComplete("/* ; */"));
    try std.testing.expect(endsComplete("/* ; */ SELECT 1;"));

    try std.testing.expect(!endsComplete("FROM c.QUERY($$a;b$$)"));
    try std.testing.expect(endsComplete("FROM c.QUERY($$a;b$$);"));
    try std.testing.expect(!endsComplete("FROM c.QUERY($q$a;b$q$)"));
    try std.testing.expect(endsComplete("FROM c.QUERY($q$a;b$q$);"));
    try std.testing.expect(endsComplete("SELECT $since;"));
}

test "splitStatements splits on statement-level semicolons only" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const parts = try splitStatements(arena.allocator(),
        \\CREATE CONNECTION erp TYPE postgres;
        \\SELECT ';' AS x; -- ; not a split
        \\SELECT 2
    );
    try std.testing.expectEqual(@as(usize, 3), parts.len);
    try std.testing.expectEqualStrings("CREATE CONNECTION erp TYPE postgres", parts[0]);
    try std.testing.expectEqualStrings("SELECT ';' AS x", parts[1]);
    try std.testing.expectEqualStrings("-- ; not a split\nSELECT 2", parts[2]);
}

test "declOf names session declarations" {
    try std.testing.expectEqual(DeclKind.connection, declOf("CREATE CONNECTION erp TYPE postgres").?.kind);
    try std.testing.expectEqualStrings("erp", declOf("CREATE CONNECTION erp TYPE postgres").?.name);
    try std.testing.expectEqualStrings("erp", declOf("create\n  or replace\n  connection erp TYPE mysql").?.name);
    try std.testing.expectEqual(DeclKind.function, declOf("CREATE FUNCTION f(a, b) AS a + b").?.kind);
    try std.testing.expectEqualStrings("f", declOf("CREATE FUNCTION f(a, b) AS a + b").?.name);
    try std.testing.expectEqual(DeclKind.param, declOf("param since date DEFAULT '2020-01-01'").?.kind);
    try std.testing.expectEqualStrings("since", declOf("param since date").?.name);
    try std.testing.expect(declOf("CREATE WIDGET w") == null);
    try std.testing.expectEqualStrings("norm", declOf("-- helper\nCREATE FUNCTION norm(s) AS trim(s)").?.name);
    try std.testing.expectEqualStrings("p", declOf("/* a */ -- b\n  /* c */ PARAM p BOOL DEFAULT false").?.name);
    try std.testing.expect(declOf("-- PARAM p INT\nSELECT 1") == null);
    for ([_][3][]const u8{
        .{ "EXPORT FUNCTION f(x) AS x", "f", "function" },
        .{ "export or replace connection pg TYPE postgres", "pg", "connection" },
        .{ "EXPORT PARAM env STRING DEFAULT 'dev'", "env", "param" },
        .{ "EXPORT LET dir = 'out'", "dir", "let" },
    }) |c| {
        const d = declOf(c[0]).?;
        try std.testing.expectEqualStrings(c[1], d.name);
        try std.testing.expectEqualStrings(c[2], @tagName(d.kind));
    }

    try std.testing.expect(declOf("SELECT 1") == null);
    try std.testing.expect(declOf("CREATE TABLE t") == null);
    try std.testing.expect(declOf("CREATE OR SOMETHING CONNECTION erp") == null);
    try std.testing.expect(declOf("") == null);
}

test "DeclStore holds a LET's value, of any kind, until the LET is declared anew or the session clears" {
    var store = DeclStore.init(std.testing.allocator);
    defer store.deinit();
    try store.put(.{ .kind = .let, .name = "b" }, "LET b = from_base64('AQI=')");
    var raw = [_]u8{ 1, 2 };
    try store.hold("b", .{ .bytes = &raw });
    raw[0] = 9;
    var held = try store.heldLets(std.testing.allocator);
    try std.testing.expectEqualSlices(u8, &.{ 1, 2 }, held[0].value.bytes);
    std.testing.allocator.free(held);
    try store.hold("b", .{ .string = "again" });
    held = try store.heldLets(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), held.len);
    try std.testing.expectEqualStrings("again", held[0].value.string);
    std.testing.allocator.free(held);
    try store.put(.{ .kind = .let, .name = "B" }, "LET B = 2");
    try std.testing.expectEqual(@as(usize, 0), store.held.items.len);
    try store.hold("c", .{ .int = 1 });
    store.clear();
    try std.testing.expectEqual(@as(usize, 0), store.held.items.len);
}
