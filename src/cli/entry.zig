//! A REPL or kernel entry as statements: where each ends, which are declarations,
//! and the session's store of declarations that later entries build on.

const Session = @import("repl.zig").Session;
const isClear = @import("repl.zig").isClear;
const isHelp = @import("repl.zig").isHelp;
const isQuit = @import("repl.zig").isQuit;
const parser = @import("../lang/sql_parser.zig");
const std = @import("std");
const Catalog = @import("catalog.zig").Catalog;
const httpResources = @import("catalog.zig").httpResources;

/// Next statement-level `;`, skipping strings, dollar quotes and comments by the
/// lexer's own rules, so the REPL agrees with the parser on where a statement ends.
pub fn nextTopSemi(s: []const u8, from: usize) ?usize {
    var i = from;
    while (i < s.len) {
        switch (s[i]) {
            ';' => return i,
            '\'' => {
                i += 1;
                while (i < s.len) : (i += 1) {
                    if (s[i] != '\'') continue;
                    if (i + 1 < s.len and s[i + 1] == '\'') {
                        i += 1;
                        continue;
                    }
                    break;
                }
                i += 1;
            },
            '"' => {
                i += 1;
                while (i < s.len) : (i += 1) {
                    if (s[i] == '\\') {
                        i += 1;
                        continue;
                    }
                    if (s[i] == '"') break;
                }
                i += 1;
            },
            '-' => {
                if (i + 1 < s.len and s[i + 1] == '-') {
                    i = std.mem.indexOfScalarPos(u8, s, i, '\n') orelse s.len;
                } else i += 1;
            },
            '/' => {
                if (i + 1 < s.len and s[i + 1] == '*') {
                    const end = std.mem.indexOfPos(u8, s, i + 2, "*/");
                    i = if (end) |e| e + 2 else s.len;
                } else i += 1;
            },
            '$' => {
                if (dollarTagLen(s, i)) |n| {
                    const end = std.mem.indexOfPos(u8, s, i + n, s[i .. i + n]);
                    i = if (end) |e| e + n else s.len;
                } else i += 1;
            },
            else => i += 1,
        }
    }
    return null;
}

/// Length of the dollar-quote opener at `s[i]` (`$$` = 2, `$tag$` = tag+2), or
/// null when this `$` starts a `$param` reference instead.
fn dollarTagLen(s: []const u8, i: usize) ?usize {
    var j = i + 1;
    while (j < s.len and s[j] != '$') : (j += 1) {
        const c = s[j];
        if (!std.ascii.isAlphanumeric(c) and c != '_') return null;
        if (j == i + 1 and std.ascii.isDigit(c)) return null;
    }
    if (j >= s.len) return null;
    return j + 1 - i;
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

pub const DeclKind = enum { connection, function, param, let, endpoint, resource };

pub const DeclId = struct { kind: DeclKind, name: []const u8 };

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

/// The session declaration a statement makes, by its first words, or null. A resource
/// is named `conn.name`, as two connections may each have one of the same name.
pub fn declOf(stmt: []const u8) ?DeclId {
    var i: usize = 0;
    var w = nextWord(stmt, &i) orelse return null;
    if (std.ascii.eqlIgnoreCase(w, "param")) {
        const n = identPrefix(nextWord(stmt, &i) orelse return null);
        return if (n.len == 0) null else .{ .kind = .param, .name = n };
    }
    if (std.ascii.eqlIgnoreCase(w, "let")) {
        const n = identPrefix(nextWord(stmt, &i) orelse return null);
        return if (n.len == 0) null else .{ .kind = .let, .name = n };
    }
    if (!std.ascii.eqlIgnoreCase(w, "create")) return null;
    w = nextWord(stmt, &i) orelse return null;
    if (std.ascii.eqlIgnoreCase(w, "or")) {
        w = nextWord(stmt, &i) orelse return null;
        if (!std.ascii.eqlIgnoreCase(w, "replace")) return null;
        w = nextWord(stmt, &i) orelse return null;
    }
    if (std.ascii.eqlIgnoreCase(w, "endpoint")) return .{ .kind = .endpoint, .name = "" };
    if (std.ascii.eqlIgnoreCase(w, "resource")) {
        const q = nextWord(stmt, &i) orelse return null;
        const conn = identPrefix(q);
        if (conn.len == 0 or conn.len + 1 >= q.len or q[conn.len] != '.') return null;
        const n = identPrefix(q[conn.len + 1 ..]);
        return if (n.len == 0) null else .{ .kind = .resource, .name = q[0 .. conn.len + 1 + n.len] };
    }
    const kind: DeclKind = if (std.ascii.eqlIgnoreCase(w, "connection"))
        .connection
    else if (std.ascii.eqlIgnoreCase(w, "function"))
        .function
    else
        return null;
    const n = identPrefix(nextWord(stmt, &i) orelse return null);
    return if (n.len == 0) null else .{ .kind = kind, .name = n };
}

pub const DeclStore = struct {
    const Entry = struct { kind: DeclKind, name: []u8, text: []u8 };

    gpa: std.mem.Allocator,
    items: std.array_list.Managed(Entry),

    pub fn init(gpa: std.mem.Allocator) DeclStore {
        return .{ .gpa = gpa, .items = std.array_list.Managed(Entry).init(gpa) };
    }
    pub fn deinit(self: *DeclStore) void {
        self.clear();
        self.items.deinit();
    }
    pub fn clear(self: *DeclStore) void {
        for (self.items.items) |e| {
            self.gpa.free(e.name);
            self.gpa.free(e.text);
        }
        self.items.clearRetainingCapacity();
    }
    pub fn put(self: *DeclStore, id: DeclId, text: []const u8) !void {
        const dup_text = try self.gpa.dupe(u8, text);
        errdefer self.gpa.free(dup_text);
        for (self.items.items) |*e| {
            if (e.kind != id.kind or !std.ascii.eqlIgnoreCase(e.name, id.name)) continue;
            self.gpa.free(e.text);
            e.text = dup_text;
            return;
        }
        const dup_name = try self.gpa.dupe(u8, id.name);
        errdefer self.gpa.free(dup_name);
        try self.items.append(.{ .kind = id.kind, .name = dup_name, .text = dup_text });
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
    try std.testing.expect(!endsComplete("SELECT \"a\\\";"));
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

test "Tab lists an http connection's resources as its tables" {
    const gpa = std.testing.allocator;
    var sess = Session{ .decls = DeclStore.init(gpa), .catalog = Catalog.init(gpa) };
    defer sess.decls.deinit();
    defer sess.catalog.deinit();
    for ([_][]const u8{
        "CREATE CONNECTION rc TYPE http OPTIONS (base_url = 'u')",
        "CREATE CONNECTION pg TYPE postgres OPTIONS (host = 'h')",
        "CREATE RESOURCE rc.countries AS GET('/all')",
        "CREATE RESOURCE rc.regions AS GET('/regions')",
        "CREATE RESOURCE rcx.other AS GET('/o')",
    }) |t| try sess.decls.put(declOf(t).?, t);

    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const got = (try httpResources(ar.allocator(), &sess.completer(), "rc")).?;
    try std.testing.expectEqual(@as(usize, 2), got.len);
    try std.testing.expectEqualStrings("countries", got[0]);
    try std.testing.expectEqualStrings("regions", got[1]);
    try std.testing.expect(try httpResources(ar.allocator(), &sess.completer(), "pg") == null);
}

test "declOf names session declarations" {
    try std.testing.expectEqual(DeclKind.connection, declOf("CREATE CONNECTION erp TYPE postgres").?.kind);
    try std.testing.expectEqualStrings("erp", declOf("CREATE CONNECTION erp TYPE postgres").?.name);
    try std.testing.expectEqualStrings("erp", declOf("create\n  or replace\n  connection erp TYPE mysql").?.name);
    try std.testing.expectEqual(DeclKind.function, declOf("CREATE FUNCTION f(a, b) AS a + b").?.kind);
    try std.testing.expectEqualStrings("f", declOf("CREATE FUNCTION f(a, b) AS a + b").?.name);
    try std.testing.expectEqual(DeclKind.param, declOf("param since date DEFAULT '2020-01-01'").?.kind);
    try std.testing.expectEqualStrings("since", declOf("param since date").?.name);
    try std.testing.expectEqual(DeclKind.endpoint, declOf("CREATE ENDPOINT '/x'").?.kind);
    try std.testing.expectEqual(DeclKind.resource, declOf("CREATE RESOURCE rc.countries AS GET('/all')").?.kind);
    try std.testing.expectEqualStrings("rc.countries", declOf("CREATE RESOURCE rc.countries AS GET('/all')").?.name);
    try std.testing.expect(declOf("CREATE RESOURCE countries AS GET('/all')") == null);

    try std.testing.expect(declOf("SELECT 1") == null);
    try std.testing.expect(declOf("CREATE TABLE t") == null);
    try std.testing.expect(declOf("CREATE OR SOMETHING CONNECTION erp") == null);
    try std.testing.expect(declOf("") == null);
}
