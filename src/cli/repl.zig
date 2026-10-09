//! `basalt repl`: the loop, its meta commands (`\\connections`, `\\save`, `\\edit`, …),
//! the startup file, and how an entry is run and its result shown.

const Catalog = @import("catalog.zig").Catalog;
const Completer = @import("catalog.zig").Completer;
const DeclId = @import("entry.zig").DeclId;
const DeclStore = @import("entry.zig").DeclStore;
const Editor = @import("line.zig").Editor;
const LetFreezer = @import("prepare.zig").LetFreezer;
const LineResult = @import("line.zig").Result;
const analyze = @import("../runtime/analyze.zig");
const ast = @import("../lang/ast.zig");
const connTables = @import("catalog.zig").connTables;
const connTypeOf = @import("catalog.zig").connTypeOf;
const connectWizard = @import("wizard.zig").connectWizard;
const endsComplete = @import("entry.zig").endsComplete;
const entryComplete = @import("entry.zig").entryComplete;
const include = @import("../lang/include.zig");
const nextTopSemi = @import("entry.zig").nextTopSemi;
const nextWord = @import("entry.zig").nextWord;
const prepareEntry = @import("prepare.zig").prepareEntry;
const runtime = @import("../runtime/run.zig");
const std = @import("std");
const suggest = @import("catalog.zig").suggest;
const table = @import("../connect/table.zig");
const view = @import("view.zig");
const DeclKind = @import("entry.zig").DeclKind;
const parser = @import("../lang/sql_parser.zig");

pub const Session = struct {
    decls: DeclStore,
    format: runtime.StdoutFormat = .table,
    tty: bool = false,
    catalog: Catalog,
    last_entry: ?[]u8 = null,
    announce: bool = true,

    pub fn completer(self: *Session) Completer {
        return .{ .gpa = self.decls.gpa, .decls = &self.decls, .catalog = &self.catalog };
    }
};

fn tilde(buf: []u8, path: []const u8) []const u8 {
    const home = std.posix.getenv("HOME") orelse return path;
    if (home.len > 1 and std.mem.startsWith(u8, path, home) and path.len > home.len and path[home.len] == '/')
        return std.fmt.bufPrint(buf, "~{s}", .{path[home.len..]}) catch path;
    return path;
}

fn banner(msg: *std.Io.Writer, color: bool) !void {
    const dim: []const u8 = if (color) "\x1b[2m" else "";
    const bold: []const u8 = if (color) "\x1b[1m" else "";
    const mark: []const u8 = if (color) "\x1b[38;5;208m" else "";
    const off: []const u8 = if (color) "\x1b[0m" else "";
    try msg.print("{s}  ▄▄▄ {s} {s}basalt{s} {s}{s}{s}\n", .{ mark, off, bold, off, dim, @import("build_options").version, off });
    try msg.print("{s}  ███ {s} {s}SQL in, rows moved: files, object stores and databases in one binary{s}\n", .{ mark, off, dim, off });
    try msg.print("{s}  ▀▀▀ {s} {s}\\help keys and commands · \\connect a new source · \\q quit{s}\n\n", .{ mark, off, dim, off });
}

fn startupPath(gpa: std.mem.Allocator) ?[]u8 {
    if (std.process.getEnvVarOwned(gpa, "XDG_CONFIG_HOME")) |x| {
        defer gpa.free(x);
        return std.fs.path.join(gpa, &.{ x, "basalt", "repl.sql" }) catch null;
    } else |_| {}
    const home = std.process.getEnvVarOwned(gpa, "HOME") catch return null;
    defer gpa.free(home);
    return std.fs.path.join(gpa, &.{ home, ".config", "basalt", "repl.sql" }) catch null;
}

fn connAttr(text: []const u8, key: []const u8) ?[]const u8 {
    var i: usize = 0;
    while (i + key.len < text.len) : (i += 1) {
        if (!std.ascii.eqlIgnoreCase(text[i .. i + key.len], key)) continue;
        if (i > 0 and (std.ascii.isAlphanumeric(text[i - 1]) or text[i - 1] == '_')) continue;
        var j = i + key.len;
        while (j < text.len and text[j] == ' ') j += 1;
        if (j >= text.len or text[j] != '=') continue;
        j += 1;
        while (j < text.len and text[j] == ' ') j += 1;
        if (j >= text.len) return null;
        if (text[j] == '\'') {
            const end = std.mem.indexOfScalarPos(u8, text, j + 1, '\'') orelse return null;
            return text[j + 1 .. end];
        }
        var e = j;
        while (e < text.len and text[e] != ',' and text[e] != ')' and text[e] != ' ') e += 1;
        return text[j..e];
    }
    return null;
}

fn listConnections(sess: *Session, msg: *std.Io.Writer, probe: bool) !void {
    var n: usize = 0;
    for (sess.decls.items.items) |e| if (e.kind == .connection) {
        n += 1;
    };
    if (n == 0) return msg.writeAll("(no connections — CREATE CONNECTION ... to add one, \\i <file> to load some)\n");
    try msg.print("{s: <14} {s: <10} {s: <28} {s: <16} {s}\n", .{ "name", "type", "host", "database", "status" });
    for (sess.decls.items.items) |e| {
        if (e.kind != .connection) continue;
        const ty = connTypeOf(e.text) orelse "?";
        const host = connAttr(e.text, "host") orelse connAttr(e.text, "fe_host") orelse connAttr(e.text, "url") orelse connAttr(e.text, "base_url") orelse "";
        const db = connAttr(e.text, "database") orelse "";
        var status: []const u8 = "not asked yet";
        if (probe and !std.mem.eql(u8, ty, "http")) _ = connTables(&sess.completer(), e.name);
        if (sess.catalog.tables.get(e.name)) |t| {
            status = if (t.len == 0) "unreachable, or no tables" else try std.fmt.allocPrint(sess.catalog.arena.allocator(), "reached, {d} tables", .{t.len});
        }
        try msg.print("{s: <14} {s: <10} {s: <28} {s: <16} {s}\n", .{ e.name, ty, host, db, status });
    }
    for (sess.decls.items.items) |e| {
        if (e.kind == .connection) continue;
        try msg.print("{s} {s}\n", .{ @tagName(e.kind), e.name });
    }
}

/// Runs a file as an entry, so its declarations join the session, which `@include` does not.
fn sourceFile(alloc: std.mem.Allocator, path: []const u8, sess: *Session, msg: *std.Io.Writer) !void {
    const text = std.fs.cwd().readFileAlloc(alloc, path, 1 << 22) catch |e|
        return msg.print("error: could not read `{s}`: {s}\n", .{ path, @errorName(e) });
    defer alloc.free(text);
    const trimmed = std.mem.trim(u8, text, " \t\r\n");
    if (trimmed.len == 0) return;
    try runBlock(alloc, trimmed, sess, msg);
}

pub fn saveDecls(alloc: std.mem.Allocator, path_arg: []const u8, sess: *Session, msg: *std.Io.Writer) !void {
    const path = if (path_arg.len > 0) try alloc.dupe(u8, path_arg) else (startupPath(alloc) orelse return msg.writeAll("error: no HOME to save under\n"));
    defer alloc.free(path);
    if (std.fs.path.dirname(path)) |d| std.fs.cwd().makePath(d) catch {};
    const f = std.fs.cwd().createFile(path, .{}) catch |e|
        return msg.print("error: could not write `{s}`: {s}\n", .{ path, @errorName(e) });
    defer f.close();
    var n: usize = 0;
    for (sess.decls.items.items) |e| {
        if (e.kind == .endpoint) continue;
        try f.writeAll(e.text);
        try f.writeAll(";\n");
        n += 1;
    }
    var tbuf: [512]u8 = undefined;
    try msg.print("saved {d} declaration{s} to {s}\n", .{ n, if (n == 1) "" else "s", tilde(&tbuf, path) });
}

fn editAndRun(alloc: std.mem.Allocator, path_arg: []const u8, sess: *Session, msg: *std.Io.Writer) !void {
    const editor = std.process.getEnvVarOwned(alloc, "EDITOR") catch try alloc.dupe(u8, "vi");
    defer alloc.free(editor);
    var tmp_buf: [64]u8 = undefined;
    const tmp = try std.fmt.bufPrint(&tmp_buf, "/tmp/basalt-edit-{d}.sql", .{std.os.linux.getpid()});
    const path = if (path_arg.len > 0) path_arg else tmp;
    if (path_arg.len == 0) {
        const f = try std.fs.cwd().createFile(tmp, .{});
        defer f.close();
        if (sess.last_entry) |l| try f.writeAll(l);
        try f.writeAll("\n");
    }
    defer if (path_arg.len == 0) std.fs.cwd().deleteFile(tmp) catch {};
    try msg.flush();
    var child = std.process.Child.init(&.{ editor, path }, alloc);
    child.stdin_behavior = .Inherit;
    child.stdout_behavior = .Inherit;
    child.stderr_behavior = .Inherit;
    const term = child.spawnAndWait() catch |e| return msg.print("error: could not run `{s}`: {s}\n", .{ editor, @errorName(e) });
    if (term != .Exited or term.Exited != 0) return msg.print("{s} exited without saving; nothing run\n", .{editor});
    try sourceFile(alloc, path, sess, msg);
}

/// A blank line also runs a pending buffer, which `echo ... | basalt repl` relies on.
/// After a ^C the abort flag is reset, so the session is not left poisoned.
pub fn cmdRepl(alloc: std.mem.Allocator) !u8 {
    var in_buf: [64 * 1024]u8 = undefined;
    var in_file = std.fs.File.stdin().reader(&in_buf);
    const in = &in_file.interface;

    var msg_buf: [4096]u8 = undefined;
    var msg_file = std.fs.File.stderr().writer(&msg_buf);
    const msg = &msg_file.interface;

    var sess = Session{ .decls = DeclStore.init(alloc), .tty = std.posix.isatty(std.fs.File.stdin().handle), .catalog = Catalog.init(alloc) };
    defer sess.decls.deinit();
    defer sess.catalog.deinit();

    var editor: ?Editor = if (sess.tty) Editor.init(alloc) else null;
    defer if (editor) |*e| e.deinit();

    table.interactive = sess.tty;
    defer table.dropLast();

    if (sess.tty) {
        try banner(msg, !std.process.hasEnvVarConstant("NO_COLOR"));
        try msg.flush();
    }

    if (startupPath(alloc)) |sp| {
        defer alloc.free(sp);
        if (std.fs.cwd().access(sp, .{})) |_| {
            sess.announce = false;
            try sourceFile(alloc, sp, &sess, msg);
            sess.announce = true;
            if (sess.tty) {
                var conns: usize = 0;
                var others: usize = 0;
                for (sess.decls.items.items) |e| if (e.kind == .connection) {
                    conns += 1;
                } else {
                    others += 1;
                };
                var tbuf: [512]u8 = undefined;
                try msg.print("loaded {s}: {d} connection{s}", .{ tilde(&tbuf, sp), conns, if (conns == 1) "" else "s" });
                if (others > 0) try msg.print(", {d} other declaration{s}", .{ others, if (others == 1) "" else "s" });
                try msg.writeAll("\n\n");
            }
            try msg.flush();
        } else |_| {}
    }
    defer if (sess.last_entry) |l| alloc.free(l);

    var block = std.array_list.Managed(u8).init(alloc);
    defer block.deinit();

    var quit = false;
    while (!quit) {
        runtime.resetAbort();
        block.clearRetainingCapacity();
        while (true) {
            var line: []const u8 = undefined;
            if (editor) |*ed| {
                switch (ed.readEntry(.{ .complete = entryComplete, .complete_ctx = &sess, .suggest = suggest, .suggest_ctx = &sess }) catch |e| blk: {
                    try msg.print("input error: {s}\n", .{@errorName(e)});
                    try msg.flush();
                    break :blk LineResult.eof;
                }) {
                    .eof => quit = true,
                    .interrupt => {},
                    .line => |l| {
                        defer alloc.free(l);
                        ed.remember(l);
                        const t = std.mem.trim(u8, l, " \t\r\n");
                        if (t.len > 0 and (t[0] == '\\' or isQuit(t) or isHelp(t) or isClear(t))) {
                            if (isQuit(t)) quit = true else {
                                const framed = !isClear(t) and !isViewCmd(t);
                                if (framed) try entryGap(&sess, msg);
                                try metaCommand(t, &sess, msg);
                                if (framed) try separator(&sess, msg);
                            }
                        } else {
                            try block.appendSlice(l);
                            if (t.len > 0 and !endsComplete(l)) try block.append(';');
                        }
                    },
                }
                break;
            } else {
                const maybe = in.takeDelimiter('\n') catch |e| {
                    try msg.print("input error: {s}\n", .{@errorName(e)});
                    try msg.flush();
                    quit = true;
                    break;
                };
                line = maybe orelse {
                    quit = true;
                    break;
                };
            }
            const t = std.mem.trim(u8, line, " \t\r\n");
            if (t.len == 0) {
                if (block.items.len == 0) continue;
                break;
            }
            if (block.items.len == 0 and (t[0] == '\\' or isQuit(t) or isHelp(t) or isClear(t))) {
                if (isQuit(t)) {
                    quit = true;
                    break;
                }
                try metaCommand(t, &sess, msg);
                continue;
            }
            try block.appendSlice(line);
            try block.append('\n');
            if (entryComplete(&sess, block.items)) break;
        }

        const trimmed = std.mem.trim(u8, block.items, " \t\r\n");
        if (trimmed.len == 0) continue;
        if (sess.last_entry) |l| alloc.free(l);
        sess.last_entry = try alloc.dupe(u8, trimmed);
        try entryGap(&sess, msg);
        try runBlock(alloc, trimmed, &sess, msg);
        try separator(&sess, msg);
    }
    if (sess.tty) {
        try msg.writeAll("bye\n");
        try msg.flush();
    }
    return 0;
}

fn separator(sess: *const Session, msg: *std.Io.Writer) !void {
    if (!sess.tty) return;
    try msg.writeAll("\n");
    try msg.flush();
}

fn entryGap(sess: *const Session, msg: *std.Io.Writer) !void {
    if (!sess.tty) return;
    try msg.writeAll("\n");
    try msg.flush();
}

fn isViewCmd(t: []const u8) bool {
    var i: usize = 0;
    const cmd = nextWord(t, &i) orelse return false;
    return std.mem.eql(u8, cmd, "\\view") or std.mem.eql(u8, cmd, "\\v");
}

const EntryStmt = struct { id: ?DeclId, text: []const u8 };

fn offsetOf(text: []const u8, pos: ast.Pos) ?usize {
    if (pos.line == 0) return null;
    var line: u32 = 1;
    var i: usize = 0;
    while (line < pos.line) : (line += 1) i = (std.mem.indexOfScalarPos(u8, text, i, '\n') orelse return null) + 1;
    return @min(text.len, i + pos.col - 1);
}

/// The entry's statements, each cut at its own last top-level `;`, so a statement
/// function is one statement however many `;`s its body holds.
pub fn entryStatements(arena: std.mem.Allocator, text: []const u8, entry_at: usize, prog: ast.Program) ![]EntryStmt {
    const Found = struct { off: usize, stmt: ast.Stmt };
    var found = std.array_list.Managed(Found).init(arena);
    for (prog.stmts[1..]) |st| {
        const pos = analyze.stmtPos(st) orelse continue;
        const off = offsetOf(text, pos) orelse continue;
        if (off < entry_at) continue;
        try found.append(.{ .off = off, .stmt = st });
    }
    std.mem.sort(Found, found.items, {}, struct {
        fn lt(_: void, x: Found, y: Found) bool {
            return x.off < y.off;
        }
    }.lt);
    var out = std.array_list.Managed(EntryStmt).init(arena);
    for (found.items, 0..) |f, i| {
        if (i + 1 < found.items.len and found.items[i + 1].off == f.off) continue;
        const end = if (i + 1 < found.items.len) found.items[i + 1].off else text.len;
        var piece = text[f.off..end];
        var last: ?usize = null;
        var k: usize = 0;
        while (nextTopSemi(piece, k)) |p| : (k = p + 1) last = p;
        if (last) |p| piece = piece[0..p];
        piece = std.mem.trim(u8, piece, " \t\r\n");
        const id: ?DeclId = switch (f.stmt) {
            .connection => |c| .{ .kind = .connection, .name = c.name },
            .func => |fd| .{ .kind = .function, .name = fd.name },
            .param => |p| .{ .kind = .param, .name = p.name },
            .let_const => |l| .{ .kind = .let, .name = l.name },
            else => null,
        };
        try out.append(.{ .id = id, .text = piece });
    }
    return out.toOwnedSlice();
}

/// Only a position inside the entry, the part the person typed, gets a caret.
fn errorCaret(msg: *std.Io.Writer, text: []const u8, entry: []const u8, line: u32, col: u32) !void {
    const entry_at = std.mem.lastIndexOf(u8, text, entry) orelse return;
    const prelude_lines = std.mem.count(u8, text[0..entry_at], "\n");
    if (line == 0 or line <= prelude_lines) return;
    var it = std.mem.splitScalar(u8, entry, '\n');
    var n: usize = prelude_lines + 1;
    while (it.next()) |ln| : (n += 1) {
        if (n != line) continue;
        try msg.print("  {s}\n  ", .{ln});
        var c: u32 = 1;
        var i: usize = 0;
        while (c < col and i < ln.len) : (c += 1) {
            try msg.writeByte(if (ln[i] == '\t') '\t' else ' ');
            i += 1;
            while (i < ln.len and ln[i] & 0xC0 == 0x80) i += 1;
        }
        try msg.writeAll("^\n");
        return;
    }
}

fn metaCommand(t: []const u8, sess: *Session, msg: *std.Io.Writer) !void {
    defer msg.flush() catch {};
    if (isHelp(t)) return replHelp(msg);

    var i: usize = 0;
    const cmd = nextWord(t, &i) orelse return;
    const rest = std.mem.trim(u8, t[i..], " \t\r\n");

    if (std.mem.eql(u8, cmd, "\\connections") or std.mem.eql(u8, cmd, "\\c")) {
        return listConnections(sess, msg, std.ascii.eqlIgnoreCase(rest, "test"));
    }
    if (std.mem.eql(u8, cmd, "\\i") or std.mem.eql(u8, cmd, "\\source")) {
        if (rest.len == 0) return msg.writeAll("usage: \\i <file.sql>  — run a file; its declarations join the session\n");
        return sourceFile(sess.decls.gpa, rest, sess, msg);
    }
    if (std.mem.eql(u8, cmd, "\\save")) return saveDecls(sess.decls.gpa, rest, sess, msg);
    if (std.mem.eql(u8, cmd, "\\connect")) {
        if (!sess.tty) return msg.writeAll("error: \\connect asks questions; it needs a terminal\n");
        return connectWizard(sess.decls.gpa, rest, sess, msg);
    }
    if (std.mem.eql(u8, cmd, "\\edit") or std.mem.eql(u8, cmd, "\\e")) return editAndRun(sess.decls.gpa, rest, sess, msg);
    if (isClear(t)) {
        return msg.writeAll("\x1b[2J\x1b[H");
    }
    if (std.mem.eql(u8, cmd, "\\reset")) {
        sess.decls.clear();
        return msg.writeAll("reset: declarations forgotten\n");
    }
    if (std.mem.eql(u8, cmd, "\\format") or std.mem.eql(u8, cmd, "\\f")) {
        if (rest.len == 0) {
            // fall through to the echo below
        } else if (parseReplFormat(rest)) |f| {
            sess.format = f;
        } else {
            return msg.print("error: \\format takes `table`, `json`, `csv` or `tsv`, got `{s}`\n", .{rest});
        }
        return msg.print("format {s}\n", .{@tagName(sess.format)});
    }
    if (std.mem.eql(u8, cmd, "\\d") or std.mem.eql(u8, cmd, "\\dt")) {
        if (rest.len == 0) return msg.writeAll(if (std.mem.eql(u8, cmd, "\\d")) "usage: \\d <conn.table | 'file' | conn.QUERY($$...$$)>  — the same as DESCRIBE\n" else "usage: \\dt <conn[.schema]> [pattern]  — the same as SHOW TABLES FROM\n");
        var text = std.array_list.Managed(u8).init(sess.decls.gpa);
        defer text.deinit();
        if (std.mem.eql(u8, cmd, "\\d")) {
            try text.writer().print("DESCRIBE {s};", .{rest});
        } else {
            var it = std.mem.tokenizeAny(u8, rest, " \t");
            const target = it.next().?;
            try text.writer().print("SHOW TABLES FROM {s}", .{target});
            if (it.next()) |pat| try text.writer().print(" LIKE '{s}'", .{pat});
            try text.appendSlice(";");
        }
        return runBlock(sess.decls.gpa, text.items, sess, msg);
    }
    if (std.mem.eql(u8, cmd, "\\view") or std.mem.eql(u8, cmd, "\\v")) {
        if (!sess.tty or !std.posix.isatty(std.fs.File.stdout().handle))
            return msg.writeAll("error: \\view needs a terminal\n");
        const g = table.last() orelse return msg.writeAll("nothing to view yet — run a SELECT first\n");
        try msg.flush();
        return view.run(sess.decls.gpa, g);
    }
    try msg.print("error: unknown command `{s}` — \\help for help\n", .{cmd});
}

fn parseReplFormat(name: []const u8) ?runtime.StdoutFormat {
    inline for (.{ runtime.StdoutFormat.table, .json, .csv, .tsv }) |f| {
        if (std.ascii.eqlIgnoreCase(name, @tagName(f))) return f;
    }
    return null;
}

/// Logs errors only, but not `quiet`, which would swallow the entry's own `PRINT`s.
pub fn runBlock(alloc: std.mem.Allocator, block: []const u8, sess: *Session, msg: *std.Io.Writer) !void {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();

    var diag: include.Diag = .{};
    var text: []const u8 = block;
    const entry = prepareEntry(a, &sess.decls, block, &diag, &text) catch |e| switch (e) {
        error.EndpointInSession => {
            try msg.writeAll("error: CREATE ENDPOINT can't run in the REPL — put it in a script and use `basalt serve <dir>`\n");
            try msg.flush();
            return;
        },
        error.ParseFailed => {
            if (diag.label.len > 0 and !std.mem.eql(u8, diag.label, "<repl>"))
                try msg.print("error: {s}:{d}:{d}: {s}\n", .{ diag.label, diag.parse.line, diag.parse.col, diag.parse.msg })
            else
                try msg.print("error: {d}:{d}: {s}\n", .{ diag.parse.line, diag.parse.col, diag.parse.msg });
            if (sess.tty) try errorCaret(msg, text, block, diag.parse.line, diag.parse.col);
            try msg.flush();
            return;
        },
        error.OutOfMemory => return error.OutOfMemory,
    };
    const prog = entry.prog;
    const pending = entry.pending;

    for (pending) |p| try sess.decls.put(p.id, p.text);

    if (entry.executable == 0) {
        for (pending) |p| if (p.id.kind == .let) {
            var freezer = LetFreezer.init(a);
            var rdiag: runtime.Diag = .{};
            _ = runtime.run(alloc, prog, .{
                .log = .{ .summary = .none, .level = .err },
                .declarations_only = true,
                .on_let = freezer.hook(),
            }, &rdiag) catch |e| {
                if (e == error.OutOfMemory) return e;
                try msg.print("error: {s}\n", .{if (rdiag.msg.len > 0) rdiag.msg else @errorName(e)});
                try msg.flush();
                return;
            };
            try freezer.commit(&sess.decls);
            break;
        };
        if (sess.announce) for (pending) |p| try msg.print("ok: {s} {s}\n", .{ @tagName(p.id.kind), p.id.name });
        try msg.flush();
        return;
    }

    const prepared = try appendDisplaySinks(a, prog);

    if (prog.explain == .plan) {
        var adiag: analyze.Diag = .{};
        const plan = analyze.analyze(a, prepared, &adiag) catch |e| switch (e) {
            error.OutOfMemory => return e,
            error.AnalyzeFailed => {
                try msg.print("error: {s}\n", .{adiag.msg});
                try msg.flush();
                return;
            },
        };
        try analyze.render(plan, msg);
        try msg.flush();
        return;
    }

    const t0 = std.time.nanoTimestamp();
    var rdiag: runtime.Diag = .{};
    var freezer = LetFreezer.init(a);
    defer freezer.commit(&sess.decls) catch {};
    _ = runtime.run(alloc, prepared, .{
        .on_let = freezer.hook(),
        .log = .{ .summary = .none, .level = .err },
        .stdout_format = sess.format,
        .explain = prog.explain == .analyze,
        .progress = sess.tty and std.posix.isatty(std.fs.File.stderr().handle),
        .items = true,
    }, &rdiag) catch |e| {
        if (e == error.OutOfMemory) return e;
        if (e == error.Aborted)
            try msg.writeAll("aborted\n")
        else if (rdiag.msg.len > 0)
            try msg.print("error: {s}\n", .{rdiag.msg})
        else
            try msg.print("error: {s}\n", .{@errorName(e)});
        try msg.flush();
        return;
    };
    if (sess.tty) {
        const us: u64 = @intCast(@divTrunc(std.time.nanoTimestamp() - t0, std.time.ns_per_us));
        try msg.print("({d}.{d} ms)\n", .{ us / 1000, us % 1000 / 100 });
        try msg.flush();
    }
}

/// Appends a `write stdout` table sink to any output pipeline not already ending in a `write`.
pub fn appendDisplaySinks(arena: std.mem.Allocator, prog: ast.Program) !ast.Program {
    const stmts = try arena.alloc(ast.Stmt, prog.stmts.len);
    for (prog.stmts, 0..) |st, i| {
        stmts[i] = st;
        if (st != .output) continue;
        const p = st.output;
        if (p.stages.len > 0 and p.stages[p.stages.len - 1].node == .write) continue;
        const stages = try arena.alloc(ast.Stage, p.stages.len + 1);
        @memcpy(stages[0..p.stages.len], p.stages);
        stages[p.stages.len] = .{
            .node = .{ .write = .{ .connector = "stdout", .form = null, .target = "", .mode = .default } },
            .hints = &.{},
            .pos = p.pos,
        };
        stmts[i] = .{ .output = .{ .stages = stages, .pos = p.pos, .show = p.show } };
    }
    return .{ .stmts = stmts };
}

pub fn isClear(t: []const u8) bool {
    inline for (.{ "\\clear", "\\cls", "clear", "cls" }) |k| {
        if (std.ascii.eqlIgnoreCase(t, k)) return true;
    }
    return false;
}

pub fn isQuit(s: []const u8) bool {
    inline for (.{ "\\q", "\\quit", ":q", "quit", "exit" }) |k| {
        if (std.mem.eql(u8, s, k)) return true;
    }
    return false;
}

pub fn isHelp(s: []const u8) bool {
    inline for (.{ "\\help", "\\h", "help", "?" }) |k| {
        if (std.mem.eql(u8, s, k)) return true;
    }
    return false;
}

fn replHelp(msg: *std.Io.Writer) !void {
    try msg.writeAll(
        \\A statement ends in `;` and runs on Enter. A terminal SELECT prints a table;
        \\LOAD INTO writes to its target. Declarations stay for the session.
        \\
        \\session
        \\  \connect [type]         make a connection by filling in a form (arrows move, esc cancels)
        \\  \connections, \c        the connections as a table; `\c test` reaches each now
        \\  \i <file>               run a file; its declarations join the session
        \\  \save [file]            write the declarations, by default to the startup file
        \\                          ~/.config/basalt/repl.sql, which every session loads
        \\  \reset                  forget every declaration
        \\  \edit, \e [file]        open the last entry (or a file) in $EDITOR, then run it
        \\
        \\sources
        \\  \dt <conn[.schema]> [p] the source's tables        (SHOW TABLES FROM ... [LIKE 'p'])
        \\  \d <conn.table|'file'>  its columns and types      (DESCRIBE ...)
        \\
        \\results
        \\  \view, \v               the last result full-screen: arrows move, s sorts, / filters, f finds, q leaves
        \\  \format table|json|csv|tsv
        \\                          the output format (bare \format shows it); \f for short
        \\  \clear, \cls            clear the screen (Ctrl+L too, mid-entry)
        \\  \help, \h, ?            this help
        \\  \q, \quit, exit         leave
        \\
        \\editing — the entry is a small text editor
        \\  Enter                   run when the entry ends in `;` and the cursor is at its end;
        \\                          otherwise a new line (after `(` it steps in, `)` on its own line)
        \\  Ctrl+J                  run the entry as it stands, `;` or not (Ctrl+Enter where sent)
        \\  Tab                     complete: keywords, connections, CTEs, $params, a path in
        \\                          quotes, `conn.` tables, the columns of tables and files named;
        \\                          Tab again cycles the choices. On selected lines: indent
        \\  Shift+Tab               dedent the selected lines
        \\  Ctrl+R                  search the history; Ctrl+R again for older, Enter keeps it
        \\  Up / Down               travel the entry; past its edge, recall history whole
        \\  PgUp / PgDn             a screenful up or down; an entry taller than the terminal
        \\                          scrolls with the cursor, ↑ ↓ in the gutter mark hidden lines
        \\
        \\  Ctrl+Left / Right       by word (Alt+B / Alt+F too)
        \\  Home / End              line start (Home toggles the indent) / line end
        \\  Ctrl+Home / End         start / end of the entry
        \\  Shift + any move        select; typing, Backspace or Delete replace the selection
        \\  Alt+Up / Down           move the line or selected lines; with Shift, duplicate them
        \\
        \\  Ctrl+A                  select all
        \\  Ctrl+C                  copy the selection — with none, drop the entry
        \\  Ctrl+X / Ctrl+V         cut / paste (copy reaches the system clipboard, OSC 52)
        \\  Ctrl+Z / Ctrl+Y         undo / redo
        \\  Ctrl+W, Alt+D           delete the word before / after the cursor
        \\  Ctrl+K / Ctrl+U         delete to the line end / the whole line
        \\  Ctrl+/                  comment the lines out with `--`, or back in
        \\  Esc                     drop the selection
        \\  Ctrl+D                  leave, when the entry is empty
        \\
        \\  ( [ { ' "  close themselves; typing the closer steps over it; over a selection
        \\  they wrap it. A paste is inserted as text, never run line by line.
        \\
    );
}

test "entryStatements: a statement function is one declaration, its body's `;`s notwithstanding" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const prelude = "CREATE CONNECTION sr TYPE starrocks OPTIONS (host = 'h', database = 'd');\n";
    const entry =
        \\CREATE FUNCTION f(x) AS x * 2;
        \\CREATE OR REPLACE FUNCTION load(name) AS
        \\  PRINT 'loading ' || $name;
        \\  LOAD INTO sr.IDENTIFIER('t_' || $name) AS SELECT 1 AS v;
        \\END;
        \\-- a trailing note
        \\SELECT f(1) AS y;
    ;
    const text = try std.mem.concat(a, u8, &.{ prelude, entry });
    var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(a, text, &diag);
    const got = try entryStatements(a, text, prelude.len, prog);
    try std.testing.expectEqual(@as(usize, 3), got.len);
    try std.testing.expectEqualStrings("f", got[0].id.?.name);
    try std.testing.expectEqualStrings("CREATE FUNCTION f(x) AS x * 2", got[0].text);
    try std.testing.expectEqual(DeclKind.function, got[1].id.?.kind);
    try std.testing.expectEqualStrings("load", got[1].id.?.name);
    try std.testing.expect(std.mem.startsWith(u8, got[1].text, "CREATE OR REPLACE FUNCTION load(name) AS"));
    try std.testing.expect(std.mem.endsWith(u8, got[1].text, "END"));
    try std.testing.expect(std.mem.indexOf(u8, got[1].text, "LOAD INTO") != null);
    try std.testing.expect(got[2].id == null);
}

test "REPL input classification: quit, help" {
    try std.testing.expect(isQuit("\\q"));
    try std.testing.expect(isQuit("exit"));
    try std.testing.expect(!isQuit("exit()"));
    try std.testing.expect(isHelp("?"));
    try std.testing.expect(isHelp("\\help"));
    try std.testing.expect(!isHelp("help me"));
}

test "appendDisplaySinks adds `write stdout` only to sink-less pipelines" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const bare = try parser.parseSource(a,
        \\SELECT * FROM 'in.csv' LIMIT 2;
    , &diag);
    const prepared = try appendDisplaySinks(a, bare);
    var found = false;
    for (prepared.stmts) |st| {
        if (st != .output) continue;
        found = true;
        const stages = st.output.stages;
        try std.testing.expect(stages[stages.len - 1].node == .write);
        try std.testing.expectEqualStrings("stdout", stages[stages.len - 1].node.write.connector);
    }
    try std.testing.expect(found);

    const sunk = try parser.parseSource(a,
        \\LOAD INTO 'out.csv' AS SELECT * FROM 'in.csv';
    , &diag);
    const kept = try appendDisplaySinks(a, sunk);
    var found_sunk = false;
    for (kept.stmts, sunk.stmts) |st, orig| {
        if (st != .output) continue;
        found_sunk = true;
        const stages = st.output.stages;
        try std.testing.expectEqualStrings("csv", stages[stages.len - 1].node.write.connector);
        try std.testing.expectEqual(orig.output.stages.len, stages.len);
    }
    try std.testing.expect(found_sunk);
}
