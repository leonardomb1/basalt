//! An entry made runnable: the session's declarations as a prelude, each LET holding
//! the value it was given when its entry ran, shared by the REPL and the kernel.

const DeclId = @import("entry.zig").DeclId;
const DeclStore = @import("entry.zig").DeclStore;
const checkText = @import("cmd_check.zig").checkText;
const Value = @import("../exec/value.zig").Value;
const ast = @import("../lang/ast.zig");
const declOf = @import("entry.zig").declOf;
const entryStatements = @import("repl/repl.zig").entryStatements;
const include = @import("../lang/include.zig");
const runtime = @import("../runtime/run.zig");
const splitStatements = @import("entry.zig").splitStatements;
const std = @import("std");
const nested = @import("../exec/nested.zig");
const parser = @import("../lang/sql_parser.zig");

pub const Pending = struct { id: DeclId, text: []const u8 };

pub const Prepared = struct {
    text: []const u8,
    entry_at: usize,
    prog: ast.Program,
    pending: []const Pending,
    executable: usize,
};

pub const PrepareError = error{ ParseFailed, OutOfMemory };

/// A declaration the entry makes again is left out of the prelude, so no name is declared
/// twice. The entry's own `@include` lines lead the text, as an include must precede
/// every statement, and are blanked where they stood so the entry's lines keep their
/// numbers.
fn withPrelude(a: std.mem.Allocator, decls: *const DeclStore, pending: []const Pending, block: []const u8) !struct { text: []const u8, entry_at: usize } {
    var buf = std.array_list.Managed(u8).init(a);
    const cut = include.directivesEnd(block);
    try buf.appendSlice(block[0..cut]);
    if (cut > 0 and block[cut - 1] != '\n') try buf.append('\n');
    for (decls.items.items) |e| {
        var shadowed = false;
        for (pending) |p| {
            if (p.id.kind == e.kind and std.ascii.eqlIgnoreCase(p.id.name, e.name)) shadowed = true;
        }
        if (shadowed) continue;
        try buf.appendSlice(e.text);
        try buf.appendSlice(";\n");
    }
    const entry_at = buf.items.len;
    for (block[0..cut]) |c| try buf.append(if (c == '\n') '\n' else ' ');
    try buf.appendSlice(block[cut..]);
    return .{ .text = buf.items, .entry_at = entry_at };
}

pub fn sessionText(a: std.mem.Allocator, decls: *const DeclStore, block: []const u8) !struct { text: []const u8, entry_at: usize } {
    var pending = std.array_list.Managed(Pending).init(a);
    for (try splitStatements(a, block)) |st| {
        if (declOf(st)) |id| try pending.append(.{ .id = id, .text = st });
    }
    const j = try withPrelude(a, decls, pending.items, block);
    return .{ .text = j.text, .entry_at = j.entry_at };
}

/// Nothing is committed here: the caller commits `pending` once the entry stands, so a
/// typo cannot poison the session. Statements are re-read from the parse, as a function
/// body's `;`s fool the pre-scan, except under `@include`, whose positions are elsewhere.
pub fn prepareEntry(a: std.mem.Allocator, decls: *const DeclStore, block: []const u8, diag: *include.Diag, text_out: ?*[]const u8) PrepareError!Prepared {
    var pending = std.array_list.Managed(Pending).init(a);
    var executable: usize = 0;
    for (try splitStatements(a, block)) |st| {
        const id = declOf(st) orelse {
            executable += 1;
            continue;
        };
        try pending.append(.{ .id = id, .text = st });
    }

    const joined = try withPrelude(a, decls, pending.items, block);
    const text = joined.text;
    const entry_at = joined.entry_at;
    if (text_out) |t| t.* = text;

    const prog = include.loadProgram(a, text, "<repl>", ".", diag) catch |e| switch (e) {
        error.ParseFailed => return error.ParseFailed,
        error.OutOfMemory => return error.OutOfMemory,
    };

    if (std.mem.indexOf(u8, block, "@include") == null) {
        const parsed = try entryStatements(a, text, entry_at, prog);
        pending.clearRetainingCapacity();
        executable = 0;
        for (parsed) |p| {
            if (p.id) |id| try pending.append(.{ .id = id, .text = p.text }) else executable += 1;
        }
    }
    return .{ .text = text, .entry_at = entry_at, .prog = prog, .pending = pending.items, .executable = executable };
}

/// Whether the session's declarations, with an entry's own (`pending`) in place of
/// those they declare anew, check without error by themselves. An entry whose
/// declarations do not is still run, to report why, but leaves the session as it
/// was, so a declaration that cannot stand never fails the entries after it.
pub fn declarationsStand(a: std.mem.Allocator, decls: *const DeclStore, pending: []const Pending) !bool {
    if (pending.len == 0) return true;
    var buf = std.array_list.Managed(u8).init(a);
    for (decls.items.items) |e| {
        const shadowed = for (pending) |p| {
            if (p.id.kind == e.kind and std.ascii.eqlIgnoreCase(p.id.name, e.name)) break true;
        } else false;
        if (shadowed) continue;
        try buf.appendSlice(e.text);
        try buf.appendSlice(";\n");
    }
    for (pending) |p| {
        try buf.appendSlice(p.text);
        try buf.appendSlice(";\n");
    }
    const issues = try checkText(a, buf.items, "<repl>", ".", .{ .declarations_only = true });
    for (issues) |is| if (!is.warning) return false;
    return true;
}

/// Collects the values an entry's LETs are given as it runs, copied out of the run,
/// for the session to hold once the entry stands.
pub const LetFreezer = struct {
    arena: std.mem.Allocator,
    given: std.array_list.Managed(Given),

    const Given = struct { name: []const u8, value: Value };

    pub fn init(arena: std.mem.Allocator) LetFreezer {
        return .{ .arena = arena, .given = std.array_list.Managed(Given).init(arena) };
    }

    pub fn hook(self: *LetFreezer) runtime.LetHook {
        return .{ .ctx = self, .f = onLet };
    }

    fn onLet(ctx: *anyopaque, name: []const u8, v: Value) void {
        const self: *LetFreezer = @ptrCast(@alignCast(ctx));
        const n = self.arena.dupe(u8, name) catch return;
        const kept = nested.detach(self.arena, v) catch return;
        self.given.append(.{ .name = n, .value = kept }) catch {};
    }

    pub fn commit(self: *const LetFreezer, decls: *DeclStore) !void {
        for (self.given.items) |g| {
            for (decls.items.items) |e| {
                if (e.kind == .let and std.ascii.eqlIgnoreCase(e.name, g.name)) break;
            } else continue;
            try decls.hold(g.name, g.value);
        }
    }
};

test "an entry's @include runs after the session has declarations, its lines keeping their numbers" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "lib.sql", .data = "CREATE FUNCTION twice(x) AS x * 2;\n" });
    const lib = try std.fs.path.join(a, &.{ try tmp.dir.realpathAlloc(a, "."), "lib.sql" });

    var decls = DeclStore.init(std.testing.allocator);
    defer decls.deinit();
    const param = "PARAM n INT DEFAULT 1";
    try decls.put(declOf(param).?, param);

    const block = try std.fmt.allocPrint(a, "@include '{s}';\nSELECT twice($n) AS m;", .{lib});
    var diag: include.Diag = .{};
    const p = try prepareEntry(a, &decls, block, &diag, null);
    try std.testing.expectEqual(std.mem.count(u8, block[0..std.mem.indexOf(u8, block, "SELECT").?], "\n"), std.mem.count(u8, p.text[p.entry_at..std.mem.indexOf(u8, p.text, "SELECT").?], "\n"));
}
