//! `@include 'path.sql';` — C-style composition, resolved before parsing.
//!
//! Directives may only appear at the TOP of a script (blank lines and `--`
//! comments may sit between them); one file per directive. Paths resolve
//! relative to the INCLUDING file's directory — for `-c` scripts, stdin and the
//! REPL that is the process cwd. Each included file is parsed on its own, so a
//! syntax error inside it reports ITS line numbers under ITS name; the resulting
//! statements are prepended to the includer's, in include order.
//!
//! Nothing is filtered: an included file may declare connections, params,
//! functions or whole pipelines, and they all become part of the program. The one
//! structural fixup is the kind decl: the parser puts one at `stmts[0]` of every
//! program it returns (batch unless the script says `CREATE ENDPOINT`), and the
//! runtime and the HTTP server both read it there — so the merged program keeps
//! exactly one, the includer's, unless only an included file declares an endpoint.
//!
//! Cycles are detected on canonical absolute paths and the nesting depth is
//! capped; both report the offending directive's position in the file that
//! wrote it.

const std = @import("std");
const ast = @import("ast.zig");
const parser = @import("sql_parser.zig");

pub const Error = parser.Error;

/// A located diagnostic plus the file it belongs to. Included files are parsed
/// separately, so `parse.line`/`parse.col` are relative to `label` — callers must
/// print `label`, not the top-level script path, or they will name the wrong file.
/// Include failures (missing file, cycle, misplaced directive) use the same shape,
/// positioned at the directive that caused them.
pub const Diag = struct {
    parse: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 },
    label: []const u8 = "",
};

/// Nesting cap: deep enough for real layering, shallow enough that a pathological
/// tree fails fast.
const max_depth: u32 = 16;
const max_file_bytes: usize = 8 << 20;

/// Parse `text` (named `label`, with `@include` paths resolved against `base_dir`)
/// into one program. `base_dir` may be "" or "." for cwd-relative resolution.
/// Everything is allocated in `arena`, and the AST slices into the include texts
/// read here, so the arena must outlive the program.
pub fn loadProgram(
    arena: std.mem.Allocator,
    text: []const u8,
    label: []const u8,
    base_dir: []const u8,
    diag: *Diag,
) Error!ast.Program {
    var ctx = Ctx{
        .arena = arena,
        .diag = diag,
        .stack = std.array_list.Managed(Frame).init(arena),
    };
    // The root only joins the cycle stack when it is a real file (`-c`, stdin and
    // the REPL have no path, and so can never be re-included).
    if (std.fs.cwd().realpathAlloc(arena, label)) |canon| {
        try ctx.stack.append(.{ .canon = canon, .shown = label });
    } else |_| {}
    return load(&ctx, text, label, base_dir);
}

const Frame = struct { canon: []const u8, shown: []const u8 };

const Directive = struct { path: []const u8, line: u32, col: u32 };

const Ctx = struct {
    arena: std.mem.Allocator,
    diag: *Diag,
    stack: std.array_list.Managed(Frame),
    depth: u32 = 0,

    fn fail(self: *Ctx, label: []const u8, line: u32, col: u32, comptime fmt: []const u8, args: anytype) Error {
        const msg: []const u8 = std.fmt.allocPrint(self.arena, fmt, args) catch "include error";
        self.diag.* = .{ .parse = .{ .msg = msg, .line = line, .col = col }, .label = label };
        return error.ParseFailed;
    }
};

fn load(ctx: *Ctx, text: []const u8, label: []const u8, base_dir: []const u8) Error!ast.Program {
    var incs = std.array_list.Managed(Directive).init(ctx.arena);
    const body_at = try scanDirectives(ctx, text, label, &incs);
    const skipped: u32 = @intCast(std.mem.count(u8, text[0..body_at], "\n"));
    try rejectLateIncludes(ctx, text[body_at..], label, skipped + 1);

    var subs = std.array_list.Managed(ast.Program).init(ctx.arena);
    for (incs.items) |d| {
        const resolved = try resolvePath(ctx, base_dir, d.path);
        const src = std.fs.cwd().readFileAlloc(ctx.arena, resolved, max_file_bytes) catch |e| {
            if (e == error.OutOfMemory) return error.OutOfMemory;
            return ctx.fail(label, d.line, d.col, "cannot read include `{s}`: {s}", .{ resolved, @errorName(e) });
        };
        const canon: []const u8 = std.fs.cwd().realpathAlloc(ctx.arena, resolved) catch resolved;
        for (ctx.stack.items, 0..) |f, i| {
            if (!std.mem.eql(u8, f.canon, canon)) continue;
            return ctx.fail(label, d.line, d.col, "include cycle: {s}", .{try cycleTrail(ctx, i, resolved)});
        }
        if (ctx.depth + 1 > max_depth)
            return ctx.fail(label, d.line, d.col, "include nesting deeper than {d} files at `{s}`", .{ max_depth, resolved });

        try ctx.stack.append(.{ .canon = canon, .shown = resolved });
        ctx.depth += 1;
        try subs.append(try load(ctx, src, resolved, std.fs.path.dirname(resolved) orelse "."));
        ctx.depth -= 1;
        _ = ctx.stack.pop();
    }

    // A script whose whole body is directives contributes no statements of its
    // own; parsing the blanked remainder would only report "empty program".
    const rest = try blankPrefix(ctx, text, body_at, incs.items.len);
    const has_body = incs.items.len == 0 or std.mem.trim(u8, rest, " \t\r\n").len > 0;
    var main: ?ast.Program = null;
    if (has_body) {
        var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        main = parser.parseSource(ctx.arena, rest, &pdiag) catch |e| switch (e) {
            error.OutOfMemory => return e,
            error.ParseFailed => {
                ctx.diag.* = .{ .parse = pdiag, .label = label };
                return error.ParseFailed;
            },
        };
    }
    if (incs.items.len == 0) return main.?;

    // One kind decl survives: the includer's, unless only an included file
    // declares an endpoint.
    var kind: ?ast.KindDecl = if (main) |m| leadKind(m) else null;
    var out = std.array_list.Managed(ast.Stmt).init(ctx.arena);
    for (subs.items) |sp| {
        if (leadKind(sp)) |k| {
            if (kind == null or (kind.?.kind != .http and k.kind == .http)) kind = k;
        }
        try out.appendSlice(stmtsOf(sp));
    }
    if (main) |m| try out.appendSlice(stmtsOf(m));
    if (kind) |k| try out.insert(0, .{ .kind = k });
    return .{ .stmts = try out.toOwnedSlice(), .explain = if (main) |m| m.explain else .none };
}

/// The kind decl the parser puts at `stmts[0]`, if this program has one.
fn leadKind(p: ast.Program) ?ast.KindDecl {
    if (p.stmts.len == 0 or p.stmts[0] != .kind) return null;
    return p.stmts[0].kind;
}

/// A program's statements without its leading kind decl.
fn stmtsOf(p: ast.Program) []const ast.Stmt {
    return if (leadKind(p) == null) p.stmts else p.stmts[1..];
}

/// The includer's text with the directive region replaced by as many newlines as
/// it held, so the parser still reports the includer's real line numbers.
fn blankPrefix(ctx: *Ctx, text: []const u8, body_at: usize, n_directives: usize) Error![]const u8 {
    if (n_directives == 0) return text;
    const lines = std.mem.count(u8, text[0..body_at], "\n");
    const buf = try ctx.arena.alloc(u8, lines + text.len - body_at);
    @memset(buf[0..lines], '\n');
    @memcpy(buf[lines..], text[body_at..]);
    return buf;
}

/// Collect the leading `@include` directives, returning the offset where the rest
/// of the script starts. Blank lines and `--` comments between directives are
/// skipped; the first other line ends the scan.
fn scanDirectives(ctx: *Ctx, text: []const u8, label: []const u8, out: *std.array_list.Managed(Directive)) Error!usize {
    var off: usize = 0;
    var line: u32 = 1;
    while (off < text.len) {
        const nl = std.mem.indexOfScalarPos(u8, text, off, '\n') orelse text.len;
        const raw = text[off..nl];
        const trimmed = std.mem.trim(u8, raw, " \t\r");
        if (trimmed.len != 0 and !std.mem.startsWith(u8, trimmed, "--")) {
            if (!std.mem.startsWith(u8, trimmed, "@include")) break;
            const col: u32 = @intCast((std.mem.indexOfScalar(u8, raw, '@') orelse 0) + 1);
            try out.append(.{ .path = try parseDirective(ctx, trimmed, label, line, col), .line = line, .col = col });
        }
        off = if (nl == text.len) text.len else nl + 1;
        line += 1;
    }
    return off;
}

/// `@include '<path>';`, with an optional trailing `--` comment.
fn parseDirective(ctx: *Ctx, s: []const u8, label: []const u8, line: u32, col: u32) Error![]const u8 {
    const syntax = "expected `@include 'path.sql';`";
    var rest = s["@include".len..];
    if (rest.len == 0 or (rest[0] != ' ' and rest[0] != '\t' and rest[0] != '\''))
        return ctx.fail(label, line, col, "{s}", .{syntax});
    rest = std.mem.trimLeft(u8, rest, " \t");
    if (rest.len == 0 or rest[0] != '\'') return ctx.fail(label, line, col, "{s}", .{syntax});
    const close = std.mem.indexOfScalarPos(u8, rest, 1, '\'') orelse
        return ctx.fail(label, line, col, "unterminated include path — {s}", .{syntax});
    const path = rest[1..close];
    if (path.len == 0) return ctx.fail(label, line, col, "empty include path", .{});
    var tail = std.mem.trimLeft(u8, rest[close + 1 ..], " \t");
    if (tail.len == 0 or tail[0] != ';') return ctx.fail(label, line, col, "missing `;` after @include", .{});
    tail = std.mem.trimLeft(u8, tail[1..], " \t");
    if (tail.len != 0 and !std.mem.startsWith(u8, tail, "--"))
        return ctx.fail(label, line, col, "unexpected text after @include — one file per directive", .{});
    return path;
}

/// A directive past the header is a hard error rather than a silently ignored
/// line. Line-oriented: a literal `@include` at the start of a line inside a
/// `$$…$$` block would be misread, which no real script does.
fn rejectLateIncludes(ctx: *Ctx, rest: []const u8, label: []const u8, first_line: u32) Error!void {
    var off: usize = 0;
    var line = first_line;
    while (off < rest.len) {
        const nl = std.mem.indexOfScalarPos(u8, rest, off, '\n') orelse rest.len;
        const raw = rest[off..nl];
        if (std.mem.startsWith(u8, std.mem.trim(u8, raw, " \t\r"), "@include")) {
            const col: u32 = @intCast((std.mem.indexOfScalar(u8, raw, '@') orelse 0) + 1);
            return ctx.fail(label, line, col, "@include must precede statements", .{});
        }
        off = if (nl == rest.len) rest.len else nl + 1;
        line += 1;
    }
}

fn resolvePath(ctx: *Ctx, base_dir: []const u8, path: []const u8) Error![]const u8 {
    if (std.fs.path.isAbsolute(path)) return path;
    if (base_dir.len == 0 or std.mem.eql(u8, base_dir, ".")) return path;
    return std.fs.path.join(ctx.arena, &.{ base_dir, path });
}

/// `a.sql -> lib/b.sql -> a.sql`, from the stack entry that closes the loop.
fn cycleTrail(ctx: *Ctx, from: usize, repeat: []const u8) Error![]const u8 {
    var buf = std.array_list.Managed(u8).init(ctx.arena);
    for (ctx.stack.items[from..]) |f| {
        try buf.appendSlice(f.shown);
        try buf.appendSlice(" -> ");
    }
    try buf.appendSlice(repeat);
    return buf.items;
}

const testing = std.testing;

/// Write `data` at `sub_path` under `tmp` (creating parent directories).
fn writeFile(tmp: *std.testing.TmpDir, sub_path: []const u8, data: []const u8) !void {
    if (std.fs.path.dirname(sub_path)) |d| try tmp.dir.makePath(d);
    try tmp.dir.writeFile(.{ .sub_path = sub_path, .data = data });
}

test "@include prepends a decls file to the including script" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try writeFile(&tmp, "lib/fns.sql", "CREATE FUNCTION dbl(x) AS x * 2;\n");

    var diag: Diag = .{};
    const prog = try loadProgram(a,
        \\@include 'lib/fns.sql';
        \\
        \\SELECT dbl(id) AS y FROM 'in.csv';
    , "main.sql", base, &diag);

    // stmts[0] is the kind decl the parser always emits (batch here).
    try testing.expectEqual(@as(usize, 3), prog.stmts.len);
    try testing.expect(prog.stmts[0] == .kind);
    try testing.expectEqual(ast.Kind.batch, prog.stmts[0].kind.kind);
    try testing.expectEqualStrings("dbl", prog.stmts[1].func.name);
    try testing.expect(prog.stmts[2] == .output);
}

test "@include keeps a leading CREATE ENDPOINT at stmts[0]" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try writeFile(&tmp, "fns.sql", "CREATE FUNCTION dbl(x) AS x * 2;\n");

    var diag: Diag = .{};
    const prog = try loadProgram(a,
        \\@include 'fns.sql';
        \\CREATE ENDPOINT '/x';
        \\SELECT dbl(id) AS y FROM 'in.csv';
    , "main.sql", base, &diag);

    try testing.expectEqual(@as(usize, 3), prog.stmts.len);
    try testing.expect(prog.stmts[0] == .kind);
    try testing.expectEqual(ast.Kind.http, prog.stmts[0].kind.kind);
    try testing.expect(prog.stmts[1] == .func);
    try testing.expect(prog.stmts[2] == .output);
}

test "nested @include resolves relative to the including file" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try writeFile(&tmp, "sub/a.sql",
        \\@include 'b.sql';
        \\CREATE FUNCTION dbl(x) AS x * 2;
    );
    try writeFile(&tmp, "sub/b.sql", "CREATE FUNCTION inc(x) AS x + 1;\n");

    var diag: Diag = .{};
    const prog = try loadProgram(a,
        \\@include 'sub/a.sql';
        \\SELECT dbl(inc(id)) AS y FROM 'in.csv';
    , "main.sql", base, &diag);

    try testing.expectEqual(@as(usize, 4), prog.stmts.len);
    try testing.expect(prog.stmts[0] == .kind);
    try testing.expectEqualStrings("inc", prog.stmts[1].func.name);
    try testing.expectEqualStrings("dbl", prog.stmts[2].func.name);
    try testing.expect(prog.stmts[3] == .output);
}

test "@include cycle is reported with the file trail" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try writeFile(&tmp, "a.sql", "@include 'b.sql';\n");
    try writeFile(&tmp, "b.sql", "@include 'a.sql';\n");

    var diag: Diag = .{};
    try testing.expectError(error.ParseFailed, loadProgram(a,
        \\@include 'a.sql';
        \\SELECT id FROM 'in.csv';
    , "main.sql", base, &diag));

    try testing.expect(std.mem.startsWith(u8, diag.parse.msg, "include cycle:"));
    try testing.expect(std.mem.indexOf(u8, diag.parse.msg, "a.sql") != null);
    try testing.expect(std.mem.indexOf(u8, diag.parse.msg, "b.sql") != null);
    // Reported against the file holding the offending directive.
    try testing.expect(std.mem.endsWith(u8, diag.label, "b.sql"));
}

test "@include after a statement is rejected" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try writeFile(&tmp, "fns.sql", "CREATE FUNCTION dbl(x) AS x * 2;\n");

    var diag: Diag = .{};
    try testing.expectError(error.ParseFailed, loadProgram(a,
        \\SELECT id FROM 'in.csv';
        \\@include 'fns.sql';
    , "main.sql", base, &diag));

    try testing.expectEqualStrings("@include must precede statements", diag.parse.msg);
    try testing.expectEqual(@as(u32, 2), diag.parse.line);
    try testing.expectEqualStrings("main.sql", diag.label);
}

test "missing include names the path it tried" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");

    var diag: Diag = .{};
    try testing.expectError(error.ParseFailed, loadProgram(a,
        \\@include 'lib/nope.sql';
        \\SELECT id FROM 'in.csv';
    , "main.sql", base, &diag));

    try testing.expect(std.mem.indexOf(u8, diag.parse.msg, "lib/nope.sql") != null);
    try testing.expectEqual(@as(u32, 1), diag.parse.line);
    try testing.expectEqualStrings("main.sql", diag.label);
}

test "a parse error inside an included file carries that file's label and line" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try writeFile(&tmp, "lib/bad.sql",
        \\CREATE FUNCTION dbl(x) AS x * 2;
        \\
        \\!;
    );

    var diag: Diag = .{};
    try testing.expectError(error.ParseFailed, loadProgram(a,
        \\@include 'lib/bad.sql';
        \\SELECT id FROM 'in.csv';
    , "main.sql", base, &diag));

    try testing.expect(std.mem.endsWith(u8, diag.label, "lib/bad.sql"));
    try testing.expectEqual(@as(u32, 3), diag.parse.line);
}

test "directive lines keep the includer's own line numbers" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try writeFile(&tmp, "fns.sql", "CREATE FUNCTION dbl(x) AS x * 2;\n");

    var diag: Diag = .{};
    try testing.expectError(error.ParseFailed, loadProgram(a,
        \\-- header
        \\@include 'fns.sql';
        \\
        \\!;
    , "main.sql", base, &diag));

    try testing.expectEqualStrings("main.sql", diag.label);
    try testing.expectEqual(@as(u32, 4), diag.parse.line);
}

test "malformed directives are parse errors" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var d1: Diag = .{};
    try testing.expectError(error.ParseFailed, loadProgram(a, "@include 'a.sql'\n", "m.sql", ".", &d1));
    try testing.expectEqualStrings("missing `;` after @include", d1.parse.msg);

    var d2: Diag = .{};
    try testing.expectError(error.ParseFailed, loadProgram(a, "@include a.sql;\n", "m.sql", ".", &d2));
    try testing.expect(std.mem.indexOf(u8, d2.parse.msg, "@include 'path.sql';") != null);

    var d3: Diag = .{};
    try testing.expectError(error.ParseFailed, loadProgram(a, "@include 'a.sql'; 'b.sql';\n", "m.sql", ".", &d3));
    try testing.expect(std.mem.indexOf(u8, d3.parse.msg, "one file per directive") != null);
}

test "a script without directives is parsed unchanged" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var diag: Diag = .{};
    const prog = try loadProgram(a, "-- just a query\nSELECT id FROM 'in.csv';\n", "m.sql", "", &diag);
    try testing.expectEqual(@as(usize, 2), prog.stmts.len);
    try testing.expect(prog.stmts[1] == .output);
}

test "an include-only script contributes just the included statements" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try writeFile(&tmp, "fns.sql", "CREATE FUNCTION dbl(x) AS x * 2;\n");

    var diag: Diag = .{};
    const prog = try loadProgram(a, "@include 'fns.sql';\n", "main.sql", base, &diag);
    try testing.expectEqual(@as(usize, 2), prog.stmts.len);
    try testing.expect(prog.stmts[0] == .kind);
    try testing.expectEqualStrings("dbl", prog.stmts[1].func.name);
}
