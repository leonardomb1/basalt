//! `@include 'path.sql';`: C-style composition, resolved before parsing.
//!
//! Directives may only appear at the top of a script (blank lines and `--`
//! comments may sit between them), one file per directive; a later one is a hard
//! error, not an ignored line. Paths resolve relative to the including file's
//! directory; for `-c` scripts, stdin and the REPL that is the process cwd. Each
//! included file is parsed on its own, so a syntax error inside it reports its
//! own line numbers under its own name (`Diag.label`, which callers must print
//! instead of the top-level path). The includer is parsed with the directive
//! region blanked to newlines, so its line numbers stay real, and it sees the
//! connections and table functions its includes declared, as if written above it.
//!
//! A file is spliced in once, at its first include, as `#pragma once` would have
//! it: two libraries that both include a third share one copy. Splicing per path
//! once defined shared `CREATE FUNCTION`s twice and made diamond-shaped trees
//! exponential. Nothing is filtered: an included file may declare connections,
//! params, functions or whole pipelines. The one structural fixup is the kind
//! decl the parser puts at `stmts[0]` of every program, which the runtime reads
//! there: the merged program keeps exactly one.
//!
//! Cycles are detected on canonical absolute paths and nesting is capped at
//! `max_depth` (with `max_total_stmts` as a backstop); both report the offending
//! directive's position in the file that wrote it. The root joins the cycle stack
//! only when it is a real file. The AST slices into the include texts read here,
//! so the arena must outlive the program.

const std = @import("std");
const ast = @import("ast.zig");
const parser = @import("sql_parser.zig");
const remote = @import("remote.zig");
const include_fetch = @import("../net/include_fetch.zig");

pub const Error = parser.Error;

pub const Diag = struct {
    parse: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 },
    label: []const u8 = "",
};

const max_depth: u32 = 16;
const max_file_bytes: usize = 8 << 20;

const max_total_stmts: usize = 100_000;

/// Parses `text` (named `label`, with `@include` paths resolved against `base_dir`,
/// which may be "" or "." for the cwd) into one program, allocated in `arena`.
pub fn loadProgram(
    arena: std.mem.Allocator,
    text: []const u8,
    label: []const u8,
    base_dir: []const u8,
    diag: *Diag,
) Error!ast.Program {
    return loadProgramOpts(arena, text, label, base_dir, diag, .{});
}

/// The root script's parse options, of which `known_tables` apply throughout and
/// `errors` (keep parsing past an error) to the root alone, since an included file
/// that does not parse fails outright; and the resolver remote includes go through,
/// without which they are refused.
pub const Options = struct {
    parse: parser.Options = .{},
    remote: ?*remote.Resolver = null,
};

pub fn loadProgramOpts(
    arena: std.mem.Allocator,
    text: []const u8,
    label: []const u8,
    base_dir: []const u8,
    diag: *Diag,
    opts: Options,
) Error!ast.Program {
    var ctx = Ctx{
        .arena = arena,
        .diag = diag,
        .stack = std.array_list.Managed(Frame).init(arena),
        .memo = std.StringHashMap(ast.Program).init(arena),
        .known_tables = opts.parse.known_tables,
        .errors = opts.parse.errors,
        .files = std.array_list.Managed([]const u8).init(arena),
        .aliases = std.StringHashMap([]const u8).init(arena),
        .remote = opts.remote,
        .root_dir = base_dir,
    };
    if (std.fs.cwd().realpathAlloc(arena, label)) |canon| {
        try ctx.stack.append(.{ .canon = canon, .shown = label });
    } else |_| {}
    try ctx.files.append(label);
    var prog = try load(&ctx, text, label, .{ .dir = base_dir }, 0, null);
    if (ctx.files.items.len > 1) prog.files = ctx.files.items;
    return prog;
}

const Frame = struct { canon: []const u8, shown: []const u8 };

const Directive = struct { path: []const u8, alias: ?[]const u8, line: u32, col: u32 };

/// Where an included file's own relative includes resolve: a local directory, or the
/// repository and ref of a remote file.
const Base = union(enum) { dir: []const u8, remote: remote.Spec };

const Ctx = struct {
    arena: std.mem.Allocator,
    diag: *Diag,
    stack: std.array_list.Managed(Frame),
    memo: std.StringHashMap(ast.Program),
    depth: u32 = 0,
    stmt_budget: usize = max_total_stmts,
    known_tables: []const []const u8 = &.{},
    errors: ?*std.array_list.Managed(parser.Diagnostic) = null,
    files: std.array_list.Managed([]const u8),
    aliases: std.StringHashMap([]const u8),
    remote: ?*remote.Resolver,
    root_dir: []const u8,

    /// The resolver remote includes go through: the caller's, else one made at the
    /// first remote include, over HTTPS and `basalt.sum` beside the root script.
    fn resolver(self: *Ctx) Error!*remote.Resolver {
        if (self.remote) |r| return r;
        const r = try self.arena.create(remote.Resolver);
        r.* = remote.Resolver.init(self.arena, self.root_dir, include_fetch.fetch(), false) catch |e| switch (e) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return self.fail(self.root_dir, 0, 0, "cannot read basalt.sum: {s}", .{@errorName(e)}),
        };
        self.remote = r;
        return r;
    }

    fn fail(self: *Ctx, label: []const u8, line: u32, col: u32, comptime fmt: []const u8, args: anytype) Error {
        const msg: []const u8 = std.fmt.allocPrint(self.arena, fmt, args) catch "include error";
        self.diag.* = .{ .parse = .{ .msg = msg, .line = line, .col = col }, .label = label };
        return error.ParseFailed;
    }
};

fn load(ctx: *Ctx, text: []const u8, label: []const u8, base: Base, file: u16, namespace: ?[]const u8) Error!ast.Program {
    var incs = std.array_list.Managed(Directive).init(ctx.arena);
    const body_at = try scanDirectives(ctx, text, label, &incs);
    const skipped: u32 = @intCast(std.mem.count(u8, text[0..body_at], "\n"));
    try rejectLateIncludes(ctx, text[body_at..], label, skipped + 1);

    var subs = std.array_list.Managed(ast.Program).init(ctx.arena);
    var seen = std.array_list.Managed(ast.Program).init(ctx.arena);
    var my_aliases = std.array_list.Managed([]const u8).init(ctx.arena);
    var same_ns = std.array_list.Managed(ast.Program).init(ctx.arena);
    for (incs.items) |d| {
        const at = try locate(ctx, base, d, label);
        const resolved = at.shown;
        const canon = at.canon;
        const canonical = at.canonical;
        for (ctx.stack.items, 0..) |f, i| {
            if (!std.mem.eql(u8, f.canon, canon)) continue;
            return ctx.fail(label, d.line, d.col, "include cycle: {s}", .{try cycleTrail(ctx, i, resolved)});
        }
        const sub_ns: ?[]const u8 = if (d.alias) |a| (if (namespace) |ns| try std.fmt.allocPrint(ctx.arena, "{s}.{s}", .{ ns, a }) else a) else namespace;
        if (d.alias) |a| {
            for (my_aliases.items) |m| if (std.ascii.eqlIgnoreCase(m, a))
                return ctx.fail(label, d.line, d.col, "alias `{s}` names two includes here; give each its own", .{a});
            try my_aliases.append(a);
            const gop = try ctx.aliases.getOrPut(try std.ascii.allocLowerString(ctx.arena, sub_ns.?));
            if (gop.found_existing and !std.mem.eql(u8, gop.value_ptr.*, canon))
                return ctx.fail(label, d.line, d.col, "`{s}` already names the library `{s}`, reached through another include; its functions would collide — choose another alias", .{ sub_ns.?, gop.value_ptr.* });
            gop.value_ptr.* = canon;
        }
        const memo_key = try std.fmt.allocPrint(ctx.arena, "{s}\x00{s}", .{ canon, sub_ns orelse "" });
        if (canonical) if (ctx.memo.get(memo_key)) |cached| {
            try seen.append(cached);
            if (d.alias == null) try same_ns.append(cached);
            continue;
        };
        const src = at.text orelse std.fs.cwd().readFileAlloc(ctx.arena, resolved, max_file_bytes) catch |e| {
            if (e == error.OutOfMemory) return error.OutOfMemory;
            return ctx.fail(label, d.line, d.col, "cannot read include `{s}`: {s}", .{ resolved, @errorName(e) });
        };
        if (ctx.depth + 1 > max_depth)
            return ctx.fail(label, d.line, d.col, "include nesting deeper than {d} files at `{s}`", .{ max_depth, resolved });

        try ctx.stack.append(.{ .canon = canon, .shown = resolved });
        ctx.depth += 1;
        const id = std.math.cast(u16, ctx.files.items.len) orelse
            return ctx.fail(label, d.line, d.col, "more than {d} @include files", .{std.math.maxInt(u16)});
        try ctx.files.append(resolved);
        const sub = try load(ctx, src, resolved, at.base, id, sub_ns);
        ctx.depth -= 1;
        _ = ctx.stack.pop();
        if (d.alias) |a| for (stmtsOf(sub)) |st| {
            if (st == .func) continue;
            const pos = ast.stmtPos(st) orelse ast.Pos{ .line = 0, .col = 0 };
            return ctx.fail(resolved, pos.line, pos.col, "`{s}` is included AS {s}, a library, so it may only declare functions; this {s} would run — include it without AS to run its statements", .{ resolved, a, stmtWord(st) });
        };
        if (canonical) try ctx.memo.put(memo_key, sub);
        try subs.append(sub);
        try seen.append(sub);
        if (d.alias == null) try same_ns.append(sub);

        const n = stmtsOf(sub).len;
        if (n > ctx.stmt_budget)
            return ctx.fail(label, d.line, d.col, "@include tree expands to more than {d} statements at `{s}`", .{ max_total_stmts, resolved });
        ctx.stmt_budget -= n;
    }

    const rest = try blankPrefix(ctx, text, body_at, incs.items.len);
    const has_body = incs.items.len == 0 or std.mem.trim(u8, rest, " \t\r\n").len > 0;
    var main: ?ast.Program = null;
    if (has_body) {
        var known = std.array_list.Managed(ast.Connection).init(ctx.arena);
        var known_fns = std.array_list.Managed(ast.FnDecl).init(ctx.arena);
        for (seen.items) |sp| for (stmtsOf(sp)) |st| {
            if (st == .connection) try known.append(st.connection);
            if (st == .func and st.func.body == .table) try known_fns.append(st.func);
        };
        var own = std.array_list.Managed([]const u8).init(ctx.arena);
        if (namespace) |ns| for (same_ns.items) |sp| for (stmtsOf(sp)) |st| if (st == .func) {
            const n = st.func.name;
            if (n.len > ns.len + 1 and std.ascii.eqlIgnoreCase(n[0..ns.len], ns) and n[ns.len] == '.')
                try own.append(n[ns.len + 1 ..]);
        };
        var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        main = parser.parseSourceOpts(ctx.arena, rest, &pdiag, .{
            .known_conns = known.items,
            .known_fns = known_fns.items,
            .known_tables = ctx.known_tables,
            .errors = if (ctx.depth == 0) ctx.errors else null,
            .file = file,
            .aliases = my_aliases.items,
            .namespace = namespace,
            .own = own.items,
        }) catch |e| switch (e) {
            error.OutOfMemory => return e,
            error.ParseFailed => {
                ctx.diag.* = .{ .parse = pdiag, .label = label };
                return error.ParseFailed;
            },
        };
    }
    if (main) |m| for (stmtsOf(m)) |st| if (st == .connection) for (my_aliases.items) |a| if (std.ascii.eqlIgnoreCase(a, st.connection.name)) {
        const pos = st.connection.pos;
        return ctx.fail(label, pos.line, pos.col, "connection `{s}` has the name of an include's alias; rename one, as `{s}.x(...)` would be ambiguous", .{ a, a });
    };
    if (incs.items.len == 0) return main.?;

    var kind: ?ast.KindDecl = if (main) |m| leadKind(m) else null;
    var out = std.array_list.Managed(ast.Stmt).init(ctx.arena);
    for (subs.items) |sp| {
        if (kind == null) kind = leadKind(sp);
        try out.appendSlice(stmtsOf(sp));
    }
    if (main) |m| try out.appendSlice(stmtsOf(m));
    if (kind) |k| try out.insert(0, .{ .kind = k });
    return .{ .stmts = try out.toOwnedSlice(), .explain = if (main) |m| m.explain else .none };
}

const Located = struct { shown: []const u8, canon: []const u8, canonical: bool, base: Base, text: ?[]const u8 };

/// Where a directive's path leads: a remote file (written as one, or relative inside
/// a remote file), fetched and verified now; else a local one, read by the caller.
fn locate(ctx: *Ctx, base: Base, d: Directive, label: []const u8) Error!Located {
    const spec: ?remote.Spec = if (remote.parse(d.path)) |sp| sp else switch (base) {
        .remote => |b| b.relative(ctx.arena, d.path) catch |e| return ctx.fail(label, d.line, d.col, "{s}", .{switch (e) {
            error.RemoteAbsoluteInclude => "a remote file cannot include a local path",
            error.RemoteEscapesRepo => "a remote file's include cannot climb out of its repository",
            else => return error.OutOfMemory,
        }}),
        .dir => null,
    };
    if (spec) |sp| {
        const r = try ctx.resolver();
        const text = r.get(sp) catch |e| switch (e) {
            error.RemoteInclude => return ctx.fail(label, d.line, d.col, "{s}", .{r.msg}),
            else => return error.OutOfMemory,
        };
        const key = try sp.key(ctx.arena);
        return .{ .shown = key, .canon = key, .canonical = true, .base = .{ .remote = sp }, .text = text };
    }
    if (remote.looksRemote(d.path))
        return ctx.fail(label, d.line, d.col, "remote include `{s}` names no version; pin one, as `{s}@v1.2.0` (a tag, branch or commit)", .{ d.path, d.path });
    const resolved = try resolvePath(ctx, base.dir, d.path);
    var canonical = true;
    const canon: []const u8 = std.fs.cwd().realpathAlloc(ctx.arena, resolved) catch blk: {
        canonical = false;
        break :blk resolved;
    };
    return .{ .shown = resolved, .canon = canon, .canonical = canonical, .base = .{ .dir = std.fs.path.dirname(resolved) orelse "." }, .text = null };
}

fn stmtWord(st: ast.Stmt) []const u8 {
    return switch (st) {
        .kind => "kind declaration",
        .param => "PARAM",
        .connection => "CREATE CONNECTION",
        .binding, .let_const => "LET",
        .output => "LOAD INTO or SELECT",
        .for_each => "FOR EACH",
        .match => "CASE",
        .func => "CREATE FUNCTION",
        .print => "PRINT",
        .call => "CALL",
        .throw => "THROW",
        .explain => "EXPLAIN",
    };
}

fn leadKind(p: ast.Program) ?ast.KindDecl {
    if (p.stmts.len == 0 or p.stmts[0] != .kind) return null;
    return p.stmts[0].kind;
}

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

/// Collects the leading `@include` directives and returns the offset where the
/// rest of the script starts; the first line that is not blank, `--` or a directive ends the scan.
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
            const pd = try parseDirective(ctx, trimmed, label, line, col);
            try out.append(.{ .path = pd.path, .alias = pd.alias, .line = line, .col = col });
        }
        off = if (nl == text.len) text.len else nl + 1;
        line += 1;
    }
    return off;
}

fn parseDirective(ctx: *Ctx, s: []const u8, label: []const u8, line: u32, col: u32) Error!struct { path: []const u8, alias: ?[]const u8 } {
    const syntax = "expected `@include 'path.sql';` or `@include 'path.sql' AS name;`";
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
    var alias: ?[]const u8 = null;
    if (tail.len >= 3 and std.ascii.eqlIgnoreCase(tail[0..2], "as") and (tail[2] == ' ' or tail[2] == '\t')) {
        tail = std.mem.trimLeft(u8, tail[2..], " \t");
        var n: usize = 0;
        while (n < tail.len and (std.ascii.isAlphanumeric(tail[n]) or tail[n] == '_')) n += 1;
        if (n == 0 or std.ascii.isDigit(tail[0]))
            return ctx.fail(label, line, col, "`AS` takes a name for the library, like `AS stats`", .{});
        alias = tail[0..n];
        tail = std.mem.trimLeft(u8, tail[n..], " \t");
    }
    if (tail.len == 0 or tail[0] != ';') return ctx.fail(label, line, col, "missing `;` after @include", .{});
    tail = std.mem.trimLeft(u8, tail[1..], " \t");
    if (tail.len != 0 and !std.mem.startsWith(u8, tail, "--"))
        return ctx.fail(label, line, col + @as(u32, @intCast(s.len - tail.len)), "unexpected text after @include — one file per directive", .{});
    return .{ .path = path, .alias = alias };
}

/// Line-oriented: a literal `@include` at the start of a line inside a `$$…$$`
/// block would be misread, which no real script does.
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

    try testing.expectEqual(@as(usize, 3), prog.stmts.len);
    try testing.expect(prog.stmts[0] == .kind);
    try testing.expectEqualStrings("dbl", prog.stmts[1].func.name);
    try testing.expect(prog.stmts[2] == .output);
}

test "@include: an included PARAM may sit beside an aggregate, as a local one may" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try writeFile(&tmp, "lib/params.sql", "PARAM tag STRING DEFAULT 'x';\n");

    var diag: Diag = .{};
    const included = try loadProgram(a,
        \\@include 'lib/params.sql';
        \\SELECT $tag AS t, g, COUNT(*) AS n FROM 'in.csv' GROUP BY g;
    , "main.sql", base, &diag);
    const local = try loadProgram(a,
        \\PARAM tag STRING DEFAULT 'x';
        \\SELECT $tag AS t, g, COUNT(*) AS n FROM 'in.csv' GROUP BY g;
    , "main.sql", base, &diag);
    for ([_]ast.Program{ included, local }) |prog| {
        try testing.expect(prog.stmts[1] == .param);
        try testing.expectEqualStrings("tag", prog.stmts[1].param.name);
        try testing.expect(prog.stmts[2] == .output);
        const stages = prog.stmts[2].output.stages;
        try testing.expect(stages[1].node == .aggregate);
        const agg = stages[1].node.aggregate;
        try testing.expectEqual(@as(usize, 1), agg.by.len);
        try testing.expectEqualStrings("g", agg.by[0].last());
        try testing.expectEqual(@as(usize, 1), agg.aggs.len);
        try testing.expectEqual(ast.AggFunc.count, agg.aggs[0].func);
        try testing.expectEqualStrings("n", agg.aggs[0].name);
        try testing.expect(stages[2].node == .select);
        const items = stages[2].node.select;
        try testing.expectEqual(@as(usize, 3), items.len);
        try testing.expectEqualStrings("t", items[0].computed.name);
        const tag = items[0].computed.expr;
        try testing.expect(tag.* == .field);
        try testing.expect(tag.field.dollar);
        try testing.expectEqualStrings("tag", tag.field.last());
    }
}

test "@include: a table function declared in a library is called in the includer" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try writeFile(&tmp, "lib/tables.sql", "CREATE FUNCTION paid(minx INT) RETURNS TABLE AS SELECT id FROM 'o.csv' WHERE x >= $minx;\n");

    var diag: Diag = .{};
    const prog = try loadProgram(a,
        \\@include 'lib/tables.sql';
        \\SELECT id FROM paid(8);
    , "main.sql", base, &diag);
    try testing.expect(prog.stmts[1].func.body == .table);
    try testing.expect(prog.stmts[2] == .binding);
    try testing.expect(prog.stmts[3] == .output);
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
    try testing.expect(std.mem.endsWith(u8, diag.label, "b.sql"));
}

test "a diamond @include DAG is parsed and spliced once per file, not once per path" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try writeFile(&tmp, "c.sql", "CREATE FUNCTION inc(x) AS x + 1;\n");
    try writeFile(&tmp, "a.sql", "@include 'c.sql';\n");
    try writeFile(&tmp, "b.sql", "@include 'c.sql';\n");

    var diag: Diag = .{};
    const prog = try loadProgram(a,
        \\@include 'a.sql';
        \\@include 'b.sql';
        \\SELECT inc(id) AS y FROM 'in.csv';
    , "main.sql", base, &diag);

    try testing.expectEqual(@as(usize, 3), prog.stmts.len);
    try testing.expect(prog.stmts[0] == .kind);
    try testing.expectEqualStrings("inc", prog.stmts[1].func.name);
    try testing.expect(prog.stmts[2] == .output);
}

test "a fan-out @include tree is spliced once per file, so it neither hangs nor blows the budget" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");

    const levels = 12;
    for (0..levels) |i| {
        var body = std.array_list.Managed(u8).init(a);
        for (0..4) |_| try body.writer().print("@include 'b{d}.sql';\n", .{i + 1});
        try writeFile(&tmp, try std.fmt.allocPrint(a, "b{d}.sql", .{i}), body.items);
    }
    try writeFile(&tmp, "b12.sql", "SELECT 1 AS x FROM 'in.csv';\n");

    var diag: Diag = .{};
    const prog = try loadProgram(a, "@include 'b0.sql';\n", "main.sql", base, &diag);
    try testing.expectEqual(@as(usize, 2), prog.stmts.len);
    try testing.expect(prog.stmts[1] == .output);
}

test "@include: a library sees a connection its own @include declares, though another library spliced that include first" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try writeFile(&tmp, "lib/stats.sql", "CREATE CONNECTION pg TYPE postgres OPTIONS (host = 'h', database = 'd');\nCREATE FUNCTION sd(x) AS x * 2;\n");
    try writeFile(&tmp, "lib/outliers.sql", "@include 'stats.sql';\nCREATE FUNCTION outl(x) AS sd(x) + 1;\n");
    try writeFile(&tmp, "lib/dispersion.sql", "@include 'stats.sql';\nLET n = (SELECT COUNT(*) AS n FROM pg.public.t);\n");

    var diag: Diag = .{};
    const prog = try loadProgram(a,
        \\@include 'lib/outliers.sql';
        \\@include 'lib/dispersion.sql';
        \\SELECT outl(1) AS a;
    , "main.sql", base, &diag);
    var sds: usize = 0;
    var conns: usize = 0;
    for (prog.stmts) |st| {
        if (st == .func and std.mem.eql(u8, st.func.name, "sd")) sds += 1;
        if (st == .connection) conns += 1;
    }
    try testing.expectEqual(@as(usize, 1), sds);
    try testing.expectEqual(@as(usize, 1), conns);
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
    try testing.expectEqual(@as(u32, 19), d3.parse.col);
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

test "@include AS: a library's functions are namespaced, call each other, and do not leak" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeFile(&tmp, "lib/s.sql", "CREATE FUNCTION sq(x) AS x * x;\nCREATE FUNCTION hyp(a, b) AS sqrt(sq(a) + sq(b));\nCREATE FUNCTION nums(n INT) RETURNS TABLE AS SELECT range AS v, sq(range) AS v2 FROM RANGE($n);\n");
    try writeFile(&tmp, "lib/o.sql", "CREATE FUNCTION sq(x) AS x + 1;\n");
    const base = try tmp.dir.realpathAlloc(a, ".");
    var diag: Diag = .{};
    const prog = try loadProgram(a, "@include 'lib/s.sql' AS st;\n@include 'lib/o.sql' AS ot;\nSELECT st.hyp(3, 4) AS h, ot.sq(1) AS o FROM st.nums(2) n;\n", "main.sql", base, &diag);
    var names = std.array_list.Managed([]const u8).init(a);
    for (prog.stmts) |s| if (s == .func) try names.append(s.func.name);
    try testing.expectEqual(@as(usize, 4), names.items.len);
    for ([_][]const u8{ "st.sq", "st.hyp", "st.nums", "ot.sq" }, names.items) |w, g| try testing.expectEqualStrings(w, g);
    const hyp = prog.stmts[2].func.body.expr;
    try testing.expectEqualStrings("st.sq", hyp.call.args[0].binary.l.call.name);
    try testing.expectError(error.ParseFailed, loadProgram(a, "@include 'lib/s.sql' AS st;\nSELECT * FROM nums(2);\n", "main.sql", base, &diag));
}

test "@include AS: a library declares functions only; aliases are one per library and not a connection's name" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeFile(&tmp, "run.sql", "CREATE FUNCTION f(x) AS x;\nLOAD INTO 'o.csv' AS SELECT 1 AS a;\n");
    try writeFile(&tmp, "f.sql", "CREATE FUNCTION f(x) AS x;\n");
    try writeFile(&tmp, "g.sql", "CREATE FUNCTION g(x) AS x;\n");
    const base = try tmp.dir.realpathAlloc(a, ".");
    var diag: Diag = .{};
    try testing.expectError(error.ParseFailed, loadProgram(a, "@include 'run.sql' AS r;\nSELECT 1 AS a;\n", "m.sql", base, &diag));
    try testing.expect(std.mem.indexOf(u8, diag.parse.msg, "may only declare functions") != null);
    _ = try loadProgram(a, "@include 'run.sql';\nSELECT f(1) AS a;\n", "m.sql", base, &diag);
    try testing.expectError(error.ParseFailed, loadProgram(a, "@include 'f.sql' AS x;\n@include 'g.sql' AS x;\nSELECT 1 AS a;\n", "m.sql", base, &diag));
    try testing.expectError(error.ParseFailed, loadProgram(a, "@include 'f.sql' AS pg;\nCREATE CONNECTION pg TYPE postgres OPTIONS (host = 'h');\nSELECT 1 AS a;\n", "m.sql", base, &diag));
    try testing.expect(std.mem.indexOf(u8, diag.parse.msg, "alias") != null);
}

const FakeHost = struct {
    files: []const [2][]const u8,
    gets: usize = 0,

    fn get(ctx: *anyopaque, arena: std.mem.Allocator, url: []const u8, _: ?std.http.Header) anyerror![]const u8 {
        const self: *FakeHost = @ptrCast(@alignCast(ctx));
        self.gets += 1;
        for (self.files) |f| if (std.mem.eql(u8, f[0], url)) return arena.dupe(u8, f[1]);
        return error.HttpNotFound;
    }
};

test "remote @include: refused until pinned, pinned by check, verified, its own includes kept in its repo" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    var host = FakeHost{ .files = &.{
        .{ "https://raw.githubusercontent.com/acme/kit/v1/sql/lib.sql", "@include 'util.sql' AS u;\nCREATE FUNCTION twice(x) AS u.inc(u.inc(x));\n" },
        .{ "https://raw.githubusercontent.com/acme/kit/v1/sql/util.sql", "CREATE FUNCTION inc(x) AS x + 1;\n" },
        .{ "https://raw.githubusercontent.com/acme/kit/v1/sql/evil.sql", "@include '/etc/passwd';\n" },
        .{ "https://raw.githubusercontent.com/acme/kit/v1/sql/up.sql", "@include '../../x.sql';\n" },
    } };
    const fetch = remote.Fetch{ .ctx = &host, .get = FakeHost.get };
    const script = "@include 'github.com/acme/kit/sql/lib.sql@v1' AS kit;\nSELECT kit.twice(1) AS v;\n";
    var diag: Diag = .{};

    var unpinned = try remote.Resolver.init(a, base, fetch, false);
    unpinned.cache_dir = null;
    try testing.expectError(error.ParseFailed, loadProgramOpts(a, script, "m.sql", base, &diag, .{ .remote = &unpinned }));
    try testing.expect(std.mem.indexOf(u8, diag.parse.msg, "not pinned") != null);

    var pinning = try remote.Resolver.init(a, base, fetch, true);
    pinning.cache_dir = null;
    _ = try loadProgramOpts(a, script, "m.sql", base, &diag, .{ .remote = &pinning });
    try pinning.save();
    try testing.expectEqual(@as(usize, 2), pinning.added.items.len);

    var pinned = try remote.Resolver.init(a, base, fetch, false);
    pinned.cache_dir = null;
    const prog = try loadProgramOpts(a, script, "m.sql", base, &diag, .{ .remote = &pinned });
    try testing.expectEqualStrings("kit.twice", prog.stmts[2].func.name);

    host.files = &.{.{ "https://raw.githubusercontent.com/acme/kit/v1/sql/lib.sql", "CREATE FUNCTION twice(x) AS x;\n" }};
    var moved = try remote.Resolver.init(a, base, fetch, false);
    moved.cache_dir = null;
    try testing.expectError(error.ParseFailed, loadProgramOpts(a, script, "m.sql", base, &diag, .{ .remote = &moved }));
    try testing.expect(std.mem.indexOf(u8, diag.parse.msg, "no longer matches its pin") != null);

    for ([_][]const u8{ "evil.sql", "up.sql" }) |f| {
        host.files = &.{
            .{ "https://raw.githubusercontent.com/acme/kit/v1/sql/evil.sql", "@include '/etc/passwd';\n" },
            .{ "https://raw.githubusercontent.com/acme/kit/v1/sql/up.sql", "@include '../../x.sql';\n" },
        };
        var r = try remote.Resolver.init(a, base, fetch, true);
        r.cache_dir = null;
        const s = try std.fmt.allocPrint(a, "@include 'github.com/acme/kit/sql/{s}@v1';\nSELECT 1 AS a;\n", .{f});
        try testing.expectError(error.ParseFailed, loadProgramOpts(a, s, "m.sql", base, &diag, .{ .remote = &r }));
    }
    try testing.expectError(error.ParseFailed, loadProgram(a, "@include 'github.com/acme/kit/sql/lib.sql';\nSELECT 1 AS a;\n", "m.sql", base, &diag));
    try testing.expect(std.mem.indexOf(u8, diag.parse.msg, "names no version") != null);
}

test "@include AS: an alias is private to its file, so two libraries may both use one, and nothing leaks" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeFile(&tmp, "a/lib.sql", "@include 'util.sql' AS u;\n@include 'help.sql';\nCREATE FUNCTION f(x) AS u.g(h(x));\n");
    try writeFile(&tmp, "a/util.sql", "CREATE FUNCTION g(x) AS x + 1;\n");
    try writeFile(&tmp, "a/help.sql", "CREATE FUNCTION h(x) AS x * 10;\n");
    try writeFile(&tmp, "b/lib.sql", "@include 'util.sql' AS u;\nCREATE FUNCTION f(x) AS u.g(x);\n");
    try writeFile(&tmp, "b/util.sql", "CREATE FUNCTION g(x) AS x - 1;\n");
    const base = try tmp.dir.realpathAlloc(a, ".");
    var diag: Diag = .{};
    const prog = try loadProgram(a, "@include 'a/lib.sql' AS a;\n@include 'b/lib.sql' AS b;\nSELECT a.f(1) AS x, b.f(1) AS y;\n", "m.sql", base, &diag);
    var names = std.array_list.Managed([]const u8).init(a);
    for (prog.stmts) |s| if (s == .func) try names.append(s.func.name);
    for ([_][]const u8{ "a.u.g", "a.h", "a.f", "b.u.g", "b.f" }, names.items) |w, g| try testing.expectEqualStrings(w, g);
    try testing.expectEqual(@as(usize, 5), names.items.len);
    try testing.expectEqualStrings("a.u.g", prog.stmts[3].func.body.expr.call.name);
    try testing.expectEqualStrings("a.h", prog.stmts[3].func.body.expr.call.args[0].call.name);
    try testing.expectError(error.ParseFailed, loadProgram(a, "@include 'a/lib.sql' AS a;\nSELECT u.g(1) AS x;\n", "m.sql", base, &diag));
}
