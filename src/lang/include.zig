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
const lexer = @import("sql_lexer.zig");
const scope = @import("scope.zig");
const remote = @import("remote.zig");
const msgfmt = @import("../base/msgfmt.zig");

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
/// or the fetch one is made over at the first remote include. With neither, remote
/// includes are refused: the network is the caller's to hand in.
pub const Options = struct {
    parse: parser.Options = .{},
    remote: ?*remote.Resolver = null,
    fetch: ?remote.Fetch = null,
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
        .memo = std.StringHashMap(Module).init(arena),
        .known_tables = opts.parse.known_tables,
        .is_builtin = opts.parse.is_builtin,
        .errors = opts.parse.errors,
        .files = std.array_list.Managed([]const u8).init(arena),
        .scopes = std.array_list.Managed(scope.Scope).init(arena),
        .remote = opts.remote,
        .fetch = opts.fetch,
        .root_dir = base_dir,
    };
    if (std.fs.cwd().realpathAlloc(arena, label)) |canon| {
        try ctx.stack.append(.{ .canon = canon, .shown = label });
    } else |_| {}
    try ctx.files.append(label);
    try ctx.scopes.append(.{ .path = label });
    var prog = (try load(&ctx, text, label, .{ .dir = base_dir }, 0)).program;
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
    memo: std.StringHashMap(Module),
    depth: u32 = 0,
    stmt_budget: usize = max_total_stmts,
    known_tables: []const []const u8 = &.{},
    is_builtin: ?*const fn ([]const u8) bool = null,
    errors: ?*std.array_list.Managed(parser.Diagnostic) = null,
    files: std.array_list.Managed([]const u8),
    scopes: std.array_list.Managed(scope.Scope),
    remote: ?*remote.Resolver,
    fetch: ?remote.Fetch,
    root_dir: []const u8,

    /// The resolver remote includes go through: the caller's, else one made at the
    /// first remote include, over the caller's fetch and `basalt.sum` beside the root
    /// script.
    fn resolver(self: *Ctx) Error!*remote.Resolver {
        if (self.remote) |r| return r;
        const r = try self.arena.create(remote.Resolver);
        r.* = remote.Resolver.init(self.arena, self.root_dir, self.fetch, false) catch |e| switch (e) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return self.fail(self.root_dir, 0, 0, "cannot read basalt.sum: {s}", .{@errorName(e)}),
        };
        self.remote = r;
        return r;
    }

    fn fail(self: *Ctx, label: []const u8, line: u32, col: u32, comptime fmt: []const u8, args: anytype) Error {
        @branchHint(.cold);
        const msg: []const u8 = msgfmt.allocPrint(self.arena, fmt, args) catch "include error";
        self.diag.* = .{ .parse = .{ .msg = msg, .line = line, .col = col }, .label = label };
        return error.ParseFailed;
    }
};

/// A file loaded once: its program, with what its own includes spliced in, and the
/// names it declares (`scope.prescan`).
const Module = struct { program: ast.Program, names: []const scope.Name, file: u16 = 0 };

/// An export a file brings in by including another without `AS`, and the
/// directive that brought it, for a clash's message.
const Import = struct { h: scope.Hidden, d: Directive };

/// Refuses a file included `AS alias` that would run something, a query LET among
/// it (including a library never runs anything), or that exports nothing.
fn checkLibrary(ctx: *Ctx, m: Module, alias: []const u8, resolved: []const u8, label: []const u8, d: Directive) Error!void {
    for (stmtsOf(m.program)) |st| {
        const ok = switch (st) {
            .func, .connection, .param => true,
            .let_const => |l| l.query == null,
            else => false,
        };
        if (ok) continue;
        const pos = ast.stmtPos(st) orelse ast.Pos{ .line = 0, .col = 0 };
        const shown = if (pos.file < ctx.files.items.len) ctx.files.items[pos.file] else resolved;
        return ctx.fail(shown, pos.line, pos.col, "`{s}` is included AS {s}, a library, so it may only declare functions, connections, PARAMs and LETs of a value; this {s} would run — include it without AS to run its statements", .{ resolved, alias, stmtWord(st) });
    }
    for (m.names) |n| if (n.exported) return;
    return ctx.fail(label, d.line, d.col, "`{s}` is included AS {s}, a library, but exports nothing: declare what it shares with EXPORT (FUNCTION, CONNECTION, PARAM or LET)", .{ resolved, alias });
}

fn sameName(kind: scope.Kind, a: []const u8, b: []const u8) bool {
    return if (kind == .func) std.ascii.eqlIgnoreCase(a, b) else std.mem.eql(u8, a, b);
}

/// Loads file `file` (`text`, shown as `label`): its includes first, each a module
/// loaded once however many files include it, then its own body, parsed in its
/// scope — its own names, the exports of the files it includes without `AS`, its
/// libraries — when the program has more than one file. The root (file 0) without
/// includes is parsed as it always was.
fn load(ctx: *Ctx, text: []const u8, label: []const u8, base: Base, file: u16) Error!Module {
    var incs = std.array_list.Managed(Directive).init(ctx.arena);
    const body_at = try scanDirectives(ctx, text, label, &incs);
    const skipped: u32 = @intCast(std.mem.count(u8, text[0..body_at], "\n"));
    try rejectLateIncludes(ctx, text[body_at..], label, skipped + 1);

    var subs = std.array_list.Managed(ast.Program).init(ctx.arena);
    var seen = std.array_list.Managed(Module).init(ctx.arena);
    var my_aliases = std.array_list.Managed([]const u8).init(ctx.arena);
    var libs = std.array_list.Managed(scope.Lib).init(ctx.arena);
    var imports = std.array_list.Managed(Import).init(ctx.arena);
    var hidden = std.array_list.Managed(scope.Hidden).init(ctx.arena);
    for (incs.items) |d| {
        const at = try locate(ctx, base, d, label);
        const resolved = at.shown;
        for (ctx.stack.items, 0..) |f, i| {
            if (!std.mem.eql(u8, f.canon, at.canon)) continue;
            return ctx.fail(label, d.line, d.col, "include cycle: {s}", .{try cycleTrail(ctx, i, resolved)});
        }
        if (d.alias) |a| {
            for (my_aliases.items) |m| if (std.ascii.eqlIgnoreCase(m, a))
                return ctx.fail(label, d.line, d.col, "alias `{s}` names two includes here; give each its own", .{a});
            try my_aliases.append(a);
        }
        const m: Module = if (at.canonical) (ctx.memo.get(at.canon) orelse try loadSub(ctx, at, d, label, &subs)) else try loadSub(ctx, at, d, label, &subs);
        try seen.append(m);
        if (d.alias) |a| {
            try checkLibrary(ctx, m, a, resolved, label, d);
            try libs.append(.{ .alias = a, .path = resolved, .names = m.names });
        } else {
            for (m.names) |n| {
                if (n.exported) try imports.append(.{ .h = .{ .name = n, .path = resolved }, .d = d }) else try hidden.append(.{ .name = n, .path = resolved });
            }
            if (m.file < ctx.scopes.items.len) for (ctx.scopes.items[m.file].imports) |deep|
                try hidden.append(.{ .name = deep.name, .path = deep.path, .via = resolved });
        }
    }

    const rest = try blankPrefix(ctx, text, body_at, incs.items.len);
    const modular = file > 0 or incs.items.len > 0;
    const scanned = try scope.prescan(ctx.arena, lexer.tokenize(ctx.arena, rest) catch return error.OutOfMemory, file);
    if (modular) {
        try checkClashes(ctx, label, scanned.names, imports.items);
        var flat = std.array_list.Managed(scope.Hidden).init(ctx.arena);
        for (imports.items) |im| try flat.append(im.h);
        ctx.scopes.items[file] = .{ .path = label, .own = scanned.names, .imports = flat.items, .libs = libs.items, .hidden = hidden.items, .locals = scanned.locals };
    }
    for (scanned.names) |n| if (n.kind == .conn) for (my_aliases.items) |a| if (std.ascii.eqlIgnoreCase(a, n.name))
        return ctx.fail(label, 1, 1, "connection `{s}` has the name of an include's alias; rename one, as `{s}.x` would be ambiguous", .{ a, a });

    const has_body = incs.items.len == 0 or std.mem.trim(u8, rest, " \t\r\n").len > 0;
    var main: ?ast.Program = null;
    if (has_body) {
        var known = std.array_list.Managed(ast.Connection).init(ctx.arena);
        var known_fns = std.array_list.Managed(ast.FnDecl).init(ctx.arena);
        var known_names = std.array_list.Managed([]const u8).init(ctx.arena);
        for (seen.items) |sm| for (stmtsOf(sm.program)) |st| {
            if (st == .connection) try known.append(st.connection);
            if (st == .func and st.func.body == .table) try known_fns.append(st.func);
            if (st == .func) try known_names.append(st.func.name);
        };
        var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        main = parser.parseSourceOpts(ctx.arena, rest, &pdiag, .{
            .known_conns = known.items,
            .known_fns = known_fns.items,
            .known_tables = ctx.known_tables,
            .is_builtin = ctx.is_builtin,
            .known_names = known_names.items,
            .errors = if (ctx.depth == 0) ctx.errors else null,
            .file = file,
            .scopes = if (modular) ctx.scopes.items else &.{},
        }) catch |e| switch (e) {
            error.OutOfMemory => return e,
            error.ParseFailed => {
                const shown = if (pdiag.file != 0 and pdiag.file != file and pdiag.file < ctx.files.items.len) ctx.files.items[pdiag.file] else label;
                ctx.diag.* = .{ .parse = pdiag, .label = shown };
                return error.ParseFailed;
            },
        };
    }
    if (incs.items.len == 0) {
        var only = main.?;
        if (file == 0) only.modular = false;
        return .{ .program = only, .names = scanned.names, .file = file };
    }

    var kind: ?ast.KindDecl = if (main) |m| leadKind(m) else null;
    var out = std.array_list.Managed(ast.Stmt).init(ctx.arena);
    for (subs.items) |sp| {
        if (kind == null) kind = leadKind(sp);
        try out.appendSlice(stmtsOf(sp));
    }
    if (main) |m| try out.appendSlice(stmtsOf(m));
    if (kind) |k| try out.insert(0, .{ .kind = k });
    var notes = std.array_list.Managed(ast.Note).init(ctx.arena);
    for (subs.items) |sp| try notes.appendSlice(sp.notes);
    if (main) |m| try notes.appendSlice(m.notes);
    var prog = ast.Program{ .stmts = try out.toOwnedSlice(), .explain = if (main) |m| m.explain else .none, .notes = notes.items };
    if (file == 0) {
        prog.modular = true;
        prog.inputs = try inputsOf(ctx, scanned.names, imports.items, libs.items);
        for (prog.stmts) |st| if (st == .param and st.param.default == null) {
            const p = st.param;
            for (prog.inputs) |in| {
                if (std.mem.eql(u8, in.internal, p.name)) break;
            } else {
                const shown = if (p.pos.file < ctx.files.items.len) ctx.files.items[p.pos.file] else label;
                return ctx.fail(shown, p.pos.line, p.pos.col, "PARAM `{s}` has no DEFAULT and the script cannot bind it: give it a DEFAULT, or declare it `EXPORT PARAM` and include its file from the script", .{scope.shown(p.name)});
            }
        };
    }
    return .{ .program = prog, .names = scanned.names, .file = file };
}

/// Reads and loads the file a directive names, the first time the program meets
/// it, under a file number of its own; its program joins `subs`, to be spliced in.
fn loadSub(ctx: *Ctx, at: Located, d: Directive, label: []const u8, subs: *std.array_list.Managed(ast.Program)) Error!Module {
    const resolved = at.shown;
    const src = at.text orelse std.fs.cwd().readFileAlloc(ctx.arena, resolved, max_file_bytes) catch |e| {
        if (e == error.OutOfMemory) return error.OutOfMemory;
        return ctx.fail(label, d.line, d.col, "cannot read include `{s}`: {s}", .{ resolved, @errorName(e) });
    };
    if (ctx.depth + 1 > max_depth)
        return ctx.fail(label, d.line, d.col, "include nesting deeper than {d} files at `{s}`", .{ max_depth, resolved });
    try ctx.stack.append(.{ .canon = at.canon, .shown = resolved });
    ctx.depth += 1;
    const id = std.math.cast(u16, ctx.files.items.len) orelse
        return ctx.fail(label, d.line, d.col, "more than {d} @include files", .{std.math.maxInt(u16)});
    try ctx.files.append(resolved);
    try ctx.scopes.append(.{ .path = resolved });
    const sub = try load(ctx, src, resolved, at.base, id);
    ctx.depth -= 1;
    _ = ctx.stack.pop();
    if (at.canonical) try ctx.memo.put(at.canon, sub);
    try subs.append(sub.program);
    const n = stmtsOf(sub.program).len;
    if (n > ctx.stmt_budget)
        return ctx.fail(label, d.line, d.col, "@include tree expands to more than {d} statements at `{s}`", .{ max_total_stmts, resolved });
    ctx.stmt_budget -= n;
    return sub;
}

/// Refuses a name a file both declares and brings in from an include, or brings in
/// from two: which one a reference meant could not be told. An alias keeps them
/// apart.
fn checkClashes(ctx: *Ctx, label: []const u8, own: []const scope.Name, imports: []const Import) Error!void {
    for (imports, 0..) |im, i| {
        const n = im.h.name;
        for (own) |o| if (o.kind == n.kind and sameName(n.kind, o.name, n.name))
            return ctx.fail(label, im.d.line, im.d.col, "`{s}` is declared here and exported by `{s}`, included without AS; include it AS an alias to tell them apart, or rename one", .{ n.name, im.h.path });
        for (imports[0..i]) |prev| if (prev.h.name.kind == n.kind and sameName(n.kind, prev.h.name.name, n.name) and !std.mem.eql(u8, prev.h.name.internal, n.internal))
            return ctx.fail(label, im.d.line, im.d.col, "`{s}` is exported both by `{s}` and by `{s}`, each included without AS; include one of them AS an alias", .{ n.name, prev.h.path, im.h.path });
    }
}

/// The PARAMs a run binds (`-p`, a kernel's params): the root's own, those the files
/// it includes without `AS` export, by their names, and its libraries' exported
/// ones as `alias.name`. Any other PARAM keeps its DEFAULT.
fn inputsOf(ctx: *Ctx, own: []const scope.Name, imports: []const Import, libs: []const scope.Lib) Error![]const ast.Input {
    var out = std.array_list.Managed(ast.Input).init(ctx.arena);
    for (own) |n| if (n.param) try out.append(.{ .external = n.name, .internal = n.internal });
    for (imports) |im| if (im.h.name.param) try out.append(.{ .external = im.h.name.name, .internal = im.h.name.internal });
    for (libs) |l| for (l.names) |n| if (n.param and n.exported)
        try out.append(.{ .external = try msgfmt.allocPrint(ctx.arena, "{s}.{s}", .{ l.alias, n.name }), .internal = n.internal });
    return out.items;
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

/// Whether a trimmed line belongs to a script's leading block: blank, a `--`
/// comment, or an `@include` directive.
fn inDirectives(trimmed: []const u8) bool {
    return trimmed.len == 0 or std.mem.startsWith(u8, trimmed, "--") or std.mem.startsWith(u8, trimmed, "@include");
}

/// The offset where `text`'s leading `@include` block ends, as `scanDirectives` cuts it.
pub fn directivesEnd(text: []const u8) usize {
    var off: usize = 0;
    while (off < text.len) {
        const nl = std.mem.indexOfScalarPos(u8, text, off, '\n') orelse text.len;
        if (!inDirectives(std.mem.trim(u8, text[off..nl], " \t\r"))) break;
        off = if (nl == text.len) text.len else nl + 1;
    }
    return off;
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
        if (!inDirectives(trimmed)) break;
        if (std.mem.startsWith(u8, trimmed, "@include")) {
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
    return switch (readDirective(s)) {
        .ok => |d| .{ .path = d.path, .alias = d.alias },
        .bad => |b| ctx.fail(label, line, col + @as(u32, @intCast(b.at)), "{s}", .{b.msg}),
    };
}

/// A directive's line read: its path and alias, or why it is not one and how far into
/// the line the fault lies.
pub const ReadDirective = union(enum) {
    ok: struct { path: []const u8, alias: ?[]const u8 },
    bad: struct { msg: []const u8, at: usize = 0 },
};

/// Reads one trimmed `@include 'path' [AS name];` line.
pub fn readDirective(s: []const u8) ReadDirective {
    const syntax = "expected `@include 'path.sql';` or `@include 'path.sql' AS name;`";
    var rest = s["@include".len..];
    if (rest.len == 0 or (rest[0] != ' ' and rest[0] != '\t' and rest[0] != '\''))
        return .{ .bad = .{ .msg = syntax } };
    rest = std.mem.trimLeft(u8, rest, " \t");
    if (rest.len == 0 or rest[0] != '\'') return .{ .bad = .{ .msg = syntax } };
    const close = std.mem.indexOfScalarPos(u8, rest, 1, '\'') orelse
        return .{ .bad = .{ .msg = "unterminated include path — " ++ syntax } };
    const path = rest[1..close];
    if (path.len == 0) return .{ .bad = .{ .msg = "empty include path" } };
    var tail = std.mem.trimLeft(u8, rest[close + 1 ..], " \t");
    var alias: ?[]const u8 = null;
    if (tail.len >= 3 and std.ascii.eqlIgnoreCase(tail[0..2], "as") and (tail[2] == ' ' or tail[2] == '\t')) {
        tail = std.mem.trimLeft(u8, tail[2..], " \t");
        var n: usize = 0;
        while (n < tail.len and (std.ascii.isAlphanumeric(tail[n]) or tail[n] == '_')) n += 1;
        if (n == 0 or std.ascii.isDigit(tail[0]))
            return .{ .bad = .{ .msg = "`AS` takes a name for the library, like `AS stats`" } };
        alias = tail[0..n];
        tail = std.mem.trimLeft(u8, tail[n..], " \t");
    }
    if (tail.len == 0 or tail[0] != ';') return .{ .bad = .{ .msg = "missing `;` after @include" } };
    tail = std.mem.trimLeft(u8, tail[1..], " \t");
    if (tail.len != 0 and !std.mem.startsWith(u8, tail, "--"))
        return .{ .bad = .{ .msg = "unexpected text after @include — one file per directive", .at = s.len - tail.len } };
    return .{ .ok = .{ .path = path, .alias = alias } };
}

/// A directive a script's leading block holds, for a reader that only looks.
pub const Written = struct { path: []const u8, alias: ?[]const u8, line: u32 };

/// The `@include` directives that lead `text`, as `scanDirectives` finds them; one
/// that does not read is passed over rather than refused.
pub fn writtenIncludes(arena: std.mem.Allocator, text: []const u8) ![]const Written {
    var out = std.array_list.Managed(Written).init(arena);
    var off: usize = 0;
    var line: u32 = 1;
    while (off < text.len) {
        const nl = std.mem.indexOfScalarPos(u8, text, off, '\n') orelse text.len;
        const trimmed = std.mem.trim(u8, text[off..nl], " \t\r");
        if (!inDirectives(trimmed)) break;
        if (std.mem.startsWith(u8, trimmed, "@include")) switch (readDirective(trimmed)) {
            .ok => |d| try out.append(.{ .path = d.path, .alias = d.alias, .line = line }),
            .bad => {},
        };
        off = if (nl == text.len) text.len else nl + 1;
        line += 1;
    }
    return out.items;
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
    try writeFile(&tmp, "lib/fns.sql", "EXPORT FUNCTION dbl(x) AS x * 2;\n");

    var diag: Diag = .{};
    const prog = try loadProgram(a,
        \\@include 'lib/fns.sql';
        \\
        \\SELECT dbl(id) AS y FROM 'in.csv';
    , "main.sql", base, &diag);

    try testing.expectEqual(@as(usize, 3), prog.stmts.len);
    try testing.expect(prog.stmts[0] == .kind);
    try testing.expectEqualStrings("__m1_dbl", prog.stmts[1].func.name);
    try testing.expect(prog.stmts[2] == .output);
    try testing.expect(prog.modular);
}

test "@include: a file's functions, connections, PARAMs and LETs are its own until it exports them" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try writeFile(&tmp, "lib.sql",
        \\CREATE FUNCTION f(x) AS x;
        \\CREATE CONNECTION pg TYPE postgres OPTIONS (host = 'h');
        \\PARAM p INT DEFAULT 1;
        \\LET v = 2;
        \\EXPORT FUNCTION g(x) AS f(x) + $v;
    );

    var diag: Diag = .{};
    const cases = [_][3][]const u8{
        .{ "SELECT f(1) AS a;", "f", "FUNCTION" },
        .{ "SELECT * FROM pg.public.t;", "pg", "CONNECTION" },
        .{ "SELECT $p AS a;", "p", "PARAM" },
        .{ "SELECT $v AS a;", "v", "LET" },
    };
    for (cases) |c| {
        const script = try msgfmt.allocPrint(a, "@include 'lib.sql';\n{s}\n", .{c[0]});
        try testing.expectError(error.ParseFailed, loadProgram(a, script, "main.sql", base, &diag));
        try testing.expectEqualStrings("main.sql", diag.label);
        try testing.expectEqual(@as(u32, 2), diag.parse.line);
        try testing.expect(std.mem.startsWith(u8, diag.parse.msg, try msgfmt.allocPrint(a, "`{s}` is declared in `", .{c[1]})));
        try testing.expect(std.mem.endsWith(u8, diag.parse.msg, try msgfmt.allocPrint(a, "lib.sql` without EXPORT, so only that file sees it; declare it `EXPORT {s}` there to use it here", .{c[2]})));
    }
    const prog = try loadProgram(a, "@include 'lib.sql';\nSELECT g(1) AS a;\n", "main.sql", base, &diag);
    const g = prog.stmts[5].func;
    try testing.expectEqualStrings("__m1_g", g.name);
    try testing.expectEqualStrings("__m1_f", g.body.expr.binary.l.call.name);
    try testing.expectEqualStrings("__m1_v", g.body.expr.binary.r.field.last());
}

test "@include: an included PARAM may sit beside an aggregate, as a local one may" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try writeFile(&tmp, "lib/params.sql", "EXPORT PARAM tag STRING DEFAULT 'x';\n");

    var diag: Diag = .{};
    const included = try loadProgram(a,
        \\@include 'lib/params.sql';
        \\SELECT $tag AS t, g, COUNT(*) AS n FROM 'in.csv' GROUP BY g;
    , "main.sql", base, &diag);
    const local = try loadProgram(a,
        \\PARAM tag STRING DEFAULT 'x';
        \\SELECT $tag AS t, g, COUNT(*) AS n FROM 'in.csv' GROUP BY g;
    , "main.sql", base, &diag);
    for ([_]ast.Program{ included, local }, [_][]const u8{ "__m1_tag", "tag" }) |prog, tag_name| {
        try testing.expect(prog.stmts[1] == .param);
        try testing.expectEqualStrings(tag_name, prog.stmts[1].param.name);
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
        try testing.expectEqualStrings(tag_name, tag.field.last());
    }
}

test "@include: a table function declared in a library is called in the includer" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try writeFile(&tmp, "lib/tables.sql", "EXPORT FUNCTION paid(minx INT) RETURNS TABLE AS SELECT id FROM 'o.csv' WHERE x >= $minx;\n");

    var diag: Diag = .{};
    const prog = try loadProgram(a,
        \\@include 'lib/tables.sql';
        \\SELECT id FROM paid(8);
    , "main.sql", base, &diag);
    try testing.expect(prog.stmts[1].func.body == .table);
    try testing.expect(prog.stmts[2] == .binding);
    try testing.expect(prog.stmts[3] == .output);
}

test "nested @include resolves relative to the including file, and its exports stop at that file" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try writeFile(&tmp, "sub/a.sql",
        \\@include 'b.sql';
        \\EXPORT FUNCTION dbl(x) AS inc(x) * 2;
    );
    try writeFile(&tmp, "sub/b.sql", "EXPORT FUNCTION inc(x) AS x + 1;\n");

    var diag: Diag = .{};
    const prog = try loadProgram(a,
        \\@include 'sub/a.sql';
        \\SELECT dbl(id) AS y FROM 'in.csv';
    , "main.sql", base, &diag);

    try testing.expectEqual(@as(usize, 4), prog.stmts.len);
    try testing.expect(prog.stmts[0] == .kind);
    try testing.expectEqualStrings("__m2_inc", prog.stmts[1].func.name);
    try testing.expectEqualStrings("__m1_dbl", prog.stmts[2].func.name);
    try testing.expectEqualStrings("__m2_inc", prog.stmts[2].func.body.expr.binary.l.call.name);
    try testing.expect(prog.stmts[3] == .output);

    try testing.expectError(error.ParseFailed, loadProgram(a,
        \\@include 'sub/a.sql';
        \\SELECT dbl(inc(id)) AS y FROM 'in.csv';
    , "main.sql", base, &diag));
    try testing.expect(std.mem.startsWith(u8, diag.parse.msg, "`inc` is exported by `"));
    try testing.expect(std.mem.indexOf(u8, diag.parse.msg, "sub/b.sql`, which this file includes only through `") != null);
    try testing.expect(std.mem.endsWith(u8, diag.parse.msg, "sub/b.sql` here"));
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
    try writeFile(&tmp, "c.sql", "EXPORT FUNCTION inc(x) AS x + 1;\n");
    try writeFile(&tmp, "a.sql", "@include 'c.sql';\nEXPORT FUNCTION ia(x) AS inc(x);\n");
    try writeFile(&tmp, "b.sql", "@include 'c.sql';\nEXPORT FUNCTION ib(x) AS inc(x);\n");

    var diag: Diag = .{};
    const prog = try loadProgram(a,
        \\@include 'a.sql';
        \\@include 'b.sql';
        \\SELECT ia(id) AS y, ib(id) AS z FROM 'in.csv';
    , "main.sql", base, &diag);

    try testing.expectEqual(@as(usize, 5), prog.stmts.len);
    try testing.expect(prog.stmts[0] == .kind);
    for ([_][]const u8{ "__m2_inc", "__m1_ia", "__m3_ib" }, prog.stmts[1..4]) |w, st| try testing.expectEqualStrings(w, st.func.name);
    try testing.expectEqualStrings("__m2_inc", prog.stmts[3].func.body.expr.call.name);
    try testing.expect(prog.stmts[4] == .output);
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
        try writeFile(&tmp, try msgfmt.allocPrint(a, "b{d}.sql", .{i}), body.items);
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
    try writeFile(&tmp, "lib/stats.sql", "EXPORT CONNECTION pg TYPE postgres OPTIONS (host = 'h', database = 'd');\nEXPORT FUNCTION sd(x) AS x * 2;\n");
    try writeFile(&tmp, "lib/outliers.sql", "@include 'stats.sql';\nEXPORT FUNCTION outl(x) AS sd(x) + 1;\n");
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
        if (st == .func and std.mem.eql(u8, st.func.name, "__m2_sd")) sds += 1;
        if (st == .connection) {
            conns += 1;
            try testing.expectEqualStrings("__m2_pg", st.connection.name);
        }
    }
    try testing.expectEqual(@as(usize, 1), sds);
    try testing.expectEqual(@as(usize, 1), conns);
}

test "@include: two files may each keep a private name, and a clash with an import is refused at the @include" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try writeFile(&tmp, "x.sql", "CREATE CONNECTION pg TYPE postgres OPTIONS (host = 'hx');\nLET v = 1;\nEXPORT FUNCTION fx() AS $v;\nEXPORT LET n = 10;\n");
    try writeFile(&tmp, "y.sql", "CREATE CONNECTION pg TYPE postgres OPTIONS (host = 'hy');\nLET v = 2;\nEXPORT FUNCTION fy() AS $v;\nEXPORT LET n = 20;\n");
    try writeFile(&tmp, "z.sql", "EXPORT FUNCTION fz() AS 3;\n");

    var diag: Diag = .{};
    const prog = try loadProgram(a,
        \\@include 'x.sql';
        \\@include 'z.sql';
        \\CREATE CONNECTION pg TYPE postgres OPTIONS (host = 'hm');
        \\SELECT fx() AS a FROM pg.public.t;
    , "main.sql", base, &diag);
    var conns = std.array_list.Managed([]const u8).init(a);
    for (prog.stmts) |st| if (st == .connection) try conns.append(st.connection.name);
    for ([_][]const u8{ "__m1_pg", "pg" }, conns.items) |w, g| try testing.expectEqualStrings(w, g);
    try testing.expectEqual(@as(usize, 2), conns.items.len);

    const both = try loadProgram(a, "@include 'x.sql' AS x;\n@include 'y.sql' AS y;\nSELECT x.fx() + y.fy() + $x.n + $y.n AS s;\n", "main.sql", base, &diag);
    var vs = std.array_list.Managed([]const u8).init(a);
    for (both.stmts) |st| if (st == .let_const) try vs.append(st.let_const.name);
    for ([_][]const u8{ "__m1_v", "__m1_n", "__m2_v", "__m2_n" }, vs.items) |w, g| try testing.expectEqualStrings(w, g);

    try testing.expectError(error.ParseFailed, loadProgram(a, "@include 'x.sql';\n@include 'y.sql';\nSELECT fx() AS a;\n", "main.sql", base, &diag));
    try testing.expectEqual(@as(u32, 2), diag.parse.line);
    try testing.expect(std.mem.startsWith(u8, diag.parse.msg, "`n` is exported both by `"));
    try testing.expect(std.mem.endsWith(u8, diag.parse.msg, "y.sql`, each included without AS; include one of them AS an alias"));

    try testing.expectError(error.ParseFailed, loadProgram(a, "@include 'z.sql';\nCREATE FUNCTION FZ() AS 4;\nSELECT fz() AS a;\n", "main.sql", base, &diag));
    try testing.expectEqual(@as(u32, 1), diag.parse.line);
    try testing.expect(std.mem.startsWith(u8, diag.parse.msg, "`fz` is declared here and exported by `"));
    try testing.expect(std.mem.endsWith(u8, diag.parse.msg, "z.sql`, included without AS; include it AS an alias to tell them apart, or rename one"));
}

test "@include AS: a library's exports are used through its prefix — connections, variables, URL hosts, holes and run inputs" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try writeFile(&tmp, "lib.sql",
        \\EXPORT CONNECTION pg TYPE postgres OPTIONS (host = 'h');
        \\EXPORT CONNECTION share TYPE smb OPTIONS (host = 'files');
        \\EXPORT PARAM env STRING DEFAULT 'dev';
        \\PARAM secret STRING DEFAULT 's';
        \\EXPORT LET dir = 'out';
        \\EXPORT FUNCTION path(f) AS '${$dir}/' || f;
    );

    var diag: Diag = .{};
    const prog = try loadProgram(a,
        \\@include 'lib.sql' AS l;
        \\PARAM day STRING DEFAULT 'mon';
        \\LOAD INTO 'smb://l.share/${$l.dir}/${l.path($day)}.csv' AS SELECT $l.env AS e FROM l.pg.public.t;
    , "main.sql", base, &diag);
    try testing.expect(prog.modular);
    var ins = std.array_list.Managed([2][]const u8).init(a);
    for (prog.inputs) |in| try ins.append(.{ in.external, in.internal });
    try testing.expectEqual(@as(usize, 2), ins.items.len);
    try testing.expectEqualStrings("day", ins.items[0][0]);
    try testing.expectEqualStrings("day", ins.items[0][1]);
    try testing.expectEqualStrings("l.env", ins.items[1][0]);
    try testing.expectEqualStrings("__m1_env", ins.items[1][1]);
    try testing.expectEqualStrings("__m1_env", prog.inputName("l.env").?);
    try testing.expect(prog.inputName("secret") == null);
    try testing.expect(prog.inputName("l.secret") == null);

    const out = prog.stmts[prog.stmts.len - 1].output;
    try testing.expectEqualStrings("smb://__m1_share/${$__m1_dir}/${__m1_path($day)}.csv", out.stages[out.stages.len - 1].node.write.target);
    const fn_path = prog.stmts[6].func;
    try testing.expectEqualStrings("__m1_path", fn_path.name);

    try testing.expectError(error.ParseFailed, loadProgram(a, "@include 'lib.sql' AS l;\nSELECT $l.secret AS s;\n", "main.sql", base, &diag));
    try testing.expect(std.mem.startsWith(u8, diag.parse.msg, "`l.secret` is private to `"));
    try testing.expect(std.mem.endsWith(u8, diag.parse.msg, "lib.sql`, which exports pg, share, env, dir, path; declare it `EXPORT PARAM` there to use it here"));
    try testing.expectError(error.ParseFailed, loadProgram(a, "@include 'lib.sql' AS l;\nSELECT * FROM l.mysql.public.t;\n", "main.sql", base, &diag));
    try testing.expect(std.mem.endsWith(u8, diag.parse.msg, "lib.sql` exports no connection `mysql`; it exports pg, share, env, dir, path"));
}

test "@include: a PARAM the root cannot bind needs a DEFAULT" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try writeFile(&tmp, "req.sql", "PARAM tenant STRING;\nEXPORT FUNCTION t() AS $tenant;\n");
    try writeFile(&tmp, "inner.sql", "EXPORT PARAM tenant STRING;\nEXPORT FUNCTION t() AS $tenant;\n");
    try writeFile(&tmp, "outer.sql", "@include 'inner.sql';\nEXPORT FUNCTION u() AS t();\n");
    try writeFile(&tmp, "pub.sql", "EXPORT PARAM tenant STRING;\nEXPORT FUNCTION t() AS $tenant;\n");

    var diag: Diag = .{};
    for ([_][]const u8{ "req.sql", "outer.sql" }) |f| {
        const s = try msgfmt.allocPrint(a, "@include '{s}';\nSELECT 1 AS a;\n", .{f});
        try testing.expectError(error.ParseFailed, loadProgram(a, s, "main.sql", base, &diag));
        try testing.expect(std.mem.startsWith(u8, diag.parse.msg, "PARAM `tenant` has no DEFAULT and the script cannot bind it"));
        try testing.expectEqual(@as(u32, 1), diag.parse.line);
    }
    const prog = try loadProgram(a, "@include 'pub.sql';\nSELECT t() AS a;\n", "main.sql", base, &diag);
    try testing.expectEqualStrings("__m1_tenant", prog.inputName("tenant").?);
    const lib = try loadProgram(a, "@include 'pub.sql' AS p;\nSELECT p.t() AS a;\n", "main.sql", base, &diag);
    try testing.expectEqualStrings("__m1_tenant", lib.inputName("p.tenant").?);
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

test "an error inside an included table function's body names that file, where the call does not" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try writeFile(&tmp, "lib.sql",
        \\-- lib
        \\
        \\
        \\EXPORT FUNCTION g() RETURNS TABLE AS SELECT * FROM 'x.csv';
        \\EXPORT OR REPLACE FUNCTION g() RETURNS TABLE AS SELECT * FROM g();
    );

    var diag: Diag = .{};
    try testing.expectError(error.ParseFailed, loadProgram(a,
        \\@include 'lib.sql';
        \\SELECT * FROM g();
    , "main.sql", base, &diag));

    try testing.expect(std.mem.endsWith(u8, diag.label, "lib.sql"));
    try testing.expectEqual(@as(u32, 5), diag.parse.line);
    try testing.expectEqual(@as(u32, 63), diag.parse.col);
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
    try testing.expectEqualStrings("__m1_dbl", prog.stmts[1].func.name);
}

test "@include AS: a library's functions are namespaced, call each other private or exported, and only the exported reach its includer" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeFile(&tmp, "lib/s.sql", "CREATE FUNCTION sq(x) AS x * x;\nEXPORT FUNCTION hyp(a, b) AS sqrt(sq(a) + sq(b));\nEXPORT FUNCTION nums(n INT) RETURNS TABLE AS SELECT range AS v, sq(range) AS v2 FROM RANGE($n);\n");
    try writeFile(&tmp, "lib/o.sql", "EXPORT FUNCTION sq(x) AS x + 1;\n");
    try writeFile(&tmp, "lib/none.sql", "CREATE FUNCTION sq(x) AS x + 1;\n");
    const base = try tmp.dir.realpathAlloc(a, ".");
    var diag: Diag = .{};
    const prog = try loadProgram(a, "@include 'lib/s.sql' AS st;\n@include 'lib/o.sql' AS ot;\nSELECT st.hyp(3, 4) AS h, ot.sq(1) AS o FROM st.nums(2) n;\n", "main.sql", base, &diag);
    var names = std.array_list.Managed([]const u8).init(a);
    for (prog.stmts) |s| if (s == .func) try names.append(s.func.name);
    try testing.expectEqual(@as(usize, 4), names.items.len);
    for ([_][]const u8{ "__m1_sq", "__m1_hyp", "__m1_nums", "__m2_sq" }, names.items) |w, g| try testing.expectEqualStrings(w, g);
    const hyp = prog.stmts[2].func.body.expr;
    try testing.expectEqualStrings("__m1_sq", hyp.call.args[0].binary.l.call.name);
    try testing.expectError(error.ParseFailed, loadProgram(a, "@include 'lib/s.sql' AS st;\nSELECT * FROM nums(2);\n", "main.sql", base, &diag));
    try testing.expectError(error.ParseFailed, loadProgram(a, "@include 'lib/s.sql' AS st;\nSELECT st.sq(2) AS v;\n", "main.sql", base, &diag));
    try testing.expect(std.mem.startsWith(u8, diag.parse.msg, "`st.sq` is private to `"));
    try testing.expect(std.mem.endsWith(u8, diag.parse.msg, "lib/s.sql`, which exports hyp, nums; declare it `EXPORT FUNCTION` there to use it here"));
    try testing.expectError(error.ParseFailed, loadProgram(a, "@include 'lib/none.sql' AS n;\nSELECT 1 AS v;\n", "main.sql", base, &diag));
    try testing.expect(std.mem.endsWith(u8, diag.parse.msg, "lib/none.sql` is included AS n, a library, but exports nothing: declare what it shares with EXPORT (FUNCTION, CONNECTION, PARAM or LET)"));
}

test "@include AS: a library only declares; aliases are one per library and not a connection's name" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeFile(&tmp, "run.sql", "EXPORT FUNCTION f(x) AS x;\nLOAD INTO 'o.csv' AS SELECT 1 AS a;\n");
    try writeFile(&tmp, "q.sql", "EXPORT FUNCTION f(x) AS x;\nEXPORT LET n = (SELECT COUNT(*) AS n FROM 'in.csv');\n");
    try writeFile(&tmp, "f.sql", "EXPORT FUNCTION f(x) AS x;\n");
    try writeFile(&tmp, "g.sql", "EXPORT FUNCTION g(x) AS x;\n");
    const base = try tmp.dir.realpathAlloc(a, ".");
    var diag: Diag = .{};
    try testing.expectError(error.ParseFailed, loadProgram(a, "@include 'run.sql' AS r;\nSELECT 1 AS a;\n", "m.sql", base, &diag));
    try testing.expect(std.mem.indexOf(u8, diag.parse.msg, "may only declare functions, connections, PARAMs and LETs of a value; this LOAD INTO or SELECT would run") != null);
    try testing.expect(std.mem.endsWith(u8, diag.label, "run.sql"));
    try testing.expectEqual(@as(u32, 2), diag.parse.line);
    try testing.expectError(error.ParseFailed, loadProgram(a, "@include 'q.sql' AS q;\nSELECT 1 AS a;\n", "m.sql", base, &diag));
    try testing.expect(std.mem.indexOf(u8, diag.parse.msg, "this LET would run") != null);
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
        .{ "https://raw.githubusercontent.com/acme/kit/v1/sql/lib.sql", "@include 'util.sql' AS u;\nEXPORT FUNCTION twice(x) AS u.inc(u.inc(x));\n" },
        .{ "https://raw.githubusercontent.com/acme/kit/v1/sql/util.sql", "EXPORT FUNCTION inc(x) AS x + 1;\n" },
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
    try testing.expectEqualStrings("__m1_twice", prog.stmts[2].func.name);
    try testing.expectEqualStrings("__m2_inc", prog.stmts[2].func.body.expr.call.name);

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
        const s = try msgfmt.allocPrint(a, "@include 'github.com/acme/kit/sql/{s}@v1';\nSELECT 1 AS a;\n", .{f});
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
    try writeFile(&tmp, "a/lib.sql", "@include 'util.sql' AS u;\n@include 'help.sql';\nEXPORT FUNCTION f(x) AS u.g(h(x));\n");
    try writeFile(&tmp, "a/util.sql", "EXPORT FUNCTION g(x) AS x + 1;\n");
    try writeFile(&tmp, "a/help.sql", "EXPORT FUNCTION h(x) AS x * 10;\n");
    try writeFile(&tmp, "b/lib.sql", "@include 'util.sql' AS u;\nEXPORT FUNCTION f(x) AS u.g(x);\n");
    try writeFile(&tmp, "b/util.sql", "EXPORT FUNCTION g(x) AS x - 1;\n");
    const base = try tmp.dir.realpathAlloc(a, ".");
    var diag: Diag = .{};
    const prog = try loadProgram(a, "@include 'a/lib.sql' AS a;\n@include 'b/lib.sql' AS b;\nSELECT a.f(1) AS x, b.f(1) AS y;\n", "m.sql", base, &diag);
    var names = std.array_list.Managed([]const u8).init(a);
    for (prog.stmts) |s| if (s == .func) try names.append(s.func.name);
    for ([_][]const u8{ "__m2_g", "__m3_h", "__m1_f", "__m5_g", "__m4_f" }, names.items) |w, g| try testing.expectEqualStrings(w, g);
    try testing.expectEqual(@as(usize, 5), names.items.len);
    try testing.expectEqualStrings("__m2_g", prog.stmts[3].func.body.expr.call.name);
    try testing.expectEqualStrings("__m3_h", prog.stmts[3].func.body.expr.call.args[0].call.name);
    try testing.expectError(error.ParseFailed, loadProgram(a, "@include 'a/lib.sql' AS a;\nSELECT u.g(1) AS x;\n", "m.sql", base, &diag));
    try testing.expectError(error.ParseFailed, loadProgram(a, "@include 'a/lib.sql' AS a;\nSELECT a.h(1) AS x;\n", "m.sql", base, &diag));
    try testing.expect(std.mem.startsWith(u8, diag.parse.msg, "`a.h`: `"));
    try testing.expect(std.mem.endsWith(u8, diag.parse.msg, "a/lib.sql` exports no function `h`; it exports f"));
}

test "a grouped query's unknown function is told from one an included file declares" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeFile(&tmp, "fns.sql", "EXPORT FUNCTION shared(x) AS x + 1;\n");
    const base = try tmp.dir.realpathAlloc(a, ".");
    const Builtins = struct {
        fn is(_: []const u8) bool {
            return false;
        }
    };
    var diag: Diag = .{};
    const opts = Options{ .parse = .{ .is_builtin = Builtins.is } };
    try testing.expectError(error.ParseFailed, loadProgramOpts(a, "@include 'fns.sql';\nSELECT g, shared(v) AS x FROM 'in.csv' GROUP BY g;\n", "m.sql", base, &diag, opts));
    try testing.expect(std.mem.startsWith(u8, diag.parse.msg, "`v` is neither an aggregate"));
    try testing.expectError(error.ParseFailed, loadProgramOpts(a, "@include 'fns.sql';\nSELECT g, sharde(v) AS x FROM 'in.csv' GROUP BY g;\n", "m.sql", base, &diag, opts));
    try testing.expectEqualStrings("unknown function `sharde`: no builtin, aggregate or function of this script has that name", diag.parse.msg);
}

test "@include: names the parser makes up stay apart across files" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const body = "LET a = (SELECT COUNT(*) AS n FROM 'in.csv' WHERE x IN (SELECT x FROM 'k.csv'));\nEXPORT FUNCTION {s}() RETURNS TABLE AS SELECT * FROM 'in.csv' WHERE v > $a;\n";
    try writeFile(&tmp, "x.sql", try msgfmt.allocPrint(a, body, .{"fx"}));
    try writeFile(&tmp, "y.sql", try msgfmt.allocPrint(a, body, .{"fy"}));
    const base = try tmp.dir.realpathAlloc(a, ".");
    var diag: Diag = .{};
    const prog = try loadProgram(a, "@include 'x.sql';\n@include 'y.sql';\nSELECT * FROM fx() UNION ALL SELECT * FROM fy();\n", "m.sql", base, &diag);
    var names = std.array_list.Managed([]const u8).init(a);
    for (prog.stmts) |st| switch (st) {
        .binding => |b| try names.append(b.name),
        .let_const => |l| try names.append(l.name),
        else => {},
    };
    try testing.expect(names.items.len >= 4);
    for (names.items, 0..) |n, i| for (names.items[i + 1 ..]) |m| {
        if (std.mem.eql(u8, n, m)) std.debug.print("repeated name {s}\n", .{n});
        try testing.expect(!std.mem.eql(u8, n, m));
    };
}
