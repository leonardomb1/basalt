//! Command-line surface:
//!   basalt run   <script>|-c <script> [-p k=v ...] [-j N] [--port N]
//!   basalt check <script>|-c <script>
//!   basalt repl
//! `run` executes (HTTP mode when the script declares an endpoint); `check`
//! validates and plans without running. A
//! script comes from a file path or, with `-c/--command`, inline. `repl` is an
//! interactive loop that runs on `;`, carries declarations across entries, and
//! prints results via the `write stdout` table sink.

const std = @import("std");
const parser = @import("../lang/sql_parser.zig");
const include = @import("../lang/include.zig");
const linemod = @import("line.zig");
const ast = @import("../lang/ast.zig");
const runtime = @import("../runtime/run.zig");
const obs = @import("../runtime/obs.zig");
const analyze = @import("../runtime/analyze.zig");
const server = @import("../server/http.zig");

/// SIGTERM/SIGINT → ask the run to stop at its next boundary (async-signal-safe:
/// one atomic store). The control plane uses this to cancel a job or roll a server.
/// A second signal means "stop being graceful": exit 130 on the spot, so an
/// interactive ^C ^C isn't held hostage by a slow upstream read.
fn onTerminate(_: i32) callconv(.c) void {
    if (runtime.aborting()) std.posix.exit(130);
    runtime.requestAbort();
}

/// SIGHUP → reload a multi-script server's directory (control plane writes new
/// scripts, then signals). Async-signal-safe: one atomic store.
fn onReload(_: i32) callconv(.c) void {
    runtime.requestReload();
}

fn installSignalHandlers() void {
    const term = std.posix.Sigaction{ .handler = .{ .handler = onTerminate }, .mask = std.posix.sigemptyset(), .flags = 0 };
    std.posix.sigaction(std.posix.SIG.TERM, &term, null);
    std.posix.sigaction(std.posix.SIG.INT, &term, null);
    const hup = std.posix.Sigaction{ .handler = .{ .handler = onReload }, .mask = std.posix.sigemptyset(), .flags = 0 };
    std.posix.sigaction(std.posix.SIG.HUP, &hup, null);
}

pub fn run(alloc: std.mem.Allocator) !void {
    installSignalHandlers();
    const args = try std.process.argsAlloc(alloc);
    defer std.process.argsFree(alloc, args);

    var stderr_buf: [4096]u8 = undefined;
    var stderr_file = std.fs.File.stderr().writer(&stderr_buf);
    const stderr = &stderr_file.interface;

    if (args.len < 2) {
        try usage(stderr);
        try stderr.flush();
        std.process.exit(2);
    }

    const verb = args[1];
    if (std.mem.eql(u8, verb, "check")) {
        std.process.exit(try cmdCheck(alloc, args));
    } else if (std.mem.eql(u8, verb, "run")) {
        std.process.exit(try cmdRun(alloc, args));
    } else if (std.mem.eql(u8, verb, "serve")) {
        std.process.exit(try cmdServe(alloc, args));
    } else if (std.mem.eql(u8, verb, "repl")) {
        std.process.exit(try cmdRepl(alloc));
    } else if (std.mem.eql(u8, verb, "help") or std.mem.eql(u8, verb, "-h") or std.mem.eql(u8, verb, "--help")) {
        var stdout_buf: [4096]u8 = undefined;
        var stdout_file = std.fs.File.stdout().writer(&stdout_buf);
        try usage(&stdout_file.interface);
        try stdout_file.interface.flush();
        return;
    }

    try stderr.print("error: unknown command `{s}`\n\n", .{verb});
    try usage(stderr);
    try stderr.flush();
    std.process.exit(2);
}

/// A script source plus a label used in diagnostics (a file path, or `<command>`).
/// `dir` is what `@include 'p.sql'` resolves against: the script's own directory,
/// or the cwd for an inline/stdin script.
const Source = struct { label: []const u8, text: []const u8, dir: []const u8 = "." };

/// Resolve the script source: `-c/--command <text>` for an inline script, else the
/// positional <script> path read from disk. Prints diagnostics and returns null on
/// failure. `text` is owned by `arena` (or by argv, also long-lived).
fn loadSource(arena: std.mem.Allocator, verb: []const u8, args: [][:0]u8, stderr: *std.Io.Writer) !?Source {
    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "-c") or std.mem.eql(u8, args[i], "--command")) {
            if (i + 1 >= args.len) {
                try stderr.print("error: missing script after `{s}`\n", .{args[i]});
                return null;
            }
            return Source{ .label = "<command>", .text = args[i + 1] };
        }
    }
    if (args.len < 3) {
        try stderr.print("error: `{s}` requires a <script> path, `-` for stdin, or `-c <script>`\n", .{verb});
        return null;
    }
    if (std.mem.eql(u8, args[2], "-")) {
        const text = std.fs.File.stdin().readToEndAlloc(arena, 8 << 20) catch |e| {
            try stderr.print("error: cannot read script from stdin: {s}\n", .{@errorName(e)});
            return null;
        };
        return Source{ .label = "<stdin>", .text = text };
    }
    if (args[2].len > 0 and args[2][0] == '-') {
        try stderr.print("error: `{s}` requires a <script> path, `-` for stdin, or `-c <script>`\n", .{verb});
        return null;
    }
    const path = args[2];
    const text = std.fs.cwd().readFileAlloc(arena, path, 8 << 20) catch |e| {
        try stderr.print("error: cannot read `{s}`: {s}\n", .{ path, @errorName(e) });
        return null;
    };
    return Source{ .label = path, .text = text, .dir = std.fs.path.dirname(path) orelse "." };
}

/// Parse a resolved source (resolving its `@include` header first), printing a
/// located diagnostic on failure. The AST is allocated in `arena` and slices into
/// `src.text` and the included files' texts, so all must outlive use. The
/// diagnostic names the file it came from — an included file reports its own
/// path and its own line numbers.
fn parseSrc(arena: std.mem.Allocator, src: Source, stderr: *std.Io.Writer) !?ast.Program {
    var diag: include.Diag = .{};
    return include.loadProgram(arena, src.text, src.label, src.dir, &diag) catch |e| switch (e) {
        error.ParseFailed => {
            const label = if (diag.label.len > 0) diag.label else src.label;
            try stderr.print("{s}:{d}:{d}: error: {s}\n", .{ label, diag.parse.line, diag.parse.col, diag.parse.msg });
            return null;
        },
        error.OutOfMemory => return e,
    };
}

fn cmdCheck(alloc: std.mem.Allocator, args: [][:0]u8) !u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();

    var out_buf: [8192]u8 = undefined;
    var out_file = std.fs.File.stdout().writer(&out_buf);
    const stdout = &out_file.interface;
    defer stdout.flush() catch {};
    var err_buf: [4096]u8 = undefined;
    var err_file = std.fs.File.stderr().writer(&err_buf);
    const stderr = &err_file.interface;
    defer stderr.flush() catch {};

    const src = (try loadSource(a, "check", args, stderr)) orelse return 1;
    const prog = (try parseSrc(a, src, stderr)) orelse return 1;

    var overrides = std.array_list.Managed(analyze.ParamOverride).init(a);
    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        if (!std.mem.eql(u8, args[i], "-p") and !std.mem.eql(u8, args[i], "--param")) continue;
        i += 1;
        if (i >= args.len) {
            try stderr.print("error: missing key=value after `-p`\n", .{});
            return 2;
        }
        const eq = std.mem.indexOfScalar(u8, args[i], '=') orelse {
            try stderr.print("error: param must be key=value, got `{s}`\n", .{args[i]});
            return 2;
        };
        try overrides.append(.{ .name = args[i][0..eq], .value = args[i][eq + 1 ..] });
    }

    var adiag = analyze.Diag{};
    _ = analyze.analyzeWith(a, prog, overrides.items, &adiag) catch |e| switch (e) {
        error.OutOfMemory => return e,
        error.AnalyzeFailed => {
            try stderr.print("{s}: error: {s}\n", .{ src.label, adiag.msg });
            return 1;
        },
    };

    try stdout.print("ok: {s} checks out\n", .{src.label});
    return 0;
}

fn cmdRun(alloc: std.mem.Allocator, args: [][:0]u8) !u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();

    var stderr_buf: [4096]u8 = undefined;
    var stderr_file = std.fs.File.stderr().writer(&stderr_buf);
    const stderr = &stderr_file.interface;
    defer stderr.flush() catch {};

    const src = (try loadSource(arena.allocator(), "run", args, stderr)) orelse return 1;
    const prog = (try parseSrc(arena.allocator(), src, stderr)) orelse return 1;

    var params = std.array_list.Managed(runtime.ParamArg).init(alloc);
    defer params.deinit();
    var port: u16 = 8080;
    var threads: usize = std.Thread.getCpuCount() catch 1;
    var log = runtime.LogConfig{};
    var stdout_json = false;
    var explain = false;
    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        const a = args[i];
        if (std.mem.eql(u8, a, "-c") or std.mem.eql(u8, a, "--command")) {
            i += 1;
            continue;
        }
        if (threadFlagValue(a, args, &i)) |tv| {
            threads = std.fmt.parseInt(usize, tv, 10) catch {
                try stderr.print("error: invalid --threads `{s}`\n", .{tv});
                return 2;
            };
            if (threads == 0) threads = 1;
        } else if (std.mem.eql(u8, a, "--format")) {
            const v = (try nextVal(args, &i, a, stderr)) orelse return 2;
            stdout_json = if (std.mem.eql(u8, v, "json")) true else if (std.mem.eql(u8, v, "table")) false else {
                try stderr.print("error: --format must be table|json\n", .{});
                return 2;
            };
        } else if (std.mem.eql(u8, a, "--explain")) {
            explain = true;
        } else if (std.mem.eql(u8, a, "--quiet") or std.mem.eql(u8, a, "-q")) {
            log.quiet = true;
        } else if (std.mem.eql(u8, a, "--log-format")) {
            const v = (try nextVal(args, &i, a, stderr)) orelse return 2;
            log.format = if (std.mem.eql(u8, v, "text")) .text else if (std.mem.eql(u8, v, "json")) .json else if (std.mem.eql(u8, v, "auto")) .auto else {
                try stderr.print("error: --log-format must be auto|text|json\n", .{});
                return 2;
            };
        } else if (std.mem.eql(u8, a, "--log-level")) {
            const v = (try nextVal(args, &i, a, stderr)) orelse return 2;
            log.level = obs.Level.parse(v) orelse {
                try stderr.print("error: --log-level must be error|warn|info|debug\n", .{});
                return 2;
            };
        } else if (std.mem.eql(u8, a, "-p") or std.mem.eql(u8, a, "--param")) {
            const kv = (try nextVal(args, &i, a, stderr)) orelse return 2;
            const eqp = std.mem.indexOfScalar(u8, kv, '=') orelse {
                try stderr.print("error: param must be key=value, got `{s}`\n", .{kv});
                return 2;
            };
            try params.append(.{ .key = kv[0..eqp], .val = kv[eqp + 1 ..] });
        } else if (std.mem.eql(u8, a, "--port")) {
            const v = (try nextVal(args, &i, a, stderr)) orelse return 2;
            port = std.fmt.parseInt(u16, v, 10) catch {
                try stderr.print("error: invalid --port `{s}`\n", .{v});
                return 2;
            };
        }
    }

    if (prog.stmts.len > 0 and prog.stmts[0] == .kind and prog.stmts[0].kind.kind == .http) {
        server.serve(alloc, prog, port) catch |e| {
            try stderr.print("{s}: serve error: {s}\n", .{ src.label, @errorName(e) });
            return 1;
        };
        return 0;
    }

    if (prog.explain == .plan) {
        var ebuf: [4096]u8 = undefined;
        var efile = std.fs.File.stdout().writer(&ebuf);
        const eout = &efile.interface;
        defer eout.flush() catch {};
        var adiag2 = analyze.Diag{};
        const plan = analyze.analyze(arena.allocator(), prog, &adiag2) catch |e| switch (e) {
            error.OutOfMemory => return e,
            error.AnalyzeFailed => {
                try stderr.print("{s}: error: {s}\n", .{ src.label, adiag2.msg });
                return 1;
            },
        };
        try analyze.render(plan, eout);
        return 0;
    }

    log.summary = if (stdout_json) .json_stdout else .stderr;

    var diag: runtime.Diag = .{};
    var sink = runtime.OutcomeSink.init(alloc);
    defer sink.deinit();
    _ = runtime.run(alloc, prog, .{ .params = params.items, .threads = threads, .outcomes = &sink, .log = log, .explain = explain or prog.explain == .analyze, .stdout_json = stdout_json }, &diag) catch |e| switch (e) {
        error.Aborted => {
            try stderr.print("{s}: aborted\n", .{src.label});
            return 130;
        },
        error.PlanFailed => {
            const tag = if (diag.retryable) " (transient)" else "";
            try stderr.print("{s}: error{s}: {s}\n", .{ src.label, tag, diag.msg });
            return if (diag.retryable) 75 else 1;
        },
        error.OutOfMemory => return e,
        else => {
            const transient = diag.retryable or runtime.isTransient(e);
            const tag = if (transient) " (transient)" else "";
            if (diag.msg.len > 0)
                try stderr.print("{s}: error{s}: {s}\n", .{ src.label, tag, diag.msg })
            else
                try stderr.print("{s}: runtime error{s}: {s}\n", .{ src.label, tag, runtime.errLabel(e) });
            return if (transient) 75 else 1;
        },
    };
    const nfail = sink.failures();
    if (nfail > 0) {
        var all_retryable = true;
        for (sink.list.items) |o| {
            if (o.ok) continue;
            if (!o.retryable) all_retryable = false;
            const tag = if (o.retryable) " (transient)" else "";
            try stderr.print("{s}: item `{s}` failed{s}: {s}\n", .{ src.label, o.item, tag, o.err });
        }
        try stderr.print("{s}: {d}/{d} item(s) failed\n", .{ src.label, nfail, sink.list.items.len });
        return if (all_retryable) 75 else 1;
    }
    return 0;
}

/// `serve <dir> [--port N]`: host every `@http` script in a directory, routing by
/// each script's declared path. SIGHUP reloads the directory.
fn cmdServe(alloc: std.mem.Allocator, args: [][:0]u8) !u8 {
    var err_buf: [4096]u8 = undefined;
    var err_file = std.fs.File.stderr().writer(&err_buf);
    const stderr = &err_file.interface;
    defer stderr.flush() catch {};

    if (args.len < 3 or (args[2].len > 0 and args[2][0] == '-')) {
        try stderr.print("error: `serve` requires a <dir> of @http scripts\n", .{});
        return 2;
    }
    const dir = args[2];

    var port: u16 = 8080;
    var watch = false;
    var i: usize = 3;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--port") or std.mem.eql(u8, args[i], "-p")) {
            const v = (try nextVal(args, &i, "--port", stderr)) orelse return 2;
            port = std.fmt.parseInt(u16, v, 10) catch {
                try stderr.print("error: invalid --port `{s}`\n", .{v});
                return 2;
            };
        } else if (std.mem.eql(u8, args[i], "--watch") or std.mem.eql(u8, args[i], "-w")) {
            watch = true;
        }
    }

    server.serveDir(alloc, dir, port, watch) catch |e| {
        try stderr.print("serve error: {s}\n", .{@errorName(e)});
        return 1;
    };
    return 0;
}

/// Index of the next `;` at statement level — outside `'...'` (with `''`
/// escapes), `"..."` (with `\` escapes), `$$`/`$tag$` dollar quotes, `--` line
/// comments and `/* */` block comments. Mirrors the lexer's trivia and string
/// rules so the REPL agrees with the parser on where a statement ends.
fn nextTopSemi(s: []const u8, from: usize) ?usize {
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

/// True when the entry is ready to run: its last non-blank character is a
/// statement-level `;`.
fn endsComplete(s: []const u8) bool {
    const t = std.mem.trim(u8, s, " \t\r\n");
    if (t.len == 0 or t[t.len - 1] != ';') return false;
    var i: usize = 0;
    while (nextTopSemi(t, i)) |p| : (i = p + 1) {
        if (p == t.len - 1) return true;
    }
    return false;
}

/// Split an entry on statement-level `;`, returning trimmed non-empty statement
/// texts with the terminator stripped. Slices point into `s`.
fn splitStatements(arena: std.mem.Allocator, s: []const u8) ![]const []const u8 {
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

const DeclKind = enum { connection, function, param, let, endpoint };
const DeclId = struct { kind: DeclKind, name: []const u8 };

/// Next whitespace-delimited word at `i.*`, advancing past it.
fn nextWord(s: []const u8, i: *usize) ?[]const u8 {
    while (i.* < s.len and std.ascii.isWhitespace(s[i.*])) i.* += 1;
    if (i.* >= s.len) return null;
    const start = i.*;
    while (i.* < s.len and !std.ascii.isWhitespace(s[i.*])) i.* += 1;
    return s[start..i.*];
}

/// Leading identifier of a word, so `f(a,` yields `f`.
/// ponytail: bare identifiers only — the dialect has no quoted decl names.
fn identPrefix(w: []const u8) []const u8 {
    var n: usize = 0;
    while (n < w.len and (std.ascii.isAlphanumeric(w[n]) or w[n] == '_')) n += 1;
    return w[0..n];
}

/// Classify a statement as a session declaration and name it:
/// `CREATE [OR REPLACE] CONNECTION|FUNCTION <name>`, `CREATE ENDPOINT ...`
/// (unnamed — the REPL rejects it), or `PARAM <name>`. Null for anything else.
fn declOf(stmt: []const u8) ?DeclId {
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
    const kind: DeclKind = if (std.ascii.eqlIgnoreCase(w, "connection"))
        .connection
    else if (std.ascii.eqlIgnoreCase(w, "function"))
        .function
    else
        return null;
    const n = identPrefix(nextWord(stmt, &i) orelse return null);
    return if (n.len == 0) null else .{ .kind = kind, .name = n };
}

/// Declarations carried across REPL entries, so `CREATE CONNECTION erp ...` in
/// one entry is still in scope for a `SELECT ... FROM erp.orders` in the next.
/// Order-preserving (declarations may reference earlier ones); re-declaring a
/// (kind, name) replaces the stored text in place. Text is duped with the
/// REPL's gpa because the input buffer is reused every line.
const DeclStore = struct {
    const Entry = struct { kind: DeclKind, name: []u8, text: []u8 };

    gpa: std.mem.Allocator,
    items: std.array_list.Managed(Entry),

    fn init(gpa: std.mem.Allocator) DeclStore {
        return .{ .gpa = gpa, .items = std.array_list.Managed(Entry).init(gpa) };
    }
    fn deinit(self: *DeclStore) void {
        self.clear();
        self.items.deinit();
    }
    fn clear(self: *DeclStore) void {
        for (self.items.items) |e| {
            self.gpa.free(e.name);
            self.gpa.free(e.text);
        }
        self.items.clearRetainingCapacity();
    }
    fn put(self: *DeclStore, id: DeclId, text: []const u8) !void {
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

/// Mutable REPL state: the declaration prelude plus per-session toggles.
const Session = struct {
    decls: DeclStore,
    json: bool = false,
    tty: bool = false,
};

/// Interactive read-eval-print loop. An entry runs when a line ends in a
/// statement-level `;` (a blank line also runs a pending buffer, which is what
/// `echo ... | basalt repl` relies on). Declarations persist across entries; a
/// terminal SELECT prints as a table (a stdout sink is appended when the entry
/// doesn't write). Prompts only on a TTY.
fn cmdRepl(alloc: std.mem.Allocator) !u8 {
    var in_buf: [64 * 1024]u8 = undefined;
    var in_file = std.fs.File.stdin().reader(&in_buf);
    const in = &in_file.interface;

    var msg_buf: [4096]u8 = undefined;
    var msg_file = std.fs.File.stderr().writer(&msg_buf);
    const msg = &msg_file.interface;

    var sess = Session{ .decls = DeclStore.init(alloc), .tty = std.posix.isatty(std.fs.File.stdin().handle) };
    defer sess.decls.deinit();

    var editor: ?linemod.Editor = if (sess.tty) linemod.Editor.init(alloc) else null;
    defer if (editor) |*e| e.deinit();

    if (sess.tty) {
        try msg.writeAll("basalt REPL — end a statement with `;` to run it. \\q quits, \\help for help; arrows recall history.\n");
        try msg.flush();
    }

    var block = std.array_list.Managed(u8).init(alloc);
    defer block.deinit();

    var quit = false;
    while (!quit) {
        // Un-poison the session: a ^C-aborted query leaves the abort flag set.
        runtime.resetAbort();
        block.clearRetainingCapacity();
        while (true) {
            var owned: ?[]u8 = null;
            defer if (owned) |o| alloc.free(o);
            var line: []const u8 = undefined;
            if (editor) |*ed| {
                const prompt: []const u8 = if (block.items.len == 0) "\xc2\xbb " else "\xe2\x80\xa6 ";
                switch (ed.readLine(prompt) catch |e| blk: {
                    try msg.print("input error: {s}\n", .{@errorName(e)});
                    try msg.flush();
                    break :blk linemod.Result.eof;
                }) {
                    .eof => {
                        quit = true;
                        break;
                    },
                    .interrupt => {
                        // ^C discards the whole pending block, not just the line.
                        block.clearRetainingCapacity();
                        continue;
                    },
                    .line => |l| {
                        owned = l;
                        line = l;
                        ed.remember(l);
                    },
                }
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
                break; // blank line still runs a pending buffer
            }
            // Meta commands only lead an entry, so `\` inside a query is untouched.
            if (block.items.len == 0 and (t[0] == '\\' or isQuit(t) or isHelp(t))) {
                if (isQuit(t)) {
                    quit = true;
                    break;
                }
                try metaCommand(t, &sess, msg);
                continue;
            }
            try block.appendSlice(line);
            try block.append('\n');
            if (endsComplete(block.items)) break;
        }

        const trimmed = std.mem.trim(u8, block.items, " \t\r\n");
        if (trimmed.len == 0) continue;
        try runBlock(alloc, trimmed, &sess, msg);
    }
    if (sess.tty) {
        try msg.writeAll("bye\n");
        try msg.flush();
    }
    return 0;
}

/// Handle a `\...` entry. Unknown ones report themselves instead of reaching
/// the parser.
fn metaCommand(t: []const u8, sess: *Session, msg: *std.Io.Writer) !void {
    defer msg.flush() catch {};
    if (isHelp(t)) return replHelp(msg);

    var i: usize = 0;
    const cmd = nextWord(t, &i) orelse return;
    const rest = std.mem.trim(u8, t[i..], " \t\r\n");

    if (std.mem.eql(u8, cmd, "\\connections") or std.mem.eql(u8, cmd, "\\c")) {
        if (sess.decls.items.items.len == 0) return msg.writeAll("(no declarations)\n");
        for (sess.decls.items.items) |e| try msg.print("{s} {s}\n", .{ @tagName(e.kind), e.name });
        return;
    }
    if (std.mem.eql(u8, cmd, "\\clear")) {
        sess.decls.clear();
        return msg.writeAll("cleared\n");
    }
    if (std.mem.eql(u8, cmd, "\\format") or std.mem.eql(u8, cmd, "\\f")) {
        if (rest.len == 0) {
            // fall through to the echo below
        } else if (std.ascii.eqlIgnoreCase(rest, "json")) {
            sess.json = true;
        } else if (std.ascii.eqlIgnoreCase(rest, "table")) {
            sess.json = false;
        } else {
            return msg.print("error: \\format takes `json` or `table`, got `{s}`\n", .{rest});
        }
        const mode: []const u8 = if (sess.json) "json" else "table";
        return msg.print("format {s}\n", .{mode});
    }
    try msg.print("error: unknown command `{s}` — \\help for help\n", .{cmd});
}

/// Parse and run one REPL entry, reporting errors without aborting the loop.
/// The entry is prefixed with the session's stored declarations so earlier
/// connections/functions/params are in scope; new declarations are committed
/// only once the combined text parses, so a typo can't poison the session.
fn runBlock(alloc: std.mem.Allocator, block: []const u8, sess: *Session, msg: *std.Io.Writer) !void {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();

    const Pending = struct { id: DeclId, text: []const u8 };
    var pending = std.array_list.Managed(Pending).init(a);
    var executable: usize = 0;
    for (try splitStatements(a, block)) |st| {
        const id = declOf(st) orelse {
            executable += 1;
            continue;
        };
        if (id.kind == .endpoint) {
            try msg.writeAll("error: CREATE ENDPOINT can't run in the REPL — put it in a script and use `basalt serve <dir>`\n");
            try msg.flush();
            return;
        }
        try pending.append(.{ .id = id, .text = st });
    }

    // Prelude + entry. A declaration this entry replaces is dropped from the
    // prelude so the combined text doesn't declare the same name twice.
    var buf = std.array_list.Managed(u8).init(a);
    for (sess.decls.items.items) |e| {
        var shadowed = false;
        for (pending.items) |p| {
            if (p.id.kind == e.kind and std.ascii.eqlIgnoreCase(p.id.name, e.name)) shadowed = true;
        }
        if (shadowed) continue;
        try buf.appendSlice(e.text);
        try buf.appendSlice(";\n");
    }
    try buf.appendSlice(block);
    const text = buf.items;

    // An entry may open with `@include 'p.sql';` (paths relative to the cwd): the
    // file's text is parsed with the entry as one program, so its declarations are
    // in scope for the statements typed below it. They live as long as the entry —
    // the REPL keeps no session store.
    var diag: include.Diag = .{};
    const prog = include.loadProgram(a, text, "<repl>", ".", &diag) catch |e| switch (e) {
        error.ParseFailed => {
            if (diag.label.len > 0 and !std.mem.eql(u8, diag.label, "<repl>"))
                try msg.print("error: {s}:{d}:{d}: {s}\n", .{ diag.label, diag.parse.line, diag.parse.col, diag.parse.msg })
            else
                try msg.print("error: {d}:{d}: {s}\n", .{ diag.parse.line, diag.parse.col, diag.parse.msg });
            try msg.flush();
            return;
        },
        error.OutOfMemory => return e,
    };

    for (pending.items) |p| try sess.decls.put(p.id, p.text);

    if (executable == 0) {
        for (pending.items) |p| try msg.print("ok: {s} {s}\n", .{ @tagName(p.id.kind), p.id.name });
        try msg.flush();
        return;
    }

    const prepared = try appendDisplaySinks(a, prog);

    // An entry that *opens* with EXPLAIN carries the program-level prefix, which
    // `basalt run` renders without executing; do the same here rather than silently
    // running the query. EXPLAIN after anything else (including the session's own
    // declaration prelude) is an ordinary statement the executor handles.
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

    const t0 = std.time.milliTimestamp();
    var rdiag: runtime.Diag = .{};
    _ = runtime.run(alloc, prepared, .{
        .log = .{ .summary = .none, .quiet = true },
        .stdout_json = sess.json,
        .explain = prog.explain == .analyze,
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
        try msg.print("({d} ms)\n", .{std.time.milliTimestamp() - t0});
        try msg.flush();
    }
}

/// Append a `write stdout` table sink to any output pipeline that doesn't already
/// end in a `write`, so REPL entries show their results.
fn appendDisplaySinks(arena: std.mem.Allocator, prog: ast.Program) !ast.Program {
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
        stmts[i] = .{ .output = .{ .stages = stages, .pos = p.pos } };
    }
    return .{ .stmts = stmts };
}

fn isQuit(s: []const u8) bool {
    inline for (.{ "\\q", "\\quit", ":q", "quit", "exit" }) |k| {
        if (std.mem.eql(u8, s, k)) return true;
    }
    return false;
}
fn isHelp(s: []const u8) bool {
    inline for (.{ "\\help", "\\h", "help", "?" }) |k| {
        if (std.mem.eql(u8, s, k)) return true;
    }
    return false;
}
fn replHelp(msg: *std.Io.Writer) !void {
    try msg.writeAll(
        \\REPL — end a statement with `;` to run it; a blank line also runs a pending entry.
        \\  • a terminal SELECT prints as a table; LOAD INTO writes to its target.
        \\  • CREATE CONNECTION / CREATE FUNCTION / PARAM stay in scope for later entries.
        \\  example:  CREATE CONNECTION erp TYPE postgres HOST 'db' DATABASE 'erp';
        \\            SELECT id, amount FROM erp.orders WHERE status = 'paid';
        \\
        \\commands:
        \\  \connections, \c   list the declarations held for this session
        \\  \clear             forget them all
        \\  \format json|table set the output format (bare \format shows it); \f is an alias
        \\  \help, \h, ?       this help
        \\  \q, \quit, exit    leave
        \\
        \\arrows browse history (persisted in ~/.basalt_history); ^A/^E home/end, ^U clears the line, ^C drops the entry
        \\
    );
    try msg.flush();
}

/// Advance past a flag to its value argument; null (after printing the
/// `missing value` error) when the flag is the last argument.
fn nextVal(args: [][:0]u8, i: *usize, flag: []const u8, stderr: *std.Io.Writer) !?[]const u8 {
    i.* += 1;
    if (i.* >= args.len) {
        try stderr.print("error: missing value after `{s}`\n", .{flag});
        return null;
    }
    return args[i.*];
}

/// Recognize the threads flag in all of `-j N`, `-jN`, `--threads N`,
/// `--threads=N`, returning the value string (advancing `i` past a separate arg).
fn threadFlagValue(a: []const u8, args: [][:0]u8, i: *usize) ?[]const u8 {
    if (std.mem.eql(u8, a, "-j") or std.mem.eql(u8, a, "--threads")) {
        if (i.* + 1 < args.len) {
            i.* += 1;
            return args[i.*];
        }
        return "";
    }
    if (std.mem.startsWith(u8, a, "-j")) return a[2..];
    if (std.mem.startsWith(u8, a, "--threads=")) return a["--threads=".len..];
    return null;
}

/// Build a mutable argv ([][:0]u8) from string literals for flag-parsing tests.
fn testArgv(arena: std.mem.Allocator, strs: []const []const u8) ![][:0]u8 {
    const out = try arena.alloc([:0]u8, strs.len);
    for (strs, 0..) |s, i| out[i] = try arena.dupeZ(u8, s);
    return out;
}

test "threadFlagValue recognizes all four -j/--threads spellings" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var args = try testArgv(a, &.{ "-j", "4" });
    var i: usize = 0;
    try std.testing.expectEqualStrings("4", threadFlagValue(args[0], args, &i).?);
    try std.testing.expectEqual(@as(usize, 1), i);
    args = try testArgv(a, &.{ "--threads", "8" });
    i = 0;
    try std.testing.expectEqualStrings("8", threadFlagValue(args[0], args, &i).?);
    try std.testing.expectEqual(@as(usize, 1), i);

    args = try testArgv(a, &.{"-j16"});
    i = 0;
    try std.testing.expectEqualStrings("16", threadFlagValue(args[0], args, &i).?);
    try std.testing.expectEqual(@as(usize, 0), i);
    args = try testArgv(a, &.{"--threads=2"});
    i = 0;
    try std.testing.expectEqualStrings("2", threadFlagValue(args[0], args, &i).?);

    args = try testArgv(a, &.{"-j"});
    i = 0;
    try std.testing.expectEqualStrings("", threadFlagValue(args[0], args, &i).?);

    args = try testArgv(a, &.{ "-p", "k=v" });
    i = 0;
    try std.testing.expect(threadFlagValue(args[0], args, &i) == null);
    try std.testing.expectEqual(@as(usize, 0), i);
}

test "REPL input classification: quit, help" {
    try std.testing.expect(isQuit("\\q"));
    try std.testing.expect(isQuit("exit"));
    try std.testing.expect(!isQuit("exit()"));
    try std.testing.expect(isHelp("?"));
    try std.testing.expect(isHelp("\\help"));
    try std.testing.expect(!isHelp("help me"));
}

test "endsComplete sees only statement-level semicolons" {
    try std.testing.expect(endsComplete("SELECT 1;"));
    try std.testing.expect(endsComplete("  SELECT 1;\n\n"));
    try std.testing.expect(!endsComplete(""));
    try std.testing.expect(!endsComplete("SELECT 1"));

    // a `;` inside a literal or a comment doesn't end the statement
    try std.testing.expect(!endsComplete("SELECT ';' AS x"));
    try std.testing.expect(endsComplete("SELECT ';' AS x;"));
    try std.testing.expect(!endsComplete("SELECT 'it''s;")); // `''` escape, still open
    try std.testing.expect(endsComplete("SELECT 'it''s;' AS x;"));
    try std.testing.expect(!endsComplete("SELECT \"a\\\";")); // `\"` escape, still open
    try std.testing.expect(endsComplete("SELECT \"a;b\" AS x;"));
    try std.testing.expect(!endsComplete("SELECT 1 -- ;"));
    try std.testing.expect(!endsComplete("/* ; */"));
    try std.testing.expect(endsComplete("/* ; */ SELECT 1;"));

    // dollar quotes, both anonymous and tagged
    try std.testing.expect(!endsComplete("FROM c.QUERY($$a;b$$)"));
    try std.testing.expect(endsComplete("FROM c.QUERY($$a;b$$);"));
    try std.testing.expect(!endsComplete("FROM c.QUERY($q$a;b$q$)"));
    try std.testing.expect(endsComplete("FROM c.QUERY($q$a;b$q$);"));
    // `$name` is a param reference, not a quote opener
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
    try std.testing.expectEqual(DeclKind.endpoint, declOf("CREATE ENDPOINT '/x'").?.kind);

    try std.testing.expect(declOf("SELECT 1") == null);
    try std.testing.expect(declOf("CREATE TABLE t") == null);
    try std.testing.expect(declOf("CREATE OR SOMETHING CONNECTION erp") == null);
    try std.testing.expect(declOf("") == null);
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
    for (kept.stmts, sunk.stmts) |st, orig| {
        if (st != .output) continue;
        const stages = st.output.stages;
        try std.testing.expectEqualStrings("csv", stages[stages.len - 1].node.write.connector);
        try std.testing.expectEqual(orig.output.stages.len, stages.len);
    }
}

fn usage(w: anytype) !void {
    try w.writeAll(
        \\basalt — a SQL-driven data pipeline engine
        \\
        \\usage:
        \\  basalt run   <script>|-|-c <script> [-p key=value ...] [-j N] [--port N]
        \\               run a pipeline; HTTP mode when the script declares CREATE ENDPOINT
        \\  basalt serve <dir> [--port N] [--watch]
        \\               host every endpoint script in a dir (SIGHUP or -w reloads)
        \\  basalt check <script>|-|-c <script>
        \\               parse and validate without running; `EXPLAIN` prints the plan
        \\  basalt repl  interactive read-eval-print loop
        \\  basalt help  show this help
        \\
        \\script:
        \\  a path, `-` for stdin, or `-c <script>` for an inline script
        \\  a terminal `SELECT ...;` prints a table; `LOAD INTO <target> AS <query>;` writes
        \\  see language.md for the dialect
        \\
        \\sources and sinks:
        \\  files      CSV and Parquet, by path or URL — the extension picks the format;
        \\             another extension needs WITH (format = 'csv'|'parquet').
        \\             WITH (delimiter = ';', encoding = 'latin1') for non-comma,
        \\             non-UTF-8 CSV (also cp1252; delimiter works on a sink too)
        \\  object     az://<account>/<container>/<path> or s3://<bucket>/<key>, and a
        \\             trailing / reads every object under that prefix as one table
        \\  databases  postgres, mysql, sqlserver, starrocks (CREATE CONNECTION ... TYPE ...)
        \\  http       REST sources and sinks; `request` for an HTTP request body
        \\  buffer     durable WAL buffer, replayed by a later run
        \\
        \\credentials:
        \\  a connection named `erp` reads ERP_USER / ERP_PASS from the environment;
        \\  explicit user = ... / password = ... override it. Azure uses AZURE_STORAGE_KEY
        \\  (and AZURE_BLOB_ENDPOINT to point at an emulator); S3 uses AWS_ACCESS_KEY_ID /
        \\  AWS_SECRET_ACCESS_KEY (+ AWS_SESSION_TOKEN, AWS_REGION, and AWS_ENDPOINT_URL
        \\  for MinIO and friends).
        \\
        \\options:
        \\  -p, --param k=v    bind a PARAM declared by the script (repeatable)
        \\  -j, --threads N    parallelism: key-range lanes for a splittable SQL read,
        \\                     byte-range chunks for a local CSV, and row-group morsels
        \\                     for a Parquet — over aggregate / distinct / top-N / join /
        \\                     map-only pipelines. `EXPLAIN` names which one a query gets.
        \\                     (default: CPU count; map output may reorder under -j>1,
        \\                     so -j 1 is the stable-order choice. A float SUM is
        \\                     reproducible for a given -j but not across values of it —
        \\                     CAST to DECIMAL for a total that never varies.)
        \\  --port N           listen port for HTTP mode
        \\  --format FMT       table|json — stdout as machine-readable JSON: NDJSON
        \\                     rows for a SELECT, a summary object for a LOAD run
        \\  --log-format FMT   text|json — stderr log format (default text;
        \\                     json is NDJSON, one object per line, for collectors)
        \\  --log-level LVL    error|warn|info|debug (default warn)
        \\  -q, --quiet        suppress warnings too: errors only (the run summary
        \\                     still prints)
        \\
    );
}
