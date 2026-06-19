//! Command-line surface:
//!   basalt run   <script>|-c <script> [-p k=v ...] [-j N] [--port N]
//!   basalt check <script>|-c <script> [-s|--show-plan] [--connect]
//!   basalt repl
//! `run` executes (mode from the script's `@kind`); `check` validates/plans. A
//! script comes from a file path or, with `-c/--command`, inline. `repl` is an
//! interactive loop that prints results via the `write stdout` table sink.

const std = @import("std");
const parser = @import("../lang/parser.zig");
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
const Source = struct { label: []const u8, text: []const u8 };

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
    // `-` reads the script from stdin, so any delivery mechanism can pipe it
    // without a temp file (`cat x.bsl | basalt run -`, a control-plane fetch, etc.).
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
    return Source{ .label = path, .text = text };
}

/// Parse a resolved source, printing a located diagnostic on failure. The AST is
/// allocated in `arena` and slices into `src.text`, so both must outlive use.
fn parseSrc(arena: std.mem.Allocator, src: Source, stderr: *std.Io.Writer) !?ast.Program {
    var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    return parser.parseSource(arena, src.text, &diag) catch |e| switch (e) {
        error.ParseFailed => {
            try stderr.print("{s}:{d}:{d}: error: {s}\n", .{ src.label, diag.line, diag.col, diag.msg });
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

    var show_plan = false;
    var connect = false;
    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "-c") or std.mem.eql(u8, arg, "--command")) {
            i += 1; // skip inline script value
            continue;
        }
        if (std.mem.eql(u8, arg, "-s") or std.mem.eql(u8, arg, "--show-plan")) show_plan = true;
        if (std.mem.eql(u8, arg, "--connect")) connect = true;
    }

    // Offline: structure + reference + param validation, plus type-flow where the
    // schema is local (CSV). `--connect` also reaches DB sources to resolve their
    // schemas and type-check the full pipeline.
    var galloc = alloc;
    const resolver: ?analyze.Resolver = if (connect) runtime.connectingResolver(&galloc) else null;
    var adiag = analyze.Diag{};
    const plan = analyze.analyze(a, prog, resolver, &adiag) catch |e| switch (e) {
        error.OutOfMemory => return e,
        error.AnalyzeFailed => {
            try stderr.print("{s}: error: {s}\n", .{ src.label, adiag.msg });
            return 1;
        },
    };

    if (show_plan) {
        try analyze.render(plan, stdout);
    } else {
        try stdout.print("ok: {s} checks out\n", .{src.label});
    }
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

    // collect `-p key=value` params and `--port N`
    var params = std.array_list.Managed(runtime.ParamArg).init(alloc);
    defer params.deinit();
    var port: u16 = 8080;
    // Default to the core count, but threads are only *used* when the source is
    // splittable (a SQL table with a discoverable key, or a query with @[split]);
    // non-splittable sources (CSV, request) run serial regardless, so this never
    // regresses the local single-stream case.
    var threads: usize = std.Thread.getCpuCount() catch 1;
    var log = runtime.LogConfig{};
    var json_summary = false;
    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        const a = args[i];
        if (std.mem.eql(u8, a, "-c") or std.mem.eql(u8, a, "--command")) {
            i += 1; // skip inline script value
            continue;
        }
        if (threadFlagValue(a, args, &i)) |tv| {
            threads = std.fmt.parseInt(usize, tv, 10) catch {
                try stderr.print("error: invalid --threads `{s}`\n", .{tv});
                return 2;
            };
            if (threads == 0) threads = 1;
        } else if (std.mem.eql(u8, a, "--json")) {
            json_summary = true;
        } else if (std.mem.eql(u8, a, "--quiet") or std.mem.eql(u8, a, "-q")) {
            log.quiet = true;
        } else if (std.mem.eql(u8, a, "--log-format")) {
            i += 1;
            if (i >= args.len) {
                try stderr.print("error: missing value after `--log-format`\n", .{});
                return 2;
            }
            log.format = if (std.mem.eql(u8, args[i], "text")) .text else if (std.mem.eql(u8, args[i], "json")) .json else if (std.mem.eql(u8, args[i], "auto")) .auto else {
                try stderr.print("error: --log-format must be auto|text|json\n", .{});
                return 2;
            };
        } else if (std.mem.eql(u8, a, "--log-level")) {
            i += 1;
            if (i >= args.len) {
                try stderr.print("error: missing value after `--log-level`\n", .{});
                return 2;
            }
            log.level = obs.Level.parse(args[i]) orelse {
                try stderr.print("error: --log-level must be error|warn|info|debug\n", .{});
                return 2;
            };
        } else if (std.mem.eql(u8, a, "-p") or std.mem.eql(u8, a, "--param")) {
            i += 1;
            if (i >= args.len) {
                try stderr.print("error: missing value after `{s}`\n", .{a});
                return 2;
            }
            const kv = args[i];
            const eqp = std.mem.indexOfScalar(u8, kv, '=') orelse {
                try stderr.print("error: param must be key=value, got `{s}`\n", .{kv});
                return 2;
            };
            try params.append(.{ .key = kv[0..eqp], .val = kv[eqp + 1 ..] });
        } else if (std.mem.eql(u8, a, "--port")) {
            i += 1;
            if (i >= args.len) {
                try stderr.print("error: missing value after `--port`\n", .{});
                return 2;
            }
            port = std.fmt.parseInt(u16, args[i], 10) catch {
                try stderr.print("error: invalid --port `{s}`\n", .{args[i]});
                return 2;
            };
        }
    }

    // @http scripts become a server; @batch runs once.
    if (prog.stmts.len > 0 and prog.stmts[0] == .kind and prog.stmts[0].kind.kind == .http) {
        server.serve(alloc, prog, port) catch |e| {
            try stderr.print("{s}: serve error: {s}\n", .{ src.label, @errorName(e) });
            return 1;
        };
        return 0;
    }

    // The summary is the run's result: --json emits it to stdout, otherwise it's
    // rendered on stderr (text on a TTY, NDJSON when piped). run() owns rendering.
    log.summary = if (json_summary) .json_stdout else .stderr;

    var diag: runtime.Diag = .{};
    var sink = runtime.OutcomeSink.init(alloc);
    defer sink.deinit();
    _ = runtime.run(alloc, prog, .{ .params = params.items, .threads = threads, .outcomes = &sink, .log = log }, &diag) catch |e| switch (e) {
        error.Aborted => {
            // Cancelled by the control plane (SIGTERM/SIGINT). 130 = 128 + SIGINT.
            try stderr.print("{s}: aborted\n", .{src.label});
            return 130;
        },
        error.PlanFailed => {
            const tag = if (diag.retryable) " (transient)" else "";
            try stderr.print("{s}: error{s}: {s}\n", .{ src.label, tag, diag.msg });
            // Exit 75 (EX_TEMPFAIL) on a transient failure so the control plane can
            // retry; 1 on a permanent failure where a retry would fail identically.
            return if (diag.retryable) 75 else 1;
        },
        error.OutOfMemory => return e,
        else => {
            const transient = diag.retryable or runtime.isTransient(e);
            const tag = if (transient) " (transient)" else "";
            // run() fills diag.msg with stage/column context for expression errors.
            if (diag.msg.len > 0)
                try stderr.print("{s}: error{s}: {s}\n", .{ src.label, tag, diag.msg })
            else
                try stderr.print("{s}: runtime error{s}: {s}\n", .{ src.label, tag, @errorName(e) });
            return if (transient) 75 else 1;
        },
    };
    // Continue-mode fan-out (e.g. `for ... @[on_error = continue]`) succeeds as a
    // run even with per-item failures. Report them and exit non-zero so the control
    // plane notices: 75 if every failure was transient (retry the batch), else 1.
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
            i += 1;
            if (i >= args.len) {
                try stderr.print("error: missing value after `--port`\n", .{});
                return 2;
            }
            port = std.fmt.parseInt(u16, args[i], 10) catch {
                try stderr.print("error: invalid --port `{s}`\n", .{args[i]});
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

/// Interactive read-eval-print loop. Each entry is one or more lines terminated by
/// a blank line (so multi-stage pipelines can span lines); `@batch` is assumed and
/// a `write stdout` table sink is appended when the entry doesn't write itself.
/// Reads from stdin (so `echo ... | basalt repl` works); prompts only on a TTY.
fn cmdRepl(alloc: std.mem.Allocator) !u8 {
    var in_buf: [64 * 1024]u8 = undefined;
    var in_file = std.fs.File.stdin().reader(&in_buf);
    const in = &in_file.interface;

    var msg_buf: [4096]u8 = undefined;
    var msg_file = std.fs.File.stderr().writer(&msg_buf);
    const msg = &msg_file.interface;

    const tty = std.posix.isatty(std.fs.File.stdin().handle);
    if (tty) {
        try msg.writeAll("basalt REPL — enter a pipeline, blank line runs it. \\q quits, \\help for help.\n");
        try msg.flush();
    }

    var block = std.array_list.Managed(u8).init(alloc);
    defer block.deinit();

    while (true) {
        if (tty) {
            try msg.writeAll("\xc2\xbb "); // "» "
            try msg.flush();
        }
        block.clearRetainingCapacity();
        var eof = false;
        while (true) {
            const maybe = in.takeDelimiter('\n') catch |e| {
                try msg.print("input error: {s}\n", .{@errorName(e)});
                try msg.flush();
                eof = true;
                break;
            };
            const line = maybe orelse {
                eof = true;
                break;
            };
            if (isBlank(line)) break;
            try block.appendSlice(line);
            try block.append('\n');
        }

        const trimmed = std.mem.trim(u8, block.items, " \t\r\n");
        if (trimmed.len == 0) {
            if (eof) break;
            continue;
        }
        if (isQuit(trimmed)) break;
        if (isHelp(trimmed)) {
            try replHelp(msg);
            if (eof) break;
            continue;
        }

        try runBlock(alloc, trimmed, msg);
        if (eof) break;
    }
    if (tty) {
        try msg.writeAll("bye\n");
        try msg.flush();
    }
    return 0;
}

/// Parse and run one REPL entry, reporting errors without aborting the loop.
fn runBlock(alloc: std.mem.Allocator, block: []const u8, msg: *std.Io.Writer) !void {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();

    // Assume @batch unless the entry opens with its own @kind tag.
    const text = if (block[0] == '@')
        block
    else
        try std.fmt.allocPrint(a, "@batch\n{s}", .{block});

    var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = parser.parseSource(a, text, &diag) catch |e| switch (e) {
        error.ParseFailed => {
            try msg.print("error: {d}:{d}: {s}\n", .{ diag.line, diag.col, diag.msg });
            try msg.flush();
            return;
        },
        error.OutOfMemory => return e,
    };

    const prepared = try appendDisplaySinks(a, prog);

    var rdiag: runtime.Diag = .{};
    _ = runtime.run(alloc, prepared, .{ .log = .{ .summary = .none, .quiet = true } }, &rdiag) catch |e| {
        if (e == error.OutOfMemory) return e;
        if (rdiag.msg.len > 0)
            try msg.print("error: {s}\n", .{rdiag.msg})
        else
            try msg.print("error: {s}\n", .{@errorName(e)});
        try msg.flush();
    };
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

fn isBlank(s: []const u8) bool {
    return std.mem.trim(u8, s, " \t\r\n").len == 0;
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
        \\REPL — enter a pipeline, then a blank line to run it (results print as a table).
        \\  • `@batch` is assumed unless you start with a @kind tag.
        \\  • `| write stdout` is appended unless you write to a sink yourself.
        \\  example:  read csv "examples/in.csv" | filter status == "paid" | select id, amount
        \\  \q quit    \help this help
        \\
    );
    try msg.flush();
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

fn usage(w: anytype) !void {
    try w.writeAll(
        \\basalt — a DSL-driven data pipeline engine
        \\
        \\usage:
        \\  basalt run   <script>|-|-c <script> [-p key=value ...] [-j N] [--port N]  run a pipeline (mode from @kind)
        \\  basalt serve <dir> [--port N] [--watch]                  host every @http script in a dir (SIGHUP/-w reloads)
        \\  basalt check <script>|-|-c <script> [-s|--show-plan] [--connect]         validate; -s prints the plan
        \\  basalt repl                                              interactive read-eval-print loop
        \\
        \\  <script> may be a path, `-` for stdin, or `-c <script>` for an inline script
        \\  use `write stdout` to print results as a table (the REPL appends it for you)
        \\  -j, --threads N    parallelism: key-range lanes for splittable SQL reads (map-only and
        \\                     aggregate), and byte-range workers for CSV aggregate / distinct /
        \\                     top-N (sort|limit) / map-only pipelines
        \\                     (default: CPU count; map output may reorder under -j>1 — -j 1 = stable)
        \\  --json             emit the run summary as JSON on stdout (machine output)
        \\  --log-format FMT   auto|text|json — logs to stderr (default auto: text on a TTY, NDJSON when piped)
        \\  --log-level LVL    error|warn|info|debug (default info)
        \\  -q, --quiet        suppress info/warn logs (the run summary still prints)
        \\  basalt help                                             show this help
        \\
    );
}
