//! Command-line surface. One verb dispatches on the script's `@kind` meta tag:
//!   pipeline run   <script> [-p k=v ...] [--port N]
//!   pipeline check <script>
//! `check` is real (parse + report). `run` parses then reports that execution
//! lands in M2.

const std = @import("std");
const parser = @import("../lang/parser.zig");
const ast = @import("../lang/ast.zig");
const runtime = @import("../runtime/run.zig");
const obs = @import("../runtime/obs.zig");
const analyze = @import("../runtime/analyze.zig");
const server = @import("../server/http.zig");

pub fn run(alloc: std.mem.Allocator) !void {
    const args = try std.process.argsAlloc(alloc);
    defer std.process.argsFree(alloc, args);

    const stderr = std.io.getStdErr().writer();

    if (args.len < 2) {
        try usage(stderr);
        std.process.exit(2);
    }

    const verb = args[1];
    if (std.mem.eql(u8, verb, "check")) {
        std.process.exit(try cmdCheck(alloc, args));
    } else if (std.mem.eql(u8, verb, "run")) {
        std.process.exit(try cmdRun(alloc, args));
    } else if (std.mem.eql(u8, verb, "help") or std.mem.eql(u8, verb, "-h") or std.mem.eql(u8, verb, "--help")) {
        try usage(std.io.getStdOut().writer());
        return;
    }

    try stderr.print("error: unknown command `{s}`\n\n", .{verb});
    try usage(stderr);
    std.process.exit(2);
}

/// Parse <script>, printing diagnostics. Returns the parsed program on success
/// (allocated in `arena`, with the source kept alive in `arena` too since the AST
/// slices into it), or null after printing the error (caller exits 1).
fn parseFile(arena: std.mem.Allocator, verb: []const u8, args: [][:0]u8) !?ast.Program {
    const stderr = std.io.getStdErr().writer();
    if (args.len < 3) {
        try stderr.print("error: `{s}` requires a <script> path\n", .{verb});
        return null;
    }
    const script = args[2];
    const src = std.fs.cwd().readFileAlloc(arena, script, 8 << 20) catch |e| {
        try stderr.print("error: cannot read `{s}`: {s}\n", .{ script, @errorName(e) });
        return null;
    };

    var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    return parser.parseSource(arena, src, &diag) catch |e| switch (e) {
        error.ParseFailed => {
            try stderr.print("{s}:{d}:{d}: error: {s}\n", .{ script, diag.line, diag.col, diag.msg });
            return null;
        },
        error.OutOfMemory => return e,
    };
}

fn cmdCheck(alloc: std.mem.Allocator, args: [][:0]u8) !u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const prog = (try parseFile(a, "check", args)) orelse return 1;

    var show_plan = false;
    var connect = false;
    for (args[3..]) |arg| {
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
            try std.io.getStdErr().writer().print("{s}: error: {s}\n", .{ args[2], adiag.msg });
            return 1;
        },
    };

    const stdout = std.io.getStdOut().writer();
    if (show_plan) {
        try analyze.render(plan, stdout);
    } else {
        try stdout.print("ok: {s} checks out\n", .{args[2]});
    }
    return 0;
}

fn cmdRun(alloc: std.mem.Allocator, args: [][:0]u8) !u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const prog = (try parseFile(arena.allocator(), "run", args)) orelse return 1;

    const stderr = std.io.getStdErr().writer();

    // collect `-p key=value` params and `--port N`
    var params = std.ArrayList(runtime.ParamArg).init(alloc);
    defer params.deinit();
    var port: u16 = 8080;
    // Default to the core count, but threads are only *used* when the source is
    // splittable (a SQL table with a discoverable key, or a query with @[split]);
    // non-splittable sources (CSV, request) run serial regardless, so this never
    // regresses the local single-stream case.
    var threads: usize = std.Thread.getCpuCount() catch 1;
    var log = runtime.LogConfig{};
    var json_summary = false;
    var i: usize = 3;
    while (i < args.len) : (i += 1) {
        const a = args[i];
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

    // @http scripts become a server; @batch/@stream run once.
    if (prog.stmts.len > 0 and prog.stmts[0] == .kind and prog.stmts[0].kind.kind == .http) {
        server.serve(alloc, prog, port) catch |e| {
            try stderr.print("{s}: serve error: {s}\n", .{ args[2], @errorName(e) });
            return 1;
        };
        return 0;
    }

    // The summary is the run's result: --json emits it to stdout, otherwise it's
    // rendered on stderr (text on a TTY, NDJSON when piped). run() owns rendering.
    log.summary = if (json_summary) .json_stdout else .stderr;

    var diag: runtime.Diag = .{};
    _ = runtime.run(alloc, prog, .{ .params = params.items, .threads = threads, .log = log }, &diag) catch |e| switch (e) {
        error.PlanFailed => {
            try stderr.print("{s}: error: {s}\n", .{ args[2], diag.msg });
            return 1;
        },
        error.OutOfMemory => return e,
        else => {
            // run() fills diag.msg with stage/column context for expression errors.
            if (diag.msg.len > 0)
                try stderr.print("{s}: error: {s}\n", .{ args[2], diag.msg })
            else
                try stderr.print("{s}: runtime error: {s}\n", .{ args[2], @errorName(e) });
            return 1;
        },
    };
    return 0;
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
        \\pipeline — a DSL-driven data pipeline engine
        \\
        \\usage:
        \\  pipeline run   <script> [-p key=value ...] [-j N] [--port N]   run a pipeline (mode from @kind)
        \\  pipeline check <script> [-s|--show-plan] [--connect]         validate; -s prints the plan, --connect resolves DB schemas
        \\
        \\  -j, --threads N    lanes for splittable SQL sources (default: CPU count; non-split sources are serial)
        \\  --json             emit the run summary as JSON on stdout (machine output)
        \\  --log-format FMT   auto|text|json — logs to stderr (default auto: text on a TTY, NDJSON when piped)
        \\  --log-level LVL    error|warn|info|debug (default info)
        \\  -q, --quiet        suppress info/warn logs (the run summary still prints)
        \\  pipeline help                                           show this help
        \\
    );
}
