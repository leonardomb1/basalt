//! Command-line surface. One verb dispatches on the script's `@kind` meta tag:
//!   pipeline run   <script> [-p k=v ...] [--port N]
//!   pipeline check <script>
//! `check` is real (parse + report). `run` parses then reports that execution
//! lands in M2.

const std = @import("std");
const parser = @import("../lang/parser.zig");
const ast = @import("../lang/ast.zig");
const runtime = @import("../runtime/run.zig");
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
    const prog = (try parseFile(arena.allocator(), "check", args)) orelse return 1;
    try std.io.getStdOut().writer().print(
        "ok: {s} parsed ({d} statements)\n",
        .{ args[2], prog.stmts.len },
    );
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
    var i: usize = 3;
    while (i < args.len) : (i += 1) {
        const a = args[i];
        if (std.mem.eql(u8, a, "-p") or std.mem.eql(u8, a, "--param")) {
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

    var diag: runtime.Diag = .{};
    const stats = runtime.run(alloc, prog, .{ .params = params.items }, &diag) catch |e| switch (e) {
        error.PlanFailed => {
            try stderr.print("{s}: error: {s}\n", .{ args[2], diag.msg });
            return 1;
        },
        error.OutOfMemory => return e,
        else => {
            try stderr.print("{s}: runtime error: {s}\n", .{ args[2], @errorName(e) });
            return 1;
        },
    };
    try std.io.getStdOut().writer().print("ok: {s} wrote {d} rows\n", .{ args[2], stats.rows_out });
    return 0;
}

fn usage(w: anytype) !void {
    try w.writeAll(
        \\pipeline — a DSL-driven data pipeline engine
        \\
        \\usage:
        \\  pipeline run   <script> [-p key=value ...] [--port N]   run a pipeline (mode from @kind)
        \\  pipeline check <script>                                 parse + type-check, no execution
        \\  pipeline help                                           show this help
        \\
    );
}
