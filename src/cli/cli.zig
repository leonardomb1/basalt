//! Command-line surface:
//!   basalt run   <script>|-c <script> [-p k=v ...] [-j N] [--port N]
//!   basalt check <script>|-c <script>
//!   basalt repl
//!   basalt kernel
//! `run` executes (HTTP mode when the script declares an endpoint); `check`
//! validates and plans without running. A
//! script comes from a file path or, with `-c/--command`, inline. `repl` is an
//! interactive loop that runs on `;`, carries declarations across entries, and
//! prints results via the `write stdout` table sink.

const std = @import("std");
const parser = @import("../lang/sql_parser.zig");
const include = @import("../lang/include.zig");
const Editor = @import("line.zig").Editor;
const LineResult = @import("line.zig").Result;
const view = @import("view.zig");
const table = @import("../connect/table.zig");
const ast = @import("../lang/ast.zig");
const aggregates = @import("../lang/aggregates.zig");
const runtime = @import("../runtime/run.zig");
const obs = @import("../runtime/obs.zig");
const analyze = @import("../runtime/analyze.zig");
const complete = @import("complete.zig");
const http_server = @import("../server/http_server.zig");
const kernel = @import("kernel.zig");
const eval = @import("../exec/eval.zig");
const Value = @import("../exec/value.zig").Value;
const types = @import("../lang/types.zig");

/// SIGTERM/SIGINT → ask the run to stop at its next boundary (async-signal-safe:
/// one atomic store). The control plane uses this to cancel a job or roll a http_server.
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
    } else if (std.mem.eql(u8, verb, "complete")) {
        std.process.exit(try cmdComplete(alloc, args));
    } else if (std.mem.eql(u8, verb, "kernel")) {
        std.process.exit(try kernel.cmdKernel(alloc, args));
    } else if (std.mem.eql(u8, verb, "repl")) {
        for (args[2..]) |a| if (try unknownOption(a, "repl", stderr)) std.process.exit(2);
        std.process.exit(try cmdRepl(alloc));
    } else if (std.mem.eql(u8, verb, "version") or std.mem.eql(u8, verb, "--version") or std.mem.eql(u8, verb, "-V")) {
        var stdout_buf: [256]u8 = undefined;
        var stdout_file = std.fs.File.stdout().writer(&stdout_buf);
        try stdout_file.interface.print("basalt {s}\n", .{@import("build_options").version});
        try stdout_file.interface.flush();
        return;
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
    const at = scriptArg(args) orelse {
        try stderr.print("error: `{s}` requires a <script> path, `-` for stdin, or `-c <script>`\n", .{verb});
        return null;
    };
    if (std.mem.eql(u8, args[at], "-")) {
        const text = std.fs.File.stdin().readToEndAlloc(arena, 8 << 20) catch |e| {
            try stderr.print("error: cannot read script from stdin: {s}\n", .{@errorName(e)});
            return null;
        };
        return Source{ .label = "<stdin>", .text = text };
    }
    const path = args[at];
    const text = std.fs.cwd().readFileAlloc(arena, path, 8 << 20) catch |e| {
        try stderr.print("error: cannot read `{s}`: {s}\n", .{ path, @errorName(e) });
        return null;
    };
    return Source{ .label = path, .text = text, .dir = std.fs.path.dirname(path) orelse "." };
}

/// Flags that take the next argument as their value, across `run` and `check`.
const valued_flags = [_][]const u8{ "-p", "--param", "-j", "--threads", "--format", "--log-format", "--log-level", "--port", "--max-rows", "--pos" };

/// Index of the script argument: the first that is neither a flag nor a flag's
/// value, `-` (stdin) included — so `run --format json x.sql` finds `x.sql`, as
/// `run x.sql --format json` always did.
fn scriptArg(args: [][:0]u8) ?usize {
    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        const a = args[i];
        if (std.mem.eql(u8, a, "-")) return i;
        if (a.len > 0 and a[0] == '-') {
            for (valued_flags) |f| if (std.mem.eql(u8, a, f)) {
                i += 1;
                break;
            };
            continue;
        }
        return i;
    }
    return null;
}

test "scriptArg: the script is found whatever order flags and it come in" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const Case = struct { argv: []const []const u8, want: ?usize };
    const cases = [_]Case{
        .{ .argv = &.{ "basalt", "run", "x.sql", "--format", "json" }, .want = 2 },
        .{ .argv = &.{ "basalt", "run", "--format", "json", "x.sql" }, .want = 4 },
        .{ .argv = &.{ "basalt", "run", "--format", "json", "-" }, .want = 4 },
        .{ .argv = &.{ "basalt", "run", "-p", "k=v", "-j", "4", "--quiet", "x.sql" }, .want = 7 },
        .{ .argv = &.{ "basalt", "run", "-j8", "x.sql" }, .want = 3 },
        .{ .argv = &.{ "basalt", "run", "--format", "json" }, .want = null },
        // a flag's value that happens to look like a path is still its value
        .{ .argv = &.{ "basalt", "check", "-p", "out.sql" }, .want = null },
    };
    for (cases) |c| try std.testing.expectEqual(c.want, scriptArg(try testArgv(a, c.argv)));
}

/// A dash-prefixed argument no branch claimed. `-` alone is the stdin script,
/// not an option. Reports it and returns true so the caller can exit 2 — a typo
/// like `--treads` used to run single-threaded without a word.
fn unknownOption(arg: []const u8, verb: []const u8, stderr: *std.Io.Writer) !bool {
    if (arg.len < 2 or arg[0] != '-') return false;
    try stderr.print("error: unknown option `{s}` for `{s}` — see `basalt help`\n", .{ arg, verb });
    return true;
}

pub fn parseLogFormat(v: []const u8) ?obs.Format {
    if (std.mem.eql(u8, v, "text")) return .text;
    if (std.mem.eql(u8, v, "json")) return .json;
    if (std.mem.eql(u8, v, "auto")) return .auto;
    return null;
}

/// `label:line:col: error: msg` when the diagnostic carries a position, else
/// `label: error: msg` — the same shape parse errors already print.
fn printDiag(stderr: *std.Io.Writer, label: []const u8, tag: []const u8, pos: ?ast.Pos, msg: []const u8) !void {
    if (pos) |p|
        try stderr.print("{s}:{d}:{d}: error{s}: {s}\n", .{ label, p.line, p.col, tag, msg })
    else
        try stderr.print("{s}: error{s}: {s}\n", .{ label, tag, msg });
}

/// Where a script's errors go: the located text line, or under `--log-format
/// json` one NDJSON object in the log's own shape, with the span an editor
/// underlines and whether a retry could help.
const ErrOut = struct {
    w: *std.Io.Writer,
    json: bool,
    label: []const u8,
    /// `check --format json`: the object alone, an element of the caller's
    /// array, without the log envelope (`ts`, `event`).
    bare: bool = false,

    const Located = struct {
        msg: []const u8,
        pos: ?ast.Pos = null,
        end: ?ast.Pos = null,
        /// A file other than the script itself, e.g. an `@include`d one.
        file: ?[]const u8 = null,
        transient: bool = false,
        event: []const u8 = "script_error",
    };

    fn report(self: ErrOut, e: Located) !void {
        const file = e.file orelse self.label;
        if (!self.json) {
            return printDiag(self.w, file, if (e.transient) " (transient)" else "", e.pos, e.msg);
        }
        if (self.bare)
            try self.w.writeAll("{\"level\":\"error\",\"msg\":")
        else
            try self.w.print("{{\"ts\":{d},\"level\":\"error\",\"event\":\"{s}\",\"msg\":", .{ std.time.milliTimestamp(), e.event });
        try std.json.Stringify.encodeJsonString(e.msg, .{}, self.w);
        try self.w.writeAll(",\"file\":");
        try std.json.Stringify.encodeJsonString(file, .{}, self.w);
        if (e.pos) |p| {
            try self.w.print(",\"line\":{d},\"col\":{d}", .{ p.line, p.col });
            if (e.end) |x| try self.w.print(",\"end_line\":{d},\"end_col\":{d}", .{ x.line, x.col });
        }
        try self.w.print(",\"class\":\"{s}\"}}", .{if (e.transient) "transient" else "permanent"});
        if (!self.bare) try self.w.writeByte('\n');
    }
};

/// `--log-format json` anywhere on the command line, read before the script is
/// parsed so a parse error is reported in the same shape as a runtime one.
fn wantsJsonLog(args: [][:0]u8) bool {
    var i: usize = 2;
    while (i + 1 < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--log-format")) return std.mem.eql(u8, args[i + 1], "json");
    }
    return false;
}

/// Parse a resolved source (resolving its `@include` header first), printing a
/// located diagnostic on failure. The AST is allocated in `arena` and slices into
/// `src.text` and the included files' texts, so all must outlive use. The
/// diagnostic names the file it came from — an included file reports its own
/// path and its own line numbers.
fn parseSrc(arena: std.mem.Allocator, src: Source, stderr: *std.Io.Writer) !?ast.Program {
    return parseSrcTo(arena, src, .{ .w = stderr, .json = false, .label = src.label });
}

fn parseSrcTo(arena: std.mem.Allocator, src: Source, eo: ErrOut) !?ast.Program {
    var diag: include.Diag = .{};
    return include.loadProgram(arena, src.text, src.label, src.dir, &diag) catch |e| switch (e) {
        error.ParseFailed => {
            const p = diag.parse;
            try eo.report(.{
                .msg = p.msg,
                .pos = .{ .line = p.line, .col = p.col },
                .end = if (p.end_line > 0) ast.Pos{ .line = p.end_line, .col = p.end_col } else null,
                .file = if (diag.label.len > 0) diag.label else null,
                .event = "parse_error",
            });
            return null;
        },
        error.OutOfMemory => return e,
    };
}

/// One problem `check` found, located in the file it names (null: the script).
pub const CheckIssue = struct { msg: []const u8, pos: ?ast.Pos = null, end: ?ast.Pos = null, file: ?[]const u8 = null };

pub const CheckOpts = struct {
    overrides: []const analyze.ParamOverride = &.{},
    known: []const analyze.KnownTable = &.{},
    /// A script of declarations alone checks out — a notebook cell.
    declarations_only: bool = false,
};

/// Every problem in `text`, in script order: each statement that does not parse
/// (parsing resumes at the next one), then each that does not check. Nothing
/// runs and nothing is connected to.
pub fn checkText(a: std.mem.Allocator, text: []const u8, label: []const u8, dir: []const u8, opts: CheckOpts) ![]CheckIssue {
    var issues = std.array_list.Managed(CheckIssue).init(a);
    var names = std.array_list.Managed([]const u8).init(a);
    for (opts.known) |k| try names.append(k.name);
    var perrs = std.array_list.Managed(parser.Diagnostic).init(a);
    var idiag: include.Diag = .{};
    const prog = include.loadProgramOpts(a, text, label, dir, &idiag, .{ .known_tables = names.items, .errors = &perrs }) catch |e| switch (e) {
        error.OutOfMemory => return e,
        // what recovery cannot pass: an unlexable token, an `@include` that fails
        error.ParseFailed => {
            const p = idiag.parse;
            try issues.append(.{ .msg = p.msg, .pos = .{ .line = p.line, .col = p.col }, .end = if (p.end_line > 0) ast.Pos{ .line = p.end_line, .col = p.end_col } else null, .file = if (idiag.label.len > 0 and !std.mem.eql(u8, idiag.label, label)) idiag.label else null });
            return issues.toOwnedSlice();
        },
    };
    for (perrs.items) |p| try issues.append(.{ .msg = p.msg, .pos = .{ .line = p.line, .col = p.col }, .end = if (p.end_line > 0) ast.Pos{ .line = p.end_line, .col = p.end_col } else null });

    var found = std.array_list.Managed(analyze.Issue).init(a);
    var adiag = analyze.Diag{};
    _ = try analyze.analyzeOpts(a, prog, .{
        .overrides = opts.overrides,
        .known_tables = opts.known,
        .issues = &found,
        // statements that did not parse are gone; "no output" would be about them
        .declarations_only = opts.declarations_only or perrs.items.len > 0,
    }, &adiag);
    for (found.items) |f| try issues.append(.{ .msg = f.msg, .pos = f.pos, .end = f.end });

    std.mem.sort(CheckIssue, issues.items, {}, struct {
        fn lt(_: void, x: CheckIssue, y: CheckIssue) bool {
            const px = x.pos orelse ast.Pos{ .line = 0, .col = 0 };
            const py = y.pos orelse ast.Pos{ .line = 0, .col = 0 };
            return px.line < py.line or (px.line == py.line and px.col < py.col);
        }
    }.lt);
    return issues.toOwnedSlice();
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

    var overrides = std.array_list.Managed(analyze.ParamOverride).init(a);
    var known = std.array_list.Managed(analyze.KnownTable).init(a);
    var json = false;
    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "-c") or std.mem.eql(u8, args[i], "--command")) {
            i += 1;
            continue;
        }
        if (std.mem.eql(u8, args[i], "--format")) {
            const v = (try nextVal(args, &i, "--format", stderr)) orelse return 2;
            if (std.mem.eql(u8, v, "json")) json = true else if (!std.mem.eql(u8, v, "text")) {
                try stderr.print("error: `check --format` must be text|json\n", .{});
                return 2;
            }
            continue;
        }
        if (std.mem.eql(u8, args[i], "--known")) {
            // tables that exist where the script runs, which it does not declare
            const v = (try nextVal(args, &i, "--known", stderr)) orelse return 2;
            var it = std.mem.splitScalar(u8, v, ',');
            while (it.next()) |n| {
                const name = std.mem.trim(u8, n, " ");
                if (name.len > 0) try known.append(.{ .name = name });
            }
            continue;
        }
        if (!std.mem.eql(u8, args[i], "-p") and !std.mem.eql(u8, args[i], "--param")) {
            if (try unknownOption(args[i], "check", stderr)) return 2;
            continue;
        }
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

    const src = (try loadSource(a, "check", args, stderr)) orelse return 1;
    // `--format json`: stdout is an array of diagnostics — empty when the script
    // checks out — for an editor to place; the text lines stay on stderr.
    const eo = ErrOut{ .w = if (json) stdout else stderr, .json = json, .bare = true, .label = src.label };
    const issues = try checkText(a, src.text, src.label, src.dir, .{ .overrides = overrides.items, .known = known.items });
    if (json) try stdout.writeAll("[");
    for (issues, 0..) |is, k| {
        if (json and k > 0) try stdout.writeByte(',');
        try eo.report(.{ .msg = is.msg, .pos = is.pos, .end = is.end, .file = is.file });
    }
    if (json) try stdout.writeAll("]\n");
    if (issues.len > 0) return 1;
    if (!json) try stdout.print("ok: {s} checks out\n", .{src.label});
    return 0;
}

/// `complete <script>|-|-c <script> --pos N [--connect]`: what Tab would offer
/// at byte offset N, as JSON — `{"start":S,"end":N,"items":[{"text","kind"}]}`,
/// the candidates replacing `script[S..N]`. Declarations come from the script
/// itself; local files contribute their columns; connections are asked for
/// their tables and columns only under `--connect`, since that means a round
/// trip to each.
fn cmdComplete(alloc: std.mem.Allocator, args: [][:0]u8) !u8 {
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

    var pos: ?usize = null;
    var connect = false;
    var utf16 = false;
    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "-c") or std.mem.eql(u8, arg, "--command")) {
            i += 1;
        } else if (std.mem.eql(u8, arg, "--pos")) {
            const v = (try nextVal(args, &i, "--pos", stderr)) orelse return 2;
            pos = std.fmt.parseInt(usize, v, 10) catch {
                try stderr.print("error: invalid --pos `{s}`\n", .{v});
                return 2;
            };
        } else if (std.mem.eql(u8, arg, "--connect")) {
            connect = true;
        } else if (std.mem.eql(u8, arg, "--utf16")) {
            utf16 = true;
        } else if (try unknownOption(arg, "complete", stderr)) return 2;
    }
    const src = (try loadSource(a, "complete", args, stderr)) orelse return 1;
    const at = if (pos) |p| (if (utf16) utf16ToByte(src.text, p) else p) else src.text.len;
    if (at > src.text.len) {
        try stderr.print("error: --pos {d} is past the script's {d} bytes\n", .{ at, src.text.len });
        return 2;
    }

    var decls = DeclStore.init(alloc);
    defer decls.deinit();
    try declareFrom(&decls, a, src.text);
    var catalog = Catalog.init(alloc);
    defer catalog.deinit();
    const cx = Completer{ .gpa = alloc, .decls = &decls, .catalog = &catalog, .connect = connect };
    const offer = try suggestFor(a, &cx, src.text, at);
    try writeOfferIn(stdout, offer, at, src.text, utf16);
    try stdout.writeByte('\n');
    return 0;
}

/// The declarations a text makes, by its statements' first words — so a script
/// still being typed, which will not parse, still has its names in scope.
pub fn declareFrom(decls: *DeclStore, arena: std.mem.Allocator, text: []const u8) !void {
    for (try splitStatements(arena, text)) |st| {
        const id = declOf(st) orelse continue;
        if (id.kind == .endpoint) continue;
        try decls.put(id, st);
    }
}

/// Byte offset of UTF-16 offset `u` in `text` — where a JavaScript editor's
/// cursor is. An offset inside a surrogate pair, or past the end, clamps to the
/// character boundary before it; bytes that are not UTF-8 count one unit each.
pub fn utf16ToByte(text: []const u8, u: usize) usize {
    var i: usize = 0;
    var units: usize = 0;
    while (i < text.len) {
        const n = std.unicode.utf8ByteSequenceLength(text[i]) catch 1;
        const cp_units: usize = if (n == 4) 2 else 1;
        if (units + cp_units > u) break;
        units += cp_units;
        i = @min(text.len, i + n);
    }
    return i;
}

/// UTF-16 offset of byte `b` in `text`: `utf16ToByte`'s inverse.
pub fn byteToUtf16(text: []const u8, b: usize) usize {
    var i: usize = 0;
    var units: usize = 0;
    while (i < @min(b, text.len)) {
        const n = std.unicode.utf8ByteSequenceLength(text[i]) catch 1;
        units += if (n == 4) 2 else 1;
        i += n;
    }
    return units;
}

/// `{"start":S,"end":N,"items":[{"text":…,"kind":…[,"detail":…]}]}`. With `utf16`, the
/// offsets are counted in UTF-16 units of `text`, as a JavaScript editor counts.
pub fn writeOfferIn(w: *std.Io.Writer, offer: Offer, cursor: usize, text: []const u8, utf16: bool) !void {
    var o = offer;
    var c = cursor;
    if (utf16) {
        o.start = byteToUtf16(text, offer.start);
        c = byteToUtf16(text, cursor);
    }
    return writeOffer(w, o, c);
}

/// `{"start":S,"end":N,"items":[{"text":…,"kind":…[,"detail":…]}]}` — `detail` only
/// when there is one: a built-in function's signature, a column's type.
pub fn writeOffer(w: *std.Io.Writer, offer: Offer, cursor: usize) !void {
    try w.print("{{\"start\":{d},\"end\":{d},\"items\":[", .{ offer.start, cursor });
    for (offer.items, 0..) |c, k| {
        if (k > 0) try w.writeByte(',');
        try w.writeAll("{\"text\":");
        try std.json.Stringify.encodeJsonString(c.text, .{}, w);
        try w.print(",\"kind\":\"{s}\"", .{@tagName(c.kind)});
        if (c.detail.len > 0) {
            try w.writeAll(",\"detail\":");
            try std.json.Stringify.encodeJsonString(c.detail, .{}, w);
        }
        try w.writeByte('}');
    }
    try w.writeAll("]}");
}

fn cmdRun(alloc: std.mem.Allocator, args: [][:0]u8) !u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();

    var stderr_buf: [4096]u8 = undefined;
    var stderr_file = std.fs.File.stderr().writer(&stderr_buf);
    const stderr = &stderr_file.interface;
    defer stderr.flush() catch {};

    const src = (try loadSource(arena.allocator(), "run", args, stderr)) orelse return 1;
    const eo = ErrOut{ .w = stderr, .json = wantsJsonLog(args), .label = src.label };
    const prog = (try parseSrcTo(arena.allocator(), src, eo)) orelse return 1;

    var params = std.array_list.Managed(runtime.ParamArg).init(alloc);
    defer params.deinit();
    var port: u16 = 8080;
    var threads: usize = std.Thread.getCpuCount() catch 1;
    var log = runtime.LogConfig{};
    var level_set = false;
    var stdout_format: runtime.StdoutFormat = .table;
    var explain = false;
    var no_progress = false;
    var max_rows: ?u64 = null;
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
            stdout_format = std.meta.stringToEnum(runtime.StdoutFormat, v) orelse {
                try stderr.print("error: --format must be table|json|csv|tsv|arrow\n", .{});
                return 2;
            };
        } else if (std.mem.eql(u8, a, "--max-rows")) {
            const v = (try nextVal(args, &i, a, stderr)) orelse return 2;
            max_rows = std.fmt.parseInt(u64, v, 10) catch {
                try stderr.print("error: invalid --max-rows `{s}`\n", .{v});
                return 2;
            };
        } else if (std.mem.eql(u8, a, "--explain")) {
            explain = true;
        } else if (std.mem.eql(u8, a, "--no-progress")) {
            no_progress = true;
        } else if (std.mem.eql(u8, a, "--quiet") or std.mem.eql(u8, a, "-q")) {
            log.quiet = true;
        } else if (std.mem.eql(u8, a, "--log-format")) {
            const v = (try nextVal(args, &i, a, stderr)) orelse return 2;
            log.format = parseLogFormat(v) orelse {
                try stderr.print("error: --log-format must be auto|text|json\n", .{});
                return 2;
            };
        } else if (std.mem.eql(u8, a, "--log-level")) {
            const v = (try nextVal(args, &i, a, stderr)) orelse return 2;
            log.level = obs.Level.parse(v) orelse {
                try stderr.print("error: --log-level must be error|warn|info|debug\n", .{});
                return 2;
            };
            level_set = true;
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
        } else if (try unknownOption(a, "run", stderr)) return 2;
    }

    if (prog.stmts.len > 0 and prog.stmts[0] == .kind and prog.stmts[0].kind.kind == .http) {
        http_server.serve(alloc, prog, port, .{ .format = log.format, .level = if (level_set) log.level else .info, .quiet = log.quiet, .summary = .stderr }) catch |e| {
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
                try eo.report(.{ .msg = adiag2.msg, .pos = adiag2.pos, .end = adiag2.end });
                return 1;
            },
        };
        if (stdout_format == .arrow) {
            var aw = std.Io.Writer.Allocating.init(arena.allocator());
            try analyze.render(plan, &aw.writer);
            _ = try runtime.printPlanArrow(alloc, aw.written(), .{ .kind = "explain", .line = 1, .col = 1, .t0_ms = std.time.milliTimestamp() });
            return 0;
        }
        try analyze.render(plan, eout);
        return 0;
    }

    log.summary = if (stdout_format == .json) .json_stdout else .stderr;

    var diag: runtime.Diag = .{};
    var sink = runtime.OutcomeSink.init(alloc);
    defer sink.deinit();
    // A person watching a terminal gets the live line; under `--log-format json`
    // a reader of the log gets a `progress` event a second instead, terminal or
    // not. A plain pipe or file and `-q` get neither.
    const progress = !no_progress and !log.quiet and (log.format == .json or std.posix.isatty(std.fs.File.stderr().handle));
    _ = runtime.run(alloc, prog, .{ .params = params.items, .threads = threads, .outcomes = &sink, .log = log, .explain = explain or prog.explain == .analyze, .stdout_format = stdout_format, .progress = progress, .items = true, .max_rows = max_rows }, &diag) catch |e| switch (e) {
        error.Aborted => {
            if (eo.json)
                try eo.report(.{ .msg = "aborted", .event = "aborted" })
            else
                try stderr.print("{s}: aborted\n", .{src.label});
            return 130;
        },
        error.PlanFailed => {
            try eo.report(.{ .msg = diag.msg, .pos = diag.pos, .end = diag.end, .transient = diag.retryable });
            return if (diag.retryable) 75 else 1;
        },
        error.OutOfMemory => return e,
        else => {
            const transient = diag.retryable or runtime.isTransient(e);
            if (diag.msg.len > 0)
                try eo.report(.{ .msg = diag.msg, .pos = diag.pos, .end = diag.end, .transient = transient })
            else if (eo.json)
                try eo.report(.{ .msg = runtime.failLabel(e), .transient = transient })
            else
                try stderr.print("{s}: runtime error{s}: {s}\n", .{ src.label, if (transient) " (transient)" else "", runtime.failLabel(e) });
            return if (transient) 75 else 1;
        },
    };
    const nfail = sink.failures();
    if (nfail > 0) {
        var all_retryable = true;
        for (sink.list.items) |o| {
            if (o.ok) continue;
            if (!o.retryable) all_retryable = false;
            if (o.shown) continue;
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
    // A host's own lines (routes, reloads, flush failures) are its output, so
    // the default is `info` where a one-shot run defaults to `warn`.
    var log = runtime.LogConfig{ .level = .info, .summary = .stderr };
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
        } else if (std.mem.eql(u8, args[i], "--log-format")) {
            const v = (try nextVal(args, &i, "--log-format", stderr)) orelse return 2;
            log.format = parseLogFormat(v) orelse {
                try stderr.print("error: --log-format must be auto|text|json\n", .{});
                return 2;
            };
        } else if (std.mem.eql(u8, args[i], "--log-level")) {
            const v = (try nextVal(args, &i, "--log-level", stderr)) orelse return 2;
            log.level = obs.Level.parse(v) orelse {
                try stderr.print("error: --log-level must be error|warn|info|debug\n", .{});
                return 2;
            };
        } else if (std.mem.eql(u8, args[i], "--quiet") or std.mem.eql(u8, args[i], "-q")) {
            log.quiet = true;
        } else if (try unknownOption(args[i], "serve", stderr)) return 2;
    }

    http_server.serveDir(alloc, dir, port, watch, log) catch |e| {
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

/// What the editor asks on Enter, and the piped loop after each line: run this,
/// or wait for more? A meta command, a quit word and a blank entry are whole as
/// they stand. SQL is whole at a top-level `;` — unless the parser, given the
/// session's declarations, runs out of input first: a `CREATE FUNCTION ... AS`
/// whose body is still open ends in `;` several times before its `END;`.
fn entryComplete(ctx: *anyopaque, s: []const u8) bool {
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

pub const DeclKind = enum { connection, function, param, let, endpoint, resource };
pub const DeclId = struct { kind: DeclKind, name: []const u8 };

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
/// `CREATE [OR REPLACE] CONNECTION|FUNCTION <name>`, `CREATE RESOURCE <conn>.<name>`,
/// `CREATE ENDPOINT ...`
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
    if (std.ascii.eqlIgnoreCase(w, "resource")) {
        // Named `conn.name`: two resources of different connections may share a name.
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

/// Declarations carried across REPL entries, so `CREATE CONNECTION erp ...` in
/// one entry is still in scope for a `SELECT ... FROM erp.orders` in the next.
/// Order-preserving (declarations may reference earlier ones); re-declaring a
/// (kind, name) replaces the stored text in place. Text is duped with the
/// REPL's gpa because the input buffer is reused every line.
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

/// Mutable REPL state: the declaration prelude plus per-session toggles.
const Session = struct {
    decls: DeclStore,
    format: runtime.StdoutFormat = .table,
    tty: bool = false,
    /// What Tab has learned about the sources so far, for as long as the session.
    catalog: Catalog,
    /// The last entry run, for `\edit`.
    last_entry: ?[]u8 = null,
    /// Say `ok: connection x` as declarations register — off while the startup
    /// file loads, which is summed up in one line instead.
    announce: bool = true,

    fn completer(self: *Session) Completer {
        return .{ .gpa = self.decls.gpa, .decls = &self.decls, .catalog = &self.catalog };
    }
};

/// A path with the home directory folded to `~`, for messages.
fn tilde(buf: []u8, path: []const u8) []const u8 {
    const home = std.posix.getenv("HOME") orelse return path;
    if (home.len > 1 and std.mem.startsWith(u8, path, home) and path.len > home.len and path[home.len] == '/')
        return std.fmt.bufPrint(buf, "~{s}", .{path[home.len..]}) catch path;
    return path;
}

/// The REPL's opening: a small mark, the name and version, what the tool is,
/// and the two things a newcomer needs. Three lines — a prompt, not a splash.
fn banner(msg: *std.Io.Writer, color: bool) !void {
    const dim: []const u8 = if (color) "\x1b[2m" else "";
    const bold: []const u8 = if (color) "\x1b[1m" else "";
    const mark: []const u8 = if (color) "\x1b[38;5;208m" else "";
    const off: []const u8 = if (color) "\x1b[0m" else "";
    try msg.print("{s}  ▄▄▄ {s} {s}basalt{s} {s}{s}{s}\n", .{ mark, off, bold, off, dim, @import("build_options").version, off });
    try msg.print("{s}  ███ {s} {s}SQL in, rows moved: files, object stores and databases in one binary{s}\n", .{ mark, off, dim, off });
    try msg.print("{s}  ▀▀▀ {s} {s}\\help keys and commands · \\connect a new source · \\q quit{s}\n\n", .{ mark, off, dim, off });
}

/// `$XDG_CONFIG_HOME/basalt/repl.sql`, else `~/.config/basalt/repl.sql`: the
/// declarations a session starts with, and where `\save` writes.
fn startupPath(gpa: std.mem.Allocator) ?[]u8 {
    if (std.process.getEnvVarOwned(gpa, "XDG_CONFIG_HOME")) |x| {
        defer gpa.free(x);
        return std.fs.path.join(gpa, &.{ x, "basalt", "repl.sql" }) catch null;
    } else |_| {}
    const home = std.process.getEnvVarOwned(gpa, "HOME") catch return null;
    defer gpa.free(home);
    return std.fs.path.join(gpa, &.{ home, ".config", "basalt", "repl.sql" }) catch null;
}

/// The value of `key = '...'` in a `CREATE CONNECTION` text, or null.
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

/// `\connections`: one row per connection — its type, host and database as
/// declared, and what the session has learned: how many tables Tab or
/// `\connections test` found, or that it could not be reached.
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

/// What each connector needs, for `\connect`: the questions, in order, with the
/// default a newcomer would want. Mirrors the keys the runtime reads
/// (`parseDbConfig`, `resolveStreamLoadConfig`, `http_client.connFromKvs`).
const Connector = struct {
    name: []const u8,
    blurb: []const u8,
    fields: []const Field,
    const Field = struct { key: []const u8, prompt: []const u8, default: []const u8 = "", secret: bool = false, int: bool = false };
};
const connectors = [_]Connector{
    .{ .name = "postgres", .blurb = "PostgreSQL (source and sink)", .fields = &.{
        .{ .key = "host", .prompt = "host", .default = "localhost" },
        .{ .key = "port", .prompt = "port", .default = "5432", .int = true },
        .{ .key = "database", .prompt = "database" },
        .{ .key = "user", .prompt = "user" },
        .{ .key = "password", .prompt = "password", .secret = true },
        .{ .key = "tls", .prompt = "tls (off, require, insecure)", .default = "off" },
    } },
    .{ .name = "mysql", .blurb = "MySQL / MariaDB (source and sink)", .fields = &.{
        .{ .key = "host", .prompt = "host", .default = "localhost" },
        .{ .key = "port", .prompt = "port", .default = "3306", .int = true },
        .{ .key = "database", .prompt = "database" },
        .{ .key = "user", .prompt = "user" },
        .{ .key = "password", .prompt = "password", .secret = true },
        .{ .key = "tls", .prompt = "tls (off, require, insecure)", .default = "off" },
    } },
    .{ .name = "sqlserver", .blurb = "SQL Server (source and sink; host\\INSTANCE resolves the port)", .fields = &.{
        .{ .key = "host", .prompt = "host" },
        .{ .key = "port", .prompt = "port (blank: 1433, or the instance's)", .int = true },
        .{ .key = "database", .prompt = "database" },
        .{ .key = "user", .prompt = "user" },
        .{ .key = "password", .prompt = "password", .secret = true },
        .{ .key = "tls", .prompt = "tls (off, require, insecure)", .default = "require" },
    } },
    .{ .name = "starrocks", .blurb = "StarRocks (read through the FE, write by stream load)", .fields = &.{
        .{ .key = "host", .prompt = "FE host" },
        .{ .key = "port", .prompt = "FE query port", .default = "9030", .int = true },
        .{ .key = "load_url", .prompt = "BE/CN stream-load URL", .default = "http://<be-host>:8040" },
        .{ .key = "database", .prompt = "database" },
        .{ .key = "user", .prompt = "user", .default = "root" },
        .{ .key = "password", .prompt = "password", .secret = true },
    } },
    .{ .name = "doris", .blurb = "Apache Doris (read through the FE, write by stream load)", .fields = &.{
        .{ .key = "host", .prompt = "FE host" },
        .{ .key = "port", .prompt = "FE query port", .default = "9030", .int = true },
        .{ .key = "load_url", .prompt = "BE stream-load URL", .default = "http://<be-host>:8040" },
        .{ .key = "database", .prompt = "database" },
        .{ .key = "user", .prompt = "user", .default = "root" },
        .{ .key = "password", .prompt = "password", .secret = true },
    } },
    .{ .name = "sftp", .blurb = "an SFTP server (files read and written as sftp://<name>/path)", .fields = &.{
        .{ .key = "host", .prompt = "host" },
        .{ .key = "port", .prompt = "port", .default = "22", .int = true },
        .{ .key = "user", .prompt = "user" },
        .{ .key = "key_file", .prompt = "key file (blank for a password)" },
        .{ .key = "password", .prompt = "password", .secret = true },
        .{ .key = "host_key", .prompt = "pinned host key (blank for ~/.ssh/known_hosts)" },
    } },
    .{ .name = "smb", .blurb = "a Windows file share or Samba server (files read and written as smb://<name>/path)", .fields = &.{
        .{ .key = "host", .prompt = "host" },
        .{ .key = "port", .prompt = "port", .default = "445", .int = true },
        .{ .key = "domain", .prompt = "domain (blank for a local account)" },
        .{ .key = "user", .prompt = "user" },
        .{ .key = "password", .prompt = "password", .secret = true },
        .{ .key = "share", .prompt = "share (blank to name it in each path)" },
    } },
    .{ .name = "http", .blurb = "a REST API (paginated sources, an endpoint sink)", .fields = &.{
        .{ .key = "base_url", .prompt = "base URL" },
        .{ .key = "auth", .prompt = "auth (blank, bearer, basic)" },
    } },
};

/// One answer from the terminal, the default when the line is empty. The terminal
/// is in cooked mode between entries, so a plain read gets a whole line; a secret
/// is read with echo off.
fn ask(msg: *std.Io.Writer, prompt: []const u8, default: []const u8, secret: bool, buf: []u8) ![]const u8 {
    if (default.len > 0) try msg.print("  {s} [{s}]: ", .{ prompt, default }) else try msg.print("  {s}: ", .{prompt});
    try msg.flush();
    const fd = std.fs.File.stdin().handle;
    var orig: ?std.posix.termios = null;
    if (secret) {
        if (std.posix.tcgetattr(fd)) |t| {
            var raw = t;
            raw.lflag.ECHO = false;
            std.posix.tcsetattr(fd, .NOW, raw) catch {};
            orig = t;
        } else |_| {}
    }
    defer if (orig) |t| {
        std.posix.tcsetattr(fd, .NOW, t) catch {};
        msg.writeAll("\n") catch {};
    };
    var n: usize = 0;
    while (n < buf.len) {
        var b: [1]u8 = undefined;
        if (try std.posix.read(fd, &b) == 0) break;
        if (b[0] == '\n') break;
        if (b[0] == '\r') continue;
        buf[n] = b[0];
        n += 1;
    }
    const line = std.mem.trim(u8, buf[0..n], " \t");
    return if (line.len == 0) default else line;
}

/// `\connect [type]`: ask what the connector needs, show the `CREATE CONNECTION`
/// it makes, register it, and offer to reach it and to save it — the way a
/// project scaffolder asks its few questions and writes the file.
fn connectWizard(alloc: std.mem.Allocator, type_arg: []const u8, sess: *Session, msg: *std.Io.Writer) !void {
    var buf: [512]u8 = undefined;
    var which: ?Connector = null;
    if (type_arg.len > 0) {
        for (connectors) |c| if (std.ascii.eqlIgnoreCase(c.name, type_arg)) {
            which = c;
        };
        if (which == null) return msg.print("error: no connector `{s}` — one of postgres, mysql, sqlserver, starrocks, http\n", .{type_arg});
    } else {
        try msg.writeAll("new connection — the type:\n");
        for (connectors, 1..) |c, i| try msg.print("  {d}. {s: <10} {s}\n", .{ i, c.name, c.blurb });
        const a = try ask(msg, "type (number or name)", "", false, &buf);
        const idx = std.fmt.parseInt(usize, a, 10) catch 0;
        if (idx >= 1 and idx <= connectors.len) which = connectors[idx - 1];
        for (connectors) |c| if (std.ascii.eqlIgnoreCase(c.name, a)) {
            which = c;
        };
        if (which == null) return msg.writeAll("no such type; nothing made\n");
    }
    const conn = which.?;
    const name = try alloc.dupe(u8, try ask(msg, "name (how the script refers to it, e.g. erp)", "", false, &buf));
    defer alloc.free(name);
    if (name.len == 0 or !std.ascii.isAlphabetic(name[0])) return msg.writeAll("a name starts with a letter; nothing made\n");
    var upper_buf: [64]u8 = undefined;
    const up = std.ascii.upperString(&upper_buf, name);

    var stmt = std.array_list.Managed(u8).init(alloc);
    defer stmt.deinit();
    try stmt.writer().print("CREATE CONNECTION {s} TYPE {s} OPTIONS (", .{ name, conn.name });
    var first = true;
    for (conn.fields) |f| {
        var default = f.default;
        var hint_buf: [96]u8 = undefined;
        var conv_buf: [80]u8 = undefined;
        const cred = f.secret or std.mem.eql(u8, f.key, "user");
        // Blank credentials mean the runtime's own convention: env(NAME_USER) / env(NAME_PASS).
        const conv = try std.fmt.bufPrint(&conv_buf, "{s}_{s}", .{ up, if (f.secret) "PASS" else "USER" });
        if (cred and f.default.len == 0) default = try std.fmt.bufPrint(&hint_buf, "env:{s}", .{conv});
        const v = try ask(msg, f.prompt, default, f.secret, &buf);
        if (v.len == 0 or std.mem.startsWith(u8, v, "<")) continue;
        if (std.mem.startsWith(u8, v, "env:")) {
            const var_name = v[4..];
            if (std.mem.eql(u8, var_name, conv)) continue;
            try stmt.writer().print("{s}{s} = env('{s}')", .{ if (first) "" else ", ", f.key, var_name });
        } else if (f.int) {
            try stmt.writer().print("{s}{s} = {s}", .{ if (first) "" else ", ", f.key, v });
        } else {
            try stmt.writer().print("{s}{s} = '{s}'", .{ if (first) "" else ", ", f.key, v });
        }
        first = false;
    }
    try stmt.appendSlice(");");
    try msg.print("\n{s}\n", .{stmt.items});
    try runBlock(alloc, stmt.items, sess, msg);
    var declared = false;
    for (sess.decls.items.items) |e| if (e.kind == .connection and std.ascii.eqlIgnoreCase(e.name, name)) {
        declared = true;
    };
    if (!declared) return;

    if (!std.mem.eql(u8, conn.name, "http")) {
        const t = try ask(msg, "reach it now? (y/n)", "y", false, &buf);
        if (std.ascii.toLower(t[0]) == 'y') {
            const reached = connTables(&sess.completer(), name).len > 0;
            try msg.print("  {s}\n", .{if (reached) "reached" else "could not reach it (or it has no tables) — \\c test retries; the connection stays declared"});
        }
    }
    const sv = try ask(msg, "save to the startup file, so every session has it? (y/n)", "n", false, &buf);
    if (std.ascii.toLower(sv[0]) == 'y') try saveDecls(alloc, "", sess, msg);
}

/// `\i <file>`: run a file as an entry, so its declarations join the session —
/// what `@include` inside an entry does not do. Also the startup file's path in.
fn sourceFile(alloc: std.mem.Allocator, path: []const u8, sess: *Session, msg: *std.Io.Writer) !void {
    const text = std.fs.cwd().readFileAlloc(alloc, path, 1 << 22) catch |e|
        return msg.print("error: could not read `{s}`: {s}\n", .{ path, @errorName(e) });
    defer alloc.free(text);
    const trimmed = std.mem.trim(u8, text, " \t\r\n");
    if (trimmed.len == 0) return;
    try runBlock(alloc, trimmed, sess, msg);
}

/// `\save [file]`: the session's declarations, one statement per line, to the
/// startup file by default — the way a session's connections become tomorrow's.
fn saveDecls(alloc: std.mem.Allocator, path_arg: []const u8, sess: *Session, msg: *std.Io.Writer) !void {
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

/// `\edit [file]`: the last entry (or the file) in `$EDITOR`, then run what
/// comes back. The terminal is in cooked mode between entries, so the editor
/// gets it whole.
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

/// Names fetched for completion, once each: a connection's tables, a table's or
/// a file's columns. A source that could not be asked is remembered as empty, so
/// a dead connection costs one wait, not one per Tab.
pub const Catalog = struct {
    arena: std.heap.ArenaAllocator,
    tables: std.StringHashMap([]const []const u8),
    columns: std.StringHashMap([]const complete.Column),

    pub fn init(gpa: std.mem.Allocator) Catalog {
        return .{ .arena = std.heap.ArenaAllocator.init(gpa), .tables = std.StringHashMap([]const []const u8).init(gpa), .columns = std.StringHashMap([]const complete.Column).init(gpa) };
    }
    pub fn deinit(self: *Catalog) void {
        self.tables.deinit();
        self.columns.deinit();
        self.arena.deinit();
    }
};

/// What completion draws on: the declarations in scope, and a cache of what
/// sources said about their tables and columns. The REPL builds one from its
/// session; `basalt complete` and the kernel's `complete` build their own.
pub const Completer = struct {
    gpa: std.mem.Allocator,
    decls: *const DeclStore,
    catalog: *Catalog,
    /// Ask connections for their tables and columns. Off, only local files
    /// (whose headers and footers cost no round trip) contribute columns.
    connect: bool = true,
};

/// A completion offer: replace `text[start..cursor]` with one of `items`.
pub const Offer = struct { start: usize = 0, items: []const complete.Candidate = &.{} };

/// Run `SELECT ...` under the session's declarations into a temporary CSV and
/// hand back its rows, first column only — how Tab asks a source a question
/// without printing anything. Errors come back as no rows.
fn fetchColumn(cx: *const Completer, select: []const u8) []const []const u8 {
    const a = cx.catalog.arena.allocator();
    const rows = fetchRows(cx, select);
    const out = a.alloc([]const u8, rows.len) catch return &.{};
    for (rows, out) |r, *o| o.* = r[0];
    return out;
}

/// Every row of `SELECT ...`, as its cells (each row has at least one). The
/// temporary file is basalt's own CSV, so a cell is split on commas outside
/// quotes and a doubled quote read as one.
fn fetchRows(cx: *const Completer, select: []const u8) []const []const []const u8 {
    const a = cx.catalog.arena.allocator();
    var scratch = std.heap.ArenaAllocator.init(cx.gpa);
    defer scratch.deinit();
    const sa = scratch.allocator();
    var path_buf: [96]u8 = undefined;
    const path = std.fmt.bufPrint(&path_buf, "/tmp/basalt-tab-{d}-{d}.csv", .{ std.os.linux.getpid(), std.time.milliTimestamp() }) catch return &.{};
    defer std.fs.cwd().deleteFile(path) catch {};

    var text = std.array_list.Managed(u8).init(sa);
    for (cx.decls.items.items) |e| {
        text.appendSlice(e.text) catch return &.{};
        text.appendSlice(";\n") catch return &.{};
    }
    text.writer().print("LOAD INTO '{s}' AS {s};", .{ path, select }) catch return &.{};
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = parser.parseSource(sa, text.items, &pdiag) catch return &.{};
    var rdiag: runtime.Diag = .{};
    _ = runtime.run(cx.gpa, prog, .{ .log = .{ .quiet = true, .summary = .none } }, &rdiag) catch return &.{};
    const data = std.fs.cwd().readFileAlloc(sa, path, 1 << 22) catch return &.{};

    var out = std.array_list.Managed([]const []const u8).init(a);
    var lines = std.mem.splitScalar(u8, data, '\n');
    _ = lines.next();
    while (lines.next()) |ln| {
        if (ln.len == 0) continue;
        out.append(csvCells(a, ln) catch return &.{}) catch return &.{};
    }
    return out.toOwnedSlice() catch &.{};
}

fn csvCells(a: std.mem.Allocator, line: []const u8) ![]const []const u8 {
    var cells = std.array_list.Managed([]const u8).init(a);
    var cell = std.array_list.Managed(u8).init(a);
    var quoted = false;
    var i: usize = 0;
    while (i < line.len) : (i += 1) {
        const c = line[i];
        if (quoted) {
            if (c != '"') {
                try cell.append(c);
            } else if (i + 1 < line.len and line[i + 1] == '"') {
                try cell.append('"');
                i += 1;
            } else quoted = false;
        } else if (c == '"') {
            quoted = true;
        } else if (c == ',') {
            try cells.append(try cell.toOwnedSlice());
        } else if (c != '\r') try cell.append(c);
    }
    try cells.append(try cell.toOwnedSlice());
    return cells.toOwnedSlice();
}

/// The resources declared on `conn` when it is an http connection — its
/// "tables", known from the session without asking the network — else null.
fn httpResources(arena: std.mem.Allocator, cx: *const Completer, conn: []const u8) !?[]const []const u8 {
    const is_http = for (cx.decls.items.items) |e| {
        if (e.kind == .connection and std.ascii.eqlIgnoreCase(e.name, conn))
            break std.ascii.eqlIgnoreCase(connTypeOf(e.text) orelse "", "http");
    } else false;
    if (!is_http) return null;
    var out = std.array_list.Managed([]const u8).init(arena);
    for (cx.decls.items.items) |e| {
        if (e.kind != .resource or e.name.len <= conn.len or e.name[conn.len] != '.') continue;
        if (std.ascii.eqlIgnoreCase(e.name[0..conn.len], conn)) try out.append(e.name[conn.len + 1 ..]);
    }
    return try out.toOwnedSlice();
}

/// The tables of `conn` as `schema.table`, fetched on first use.
fn connTables(cx: *const Completer, conn: []const u8) []const []const u8 {
    if (cx.catalog.tables.get(conn)) |t| return t;
    if (!cx.connect) return &.{};
    const a = cx.catalog.arena.allocator();
    const q = std.fmt.allocPrint(a, "SELECT table_schema || '.' || table_name AS t FROM {s}.QUERY($$SELECT TABLE_SCHEMA AS table_schema, TABLE_NAME AS table_name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE IN ('BASE TABLE', 'VIEW') AND TABLE_SCHEMA NOT IN ('information_schema', 'pg_catalog', 'mysql', 'performance_schema', 'sys', '_statistics_') ORDER BY 1, 2$$)", .{conn}) catch return &.{};
    const rows = fetchColumn(cx, q);
    cx.catalog.tables.put(a.dupe(u8, conn) catch return rows, rows) catch {};
    return rows;
}

/// The columns of `conn.schema.table`, or of a file path, fetched on first use.
fn sourceColumns(cx: *const Completer, key: []const u8) []const complete.Column {
    if (cx.catalog.columns.get(key)) |c| return c;
    const a = cx.catalog.arena.allocator();
    var rows: []const complete.Column = &.{};
    if (key[0] == '\'') {
        // A file: the analyzer reads its schema without moving a row.
        var scratch = std.heap.ArenaAllocator.init(cx.gpa);
        defer scratch.deinit();
        const sa = scratch.allocator();
        blk: {
            const text = std.fmt.allocPrint(sa, "SELECT * FROM {s};", .{key}) catch break :blk;
            var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
            const prog = parser.parseSource(sa, text, &pdiag) catch break :blk;
            var adiag = analyze.Diag{};
            const plan = analyze.analyze(sa, prog, &adiag) catch break :blk;
            const schema = plan.outputs[0].source.schema orelse break :blk;
            const cols = a.alloc(complete.Column, schema.fields.len) catch break :blk;
            for (schema.fields, cols) |f, *c| c.* = .{
                .name = a.dupe(u8, f.name) catch break :blk,
                .type = f.ty.name(a) catch break :blk,
            };
            rows = cols;
        }
    } else {
        if (!cx.connect) return rows;
        var parts = std.mem.splitScalar(u8, key, '.');
        const conn = parts.next().?;
        const schema = parts.next() orelse return rows;
        const tbl = parts.next() orelse return rows;
        // Upper case, as `SHOW TABLES` spells it (a case-sensitive SQL Server
        // resolves nothing else), aliased so every dialect answers the same names.
        const q = std.fmt.allocPrint(a, "SELECT col_name, col_type FROM {s}.QUERY($$SELECT COLUMN_NAME AS col_name, DATA_TYPE AS col_type FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{s}' AND TABLE_NAME = '{s}' ORDER BY ORDINAL_POSITION$$)", .{ conn, schema, tbl }) catch return rows;
        const got = fetchRows(cx, q);
        const cols = a.alloc(complete.Column, got.len) catch return rows;
        for (got, cols) |r, *c| c.* = .{ .name = r[0], .type = if (r.len > 1) r[1] else "" };
        rows = cols;
    }
    cx.catalog.columns.put(a.dupe(u8, key) catch return rows, rows) catch {};
    return rows;
}

/// The connector type of a declared connection, read off its `CREATE CONNECTION`
/// text (`TYPE <word>`).
fn connTypeOf(text: []const u8) ?[]const u8 {
    var it = std.mem.tokenizeAny(u8, text, " \t\r\n(");
    while (it.next()) |w| {
        if (std.ascii.eqlIgnoreCase(w, "type")) return it.next();
    }
    return null;
}

/// Tab's provider: the session's names, plus the columns of every table and
/// file the entry mentions, handed to the matcher; a path is listed here.
fn suggest(ctx: *anyopaque, arena: std.mem.Allocator, text: []const u8, cursor: usize) anyerror!Editor.Suggestions {
    const sess: *Session = @ptrCast(@alignCast(ctx));
    const cx = sess.completer();
    const offer = try suggestFor(arena, &cx, text, cursor);
    const items = try arena.alloc([]const u8, offer.items.len);
    for (offer.items, items) |cand, *it| it.* = cand.text;
    return .{ .start = offer.start, .items = items };
}

/// Completion at `cursor` in `text`: keywords, the declared names in scope, the
/// entry's CTEs, a path inside an open quote, a connection's tables after
/// `conn.`, and the columns of every table and file the text names.
pub fn suggestFor(arena: std.mem.Allocator, cx: *const Completer, text: []const u8, cursor: usize) anyerror!Offer {
    var conns = std.array_list.Managed([]const u8).init(arena);
    var fns = std.array_list.Managed([]const u8).init(arena);
    var params = std.array_list.Managed([]const u8).init(arena);
    for (cx.decls.items.items) |e| switch (e.kind) {
        .connection => try conns.append(e.name),
        .function => try fns.append(e.name),
        .param, .let => try params.append(e.name),
        .endpoint, .resource => {},
    };

    // CTEs of the entry: `WITH name AS (` and `, name AS (`.
    var ctes = std.array_list.Managed([]const u8).init(arena);
    var i: usize = 0;
    while (i + 4 < text.len) : (i += 1) {
        const at_with = std.ascii.eqlIgnoreCase(text[i..@min(text.len, i + 4)], "with") and (i == 0 or !std.ascii.isAlphanumeric(text[i - 1]));
        if (!(at_with or text[i] == ',')) continue;
        var j = if (at_with) i + 4 else i + 1;
        while (j < text.len and (text[j] == ' ' or text[j] == '\n')) j += 1;
        const ns = j;
        while (j < text.len and (std.ascii.isAlphanumeric(text[j]) or text[j] == '_')) j += 1;
        if (j == ns) continue;
        var k = j;
        while (k < text.len and text[k] == ' ') k += 1;
        if (k + 2 < text.len and std.ascii.eqlIgnoreCase(text[k .. k + 2], "as") and text[k + 2] == ' ') try ctes.append(text[ns..j]);
    }

    // The tables of every connection the entry names with a dot, and the columns
    // of every `conn.schema.table` and `'file'` in it.
    var tables = std.array_list.Managed(complete.ConnTables).init(arena);
    var columns = std.array_list.Managed(complete.Column).init(arena);
    for (conns.items) |c| {
        var pos: usize = 0;
        var wanted = false;
        while (std.mem.indexOfPos(u8, text, pos, c)) |p| : (pos = p + c.len) {
            if (p > 0 and (std.ascii.isAlphanumeric(text[p - 1]) or text[p - 1] == '_')) continue;
            if (p + c.len >= text.len or text[p + c.len] != '.') continue;
            wanted = true;
            var e = p + c.len + 1;
            var dots: usize = 0;
            while (e < text.len and (std.ascii.isAlphanumeric(text[e]) or text[e] == '_' or text[e] == '.')) : (e += 1) {
                if (text[e] == '.') dots += 1;
            }
            // `conn.QUERY(` is no table; a name at the very end of the text is
            // (`SELECT a FROM erp.dbo.t`), unless the cursor is still typing it —
            // then it is half a name, and asking for its columns would send a
            // catalog query on every keystroke.
            const typing = cursor > p and cursor <= e;
            if (dots == 1 and !typing and (e == text.len or text[e] != '(')) for (sourceColumns(cx, text[p..e])) |col| try columns.append(col);
        }
        if (wanted) try tables.append(.{ .conn = c, .tables = try httpResources(arena, cx, c) orelse connTables(cx, c) });
    }
    var q: usize = 0;
    while (std.mem.indexOfScalarPos(u8, text, q, '\'')) |open| {
        const close = std.mem.indexOfScalarPos(u8, text, open + 1, '\'') orelse break;
        q = close + 1;
        if (q >= cursor and open < cursor) continue;
        const lit = text[open..q];
        const file_like = for ([_][]const u8{ ".csv'", ".parquet'", ".gz'", ".zst'", ".arrow'", ".arrows'", ".feather'", ".ipc'" }) |ext| {
            if (std.ascii.endsWithIgnoreCase(lit, ext)) break true;
        } else false;
        if (file_like)
            for (sourceColumns(cx, lit)) |col| try columns.append(col);
    }

    const r = try complete.complete(arena, .{
        .connections = conns.items,
        .ctes = ctes.items,
        .functions = fns.items,
        .params = params.items,
        .tables = tables.items,
        .columns = columns.items,
    }, text, cursor);
    switch (r) {
        .none => return .{ .start = complete.wordStart(text, cursor) },
        .candidates => |c| return .{ .start = c.start, .items = c.items },
        .path => |p| {
            const paths = try listPaths(arena, p.partial);
            const items = try arena.alloc(complete.Candidate, paths.len);
            for (paths, items) |pth, *it| it.* = .{ .text = pth, .kind = .path };
            return .{ .start = p.start, .items = items };
        },
    }
}

/// The entries of the directory `partial` is in, that begin as it does — a
/// directory with a `/` after it, so the next Tab goes inside.
fn listPaths(arena: std.mem.Allocator, partial: []const u8) ![]const []const u8 {
    const slash = std.mem.lastIndexOfScalar(u8, partial, '/');
    const dir_part = if (slash) |s| partial[0 .. s + 1] else "";
    const name_part = if (slash) |s| partial[s + 1 ..] else partial;
    var dir = std.fs.cwd().openDir(if (dir_part.len == 0) "." else dir_part, .{ .iterate = true }) catch return &.{};
    defer dir.close();
    var out = std.array_list.Managed([]const u8).init(arena);
    var it = dir.iterate();
    while (try it.next()) |e| {
        if (name_part.len == 0 and e.name[0] == '.') continue;
        if (!std.mem.startsWith(u8, e.name, name_part)) continue;
        try out.append(try std.fmt.allocPrint(arena, "{s}{s}{s}", .{ dir_part, e.name, if (e.kind == .directory) "/" else "" }));
    }
    std.mem.sort([]const u8, out.items, {}, struct {
        fn lt(_: void, x: []const u8, y: []const u8) bool {
            return std.mem.lessThan(u8, x, y);
        }
    }.lt);
    return out.toOwnedSlice();
}

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

    var sess = Session{ .decls = DeclStore.init(alloc), .tty = std.posix.isatty(std.fs.File.stdin().handle), .catalog = Catalog.init(alloc) };
    defer sess.decls.deinit();
    defer sess.catalog.deinit();

    var editor: ?Editor = if (sess.tty) Editor.init(alloc) else null;
    defer if (editor) |*e| e.deinit();

    // Only here are tables fitted to the terminal and the last one kept for `\view`.
    table.interactive = sess.tty;
    defer table.dropLast();

    if (sess.tty) {
        try banner(msg, !std.process.hasEnvVarConstant("NO_COLOR"));
        try msg.flush();
    }

    // The startup file, when there is one: connections a session should start with.
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
        // Un-poison the session: a ^C-aborted query leaves the abort flag set.
        runtime.resetAbort();
        block.clearRetainingCapacity();
        while (true) {
            var line: []const u8 = undefined;
            if (editor) |*ed| {
                // The editor hands back a whole entry, however many lines it took.
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
                            if (isQuit(t)) quit = true else try metaCommand(t, &sess, msg);
                        } else {
                            try block.appendSlice(l);
                            // Run without its `;` (Ctrl+J), the entry
                            // may lack one — which is all the parser would say about it.
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
                break; // blank line still runs a pending buffer
            }
            // Meta commands only lead an entry, so `\` inside a query is untouched.
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
        try runBlock(alloc, trimmed, &sess, msg);
        // Room between one result and the next entry, in either mode.
        if (sess.tty) {
            try msg.writeAll("\n");
            try msg.flush();
        }
    }
    if (sess.tty) {
        try msg.writeAll("bye\n");
        try msg.flush();
    }
    return 0;
}

/// One statement of an entry: the declaration it is, or null for something to
/// run, and its text (a declaration's, without the closing `;`).
const EntryStmt = struct { id: ?DeclId, text: []const u8 };

fn offsetOf(text: []const u8, pos: ast.Pos) ?usize {
    if (pos.line == 0) return null;
    var line: u32 = 1;
    var i: usize = 0;
    while (line < pos.line) : (line += 1) i = (std.mem.indexOfScalarPos(u8, text, i, '\n') orelse return null) + 1;
    return @min(text.len, i + pos.col - 1);
}

/// The statements of the entry (the part of `text` from `entry_at`), in source
/// order, each with its text cut at its own last top-level `;` — so a statement
/// function is one statement however many `;`s its body holds.
fn entryStatements(arena: std.mem.Allocator, text: []const u8, entry_at: usize, prog: ast.Program) ![]EntryStmt {
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
        // A hoisted CTE and the query that reads it share a start; keep the query.
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

/// The line a parse error names, with a caret under the column — as an editor
/// marks a squiggle. `text` is prelude + entry; only a position inside the entry
/// (the part the person typed) is shown.
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

/// Handle a `\...` entry. Unknown ones report themselves instead of reaching
/// the parser.
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
        // The whole screen, cursor home; the terminal's scrollback is left alone.
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
    // psql's spellings, as the statements they stand for.
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

/// The formats a REPL session can print in. Arrow is left out: a binary stream in
/// a terminal is noise, and `basalt run --format arrow` is the way to pipe one.
fn parseReplFormat(name: []const u8) ?runtime.StdoutFormat {
    inline for (.{ runtime.StdoutFormat.table, .json, .csv, .tsv }) |f| {
        if (std.ascii.eqlIgnoreCase(name, @tagName(f))) return f;
    }
    return null;
}

/// A declaration an entry makes, committed to the session once the entry parses.
pub const Pending = struct { id: DeclId, text: []const u8 };

/// An entry ready to run: the session's declarations as a prelude, the entry
/// after them, parsed as one program.
pub const Prepared = struct {
    /// Prelude + entry: what the parser saw, and what diagnostics' lines count in.
    text: []const u8,
    /// Where the entry starts in `text`.
    entry_at: usize,
    prog: ast.Program,
    pending: []const Pending,
    /// Statements that do something beyond declaring.
    executable: usize,
};

pub const PrepareError = error{ ParseFailed, EndpointInSession, OutOfMemory };

/// Parse one entry against the session's declarations. The entry is prefixed with
/// the stored declarations so earlier connections/functions/params are in scope;
/// nothing is committed here — the caller commits `pending` once it has decided
/// the entry stands, so a typo can't poison the session. On `ParseFailed`,
/// `diag` holds the position (in `Prepared.text` lines, or the included file's),
/// and `text_out`, when given, the prelude + entry text those lines count in.
/// The session's declarations, then `block`: the text an entry runs as, and
/// where the entry starts in it. A declaration the entry makes again is left
/// out of the prelude, so the text never declares one name twice.
fn withPrelude(a: std.mem.Allocator, decls: *const DeclStore, pending: []const Pending, block: []const u8) !struct { text: []const u8, entry_at: usize } {
    var buf = std.array_list.Managed(u8).init(a);
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
    try buf.appendSlice(block);
    return .{ .text = buf.items, .entry_at = entry_at };
}

/// `withPrelude` for a block not yet split into its declarations: what `check`
/// reads a cell against, without touching the session.
pub fn sessionText(a: std.mem.Allocator, decls: *const DeclStore, block: []const u8) !struct { text: []const u8, entry_at: usize } {
    var pending = std.array_list.Managed(Pending).init(a);
    for (try splitStatements(a, block)) |st| {
        if (declOf(st)) |id| try pending.append(.{ .id = id, .text = st });
    }
    const j = try withPrelude(a, decls, pending.items, block);
    return .{ .text = j.text, .entry_at = j.entry_at };
}

pub fn prepareEntry(a: std.mem.Allocator, decls: *const DeclStore, block: []const u8, diag: *include.Diag, text_out: ?*[]const u8) PrepareError!Prepared {
    var pending = std.array_list.Managed(Pending).init(a);
    var executable: usize = 0;
    for (try splitStatements(a, block)) |st| {
        const id = declOf(st) orelse {
            executable += 1;
            continue;
        };
        if (id.kind == .endpoint) return error.EndpointInSession;
        try pending.append(.{ .id = id, .text = st });
    }

    const joined = try withPrelude(a, decls, pending.items, block);
    const text = joined.text;
    const entry_at = joined.entry_at;
    if (text_out) |t| t.* = text;

    // An entry may open with `@include 'p.sql';` (paths relative to the cwd): the
    // file's text is parsed with the entry as one program, so its declarations are
    // in scope for the statements typed below it. They live as long as the entry —
    // the session keeps no include store.
    const prog = include.loadProgram(a, text, "<repl>", ".", diag) catch |e| switch (e) {
        error.ParseFailed => return error.ParseFailed,
        error.OutOfMemory => return error.OutOfMemory,
    };

    // The entry's statements as the parser saw them. The pre-scan above only knows
    // first words and top-level `;`s, and a statement function's body has `;`s of
    // its own — it would be cut into a truncated declaration and stray fragments.
    // An `@include` puts other files' positions in the program, so it keeps the
    // pre-scan's reading.
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

/// The `LET` values a run decided, kept as literal declarations: a session
/// replays its declarations ahead of every later entry, and replaying
/// `LET t = now()` or `LET n = (SELECT count(*) ...)` would give each entry a
/// value of its own — a different instant, a re-run query — where the entry
/// that declared it saw one.
pub const LetFreezer = struct {
    arena: std.mem.Allocator,
    frozen: std.array_list.Managed(Frozen),

    const Frozen = struct { name: []const u8, text: []const u8 };

    pub fn init(arena: std.mem.Allocator) LetFreezer {
        return .{ .arena = arena, .frozen = std.array_list.Managed(Frozen).init(arena) };
    }

    pub fn hook(self: *LetFreezer) runtime.LetHook {
        return .{ .ctx = self, .f = onLet };
    }

    fn onLet(ctx: *anyopaque, name: []const u8, v: Value) void {
        const self: *LetFreezer = @ptrCast(@alignCast(ctx));
        const lit = letLiteral(self.arena, v) catch return orelse return;
        const text = std.fmt.allocPrint(self.arena, "LET {s} = {s}", .{ name, lit }) catch return;
        const n = self.arena.dupe(u8, name) catch return;
        self.frozen.append(.{ .name = n, .text = text }) catch {};
    }

    /// Replace each LET's stored text with its value. A LET the session does not
    /// hold (none, today — every top-level LET is a declaration) is left alone.
    pub fn commit(self: *const LetFreezer, decls: *DeclStore) !void {
        for (self.frozen.items) |f| {
            for (decls.items.items) |e| {
                if (e.kind == .let and std.ascii.eqlIgnoreCase(e.name, f.name)) break;
            } else continue;
            try decls.put(.{ .kind = .let, .name = f.name }, f.text);
        }
    }
};

/// A SQL expression that folds back to exactly `v`, or null for a value with
/// no literal form (bytes, nested), which then keeps its expression. Text the
/// lexer has no literal for — a date, an exponent, the one int whose magnitude
/// overflows — goes through a CAST from its printed form, which reads back exact.
pub fn letLiteral(arena: std.mem.Allocator, v: Value) !?[]const u8 {
    return switch (v) {
        .null => "NULL",
        .bool => |b| if (b) "TRUE" else "FALSE",
        .int => |x| if (x == std.math.minInt(i64))
            "CAST('-9223372036854775808' AS BIGINT)"
        else
            try std.fmt.allocPrint(arena, "{d}", .{x}),
        .float => |x| try std.fmt.allocPrint(arena, "CAST('{e}' AS DOUBLE)", .{x}),
        .decimal => |d| try std.fmt.allocPrint(arena, "CAST('{s}' AS DECIMAL(38, {d}))", .{ try eval.valueToString(arena, v), d.scale }),
        .string => |s| blk: {
            var out = std.array_list.Managed(u8).init(arena);
            try out.append('\'');
            for (s) |c| {
                if (c == '\'') try out.append('\'');
                try out.append(c);
            }
            try out.append('\'');
            break :blk out.items;
        },
        .date => try std.fmt.allocPrint(arena, "CAST('{s}' AS DATE)", .{try eval.valueToString(arena, v)}),
        .time => try std.fmt.allocPrint(arena, "CAST('{s}' AS TIME)", .{try eval.valueToString(arena, v)}),
        .timestamp => try std.fmt.allocPrint(arena, "CAST('{s}' AS TIMESTAMP)", .{try eval.valueToString(arena, v)}),
        else => null,
    };
}

/// Parse and run one REPL entry, reporting errors without aborting the loop.
fn runBlock(alloc: std.mem.Allocator, block: []const u8, sess: *Session, msg: *std.Io.Writer) !void {
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
        // A LET's value is decided in the entry that declares it, not by the
        // next one that happens to run something.
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

    const t0 = std.time.nanoTimestamp();
    var rdiag: runtime.Diag = .{};
    var freezer = LetFreezer.init(a);
    defer freezer.commit(&sess.decls) catch {};
    _ = runtime.run(alloc, prepared, .{
        .on_let = freezer.hook(),
        // Errors only, but not `quiet`: that would swallow the entry's own `PRINT`s.
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

/// Append a `write stdout` table sink to any output pipeline that doesn't already
/// end in a `write`, so REPL entries show their results.
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

/// `\clear`, `clear`, `cls`: clear the screen — what a person typing any of them
/// means, so none of them may do anything else.
fn isClear(t: []const u8) bool {
    inline for (.{ "\\clear", "\\cls", "clear", "cls" }) |k| {
        if (std.ascii.eqlIgnoreCase(t, k)) return true;
    }
    return false;
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
        \\A statement ends in `;` and runs on Enter. A terminal SELECT prints a table;
        \\LOAD INTO writes to its target. Declarations stay for the session.
        \\
        \\session
        \\  \connect [type]         make a connection by answering a few questions
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
        \\  \view, \v               scroll the last result: arrows by column and row, q leaves
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
        \\  basalt serve <dir> [--port N] [--watch] [--log-format FMT] [--log-level LVL]
        \\               host every endpoint script in a dir (SIGHUP or -w reloads)
        \\  basalt check <script>|-|-c <script> [--format json] [--known t1,t2]
        \\               parse and validate without running, reporting every problem;
        \\               `EXPLAIN` prints the plan; json: an array of diagnostics with
        \\               their ranges; --known: tables the script reads, undeclared
        \\  basalt complete <script>|-|-c <script> --pos N [--connect] [--utf16]
        \\               what Tab offers at offset N (bytes, or UTF-16 units), as JSON
        \\  basalt repl  interactive read-eval-print loop
        \\  basalt kernel [--format FMT] [-j N]
        \\               a session for a notebook: NDJSON requests on stdin, framed
        \\               results and a JSON status per script on stdout
        \\  basalt version  print the version and exit
        \\  basalt help  show this help
        \\
        \\script:
        \\  a path, `-` for stdin, or `-c <script>` for an inline script
        \\  a terminal `SELECT ...;` prints a table; `LOAD INTO <target> AS <query>;` writes
        \\  see language.md for the dialect
        \\
        \\sources and sinks:
        \\  files      CSV and Parquet, by path or URL, Arrow IPC (.arrow, .feather,
        \\             .ipc, .arrows; local) and Excel (.xlsx, read; WITH (sheet =
        \\             'Name', range = 'B3:F200', header = false)) — the extension picks
        \\             the format; another needs WITH (format = 'csv'|'parquet'|'arrow').
        \\             WITH (delimiter = ';', encoding = 'latin1') for non-comma,
        \\             non-UTF-8 CSV (also cp1252; delimiter works on a sink too)
        \\  archives   file.csv.gz / .csv.zst stream through the codec;
        \\             'archive.zip :: inner.csv' reads one member (`::` optional
        \\             when the zip holds one file). Neither is splittable, so
        \\             both read serially whatever -j says
        \\  object     az://<account>/<container>/<path> or s3://<bucket>/<key>, and a
        \\             trailing / reads every object under that prefix as one table
        \\  sftp       sftp://<conn>/<path> through CREATE CONNECTION <conn> TYPE sftp
        \\             (or sftp://user@host/path), read and written; the host key is
        \\             checked against known_hosts or a pinned host_key, never trusted
        \\  smb        smb://<conn>/<share>/<path> through CREATE CONNECTION <conn> TYPE smb
        \\             (or smb://user@host/share/path), read and written; NTLMv2, every
        \\             message signed (SMB 2.1 to 3.1.1)
        \\  databases  postgres, mysql, sqlserver, starrocks, doris (CREATE CONNECTION ... TYPE ...)
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
        \\                     (default: CPU count; a map pipeline keeps file order at
        \\                     any -j, except into a table, which has none. A float SUM is
        \\                     reproducible for a given -j but not across values of it —
        \\                     CAST to DECIMAL for a total that never varies.)
        \\  --max-rows N       a SELECT printed to stdout keeps its first N rows and stops
        \\                     reading there — a preview of a huge source in milliseconds
        \\  --port N           listen port for HTTP mode
        \\  --format FMT       table|json|csv|tsv|arrow — what a SELECT writes to stdout.
        \\                     table: every row and column, for reading, closed by a
        \\                     `(N rows)` line. json: NDJSON rows (and a summary object
        \\                     for a LOAD run). csv, tsv: a header and the rows, quoted
        \\                     as a .csv sink quotes them, nothing else. arrow: an Arrow
        \\                     IPC stream per result, each naming its statement, kind
        \\                     and position in schema metadata; EXPLAIN is a result
        \\  --log-format FMT   text|json — stderr log format (default text;
        \\                     json is NDJSON, one object per line, for collectors)
        \\  --log-level LVL    error|warn|info|debug (default warn)
        \\  --no-progress      no live progress line (it is only ever drawn when
        \\                     stderr is a terminal, and never under -q or
        \\                     --log-format json)
        \\  -q, --quiet        suppress warnings too: errors only (the run summary
        \\                     still prints)
        \\
    );
}

test "every meta command the REPL handles is one Tab offers, and the other way round" {
    const src = @embedFile("cli.zig");
    for (complete.meta_commands) |m| {
        if (std.mem.eql(u8, m, "\\quit") or std.mem.eql(u8, m, "\\h") or std.mem.eql(u8, m, "\\q") or std.mem.eql(u8, m, "\\help") or std.mem.eql(u8, m, "\\cls") or std.mem.eql(u8, m, "\\clear")) continue;
        var needle_buf: [64]u8 = undefined;
        const needle = try std.fmt.bufPrint(&needle_buf, "\"\\\\{s}\"", .{m[1..]});
        if (std.mem.indexOf(u8, src, needle) == null) {
            std.debug.print("Tab offers `{s}` but metaCommand does not handle it\n", .{m});
            return error.TestUnexpectedResult;
        }
    }
    // The reverse: each command `metaCommand` compares `cmd` against is offered.
    const body_start = std.mem.indexOf(u8, src, "\nfn metaCommand(").?;
    const body_end = std.mem.indexOfPos(u8, src, body_start + 1, "\nfn ").?;
    var it = std.mem.splitSequence(u8, src[body_start..body_end], "std.mem.eql(u8, cmd, \"\\\\");
    _ = it.next();
    while (it.next()) |rest| {
        const end = std.mem.indexOfScalar(u8, rest, '"') orelse continue;
        var cmd_buf: [64]u8 = undefined;
        const cmd = try std.fmt.bufPrint(&cmd_buf, "\\{s}", .{rest[0..end]});
        var offered = false;
        for (complete.meta_commands) |m| if (std.mem.eql(u8, m, cmd)) {
            offered = true;
        };
        if (!offered) {
            std.debug.print("metaCommand handles `{s}` but Tab does not offer it\n", .{cmd});
            return error.TestUnexpectedResult;
        }
    }
}

test "suggestFor: a half-typed script's own declarations and a local file's columns" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "t.csv", .data = "order_id,order_total\n1,2\n" });
    const dir = try tmp.dir.realpathAlloc(a, ".");

    var decls = DeclStore.init(std.testing.allocator);
    defer decls.deinit();
    var catalog = Catalog.init(std.testing.allocator);
    defer catalog.deinit();
    const text = try std.fmt.allocPrint(a, "CREATE FUNCTION tax(x) AS x * 2;\nPARAM since DATE;\nSELECT ta FROM '{s}/t.csv' WHERE order_", .{dir});
    try declareFrom(&decls, a, text);
    // offline: no connection is asked anything
    const cx = Completer{ .gpa = std.testing.allocator, .decls = &decls, .catalog = &catalog, .connect = false };

    const at_fn = std.mem.indexOf(u8, text, "SELECT ta").? + "SELECT ta".len;
    const fns = try suggestFor(a, &cx, text, at_fn);
    try std.testing.expectEqualStrings("tax", fns.items[0].text);
    try std.testing.expectEqual(complete.Kind.function, fns.items[0].kind);

    const cols = try suggestFor(a, &cx, text, text.len);
    try std.testing.expectEqual(@as(usize, 2), cols.items.len);
    try std.testing.expectEqualStrings("order_id", cols.items[0].text);
    try std.testing.expectEqual(complete.Kind.column, cols.items[1].kind);
    try std.testing.expectEqual(text.len - "order_".len, cols.start);

    var aw = std.Io.Writer.Allocating.init(a);
    try writeOffer(&aw.writer, cols, text.len);
    const parsed = try std.json.parseFromSlice(std.json.Value, a, aw.written(), .{});
    try std.testing.expectEqual(@as(usize, 2), parsed.value.object.get("items").?.array.items.len);
}

test "an offer with no candidates still starts at the word being typed, not the top of the cell" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var decls = DeclStore.init(std.testing.allocator);
    defer decls.deinit();
    var catalog = Catalog.init(std.testing.allocator);
    defer catalog.deinit();
    const cx = Completer{ .gpa = std.testing.allocator, .decls = &decls, .catalog = &catalog, .connect = false };

    const text = "SELECT * FROM sal";
    const none = try suggestFor(a, &cx, text, text.len);
    try std.testing.expectEqual(@as(usize, 0), none.items.len);
    try std.testing.expectEqual(@as(usize, 14), none.start);

    const gap = try suggestFor(a, &cx, "SELECT ", 7);
    try std.testing.expectEqual(@as(usize, 0), gap.items.len);
    try std.testing.expectEqual(@as(usize, 7), gap.start);

    // UTF-16 offsets too: `é` is two bytes, one unit.
    const wide = "SELECT 'é' FROM sal";
    const w = try suggestFor(a, &cx, wide, wide.len);
    var aw = std.Io.Writer.Allocating.init(a);
    try writeOfferIn(&aw.writer, w, wide.len, wide, true);
    try std.testing.expectEqualStrings("{\"start\":16,\"end\":19,\"items\":[]}", aw.written());
}

test "Tab's built-in functions are exactly the engine's: every scalar, aggregate and window function, nothing else" {
    const listed = struct {
        fn has(n: []const u8) bool {
            for (complete.builtin_functions) |f| if (std.mem.eql(u8, f.name, n)) return true;
            return false;
        }
    };
    for (eval.builtins) |b| if (!listed.has(b.name)) {
        std.debug.print("scalar builtin `{s}` is missing from complete.builtin_functions\n", .{b.name});
        return error.TestUnexpectedResult;
    };
    for (aggregates.specs) |sp| for (sp.names) |n| if (!listed.has(n)) {
        std.debug.print("aggregate `{s}` is missing from complete.builtin_functions\n", .{n});
        return error.TestUnexpectedResult;
    };
    inline for (@typeInfo(ast.WinKind).@"enum".fields) |f| try std.testing.expect(listed.has(f.name));
    const syntax = [_][]const u8{ "cast", "try_cast", "if" };
    for (complete.builtin_functions, 0..) |f, i| {
        for (complete.builtin_functions[0..i]) |prev| try std.testing.expect(!std.mem.eql(u8, prev.name, f.name));
        try std.testing.expect(std.mem.startsWith(u8, f.sig, f.name) and f.sig[f.name.len] == '(');
        const known = eval.lookupBuiltin(f.name) != null or
            aggregates.lookup(f.name) != null or
            std.meta.stringToEnum(ast.WinKind, f.name) != null or
            for (syntax) |x| (if (std.mem.eql(u8, x, f.name)) break true) else false;
        if (!known) {
            std.debug.print("complete.builtin_functions lists `{s}`, which the engine does not know\n", .{f.name});
            return error.TestUnexpectedResult;
        }
    }
}

test "a file's columns are offered with their type, and each word once" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "t.csv", .data = "name,amount,when_at\nx,1.5,2024-01-02\n" });
    const dir = try tmp.dir.realpathAlloc(a, ".");

    var decls = DeclStore.init(std.testing.allocator);
    defer decls.deinit();
    var catalog = Catalog.init(std.testing.allocator);
    defer catalog.deinit();
    const cx = Completer{ .gpa = std.testing.allocator, .decls = &decls, .catalog = &catalog, .connect = false };

    const text = try std.fmt.allocPrint(a, "SELECT * FROM '{s}/t.csv' WHERE na", .{dir});
    const n = try suggestFor(a, &cx, text, text.len);
    try std.testing.expectEqual(@as(usize, 1), n.items.len);
    try std.testing.expectEqual(complete.Kind.column, n.items[0].kind);
    try std.testing.expectEqualStrings("string", n.items[0].detail);

    var aw = std.Io.Writer.Allocating.init(a);
    try writeOffer(&aw.writer, n, text.len);
    const parsed = try std.json.parseFromSlice(std.json.Value, a, aw.written(), .{});
    const item = parsed.value.object.get("items").?.array.items[0].object;
    try std.testing.expectEqualStrings("string", item.get("detail").?.string);

    const w = try suggestFor(a, &cx, text[0 .. text.len - 2], text.len - 2);
    for (w.items) |c| if (std.mem.eql(u8, c.text, "when_at")) try std.testing.expect(c.detail.len > 0);
}

test "csvCells: quoted cells keep their commas and doubled quotes" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const c = try csvCells(ar.allocator(), "amt,\"numeric(10,2)\",\"say \"\"hi\"\"\"\r");
    try std.testing.expectEqual(@as(usize, 3), c.len);
    try std.testing.expectEqualStrings("amt", c[0]);
    try std.testing.expectEqualStrings("numeric(10,2)", c[1]);
    try std.testing.expectEqualStrings("say \"hi\"", c[2]);
}

test "letLiteral: every value folds back to itself" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const values = [_]Value{
        .null,                                               .{ .bool = true },
        .{ .int = std.math.minInt(i64) },                    .{ .int = std.math.maxInt(i64) },
        .{ .int = -7 },                                      .{ .float = 0.1 },
        .{ .float = -1.5e300 },                              .{ .float = 5e-324 },
        .{ .decimal = .{ .unscaled = -12345, .scale = 3 } }, .{ .string = "it's\nfine" },
        .{ .string = "" },                                   .{ .date = -1 },
        .{ .time = 86_399_999_999 },                         .{ .timestamp = -500_000 },
        .{ .timestamp = 1_767_268_800_123_456 },
    };
    for (values) |v| {
        const lit = (try letLiteral(a, v)).?;
        const src = try std.fmt.allocPrint(a, "LET x = {s};", .{lit});
        var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        const prog = try parser.parseSource(a, src, &diag);
        const back = try eval.constEval(a, prog.stmts[1].let_const.expr.?, &.{}, &.{});
        errdefer std.debug.print("{s} -> {any}\n", .{ lit, back });
        try std.testing.expectEqual(std.meta.activeTag(v), std.meta.activeTag(back));
        if (v != .null) try std.testing.expectEqual(std.math.Order.eq, eval.compareValues(v, back).?);
    }
    // bytes have no literal: the LET keeps its expression
    try std.testing.expect((try letLiteral(a, .{ .bytes = "\x00" })) == null);
}

test "utf16 offsets: a JavaScript editor's positions map to bytes and back" {
    const text = "SELECT 'é😀' AS x"; // é: 2 bytes, 1 unit; 😀: 4 bytes, 2 units
    try std.testing.expectEqual(@as(usize, 8), utf16ToByte(text, 8)); // before é
    try std.testing.expectEqual(@as(usize, 10), utf16ToByte(text, 9)); // after é
    try std.testing.expectEqual(@as(usize, 10), utf16ToByte(text, 10)); // inside the pair: clamped
    try std.testing.expectEqual(@as(usize, 14), utf16ToByte(text, 11)); // after 😀
    try std.testing.expectEqual(text.len, utf16ToByte(text, 1000));
    for ([_]usize{ 0, 8, 10, 14, text.len }) |b| try std.testing.expectEqual(b, utf16ToByte(text, byteToUtf16(text, b)));
    try std.testing.expectEqual(@as(usize, 17), byteToUtf16(text, text.len));
}

test "checkText: every problem in script order, known tables typed or not, nothing stops at the first" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const text =
        \\SELECT nope FROM RANGE(3);
        \\SELECT id, amt * 2 AS a2 FROM enrich WHERE day >= '2024-01-01';
        \\SELECT 1 +;
        \\SELECT missing FROM enrich;
        \\SELECT whatever FROM loose;
    ;
    const fields = [_]types.Schema.Field{
        .{ .name = "id", .ty = types.Type.init(.int) },
        .{ .name = "amt", .ty = types.Type.decimal(10, 2) },
        .{ .name = "day", .ty = types.Type.init(.date) },
    };
    const known = [_]analyze.KnownTable{ .{ .name = "enrich", .schema = .{ .fields = &fields } }, .{ .name = "loose" } };
    const issues = try checkText(a, text, "t.sql", ".", .{ .known = &known });
    try std.testing.expectEqual(@as(usize, 3), issues.len);
    try std.testing.expectEqual(@as(u32, 1), issues[0].pos.?.line);
    try std.testing.expect(std.mem.indexOf(u8, issues[0].msg, "`nope`") != null);
    try std.testing.expectEqual(@as(u32, 3), issues[1].pos.?.line);
    try std.testing.expectEqual(@as(u32, 4), issues[2].pos.?.line);
    try std.testing.expect(std.mem.indexOf(u8, issues[2].msg, "`missing`") != null);

    // not known: the name is the one problem on its line, and the rest still check
    const unknown = try checkText(a, "SELECT * FROM enrich;\nSELECT nope FROM RANGE(1);", "t.sql", ".", .{});
    try std.testing.expectEqual(@as(usize, 2), unknown.len);
    try std.testing.expect(std.mem.indexOf(u8, unknown[0].msg, "unknown source `enrich`") != null);

    // a cell that only declares is whole when asked; a script, not
    try std.testing.expectEqual(@as(usize, 0), (try checkText(a, "PARAM n INT DEFAULT 1;", "t.sql", ".", .{ .declarations_only = true })).len);
    try std.testing.expectEqual(@as(usize, 1), (try checkText(a, "PARAM n INT DEFAULT 1;", "t.sql", ".", .{})).len);
}
