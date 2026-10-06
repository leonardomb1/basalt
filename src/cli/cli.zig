//! Command-line surface:
//!   basalt run   <script>|-c <script> [-p k=v ...] [-j N] [--port N]
//!   basalt check <script>|-c <script>
//!   basalt repl
//!   basalt kernel
//! `run` executes (HTTP mode when the script declares an endpoint); `check`
//! validates and plans without running; `serve <dir>` hosts every `@http` script
//! in a directory and reloads it on SIGHUP. A script comes from a file path or,
//! with `-c/--command`, inline; `@include` resolves against its directory, or the
//! cwd for an inline or stdin script. SIGTERM and SIGINT ask a run to stop at its
//! next boundary, which is how the control plane cancels a job. `--log-format
//! json` is read before parsing, so a parse error has a runtime error's shape.
//! Progress is a live line on a terminal, a `progress` event a second under a
//! JSON log, and nothing on a pipe or under `-q`. A server's own lines are its
//! output, so `serve` logs at `info` where a one-shot run logs at `warn`.
//!
//! `repl` is an interactive loop that runs on `;` and prints results through a
//! `write stdout` table sink. Declarations carry across entries in a `DeclStore`:
//! order-preserving, as one may reference another; re-declaring a kind and name
//! replaces the text in place; text is duped, as the input buffer is reused.
//! Each entry is parsed with the stored declarations as a prelude, and its own
//! are committed only once it parses. An `@include` in an entry lasts for that
//! entry; `\i` joins a file to the session. An entry that opens with `EXPLAIN`
//! renders its plan without running, as `basalt run` does. A LET's value is
//! frozen as a literal in the entry that declares it, since replaying `LET t =
//! now()` ahead of every later entry would give each its own instant. Arrow is
//! not a REPL format: a binary stream in a terminal is noise. The startup file,
//! `$XDG_CONFIG_HOME/basalt/repl.sql` (else `~/.config/...`), holds the
//! declarations a session starts with and is where `\save` writes.
//!
//! Completion (the REPL's Tab, `basalt complete`, the kernel's `complete`) draws
//! on the declarations in scope and a `Catalog` of what sources said, fetched
//! once each. A source that could not be asked is cached as empty, so a dead
//! connection costs one wait, not one per Tab. Without `connect`, only local
//! files, whose headers cost no round trip, contribute columns. The `\connect`
//! form asks for the keys the runtime reads (`parseDbConfig`,
//! `resolveStreamLoadConfig`, `http_client.connFromKvs`); a default with `<…>` in
//! it is only a pattern, and a blank there writes nothing.

const std = @import("std");
const parser = @import("../lang/sql_parser.zig");
const include = @import("../lang/include.zig");
const Editor = @import("line.zig").Editor;
const LineResult = @import("line.zig").Result;
const view = @import("view.zig");
const form = @import("form.zig");
const hilite = @import("hilite.zig");
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

/// Asks the run to stop at its next boundary with one atomic store (async-signal-safe).
/// A second signal exits 130 at once, so ^C ^C is not held hostage by a slow read.
fn onTerminate(_: i32) callconv(.c) void {
    if (runtime.aborting()) std.posix.exit(130);
    runtime.requestAbort();
}

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

const Source = struct { label: []const u8, text: []const u8, dir: []const u8 = "." };

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

const valued_flags = [_][]const u8{ "-p", "--param", "-j", "--threads", "--format", "--log-format", "--log-level", "--port", "--max-rows", "--pos" };

/// The first argument that is neither a flag nor a flag's value, `-` included, so the
/// script may come before or after its flags.
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
        .{ .argv = &.{ "basalt", "check", "-p", "out.sql" }, .want = null },
    };
    for (cases) |c| try std.testing.expectEqual(c.want, scriptArg(try testArgv(a, c.argv)));
}

/// Reports a dash-prefixed argument no branch claimed (`-` alone is stdin), so the
/// caller exits 2: a typo like `--treads` once ran single-threaded without a word.
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

fn printDiag(stderr: *std.Io.Writer, label: []const u8, tag: []const u8, pos: ?ast.Pos, msg: []const u8) !void {
    if (pos) |p|
        try stderr.print("{s}:{d}:{d}: error{s}: {s}\n", .{ label, p.line, p.col, tag, msg })
    else
        try stderr.print("{s}: error{s}: {s}\n", .{ label, tag, msg });
}

const ErrOut = struct {
    w: *std.Io.Writer,
    json: bool,
    label: []const u8,
    bare: bool = false,

    const Located = struct {
        msg: []const u8,
        pos: ?ast.Pos = null,
        end: ?ast.Pos = null,
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

fn wantsJsonLog(args: [][:0]u8) bool {
    var i: usize = 2;
    while (i + 1 < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--log-format")) return std.mem.eql(u8, args[i + 1], "json");
    }
    return false;
}

/// The AST slices into `src.text` and the included files' texts, so all must outlive
/// it. An error in an included file names that file's path and lines.
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

pub const CheckIssue = struct { msg: []const u8, pos: ?ast.Pos = null, end: ?ast.Pos = null, file: ?[]const u8 = null };

pub const CheckOpts = struct {
    overrides: []const analyze.ParamOverride = &.{},
    known: []const analyze.KnownTable = &.{},
    declarations_only: bool = false,
};

/// Every problem in `text`, in script order: each statement that does not parse
/// (parsing resumes at the next), then each that does not check. Nothing runs or connects.
pub fn checkText(a: std.mem.Allocator, text: []const u8, label: []const u8, dir: []const u8, opts: CheckOpts) ![]CheckIssue {
    var issues = std.array_list.Managed(CheckIssue).init(a);
    var names = std.array_list.Managed([]const u8).init(a);
    for (opts.known) |k| try names.append(k.name);
    var perrs = std.array_list.Managed(parser.Diagnostic).init(a);
    var idiag: include.Diag = .{};
    const prog = include.loadProgramOpts(a, text, label, dir, &idiag, .{ .known_tables = names.items, .errors = &perrs }) catch |e| switch (e) {
        error.OutOfMemory => return e,
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

/// What Tab would offer at byte `--pos`, as JSON. Connections are asked for their
/// tables and columns only under `--connect`, as that is a round trip to each.
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

/// The declarations a text makes, by its statements' first words, so a script still
/// being typed, which will not parse, still has its names in scope.
pub fn declareFrom(decls: *DeclStore, arena: std.mem.Allocator, text: []const u8) !void {
    for (try splitStatements(arena, text)) |st| {
        const id = declOf(st) orelse continue;
        if (id.kind == .endpoint) continue;
        try decls.put(id, st);
    }
}

/// Byte offset of UTF-16 offset `u`, a JavaScript editor's cursor. Inside a surrogate
/// pair or past the end clamps back to a boundary; non-UTF-8 bytes count one unit.
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

/// With `utf16`, offsets count UTF-16 units of `text`, as a JavaScript editor does.
pub fn writeOfferIn(w: *std.Io.Writer, offer: Offer, cursor: usize, text: []const u8, utf16: bool) !void {
    var o = offer;
    var c = cursor;
    if (utf16) {
        o.start = byteToUtf16(text, offer.start);
        c = byteToUtf16(text, cursor);
    }
    return writeOffer(w, o, c);
}

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

/// Next statement-level `;`, skipping strings, dollar quotes and comments by the
/// lexer's own rules, so the REPL agrees with the parser on where a statement ends.
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

/// Run or wait for more? SQL is whole at a top-level `;` unless the parser, given
/// the session, runs out of input: an open `CREATE FUNCTION` body holds `;`s before `END;`.
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

fn endsComplete(s: []const u8) bool {
    const t = std.mem.trim(u8, s, " \t\r\n");
    if (t.len == 0 or t[t.len - 1] != ';') return false;
    var i: usize = 0;
    while (nextTopSemi(t, i)) |p| : (i = p + 1) {
        if (p == t.len - 1) return true;
    }
    return false;
}

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

fn nextWord(s: []const u8, i: *usize) ?[]const u8 {
    while (i.* < s.len and std.ascii.isWhitespace(s[i.*])) i.* += 1;
    if (i.* >= s.len) return null;
    const start = i.*;
    while (i.* < s.len and !std.ascii.isWhitespace(s[i.*])) i.* += 1;
    return s[start..i.*];
}

/// Bare identifiers only: the dialect has no quoted declaration names.
fn identPrefix(w: []const u8) []const u8 {
    var n: usize = 0;
    while (n < w.len and (std.ascii.isAlphanumeric(w[n]) or w[n] == '_')) n += 1;
    return w[0..n];
}

/// The session declaration a statement makes, by its first words, or null. A resource
/// is named `conn.name`, as two connections may each have one of the same name.
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

const Session = struct {
    decls: DeclStore,
    format: runtime.StdoutFormat = .table,
    tty: bool = false,
    catalog: Catalog,
    last_entry: ?[]u8 = null,
    announce: bool = true,

    fn completer(self: *Session) Completer {
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

const Connector = struct {
    name: []const u8,
    blurb: []const u8,
    fields: []const Field,
    const Field = struct {
        key: []const u8,
        hint: []const u8 = "",
        default: []const u8 = "",
        secret: bool = false,
        int: bool = false,
        choices: []const []const u8 = &.{},
        omit: []const u8 = "",
        when: ?struct { key: []const u8, value: []const u8 } = null,
    };
};
const tls_choices: []const []const u8 = &.{ "off", "require", "insecure" };
const connectors = [_]Connector{
    .{ .name = "postgres", .blurb = "PostgreSQL (source and sink)", .fields = &.{
        .{ .key = "host", .default = "localhost" },
        .{ .key = "port", .default = "5432", .int = true },
        .{ .key = "database" },
        .{ .key = "user" },
        .{ .key = "password", .secret = true },
        .{ .key = "tls", .choices = tls_choices, .default = "off" },
    } },
    .{ .name = "mysql", .blurb = "MySQL / MariaDB (source and sink)", .fields = &.{
        .{ .key = "host", .default = "localhost" },
        .{ .key = "port", .default = "3306", .int = true },
        .{ .key = "database" },
        .{ .key = "user" },
        .{ .key = "password", .secret = true },
        .{ .key = "tls", .choices = tls_choices, .default = "off" },
    } },
    .{ .name = "sqlserver", .blurb = "SQL Server (source and sink; host\\INSTANCE resolves the port)", .fields = &.{
        .{ .key = "host", .hint = "host\\INSTANCE for a named instance" },
        .{ .key = "port", .hint = "blank: 1433, or the instance's", .int = true },
        .{ .key = "database" },
        .{ .key = "auth", .choices = &.{ "sql", "ntlm", "kerberos", "aad" }, .default = "sql", .omit = "sql", .hint = "a SQL login, a domain account, Entra ID" },
        .{ .key = "realm", .hint = "the domain's DNS name, as CORP.LOCAL", .when = .{ .key = "auth", .value = "kerberos" } },
        .{ .key = "user" },
        .{ .key = "password", .secret = true },
        .{ .key = "tls", .choices = tls_choices, .default = "require" },
    } },
    .{ .name = "starrocks", .blurb = "StarRocks (read through the FE, write by stream load)", .fields = &.{
        .{ .key = "host", .hint = "the FE" },
        .{ .key = "port", .default = "9030", .int = true, .hint = "the FE's query port" },
        .{ .key = "load_url", .default = "http://<be-host>:8040", .hint = "a BE or CN, for stream load" },
        .{ .key = "database" },
        .{ .key = "user", .default = "root" },
        .{ .key = "password", .secret = true },
    } },
    .{ .name = "doris", .blurb = "Apache Doris (read through the FE, write by stream load)", .fields = &.{
        .{ .key = "host", .hint = "the FE" },
        .{ .key = "port", .default = "9030", .int = true, .hint = "the FE's query port" },
        .{ .key = "load_url", .default = "http://<be-host>:8040", .hint = "a BE, for stream load" },
        .{ .key = "database" },
        .{ .key = "user", .default = "root" },
        .{ .key = "password", .secret = true },
    } },
    .{ .name = "sftp", .blurb = "an SFTP server (files read and written as sftp://<name>/path)", .fields = &.{
        .{ .key = "host" },
        .{ .key = "port", .default = "22", .int = true },
        .{ .key = "user" },
        .{ .key = "key_file", .hint = "blank for a password" },
        .{ .key = "password", .secret = true },
        .{ .key = "host_key", .hint = "pinned; blank for ~/.ssh/known_hosts" },
    } },
    .{ .name = "smb", .blurb = "a Windows file share or Samba server (files read and written as smb://<name>/path)", .fields = &.{
        .{ .key = "host" },
        .{ .key = "port", .default = "445", .int = true },
        .{ .key = "domain", .hint = "blank for a local account" },
        .{ .key = "realm", .hint = "Kerberos, as CORP.LOCAL; blank for NTLM" },
        .{ .key = "user" },
        .{ .key = "password", .secret = true },
        .{ .key = "share", .hint = "blank to name it in each path" },
    } },
    .{ .name = "http", .blurb = "a REST API (paginated sources, an endpoint sink)", .fields = &.{
        .{ .key = "base_url" },
        .{ .key = "auth", .choices = &.{ "none", "bearer", "basic" }, .default = "none", .omit = "none" },
    } },
};

const Wizard = struct {
    conn: Connector,
    user_ph: [96]u8 = undefined,
    pass_ph: [96]u8 = undefined,

    fn convention(name: []const u8, suffix: []const u8, buf: []u8) []const u8 {
        var n: usize = 0;
        for (name) |ch| {
            if (n + suffix.len + 1 >= buf.len) break;
            buf[n] = std.ascii.toUpper(ch);
            n += 1;
        }
        if (n == 0) {
            @memcpy(buf[0..4], "NAME");
            n = 4;
        }
        buf[n] = '_';
        @memcpy(buf[n + 1 ..][0..suffix.len], suffix);
        return buf[0 .. n + 1 + suffix.len];
    }

    fn refresh(ctx: *anyopaque, fields: []form.Field) void {
        const self: *Wizard = @ptrCast(@alignCast(ctx));
        const name = fields[0].value();
        for (self.conn.fields, fields[1..]) |cf, *f| {
            if (cf.when) |w| {
                f.hidden = true;
                for (self.conn.fields, fields[1..]) |other, of| if (std.mem.eql(u8, other.key, w.key)) {
                    f.hidden = !std.mem.eql(u8, of.value(), w.value);
                };
            }
            if (cf.default.len > 0) continue;
            if (cf.secret) {
                @memcpy(self.pass_ph[0..4], "env:");
                f.placeholder = self.pass_ph[0 .. 4 + convention(name, "PASS", self.pass_ph[4..]).len];
            } else if (std.mem.eql(u8, cf.key, "user")) {
                @memcpy(self.user_ph[0..4], "env:");
                f.placeholder = self.user_ph[0 .. 4 + convention(name, "USER", self.user_ph[4..]).len];
            }
        }
    }

    fn check(_: *anyopaque, fields: []form.Field) ?form.Form.Problem {
        const name = fields[0].value();
        if (name.len == 0) return .{ .field = 0, .why = "a name is needed — the script calls the connection by it" };
        if (!std.ascii.isAlphabetic(name[0])) return .{ .field = 0, .why = "a name starts with a letter" };
        for (name) |ch| if (!std.ascii.isAlphanumeric(ch) and ch != '_')
            return .{ .field = 0, .why = "a name is letters, digits and _" };
        return null;
    }
};

/// Blank credentials are left out, meaning the runtime's `env(NAME_USER)` and
/// `env(NAME_PASS)` convention; a typed password is never echoed.
fn connectWizard(alloc: std.mem.Allocator, type_arg: []const u8, sess: *Session, msg: *std.Io.Writer) !void {
    var term = form.Term.init(alloc, msg);
    var which: ?Connector = null;
    if (type_arg.len > 0) {
        for (connectors) |c| if (std.ascii.eqlIgnoreCase(c.name, type_arg)) {
            which = c;
        };
        if (which == null) {
            try msg.print("error: no connector `{s}` — one of", .{type_arg});
            for (connectors, 0..) |c, i| try msg.print("{s} {s}", .{ if (i == 0) "" else ",", c.name });
            return msg.writeAll("\n");
        }
    } else {
        var items: [connectors.len]form.Item = undefined;
        for (connectors, &items) |c, *it| it.* = .{ .name = c.name, .detail = c.blurb };
        var picker = form.Picker{ .title = "new connection", .items = &items };
        if (!try term.run(&picker)) return msg.writeAll("cancelled; nothing made\n");
        which = connectors[picker.focus];
    }
    const conn = which.?;

    var fields = std.array_list.Managed(form.Field).init(alloc);
    defer {
        for (fields.items) |*f| f.buf.deinit();
        fields.deinit();
    }
    try fields.append(form.Field.init(alloc, .{ .label = "name", .hint = "how scripts refer to it, e.g. erp" }));
    for (conn.fields) |cf| {
        var choice: usize = 0;
        for (cf.choices, 0..) |ch, i| if (std.mem.eql(u8, ch, cf.default)) {
            choice = i;
        };
        try fields.append(form.Field.init(alloc, .{
            .label = cf.key,
            .hint = cf.hint,
            .placeholder = cf.default,
            .secret = cf.secret,
            .digits = cf.int,
            .choices = cf.choices,
            .choice = choice,
        }));
    }
    var wiz = Wizard{ .conn = conn };
    var title_buf: [64]u8 = undefined;
    var f = form.Form{
        .title = std.fmt.bufPrint(&title_buf, "new {s} connection", .{conn.name}) catch "new connection",
        .fields = fields.items,
        .hooks = .{ .ctx = &wiz, .refresh = Wizard.refresh, .check = Wizard.check },
    };
    if (!try term.run(&f)) return msg.writeAll("cancelled; nothing made\n");

    const name = fields.items[0].value();
    var upper_buf: [96]u8 = undefined;
    const user_conv = Wizard.convention(name, "USER", &upper_buf);
    var pass_buf: [96]u8 = undefined;
    const pass_conv = Wizard.convention(name, "PASS", &pass_buf);

    var stmt = std.array_list.Managed(u8).init(alloc);
    defer stmt.deinit();
    var shown = std.array_list.Managed(u8).init(alloc);
    defer shown.deinit();
    const out = [_]*std.array_list.Managed(u8){ &stmt, &shown };
    for (out) |o| try o.writer().print("CREATE CONNECTION {s} TYPE {s} OPTIONS (", .{ name, conn.name });
    var first = true;
    for (conn.fields, fields.items[1..]) |cf, *fld| {
        if (fld.hidden) continue;
        var v = fld.value();
        if (cf.choices.len > 0 and std.mem.eql(u8, v, cf.omit)) continue;
        if (v.len == 0) {
            if (cf.secret or std.mem.eql(u8, cf.key, "user")) continue;
            if (std.mem.indexOfScalar(u8, cf.default, '<') != null) continue;
            v = cf.default;
        }
        if (v.len == 0) continue;
        const sep: []const u8 = if (first) "" else ", ";
        first = false;
        if (std.mem.startsWith(u8, v, "env:")) {
            const var_name = v[4..];
            if (std.mem.eql(u8, var_name, if (cf.secret) pass_conv else user_conv)) {
                first = sep.len == 0;
                continue;
            }
            for (out) |o| try o.writer().print("{s}{s} = env('{s}')", .{ sep, cf.key, var_name });
        } else if (cf.int) {
            for (out) |o| try o.writer().print("{s}{s} = {s}", .{ sep, cf.key, v });
        } else {
            for (out) |o| try o.writer().print("{s}{s} = '", .{ sep, cf.key });
            for (v) |ch| try stmt.appendSlice(if (ch == '\'') "''" else &.{ch});
            if (cf.secret) try shown.appendSlice("********") else for (v) |ch| try shown.appendSlice(if (ch == '\'') "''" else &.{ch});
            for (out) |o| try o.append('\'');
        }
    }
    for (out) |o| try o.appendSlice(");");
    try msg.writeAll("\n");
    try writeHighlighted(alloc, msg, shown.items, term.colors.off.len > 0);
    try msg.writeAll("\n");
    try runBlock(alloc, stmt.items, sess, msg);
    var declared = false;
    for (sess.decls.items.items) |e| if (e.kind == .connection and std.ascii.eqlIgnoreCase(e.name, name)) {
        declared = true;
    };
    if (!declared) return;

    if (!std.mem.eql(u8, conn.name, "http")) {
        var reach = form.Confirm{ .question = "reach it now?", .yes = true };
        if (try term.run(&reach) and reach.yes) {
            const reached = connTables(&sess.completer(), name).len > 0;
            try msg.print("  {s}\n", .{if (reached) "reached" else "could not reach it (or it has no tables) — \\c test retries; the connection stays declared"});
        }
    }
    var save = form.Confirm{ .question = "save to the startup file, so every session has it?", .yes = false };
    if (try term.run(&save) and save.yes) try saveDecls(alloc, "", sess, msg);
}

fn writeHighlighted(gpa: std.mem.Allocator, w: *std.Io.Writer, text: []const u8, color: bool) !void {
    if (!color) return w.writeAll(text);
    const styles = try gpa.alloc(hilite.Style, text.len);
    defer gpa.free(styles);
    hilite.scan(text, styles);
    var style: hilite.Style = .plain;
    for (text, styles) |ch, st| {
        if (st != style) {
            style = st;
            try w.writeAll(hilite.sgr(st));
        }
        try w.writeByte(ch);
    }
    try w.writeAll(hilite.sgr_reset);
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

pub const Completer = struct {
    gpa: std.mem.Allocator,
    decls: *const DeclStore,
    catalog: *Catalog,
    connect: bool = true,
};

pub const Offer = struct { start: usize = 0, items: []const complete.Candidate = &.{} };

/// How Tab asks a source a question without printing anything; errors read as no rows.
fn fetchColumn(cx: *const Completer, select: []const u8) []const []const u8 {
    const a = cx.catalog.arena.allocator();
    const rows = fetchRows(cx, select);
    const out = a.alloc([]const u8, rows.len) catch return &.{};
    for (rows, out) |r, *o| o.* = r[0];
    return out;
}

/// Every row of `SELECT ...` as its cells, split as basalt's own CSV writes them:
/// on commas outside quotes, a doubled quote read as one.
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

fn connTables(cx: *const Completer, conn: []const u8) []const []const u8 {
    if (cx.catalog.tables.get(conn)) |t| return t;
    if (!cx.connect) return &.{};
    const a = cx.catalog.arena.allocator();
    const q = std.fmt.allocPrint(a, "SELECT table_schema || '.' || table_name AS t FROM {s}.QUERY($$SELECT TABLE_SCHEMA AS table_schema, TABLE_NAME AS table_name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE IN ('BASE TABLE', 'VIEW') AND TABLE_SCHEMA NOT IN ('information_schema', 'pg_catalog', 'mysql', 'performance_schema', 'sys', '_statistics_') ORDER BY 1, 2$$)", .{conn}) catch return &.{};
    const rows = fetchColumn(cx, q);
    cx.catalog.tables.put(a.dupe(u8, conn) catch return rows, rows) catch {};
    return rows;
}

/// Table names are queried in upper case, as `SHOW TABLES` spells them, and
/// aliased so every dialect answers the same names.
fn sourceColumns(cx: *const Completer, key: []const u8) []const complete.Column {
    if (cx.catalog.columns.get(key)) |c| return c;
    const a = cx.catalog.arena.allocator();
    var rows: []const complete.Column = &.{};
    if (key[0] == '\'') {
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
        const q = std.fmt.allocPrint(a, "SELECT col_name, col_type FROM {s}.QUERY($$SELECT COLUMN_NAME AS col_name, DATA_TYPE AS col_type FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{s}' AND TABLE_NAME = '{s}' ORDER BY ORDINAL_POSITION$$)", .{ conn, schema, tbl }) catch return rows;
        const got = fetchRows(cx, q);
        const cols = a.alloc(complete.Column, got.len) catch return rows;
        for (got, cols) |r, *c| c.* = .{ .name = r[0], .type = if (r.len > 1) r[1] else "" };
        rows = cols;
    }
    cx.catalog.columns.put(a.dupe(u8, key) catch return rows, rows) catch {};
    return rows;
}

fn connTypeOf(text: []const u8) ?[]const u8 {
    var it = std.mem.tokenizeAny(u8, text, " \t\r\n(");
    while (it.next()) |w| {
        if (std.ascii.eqlIgnoreCase(w, "type")) return it.next();
    }
    return null;
}

fn suggest(ctx: *anyopaque, arena: std.mem.Allocator, text: []const u8, cursor: usize) anyerror!Editor.Suggestions {
    const sess: *Session = @ptrCast(@alignCast(ctx));
    const cx = sess.completer();
    const offer = try suggestFor(arena, &cx, text, cursor);
    const items = try arena.alloc([]const u8, offer.items.len);
    for (offer.items, items) |cand, *it| it.* = cand.text;
    return .{ .start = offer.start, .items = items };
}

/// Never asks for the columns of a name the cursor is still typing at the end of the
/// text, which would send a catalog query on every keystroke.
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

/// A blank line also runs a pending buffer, which `echo ... | basalt repl` relies on.
/// After a ^C the abort flag is reset, so the session is not left poisoned.
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

pub const Pending = struct { id: DeclId, text: []const u8 };

pub const Prepared = struct {
    text: []const u8,
    entry_at: usize,
    prog: ast.Program,
    pending: []const Pending,
    executable: usize,
};

pub const PrepareError = error{ ParseFailed, EndpointInSession, OutOfMemory };

/// A declaration the entry makes again is left out of the prelude, so no name is declared twice.
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
        if (id.kind == .endpoint) return error.EndpointInSession;
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

    pub fn commit(self: *const LetFreezer, decls: *DeclStore) !void {
        for (self.frozen.items) |f| {
            for (decls.items.items) |e| {
                if (e.kind == .let and std.ascii.eqlIgnoreCase(e.name, f.name)) break;
            } else continue;
            try decls.put(.{ .kind = .let, .name = f.name }, f.text);
        }
    }
};

/// A SQL expression that folds back to exactly `v`, or null when there is no literal
/// form. A date, an exponent or the one overflowing int goes through a CAST of its text.
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

/// Logs errors only, but not `quiet`, which would swallow the entry's own `PRINT`s.
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
        \\  \view, \v               the last result full-screen: arrows move, s sorts, / filters, q leaves
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

fn nextVal(args: [][:0]u8, i: *usize, flag: []const u8, stderr: *std.Io.Writer) !?[]const u8 {
    i.* += 1;
    if (i.* >= args.len) {
        try stderr.print("error: missing value after `{s}`\n", .{flag});
        return null;
    }
    return args[i.*];
}

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

    try std.testing.expect(!endsComplete("SELECT ';' AS x"));
    try std.testing.expect(endsComplete("SELECT ';' AS x;"));
    try std.testing.expect(!endsComplete("SELECT 'it''s;"));
    try std.testing.expect(endsComplete("SELECT 'it''s;' AS x;"));
    try std.testing.expect(!endsComplete("SELECT \"a\\\";"));
    try std.testing.expect(endsComplete("SELECT \"a;b\" AS x;"));
    try std.testing.expect(!endsComplete("SELECT 1 -- ;"));
    try std.testing.expect(!endsComplete("/* ; */"));
    try std.testing.expect(endsComplete("/* ; */ SELECT 1;"));

    try std.testing.expect(!endsComplete("FROM c.QUERY($$a;b$$)"));
    try std.testing.expect(endsComplete("FROM c.QUERY($$a;b$$);"));
    try std.testing.expect(!endsComplete("FROM c.QUERY($q$a;b$q$)"));
    try std.testing.expect(endsComplete("FROM c.QUERY($q$a;b$q$);"));
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
        \\  see docs/language.md for the dialect
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
        \\             (or smb://user@host/share/path), read and written; NTLMv2 or
        \\             Kerberos (realm), every message signed (SMB 2.1 to 3.1.1)
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
    try std.testing.expect((try letLiteral(a, .{ .bytes = "\x00" })) == null);
}

test "utf16 offsets: a JavaScript editor's positions map to bytes and back" {
    const text = "SELECT 'é😀' AS x";
    try std.testing.expectEqual(@as(usize, 8), utf16ToByte(text, 8));
    try std.testing.expectEqual(@as(usize, 10), utf16ToByte(text, 9));
    try std.testing.expectEqual(@as(usize, 10), utf16ToByte(text, 10));
    try std.testing.expectEqual(@as(usize, 14), utf16ToByte(text, 11));
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

    const unknown = try checkText(a, "SELECT * FROM enrich;\nSELECT nope FROM RANGE(1);", "t.sql", ".", .{});
    try std.testing.expectEqual(@as(usize, 2), unknown.len);
    try std.testing.expect(std.mem.indexOf(u8, unknown[0].msg, "unknown source `enrich`") != null);

    try std.testing.expectEqual(@as(usize, 0), (try checkText(a, "PARAM n INT DEFAULT 1;", "t.sql", ".", .{ .declarations_only = true })).len);
    try std.testing.expectEqual(@as(usize, 1), (try checkText(a, "PARAM n INT DEFAULT 1;", "t.sql", ".", .{})).len);
}
