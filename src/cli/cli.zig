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
//!
//! This file keeps the signal handling, the dispatch and the usage text. Each
//! command is its own file (`cmd_run.zig`, `cmd_check.zig`, `cmd_complete.zig`,
//! `cmd_serve.zig`, `repl.zig`, `kernel.zig`), with what they share in
//! `args.zig` (script and flags), `entry.zig` (statements and the session's
//! declarations), `prepare.zig` (an entry made runnable), `catalog.zig` (what Tab
//! fetches) and `wizard.zig` (`\connect`).

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

const scriptArg = @import("args.zig").scriptArg;
const unknownOption = @import("args.zig").unknownOption;
pub const parseLogFormat = @import("args.zig").parseLogFormat;
pub const CheckIssue = @import("cmd_check.zig").CheckIssue;
pub const CheckOpts = @import("cmd_check.zig").CheckOpts;
pub const checkText = @import("cmd_check.zig").checkText;
const cmdCheck = @import("cmd_check.zig").cmdCheck;
const cmdComplete = @import("cmd_complete.zig").cmdComplete;
pub const declareFrom = @import("cmd_complete.zig").declareFrom;
pub const utf16ToByte = @import("cmd_complete.zig").utf16ToByte;
pub const byteToUtf16 = @import("cmd_complete.zig").byteToUtf16;
pub const writeOfferIn = @import("cmd_complete.zig").writeOfferIn;
pub const writeOffer = @import("cmd_complete.zig").writeOffer;
const cmdRun = @import("cmd_run.zig").cmdRun;
const cmdServe = @import("cmd_serve.zig").cmdServe;
const endsComplete = @import("entry.zig").endsComplete;
const splitStatements = @import("entry.zig").splitStatements;
pub const DeclKind = @import("entry.zig").DeclKind;
pub const DeclId = @import("entry.zig").DeclId;
const declOf = @import("entry.zig").declOf;
pub const DeclStore = @import("entry.zig").DeclStore;
const Session = @import("repl.zig").Session;
pub const Catalog = @import("catalog.zig").Catalog;
pub const Completer = @import("catalog.zig").Completer;
pub const Offer = @import("catalog.zig").Offer;
const csvCells = @import("catalog.zig").csvCells;
const httpResources = @import("catalog.zig").httpResources;
pub const suggestFor = @import("catalog.zig").suggestFor;
const cmdRepl = @import("repl.zig").cmdRepl;
const entryStatements = @import("repl.zig").entryStatements;
pub const Pending = @import("prepare.zig").Pending;
pub const Prepared = @import("prepare.zig").Prepared;
pub const PrepareError = @import("prepare.zig").PrepareError;
pub const sessionText = @import("prepare.zig").sessionText;
pub const prepareEntry = @import("prepare.zig").prepareEntry;
pub const LetFreezer = @import("prepare.zig").LetFreezer;
pub const letLiteral = @import("prepare.zig").letLiteral;
pub const appendDisplaySinks = @import("repl.zig").appendDisplaySinks;
const isQuit = @import("repl.zig").isQuit;
const isHelp = @import("repl.zig").isHelp;
const threadFlagValue = @import("args.zig").threadFlagValue;
const testArgv = @import("args.zig").testArgv;

fn usage(w: anytype) !void {
    try w.writeAll(
        \\basalt — a SQL-driven data pipeline engine
        \\
        \\usage:
        \\  basalt run   <script>|-|-c <script> [-p key=value ...] [-j N] [--port N] [--host IP]
        \\               run a pipeline; HTTP mode when the script declares CREATE ENDPOINT
        \\  basalt serve <dir> [--port N] [--host IP] [--watch] [--log-format FMT] [--log-level LVL]
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
        \\  see https://leonardomb1.github.io/basalt/ (or docs/ in the source) for the dialect
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
        \\  ftp        ftp://[user[:pass]@]host/path or ftp://<conn>/path through
        \\             CREATE CONNECTION <conn> TYPE ftp, read only; anonymous by
        \\             default; each file is downloaded once per run and read
        \\             locally, a trailing / a folder with its subfolders
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
        \\  --host IP          listen address for HTTP mode (default 0.0.0.0, every interface)
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
    const src = @embedFile("repl.zig");
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
    var scraped: usize = 0;
    while (it.next()) |rest| {
        const end = std.mem.indexOfScalar(u8, rest, '"') orelse continue;
        scraped += 1;
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
    try std.testing.expect(scraped >= 17);
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

test {
    _ = @import("args.zig");
    _ = @import("catalog.zig");
    _ = @import("cmd_check.zig");
    _ = @import("cmd_complete.zig");
    _ = @import("cmd_run.zig");
    _ = @import("cmd_serve.zig");
    _ = @import("entry.zig");
    _ = @import("prepare.zig");
    _ = @import("repl.zig");
    _ = @import("wizard.zig");
}
