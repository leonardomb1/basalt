//! `basalt kernel`: a long-lived session for a notebook or editor frontend.
//!
//! The REPL carries declarations across entries; this exposes the same session
//! to a program. Each script runs against every `CREATE CONNECTION`, `CREATE
//! FUNCTION`, `CREATE RESOURCE`, `PARAM` and `LET` an earlier script declared —
//! the declarations are kept as text and replayed ahead of the next script, so
//! a script sees exactly what a single file holding them all would. A `WITH`
//! belongs to its query and is not kept: two scripts may reuse a CTE name.
//!
//! Protocol. Requests are NDJSON on stdin, one object per line:
//!
//!   {"op":"run","id":"c1","script":"SELECT 1;","params":{"k":"v"},"format":"arrow"}
//!   {"op":"cancel"}             stop the running script; the session survives
//!   {"op":"reset"}              forget every declaration
//!   {"op":"close"}              exit (as does EOF on stdin)
//!
//! `id` is echoed on every frame the request produces; `params` binds PARAMs for
//! that script only; `format` overrides `--format` for that script. SIGINT also
//! cancels the running script rather than killing the process.
//!
//! Replies are frames on stdout. Each frame is a JSON header line, and a `data`
//! header is followed by exactly `len` raw bytes:
//!
//!   {"type":"data","id":"c1","len":1234}\n<1234 bytes>
//!   {"type":"status","id":"c1","ok":true,"elapsed_ms":8,"declared":[...]}\n
//!
//! `data` carries whatever the script wrote to stdout — the Arrow IPC streams,
//! NDJSON or CSV of its results, in order, possibly split over several frames.
//! Exactly one `status` ends each request; after it, the script wrote nothing
//! more. A failed script's status carries `error` with the message, the line and
//! column in the script as sent (not in the replayed declarations), and whether
//! it is `transient`. Logs and `PRINT` stay on stderr.

const std = @import("std");
const cli = @import("cli.zig");
const include = @import("../lang/include.zig");
const ast = @import("../lang/ast.zig");
const runtime = @import("../runtime/run.zig");
const analyze = @import("../runtime/analyze.zig");
const obs = @import("../runtime/obs.zig");

/// One request line. Unknown fields are ignored, so a newer frontend can talk to
/// an older kernel.
const Request = struct {
    op: []const u8,
    id: ?[]const u8 = null,
    script: ?[]const u8 = null,
    params: ?std.json.Value = null,
    format: ?[]const u8 = null,
};

/// Requests the stdin thread hands to the main loop. `cancel` never queues: it
/// has to reach a script that is busy running.
const Queue = struct {
    mu: std.Thread.Mutex = .{},
    cond: std.Thread.Condition = .{},
    items: std.array_list.Managed([]u8),
    eof: bool = false,
    /// Id of the script now running, so a late cancel cannot stop the next one.
    running: ?[]const u8 = null,
    active: bool = false,

    fn push(self: *Queue, line: []u8) !void {
        self.mu.lock();
        defer self.mu.unlock();
        try self.items.append(line);
        self.cond.signal();
    }

    fn close(self: *Queue) void {
        self.mu.lock();
        defer self.mu.unlock();
        self.eof = true;
        self.cond.signal();
    }

    /// Next request line, or null at EOF. Caller owns the line.
    fn pop(self: *Queue) ?[]u8 {
        self.mu.lock();
        defer self.mu.unlock();
        while (self.items.items.len == 0 and !self.eof) self.cond.wait(&self.mu);
        if (self.items.items.len == 0) return null;
        return self.items.orderedRemove(0);
    }

    fn setRunning(self: *Queue, id: ?[]const u8) void {
        self.mu.lock();
        defer self.mu.unlock();
        self.running = id;
        self.active = id != null;
    }

    /// Abort the running script if `id` names it (or names nothing).
    fn cancel(self: *Queue, id: ?[]const u8) void {
        self.mu.lock();
        defer self.mu.unlock();
        if (!self.active) return;
        if (id) |want| if (self.running) |cur| if (!std.mem.eql(u8, want, cur)) return;
        runtime.requestAbort();
    }
};

/// The real stdout, kept once fd 1 starts being swapped for a capture pipe.
/// Frames from the pump thread and the main loop share it.
const Out = struct {
    fd: std.posix.fd_t,
    mu: std.Thread.Mutex = .{},

    fn writeAll(self: *Out, bytes: []const u8) void {
        var off: usize = 0;
        while (off < bytes.len) {
            const n = std.posix.write(self.fd, bytes[off..]) catch return;
            if (n == 0) return;
            off += n;
        }
    }

    fn data(self: *Out, id: []const u8, bytes: []const u8) void {
        var hbuf: [512]u8 = undefined;
        var w = std.Io.Writer.fixed(&hbuf);
        w.writeAll("{\"type\":\"data\",\"id\":") catch return;
        std.json.Stringify.encodeJsonString(clip(id), .{}, &w) catch return;
        w.print(",\"len\":{d}}}\n", .{bytes.len}) catch return;
        self.mu.lock();
        defer self.mu.unlock();
        self.writeAll(w.buffered());
        self.writeAll(bytes);
    }

    /// One JSON header line, already rendered.
    fn line(self: *Out, bytes: []const u8) void {
        self.mu.lock();
        defer self.mu.unlock();
        self.writeAll(bytes);
    }
};

/// Ids are echoed into a fixed header buffer; a frontend has no business sending
/// a longer one, and a clipped echo still pairs with the request in order.
fn clip(id: []const u8) []const u8 {
    return id[0..@min(id.len, 256)];
}

fn onInterrupt(_: i32) callconv(.c) void {
    runtime.requestAbort();
}

const Opts = struct {
    format: runtime.StdoutFormat = .arrow,
    threads: usize = 1,
    log: runtime.LogConfig = .{},
};

pub fn cmdKernel(alloc: std.mem.Allocator, args: [][:0]u8) !u8 {
    var err_buf: [4096]u8 = undefined;
    var err_file = std.fs.File.stderr().writer(&err_buf);
    const stderr = &err_file.interface;
    defer stderr.flush() catch {};

    var opts = Opts{ .threads = std.Thread.getCpuCount() catch 1 };
    // A session's own logs would interleave with a frontend's; warnings and
    // errors only unless asked, and never the per-run summary — the status
    // frame carries it.
    opts.log = .{ .level = .warn, .summary = .none };
    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        const a = args[i];
        if (std.mem.eql(u8, a, "--format")) {
            i += 1;
            if (i >= args.len) return usageErr(stderr, "missing value after `--format`");
            opts.format = std.meta.stringToEnum(runtime.StdoutFormat, args[i]) orelse
                return usageErr(stderr, "--format must be table|json|csv|tsv|arrow");
        } else if (std.mem.eql(u8, a, "-j") or std.mem.eql(u8, a, "--threads")) {
            i += 1;
            if (i >= args.len) return usageErr(stderr, "missing value after `--threads`");
            opts.threads = @max(1, std.fmt.parseInt(usize, args[i], 10) catch
                return usageErr(stderr, "invalid --threads"));
        } else if (std.mem.eql(u8, a, "--log-format")) {
            i += 1;
            if (i >= args.len) return usageErr(stderr, "missing value after `--log-format`");
            opts.log.format = cli.parseLogFormat(args[i]) orelse return usageErr(stderr, "--log-format must be auto|text|json");
        } else if (std.mem.eql(u8, a, "--log-level")) {
            i += 1;
            if (i >= args.len) return usageErr(stderr, "missing value after `--log-level`");
            opts.log.level = obs.Level.parse(args[i]) orelse return usageErr(stderr, "--log-level must be error|warn|info|debug");
        } else {
            try stderr.print("error: unknown option `{s}` for `kernel` — see `basalt help`\n", .{a});
            return 2;
        }
    }

    // ^C cancels the script, never the session. The CLI's own handler exits on a
    // second signal, which would take every declaration with it.
    const act = std.posix.Sigaction{ .handler = .{ .handler = onInterrupt }, .mask = std.posix.sigemptyset(), .flags = 0 };
    std.posix.sigaction(std.posix.SIG.INT, &act, null);

    // Frames go to a private copy of stdout; fd 1 itself points at stderr between
    // scripts (so a stray write cannot corrupt the stream) and at a capture pipe
    // while one runs.
    var out = Out{ .fd = try std.posix.dup(std.posix.STDOUT_FILENO) };
    defer std.posix.close(out.fd);
    try std.posix.dup2(std.posix.STDERR_FILENO, std.posix.STDOUT_FILENO);

    var queue = Queue{ .items = std.array_list.Managed([]u8).init(alloc) };
    defer {
        for (queue.items.items) |l| alloc.free(l);
        queue.items.deinit();
    }
    const reader = try std.Thread.spawn(.{}, readRequests, .{ alloc, &queue });
    reader.detach();

    var decls = cli.DeclStore.init(alloc);
    defer decls.deinit();

    while (queue.pop()) |raw| {
        defer alloc.free(raw);
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const req = std.json.parseFromSliceLeaky(Request, a, raw, .{ .ignore_unknown_fields = true }) catch {
            try reply(a, &out, null, .{ .ok = false, .@"error" = .{ .msg = "request is not a JSON object with an `op`" } });
            continue;
        };
        if (std.mem.eql(u8, req.op, "run")) {
            try runScript(alloc, a, &out, &queue, &decls, req, opts);
        } else if (std.mem.eql(u8, req.op, "reset")) {
            decls.clear();
            try reply(a, &out, req.id, .{ .ok = true });
        } else if (std.mem.eql(u8, req.op, "close")) {
            try reply(a, &out, req.id, .{ .ok = true });
            break;
        } else {
            const m = try std.fmt.allocPrint(a, "unknown op `{s}` — run, cancel, reset or close", .{req.op});
            try reply(a, &out, req.id, .{ .ok = false, .@"error" = .{ .msg = m } });
        }
    }
    return 0;
}

fn usageErr(stderr: *std.Io.Writer, m: []const u8) !u8 {
    try stderr.print("error: {s}\n", .{m});
    return 2;
}

/// Reads request lines until EOF. `cancel` acts here, at once; every other
/// request queues for the main loop in arrival order.
fn readRequests(alloc: std.mem.Allocator, q: *Queue) void {
    defer q.close();
    var buf: [64 * 1024]u8 = undefined;
    var fr = std.fs.File.stdin().readerStreaming(&buf);
    const r = &fr.interface;
    // A script can be larger than the read buffer; gather the whole line.
    var acc = std.Io.Writer.Allocating.init(alloc);
    defer acc.deinit();
    while (true) {
        acc.clearRetainingCapacity();
        _ = r.streamDelimiterEnding(&acc.writer, '\n') catch return;
        const t = std.mem.trim(u8, acc.written(), " \t\r\n");
        if (t.len > 0) {
            if (isCancel(alloc, t)) |id| {
                q.cancel(id);
                if (id) |s| alloc.free(s);
            } else {
                const line = alloc.dupe(u8, t) catch return;
                q.push(line) catch {
                    alloc.free(line);
                    return;
                };
            }
        }
        // stopped either at the delimiter, which is stepped over, or at EOF
        _ = r.takeByte() catch return;
    }
}

/// Parses `{"op":"cancel",...}`: null when the line is anything else, else the
/// optional id it names (owned by `alloc`).
fn isCancel(alloc: std.mem.Allocator, line: []const u8) ??[]u8 {
    if (std.mem.indexOf(u8, line, "cancel") == null) return null;
    const C = struct { op: []const u8, id: ?[]const u8 = null };
    const p = std.json.parseFromSlice(C, alloc, line, .{ .ignore_unknown_fields = true }) catch return null;
    defer p.deinit();
    if (!std.mem.eql(u8, p.value.op, "cancel")) return null;
    const id = p.value.id orelse return @as(?[]u8, null);
    return alloc.dupe(u8, id) catch @as(?[]u8, null);
}

const ErrorInfo = struct {
    msg: []const u8,
    /// Where it is: `script` for the script as sent, a path for an included file,
    /// `session` for a declaration an earlier script made.
    file: ?[]const u8 = null,
    line: ?u32 = null,
    col: ?u32 = null,
    transient: bool = false,
};

const Declared = struct { kind: []const u8, name: []const u8 };

const Status = struct {
    ok: bool,
    cancelled: bool = false,
    elapsed_ms: ?u64 = null,
    declared: ?[]const Declared = null,
    @"error": ?ErrorInfo = null,
};

fn reply(a: std.mem.Allocator, out: *Out, id: ?[]const u8, st: Status) !void {
    var aw = std.Io.Writer.Allocating.init(a);
    const w = &aw.writer;
    try w.writeAll("{\"type\":\"status\",\"id\":");
    if (id) |s| try std.json.Stringify.encodeJsonString(clip(s), .{}, w) else try w.writeAll("null");
    try w.print(",\"ok\":{},\"cancelled\":{}", .{ st.ok, st.cancelled });
    if (st.elapsed_ms) |ms| try w.print(",\"elapsed_ms\":{d}", .{ms});
    if (st.declared) |d| {
        try w.writeAll(",\"declared\":");
        try std.json.Stringify.value(d, .{}, w);
    }
    if (st.@"error") |e| {
        try w.writeAll(",\"error\":");
        try std.json.Stringify.value(e, .{ .emit_null_optional_fields = false }, w);
    }
    try w.writeAll("}\n");
    out.line(aw.written());
}

/// Maps a position in prelude + script back to the script as the frontend sent
/// it. A position inside the prelude belongs to an earlier script's declaration.
fn locate(e: *ErrorInfo, prepared_text: []const u8, entry_at: usize, label: []const u8, pos: ?ast.Pos) void {
    const p = pos orelse return;
    if (p.line == 0) return;
    if (label.len > 0 and !std.mem.eql(u8, label, "<repl>")) {
        e.file = label;
        e.line = p.line;
        e.col = p.col;
        return;
    }
    const prelude_lines: u32 = @intCast(std.mem.count(u8, prepared_text[0..@min(entry_at, prepared_text.len)], "\n"));
    if (p.line <= prelude_lines) {
        e.file = "session";
        return;
    }
    e.file = "script";
    e.line = p.line - prelude_lines;
    e.col = p.col;
}

fn paramArgs(a: std.mem.Allocator, v: ?std.json.Value) ![]runtime.ParamArg {
    const obj = switch (v orelse return &.{}) {
        .object => |o| o,
        else => return error.BadParams,
    };
    var out = std.array_list.Managed(runtime.ParamArg).init(a);
    var it = obj.iterator();
    while (it.next()) |kv| {
        // A PARAM parses its value from text, as it does from `-p k=v`.
        const val: []const u8 = switch (kv.value_ptr.*) {
            .string => |s| s,
            .integer => |n| try std.fmt.allocPrint(a, "{d}", .{n}),
            .float => |f| try std.fmt.allocPrint(a, "{d}", .{f}),
            .number_string => |s| s,
            .bool => |b| if (b) "true" else "false",
            .null => "",
            else => return error.BadParams,
        };
        try out.append(.{ .key = kv.key_ptr.*, .val = val });
    }
    return out.items;
}

/// Copies the capture pipe into `data` frames until the write end closes.
fn pump(out: *Out, fd: std.posix.fd_t, id: []const u8) void {
    var buf: [64 * 1024]u8 = undefined;
    while (true) {
        const n = std.posix.read(fd, &buf) catch return;
        if (n == 0) return;
        out.data(id, buf[0..n]);
    }
}

fn runScript(
    gpa: std.mem.Allocator,
    a: std.mem.Allocator,
    out: *Out,
    q: *Queue,
    decls: *cli.DeclStore,
    req: Request,
    opts: Opts,
) !void {
    const id = req.id orelse "";
    const script = req.script orelse
        return reply(a, out, req.id, .{ .ok = false, .@"error" = .{ .msg = "`run` needs a `script`" } });
    const params = paramArgs(a, req.params) catch
        return reply(a, out, req.id, .{ .ok = false, .@"error" = .{ .msg = "`params` must be an object of scalar values" } });
    var format = opts.format;
    if (req.format) |f| format = std.meta.stringToEnum(runtime.StdoutFormat, f) orelse
        return reply(a, out, req.id, .{ .ok = false, .@"error" = .{ .msg = "`format` must be table|json|csv|tsv|arrow" } });

    const t0 = std.time.milliTimestamp();
    var pdiag: include.Diag = .{};
    var text: []const u8 = script;
    const entry = cli.prepareEntry(a, decls, script, &pdiag, &text) catch |e| switch (e) {
        error.EndpointInSession => return reply(a, out, req.id, .{ .ok = false, .@"error" = .{
            .msg = "CREATE ENDPOINT can't run in a session — put it in a script and use `basalt serve <dir>`",
        } }),
        error.ParseFailed => {
            var ei = ErrorInfo{ .msg = pdiag.parse.msg };
            locate(&ei, text, text.len - script.len, pdiag.label, .{ .line = pdiag.parse.line, .col = pdiag.parse.col });
            return reply(a, out, req.id, .{ .ok = false, .@"error" = ei });
        },
        error.OutOfMemory => return error.OutOfMemory,
    };

    for (entry.pending) |p| try decls.put(p.id, p.text);
    const declared = try a.alloc(Declared, entry.pending.len);
    for (declared, entry.pending) |*d, p| d.* = .{ .kind = @tagName(p.id.kind), .name = p.id.name };

    if (entry.executable == 0)
        return reply(a, out, req.id, .{ .ok = true, .elapsed_ms = @intCast(std.time.milliTimestamp() - t0), .declared = declared });

    // Capture fd 1 for the script's lifetime: every sink that writes stdout
    // lands in the pipe, and the pump turns it into frames as it arrives.
    const pipe = try std.posix.pipe2(.{ .CLOEXEC = true });
    try std.posix.dup2(pipe[1], std.posix.STDOUT_FILENO);
    std.posix.close(pipe[1]);
    const pumper = std.Thread.spawn(.{}, pump, .{ out, pipe[0], id }) catch |e| {
        std.posix.dup2(std.posix.STDERR_FILENO, std.posix.STDOUT_FILENO) catch {};
        std.posix.close(pipe[0]);
        return e;
    };

    runtime.resetAbort();
    q.setRunning(id);
    var rdiag: runtime.Diag = .{};
    var failed: ?anyerror = null;
    if (entry.prog.explain == .plan) {
        // An EXPLAIN that opens the program renders without executing, as `basalt
        // run` does; it only happens with no declarations ahead of it.
        var adiag: analyze.Diag = .{};
        if (analyze.analyzeWith(a, entry.prog, &.{}, &adiag)) |plan| {
            var ebuf: [4096]u8 = undefined;
            var ef = std.fs.File.stdout().writerStreaming(&ebuf);
            analyze.render(plan, &ef.interface) catch {};
            ef.interface.flush() catch {};
        } else |e| {
            failed = e;
            rdiag.msg = adiag.msg;
            rdiag.pos = adiag.pos;
        }
    } else {
        _ = runtime.run(gpa, entry.prog, .{
            .params = params,
            .threads = opts.threads,
            .log = opts.log,
            .explain = entry.prog.explain == .analyze,
            .stdout_format = format,
            .items = false,
        }, &rdiag) catch |e| {
            failed = e;
        };
    }
    q.setRunning(null);

    // Hand fd 1 back to stderr, which closes the pipe's last write end: the pump
    // drains what the script wrote and stops, and only then does the status go.
    std.posix.dup2(std.posix.STDERR_FILENO, std.posix.STDOUT_FILENO) catch {};
    pumper.join();
    std.posix.close(pipe[0]);

    const elapsed: u64 = @intCast(std.time.milliTimestamp() - t0);
    const e = failed orelse return reply(a, out, req.id, .{ .ok = true, .elapsed_ms = elapsed, .declared = declared });
    if (e == error.OutOfMemory) return error.OutOfMemory;
    if (e == error.Aborted) {
        runtime.resetAbort();
        return reply(a, out, req.id, .{ .ok = false, .cancelled = true, .elapsed_ms = elapsed, .declared = declared });
    }
    var ei = ErrorInfo{
        .msg = if (rdiag.msg.len > 0) try a.dupe(u8, rdiag.msg) else runtime.errLabel(e),
        .transient = rdiag.retryable or runtime.isTransient(e),
    };
    locate(&ei, entry.text, entry.entry_at, "", rdiag.pos);
    return reply(a, out, req.id, .{ .ok = false, .elapsed_ms = elapsed, .declared = declared, .@"error" = ei });
}

test "locate: positions count in the script as sent, not the replayed prelude" {
    const text = "CREATE FUNCTION f(x) AS x;\nPARAM n INT DEFAULT 1;\nSELECT 1;\nSELECT nope;";
    const entry_at = std.mem.indexOf(u8, text, "SELECT 1").?;

    var e = ErrorInfo{ .msg = "" };
    locate(&e, text, entry_at, "", .{ .line = 4, .col = 8 });
    try std.testing.expectEqualStrings("script", e.file.?);
    try std.testing.expectEqual(@as(?u32, 2), e.line);
    try std.testing.expectEqual(@as(?u32, 8), e.col);

    // inside the prelude: an earlier script's declaration, with no line to give
    var p = ErrorInfo{ .msg = "" };
    locate(&p, text, entry_at, "", .{ .line = 1, .col = 1 });
    try std.testing.expectEqualStrings("session", p.file.?);
    try std.testing.expectEqual(@as(?u32, null), p.line);

    // an included file keeps its own path and lines
    var inc = ErrorInfo{ .msg = "" };
    locate(&inc, text, entry_at, "lib.sql", .{ .line = 7, .col = 3 });
    try std.testing.expectEqualStrings("lib.sql", inc.file.?);
    try std.testing.expectEqual(@as(?u32, 7), inc.line);

    // no position, nothing claimed
    var none = ErrorInfo{ .msg = "" };
    locate(&none, text, entry_at, "", null);
    try std.testing.expectEqual(@as(?[]const u8, null), none.file);
}

test "paramArgs: JSON scalars bind as the text a -p would carry" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const v = try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"s\":\"x\",\"n\":21,\"f\":1.5,\"b\":true}", .{});
    const got = try paramArgs(a, v);
    try std.testing.expectEqual(@as(usize, 4), got.len);
    try std.testing.expectEqualStrings("x", got[0].val);
    try std.testing.expectEqualStrings("21", got[1].val);
    try std.testing.expectEqualStrings("1.5", got[2].val);
    try std.testing.expectEqualStrings("true", got[3].val);
    try std.testing.expectEqual(@as(usize, 0), (try paramArgs(a, null)).len);

    const arr = try std.json.parseFromSliceLeaky(std.json.Value, a, "[1]", .{});
    try std.testing.expectError(error.BadParams, paramArgs(a, arr));
    const nested = try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"o\":{}}", .{});
    try std.testing.expectError(error.BadParams, paramArgs(a, nested));
}

test "isCancel: only a cancel op is one, with or without an id" {
    const a = std.testing.allocator;
    try std.testing.expect(isCancel(a, "{\"op\":\"run\",\"script\":\"SELECT 'cancel';\"}") == null);
    const bare = isCancel(a, "{\"op\":\"cancel\"}").?;
    try std.testing.expect(bare == null);
    const named = isCancel(a, "{\"op\":\"cancel\",\"id\":\"c7\"}").?.?;
    defer a.free(named);
    try std.testing.expectEqualStrings("c7", named);
}
