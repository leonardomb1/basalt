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
//!   {"op":"run","id":"c1","script":"SELECT 1;","params":{"k":"v"},"format":"arrow","max_rows":5000}
//!   {"op":"complete","id":"c2","script":"SELECT erp.","pos":11}
//!                               what Tab offers there: the status carries
//!                               `complete: {start, end, items: [{text, kind}]}`
//!   {"op":"cancel"}             stop the running script; the session survives
//!   {"op":"reset"}              forget every declaration
//!   {"op":"close"}              exit (as does EOF on stdin)
//!
//! `id` is echoed on every frame the request produces; `params` binds PARAMs for
//! that script only; `format` and `max_rows` override `--format` and
//! `--max-rows` for that script. SIGINT also cancels the running script rather
//! than killing the process.
//!
//! Replies are frames on stdout. Each frame is a JSON header line, and a `data`
//! header is followed by exactly `len` raw bytes:
//!
//!   {"type":"data","id":"c1","len":1234}\n<1234 bytes>
//!   {"type":"result","id":"c1","statement":0,"kind":"select","line":1,"col":1,"rows":3,...}\n
//!   {"type":"status","id":"c1","ok":true,"elapsed_ms":8,"declared":[...],"results":[...]}\n
//!
//! `data` carries whatever the script wrote to stdout — the Arrow IPC streams,
//! NDJSON or CSV of its results, in order, possibly split over several frames.
//! A `result` frame follows each result's last byte, so the data between two of
//! them is one result. A statement that runs past 400 ms sends a `progress`
//! frame a second (`target`, `rows`, `rows_per_sec`, `elapsed_ms`, and
//! `loop_done`/`loop_total` inside a `FOR EACH`). Each `LOAD` sends a `load`
//! frame as it finishes, written or failed; the status lists them again with
//! the run's totals. Exactly one `status` ends each request; after it, the
//! script wrote nothing more. A failed script's status carries `error` with the message, the line and
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
    max_rows: ?u64 = null,
    /// `complete`: the byte offset in `script` to complete at (default: its end),
    /// and whether connections may be asked for their tables and columns.
    pos: ?usize = null,
    connect: bool = true,
    /// `pos`, and the `start`/`end` of the answer, count UTF-16 units — what a
    /// JavaScript editor's offsets are — rather than bytes.
    utf16: bool = false,
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
    max_rows: ?u64 = null,
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
        } else if (std.mem.eql(u8, a, "--max-rows")) {
            i += 1;
            if (i >= args.len) return usageErr(stderr, "missing value after `--max-rows`");
            opts.max_rows = std.fmt.parseInt(u64, args[i], 10) catch return usageErr(stderr, "invalid --max-rows");
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
    // What sources said about their tables and columns, kept for the session as
    // the REPL keeps it: asked once, on first use.
    var catalog = cli.Catalog.init(alloc);
    defer catalog.deinit();

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
        } else if (std.mem.eql(u8, req.op, "complete")) {
            try completeScript(alloc, a, &out, &decls, &catalog, req);
        } else if (std.mem.eql(u8, req.op, "reset")) {
            decls.clear();
            catalog.deinit();
            catalog = cli.Catalog.init(alloc);
            try reply(a, &out, req.id, .{ .ok = true });
        } else if (std.mem.eql(u8, req.op, "close")) {
            try reply(a, &out, req.id, .{ .ok = true });
            break;
        } else {
            const m = try std.fmt.allocPrint(a, "unknown op `{s}` — run, complete, cancel, reset or close", .{req.op});
            try reply(a, &out, req.id, .{ .ok = false, .@"error" = .{ .msg = m } });
        }
    }
    return 0;
}

/// `{"op":"complete","script":…,"pos":N}`: what Tab would offer at byte `pos`
/// of a cell, against the session's declarations and the cell's own. Answered
/// by the status, which carries `complete: {start, end, items}`.
fn completeScript(gpa: std.mem.Allocator, a: std.mem.Allocator, out: *Out, decls: *const cli.DeclStore, catalog: *cli.Catalog, req: Request) !void {
    const script = req.script orelse "";
    const at = if (req.pos) |p| (if (req.utf16) cli.utf16ToByte(script, p) else p) else script.len;
    if (at > script.len)
        return reply(a, out, req.id, .{ .ok = false, .@"error" = .{ .msg = "`pos` is past the end of `script`" } });
    var scope = cli.DeclStore.init(gpa);
    defer scope.deinit();
    for (decls.items.items) |e| try scope.put(.{ .kind = e.kind, .name = e.name }, e.text);
    try cli.declareFrom(&scope, a, script);
    const cx = cli.Completer{ .gpa = gpa, .decls = &scope, .catalog = catalog, .connect = req.connect };
    const offer = try cli.suggestFor(a, &cx, script, at);
    var aw = std.Io.Writer.Allocating.init(a);
    try cli.writeOfferIn(&aw.writer, offer, at, script, req.utf16);
    try reply(a, out, req.id, .{ .ok = true, .complete = aw.written() });
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
    /// Just past the offending text, when the error names a span.
    end_line: ?u32 = null,
    end_col: ?u32 = null,
    transient: bool = false,
};

const Declared = struct { kind: []const u8, name: []const u8 };

const Status = struct {
    ok: bool,
    cancelled: bool = false,
    elapsed_ms: ?u64 = null,
    declared: ?[]const Declared = null,
    results: ?[]const ResultFrame = null,
    /// The loads the script ran and the run's totals; null when nothing ran.
    loads: ?Loads = null,
    @"error": ?ErrorInfo = null,
    /// A `complete` answer, already JSON.
    complete: ?[]const u8 = null,
};

fn anyTruncated(results: ?[]const ResultFrame) bool {
    for (results orelse return false) |r| if (r.truncated) return true;
    return false;
}

fn reply(a: std.mem.Allocator, out: *Out, id: ?[]const u8, st: Status) !void {
    var aw = std.Io.Writer.Allocating.init(a);
    const w = &aw.writer;
    try w.writeAll("{\"type\":\"status\",\"id\":");
    if (id) |s| try std.json.Stringify.encodeJsonString(clip(s), .{}, w) else try w.writeAll("null");
    try w.print(",\"ok\":{},\"cancelled\":{},\"truncated\":{}", .{ st.ok, st.cancelled, anyTruncated(st.results) });
    if (st.elapsed_ms) |ms| try w.print(",\"elapsed_ms\":{d}", .{ms});
    if (st.declared) |d| {
        try w.writeAll(",\"declared\":");
        try std.json.Stringify.value(d, .{}, w);
    }
    if (st.results) |r| {
        try w.writeAll(",\"results\":");
        try std.json.Stringify.value(r, .{}, w);
    }
    if (st.loads) |l| {
        try w.writeAll(",\"loads\":[");
        for (l.list, 0..) |ld, k| {
            try w.writeAll(if (k > 0) ",{" else "{");
            try writeLoadFields(w, ld);
            try w.writeByte('}');
        }
        const s = l.summary;
        try w.print("],\"loads_ok\":{d},\"loads_failed\":{d},\"rows_read\":{d},\"rows_loaded\":{d},\"lanes\":{d}", .{ s.loads, s.loads_failed, s.rows_read, s.rows_loaded orelse 0, s.threads });
    }
    if (st.complete) |c| {
        try w.writeAll(",\"complete\":");
        try w.writeAll(c);
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
fn locate(e: *ErrorInfo, prepared_text: []const u8, entry_at: usize, label: []const u8, pos: ?ast.Pos, end: ?ast.Pos) void {
    const p = pos orelse return;
    if (p.line == 0) return;
    var base: u32 = 0;
    if (label.len > 0 and !std.mem.eql(u8, label, "<repl>")) {
        e.file = label;
    } else {
        base = @intCast(std.mem.count(u8, prepared_text[0..@min(entry_at, prepared_text.len)], "\n"));
        if (p.line <= base) {
            e.file = "session";
            return;
        }
        e.file = "script";
    }
    e.line = p.line - base;
    e.col = p.col;
    if (end) |x| if (x.line > base) {
        e.end_line = x.line - base;
        e.end_col = x.col;
    };
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

fn declaresLet(pending: []const cli.Pending) bool {
    for (pending) |p| if (p.id.kind == .let) return true;
    return false;
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

/// What the status says of a run's loads: each one, and the summary's totals.
const Loads = struct { list: []const runtime.LoadDone, summary: obs.Summary };

/// One load's fields, without braces: the `load` frame carries them after its
/// `type`/`id`, and the status's `loads` lists each as an object. Fields a load has no value for are left
/// out: `reason`/`transient` on success, the loop's when not in one.
fn writeLoadFields(w: *std.Io.Writer, l: runtime.LoadDone) !void {
    try w.print("\"load\":{d},\"target\":", .{l.ordinal});
    try std.json.Stringify.encodeJsonString(l.target, .{}, w);
    try w.print(",\"line\":{d},\"col\":{d}", .{ l.line, l.col });
    try w.print(",\"rows_read\":{d},\"rows_written\":{d},\"elapsed_ms\":{d},\"lanes\":{d},\"ok\":{}", .{ l.rows_read, l.rows_written, l.elapsed_ms, l.lanes, l.ok });
    if (!l.ok) {
        try w.writeAll(",\"reason\":");
        try std.json.Stringify.encodeJsonString(l.reason, .{}, w);
        try w.print(",\"transient\":{}", .{l.transient});
    }
    if (l.loop_rows > 0) try w.print(",\"loop_row\":{d},\"loop_rows\":{d}", .{ l.loop_row, l.loop_rows });
    if (l.loop_total > 0) try w.print(",\"loop_done\":{d},\"loop_total\":{d}", .{ l.loop_done, l.loop_total });
}

/// A finished result, as the `result` frame and the status list it.
const ResultFrame = struct {
    statement: u32,
    kind: []const u8,
    line: u32,
    col: u32,
    rows: u64,
    elapsed_ms: u64,
    truncated: bool,
};

/// fd 1 routed into a pipe whose pump frames it. A result's close swaps in a
/// fresh pipe — the old one then holds exactly that result's bytes — so the
/// `result` frame can follow them: everything between two `result` frames is
/// one result, in any format.
const Capture = struct {
    out: *Out,
    id: []const u8,
    read_fd: std.posix.fd_t = -1,
    thread: ?std.Thread = null,
    results: std.array_list.Managed(ResultFrame),
    /// A failed swap: later bytes still frame, just without a boundary.
    broken: bool = false,
    /// Finished loads, copied out of the run's arena. Appended from the worker
    /// threads of a parallel `FOR EACH`, hence the lock (the arena is not
    /// thread-safe either).
    loads: std.array_list.Managed(runtime.LoadDone),
    loads_mu: std.Thread.Mutex = .{},

    fn start(self: *Capture) !void {
        const pipe = try std.posix.pipe2(.{ .CLOEXEC = true });
        errdefer std.posix.close(pipe[0]);
        // replacing fd 1 closes the previous pipe's last write end
        std.posix.dup2(pipe[1], std.posix.STDOUT_FILENO) catch |e| {
            std.posix.close(pipe[1]);
            return e;
        };
        std.posix.close(pipe[1]);
        self.thread = try std.Thread.spawn(.{}, pump, .{ self.out, pipe[0], self.id });
        self.read_fd = pipe[0];
    }

    /// Wait for the pump of a pipe whose write end is gone.
    fn drain(self: *Capture, t: ?std.Thread, fd: std.posix.fd_t) void {
        _ = self;
        if (t) |th| th.join();
        if (fd >= 0) std.posix.close(fd);
    }

    /// Hand fd 1 back to stderr and drain what the script wrote.
    fn stop(self: *Capture) void {
        std.posix.dup2(std.posix.STDERR_FILENO, std.posix.STDOUT_FILENO) catch {};
        self.drain(self.thread, self.read_fd);
        self.thread = null;
        self.read_fd = -1;
    }

    fn onResult(ctx: *anyopaque, done: runtime.ResultDone) void {
        const self: *Capture = @ptrCast(@alignCast(ctx));
        const r = ResultFrame{
            .statement = done.info.statement,
            .kind = done.info.kind,
            .line = done.info.line,
            .col = done.info.col,
            .rows = done.rows,
            .elapsed_ms = @intCast(@max(0, std.time.milliTimestamp() - done.info.t0_ms)),
            .truncated = done.truncated,
        };
        self.results.append(r) catch {};
        if (self.broken) return;
        const old_t = self.thread;
        const old_fd = self.read_fd;
        self.start() catch {
            self.broken = true;
            return;
        };
        self.drain(old_t, old_fd);
        var hbuf: [640]u8 = undefined;
        var w = std.Io.Writer.fixed(&hbuf);
        w.writeAll("{\"type\":\"result\",\"id\":") catch return;
        std.json.Stringify.encodeJsonString(clip(self.id), .{}, &w) catch return;
        w.print(",\"statement\":{d},\"kind\":\"{s}\",\"line\":{d},\"col\":{d},\"rows\":{d},\"elapsed_ms\":{d},\"truncated\":{}}}\n", .{ r.statement, r.kind, r.line, r.col, r.rows, r.elapsed_ms, r.truncated }) catch return;
        self.out.line(w.buffered());
    }

    fn hook(self: *Capture) runtime.ResultHook {
        return .{ .ctx = self, .f = onResult };
    }

    /// A `LOAD` finished: its `load` frame now, and a copy for the status.
    fn onLoad(ctx: *anyopaque, done: runtime.LoadDone) void {
        const self: *Capture = @ptrCast(@alignCast(ctx));
        self.loads_mu.lock();
        defer self.loads_mu.unlock();
        const a = self.loads.allocator;
        var l = done;
        l.target = a.dupe(u8, done.target) catch "";
        l.reason = a.dupe(u8, done.reason) catch "";
        self.loads.append(l) catch {};
        var aw = std.Io.Writer.Allocating.init(a);
        const w = &aw.writer;
        w.writeAll("{\"type\":\"load\",\"id\":") catch return;
        std.json.Stringify.encodeJsonString(clip(self.id), .{}, w) catch return;
        w.writeByte(',') catch return;
        writeLoadFields(w, l) catch return;
        w.writeAll("}\n") catch return;
        self.out.line(aw.written());
    }

    /// A statement still moving rows: what the terminal's progress line says,
    /// as a `progress` frame, once a second once it has run 400 ms.
    fn onProgress(ctx: *anyopaque, ev: obs.Progress.Event) void {
        const self: *Capture = @ptrCast(@alignCast(ctx));
        var hbuf: [768]u8 = undefined;
        var w = std.Io.Writer.fixed(&hbuf);
        w.writeAll("{\"type\":\"progress\",\"id\":") catch return;
        std.json.Stringify.encodeJsonString(clip(self.id), .{}, &w) catch return;
        w.writeAll(",\"target\":") catch return;
        std.json.Stringify.encodeJsonString(ev.target, .{}, &w) catch return;
        w.print(",\"rows\":{d},\"rows_per_sec\":{d},\"elapsed_ms\":{d}", .{ ev.rows, ev.rows_per_sec, ev.elapsed_ms }) catch return;
        if (ev.loop_total > 0) w.print(",\"loop_done\":{d},\"loop_total\":{d}", .{ ev.loop_done, ev.loop_total }) catch return;
        w.writeAll("}\n") catch return;
        self.out.line(w.buffered());
    }
};

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
            const pe = pdiag.parse;
            locate(&ei, text, text.len - script.len, pdiag.label, .{ .line = pe.line, .col = pe.col }, if (pe.end_line > 0) ast.Pos{ .line = pe.end_line, .col = pe.end_col } else null);
            return reply(a, out, req.id, .{ .ok = false, .@"error" = ei });
        },
        error.OutOfMemory => return error.OutOfMemory,
    };

    for (entry.pending) |p| try decls.put(p.id, p.text);
    const declared = try a.alloc(Declared, entry.pending.len);
    for (declared, entry.pending) |*d, p| d.* = .{ .kind = @tagName(p.id.kind), .name = p.id.name };

    if (entry.executable == 0) {
        // Declarations only — but a LET's value is decided here, in the cell
        // that declares it, not by whichever later cell first runs.
        if (declaresLet(entry.pending)) {
            var freezer = cli.LetFreezer.init(a);
            var rdiag: runtime.Diag = .{};
            _ = runtime.run(gpa, entry.prog, .{
                .params = params,
                .log = opts.log,
                .declarations_only = true,
                .on_let = freezer.hook(),
            }, &rdiag) catch |e| {
                if (e == error.OutOfMemory) return error.OutOfMemory;
                var ei = ErrorInfo{
                    .msg = if (rdiag.msg.len > 0) try a.dupe(u8, rdiag.msg) else runtime.errLabel(e),
                    .transient = rdiag.retryable or runtime.isTransient(e),
                };
                locate(&ei, entry.text, entry.entry_at, "", rdiag.pos, rdiag.end);
                return reply(a, out, req.id, .{ .ok = false, .elapsed_ms = @intCast(std.time.milliTimestamp() - t0), .declared = declared, .@"error" = ei });
            };
            try freezer.commit(decls);
        }
        return reply(a, out, req.id, .{ .ok = true, .elapsed_ms = @intCast(std.time.milliTimestamp() - t0), .declared = declared });
    }

    // Capture fd 1 for the script's lifetime: every sink that writes stdout
    // lands in the pipe, and the pump turns it into frames as it arrives.
    var cap = Capture{ .out = out, .id = id, .results = std.array_list.Managed(ResultFrame).init(a), .loads = std.array_list.Managed(runtime.LoadDone).init(a) };
    var summary: obs.Summary = .{ .run_id = 0, .loads = 0 };
    var ran = false;
    cap.start() catch |e| {
        std.posix.dup2(std.posix.STDERR_FILENO, std.posix.STDOUT_FILENO) catch {};
        return e;
    };

    // LET values this script decides replace their expressions in the session,
    // whether or not the rest of it succeeds: they were decided either way
    var freezer = cli.LetFreezer.init(a);
    defer freezer.commit(decls) catch {};

    runtime.resetAbort();
    q.setRunning(id);
    var rdiag: runtime.Diag = .{};
    var failed: ?anyerror = null;
    if (entry.prog.explain == .plan) {
        // An EXPLAIN that opens the program renders without executing, as `basalt
        // run` does; it only happens with no declarations ahead of it.
        var adiag: analyze.Diag = .{};
        if (analyze.analyzeWith(a, entry.prog, &.{}, &adiag)) |plan| {
            var aw = std.Io.Writer.Allocating.init(a);
            try analyze.render(plan, &aw.writer);
            if (format == .arrow) {
                const info = runtime.ResultInfo{ .kind = "explain", .line = 1, .col = 1, .t0_ms = t0 };
                if (runtime.printPlanArrow(gpa, aw.written(), info)) |n| {
                    Capture.onResult(&cap, .{ .info = info, .rows = n });
                } else |e| failed = e;
            } else std.fs.File.stdout().writeAll(aw.written()) catch {};
        } else |e| {
            failed = e;
            rdiag.msg = adiag.msg;
            rdiag.pos = adiag.pos;
            rdiag.end = adiag.end;
        }
    } else {
        _ = runtime.run(gpa, entry.prog, .{
            .params = params,
            .threads = opts.threads,
            .log = opts.log,
            .explain = entry.prog.explain == .analyze,
            .stdout_format = format,
            .items = false,
            // result metadata counts lines in the script as sent
            .line_base = @intCast(std.mem.count(u8, entry.text[0..entry.entry_at], "\n")),
            .on_result = cap.hook(),
            .progress_hook = .{ .ctx = &cap, .f = Capture.onProgress },
            .max_rows = req.max_rows orelse opts.max_rows,
            .on_let = freezer.hook(),
            .on_load = .{ .ctx = &cap, .f = Capture.onLoad },
            .summary_out = &summary,
        }, &rdiag) catch |e| {
            failed = e;
        };
        ran = true;
    }
    q.setRunning(null);

    // Hand fd 1 back to stderr, which closes the pipe's last write end: the pump
    // drains what the script wrote and stops, and only then does the status go.
    cap.stop();
    const results = cap.results.items;
    const loads: ?Loads = if (ran) .{ .list = cap.loads.items, .summary = summary } else null;

    const elapsed: u64 = @intCast(std.time.milliTimestamp() - t0);
    const e = failed orelse return reply(a, out, req.id, .{ .ok = true, .elapsed_ms = elapsed, .declared = declared, .results = results, .loads = loads });
    if (e == error.OutOfMemory) return error.OutOfMemory;
    if (e == error.Aborted) {
        runtime.resetAbort();
        return reply(a, out, req.id, .{ .ok = false, .cancelled = true, .elapsed_ms = elapsed, .declared = declared, .results = results, .loads = loads });
    }
    var ei = ErrorInfo{
        .msg = if (rdiag.msg.len > 0) try a.dupe(u8, rdiag.msg) else runtime.errLabel(e),
        .transient = rdiag.retryable or runtime.isTransient(e),
    };
    locate(&ei, entry.text, entry.entry_at, "", rdiag.pos, rdiag.end);
    return reply(a, out, req.id, .{ .ok = false, .elapsed_ms = elapsed, .declared = declared, .results = results, .loads = loads, .@"error" = ei });
}

test "locate: positions count in the script as sent, not the replayed prelude" {
    const text = "CREATE FUNCTION f(x) AS x;\nPARAM n INT DEFAULT 1;\nSELECT 1;\nSELECT nope;";
    const entry_at = std.mem.indexOf(u8, text, "SELECT 1").?;

    var e = ErrorInfo{ .msg = "" };
    locate(&e, text, entry_at, "", .{ .line = 4, .col = 8 }, .{ .line = 4, .col = 12 });
    try std.testing.expectEqualStrings("script", e.file.?);
    try std.testing.expectEqual(@as(?u32, 2), e.line);
    try std.testing.expectEqual(@as(?u32, 8), e.col);
    try std.testing.expectEqual(@as(?u32, 2), e.end_line);
    try std.testing.expectEqual(@as(?u32, 12), e.end_col);

    // inside the prelude: an earlier script's declaration, with no line to give
    var p = ErrorInfo{ .msg = "" };
    locate(&p, text, entry_at, "", .{ .line = 1, .col = 1 }, null);
    try std.testing.expectEqualStrings("session", p.file.?);
    try std.testing.expectEqual(@as(?u32, null), p.line);

    // an included file keeps its own path and lines
    var inc = ErrorInfo{ .msg = "" };
    locate(&inc, text, entry_at, "lib.sql", .{ .line = 7, .col = 3 }, null);
    try std.testing.expectEqualStrings("lib.sql", inc.file.?);
    try std.testing.expectEqual(@as(?u32, 7), inc.line);

    // no position, nothing claimed
    var none = ErrorInfo{ .msg = "" };
    locate(&none, text, entry_at, "", null, null);
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
