//! HTTP trigger host for `@http` scripts. Each matching request runs the pipeline
//! once: query-string values bind params, the request body feeds `read request`,
//! and the response is a JSON run summary. A thread per connection (64 at most;
//! past that a connection gets 503 + Retry-After) keeps the health probes
//! answering, and request bodies are read before the run lock, but pipeline runs
//! go one at a time; scale out with replicas behind a load balancer. A body read
//! idle for 30s gets 408.
//!
//! Two entry points: `serve` hosts a single program; `serveDir` hosts every `@http`
//! script in a directory, routing by each script's declared `@http(path=…)` and
//! reloading on SIGHUP (the control plane writes scripts, then signals). A reload
//! thread loads the new table and swaps it in between runs, so the accept loop
//! never waits on one. The listener has a 1s accept timeout so the loop re-checks
//! the shutdown flag even when idle. `g_log` is the host's one logger, shared by every route
//! and the flusher, so `--log-format json` covers loads, reloads and flush failures.
//!
//! `ACCEPT ... INTO BUFFER` endpoints validate a request against the declared
//! schema, append it to the WAL as JSONL and ack only after fsync; past the
//! `MAX n MB|GB` limit (1 GiB default) they answer 503 with Retry-After, so the
//! client is the queue. A flusher thread drains completed segments through the
//! pipeline, one run per segment, labelled after it (prefix = segment stem,
//! run_id = seq), so a replay produces identical labels and the sink dedups. A
//! failed segment stops the drain, preserving order, and retries on the next tick.
//!
//! The first SIGTERM/SIGINT is a graceful stop (`runtime.shuttingDown`): accepts
//! end, requests already in hand finish, and a flusher completes and marks the
//! segment it is loading, then starts no other; the rest load on the next start.
//! Draining everything at a stop would widen the window for a SIGKILL mid-load,
//! the case that loads a segment twice into a sink without label dedup. A second
//! signal aborts (`runtime.aborting`): the load in flight stops unmarked and replays.

const std = @import("std");
const ast = @import("../lang/ast.zig");
const parser = @import("../lang/sql_parser.zig");
const include = @import("../lang/include.zig");
const runtime = @import("../runtime/run.zig");
const obs = @import("../runtime/obs.zig");
const Wal = @import("../connect/wal.zig").Wal;
const request = @import("../connect/request.zig");

var g_log: obs.Logger = undefined;
var g_log_cfg: runtime.LogConfig = .{};

fn log(level: obs.Level, comptime fmt: []const u8, args: anytype) void {
    g_log.log(level, fmt, args);
}

fn initLog(cfg: runtime.LogConfig) void {
    g_log_cfg = cfg;
    g_log = obs.Logger.init(0, cfg.format, if (cfg.quiet) .err else cfg.level);
    g_log.quiet = cfg.quiet;
}

pub const Route = struct {
    path: []const u8,
    program: ast.Program,
    label: []const u8,
    doc: []const u8 = "",
    buf: ?*BufState = null,
};

pub const BufState = struct {
    wal: Wal,
    decl: ast.BufferDecl,
    program: ast.Program,
    flush_secs: u64 = 5,
    flush_rows: u64 = 50_000,
    max_bytes: u64 = 1 << 30,
    rows_since: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    drain_on_stop: bool = true,
    thread: ?std.Thread = null,

    /// With `drain`, the flusher first loads every pending segment and the open
    /// one, as when a reload drops the buffer; without, it stops after its
    /// in-flight segment and leaves the open segment open, so a reload can hand
    /// this buffer on as it is. Once the process is shutting down no new segment
    /// load starts either way: pending ones wait in the WAL for the next start.
    pub fn stopFlusher(self: *BufState, drain: bool) void {
        self.drain_on_stop = drain;
        self.stop.store(true, .seq_cst);
        if (self.thread) |t| t.join();
        self.thread = null;
    }

    pub fn shutdown(self: *BufState) void {
        self.stopFlusher(true);
        self.wal.close();
    }

    /// Take the definition of a reloaded script; its flusher must be stopped
    /// and no accept running.
    fn adopt(self: *BufState, program: ast.Program) void {
        const decl = bufferDecl(program).?;
        self.decl = decl;
        self.program = program;
        self.max_bytes = decl.max_bytes;
        self.wal.segment_bytes = decl.segment_bytes;
        self.flush_secs = 5;
        self.flush_rows = 50_000;
        self.readHints();
    }

    fn readHints(self: *BufState) void {
        for (self.program.stmts) |s| {
            if (s != .output) continue;
            for (s.output.stages) |st| {
                if (st.node != .read or st.node.read.form != .buffer) continue;
                for (st.hints) |h| {
                    if (std.mem.eql(u8, h.key, "flush_secs") and h.value == .int)
                        self.flush_secs = @intCast(@max(1, h.value.int));
                    if (std.mem.eql(u8, h.key, "flush_rows") and h.value == .int)
                        self.flush_rows = @intCast(@max(1, h.value.int));
                }
            }
        }
    }
};

fn bufferDecl(program: ast.Program) ?ast.BufferDecl {
    if (program.stmts.len == 0 or program.stmts[0] != .kind) return null;
    return program.stmts[0].kind.buffer;
}

/// Fails with BufferLocked when the buffer is already open, here or in another process.
pub fn initBufState(gpa: std.mem.Allocator, program: ast.Program) !?*BufState {
    const decl = bufferDecl(program) orelse return null;

    const bs = try gpa.create(BufState);
    errdefer gpa.destroy(bs);
    bs.* = .{
        .wal = Wal.open(gpa, decl.dir, decl.name, decl.segment_bytes) catch |e| {
            if (e == error.BufferLocked)
                log(.err, "buffer `{s}` at {s} is already open by another basalt serve or script (one owner per WAL directory)", .{ decl.name, decl.dir });
            return e;
        },
        .decl = decl,
        .program = program,
        .max_bytes = decl.max_bytes,
    };
    bs.readHints();
    return bs;
}

/// Returns the row count. BodySchemaViolation or bad JSON maps to 422 (with
/// `msg_out` saying why), Backpressure to 503.
pub fn acceptIntoBuffer(bs: *BufState, arena: std.mem.Allocator, body: []const u8, msg_out: *[]const u8) !usize {
    if (bs.wal.bytesOnDisk() > bs.max_bytes) return error.Backpressure;

    const root = std.json.parseFromSliceLeaky(std.json.Value, arena, body, .{}) catch {
        msg_out.* = "invalid JSON in request body";
        return error.BodySchemaViolation;
    };
    const items: []const std.json.Value = switch (root) {
        .array => |arr| arr.items,
        .object => blk: {
            const one = try arena.alloc(std.json.Value, 1);
            one[0] = root;
            break :blk one;
        },
        else => {
            msg_out.* = "request body must be a JSON object or array";
            return error.BodySchemaViolation;
        },
    };
    try request.validateBody(items, bs.decl.schema, arena, msg_out);

    for (items) |item| {
        const line = try std.json.Stringify.valueAlloc(arena, item, .{});
        try bs.wal.append(line);
    }
    try bs.wal.sync();
    _ = bs.rows_since.fetchAdd(items.len, .seq_cst);
    return items.len;
}

pub fn drainPending(gpa: std.mem.Allocator, bs: *BufState) void {
    const pending = bs.wal.pendingSegments(gpa) catch |e| {
        log(.err, "buffer {s}: listing segments failed: {s}", .{ bs.decl.name, @errorName(e) });
        return;
    };
    defer gpa.free(pending);
    for (pending) |s| {
        if (runtime.shuttingDown() or (bs.stop.load(.seq_cst) and !bs.drain_on_stop)) return;
        var lbuf: [300]u8 = undefined;
        const label = bs.wal.labelFor(&lbuf, s);
        var diag: runtime.Diag = .{};
        _ = runtime.run(gpa, bs.program, .{
            .buffer_segment = s,
            .load_label_prefix = label,
            .load_run_id = s,
            .log = g_log_cfg,
        }, &diag) catch |e| {
            log(.err, "buffer {s}: flush of segment {d} failed: {s} ({s}) — will retry", .{ bs.decl.name, s, @errorName(e), diag.msg });
            return;
        };
        bs.wal.markLoaded(s) catch |e| {
            log(.err, "buffer {s}: manifest update failed after segment {d}: {s}", .{ bs.decl.name, s, @errorName(e) });
            return;
        };
        if (bs.decl.retain_hours == null) _ = bs.wal.purgeLoaded() catch 0;
    }
    if (bs.decl.retain_hours) |h| _ = bs.wal.purgeOlderThan(h) catch 0;
}

fn flusherMain(gpa: std.mem.Allocator, bs: *BufState) void {
    var last: i64 = std.time.milliTimestamp();
    while (!bs.stop.load(.seq_cst) and !runtime.shuttingDown()) {
        std.Thread.sleep(200 * std.time.ns_per_ms);
        const now = std.time.milliTimestamp();
        const due_time = now - last >= @as(i64, @intCast(bs.flush_secs * 1000));
        const due_rows = bs.rows_since.load(.seq_cst) >= bs.flush_rows;
        if (!due_time and !due_rows) continue;
        last = now;
        bs.rows_since.store(0, .seq_cst);
        bs.wal.rotateIfNonEmpty() catch |e| {
            log(.err, "buffer {s}: rotate failed: {s}", .{ bs.decl.name, @errorName(e) });
            continue;
        };
        drainPending(gpa, bs);
    }
    if (!bs.drain_on_stop or runtime.shuttingDown()) return;
    bs.wal.rotateIfNonEmpty() catch {};
    drainPending(gpa, bs);
}

pub fn startFlusher(gpa: std.mem.Allocator, bs: *BufState) !void {
    bs.stop.store(false, .seq_cst);
    bs.thread = try std.Thread.spawn(.{}, flusherMain, .{ gpa, bs });
}

fn httpPath(program: ast.Program) []const u8 {
    return httpAttr(program, "path") orelse "/";
}

fn httpDoc(program: ast.Program) []const u8 {
    return httpAttr(program, "doc") orelse "";
}

fn httpAttr(program: ast.Program, key: []const u8) ?[]const u8 {
    if (program.stmts.len == 0 or program.stmts[0] != .kind) return null;
    var v: ?[]const u8 = null;
    for (program.stmts[0].kind.config) |attr| {
        if (std.mem.eql(u8, attr.key, key)) v = attrToStr(attr.value);
    }
    return v;
}

/// Every interface unless `--host` names one, so a container's port mapping reaches it.
pub const default_host = "0.0.0.0";

const max_body_bytes = 64 * 1024 * 1024;

/// SO_REUSEADDR only (a restart may rebind past TIME_WAIT); no SO_REUSEPORT, so a
/// second server on a port already taken fails with AddressInUse instead of
/// silently sharing its traffic.
fn listen(host: []const u8, port: u16) !std.net.Server {
    const posix = std.posix;
    const address = try std.net.Address.parseIp(host, port);
    const fd = try posix.socket(address.any.family, posix.SOCK.STREAM | posix.SOCK.CLOEXEC, posix.IPPROTO.TCP);
    var net_server: std.net.Server = .{ .listen_address = undefined, .stream = .{ .handle = fd } };
    errdefer net_server.deinit();
    try posix.setsockopt(fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));
    var socklen = address.getOsSockLen();
    posix.bind(fd, &address.any, socklen) catch |e| {
        if (e == error.AddressInUse) log(.err, "{f} is already in use", .{address});
        return e;
    };
    try posix.listen(fd, 128);
    try posix.getsockname(fd, &net_server.listen_address.any, &socklen);
    const tv = posix.timeval{ .sec = 1, .usec = 0 };
    posix.setsockopt(fd, posix.SOL.SOCKET, posix.SO.RCVTIMEO, std.mem.asBytes(&tv)) catch {};
    return net_server;
}

/// Answer a request whose head (`seen`, the bytes read of it) could not be read:
/// 431 when its headers overflow the read buffer, 400 when its request line does
/// or it is not valid or complete HTTP; nothing when the client sent nothing.
fn rejectHead(out: *std.Io.Writer, err: std.http.Server.ReceiveHeadError, seen: []const u8) void {
    const status: std.http.Status, const msg: []const u8 = switch (err) {
        error.HttpHeadersOversize => if (std.mem.indexOfScalar(u8, seen, '\n') == null)
            .{ .bad_request, "request line over the 64 KiB limit" }
        else
            .{ .request_header_fields_too_large, "request head over the 64 KiB limit" },
        error.HttpHeadersInvalid, error.HttpRequestTruncated => .{ .bad_request, "malformed HTTP request" },
        else => return,
    };
    const prefix = "{\"status\":\"error\",\"retryable\":false,\"error\":\"";
    const suffix = "\"}\n";
    out.print("HTTP/1.1 {d} {s}\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: {d}\r\n\r\n{s}{s}{s}", .{
        @intFromEnum(status), status.phrase() orelse "", prefix.len + msg.len + suffix.len, prefix, msg, suffix,
    }) catch return;
    out.flush() catch {};
}

fn banner(address: std.net.Address, routes: []const Route) void {
    log(.info, "basalt serving {d} route(s) on http://{f}", .{ routes.len, address });
    for (routes) |r| {
        if (r.doc.len > 0)
            log(.info, "route {s} <- {s} — {s}", .{ r.path, r.label, r.doc })
        else
            log(.info, "route {s} <- {s}", .{ r.path, r.label });
    }
}

fn logStopping() void {
    if (runtime.aborting())
        log(.info, "aborting: in-flight runs and segment loads stop now", .{})
    else
        log(.info, "stopping: finishing in-flight requests and the segment load in flight (signal again to abort)", .{});
}

pub fn serve(gpa: std.mem.Allocator, program: ast.Program, host: []const u8, port: u16, log_cfg: runtime.LogConfig) !void {
    runtime.enterServeMode();
    initLog(log_cfg);
    const bs = try initBufState(gpa, program);
    defer if (bs) |b| {
        b.shutdown();
        gpa.destroy(b);
    };
    if (bs) |b| try startFlusher(gpa, b);
    const routes = [_]Route{.{ .path = httpPath(program), .program = program, .label = "<script>", .doc = httpDoc(program), .buf = bs }};
    var net_server = try listen(host, port);
    defer net_server.deinit();
    banner(net_server.listen_address, &routes);
    var conns = Conns{ .gpa = gpa, .routes = &routes, .threads = null };
    defer conns.drain();
    while (!runtime.shuttingDown()) {
        const conn = net_server.accept() catch continue;
        conns.dispatch(conn);
    }
    logStopping();
}

const Conns = struct {
    gpa: std.mem.Allocator,
    routes: []const Route,
    threads: ?usize,
    work: std.Thread.Mutex = .{},
    in_flight: std.atomic.Value(usize) = .init(0),
    max_threads: usize = 64,
    read_timeout_s: u32 = 30,

    /// Past `max_threads`, or when a spawn fails, the connection gets 503 with
    /// Retry-After right away, so the accept thread never serves a request.
    fn dispatch(self: *Conns, conn: std.net.Server.Connection) void {
        if (self.in_flight.fetchAdd(1, .acq_rel) < self.max_threads) {
            if (std.Thread.spawn(.{}, serveConn, .{ self, conn })) |t| {
                t.detach();
                return;
            } else |_| {}
        }
        _ = self.in_flight.fetchSub(1, .acq_rel);
        refuseBusy(conn.stream);
    }

    fn serveConn(self: *Conns, conn: std.net.Server.Connection) void {
        defer _ = self.in_flight.fetchSub(1, .acq_rel);
        const tv = std.posix.timeval{ .sec = self.read_timeout_s, .usec = 0 };
        std.posix.setsockopt(conn.stream.handle, std.posix.SOL.SOCKET, std.posix.SO.RCVTIMEO, std.mem.asBytes(&tv)) catch {};
        var read_buf: [64 * 1024]u8 = undefined;
        handleConn(self, conn, &read_buf) catch {};
        conn.stream.close();
    }

    fn drain(self: *Conns) void {
        while (self.in_flight.load(.acquire) > 0) std.Thread.sleep(10 * std.time.ns_per_ms);
    }
};

/// Answer 503 without reading the request, then drain what already arrived without
/// waiting, so the close does not reset the connection before the client reads it.
fn refuseBusy(stream: std.net.Stream) void {
    const body = "{\"status\":\"error\",\"retryable\":true,\"error\":\"server busy\"}\n";
    var buf: [256]u8 = undefined;
    const msg = std.fmt.bufPrint(&buf, "HTTP/1.1 503 Service Unavailable\r\nconnection: close\r\ncontent-type: application/json\r\nretry-after: 5\r\ncontent-length: {d}\r\n\r\n{s}", .{ body.len, body }) catch unreachable;
    _ = std.posix.send(stream.handle, msg, std.posix.MSG.DONTWAIT | std.posix.MSG.NOSIGNAL) catch {};
    std.posix.shutdown(stream.handle, .send) catch {};
    var sink: [64 * 1024]u8 = undefined;
    _ = std.posix.recv(stream.handle, &sink, std.posix.MSG.DONTWAIT) catch {};
    stream.close();
}

const Registry = struct {
    gpa: std.mem.Allocator,
    arena: std.heap.ArenaAllocator,
    routes: []Route,

    fn owns(self: Registry, bs: *BufState) bool {
        for (self.routes) |r| {
            if (r.buf == bs) return true;
        }
        return false;
    }

    /// The open buffer named `decl`, matched by name and resolved directory.
    fn findBuffer(self: Registry, decl: ast.BufferDecl) ?*BufState {
        var want_buf: [std.fs.max_path_bytes]u8 = undefined;
        const want = std.fs.cwd().realpath(decl.dir, &want_buf) catch return null;
        for (self.routes) |r| {
            const b = r.buf orelse continue;
            if (!std.mem.eql(u8, b.decl.name, decl.name)) continue;
            var have_buf: [std.fs.max_path_bytes]u8 = undefined;
            const have = b.wal.dir.realpath(".", &have_buf) catch continue;
            if (std.mem.eql(u8, have, want)) return b;
        }
        return null;
    }

    fn deinit(self: *Registry) void {
        for (self.routes) |r| {
            if (r.buf) |b| {
                b.shutdown();
                self.gpa.destroy(b);
            }
        }
        self.arena.deinit();
    }
};

/// Route table of every `CREATE ENDPOINT` `*.sql` script in `dir_path`. Other
/// scripts and parse failures are logged and skipped, so one bad file spares the rest.
/// On a reload (`prev` set) a buffer `prev` already has open is shared, not
/// reopened, and its flusher left to `reloadOnce`; at startup a buffer another
/// process holds is fatal.
fn loadDir(gpa: std.mem.Allocator, dir_path: []const u8, prev: ?*const Registry) !Registry {
    var arena = std.heap.ArenaAllocator.init(gpa);
    errdefer arena.deinit();
    const a = arena.allocator();
    var routes = std.array_list.Managed(Route).init(a);
    errdefer for (routes.items) |r| {
        const b = r.buf orelse continue;
        if (prev != null and prev.?.owns(b)) continue;
        b.shutdown();
        gpa.destroy(b);
    };

    var dir = try std.fs.cwd().openDir(dir_path, .{ .iterate = true });
    defer dir.close();
    var it = dir.iterate();
    while (try it.next()) |entry| {
        if (entry.kind != .file or !std.mem.endsWith(u8, entry.name, ".sql")) continue;
        const text = dir.readFileAlloc(a, entry.name, 8 << 20) catch |e| {
            log(.warn, "skip {s}: read failed: {s}", .{ entry.name, @errorName(e) });
            continue;
        };
        var pdiag: include.Diag = .{};
        const prog = include.loadProgram(a, text, entry.name, dir_path, &pdiag) catch {
            const at = if (pdiag.label.len > 0) pdiag.label else entry.name;
            log(.warn, "skip {s}: {s}:{d}:{d}: {s}", .{ entry.name, at, pdiag.parse.line, pdiag.parse.col, pdiag.parse.msg });
            continue;
        };
        if (prog.stmts.len == 0 or prog.stmts[0] != .kind or prog.stmts[0].kind.kind != .http) {
            log(.warn, "skip {s}: no `CREATE ENDPOINT`", .{entry.name});
            continue;
        }
        const label = try a.dupe(u8, entry.name);
        const path = httpPath(prog);
        var dup = false;
        for (routes.items) |r| {
            if (std.mem.eql(u8, r.path, path)) {
                log(.warn, "skip {s}: path `{s}` already served by {s}", .{ entry.name, path, r.label });
                dup = true;
                break;
            }
        }
        if (dup) continue;
        if (prev) |p| if (bufferDecl(prog)) |decl| if (p.findBuffer(decl)) |old| {
            for (routes.items) |r| {
                if (r.buf == old) {
                    log(.warn, "skip {s}: buffer `{s}` already served by {s}", .{ entry.name, decl.name, r.label });
                    dup = true;
                    break;
                }
            }
            if (dup) continue;
            try routes.append(.{ .path = path, .program = prog, .label = label, .doc = httpDoc(prog), .buf = old });
            continue;
        };
        const bs = initBufState(gpa, prog) catch |e| {
            if (prev == null and e == error.BufferLocked) return e;
            log(.warn, "skip {s}: buffer setup failed: {s}", .{ entry.name, @errorName(e) });
            continue;
        };
        if (bs) |b| startFlusher(gpa, b) catch |e| {
            log(.warn, "skip {s}: flusher spawn failed: {s}", .{ entry.name, @errorName(e) });
            b.shutdown();
            gpa.destroy(b);
            continue;
        };
        try routes.append(.{ .path = path, .program = prog, .label = label, .doc = httpDoc(prog), .buf = bs });
    }
    if (routes.items.len == 0) return error.NoRoutes;
    return .{ .gpa = gpa, .arena = arena, .routes = try routes.toOwnedSlice() };
}

/// A cheap fingerprint (name + mtime + size) of the `.sql` files in `dir_path`; it
/// changes when a git-sync `current` symlink repoints. 0 on error.
fn dirFingerprint(dir_path: []const u8) u64 {
    var fp: u64 = 0xcbf29ce484222325;
    var dir = std.fs.cwd().openDir(dir_path, .{ .iterate = true }) catch return 0;
    defer dir.close();
    var it = dir.iterate();
    while (it.next() catch null) |e| {
        if (e.kind != .file or !std.mem.endsWith(u8, e.name, ".sql")) continue;
        const st = dir.statFile(e.name) catch continue;
        for (e.name) |c| fp = (fp ^ c) *% 0x100000001b3;
        fp = (fp ^ @as(u64, @truncate(@as(u128, @bitCast(st.mtime))))) *% 0x100000001b3;
        fp = (fp ^ st.size) *% 0x100000001b3;
    }
    return fp;
}

/// With `watch` set, also reloads when the directory's contents change, checked
/// at most every ~2s. Reloads run on their own thread so the accept loop never waits.
pub fn serveDir(gpa: std.mem.Allocator, dir_path: []const u8, host: []const u8, port: u16, watch: bool, log_cfg: runtime.LogConfig) !void {
    runtime.enterServeMode();
    initLog(log_cfg);
    var reg = try loadDir(gpa, dir_path, null);
    defer reg.deinit();
    var net_server = try listen(host, port);
    defer net_server.deinit();
    banner(net_server.listen_address, reg.routes);
    if (watch) log(.info, "watching {s} for changes", .{dir_path});
    var conns = Conns{ .gpa = gpa, .routes = reg.routes, .threads = null };
    const reloader = try std.Thread.spawn(.{}, reloadLoop, .{ &conns, &reg, dir_path, watch, net_server.listen_address });
    defer reloader.join();
    defer conns.drain();
    while (!runtime.shuttingDown()) {
        const conn = net_server.accept() catch continue;
        conns.dispatch(conn);
    }
    logStopping();
}

fn reloadLoop(conns: *Conns, reg: *Registry, dir_path: []const u8, watch: bool, address: std.net.Address) void {
    var fp: u64 = if (watch) dirFingerprint(dir_path) else 0;
    var last_check: i64 = 0;
    while (!runtime.shuttingDown()) {
        std.Thread.sleep(200 * std.time.ns_per_ms);
        var do_reload = runtime.takeReload();
        if (watch) {
            const now_ms = std.time.milliTimestamp();
            if (now_ms - last_check >= 2000) {
                last_check = now_ms;
                const cur = dirFingerprint(dir_path);
                if (cur != fp) {
                    fp = cur;
                    do_reload = true;
                }
            }
        }
        if (do_reload and reloadOnce(conns, reg, dir_path)) banner(address, reg.routes);
    }
}

/// Load the directory off the lock, then swap the table once the current run, if
/// any, finishes; requests read their route under the same lock. False when the
/// load fails and the current routes stay. A buffer the new table keeps passes
/// over open, so each segment has one flusher: its flusher pauses before the
/// swap, the buffer takes the new script under the lock, and the flusher resumes
/// after; a buffer the new table drops drains and closes with the old table,
/// unless a stop is pending, when it stops after its in-flight segment.
fn reloadOnce(conns: *Conns, reg: *Registry, dir_path: []const u8) bool {
    const new_reg = loadDir(conns.gpa, dir_path, reg) catch |e| {
        log(.err, "reload failed (keeping current routes): {s}", .{@errorName(e)});
        return false;
    };
    for (new_reg.routes) |r| if (r.buf) |b| if (reg.owns(b)) b.stopFlusher(false);
    conns.work.lock();
    for (new_reg.routes) |r| if (r.buf) |b| if (reg.owns(b)) b.adopt(r.program);
    var old = reg.*;
    reg.* = new_reg;
    conns.routes = reg.routes;
    conns.work.unlock();
    for (old.routes) |*r| {
        const b = r.buf orelse continue;
        if (!reg.owns(b)) continue;
        r.buf = null;
        startFlusher(conns.gpa, b) catch |e|
            log(.err, "buffer {s}: flusher restart failed: {s}", .{ b.decl.name, @errorName(e) });
    }
    old.deinit();
    log(.info, "reloaded {s}", .{dir_path});
    return true;
}

fn findRoute(routes: []const Route, req_path: []const u8) ?Route {
    for (routes) |r| {
        if (std.mem.eql(u8, r.path, req_path)) return r;
    }
    return null;
}

/// The body is read before the run lock, so a slow upload holds only its own
/// thread. A request with neither Content-Length nor chunked encoding has an empty
/// body (RFC 9112 §6.3), not one read until the client closes.
fn handleConn(conns: *Conns, conn: std.net.Server.Connection, read_buf: []u8) !void {
    const gpa = conns.gpa;
    var send_buf: [16 * 1024]u8 = undefined;
    var creader = conn.stream.reader(read_buf);
    var cwriter = conn.stream.writer(&send_buf);
    var http = std.http.Server.init(creader.interface(), &cwriter.interface);
    while (http.reader.state == .ready) {
        var req = http.receiveHead() catch |e| return rejectHead(&cwriter.interface, e, http.reader.in.buffered());

        const target = req.head.target;
        const q = std.mem.indexOfScalar(u8, target, '?');
        const req_path = if (q) |qi| target[0..qi] else target;
        const query = if (q) |qi| target[qi + 1 ..] else "";

        if (std.mem.eql(u8, req_path, "/healthz") or std.mem.eql(u8, req_path, "/readyz")) {
            return respondJson(&req, .ok, "{\"status\":\"ok\"}\n");
        }

        var req_arena = std.heap.ArenaAllocator.init(gpa);
        defer req_arena.deinit();
        const ra = req_arena.allocator();
        const path = try ra.dupe(u8, req_path);
        var params = std.array_list.Managed(runtime.ParamArg).init(ra);
        try parseQuery(ra, &params, query);
        var hdrs = std.array_list.Managed(std.http.Header).init(ra);
        var hit = req.iterateHeaders();
        while (hit.next()) |h| try hdrs.append(.{ .name = try ra.dupe(u8, h.name), .value = try ra.dupe(u8, h.value) });

        const too_large = "request body over the 64 MiB limit";
        if ((req.head.content_length orelse 0) > max_body_bytes) return respondError(ra, &req, .payload_too_large, false, too_large);
        if (req.head.transfer_encoding == .none and req.head.content_length == null) req.head.content_length = 0;
        var body_buf: [64 * 1024]u8 = undefined;
        const reader = try req.readerExpectContinue(&body_buf);
        const body = reader.allocRemaining(gpa, .limited(max_body_bytes)) catch |e| switch (e) {
            error.StreamTooLong => return respondError(ra, &req, .payload_too_large, false, too_large),
            error.ReadFailed => {
                if (creader.getError() orelse e != error.WouldBlock) return e;
                return respondError(ra, &req, .request_timeout, true, "request body stalled");
            },
            else => return e,
        };
        defer gpa.free(body);

        conns.work.lock();
        defer conns.work.unlock();
        const route = findRoute(conns.routes, path) orelse return respondError(ra, &req, .not_found, false, "not found");
        dropForeignParams(&params, route.program);
        try bindHeaderParams(&params, route.program, hdrs.items);

        if (route.buf) |bs| {
            var vmsg: []const u8 = "";
            if (acceptIntoBuffer(bs, ra, body, &vmsg)) |rows| {
                var ok_buf: [96]u8 = undefined;
                const ok = std.fmt.bufPrint(&ok_buf, "{{\"status\":\"accepted\",\"rows\":{d}}}\n", .{rows}) catch unreachable;
                try respondJson(&req, .ok, ok);
            } else |e| switch (e) {
                error.Backpressure => try respondError(ra, &req, .service_unavailable, true, "buffer full"),
                error.BodySchemaViolation => try respondError(ra, &req, .unprocessable_entity, false, vmsg),
                else => return e,
            }
            return;
        }

        var diag: runtime.Diag = .{};
        var sink = runtime.OutcomeSink.init(gpa);
        defer sink.deinit();
        const result = runtime.run(gpa, route.program, .{ .params = params.items, .request_body = body, .threads = conns.threads, .outcomes = &sink, .log = g_log_cfg }, &diag);

        var out = std.array_list.Managed(u8).init(gpa);
        defer out.deinit();
        const w = out.writer();
        var status: std.http.Status = .ok;

        if (result) |stats| {
            const nfail = sink.failures();
            if (nfail == 0) {
                try w.print("{{\"status\":\"ok\",\"rows\":{d}}}\n", .{stats.rows_out});
            } else {
                status = .multi_status;
                try w.print("{{\"status\":\"partial\",\"ok\":{d},\"failed\":{d},\"rows\":{d},\"items\":[", .{ sink.list.items.len - nfail, nfail, stats.rows_out });
                var first = true;
                for (sink.list.items) |o| {
                    if (o.ok) continue;
                    if (!first) try w.writeByte(',');
                    first = false;
                    try w.writeAll("{\"item\":\"");
                    try writeJsonStr(w, o.item);
                    try w.writeAll("\",\"error\":\"");
                    try writeJsonStr(w, o.err);
                    try w.print("\",\"retryable\":{}}}", .{o.retryable});
                }
                try w.writeAll("]}\n");
            }
        } else |err| {
            const transient = diag.retryable or runtime.isTransient(err);
            status = if (transient) .service_unavailable else .unprocessable_entity;
            try w.print("{{\"status\":\"error\",\"retryable\":{},", .{transient});
            if (diag.pos) |p| try w.print("\"line\":{d},\"col\":{d},", .{ p.line, p.col });
            try w.writeAll("\"error\":\"");
            try writeJsonStr(w, @errorName(err));
            try w.writeAll(": ");
            try writeJsonStr(w, diag.msg);
            try w.writeAll("\"}\n");
        }

        return respondJson(&req, status, out.items);
    }
}

/// A JSON answer that closes the connection; a 503 says when to retry.
fn respondJson(req: *std.http.Server.Request, status: std.http.Status, body: []const u8) !void {
    const json_ct = std.http.Header{ .name = "content-type", .value = "application/json" };
    const extra: []const std.http.Header = if (status == .service_unavailable) &.{ json_ct, .{ .name = "retry-after", .value = "5" } } else &.{json_ct};
    try req.respond(body, .{ .status = status, .keep_alive = false, .extra_headers = extra });
}

fn respondError(arena: std.mem.Allocator, req: *std.http.Server.Request, status: std.http.Status, retryable: bool, msg: []const u8) !void {
    var out = std.array_list.Managed(u8).init(arena);
    try out.writer().print("{{\"status\":\"error\",\"retryable\":{},\"error\":\"", .{retryable});
    try writeJsonStr(out.writer(), msg);
    try out.appendSlice("\"}\n");
    try respondJson(req, status, out.items);
}

fn writeJsonStr(w: anytype, s: []const u8) !void {
    for (s) |c| switch (c) {
        '"' => try w.writeAll("\\\""),
        '\\' => try w.writeAll("\\\\"),
        '\n' => try w.writeAll("\\n"),
        '\r' => try w.writeAll("\\r"),
        '\t' => try w.writeAll("\\t"),
        else => if (c < 0x20) try w.print("\\u{x:0>4}", .{c}) else try w.writeByte(c),
    };
}

/// Bind every `FROM HEADER` param whose header (`header_name`, else the param's own
/// name) is present. Header names compare case-insensitively (RFC 9110).
fn bindHeaderParams(
    params: *std.array_list.Managed(runtime.ParamArg),
    program: ast.Program,
    headers: []const std.http.Header,
) !void {
    for (program.stmts) |s| {
        if (s != .param) continue;
        const p = s.param;
        const src = p.source orelse continue;
        if (src != .header) continue;
        const want = p.header_name orelse p.name;
        for (headers) |h| {
            if (std.ascii.eqlIgnoreCase(h.name, want)) {
                try params.append(.{ .key = p.name, .val = h.value });
                break;
            }
        }
    }
}

/// Drop any query-string binding that names a sealed `LET` or a `PARAM` declared to
/// come from somewhere else (a header, or the body, the default for `JSON`), so a
/// caller cannot override a header or body value through the URL.
fn dropForeignParams(params: *std.array_list.Managed(runtime.ParamArg), program: ast.Program) void {
    var i: usize = 0;
    while (i < params.items.len) {
        var foreign = false;
        for (program.stmts) |s| switch (s) {
            .let_const => |l| if (std.mem.eql(u8, l.name, params.items[i].key)) {
                foreign = true;
            },
            .param => |p| if (std.mem.eql(u8, p.name, params.items[i].key)) {
                const src = p.source orelse (if (p.is_json) ast.ParamSource.body else .query);
                if (src != .query) foreign = true;
            },
            else => {},
        };
        if (foreign) {
            _ = params.orderedRemove(i);
        } else {
            i += 1;
        }
    }
}

/// Parse a `k=v&k2=v2` query string into params, each side decoded as a browser or
/// a form encodes it: `%XX` is that byte and `+` a space (`%2B` is a plus).
fn parseQuery(arena: std.mem.Allocator, params: *std.array_list.Managed(runtime.ParamArg), query: []const u8) !void {
    var it = std.mem.splitScalar(u8, query, '&');
    while (it.next()) |pair| {
        if (pair.len == 0) continue;
        const eq = std.mem.indexOfScalar(u8, pair, '=') orelse continue;
        try params.append(.{ .key = try decodeQueryPart(arena, pair[0..eq]), .val = try decodeQueryPart(arena, pair[eq + 1 ..]) });
    }
}

/// A `%` not followed by two hex digits is kept as written. The result is always
/// arena-owned: the request head it came from does not outlive the body read.
fn decodeQueryPart(arena: std.mem.Allocator, s: []const u8) ![]const u8 {
    if (std.mem.indexOfAny(u8, s, "%+") == null) return arena.dupe(u8, s);
    var out = try std.array_list.Managed(u8).initCapacity(arena, s.len);
    var i: usize = 0;
    while (i < s.len) : (i += 1) {
        if (s[i] == '+') {
            out.appendAssumeCapacity(' ');
        } else if (s[i] == '%' and i + 2 < s.len) {
            if (std.fmt.parseInt(u8, s[i + 1 .. i + 3], 16)) |b| {
                out.appendAssumeCapacity(b);
                i += 2;
            } else |_| out.appendAssumeCapacity('%');
        } else out.appendAssumeCapacity(s[i]);
    }
    return out.items;
}

fn attrToStr(e: *const ast.Expr) []const u8 {
    return switch (e.*) {
        .str_lit => |s| s,
        .field => |q| q.parts[q.parts.len - 1],
        else => "/",
    };
}

test "bindHeaderParams: named + bare FROM HEADER, case-insensitive, missing skipped" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(a,
        \\CREATE ENDPOINT '/x';
        \\PARAM tenant STRING FROM HEADER('X-Tenant');
        \\PARAM trace  STRING FROM HEADER;
        \\PARAM ghost  STRING FROM HEADER('X-Ghost');
    , &diag);

    var params = std.array_list.Managed(runtime.ParamArg).init(a);
    const headers = [_]std.http.Header{
        .{ .name = "x-tenant", .value = "acme" },
        .{ .name = "Trace", .value = "t-123" },
        .{ .name = "Content-Type", .value = "application/json" },
    };
    try bindHeaderParams(&params, prog, &headers);

    try std.testing.expectEqual(@as(usize, 2), params.items.len);
    try std.testing.expectEqualStrings("tenant", params.items[0].key);
    try std.testing.expectEqualStrings("acme", params.items[0].val);
    try std.testing.expectEqualStrings("trace", params.items[1].key);
    try std.testing.expectEqualStrings("t-123", params.items[1].val);
}

test "buffered endpoint: accept -> WAL -> drain through the pipeline" {
    initLog(.{ .quiet = true });
    const gpa = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    const base = try tmp.dir.realpathAlloc(a, ".");

    const script = try std.fmt.allocPrint(a,
        \\CREATE ENDPOINT '/ev'
        \\  ACCEPT BODY (device_id STRING NOT NULL, v INT)
        \\  INTO BUFFER 'ev' AT '{s}/wal' SEGMENT 1 MB RETAIN UNTIL LOADED;
        \\LOAD INTO '{s}/out.csv' AS
        \\SELECT device_id, CAST(v AS INT) AS v FROM BUFFER 'ev' FLUSH EVERY 1 SECONDS;
    , .{ base, base });
    var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(a, script, &diag);

    const bs = (try initBufState(gpa, prog)).?;
    defer {
        bs.shutdown();
        gpa.destroy(bs);
    }
    try std.testing.expectEqual(@as(u64, 1), bs.flush_secs);

    var vmsg: []const u8 = "";
    const n = try acceptIntoBuffer(bs, a,
        \\[{"device_id":"a","v":1},{"device_id":"b","v":2}]
    , &vmsg);
    try std.testing.expectEqual(@as(usize, 2), n);

    try std.testing.expectError(error.BodySchemaViolation, acceptIntoBuffer(bs, a,
        \\[{"v":3}]
    , &vmsg));
    try std.testing.expect(std.mem.indexOf(u8, vmsg, "device_id") != null);

    const saved_max = bs.max_bytes;
    bs.max_bytes = 1;
    try std.testing.expectError(error.Backpressure, acceptIntoBuffer(bs, a,
        \\[{"device_id":"c","v":3}]
    , &vmsg));
    bs.max_bytes = saved_max;

    try bs.wal.rotateIfNonEmpty();
    drainPending(gpa, bs);

    const out = try tmp.dir.readFileAlloc(a, "out.csv", 1 << 20);
    try std.testing.expectEqualStrings("device_id,v\na,1\nb,2\n", out);
    try std.testing.expectEqual(@as(u64, 1), bs.wal.loadedUpTo());
    const pending = try bs.wal.pendingSegments(gpa);
    defer gpa.free(pending);
    try std.testing.expectEqual(@as(usize, 0), pending.len);
    try std.testing.expectEqual(@as(u64, 0), bs.wal.bytesOnDisk());
}

fn acceptN(gpa: std.mem.Allocator, routes: []const Route, srv: *std.net.Server, n: usize) void {
    var conns = Conns{ .gpa = gpa, .routes = routes, .threads = 1 };
    var read_buf: [64 * 1024]u8 = undefined;
    var i: usize = 0;
    while (i < n) : (i += 1) {
        const conn = srv.accept() catch return;
        handleConn(&conns, conn, &read_buf) catch {};
        conn.stream.close();
    }
}

test "serve integration: buffered accept and FROM HEADER over real HTTP" {
    initLog(.{ .quiet = true });
    const gpa = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n7\n" });

    const ev_script = try std.fmt.allocPrint(a,
        \\CREATE ENDPOINT '/ev'
        \\  ACCEPT BODY (device_id STRING NOT NULL, v INT)
        \\  INTO BUFFER 'ev' AT '{s}/wal' SEGMENT 1 MB MAX 512 MB RETAIN UNTIL LOADED;
        \\LOAD INTO '{s}/out_ev.csv' AS
        \\SELECT device_id, CAST(v AS INT) AS v FROM BUFFER 'ev';
    , .{ base, base });
    const hdr_script = try std.fmt.allocPrint(a,
        \\CREATE ENDPOINT '/hdr';
        \\PARAM tenant STRING FROM HEADER('X-Tenant');
        \\LOAD INTO '{s}/out_hdr.csv' AS
        \\SELECT id, $tenant AS tenant FROM '{s}/in.csv';
    , .{ base, base });

    var d1: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const ev_prog = try parser.parseSource(a, ev_script, &d1);
    var d2: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const hdr_prog = try parser.parseSource(a, hdr_script, &d2);

    const bs = (try initBufState(gpa, ev_prog)).?;
    defer {
        bs.shutdown();
        gpa.destroy(bs);
    }
    try std.testing.expectEqual(@as(u64, 512 << 20), bs.max_bytes);
    const routes = [_]Route{
        .{ .path = "/ev", .program = ev_prog, .label = "ev", .buf = bs },
        .{ .path = "/hdr", .program = hdr_prog, .label = "hdr" },
    };

    const addr = try std.net.Address.parseIp("127.0.0.1", 0);
    var srv = try addr.listen(.{ .reuse_address = true });
    defer srv.deinit();
    const port = srv.listen_address.getPort();
    const th = try std.Thread.spawn(.{}, acceptN, .{ gpa, &routes, &srv, @as(usize, 6) });
    var joined = false;
    defer if (!joined) {
        for (0..6) |_| if (std.net.tcpConnectToAddress(srv.listen_address)) |c| c.close() else |_| {};
        th.join();
    };

    var client = std.http.Client{ .allocator = gpa };
    defer client.deinit();
    const ev_url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/ev", .{port});
    const hdr_url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/hdr", .{port});

    {
        var aw = std.Io.Writer.Allocating.init(gpa);
        defer aw.deinit();
        const res = try client.fetch(.{
            .location = .{ .url = ev_url },
            .method = .POST,
            .payload =
            \\[{"device_id":"a","v":1},{"device_id":"b","v":2}]
            ,
            .response_writer = &aw.writer,
            .keep_alive = false,
        });
        try std.testing.expectEqual(std.http.Status.ok, res.status);
        try std.testing.expect(std.mem.indexOf(u8, aw.written(), "accepted") != null);
    }
    {
        var aw = std.Io.Writer.Allocating.init(gpa);
        defer aw.deinit();
        const res = try client.fetch(.{
            .location = .{ .url = ev_url },
            .method = .POST,
            .payload =
            \\[{"v":9}]
            ,
            .response_writer = &aw.writer,
            .keep_alive = false,
        });
        try std.testing.expectEqual(std.http.Status.unprocessable_entity, res.status);
        try std.testing.expect(std.mem.indexOf(u8, aw.written(), "device_id") != null);
    }
    {
        var aw = std.Io.Writer.Allocating.init(gpa);
        defer aw.deinit();
        const big = try a.alloc(u8, 200 * 1024);
        @memset(big, 'x');
        const res = try client.fetch(.{
            .location = .{ .url = try std.fmt.allocPrint(a, "{s}?tenant=spoof", .{hdr_url}) },
            .method = .POST,
            .payload = big,
            .response_writer = &aw.writer,
            .keep_alive = false,
            .extra_headers = &.{.{ .name = "X-Tenant", .value = "acme" }},
        });
        try std.testing.expectEqual(std.http.Status.ok, res.status);
    }
    for ([_][2][]const u8{
        .{ "GARBAGE\r\n\r\n", "HTTP/1.1 400 " },
        .{ "GET /hdr HTTP/1.1\r\nHost: x\r\n", "HTTP/1.1 400 " },
        .{ "POST /hdr HTTP/1.1\r\ncontent-length: 99999999999\r\n\r\n", "HTTP/1.1 413 " },
    }) |c| {
        const s = try std.net.tcpConnectToAddress(srv.listen_address);
        defer s.close();
        try s.writeAll(c[0]);
        try std.posix.shutdown(s.handle, .send);
        var rbuf: [512]u8 = undefined;
        var r = s.reader(&rbuf);
        const got = try r.interface().takeArray(13);
        try std.testing.expectEqualStrings(c[1], got);
    }
    th.join();
    joined = true;

    const hdr_out = try tmp.dir.readFileAlloc(a, "out_hdr.csv", 1 << 20);
    try std.testing.expectEqualStrings("id,tenant\n7,acme\n", hdr_out);

    try bs.wal.rotateIfNonEmpty();
    drainPending(gpa, bs);
    const ev_out = try tmp.dir.readFileAlloc(a, "out_ev.csv", 1 << 20);
    try std.testing.expectEqualStrings("device_id,v\na,1\nb,2\n", ev_out);
}

test "findRoute matches exact paths only" {
    const routes = [_]Route{
        .{ .path = "/ingest", .program = .{ .stmts = &.{} }, .label = "a.bsl" },
        .{ .path = "/ingest/v2", .program = .{ .stmts = &.{} }, .label = "b.bsl" },
    };
    try std.testing.expectEqualStrings("a.bsl", findRoute(&routes, "/ingest").?.label);
    try std.testing.expectEqualStrings("b.bsl", findRoute(&routes, "/ingest/v2").?.label);
    try std.testing.expect(findRoute(&routes, "/ingest/") == null);
    try std.testing.expect(findRoute(&routes, "/inge") == null);
    try std.testing.expect(findRoute(&routes, "/") == null);
}

test "parseQuery binds k=v pairs, decodes each side as a form encodes it, and skips malformed pairs" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var params = std.array_list.Managed(runtime.ParamArg).init(a);
    try parseQuery(a, &params, "a=1&b=x%20y&novalue&&c=&city=S%C3%A3o+Paulo&q=1%2B1&n%61me=v&bad=%zz%4");
    const want = [_][2][]const u8{
        .{ "a", "1" },   .{ "b", "x y" },  .{ "c", "" },        .{ "city", "S\u{e3}o Paulo" },
        .{ "q", "1+1" }, .{ "name", "v" }, .{ "bad", "%zz%4" },
    };
    try std.testing.expectEqual(want.len, params.items.len);
    for (want, params.items) |w, got| {
        try std.testing.expectEqualStrings(w[0], got.key);
        try std.testing.expectEqualStrings(w[1], got.val);
    }

    params.clearRetainingCapacity();
    try parseQuery(a, &params, "");
    try std.testing.expectEqual(@as(usize, 0), params.items.len);
}

test "dropForeignParams: a query string binds only query params, never a LET, header or body param" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(a,
        \\CREATE ENDPOINT '/r';
        \\PARAM tenant STRING DEFAULT 'x' FROM QUERY;
        \\LET cutoff = 5;
        \\PARAM org STRING FROM HEADER('X-Org');
        \\PARAM job JSON;
        \\PARAM day INT;
        \\SELECT id FROM 'in.csv' WHERE id > $cutoff;
    , &diag);

    var params = std.array_list.Managed(runtime.ParamArg).init(a);
    try parseQuery(a, &params, "tenant=acme&cutoff=999&org=evil&job=%7B%7D&day=3");
    dropForeignParams(&params, prog);
    try std.testing.expectEqual(@as(usize, 2), params.items.len);
    try std.testing.expectEqualStrings("tenant", params.items[0].key);
    try std.testing.expectEqualStrings("acme", params.items[0].val);
    try std.testing.expectEqualStrings("day", params.items[1].key);
}

test "health probes answer while a request holds the run lock" {
    initLog(.{ .quiet = true });
    var srv = try listen("127.0.0.1", 0);
    defer srv.deinit();
    var conns = Conns{ .gpa = std.testing.allocator, .routes = &.{}, .threads = 1 };
    conns.work.lock();
    defer conns.drain();
    defer conns.work.unlock();

    const s = try std.net.tcpConnectToAddress(srv.listen_address);
    defer s.close();
    const tv = std.posix.timeval{ .sec = 5, .usec = 0 };
    try std.posix.setsockopt(s.handle, std.posix.SOL.SOCKET, std.posix.SO.RCVTIMEO, std.mem.asBytes(&tv));
    try s.writeAll("GET /healthz HTTP/1.1\r\n\r\n");
    conns.dispatch(try srv.accept());
    var rbuf: [256]u8 = undefined;
    var r = s.reader(&rbuf);
    try std.testing.expectEqualStrings("HTTP/1.1 200 ", try r.interface().takeArray(13));
}

fn testConnect(srv: *std.net.Server, conns: *Conns, req: []const u8) !std.net.Stream {
    const s = try std.net.tcpConnectToAddress(srv.listen_address);
    errdefer s.close();
    const tv = std.posix.timeval{ .sec = 5, .usec = 0 };
    try std.posix.setsockopt(s.handle, std.posix.SOL.SOCKET, std.posix.SO.RCVTIMEO, std.mem.asBytes(&tv));
    try s.writeAll(req);
    conns.dispatch(try srv.accept());
    return s;
}

fn expectStatus(s: std.net.Stream, comptime want: []const u8) !void {
    var rbuf: [512]u8 = undefined;
    var r = s.reader(&rbuf);
    try std.testing.expectEqualStrings(want, try r.interface().takeArray(want.len));
}

test "a slow request body blocks neither health probes nor other endpoints; a stalled one gets 408" {
    initLog(.{ .quiet = true });
    const gpa = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n7\n" });
    var d: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const pa = try parser.parseSource(a, try std.fmt.allocPrint(a, "CREATE ENDPOINT '/a'; LOAD INTO '{s}/a.csv' AS SELECT id FROM '{s}/in.csv';", .{ base, base }), &d);
    const pb = try parser.parseSource(a, try std.fmt.allocPrint(a, "CREATE ENDPOINT '/b'; LOAD INTO '{s}/b.csv' AS SELECT id FROM '{s}/in.csv';", .{ base, base }), &d);
    const routes = [_]Route{
        .{ .path = "/a", .program = pa, .label = "a" },
        .{ .path = "/b", .program = pb, .label = "b" },
    };

    var srv = try listen("127.0.0.1", 0);
    defer srv.deinit();
    var conns = Conns{ .gpa = gpa, .routes = &routes, .threads = 1 };
    defer conns.drain();

    const slow = try testConnect(&srv, &conns, "POST /a HTTP/1.1\r\ncontent-length: 6\r\n\r\nabc");
    defer slow.close();
    {
        const h = try testConnect(&srv, &conns, "GET /healthz HTTP/1.1\r\n\r\n");
        defer h.close();
        try expectStatus(h, "HTTP/1.1 200 ");
    }
    {
        const b = try testConnect(&srv, &conns, "POST /b HTTP/1.1\r\n\r\n");
        defer b.close();
        try expectStatus(b, "HTTP/1.1 200 ");
    }
    try slow.writeAll("def");
    try expectStatus(slow, "HTTP/1.1 200 ");

    conns.drain();
    conns.read_timeout_s = 1;
    const stalled = try testConnect(&srv, &conns, "POST /a HTTP/1.1\r\ncontent-length: 6\r\n\r\nabc");
    defer stalled.close();
    try expectStatus(stalled, "HTTP/1.1 408 ");
}

fn testReload(conns: *Conns, reg: *Registry, dir: []const u8) void {
    _ = reloadOnce(conns, reg, dir);
}

test "a reload waiting on a long run leaves new connections accepted and answered" {
    initLog(.{ .quiet = true });
    const gpa = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const dir = try tmp.dir.realpathAlloc(ar.allocator(), ".");
    try tmp.dir.writeFile(.{ .sub_path = "one.sql", .data = "CREATE ENDPOINT '/one'; LOAD INTO 'o.csv' AS SELECT 1 AS x;" });

    var reg = try loadDir(gpa, dir, null);
    defer reg.deinit();
    var srv = try listen("127.0.0.1", 0);
    defer srv.deinit();
    var conns = Conns{ .gpa = gpa, .routes = reg.routes, .threads = 1 };
    defer conns.drain();

    try tmp.dir.writeFile(.{ .sub_path = "two.sql", .data = "CREATE ENDPOINT '/two'; LOAD INTO 't.csv' AS SELECT 1 AS x;" });
    conns.work.lock();
    const reloader = try std.Thread.spawn(.{}, testReload, .{ &conns, &reg, dir });
    var locked = true;
    defer if (locked) {
        conns.work.unlock();
        reloader.join();
    };
    std.Thread.sleep(100 * std.time.ns_per_ms);
    {
        const h = try testConnect(&srv, &conns, "GET /healthz HTTP/1.1\r\n\r\n");
        defer h.close();
        try expectStatus(h, "HTTP/1.1 200 ");
    }
    try std.testing.expectEqual(@as(usize, 1), conns.routes.len);
    conns.work.unlock();
    reloader.join();
    locked = false;
    try std.testing.expectEqual(@as(usize, 2), conns.routes.len);
    try std.testing.expect(findRoute(conns.routes, "/two") != null);
}

fn evScript(a: std.mem.Allocator, base: []const u8, flush: []const u8) ![]const u8 {
    return std.fmt.allocPrint(a,
        \\CREATE ENDPOINT '/ev'
        \\  ACCEPT BODY (id INT NOT NULL)
        \\  INTO BUFFER 'ev' AT '{s}/wal' SEGMENT 1 MB RETAIN UNTIL LOADED;
        \\LOAD INTO '{s}/out.csv' APPEND AS
        \\SELECT CAST(id AS INT) AS id FROM BUFFER 'ev' {s};
    , .{ base, base, flush });
}

fn expectEachIdOnce(a: std.mem.Allocator, dir: std.fs.Dir, n: usize) !void {
    const out = try dir.readFileAlloc(a, "out.csv", 1 << 20);
    const seen = try a.alloc(u8, n);
    @memset(seen, 0);
    var it = std.mem.tokenizeScalar(u8, out, '\n');
    try std.testing.expectEqualStrings("id", it.next().?);
    while (it.next()) |line| seen[try std.fmt.parseInt(usize, line, 10)] += 1;
    for (seen) |c| try std.testing.expectEqual(@as(u8, 1), c);
}

fn acceptLoop(conns: *Conns, n: usize, accepted: *std.atomic.Value(usize)) void {
    for (0..n) |i| {
        var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer ar.deinit();
        const body = std.fmt.allocPrint(ar.allocator(), "{{\"id\":{d}}}", .{i}) catch return;
        var msg: []const u8 = "";
        conns.work.lock();
        const bs = findRoute(conns.routes, "/ev").?.buf.?;
        _ = acceptIntoBuffer(bs, ar.allocator(), body, &msg) catch {
            conns.work.unlock();
            return;
        };
        conns.work.unlock();
        _ = accepted.fetchAdd(1, .seq_cst);
        std.Thread.sleep(2 * std.time.ns_per_ms);
    }
}

test "reloads hand an open buffer over: every accepted row is loaded exactly once" {
    initLog(.{ .quiet = true });
    const gpa = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try tmp.dir.makeDir("scripts");
    const scripts = try std.fs.path.join(a, &.{ base, "scripts" });
    var sdir = try tmp.dir.openDir("scripts", .{});
    defer sdir.close();
    try sdir.writeFile(.{ .sub_path = "ev.sql", .data = try evScript(a, base, "FLUSH EVERY 1 SECONDS OR 1 ROWS") });

    var reg = try loadDir(gpa, scripts, null);
    var reg_live = true;
    defer if (reg_live) reg.deinit();
    var conns = Conns{ .gpa = gpa, .routes = reg.routes, .threads = 1 };

    const n = 400;
    var accepted = std.atomic.Value(usize).init(0);
    const writer = try std.Thread.spawn(.{}, acceptLoop, .{ &conns, @as(usize, n), &accepted });
    for (0..6) |i| {
        std.Thread.sleep(150 * std.time.ns_per_ms);
        if (i == 2) try sdir.writeFile(.{ .sub_path = "ev.sql", .data = try evScript(a, base, "FLUSH EVERY 1 SECONDS OR 3 ROWS") });
        if (i == 4) try sdir.rename("ev.sql", "renamed.sql");
        try std.testing.expect(reloadOnce(&conns, &reg, scripts));
    }
    writer.join();
    try std.testing.expectEqual(@as(usize, n), accepted.load(.seq_cst));
    try std.testing.expectEqual(@as(u64, 3), findRoute(conns.routes, "/ev").?.buf.?.flush_rows);
    reg.deinit();
    reg_live = false;
    try expectEachIdOnce(a, tmp.dir, n);
}

test "a restart resumes pending segments once; a buffer already open is refused" {
    initLog(.{ .quiet = true });
    const gpa = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try tmp.dir.makeDir("scripts");
    const scripts = try std.fs.path.join(a, &.{ base, "scripts" });
    const text = try evScript(a, base, "");
    try tmp.dir.writeFile(.{ .sub_path = "scripts/ev.sql", .data = text });
    var d: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(a, text, &d);

    var msg: []const u8 = "";
    {
        const bs = (try initBufState(gpa, prog)).?;
        defer gpa.destroy(bs);
        defer bs.wal.close();
        try std.testing.expectError(error.BufferLocked, initBufState(gpa, prog));
        try std.testing.expectError(error.BufferLocked, loadDir(gpa, scripts, null));
        _ = try acceptIntoBuffer(bs, a, "[{\"id\":0},{\"id\":1}]", &msg);
        try bs.wal.rotate();
        drainPending(gpa, bs);
        _ = try acceptIntoBuffer(bs, a, "{\"id\":2}", &msg);
        try bs.wal.rotate();
        _ = try acceptIntoBuffer(bs, a, "{\"id\":3}", &msg);
    }
    const bs = (try initBufState(gpa, prog)).?;
    defer {
        bs.shutdown();
        gpa.destroy(bs);
    }
    _ = try acceptIntoBuffer(bs, a, "{\"id\":4}", &msg);
    try startFlusher(gpa, bs);
    bs.stopFlusher(true);
    try expectEachIdOnce(a, tmp.dir, 5);
}

test "a graceful stop starts no further segment load; they wait for the next start" {
    initLog(.{ .quiet = true });
    const gpa = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    const base = try tmp.dir.realpathAlloc(a, ".");
    var d: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(a, try evScript(a, base, ""), &d);

    var msg: []const u8 = "";
    {
        const bs = (try initBufState(gpa, prog)).?;
        defer gpa.destroy(bs);
        defer bs.wal.close();
        _ = try acceptIntoBuffer(bs, a, "[{\"id\":0},{\"id\":1}]", &msg);
        try bs.wal.rotate();
        _ = try acceptIntoBuffer(bs, a, "{\"id\":2}", &msg);
        try bs.wal.rotate();
        runtime.enterServeMode();
        defer runtime.resetShutdown();
        try std.testing.expect(runtime.terminate());
        try std.testing.expect(!runtime.aborting());
        drainPending(gpa, bs);
        try startFlusher(gpa, bs);
        bs.stopFlusher(true);
        try std.testing.expectEqual(@as(u64, 0), bs.wal.loadedUpTo());
    }
    try std.testing.expectError(error.FileNotFound, tmp.dir.access("out.csv", .{}));
    const bs = (try initBufState(gpa, prog)).?;
    defer {
        bs.shutdown();
        gpa.destroy(bs);
    }
    try bs.wal.rotateIfNonEmpty();
    drainPending(gpa, bs);
    try expectEachIdOnce(a, tmp.dir, 3);
}

test "past the connection cap a request gets 503 + Retry-After instead of an inline run" {
    initLog(.{ .quiet = true });
    var srv = try listen("127.0.0.1", 0);
    defer srv.deinit();
    var conns = Conns{ .gpa = std.testing.allocator, .routes = &.{}, .threads = 1, .max_threads = 1 };
    defer conns.drain();

    const busy = try testConnect(&srv, &conns, "GET /healthz HTTP/1.1\r\n");
    defer busy.close();
    const s = try testConnect(&srv, &conns, "GET /healthz HTTP/1.1\r\n\r\n");
    defer s.close();
    var rbuf: [512]u8 = undefined;
    var r = s.reader(&rbuf);
    const resp = try r.interface().allocRemaining(std.testing.allocator, .limited(4096));
    defer std.testing.allocator.free(resp);
    try std.testing.expect(std.mem.startsWith(u8, resp, "HTTP/1.1 503 "));
    try std.testing.expect(std.mem.indexOf(u8, resp, "retry-after: 5\r\n") != null);
    try std.posix.shutdown(busy.handle, .send);
}

test "listen refuses a port another server already holds" {
    initLog(.{ .quiet = true });
    var first = try listen("127.0.0.1", 0);
    defer first.deinit();
    try std.testing.expectError(error.AddressInUse, listen("127.0.0.1", first.listen_address.getPort()));
}

test "writeJsonStr escapes everything that would break the response body" {
    var buf = std.array_list.Managed(u8).init(std.testing.allocator);
    defer buf.deinit();
    try writeJsonStr(buf.writer(), "say \"hi\"\\\n\ttab\r");
    try std.testing.expectEqualStrings("say \\\"hi\\\"\\\\\\n\\ttab\\r", buf.items);
    buf.clearRetainingCapacity();
    try writeJsonStr(buf.writer(), "ctrl:\x01");
    try std.testing.expectEqualStrings("ctrl:\\u0001", buf.items);
}

test "httpPath/httpDoc read CREATE ENDPOINT config, with defaults" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(a,
        \\CREATE ENDPOINT '/events' DOC 'Ingest events';
        \\LOAD INTO 'out.csv' AS SELECT * FROM BODY (id INT);
    , &diag);
    try std.testing.expectEqualStrings("/events", httpPath(prog));
    try std.testing.expectEqualStrings("Ingest events", httpDoc(prog));

    const bare = try parser.parseSource(a,
        \\CREATE ENDPOINT '/';
        \\LOAD INTO 'out.csv' AS SELECT * FROM BODY (id INT);
    , &diag);
    try std.testing.expectEqualStrings("/", httpPath(bare));
    try std.testing.expectEqualStrings("", httpDoc(bare));

    const batch = try parser.parseSource(a,
        \\LOAD INTO 'out.csv' AS SELECT * FROM 'in.csv';
    , &diag);
    try std.testing.expectEqualStrings("/", httpPath(batch));
    try std.testing.expectEqualStrings("", httpDoc(batch));
    try std.testing.expectEqualStrings("/", httpPath(.{ .stmts = &.{} }));
}
