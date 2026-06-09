//! HTTP trigger host for `@http` scripts. Each matching request runs the pipeline
//! once: query-string values bind params, the request body feeds `read request`,
//! and the response is a JSON run summary. Single-threaded accept loop (one request
//! at a time) — scale out with replicas behind a load balancer.
//!
//! Two entry points: `serve` hosts a single program; `serveDir` hosts every `@http`
//! script in a directory, routing by each script's declared `@http(path=…)` and
//! reloading the directory on SIGHUP (the control plane writes scripts, then signals).

const std = @import("std");
const ast = @import("../lang/ast.zig");
const parser = @import("../lang/parser.zig");
const runtime = @import("../runtime/run.zig");

pub const Route = struct { path: []const u8, program: ast.Program, label: []const u8 };

/// The `@http(path=…)` of a program (defaults to "/").
fn httpPath(program: ast.Program) []const u8 {
    if (program.stmts.len == 0 or program.stmts[0] != .kind) return "/";
    var p: []const u8 = "/";
    for (program.stmts[0].kind.config) |attr| {
        if (std.mem.eql(u8, attr.key, "path")) p = attrToStr(attr.value);
    }
    return p;
}

/// Listen on `0.0.0.0:port` with a 1s accept timeout so the loop re-checks the
/// shutdown/reload flags even when idle (SIGTERM/SIGHUP take effect within ~1s).
fn listen(port: u16) !std.net.Server {
    const address = try std.net.Address.parseIp("0.0.0.0", port);
    var net_server = try address.listen(.{ .reuse_address = true });
    errdefer net_server.deinit();
    const tv = std.posix.timeval{ .sec = 1, .usec = 0 };
    std.posix.setsockopt(net_server.stream.handle, std.posix.SOL.SOCKET, std.posix.SO.RCVTIMEO, std.mem.asBytes(&tv)) catch {};
    return net_server;
}

fn banner(port: u16, routes: []const Route) void {
    std.debug.print("basalt serving {d} route(s) on http://0.0.0.0:{d}\n", .{ routes.len, port });
    for (routes) |r| std.debug.print("  {s}  <- {s}\n", .{ r.path, r.label });
}

/// Serve a single `@http` program.
pub fn serve(gpa: std.mem.Allocator, program: ast.Program, port: u16) !void {
    const routes = [_]Route{.{ .path = httpPath(program), .program = program, .label = "<script>" }};
    var net_server = try listen(port);
    defer net_server.deinit();
    banner(port, &routes);
    const threads = std.Thread.getCpuCount() catch 1;
    var read_buf: [64 * 1024]u8 = undefined;
    while (!runtime.aborting()) {
        const conn = net_server.accept() catch continue;
        handleConn(gpa, &routes, conn, &read_buf, threads) catch {};
        conn.stream.close();
    }
}

const Registry = struct {
    arena: std.heap.ArenaAllocator,
    routes: []const Route,
    fn deinit(self: *Registry) void {
        self.arena.deinit();
    }
};

/// Parse every `*.bsl` `@http` script in `dir_path` into a route table. Non-`@http`
/// scripts and parse failures are skipped (logged), so one bad file doesn't take
/// down the rest of the fleet.
fn loadDir(gpa: std.mem.Allocator, dir_path: []const u8) !Registry {
    var arena = std.heap.ArenaAllocator.init(gpa);
    errdefer arena.deinit();
    const a = arena.allocator();
    var routes = std.array_list.Managed(Route).init(a);

    var dir = try std.fs.cwd().openDir(dir_path, .{ .iterate = true });
    defer dir.close();
    var it = dir.iterate();
    while (try it.next()) |entry| {
        if (entry.kind != .file or !std.mem.endsWith(u8, entry.name, ".bsl")) continue;
        const text = dir.readFileAlloc(a, entry.name, 8 << 20) catch |e| {
            std.debug.print("skip {s}: read failed: {s}\n", .{ entry.name, @errorName(e) });
            continue;
        };
        var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        const prog = parser.parseSource(a, text, &pdiag) catch {
            std.debug.print("skip {s}: {d}:{d}: {s}\n", .{ entry.name, pdiag.line, pdiag.col, pdiag.msg });
            continue;
        };
        if (prog.stmts.len == 0 or prog.stmts[0] != .kind or prog.stmts[0].kind.kind != .http) {
            std.debug.print("skip {s}: not an @http script\n", .{entry.name});
            continue;
        }
        const label = try a.dupe(u8, entry.name); // entry.name is reused by the iterator
        const path = httpPath(prog);
        var dup = false;
        for (routes.items) |r| {
            if (std.mem.eql(u8, r.path, path)) {
                std.debug.print("skip {s}: path `{s}` already served by {s}\n", .{ entry.name, path, r.label });
                dup = true;
                break;
            }
        }
        if (dup) continue;
        try routes.append(.{ .path = path, .program = prog, .label = label });
    }
    if (routes.items.len == 0) return error.NoRoutes;
    return .{ .arena = arena, .routes = try routes.toOwnedSlice() };
}

/// A cheap content fingerprint of the `.bsl` files in `dir_path` (name + mtime +
/// size). Changes when a script is added, removed, or edited — including when a
/// git-sync `current` symlink repoints to a fresh checkout. 0 on error.
fn dirFingerprint(dir_path: []const u8) u64 {
    var fp: u64 = 0xcbf29ce484222325; // FNV-1a basis as a seed
    var dir = std.fs.cwd().openDir(dir_path, .{ .iterate = true }) catch return 0;
    defer dir.close();
    var it = dir.iterate();
    while (it.next() catch null) |e| {
        if (e.kind != .file or !std.mem.endsWith(u8, e.name, ".bsl")) continue;
        const st = dir.statFile(e.name) catch continue;
        for (e.name) |c| fp = (fp ^ c) *% 0x100000001b3;
        fp = (fp ^ @as(u64, @truncate(@as(u128, @bitCast(st.mtime))))) *% 0x100000001b3;
        fp = (fp ^ st.size) *% 0x100000001b3;
    }
    return fp;
}

/// Serve every `@http` script in a directory, routing by path. Reloads on SIGHUP,
/// and — when `watch` is set — automatically when the directory's contents change
/// (e.g. a git-sync sidecar pulled new scripts), checked at most every ~2s.
pub fn serveDir(gpa: std.mem.Allocator, dir_path: []const u8, port: u16, watch: bool) !void {
    var reg = try loadDir(gpa, dir_path);
    defer reg.deinit();
    var net_server = try listen(port);
    defer net_server.deinit();
    banner(port, reg.routes);
    if (watch) std.debug.print("watching {s} for changes\n", .{dir_path});
    const threads = std.Thread.getCpuCount() catch 1;
    var read_buf: [64 * 1024]u8 = undefined;
    var fp: u64 = if (watch) dirFingerprint(dir_path) else 0;
    var last_check: i64 = 0;
    while (!runtime.aborting()) {
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
        if (do_reload) {
            if (loadDir(gpa, dir_path)) |new_reg| {
                reg.deinit();
                reg = new_reg;
                if (watch) fp = dirFingerprint(dir_path); // settle after the read
                std.debug.print("reloaded {s}\n", .{dir_path});
                banner(port, reg.routes);
            } else |e| std.debug.print("reload failed (keeping current routes): {s}\n", .{@errorName(e)});
        }
        const conn = net_server.accept() catch continue;
        handleConn(gpa, reg.routes, conn, &read_buf, threads) catch {};
        conn.stream.close();
    }
}

fn findRoute(routes: []const Route, req_path: []const u8) ?Route {
    for (routes) |r| {
        if (std.mem.eql(u8, r.path, req_path)) return r;
    }
    return null;
}

fn handleConn(gpa: std.mem.Allocator, routes: []const Route, conn: std.net.Server.Connection, read_buf: []u8, threads: usize) !void {
    var send_buf: [16 * 1024]u8 = undefined;
    var creader = conn.stream.reader(read_buf);
    var cwriter = conn.stream.writer(&send_buf);
    var http = std.http.Server.init(creader.interface(), &cwriter.interface);
    while (http.reader.state == .ready) {
        var req = http.receiveHead() catch return;

        const target = req.head.target;
        const q = std.mem.indexOfScalar(u8, target, '?');
        const req_path = if (q) |qi| target[0..qi] else target;
        const query = if (q) |qi| target[qi + 1 ..] else "";

        // Liveness/readiness for k8s probes & load balancers — always 200 while up.
        if (std.mem.eql(u8, req_path, "/healthz") or std.mem.eql(u8, req_path, "/readyz")) {
            try req.respond("{\"status\":\"ok\"}\n", .{ .status = .ok, .keep_alive = false, .extra_headers = &.{
                .{ .name = "content-type", .value = "application/json" },
            } });
            return;
        }

        const route = findRoute(routes, req_path) orelse {
            try req.respond("{\"status\":\"error\",\"error\":\"not found\"}\n", .{ .status = .not_found, .keep_alive = false });
            return;
        };

        var body_buf: [64 * 1024]u8 = undefined;
        const reader = try req.readerExpectContinue(&body_buf);
        const body = try reader.allocRemaining(gpa, .limited(64 * 1024 * 1024));
        defer gpa.free(body);

        var params = std.array_list.Managed(runtime.ParamArg).init(gpa);
        defer params.deinit();
        try parseQuery(&params, query);

        var diag: runtime.Diag = .{};
        var sink = runtime.OutcomeSink.init(gpa);
        defer sink.deinit();
        // Each request logs a completion line to stderr (NDJSON in a container); the
        // body below carries a status JSON. The HTTP status mirrors the CLI exit-code
        // contract: 200 ok · 207 partial · 503 transient (retry) · 500 permanent.
        const result = runtime.run(gpa, route.program, .{ .params = params.items, .request_body = body, .threads = threads, .outcomes = &sink, .log = .{ .summary = .stderr } }, &diag);

        var out = std.array_list.Managed(u8).init(gpa);
        defer out.deinit();
        const w = out.writer();
        var status: std.http.Status = .ok;
        var retry_after = false;

        if (result) |stats| {
            const nfail = sink.failures();
            if (nfail == 0) {
                try w.print("{{\"status\":\"ok\",\"rows\":{d}}}\n", .{stats.rows_out});
            } else {
                // 207: per-item breakdown so the caller can re-POST just the failed
                // (and retryable) items instead of the whole batch.
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
            status = if (transient) .service_unavailable else .internal_server_error;
            retry_after = transient;
            try w.print("{{\"status\":\"error\",\"retryable\":{},\"error\":\"", .{transient});
            try writeJsonStr(w, @errorName(err));
            try w.writeAll(": ");
            try writeJsonStr(w, diag.msg);
            try w.writeAll("\"}\n");
        }

        const ct = std.http.Header{ .name = "content-type", .value = "application/json" };
        if (retry_after) {
            try req.respond(out.items, .{ .status = status, .keep_alive = false, .extra_headers = &.{ ct, .{ .name = "retry-after", .value = "5" } } });
        } else {
            try req.respond(out.items, .{ .status = status, .keep_alive = false, .extra_headers = &.{ct} });
        }
        return;
    }
}

/// Write `s` JSON-string-escaped (no surrounding quotes) — error messages may
/// contain quotes/newlines that would otherwise break the response body.
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

/// Parse a `k=v&k2=v2` query string into params (no percent-decoding for now).
fn parseQuery(params: *std.array_list.Managed(runtime.ParamArg), query: []const u8) !void {
    var it = std.mem.splitScalar(u8, query, '&');
    while (it.next()) |pair| {
        if (pair.len == 0) continue;
        const eq = std.mem.indexOfScalar(u8, pair, '=') orelse continue;
        try params.append(.{ .key = pair[0..eq], .val = pair[eq + 1 ..] });
    }
}

fn attrToStr(e: *const ast.Expr) []const u8 {
    return switch (e.*) {
        .str_lit => |s| s,
        .field => |q| q.parts[q.parts.len - 1],
        else => "/",
    };
}
