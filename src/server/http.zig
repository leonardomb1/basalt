//! HTTP trigger host for `@http` scripts. Each matching request runs the pipeline
//! once: query-string values bind params, the request body feeds `read request`,
//! and the response is a JSON run summary. Single-threaded accept loop (one
//! request at a time) — a worker pool / bounded queue is the scaling step.

const std = @import("std");
const ast = @import("../lang/ast.zig");
const runtime = @import("../runtime/run.zig");

pub fn serve(gpa: std.mem.Allocator, program: ast.Program, port: u16) !void {
    const kind = program.stmts[0].kind;
    var path: []const u8 = "/";
    for (kind.config) |attr| {
        if (std.mem.eql(u8, attr.key, "path")) path = attrToStr(attr.value);
    }

    const address = try std.net.Address.parseIp("0.0.0.0", port);
    var net_server = try address.listen(.{ .reuse_address = true });
    defer net_server.deinit();

    // A 1s accept timeout makes accept() return periodically (EWOULDBLOCK) so the
    // loop can re-check the shutdown flag even with no traffic — SIGTERM then exits
    // within ~1s instead of blocking until the next connection or SIGKILL.
    const tv = std.posix.timeval{ .sec = 1, .usec = 0 };
    std.posix.setsockopt(net_server.stream.handle, std.posix.SOL.SOCKET, std.posix.SO.RCVTIMEO, std.mem.asBytes(&tv)) catch {};

    var stderr_buf: [512]u8 = undefined;
    var stderr_file = std.fs.File.stderr().writer(&stderr_buf);
    const stderr = &stderr_file.interface;
    try stderr.print("pipeline serving @http on http://0.0.0.0:{d}{s}\n", .{ port, path });
    try stderr.flush();

    // Worker count for parallel `for`-each fan-out (e.g. many tables per request);
    // matches the CLI's default. Detected once, reused for every request.
    const threads = std.Thread.getCpuCount() catch 1;

    var read_buf: [64 * 1024]u8 = undefined;
    // Stop accepting once the control plane signals shutdown (SIGTERM/SIGINT). An
    // idle server blocked in accept() exits on the next connection or SIGKILL; an
    // in-flight request finishes (or aborts at its next batch boundary) first.
    while (!runtime.aborting()) {
        const conn = net_server.accept() catch continue;
        handleConn(gpa, program, conn, path, &read_buf, threads) catch {};
        conn.stream.close();
    }
}

fn handleConn(gpa: std.mem.Allocator, program: ast.Program, conn: std.net.Server.Connection, path: []const u8, read_buf: []u8, threads: usize) !void {
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

        if (!std.mem.eql(u8, req_path, path)) {
            try req.respond("{\"status\":\"error\",\"error\":\"not found\"}", .{ .status = .not_found, .keep_alive = false });
            return;
        }

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
        const result = runtime.run(gpa, program, .{ .params = params.items, .request_body = body, .threads = threads, .outcomes = &sink, .log = .{ .summary = .stderr } }, &diag);

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
