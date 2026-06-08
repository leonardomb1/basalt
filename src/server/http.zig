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

    var stderr_buf: [512]u8 = undefined;
    var stderr_file = std.fs.File.stderr().writer(&stderr_buf);
    const stderr = &stderr_file.interface;
    try stderr.print("pipeline serving @http on http://0.0.0.0:{d}{s}\n", .{ port, path });
    try stderr.flush();

    var read_buf: [64 * 1024]u8 = undefined;
    while (true) {
        const conn = net_server.accept() catch continue;
        handleConn(gpa, program, conn, path, &read_buf) catch {};
        conn.stream.close();
    }
}

fn handleConn(gpa: std.mem.Allocator, program: ast.Program, conn: std.net.Server.Connection, path: []const u8, read_buf: []u8) !void {
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
        // Each request logs a completion line to stderr (NDJSON in a container); the
        // HTTP response body is a small ad-hoc status JSON ({status, rows}) built below.
        const result = runtime.run(gpa, program, .{ .params = params.items, .request_body = body, .log = .{ .summary = .stderr } }, &diag);

        if (result) |stats| {
            const resp = try std.fmt.allocPrint(gpa, "{{\"status\":\"ok\",\"rows\":{d}}}\n", .{stats.rows_out});
            defer gpa.free(resp);
            try req.respond(resp, .{ .status = .ok, .keep_alive = false, .extra_headers = &.{
                .{ .name = "content-type", .value = "application/json" },
            } });
        } else |err| {
            const resp = try std.fmt.allocPrint(gpa, "{{\"status\":\"error\",\"error\":\"{s}: {s}\"}}\n", .{ @errorName(err), diag.msg });
            defer gpa.free(resp);
            try req.respond(resp, .{ .status = .internal_server_error, .keep_alive = false, .extra_headers = &.{
                .{ .name = "content-type", .value = "application/json" },
            } });
        }
        return;
    }
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
