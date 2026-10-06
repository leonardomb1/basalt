//! `basalt serve`: host every endpoint script in a directory.

const http_server = @import("../server/http_server.zig");
const nextVal = @import("args.zig").nextVal;
const obs = @import("../runtime/obs.zig");
const parseLogFormat = @import("args.zig").parseLogFormat;
const runtime = @import("../runtime/run.zig");
const std = @import("std");
const unknownOption = @import("args.zig").unknownOption;

pub fn cmdServe(alloc: std.mem.Allocator, args: [][:0]u8) !u8 {
    var err_buf: [4096]u8 = undefined;
    var err_file = std.fs.File.stderr().writer(&err_buf);
    const stderr = &err_file.interface;
    defer stderr.flush() catch {};

    if (args.len < 3 or (args[2].len > 0 and args[2][0] == '-')) {
        try stderr.print("error: `serve` requires a <dir> of endpoint scripts (`CREATE ENDPOINT`)\n", .{});
        return 2;
    }
    const dir = args[2];

    var port: u16 = 8080;
    var host: []const u8 = http_server.default_host;
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
        } else if (std.mem.eql(u8, args[i], "--host")) {
            host = (try nextVal(args, &i, "--host", stderr)) orelse return 2;
            _ = std.net.Address.parseIp(host, 0) catch {
                try stderr.print("error: invalid --host `{s}` (an IP address, such as 127.0.0.1)\n", .{host});
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

    http_server.serveDir(alloc, dir, host, port, watch, log) catch |e| {
        try stderr.print("serve error: {s}\n", .{@errorName(e)});
        return 1;
    };
    return 0;
}
