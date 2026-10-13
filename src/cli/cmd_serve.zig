//! `basalt serve`: host every endpoint script in a directory.

const Flags = @import("args.zig").Flags;
const http_server = @import("../server/http_server.zig");
const sharedFlag = @import("args.zig").sharedFlag;
const runtime = @import("../runtime/run.zig");
const std = @import("std");
const unknownOption = @import("args.zig").unknownOption;

pub fn cmdServe(alloc: std.mem.Allocator, args: [][:0]u8) !u8 {
    var err_buf: [4096]u8 = undefined;
    var err_file = std.fs.File.stderr().writerStreaming(&err_buf);
    const stderr = &err_file.interface;
    defer stderr.flush() catch {};

    if (args.len < 3 or (args[2].len > 0 and args[2][0] == '-')) {
        try stderr.print("error: `serve` requires a <dir> of endpoint scripts (`CREATE ENDPOINT`)\n", .{});
        return 2;
    }
    const dir = args[2];

    var fl = Flags{ .log = .{ .level = .info, .summary = .stderr } };
    var watch = false;
    var i: usize = 3;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--watch") or std.mem.eql(u8, args[i], "-w")) {
            watch = true;
        } else if ((try sharedFlag(args, &i, &fl, .{ .listen = true, .p_port = true }, stderr)) orelse return 2) {} else if (try unknownOption(args[i], "serve", stderr)) return 2;
    }

    http_server.serveDir(alloc, dir, fl.host orelse http_server.default_host, fl.port orelse 8080, watch, fl.log) catch |e| {
        try stderr.print("serve error: {s}\n", .{@errorName(e)});
        return 1;
    };
    return 0;
}
