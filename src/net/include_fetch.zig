//! The HTTPS GET remote `@include`s are fetched with (`lang/remote.zig`): one
//! request, redirects followed, its body capped at an include's size limit.

const std = @import("std");
const http_client = @import("http_client.zig");
const objstore = @import("../store/objstore.zig");
const remote = @import("../lang/remote.zig");

const max_bytes = 8 << 20;

var anchor: u8 = 0;

pub fn fetch() remote.Fetch {
    return .{ .ctx = &anchor, .get = get };
}

fn get(_: *anyopaque, arena: std.mem.Allocator, url: []const u8, header: ?std.http.Header) anyerror![]const u8 {
    var client = http_client.initClient(arena);
    defer client.deinit();
    const one = [1]std.http.Header{header orelse .{ .name = "", .value = "" }};
    const r = objstore.exchange(arena, &client, .{ .method = .GET, .url = url, .headers = if (header != null) &one else &.{}, .max_body = max_bytes }) catch |e| switch (e) {
        error.HttpBodyTooLarge => return error.IncludeTooLarge,
        else => |x| return x,
    };
    if (r.code != 200) return http_client.statusError(r.code);
    return r.body;
}
