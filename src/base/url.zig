//! A URL as logs and messages show it, its credentials hidden: the user and password
//! dropped and each query value replaced by `...`. std only, so the tracer below
//! every store and the stores themselves share it.

const std = @import("std");

/// `url` as logs and messages show it: without a `user:pass@` and with each query
/// value replaced by `...`, as either may hold a credential. `(url)` when it does
/// not fit `buf`.
pub fn shown(buf: []u8, url: []const u8) []const u8 {
    var w = std.Io.Writer.fixed(buf);
    writeShown(&w, url) catch return "(url)";
    return w.buffered();
}

pub fn writeShown(w: *std.Io.Writer, url: []const u8) std.Io.Writer.Error!void {
    var tail = url;
    if (std.mem.indexOf(u8, tail, "://")) |at| {
        try w.writeAll(tail[0 .. at + 3]);
        tail = tail[at + 3 ..];
        const host_end = std.mem.indexOfAny(u8, tail, "/?#") orelse tail.len;
        if (std.mem.lastIndexOfScalar(u8, tail[0..host_end], '@')) |amp| tail = tail[amp + 1 ..];
    }
    const q = std.mem.indexOfScalar(u8, tail, '?') orelse return w.writeAll(tail);
    try w.writeAll(tail[0 .. q + 1]);
    var it = std.mem.splitScalar(u8, tail[q + 1 ..], '&');
    var first = true;
    while (it.next()) |kv| {
        if (!first) try w.writeByte('&');
        first = false;
        const eq = std.mem.indexOfScalar(u8, kv, '=') orelse kv.len;
        try w.writeAll(kv[0..eq]);
        if (eq < kv.len) try w.writeAll("=...");
    }
}

/// `url` through `shown` for a `{f}` placeholder, with no buffer to size.
pub fn fmtShown(url: []const u8) std.fmt.Alt([]const u8, formatShown) {
    return .{ .data = url };
}

fn formatShown(url: []const u8, w: *std.Io.Writer) std.Io.Writer.Error!void {
    return writeShown(w, url);
}

const testing = std.testing;

test "shown drops userinfo and every query value" {
    var buf: [256]u8 = undefined;
    const cases = [_]struct { []const u8, []const u8 }{
        .{ "https://user:secret@api.example.com/v1/x?api_key=abc&page=2", "https://api.example.com/v1/x?api_key=...&page=..." },
        .{ "http://h/p", "http://h/p" },
        .{ "http://h/p?flag", "http://h/p?flag" },
        .{ "http://h/p?", "http://h/p?" },
        .{ "http://h/p?a=&b", "http://h/p?a=...&b" },
        .{ "http://h/p?a=1&&b=2", "http://h/p?a=...&&b=..." },
        .{ "sftp://u:pw@h/x", "sftp://h/x" },
        .{ "SFTP://u:p@w@h:22/x", "SFTP://h:22/x" },
        .{ "smb://d;u:pw@h/s/x", "smb://h/s/x" },
        .{ "s3://b/k@v", "s3://b/k@v" },
        .{ "http://h?x=@y", "http://h?x=..." },
        .{ "http://u@h#f@g", "http://h#f@g" },
        .{ "http://u:pw@h", "http://h" },
        .{ "/local/a@b?c=d", "/local/a@b?c=..." },
        .{ "", "" },
    };
    for (cases) |c| try testing.expectEqualStrings(c[1], shown(&buf, c[0]));
    var small: [8]u8 = undefined;
    try testing.expectEqualStrings("(url)", shown(&small, "https://h/a/long/path"));
    try testing.expectEqualStrings("http://h", shown(&small, "http://h"));
    var empty: [0]u8 = undefined;
    try testing.expectEqualStrings("", shown(&empty, ""));
    try testing.expectEqualStrings("(url)", shown(&empty, "x"));
}

test "shown equals http_client's old redactUrl byte for byte, and fmtShown equals shown" {
    const http_client = @import("../net/http_client.zig");
    const msgfmt = @import("msgfmt.zig");
    const Orig = struct {
        /// http_client's `redactUrl` before it became `shown`, the oracle `shown` must match.
        fn oldRedactUrl(buf: []u8, url: []const u8) []const u8 {
            var w = std.Io.Writer.fixed(buf);
            var rest = url;
            if (std.mem.indexOf(u8, rest, "://")) |at| {
                w.writeAll(rest[0 .. at + 3]) catch return "(url)";
                rest = rest[at + 3 ..];
                const host_end = std.mem.indexOfAny(u8, rest, "/?#") orelse rest.len;
                if (std.mem.lastIndexOfScalar(u8, rest[0..host_end], '@')) |amp| rest = rest[amp + 1 ..];
            }
            const q = std.mem.indexOfScalar(u8, rest, '?') orelse {
                w.writeAll(rest) catch return "(url)";
                return w.buffered();
            };
            w.writeAll(rest[0 .. q + 1]) catch return "(url)";
            var it = std.mem.splitScalar(u8, rest[q + 1 ..], '&');
            var first = true;
            while (it.next()) |kv| {
                if (!first) w.writeByte('&') catch return "(url)";
                first = false;
                const eq = std.mem.indexOfScalar(u8, kv, '=') orelse kv.len;
                w.writeAll(kv[0..eq]) catch return "(url)";
                if (eq < kv.len) w.writeAll("=...") catch return "(url)";
            }
            return w.buffered();
        }
    };
    const urls = [_][]const u8{
        "https://user:secret@api.example.com/v1/x?api_key=abc&page=2",
        "http://h/p",
        "http://h/p?flag",
        "http://h/p?a=&b",
        "sftp://u:pw@h/x",
        "http://u@h#f@g",
        "http://h?x=@y",
        "/local/a@b?c=d",
        "",
        "https://h/a/long/path/that/does/not/fit/the/buffer/at/all",
    };
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    for (urls) |u| for ([_]usize{ 0, 8, 16, 32, 256 }) |cap| {
        const b1 = try ar.allocator().alloc(u8, cap);
        const b2 = try ar.allocator().alloc(u8, cap);
        try testing.expectEqualStrings(Orig.oldRedactUrl(b1, u), shown(b2, u));
        try testing.expectEqualStrings(Orig.oldRedactUrl(b1, u), http_client.redactUrl(b2, u));
    };
    for (urls) |u| {
        var buf: [256]u8 = undefined;
        const s = try msgfmt.allocPrint(ar.allocator(), "{f}", .{fmtShown(u)});
        try testing.expectEqualStrings(shown(&buf, u), s);
    }
}
