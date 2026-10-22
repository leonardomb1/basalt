//! Loopback HTTP/1.1 servers for tests: the scripted `FakeServer`, the ranged
//! `RangeServer`, and the listen, wake and request-reading steps hand-written test
//! servers share.

const std = @import("std");

/// A listener on 127.0.0.1 at a port the kernel picks.
pub fn listen(kernel_backlog: u31) !std.net.Server {
    const addr = try std.net.Address.parseIp("127.0.0.1", 0);
    return addr.listen(.{ .reuse_address = true, .kernel_backlog = kernel_backlog });
}

pub fn urlOf(arena: std.mem.Allocator, listener: *const std.net.Server, path: []const u8) ![]const u8 {
    return std.fmt.allocPrint(arena, "http://127.0.0.1:{d}{s}", .{ listener.listen_address.getPort(), path });
}

/// Wakes a server thread blocked in `accept` with a connection of its own, so a test
/// whose client never connected stops instead of hanging at the join.
pub fn wake(listener: *const std.net.Server) void {
    if (std.net.tcpConnectToAddress(listener.listen_address)) |c| c.close() else |_| {}
}

/// Reads into `head` until the blank line that ends a request's headers, `n` the
/// bytes read so far (some of the body, perhaps); returns where the headers end.
pub fn readHead(stream: std.net.Stream, head: []u8, n: *usize) !usize {
    while (true) {
        if (std.mem.indexOf(u8, head[0..n.*], "\r\n\r\n")) |at| return at + 4;
        if (n.* == head.len) return error.HeadTooLarge;
        const got = try stream.read(head[n.*..]);
        if (got == 0) return error.EndOfStream;
        n.* += got;
    }
}

/// The `Content-Length` a request head declares, 0 when it has none.
pub fn contentLength(head: []const u8) usize {
    var it = std.mem.splitSequence(u8, head, "\r\n");
    while (it.next()) |h| {
        const colon = std.mem.indexOfScalar(u8, h, ':') orelse continue;
        if (!std.ascii.eqlIgnoreCase(h[0..colon], "content-length")) continue;
        return std.fmt.parseInt(usize, std.mem.trim(u8, h[colon + 1 ..], " "), 10) catch 0;
    }
    return 0;
}

/// A scripted HTTP/1.1 server for request-path tests: each connection gets the next
/// reply, `null` resetting it unanswered; past the script every request gets a 418.
/// Request lines are recorded so a test can check what was replayed.
pub const FakeServer = struct {
    listener: std.net.Server,
    replies: []const ?[]const u8,
    thread: std.Thread = undefined,
    stopping: std.atomic.Value(bool) = .init(false),
    count: std.atomic.Value(usize) = .init(0),
    lines: [16][128]u8 = undefined,
    lens: [16]usize = .{0} ** 16,

    pub fn start(self: *FakeServer, replies: []const ?[]const u8) !void {
        self.* = .{ .listener = try listen(128), .replies = replies };
        self.thread = try std.Thread.spawn(.{}, serve, .{self});
    }

    pub fn url(self: *FakeServer, arena: std.mem.Allocator, path: []const u8) ![]const u8 {
        return urlOf(arena, &self.listener, path);
    }

    /// A whole response: `status` like "503 Slow Down", `extra` header lines each
    /// ending in CRLF.
    pub fn reply(comptime status: []const u8, comptime extra: []const u8, comptime body: []const u8) []const u8 {
        return std.fmt.comptimePrint("HTTP/1.1 {s}\r\n{s}content-length: {d}\r\nconnection: close\r\n\r\n{s}", .{ status, extra, body.len, body });
    }

    pub fn requests(self: *FakeServer) usize {
        return self.count.load(.seq_cst);
    }

    pub fn line(self: *FakeServer, i: usize) []const u8 {
        return self.lines[i][0..self.lens[i]];
    }

    pub fn stop(self: *FakeServer) void {
        self.stopping.store(true, .seq_cst);
        wake(&self.listener);
        self.thread.join();
        self.listener.deinit();
    }

    fn serve(self: *FakeServer) void {
        var i: usize = 0;
        while (true) : (i += 1) {
            const conn = self.listener.accept() catch return;
            defer conn.stream.close();
            if (self.stopping.load(.seq_cst)) return;
            self.record(conn.stream) catch continue;
            const next: ?[]const u8 = if (i < self.replies.len) self.replies[i] else reply("418 Unscripted", "", "");
            if (next) |text| {
                conn.stream.writeAll(text) catch {};
            } else {
                const linger = extern struct { onoff: c_int, secs: c_int }{ .onoff = 1, .secs = 0 };
                std.posix.setsockopt(conn.stream.handle, std.posix.SOL.SOCKET, std.posix.SO.LINGER, std.mem.asBytes(&linger)) catch {};
            }
        }
    }

    fn record(self: *FakeServer, stream: std.net.Stream) !void {
        var head: [8192]u8 = undefined;
        var n: usize = 0;
        const end = try readHead(stream, &head, &n);
        var left = contentLength(head[0..end]) -| (n - end);
        var sink: [4096]u8 = undefined;
        while (left > 0) {
            const got = try stream.read(sink[0..@min(sink.len, left)]);
            if (got == 0) return error.EndOfStream;
            left -= got;
        }
        const first = head[0 .. std.mem.indexOf(u8, head[0..end], "\r\n") orelse 0];
        const k = self.count.fetchAdd(1, .seq_cst);
        if (k < self.lines.len) {
            const len = @min(first.len, self.lines[k].len);
            @memcpy(self.lines[k][0..len], first[0..len]);
            self.lens[k] = len;
        }
    }
};

/// An HTTP/1.1 server of one file's bytes for range-read tests: HEAD answers its
/// length, a GET with `Range: bytes=a-b` its slice as a 206, or the whole file as a
/// 200 when `ignore_range` is set, as some servers do. One request per connection,
/// served in turn. Counts the HEADs answered and the 206 and 200 bodies sent.
pub const RangeServer = struct {
    listener: std.net.Server,
    data: []const u8,
    ignore_range: bool,
    thread: std.Thread = undefined,
    stopping: std.atomic.Value(bool) = .init(false),
    heads: std.atomic.Value(usize) = .init(0),
    partial: std.atomic.Value(usize) = .init(0),
    whole: std.atomic.Value(usize) = .init(0),

    pub fn start(self: *RangeServer, data: []const u8, ignore_range: bool) !void {
        self.* = .{ .listener = try listen(64), .data = data, .ignore_range = ignore_range };
        self.thread = try std.Thread.spawn(.{}, serve, .{self});
    }

    pub fn url(self: *RangeServer, arena: std.mem.Allocator, path: []const u8) ![]const u8 {
        return urlOf(arena, &self.listener, path);
    }

    pub fn stop(self: *RangeServer) void {
        self.stopping.store(true, .seq_cst);
        wake(&self.listener);
        self.thread.join();
        self.listener.deinit();
    }

    fn serve(self: *RangeServer) void {
        while (true) {
            const conn = self.listener.accept() catch return;
            defer conn.stream.close();
            if (self.stopping.load(.seq_cst)) return;
            self.answer(conn.stream) catch {};
        }
    }

    fn answer(self: *RangeServer, stream: std.net.Stream) !void {
        var head: [8192]u8 = undefined;
        var n: usize = 0;
        const end = try readHead(stream, &head, &n);
        var hdr: [256]u8 = undefined;
        const data = self.data;
        if (std.mem.startsWith(u8, head[0..end], "HEAD ")) {
            _ = self.heads.fetchAdd(1, .seq_cst);
            const text = try std.fmt.bufPrint(&hdr, "HTTP/1.1 200 OK\r\ncontent-length: {d}\r\naccept-ranges: bytes\r\nconnection: close\r\n\r\n", .{data.len});
            return stream.writeAll(text);
        }
        if (!self.ignore_range) if (rangeOf(head[0..end])) |r| if (r[0] <= r[1] and r[1] < data.len) {
            _ = self.partial.fetchAdd(1, .seq_cst);
            const text = try std.fmt.bufPrint(&hdr, "HTTP/1.1 206 Partial Content\r\ncontent-range: bytes {d}-{d}/{d}\r\ncontent-length: {d}\r\nconnection: close\r\n\r\n", .{ r[0], r[1], data.len, r[1] - r[0] + 1 });
            try stream.writeAll(text);
            return stream.writeAll(data[r[0] .. r[1] + 1]);
        };
        _ = self.whole.fetchAdd(1, .seq_cst);
        const text = try std.fmt.bufPrint(&hdr, "HTTP/1.1 200 OK\r\ncontent-length: {d}\r\nconnection: close\r\n\r\n", .{data.len});
        try stream.writeAll(text);
        return stream.writeAll(data);
    }

    fn rangeOf(head: []const u8) ?[2]usize {
        var it = std.mem.splitSequence(u8, head, "\r\n");
        while (it.next()) |h| {
            const colon = std.mem.indexOfScalar(u8, h, ':') orelse continue;
            if (!std.ascii.eqlIgnoreCase(h[0..colon], "range")) continue;
            const v = std.mem.trim(u8, h[colon + 1 ..], " ");
            if (!std.mem.startsWith(u8, v, "bytes=")) return null;
            const dash = std.mem.indexOfScalar(u8, v, '-') orelse return null;
            const lo = std.fmt.parseInt(usize, v[6..dash], 10) catch return null;
            const hi = std.fmt.parseInt(usize, v[dash + 1 ..], 10) catch return null;
            return .{ lo, hi };
        }
        return null;
    }
};
