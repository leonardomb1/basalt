//! The socket half every SQL driver (postgres, mysql, tds) shares: the stream and
//! its 64 KB buffers, the optional TLS session, the current message body, and the
//! server's last error. A driver embeds one `Conn` as `wire` and keeps only its
//! framing and auth. `Reader` is the bounds-checked cursor their message parsers
//! walk a received body with.
//!
//! The socket buffers matter: unbuffered, every backend message costs two recv
//! syscalls (header and body), about two per row, which caps throughput.
//!
//! TLS rides over a carrier: the raw socket for postgres and mysql, a shim that
//! wraps the handshake in PRELOGIN packets for tds. A flush goes TLS, then the
//! carrier, then the socket, so bytes buffered at any layer leave together.

const std = @import("std");
const sql = @import("sql.zig");
const driver = @import("../connect/driver.zig");

const SOCK_BUF = 64 * 1024;

pub const Conn = struct {
    gpa: std.mem.Allocator,
    stream: std.net.Stream,
    read_buf: [SOCK_BUF]u8 = undefined,
    write_buf: [SOCK_BUF]u8 = undefined,
    sr: std.net.Stream.Reader = undefined,
    sw: std.net.Stream.Writer = undefined,
    msg: std.array_list.Managed(u8),
    last_error: []const u8 = "",
    tls: ?*sql.TlsState = null,
    carrier: *std.Io.Writer = undefined,

    /// A heap `T` whose `wire` is connected to `host:port`; every other field of
    /// `T` takes its default. Built in place: the readers point into the buffers.
    pub fn dial(comptime T: type, gpa: std.mem.Allocator, host: []const u8, port: u16) !*T {
        const stream = try std.net.tcpConnectToHost(gpa, host, port);
        errdefer stream.close();
        driver.tuneSocket(stream.handle);
        const self = try gpa.create(T);
        self.* = .{ .wire = .{ .gpa = gpa, .stream = stream, .msg = .init(gpa) } };
        self.wire.sr = .init(stream, &self.wire.read_buf);
        self.wire.sw = .init(stream, &self.wire.write_buf);
        return self;
    }

    /// Frees what the wire holds and destroys `owner`, the struct it is embedded in.
    pub fn close(self: *Conn, owner: anytype) void {
        const gpa = self.gpa;
        if (self.last_error.len > 0) gpa.free(self.last_error);
        if (self.tls) |t| t.deinit(gpa);
        self.msg.deinit();
        self.stream.close();
        gpa.destroy(owner);
    }

    /// TLS straight over the socket.
    pub fn startTls(self: *Conn, host: []const u8, mode: sql.TlsMode) !void {
        return self.startTlsOver(host, mode, self.sr.interface(), &self.sw.interface);
    }

    /// TLS over a carrier that itself writes to the socket.
    pub fn startTlsOver(self: *Conn, host: []const u8, mode: sql.TlsMode, r: *std.Io.Reader, w: *std.Io.Writer) !void {
        const ts = try self.gpa.create(sql.TlsState);
        errdefer self.gpa.destroy(ts);
        try ts.start(self.gpa, r, w, host, mode);
        self.carrier = w;
        self.tls = ts;
    }

    pub fn rd(self: *Conn) *std.Io.Reader {
        return if (self.tls) |t| &t.client.reader else self.sr.interface();
    }

    pub fn wr(self: *Conn) *std.Io.Writer {
        return if (self.tls) |t| &t.client.writer else &self.sw.interface;
    }

    pub fn flush(self: *Conn) !void {
        if (self.tls) |t| {
            try t.client.writer.flush();
            try self.carrier.flush();
        }
        try self.sw.interface.flush();
    }

    /// Stores `owned` as `last_error`, freeing the message it replaces.
    pub fn putError(self: *Conn, owned: []const u8) void {
        if (self.last_error.len > 0) self.gpa.free(self.last_error);
        self.last_error = owned;
    }

    /// `putError` with a copy of `msg`.
    pub fn setError(self: *Conn, msg: []const u8) !void {
        self.putError(try self.gpa.dupe(u8, msg));
    }

    /// `putError` with a formatted message.
    pub fn printError(self: *Conn, comptime fmt: []const u8, args: anytype) !void {
        self.putError(try std.fmt.allocPrint(self.gpa, fmt, args));
    }
};

test "the last error frees the message it replaces" {
    var c: Conn = .{ .gpa = std.testing.allocator, .stream = undefined, .msg = .init(std.testing.allocator) };
    defer c.msg.deinit();
    try c.setError("first");
    try c.setError("second");
    try c.printError("third {d}", .{3});
    try std.testing.expectEqualStrings("third 3", c.last_error);
    c.putError("");
}
