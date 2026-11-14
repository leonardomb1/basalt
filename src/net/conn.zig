//! Connecting to a server and explaining why it failed, shared by every connector:
//! `dial` and the socket tuning and deadlines it applies, the login timeout and
//! failure helpers, the thread-local `LoginNote` holding the server's own words,
//! and `classify`, which sorts an error into the transient sets the connector and
//! run retry loops use. The engine's side of a connector, `Source` and `Sink`,
//! lives in `exec/driver.zig`.

const std = @import("std");
const msgfmt = @import("../base/msgfmt.zig");

/// Words held in `n` bytes and cut to fit; `set` takes text that may lie in the
/// note itself.
pub fn ThreadNote(comptime n: usize) type {
    return struct {
        buf: [n]u8 = undefined,
        len: usize = 0,

        pub fn get(self: *const @This()) []const u8 {
            return self.buf[0..self.len];
        }

        pub fn clear(self: *@This()) void {
            self.len = 0;
        }

        pub fn set(self: *@This(), text: []const u8) void {
            self.len = @min(text.len, n);
            std.mem.copyForwards(u8, self.buf[0..self.len], text[0..self.len]);
        }

        pub fn print(self: *@This(), comptime fmt: []const u8, args: anytype) void {
            @branchHint(.cold);
            self.len = (msgfmt.bufPrint(&self.buf, fmt, args) catch self.buf[0..]).len;
        }
    };
}

/// TCP_NODELAY, since drivers flush whole messages then wait (Nagle + delayed ACK
/// stalls each exchange); SO_KEEPALIVE so idle multi-hour loads behind NATs surface
/// a dead peer as an error, not a hang. On Linux the probes start after 60 s idle,
/// every 15 s, 56 of them, and TCP_USER_TIMEOUT drops a peer that leaves sent data
/// unacknowledged as long (it also overrides the probe count): idle or writing, a
/// dead peer fails the call within `dead_peer_s`. A live peer that answers is never
/// dropped, but one that stops reading (a zero window) while we write is, after the
/// same time, long enough for a server stalled on locks or log growth. Best-effort: never fails a connect.
pub fn tuneSocket(handle: std.posix.socket_t) void {
    const one: c_int = 1;
    std.posix.setsockopt(handle, std.posix.IPPROTO.TCP, std.posix.TCP.NODELAY, std.mem.asBytes(&one)) catch {};
    std.posix.setsockopt(handle, std.posix.SOL.SOCKET, std.posix.SO.KEEPALIVE, std.mem.asBytes(&one)) catch {};
    if (@import("builtin").os.tag == .linux) {
        const TCP = std.os.linux.TCP;
        const opts = [_]struct { u32, c_int }{
            .{ TCP.KEEPIDLE, keep_idle_s },
            .{ TCP.KEEPINTVL, keep_intvl_s },
            .{ TCP.KEEPCNT, keep_cnt },
            .{ TCP.USER_TIMEOUT, dead_peer_s * 1000 },
        };
        for (opts) |o| std.posix.setsockopt(handle, std.posix.IPPROTO.TCP, o[0], std.mem.asBytes(&o[1])) catch {};
    }
}

pub const keep_idle_s: c_int = 60;
pub const keep_intvl_s: c_int = 15;
pub const keep_cnt: c_int = 56;
pub const dead_peer_s: c_int = keep_idle_s + keep_intvl_s * keep_cnt;

/// How long one request may take, its retries included, unless its read says
/// otherwise: an HTTP page or login, an identity provider's token call, a KDC exchange.
pub const request_timeout_ms: i64 = 300_000;

/// How `dial` sets a socket up. `tune = false` keeps the OS defaults, for a one-shot
/// exchange that neither idles nor streams (a KDC request, a TLS chain probe);
/// `io_timeout_s` adds read and write timeouts so a silent server fails the call,
/// and bounds the TCP connect itself.
pub const Dial = struct {
    tune: bool = true,
    io_timeout_s: ?u32 = null,
};

/// A TCP connection to `host:port`, tuned by `tuneSocket` and set up as `opts` asks:
/// the one way a connector opens a socket. Each address the name resolves to is
/// tried in turn while it refuses, as `std.net.tcpConnectToHost` does. A connect
/// that outlasts `io_timeout_s` fails as `loginTimedOut` in phase `connect`: the
/// send timeout is set before `connect`, which Linux honours (EINPROGRESS); macOS
/// ignores it there and gives up after its own SYN retries, about 75 s. Clears this
/// thread's `LoginNote`.
pub fn dial(gpa: std.mem.Allocator, host: []const u8, port: u16, opts: Dial) !std.net.Stream {
    clearNote();
    const list = try std.net.getAddressList(gpa, host, port);
    defer list.deinit();
    if (list.addrs.len == 0) return error.UnknownHostName;
    for (list.addrs) |addr| {
        return dialAddress(addr, opts) catch |e| switch (e) {
            error.ConnectionRefused => continue,
            error.WouldBlock => loginTimedOut(host, port, "connect", opts.io_timeout_s orelse 0),
            else => e,
        };
    }
    return error.ConnectionRefused;
}

fn dialAddress(addr: std.net.Address, opts: Dial) !std.net.Stream {
    const posix = std.posix;
    const sock = try posix.socket(addr.any.family, posix.SOCK.STREAM | posix.SOCK.CLOEXEC, posix.IPPROTO.TCP);
    errdefer posix.close(sock);
    if (opts.io_timeout_s) |s| setIoTimeout(sock, s);
    try posix.connect(sock, &addr.any, addr.getOsSockLen());
    if (opts.tune) tuneSocket(sock);
    return .{ .handle = sock };
}

/// Read and write timeouts of `s` seconds on a socket, 0 clearing them: a recv or
/// send blocked that long fails with `WouldBlock`. Best-effort.
pub fn setIoTimeout(handle: std.posix.socket_t, s: u32) void {
    const tv = std.posix.timeval{ .sec = s, .usec = 0 };
    std.posix.setsockopt(handle, std.posix.SOL.SOCKET, std.posix.SO.RCVTIMEO, std.mem.asBytes(&tv)) catch {};
    std.posix.setsockopt(handle, std.posix.SOL.SOCKET, std.posix.SO.SNDTIMEO, std.mem.asBytes(&tv)) catch {};
}

/// The `connect_timeout` a database, SMB or SSH connection gets unless it says otherwise.
pub const default_connect_timeout_s: u32 = 60;

/// A connection's two deadlines in seconds, 0 for none. `connect_s` bounds every
/// read and write from the dial until the session is up (TLS, authentication,
/// session setup); `query_s` bounds each one after that, so by default a live
/// query is never timed out.
pub const Timeouts = struct {
    connect_s: u32 = default_connect_timeout_s,
    query_s: u32 = 0,

    /// The `Dial` that opens the login phase.
    pub fn dialOpts(self: Timeouts) Dial {
        return .{ .io_timeout_s = self.connect_s };
    }

    /// The session is up: the login deadline gives way to `query_s`.
    pub fn sessionUp(self: Timeouts, handle: std.posix.socket_t) void {
        setIoTimeout(handle, self.query_s);
    }
};

/// Whether a socket reader or writer failed because a `setIoTimeout` deadline fired.
pub fn ioTimedOut(sr: *const std.net.Stream.Reader, sw: *const std.net.Stream.Writer) bool {
    if (sr.getError()) |e| if (e == error.WouldBlock) return true;
    if (sw.err) |e| if (e == error.WouldBlock) return true;
    return false;
}

/// Why the last connector call on this thread failed, in the server's or basalt's
/// words, and the error those words explain. Every connector shares the one note,
/// thread-local as lanes connect in parallel: the next failure overwrites it, a
/// `dial` clears it, and a reader copies it before asking anything else.
pub const LoginNote = struct {
    err: ?anyerror = null,
    text: ThreadNote(700) = .{},
};

threadlocal var login_note: LoginNote = .{};

/// Keeps `text` as this thread's words for `e`; `text` may be the note's own.
pub fn explain(e: anyerror, text: []const u8) void {
    login_note.err = e;
    login_note.text.set(text);
}

/// `explain` with the words formatted, `args` free to quote the note itself.
pub fn explainf(e: anyerror, comptime fmt: []const u8, args: anytype) void {
    @branchHint(.cold);
    var words: ThreadNote(700) = .{};
    words.print(fmt, args);
    explain(e, words.get());
}

/// This thread's note, whatever error it explains, or "": for a reader whose error
/// may have been renamed on its way up.
pub fn lastNote() []const u8 {
    return login_note.text.get();
}

/// This thread's note when it explains `e`, else "".
pub fn noteFor(e: anyerror) []const u8 {
    const err = login_note.err orelse return "";
    return if (err == e) login_note.text.get() else "";
}

pub fn clearNote() void {
    login_note = .{};
}

/// `ConnectionTimedOut` for a connection to `host:port` whose `phase` (`login`, `TLS
/// handshake`, …) went `s` seconds without an answer; the `LoginNote` keeps the words.
pub fn loginTimedOut(host: []const u8, port: u16, phase: []const u8, s: u32) error{ConnectionTimedOut} {
    explainf(error.ConnectionTimedOut, "{s}:{d}: {s} did not answer within {d} s", .{ host, port, phase, s });
    return error.ConnectionTimedOut;
}

/// `e`, or the login timeout it stands for when `timed_out`.
pub fn loginFailed(e: anytype, timed_out: bool, host: []const u8, port: u16, phase: []const u8, s: u32) (@TypeOf(e) || error{ConnectionTimedOut}) {
    if (timed_out) return loginTimedOut(host, port, phase, s);
    return e;
}

/// What kind of failure an error is, for the two retry levels. `net`: the socket
/// failed, transient everywhere. `wire`: an ambient std.Io error a connector raised
/// mid-exchange, transient only at the connector (a disk-full CSV write is not).
/// `lookup`: DNS does not know the host yet, transient for the run only. `busy`: a
/// server said to come back later, transient for the run (a connector's own loop
/// retries by status). `other`: never transient.
pub const Class = enum { net, wire, lookup, busy, other };

pub fn classify(e: anyerror) Class {
    return switch (e) {
        error.ConnectionRefused,
        error.ConnectionTimedOut,
        error.ConnectionResetByPeer,
        error.NetworkUnreachable,
        error.HostUnreachable,
        error.BrokenPipe,
        error.TemporaryNameServerFailure,
        error.ServerClosedConnection,
        error.ConnectionIoFailed,
        error.SmbDisconnected,
        error.SshConnectionLost,
        => .net,
        error.EndOfStream,
        error.ReadFailed,
        error.WriteFailed,
        error.UnexpectedConnectFailure,
        => .wire,
        error.NameServerFailure,
        error.HostLacksNetworkAddresses,
        error.UnknownHostName,
        => .lookup,
        error.HttpServerBusy,
        error.FtpServerBusy,
        error.HttpTransportFailed,
        error.S3Throttled,
        error.AzureThrottled,
        => .busy,
        else => .other,
    };
}

/// The socket-level transient set for connector retry layers.
pub fn transientNet(e: anyerror) bool {
    return switch (classify(e)) {
        .net, .wire => true,
        .lookup, .busy, .other => false,
    };
}

/// The run-level transient set, behind exit code 75: an unknown host counts, since
/// DNS may not know a host that is still coming up.
pub fn transientRun(e: anyerror) bool {
    return switch (classify(e)) {
        .net, .lookup, .busy => true,
        .wire, .other => false,
    };
}

/// The HTTP statuses every connector retries, and that exit 75 once retries run out:
/// 408, 429 and every 5xx except those that describe the request or the server's
/// abilities rather than a passing condition (501, 505, 511). DuckDB's rule less its
/// 418; the AWS SDK (StarRocks) and ClickHouse agree on 408, 429 and 501.
pub fn transientStatus(code: u16) bool {
    return switch (code) {
        408, 429 => true,
        501, 505, 511 => false,
        else => code >= 500 and code < 600,
    };
}

/// A connector failure as `{layer}: {Err}: {note}`, the note being the server's own
/// words; the note part is left out when there is none, the layer part when `layer`
/// is empty.
pub fn connectorNote(w: *std.Io.Writer, layer: []const u8, e: anyerror, note: []const u8) std.Io.Writer.Error!void {
    if (layer.len > 0) try w.print("{s}: ", .{layer});
    try w.writeAll(@errorName(e));
    if (note.len > 0) try w.print(": {s}", .{note});
}

pub fn connectorNoteAlloc(arena: std.mem.Allocator, layer: []const u8, e: anyerror, note: []const u8) ![]const u8 {
    var out: std.Io.Writer.Allocating = .init(arena);
    connectorNote(&out.writer, layer, e, note) catch return error.OutOfMemory;
    return out.toOwnedSlice();
}

const testing = std.testing;

const error_corpus = [_]anyerror{
    error.ConnectionRefused,  error.ConnectionTimedOut,       error.ConnectionResetByPeer,      error.NetworkUnreachable,
    error.HostUnreachable,    error.BrokenPipe,               error.EndOfStream,                error.ReadFailed,
    error.WriteFailed,        error.UnexpectedConnectFailure, error.TemporaryNameServerFailure, error.ServerClosedConnection,
    error.ConnectionIoFailed, error.NameServerFailure,        error.HostLacksNetworkAddresses,  error.UnknownHostName,
    error.HttpServerBusy,     error.FtpServerBusy,            error.HttpTransportFailed,        error.S3Throttled,
    error.AzureThrottled,     error.HttpConnectionClosing,    error.WouldBlock,                 error.OutOfMemory,
    error.PlanFailed,         error.S3RequestFailed,          error.AzureRequestFailed,         error.HttpRequestFailed,
    error.LoginFailed,        error.Aborted,                  error.AccessDenied,               error.FileNotFound,
};

test "classify: transientNet keeps its socket set and transientRun matches the run's isTransient" {
    const Orig = struct {
        fn oldTransientNet(e: anyerror) bool {
            return switch (e) {
                error.ConnectionRefused,
                error.ConnectionTimedOut,
                error.ConnectionResetByPeer,
                error.NetworkUnreachable,
                error.HostUnreachable,
                error.BrokenPipe,
                error.EndOfStream,
                error.ReadFailed,
                error.WriteFailed,
                error.UnexpectedConnectFailure,
                error.TemporaryNameServerFailure,
                error.ServerClosedConnection,
                error.ConnectionIoFailed,
                => true,
                else => false,
            };
        }
        fn oldIsTransient(e: anyerror) bool {
            return switch (e) {
                error.ConnectionRefused,
                error.ConnectionTimedOut,
                error.ConnectionResetByPeer,
                error.NetworkUnreachable,
                error.HostUnreachable,
                error.BrokenPipe,
                error.TemporaryNameServerFailure,
                error.NameServerFailure,
                error.HostLacksNetworkAddresses,
                error.UnknownHostName,
                error.HttpServerBusy,
                error.FtpServerBusy,
                error.HttpTransportFailed,
                error.ServerClosedConnection,
                error.ConnectionIoFailed,
                error.S3Throttled,
                error.AzureThrottled,
                => true,
                else => false,
            };
        }
    };
    const env = @import("../runtime/env.zig");
    for (error_corpus) |e| {
        try testing.expectEqual(Orig.oldTransientNet(e), transientNet(e));
        try testing.expectEqual(Orig.oldIsTransient(e), transientRun(e));
        try testing.expectEqual(env.isTransient(e), transientRun(e));
    }
    try testing.expectEqual(Class.wire, classify(error.EndOfStream));
    try testing.expectEqual(Class.lookup, classify(error.UnknownHostName));
    try testing.expectEqual(Class.busy, classify(error.S3Throttled));
    try testing.expectEqual(Class.other, classify(error.HttpConnectionClosing));
}

test "transientStatus retries 408, 429 and every 5xx but 501, 505 and 511, alike on every HTTP path and in the exit code" {
    const http_client = @import("../net/http_client.zig");
    const objstore = @import("../store/objstore.zig");
    const env = @import("../runtime/env.zig");
    var code: u16 = 0;
    while (code < 1000) : (code += 1) {
        const want = code == 408 or code == 429 or (code >= 500 and code < 600 and code != 501 and code != 505 and code != 511);
        try testing.expectEqual(want, transientStatus(code));
        try testing.expectEqual(want, objstore.retriable(code));
        try testing.expectEqual(want, http_client.statusError(code) == error.HttpServerBusy);
        try testing.expectEqual(want, env.isTransient(http_client.statusError(code)));
        try testing.expectEqual(want, transientRun(http_client.statusError(code)));
    }
    for ([_]u16{ 408, 429, 500, 502, 503, 504, 507, 509, 598, 599 }) |c| try testing.expect(transientStatus(c));
    for ([_]u16{ 400, 404, 418, 501, 505, 511, 600, 999 }) |c| try testing.expect(!transientStatus(c));
}

test "connectorNote: layer, error and the server's words, each part left out when empty" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try testing.expectEqualStrings("sftp: ConnectionResetByPeer: kex failed", try connectorNoteAlloc(a, "sftp", error.ConnectionResetByPeer, "kex failed"));
    try testing.expectEqualStrings("smb: SmbFailed", try connectorNoteAlloc(a, "smb", error.SmbFailed, ""));
    try testing.expectEqualStrings("LoginFailed: Login failed for user 'x'.", try connectorNoteAlloc(a, "", error.LoginFailed, "Login failed for user 'x'."));
    try testing.expectEqualStrings("LoginFailed", try connectorNoteAlloc(a, "", error.LoginFailed, ""));
    var buf: [12]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try testing.expectError(error.WriteFailed, connectorNote(&w, "sftp", error.EndOfStream, "x"));
}

test "dial connects and tunes the socket: no Nagle, keepalive on, a dead peer dropped in fifteen minutes on Linux" {
    const addr = try std.net.Address.parseIp("127.0.0.1", 0);
    var server = try addr.listen(.{});
    defer server.deinit();
    const stream = try dial(testing.allocator, "127.0.0.1", server.listen_address.getPort(), .{});
    defer stream.close();
    var one: c_int = 0;
    try std.posix.getsockopt(stream.handle, std.posix.IPPROTO.TCP, std.posix.TCP.NODELAY, std.mem.asBytes(&one));
    try testing.expect(one != 0);
    one = 0;
    try std.posix.getsockopt(stream.handle, std.posix.SOL.SOCKET, std.posix.SO.KEEPALIVE, std.mem.asBytes(&one));
    try testing.expect(one != 0);
    if (@import("builtin").os.tag != .linux) return;
    const TCP = std.os.linux.TCP;
    const want = [_]struct { u32, c_int }{ .{ TCP.KEEPIDLE, 60 }, .{ TCP.KEEPINTVL, 15 }, .{ TCP.KEEPCNT, 56 }, .{ TCP.USER_TIMEOUT, 900_000 } };
    for (want) |w| {
        var v: c_int = 0;
        try std.posix.getsockopt(stream.handle, std.posix.IPPROTO.TCP, w[0], std.mem.asBytes(&v));
        try testing.expectEqual(w[1], v);
    }
}

test "setIoTimeout sets both deadlines and 0 clears them; a fired one reads as a timeout" {
    const addr = try std.net.Address.parseIp("127.0.0.1", 0);
    var server = try addr.listen(.{});
    defer server.deinit();
    const stream = try dial(testing.allocator, "127.0.0.1", server.listen_address.getPort(), (Timeouts{ .connect_s = 1 }).dialOpts());
    defer stream.close();
    var rb: [64]u8 = undefined;
    var wb: [64]u8 = undefined;
    var sr = stream.reader(&rb);
    var sw = stream.writer(&wb);
    try testing.expect(!ioTimedOut(&sr, &sw));
    try testing.expectError(error.ReadFailed, sr.interface().takeByte());
    try testing.expect(ioTimedOut(&sr, &sw));
    (Timeouts{ .connect_s = 1 }).sessionUp(stream.handle);
    for ([_]u32{ std.posix.SO.RCVTIMEO, std.posix.SO.SNDTIMEO }) |opt| {
        var tv = std.posix.timeval{ .sec = 9, .usec = 9 };
        try std.posix.getsockopt(stream.handle, std.posix.SOL.SOCKET, opt, std.mem.asBytes(&tv));
        try testing.expectEqual(@as(@TypeOf(tv.sec), 0), tv.sec);
        try testing.expectEqual(@as(@TypeOf(tv.usec), 0), tv.usec);
    }
    (Timeouts{ .query_s = 7 }).sessionUp(stream.handle);
    var tv = std.posix.timeval{ .sec = 0, .usec = 0 };
    try std.posix.getsockopt(stream.handle, std.posix.SOL.SOCKET, std.posix.SO.RCVTIMEO, std.mem.asBytes(&tv));
    try testing.expectEqual(@as(@TypeOf(tv.sec), 7), tv.sec);
}

test "a TCP connect the server never completes fails within io_timeout_s, naming host and phase" {
    if (@import("builtin").os.tag != .linux) return error.SkipZigTest;
    const addr = try std.net.Address.parseIp("127.0.0.1", 0);
    var server = try addr.listen(.{ .kernel_backlog = 0 });
    defer server.deinit();
    const port = server.listen_address.getPort();
    var held: [16]std.net.Stream = undefined;
    var n: usize = 0;
    defer for (held[0..n]) |h| h.close();
    const e = while (n < held.len) {
        var t = try std.time.Timer.start();
        if (dial(testing.allocator, "127.0.0.1", port, .{ .tune = false, .io_timeout_s = 1 })) |st| {
            held[n] = st;
            n += 1;
        } else |e| {
            try testing.expect(t.read() < 3 * std.time.ns_per_s);
            break e;
        }
    } else return error.TestUnexpectedResult;
    try testing.expectEqual(error.ConnectionTimedOut, e);
    var want: [64]u8 = undefined;
    try testing.expectEqualStrings(try std.fmt.bufPrint(&want, "127.0.0.1:{d}: connect did not answer within 1 s", .{port}), noteFor(error.ConnectionTimedOut));
}

test "a login timeout names host, port and phase; it and the dead-peer errors are transient, run and connector alike" {
    try testing.expectEqual(error.ConnectionTimedOut, loginTimedOut("db.internal", 5432, "login", 60));
    try testing.expectEqualStrings("db.internal:5432: login did not answer within 60 s", noteFor(error.ConnectionTimedOut));
    const env = @import("../runtime/env.zig");
    for ([_]anyerror{ error.ConnectionTimedOut, error.ConnectionResetByPeer, error.BrokenPipe, error.ConnectionIoFailed, error.SmbDisconnected, error.SshConnectionLost }) |e| {
        try testing.expect(transientNet(e));
        try testing.expect(transientRun(e));
        try testing.expect(env.isTransient(e));
    }
    try testing.expect(!env.isTransient(error.SshDisconnected));
    try testing.expect(!transientRun(error.SshDisconnected));
}

test "dial leaves an untuned socket at the OS defaults and sets io timeouts when asked" {
    const addr = try std.net.Address.parseIp("127.0.0.1", 0);
    var server = try addr.listen(.{});
    defer server.deinit();
    const port = server.listen_address.getPort();
    const plain = try dial(testing.allocator, "127.0.0.1", port, .{ .tune = false });
    defer plain.close();
    var one: c_int = 1;
    try std.posix.getsockopt(plain.handle, std.posix.IPPROTO.TCP, std.posix.TCP.NODELAY, std.mem.asBytes(&one));
    try testing.expectEqual(@as(c_int, 0), one);
    var tv = std.posix.timeval{ .sec = 0, .usec = 0 };
    try std.posix.getsockopt(plain.handle, std.posix.SOL.SOCKET, std.posix.SO.RCVTIMEO, std.mem.asBytes(&tv));
    try testing.expectEqual(@as(@TypeOf(tv.sec), 0), tv.sec);

    const timed = try dial(testing.allocator, "127.0.0.1", port, .{ .io_timeout_s = 60 });
    defer timed.close();
    try std.posix.getsockopt(timed.handle, std.posix.SOL.SOCKET, std.posix.SO.RCVTIMEO, std.mem.asBytes(&tv));
    try testing.expectEqual(@as(@TypeOf(tv.sec), 60), tv.sec);
    tv.sec = 0;
    try std.posix.getsockopt(timed.handle, std.posix.SOL.SOCKET, std.posix.SO.SNDTIMEO, std.mem.asBytes(&tv));
    try testing.expectEqual(@as(@TypeOf(tv.sec), 60), tv.sec);
    one = 0;
    try std.posix.getsockopt(timed.handle, std.posix.IPPROTO.TCP, std.posix.TCP.NODELAY, std.mem.asBytes(&one));
    try testing.expect(one != 0);
}
