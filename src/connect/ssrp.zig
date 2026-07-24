//! SQL Server Resolution Protocol (SSRP / [MC-SQLR]) — resolves a named
//! instance (`host\INSTANCE`) to its dynamic TCP port via the SQL Server
//! Browser service on UDP 1434. Basalt's TDS driver only speaks direct
//! host:port, so this runs first when a connection names an instance and no
//! explicit `port` was given.
//!
//! Exchange: send one request byte `0x03` (CLNT_UCAST_EX — "list every
//! instance on this server") to <host>:1434; the Browser replies with one
//! datagram: `0x05`, a 2-byte little-endian length, then an ASCII string of
//! `;`-delimited `key;value` pairs, one instance block per `;;`:
//!   ServerName;HOST;InstanceName;WMS;IsClustered;No;Version;15.0.2000.5;tcp;51000;;
//! We find the block whose InstanceName matches (case-insensitive) and read its
//! `tcp` port.
//!
//! Caveat: UDP 1434 is frequently firewalled even where the TDS port is open,
//! so an explicit `port` (which skips this lookup) stays the robust choice in
//! locked-down networks.

const std = @import("std");

pub const Error = error{
    NoInstance,
    BrowserTimeout,
    InstanceNotFound,
    TcpDisabled,
} || std.mem.Allocator.Error;

pub const HostInstance = struct { host: []const u8, instance: ?[]const u8 };

/// Split `host\INSTANCE` on the first backslash. No backslash ⇒ instance null
/// (a plain host:port connection). Slices borrow from `host`.
pub fn splitHostInstance(host: []const u8) HostInstance {
    if (std.mem.indexOfScalar(u8, host, '\\')) |bs| {
        return .{ .host = host[0..bs], .instance = host[bs + 1 ..] };
    }
    return .{ .host = host, .instance = null };
}

/// Parse an SSRP response, returning the TCP port of the block whose
/// InstanceName matches `instance` (case-insensitive). Errors distinguish
/// "no such instance" from "found but TCP disabled" so the caller can say
/// something actionable. Offline-testable — no socket involved.
pub fn parsePort(data: []const u8, instance: []const u8) Error!u16 {
    // Skip the `0x05` + 2-byte length header when present; be lenient if not.
    var body = data;
    if (body.len >= 3 and body[0] == 0x05) body = body[3..];

    var found = false;
    var blocks = std.mem.splitSequence(u8, body, ";;");
    while (blocks.next()) |block| {
        if (block.len == 0) continue;
        var toks = std.mem.splitScalar(u8, block, ';');
        var name: ?[]const u8 = null;
        var tcp: ?u16 = null;
        while (toks.next()) |key| {
            const val = toks.next() orelse break; // pairs; a lone trailing key is ignored
            if (std.ascii.eqlIgnoreCase(key, "InstanceName")) {
                name = val;
            } else if (std.ascii.eqlIgnoreCase(key, "tcp")) {
                tcp = std.fmt.parseInt(u16, std.mem.trim(u8, val, " "), 10) catch null;
            }
        }
        if (name) |n| if (std.ascii.eqlIgnoreCase(n, instance)) {
            found = true;
            if (tcp) |p| return p;
        };
    }
    return if (found) Error.TcpDisabled else Error.InstanceNotFound;
}

/// Resolve `host\instance` → TCP port by querying the SQL Server Browser at
/// `host:1434`. One 2s-timeout UDP round-trip, retried once (UDP is lossy).
pub fn resolveInstancePort(gpa: std.mem.Allocator, host: []const u8, instance: []const u8) !u16 {
    const list = try std.net.getAddressList(gpa, host, 1434);
    defer list.deinit();
    if (list.addrs.len == 0) return error.UnknownHostName;
    const addr = list.addrs[0];

    const sock = try std.posix.socket(addr.any.family, std.posix.SOCK.DGRAM, std.posix.IPPROTO.UDP);
    defer std.posix.close(sock);
    const tv = std.posix.timeval{ .sec = 2, .usec = 0 };
    try std.posix.setsockopt(sock, std.posix.SOL.SOCKET, std.posix.SO.RCVTIMEO, std.mem.asBytes(&tv));

    const req = [_]u8{0x03}; // CLNT_UCAST_EX
    var buf: [4096]u8 = undefined;
    var attempt: usize = 0;
    while (attempt < 2) : (attempt += 1) {
        _ = std.posix.sendto(sock, &req, 0, &addr.any, addr.getOsSockLen()) catch |e| return e;
        const n = std.posix.recvfrom(sock, &buf, 0, null, null) catch |e| switch (e) {
            error.WouldBlock => continue, // timeout — retry once, then give up
            else => return e,
        };
        return parsePort(buf[0..n], instance);
    }
    return Error.BrowserTimeout;
}

// ---------------------------------------------------------------------------

const testing = std.testing;

test "splitHostInstance: named instance vs plain host" {
    const a = splitHostInstance("10.110.2.5\\WMS");
    try testing.expectEqualStrings("10.110.2.5", a.host);
    try testing.expectEqualStrings("WMS", a.instance.?);

    const b = splitHostInstance("sql.internal");
    try testing.expectEqualStrings("sql.internal", b.host);
    try testing.expect(b.instance == null);
}

test "parsePort: picks the matching instance's tcp port (case-insensitive)" {
    // A two-instance response with the 0x05 + length header.
    const payload =
        "ServerName;HOST;InstanceName;MSSQLSERVER;IsClustered;No;Version;15.0.2000.5;tcp;1433;;" ++
        "ServerName;HOST;InstanceName;WMS;IsClustered;No;Version;15.0.2000.5;tcp;51000;;";
    var data: [3 + payload.len]u8 = undefined;
    data[0] = 0x05;
    std.mem.writeInt(u16, data[1..3], @intCast(payload.len), .little);
    @memcpy(data[3..], payload);

    try testing.expectEqual(@as(u16, 51000), try parsePort(&data, "wms")); // case-insensitive
    try testing.expectEqual(@as(u16, 1433), try parsePort(&data, "MSSQLSERVER"));
}

test "parsePort: header-less body and error cases" {
    const one = "ServerName;H;InstanceName;WMS;tcp;51000;;";
    try testing.expectEqual(@as(u16, 51000), try parsePort(one, "WMS")); // no 0x05 header
    try testing.expectError(Error.InstanceNotFound, parsePort(one, "OTHER"));

    // TCP disabled: instance present, only a named-pipe entry, no tcp.
    const np = "ServerName;H;InstanceName;WMS;np;\\\\H\\pipe\\sql\\query;;";
    try testing.expectError(Error.TcpDisabled, parsePort(np, "WMS"));
}
