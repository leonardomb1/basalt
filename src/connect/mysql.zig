//! Minimal MySQL-protocol client — just enough to run DDL against a StarRocks FE
//! (handshake + mysql_native_password auth + COM_QUERY, parsing only OK/ERR; no
//! result sets). Used by the StarRocks sink for CREATE TABLE / TRUNCATE.
//!
//! Network-tested only against a live server; the auth token math is unit-tested
//! in starrocks.zig.

const std = @import("std");
const sr = @import("starrocks.zig");

const CLIENT_LONG_PASSWORD = 0x00000001;
const CLIENT_CONNECT_WITH_DB = 0x00000008;
const CLIENT_PROTOCOL_41 = 0x00000200;
const CLIENT_SECURE_CONNECTION = 0x00008000;
const CLIENT_PLUGIN_AUTH = 0x00080000;

pub const Error = error{ MysqlAuthFailed, MysqlQueryFailed, MysqlProtocol } || std.mem.Allocator.Error;

pub const Conn = struct {
    gpa: std.mem.Allocator,
    stream: std.net.Stream,
    buf: std.ArrayList(u8),
    last_error: []const u8 = "",

    pub fn connect(gpa: std.mem.Allocator, host: []const u8, port: u16, user: []const u8, password: []const u8, database: []const u8) !Conn {
        const stream = try std.net.tcpConnectToHost(gpa, host, port);
        var self = Conn{ .gpa = gpa, .stream = stream, .buf = std.ArrayList(u8).init(gpa) };
        errdefer self.close();

        // 1. server handshake
        const seq = try self.readPacket();
        const salt = try self.parseHandshake(self.buf.items);

        // 2. handshake response
        try self.writeHandshakeResponse(seq + 1, user, password, database, salt);

        // 3. auth result
        _ = try self.readPacket();
        const p = self.buf.items;
        if (p.len == 0) return error.MysqlProtocol;
        switch (p[0]) {
            0x00 => {}, // OK
            0xff => {
                self.last_error = try self.gpa.dupe(u8, errMessage(p));
                return error.MysqlAuthFailed;
            },
            0xfe => {
                // auth switch request: payload = 0xfe + plugin(null) + salt(...)
                const new_salt = parseAuthSwitch(p);
                const token = sr.mysqlAuthToken(password, &new_salt);
                try self.writePacket(3, if (password.len == 0) "" else &token);
                _ = try self.readPacket();
                if (self.buf.items.len == 0 or self.buf.items[0] == 0xff) {
                    if (self.buf.items.len > 0) self.last_error = try self.gpa.dupe(u8, errMessage(self.buf.items));
                    return error.MysqlAuthFailed;
                }
            },
            else => return error.MysqlProtocol,
        }
        return self;
    }

    /// Run a statement that returns no result set (DDL). Errors on an ERR packet.
    pub fn exec(self: *Conn, sql: []const u8) !void {
        const payload = try self.gpa.alloc(u8, sql.len + 1);
        defer self.gpa.free(payload);
        payload[0] = 0x03; // COM_QUERY
        @memcpy(payload[1..], sql);
        try self.writePacket(0, payload); // COM_QUERY resets sequence to 0

        _ = try self.readPacket();
        const p = self.buf.items;
        if (p.len > 0 and p[0] == 0xff) {
            self.last_error = try self.gpa.dupe(u8, errMessage(p));
            return error.MysqlQueryFailed;
        }
        // 0x00 (OK) or a result-set header — DDL yields OK; accept anything non-ERR.
    }

    pub fn close(self: *Conn) void {
        self.buf.deinit();
        self.stream.close();
    }

    // --- packet framing ---

    fn readPacket(self: *Conn) !u8 {
        var header: [4]u8 = undefined;
        try self.stream.reader().readNoEof(&header);
        const len: usize = @as(usize, header[0]) | (@as(usize, header[1]) << 8) | (@as(usize, header[2]) << 16);
        try self.buf.resize(len);
        try self.stream.reader().readNoEof(self.buf.items[0..len]);
        return header[3];
    }

    fn writePacket(self: *Conn, seq: u8, payload: []const u8) !void {
        var header: [4]u8 = undefined;
        header[0] = @intCast(payload.len & 0xff);
        header[1] = @intCast((payload.len >> 8) & 0xff);
        header[2] = @intCast((payload.len >> 16) & 0xff);
        header[3] = seq;
        try self.stream.writer().writeAll(&header);
        try self.stream.writer().writeAll(payload);
    }

    fn parseHandshake(self: *Conn, p: []const u8) ![20]u8 {
        _ = self;
        if (p.len < 1 or p[0] != 10) return error.MysqlProtocol;
        var i: usize = 1;
        while (i < p.len and p[i] != 0) : (i += 1) {} // server version
        i += 1;
        i += 4; // thread id
        if (i + 8 > p.len) return error.MysqlProtocol;
        var salt: [20]u8 = undefined;
        @memcpy(salt[0..8], p[i .. i + 8]); // auth-plugin-data part 1
        i += 8;
        i += 1; // filler
        i += 2; // capability lower
        i += 1; // charset
        i += 2; // status
        i += 2; // capability upper
        i += 1; // auth-plugin-data length
        i += 10; // reserved
        if (i + 12 > p.len) return error.MysqlProtocol;
        @memcpy(salt[8..20], p[i .. i + 12]); // auth-plugin-data part 2 (first 12)
        return salt;
    }

    fn writeHandshakeResponse(self: *Conn, seq: u8, user: []const u8, password: []const u8, database: []const u8, salt: [20]u8) !void {
        var caps: u32 = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH | CLIENT_LONG_PASSWORD;
        if (database.len > 0) caps |= CLIENT_CONNECT_WITH_DB;

        var out = std.ArrayList(u8).init(self.gpa);
        defer out.deinit();
        const w = out.writer();

        try w.writeInt(u32, caps, .little);
        try w.writeInt(u32, 0x01000000, .little); // max packet 16M
        try w.writeByte(33); // utf8_general_ci
        try w.writeByteNTimes(0, 23); // reserved
        try w.writeAll(user);
        try w.writeByte(0);

        if (password.len == 0) {
            try w.writeByte(0); // empty auth response
        } else {
            const token = sr.mysqlAuthToken(password, &salt);
            try w.writeByte(20);
            try w.writeAll(&token);
        }

        if (database.len > 0) {
            try w.writeAll(database);
            try w.writeByte(0);
        }
        try w.writeAll("mysql_native_password");
        try w.writeByte(0);

        try self.writePacket(seq, out.items);
    }
};

fn parseAuthSwitch(p: []const u8) [20]u8 {
    // 0xfe, plugin name (null-terminated), then salt
    var i: usize = 1;
    while (i < p.len and p[i] != 0) : (i += 1) {}
    i += 1;
    var salt: [20]u8 = [_]u8{0} ** 20;
    const n = @min(@as(usize, 20), p.len -| i);
    if (n > 0) @memcpy(salt[0..n], p[i .. i + n]);
    return salt;
}

fn errMessage(p: []const u8) []const u8 {
    // 0xff, error_code(2), if 41: '#' + sql_state(5), then message
    if (p.len < 3) return "mysql error";
    var i: usize = 3;
    if (p.len > 3 and p[3] == '#') i = 9; // skip '#XXXXX'
    if (i > p.len) return "mysql error";
    return p[i..];
}
