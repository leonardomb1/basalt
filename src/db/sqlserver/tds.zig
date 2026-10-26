//! Minimal TDS (SQL Server) client: packet framing, PRELOGIN, optional TDS 7.x
//! tunneled TLS, LOGIN7 with SQL, federated (Azure AD), NTLMv2 or Kerberos (SSPI)
//! auth, SQLBatch, INSERT BULK, and a streaming token-stream reader
//! (`PacketReader`/`TdsCursor`) that pulls packets on demand, so a ROW token may
//! span packet boundaries invisibly to the parser. `TdsCursor` has its own batch
//! reader, so its cursor vtable is hand-written rather than `sql.textCursorVTable`.
//!
//! Tunneled TLS: while handshaking, `TlsShim` wraps outgoing TLS flights in
//! PRELOGIN packets and unwraps the server's replies; afterwards both directions
//! pass through raw, with whole TDS packets riding inside the session. Federated,
//! NTLM and Kerberos logins always require TLS: a cleartext NTLM exchange hands the
//! challenge/response to any passive observer for offline cracking, and nothing
//! else protects the session.
//!
//! Outgoing messages are split into packets of the negotiated size with EOM only
//! on the last: a SQLBatch sent as one oversized packet is reset by the server or
//! gateway. A negotiated packet size is accepted only within the spec's 512-32767.
//!
//! After login every session sets what the ODBC, OLE DB, JDBC and .NET drivers set,
//! since a bare TDS login gets legacy defaults: TEXTSIZE (else (max) values arrive
//! cut at 4096 bytes), ANSI_WARNINGS (else overlong values truncate and division by
//! zero becomes NULL silently), ANSI_NULLS (the three-valued logic every translated
//! predicate assumes), ANSI_PADDING, ANSI_NULL_DFLT_ON, and QUOTED_IDENTIFIER,
//! CONCAT_NULL_YIELDS_NULL and ARITHABORT, without which writes to a table with a
//! filtered index, indexed view or indexed computed column are refused.
//!
//! Every token and cell parser reads server-controlled bytes, often before
//! authentication, so lengths are bounded against the message (an unbounded error
//! text once disclosed adjacent heap pre-auth), wire integers use checked casts and
//! saturating math (raw day counts and time units once panicked), and shifts stop
//! at the accumulator's width. A corrupt cell reads as null or a clamped value.
//!
//! Decoding notes: `tinyint` is the only 1-byte integer and is unsigned (sign
//! extension once turned 200 into -56). MONEY is a scaled integer of
//! ten-thousandths, the 8-byte form sent high word first. Non-Unicode text is
//! transcoded from Windows-1252, the code page behind the common Latin collations,
//! so accented text does not reach a UTF-8 sink as invalid bytes. `uniqueidentifier`
//! renders as a canonical GUID and binary as `0x...` hex, since raw bytes would
//! carry separators and newlines into a delimited bulk load.
//!
//! INSERT BULK declares every column nvarchar(4000); each segment carries its own
//! COLMETADATA then ROW tokens of UTF-16 cells, verified against the DONE count.

const std = @import("std");
const types = @import("../../lang/types.zig");
const column = @import("../../exec/column.zig");
const celltext = @import("../../exec/celltext.zig");
const Batch = @import("../../exec/batch.zig").Batch;
const Value = @import("../../exec/value.zig").Value;
const Decimal = @import("../../exec/value.zig").Decimal;
const cell = @import("../../exec/cell.zig");
const decimal = @import("../../exec/decimal.zig");
const civil = @import("../../exec/civil.zig");
const charset = @import("../../format/charset.zig");
const hex = @import("../../format/hex.zig");
const uuid = @import("../../format/uuid.zig");
const utf16 = @import("../../format/utf16.zig");
const eval = @import("../../exec/eval.zig");
const driver = @import("../../connect/driver.zig");
const sql = @import("../sql.zig");
const wire = @import("../wire.zig");
const ntlm = @import("../../net/auth/ntlm.zig");
const krb5 = @import("../../net/auth/krb5.zig");
const spnego = @import("../../net/auth/spnego.zig");

const PKT_PRELOGIN = 0x12;
const PKT_LOGIN7 = 0x10;
const PKT_SQLBATCH = 0x01;
const PKT_BULK = 0x07;
const PKT_SSPI = 0x11;
const STATUS_EOM = 0x01;
const BULK_PKT_PAYLOAD = 4088;

pub const Error = error{ TdsProtocol, LoginFailed, QueryFailed, EncryptionRequired, TdsTlsRefused, UnsupportedTdsType } || std.mem.Allocator.Error || std.net.Stream.WriteError || std.net.Stream.ReadError;

threadlocal var rejected: driver.ThreadNote(512) = .{};

/// The server's own words for the last login it rejected on this thread, or "".
pub fn lastError() []const u8 {
    return rejected.get();
}

pub const Conn = struct {
    wire: wire.Conn,
    txn_desc: [8]u8 = @splat(0),
    sspi_reply: []const u8 = "",
    shim: TlsShim = undefined,
    fed_required: bool = false,
    fed_nonce: ?[32]u8 = null,
    pkt_payload: usize = BULK_PKT_PAYLOAD,
    last_done_count: ?u64 = null,

    pub fn connect(gpa: std.mem.Allocator, host: []const u8, port: u16, user: []const u8, password: []const u8, database: []const u8, tls_mode: sql.TlsMode) !*Conn {
        rejected.clear();
        const self = try wire.Conn.dial(Conn, gpa, host, port);
        errdefer self.close();
        try self.prelogin(tls_mode != .off);
        if (tls_mode != .off) try self.startTls(host, tls_mode);
        self.login(user, password, database, host) catch |e| {
            rejected.set(self.wire.last_error);
            return e;
        };
        try self.afterLogin();
        return self;
    }

    /// Azure AD access token (Dataverse / Azure SQL): PRELOGIN advertises
    /// FEDAUTHREQUIRED and LOGIN7 carries the token in a FEDAUTH feature extension. The
    /// caller fetches the token (see aad.ropcToken).
    pub fn connectAad(gpa: std.mem.Allocator, host: []const u8, port: u16, token: []const u8, database: []const u8, tls_mode: sql.TlsMode) !*Conn {
        rejected.clear();
        if (tls_mode == .off) return error.EncryptionRequired;
        const self = try wire.Conn.dial(Conn, gpa, host, port);
        errdefer self.close();
        try self.preloginFed();
        try self.startTls(host, tls_mode);
        const payload = try buildLogin7Fedauth(gpa, token, database, host, self.fed_required, self.fed_nonce);
        defer gpa.free(payload);
        try self.writePacket(PKT_LOGIN7, payload);
        try self.readMessage();
        self.parseLoginResponse() catch |e| {
            rejected.set(self.wire.last_error);
            return e;
        };
        try self.afterLogin();
        return self;
    }

    /// NTLMv2 with an explicit `DOMAIN\user` password, not OS single sign-on.
    pub fn connectNtlm(gpa: std.mem.Allocator, host: []const u8, port: u16, cred: ntlm.Credential, database: []const u8, tls_mode: sql.TlsMode) !*Conn {
        rejected.clear();
        if (tls_mode == .off) return error.EncryptionRequired;
        const self = try wire.Conn.dial(Conn, gpa, host, port);
        errdefer self.close();
        try self.prelogin(true);
        try self.startTls(host, tls_mode);
        try self.loginNtlm(cred, database, host);
        try self.afterLogin();
        return self;
    }

    /// A ticket for `spn` (`MSSQLSvc/<host>:<port>`) asked of the KDC with the password,
    /// its AP-REQ in SPNEGO in the LOGIN7 SSPI field, the server's AP-REP checked.
    pub fn connectKerberos(gpa: std.mem.Allocator, host: []const u8, port: u16, cred: krb5.Credential, spn: []const u8, database: []const u8, tls_mode: sql.TlsMode) !*Conn {
        rejected.clear();
        if (tls_mode == .off) return error.EncryptionRequired;
        const self = try wire.Conn.dial(Conn, gpa, host, port);
        errdefer self.close();
        try self.prelogin(true);
        try self.startTls(host, tls_mode);
        self.loginKerberos(cred, spn, database, host) catch |e| {
            rejected.set(self.wire.last_error);
            return e;
        };
        try self.afterLogin();
        return self;
    }

    fn startTls(self: *Conn, host: []const u8, mode: sql.TlsMode) !void {
        self.shim = .{ .inner_r = self.wire.sr.interface(), .inner_w = &self.wire.sw.interface, .reader = undefined, .writer = undefined };
        self.shim.reader = .{ .vtable = &TlsShim.reader_vtable, .buffer = &self.shim.rbuf, .seek = 0, .end = 0 };
        self.shim.writer = .{ .vtable = &TlsShim.writer_vtable, .buffer = &self.shim.wbuf };
        try self.wire.startTlsOver(host, mode, &self.shim.reader, &self.shim.writer);
        self.shim.handshaking = false;
    }

    pub fn close(self: *Conn) void {
        self.wire.close(self);
    }

    pub fn sqlConn(self: *Conn) sql.Conn {
        return .{ .ptr = self, .vtable = &sql_vtable };
    }

    pub fn queryCursor(self: *Conn, stmt: []const u8) !sql.Cursor {
        try self.sendBatch(stmt);
        const cur = try self.wire.gpa.create(TdsCursor);
        cur.* = .{
            .gpa = self.wire.gpa,
            .conn = self,
            .reader = PacketReader.init(self.wire.gpa, self.wire.rd()),
            .meta_arena = std.heap.ArenaAllocator.init(self.wire.gpa),
            .cols = &.{},
            .schema = undefined,
            .done = false,
        };
        cur.readHeader() catch |e| {
            cur.reader.deinit();
            cur.meta_arena.deinit();
            self.wire.gpa.destroy(cur);
            return e;
        };
        return .{ .ptr = cur, .vtable = &cursor_vtable };
    }

    fn afterLogin(self: *Conn) !void {
        try self.exec("SET TEXTSIZE 2147483647; SET ANSI_WARNINGS ON; SET ANSI_NULLS ON; SET ANSI_PADDING ON; " ++
            "SET ANSI_NULL_DFLT_ON ON; SET QUOTED_IDENTIFIER ON; SET CONCAT_NULL_YIELDS_NULL ON; SET ARITHABORT ON; " ++
            "SET NUMERIC_ROUNDABORT OFF; SET IMPLICIT_TRANSACTIONS OFF");
    }

    pub fn exec(self: *Conn, statement: []const u8) !void {
        try self.sendBatch(statement);
        try self.readMessage();
        try self.scanResultTokens();
    }

    /// Scan a reply's tokens; an ERROR token sets `last_error` and returns QueryFailed.
    fn scanResultTokens(self: *Conn) !void {
        var r: Body = .{ .p = self.wire.msg.items };
        while (try nextToken(&r)) |t| switch (t.id) {
            0xAA => return self.failWith(t.body, error.QueryFailed),
            0xE3 => self.noteEnvChange(t.body),
            0xFD, 0xFE, 0xFF => if (parseDoneRowCount(t.id, t.body)) |n| {
                self.last_done_count = n;
            },
            else => {},
        };
    }

    /// The server's ERROR token text as `last_error`, then `e`.
    fn failWith(self: *Conn, body: []const u8, comptime e: anytype) (@TypeOf(e) || std.mem.Allocator.Error) {
        self.wire.putError(try errorText(self.wire.gpa, body));
        return e;
    }

    /// The transaction descriptor a BEGIN TRANSACTION reports (ENVCHANGE type 8),
    /// sent back in every later request's headers until a commit or rollback (9, 10)
    /// clears it; a request with none while a transaction is open is refused.
    fn noteEnvChange(self: *Conn, d: []const u8) void {
        if (d.len < 2) return;
        switch (d[0]) {
            8 => if (d[1] == 8 and d.len >= 10) @memcpy(&self.txn_desc, d[2..10]),
            9, 10 => self.txn_desc = @splat(0),
            else => {},
        }
    }

    fn writePacket(self: *Conn, ptype: u8, payload: []const u8) !void {
        const w = self.wire.wr();
        var off: usize = 0;
        while (true) {
            const chunk: usize = @min(payload.len - off, self.pkt_payload);
            const last = off + chunk == payload.len;
            var header: [8]u8 = .{ ptype, if (last) STATUS_EOM else 0x00, 0, 0, 0, 0, 0, 0 };
            const total: u16 = @intCast(chunk + 8);
            header[2] = @intCast(total >> 8);
            header[3] = @intCast(total & 0xff);
            try w.writeAll(&header);
            try w.writeAll(payload[off .. off + chunk]);
            off += chunk;
            if (last) break;
        }
        try self.wire.flush();
    }

    fn readMessage(self: *Conn) !void {
        self.wire.msg.clearRetainingCapacity();
        const r = self.wire.rd();
        while (true) {
            var header: [8]u8 = undefined;
            try r.readSliceAll(&header);
            const len: usize = (@as(usize, header[2]) << 8) | header[3];
            if (len < 8) return error.TdsProtocol;
            const start = self.wire.msg.items.len;
            if (len - 8 > max_message - start) return error.TdsProtocol;
            try self.wire.msg.resize(start + (len - 8));
            try r.readSliceAll(self.wire.msg.items[start..]);
            if (header[1] & STATUS_EOM != 0) break;
        }
    }

    /// PRELOGIN advertising FEDAUTHREQUIRED and encryption on; parses the server's
    /// FEDAUTHREQUIRED value and any 32-byte nonce from the response.
    fn preloginFed(self: *Conn) !void {
        const payload = [_]u8{
            0x00, 0x00, 0x10, 0x00, 0x06,
            0x01, 0x00, 0x16, 0x00, 0x01,
            0x06, 0x00, 0x17, 0x00, 0x01,
            0xFF, 0x11, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x01, 0x01,
        };
        try self.writePacket(PKT_PRELOGIN, &payload);
        try self.readMessage();

        const p = self.wire.msg.items;
        var i: usize = 0;
        while (i + 5 <= p.len and p[i] != 0xFF) : (i += 5) {
            const tok = p[i];
            const off: usize = (@as(usize, p[i + 1]) << 8) | p[i + 2];
            const len: usize = (@as(usize, p[i + 3]) << 8) | p[i + 4];
            if (off + len > p.len) continue;
            switch (tok) {
                0x01 => {
                    if (len >= 1 and !(p[off] == 0x01 or p[off] == 0x03)) return error.TdsTlsRefused;
                },
                0x06 => self.fed_required = (len >= 1 and p[off] == 0x01),
                0x07 => if (len == 32) {
                    var n: [32]u8 = undefined;
                    @memcpy(&n, p[off .. off + 32]);
                    self.fed_nonce = n;
                },
                else => {},
            }
        }
    }

    fn prelogin(self: *Conn, want_tls: bool) !void {
        const payload = [_]u8{
            0x00, 0x00, 0x0B,                                  0x00, 0x06,
            0x01, 0x00, 0x11,                                  0x00, 0x01,
            0xFF, 0x11, 0x00,                                  0x00, 0x00,
            0x00, 0x00, if (want_tls) @as(u8, 0x01) else 0x02,
        };
        try self.writePacket(PKT_PRELOGIN, &payload);
        try self.readMessage();

        const p = self.wire.msg.items;
        var i: usize = 0;
        while (i + 5 <= p.len and p[i] != 0xFF) : (i += 5) {
            if (p[i] == 0x01) {
                const off: usize = (@as(usize, p[i + 1]) << 8) | p[i + 2];
                if (off >= p.len) return error.TdsProtocol;
                const enc_on = (p[off] == 0x01 or p[off] == 0x03);
                if (want_tls and !enc_on) return error.TdsTlsRefused;
                if (!want_tls and enc_on) return error.EncryptionRequired;
            }
        }
    }

    fn login(self: *Conn, user: []const u8, password: []const u8, database: []const u8, host: []const u8) !void {
        const payload = try buildLogin7(self.wire.gpa, user, password, database, host);
        defer self.wire.gpa.free(payload);
        try self.writePacket(PKT_LOGIN7, payload);
        try self.readMessage();
        try self.parseLoginResponse();
    }

    /// Three-leg NTLMv2 handshake (MS-TDS 2.2.6.8): Type 1 in the LOGIN7 SSPI field, Type 2
    /// in the server's SSPI token, Type 3 back in an SSPI packet (0x11).
    fn loginNtlm(self: *Conn, cred: ntlm.Credential, database: []const u8, host: []const u8) !void {
        const neg = try ntlm.negotiate(self.wire.gpa, cred);
        defer self.wire.gpa.free(neg);
        const payload = try buildLogin7Sspi(self.wire.gpa, neg, database, host);
        defer self.wire.gpa.free(payload);
        try self.writePacket(PKT_LOGIN7, payload);
        try self.readMessage();

        const challenge = try self.sspiToken();
        var nonce: [8]u8 = undefined;
        std.crypto.random.bytes(&nonce);
        const auth = try ntlm.authenticate(self.wire.gpa, cred, challenge, ntlm.filetimeNow(), nonce);
        defer self.wire.gpa.free(auth);
        try self.writePacket(PKT_SSPI, auth);
        try self.readMessage();
        self.parseLoginResponse() catch |e| {
            rejected.set(self.wire.last_error);
            return e;
        };
    }

    /// One round (MS-TDS 2.2.6.4): AP-REQ in SPNEGO in the LOGIN7 SSPI field. The AP-REP
    /// beside the LOGINACK is required and checked, so the server proves it holds the
    /// service key; a login without it is refused.
    fn loginKerberos(self: *Conn, cred: krb5.Credential, spn: []const u8, database: []const u8, host: []const u8) !void {
        const t = krb5.serviceTicket(self.wire.gpa, cred, spn) catch |e| return self.krbFailed(e);
        const ctx = krb5.initContext(self.wire.gpa, cred, &t) catch |e| return self.krbFailed(e);
        defer self.wire.gpa.free(ctx.token);
        const tok = try spnego.initWith(self.wire.gpa, &.{ &spnego.oid_krb5, &spnego.oid_ms_krb5 }, ctx.token);
        defer self.wire.gpa.free(tok);
        const payload = try buildLogin7Sspi(self.wire.gpa, tok, database, host);
        defer self.wire.gpa.free(payload);
        try self.writePacket(PKT_LOGIN7, payload);
        try self.readMessage();

        self.parseLoginResponse() catch |e| {
            if (e == error.LoginFailed and self.wire.last_error.len == 0 and self.sspi_reply.len > 0)
                return self.loginRefused("the server asked for a second Kerberos round, which basalt does not do", .{});
            return e;
        };
        if (self.sspi_reply.len == 0)
            return self.loginRefused("the server accepted the ticket without proving itself (no AP-REP)", .{});
        const reply = spnego.token(self.sspi_reply) orelse self.sspi_reply;
        _ = krb5.acceptReply(self.wire.gpa, &ctx, reply) catch |e| return self.krbFailed(e);
        self.sspi_reply = "";
    }

    fn krbFailed(self: *Conn, e: anyerror) Error {
        if (e == error.OutOfMemory) return error.OutOfMemory;
        const why = krb5.lastError();
        return self.loginRefused("kerberos: {s}: {s}", .{ @errorName(e), if (why.len > 0) why else "no further detail" });
    }

    fn loginRefused(self: *Conn, comptime fmt: []const u8, args: anytype) Error {
        try self.wire.printError(fmt, args);
        return error.LoginFailed;
    }

    /// The server's SSPI token (0xED), a US_VARBYTE security blob. Returns a slice into
    /// the message, valid until the next `readMessage`.
    fn sspiToken(self: *Conn) ![]const u8 {
        var r: Body = .{ .p = self.wire.msg.items };
        while (try nextToken(&r)) |t| switch (t.id) {
            0xED => return t.body,
            0xAA => return self.failWith(t.body, error.LoginFailed),
            else => {},
        };
        return error.TdsProtocol;
    }

    /// Also notes the response's SSPI token, if any, in `sspi_reply`.
    fn parseLoginResponse(self: *Conn) !void {
        var r: Body = .{ .p = self.wire.msg.items };
        var ok = false;
        self.sspi_reply = "";
        while (try nextToken(&r)) |t| switch (t.id) {
            0xAD => ok = true,
            0xAA => return self.failWith(t.body, error.LoginFailed),
            0xED => self.sspi_reply = t.body,
            0xE3 => if (parseEnvPacketSize(t.body)) |sz| {
                self.pkt_payload = sz - 8;
            },
            else => {},
        };
        if (!ok) return error.LoginFailed;
    }

    fn sendBatch(self: *Conn, stmt: []const u8) !void {
        var payload = std.array_list.Managed(u8).init(self.wire.gpa);
        defer payload.deinit();
        try payload.appendSlice(&[_]u8{
            22,   0,    0, 0,
            18,   0,    0, 0,
            0x02, 0x00, 0, 0,
            0,    0,    0, 0,
            0,    0,    1, 0,
            0,    0,
        });
        @memcpy(payload.items[10..18], &self.txn_desc);
        const text = try utf16.encodeLeAlloc(self.wire.gpa, stmt);
        defer self.wire.gpa.free(text);
        try payload.appendSlice(text);
        try self.writePacket(PKT_SQLBATCH, payload.items);
    }

    pub fn bulkStart(self: *Conn, insert_bulk_sql: []const u8) !void {
        try self.sendBatch(insert_bulk_sql);
        try self.readBulkResponse();
    }

    /// One packet of a bulk load into the socket's buffer. In the clear it is flushed
    /// only with the last (EOM), so a segment goes out in as few writes as the buffer
    /// allows; under TLS every packet is flushed, as SQL Server drops a TLS bulk load
    /// whose packets were held back (`ConnectionIoFailed` in the volume test).
    pub fn bulkPacket(self: *Conn, status: u8, payload: []const u8) !void {
        var header: [8]u8 = .{ PKT_BULK, status, 0, 0, 0, 0, 0, 0 };
        const total: u16 = @intCast(payload.len + 8);
        header[2] = @intCast(total >> 8);
        header[3] = @intCast(total & 0xff);
        const w = self.wire.wr();
        try w.writeAll(&header);
        try w.writeAll(payload);
        if (self.wire.tls != null or status & STATUS_EOM != 0) try self.wire.flush();
    }

    /// Returns the DONE row count, the server's word on how many rows actually landed.
    pub fn bulkFinish(self: *Conn) !?u64 {
        self.last_done_count = null;
        try self.readBulkResponse();
        return self.last_done_count;
    }

    fn readBulkResponse(self: *Conn) !void {
        try self.readMessage();
        try self.scanResultTokens();
    }
};

const TlsShim = struct {
    inner_r: *std.Io.Reader,
    inner_w: *std.Io.Writer,
    handshaking: bool = true,
    remaining: usize = 0,
    reader: std.Io.Reader,
    writer: std.Io.Writer,
    rbuf: [shim_buf_len]u8 = undefined,
    wbuf: [shim_buf_len]u8 = undefined,

    const shim_buf_len = std.crypto.tls.max_ciphertext_record_len;
    const reader_vtable = std.Io.Reader.VTable{ .stream = readStream };
    const writer_vtable = std.Io.Writer.VTable{ .drain = drainFn };

    fn readStream(r: *std.Io.Reader, w: *std.Io.Writer, limit: std.Io.Limit) std.Io.Reader.StreamError!usize {
        const self: *TlsShim = @fieldParentPtr("reader", r);
        if (self.handshaking) {
            while (self.remaining == 0) {
                const hdr = self.inner_r.peek(8) catch |e| switch (e) {
                    error.EndOfStream => return error.EndOfStream,
                    else => return error.ReadFailed,
                };
                const len: usize = (@as(usize, hdr[2]) << 8) | hdr[3];
                if (len < 8) return error.ReadFailed;
                self.inner_r.toss(8);
                self.remaining = len - 8;
            }
        }
        if (self.inner_r.buffered().len == 0) {
            _ = self.inner_r.peek(1) catch |e| switch (e) {
                error.EndOfStream => return error.EndOfStream,
                else => return error.ReadFailed,
            };
        }
        const avail = self.inner_r.buffered();
        var cap = limit.minInt(avail.len);
        if (self.handshaking) cap = @min(cap, self.remaining);
        const n = try w.write(avail[0..cap]);
        self.inner_r.toss(n);
        if (self.handshaking) self.remaining -= n;
        return n;
    }

    fn drainFn(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *TlsShim = @fieldParentPtr("writer", w);
        var total: usize = 0;
        total += try self.put(w.buffered());
        for (data[0 .. data.len - 1]) |d| total += try self.put(d);
        const last = data[data.len - 1];
        for (0..splat) |_| total += try self.put(last);
        if (self.handshaking) {
            self.inner_w.flush() catch return error.WriteFailed;
        }
        return w.consume(total);
    }

    fn put(self: *TlsShim, bytes: []const u8) std.Io.Writer.Error!usize {
        if (bytes.len == 0) return 0;
        if (!self.handshaking) {
            self.inner_w.writeAll(bytes) catch return error.WriteFailed;
            return bytes.len;
        }
        var off: usize = 0;
        while (off < bytes.len) {
            const chunk: usize = @min(bytes.len - off, BULK_PKT_PAYLOAD);
            const is_last = off + chunk == bytes.len;
            var header: [8]u8 = .{ PKT_PRELOGIN, if (is_last) STATUS_EOM else 0x00, 0, 0, 0, 0, 0, 0 };
            const tot: u16 = @intCast(chunk + 8);
            header[2] = @intCast(tot >> 8);
            header[3] = @intCast(tot & 0xff);
            self.inner_w.writeAll(&header) catch return error.WriteFailed;
            self.inner_w.writeAll(bytes[off..][0..chunk]) catch return error.WriteFailed;
            off += chunk;
        }
        return bytes.len;
    }
};

const sql_vtable = sql.connVTable(Conn);

const cursor_vtable = sql.Cursor.VTable{ .schema = curSchema, .nextBatch = curNext, .close = curClose, .last_error = curLastError };

fn curLastError(ptr: *anyopaque) []const u8 {
    const self: *TdsCursor = @ptrCast(@alignCast(ptr));
    return self.conn.wire.last_error;
}

fn curSchema(ptr: *anyopaque) types.Schema {
    const self: *TdsCursor = @ptrCast(@alignCast(ptr));
    return self.schema.*;
}
fn curNext(ptr: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
    const self: *TdsCursor = @ptrCast(@alignCast(ptr));
    return self.fetchBatch(arena);
}
fn curClose(ptr: *anyopaque) void {
    const self: *TdsCursor = @ptrCast(@alignCast(ptr));
    self.closeCursor();
}

/// SQL Server's largest (max) or text value, 2^31 - 1 bytes.
const max_lob: usize = (1 << 31) - 1;
/// A non-rowset reply (login, a batch's messages, a bulk load's DONE) buffered whole.
const max_message: usize = 1 << 30;

const PacketReader = struct {
    r: *std.Io.Reader,
    buf: std.array_list.Managed(u8),
    pos: usize = 0,
    eom: bool = false,

    fn init(gpa: std.mem.Allocator, r: *std.Io.Reader) PacketReader {
        return .{ .r = r, .buf = std.array_list.Managed(u8).init(gpa) };
    }
    fn deinit(self: *PacketReader) void {
        self.buf.deinit();
    }

    fn ensure(self: *PacketReader) !bool {
        while (self.pos >= self.buf.items.len) {
            if (self.eom) return false;
            const r = self.r;
            var hdr: [8]u8 = undefined;
            try r.readSliceAll(&hdr);
            const len: usize = (@as(usize, hdr[2]) << 8) | hdr[3];
            if (len < 8) return error.TdsProtocol;
            self.eom = (hdr[1] & STATUS_EOM) != 0;
            try self.buf.resize(len - 8);
            try r.readSliceAll(self.buf.items);
            self.pos = 0;
        }
        return true;
    }

    fn readByte(self: *PacketReader) !u8 {
        if (self.pos >= self.buf.items.len and !try self.ensure()) return error.EndOfMessage;
        const b = self.buf.items[self.pos];
        self.pos += 1;
        return b;
    }
    fn readBytes(self: *PacketReader, dst: []u8) !void {
        if (self.pos + dst.len <= self.buf.items.len) {
            @memcpy(dst, self.buf.items[self.pos..][0..dst.len]);
            self.pos += dst.len;
            return;
        }
        var off: usize = 0;
        while (off < dst.len) {
            if (!try self.ensure()) return error.EndOfMessage;
            const avail = self.buf.items.len - self.pos;
            const take = @min(avail, dst.len - off);
            @memcpy(dst[off..][0..take], self.buf.items[self.pos..][0..take]);
            self.pos += take;
            off += take;
        }
    }
    fn readU16(self: *PacketReader) !u16 {
        if (self.pos + 2 <= self.buf.items.len) {
            const v = std.mem.readInt(u16, self.buf.items[self.pos..][0..2], .little);
            self.pos += 2;
            return v;
        }
        var b: [2]u8 = undefined;
        try self.readBytes(&b);
        return @as(u16, b[0]) | (@as(u16, b[1]) << 8);
    }
    fn readU32(self: *PacketReader) !u32 {
        var b: [4]u8 = undefined;
        try self.readBytes(&b);
        return std.mem.readInt(u32, &b, .little);
    }
    fn readU64(self: *PacketReader) !u64 {
        var b: [8]u8 = undefined;
        try self.readBytes(&b);
        return std.mem.readInt(u64, &b, .little);
    }
    /// PLP (Partially Length-Prefixed), how (max) types are framed: an 8-byte total
    /// (all ones = NULL, else ignored), then 4-byte-prefixed chunks until a 0-length one.
    /// A total or chunks past `max_lob` are refused; a declared total may also be the
    /// unknown-length marker.
    fn readPlp(self: *PacketReader, arena: std.mem.Allocator) !?[]u8 {
        const total = try self.readU64();
        if (total == 0xFFFFFFFFFFFFFFFF) return null;
        if (total != 0xFFFFFFFFFFFFFFFE and total > max_lob) return error.TdsProtocol;
        var buf = std.array_list.Managed(u8).init(arena);
        while (true) {
            const chunk = try self.readU32();
            if (chunk == 0) break;
            if (chunk > max_lob - buf.items.len) return error.TdsProtocol;
            try self.readGrowing(&buf, chunk);
        }
        return try buf.toOwnedSlice();
    }
    fn readSlice(self: *PacketReader, arena: std.mem.Allocator, n: usize) ![]u8 {
        const s = try arena.alloc(u8, n);
        try self.readBytes(s);
        return s;
    }
    /// A text, ntext or image value of `n` bytes, which the server announces up front.
    fn readLob(self: *PacketReader, arena: std.mem.Allocator, n: usize) ![]u8 {
        if (n > max_lob) return error.TdsProtocol;
        var buf = std.array_list.Managed(u8).init(arena);
        try self.readGrowing(&buf, n);
        return try buf.toOwnedSlice();
    }
    /// Appends `n` bytes, growing `buf` only as they arrive, so a length the server
    /// merely claims reserves nothing up front.
    fn readGrowing(self: *PacketReader, buf: *std.array_list.Managed(u8), n: usize) !void {
        var left = n;
        while (left > 0) {
            const step = @min(left, 1 << 20);
            const start = buf.items.len;
            try buf.resize(start + step);
            try self.readBytes(buf.items[start..]);
            left -= step;
        }
    }
    fn skip(self: *PacketReader, n: usize) !void {
        var rem = n;
        while (rem > 0) {
            if (!try self.ensure()) return error.EndOfMessage;
            const avail = self.buf.items.len - self.pos;
            const take = @min(avail, rem);
            self.pos += take;
            rem -= take;
        }
    }
};

const TdsCursor = struct {
    gpa: std.mem.Allocator,
    conn: *Conn,
    reader: PacketReader,
    meta_arena: std.heap.ArenaAllocator,
    cols: []ColumnDesc,
    schema: *types.Schema,
    done: bool,
    full: bool = false,
    cell: [0xFFFF]u8 = undefined,
    nullbuf: [512]u8 = undefined,

    fn readHeader(self: *TdsCursor) !void {
        const ma = self.meta_arena.allocator();
        const empty = try ma.create(types.Schema);
        empty.* = .{ .fields = &.{} };
        self.schema = empty;
        while (true) {
            const token = self.reader.readByte() catch |e| {
                if (e == error.EndOfMessage) {
                    self.done = true;
                    return;
                }
                return e;
            };
            switch (token) {
                0x81 => return self.parseColMeta(ma),
                0xAA => try self.handleError(),
                0xAB, 0xE3, 0xA9, 0xA4, 0xA5 => try self.reader.skip(try self.reader.readU16()),
                0x79 => try self.reader.skip(4),
                0xFD, 0xFE, 0xFF => {
                    if (try self.doneMore()) continue;
                    self.done = true;
                    return;
                },
                else => return error.TdsProtocol,
            }
        }
    }

    /// After the rowset the batch is run to its end, so a later statement executes and
    /// its error is seen; closing early would have SQL Server abandon it silently.
    /// Once a batch fills, the next one's columns are sized for a full batch up front.
    fn fetchBatch(self: *TdsCursor, arena: std.mem.Allocator) !?Batch {
        if (self.done) return null;
        const ncol = self.cols.len;
        if (ncol == 0) return null;
        const builders = try arena.alloc(column.Builder, ncol);
        for (self.cols, builders) |c, *b| b.* = if (self.full)
            try column.Builder.initCapacity(arena, c.engine_type, sql.STREAM_ROWS)
        else
            column.Builder.init(arena, c.engine_type);

        var n: usize = 0;
        while (n < sql.STREAM_ROWS) {
            const token = self.reader.readByte() catch |e| {
                if (e == error.EndOfMessage) return error.TdsProtocol;
                return e;
            };
            switch (token) {
                0xD1 => {
                    try self.parseRow(arena, builders, false);
                    n += 1;
                },
                0xD2 => {
                    try self.parseRow(arena, builders, true);
                    n += 1;
                },
                0xAB, 0xE3, 0xA9, 0xA4, 0xA5 => try self.reader.skip(try self.reader.readU16()),
                0x79 => try self.reader.skip(4),
                0xFD, 0xFE, 0xFF => {
                    if (try self.doneMore()) try self.drain();
                    self.done = true;
                    break;
                },
                0xAA => try self.handleError(),
                else => return error.TdsProtocol,
            }
        }
        self.full = n == sql.STREAM_ROWS;
        if (n == 0) return null;
        const out = try arena.alloc(column.Column, ncol);
        for (builders, 0..) |*b, k| out[k] = try b.finish();
        return .{ .schema = self.schema, .columns = out, .len = n };
    }

    /// Whether more results follow in the batch (the DONE_MORE bit of the status).
    fn doneMore(self: *TdsCursor) !bool {
        const status = try self.reader.readU16();
        try self.reader.skip(10);
        return status & 0x0001 != 0;
    }

    /// Statements with no rows run to their end; an error in one fails the read; a
    /// second rowset is refused (`sql.one_result_set`).
    fn drain(self: *TdsCursor) !void {
        while (true) {
            const token = self.reader.readByte() catch |e| {
                if (e == error.EndOfMessage) return;
                return e;
            };
            switch (token) {
                0xFD, 0xFE, 0xFF => if (!try self.doneMore()) return,
                0xAA => try self.handleError(),
                0xAB, 0xE3, 0xA9, 0xA4, 0xA5 => try self.reader.skip(try self.reader.readU16()),
                0x79 => try self.reader.skip(4),
                0x81 => {
                    try self.conn.wire.setError(sql.one_result_set);
                    return error.QueryFailed;
                },
                else => return error.TdsProtocol,
            }
        }
    }

    fn parseColMeta(self: *TdsCursor, ma: std.mem.Allocator) !void {
        const count = try self.reader.readU16();
        if (count == 0xFFFF) return;
        self.cols = try ma.alloc(ColumnDesc, count);
        const fields = try ma.alloc(types.Schema.Field, count);
        for (0..count) |k| {
            try self.reader.skip(6);
            var d = try self.parseTypeInfo();
            d.decode = decodeOf(d);
            const namelen: usize = try self.reader.readByte();
            const nbytes = try self.reader.readSlice(ma, namelen * 2);
            d.name = try utf16.decodeLeAlloc(.replacement, ma, nbytes);
            self.cols[k] = d;
            fields[k] = .{ .name = d.name, .ty = d.engine_type };
        }
        const sch = try ma.create(types.Schema);
        sch.* = .{ .fields = fields };
        self.schema = sch;
    }

    fn parseTypeInfo(self: *TdsCursor) !ColumnDesc {
        const intT = types.Type.init(.int).asNullable();
        const boolT = types.Type.init(.bool).asNullable();
        const floatT = types.Type.init(.float).asNullable();
        const strT = types.Type.init(.string).asNullable();
        const dateT = types.Type.init(.date).asNullable();
        const tsT = types.Type.init(.timestamp).asNullable();

        const t = try self.reader.readByte();
        var d = ColumnDesc{ .tds_type = t, .engine_type = strT, .kind = .bytelen };
        switch (t) {
            0x38 => {
                d.kind = .fixed;
                d.fixed_len = 4;
                d.engine_type = intT;
            },
            0x34 => {
                d.kind = .fixed;
                d.fixed_len = 2;
                d.engine_type = intT;
            },
            0x30 => {
                d.kind = .fixed;
                d.fixed_len = 1;
                d.engine_type = intT;
            },
            0x7F => {
                d.kind = .fixed;
                d.fixed_len = 8;
                d.engine_type = intT;
            },
            0x32 => {
                d.kind = .fixed;
                d.fixed_len = 1;
                d.engine_type = boolT;
            },
            0x3B => {
                d.kind = .fixed;
                d.fixed_len = 4;
                d.engine_type = floatT;
            },
            0x3E => {
                d.kind = .fixed;
                d.fixed_len = 8;
                d.engine_type = floatT;
            },
            0x3D => {
                d.kind = .fixed;
                d.fixed_len = 8;
                d.engine_type = tsT;
            },
            0x3A => {
                d.kind = .fixed;
                d.fixed_len = 4;
                d.engine_type = tsT;
            },
            0x26 => {
                _ = try self.reader.readByte();
                d.engine_type = intT;
            },
            0x68 => {
                _ = try self.reader.readByte();
                d.engine_type = boolT;
            },
            0x6D => {
                _ = try self.reader.readByte();
                d.engine_type = floatT;
            },
            0x6E => {
                const ml = try self.reader.readByte();
                d.is_money = true;
                d.scale = 4;
                d.engine_type = types.Type.decimal(if (ml == 4) 10 else 19, 4).asNullable();
            },
            0x3C, 0x7A => {
                d.kind = .fixed;
                d.fixed_len = if (t == 0x7A) 4 else 8;
                d.is_money = true;
                d.scale = 4;
                d.engine_type = types.Type.decimal(if (t == 0x7A) 10 else 19, 4).asNullable();
            },
            0x6F => {
                _ = try self.reader.readByte();
                d.engine_type = tsT;
            },
            0x6A, 0x6C => {
                _ = try self.reader.readByte();
                const prec = try self.reader.readByte();
                const scale = try self.reader.readByte();
                d.scale = scale;
                d.engine_type = types.Type.decimal(prec, scale).asNullable();
            },
            0x24 => {
                _ = try self.reader.readByte();
                d.engine_type = strT;
                d.is_guid = true;
            },
            0x28 => {
                d.engine_type = dateT;
            },
            0x29 => {
                d.scale = try self.reader.readByte();
                if (d.scale > 7) return error.InvalidTdsScale;
                d.engine_type = types.Type.init(.time).asNullable();
            },
            0x2A, 0x2B => {
                d.scale = try self.reader.readByte();
                if (d.scale > 7) return error.InvalidTdsScale;
                d.engine_type = tsT;
            },
            0xA7, 0xAF => {
                const ml = try self.reader.readU16();
                try self.reader.skip(5);
                d.kind = if (ml == 0xFFFF) .plp else .ushortlen;
                d.engine_type = strT;
            },
            0xE7, 0xEF => {
                const ml = try self.reader.readU16();
                try self.reader.skip(5);
                d.kind = if (ml == 0xFFFF) .plp else .ushortlen;
                d.engine_type = strT;
                d.is_unicode = true;
            },
            0xA5, 0xAD => {
                const ml = try self.reader.readU16();
                d.kind = if (ml == 0xFFFF) .plp else .ushortlen;
                d.engine_type = strT;
                d.is_binary = true;
            },
            0xF1 => {
                if (try self.reader.readByte() != 0) {
                    try self.reader.skip(@as(usize, try self.reader.readByte()) * 2);
                    try self.reader.skip(@as(usize, try self.reader.readByte()) * 2);
                    try self.reader.skip(@as(usize, try self.reader.readU16()) * 2);
                }
                d.kind = .plp;
                d.engine_type = strT;
                d.is_unicode = true;
            },
            0x22, 0x23, 0x63 => {
                _ = try self.reader.readU32();
                if (t == 0x23 or t == 0x63) try self.reader.skip(5);
                var parts = try self.reader.readByte();
                while (parts > 0) : (parts -= 1) try self.reader.skip(@as(usize, try self.reader.readU16()) * 2);
                d.kind = .textptr;
                d.engine_type = strT;
                if (t == 0x63) d.is_unicode = true;
                if (t == 0x22) d.is_binary = true;
            },
            else => return error.UnsupportedTdsType,
        }
        return d;
    }

    fn parseRow(self: *TdsCursor, arena: std.mem.Allocator, builders: []column.Builder, nbc: bool) !void {
        var nullbits: []const u8 = &.{};
        if (nbc) {
            const nbytes = (self.cols.len + 7) / 8;
            nullbits = if (nbytes <= self.nullbuf.len) blk: {
                try self.reader.readBytes(self.nullbuf[0..nbytes]);
                break :blk self.nullbuf[0..nbytes];
            } else try self.reader.readSlice(arena, nbytes);
        }
        for (self.cols, builders, 0..) |col, *b, ci| {
            if (nbc and (nullbits[ci / 8] >> @intCast(ci % 8)) & 1 != 0) {
                try b.appendNull();
                continue;
            }
            if (col.decode == .plain_text) {
                try self.readPlainText(b, col);
                continue;
            }
            const raw = (try self.readRaw(arena, col)) orelse {
                try b.appendNull();
                continue;
            };
            try appendCell(b, col, raw, arena);
        }
    }

    /// A varchar or char cell read from the packet straight into the column, then
    /// re-encoded from the cell buffer only when it holds a byte above 0x7F.
    fn readPlainText(self: *TdsCursor, b: *column.Builder, col: ColumnDesc) !void {
        const len: usize = switch (col.kind) {
            .bytelen => try self.reader.readByte(),
            else => try self.reader.readU16(),
        };
        if ((col.kind == .bytelen and len == 0) or (col.kind == .ushortlen and len == 0xFFFF)) return b.append(.null);
        const dst = try b.reserveBytes(len);
        try self.reader.readBytes(dst);
        if (charset.isAscii(dst)) return b.commitBytes(len);
        const raw = self.cell[0..len];
        @memcpy(raw, dst);
        const out = try b.reserveBytes(charset.decodedLen(.cp1252, raw));
        return b.commitBytes(charset.decodeInto(.cp1252, out, raw));
    }

    /// Fixed and length-prefixed cells, never longer than the cell buffer, are read
    /// into it; `appendRaw` copies whatever it keeps.
    fn readCell(self: *TdsCursor, len: usize) ![]u8 {
        try self.reader.readBytes(self.cell[0..len]);
        return self.cell[0..len];
    }

    /// A cell's bytes as the wire holds them, null for NULL; a short one is in the
    /// cell buffer until the next read.
    fn readRaw(self: *TdsCursor, arena: std.mem.Allocator, col: ColumnDesc) !?[]const u8 {
        switch (col.kind) {
            .fixed => return try self.readCell(col.fixed_len),
            .bytelen => {
                const len: usize = try self.reader.readByte();
                if (len == 0) return null;
                return try self.readCell(len);
            },
            .ushortlen => {
                const len = try self.reader.readU16();
                if (len == 0xFFFF) return null;
                return try self.readCell(len);
            },
            .plp => return self.reader.readPlp(arena),
            .textptr => {
                const ptrlen = try self.reader.readByte();
                if (ptrlen == 0) return null;
                try self.reader.skip(@as(usize, ptrlen) + 8);
                const dlen = try self.reader.readU32();
                return try self.reader.readLob(arena, dlen);
            },
        }
    }

    fn handleError(self: *TdsCursor) !void {
        const len = try self.reader.readU16();
        const body = try self.reader.readSlice(self.gpa, len);
        defer self.gpa.free(body);
        return self.conn.failWith(body, error.QueryFailed);
    }

    fn closeCursor(self: *TdsCursor) void {
        self.reader.deinit();
        self.meta_arena.deinit();
        self.conn.close();
        self.gpa.destroy(self);
    }
};

const NVARCHAR_MAX_BYTES = 8000;
const BULK_COLLATION = [5]u8{ 0x09, 0x04, 0xD0, 0x00, 0x34 };

const BulkProto = struct {
    pub const Connection = Conn;
    pub const dialect: sql.Dialect = .sqlserver;
    pub const stmt = "INSERT BULK";

    pub fn command(arena: std.mem.Allocator, qtable: []const u8, schema: types.Schema) ![]const u8 {
        var cols = std.array_list.Managed(u8).init(arena);
        for (schema.fields, 0..) |f, i| {
            if (i > 0) try cols.appendSlice(", ");
            try cols.appendSlice(try sql.quoteName(arena, .sqlserver, f.name));
            try cols.appendSlice(" nvarchar(4000)");
        }
        return std.fmt.allocPrint(arena, "INSERT BULK {s} ({s})", .{ qtable, cols.items });
    }

    pub fn segmentHeader(w: anytype, gpa: std.mem.Allocator, schema: types.Schema) !void {
        try w.writeByte(0x81);
        try writeU16(w, @intCast(schema.fields.len));
        for (schema.fields) |f| {
            try writeU32(w, 0);
            try writeU16(w, 0x0009);
            try w.writeByte(0xE7);
            try writeU16(w, NVARCHAR_MAX_BYTES);
            try w.writeAll(&BULK_COLLATION);
            const name16 = try utf16.encodeLeAlloc(gpa, f.name);
            defer gpa.free(name16);
            try w.writeByte(@intCast(name16.len / 2));
            try w.writeAll(name16);
        }
    }

    /// Each cell's text is transcoded through one UTF-16 buffer, grown to the
    /// longest cell of the batch.
    pub fn appendBatch(w: anytype, arena: std.mem.Allocator, batch: Batch) !void {
        var units = std.array_list.Managed(u8).init(arena);
        var buf: [celltext.scalar_bound]u8 = undefined;
        for (0..batch.len) |r| {
            try w.writeByte(0xD1);
            for (batch.columns, batch.schema.fields) |*col, f| {
                if (celltext.isNull(col, r)) {
                    try writeU16(w, 0xFFFF);
                    continue;
                }
                const s = celltext.text(col, r, &buf, sql.BulkText(.{ .bool_true = "1", .bool_false = "0" }){});
                try units.resize(2 * s.len);
                const le = units.items[0 .. utf16.encodeLe(units.items, s) catch return notUtf8(f.name)];
                const blen = utf16.cutLe(le, NVARCHAR_MAX_BYTES);
                try writeU16(w, @intCast(blen));
                try w.writeAll(le[0..blen]);
            }
        }
    }

    pub fn send(conn: *Conn, cmd: []const u8, data: []const u8, rows: u64) !u64 {
        try conn.bulkStart(cmd);
        var off: usize = 0;
        while (data.len - off > conn.pkt_payload) {
            try conn.bulkPacket(0x00, data[off .. off + conn.pkt_payload]);
            off += conn.pkt_payload;
        }
        try conn.bulkPacket(STATUS_EOM, data[off..]);
        return (try conn.bulkFinish()) orelse rows;
    }
};

pub const BulkSink = sql.BulkSink(BulkProto);

fn textBatch(a: std.mem.Allocator, kind: types.TypeKind, texts: []const []const u8) !Batch {
    const ty = types.Type.init(kind).asNullable();
    var b = column.Builder.init(a, ty);
    for (texts) |t| try b.append(if (kind == .bytes) .{ .bytes = t } else .{ .string = t });
    const cols = try a.alloc(column.Column, 1);
    cols[0] = try b.finish();
    const schema = try a.create(types.Schema);
    schema.* = .{ .fields = try a.dupe(types.Schema.Field, &.{.{ .name = "c", .ty = ty }}) };
    return .{ .schema = schema, .columns = cols, .len = texts.len };
}

inline fn wide(comptime s: []const u8) []const u8 {
    @setEvalBranchQuota(1_000_000);
    return comptime std.mem.sliceAsBytes(std.unicode.utf8ToUtf16LeStringLiteral(s));
}

test "INSERT BULK rows of every kind" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const batches = try celltext.goldenBatches(a);
    const got = try a.alloc([]const u8, batches.len);
    for (batches, got) |batch, *g| {
        var w = std.Io.Writer.Allocating.init(a);
        try BulkProto.appendBatch(&w.writer, a, batch);
        g.* = w.written();
    }
    const tiny = celltext.golden_tiny;
    try celltext.expectGolden(&.{
        "\xd1\x02\x00" ++ wide("1") ++ "\xd1\x02\x00" ++ wide("0") ++ "\xd1\xff\xff",
        "\xd1\x02\x00" ++ wide("0") ++ "\xd1\x04\x00" ++ wide("-1") ++ "\xd1\x28\x00" ++ wide("-9223372036854775808") ++ "\xd1\x26\x00" ++ wide("9223372036854775807") ++ "\xd1\xff\xff",
        "\xd1\x06\x00" ++ wide("0.1") ++ "\xd1\x02\x00" ++ wide("0") ++ "\xd1\x04\x00" ++ wide("-0") ++ "\xd1\x8c\x02" ++ wide(tiny) ++ "\xd1\x2c\x00" ++ wide("1000000000000000000000") ++ "\xd1\x0a\x00" ++ wide("-2.25") ++ "\xd1\x06\x00" ++ wide("nan") ++ "\xd1\x06\x00" ++ wide("inf") ++ "\xd1\x08\x00" ++ wide("-inf") ++ "\xd1\xff\xff",
        "\xd1\x0c\x00" ++ wide("123.45") ++ "\xd1\x0e\x00" ++ wide("-0.0007") ++ "\xd1\x02\x00" ++ wide("5") ++ "\xd1\x0e\x00" ++ wide("0.12345") ++ "\xd1\x2c\x00" ++ wide("-18446744073709.551615") ++ "\xd1\x08\x00" ++ wide("0.00") ++ "\xd1\xff\xff",
        "\xd1\x00\x00\xd1\x0a\x00" ++ wide("plain") ++ "\xd1\x10\x00" ++ wide("tab\there") ++ "\xd1\x14\x00" ++ wide("line\nbreak") ++ "\xd1\x0e\x00" ++ wide("cr\rlf\r\n") ++ "\xd1\x10\x00" ++ wide("say \"hi\"") ++ "\xd1\x0e\x00" ++ wide("O'Brien") ++ "\xd1\x16\x00" ++ wide("back\\slash\\") ++ "\xd1\x08\x00" ++ wide("a\\Nb") ++ "\xd1\x06\x00" ++ wide("\\N ") ++ "\xd1\x08\x00" ++ wide("NULL") ++ "\xd1\x0a\x00" ++ wide("a\x01b\x02c") ++ "\xd1\x1a\x00" ++ wide("a,b;c:d.e-1 0") ++ "\xd1\x08\x00" ++ wide("\xc3\xb3 \xf0\x9f\x98\x80") ++ "\xd1\x08\x00" ++ wide("\x00nul") ++ "\xd1\x2e\x00" ++ wide("sixteen bytes and more\x01") ++ "\xd1\xff\xff",
        "\xd1\x00\x00\xd1\x0a\x00" ++ wide("plain") ++ "\xd1\x10\x00" ++ wide("\x01\x02 \\ \t\n\"") ++ "\xd1\x02\x00" ++ wide("\xc3\xb3") ++ "\xd1\xff\xff",
        "\xd1\x14\x00" ++ wide("1970-01-01") ++ "\xd1\x14\x00" ++ wide("2024-01-01") ++ "\xd1\x14\x00" ++ wide("0000-01-01") ++ "\xd1\x1c\x00" ++ wide("-5877641-06-24") ++ "\xd1\xff\xff",
        "\xd1\x10\x00" ++ wide("00:00:00") ++ "\xd1\x1e\x00" ++ wide("01:02:03.000001") ++ "\xd1\x1e\x00" ++ wide("23:59:59.999999") ++ "\xd1\xff\xff",
        "\xd1\x26\x00" ++ wide("1970-01-01 00:00:00") ++ "\xd1\x34\x00" ++ wide("2024-01-01 00:00:00.123456") ++ "\xd1\x34\x00" ++ wide("1969-12-31 23:59:59.999999") ++ "\xd1\x26\x00" ++ wide("0001-01-01 00:00:00") ++ "\xd1\xff\xff",
        "\xd1\xff\xff\xd1\xff\xff",
        "\xd1\x02\x00" ++ wide("1") ++ "\x06\x00" ++ wide("a b") ++ "\x08\x00" ++ wide("2.50") ++ "\xd1\xff\xff\xff\xff\xff\xff\xd1\x04\x00" ++ wide("-2") ++ "\x00\x00\x08\x00" ++ wide("-0.1"),
    }, got);
}

test "INSERT BULK cuts a long cell before a split surrogate pair and refuses invalid UTF-8, never repairs it (D7)" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const emoji = "\xf0\x9f\x98\x80";
    var texts: [4][]const u8 = undefined;
    for (&texts, 3997..) |*t, lead| {
        const s = try a.alloc(u8, lead + emoji.len * 3);
        @memset(s[0..lead], 'x');
        for (0..3) |k| @memcpy(s[lead + k * emoji.len ..][0..emoji.len], emoji);
        t.* = s;
    }
    var got = std.array_list.Managed(u8).init(a);
    try BulkProto.appendBatch(got.writer(), a, try textBatch(a, .string, &texts));
    const kept = [_]struct { usize, []const u8 }{
        .{ 7998, "x\x00=\xd8\x00\xde" },
        .{ 8000, "x\x00=\xd8\x00\xde" },
        .{ 7998, "x\x00x\x00" },
        .{ 8000, "x\x00x\x00" },
    };
    var off: usize = 0;
    for (kept) |k| {
        try std.testing.expectEqual(@as(u8, 0xD1), got.items[off]);
        try std.testing.expectEqual(k[0], std.mem.readInt(u16, got.items[off + 1 ..][0..2], .little));
        off += 3 + k[0];
        try std.testing.expectEqualSlices(u8, k[1], got.items[off - k[1].len .. off]);
    }
    try std.testing.expectEqual(got.items.len, off);
    try std.testing.expectError(error.InvalidUtf8, BulkProto.appendBatch(got.writer(), a, try textBatch(a, .bytes, &.{ "ok", "\xff\xfe bad" })));
    try std.testing.expectError(error.InvalidUtf8, BulkProto.appendBatch(got.writer(), a, try textBatch(a, .bytes, &.{"\xc3"})));
    try std.testing.expectEqualStrings("column `c` holds text that is not valid UTF-8, which SQL Server's nvarchar cannot take unaltered; repair or drop it before the load", eval.takeFailure(error.InvalidUtf8).?);
}

fn writeU16(w: anytype, v: u16) !void {
    try w.writeByte(@intCast(v & 0xff));
    try w.writeByte(@intCast(v >> 8));
}
fn writeU32(w: anytype, v: u32) !void {
    var b: [4]u8 = undefined;
    std.mem.writeInt(u32, &b, v, .little);
    try w.writeAll(&b);
}

const ColKind = enum { fixed, bytelen, ushortlen, plp, textptr };
const ColumnDesc = struct {
    name: []const u8 = "",
    tds_type: u8,
    engine_type: types.Type,
    kind: ColKind,
    fixed_len: u8 = 0,
    scale: u8 = 0,
    is_unicode: bool = false,
    is_guid: bool = false,
    is_binary: bool = false,
    is_money: bool = false,
    decode: Decode = .raw_text,
};

/// How a column's cells reach its builder, chosen once from its metadata
/// (`decodeOf`); each is an `appendRaw` instantiated at compile time.
/// `plain_text` is a varchar read from the packet straight into its column.
const Decode = enum { int, bool, float, decimal, money, date, time, datetime, plain_text, win1252, unicode_text, guid, hex, bytes, raw_text };

fn decodeOf(d: ColumnDesc) Decode {
    return switch (d.engine_type.kind) {
        .int => .int,
        .bool => .bool,
        .float => .float,
        .decimal => if (d.is_money) .money else .decimal,
        .date => .date,
        .time => .time,
        .timestamp => .datetime,
        .string => if (d.is_guid) .guid else if (d.is_binary) .hex else if (d.is_unicode) .unicode_text else if (d.kind == .bytelen or d.kind == .ushortlen) .plain_text else .win1252,
        .bytes => .bytes,
        else => .raw_text,
    };
}

/// A cell's wire bytes into its column's storage as `c` decodes them, with no
/// `Value` between.
fn appendRaw(comptime c: Decode, b: *column.Builder, d: ColumnDesc, raw: []const u8, arena: std.mem.Allocator) !void {
    switch (c) {
        .int => try b.appendInt(readIntLE(raw)),
        .bool => try b.appendAs(.b, raw.len > 0 and raw[0] != 0),
        .float => try b.appendFloat(if (raw.len == 4) @as(f64, @as(f32, @bitCast(@as(u32, @truncate(readULE(raw)))))) else @bitCast(readULE(raw))),
        .decimal => try b.appendDecimal(try decodeDecimal(d, raw)),
        .money => try b.appendDecimal(try decodeMoney(raw)),
        .date => if (std.math.cast(i32, @as(i64, @bitCast(readULE(raw))) + civil.epoch.ce1)) |days| try b.appendAs(.i32, days) else try b.appendNull(),
        .time => try b.appendInt(try decodeTime(d, raw)),
        .datetime => try b.appendInt(try decodeDateTime(d, raw)),
        .plain_text, .win1252 => {
            if (charset.isAscii(raw)) return b.appendStr(raw);
            return b.commitBytes(charset.decodeInto(.cp1252, try b.reserveBytes(charset.decodedLen(.cp1252, raw)), raw));
        },
        .unicode_text => try b.commitBytes(utf16.decodeLe(.fail, try b.reserveBytes(utf16.decodedBound(raw.len)), raw) catch |e| return notUtf16(d.name, e)),
        .guid => try b.appendStr(if (raw.len < 16) "" else &uuid.text(raw[0..16], .mixed)),
        .hex => try b.appendStr(try bytesToHex(arena, raw)),
        .bytes, .raw_text => try b.appendStr(raw),
    }
}

/// Fails a read on an nvarchar cell that is not UTF-16 text, naming its column.
fn notUtf16(name: []const u8, e: utf16.DecodeError) utf16.DecodeError {
    var buf: [480]u8 = undefined;
    const msg = switch (e) {
        error.LoneSurrogate => std.fmt.bufPrint(&buf, "SQL Server column `{s}` holds an unpaired UTF-16 surrogate half, which is not Unicode text; REPLACE it at the source or read the column CAST(... AS varbinary)", .{eval.clip(name)}),
        error.OddUtf16 => std.fmt.bufPrint(&buf, "SQL Server column `{s}` holds a value of an odd number of bytes, which is not UTF-16 text; read the column CAST(... AS varbinary)", .{eval.clip(name)}),
    } catch unreachable;
    return @errorCast(eval.explain(e, msg));
}

/// Fails a bulk load on text that is not valid UTF-8, naming its column.
fn notUtf8(name: []const u8) error{InvalidUtf8} {
    var buf: [480]u8 = undefined;
    return @errorCast(eval.explain(error.InvalidUtf8, std.fmt.bufPrint(&buf, "column `{s}` holds text that is not valid UTF-8, which SQL Server's nvarchar cannot take unaltered; repair or drop it before the load", .{eval.clip(name)}) catch unreachable));
}

/// `appendRaw` for a column whose decode is known only at run time.
fn appendCell(b: *column.Builder, d: ColumnDesc, raw: []const u8, arena: std.mem.Allocator) !void {
    switch (d.decode) {
        inline else => |c| try appendRaw(c, b, d, raw, arena),
    }
}

/// A value body whose length its type cannot have. It fails the read rather than
/// decoding to a plausible wrong value (a cut-off decimal, a 1970 date).
const MalformedValue = error{MalformedTdsValue};

/// DECIMALN: a sign byte, then a 4, 8, 12 or 16-byte little-endian magnitude.
fn decodeDecimal(d: ColumnDesc, bytes: []const u8) MalformedValue!Decimal {
    switch (bytes.len) {
        5, 9, 13, 17 => {},
        else => return error.MalformedTdsValue,
    }
    const v = decimal.signMagnitudeDecimal(bytes[0] == 1, bytes[1..]) orelse return error.MalformedTdsValue;
    return .{ .unscaled = v, .scale = d.scale };
}

fn decodeMoney(bytes: []const u8) MalformedValue!Decimal {
    return decimal.money(bytes) orelse error.MalformedTdsValue;
}

/// TIMENTYPE: 3-5 little-endian bytes counting 10^-scale seconds since midnight.
fn decodeTime(d: ColumnDesc, bytes: []const u8) MalformedValue!i64 {
    if (bytes.len < 3 or bytes.len > 5) return error.MalformedTdsValue;
    const tu: i64 = @intCast(readULE(bytes));
    return @divTrunc(tu * 1_000_000, pow10(d.scale));
}

/// DATETIME2/DATETIMEOFFSET: 3-5 bytes of time, 3 of date (and 2 of offset);
/// DATETIME: 8 bytes; SMALLDATETIME: 4.
fn decodeDateTime(d: ColumnDesc, bytes: []const u8) MalformedValue!i64 {
    switch (d.tds_type) {
        0x2A, 0x2B => {
            const off_bytes: usize = if (d.tds_type == 0x2B) 2 else 0;
            if (bytes.len < 3 + off_bytes) return error.MalformedTdsValue;
            const tlen = bytes.len - 3 - off_bytes;
            const time_micros = try decodeTime(d, bytes[0..tlen]);
            const days: i64 = @intCast(readULE(bytes[tlen .. tlen + 3]));
            return civil.dayMicros(days + civil.epoch.ce1) +| time_micros;
        },
        else => switch (bytes.len) {
            8 => {
                const date4 = readIntLE(bytes[0..4]);
                const ticks: i64 = @intCast(readULE(bytes[4..8]));
                return civil.dayMicros(date4 + civil.epoch.sql1900) +| @divTrunc(ticks * 1_000_000, 300);
            },
            4 => {
                const days: i64 = std.mem.readInt(u16, bytes[0..2], .little);
                const mins: i64 = std.mem.readInt(u16, bytes[2..4], .little);
                return civil.dayMicros(days + civil.epoch.sql1900) + mins * 60_000_000;
            },
            else => return error.MalformedTdsValue,
        },
    }
}

/// 10^18 is the largest power of ten an i64 holds; a wire scale is capped there.
/// TDS never sends more than 7.
fn pow10(n: u8) i64 {
    var r: i64 = 1;
    var k: u8 = 0;
    while (k < @min(n, 18)) : (k += 1) r *= 10;
    return r;
}

fn readULE(bytes: []const u8) u64 {
    var u: u64 = 0;
    for (bytes, 0..) |b, k| {
        if (k >= 8) break;
        u |= @as(u64, b) << @intCast(k * 8);
    }
    return u;
}

fn readIntLE(bytes: []const u8) i64 {
    return switch (bytes.len) {
        0 => 0,
        1 => @as(i64, bytes[0]),
        2 => std.mem.readInt(i16, bytes[0..2], .little),
        4 => std.mem.readInt(i32, bytes[0..4], .little),
        8 => std.mem.readInt(i64, bytes[0..8], .little),
        else => blk: {
            var b8: [8]u8 = std.mem.zeroes([8]u8);
            const n = @min(bytes.len, 8);
            @memcpy(b8[0..n], bytes[0..n]);
            if (bytes[n - 1] & 0x80 != 0) {
                for (b8[n..]) |*x| x.* = 0xFF;
            }
            break :blk std.mem.readInt(i64, &b8, .little);
        },
    };
}

fn bytesToHex(arena: std.mem.Allocator, b: []const u8) ![]const u8 {
    const out = try arena.alloc(u8, 2 + b.len * 2);
    out[0..2].* = "0x".*;
    _ = hex.encode(out[2..], b, .lower);
    return out;
}

test "bytes to hex string" {
    const alloc = std.testing.allocator;
    const out = try bytesToHex(alloc, &[_]u8{ 0x00, 0x01, 0xAB, 0xFF });
    defer alloc.free(out);
    try std.testing.expectEqualStrings("0x0001abff", out);
}

test "tds: a uniqueidentifier cell reads as the mixed-endian GUID the old formatGuid wrote" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const old = struct {
        fn formatGuid(arena: std.mem.Allocator, b: []const u8) ![]const u8 {
            if (b.len < 16) return arena.dupe(u8, "");
            return std.fmt.allocPrint(arena, "{x:0>2}{x:0>2}{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}", .{
                b[3], b[2], b[1], b[0], b[5], b[4], b[7], b[6], b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15],
            });
        }
    };
    var d = ColumnDesc{ .tds_type = 0x24, .engine_type = types.Type.init(.string).asNullable(), .kind = .bytelen, .is_guid = true };
    d.decode = decodeOf(d);
    try std.testing.expectEqual(Decode.guid, d.decode);
    const b = [_]u8{ 0x10, 0x32, 0x54, 0x76, 0x98, 0xBA, 0xDC, 0xFE, 0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF };
    try std.testing.expectEqualStrings("76543210-ba98-fedc-0123-456789abcdef", (try cellValue(a, d, &b)).string);
    var prng = std.Random.DefaultPrng.init(0x6d1d);
    const r = prng.random();
    var buf: [20]u8 = undefined;
    for (0..2000) |_| {
        const n = r.uintAtMost(usize, buf.len);
        r.bytes(buf[0..n]);
        try std.testing.expectEqualStrings(try old.formatGuid(a, buf[0..n]), (try cellValue(a, d, buf[0..n])).string);
    }
}

test "tds: binary text is 0x and lower-case hex, as the old bytesToHex wrote it" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const old = struct {
        fn bytesToHex(arena: std.mem.Allocator, b: []const u8) ![]const u8 {
            const digits = "0123456789abcdef";
            const out = try arena.alloc(u8, 2 + b.len * 2);
            out[0] = '0';
            out[1] = 'x';
            for (b, 0..) |byte, i| {
                out[2 + i * 2] = digits[byte >> 4];
                out[2 + i * 2 + 1] = digits[byte & 0x0F];
            }
            return out;
        }
    };
    var prng = std.Random.DefaultPrng.init(0x0bad);
    const r = prng.random();
    var buf: [40]u8 = undefined;
    for (0..2000) |_| {
        const n = r.uintAtMost(usize, buf.len);
        r.bytes(buf[0..n]);
        try std.testing.expectEqualStrings(try old.bytesToHex(a, buf[0..n]), try bytesToHex(a, buf[0..n]));
    }
}

/// One varchar cell's wire bytes through the read path's decode, as text.
fn varcharCell(a: std.mem.Allocator, raw: []const u8) ![]const u8 {
    var d = ColumnDesc{ .tds_type = 0xA7, .engine_type = types.Type.init(.string).asNullable(), .kind = .ushortlen };
    d.decode = decodeOf(d);
    return (try cellValue(a, d, raw)).string;
}

test "tds: a varchar cell decodes as charset's cp1252 does, the unassigned bytes to C1 as SQL Server converts them (D5)" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    for (0..256) |i| {
        const b = [1]u8{@intCast(i)};
        try std.testing.expectEqualStrings(try charset.decode(a, .cp1252, &b), try varcharCell(a, &b));
    }
    try std.testing.expectEqualStrings("\xc2\x81\xc2\x8d\xc2\x8f\xc2\x90\xc2\x9d", try varcharCell(a, "\x81\x8d\x8f\x90\x9d"));
    try std.testing.expectEqualStrings("\xc2\x81\xc2\x8d\xc2\x8f\xc2\x90\xc2\x9d", try charset.decode(a, .cp1252, "\x81\x8d\x8f\x90\x9d"));
}

test "win1252 transcode to utf8" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings("RELÓGIO", try varcharCell(a, &[_]u8{ 'R', 'E', 'L', 0xD3, 'G', 'I', 'O' }));
    try std.testing.expectEqualStrings("plain", try varcharCell(a, "plain"));
    try std.testing.expectEqualStrings("€", try varcharCell(a, &[_]u8{0x80}));
}

test "win1252 decodes every byte" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    var all: [256]u8 = undefined;
    for (&all, 0..) |*b, k| b.* = @intCast(k);
    const got = try varcharCell(ar.allocator(), &all);
    try std.testing.expectEqualStrings("\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\x0c\r\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~\x7f\xe2\x82\xac\xc2\x81\xe2\x80\x9a\xc6\x92\xe2\x80\x9e\xe2\x80\xa6\xe2\x80\xa0\xe2\x80\xa1\xcb\x86\xe2\x80\xb0\xc5\xa0\xe2\x80\xb9\xc5\x92\xc2\x8d\xc5\xbd\xc2\x8f\xc2\x90\xe2\x80\x98\xe2\x80\x99\xe2\x80\x9c\xe2\x80\x9d\xe2\x80\xa2\xe2\x80\x93\xe2\x80\x94\xcb\x9c\xe2\x84\xa2\xc5\xa1\xe2\x80\xba\xc5\x93\xc2\x9d\xc5\xbe\xc5\xb8\xc2\xa0\xc2\xa1\xc2\xa2\xc2\xa3\xc2\xa4\xc2\xa5\xc2\xa6\xc2\xa7\xc2\xa8\xc2\xa9\xc2\xaa\xc2\xab\xc2\xac\xc2\xad\xc2\xae\xc2\xaf\xc2\xb0\xc2\xb1\xc2\xb2\xc2\xb3\xc2\xb4\xc2\xb5\xc2\xb6\xc2\xb7\xc2\xb8\xc2\xb9\xc2\xba\xc2\xbb\xc2\xbc\xc2\xbd\xc2\xbe\xc2\xbf\xc3\x80\xc3\x81\xc3\x82\xc3\x83\xc3\x84\xc3\x85\xc3\x86\xc3\x87\xc3\x88\xc3\x89\xc3\x8a\xc3\x8b\xc3\x8c\xc3\x8d\xc3\x8e\xc3\x8f\xc3\x90\xc3\x91\xc3\x92\xc3\x93\xc3\x94\xc3\x95\xc3\x96\xc3\x97\xc3\x98\xc3\x99\xc3\x9a\xc3\x9b\xc3\x9c\xc3\x9d\xc3\x9e\xc3\x9f\xc3\xa0\xc3\xa1\xc3\xa2\xc3\xa3\xc3\xa4\xc3\xa5\xc3\xa6\xc3\xa7\xc3\xa8\xc3\xa9\xc3\xaa\xc3\xab\xc3\xac\xc3\xad\xc3\xae\xc3\xaf\xc3\xb0\xc3\xb1\xc3\xb2\xc3\xb3\xc3\xb4\xc3\xb5\xc3\xb6\xc3\xb7\xc3\xb8\xc3\xb9\xc3\xba\xc3\xbb\xc3\xbc\xc3\xbd\xc3\xbe\xc3\xbf", got);
}

test "win1252 text decodes as its pieces decode, split anywhere" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var prng = std.Random.DefaultPrng.init(0x1252);
    const rnd = prng.random();
    var buf: [80]u8 = undefined;
    for (0..4000) |it| {
        _ = ar.reset(.retain_capacity);
        const len = rnd.uintLessThan(usize, buf.len + 1);
        for (buf[0..len]) |*b| b.* = if (it % 3 == 0) rnd.int(u8) else rnd.uintLessThan(u8, 0x80);
        if (it % 5 == 0 and len > 0) buf[rnd.uintLessThan(usize, len)] = 0x80 + rnd.uintLessThan(u8, 0x80);
        const cut = rnd.uintAtMost(usize, len);
        const whole = try varcharCell(a, buf[0..len]);
        const head = try varcharCell(a, buf[0..cut]);
        const tail = try varcharCell(a, buf[cut..len]);
        try std.testing.expectEqualSlices(u8, head, whole[0..head.len]);
        try std.testing.expectEqualSlices(u8, tail, whole[head.len..]);
        try std.testing.expectEqual(whole.len == len, charset.isAscii(buf[0..len]));
    }
}

/// A final DONE token: status 0, no more results, row count 0.
const done_final = [_]u8{0xFD} ++ [_]u8{0} ** 12;

/// `payload` framed as TDS packets of at most `chunk` bytes, the last flagged EOM.
fn framePackets(a: std.mem.Allocator, payload: []const u8, chunk: usize) ![]const u8 {
    var stream = std.array_list.Managed(u8).init(a);
    var off: usize = 0;
    while (off < payload.len) {
        const n = @min(chunk, payload.len - off, 0xFFFF - 8);
        const last = off + n == payload.len;
        try stream.appendSlice(&.{ 0x04, if (last) STATUS_EOM else 0, @intCast((n + 8) >> 8), @intCast((n + 8) & 0xFF), 0, 0, 0, 0 });
        try stream.appendSlice(payload[off..][0..n]);
        off += n;
    }
    return stream.items;
}

test "a rowset cut off before its DONE, by a closed socket, an early EOM or an unknown token, is an error, never a short read" {
    const gpa = std.testing.allocator;
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var cols = [_]ColumnDesc{.{ .tds_type = 0x38, .engine_type = types.Type.init(.int).asNullable(), .kind = .fixed, .fixed_len = 4, .decode = .int }};
    const row = [_]u8{ 0xD1, 7, 0, 0, 0 };
    const whole = try framePackets(a, &(row ++ row ++ done_final), 64);
    const closed = whole[0 .. 8 + row.len];
    var unflagged = try a.dupe(u8, closed);
    unflagged[1] = 0;
    const early_eom = try framePackets(a, &(row ++ row), 64);
    const unknown = try framePackets(a, &(row ++ [_]u8{0x42} ++ row ++ done_final), 64);
    const cases = [_]struct { bytes: []const u8, want: ?anyerror }{
        .{ .bytes = whole, .want = null },
        .{ .bytes = unflagged, .want = error.EndOfStream },
        .{ .bytes = early_eom, .want = error.TdsProtocol },
        .{ .bytes = unknown, .want = error.TdsProtocol },
    };
    for (cases) |c| {
        var src = std.Io.Reader.fixed(c.bytes);
        const cur = try gpa.create(TdsCursor);
        defer gpa.destroy(cur);
        cur.* = .{ .gpa = gpa, .conn = undefined, .reader = PacketReader.init(gpa, &src), .meta_arena = undefined, .cols = &cols, .schema = undefined, .done = false };
        defer cur.reader.deinit();
        if (c.want) |e| {
            try std.testing.expectError(e, cur.fetchBatch(a));
        } else {
            const b = (try cur.fetchBatch(a)).?;
            try std.testing.expectEqual(@as(usize, 2), b.len);
            try std.testing.expect(cur.done);
        }
    }
}

test "rows split across packets of every small size decode as each cell decoded alone" {
    const gpa = std.testing.allocator;
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    const strT = types.Type.init(.string).asNullable();
    var cols = [_]ColumnDesc{
        .{ .tds_type = 0x38, .engine_type = types.Type.init(.int).asNullable(), .kind = .fixed, .fixed_len = 4 },
        .{ .tds_type = 0xA7, .engine_type = strT, .kind = .ushortlen },
        .{ .tds_type = 0x2F, .engine_type = strT, .kind = .bytelen },
        .{ .tds_type = 0x6C, .engine_type = types.Type.decimal(14, 2).asNullable(), .kind = .bytelen, .scale = 2 },
        .{ .tds_type = 0xE7, .engine_type = strT, .kind = .ushortlen, .is_unicode = true },
        .{ .tds_type = 0x3E, .engine_type = types.Type.init(.float).asNullable(), .kind = .fixed, .fixed_len = 8 },
    };
    for (&cols) |*c| c.decode = decodeOf(c.*);
    var prng = std.Random.DefaultPrng.init(0x7d5);
    const rnd = prng.random();
    const rows = 60;
    var payload = std.array_list.Managed(u8).init(a);
    const want = try a.alloc([cols.len]Value, rows);
    for (want) |*row| {
        const nbc = rnd.boolean();
        try payload.append(if (nbc) 0xD2 else 0xD1);
        var nulls: u8 = 0;
        if (nbc) {
            nulls = rnd.int(u8) & 0x3F;
            try payload.append(nulls);
        }
        for (&cols, row, 0..) |col, *want_cell, ci| {
            if (nulls >> @intCast(ci) & 1 != 0) {
                want_cell.* = .null;
                continue;
            }
            var raw: [64]u8 = undefined;
            const len: usize = switch (col.kind) {
                .fixed => col.fixed_len,
                else => if (col.engine_type.kind == .decimal) 9 else rnd.uintAtMost(usize, 40) & if (col.is_unicode) ~@as(usize, 1) else ~@as(usize, 0),
            };
            for (raw[0..len]) |*b| b.* = if (rnd.uintLessThan(u8, 4) == 0) rnd.int(u8) else rnd.intRangeAtMost(u8, ' ', '~');
            if (col.engine_type.kind == .decimal) {
                raw[0] = rnd.uintAtMost(u8, 1);
                raw[8] = 0;
            }
            if (col.is_unicode) for (0..len / 2) |k| {
                if (rnd.uintLessThan(u8, 4) != 0 or raw[2 * k + 1] & 0xF8 == 0xD8) raw[2 * k + 1] = 0;
            };
            const is_null = col.kind != .fixed and rnd.uintLessThan(u8, 8) == 0;
            switch (col.kind) {
                .fixed => {},
                .bytelen => try payload.append(if (is_null) 0 else @intCast(len)),
                else => {
                    var l: [2]u8 = undefined;
                    std.mem.writeInt(u16, &l, if (is_null) 0xFFFF else @intCast(len), .little);
                    try payload.appendSlice(&l);
                },
            }
            if (is_null or (col.kind == .bytelen and len == 0)) {
                want_cell.* = .null;
                continue;
            }
            try payload.appendSlice(raw[0..len]);
            want_cell.* = try cellValue(a, col, raw[0..len]);
        }
    }
    try payload.appendSlice(&done_final);
    for ([_]usize{ 1, 2, 3, 5, 7, 13, 64, 1 << 16 }) |chunk| {
        var src = std.Io.Reader.fixed(try framePackets(a, payload.items, chunk));
        const cur = try gpa.create(TdsCursor);
        defer gpa.destroy(cur);
        cur.* = .{ .gpa = gpa, .conn = undefined, .reader = PacketReader.init(gpa, &src), .meta_arena = undefined, .cols = &cols, .schema = undefined, .done = false };
        defer cur.reader.deinit();
        const batch = (try cur.fetchBatch(a)).?;
        try std.testing.expectEqual(@as(usize, rows), batch.len);
        for (want, 0..) |row, r| for (row, 0..) |w, ci| {
            const got = batch.columns[ci].getValue(r);
            try std.testing.expectEqual(std.meta.activeTag(w), std.meta.activeTag(got));
            switch (w) {
                .string => |s| try std.testing.expectEqualStrings(s, got.string),
                else => try std.testing.expect(std.meta.eql(w, got)),
            }
        };
    }
}

test "readIntLE sign-extends every TDS integer width" {
    try std.testing.expectEqual(@as(i64, 0), readIntLE(&.{}));
    try std.testing.expectEqual(@as(i64, 255), readIntLE(&.{0xFF}));
    try std.testing.expectEqual(@as(i64, 127), readIntLE(&.{0x7F}));
    try std.testing.expectEqual(@as(i64, -2), readIntLE(&.{ 0xFE, 0xFF }));
    try std.testing.expectEqual(@as(i64, -1), readIntLE(&.{ 0xFF, 0xFF, 0xFF, 0xFF }));
    try std.testing.expectEqual(@as(i64, 1), readIntLE(&.{ 1, 0, 0, 0, 0, 0, 0, 0 }));
    try std.testing.expectEqual(std.math.minInt(i64), readIntLE(&.{ 0, 0, 0, 0, 0, 0, 0, 0x80 }));
    try std.testing.expectEqual(@as(i64, -1), readIntLE(&.{ 0xFF, 0xFF, 0xFF }));
    try std.testing.expectEqual(@as(i64, 0x010203), readIntLE(&.{ 0x03, 0x02, 0x01 }));
}

test "decodeDecimal: sign byte + little-endian magnitude at the column scale" {
    const d = ColumnDesc{ .tds_type = 0x6C, .engine_type = types.Type.decimal(10, 2).asNullable(), .kind = .bytelen, .scale = 2 };
    const pos = try decodeDecimal(d, &.{ 1, 0x39, 0x30, 0, 0 });
    try std.testing.expectEqual(@as(i128, 12345), pos.unscaled);
    try std.testing.expectEqual(@as(u8, 2), pos.scale);
    const neg = try decodeDecimal(d, &.{ 0, 0x39, 0x30, 0, 0 });
    try std.testing.expectEqual(@as(i128, -12345), neg.unscaled);
}

test "a body of a length its type cannot have is an error, never a cut-off value or a 1970 date" {
    const sdt = ColumnDesc{ .tds_type = 0x3A, .engine_type = types.Type.init(.timestamp).asNullable(), .kind = .fixed, .fixed_len = 4 };
    try std.testing.expectError(error.MalformedTdsValue, decodeDateTime(sdt, &.{ 1, 2, 3 }));
    try std.testing.expectError(error.MalformedTdsValue, decodeDateTime(sdt, &.{}));
    try std.testing.expectError(error.MalformedTdsValue, decodeDateTime(sdt, &.{ 1, 2, 3, 4, 5, 6 }));

    const dt2 = ColumnDesc{ .tds_type = 0x2A, .engine_type = types.Type.init(.timestamp).asNullable(), .kind = .bytelen, .scale = 40 };
    try std.testing.expectEqual(@as(i64, (0x070605 - 719162) * 86_400_000_000), try decodeDateTime(dt2, &.{ 1, 2, 3, 4, 5, 6, 7 }));
    var long_dt: [64]u8 = undefined;
    @memset(&long_dt, 0xFF);
    try std.testing.expectError(error.MalformedTdsValue, decodeDateTime(dt2, &long_dt));
    try std.testing.expectError(error.MalformedTdsValue, decodeDateTime(dt2, &.{ 1, 2, 3, 4 }));

    const tm = ColumnDesc{ .tds_type = 0x29, .engine_type = types.Type.init(.time).asNullable(), .kind = .bytelen, .scale = 7 };
    try std.testing.expectError(error.MalformedTdsValue, decodeTime(tm, &.{ 1, 2 }));
    try std.testing.expectError(error.MalformedTdsValue, decodeTime(tm, &.{ 1, 2, 3, 4, 5, 6 }));

    const dec = ColumnDesc{ .tds_type = 0x6C, .engine_type = types.Type.decimal(38, 2).asNullable(), .kind = .bytelen, .scale = 2 };
    const max38: i128 = 99999999999999999999999999999999999999;
    var widest: [17]u8 = undefined;
    widest[0] = 1;
    std.mem.writeInt(i128, widest[1..17], max38, .little);
    try std.testing.expectEqual(max38, (try decodeDecimal(dec, &widest)).unscaled);
    widest[0] = 0;
    try std.testing.expectEqual(-max38, (try decodeDecimal(dec, &widest)).unscaled);

    var long: [40]u8 = undefined;
    @memset(&long, 0xFF);
    long[0] = 1;
    try std.testing.expectError(error.MalformedTdsValue, decodeDecimal(dec, &long));
    try std.testing.expectError(error.MalformedTdsValue, decodeDecimal(dec, &long[0..17].*));
    try std.testing.expectError(error.MalformedTdsValue, decodeDecimal(dec, &.{}));
    try std.testing.expectError(error.MalformedTdsValue, decodeDecimal(dec, &.{ 1, 2, 3 }));
    try std.testing.expectError(error.MalformedTdsValue, decodeMoney(&.{ 1, 2, 3 }));
}

test "decodeMoney: ten-thousandths, high word first for the 8-byte form" {
    try std.testing.expectEqual(@as(i128, -1), (try decodeMoney(&(.{0xFF} ** 8))).unscaled);
    try std.testing.expectEqual(@as(i128, -1), (try decodeMoney(&(.{0xFF} ** 4))).unscaled);
    try std.testing.expectEqual(@as(u8, 4), (try decodeMoney(&(.{0xFF} ** 8))).scale);

    var b: [8]u8 = undefined;
    std.mem.writeInt(i32, b[0..4], 0x7FFFFFFF, .little);
    std.mem.writeInt(u32, b[4..8], 0xFFFFFFFF, .little);
    try std.testing.expectEqual(@as(i128, std.math.maxInt(i64)), (try decodeMoney(&b)).unscaled);

    std.mem.writeInt(i32, b[0..4], 0, .little);
    std.mem.writeInt(u32, b[4..8], 10000, .little);
    try std.testing.expectEqual(@as(i128, 10000), (try decodeMoney(&b)).unscaled);

    var b4: [4]u8 = undefined;
    std.mem.writeInt(i32, &b4, std.math.minInt(i32), .little);
    try std.testing.expectEqual(@as(i128, std.math.minInt(i32)), (try decodeMoney(&b4)).unscaled);
}

test "decodeTime: 10^-scale seconds since midnight to micros" {
    var d = ColumnDesc{ .tds_type = 0x29, .engine_type = types.Type.init(.time).asNullable(), .kind = .bytelen, .scale = 0 };
    try std.testing.expectEqual(@as(i64, 12 * 3600 * 1_000_000), try decodeTime(d, &.{ 0xC0, 0xA8, 0x00 }));

    d.scale = 7;
    var b5: [5]u8 = .{ 0, 0, 0, 0, 0 };
    const tenths_us: u40 = 863_999_999_999;
    inline for (0..5) |k| b5[k] = @intCast((tenths_us >> (k * 8)) & 0xFF);
    try std.testing.expectEqual(@as(i64, 86_399_999_999), try decodeTime(d, &b5));
}

test "decodeDateTime: DATETIME ticks, SMALLDATETIME minutes, DATETIME2 scale" {
    var dt = ColumnDesc{ .tds_type = 0x3D, .engine_type = types.Type.init(.timestamp).asNullable(), .kind = .fixed, .fixed_len = 8 };
    var bytes8: [8]u8 = undefined;
    std.mem.writeInt(i32, bytes8[0..4], 25567, .little);
    std.mem.writeInt(u32, bytes8[4..8], 300, .little);
    try std.testing.expectEqual(@as(i64, 1_000_000), try decodeDateTime(dt, &bytes8));

    dt.tds_type = 0x3A;
    var bytes4: [4]u8 = undefined;
    std.mem.writeInt(u16, bytes4[0..2], 25568, .little);
    std.mem.writeInt(u16, bytes4[2..4], 90, .little);
    try std.testing.expectEqual(@as(i64, 86_400_000_000 + 90 * 60_000_000), try decodeDateTime(dt, &bytes4));

    const dt2 = ColumnDesc{ .tds_type = 0x2A, .engine_type = types.Type.init(.timestamp).asNullable(), .kind = .bytelen, .scale = 3 };
    var b7: [7]u8 = .{ 0, 0, 0, 0, 0, 0, 0 };
    std.mem.writeInt(u32, b7[0..4], 1500, .little);
    const days: u24 = 719162 + 1;
    b7[4] = @intCast(days & 0xFF);
    b7[5] = @intCast((days >> 8) & 0xFF);
    b7[6] = @intCast(days >> 16);
    try std.testing.expectEqual(@as(i64, 86_400_000_000 + 1_500_000), try decodeDateTime(dt2, &b7));
}

test "tds: epochs from civil are the day counts the decoders used before" {
    try std.testing.expectEqual(@as(i64, -719162), civil.epoch.ce1);
    try std.testing.expectEqual(@as(i64, -25567), civil.epoch.sql1900);
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var d = ColumnDesc{ .tds_type = 0x28, .engine_type = types.Type.init(.date).asNullable(), .kind = .bytelen };
    d.decode = decodeOf(d);
    for ([_]u24{ 0, 1, 719162, 719163, 0xFFFFFF }) |days| {
        var raw: [3]u8 = undefined;
        std.mem.writeInt(u24, &raw, days, .little);
        try std.testing.expectEqual(@as(i32, @intCast(@as(i64, days) - 719162)), (try cellValue(a, d, &raw)).date);
    }
    const dt2 = ColumnDesc{ .tds_type = 0x2A, .engine_type = types.Type.init(.timestamp).asNullable(), .kind = .bytelen, .scale = 7 };
    const dt = ColumnDesc{ .tds_type = 0x3D, .engine_type = types.Type.init(.timestamp).asNullable(), .kind = .fixed, .fixed_len = 8 };
    const sdt = ColumnDesc{ .tds_type = 0x3A, .engine_type = types.Type.init(.timestamp).asNullable(), .kind = .fixed, .fixed_len = 4 };
    var prng = std.Random.DefaultPrng.init(0xe0c);
    const r = prng.random();
    for (0..2000) |_| {
        var b8: [8]u8 = undefined;
        r.bytes(&b8);
        const tu: i64 = @intCast(std.mem.readInt(u40, b8[0..5], .little));
        const days: i64 = std.mem.readInt(u24, b8[5..8], .little);
        try std.testing.expectEqual((days - 719162) *| 86_400_000_000 +| @divTrunc(tu * 1_000_000, 10_000_000), try decodeDateTime(dt2, &b8));
        const date4: i64 = std.mem.readInt(i32, b8[0..4], .little);
        const ticks: i64 = std.mem.readInt(u32, b8[4..8], .little);
        try std.testing.expectEqual((date4 - 25567) *| 86_400_000_000 +| @divTrunc(ticks * 1_000_000, 300), try decodeDateTime(dt, &b8));
        const sdays: i64 = std.mem.readInt(u16, b8[0..2], .little);
        const mins: i64 = std.mem.readInt(u16, b8[2..4], .little);
        try std.testing.expectEqual((sdays - 25567) * 86_400_000_000 + mins * 60_000_000, try decodeDateTime(sdt, b8[0..4]));
    }
}

/// The row count of a final DONE (0xFD) with DONE_COUNT (0x10) set.
fn parseDoneRowCount(token: u8, d: []const u8) ?u64 {
    if (token != 0xFD or d.len != 12) return null;
    if (std.mem.readInt(u16, d[0..2], .little) & 0x10 == 0) return null;
    return std.mem.readInt(u64, d[4..12], .little);
}

test "parseDoneRowCount: counted DONE, uncounted DONE, DONEINPROC" {
    const counted = [_]u8{ 0x10, 0x00, 0x00, 0x00, 7, 0, 0, 0, 0, 0, 0, 0 };
    try std.testing.expectEqual(@as(?u64, 7), parseDoneRowCount(0xFD, &counted));
    const uncounted = [_]u8{ 0x00, 0x00, 0x00, 0x00, 7, 0, 0, 0, 0, 0, 0, 0 };
    try std.testing.expectEqual(@as(?u64, null), parseDoneRowCount(0xFD, &uncounted));
    try std.testing.expectEqual(@as(?u64, null), parseDoneRowCount(0xFF, &counted));
}

fn parseEnvPacketSize(d: []const u8) ?usize {
    if (d.len < 2 or d[0] != 4) return null;
    const n: usize = d[1];
    if (n == 0 or d.len < 2 + 2 * n) return null;
    var v: usize = 0;
    for (0..n) |k| {
        const ch = d[2 + 2 * k];
        if (ch < '0' or ch > '9' or d[3 + 2 * k] != 0) return null;
        v = v * 10 + (ch - '0');
    }
    return if (v >= 512 and v <= 32767) v else null;
}

test "parseEnvPacketSize: negotiated size, wrong type, junk" {
    const good = [_]u8{ 4, 5, '1', 0, '6', 0, '3', 0, '8', 0, '4', 0 };
    try std.testing.expectEqual(@as(?usize, 16384), parseEnvPacketSize(&good));
    const wrong_type = [_]u8{ 1, 5, '1', 0, '6', 0, '3', 0, '8', 0, '4', 0 };
    try std.testing.expectEqual(@as(?usize, null), parseEnvPacketSize(&wrong_type));
    const non_digit = [_]u8{ 4, 2, 'x', 0, '1', 0 };
    try std.testing.expectEqual(@as(?usize, null), parseEnvPacketSize(&non_digit));
    const truncated = [_]u8{ 4, 5, '1', 0 };
    try std.testing.expectEqual(@as(?usize, null), parseEnvPacketSize(&truncated));
    const out_of_range = [_]u8{ 4, 2, '6', 0, '4', 0 };
    try std.testing.expectEqual(@as(?usize, null), parseEnvPacketSize(&out_of_range));
}

fn buildLogin7(gpa: std.mem.Allocator, user: []const u8, password: []const u8, database: []const u8, host: []const u8) ![]u8 {
    var fixed = std.mem.zeroes([94]u8);
    fixed[4] = 0x04;
    fixed[7] = 0x74;
    fixed[8] = 0x00;
    fixed[9] = 0x40;

    var vd = std.array_list.Managed(u8).init(gpa);
    defer vd.deinit();

    try addField(&fixed, 36, host, &vd, false);
    try addField(&fixed, 40, user, &vd, false);
    try addField(&fixed, 44, password, &vd, true);
    try addField(&fixed, 48, "basalt", &vd, false);
    try addField(&fixed, 52, host, &vd, false);
    try addField(&fixed, 56, "", &vd, false);
    try addField(&fixed, 60, "basalt", &vd, false);
    try addField(&fixed, 64, "", &vd, false);
    try addField(&fixed, 68, database, &vd, false);
    try addField(&fixed, 78, "", &vd, false);
    try addField(&fixed, 82, "", &vd, false);
    try addField(&fixed, 86, "", &vd, false);

    const total: u32 = @intCast(94 + vd.items.len);
    std.mem.writeInt(u32, fixed[0..4], total, .little);

    const out = try gpa.alloc(u8, total);
    @memcpy(out[0..94], &fixed);
    @memcpy(out[94..], vd.items);
    return out;
}

/// LOGIN7 with a FEDAUTH feature extension (Security Token library) and no SQL
/// credentials. `echo` mirrors FEDAUTHREQUIRED; fExtension (0x10) flags the feature
/// block, whose offset sits in a slot the Extension offset/length pair points to.
fn buildLogin7Fedauth(gpa: std.mem.Allocator, token: []const u8, database: []const u8, host: []const u8, echo: bool, nonce: ?[32]u8) ![]u8 {
    var fixed = std.mem.zeroes([94]u8);
    fixed[4] = 0x04;
    fixed[7] = 0x74;
    fixed[9] = 0x10;
    fixed[27] = 0x10;

    var vd = std.array_list.Managed(u8).init(gpa);
    defer vd.deinit();

    try addField(&fixed, 36, host, &vd, false);
    try addField(&fixed, 40, "", &vd, false);
    try addField(&fixed, 44, "", &vd, true);
    try addField(&fixed, 48, "basalt", &vd, false);
    try addField(&fixed, 52, host, &vd, false);
    const ext_ib: u16 = @intCast(94 + vd.items.len);
    std.mem.writeInt(u16, fixed[56..][0..2], ext_ib, .little);
    std.mem.writeInt(u16, fixed[58..][0..2], 4, .little);
    const ext_slot = vd.items.len;
    try vd.appendSlice(&[_]u8{ 0, 0, 0, 0 });
    try addField(&fixed, 60, "basalt", &vd, false);
    try addField(&fixed, 64, "", &vd, false);
    try addField(&fixed, 68, database, &vd, false);
    try addField(&fixed, 78, "", &vd, false);
    try addField(&fixed, 82, "", &vd, false);
    try addField(&fixed, 86, "", &vd, false);

    const feat_off: u32 = @intCast(94 + vd.items.len);
    std.mem.writeInt(u32, vd.items[ext_slot..][0..4], feat_off, .little);

    const have_nonce = echo and nonce != null;
    const tok_bytes: u32 = @intCast(token.len * 2);
    const data_len: u32 = 1 + 4 + tok_bytes + (if (have_nonce) @as(u32, 32) else 0);
    try vd.append(0x02);
    var lb: [4]u8 = undefined;
    std.mem.writeInt(u32, &lb, data_len, .little);
    try vd.appendSlice(&lb);
    try vd.append((0x01 << 1) | @as(u8, if (echo) 1 else 0));
    std.mem.writeInt(u32, &lb, tok_bytes, .little);
    try vd.appendSlice(&lb);
    for (token) |ch| {
        try vd.append(ch);
        try vd.append(0);
    }
    if (have_nonce) try vd.appendSlice(&nonce.?);
    try vd.append(0xFF);

    const total: u32 = @intCast(94 + vd.items.len);
    std.mem.writeInt(u32, fixed[0..4], total, .little);
    const out = try gpa.alloc(u8, total);
    @memcpy(out[0..94], &fixed);
    @memcpy(out[94..], vd.items);
    return out;
}

/// LOGIN7 with an SSPI blob (MS-TDS 2.2.6.4): fIntSecurity (0x80, byte 25) selects
/// integrated security; ibSSPI/cbSSPI (byte 78) point at raw bytes. cbSSPILong stays 0,
/// consulted only when cbSSPI is 0xFFFF.
fn buildLogin7Sspi(gpa: std.mem.Allocator, sspi: []const u8, database: []const u8, host: []const u8) ![]u8 {
    var fixed = std.mem.zeroes([94]u8);
    fixed[4] = 0x04;
    fixed[7] = 0x74;
    fixed[8] = 0x00;
    fixed[9] = 0x40;
    fixed[25] = 0x80;

    var vd = std.array_list.Managed(u8).init(gpa);
    defer vd.deinit();

    try addField(&fixed, 36, host, &vd, false);
    try addField(&fixed, 40, "", &vd, false);
    try addField(&fixed, 44, "", &vd, true);
    try addField(&fixed, 48, "basalt", &vd, false);
    try addField(&fixed, 52, host, &vd, false);
    try addField(&fixed, 56, "", &vd, false);
    try addField(&fixed, 60, "basalt", &vd, false);
    try addField(&fixed, 64, "", &vd, false);
    try addField(&fixed, 68, database, &vd, false);
    try addRaw(&fixed, 78, sspi, &vd);
    try addField(&fixed, 82, "", &vd, false);
    try addField(&fixed, 86, "", &vd, false);

    const total: u32 = @intCast(94 + vd.items.len);
    std.mem.writeInt(u32, fixed[0..4], total, .little);

    const out = try gpa.alloc(u8, total);
    @memcpy(out[0..94], &fixed);
    @memcpy(out[94..], vd.items);
    return out;
}

fn addRaw(fixed: []u8, ib_pos: usize, bytes: []const u8, vd: *std.array_list.Managed(u8)) !void {
    const ib: u16 = @intCast(94 + vd.items.len);
    try vd.appendSlice(bytes);
    std.mem.writeInt(u16, fixed[ib_pos..][0..2], ib, .little);
    std.mem.writeInt(u16, fixed[ib_pos + 2 ..][0..2], @intCast(bytes.len), .little);
}

/// Writes an ASCII string as UTF-16LE (offset, char count); creds and identifiers only.
fn addField(fixed: []u8, ib_pos: usize, s: []const u8, vd: *std.array_list.Managed(u8), obfuscate: bool) !void {
    const ib: u16 = @intCast(94 + vd.items.len);
    const start = vd.items.len;
    for (s) |ch| {
        try vd.append(ch);
        try vd.append(0);
    }
    if (obfuscate) {
        for (vd.items[start..]) |*b| {
            const swapped: u8 = ((b.* << 4) | (b.* >> 4));
            b.* = swapped ^ 0xA5;
        }
    }
    std.mem.writeInt(u16, fixed[ib_pos..][0..2], ib, .little);
    std.mem.writeInt(u16, fixed[ib_pos + 2 ..][0..2], @intCast(s.len), .little);
}

const Body = wire.Reader(.little, error.TdsProtocol);

const Token = struct { id: u8, body: []const u8 };

/// The next token of a reply that carries no rows, framed by its kind's length
/// rule; null at the end of the message or at a token kind not framed here.
fn nextToken(r: *Body) Body.Error!?Token {
    if (r.left() == 0) return null;
    const id = try r.int(u8);
    const body = switch (id) {
        0xAA, 0xAB, 0xAD, 0xED, 0xE3, 0xA9, 0xA4, 0xA5 => try r.bytes(try r.int(u16)),
        0xEE => try r.bytes(try r.int(u32)),
        0x79 => try r.bytes(4),
        0xFD, 0xFE, 0xFF => try r.bytes(12),
        0xAE => try featureExtAck(r),
        else => return null,
    };
    return .{ .id = id, .body = body };
}

/// FEATUREEXTACK: (feature id, u32 length, data) entries up to a 0xFF terminator.
fn featureExtAck(r: *Body) Body.Error![]const u8 {
    const start = r.i;
    while (try r.int(u8) != 0xFF) try r.skip(try r.int(u32));
    return r.p[start..r.i];
}

/// ERROR token body: Number(4) State(1) Class(1) MsgLen(2) Msg(UTF-16), kept as its
/// low bytes.
fn errorText(gpa: std.mem.Allocator, body: []const u8) ![]const u8 {
    if (body.len < 8) return gpa.dupe(u8, "the server sent a malformed error");
    const msglen: usize = std.mem.readInt(u16, body[6..8], .little);
    return utf16.decodeLeAlloc(.replacement, gpa, body[8..@min(body.len, 8 + msglen * 2)]);
}

test "PLP and text values past SQL Server's 2 GiB limit are refused before room is made for them" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const pkt = struct {
        fn of(comptime body: []const u8) []const u8 {
            const len = body.len + 8;
            return &[_]u8{ 0x04, STATUS_EOM, len >> 8, len & 0xff, 0, 0, 0, 0 } ++ body;
        }
    }.of;

    var plp_total = std.Io.Reader.fixed(pkt("\x00\x00\x00\x80\x00\x00\x00\x00"));
    var r1 = PacketReader.init(std.testing.allocator, &plp_total);
    defer r1.deinit();
    try std.testing.expectError(error.TdsProtocol, r1.readPlp(a));

    var plp_chunk = std.Io.Reader.fixed(pkt("\xfe\xff\xff\xff\xff\xff\xff\xff\x02\x00\x00\x00hi\xff\xff\xff\xff"));
    var r2 = PacketReader.init(std.testing.allocator, &plp_chunk);
    defer r2.deinit();
    try std.testing.expectError(error.TdsProtocol, r2.readPlp(a));

    var plp_short = std.Io.Reader.fixed(pkt("\xfe\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x7fabc"));
    var r3 = PacketReader.init(std.testing.allocator, &plp_short);
    defer r3.deinit();
    try std.testing.expectError(error.EndOfMessage, r3.readPlp(a));
    try std.testing.expect(arena.queryCapacity() < 4 << 20);

    var plp_ok = std.Io.Reader.fixed(pkt("\x02\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00hi\x00\x00\x00\x00"));
    var r4 = PacketReader.init(std.testing.allocator, &plp_ok);
    defer r4.deinit();
    try std.testing.expectEqualStrings("hi", (try r4.readPlp(a)).?);

    var text = std.Io.Reader.fixed(pkt("\xff\xff\xff\xff"));
    var r5 = PacketReader.init(std.testing.allocator, &text);
    defer r5.deinit();
    try std.testing.expectError(error.TdsProtocol, r5.readLob(a, try r5.readU32()));
}

test "parseLoginResponse keeps the SSPI token beside the LOGINACK" {
    var c: Conn = .{ .wire = .{ .gpa = std.testing.allocator, .stream = undefined, .msg = .init(std.testing.allocator) } };
    defer c.wire.msg.deinit();
    try c.wire.msg.appendSlice("\xED\x06\x00ap-rep\xAD\x01\x00\x00");
    try c.parseLoginResponse();
    try std.testing.expectEqualStrings("ap-rep", c.sspi_reply);
    c.wire.msg.clearRetainingCapacity();
    try c.wire.msg.appendSlice(&.{ 0xED, 2, 0, 'h', 'i' });
    try std.testing.expectError(error.LoginFailed, c.parseLoginResponse());
    try std.testing.expectEqualStrings("hi", c.sspi_reply);
    c.wire.msg.clearRetainingCapacity();
    try c.wire.msg.appendSlice(&.{ 0xED, 9, 0, 'x' });
    try std.testing.expectError(error.TdsProtocol, c.parseLoginResponse());
}

test "login7 packet has sane framing" {
    const gpa = std.testing.allocator;
    const pkt = try buildLogin7(gpa, "sa", "pw", "master", "host");
    defer gpa.free(pkt);
    try std.testing.expectEqual(@as(u32, @intCast(pkt.len)), std.mem.readInt(u32, pkt[0..4], .little));
    const ib_user = std.mem.readInt(u16, pkt[40..42], .little);
    const cch_user = std.mem.readInt(u16, pkt[42..44], .little);
    try std.testing.expectEqual(@as(u16, 2), cch_user);
    try std.testing.expect(ib_user >= 94 and ib_user < pkt.len);
    try std.testing.expectEqualSlices(u8, "s\x00a\x00", pkt[ib_user..][0..4]);

    const ib_pw = std.mem.readInt(u16, pkt[44..46], .little);
    try std.testing.expectEqual(@as(u16, 2), std.mem.readInt(u16, pkt[46..48], .little));
    try std.testing.expectEqualSlices(u8, &.{ 0xA2, 0xA5, 0xD2, 0xA5 }, pkt[ib_pw..][0..4]);

    const ib_host = std.mem.readInt(u16, pkt[36..38], .little);
    try std.testing.expectEqual(@as(u16, 4), std.mem.readInt(u16, pkt[38..40], .little));
    try std.testing.expectEqualSlices(u8, "h\x00o\x00s\x00t\x00", pkt[ib_host..][0..8]);

    const ib_db = std.mem.readInt(u16, pkt[68..70], .little);
    try std.testing.expectEqual(@as(u16, 6), std.mem.readInt(u16, pkt[70..72], .little));
    try std.testing.expectEqualSlices(u8, "m\x00a\x00s\x00t\x00e\x00r\x00", pkt[ib_db..][0..12]);
    try std.testing.expectEqual(pkt.len, ib_db + 12);
}

test "buildLogin7Fedauth: fExtension flag, empty creds, FEDAUTH ext layout" {
    const a = std.testing.allocator;
    const token = "HEADER.PAYLOAD.SIG";
    const out = try buildLogin7Fedauth(a, token, "db", "h", true, null);
    defer a.free(out);

    try std.testing.expectEqual(@as(u32, @intCast(out.len)), std.mem.readInt(u32, out[0..4], .little));
    try std.testing.expectEqual(@as(u8, 0x10), out[27] & 0x10);
    try std.testing.expectEqual(@as(u16, 0), std.mem.readInt(u16, out[42..44], .little));
    try std.testing.expectEqual(@as(u16, 0), std.mem.readInt(u16, out[46..48], .little));

    const ext_ib = std.mem.readInt(u16, out[56..58], .little);
    try std.testing.expectEqual(@as(u16, 4), std.mem.readInt(u16, out[58..60], .little));
    const feat_off = std.mem.readInt(u32, out[ext_ib..][0..4], .little);
    try std.testing.expectEqual(@as(u8, 0x02), out[feat_off]);
    const opt = out[feat_off + 5];
    try std.testing.expectEqual(@as(u8, 0x03), opt);
    const tlen = std.mem.readInt(u32, out[feat_off + 6 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, token.len * 2), tlen);
    try std.testing.expectEqual(@as(u8, 0xFF), out[out.len - 1]);
    const dlen = std.mem.readInt(u32, out[feat_off + 1 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 1 + 4 + token.len * 2), dlen);
}

test "buildLogin7Sspi: fIntSecurity set, creds empty, blob verbatim at ibSSPI" {
    const a = std.testing.allocator;
    const blob = [_]u8{ 'N', 'T', 'L', 'M', 'S', 'S', 'P', 0, 1, 0, 0, 0 };
    const out = try buildLogin7Sspi(a, &blob, "db", "h");
    defer a.free(out);

    try std.testing.expectEqual(@as(u32, @intCast(out.len)), std.mem.readInt(u32, out[0..4], .little));
    try std.testing.expectEqual(@as(u8, 0x80), out[25] & 0x80);
    try std.testing.expectEqual(@as(u16, 0), std.mem.readInt(u16, out[42..44], .little));
    try std.testing.expectEqual(@as(u16, 0), std.mem.readInt(u16, out[46..48], .little));

    const ib_sspi = std.mem.readInt(u16, out[78..80], .little);
    try std.testing.expectEqual(@as(u16, blob.len), std.mem.readInt(u16, out[80..82], .little));
    try std.testing.expect(ib_sspi >= 94 and ib_sspi + blob.len <= out.len);
    try std.testing.expectEqualSlices(u8, &blob, out[ib_sspi..][0..blob.len]);
    try std.testing.expectEqual(@as(u32, 0), std.mem.readInt(u32, out[90..94], .little));
}

test "buildLogin7Fedauth: nonce appended when echo && nonce present" {
    const a = std.testing.allocator;
    var nonce: [32]u8 = undefined;
    for (&nonce, 0..) |*b, i| b.* = @intCast(i);
    const out = try buildLogin7Fedauth(a, "tok", "db", "h", true, nonce);
    defer a.free(out);
    const ext_ib = std.mem.readInt(u16, out[56..58], .little);
    const feat_off = std.mem.readInt(u32, out[ext_ib..][0..4], .little);
    const dlen = std.mem.readInt(u32, out[feat_off + 1 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 1 + 4 + 3 * 2 + 32), dlen);
    try std.testing.expectEqualSlices(u8, &nonce, out[out.len - 33 .. out.len - 1]);
}

/// Sweeps the column-description space from the first two bytes and feeds the rest
/// as a cell, into a fixed buffer so a hostile size gets OutOfMemory, not the memory.
fn fuzzCell(_: void, input: []const u8) anyerror!void {
    if (input.len < 2) return;
    const kinds = [_]types.TypeKind{ .int, .bool, .float, .decimal, .string, .bytes, .date, .time, .timestamp };
    const kind = kinds[input[0] % kinds.len];
    const flags = input[1];
    var ty = types.Type.init(kind);
    ty.scale = flags % 8;
    var d = ColumnDesc{
        .tds_type = input[0],
        .engine_type = ty,
        .kind = .bytelen,
        .scale = flags % 8,
        .is_unicode = flags & 1 != 0,
        .is_guid = flags & 2 != 0,
        .is_binary = flags & 4 != 0,
        .is_money = flags & 8 != 0,
    };
    d.decode = decodeOf(d);
    var mem: [256 * 1024]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&mem);
    var arena = std.heap.ArenaAllocator.init(fba.allocator());
    defer arena.deinit();
    _ = cellValue(arena.allocator(), d, input[2..]) catch return;
}

/// One cell's bytes through its column's decode, as a run appends them, read back.
fn cellValue(a: std.mem.Allocator, d: ColumnDesc, raw: []const u8) !Value {
    var b = column.Builder.init(a, d.engine_type);
    try appendCell(&b, d, raw, a);
    return (try b.finish()).getValue(0);
}

const fuzzCell_corpus = [_][]const u8{
    "\x00\x00\x2a\x00\x00\x00\x00\x00\x00\x00",
    "\x03\x08\x01\x00\xd2\x04",
    "\x04\x01A\x00B\x00",
    "\x06\x00\xda\xb9\x0a\x00",
};

test "fuzz: row cell decode survives arbitrary bytes" {
    try std.testing.fuzz({}, fuzzCell, .{ .corpus = &fuzzCell_corpus });
    try @import("../../testing/fuzzutil.zig").pound(fuzzCell, &fuzzCell_corpus);
}

test "tds: an nvarchar cell with an unpaired surrogate half fails the read, never `?` or U+FFFD (D7)" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var d = ColumnDesc{ .tds_type = 0xE7, .engine_type = types.Type.init(.string).asNullable(), .kind = .ushortlen, .is_unicode = true };
    d.decode = decodeOf(d);
    try std.testing.expectEqual(Decode.unicode_text, d.decode);
    try std.testing.expectEqualStrings("a\u{e9}😀?", (try cellValue(a, d, "a\x00\xe9\x00\x3d\xd8\x00\xde?\x00")).string);
    d.name = "nm";
    for ([_][]const u8{ "\x3d\xd8a\x00", "a\x00\x00\xde", "a\x00\x3d\xd8", "\x3d\xd8\x3d\xd8\x00\xde" }) |raw| {
        try std.testing.expectError(error.LoneSurrogate, cellValue(a, d, raw));
        try std.testing.expect(std.mem.startsWith(u8, eval.takeFailure(error.LoneSurrogate).?, "SQL Server column `nm` holds an unpaired UTF-16 surrogate half"));
    }
    for ([_][]const u8{ "a", "a\x00b", "a\x00\x3d\xd8\x00\xde" ++ "b" }) |raw| {
        try std.testing.expectError(error.OddUtf16, cellValue(a, d, raw));
        try std.testing.expect(std.mem.startsWith(u8, eval.takeFailure(error.OddUtf16).?, "SQL Server column `nm` holds a value of an odd number of bytes"));
    }
    var prng = std.Random.DefaultPrng.init(0x16_16);
    const r = prng.random();
    const pool = [_]u16{ 'a', 'Z', ' ', 0x7F, 0x80, 0xE7, 0x20AC, 0x4E2D, 0xFFFD, 0xFFFF, 0xD83D, 0xDE00, 0xDBFF, 0xDC00 };
    for (0..20_000) |_| {
        _ = ar.reset(.retain_capacity);
        const n = r.uintLessThan(usize, 48);
        const units = try a.alloc(u16, n);
        for (units) |*u| u.* = if (r.boolean()) r.intRangeAtMost(u16, 0x20, 0x7E) else pool[r.uintLessThan(usize, pool.len)];
        const bytes = try a.alloc(u8, n * 2);
        for (units, 0..) |u, i| std.mem.writeInt(u16, bytes[i * 2 ..][0..2], u, .little);
        if (std.unicode.utf16LeToUtf8Alloc(a, units)) |want| {
            try std.testing.expectEqualStrings(want, (try cellValue(a, d, bytes)).string);
        } else |_| try std.testing.expectError(error.LoneSurrogate, cellValue(a, d, bytes));
    }
    _ = eval.takeFailure(error.LoneSurrogate);
}

test "tds: a column name with an unpaired surrogate half reads with U+FFFD, not `?` (D7)" {
    const gpa = std.testing.allocator;
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    const col = [_]u8{ 0, 0, 0, 0, 0, 0, 0xE7, 0x40, 0x00, 0x09, 0x04, 0xD0, 0x00, 0x34 };
    const payload = [_]u8{ 2, 0 } ++ col ++ [_]u8{ 3, 'a', 0, 0x3d, 0xd8, 'b', 0 } ++ col ++ [_]u8{ 3, 'c', 0, 0x3d, 0xd8, 0x00, 0xde };
    var src = std.Io.Reader.fixed(try framePackets(a, &payload, 64));
    const cur = try gpa.create(TdsCursor);
    defer gpa.destroy(cur);
    cur.* = .{ .gpa = gpa, .conn = undefined, .reader = PacketReader.init(gpa, &src), .meta_arena = undefined, .cols = &.{}, .schema = undefined, .done = false };
    defer cur.reader.deinit();
    try cur.parseColMeta(a);
    try std.testing.expectEqualStrings("a\u{FFFD}b", cur.schema.fields[0].name);
    try std.testing.expectEqualStrings("c😀", cur.schema.fields[1].name);
}

test "tds: a server error message keeps its non-ASCII text, an unpaired half as U+FFFD (D7)" {
    const gpa = std.testing.allocator;
    const body = [_]u8{ 0x10, 0x27, 0, 0, 1, 16, 5, 0 } ++ "N\x00\xe3\x00o\x00\x00\xde\x20\xac".*;
    const msg = try errorText(gpa, &body);
    defer gpa.free(msg);
    try std.testing.expectEqualStrings("N\u{e3}o\u{FFFD}\u{ac20}", msg);
    const ascii = try errorText(gpa, &([_]u8{ 0x10, 0x27, 0, 0, 1, 16, 2, 0 } ++ "o\x00k\x00".*));
    defer gpa.free(ascii);
    try std.testing.expectEqualStrings("ok", ascii);
}

test "tds: INSERT BULK names a column by its UTF-16 unit count, a pair counting two" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const fields = [_]types.Schema.Field{
        .{ .name = "a\u{e7}\u{e3}o\u{1F600}", .ty = types.Type.init(.string).asNullable() },
    };
    var w = std.Io.Writer.Allocating.init(a);
    try BulkProto.segmentHeader(&w.writer, a, .{ .fields = &fields });
    const got = w.written();
    const name = wide("a\u{e7}\u{e3}o\u{1F600}");
    try std.testing.expectEqual(@as(usize, 6), name.len / 2);
    try std.testing.expectEqual(@as(u8, 6), got[got.len - name.len - 1]);
    try std.testing.expectEqualSlices(u8, name, got[got.len - name.len ..]);
}
