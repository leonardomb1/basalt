//! Minimal TDS (SQL Server) client. Packet framing, PRELOGIN, optional TDS 7.x
//! tunneled TLS (the handshake rides inside PRELOGIN packets; all TDS traffic
//! then flows inside the session), LOGIN7 with SQL auth, SQLBatch, and a
//! streaming token-stream reader (PacketReader/TdsCursor) that pulls packets
//! on demand so ROW tokens may span packet boundaries.

const std = @import("std");
const types = @import("../lang/types.zig");
const ast = @import("../lang/ast.zig");
const column = @import("../exec/column.zig");
const batchmod = @import("../exec/batch.zig");
const valuemod = @import("../exec/value.zig");
const driver = @import("driver.zig");
const sqlmod = @import("sql.zig");

const Value = valuemod.Value;
const Batch = batchmod.Batch;

const PKT_PRELOGIN = 0x12;
const PKT_LOGIN7 = 0x10;
const PKT_SQLBATCH = 0x01;
const PKT_BULK = 0x07; // Bulk Load BCP data
const STATUS_EOM = 0x01;
const BULK_PKT_PAYLOAD = 4088; // default 4096-byte TDS packet minus the 8-byte header

pub const Error = error{ TdsProtocol, LoginFailed, QueryFailed, EncryptionRequired, TdsTlsRefused, UnsupportedTdsType } || std.mem.Allocator.Error || std.net.Stream.WriteError || std.net.Stream.ReadError;

pub const Conn = struct {
    gpa: std.mem.Allocator,
    stream: std.net.Stream,
    read_buf: [SOCK_BUF]u8 = undefined,
    write_buf: [SOCK_BUF]u8 = undefined,
    sr: std.net.Stream.Reader = undefined,
    sw: std.net.Stream.Writer = undefined,
    msg: std.array_list.Managed(u8), // reassembled response message payload (login path)
    last_error: []const u8 = "",
    tls: ?*sqlmod.TlsState = null,
    shim: TlsShim = undefined, // valid iff tls != null
    fed_required: bool = false, // server asked for federated (Azure AD) auth
    fed_nonce: ?[32]u8 = null, // server PRELOGIN nonce, echoed in the FEDAUTH ext
    // Max packet payload for outgoing SQLBatch/bulk packets. Starts at the 4096
    // default; LOGIN7 requests 16384 and the server's ENVCHANGE(4) sets the
    // negotiated value (fewer header+flush round trips per MB of bulk data).
    pkt_payload: usize = BULK_PKT_PAYLOAD,
    // Row count from the last DONE token whose DONE_COUNT flag was set; reset
    // by bulkFinish so a bulk load's ack can't be mistaken for its result.
    last_done_count: ?u64 = null,

    pub fn connect(gpa: std.mem.Allocator, host: []const u8, port: u16, user: []const u8, password: []const u8, database: []const u8, tls_mode: sqlmod.TlsMode) !*Conn {
        const stream = try std.net.tcpConnectToHost(gpa, host, port);
        driver.tuneSocket(stream.handle);
        const self = try gpa.create(Conn);
        self.* = .{ .gpa = gpa, .stream = stream, .msg = std.array_list.Managed(u8).init(gpa) };
        self.sr = std.net.Stream.Reader.init(stream, &self.read_buf);
        self.sw = std.net.Stream.Writer.init(stream, &self.write_buf);
        errdefer self.close();
        try self.prelogin(tls_mode != .off);
        if (tls_mode != .off) try self.startTls(host, tls_mode);
        try self.login(user, password, database, host);
        return self;
    }

    /// Connect with an Azure AD access token (the Dataverse / Azure SQL TDS
    /// endpoints). PRELOGIN advertises FEDAUTHREQUIRED, TLS is mandatory, and the
    /// LOGIN7 carries the token in a FEDAUTH feature extension (Security Token
    /// library) instead of a SQL password. The caller fetches the token (see
    /// aad.ropcToken); this only speaks the wire protocol.
    pub fn connectAad(gpa: std.mem.Allocator, host: []const u8, port: u16, token: []const u8, database: []const u8, tls_mode: sqlmod.TlsMode) !*Conn {
        if (tls_mode == .off) return error.EncryptionRequired; // AAD requires TLS
        const stream = try std.net.tcpConnectToHost(gpa, host, port);
        driver.tuneSocket(stream.handle);
        const self = try gpa.create(Conn);
        self.* = .{ .gpa = gpa, .stream = stream, .msg = std.array_list.Managed(u8).init(gpa) };
        self.sr = std.net.Stream.Reader.init(stream, &self.read_buf);
        self.sw = std.net.Stream.Writer.init(stream, &self.write_buf);
        errdefer self.close();
        try self.preloginFed();
        try self.startTls(host, tls_mode);
        const payload = try buildLogin7Fedauth(gpa, token, database, host, self.fed_required, self.fed_nonce);
        defer gpa.free(payload);
        try self.writePacket(PKT_LOGIN7, payload);
        try self.readMessage();
        self.parseLoginResponse() catch |e| {
            if (self.last_error.len > 0) std.debug.print("[tds] login rejected: {s}\n", .{self.last_error});
            return e;
        };
        return self;
    }

    /// TDS 7.x tunneled TLS: the handshake's TLS records are wrapped in
    /// PRELOGIN packets by `shim`; once established, the shim switches to
    /// passthrough and every TDS packet flows inside the session.
    fn startTls(self: *Conn, host: []const u8, mode: sqlmod.TlsMode) !void {
        self.shim = .{ .inner_r = self.sr.interface(), .inner_w = &self.sw.interface, .reader = undefined, .writer = undefined };
        self.shim.reader = .{ .vtable = &TlsShim.reader_vtable, .buffer = &self.shim.rbuf, .seek = 0, .end = 0 };
        self.shim.writer = .{ .vtable = &TlsShim.writer_vtable, .buffer = &self.shim.wbuf };

        const ts = try self.gpa.create(sqlmod.TlsState);
        errdefer self.gpa.destroy(ts);
        try ts.start(self.gpa, &self.shim.reader, &self.shim.writer, host, mode);
        self.tls = ts;
        self.shim.handshaking = false;
    }

    pub fn close(self: *Conn) void {
        if (self.last_error.len > 0) self.gpa.free(self.last_error);
        if (self.tls) |t| t.deinit(self.gpa);
        self.msg.deinit();
        self.stream.close();
        self.gpa.destroy(self);
    }

    /// Cleartext reader/writer: through the TLS session when enabled, else the
    /// plain socket interfaces.
    fn rd(self: *Conn) *std.Io.Reader {
        return if (self.tls) |t| &t.client.reader else self.sr.interface();
    }
    fn wr(self: *Conn) *std.Io.Writer {
        return if (self.tls) |t| &t.client.writer else &self.sw.interface;
    }
    fn flushOut(self: *Conn) !void {
        if (self.tls) |t| {
            try t.client.writer.flush(); // seal TLS records into the shim...
            try self.shim.writer.flush(); // ...forward them to the socket writer...
        }
        try self.sw.interface.flush(); // ...and push everything down the socket
    }

    pub fn sqlConn(self: *Conn) sqlmod.Conn {
        return .{ .ptr = self, .vtable = &sql_vtable };
    }

    /// Streaming cursor: send the query, then read tokens incrementally via a
    /// packet-streaming reader (bounded memory; ROW tokens may span packets).
    pub fn queryCursor(self: *Conn, sql: []const u8) !sqlmod.Cursor {
        try self.sendBatch(sql);
        const cur = try self.gpa.create(TdsCursor);
        cur.* = .{
            .gpa = self.gpa,
            .conn = self,
            // Reads through the conn's own (possibly TLS) reader — sharing one
            // buffered reader also removes the old fragile second-reader setup.
            .reader = PacketReader.init(self.gpa, self.rd()),
            .meta_arena = std.heap.ArenaAllocator.init(self.gpa),
            .cols = &.{},
            .schema = undefined,
            .done = false,
        };
        cur.readHeader() catch |e| {
            cur.reader.deinit();
            cur.meta_arena.deinit();
            self.gpa.destroy(cur);
            return e; // leave conn open: the caller owns it (can read last_error) and closes it
        };
        return .{ .ptr = cur, .vtable = &cursor_vtable };
    }

    /// Run a statement with no result set (DDL/INSERT/MERGE); errors on ERROR token.
    pub fn exec(self: *Conn, statement: []const u8) !void {
        try self.sendBatch(statement);
        try self.readMessage();
        try self.scanResultTokens();
    }

    /// Scan a server response token stream (DONE/ERROR/ENVCHANGE/…) in self.msg.
    /// Bounds-checked against truncated/malformed tokens (mirrors the guards in
    /// parseLoginResponse); an ERROR token sets last_error and returns QueryFailed.
    fn scanResultTokens(self: *Conn) !void {
        const p = self.msg.items;
        var i: usize = 0;
        while (i < p.len) {
            const token = p[i];
            i += 1;
            switch (token) {
                0xAA => { // ERROR
                    if (i + 2 > p.len) return error.TdsProtocol;
                    const len = rdU16(p, i);
                    if (i + 2 + len > p.len) return error.TdsProtocol;
                    self.last_error = try self.decodeError(p[i + 2 .. i + 2 + len]);
                    return error.QueryFailed;
                },
                0xAB, 0xE3, 0xA9, 0xA4, 0xA5 => { // length-prefixed (INFO/ENVCHANGE/…)
                    if (i + 2 > p.len) return error.TdsProtocol;
                    i += 2 + rdU16(p, i);
                },
                0x79 => i += 4, // RETURNSTATUS
                0xFD, 0xFE, 0xFF => { // DONE/DONEPROC/DONEINPROC
                    if (i + 12 > p.len) return error.TdsProtocol;
                    if (parseDoneRowCount(token, p[i .. i + 12])) |n| self.last_done_count = n;
                    i += 12;
                },
                else => break,
            }
        }
    }

    // --- packet framing ---

    /// Frame a message into one or more TDS packets of <= the negotiated packet
    /// size (4096; 4088 payload), setting EOM only on the last. Splitting matters:
    /// a SQLBatch larger than one packet (e.g. a wide UNION query) sent as a single
    /// oversized packet is reset by the server/gateway.
    fn writePacket(self: *Conn, ptype: u8, payload: []const u8) !void {
        const w = self.wr();
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
        try self.flushOut();
    }

    /// Reassemble a full message (across packets) into self.msg.
    fn readMessage(self: *Conn) !void {
        self.msg.clearRetainingCapacity();
        const r = self.rd();
        while (true) {
            var header: [8]u8 = undefined;
            try r.readSliceAll(&header);
            const len: usize = (@as(usize, header[2]) << 8) | header[3];
            if (len < 8) return error.TdsProtocol;
            const start = self.msg.items.len;
            try self.msg.resize(start + (len - 8));
            try r.readSliceAll(self.msg.items[start..]);
            if (header[1] & STATUS_EOM != 0) break;
        }
    }

    // --- prelogin ---

    /// PRELOGIN that advertises FEDAUTHREQUIRED + encryption-on (Azure AD). The
    /// option table holds VERSION, ENCRYPTION, FEDAUTHREQUIRED, then data. Parses
    /// the response for the server's FEDAUTHREQUIRED value and any 32-byte nonce.
    fn preloginFed(self: *Conn) !void {
        const payload = [_]u8{
            0x00, 0x00, 0x10, 0x00, 0x06, // VERSION         off=16 len=6
            0x01, 0x00, 0x16, 0x00, 0x01, // ENCRYPTION      off=22 len=1
            0x06, 0x00, 0x17, 0x00, 0x01, // FEDAUTHREQUIRED off=23 len=1
            0xFF, // terminator
            0x11, 0x00, 0x00, 0x00, 0x00, 0x00, // version (off 16)
            0x01, // ENCRYPT_ON (off 22) — AAD mandates TLS
            0x01, // FEDAUTHREQUIRED = yes, client supports it (off 23)
        };
        try self.writePacket(PKT_PRELOGIN, &payload);
        try self.readMessage();

        const p = self.msg.items;
        var i: usize = 0;
        while (i + 5 <= p.len and p[i] != 0xFF) : (i += 5) {
            const tok = p[i];
            const off: usize = (@as(usize, p[i + 1]) << 8) | p[i + 2];
            const len: usize = (@as(usize, p[i + 3]) << 8) | p[i + 4];
            if (off + len > p.len) continue;
            switch (tok) {
                0x01 => { // ENCRYPTION — must be on/required (we offered on)
                    if (len >= 1 and !(p[off] == 0x01 or p[off] == 0x03)) return error.TdsTlsRefused;
                },
                0x06 => self.fed_required = (len >= 1 and p[off] == 0x01), // FEDAUTHREQUIRED
                0x07 => if (len == 32) { // NONCEOPT
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
            0x00, 0x00, 0x0B, 0x00, 0x06, // VERSION  off=11 len=6
            0x01, 0x00, 0x11, 0x00, 0x01, // ENCRYPTION off=17 len=1
            0xFF, // terminator
            0x11, 0x00, 0x00, 0x00, 0x00, 0x00, // version
            if (want_tls) @as(u8, 0x01) else 0x02, // ENCRYPT_ON / ENCRYPT_NOT_SUP
        };
        try self.writePacket(PKT_PRELOGIN, &payload);
        try self.readMessage();

        // find ENCRYPTION (token 0x01) in the response option table
        const p = self.msg.items;
        var i: usize = 0;
        while (i + 5 <= p.len and p[i] != 0xFF) : (i += 5) {
            if (p[i] == 0x01) {
                const off: usize = (@as(usize, p[i + 1]) << 8) | p[i + 2];
                if (off >= p.len) return error.TdsProtocol;
                const enc_on = (p[off] == 0x01 or p[off] == 0x03); // ENCRYPT_ON / ENCRYPT_REQ
                if (want_tls and !enc_on) return error.TdsTlsRefused; // no silent plaintext downgrade
                if (!want_tls and enc_on) return error.EncryptionRequired;
            }
        }
    }

    // --- login7 ---

    fn login(self: *Conn, user: []const u8, password: []const u8, database: []const u8, host: []const u8) !void {
        const payload = try buildLogin7(self.gpa, user, password, database, host);
        defer self.gpa.free(payload);
        try self.writePacket(PKT_LOGIN7, payload);
        try self.readMessage();
        try self.parseLoginResponse();
    }

    fn parseLoginResponse(self: *Conn) !void {
        const p = self.msg.items;
        var i: usize = 0;
        var ok = false;
        while (i < p.len) {
            const token = p[i];
            i += 1;
            switch (token) {
                0xAD => { // LOGINACK
                    if (i + 2 > p.len) return error.TdsProtocol;
                    const len = rdU16(p, i);
                    i += 2 + len;
                    ok = true;
                },
                0xAA => { // ERROR
                    if (i + 2 > p.len) return error.TdsProtocol;
                    const len = rdU16(p, i);
                    self.last_error = try self.decodeError(p[i + 2 .. i + 2 + len]);
                    return error.LoginFailed;
                },
                0xAB => { // INFO
                    if (i + 2 > p.len) return error.TdsProtocol;
                    i += 2 + rdU16(p, i);
                },
                0xE3 => { // ENVCHANGE; type 4 = negotiated packet size
                    if (i + 2 > p.len) return error.TdsProtocol;
                    const len = rdU16(p, i);
                    if (i + 2 + len > p.len) return error.TdsProtocol;
                    if (parseEnvPacketSize(p[i + 2 .. i + 2 + len])) |sz| self.pkt_payload = sz - 8;
                    i += 2 + len;
                },
                0xEE => { // FEDAUTHINFO — 4-byte length + data (AAD flows)
                    if (i + 4 > p.len) return error.TdsProtocol;
                    i += 4 + std.mem.readInt(u32, p[i..][0..4], .little);
                },
                0xAE => { // FEATUREEXTACK — (FeatureId, u32 len, data)* terminated by 0xFF
                    while (i < p.len) {
                        const fid = p[i];
                        i += 1;
                        if (fid == 0xFF) break;
                        if (i + 4 > p.len) return error.TdsProtocol;
                        i += 4 + std.mem.readInt(u32, p[i..][0..4], .little);
                    }
                },
                0xFD, 0xFE, 0xFF => i += 12, // DONE*
                0x79 => i += 4, // RETURNSTATUS
                else => break,
            }
        }
        if (!ok) return error.LoginFailed;
    }

    /// ERROR token body: Number(4) State(1) Class(1) MsgLen(2) Msg(UTF16)...
    fn decodeError(self: *Conn, body: []const u8) ![]const u8 {
        if (body.len < 8) return try self.gpa.dupe(u8, "tds login error");
        const msglen: usize = rdU16(body, 6);
        const utf16 = body[8..@min(body.len, 8 + msglen * 2)];
        const out = try self.gpa.alloc(u8, utf16.len / 2);
        for (out, 0..) |*c, k| c.* = utf16[k * 2]; // ASCII slice of UTF16LE
        return out;
    }

    // --- query ---

    fn sendBatch(self: *Conn, sql: []const u8) !void {
        var payload = std.array_list.Managed(u8).init(self.gpa);
        defer payload.deinit();
        // ALL_HEADERS: a single transaction-descriptor header (no active txn)
        try payload.appendSlice(&[_]u8{
            22, 0, 0, 0, // TotalLength
            18, 0, 0, 0, // HeaderLength
            0x02, 0x00, // header type = transaction descriptor
            0, 0, 0, 0, 0, 0, 0, 0, // transaction descriptor
            1, 0, 0, 0, // outstanding request count
        });
        const u16s = try std.unicode.utf8ToUtf16LeAlloc(self.gpa, sql);
        defer self.gpa.free(u16s);
        for (u16s) |u| {
            try payload.append(@intCast(u & 0xff));
            try payload.append(@intCast(u >> 8));
        }
        try self.writePacket(PKT_SQLBATCH, payload.items);
    }

    // --- bulk load (INSERT BULK + BCP token stream) ---

    /// Send `INSERT BULK …` and read the server's acknowledgment; the bulk data
    /// then streams via `bulkPacket`, finished by `bulkFinish`.
    pub fn bulkStart(self: *Conn, insert_bulk_sql: []const u8) !void {
        try self.sendBatch(insert_bulk_sql);
        try self.readBulkResponse();
    }

    /// One Bulk Load (0x07) packet; `status` carries EOM on the final one.
    pub fn bulkPacket(self: *Conn, status: u8, payload: []const u8) !void {
        var header: [8]u8 = .{ PKT_BULK, status, 0, 0, 0, 0, 0, 0 };
        const total: u16 = @intCast(payload.len + 8);
        header[2] = @intCast(total >> 8);
        header[3] = @intCast(total & 0xff);
        const w = self.wr();
        try w.writeAll(&header);
        try w.writeAll(payload);
        try self.flushOut();
    }

    /// Read the server's response to the bulk load (DONE on success, ERROR
    /// token). Returns the DONE row count, the server's word on how many rows
    /// actually landed.
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

/// TDS 7.x tunneled TLS framing between the socket and the TLS client. While
/// `handshaking`, outgoing TLS flights are wrapped in PRELOGIN packets and the
/// server's wrapped replies are unwrapped; afterwards both directions pass
/// through raw (the TLS records themselves frame the post-login stream, with
/// whole TDS packets riding inside the session).
const TlsShim = struct {
    inner_r: *std.Io.Reader,
    inner_w: *std.Io.Writer,
    handshaking: bool = true,
    remaining: usize = 0, // unread payload bytes of the current wrapped packet
    reader: std.Io.Reader,
    writer: std.Io.Writer,
    rbuf: [shim_buf_len]u8 = undefined,
    wbuf: [shim_buf_len]u8 = undefined,

    // The TLS client requires its input reader to buffer at least one
    // ciphertext record, and asks its output writer for record-sized slices.
    const shim_buf_len = @import("tls_client.zig").min_buffer_len;
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
            // Nothing else flushes the socket during the handshake (the TLS
            // client only flushes *this* writer), so push the flight out now.
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
        // Wrap in PRELOGIN packets (EOM on the last chunk). Handshake flights
        // are small; chunking only matters if one ever exceeds a packet.
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

const sql_vtable = sqlmod.Conn.VTable{ .queryCursor = sqlQueryCursor, .exec = sqlExec, .close = sqlClose };

fn sqlQueryCursor(ptr: *anyopaque, q: []const u8) anyerror!sqlmod.Cursor {
    const self: *Conn = @ptrCast(@alignCast(ptr));
    return self.queryCursor(q);
}
fn sqlExec(ptr: *anyopaque, q: []const u8) anyerror!void {
    const self: *Conn = @ptrCast(@alignCast(ptr));
    return self.exec(q);
}
fn sqlClose(ptr: *anyopaque) void {
    const self: *Conn = @ptrCast(@alignCast(ptr));
    self.close();
}

const cursor_vtable = sqlmod.Cursor.VTable{ .schema = curSchema, .nextBatch = curNext, .close = curClose };

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

// ---------------------------------------------------------------------------
// Streaming token reader
// ---------------------------------------------------------------------------

/// Reads the TDS response as a continuous byte stream, pulling packets on
/// demand (respecting the EOM bit). Multi-byte reads transparently span packet
/// boundaries — so a ROW token straddling two packets is invisible to the parser.
const SOCK_BUF = 64 * 1024;

const PacketReader = struct {
    r: *std.Io.Reader, // the connection's cleartext reader (socket or TLS)
    buf: std.array_list.Managed(u8),
    pos: usize = 0,
    eom: bool = false,

    fn init(gpa: std.mem.Allocator, r: *std.Io.Reader) PacketReader {
        return .{ .r = r, .buf = std.array_list.Managed(u8).init(gpa) };
    }
    fn deinit(self: *PacketReader) void {
        self.buf.deinit();
    }

    /// Ensure at least one byte is available; returns false at end of message.
    fn ensure(self: *PacketReader) !bool {
        while (self.pos >= self.buf.items.len) {
            if (self.eom) return false;
            const r = self.r;
            var hdr: [8]u8 = undefined;
            r.readSliceAll(&hdr) catch |e| {
                if (e == error.EndOfStream) return false;
                return e;
            };
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
        if (!try self.ensure()) return error.EndOfMessage;
        const b = self.buf.items[self.pos];
        self.pos += 1;
        return b;
    }
    fn readBytes(self: *PacketReader, dst: []u8) !void {
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
    /// Read a PLP (Partially Length-Prefixed) value — how SQL Server frames
    /// varchar(max)/nvarchar(max)/varbinary(max): an 8-byte total length (0xFF…FF =
    /// NULL; otherwise ignored), then 4-byte-prefixed chunks until a 0-length chunk.
    fn readPlp(self: *PacketReader, arena: std.mem.Allocator) !?[]u8 {
        const total = try self.readU64();
        if (total == 0xFFFFFFFFFFFFFFFF) return null; // PLP_NULL
        var buf = std.array_list.Managed(u8).init(arena);
        while (true) {
            const chunk = try self.readU32();
            if (chunk == 0) break; // terminator
            const start = buf.items.len;
            try buf.resize(start + chunk);
            try self.readBytes(buf.items[start..]);
        }
        return try buf.toOwnedSlice();
    }
    fn readSlice(self: *PacketReader, arena: std.mem.Allocator, n: usize) ![]u8 {
        const s = try arena.alloc(u8, n);
        try self.readBytes(s);
        return s;
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

    /// Read tokens up to (and including) COLMETADATA, building the schema.
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
                0x81 => return self.parseColMeta(ma), // COLMETADATA -> header complete
                0xAA => try self.handleError(),
                0xAB, 0xE3, 0xA9, 0xA4, 0xA5 => try self.reader.skip(try self.reader.readU16()),
                0x79 => try self.reader.skip(4),
                0xFD, 0xFE, 0xFF => {
                    try self.reader.skip(12);
                    self.done = true;
                    return;
                },
                else => {
                    self.done = true;
                    return;
                },
            }
        }
    }

    fn fetchBatch(self: *TdsCursor, arena: std.mem.Allocator) !?Batch {
        if (self.done) return null;
        const ncol = self.cols.len;
        if (ncol == 0) return null;
        const builders = try arena.alloc(column.Builder, ncol);
        for (self.cols, builders) |c, *b| b.* = column.Builder.init(arena, c.engine_type);

        var n: usize = 0;
        while (n < sqlmod.STREAM_ROWS) {
            const token = self.reader.readByte() catch |e| {
                if (e == error.EndOfMessage) {
                    self.done = true;
                    break;
                }
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
                    try self.reader.skip(12);
                    self.done = true;
                    break;
                },
                0xAA => try self.handleError(),
                else => {
                    self.done = true;
                    break;
                },
            }
        }
        if (n == 0) return null;
        const out = try arena.alloc(column.Column, ncol);
        for (builders, 0..) |*b, k| out[k] = try b.finish();
        return .{ .schema = self.schema, .columns = out, .len = n };
    }

    fn parseColMeta(self: *TdsCursor, ma: std.mem.Allocator) !void {
        const count = try self.reader.readU16();
        if (count == 0xFFFF) return;
        self.cols = try ma.alloc(ColumnDesc, count);
        const fields = try ma.alloc(types.Schema.Field, count);
        for (0..count) |k| {
            try self.reader.skip(6); // UserType(4) + Flags(2)
            var d = try self.parseTypeInfo();
            const namelen: usize = try self.reader.readByte();
            const nbytes = try self.reader.readSlice(ma, namelen * 2);
            d.name = try utf16ToUtf8(ma, nbytes);
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
            0x6D, 0x6E => {
                _ = try self.reader.readByte();
                d.engine_type = floatT;
            },
            0x6F => {
                _ = try self.reader.readByte();
                d.engine_type = tsT;
            },
            0x6A, 0x6C => {
                _ = try self.reader.readByte(); // max len
                const prec = try self.reader.readByte();
                const scale = try self.reader.readByte();
                d.scale = scale;
                d.engine_type = types.Type.decimal(prec, scale).asNullable();
            },
            0x24 => { // GUIDTYPE (uniqueidentifier): 16 raw bytes -> formatted GUID string
                _ = try self.reader.readByte();
                d.engine_type = strT;
                d.is_guid = true;
            },
            0x28 => {
                d.engine_type = dateT;
            },
            0x29 => {
                d.scale = try self.reader.readByte();
                d.engine_type = strT;
            },
            0x2A => {
                d.scale = try self.reader.readByte();
                d.engine_type = tsT;
            },
            0x2B => {
                d.scale = try self.reader.readByte();
                d.engine_type = tsT;
            },
            0xA7, 0xAF => { // (BIG)VARCHAR / (BIG)CHAR — incl. varchar(max) via PLP
                const ml = try self.reader.readU16();
                try self.reader.skip(5); // collation
                d.kind = if (ml == 0xFFFF) .plp else .ushortlen;
                d.engine_type = strT;
            },
            0xE7, 0xEF => { // (BIG)NVARCHAR / NCHAR — incl. nvarchar(max) via PLP
                const ml = try self.reader.readU16();
                try self.reader.skip(5);
                d.kind = if (ml == 0xFFFF) .plp else .ushortlen;
                d.engine_type = strT;
                d.is_unicode = true;
            },
            0xA5, 0xAD => { // (BIG)VARBINARY / BINARY — incl. varbinary(max) via PLP; -> hex string
                const ml = try self.reader.readU16();
                d.kind = if (ml == 0xFFFF) .plp else .ushortlen;
                d.engine_type = strT;
                d.is_binary = true;
            },
            0xF1 => { // XMLTYPE: optional schema info, then a PLP UTF-16 value
                if (try self.reader.readByte() != 0) { // SchemaPresent
                    try self.reader.skip(@as(usize, try self.reader.readByte()) * 2); // DBName (B_VARCHAR)
                    try self.reader.skip(@as(usize, try self.reader.readByte()) * 2); // OwningSchema (B_VARCHAR)
                    try self.reader.skip(@as(usize, try self.reader.readU16()) * 2); // XmlSchemaCollection (US_VARCHAR)
                }
                d.kind = .plp;
                d.engine_type = strT;
                d.is_unicode = true;
            },
            0x22, 0x23, 0x63 => { // IMAGE / TEXT / NTEXT (legacy LOB; value is TEXTPTR-framed)
                _ = try self.reader.readU32(); // LONGLEN max length
                if (t == 0x23 or t == 0x63) try self.reader.skip(5); // collation (text/ntext only)
                // TableName: NumParts (BYTE) then that many US_VARCHAR parts
                var parts = try self.reader.readByte();
                while (parts > 0) : (parts -= 1) try self.reader.skip(@as(usize, try self.reader.readU16()) * 2);
                d.kind = .textptr;
                d.engine_type = strT;
                if (t == 0x63) d.is_unicode = true; // ntext -> UTF-16
                if (t == 0x22) d.is_binary = true; // image -> hex
            },
            else => return error.UnsupportedTdsType,
        }
        return d;
    }

    fn parseRow(self: *TdsCursor, arena: std.mem.Allocator, builders: []column.Builder, nbc: bool) !void {
        var nullbits: []const u8 = &.{};
        if (nbc) {
            const nbytes = (self.cols.len + 7) / 8;
            nullbits = try self.reader.readSlice(arena, nbytes);
        }
        for (self.cols, 0..) |col, ci| {
            if (nbc and (nullbits[ci / 8] >> @intCast(ci % 8)) & 1 != 0) {
                try builders[ci].append(.null);
                continue;
            }
            try builders[ci].append(try self.readColumnValue(arena, col));
        }
    }

    fn readColumnValue(self: *TdsCursor, arena: std.mem.Allocator, col: ColumnDesc) !Value {
        switch (col.kind) {
            .fixed => {
                const bytes = try self.reader.readSlice(arena, col.fixed_len);
                return decodeValue(arena, col, bytes);
            },
            .bytelen => {
                const len: usize = try self.reader.readByte();
                if (len == 0) return .null;
                const bytes = try self.reader.readSlice(arena, len);
                return decodeValue(arena, col, bytes);
            },
            .ushortlen => {
                const len = try self.reader.readU16();
                if (len == 0xFFFF) return .null;
                const bytes = try self.reader.readSlice(arena, len);
                return decodeValue(arena, col, bytes);
            },
            .plp => {
                const bytes = (try self.reader.readPlp(arena)) orelse return .null;
                return decodeValue(arena, col, bytes);
            },
            .textptr => { // legacy LOB: TextPtr(len) + Timestamp(8) + DataLen(4) + Data
                const ptrlen = try self.reader.readByte();
                if (ptrlen == 0) return .null;
                try self.reader.skip(@as(usize, ptrlen) + 8); // text pointer + timestamp
                const dlen = try self.reader.readU32();
                const bytes = try self.reader.readSlice(arena, dlen);
                return decodeValue(arena, col, bytes);
            },
        }
    }

    fn handleError(self: *TdsCursor) !void {
        const len = try self.reader.readU16();
        const body = try self.reader.readSlice(self.gpa, len);
        defer self.gpa.free(body);
        if (body.len >= 8) {
            const msglen = rdU16(body, 6);
            const utf16 = body[8..@min(body.len, 8 + msglen * 2)];
            const ascii = try self.gpa.alloc(u8, utf16.len / 2);
            for (ascii, 0..) |*c, k| c.* = utf16[k * 2];
            self.conn.last_error = ascii;
        }
        return error.QueryFailed;
    }

    fn closeCursor(self: *TdsCursor) void {
        self.reader.deinit();
        self.meta_arena.deinit();
        self.conn.close();
        self.gpa.destroy(self);
    }
};

// ---------------------------------------------------------------------------
// Bulk sink (INSERT BULK / BCP). Declares every column as NVARCHAR and lets SQL
// Server convert to the table's real types (created via DDL) during the bulk
// insert — one streamed BCP token sequence instead of per-batch INSERTs. Flow:
// `INSERT BULK <table> (cols nvarchar(4000))` → read the ack → stream a 0x07 Bulk
// Load packet (COLMETADATA + 0xD1 ROW tokens, ≤4088 bytes/packet, EOM on the
// last) → read the result. The COLMETADATA must byte-match what the server emits
// for these columns (verified via SELECT … WHERE 1=0): the key detail was the
// flags = 0x0008. Append/overwrite only; upsert routes to the INSERT sink.
// ---------------------------------------------------------------------------

const NVARCHAR_MAX_BYTES = 8000; // must match the table's NVARCHAR(4000) = 8000 bytes
const BULK_COLLATION = [5]u8{ 0x09, 0x04, 0xD0, 0x00, 0x34 }; // SQL_Latin1_General_CP1_CI_AS

pub const BulkSink = struct {
    gpa: std.mem.Allocator,
    conn: *Conn,
    schema: types.Schema,
    // Current segment's bulk token stream (COLMETADATA + ROW tokens) — kept
    // whole until the server confirms the segment, so it can be replayed.
    buffer: std.array_list.Managed(u8),
    insert_sql: []const u8 = "", // gpa-owned; issued once per segment
    seg_rows: u64 = 0,
    redial: ?sqlmod.Redial = null,

    pub fn open(gpa: std.mem.Allocator, conn: *Conn, table_name: []const u8, schema: types.Schema, mode: ast.WriteMode, redial: ?sqlmod.Redial) !*BulkSink {
        // On error we free only what we allocate here; the caller keeps `conn`
        // (so it can read conn.last_error) and closes it on failure.
        const self = try gpa.create(BulkSink);
        errdefer gpa.destroy(self);
        // own a copy of the schema (field names) — the caller's may not outlive us
        const fields = try gpa.alloc(types.Schema.Field, schema.fields.len);
        errdefer gpa.free(fields);
        var nf: usize = 0;
        errdefer for (fields[0..nf]) |f| gpa.free(f.name);
        for (schema.fields, fields) |f, *o| {
            o.* = .{ .name = try gpa.dupe(u8, f.name), .ty = f.ty };
            nf += 1;
        }
        self.* = .{ .gpa = gpa, .conn = conn, .schema = .{ .fields = fields }, .buffer = std.array_list.Managed(u8).init(gpa), .redial = redial };
        errdefer self.buffer.deinit();

        var aa = std.heap.ArenaAllocator.init(gpa);
        defer aa.deinit();
        const a = aa.allocator();
        const qtable = try sqlmod.quoteIdent(a, .sqlserver, table_name);
        try conn.exec(try sqlmod.createTableSql(a, .sqlserver, qtable, schema, mode));
        if (mode == .overwrite) try conn.exec(try std.fmt.allocPrint(a, "DELETE FROM {s}", .{qtable}));

        var cols = std.array_list.Managed(u8).init(a);
        for (schema.fields, 0..) |f, i| {
            if (i > 0) try cols.appendSlice(", ");
            try cols.appendSlice(try sqlmod.quoteIdent(a, .sqlserver, f.name));
            try cols.appendSlice(" nvarchar(4000)");
        }
        self.insert_sql = try std.fmt.allocPrint(gpa, "INSERT BULK {s} ({s})", .{ qtable, cols.items });
        try self.writeColMetadata(); // each segment's stream starts with COLMETADATA
        return self;
    }

    pub fn sink(self: *BulkSink) driver.Sink {
        return .{ .ptr = self, .vtable = &bulk_vtable };
    }

    fn writeColMetadata(self: *BulkSink) !void {
        const w = self.buffer.writer();
        try w.writeByte(0x81); // COLMETADATA
        try writeU16(w, @intCast(self.schema.fields.len));
        for (self.schema.fields) |f| {
            try writeU32(w, 0); // UserType (TDS 7.2+ : 4 bytes)
            try writeU16(w, 0x0009); // Flags: fNullable | usUpdateable=read-write — without fNullable the server silently drops rows containing a NULL
            try w.writeByte(0xE7); // NVARCHARTYPE
            try writeU16(w, NVARCHAR_MAX_BYTES);
            try w.writeAll(&BULK_COLLATION);
            // Column name as B_VARCHAR (1-byte char count + UCS2), like SqlBulkCopy.
            const name16 = try std.unicode.utf8ToUtf16LeAlloc(self.gpa, f.name);
            defer self.gpa.free(name16);
            try w.writeByte(@intCast(name16.len));
            for (name16) |u| {
                try w.writeByte(@intCast(u & 0xff));
                try w.writeByte(@intCast(u >> 8));
            }
        }
    }

    fn writeBatch(self: *BulkSink, arena: std.mem.Allocator, batch: Batch) !void {
        const w = self.buffer.writer();
        const fmt = sqlmod.BulkFormat{ .bool_true = "1", .bool_false = "0" };
        var r: usize = 0;
        while (r < batch.len) : (r += 1) {
            try w.writeByte(0xD1); // ROW
            for (batch.columns) |*col| {
                const v = col.getValue(r);
                if (v.isNull()) {
                    try writeU16(w, 0xFFFF); // CHARBIN_NULL
                    continue;
                }
                const u16s = try std.unicode.utf8ToUtf16LeAlloc(arena, try sqlmod.valueText(arena, v, fmt));
                var blen = @min(u16s.len * 2, NVARCHAR_MAX_BYTES);
                // Don't truncate in the middle of a surrogate pair: if the last kept
                // code unit is a high surrogate, drop it so we never emit a lone half.
                if (blen < u16s.len * 2 and blen >= 2) {
                    const last = u16s[blen / 2 - 1];
                    if (last >= 0xD800 and last <= 0xDBFF) blen -= 2;
                }
                try writeU16(w, @intCast(blen));
                var k: usize = 0;
                while (k * 2 < blen) : (k += 1) {
                    try w.writeByte(@intCast(u16s[k] & 0xff));
                    try w.writeByte(@intCast(u16s[k] >> 8));
                }
            }
        }
        self.seg_rows += batch.len;
        if (self.buffer.items.len >= sqlmod.SEGMENT_BYTES) {
            try self.commitSegment();
            try self.writeColMetadata(); // next segment's stream header
        }
    }

    /// Transmit the buffered segment as one INSERT BULK statement and verify the
    /// DONE row count. Transient failure → redial once and resend the intact
    /// segment (the server rolls back a bulk batch when its connection dies).
    /// Same lost-reply double-write window as the other bulk sinks.
    fn commitSegment(self: *BulkSink) !void {
        if (self.seg_rows == 0) {
            self.buffer.clearRetainingCapacity(); // drop the unused COLMETADATA header
            return;
        }
        self.sendSegment() catch |e| {
            const rd = self.redial orelse return e;
            if (!driver.transientNet(e)) return e;
            const fresh = try rd.dial(rd.ctx, self.gpa);
            self.conn.close();
            // Redial ctx is kind-matched: the vtable ptr is always a *tds.Conn.
            self.conn = @ptrCast(@alignCast(fresh.ptr));
            try self.sendSegment();
        };
        self.buffer.clearRetainingCapacity();
        self.seg_rows = 0;
    }

    fn sendSegment(self: *BulkSink) !void {
        try self.conn.bulkStart(self.insert_sql);
        var off: usize = 0;
        while (self.buffer.items.len - off > self.conn.pkt_payload) {
            try self.conn.bulkPacket(0x00, self.buffer.items[off .. off + self.conn.pkt_payload]);
            off += self.conn.pkt_payload;
        }
        try self.conn.bulkPacket(STATUS_EOM, self.buffer.items[off..]);
        const n = (try self.conn.bulkFinish()) orelse self.seg_rows; // no DONE_COUNT → trust ERROR-token detection
        if (n != self.seg_rows) {
            if (self.conn.last_error.len == 0)
                self.conn.last_error = try std.fmt.allocPrint(self.gpa, "INSERT BULK count mismatch: sent {d} rows, server loaded {d}", .{ self.seg_rows, n });
            return error.BulkCountMismatch;
        }
    }

    fn closeImpl(self: *BulkSink) !void {
        // Release everything even if the final segment fails — otherwise a failed
        // INSERT BULK on close leaks the connection, buffer, schema and sink.
        defer self.teardown();
        try self.commitSegment();
    }

    /// Failure path: drop the buffer and close the socket mid-INSERT BULK; the
    /// server rolls the bulk batch back when the connection dies.
    fn abortImpl(self: *BulkSink) void {
        self.teardown();
    }

    fn teardown(self: *BulkSink) void {
        self.conn.close();
        self.buffer.deinit();
        if (self.insert_sql.len > 0) self.gpa.free(self.insert_sql);
        for (self.schema.fields) |f| self.gpa.free(f.name);
        self.gpa.free(self.schema.fields);
        self.gpa.destroy(self);
    }
};

const bulk_vtable = driver.Sink.VTable{ .writeBatch = bulkWrite, .close = bulkClose, .abort = bulkAbort };

fn bulkWrite(ptr: *anyopaque, arena: std.mem.Allocator, b: Batch) anyerror!void {
    const self: *BulkSink = @ptrCast(@alignCast(ptr));
    return self.writeBatch(arena, b);
}
fn bulkClose(ptr: *anyopaque) anyerror!void {
    const self: *BulkSink = @ptrCast(@alignCast(ptr));
    return self.closeImpl();
}
fn bulkAbort(ptr: *anyopaque) void {
    const self: *BulkSink = @ptrCast(@alignCast(ptr));
    self.abortImpl();
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
    is_binary: bool = false, // SQL Server binary/varbinary(+MAX) -> hex string (CSV-safe)
};

fn decodeValue(arena: std.mem.Allocator, d: ColumnDesc, bytes: []const u8) !Value {
    return switch (d.engine_type.kind) {
        .int => .{ .int = readIntLE(bytes) },
        .bool => .{ .bool = bytes.len > 0 and bytes[0] != 0 },
        .float => .{ .float = if (bytes.len == 4) @as(f64, @as(f32, @bitCast(@as(u32, @truncate(readULE(bytes))))) ) else @bitCast(readULE(bytes)) },
        .decimal => decodeDecimal(d, bytes),
        .string => .{ .string = if (d.is_guid) try formatGuid(arena, bytes) else if (d.is_binary) try bytesToHex(arena, bytes) else if (d.is_unicode) try utf16ToUtf8(arena, bytes) else try win1252ToUtf8(arena, bytes) },
        .bytes => .{ .bytes = try arena.dupe(u8, bytes) },
        .date => .{ .date = @intCast(@as(i64, @intCast(readULE(bytes))) - 719162) },
        .timestamp => .{ .timestamp = decodeDateTime(d, bytes) },
        else => .{ .string = try arena.dupe(u8, bytes) },
    };
}

fn decodeDecimal(d: ColumnDesc, bytes: []const u8) Value {
    if (bytes.len < 1) return .null;
    const positive = bytes[0] == 1;
    var mag: i128 = 0;
    for (bytes[1..], 0..) |b, k| mag |= @as(i128, b) << @intCast(k * 8);
    return .{ .decimal = .{ .unscaled = if (positive) mag else -mag, .scale = d.scale } };
}

fn decodeDateTime(d: ColumnDesc, bytes: []const u8) i64 {
    switch (d.tds_type) {
        0x2A, 0x2B => { // DATETIME2 / DATETIMEOFFSET
            const off_bytes: usize = if (d.tds_type == 0x2B) 2 else 0;
            if (bytes.len < 3 + off_bytes) return 0;
            const tlen = bytes.len - 3 - off_bytes;
            var tu: i64 = 0;
            for (bytes[0..tlen], 0..) |b, k| tu |= @as(i64, b) << @intCast(k * 8);
            const days: i64 = @intCast(u24le(bytes[tlen .. tlen + 3]));
            const days1970 = days - 719162;
            const time_micros = @divTrunc(tu * 1_000_000, pow10(d.scale));
            return days1970 * 86_400_000_000 + time_micros;
        },
        else => { // DATETIME (8) / SMALLDATETIME (4)
            if (bytes.len >= 8) {
                const date4 = readIntLE(bytes[0..4]);
                const ticks: i64 = @intCast(readULE(bytes[4..8]));
                return (date4 - 25567) * 86_400_000_000 + @divTrunc(ticks * 1_000_000, 300);
            } else {
                const days: i64 = @intCast(rdU16(bytes, 0));
                const mins: i64 = @intCast(rdU16(bytes, 2));
                return (days - 25567) * 86_400_000_000 + mins * 60_000_000;
            }
        },
    }
}

fn pow10(n: u8) i64 {
    var r: i64 = 1;
    var k: u8 = 0;
    while (k < n) : (k += 1) r *= 10;
    return r;
}

fn u24le(b: []const u8) u32 {
    return @as(u32, b[0]) | (@as(u32, b[1]) << 8) | (@as(u32, b[2]) << 16);
}

/// Little-endian unsigned of up to 8 bytes.
fn readULE(bytes: []const u8) u64 {
    var u: u64 = 0;
    for (bytes, 0..) |b, k| {
        if (k >= 8) break;
        u |= @as(u64, b) << @intCast(k * 8);
    }
    return u;
}

/// Little-endian signed integer (sign-extended) for TDS integer widths.
fn readIntLE(bytes: []const u8) i64 {
    return switch (bytes.len) {
        0 => 0,
        1 => @as(i8, @bitCast(bytes[0])),
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

/// Map a high byte (0x80–0xFF) of Windows-1252 to its Unicode code point. 0xA0–0xFF
/// match Latin-1 (cp == byte); 0x80–0x9F carry the cp1252-specific punctuation;
/// the five undefined slots (0x81/0x8D/0x8F/0x90/0x9D) fall back to the byte value.
fn cp1252High(b: u8) u21 {
    return switch (b) {
        0x80 => 0x20AC, 0x82 => 0x201A, 0x83 => 0x0192, 0x84 => 0x201E, 0x85 => 0x2026,
        0x86 => 0x2020, 0x87 => 0x2021, 0x88 => 0x02C6, 0x89 => 0x2030, 0x8A => 0x0160,
        0x8B => 0x2039, 0x8C => 0x0152, 0x8E => 0x017D, 0x91 => 0x2018, 0x92 => 0x2019,
        0x93 => 0x201C, 0x94 => 0x201D, 0x95 => 0x2022, 0x96 => 0x2013, 0x97 => 0x2014,
        0x98 => 0x02DC, 0x99 => 0x2122, 0x9A => 0x0161, 0x9B => 0x203A, 0x9C => 0x0153,
        0x9E => 0x017E, 0x9F => 0x0178,
        else => b,
    };
}

/// Transcode a non-Unicode (single-byte) SQL Server char value to UTF-8, assuming
/// Windows-1252 — the code page behind the common Latin collations (e.g.
/// SQL_Latin1_General_CP1). Pure-ASCII input is duped as-is (no allocation growth).
/// Without this, accented Latin text (RELÓGIO, SINALIZAÇÃO) reaches a UTF-8 sink as
/// invalid bytes and is rejected (e.g. StarRocks "Invalid UTF-8 row").
fn win1252ToUtf8(arena: std.mem.Allocator, bytes: []const u8) ![]const u8 {
    var high = false;
    for (bytes) |b| {
        if (b >= 0x80) {
            high = true;
            break;
        }
    }
    if (!high) return arena.dupe(u8, bytes);

    var out = std.array_list.Managed(u8).init(arena);
    try out.ensureTotalCapacity(bytes.len + bytes.len / 2 + 4);
    var tmp: [4]u8 = undefined;
    for (bytes) |b| {
        const cp: u21 = if (b < 0x80) b else cp1252High(b);
        const n = std.unicode.utf8Encode(cp, &tmp) catch unreachable; // cp1252 maps only to valid scalars
        try out.appendSlice(tmp[0..n]);
    }
    return out.toOwnedSlice();
}

/// Format a SQL Server `uniqueidentifier` (16 wire bytes, mixed-endian) as the
/// canonical lowercase GUID string. Decoding it as raw text would emit control
/// bytes (incl. the load separator/newline) and corrupt a delimited bulk load.
/// Render binary as a `0x…` lowercase hex string (CSV/Stream-Load safe — raw
/// binary bytes would otherwise carry separators/newlines and corrupt the load).
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

fn formatGuid(arena: std.mem.Allocator, b: []const u8) ![]const u8 {
    if (b.len < 16) return arena.dupe(u8, "");
    return std.fmt.allocPrint(arena, "{x:0>2}{x:0>2}{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}", .{
        b[3], b[2], b[1], b[0], b[5], b[4], b[7], b[6], b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15],
    });
}

fn utf16ToUtf8(arena: std.mem.Allocator, bytes: []const u8) ![]const u8 {
    var out = std.array_list.Managed(u8).init(arena);
    var k: usize = 0;
    while (k + 1 < bytes.len) : (k += 2) {
        const cu = @as(u16, bytes[k]) | (@as(u16, bytes[k + 1]) << 8);
        var tmp: [4]u8 = undefined;
        const n = std.unicode.utf8Encode(cu, &tmp) catch {
            try out.append('?');
            continue;
        };
        try out.appendSlice(tmp[0..n]);
    }
    return out.toOwnedSlice();
}

test "bytes to hex string" {
    const alloc = std.testing.allocator;
    const out = try bytesToHex(alloc, &[_]u8{ 0x00, 0x01, 0xAB, 0xFF });
    defer alloc.free(out);
    try std.testing.expectEqualStrings("0x0001abff", out);
}

test "format sql server guid (mixed-endian)" {
    const alloc = std.testing.allocator;
    const b = [_]u8{ 0x10, 0x32, 0x54, 0x76, 0x98, 0xBA, 0xDC, 0xFE, 0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF };
    const out = try formatGuid(alloc, &b);
    defer alloc.free(out);
    try std.testing.expectEqualStrings("76543210-ba98-fedc-0123-456789abcdef", out);
}

test "win1252 transcode to utf8" {
    const alloc = std.testing.allocator;
    // 0xD3 is 'Ó' in Windows-1252/Latin-1 (the RELÓGIO case from the SQL Server data)
    const out = try win1252ToUtf8(alloc, &[_]u8{ 'R', 'E', 'L', 0xD3, 'G', 'I', 'O' });
    defer alloc.free(out);
    try std.testing.expectEqualStrings("RELÓGIO", out);
    // pure ASCII passes through unchanged
    const a = try win1252ToUtf8(alloc, "plain");
    defer alloc.free(a);
    try std.testing.expectEqualStrings("plain", a);
    // 0x80 is the cp1252-specific euro sign (not Latin-1)
    const e = try win1252ToUtf8(alloc, &[_]u8{0x80});
    defer alloc.free(e);
    try std.testing.expectEqualStrings("€", e);
}

test "readIntLE sign-extends every TDS integer width" {
    try std.testing.expectEqual(@as(i64, 0), readIntLE(&.{}));
    try std.testing.expectEqual(@as(i64, -1), readIntLE(&.{0xFF})); // tinyint as i8
    try std.testing.expectEqual(@as(i64, 127), readIntLE(&.{0x7F}));
    try std.testing.expectEqual(@as(i64, -2), readIntLE(&.{ 0xFE, 0xFF })); // smallint
    try std.testing.expectEqual(@as(i64, -1), readIntLE(&.{ 0xFF, 0xFF, 0xFF, 0xFF })); // int
    try std.testing.expectEqual(@as(i64, 1), readIntLE(&.{ 1, 0, 0, 0, 0, 0, 0, 0 })); // bigint
    try std.testing.expectEqual(std.math.minInt(i64), readIntLE(&.{ 0, 0, 0, 0, 0, 0, 0, 0x80 }));
    // odd width (3 bytes) takes the sign-extension fallback
    try std.testing.expectEqual(@as(i64, -1), readIntLE(&.{ 0xFF, 0xFF, 0xFF }));
    try std.testing.expectEqual(@as(i64, 0x010203), readIntLE(&.{ 0x03, 0x02, 0x01 }));
}

test "decodeDecimal: sign byte + little-endian magnitude at the column scale" {
    const d = ColumnDesc{ .tds_type = 0x6C, .engine_type = types.Type.decimal(10, 2).asNullable(), .kind = .bytelen, .scale = 2 };
    // sign=1 (positive), magnitude 0x3039 = 12345 -> 123.45
    const pos = decodeDecimal(d, &.{ 1, 0x39, 0x30, 0, 0 });
    try std.testing.expectEqual(@as(i128, 12345), pos.decimal.unscaled);
    try std.testing.expectEqual(@as(u8, 2), pos.decimal.scale);
    // sign=0 -> negative
    const neg = decodeDecimal(d, &.{ 0, 0x39, 0x30, 0, 0 });
    try std.testing.expectEqual(@as(i128, -12345), neg.decimal.unscaled);
    try std.testing.expect(decodeDecimal(d, &.{}) == .null);
}

test "decodeDateTime: DATETIME ticks, SMALLDATETIME minutes, DATETIME2 scale" {
    // DATETIME: days since 1900 (25567 = 1970-01-01), 1/300s ticks. 300 ticks = 1s.
    var dt = ColumnDesc{ .tds_type = 0x3D, .engine_type = types.Type.init(.timestamp).asNullable(), .kind = .fixed, .fixed_len = 8 };
    var bytes8: [8]u8 = undefined;
    std.mem.writeInt(i32, bytes8[0..4], 25567, .little);
    std.mem.writeInt(u32, bytes8[4..8], 300, .little);
    try std.testing.expectEqual(@as(i64, 1_000_000), decodeDateTime(dt, &bytes8));

    // SMALLDATETIME: days(2) + minutes(2). One day + 90 min past epoch.
    dt.tds_type = 0x3A;
    var bytes4: [4]u8 = undefined;
    std.mem.writeInt(u16, bytes4[0..2], 25568, .little);
    std.mem.writeInt(u16, bytes4[2..4], 90, .little);
    try std.testing.expectEqual(@as(i64, 86_400_000_000 + 90 * 60_000_000), decodeDateTime(dt, &bytes4));

    // DATETIME2(3): time units at 10^-3 s in N bytes, then 3-byte days since 0001-01-01
    // (719162 = 1970-01-01). 1500ms -> 1.5s.
    const dt2 = ColumnDesc{ .tds_type = 0x2A, .engine_type = types.Type.init(.timestamp).asNullable(), .kind = .bytelen, .scale = 3 };
    var b7: [7]u8 = .{ 0, 0, 0, 0, 0, 0, 0 };
    std.mem.writeInt(u32, b7[0..4], 1500, .little); // fits in the low 4 time bytes
    const days: u24 = 719162 + 1; // 1970-01-02
    b7[4] = @intCast(days & 0xFF);
    b7[5] = @intCast((days >> 8) & 0xFF);
    b7[6] = @intCast(days >> 16);
    try std.testing.expectEqual(@as(i64, 86_400_000_000 + 1_500_000), decodeDateTime(dt2, &b7));
}

test "utf16ToUtf8 decodes BMP text and replaces invalid units" {
    const alloc = std.testing.allocator;
    const ok = try utf16ToUtf8(alloc, "h\x00i\x00\xe9\x00"); // "hié" in UTF-16LE
    defer alloc.free(ok);
    try std.testing.expectEqualStrings("hié", ok);
    // a lone surrogate half becomes '?', not invalid UTF-8
    const bad = try utf16ToUtf8(alloc, "\x00\xd8");
    defer alloc.free(bad);
    try std.testing.expectEqualStrings("?", bad);
}

/// DONE token body (status u16, curcmd u16, rowcount u64, all LE) → the count
/// when this is a final DONE (0xFD) with the DONE_COUNT flag (0x10) set.
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

/// ENVCHANGE data → negotiated packet size, or null if it's another env type or
/// malformed. Layout: Type u8; type 4's NewValue is a B_VARCHAR (char count,
/// UCS-2 digits). Accepts the server's value only within the spec's 512-32767.
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
    // type 4, "16384" as UCS-2
    const good = [_]u8{ 4, 5, '1', 0, '6', 0, '3', 0, '8', 0, '4', 0 };
    try std.testing.expectEqual(@as(?usize, 16384), parseEnvPacketSize(&good));
    const wrong_type = [_]u8{ 1, 5, '1', 0, '6', 0, '3', 0, '8', 0, '4', 0 };
    try std.testing.expectEqual(@as(?usize, null), parseEnvPacketSize(&wrong_type));
    const non_digit = [_]u8{ 4, 2, 'x', 0, '1', 0 };
    try std.testing.expectEqual(@as(?usize, null), parseEnvPacketSize(&non_digit));
    const truncated = [_]u8{ 4, 5, '1', 0 };
    try std.testing.expectEqual(@as(?usize, null), parseEnvPacketSize(&truncated));
    const out_of_range = [_]u8{ 4, 2, '6', 0, '4', 0 }; // 64 < 512
    try std.testing.expectEqual(@as(?usize, null), parseEnvPacketSize(&out_of_range));
}

// --- LOGIN7 construction ---

fn buildLogin7(gpa: std.mem.Allocator, user: []const u8, password: []const u8, database: []const u8, host: []const u8) ![]u8 {
    var fixed = std.mem.zeroes([94]u8);
    // TDSVersion 7.4 = 0x74000004 (LE), PacketSize 16384 requested; the server
    // answers with ENVCHANGE(4) carrying the granted size (see parseLoginResponse).
    fixed[4] = 0x04;
    fixed[7] = 0x74;
    fixed[8] = 0x00;
    fixed[9] = 0x40; // 0x4000 = 16384

    var vd = std.array_list.Managed(u8).init(gpa);
    defer vd.deinit();

    try addField(&fixed, 36, host, &vd, false); // HostName
    try addField(&fixed, 40, user, &vd, false); // UserName
    try addField(&fixed, 44, password, &vd, true); // Password (obfuscated)
    try addField(&fixed, 48, "basalt", &vd, false); // AppName
    try addField(&fixed, 52, host, &vd, false); // ServerName
    try addField(&fixed, 56, "", &vd, false); // Extension (unused)
    try addField(&fixed, 60, "basalt", &vd, false); // CltIntName
    try addField(&fixed, 64, "", &vd, false); // Language
    try addField(&fixed, 68, database, &vd, false); // Database
    // ClientID at 72..78 = zeros
    try addField(&fixed, 78, "", &vd, false); // SSPI
    try addField(&fixed, 82, "", &vd, false); // AtchDBFile
    try addField(&fixed, 86, "", &vd, false); // ChangePassword
    // cbSSPILong at 90..94 = 0

    const total: u32 = @intCast(94 + vd.items.len);
    std.mem.writeInt(u32, fixed[0..4], total, .little);

    const out = try gpa.alloc(u8, total);
    @memcpy(out[0..94], &fixed);
    @memcpy(out[94..], vd.items);
    return out;
}

/// LOGIN7 carrying an Azure AD access token in a FEDAUTH feature extension
/// (Security Token library) — no SQL username/password. The token rides as
/// UTF-16LE bytes; `echo` mirrors the server's FEDAUTHREQUIRED and, when set with
/// a server nonce, the nonce is appended. The OptionFlags3 fExtension bit (0x10)
/// flags the feature block, whose offset lives in a 4-byte slot referenced by the
/// repurposed "Extension" offset/length pair.
fn buildLogin7Fedauth(gpa: std.mem.Allocator, token: []const u8, database: []const u8, host: []const u8, echo: bool, nonce: ?[32]u8) ![]u8 {
    var fixed = std.mem.zeroes([94]u8);
    fixed[4] = 0x04; // TDS 7.4
    fixed[7] = 0x74;
    fixed[9] = 0x10; // packet size 4096
    fixed[27] = 0x10; // OptionFlags3: fExtension

    var vd = std.array_list.Managed(u8).init(gpa);
    defer vd.deinit();

    try addField(&fixed, 36, host, &vd, false); // HostName
    try addField(&fixed, 40, "", &vd, false); // UserName — empty (fedauth)
    try addField(&fixed, 44, "", &vd, true); // Password — empty
    try addField(&fixed, 48, "basalt", &vd, false); // AppName
    try addField(&fixed, 52, host, &vd, false); // ServerName
    // Extension field (offset 56): points at a 4-byte slot holding the FeatureExt
    // offset (filled in once all var-data is laid out).
    const ext_ib: u16 = @intCast(94 + vd.items.len);
    std.mem.writeInt(u16, fixed[56..][0..2], ext_ib, .little);
    std.mem.writeInt(u16, fixed[58..][0..2], 4, .little);
    const ext_slot = vd.items.len;
    try vd.appendSlice(&[_]u8{ 0, 0, 0, 0 });
    try addField(&fixed, 60, "basalt", &vd, false); // CltIntName
    try addField(&fixed, 64, "", &vd, false); // Language
    try addField(&fixed, 68, database, &vd, false); // Database
    // ClientID 72..78 = zero
    try addField(&fixed, 78, "", &vd, false); // SSPI
    try addField(&fixed, 82, "", &vd, false); // AtchDBFile
    try addField(&fixed, 86, "", &vd, false); // ChangePassword

    // FeatureExt begins here; backpatch the slot with its message offset.
    const feat_off: u32 = @intCast(94 + vd.items.len);
    std.mem.writeInt(u32, vd.items[ext_slot..][0..4], feat_off, .little);

    const have_nonce = echo and nonce != null;
    const tok_bytes: u32 = @intCast(token.len * 2);
    const data_len: u32 = 1 + 4 + tok_bytes + (if (have_nonce) @as(u32, 32) else 0);
    try vd.append(0x02); // FEATUREID FEDAUTH
    var lb: [4]u8 = undefined;
    std.mem.writeInt(u32, &lb, data_len, .little);
    try vd.appendSlice(&lb);
    try vd.append((0x01 << 1) | @as(u8, if (echo) 1 else 0)); // SECURITYTOKEN lib | echo
    std.mem.writeInt(u32, &lb, tok_bytes, .little);
    try vd.appendSlice(&lb);
    for (token) |ch| {
        try vd.append(ch);
        try vd.append(0); // UTF-16LE
    }
    if (have_nonce) try vd.appendSlice(&nonce.?);
    try vd.append(0xFF); // FeatureExt terminator

    const total: u32 = @intCast(94 + vd.items.len);
    std.mem.writeInt(u32, fixed[0..4], total, .little);
    const out = try gpa.alloc(u8, total);
    @memcpy(out[0..94], &fixed);
    @memcpy(out[94..], vd.items);
    return out;
}

/// Append a UTF-16LE string to var-data and write its (offset, char-count) into
/// the fixed offset/length block. ASCII only (sufficient for creds/identifiers).
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

fn rdU16(buf: []const u8, i: usize) usize {
    return @as(usize, buf[i]) | (@as(usize, buf[i + 1]) << 8);
}

test "login7 packet has sane framing" {
    const gpa = std.testing.allocator;
    const pkt = try buildLogin7(gpa, "sa", "pw", "master", "host");
    defer gpa.free(pkt);
    // length field == total length
    try std.testing.expectEqual(@as(u32, @intCast(pkt.len)), std.mem.readInt(u32, pkt[0..4], .little));
    // user offset points within the packet, length 2 chars
    const ib_user = std.mem.readInt(u16, pkt[40..42], .little);
    const cch_user = std.mem.readInt(u16, pkt[42..44], .little);
    try std.testing.expectEqual(@as(u16, 2), cch_user);
    try std.testing.expect(ib_user >= 94 and ib_user < pkt.len);
    try std.testing.expectEqual(@as(u8, 's'), pkt[ib_user]); // UTF16LE 's','\0'
}


test "buildLogin7Fedauth: fExtension flag, empty creds, FEDAUTH ext layout" {
    const a = std.testing.allocator;
    const token = "HEADER.PAYLOAD.SIG";
    const out = try buildLogin7Fedauth(a, token, "db", "h", true, null);
    defer a.free(out);

    // total length header matches buffer
    try std.testing.expectEqual(@as(u32, @intCast(out.len)), std.mem.readInt(u32, out[0..4], .little));
    // OptionFlags3 fExtension bit
    try std.testing.expectEqual(@as(u8, 0x10), out[27] & 0x10);
    // UserName (offset 40) and Password (44) are empty
    try std.testing.expectEqual(@as(u16, 0), std.mem.readInt(u16, out[42..44], .little));
    try std.testing.expectEqual(@as(u16, 0), std.mem.readInt(u16, out[46..48], .little));

    // Extension field (56): ib points at a 4-byte slot whose value is the
    // FeatureExt offset; that offset must land on FEATUREID FEDAUTH (0x02).
    const ext_ib = std.mem.readInt(u16, out[56..58], .little);
    try std.testing.expectEqual(@as(u16, 4), std.mem.readInt(u16, out[58..60], .little));
    const feat_off = std.mem.readInt(u32, out[ext_ib..][0..4], .little);
    try std.testing.expectEqual(@as(u8, 0x02), out[feat_off]); // FEDAUTH FeatureId
    // options byte = (SECURITYTOKEN<<1)|echo = 0x02|0x01
    const opt = out[feat_off + 5];
    try std.testing.expectEqual(@as(u8, 0x03), opt);
    // token length field = UTF-16LE byte length
    const tlen = std.mem.readInt(u32, out[feat_off + 6 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, token.len * 2), tlen);
    // ends with FeatureExt terminator
    try std.testing.expectEqual(@as(u8, 0xFF), out[out.len - 1]);
    // FeatureDataLen = 1 + 4 + tokbytes (no nonce since nonce==null)
    const dlen = std.mem.readInt(u32, out[feat_off + 1 ..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 1 + 4 + token.len * 2), dlen);
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
    // nonce sits right before the 0xFF terminator
    try std.testing.expectEqualSlices(u8, &nonce, out[out.len - 33 .. out.len - 1]);
}
