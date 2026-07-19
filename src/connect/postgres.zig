//! Minimal PostgreSQL v3 wire-protocol client: startup, auth (trust/cleartext/
//! md5/SCRAM-SHA-256), simple Query, and RowDescription/DataRow parsing into a
//! batch. Values arrive in text format, so the shared `sql.coerceText` does the
//! typing. Exposes the `sql.Conn` interface.

const std = @import("std");
const types = @import("../lang/types.zig");
const ast = @import("../lang/ast.zig");
const column = @import("../exec/column.zig");
const batchmod = @import("../exec/batch.zig");
const driver = @import("driver.zig");
const sqlmod = @import("sql.zig");

const Batch = batchmod.Batch;

pub const Error = error{ PgProtocol, PgAuthFailed, PgQueryFailed, PgAuthUnsupported, PgTlsRefused } || std.mem.Allocator.Error;

/// Buffered socket reads: without this, every backend message costs two recv
/// syscalls (5-byte header + body) — ~2 per row — which dominates read time and
/// caps throughput. A 64 KB buffer turns that into one syscall per ~64 KB.
const SOCK_BUF = 64 * 1024;

pub const Conn = struct {
    gpa: std.mem.Allocator,
    stream: std.net.Stream,
    read_buf: [SOCK_BUF]u8 = undefined,
    write_buf: [SOCK_BUF]u8 = undefined,
    sr: std.net.Stream.Reader = undefined,
    sw: std.net.Stream.Writer = undefined,
    payload: std.array_list.Managed(u8), // current message payload
    last_error: []const u8 = "",
    tls: ?*sqlmod.TlsState = null,
    // streaming cursor state (valid between queryCursor and close)
    meta_arena: std.heap.ArenaAllocator = undefined,
    cols: []PgCol = &.{},
    cur_schema: *types.Schema = undefined,
    done: bool = false,

    pub fn connect(gpa: std.mem.Allocator, host: []const u8, port: u16, user: []const u8, password: []const u8, database: []const u8, tls_mode: sqlmod.TlsMode) !*Conn {
        const stream = try std.net.tcpConnectToHost(gpa, host, port);
        driver.tuneSocket(stream.handle);
        const self = try gpa.create(Conn);
        self.* = .{ .gpa = gpa, .stream = stream, .payload = std.array_list.Managed(u8).init(gpa) };
        self.sr = std.net.Stream.Reader.init(stream, &self.read_buf);
        self.sw = std.net.Stream.Writer.init(stream, &self.write_buf);
        errdefer self.close();
        if (tls_mode != .off) try self.startTls(host, tls_mode);
        try self.startup(user, database);
        try self.authenticate(user, password);
        return self;
    }

    /// SSLRequest (len=8, code 80877103) → server answers one byte: 'S' starts
    /// the TLS handshake, 'N' means TLS is disabled server-side (we error rather
    /// than silently downgrading to plaintext).
    fn startTls(self: *Conn, host: []const u8, mode: sqlmod.TlsMode) !void {
        var req: [8]u8 = undefined;
        std.mem.writeInt(u32, req[0..4], 8, .big);
        std.mem.writeInt(u32, req[4..8], 80877103, .big);
        const w = &self.sw.interface;
        try w.writeAll(&req);
        try w.flush();
        var resp: [1]u8 = undefined;
        try self.sr.interface().readSliceAll(&resp);
        if (resp[0] != 'S') return error.PgTlsRefused;

        const ts = try self.gpa.create(sqlmod.TlsState);
        errdefer self.gpa.destroy(ts);
        try ts.start(self.gpa, self.sr.interface(), &self.sw.interface, host, mode);
        self.tls = ts;
    }

    pub fn close(self: *Conn) void {
        if (self.last_error.len > 0) self.gpa.free(self.last_error);
        if (self.tls) |t| t.deinit(self.gpa);
        self.payload.deinit();
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
        if (self.tls) |t| try t.client.writer.flush(); // seal TLS records...
        try self.sw.interface.flush(); // ...then push them down the socket
    }

    pub fn sqlConn(self: *Conn) sqlmod.Conn {
        return .{ .ptr = self, .vtable = &sql_vtable };
    }

    // --- handshake ---

    fn startup(self: *Conn, user: []const u8, database: []const u8) !void {
        var body = std.array_list.Managed(u8).init(self.gpa);
        defer body.deinit();
        try writeI32(body.writer(), 0x0003_0000); // protocol 3.0
        try appendCStr(&body, "user");
        try appendCStr(&body, user);
        if (database.len > 0) {
            try appendCStr(&body, "database");
            try appendCStr(&body, database);
        }
        try body.append(0); // params terminator

        // startup packet has no type byte: just length + body
        var hdr: [4]u8 = undefined;
        std.mem.writeInt(u32, &hdr, @intCast(body.items.len + 4), .big);
        const w = self.wr();
        try w.writeAll(&hdr);
        try w.writeAll(body.items);
        try self.flushOut();
    }

    fn authenticate(self: *Conn, user: []const u8, password: []const u8) !void {
        while (true) {
            const t = try self.readMsg();
            const p = self.payload.items;
            switch (t) {
                'R' => { // Authentication
                    const code = try readI32(p, 0);
                    switch (code) {
                        0 => {}, // AuthenticationOk
                        3 => try self.sendPassword(password), // cleartext
                        5 => try self.sendMd5(user, password, p[4..8]), // md5
                        10 => try self.scram(password), // SASL SCRAM-SHA-256
                        else => return error.PgAuthUnsupported,
                    }
                },
                'E' => {
                    self.last_error = try self.gpa.dupe(u8, errMessage(p));
                    return error.PgAuthFailed;
                },
                'Z' => return, // ReadyForQuery
                'S', 'K', 'N' => {}, // ParameterStatus / BackendKeyData / Notice
                else => {},
            }
        }
    }

    fn sendPassword(self: *Conn, password: []const u8) !void {
        var body = std.array_list.Managed(u8).init(self.gpa);
        defer body.deinit();
        try appendCStr(&body, password);
        try self.writeMsg('p', body.items);
    }

    fn sendMd5(self: *Conn, user: []const u8, password: []const u8, salt: []const u8) !void {
        const Md5 = std.crypto.hash.Md5;
        var inner: [16]u8 = undefined;
        var h = Md5.init(.{});
        h.update(password);
        h.update(user);
        h.final(&inner);
        const inner_hex: [32]u8 = std.fmt.bytesToHex(&inner, .lower);

        var outer: [16]u8 = undefined;
        h = Md5.init(.{});
        h.update(&inner_hex);
        h.update(salt);
        h.final(&outer);

        var body = std.array_list.Managed(u8).init(self.gpa);
        defer body.deinit();
        try body.appendSlice("md5");
        try body.appendSlice(&std.fmt.bytesToHex(&outer, .lower));
        try body.append(0);
        try self.writeMsg('p', body.items);
    }

    /// SASL SCRAM-SHA-256 (RFC 5802). The server-first/final messages are read
    /// here; the outer auth loop then continues to AuthenticationOk + ReadyForQuery.
    fn scram(self: *Conn, password: []const u8) !void {
        var aa = std.heap.ArenaAllocator.init(self.gpa);
        defer aa.deinit();
        const a = aa.allocator();

        const enc = std.base64.standard.Encoder;
        const dec = std.base64.standard.Decoder;

        // client-first
        var raw_nonce: [18]u8 = undefined;
        std.crypto.random.bytes(&raw_nonce);
        var nb: [40]u8 = undefined;
        const client_nonce = enc.encode(&nb, &raw_nonce);
        const cfb = try std.fmt.allocPrint(a, "n=,r={s}", .{client_nonce}); // client-first-bare
        const client_first = try std.fmt.allocPrint(a, "n,,{s}", .{cfb});

        var init_msg = std.array_list.Managed(u8).init(a);
        try appendCStr(&init_msg, "SCRAM-SHA-256");
        try writeI32(init_msg.writer(), @intCast(client_first.len));
        try init_msg.appendSlice(client_first);
        try self.writeMsg('p', init_msg.items);

        // server-first (R, code 11)
        if ((try self.readMsg()) != 'R' or (try readI32(self.payload.items, 0)) != 11) return error.PgAuthFailed;
        const server_first = try a.dupe(u8, self.payload.items[4..]);
        const sr = scramAttr(server_first, 'r') orelse return error.PgProtocol;
        const ss = scramAttr(server_first, 's') orelse return error.PgProtocol;
        const si = scramAttr(server_first, 'i') orelse return error.PgProtocol;
        const iters = std.fmt.parseInt(u32, si, 10) catch return error.PgProtocol;

        const salt = try a.alloc(u8, try dec.calcSizeForSlice(ss));
        try dec.decode(salt, ss);

        const keys = try scramKeys(password, salt, iters);

        const cfwp = try std.fmt.allocPrint(a, "c=biws,r={s}", .{sr}); // client-final-without-proof
        const auth_message = try std.fmt.allocPrint(a, "{s},{s},{s}", .{ cfb, server_first, cfwp });

        const proof = scramProof(keys, auth_message);
        var proof_b64: [64]u8 = undefined;
        const proof_enc = enc.encode(&proof_b64, &proof);

        const client_final = try std.fmt.allocPrint(a, "{s},p={s}", .{ cfwp, proof_enc });
        try self.writeMsg('p', client_final);

        // server-final (R, code 12) or ErrorResponse
        const t = try self.readMsg();
        if (t == 'E') {
            self.last_error = try self.gpa.dupe(u8, errMessage(self.payload.items));
            return error.PgAuthFailed;
        }
        if (t != 'R' or (try readI32(self.payload.items, 0)) != 12) return error.PgAuthFailed;

        // Verify the server signature (`v=`): proves the server also knows the
        // password derivation (mutual auth) — without this, anything that can
        // intercept the connection can pose as the server past this point.
        const sv = scramAttr(self.payload.items[4..], 'v') orelse return error.PgAuthFailed;
        var server_sig: [32]u8 = undefined;
        if ((dec.calcSizeForSlice(sv) catch return error.PgAuthFailed) != 32) return error.PgAuthFailed;
        dec.decode(&server_sig, sv) catch return error.PgAuthFailed;
        if (!std.mem.eql(u8, &server_sig, &scramServerSig(keys, auth_message))) return error.PgAuthFailed;
    }

    // --- streaming query ---

    pub fn queryCursor(self: *Conn, sql: []const u8) !sqlmod.Cursor {
        return sqlmod.openTextCursor(self, sql, &cursor_vtable);
    }

    /// Send the query and read up to RowDescription (the header); leaves the
    /// connection positioned just before the DataRows.
    pub fn openCursor(self: *Conn, sql: []const u8) !void {
        try self.sendQuery(sql);
        const ma = self.meta_arena.allocator();
        self.cols = &.{};
        self.done = false;
        const empty = try ma.create(types.Schema);
        empty.* = .{ .fields = &.{} };
        self.cur_schema = empty;
        while (true) {
            const t = try self.readMsg();
            const p = self.payload.items;
            switch (t) {
                'T' => {
                    const rowdesc = try parseRowDescription(ma, p);
                    self.cols = rowdesc.cols;
                    const sch = try ma.create(types.Schema);
                    sch.* = .{ .fields = rowdesc.fields };
                    self.cur_schema = sch;
                    return;
                },
                'C' => {}, // CommandComplete (statement with no result set)
                'E' => {
                    self.last_error = try self.gpa.dupe(u8, errMessage(p));
                    return error.PgQueryFailed;
                },
                'Z' => {
                    self.done = true;
                    return;
                },
                else => {},
            }
        }
    }

    /// One backend message, classified for `sql.fetchTextBatch`: a DataRow
    /// (values appended), ReadyForQuery (end), or a server error.
    pub fn nextRow(self: *Conn, arena: std.mem.Allocator, builders: []column.Builder) !sqlmod.RowStep {
        while (true) {
            const t = try self.readMsg();
            const p = self.payload.items;
            switch (t) {
                'D' => {
                    try parseDataRow(self, arena, p, builders);
                    return .row;
                },
                'Z' => return .end,
                'E' => {
                    self.last_error = try self.gpa.dupe(u8, errMessage(p));
                    return error.PgQueryFailed;
                },
                else => {}, // CommandComplete, notices, …
            }
        }
    }

    pub fn exec(self: *Conn, sql: []const u8) !void {
        try self.sendQuery(sql);
        while (true) {
            const t = try self.readMsg();
            switch (t) {
                'E' => {
                    self.last_error = try self.gpa.dupe(u8, errMessage(self.payload.items));
                    return error.PgQueryFailed;
                },
                'Z' => break,
                else => {},
            }
        }
    }

    fn sendQuery(self: *Conn, sql: []const u8) !void {
        var body = std.array_list.Managed(u8).init(self.gpa);
        defer body.deinit();
        try appendCStr(&body, sql);
        try self.writeMsg('Q', body.items);
    }

    // --- COPY FROM STDIN (bulk load) ---

    /// Send `COPY … FROM STDIN` and wait for the server's CopyInResponse ('G').
    pub fn copyIn(self: *Conn, cmd: []const u8) !void {
        try self.sendQuery(cmd);
        while (true) {
            switch (try self.readMsg()) {
                'G' => return, // ready to receive CopyData
                'E' => {
                    self.last_error = try self.gpa.dupe(u8, errMessage(self.payload.items));
                    return error.PgQueryFailed;
                },
                'Z' => return error.PgProtocol, // ReadyForQuery without 'G'
                else => {},
            }
        }
    }

    /// Send one CopyData chunk ('d').
    pub fn copyData(self: *Conn, data: []const u8) !void {
        try self.writeMsg('d', data);
    }

    /// Send CopyDone ('c') and drain to ReadyForQuery, surfacing any error.
    /// Returns the server's row count from the "COPY <n>" CommandComplete tag.
    pub fn copyDone(self: *Conn) !u64 {
        try self.writeMsg('c', "");
        var count: ?u64 = null;
        while (true) {
            switch (try self.readMsg()) {
                'C' => count = parseCopyCount(self.payload.items),
                'E' => {
                    self.last_error = try self.gpa.dupe(u8, errMessage(self.payload.items));
                    return error.PgQueryFailed;
                },
                'Z' => return count orelse error.PgProtocol,
                else => {},
            }
        }
    }

    // --- message framing ---

    fn writeMsg(self: *Conn, msg_type: u8, body: []const u8) !void {
        var hdr: [5]u8 = undefined;
        hdr[0] = msg_type;
        std.mem.writeInt(u32, hdr[1..5], @intCast(body.len + 4), .big);
        const w = self.wr();
        try w.writeAll(&hdr);
        try w.writeAll(body);
        try self.flushOut();
    }

    fn readMsg(self: *Conn) !u8 {
        const r = self.rd();
        var hdr: [5]u8 = undefined;
        try r.readSliceAll(&hdr);
        const len = std.mem.readInt(u32, hdr[1..5], .big);
        if (len < 4) return error.PgProtocol;
        try self.payload.resize(len - 4);
        try r.readSliceAll(self.payload.items);
        return hdr[0];
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
    const self: *Conn = @ptrCast(@alignCast(ptr));
    return self.cur_schema.*;
}
fn curNext(ptr: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
    const self: *Conn = @ptrCast(@alignCast(ptr));
    return sqlmod.fetchTextBatch(self, arena);
}
fn curClose(ptr: *anyopaque) void {
    const self: *Conn = @ptrCast(@alignCast(ptr));
    sqlmod.closeTextCursor(self);
}

// --- result parsing ---

const PgCol = struct { name: []const u8, oid: i32, engine_type: types.Type };
const RowDesc = struct { cols: []PgCol, fields: []types.Schema.Field };

fn parseRowDescription(arena: std.mem.Allocator, p: []const u8) !RowDesc {
    const n: usize = @intCast(try readI16(p, 0));
    var i: usize = 2;
    const cols = try arena.alloc(PgCol, n);
    const fields = try arena.alloc(types.Schema.Field, n);
    for (0..n) |k| {
        const name = readCStr(p, &i);
        i += 4; // table OID
        i += 2; // column attr number
        const oid = try readI32(p, i);
        i += 4;
        i += 2; // type size
        const typmod = try readI32(p, i);
        i += 4;
        i += 2; // format code
        const ty = pgType(oid, typmod);
        cols[k] = .{ .name = try arena.dupe(u8, name), .oid = oid, .engine_type = ty };
        fields[k] = .{ .name = cols[k].name, .ty = ty };
    }
    return .{ .cols = cols, .fields = fields };
}

fn parseDataRow(conn: *Conn, arena: std.mem.Allocator, p: []const u8, builders: []column.Builder) !void {
    const n: usize = @intCast(try readI16(p, 0));
    var i: usize = 2;
    for (0..n) |k| {
        const len = try readI32(p, i);
        i += 4;
        if (len < 0) {
            try builders[k].append(.null);
        } else {
            const ulen: usize = @intCast(len);
            if (i + ulen > p.len) return error.PgProtocol;
            const cell = p[i .. i + ulen];
            const v = sqlmod.coerceText(arena, cell, conn.cols[k].engine_type) catch |e| {
                if (e == error.UnparseableNumber)
                    conn.last_error = try std.fmt.allocPrint(conn.gpa, "column \"{s}\": unparseable numeric value \"{s}\"", .{ conn.cols[k].name, cell });
                return e;
            };
            try builders[k].append(v);
            i += ulen;
        }
    }
}

fn pgType(oid: i32, typmod: i32) types.Type {
    return (switch (oid) {
        16 => types.Type.init(.bool),
        20, 21, 23 => types.Type.init(.int), // int8/int2/int4
        700, 701 => types.Type.init(.float), // float4/float8
        1700 => decimalFromTypmod(typmod), // numeric
        1082 => types.Type.init(.date),
        1114, 1184 => types.Type.init(.timestamp),
        else => types.Type.init(.string),
    }).asNullable();
}

fn decimalFromTypmod(typmod: i32) types.Type {
    if (typmod < 4) return types.Type.decimal(38, 6);
    const m = typmod - 4;
    const precision: u8 = @intCast((@as(u32, @bitCast(m)) >> 16) & 0xFFFF);
    const scale: u8 = @intCast(@as(u32, @bitCast(m)) & 0xFFFF);
    return types.Type.decimal(precision, scale);
}

fn scramAttr(s: []const u8, key: u8) ?[]const u8 {
    var it = std.mem.splitScalar(u8, s, ',');
    while (it.next()) |part| {
        if (part.len >= 2 and part[0] == key and part[1] == '=') return part[2..];
    }
    return null;
}

// --- SCRAM-SHA-256 key derivation (RFC 5802/7677), pure and vector-tested ---

const HmacSha256 = std.crypto.auth.hmac.sha2.HmacSha256;

const ScramKeys = struct {
    client_key: [32]u8,
    stored_key: [32]u8,
    server_key: [32]u8,
};

fn scramKeys(password: []const u8, salt: []const u8, iters: u32) !ScramKeys {
    var salted: [32]u8 = undefined;
    try std.crypto.pwhash.pbkdf2(&salted, password, salt, iters, HmacSha256);
    var keys: ScramKeys = undefined;
    HmacSha256.create(&keys.client_key, "Client Key", &salted);
    std.crypto.hash.sha2.Sha256.hash(&keys.client_key, &keys.stored_key, .{});
    HmacSha256.create(&keys.server_key, "Server Key", &salted);
    return keys;
}

/// ClientProof = ClientKey XOR HMAC(StoredKey, AuthMessage)
fn scramProof(keys: ScramKeys, auth_message: []const u8) [32]u8 {
    var client_sig: [32]u8 = undefined;
    HmacSha256.create(&client_sig, auth_message, &keys.stored_key);
    var proof: [32]u8 = undefined;
    for (&proof, 0..) |*b, k| b.* = keys.client_key[k] ^ client_sig[k];
    return proof;
}

/// ServerSignature = HMAC(ServerKey, AuthMessage) — what the server's `v=` must equal.
fn scramServerSig(keys: ScramKeys, auth_message: []const u8) [32]u8 {
    var sig: [32]u8 = undefined;
    HmacSha256.create(&sig, auth_message, &keys.server_key);
    return sig;
}

test "SCRAM-SHA-256 proof and server signature match the RFC 7677 vector" {
    // RFC 7677 §3: user "user", password "pencil", i=4096.
    const dec = std.base64.standard.Decoder;
    const enc = std.base64.standard.Encoder;

    var salt: [16]u8 = undefined;
    try dec.decode(&salt, "W22ZaJ0SNY7soEsUEjb6gQ==");
    const keys = try scramKeys("pencil", &salt, 4096);

    const auth_message = "n=user,r=rOprNGfwEbeRWgbNEkqO," ++
        "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096," ++
        "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";

    var b64: [64]u8 = undefined;
    const proof = scramProof(keys, auth_message);
    try std.testing.expectEqualStrings("dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=", enc.encode(&b64, &proof));

    const sig = scramServerSig(keys, auth_message);
    try std.testing.expectEqualStrings("6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=", enc.encode(&b64, &sig));
}

test "pgType maps OIDs; numeric typmod carries precision and scale" {
    try std.testing.expectEqual(types.TypeKind.bool, pgType(16, -1).kind);
    try std.testing.expectEqual(types.TypeKind.int, pgType(20, -1).kind); // int8
    try std.testing.expectEqual(types.TypeKind.int, pgType(23, -1).kind); // int4
    try std.testing.expectEqual(types.TypeKind.float, pgType(701, -1).kind); // float8
    try std.testing.expectEqual(types.TypeKind.date, pgType(1082, -1).kind);
    try std.testing.expectEqual(types.TypeKind.timestamp, pgType(1184, -1).kind); // timestamptz
    try std.testing.expectEqual(types.TypeKind.string, pgType(25, -1).kind); // text -> string
    try std.testing.expect(pgType(23, -1).nullable); // wire types are always nullable

    // NUMERIC(10,2): typmod = (precision << 16 | scale) + 4
    const d = pgType(1700, (10 << 16 | 2) + 4);
    try std.testing.expectEqual(types.TypeKind.decimal, d.kind);
    try std.testing.expectEqual(@as(u8, 10), d.precision);
    try std.testing.expectEqual(@as(u8, 2), d.scale);
    // unconstrained NUMERIC (typmod -1) falls back to (38,6)
    const u = pgType(1700, -1);
    try std.testing.expectEqual(@as(u8, 38), u.precision);
    try std.testing.expectEqual(@as(u8, 6), u.scale);
}

test "ErrorResponse extraction picks the M field" {
    // (type byte, cstr) pairs: severity, code, then message
    const payload = "SFATAL\x00C28P01\x00Mpassword authentication failed\x00\x00";
    try std.testing.expectEqualStrings("password authentication failed", errMessage(payload));
    // no M field -> stable fallback
    try std.testing.expectEqualStrings("postgres error", errMessage("SERROR\x00\x00"));
}

test "scramAttr finds comma-separated attributes exactly" {
    const server_first = "r=abcdef,s=c2FsdA==,i=4096";
    try std.testing.expectEqualStrings("abcdef", scramAttr(server_first, 'r').?);
    try std.testing.expectEqualStrings("c2FsdA==", scramAttr(server_first, 's').?);
    try std.testing.expectEqualStrings("4096", scramAttr(server_first, 'i').?);
    try std.testing.expect(scramAttr(server_first, 'v') == null);
    // value containing '=' (base64 padding) is returned whole
    try std.testing.expect(scramAttr("x=,r=a", 'x').?.len == 0);
}

test "parseRowDescription decodes names, OIDs, and column order" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    // 2 columns: id (int4, oid 23) and name (text, oid 25); big-endian fields
    var p = std.array_list.Managed(u8).init(a);
    try p.appendSlice(&.{ 0, 2 }); // column count
    try appendCStr(&p, "id");
    try p.appendSlice(&.{ 0, 0, 0, 0 }); // table oid
    try p.appendSlice(&.{ 0, 0 }); // attr number
    try p.appendSlice(&.{ 0, 0, 0, 23 }); // type oid int4
    try p.appendSlice(&.{ 0, 4 }); // type size
    try p.appendSlice(&.{ 0xFF, 0xFF, 0xFF, 0xFF }); // typmod -1
    try p.appendSlice(&.{ 0, 0 }); // format code
    try appendCStr(&p, "name");
    try p.appendSlice(&.{ 0, 0, 0, 0, 0, 0 });
    try p.appendSlice(&.{ 0, 0, 0, 25 }); // text
    try p.appendSlice(&.{ 0xFF, 0xFF });
    try p.appendSlice(&.{ 0xFF, 0xFF, 0xFF, 0xFF });
    try p.appendSlice(&.{ 0, 0 });

    const rd = try parseRowDescription(a, p.items);
    try std.testing.expectEqual(@as(usize, 2), rd.cols.len);
    try std.testing.expectEqualStrings("id", rd.cols[0].name);
    try std.testing.expectEqual(types.TypeKind.int, rd.cols[0].engine_type.kind);
    try std.testing.expectEqualStrings("name", rd.fields[1].name);
    try std.testing.expectEqual(types.TypeKind.string, rd.fields[1].ty.kind);
}

fn appendCStr(list: *std.array_list.Managed(u8), s: []const u8) !void {
    try list.appendSlice(s);
    try list.append(0);
}

fn writeI32(w: anytype, v: u32) !void {
    var b: [4]u8 = undefined;
    std.mem.writeInt(u32, &b, v, .big);
    try w.writeAll(&b);
}

fn readI16(p: []const u8, i: usize) !i16 {
    if (i + 2 > p.len) return error.PgProtocol;
    return std.mem.readInt(i16, p[i..][0..2], .big);
}
fn readI32(p: []const u8, i: usize) !i32 {
    if (i + 4 > p.len) return error.PgProtocol;
    return std.mem.readInt(i32, p[i..][0..4], .big);
}
fn readCStr(p: []const u8, i: *usize) []const u8 {
    const start = i.*;
    while (i.* < p.len and p[i.*] != 0) : (i.* += 1) {}
    const s = p[start..i.*];
    i.* += 1; // skip null
    return s;
}

// ---------------------------------------------------------------------------
// COPY-based bulk sink (append/overwrite). 10-100× faster than row INSERTs:
// data streams to the server in CopyData chunks instead of per-batch statements.
// Upsert isn't expressible in COPY — callers route upsert to the generic INSERT
// sink (a COPY-to-staging + ON CONFLICT path is the follow-up).
// ---------------------------------------------------------------------------

/// CommandComplete tag for COPY: "COPY <n>". Null on any other tag shape.
fn parseCopyCount(p: []const u8) ?u64 {
    const tag = std.mem.sliceTo(p, 0);
    if (!std.mem.startsWith(u8, tag, "COPY ")) return null;
    return std.fmt.parseInt(u64, tag["COPY ".len..], 10) catch null;
}

test "parseCopyCount: COPY tag, other tags, junk" {
    try std.testing.expectEqual(@as(?u64, 42), parseCopyCount("COPY 42\x00"));
    try std.testing.expectEqual(@as(?u64, 0), parseCopyCount("COPY 0"));
    try std.testing.expectEqual(@as(?u64, null), parseCopyCount("INSERT 0 5\x00"));
    try std.testing.expectEqual(@as(?u64, null), parseCopyCount("COPY x"));
}

pub const CopySink = struct {
    gpa: std.mem.Allocator,
    conn: *Conn,
    table: []const u8, // quoted, qualified
    ncols: usize,
    buffer: std.array_list.Managed(u8), // current segment's encoded rows (replay unit)
    copy_cmd: []const u8, // gpa-owned; issued once per segment
    seg_rows: u64 = 0,
    redial: ?sqlmod.Redial = null,

    pub fn open(gpa: std.mem.Allocator, conn: *Conn, table_name: []const u8, schema: types.Schema, mode: ast.WriteMode, redial: ?sqlmod.Redial) !*CopySink {
        // On error we free only what we allocate here; the caller keeps `conn`
        // (so it can read conn.last_error) and closes it on failure.
        const self = try gpa.create(CopySink);
        errdefer gpa.destroy(self);
        const qtable = try sqlmod.quoteIdent(gpa, .postgres, table_name);
        errdefer gpa.free(qtable);
        self.* = .{ .gpa = gpa, .conn = conn, .table = qtable, .ncols = schema.fields.len, .buffer = std.array_list.Managed(u8).init(gpa), .copy_cmd = "", .redial = redial };
        errdefer self.buffer.deinit();

        var aa = std.heap.ArenaAllocator.init(gpa);
        defer aa.deinit();
        const a = aa.allocator();
        try conn.exec(try sqlmod.createTableSql(a, .postgres, qtable, schema, mode));
        if (mode == .overwrite) try conn.exec(try std.fmt.allocPrint(a, "DELETE FROM {s}", .{qtable}));

        // COPY <table> ("c1","c2",…) FROM STDIN — issued once per segment.
        var cols = std.array_list.Managed(u8).init(a);
        for (schema.fields, 0..) |f, i| {
            if (i > 0) try cols.append(',');
            try cols.appendSlice(try sqlmod.quoteIdent(a, .postgres, f.name));
        }
        self.copy_cmd = try std.fmt.allocPrint(gpa, "COPY {s} ({s}) FROM STDIN", .{ qtable, cols.items });
        return self;
    }

    pub fn sink(self: *CopySink) driver.Sink {
        return .{ .ptr = self, .vtable = &copy_vtable };
    }

    fn writeBatch(self: *CopySink, arena: std.mem.Allocator, batch: Batch) !void {
        try sqlmod.appendBulkText(self.buffer.writer(), arena, batch, .{}); // PG: bool t/f
        self.seg_rows += batch.len;
        if (self.buffer.items.len >= sqlmod.SEGMENT_BYTES) try self.commitSegment();
    }

    /// Transmit the buffered segment as one COPY statement and verify the
    /// server's "COPY n" count. A transient network failure redials once and
    /// resends the intact segment: a COPY that dies mid-statement rolls back
    /// server-side, so the replay cannot duplicate. (The one unavoidable window:
    /// if the connection dies AFTER the server committed but before its reply
    /// arrived, the replay double-writes — same tradeoff as the INSERT sink.)
    fn commitSegment(self: *CopySink) !void {
        if (self.seg_rows == 0) return;
        self.sendSegment() catch |e| {
            const rd = self.redial orelse return e;
            if (!driver.transientNet(e)) return e;
            const fresh = try rd.dial(rd.ctx, self.gpa);
            self.conn.close();
            // The redial ctx is built for this driver kind, so the vtable ptr is
            // always a *postgres.Conn.
            self.conn = @ptrCast(@alignCast(fresh.ptr));
            try self.sendSegment();
        };
        self.buffer.clearRetainingCapacity();
        self.seg_rows = 0;
    }

    fn sendSegment(self: *CopySink) !void {
        try self.conn.copyIn(self.copy_cmd);
        try self.conn.copyData(self.buffer.items);
        const n = try self.conn.copyDone();
        if (n != self.seg_rows) {
            if (self.conn.last_error.len == 0)
                self.conn.last_error = try std.fmt.allocPrint(self.gpa, "COPY count mismatch: sent {d} rows, server loaded {d}", .{ self.seg_rows, n });
            return error.BulkCountMismatch;
        }
    }

    fn closeImpl(self: *CopySink) !void {
        // Release everything even if the final segment fails — otherwise a
        // failed COPY on close leaks the connection, buffer and sink.
        defer self.teardown();
        try self.commitSegment();
    }

    /// Failure path: drop the buffer and close the socket mid-COPY. The server
    /// aborts the COPY when the connection dies, so none of it is committed.
    fn abortImpl(self: *CopySink) void {
        self.teardown();
    }

    fn teardown(self: *CopySink) void {
        self.conn.close();
        self.buffer.deinit();
        if (self.copy_cmd.len > 0) self.gpa.free(self.copy_cmd);
        self.gpa.free(self.table);
        self.gpa.destroy(self);
    }
};

const copy_vtable = driver.Sink.VTable{ .writeBatch = copyWrite, .close = copyClose, .abort = copyAbort };

fn copyWrite(ptr: *anyopaque, arena: std.mem.Allocator, b: Batch) anyerror!void {
    const self: *CopySink = @ptrCast(@alignCast(ptr));
    return self.writeBatch(arena, b);
}
fn copyClose(ptr: *anyopaque) anyerror!void {
    const self: *CopySink = @ptrCast(@alignCast(ptr));
    return self.closeImpl();
}
fn copyAbort(ptr: *anyopaque) void {
    const self: *CopySink = @ptrCast(@alignCast(ptr));
    self.abortImpl();
}

fn errMessage(p: []const u8) []const u8 {
    // ErrorResponse: series of (field-type byte, value cstr), 'M' is the message
    var i: usize = 0;
    while (i < p.len and p[i] != 0) {
        const ft = p[i];
        i += 1;
        const start = i;
        while (i < p.len and p[i] != 0) : (i += 1) {}
        const val = p[start..i];
        i += 1;
        if (ft == 'M') return val;
    }
    return "postgres error";
}
