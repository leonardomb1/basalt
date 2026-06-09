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

pub const Error = error{ PgProtocol, PgAuthFailed, PgQueryFailed, PgAuthUnsupported } || std.mem.Allocator.Error;

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
    // streaming cursor state (valid between queryCursor and close)
    meta_arena: std.heap.ArenaAllocator = undefined,
    cols: []PgCol = &.{},
    cur_schema: *types.Schema = undefined,
    done: bool = false,

    pub fn connect(gpa: std.mem.Allocator, host: []const u8, port: u16, user: []const u8, password: []const u8, database: []const u8) !*Conn {
        const stream = try std.net.tcpConnectToHost(gpa, host, port);
        const self = try gpa.create(Conn);
        self.* = .{ .gpa = gpa, .stream = stream, .payload = std.array_list.Managed(u8).init(gpa) };
        self.sr = std.net.Stream.Reader.init(stream, &self.read_buf);
        self.sw = std.net.Stream.Writer.init(stream, &self.write_buf);
        errdefer self.close();
        try self.startup(user, database);
        try self.authenticate(user, password);
        return self;
    }

    pub fn close(self: *Conn) void {
        if (self.last_error.len > 0) self.gpa.free(self.last_error);
        self.payload.deinit();
        self.stream.close();
        self.gpa.destroy(self);
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
        const w = &self.sw.interface;
        try w.writeAll(&hdr);
        try w.writeAll(body.items);
        try w.flush();
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

        const Hmac = std.crypto.auth.hmac.sha2.HmacSha256;
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

        var salted: [32]u8 = undefined;
        try std.crypto.pwhash.pbkdf2(&salted, password, salt, iters, Hmac);
        var client_key: [32]u8 = undefined;
        Hmac.create(&client_key, "Client Key", &salted);
        var stored_key: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(&client_key, &stored_key, .{});

        const cfwp = try std.fmt.allocPrint(a, "c=biws,r={s}", .{sr}); // client-final-without-proof
        const auth_message = try std.fmt.allocPrint(a, "{s},{s},{s}", .{ cfb, server_first, cfwp });

        var client_sig: [32]u8 = undefined;
        Hmac.create(&client_sig, auth_message, &stored_key);
        var proof: [32]u8 = undefined;
        for (&proof, 0..) |*b, k| b.* = client_key[k] ^ client_sig[k];
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
    }

    // --- streaming query ---

    pub fn queryCursor(self: *Conn, sql: []const u8) !sqlmod.Cursor {
        self.meta_arena = std.heap.ArenaAllocator.init(self.gpa);
        self.openCursor(sql) catch |e| {
            self.meta_arena.deinit();
            return e; // leave conn open: the caller owns it (can read last_error) and closes it
        };
        return .{ .ptr = self, .vtable = &cursor_vtable };
    }

    /// Send the query and read up to RowDescription (the header); leaves the
    /// connection positioned just before the DataRows.
    fn openCursor(self: *Conn, sql: []const u8) !void {
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
                    const rd = try parseRowDescription(ma, p);
                    self.cols = rd.cols;
                    const sch = try ma.create(types.Schema);
                    sch.* = .{ .fields = rd.fields };
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

    fn fetchBatch(self: *Conn, arena: std.mem.Allocator) !?Batch {
        const ncol = self.cols.len;
        if (ncol == 0) return null;
        const builders = try arena.alloc(column.Builder, ncol);
        for (self.cols, builders) |c, *b| b.* = column.Builder.init(arena, c.engine_type);

        var n: usize = 0;
        while (n < sqlmod.STREAM_ROWS and !self.done) {
            const t = try self.readMsg();
            const p = self.payload.items;
            switch (t) {
                'D' => {
                    try parseDataRow(self, arena, p, builders);
                    n += 1;
                },
                'C' => {},
                'Z' => self.done = true,
                'E' => {
                    self.last_error = try self.gpa.dupe(u8, errMessage(p));
                    return error.PgQueryFailed;
                },
                else => {},
            }
        }
        if (n == 0) return null;
        const out = try arena.alloc(column.Column, ncol);
        for (builders, 0..) |*b, k| out[k] = try b.finish();
        return .{ .schema = self.cur_schema, .columns = out, .len = n };
    }

    fn cursorClose(self: *Conn) void {
        self.meta_arena.deinit();
        self.close();
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
    pub fn copyDone(self: *Conn) !void {
        try self.writeMsg('c', "");
        while (true) {
            switch (try self.readMsg()) {
                'E' => {
                    self.last_error = try self.gpa.dupe(u8, errMessage(self.payload.items));
                    return error.PgQueryFailed;
                },
                'Z' => return,
                else => {}, // CommandComplete, etc.
            }
        }
    }

    // --- message framing ---

    fn writeMsg(self: *Conn, msg_type: u8, body: []const u8) !void {
        var hdr: [5]u8 = undefined;
        hdr[0] = msg_type;
        std.mem.writeInt(u32, hdr[1..5], @intCast(body.len + 4), .big);
        const w = &self.sw.interface;
        try w.writeAll(&hdr);
        try w.writeAll(body);
        try w.flush();
    }

    fn readMsg(self: *Conn) !u8 {
        const r = self.sr.interface();
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
    return self.fetchBatch(arena);
}
fn curClose(ptr: *anyopaque) void {
    const self: *Conn = @ptrCast(@alignCast(ptr));
    self.cursorClose();
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

const COPY_FLUSH_BYTES = 1 << 20; // ~1MB per CopyData chunk

pub const CopySink = struct {
    gpa: std.mem.Allocator,
    conn: *Conn,
    table: []const u8, // quoted, qualified
    ncols: usize,
    buffer: std.array_list.Managed(u8),

    pub fn open(gpa: std.mem.Allocator, conn: *Conn, table_name: []const u8, schema: types.Schema, mode: ast.WriteMode) !*CopySink {
        // On error we free only what we allocate here; the caller keeps `conn`
        // (so it can read conn.last_error) and closes it on failure.
        const self = try gpa.create(CopySink);
        errdefer gpa.destroy(self);
        const qtable = try sqlmod.quoteIdent(gpa, .postgres, table_name);
        errdefer gpa.free(qtable);
        self.* = .{ .gpa = gpa, .conn = conn, .table = qtable, .ncols = schema.fields.len, .buffer = std.array_list.Managed(u8).init(gpa) };
        errdefer self.buffer.deinit();

        var aa = std.heap.ArenaAllocator.init(gpa);
        defer aa.deinit();
        const a = aa.allocator();
        try conn.exec(try sqlmod.createTableSql(a, .postgres, qtable, schema, mode));
        if (mode == .overwrite) try conn.exec(try std.fmt.allocPrint(a, "DELETE FROM {s}", .{qtable}));

        // COPY <table> ("c1","c2",…) FROM STDIN  (default text format)
        var cols = std.array_list.Managed(u8).init(a);
        for (schema.fields, 0..) |f, i| {
            if (i > 0) try cols.append(',');
            try cols.appendSlice(try sqlmod.quoteIdent(a, .postgres, f.name));
        }
        try conn.copyIn(try std.fmt.allocPrint(a, "COPY {s} ({s}) FROM STDIN", .{ qtable, cols.items }));
        return self;
    }

    pub fn sink(self: *CopySink) driver.Sink {
        return .{ .ptr = self, .vtable = &copy_vtable };
    }

    fn writeBatch(self: *CopySink, arena: std.mem.Allocator, batch: Batch) !void {
        try sqlmod.appendBulkText(self.buffer.writer(), arena, batch, .{}); // PG: bool t/f
        if (self.buffer.items.len >= COPY_FLUSH_BYTES) try self.flush();
    }

    fn flush(self: *CopySink) !void {
        if (self.buffer.items.len == 0) return;
        try self.conn.copyData(self.buffer.items);
        self.buffer.clearRetainingCapacity();
    }

    fn closeImpl(self: *CopySink) !void {
        // Release everything even if the final flush/CopyDone fails — otherwise
        // a failed COPY on close leaks the connection, buffer and sink.
        defer self.teardown();
        try self.flush();
        try self.conn.copyDone();
    }

    /// Failure path: drop the buffer and close the socket mid-COPY. The server
    /// aborts the COPY when the connection dies, so none of it is committed.
    fn abortImpl(self: *CopySink) void {
        self.teardown();
    }

    fn teardown(self: *CopySink) void {
        self.conn.close();
        self.buffer.deinit();
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
