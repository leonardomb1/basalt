//! Minimal MySQL-protocol client — just enough to run DDL against a StarRocks FE
//! (handshake + mysql_native_password auth + COM_QUERY, parsing only OK/ERR; no
//! result sets). Used by the StarRocks sink for CREATE TABLE / TRUNCATE.
//!
//! Network-tested only against a live server; the auth token math is unit-tested
//! in starrocks.zig.

const std = @import("std");
const sr = @import("starrocks.zig");
const sqlmod = @import("sql.zig");
const types = @import("../lang/types.zig");
const ast = @import("../lang/ast.zig");
const column = @import("../exec/column.zig");
const valuemod = @import("../exec/value.zig");
const batchmod = @import("../exec/batch.zig");
const driver = @import("driver.zig");

const Value = valuemod.Value;
const Batch = batchmod.Batch;

const CLIENT_LONG_PASSWORD = 0x00000001;
const CLIENT_CONNECT_WITH_DB = 0x00000008;
const CLIENT_LOCAL_FILES = 0x00000080; // enables LOAD DATA LOCAL INFILE
const CLIENT_PROTOCOL_41 = 0x00000200;
const CLIENT_SECURE_CONNECTION = 0x00008000;
const CLIENT_PLUGIN_AUTH = 0x00080000;

pub const Error = error{ MysqlAuthFailed, MysqlQueryFailed, MysqlProtocol } || std.mem.Allocator.Error;

/// Buffered socket reads: each result row is its own MySQL packet, so without
/// buffering every row costs two recv syscalls (4-byte header + body). A 64 KB
/// buffer collapses that to one syscall per ~64 KB — the dominant read-time win.
const SOCK_BUF = 64 * 1024;

pub const Conn = struct {
    gpa: std.mem.Allocator,
    stream: std.net.Stream,
    read_buf: [SOCK_BUF]u8 = undefined,
    write_buf: [SOCK_BUF]u8 = undefined,
    sr: std.net.Stream.Reader = undefined,
    sw: std.net.Stream.Writer = undefined,
    buf: std.array_list.Managed(u8),
    last_error: []const u8 = "",
    // streaming cursor state (valid between queryCursor and close)
    meta_arena: std.heap.ArenaAllocator = undefined,
    cols: []MyCol = &.{},
    cur_schema: *types.Schema = undefined,
    done: bool = false,
    ld_seq: u8 = 0, // packet sequence during a LOAD DATA exchange

    pub fn connect(gpa: std.mem.Allocator, host: []const u8, port: u16, user: []const u8, password: []const u8, database: []const u8) !*Conn {
        const stream = try std.net.tcpConnectToHost(gpa, host, port);
        const self = try gpa.create(Conn);
        self.* = .{ .gpa = gpa, .stream = stream, .buf = std.array_list.Managed(u8).init(gpa) };
        self.sr = std.net.Stream.Reader.init(stream, &self.read_buf);
        self.sw = std.net.Stream.Writer.init(stream, &self.write_buf);
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

    // --- LOAD DATA LOCAL INFILE (bulk load) ---

    /// Send the `LOAD DATA LOCAL INFILE …` query and wait for the server's local
    /// infile request (a packet whose first byte is 0xFB). Data then streams via
    /// `loadDataChunk`; `loadDataEnd` finishes it.
    pub fn loadDataStart(self: *Conn, cmd: []const u8) !void {
        const payload = try self.gpa.alloc(u8, cmd.len + 1);
        defer self.gpa.free(payload);
        payload[0] = 0x03; // COM_QUERY
        @memcpy(payload[1..], cmd);
        try self.writePacket(0, payload);

        const rseq = try self.readPacket();
        const p = self.buf.items;
        if (p.len > 0 and p[0] == 0xff) {
            self.last_error = try self.gpa.dupe(u8, errMessage(p));
            return error.MysqlQueryFailed;
        }
        if (p.len == 0 or p[0] != 0xfb) return error.MysqlProtocol; // expected local-infile request
        self.ld_seq = rseq +% 1;
    }

    /// One data packet (must be < 16MB; the sink flushes well under that).
    pub fn loadDataChunk(self: *Conn, data: []const u8) !void {
        if (data.len == 0) return;
        try self.writePacket(self.ld_seq, data);
        self.ld_seq +%= 1;
    }

    /// Empty packet = end of data; then read the server's OK/ERR.
    pub fn loadDataEnd(self: *Conn) !void {
        try self.writePacket(self.ld_seq, "");
        _ = try self.readPacket();
        const p = self.buf.items;
        if (p.len > 0 and p[0] == 0xff) {
            self.last_error = try self.gpa.dupe(u8, errMessage(p));
            return error.MysqlQueryFailed;
        }
    }

    pub fn close(self: *Conn) void {
        if (self.last_error.len > 0) self.gpa.free(self.last_error);
        self.buf.deinit();
        self.stream.close();
        self.gpa.destroy(self);
    }

    pub fn sqlConn(self: *Conn) sqlmod.Conn {
        return .{ .ptr = self, .vtable = &sql_vtable };
    }

    /// Start streaming a query: send it, parse the column-def header.
    pub fn queryCursor(self: *Conn, sql: []const u8) !sqlmod.Cursor {
        self.meta_arena = std.heap.ArenaAllocator.init(self.gpa);
        self.openCursor(sql) catch |e| {
            self.meta_arena.deinit();
            return e; // leave conn open: the caller owns it (can read last_error) and closes it
        };
        return .{ .ptr = self, .vtable = &cursor_vtable };
    }

    fn openCursor(self: *Conn, sql: []const u8) !void {
        const ma = self.meta_arena.allocator();
        const payload = try self.gpa.alloc(u8, sql.len + 1);
        defer self.gpa.free(payload);
        payload[0] = 0x03; // COM_QUERY
        @memcpy(payload[1..], sql);
        try self.writePacket(0, payload);

        _ = try self.readPacket();
        const first = self.buf.items;
        if (first.len == 0) return error.MysqlProtocol;
        if (first[0] == 0xff) {
            self.last_error = try self.gpa.dupe(u8, errMessage(first));
            return error.MysqlQueryFailed;
        }
        if (first[0] == 0x00 or first[0] == 0xfe) {
            self.cols = &.{};
            const empty = try ma.create(types.Schema);
            empty.* = .{ .fields = &.{} };
            self.cur_schema = empty;
            self.done = true;
            return;
        }

        var ci: usize = 0;
        const ncol: usize = @intCast(try lenencInt(first, &ci));
        self.cols = try ma.alloc(MyCol, ncol);
        for (0..ncol) |k| {
            _ = try self.readPacket();
            self.cols[k] = try parseColDef(ma, self.buf.items);
        }
        _ = try self.readPacket(); // EOF after column defs

        const fields = try ma.alloc(types.Schema.Field, ncol);
        for (self.cols, 0..) |c, k| fields[k] = .{ .name = c.name, .ty = c.engine_type };
        const sch = try ma.create(types.Schema);
        sch.* = .{ .fields = fields };
        self.cur_schema = sch;
        self.done = false;
    }

    fn fetchBatch(self: *Conn, arena: std.mem.Allocator) !?Batch {
        if (self.done) return null;
        const ncol = self.cols.len;
        if (ncol == 0) return null;
        const builders = try arena.alloc(column.Builder, ncol);
        for (self.cols, builders) |c, *b| b.* = column.Builder.init(arena, c.engine_type);

        var n: usize = 0;
        while (n < sqlmod.STREAM_ROWS) {
            _ = try self.readPacket();
            const r = self.buf.items;
            if (r.len > 0 and r[0] == 0xfe and r.len < 9) { // EOF = end of rows
                self.done = true;
                break;
            }
            var ri: usize = 0;
            for (self.cols, 0..) |c, k| {
                const text = try lenencStrOrNull(r, &ri);
                try builders[k].append(try sqlmod.coerceText(arena, text, c.engine_type));
            }
            n += 1;
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

    // --- packet framing ---

    fn readPacket(self: *Conn) !u8 {
        const r = self.sr.interface();
        var header: [4]u8 = undefined;
        try r.readSliceAll(&header);
        const len: usize = @as(usize, header[0]) | (@as(usize, header[1]) << 8) | (@as(usize, header[2]) << 16);
        try self.buf.resize(len);
        try r.readSliceAll(self.buf.items[0..len]);
        return header[3];
    }

    fn writePacket(self: *Conn, seq: u8, payload: []const u8) !void {
        var header: [4]u8 = undefined;
        header[0] = @intCast(payload.len & 0xff);
        header[1] = @intCast((payload.len >> 8) & 0xff);
        header[2] = @intCast((payload.len >> 16) & 0xff);
        header[3] = seq;
        const w = &self.sw.interface;
        try w.writeAll(&header);
        try w.writeAll(payload);
        try w.flush();
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
        var caps: u32 = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH | CLIENT_LONG_PASSWORD | CLIENT_LOCAL_FILES;
        if (database.len > 0) caps |= CLIENT_CONNECT_WITH_DB;

        var out = std.array_list.Managed(u8).init(self.gpa);
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
    var salt: [20]u8 = std.mem.zeroes([20]u8);
    const n = @min(@as(usize, 20), p.len -| i);
    if (n > 0) @memcpy(salt[0..n], p[i .. i + n]);
    return salt;
}

// ---------------------------------------------------------------------------
// LOAD DATA LOCAL INFILE bulk sink (append/overwrite). Streams the same tab-
// separated text COPY uses (MySQL LOAD DATA defaults: TERMINATED BY '\t', ESCAPED
// BY '\\', LINES BY '\n', NULL = \N). Requires server `local_infile=ON`. Upsert
// routes to the INSERT sink. (Bool literal is 1/0 for MySQL, not t/f.)
// ---------------------------------------------------------------------------

const LD_FLUSH_BYTES = 1 << 20; // ~1MB per data packet (well under MySQL's 16MB cap)

pub const LoadDataSink = struct {
    gpa: std.mem.Allocator,
    conn: *Conn,
    buffer: std.array_list.Managed(u8),

    pub fn open(gpa: std.mem.Allocator, conn: *Conn, table_name: []const u8, schema: types.Schema, mode: ast.WriteMode) !*LoadDataSink {
        // On error we free only what we allocate here; the caller keeps `conn`
        // (so it can read conn.last_error) and closes it on failure.
        const self = try gpa.create(LoadDataSink);
        errdefer gpa.destroy(self);
        self.* = .{ .gpa = gpa, .conn = conn, .buffer = std.array_list.Managed(u8).init(gpa) };
        errdefer self.buffer.deinit();

        var aa = std.heap.ArenaAllocator.init(gpa);
        defer aa.deinit();
        const a = aa.allocator();
        const qtable = try sqlmod.quoteIdent(a, .mysql, table_name);
        try conn.exec(try sqlmod.createTableSql(a, .mysql, qtable, schema, mode));
        if (mode == .overwrite) try conn.exec(try std.fmt.allocPrint(a, "DELETE FROM {s}", .{qtable}));

        var cols = std.array_list.Managed(u8).init(a);
        for (schema.fields, 0..) |f, i| {
            if (i > 0) try cols.append(',');
            try cols.appendSlice(try sqlmod.quoteIdent(a, .mysql, f.name));
        }
        try conn.loadDataStart(try std.fmt.allocPrint(a, "LOAD DATA LOCAL INFILE 'pipe' INTO TABLE {s} ({s})", .{ qtable, cols.items }));
        return self;
    }

    pub fn sink(self: *LoadDataSink) driver.Sink {
        return .{ .ptr = self, .vtable = &ld_vtable };
    }

    fn writeBatch(self: *LoadDataSink, arena: std.mem.Allocator, batch: Batch) !void {
        try sqlmod.appendBulkText(self.buffer.writer(), arena, batch, .{ .bool_true = "1", .bool_false = "0" });
        if (self.buffer.items.len >= LD_FLUSH_BYTES) try self.flush();
    }

    fn flush(self: *LoadDataSink) !void {
        if (self.buffer.items.len == 0) return;
        try self.conn.loadDataChunk(self.buffer.items);
        self.buffer.clearRetainingCapacity();
    }

    fn closeImpl(self: *LoadDataSink) !void {
        try self.flush();
        try self.conn.loadDataEnd();
        self.conn.close();
        self.buffer.deinit();
        self.gpa.destroy(self);
    }
};

const ld_vtable = driver.Sink.VTable{ .writeBatch = ldWrite, .close = ldClose };

fn ldWrite(ptr: *anyopaque, arena: std.mem.Allocator, b: Batch) anyerror!void {
    const self: *LoadDataSink = @ptrCast(@alignCast(ptr));
    return self.writeBatch(arena, b);
}
fn ldClose(ptr: *anyopaque) anyerror!void {
    const self: *LoadDataSink = @ptrCast(@alignCast(ptr));
    return self.closeImpl();
}

fn errMessage(p: []const u8) []const u8 {
    // 0xff, error_code(2), if 41: '#' + sql_state(5), then message
    if (p.len < 3) return "mysql error";
    var i: usize = 3;
    if (p.len > 3 and p[3] == '#') i = 9; // skip '#XXXXX'
    if (i > p.len) return "mysql error";
    return p[i..];
}

// --- result-set parsing ---

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

const MyCol = struct {
    name: []const u8,
    mtype: u8,
    decimals: u8,
    engine_type: types.Type,
};

fn parseColDef(arena: std.mem.Allocator, p: []const u8) !MyCol {
    var i: usize = 0;
    _ = try lenencStr(p, &i); // catalog
    _ = try lenencStr(p, &i); // schema
    _ = try lenencStr(p, &i); // table
    _ = try lenencStr(p, &i); // org_table
    const name = try lenencStr(p, &i);
    _ = try lenencStr(p, &i); // org_name
    _ = try lenencInt(p, &i); // length of fixed fields (0x0c)
    if (i + 10 > p.len) return error.MysqlProtocol; // charset(2)+collen(4)+type(1)+flags(2)+decimals(1)
    i += 2; // charset
    i += 4; // column length
    const mtype = p[i];
    i += 1;
    i += 2; // flags
    const decimals = p[i];
    return .{ .name = try arena.dupe(u8, name), .mtype = mtype, .decimals = decimals, .engine_type = engineTypeFor(mtype, decimals) };
}

fn engineTypeFor(mtype: u8, decimals: u8) types.Type {
    return (switch (mtype) {
        0x01, 0x02, 0x03, 0x08, 0x09, 0x0d => types.Type.init(.int), // TINY/SHORT/LONG/LONGLONG/INT24/YEAR
        0x04, 0x05 => types.Type.init(.float), // FLOAT/DOUBLE
        0x00, 0xf6 => types.Type.decimal(38, decimals), // DECIMAL/NEWDECIMAL
        0x0a => types.Type.init(.date), // DATE
        0x07, 0x0c => types.Type.init(.timestamp), // TIMESTAMP/DATETIME
        0x10 => types.Type.init(.int), // BIT
        else => types.Type.init(.string),
    }).asNullable();
}

fn lenencInt(buf: []const u8, i: *usize) !u64 {
    if (i.* >= buf.len) return error.MysqlProtocol;
    const b = buf[i.*];
    i.* += 1;
    if (b < 0xfb) return b;
    const nbytes: usize = if (b == 0xfc) 2 else if (b == 0xfd) 3 else 8;
    if (i.* + nbytes > buf.len) return error.MysqlProtocol;
    var v: u64 = 0;
    for (0..nbytes) |k| v |= @as(u64, buf[i.* + k]) << @intCast(k * 8);
    i.* += nbytes;
    return v;
}

fn lenencStr(buf: []const u8, i: *usize) ![]const u8 {
    const n: usize = @intCast(try lenencInt(buf, i));
    if (i.* + n > buf.len) return error.MysqlProtocol;
    const s = buf[i.* .. i.* + n];
    i.* += n;
    return s;
}

fn lenencStrOrNull(buf: []const u8, i: *usize) !?[]const u8 {
    if (i.* >= buf.len) return error.MysqlProtocol;
    if (buf[i.*] == 0xfb) {
        i.* += 1;
        return null;
    }
    return try lenencStr(buf, i);
}
