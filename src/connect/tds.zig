//! Minimal TDS (SQL Server) client — plaintext only (the dev server accepts
//! ENCRYPT_NOT_SUP, so no TLS). Implements packet framing, PRELOGIN, LOGIN7 with
//! SQL auth, and (later) SQLBatch + token-stream parsing into batches.

const std = @import("std");
const types = @import("../lang/types.zig");
const column = @import("../exec/column.zig");
const batchmod = @import("../exec/batch.zig");
const valuemod = @import("../exec/value.zig");
const driver = @import("driver.zig");

const Value = valuemod.Value;
const Batch = batchmod.Batch;

const PKT_PRELOGIN = 0x12;
const PKT_LOGIN7 = 0x10;
const PKT_SQLBATCH = 0x01;
const STATUS_EOM = 0x01;

pub const Error = error{ TdsProtocol, LoginFailed, QueryFailed, EncryptionRequired, UnsupportedTdsType } || std.mem.Allocator.Error || std.net.Stream.WriteError || std.net.Stream.ReadError;

pub const Conn = struct {
    gpa: std.mem.Allocator,
    stream: std.net.Stream,
    msg: std.ArrayList(u8), // reassembled response message payload
    last_error: []const u8 = "",

    pub fn connect(gpa: std.mem.Allocator, host: []const u8, port: u16, user: []const u8, password: []const u8, database: []const u8) !Conn {
        const stream = try std.net.tcpConnectToHost(gpa, host, port);
        var self = Conn{ .gpa = gpa, .stream = stream, .msg = std.ArrayList(u8).init(gpa) };
        errdefer self.close();
        try self.prelogin();
        try self.login(user, password, database, host);
        return self;
    }

    pub fn close(self: *Conn) void {
        self.msg.deinit();
        self.stream.close();
    }

    // --- packet framing ---

    fn writePacket(self: *Conn, ptype: u8, payload: []const u8) !void {
        var header: [8]u8 = .{ ptype, STATUS_EOM, 0, 0, 0, 0, 0, 0 };
        const total: u16 = @intCast(payload.len + 8);
        header[2] = @intCast(total >> 8);
        header[3] = @intCast(total & 0xff);
        try self.stream.writer().writeAll(&header);
        try self.stream.writer().writeAll(payload);
    }

    /// Reassemble a full message (across packets) into self.msg.
    fn readMessage(self: *Conn) !void {
        self.msg.clearRetainingCapacity();
        while (true) {
            var header: [8]u8 = undefined;
            try self.stream.reader().readNoEof(&header);
            const len: usize = (@as(usize, header[2]) << 8) | header[3];
            if (len < 8) return error.TdsProtocol;
            const start = self.msg.items.len;
            try self.msg.resize(start + (len - 8));
            try self.stream.reader().readNoEof(self.msg.items[start..]);
            if (header[1] & STATUS_EOM != 0) break;
        }
    }

    // --- prelogin ---

    fn prelogin(self: *Conn) !void {
        const payload = [_]u8{
            0x00, 0x00, 0x0B, 0x00, 0x06, // VERSION  off=11 len=6
            0x01, 0x00, 0x11, 0x00, 0x01, // ENCRYPTION off=17 len=1
            0xFF, // terminator
            0x11, 0x00, 0x00, 0x00, 0x00, 0x00, // version
            0x02, // ENCRYPT_NOT_SUP
        };
        try self.writePacket(PKT_PRELOGIN, &payload);
        try self.readMessage();

        // find ENCRYPTION (token 0x01) in the response option table
        const p = self.msg.items;
        var i: usize = 0;
        while (i + 5 <= p.len and p[i] != 0xFF) : (i += 5) {
            if (p[i] == 0x01) {
                const off: usize = (@as(usize, p[i + 1]) << 8) | p[i + 2];
                if (off < p.len and (p[off] == 0x01 or p[off] == 0x03)) return error.EncryptionRequired;
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
                0xAB, 0xE3 => { // INFO, ENVCHANGE
                    if (i + 2 > p.len) return error.TdsProtocol;
                    i += 2 + rdU16(p, i);
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
        if (body.len < 8) return "tds login error";
        const msglen: usize = rdU16(body, 6);
        const utf16 = body[8..@min(body.len, 8 + msglen * 2)];
        const out = try self.gpa.alloc(u8, utf16.len / 2);
        for (out, 0..) |*c, k| c.* = utf16[k * 2]; // ASCII slice of UTF16LE
        return out;
    }

    // --- query ---

    /// Run `sql`, parse the whole result set into one batch (allocated in arena).
    pub fn runQuery(self: *Conn, arena: std.mem.Allocator, sql: []const u8) !Result {
        try self.sendBatch(sql);
        try self.readMessage();
        return self.parseResult(arena);
    }

    fn sendBatch(self: *Conn, sql: []const u8) !void {
        var payload = std.ArrayList(u8).init(self.gpa);
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

    fn parseResult(self: *Conn, arena: std.mem.Allocator) !Result {
        const p = self.msg.items;
        var i: usize = 0;
        var cols: []ColumnDesc = &.{};
        var builders: []column.Builder = &.{};
        var fields: []types.Schema.Field = &.{};
        var nrows: usize = 0;

        while (i < p.len) {
            const token = p[i];
            i += 1;
            switch (token) {
                0x81 => { // COLMETADATA
                    const r = try parseColMeta(arena, p, &i);
                    cols = r.cols;
                    fields = r.fields;
                    builders = try arena.alloc(column.Builder, cols.len);
                    for (cols, builders) |c, *b| b.* = column.Builder.init(arena, c.engine_type);
                },
                0xD1 => { // ROW
                    try parseRow(arena, p, &i, cols, builders, false);
                    nrows += 1;
                },
                0xD2 => { // NBCROW (null-bitmap compressed)
                    try parseRow(arena, p, &i, cols, builders, true);
                    nrows += 1;
                },
                0xAA => { // ERROR
                    const len = rdU16(p, i);
                    self.last_error = try self.decodeError(p[i + 2 .. i + 2 + len]);
                    return error.QueryFailed;
                },
                0xAB, 0xE3, 0xA9, 0xA4, 0xA5 => i += 2 + rdU16(p, i), // INFO/ENVCHANGE/ORDER/TABNAME/COLINFO
                0x79 => i += 4, // RETURNSTATUS
                0xFD, 0xFE, 0xFF => i += 12, // DONE / DONEPROC / DONEINPROC
                else => break,
            }
        }

        const schema = try arena.create(types.Schema);
        schema.* = .{ .fields = fields };
        const out_cols = try arena.alloc(column.Column, cols.len);
        for (builders, 0..) |*b, k| out_cols[k] = try b.finish();
        return .{ .schema = schema, .batch = .{ .schema = schema, .columns = out_cols, .len = nrows } };
    }
};

pub const Result = struct { schema: *types.Schema, batch: Batch };

pub const SqlConfig = struct {
    host: []const u8 = "127.0.0.1",
    port: u16 = 1433,
    user: []const u8 = "sa",
    password: []const u8 = "",
    database: []const u8 = "master",
};

/// A `driver.Source` over a TDS query. The whole result set is materialized once
/// (into a source-owned arena) and handed out as a single batch. Streaming
/// (token-by-token) is a future improvement.
pub const Source = struct {
    gpa: std.mem.Allocator,
    arena_inst: std.heap.ArenaAllocator,
    result: Result,
    yielded: bool = false,

    pub fn open(gpa: std.mem.Allocator, cfg: SqlConfig, sql: []const u8) !*Source {
        const self = try gpa.create(Source);
        self.* = .{ .gpa = gpa, .arena_inst = std.heap.ArenaAllocator.init(gpa), .result = undefined };
        errdefer {
            self.arena_inst.deinit();
            gpa.destroy(self);
        }
        var conn = try Conn.connect(gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database);
        defer conn.close();
        self.result = conn.runQuery(self.arena_inst.allocator(), sql) catch |e| {
            if (conn.last_error.len > 0) std.debug.print("sqlserver: {s}\n", .{conn.last_error});
            return e;
        };
        return self;
    }

    pub fn source(self: *Source) driver.Source {
        return .{ .ptr = self, .vtable = &source_vtable };
    }
};

const source_vtable = driver.Source.VTable{ .schema = srcSchema, .next = srcNext, .close = srcClose };

fn srcSchema(ptr: *anyopaque) types.Schema {
    const self: *Source = @ptrCast(@alignCast(ptr));
    return self.result.schema.*;
}
fn srcNext(ptr: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
    _ = arena;
    const self: *Source = @ptrCast(@alignCast(ptr));
    if (self.yielded) return null;
    self.yielded = true;
    return self.result.batch;
}
fn srcClose(ptr: *anyopaque) void {
    const self: *Source = @ptrCast(@alignCast(ptr));
    self.arena_inst.deinit();
    self.gpa.destroy(self);
}

const ColKind = enum { fixed, bytelen, ushortlen };
const ColumnDesc = struct {
    name: []const u8 = "",
    tds_type: u8,
    engine_type: types.Type,
    kind: ColKind,
    fixed_len: u8 = 0,
    scale: u8 = 0,
    is_unicode: bool = false,
};

const ColMeta = struct { cols: []ColumnDesc, fields: []types.Schema.Field };

fn parseColMeta(arena: std.mem.Allocator, p: []const u8, i: *usize) !ColMeta {
    const count = rdU16(p, i.*);
    i.* += 2;
    if (count == 0xFFFF) return .{ .cols = &.{}, .fields = &.{} };
    const cols = try arena.alloc(ColumnDesc, count);
    const fields = try arena.alloc(types.Schema.Field, count);
    for (0..count) |k| {
        i.* += 4; // UserType
        i.* += 2; // Flags
        var d = try parseTypeInfo(p, i);
        const namelen: usize = p[i.*];
        i.* += 1;
        const name = try utf16ToUtf8(arena, p[i.* .. i.* + namelen * 2]);
        i.* += namelen * 2;
        d.name = name;
        cols[k] = d;
        fields[k] = .{ .name = name, .ty = d.engine_type };
    }
    return .{ .cols = cols, .fields = fields };
}

fn parseTypeInfo(p: []const u8, i: *usize) !ColumnDesc {
    const intT = types.Type.init(.int).asNullable();
    const boolT = types.Type.init(.bool).asNullable();
    const floatT = types.Type.init(.float).asNullable();
    const strT = types.Type.init(.string).asNullable();
    const bytesT = types.Type.init(.bytes).asNullable();
    const dateT = types.Type.init(.date).asNullable();
    const tsT = types.Type.init(.timestamp).asNullable();

    const t = p[i.*];
    i.* += 1;
    var d = ColumnDesc{ .tds_type = t, .engine_type = strT, .kind = .bytelen };
    switch (t) {
        0x38 => {
            d.kind = .fixed;
            d.fixed_len = 4;
            d.engine_type = intT;
        }, // INT4
        0x34 => {
            d.kind = .fixed;
            d.fixed_len = 2;
            d.engine_type = intT;
        }, // INT2
        0x30 => {
            d.kind = .fixed;
            d.fixed_len = 1;
            d.engine_type = intT;
        }, // INT1
        0x7F => {
            d.kind = .fixed;
            d.fixed_len = 8;
            d.engine_type = intT;
        }, // INT8
        0x32 => {
            d.kind = .fixed;
            d.fixed_len = 1;
            d.engine_type = boolT;
        }, // BIT
        0x3B => {
            d.kind = .fixed;
            d.fixed_len = 4;
            d.engine_type = floatT;
        }, // REAL
        0x3E => {
            d.kind = .fixed;
            d.fixed_len = 8;
            d.engine_type = floatT;
        }, // FLOAT
        0x3D => {
            d.kind = .fixed;
            d.fixed_len = 8;
            d.engine_type = tsT;
        }, // DATETIME
        0x3A => {
            d.kind = .fixed;
            d.fixed_len = 4;
            d.engine_type = tsT;
        }, // SMALLDATETIME
        0x26 => {
            i.* += 1;
            d.engine_type = intT;
        }, // INTN
        0x68 => {
            i.* += 1;
            d.engine_type = boolT;
        }, // BITN
        0x6D => {
            i.* += 1;
            d.engine_type = floatT;
        }, // FLTN
        0x6E => {
            i.* += 1;
            d.engine_type = floatT;
        }, // MONEYN -> float (approx)
        0x6F => {
            i.* += 1;
            d.engine_type = tsT;
        }, // DATETIMN
        0x6A, 0x6C => { // DECIMALN / NUMERICN
            i.* += 1; // max len
            const prec = p[i.*];
            i.* += 1;
            const scale = p[i.*];
            i.* += 1;
            d.scale = scale;
            d.engine_type = types.Type.decimal(prec, scale).asNullable();
        },
        0x24 => {
            i.* += 1;
            d.engine_type = strT;
        }, // UNIQUEIDENTIFIER -> string
        0x28 => {
            d.engine_type = dateT;
        }, // DATE
        0x29 => {
            d.scale = p[i.*];
            i.* += 1;
            d.engine_type = strT;
        }, // TIME -> string (approx)
        0x2A => {
            d.scale = p[i.*];
            i.* += 1;
            d.engine_type = tsT;
        }, // DATETIME2
        0x2B => {
            d.scale = p[i.*];
            i.* += 1;
            d.engine_type = tsT;
        }, // DATETIMEOFFSET
        0xA7, 0xAF => { // VARCHAR / CHAR
            const ml = rdU16(p, i.*);
            i.* += 2 + 5; // max len + collation
            if (ml == 0xFFFF) return error.UnsupportedTdsType;
            d.kind = .ushortlen;
            d.engine_type = strT;
        },
        0xE7, 0xEF => { // NVARCHAR / NCHAR
            const ml = rdU16(p, i.*);
            i.* += 2 + 5;
            if (ml == 0xFFFF) return error.UnsupportedTdsType;
            d.kind = .ushortlen;
            d.engine_type = strT;
            d.is_unicode = true;
        },
        0xA5, 0xAD => { // VARBINARY / BINARY
            const ml = rdU16(p, i.*);
            i.* += 2;
            if (ml == 0xFFFF) return error.UnsupportedTdsType;
            d.kind = .ushortlen;
            d.engine_type = bytesT;
        },
        else => return error.UnsupportedTdsType,
    }
    return d;
}

fn parseRow(arena: std.mem.Allocator, p: []const u8, i: *usize, cols: []const ColumnDesc, builders: []column.Builder, nbc: bool) !void {
    var nullbits: []const u8 = &.{};
    if (nbc) {
        const nbytes = (cols.len + 7) / 8;
        nullbits = p[i.* .. i.* + nbytes];
        i.* += nbytes;
    }
    for (cols, 0..) |col, ci| {
        if (nbc and (nullbits[ci / 8] >> @intCast(ci % 8)) & 1 != 0) {
            try builders[ci].append(.null);
            continue;
        }
        const v = try readColumnValue(arena, p, i, col);
        try builders[ci].append(v);
    }
}

fn readColumnValue(arena: std.mem.Allocator, p: []const u8, i: *usize, col: ColumnDesc) !Value {
    switch (col.kind) {
        .fixed => {
            const bytes = p[i.* .. i.* + col.fixed_len];
            i.* += col.fixed_len;
            return decodeValue(arena, col, bytes);
        },
        .bytelen => {
            const len: usize = p[i.*];
            i.* += 1;
            if (len == 0) return .null;
            const bytes = p[i.* .. i.* + len];
            i.* += len;
            return decodeValue(arena, col, bytes);
        },
        .ushortlen => {
            const len = rdU16(p, i.*);
            i.* += 2;
            if (len == 0xFFFF) return .null;
            const bytes = p[i.* .. i.* + len];
            i.* += len;
            return decodeValue(arena, col, bytes);
        },
    }
}

fn decodeValue(arena: std.mem.Allocator, d: ColumnDesc, bytes: []const u8) !Value {
    return switch (d.engine_type.kind) {
        .int => .{ .int = readIntLE(bytes) },
        .bool => .{ .bool = bytes.len > 0 and bytes[0] != 0 },
        .float => .{ .float = if (bytes.len == 4) @as(f64, @as(f32, @bitCast(@as(u32, @truncate(readULE(bytes))))) ) else @bitCast(readULE(bytes)) },
        .decimal => decodeDecimal(d, bytes),
        .string => .{ .string = if (d.is_unicode) try utf16ToUtf8(arena, bytes) else try arena.dupe(u8, bytes) },
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
            var b8: [8]u8 = .{0} ** 8;
            const n = @min(bytes.len, 8);
            @memcpy(b8[0..n], bytes[0..n]);
            if (bytes[n - 1] & 0x80 != 0) {
                for (b8[n..]) |*x| x.* = 0xFF;
            }
            break :blk std.mem.readInt(i64, &b8, .little);
        },
    };
}

fn utf16ToUtf8(arena: std.mem.Allocator, bytes: []const u8) ![]const u8 {
    var out = std.ArrayList(u8).init(arena);
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

// --- LOGIN7 construction ---

fn buildLogin7(gpa: std.mem.Allocator, user: []const u8, password: []const u8, database: []const u8, host: []const u8) ![]u8 {
    var fixed = [_]u8{0} ** 94;
    // TDSVersion 7.4 = 0x74000004 (LE), PacketSize 4096
    fixed[4] = 0x04;
    fixed[7] = 0x74;
    fixed[8] = 0x00;
    fixed[9] = 0x10; // 0x1000 = 4096

    var vd = std.ArrayList(u8).init(gpa);
    defer vd.deinit();

    try addField(&fixed, 36, host, &vd, false); // HostName
    try addField(&fixed, 40, user, &vd, false); // UserName
    try addField(&fixed, 44, password, &vd, true); // Password (obfuscated)
    try addField(&fixed, 48, "pipeline", &vd, false); // AppName
    try addField(&fixed, 52, host, &vd, false); // ServerName
    try addField(&fixed, 56, "", &vd, false); // Extension (unused)
    try addField(&fixed, 60, "pipeline", &vd, false); // CltIntName
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

/// Append a UTF-16LE string to var-data and write its (offset, char-count) into
/// the fixed offset/length block. ASCII only (sufficient for creds/identifiers).
fn addField(fixed: []u8, ib_pos: usize, s: []const u8, vd: *std.ArrayList(u8), obfuscate: bool) !void {
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
