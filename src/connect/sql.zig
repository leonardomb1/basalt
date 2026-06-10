//! Shared SQL-database plumbing. A `Conn` is any protocol client (mysql/postgres/
//! tds) that can run a query and exec a statement. `Dialect` captures the only
//! per-database differences (type mapping, identifier quoting, upsert syntax).
//! `Source` and `Sink` are written once on top of these — each new database is
//! just a protocol client + a dialect.

const std = @import("std");
const TlsClient = @import("tls_client.zig");
const types = @import("../lang/types.zig");
const batchmod = @import("../exec/batch.zig");
const valuemod = @import("../exec/value.zig");
const column = @import("../exec/column.zig");
const eval = @import("../exec/eval.zig");
const driver = @import("driver.zig");
const ast = @import("../lang/ast.zig");

const Value = valuemod.Value;
const Batch = batchmod.Batch;

/// Rows are this many per streamed batch.
pub const STREAM_ROWS = 4096;

/// Socket-level failures worth retrying (the connection broke; the server
/// did not report a SQL error). Server-sent errors (PgQueryFailed etc.) are
/// permanent and never retried.
pub fn transientNet(e: anyerror) bool {
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
        => true,
        else => false,
    };
}

// ---------------------------------------------------------------------------
// TLS (shared by the protocol clients)
// ---------------------------------------------------------------------------

/// `require` = full verification (system CA bundle + hostname); `insecure` =
/// encrypt but skip verification (self-signed dev/test servers).
pub const TlsMode = enum { off, require, insecure };

/// A TLS session layered over an established plain socket. Must live at a
/// stable heap address before `start` (the client holds internal pointers),
/// and per std.crypto.tls the socket reader's buffer must hold at least one
/// ciphertext record (the drivers' 64 KB socket buffers satisfy this).
///
/// Uses the vendored `tls_client.zig` (std's client + TLS 1.3 client-cert
/// request handling) because MySQL servers always request a client certificate.
pub const TlsState = struct {
    client: TlsClient,
    read_buf: [TlsClient.min_buffer_len]u8,
    write_buf: [TlsClient.min_buffer_len]u8,
    bundle: std.crypto.Certificate.Bundle,

    pub fn start(self: *TlsState, gpa: std.mem.Allocator, input: *std.Io.Reader, output: *std.Io.Writer, host: []const u8, mode: TlsMode) !void {
        self.bundle = .{};
        if (mode == .require) try self.bundle.rescan(gpa);
        errdefer self.bundle.deinit(gpa);
        self.client = try TlsClient.init(input, output, .{
            .host = if (mode == .require) .{ .explicit = host } else .no_verification,
            .ca = if (mode == .require) .{ .bundle = self.bundle } else .no_verification,
            .read_buffer = &self.read_buf,
            .write_buffer = &self.write_buf,
        });
    }

    pub fn deinit(self: *TlsState, gpa: std.mem.Allocator) void {
        self.bundle.deinit(gpa);
        gpa.destroy(self);
    }
};

/// A streaming cursor over a result set: read the schema up front, then pull
/// batches of rows on demand (bounded memory). `close` releases the connection.
pub const Cursor = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        schema: *const fn (*anyopaque) types.Schema,
        nextBatch: *const fn (*anyopaque, std.mem.Allocator) anyerror!?Batch,
        close: *const fn (*anyopaque) void,
    };

    pub fn schema(self: Cursor) types.Schema {
        return self.vtable.schema(self.ptr);
    }
    pub fn nextBatch(self: Cursor, arena: std.mem.Allocator) anyerror!?Batch {
        return self.vtable.nextBatch(self.ptr, arena);
    }
    pub fn close(self: Cursor) void {
        self.vtable.close(self.ptr);
    }
};

// ---------------------------------------------------------------------------
// Shared text-protocol cursor skeleton (postgres/mysql)
//
// Both drivers keep identical cursor state on their Conn (`meta_arena`,
// `cols`, `cur_schema`, `done`) and used to duplicate the open-guard and the
// fetch loop — which drifted once (a mid-stream server error parsed as row
// data). The skeleton owns the lifecycle and the loop; a driver provides only
// `nextRow`, which MUST classify every wire packet as a row, the end of the
// stream, or an error — leaving no place to forget a case. The TDS driver is
// not covered: it is a token-stream cursor with a different shape.
// ---------------------------------------------------------------------------

pub const RowStep = enum { row, end };

/// `conn` requires: `meta_arena`, `openCursor(sql)`, and `last_error`
/// conventions. On open failure the connection is left open so the caller can
/// read `last_error`, and the caller closes it.
pub fn openTextCursor(conn: anytype, sql_text: []const u8, vt: *const Cursor.VTable) !Cursor {
    conn.meta_arena = std.heap.ArenaAllocator.init(conn.gpa);
    conn.openCursor(sql_text) catch |e| {
        conn.meta_arena.deinit();
        return e;
    };
    return .{ .ptr = conn, .vtable = vt };
}

/// `conn` requires: `cols` (each with `.engine_type`), `cur_schema`, `done`,
/// and `nextRow(arena, builders) !RowStep` appending exactly one value per
/// column on `.row`.
pub fn fetchTextBatch(conn: anytype, arena: std.mem.Allocator) !?Batch {
    if (conn.done) return null;
    const ncol = conn.cols.len;
    if (ncol == 0) return null;
    const builders = try arena.alloc(column.Builder, ncol);
    for (conn.cols, builders) |c, *b| b.* = column.Builder.init(arena, c.engine_type);

    var n: usize = 0;
    while (n < STREAM_ROWS) {
        switch (try conn.nextRow(arena, builders)) {
            .row => n += 1,
            .end => {
                conn.done = true;
                break;
            },
        }
    }
    if (n == 0) return null;
    const out = try arena.alloc(column.Column, ncol);
    for (builders, 0..) |*b, k| out[k] = try b.finish();
    return .{ .schema = conn.cur_schema, .columns = out, .len = n };
}

pub fn closeTextCursor(conn: anytype) void {
    conn.meta_arena.deinit();
    conn.close();
}

/// A connection to a SQL database (protocol-agnostic). `queryCursor` sends a
/// query, reads the result-set header, and returns a streaming cursor (which
/// then owns the connection until closed). On error it leaves the connection
/// open: the caller owns it (so it can read `last_error`) and must close it.
pub const Conn = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        queryCursor: *const fn (*anyopaque, []const u8) anyerror!Cursor,
        exec: *const fn (*anyopaque, []const u8) anyerror!void,
        close: *const fn (*anyopaque) void,
    };

    pub fn queryCursor(self: Conn, sql: []const u8) anyerror!Cursor {
        return self.vtable.queryCursor(self.ptr, sql);
    }
    pub fn exec(self: Conn, sql: []const u8) anyerror!void {
        return self.vtable.exec(self.ptr, sql);
    }
    pub fn close(self: Conn) void {
        self.vtable.close(self.ptr);
    }
};

pub const Dialect = enum {
    postgres,
    mysql,
    sqlserver,

    fn qOpen(self: Dialect) u8 {
        return switch (self) {
            .postgres => '"',
            .mysql => '`',
            .sqlserver => '[',
        };
    }
    fn qClose(self: Dialect) u8 {
        return switch (self) {
            .postgres => '"',
            .mysql => '`',
            .sqlserver => ']',
        };
    }

    fn ddlType(self: Dialect, arena: std.mem.Allocator, ty: types.Type, is_key: bool) ![]const u8 {
        return switch (ty.kind) {
            .bool => switch (self) {
                .sqlserver => "BIT",
                .mysql => "TINYINT(1)",
                .postgres => "BOOLEAN",
            },
            .int => "BIGINT",
            .float => switch (self) {
                .postgres => "DOUBLE PRECISION",
                .mysql => "DOUBLE",
                .sqlserver => "FLOAT",
            },
            .decimal => try std.fmt.allocPrint(arena, "DECIMAL({d},{d})", .{ ty.precision, ty.scale }),
            .string => switch (self) {
                .postgres => if (is_key) "VARCHAR(255)" else "TEXT",
                .mysql => "VARCHAR(255)",
                // NVARCHAR(MAX) uses PLP encoding our TDS reader doesn't parse; cap at 4000.
                .sqlserver => if (is_key) "NVARCHAR(255)" else "NVARCHAR(4000)",
            },
            .bytes => switch (self) {
                .postgres => "BYTEA",
                .mysql => "BLOB",
                .sqlserver => "VARBINARY(MAX)",
            },
            .date => "DATE",
            .time => "TIME",
            .timestamp => switch (self) {
                .postgres => "TIMESTAMP",
                .mysql => "DATETIME",
                .sqlserver => "DATETIME2",
            },
            else => "TEXT",
        };
    }

};

// ---------------------------------------------------------------------------
// Generic source
// ---------------------------------------------------------------------------

pub const Source = struct {
    gpa: std.mem.Allocator,
    cursor: Cursor,

    /// Start streaming `sql` on `conn`. The cursor owns `conn` from here on.
    pub fn open(gpa: std.mem.Allocator, conn: Conn, sql: []const u8) !*Source {
        const self = try gpa.create(Source);
        errdefer gpa.destroy(self);
        self.* = .{ .gpa = gpa, .cursor = try conn.queryCursor(sql) };
        return self;
    }

    pub fn source(self: *Source) driver.Source {
        return .{ .ptr = self, .vtable = &source_vtable };
    }
};

const source_vtable = driver.Source.VTable{ .schema = srcSchema, .next = srcNext, .close = srcClose };

fn srcSchema(ptr: *anyopaque) types.Schema {
    const self: *Source = @ptrCast(@alignCast(ptr));
    return self.cursor.schema();
}
fn srcNext(ptr: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
    const self: *Source = @ptrCast(@alignCast(ptr));
    return self.cursor.nextBatch(arena);
}
fn srcClose(ptr: *anyopaque) void {
    const self: *Source = @ptrCast(@alignCast(ptr));
    self.cursor.close();
    self.gpa.destroy(self);
}

// ---------------------------------------------------------------------------
// Generic sink: auto-create then batched INSERT / upsert
// ---------------------------------------------------------------------------

/// Rows per multi-row INSERT. Bigger = fewer round-trips/commits (the dominant
/// cost of row loading), but SQL Server caps a `VALUES` list at 1000 rows.
fn flushRowsFor(dialect: Dialect) usize {
    return switch (dialect) {
        .sqlserver => 1000,
        .postgres, .mysql => 5000,
    };
}

/// Re-establishes a connection for the INSERT sink's transient retry. `ctx`
/// is read-only shared config (safe across lanes); the allocator is the
/// calling sink's own, so lane-confined allocators stay lane-confined.
pub const Redial = struct {
    ctx: *const anyopaque,
    dial: *const fn (*const anyopaque, std.mem.Allocator) anyerror!Conn,
};

pub const Sink = struct {
    gpa: std.mem.Allocator,
    conn: Conn,
    dialect: Dialect,
    table: []const u8, // quoted, qualified
    schema: types.Schema, // field names + types (owned in gpa)
    mode: ast.WriteMode,
    rows: std.array_list.Managed([]const u8), // serialized "(...)" tuples (slices into tuple_arena)
    tuple_arena: std.heap.ArenaAllocator, // backs the tuple bytes; reset per flush (no per-row malloc/free)
    flush_rows: usize, // rows per INSERT (dialect-dependent cap)
    redial: ?Redial = null,
    conn_alive: bool = true,

    pub fn open(gpa: std.mem.Allocator, conn: Conn, dialect: Dialect, table_name: []const u8, schema: types.Schema, mode: ast.WriteMode, redial: ?Redial) !*Sink {
        // On error we free only what we allocate here; the caller keeps `conn`
        // (so it can read conn.last_error) and closes it on failure.
        const self = try gpa.create(Sink);
        errdefer gpa.destroy(self);
        // own copies of schema (field names) + quoted table name
        const fields = try gpa.alloc(types.Schema.Field, schema.fields.len);
        errdefer gpa.free(fields);
        var nf: usize = 0;
        errdefer for (fields[0..nf]) |f| gpa.free(f.name);
        for (schema.fields, 0..) |f, i| {
            fields[i] = .{ .name = try gpa.dupe(u8, f.name), .ty = f.ty };
            nf += 1;
        }
        const qtable = try quoteIdent(gpa, dialect, table_name);
        errdefer gpa.free(qtable);
        self.* = .{
            .gpa = gpa,
            .conn = conn,
            .dialect = dialect,
            .table = qtable,
            .schema = .{ .fields = fields },
            .mode = mode,
            .rows = std.array_list.Managed([]const u8).init(gpa),
            .tuple_arena = std.heap.ArenaAllocator.init(gpa),
            .flush_rows = flushRowsFor(dialect),
            .redial = redial,
        };
        errdefer self.rows.deinit();
        errdefer self.tuple_arena.deinit();

        var aa = std.heap.ArenaAllocator.init(gpa);
        defer aa.deinit();
        const ddl = try createTableSql(aa.allocator(), dialect, self.table, schema, mode);
        try conn.exec(ddl);
        if (mode == .overwrite) {
            const del = try std.fmt.allocPrint(aa.allocator(), "DELETE FROM {s}", .{self.table});
            try conn.exec(del);
        }
        return self;
    }

    pub fn sink(self: *Sink) driver.Sink {
        return .{ .ptr = self, .vtable = &sink_vtable };
    }

    fn writeBatch(self: *Sink, arena: std.mem.Allocator, batch: Batch) !void {
        var r: usize = 0;
        while (r < batch.len) : (r += 1) {
            const tuple = try serializeRow(self.tuple_arena.allocator(), self.dialect, batch, r);
            try self.rows.append(tuple);
            if (self.rows.items.len >= self.flush_rows) try self.flush();
        }
        _ = arena;
    }

    fn flush(self: *Sink) !void {
        if (self.rows.items.len == 0) return;
        var aa = std.heap.ArenaAllocator.init(self.gpa);
        defer aa.deinit();
        const stmt = try buildStatement(aa.allocator(), self.dialect, self.table, self.schema, self.mode, self.rows.items);
        self.conn.exec(stmt) catch |e| {
            // Retry once over a fresh connection if the socket broke (a SQL
            // error from the server is permanent — never retried). Ambiguity
            // note: if the dropped exec actually committed, the retry can
            // double-insert on append; upsert is idempotent. Same contract as
            // re-runs (downstream dedup owns exactly-once, per split.zig).
            const rd = self.redial orelse return e;
            if (!transientNet(e)) return e;
            self.conn.close();
            self.conn_alive = false;
            std.Thread.sleep(500 * std.time.ns_per_ms);
            self.conn = try rd.dial(rd.ctx, self.gpa);
            self.conn_alive = true;
            try self.conn.exec(stmt);
        };
        self.rows.clearRetainingCapacity();
        _ = self.tuple_arena.reset(.retain_capacity);
    }

    fn closeImpl(self: *Sink) !void {
        // Release everything even if the final flush fails — otherwise a failed
        // INSERT on close leaks the connection, the tuple buffers and the sink.
        defer self.teardown();
        try self.flush();
    }

    /// Failure path: drop the buffered tuples without a final INSERT. Flushes
    /// that already ran are autocommitted and stay (downstream dedup owns
    /// exactly-once, per split.zig).
    fn abortImpl(self: *Sink) void {
        self.teardown();
    }

    fn teardown(self: *Sink) void {
        if (self.conn_alive) self.conn.close();
        self.rows.deinit();
        self.tuple_arena.deinit();
        for (self.schema.fields) |f| self.gpa.free(f.name);
        self.gpa.free(self.schema.fields);
        self.gpa.free(self.table);
        self.gpa.destroy(self);
    }
};

const sink_vtable = driver.Sink.VTable{ .writeBatch = sinkWrite, .close = sinkClose, .abort = sinkAbort };

fn sinkWrite(ptr: *anyopaque, arena: std.mem.Allocator, b: Batch) anyerror!void {
    const self: *Sink = @ptrCast(@alignCast(ptr));
    return self.writeBatch(arena, b);
}
fn sinkClose(ptr: *anyopaque) anyerror!void {
    const self: *Sink = @ptrCast(@alignCast(ptr));
    return self.closeImpl();
}
fn sinkAbort(ptr: *anyopaque) void {
    const self: *Sink = @ptrCast(@alignCast(ptr));
    self.abortImpl();
}

// ---------------------------------------------------------------------------
// SQL generation
// ---------------------------------------------------------------------------

pub fn createTableSql(arena: std.mem.Allocator, dialect: Dialect, qtable: []const u8, schema: types.Schema, mode: ast.WriteMode) ![]const u8 {
    const keys: []const []const u8 = switch (mode) {
        .upsert => |u| u.keys,
        else => &.{},
    };
    var buf = std.array_list.Managed(u8).init(arena);
    const w = buf.writer();

    if (dialect == .sqlserver) {
        try w.print("IF OBJECT_ID(N'{s}', N'U') IS NULL CREATE TABLE {s} (", .{ stripQuotes(qtable), qtable });
    } else {
        try w.print("CREATE TABLE IF NOT EXISTS {s} (", .{qtable});
    }

    for (schema.fields, 0..) |f, i| {
        if (i > 0) try w.writeByte(',');
        const is_key = nameIn(keys, f.name);
        const qn = try quoteIdent(arena, dialect, f.name);
        try w.print("{s} {s}", .{ qn, try dialect.ddlType(arena, f.ty, is_key) });
        if (is_key) try w.writeAll(" NOT NULL");
    }
    if (keys.len > 0) {
        try w.writeAll(", PRIMARY KEY (");
        for (keys, 0..) |k, i| {
            if (i > 0) try w.writeByte(',');
            try w.writeAll(try quoteIdent(arena, dialect, k));
        }
        try w.writeByte(')');
    }
    try w.writeByte(')');
    return buf.toOwnedSlice();
}

fn buildStatement(arena: std.mem.Allocator, dialect: Dialect, qtable: []const u8, schema: types.Schema, mode: ast.WriteMode, rows: []const []const u8) ![]const u8 {
    const cols = try colList(arena, dialect, schema);
    const values = try std.mem.join(arena, ",", rows);

    if (dialect == .sqlserver and mode == .upsert) {
        return buildMerge(arena, dialect, qtable, schema, mode.upsert.keys, cols, values);
    }

    var buf = std.array_list.Managed(u8).init(arena);
    const w = buf.writer();
    try w.print("INSERT INTO {s} ({s}) VALUES {s}", .{ qtable, cols, values });

    switch (mode) {
        .upsert => |u| switch (dialect) {
            .postgres => {
                try w.writeAll(" ON CONFLICT (");
                for (u.keys, 0..) |k, i| {
                    if (i > 0) try w.writeByte(',');
                    try w.writeAll(try quoteIdent(arena, dialect, k));
                }
                try w.writeAll(") DO UPDATE SET ");
                try writeUpdateSet(w, arena, dialect, schema, u.keys, "EXCLUDED.");
            },
            .mysql => {
                try w.writeAll(" ON DUPLICATE KEY UPDATE ");
                try writeMysqlUpdate(w, arena, dialect, schema, u.keys);
            },
            .sqlserver => unreachable,
        },
        else => {},
    }
    return buf.toOwnedSlice();
}

fn buildMerge(arena: std.mem.Allocator, dialect: Dialect, qtable: []const u8, schema: types.Schema, keys: []const []const u8, cols: []const u8, values: []const u8) ![]const u8 {
    var buf = std.array_list.Managed(u8).init(arena);
    const w = buf.writer();
    try w.print("MERGE INTO {s} AS T USING (VALUES {s}) AS S ({s}) ON ", .{ qtable, values, cols });
    for (keys, 0..) |k, i| {
        if (i > 0) try w.writeAll(" AND ");
        const qk = try quoteIdent(arena, dialect, k);
        try w.print("T.{s}=S.{s}", .{ qk, qk });
    }
    try w.writeAll(" WHEN MATCHED THEN UPDATE SET ");
    try writeUpdateSet(w, arena, dialect, schema, keys, "S.");
    try w.print(" WHEN NOT MATCHED THEN INSERT ({s}) VALUES (", .{cols});
    for (schema.fields, 0..) |f, i| {
        if (i > 0) try w.writeByte(',');
        try w.print("S.{s}", .{try quoteIdent(arena, dialect, f.name)});
    }
    try w.writeAll(");");
    return buf.toOwnedSlice();
}

fn writeUpdateSet(w: anytype, arena: std.mem.Allocator, dialect: Dialect, schema: types.Schema, keys: []const []const u8, src_prefix: []const u8) !void {
    var first = true;
    for (schema.fields) |f| {
        if (nameIn(keys, f.name)) continue;
        if (!first) try w.writeByte(',');
        first = false;
        const qn = try quoteIdent(arena, dialect, f.name);
        try w.print("{s}={s}{s}", .{ qn, src_prefix, qn });
    }
}

fn writeMysqlUpdate(w: anytype, arena: std.mem.Allocator, dialect: Dialect, schema: types.Schema, keys: []const []const u8) !void {
    var first = true;
    for (schema.fields) |f| {
        if (nameIn(keys, f.name)) continue;
        if (!first) try w.writeByte(',');
        first = false;
        const qn = try quoteIdent(arena, dialect, f.name);
        try w.print("{s}=VALUES({s})", .{ qn, qn });
    }
}

fn colList(arena: std.mem.Allocator, dialect: Dialect, schema: types.Schema) ![]const u8 {
    var buf = std.array_list.Managed(u8).init(arena);
    for (schema.fields, 0..) |f, i| {
        if (i > 0) try buf.append(',');
        try buf.appendSlice(try quoteIdent(arena, dialect, f.name));
    }
    return buf.toOwnedSlice();
}

fn serializeRow(gpa: std.mem.Allocator, dialect: Dialect, batch: Batch, row: usize) ![]const u8 {
    var buf = std.array_list.Managed(u8).init(gpa);
    try buf.append('(');
    for (batch.columns, 0..) |*c, i| {
        if (i > 0) try buf.append(',');
        try serializeValue(buf.writer(), dialect, c.getValue(row), gpa);
    }
    try buf.append(')');
    return buf.toOwnedSlice();
}

fn serializeValue(w: anytype, dialect: Dialect, v: Value, scratch: std.mem.Allocator) !void {
    switch (v) {
        .null => try w.writeAll("NULL"),
        .bool => |b| try w.writeAll(if (dialect == .sqlserver) (if (b) "1" else "0") else (if (b) "TRUE" else "FALSE")),
        .int => |x| try w.print("{d}", .{x}),
        .float => |x| try w.print("{d}", .{x}),
        .decimal => |d| {
            const s = try eval.formatDecimal(scratch, d.unscaled, d.scale);
            defer scratch.free(s);
            try w.writeAll(s);
        },
        .string => |s| try quoteString(w, dialect, s),
        .bytes => |s| try hexLiteral(w, dialect, s),
        .date => |days| try w.print("'{s}'", .{try eval.formatDate(scratch, days)}),
        .time => |t| try w.print("'{s}'", .{try eval.formatTime(scratch, t)}),
        .timestamp => |micros| try w.print("'{s}'", .{try eval.formatTimestamp(scratch, micros)}),
    }
}

fn quoteString(w: anytype, dialect: Dialect, s: []const u8) !void {
    try w.writeByte('\'');
    for (s) |ch| {
        if (ch == '\'') {
            try w.writeAll("''");
        } else if (ch == '\\' and dialect == .mysql) {
            try w.writeAll("\\\\");
        } else {
            try w.writeByte(ch);
        }
    }
    try w.writeByte('\'');
}

fn hexLiteral(w: anytype, dialect: Dialect, s: []const u8) !void {
    if (dialect == .postgres) {
        try w.writeAll("'\\x");
        for (s) |b| try w.print("{x:0>2}", .{b});
        try w.writeByte('\'');
    } else {
        try w.writeAll("0x");
        for (s) |b| try w.print("{x:0>2}", .{b});
    }
}

// ---------------------------------------------------------------------------
// Bulk-load text format (PostgreSQL COPY / MySQL LOAD DATA share it): tab-separated
// fields, `\N` for null, newline-terminated rows, `\`-escaped t/n/r/backslash, and
// dates/timestamps as real text. The only difference is the bool literal.
// ---------------------------------------------------------------------------

pub const BulkFormat = struct {
    bool_true: []const u8 = "t",
    bool_false: []const u8 = "f",
};

pub fn appendBulkText(w: anytype, arena: std.mem.Allocator, batch: Batch, fmt: BulkFormat) !void {
    var r: usize = 0;
    while (r < batch.len) : (r += 1) {
        for (batch.columns, 0..) |*col, i| {
            if (i > 0) try w.writeByte('\t');
            try bulkValue(w, arena, col.getValue(r), fmt);
        }
        try w.writeByte('\n');
    }
}

fn bulkValue(w: anytype, arena: std.mem.Allocator, v: Value, fmt: BulkFormat) !void {
    if (v.isNull()) {
        try w.writeAll("\\N");
    } else {
        try bulkEscaped(w, try valueText(arena, v, fmt));
    }
}

/// The plain text of a non-null value (no escaping), with real date/timestamp
/// formatting — shared by the delimited bulk format and the TDS NVARCHAR encoding.
pub fn valueText(arena: std.mem.Allocator, v: Value, fmt: BulkFormat) ![]const u8 {
    return switch (v) {
        .null => "",
        .bool => |b| if (b) fmt.bool_true else fmt.bool_false,
        .date => |days| try eval.formatDate(arena, days),
        .time => |t| try eval.formatTime(arena, t),
        .timestamp => |micros| try eval.formatTimestamp(arena, micros),
        else => try eval.valueToString(arena, v),
    };
}

fn bulkEscaped(w: anytype, s: []const u8) !void {
    for (s) |c| switch (c) {
        '\\' => try w.writeAll("\\\\"),
        '\t' => try w.writeAll("\\t"),
        '\n' => try w.writeAll("\\n"),
        '\r' => try w.writeAll("\\r"),
        else => try w.writeByte(c),
    };
}

pub fn quoteIdent(arena: std.mem.Allocator, dialect: Dialect, name: []const u8) ![]const u8 {
    var buf = std.array_list.Managed(u8).init(arena);
    var it = std.mem.splitScalar(u8, name, '.');
    var first = true;
    while (it.next()) |part| {
        if (!first) try buf.append('.');
        first = false;
        try buf.append(dialect.qOpen());
        try buf.appendSlice(part);
        try buf.append(dialect.qClose());
    }
    return buf.toOwnedSlice();
}

fn stripQuotes(qname: []const u8) []const u8 {
    // for sqlserver OBJECT_ID we want the unquoted dotted name
    return qname; // caller passes the quoted form; OBJECT_ID accepts [a].[b]
}

fn nameIn(names: []const []const u8, n: []const u8) bool {
    for (names) |x| {
        if (std.mem.eql(u8, x, n)) return true;
    }
    return false;
}

// ---------------------------------------------------------------------------
// Text-format value parsing (shared by MySQL + Postgres result sets)
// ---------------------------------------------------------------------------

/// Errors with `UnparseableNumber` when an int/float cell doesn't parse:
/// silently coercing bad source text to 0 would corrupt the pipeline's output
/// with valid-looking values. The driver cursors wrap the error with the failing
/// column's name in `last_error`.
pub fn coerceText(arena: std.mem.Allocator, text: ?[]const u8, ty: types.Type) !Value {
    const t = text orelse return .null;
    return switch (ty.kind) {
        .int => .{ .int = std.fmt.parseInt(i64, std.mem.trim(u8, t, " "), 10) catch return error.UnparseableNumber },
        .float => .{ .float = std.fmt.parseFloat(f64, t) catch return error.UnparseableNumber },
        .decimal => parseDecimalText(t),
        .bool => .{ .bool = t.len > 0 and (t[0] == '1' or t[0] == 't' or t[0] == 'T' or t[0] == 'y' or t[0] == 'Y') },
        .date => .{ .date = @intCast(parseDateText(t)) },
        .timestamp => .{ .timestamp = parseDatetimeText(t) },
        else => .{ .string = try arena.dupe(u8, t) },
    };
}

pub fn parseDecimalText(t: []const u8) Value {
    var neg = false;
    var s = t;
    if (s.len > 0 and s[0] == '-') {
        neg = true;
        s = s[1..];
    }
    var unscaled: i128 = 0;
    var scale: u8 = 0;
    var after_dot = false;
    for (s) |c| {
        if (c == '.') {
            after_dot = true;
            continue;
        }
        if (c < '0' or c > '9') continue;
        unscaled = unscaled * 10 + (c - '0');
        if (after_dot) scale += 1;
    }
    return .{ .decimal = .{ .unscaled = if (neg) -unscaled else unscaled, .scale = scale } };
}

fn parseDateText(t: []const u8) i64 {
    if (t.len < 10) return 0;
    return daysFromCivil(atoiN(t[0..4]), @intCast(atoiN(t[5..7])), @intCast(atoiN(t[8..10])));
}

fn parseDatetimeText(t: []const u8) i64 {
    const days = parseDateText(t);
    var secs: i64 = 0;
    if (t.len >= 19) secs = atoiN(t[11..13]) * 3600 + atoiN(t[14..16]) * 60 + atoiN(t[17..19]);
    return days * 86_400_000_000 + secs * 1_000_000;
}

fn atoiN(s: []const u8) i64 {
    var v: i64 = 0;
    for (s) |c| {
        if (c >= '0' and c <= '9') v = v * 10 + (c - '0');
    }
    return v;
}

fn daysFromCivil(y0: i64, m: u32, d: u32) i64 {
    const y = if (m <= 2) y0 - 1 else y0;
    const era = @divFloor(if (y >= 0) y else y - 399, 400);
    const yoe = y - era * 400;
    const mi: i64 = @intCast(m);
    const di: i64 = @intCast(d);
    const doy = @divFloor(153 * (if (m > 2) mi - 3 else mi + 9) + 2, 5) + di - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146097 + doe - 719468;
}


test "create table + upsert SQL (postgres)" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const schema = types.Schema{ .fields = &.{
        .{ .name = "id", .ty = types.Type.init(.int) },
        .{ .name = "name", .ty = types.Type.init(.string) },
    } };
    const qt = try quoteIdent(a, .postgres, "people");
    const ddl = try createTableSql(a, .postgres, qt, schema, .{ .upsert = .{ .keys = &.{"id"} } });
    try std.testing.expect(std.mem.indexOf(u8, ddl, "CREATE TABLE IF NOT EXISTS \"people\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, ddl, "\"id\" BIGINT NOT NULL") != null);
    try std.testing.expect(std.mem.indexOf(u8, ddl, "PRIMARY KEY (\"id\")") != null);

    const stmt = try buildStatement(a, .postgres, qt, schema, .{ .upsert = .{ .keys = &.{"id"} } }, &.{ "(1,'a')", "(2,'b')" });
    try std.testing.expect(std.mem.indexOf(u8, stmt, "INSERT INTO \"people\" (\"id\",\"name\") VALUES (1,'a'),(2,'b')") != null);
    try std.testing.expect(std.mem.indexOf(u8, stmt, "ON CONFLICT (\"id\") DO UPDATE SET \"name\"=EXCLUDED.\"name\"") != null);
}

test "coerceText: bad numeric text errors instead of silently zeroing" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqual(@as(i64, 42), (try coerceText(a, "42", types.Type.init(.int))).int);
    try std.testing.expectEqual(@as(i64, 7), (try coerceText(a, " 7 ", types.Type.init(.int))).int);
    try std.testing.expect((try coerceText(a, null, types.Type.init(.int))).isNull());
    try std.testing.expectError(error.UnparseableNumber, coerceText(a, "abc", types.Type.init(.int)));
    try std.testing.expectError(error.UnparseableNumber, coerceText(a, "", types.Type.init(.int)));
    try std.testing.expectError(error.UnparseableNumber, coerceText(a, "1.2.3", types.Type.init(.float)));
}

test "value serialization escapes quotes and formats dates" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var buf = std.array_list.Managed(u8).init(a);
    try serializeValue(buf.writer(), .postgres, .{ .string = "O'Brien" }, a);
    try std.testing.expectEqualStrings("'O''Brien'", buf.items);

    buf.clearRetainingCapacity();
    try serializeValue(buf.writer(), .postgres, .{ .date = 0 }, a); // 1970-01-01
    try std.testing.expectEqualStrings("'1970-01-01'", buf.items);
}
