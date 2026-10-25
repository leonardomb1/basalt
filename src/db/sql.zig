//! Shared SQL-database plumbing. A `Conn` is any protocol client (mysql/postgres/
//! tds) that can run a query and exec a statement. `Dialect` captures the only
//! per-database differences (type mapping, identifier quoting, upsert syntax).
//! `Source` and `Sink` are written once on top of these, so each new database is
//! just a protocol client and a dialect. StarRocks and Doris speak MySQL on the
//! wire but have their own DDL: type names and a mandatory table model and
//! distribution clause.
//!
//! Ownership: `Conn.queryCursor` returns a cursor that owns the connection until
//! closed; on error the connection stays open so the caller can read `last_error`,
//! and the caller closes it. A `Report` carries the server's own message for a read
//! that fails mid-stream. A batch returning a second result set is refused: a read
//! has one schema, and dropping or appending the rest would answer something the
//! batch never said.
//!
//! TLS: `require` verifies against the system CA bundle and hostname, `insecure`
//! encrypts without verifying. A `TlsState` must sit at a stable heap address
//! before `start`, and the socket reader's buffer must hold a whole ciphertext
//! record. It uses the vendored `tls_client.zig` because MySQL servers always
//! request a client certificate, which std's client does not handle.
//!
//! Sinks: the INSERT sink batches multi-row INSERTs (SQL Server caps a VALUES list
//! at 1000 rows) and redials on a transient error. Bulk sinks (COPY, LOAD DATA,
//! INSERT BULK) accumulate a `SEGMENT_BYTES` segment, send it as one statement,
//! verify the server's row count, and only then discard it, so the segment is the
//! replay unit: a dead connection rolls the statement back, and the count check
//! turns silent row drops into errors. The one unavoidable window is a connection
//! lost after the server committed but before its reply, where the replay
//! double-writes. Flushes already committed stay on abort; downstream dedup owns
//! exactly-once (see split.zig).
//!
//! Text from the wire is typed by `cell.wire`. A cell that does not parse as its
//! numeric type is an error, never a quiet 0 or a different number.

const std = @import("std");
const op = @import("../exec/op.zig");
const TlsClient = @import("../net/tls_client.zig");
const types = @import("../lang/types.zig");
const Batch = @import("../exec/batch.zig").Batch;
const Value = @import("../exec/value.zig").Value;
const column = @import("../exec/column.zig");
const celltext = @import("../exec/celltext.zig");
const eval = @import("../exec/eval.zig");
const driver = @import("../connect/driver.zig");
const retry = @import("../connect/retry.zig");
const ast = @import("../lang/ast.zig");
const typemap = @import("typemap.zig");
const hexfmt = @import("../format/hex.zig");

pub const STREAM_ROWS = 4096;

pub const SEGMENT_BYTES = 4 << 20;

pub const TlsMode = enum { off, require, insecure };

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

pub const Cursor = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        schema: *const fn (*anyopaque) types.Schema,
        nextBatch: *const fn (*anyopaque, std.mem.Allocator) anyerror!?Batch,
        close: *const fn (*anyopaque) void,
        last_error: ?*const fn (*anyopaque) []const u8 = null,
    };

    pub fn schema(self: Cursor) types.Schema {
        return self.vtable.schema(self.ptr);
    }
    pub fn lastError(self: Cursor) []const u8 {
        const f = self.vtable.last_error orelse return "";
        return f(self.ptr);
    }
    pub fn nextBatch(self: Cursor, arena: std.mem.Allocator) anyerror!?Batch {
        return self.vtable.nextBatch(self.ptr, arena) catch |e| return onWire(e);
    }
    pub fn close(self: Cursor) void {
        self.vtable.close(self.ptr);
    }
};

pub const RowStep = enum { row, end };

pub const one_result_set = "the query returned more than one result set; a read takes one — keep a single SELECT in the batch (statements before or after it are fine)";

/// Rename a socket's `EndOfStream`/`ReadFailed`/`WriteFailed` to names the exit
/// code treats as transient. Applied where an operation leaves a driver, where the
/// peer is known to be a database socket rather than a local file.
pub fn onWire(e: anyerror) anyerror {
    return switch (e) {
        error.EndOfStream, error.TlsConnectionTruncated => error.ServerClosedConnection,
        error.ReadFailed, error.WriteFailed => error.ConnectionIoFailed,
        else => e,
    };
}

/// `conn` needs `meta_arena`, `openCursor(sql)` and a `wire`. On open failure
/// the connection is left open for the caller to read `last_error` and close.
pub fn openTextCursor(conn: anytype, sql_text: []const u8, vt: *const Cursor.VTable) !Cursor {
    conn.meta_arena = std.heap.ArenaAllocator.init(conn.wire.gpa);
    conn.openCursor(sql_text) catch |e| {
        conn.meta_arena.deinit();
        return e;
    };
    return .{ .ptr = conn, .vtable = vt };
}

/// `conn` needs `cols` (each with `.engine_type`), `cur_schema`, `done`, and a
/// `nextRow(arena, builders) !RowStep` appending exactly one value per column on `.row`.
pub fn fetchTextBatch(conn: anytype, arena: std.mem.Allocator) !?Batch {
    if (conn.done) return null;
    const ncol = conn.cols.len;
    if (ncol == 0) return null;
    const builders = try arena.alloc(column.Builder, ncol);
    for (conn.cols, builders) |c, *b| b.* = try column.Builder.initCapacity(arena, c.engine_type, STREAM_ROWS);

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

pub const Conn = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        queryCursor: *const fn (*anyopaque, []const u8) anyerror!Cursor,
        exec: *const fn (*anyopaque, []const u8) anyerror!void,
        close: *const fn (*anyopaque) void,
        last_error: ?*const fn (*anyopaque) []const u8 = null,
    };

    pub fn lastError(self: Conn) []const u8 {
        const f = self.vtable.last_error orelse return "";
        return f(self.ptr);
    }

    pub fn queryCursor(self: Conn, sql: []const u8) anyerror!Cursor {
        return self.vtable.queryCursor(self.ptr, sql) catch |e| return onWire(e);
    }
    pub fn exec(self: Conn, sql: []const u8) anyerror!void {
        return self.vtable.exec(self.ptr, sql) catch |e| return onWire(e);
    }
    pub fn close(self: Conn) void {
        self.vtable.close(self.ptr);
    }
};

/// The `*anyopaque` shims of `Conn.VTable` for a connection type exposing
/// `queryCursor`/`exec`/`close`.
pub fn connVTable(comptime T: type) Conn.VTable {
    return .{
        .queryCursor = struct {
            fn f(p: *anyopaque, q: []const u8) anyerror!Cursor {
                return @as(*T, @ptrCast(@alignCast(p))).queryCursor(q);
            }
        }.f,
        .exec = struct {
            fn f(p: *anyopaque, q: []const u8) anyerror!void {
                return @as(*T, @ptrCast(@alignCast(p))).exec(q);
            }
        }.f,
        .close = struct {
            fn f(p: *anyopaque) void {
                @as(*T, @ptrCast(@alignCast(p))).close();
            }
        }.f,
        .last_error = struct {
            fn f(p: *anyopaque) []const u8 {
                return @as(*T, @ptrCast(@alignCast(p))).wire.last_error;
            }
        }.f,
    };
}

/// A load the server refused: its own message into `errctx`, so the run reports
/// why (a duplicate key, a check constraint) rather than `PgQueryFailed`.
fn refused(errctx: ?*op.ErrCtx, dialect: Dialect, msg: []const u8, e: anyerror) anyerror {
    if (errctx) |ec| if (msg.len > 0) ec.set("{s} refused the load: {s}", .{ @tagName(dialect), msg });
    return e;
}

/// `Cursor.VTable` for drivers whose cursor is the connection itself and which pull
/// rows through the shared text-protocol reader; tds writes its own.
pub fn textCursorVTable(comptime T: type) Cursor.VTable {
    return .{
        .schema = struct {
            fn f(p: *anyopaque) types.Schema {
                return @as(*T, @ptrCast(@alignCast(p))).cur_schema.*;
            }
        }.f,
        .nextBatch = struct {
            fn f(p: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
                return fetchTextBatch(@as(*T, @ptrCast(@alignCast(p))), arena);
            }
        }.f,
        .close = struct {
            fn f(p: *anyopaque) void {
                closeTextCursor(@as(*T, @ptrCast(@alignCast(p))));
            }
        }.f,
        .last_error = struct {
            fn f(p: *anyopaque) []const u8 {
                return @as(*T, @ptrCast(@alignCast(p))).wire.last_error;
            }
        }.f,
    };
}

/// What a string literal is for: a value compared in a pushed predicate, a value
/// written into a table, or a name looked up in the catalog.
pub const LitKind = enum { predicate, data, catalog_name };

pub const Dialect = enum {
    postgres,
    mysql,
    sqlserver,
    starrocks,
    doris,

    /// Read over the MySQL protocol and written as MySQL writes SQL: backticks, and
    /// `\` an escape inside a string literal.
    pub fn mysqlWire(self: Dialect) bool {
        return switch (self) {
            .mysql, .starrocks, .doris => true,
            .postgres, .sqlserver => false,
        };
    }

    /// `writeStrLit` of a `.predicate` literal, allocated.
    pub fn strLit(self: Dialect, arena: std.mem.Allocator, s: []const u8) ![]const u8 {
        var out = std.array_list.Managed(u8).init(arena);
        try self.writeStrLit(out.writer(), s, .predicate);
        return out.toOwnedSlice();
    }

    /// `s` as a string literal: quotes doubled, and on the MySQL wire a `\`
    /// doubled too, so text from data cannot end the literal. On SQL Server a
    /// `.data` value and a `.catalog_name` are `N'...'`, so text outside the code
    /// page survives; a `.predicate` stays `'...'` (ASCII only, see `textLitOk`)
    /// so a VARCHAR column is compared without being converted.
    pub fn writeStrLit(self: Dialect, w: anytype, s: []const u8, kind: LitKind) !void {
        if (self == .sqlserver and kind != .predicate) try w.writeByte('N');
        try w.writeByte('\'');
        for (s) |c| {
            if (c == '\'' or (c == '\\' and self.mysqlWire())) try w.writeByte(c);
            try w.writeByte(c);
        }
        try w.writeByte('\'');
    }

    /// Whether the dialect can quote `name` at all: StarRocks's parser collapses a
    /// doubled backtick to nothing, so `a``b` would name `ab`.
    pub fn quotable(self: Dialect, name: []const u8) bool {
        return self != .starrocks or std.mem.indexOfScalar(u8, name, '`') == null;
    }

    /// Whether `s` may be sent as a string literal at all: SQL Server reads a
    /// non-ASCII one through the column's code page, and the MySQL wire reads `\`
    /// as an escape.
    pub fn textLitOk(self: Dialect, s: []const u8) bool {
        return switch (self) {
            .sqlserver => for (s) |c| {
                if (!std.ascii.isPrint(c)) break false;
            } else true,
            .mysql, .starrocks, .doris => std.mem.indexOfScalar(u8, s, '\\') == null,
            .postgres => true,
        };
    }

    /// A bool as a value; SQL Server has no TRUE, and a predicate is no value there.
    pub fn boolLit(self: Dialect, b: bool) []const u8 {
        return switch (self) {
            .sqlserver => if (b) "1" else "0",
            .postgres, .mysql, .starrocks, .doris => if (b) "TRUE" else "FALSE",
        };
    }

    /// `l IS [NOT] DISTINCT FROM r`: PostgreSQL as written, the MySQL family as
    /// `<=>`; null on SQL Server, which before 2022 has neither.
    pub fn distinctPred(self: Dialect, arena: std.mem.Allocator, l: []const u8, r: []const u8, distinct: bool) !?[]const u8 {
        return switch (self) {
            .postgres => try std.fmt.allocPrint(arena, "({s} IS {s}DISTINCT FROM {s})", .{ l, if (distinct) "" else "NOT ", r }),
            .mysql, .starrocks, .doris => if (distinct)
                try std.fmt.allocPrint(arena, "(NOT ({s} <=> {s}))", .{ l, r })
            else
                try std.fmt.allocPrint(arena, "({s} <=> {s})", .{ l, r }),
            .sqlserver => null,
        };
    }

    /// The function giving text's length in bytes, trailing spaces counted, or
    /// null where none is used.
    pub fn byteLengthFn(self: Dialect) ?[]const u8 {
        return switch (self) {
            .sqlserver => "DATALENGTH",
            .mysql, .starrocks, .doris => "LENGTH",
            .postgres => null,
        };
    }

    /// `c` matched as itself inside a LIKE pattern, appended to `out`; false when
    /// it cannot be. SQL Server brackets `%`, `_` and `[`; elsewhere `\` is the
    /// escape, and `%`, `_` and `\` are refused rather than escaped.
    pub fn likeLiteral(self: Dialect, out: *std.array_list.Managed(u8), c: u8) !bool {
        switch (self) {
            .sqlserver => switch (c) {
                '%', '_', '[' => try out.writer().print("[{c}]", .{c}),
                else => try out.append(c),
            },
            .postgres, .mysql, .starrocks, .doris => switch (c) {
                '%', '_', '\\' => return false,
                else => try out.append(c),
            },
        }
        return true;
    }

    /// Whether CONCAT is NULL when any argument is, as the engine's: the MySQL
    /// family's is, PostgreSQL's and SQL Server's skip NULLs.
    pub fn concatNullable(self: Dialect) bool {
        return switch (self) {
            .mysql, .starrocks, .doris => true,
            .postgres, .sqlserver => false,
        };
    }

    pub fn trimExpr(self: Dialect, arena: std.mem.Allocator, arg: []const u8) ![]const u8 {
        return switch (self) {
            .sqlserver => std.fmt.allocPrint(arena, "LTRIM(RTRIM({s}))", .{arg}),
            .postgres, .mysql, .starrocks, .doris => std.fmt.allocPrint(arena, "TRIM({s})", .{arg}),
        };
    }

    pub fn ceilFn(self: Dialect) []const u8 {
        return switch (self) {
            .sqlserver => "CEILING",
            .postgres, .mysql, .starrocks, .doris => "CEIL",
        };
    }

    /// SQL Server has only `%`; both spellings take the dividend's sign.
    pub fn modExpr(self: Dialect, arena: std.mem.Allocator, a: []const u8, b: []const u8) ![]const u8 {
        return switch (self) {
            .sqlserver => std.fmt.allocPrint(arena, "({s} % {s})", .{ a, b }),
            .postgres, .mysql, .starrocks, .doris => std.fmt.allocPrint(arena, "MOD({s}, {s})", .{ a, b }),
        };
    }

    pub fn repeatFn(self: Dialect) []const u8 {
        return switch (self) {
            .sqlserver => "REPLICATE",
            .postgres, .mysql, .starrocks, .doris => "REPEAT",
        };
    }

    /// COUNT that cannot overflow a 32-bit result, as SQL Server's COUNT does.
    pub fn countFn(self: Dialect) []const u8 {
        return switch (self) {
            .sqlserver => "COUNT_BIG",
            .postgres, .mysql, .starrocks, .doris => "COUNT",
        };
    }

    /// SUM of an integer column that cannot overflow the column's own type, as
    /// SQL Server's int SUM does.
    pub fn intSum(self: Dialect, arena: std.mem.Allocator, col: []const u8) ![]const u8 {
        return switch (self) {
            .sqlserver => std.fmt.allocPrint(arena, "SUM(CAST({s} AS BIGINT))", .{col}),
            .postgres, .mysql, .starrocks, .doris => std.fmt.allocPrint(arena, "SUM({s})", .{col}),
        };
    }

    /// One ORDER BY key with its nulls placed explicitly: `NULLS FIRST/LAST` on
    /// postgres, a leading `k IS NULL` key on the MySQL family, a `CASE` key on SQL Server.
    pub fn orderKey(self: Dialect, w: anytype, q: []const u8, desc: bool, nulls_first: bool) !void {
        const dir = if (desc) " DESC" else "";
        switch (self) {
            .postgres => try w.print("{s}{s} {s}", .{ q, dir, if (nulls_first) "NULLS FIRST" else "NULLS LAST" }),
            .mysql, .starrocks, .doris => try w.print("({s} IS NULL){s}, {s}{s}", .{ q, if (nulls_first) " DESC" else "", q, dir }),
            .sqlserver => try w.print("CASE WHEN {s} IS NULL THEN {d} ELSE {d} END, {s}{s}", .{ q, @intFromBool(!nulls_first), @intFromBool(nulls_first), q, dir }),
        }
    }

    /// The first `rows` rows of `base` in `order` (an ` ORDER BY ...` clause or "").
    pub fn firstRows(self: Dialect, arena: std.mem.Allocator, base: []const u8, order: []const u8, rows: u64) ![]const u8 {
        return switch (self) {
            .sqlserver => std.fmt.allocPrint(arena, "SELECT TOP ({d}) * FROM ({s}) _t{s}", .{ rows, base, order }),
            .postgres, .mysql, .starrocks, .doris => std.fmt.allocPrint(arena, "SELECT * FROM ({s}) _t{s} LIMIT {d}", .{ base, order, rows }),
        };
    }

    fn qOpen(self: Dialect) u8 {
        return switch (self) {
            .postgres => '"',
            .mysql, .starrocks, .doris => '`',
            .sqlserver => '[',
        };
    }
    fn qClose(self: Dialect) u8 {
        return switch (self) {
            .postgres => '"',
            .mysql, .starrocks, .doris => '`',
            .sqlserver => ']',
        };
    }

    /// The column type a CREATE TABLE gets for an engine type, from `typemap`.
    pub fn ddlType(self: Dialect, arena: std.mem.Allocator, ty: types.Type, is_key: bool) ![]const u8 {
        return typemap.ddlType(typemap.dialectOf(self), arena, ty, is_key);
    }

    /// The type a pushed-down `CAST` uses, or null when the predicate stays in the
    /// engine, from `typemap`.
    pub fn castType(self: Dialect, arena: std.mem.Allocator, ty: types.Type) !?[]const u8 {
        return typemap.castType(typemap.dialectOf(self), arena, ty);
    }
};

pub const Report = struct {
    ctx: *anyopaque,
    f: *const fn (ctx: *anyopaque, e: anyerror, msg: []const u8) void,
};

pub const Source = struct {
    gpa: std.mem.Allocator,
    cursor: Cursor,
    report: ?Report = null,

    pub fn open(gpa: std.mem.Allocator, conn: Conn, sql: []const u8) !*Source {
        const self = try gpa.create(Source);
        errdefer gpa.destroy(self);
        self.* = .{ .gpa = gpa, .cursor = try conn.queryCursor(sql) };
        return self;
    }

    pub fn source(self: *Source) driver.Source {
        return .{ .ptr = self, .vtable = &source_vtable };
    }

    pub fn schema(self: *Source) types.Schema {
        return self.cursor.schema();
    }
    pub fn next(self: *Source, arena: std.mem.Allocator) anyerror!?Batch {
        return self.cursor.nextBatch(arena) catch |e| {
            if (self.report) |r| {
                const msg = self.cursor.lastError();
                if (msg.len > 0) r.f(r.ctx, e, msg);
            }
            return e;
        };
    }
    pub fn close(self: *Source) void {
        self.cursor.close();
        self.gpa.destroy(self);
    }
};

const source_vtable = driver.sourceVTable(Source);

fn flushRowsFor(dialect: Dialect) usize {
    return switch (dialect) {
        .sqlserver => 1000,
        .postgres, .mysql, .starrocks, .doris => 5000,
    };
}

fn ownedSchema(gpa: std.mem.Allocator, schema: types.Schema) !types.Schema {
    const fields = try gpa.alloc(types.Schema.Field, schema.fields.len);
    errdefer gpa.free(fields);
    var nf: usize = 0;
    errdefer for (fields[0..nf]) |f| gpa.free(f.name);
    for (schema.fields, fields) |f, *o| {
        o.* = .{ .name = try gpa.dupe(u8, f.name), .ty = f.ty };
        nf += 1;
    }
    return .{ .fields = fields };
}

fn freeSchema(gpa: std.mem.Allocator, schema: types.Schema) void {
    for (schema.fields) |f| gpa.free(f.name);
    gpa.free(schema.fields);
}

pub const Redial = struct {
    ctx: *const anyopaque,
    dial: *const fn (*const anyopaque, std.mem.Allocator) anyerror!Conn,
};

pub const Sink = struct {
    gpa: std.mem.Allocator,
    conn: Conn,
    dialect: Dialect,
    table: []const u8,
    schema: types.Schema,
    mode: ast.WriteMode,
    stmt: std.array_list.Managed(u8),
    head_len: usize,
    tail: []const u8,
    pending: usize = 0,
    flush_rows: usize,
    errctx: ?*op.ErrCtx = null,
    redial: ?Redial = null,
    conn_alive: bool = true,

    pub fn open(gpa: std.mem.Allocator, conn: Conn, dialect: Dialect, table_name: []const u8, schema: types.Schema, mode: ast.WriteMode, redial: ?Redial) !*Sink {
        const self = try gpa.create(Sink);
        errdefer gpa.destroy(self);
        const owned = try ownedSchema(gpa, schema);
        errdefer freeSchema(gpa, owned);
        const qtable = try quoteIdent(gpa, dialect, table_name);
        errdefer gpa.free(qtable);
        var aa = std.heap.ArenaAllocator.init(gpa);
        defer aa.deinit();
        const parts = try statementParts(aa.allocator(), dialect, qtable, schema, mode);
        var stmt = std.array_list.Managed(u8).init(gpa);
        errdefer stmt.deinit();
        try stmt.appendSlice(parts.head);
        const tail = try gpa.dupe(u8, parts.tail);
        errdefer gpa.free(tail);
        self.* = .{
            .gpa = gpa,
            .conn = conn,
            .dialect = dialect,
            .table = qtable,
            .schema = owned,
            .mode = mode,
            .stmt = stmt,
            .head_len = parts.head.len,
            .tail = tail,
            .flush_rows = flushRowsFor(dialect),
            .redial = redial,
        };

        const ddl = try createTableSql(aa.allocator(), dialect, self.table, schema, mode);
        try conn.exec(ddl);
        return self;
    }

    pub fn sink(self: *Sink) driver.Sink {
        return .{ .ptr = self, .vtable = &sink_vtable };
    }

    /// Each row's tuple is appended straight into the pending statement, after its head.
    pub fn writeBatch(self: *Sink, _: std.mem.Allocator, batch: Batch) !void {
        switch (self.dialect) {
            inline else => |d| for (0..batch.len) |r| {
                if (self.pending > 0) try self.stmt.append(',');
                try writeRow(d, &self.stmt, batch, r);
                self.pending += 1;
                if (self.pending >= self.flush_rows) try self.flush();
            },
        }
    }

    fn flush(self: *Sink) !void {
        if (self.pending == 0) return;
        try self.stmt.appendSlice(self.tail);
        const stmt = self.stmt.items;
        self.conn.exec(stmt) catch |e| {
            const rd = self.redial orelse return refused(self.errctx, self.dialect, self.conn.lastError(), e);
            if (!driver.transientNet(e)) return refused(self.errctx, self.dialect, self.conn.lastError(), e);
            self.conn.close();
            self.conn_alive = false;
            if (!retry.pauseWith(std.Thread.sleep, 500)) return e;
            self.conn = try rd.dial(rd.ctx, self.gpa);
            self.conn_alive = true;
            try self.conn.exec(stmt);
        };
        self.stmt.shrinkRetainingCapacity(self.head_len);
        self.pending = 0;
    }

    pub fn close(self: *Sink) !void {
        defer self.teardown();
        try self.flush();
    }

    pub fn abort(self: *Sink) void {
        self.teardown();
    }

    fn teardown(self: *Sink) void {
        if (self.conn_alive) self.conn.close();
        self.stmt.deinit();
        self.gpa.free(self.tail);
        freeSchema(self.gpa, self.schema);
        self.gpa.free(self.table);
        self.gpa.destroy(self);
    }
};

const sink_vtable = driver.sinkVTable(Sink);

/// The segment-streaming sink shared by every bulk protocol. `Proto` supplies
/// `Connection`, `dialect`, `stmt`, `command`, `appendBatch`, `send` (returning the
/// server's row count) and optionally `segmentHeader` (tds COLMETADATA).
pub fn BulkSink(comptime Proto: type) type {
    return struct {
        gpa: std.mem.Allocator,
        conn: *Proto.Connection,
        schema: types.Schema,
        buffer: std.array_list.Managed(u8),
        command: []const u8,
        seg_rows: u64 = 0,
        errctx: ?*op.ErrCtx = null,
        redial: ?Redial = null,

        const Self = @This();
        const vtable = driver.sinkVTable(Self);

        pub fn open(gpa: std.mem.Allocator, conn: *Proto.Connection, table_name: []const u8, schema: types.Schema, mode: ast.WriteMode, redial: ?Redial) !*Self {
            const self = try gpa.create(Self);
            errdefer gpa.destroy(self);
            const owned = try ownedSchema(gpa, schema);
            errdefer freeSchema(gpa, owned);

            var aa = std.heap.ArenaAllocator.init(gpa);
            defer aa.deinit();
            const a = aa.allocator();
            const qtable = try quoteIdent(a, Proto.dialect, table_name);
            conn.exec(try createTableSql(a, Proto.dialect, qtable, schema, mode)) catch |e| return onWire(e);
            const command = try gpa.dupe(u8, try Proto.command(a, qtable, schema));
            errdefer gpa.free(command);

            self.* = .{ .gpa = gpa, .conn = conn, .schema = owned, .buffer = std.array_list.Managed(u8).init(gpa), .command = command, .redial = redial };
            errdefer self.buffer.deinit();
            try self.startSegment();
            return self;
        }

        pub fn sink(self: *Self) driver.Sink {
            return .{ .ptr = self, .vtable = &vtable };
        }

        pub fn writeBatch(self: *Self, arena: std.mem.Allocator, batch: Batch) !void {
            try Proto.appendBatch(self.buffer.writer(), arena, batch);
            self.seg_rows += batch.len;
            if (self.buffer.items.len >= SEGMENT_BYTES) {
                try self.commitSegment();
                try self.startSegment();
            }
        }

        fn startSegment(self: *Self) !void {
            if (@hasDecl(Proto, "segmentHeader")) try Proto.segmentHeader(self.buffer.writer(), self.gpa, self.schema);
        }

        /// Send the buffered segment and verify the row count, redialing once and
        /// resending on a transient failure. A segment holding only a header is dropped
        /// without a round-trip.
        fn commitSegment(self: *Self) !void {
            if (self.seg_rows == 0) {
                self.buffer.clearRetainingCapacity();
                return;
            }
            self.sendSegment() catch |e| {
                const rd = self.redial orelse return refused(self.errctx, Proto.dialect, self.conn.wire.last_error, e);
                if (!driver.transientNet(e)) return refused(self.errctx, Proto.dialect, self.conn.wire.last_error, e);
                const fresh = try rd.dial(rd.ctx, self.gpa);
                self.conn.close();
                self.conn = @ptrCast(@alignCast(fresh.ptr));
                try self.sendSegment();
            };
            self.buffer.clearRetainingCapacity();
            self.seg_rows = 0;
        }

        fn sendSegment(self: *Self) !void {
            const n = Proto.send(self.conn, self.command, self.buffer.items, self.seg_rows) catch |e| return onWire(e);
            if (n != self.seg_rows) {
                if (self.conn.wire.last_error.len == 0)
                    try self.conn.wire.printError("{s} count mismatch: sent {d} rows, server loaded {d}", .{ Proto.stmt, self.seg_rows, n });
                return error.BulkCountMismatch;
            }
        }

        pub fn close(self: *Self) !void {
            defer self.teardown();
            try self.commitSegment();
        }

        /// Drop the buffer and close the socket mid-statement, so the server aborts the
        /// in-flight statement and none of it is committed.
        pub fn abort(self: *Self) void {
            self.teardown();
        }

        fn teardown(self: *Self) void {
            self.conn.close();
            self.buffer.deinit();
            self.gpa.free(self.command);
            freeSchema(self.gpa, self.schema);
            self.gpa.destroy(self);
        }
    };
}

pub const TableOpts = struct {
    buckets: u32 = 4,
    replication_num: u32 = 1,
};

pub fn createTableSql(arena: std.mem.Allocator, dialect: Dialect, qtable: []const u8, schema: types.Schema, mode: ast.WriteMode) ![]const u8 {
    return createTableSqlWith(arena, dialect, qtable, schema, mode, .{});
}

pub fn createTableSqlWith(arena: std.mem.Allocator, dialect: Dialect, qtable: []const u8, schema: types.Schema, mode: ast.WriteMode, opts: TableOpts) ![]const u8 {
    if (dialect == .starrocks) return starrocksTableSql(arena, qtable, schema, mode, opts);
    if (dialect == .doris) return dorisTableSql(arena, qtable, schema, mode, opts);
    const keys: []const []const u8 = switch (mode) {
        .upsert => |u| u.keys,
        else => &.{},
    };
    var buf = std.array_list.Managed(u8).init(arena);
    const w = buf.writer();

    if (dialect == .sqlserver) {
        try w.writeAll("IF OBJECT_ID(");
        try dialect.writeStrLit(w, qtable, .catalog_name);
        try w.print(", N'U') IS NULL CREATE TABLE {s} (", .{qtable});
    } else {
        try w.print("CREATE TABLE IF NOT EXISTS {s} (", .{qtable});
    }

    for (schema.fields, 0..) |f, i| {
        if (i > 0) try w.writeByte(',');
        const is_key = nameIn(keys, f.name);
        const qn = try quoteName(arena, dialect, f.name);
        try w.print("{s} {s}", .{ qn, try dialect.ddlType(arena, f.ty, is_key) });
        if (is_key) {
            try w.writeAll(" NOT NULL");
        } else if (dialect == .sqlserver) {
            try w.writeAll(" NULL");
        }
    }
    if (keys.len > 0) try w.print(", PRIMARY KEY ({s})", .{try quoteNames(arena, dialect, keys)});
    try w.writeByte(')');
    return buf.toOwnedSlice();
}

/// StarRocks: `upsert on k` makes a PRIMARY KEY table (keys first and NOT NULL),
/// append/overwrite a DUPLICATE KEY on the first column; the same columns drive
/// DISTRIBUTED BY HASH.
fn starrocksTableSql(arena: std.mem.Allocator, qtable: []const u8, schema: types.Schema, mode: ast.WriteMode, opts: TableOpts) ![]const u8 {
    const is_pk = (mode == .upsert);
    const keys: []const []const u8 = switch (mode) {
        .upsert => |u| u.keys,
        else => &.{schema.fields[0].name},
    };
    if (is_pk and keys.len == 0) return error.UpsertKeysUnresolved;

    var ordered = std.array_list.Managed(types.Schema.Field).init(arena);
    if (is_pk) {
        for (keys) |k| try ordered.append(findField(schema, k) orelse return error.UnknownKeyColumn);
        for (schema.fields) |f| {
            if (!nameIn(keys, f.name)) try ordered.append(f);
        }
    } else {
        try ordered.appendSlice(schema.fields);
    }

    var buf = std.array_list.Managed(u8).init(arena);
    const w = buf.writer();
    try w.print("CREATE TABLE IF NOT EXISTS {s} (\n", .{qtable});
    for (ordered.items, 0..) |f, i| {
        const is_key = is_pk and nameIn(keys, f.name);
        const qn = try quoteName(arena, .starrocks, f.name);
        try w.print("  {s} {s}{s}", .{ qn, try Dialect.starrocks.ddlType(arena, f.ty, is_key), if (is_key) " NOT NULL" else "" });
        if (i + 1 < ordered.items.len) try w.writeByte(',');
        try w.writeByte('\n');
    }
    try w.writeAll(") ENGINE=OLAP\n");
    const qkeys = try quoteNames(arena, .starrocks, keys);
    try w.print("{s}({s})\n", .{ if (is_pk) "PRIMARY KEY" else "DUPLICATE KEY", qkeys });
    try w.print("DISTRIBUTED BY HASH({s}) BUCKETS {d}\n", .{ qkeys, opts.buckets });
    try w.print("PROPERTIES(\"replication_num\"=\"{d}\");", .{opts.replication_num});
    return buf.toOwnedSlice();
}

/// Doris: `upsert on k` makes a merge-on-write UNIQUE KEY table, keys first and
/// NOT NULL. Otherwise a duplicate table with no sort key, since Doris refuses a
/// FLOAT, DOUBLE or STRING key column.
fn dorisTableSql(arena: std.mem.Allocator, qtable: []const u8, schema: types.Schema, mode: ast.WriteMode, opts: TableOpts) ![]const u8 {
    const keys: []const []const u8 = switch (mode) {
        .upsert => |u| u.keys,
        else => &.{},
    };
    if (mode == .upsert and keys.len == 0) return error.UpsertKeysUnresolved;

    var ordered = std.array_list.Managed(types.Schema.Field).init(arena);
    for (keys) |k| try ordered.append(findField(schema, k) orelse return error.UnknownKeyColumn);
    for (schema.fields) |f| {
        if (!nameIn(keys, f.name)) try ordered.append(f);
    }

    var buf = std.array_list.Managed(u8).init(arena);
    const w = buf.writer();
    try w.print("CREATE TABLE IF NOT EXISTS {s} (\n", .{qtable});
    for (ordered.items, 0..) |f, i| {
        const is_key = nameIn(keys, f.name);
        const qn = try quoteName(arena, .doris, f.name);
        try w.print("  {s} {s}{s}", .{ qn, try Dialect.doris.ddlType(arena, f.ty, is_key), if (is_key) " NOT NULL" else "" });
        if (i + 1 < ordered.items.len) try w.writeByte(',');
        try w.writeByte('\n');
    }
    try w.writeAll(") ENGINE=OLAP\n");
    if (keys.len > 0) {
        const qkeys = try quoteNames(arena, .doris, keys);
        try w.print("UNIQUE KEY({s})\nDISTRIBUTED BY HASH({s}) BUCKETS {d}\n", .{ qkeys, qkeys, opts.buckets });
        try w.print("PROPERTIES(\"replication_num\"=\"{d}\", \"enable_unique_key_merge_on_write\"=\"true\");", .{opts.replication_num});
    } else {
        try w.print("DISTRIBUTED BY RANDOM BUCKETS {d}\n", .{opts.buckets});
        try w.print("PROPERTIES(\"replication_num\"=\"{d}\", \"enable_duplicate_without_keys_by_default\"=\"true\");", .{opts.replication_num});
    }
    return buf.toOwnedSlice();
}

fn findField(schema: types.Schema, name: []const u8) ?types.Schema.Field {
    for (schema.fields) |f| {
        if (std.mem.eql(u8, f.name, name)) return f;
    }
    return null;
}

/// What a statement holds before and after its comma-separated tuples.
const Parts = struct { head: []const u8, tail: []const u8 };

fn statementParts(arena: std.mem.Allocator, dialect: Dialect, qtable: []const u8, schema: types.Schema, mode: ast.WriteMode) !Parts {
    const cols = try colList(arena, dialect, schema);

    if (dialect == .sqlserver and mode == .upsert) {
        return mergeParts(arena, dialect, qtable, schema, mode.upsert.keys, cols);
    }

    const head = try std.fmt.allocPrint(arena, "INSERT INTO {s} ({s}) VALUES ", .{ qtable, cols });
    var buf = std.array_list.Managed(u8).init(arena);
    const w = buf.writer();

    switch (mode) {
        .upsert => |u| switch (dialect) {
            .postgres => {
                try w.print(" ON CONFLICT ({s}) DO UPDATE SET ", .{try quoteNames(arena, dialect, u.keys)});
                try writeUpdateSet(w, arena, dialect, schema, u.keys, "EXCLUDED.");
            },
            .mysql => {
                try w.writeAll(" ON DUPLICATE KEY UPDATE ");
                try writeMysqlUpdate(w, arena, dialect, schema, u.keys);
            },
            .starrocks, .doris => {},
            .sqlserver => unreachable,
        },
        else => {},
    }
    return .{ .head = head, .tail = try buf.toOwnedSlice() };
}

fn mergeParts(arena: std.mem.Allocator, dialect: Dialect, qtable: []const u8, schema: types.Schema, keys: []const []const u8, cols: []const u8) !Parts {
    const head = try std.fmt.allocPrint(arena, "MERGE INTO {s} AS T USING (VALUES ", .{qtable});
    var buf = std.array_list.Managed(u8).init(arena);
    const w = buf.writer();
    try w.print(") AS S ({s}) ON ", .{cols});
    for (keys, 0..) |k, i| {
        if (i > 0) try w.writeAll(" AND ");
        const qk = try quoteName(arena, dialect, k);
        try w.print("T.{s}=S.{s}", .{ qk, qk });
    }
    try w.writeAll(" WHEN MATCHED THEN UPDATE SET ");
    try writeUpdateSet(w, arena, dialect, schema, keys, "S.");
    try w.print(" WHEN NOT MATCHED THEN INSERT ({s}) VALUES (", .{cols});
    for (schema.fields, 0..) |f, i| {
        if (i > 0) try w.writeByte(',');
        try w.print("S.{s}", .{try quoteName(arena, dialect, f.name)});
    }
    try w.writeAll(");");
    return .{ .head = head, .tail = try buf.toOwnedSlice() };
}

fn writeUpdateSet(w: anytype, arena: std.mem.Allocator, dialect: Dialect, schema: types.Schema, keys: []const []const u8, src_prefix: []const u8) !void {
    var first = true;
    for (schema.fields) |f| {
        if (nameIn(keys, f.name)) continue;
        if (!first) try w.writeByte(',');
        first = false;
        const qn = try quoteName(arena, dialect, f.name);
        try w.print("{s}={s}{s}", .{ qn, src_prefix, qn });
    }
}

fn writeMysqlUpdate(w: anytype, arena: std.mem.Allocator, dialect: Dialect, schema: types.Schema, keys: []const []const u8) !void {
    var first = true;
    for (schema.fields) |f| {
        if (nameIn(keys, f.name)) continue;
        if (!first) try w.writeByte(',');
        first = false;
        const qn = try quoteName(arena, dialect, f.name);
        try w.print("{s}=VALUES({s})", .{ qn, qn });
    }
}

pub fn colList(arena: std.mem.Allocator, dialect: Dialect, schema: types.Schema) ![]const u8 {
    var buf = std.array_list.Managed(u8).init(arena);
    for (schema.fields, 0..) |f, i| {
        if (i > 0) try buf.append(',');
        try writeQuotedName(&buf, dialect, f.name);
    }
    return buf.toOwnedSlice();
}

fn quoteNames(arena: std.mem.Allocator, dialect: Dialect, names: []const []const u8) ![]const u8 {
    var buf = std.array_list.Managed(u8).init(arena);
    for (names, 0..) |n, i| {
        if (i > 0) try buf.append(',');
        try writeQuotedName(&buf, dialect, n);
    }
    return buf.toOwnedSlice();
}

/// Row `row` of `batch` as a `(...)` tuple of literals, appended to `buf`.
fn writeRow(comptime dialect: Dialect, buf: *std.array_list.Managed(u8), batch: Batch, row: usize) !void {
    try buf.append('(');
    for (batch.columns, 0..) |*c, i| {
        if (i > 0) try buf.append(',');
        try celltext.write(buf.writer(), c, row, Literal(dialect){});
    }
    try buf.append(')');
}

fn serializeRow(gpa: std.mem.Allocator, dialect: Dialect, batch: Batch, row: usize) ![]const u8 {
    var buf = std.array_list.Managed(u8).init(gpa);
    switch (dialect) {
        inline else => |d| try writeRow(d, &buf, batch, row),
    }
    return buf.toOwnedSlice();
}

/// The `celltext.write` policy for a cell as a literal in an INSERT's VALUES.
fn Literal(comptime dialect: Dialect) type {
    return struct {
        pub const null_text = "NULL";
        pub const bool_true = dialect.boolLit(true);
        pub const bool_false = dialect.boolLit(false);
        pub const quote_temporal = "'";

        pub fn string(_: @This(), w: anytype, s: []const u8) !void {
            return dialect.writeStrLit(w, s, .data);
        }

        pub fn bytes(_: @This(), w: anytype, s: []const u8) !void {
            return hexLiteral(w, dialect, s);
        }

        pub fn float(_: @This(), w: anytype, x: f64) !void {
            return floatLiteral(w, dialect, x);
        }
    };
}

/// A float as a literal the target keeps: `-0` as a float, not the integer 0, and
/// NaN and the infinities as PostgreSQL spells them; elsewhere they have no value.
fn floatLiteral(w: anytype, dialect: Dialect, x: f64) !void {
    if (std.math.isNan(x) or std.math.isInf(x)) {
        if (dialect != .postgres) return error.NonFiniteFloat;
        const s = if (std.math.isNan(x)) "NaN" else if (x > 0) "Infinity" else "-Infinity";
        return w.print("'{s}'::float8", .{s});
    }
    if (x == 0 and std.math.signbit(x)) {
        return w.writeAll(if (dialect == .postgres) "'-0'::float8" else "-0e0");
    }
    try w.print("{d}", .{x});
}

test "floatLiteral keeps -0, NaN and the infinities on PostgreSQL" {
    var buf: [64]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try floatLiteral(&w, .postgres, std.math.nan(f64));
    try floatLiteral(&w, .postgres, -std.math.inf(f64));
    try floatLiteral(&w, .postgres, -0.0);
    try floatLiteral(&w, .postgres, 1.5);
    try std.testing.expectEqualStrings("'NaN'::float8'-Infinity'::float8'-0'::float81.5", w.buffered());
    try std.testing.expectError(error.NonFiniteFloat, floatLiteral(&w, .sqlserver, std.math.inf(f64)));
}

fn hexLiteral(w: anytype, dialect: Dialect, s: []const u8) !void {
    if (dialect == .postgres) {
        try w.writeAll("'\\x");
        try writeHex(w, s);
        try w.writeByte('\'');
    } else {
        try w.writeAll("0x");
        try writeHex(w, s);
    }
}

/// `s` as lowercase hex digits, through a stack buffer.
fn writeHex(w: anytype, s: []const u8) !void {
    var buf: [256]u8 = undefined;
    var i: usize = 0;
    while (i < s.len) {
        const n = @min(s.len - i, buf.len / 2);
        try w.writeAll(hexfmt.encode(&buf, s[i..][0..n], .lower));
        i += n;
    }
}

/// How a bulk text format spells a BYTES cell: escaped raw bytes (LOAD DATA into
/// a BLOB), or PostgreSQL's bytea hex `\x...` with its backslash escaped (COPY).
pub const BulkBytes = enum { escaped, pg_hex };

pub const BulkFormat = struct {
    bool_true: []const u8 = "t",
    bool_false: []const u8 = "f",
    bytes: BulkBytes = .escaped,
};

/// The `celltext.write` policy for COPY and LOAD DATA text: `\N` for null, and a
/// string's backslash, tab, newline and carriage return escaped.
pub fn BulkText(comptime fmt: BulkFormat) type {
    return struct {
        pub const null_text = "\\N";
        pub const bool_true = fmt.bool_true;
        pub const bool_false = fmt.bool_false;

        pub fn string(_: @This(), w: anytype, s: []const u8) !void {
            return bulkEscaped(w, s);
        }

        pub fn bytes(_: @This(), w: anytype, s: []const u8) !void {
            switch (fmt.bytes) {
                .escaped => return bulkEscaped(w, s),
                .pg_hex => {
                    try w.writeAll("\\\\x");
                    return writeHex(w, s);
                },
            }
        }
    };
}

pub fn appendBulkText(w: anytype, batch: Batch, comptime fmt: BulkFormat) !void {
    for (0..batch.len) |r| {
        for (batch.columns, 0..) |*col, i| {
            if (i > 0) try w.writeByte('\t');
            try celltext.write(w, col, r, BulkText(fmt){});
        }
        try w.writeByte('\n');
    }
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
    try writeQuoted(&buf, dialect, name);
    return buf.toOwnedSlice();
}

/// One identifier, dots and all, as a single quoted name: a column, or one part of
/// a qualified table name.
pub fn quoteName(arena: std.mem.Allocator, dialect: Dialect, name: []const u8) ![]const u8 {
    var buf = std.array_list.Managed(u8).init(arena);
    try writeQuotedName(&buf, dialect, name);
    return buf.toOwnedSlice();
}

/// A qualified name, each part quoted as `quoteName` quotes it.
pub fn quoteParts(arena: std.mem.Allocator, dialect: Dialect, parts: []const []const u8) ![]const u8 {
    var buf = std.array_list.Managed(u8).init(arena);
    for (parts, 0..) |p, i| {
        if (i > 0) try buf.append('.');
        try writeQuotedName(&buf, dialect, p);
    }
    return buf.toOwnedSlice();
}

/// A table named in a catalog lookup as a literal of its quoted parts, the way a
/// read names it: `'"s"."T"'::regclass` keeps the case PostgreSQL would fold.
pub fn objectLit(arena: std.mem.Allocator, dialect: Dialect, parts: []const []const u8) ![]const u8 {
    return dialect.strLit(arena, try quoteParts(arena, dialect, parts));
}

/// The schema of a qualified name as a literal for an information_schema lookup,
/// or the session's database when the name has no schema part.
pub fn schemaLit(arena: std.mem.Allocator, dialect: Dialect, parts: []const []const u8) ![]const u8 {
    if (parts.len < 2) return "DATABASE()";
    return dialect.strLit(arena, parts[parts.len - 2]);
}

/// `quoteIdent` straight into a list, so a sink on a general-purpose allocator does
/// not leak one allocation per column (a 306-column load once leaked thousands).
fn writeQuoted(buf: *std.array_list.Managed(u8), dialect: Dialect, name: []const u8) !void {
    var it = std.mem.splitScalar(u8, name, '.');
    var first = true;
    while (it.next()) |part| {
        if (!first) try buf.append('.');
        first = false;
        try writeQuotedName(buf, dialect, part);
    }
}

/// The dialect's quotes around `name`, a closing quote inside it doubled.
fn writeQuotedName(buf: *std.array_list.Managed(u8), dialect: Dialect, name: []const u8) !void {
    const close = dialect.qClose();
    try buf.append(dialect.qOpen());
    for (name) |c| {
        if (c == close) try buf.append(close);
        try buf.append(c);
    }
    try buf.append(close);
}

const nameIn = ast.nameIn;

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

    const stmt = try testStatement(a, .postgres, qt, schema, .{ .upsert = .{ .keys = &.{"id"} } }, "(1,'a'),(2,'b')");
    try std.testing.expect(std.mem.indexOf(u8, stmt, "INSERT INTO \"people\" (\"id\",\"name\") VALUES (1,'a'),(2,'b')") != null);
    try std.testing.expect(std.mem.indexOf(u8, stmt, "ON CONFLICT (\"id\") DO UPDATE SET \"name\"=EXCLUDED.\"name\"") != null);
}

test "create table (sqlserver): non-key columns are explicit NULL, keys NOT NULL" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const schema = types.Schema{ .fields = &.{
        .{ .name = "id", .ty = types.Type.init(.int) },
        .{ .name = "name", .ty = types.Type.init(.string) },
    } };
    const qt = try quoteIdent(a, .sqlserver, "people");
    const ddl = try createTableSql(a, .sqlserver, qt, schema, .{ .upsert = .{ .keys = &.{"id"} } });
    try std.testing.expect(std.mem.indexOf(u8, ddl, "[id] BIGINT NOT NULL") != null);
    try std.testing.expect(std.mem.indexOf(u8, ddl, "[name] NVARCHAR(4000) NULL") != null);
}

test "upsert SQL (mysql): ON DUPLICATE KEY UPDATE with VALUES()" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const schema = types.Schema{ .fields = &.{
        .{ .name = "id", .ty = types.Type.init(.int) },
        .{ .name = "name", .ty = types.Type.init(.string) },
    } };
    const qt = try quoteIdent(a, .mysql, "people");
    const stmt = try testStatement(a, .mysql, qt, schema, .{ .upsert = .{ .keys = &.{"id"} } }, "(1,'a')");
    try std.testing.expect(std.mem.indexOf(u8, stmt, "INSERT INTO `people` (`id`,`name`) VALUES (1,'a')") != null);
    try std.testing.expect(std.mem.indexOf(u8, stmt, "ON DUPLICATE KEY UPDATE `name`=VALUES(`name`)") != null);
    try std.testing.expect(std.mem.indexOf(u8, stmt, "`id`=VALUES") == null);
}

test "upsert SQL (sqlserver): MERGE keyed on T/S join, keys excluded from UPDATE" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const schema = types.Schema{ .fields = &.{
        .{ .name = "id", .ty = types.Type.init(.int) },
        .{ .name = "name", .ty = types.Type.init(.string) },
    } };
    const qt = try quoteIdent(a, .sqlserver, "dbo.people");
    const stmt = try testStatement(a, .sqlserver, qt, schema, .{ .upsert = .{ .keys = &.{"id"} } }, "(1,'a')");
    try std.testing.expect(std.mem.indexOf(u8, stmt, "MERGE INTO [dbo].[people] AS T USING (VALUES (1,'a')) AS S ([id],[name]) ON T.[id]=S.[id]") != null);
    try std.testing.expect(std.mem.indexOf(u8, stmt, "WHEN MATCHED THEN UPDATE SET [name]=S.[name]") != null);
    try std.testing.expect(std.mem.indexOf(u8, stmt, "WHEN NOT MATCHED THEN INSERT ([id],[name]) VALUES (S.[id],S.[name])") != null);
    try std.testing.expect(std.mem.indexOf(u8, stmt, "[id]=S.[id],") == null);
}

test "quoteIdent quotes each dotted part per dialect" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings("\"public\".\"t\"", try quoteIdent(a, .postgres, "public.t"));
    try std.testing.expectEqualStrings("`db`.`t`", try quoteIdent(a, .mysql, "db.t"));
    try std.testing.expectEqualStrings("[dbo].[t]", try quoteIdent(a, .sqlserver, "dbo.t"));
    try std.testing.expectEqualStrings("`t`", try quoteIdent(a, .mysql, "t"));
}

test "quoteIdent and quoteName double the closing quote; quoteName keeps dots" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings("\"a\"\"b\".\"t\"", try quoteIdent(a, .postgres, "a\"b.t"));
    try std.testing.expectEqualStrings("`a``b`.`t`", try quoteIdent(a, .mysql, "a`b.t"));
    try std.testing.expectEqualStrings("`a``b`", try quoteIdent(a, .starrocks, "a`b"));
    try std.testing.expectEqualStrings("`a``b`", try quoteIdent(a, .doris, "a`b"));
    try std.testing.expectEqualStrings("[a]]b].[t]", try quoteIdent(a, .sqlserver, "a]b.t"));
    try std.testing.expectEqualStrings("[a[b]", try quoteIdent(a, .sqlserver, "a[b"));
    try std.testing.expectEqualStrings("\"a.b\"", try quoteName(a, .postgres, "a.b"));
    try std.testing.expectEqualStrings("[a.]]b]", try quoteName(a, .sqlserver, "a.]b"));
}

test "create table and insert quote odd column names whole" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const schema = types.Schema{ .fields = &.{
        .{ .name = "i]d", .ty = types.Type.init(.int) },
        .{ .name = "x.y", .ty = types.Type.init(.string) },
    } };
    const qt = try quoteIdent(a, .sqlserver, "o'brien");
    const ddl = try createTableSql(a, .sqlserver, qt, schema, .{ .upsert = .{ .keys = &.{"i]d"} } });
    try std.testing.expect(std.mem.startsWith(u8, ddl, "IF OBJECT_ID(N'[o''brien]', N'U') IS NULL CREATE TABLE [o'brien] ("));
    try std.testing.expect(std.mem.indexOf(u8, ddl, "[i]]d] BIGINT NOT NULL,[x.y] NVARCHAR(4000) NULL, PRIMARY KEY ([i]]d])") != null);
    const stmt = try testStatement(a, .postgres, try quoteIdent(a, .postgres, "t"), schema, .append, "(1,'a')");
    try std.testing.expectEqualStrings("INSERT INTO \"t\" (\"i]d\",\"x.y\") VALUES (1,'a')", stmt);
}

test "the INSERT sink appends tuples into one statement, flushed every flush_rows and at close" {
    const gpa = std.testing.allocator;
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    const Fake = struct {
        a: std.mem.Allocator,
        seen: std.ArrayListUnmanaged([]const u8) = .empty,
        fn queryCursor(_: *anyopaque, _: []const u8) anyerror!Cursor {
            return error.Unsupported;
        }
        fn exec(p: *anyopaque, s: []const u8) anyerror!void {
            const f: *@This() = @ptrCast(@alignCast(p));
            try f.seen.append(f.a, try f.a.dupe(u8, s));
        }
        fn close(_: *anyopaque) void {}
        const vt = Conn.VTable{ .queryCursor = queryCursor, .exec = exec, .close = close };
    };
    var fake = Fake{ .a = a };
    const schema = types.Schema{ .fields = &.{
        .{ .name = "id", .ty = types.Type.init(.int).asNullable() },
        .{ .name = "name", .ty = types.Type.init(.string).asNullable() },
    } };
    var ids = [_]?i64{ 1, 2, null };
    var nb = column.Builder.init(a, schema.fields[1].ty);
    for ([_]Value{ .{ .string = "a" }, .null, .{ .string = "c'd" } }) |v| try nb.append(v);
    var cols = [_]column.Column{ try column.intColumn(a, &ids), try nb.finish() };
    const batch = Batch{ .schema = &schema, .columns = &cols, .len = 3 };

    const s = try Sink.open(gpa, .{ .ptr = &fake, .vtable = &Fake.vt }, .postgres, "t", schema, .{ .upsert = .{ .keys = &.{"id"} } }, null);
    s.flush_rows = 4;
    try s.writeBatch(a, batch);
    try s.writeBatch(a, batch);
    try s.close();
    const tail = " ON CONFLICT (\"id\") DO UPDATE SET \"name\"=EXCLUDED.\"name\"";
    try std.testing.expectEqual(@as(usize, 3), fake.seen.items.len);
    try std.testing.expectEqualStrings("INSERT INTO \"t\" (\"id\",\"name\") VALUES (1,'a'),(2,NULL),(NULL,'c''d'),(1,'a')" ++ tail, fake.seen.items[1]);
    try std.testing.expectEqualStrings("INSERT INTO \"t\" (\"id\",\"name\") VALUES (2,NULL),(NULL,'c''d')" ++ tail, fake.seen.items[2]);
}

fn testStatement(a: std.mem.Allocator, dialect: Dialect, qtable: []const u8, schema: types.Schema, mode: ast.WriteMode, values: []const u8) ![]const u8 {
    const p = try statementParts(a, dialect, qtable, schema, mode);
    return std.mem.concat(a, u8, &.{ p.head, values, p.tail });
}

fn literalOf(a: std.mem.Allocator, dialect: Dialect, kind: types.TypeKind, v: Value) ![]const u8 {
    const ty = types.Type.init(kind).asNullable();
    var b = column.Builder.init(a, ty);
    try b.append(v);
    const cols = try a.alloc(column.Column, 1);
    cols[0] = try b.finish();
    var schema = types.Schema{ .fields = &.{.{ .name = "c", .ty = ty }} };
    const row = try serializeRow(a, dialect, .{ .schema = &schema, .columns = cols, .len = 1 }, 0);
    return row[1 .. row.len - 1];
}

test "INSERT literals: dialect-specific escaping, bools, bytes, null" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings("'c:\\\\tmp'", try literalOf(a, .mysql, .string, .{ .string = "c:\\tmp" }));
    try std.testing.expectEqualStrings("'c:\\tmp'", try literalOf(a, .postgres, .string, .{ .string = "c:\\tmp" }));
    try std.testing.expectEqualStrings("'c:\\\\tmp'", try literalOf(a, .starrocks, .string, .{ .string = "c:\\tmp" }));
    try std.testing.expectEqualStrings("'c:\\\\tmp'", try literalOf(a, .doris, .string, .{ .string = "c:\\tmp" }));
    try std.testing.expectEqualStrings("1", try literalOf(a, .sqlserver, .bool, .{ .bool = true }));
    try std.testing.expectEqualStrings("FALSE", try literalOf(a, .postgres, .bool, .{ .bool = false }));
    try std.testing.expectEqualStrings("'\\x01ab'", try literalOf(a, .postgres, .bytes, .{ .bytes = "\x01\xab" }));
    try std.testing.expectEqualStrings("0x01ab", try literalOf(a, .mysql, .bytes, .{ .bytes = "\x01\xab" }));
    try std.testing.expectEqualStrings("NULL", try literalOf(a, .postgres, .int, .null));
    try std.testing.expectEqualStrings("'O''Brien'", try literalOf(a, .postgres, .string, .{ .string = "O'Brien" }));
    try std.testing.expectEqualStrings("'1970-01-01'", try literalOf(a, .postgres, .date, .{ .date = 0 }));
}

fn goldenLiterals(a: std.mem.Allocator, d: Dialect) ![]const []const u8 {
    const batches = try celltext.goldenBatches(a);
    const out = try a.alloc([]const u8, batches.len);
    for (batches, out) |batch, *o| {
        var w = std.Io.Writer.Allocating.init(a);
        for (0..batch.len) |r| {
            if (r > 0) try w.writer.writeByte(' ');
            if (serializeRow(a, d, batch, r)) |row| try w.writer.writeAll(row) else |e| try w.writer.writeAll(@errorName(e));
        }
        o.* = w.written();
    }
    return out;
}

test "INSERT literals of every kind, in every dialect" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const tiny = celltext.golden_tiny;
    const words = "(TRUE) (FALSE) (NULL)";
    const ints = "(0) (-1) (-9223372036854775808) (9223372036854775807) (NULL)";
    const floats = "(0.1) (0) (-0e0) (" ++ tiny ++ ") (1000000000000000000000) (-2.25) NonFiniteFloat NonFiniteFloat NonFiniteFloat (NULL)";
    const decimals = "(123.45) (-0.0007) (5) (0.12345) (-18446744073709.551615) (0.00) (NULL)";
    const strings = "('') ('plain') ('tab\there') ('line\nbreak') ('cr\rlf\r\n') ('say \"hi\"') ('O''Brien') ('back\\slash\\') ('a\\Nb') ('\\N ') ('NULL') ('a\x01b\x02c') ('a,b;c:d.e-1 0') ('\xc3\xb3 \xf0\x9f\x98\x80') ('\x00nul') ('sixteen bytes and more\x01') (NULL)";
    const hex = "(0x) (0x706c61696e) (0x0102205c20090a22) (0xc3b3) (NULL)";
    const dates = "('1970-01-01') ('2024-01-01') ('0000-01-01') ('-5877641-06-24') (NULL)";
    const times = "('00:00:00') ('01:02:03.000001') ('23:59:59.999999') (NULL)";
    const stamps = "('1970-01-01 00:00:00') ('2024-01-01 00:00:00.123456') ('1969-12-31 23:59:59.999999') ('0001-01-01 00:00:00') (NULL)";
    const nested = "(NULL) (NULL)";
    const rows = "(1,'a b',2.50) (NULL,NULL,NULL) (-2,'',-0.1)";
    const n_strings = "(N'') (N'plain') (N'tab\there') (N'line\nbreak') (N'cr\rlf\r\n') (N'say \"hi\"') (N'O''Brien') (N'back\\slash\\') (N'a\\Nb') (N'\\N ') (N'NULL') (N'a\x01b\x02c') (N'a,b;c:d.e-1 0') (N'\xc3\xb3 \xf0\x9f\x98\x80') (N'\x00nul') (N'sixteen bytes and more\x01') (NULL)";
    const wire_strings = "('') ('plain') ('tab\there') ('line\nbreak') ('cr\rlf\r\n') ('say \"hi\"') ('O''Brien') ('back\\\\slash\\\\') ('a\\\\Nb') ('\\\\N ') ('NULL') ('a\x01b\x02c') ('a,b;c:d.e-1 0') ('\xc3\xb3 \xf0\x9f\x98\x80') ('\x00nul') ('sixteen bytes and more\x01') (NULL)";
    const want = [_]struct { Dialect, [11][]const u8 }{
        .{ .postgres, .{
            words,
            ints,
            "(0.1) (0) ('-0'::float8) (" ++ tiny ++ ") (1000000000000000000000) (-2.25) ('NaN'::float8) ('Infinity'::float8) ('-Infinity'::float8) (NULL)",
            decimals,
            strings,
            "('\\x') ('\\x706c61696e') ('\\x0102205c20090a22') ('\\xc3b3') (NULL)",
            dates,
            times,
            stamps,
            nested,
            rows,
        } },
        .{ .mysql, .{
            words,
            ints,
            floats,
            decimals,
            wire_strings,
            hex,
            dates,
            times,
            stamps,
            nested,
            rows,
        } },
        .{ .sqlserver, .{ "(1) (0) (NULL)", ints, floats, decimals, n_strings, hex, dates, times, stamps, nested, "(1,N'a b',2.50) (NULL,NULL,NULL) (-2,N'',-0.1)" } },
        .{ .starrocks, .{ words, ints, floats, decimals, wire_strings, hex, dates, times, stamps, nested, rows } },
        .{ .doris, .{ words, ints, floats, decimals, wire_strings, hex, dates, times, stamps, nested, rows } },
    };
    try std.testing.expectEqual(std.enums.values(Dialect).len, want.len);
    for (want) |x| try celltext.expectGolden(&x[1], try goldenLiterals(a, x[0]));
}

fn goldenBulkText(a: std.mem.Allocator, comptime fmt: BulkFormat) ![]const []const u8 {
    const batches = try celltext.goldenBatches(a);
    const out = try a.alloc([]const u8, batches.len);
    for (batches, out) |batch, *o| {
        var w = std.Io.Writer.Allocating.init(a);
        try appendBulkText(&w.writer, batch, fmt);
        o.* = w.written();
    }
    return out;
}

test "COPY and LOAD DATA text of every kind" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var want = [_][]const u8{
        "t\nf\n\\N\n",
        "0\n-1\n-9223372036854775808\n9223372036854775807\n\\N\n",
        "0.1\n0\n-0\n" ++ celltext.golden_tiny ++ "\n1000000000000000000000\n-2.25\nnan\ninf\n-inf\n\\N\n",
        "123.45\n-0.0007\n5\n0.12345\n-18446744073709.551615\n0.00\n\\N\n",
        "\nplain\ntab\\there\nline\\nbreak\ncr\\rlf\\r\\n\nsay \"hi\"\nO'Brien\nback\\\\slash\\\\\na\\\\Nb\n\\\\N \nNULL\na\x01b\x02c\na,b;c:d.e-1 0\n\xc3\xb3 \xf0\x9f\x98\x80\n\x00nul\nsixteen bytes and more\x01\n\\N\n",
        "\nplain\n\x01\x02 \\\\ \\t\\n\"\n\xc3\xb3\n\\N\n",
        "1970-01-01\n2024-01-01\n0000-01-01\n-5877641-06-24\n\\N\n",
        "00:00:00\n01:02:03.000001\n23:59:59.999999\n\\N\n",
        "1970-01-01 00:00:00\n2024-01-01 00:00:00.123456\n1969-12-31 23:59:59.999999\n0001-01-01 00:00:00\n\\N\n",
        "\\N\n\\N\n",
        "1\ta b\t2.50\n\\N\t\\N\t\\N\n-2\t\t-0.1\n",
    };
    try celltext.expectGolden(&want, try goldenBulkText(a, .{}));
    want[5] = "\\\\x\n\\\\x706c61696e\n\\\\x0102205c20090a22\n\\\\xc3b3\n\\N\n";
    try celltext.expectGolden(&want, try goldenBulkText(a, .{ .bytes = .pg_hex }));
    want[0] = "1\n0\n\\N\n";
    want[5] = "\nplain\n\x01\x02 \\\\ \\t\\n\"\n\xc3\xb3\n\\N\n";
    try celltext.expectGolden(&want, try goldenBulkText(a, .{ .bool_true = "1", .bool_false = "0" }));
}

test "bulk text format: tab/newline/backslash escaping and \\N nulls" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const str_ty = types.Type.init(.string).asNullable();
    const bool_ty = types.Type.init(.bool).asNullable();
    var b0 = column.Builder.init(a, str_ty);
    try b0.append(.{ .string = "a\tb\nc\\d" });
    try b0.append(.null);
    var b1 = column.Builder.init(a, bool_ty);
    try b1.append(.{ .bool = true });
    try b1.append(.{ .bool = false });
    const cols = try a.alloc(column.Column, 2);
    cols[0] = try b0.finish();
    cols[1] = try b1.finish();
    var schema = types.Schema{ .fields = &.{
        .{ .name = "s", .ty = str_ty },
        .{ .name = "b", .ty = bool_ty },
    } };
    const batch = Batch{ .schema = &schema, .columns = cols, .len = 2 };

    var out = std.array_list.Managed(u8).init(a);
    try appendBulkText(out.writer(), batch, .{});
    try std.testing.expectEqualStrings("a\\tb\\nc\\\\d\tt\n\\N\tf\n", out.items);

    out.clearRetainingCapacity();
    try appendBulkText(out.writer(), batch, .{ .bool_true = "1", .bool_false = "0" });
    try std.testing.expectEqualStrings("a\\tb\\nc\\\\d\t1\n\\N\t0\n", out.items);
}

const FakeBulk = struct {
    const Connection = struct {
        wire: Wire = .{},
        fail_once: bool = false,
        sends: usize = 0,
        closed: bool = false,
        last_len: usize = 0,
        fn exec(_: *Connection, _: []const u8) !void {}
        fn close(self: *Connection) void {
            self.closed = true;
        }
    };
    const Wire = struct {
        last_error: []const u8 = "",
        fn printError(_: *Wire, comptime _: []const u8, _: anytype) !void {}
    };
    const dialect: Dialect = .postgres;
    const stmt = "FAKE";
    fn command(_: std.mem.Allocator, _: []const u8, _: types.Schema) ![]const u8 {
        return "CMD";
    }
    fn appendBatch(w: anytype, _: std.mem.Allocator, batch: Batch) !void {
        for (0..batch.len) |_| try w.writeAll("row\n");
    }
    fn send(conn: *Connection, _: []const u8, segment: []const u8, rows: u64) !u64 {
        conn.sends += 1;
        if (conn.fail_once) {
            conn.fail_once = false;
            return error.ConnectionResetByPeer;
        }
        conn.last_len = segment.len;
        return rows;
    }
    fn dial(ctx: *const anyopaque, _: std.mem.Allocator) anyerror!Conn {
        const fresh: *Connection = @ptrCast(@alignCast(@constCast(ctx)));
        return .{ .ptr = fresh, .vtable = &conn_vtable };
    }
    fn noCursor(_: *anyopaque, _: []const u8) anyerror!Cursor {
        return error.Unsupported;
    }
    fn noExec(_: *anyopaque, _: []const u8) anyerror!void {}
    fn noClose(_: *anyopaque) void {}
    const conn_vtable = Conn.VTable{ .queryCursor = noCursor, .exec = noExec, .close = noClose };
};

test "BulkSink: a transient send failure redials once and resends the intact segment" {
    const gpa = std.testing.allocator;
    var first = FakeBulk.Connection{ .fail_once = true };
    var second = FakeBulk.Connection{};
    const schema = types.Schema{ .fields = &.{} };
    const batch = Batch{ .schema = &schema, .columns = &.{}, .len = 3 };

    const s = try BulkSink(FakeBulk).open(gpa, &first, "t", schema, .append, .{ .ctx = &second, .dial = FakeBulk.dial });
    try s.writeBatch(gpa, batch);
    try s.close();

    try std.testing.expectEqual(@as(usize, 1), first.sends);
    try std.testing.expect(first.closed);
    try std.testing.expectEqual(@as(usize, 1), second.sends);
    try std.testing.expectEqual(@as(usize, 12), second.last_len);
    try std.testing.expect(second.closed);
}

test "BulkSink: without a redial the transient failure is the caller's" {
    const gpa = std.testing.allocator;
    var only = FakeBulk.Connection{ .fail_once = true };
    const schema = types.Schema{ .fields = &.{} };
    const batch = Batch{ .schema = &schema, .columns = &.{}, .len = 1 };

    const s = try BulkSink(FakeBulk).open(gpa, &only, "t", schema, .append, null);
    try s.writeBatch(gpa, batch);
    try std.testing.expectError(error.ConnectionResetByPeer, s.close());
    try std.testing.expectEqual(@as(usize, 1), only.sends);
    try std.testing.expect(only.closed);
}

test "DDL and CAST spell each kind the way each dialect declares it" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const dec = types.Type{ .kind = .decimal, .precision = 10, .scale = 2 };
    const ts = types.Type.init(.timestamp);
    const str = types.Type.init(.string);
    const Case = struct { d: Dialect, ts: []const u8, str: []const u8, str_key: []const u8, str_cast: []const u8 };
    const cases = [_]Case{
        .{ .d = .postgres, .ts = "TIMESTAMP", .str = "TEXT", .str_key = "VARCHAR(255)", .str_cast = "TEXT" },
        .{ .d = .mysql, .ts = "DATETIME", .str = "VARCHAR(255)", .str_key = "VARCHAR(255)", .str_cast = "CHAR" },
        .{ .d = .sqlserver, .ts = "DATETIME2", .str = "NVARCHAR(4000)", .str_key = "NVARCHAR(255)", .str_cast = "VARCHAR(MAX)" },
        .{ .d = .starrocks, .ts = "DATETIME", .str = "VARCHAR(65533)", .str_key = "VARCHAR(65533)", .str_cast = "CHAR" },
        .{ .d = .doris, .ts = "DATETIME(6)", .str = "STRING", .str_key = "VARCHAR(65533)", .str_cast = "CHAR" },
    };
    for (cases) |c| {
        try std.testing.expectEqualStrings("DECIMAL(10,2)", try c.d.ddlType(a, dec, false));
        try std.testing.expectEqualStrings("DECIMAL(10,2)", (try c.d.castType(a, dec)).?);
        try std.testing.expectEqualStrings(c.ts, try c.d.ddlType(a, ts, false));
        try std.testing.expectEqualStrings(c.str, try c.d.ddlType(a, str, false));
        try std.testing.expectEqualStrings(c.str_key, try c.d.ddlType(a, str, true));
        try std.testing.expectEqualStrings(c.str_cast, (try c.d.castType(a, str)).?);
        try std.testing.expect((try c.d.castType(a, types.Type.init(.bool))) == null);
        try std.testing.expect((try c.d.castType(a, types.Type.init(.bytes))) == null);
    }
    try std.testing.expectEqualStrings("BIT", try Dialect.sqlserver.ddlType(a, types.Type.init(.bool), false));
    try std.testing.expectEqualStrings("TINYINT(1)", try Dialect.mysql.ddlType(a, types.Type.init(.bool), false));
    try std.testing.expectEqualStrings("BIGINT", try Dialect.mysql.ddlType(a, types.Type.init(.int), false));
    try std.testing.expectEqualStrings("SIGNED", (try Dialect.mysql.castType(a, types.Type.init(.int))).?);
}

test "a string literal keeps a name from data inside it in every dialect" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings("'x''); DROP TABLE t;--'", try Dialect.sqlserver.strLit(a, "x'); DROP TABLE t;--"));
    try std.testing.expectEqualStrings("'a\\b'", try Dialect.postgres.strLit(a, "a\\b"));
    try std.testing.expectEqualStrings("'a\\\\'' OR 1=1 -- '", try Dialect.mysql.strLit(a, "a\\' OR 1=1 -- "));
}

test "the literal writers equal their pre-writeStrLit bodies, but for SQL Server's N on a value" {
    const Orig = struct {
        /// The literal writers at 10e697a, before `writeStrLit`: `quoteString`, `strLit`,
        /// the OBJECT_ID literal of `createTableSqlWith`, and `hexLiteral`.
        const Pinned = struct {
            fn quoteString(w: anytype, dialect: Dialect, s: []const u8) !void {
                try w.writeByte('\'');
                for (s) |ch| {
                    if (ch == '\'') {
                        try w.writeAll("''");
                    } else if (ch == '\\' and dialect.mysqlWire()) {
                        try w.writeAll("\\\\");
                    } else {
                        try w.writeByte(ch);
                    }
                }
                try w.writeByte('\'');
            }

            fn strLit(self: Dialect, arena: std.mem.Allocator, s: []const u8) ![]const u8 {
                var out = std.array_list.Managed(u8).init(arena);
                try out.append('\'');
                for (s) |c| {
                    if (c == '\'' or (c == '\\' and self.mysqlWire())) try out.append(c);
                    try out.append(c);
                }
                try out.append('\'');
                return out.toOwnedSlice();
            }

            fn objectId(arena: std.mem.Allocator, qtable: []const u8) ![]const u8 {
                const lit = try std.mem.replaceOwned(u8, arena, qtable, "'", "''");
                return std.fmt.allocPrint(arena, "IF OBJECT_ID(N'{s}', N'U') IS NULL CREATE TABLE {s} (", .{ lit, qtable });
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

            fn unquotable(dialect: Dialect, name: []const u8) bool {
                return dialect == .starrocks and std.mem.indexOfScalar(u8, name, '`') != null;
            }
        };
    };
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var prng = std.Random.DefaultPrng.init(11);
    const r = prng.random();
    var buf: [300]u8 = undefined;
    for (0..3000) |i| {
        const n = if (i < 8) i * 37 else r.uintAtMost(usize, 40);
        for (buf[0..n]) |*b| b.* = if (i % 3 == 0) r.int(u8) else "'\\`[]\".ab\x00\xc3\xb3"[r.uintLessThan(usize, 12)];
        const s = buf[0..n];
        for (std.enums.values(Dialect)) |d| {
            var want: std.Io.Writer.Allocating = .init(a);
            var got: std.Io.Writer.Allocating = .init(a);
            try Orig.Pinned.quoteString(&want.writer, d, s);
            try d.writeStrLit(&got.writer, s, .data);
            const n_prefix: []const u8 = if (d == .sqlserver) "N" else "";
            try std.testing.expectEqualStrings(try std.mem.concat(a, u8, &.{ n_prefix, want.written() }), got.written());
            try std.testing.expectEqualStrings(try Orig.Pinned.strLit(d, a, s), try d.strLit(a, s));
            try std.testing.expectEqual(Orig.Pinned.unquotable(d, s), !d.quotable(s));

            var hw: std.Io.Writer.Allocating = .init(a);
            var hg: std.Io.Writer.Allocating = .init(a);
            try Orig.Pinned.hexLiteral(&hw.writer, d, s);
            try hexLiteral(&hg.writer, d, s);
            try std.testing.expectEqualStrings(hw.written(), hg.written());
        }
        const qt = try quoteIdent(a, .sqlserver, s);
        const ddl = try createTableSql(a, .sqlserver, qt, .{ .fields = &.{.{ .name = "c", .ty = types.Type.init(.int) }} }, .append);
        try std.testing.expect(std.mem.startsWith(u8, ddl, try Orig.Pinned.objectId(a, qt)));
    }
}

test "a SQL Server value literal is N-prefixed, so text outside the code page survives a MERGE" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings("N'\xc3\xb3 \xf0\x9f\x98\x80'", try literalOf(a, .sqlserver, .string, .{ .string = "\xc3\xb3 \xf0\x9f\x98\x80" }));
    try std.testing.expectEqualStrings("N'O''Brien'", try literalOf(a, .sqlserver, .string, .{ .string = "O'Brien" }));
    try std.testing.expectEqualStrings("'\xc3\xb3'", try literalOf(a, .postgres, .string, .{ .string = "\xc3\xb3" }));
    var w: std.Io.Writer.Allocating = .init(a);
    try Dialect.sqlserver.writeStrLit(&w.writer, "[dbo].[A\xc3\xa7\xc3\xa3o]", .catalog_name);
    try Dialect.sqlserver.writeStrLit(&w.writer, "x", .predicate);
    try Dialect.mysql.writeStrLit(&w.writer, "x", .catalog_name);
    try std.testing.expectEqualStrings("N'[dbo].[A\xc3\xa7\xc3\xa3o]''x''x'", w.written());
}

test "the INSERT sink gives up its redial pause, and the redial, once the run is aborting" {
    const gpa = std.testing.allocator;
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    const Fake = struct {
        dials: usize = 0,
        fn queryCursor(_: *anyopaque, _: []const u8) anyerror!Cursor {
            return error.Unsupported;
        }
        fn exec(_: *anyopaque, s: []const u8) anyerror!void {
            if (std.mem.startsWith(u8, s, "INSERT")) return error.ConnectionResetByPeer;
        }
        fn close(_: *anyopaque) void {}
        fn dial(ctx: *const anyopaque, _: std.mem.Allocator) anyerror!Conn {
            const f: *@This() = @ptrCast(@alignCast(@constCast(ctx)));
            f.dials += 1;
            return .{ .ptr = f, .vtable = &vt };
        }
        const vt = Conn.VTable{ .queryCursor = queryCursor, .exec = exec, .close = close };
    };
    var fake = Fake{};
    const schema = types.Schema{ .fields = &.{.{ .name = "id", .ty = types.Type.init(.int).asNullable() }} };
    var ids = [_]?i64{1};
    var cols = [_]column.Column{try column.intColumn(a, &ids)};
    const batch = Batch{ .schema = &schema, .columns = &cols, .len = 1 };
    const s = try Sink.open(gpa, .{ .ptr = &fake, .vtable = &Fake.vt }, .postgres, "t", schema, .append, .{ .ctx = &fake, .dial = Fake.dial });
    try s.writeBatch(a, batch);
    driver.requestAbort();
    defer driver.resetAbort();
    const t0 = try std.time.Instant.now();
    try std.testing.expectError(error.ConnectionResetByPeer, s.close());
    try std.testing.expect((try std.time.Instant.now()).since(t0) < 400 * std.time.ns_per_ms);
    try std.testing.expectEqual(@as(usize, 0), fake.dials);
}
