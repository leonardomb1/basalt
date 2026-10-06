//! Source and sink resolution: connector-name dispatch, connection config
//! (SQL, AAD, NTLM, StarRocks), split planning, and the parallel sink specs.
//!
//! The `csv` connector covers every file path, so a file's format comes from an
//! explicit `format` hint, else its extension: without that a `.parquet` path was
//! memory-mapped and parsed as CSV text, and a `.parquet` target written as CSV.
//! Summary labels follow the same rule, so telemetry does not report parquet as
//! csv. A file sink truncates unless the write says `APPEND`, which is refused
//! where bytes cannot be added (a parquet footer, a block blob).
//!
//! Parallel writes: a split pipeline's sink runs its DDL (and an overwrite's
//! DELETE or TRUNCATE) once at plan time, then each lane opens its own Stream
//! Load stream (shared run_id, lane-distinct labels) or SQL connection. That is
//! safe because splits are disjoint key ranges, so no two lanes write the same
//! key. Append and overwrite take the dialect's bulk loader (COPY, LOAD DATA,
//! INSERT BULK); upsert takes the generic INSERT sink, the only one that can
//! redial on a transient error, since the bulk loaders are mid-protocol streams.
//! Postgres COPY benchmarks faster serial, so it is not split. Other sinks (CSV)
//! share one sink behind a mutex.
//!
//! A SQL read is narrowed to the columns later stages need when that is provable,
//! and only ever to a superset, so a wrong guess fails at the source rather than
//! quietly. `SELECT * EXCEPT (...)` is resolved with a zero-row probe so unwanted
//! wide columns never leave the server.
//!
//! The parts are in connect/: `source.zig` and `sink.zig` open reads and writes,
//! `sql_read.zig` shapes what a SQL read sends, `dbconfig.zig` connects a database,
//! `facts.zig` asks a catalog, `lane_sink.zig` gives each parallel lane its sink, and
//! `register.zig` registers SFTP and SMB connections.

const std = @import("std");
const ast = @import("../lang/ast.zig");
const types = @import("../lang/types.zig");
const op = @import("../exec/op.zig");
const Batch = @import("../exec/batch.zig").Batch;
const column = @import("../exec/column.zig");
const csv = @import("../format/csv.zig");
const pqdecode = @import("../format/parquet/read.zig");
const folder = @import("../connect/folder.zig");
const pqwrite = @import("../format/parquet/write.zig");
const arrowread = @import("../format/arrowread.zig");
const xlsx = @import("../format/xlsx.zig");
const sftp = @import("../store/sftp.zig");
const smb = @import("../store/smb.zig");
const JsonWriter = @import("../connect/table.zig").JsonWriter;
const arrow = @import("../format/arrow.zig");
const ArrowWriter = arrow.ArrowWriter;
const ArrowFileSink = arrow.FileSink;
const TableWriter = @import("../connect/table.zig").TableWriter;
const driver = @import("../connect/driver.zig");
const streamload = @import("../db/streamload.zig");
const registry = @import("../connect/registry.zig");
const projectedColumns = @import("plan.zig").projectedColumns;
const tds = @import("../db/tds.zig");
const mysql = @import("../db/mysql.zig");
const postgres = @import("../db/postgres.zig");
const sql = @import("../db/sql.zig");
const pushdown = @import("pushdown.zig");
const Value = @import("../exec/value.zig").Value;
const request = @import("../connect/request.zig");
const http_client = @import("../net/http_client.zig");
const aad = @import("../db/aad.zig");
const split = @import("../connect/split.zig");
const ssrp = @import("../db/ssrp.zig");
const ntlm = @import("../net/ntlm.zig");
const krb5 = @import("../net/krb5.zig");
const BufferSource = @import("../connect/wal.zig").BufferSource;
const azure = @import("../store/azure.zig");
const s3 = @import("../store/s3.zig");
const gen = @import("../connect/gen.zig");
const parallel = @import("parallel.zig");
const analyze = @import("analyze.zig");
const obs = @import("obs.zig");

pub const DbAuth = @import("env.zig").DbAuth;
const DbConfig = @import("env.zig").DbConfig;
const Env = @import("env.zig").Env;
const env_mod = @import("env.zig");
const eqlAny = @import("env.zig").eqlAny;
const forHintName = @import("env.zig").forHintName;
const pathFail = @import("env.zig").pathFail;
const planErr = @import("env.zig").planErr;
const planErrT = @import("env.zig").planErrT;
const SqlDesc = @import("env.zig").SqlDesc;
pub const SqlKind = @import("env.zig").SqlKind;
const srOpenErr = @import("env.zig").srOpenErr;

pub const DiscardSink = struct {
    fn writeBatch(_: *anyopaque, _: std.mem.Allocator, _: Batch) anyerror!void {}
    fn close(_: *anyopaque) anyerror!void {}
    fn abort(_: *anyopaque) void {}
    const vtable = driver.Sink.VTable{ .writeBatch = writeBatch, .close = close, .abort = abort };
    var unit: u8 = 0;
    pub fn sink() driver.Sink {
        return .{ .ptr = &unit, .vtable = &vtable };
    }
};

pub const SplitCtx = struct {
    gpa: std.mem.Allocator,
    kind: SqlKind,
    cfg: DbConfig,
    base_sql: []const u8,
    proj_select: ?[]const u8 = null,
    where_extra: ?[]const u8 = null,
    report: ?sql.Report = null,
};

const ReadReport = struct {
    errctx: *op.ErrCtx,
    connector: []const u8,

    fn f(ctx: *anyopaque, e: anyerror, msg: []const u8) void {
        const self: *ReadReport = @ptrCast(@alignCast(ctx));
        self.errctx.set("{s} read failed ({s}): {s}", .{ self.connector, @errorName(e), msg });
    }
};

pub fn readReport(env: *Env, connector: []const u8) !sql.Report {
    const r = try env.arena.create(ReadReport);
    r.* = .{ .errctx = env.errctx, .connector = connector };
    return .{ .ctx = r, .f = ReadReport.f };
}

/// The concrete driver for one `SqlKind`: `connect` (sqlserver via `tdsConnect`)
/// and its `Bulk` sink. Comptime, so callers reach the concrete conn type with
/// `switch (kind) { inline else => |k| ... }`.
pub fn SqlDriver(comptime kind: SqlKind) type {
    return switch (kind) {
        .postgres => struct {
            pub const Bulk = postgres.CopySink;
            pub fn connect(gpa: std.mem.Allocator, cfg: DbConfig) !*postgres.Conn {
                return postgres.Conn.connect(gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database, cfg.tls) catch |e| return sql.onWire(e);
            }
        },
        .mysql => struct {
            pub const Bulk = mysql.LoadDataSink;
            pub fn connect(gpa: std.mem.Allocator, cfg: DbConfig) !*mysql.Conn {
                return mysql.Conn.connect(gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database, cfg.tls) catch |e| return sql.onWire(e);
            }
        },
        .sqlserver => struct {
            pub const Bulk = tds.BulkSink;
            pub fn connect(gpa: std.mem.Allocator, cfg: DbConfig) !*tds.Conn {
                return tdsConnect(gpa, cfg) catch |e| return sql.onWire(e);
            }
        },
    };
}

pub fn connectSql(gpa: std.mem.Allocator, kind: SqlKind, cfg: DbConfig) !sql.Conn {
    switch (kind) {
        inline else => |k| return (try SqlDriver(k).connect(gpa, cfg)).sqlConn(),
    }
}

pub fn openSplitSource(ctx_ptr: *anyopaque, gpa: std.mem.Allocator, pred: []const u8) anyerror!driver.Source {
    const ctx: *SplitCtx = @ptrCast(@alignCast(ctx_ptr));
    const q = try split.wrapProjected(gpa, ctx.base_sql, ctx.proj_select, pred, ctx.where_extra);
    defer gpa.free(q);
    return openSqlQuery(ctx, gpa, q);
}

pub fn openSqlQuery(ctx: *const SplitCtx, gpa: std.mem.Allocator, query: []const u8) anyerror!driver.Source {
    const conn = try connectSql(gpa, ctx.kind, ctx.cfg);
    errdefer conn.close();
    const s = try sql.Source.open(gpa, conn, query);
    s.report = ctx.report;
    return s.source();
}

pub const buildParallelSink = @import("connect/lane_sink.zig").buildParallelSink;
pub const ConstSource = @import("connect/source.zig").ConstSource;
pub const openSource = @import("connect/source.zig").openSource;
pub const openSourceProjected = @import("connect/source.zig").openSourceProjected;
pub const resolveFolder = @import("connect/source.zig").resolveFolder;
pub const resolveFolderFmt = @import("connect/source.zig").resolveFolderFmt;
pub const FolderRead = @import("connect/source.zig").FolderRead;
pub const noteParquet = @import("connect/source.zig").noteParquet;
const tdsConnect = @import("connect/dbconfig.zig").tdsConnect;
const parseDbConfig = @import("connect/dbconfig.zig").parseDbConfig;
pub const selectListFor = @import("connect/sql_read.zig").selectListFor;
pub const projectSqlRead = @import("connect/sql_read.zig").projectSqlRead;
pub const exceptColumns = @import("connect/sql_read.zig").exceptColumns;
pub const sqlWithWhere = @import("connect/sql_read.zig").sqlWithWhere;
pub const sqlDescForStage = @import("connect/sql_read.zig").sqlDescForStage;
pub const planSplit = @import("connect/sql_read.zig").planSplit;
pub const columnFacts = @import("connect/facts.zig").columnFacts;
pub const factsIfWanted = @import("connect/facts.zig").factsIfWanted;
pub const resolveUpsertKeys = @import("connect/sink.zig").resolveUpsertKeys;
pub const guardFileFormat = @import("connect/sink.zig").guardFileFormat;
pub const openSink = @import("connect/sink.zig").openSink;
pub const registerSftp = @import("connect/register.zig").registerSftp;
pub const registerSmb = @import("connect/register.zig").registerSmb;

/// Gates the mmap'd parallel-CSV fast paths; a `.parquet` path shares the `csv`
/// connector and must not be parsed as text.
pub fn isLocalCsvRead(rd: ast.Read) bool {
    if (!std.mem.eql(u8, rd.connector, "csv")) return false;
    return switch (rd.form) {
        .path => |p| !csv.CsvReader.isUrl(p) and !folder.isFolder(p) and analyze.readFormat(p, null) == .csv,
        else => false,
    };
}

pub fn isFolderRead(rd: ast.Read) bool {
    if (!std.mem.eql(u8, rd.connector, "csv")) return false;
    return rd.form == .path and folder.isFolder(rd.form.path);
}

pub fn isLocalParquetRead(rd: ast.Read) bool {
    if (!std.mem.eql(u8, rd.connector, "csv")) return false;
    return switch (rd.form) {
        .path => |p| !csv.CsvReader.isUrl(p) and pqdecode.Reader.isPath(p),
        else => false,
    };
}

pub const mem_connector = "__memory";

pub fn sinkLabel(env: *Env, w: ast.Write) []const u8 {
    if (std.mem.eql(u8, w.connector, mem_connector)) return "memory";
    if (std.mem.eql(u8, w.connector, "csv") and pqwrite.Writer.isPath(w.target)) return "parquet";
    if (std.mem.eql(u8, w.connector, "csv") and arrowread.isPath(w.target)) return "arrow";
    return connectorType(env, w.connector);
}

pub fn sourceLabel(env: *Env, rd: ast.Read, hints: []const ast.Hint) []const u8 {
    return switch (rd.form) {
        .path => |p| analyze.formatLabel(p, hints),
        else => connectorType(env, rd.connector),
    };
}

pub fn connectorType(env: *Env, name: []const u8) []const u8 {
    if (std.mem.eql(u8, name, "csv") or std.mem.eql(u8, name, "request") or
        std.mem.eql(u8, name, "http") or std.mem.eql(u8, name, "buffer")) return name;
    if (env.connections.get(name)) |c| return c.connector;
    return name;
}

pub const SqlConnInfo = registry.SqlRead;

/// Null for connectors without a SQL wire protocol. A `starrocks` connection
/// qualifies for reads; every write path checks for Stream Load first.
pub fn sqlConnInfo(conn: ast.Connection) ?SqlConnInfo {
    const c = registry.Connector.parse(conn.connector) orelse return null;
    return c.sqlRead();
}

/// Every connection carries `user`/`password`, but http reads them only for
/// basic auth, and for oauth2 without client_id/client_secret.
pub fn httpAttrUsed(conn: ast.Connection, auth: []const u8, key: []const u8) bool {
    const alias: []const u8 = if (std.mem.eql(u8, key, "user"))
        "client_id"
    else if (std.mem.eql(u8, key, "password"))
        "client_secret"
    else
        return true;
    if (std.mem.eql(u8, auth, "basic")) return true;
    if (!std.mem.eql(u8, auth, "oauth2")) return false;
    for (conn.config) |attr| {
        if (std.mem.eql(u8, attr.key, alias)) return false;
    }
    return true;
}

/// A name given twice (a resource's, then the read's own) keeps the later value.
pub fn queryParams(arena: std.mem.Allocator, hints: []const ast.Hint) ![]const http_client.KV {
    var out = std.array_list.Managed(http_client.KV).init(arena);
    for (hints) |h| {
        if (!std.mem.startsWith(u8, h.key, "query:")) continue;
        const key = h.key["query:".len..];
        const val = switch (h.value) {
            .str, .ident => |s| s,
            .int => |i| try std.fmt.allocPrint(arena, "{d}", .{i}),
            .flag => "",
        };
        for (out.items) |*kv| {
            if (std.mem.eql(u8, kv.key, key)) {
                kv.value = val;
                break;
            }
        } else try out.append(.{ .key = key, .value = val });
    }
    return out.toOwnedSlice();
}

fn cfgStr(arena: std.mem.Allocator, expr: *const ast.Expr) ?[]const u8 {
    return switch (expr.*) {
        .str_lit => |s| s,
        .int_lit => |i| std.fmt.allocPrint(arena, "{d}", .{i}) catch null,
        .call => |c| if ((std.mem.eql(u8, c.name, "env") or std.mem.eql(u8, c.name, "secret")) and c.args.len == 1 and c.args[0].* == .str_lit)
            (std.process.getEnvVarOwned(arena, c.args[0].str_lit) catch null)
        else
            null,
        else => null,
    };
}

pub fn dupeSchema(arena: std.mem.Allocator, s: types.Schema) !types.Schema {
    const fields = try arena.alloc(types.Schema.Field, s.fields.len);
    for (s.fields, fields) |f, *o| o.* = .{ .name = try arena.dupe(u8, f.name), .ty = f.ty };
    return .{ .fields = fields };
}

pub const OneBatch = struct {
    b: ?Batch,
    sch: types.Schema,
    pub fn source(self: *OneBatch) driver.Source {
        return .{ .ptr = self, .vtable = &ob_vtable };
    }
};

const ob_vtable = driver.Source.VTable{ .schema = obSchema, .next = obNext, .close = obClose };

fn obSchema(ptr: *anyopaque) types.Schema {
    return @as(*OneBatch, @ptrCast(@alignCast(ptr))).sch;
}

fn obNext(ptr: *anyopaque, _: std.mem.Allocator) anyerror!?Batch {
    const self: *OneBatch = @ptrCast(@alignCast(ptr));
    defer self.b = null;
    return self.b;
}

fn obClose(_: *anyopaque) void {}

test "an http connection reads user/password only where its auth uses them" {
    var v = ast.Expr{ .str_lit = "x" };
    const pos = ast.Pos{ .line = 0, .col = 0 };
    const plain = ast.Connection{ .name = "rc", .connector = "http", .pos = pos, .config = &.{
        .{ .key = "base_url", .value = &v, .pos = pos },
        .{ .key = "user", .value = &v, .pos = pos },
        .{ .key = "password", .value = &v, .pos = pos },
    } };
    try std.testing.expect(httpAttrUsed(plain, "", "base_url"));
    for ([_][]const u8{ "", "bearer", "header", "login_json" }) |auth| {
        try std.testing.expect(!httpAttrUsed(plain, auth, "user"));
        try std.testing.expect(!httpAttrUsed(plain, auth, "password"));
    }
    try std.testing.expect(httpAttrUsed(plain, "basic", "user"));
    try std.testing.expect(httpAttrUsed(plain, "basic", "password"));
    try std.testing.expect(httpAttrUsed(plain, "oauth2", "user"));

    const oauth = ast.Connection{ .name = "rc", .connector = "http", .pos = pos, .config = &.{
        .{ .key = "client_id", .value = &v, .pos = pos },
        .{ .key = "user", .value = &v, .pos = pos },
        .{ .key = "password", .value = &v, .pos = pos },
    } };
    try std.testing.expect(!httpAttrUsed(oauth, "oauth2", "user"));
    try std.testing.expect(httpAttrUsed(oauth, "oauth2", "password"));
}

test {
    _ = @import("connect/dbconfig.zig");
    _ = @import("connect/facts.zig");
    _ = @import("connect/lane_sink.zig");
    _ = @import("connect/register.zig");
    _ = @import("connect/sink.zig");
    _ = @import("connect/source.zig");
    _ = @import("connect/sql_read.zig");
    _ = @import("connect/testing_util.zig");
}
