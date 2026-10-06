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

const DbAuth = @import("env.zig").DbAuth;
const DbConfig = @import("env.zig").DbConfig;
const Env = @import("env.zig").Env;
const env_mod = @import("env.zig");
const eqlAny = @import("env.zig").eqlAny;
const forHintName = @import("env.zig").forHintName;
const pathFail = @import("env.zig").pathFail;
const planErr = @import("env.zig").planErr;
const planErrT = @import("env.zig").planErrT;
const SqlDesc = @import("env.zig").SqlDesc;
const SqlKind = @import("env.zig").SqlKind;
const srOpenErr = @import("env.zig").srOpenErr;

const DiscardSink = struct {
    fn writeBatch(_: *anyopaque, _: std.mem.Allocator, _: Batch) anyerror!void {}
    fn close(_: *anyopaque) anyerror!void {}
    fn abort(_: *anyopaque) void {}
    const vtable = driver.Sink.VTable{ .writeBatch = writeBatch, .close = close, .abort = abort };
    var unit: u8 = 0;
    fn sink() driver.Sink {
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
fn SqlDriver(comptime kind: SqlKind) type {
    return switch (kind) {
        .postgres => struct {
            const Bulk = postgres.CopySink;
            fn connect(gpa: std.mem.Allocator, cfg: DbConfig) !*postgres.Conn {
                return postgres.Conn.connect(gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database, cfg.tls) catch |e| return sql.onWire(e);
            }
        },
        .mysql => struct {
            const Bulk = mysql.LoadDataSink;
            fn connect(gpa: std.mem.Allocator, cfg: DbConfig) !*mysql.Conn {
                return mysql.Conn.connect(gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database, cfg.tls) catch |e| return sql.onWire(e);
            }
        },
        .sqlserver => struct {
            const Bulk = tds.BulkSink;
            fn connect(gpa: std.mem.Allocator, cfg: DbConfig) !*tds.Conn {
                return tdsConnect(gpa, cfg) catch |e| return sql.onWire(e);
            }
        },
    };
}

fn connectSql(gpa: std.mem.Allocator, kind: SqlKind, cfg: DbConfig) !sql.Conn {
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

const StreamLoadSpec = struct {
    cfg: streamload.Config,
    target: []const u8,
    schema: types.Schema,
    mode: ast.WriteMode,
    logger: ?*obs.Logger = null,
    errctx: ?*op.ErrCtx = null,
};

fn openLaneStreamLoadSink(ctx_ptr: *anyopaque, gpa: std.mem.Allocator, lane_idx: usize) anyerror!driver.Sink {
    const spec: *StreamLoadSpec = @ptrCast(@alignCast(ctx_ptr));
    var cfg = spec.cfg;
    const lp = try std.fmt.allocPrint(gpa, "{s}_l{d}", .{ spec.cfg.label_prefix, lane_idx });
    defer gpa.free(lp);
    cfg.label_prefix = lp;
    const s = try streamload.StreamLoadSink.open(gpa, cfg, spec.target, spec.schema, spec.mode);
    s.logger = spec.logger;
    s.errctx = spec.errctx;
    return s.sink();
}

const SqlSinkSpec = struct {
    kind: SqlKind,
    dialect: sql.Dialect,
    cfg: DbConfig,
    target: []const u8,
    schema: types.Schema,
    lane_mode: ast.WriteMode,
    redial: sql.Redial,
};

const DialSpec = struct { kind: SqlKind, cfg: DbConfig };

fn dialSqlConn(ctx: *const anyopaque, gpa: std.mem.Allocator) anyerror!sql.Conn {
    const spec: *const DialSpec = @ptrCast(@alignCast(ctx));
    return connectSql(gpa, spec.kind, spec.cfg);
}

fn redialFor(arena: std.mem.Allocator, kind: SqlKind, cfg: DbConfig) !sql.Redial {
    const ds = try arena.create(DialSpec);
    ds.* = .{ .kind = kind, .cfg = cfg };
    return .{ .ctx = ds, .dial = dialSqlConn };
}

/// The bulk-vs-INSERT rule, shared by the serial and per-lane paths so they
/// cannot drift. On error the caller still owns and closes `conn`.
fn openBulkOrInsert(gpa: std.mem.Allocator, conn: anytype, comptime BulkSink: type, dialect: sql.Dialect, target: []const u8, schema: types.Schema, mode: ast.WriteMode, redial: ?sql.Redial) !driver.Sink {
    if (mode != .upsert) return (try BulkSink.open(gpa, conn, target, schema, mode, redial)).sink();
    return (try sql.Sink.open(gpa, conn.sqlConn(), dialect, target, schema, mode, redial)).sink();
}

fn openLaneSqlSink(ctx_ptr: *anyopaque, gpa: std.mem.Allocator, lane_idx: usize) anyerror!driver.Sink {
    _ = lane_idx;
    const spec: *SqlSinkSpec = @ptrCast(@alignCast(ctx_ptr));
    switch (spec.kind) {
        inline else => |k| {
            const c = try SqlDriver(k).connect(gpa, spec.cfg);
            errdefer c.close();
            return openBulkOrInsert(gpa, c, SqlDriver(k).Bulk, spec.dialect, spec.target, spec.schema, spec.lane_mode, spec.redial);
        },
    }
}

/// A per-lane Stream Load or SQL sink for a split pipeline, or null for the
/// shared-mutex path.
pub fn buildParallelSink(env: *Env, w: ast.Write, schema: types.Schema) !?parallel.SinkMode {
    if (try buildStreamLoadSpec(env, w, schema)) |spec|
        return parallel.SinkMode{ .per_lane = .{ .open = openLaneStreamLoadSink, .ctx = spec } };
    if (try buildSqlSinkSpec(env, w, schema)) |spec|
        return parallel.SinkMode{ .per_lane = .{ .open = openLaneSqlSink, .ctx = spec } };
    return null;
}

fn buildSqlSinkSpec(env: *Env, w: ast.Write, schema: types.Schema) !?*SqlSinkSpec {
    const conn = env.connections.get(w.connector) orelse return null;
    const info = sqlConnInfo(conn) orelse return null;
    const kind = info.kind;
    const dialect = info.dialect;
    const cfg = try resolveDbConfig(env, conn, info.port);

    const setup_conn = connectSql(env.gpa, kind, cfg) catch |e|
        return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "{s} sink connect failed: {s}", .{ conn.connector, try connectWhy(env.arena, conn.connector, e) }));
    const setup = sql.Sink.open(env.gpa, setup_conn, dialect, w.target, schema, w.mode, null) catch |e|
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "{s} sink setup failed: {s}", .{ conn.connector, @errorName(e) }));
    setup.sink().close() catch |e|
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "{s} sink setup close failed: {s}", .{ conn.connector, @errorName(e) }));

    const spec = try env.arena.create(SqlSinkSpec);
    spec.* = .{
        .kind = kind,
        .dialect = dialect,
        .cfg = cfg,
        .target = w.target,
        .schema = schema,
        .lane_mode = if (w.mode == .overwrite) .append else w.mode,
        .redial = try redialFor(env.arena, kind, cfg),
    };
    return spec;
}

fn buildStreamLoadSpec(env: *Env, w: ast.Write, schema: types.Schema) !?*StreamLoadSpec {
    const conn = env.connections.get(w.connector) orelse return null;
    const flavor = streamload.Flavor.of(conn.connector) orelse return null;

    var cfg = try resolveStreamLoadConfig(env, conn, flavor);
    cfg.run_id = if (cfg.run_id != 0) cfg.run_id else @intCast(std.time.milliTimestamp());

    const setup = streamload.StreamLoadSink.open(env.gpa, cfg, w.target, schema, w.mode) catch |e|
        return srOpenErr(env, e, try std.fmt.allocPrint(env.arena, "{s} setup failed", .{flavor.name()}));
    setup.logger = env.log;
    setup.errctx = env.errctx;
    setup.sink().close() catch |e|
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "{s} setup close failed: {s}", .{ flavor.name(), @errorName(e) }));
    cfg.auto_create = false;

    const spec = try env.arena.create(StreamLoadSpec);
    spec.* = .{ .cfg = cfg, .target = w.target, .schema = schema, .mode = if (w.mode == .overwrite) .append else w.mode, .logger = env.log, .errctx = env.errctx };
    return spec;
}

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

fn connectorType(env: *Env, name: []const u8) []const u8 {
    if (std.mem.eql(u8, name, "csv") or std.mem.eql(u8, name, "request") or
        std.mem.eql(u8, name, "http") or std.mem.eql(u8, name, "buffer")) return name;
    if (env.connections.get(name)) |c| return c.connector;
    return name;
}

pub const ConstSource = struct {
    batch: Batch,
    out: *const types.Schema,
    yielded: bool = false,

    fn schemaFn(ptr: *anyopaque) types.Schema {
        const self: *ConstSource = @ptrCast(@alignCast(ptr));
        return self.out.*;
    }
    fn nextFn(ptr: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
        _ = arena;
        const self: *ConstSource = @ptrCast(@alignCast(ptr));
        if (self.yielded) return null;
        self.yielded = true;
        return self.batch;
    }
    fn closeFn(ptr: *anyopaque) void {
        _ = ptr;
    }
    pub const vtable = driver.Source.VTable{ .schema = schemaFn, .next = nextFn, .close = closeFn };
};

pub fn openSource(env: *Env, rd: ast.Read, hints: []const ast.Hint) !driver.Source {
    return openSourceProjected(env, rd, hints, null, &.{});
}

/// Only the parquet and Arrow readers act on `project`. Parquet readers are counted
/// so a top-N bound is pushed only into a pipeline with exactly one of them.
pub fn openSourceProjected(
    env: *Env,
    rd: ast.Read,
    hints: []const ast.Hint,
    project: ?[][]const u8,
    bounds: []const pqdecode.Bound,
) !driver.Source {
    if (std.mem.eql(u8, rd.connector, "csv") and rd.form == .path) {
        if (try resolveFolder(env, rd.form.path, hints)) |fr| if (fr.kind == .parquet) {
            env.folder_memo = null;
            const pf = try openParquetFolder(env, rd.form.path, fr.files, project);
            pf.bounds = bounds;
            env.pq_readers += 1;
            env.pq_folder = pf;
            return pf.source();
        };
    }
    const is_pq = std.mem.eql(u8, rd.connector, "csv") and rd.form == .path and
        pqdecode.Reader.isPath(rd.form.path);
    if (is_pq) {
        const pr = pqdecode.Reader.openProjected(env.arena, rd.form.path, project) catch |e|
            return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not read parquet `{s}` ({s})", .{ rd.form.path, try pathFail(env.arena, rd.form.path, e) }));
        pr.bounds = bounds;
        noteParquet(env, pr);
        env.pq_readers += 1;
        env.pq_reader = pr;
        return pr.source();
    }
    if (std.mem.eql(u8, rd.connector, "csv") and rd.form == .path and project != null) {
        var fdiag = analyze.Diag{};
        const want = analyze.formatFromHints(hints, &fdiag) catch null;
        if (analyze.readFormat(rd.form.path, want) == .arrow and analyze.unreadableTarget(rd.form.path, want) == null)
            return openArrow(env, rd.form.path, project);
    }
    return openSourceCols(env, rd, hints, project);
}

/// A folder's format and files, or null when `path` is not a folder. Without
/// `format` the listing decides; a mix of Parquet and CSV is refused. Remembered
/// until the source opens, since the read is resolved twice and listing is a round trip.
pub fn resolveFolder(env: *Env, path: []const u8, hints: []const ast.Hint) !?FolderRead {
    if (!folder.isFolder(path)) return null;
    var fdiag = analyze.Diag{};
    const want = analyze.formatFromHints(hints, &fdiag) catch
        return planErr(env.diag, try env.arena.dupe(u8, fdiag.msg));
    return resolveFolderFmt(env, path, want);
}

pub fn resolveFolderFmt(env: *Env, path: []const u8, want: ?analyze.FileFormat) !?FolderRead {
    if (!folder.isFolder(path)) return null;
    if (env.folder_memo) |m| if (std.mem.eql(u8, m.path, path)) return m.read;
    const all = folder.list(env.arena, path) catch |e| {
        if (e == azure.Error.AzureEmptyPrefix)
            return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "no blobs under prefix `{s}`", .{path}));
        if (e == s3.Error.S3EmptyPrefix)
            return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "no objects under prefix `{s}`", .{path}));
        return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not list folder `{s}` ({s})", .{ path, try pathFail(env.arena, path, e) }));
    };
    if (all.len == 0) return planErr(env.diag, try std.fmt.allocPrint(env.arena, "no files in folder `{s}`", .{path}));
    const kind: folder.Kind = if (want) |w| switch (w) {
        .parquet => .parquet,
        .csv => .csv,
        else => return planErr(env.diag, try std.fmt.allocPrint(env.arena, "folder `{s}`: a folder reads Parquet or CSV files", .{path})),
    } else switch (folder.kindOf(all)) {
        .kind => |k| k,
        .mixed => |m| return planErr(env.diag, try std.fmt.allocPrint(env.arena, "folder `{s}` holds both Parquet (`{s}`) and CSV (`{s}`) files; one table reads one format — name it with WITH (format = 'parquet') or 'csv'", .{ path, folder.below(m.parquet, path), folder.below(m.csv, path) })),
        .empty => unreachable,
        .neither => return planErr(env.diag, try std.fmt.allocPrint(env.arena, "no .parquet, .csv, .tsv or .txt file in folder `{s}`", .{path})),
    };
    const files = try folder.only(env.arena, all, kind);
    if (files.len == 0) return planErr(env.diag, try std.fmt.allocPrint(env.arena, "no {s} file in folder `{s}`", .{ if (kind == .parquet) ".parquet" else ".csv, .tsv or .txt", path }));
    const read = FolderRead{ .kind = kind, .files = files };
    env.folder_memo = .{ .path = try env.arena.dupe(u8, path), .read = read };
    return read;
}

pub const FolderRead = env_mod.FolderRead;

fn openParquetFolder(env: *Env, path: []const u8, files: []const []const u8, project: ?[][]const u8) !*pqdecode.Folder {
    const pf = pqdecode.Folder.open(env.arena, path, files, project) catch |e|
        return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not read parquet `{s}` in folder `{s}` ({s})", .{ files[0], path, try pathFail(env.arena, files[0], e) }));
    if (pf.firstReader()) |first| {
        noteParquet(env, first);
        pf.tally = first.tally;
    }
    return pf;
}

pub fn noteParquet(env: *Env, pr: *pqdecode.Reader) void {
    const t = env.scan orelse return;
    pr.tally = t;
    driver.ScanTally.add(&t.columns_read, pr.leaves.len);
    driver.ScanTally.add(&t.columns_total, pr.md.leafCount());
}

fn openArrow(env: *Env, path: []const u8, project: ?[][]const u8) !driver.Source {
    const r = arrowread.Reader.openProjected(env.arena, path, project) catch |e| {
        const why = switch (e) {
            error.UnsupportedArrow => "an Arrow type or feature basalt does not read",
            error.NotArrow => "not an Arrow IPC file or stream",
            error.CorruptArrow => "the file is truncated or corrupt",
            else => try pathFail(env.arena, path, e),
        };
        return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not read Arrow IPC `{s}` ({s})", .{ path, why }));
    };
    if (env.scan) |t| {
        driver.ScanTally.add(&t.columns_read, r.keep.len);
        driver.ScanTally.add(&t.columns_total, r.fields.len);
    }
    return r.source();
}

fn openXlsx(env: *Env, path: []const u8, hints: []const ast.Hint) !driver.Source {
    var odiag = analyze.Diag{};
    const opts = analyze.xlsxOptions(hints, &odiag) catch return planErr(env.diag, odiag.msg);
    const r = xlsx.Reader.open(env.arena, env.gpa, path, opts) catch |e| {
        const why = switch (e) {
            error.XlsxSheetNotFound => blk: {
                const names = xlsx.sheetNames(env.arena, env.gpa, path) catch &.{};
                const list = try std.mem.join(env.arena, ", ", names);
                break :blk if (opts.sheet) |w|
                    try std.fmt.allocPrint(env.arena, "no sheet `{s}` (sheets: {s})", .{ w, list })
                else
                    "the workbook has no worksheet";
            },
            error.NotXlsx, error.ZipNoEndRecord, error.ZipMemberNotFound => "not an Excel workbook (.xlsx)",
            error.BadXml, error.XmlTokenTooLong => "the workbook's XML is malformed",
            error.XlsxTooManyStrings => "its shared strings pass 512 MB",
            else => try pathFail(env.arena, path, e),
        };
        return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not read Excel workbook `{s}` ({s})", .{ path, why }));
    };
    return r.source();
}

fn openSourceAll(env: *Env, rd: ast.Read, hints: []const ast.Hint) !driver.Source {
    return openSourceCols(env, rd, hints, null);
}

/// `openSourceAll`, a CSV converting only the columns `project` names. A union's
/// branches already carry its `where` hint, so it is not folded in twice.
fn openSourceCols(env: *Env, rd: ast.Read, hints: []const ast.Hint, project: ?[][]const u8) !driver.Source {
    if (std.mem.eql(u8, rd.connector, "request")) {
        const body = env.request_body orelse
            return planErr(env.diag, "`read request` is only available when serving HTTP (@http)");
        const declared: ?[]const types.BodyCol = if (rd.form == .request) rd.form.request else null;
        var reject: []const u8 = "";
        const s = request.RequestSource.open(env.gpa, body, declared, env.arena, &reject) catch |e| {
            if (e == error.BodySchemaViolation)
                return planErr(env.diag, try std.fmt.allocPrint(env.arena, "request body rejected: {s}", .{reject}));
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "could not parse request body as JSON: {s}", .{@errorName(e)}));
        };
        return s.source();
    }
    if (std.mem.eql(u8, rd.connector, "buffer")) {
        const ref = rd.form.buffer;
        var dir = ref.dir;
        var declared: ?[]const types.BodyCol = null;
        if (env.buffer_decl) |decl| {
            if (std.mem.eql(u8, decl.name, ref.name)) {
                if (dir.len == 0) dir = decl.dir;
                declared = decl.schema;
            }
        }
        if (dir.len == 0)
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "buffer `{s}`: no INTO BUFFER declaration in this script — name its directory with AT '<dir>'", .{ref.name}));
        const s = BufferSource.open(env.gpa, dir, ref.name, declared, env.buffer_segment) catch |e| switch (e) {
            error.BufferEmpty => return planErr(env.diag, try std.fmt.allocPrint(env.arena, "buffer `{s}` at {s}: no segments to replay (and no declared schema)", .{ ref.name, dir })),
            else => return planErr(env.diag, try std.fmt.allocPrint(env.arena, "buffer `{s}` at {s}: {s}", .{ ref.name, dir, @errorName(e) })),
        };
        return s.source();
    }
    if (std.mem.eql(u8, rd.connector, "http")) {
        if (rd.form != .path) return planErr(env.diag, "read http needs a quoted URL");
        var hopts = http_client.optsFromHints(hints);
        hopts.logger = env.log;
        const s = http_client.HttpSource.open(env.arena, env.gpa, rd.form.path, hopts) catch |e|
            return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "http read failed for `{s}` ({s})", .{ rd.form.path, @errorName(e) }));
        return s.source();
    }
    if (std.mem.eql(u8, rd.connector, "unit")) {
        const s = gen.UnitSource.open(env.gpa) catch return error.OutOfMemory;
        return s.source();
    }
    if (std.mem.eql(u8, rd.connector, "range")) {
        const r = rd.form.range;
        const s = gen.RangeSource.open(env.gpa, try rangeBound(env, r.lo), try rangeBound(env, r.hi)) catch
            return error.OutOfMemory;
        return s.source();
    }
    if (std.mem.eql(u8, rd.connector, "csv")) {
        if (rd.form != .path) return planErr(env.diag, "read csv needs a quoted path");
        var fdiag = analyze.Diag{};
        const want = analyze.formatFromHints(hints, &fdiag) catch
            return planErr(env.diag, try env.arena.dupe(u8, fdiag.msg));
        try guardFileFormat(env, rd.form.path, want, "read");
        const fr = try resolveFolder(env, rd.form.path, hints);
        env.folder_memo = null;
        if (fr) |f| if (f.kind == .parquet) return (try openParquetFolder(env, rd.form.path, f.files, null)).source();
        const boxed = csv.splitCodec(rd.form.path).codec != .none or csv.splitArchive(rd.form.path) != null;
        const rfmt = want orelse (if (!boxed and pqdecode.Reader.isPath(rd.form.path)) analyze.FileFormat.parquet else analyze.FileFormat.csv);
        if (rfmt == .parquet) {
            const pr = pqdecode.Reader.open(env.arena, rd.form.path) catch |e|
                return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not read parquet `{s}` ({s})", .{ rd.form.path, try pathFail(env.arena, rd.form.path, e) }));
            noteParquet(env, pr);
            return pr.source();
        }
        if (analyze.readFormat(rd.form.path, want) == .arrow) return openArrow(env, rd.form.path, null);
        if (analyze.readFormat(rd.form.path, want) == .xlsx) return openXlsx(env, rd.form.path, hints);
        var ddiag = analyze.Diag{};
        const d = analyze.dialectFromHints(hints, &ddiag) catch
            return planErr(env.diag, try env.arena.dupe(u8, ddiag.msg));
        if (analyze.archiveProblem(env.arena, rd.form.path, want, true)) |why|
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "cannot read `{s}`: {s}", .{ rd.form.path, why }));
        const opened = if (fr) |f| csv.CsvReader.openList(env.arena, f.files, d) else csv.CsvReader.open(env.arena, rd.form.path, d);
        const reader = opened catch |e| {
            if (e == azure.Error.AzureEmptyPrefix)
                return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "no blobs under prefix `{s}`", .{rd.form.path}));
            if (e == error.NoCsvInFolder)
                return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "no .csv, .tsv or .txt file in folder `{s}`", .{rd.form.path}));
            if (e == error.EmptyFolder)
                return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "no files in folder `{s}`", .{rd.form.path}));
            if (e == s3.Error.S3EmptyPrefix)
                return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "no objects under prefix `{s}`", .{rd.form.path}));
            const what: []const u8 = if (csv.splitArchive(rd.form.path) != null) "archive" else "input CSV";
            return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not open {s} `{s}` ({s})", .{ what, rd.form.path, try pathFail(env.arena, rd.form.path, e) }));
        };
        if (project) |p| try reader.project(p);
        return reader.source();
    }
    const conn = env.connections.get(rd.connector) orelse
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "unknown connection `{s}`", .{rd.connector}));
    var rd_eff = rd;
    if (forHintName(hints, "where")) |wh| {
        if (wh.len > 0 and !std.mem.eql(u8, wh, rd.where)) {
            rd_eff.where = if (rd.where.len > 0)
                try std.fmt.allocPrint(env.arena, "({s}) AND ({s})", .{ wh, rd.where })
            else
                wh;
        }
    }
    if (rd_eff.where.len > 0 and !std.mem.eql(u8, conn.connector, "http"))
        if (env.scan) |t| driver.ScanTally.add(&t.sql_filters, 1);
    if (std.mem.eql(u8, conn.connector, "http")) {
        if (rd.form != .path) return planErr(env.diag, "reading an http connection needs a path: conn.GET('/path') or a CREATE RESOURCE");
        var auth: []const u8 = "";
        for (conn.config) |attr| {
            if (std.mem.eql(u8, attr.key, "auth")) auth = try evalCfgStr(env, attr.value);
        }
        var kvs = std.array_list.Managed(http_client.KV).init(env.arena);
        for (conn.config) |attr| {
            if (!httpAttrUsed(conn, auth, attr.key)) continue;
            try kvs.append(.{ .key = attr.key, .value = try evalCfgStr(env, attr.value) });
        }
        var errmsg: []const u8 = "";
        const cc = http_client.connFromKvs(env.arena, kvs.items, &errmsg) catch
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "http connection `{s}`: {s}", .{ rd.connector, errmsg }));
        const all = try std.mem.concat(env.arena, ast.Hint, &.{ conn.hints, hints });
        var hopts = http_client.optsFromHints(all);
        hopts.logger = env.log;
        const path = try http_client.withQuery(env.arena, rd.form.path, try queryParams(env.arena, all));
        const s = http_client.HttpSource.openConn(env.arena, env.gpa, cc, path, hopts) catch |e|
            return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "http read failed for `{s}` ({s})", .{ path, @errorName(e) }));
        return s.source();
    }
    if (sqlConnInfo(conn)) |info| {
        const cfg = try resolveDbConfig(env, conn, info.port);
        const query = try readSql(env, rd_eff);
        env.log.log(.debug, "sql read ({s}): {s}", .{ rd.connector, query });
        switch (info.kind) {
            inline else => |k| {
                const c = SqlDriver(k).connect(env.gpa, cfg) catch |e|
                    return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "{s} connect failed: {s}", .{ conn.connector, try connectWhy(env.arena, conn.connector, e) }));
                const s = sql.Source.open(env.gpa, c.sqlConn(), query) catch |e| {
                    defer c.close();
                    return planErr(env.diag, try std.fmt.allocPrint(env.arena, "{s} read failed ({s}): {s}", .{ conn.connector, @errorName(e), c.last_error }));
                };
                s.report = try readReport(env, conn.connector);
                env.sql_desc = try sqlDescFor(env, info.kind, info.dialect, cfg, query, rd_eff);
                return s.source();
            },
        }
    }
    return planErr(env.diag, try std.fmt.allocPrint(env.arena, "unsupported source connector `{s}`", .{conn.connector}));
}

/// Params substitute as literals; a leading `-` is folded here since `substExpr`
/// does not.
fn rangeBound(env: *Env, e: *const ast.Expr) !i64 {
    const r = try analyze.substExpr(env.arena, e, env.params_expr);
    switch (r.*) {
        .int_lit => |v| return v,
        .unary => |u| if (u.op == .neg and u.e.* == .int_lit) return -u.e.int_lit,
        else => {},
    }
    return planErr(env.diag, "RANGE bounds must be integer literals or integer params");
}

/// A connect failure's error name, with the server's or identity provider's own words
/// when they refused the login.
fn connectWhy(arena: std.mem.Allocator, connector: []const u8, e: anyerror) ![]const u8 {
    if (e == error.LoginFailed and std.mem.eql(u8, connector, "sqlserver") and tds.lastError().len > 0)
        return std.fmt.allocPrint(arena, "{s}: {s}", .{ @errorName(e), tds.lastError() });
    if (e == error.AadTokenFailed and aad.lastError().len > 0)
        return std.fmt.allocPrint(arena, "{s}: {s}", .{ @errorName(e), aad.lastError() });
    return @errorName(e);
}

/// Azure AD (ROPC token, FEDAUTH) for `auth = aad`, NTLMv2 or Kerberos for
/// `ntlm` / `kerberos`, else a SQL login.
fn tdsConnect(gpa: std.mem.Allocator, cfg_in: DbConfig) !*tds.Conn {
    var cfg = cfg_in;
    const hi = ssrp.splitHostInstance(cfg.host);
    if (hi.instance) |inst| {
        cfg.host = hi.host;
        if (!cfg.port_explicit) cfg.port = try ssrp.resolveInstancePort(gpa, hi.host, inst);
    }
    if (cfg.auth == .sql) return tds.Conn.connect(gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database, cfg.tls);
    const mode: sql.TlsMode = if (cfg.tls == .off) .require else cfg.tls;
    if (cfg.auth == .ntlm) return tds.Conn.connectNtlm(gpa, cfg.host, cfg.port, ntlmCredential(cfg), cfg.database, mode);
    if (cfg.auth == .kerberos) {
        var realm_buf: [256]u8 = undefined;
        var spn_buf: [300]u8 = undefined;
        const cred = krb5.credential(cfg.user, cfg.password, cfg.realm, cfg.kdc, &realm_buf) orelse return error.LoginFailed;
        const spn = if (cfg.spn.len > 0) cfg.spn else try std.fmt.bufPrint(&spn_buf, "MSSQLSvc/{s}:{d}", .{ cfg.host, cfg.port });
        return tds.Conn.connectKerberos(gpa, cfg.host, cfg.port, cred, spn, cfg.database, mode);
    }
    if (cfg.token.len > 0) return tds.Conn.connectAad(gpa, cfg.host, cfg.port, cfg.token, cfg.database, mode);
    const client_id = if (cfg.client_id.len > 0) cfg.client_id else aad.ado_client_id;
    var rbuf: ?[]u8 = null;
    defer if (rbuf) |b| gpa.free(b);
    const resource = if (cfg.resource.len > 0) cfg.resource else if (std.mem.endsWith(u8, cfg.host, ".dynamics.com")) blk: {
        rbuf = try std.fmt.allocPrint(gpa, "https://{s}", .{cfg.host});
        break :blk rbuf.?;
    } else aad.sql_resource;
    const token = try aad.passwordToken(gpa, client_id, cfg.user, cfg.password, resource);
    defer gpa.free(token);
    return tds.Conn.connectAad(gpa, cfg.host, cfg.port, token, cfg.database, mode);
}

/// `user = 'DOMAIN\me'` carries the domain inline; an explicit `domain` wins,
/// but the prefix is stripped from the user either way.
fn ntlmCredential(cfg: DbConfig) ntlm.Credential {
    var domain = cfg.domain;
    var user = cfg.user;
    if (std.mem.indexOfScalar(u8, user, '\\')) |i| {
        if (domain.len == 0) domain = user[0..i];
        user = user[i + 1 ..];
    }
    return .{ .domain = domain, .user = user, .password = cfg.password };
}

/// `f` fetches each value. Generic because a lenient offline fetcher once existed;
/// the seam is kept so one can return. `fe_host`/`fe_port` are StarRocks spellings.
fn parseDbConfig(conn: ast.Connection, default_port: u16, f: anytype) anyerror!DbConfig {
    var cfg = DbConfig{ .port = default_port };
    for (conn.config) |attr| {
        const k = attr.key;
        if (eqlAny(k, &.{ "port", "fe_port" })) {
            if (try f.port(attr.value)) |p| {
                cfg.port = p;
                cfg.port_explicit = true;
            }
            continue;
        }
        if (!eqlAny(k, &.{ "host", "fe_host", "user", "password", "database", "tls", "auth", "domain", "realm", "kdc", "spn", "client_id", "resource", "token" })) continue;
        const v = (try f.str(attr.value)) orelse continue;
        if (eqlAny(k, &.{ "host", "fe_host" })) {
            cfg.host = v;
        } else if (std.mem.eql(u8, k, "user")) {
            cfg.user = v;
        } else if (std.mem.eql(u8, k, "password")) {
            cfg.password = v;
        } else if (std.mem.eql(u8, k, "database")) {
            cfg.database = v;
        } else if (std.mem.eql(u8, k, "tls")) {
            cfg.tls = try f.tls(v);
        } else if (std.mem.eql(u8, k, "auth")) {
            cfg.auth = try f.auth(v);
        } else if (std.mem.eql(u8, k, "domain")) {
            cfg.domain = v;
        } else if (std.mem.eql(u8, k, "realm")) {
            cfg.realm = v;
        } else if (std.mem.eql(u8, k, "kdc")) {
            cfg.kdc = v;
        } else if (std.mem.eql(u8, k, "spn")) {
            cfg.spn = v;
        } else if (std.mem.eql(u8, k, "client_id")) {
            cfg.client_id = v;
        } else if (std.mem.eql(u8, k, "resource")) {
            cfg.resource = v;
        } else if (std.mem.eql(u8, k, "token")) {
            cfg.token = v;
        }
    }
    return cfg;
}

const EnvCfg = struct {
    env: *Env,
    fn str(self: EnvCfg, e: *const ast.Expr) !?[]const u8 {
        return try evalCfgStr(self.env, e);
    }
    fn port(self: EnvCfg, e: *const ast.Expr) !?u16 {
        const p: u16 = @intCast(try evalCfgInt(self.env, e));
        return p;
    }
    fn tls(self: EnvCfg, v: []const u8) !sql.TlsMode {
        return std.meta.stringToEnum(sql.TlsMode, v) orelse
            planErr(self.env.diag, "connection `tls` must be \"off\", \"require\" or \"insecure\"");
    }
    fn auth(self: EnvCfg, v: []const u8) !DbAuth {
        return std.meta.stringToEnum(DbAuth, v) orelse
            planErr(self.env.diag, "connection `auth` must be \"sql\", \"aad\", \"ntlm\" or \"kerberos\"");
    }
};

fn resolveDbConfig(env: *Env, conn: ast.Connection, default_port: u16) !DbConfig {
    const cfg = try parseDbConfig(conn, default_port, EnvCfg{ .env = env });
    if (cfg.host.len == 0) return planErr(env.diag, "connection needs a `host`");
    if (cfg.auth == .kerberos and cfg.realm.len == 0 and std.mem.indexOfScalar(u8, cfg.user, '@') == null)
        return planErr(env.diag, "connection `auth = 'kerberos'` needs the `realm` — the domain's DNS name, as CORP.LOCAL, not its NetBIOS name — or a user written `me@CORP.LOCAL`");
    if (cfg.auth == .ntlm and cfg.tls == .off) return planErr(env.diag, "connection `auth = 'ntlm'` requires an encrypted channel: set `tls = 'require'`, or `tls = 'insecure'` for a self-signed server certificate");
    return cfg;
}

fn readSql(env: *Env, rd: ast.Read) ![]const u8 {
    const base = switch (rd.form) {
        .query => |q| q,
        .table => |t| try std.fmt.allocPrint(env.arena, "SELECT {s} FROM {s}", .{ try selectList(env, rd), try qualStr(env.arena, t) }),
        else => return planErr(env.diag, "a DB read needs `table <name>` or `query \"...\"`"),
    };
    return sqlWithWhere(env.arena, base, rd.form == .query, rd.where);
}

fn selectList(env: *Env, rd: ast.Read) ![]const u8 {
    if (rd.cols.len == 0) return "*";
    const conn = env.connections.get(rd.connector) orelse return "*";
    const info = sqlConnInfo(conn) orelse return "*";
    return selectListFor(env.arena, info.dialect, rd.cols);
}

pub fn selectListFor(arena: std.mem.Allocator, dialect: sql.Dialect, cols: []const []const u8) ![]const u8 {
    if (cols.len == 0) return "*";
    var out = std.array_list.Managed(u8).init(arena);
    for (cols, 0..) |c, i| {
        if (i > 0) try out.appendSlice(", ");
        try out.appendSlice(try sql.quoteIdent(arena, dialect, c));
    }
    return out.toOwnedSlice();
}

/// After a join an unqualified name may be the other side's, so only the table's
/// own columns are kept, learnt from a zero-row probe; a failed probe reads all.
pub fn projectSqlRead(env: *Env, stages: []const ast.Stage) ![]const ast.Stage {
    if (stages.len == 0 or stages[0].node != .read) return stages;
    const rd = stages[0].node.read;
    if (rd.form != .table or rd.cols.len > 0) return stages;
    const conn = env.connections.get(rd.connector) orelse return stages;
    if (sqlConnInfo(conn) == null) return stages;
    const cols = (try projectedColumns(env, stages[1..])) orelse blk: {
        const except = starExcept(stages[1..]) orelse return stages;
        break :blk (try exceptColumns(env, rd, stages[0].hints, except)) orelse return stages;
    };
    if (cols.len == 0) return stages;
    for (cols) |c| if (std.mem.indexOfScalar(u8, c, '.') != null) return stages;
    var own = cols;
    for (stages[1..]) |st| if (st.node == .join) {
        own = (try tableColumnsAmong(env, rd, stages[0].hints, cols)) orelse return stages;
        break;
    };
    if (own.len == 0) return stages;
    const out = try env.arena.dupe(ast.Stage, stages);
    var nrd = rd;
    nrd.cols = own;
    out[0].node = .{ .read = nrd };
    return out;
}

fn tableColumnsAmong(env: *Env, rd: ast.Read, hints: []const ast.Hint, names: []const []const u8) !?[]const []const u8 {
    var probe = rd;
    probe.where = "1 = 0";
    probe.cols = &.{};
    const src = openSourceProjected(env, probe, hints, null, &.{}) catch return null;
    defer src.close();
    var out = std.array_list.Managed([]const u8).init(env.arena);
    for (src.schema().fields) |f| {
        for (names) |n| if (std.ascii.eqlIgnoreCase(n, f.name)) {
            try out.append(try env.arena.dupe(u8, f.name));
            break;
        };
    }
    return try out.toOwnedSlice();
}

/// The names a `SELECT * EXCEPT (...)` right after the read leaves out, when that
/// select has no other star.
fn starExcept(after: []const ast.Stage) ?[]const []const u8 {
    if (after.len == 0 or after[0].node != .select) return null;
    var found: ?[]const []const u8 = null;
    for (after[0].node.select) |it| switch (it) {
        .star_except => |names| found = names,
        .star, .star_rename => return null,
        else => {},
    };
    return found;
}

pub fn exceptColumns(env: *Env, rd: ast.Read, hints: []const ast.Hint, except: []const []const u8) !?[]const []const u8 {
    var probe = rd;
    probe.where = "1 = 0";
    probe.cols = &.{};
    const src = openSourceProjected(env, probe, hints, null, &.{}) catch return null;
    defer src.close();
    const schema = src.schema();
    var out = std.array_list.Managed([]const u8).init(env.arena);
    for (schema.fields) |f| {
        var drop = false;
        for (except) |x| if (std.ascii.eqlIgnoreCase(x, f.name)) {
            drop = true;
        };
        if (!drop) try out.append(try env.arena.dupe(u8, f.name));
    }
    if (out.items.len == 0 or out.items.len == schema.fields.len) return null;
    return try out.toOwnedSlice();
}

/// Table reads get a plain `WHERE`; query reads are wrapped as a subquery. An
/// empty predicate (a `${var}` that rendered empty) means a full scan.
pub fn sqlWithWhere(arena: std.mem.Allocator, base: []const u8, is_query: bool, where: []const u8) ![]const u8 {
    if (where.len == 0) return base;
    if (is_query) return std.fmt.allocPrint(arena, "SELECT * FROM ({s}) _w WHERE {s}", .{ base, where });
    return std.fmt.allocPrint(arena, "{s} WHERE {s}", .{ base, where });
}

fn sqlDescFor(env: *Env, kind: SqlKind, dialect: sql.Dialect, cfg: DbConfig, base_sql: []const u8, rd: ast.Read) !SqlDesc {
    const table: ?[]const u8 = switch (rd.form) {
        .table => |t| try qualStr(env.arena, t),
        else => null,
    };
    return .{ .kind = kind, .dialect = dialect, .cfg = cfg, .base_sql = base_sql, .table = table, .read = rd };
}

/// Recomputed rather than read from `env.sql_desc`, which is last-writer-wins: a
/// join plans its build side after the probe read. Null for a non-splittable read.
pub fn sqlDescForStage(env: *Env, stage: ast.Stage) !?SqlDesc {
    if (stage.node != .read) return null;
    var rd = stage.node.read;
    if (rd.form != .table and rd.form != .query) return null;
    const conn = env.connections.get(rd.connector) orelse return null;
    const info = sqlConnInfo(conn) orelse return null;
    if (forHintName(stage.hints, "where")) |wh| {
        if (wh.len > 0 and !std.mem.eql(u8, wh, rd.where)) {
            rd.where = if (rd.where.len > 0)
                try std.fmt.allocPrint(env.arena, "({s}) AND ({s})", .{ wh, rd.where })
            else
                wh;
        }
    }
    const cfg = try resolveDbConfig(env, conn, info.port);
    return try sqlDescFor(env, info.kind, info.dialect, cfg, try readSql(env, rd), rd);
}

const SplitHints = struct { col: ?[]const u8 = null, count: ?usize = null, kind: ?split.KeyKind = null };
fn splitHints(stage: ast.Stage) SplitHints {
    var h = SplitHints{};
    for (stage.hints) |hint| {
        if (std.mem.eql(u8, hint.key, "split")) {
            if (hint.value == .ident) h.col = hint.value.ident;
        } else if (std.mem.eql(u8, hint.key, "splits")) {
            if (hint.value == .int and hint.value.int > 0) h.count = @intCast(hint.value.int);
        } else if (std.mem.eql(u8, hint.key, "split_kind")) {
            if (hint.value == .ident) h.kind = std.meta.stringToEnum(split.KeyKind, hint.value.ident);
        }
    }
    return h;
}

fn isPostgresCopySink(env: *Env, w: ast.Write) bool {
    return w.mode != .upsert and std.mem.eql(u8, connectorType(env, w.connector), "postgres");
}

/// Null (run serial) when the read is not splittable, no key is usable, or the
/// table is too small. A projection that dropped the split key takes it back.
pub fn planSplit(env: *Env, desc: SqlDesc, lead: ast.Stage, threads: usize, w: ast.Write) !?split.Plan {
    const hints = splitHints(lead);
    const forced = hints.col != null or hints.count != null;
    const m: usize = hints.count orelse @min(@as(usize, 64), threads * 4);
    if (m < 2) return null;
    if (!forced and isPostgresCopySink(env, w)) return null;

    var pctx = SplitCtx{ .gpa = env.gpa, .kind = desc.kind, .cfg = desc.cfg, .base_sql = desc.base_sql, .report = try readReport(env, @tagName(desc.kind)) };
    const prober = split.Prober{ .ctx = &pctx, .openFn = proberOpen };

    var key: split.Key = undefined;
    if (hints.col) |col| {
        key = .{ .col = col, .kind = hints.kind orelse .int };
    } else if (desc.table) |table| {
        const info = (try split.introspectKey(env.arena, prober, desc.dialect, table)) orelse return null;
        if (!forced and info.est_rows < split.min_rows_to_split) return null;
        key = info.key;
    } else {
        return null;
    }
    var base = desc.base_sql;
    if (desc.read.cols.len > 0) {
        var has = false;
        for (desc.read.cols) |c| if (std.ascii.eqlIgnoreCase(c, key.col)) {
            has = true;
        };
        if (!has) {
            var rd = desc.read;
            const cols = try env.arena.alloc([]const u8, rd.cols.len + 1);
            @memcpy(cols[0..rd.cols.len], rd.cols);
            cols[rd.cols.len] = key.col;
            rd.cols = cols;
            base = try readSql(env, rd);
            pctx.base_sql = base;
        }
    }
    return split.plan(env.arena, prober, desc.dialect, base, key, m);
}

fn proberOpen(ctx_ptr: *anyopaque) anyerror!sql.Conn {
    const ctx: *SplitCtx = @ptrCast(@alignCast(ctx_ptr));
    return connectSql(ctx.gpa, ctx.kind, ctx.cfg);
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
fn httpAttrUsed(conn: ast.Connection, auth: []const u8, key: []const u8) bool {
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
fn queryParams(arena: std.mem.Allocator, hints: []const ast.Hint) ![]const http_client.KV {
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

/// Per-column text and collation facts that decide which text comparisons may
/// descend. A catalog that cannot be asked answers nothing, keeping them in the engine.
pub fn columnFacts(env: *Env, rd: ast.Read) !?*const pushdown.Facts {
    if (rd.form != .table) return null;
    const conn = env.connections.get(rd.connector) orelse return null;
    const info = sqlConnInfo(conn) orelse return null;
    const cache = env.facts_cache orelse return null;
    cache.mu.lock();
    defer cache.mu.unlock();
    const ca = cache.arena.allocator();
    const key = try std.fmt.allocPrint(ca, "{s}\x00{s}", .{ rd.connector, try qualStr(ca, rd.form.table) });
    if (cache.map.get(key)) |f| return f;

    const facts = try ca.create(pushdown.Facts);
    facts.* = pushdown.Facts.init(ca);
    try cache.map.put(key, facts);
    probeFacts(env, ca, conn, info, rd.form.table.parts, facts) catch |e| {
        if (e == error.OutOfMemory) return e;
        facts.clearRetainingCapacity();
        env.log.log(.debug, "catalog facts for {s}: not available ({s}); text comparisons stay in the engine", .{ key[rd.connector.len + 1 ..], @errorName(e) });
    };
    return facts;
}

pub fn factsIfWanted(env: *Env, rd: ast.Read, dialect: sql.Dialect, stages: []const ast.Stage, schema: types.Schema, check_fields: bool, need: pushdown.Need) !?*const pushdown.Facts {
    if (rd.form != .table) return null;
    if (!try pushdown.wantsFacts(env.arena, dialect, stages, schema, check_fields, need)) return null;
    return columnFacts(env, rd);
}

/// Restores the run's diag, so a failed probe leaves no message behind.
fn probeFacts(env: *Env, ca: std.mem.Allocator, conn: ast.Connection, info: SqlConnInfo, parts: []const []const u8, out: *pushdown.Facts) !void {
    const saved = env.diag.*;
    defer env.diag.* = saved;
    const cfg = try resolveDbConfig(env, conn, info.port);
    const q = try factsQuery(env.arena, info.dialect, parts);
    const c = try connectSql(env.gpa, info.kind, cfg);
    var cur = c.queryCursor(q) catch |e| {
        c.close();
        return e;
    };
    defer cur.close();
    var scratch = std.heap.ArenaAllocator.init(env.gpa);
    defer scratch.deinit();
    while (try cur.nextBatch(scratch.allocator())) |b| {
        var r: usize = 0;
        while (r < b.len) : (r += 1) {
            const name = b.columns[0].getValue(r);
            if (name.isNull()) continue;
            try out.put(try ca.dupe(u8, name.string), .{
                .text = flag(b.columns[1].getValue(r)),
                .byte_order = flag(b.columns[2].getValue(r)),
                .pads = flag(b.columns[3].getValue(r)),
                .wide = flag(b.columns[4].getValue(r)),
            });
        }
    }
}

fn flag(v: Value) bool {
    return switch (v) {
        .int => |i| i != 0,
        .bool => |x| x,
        .string => |s| std.mem.eql(u8, std.mem.trim(u8, s, " "), "1"),
        else => false,
    };
}

/// One row per column: name, is text, compares by byte, ignores trailing spaces,
/// multi-byte. Byte order is judged from the server's own collation data (C/POSIX,
/// builtin, `_bin`/`_BIN2`), never a locale's name; musl libc locales compare bytes.
fn factsQuery(arena: std.mem.Allocator, dialect: sql.Dialect, parts: []const []const u8) ![]const u8 {
    const table = parts[parts.len - 1];
    switch (dialect) {
        .sqlserver => {
            var obj = std.array_list.Managed(u8).init(arena);
            for (parts, 0..) |p, i| {
                if (i > 0) try obj.append('.');
                try obj.append('[');
                for (p) |ch| if (ch == ']') try obj.appendSlice("]]") else try obj.append(ch);
                try obj.append(']');
            }
            return std.fmt.allocPrint(arena,
                \\SELECT c.name,
                \\ CASE WHEN c.collation_name IS NULL THEN 0 ELSE 1 END,
                \\ CASE WHEN c.collation_name LIKE '%[_]BIN2%' THEN 1
                \\      WHEN c.collation_name LIKE '%[_]BIN' AND TYPE_NAME(c.system_type_id) IN ('char', 'varchar', 'text') THEN 1 ELSE 0 END,
                \\ 1,
                \\ CASE WHEN TYPE_NAME(c.system_type_id) IN ('nchar', 'nvarchar', 'ntext') THEN 1 ELSE 0 END
                \\FROM sys.columns c WHERE c.object_id = OBJECT_ID({s})
            , .{try sqlLit(arena, obj.items)});
        },
        .postgres => {
            var obj = std.array_list.Managed(u8).init(arena);
            for (parts, 0..) |p, i| {
                if (i > 0) try obj.append('.');
                try obj.append('"');
                for (p) |ch| if (ch == '"') try obj.appendSlice("\"\"") else try obj.append(ch);
                try obj.append('"');
            }
            return std.fmt.allocPrint(arena,
                \\SELECT a.attname,
                \\ CASE WHEN t.typcategory = 'S' THEN 1 ELSE 0 END,
                \\ CASE WHEN t.typcategory <> 'S' OR t.typname = 'citext' THEN 0
                \\      WHEN co.collname IN ('C', 'POSIX', 'ucs_basic') THEN 1
                \\      WHEN prov = 'b' THEN 1
                \\      WHEN prov <> 'c' THEN 0
                \\      WHEN version() LIKE '%-musl%' THEN 1
                \\      WHEN locale IN ('C', 'POSIX', 'C.UTF-8', 'C.utf8') THEN 1
                \\      ELSE 0 END,
                \\ CASE WHEN t.typname = 'bpchar' THEN 1 ELSE 0 END,
                \\ 0
                \\FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid
                \\LEFT JOIN pg_collation co ON co.oid = a.attcollation
                \\CROSS JOIN pg_database d
                \\CROSS JOIN LATERAL (SELECT
                \\   CASE WHEN co.collname = 'default' THEN coalesce(to_jsonb(d) ->> 'datlocprovider', 'c')
                \\        ELSE coalesce(to_jsonb(co) ->> 'collprovider', 'c') END AS prov,
                \\   CASE WHEN co.collname = 'default' THEN d.datcollate ELSE to_jsonb(co) ->> 'collcollate' END AS locale) x
                \\WHERE a.attrelid = {s}::regclass AND a.attnum > 0 AND NOT a.attisdropped AND d.datname = current_database()
            , .{try sqlLit(arena, obj.items)});
        },
        .mysql => {
            const schema = if (parts.len >= 2) try sqlLit(arena, parts[parts.len - 2]) else "DATABASE()";
            return std.fmt.allocPrint(arena,
                \\SELECT c.COLUMN_NAME,
                \\ CASE WHEN c.COLLATION_NAME IS NULL THEN 0 ELSE 1 END,
                \\ CASE WHEN c.COLLATION_NAME = 'binary' OR RIGHT(c.COLLATION_NAME, 4) = '_bin' THEN 1 ELSE 0 END,
                \\ CASE WHEN co.PAD_ATTRIBUTE = 'NO PAD' THEN 0 ELSE 1 END,
                \\ CASE WHEN c.CHARACTER_SET_NAME IN ('ucs2', 'utf16', 'utf16le', 'utf32') THEN 1 ELSE 0 END
                \\FROM information_schema.COLUMNS c LEFT JOIN information_schema.COLLATIONS co ON co.COLLATION_NAME = c.COLLATION_NAME
                \\WHERE c.TABLE_SCHEMA = {s} AND c.TABLE_NAME = {s}
            , .{ schema, try sqlLit(arena, table) });
        },
        .starrocks, .doris => {
            const schema = if (parts.len >= 2) try sqlLit(arena, parts[parts.len - 2]) else "DATABASE()";
            return std.fmt.allocPrint(arena,
                \\SELECT COLUMN_NAME,
                \\ CASE WHEN LOWER(DATA_TYPE) IN ('varchar', 'char', 'string') THEN 1 ELSE 0 END,
                \\ 1,
                \\ CASE WHEN LOWER(DATA_TYPE) = 'char' THEN 1 ELSE 0 END,
                \\ 0
                \\FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = {s} AND TABLE_NAME = {s}
            , .{ schema, try sqlLit(arena, table) });
        },
    }
}

fn sqlLit(arena: std.mem.Allocator, s: []const u8) ![]const u8 {
    var out = std.array_list.Managed(u8).init(arena);
    try out.append('\'');
    for (s) |c| {
        if (c == '\'') try out.append('\'');
        try out.append(c);
    }
    try out.append('\'');
    return out.toOwnedSlice();
}

fn qualStr(arena: std.mem.Allocator, q: ast.QualName) ![]const u8 {
    if (q.parts.len == 1) return q.parts[0];
    return std.mem.join(arena, ".", q.parts);
}

/// Bare `upsert` takes its keys from the source table's primary key; a non-table
/// source gets an error pointing at `upsert on <col>`.
pub fn resolveUpsertKeys(env: *Env, w: ast.Write) !ast.Write {
    if (w.mode != .upsert or w.mode.upsert.keys.len > 0) return w;
    const desc = env.sql_desc orelse return planErr(env.diag, "`upsert` without `on <key>` infers the primary key from the source, which needs a SQL `table` read — this pipeline's source can't be introspected; name the key with `upsert on <col>`");
    const table = desc.table orelse return planErr(env.diag, "`upsert` key inference needs `read <conn> table <name>` (a `query` source has no single table to introspect); name the key with `upsert on <col>`");
    var pctx = SplitCtx{ .gpa = env.gpa, .kind = desc.kind, .cfg = desc.cfg, .base_sql = desc.base_sql, .report = try readReport(env, @tagName(desc.kind)) };
    const prober = split.Prober{ .ctx = &pctx, .openFn = proberOpen };
    const keys = split.introspectPkCols(env.arena, prober, desc.dialect, table) catch |e|
        return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not read primary key of `{s}`: {s}", .{ table, @errorName(e) }));
    if (keys.len == 0) return planErr(env.diag, try std.fmt.allocPrint(env.arena, "no primary key found on `{s}`; name the key with `upsert on <col>`", .{table}));
    env.log.log(.debug, "upsert: inferred key on {s}: {s}", .{ table, try std.mem.join(env.arena, ", ", keys) });
    var out = w;
    out.mode = .{ .upsert = .{ .keys = keys, .partial = w.mode.upsert.partial } };
    return out;
}

fn fileWriteMode(env: *Env, w: ast.Write) !driver.FileMode {
    if (w.mode != .append) return .truncate;
    const why = analyze.appendUnsupported(w.target) orelse return .append;
    return planErr(env.diag, try std.fmt.allocPrint(env.arena, "`APPEND` into `{s}` is not supported: {s}. Use `REPLACE`, write each run to its own path, or accumulate with `INTO BUFFER` and load the buffer once", .{ w.target, why }));
}

/// `run` does not analyze first; without this a `.zip` was parsed as CSV and
/// `COUNT(*)` answered with its deflate stream's newline count.
pub fn guardFileFormat(env: *Env, path: []const u8, explicit: ?analyze.FileFormat, comptime verb: []const u8) !void {
    const unwritable = if (comptime std.mem.eql(u8, verb, "write")) analyze.unwritableTarget(path, explicit) else null;
    if (unwritable orelse analyze.unreadableTarget(path, explicit)) |why|
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "cannot " ++ verb ++ " `{s}`: {s}", .{ path, why }));
}

const StdoutSink = struct {
    inner: driver.Sink,
    gpa: std.mem.Allocator,
    info: arrow.ResultInfo,
    hook: ?env_mod.ResultHook,
    cap: ?*driver.RowCap,
    log: *obs.Logger,
    rows: std.atomic.Value(u64) = .init(0),
    mu: std.Thread.Mutex = .{},

    fn wrap(env: *Env, inner: driver.Sink, info: arrow.ResultInfo, cap: ?*driver.RowCap) !driver.Sink {
        const self = try env.gpa.create(StdoutSink);
        self.* = .{ .inner = inner, .gpa = env.gpa, .info = info, .hook = env.on_result, .cap = cap, .log = env.log };
        return .{ .ptr = self, .vtable = if (cap == null and inner.canRender()) &vt_render else &vt_plain };
    }

    pub fn writeBatch(self: *StdoutSink, arena: std.mem.Allocator, b: Batch) !void {
        const c = self.cap orelse {
            try self.inner.writeBatch(arena, b);
            _ = self.rows.fetchAdd(b.len, .monotonic);
            return;
        };
        self.mu.lock();
        defer self.mu.unlock();
        const room = c.max - c.kept;
        if (b.len > room) {
            c.truncated = true;
            driver.requestStop();
        }
        const take: usize = @intCast(@min(room, b.len));
        if (take == 0) return;
        const part = if (take == b.len) b else try op.sliceBatch(arena, b, 0, take);
        try self.inner.writeBatch(arena, part);
        c.kept += take;
        _ = self.rows.fetchAdd(take, .monotonic);
    }

    pub fn close(self: *StdoutSink) !void {
        defer self.gpa.destroy(self);
        defer driver.resetStop();
        try self.inner.close();
        const truncated = if (self.cap) |c| c.truncated else false;
        if (truncated) self.log.log(.warn, "result cut at --max-rows {d}", .{self.cap.?.max});
        if (self.hook) |h| h.call(.{ .info = self.info, .rows = self.rows.load(.monotonic), .truncated = truncated });
    }

    pub fn abort(self: *StdoutSink) void {
        driver.resetStop();
        self.inner.abort();
        self.gpa.destroy(self);
    }

    const vt_plain = driver.sinkVTable(StdoutSink);
    const vt_render = blk: {
        var v = driver.sinkVTable(StdoutSink);
        v.renderBatch = struct {
            fn f(p: *anyopaque, arena: std.mem.Allocator, b: Batch) anyerror![]const u8 {
                const self: *StdoutSink = @ptrCast(@alignCast(p));
                const out = try self.inner.renderBatch(arena, b).?;
                _ = self.rows.fetchAdd(b.len, .monotonic);
                return out;
            }
        }.f;
        v.writeRendered = struct {
            fn f(p: *anyopaque, bytes: []const u8) anyerror!void {
                const self: *StdoutSink = @ptrCast(@alignCast(p));
                return self.inner.writeRendered(bytes);
            }
        }.f;
        break :blk v;
    };
};

pub fn openSink(env: *Env, w: ast.Write, schema: types.Schema) !driver.Sink {
    if (std.mem.eql(u8, w.connector, mem_connector)) {
        env.mem_sink.?.schema = try dupeSchema(env.arena, schema);
        return env.mem_sink.?.sink();
    }
    if (env.explain) return DiscardSink.sink();
    if (std.mem.eql(u8, w.connector, "stdout")) {
        var info = env.takeResult();
        var cap: ?*driver.RowCap = null;
        if (env.max_rows) |n| {
            cap = try env.arena.create(driver.RowCap);
            cap.?.* = .{ .max = n };
            info.cap = cap;
        }
        driver.resetStop();
        const inner = try openStdoutSink(env, schema, info);
        if (env.on_result == null and cap == null) return inner;
        errdefer inner.abort();
        return StdoutSink.wrap(env, inner, info, cap);
    }
    return openTargetSink(env, w, schema);
}

fn openStdoutSink(env: *Env, schema: types.Schema, info: arrow.ResultInfo) !driver.Sink {
    switch (env.stdout_format) {
        .json => {
            const writer = JsonWriter.open(env.gpa, schema) catch
                return planErr(env.diag, "could not open stdout json writer");
            return writer.sink();
        },
        .arrow => {
            const writer = ArrowWriter.open(env.gpa, schema, info) catch |e| switch (e) {
                error.ArrowUnsupportedType => return planErr(env.diag, "arrow output cannot carry an array or struct column"),
                else => return planErr(env.diag, "could not open stdout arrow writer"),
            };
            return writer.sink();
        },
        .csv, .tsv => {
            const delim: u8 = if (env.stdout_format == .tsv) '\t' else ',';
            const writer = csv.CsvWriter.openStdout(env.arena, schema, .{ .delim = delim }) catch
                return planErr(env.diag, "could not open stdout csv writer");
            return writer.sink();
        },
        .table => {
            const writer = TableWriter.open(env.gpa, schema) catch
                return planErr(env.diag, "could not open stdout table");
            return writer.sink();
        },
    }
}

fn openTargetSink(env: *Env, w: ast.Write, schema: types.Schema) !driver.Sink {
    if (std.mem.eql(u8, w.connector, "csv")) {
        const fmode = try fileWriteMode(env, w);
        if ((env.fmt_out == null and xlsx.isPath(w.target)) or env.fmt_out == .xlsx)
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "cannot write `{s}`: basalt reads Excel workbooks but does not write them; write a `.csv` or `.parquet`", .{w.target}));
        const wfmt = env.fmt_out orelse (if (pqwrite.Writer.isPath(w.target)) analyze.FileFormat.parquet else if (arrowread.isPath(w.target)) analyze.FileFormat.arrow else analyze.FileFormat.csv);
        if (wfmt == .arrow) {
            const aw = ArrowFileSink.open(env.gpa, w.target, schema) catch |e| switch (e) {
                error.ArrowRemoteSink => return planErr(env.diag, try std.fmt.allocPrint(env.arena, "cannot write `{s}`: Arrow IPC is written to a local file", .{w.target})),
                error.ArrowUnsupportedType => return planErr(env.diag, "Arrow IPC cannot carry an array or struct column"),
                else => return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not open output Arrow IPC `{s}` ({s})", .{ w.target, try pathFail(env.arena, w.target, e) })),
            };
            return aw.sink();
        }
        if (wfmt == .parquet) {
            const pw = pqwrite.Writer.open(env.arena, w.target, schema, .snappy, fmode) catch |e|
                return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not open output parquet `{s}` ({s})", .{ w.target, try pathFail(env.arena, w.target, e) }));
            return pw.sink();
        }
        const writer = csv.CsvWriter.open(env.arena, w.target, schema, fmode, env.csv_out) catch |e|
            return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not open output CSV `{s}` ({s})", .{ w.target, try pathFail(env.arena, w.target, e) }));
        return writer.sink();
    }
    const conn = env.connections.get(w.connector) orelse
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "unknown connection `{s}`", .{w.connector}));
    if (streamload.Flavor.of(conn.connector)) |flavor| {
        const cfg = try resolveStreamLoadConfig(env, conn, flavor);
        const s = streamload.StreamLoadSink.open(env.gpa, cfg, w.target, schema, w.mode) catch |e|
            return srOpenErr(env, e, try std.fmt.allocPrint(env.arena, "{s} sink open failed", .{flavor.name()}));
        s.logger = env.log;
        s.errctx = env.errctx;
        return s.sink();
    }
    if (sqlConnInfo(conn)) |info| {
        const cfg = try resolveDbConfig(env, conn, info.port);
        switch (info.kind) {
            inline else => |k| {
                const c = SqlDriver(k).connect(env.gpa, cfg) catch |e|
                    return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "{s} connect failed: {s}", .{ conn.connector, try connectWhy(env.arena, conn.connector, e) }));
                return openBulkOrInsert(env.gpa, c, SqlDriver(k).Bulk, info.dialect, w.target, schema, w.mode, try redialFor(env.arena, info.kind, cfg)) catch |e| {
                    defer c.close();
                    return planErr(env.diag, try std.fmt.allocPrint(env.arena, "{s} sink failed ({s}): {s}", .{ conn.connector, @errorName(e), c.last_error }));
                };
            },
        }
    }
    return planErr(env.diag, try std.fmt.allocPrint(env.arena, "unsupported sink connector `{s}`", .{conn.connector}));
}

fn resolveStreamLoadConfig(env: *Env, conn: ast.Connection, flavor: streamload.Flavor) !streamload.Config {
    var cfg = streamload.Config{ .flavor = flavor, .database = "", .errctx = env.errctx };
    for (conn.config) |attr| {
        const k = attr.key;
        if (eqlAny(k, &.{ "host", "fe_host" })) {
            cfg.fe_host = try evalCfgStr(env, attr.value);
        } else if (eqlAny(k, &.{ "port", "fe_port" })) {
            cfg.fe_port = @intCast(try evalCfgInt(env, attr.value));
        } else if (eqlAny(k, &.{ "be_url", "load_url" })) {
            cfg.load_url = try evalCfgStr(env, attr.value);
        } else if (std.mem.eql(u8, k, "database")) {
            cfg.database = try evalCfgStr(env, attr.value);
        } else if (std.mem.eql(u8, k, "user")) {
            cfg.user = try evalCfgStr(env, attr.value);
        } else if (std.mem.eql(u8, k, "password")) {
            cfg.password = try evalCfgStr(env, attr.value);
        } else if (std.mem.eql(u8, k, "buckets")) {
            cfg.buckets = @intCast(try evalCfgInt(env, attr.value));
        } else if (std.mem.eql(u8, k, "replication_num")) {
            cfg.replication_num = @intCast(try evalCfgInt(env, attr.value));
        } else if (std.mem.eql(u8, k, "auto_create")) {
            cfg.auto_create = try evalCfgBool(env, attr.value);
        } else if (std.mem.eql(u8, k, "label_prefix")) {
            cfg.label_prefix = try evalCfgStr(env, attr.value);
        }
    }
    if (cfg.database.len == 0) return planErr(env.diag, try std.fmt.allocPrint(env.arena, "{s} connection needs a `database`", .{flavor.name()}));
    if (env.load_label_prefix) |lp| cfg.label_prefix = lp;
    if (env.load_run_id) |rid| cfg.run_id = rid;
    return cfg;
}

/// A password is optional (a key logs in without one); an unknown option is an
/// error naming the known ones.
pub fn registerSftp(env: *Env, conn: ast.Connection) !void {
    if (!std.mem.eql(u8, conn.connector, "sftp")) return;
    var c = sftp.Conn{ .host = "" };
    for (conn.config) |attr| {
        const k = attr.key;
        if (std.mem.eql(u8, k, "port")) {
            c.port = std.math.cast(u16, try evalCfgInt(env, attr.value)) orelse return planErr(env.diag, "sftp connection `port` is out of range");
            continue;
        }
        const v = optCfgStr(env, attr.value) catch |e| return e;
        if (std.mem.eql(u8, k, "host")) {
            c.host = v orelse "";
        } else if (std.mem.eql(u8, k, "user")) {
            c.user = v;
        } else if (std.mem.eql(u8, k, "password")) {
            c.password = v;
        } else if (std.mem.eql(u8, k, "key_file")) {
            c.key_file = v;
        } else if (std.mem.eql(u8, k, "key_passphrase")) {
            c.key_passphrase = v;
        } else if (std.mem.eql(u8, k, "known_hosts")) {
            c.known_hosts = v;
        } else if (std.mem.eql(u8, k, "host_key")) {
            c.host_key = v;
        } else return planErr(env.diag, try std.fmt.allocPrint(env.arena, "sftp connection `{s}`: unknown option `{s}` (host, port, user, password, key_file, key_passphrase, known_hosts, host_key)", .{ conn.name, k }));
    }
    if (c.host.len == 0) return planErr(env.diag, try std.fmt.allocPrint(env.arena, "sftp connection `{s}` needs a `host`", .{conn.name}));
    try sftp.register(conn.name, c);
}

/// With a `share`, paths under the name are inside it; without one the first path
/// part names the share. An unknown option is an error naming the known ones.
pub fn registerSmb(env: *Env, conn: ast.Connection) !void {
    if (!std.mem.eql(u8, conn.connector, "smb")) return;
    var c = smb.Conn{ .host = "" };
    for (conn.config) |attr| {
        const k = attr.key;
        if (std.mem.eql(u8, k, "port")) {
            c.port = std.math.cast(u16, try evalCfgInt(env, attr.value)) orelse return planErr(env.diag, "smb connection `port` is out of range");
            continue;
        }
        const v = optCfgStr(env, attr.value) catch |e| return e;
        if (std.mem.eql(u8, k, "host")) {
            c.host = v orelse "";
        } else if (std.mem.eql(u8, k, "user")) {
            c.user = v;
        } else if (std.mem.eql(u8, k, "password")) {
            c.password = v;
        } else if (std.mem.eql(u8, k, "domain")) {
            c.domain = v;
        } else if (std.mem.eql(u8, k, "share")) {
            c.share = if (v) |sh| std.mem.trim(u8, sh, "/\\") else null;
        } else if (std.mem.eql(u8, k, "realm")) {
            c.realm = if (v) |r| try std.ascii.allocUpperString(env.arena, r) else null;
        } else if (std.mem.eql(u8, k, "kdc")) {
            c.kdc = v;
        } else if (std.mem.eql(u8, k, "spn")) {
            c.spn = v;
        } else if (std.mem.eql(u8, k, "auth")) {
            const a = v orelse "";
            c.auth = if (std.ascii.eqlIgnoreCase(a, "kerberos")) .kerberos else if (std.ascii.eqlIgnoreCase(a, "ntlm")) .ntlm else return planErr(env.diag, try std.fmt.allocPrint(env.arena, "smb connection `{s}`: `auth` is kerberos or ntlm, not `{s}`", .{ conn.name, a }));
        } else return planErr(env.diag, try std.fmt.allocPrint(env.arena, "smb connection `{s}`: unknown option `{s}` (host, port, user, password, domain, share, auth, realm, kdc, spn)", .{ conn.name, k }));
    }
    if (c.host.len == 0) return planErr(env.diag, try std.fmt.allocPrint(env.arena, "smb connection `{s}` needs a `host`", .{conn.name}));
    if (c.auth == .kerberos and c.realm == null and std.mem.indexOfScalar(u8, c.user orelse "", '@') == null)
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "smb connection `{s}`: Kerberos needs the `realm` — the domain's DNS name, as CORP.LOCAL, not its NetBIOS name — or a user written `me@CORP.LOCAL`", .{conn.name}));
    try smb.register(conn.name, c);
}

fn optCfgStr(env: *Env, expr: *const ast.Expr) !?[]const u8 {
    if (expr.* == .call) {
        const c = expr.call;
        if ((std.mem.eql(u8, c.name, "env") or std.mem.eql(u8, c.name, "secret")) and c.args.len == 1 and c.args[0].* == .str_lit)
            return std.process.getEnvVarOwned(env.arena, c.args[0].str_lit) catch null;
    }
    return try evalCfgStr(env, expr);
}

fn evalCfgStr(env: *Env, expr: *const ast.Expr) ![]const u8 {
    switch (expr.*) {
        .str_lit => |s| return s,
        .int_lit => |i| return std.fmt.allocPrint(env.arena, "{d}", .{i}),
        .bool_lit => |b| return if (b) "true" else "false",
        .call => |c| {
            if ((std.mem.eql(u8, c.name, "env") or std.mem.eql(u8, c.name, "secret")) and
                c.args.len == 1 and c.args[0].* == .str_lit)
            {
                const name = c.args[0].str_lit;
                return std.process.getEnvVarOwned(env.arena, name) catch
                    return planErr(env.diag, try std.fmt.allocPrint(env.arena, "env var `{s}` is not set", .{name}));
            }
            return planErr(env.diag, "config value must be a literal or env()/secret()");
        },
        else => return planErr(env.diag, "config value must be a literal or env()/secret()"),
    }
}

fn evalCfgInt(env: *Env, expr: *const ast.Expr) !i64 {
    return switch (expr.*) {
        .int_lit => |i| i,
        .str_lit => |s| std.fmt.parseInt(i64, s, 10) catch return planErr(env.diag, "invalid integer config value"),
        else => planErr(env.diag, "config value must be an integer"),
    };
}

fn evalCfgBool(env: *Env, expr: *const ast.Expr) !bool {
    return switch (expr.*) {
        .bool_lit => |b| b,
        .str_lit => |s| std.mem.eql(u8, s, "true"),
        else => planErr(env.diag, "config value must be a bool"),
    };
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

const LitCfg = struct {
    fn str(_: LitCfg, e: *const ast.Expr) !?[]const u8 {
        return if (e.* == .str_lit) e.str_lit else null;
    }
    fn port(_: LitCfg, e: *const ast.Expr) !?u16 {
        return if (e.* == .int_lit) @intCast(e.int_lit) else null;
    }
    fn tls(_: LitCfg, _: []const u8) !sql.TlsMode {
        return .off;
    }
    fn auth(_: LitCfg, _: []const u8) !DbAuth {
        return .sql;
    }
};

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

test "a starrocks connection reads over MySQL: FE host and port, default 9030" {
    var host = ast.Expr{ .str_lit = "fe.internal" };
    var port = ast.Expr{ .int_lit = 9031 };
    var user = ast.Expr{ .str_lit = "etl" };
    const pos = ast.Pos{ .line = 0, .col = 0 };
    const conn = ast.Connection{ .name = "sr", .connector = "starrocks", .pos = pos, .config = &.{
        .{ .key = "fe_host", .value = &host, .pos = pos },
        .{ .key = "user", .value = &user, .pos = pos },
    } };
    const info = sqlConnInfo(conn).?;
    try std.testing.expectEqual(SqlKind.mysql, info.kind);
    try std.testing.expectEqual(sql.Dialect.starrocks, info.dialect);

    const cfg = try parseDbConfig(conn, info.port, LitCfg{});
    try std.testing.expectEqualStrings("fe.internal", cfg.host);
    try std.testing.expectEqualStrings("etl", cfg.user);
    try std.testing.expectEqual(@as(u16, 9030), cfg.port);

    const explicit = ast.Connection{ .name = "sr", .connector = "starrocks", .pos = pos, .config = &.{
        .{ .key = "host", .value = &host, .pos = pos },
        .{ .key = "fe_port", .value = &port, .pos = pos },
    } };
    try std.testing.expectEqual(@as(u16, 9031), (try parseDbConfig(explicit, info.port, LitCfg{})).port);
}

test "sqlWithWhere: table appends WHERE, query wraps, empty is a no-op" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings(
        "SELECT * FROM SC1010 WHERE S_T_A_M_P_ >= '2026-05-09'",
        try sqlWithWhere(a, "SELECT * FROM SC1010", false, "S_T_A_M_P_ >= '2026-05-09'"),
    );
    try std.testing.expectEqualStrings(
        "SELECT * FROM (SELECT id FROM t WHERE x = 1) _w WHERE id > 5",
        try sqlWithWhere(a, "SELECT id FROM t WHERE x = 1", true, "id > 5"),
    );
    try std.testing.expectEqualStrings(
        "SELECT * FROM SC1010",
        try sqlWithWhere(a, "SELECT * FROM SC1010", false, ""),
    );
}

test "a projected SQL read asks for its columns, quoted per dialect; none means *" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings("*", try selectListFor(a, .sqlserver, &.{}));
    try std.testing.expectEqualStrings("[E1_NUM], [E1_VALOR]", try selectListFor(a, .sqlserver, &.{ "E1_NUM", "E1_VALOR" }));
    try std.testing.expectEqualStrings("\"id\", \"amount\"", try selectListFor(a, .postgres, &.{ "id", "amount" }));
    try std.testing.expectEqualStrings("`id`", try selectListFor(a, .mysql, &.{"id"}));
}
