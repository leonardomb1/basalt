//! Source and sink resolution: connector-name dispatch, connection config
//! (SQL, AAD, NTLM, StarRocks), split planning, and the parallel sink specs.

const std = @import("std");
const ast = @import("../lang/ast.zig");
const types = @import("../lang/types.zig");
const op = @import("../exec/op.zig");
const Batch = @import("../exec/batch.zig").Batch;
const column = @import("../exec/column.zig");
const csv = @import("../connect/csv.zig");
const pqdecode = @import("../connect/pqdecode.zig");
const pqwrite = @import("../connect/pqwrite.zig");
const JsonWriter = @import("../connect/table.zig").JsonWriter;
const ArrowWriter = @import("../connect/arrow.zig").ArrowWriter;
const TableWriter = @import("../connect/table.zig").TableWriter;
const driver = @import("../connect/driver.zig");
const starrocks = @import("../connect/starrocks.zig");
const registry = @import("../connect/registry.zig");
const tds = @import("../connect/tds.zig");
const mysql = @import("../connect/mysql.zig");
const postgres = @import("../connect/postgres.zig");
const sql = @import("../connect/sql.zig");
const request = @import("../connect/request.zig");
const http_client = @import("../connect/http_client.zig");
const aad = @import("../connect/aad.zig");
const split = @import("../connect/split.zig");
const ssrp = @import("../connect/ssrp.zig");
const ntlm = @import("../connect/ntlm.zig");
const BufferSource = @import("../connect/wal.zig").BufferSource;
const azure = @import("../connect/azure.zig");
const s3 = @import("../connect/s3.zig");
const gen = @import("../connect/gen.zig");
const parallel = @import("parallel.zig");
const analyze = @import("analyze.zig");
const obs = @import("obs.zig");

const DbAuth = @import("env.zig").DbAuth;
const DbConfig = @import("env.zig").DbConfig;
const Env = @import("env.zig").Env;
const eqlAny = @import("env.zig").eqlAny;
const forHintName = @import("env.zig").forHintName;
const pathFail = @import("env.zig").pathFail;
const planErr = @import("env.zig").planErr;
const planErrT = @import("env.zig").planErrT;
const SqlDesc = @import("env.zig").SqlDesc;
const SqlKind = @import("env.zig").SqlKind;
const srOpenErr = @import("env.zig").srOpenErr;

/// Drains every batch and keeps none: the rows must still be pulled for the
/// measured counts to be real.
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

/// Connection config carried into the split lanes (referenced via *anyopaque).
pub const SplitCtx = struct {
    gpa: std.mem.Allocator,
    kind: SqlKind,
    cfg: DbConfig,
    base_sql: []const u8,
    proj_select: ?[]const u8 = null,
    where_extra: ?[]const u8 = null,
};

/// The concrete driver for one `SqlKind`: `connect` opens the driver connection
/// (sqlserver routes through `tdsConnect` for AAD) and `Bulk` is its bulk write
/// strategy. Comptime, so callers that need the concrete conn type (bulk sinks,
/// `last_error`) can reach it via `switch (kind) { inline else => |k| ... }`.
fn SqlDriver(comptime kind: SqlKind) type {
    return switch (kind) {
        .postgres => struct {
            const Bulk = postgres.CopySink;
            fn connect(gpa: std.mem.Allocator, cfg: DbConfig) !*postgres.Conn {
                return postgres.Conn.connect(gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database, cfg.tls);
            }
        },
        .mysql => struct {
            const Bulk = mysql.LoadDataSink;
            fn connect(gpa: std.mem.Allocator, cfg: DbConfig) !*mysql.Conn {
                return mysql.Conn.connect(gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database, cfg.tls);
            }
        },
        .sqlserver => struct {
            const Bulk = tds.BulkSink;
            const connect = tdsConnect;
        },
    };
}

fn connectSql(gpa: std.mem.Allocator, kind: SqlKind, cfg: DbConfig) !sql.Conn {
    switch (kind) {
        inline else => |k| return (try SqlDriver(k).connect(gpa, cfg)).sqlConn(),
    }
}

/// `parallel.OpenSplitFn`: open a fresh source for one split predicate.
pub fn openSplitSource(ctx_ptr: *anyopaque, gpa: std.mem.Allocator, pred: []const u8) anyerror!driver.Source {
    const ctx: *SplitCtx = @ptrCast(@alignCast(ctx_ptr));
    const q = try split.wrapProjected(gpa, ctx.base_sql, ctx.proj_select, pred, ctx.where_extra);
    defer gpa.free(q);
    return openSqlQuery(ctx, gpa, q);
}

/// Open a SQL source for a ready-built lane query (one connection per call).
pub fn openSqlQuery(ctx: *const SplitCtx, gpa: std.mem.Allocator, query: []const u8) anyerror!driver.Source {
    const conn = try connectSql(gpa, ctx.kind, ctx.cfg);
    errdefer conn.close();
    const s = try sql.Source.open(gpa, conn, query);
    return s.source();
}

/// Resolved config for a per-lane StarRocks sink (DDL already done once at plan
/// time; lanes just stream-load with a shared run_id and lane-distinct labels).
const StarrocksSinkSpec = struct {
    cfg: starrocks.Config,
    target: []const u8,
    schema: types.Schema,
    mode: ast.WriteMode,
    logger: ?*obs.Logger = null,
    errctx: ?*op.ErrCtx = null,
};

/// `parallel.OpenSinkFn`: one StarRocks stream-load stream per lane.
fn openLaneStarrocksSink(ctx_ptr: *anyopaque, gpa: std.mem.Allocator, lane_idx: usize) anyerror!driver.Sink {
    const spec: *StarrocksSinkSpec = @ptrCast(@alignCast(ctx_ptr));
    var cfg = spec.cfg;
    const lp = try std.fmt.allocPrint(gpa, "{s}_l{d}", .{ spec.cfg.label_prefix, lane_idx });
    defer gpa.free(lp);
    cfg.label_prefix = lp;
    const s = try starrocks.StreamLoadSink.open(gpa, cfg, spec.target, spec.schema, spec.mode);
    s.logger = spec.logger;
    s.errctx = spec.errctx;
    return s.sink();
}

/// Resolved config for a per-lane SQL sink (reverse-ETL). DDL + any overwrite
/// DELETE run once at plan time; each lane opens its own connection and INSERTs.
/// Safe under concurrency because the source splits are disjoint key ranges, so no
/// two lanes ever write the same key (upserts never collide cross-lane).
const SqlSinkSpec = struct {
    kind: SqlKind,
    dialect: sql.Dialect,
    cfg: DbConfig,
    target: []const u8,
    schema: types.Schema,
    lane_mode: ast.WriteMode,
    redial: sql.Redial,
};

/// Read-only dial config for the INSERT sink's transient-retry reconnect.
/// Allocated in the plan arena; shared (immutably) across lanes.
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

/// Open the per-dialect write strategy from an already-connected conn: a bulk
/// loader (COPY / LOAD DATA / INSERT BULK) for append/overwrite, or the generic
/// INSERT `sql.Sink` for upsert. Centralizes the bulk-vs-INSERT rule so the serial
/// (`openSink`) and per-lane (`openLaneSqlSink`) paths can't drift. `conn` is the
/// concrete driver connection; on error the caller still owns and closes it.
/// `redial` arms the INSERT sink's transient retry; the bulk loaders are
/// mid-protocol streams (COPY/LOAD DATA/INSERT BULK) that cannot resume on a
/// fresh connection, so they stay fail-fast.
fn openBulkOrInsert(gpa: std.mem.Allocator, conn: anytype, comptime BulkSink: type, dialect: sql.Dialect, target: []const u8, schema: types.Schema, mode: ast.WriteMode, redial: ?sql.Redial) !driver.Sink {
    if (mode != .upsert) return (try BulkSink.open(gpa, conn, target, schema, mode, redial)).sink();
    return (try sql.Sink.open(gpa, conn.sqlConn(), dialect, target, schema, mode, redial)).sink();
}

/// `parallel.OpenSinkFn`: one DB stream per lane (append/overwrite → bulk loader,
/// upsert → INSERT, per `openBulkOrInsert`).
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

/// Build the parallel-sink mode for a split pipeline: a per-lane StarRocks or SQL
/// sink, or null to fall back to the shared-mutex path (CSV).
pub fn buildParallelSink(env: *Env, w: ast.Write, schema: types.Schema) !?parallel.SinkMode {
    if (try buildStarrocksSpec(env, w, schema)) |spec|
        return parallel.SinkMode{ .per_lane = .{ .open = openLaneStarrocksSink, .ctx = spec } };
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
        return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "{s} sink connect failed: {s}", .{ conn.connector, @errorName(e) }));
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

/// If `w` writes to StarRocks, run the one-time DDL/truncate now and return a spec
/// the lanes use to open their own stream-load streams. Returns null for any other
/// sink (those use the shared mutex path).
fn buildStarrocksSpec(env: *Env, w: ast.Write, schema: types.Schema) !?*StarrocksSinkSpec {
    const conn = env.connections.get(w.connector) orelse return null;
    if (!std.mem.eql(u8, conn.connector, "starrocks")) return null;

    var cfg = try resolveStarrocksConfig(env, conn);
    cfg.run_id = if (cfg.run_id != 0) cfg.run_id else @intCast(std.time.milliTimestamp());

    const setup = starrocks.StreamLoadSink.open(env.gpa, cfg, w.target, schema, w.mode) catch |e|
        return srOpenErr(env, e, "starrocks setup failed");
    setup.logger = env.log;
    setup.errctx = env.errctx;
    setup.sink().close() catch |e|
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "starrocks setup close failed: {s}", .{@errorName(e)}));
    cfg.auto_create = false;

    const spec = try env.arena.create(StarrocksSinkSpec);
    spec.* = .{ .cfg = cfg, .target = w.target, .schema = schema, .mode = if (w.mode == .overwrite) .append else w.mode, .logger = env.log, .errctx = env.errctx };
    return spec;
}

/// Gates the mmap'd parallel-CSV fast paths. A `.parquet` path shares the `csv`
/// connector but is not CSV — without this it would be memory-mapped and parsed
/// as text, silently yielding binary garbage instead of rows.
pub fn isLocalCsvRead(rd: ast.Read) bool {
    if (!std.mem.eql(u8, rd.connector, "csv")) return false;
    return switch (rd.form) {
        .path => |p| !csv.CsvReader.isUrl(p) and !pqdecode.Reader.isPath(p),
        else => false,
    };
}

pub fn isLocalParquetRead(rd: ast.Read) bool {
    if (!std.mem.eql(u8, rd.connector, "csv")) return false;
    return switch (rd.form) {
        .path => |p| !csv.CsvReader.isUrl(p) and pqdecode.Reader.isPath(p),
        else => false,
    };
}

/// Resolve a sink/source connector name to its driver type for the summary
/// (`csv`/`request` are types; a connection name maps to its `connector`).
/// Label for a *write* target: the `csv` connector covers every file sink, so
/// the format has to come from the target itself or telemetry reports parquet
/// writes as csv.
pub fn sinkLabel(env: *Env, w: ast.Write) []const u8 {
    if (std.mem.eql(u8, w.connector, "csv") and pqwrite.Writer.isPath(w.target)) return "parquet";
    return connectorType(env, w.connector);
}

/// The summary label for a source. A file read carries its format in the path (or a
/// `format` hint), not in the connector name, so it resolves separately; everything
/// else is named by its connector.
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

/// A single pre-computed batch, so a metadata answer can enter the pipeline
/// through the ordinary scan path.
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

/// `openSource`, with the column set the pipeline needs when it is known.
/// Only the parquet reader can act on it; every other source ignores it.
pub fn openSourceProjected(
    env: *Env,
    rd: ast.Read,
    hints: []const ast.Hint,
    project: ?[][]const u8,
    bounds: []const pqdecode.Bound,
) !driver.Source {
    // Track parquet readers so a top-N bound is only ever pushed into a pipeline
    // with exactly one of them; with two the single slot would be ambiguous and
    // could skip groups of the wrong file.
    const is_pq = std.mem.eql(u8, rd.connector, "csv") and rd.form == .path and
        pqdecode.Reader.isPath(rd.form.path);
    if (is_pq) {
        const pr = pqdecode.Reader.openProjected(env.arena, rd.form.path, project) catch |e|
            return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not read parquet `{s}` ({s})", .{ rd.form.path, try pathFail(env.arena, rd.form.path, e) }));
        pr.bounds = bounds;
        env.pq_readers += 1;
        env.pq_reader = pr;
        return pr.source();
    }
    return openSourceAll(env, rd, hints);
}

fn openSourceAll(env: *Env, rd: ast.Read, hints: []const ast.Hint) !driver.Source {
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
        // An explicit `format` decides, so a parquet under an unusual name is not
        // handed to the CSV parser; otherwise the extension does, as before.
        const boxed = csv.splitCodec(rd.form.path).codec != .none or csv.splitArchive(rd.form.path) != null;
        const rfmt = want orelse (if (!boxed and pqdecode.Reader.isPath(rd.form.path)) analyze.FileFormat.parquet else analyze.FileFormat.csv);
        if (rfmt == .parquet) {
            const pr = pqdecode.Reader.open(env.arena, rd.form.path) catch |e|
                return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not read parquet `{s}` ({s})", .{ rd.form.path, try pathFail(env.arena, rd.form.path, e) }));
            return pr.source();
        }
        var ddiag = analyze.Diag{};
        const d = analyze.dialectFromHints(hints, &ddiag) catch
            return planErr(env.diag, try env.arena.dupe(u8, ddiag.msg));
        // `run` never analyzed the pipeline, so it repeats the plan-time question
        // here: which member did you mean?
        if (analyze.archiveProblem(env.arena, rd.form.path, want)) |why|
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "cannot read `{s}`: {s}", .{ rd.form.path, why }));
        const reader = csv.CsvReader.open(env.arena, rd.form.path, d) catch |e| {
            // A mistyped prefix and a truly empty one are the same listing; say
            // which prefix came back empty rather than blaming the CSV parser.
            if (e == azure.Error.AzureEmptyPrefix)
                return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "no blobs under prefix `{s}`", .{rd.form.path}));
            if (e == s3.Error.S3EmptyPrefix)
                return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "no objects under prefix `{s}`", .{rd.form.path}));
            // A malformed container is not a CSV problem, and saying so sends the
            // reader looking in the wrong place.
            const what: []const u8 = if (csv.splitArchive(rd.form.path) != null) "archive" else "input CSV";
            return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not open {s} `{s}` ({s})", .{ what, rd.form.path, try pathFail(env.arena, rd.form.path, e) }));
        };
        return reader.source();
    }
    const conn = env.connections.get(rd.connector) orelse
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "unknown connection `{s}`", .{rd.connector}));
    var rd_eff = rd;
    if (forHintName(hints, "where")) |wh| {
        if (wh.len > 0) {
            rd_eff.where = if (rd.where.len > 0)
                try std.fmt.allocPrint(env.arena, "({s}) AND ({s})", .{ wh, rd.where })
            else
                wh;
        }
    }
    if (std.mem.eql(u8, conn.connector, "http")) {
        if (rd.form != .path) return planErr(env.diag, "reading an http connection needs a quoted path");
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
        var hopts = http_client.optsFromHints(hints);
        hopts.logger = env.log;
        const s = http_client.HttpSource.openConn(env.arena, env.gpa, cc, rd.form.path, hopts) catch |e|
            return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "http read failed for `{s}` ({s})", .{ rd.form.path, @errorName(e) }));
        return s.source();
    }
    if (sqlConnInfo(conn)) |info| {
        const cfg = try resolveDbConfig(env, conn, info.port);
        const query = try readSql(env, rd_eff);
        switch (info.kind) {
            inline else => |k| {
                const c = SqlDriver(k).connect(env.gpa, cfg) catch |e|
                    return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "{s} connect failed: {s}", .{ conn.connector, @errorName(e) }));
                const s = sql.Source.open(env.gpa, c.sqlConn(), query) catch |e| {
                    defer c.close();
                    return planErr(env.diag, try std.fmt.allocPrint(env.arena, "{s} read failed ({s}): {s}", .{ conn.connector, @errorName(e), c.last_error }));
                };
                env.sql_desc = try sqlDescFor(env, info.kind, info.dialect, cfg, query, rd_eff);
                return s.source();
            },
        }
    }
    return planErr(env.diag, try std.fmt.allocPrint(env.arena, "unsupported source connector `{s}`", .{conn.connector}));
}

/// Resolve a RANGE bound to an i64 at plan time: params substitute as
/// literals; a leading `-` is folded here since substExpr doesn't.
fn rangeBound(env: *Env, e: *const ast.Expr) !i64 {
    const r = try analyze.substExpr(env.arena, e, env.params_expr);
    switch (r.*) {
        .int_lit => |v| return v,
        .unary => |u| if (u.op == .neg and u.e.* == .int_lit) return -u.e.int_lit,
        else => {},
    }
    return planErr(env.diag, "RANGE bounds must be integer literals or integer params");
}

/// Open a SQL Server connection: Azure AD (ROPC token -> FEDAUTH) for `auth =
/// aad`, Windows NTLMv2 (SSPI) for `auth = ntlm`, else a normal SQL login.
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

/// Split a Windows login into domain + user. `user = 'DOMAIN\me'` carries the
/// domain inline; an explicit `domain` option wins when both are given, but the
/// `DOMAIN\` prefix is stripped off the user name either way.
fn ntlmCredential(cfg: DbConfig) ntlm.Credential {
    var domain = cfg.domain;
    var user = cfg.user;
    if (std.mem.indexOfScalar(u8, user, '\\')) |i| {
        if (domain.len == 0) domain = user[0..i];
        user = user[i + 1 ..];
    }
    return .{ .domain = domain, .user = user, .password = cfg.password };
}

/// One key-dispatch for the shared DB connection attributes. `f` supplies the
/// values: `resolveDbConfig` evaluates them strictly and reports through the
/// diag. It is generic because a second, lenient fetcher used to exist for
/// offline resolution; the seam is kept so one can return without moving this.
fn parseDbConfig(conn: ast.Connection, default_port: u16, f: anytype) anyerror!DbConfig {
    var cfg = DbConfig{ .port = default_port };
    for (conn.config) |attr| {
        const k = attr.key;
        // `fe_host`/`fe_port` are the StarRocks spellings of the same two attributes.
        if (eqlAny(k, &.{ "port", "fe_port" })) {
            if (try f.port(attr.value)) |p| {
                cfg.port = p;
                cfg.port_explicit = true;
            }
            continue;
        }
        if (!eqlAny(k, &.{ "host", "fe_host", "user", "password", "database", "tls", "auth", "domain", "client_id", "resource", "token" })) continue;
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

/// Strict attribute fetcher for `parseDbConfig`: literals + env()/secret(), with
/// plan errors on anything unresolvable (the run-time path).
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
            planErr(self.env.diag, "connection `auth` must be \"sql\", \"aad\" or \"ntlm\"");
    }
};

fn resolveDbConfig(env: *Env, conn: ast.Connection, default_port: u16) !DbConfig {
    const cfg = try parseDbConfig(conn, default_port, EnvCfg{ .env = env });
    if (cfg.host.len == 0) return planErr(env.diag, "connection needs a `host`");
    if (cfg.auth == .ntlm and cfg.tls == .off) return planErr(env.diag, "connection `auth = 'ntlm'` requires an encrypted channel: set `tls = 'require'`, or `tls = 'insecure'` for a self-signed server certificate");
    return cfg;
}

fn readSql(env: *Env, rd: ast.Read) ![]const u8 {
    const base = switch (rd.form) {
        .query => |q| q,
        .table => |t| try std.fmt.allocPrint(env.arena, "SELECT * FROM {s}", .{try qualStr(env.arena, t)}),
        else => return planErr(env.diag, "a DB read needs `table <name>` or `query \"...\"`"),
    };
    return sqlWithWhere(env.arena, base, rd.form == .query, rd.where);
}

/// Compose a pushed-down predicate into a read's SQL. Table reads get a plain
/// `WHERE`; query reads are wrapped as a subquery so the predicate composes with
/// whatever the query already filters (same shape split.zig uses for lane ranges).
/// An empty predicate is "no WHERE" — a for-loop `${var}` that rendered empty
/// (e.g. no `since` field on a full extraction) falls through to a full scan.
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
    return .{ .kind = kind, .dialect = dialect, .cfg = cfg, .base_sql = base_sql, .table = table };
}

/// The `SqlDesc` a read stage would produce, recomputed without opening anything.
/// `env.sql_desc` is last-writer-wins across every SQL source a plan opens, and a join
/// plans its build side *after* the probe read — so a pipeline with a SQL binding
/// leaves `env.sql_desc` describing the binding, not the read. Mirrors
/// `openSourceProjected`: the `where` hint folds into the read the same way.
/// Null → not a splittable SQL read.
pub fn sqlDescForStage(env: *Env, stage: ast.Stage) !?SqlDesc {
    if (stage.node != .read) return null;
    var rd = stage.node.read;
    if (rd.form != .table and rd.form != .query) return null;
    const conn = env.connections.get(rd.connector) orelse return null;
    const info = sqlConnInfo(conn) orelse return null;
    if (forHintName(stage.hints, "where")) |wh| {
        if (wh.len > 0) {
            rd.where = if (rd.where.len > 0)
                try std.fmt.allocPrint(env.arena, "({s}) AND ({s})", .{ wh, rd.where })
            else
                wh;
        }
    }
    const cfg = try resolveDbConfig(env, conn, info.port);
    return try sqlDescFor(env, info.kind, info.dialect, cfg, try readSql(env, rd), rd);
}

/// Pull `@[split = col]` / `@[splits = N]` / `@[split_kind = int|uuid|date]`
/// off the leading read stage.
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

/// Try to build a split plan for a map-only SQL pipeline. Returns null (→ run
/// serial) when the source isn't a splittable SQL table/query, no usable key is
/// found, or the table is too small to split.
/// True when this sink is the Postgres COPY path (append/overwrite to a postgres
/// connection), which benchmarks faster run serially than split — see planSplit.
fn isPostgresCopySink(env: *Env, w: ast.Write) bool {
    return w.mode != .upsert and std.mem.eql(u8, connectorType(env, w.connector), "postgres");
}

pub fn planSplit(env: *Env, desc: SqlDesc, lead: ast.Stage, threads: usize, w: ast.Write) !?split.Plan {
    const hints = splitHints(lead);
    const forced = hints.col != null or hints.count != null;
    const m: usize = hints.count orelse @min(@as(usize, 64), threads * 4);
    if (m < 2) return null;
    if (!forced and isPostgresCopySink(env, w)) return null;

    var pctx = SplitCtx{ .gpa = env.gpa, .kind = desc.kind, .cfg = desc.cfg, .base_sql = desc.base_sql };
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
    return split.plan(env.arena, prober, desc.dialect, desc.base_sql, key, m);
}

fn proberOpen(ctx_ptr: *anyopaque) anyerror!sql.Conn {
    const ctx: *SplitCtx = @ptrCast(@alignCast(ctx_ptr));
    return connectSql(ctx.gpa, ctx.kind, ctx.cfg);
}

pub const SqlConnInfo = registry.SqlRead;

/// What a `CREATE CONNECTION` resolves to when it can be read over a SQL wire
/// protocol; null for every other connector. A `starrocks` connection qualifies
/// (see `Connector.sqlRead`) — its *sink* is stream load, which every write path
/// checks for before asking this.
pub fn sqlConnInfo(conn: ast.Connection) ?SqlConnInfo {
    const c = registry.Connector.parse(conn.connector) orelse return null;
    return c.sqlRead();
}

/// Every connection carries `user`/`password` — explicit, or the parser's
/// env(NAME_USER/NAME_PASS) default — but an http connection reads them only
/// for basic auth, and for oauth2 where no client_id/client_secret is given.
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

fn qualStr(arena: std.mem.Allocator, q: ast.QualName) ![]const u8 {
    if (q.parts.len == 1) return q.parts[0];
    return std.mem.join(arena, ".", q.parts);
}

/// Bare `upsert` (no `on <key>`) infers the upsert keys from the source table's
/// primary key at plan time. Needs the lead read to be a SQL `table` source
/// (env.sql_desc.table set); a `query` source or non-SQL source can't be
/// introspected and gets a clear error pointing at `upsert on <col>`.
pub fn resolveUpsertKeys(env: *Env, w: ast.Write) !ast.Write {
    if (w.mode != .upsert or w.mode.upsert.keys.len > 0) return w;
    const desc = env.sql_desc orelse return planErr(env.diag, "`upsert` without `on <key>` infers the primary key from the source, which needs a SQL `table` read — this pipeline's source can't be introspected; name the key with `upsert on <col>`");
    const table = desc.table orelse return planErr(env.diag, "`upsert` key inference needs `read <conn> table <name>` (a `query` source has no single table to introspect); name the key with `upsert on <col>`");
    var pctx = SplitCtx{ .gpa = env.gpa, .kind = desc.kind, .cfg = desc.cfg, .base_sql = desc.base_sql };
    const prober = split.Prober{ .ctx = &pctx, .openFn = proberOpen };
    const keys = split.introspectPkCols(env.arena, prober, desc.dialect, table) catch |e|
        return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not read primary key of `{s}`: {s}", .{ table, @errorName(e) }));
    if (keys.len == 0) return planErr(env.diag, try std.fmt.allocPrint(env.arena, "no primary key found on `{s}`; name the key with `upsert on <col>`", .{table}));
    env.log.log(.debug, "upsert: inferred key on {s}: {s}", .{ table, try std.mem.join(env.arena, ", ", keys) });
    var out = w;
    out.mode = .{ .upsert = .{ .keys = keys, .partial = w.mode.upsert.partial } };
    return out;
}

/// Narrows a write disposition to how a file target is opened. A bare `LOAD INTO`
/// and an explicit `REPLACE` both create-or-truncate — one-shot output is what a
/// file sink is for, and that is unchanged. Only an explicit `APPEND` accumulates,
/// and only where bytes can actually be added to what is already there: a parquet
/// footer indexes the whole file and is written last, and a block blob is committed
/// as a new object rather than extended, so both are refused here instead of
/// quietly truncating the target the pipeline meant to grow.
fn fileWriteMode(env: *Env, w: ast.Write) !driver.FileMode {
    if (w.mode != .append) return .truncate;
    const why = analyze.appendUnsupported(w.target) orelse return .append;
    return planErr(env.diag, try std.fmt.allocPrint(env.arena, "`APPEND` into `{s}` is not supported: {s}. Use `REPLACE`, write each run to its own path, or accumulate with `INTO BUFFER` and load the buffer once", .{ w.target, why }));
}

/// Refuse a file path whose extension names no format basalt reads.
///
/// `check` applies this through `analyze`, but `run` does not analyze the pipeline
/// first, so without this call the runtime still parsed a `.zip` as CSV and
/// answered `SELECT COUNT(*)` with the newline count of its deflate stream.
pub fn guardFileFormat(env: *Env, path: []const u8, explicit: ?analyze.FileFormat, comptime verb: []const u8) !void {
    if (analyze.unreadableTarget(path, explicit)) |why|
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "cannot " ++ verb ++ " `{s}`: {s}", .{ path, why }));
}

pub fn openSink(env: *Env, w: ast.Write, schema: types.Schema) !driver.Sink {
    if (env.explain) return DiscardSink.sink();
    if (std.mem.eql(u8, w.connector, "stdout")) {
        switch (env.stdout_format) {
            .json => {
                const writer = JsonWriter.open(env.gpa, schema) catch
                    return planErr(env.diag, "could not open stdout json writer");
                return writer.sink();
            },
            .arrow => {
                const writer = ArrowWriter.open(env.gpa, schema) catch |e| switch (e) {
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
    if (std.mem.eql(u8, w.connector, "csv")) {
        const fmode = try fileWriteMode(env, w);
        // A `.parquet` target shares the csv connector but is a different format;
        // without this it would be written as CSV text under a .parquet name. An
        // explicit `WITH (format = ...)` overrides the extension.
        const wfmt = env.fmt_out orelse (if (pqwrite.Writer.isPath(w.target)) analyze.FileFormat.parquet else analyze.FileFormat.csv);
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
    if (std.mem.eql(u8, conn.connector, "starrocks")) {
        const cfg = try resolveStarrocksConfig(env, conn);
        const s = starrocks.StreamLoadSink.open(env.gpa, cfg, w.target, schema, w.mode) catch |e|
            return srOpenErr(env, e, "starrocks sink open failed");
        s.logger = env.log;
        s.errctx = env.errctx;
        return s.sink();
    }
    if (sqlConnInfo(conn)) |info| {
        const cfg = try resolveDbConfig(env, conn, info.port);
        switch (info.kind) {
            inline else => |k| {
                const c = SqlDriver(k).connect(env.gpa, cfg) catch |e|
                    return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "{s} connect failed: {s}", .{ conn.connector, @errorName(e) }));
                return openBulkOrInsert(env.gpa, c, SqlDriver(k).Bulk, info.dialect, w.target, schema, w.mode, try redialFor(env.arena, info.kind, cfg)) catch |e| {
                    defer c.close();
                    return planErr(env.diag, try std.fmt.allocPrint(env.arena, "{s} sink failed ({s}): {s}", .{ conn.connector, @errorName(e), c.last_error }));
                };
            },
        }
    }
    return planErr(env.diag, try std.fmt.allocPrint(env.arena, "unsupported sink connector `{s}`", .{conn.connector}));
}

fn resolveStarrocksConfig(env: *Env, conn: ast.Connection) !starrocks.Config {
    var cfg = starrocks.Config{ .database = "" };
    for (conn.config) |attr| {
        const k = attr.key;
        if (eqlAny(k, &.{ "host", "fe_host" })) {
            cfg.fe_host = try evalCfgStr(env, attr.value);
        } else if (std.mem.eql(u8, k, "fe_port")) {
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
    if (cfg.database.len == 0) return planErr(env.diag, "starrocks connection needs a `database`");
    if (env.load_label_prefix) |lp| cfg.label_prefix = lp;
    if (env.load_run_id) |rid| cfg.run_id = rid;
    return cfg;
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

/// A `driver.Source` yielding one in-memory batch then EOF — used to run a small
/// post-aggregate `sort`/`limit` tail over the merged result.
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
