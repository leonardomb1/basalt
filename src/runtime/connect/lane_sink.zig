//! The sink each parallel lane writes through: a SQL bulk/insert connection or a
//! Stream Load of its own, dialed per lane from one spec.

const DbConfig = @import("../env.zig").DbConfig;
const Env = @import("../env.zig").Env;
const SqlDriver = @import("../connect.zig").SqlDriver;
pub const SqlKind = @import("../env.zig").SqlKind;
const ast = @import("../../lang/ast.zig");
const connectSql = @import("../connect.zig").connectSql;
const connectWhy = @import("dbconfig.zig").connectWhy;
const driver = @import("../../connect/driver.zig");
const obs = @import("../obs.zig");
const op = @import("../../exec/op.zig");
const parallel = @import("../parallel.zig");
const planErr = @import("../env.zig").planErr;
const planErrT = @import("../env.zig").planErrT;
const resolveDbConfig = @import("dbconfig.zig").resolveDbConfig;
const resolveStreamLoadConfig = @import("sink.zig").resolveStreamLoadConfig;
const sql = @import("../../db/sql.zig");
const sqlConnInfo = @import("../connect.zig").sqlConnInfo;
const srOpenErr = @import("../env.zig").srOpenErr;
const std = @import("std");
const streamload = @import("../../db/streamload.zig");
const types = @import("../../lang/types.zig");

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

pub fn redialFor(arena: std.mem.Allocator, kind: SqlKind, cfg: DbConfig) !sql.Redial {
    const ds = try arena.create(DialSpec);
    ds.* = .{ .kind = kind, .cfg = cfg };
    return .{ .ctx = ds, .dial = dialSqlConn };
}

/// The bulk-vs-INSERT rule, shared by the serial and per-lane paths so they
/// cannot drift. On error the caller still owns and closes `conn`.
pub fn openBulkOrInsert(gpa: std.mem.Allocator, conn: anytype, comptime BulkSink: type, dialect: sql.Dialect, target: []const u8, schema: types.Schema, mode: ast.WriteMode, redial: ?sql.Redial) !driver.Sink {
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
