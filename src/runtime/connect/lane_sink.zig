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
const msgfmt = @import("../../format/msgfmt.zig");
const replace = @import("../../db/replace.zig");
const Batch = @import("../../exec/batch.zig").Batch;
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
    const lp = try msgfmt.allocPrint(gpa, "{s}_l{d}", .{ spec.cfg.label_prefix, lane_idx });
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
    errctx: ?*op.ErrCtx = null,
    replace_plan: ?*ReplacePlan = null,
};

/// A REPLACE's staging and cutover (see `db/replace.zig`), each step on a short
/// connection of its own, so the lanes keep theirs and their redials.
pub const ReplacePlan = struct {
    gpa: std.mem.Allocator,
    kind: SqlKind,
    cfg: DbConfig,
    dialect: sql.Dialect,
    target: []const u8,
    schema: types.Schema,
    how: replace.Cutover,
    errctx: ?*op.ErrCtx,

    pub fn stagingTarget(self: *const ReplacePlan, arena: std.mem.Allocator) ![]const u8 {
        return replace.stagingName(arena, self.target);
    }

    pub fn prepare(self: *ReplacePlan) !void {
        var aa = std.heap.ArenaAllocator.init(self.gpa);
        defer aa.deinit();
        try self.run(try replace.setupSql(aa.allocator(), self.dialect, self.target), "staging");
    }

    pub fn finish(self: *ReplacePlan) !void {
        var aa = std.heap.ArenaAllocator.init(self.gpa);
        defer aa.deinit();
        const a = aa.allocator();
        self.run(try replace.cutoverSql(a, self.dialect, self.target, self.schema, self.how), "cutover") catch |e| {
            self.abort();
            return e;
        };
        self.run(&.{try replace.dropSql(a, self.dialect, self.target)}, "staging cleanup") catch {};
    }

    pub fn abort(self: *ReplacePlan) void {
        var aa = std.heap.ArenaAllocator.init(self.gpa);
        defer aa.deinit();
        const drop = replace.dropSql(aa.allocator(), self.dialect, self.target) catch return;
        self.run(&.{drop}, "staging cleanup") catch {};
    }

    fn run(self: *ReplacePlan, stmts: []const []const u8, step: []const u8) !void {
        const conn = try connectSql(self.gpa, self.kind, self.cfg);
        defer conn.close();
        replace.execAll(conn, stmts) catch |e| {
            if (self.errctx) |ec| if (conn.lastError().len > 0) ec.set("{s} refused the REPLACE {s}: {s}", .{ @tagName(self.dialect), step, conn.lastError() });
            return e;
        };
    }
};

fn finishLaneReplace(ctx: *anyopaque) anyerror!void {
    const spec: *SqlSinkSpec = @ptrCast(@alignCast(ctx));
    if (spec.replace_plan) |p| try p.finish();
}

fn abortLaneReplace(ctx: *anyopaque) void {
    const spec: *SqlSinkSpec = @ptrCast(@alignCast(ctx));
    if (spec.replace_plan) |p| p.abort();
}

/// The serial REPLACE: one sink loading the staging table, cut over on close.
pub const ReplaceSink = struct {
    inner: driver.Sink,
    plan: *ReplacePlan,

    pub fn writeBatch(self: *ReplaceSink, arena: std.mem.Allocator, b: Batch) !void {
        return self.inner.writeBatch(arena, b);
    }

    pub fn close(self: *ReplaceSink) !void {
        self.inner.close() catch |e| {
            self.plan.abort();
            return e;
        };
        try self.plan.finish();
    }

    pub fn abort(self: *ReplaceSink) void {
        self.inner.abort();
        self.plan.abort();
    }

    const vtable = driver.sinkVTable(ReplaceSink);

    pub fn sink(self: *ReplaceSink) driver.Sink {
        return .{ .ptr = self, .vtable = &vtable };
    }
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
pub fn openBulkOrInsert(gpa: std.mem.Allocator, conn: anytype, comptime BulkSink: type, dialect: sql.Dialect, target: []const u8, schema: types.Schema, mode: ast.WriteMode, redial: ?sql.Redial, errctx: ?*op.ErrCtx) !driver.Sink {
    if (mode != .upsert) {
        const b = try BulkSink.open(gpa, conn, target, schema, mode, redial);
        b.errctx = errctx;
        return b.sink();
    }
    const s = try sql.Sink.open(gpa, conn.sqlConn(), dialect, target, schema, mode, redial);
    s.errctx = errctx;
    return s.sink();
}

fn openLaneSqlSink(ctx_ptr: *anyopaque, gpa: std.mem.Allocator, lane_idx: usize) anyerror!driver.Sink {
    _ = lane_idx;
    const spec: *SqlSinkSpec = @ptrCast(@alignCast(ctx_ptr));
    switch (spec.kind) {
        inline else => |k| {
            const c = try SqlDriver(k).connect(gpa, spec.cfg);
            errdefer c.close();
            return openBulkOrInsert(gpa, c, SqlDriver(k).Bulk, spec.dialect, spec.target, spec.schema, spec.lane_mode, spec.redial, spec.errctx);
        },
    }
}

/// A per-lane Stream Load or SQL sink for a split pipeline, or null for the
/// shared-mutex path. Null under `EXPLAIN ANALYZE`, whose lanes write to the one
/// discarding sink: a lane sink's setup alone creates the target table.
pub fn buildParallelSink(env: *Env, w: ast.Write, schema: types.Schema) !?parallel.SinkMode {
    try @import("sink.zig").uniqueTargetColumns(env, w, schema);
    if (env.explain) return null;
    if (try buildStreamLoadSpec(env, w, schema)) |spec|
        return parallel.SinkMode{ .per_lane = .{ .open = openLaneStreamLoadSink, .ctx = spec } };
    if (try buildSqlSinkSpec(env, w, schema)) |spec|
        return parallel.SinkMode{ .per_lane = .{ .open = openLaneSqlSink, .ctx = spec, .finish = finishLaneReplace, .abort = abortLaneReplace } };
    return null;
}

fn buildSqlSinkSpec(env: *Env, w: ast.Write, schema: types.Schema) !?*SqlSinkSpec {
    const conn = env.connections.get(w.connector) orelse return null;
    const info = sqlConnInfo(conn) orelse return null;
    const kind = info.kind;
    const dialect = info.dialect;
    const cfg = try resolveDbConfig(env, conn, info.port);

    const setup_conn = connectSql(env.gpa, kind, cfg) catch |e|
        return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "{s} sink connect failed: {s}", .{ conn.connector, try connectWhy(env.arena, conn.connector, e) }));
    const setup = sql.Sink.open(env.gpa, setup_conn, dialect, w.target, schema, if (w.mode == .overwrite) .append else w.mode, null) catch |e| {
        defer setup_conn.close();
        const why = try driver.connectorNoteAlloc(env.arena, "", e, setup_conn.lastError());
        return planErr(env.diag, try msgfmt.allocPrint(env.arena, "{s} sink setup failed: {s}", .{ conn.connector, why }));
    };
    setup.sink().close() catch |e|
        return planErr(env.diag, try msgfmt.allocPrint(env.arena, "{s} sink setup close failed: {s}", .{ conn.connector, @errorName(e) }));

    const spec = try env.arena.create(SqlSinkSpec);
    spec.* = .{
        .kind = kind,
        .dialect = dialect,
        .cfg = cfg,
        .target = w.target,
        .schema = schema,
        .lane_mode = if (w.mode == .overwrite) .append else w.mode,
        .redial = try redialFor(env.arena, kind, cfg),
        .errctx = env.errctx,
    };
    if (w.mode == .overwrite) {
        const plan = try env.arena.create(ReplacePlan);
        plan.* = .{ .gpa = env.gpa, .kind = kind, .cfg = cfg, .dialect = dialect, .target = w.target, .schema = schema, .how = env.cutover, .errctx = env.errctx };
        plan.prepare() catch |e|
            return planErr(env.diag, try msgfmt.allocPrint(env.arena, "{s} REPLACE staging failed ({s})", .{ conn.connector, @errorName(e) }));
        spec.target = try plan.stagingTarget(env.arena);
        spec.replace_plan = plan;
    }
    return spec;
}

fn buildStreamLoadSpec(env: *Env, w: ast.Write, schema: types.Schema) !?*StreamLoadSpec {
    const conn = env.connections.get(w.connector) orelse return null;
    const flavor = streamload.Flavor.of(conn.connector) orelse return null;

    var cfg = try resolveStreamLoadConfig(env, conn, flavor);
    cfg.run_id = if (cfg.run_id != 0) cfg.run_id else @intCast(std.time.milliTimestamp());

    const setup = streamload.StreamLoadSink.open(env.gpa, cfg, w.target, schema, w.mode) catch |e|
        return srOpenErr(env, e, try msgfmt.allocPrint(env.arena, "{s} setup failed", .{flavor.name()}));
    setup.logger = env.log;
    setup.errctx = env.errctx;
    setup.sink().close() catch |e|
        return planErr(env.diag, try msgfmt.allocPrint(env.arena, "{s} setup close failed: {s}", .{ flavor.name(), @errorName(e) }));
    cfg.auto_create = false;

    const spec = try env.arena.create(StreamLoadSpec);
    spec.* = .{ .cfg = cfg, .target = w.target, .schema = schema, .mode = if (w.mode == .overwrite) .append else w.mode, .logger = env.log, .errctx = env.errctx };
    return spec;
}
