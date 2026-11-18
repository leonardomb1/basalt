//! The sink each parallel lane writes through: a SQL bulk/insert connection or a
//! Stream Load of its own, dialed per lane from one spec.

const DbConfig = @import("../env.zig").DbConfig;
const Env = @import("../env.zig").Env;
const SqlDriver = @import("../connect.zig").SqlDriver;
pub const SqlKind = @import("../env.zig").SqlKind;
const ast = @import("../../lang/ast.zig");
const connectSql = @import("../connect.zig").connectSql;
const connectWhy = @import("dbconfig.zig").connectWhy;
const driver = @import("../../exec/driver.zig");
const netconn = @import("../../net/conn.zig");
const obs = @import("../../exec/obs.zig");
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
const retry = @import("../../net/retry.zig");
const msgfmt = @import("../../base/msgfmt.zig");
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
    /// An INSERT's staging, loaded by the lanes and applied once they are done.
    insert_plan: ?*ReplacePlan = null,
};

fn finishLaneStreamInsert(ctx: *anyopaque) anyerror!void {
    const spec: *StreamLoadSpec = @ptrCast(@alignCast(ctx));
    if (spec.insert_plan) |p| try p.finish();
}

fn abortLaneStreamInsert(ctx: *anyopaque) void {
    const spec: *StreamLoadSpec = @ptrCast(@alignCast(ctx));
    if (spec.insert_plan) |p| p.abort();
}

/// A StarRocks or Doris INSERT's staging: a table of the run's own made over the
/// FE's SQL connection (`CREATE TABLE ... AS SELECT`, the target's columns the
/// INSERT names, emptied), the Stream Load target until one `INSERT ... SELECT`
/// moves the rows into the target.
pub fn streamInsertPlan(env: *Env, conn: ast.Connection, w: ast.Write, schema: types.Schema) !*ReplacePlan {
    const info = sqlConnInfo(conn).?;
    const cfg = try resolveDbConfig(env, conn, info.port);
    const p = try env.arena.create(ReplacePlan);
    p.* = .{ .gpa = env.gpa, .kind = info.kind, .cfg = cfg, .dialect = info.dialect, .target = w.target, .schema = schema, .how = env.cutover, .errctx = env.errctx, .insert = true };
    p.prepare(env.arena) catch |e| return planErr(env.diag, try @import("sink.zig").insertSetupFailed(env, conn.connector, w.target, e));
    return p;
}

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
    /// Whether a bulk load may take the table lock: always into a REPLACE's
    /// staging table, its run's own; else as `table_lock` says.
    lock: bool = true,
};

/// A REPLACE's or an INSERT's staging and the one step that applies it to the target
/// (see `db/replace.zig`), each on a short connection of its own, so the lanes keep
/// theirs and their redials.
pub const ReplacePlan = struct {
    gpa: std.mem.Allocator,
    kind: SqlKind,
    cfg: DbConfig,
    dialect: sql.Dialect,
    target: []const u8,
    schema: types.Schema,
    how: replace.Cutover,
    errctx: ?*op.ErrCtx,
    staging: []const u8 = "",
    /// An INSERT: staged by its own columns, applied by one `INSERT … SELECT`.
    insert: bool = false,

    pub fn stagingTarget(self: *const ReplacePlan) []const u8 {
        return self.staging;
    }

    /// Names this run's staging table, in `arena`, and creates it.
    pub fn prepare(self: *ReplacePlan, arena: std.mem.Allocator) !void {
        self.staging = try replace.stagingName(arena, self.target, std.crypto.random.int(u32));
        var aa = std.heap.ArenaAllocator.init(self.gpa);
        defer aa.deinit();
        const a = aa.allocator();
        const setup = if (self.insert) try replace.insertSetupSql(a, self.dialect, self.target, self.staging, self.schema) else try replace.setupSql(a, self.dialect, self.target, self.staging);
        try self.run(setup, "staging", true);
    }

    pub fn finish(self: *ReplacePlan) !void {
        var aa = std.heap.ArenaAllocator.init(self.gpa);
        defer aa.deinit();
        const a = aa.allocator();
        const apply = if (self.insert) try replace.insertApplySql(a, self.dialect, self.target, self.staging, self.schema) else try replace.cutoverSql(a, self.dialect, self.target, self.staging, self.schema, self.how);
        self.run(apply, if (self.insert) "" else "cutover", !self.insert) catch |e| {
            self.abort();
            return e;
        };
        self.run(&.{try replace.dropSql(a, self.dialect, self.staging)}, "staging cleanup", true) catch {};
    }

    pub fn abort(self: *ReplacePlan) void {
        var aa = std.heap.ArenaAllocator.init(self.gpa);
        defer aa.deinit();
        const drop = replace.dropSql(aa.allocator(), self.dialect, self.staging) catch return;
        self.run(&.{drop}, "staging cleanup", true) catch {};
    }

    /// `stmts` on a connection of their own, run again from the start on a fresh one
    /// (`net/retry.zig`) when the connection could not be made, when the server says
    /// it rolled them back (a deadlock victim), and, for a step that may run twice
    /// (`again`), after a network failure: the setup drops its own table first, the
    /// cutover clears the target and copies from staging again, and a transaction the
    /// failure cut short rolls back as its connection closes. An INSERT's step may not:
    /// sent once, it might have committed before the connection broke, so that
    /// failure ends the run saying so rather than inserting the rows twice.
    fn run(self: *ReplacePlan, stmts: []const []const u8, step: []const u8, again: bool) !void {
        const Try = struct {
            plan: *ReplacePlan,
            stmts: []const []const u8,
            again: bool,
            msg: std.array_list.Managed(u8),
            rerun: bool = false,
            unknown: bool = false,

            fn attempt(t: *@This()) !retry.Verdict {
                const p = t.plan;
                const conn = try connectSql(p.gpa, p.kind, p.cfg);
                defer conn.close();
                replace.execAll(conn, t.stmts) catch |e| {
                    t.msg.clearRetainingCapacity();
                    try t.msg.appendSlice(conn.lastError());
                    t.rerun = conn.rerunnable();
                    if (t.rerun) return .{ .failed = .{ .code = 0, .err = e, .retriable = true } };
                    if (netconn.transientNet(e)) {
                        if (t.again) return e;
                        t.unknown = true;
                    }
                    return .{ .failed = .{ .code = 0, .err = e, .retriable = false } };
                };
                return .done;
            }
        };
        var prng = retry.jitterPrng();
        var t = Try{ .plan = self, .stmts = stmts, .again = again, .msg = std.array_list.Managed(u8).init(self.gpa) };
        defer t.msg.deinit();
        const what: []const u8 = if (self.insert) "INSERT" else "REPLACE";
        retry.retry(.{ .rand = prng.random(), .base_ms = 250 }, &t, Try.attempt) catch |e| {
            if (self.errctx) |ec| {
                if (t.unknown)
                    ec.set("the connection to {s} broke while the {s} was applied, so whether its rows landed is unknown; check the table before running it again", .{ @tagName(self.dialect), what })
                else if (t.msg.items.len > 0)
                    ec.set("{s} refused the {s}{s}{s}: {s}", .{ @tagName(self.dialect), what, if (step.len > 0) " " else "", step, t.msg.items });
            }
            return sql.rerunError(t.rerun, e);
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
/// cannot drift. On error the caller still owns and closes `conn`. `lock` lets the
/// bulk load take the table lock (`sql.BulkSink`).
pub fn openBulkOrInsert(gpa: std.mem.Allocator, conn: anytype, comptime BulkSink: type, dialect: sql.Dialect, target: []const u8, schema: types.Schema, mode: ast.WriteMode, redial: ?sql.Redial, errctx: ?*op.ErrCtx, lock: bool) !driver.Sink {
    if (mode != .upsert) {
        const b = try BulkSink.open(gpa, conn, target, schema, mode, redial, lock);
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
            return openBulkOrInsert(gpa, c, SqlDriver(k).Bulk, spec.dialect, spec.target, spec.schema, spec.lane_mode, spec.redial, spec.errctx, spec.lock);
        },
    }
}

/// A per-lane Stream Load or SQL sink for a split pipeline, or null for the
/// shared-mutex path. Null under `EXPLAIN ANALYZE`, whose lanes write to the one
/// discarding sink: a lane sink's setup alone creates the target table. Null too
/// inside a union whose branches share one sink (`Env.union_sink`).
pub fn buildParallelSink(env: *Env, w: ast.Write, schema_in: types.Schema) !?parallel.SinkMode {
    const schema = try @import("sink.zig").insertSchema(env, w, schema_in);
    try @import("sink.zig").uniqueTargetColumns(env, w, schema);
    if (env.explain or env.union_sink != null) return null;
    if (try buildStreamLoadSpec(env, w, schema)) |spec|
        return parallel.SinkMode{ .per_lane = .{ .open = openLaneStreamLoadSink, .ctx = spec, .finish = finishLaneStreamInsert, .abort = abortLaneStreamInsert } };
    if (try buildSqlSinkSpec(env, w, schema)) |spec|
        return parallel.SinkMode{ .per_lane = .{ .open = openLaneSqlSink, .ctx = spec, .finish = finishLaneReplace, .abort = abortLaneReplace } };
    return null;
}

/// Whether `w` goes to a target `buildParallelSink` opens one sink per lane for.
pub fn opensPerLane(env: *Env, w: ast.Write) bool {
    const conn = env.connections.get(w.connector) orelse return false;
    return streamload.Flavor.of(conn.connector) != null or sqlConnInfo(conn) != null;
}

fn buildSqlSinkSpec(env: *Env, w: ast.Write, schema: types.Schema) !?*SqlSinkSpec {
    const conn = env.connections.get(w.connector) orelse return null;
    const info = sqlConnInfo(conn) orelse return null;
    const kind = info.kind;
    const dialect = info.dialect;
    const cfg = try resolveDbConfig(env, conn, info.port);

    if (w.insert == null) {
        const setup_conn = connectSql(env.gpa, kind, cfg) catch |e|
            return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "{s} sink connect failed: {s}", .{ conn.connector, try connectWhy(env.arena, e) }));
        const setup = sql.Sink.open(env.gpa, setup_conn, dialect, w.target, schema, if (w.mode == .overwrite) .append else w.mode, null) catch |e| {
            defer setup_conn.close();
            const why = try netconn.connectorNoteAlloc(env.arena, "", e, setup_conn.lastError());
            return planErr(env.diag, try msgfmt.allocPrint(env.arena, "{s} sink setup failed: {s}", .{ conn.connector, why }));
        };
        setup.sink().close() catch |e|
            return planErr(env.diag, try msgfmt.allocPrint(env.arena, "{s} sink setup close failed: {s}", .{ conn.connector, @errorName(e) }));
    }

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
        .lock = w.mode == .overwrite or w.insert != null or env.table_lock,
    };
    if (w.mode == .overwrite or w.insert != null) {
        const plan = try env.arena.create(ReplacePlan);
        plan.* = .{ .gpa = env.gpa, .kind = kind, .cfg = cfg, .dialect = dialect, .target = w.target, .schema = schema, .how = env.cutover, .errctx = env.errctx, .insert = w.insert != null };
        plan.prepare(env.arena) catch |e| {
            if (w.insert != null) return planErr(env.diag, try @import("sink.zig").insertSetupFailed(env, conn.connector, w.target, e));
            return planErr(env.diag, try msgfmt.allocPrint(env.arena, "{s} REPLACE staging failed ({s})", .{ conn.connector, @errorName(e) }));
        };
        spec.target = plan.stagingTarget();
        spec.replace_plan = plan;
    }
    return spec;
}

fn buildStreamLoadSpec(env: *Env, w: ast.Write, schema: types.Schema) !?*StreamLoadSpec {
    const conn = env.connections.get(w.connector) orelse return null;
    const flavor = streamload.Flavor.of(conn.connector) orelse return null;
    var cfg = try resolveStreamLoadConfig(env, conn, flavor);
    cfg.run_id = if (cfg.run_id != 0) cfg.run_id else @intCast(std.time.milliTimestamp());
    if (w.insert != null) {
        const plan = try streamInsertPlan(env, conn, w, schema);
        cfg.auto_create = false;
        const spec = try env.arena.create(StreamLoadSpec);
        spec.* = .{ .cfg = cfg, .target = plan.stagingTarget(), .schema = schema, .mode = .append, .logger = env.log, .errctx = env.errctx, .insert_plan = plan };
        return spec;
    }

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
