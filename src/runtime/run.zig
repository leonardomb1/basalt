//! Build executable operator trees from a parsed @batch program and drive them.
//! Handles full program structure: `param`s (bound from the CLI or defaults),
//! `let` bindings (recompiled per reference), `ref` sources, and multiple output
//! pipelines. Params are substituted into expressions as literals before planning.

const std = @import("std");
const ast = @import("../lang/ast.zig");
const expand = @import("../lang/expand.zig");
const types = @import("../lang/types.zig");
const op = @import("../exec/op.zig");
const batchmod = @import("../exec/batch.zig");
const column = @import("../exec/column.zig");
const keyhash = @import("../exec/keyhash.zig");
const eval = @import("../exec/eval.zig");
const csv = @import("../connect/csv.zig");
const pqdecode = @import("../connect/pqdecode.zig");
const pqwrite = @import("../connect/pqwrite.zig");
const tablemod = @import("../connect/table.zig");
const driver = @import("../connect/driver.zig");
const starrocks = @import("../connect/starrocks.zig");
const tds = @import("../connect/tds.zig");
const mysql = @import("../connect/mysql.zig");
const postgres = @import("../connect/postgres.zig");
const sql = @import("../connect/sql.zig");
const request = @import("../connect/request.zig");
const httpsrc = @import("../connect/http.zig");
const aad = @import("../connect/aad.zig");
const splitmod = @import("../connect/split.zig");
const ssrp = @import("../connect/ssrp.zig");
const ntlm = @import("../connect/ntlm.zig");
const walmod = @import("../connect/wal.zig");
const azure = @import("../connect/azure.zig");
const gen = @import("../connect/gen.zig");
const parallel = @import("parallel.zig");
const analyze = @import("analyze.zig");
const pushdown = @import("pushdown.zig");
const obs = @import("obs.zig");
const valuemod = @import("../exec/value.zig");

const Value = valuemod.Value;

/// `msg` points into the inline `buf`, so it outlives the run's plan arena.
pub const Diag = struct {
    buf: [512]u8 = undefined,
    msg: []const u8 = "",
    /// Set when the failure looks transient (network/connection) so the control
    /// plane can retry; left false for permanent failures (bad SQL, schema, auth,
    /// rejected data) where a retry would just fail the same way.
    retryable: bool = false,
};

pub const requestAbort = driver.requestAbort;
pub const aborting = driver.aborting;
pub const resetAbort = driver.resetAbort;

var g_reload = std.atomic.Value(bool).init(false);
pub fn requestReload() void {
    g_reload.store(true, .seq_cst);
}
pub fn takeReload() bool {
    return g_reload.swap(false, .seq_cst);
}

/// A failure worth retrying: connection/network-level, not a config or data error.
/// Host resolution (`UnknownHostName`) is treated as permanent — usually a typo.
pub fn isTransient(e: anyerror) bool {
    return switch (e) {
        error.ConnectionRefused,
        error.ConnectionTimedOut,
        error.ConnectionResetByPeer,
        error.NetworkUnreachable,
        error.HostUnreachable,
        error.BrokenPipe,
        error.TemporaryNameServerFailure,
        error.NameServerFailure,
        error.HostLacksNetworkAddresses,
        error.HttpServerBusy,
        error.HttpTransportFailed,
        => true,
        else => false,
    };
}
pub const Stats = struct {
    rows_out: usize = 0,
    rows_read: usize = 0,
    run_id: u64 = 0,
    elapsed_ms: u64 = 0,
    source: []const u8 = "",
    sink: []const u8 = "",
};
pub const ParamArg = struct { key: []const u8, val: []const u8 };

/// Where the end-of-run summary goes. `.none` keeps `run()` silent when embedded
/// (tests, the HTTP server); the CLI opts into `.stderr` or `.json_stdout`.
pub const SummaryMode = enum { none, stderr, json_stdout };

/// Logging/output config (from CLI flags). `format = .auto` picks human text on a
/// TTY, NDJSON when piped. The default level is `warn`, so a healthy run is silent
/// unless something needs attention; plan shape lives at `debug` (and in EXPLAIN).
pub const LogConfig = struct {
    format: obs.Format = .auto,
    level: obs.Level = .warn,
    quiet: bool = false,
    summary: SummaryMode = .none,
};

/// Inputs to a run: params (from CLI flags or an HTTP request's query string) and
/// an optional request body that `read request` consumes.
/// The result of one for-each item (one table in a fan-out batch), so the control
/// plane can retry just the failures rather than the whole batch.
pub const ItemOutcome = struct {
    item: []const u8,
    ok: bool,
    err: []const u8 = "",
    retryable: bool = false,
};

/// A thread-safe collector for per-item outcomes. Strings are duped into the
/// caller-provided allocator so they outlive the run's internal arena.
pub const OutcomeSink = struct {
    alloc: std.mem.Allocator,
    list: std.array_list.Managed(ItemOutcome),
    mutex: std.Thread.Mutex = .{},

    pub fn init(alloc: std.mem.Allocator) OutcomeSink {
        return .{ .alloc = alloc, .list = std.array_list.Managed(ItemOutcome).init(alloc) };
    }
    pub fn deinit(self: *OutcomeSink) void {
        self.list.deinit();
    }
    fn record(self: *OutcomeSink, item: []const u8, ok: bool, err: []const u8, retryable: bool) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.list.append(.{
            .item = self.alloc.dupe(u8, item) catch item,
            .ok = ok,
            .err = self.alloc.dupe(u8, err) catch err,
            .retryable = retryable,
        }) catch {};
    }
    pub fn failures(self: *OutcomeSink) usize {
        var n: usize = 0;
        for (self.list.items) |o| {
            if (!o.ok) n += 1;
        }
        return n;
    }
};

pub const RunOptions = struct {
    params: []const ParamArg = &.{},
    request_body: ?[]const u8 = null,
    /// Worker threads for map-only pipelines (scan → filter/project/explode). 1 =
    /// the serial driver (deterministic, used by the in-process test harness); the
    /// CLI defaults this to the detected core count.
    threads: usize = 1,
    log: LogConfig = .{},
    /// Optional collector for per-item outcomes of a `for`-each fan-out. When set,
    /// continue-mode partial failures are reported here instead of failing the run.
    outcomes: ?*OutcomeSink = null,
    /// Serve flusher: restrict every `FROM BUFFER` source to this one segment.
    buffer_segment: ?u64 = null,
    /// Print the executed operator tree with measured time and row counts.
    explain: bool = false,
    /// `--format json`: terminal SELECTs emit NDJSON rows instead of the table.
    stdout_json: bool = false,
    /// Serve flusher: pin the StarRocks label prefix (the segment label) and
    /// run_id (the segment seq) so a replayed segment produces the SAME labels
    /// — the sink's dedup then makes redelivery effectively-once.
    load_label_prefix: ?[]const u8 = null,
    load_run_id: ?u64 = null,
};

const SqlKind = enum { postgres, mysql, sqlserver };

/// Captured when a SQL source is opened, so the planner can re-open the same
/// source per split (each split = the base query wrapped with a key-range WHERE).
const SqlDesc = struct {
    kind: SqlKind,
    dialect: sql.Dialect,
    cfg: DbConfig,
    base_sql: []const u8,
    table: ?[]const u8,
};

const Env = struct {
    arena: std.mem.Allocator,
    gpa: std.mem.Allocator,
    params: *std.StringHashMap(Value),
    bindings: *std.StringHashMap(ast.Pipeline),
    connections: *std.StringHashMap(ast.Connection),
    sources: *std.array_list.Managed(driver.Source),
    request_body: ?[]const u8,
    diag: *Diag,
    log: *obs.Logger,
    /// Param name → literal expr, for substitution in stage expressions.
    params_expr: *std.StringHashMap(*const ast.Expr),
    /// Runtime expression-error context (which stage/column failed).
    errctx: *op.ErrCtx,
    /// Emitted-row counter shared by every source (via `obs.CountingSource`).
    rows_read: *std.atomic.Value(u64),
    /// Parsed JSON params (the request body), navigated by `for x in p.path`.
    /// Scalar `p.a.b` path access is substituted at plan time (expand.zig).
    json_params: *std.StringHashMap(std.json.Value),
    /// Statement-form `CREATE FUNCTION` declarations by name — the macro table
    /// `CALL` renders through the for-each machinery. Expression-form functions
    /// never reach here; expand.zig inlined them.
    fns: *const std.StringHashMap(ast.FnDecl),
    /// Nesting depth of the `CALL` currently being rendered (recursion guard).
    call_depth: usize = 0,
    /// The endpoint's `INTO BUFFER` declaration, if any (dir + declared schema
    /// for `FROM BUFFER` reads that don't name them).
    buffer_decl: ?ast.BufferDecl = null,
    /// See RunOptions: flusher-mode segment restriction and label pinning.
    buffer_segment: ?u64 = null,
    load_label_prefix: ?[]const u8 = null,
    load_run_id: ?u64 = null,
    /// Set by `openSource` to the leading SQL source of the pipeline being built.
    sql_desc: ?SqlDesc = null,
    /// Connector types of the first source/sink, for the run summary.
    src_name: []const u8 = "",
    /// Parquet readers opened for the current pipeline, and the last one — used
    /// to push a top-N bound only when there is exactly one.
    pq_readers: usize = 0,
    pq_reader: ?*pqdecode.Reader = null,
    sink_name: []const u8 = "",
    /// Set once any pipeline writes somewhere other than the stdout table, so a
    /// terminal `SELECT` — whose printed table is its own feedback — gets no summary.
    wrote_sink: bool = false,
    /// `--format json`: stdout sinks emit NDJSON rows instead of the table.
    stdout_json: bool = false,
    /// `EXPLAIN ANALYZE`: run the pipeline for its actuals, write nothing.
    explain: bool = false,
};

/// Drains every batch and keeps none: the rows must still be pulled for the
/// measured counts to be real.
const DiscardSink = struct {
    fn writeBatch(_: *anyopaque, _: std.mem.Allocator, _: batchmod.Batch) anyerror!void {}
    fn close(_: *anyopaque) anyerror!void {}
    fn abort(_: *anyopaque) void {}
    const vtable = driver.Sink.VTable{ .writeBatch = writeBatch, .close = close, .abort = abort };
    var unit: u8 = 0;
    fn sink() driver.Sink {
        return .{ .ptr = &unit, .vtable = &vtable };
    }
};

const PipeRes = struct { op: op.Op, schema: types.Schema };

/// Connection config carried into the split lanes (referenced via *anyopaque).
const SplitCtx = struct {
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
fn openSplitSource(ctx_ptr: *anyopaque, gpa: std.mem.Allocator, pred: []const u8) anyerror!driver.Source {
    const ctx: *SplitCtx = @ptrCast(@alignCast(ctx_ptr));
    const q = try splitmod.wrapProjected(gpa, ctx.base_sql, ctx.proj_select, pred, ctx.where_extra);
    defer gpa.free(q);
    return openSqlQuery(ctx, gpa, q);
}

/// Open a SQL source for a ready-built lane query (one connection per call).
fn openSqlQuery(ctx: *const SplitCtx, gpa: std.mem.Allocator, query: []const u8) anyerror!driver.Source {
    const conn = try connectSql(gpa, ctx.kind, ctx.cfg);
    errdefer conn.close();
    const s = try sql.Source.open(gpa, conn, query);
    return s.source();
}

/// Rebuild a map-only `filter`/`select` chain against the projected source schema and
/// return its linearized stages for `parallel.run` — so projecting fewer columns at the
/// source keeps each `project` op's column indices in step. Returns null (caller keeps
/// the full chain) if anything doesn't fit: a non-filter/select stage, or an analyze
/// hitch. The throwaway scan is only a chain anchor; `linearize` drops it and the stages
/// apply statelessly, so it's never read.
fn rebuildMapStages(env: *Env, middle: []const ast.Stage, proj_schema: *const types.Schema) ?[]const op.Stage {
    for (middle) |st| switch (st.node) {
        .filter, .select => {},
        else => return null,
    };
    const ob = env.arena.create(OneBatch) catch return null;
    ob.* = .{ .b = null, .sch = proj_schema.* };
    const scan = env.arena.create(op.Scan) catch return null;
    scan.* = .{ .src = ob.source() };
    const chain = buildMapChain(env.arena, env.params_expr, middle, scan, proj_schema) catch return null;
    const lin = (op.linearize(env.arena, chain) catch return null) orelse return null;
    return lin.stages;
}

/// Resolved config for a per-lane StarRocks sink (DDL already done once at plan
/// time; lanes just stream-load with a shared run_id and lane-distinct labels).
const StarrocksSinkSpec = struct {
    cfg: starrocks.Config,
    target: []const u8,
    schema: types.Schema,
    mode: ast.WriteMode,
    logger: ?*obs.Logger = null,
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
fn buildParallelSink(env: *Env, w: ast.Write, schema: types.Schema) !?parallel.SinkMode {
    if (try buildStarrocksSpec(env, w, schema)) |spec|
        return parallel.SinkMode{ .per_lane = .{ .open = openLaneStarrocksSink, .ctx = spec } };
    if (try buildSqlSinkSpec(env, w, schema)) |spec|
        return parallel.SinkMode{ .per_lane = .{ .open = openLaneSqlSink, .ctx = spec } };
    return null;
}

fn buildSqlSinkSpec(env: *Env, w: ast.Write, schema: types.Schema) !?*SqlSinkSpec {
    const conn = env.connections.get(w.connector) orelse return null;
    var kind: SqlKind = undefined;
    var port: u16 = undefined;
    if (std.mem.eql(u8, conn.connector, "mysql")) {
        kind = .mysql;
        port = 3306;
    } else if (std.mem.eql(u8, conn.connector, "postgres")) {
        kind = .postgres;
        port = 5432;
    } else if (std.mem.eql(u8, conn.connector, "sqlserver")) {
        kind = .sqlserver;
        port = 1433;
    } else return null;
    const dialect: sql.Dialect = switch (kind) {
        .mysql => .mysql,
        .postgres => .postgres,
        .sqlserver => .sqlserver,
    };
    const cfg = try resolveDbConfig(env, conn, port);

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
        return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "starrocks setup failed ({s}) — {s}", .{ @errorName(e), env.diag.msg }));
    setup.logger = env.log;
    setup.sink().close() catch |e|
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "starrocks setup close failed: {s}", .{@errorName(e)}));
    cfg.auto_create = false;

    const spec = try env.arena.create(StarrocksSinkSpec);
    spec.* = .{ .cfg = cfg, .target = w.target, .schema = schema, .mode = if (w.mode == .overwrite) .append else w.mode, .logger = env.log };
    return spec;
}

pub fn run(gpa: std.mem.Allocator, raw_program: ast.Program, opts: RunOptions, diag: *Diag) !Stats {
    var plan_arena = std.heap.ArenaAllocator.init(gpa);
    defer plan_arena.deinit();
    const arena = plan_arena.allocator();

    var expand_msg: []const u8 = "";
    const program = expand.expandProgram(arena, raw_program, opts.request_body, &expand_msg) catch |e| switch (e) {
        error.OutOfMemory => return e,
        error.ExpandFailed => return planErr(diag, expand_msg),
    };

    if (program.stmts.len == 0 or program.stmts[0] != .kind)
        return planErr(diag, "script must begin with a @kind tag");
    var params = std.StringHashMap(Value).init(arena);
    try resolveParams(arena, program, opts.params, &params, diag);
    var params_expr = std.StringHashMap(*const ast.Expr).init(arena);
    var pit = params.iterator();
    while (pit.next()) |kv| try params_expr.put(kv.key_ptr.*, try mkLit(arena, kv.value_ptr.*));
    try resolveLets(arena, program, &params, &params_expr, diag);

    var json_params = std.StringHashMap(std.json.Value).init(arena);
    for (program.stmts) |s| {
        if (s != .param or !s.param.is_json) continue;
        const name = s.param.name;
        if (opts.request_body) |b| {
            if (std.json.parseFromSliceLeaky(std.json.Value, arena, b, .{})) |jv| {
                try json_params.put(name, jv);
            } else |_| {}
        }
        for (opts.params) |kv| {
            if (!std.mem.eql(u8, kv.key, name)) continue;
            if (std.json.parseFromSliceLeaky(std.json.Value, arena, kv.val, .{})) |jv| {
                try json_params.put(name, jv);
            } else |_| return planErr(diag, try std.fmt.allocPrint(arena, "param `{s}`: value is not valid JSON", .{name}));
        }
    }

    var bindings = std.StringHashMap(ast.Pipeline).init(arena);
    var connections = std.StringHashMap(ast.Connection).init(arena);
    var fns = std.StringHashMap(ast.FnDecl).init(arena);
    var runnable: usize = 0;
    for (program.stmts[1..]) |s| switch (s) {
        .binding => |b| try bindings.put(b.name, b.pipeline),
        .connection => |c| try connections.put(c.name, c),
        .func => |fd| try fns.put(fd.name, fd),
        .output, .for_each, .match, .call => runnable += 1,
        .param, .kind, .let_const => {},
    };
    if (runnable == 0)
        return planErr(diag, "no output pipeline (a pipeline ending in `write`)");

    const run_id: u64 = @intCast(std.time.milliTimestamp());
    var logger = obs.Logger.init(run_id, opts.log.format, if (opts.log.quiet) .err else opts.log.level);
    const t0 = std.time.milliTimestamp();
    var rows_read = std.atomic.Value(u64).init(0);

    var errctx = op.ErrCtx{};
    errdefer if (errctx.msg.len > 0) setMsg(diag, errctx.msg);

    var sources = std.array_list.Managed(driver.Source).init(arena);
    var buffer_decl: ?ast.BufferDecl = null;
    for (program.stmts) |s| {
        if (s == .kind) buffer_decl = s.kind.buffer;
    }
    var env = Env{ .arena = arena, .gpa = gpa, .params = &params, .bindings = &bindings, .connections = &connections, .sources = &sources, .request_body = opts.request_body, .diag = diag, .log = &logger, .params_expr = &params_expr, .errctx = &errctx, .rows_read = &rows_read, .json_params = &json_params, .buffer_decl = buffer_decl, .buffer_segment = opts.buffer_segment, .load_label_prefix = opts.load_label_prefix, .load_run_id = opts.load_run_id, .stdout_json = opts.stdout_json, .explain = opts.explain, .fns = &fns };

    var batch_arena = std.heap.ArenaAllocator.init(gpa);
    defer batch_arena.deinit();

    var stats = Stats{ .run_id = run_id };
    var lanes_used: usize = 1;
    for (program.stmts[1..]) |s| switch (s) {
        .output => |p| try runOutput(&env, p, opts, &stats, &lanes_used, &batch_arena),
        .for_each => |fe| try runForEach(&env, fe, opts, &stats, &lanes_used, &batch_arena),
        .match => |m| try runStmtMatch(&env, m, opts, &stats, &lanes_used, &batch_arena),
        .call => |c| try runCall(&env, c, no_loop_vars, opts, &stats, &lanes_used, &batch_arena),
        else => {},
    };
    for (sources.items) |sc| sc.close();

    stats.rows_read = rows_read.load(.monotonic);
    stats.elapsed_ms = @intCast(std.time.milliTimestamp() - t0);
    stats.source = env.src_name;
    stats.sink = env.sink_name;

    const summary = obs.Summary{
        .run_id = run_id,
        .source = stats.source,
        .sink = stats.sink,
        .rows_read = stats.rows_read,
        .rows_written = stats.rows_out,
        .elapsed_ms = stats.elapsed_ms,
        .threads = lanes_used,
    };
    switch (opts.log.summary) {
        // `--format json`: a LOAD run's stdout is the summary object; a SELECT
        // run's stdout is the NDJSON rows — never both on one stream.
        .json_stdout => if (env.wrote_sink) {
            var sbuf: [1024]u8 = undefined;
            var sfw = std.fs.File.stdout().writer(&sbuf);
            summary.renderJson(&sfw.interface) catch {};
            sfw.interface.flush() catch {};
        },
        .stderr => if (env.wrote_sink) logger.summary(summary),
        .none => {},
    }
    return stats;
}

/// Equality for plan-time `match`: numbers/strings/bools/temporals compare by value;
/// a typed-vs-literal mismatch (e.g. a `port:int` subject vs a `"9030"` string
/// pattern) falls back to a textual compare so a typed value still matches a string
/// pattern instead of silently never matching.
fn valuesEqualLoose(arena: std.mem.Allocator, a: Value, b: Value) bool {
    if (eval.compareValues(a, b)) |ord| return ord == .eq;
    const as = eval.valueToString(arena, a) catch return false;
    const bs = eval.valueToString(arena, b) catch return false;
    return std.mem.eql(u8, as, bs);
}

/// Evaluate a statement-`match`'s subject/guards/patterns over the bound names/values
/// and return the index of the first matching arm (a `_` default matches), or null if
/// none. Shared by the param-level (`runStmtMatch`) and per-row for-loop
/// (`runForMatch`) runners, which differ only in how they bind names/values and how
/// they run the chosen arm's body. `ctx` prefixes any eval-error message.
fn matchArmIndex(env: *Env, m: ast.StmtMatch, ns: []const []const u8, vs: []const Value, ctx: []const u8) anyerror!?usize {
    var subj: ?Value = null;
    if (m.subject) |s| subj = eval.constEval(env.arena, s, ns, vs) catch |e|
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "{s} subject: {s}", .{ ctx, @errorName(e) }));
    for (m.arms, 0..) |arm, i| {
        if (arm.is_default) return i;
        if (arm.guard) |g| {
            const gv = eval.constEval(env.arena, g, ns, vs) catch |e|
                return planErr(env.diag, try std.fmt.allocPrint(env.arena, "{s} guard: {s}", .{ ctx, @errorName(e) }));
            if (gv == .bool and gv.bool) return i;
            continue;
        }
        const sv = subj orelse continue;
        for (arm.pats) |p| {
            const pv = eval.constEval(env.arena, p, ns, vs) catch |e|
                return planErr(env.diag, try std.fmt.allocPrint(env.arena, "{s} pattern: {s}", .{ ctx, @errorName(e) }));
            if (valuesEqualLoose(env.arena, sv, pv)) return i;
        }
    }
    return null;
}

/// Plan-time structural dispatch: evaluate the subject/guards over the resolved
/// params and run the first matching arm's block. No matching arm (and no `_`) is
/// a no-op. Subject form compares the subject to each pattern; guard form runs the
/// first arm whose boolean condition holds.
fn runStmtMatch(env: *Env, m: ast.StmtMatch, opts: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!void {
    var names = std.array_list.Managed([]const u8).init(env.arena);
    var values = std.array_list.Managed(Value).init(env.arena);
    var it = env.params.iterator();
    while (it.next()) |kv| {
        try names.append(kv.key_ptr.*);
        try values.append(kv.value_ptr.*);
    }
    const idx = (try matchArmIndex(env, m, names.items, values.items, "match")) orelse return;
    for (m.arms[idx].body) |*st| try runStmt(env, st, opts, stats, lanes_used, batch_arena);
}

/// Execute one statement — used for match arm bodies. Registers declarations into
/// the env and runs output / for-each / nested match.
fn runStmt(env: *Env, s: *const ast.Stmt, opts: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!void {
    switch (s.*) {
        .output => |p| try runOutput(env, p, opts, stats, lanes_used, batch_arena),
        .for_each => |fe| try runForEach(env, fe, opts, stats, lanes_used, batch_arena),
        .match => |mm| try runStmtMatch(env, mm, opts, stats, lanes_used, batch_arena),
        .call => |c| try runCall(env, c, no_loop_vars, opts, stats, lanes_used, batch_arena),
        .binding => |b| try env.bindings.put(b.name, b.pipeline),
        .connection => |c| try env.connections.put(c.name, c),
        // A LET is folded once, before anything runs; one nested in a branch would
        // silently miss that pass, so say so instead of resolving to nothing.
        .let_const => |l| return planErr(env.diag, try std.fmt.allocPrint(env.arena, "LET `{s}` must be declared at the top level of the script", .{l.name})),
        .param, .kind, .func => {},
    }
}

/// Run one output pipeline (ending in `write`): build it, then either split it
/// into parallel key-range lanes or stream it serially into the sink.
fn runOutput(env: *Env, out: ast.Pipeline, opts: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) !void {
    const arena = env.arena;
    const gpa = env.gpa;
    var stages = out.stages;
    if (stages.len == 0) return planErr(env.diag, "empty pipeline");
    const last = stages[stages.len - 1].node;
    if (last != .write) return planErr(env.diag, "a top-level pipeline must end in `write`");
    env.sink_name = sinkLabel(env, last.write);
    if (!env.explain and !std.mem.eql(u8, last.write.connector, "stdout")) env.wrote_sink = true;

    if (stages[0].node == .read) implicit: {
        const rd = stages[0].node.read;
        if (rd.form != .table and rd.form != .query) break :implicit;
        const conn = env.connections.get(rd.connector) orelse break :implicit;
        const d: sql.Dialect = if (std.mem.eql(u8, conn.connector, "sqlserver"))
            .sqlserver
        else if (std.mem.eql(u8, conn.connector, "mysql"))
            .mysql
        else if (std.mem.eql(u8, conn.connector, "postgres"))
            .postgres
        else
            break :implicit;
        const extra = (try pushdown.serialWhere(arena, d, stages)) orelse break :implicit;
        const new_stages = try arena.dupe(ast.Stage, stages);
        var nrd = rd;
        nrd.where = if (rd.where.len > 0)
            try std.fmt.allocPrint(arena, "({s}) AND ({s})", .{ rd.where, extra })
        else
            extra;
        new_stages[0].node = .{ .read = nrd };
        stages = new_stages;
    }

    if (stages[0].node == .union_ and opts.threads > 1 and
        !std.mem.eql(u8, last.write.connector, "csv") and
        unionDownstreamMapOnly(stages[1 .. stages.len - 1]))
    {
        return runUnionSplit(env, stages[0].node.union_, stages[0].hints, stages[1 .. stages.len - 1], stages[stages.len - 1], opts, stats, lanes_used, batch_arena);
    }

    if (opts.threads > 1 and stages.len >= 2 and
        stages[0].node == .read and stages[0].hints.len == 0 and isLocalParquetRead(stages[0].node.read))
    {
        if (classifyAggPipeline(stages)) |shape| {
            if (try runParallelParquetAgg(env, stages[0].node.read, stages[0 .. stages.len - 1], shape.prefix, shape.ag, shape.tail, last.write, opts, stats, lanes_used)) return;
        } else if (classifyDistinctPipeline(stages)) |ds| {
            if (try runParallelParquetDistinct(env, stages[0].node.read, stages[0 .. stages.len - 1], ds.prefix, ds.dist, ds.tail, last.write, opts, stats, lanes_used)) return;
        } else if (classifyTopNPipeline(stages)) |tn| {
            if (try runParallelParquetTopN(env, stages[0].node.read, stages[0 .. stages.len - 1], tn.prefix, tn.srt, tn.lim, last.write, opts, stats, lanes_used)) return;
        } else if (classifyMapPipeline(stages)) |map_stages| {
            if (try runParallelParquetMap(env, stages[0].node.read, stages[0 .. stages.len - 1], map_stages, last.write, opts, stats, lanes_used)) return;
        }
    }

    if (opts.threads > 1 and stages.len >= 2 and
        stages[0].node == .read and stages[0].hints.len == 0 and isLocalCsvRead(stages[0].node.read))
    {
        if (classifyAggPipeline(stages)) |shape| {
            if (try runParallelCsvAgg(env, stages[0].node.read, shape.prefix, shape.ag, shape.tail, last.write, opts, stats, lanes_used)) return;
        } else if (classifyDistinctPipeline(stages)) |ds| {
            if (try runParallelCsvDistinct(env, stages[0].node.read, ds.prefix, ds.dist, ds.tail, last.write, opts, stats, lanes_used)) return;
        } else if (classifyTopNPipeline(stages)) |tn| {
            if (try runParallelCsvTopN(env, stages[0].node.read, tn.prefix, tn.srt, tn.lim, last.write, opts, stats, lanes_used)) return;
        } else if (classifyMapPipeline(stages)) |map_stages| {
            if (try runParallelCsvMap(env, stages[0].node.read, map_stages, last.write, opts, stats, lanes_used)) return;
        }
    }

    env.sql_desc = null;
    env.src_name = "";
    const src_base = env.sources.items.len;
    const res = try buildPipeline(env, stages[0 .. stages.len - 1]);

    const wr = try resolveUpsertKeys(env, last.write);

    if (opts.threads > 1 and env.sql_desc != null) {
        if (stages[0].node == .read) {
            if (classifyAggPipeline(stages)) |shape| {
                if (try runParallelSqlAgg(env, stages, shape.prefix, shape.ag, shape.tail, wr, opts, stats, lanes_used, src_base)) return;
            }
        }
        if (try op.linearize(arena, res.op)) |lin| {
            if (try planSplit(env, env.sql_desc.?, stages[0], opts.threads, wr)) |sp| {
                const schema = try dupeSchema(arena, res.schema);

                const middle = stages[1 .. stages.len - 1];
                var lane_stages = lin.stages;
                var proj_select: ?[]const u8 = null;
                var where_extra: ?[]const u8 = null;
                if (stages[0].node == .read) {
                    const src_schema = try dupeSchema(arena, env.sources.items[src_base].schema());
                    const out_cols = try arena.alloc([]const u8, schema.fields.len);
                    for (schema.fields, out_cols) |f, *o| o.* = f.name;
                    const mp = try pushdown.planMap(arena, env.sql_desc.?.dialect, src_schema, middle, out_cols);
                    where_extra = mp.where_extra;
                    if (mp.proj_schema) |ps| {
                        if (rebuildMapStages(env, mp.stages orelse middle, try schemaPtr(arena, ps))) |rs| {
                            lane_stages = rs;
                            proj_select = mp.proj_select;
                        }
                    }
                }

                for (env.sources.items[src_base..]) |sc| sc.close();
                env.sources.shrinkRetainingCapacity(src_base);
                var ctx = SplitCtx{ .gpa = gpa, .kind = env.sql_desc.?.kind, .cfg = env.sql_desc.?.cfg, .base_sql = env.sql_desc.?.base_sql, .proj_select = proj_select, .where_extra = where_extra };
                lanes_used.* = @max(lanes_used.*, @min(opts.threads, sp.predicates.len));
                env.log.log(.debug, "split-parallel: {d} splits over {d} lanes on key range (projection: {s}, filter pushdown: {s})", .{ sp.predicates.len, @min(opts.threads, sp.predicates.len), proj_select orelse "all", if (where_extra != null) "yes" else "no" });
                if (try buildParallelSink(env, wr, schema)) |mode| {
                    stats.rows_out += try parallel.run(gpa, sp.predicates, openSplitSource, &ctx, lane_stages, mode, opts.threads, env.rows_read);
                } else {
                    const snk = try openSink(env, wr, schema);
                    var snk_open = true;
                    errdefer if (snk_open) snk.abort();
                    stats.rows_out += try parallel.run(gpa, sp.predicates, openSplitSource, &ctx, lane_stages, .{ .shared = snk }, opts.threads, env.rows_read);
                    snk_open = false;
                    try snk.close();
                }
                return;
            }
        }
    }

    if (hasFlagHint(stages[0].hints, "buffer")) {
        var batches = std.array_list.Managed(batchmod.Batch).init(arena);
        while (true) {
            if (aborting()) return error.Aborted;
            const b = (try res.op.next(batch_arena.allocator())) orelse break;
            try batches.append(b);
        }
        for (env.sources.items[src_base..]) |sc| sc.close();
        env.sources.shrinkRetainingCapacity(src_base);

        const snk = try openSink(env, wr, res.schema);
        var snk_open = true;
        errdefer if (snk_open) snk.abort();
        for (batches.items) |b| {
            if (aborting()) return error.Aborted;
            try snk.writeBatch(batch_arena.allocator(), b);
            stats.rows_out += b.len;
        }
        snk_open = false;
        try snk.close();
        return;
    }

    const snk = try openSink(env, wr, res.schema);
    var snk_open = true;
    errdefer if (snk_open) snk.abort();

    var pw = parallel.PipelinedSink{ .snk = snk, .gpa = gpa };
    try pw.start();
    defer pw.shutdown();
    var ping_pong: [2]std.heap.ArenaAllocator = .{ std.heap.ArenaAllocator.init(gpa), std.heap.ArenaAllocator.init(gpa) };
    defer for (&ping_pong) |*a| a.deinit();
    var cur: usize = 0;
    while (true) {
        if (aborting()) return error.Aborted;
        const b = (try res.op.next(ping_pong[cur].allocator())) orelse break;
        try pw.submit(b);
        stats.rows_out += b.len;
        cur ^= 1;
        _ = ping_pong[cur].reset(.retain_capacity);
    }
    try pw.finish();
    snk_open = false;
    try snk.close();
    if (opts.explain) try explainTree(arena, res.op);
}

/// Print the operator tree with per-stage actuals. Time is *exclusive*: an
/// operator's own cost with its inputs' subtracted, since a pull pipeline
/// nests children inside the parent's `next`.
fn explainTree(arena: std.mem.Allocator, root: op.Op) !void {
    var buf = std.array_list.Managed(u8).init(arena);
    try buf.appendSlice("plan (actuals, exclusive time)\n");
    try explainNode(arena, root, &buf, 1);
    std.debug.print("{s}", .{buf.items});
}

fn explainNode(arena: std.mem.Allocator, node: op.Op, buf: *std.array_list.Managed(u8), depth: usize) !void {
    var kids = std.array_list.Managed(op.Op).init(arena);
    try node.inputs(&kids);
    var child_ns: u64 = 0;
    for (kids.items) |k| child_ns += k.stats().ns;
    const st = node.stats();
    const excl: u64 = if (st.ns > child_ns) st.ns - child_ns else 0;
    try buf.appendNTimes(' ', depth * 2);
    try buf.writer().print("{s:<10} {d:>9.1}ms {d:>12} rows {d:>8} batches\n", .{
        @tagName(node),
        @as(f64, @floatFromInt(excl)) / 1e6,
        st.rows,
        st.calls,
    });
    for (kids.items) |k| try explainNode(arena, k, buf, depth + 1);
}

/// Gates the mmap'd parallel-CSV fast paths. A `.parquet` path shares the `csv`
/// connector but is not CSV — without this it would be memory-mapped and parsed
/// as text, silently yielding binary garbage instead of rows.
fn isLocalCsvRead(rd: ast.Read) bool {
    if (!std.mem.eql(u8, rd.connector, "csv")) return false;
    return switch (rd.form) {
        .path => |p| !csv.CsvReader.isUrl(p) and !pqdecode.Reader.isPath(p),
        else => false,
    };
}

fn isLocalParquetRead(rd: ast.Read) bool {
    if (!std.mem.eql(u8, rd.connector, "csv")) return false;
    return switch (rd.form) {
        .path => |p| !csv.CsvReader.isUrl(p) and pqdecode.Reader.isPath(p),
        else => false,
    };
}

const AggShape = struct { prefix: []const ast.Stage, ag: ast.Aggregate, tail: []const ast.Stage };

/// Recognize `read … | (filter|select)* | aggregate | (sort|limit)* | write` — the
/// shape the parallel CSV-aggregate path handles: a map-only prefix (folded in
/// parallel), exactly one aggregate (the breaker), and a small post-aggregate tail
/// (run serially on the merged result). Anything else → null (serial path).
fn classifyAggPipeline(stages: []const ast.Stage) ?AggShape {
    const middle = stages[1 .. stages.len - 1];
    var ai: ?usize = null;
    for (middle, 0..) |st, i| switch (st.node) {
        .filter, .select => if (ai != null) return null,
        .aggregate => {
            if (ai != null) return null;
            ai = i;
        },
        .sort, .limit => if (ai == null) return null,
        else => return null,
    };
    const a = ai orelse return null;
    return .{ .prefix = middle[0..a], .ag = middle[a].node.aggregate, .tail = middle[a + 1 ..] };
}

/// Build the map-only prefix (`filter`/`select`) onto `scan` using `ta` (a thread
/// arena). `checkFilter`/`selectCols` are pure (arena + read-only schema/params), so
/// each worker rebuilds its own prefix chain safely from the shared AST stages — the
/// resolution is plan-time-cheap and avoids sharing mutable op state across threads.
fn buildMapChain(ta: std.mem.Allocator, params: *std.StringHashMap(*const ast.Expr), prefix: []const ast.Stage, scan: *op.Scan, csv_schema: *const types.Schema) !op.Op {
    var cur: op.Op = .{ .scan = scan };
    var sch = csv_schema.*;
    for (prefix) |st| switch (st.node) {
        .filter => |pred0| {
            var ad = analyze.Diag{};
            const pred = try analyze.checkFilter(ta, sch, pred0, params, &ad);
            const f = try ta.create(op.Filter);
            f.* = .{ .child = cur, .pred = pred, .err = null };
            cur = .{ .filter = f };
        },
        .select => |items| {
            var ad = analyze.Diag{};
            const rcols = try analyze.selectCols(ta, sch, items, params, &ad);
            const cols = try ta.alloc(op.Project.Col, rcols.len);
            for (rcols, cols) |rc, *c| c.* = .{ .source = switch (rc.source) {
                .passthrough => |x| .{ .passthrough = x },
                .expr => |e| .{ .expr = e },
            }, .ty = rc.ty };
            const out = try ta.create(types.Schema);
            out.* = try analyze.schemaOfCols(ta, rcols);
            const p = try ta.create(op.Project);
            p.* = .{ .child = cur, .cols = cols, .out_schema = out, .err = null };
            cur = .{ .project = p };
            sch = out.*;
        },
        else => unreachable,
    };
    return cur;
}

/// The schema after the map-only prefix — validated once serially (so worker rebuilds
/// can't hit an analyze error) and used as the aggregate's input schema.
fn mapChainSchema(env: *Env, prefix: []const ast.Stage, csv_schema: types.Schema) !types.Schema {
    var sch = csv_schema;
    for (prefix) |st| switch (st.node) {
        .filter => |pred0| {
            var ad = analyze.Diag{};
            _ = analyze.checkFilter(env.arena, sch, pred0, env.params_expr, &ad) catch |e| return aErr(env, &ad, e);
        },
        .select => |items| {
            var ad = analyze.Diag{};
            const rcols = analyze.selectCols(env.arena, sch, items, env.params_expr, &ad) catch |e| return aErr(env, &ad, e);
            sch = analyze.schemaOfCols(env.arena, rcols) catch |e| return aErr(env, &ad, e);
        },
        else => unreachable,
    };
    return sch;
}

/// A `driver.Source` yielding one in-memory batch then EOF — used to run a small
/// post-aggregate `sort`/`limit` tail over the merged result.
const OneBatch = struct {
    b: ?batchmod.Batch,
    sch: types.Schema,
    pub fn source(self: *OneBatch) driver.Source {
        return .{ .ptr = self, .vtable = &ob_vtable };
    }
};
const ob_vtable = driver.Source.VTable{ .schema = obSchema, .next = obNext, .close = obClose };
fn obSchema(ptr: *anyopaque) types.Schema {
    return @as(*OneBatch, @ptrCast(@alignCast(ptr))).sch;
}
fn obNext(ptr: *anyopaque, _: std.mem.Allocator) anyerror!?batchmod.Batch {
    const self: *OneBatch = @ptrCast(@alignCast(ptr));
    defer self.b = null;
    return self.b;
}
fn obClose(_: *anyopaque) void {}

/// Shared header for the parallel workers: a work-stealing item counter plus the
/// first-error latch. `failed` flags the other workers to stop (checked lock-free);
/// `first_err` is what the caller re-raises after the join.
const WorkQueue = struct {
    nitems: usize,
    next: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    err_mtx: std.Thread.Mutex = .{},
    first_err: ?anyerror = null,
    failed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    fn fail(q: *WorkQueue, e: anyerror) void {
        q.err_mtx.lock();
        if (q.first_err == null) q.first_err = e;
        q.err_mtx.unlock();
        q.failed.store(true, .seq_cst);
    }
};

/// The worker-dispatch loop shared by the parallel CSV/SQL paths: steal the next
/// item off `ctx.queue`, run `workOne(ctx, i)`, and latch the first error (which
/// also stops the other workers). `Ctx` only needs a `queue: WorkQueue` field.
fn dispatchWorker(comptime Ctx: type, comptime workOne: anytype) fn (*Ctx, usize) void {
    return struct {
        fn go(ctx: *Ctx, _: usize) void {
            while (true) {
                if (ctx.queue.failed.load(.seq_cst)) return;
                const i = ctx.queue.next.fetchAdd(1, .seq_cst);
                if (i >= ctx.queue.nitems) break;
                workOne(ctx, i) catch |e| {
                    ctx.queue.fail(e);
                    return;
                };
            }
        }
    }.go;
}

/// Finalize merged parallel-aggregate groups into one output batch (a "merger"
/// Aggregate just for `emit`; it never pulls a child).
fn emitMergedGroups(env: *Env, agg_in: *const types.Schema, by: []const usize, aggs: []const op.Aggregate.Agg, out_schema: *const types.Schema, groups: []const op.Aggregate.Group) !batchmod.Batch {
    var merger = op.Aggregate{
        .child = undefined,
        .in_schema = agg_in,
        .by = by,
        .aggs = aggs,
        .out_schema = out_schema,
        .state = env.arena,
        .gpa = env.gpa,
    };
    return merger.emit(env.arena, groups);
}

/// Write a parallel breaker's merged batch to the sink, first running the small
/// post-breaker `sort`/`limit` tail (which preserves `schema`) serially over it.
fn writeTail(env: *Env, snk: driver.Sink, batch: batchmod.Batch, schema: types.Schema, tail: []const ast.Stage, stats: *Stats) !void {
    const arena = env.arena;
    if (tail.len == 0) {
        if (batch.len > 0) {
            try snk.writeBatch(arena, batch);
            stats.rows_out += batch.len;
        }
        return;
    }
    var ob = OneBatch{ .b = batch, .sch = schema };
    var scan = op.Scan{ .src = ob.source() };
    var cur: op.Op = .{ .scan = &scan };
    var sch = schema;
    for (tail) |st| {
        const r = try buildStage(env, st, cur, sch);
        cur = r.op;
        sch = r.schema;
    }
    while (try cur.next(arena)) |outb| {
        try snk.writeBatch(arena, outb);
        stats.rows_out += outb.len;
    }
}

/// Shared state for the parallel-aggregate workers. Each worker rebuilds the prefix
/// chain on its own arena, folds one or more newline-aligned file chunks into a
/// thread-local partial group set, then merges it into the combined set under `mtx`
/// (deep-copying keys/min-max into the plan arena, so its own arena can be freed).
/// The merge is small (O(groups)) vs the fold (O(rows)), so lock contention is low.
const AggCtx = struct {
    mapped: *csv.MappedCsv,
    csv_schema: *const types.Schema,
    agg_in_schema: *const types.Schema,
    out_schema: *const types.Schema,
    prefix: []const ast.Stage,
    params: *std.StringHashMap(*const ast.Expr),
    by: []const usize,
    aggs: []const op.Aggregate.Agg,
    queue: WorkQueue,
    cmap: *op.Aggregate.GroupMap(),
    cgroups: *std.array_list.Managed(op.Aggregate.Group),
    mtx: std.Thread.Mutex = .{},
    plan_arena: std.mem.Allocator,
    rows_read: *std.atomic.Value(u64),
};

const aggWorker = dispatchWorker(AggCtx, aggWorkOne);

fn aggWorkOne(ctx: *AggCtx, i: usize) !void {
    var wgpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = false }){};
    defer _ = wgpa.deinit();
    var warena = std.heap.ArenaAllocator.init(wgpa.allocator());
    defer warena.deinit();
    const wa = warena.allocator();

    var reader = csv.CsvSliceReader{ .data = ctx.mapped.chunk(i, ctx.queue.nitems), .schema = ctx.csv_schema };
    var cs = obs.CountingSource{ .inner = reader.source(), .count = ctx.rows_read };
    var scan = op.Scan{ .src = cs.source() };
    const child = try buildMapChain(wa, ctx.params, ctx.prefix, &scan, ctx.csv_schema);
    var agg = op.Aggregate{
        .child = child,
        .in_schema = ctx.agg_in_schema,
        .by = ctx.by,
        .aggs = ctx.aggs,
        .out_schema = ctx.out_schema,
        .err = null,
        .state = wa,
        .gpa = wgpa.allocator(),
    };
    const groups = try agg.drainGroups();

    ctx.mtx.lock();
    defer ctx.mtx.unlock();
    try op.Aggregate.mergeGroups(ctx.cmap, ctx.cgroups, ctx.plan_arena, groups, ctx.aggs);
}

/// One parallel-aggregate lane: its own arena and its own group table, so the
/// fold phase never takes a lock.
const PqLane = struct {
    arena: std.heap.ArenaAllocator,
    groups: []op.Aggregate.Group = &.{},
    hashes: []u64 = &.{},
    /// The lane's groups split by key hash, computed once when the lane runs
    /// dry. The merge then reads a partition's slice directly instead of
    /// rescanning every lane for every partition.
    buckets: []std.array_list.Managed(u32) = &.{},
};

/// One radix partition of the merge: the lanes' groups are split by key hash, so
/// each partition is owned outright by one task and needs no lock either.
const PqPart = struct {
    arena: std.heap.ArenaAllocator,
    map: op.Aggregate.GroupMap(),
    groups: std.array_list.Managed(op.Aggregate.Group),
};

/// Number of radix partitions. Comfortably above the lane count so the merge
/// stays balanced when key hashes are uneven.
const pq_parts: usize = 64;

/// Fewest lanes worth splitting an aggregate across. Two suffices now that the
/// merge reuses the hashes the fold produced; while it re-hashed every key, the
/// extra pass cost more than two lanes could win back.
const pq_min_lanes: usize = 2;

/// Shared state for parallel *parquet* aggregate lanes. The morsel is a row
/// group: each lane opens its own reader over a disjoint window and folds into
/// its own table. Nothing is shared until the radix merge.
const PqAggCtx = struct {
    morsels: PqMorsels,
    agg_in_schema: *const types.Schema,
    out_schema: *const types.Schema,
    prefix: []const ast.Stage,
    params: *std.StringHashMap(*const ast.Expr),
    by: []const usize,
    aggs: []const op.Aggregate.Agg,
    lanes: []PqLane,
    rows_read: *std.atomic.Value(u64),
};

/// Pulls row-group morsels off the shared queue and presents them as one
/// continuous stream. This is what lets a lane run a *single* aggregate over
/// everything it steals: folding per morsel and merging afterwards would walk
/// the lane's groups an extra time, which at high cardinality costs more than
/// the parallelism buys.
/// The shared row-group work list a parallel parquet path hands to its lanes.
const PqMorsels = struct {
    path: []const u8,
    project: ?[][]const u8,
    bounds: []const pqdecode.Bound,
    src_schema: *const types.Schema,
    per_item: usize,
    queue: WorkQueue,
};

const MorselSource = struct {
    m: *PqMorsels,
    scratch: std.mem.Allocator,
    cur: ?*pqdecode.Reader = null,

    fn schemaFn(ptr: *anyopaque) types.Schema {
        const self: *MorselSource = @ptrCast(@alignCast(ptr));
        return self.m.src_schema.*;
    }
    fn nextFn(ptr: *anyopaque, arena: std.mem.Allocator) anyerror!?batchmod.Batch {
        const self: *MorselSource = @ptrCast(@alignCast(ptr));
        while (true) {
            if (self.cur) |r| {
                if (try r.next(arena)) |b| return b;
                self.cur = null;
            }
            if (self.m.queue.failed.load(.seq_cst)) return null;
            const i = self.m.queue.next.fetchAdd(1, .seq_cst);
            if (i >= self.m.queue.nitems) return null;
            const r = try pqdecode.Reader.openProjected(self.scratch, self.m.path, self.m.project);
            r.bounds = self.m.bounds;
            r.rg = i * self.m.per_item;
            if (r.rg >= r.md.row_groups.len) continue;
            r.rg_end = @min(r.rg + self.m.per_item, r.md.row_groups.len);
            self.cur = r;
        }
    }
    fn closeFn(ptr: *anyopaque) void {
        _ = ptr;
    }
    const vtable = driver.Source.VTable{ .schema = schemaFn, .next = nextFn, .close = closeFn };
};

fn pqAggLane(ctx: *PqAggCtx, lane_idx: usize) void {
    pqAggLaneRun(ctx, lane_idx) catch |e| ctx.morsels.queue.fail(e);
}

fn pqAggLaneRun(ctx: *PqAggCtx, lane_idx: usize) !void {
    const ls = &ctx.lanes[lane_idx];
    const la = ls.arena.allocator();

    var ms = MorselSource{ .m = &ctx.morsels, .scratch = la };
    var cs = obs.CountingSource{ .inner = .{ .ptr = &ms, .vtable = &MorselSource.vtable }, .count = ctx.rows_read };
    var scan = op.Scan{ .src = cs.source() };
    const child = try buildMapChain(la, ctx.params, ctx.prefix, &scan, ctx.morsels.src_schema);
    var agg = op.Aggregate{
        .child = child,
        .in_schema = ctx.agg_in_schema,
        .by = ctx.by,
        .aggs = ctx.aggs,
        .out_schema = ctx.out_schema,
        .err = null,
        .state = la,
        .gpa = ls.arena.child_allocator,
    };
    const drained = try agg.drainGroupsHashed();
    ls.groups = drained.groups;
    ls.hashes = drained.hashes;
    try bucketLane(ls);
}

/// Split this lane's groups into radix buckets — one hash per group, once.
/// Split this lane's group indices into radix buckets, reusing the hash the
/// fold already produced.
fn bucketLane(ls: *PqLane) !void {
    const a = ls.arena.allocator();
    ls.buckets = try a.alloc(std.array_list.Managed(u32), pq_parts);
    for (ls.buckets) |*b| b.* = std.array_list.Managed(u32).init(a);
    for (ls.hashes, 0..) |h, i| {
        const p: usize = @intCast((h >> 32) % pq_parts);
        try ls.buckets[p].append(@intCast(i));
    }
}

const PqMergeCtx = struct {
    lanes: []PqLane,
    parts: []PqPart,
    aggs: []const op.Aggregate.Agg,
    queue: WorkQueue,
};

const pqMergeWorker = dispatchWorker(PqMergeCtx, pqMergeOne);

fn pqMergeOne(ctx: *PqMergeCtx, p: usize) !void {
    const dst = &ctx.parts[p];
    const da = dst.arena.allocator();
    var tbl = try op.Aggregate.MergeTable.init(da, 256);
    var hashes = std.array_list.Managed(u64).init(da);
    var any_distinct = false;
    for (ctx.aggs) |a| {
        if (a.distinct) any_distinct = true;
    }
    for (ctx.lanes) |*ls| {
        if (ls.buckets.len == 0) continue;
        for (ls.buckets[p].items) |gi| {
            const g = ls.groups[gi];
            const h = ls.hashes[gi];
            if (try tbl.find(h, g.key_vals, dst.groups.items, hashes.items, dst.groups.items.len)) |at| {
                const cg = &dst.groups.items[at];
                for (g.accs, ctx.aggs, 0..) |src, agg, j| try op.Aggregate.mergeAcc(da, &cg.accs[j], src, agg);
            } else {
                try dst.groups.append(try op.Aggregate.adoptOne(da, g, ctx.aggs, any_distinct));
                try hashes.append(h);
            }
        }
    }
}

/// The `sort … limit` shape a parallel aggregate can push into its partitions.
const TopNTail = struct { keys: []const ast.SortKey, n: usize };

/// Recognise a tail that is only sorts and limits, so each partition can drop to
/// its own best `n` rows before anything is materialised. An `OFFSET` disables
/// it: the rows a partition discards could be the ones the offset lands on.
fn topNTail(tail: []const ast.Stage) ?TopNTail {
    var keys: []const ast.SortKey = &.{};
    var n: ?usize = null;
    for (tail) |st| switch (st.node) {
        .sort => |so| keys = so.keys,
        .limit => |l| {
            if (l.offset != 0) return null;
            n = @intCast(l.count);
        },
        else => return null,
    };
    if (keys.len == 0) return null;
    return .{ .keys = keys, .n = n orelse return null };
}

/// The output value a sort key refers to: the group keys come first in the
/// aggregate's schema, the aggregates after them.
fn groupSortValue(g: op.Aggregate.Group, col: usize, by_len: usize, aggs: []const op.Aggregate.Agg) Value {
    if (col < by_len) return g.key_vals[col];
    return op.Aggregate.finalizeAcc(g.accs[col - by_len], aggs[col - by_len]);
}

const GroupOrder = struct {
    cols: []const usize,
    desc: []const bool,
    by_len: usize,
    aggs: []const op.Aggregate.Agg,

    fn less(self: GroupOrder, a: op.Aggregate.Group, b: op.Aggregate.Group) bool {
        for (self.cols, self.desc) |c, d| {
            const av = groupSortValue(a, c, self.by_len, self.aggs);
            const bv = groupSortValue(b, c, self.by_len, self.aggs);
            const o = eval.compareValues(av, bv) orelse .eq;
            if (o != .eq) return if (d) o == .gt else o == .lt;
        }
        return false;
    }
};

const PqTopNCtx = struct {
    parts: []PqPart,
    order: GroupOrder,
    n: usize,
    queue: WorkQueue,
};

const pqTopNWorker = dispatchWorker(PqTopNCtx, pqTopNOne);

fn pqTopNOne(ctx: *PqTopNCtx, p: usize) !void {
    const g = &ctx.parts[p].groups;
    if (g.items.len <= ctx.n) return;
    std.sort.pdq(op.Aggregate.Group, g.items, ctx.order, GroupOrder.less);
    g.shrinkRetainingCapacity(ctx.n);
}

/// Map-only pipeline (scan -> filter/project/explode -> write) over a local
/// parquet file. Each lane pulls row-group morsels and writes its own output,
/// so nothing is shared except the work list and, for a shared sink, the write
/// lock.
const PqMapCtx = struct {
    morsels: PqMorsels,
    map_stages: []const ast.Stage,
    params: *std.StringHashMap(*const ast.Expr),
    sink_mode: parallel.SinkMode,
    sink_mtx: std.Thread.Mutex = .{},
    rows_out: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    rows_read: *std.atomic.Value(u64),
};

fn pqMapLane(ctx: *PqMapCtx, lane_idx: usize) void {
    pqMapLaneRun(ctx, lane_idx) catch |e| ctx.morsels.queue.fail(e);
}

fn pqMapLaneRun(ctx: *PqMapCtx, lane_idx: usize) !void {
    var wgpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = false }){};
    defer _ = wgpa.deinit();
    const wa = wgpa.allocator();
    var warena = std.heap.ArenaAllocator.init(wa);
    defer warena.deinit();
    var batch_arena = std.heap.ArenaAllocator.init(wa);
    defer batch_arena.deinit();

    const own_sink: ?driver.Sink = switch (ctx.sink_mode) {
        .shared => null,
        .per_lane => |pl| try pl.open(pl.ctx, wa, lane_idx),
    };
    var own_sink_open = own_sink != null;
    defer if (own_sink) |sk| {
        if (own_sink_open) sk.abort();
    };

    var ms = MorselSource{ .m = &ctx.morsels, .scratch = warena.allocator() };
    var cs = obs.CountingSource{ .inner = .{ .ptr = &ms, .vtable = &MorselSource.vtable }, .count = ctx.rows_read };
    var scan = op.Scan{ .src = cs.source() };
    const chain = try buildMapChain(warena.allocator(), ctx.params, ctx.map_stages, &scan, ctx.morsels.src_schema);

    var out: u64 = 0;
    while (try chain.next(batch_arena.allocator())) |b| {
        try parallel.writeLaneBatch(ctx.sink_mode, &ctx.sink_mtx, own_sink, batch_arena.allocator(), b);
        out += b.len;
        _ = batch_arena.reset(.retain_capacity);
    }
    _ = ctx.rows_out.fetchAdd(out, .monotonic);
    if (own_sink) |sk| {
        own_sink_open = false;
        try sk.close();
    }
}

fn runParallelParquetMap(env: *Env, rd: ast.Read, pipeline: []const ast.Stage, map_stages: []const ast.Stage, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    const arena = env.arena;
    const path = switch (rd.form) {
        .path => |p| p,
        else => return false,
    };
    if (std.mem.eql(u8, w.connector, "stdout")) return false;
    if (w.mode == .upsert and w.mode.upsert.keys.len == 0) return false;
    if (opts.threads < 2) return false;

    const project = try projectedColumns(env, pipeline[1..]);
    const bounds = try filterBounds(env, pipeline[1..]);
    const probe = pqdecode.Reader.openProjected(arena, path, project) catch return false;
    const ngroups = probe.md.row_groups.len;
    if (ngroups < 2) return false;

    const src_schema = try schemaPtr(arena, probe.schema);
    const out_schema = try mapChainSchema(env, map_stages, src_schema.*);

    env.src_name = "parquet";
    env.sink_name = sinkLabel(env, w);

    const wr = try resolveUpsertKeys(env, w);
    const sink_mode: parallel.SinkMode = (try buildParallelSink(env, wr, out_schema)) orelse
        .{ .shared = try openSink(env, wr, out_schema) };
    var shared_open = sink_mode == .shared;
    errdefer if (shared_open) sink_mode.shared.abort();

    const nthreads = @max(@as(usize, 1), opts.threads);
    var ctx = PqMapCtx{
        .morsels = .{
            .path = path,
            .project = project,
            .bounds = bounds,
            .src_schema = src_schema,
            .per_item = 1,
            .queue = .{ .nitems = ngroups },
        },
        .map_stages = map_stages,
        .params = env.params_expr,
        .sink_mode = sink_mode,
        .rows_read = env.rows_read,
    };

    const lanes = try parallel.spawnJoin(arena, nthreads, pqMapLane, &ctx);
    lanes_used.* = @max(lanes_used.*, lanes);
    env.log.log(.debug, "parallel parquet map: {d} row groups over {d} lanes ({s} sink)", .{ ngroups, lanes, @tagName(sink_mode) });
    if (opts.explain) {
        std.debug.print("actuals (parallel map, {d} lanes over {d} row groups): {d} rows out\n", .{
            lanes, ngroups, ctx.rows_out.load(.monotonic),
        });
    }

    if (ctx.morsels.queue.first_err) |e| return e;
    stats.rows_out += ctx.rows_out.load(.monotonic);
    if (sink_mode == .shared) {
        shared_open = false;
        try sink_mode.shared.close();
    }
    return true;
}

/// Parallel aggregate over a local parquet file, one morsel per row group.
/// Parallel aggregate over a local parquet file, one morsel per row group.
/// Two phases, neither of which takes a lock: lanes fold disjoint row groups
/// into private tables, then the tables are merged by hash partition.
///
/// Returns false (serial fallback) when the file has too few row groups to
/// split, or when fewer than `pq_min_lanes` lanes are available.
fn runParallelParquetAgg(env: *Env, rd: ast.Read, pipeline: []const ast.Stage, prefix: []const ast.Stage, ag: ast.Aggregate, tail: []const ast.Stage, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    const arena = env.arena;
    const path = switch (rd.form) {
        .path => |p| p,
        else => return false,
    };
    if (w.mode == .upsert and w.mode.upsert.keys.len == 0) return false;
    if (opts.threads < pq_min_lanes) return false;

    const project = try projectedColumns(env, pipeline[1..]);
    const bounds = try filterBounds(env, pipeline[1..]);
    const probe = pqdecode.Reader.openProjected(arena, path, project) catch return false;
    const ngroups = probe.md.row_groups.len;
    if (ngroups < 2) return false;

    const src_schema = try schemaPtr(arena, probe.schema);
    const agg_in = try schemaPtr(arena, try mapChainSchema(env, prefix, src_schema.*));

    var ad = analyze.Diag{};
    const apl = analyze.aggregatePlan(arena, agg_in.*, ag, env.params_expr, &ad) catch |e| return aErr(env, &ad, e);
    // An ungrouped aggregate (`SELECT COUNT(*) FROM ...`, no GROUP BY) has no
    // key to hash, so every lane folds into zero groups and the merge emits the
    // identity — COUNT(*) came back 0 instead of the row count, silently. The
    // two-phase fold-then-radix-merge below is grouped-aggregate machinery; the
    // ungrouped case belongs on the serial path, which is correct.
    if (apl.by.len == 0) return false;

    const aggs = try arena.alloc(op.Aggregate.Agg, apl.aggs.len);
    for (apl.aggs, aggs) |ra, *a| a.* = .{ .func = ra.func, .arg = ra.arg, .ty = ra.ty, .distinct = ra.distinct };
    const out_schema = try schemaPtr(arena, apl.schema);

    env.src_name = "parquet";
    env.sink_name = sinkLabel(env, w);

    const nthreads = @max(@as(usize, 1), opts.threads);
    const per_item: usize = 1;
    const nitems = ngroups;

    const lanes = try env.gpa.alloc(PqLane, nthreads);
    defer env.gpa.free(lanes);
    for (lanes) |*l| {
        l.* = .{ .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator) };
    }
    defer for (lanes) |*l| l.arena.deinit();

    var ctx = PqAggCtx{
        .morsels = .{
            .path = path,
            .project = project,
            .bounds = bounds,
            .src_schema = src_schema,
            .per_item = per_item,
            .queue = .{ .nitems = nitems },
        },
        .agg_in_schema = agg_in,
        .out_schema = out_schema,
        .prefix = prefix,
        .params = env.params_expr,
        .by = apl.by,
        .aggs = aggs,
        .lanes = lanes,
        .rows_read = env.rows_read,
    };

    const t_fold0 = std.time.Instant.now() catch unreachable;
    const used = try parallel.spawnJoin(arena, nthreads, pqAggLane, &ctx);
    const t_fold1 = std.time.Instant.now() catch unreachable;
    lanes_used.* = @max(lanes_used.*, used);
    if (ctx.morsels.queue.first_err) |e| return e;

    const parts = try env.gpa.alloc(PqPart, pq_parts);
    defer env.gpa.free(parts);
    for (parts) |*pp| {
        pp.* = .{ .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator), .map = undefined, .groups = undefined };
        pp.map = op.Aggregate.GroupMap().init(pp.arena.allocator());
        pp.groups = std.array_list.Managed(op.Aggregate.Group).init(pp.arena.allocator());
    }
    defer for (parts) |*pp| pp.arena.deinit();

    var mctx = PqMergeCtx{ .lanes = lanes, .parts = parts, .aggs = aggs, .queue = .{ .nitems = pq_parts } };
    const t_mrg0 = std.time.Instant.now() catch unreachable;
    _ = try parallel.spawnJoin(arena, nthreads, pqMergeWorker, &mctx);
    const t_mrg1 = std.time.Instant.now() catch unreachable;
    if (mctx.queue.first_err) |e| return e;
    var lane_groups: usize = 0;
    for (lanes) |*l| lane_groups += l.groups.len;
    env.log.log(.debug, "pq agg phases: fold {d}ms (incl. bucket), merge {d}ms, {d} lane groups", .{
        t_fold1.since(t_fold0) / 1_000_000,
        t_mrg1.since(t_mrg0) / 1_000_000,
        lane_groups,
    });
    if (opts.explain) {
        std.debug.print(
            \\actuals (parallel aggregate, {d} lanes over {d} row groups):
            \\  fold+bucket  {d:>8.1}ms {d:>12} groups
            \\  merge        {d:>8.1}ms {d:>12} partitions
            \\
        , .{
            used,                                                        ngroups,
            @as(f64, @floatFromInt(t_fold1.since(t_fold0))) / 1e6,        lane_groups,
            @as(f64, @floatFromInt(t_mrg1.since(t_mrg0))) / 1e6,          pq_parts,
        });
    }

    env.log.log(.debug, "parallel parquet aggregate: {d} row groups in {d} morsels over {d} lanes, merged in {d} partitions", .{ ngroups, nitems, used, pq_parts });

    if (topNTail(tail)) |tn| {
        var cols = std.array_list.Managed(usize).init(arena);
        var descs = std.array_list.Managed(bool).init(arena);
        var ok = true;
        for (tn.keys) |k| {
            const idx = out_schema.indexOf(k.field.last()) orelse {
                ok = false;
                break;
            };
            try cols.append(idx);
            try descs.append(k.desc);
        }
        if (ok) {
            var tctx = PqTopNCtx{
                .parts = parts,
                .order = .{ .cols = cols.items, .desc = descs.items, .by_len = apl.by.len, .aggs = aggs },
                .n = tn.n,
                .queue = .{ .nitems = pq_parts },
            };
            _ = try parallel.spawnJoin(arena, nthreads, pqTopNWorker, &tctx);
            if (tctx.queue.first_err) |e| return e;
        }
    }

    const t_emit0 = std.time.Instant.now() catch unreachable;
    var total: usize = 0;
    for (parts) |*pp| total += pp.groups.items.len;
    const all = try arena.alloc(op.Aggregate.Group, total);
    var at: usize = 0;
    for (parts) |*pp| {
        for (pp.groups.items) |g| {
            all[at] = g;
            at += 1;
        }
    }

    const batch = try emitMergedGroups(env, agg_in, apl.by, aggs, out_schema, all);
    const t_emit1 = std.time.Instant.now() catch unreachable;

    const wr = try resolveUpsertKeys(env, w);
    const snk = try openSink(env, wr, out_schema.*);
    var snk_open = true;
    errdefer if (snk_open) snk.abort();

    const t_tail0 = std.time.Instant.now() catch unreachable;
    try writeTail(env, snk, batch, out_schema.*, tail, stats);
    const t_tail1 = std.time.Instant.now() catch unreachable;
    env.log.log(.debug, "pq agg tail: emit {d}ms ({d} groups), sort+limit+write {d}ms", .{
        t_emit1.since(t_emit0) / 1_000_000, total, t_tail1.since(t_tail0) / 1_000_000,
    });
    if (opts.explain) {
        std.debug.print(
            \\  emit         {d:>8.1}ms {d:>12} rows
            \\  sort+write   {d:>8.1}ms
            \\
        , .{
            @as(f64, @floatFromInt(t_emit1.since(t_emit0))) / 1e6, total,
            @as(f64, @floatFromInt(t_tail1.since(t_tail0))) / 1e6,
        });
    }
    snk_open = false;
    try snk.close();
    return true;
}

/// Returns true if it handled the pipeline in parallel; false to fall back to serial.
fn runParallelCsvAgg(env: *Env, rd: ast.Read, prefix: []const ast.Stage, ag: ast.Aggregate, tail: []const ast.Stage, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    const arena = env.arena;
    const path = switch (rd.form) {
        .path => |p| p,
        else => return false,
    };
    if (w.mode == .upsert and w.mode.upsert.keys.len == 0) return false;

    const mapped = csv.MappedCsv.open(arena, path) catch return false;
    defer mapped.close();

    const agg_in = try schemaPtr(arena, try mapChainSchema(env, prefix, mapped.schema));

    var ad = analyze.Diag{};
    const apl = analyze.aggregatePlan(arena, agg_in.*, ag, env.params_expr, &ad) catch |e| return aErr(env, &ad, e);
    const aggs = try arena.alloc(op.Aggregate.Agg, apl.aggs.len);
    for (apl.aggs, aggs) |ra, *a| a.* = .{ .func = ra.func, .arg = ra.arg, .ty = ra.ty, .distinct = ra.distinct };
    const out_schema = try schemaPtr(arena, apl.schema);

    env.src_name = "csv";
    env.sink_name = sinkLabel(env, w);

    const nthreads = @max(@as(usize, 1), opts.threads);
    var cmap = op.Aggregate.GroupMap().init(arena);
    var cgroups = std.array_list.Managed(op.Aggregate.Group).init(arena);
    var ctx = AggCtx{
        .mapped = mapped,
        .csv_schema = &mapped.schema,
        .agg_in_schema = agg_in,
        .out_schema = out_schema,
        .prefix = prefix,
        .params = env.params_expr,
        .by = apl.by,
        .aggs = aggs,
        .queue = .{ .nitems = nthreads },
        .cmap = &cmap,
        .cgroups = &cgroups,
        .plan_arena = arena,
        .rows_read = env.rows_read,
    };

    const lanes = try parallel.spawnJoin(arena, nthreads, aggWorker, &ctx);
    lanes_used.* = @max(lanes_used.*, lanes);
    env.log.log(.debug, "parallel csv aggregate: {d} chunks over {d} lanes", .{ nthreads, lanes });

    if (ctx.queue.first_err) |e| return e;

    const batch = try emitMergedGroups(env, agg_in, apl.by, aggs, out_schema, cgroups.items);

    const wr = try resolveUpsertKeys(env, w);
    const snk = try openSink(env, wr, out_schema.*);
    var snk_open = true;
    errdefer if (snk_open) snk.abort();

    try writeTail(env, snk, batch, out_schema.*, tail, stats);
    snk_open = false;
    try snk.close();
    return true;
}

/// Shared state for parallel SQL-aggregate lanes. Mirrors `AggCtx`, but each lane opens
/// its own DB connection over one key-range predicate (`openSplitSource`) instead of
/// reading a CSV byte-range. Each lane folds its range into a thread-local partial group
/// set, then merges it into the combined set under `mtx` at the raw-`Acc` level (so AVG
/// stays correct). The merge is O(groups) vs the O(rows) fold, so contention is low.
const SqlAggCtx = struct {
    split: SplitCtx,
    predicates: []const []const u8,
    proj_select: ?[]const u8,
    where_extra: ?[]const u8,
    src_schema: *const types.Schema,
    agg_in_schema: *const types.Schema,
    out_schema: *const types.Schema,
    prefix: []const ast.Stage,
    params: *std.StringHashMap(*const ast.Expr),
    by: []const usize,
    aggs: []const op.Aggregate.Agg,
    queue: WorkQueue,
    cmap: *op.Aggregate.GroupMap(),
    cgroups: *std.array_list.Managed(op.Aggregate.Group),
    mtx: std.Thread.Mutex = .{},
    plan_arena: std.mem.Allocator,
    rows_read: *std.atomic.Value(u64),
};

const sqlAggWorker = dispatchWorker(SqlAggCtx, sqlAggWorkOne);

fn sqlAggWorkOne(ctx: *SqlAggCtx, i: usize) !void {
    var wgpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = false }){};
    defer _ = wgpa.deinit();
    var warena = std.heap.ArenaAllocator.init(wgpa.allocator());
    defer warena.deinit();
    const wa = warena.allocator();

    const q = try splitmod.wrapProjected(wa, ctx.split.base_sql, ctx.proj_select, ctx.predicates[i], ctx.where_extra);
    const src = try openSqlQuery(&ctx.split, wgpa.allocator(), q);
    defer src.close();

    var cs = obs.CountingSource{ .inner = src, .count = ctx.rows_read };
    var scan = op.Scan{ .src = cs.source() };
    const child = try buildMapChain(wa, ctx.params, ctx.prefix, &scan, ctx.src_schema);
    var agg = op.Aggregate{
        .child = child,
        .in_schema = ctx.agg_in_schema,
        .by = ctx.by,
        .aggs = ctx.aggs,
        .out_schema = ctx.out_schema,
        .err = null,
        .state = wa,
        .gpa = wgpa.allocator(),
    };
    const groups = try agg.drainGroups();

    ctx.mtx.lock();
    defer ctx.mtx.unlock();
    try op.Aggregate.mergeGroups(ctx.cmap, ctx.cgroups, ctx.plan_arena, groups, ctx.aggs);
}

/// Parallel SQL aggregate: `read <sqltable> | (filter|select)* | aggregate | (sort|limit)* | write`
/// over a splittable source. Fans into key-range lanes (one DB connection each), folds a
/// partial group set per lane, merges at the raw-`Acc` level, then runs the small
/// post-aggregate tail serially over the merged batch. Returns false to fall back to the
/// serial path (non-splittable source, no split plan, bare upsert). NOTE: exercised only
/// against a live DB — there is no local DB in the test suite, so this path is covered by
/// the shared CSV-aggregate machinery (`drainGroups`/`mergeGroups`/`emit`) it reuses, not
/// by a direct test.
fn runParallelSqlAgg(env: *Env, stages: []const ast.Stage, prefix: []const ast.Stage, ag: ast.Aggregate, tail: []const ast.Stage, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize, src_base: usize) anyerror!bool {
    const arena = env.arena;
    const desc = env.sql_desc orelse return false;
    if (stages[0].node != .read) return false;
    if (w.mode == .upsert and w.mode.upsert.keys.len == 0) return false;
    if (src_base >= env.sources.items.len) return false;

    const src_schema = try schemaPtr(arena, try dupeSchema(arena, env.sources.items[src_base].schema()));

    const pd = try pushdown.planAgg(arena, desc.dialect, src_schema.*, prefix, ag);
    const eff = if (pd.proj_schema) |ps| try schemaPtr(arena, ps) else src_schema;

    const agg_in = try schemaPtr(arena, try mapChainSchema(env, prefix, eff.*));

    var ad = analyze.Diag{};
    const apl = analyze.aggregatePlan(arena, agg_in.*, ag, env.params_expr, &ad) catch |e| return aErr(env, &ad, e);
    const aggs = try arena.alloc(op.Aggregate.Agg, apl.aggs.len);
    for (apl.aggs, aggs) |ra, *a| a.* = .{ .func = ra.func, .arg = ra.arg, .ty = ra.ty, .distinct = ra.distinct };
    const out_schema = try schemaPtr(arena, apl.schema);

    const sp = (try planSplit(env, desc, stages[0], opts.threads, w)) orelse return false;

    for (env.sources.items[src_base..]) |sc| sc.close();
    env.sources.shrinkRetainingCapacity(src_base);

    env.sink_name = sinkLabel(env, w);

    const nlanes = @min(@max(@as(usize, 1), opts.threads), sp.predicates.len);
    var cmap = op.Aggregate.GroupMap().init(arena);
    var cgroups = std.array_list.Managed(op.Aggregate.Group).init(arena);
    var ctx = SqlAggCtx{
        .split = .{ .gpa = env.gpa, .kind = desc.kind, .cfg = desc.cfg, .base_sql = desc.base_sql },
        .predicates = sp.predicates,
        .proj_select = pd.proj_select,
        .where_extra = pd.where_extra,
        .src_schema = eff,
        .agg_in_schema = agg_in,
        .out_schema = out_schema,
        .prefix = prefix,
        .params = env.params_expr,
        .by = apl.by,
        .aggs = aggs,
        .queue = .{ .nitems = sp.predicates.len },
        .cmap = &cmap,
        .cgroups = &cgroups,
        .plan_arena = arena,
        .rows_read = env.rows_read,
    };

    const lanes = try parallel.spawnJoin(arena, nlanes, sqlAggWorker, &ctx);
    lanes_used.* = @max(lanes_used.*, lanes);
    env.log.log(.debug, "parallel sql aggregate: {d} splits over {d} lanes on key range (projection: {d}/{d} cols, filter pushdown: {s})", .{
        sp.predicates.len,
        lanes,
        if (pd.proj_schema) |ps| ps.fields.len else src_schema.fields.len,
        src_schema.fields.len,
        if (pd.where_extra != null) "yes" else "no",
    });

    if (ctx.queue.first_err) |e| return e;

    const batch = try emitMergedGroups(env, agg_in, apl.by, aggs, out_schema, cgroups.items);

    const snk = try openSink(env, w, out_schema.*);
    var snk_open = true;
    errdefer if (snk_open) snk.abort();

    try writeTail(env, snk, batch, out_schema.*, tail, stats);
    snk_open = false;
    try snk.close();
    return true;
}

/// Recognize a map-only `read … | (filter|select)* | write` (no breaker, no limit),
/// returning the middle stages. Such a pipeline parallelizes by byte-range chunks
/// with each worker writing to a shared sink. null → not eligible.
fn classifyMapPipeline(stages: []const ast.Stage) ?[]const ast.Stage {
    const middle = stages[1 .. stages.len - 1];
    for (middle) |st| switch (st.node) {
        .filter, .select => {},
        else => return null,
    };
    return middle;
}

const MapCtx = struct {
    mapped: *csv.MappedCsv,
    csv_schema: *const types.Schema,
    map_stages: []const ast.Stage,
    params: *std.StringHashMap(*const ast.Expr),
    queue: WorkQueue,
    sink_mode: parallel.SinkMode,
    sink_mtx: std.Thread.Mutex = .{},
    rows_out: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    rows_read: *std.atomic.Value(u64),
};

const mapWorker = dispatchWorker(MapCtx, mapWorkOne);

fn mapWorkOne(ctx: *MapCtx, i: usize) !void {
    var wgpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = false }){};
    defer _ = wgpa.deinit();
    const wa = wgpa.allocator();
    var warena = std.heap.ArenaAllocator.init(wa);
    defer warena.deinit();
    var batch_arena = std.heap.ArenaAllocator.init(wa);
    defer batch_arena.deinit();

    const own_sink: ?driver.Sink = switch (ctx.sink_mode) {
        .shared => null,
        .per_lane => |pl| try pl.open(pl.ctx, wa, i),
    };
    var own_sink_open = own_sink != null;
    defer if (own_sink) |s| {
        if (own_sink_open) s.abort();
    };

    var reader = csv.CsvSliceReader{ .data = ctx.mapped.chunk(i, ctx.queue.nitems), .schema = ctx.csv_schema };
    var cs = obs.CountingSource{ .inner = reader.source(), .count = ctx.rows_read };
    var scan = op.Scan{ .src = cs.source() };
    const chain = try buildMapChain(warena.allocator(), ctx.params, ctx.map_stages, &scan, ctx.csv_schema);

    while (try chain.next(batch_arena.allocator())) |b| {
        if (ctx.queue.failed.load(.seq_cst)) return;
        if (b.len > 0) {
            try parallel.writeLaneBatch(ctx.sink_mode, &ctx.sink_mtx, own_sink, batch_arena.allocator(), b);
            _ = ctx.rows_out.fetchAdd(b.len, .monotonic);
        }
        _ = batch_arena.reset(.retain_capacity);
    }

    if (own_sink) |s| {
        own_sink_open = false;
        try s.close();
    }
}

/// Parallel map-only CSV pipeline (`read csv <local> | (filter|select)* | write`):
/// fans the file into byte-range chunks, runs the map chain on each worker, and
/// writes batches to a shared sink under a mutex. Row ORDER is not preserved (chunks
/// interleave); use `-j 1` for a deterministic order. Returns false to fall back.
fn runParallelCsvMap(env: *Env, rd: ast.Read, map_stages: []const ast.Stage, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    const arena = env.arena;
    const path = switch (rd.form) {
        .path => |p| p,
        else => return false,
    };
    if (std.mem.eql(u8, w.connector, "stdout")) return false;
    if (w.mode == .upsert and w.mode.upsert.keys.len == 0) return false;

    const mapped = csv.MappedCsv.open(arena, path) catch return false;
    defer mapped.close();

    const out_schema = try mapChainSchema(env, map_stages, mapped.schema);

    env.src_name = "csv";
    env.sink_name = sinkLabel(env, w);

    const wr = try resolveUpsertKeys(env, w);
    const sink_mode: parallel.SinkMode = (try buildParallelSink(env, wr, out_schema)) orelse
        .{ .shared = try openSink(env, wr, out_schema) };
    var shared_open = sink_mode == .shared;
    errdefer if (shared_open) sink_mode.shared.abort();

    const nthreads = @max(@as(usize, 1), opts.threads);
    var ctx = MapCtx{
        .mapped = mapped,
        .csv_schema = &mapped.schema,
        .map_stages = map_stages,
        .params = env.params_expr,
        .queue = .{ .nitems = nthreads },
        .sink_mode = sink_mode,
        .rows_read = env.rows_read,
    };

    const lanes = try parallel.spawnJoin(arena, nthreads, mapWorker, &ctx);
    lanes_used.* = @max(lanes_used.*, lanes);
    env.log.log(.debug, "parallel csv map: {d} chunks over {d} lanes ({s} sink)", .{ nthreads, lanes, @tagName(sink_mode) });

    if (ctx.queue.first_err) |e| return e;
    stats.rows_out += ctx.rows_out.load(.monotonic);
    if (sink_mode == .shared) {
        shared_open = false;
        try sink_mode.shared.close();
    }
    return true;
}

const TopNShape = struct { prefix: []const ast.Stage, srt: ast.Sort, lim: ast.Limit };

/// Recognize `read … | (filter|select)* | sort | limit | write` — a Top-N. The sort
/// must be immediately followed by the limit (the fusable form). null otherwise.
fn classifyTopNPipeline(stages: []const ast.Stage) ?TopNShape {
    const middle = stages[1 .. stages.len - 1];
    if (middle.len < 2) return null;
    if (middle[middle.len - 1].node != .limit or middle[middle.len - 2].node != .sort) return null;
    const prefix = middle[0 .. middle.len - 2];
    for (prefix) |st| switch (st.node) {
        .filter, .select => {},
        else => return null,
    };
    return .{ .prefix = prefix, .srt = middle[middle.len - 2].node.sort, .lim = middle[middle.len - 1].node.limit };
}

const TopNCtx = struct {
    mapped: *csv.MappedCsv,
    csv_schema: *const types.Schema,
    row_schema: *const types.Schema,
    prefix: []const ast.Stage,
    params: *std.StringHashMap(*const ast.Expr),
    keys: []const op.Sort.Key,
    cap: u64,
    queue: WorkQueue,
    builders: []column.Builder,
    mtx: std.Thread.Mutex = .{},
    rows_read: *std.atomic.Value(u64),
};

const topnWorker = dispatchWorker(TopNCtx, topnWorkOne);

fn topnWorkOne(ctx: *TopNCtx, i: usize) !void {
    var wgpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = false }){};
    defer _ = wgpa.deinit();
    var warena = std.heap.ArenaAllocator.init(wgpa.allocator());
    defer warena.deinit();
    var batch_arena = std.heap.ArenaAllocator.init(wgpa.allocator());
    defer batch_arena.deinit();

    var reader = csv.CsvSliceReader{ .data = ctx.mapped.chunk(i, ctx.queue.nitems), .schema = ctx.csv_schema };
    var cs = obs.CountingSource{ .inner = reader.source(), .count = ctx.rows_read };
    var scan = op.Scan{ .src = cs.source() };
    const child = try buildMapChain(warena.allocator(), ctx.params, ctx.prefix, &scan, ctx.csv_schema);
    var tn = op.TopN{
        .child = child,
        .in_schema = ctx.row_schema,
        .keys = ctx.keys,
        .count = ctx.cap,
        .offset = 0,
        .state = batch_arena.allocator(),
        .gpa = wgpa.allocator(),
    };
    const local = (try tn.next(batch_arena.allocator())) orelse return;

    ctx.mtx.lock();
    defer ctx.mtx.unlock();
    var r: usize = 0;
    while (r < local.len) : (r += 1) {
        for (local.columns, ctx.builders) |*col, *bld| try bld.append(col.getValue(r));
    }
}

/// Parallel CSV Top-N: each worker keeps its chunk's top `offset+count` rows, then a
/// global Top-N over the union produces the final sorted, offset/limited output. The
/// output is small and sorted, so (unlike map-only) it stays deterministic.
/// Top-N over a local parquet file: each lane keeps its own heap over the row
/// groups it steals, then the lane winners go through one final top-N. Only
/// N x lanes rows ever reach the combine step.
const PqTopNLaneCtx = struct {
    morsels: PqMorsels,
    row_schema: *const types.Schema,
    prefix: []const ast.Stage,
    params: *std.StringHashMap(*const ast.Expr),
    keys: []const op.Sort.Key,
    cap: u64,
    builders: []column.Builder,
    mtx: std.Thread.Mutex = .{},
    rows_read: *std.atomic.Value(u64),
};

fn pqTopNLane(ctx: *PqTopNLaneCtx, lane_idx: usize) void {
    _ = lane_idx;
    pqTopNLaneRun(ctx) catch |e| ctx.morsels.queue.fail(e);
}

fn pqTopNLaneRun(ctx: *PqTopNLaneCtx) !void {
    var wgpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = false }){};
    defer _ = wgpa.deinit();
    var warena = std.heap.ArenaAllocator.init(wgpa.allocator());
    defer warena.deinit();
    var batch_arena = std.heap.ArenaAllocator.init(wgpa.allocator());
    defer batch_arena.deinit();

    var ms = MorselSource{ .m = &ctx.morsels, .scratch = warena.allocator() };
    var cs = obs.CountingSource{ .inner = .{ .ptr = &ms, .vtable = &MorselSource.vtable }, .count = ctx.rows_read };
    var scan = op.Scan{ .src = cs.source() };
    const child = try buildMapChain(warena.allocator(), ctx.params, ctx.prefix, &scan, ctx.morsels.src_schema);
    var tn = op.TopN{
        .child = child,
        .in_schema = ctx.row_schema,
        .keys = ctx.keys,
        .count = ctx.cap,
        .offset = 0,
        .state = batch_arena.allocator(),
        .gpa = wgpa.allocator(),
    };
    const local = (try tn.next(batch_arena.allocator())) orelse return;

    ctx.mtx.lock();
    defer ctx.mtx.unlock();
    var r: usize = 0;
    while (r < local.len) : (r += 1) {
        for (local.columns, ctx.builders) |col, *b| try b.append(col.getValue(r));
    }
}

fn runParallelParquetTopN(env: *Env, rd: ast.Read, pipeline: []const ast.Stage, prefix: []const ast.Stage, srt: ast.Sort, lim: ast.Limit, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    const arena = env.arena;
    const path = switch (rd.form) {
        .path => |p| p,
        else => return false,
    };
    if (w.mode == .upsert and w.mode.upsert.keys.len == 0) return false;
    if (opts.threads < 2) return false;

    const project = try projectedColumns(env, pipeline[1..]);
    const bounds = try filterBounds(env, pipeline[1..]);
    const probe = pqdecode.Reader.openProjected(arena, path, project) catch return false;
    const ngroups = probe.md.row_groups.len;
    if (ngroups < 2) return false;

    const src_schema = try schemaPtr(arena, probe.schema);
    const row_schema = try schemaPtr(arena, try mapChainSchema(env, prefix, src_schema.*));

    const qs = try arena.alloc(ast.QualName, srt.keys.len);
    for (srt.keys, qs) |sk, *q| q.* = sk.field;
    var ad = analyze.Diag{};
    const idxs = analyze.fieldIndices(arena, row_schema.*, qs, &ad) catch |e| return aErr(env, &ad, e);
    const ks = try arena.alloc(op.Sort.Key, srt.keys.len);
    for (srt.keys, idxs, ks) |sk, idx, *k| k.* = .{ .idx = idx, .desc = sk.desc };

    env.src_name = "parquet";
    env.sink_name = sinkLabel(env, w);

    const nthreads = @max(@as(usize, 1), opts.threads);
    const builders = try arena.alloc(column.Builder, row_schema.fields.len);
    for (builders, row_schema.fields) |*b, f| b.* = column.Builder.init(arena, f.ty);
    var ctx = PqTopNLaneCtx{
        .morsels = .{
            .path = path,
            .project = project,
            .bounds = bounds,
            .src_schema = src_schema,
            .per_item = 1,
            .queue = .{ .nitems = ngroups },
        },
        .row_schema = row_schema,
        .prefix = prefix,
        .params = env.params_expr,
        .keys = ks,
        .cap = lim.offset + lim.count,
        .builders = builders,
        .rows_read = env.rows_read,
    };

    const lanes = try parallel.spawnJoin(arena, nthreads, pqTopNLane, &ctx);
    lanes_used.* = @max(lanes_used.*, lanes);
    env.log.log(.debug, "parallel parquet top-n: {d} row groups over {d} lanes", .{ ngroups, lanes });
    if (ctx.morsels.queue.first_err) |e| return e;

    const cols = try arena.alloc(column.Column, builders.len);
    for (builders, cols) |*b, *c| c.* = try b.finish();
    var combined = OneBatch{ .b = .{ .schema = row_schema, .columns = cols, .len = cols[0].len }, .sch = row_schema.* };
    var gscan = op.Scan{ .src = combined.source() };
    var global = op.TopN{
        .child = .{ .scan = &gscan },
        .in_schema = row_schema,
        .keys = ks,
        .count = lim.count,
        .offset = lim.offset,
        .state = arena,
        .gpa = env.gpa,
    };

    const wr = try resolveUpsertKeys(env, w);
    const snk = try openSink(env, wr, row_schema.*);
    var snk_open = true;
    errdefer if (snk_open) snk.abort();
    while (try global.next(arena)) |b| {
        try snk.writeBatch(arena, b);
        stats.rows_out += b.len;
    }
    snk_open = false;
    try snk.close();
    return true;
}

fn runParallelCsvTopN(env: *Env, rd: ast.Read, prefix: []const ast.Stage, srt: ast.Sort, lim: ast.Limit, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    const arena = env.arena;
    const path = switch (rd.form) {
        .path => |p| p,
        else => return false,
    };
    if (w.mode == .upsert and w.mode.upsert.keys.len == 0) return false;

    const mapped = csv.MappedCsv.open(arena, path) catch return false;
    defer mapped.close();

    const row_schema = try schemaPtr(arena, try mapChainSchema(env, prefix, mapped.schema));

    const qs = try arena.alloc(ast.QualName, srt.keys.len);
    for (srt.keys, qs) |sk, *q| q.* = sk.field;
    var ad = analyze.Diag{};
    const idxs = analyze.fieldIndices(arena, row_schema.*, qs, &ad) catch |e| return aErr(env, &ad, e);
    const ks = try arena.alloc(op.Sort.Key, srt.keys.len);
    for (srt.keys, idxs, ks) |sk, idx, *k| k.* = .{ .idx = idx, .desc = sk.desc };

    env.src_name = "csv";
    env.sink_name = sinkLabel(env, w);

    const nthreads = @max(@as(usize, 1), opts.threads);
    const builders = try arena.alloc(column.Builder, row_schema.fields.len);
    for (builders, row_schema.fields) |*b, f| b.* = column.Builder.init(arena, f.ty);
    var ctx = TopNCtx{
        .mapped = mapped,
        .csv_schema = &mapped.schema,
        .row_schema = row_schema,
        .prefix = prefix,
        .params = env.params_expr,
        .keys = ks,
        .cap = lim.offset + lim.count,
        .queue = .{ .nitems = nthreads },
        .builders = builders,
        .rows_read = env.rows_read,
    };

    const lanes = try parallel.spawnJoin(arena, nthreads, topnWorker, &ctx);
    lanes_used.* = @max(lanes_used.*, lanes);
    env.log.log(.debug, "parallel csv top-n: {d} chunks over {d} lanes", .{ nthreads, lanes });
    if (ctx.queue.first_err) |e| return e;

    const cols = try arena.alloc(column.Column, builders.len);
    for (builders, cols) |*b, *c| c.* = try b.finish();
    var combined = OneBatch{ .b = .{ .schema = row_schema, .columns = cols, .len = cols[0].len }, .sch = row_schema.* };
    var gscan = op.Scan{ .src = combined.source() };
    var global = op.TopN{
        .child = .{ .scan = &gscan },
        .in_schema = row_schema,
        .keys = ks,
        .count = lim.count,
        .offset = lim.offset,
        .state = arena,
        .gpa = env.gpa,
    };

    const wr = try resolveUpsertKeys(env, w);
    const snk = try openSink(env, wr, row_schema.*);
    var snk_open = true;
    errdefer if (snk_open) snk.abort();
    while (try global.next(arena)) |b| {
        try snk.writeBatch(arena, b);
        stats.rows_out += b.len;
    }
    snk_open = false;
    try snk.close();
    return true;
}

const DistinctShape = struct { prefix: []const ast.Stage, dist: ast.Distinct, tail: []const ast.Stage };

/// Recognize `read … | (filter|select)* | distinct | (sort|limit)* | write`.
fn classifyDistinctPipeline(stages: []const ast.Stage) ?DistinctShape {
    const middle = stages[1 .. stages.len - 1];
    var di: ?usize = null;
    for (middle, 0..) |st, i| switch (st.node) {
        .filter, .select => if (di != null) return null,
        .distinct => {
            if (di != null) return null;
            di = i;
        },
        .sort, .limit => if (di == null) return null,
        else => return null,
    };
    const d = di orelse return null;
    return .{ .prefix = middle[0..d], .dist = middle[d].node.distinct, .tail = middle[d + 1 ..] };
}

const DistinctCtx = struct {
    mapped: *csv.MappedCsv,
    csv_schema: *const types.Schema,
    row_schema: *const types.Schema,
    prefix: []const ast.Stage,
    params: *std.StringHashMap(*const ast.Expr),
    local_keys: ?[]const usize,
    key_idx: []const usize,
    queue: WorkQueue,
    seen: *op.Aggregate.GroupMap(),
    builders: []column.Builder,
    mtx: std.Thread.Mutex = .{},
    plan_arena: std.mem.Allocator,
    rows_read: *std.atomic.Value(u64),
};

const distinctWorker = dispatchWorker(DistinctCtx, distinctWorkOne);

fn distinctWorkOne(ctx: *DistinctCtx, i: usize) !void {
    var wgpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = false }){};
    defer _ = wgpa.deinit();
    var warena = std.heap.ArenaAllocator.init(wgpa.allocator());
    defer warena.deinit();
    var batch_arena = std.heap.ArenaAllocator.init(wgpa.allocator());
    defer batch_arena.deinit();

    var reader = csv.CsvSliceReader{ .data = ctx.mapped.chunk(i, ctx.queue.nitems), .schema = ctx.csv_schema };
    var cs = obs.CountingSource{ .inner = reader.source(), .count = ctx.rows_read };
    var scan = op.Scan{ .src = cs.source() };
    const child = try buildMapChain(warena.allocator(), ctx.params, ctx.prefix, &scan, ctx.csv_schema);
    var d = op.Distinct{ .child = child, .in_schema = ctx.row_schema, .keys = ctx.local_keys, .state = warena.allocator(), .gpa = wgpa.allocator() };

    const probe = try warena.allocator().alloc(Value, ctx.key_idx.len);
    while (try d.next(batch_arena.allocator())) |b| {
        ctx.mtx.lock();
        var r: usize = 0;
        while (r < b.len) : (r += 1) {
            for (ctx.key_idx, 0..) |ci, j| probe[j] = b.columns[ci].getValue(r);
            const gop = ctx.seen.getOrPut(probe) catch |e| {
                ctx.mtx.unlock();
                return e;
            };
            if (!gop.found_existing) {
                const kv = ctx.plan_arena.alloc(Value, ctx.key_idx.len) catch |e| {
                    ctx.mtx.unlock();
                    return e;
                };
                for (ctx.key_idx, 0..) |ci, j| kv[j] = try op.dupeValue(ctx.plan_arena, b.columns[ci].getValue(r));
                gop.key_ptr.* = kv;
                gop.value_ptr.* = 0;
                for (b.columns, ctx.builders) |*col, *bld| try bld.append(col.getValue(r));
            }
        }
        ctx.mtx.unlock();
        _ = batch_arena.reset(.retain_capacity);
    }
}

/// Parallel distinct over a local parquet file: each lane dedups the row groups
/// it steals, then a shared seen-set dedups across lanes. Output order varies
/// under `-j > 1`.
const PqDistinctCtx = struct {
    morsels: PqMorsels,
    row_schema: *const types.Schema,
    prefix: []const ast.Stage,
    params: *std.StringHashMap(*const ast.Expr),
    local_keys: ?[]const usize,
    key_idx: []const usize,
    seen: *op.Aggregate.GroupMap(),
    builders: []column.Builder,
    mtx: std.Thread.Mutex = .{},
    plan_arena: std.mem.Allocator,
    rows_read: *std.atomic.Value(u64),
};

fn pqDistinctLane(ctx: *PqDistinctCtx, lane_idx: usize) void {
    _ = lane_idx;
    pqDistinctLaneRun(ctx) catch |e| ctx.morsels.queue.fail(e);
}

fn pqDistinctLaneRun(ctx: *PqDistinctCtx) !void {
    var wgpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = false }){};
    defer _ = wgpa.deinit();
    var warena = std.heap.ArenaAllocator.init(wgpa.allocator());
    defer warena.deinit();
    var batch_arena = std.heap.ArenaAllocator.init(wgpa.allocator());
    defer batch_arena.deinit();

    var ms = MorselSource{ .m = &ctx.morsels, .scratch = warena.allocator() };
    var cs = obs.CountingSource{ .inner = .{ .ptr = &ms, .vtable = &MorselSource.vtable }, .count = ctx.rows_read };
    var scan = op.Scan{ .src = cs.source() };
    const child = try buildMapChain(warena.allocator(), ctx.params, ctx.prefix, &scan, ctx.morsels.src_schema);
    var d = op.Distinct{ .child = child, .in_schema = ctx.row_schema, .keys = ctx.local_keys, .state = warena.allocator(), .gpa = wgpa.allocator() };

    const probe = try warena.allocator().alloc(Value, ctx.key_idx.len);
    while (try d.next(batch_arena.allocator())) |b| {
        ctx.mtx.lock();
        var r: usize = 0;
        while (r < b.len) : (r += 1) {
            for (ctx.key_idx, 0..) |ci, j| probe[j] = b.columns[ci].getValue(r);
            const gop = ctx.seen.getOrPut(probe) catch |e| {
                ctx.mtx.unlock();
                return e;
            };
            if (!gop.found_existing) {
                const kv = ctx.plan_arena.alloc(Value, ctx.key_idx.len) catch |e| {
                    ctx.mtx.unlock();
                    return e;
                };
                for (ctx.key_idx, 0..) |ci, j| kv[j] = try op.dupeValue(ctx.plan_arena, b.columns[ci].getValue(r));
                gop.key_ptr.* = kv;
                gop.value_ptr.* = 0;
                for (b.columns, ctx.builders) |*col, *bld| try bld.append(col.getValue(r));
            }
        }
        ctx.mtx.unlock();
        _ = batch_arena.reset(.retain_capacity);
    }
}

fn runParallelParquetDistinct(env: *Env, rd: ast.Read, pipeline: []const ast.Stage, prefix: []const ast.Stage, dist: ast.Distinct, tail: []const ast.Stage, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    const arena = env.arena;
    const path = switch (rd.form) {
        .path => |p| p,
        else => return false,
    };
    if (w.mode == .upsert and w.mode.upsert.keys.len == 0) return false;
    if (opts.threads < 2) return false;

    const project = try projectedColumns(env, pipeline[1..]);
    const bounds = try filterBounds(env, pipeline[1..]);
    const probe_rdr = pqdecode.Reader.openProjected(arena, path, project) catch return false;
    const ngroups = probe_rdr.md.row_groups.len;
    if (ngroups < 2) return false;

    const src_schema = try schemaPtr(arena, probe_rdr.schema);
    const row_schema = try schemaPtr(arena, try mapChainSchema(env, prefix, src_schema.*));

    var local_keys: ?[]const usize = null;
    var key_idx: []const usize = undefined;
    if (dist.on) |fields| {
        var ad = analyze.Diag{};
        const ks = analyze.fieldIndices(arena, row_schema.*, fields, &ad) catch |e| return aErr(env, &ad, e);
        local_keys = ks;
        key_idx = ks;
    } else {
        const all = try arena.alloc(usize, row_schema.fields.len);
        for (all, 0..) |*x, j| x.* = j;
        key_idx = all;
    }

    env.src_name = "parquet";
    env.sink_name = sinkLabel(env, w);

    const nthreads = @max(@as(usize, 1), opts.threads);
    var seen = op.Aggregate.GroupMap().init(arena);
    const builders = try arena.alloc(column.Builder, row_schema.fields.len);
    for (builders, row_schema.fields) |*b, f| b.* = column.Builder.init(arena, f.ty);
    var ctx = PqDistinctCtx{
        .morsels = .{
            .path = path,
            .project = project,
            .bounds = bounds,
            .src_schema = src_schema,
            .per_item = 1,
            .queue = .{ .nitems = ngroups },
        },
        .row_schema = row_schema,
        .prefix = prefix,
        .params = env.params_expr,
        .local_keys = local_keys,
        .key_idx = key_idx,
        .seen = &seen,
        .builders = builders,
        .plan_arena = arena,
        .rows_read = env.rows_read,
    };

    const lanes = try parallel.spawnJoin(arena, nthreads, pqDistinctLane, &ctx);
    lanes_used.* = @max(lanes_used.*, lanes);
    env.log.log(.debug, "parallel parquet distinct: {d} row groups over {d} lanes", .{ ngroups, lanes });
    if (ctx.morsels.queue.first_err) |e| return e;

    const cols = try arena.alloc(column.Column, builders.len);
    for (builders, cols) |*b, *c| c.* = try b.finish();
    const merged = batchmod.Batch{ .schema = row_schema, .columns = cols, .len = cols[0].len };

    const wr = try resolveUpsertKeys(env, w);
    const snk = try openSink(env, wr, row_schema.*);
    var snk_open = true;
    errdefer if (snk_open) snk.abort();
    try writeTail(env, snk, merged, row_schema.*, tail, stats);
    snk_open = false;
    try snk.close();
    return true;
}

/// Parallel CSV distinct: each worker dedups its chunk locally, then a global
/// seen-set dedups across workers and collects the surviving rows. Output rows may
/// reorder under -j>1 (use -j 1 for stable order). Returns false to fall back.
fn runParallelCsvDistinct(env: *Env, rd: ast.Read, prefix: []const ast.Stage, dist: ast.Distinct, tail: []const ast.Stage, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    const arena = env.arena;
    const path = switch (rd.form) {
        .path => |p| p,
        else => return false,
    };
    if (std.mem.eql(u8, w.connector, "stdout")) return false;
    if (w.mode == .upsert and w.mode.upsert.keys.len == 0) return false;

    const mapped = csv.MappedCsv.open(arena, path) catch return false;
    defer mapped.close();

    const row_schema = try schemaPtr(arena, try mapChainSchema(env, prefix, mapped.schema));

    var local_keys: ?[]const usize = null;
    var key_idx: []const usize = undefined;
    if (dist.on) |fields| {
        var ad = analyze.Diag{};
        const ks = analyze.fieldIndices(arena, row_schema.*, fields, &ad) catch |e| return aErr(env, &ad, e);
        local_keys = ks;
        key_idx = ks;
    } else {
        const all = try arena.alloc(usize, row_schema.fields.len);
        for (all, 0..) |*x, j| x.* = j;
        key_idx = all;
    }

    env.src_name = "csv";
    env.sink_name = sinkLabel(env, w);

    const nthreads = @max(@as(usize, 1), opts.threads);
    var seen = op.Aggregate.GroupMap().init(arena);
    const builders = try arena.alloc(column.Builder, row_schema.fields.len);
    for (builders, row_schema.fields) |*b, f| b.* = column.Builder.init(arena, f.ty);
    var ctx = DistinctCtx{
        .mapped = mapped,
        .csv_schema = &mapped.schema,
        .row_schema = row_schema,
        .prefix = prefix,
        .params = env.params_expr,
        .local_keys = local_keys,
        .key_idx = key_idx,
        .queue = .{ .nitems = nthreads },
        .seen = &seen,
        .builders = builders,
        .plan_arena = arena,
        .rows_read = env.rows_read,
    };

    const lanes = try parallel.spawnJoin(arena, nthreads, distinctWorker, &ctx);
    lanes_used.* = @max(lanes_used.*, lanes);
    env.log.log(.debug, "parallel csv distinct: {d} chunks over {d} lanes", .{ nthreads, lanes });
    if (ctx.queue.first_err) |e| return e;

    const cols = try arena.alloc(column.Column, builders.len);
    for (builders, cols) |*b, *c| c.* = try b.finish();
    const merged = batchmod.Batch{ .schema = row_schema, .columns = cols, .len = cols[0].len };

    const wr = try resolveUpsertKeys(env, w);
    const snk = try openSink(env, wr, row_schema.*);
    var snk_open = true;
    errdefer if (snk_open) snk.abort();

    try writeTail(env, snk, merged, row_schema.*, tail, stats);
    snk_open = false;
    try snk.close();
    return true;
}

const ForMode = enum { sequential, parallel };
const OnError = enum { stop, continue_ };

fn forHintIdent(hints: []const ast.Hint, key: []const u8) ?[]const u8 {
    for (hints) |h| {
        if (std.mem.eql(u8, h.key, key) and h.value == .ident) return h.value.ident;
    }
    return null;
}

/// One discovery row: the first `var_names.len` columns coerced to text.
const Row = []const []const u8;

/// A for-loop's per-row binding, threaded as one value through the interpolation and
/// render chain: the loop variable names, their declared types (parallel; empty = all
/// untyped), and this row's raw text cells. Bundled so a new per-variable attribute
/// doesn't mean re-touching every render signature.
const LoopRow = struct {
    names: []const []const u8,
    types: []const ?types.Type = &.{},
    cells: Row,

    fn typeAt(self: LoopRow, i: usize) ?types.Type {
        return if (i < self.types.len) self.types[i] else null;
    }
};

/// Identify a for-each row the way the script names it — `n=0`, or
/// `db=sales, tbl=orders` — so a failure points at a binding rather than an index.
/// Falls back to the first cell if the label cannot be built.
fn rowLabel(arena: std.mem.Allocator, names: []const []const u8, row: Row) []const u8 {
    var out: []const u8 = "";
    for (names, 0..) |n, i| {
        if (i >= row.len) break;
        const sep: []const u8 = if (out.len > 0) ", " else "";
        out = std.fmt.allocPrint(arena, "{s}{s}{s}={s}", .{ out, sep, n, row[i] }) catch return row[0];
    }
    return if (out.len > 0) out else row[0];
}

/// Append one discovery batch's first `ncols` columns to `rows` as text
/// (strings/ints; null → ""). Shared by every discovery form so they all agree
/// on the coercion rules and the column-count error.
fn appendDiscoveryRows(env: *Env, rows: *std.array_list.Managed(Row), b: batchmod.Batch, ncols: usize) !void {
    if (b.columns.len == 0) return;
    if (b.columns.len < ncols)
        return planErr(env.diag, "for-each: the discovery query returns fewer columns than loop variables");
    for (0..b.len) |r| {
        const row = try env.arena.alloc([]const u8, ncols);
        for (0..ncols) |j| {
            row[j] = switch (b.columns[j].getValue(r)) {
                .null => "",
                .string, .bytes => |s| try env.arena.dupe(u8, s),
                .int => |x| try std.fmt.allocPrint(env.arena, "{d}", .{x}),
                else => return planErr(env.diag, "for-each values must be string or int"),
            };
        }
        try rows.append(row);
    }
}

/// Run the discovery source once and collect its first `ncols` columns as rows of
/// text (strings/ints; null → ""). The list is small — a table catalog — so it is
/// fully materialized into the plan arena.
fn discoverRows(env: *Env, src_read: ast.Read, ncols: usize) ![]const Row {
    const src = openSource(env, src_read, &.{}) catch |e| {
        // `openSource` already recorded why it failed; reporting only the error
        // name would replace "sqlserver connect failed: …" with "PlanFailed"
        // and throw the cause away.
        const why = if (env.diag.msg.len > 0) env.diag.msg else @errorName(e);
        return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "for-each discovery failed: {s}", .{why}));
    };
    defer src.close();
    var rows = std.array_list.Managed(Row).init(env.arena);
    var da = std.heap.ArenaAllocator.init(env.gpa);
    defer da.deinit();
    while (true) {
        _ = da.reset(.retain_capacity);
        const b = (try src.next(da.allocator())) orelse break;
        try appendDiscoveryRows(env, &rows, b, ncols);
    }
    return rows.toOwnedSlice();
}

/// Discovery from a full basalt query (`FOR EACH ROW OF (SELECT ...)` /
/// `EACH TABLE OF (SELECT ...)`): plan and execute the sub-pipeline through the
/// normal machinery, then collect it into the same rows-of-text shape as
/// `discoverRows`. Predicate/projection pushdown for SQL sources therefore comes
/// free from `buildPipeline`.
///
/// The sub-pipeline's sources are opened into `env.sources` like any other, so
/// they are closed here rather than left for the enclosing statement; the
/// per-pipeline scratch fields are restored so discovery cannot influence how the
/// body pipelines are planned.
fn discoverRowsPipeline(env: *Env, pipe: ast.Pipeline, ncols: usize) anyerror![]const Row {
    if (pipe.stages.len == 0) return planErr(env.diag, "for-each: empty discovery query");
    const src_base = env.sources.items.len;
    const saved_src_name = env.src_name;
    const saved_sql_desc = env.sql_desc;
    const saved_pq_readers = env.pq_readers;
    const saved_pq_reader = env.pq_reader;
    defer {
        for (env.sources.items[src_base..]) |sc| sc.close();
        env.sources.shrinkRetainingCapacity(src_base);
        env.src_name = saved_src_name;
        env.sql_desc = saved_sql_desc;
        env.pq_readers = saved_pq_readers;
        env.pq_reader = saved_pq_reader;
    }

    const res = buildPipeline(env, pipe.stages) catch |e| {
        const why = if (env.diag.msg.len > 0) env.diag.msg else @errorName(e);
        return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "for-each discovery failed: {s}", .{why}));
    };
    var rows = std.array_list.Managed(Row).init(env.arena);
    var da = std.heap.ArenaAllocator.init(env.gpa);
    defer da.deinit();
    while (true) {
        if (aborting()) return error.Aborted;
        _ = da.reset(.retain_capacity);
        const b = (try res.op.next(da.allocator())) orelse break;
        try appendDiscoveryRows(env, &rows, b, ncols);
    }
    return rows.toOwnedSlice();
}

/// Discover for-each rows from a JSON array param (`for a, b in job.tables`):
/// navigate to the array, then bind each loop variable to the like-named field of
/// each object element (coerced to text). Mirrors `discoverRows` for reads.
fn discoverRowsJson(env: *Env, path: ast.QualName, var_names: []const []const u8) ![]const Row {
    const head = path.parts[0];
    var cur = env.json_params.get(head) orelse
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "for-each: `{s}` is not a JSON param", .{head}));
    for (path.parts[1..], 0..) |key, j| {
        const seg_safe = j < path.safe.len and path.safe[j];
        cur = switch (cur) {
            .object => |o| o.get(key) orelse {
                if (seg_safe) return &.{};
                return planErr(env.diag, try std.fmt.allocPrint(env.arena, "for-each: json key `{s}` not found", .{key}));
            },
            else => {
                if (seg_safe) return &.{};
                return planErr(env.diag, "for-each: json path is not an object");
            },
        };
    }
    const arr = switch (cur) {
        .array => |a| a,
        else => return planErr(env.diag, "for-each: json source is not an array"),
    };
    var rows = std.array_list.Managed(Row).init(env.arena);
    for (arr.items) |elem| {
        const row = try env.arena.alloc([]const u8, var_names.len);
        for (var_names, 0..) |vn, i| {
            row[i] = switch (elem) {
                .object => |o| if (o.get(vn)) |fv| try jsonToStr(env.arena, fv) else "",
                else => "",
            };
        }
        try rows.append(row);
    }
    return rows.toOwnedSlice();
}

/// Coerce a loop variable's raw text cell to its declared type for expression
/// evaluation. Untyped (`null`) stays a string; an empty/blank cell or an
/// unparseable value under a declared numeric/bool type binds to null (so a typed
/// guard simply doesn't match a missing value). String/bytes/temporal/decimal types
/// keep the raw text — the eval layer coerces those as needed.
fn loopValue(arena: std.mem.Allocator, cell: []const u8, ty: ?types.Type) Value {
    const t = ty orelse return .{ .string = cell };
    if (cell.len == 0) return .null;
    return switch (t.kind) {
        .int, .float, .bool => eval.castValue(arena, .{ .string = cell }, t.kind) catch .null,
        else => .{ .string = cell },
    };
}

fn jsonToStr(arena: std.mem.Allocator, v: std.json.Value) ![]const u8 {
    return switch (v) {
        .null => "",
        .bool => |b| if (b) "true" else "false",
        .integer => |i| try std.fmt.allocPrint(arena, "{d}", .{i}),
        .float => |f| try std.fmt.allocPrint(arena, "{d}", .{f}),
        .number_string, .string => |s| s,
        .array, .object => try std.json.Stringify.valueAlloc(arena, v, .{}),
    };
}

/// Render every `${ ... }` placeholder in `s`, chaining over the loop variables
/// `names[i]` → `values[i]`. Two body shapes:
///   * `${var}` — fast path: substitute the loop variable's value. An unknown
///     variable is left verbatim, so non-loop `${...}` text rides through untouched.
///   * `${ <expr> }` — anything else is parsed as an expression and evaluated with
///     the loop variables in scope (bound as strings, or as their declared type when
///     the `for` header annotated one, e.g. `port:int`), then formatted to text. This
///     is where conditionals/coalesce/case-folding live: `${lower(name)}`,
///     `${if(pk == "", concat(name, "id"), pk)}`, `${coalesce(pk, "default")}`.
/// Strings without `${` are returned as-is (no allocation).
fn interpAll(arena: std.mem.Allocator, s: []const u8, lr: LoopRow) ![]const u8 {
    if (std.mem.indexOf(u8, s, "${") == null) return s;
    var out = std.array_list.Managed(u8).init(arena);
    errdefer out.deinit();
    var i: usize = 0;
    while (i < s.len) {
        if (s[i] == '$' and i + 1 < s.len and s[i + 1] == '{') {
            const close = interpClose(s, i + 2) orelse {
                try out.appendSlice(s[i..]);
                break;
            };
            const inner = s[i + 2 .. close];
            if (bareInterp(inner)) {
                var found = false;
                for (lr.names, lr.cells) |nm, val| {
                    if (std.mem.eql(u8, nm, inner)) {
                        try out.appendSlice(val);
                        found = true;
                        break;
                    }
                }
                if (!found) try out.appendSlice(s[i .. close + 1]);
            } else {
                try out.appendSlice(try evalInterpExpr(arena, inner, lr));
            }
            i = close + 1;
        } else {
            try out.append(s[i]);
            i += 1;
        }
    }
    return out.toOwnedSlice();
}

/// The index of the `}` that closes a `${` opened just before `start`. Scanning
/// is quote-aware — a `}` inside `'...'` (a string) or `"..."` (a quoted column
/// name) does not close — and brace-balanced. Both quotes escape by doubling.
fn interpClose(s: []const u8, start: usize) ?usize {
    var depth: usize = 1;
    var k = start;
    while (k < s.len) {
        switch (s[k]) {
            '\'', '"' => {
                const q = s[k];
                k += 1;
                while (k < s.len) {
                    if (s[k] == q) {
                        if (k + 1 < s.len and s[k + 1] == q) {
                            k += 2;
                            continue;
                        }
                        k += 1;
                        break;
                    }
                    k += 1;
                }
            },
            '{' => {
                depth += 1;
                k += 1;
            },
            '}' => {
                depth -= 1;
                if (depth == 0) return k;
                k += 1;
            },
            else => k += 1,
        }
    }
    return null;
}

/// A `${var}` body — a bare identifier (the fast variable-substitution path).
/// Anything richer (calls, operators, a path) is evaluated as an expression.
fn bareInterp(inner: []const u8) bool {
    if (inner.len == 0 or !(std.ascii.isAlphabetic(inner[0]) or inner[0] == '_')) return false;
    for (inner[1..]) |c| if (!(std.ascii.isAlphanumeric(c) or c == '_')) return false;
    return true;
}

/// Detect constructs that only work after plan-time expansion and so cannot appear in
/// a `${...}` hole (which is parsed/evaluated fresh, without expansion): `let … in`
/// and `?.` safe navigation. Returns a user-facing reason, or null if the expression
/// is fine for interpolation.
fn interpUnsupported(e: *const ast.Expr) ?[]const u8 {
    return switch (e.*) {
        .let_in => "`let … in` is not supported inside ${...}; bind it in a select/fn instead",
        .field => |q| if (q.safe.len > 0) "`?.` safe navigation is not supported inside ${...}" else null,
        .unary => |u| interpUnsupported(u.e),
        .binary => |b| interpUnsupported(b.l) orelse interpUnsupported(b.r),
        .cond => |c| interpUnsupported(c.cond) orelse interpUnsupported(c.then) orelse interpUnsupported(c.els),
        .cast => |c| interpUnsupported(c.e),
        .is_null => |n| interpUnsupported(n.e),
        .call => |c| {
            for (c.args) |a| if (interpUnsupported(a)) |why| return why;
            return null;
        },
        .match => |m| {
            if (m.subject) |s| if (interpUnsupported(s)) |why| return why;
            for (m.arms) |arm| {
                for (arm.pats) |p| if (interpUnsupported(p)) |why| return why;
                if (arm.guard) |g| if (interpUnsupported(g)) |why| return why;
                if (interpUnsupported(arm.value)) |why| return why;
            }
            return null;
        },
        else => null,
    };
}

/// Parse a `${ <expr> }` body and evaluate it with the loop variables in scope,
/// returning the result formatted as text. Variables bind as strings unless the
/// `for` header declared a type (`port:int`), in which case the coerced value is
/// used — so `${if(port > 1000, ...)}` compares numerically, matching the `match`
/// path. A parse/eval failure is a permanent error (reported on stderr).
fn evalInterpExpr(arena: std.mem.Allocator, text: []const u8, lr: LoopRow) ![]const u8 {
    var diag = parser.Diagnostic{ .msg = "", .line = 0, .col = 0 };
    const e = parser.parseExprStr(arena, text, &diag) catch |err| {
        if (err == error.OutOfMemory) return error.OutOfMemory;
        std.debug.print("[interp] ${{{s}}}: {s}\n", .{ text, diag.msg });
        return error.InterpFailed;
    };
    if (interpUnsupported(e)) |why| {
        std.debug.print("[interp] ${{{s}}}: {s}\n", .{ text, why });
        return error.InterpFailed;
    }
    const vals = try arena.alloc(Value, lr.cells.len);
    for (lr.cells, vals, 0..) |cell, *v, i| v.* = loopValue(arena, cell, lr.typeAt(i));
    const result = eval.constEval(arena, e, lr.names, vals) catch |err| {
        if (err == error.OutOfMemory) return error.OutOfMemory;
        std.debug.print("[interp] ${{{s}}}: {s}\n", .{ text, @errorName(err) });
        return error.InterpFailed;
    };
    return eval.valueToString(arena, result);
}

fn renderQual(arena: std.mem.Allocator, q: ast.QualName, lr: LoopRow) !ast.QualName {
    const parts = try arena.alloc([]const u8, q.parts.len);
    for (q.parts, parts) |s, *dst| dst.* = try interpAll(arena, s, lr);
    return .{ .parts = parts, .safe = q.safe };
}

fn renderRead(arena: std.mem.Allocator, rd: ast.Read, lr: LoopRow) !ast.Read {
    return .{ .connector = rd.connector, .form = switch (rd.form) {
        .table => |q| .{ .table = try renderQual(arena, q, lr) },
        .query => |s| .{ .query = try interpAll(arena, s, lr) },
        .path => |s| .{ .path = try interpAll(arena, s, lr) },
        .request => |d| .{ .request = d },
        .unit => .unit,
        .range => |r| .{ .range = .{
            .lo = try renderExpr(arena, r.lo, lr),
            .hi = try renderExpr(arena, r.hi, lr),
        } },
        .buffer => |b| .{ .buffer = .{
            .name = try interpAll(arena, b.name, lr),
            .dir = try interpAll(arena, b.dir, lr),
        } },
    }, .where = try interpAll(arena, rd.where, lr) };
}

/// Interpolate `${var}` into a union stage: the discovered form's discovery query
/// (raw text or in-engine sub-pipeline), or each explicit branch's read target +
/// tag. Lets a for-loop drive which tables a union reconciles (e.g. a per-table
/// discovery query keyed by the loop value).
fn renderUnion(arena: std.mem.Allocator, u: ast.Union, lr: LoopRow) anyerror!ast.Union {
    if (u.branches.len > 0) {
        const branches = try arena.alloc(ast.UnionBranch, u.branches.len);
        for (u.branches, branches) |b, *o| o.* = .{
            .read = try renderRead(arena, b.read, lr),
            .tag = if (b.tag) |t| try interpAll(arena, t, lr) else null,
        };
        return .{ .branches = branches, .discover_conn = u.discover_conn, .discover_query = u.discover_query, .discover_json = u.discover_json, .discover_pipeline = u.discover_pipeline, .pos = u.pos };
    }
    return .{
        .branches = u.branches,
        .discover_conn = u.discover_conn,
        .discover_query = try interpAll(arena, u.discover_query, lr),
        .discover_json = try interpAll(arena, u.discover_json, lr),
        .discover_pipeline = if (u.discover_pipeline) |p| try renderPipeline(arena, p, lr) else null,
        .pos = u.pos,
    };
}

fn renderMode(arena: std.mem.Allocator, mode: ast.WriteMode, lr: LoopRow) !ast.WriteMode {
    switch (mode) {
        .upsert => |u| {
            const keys = try arena.alloc([]const u8, u.keys.len);
            for (u.keys, keys) |k, *dst| dst.* = try interpAll(arena, k, lr);
            var partial: ?[]const []const u8 = null;
            if (u.partial) |pc| {
                const out = try arena.alloc([]const u8, pc.len);
                for (pc, out) |c, *dst| dst.* = try interpAll(arena, c, lr);
                partial = out;
            }
            return .{ .upsert = .{ .keys = keys, .partial = partial } };
        },
        else => return mode,
    }
}

fn renderWrite(arena: std.mem.Allocator, w: ast.Write, lr: LoopRow) !ast.Write {
    return .{
        .connector = w.connector,
        .form = if (w.form) |f| try interpAll(arena, f, lr) else null,
        .target = try interpAll(arena, w.target, lr),
        .mode = try renderMode(arena, w.mode, lr),
    };
}

/// Instantiate the body template for one row: interpolate each `${var}` into the
/// read/write targets + upsert keys (v1 = targets only; expressions untouched).
fn renderHints(arena: std.mem.Allocator, hints: []const ast.Hint, lr: LoopRow) ![]const ast.Hint {
    if (hints.len == 0) return hints;
    const out = try arena.alloc(ast.Hint, hints.len);
    for (hints, out) |h, *o| {
        o.* = h;
        o.value = switch (h.value) {
            .str => |s| .{ .str = try interpAll(arena, s, lr) },
            .ident => |s| .{ .ident = try interpAll(arena, s, lr) },
            else => h.value,
        };
    }
    return out;
}

fn renderPipeline(arena: std.mem.Allocator, body: ast.Pipeline, lr: LoopRow) anyerror!ast.Pipeline {
    const stages = try arena.alloc(ast.Stage, body.stages.len);
    for (body.stages, stages) |src, *dst| {
        dst.* = src;
        dst.hints = try renderHints(arena, src.hints, lr);
        switch (src.node) {
            .read => |rd| dst.node = .{ .read = try renderRead(arena, rd, lr) },
            .union_ => |u| dst.node = .{ .union_ = try renderUnion(arena, u, lr) },
            .write => |w| dst.node = .{ .write = try renderWrite(arena, w, lr) },
            .filter => |e| dst.node = .{ .filter = try renderExpr(arena, e, lr) },
            .select => |items| dst.node = .{ .select = try renderSelect(arena, items, lr) },
            .aggregate => |ag| {
                const aggs = try arena.alloc(ast.AggItem, ag.aggs.len);
                for (ag.aggs, 0..) |a, i| aggs[i] = .{ .name = a.name, .func = a.func, .arg = if (a.arg) |e| try renderExpr(arena, e, lr) else null, .distinct = a.distinct };
                dst.node = .{ .aggregate = .{ .aggs = aggs, .by = ag.by } };
            },
            else => {},
        }
    }
    return .{ .stages = stages, .pos = body.pos };
}

/// Deep-copy an expression, interpolating `${var}` needles into every string
/// literal and folding bare loop-variable references to this row's value.
/// Other leaves are reused as-is.
const RenderCtx = struct { arena: std.mem.Allocator, lr: LoopRow };

fn renderRecur(ctx: RenderCtx, e: *const ast.Expr) anyerror!*ast.Expr {
    return renderExpr(ctx.arena, e, ctx.lr);
}

fn renderExpr(arena: std.mem.Allocator, e: *const ast.Expr, lr: LoopRow) anyerror!*ast.Expr {
    if (e.* == .str_lit) return try mk(arena, .{ .str_lit = try interpAll(arena, e.str_lit, lr) });
    // `$name` parses to a plain single-part field, so a loop variable used as a
    // value is indistinguishable from a column here — bind it to the row's cell,
    // shadowing a same-named source column (the rule scalar params already use).
    // Multi-part paths (`$job.x`) belong to expand.zig and are left alone.
    if (e.* == .field) {
        if (e.field.single()) |nm| {
            for (lr.names, lr.cells, 0..) |n, cell, i| {
                if (std.mem.eql(u8, n, nm)) return mkLit(arena, loopValue(arena, cell, lr.typeAt(i)));
            }
        }
    }
    return ast.rebuildExpr(arena, e, RenderCtx{ .arena = arena, .lr = lr }, renderRecur);
}

fn renderSelect(arena: std.mem.Allocator, items: []const ast.SelectItem, lr: LoopRow) ![]const ast.SelectItem {
    const out = try arena.alloc(ast.SelectItem, items.len);
    for (items, 0..) |it, i| out[i] = switch (it) {
        .computed => |c| .{ .computed = .{
            .name = try interpAll(arena, c.name, lr),
            .expr = try renderExpr(arena, c.expr, lr),
        } },
        else => it,
    };
    return out;
}

/// Shared state for parallel-for workers. Per-table work uses a worker-private
/// arena/env; only the counters + the first-error buffer are shared (atomics +
/// a mutex), so no allocation ever races on the plan arena.
const ForCtx = struct {
    fe: ast.ForEach,
    needles: []const []const u8,
    rows: []const Row,
    base: *Env,
    worker_opts: RunOptions,
    on_error: OnError,
    outcomes: ?*OutcomeSink = null,
    next: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    rows_out: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    failures: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    /// Workers run on their own Env, so the summary gate has to be carried back.
    wrote_sink: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    mu: std.Thread.Mutex = .{},
    /// Wide enough to hold a full `Diag` message (512) plus the row label.
    first_err_buf: [640]u8 = undefined,
    first_err_len: usize = 0,
    first_retryable: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
};

/// `item` is the retry identity recorded in the outcome sink (the first cell);
/// `label` is the human row identity and `msg` the already-resolved diagnostic.
fn forRecordFail(ctx: *ForCtx, item: []const u8, label: []const u8, msg: []const u8, retryable: bool) void {
    _ = ctx.failures.fetchAdd(1, .monotonic);
    if (ctx.outcomes) |sink| sink.record(item, false, msg, retryable);
    ctx.mu.lock();
    defer ctx.mu.unlock();
    if (ctx.first_err_len == 0) {
        const lab = label[0..@min(label.len, 96)];
        const why = msg[0..@min(msg.len, 512)];
        const s = std.fmt.bufPrint(&ctx.first_err_buf, "row {s}: {s}", .{ lab, why }) catch ctx.first_err_buf[0..0];
        ctx.first_err_len = s.len;
        ctx.first_retryable.store(retryable, .monotonic);
    }
    if (ctx.on_error == .stop) ctx.stop.store(true, .release);
}

fn forWorker(ctx: *ForCtx, _: usize) void {
    const gpa = ctx.base.gpa;
    while (true) {
        if (aborting()) break;
        if (ctx.on_error == .stop and ctx.stop.load(.acquire)) break;
        const i = ctx.next.fetchAdd(1, .monotonic);
        if (i >= ctx.rows.len) break;
        const row = ctx.rows[i];

        var w_arena = std.heap.ArenaAllocator.init(gpa);
        defer w_arena.deinit();
        var w_batch = std.heap.ArenaAllocator.init(gpa);
        defer w_batch.deinit();
        var w_sources = std.array_list.Managed(driver.Source).init(w_arena.allocator());
        var w_diag = Diag{};
        var w_errctx = op.ErrCtx{};
        var w_env = Env{
            .arena = w_arena.allocator(),
            .gpa = gpa,
            .params = ctx.base.params,
            .bindings = ctx.base.bindings,
            .connections = ctx.base.connections,
            .sources = &w_sources,
            .request_body = ctx.base.request_body,
            .diag = &w_diag,
            .log = ctx.base.log,
            .params_expr = ctx.base.params_expr,
            .errctx = &w_errctx,
            .rows_read = ctx.base.rows_read,
            .json_params = ctx.base.json_params,
            .fns = ctx.base.fns,
            .call_depth = ctx.base.call_depth,
        };
        var st = Stats{ .run_id = 0 };
        var lanes: usize = 1;
        const lr = LoopRow{ .names = ctx.needles, .types = ctx.fe.var_types, .cells = row };
        if (runForBody(&w_env, ctx.fe.body, lr, ctx.worker_opts, &st, &lanes, &w_batch)) |_| {
            _ = ctx.rows_out.fetchAdd(st.rows_out, .monotonic);
            if (ctx.outcomes) |sink| sink.record(row[0], true, "", false);
        } else |e| {
            if (e == error.Aborted) {
                for (w_sources.items) |sc| sc.close();
                break;
            }
            const emsg = if (w_diag.msg.len > 0) w_diag.msg else @errorName(e);
            const label = rowLabel(w_arena.allocator(), ctx.needles, row);
            if (ctx.on_error == .continue_) w_env.log.log(.err, "for-each row {s}: {s}", .{ label, emsg });
            forRecordFail(ctx, row[0], label, emsg, isTransient(e) or w_diag.retryable);
        }
        if (w_env.wrote_sink) ctx.wrote_sink.store(true, .monotonic);
        for (w_sources.items) |sc| sc.close();
    }
}

/// Run one row of a `for` body. The body is a statement block (a bare pipeline is a
/// one-statement block): each pipeline is rendered with the row's `${var}` values and
/// executed; a `match` branches on the loop variables and runs the winning arm.
fn runForBody(env: *Env, body: []const ast.Stmt, lr: LoopRow, opts: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!void {
    for (body) |*st| try runForStmt(env, st, lr, opts, stats, lanes_used, batch_arena);
}

fn runForStmt(env: *Env, s: *const ast.Stmt, lr: LoopRow, opts: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!void {
    switch (s.*) {
        .output => |p| {
            const pipe = try renderPipeline(env.arena, p, lr);
            try runOutput(env, pipe, opts, stats, lanes_used, batch_arena);
        },
        .match => |m| try runForMatch(env, m, lr, opts, stats, lanes_used, batch_arena),
        .call => |c| try runCall(env, c, lr, opts, stats, lanes_used, batch_arena),
        else => return planErr(env.diag, "a `for` or statement-function body may contain only pipelines, `CASE` and `CALL` statements"),
    }
}

/// A `CALL` renders at most this deep. Statement functions may call each other, but
/// a cycle would otherwise render forever (there is no value to terminate on, the
/// way an expression `fn` bottoms out at a literal).
const max_call_depth = 16;

/// No loop variables in scope — the binding a top-level `CALL` starts from.
const no_loop_vars = LoopRow{ .names = &[_][]const u8{}, .cells = &[_][]const u8{} };

/// `CALL f(a, b)` — a plan-time statement macro. The declared parameters become
/// loop variables and the argument values their cells, so the body is rendered and
/// executed by exactly the machinery one sequential `FOR EACH ROW OF` iteration
/// uses (`LoopRow` → `renderPipeline` → `runForBody`); a call is one such iteration
/// over a hand-built row. `outer` is the binding in scope at the call site, so an
/// argument may be a literal, a `$param`, or an enclosing loop variable.
fn runCall(env: *Env, c: ast.CallStmt, outer: LoopRow, opts: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!void {
    const fd = env.fns.get(c.name) orelse
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "CALL: no function named `{s}`", .{c.name}));
    const body = switch (fd.body) {
        .stmts => |b| b,
        .expr => return planErr(env.diag, try std.fmt.allocPrint(env.arena, "`{s}` is a scalar function — use it in an expression, not CALL", .{c.name})),
    };
    // expand.zig has already filled defaults and checked the count; a mismatch here
    // would mean an unexpanded program, so report it rather than index past the end.
    if (c.args.len != fd.params.len)
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "`{s}` expects {d} argument(s), got {d}", .{ c.name, fd.params.len, c.args.len }));
    if (env.call_depth >= max_call_depth)
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "CALL nesting too deep (recursive?) at `{s}`", .{c.name}));

    // Argument scope: the enclosing loop variables first (they shadow same-named
    // params, matching `runForMatch`), then the resolved params.
    var names = std.array_list.Managed([]const u8).init(env.arena);
    var values = std.array_list.Managed(Value).init(env.arena);
    for (outer.names, outer.cells, 0..) |nm, cell, i| {
        try names.append(nm);
        try values.append(loopValue(env.arena, cell, outer.typeAt(i)));
    }
    var it = env.params.iterator();
    while (it.next()) |kv| {
        try names.append(kv.key_ptr.*);
        try values.append(kv.value_ptr.*);
    }

    const pnames = try env.arena.alloc([]const u8, fd.params.len);
    const ptypes = try env.arena.alloc(?types.Type, fd.params.len);
    const cells = try env.arena.alloc([]const u8, fd.params.len);
    for (fd.params, c.args, 0..) |p, a, i| {
        pnames[i] = p.name;
        ptypes[i] = p.ty;
        const v = eval.constEval(env.arena, a, names.items, values.items) catch |e|
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "CALL {s}: argument `{s}`: {s}", .{ c.name, p.name, @errorName(e) }));
        cells[i] = try eval.valueToString(env.arena, v);
    }

    env.log.log(.info, "call {s}: {d} argument(s) [depth {d}]", .{ c.name, cells.len, env.call_depth + 1 });
    env.call_depth += 1;
    defer env.call_depth -= 1;
    try runForBody(env, body, .{ .names = pnames, .types = ptypes, .cells = cells }, opts, stats, lanes_used, batch_arena);
}

/// A `match` evaluated per row of a `for`: the loop variables are bound (shadowing
/// same-named params), so a guard like `pk == ""` picks a branch. Untyped variables
/// bind as strings; a `name:type` annotation binds the coerced value, so a guard like
/// `port >= 1000` compares numerically rather than lexically.
fn runForMatch(env: *Env, m: ast.StmtMatch, lr: LoopRow, opts: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!void {
    var names = std.array_list.Managed([]const u8).init(env.arena);
    var values = std.array_list.Managed(Value).init(env.arena);
    for (lr.names, lr.cells, 0..) |nm, cell, i| {
        try names.append(nm);
        try values.append(loopValue(env.arena, cell, lr.typeAt(i)));
    }
    var it = env.params.iterator();
    while (it.next()) |kv| {
        try names.append(kv.key_ptr.*);
        try values.append(kv.value_ptr.*);
    }
    const idx = (try matchArmIndex(env, m, names.items, values.items, "for/match")) orelse return;
    for (m.arms[idx].body) |*st| try runForStmt(env, st, lr, opts, stats, lanes_used, batch_arena);
}

/// Expand a `for <vars> in <source>` block, running its body once per discovered row.
/// `mode` (sequential|parallel) and `on_error` (stop|continue) come from `@[...]`.
fn runForEach(env: *Env, fe: ast.ForEach, opts: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) !void {
    const mode: ForMode = if (forHintIdent(fe.hints, "mode")) |m|
        (if (std.mem.eql(u8, m, "parallel")) ForMode.parallel else ForMode.sequential)
    else
        .sequential;
    const on_error: OnError = if (forHintIdent(fe.hints, "on_error")) |m|
        (if (std.mem.eql(u8, m, "continue")) OnError.continue_ else OnError.stop)
    else
        .stop;

    const rows = switch (fe.source) {
        .read => |rd| try discoverRows(env, rd, fe.var_names.len),
        .json_path => |p| try discoverRowsJson(env, p, fe.var_names),
        .pipeline => |p| try discoverRowsPipeline(env, p, fe.var_names.len),
    };
    env.log.log(.debug, "for-each {s}: {d} row(s) [{s}, on_error={s}]", .{ fe.var_names[0], rows.len, @tagName(mode), if (on_error == .continue_) "continue" else "stop" });
    if (rows.len == 0) return;
    const needles = fe.var_names;

    switch (mode) {
        .sequential => {
            var failures: usize = 0;
            var first_err: ?[]const u8 = null;
            for (rows) |row| {
                if (aborting()) return error.Aborted;
                const base = env.sources.items.len;
                // Cleared per row so a failure never reports the previous row's message.
                env.diag.retryable = false;
                env.diag.msg = "";
                const lr = LoopRow{ .names = needles, .types = fe.var_types, .cells = row };
                if (runForBody(env, fe.body, lr, opts, stats, lanes_used, batch_arena)) |_| {
                    for (env.sources.items[base..]) |sc| sc.close();
                    env.sources.shrinkRetainingCapacity(base);
                    if (opts.outcomes) |sink| sink.record(row[0], true, "", false);
                } else |e| {
                    for (env.sources.items[base..]) |sc| sc.close();
                    env.sources.shrinkRetainingCapacity(base);
                    if (e == error.Aborted) return error.Aborted;
                    failures += 1;
                    const emsg = if (env.diag.msg.len > 0) env.diag.msg else @errorName(e);
                    const label = rowLabel(env.arena, needles, row);
                    if (opts.outcomes) |sink| sink.record(row[0], false, emsg, isTransient(e) or env.diag.retryable);
                    // In stop mode the same text comes back out as the run's error, so
                    // only a loop that carries on logs the row here.
                    if (on_error == .continue_) env.log.log(.err, "for-each row {s}: {s}", .{ label, emsg });
                    if (first_err == null)
                        first_err = std.fmt.allocPrint(env.arena, "row {s}: {s}", .{ label, emsg }) catch null;
                    if (on_error == .stop) {
                        if (isTransient(e)) env.diag.retryable = true;
                        // `emsg` may alias `diag.buf`; rewriting the diag with it would
                        // be a self-copy, so on an alloc failure keep what is there.
                        if (first_err) |why|
                            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "for-each {s}", .{why}));
                        return error.PlanFailed;
                    }
                }
            }
            if (failures > 0 and opts.outcomes == null)
                return planErr(env.diag, try std.fmt.allocPrint(env.arena, "for-each: {d}/{d} failed (first: {s})", .{ failures, rows.len, first_err orelse "?" }));
        },
        .parallel => {
            var wopts = opts;
            wopts.threads = 1;
            const nworkers = @min(@max(opts.threads, @as(usize, 1)), rows.len);
            var ctx = ForCtx{ .fe = fe, .needles = needles, .rows = rows, .base = env, .worker_opts = wopts, .on_error = on_error, .outcomes = opts.outcomes };
            const lanes = try parallel.spawnJoin(env.arena, nworkers, forWorker, &ctx);
            if (aborting()) return error.Aborted;
            stats.rows_out += ctx.rows_out.load(.monotonic);
            lanes_used.* = @max(lanes_used.*, lanes);
            if (ctx.wrote_sink.load(.monotonic)) env.wrote_sink = true;
            const fails = ctx.failures.load(.monotonic);
            if (fails > 0 and (on_error == .stop or opts.outcomes == null)) {
                if (ctx.first_retryable.load(.monotonic)) env.diag.retryable = true;
                const first = ctx.first_err_buf[0..ctx.first_err_len];
                const why = if (on_error == .stop)
                    try std.fmt.allocPrint(env.arena, "for-each {s}", .{first})
                else
                    try std.fmt.allocPrint(env.arena, "for-each: {d}/{d} failed (first: {s})", .{ fails, rows.len, first });
                return planErr(env.diag, why);
            }
        },
    }
}

/// Resolve a sink/source connector name to its driver type for the summary
/// (`csv`/`request` are types; a connection name maps to its `connector`).
/// Label for a *write* target: the `csv` connector covers every file sink, so
/// the format has to come from the target itself or telemetry reports parquet
/// writes as csv.
fn sinkLabel(env: *Env, w: ast.Write) []const u8 {
    if (std.mem.eql(u8, w.connector, "csv") and pqwrite.Writer.isPath(w.target)) return "parquet";
    return connectorType(env, w.connector);
}

fn connectorType(env: *Env, name: []const u8) []const u8 {
    if (std.mem.eql(u8, name, "csv") or std.mem.eql(u8, name, "request") or
        std.mem.eql(u8, name, "http") or std.mem.eql(u8, name, "buffer")) return name;
    if (env.connections.get(name)) |c| return c.connector;
    return name;
}

/// Source columns the stages after a read actually need, or null when that set
/// cannot be proven — in which case every column is read.
///
/// Only used to skip decoding work: if this under-reports, a later stage fails
/// to resolve a field and the query errors loudly. It can never silently return
/// wrong rows, which is what makes the optimisation safe to attempt.
fn projectedColumns(env: *Env, stages: []const ast.Stage) !?[][]const u8 {
    var set = std.StringHashMap(void).init(env.arena);
    // Projection is only sound once some stage *defines* the output columns.
    // With no such stage the read's own columns are the result — an empty
    // reference set then means "everything", not "nothing".
    var defines_output = false;
    for (stages) |st| {
        switch (st.node) {
            .filter => |e| try pushdown.collectFields(e, &set),
            .sort => |so| for (so.keys) |k| try set.put(k.field.parts[0], {}),
            .distinct => |d| {
                // `distinct` with no key list looks at every column
                const on = d.on orelse return null;
                for (on) |q| try set.put(q.parts[0], {});
            },
            .aggregate => |ag| {
                for (ag.by) |q| try set.put(q.parts[0], {});
                for (ag.aggs) |a| if (a.arg) |e| try pushdown.collectFields(e, &set);
                defines_output = true;
                break;
            },
            .select => |items| {
                for (items) |it| switch (it) {
                    // a star keeps every column, and after it names are no
                    // longer the source's, so nothing further can be proven
                    .star, .star_except, .star_rename => return null,
                    .field => |q| try set.put(q.parts[0], {}),
                    .computed => |c| try pushdown.collectFields(c.expr, &set),
                };
                // downstream stages refer to this select's outputs, not the
                // source's columns, so the set is complete here
                defines_output = true;
                break;
            },
            .limit => {},
            // anything else may reference columns in ways not modelled here
            else => return null,
        }
    }
    if (!defines_output) return null;
    var out = std.array_list.Managed([]const u8).init(env.arena);
    var it = set.keyIterator();
    while (it.next()) |k| try out.append(k.*);
    return try out.toOwnedSlice();
}

/// Simple `column <op> literal` conjuncts of a filter, usable to skip whole
/// row groups from their statistics.
///
/// Only `AND`-joined comparisons are collected. Anything else contributes no
/// bound, which loses an optimisation but can never exclude a matching row.
fn filterBounds(env: *Env, stages: []const ast.Stage) ![]pqdecode.Bound {
    var out = std.array_list.Managed(pqdecode.Bound).init(env.arena);
    for (stages) |st| {
        switch (st.node) {
            .filter => |e| try collectBounds(e, &out),
            // stop at the first stage that redefines the columns
            .select, .aggregate, .join, .explode, .union_ => break,
            else => {},
        }
    }
    return out.toOwnedSlice();
}

fn collectBounds(e: *const ast.Expr, out: *std.array_list.Managed(pqdecode.Bound)) !void {
    const b = switch (e.*) {
        .binary => |x| x,
        else => return,
    };
    if (b.op == .@"and") {
        try collectBounds(b.l, out);
        try collectBounds(b.r, out);
        return;
    }
    // only `field <op> literal` in that order; the mirrored form is left alone
    const name = switch (b.l.*) {
        .field => |q| q.parts[q.parts.len - 1],
        else => return,
    };
    const v: valuemod.Value = switch (b.r.*) {
        .int_lit => |x| .{ .int = x },
        .float_lit => |x| .{ .float = x },
        .str_lit => |x| .{ .string = x },
        else => return,
    };
    const bop: pqdecode.Bound.Op = switch (b.op) {
        .lt => .lt,
        .le => .le,
        .gt => .gt,
        .ge => .ge,
        .eq => .eq,
        else => return,
    };
    try out.append(.{ .column = name, .op = bop, .value = v });
}

/// A single pre-computed batch, so a metadata answer can enter the pipeline
/// through the ordinary scan path.
const ConstSource = struct {
    batch: batchmod.Batch,
    out: *const types.Schema,
    yielded: bool = false,

    fn schemaFn(ptr: *anyopaque) types.Schema {
        const self: *ConstSource = @ptrCast(@alignCast(ptr));
        return self.out.*;
    }
    fn nextFn(ptr: *anyopaque, arena: std.mem.Allocator) anyerror!?batchmod.Batch {
        _ = arena;
        const self: *ConstSource = @ptrCast(@alignCast(ptr));
        if (self.yielded) return null;
        self.yielded = true;
        return self.batch;
    }
    fn closeFn(ptr: *anyopaque) void {
        _ = ptr;
    }
    const vtable = driver.Source.VTable{ .schema = schemaFn, .next = nextFn, .close = closeFn };
};

/// Answer `COUNT(*)`, `MIN(col)` and `MAX(col)` over a whole Parquet file from
/// the footer instead of scanning it. Deliberately narrow: an unfiltered,
/// ungrouped aggregate reading the file directly. Anything else — a filter, a
/// GROUP BY, a URL, a missing statistic — falls through to the real pipeline,
/// so the shortcut can only ever be as correct as the scan it replaces.
fn metaShortcut(env: *Env, stages: []const ast.Stage) anyerror!?PipeRes {
    if (stages.len != 2) return null;
    if (stages[0].node != .read or stages[1].node != .aggregate) return null;
    const rd = stages[0].node.read;
    if (!std.mem.eql(u8, rd.connector, "csv")) return null;
    const path = switch (rd.form) {
        .path => |p| p,
        else => return null,
    };
    if (!pqdecode.Reader.isPath(path) or csv.CsvReader.isUrl(path)) return null;
    const ag = stages[1].node.aggregate;
    if (ag.by.len != 0 or ag.aggs.len == 0) return null;

    const rdr = pqdecode.Reader.open(env.arena, path) catch return null;
    var ad = analyze.Diag{};
    const ap = analyze.aggregatePlan(env.arena, rdr.schema, ag, env.params_expr, &ad) catch return null;

    const vals = try env.arena.alloc(Value, ap.aggs.len);
    for (ap.aggs, ag.aggs, vals) |ra, item, *out| {
        if (item.distinct) return null;
        switch (ra.func) {
            .count => {
                if (ra.arg != null) return null; // COUNT(col) needs null counts
                out.* = .{ .int = rdr.md.num_rows };
            },
            .min, .max => {
                const arg = ra.arg orelse return null;
                if (arg.* != .field) return null;
                const mm = pqdecode.fileMinMax(rdr, arg.field.last()) orelse return null;
                out.* = try op.dupeValue(env.arena, if (ra.func == .min) mm.min else mm.max);
            },
            else => return null,
        }
    }

    const out = try schemaPtr(env.arena, ap.schema);
    const cols = try env.arena.alloc(column.Column, ap.aggs.len);
    for (ap.aggs, vals, cols) |ra, v, *c| {
        var bd = column.Builder.init(env.arena, ra.ty);
        try bd.append(v);
        c.* = try bd.finish();
    }

    const cs = try env.arena.create(ConstSource);
    cs.* = .{ .batch = .{ .schema = out, .columns = cols, .len = 1 }, .out = out };
    const scan = try env.arena.create(op.Scan);
    scan.* = .{ .src = .{ .ptr = cs, .vtable = &ConstSource.vtable } };
    if (env.src_name.len == 0) env.src_name = "parquet";
    return .{ .op = .{ .scan = scan }, .schema = out.* };
}

fn buildPipeline(env: *Env, stages: []const ast.Stage) anyerror!PipeRes {
    if (stages.len == 0) return planErr(env.diag, "empty pipeline");
    if (try metaShortcut(env, stages)) |r| return r;

    var current: op.Op = undefined;
    var schema: types.Schema = undefined;

    switch (stages[0].node) {
        .read => |rd| {
            const raw = try openSourceProjected(env, rd, stages[0].hints, try projectedColumns(env, stages[1..]), try filterBounds(env, stages[1..]));
            const cs = try env.arena.create(obs.CountingSource);
            cs.* = .{ .inner = raw, .count = env.rows_read };
            const src = cs.source();
            try env.sources.append(src);
            if (env.src_name.len == 0) env.src_name = connectorType(env, rd.connector);
            const scan = try env.arena.create(op.Scan);
            scan.* = .{ .src = src };
            current = .{ .scan = scan };
            schema = src.schema();
        },
        .ref => |name| {
            const b = env.bindings.get(name) orelse
                return planErr(env.diag, try std.fmt.allocPrint(env.arena, "unknown binding `{s}`", .{name}));
            const r = try buildPipeline(env, b.stages);
            current = r.op;
            schema = r.schema;
        },
        .union_ => |u| {
            const r = try buildUnion(env, u, stages[0].hints);
            current = r.op;
            schema = r.schema;
        },
        else => return planErr(env.diag, "a pipeline must start with `read`, `union`, or a binding reference"),
    }

    var si: usize = 1;
    while (si < stages.len) : (si += 1) {
        const stage = stages[si];
        if (stage.node == .sort and si + 1 < stages.len and stages[si + 1].node == .limit) {
            const r = try buildTopN(env, stage.node.sort, stages[si + 1].node.limit, current, schema);
            current = r.op;
            schema = r.schema;
            si += 1;
            continue;
        }
        const r = try buildStage(env, stage, current, schema);
        current = r.op;
        schema = r.schema;
    }
    return .{ .op = current, .schema = schema };
}

fn buildTopN(env: *Env, s: ast.Sort, lim: ast.Limit, child: op.Op, schema: types.Schema) anyerror!PipeRes {
    const arena = env.arena;
    const qs = try arena.alloc(ast.QualName, s.keys.len);
    for (s.keys, qs) |sk, *q| q.* = sk.field;
    var ad = analyze.Diag{};
    const idxs = analyze.fieldIndices(arena, schema, qs, &ad) catch |e| return aErr(env, &ad, e);
    const ks = try arena.alloc(op.Sort.Key, s.keys.len);
    for (s.keys, idxs, ks) |sk, idx, *k| k.* = .{ .idx = idx, .desc = sk.desc };
    const o = try arena.create(op.TopN);
    o.* = .{ .child = child, .in_schema = try schemaPtr(arena, schema), .keys = ks, .count = lim.count, .offset = lim.offset, .state = arena, .gpa = env.gpa };

    // Push the running K-th-best bound into a single parquet source so it can
    // skip row groups its statistics rule out. Requires exactly one parquet
    // reader and one sort key, so the bound is unambiguous.
    if (env.pq_readers == 1 and s.keys.len == 1) {
        if (env.pq_reader) |pr| {
            const t = try arena.create(valuemod.Threshold);
            t.* = .{ .column = s.keys[0].field.last(), .desc = s.keys[0].desc };
            pr.threshold = t;
            o.threshold = t;
        }
    }
    return .{ .op = .{ .top_n = o }, .schema = schema };
}

fn readName(rd: ast.Read) []const u8 {
    return switch (rd.form) {
        .table => |q| q.last(),
        else => "",
    };
}

/// Synthesize the per-branch "reconcile to canon" projection as a `select`: an
/// optional tag literal, then every canon column cast to its canon type — taking
/// the source field when present, else NULL. (Extra source columns aren't listed,
/// so they're dropped.) Reusing `select` gets us the vectorized cast/eval for free.
fn synthReconcile(arena: std.mem.Allocator, src: types.Schema, canon: types.Schema, tag_col: ?[]const u8, tag_val: ?[]const u8) ![]const ast.SelectItem {
    var items = std.array_list.Managed(ast.SelectItem).init(arena);
    if (tag_col) |tc|
        try items.append(.{ .computed = .{ .name = tc, .expr = try mk(arena, .{ .str_lit = tag_val orelse "" }) } });
    for (canon.fields) |cf| {
        var present = false;
        for (src.fields) |sf| {
            if (std.mem.eql(u8, sf.name, cf.name)) {
                present = true;
                break;
            }
        }
        const parts = try arena.alloc([]const u8, 1);
        parts[0] = cf.name;
        const inner = if (present) try mk(arena, .{ .field = .{ .parts = parts } }) else try mk(arena, .null_lit);
        const e = try mk(arena, .{ .cast = .{ .e = inner, .ty = cf.ty } });
        try items.append(.{ .computed = .{ .name = cf.name, .expr = e } });
    }
    return items.toOwnedSlice();
}

const UnionSpec = struct { read: ast.Read, tag: ?[]const u8, name: []const u8 };

/// Resolve a union's branch list — explicit branches, or tables discovered via a
/// `(table_name, tag)` query.
fn unionSpecs(env: *Env, u: ast.Union, hints: []const ast.Hint) ![]UnionSpec {
    const arena = env.arena;
    var specs = std.array_list.Managed(UnionSpec).init(arena);
    const where = forHintName(hints, "where") orelse "";
    if (u.discover_json.len > 0) {
        const table_key = forHintName(hints, "table_field");
        const tag_key = forHintName(hints, "tag_field");
        const tag_substr = forHintName(hints, "tag_substr");
        const parsed = std.json.parseFromSliceLeaky(std.json.Value, arena, u.discover_json, .{}) catch
            return planErr(env.diag, try std.fmt.allocPrint(arena, "union json: invalid JSON: {s}", .{u.discover_json}));
        const items = switch (parsed) {
            .array => |a| a.items,
            else => return planErr(env.diag, "union json: expected a JSON array"),
        };
        for (items) |elem| {
            var tbl: []const u8 = undefined;
            var tag: ?[]const u8 = null;
            switch (elem) {
                .string => |s| tbl = s,
                .object => |o| {
                    tbl = (if (table_key) |k| jsonStrField(o, k) else null) orelse
                        jsonStrField(o, "table") orelse jsonStrField(o, "name") orelse
                        return planErr(env.diag, "union json: element has no table name");
                    tag = (if (tag_key) |k| jsonStrField(o, k) else null) orelse
                        jsonStrField(o, "tag") orelse jsonStrField(o, "emp");
                },
                else => return planErr(env.diag, "union json: each element must be a string or object"),
            }
            if (tag == null) if (tag_substr) |spec| {
                tag = deriveSubstr(tbl, spec);
            };
            const parts = try arena.alloc([]const u8, 1);
            parts[0] = tbl;
            try specs.append(.{
                .read = .{ .connector = u.discover_conn, .form = .{ .table = .{ .parts = parts } }, .where = where },
                .tag = tag,
                .name = tbl,
            });
        }
    } else if (u.discover_pipeline) |pipe| {
        for (try discoverRowsPipeline(env, pipe, 2)) |row| {
            const parts = try arena.alloc([]const u8, 1);
            parts[0] = row[0];
            try specs.append(.{ .read = .{ .connector = u.discover_conn, .form = .{ .table = .{ .parts = parts } }, .where = where }, .tag = row[1], .name = row[0] });
        }
    } else if (u.discover_query.len > 0) {
        const disc = ast.Read{ .connector = u.discover_conn, .form = .{ .query = u.discover_query } };
        for (try discoverRows(env, disc, 2)) |row| {
            const parts = try arena.alloc([]const u8, 1);
            parts[0] = row[0];
            try specs.append(.{ .read = .{ .connector = u.discover_conn, .form = .{ .table = .{ .parts = parts } }, .where = where }, .tag = row[1], .name = row[0] });
        }
    } else for (u.branches) |b| {
        var rd = b.read;
        if (where.len > 0) rd.where = where;
        try specs.append(.{ .read = rd, .tag = b.tag, .name = readName(b.read) });
    }
    return specs.toOwnedSlice();
}

/// A string-valued field of a JSON object, or null if absent / not a string.
fn jsonStrField(obj: std.json.ObjectMap, key: []const u8) ?[]const u8 {
    return switch (obj.get(key) orelse return null) {
        .string => |s| s,
        else => null,
    };
}

/// A hint value as a name, accepting either a bare ident or a quoted string
/// (`@[table_field = physical]` or `@[tag_substr = "4,2"]`).
fn hasFlagHint(hints: []const ast.Hint, key: []const u8) bool {
    for (hints) |h| {
        if (std.mem.eql(u8, h.key, key)) return true;
    }
    return false;
}

fn forHintName(hints: []const ast.Hint, key: []const u8) ?[]const u8 {
    for (hints) |h| {
        if (std.mem.eql(u8, h.key, key)) return switch (h.value) {
            .ident => |s| s,
            .str => |s| s,
            else => null,
        };
    }
    return null;
}

/// Derive a substring of `s` from a `"start,len"` spec (1-based start, matching the
/// `substr` builtin). Returns null on a malformed/out-of-range spec.
fn deriveSubstr(s: []const u8, spec: []const u8) ?[]const u8 {
    const comma = std.mem.indexOfScalar(u8, spec, ',') orelse return null;
    const start = std.fmt.parseInt(usize, std.mem.trim(u8, spec[0..comma], " "), 10) catch return null;
    const len = std.fmt.parseInt(usize, std.mem.trim(u8, spec[comma + 1 ..], " "), 10) catch return null;
    if (start == 0 or start > s.len) return null;
    const a = start - 1;
    return s[a..@min(a + len, s.len)];
}

/// Pick the canon schema among the branch schemas: a named source table, or the
/// first branch.
fn unionCanon(env: *Env, specs: []const UnionSpec, schemas: []const types.Schema, canon_opt: ?[]const u8) !types.Schema {
    if (canon_opt) |c| if (!std.mem.eql(u8, c, "first")) {
        for (specs, schemas) |s, sch| if (std.mem.eql(u8, s.name, c)) return sch;
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "union canon `{s}` is not one of the source tables", .{c}));
    };
    return schemas[0];
}

fn unionDownstreamMapOnly(stages: []const ast.Stage) bool {
    for (stages) |s| switch (s.node) {
        .filter, .select, .explode => {},
        else => return false,
    };
    return true;
}

/// Build the serial union op: open every branch (kept open, drained in order by
/// op.Union), reconcile each to the canon, and concatenate. Used when split isn't
/// applicable (threads=1, a breaker downstream, or non-splittable branches).
fn buildUnion(env: *Env, u: ast.Union, hints: []const ast.Hint) anyerror!PipeRes {
    const arena = env.arena;
    const tag_col = forHintIdent(hints, "tag");
    const canon_opt = forHintIdent(hints, "canon");
    const specs = try unionSpecs(env, u, hints);
    if (specs.len == 0) return planErr(env.diag, "union has no source tables");

    const children = try arena.alloc(op.Op, specs.len);
    const schemas = try arena.alloc(types.Schema, specs.len);
    for (specs, 0..) |s, i| {
        const raw = try openSource(env, s.read, hints);
        const cs = try arena.create(obs.CountingSource);
        cs.* = .{ .inner = raw, .count = env.rows_read };
        const src = cs.source();
        try env.sources.append(src);
        if (env.src_name.len == 0) env.src_name = connectorType(env, s.read.connector);
        const scan = try arena.create(op.Scan);
        scan.* = .{ .src = src };
        children[i] = .{ .scan = scan };
        schemas[i] = src.schema();
    }
    const canon = try dupeSchema(arena, try unionCanon(env, specs, schemas, canon_opt));

    var out_schema: types.Schema = undefined;
    for (specs, 0..) |s, i| {
        const items = try synthReconcile(arena, schemas[i], canon, tag_col, s.tag);
        const proj = try buildProject(env, items, schemas[i], children[i]);
        children[i] = proj.op;
        out_schema = proj.schema;
    }
    const un = try arena.create(op.Union);
    un.* = .{ .children = children };
    return .{ .op = .{ .union_ = un }, .schema = out_schema };
}

/// Split-parallel union: expand each branch into a `read | select(reconcile) |
/// <downstream maps> | write` pipeline and run it through runOutput, which
/// split-reads the single branch source into key-range lanes. Branches share the
/// sink — the first keeps the write mode (so `overwrite` truncates once), later
/// branches append/upsert into it.
fn runUnionSplit(env: *Env, u: ast.Union, hints: []const ast.Hint, downstream: []const ast.Stage, write_stage: ast.Stage, opts: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) !void {
    const arena = env.arena;
    const tag_col = forHintIdent(hints, "tag");
    const canon_opt = forHintIdent(hints, "canon");
    const specs = try unionSpecs(env, u, hints);
    if (specs.len == 0) return planErr(env.diag, "union has no source tables");

    const schemas = try arena.alloc(types.Schema, specs.len);
    for (specs, schemas) |s, *sch| {
        const src = try openSource(env, s.read, hints);
        sch.* = try dupeSchema(arena, src.schema());
        src.close();
    }
    const canon = try unionCanon(env, specs, schemas, canon_opt);

    var split_hints = std.array_list.Managed(ast.Hint).init(arena);
    for (hints) |h| {
        if (std.mem.eql(u8, h.key, "split") or std.mem.eql(u8, h.key, "splits") or std.mem.eql(u8, h.key, "split_kind"))
            try split_hints.append(h);
    }
    const branch_hints = try split_hints.toOwnedSlice();

    const w = write_stage.node.write;
    for (specs, schemas, 0..) |s, sch, i| {
        const items = try synthReconcile(arena, sch, canon, tag_col, s.tag);
        var bstages = std.array_list.Managed(ast.Stage).init(arena);
        try bstages.append(.{ .node = .{ .read = s.read }, .hints = branch_hints, .pos = u.pos });
        try bstages.append(.{ .node = .{ .select = items }, .hints = &.{}, .pos = u.pos });
        try bstages.appendSlice(downstream);
        const bmode: ast.WriteMode = if (i == 0 or w.mode != .overwrite) w.mode else .append;
        const bw = ast.Write{ .connector = w.connector, .form = w.form, .target = w.target, .mode = bmode };
        try bstages.append(.{ .node = .{ .write = bw }, .hints = write_stage.hints, .pos = write_stage.pos });
        try runOutput(env, .{ .stages = try bstages.toOwnedSlice(), .pos = u.pos }, opts, stats, lanes_used, batch_arena);
    }
}

/// Bridge an analyze-layer error (which writes `ad.msg`) into a plan error.
fn aErr(env: *Env, ad: *analyze.Diag, e: analyze.Error) anyerror {
    return switch (e) {
        error.OutOfMemory => error.OutOfMemory,
        error.AnalyzeFailed => planErr(env.diag, ad.msg),
    };
}

fn buildStage(env: *Env, stage: ast.Stage, child: op.Op, schema: types.Schema) anyerror!PipeRes {
    const arena = env.arena;
    switch (stage.node) {
        .filter => |pred0| {
            var ad = analyze.Diag{};
            const pred = analyze.checkFilter(arena, schema, pred0, env.params_expr, &ad) catch |e| return aErr(env, &ad, e);
            const f = try arena.create(op.Filter);
            f.* = .{ .child = child, .pred = pred, .err = env.errctx };
            return .{ .op = .{ .filter = f }, .schema = schema };
        },
        .select => |items| return buildProject(env, items, schema, child),
        .limit => |lim| {
            const l = try arena.create(op.Limit);
            l.* = .{ .child = child, .remaining = lim.count, .to_skip = lim.offset };
            return .{ .op = .{ .limit = l }, .schema = schema };
        },
        .distinct => |d| {
            var keys: ?[]const usize = null;
            if (d.on) |fields| {
                var ad = analyze.Diag{};
                keys = analyze.fieldIndices(arena, schema, fields, &ad) catch |e| return aErr(env, &ad, e);
            }
            const o = try arena.create(op.Distinct);
            o.* = .{ .child = child, .in_schema = try schemaPtr(arena, schema), .keys = keys, .state = arena, .gpa = env.gpa };
            return .{ .op = .{ .distinct = o }, .schema = schema };
        },
        .sort => |s| {
            const qs = try arena.alloc(ast.QualName, s.keys.len);
            for (s.keys, qs) |sk, *q| q.* = sk.field;
            var ad = analyze.Diag{};
            const idxs = analyze.fieldIndices(arena, schema, qs, &ad) catch |e| return aErr(env, &ad, e);
            const ks = try arena.alloc(op.Sort.Key, s.keys.len);
            for (s.keys, idxs, ks) |sk, idx, *k| k.* = .{ .idx = idx, .desc = sk.desc };
            const o = try arena.create(op.Sort);
            o.* = .{ .child = child, .in_schema = try schemaPtr(arena, schema), .keys = ks };
            return .{ .op = .{ .sort = o }, .schema = schema };
        },
        .aggregate => |ag| return buildAggregate(env, ag, schema, child),
        .join => |j| return buildJoin(env, j, schema, child),
        .explode => |ex| {
            var ad = analyze.Diag{};
            const ep = analyze.explodePlan(arena, schema, ex, &ad) catch |e| return aErr(env, &ad, e);
            const out = try schemaPtr(arena, ep.schema);
            const o = try arena.create(op.Explode);
            o.* = .{ .child = child, .field_idx = ep.idx, .delim = ex.delim orelse ",", .out_schema = out };
            return .{ .op = .{ .explode = o }, .schema = out.* };
        },
        .read, .ref, .write, .union_ => return planErr(env.diag, "unexpected operator in the middle of a pipeline"),
    }
}

fn buildProject(env: *Env, items: []const ast.SelectItem, in_schema: types.Schema, child: op.Op) anyerror!PipeRes {
    const arena = env.arena;
    var ad = analyze.Diag{};
    const rcols = analyze.selectCols(arena, in_schema, items, env.params_expr, &ad) catch |e| return aErr(env, &ad, e);

    const cols = try arena.alloc(op.Project.Col, rcols.len);
    for (rcols, cols) |rc, *c| c.* = .{
        .source = switch (rc.source) {
            .passthrough => |idx| .{ .passthrough = idx },
            .expr => |e| .{ .expr = e },
        },
        .ty = rc.ty,
    };
    const out = try arena.create(types.Schema);
    out.* = try analyze.schemaOfCols(arena, rcols);
    const p = try arena.create(op.Project);
    p.* = .{ .child = child, .cols = cols, .out_schema = out, .err = env.errctx };
    return .{ .op = .{ .project = p }, .schema = out.* };
}

fn buildAggregate(env: *Env, ag: ast.Aggregate, schema: types.Schema, child: op.Op) anyerror!PipeRes {
    const arena = env.arena;
    var ad = analyze.Diag{};
    const ap = analyze.aggregatePlan(arena, schema, ag, env.params_expr, &ad) catch |e| return aErr(env, &ad, e);
    const aggs = try arena.alloc(op.Aggregate.Agg, ap.aggs.len);
    for (ap.aggs, aggs) |ra, *a| a.* = .{ .func = ra.func, .arg = ra.arg, .ty = ra.ty, .distinct = ra.distinct };
    const out = try schemaPtr(arena, ap.schema);
    const o = try arena.create(op.Aggregate);
    o.* = .{ .child = child, .in_schema = try schemaPtr(arena, schema), .by = ap.by, .aggs = aggs, .out_schema = out, .err = env.errctx, .state = arena, .gpa = env.gpa };
    return .{ .op = .{ .aggregate = o }, .schema = out.* };
}

fn buildJoin(env: *Env, j: ast.Join, left_schema: types.Schema, probe: op.Op) anyerror!PipeRes {
    const arena = env.arena;
    if (env.bindings.get(j.binding) == null)
        return planErr(env.diag, try std.fmt.allocPrint(arena, "unknown binding `{s}` in join", .{j.binding}));
    const build = try buildPipeline(env, env.bindings.get(j.binding).?.stages);

    var ad = analyze.Diag{};
    const jp = analyze.joinPlan(arena, left_schema, build.schema, j, &ad) catch |e| return aErr(env, &ad, e);
    const out = try schemaPtr(arena, jp.schema);
    const o = try arena.create(op.Join);
    o.* = .{
        .probe = probe,
        .build = build.op,
        .left_key = jp.lk,
        .right_key = jp.rk,
        .left_schema = try schemaPtr(arena, left_schema),
        .right_schema = try schemaPtr(arena, build.schema),
        .out_schema = out,
        .kind = j.kind,
        .state = arena,
    };
    return .{ .op = .{ .join = o }, .schema = out.* };
}

fn openSource(env: *Env, rd: ast.Read, hints: []const ast.Hint) !driver.Source {
    return openSourceProjected(env, rd, hints, null, &.{});
}

/// `openSource`, with the column set the pipeline needs when it is known.
/// Only the parquet reader can act on it; every other source ignores it.
fn openSourceProjected(
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
        const s = walmod.BufferSource.open(env.gpa, dir, ref.name, declared, env.buffer_segment) catch |e| switch (e) {
            error.BufferEmpty => return planErr(env.diag, try std.fmt.allocPrint(env.arena, "buffer `{s}` at {s}: no segments to replay (and no declared schema)", .{ ref.name, dir })),
            else => return planErr(env.diag, try std.fmt.allocPrint(env.arena, "buffer `{s}` at {s}: {s}", .{ ref.name, dir, @errorName(e) })),
        };
        return s.source();
    }
    if (std.mem.eql(u8, rd.connector, "http")) {
        if (rd.form != .path) return planErr(env.diag, "read http needs a quoted URL");
        var hopts = httpsrc.optsFromHints(hints);
        hopts.logger = env.log;
        const s = httpsrc.HttpSource.open(env.arena, env.gpa, rd.form.path, hopts) catch |e|
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
        if (pqdecode.Reader.isPath(rd.form.path)) {
            const pr = pqdecode.Reader.open(env.arena, rd.form.path) catch |e|
                return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not read parquet `{s}` ({s})", .{ rd.form.path, try pathFail(env.arena, rd.form.path, e) }));
            return pr.source();
        }
        const reader = csv.CsvReader.open(env.arena, rd.form.path) catch |e| {
            // A mistyped prefix and a truly empty one are the same listing; say
            // which prefix came back empty rather than blaming the CSV parser.
            if (e == azure.Error.AzureEmptyPrefix)
                return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "no blobs under prefix `{s}`", .{rd.form.path}));
            return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not open input CSV `{s}` ({s})", .{ rd.form.path, try pathFail(env.arena, rd.form.path, e) }));
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
        const kvs = try env.arena.alloc(httpsrc.KV, conn.config.len);
        for (conn.config, kvs) |attr, *kv| kv.* = .{ .key = attr.key, .value = try evalCfgStr(env, attr.value) };
        var errmsg: []const u8 = "";
        const cc = httpsrc.connFromKvs(env.arena, kvs, &errmsg) catch
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "http connection `{s}`: {s}", .{ rd.connector, errmsg }));
        var hopts = httpsrc.optsFromHints(hints);
        hopts.logger = env.log;
        const s = httpsrc.HttpSource.openConn(env.arena, env.gpa, cc, rd.form.path, hopts) catch |e|
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

/// How a SQL Server connection authenticates: a SQL login (default), an Azure AD
/// token (`auth = 'aad'`), or Windows NTLMv2 (`auth = 'ntlm'`).
const DbAuth = enum { sql, aad, ntlm };

const DbConfig = struct {
    host: []const u8 = "",
    port: u16,
    /// Was `port` set explicitly in the connection config? A named instance
    /// (`host\INSTANCE`) resolves its port via the SQL Server Browser only when
    /// this is false — an explicit port skips the lookup (firewalled UDP 1434).
    port_explicit: bool = false,
    user: []const u8 = "",
    password: []const u8 = "",
    database: []const u8 = "",
    tls: sql.TlsMode = .off,
    auth: DbAuth = .sql,
    domain: []const u8 = "",
    client_id: []const u8 = "",
    resource: []const u8 = "",
    token: []const u8 = "",
};

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
        if (std.mem.eql(u8, k, "port")) {
            if (try f.port(attr.value)) |p| {
                cfg.port = p;
                cfg.port_explicit = true;
            }
            continue;
        }
        if (!eqlAny(k, &.{ "host", "user", "password", "database", "tls", "auth", "domain", "client_id", "resource", "token" })) continue;
        const v = (try f.str(attr.value)) orelse continue;
        if (std.mem.eql(u8, k, "host")) {
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
fn sqlWithWhere(arena: std.mem.Allocator, base: []const u8, is_query: bool, where: []const u8) ![]const u8 {
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

/// Pull `@[split = col]` / `@[splits = N]` / `@[split_kind = int|uuid|date]`
/// off the leading read stage.
const SplitHints = struct { col: ?[]const u8 = null, count: ?usize = null, kind: ?splitmod.KeyKind = null };
fn splitHints(stage: ast.Stage) SplitHints {
    var h = SplitHints{};
    for (stage.hints) |hint| {
        if (std.mem.eql(u8, hint.key, "split")) {
            if (hint.value == .ident) h.col = hint.value.ident;
        } else if (std.mem.eql(u8, hint.key, "splits")) {
            if (hint.value == .int and hint.value.int > 0) h.count = @intCast(hint.value.int);
        } else if (std.mem.eql(u8, hint.key, "split_kind")) {
            if (hint.value == .ident) h.kind = std.meta.stringToEnum(splitmod.KeyKind, hint.value.ident);
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

fn planSplit(env: *Env, desc: SqlDesc, lead: ast.Stage, threads: usize, w: ast.Write) !?splitmod.Plan {
    const hints = splitHints(lead);
    const forced = hints.col != null or hints.count != null;
    const m: usize = hints.count orelse @min(@as(usize, 64), threads * 4);
    if (m < 2) return null;
    if (!forced and isPostgresCopySink(env, w)) return null;

    var pctx = SplitCtx{ .gpa = env.gpa, .kind = desc.kind, .cfg = desc.cfg, .base_sql = desc.base_sql };
    const prober = splitmod.Prober{ .ctx = &pctx, .openFn = proberOpen };

    var key: splitmod.Key = undefined;
    if (hints.col) |col| {
        key = .{ .col = col, .kind = hints.kind orelse .int };
    } else if (desc.table) |table| {
        const info = (try splitmod.introspectKey(env.arena, prober, desc.dialect, table)) orelse return null;
        if (!forced and info.est_rows < splitmod.min_rows_to_split) return null;
        key = info.key;
    } else {
        return null;
    }
    return splitmod.plan(env.arena, prober, desc.dialect, desc.base_sql, key, m);
}

fn proberOpen(ctx_ptr: *anyopaque) anyerror!sql.Conn {
    const ctx: *SplitCtx = @ptrCast(@alignCast(ctx_ptr));
    return connectSql(ctx.gpa, ctx.kind, ctx.cfg);
}

const SqlConnInfo = struct { kind: SqlKind, dialect: sql.Dialect, port: u16 };

fn sqlConnInfo(conn: ast.Connection) ?SqlConnInfo {
    if (std.mem.eql(u8, conn.connector, "postgres")) return .{ .kind = .postgres, .dialect = .postgres, .port = 5432 };
    if (std.mem.eql(u8, conn.connector, "mysql")) return .{ .kind = .mysql, .dialect = .mysql, .port = 3306 };
    if (std.mem.eql(u8, conn.connector, "sqlserver")) return .{ .kind = .sqlserver, .dialect = .sqlserver, .port = 1433 };
    return null;
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

fn dupeSchema(arena: std.mem.Allocator, s: types.Schema) !types.Schema {
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
fn resolveUpsertKeys(env: *Env, w: ast.Write) !ast.Write {
    if (w.mode != .upsert or w.mode.upsert.keys.len > 0) return w;
    const desc = env.sql_desc orelse return planErr(env.diag, "`upsert` without `on <key>` infers the primary key from the source, which needs a SQL `table` read — this pipeline's source can't be introspected; name the key with `upsert on <col>`");
    const table = desc.table orelse return planErr(env.diag, "`upsert` key inference needs `read <conn> table <name>` (a `query` source has no single table to introspect); name the key with `upsert on <col>`");
    var pctx = SplitCtx{ .gpa = env.gpa, .kind = desc.kind, .cfg = desc.cfg, .base_sql = desc.base_sql };
    const prober = splitmod.Prober{ .ctx = &pctx, .openFn = proberOpen };
    const keys = splitmod.introspectPkCols(env.arena, prober, desc.dialect, table) catch |e|
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

fn openSink(env: *Env, w: ast.Write, schema: types.Schema) !driver.Sink {
    if (env.explain) return DiscardSink.sink();
    if (std.mem.eql(u8, w.connector, "stdout")) {
        if (env.stdout_json) {
            const writer = tablemod.JsonWriter.open(env.gpa, schema) catch
                return planErr(env.diag, "could not open stdout json writer");
            return writer.sink();
        }
        const writer = tablemod.TableWriter.open(env.gpa, schema) catch
            return planErr(env.diag, "could not open stdout table");
        return writer.sink();
    }
    if (std.mem.eql(u8, w.connector, "csv")) {
        const fmode = try fileWriteMode(env, w);
        // A `.parquet` target shares the csv connector but is a different format;
        // without this it would be written as CSV text under a .parquet name.
        if (pqwrite.Writer.isPath(w.target)) {
            const pw = pqwrite.Writer.open(env.arena, w.target, schema, .snappy, fmode) catch |e|
                return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not open output parquet `{s}` ({s})", .{ w.target, try pathFail(env.arena, w.target, e) }));
            return pw.sink();
        }
        const writer = csv.CsvWriter.open(env.arena, w.target, schema, fmode) catch |e|
            return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not open output CSV `{s}` ({s})", .{ w.target, try pathFail(env.arena, w.target, e) }));
        return writer.sink();
    }
    const conn = env.connections.get(w.connector) orelse
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "unknown connection `{s}`", .{w.connector}));
    if (std.mem.eql(u8, conn.connector, "starrocks")) {
        const cfg = try resolveStarrocksConfig(env, conn);
        const s = starrocks.StreamLoadSink.open(env.gpa, cfg, w.target, schema, w.mode) catch |e|
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "starrocks sink open failed ({s}) — {s}", .{ @errorName(e), env.diag.msg }));
        s.logger = env.log;
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

fn eqlAny(k: []const u8, opts: []const []const u8) bool {
    for (opts) |o| {
        if (std.mem.eql(u8, k, o)) return true;
    }
    return false;
}

fn resolveParams(arena: std.mem.Allocator, program: ast.Program, cli: []const ParamArg, params: *std.StringHashMap(Value), diag: *Diag) !void {
    // A LET is sealed: it is computed by the script, never bound from outside.
    // Naming one on the command line is a mistake worth reporting, not ignoring.
    for (cli) |kv| {
        for (program.stmts) |s| {
            if (s != .let_const) continue;
            if (std.mem.eql(u8, kv.key, s.let_const.name))
                return planErr(diag, try std.fmt.allocPrint(arena, "`{s}` is a LET, not a PARAM — it cannot be bound externally", .{kv.key}));
        }
    }
    for (program.stmts) |s| {
        if (s != .param) continue;
        const p = s.param;
        if (p.is_json) continue;
        var v: ?Value = null;
        for (cli) |kv| {
            if (std.mem.eql(u8, kv.key, p.name)) {
                v = try parseParamValue(arena, p.ty, kv.val, diag);
                break;
            }
        }
        if (v == null) {
            if (p.default) |d| {
                v = try constEvalDefault(d, diag);
            } else {
                return planErr(diag, try std.fmt.allocPrint(arena, "missing required param `{s}`", .{p.name}));
            }
        }
        try params.put(p.name, v.?);
    }
}

/// Fold every statement-level `LET name = <expr>;` into a plan-time constant, in
/// declaration order, and register it under the same two maps a PARAM uses — so
/// `$name` substitutes through the ordinary machinery and every pipeline in the
/// script sees one identical value. Each expression is evaluated with the params
/// and the earlier LETs in scope; a LET is never bound from outside, so this is
/// the only place its value is decided.
fn resolveLets(
    arena: std.mem.Allocator,
    program: ast.Program,
    params: *std.StringHashMap(Value),
    params_expr: *std.StringHashMap(*const ast.Expr),
    diag: *Diag,
) !void {
    var names = std.array_list.Managed([]const u8).init(arena);
    var values = std.array_list.Managed(Value).init(arena);
    var it = params.iterator();
    while (it.next()) |kv| {
        try names.append(kv.key_ptr.*);
        try values.append(kv.value_ptr.*);
    }

    for (program.stmts) |s| {
        if (s != .let_const) continue;
        const l = s.let_const;
        for (program.stmts) |p| {
            if (p == .param and std.mem.eql(u8, p.param.name, l.name))
                return planErr(diag, try std.fmt.allocPrint(arena, "`{s}` is declared twice: LET and PARAM share one name space", .{l.name}));
        }
        if (params.contains(l.name))
            return planErr(diag, try std.fmt.allocPrint(arena, "duplicate LET `{s}`", .{l.name}));

        const v = eval.constEval(arena, l.expr, names.items, values.items) catch |e|
            return planErr(diag, try std.fmt.allocPrint(arena, "LET `{s}`: {s}", .{ l.name, @errorName(e) }));
        try names.append(l.name);
        try values.append(v);
        try params.put(l.name, v);
        try params_expr.put(l.name, try mkLit(arena, v));
    }
}

fn parseParamValue(arena: std.mem.Allocator, ty: types.Type, str: []const u8, diag: *Diag) !Value {
    return switch (ty.kind) {
        .int => .{ .int = std.fmt.parseInt(i64, str, 10) catch return planErr(diag, "invalid integer param value") },
        .float => .{ .float = std.fmt.parseFloat(f64, str) catch return planErr(diag, "invalid float param value") },
        .string => .{ .string = try arena.dupe(u8, str) },
        .bool => if (std.mem.eql(u8, str, "true")) Value{ .bool = true } else if (std.mem.eql(u8, str, "false")) Value{ .bool = false } else planErr(diag, "invalid bool param value"),
        else => planErr(diag, "unsupported param type for CLI binding"),
    };
}

fn constEvalDefault(expr: *const ast.Expr, diag: *Diag) !Value {
    return switch (expr.*) {
        .int_lit => |i| .{ .int = i },
        .float_lit => |f| .{ .float = f },
        .str_lit => |s| .{ .string = s },
        .bool_lit => |b| .{ .bool = b },
        .null_lit => .null,
        else => planErr(diag, "param default must be a literal"),
    };
}

fn mk(arena: std.mem.Allocator, e: ast.Expr) anyerror!*ast.Expr {
    const p = try arena.create(ast.Expr);
    p.* = e;
    return p;
}

fn mkLit(arena: std.mem.Allocator, v: Value) anyerror!*ast.Expr {
    return switch (v) {
        .null => mk(arena, .null_lit),
        .bool => |b| mk(arena, .{ .bool_lit = b }),
        .int => |i| mk(arena, .{ .int_lit = i }),
        .float => |f| mk(arena, .{ .float_lit = f }),
        .string => |s| mk(arena, .{ .str_lit = s }),
        // The DSL has no date/time literal; comparisons coerce a string against a
        // temporal column either way, so the ISO text is the faithful literal for
        // a folded `LET cutoff = date_add('day', -7, today())`.
        .date, .time, .timestamp => mk(arena, .{ .str_lit = try eval.valueToString(arena, v) }),
        else => mk(arena, .null_lit),
    };
}

fn setMsg(diag: *Diag, msg: []const u8) void {
    const n = @min(msg.len, diag.buf.len);
    @memcpy(diag.buf[0..n], msg[0..n]);
    diag.msg = diag.buf[0..n];
}

fn planErr(diag: *Diag, msg: []const u8) error{PlanFailed} {
    setMsg(diag, msg);
    return error.PlanFailed;
}

/// `planErr` that also classifies the underlying error as transient/permanent,
/// so the wrapped (PlanFailed) result still carries retry intent to the CLI.
fn planErrT(diag: *Diag, e: anyerror, msg: []const u8) error{PlanFailed} {
    if (isTransient(e)) diag.retryable = true;
    setMsg(diag, msg);
    return error.PlanFailed;
}

/// Which layer a file-shaped path resolves to, for error messages. A bare
/// `FileNotFound` on an `az://` path reads as a storage-account problem and
/// sends the reader off to check credentials and containers; naming the layer
/// that actually failed ends that detour at the first error.
fn pathLayer(path: []const u8) []const u8 {
    if (azure.isUrl(path)) return "azure blob";
    if (std.mem.startsWith(u8, path, "http://") or std.mem.startsWith(u8, path, "https://")) return "http";
    return "local file";
}

/// `<layer>: <ErrorName>`, the parenthetical every file-shaped open error carries.
fn pathFail(arena: std.mem.Allocator, path: []const u8, e: anyerror) ![]const u8 {
    return std.fmt.allocPrint(arena, "{s}: {s}", .{ pathLayer(path), @errorName(e) });
}

fn schemaPtr(arena: std.mem.Allocator, schema: types.Schema) !*types.Schema {
    const p = try arena.create(types.Schema);
    p.* = schema;
    return p;
}

const parser = @import("../lang/sql_parser.zig");

/// Run `LOAD INTO out.csv AS <query>` over `input`. `$IN` in the query is
/// replaced with the input CSV's path.
fn runToString(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir, input: []const u8, query: []const u8) ![]u8 {
    return runToStringP(alloc, tmp, input, query, &[_]ParamArg{});
}

fn runToStringP(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir, input: []const u8, query: []const u8, cli_params: []const ParamArg) ![]u8 {
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = input });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const q = try std.mem.replaceOwned(u8, alloc, query, "$IN", in_path);
    defer alloc.free(q);
    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}' AS {s};", .{ out_path, q });
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    _ = run(alloc, prog, .{ .params = cli_params }, &rdiag) catch |e| {
        std.debug.print("run error: {s} ({s})\n", .{ @errorName(e), rdiag.msg });
        return e;
    };
    return tmp.dir.readFileAlloc(alloc, "out.csv", 1 << 20);
}

test "CSV -> filter/select -> CSV round-trips" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(alloc, &tmp,
        "id,status,amount\n1,paid,100\n2,pending,50\n3,paid,200\n",
        "SELECT id, amount FROM '$IN' WHERE status = 'paid'",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,amount\n1,100\n3,200\n", out);
}

test "aggregate: count and sum by group (nulls skipped)" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(alloc, &tmp,
        "status,amount\npaid,100\npending,50\npaid,200\npaid,\n",
        "SELECT status, COUNT(*) AS n, SUM(CAST(amount AS INT)) AS total FROM '$IN' GROUP BY status ORDER BY status ASC",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("status,n,total\npaid,3,300\npending,1,50\n", out);
}

test "sort: numeric desc, nulls last" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(alloc, &tmp,
        "id,amount\n1,100\n2,\n3,200\n",
        "SELECT id, CAST(amount AS INT) AS amt FROM '$IN' ORDER BY amt DESC",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,amt\n3,200\n1,100\n2,\n", out);
}

test "aggregate: group by a numeric (int) key (value-keyed hashing)" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(alloc, &tmp,
        "id,n\n1,5\n2,5\n3,7\n4,5\n",
        "SELECT CAST(n AS INT) AS g, COUNT(*) AS c FROM '$IN' GROUP BY g ORDER BY g ASC",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("g,c\n5,3\n7,1\n", out);
}

/// Run `read csv | <body> | write csv` with an explicit thread count, returning
/// out.csv. Used to exercise the parallel CSV-aggregate path (`threads > 1`), which
/// the default in-process harness (`threads = 1`) never reaches.
fn runCsvThreaded(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir, input: []const u8, query: []const u8, threads: usize) ![]u8 {
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = input });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);
    const q = try std.mem.replaceOwned(u8, alloc, query, "$IN", in_path);
    defer alloc.free(q);
    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}' AS {s};", .{ out_path, q });
    defer alloc.free(script);
    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    _ = run(alloc, prog, .{ .threads = threads }, &rdiag) catch |e| {
        std.debug.print("run error: {s} ({s})\n", .{ @errorName(e), rdiag.msg });
        return e;
    };
    return tmp.dir.readFileAlloc(alloc, "out.csv", 1 << 20);
}

test "parallel CSV aggregate: global agg (threads>1) matches serial" {
    const alloc = std.testing.allocator;
    const input = "id,v\n1,10\n2,20\n3,30\n4,40\n5,50\n";
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const par = try runCsvThreaded(alloc, &tmp, input, "SELECT COUNT(*) AS n, SUM(CAST(v AS INT)) AS s FROM '$IN'", 4);
    defer alloc.free(par);
    try std.testing.expectEqualStrings("n,s\n5,150\n", par);
}

test "parallel CSV aggregate: filter/select prefix + sort/limit tail (threads>1)" {
    const alloc = std.testing.allocator;
    const input = "id,g,v\n1,a,10\n2,b,20\n3,a,30\n4,b,5\n5,a,50\n";
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runCsvThreaded(alloc, &tmp, input,
        "SELECT g, SUM(CAST(v AS INT)) AS s FROM '$IN' WHERE CAST(v AS INT) > 6 GROUP BY g ORDER BY s DESC LIMIT 1",
        4);
    defer alloc.free(out);
    try std.testing.expectEqualStrings("g,s\na,90\n", out);
}

test "parallel CSV distinct (threads>1): dedups across chunks" {
    const alloc = std.testing.allocator;
    const input = "id,g\n1,a\n2,b\n3,a\n4,c\n5,b\n6,a\n";
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runCsvThreaded(alloc, &tmp, input,
        "SELECT DISTINCT g FROM '$IN' ORDER BY g ASC",
        4);
    defer alloc.free(out);
    try std.testing.expectEqualStrings("g\na\nb\nc\n", out);
}

test "parallel CSV Top-N: sort | limit (threads>1) matches serial" {
    const alloc = std.testing.allocator;
    const input = "id,v\n1,10\n2,40\n3,20\n4,50\n5,30\n";
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runCsvThreaded(alloc, &tmp, input,
        "SELECT id, CAST(v AS INT) AS v FROM '$IN' ORDER BY v DESC, id ASC LIMIT 3",
        4);
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,v\n4,50\n2,40\n5,30\n", out);
}

test "parallel CSV aggregate: grouped agg (threads>1) merges partials by key" {
    const alloc = std.testing.allocator;
    const input = "id,g,v\n1,a,10\n2,b,20\n3,a,30\n4,b,40\n5,a,50\n";
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const par = try runCsvThreaded(alloc, &tmp, input, "SELECT g, SUM(CAST(v AS INT)) AS s FROM '$IN' GROUP BY g", 4);
    defer alloc.free(par);
    try std.testing.expect(std.mem.startsWith(u8, par, "g,s\n"));
    try std.testing.expect(std.mem.indexOf(u8, par, "a,90\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, par, "b,60\n") != null);
    try std.testing.expectEqual(@as(usize, 3), std.mem.count(u8, par, "\n"));
}

test "distinct: multi-column key (value-keyed)" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(alloc, &tmp,
        "a,b\nx,1\nx,1\nx,2\ny,1\n",
        "SELECT DISTINCT ON (a, b) * FROM '$IN'",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("a,b\nx,1\nx,2\ny,1\n", out);
}

test "top-N: sort | limit fuses to the K largest, in order" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(alloc, &tmp,
        "id,amount\n1,100\n2,50\n3,200\n4,\n5,150\n",
        "SELECT id, CAST(amount AS INT) AS amt FROM '$IN' ORDER BY amt DESC LIMIT 2",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,amt\n3,200\n5,150\n", out);
}

test "top-N: offset skips before taking" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(alloc, &tmp,
        "id,amount\n1,100\n2,50\n3,200\n4,\n5,150\n",
        "SELECT id, CAST(amount AS INT) AS amt FROM '$IN' ORDER BY amt DESC LIMIT 2 OFFSET 1",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,amt\n5,150\n1,100\n", out);
}

test "top-N: nulls sort last, matching a full sort | limit-all" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(alloc, &tmp,
        "id,amount\n1,100\n2,50\n3,200\n4,\n5,150\n",
        "SELECT id, CAST(amount AS INT) AS amt FROM '$IN' ORDER BY amt DESC LIMIT 99",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,amt\n3,200\n5,150\n1,100\n2,50\n4,\n", out);
}

test "distinct keeps first row per key" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(alloc, &tmp,
        "status,amount\npaid,100\npending,50\npaid,200\n",
        "SELECT DISTINCT ON (status) * FROM '$IN'",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("status,amount\npaid,100\npending,50\n", out);
}

test "for-each over a JSON array param iterates and binds fields by name" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,status\n1,paid\n2,pending\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const body = try std.fmt.allocPrint(alloc, "{{\"tables\":[{{\"name\":\"{s}\"}}]}}", .{in_path});
    defer alloc.free(body);
    const script = try std.fmt.allocPrint(alloc, "PARAM job JSON FROM BODY;\n" ++
        "FOR EACH ROW OF ($job.tables) AS (name) SEQUENTIAL\n" ++
        "  LOAD INTO '{s}' AS SELECT id FROM '${{name}}';\n" ++
        "END FOR;", .{out_path});
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    _ = run(alloc, prog, .{ .request_body = body }, &rdiag) catch |e| {
        std.debug.print("run error: {s} ({s})\n", .{ @errorName(e), rdiag.msg });
        return e;
    };
    const out = try tmp.dir.readFileAlloc(alloc, "out.csv", 1 << 20);
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id\n1\n2\n", out);
}

test "for-each loop var interpolates into a select column value" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,status\n1,paid\n2,pending\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const body = try std.fmt.allocPrint(alloc, "{{\"tables\":[{{\"name\":\"{s}\",\"emp\":\"01\"}}]}}", .{in_path});
    defer alloc.free(body);
    const script = try std.fmt.allocPrint(alloc, "PARAM job JSON FROM BODY;\n" ++
        "FOR EACH ROW OF ($job.tables) AS (name, emp) SEQUENTIAL\n" ++
        "  LOAD INTO '{s}' AS SELECT id, '${{emp}}' AS EMPRESA FROM '${{name}}';\n" ++
        "END FOR;", .{out_path});
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    _ = run(alloc, prog, .{ .request_body = body }, &rdiag) catch |e| {
        std.debug.print("run error: {s} ({s})\n", .{ @errorName(e), rdiag.msg });
        return e;
    };
    const out = try tmp.dir.readFileAlloc(alloc, "out.csv", 1 << 20);
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,EMPRESA\n1,01\n2,01\n", out);
}

test "for-each loop var used as an expression value binds per row" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "names.csv", .data = "name\nalpha\nbeta\n" });
    try tmp.dir.writeFile(.{ .sub_path = "alpha.csv", .data = "id\n1\n" });
    try tmp.dir.writeFile(.{ .sub_path = "beta.csv", .data = "id\n2\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(alloc,
        "FOR EACH ROW OF ('{s}/names.csv') AS (name)\n" ++
            "  LOAD INTO '{s}/out_${{name}}.csv' AS SELECT id, $name AS empresa FROM '{s}/${{name}}.csv';\nEND FOR;",
        .{ base, base, base });
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    _ = run(alloc, prog, .{}, &rdiag) catch |e| {
        std.debug.print("run error: {s} ({s})\n", .{ @errorName(e), rdiag.msg });
        return e;
    };

    const a = try tmp.dir.readFileAlloc(alloc, "out_alpha.csv", 1 << 20);
    defer alloc.free(a);
    const b = try tmp.dir.readFileAlloc(alloc, "out_beta.csv", 1 << 20);
    defer alloc.free(b);
    try std.testing.expectEqualStrings("id,empresa\n1,alpha\n", a);
    try std.testing.expectEqualStrings("id,empresa\n2,beta\n", b);
}

test "for-each: a typed loop var used as a value binds as its declared type" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "nums.csv", .data = "n\n5\n" });
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n7\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    // `$n * 2` only type-checks (and yields 10) if `n` bound as an int, not text.
    const script = try std.fmt.allocPrint(alloc,
        "FOR EACH ROW OF ('{s}/nums.csv') AS (n:INT)\n" ++
            "  LOAD INTO '{s}/out.csv' AS SELECT id, $n * 2 AS twice FROM '{s}/in.csv';\nEND FOR;",
        .{ base, base, base });
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,twice\n7,10\n", out);
}

test "for-each: a loop var shadows a same-named source column" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "names.csv", .data = "name\nalpha\n" });
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,name\n1,from_file\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(alloc,
        "FOR EACH ROW OF ('{s}/names.csv') AS (name)\n" ++
            "  LOAD INTO '{s}/out.csv' AS SELECT id, $name AS who FROM '{s}/in.csv';\nEND FOR;",
        .{ base, base, base });
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,who\n1,alpha\n", out);
}

test "interpAll: bare-var fast path and expression bodies" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const names = [_][]const u8{ "name", "pk" };

    const empty = [_][]const u8{ "Account", "" };
    const empty_row = LoopRow{ .names = &names, .cells = &empty };
    try std.testing.expectEqualStrings("Account", try interpAll(a, "${name}", empty_row));
    try std.testing.expectEqualStrings("${nope}", try interpAll(a, "${nope}", empty_row));

    try std.testing.expectEqualStrings("crm_account", try interpAll(a, "crm_${lower(name)}", empty_row));
    try std.testing.expectEqualStrings("ACCOUNT", try interpAll(a, "${upper(name)}", empty_row));

    const key = "${if(pk == '', concat(lower(name), 'id'), pk)}";
    try std.testing.expectEqualStrings("accountid", try interpAll(a, key, empty_row));

    const given = [_][]const u8{ "ListMember", "lm_custom_id" };
    try std.testing.expectEqualStrings("lm_custom_id", try interpAll(a, key, .{ .names = &names, .cells = &given }));

    try std.testing.expectEqualStrings("}", try interpAll(a, "${if(pk == '', '}', pk)}", empty_row));
}

test "interpAll: malformed bodies error" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const names = [_][]const u8{ "name", "pk" };
    const vals = [_][]const u8{ "Account", "" };
    const row = LoopRow{ .names = &names, .cells = &vals };
    try std.testing.expectError(error.InterpFailed, interpAll(a, "${if(pk ==)}", row));
    try std.testing.expectError(error.InterpFailed, interpAll(a, "${name:lower}", row));
}

test "interpAll: a typed loop var binds as its type in an expression body" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const names = [_][]const u8{"port"};
    const expr = "${if(port >= 1000, 'big', 'small')}";

    const typed = [_]?types.Type{types.Type.init(.int)};
    try std.testing.expectEqualStrings("big", try interpAll(a, expr, .{ .names = &names, .types = &typed, .cells = &[_][]const u8{"9030"} }));
    try std.testing.expectEqualStrings("small", try interpAll(a, expr, .{ .names = &names, .types = &typed, .cells = &[_][]const u8{"80"} }));

    const untyped = [_]?types.Type{null};
    try std.testing.expectError(error.InterpFailed, interpAll(a, expr, .{ .names = &names, .types = &untyped, .cells = &[_][]const u8{"9030"} }));
}

/// Parse and run a fully-assembled `script`, returning the contents of out.csv.
fn runScript(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir, script: []const u8, cli_params: []const ParamArg) ![]u8 {
    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    _ = run(alloc, prog, .{ .params = cli_params }, &rdiag) catch |e| {
        std.debug.print("run error: {s} ({s})\n", .{ @errorName(e), rdiag.msg });
        return e;
    };
    return tmp.dir.readFileAlloc(alloc, "out.csv", 1 << 20);
}

test "param substitution filters by a CLI-bound value" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,amount\n1,100\n2,200\n3,50\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const script = try std.fmt.allocPrint(alloc,
        "PARAM min INT DEFAULT 0;\nLOAD INTO '{s}' AS SELECT id FROM '{s}' WHERE CAST(amount AS INT) >= $min;",
        .{ out_path, in_path },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{.{ .key = "min", .val = "100" }});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id\n1\n2\n", out);
}

test "explode splits a delimited column into rows" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(alloc, &tmp,
        "id,tags\n1,\"a,b,c\"\n2,x\n3,\n",
        "SELECT * FROM '$IN' CROSS JOIN UNNEST(tags) AS tag",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,tag\n1,a\n1,b\n1,c\n2,x\n", out);
}

test "parallel driver matches serial output across many batches" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var in = std.array_list.Managed(u8).init(alloc);
    defer in.deinit();
    try in.appendSlice("id,amount\n");
    var k: usize = 0;
    while (k < 5000) : (k += 1) try in.writer().print("{d},{d}\n", .{ k, (k * 7) % 1000 });
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = in.items });

    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);

    var outputs: [2][]u8 = undefined;
    for ([_]usize{ 1, 4 }, 0..) |nthreads, idx| {
        const out_path = try std.fs.path.join(alloc, &.{ base, if (idx == 0) "s.csv" else "p.csv" });
        defer alloc.free(out_path);
        const script = try std.fmt.allocPrint(alloc,
            "LOAD INTO '{s}' AS SELECT id, CAST(amount AS INT) * 2 AS doubled FROM '{s}' WHERE CAST(amount AS INT) >= 500;",
            .{ out_path, in_path });
        defer alloc.free(script);

        var parena = std.heap.ArenaAllocator.init(alloc);
        defer parena.deinit();
        var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

        var rdiag: Diag = .{};
        _ = try run(alloc, prog, .{ .threads = nthreads }, &rdiag);
        outputs[idx] = try tmp.dir.readFileAlloc(alloc, if (idx == 0) "s.csv" else "p.csv", 1 << 20);
    }
    defer alloc.free(outputs[0]);
    defer alloc.free(outputs[1]);

    const s = try sortedLines(alloc, outputs[0]);
    defer alloc.free(s);
    const p = try sortedLines(alloc, outputs[1]);
    defer alloc.free(p);
    try std.testing.expectEqualStrings(s, p);
    try std.testing.expect(std.mem.indexOf(u8, outputs[1], "id,doubled\n") != null);
}

/// Sort the newline-separated lines of `text` (for order-insensitive comparison of
/// parallel vs serial output). Caller frees.
fn sortedLines(alloc: std.mem.Allocator, text: []const u8) ![]u8 {
    var lines = std.array_list.Managed([]const u8).init(alloc);
    defer lines.deinit();
    var it = std.mem.splitScalar(u8, text, '\n');
    while (it.next()) |l| try lines.append(l);
    std.mem.sort([]const u8, lines.items, {}, struct {
        fn lt(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lt);
    var out = std.array_list.Managed(u8).init(alloc);
    errdefer out.deinit();
    for (lines.items) |l| {
        try out.appendSlice(l);
        try out.append('\n');
    }
    return out.toOwnedSlice();
}

test "let binding + inner join" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,code\n1,A\n2,B\n3,Z\n" });
    try tmp.dir.writeFile(.{ .sub_path = "lookup.csv", .data = "code,label\nA,Apple\nB,Banana\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);
    const lookup_path = try std.fs.path.join(alloc, &.{ base, "lookup.csv" });
    defer alloc.free(lookup_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const script = try std.fmt.allocPrint(alloc,
        "LOAD INTO '{s}' AS\nWITH labels AS (SELECT * FROM '{s}')\nSELECT t.id, l.label FROM '{s}' t JOIN labels l ON t.code = l.code;",
        .{ out_path, lookup_path, in_path },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,label\n1,Apple\n2,Banana\n", out);
}

test "aggregate folds groups across multiple batches" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var in_buf = std.array_list.Managed(u8).init(alloc);
    defer in_buf.deinit();
    try in_buf.appendSlice("code,amount,name\n");
    var i: usize = 0;
    while (i < 3000) : (i += 1) {
        try in_buf.writer().print("{c},{d},n{d:0>4}\n", .{ "XY"[i % 2], i, i });
    }
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = in_buf.items });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(alloc,
        "LOAD INTO '{s}/out.csv' AS SELECT code, COUNT(*) AS n, SUM(CAST(amount AS INT)) AS total, MIN(name) AS first_name FROM '{s}/in.csv' GROUP BY code ORDER BY code;",
        .{ base, base },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("code,n,total,first_name\nX,1500,2248500,n0000\nY,1500,2250000,n0001\n", out);
}

test "global aggregate streams vectorized partials across batches" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var in_buf = std.array_list.Managed(u8).init(alloc);
    defer in_buf.deinit();
    try in_buf.appendSlice("amount\n");
    var i: usize = 0;
    while (i < 3000) : (i += 1) {
        try in_buf.writer().print("{d}\n", .{i});
    }
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = in_buf.items });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(alloc,
        "LOAD INTO '{s}/out.csv' AS SELECT COUNT(*) AS n, SUM(CAST(amount AS INT)) AS total, MIN(CAST(amount AS INT)) AS lo, MAX(CAST(amount AS INT)) AS hi FROM '{s}/in.csv';",
        .{ base, base },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("n,total,lo,hi\n3000,4498500,0,2999\n", out);
}

test "distinct dedups across multiple batches" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var in_buf = std.array_list.Managed(u8).init(alloc);
    defer in_buf.deinit();
    try in_buf.appendSlice("code\n");
    var i: usize = 0;
    while (i < 3000) : (i += 1) {
        try in_buf.writer().print("{c}\n", .{"XYZ"[i % 3]});
    }
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = in_buf.items });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(alloc,
        "LOAD INTO '{s}/out.csv' AS SELECT DISTINCT * FROM '{s}/in.csv';",
        .{ base, base },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("code\nX\nY\nZ\n", out);
}

test "join probe side spanning multiple batches" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var in_buf = std.array_list.Managed(u8).init(alloc);
    defer in_buf.deinit();
    try in_buf.appendSlice("id,code\n");
    var i: usize = 0;
    while (i < 2500) : (i += 1) {
        try in_buf.writer().print("{d},{s}\n", .{ i, if (i % 2 == 0) "A" else "B" });
    }
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = in_buf.items });
    try tmp.dir.writeFile(.{ .sub_path = "lookup.csv", .data = "code,label\nA,Apple\nB,Banana\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);
    const lookup_path = try std.fs.path.join(alloc, &.{ base, "lookup.csv" });
    defer alloc.free(lookup_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const script = try std.fmt.allocPrint(alloc,
        "LOAD INTO '{s}' AS\nWITH labels AS (SELECT * FROM '{s}')\nSELECT t.id, l.label FROM '{s}' t JOIN labels l ON t.code = l.code;",
        .{ out_path, lookup_path, in_path },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqual(@as(usize, 2501), std.mem.count(u8, out, "\n"));
    try std.testing.expect(std.mem.startsWith(u8, out, "id,label\n0,Apple\n1,Banana\n"));
    try std.testing.expect(std.mem.indexOf(u8, out, "\n2499,Banana\n") != null);
}

test "union reconciles branches to a canon schema (tag, null-fill, drop-extra)" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "a.csv", .data = "id,v\n1,10\n2,20\n" });
    try tmp.dir.writeFile(.{ .sub_path = "b.csv", .data = "id,w\n3,99\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(
        alloc,
        "LOAD INTO '{s}/out.csv' AS\nSELECT '01' AS src, t.* FROM '{s}/a.csv' t\nUNION ALL BY NAME\nSELECT '02' AS src, t.* FROM '{s}/b.csv' t\nANCHOR SCHEMA first;",
        .{ base, base, base },
    );
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    _ = run(alloc, prog, .{}, &rdiag) catch |e| {
        std.debug.print("run error: {s} ({s})\n", .{ @errorName(e), rdiag.msg });
        return e;
    };
    const out = try tmp.dir.readFileAlloc(alloc, "out.csv", 1 << 20);
    defer alloc.free(out);
    try std.testing.expect(std.mem.startsWith(u8, out, "src,id,v\n"));
    try std.testing.expect(std.mem.indexOf(u8, out, "w") == null);
    try std.testing.expect(std.mem.indexOf(u8, out, "01,1,10") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "01,2,20") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "02,3,") != null);
}

test "for-each fans out over a discovered list with interpolation" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "names.csv", .data = "name\nalpha\nbeta\n" });
    try tmp.dir.writeFile(.{ .sub_path = "alpha.csv", .data = "id,v\n1,10\n2,20\n" });
    try tmp.dir.writeFile(.{ .sub_path = "beta.csv", .data = "id,v\n3,30\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(
        alloc,
        "FOR EACH ROW OF ('{s}/names.csv') AS (name)\n  LOAD INTO '{s}/out_${{name}}.csv' AS SELECT id, v FROM '{s}/${{name}}.csv';\nEND FOR;",
        .{ base, base, base },
    );
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    const stats = run(alloc, prog, .{}, &rdiag) catch |e| {
        std.debug.print("run error: {s} ({s})\n", .{ @errorName(e), rdiag.msg });
        return e;
    };
    try std.testing.expectEqual(@as(u64, 3), stats.rows_out);

    const a = try tmp.dir.readFileAlloc(alloc, "out_alpha.csv", 1 << 20);
    defer alloc.free(a);
    const b = try tmp.dir.readFileAlloc(alloc, "out_beta.csv", 1 << 20);
    defer alloc.free(b);
    try std.testing.expectEqualStrings("id,v\n1,10\n2,20\n", a);
    try std.testing.expectEqualStrings("id,v\n3,30\n", b);
}

test "log defaults to warn: a healthy run says nothing" {
    try std.testing.expectEqual(obs.Level.warn, (LogConfig{}).level);
}

test "for-each discovers its rows from an in-engine SELECT" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "catalog.csv", .data = "name,active\nALPHA,1\nBETA,1\nGAMMA,0\n" });
    try tmp.dir.writeFile(.{ .sub_path = "alpha.csv", .data = "id,v\n1,10\n2,20\n" });
    try tmp.dir.writeFile(.{ .sub_path = "beta.csv", .data = "id,v\n3,30\n" });
    try tmp.dir.writeFile(.{ .sub_path = "gamma.csv", .data = "id,v\n9,90\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(
        alloc,
        "FOR EACH ROW OF (SELECT name, lower(name) AS slug FROM '{s}/catalog.csv' WHERE active = 1) AS (name, slug)\n  LOAD INTO '{s}/out_${{slug}}.csv' AS SELECT id, v FROM '{s}/${{slug}}.csv';\nEND FOR;",
        .{ base, base, base },
    );
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    const stats = run(alloc, prog, .{}, &rdiag) catch |e| {
        std.debug.print("run error: {s} ({s})\n", .{ @errorName(e), rdiag.msg });
        return e;
    };
    try std.testing.expectEqual(@as(u64, 3), stats.rows_out);

    const a = try tmp.dir.readFileAlloc(alloc, "out_alpha.csv", 1 << 20);
    defer alloc.free(a);
    const b = try tmp.dir.readFileAlloc(alloc, "out_beta.csv", 1 << 20);
    defer alloc.free(b);
    try std.testing.expectEqualStrings("id,v\n1,10\n2,20\n", a);
    try std.testing.expectEqualStrings("id,v\n3,30\n", b);
    // `active = 0` never reached the body: the discovery filter ran in-engine.
    try std.testing.expectError(error.FileNotFound, tmp.dir.access("out_gamma.csv", .{}));
}

test "for-each SELECT discovery with fewer columns than loop variables fails to plan" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "catalog.csv", .data = "name\nalpha\n" });
    try tmp.dir.writeFile(.{ .sub_path = "alpha.csv", .data = "id\n1\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(
        alloc,
        "FOR EACH ROW OF (SELECT name FROM '{s}/catalog.csv') AS (name, slug)\n  LOAD INTO '{s}/out_${{name}}.csv' AS SELECT id FROM '{s}/${{name}}.csv';\nEND FOR;",
        .{ base, base, base },
    );
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, run(alloc, prog, .{}, &rdiag));
    try std.testing.expect(std.mem.indexOf(u8, rdiag.msg, "fewer columns than loop variables") != null);
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

test "for-each parallel + on_error=continue isolates a failing table" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "names.csv", .data = "name\nalpha\nghost\n" });
    try tmp.dir.writeFile(.{ .sub_path = "alpha.csv", .data = "id\n7\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(
        alloc,
        "FOR EACH ROW OF ('{s}/names.csv') AS (name) PARALLEL ON ERROR CONTINUE\n  LOAD INTO '{s}/out_${{name}}.csv' AS SELECT * FROM '{s}/${{name}}.csv';\nEND FOR;",
        .{ base, base, base },
    );
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, run(alloc, prog, .{ .threads = 2 }, &rdiag));
    const a = try tmp.dir.readFileAlloc(alloc, "out_alpha.csv", 1 << 20);
    defer alloc.free(a);
    try std.testing.expectEqualStrings("id\n7\n", a);
}

test "FROM BUFFER replays WAL segments as a source (batch mode)" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const wal_dir = try std.fs.path.join(alloc, &.{ base, "wal" });
    defer alloc.free(wal_dir);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    {
        var w = try walmod.Wal.open(alloc, wal_dir, "ev", 1 << 20);
        defer w.close();
        try w.append("{\"device_id\":\"a\",\"v\":1}");
        try w.append("{\"device_id\":\"b\",\"v\":2}");
        try w.rotate();
        try w.append("{\"device_id\":\"c\",\"v\":3}");
        try w.sync();
    }

    const script = try std.fmt.allocPrint(alloc,
        "LOAD INTO '{s}' AS SELECT device_id, CAST(v AS INT) AS v FROM BUFFER 'ev' AT '{s}';",
        .{ out_path, wal_dir },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("device_id,v\na,1\nb,2\nc,3\n", out);
}

test "empty source and an all-dropping filter still write just the header" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const empty = try runToString(alloc, &tmp, "id,v\n", "SELECT id FROM '$IN'");
    defer alloc.free(empty);
    try std.testing.expectEqualStrings("id\n", empty);
    const dropped = try runToString(alloc, &tmp, "id,v\n1,10\n2,20\n", "SELECT * FROM '$IN' WHERE v = 999");
    defer alloc.free(dropped);
    try std.testing.expectEqualStrings("id,v\n", dropped);
}

test "csv aggregate: min/max on inferred numeric columns compare numerically, not lexically" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const got = try runToString(alloc, &tmp, "id\n9\n10\n2\n", "SELECT MIN(id) AS mn, MAX(id) AS mx, SUM(id) AS s FROM '$IN'");
    defer alloc.free(got);
    try std.testing.expectEqualStrings("mn,mx,s\n2,10,21\n", got);
}

test "limit: plain (unfused) limit takes the first N; offset past the end empties" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const first2 = try runToString(alloc, &tmp, "id\n1\n2\n3\n", "SELECT * FROM '$IN' LIMIT 2");
    defer alloc.free(first2);
    try std.testing.expectEqualStrings("id\n1\n2\n", first2);
    const none = try runToString(alloc, &tmp, "id\n1\n2\n3\n", "SELECT * FROM '$IN' LIMIT 5 OFFSET 100");
    defer alloc.free(none);
    try std.testing.expectEqualStrings("id\n", none);
}

test "join: an empty build side drops all rows (inner) and null-fills (left)" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,code\n1,A\n2,B\n" });
    try tmp.dir.writeFile(.{ .sub_path = "lookup.csv", .data = "code,label\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(alloc,
        "LOAD INTO '{s}/inner.csv' AS WITH labels AS (SELECT * FROM '{s}/lookup.csv') " ++
            "SELECT t.id, l.label FROM '{s}/in.csv' t JOIN labels l ON t.code = l.code;\n" ++
            "LOAD INTO '{s}/left.csv' AS WITH labels AS (SELECT * FROM '{s}/lookup.csv') " ++
            "SELECT t.id, l.label FROM '{s}/in.csv' t LEFT JOIN labels l ON t.code = l.code;",
        .{ base, base, base, base, base, base });
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    _ = run(alloc, prog, .{}, &rdiag) catch |e| {
        std.debug.print("run error: {s} ({s})\n", .{ @errorName(e), rdiag.msg });
        return e;
    };
    const inner = try tmp.dir.readFileAlloc(alloc, "inner.csv", 1 << 20);
    defer alloc.free(inner);
    try std.testing.expectEqualStrings("id,label\n", inner);
    const left = try tmp.dir.readFileAlloc(alloc, "left.csv", 1 << 20);
    defer alloc.free(left);
    try std.testing.expectEqualStrings("id,label\n1,\n2,\n", left);
}

test "join: duplicate build keys fan out (inner); semi/anti reduce to existence" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,code\n1,A\n2,Z\n" });
    try tmp.dir.writeFile(.{ .sub_path = "lookup.csv", .data = "code,label\nA,x1\nA,x2\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(alloc,
        "LOAD INTO '{s}/inner.csv' AS WITH labels AS (SELECT * FROM '{s}/lookup.csv') " ++
            "SELECT t.id, l.label FROM '{s}/in.csv' t JOIN labels l ON t.code = l.code;\n" ++
            "LOAD INTO '{s}/semi.csv' AS WITH labels AS (SELECT * FROM '{s}/lookup.csv') " ++
            "SELECT * FROM '{s}/in.csv' t SEMI JOIN labels l ON t.code = l.code;\n" ++
            "LOAD INTO '{s}/anti.csv' AS WITH labels AS (SELECT * FROM '{s}/lookup.csv') " ++
            "SELECT * FROM '{s}/in.csv' t ANTI JOIN labels l ON t.code = l.code;",
        .{ base, base, base, base, base, base, base, base, base });
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    _ = run(alloc, prog, .{}, &rdiag) catch |e| {
        std.debug.print("run error: {s} ({s})\n", .{ @errorName(e), rdiag.msg });
        return e;
    };
    const inner = try tmp.dir.readFileAlloc(alloc, "inner.csv", 1 << 20);
    defer alloc.free(inner);
    try std.testing.expectEqualStrings("id,label\n1,x1\n1,x2\n", inner);
    const semi = try tmp.dir.readFileAlloc(alloc, "semi.csv", 1 << 20);
    defer alloc.free(semi);
    try std.testing.expectEqualStrings("id,code\n1,A\n", semi);
    const anti = try tmp.dir.readFileAlloc(alloc, "anti.csv", 1 << 20);
    defer alloc.free(anti);
    try std.testing.expectEqualStrings("id,code\n2,Z\n", anti);
}

test "statement-level CASE dispatches on a resolved param (default arm otherwise)" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,v\n1,10\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(alloc,
        "PARAM mode STRING DEFAULT 'small';\nCASE $mode\n" ++
            "  WHEN 'big' THEN LOAD INTO '{s}/out.csv' AS SELECT id, v FROM '{s}/in.csv';\n" ++
            "  ELSE LOAD INTO '{s}/out.csv' AS SELECT id FROM '{s}/in.csv';\nEND CASE;",
        .{ base, base, base, base });
    defer alloc.free(script);

    const dflt = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(dflt);
    try std.testing.expectEqualStrings("id\n1\n", dflt);
    const big = try runScript(alloc, &tmp, script, &[_]ParamArg{.{ .key = "mode", .val = "big" }});
    defer alloc.free(big);
    try std.testing.expectEqualStrings("id,v\n1,10\n", big);
}

test "for-each on_error=continue with an OutcomeSink: run succeeds, failure recorded" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "names.csv", .data = "name\nalpha\nghost\n" });
    try tmp.dir.writeFile(.{ .sub_path = "alpha.csv", .data = "id\n7\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(alloc,
        "FOR EACH ROW OF ('{s}/names.csv') AS (name) SEQUENTIAL ON ERROR CONTINUE\n" ++
            "  LOAD INTO '{s}/out_${{name}}.csv' AS SELECT * FROM '{s}/${{name}}.csv';\nEND FOR;",
        .{ base, base, base });
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var oc_arena = std.heap.ArenaAllocator.init(alloc);
    defer oc_arena.deinit();
    var outcomes = OutcomeSink.init(oc_arena.allocator());
    defer outcomes.deinit();

    var rdiag: Diag = .{};
    const stats = try run(alloc, prog, .{ .outcomes = &outcomes }, &rdiag);
    try std.testing.expectEqual(@as(usize, 1), stats.rows_out);
    try std.testing.expectEqual(@as(usize, 2), outcomes.list.items.len);
    try std.testing.expectEqual(@as(usize, 1), outcomes.failures());
    for (outcomes.list.items) |o| {
        if (o.ok) {
            try std.testing.expectEqualStrings("alpha", o.item);
        } else {
            try std.testing.expectEqualStrings("ghost", o.item);
            try std.testing.expect(o.err.len > 0);
            try std.testing.expect(!o.retryable);
        }
    }
}

test "for-each with an empty discovery list is a no-op" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "names.csv", .data = "name\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(alloc,
        "FOR EACH ROW OF ('{s}/names.csv') AS (name)\n" ++
            "  LOAD INTO '{s}/out.csv' AS SELECT * FROM '{s}/${{name}}.csv';\nEND FOR;",
        .{ base, base, base });
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    const stats = try run(alloc, prog, .{}, &rdiag);
    try std.testing.expectEqual(@as(usize, 0), stats.rows_out);
    try std.testing.expectError(error.FileNotFound, tmp.dir.readFileAlloc(alloc, "out.csv", 1 << 20));
}

test "generated sources: SELECT with no FROM and RANGE(lo, hi)" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}/one.csv' AS SELECT 1 AS x, 'a' AS s;\n" ++
        "LOAD INTO '{s}/r.csv' AS SELECT range AS i FROM RANGE(2, 5) WHERE range != 3;\n", .{ base, base });
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    _ = try run(alloc, prog, .{}, &rdiag);

    const one = try tmp.dir.readFileAlloc(alloc, "one.csv", 1 << 20);
    defer alloc.free(one);
    try std.testing.expectEqualStrings("x,s\n1,a\n", one);
    const r = try tmp.dir.readFileAlloc(alloc, "r.csv", 1 << 20);
    defer alloc.free(r);
    try std.testing.expectEqualStrings("i\n2\n4\n", r);
}

/// Build the LET fixture script: a param, a LET over it, a LET over that LET, and
/// two pipelines that both reference them.
fn letScript(alloc: std.mem.Allocator, base: []const u8) ![]u8 {
    return std.fmt.allocPrint(alloc,
        \\PARAM days INT DEFAULT 3;
        \\LET span = $days * 10;
        \\LET label = concat('n', $span);
        \\LOAD INTO '{s}/a.csv' AS SELECT id, $span AS s, $label AS l FROM '{s}/in.csv';
        \\LOAD INTO '{s}/b.csv' AS SELECT $label AS l FROM '{s}/in.csv';
    , .{ base, base, base, base });
}

fn runLetScript(alloc: std.mem.Allocator, script: []const u8, cli: []const ParamArg, rdiag: *Diag) !void {
    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    _ = try run(alloc, prog, .{ .params = cli, .log = .{ .summary = .none, .quiet = true } }, rdiag);
}

test "LET folds once at plan time and hands every pipeline the same value" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n1\n2\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try letScript(alloc, base);
    defer alloc.free(script);
    var rdiag: Diag = .{};
    try runLetScript(alloc, script, &[_]ParamArg{}, &rdiag);

    const a = try tmp.dir.readFileAlloc(alloc, "a.csv", 1 << 20);
    defer alloc.free(a);
    try std.testing.expectEqualStrings("id,s,l\n1,30,n30\n2,30,n30\n", a);
    const b = try tmp.dir.readFileAlloc(alloc, "b.csv", 1 << 20);
    defer alloc.free(b);
    try std.testing.expectEqualStrings("l\nn30\nn30\n", b);
}

test "LET reads a param, so `-p` reaches it indirectly" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n1\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try letScript(alloc, base);
    defer alloc.free(script);
    var rdiag: Diag = .{};
    try runLetScript(alloc, script, &[_]ParamArg{.{ .key = "days", .val = "7" }}, &rdiag);

    const a = try tmp.dir.readFileAlloc(alloc, "a.csv", 1 << 20);
    defer alloc.free(a);
    try std.testing.expectEqualStrings("id,s,l\n1,70,n70\n", a);
}

test "a LET is sealed: `-p` naming one is a plan error, not a silent override" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n1\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try letScript(alloc, base);
    defer alloc.free(script);
    var rdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, runLetScript(alloc, script, &[_]ParamArg{.{ .key = "span", .val = "99" }}, &rdiag));
    try std.testing.expect(std.mem.indexOf(u8, rdiag.msg, "`span` is a LET, not a PARAM") != null);
    try std.testing.expectError(error.FileNotFound, tmp.dir.readFileAlloc(alloc, "a.csv", 1 << 20));
}

test "a LET and a PARAM may not share a name" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n1\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(alloc,
        \\PARAM span INT DEFAULT 1;
        \\LET span = 5;
        \\LOAD INTO '{s}/a.csv' AS SELECT id FROM '{s}/in.csv';
    , .{ base, base });
    defer alloc.free(script);
    var rdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, runLetScript(alloc, script, &[_]ParamArg{}, &rdiag));
    try std.testing.expect(std.mem.indexOf(u8, rdiag.msg, "declared twice") != null);
}

test "CALL renders a statement function per call (defaults fill the omitted arg)" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "alpha.csv", .data = "id,v\n1,10\n2,20\n" });
    try tmp.dir.writeFile(.{ .sub_path = "beta.csv", .data = "id,v\n3,30\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(
        alloc,
        "CREATE FUNCTION sync(name, tag DEFAULT 'Z') AS\n" ++
            "  LOAD INTO '{s}/out_${{name}}.csv' AS SELECT id, v, '${{tag}}' AS tag FROM '{s}/${{name}}.csv';\n" ++
            "END;\n" ++
            "CALL sync('alpha', 'A');\n" ++
            "CALL sync('beta');\n",
        .{ base, base },
    );
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    const stats = run(alloc, prog, .{}, &rdiag) catch |e| {
        std.debug.print("run error: {s} ({s})\n", .{ @errorName(e), rdiag.msg });
        return e;
    };
    try std.testing.expectEqual(@as(u64, 3), stats.rows_out);

    const a = try tmp.dir.readFileAlloc(alloc, "out_alpha.csv", 1 << 20);
    defer alloc.free(a);
    const b = try tmp.dir.readFileAlloc(alloc, "out_beta.csv", 1 << 20);
    defer alloc.free(b);
    try std.testing.expectEqualStrings("id,v,tag\n1,10,A\n2,20,A\n", a);
    try std.testing.expectEqualStrings("id,v,tag\n3,30,Z\n", b);
}

test "CALL nesting is depth-guarded (mutual recursion)" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id\n1\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(
        alloc,
        "CREATE FUNCTION ping(n) AS\n  CALL pong($n);\nEND;\n" ++
            "CREATE FUNCTION pong(n) AS\n  LOAD INTO '{s}/out.csv' AS SELECT id FROM '{s}/in.csv';\n  CALL ping($n);\nEND;\n" ++
            "CALL ping('x');\n",
        .{ base, base },
    );
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, run(alloc, prog, .{}, &rdiag));
    try std.testing.expect(std.mem.indexOf(u8, rdiag.msg, "too deep") != null);
}
