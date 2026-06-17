//! Build executable operator trees from a parsed @batch program and drive them.
//! Handles full program structure: `param`s (bound from the CLI or defaults),
//! `let` bindings (recompiled per reference), `ref` sources, and multiple output
//! pipelines. Params are substituted into expressions as literals before planning.

const std = @import("std");
const ast = @import("../lang/ast.zig");
const expand = @import("../lang/expand.zig");
const types = @import("../lang/types.zig");
const op = @import("../exec/op.zig");
const eval = @import("../exec/eval.zig");
const csv = @import("../connect/csv.zig");
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
const parallel = @import("parallel.zig");
const analyze = @import("analyze.zig");
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

// --- cooperative cancellation (SIGTERM/SIGINT from the control plane) ---
// The flag itself lives in connect/driver.zig so connectors can check it
// between network requests; these re-exports keep the public surface here.
pub const requestAbort = driver.requestAbort;
pub const aborting = driver.aborting;

// SIGHUP → reload a multi-script server's script directory (control plane writes
// updated scripts, then signals). Consumed (swapped back to false) by the server.
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
        error.HttpServerBusy, // http source: 429 / 5xx / declared retry_statuses
        // Socket failure classified AT THE SITE (http source, after its retry
        // budget): the source maps the ambient std.Io names to this specific
        // one only when the peer was a network socket. The ambient names
        // (WriteFailed/ReadFailed/EndOfStream) stay non-transient here — a CSV
        // sink's disk-full or a closed stdout pipe must exit 1, not retry.
        error.HttpTransportFailed,
        => true,
        else => false,
    };
}
pub const Stats = struct {
    rows_out: usize = 0, // rows written (kept name for the HTTP response)
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
/// TTY, NDJSON when piped.
pub const LogConfig = struct {
    format: obs.Format = .auto,
    level: obs.Level = .info,
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
};

const SqlKind = enum { postgres, mysql, sqlserver };

/// Captured when a SQL source is opened, so the planner can re-open the same
/// source per split (each split = the base query wrapped with a key-range WHERE).
const SqlDesc = struct {
    kind: SqlKind,
    dialect: sql.Dialect,
    cfg: DbConfig,
    base_sql: []const u8,
    table: ?[]const u8, // null for `query` reads (key must come from @[split])
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
    /// Set by `openSource` to the leading SQL source of the pipeline being built.
    sql_desc: ?SqlDesc = null,
    /// Connector types of the first source/sink, for the run summary.
    src_name: []const u8 = "",
    sink_name: []const u8 = "",
};

const PipeRes = struct { op: op.Op, schema: types.Schema };

/// Connection config carried into the split lanes (referenced via *anyopaque).
const SplitCtx = struct { gpa: std.mem.Allocator, kind: SqlKind, cfg: DbConfig, base_sql: []const u8 };

fn connectSql(gpa: std.mem.Allocator, kind: SqlKind, cfg: DbConfig) !sql.Conn {
    return switch (kind) {
        .postgres => (try postgres.Conn.connect(gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database, cfg.tls)).sqlConn(),
        .mysql => (try mysql.Conn.connect(gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database, cfg.tls)).sqlConn(),
        .sqlserver => (try tdsConnect(gpa, cfg)).sqlConn(),
    };
}

/// `parallel.OpenSplitFn`: open a fresh source for one split predicate.
fn openSplitSource(ctx_ptr: *anyopaque, gpa: std.mem.Allocator, pred: []const u8) anyerror!driver.Source {
    const ctx: *SplitCtx = @ptrCast(@alignCast(ctx_ptr));
    const q = try splitmod.wrap(gpa, ctx.base_sql, pred);
    defer gpa.free(q); // the cursor sends the query during open; we don't retain it
    const conn = try connectSql(gpa, ctx.kind, ctx.cfg);
    errdefer conn.close(); // queryCursor leaves conn open on error (the caller owns it)
    const s = try sql.Source.open(gpa, conn, q);
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
};

/// `parallel.OpenSinkFn`: one StarRocks stream-load stream per lane.
fn openLaneStarrocksSink(ctx_ptr: *anyopaque, gpa: std.mem.Allocator, lane_idx: usize) anyerror!driver.Sink {
    const spec: *StarrocksSinkSpec = @ptrCast(@alignCast(ctx_ptr));
    var cfg = spec.cfg;
    // Lane-distinct prefix → labels never collide across lanes. (StreamLoadSink.open
    // dupes this, so the temp is freed here.)
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
    lane_mode: ast.WriteMode, // overwrite -> append for lanes (DELETE already done)
    redial: sql.Redial, // INSERT-sink transient retry (plan-arena, lane-shared, read-only)
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
    if (mode != .upsert) return (try BulkSink.open(gpa, conn, target, schema, mode)).sink();
    return (try sql.Sink.open(gpa, conn.sqlConn(), dialect, target, schema, mode, redial)).sink();
}

/// `parallel.OpenSinkFn`: one DB stream per lane (append/overwrite → bulk loader,
/// upsert → INSERT, per `openBulkOrInsert`).
fn openLaneSqlSink(ctx_ptr: *anyopaque, gpa: std.mem.Allocator, lane_idx: usize) anyerror!driver.Sink {
    _ = lane_idx; // SQL sinks need no per-lane discriminator (INSERTs aren't labelled)
    const spec: *SqlSinkSpec = @ptrCast(@alignCast(ctx_ptr));
    const cfg = spec.cfg;
    switch (spec.kind) {
        .postgres => {
            const c = try postgres.Conn.connect(gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database, cfg.tls);
            errdefer c.close();
            return openBulkOrInsert(gpa, c, postgres.CopySink, spec.dialect, spec.target, spec.schema, spec.lane_mode, spec.redial);
        },
        .mysql => {
            const c = try mysql.Conn.connect(gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database, cfg.tls);
            errdefer c.close();
            return openBulkOrInsert(gpa, c, mysql.LoadDataSink, spec.dialect, spec.target, spec.schema, spec.lane_mode, spec.redial);
        },
        .sqlserver => {
            const c = try tdsConnect(gpa, cfg);
            errdefer c.close();
            return openBulkOrInsert(gpa, c, tds.BulkSink, spec.dialect, spec.target, spec.schema, spec.lane_mode, spec.redial);
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

    // One-time setup: create the table (and DELETE once for overwrite), so lanes
    // don't race DDL or repeatedly delete. Lanes then append into it.
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

    // One-time setup: create DB/table (and TRUNCATE for overwrite) once, so the
    // lanes don't race DDL or repeatedly truncate.
    const setup = starrocks.StreamLoadSink.open(env.gpa, cfg, w.target, schema, w.mode) catch |e|
        return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "starrocks setup failed ({s}) — {s}", .{ @errorName(e), env.diag.msg }));
    setup.logger = env.log;
    setup.sink().close() catch |e|
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "starrocks setup close failed: {s}", .{@errorName(e)}));
    cfg.auto_create = false;

    const spec = try env.arena.create(StarrocksSinkSpec);
    // Lanes must not re-truncate (overwrite's TRUNCATE already ran once in setup
    // above), so map overwrite -> append for the per-lane sinks — same as buildSqlSinkSpec.
    spec.* = .{ .cfg = cfg, .target = w.target, .schema = schema, .mode = if (w.mode == .overwrite) .append else w.mode, .logger = env.log };
    return spec;
}

pub fn run(gpa: std.mem.Allocator, raw_program: ast.Program, opts: RunOptions, diag: *Diag) !Stats {
    var plan_arena = std.heap.ArenaAllocator.init(gpa);
    defer plan_arena.deinit();
    const arena = plan_arena.allocator();

    // Expand user-defined `fn`s inline (and drop their declarations) up front, so
    // nothing downstream sees a user function.
    var expand_msg: []const u8 = "";
    const program = expand.expandProgram(arena, raw_program, opts.request_body, &expand_msg) catch |e| switch (e) {
        error.OutOfMemory => return e,
        error.ExpandFailed => return planErr(diag, expand_msg),
    };

    if (program.stmts.len == 0 or program.stmts[0] != .kind)
        return planErr(diag, "script must begin with a @kind tag");
    // @batch runs once; @http reuses this for each request (with a request body).
    var params = std.StringHashMap(Value).init(arena);
    try resolveParams(arena, program, opts.params, &params, diag);
    // Substitution map: param name → a literal expression of its resolved value.
    var params_expr = std.StringHashMap(*const ast.Expr).init(arena);
    var pit = params.iterator();
    while (pit.next()) |kv| try params_expr.put(kv.key_ptr.*, try mkLit(arena, kv.value_ptr.*));

    // JSON params for runtime navigation by for-each (`for x in tables`). Two
    // sources: the HTTP request body (@http — the whole body is the document), or a
    // `-p name=<json>` CLI flag (@batch — each json param gets its own value).
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
                try json_params.put(name, jv); // -p overrides the body if both given
            } else |_| return planErr(diag, try std.fmt.allocPrint(arena, "param `{s}`: value is not valid JSON", .{name}));
        }
    }

    var bindings = std.StringHashMap(ast.Pipeline).init(arena);
    var connections = std.StringHashMap(ast.Connection).init(arena);
    var runnable: usize = 0; // outputs + for-each blocks
    for (program.stmts[1..]) |s| switch (s) {
        .binding => |b| try bindings.put(b.name, b.pipeline),
        .connection => |c| try connections.put(c.name, c),
        .output, .for_each, .match => runnable += 1,
        .param, .kind, .func => {},
    };
    if (runnable == 0)
        return planErr(diag, "no output pipeline (a pipeline ending in `write`)");

    const run_id: u64 = @intCast(std.time.milliTimestamp());
    var logger = obs.Logger.init(run_id, opts.log.format, if (opts.log.quiet) .err else opts.log.level);
    const t0 = std.time.milliTimestamp();
    var rows_read = std.atomic.Value(u64).init(0);

    // On any error exit, surface the runtime expression-error context (set deep in
    // an operator) as the diagnostic the CLI prints.
    var errctx = op.ErrCtx{};
    errdefer if (errctx.msg.len > 0) setMsg(diag, errctx.msg);

    var sources = std.array_list.Managed(driver.Source).init(arena);
    var env = Env{ .arena = arena, .gpa = gpa, .params = &params, .bindings = &bindings, .connections = &connections, .sources = &sources, .request_body = opts.request_body, .diag = diag, .log = &logger, .params_expr = &params_expr, .errctx = &errctx, .rows_read = &rows_read, .json_params = &json_params };

    var batch_arena = std.heap.ArenaAllocator.init(gpa);
    defer batch_arena.deinit();

    var stats = Stats{ .run_id = run_id };
    var lanes_used: usize = 1; // actual parallelism (1 unless split-parallel engaged)
    // Execute outputs and for-each blocks in program order.
    for (program.stmts[1..]) |s| switch (s) {
        .output => |p| try runOutput(&env, p, opts, &stats, &lanes_used, &batch_arena),
        .for_each => |fe| try runForEach(&env, fe, opts, &stats, &lanes_used, &batch_arena),
        .match => |m| try runStmtMatch(&env, m, opts, &stats, &lanes_used, &batch_arena),
        else => {},
    };
    for (sources.items) |sc| sc.close();

    stats.rows_read = rows_read.load(.monotonic);
    stats.elapsed_ms = @intCast(std.time.milliTimestamp() - t0);
    stats.source = env.src_name;
    stats.sink = env.sink_name;

    // The run summary is a result, not a log line: `--json` emits it to stdout
    // (machine output); otherwise the logger renders it to stderr (human text on a
    // TTY, NDJSON when piped). The HTTP path skips both and builds its own response.
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
        .json_stdout => {
            var sbuf: [1024]u8 = undefined;
            var sfw = std.fs.File.stdout().writer(&sbuf);
            summary.renderJson(&sfw.interface) catch {};
            sfw.interface.flush() catch {};
        },
        .stderr => logger.summary(summary),
        .none => {},
    }
    return stats;
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
    const ns = names.items;
    const vs = values.items;

    var subj: ?Value = null;
    if (m.subject) |s| subj = eval.constEval(env.arena, s, ns, vs) catch |e|
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "match subject: {s}", .{@errorName(e)}));

    for (m.arms) |arm| {
        const hit = blk: {
            if (arm.is_default) break :blk true;
            if (arm.guard) |g| {
                const gv = eval.constEval(env.arena, g, ns, vs) catch |e|
                    return planErr(env.diag, try std.fmt.allocPrint(env.arena, "match guard: {s}", .{@errorName(e)}));
                break :blk (gv == .bool and gv.bool);
            }
            const sv = subj orelse break :blk false;
            for (arm.pats) |p| {
                const pv = eval.constEval(env.arena, p, ns, vs) catch |e|
                    return planErr(env.diag, try std.fmt.allocPrint(env.arena, "match pattern: {s}", .{@errorName(e)}));
                if (eval.compareValues(sv, pv)) |ord| if (ord == .eq) break :blk true;
            }
            break :blk false;
        };
        if (hit) {
            for (arm.body) |*st| try runStmt(env, st, opts, stats, lanes_used, batch_arena);
            return; // first matching arm wins
        }
    }
}

/// Execute one statement — used for match arm bodies. Registers declarations into
/// the env and runs output / for-each / nested match.
fn runStmt(env: *Env, s: *const ast.Stmt, opts: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!void {
    switch (s.*) {
        .output => |p| try runOutput(env, p, opts, stats, lanes_used, batch_arena),
        .for_each => |fe| try runForEach(env, fe, opts, stats, lanes_used, batch_arena),
        .match => |mm| try runStmtMatch(env, mm, opts, stats, lanes_used, batch_arena),
        .binding => |b| try env.bindings.put(b.name, b.pipeline),
        .connection => |c| try env.connections.put(c.name, c),
        .param, .kind, .func => {},
    }
}

/// Run one output pipeline (ending in `write`): build it, then either split it
/// into parallel key-range lanes or stream it serially into the sink.
fn runOutput(env: *Env, out: ast.Pipeline, opts: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) !void {
    const arena = env.arena;
    const gpa = env.gpa;
    const stages = out.stages;
    if (stages.len == 0) return planErr(env.diag, "empty pipeline");
    const last = stages[stages.len - 1].node;
    if (last != .write) return planErr(env.diag, "a top-level pipeline must end in `write`");
    env.sink_name = connectorType(env, last.write.connector);

    // Union split-per-branch: when threads>1 and the post-union stages are map-only,
    // expand into one `read branch | select(reconcile) | … | write` pipeline per
    // branch and run each through runOutput, which split-reads a single source — so a
    // big tenant table (e.g. CT2010) reads in key-range lanes instead of serially. A
    // breaker after the union (sort/aggregate/distinct) needs the whole union at once,
    // so those fall through to the serial op.Union below. CSV is excluded because each
    // branch opens the sink afresh and the CSV writer truncates — DB sinks (Stream
    // Load / bulk) accumulate across opens, so branches add to the same table.
    if (stages[0].node == .union_ and opts.threads > 1 and
        !std.mem.eql(u8, last.write.connector, "csv") and
        unionDownstreamMapOnly(stages[1 .. stages.len - 1]))
    {
        return runUnionSplit(env, stages[0].node.union_, stages[0].hints, stages[1 .. stages.len - 1], stages[stages.len - 1], opts, stats, lanes_used, batch_arena);
    }

    env.sql_desc = null;
    env.src_name = ""; // reset per output so this pipeline's first read sets it
    const src_base = env.sources.items.len; // sources this output opens (for early release on the split path)
    const res = try buildPipeline(env, stages[0 .. stages.len - 1]);

    // Resolve a bare `upsert` (inferred PK) now that the read is open and
    // env.sql_desc names the source table. Used by every sink path below.
    const wr = try resolveUpsertKeys(env, last.write);

    // Split-parallel: a map-only pipeline (no breakers/limit) reading a splittable
    // SQL source fans out into N key-range lanes, each on its own connection. A
    // StarRocks sink also fans out (one stream-load stream per lane); other sinks
    // (CSV) stay shared under a mutex. Non-splittable/stateful pipelines stay serial.
    if (opts.threads > 1 and env.sql_desc != null) {
        if (try op.linearize(arena, res.op)) |lin| {
            if (try planSplit(env, env.sql_desc.?, stages[0], opts.threads, wr)) |sp| {
                // The planning source was opened only to build res.op + discover the
                // split descriptor (which copies its own config); the lanes open their
                // own connections, so close it now rather than holding an idle
                // connection with an unconsumed result set for the whole parallel run.
                // res.schema's column names live in the source cursor's arena, so dupe
                // them into the run arena first — the sinks use the schema all run.
                const schema = try dupeSchema(arena, res.schema);
                for (env.sources.items[src_base..]) |sc| sc.close();
                env.sources.shrinkRetainingCapacity(src_base);
                var ctx = SplitCtx{ .gpa = gpa, .kind = env.sql_desc.?.kind, .cfg = env.sql_desc.?.cfg, .base_sql = env.sql_desc.?.base_sql };
                lanes_used.* = @max(lanes_used.*, @min(opts.threads, sp.predicates.len));
                env.log.log(.info, "split-parallel: {d} splits over {d} lanes on key range", .{ sp.predicates.len, @min(opts.threads, sp.predicates.len) });
                if (try buildParallelSink(env, wr, schema)) |mode| {
                    stats.rows_out += try parallel.run(gpa, sp.predicates, openSplitSource, &ctx, lin.stages, mode, opts.threads, env.rows_read);
                } else {
                    const snk = try openSink(env, wr, schema);
                    var snk_open = true;
                    errdefer if (snk_open) snk.abort(); // failed run: discard the tail buffer, don't commit it
                    stats.rows_out += try parallel.run(gpa, sp.predicates, openSplitSource, &ctx, lin.stages, .{ .shared = snk }, opts.threads, env.rows_read);
                    snk_open = false;
                    try snk.close();
                }
                return;
            }
        }
    }

    const snk = try openSink(env, wr, res.schema);
    // On any error before close, abort the sink: discard its tail buffer instead
    // of letting a later close commit partial data, and release its connection.
    // Once close() runs it owns teardown (success or failure), so the flag keeps
    // a failed close from double-freeing via abort.
    var snk_open = true;
    errdefer if (snk_open) snk.abort();
    while (true) {
        if (aborting()) return error.Aborted; // cancelled by the control plane
        _ = batch_arena.reset(.retain_capacity);
        const b = (try res.op.next(batch_arena.allocator())) orelse break;
        try snk.writeBatch(batch_arena.allocator(), b);
        stats.rows_out += b.len;
    }
    snk_open = false;
    try snk.close();
}

// --- for-each: plan-time fan-out over a discovered value list ---

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

/// Run the discovery source once and collect its first `ncols` columns as rows of
/// text (strings/ints; null → ""). The list is small — a table catalog — so it is
/// fully materialized into the plan arena.
fn discoverRows(env: *Env, src_read: ast.Read, ncols: usize) ![]const Row {
    const src = openSource(env, src_read, &.{}) catch |e|
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "for-each discovery failed: {s}", .{@errorName(e)}));
    defer src.close();
    var rows = std.array_list.Managed(Row).init(env.arena);
    var da = std.heap.ArenaAllocator.init(env.gpa);
    defer da.deinit();
    while (true) {
        _ = da.reset(.retain_capacity);
        const b = (try src.next(da.allocator())) orelse break;
        if (b.columns.len == 0) continue;
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
    return rows.toOwnedSlice();
}

/// Discover for-each rows from a JSON array param (`for a, b in job.tables`):
/// navigate to the array, then bind each loop variable to the like-named field of
/// each object element (coerced to text). Mirrors `discoverRows` for reads.
fn discoverRowsJson(env: *Env, path: ast.QualName, var_names: []const []const u8) ![]const Row {
    const head = path.parts[0];
    var cur = env.json_params.get(head) orelse
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "for-each: `{s}` is not a JSON param", .{head}));
    for (path.parts[1..]) |key| {
        cur = switch (cur) {
            .object => |o| o.get(key) orelse
                return planErr(env.diag, try std.fmt.allocPrint(env.arena, "for-each: json key `{s}` not found", .{key})),
            else => return planErr(env.diag, "for-each: json path is not an object"),
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

fn jsonToStr(arena: std.mem.Allocator, v: std.json.Value) ![]const u8 {
    return switch (v) {
        .null => "",
        .bool => |b| if (b) "true" else "false",
        .integer => |i| try std.fmt.allocPrint(arena, "{d}", .{i}),
        .float => |f| try std.fmt.allocPrint(arena, "{d}", .{f}),
        .number_string, .string => |s| s,
        // Serialize a nested field back to JSON text so it can flow through a `${var}`
        // (e.g. a per-table `source` array consumed by `union <conn> json "${source}"`).
        .array, .object => try std.json.Stringify.valueAlloc(arena, v, .{}),
    };
}

/// Replace every needle (`${var}`) in `s` with its row value (chained over all
/// loop variables). Returns `s` unchanged (no copy) when no needle is present.
/// Replace `${var}` / `${var:modifier}` occurrences in `s`. `names[i]` is a loop
/// variable bound to `values[i]`. Supported modifiers: `lower`, `upper`. An unknown
/// `${...}` (no matching var) is left verbatim. Strings without `${` are returned
/// as-is (no allocation).
fn interpAll(arena: std.mem.Allocator, s: []const u8, names: []const []const u8, values: Row) ![]const u8 {
    if (std.mem.indexOf(u8, s, "${") == null) return s;
    var out = std.array_list.Managed(u8).init(arena);
    errdefer out.deinit();
    var i: usize = 0;
    while (i < s.len) {
        if (s[i] == '$' and i + 1 < s.len and s[i + 1] == '{') {
            const close = std.mem.indexOfScalarPos(u8, s, i + 2, '}') orelse {
                try out.appendSlice(s[i..]); // unterminated `${` — emit literally
                break;
            };
            const inner = s[i + 2 .. close]; // `var` or `var:mod`
            const colon = std.mem.indexOfScalar(u8, inner, ':');
            const vname = if (colon) |c| inner[0..c] else inner;
            const mod: ?[]const u8 = if (colon) |c| inner[c + 1 ..] else null;
            var found = false;
            for (names, values) |nm, val| {
                if (std.mem.eql(u8, nm, vname)) {
                    try appendInterp(&out, val, mod);
                    found = true;
                    break;
                }
            }
            if (!found) try out.appendSlice(s[i .. close + 1]); // unknown var: leave verbatim
            i = close + 1;
        } else {
            try out.append(s[i]);
            i += 1;
        }
    }
    return out.toOwnedSlice();
}

fn appendInterp(out: *std.array_list.Managed(u8), val: []const u8, mod: ?[]const u8) !void {
    const m = mod orelse return out.appendSlice(val);
    if (std.mem.eql(u8, m, "lower")) {
        for (val) |c| try out.append(std.ascii.toLower(c));
    } else if (std.mem.eql(u8, m, "upper")) {
        for (val) |c| try out.append(std.ascii.toUpper(c));
    } else {
        try out.appendSlice(val); // unknown modifier: emit value unchanged
    }
}

fn renderQual(arena: std.mem.Allocator, q: ast.QualName, needles: []const []const u8, row: Row) !ast.QualName {
    const parts = try arena.alloc([]const u8, q.parts.len);
    for (q.parts, parts) |s, *dst| dst.* = try interpAll(arena, s, needles, row);
    return .{ .parts = parts };
}

fn renderRead(arena: std.mem.Allocator, rd: ast.Read, needles: []const []const u8, row: Row) !ast.Read {
    return .{ .connector = rd.connector, .form = switch (rd.form) {
        .table => |q| .{ .table = try renderQual(arena, q, needles, row) },
        .query => |s| .{ .query = try interpAll(arena, s, needles, row) },
        .path => |s| .{ .path = try interpAll(arena, s, needles, row) },
        .request => .request,
    }, .where = try interpAll(arena, rd.where, needles, row) };
}

/// Interpolate `${var}` into a union stage: the discovered form's discovery query,
/// or each explicit branch's read target + tag. Lets a for-loop drive which tables
/// a union reconciles (e.g. a per-table discovery query keyed by the loop value).
fn renderUnion(arena: std.mem.Allocator, u: ast.Union, needles: []const []const u8, row: Row) !ast.Union {
    if (u.branches.len > 0) {
        const branches = try arena.alloc(ast.UnionBranch, u.branches.len);
        for (u.branches, branches) |b, *o| o.* = .{
            .read = try renderRead(arena, b.read, needles, row),
            .tag = if (b.tag) |t| try interpAll(arena, t, needles, row) else null,
        };
        return .{ .branches = branches, .discover_conn = u.discover_conn, .discover_query = u.discover_query, .discover_json = u.discover_json, .pos = u.pos };
    }
    return .{
        .branches = u.branches,
        .discover_conn = u.discover_conn,
        .discover_query = try interpAll(arena, u.discover_query, needles, row),
        .discover_json = try interpAll(arena, u.discover_json, needles, row),
        .pos = u.pos,
    };
}

fn renderMode(arena: std.mem.Allocator, mode: ast.WriteMode, needles: []const []const u8, row: Row) !ast.WriteMode {
    switch (mode) {
        .upsert => |u| {
            const keys = try arena.alloc([]const u8, u.keys.len);
            for (u.keys, keys) |k, *dst| dst.* = try interpAll(arena, k, needles, row);
            var partial: ?[]const []const u8 = null;
            if (u.partial) |pc| {
                const out = try arena.alloc([]const u8, pc.len);
                for (pc, out) |c, *dst| dst.* = try interpAll(arena, c, needles, row);
                partial = out;
            }
            return .{ .upsert = .{ .keys = keys, .partial = partial } };
        },
        else => return mode,
    }
}

fn renderWrite(arena: std.mem.Allocator, w: ast.Write, needles: []const []const u8, row: Row) !ast.Write {
    return .{
        .connector = w.connector,
        .form = if (w.form) |f| try interpAll(arena, f, needles, row) else null,
        .target = try interpAll(arena, w.target, needles, row),
        .mode = try renderMode(arena, w.mode, needles, row),
    };
}

/// Instantiate the body template for one row: interpolate each `${var}` into the
/// read/write targets + upsert keys (v1 = targets only; expressions untouched).
fn renderHints(arena: std.mem.Allocator, hints: []const ast.Hint, needles: []const []const u8, row: Row) ![]const ast.Hint {
    if (hints.len == 0) return hints;
    const out = try arena.alloc(ast.Hint, hints.len);
    for (hints, out) |h, *o| {
        o.* = h;
        o.value = switch (h.value) {
            .str => |s| .{ .str = try interpAll(arena, s, needles, row) },
            .ident => |s| .{ .ident = try interpAll(arena, s, needles, row) },
            else => h.value,
        };
    }
    return out;
}

fn renderPipeline(env: *Env, body: ast.Pipeline, needles: []const []const u8, row: Row) !ast.Pipeline {
    const arena = env.arena;
    const stages = try arena.alloc(ast.Stage, body.stages.len);
    for (body.stages, stages) |src, *dst| {
        dst.* = src;
        dst.hints = try renderHints(arena, src.hints, needles, row); // e.g. @[split=${pk}]
        switch (src.node) {
            .read => |rd| dst.node = .{ .read = try renderRead(arena, rd, needles, row) },
            .union_ => |u| dst.node = .{ .union_ = try renderUnion(arena, u, needles, row) },
            .write => |w| dst.node = .{ .write = try renderWrite(arena, w, needles, row) },
            // Interpolate `${var}` inside expression string-literals too, so loop
            // values can drive computed columns / predicates, not just targets.
            .filter => |e| dst.node = .{ .filter = try renderExpr(arena, e, needles, row) },
            .select => |items| dst.node = .{ .select = try renderSelect(arena, items, needles, row) },
            .aggregate => |ag| {
                const aggs = try arena.alloc(ast.AggItem, ag.aggs.len);
                for (ag.aggs, 0..) |a, i| aggs[i] = .{ .name = a.name, .func = a.func, .arg = if (a.arg) |e| try renderExpr(arena, e, needles, row) else null };
                dst.node = .{ .aggregate = .{ .aggs = aggs, .by = ag.by } };
            },
            else => {},
        }
    }
    return .{ .stages = stages, .pos = body.pos };
}

/// Deep-copy an expression, interpolating `${var}` needles into every string
/// literal. Non-string leaves are reused as-is (identifiers can't hold needles).
fn renderExpr(arena: std.mem.Allocator, e: *const ast.Expr, needles: []const []const u8, row: Row) anyerror!*ast.Expr {
    return switch (e.*) {
        .str_lit => |s| try mk(arena, .{ .str_lit = try interpAll(arena, s, needles, row) }),
        .null_lit, .bool_lit, .int_lit, .float_lit, .field => @constCast(e),
        .unary => |u| try mk(arena, .{ .unary = .{ .op = u.op, .e = try renderExpr(arena, u.e, needles, row) } }),
        .binary => |b| try mk(arena, .{ .binary = .{ .op = b.op, .l = try renderExpr(arena, b.l, needles, row), .r = try renderExpr(arena, b.r, needles, row) } }),
        .cond => |c| try mk(arena, .{ .cond = .{ .cond = try renderExpr(arena, c.cond, needles, row), .then = try renderExpr(arena, c.then, needles, row), .els = try renderExpr(arena, c.els, needles, row) } }),
        .cast => |c| try mk(arena, .{ .cast = .{ .e = try renderExpr(arena, c.e, needles, row), .ty = c.ty } }),
        .is_null => |n| try mk(arena, .{ .is_null = .{ .e = try renderExpr(arena, n.e, needles, row), .negated = n.negated } }),
        .call => |c| blk: {
            const args = try arena.alloc(*ast.Expr, c.args.len);
            for (c.args, 0..) |a, i| args[i] = try renderExpr(arena, a, needles, row);
            break :blk try mk(arena, .{ .call = .{ .name = c.name, .args = args } });
        },
        .match => |m| blk: {
            const subject = if (m.subject) |s| try renderExpr(arena, s, needles, row) else null;
            const arms = try arena.alloc(ast.MatchArm, m.arms.len);
            for (m.arms, 0..) |arm, i| {
                const pats = try arena.alloc(*ast.Expr, arm.pats.len);
                for (arm.pats, 0..) |p, j| pats[j] = try renderExpr(arena, p, needles, row);
                arms[i] = .{ .pats = pats, .guard = if (arm.guard) |g| try renderExpr(arena, g, needles, row) else null, .value = try renderExpr(arena, arm.value, needles, row), .is_default = arm.is_default };
            }
            break :blk try mk(arena, .{ .match = .{ .subject = subject, .arms = arms } });
        },
    };
}

fn renderSelect(arena: std.mem.Allocator, items: []const ast.SelectItem, needles: []const []const u8, row: Row) ![]const ast.SelectItem {
    // Computed items interpolate both the expression and the alias name. A bare
    // ident alias has no `${var}` so interpAll is a no-op; a quoted-string alias
    // (`"${name}_EMPRESA" = emp`) is where the loop value builds the column name.
    // Bare field/except identifiers are NOT templated.
    const out = try arena.alloc(ast.SelectItem, items.len);
    for (items, 0..) |it, i| out[i] = switch (it) {
        .computed => |c| .{ .computed = .{
            .name = try interpAll(arena, c.name, needles, row),
            .expr = try renderExpr(arena, c.expr, needles, row),
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
    stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    mu: std.Thread.Mutex = .{},
    first_err_buf: [256]u8 = undefined,
    first_err_len: usize = 0,
    first_retryable: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
};

fn forRecordFail(ctx: *ForCtx, label: []const u8, ename: []const u8, msg: []const u8, retryable: bool) void {
    _ = ctx.failures.fetchAdd(1, .monotonic);
    if (ctx.outcomes) |sink| sink.record(label, false, if (msg.len > 0) msg else ename, retryable);
    ctx.mu.lock();
    defer ctx.mu.unlock();
    if (ctx.first_err_len == 0) {
        const s = std.fmt.bufPrint(&ctx.first_err_buf, "{s}: {s} {s}", .{ label, ename, msg }) catch ctx.first_err_buf[0..0];
        ctx.first_err_len = s.len;
        ctx.first_retryable.store(retryable, .monotonic);
    }
    if (ctx.on_error == .stop) ctx.stop.store(true, .release);
}

fn forWorker(ctx: *ForCtx) void {
    const gpa = ctx.base.gpa;
    while (true) {
        if (aborting()) break; // cancelled — workers stop pulling new items
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
        };
        var st = Stats{ .run_id = 0 };
        var lanes: usize = 1;
        const pipe = renderPipeline(&w_env, ctx.fe.body, ctx.needles, row) catch |e| {
            forRecordFail(ctx, row[0], @errorName(e), "", isTransient(e));
            continue;
        };
        if (runOutput(&w_env, pipe, ctx.worker_opts, &st, &lanes, &w_batch)) |_| {
            _ = ctx.rows_out.fetchAdd(st.rows_out, .monotonic);
            if (ctx.outcomes) |sink| sink.record(row[0], true, "", false);
        } else |e| {
            if (e == error.Aborted) {
                // Cancellation, not an item failure — the join path raises it.
                for (w_sources.items) |sc| sc.close();
                break;
            }
            forRecordFail(ctx, row[0], @errorName(e), w_diag.msg, isTransient(e) or w_diag.retryable);
        }
        for (w_sources.items) |sc| sc.close();
    }
}

/// Expand a `for <vars> in <source>` block into one pipeline per discovered row.
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
    };
    env.log.log(.info, "for-each {s}: {d} row(s) [{s}, on_error={s}]", .{ fe.var_names[0], rows.len, @tagName(mode), if (on_error == .continue_) "continue" else "stop" });
    if (rows.len == 0) return;
    // `interpAll` scans for `${var}` / `${var:mod}` itself, so it takes the raw
    // variable names (not pre-formatted `${var}` needles) paired with row values.
    const needles = fe.var_names;

    switch (mode) {
        .sequential => {
            var failures: usize = 0;
            var first_err: ?[]const u8 = null;
            for (rows) |row| {
                if (aborting()) return error.Aborted; // stop starting new items
                const base = env.sources.items.len;
                env.diag.retryable = false; // classify this item's failure freshly
                const pipe = try renderPipeline(env, fe.body, needles, row);
                if (runOutput(env, pipe, opts, stats, lanes_used, batch_arena)) |_| {
                    for (env.sources.items[base..]) |sc| sc.close();
                    env.sources.shrinkRetainingCapacity(base);
                    if (opts.outcomes) |sink| sink.record(row[0], true, "", false);
                } else |e| {
                    for (env.sources.items[base..]) |sc| sc.close();
                    env.sources.shrinkRetainingCapacity(base);
                    // Cancellation is not an item failure: surface it as the
                    // abort it is (exit 130), not a failed-items run (exit 1).
                    if (e == error.Aborted) return error.Aborted;
                    failures += 1;
                    const emsg = if (env.diag.msg.len > 0) env.diag.msg else @errorName(e);
                    if (opts.outcomes) |sink| sink.record(row[0], false, emsg, isTransient(e) or env.diag.retryable);
                    env.log.log(.err, "for-each {s} failed: {s}", .{ row[0], @errorName(e) });
                    if (first_err == null)
                        first_err = std.fmt.allocPrint(env.arena, "{s}: {s}", .{ row[0], @errorName(e) }) catch null;
                    if (on_error == .stop) {
                        // Preserve the transient/permanent split through the
                        // wrap, or a textbook-transient timeout exits 1.
                        if (isTransient(e)) env.diag.retryable = true;
                        return planErr(env.diag, first_err orelse "for-each failed");
                    }
                }
            }
            // Continue-mode partial failures surface via the sink (the run succeeds).
            // Without a sink (embedded/test callers), preserve the legacy run failure.
            if (failures > 0 and opts.outcomes == null)
                return planErr(env.diag, try std.fmt.allocPrint(env.arena, "for-each: {d}/{d} failed (first: {s})", .{ failures, rows.len, first_err orelse "?" }));
        },
        .parallel => {
            var wopts = opts;
            wopts.threads = 1; // each table runs serially; the for-loop provides the parallelism
            const nworkers = @min(@max(opts.threads, @as(usize, 1)), rows.len);
            var ctx = ForCtx{ .fe = fe, .needles = needles, .rows = rows, .base = env, .worker_opts = wopts, .on_error = on_error, .outcomes = opts.outcomes };
            const threads = try env.arena.alloc(std.Thread, nworkers);
            var spawned: usize = 0;
            while (spawned < nworkers) : (spawned += 1) {
                threads[spawned] = std.Thread.spawn(.{}, forWorker, .{&ctx}) catch break;
            }
            if (spawned == 0) forWorker(&ctx) else for (threads[0..spawned]) |t| t.join();
            // Cancellation outranks failure accounting: a SIGTERM mid-run is an
            // abort (exit 130), not "N items failed" (exit 1).
            if (aborting()) return error.Aborted;
            stats.rows_out += ctx.rows_out.load(.monotonic);
            lanes_used.* = @max(lanes_used.*, @max(spawned, @as(usize, 1)));
            const fails = ctx.failures.load(.monotonic);
            // stop-mode is a whole-request failure; continue-mode with a sink is a
            // partial success (failures reported via outcomes, the run succeeds).
            if (fails > 0 and (on_error == .stop or opts.outcomes == null)) {
                // Preserve the transient/permanent split through the wrap.
                if (ctx.first_retryable.load(.monotonic)) env.diag.retryable = true;
                return planErr(env.diag, try std.fmt.allocPrint(env.arena, "for-each: {d}/{d} failed (first: {s})", .{ fails, rows.len, ctx.first_err_buf[0..ctx.first_err_len] }));
            }
        },
    }
}

/// Resolve a sink/source connector name to its driver type for the summary
/// (`csv`/`request` are types; a connection name maps to its `connector`).
fn connectorType(env: *Env, name: []const u8) []const u8 {
    if (std.mem.eql(u8, name, "csv") or std.mem.eql(u8, name, "request") or std.mem.eql(u8, name, "http")) return name;
    if (env.connections.get(name)) |c| return c.connector;
    return name;
}

// --- pipeline construction ---

fn buildPipeline(env: *Env, stages: []const ast.Stage) anyerror!PipeRes {
    if (stages.len == 0) return planErr(env.diag, "empty pipeline");

    var current: op.Op = undefined;
    var schema: types.Schema = undefined;

    switch (stages[0].node) {
        .read => |rd| {
            const raw = try openSource(env, rd, stages[0].hints);
            // Count rows read through every source without per-operator wiring.
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

    for (stages[1..]) |stage| {
        const r = try buildStage(env, stage, current, schema);
        current = r.op;
        schema = r.schema;
    }
    return .{ .op = current, .schema = schema };
}

// --- union: reconcile N tables to a canon schema, then concatenate ---

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
    // `@[where = "..."]` pushes a raw source-dialect predicate into every branch's
    // SQL (incremental extraction: only changed rows cross the wire). The hint
    // value was already `${var}`-interpolated by renderHints when inside a for-loop.
    const where = forHintName(hints, "where") orelse "";
    if (u.discover_json.len > 0) {
        // Which JSON keys hold the table / tag, and an optional substring rule to
        // derive the tag from the table name — all configurable via the stage hints
        // (`@[table_field=.., tag_field=.., tag_substr="start,len"]`). Defaults below.
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
                // A bare string element is just the table name (tag derived/absent).
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
            // No explicit tag → derive it from the table name via `tag_substr`.
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

    // Probe each branch's schema (open, read COLMETADATA, close without draining);
    // the canon must be known before any branch runs.
    const schemas = try arena.alloc(types.Schema, specs.len);
    for (specs, schemas) |s, *sch| {
        const src = try openSource(env, s.read, hints);
        sch.* = try dupeSchema(arena, src.schema());
        src.close();
    }
    const canon = try unionCanon(env, specs, schemas, canon_opt);

    const w = write_stage.node.write;
    for (specs, schemas, 0..) |s, sch, i| {
        const items = try synthReconcile(arena, sch, canon, tag_col, s.tag);
        var bstages = std.array_list.Managed(ast.Stage).init(arena);
        try bstages.append(.{ .node = .{ .read = s.read }, .hints = &.{}, .pos = u.pos });
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
            o.* = .{ .child = child, .in_schema = try schemaPtr(arena, schema), .keys = keys, .state = arena };
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
    for (ap.aggs, aggs) |ra, *a| a.* = .{ .func = ra.func, .arg = ra.arg, .ty = ra.ty };
    const out = try schemaPtr(arena, ap.schema);
    const o = try arena.create(op.Aggregate);
    o.* = .{ .child = child, .in_schema = try schemaPtr(arena, schema), .by = ap.by, .aggs = aggs, .out_schema = out, .err = env.errctx, .state = arena };
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
        // Build-side batch + hash index survive across pulls; the per-batch arena
        // is reset before every pull, so they live in the plan arena instead.
        .state = arena,
    };
    return .{ .op = .{ .join = o }, .schema = out.* };
}

fn openSource(env: *Env, rd: ast.Read, hints: []const ast.Hint) !driver.Source {
    if (std.mem.eql(u8, rd.connector, "request")) {
        const body = env.request_body orelse
            return planErr(env.diag, "`read request` is only available when serving HTTP (@http)");
        const s = request.RequestSource.open(env.gpa, body) catch |e|
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "could not parse request body as JSON: {s}", .{@errorName(e)}));
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
    if (std.mem.eql(u8, rd.connector, "csv")) {
        if (rd.form != .path) return planErr(env.diag, "read csv needs a quoted path");
        const reader = csv.CsvReader.open(env.arena, rd.form.path) catch |e|
            return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not open input CSV `{s}` ({s})", .{ rd.form.path, @errorName(e) }));
        return reader.source();
    }
    const conn = env.connections.get(rd.connector) orelse
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "unknown connection `{s}`", .{rd.connector}));
    // A read stage may carry a `@[where = "..."]` predicate (pushed into the SQL,
    // already `${var}`-interpolated by renderHints) — same hint the union form
    // uses, now honored on a plain `read <conn> table/query` too.
    var rd_eff = rd;
    if (forHintName(hints, "where")) |wh| {
        if (wh.len > 0) rd_eff.where = wh;
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
    if (std.mem.eql(u8, conn.connector, "sqlserver")) {
        const cfg = try resolveDbConfig(env, conn, 1433);
        const query = try readSql(env, rd_eff);
        const c = tdsConnect(env.gpa, cfg) catch |e|
            return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "sqlserver connect failed: {s}", .{@errorName(e)}));
        const s = sql.Source.open(env.gpa, c.sqlConn(), query) catch |e| {
            defer c.close();
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "sqlserver read failed ({s}): {s}", .{ @errorName(e), c.last_error }));
        };
        env.sql_desc = try sqlDescFor(env, .sqlserver, .sqlserver, cfg, query, rd_eff);
        return s.source();
    }
    if (std.mem.eql(u8, conn.connector, "mysql")) {
        const cfg = try resolveDbConfig(env, conn, 3306);
        const query = try readSql(env, rd_eff);
        const c = mysql.Conn.connect(env.gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database, cfg.tls) catch |e|
            return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "mysql connect failed: {s}", .{@errorName(e)}));
        const s = sql.Source.open(env.gpa, c.sqlConn(), query) catch |e| {
            defer c.close();
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "mysql read failed ({s}): {s}", .{ @errorName(e), c.last_error }));
        };
        env.sql_desc = try sqlDescFor(env, .mysql, .mysql, cfg, query, rd_eff);
        return s.source();
    }
    if (std.mem.eql(u8, conn.connector, "postgres")) {
        const cfg = try resolveDbConfig(env, conn, 5432);
        const query = try readSql(env, rd_eff);
        const c = postgres.Conn.connect(env.gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database, cfg.tls) catch |e|
            return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "postgres connect failed: {s}", .{@errorName(e)}));
        const s = sql.Source.open(env.gpa, c.sqlConn(), query) catch |e| {
            defer c.close();
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "postgres read failed ({s}): {s}", .{ @errorName(e), c.last_error }));
        };
        env.sql_desc = try sqlDescFor(env, .postgres, .postgres, cfg, query, rd_eff);
        return s.source();
    }
    return planErr(env.diag, try std.fmt.allocPrint(env.arena, "unsupported source connector `{s}`", .{conn.connector}));
}

const DbConfig = struct {
    host: []const u8 = "",
    port: u16,
    user: []const u8 = "",
    password: []const u8 = "",
    database: []const u8 = "",
    tls: sql.TlsMode = .off,
    // Azure AD (sqlserver only): when set, `user`/`password` are the AAD
    // username/password (ROPC grant) and a federated token is sent instead of a
    // SQL login. TLS is forced on. `resource` defaults to https://<host>.
    aad: bool = false,
    tenant: []const u8 = "",
    client_id: []const u8 = "",
    resource: []const u8 = "",
    token: []const u8 = "", // pre-fetched AAD access token (skips ROPC) — for
    // federated tenants where the token is obtained out-of-band (az / MSAL / C#).
};

/// Open a SQL Server connection, using Azure AD (ROPC token -> FEDAUTH) when the
/// connection declared `auth = aad`, else a normal SQL login.
fn tdsConnect(gpa: std.mem.Allocator, cfg: DbConfig) !*tds.Conn {
    if (!cfg.aad) return tds.Conn.connect(gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database, cfg.tls);
    // Defaults mirror Microsoft.Data.SqlClient's "Active Directory Password":
    // the built-in ADO.NET first-party client (pre-consented in every tenant, so
    // no app registration), tenant discovery via "organizations", and the Azure
    // SQL resource — the Dataverse TDS endpoint accepts database.windows.net tokens.
    const mode: sql.TlsMode = if (cfg.tls == .off) .require else cfg.tls; // AAD mandates TLS
    // A pre-fetched token wins (federated tenants: get it via az/MSAL out-of-band).
    if (cfg.token.len > 0) return tds.Conn.connectAad(gpa, cfg.host, cfg.port, cfg.token, cfg.database, mode);
    // Otherwise ROPC, defaulting to SqlClient's "Active Directory Password" values.
    // Username/password: auto-detect managed (ROPC) vs federated/ADFS (WS-Trust),
    // mirroring Microsoft.Data.SqlClient "Active Directory Password" — no app reg.
    const client_id = if (cfg.client_id.len > 0) cfg.client_id else aad.ado_client_id;
    // Dataverse (*.dynamics.com) wants a token for the org URL; Azure SQL wants
    // database.windows.net. Both overridable via `resource`.
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

fn resolveDbConfig(env: *Env, conn: ast.Connection, default_port: u16) !DbConfig {
    var cfg = DbConfig{ .port = default_port };
    for (conn.config) |attr| {
        const k = attr.key;
        if (std.mem.eql(u8, k, "host")) {
            cfg.host = try evalCfgStr(env, attr.value);
        } else if (std.mem.eql(u8, k, "port")) {
            cfg.port = @intCast(try evalCfgInt(env, attr.value));
        } else if (std.mem.eql(u8, k, "user")) {
            cfg.user = try evalCfgStr(env, attr.value);
        } else if (std.mem.eql(u8, k, "password")) {
            cfg.password = try evalCfgStr(env, attr.value);
        } else if (std.mem.eql(u8, k, "database")) {
            cfg.database = try evalCfgStr(env, attr.value);
        } else if (std.mem.eql(u8, k, "tls")) {
            const v = try evalCfgStr(env, attr.value);
            cfg.tls = std.meta.stringToEnum(sql.TlsMode, v) orelse
                return planErr(env.diag, "connection `tls` must be \"off\", \"require\" or \"insecure\"");
        } else if (std.mem.eql(u8, k, "auth")) {
            cfg.aad = std.mem.eql(u8, try evalCfgStr(env, attr.value), "aad");
        } else if (std.mem.eql(u8, k, "tenant")) {
            cfg.tenant = try evalCfgStr(env, attr.value);
        } else if (std.mem.eql(u8, k, "client_id")) {
            cfg.client_id = try evalCfgStr(env, attr.value);
        } else if (std.mem.eql(u8, k, "resource")) {
            cfg.resource = try evalCfgStr(env, attr.value);
        } else if (std.mem.eql(u8, k, "token")) {
            cfg.token = try evalCfgStr(env, attr.value);
        }
    }
    if (cfg.host.len == 0) return planErr(env.diag, "connection needs a `host`");
    // tenant/client_id are optional for aad — they default to SqlClient's
    // built-in "Active Directory Password" values (see tdsConnect).
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
    // Default to a few splits per worker so faster lanes can steal from slower
    // ones (skew tolerance); cap to keep connection churn bounded.
    const forced = hints.col != null or hints.count != null;
    const m: usize = hints.count orelse @min(@as(usize, 64), threads * 4);
    if (m < 2) return null;
    // Sink-aware gate: Postgres COPY (append/overwrite) is faster serial than split
    // at the sizes measured — the per-lane connection + COPY-stream overhead exceeds
    // the benefit — so don't auto-split it. StarRocks (benefits from splitting),
    // mysql LOAD DATA, sqlserver BULK, and INSERT/upsert keep the size-gated default;
    // an explicit @[split]/@[splits] still forces a split.
    if (!forced and isPostgresCopySink(env, w)) return null;

    // Each probe opens its own connection (a cursor owns+closes its conn), so the
    // prober hands out fresh connections rather than sharing one.
    var pctx = SplitCtx{ .gpa = env.gpa, .kind = desc.kind, .cfg = desc.cfg, .base_sql = desc.base_sql };
    const prober = splitmod.Prober{ .ctx = &pctx, .openFn = proberOpen };

    var key: splitmod.Key = undefined;
    if (hints.col) |col| {
        key = .{ .col = col, .kind = hints.kind orelse .int }; // explicit key: int unless @[split_kind] says otherwise
    } else if (desc.table) |table| {
        const info = (try splitmod.introspectKey(env.arena, prober, desc.dialect, table)) orelse return null;
        // Size gate: small tables aren't worth the per-lane connection setup.
        if (!forced and info.est_rows < splitmod.min_rows_to_split) return null;
        key = info.key;
    } else {
        return null; // a query read with no @[split] hint
    }
    return splitmod.plan(env.arena, prober, desc.dialect, desc.base_sql, key, m);
}

fn proberOpen(ctx_ptr: *anyopaque) anyerror!sql.Conn {
    const ctx: *SplitCtx = @ptrCast(@alignCast(ctx_ptr));
    return connectSql(ctx.gpa, ctx.kind, ctx.cfg);
}

// --- `check --connect`: resolve a DB source's schema by connecting ---

/// An `analyze.Resolver` that connects to DB sources and reads their result-set
/// schema (CSV is handled offline by the analyzer). `ctx` is a `*std.mem.Allocator`.
pub fn connectingResolver(gpa_ptr: *std.mem.Allocator) analyze.Resolver {
    return .{ .ctx = gpa_ptr, .resolveFn = resolveSchema, .splitFn = probeSplit };
}

const SqlConnInfo = struct { kind: SqlKind, dialect: sql.Dialect, port: u16 };

fn sqlConnInfo(conn: ast.Connection) ?SqlConnInfo {
    if (std.mem.eql(u8, conn.connector, "postgres")) return .{ .kind = .postgres, .dialect = .postgres, .port = 5432 };
    if (std.mem.eql(u8, conn.connector, "mysql")) return .{ .kind = .mysql, .dialect = .mysql, .port = 3306 };
    if (std.mem.eql(u8, conn.connector, "sqlserver")) return .{ .kind = .sqlserver, .dialect = .sqlserver, .port = 1433 };
    return null;
}

fn resolveSchema(ctx_ptr: *anyopaque, arena: std.mem.Allocator, rd: ast.Read, conn_opt: ?ast.Connection) anyerror!?types.Schema {
    const gpa = @as(*std.mem.Allocator, @ptrCast(@alignCast(ctx_ptr))).*;
    // A URL CSV is a network source: its schema is only resolvable when the user
    // opted into connecting (this resolver IS the --connect path). Fetch the
    // header, then drop the connection.
    if (std.mem.eql(u8, rd.connector, "csv") and rd.form == .path and csv.CsvReader.isUrl(rd.form.path)) {
        const r = csv.CsvReader.open(arena, rd.form.path) catch return null;
        defer r.close();
        return r.schema;
    }
    // Same for `read http` — default options only (the resolver has no stage
    // hints), so auth-gated APIs come back unresolved rather than failing check.
    if (std.mem.eql(u8, rd.connector, "http") and rd.form == .path) {
        const r = httpsrc.HttpSource.open(arena, gpa, rd.form.path, .{}) catch return null;
        defer r.close();
        return r.schema.*;
    }
    const conn = conn_opt orelse return null;
    const info = sqlConnInfo(conn) orelse return null;
    const cfg = dbConfigOf(arena, conn, info.port) orelse return null;
    const query = switch (rd.form) {
        .query => |q| q,
        .table => |t| try std.fmt.allocPrint(arena, "SELECT * FROM {s}", .{try qualStr(arena, t)}),
        else => return null,
    };
    const c = connectSql(gpa, info.kind, cfg) catch return null;
    var cur = c.queryCursor(query) catch {
        c.close();
        return null;
    };
    defer cur.close();
    return try dupeSchema(arena, cur.schema());
}

/// `analyze.Resolver.splitFn`: introspect a table's PK + estimated size to report
/// the real split decision (mirrors the runtime planner's gate).
fn probeSplit(ctx_ptr: *anyopaque, arena: std.mem.Allocator, rd: ast.Read, conn_opt: ?ast.Connection) anyerror!?analyze.SplitProbe {
    const gpa = @as(*std.mem.Allocator, @ptrCast(@alignCast(ctx_ptr))).*;
    const conn = conn_opt orelse return null;
    const table = switch (rd.form) {
        .table => |t| try qualStr(arena, t),
        else => return null, // query reads declare the key via @[split]; nothing to introspect
    };
    const info = sqlConnInfo(conn) orelse return null;
    const cfg = dbConfigOf(arena, conn, info.port) orelse return null;
    var pctx = SplitCtx{ .gpa = gpa, .kind = info.kind, .cfg = cfg, .base_sql = "" };
    const prober = splitmod.Prober{ .ctx = &pctx, .openFn = proberOpen };
    const key = (try splitmod.introspectKey(arena, prober, info.dialect, table)) orelse
        return analyze.SplitProbe{ .key = "", .est_rows = 0, .will_split = false };
    return .{ .key = key.key.col, .est_rows = key.est_rows, .will_split = key.est_rows >= splitmod.min_rows_to_split };
}

/// Resolve a connection's host/port/user/password/database (literals + env()/
/// secret()), independent of the run `Env`. Returns null if `host` is missing.
fn dbConfigOf(arena: std.mem.Allocator, conn: ast.Connection, default_port: u16) ?DbConfig {
    var cfg = DbConfig{ .port = default_port };
    for (conn.config) |attr| {
        const v = cfgStr(arena, attr.value) orelse continue;
        if (std.mem.eql(u8, attr.key, "host")) {
            cfg.host = v;
        } else if (std.mem.eql(u8, attr.key, "port")) {
            cfg.port = std.fmt.parseInt(u16, v, 10) catch default_port;
        } else if (std.mem.eql(u8, attr.key, "user")) {
            cfg.user = v;
        } else if (std.mem.eql(u8, attr.key, "password")) {
            cfg.password = v;
        } else if (std.mem.eql(u8, attr.key, "database")) {
            cfg.database = v;
        } else if (std.mem.eql(u8, attr.key, "tls")) {
            cfg.tls = std.meta.stringToEnum(sql.TlsMode, v) orelse .off;
        } else if (std.mem.eql(u8, attr.key, "auth")) {
            cfg.aad = std.mem.eql(u8, v, "aad");
        } else if (std.mem.eql(u8, attr.key, "tenant")) {
            cfg.tenant = v;
        } else if (std.mem.eql(u8, attr.key, "client_id")) {
            cfg.client_id = v;
        } else if (std.mem.eql(u8, attr.key, "resource")) {
            cfg.resource = v;
        } else if (std.mem.eql(u8, attr.key, "token")) {
            cfg.token = v;
        }
    }
    if (cfg.host.len == 0) return null;
    return cfg;
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
    env.log.log(.info, "upsert: inferred key on {s}: {s}", .{ table, try std.mem.join(env.arena, ", ", keys) });
    var out = w;
    out.mode = .{ .upsert = .{ .keys = keys, .partial = w.mode.upsert.partial } };
    return out;
}

fn openSink(env: *Env, w: ast.Write, schema: types.Schema) !driver.Sink {
    if (std.mem.eql(u8, w.connector, "stdout")) {
        const writer = tablemod.TableWriter.open(env.gpa, schema) catch
            return planErr(env.diag, "could not open stdout table");
        return writer.sink();
    }
    if (std.mem.eql(u8, w.connector, "csv")) {
        const writer = csv.CsvWriter.open(env.arena, w.target, schema) catch
            return planErr(env.diag, "could not open output CSV");
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
    if (std.mem.eql(u8, conn.connector, "mysql")) {
        const cfg = try resolveDbConfig(env, conn, 3306);
        const c = mysql.Conn.connect(env.gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database, cfg.tls) catch |e|
            return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "mysql connect failed: {s}", .{@errorName(e)}));
        // append/overwrite → LOAD DATA LOCAL INFILE (bulk); upsert → INSERT.
        return openBulkOrInsert(env.gpa, c, mysql.LoadDataSink, .mysql, w.target, schema, w.mode, try redialFor(env.arena, .mysql, cfg)) catch |e| {
            defer c.close();
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "mysql sink failed ({s}): {s}", .{ @errorName(e), c.last_error }));
        };
    }
    if (std.mem.eql(u8, conn.connector, "sqlserver")) {
        const cfg = try resolveDbConfig(env, conn, 1433);
        const c = tdsConnect(env.gpa, cfg) catch |e|
            return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "sqlserver connect failed: {s}", .{@errorName(e)}));
        // append/overwrite → INSERT BULK; upsert → INSERT.
        return openBulkOrInsert(env.gpa, c, tds.BulkSink, .sqlserver, w.target, schema, w.mode, try redialFor(env.arena, .sqlserver, cfg)) catch |e| {
            defer c.close();
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "sqlserver sink failed ({s}): {s}", .{ @errorName(e), c.last_error }));
        };
    }
    if (std.mem.eql(u8, conn.connector, "postgres")) {
        const cfg = try resolveDbConfig(env, conn, 5432);
        const c = postgres.Conn.connect(env.gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database, cfg.tls) catch |e|
            return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "postgres connect failed: {s}", .{@errorName(e)}));
        // append/overwrite → COPY FROM STDIN (bulk, fast); upsert → INSERT.
        return openBulkOrInsert(env.gpa, c, postgres.CopySink, .postgres, w.target, schema, w.mode, try redialFor(env.arena, .postgres, cfg)) catch |e| {
            defer c.close();
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "postgres sink failed ({s}): {s}", .{ @errorName(e), c.last_error }));
        };
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

// --- params ---

fn resolveParams(arena: std.mem.Allocator, program: ast.Program, cli: []const ParamArg, params: *std.StringHashMap(Value), diag: *Diag) !void {
    for (program.stmts) |s| {
        if (s != .param) continue;
        const p = s.param;
        if (p.is_json) continue; // JSON params live in a separate namespace (expand.zig)
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
        else => mk(arena, .null_lit),
    };
}

// --- small helpers ---

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

fn schemaPtr(arena: std.mem.Allocator, schema: types.Schema) !*types.Schema {
    const p = try arena.create(types.Schema);
    p.* = schema;
    return p;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const parser = @import("../lang/parser.zig");

/// Run `@batch read csv | <body> | write csv` over `input`, returning the output.
fn runToString(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir, input: []const u8, body: []const u8) ![]u8 {
    return runToStringP(alloc, tmp, input, body, &[_]ParamArg{});
}

fn runToStringP(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir, input: []const u8, body: []const u8, cli_params: []const ParamArg) ![]u8 {
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = input });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const script = try std.fmt.allocPrint(alloc,
        "@batch\nread csv \"{s}\"\n{s}\n  | write csv \"{s}\"",
        .{ in_path, body, out_path },
    );
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
        "  | filter status == \"paid\"\n  | select id, amount",
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
        "  | aggregate n = count(), total = sum(cast(amount as int)) by status",
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
        "  | select id, amt = cast(amount as int)\n  | sort amt desc",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,amt\n3,200\n1,100\n2,\n", out);
}

test "distinct keeps first row per key" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(alloc, &tmp,
        "status,amount\npaid,100\npending,50\npaid,200\n",
        "  | distinct on status",
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

    // Job spec body: one table entry whose `name` field is the input path.
    const body = try std.fmt.allocPrint(alloc, "{{\"tables\":[{{\"name\":\"{s}\"}}]}}", .{in_path});
    defer alloc.free(body);
    const script = try std.fmt.allocPrint(alloc, "@batch\nparam job json from body\n" ++
        "for name in job.tables @[mode = sequential]\n" ++
        "  read csv \"${{name}}\" | select id | write csv \"{s}\"", .{out_path});
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
    // `${emp}` flows into a computed select column VALUE (the new capability).
    const script = try std.fmt.allocPrint(alloc, "@batch\nparam job json from body\n" ++
        "for name, emp in job.tables @[mode = sequential]\n" ++
        "  read csv \"${{name}}\" | select id, EMPRESA = \"${{emp}}\" | write csv \"{s}\"", .{out_path});
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
        "@batch\nparam min int = 0\nread csv \"{s}\"\n  | filter cast(amount as int) >= min\n  | select id\n  | write csv \"{s}\"",
        .{ in_path, out_path },
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
        "  | explode tags as tag",
    );
    defer alloc.free(out);
    // row 1 -> 3 rows; row 2 -> 1 row; row 3 (null) -> 0 rows
    try std.testing.expectEqualStrings("id,tag\n1,a\n1,b\n1,c\n2,x\n", out);
}

test "parallel driver matches serial output across many batches" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // ~5000 rows -> several 1024-row batches. CSV is non-splittable, so both the
    // 1- and 4-thread runs execute serially (split-parallel needs a SQL source);
    // this guards that requesting threads does not change or corrupt the output.
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

    const body = "  | filter cast(amount as int) >= 500\n  | select id, doubled = cast(amount as int) * 2";

    var outputs: [2][]u8 = undefined;
    for ([_]usize{ 1, 4 }, 0..) |nthreads, idx| {
        const out_path = try std.fs.path.join(alloc, &.{ base, if (idx == 0) "s.csv" else "p.csv" });
        defer alloc.free(out_path);
        const script = try std.fmt.allocPrint(alloc, "@batch\nread csv \"{s}\"\n{s}\n  | write csv \"{s}\"", .{ in_path, body, out_path });
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

    // Byte-identical: serial output is unchanged by the thread count.
    try std.testing.expectEqualStrings(outputs[0], outputs[1]);
    try std.testing.expect(std.mem.indexOf(u8, outputs[1], "id,doubled\n") != null);
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
        "@batch\nlet labels = read csv \"{s}\"\nread csv \"{s}\"\n  | join inner labels on code = code\n  | select id, label\n  | write csv \"{s}\"",
        .{ lookup_path, in_path, out_path },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    // id 3 (code Z) has no match -> dropped by inner join
    try std.testing.expectEqualStrings("id,label\n1,Apple\n2,Banana\n", out);
}

test "aggregate folds groups across multiple batches" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // 3000 rows over several 1024-row CSV batches: group accumulators (and the
    // string min, which must be deep-copied into state) carry across pulls.
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
        "@batch\nread csv \"{s}/in.csv\"\n  | aggregate n = count(), total = sum(cast(amount as int)), first_name = min(name) by code\n  | sort code\n  | write csv \"{s}/out.csv\"",
        .{ base, base },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    // X: even i (0..2998) -> n=1500, sum=2248500, min name n0000
    // Y: odd  i (1..2999) -> n=1500, sum=2250000, min name n0001
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
        "@batch\nread csv \"{s}/in.csv\"\n  | select amt = cast(amount as int)\n  | aggregate n = count(), total = sum(amt), lo = min(amt), hi = max(amt)\n  | write csv \"{s}/out.csv\"",
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

    // 3000 rows / 3 distinct codes, spanning several 1024-row CSV batches: the
    // streaming seen-set must carry across pulls (and arena resets).
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
        "@batch\nread csv \"{s}/in.csv\"\n  | distinct\n  | write csv \"{s}/out.csv\"",
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

    // More rows than one CSV batch (1024), so the join probes its build index
    // across several pulls — the per-batch arena is reset between pulls, which
    // must not invalidate the build batch or hash index (they live in the plan
    // arena via Join.state).
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
        "@batch\nlet labels = read csv \"{s}\"\nread csv \"{s}\"\n  | join inner labels on code = code\n  | select id, label\n  | write csv \"{s}\"",
        .{ lookup_path, in_path, out_path },
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
    try tmp.dir.writeFile(.{ .sub_path = "b.csv", .data = "id,w\n3,99\n" }); // missing v, extra w
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    // canon = first (a: id, v) + tag `src`. b: id present, v -> NULL, w dropped.
    const script = try std.fmt.allocPrint(
        alloc,
        "@batch\nunion from csv \"{s}/a.csv\" as \"01\" from csv \"{s}/b.csv\" as \"02\"\n  @[tag = src, canon = first]\n  | write csv \"{s}/out.csv\"",
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
    // header is the canon (tag, id, v) — `w` is dropped; b's missing `v` is null.
    try std.testing.expect(std.mem.startsWith(u8, out, "src,id,v\n"));
    try std.testing.expect(std.mem.indexOf(u8, out, "w") == null);
    try std.testing.expect(std.mem.indexOf(u8, out, "01,1,10") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "01,2,20") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "02,3,") != null); // b: tag 02, id 3, v null
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

    // for name in csv "<base>/names.csv"
    //   read csv "<base>/${name}.csv" | select id, v | write csv "<base>/out_${name}.csv"
    const script = try std.fmt.allocPrint(
        alloc,
        "@batch\nfor name in csv \"{s}/names.csv\"\n  read csv \"{s}/${{name}}.csv\"\n    | select id, v\n    | write csv \"{s}/out_${{name}}.csv\"",
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
    try std.testing.expectEqual(@as(u64, 3), stats.rows_out); // 2 (alpha) + 1 (beta)

    const a = try tmp.dir.readFileAlloc(alloc, "out_alpha.csv", 1 << 20);
    defer alloc.free(a);
    const b = try tmp.dir.readFileAlloc(alloc, "out_beta.csv", 1 << 20);
    defer alloc.free(b);
    try std.testing.expectEqualStrings("id,v\n1,10\n2,20\n", a);
    try std.testing.expectEqualStrings("id,v\n3,30\n", b);
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
    // empty predicate (e.g. `${since}` rendered empty on a full extraction) -> base untouched
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
    // ghost.csv is intentionally missing -> that table's read fails.
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(
        alloc,
        "@batch\nfor name in csv \"{s}/names.csv\" @[mode = parallel, on_error = continue]\n  read csv \"{s}/${{name}}.csv\"\n    | write csv \"{s}/out_${{name}}.csv\"",
        .{ base, base, base },
    );
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    // one table fails, so the run reports a non-zero (PlanFailed) result...
    try std.testing.expectError(error.PlanFailed, run(alloc, prog, .{ .threads = 2 }, &rdiag));
    // ...but the healthy table still produced its output (failure was isolated).
    const a = try tmp.dir.readFileAlloc(alloc, "out_alpha.csv", 1 << 20);
    defer alloc.free(a);
    try std.testing.expectEqualStrings("id\n7\n", a);
}
