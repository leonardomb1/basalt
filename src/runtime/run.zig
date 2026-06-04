//! Build executable operator trees from a parsed @batch program and drive them.
//! Handles full program structure: `param`s (bound from the CLI or defaults),
//! `let` bindings (recompiled per reference), `ref` sources, and multiple output
//! pipelines. Params are substituted into expressions as literals before planning.

const std = @import("std");
const ast = @import("../lang/ast.zig");
const types = @import("../lang/types.zig");
const op = @import("../exec/op.zig");
const csv = @import("../connect/csv.zig");
const driver = @import("../connect/driver.zig");
const starrocks = @import("../connect/starrocks.zig");
const tds = @import("../connect/tds.zig");
const mysql = @import("../connect/mysql.zig");
const postgres = @import("../connect/postgres.zig");
const sql = @import("../connect/sql.zig");
const request = @import("../connect/request.zig");
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
};
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
pub const RunOptions = struct {
    params: []const ParamArg = &.{},
    request_body: ?[]const u8 = null,
    /// Worker threads for map-only pipelines (scan → filter/project/explode). 1 =
    /// the serial driver (deterministic, used by the in-process test harness); the
    /// CLI defaults this to the detected core count.
    threads: usize = 1,
    log: LogConfig = .{},
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
    sources: *std.ArrayList(driver.Source),
    request_body: ?[]const u8,
    diag: *Diag,
    log: *obs.Logger,
    /// Param name → literal expr, for substitution in stage expressions.
    params_expr: *std.StringHashMap(*const ast.Expr),
    /// Runtime expression-error context (which stage/column failed).
    errctx: *op.ErrCtx,
    /// Emitted-row counter shared by every source (via `obs.CountingSource`).
    rows_read: *std.atomic.Value(u64),
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
        .postgres => (try postgres.Conn.connect(gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database)).sqlConn(),
        .mysql => (try mysql.Conn.connect(gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database)).sqlConn(),
        .sqlserver => (try tds.Conn.connect(gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database)).sqlConn(),
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
};

/// Open the per-dialect write strategy from an already-connected conn: a bulk
/// loader (COPY / LOAD DATA / INSERT BULK) for append/overwrite, or the generic
/// INSERT `sql.Sink` for upsert. Centralizes the bulk-vs-INSERT rule so the serial
/// (`openSink`) and per-lane (`openLaneSqlSink`) paths can't drift. `conn` is the
/// concrete driver connection; on error the caller still owns and closes it.
fn openBulkOrInsert(gpa: std.mem.Allocator, conn: anytype, comptime BulkSink: type, dialect: sql.Dialect, target: []const u8, schema: types.Schema, mode: ast.WriteMode) !driver.Sink {
    if (mode != .upsert) return (try BulkSink.open(gpa, conn, target, schema, mode)).sink();
    return (try sql.Sink.open(gpa, conn.sqlConn(), dialect, target, schema, mode)).sink();
}

/// `parallel.OpenSinkFn`: one DB stream per lane (append/overwrite → bulk loader,
/// upsert → INSERT, per `openBulkOrInsert`).
fn openLaneSqlSink(ctx_ptr: *anyopaque, gpa: std.mem.Allocator, lane_idx: usize) anyerror!driver.Sink {
    _ = lane_idx; // SQL sinks need no per-lane discriminator (INSERTs aren't labelled)
    const spec: *SqlSinkSpec = @ptrCast(@alignCast(ctx_ptr));
    const cfg = spec.cfg;
    switch (spec.kind) {
        .postgres => {
            const c = try postgres.Conn.connect(gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database);
            errdefer c.close();
            return openBulkOrInsert(gpa, c, postgres.CopySink, spec.dialect, spec.target, spec.schema, spec.lane_mode);
        },
        .mysql => {
            const c = try mysql.Conn.connect(gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database);
            errdefer c.close();
            return openBulkOrInsert(gpa, c, mysql.LoadDataSink, spec.dialect, spec.target, spec.schema, spec.lane_mode);
        },
        .sqlserver => {
            const c = try tds.Conn.connect(gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database);
            errdefer c.close();
            return openBulkOrInsert(gpa, c, tds.BulkSink, spec.dialect, spec.target, spec.schema, spec.lane_mode);
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
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "{s} sink connect failed: {s}", .{ conn.connector, @errorName(e) }));
    const setup = sql.Sink.open(env.gpa, setup_conn, dialect, w.target, schema, w.mode) catch |e|
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
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "starrocks setup failed ({s}) — {s}", .{ @errorName(e), env.diag.msg }));
    setup.sink().close() catch |e|
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "starrocks setup close failed: {s}", .{@errorName(e)}));
    cfg.auto_create = false;

    const spec = try env.arena.create(StarrocksSinkSpec);
    // Lanes must not re-truncate (overwrite's TRUNCATE already ran once in setup
    // above), so map overwrite -> append for the per-lane sinks — same as buildSqlSinkSpec.
    spec.* = .{ .cfg = cfg, .target = w.target, .schema = schema, .mode = if (w.mode == .overwrite) .append else w.mode };
    return spec;
}

pub fn run(gpa: std.mem.Allocator, program: ast.Program, opts: RunOptions, diag: *Diag) !Stats {
    var plan_arena = std.heap.ArenaAllocator.init(gpa);
    defer plan_arena.deinit();
    const arena = plan_arena.allocator();

    if (program.stmts.len == 0 or program.stmts[0] != .kind)
        return planErr(diag, "script must begin with a @kind tag");
    // @batch runs once; @http reuses this for each request (with a request body).
    if (program.stmts[0].kind.kind == .stream)
        return planErr(diag, "@stream is not implemented yet");

    var params = std.StringHashMap(Value).init(arena);
    try resolveParams(arena, program, opts.params, &params, diag);
    // Substitution map: param name → a literal expression of its resolved value.
    var params_expr = std.StringHashMap(*const ast.Expr).init(arena);
    var pit = params.iterator();
    while (pit.next()) |kv| try params_expr.put(kv.key_ptr.*, try mkLit(arena, kv.value_ptr.*));

    var bindings = std.StringHashMap(ast.Pipeline).init(arena);
    var connections = std.StringHashMap(ast.Connection).init(arena);
    var runnable: usize = 0; // outputs + for-each blocks
    for (program.stmts[1..]) |s| switch (s) {
        .binding => |b| try bindings.put(b.name, b.pipeline),
        .connection => |c| try connections.put(c.name, c),
        .output, .for_each => runnable += 1,
        .param, .kind => {},
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

    var sources = std.ArrayList(driver.Source).init(arena);
    var env = Env{ .arena = arena, .gpa = gpa, .params = &params, .bindings = &bindings, .connections = &connections, .sources = &sources, .request_body = opts.request_body, .diag = diag, .log = &logger, .params_expr = &params_expr, .errctx = &errctx, .rows_read = &rows_read };

    var batch_arena = std.heap.ArenaAllocator.init(gpa);
    defer batch_arena.deinit();

    var stats = Stats{ .run_id = run_id };
    var lanes_used: usize = 1; // actual parallelism (1 unless split-parallel engaged)
    // Execute outputs and for-each blocks in program order.
    for (program.stmts[1..]) |s| switch (s) {
        .output => |p| try runOutput(&env, p, opts, &stats, &lanes_used, &batch_arena),
        .for_each => |fe| try runForEach(&env, fe, opts, &stats, &lanes_used, &batch_arena),
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
        .json_stdout => summary.renderJson(std.io.getStdOut().writer()) catch {},
        .stderr => logger.summary(summary),
        .none => {},
    }
    return stats;
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

    env.sql_desc = null;
    env.src_name = ""; // reset per output so this pipeline's first read sets it
    const src_base = env.sources.items.len; // sources this output opens (for early release on the split path)
    const res = try buildPipeline(env, stages[0 .. stages.len - 1]);

    // Split-parallel: a map-only pipeline (no breakers/limit) reading a splittable
    // SQL source fans out into N key-range lanes, each on its own connection. A
    // StarRocks sink also fans out (one stream-load stream per lane); other sinks
    // (CSV) stay shared under a mutex. Non-splittable/stateful pipelines stay serial.
    if (opts.threads > 1 and env.sql_desc != null) {
        if (try op.linearize(arena, res.op)) |lin| {
            if (try planSplit(env, env.sql_desc.?, stages[0], opts.threads, last.write)) |sp| {
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
                if (try buildParallelSink(env, last.write, schema)) |mode| {
                    stats.rows_out += try parallel.run(gpa, sp.predicates, openSplitSource, &ctx, lin.stages, mode, opts.threads, env.rows_read);
                } else {
                    const snk = try openSink(env, last.write, schema);
                    stats.rows_out += try parallel.run(gpa, sp.predicates, openSplitSource, &ctx, lin.stages, .{ .shared = snk }, opts.threads, env.rows_read);
                    try snk.close();
                }
                return;
            }
        }
    }

    const snk = try openSink(env, last.write, res.schema);
    while (true) {
        _ = batch_arena.reset(.retain_capacity);
        const b = (try res.op.next(batch_arena.allocator())) orelse break;
        try snk.writeBatch(batch_arena.allocator(), b);
        stats.rows_out += b.len;
    }
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
    const src = openSource(env, src_read) catch |e|
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "for-each discovery failed: {s}", .{@errorName(e)}));
    defer src.close();
    var rows = std.ArrayList(Row).init(env.arena);
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

/// Replace every needle (`${var}`) in `s` with its row value (chained over all
/// loop variables). Returns `s` unchanged (no copy) when no needle is present.
fn interpAll(arena: std.mem.Allocator, s: []const u8, needles: []const []const u8, row: Row) ![]const u8 {
    var cur = s;
    for (needles, row) |needle, value| {
        if (std.mem.indexOf(u8, cur, needle) == null) continue;
        const size = std.mem.replacementSize(u8, cur, needle, value);
        const buf = try arena.alloc(u8, size);
        _ = std.mem.replace(u8, cur, needle, value, buf);
        cur = buf;
    }
    return cur;
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
    } };
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
            .write => |w| dst.node = .{ .write = try renderWrite(arena, w, needles, row) },
            else => {},
        }
    }
    return .{ .stages = stages, .pos = body.pos };
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
    next: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    rows_out: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    failures: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    mu: std.Thread.Mutex = .{},
    first_err_buf: [256]u8 = undefined,
    first_err_len: usize = 0,
};

fn forRecordFail(ctx: *ForCtx, label: []const u8, ename: []const u8, msg: []const u8) void {
    _ = ctx.failures.fetchAdd(1, .monotonic);
    ctx.mu.lock();
    defer ctx.mu.unlock();
    if (ctx.first_err_len == 0) {
        const s = std.fmt.bufPrint(&ctx.first_err_buf, "{s}: {s} {s}", .{ label, ename, msg }) catch ctx.first_err_buf[0..0];
        ctx.first_err_len = s.len;
    }
    if (ctx.on_error == .stop) ctx.stop.store(true, .release);
}

fn forWorker(ctx: *ForCtx) void {
    const gpa = ctx.base.gpa;
    while (true) {
        if (ctx.on_error == .stop and ctx.stop.load(.acquire)) break;
        const i = ctx.next.fetchAdd(1, .monotonic);
        if (i >= ctx.rows.len) break;
        const row = ctx.rows[i];

        var w_arena = std.heap.ArenaAllocator.init(gpa);
        defer w_arena.deinit();
        var w_batch = std.heap.ArenaAllocator.init(gpa);
        defer w_batch.deinit();
        var w_sources = std.ArrayList(driver.Source).init(w_arena.allocator());
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
        };
        var st = Stats{ .run_id = 0 };
        var lanes: usize = 1;
        const pipe = renderPipeline(&w_env, ctx.fe.body, ctx.needles, row) catch |e| {
            forRecordFail(ctx, row[0], @errorName(e), "");
            continue;
        };
        if (runOutput(&w_env, pipe, ctx.worker_opts, &st, &lanes, &w_batch)) |_| {
            _ = ctx.rows_out.fetchAdd(st.rows_out, .monotonic);
        } else |e| {
            forRecordFail(ctx, row[0], @errorName(e), w_diag.msg);
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

    const rows = try discoverRows(env, fe.source, fe.var_names.len);
    env.log.log(.info, "for-each {s}: {d} row(s) [{s}, on_error={s}]", .{ fe.var_names[0], rows.len, @tagName(mode), if (on_error == .continue_) "continue" else "stop" });
    if (rows.len == 0) return;
    const needles = try env.arena.alloc([]const u8, fe.var_names.len);
    for (fe.var_names, needles) |name, *n| n.* = try std.fmt.allocPrint(env.arena, "${{{s}}}", .{name});

    switch (mode) {
        .sequential => {
            var failures: usize = 0;
            var first_err: ?[]const u8 = null;
            for (rows) |row| {
                const base = env.sources.items.len;
                const pipe = try renderPipeline(env, fe.body, needles, row);
                if (runOutput(env, pipe, opts, stats, lanes_used, batch_arena)) |_| {
                    for (env.sources.items[base..]) |sc| sc.close();
                    env.sources.shrinkRetainingCapacity(base);
                } else |e| {
                    for (env.sources.items[base..]) |sc| sc.close();
                    env.sources.shrinkRetainingCapacity(base);
                    failures += 1;
                    env.log.log(.err, "for-each {s} failed: {s}", .{ row[0], @errorName(e) });
                    if (first_err == null)
                        first_err = std.fmt.allocPrint(env.arena, "{s}: {s}", .{ row[0], @errorName(e) }) catch null;
                    if (on_error == .stop) return planErr(env.diag, first_err orelse "for-each failed");
                }
            }
            if (failures > 0)
                return planErr(env.diag, try std.fmt.allocPrint(env.arena, "for-each: {d}/{d} failed (first: {s})", .{ failures, rows.len, first_err orelse "?" }));
        },
        .parallel => {
            var wopts = opts;
            wopts.threads = 1; // each table runs serially; the for-loop provides the parallelism
            const nworkers = @min(@max(opts.threads, @as(usize, 1)), rows.len);
            var ctx = ForCtx{ .fe = fe, .needles = needles, .rows = rows, .base = env, .worker_opts = wopts, .on_error = on_error };
            const threads = try env.arena.alloc(std.Thread, nworkers);
            var spawned: usize = 0;
            while (spawned < nworkers) : (spawned += 1) {
                threads[spawned] = std.Thread.spawn(.{}, forWorker, .{&ctx}) catch break;
            }
            if (spawned == 0) forWorker(&ctx) else for (threads[0..spawned]) |t| t.join();
            stats.rows_out += ctx.rows_out.load(.monotonic);
            lanes_used.* = @max(lanes_used.*, @max(spawned, @as(usize, 1)));
            const fails = ctx.failures.load(.monotonic);
            if (fails > 0)
                return planErr(env.diag, try std.fmt.allocPrint(env.arena, "for-each: {d}/{d} failed (first: {s})", .{ fails, rows.len, ctx.first_err_buf[0..ctx.first_err_len] }));
        },
    }
}

/// Resolve a sink/source connector name to its driver type for the summary
/// (`csv`/`request` are types; a connection name maps to its `connector`).
fn connectorType(env: *Env, name: []const u8) []const u8 {
    if (std.mem.eql(u8, name, "csv") or std.mem.eql(u8, name, "request")) return name;
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
            const raw = try openSource(env, rd);
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
    var items = std.ArrayList(ast.SelectItem).init(arena);
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

fn buildUnion(env: *Env, u: ast.Union, hints: []const ast.Hint) anyerror!PipeRes {
    const arena = env.arena;
    const tag_col = forHintIdent(hints, "tag");
    const canon_opt = forHintIdent(hints, "canon");

    // 1. resolve the branch list: explicit, or discovered via a (table, tag) query.
    const Spec = struct { read: ast.Read, tag: ?[]const u8, name: []const u8 };
    var specs = std.ArrayList(Spec).init(arena);
    if (u.discover_query.len > 0) {
        const disc = ast.Read{ .connector = u.discover_conn, .form = .{ .query = u.discover_query } };
        for (try discoverRows(env, disc, 2)) |row| {
            const parts = try arena.alloc([]const u8, 1);
            parts[0] = row[0];
            try specs.append(.{ .read = .{ .connector = u.discover_conn, .form = .{ .table = .{ .parts = parts } } }, .tag = row[1], .name = row[0] });
        }
    } else for (u.branches) |b| try specs.append(.{ .read = b.read, .tag = b.tag, .name = readName(b.read) });
    if (specs.items.len == 0) return planErr(env.diag, "union has no source tables");

    // 2. open each branch source (kept open — drained sequentially by the Union op).
    const Br = struct { sop: op.Op, schema: types.Schema, name: []const u8, tag: ?[]const u8 };
    var brs = std.ArrayList(Br).init(arena);
    for (specs.items) |s| {
        const raw = try openSource(env, s.read);
        const cs = try arena.create(obs.CountingSource);
        cs.* = .{ .inner = raw, .count = env.rows_read };
        const src = cs.source();
        try env.sources.append(src);
        if (env.src_name.len == 0) env.src_name = connectorType(env, s.read.connector);
        const scan = try arena.create(op.Scan);
        scan.* = .{ .src = src };
        try brs.append(.{ .sop = .{ .scan = scan }, .schema = src.schema(), .name = s.name, .tag = s.tag });
    }

    // 3. pick the canon schema (a named source table, or `first`).
    var canon_src = brs.items[0].schema;
    if (canon_opt) |c| if (!std.mem.eql(u8, c, "first")) {
        var found = false;
        for (brs.items) |b| if (std.mem.eql(u8, b.name, c)) {
            canon_src = b.schema;
            found = true;
            break;
        };
        if (!found) return planErr(env.diag, try std.fmt.allocPrint(arena, "union canon `{s}` is not one of the source tables", .{c}));
    };
    const canon = try dupeSchema(arena, canon_src);

    // 4. reconcile each branch to canon, then union the children.
    const children = try arena.alloc(op.Op, brs.items.len);
    var out_schema: types.Schema = undefined;
    for (brs.items, 0..) |b, i| {
        const items = try synthReconcile(arena, b.schema, canon, tag_col, b.tag);
        const proj = try buildProject(env, items, b.schema, b.sop);
        children[i] = proj.op;
        out_schema = proj.schema;
    }
    const un = try arena.create(op.Union);
    un.* = .{ .children = children };
    return .{ .op = .{ .union_ = un }, .schema = out_schema };
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
            o.* = .{ .child = child, .in_schema = try schemaPtr(arena, schema), .keys = keys };
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
    o.* = .{ .child = child, .in_schema = try schemaPtr(arena, schema), .by = ap.by, .aggs = aggs, .out_schema = out, .err = env.errctx };
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
    };
    return .{ .op = .{ .join = o }, .schema = out.* };
}

fn openSource(env: *Env, rd: ast.Read) !driver.Source {
    if (std.mem.eql(u8, rd.connector, "request")) {
        const body = env.request_body orelse
            return planErr(env.diag, "`read request` is only available when serving HTTP (@http)");
        const s = request.RequestSource.open(env.gpa, body) catch |e|
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "could not parse request body as JSON: {s}", .{@errorName(e)}));
        return s.source();
    }
    if (std.mem.eql(u8, rd.connector, "csv")) {
        if (rd.form != .path) return planErr(env.diag, "read csv needs a quoted path");
        const reader = csv.CsvReader.open(env.arena, rd.form.path) catch
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "could not open input CSV `{s}`", .{rd.form.path}));
        return reader.source();
    }
    const conn = env.connections.get(rd.connector) orelse
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "unknown connection `{s}`", .{rd.connector}));
    if (std.mem.eql(u8, conn.connector, "sqlserver")) {
        const cfg = try resolveDbConfig(env, conn, 1433);
        const query = try readSql(env, rd);
        const c = tds.Conn.connect(env.gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database) catch |e|
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "sqlserver connect failed: {s}", .{@errorName(e)}));
        const s = sql.Source.open(env.gpa, c.sqlConn(), query) catch |e| {
            defer c.close();
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "sqlserver read failed ({s}): {s}", .{ @errorName(e), c.last_error }));
        };
        env.sql_desc = try sqlDescFor(env, .sqlserver, .sqlserver, cfg, query, rd);
        return s.source();
    }
    if (std.mem.eql(u8, conn.connector, "mysql")) {
        const cfg = try resolveDbConfig(env, conn, 3306);
        const query = try readSql(env, rd);
        const c = mysql.Conn.connect(env.gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database) catch |e|
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "mysql connect failed: {s}", .{@errorName(e)}));
        const s = sql.Source.open(env.gpa, c.sqlConn(), query) catch |e| {
            defer c.close();
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "mysql read failed ({s}): {s}", .{ @errorName(e), c.last_error }));
        };
        env.sql_desc = try sqlDescFor(env, .mysql, .mysql, cfg, query, rd);
        return s.source();
    }
    if (std.mem.eql(u8, conn.connector, "postgres")) {
        const cfg = try resolveDbConfig(env, conn, 5432);
        const query = try readSql(env, rd);
        const c = postgres.Conn.connect(env.gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database) catch |e|
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "postgres connect failed: {s}", .{@errorName(e)}));
        const s = sql.Source.open(env.gpa, c.sqlConn(), query) catch |e| {
            defer c.close();
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "postgres read failed ({s}): {s}", .{ @errorName(e), c.last_error }));
        };
        env.sql_desc = try sqlDescFor(env, .postgres, .postgres, cfg, query, rd);
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
};

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
        }
    }
    if (cfg.host.len == 0) return planErr(env.diag, "connection needs a `host`");
    return cfg;
}

fn readSql(env: *Env, rd: ast.Read) ![]const u8 {
    return switch (rd.form) {
        .query => |q| q,
        .table => |t| try std.fmt.allocPrint(env.arena, "SELECT * FROM {s}", .{try qualStr(env.arena, t)}),
        else => planErr(env.diag, "a DB read needs `table <name>` or `query \"...\"`"),
    };
}

fn sqlDescFor(env: *Env, kind: SqlKind, dialect: sql.Dialect, cfg: DbConfig, base_sql: []const u8, rd: ast.Read) !SqlDesc {
    const table: ?[]const u8 = switch (rd.form) {
        .table => |t| try qualStr(env.arena, t),
        else => null,
    };
    return .{ .kind = kind, .dialect = dialect, .cfg = cfg, .base_sql = base_sql, .table = table };
}

/// Pull `@[split = col]` / `@[splits = N]` off the leading read stage.
const SplitHints = struct { col: ?[]const u8 = null, count: ?usize = null };
fn splitHints(stage: ast.Stage) SplitHints {
    var h = SplitHints{};
    for (stage.hints) |hint| {
        if (std.mem.eql(u8, hint.key, "split")) {
            if (hint.value == .ident) h.col = hint.value.ident;
        } else if (std.mem.eql(u8, hint.key, "splits")) {
            if (hint.value == .int and hint.value.int > 0) h.count = @intCast(hint.value.int);
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
        key = .{ .col = col, .kind = .int }; // explicit key: assume int (range)
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

fn openSink(env: *Env, w: ast.Write, schema: types.Schema) !driver.Sink {
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
        return s.sink();
    }
    if (std.mem.eql(u8, conn.connector, "mysql")) {
        const cfg = try resolveDbConfig(env, conn, 3306);
        const c = mysql.Conn.connect(env.gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database) catch |e|
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "mysql connect failed: {s}", .{@errorName(e)}));
        // append/overwrite → LOAD DATA LOCAL INFILE (bulk); upsert → INSERT.
        return openBulkOrInsert(env.gpa, c, mysql.LoadDataSink, .mysql, w.target, schema, w.mode) catch |e| {
            defer c.close();
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "mysql sink failed ({s}): {s}", .{ @errorName(e), c.last_error }));
        };
    }
    if (std.mem.eql(u8, conn.connector, "sqlserver")) {
        const cfg = try resolveDbConfig(env, conn, 1433);
        const c = tds.Conn.connect(env.gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database) catch |e|
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "sqlserver connect failed: {s}", .{@errorName(e)}));
        // append/overwrite → INSERT BULK; upsert → INSERT.
        return openBulkOrInsert(env.gpa, c, tds.BulkSink, .sqlserver, w.target, schema, w.mode) catch |e| {
            defer c.close();
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "sqlserver sink failed ({s}): {s}", .{ @errorName(e), c.last_error }));
        };
    }
    if (std.mem.eql(u8, conn.connector, "postgres")) {
        const cfg = try resolveDbConfig(env, conn, 5432);
        const c = postgres.Conn.connect(env.gpa, cfg.host, cfg.port, cfg.user, cfg.password, cfg.database) catch |e|
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "postgres connect failed: {s}", .{@errorName(e)}));
        // append/overwrite → COPY FROM STDIN (bulk, fast); upsert → INSERT.
        return openBulkOrInsert(env.gpa, c, postgres.CopySink, .postgres, w.target, schema, w.mode) catch |e| {
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
    var in = std.ArrayList(u8).init(alloc);
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
