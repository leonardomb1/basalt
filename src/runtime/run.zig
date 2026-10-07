//! Runtime entry: parse-tree to execution. `run` walks the statements; each
//! pipeline is planned (plan.zig), resolved to sources/sinks (connect.zig), and
//! either fanned out (lanes.zig) or driven serially here.
//!
//! Statement order is scope order. Expression LETs are folded once before anything
//! runs (a LET nested in a branch is refused rather than missed); a query LET runs
//! in statement order, and a `WITH` is registered where it stands, so two
//! statements may reuse a CTE name and each sees its own (registering all up front
//! once made the last win for both). A `for` body's bindings are scoped to the
//! body and restored after each row. Sources are closed on every way out: they own
//! sockets and gpa memory outside the plan arena, and a failed run under `serve`
//! once leaked a socket per request. `EXPLAIN ANALYZE` runs serially: ten parallel
//! paths would each need their own lane figures, and a plan must not depend on the
//! pipeline's shape to print at all.
//!
//! Under `--format json` a LOAD run's stdout is the summary object and a SELECT
//! run's the NDJSON rows, never both; a SELECT's summary then goes to a JSON log
//! only. Statement `EXPLAIN` writes to stderr for the same reason, in one
//! `writeAll`: a `File.Writer` opened on stderr mid-run starts at position zero and
//! overwrote the logger's output when stderr was a regular file. Under Arrow
//! output the plan is a one-column result instead.
//!
//! `runOutputBody` prepares a pipeline in a fixed order, each step there because a
//! past bug lived in its absence: inline head bindings so the binding's WHERE
//! reaches the source (a SQL table read through a CTE used to be read whole);
//! refuse unreadable extensions as `check` does (a zip's COUNT(*) once answered
//! 46204); substitute `$param`s in filters before they become SQL; hoist filters
//! past joins so `serialWhere` sees them; carry `SPLIT BY ... JOBS n` from the sink
//! to the read's hints; narrow SQL reads to the needed columns. Then one
//! classification picks the lane shape: `runParquetLane`/`runCsvLane` switch
//! exhaustively over `LaneShape`, since two parallel if-chains drifting apart
//! caused both 0.5.8 lane bugs. Whole-aggregate and top-N descent to a SQL source
//! win over splitting (one small result beats N range queries shipping raw rows);
//! an aggregate that cannot descend is logged, being the costliest silent
//! behaviour the engine has. In the serial driver the ping-pong arenas are
//! declared before the `PipelinedSink` so its `shutdown` defer runs first: the
//! reverse freed the arenas while the writer still serialised from them.
//!
//! The statements run here; an output pipeline's execution is `run/output.zig`,
//! EXPLAIN `run/explain.zig`, and PARAM/LET binding `run/params.zig`.

const std = @import("std");
const ast = @import("../lang/ast.zig");
const expand = @import("../lang/expand.zig");
const ftp = @import("../store/ftp.zig");
const types = @import("../lang/types.zig");
const op = @import("../exec/op.zig");
const Batch = @import("../exec/batch.zig").Batch;
const csv = @import("../format/csv.zig");
const column = @import("../exec/column.zig");
const arrow = @import("../format/arrow.zig");
const eval = @import("../exec/eval.zig");
const driver = @import("../connect/driver.zig");
const sql = @import("../db/sql.zig");
const request = @import("../connect/request.zig");
const parallel = @import("parallel.zig");
const analyze = @import("analyze.zig");
const pushdown = @import("pushdown.zig");
const obs = @import("obs.zig");
const Value = @import("../exec/value.zig").Value;

pub const aborting = @import("env.zig").aborting;
pub const Diag = @import("env.zig").Diag;
const Env = @import("env.zig").Env;
pub const errLabel = @import("env.zig").errLabel;
pub const failLabel = op.failLabel;
const forHintIdent = @import("env.zig").forHintIdent;
const hasFlagHint = @import("env.zig").hasFlagHint;
pub const isTransient = @import("env.zig").isTransient;
pub const ItemOutcome = @import("env.zig").ItemOutcome;
pub const LogConfig = @import("env.zig").LogConfig;
const LoopRow = @import("env.zig").LoopRow;
const mkLit = @import("env.zig").mkLit;
const no_loop_vars = @import("env.zig").no_loop_vars;
const body_stmt_rule = @import("env.zig").body_stmt_rule;
pub const OutcomeSink = @import("env.zig").OutcomeSink;
pub const ParamArg = @import("env.zig").ParamArg;
const planErr = @import("env.zig").planErr;
pub const requestAbort = @import("env.zig").requestAbort;
pub const requestReload = @import("env.zig").requestReload;
pub const resetAbort = @import("env.zig").resetAbort;
pub const RunOptions = @import("env.zig").RunOptions;
const schemaPtr = @import("env.zig").schemaPtr;
const setMsg = @import("env.zig").setMsg;
pub const Stats = @import("env.zig").Stats;
pub const StdoutFormat = @import("env.zig").StdoutFormat;
pub const ResultDone = @import("env.zig").ResultDone;
pub const ResultHook = @import("env.zig").ResultHook;
pub const LetHook = @import("env.zig").LetHook;
pub const LoadDone = @import("env.zig").LoadDone;
pub const LoadHook = @import("env.zig").LoadHook;
pub const ResultInfo = arrow.ResultInfo;
pub const SummaryMode = @import("env.zig").SummaryMode;
pub const takeReload = @import("env.zig").takeReload;

const buildParallelSink = @import("connect.zig").buildParallelSink;
const dupeSchema = @import("connect.zig").dupeSchema;
const guardFileFormat = @import("connect.zig").guardFileFormat;
const registerSftp = @import("connect.zig").registerSftp;
const registerFtp = @import("connect.zig").registerFtp;
const registerSmb = @import("connect.zig").registerSmb;
const isLocalCsvRead = @import("connect.zig").isLocalCsvRead;
const isLocalParquetRead = @import("connect.zig").isLocalParquetRead;
const isFolderRead = @import("connect.zig").isFolderRead;
const openSink = @import("connect.zig").openSink;
const openSource = @import("connect.zig").openSource;
const openSplitSource = @import("connect.zig").openSplitSource;
const planSplit = @import("connect.zig").planSplit;
const projectSqlRead = @import("connect.zig").projectSqlRead;
const exceptColumns = @import("connect.zig").exceptColumns;
const resolveUpsertKeys = @import("connect.zig").resolveUpsertKeys;
const sinkLabel = @import("connect.zig").sinkLabel;
const sqlConnInfo = @import("connect.zig").sqlConnInfo;
const factsIfWanted = @import("connect.zig").factsIfWanted;
const readReport = @import("connect.zig").readReport;
const env_mod = @import("env.zig");
const mem_connector = @import("connect.zig").mem_connector;
const SplitCtx = @import("connect.zig").SplitCtx;

const buildPipeline = @import("plan.zig").buildPipeline;
const descendLeadingWhere = @import("plan.zig").descendLeadingWhere;
const inlineHeadBindings = @import("plan.zig").inlineHeadBindings;
const rebuildMapStages = @import("plan.zig").rebuildMapStages;
const synthReconcile = @import("plan.zig").synthReconcile;
const unionCanon = @import("plan.zig").unionCanon;
const unionExceptNames = @import("plan.zig").unionExceptNames;
const unionDownstreamMapOnly = @import("plan.zig").unionDownstreamMapOnly;
const unionSpecs = @import("plan.zig").unionSpecs;

const classifyAggPipeline = @import("lanes.zig").classifyAggPipeline;
const classifyLaneShape = @import("lanes.zig").classifyLaneShape;
const classifyMapJoinPipeline = @import("lanes.zig").classifyMapJoinPipeline;
const classifyWholeAgg = @import("lanes.zig").classifyWholeAgg;
const laneEligible = @import("lanes.zig").laneEligible;
const projectedColumns = @import("plan.zig").projectedColumns;
const runCsvLane = @import("lanes.zig").runCsvLane;
const runParallelSqlAgg = @import("lanes.zig").runParallelSqlAgg;
const runParallelSqlMapJoin = @import("lanes.zig").runParallelSqlMapJoin;
const runParquetLane = @import("lanes.zig").runParquetLane;
const wholeAggStages = @import("lanes.zig").wholeAggStages;
const topNStages = @import("lanes.zig").topNStages;

const buildScriptScope = @import("script.zig").buildScriptScope;
const renderPipeline = @import("script.zig").renderPipeline;
const renderScriptScope = @import("script.zig").renderScriptScope;
const runCall = @import("script.zig").runCall;
const runForEach = @import("script.zig").runForEach;
const renderForSource = @import("script.zig").renderForSource;
const runPrint = @import("script.zig").runPrint;
const runThrow = @import("script.zig").runThrow;

pub fn run(gpa: std.mem.Allocator, raw_program: ast.Program, opts_in: RunOptions, diag: *Diag) !Stats {
    var opts = opts_in;
    if (opts.explain) opts.threads = 1;
    ftp.beginRun();
    defer {
        if (diag.msg.len > 0) {
            var tmp: [diag.buf.len]u8 = undefined;
            const m = ftp.unlocalize(&tmp, diag.msg);
            @memcpy(diag.buf[0..m.len], m);
            diag.msg = diag.buf[0..m.len];
        }
        ftp.endRun();
    }

    var plan_arena = std.heap.ArenaAllocator.init(gpa);
    defer plan_arena.deinit();
    const arena = plan_arena.allocator();

    var expand_msg: []const u8 = "";
    const json_args = try arena.alloc(expand.JsonArg, opts.params.len);
    for (opts.params, json_args) |kv, *ja| ja.* = .{ .name = kv.key, .text = kv.val };
    const program = expand.expandProgramWith(arena, raw_program, opts.request_body, json_args, &expand_msg) catch |e| switch (e) {
        error.OutOfMemory => return e,
        error.ExpandFailed => return planErr(diag, expand_msg),
    };

    if (program.stmts.len == 0 or program.stmts[0] != .kind)
        return planErr(diag, "internal error: the parsed program has no kind statement");
    var params = std.StringHashMap(Value).init(arena);
    try resolveParams(arena, program, opts.params, &params, diag);
    var params_expr = std.StringHashMap(*const ast.Expr).init(arena);
    var pit = params.iterator();
    while (pit.next()) |kv| try params_expr.put(kv.key_ptr.*, try mkLit(arena, kv.value_ptr.*));
    try resolveLets(arena, program, &params, &params_expr, diag);
    if (opts.on_let) |h| for (program.stmts) |s| {
        if (s == .let_const and s.let_const.expr != null)
            if (params.get(s.let_const.name)) |v| h.f(h.ctx, s.let_const.name, v);
    };

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
        .connection => |c| {
            if (!analyze.isConnectionType(c.connector)) {
                const e = planErr(diag, try std.fmt.allocPrint(arena, "unknown connection type `{s}` (one of {s})", .{ c.connector, analyze.connection_types }));
                diag.pos = c.pos;
                return e;
            }
            if (analyze.sqlOptionProblem(c)) |why| {
                const e = planErr(diag, why);
                diag.pos = c.pos;
                return e;
            }
            try connections.put(c.name, c);
        },
        .func => |fd| try fns.put(fd.name, fd),
        .print, .binding => {},
        .output, .for_each, .match, .call, .explain => runnable += 1,
        .param, .kind, .let_const, .throw => {},
    };
    if (runnable == 0 and !opts.declarations_only)
        return planErr(diag, "nothing to run: the script needs a `LOAD INTO` or a terminal `SELECT`");

    const run_id: u64 = @intCast(std.time.milliTimestamp());
    var logger = obs.Logger.init(run_id, opts.log.format, if (opts.log.quiet) .err else opts.log.level);
    logger.quiet = opts.log.quiet;
    const t0 = std.time.milliTimestamp();
    var rows_read = obs.RowCounter.init(0);

    var errctx = op.ErrCtx{};
    errdefer if (errctx.msg.len > 0) {
        const at = diag.pos;
        setMsg(diag, errctx.msg);
        diag.pos = at;
    };

    var sources = std.array_list.Managed(driver.Source).init(arena);
    defer for (sources.items) |sc| sc.close();
    var buffer_decl: ?ast.BufferDecl = null;
    for (program.stmts) |s| {
        if (s == .kind) buffer_decl = s.kind.buffer;
    }
    var env = Env{ .arena = arena, .gpa = gpa, .params = &params, .bindings = &bindings, .connections = &connections, .sources = &sources, .request_body = opts.request_body, .diag = diag, .log = &logger, .params_expr = &params_expr, .errctx = &errctx, .rows_read = &rows_read, .json_params = &json_params, .buffer_decl = buffer_decl, .buffer_segment = opts.buffer_segment, .load_label_prefix = opts.load_label_prefix, .load_run_id = opts.load_run_id, .stdout_format = opts.stdout_format, .explain = opts.explain, .line_base = opts.line_base, .on_result = opts.on_result, .max_rows = opts.max_rows, .on_let = opts.on_let, .on_load = opts.on_load, .kind_name = @tagName(program.stmts[0].kind.kind), .fns = &fns };

    var cit = connections.valueIterator();
    while (cit.next()) |c| {
        try registerSftp(&env, c.*);
        try registerSmb(&env, c.*);
        try registerFtp(&env, c.*);
    }

    var batch_arena = std.heap.ArenaAllocator.init(gpa);
    defer batch_arena.deinit();

    var scan = driver.ScanTally{};
    env.scan = &scan;
    var loads = obs.LoadTally{};
    var facts_cache = env_mod.FactsCache.init(gpa);
    defer facts_cache.deinit();
    env.facts_cache = &facts_cache;
    env.loads = &loads;
    env.items = opts.items and manyLoads(program.stmts[1..]);

    var progress = obs.Progress{ .logger = &logger, .rows = &rows_read };
    if (opts.progress_hook) |h| {
        progress.mode = .hook;
        progress.hook = h;
        progress.start();
        env.progress = &progress;
    } else if (opts.progress) {
        progress.mode = if (opts.log.format == .json) .json else .line;
        progress.start();
        env.progress = &progress;
    }
    defer if (env.progress != null) progress.stop();

    env.script_scope = try buildScriptScope(arena, &params);

    var stats = Stats{ .run_id = run_id };
    var lanes_used: usize = 1;
    defer if (opts.summary_out) |so| {
        so.* = runSummary(&env, run_id, stats.rows_out, rows_read.load(.monotonic), @intCast(std.time.milliTimestamp() - t0), lanes_used, &loads, &scan, runnable == 1);
    };
    for (program.stmts[1..]) |s| switch (s) {
        .binding => |b| try bindings.put(b.name, try renderScriptScope(&env, b.pipeline)),
        .output => |p| {
            env.noteResult(if (p.show) "show" else "select", p.pos);
            try runOutput(&env, try renderScriptScope(&env, p), opts, &stats, &lanes_used, &batch_arena);
        },
        .explain => |e| {
            env.noteResult(if (e.mode == .describe) "describe" else "explain", e.pos);
            try runExplain(&env, .{ .mode = e.mode, .pipeline = try renderScriptScope(&env, e.pipeline), .pos = e.pos }, opts, &stats, &lanes_used, &batch_arena);
        },
        .for_each => |fe| try runForEach(&env, fe, opts, &stats, &lanes_used, &batch_arena, runForBody, &env.script_scope),
        .match => |m| try runStmtMatch(&env, m, opts, &stats, &lanes_used, &batch_arena),
        .print => |p| try runPrint(&env, p, no_loop_vars),
        .call => |c| try runCall(&env, c, no_loop_vars, opts, &stats, &lanes_used, &batch_arena, runForBody),
        .throw => |t| try runThrow(&env, t, no_loop_vars),
        .let_const => |l| if (l.query != null) try runScalarLet(&env, l),
        else => {},
    };

    stats.rows_read = rows_read.load(.monotonic);
    stats.elapsed_ms = @intCast(std.time.milliTimestamp() - t0);
    stats.source = env.src_name;
    stats.sink = env.sink_name;

    const summary = runSummary(&env, run_id, stats.rows_out, stats.rows_read, stats.elapsed_ms, lanes_used, &loads, &scan, runnable == 1);
    switch (opts.log.summary) {
        .json_stdout => if (env.wrote_sink) {
            var sbuf: [1024]u8 = undefined;
            var sfw = std.fs.File.stdout().writerStreaming(&sbuf);
            summary.renderJson(&sfw.interface) catch {};
            sfw.interface.flush() catch {};
        } else if (opts.log.format == .json) logger.summary(summary),
        .stderr => if (env.wrote_sink or opts.log.format == .json) logger.summary(summary),
        .none => {},
    }
    return stats;
}

fn runSummary(env: *const Env, run_id: u64, rows_written: u64, rows_read: u64, elapsed_ms: u64, lanes: usize, loads: *obs.LoadTally, scan: *driver.ScanTally, lone_load: bool) obs.Summary {
    return .{
        .run_id = run_id,
        .source = env.src_name,
        .sink = env.sink_name,
        .rows_read = rows_read,
        .rows_written = rows_written,
        .elapsed_ms = elapsed_ms,
        .threads = lanes,
        .loads = loads.ok.load(.monotonic),
        .loads_failed = loads.failed.load(.monotonic),
        .target = env.last_target,
        .rows_loaded = loads.rows.load(.monotonic),
        .lone_load = lone_load,
        .pushdown = .{
            .row_groups = scan.row_groups.load(.monotonic),
            .row_groups_skipped = scan.row_groups_skipped.load(.monotonic),
            .columns_read = scan.columns_read.load(.monotonic),
            .columns_total = scan.columns_total.load(.monotonic),
            .sql_filtered_reads = scan.sql_filters.load(.monotonic),
        },
    };
}

/// Equality for plan-time `match`. A typed-vs-literal mismatch (a `port:int` subject
/// vs a `"9030"` pattern) falls back to a textual compare instead of never matching.
fn valuesEqualLoose(arena: std.mem.Allocator, a: Value, b: Value) bool {
    if (eval.compareValues(a, b)) |ord| return ord == .eq;
    const as = eval.valueToString(arena, a) catch return false;
    const bs = eval.valueToString(arena, b) catch return false;
    return std.mem.eql(u8, as, bs);
}

/// The index of the first `match` arm whose pattern or guard holds (`_` matches), or
/// null. Shared by `runStmtMatch` and `runForMatch`; `ctx` prefixes eval errors.
pub fn matchArmIndex(env: *Env, m: ast.StmtMatch, ns: []const []const u8, vs: []const Value, ctx: []const u8) anyerror!?usize {
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

/// No matching arm (and no `_`) is a no-op.
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

fn runStmt(env: *Env, s: *const ast.Stmt, opts: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!void {
    env.diag.pos = null;
    switch (s.*) {
        .output => |p| {
            env.noteResult(if (p.show) "show" else "select", p.pos);
            try runOutput(env, try renderScriptScope(env, p), opts, stats, lanes_used, batch_arena);
        },
        .explain => |e| {
            env.noteResult(if (e.mode == .describe) "describe" else "explain", e.pos);
            try runExplain(env, .{ .mode = e.mode, .pipeline = try renderScriptScope(env, e.pipeline), .pos = e.pos }, opts, stats, lanes_used, batch_arena);
        },
        .for_each => |fe| try runForEach(env, fe, opts, stats, lanes_used, batch_arena, runForBody, &env.script_scope),
        .match => |mm| try runStmtMatch(env, mm, opts, stats, lanes_used, batch_arena),
        .print => |p| try runPrint(env, p, no_loop_vars),
        .call => |c| try runCall(env, c, no_loop_vars, opts, stats, lanes_used, batch_arena, runForBody),
        .throw => |t| try runThrow(env, t, no_loop_vars),
        .binding => |b| try env.bindings.put(b.name, try renderScriptScope(env, b.pipeline)),
        .connection => |c| {
            try env.connections.put(c.name, c);
            try registerSftp(env, c);
            try registerSmb(env, c);
            try registerFtp(env, c);
        },
        .let_const => |l| {
            const e = planErr(env.diag, try std.fmt.allocPrint(env.arena, "LET `{s}` must be declared at the top level of the script", .{l.name}));
            env.diag.pos = l.pos;
            return e;
        },
        .param => |pd| {
            const e = planErr(env.diag, try std.fmt.allocPrint(env.arena, "PARAM `{s}` must be declared at the top level of the script", .{pd.name}));
            env.diag.pos = pd.pos;
            return e;
        },
        .func => |fd| {
            const e = planErr(env.diag, try std.fmt.allocPrint(env.arena, "function `{s}` must be declared at the top level of the script", .{fd.name}));
            env.diag.pos = fd.pos;
            return e;
        },
        .kind => {},
    }
}

/// `LET x = (SELECT ...);`: run the query now and keep its single cell as `$x`. Also
/// the desugared form of a WHERE scalar subquery, which becomes a literal that rides
/// filter pushdown. One column required, zero rows is NULL, more than one an error.
fn runScalarLet(env: *Env, l: ast.LetConst) !void {
    const what: []const u8 = if (std.mem.startsWith(u8, l.name, "__scalar"))
        "scalar subquery"
    else
        try std.fmt.allocPrint(env.arena, "LET `{s}`", .{l.name});
    const pipe = try buildPipeline(env, l.query.?.stages);
    if (pipe.schema.fields.len != 1)
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "{s}: must produce exactly one column, got {d}", .{ what, pipe.schema.fields.len }));

    var scratch = std.heap.ArenaAllocator.init(env.gpa);
    defer scratch.deinit();

    var v: Value = .null;
    var rows: u64 = 0;
    var cur = pipe.op;
    while (try cur.next(scratch.allocator())) |b| {
        if (b.len > 0 and rows == 0) {
            v = try op.dupeValue(env.arena, b.columns[0].getValue(0));
        }
        rows += b.len;
        if (rows > 1)
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "{s}: returned more than one row", .{what}));
        _ = scratch.reset(.retain_capacity);
    }

    try env.params.put(l.name, v);
    try env.params_expr.put(l.name, try mkLit(env.arena, v));
    if (!std.mem.startsWith(u8, l.name, "__scalar")) if (env.on_let) |h| h.f(h.ctx, l.name, v);
    env.log.log(.debug, "LET {s} = scalar query result ({s})", .{ l.name, @tagName(std.meta.activeTag(v)) });
}

pub const runOutput = @import("run/output.zig").runOutput;
pub const runExplain = @import("run/explain.zig").runExplain;
pub const printPlanArrow = @import("run/explain.zig").printPlanArrow;
const resolveParams = @import("run/params.zig").resolveParams;
const resolveLets = @import("run/params.zig").resolveLets;

pub fn unionBranchesAreReads(u: ast.Union) bool {
    for (u.branches) |b| if (b.pipeline != null) return false;
    return true;
}

pub fn constEvalDefault(expr: *const ast.Expr, diag: *Diag) !Value {
    return switch (expr.*) {
        .int_lit => |i| .{ .int = i },
        .float_lit => |f| .{ .float = f },
        .str_lit => |s| .{ .string = s },
        .bool_lit => |b| .{ .bool = b },
        .null_lit => .null,
        else => planErr(diag, "param default must be a literal"),
    };
}

const describe_fields = [_]types.Schema.Field{
    .{ .name = "column", .ty = types.Type.init(.string) },
    .{ .name = "type", .ty = types.Type.init(.string) },
    .{ .name = "nullable", .ty = types.Type.init(.string) },
};

/// The rows `DESCRIBE` prints for `schema`, as CSV text (no header). The type is
/// quoted like the name, or `decimal(p,s)`'s comma would shift its scale a column.
pub fn describeRows(arena: std.mem.Allocator, schema: types.Schema) ![]const u8 {
    var text = std.array_list.Managed(u8).init(arena);
    for (schema.fields) |f| {
        try csv.writeField(text.writer(), f.name, ',');
        try text.append(',');
        try csv.writeField(text.writer(), try f.ty.name(arena), ',');
        try text.writer().print(",{s}\n", .{if (f.ty.nullable) "yes" else "no"});
    }
    return text.toOwnedSlice();
}

/// Open the source for its schema alone and print one row per column through the
/// ordinary stdout sink, so `--format json|csv` shape it like any result.
pub fn runDescribe(env: *Env, pipe: ast.Pipeline, stats: *Stats) anyerror!void {
    errdefer env.diag.stamp(pipe.pos);
    const arena = env.arena;
    const stages = pipe.stages[0 .. pipe.stages.len - 1];
    var ddiag = analyze.Diag{};
    env.csv_in = analyze.dialectFromHints(stages[0].hints, &ddiag) catch return planErr(env.diag, try arena.dupe(u8, ddiag.msg));
    env.fmt_in = analyze.formatFromHints(stages[0].hints, &ddiag) catch return planErr(env.diag, try arena.dupe(u8, ddiag.msg));
    if (stages[0].node == .read and stages[0].node.read.form == .path and std.mem.eql(u8, stages[0].node.read.connector, "csv"))
        try guardFileFormat(env, stages[0].node.read.form.path, env.fmt_in, "read");

    const base = env.sources.items.len;
    defer {
        for (env.sources.items[base..]) |sc| sc.close();
        env.sources.shrinkRetainingCapacity(base);
    }
    const res = try buildPipeline(env, stages);

    const schema = types.Schema{ .fields = &describe_fields };
    const batch = try csv.parseSlice(arena, &schema, try describeRows(arena, res.schema));
    const snk = try openSink(env, pipe.stages[pipe.stages.len - 1].node.write, schema);
    errdefer snk.abort();
    try snk.writeBatch(arena, batch);
    try snk.close();
    stats.rows_out += batch.len;
}

/// More than one `LOAD` to report: several statements, or a `FOR EACH`, `CASE` or
/// `CALL` that can run one repeatedly. A lone `LOAD` needs no item line.
fn manyLoads(stmts: []const ast.Stmt) bool {
    var n: usize = 0;
    for (stmts) |s| switch (s) {
        .output => n += 1,
        .for_each, .match, .call => return true,
        else => {},
    };
    return n > 1;
}

const LoadFacts = struct { rows: u64, rows_read: u64, elapsed_ms: u64, lanes: usize };

/// A ^C is not a failed load: the run says `aborted` itself.
pub fn noteLoad(env: *Env, pos: ?ast.Pos, err: ?anyerror, f: LoadFacts) void {
    const failed = err != null;
    if (failed and aborting()) return;
    if (env.loads) |t| {
        _ = (if (failed) &t.failed else &t.ok).fetchAdd(1, .monotonic);
        if (!failed) _ = t.rows.fetchAdd(f.rows, .monotonic);
    }
    const why: []const u8 = if (err) |e|
        (if (env.errctx.msg.len > 0) env.errctx.msg else if (env.diag.msg.len > 0) env.diag.msg else failLabel(e))
    else
        "";
    if (env.on_load) |h| {
        const p = pos orelse ast.Pos{ .line = 0, .col = 0 };
        const loop: @TypeOf(env.progress.?.loopState()) = if (env.progress) |pr| pr.loopState() else .{ .done = 0, .total = 0 };
        h.f(h.ctx, .{
            .ordinal = if (env.loads) |t| t.seq.fetchAdd(1, .monotonic) else 0,
            .target = env.last_target,
            .line = if (p.line > env.line_base) p.line - env.line_base else 0,
            .col = if (p.line > env.line_base) p.col else 0,
            .rows_read = f.rows_read,
            .rows_written = if (failed) 0 else f.rows,
            .elapsed_ms = f.elapsed_ms,
            .lanes = f.lanes,
            .ok = !failed,
            .reason = why,
            .transient = if (err) |e| isTransient(e) or env.diag.retryable else false,
            .loop_row = env.loop_row,
            .loop_rows = env.loop_rows,
            .loop_done = loop.done,
            .loop_total = loop.total,
        });
    }
    if (!env.items) return;
    if (failed) {
        env.log.item(.{ .target = env.last_target, .reason = why });
        env.item_reported = true;
        return;
    }
    env.log.item(.{ .target = env.last_target, .rows = f.rows, .elapsed_ms = f.elapsed_ms });
}

pub fn targetLabel(arena: std.mem.Allocator, w: ast.Write) ![]const u8 {
    if (std.mem.eql(u8, w.connector, "csv") or w.target.len == 0) return w.target;
    return std.fmt.allocPrint(arena, "{s}.{s}", .{ w.connector, w.target });
}

/// `source → sink` for the progress line, each as the script spelled it.
pub fn moveLabel(arena: std.mem.Allocator, first: ast.Stage, w: ast.Write) ![]const u8 {
    const src: []const u8 = switch (first.node) {
        .read => |rd| switch (rd.form) {
            .table => |q| try std.fmt.allocPrint(arena, "{s}.{s}", .{ rd.connector, try std.mem.join(arena, ".", q.parts) }),
            .path => |p| p,
            .query => try std.fmt.allocPrint(arena, "{s} query", .{rd.connector}),
            else => rd.connector,
        },
        .ref => |name| name,
        .union_ => "union",
        else => "?",
    };
    const dst = if (std.mem.eql(u8, w.connector, "stdout")) "stdout" else try targetLabel(arena, w);
    return std.fmt.allocPrint(arena, "{s} → {s}", .{ src, dst });
}

pub fn hasAggregate(stages: []const ast.Stage) bool {
    for (stages) |st| if (st.node == .aggregate) return true;
    return false;
}

pub fn hasSort(stages: []const ast.Stage) bool {
    for (stages) |st| if (st.node == .sort) return true;
    return false;
}

/// Run one row of a `for` body: each pipeline is rendered with the row's `${var}`
/// values; a nested loop's source is rendered with the outer row and chains to it.
pub fn runForBody(env: *Env, body: []const ast.Stmt, lr: LoopRow, opts: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!void {
    var saved = std.array_list.Managed(struct { name: []const u8, prev: ?ast.Pipeline }).init(env.arena);
    for (body) |st| if (st == .binding) try saved.append(.{ .name = st.binding.name, .prev = env.bindings.get(st.binding.name) });
    defer {
        var i = saved.items.len;
        while (i > 0) {
            i -= 1;
            const sv = saved.items[i];
            if (sv.prev) |p| env.bindings.put(sv.name, p) catch {} else _ = env.bindings.remove(sv.name);
        }
    }
    for (body) |*st| try runForStmt(env, st, lr, opts, stats, lanes_used, batch_arena);
}

fn runForStmt(env: *Env, s: *const ast.Stmt, lr: LoopRow, opts: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!void {
    switch (s.*) {
        .output => |p| {
            const pipe = try renderPipeline(env.arena, p, lr);
            try runOutput(env, pipe, opts, stats, lanes_used, batch_arena);
        },
        .explain => |e| {
            const pipe = try renderPipeline(env.arena, e.pipeline, lr);
            try runExplain(env, .{ .mode = e.mode, .pipeline = pipe, .pos = e.pos }, opts, stats, lanes_used, batch_arena);
        },
        .match => |m| try runForMatch(env, m, lr, opts, stats, lanes_used, batch_arena),
        .print => |p| try runPrint(env, p, lr),
        .call => |c| try runCall(env, c, lr, opts, stats, lanes_used, batch_arena, runForBody),
        .throw => |t| try runThrow(env, t, lr),
        .binding => |b| try env.bindings.put(b.name, try renderPipeline(env.arena, b.pipeline, lr)),
        .for_each => |fe| try runForEach(env, try renderForSource(env.arena, fe, lr), opts, stats, lanes_used, batch_arena, runForBody, &lr),
        .let_const => |l| {
            env.diag.stamp(l.pos);
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "LET `{s}` must be declared at the top level of the script", .{l.name}));
        },
        .param, .kind, .connection, .func => return planErr(env.diag, body_stmt_rule),
    }
}

/// The loop variables are bound over same-named params. Untyped ones bind as strings;
/// a `name:type` one binds the coerced value, so `port >= 1000` compares numerically.
fn runForMatch(env: *Env, m: ast.StmtMatch, lr: LoopRow, opts: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!void {
    var names = std.array_list.Managed([]const u8).init(env.arena);
    var values = std.array_list.Managed(Value).init(env.arena);
    try lr.appendLoopVars(env.arena, &names, &values);
    var it = env.params.iterator();
    while (it.next()) |kv| {
        try names.append(kv.key_ptr.*);
        try values.append(kv.value_ptr.*);
    }
    const idx = (try matchArmIndex(env, m, names.items, values.items, "for/match")) orelse return;
    try runForBody(env, m.arms[idx].body, lr, opts, stats, lanes_used, batch_arena);
}

test {
    _ = @import("env.zig");
    _ = @import("connect.zig");
    _ = @import("plan.zig");
    _ = @import("lanes.zig");
    _ = @import("script.zig");
}

test "DESCRIBE rows: name, engine type (decimal with its precision), nullable" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const fields = [_]types.Schema.Field{
        .{ .name = "id", .ty = types.Type.init(.int) },
        .{ .name = "amt", .ty = types.Type.decimal(10, 2).asNullable() },
        .{ .name = "a,b", .ty = types.Type.init(.string).asNullable() },
    };
    const rows = try describeRows(ar.allocator(), .{ .fields = &fields });
    try std.testing.expectEqualStrings("id,int,no\namt,\"decimal(10,2)\",yes\n\"a,b\",string,yes\n", rows);
}

test {
    _ = @import("run/explain.zig");
    _ = @import("run/output.zig");
    _ = @import("run/params.zig");
}
