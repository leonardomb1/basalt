//! Runtime entry: parse-tree to execution. `run` walks the statements; each
//! pipeline is planned (plan.zig), resolved to sources/sinks (connect.zig), and
//! either fanned out (lanes.zig) or driven serially here.

const std = @import("std");
const ast = @import("../lang/ast.zig");
const expand = @import("../lang/expand.zig");
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
/// `errLabel`, or the account the failing code left of it (`eval.explain`).
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
    // `EXPLAIN ANALYZE` runs serially. Ten parallel paths exist and each would
    // have to report its own lane figures; the one operator tree is the useful
    // artifact, and a plan is worthless if the shape of the pipeline decides
    // whether anything prints at all. The timings are therefore a serial
    // profile, which is what the tree has always claimed to be.
    var opts = opts_in;
    if (opts.explain) opts.threads = 1;

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
        .connection => |c| try connections.put(c.name, c),
        .func => |fd| try fns.put(fd.name, fd),
        .print, .binding => {},
        // An EXPLAIN counts: a script whose only pipeline is explained is a complete
        // script, not one that forgot to write anywhere.
        .output, .for_each, .match, .call, .explain => runnable += 1,
        .param, .kind, .let_const, .throw => {},
    };
    if (runnable == 0 and !opts.declarations_only)
        return planErr(diag, "no output pipeline (a pipeline ending in `write`)");

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
    // A source's `close` releases the socket and its gpa allocations — neither
    // owned by the plan arena — so a failed run used to leak an fd per open
    // connection. Under `serve` that is one leaked socket per failing request.
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
        // Under `--log-format json` the line becomes a once-a-second event.
        progress.mode = if (opts.log.format == .json) .json else .line;
        progress.start();
        env.progress = &progress;
    }
    defer if (env.progress != null) progress.stop();

    env.script_scope = try buildScriptScope(arena, &params);

    var stats = Stats{ .run_id = run_id };
    var lanes_used: usize = 1;
    // On every way out, a failed run's included: the loads before the failure
    // happened, and a caller showing them wants their totals.
    defer if (opts.summary_out) |so| {
        so.* = runSummary(&env, run_id, stats.rows_out, rows_read.load(.monotonic), @intCast(std.time.milliTimestamp() - t0), lanes_used, &loads, &scan, runnable == 1);
    };
    for (program.stmts[1..]) |s| switch (s) {
        // A `WITH` is registered where it stands, rendered with script scope like the
        // query that reads it: two statements may reuse a CTE name, and each must see
        // its own — registering them all up front made the last one win for both.
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
        // Expression LETs were folded before this loop; a query LET runs here,
        // in statement order, so it sees every binding declared above it and
        // every statement below it sees its value.
        .let_const => |l| if (l.query != null) try runScalarLet(&env, l),
        else => {},
    };

    stats.rows_read = rows_read.load(.monotonic);
    stats.elapsed_ms = @intCast(std.time.milliTimestamp() - t0);
    stats.source = env.src_name;
    stats.sink = env.sink_name;

    const summary = runSummary(&env, run_id, stats.rows_out, stats.rows_read, stats.elapsed_ms, lanes_used, &loads, &scan, runnable == 1);
    switch (opts.log.summary) {
        // `--format json`: a LOAD run's stdout is the summary object; a SELECT
        // run's stdout is the NDJSON rows — never both on one stream, so a
        // SELECT's summary goes to the log, and only a JSON log: the terminal's
        // printed table is a SELECT's own feedback.
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
        },
        // A LET is folded once, before anything runs; one nested in a branch would
        // silently miss that pass, so say so instead of resolving to nothing.
        .let_const => |l| return planErr(env.diag, try std.fmt.allocPrint(env.arena, "LET `{s}` must be declared at the top level of the script", .{l.name})),
        .param, .kind, .func => {},
    }
}

/// Run one output pipeline (ending in `write`): build it, then either split it
/// into parallel key-range lanes or stream it serially into the sink.
/// `LET x = (SELECT ...);` — run the query now, keep its single cell as the
/// constant `$x` substitutes to. Also the desugared form of a scalar subquery
/// in a WHERE: the parser lifts `(SELECT max(ts) FROM ...)` into an anonymous
/// query LET ahead of the statement, so by the time the outer pipeline plans,
/// the subquery is a literal — which is what lets the comparison ride the
/// ordinary filter pushdown to the source.
///
/// SQL scalar-subquery semantics: one column required, zero rows is NULL, more
/// than one row is an error.
fn runScalarLet(env: *Env, l: ast.LetConst) !void {
    // Inline scalar subqueries desugar to LETs with generated names; error
    // text should name what the user wrote, not the internal binding.
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
            // The batch dies with the scratch arena; string payloads must not.
            v = try op.dupeValue(env.arena, b.columns[0].getValue(0));
        }
        rows += b.len;
        if (rows > 1)
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "{s}: returned more than one row", .{what}));
        _ = scratch.reset(.retain_capacity);
    }

    try env.params.put(l.name, v);
    try env.params_expr.put(l.name, try mkLit(env.arena, v));
    // a desugared scalar subquery is the statement's own, not a name to keep
    if (!std.mem.startsWith(u8, l.name, "__scalar")) if (env.on_let) |h| h.f(h.ctx, l.name, v);
    env.log.log(.debug, "LET {s} = scalar query result ({s})", .{ l.name, @tagName(std.meta.activeTag(v)) });
}

pub fn runOutput(env: *Env, out: ast.Pipeline, opts_in: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!void {
    errdefer env.diag.stamp(out.pos);
    const arena = env.arena;
    const stages = out.stages;
    if (stages.len == 0) return planErr(env.diag, "empty pipeline");
    const last = stages[stages.len - 1].node;
    if (last != .write) return planErr(env.diag, "a top-level pipeline must end in `write`");
    env.sink_name = sinkLabel(env, last.write);
    if (!env.explain and !std.mem.eql(u8, last.write.connector, "stdout")) env.wrote_sink = true;
    // Only a real `LOAD`: a terminal SELECT prints rows to the same terminal.
    const is_load = !env.explain and !std.mem.eql(u8, last.write.connector, "stdout");
    if (is_load) env.last_target = try targetLabel(arena, last.write);
    // A terminal SELECT draws no line — its rows go to the same screen — but a
    // caller taking events wants to see one fill.
    const moving = env.progress != null and (is_load or env.progress.?.mode == .hook);
    if (moving) env.progress.?.begin(try moveLabel(arena, stages[0], last.write));
    const load_t0 = std.time.milliTimestamp();
    const load_rows0 = stats.rows_out;
    const read0 = env.rows_read.load(.monotonic);
    // Lanes are a running maximum; counted from 1 here, this load's own show,
    // and the maximum is restored on the way out.
    const lanes0 = lanes_used.*;
    lanes_used.* = 1;
    var load_err: ?anyerror = null;
    // A union that fans out runs one `runOutput` per branch, and those report
    // themselves; this outer call then counts nothing, or the rows count twice.
    var delegated = false;
    defer {
        if (moving) env.progress.?.end();
        const lanes = lanes_used.*;
        lanes_used.* = @max(lanes0, lanes);
        if (is_load and !delegated) noteLoad(env, out.pos, load_err, .{
            .rows = stats.rows_out - load_rows0,
            .rows_read = env.rows_read.load(.monotonic) - read0,
            .elapsed_ms = @intCast(std.time.milliTimestamp() - load_t0),
            .lanes = lanes,
        });
    }
    runOutputBody(env, opts_in, stages, last, stats, lanes_used, batch_arena, &delegated) catch |e| {
        load_err = e;
        return e;
    };
}

/// `runOutput` past the bookkeeping: plan and move the rows. Split off so the
/// caller sees the error a load failed with, for its report.
/// `SELECT ... FROM (SELECT k, SUM(v) FROM t GROUP BY k) x` — or the same through
/// a CTE: the binding is an aggregate over a file or table, which the parallel
/// paths only take when it is the pipeline itself. Built where it is read, it ran on
/// one thread whatever `-j` said. So it is run first, on those paths, into memory,
/// and the pipeline reads the rows back; an aggregate holds all its groups anyway,
/// so this keeps nothing a serial run would not. Returns the binding's name when it
/// was materialized, for the caller to drop afterwards.
fn materializeBinding(env: *Env, opts: RunOptions, stages: []const ast.Stage, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!?[]const u8 {
    if (opts.threads < 2 or env.explain or stages[0].node != .ref) return null;
    const name = stages[0].node.ref;
    if (env.materialized.contains(name)) return null;
    const b = env.bindings.get(name) orelse return null;
    if (b.stages.len < 2 or b.stages[0].node != .read) return null;
    for (b.stages) |st| {
        if (st.node == .aggregate) break;
    } else return null;

    const inner = try env.arena.alloc(ast.Stage, b.stages.len + 1);
    @memcpy(inner[0..b.stages.len], b.stages);
    const w = ast.Write{ .connector = mem_connector, .form = null, .target = "", .mode = .overwrite };
    inner[b.stages.len] = .{ .node = .{ .write = w }, .hints = &.{}, .pos = b.pos };
    var ms = env_mod.MemSink{ .arena = env.arena, .batches = std.array_list.Managed(Batch).init(env.arena) };
    const outer_sink = env.mem_sink;
    env.mem_sink = &ms;
    defer env.mem_sink = outer_sink;
    var scratch: Stats = .{};
    var delegated = false;
    try runOutputBody(env, opts, inner, inner[b.stages.len].node, &scratch, lanes_used, batch_arena, &delegated);
    try env.materialized.put(env.arena, name, .{ .schema = ms.schema, .batches = ms.batches.items });
    return name;
}

const window_input = "__window_input";

/// `read → (filter|select)* → window → …` over a file, or `… → ORDER BY → …`
/// past the top-N: the window, as the sort, holds every row before it emits one, so its input is read first, on the parallel paths, into memory
/// — the columns it and the stages after it use, in file order, as the shared
/// sink keeps it — and the window runs over that. Built in place, the read ran on
/// one thread whatever `-j` said. Not when a filter follows the window: that may
/// be `WHERE rn <= k`, whose window keeps only `k` rows a partition as they stream.
fn windowInput(env: *Env, opts: RunOptions, stages_in: []const ast.Stage, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!?[]const ast.Stage {
    // A window in a derived table or CTE (`… FROM (SELECT …, ROW_NUMBER() OVER …)`)
    // is a binding `inlineHeadBindings` keeps whole, as a filter must not cross
    // the window; laid end to end here only to be split at the window, nothing moves.
    var stages = stages_in;
    if (stages.len > 0 and stages[0].node == .ref) {
        if (env.materialized.contains(stages[0].node.ref)) return null;
        const b = env.bindings.get(stages[0].node.ref) orelse return null;
        if (b.stages.len == 0 or b.stages[0].node != .read) return null;
        const joined = try env.arena.alloc(ast.Stage, b.stages.len + stages.len - 1);
        @memcpy(joined[0..b.stages.len], b.stages);
        @memcpy(joined[b.stages.len..], stages[1..]);
        stages = joined;
    }
    if (opts.threads < 2 or env.explain or stages.len < 3 or !laneEligible(stages, opts)) return null;
    const rd = stages[0].node.read;
    if (!isLocalCsvRead(rd) and !isLocalParquetRead(rd) and !isFolderRead(rd)) return null;
    var wi: usize = 1;
    var narrowed = false;
    while (wi < stages.len - 1) : (wi += 1) switch (stages[wi].node) {
        .filter => {},
        .select => narrowed = true,
        else => break,
    };
    switch (stages[wi].node) {
        .window => |w| {
            if (w.top_k != null) return null;
            for (stages[wi + 1 ..]) |st| if (st.node == .filter) return null;
        },
        // a full sort holds every row too; ORDER BY with a small LIMIT is the
        // parallel top-N's already
        .sort => if (wi + 1 < stages.len and stages[wi + 1].node == .limit and op.TopN.fits(stages[wi + 1].node.limit)) return null,
        else => return null,
    }
    if (env.materialized.contains(window_input)) return null;

    // the source columns read past here: the names the window adds are its own
    // (the parser puts a `select` of exactly those ahead of the window already)
    var keep = std.array_list.Managed(ast.SelectItem).init(env.arena);
    if (!narrowed) if (try projectedColumns(env, stages[1..])) |cols| {
        names: for (cols) |c| {
            if (stages[wi].node == .window) for (stages[wi].node.window.funcs) |f| if (std.mem.eql(u8, f.out, c)) continue :names;
            const parts = try env.arena.alloc([]const u8, 1);
            parts[0] = c;
            try keep.append(.{ .field = .{ .parts = parts } });
        }
    };
    const inner = try env.arena.alloc(ast.Stage, wi + @as(usize, if (keep.items.len > 0) 2 else 1));
    @memcpy(inner[0..wi], stages[0..wi]);
    if (keep.items.len > 0) inner[wi] = .{ .node = .{ .select = keep.items }, .hints = &.{}, .pos = stages[wi].pos };
    const w = ast.Write{ .connector = mem_connector, .form = null, .target = "", .mode = .overwrite };
    inner[inner.len - 1] = .{ .node = .{ .write = w }, .hints = &.{}, .pos = stages[wi].pos };

    var ms = env_mod.MemSink{ .arena = env.arena, .batches = std.array_list.Managed(Batch).init(env.arena) };
    const outer_sink = env.mem_sink;
    env.mem_sink = &ms;
    defer env.mem_sink = outer_sink;
    var scratch: Stats = .{};
    var delegated = false;
    try runOutputBody(env, opts, inner, inner[inner.len - 1].node, &scratch, lanes_used, batch_arena, &delegated);
    try env.materialized.put(env.arena, window_input, .{ .schema = ms.schema, .batches = ms.batches.items });

    const outer = try env.arena.alloc(ast.Stage, stages.len - wi + 1);
    outer[0] = .{ .node = .{ .ref = window_input }, .hints = &.{}, .pos = stages[0].pos };
    @memcpy(outer[1..], stages[wi..]);
    return outer;
}

fn runOutputBody(env: *Env, opts_in: RunOptions, stages_in: []const ast.Stage, last: @FieldType(ast.Stage, "node"), stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator, delegated: *bool) anyerror!void {
    var opts = opts_in;
    const arena = env.arena;
    const gpa = env.gpa;
    var stages = stages_in;
    env.sort_threads = opts.threads;

    const mat = try materializeBinding(env, opts, stages, lanes_used, batch_arena);
    defer if (mat) |name| {
        _ = env.materialized.remove(name);
    };
    // A CTE, derived table or table-function call at the head reads through its
    // binding, which `buildPipeline` builds from the binding's own stages and then
    // the rest — the same rows as the stages laid end to end. Laid out here, the
    // read leads, so the descent below sends the binding's WHERE to the source:
    // through a binding, a SQL table used to be read whole and filtered here.
    stages = try inlineHeadBindings(env, stages);
    const win = try windowInput(env, opts, stages, lanes_used, batch_arena);
    defer if (win != null) {
        _ = env.materialized.remove(window_input);
    };
    if (win) |w| stages = w;

    var ddiag = analyze.Diag{};
    env.csv_in = analyze.dialectFromHints(stages[0].hints, &ddiag) catch
        return planErr(env.diag, try env.arena.dupe(u8, ddiag.msg));
    env.csv_out = analyze.dialectFromHints(stages[stages.len - 1].hints, &ddiag) catch
        return planErr(env.diag, try env.arena.dupe(u8, ddiag.msg));
    env.fmt_in = analyze.formatFromHints(stages[0].hints, &ddiag) catch
        return planErr(env.diag, try env.arena.dupe(u8, ddiag.msg));
    env.fmt_out = analyze.formatFromHints(stages[stages.len - 1].hints, &ddiag) catch
        return planErr(env.diag, try env.arena.dupe(u8, ddiag.msg));
    // `run` does not analyze the pipeline the way `check` does, so the guard that
    // stops an unreadable extension being parsed as CSV has to be applied here
    // too — this is the path that answered a zip's COUNT(*) with 46204.
    if (stages[0].node == .read and stages[0].node.read.form == .path and
        std.mem.eql(u8, stages[0].node.read.connector, "csv"))
        try guardFileFormat(env, stages[0].node.read.form.path, env.fmt_in, "read");
    if (std.mem.eql(u8, last.write.connector, "csv") and last.write.target.len > 0)
        try guardFileFormat(env, last.write.target, env.fmt_out, "write");

    // Every descent below translates filters to SQL, and a `$param` there must be
    // its value, not a column of that name.
    stages = try analyze.substFilterParams(arena, stages, env.params_expr);

    // Before the descent below: move whatever filters the join structure allows to
    // sit ahead of the joins, so the contiguous prefix `serialWhere` reads actually
    // contains them. Without this a join between the read and the WHERE meant no
    // predicate descended at all.
    if (try pushdown.hoistFilters(arena, env.gpa, stages, env.bindings)) |hoisted| {
        env.log.log(.debug, "filter hoisted through a projection or join: {d} -> {d} stages", .{ stages.len, hoisted.len });
        stages = hoisted;
    }

    // `LOAD INTO ... SPLIT BY (col) JOBS n` is written on the sink, but the split
    // is a property of the read: carry the key to the lead's hints — a read's, or
    // a union's, whose branches inherit them — where the planner looks, and let
    // JOBS set this pipeline's lane count (also inside a PARALLEL FOR EACH, whose
    // workers otherwise run single-lane: the person asked for lanes by name).
    if (stages[0].node == .read or stages[0].node == .union_) {
        var extra = std.array_list.Managed(ast.Hint).init(arena);
        for (stages[stages.len - 1].hints) |h| {
            if (std.mem.eql(u8, h.key, "split")) try extra.append(h);
            if (std.mem.eql(u8, h.key, "jobs") and h.value == .int and h.value.int > 0) {
                opts.threads = @intCast(h.value.int);
                env.sort_threads = opts.threads;
            }
        }
        if (extra.items.len > 0) {
            const with = try arena.dupe(ast.Stage, stages);
            try extra.appendSlice(stages[0].hints);
            with[0].hints = try extra.toOwnedSlice();
            stages = with;
        }
    }

    // Narrow a SQL table read to the columns the pipeline needs, before either
    // path (serial or split lanes) renders its SQL from the stage.
    stages = try projectSqlRead(env, stages);

    stages = try descendLeadingWhere(env, stages);

    // Split lanes re-read each branch's source by key range, so an arm that is a
    // query rather than a table read has nothing to split; the serial union builds it.
    if (stages[0].node == .union_ and opts.threads > 1 and
        !std.mem.eql(u8, last.write.connector, "csv") and
        unionBranchesAreReads(stages[0].node.union_) and
        unionDownstreamMapOnly(stages[1 .. stages.len - 1]))
    {
        delegated.* = true;
        return runUnionSplit(env, stages[0].node.union_, stages[0].hints, stages[1 .. stages.len - 1], stages[stages.len - 1], opts, stats, lanes_used, batch_arena);
    }

    // One classification, one eligibility check, one switch per source. This used to
    // be two structurally identical if-else chains — and a shape wired into one chain
    // and forgotten in the other is exactly how both 0.5.8 lane bugs happened: an
    // ungrouped aggregate fanned out over CSV and ran serially over parquet, and the
    // join-kind guard that three map paths applied was missing from the aggregate one.
    // `runParquetLane`/`runCsvLane` switch exhaustively over `LaneShape`, so adding a
    // shape is a compile error until both sources handle it.
    // A derived table or CTE is a binding reference at the head of the pipeline,
    // and the lanes want the read it wraps. Inline it, as `buildPipeline` will
    // anyway, so `COUNT(*) FROM (SELECT DISTINCT …)` and a CTE-fed aggregate fan
    // out instead of falling to the serial driver on the `.ref`.
    var head_stages = stages;
    var inlined: usize = 0;
    while (head_stages[0].node == .ref and inlined < 16) : (inlined += 1) {
        const b = env.bindings.get(head_stages[0].node.ref) orelse break;
        if (b.stages.len == 0) break;
        const joined = try arena.alloc(ast.Stage, b.stages.len + head_stages.len - 1);
        @memcpy(joined[0..b.stages.len], b.stages);
        @memcpy(joined[b.stages.len..], head_stages[1..]);
        head_stages = joined;
    }
    if (laneEligible(head_stages, opts)) {
        if (classifyLaneShape(head_stages)) |shape| {
            const rd = head_stages[0].node.read;
            if (isLocalParquetRead(rd) or isFolderRead(rd)) {
                if (try runParquetLane(env, head_stages, shape, last.write, opts, stats, lanes_used)) return;
            } else if (isLocalCsvRead(rd)) {
                if (try runCsvLane(env, head_stages, shape, last.write, opts, stats, lanes_used)) return;
            }
        }
    }

    env.sql_desc = null;
    env.src_name = "";
    const src_base = env.sources.items.len;
    var res = try buildPipeline(env, stages[0 .. stages.len - 1]);

    const wr = try resolveUpsertKeys(env, last.write);

    // Whole-aggregate descent takes precedence over splitting: one small grouped
    // result beats N range queries that each ship raw rows here to be folded. The
    // source schema is only knowable once the read is open, so the plain pipeline is
    // built first and thrown away when the descent is eligible — the same "open, plan,
    // reopen" the split path below does.
    var whole_agg = false;
    if (env.sql_desc != null and src_base < env.sources.items.len) {
        var why: []const u8 = "the pipeline is not read -> filter* -> aggregate";
        if (classifyWholeAgg(stages)) |shape| {
            if (try wholeAggStages(env, stages, shape, src_base, &why)) |ns| {
                for (env.sources.items[src_base..]) |sc| sc.close();
                env.sources.shrinkRetainingCapacity(src_base);
                env.sql_desc = null;
                stages = ns;
                res = try buildPipeline(env, ns[0 .. ns.len - 1]);
                whole_agg = true;
            }
        }
        // The single most expensive silent behaviour the engine has: every matching
        // source row ships here to be grouped. Say so, and why, where the run log is.
        if (!whole_agg and hasAggregate(stages))
            env.log.log(.warn, "aggregate over {s} runs engine-side, not in the source ({s}); every matching row is streamed here to be grouped", .{ @tagName(env.sql_desc.?.dialect), why });
    }

    // A LIMIT, and the ORDER BY before it, descend the same way — built, checked
    // against the source schema, rebuilt — so "the latest 1000 rows" asks the
    // source for 1000 rather than streaming the table here to be sorted. The
    // pushed read is one capped statement, so the split below is skipped.
    var top_n = false;
    if (!whole_agg and env.sql_desc != null and src_base < env.sources.items.len) {
        var why: []const u8 = "";
        const dialect = env.sql_desc.?.dialect;
        if (try topNStages(env, stages, src_base, &why)) |ns| {
            for (env.sources.items[src_base..]) |sc| sc.close();
            env.sources.shrinkRetainingCapacity(src_base);
            env.sql_desc = null;
            stages = ns;
            res = try buildPipeline(env, ns[0 .. ns.len - 1]);
            top_n = true;
        } else if (why.len > 0 and hasSort(stages))
            env.log.log(.warn, "ORDER BY ... LIMIT over {s} sorts engine-side, not in the source ({s}); every matching row is streamed here to be sorted", .{ @tagName(dialect), why });
    }

    if (!whole_agg and !top_n and opts.threads > 1 and env.sql_desc != null) {
        if (stages[0].node == .read) {
            if (classifyAggPipeline(stages)) |shape| {
                if (try runParallelSqlAgg(env, stages, shape.prefix, shape.ag, shape.tail, wr, opts, stats, lanes_used, src_base)) return;
            } else if (classifyMapJoinPipeline(stages)) |js| {
                // A join is not linearizable, so the map split path below never sees
                // this shape — it fans out over the same key ranges from here instead.
                if (try runParallelSqlMapJoin(env, stages, js, wr, opts, stats, lanes_used, src_base)) return;
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
                    const facts = try factsIfWanted(env, stages[0].node.read, env.sql_desc.?.dialect, middle, src_schema, true, .superset);
                    const mp = try pushdown.planMapWith(arena, env.sql_desc.?.dialect, src_schema, middle, out_cols, facts);
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
                var ctx = SplitCtx{ .gpa = gpa, .kind = env.sql_desc.?.kind, .cfg = env.sql_desc.?.cfg, .base_sql = sp.base_sql, .proj_select = proj_select, .where_extra = where_extra, .report = try readReport(env, @tagName(env.sql_desc.?.kind)) };
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
        var batches = std.array_list.Managed(Batch).init(arena);
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

    // Order matters: defers run LIFO, so the arenas must be declared FIRST and
    // `shutdown` (which joins the writer thread) LAST. Reversed, an error or a
    // ^C between `submit` and `finish` freed the batch arenas while the writer
    // was still serializing the batch out of them.
    var ping_pong: [2]std.heap.ArenaAllocator = .{ std.heap.ArenaAllocator.init(gpa), std.heap.ArenaAllocator.init(gpa) };
    defer for (&ping_pong) |*a| a.deinit();
    var pw = parallel.PipelinedSink{ .snk = snk, .gpa = gpa };
    try pw.start();
    defer pw.shutdown();
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

/// `EXPLAIN [ANALYZE] <query>;` where it stands — explained against the connections,
/// CTE bindings, params and LETs the statements above it put in scope, which is the
/// whole point of the statement form over the program-level prefix.
///
/// `ANALYZE` is the ordinary pipeline run with the sink discarded (`env.explain`) and
/// serially (a plan is worthless if the shape of the pipeline decides what prints), so
/// it prints exactly the operator tree a whole-script `EXPLAIN ANALYZE` prints. The
/// plain form executes nothing and renders the static plan — the same IR `basalt
/// check` builds, through the same `analyze.render`. Both are scoped to this one
/// statement: everything before and after it runs normally, at full parallelism.
pub fn runExplain(env: *Env, e: ast.ExplainStmt, opts: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!void {
    if (e.mode == .describe) return runDescribe(env, e.pipeline, stats);
    if (e.mode == .analyze) {
        const outer = env.explain;
        env.explain = true;
        defer env.explain = outer;
        var o = opts;
        o.explain = true;
        o.threads = 1;
        return runOutput(env, e.pipeline, o, stats, lanes_used, batch_arena);
    }

    var adiag = analyze.Diag{};
    const plan = analyze.analyzeOne(env.arena, env.kind_name, e.pipeline, env.bindings, env.connections, env.params_expr, &adiag) catch |err| switch (err) {
        error.OutOfMemory => return err,
        // `adiag`'s message lives in its own stack buffer; copy it out before it goes.
        error.AnalyzeFailed => return planErr(env.diag, try env.arena.dupe(u8, adiag.msg)),
    };

    // stderr, not stdout: stdout is the data contract (NDJSON rows under
    // `--format json`, or the summary object), and a script may run pipelines
    // that write there before or after this statement. The whole-script
    // `EXPLAIN <script>` prefix still prints to stdout — there it IS the
    // invocation's only output. This also matches EXPLAIN ANALYZE, whose
    // operator tree already goes to stderr.
    //
    // Rendered into memory and handed to ONE `writeAll`, the way `obs.Logger`
    // writes: a `File.Writer` opened on stderr mid-run starts its own position
    // at zero, so when stderr is a regular file it overwrites whatever the
    // logger already wrote there instead of appending.
    var aw = std.Io.Writer.Allocating.init(env.gpa);
    defer aw.deinit();
    try analyze.render(plan, &aw.writer);
    // Arrow output is for a program, which cannot read stderr as a result: there
    // the plan is a result of its own, a `plan` column of one row per line.
    if (env.stdout_format == .arrow) {
        const info = env.takeResult();
        const n = try printPlanArrow(env.gpa, aw.writer.buffered(), info);
        stats.rows_out += n;
        if (env.on_result) |h| h.call(.{ .info = info, .rows = n });
        return;
    }
    std.fs.File.stderr().writeAll(aw.writer.buffered()) catch {};
}

/// Writes a rendered plan to stdout as an Arrow stream with one string column,
/// `plan`, one row per line. Returns the row count.
pub fn printPlanArrow(gpa: std.mem.Allocator, text: []const u8, info: arrow.ResultInfo) !usize {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    const fields = [_]types.Schema.Field{.{ .name = "plan", .ty = .{ .kind = .string } }};
    const schema = types.Schema{ .fields = &fields };
    var b = column.Builder.init(a, fields[0].ty);
    var it = std.mem.splitScalar(u8, std.mem.trimRight(u8, text, "\n"), '\n');
    var n: usize = 0;
    while (it.next()) |ln| : (n += 1) try b.append(.{ .string = ln });
    const cols = try a.alloc(column.Column, 1);
    cols[0] = try b.finish();
    const w = try arrow.ArrowWriter.open(gpa, schema, info);
    errdefer w.abort();
    try w.writeBatch(a, .{ .schema = &schema, .columns = cols, .len = n });
    try w.close();
    return n;
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

fn unionBranchesAreReads(u: ast.Union) bool {
    for (u.branches) |b| if (b.pipeline != null) return false;
    return true;
}

/// Split-parallel union: expand each branch into a `read | select(reconcile) |
/// <downstream maps> | write` pipeline and run it through runOutput, which
/// split-reads the single branch source into key-range lanes. Branches share the
/// sink — the first keeps the write mode (so `overwrite` truncates once), later
/// branches append/upsert into it.
fn runUnionSplit(env: *Env, u: ast.Union, hints: []const ast.Hint, downstream: []const ast.Stage, write_stage: ast.Stage, opts: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!void {
    const arena = env.arena;
    const tag_col = forHintIdent(hints, "tag");
    const canon_opt = forHintIdent(hints, "canon");
    const specs = try unionSpecs(env, u, hints);
    if (specs.len == 0) return planErr(env.diag, "union has no source tables");

    const except = unionExceptNames(downstream);
    const schemas = try arena.alloc(types.Schema, specs.len);
    for (specs, schemas) |*s, *sch| {
        // The shape only: no rows are wanted here, and with an EXCEPT in play the
        // branch is then asked for every column but those, by name.
        if (except.len > 0) {
            if (try exceptColumns(env, s.read, hints, except)) |cols| s.read.cols = cols;
        }
        var probe = s.read;
        probe.where = "1 = 0";
        const src = try openSource(env, probe, hints);
        sch.* = try dupeSchema(arena, src.schema());
        src.close();
    }
    const canon = try unionCanon(env, specs, schemas, canon_opt, except);

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
                // the declared type, as a bound value gets it: `PARAM d DATE
                // DEFAULT '2026-01-01'` is a DATE, not the text of one
                const raw = try constEvalDefault(d, diag);
                v = switch (p.ty.kind) {
                    .date, .time, .timestamp, .decimal => if (raw == .null) raw else eval.castValueTyped(arena, raw, p.ty) catch
                        return planErr(diag, try std.fmt.allocPrint(arena, "PARAM `{s}`: `{s}` is not a {s}", .{ p.name, if (raw == .string) raw.string else "its DEFAULT", try p.ty.name(arena) })),
                    else => raw,
                };
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

        // A query LET has no expression to fold here — its value comes from
        // running the query, which happens in statement order in the main loop
        // (`runScalarLet`), after the bindings it may reference exist.
        const le = l.expr orelse continue;
        const v = eval.constEval(arena, le, names.items, values.items) catch |e|
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
        // The text a CAST takes, read as a CAST reads it — so `-p d=2026-02-01`
        // binds exactly what `CAST('2026-02-01' AS DATE)` would, a date alone
        // reaches a TIMESTAMP as its midnight, and a decimal rounds as a cast does.
        .date, .time, .timestamp, .decimal => eval.castValueTyped(arena, .{ .string = str }, ty) catch
            planErr(diag, try std.fmt.allocPrint(arena, "invalid {s} param value `{s}` — expected {s}", .{ try ty.name(arena), str, switch (ty.kind) {
                .date => "YYYY-MM-DD",
                .time => "HH:MM[:SS[.ffffff]]",
                .timestamp => "YYYY-MM-DD[ HH:MM:SS[.ffffff]]",
                else => "a number",
            } })),
        else => planErr(diag, try std.fmt.allocPrint(arena, "a {s} param cannot be bound from text", .{try ty.name(arena)})),
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

test {
    _ = @import("env.zig");
    _ = @import("connect.zig");
    _ = @import("plan.zig");
    _ = @import("lanes.zig");
    _ = @import("script.zig");
    _ = @import("run_test.zig");
}

const describe_fields = [_]types.Schema.Field{
    .{ .name = "column", .ty = types.Type.init(.string) },
    .{ .name = "type", .ty = types.Type.init(.string) },
    .{ .name = "nullable", .ty = types.Type.init(.string) },
};

/// The rows `DESCRIBE` prints for `schema`, as CSV text (no header): one per
/// column, its engine type spelled the way `EXPLAIN` spells it, `yes`/`no`.
pub fn describeRows(arena: std.mem.Allocator, schema: types.Schema) ![]const u8 {
    var text = std.array_list.Managed(u8).init(arena);
    for (schema.fields) |f| {
        try csv.writeField(text.writer(), f.name, ',');
        try text.append(',');
        // `decimal(p,s)` holds the delimiter: quoted like the name, or its scale
        // lands in the `nullable` column.
        try csv.writeField(text.writer(), try f.ty.name(arena), ',');
        try text.writer().print(",{s}\n", .{if (f.ty.nullable) "yes" else "no"});
    }
    return text.toOwnedSlice();
}

/// `DESCRIBE`: open the source for its schema alone — the engine's types, which
/// are what a sink would receive — and print one row per column through the
/// ordinary stdout sink, so `--format json|csv` shape it like any result.
fn runDescribe(env: *Env, pipe: ast.Pipeline, stats: *Stats) anyerror!void {
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

/// Does the script have more than one `LOAD` to report — several statements, or
/// anything (`FOR EACH`, `CASE`, `CALL`) that can run one more than once? A lone
/// `LOAD` is summed up by the closing sentence and needs no item line.
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

/// Count a finished `LOAD`, report it when the run is showing item lines, and
/// hand it to the caller's `on_load`.
fn noteLoad(env: *Env, pos: ?ast.Pos, err: ?anyerror, f: LoadFacts) void {
    const failed = err != null;
    // A ^C is not a failed load; the run is about to say `aborted` itself.
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

/// The write target as the script spelled it: a path, or `conn.table`.
fn targetLabel(arena: std.mem.Allocator, w: ast.Write) ![]const u8 {
    if (std.mem.eql(u8, w.connector, "csv") or w.target.len == 0) return w.target;
    return std.fmt.allocPrint(arena, "{s}.{s}", .{ w.connector, w.target });
}

/// `source → sink` for the progress line: the table, path or binding being read,
/// and the target being written, each as the script spelled it.
fn moveLabel(arena: std.mem.Allocator, first: ast.Stage, w: ast.Write) ![]const u8 {
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

fn hasAggregate(stages: []const ast.Stage) bool {
    for (stages) |st| if (st.node == .aggregate) return true;
    return false;
}

fn hasSort(stages: []const ast.Stage) bool {
    for (stages) |st| if (st.node == .sort) return true;
    return false;
}

/// Run one row of a `for` body. The body is a statement block (a bare pipeline is a
/// one-statement block): each pipeline is rendered with the row's `${var}` values and
/// executed; a `match` branches on the loop variables and runs the winning arm.
pub fn runForBody(env: *Env, body: []const ast.Stmt, lr: LoopRow, opts: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!void {
    // A body's `WITH`/derived table is scoped to the body: whatever the name meant
    // outside comes back once the row is done, so the next row (or the top level)
    // never reads a stale per-row pipeline.
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
        // A `WITH`/derived table inside a body is rendered with the row like the
        // pipeline that reads it, so a per-row reconciliation can join.
        .binding => |b| try env.bindings.put(b.name, try renderPipeline(env.arena, b.pipeline, lr)),
        // A nested loop: its discovery source is rendered with the outer row, and
        // every inner row chains to it so `${outer}` still resolves in the body.
        .for_each => |fe| try runForEach(env, try renderForSource(env.arena, fe, lr), opts, stats, lanes_used, batch_arena, runForBody, &lr),
        .let_const => |l| {
            env.diag.stamp(l.pos);
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "LET `{s}` must be declared at the top level of the script", .{l.name}));
        },
        .param, .kind, .connection, .func => return planErr(env.diag, body_stmt_rule),
    }
}

/// A `match` evaluated per row of a `for`: the loop variables are bound (shadowing
/// same-named params), so a guard like `pk == ""` picks a branch. Untyped variables
/// bind as strings; a `name:type` annotation binds the coerced value, so a guard like
/// `port >= 1000` compares numerically rather than lexically.
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
