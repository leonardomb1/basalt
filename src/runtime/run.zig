//! Runtime entry: parse-tree to execution. `run` walks the statements; each
//! pipeline is planned (plan.zig), resolved to sources/sinks (connect.zig), and
//! either fanned out (lanes.zig) or driven serially here.

const std = @import("std");
const ast = @import("../lang/ast.zig");
const expand = @import("../lang/expand.zig");
const types = @import("../lang/types.zig");
const op = @import("../exec/op.zig");
const Batch = @import("../exec/batch.zig").Batch;
const column = @import("../exec/column.zig");
const eval = @import("../exec/eval.zig");
const driver = @import("../connect/driver.zig");
const sql = @import("../connect/sql.zig");
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
const forHintIdent = @import("env.zig").forHintIdent;
const hasFlagHint = @import("env.zig").hasFlagHint;
pub const isTransient = @import("env.zig").isTransient;
pub const ItemOutcome = @import("env.zig").ItemOutcome;
pub const LogConfig = @import("env.zig").LogConfig;
const LoopRow = @import("env.zig").LoopRow;
const loopValue = @import("env.zig").loopValue;
const mkLit = @import("env.zig").mkLit;
const no_loop_vars = @import("env.zig").no_loop_vars;
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
pub const SummaryMode = @import("env.zig").SummaryMode;
pub const takeReload = @import("env.zig").takeReload;

const buildParallelSink = @import("connect.zig").buildParallelSink;
const dupeSchema = @import("connect.zig").dupeSchema;
const guardFileFormat = @import("connect.zig").guardFileFormat;
const isLocalCsvRead = @import("connect.zig").isLocalCsvRead;
const isLocalParquetRead = @import("connect.zig").isLocalParquetRead;
const openSink = @import("connect.zig").openSink;
const openSource = @import("connect.zig").openSource;
const openSplitSource = @import("connect.zig").openSplitSource;
const planSplit = @import("connect.zig").planSplit;
const resolveUpsertKeys = @import("connect.zig").resolveUpsertKeys;
const sinkLabel = @import("connect.zig").sinkLabel;
const sqlConnInfo = @import("connect.zig").sqlConnInfo;
const SplitCtx = @import("connect.zig").SplitCtx;

const buildPipeline = @import("plan.zig").buildPipeline;
const rebuildMapStages = @import("plan.zig").rebuildMapStages;
const synthReconcile = @import("plan.zig").synthReconcile;
const unionCanon = @import("plan.zig").unionCanon;
const unionDownstreamMapOnly = @import("plan.zig").unionDownstreamMapOnly;
const unionSpecs = @import("plan.zig").unionSpecs;

const classifyAggPipeline = @import("lanes.zig").classifyAggPipeline;
const classifyLaneShape = @import("lanes.zig").classifyLaneShape;
const classifyMapJoinPipeline = @import("lanes.zig").classifyMapJoinPipeline;
const classifyWholeAgg = @import("lanes.zig").classifyWholeAgg;
const laneEligible = @import("lanes.zig").laneEligible;
const runCsvLane = @import("lanes.zig").runCsvLane;
const runParallelSqlAgg = @import("lanes.zig").runParallelSqlAgg;
const runParallelSqlMapJoin = @import("lanes.zig").runParallelSqlMapJoin;
const runParquetLane = @import("lanes.zig").runParquetLane;
const wholeAggStages = @import("lanes.zig").wholeAggStages;

const buildScriptScope = @import("script.zig").buildScriptScope;
const renderPipeline = @import("script.zig").renderPipeline;
const renderScriptScope = @import("script.zig").renderScriptScope;
const runCall = @import("script.zig").runCall;
const runForEach = @import("script.zig").runForEach;
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
        .print => {},
        // An EXPLAIN counts: a script whose only pipeline is explained is a complete
        // script, not one that forgot to write anywhere.
        .output, .for_each, .match, .call, .explain => runnable += 1,
        .param, .kind, .let_const, .throw => {},
    };
    if (runnable == 0)
        return planErr(diag, "no output pipeline (a pipeline ending in `write`)");

    const run_id: u64 = @intCast(std.time.milliTimestamp());
    var logger = obs.Logger.init(run_id, opts.log.format, if (opts.log.quiet) .err else opts.log.level);
    logger.quiet = opts.log.quiet;
    const t0 = std.time.milliTimestamp();
    var rows_read = std.atomic.Value(u64).init(0);

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
    var env = Env{ .arena = arena, .gpa = gpa, .params = &params, .bindings = &bindings, .connections = &connections, .sources = &sources, .request_body = opts.request_body, .diag = diag, .log = &logger, .params_expr = &params_expr, .errctx = &errctx, .rows_read = &rows_read, .json_params = &json_params, .buffer_decl = buffer_decl, .buffer_segment = opts.buffer_segment, .load_label_prefix = opts.load_label_prefix, .load_run_id = opts.load_run_id, .stdout_format = opts.stdout_format, .explain = opts.explain, .kind_name = @tagName(program.stmts[0].kind.kind), .fns = &fns };

    var batch_arena = std.heap.ArenaAllocator.init(gpa);
    defer batch_arena.deinit();

    env.script_scope = try buildScriptScope(arena, &params);

    // CTE bodies are registered by the pre-pass above, before script scope exists,
    // so a dynamic path inside one (`IDENTIFIER($data || '/t.parquet')`) would reach
    // the reader as a literal `${data}`. Render every binding with the same scope its
    // sibling `.output` statements get at the loop below.
    if (env.script_scope.names.len != 0) {
        var bit = bindings.iterator();
        while (bit.next()) |kv| kv.value_ptr.* = try renderScriptScope(&env, kv.value_ptr.*);
    }

    var stats = Stats{ .run_id = run_id };
    var lanes_used: usize = 1;
    for (program.stmts[1..]) |s| switch (s) {
        .output => |p| try runOutput(&env, try renderScriptScope(&env, p), opts, &stats, &lanes_used, &batch_arena),
        .explain => |e| try runExplain(&env, e, opts, &stats, &lanes_used, &batch_arena),
        .for_each => |fe| try runForEach(&env, fe, opts, &stats, &lanes_used, &batch_arena, runForBody),
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
        .output => |p| try runOutput(env, try renderScriptScope(env, p), opts, stats, lanes_used, batch_arena),
        .explain => |e| try runExplain(env, e, opts, stats, lanes_used, batch_arena),
        .for_each => |fe| try runForEach(env, fe, opts, stats, lanes_used, batch_arena, runForBody),
        .match => |mm| try runStmtMatch(env, mm, opts, stats, lanes_used, batch_arena),
        .print => |p| try runPrint(env, p, no_loop_vars),
        .call => |c| try runCall(env, c, no_loop_vars, opts, stats, lanes_used, batch_arena, runForBody),
        .throw => |t| try runThrow(env, t, no_loop_vars),
        .binding => |b| try env.bindings.put(b.name, try renderScriptScope(env, b.pipeline)),
        .connection => |c| try env.connections.put(c.name, c),
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
    env.log.log(.debug, "LET {s} = scalar query result ({s})", .{ l.name, @tagName(std.meta.activeTag(v)) });
}

pub fn runOutput(env: *Env, out: ast.Pipeline, opts: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!void {
    errdefer env.diag.stamp(out.pos);
    const arena = env.arena;
    const gpa = env.gpa;
    var stages = out.stages;
    if (stages.len == 0) return planErr(env.diag, "empty pipeline");
    const last = stages[stages.len - 1].node;
    if (last != .write) return planErr(env.diag, "a top-level pipeline must end in `write`");
    env.sink_name = sinkLabel(env, last.write);
    if (!env.explain and !std.mem.eql(u8, last.write.connector, "stdout")) env.wrote_sink = true;

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

    // Before the descent below: move whatever filters the join structure allows to
    // sit ahead of the joins, so the contiguous prefix `serialWhere` reads actually
    // contains them. Without this a join between the read and the WHERE meant no
    // predicate descended at all.
    if (try pushdown.hoistThroughJoins(arena, env.gpa, stages, env.bindings)) |hoisted| {
        env.log.log(.debug, "filter hoisted through join: {d} -> {d} stages", .{ stages.len, hoisted.len });
        stages = hoisted;
    }

    if (stages[0].node == .read) implicit: {
        const rd = stages[0].node.read;
        if (rd.form != .table and rd.form != .query) break :implicit;
        const conn = env.connections.get(rd.connector) orelse break :implicit;
        const d = (sqlConnInfo(conn) orelse break :implicit).dialect;
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
            if (isLocalParquetRead(rd)) {
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
        if (classifyWholeAgg(stages)) |shape| {
            if (try wholeAggStages(env, stages, shape, src_base)) |ns| {
                for (env.sources.items[src_base..]) |sc| sc.close();
                env.sources.shrinkRetainingCapacity(src_base);
                env.sql_desc = null;
                stages = ns;
                res = try buildPipeline(env, ns[0 .. ns.len - 1]);
                whole_agg = true;
            }
        }
    }

    if (!whole_agg and opts.threads > 1 and env.sql_desc != null) {
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
    std.fs.File.stderr().writeAll(aw.writer.buffered()) catch {};
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

test {
    _ = @import("env.zig");
    _ = @import("connect.zig");
    _ = @import("plan.zig");
    _ = @import("lanes.zig");
    _ = @import("script.zig");
    _ = @import("run_test.zig");
}

/// Run one row of a `for` body. The body is a statement block (a bare pipeline is a
/// one-statement block): each pipeline is rendered with the row's `${var}` values and
/// executed; a `match` branches on the loop variables and runs the winning arm.
pub fn runForBody(env: *Env, body: []const ast.Stmt, lr: LoopRow, opts: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!void {
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
        else => return planErr(env.diag, "a `for` or statement-function body may contain only pipelines, `CASE`, `CALL`, `PRINT`, `EXPLAIN` and `THROW` statements"),
    }
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
