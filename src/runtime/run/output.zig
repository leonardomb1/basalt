//! Running an output pipeline: bindings materialized ahead, the parallel paths tried
//! in order, else the serial driver, with its sink opened, written and closed.

const Batch = @import("../../exec/batch.zig").Batch;
const Env = @import("../env.zig").Env;
const RunOptions = @import("../env.zig").RunOptions;
const SplitCtx = @import("../connect.zig").SplitCtx;
const Stats = @import("../env.zig").Stats;
const aborting = @import("../env.zig").aborting;
const analyze = @import("../analyze.zig");
const ast = @import("../../lang/ast.zig");
const buildParallelSink = @import("../connect.zig").buildParallelSink;
const buildPipeline = @import("../plan.zig").buildPipeline;
const classifyAggPipeline = @import("../lanes.zig").classifyAggPipeline;
const classifyLaneShape = @import("../lanes.zig").classifyLaneShape;
const classifyMapJoinPipeline = @import("../lanes.zig").classifyMapJoinPipeline;
const classifyWholeAgg = @import("../lanes.zig").classifyWholeAgg;
const descendLeadingWhere = @import("../plan.zig").descendLeadingWhere;
const dupeSchema = @import("../connect.zig").dupeSchema;
const env_mod = @import("../env.zig");
const exceptColumns = @import("../connect.zig").exceptColumns;
const explainTree = @import("explain.zig").explainTree;
const factsIfWanted = @import("../connect.zig").factsIfWanted;
const forHintIdent = @import("../env.zig").forHintIdent;
const guardFileFormat = @import("../connect.zig").guardFileFormat;
const hasAggregate = @import("../run.zig").hasAggregate;
const hasFlagHint = @import("../env.zig").hasFlagHint;
const hasSort = @import("../run.zig").hasSort;
const inlineHeadBindings = @import("../plan.zig").inlineHeadBindings;
const isFolderRead = @import("../connect.zig").isFolderRead;
const isLocalCsvRead = @import("../connect.zig").isLocalCsvRead;
const isLocalParquetRead = @import("../connect.zig").isLocalParquetRead;
const laneEligible = @import("../lanes.zig").laneEligible;
const mem_connector = @import("../connect.zig").mem_connector;
const moveLabel = @import("../run.zig").moveLabel;
const noteLoad = @import("../run.zig").noteLoad;
const op = @import("../../exec/op.zig");
const openSink = @import("../connect.zig").openSink;
const openSource = @import("../connect.zig").openSource;
const openSplitSource = @import("../connect.zig").openSplitSource;
const parallel = @import("../parallel.zig");
const planErr = @import("../env.zig").planErr;
const planSplit = @import("../connect.zig").planSplit;
const projectSqlRead = @import("../connect.zig").projectSqlRead;
const projectedColumns = @import("../plan.zig").projectedColumns;
const pushdown = @import("../pushdown.zig");
const readReport = @import("../connect.zig").readReport;
const rebuildMapStages = @import("../plan.zig").rebuildMapStages;
const resolveUpsertKeys = @import("../connect.zig").resolveUpsertKeys;
const runCsvLane = @import("../lanes.zig").runCsvLane;
const runParallelSqlAgg = @import("../lanes.zig").runParallelSqlAgg;
const runParallelSqlMapJoin = @import("../lanes.zig").runParallelSqlMapJoin;
const runParquetLane = @import("../lanes.zig").runParquetLane;
const schemaPtr = @import("../env.zig").schemaPtr;
const sinkLabel = @import("../connect.zig").sinkLabel;
const std = @import("std");
const synthReconcile = @import("../plan.zig").synthReconcile;
const targetLabel = @import("../run.zig").targetLabel;
const topNStages = @import("../lanes.zig").topNStages;
const types = @import("../../lang/types.zig");
const unionBranchesAreReads = @import("../run.zig").unionBranchesAreReads;
const unionCanon = @import("../plan.zig").unionCanon;
const unionDownstreamMapOnly = @import("../plan.zig").unionDownstreamMapOnly;
const unionExceptNames = @import("../plan.zig").unionExceptNames;
const unionSpecs = @import("../plan.zig").unionSpecs;
const wholeAggStages = @import("../lanes.zig").wholeAggStages;

/// Run one output pipeline: split it into key-range lanes or stream it serially. A
/// fanned-out union reports per branch, so this outer call then counts nothing.
pub fn runOutput(env: *Env, out: ast.Pipeline, opts_in: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!void {
    errdefer env.diag.stamp(out.pos);
    const arena = env.arena;
    const stages = out.stages;
    if (stages.len == 0) return planErr(env.diag, "empty pipeline");
    const last = stages[stages.len - 1].node;
    if (last != .write) return planErr(env.diag, "a top-level pipeline must end in `write`");
    env.sink_name = sinkLabel(env, last.write);
    if (!env.explain and !std.mem.eql(u8, last.write.connector, "stdout")) env.wrote_sink = true;
    const is_load = !env.explain and !std.mem.eql(u8, last.write.connector, "stdout");
    if (is_load) env.last_target = try targetLabel(arena, last.write);
    const moving = env.progress != null and (is_load or env.progress.?.mode == .hook);
    if (moving) env.progress.?.begin(try moveLabel(arena, stages[0], last.write));
    const load_t0 = std.time.milliTimestamp();
    const load_rows0 = stats.rows_out;
    const read0 = env.rows_read.load(.monotonic);
    const lanes0 = lanes_used.*;
    lanes_used.* = 1;
    var load_err: ?anyerror = null;
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

/// A derived table or CTE that aggregates a file or table is run first, in parallel,
/// into memory, since built where it is read it ran on one thread. Returns the
/// binding's name when materialized, for the caller to drop afterwards.
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

/// A window (or full sort) over a file reads its input first, in parallel, into
/// memory, since built in place the read ran on one thread. Not when a filter follows
/// the window: `WHERE rn <= k` keeps only `k` rows a partition as they stream.
fn windowInput(env: *Env, opts: RunOptions, stages_in: []const ast.Stage, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!?[]const ast.Stage {
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
        .sort => if (wi + 1 < stages.len and stages[wi + 1].node == .limit and op.TopN.fits(stages[wi + 1].node.limit)) return null,
        else => return null,
    }
    if (env.materialized.contains(window_input)) return null;

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
    if (stages[0].node == .read and stages[0].node.read.form == .path and
        std.mem.eql(u8, stages[0].node.read.connector, "csv"))
        try guardFileFormat(env, stages[0].node.read.form.path, env.fmt_in, "read");
    if (std.mem.eql(u8, last.write.connector, "csv") and last.write.target.len > 0)
        try guardFileFormat(env, last.write.target, env.fmt_out, "write");
    const is_file_sink = std.mem.eql(u8, last.write.connector, "csv") or std.mem.eql(u8, last.write.connector, "stdout");
    if (is_file_sink)
        analyze.checkFileSink(last.write, stages[stages.len - 1].hints, &ddiag) catch |e| switch (e) {
            error.OutOfMemory => return e,
            error.AnalyzeFailed => return planErr(env.diag, try env.arena.dupe(u8, ddiag.msg)),
        };
    const sink_connector = if (is_file_sink) last.write.connector else if (env.connections.get(last.write.connector)) |c| c.connector else last.write.connector;
    analyze.checkSinkForm(last.write, sink_connector, &ddiag) catch |e| switch (e) {
        error.OutOfMemory => return e,
        error.AnalyzeFailed => return planErr(env.diag, try env.arena.dupe(u8, ddiag.msg)),
    };
    env.sink_label_prefix = analyze.hintText(stages[stages.len - 1].hints, "label_prefix");

    stages = try analyze.substFilterParams(arena, stages, env.params_expr);

    if (try pushdown.hoistFilters(arena, env.gpa, stages, env.bindings)) |hoisted| {
        env.log.log(.debug, "filter hoisted through a projection or join: {d} -> {d} stages", .{ stages.len, hoisted.len });
        stages = hoisted;
    }

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

    stages = try projectSqlRead(env, stages);

    stages = try descendLeadingWhere(env, stages);

    if (stages[0].node == .union_ and opts.threads > 1 and
        !std.mem.eql(u8, last.write.connector, "csv") and
        unionBranchesAreReads(stages[0].node.union_) and
        unionDownstreamMapOnly(stages[1 .. stages.len - 1]))
    {
        delegated.* = true;
        return runUnionSplit(env, stages[0].node.union_, stages[0].hints, stages[1 .. stages.len - 1], stages[stages.len - 1], opts, stats, lanes_used, batch_arena);
    }

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
        if (!whole_agg and hasAggregate(stages))
            env.log.log(.warn, "aggregate over {s} runs engine-side, not in the source ({s}); every matching row is streamed here to be grouped", .{ @tagName(env.sql_desc.?.dialect), why });
    }

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

/// Split-parallel union: each branch runs as its own split pipeline into the shared
/// sink; the first keeps the write mode (so `overwrite` truncates once), later ones append.
fn runUnionSplit(env: *Env, u: ast.Union, hints: []const ast.Hint, downstream: []const ast.Stage, write_stage: ast.Stage, opts: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!void {
    const arena = env.arena;
    const tag_col = forHintIdent(hints, "tag");
    const canon_opt = forHintIdent(hints, "canon");
    const specs = try unionSpecs(env, u, hints);
    if (specs.len == 0) return planErr(env.diag, "union has no source tables");

    const except = unionExceptNames(downstream);
    const schemas = try arena.alloc(types.Schema, specs.len);
    for (specs, schemas) |*s, *sch| {
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
