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
const buildProject = @import("../plan.zig").buildProject;
const buildStage = @import("../plan.zig").buildStage;
const driver = @import("../../connect/driver.zig");
const descendLeadingWhere = @import("../plan.zig").descendLeadingWhere;
const dupeSchema = @import("../connect.zig").dupeSchema;
const env_mod = @import("../env.zig");
const exceptColumns = @import("../connect.zig").exceptColumns;
const factsIfWanted = @import("../connect.zig").factsIfWanted;
const forHintIdent = @import("../env.zig").forHintIdent;
const ftp = @import("../../store/ftp.zig");
const guardFileFormat = @import("../connect.zig").guardFileFormat;
const hasAggregate = @import("../run.zig").hasAggregate;
const hasFlagHint = @import("../env.zig").hasFlagHint;
const hasSort = @import("../run.zig").hasSort;
const inlineHeadBindings = @import("../plan.zig").inlineHeadBindings;
const stampedBinding = @import("../plan.zig").stampedBinding;
const keysPreferSerial = @import("../plan.zig").keysPreferSerial;
const localRead = @import("../connect/source.zig").localRead;
const mem_connector = @import("../connect.zig").mem_connector;
const moveLabel = @import("../run.zig").moveLabel;
const noteLoad = @import("../run.zig").noteLoad;
const notePlan = @import("explain.zig").notePlan;
const op = @import("../../exec/op.zig");
const openSink = @import("../connect.zig").openSink;
const opensPerLane = @import("../connect.zig").opensPerLane;
const openSource = @import("../connect.zig").openSource;
const openSplitSource = @import("../connect.zig").openSplitSource;
const parallel = @import("../parallel.zig");
const path = @import("../path.zig");
const aErr = @import("../env.zig").aErr;
const planErr = @import("../env.zig").planErr;
const planSplit = @import("../connect.zig").planSplit;
const printProfile = @import("explain.zig").printProfile;
const Profile = @import("../../exec/profile.zig").Profile;
const profile = @import("../../exec/profile.zig");
const projectSqlRead = @import("../connect.zig").projectSqlRead;
const projectedColumns = @import("../plan.zig").projectedColumns;
const pruneKnown = @import("../plan.zig").pruneKnown;
const pushdown = @import("../pushdown.zig");
const readReport = @import("../connect.zig").readReport;
const rebuildMapStages = @import("../plan.zig").rebuildMapStages;
const resolveUpsertKeys = @import("../connect.zig").resolveUpsertKeys;
const runCsvLane = @import("../lanes.zig").runCsvLane;
const runParallelSqlAgg = @import("../lanes.zig").runParallelSqlAgg;
const runParallelSqlMapJoin = @import("../lanes.zig").runParallelSqlMapJoin;
const runParquetLane = @import("../lanes.zig").runParquetLane;
const SinkGuard = @import("../../connect/guard.zig").SinkGuard;
const schemaPtr = @import("../env.zig").schemaPtr;
const sinkLabel = @import("../connect.zig").sinkLabel;
const std = @import("std");
const msgfmt = @import("../../format/msgfmt.zig");
const synthReconcile = @import("../plan.zig").synthReconcile;
const targetLabel = @import("../run.zig").targetLabel;
const topNStages = @import("../lanes.zig").topNStages;
const types = @import("../../lang/types.zig");
const unionCanon = @import("../plan.zig").unionCanon;
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
    if (last != .write) return planErr(env.diag, "a top-level query must be a `LOAD INTO` or a terminal `SELECT`");
    env.sink_name = sinkLabel(env, last.write);
    if (!env.explain and !std.mem.eql(u8, last.write.connector, "stdout")) env.wrote_sink = true;
    const is_load = !env.explain and !std.mem.eql(u8, last.write.connector, "stdout");
    if (is_load) env.last_target = try targetLabel(arena, last.write);
    defer env.endStatement(is_load);
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
            .elapsed_ms = @intCast(@max(0, std.time.milliTimestamp() - load_t0)),
            .lanes = lanes,
        });
    }
    var prof: Profile = undefined;
    const own_profile = opts_in.explain and env.profile == null;
    if (own_profile) {
        prof = Profile.init(env.gpa);
        env.profile = &prof;
    }
    defer if (own_profile) {
        env.profile = null;
        prof.deinit();
    };
    const t0 = std.time.Instant.now() catch null;
    const scan0 = scanCounts(env);
    const held0: u64 = if (own_profile) if (env.space) |sp| sp.restartPeak() else 0 else 0;
    runOutputBody(env, opts_in, stages, last, stats, lanes_used, batch_arena, &delegated) catch |e| {
        load_err = e;
        return e;
    };
    if (own_profile) {
        const scan1 = scanCounts(env);
        try printProfile(env, &prof, .{
            .wall_ns = if (t0) |s| if (std.time.Instant.now()) |now| now.since(s) else |_| 0 else 0,
            .rows_read = env.rows_read.load(.monotonic) - read0,
            .rows_out = stats.rows_out - load_rows0,
            .lanes = lanes_used.*,
            .row_groups = scan1[0] - scan0[0],
            .row_groups_skipped = scan1[1] - scan0[1],
            .columns_read = scan1[2] - scan0[2],
            .columns_total = scan1[3] - scan0[3],
            .spill_peak = if (env.space) |sp| sp.peak() -| held0 else 0,
            .sink = std.mem.trimRight(u8, try msgfmt.allocPrint(arena, "{s} {s}", .{ env.sink_name, try targetLabel(arena, last.write) }), " "),
        });
    }
    if (!is_load and !env.explain) if (env.max_rows) |cap| {
        stats.rows_out = load_rows0 + @min(stats.rows_out - load_rows0, cap);
    };
}

/// The run's read counts so far: row groups read and skipped, columns read and offered.
fn scanCounts(env: *Env) [4]u64 {
    const t = env.scan orelse return .{ 0, 0, 0, 0 };
    return .{ t.row_groups.load(.monotonic), t.row_groups_skipped.load(.monotonic), t.columns_read.load(.monotonic), t.columns_total.load(.monotonic) };
}

/// A derived table or CTE that aggregates a file or table is run first, in parallel,
/// into memory, since built where it is read it ran on one thread. Returns the
/// binding's name when materialized, for the caller to drop afterwards.
fn materializeBinding(env: *Env, opts: RunOptions, stages: []const ast.Stage, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!?[]const u8 {
    if (stages[0].node != .ref) return null;
    const name = stages[0].node.ref;
    if (env.materialized.contains(name)) return null;
    const b = (try stampedBinding(env, name)) orelse return null;
    if (!path.materializes(b.stages)) return null;

    const inner = try env.arena.alloc(ast.Stage, b.stages.len + 1);
    @memcpy(inner[0..b.stages.len], b.stages);
    const w = ast.Write{ .connector = mem_connector, .form = null, .target = "", .mode = .overwrite };
    inner[b.stages.len] = .{ .node = .{ .write = w }, .hints = &.{}, .pos = b.pos };
    var ms = env_mod.MemSink{ .arena = env.shared orelse env.arena, .batches = std.array_list.Managed(Batch).init(env.shared orelse env.arena) };
    const outer_sink = env.mem_sink;
    const outer_name = env.sink_name;
    env.mem_sink = &ms;
    defer env.mem_sink = outer_sink;
    defer env.sink_name = outer_name;
    var scratch: Stats = .{};
    var delegated = false;
    {
        const outer_scope = enterScope(env, name);
        defer leaveScope(env, outer_scope);
        try runOutputBody(env, opts, inner, inner[b.stages.len].node, &scratch, lanes_used, batch_arena, &delegated);
    }
    try notePrelude(env, name, stages[0].pos);
    try env.materialized.put(env.arena, name, .{ .schema = ms.schema, .batches = ms.batches.items });
    return name;
}

/// Under `EXPLAIN ANALYZE`, a run inside the pipeline's (an input read into memory
/// first) is profiled in its own scope, its operators apart from the pipeline's
/// built from the same stages; returns the scope to restore.
fn enterScope(env: *Env, what: []const u8) u64 {
    const p = env.profile orelse return 0;
    const outer = p.scope;
    p.scope = std.hash.Wyhash.hash(outer, what);
    return outer;
}

fn leaveScope(env: *Env, outer: u64) void {
    if (env.profile) |p| p.scope = outer;
}

/// Under `EXPLAIN ANALYZE`, the path names the input read into memory first and how,
/// until the pipeline over it names its own, and the scan at `pos` over it is named.
fn notePrelude(env: *Env, what: []const u8, pos: ast.Pos) !void {
    const p = env.profile orelse return;
    p.path = try msgfmt.allocPrint(env.arena, "{s} read into memory first ({s}), then serial", .{ what, p.path });
    try p.describe(profile.keyOf(pos), .scan, try msgfmt.allocPrint(env.arena, "{s}, in memory", .{what}));
}

const window_input = "__window_input";

/// A window (or full sort) over a file reads its input first, in parallel, into
/// memory, since built in place the read ran on one thread (`path.windowInputAt`).
fn windowInput(env: *Env, opts: RunOptions, stages_in: []const ast.Stage, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!?[]const ast.Stage {
    var stages = stages_in;
    if (stages.len > 0 and stages[0].node == .ref) {
        if (env.materialized.contains(stages[0].node.ref)) return null;
        const b = (try stampedBinding(env, stages[0].node.ref)) orelse return null;
        if (b.stages.len == 0 or b.stages[0].node != .read) return null;
        const joined = try env.arena.alloc(ast.Stage, b.stages.len + stages.len - 1);
        @memcpy(joined[0..b.stages.len], b.stages);
        @memcpy(joined[b.stages.len..], stages[1..]);
        stages = joined;
    }
    const wi = path.windowInputAt(stages, opts.lanes()) orelse return null;
    if (env.materialized.contains(window_input)) return null;
    const narrowed = for (stages[1..wi]) |st| {
        if (st.node == .select) break true;
    } else false;

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

    var ms = env_mod.MemSink{ .arena = env.shared orelse env.arena, .batches = std.array_list.Managed(Batch).init(env.shared orelse env.arena) };
    const outer_sink = env.mem_sink;
    const outer_name = env.sink_name;
    env.mem_sink = &ms;
    defer env.mem_sink = outer_sink;
    defer env.sink_name = outer_name;
    var scratch: Stats = .{};
    var delegated = false;
    {
        const outer_scope = enterScope(env, window_input);
        defer leaveScope(env, outer_scope);
        try runOutputBody(env, opts, inner, inner[inner.len - 1].node, &scratch, lanes_used, batch_arena, &delegated);
    }
    try notePrelude(env, "the window's input", stages[0].pos);
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
    const outer_pipe = env.pipe;
    env.pipe = .{ .threads = opts.lanes() };
    defer {
        if (env.pipe.pq_held) |h| h.src.close();
        env.pipe = outer_pipe;
    }
    const pipe = &env.pipe;
    const outer_stop = env.stop;
    defer env.stop = outer_stop;
    env.stop = if (env.union_sink != null) outer_stop else if (env.max_rows != null and last == .write and std.mem.eql(u8, last.write.connector, "stdout")) blk: {
        const f = try arena.create(std.atomic.Value(bool));
        f.* = .init(false);
        break :blk f;
    } else null;

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
    if (stages[0].node == .read and stages[0].node.read.form == .path and ftp.isUrl(stages[0].node.read.form.path)) {
        const copy = try env.arena.dupe(ast.Stage, stages);
        copy[0].node.read = try localRead(env, copy[0].node.read);
        stages = copy;
    }

    var ddiag = analyze.Diag{};
    analyze.checkJoinHints(env.arena, stages, &ddiag) catch |e| return aErr(env, &ddiag, e);
    env.csv_in = analyze.dialectFromHints(stages[0].hints, &ddiag) catch |e| return aErr(env, &ddiag, e);
    env.csv_out = analyze.dialectFromHints(stages[stages.len - 1].hints, &ddiag) catch |e| return aErr(env, &ddiag, e);
    env.fmt_in = analyze.formatFromHints(stages[0].hints, &ddiag) catch |e| return aErr(env, &ddiag, e);
    env.fmt_out = analyze.formatFromHints(stages[stages.len - 1].hints, &ddiag) catch |e| return aErr(env, &ddiag, e);
    env.write_opts = analyze.writeOptions(arena, last.write, stages[stages.len - 1].hints, &ddiag) catch |e| return aErr(env, &ddiag, e);
    env.cutover = analyze.cutoverProblem(last.write, stages[stages.len - 1].hints, &ddiag) catch |e| return aErr(env, &ddiag, e);
    if (stages[0].node == .read and stages[0].node.read.form == .path and
        std.mem.eql(u8, stages[0].node.read.connector, "csv"))
        try guardFileFormat(env, stages[0].node.read.form.path, env.fmt_in, "read");
    if (std.mem.eql(u8, last.write.connector, "csv") and last.write.target.len > 0)
        try guardFileFormat(env, last.write.target, env.fmt_out, "write");
    const is_file_sink = std.mem.eql(u8, last.write.connector, "csv") or std.mem.eql(u8, last.write.connector, "stdout");
    if (is_file_sink)
        analyze.checkFileSink(last.write, stages[stages.len - 1].hints, &ddiag) catch |e| return aErr(env, &ddiag, e);
    const sink_connector = if (is_file_sink) last.write.connector else if (env.connections.get(last.write.connector)) |c| c.connector else last.write.connector;
    analyze.checkSinkForm(last.write, stages[stages.len - 1].hints, sink_connector, &ddiag) catch |e| return aErr(env, &ddiag, e);
    if (!is_file_sink) if (env.connections.get(last.write.connector)) |c|
        analyze.checkSinkConnection(arena, c, &ddiag) catch |e| return aErr(env, &ddiag, e);
    analyze.checkBareUpsert(stages, env.connections, &ddiag) catch |e| return aErr(env, &ddiag, e);
    env.sink_label_prefix = analyze.hintText(stages[stages.len - 1].hints, "label_prefix");

    stages = try analyze.substFilterParams(arena, stages, env.params_expr);

    if (try pushdown.hoistFilters(arena, env.gpa, stages, env.bindings, env.tables)) |hoisted| {
        env.log.log(.debug, "filter hoisted through a projection or join: {d} -> {d} stages", .{ stages.len, hoisted.len });
        stages = hoisted;
    }
    stages = try pruneKnown(env, stages);

    if (stages[0].node == .read or stages[0].node == .union_) {
        var extra = std.array_list.Managed(ast.Hint).init(arena);
        if (last.write.split) |sp| {
            try extra.append(.{ .key = "split", .value = .{ .ident = sp.col }, .pos = sp.pos });
            if (sp.jobs) |n| {
                opts.threads = n;
                pipe.threads = opts.lanes();
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

    var head_stages = stages;
    var inlined: usize = 0;
    while (head_stages[0].node == .ref and inlined < 16) : (inlined += 1) {
        if (env.materialized.contains(head_stages[0].node.ref)) break;
        const b = (try stampedBinding(env, head_stages[0].node.ref)) orelse break;
        if (b.stages.len == 0) break;
        const joined = try arena.alloc(ast.Stage, b.stages.len + head_stages.len - 1);
        @memcpy(joined[0..b.stages.len], b.stages);
        @memcpy(joined[b.stages.len..], head_stages[1..]);
        head_stages = joined;
    }
    if (inlined > 0) {
        if (try pushdown.hoistFilters(arena, env.gpa, head_stages, env.bindings, env.tables)) |h| head_stages = h;
        head_stages = try pruneKnown(env, head_stages);
    }
    const chosen = try path.choose(arena, stages, head_stages, .{
        .lanes = opts.lanes(),
        .sink = last.write,
        .source = path.sourceOf(stages[0], env.connections),
        .head_source = path.sourceOf(head_stages[0], env.connections),
    });
    if (chosen == .union_split) {
        delegated.* = true;
        return runUnionSplit(env, stages[0].node.union_, stages[0].hints, stages[1 .. stages.len - 1], stages[stages.len - 1], opts, stats, lanes_used, batch_arena);
    }
    if (env.profile) |p| {
        try notePlan(env, p, head_stages);
        try notePlan(env, p, stages);
    }
    if (chosen == .lanes and !try keysPreferSerial(env, head_stages)) {
        const ln = chosen.lanes;
        if (ln.source == .parquet) {
            if (try runParquetLane(env, head_stages, ln.shape, last.write, opts, stats, lanes_used)) return;
        } else if (try runCsvLane(env, head_stages, ln.shape, last.write, opts, stats, lanes_used)) return;
    }
    const sql: ?path.Sql = if (chosen == .sql) chosen.sql else null;

    pipe.sql_desc = null;
    env.src_name = "";
    const src_base = env.sources.items.len;
    const t_open = std.time.Instant.now() catch null;
    var res = try buildPipeline(env, stages[0 .. stages.len - 1]);
    defer res.op.release();
    if (env.profile) |p| if (t_open) |t| try p.phase("plan and open the sources", (std.time.Instant.now() catch t).since(t));

    const wr = try resolveUpsertKeys(env, last.write);

    var whole_agg = false;
    if (pipe.sql_desc != null and src_base < env.sources.items.len) {
        var why: []const u8 = "the pipeline is not read -> filter* -> aggregate";
        if (sql) |s| if (s.whole_agg) |shape| {
            if (try wholeAggStages(env, stages, shape, src_base, &why)) |ns| {
                for (env.sources.items[src_base..]) |sc| sc.close();
                env.sources.shrinkRetainingCapacity(src_base);
                pipe.sql_desc = null;
                stages = ns;
                res.op.release();
                res = try buildPipeline(env, ns[0 .. ns.len - 1]);
                whole_agg = true;
                if (env.profile) |p| p.path = "serial; the aggregate runs in the source";
            }
        };
        if (!whole_agg and hasAggregate(stages))
            env.log.log(.warn, "aggregate over {s} runs engine-side, not in the source ({s}); every matching row is streamed here to be grouped", .{ @tagName(pipe.sql_desc.?.dialect), why });
    }

    var top_n = false;
    if (!whole_agg and pipe.sql_desc != null and src_base < env.sources.items.len) {
        var why: []const u8 = "";
        const dialect = pipe.sql_desc.?.dialect;
        if (try topNStages(env, stages, src_base, &why)) |ns| {
            for (env.sources.items[src_base..]) |sc| sc.close();
            env.sources.shrinkRetainingCapacity(src_base);
            pipe.sql_desc = null;
            stages = ns;
            res.op.release();
            res = try buildPipeline(env, ns[0 .. ns.len - 1]);
            top_n = true;
            if (env.profile) |p| p.path = "serial; ORDER BY ... LIMIT runs in the source";
        } else if (why.len > 0 and hasSort(stages))
            env.log.log(.warn, "ORDER BY ... LIMIT over {s} sorts engine-side, not in the source ({s}); every matching row is streamed here to be sorted", .{ @tagName(dialect), why });
    }

    if (!whole_agg and !top_n and opts.lanes() > 1 and pipe.sql_desc != null) {
        if (sql) |s| {
            const ran = switch (s.split) {
                .agg => |shape| try runParallelSqlAgg(env, pipe, stages, shape, wr, opts, stats, lanes_used, src_base),
                .map_join => |js| try runParallelSqlMapJoin(env, stages, js, wr, opts, stats, lanes_used, src_base),
                .map, .none => false,
            };
            if (ran) return;
            if (env.sources.items.len == src_base) {
                pipe.sql_desc = null;
                res.op.release();
                res = try buildPipeline(env, stages[0 .. stages.len - 1]);
            }
        }
        if (try op.linearize(arena, res.op)) |lin| {
            if (try planSplit(env, pipe.sql_desc.?, stages[0], opts.lanes(), wr)) |sp| {
                const schema = try dupeSchema(arena, res.schema);

                const middle = stages[1 .. stages.len - 1];
                var lane_stages = lin.stages;
                var proj_select: ?[]const u8 = null;
                var where_extra: ?[]const u8 = null;
                var want: ?*const types.Schema = null;
                if (stages[0].node == .read) {
                    const src_schema = try dupeSchema(arena, env.sources.items[src_base].schema());
                    want = try schemaPtr(arena, src_schema);
                    const out_cols = try arena.alloc([]const u8, schema.fields.len);
                    for (schema.fields, out_cols) |f, *o| o.* = f.name;
                    const facts = try factsIfWanted(env, stages[0].node.read, pipe.sql_desc.?.dialect, middle, src_schema, true, .superset);
                    const mp = try pushdown.planMap(arena, pipe.sql_desc.?.dialect, src_schema, middle, out_cols, facts);
                    where_extra = mp.where_extra;
                    if (mp.proj_schema) |ps| {
                        if (rebuildMapStages(env, mp.stages orelse middle, try schemaPtr(arena, ps))) |rs| {
                            lane_stages = rs;
                            proj_select = mp.proj_select;
                            want = try schemaPtr(arena, ps);
                        }
                    }
                }

                for (env.sources.items[src_base..]) |sc| sc.close();
                env.sources.shrinkRetainingCapacity(src_base);
                var ctx = SplitCtx{ .gpa = gpa, .kind = pipe.sql_desc.?.kind, .cfg = pipe.sql_desc.?.cfg, .base_sql = sp.base_sql, .proj_select = proj_select, .where_extra = where_extra, .report = try readReport(env, @tagName(pipe.sql_desc.?.kind)), .want = want };
                lanes_used.* = @max(lanes_used.*, @min(opts.lanes(), sp.predicates.len));
                env.log.log(.debug, "split-parallel: {d} splits over {d} lanes on key range (projection: {s}, filter pushdown: {s})", .{ sp.predicates.len, @min(opts.lanes(), sp.predicates.len), proj_select orelse "all", if (where_extra != null) "yes" else "no" });
                const profiled: ?parallel.Profiled = if (env.profile) |p| blk: {
                    const root = if (lane_stages.len > 0) lane_stages[lane_stages.len - 1].asOp() else res.op;
                    const ids = try arena.alloc(profile.Id, lane_stages.len + 1);
                    p.path = try msgfmt.allocPrint(arena, "{s} split-parallel, {d} key ranges", .{ @tagName(pipe.sql_desc.?.kind), sp.predicates.len });
                    try p.noteSent("{s}, once per key range: {s}", .{ @tagName(pipe.sql_desc.?.kind), sp.base_sql });
                    break :blk .{ .prof = p, .ids = p.chainIds(try p.shape(root), ids) };
                } else null;
                if (try buildParallelSink(env, wr, schema)) |mode| {
                    errdefer parallel.abortLanes(mode);
                    stats.rows_out += try parallel.run(gpa, sp.predicates, openSplitSource, &ctx, lane_stages, mode, opts.lanes(), env.rows_read, profiled);
                    try parallel.finishLanes(mode);
                } else {
                    const snk = try openSink(env, wr, schema);
                    var guard = SinkGuard.init(snk);
                    defer guard.abort();
                    stats.rows_out += try parallel.run(gpa, sp.predicates, openSplitSource, &ctx, lane_stages, .{ .shared = snk }, opts.lanes(), env.rows_read, profiled);
                    try guard.close();
                }
                return;
            }
        }
    }

    if (hasFlagHint(stages[0].hints, "buffer")) {
        var held = std.heap.ArenaAllocator.init(gpa);
        defer held.deinit();
        var batches = std.array_list.Managed(Batch).init(held.allocator());
        while (true) {
            if (aborting()) return error.Aborted;
            const b = (try res.op.next(held.allocator())) orelse break;
            try batches.append(b);
        }
        for (env.sources.items[src_base..]) |sc| sc.close();
        env.sources.shrinkRetainingCapacity(src_base);

        const snk = try openSink(env, wr, res.schema);
        var guard = SinkGuard.init(snk);
        defer guard.abort();
        for (batches.items) |b| {
            if (aborting()) return error.Aborted;
            try snk.writeBatch(batch_arena.allocator(), b);
            stats.rows_out += b.len;
            _ = batch_arena.reset(.retain_capacity);
        }
        try guard.close();
        if (env.profile) |p| _ = try p.absorb(res.op);
        return;
    }

    const snk = try openSink(env, wr, res.schema);
    var guard = SinkGuard.init(snk);
    defer guard.abort();

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
    try guard.close();
    if (env.profile) |p| _ = try p.absorb(res.op);
}

/// Split-parallel union: each branch runs as its own split pipeline. A target with
/// per-lane sinks takes each branch on its own, the first keeping the write mode (so
/// `overwrite` truncates once) and later ones appending; any other is opened once, on
/// the union's schema, and every branch writes to it, so the union is one result.
fn runUnionSplit(env: *Env, u: ast.Union, hints: []const ast.Hint, downstream: []const ast.Stage, write_stage: ast.Stage, opts: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!void {
    const arena = env.arena;
    const tag_col = forHintIdent(hints, "tag");
    const canon_opt = forHintIdent(hints, "canon");
    const specs = try unionSpecs(env, u, hints);
    if (specs.len == 0) return planErr(env.diag, "union has no source tables");

    const except = unionExceptNames(downstream);
    const schemas = try arena.alloc(types.Schema, specs.len);
    const probes = try arena.alloc(driver.Source, specs.len);
    var opened: usize = 0;
    defer for (probes[0..opened]) |p| p.close();
    for (specs, schemas, probes) |*s, *sch, *p| {
        if (except.len > 0) {
            if (try exceptColumns(env, s.read, hints, except)) |cols| s.read.cols = cols;
        }
        var probe = s.read;
        probe.where = "1 = 0";
        p.* = try openSource(env, probe, hints);
        opened += 1;
        sch.* = try dupeSchema(arena, p.schema());
    }
    const canon = try unionCanon(env, specs, schemas, canon_opt, except);
    const w = write_stage.node.write;
    const outer_sink = env.union_sink;
    defer env.union_sink = outer_sink;
    var shared: SinkGuard = .empty;
    defer shared.abort();
    if (!opensPerLane(env, w)) {
        var out_fields: []types.Schema.Field = &.{};
        for (specs, schemas, probes, 0..) |s, sch, p, i| {
            const scan = try arena.create(op.Scan);
            scan.* = .{ .src = p };
            var r = try buildProject(env, try synthReconcile(arena, sch, canon, tag_col, s.tag), sch, .{ .scan = scan });
            for (downstream) |st| r = try buildStage(env, st, r.op, r.schema);
            r.op.release();
            if (i == 0) {
                out_fields = try arena.dupe(types.Schema.Field, r.schema.fields);
            } else for (out_fields, r.schema.fields) |*f, b| {
                f.ty.nullable = f.ty.nullable or b.ty.nullable;
            }
        }
        const snk = try openSink(env, w, .{ .fields = out_fields });
        shared.arm(snk);
        env.union_sink = try keptOpen(arena, snk);
    }
    for (probes[0..opened]) |p| p.close();
    opened = 0;

    var split_hints = std.array_list.Managed(ast.Hint).init(arena);
    for (hints) |h| {
        if (std.mem.eql(u8, h.key, "split") or std.mem.eql(u8, h.key, "splits") or std.mem.eql(u8, h.key, "split_kind"))
            try split_hints.append(h);
    }
    const branch_hints = try split_hints.toOwnedSlice();

    const outer_scope = if (env.profile) |p| p.scope else 0;
    defer if (env.profile) |p| {
        p.scope = outer_scope;
    };
    for (specs, schemas, 0..) |s, sch, i| {
        if (env.stop) |f| if (f.load(.monotonic)) break;
        if (env.profile) |p| p.scope = std.hash.Wyhash.hash(outer_scope, std.mem.asBytes(&i));
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
    try shared.close();
    if (env.profile) |p| p.path = try msgfmt.allocPrint(arena, "union split-parallel, {d} branches", .{specs.len});
}

/// `s` with close and abort doing nothing, for the branches of a union whose
/// owner closes it once.
fn keptOpen(arena: std.mem.Allocator, s: driver.Sink) !driver.Sink {
    const vt = try arena.create(driver.Sink.VTable);
    vt.* = s.vtable.*;
    vt.close = struct {
        fn f(_: *anyopaque) anyerror!void {}
    }.f;
    vt.abort = struct {
        fn f(_: *anyopaque) void {}
    }.f;
    return .{ .ptr = s.ptr, .vtable = vt };
}
