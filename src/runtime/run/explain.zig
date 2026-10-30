//! EXPLAIN and EXPLAIN ANALYZE: the plan printed, or run for its actuals and printed
//! with them, writing nothing.
//!
//! The ANALYZE report is laid out as StarRocks lays out a query profile: a summary
//! first (wall time, operator time, rows, the hottest operators by their own time and
//! their share of it), then the tree, each node with its time, rows in and out, and
//! across lanes the least and most any lane spent, since a mean hides the lane that
//! held the run up. Shares are of operator time, summed over lanes, not wall time: at
//! `-j 8` eight lanes' work fills one wall second. A node past 30% of it is red, past
//! 15% yellow, on a terminal without `NO_COLOR`.

const Env = @import("../env.zig").Env;
const Profile = @import("../../exec/profile.zig").Profile;
const RunOptions = @import("../env.zig").RunOptions;
const Guard = @import("../../connect/guard.zig").Guard;
const Stats = @import("../env.zig").Stats;
const aErr = @import("../env.zig").aErr;
const analyze = @import("../analyze.zig");
const arrow = @import("../../format/arrow/write.zig");
const ast = @import("../../lang/ast.zig");
const column = @import("../../exec/column.zig");
const jsontext = @import("../../exec/jsontext.zig");
const print = @import("../../lang/print.zig");
const profile = @import("../../exec/profile.zig");
const runDescribe = @import("../run.zig").runDescribe;
const runOutput = @import("output.zig").runOutput;
const sourceLabel = @import("../connect.zig").sourceLabel;
const resolveFolder = @import("../connect.zig").resolveFolder;
const isFolderRead = @import("../connect.zig").isFolderRead;
const std = @import("std");
const types = @import("../../lang/types.zig");

/// `EXPLAIN [ANALYZE] <query>;` against what the statements above put in scope.
/// ANALYZE runs the pipeline as it would run, with the sink discarded; the plain
/// form renders the static plan through `analyze.render`. Both are scoped to this
/// one statement.
pub fn runExplain(env: *Env, e: ast.ExplainStmt, opts: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!void {
    defer env.endStatement(false);
    if (e.mode == .describe) return runDescribe(env, e.pipeline, stats);
    if (e.mode == .analyze) {
        const outer = env.explain;
        env.explain = true;
        defer env.explain = outer;
        var o = opts;
        o.explain = true;
        return runOutput(env, e.pipeline, o, stats, lanes_used, batch_arena);
    }

    var adiag = analyze.Diag{};
    const plan = analyze.analyzeOne(env.arena, e.pipeline, env.bindings, env.connections, env.params_expr, &adiag, env.tables, opts.lanes()) catch |err| return aErr(env, &adiag, err);

    var aw = std.Io.Writer.Allocating.init(env.gpa);
    defer aw.deinit();
    try analyze.render(plan, &aw.writer);
    if (env.stdout_format == .arrow) {
        const info = env.takeResult();
        const n = try printPlanArrow(env.gpa, aw.writer.buffered(), info);
        stats.rows_out += n;
        if (env.on_result) |h| h.call(.{ .info = info, .rows = n });
        return;
    }
    std.fs.File.stderr().writeAll(aw.writer.buffered()) catch {};
}

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
    var guard = Guard(*arrow.ArrowWriter).init(w);
    defer guard.abort();
    try w.writeBatch(a, .{ .schema = &schema, .columns = cols, .len = n });
    try guard.close();
    return n;
}

/// Tells the profile what each stage of `stages` does, the bindings a join or a
/// reference reads included, before any lane builds an operator from them; and
/// which stage of each kind an operator a lane builds outside the planner stands for.
pub fn notePlan(env: *Env, prof: *Profile, stages: []const ast.Stage) !void {
    for (stages) |st| for (profile.kindsOf(st.node)) |k| {
        if (prof.fallback.get(k) == 0) prof.fallback.set(k, profile.keyOf(st.pos));
    };
    try prof.describe(profile.merged_at, .scan, "the lanes' partial results, merged");
    try noteStages(env, prof, stages, 0);
}

fn noteStages(env: *Env, prof: *Profile, stages: []const ast.Stage, depth: usize) !void {
    if (depth > 16) return;
    for (stages, 0..) |st, i| {
        const at = profile.keyOf(st.pos);
        switch (st.node) {
            .read => |rd| try prof.describe(at, .scan, try readText(env, rd, st.hints)),
            .ref => |name| if (env.bindings.get(name)) |b| try noteStages(env, prof, b.stages, depth + 1),
            .sort => |s| {
                try prof.describe(at, .sort, try profile.stageText(env.arena, st));
                if (i + 1 < stages.len and stages[i + 1].node == .limit)
                    try prof.describe(at, .top_n, try profile.topNText(env.arena, s, stages[i + 1].node.limit));
            },
            else => {
                const text = try profile.stageText(env.arena, st);
                for (profile.kindsOf(st.node)) |k| try prof.describe(at, k, text);
                if (st.node == .join) if (env.bindings.get(st.node.join.binding)) |b| try noteStages(env, prof, b.stages, depth + 1);
            },
        }
    }
}

/// A read as the tree names it: the connector, what it reads, and the WHERE it sends.
/// A lake table is resolved here, as its run reuses: its kind and the version read.
fn readText(env: *Env, rd: ast.Read, hints: []const ast.Hint) ![]const u8 {
    var out = std.Io.Writer.Allocating.init(env.arena);
    const w = &out.writer;
    if (isFolderRead(rd)) if (try resolveFolder(env, rd.form.path, hints)) |fr| if (fr.table) |t| {
        try w.print("{s} {s} ({s})", .{ @tagName(t.kind), rd.form.path, t.label });
        return out.written();
    };
    try w.writeAll(sourceLabel(env, rd, hints));
    switch (rd.form) {
        .path => |p| try w.print(" {s}", .{p}),
        .table => |q| {
            try w.writeAll(" table ");
            try print.name(w, q);
        },
        .query => try w.writeAll(" query"),
        .builtin => |b| try w.print(" {s}()", .{b.name}),
        .unit => {},
    }
    if (rd.where.len > 0) try w.print(" [pushdown: {s}]", .{rd.where});
    return out.written();
}

/// What the run around the operators came to, for the summary. The read counts are
/// the pipeline's share of the run's `ScanTally`.
pub const RunFacts = struct {
    wall_ns: u64,
    rows_read: u64,
    rows_out: u64,
    lanes: usize,
    row_groups: u64 = 0,
    row_groups_skipped: u64 = 0,
    columns_read: u64 = 0,
    columns_total: u64 = 0,
    spill_peak: u64 = 0,
    /// Where a run would write, which `EXPLAIN ANALYZE` never does: `parquet out.parquet`.
    sink: []const u8 = "",
};

/// The report, written to stderr in one piece: stdout carries the data, and a
/// writer opened on stderr mid-run overwrote the log when stderr was a file.
pub fn printProfile(env: *Env, prof: *const Profile, facts: RunFacts) !void {
    var aw = std.Io.Writer.Allocating.init(env.gpa);
    defer aw.deinit();
    const color = std.posix.isatty(std.fs.File.stderr().handle) and !std.process.hasEnvVarConstant("NO_COLOR");
    try renderProfile(env.arena, prof, facts, color, &aw.writer);
    std.fs.File.stderr().writeAll(aw.writer.buffered()) catch {};
}

const Ordered = struct { ids: []const profile.Id, total_own: u64 };

/// Every node once, root first and inputs after, as the tree prints them; a node's
/// number is its place here.
fn ordered(arena: std.mem.Allocator, prof: *const Profile) !Ordered {
    var ids = std.array_list.Managed(profile.Id).init(arena);
    var seen = std.AutoHashMap(profile.Id, void).init(arena);
    for (prof.roots.items) |r| try visit(prof, r, &ids, &seen);
    var total: u64 = 0;
    for (ids.items) |id| total += prof.nodes.getPtr(id).?.exclusive(prof);
    return .{ .ids = ids.items, .total_own = total };
}

fn visit(prof: *const Profile, id: profile.Id, ids: *std.array_list.Managed(profile.Id), seen: *std.AutoHashMap(profile.Id, void)) !void {
    if ((try seen.getOrPut(id)).found_existing) return;
    try ids.append(id);
    const n = prof.nodes.getPtr(id) orelse return;
    for (n.children) |c| try visit(prof, c.id, ids, seen);
}

fn numberOf(o: Ordered, id: profile.Id) usize {
    for (o.ids, 1..) |x, i| if (std.meta.eql(x, id)) return i;
    return 0;
}

pub fn renderProfile(arena: std.mem.Allocator, prof: *const Profile, facts: RunFacts, color: bool, w: *std.Io.Writer) !void {
    const o = try ordered(arena, prof);
    const lanes = @max(prof.laneCount(), 1);

    try w.writeAll("Summary\n");
    try w.print("  Wall time: {f}   Operator time: {f}", .{ Dur{ .ns = facts.wall_ns }, Dur{ .ns = o.total_own } });
    if (facts.lanes > 1) try w.print(" (summed over {d} lanes)", .{facts.lanes});
    try w.writeAll("\n");
    try w.print("  Rows read: {f}   Rows out: {f}   Path: {s}\n", .{ Count{ .n = facts.rows_read }, Count{ .n = facts.rows_out }, prof.path });
    if (facts.sink.len > 0) try w.print("  Sink: {s}, not written: EXPLAIN ANALYZE moves no data\n", .{facts.sink});
    if (facts.row_groups + facts.row_groups_skipped > 0)
        try w.print("  Row groups: {d} read, {d} skipped\n", .{ facts.row_groups, facts.row_groups_skipped });
    if (facts.columns_total > 0) try w.print("  Columns read: {d} of {d}\n", .{ facts.columns_read, facts.columns_total });
    if (facts.spill_peak > 0) try w.print("  Spilled to disk, at most: {f}\n", .{Bytes{ .n = facts.spill_peak }});
    for (prof.sent.items) |s| try w.print("  Sent to {s}\n", .{jsontext.clipUtf8(s, 400)});
    for (prof.phases.items) |ph| try w.print("  {s}: {f}\n", .{ ph.name, Dur{ .ns = ph.ns } });

    const top = try hottest(arena, prof, o, 5);
    if (top.len > 0) {
        try w.writeAll("  Top operators by own time:\n");
        for (top, 1..) |id, rank| {
            const n = prof.nodes.getPtr(id).?;
            const own = n.exclusive(prof);
            const sh = share(own, o.total_own);
            try w.writeAll("    ");
            try paint(w, color, sh, true);
            try w.print("{d}. {s} (#{d}): {f} ({f})", .{ rank, kindName(id.kind), numberOf(o, id), Dur{ .ns = own }, Pct{ .v = sh } });
            try paint(w, color, 0, false);
            try w.writeAll("\n");
        }
    }

    try w.writeAll("plan (actuals)\n");
    for (prof.roots.items, 0..) |r, i| try node(prof, o, r, .input, "", i + 1 == prof.roots.items.len, lanes, color, w);
}

fn hottest(arena: std.mem.Allocator, prof: *const Profile, o: Ordered, limit: usize) ![]const profile.Id {
    const ids = try arena.dupe(profile.Id, o.ids);
    const Ctx = struct {
        p: *const Profile,
        fn more(c: @This(), a: profile.Id, b: profile.Id) bool {
            return c.p.nodes.getPtr(a).?.exclusive(c.p) > c.p.nodes.getPtr(b).?.exclusive(c.p);
        }
    };
    std.sort.insertion(profile.Id, ids, Ctx{ .p = prof }, Ctx.more);
    var n: usize = 0;
    while (n < ids.len and n < limit and prof.nodes.getPtr(ids[n]).?.exclusive(prof) > 0) n += 1;
    return ids[0..n];
}

fn node(prof: *const Profile, o: Ordered, id: profile.Id, role: profile.Role, prefix: []const u8, last: bool, lanes: usize, color: bool, w: *std.Io.Writer) !void {
    const n = prof.nodes.getPtr(id) orelse return;
    const own = n.exclusive(prof);
    const sh = share(own, o.total_own);

    try w.print("{s}{s}", .{ prefix, if (last) "└─ " else "├─ " });
    try paint(w, color, sh, true);
    switch (role) {
        .input => {},
        .probe => try w.writeAll("<probe> "),
        .build => try w.writeAll("<build> "),
    }
    try w.print("{s} (#{d})", .{ kindName(id.kind), numberOf(o, id) });
    const text = prof.detail(id);
    if (text.len > 0) try w.print("  {s}", .{jsontext.clipUtf8(text, 160)});
    try paint(w, color, 0, false);
    try w.writeAll("\n");

    var kid_prefix_buf: [512]u8 = undefined;
    const kid_prefix = std.fmt.bufPrint(&kid_prefix_buf, "{s}{s}", .{ prefix, if (last) "   " else "│  " }) catch prefix;
    var line_buf: [520]u8 = undefined;
    const line = std.fmt.bufPrint(&line_buf, "{s}{s}", .{ kid_prefix, if (n.children.len > 0) "│  " else "   " }) catch kid_prefix;

    try w.print("{s}time: {f} ({f})", .{ line, Dur{ .ns = own }, Pct{ .v = sh } });
    if (spread(n, prof, lanes, .time)) |s| try w.print(" [min={f}, max={f}]", .{ Dur{ .ns = s.min }, Dur{ .ns = s.max } });
    if (skew(n, prof, lanes, sh)) |x| {
        try paint(w, color, 1, true);
        try w.print("  skew {d:.1}x", .{x});
        try paint(w, color, 0, false);
    }
    try w.writeAll("\n");

    try w.print("{s}rows: ", .{line});
    if (n.children.len > 0) {
        const in = n.rowsIn(prof);
        try w.print("{f} -> {f}", .{ Count{ .n = in }, Count{ .n = n.total.rows } });
        if (in > 0 and keepsRows(id.kind)) try w.print(" ({f} kept)", .{Pct{ .v = @as(f64, @floatFromInt(n.total.rows)) / @as(f64, @floatFromInt(in)) }});
    } else try w.print("{f}", .{Count{ .n = n.total.rows }});
    try w.print("   batches: {d}", .{n.total.calls});
    if (spread(n, prof, lanes, .rows)) |s| try w.print("   rows per lane [min={f}, max={f}]", .{ Count{ .n = s.min }, Count{ .n = s.max } });
    try w.writeAll("\n");

    if (n.facts.items.len > 0) {
        try w.writeAll(line);
        for (n.facts.items, 0..) |f, i| {
            if (i > 0) try w.writeAll("   ");
            try w.print("{s}: ", .{f.name});
            switch (f.unit) {
                .count => try w.print("{f}", .{Count{ .n = f.value }}),
                .bytes => try w.print("{f}", .{Bytes{ .n = f.value }}),
                .ns => try w.print("{f}", .{Dur{ .ns = f.value }}),
            }
        }
        try w.writeAll("\n");
    }
    if (n.note.len > 0) try w.print("{s}{s}\n", .{ line, n.note });

    for (n.children, 0..) |c, i| try node(prof, o, c.id, c.role, kid_prefix, i + 1 == n.children.len, lanes, color, w);
}

/// Whether rows out over rows in is the operator's selectivity, worth a percentage.
fn keepsRows(k: profile.Kind) bool {
    return switch (k) {
        .filter, .join, .nl_join, .distinct, .limit, .top_n, .runtime_filter => true,
        else => false,
    };
}

const Measure = enum { time, rows };
const Spread = struct { min: u64, max: u64 };

/// The least and most any lane that ran the node spent or produced, when more than one did.
fn spread(n: *const profile.Node, prof: *const Profile, lanes: usize, m: Measure) ?Spread {
    if (lanes < 2) return null;
    var s = Spread{ .min = std.math.maxInt(u64), .max = 0 };
    var ran: usize = 0;
    for (n.lanes.items, 0..) |t, lane| {
        if (t.calls == 0) continue;
        ran += 1;
        const v = switch (m) {
            .time => n.laneExclusive(prof, lane),
            .rows => t.rows,
        };
        s.min = @min(s.min, v);
        s.max = @max(s.max, v);
    }
    return if (ran > 1) s else null;
}

/// The busiest lane's time over the lanes' mean, when it is worth a look: at least
/// 1.5x, on a node holding 5% of the operator time or more.
fn skew(n: *const profile.Node, prof: *const Profile, lanes: usize, sh: f64) ?f64 {
    if (lanes < 2 or sh < 0.05) return null;
    var sum: u64 = 0;
    var max: u64 = 0;
    var ran: u64 = 0;
    for (n.lanes.items, 0..) |t, lane| {
        if (t.calls == 0) continue;
        const v = n.laneExclusive(prof, lane);
        sum += v;
        max = @max(max, v);
        ran += 1;
    }
    if (ran < 2 or sum == 0) return null;
    const mean = @as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(ran));
    const x = @as(f64, @floatFromInt(max)) / mean;
    return if (x >= 1.5) x else null;
}

fn share(part: u64, whole: u64) f64 {
    if (whole == 0) return 0;
    return @as(f64, @floatFromInt(part)) / @as(f64, @floatFromInt(whole));
}

fn paint(w: *std.Io.Writer, on: bool, sh: f64, start: bool) !void {
    if (!on) return;
    if (!start) return w.writeAll("\x1b[0m");
    if (sh >= 0.30) try w.writeAll("\x1b[1;31m") else if (sh >= 0.15) try w.writeAll("\x1b[1;33m");
}

fn kindName(k: profile.Kind) []const u8 {
    return switch (k) {
        .union_ => "union",
        .top_n => "top-n",
        .nl_join => "nested-loop join",
        .runtime_filter => "runtime filter",
        else => @tagName(k),
    };
}

const Dur = struct {
    ns: u64,
    pub fn format(self: Dur, w: *std.Io.Writer) std.Io.Writer.Error!void {
        const v: f64 = @floatFromInt(self.ns);
        if (self.ns < 1_000) return w.print("{d}ns", .{self.ns});
        if (self.ns < 1_000_000) return w.print("{d:.1}us", .{v / 1e3});
        if (self.ns < 1_000_000_000) return w.print("{d:.2}ms", .{v / 1e6});
        return w.print("{d:.2}s", .{v / 1e9});
    }
};

/// A count as StarRocks prints one: `1.634M (1634133)`, the exact figure kept.
const Count = struct {
    n: u64,
    pub fn format(self: Count, w: *std.Io.Writer) std.Io.Writer.Error!void {
        const v: f64 = @floatFromInt(self.n);
        if (self.n < 1_000) return w.print("{d}", .{self.n});
        if (self.n < 1_000_000) return w.print("{d:.1}K ({d})", .{ v / 1e3, self.n });
        if (self.n < 1_000_000_000) return w.print("{d:.3}M ({d})", .{ v / 1e6, self.n });
        return w.print("{d:.3}B ({d})", .{ v / 1e9, self.n });
    }
};

const Bytes = struct {
    n: u64,
    pub fn format(self: Bytes, w: *std.Io.Writer) std.Io.Writer.Error!void {
        const v: f64 = @floatFromInt(self.n);
        if (self.n < 1 << 10) return w.print("{d} B", .{self.n});
        if (self.n < 1 << 20) return w.print("{d:.1} KB", .{v / 1024});
        if (self.n < 1 << 30) return w.print("{d:.1} MB", .{v / (1024 * 1024)});
        return w.print("{d:.2} GB", .{v / (1024 * 1024 * 1024)});
    }
};

const Pct = struct {
    v: f64,
    pub fn format(self: Pct, w: *std.Io.Writer) std.Io.Writer.Error!void {
        return w.print("{d:.1}%", .{self.v * 100});
    }
};

const testing = std.testing;
const op = @import("../../exec/op.zig");

test "explain: the report ranks operators by own time and draws the tree with each node's rows" {
    var p = Profile.init(testing.allocator);
    defer p.deinit();
    var scan = op.Scan{ .src = undefined };
    scan.stats = .{ .at = 1, .ns = 3_000_000, .rows = 2_000, .calls = 2 };
    var f = op.Filter{ .child = .{ .scan = &scan }, .pred = undefined, .back = testing.allocator };
    f.stats = .{ .at = 2, .ns = 4_000_000, .rows = 500, .calls = 2 };
    _ = try p.absorb(.{ .filter = &f });
    try p.describe(1, .scan, "csv x.csv");
    try p.describe(2, .filter, "v > 5");

    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    var out = std.Io.Writer.Allocating.init(testing.allocator);
    defer out.deinit();
    try renderProfile(ar.allocator(), &p, .{ .wall_ns = 5_000_000, .rows_read = 2_000, .rows_out = 500, .lanes = 1 }, false, &out.writer);
    try testing.expectEqualStrings(
        \\Summary
        \\  Wall time: 5.00ms   Operator time: 4.00ms
        \\  Rows read: 2.0K (2000)   Rows out: 500   Path: serial
        \\  Top operators by own time:
        \\    1. scan (#2): 3.00ms (75.0%)
        \\    2. filter (#1): 1.00ms (25.0%)
        \\plan (actuals)
        \\└─ filter (#1)  v > 5
        \\   │  time: 1.00ms (25.0%)
        \\   │  rows: 2.0K (2000) -> 500 (25.0% kept)   batches: 2
        \\   └─ scan (#2)  csv x.csv
        \\         time: 3.00ms (75.0%)
        \\         rows: 2.0K (2000)   batches: 2
        \\
    , out.written());
}

test "explain: lanes show their spread, and a lane far above the mean is called out" {
    var p = Profile.init(testing.allocator);
    defer p.deinit();
    const Lane = struct {
        fn run(prof: *Profile, go: *std.Thread.ResetEvent, ns: u64) !void {
            go.wait();
            var scan = op.Scan{ .src = undefined };
            scan.stats = .{ .at = 1, .ns = ns, .rows = ns / 1000, .calls = 1 };
            _ = try prof.absorb(.{ .scan = &scan });
        }
    };
    var go = std.Thread.ResetEvent{};
    var threads: [3]std.Thread = undefined;
    for (&threads, [_]u64{ 1_000_000, 1_000_000, 7_000_000 }) |*t, ns| t.* = try std.Thread.spawn(.{}, Lane.run, .{ &p, &go, ns });
    go.set();
    for (threads) |t| t.join();
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    var out = std.Io.Writer.Allocating.init(testing.allocator);
    defer out.deinit();
    try renderProfile(ar.allocator(), &p, .{ .wall_ns = 7_000_000, .rows_read = 9_000, .rows_out = 9_000, .lanes = 3 }, false, &out.writer);
    try testing.expect(std.mem.indexOf(u8, out.written(), "time: 9.00ms (100.0%) [min=1.00ms, max=7.00ms]  skew 2.3x") != null);
    try testing.expect(std.mem.indexOf(u8, out.written(), "rows per lane [min=1.0K (1000), max=7.0K (7000)]") != null);
    try testing.expect(std.mem.indexOf(u8, out.written(), "(summed over 3 lanes)") != null);
}

test "explain: an Arrow plan whose stdout write fails returns the error and frees once" {
    const full = try std.posix.open("/dev/full", .{ .ACCMODE = .WRONLY }, 0);
    defer std.posix.close(full);
    const saved = try std.posix.dup(std.posix.STDOUT_FILENO);
    defer std.posix.close(saved);
    try std.posix.dup2(full, std.posix.STDOUT_FILENO);
    defer std.posix.dup2(saved, std.posix.STDOUT_FILENO) catch {};
    try testing.expectError(error.WriteFailed, printPlanArrow(testing.allocator, "scan\n  filter\n", .{}));
}

test "the profile's text clips through jsontext.clipUtf8 as its own clip did on UTF-8 text" {
    const old = struct {
        fn clip(s: []const u8, max: usize) []const u8 {
            if (s.len <= max) return s;
            var end = max;
            while (end > 0 and (s[end] & 0xC0) == 0x80) end -= 1;
            return s[0..end];
        }
    };
    var prng = std.Random.DefaultPrng.init(0x636c6970);
    const rand = prng.random();
    const pieces = [_][]const u8{ "a", " ", "\xc3\xb3", "\xe2\x82\xac", "\xf0\x9f\x98\x80" };
    var in: std.ArrayList(u8) = .empty;
    defer in.deinit(testing.allocator);
    for (0..3000) |_| {
        in.clearRetainingCapacity();
        for (0..rand.uintLessThan(usize, 120)) |_| try in.appendSlice(testing.allocator, pieces[rand.uintLessThan(usize, pieces.len)]);
        for ([_]usize{ 0, 1, 2, 3, 160, 400, rand.uintLessThan(usize, 500) }) |max|
            try testing.expectEqualStrings(old.clip(in.items, max), jsontext.clipUtf8(in.items, max));
    }
    try testing.expectEqualStrings("", old.clip("\x80\x80\x80\x80\x80", 4));
    try testing.expectEqualStrings("\x80\x80\x80\x80", jsontext.clipUtf8("\x80\x80\x80\x80\x80", 4));
}
