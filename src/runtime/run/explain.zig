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
const Guard = @import("../../exec/guard.zig").Guard;
const Stats = @import("../env.zig").Stats;
const aErr = @import("../env.zig").aErr;
const analyze = @import("../analyze.zig");
const arrow = @import("../../format/arrow/write.zig");
const ast = @import("../../lang/ast.zig");
const column = @import("../../exec/column.zig");
const jsontext = @import("../../exec/jsontext.zig");
const msgfmt = @import("../../base/msgfmt.zig");
const profile = @import("../../exec/profile.zig");
const readLabel = @import("../connect.zig").readLabel;
const trace = @import("../../base/iotrace.zig");
const runDescribe = @import("../run.zig").runDescribe;
const runOutput = @import("output.zig").runOutput;
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

/// `notePlan` for a chain run for what the plan needs: no stage of it stands for an
/// operator a lane builds.
pub fn noteStep(env: *Env, prof: *Profile, stages: []const ast.Stage) !void {
    try noteStages(env, prof, stages, 0);
}

/// A profile of its own for work outside any statement's, a FOR EACH discovery or a
/// query LET, when the run is explaining it; null when not, or when inside a report.
pub fn openStep(env: *Env, explaining: bool, tree: []const u8) !?*Profile {
    if (!explaining or env.profile != null) return null;
    const p = try env.gpa.create(Profile);
    p.* = Profile.init(env.gpa);
    p.tree = tree;
    env.profile = p;
    trace.current = p.tracer();
    return p;
}

/// A line of the report for what a script chose, a CASE's arm, a loop's row, a call's
/// arguments, so the trees after it read in their context; nothing when not explaining.
pub fn say(env: *Env, comptime fmt: []const u8, args: anytype) void {
    if (!env.explain or env.loop_report != null) return;
    var aw = std.Io.Writer.Allocating.init(env.gpa);
    defer aw.deinit();
    aw.writer.print(fmt ++ "\n", args) catch return;
    std.fs.File.stderr().writeAll(aw.writer.buffered()) catch {};
}

/// Prints what `openStep`'s profile saw, if it saw anything, and ends it. A failed
/// step's report is what it did until then; printing it never fails the run.
pub fn closeStep(env: *Env, step: ?*Profile) void {
    const p = step orelse return;
    trace.current = null;
    env.profile = null;
    if (env.loop_report) |lr| {
        lr.adopt(env.report_row, p, .{ .wall_ns = 0, .rows_read = 0, .rows_out = 0, .lanes = 1 }, true) catch {};
        return;
    }
    defer {
        p.deinit();
        env.gpa.destroy(p);
    }
    var aw = std.Io.Writer.Allocating.init(env.gpa);
    defer aw.deinit();
    renderSteps(env.arena, p, &aw.writer) catch return;
    std.fs.File.stderr().writeAll(aw.writer.buffered()) catch {};
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

/// A read as the tree names it: the connector and what it reads; its `sent:` line says
/// the rest. A lake table is resolved here, as its run reuses: its kind and the version
/// read.
fn readText(env: *Env, rd: ast.Read, hints: []const ast.Hint) ![]const u8 {
    if (isFolderRead(rd)) if (try resolveFolder(env, rd.form.path, hints)) |fr| if (fr.table) |t|
        return msgfmt.allocPrint(env.arena, "{s} {s} ({s})", .{ @tagName(t.kind), rd.form.path, t.label });
    return readLabel(env, rd, hints);
}

/// A long `IN` list's first values and a count of the rest, so what follows a key
/// pushdown's thousand values still shows.
fn shortLists(arena: std.mem.Allocator, s: []const u8) ![]const u8 {
    const mark = " IN (";
    const shown = 8;
    var out = std.array_list.Managed(u8).init(arena);
    var quote: u8 = 0;
    var i: usize = 0;
    while (i < s.len) {
        const c = s[i];
        if (quote != 0) {
            if (c == quote) quote = 0;
        } else if (c == '\'' or c == '"' or c == '`') {
            quote = c;
        } else if (c == '[') {
            quote = ']';
        } else if (std.ascii.startsWithIgnoreCase(s[i..], mark)) {
            var depth: usize = 1;
            var q: u8 = 0;
            var commas: usize = 0;
            var cut: usize = 0;
            var j = i + mark.len;
            while (j < s.len and depth > 0) : (j += 1) {
                const d = s[j];
                if (q != 0) {
                    if (d == q) q = 0;
                    continue;
                }
                switch (d) {
                    '\'', '"', '`' => q = d,
                    '[' => q = ']',
                    '(' => depth += 1,
                    ')' => depth -= 1,
                    ',' => if (depth == 1) {
                        commas += 1;
                        if (commas == shown) cut = j;
                    },
                    else => {},
                }
            }
            if (depth == 0 and cut > 0) {
                try out.appendSlice(s[i..cut]);
                try out.writer().print(", ... (+{d} values))", .{commas + 1 - shown});
                i = j;
                continue;
            }
        }
        try out.append(c);
        i += 1;
    }
    return out.items;
}

/// A request as the `sent:` line prints it: on one line, the first columns of a long
/// list and the first values of a long `IN`, the rest of the query in full, up to a
/// length no terminal is served by.
pub fn sentText(arena: std.mem.Allocator, text: []const u8) ![]const u8 {
    const flat = try arena.alloc(u8, text.len);
    var n: usize = 0;
    for (text) |c| {
        if (std.ascii.isWhitespace(c)) {
            if (n > 0 and flat[n - 1] != ' ') {
                flat[n] = ' ';
                n += 1;
            }
            continue;
        }
        flat[n] = c;
        n += 1;
    }
    const s = try shortLists(arena, try shortColumns(arena, std.mem.trimRight(u8, flat[0..n], " ")));
    const max = 4000;
    if (s.len <= max) return s;
    const kept = jsontext.clipUtf8(s, max);
    return msgfmt.allocPrint(arena, "{s} ... (+{d} bytes)", .{ kept, s.len - kept.len });
}

/// `s` with a top-level SELECT list past `max_list` bytes cut to its first columns and
/// a count of the rest.
fn shortColumns(arena: std.mem.Allocator, s: []const u8) ![]const u8 {
    const head = "SELECT ";
    const shown = 6;
    const max_list = 120;
    if (s.len < head.len or !std.ascii.eqlIgnoreCase(s[0..head.len], head)) return s;
    var depth: usize = 0;
    var quote: u8 = 0;
    var commas: usize = 0;
    var cut: usize = 0;
    var i: usize = head.len;
    while (i < s.len) : (i += 1) {
        const c = s[i];
        if (quote != 0) {
            if (c == quote) quote = 0;
            continue;
        }
        switch (c) {
            '\'', '"', '`' => quote = c,
            '[' => quote = ']',
            '(' => depth += 1,
            ')' => depth -|= 1,
            ',' => if (depth == 0) {
                commas += 1;
                if (commas == shown) cut = i;
            },
            ' ' => if (depth == 0 and std.ascii.startsWithIgnoreCase(s[i..], " FROM ")) {
                if (cut == 0 or i - head.len <= max_list) return s;
                return msgfmt.allocPrint(arena, "{s}, ... (+{d}){s}", .{ s[0..cut], commas + 1 - shown, s[i..] });
            },
            else => {},
        }
    }
    return s;
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

/// An explained loop's reports, held until the loop ends and then printed once per
/// plan: the rows whose statement ran the same plan (the same operators, in the same
/// tree) are one report, its figures summed over them and a node whose text differed
/// between them saying so. Reports are grouped in row order, so what prints is the
/// same at any `-j`, a PARALLEL loop's rows included. A loop inside an explained loop
/// reports into the outer one's.
pub const LoopReport = struct {
    gpa: std.mem.Allocator,
    labels: []const []const u8,
    text: std.heap.ArenaAllocator,
    mtx: std.Thread.Mutex = .{},
    entries: std.ArrayListUnmanaged(Entry) = .empty,

    const Entry = struct { row: usize, seq: usize, prof: *Profile, facts: RunFacts, steps: bool };

    /// Takes `prof`'s contents, leaving it empty for its owner to free.
    pub fn take(self: *LoopReport, row: usize, prof: *Profile, facts: RunFacts) !void {
        const p = try self.gpa.create(Profile);
        p.* = prof.*;
        prof.* = Profile.init(self.gpa);
        try self.adopt(row, p, facts, false);
    }

    /// Takes `p`, a profile on the run's allocator. The text it and `facts` hold on
    /// the row's arena is copied: a PARALLEL loop frees a row's arena with the row.
    pub fn adopt(self: *LoopReport, row: usize, p: *Profile, facts_in: RunFacts, steps: bool) !void {
        self.mtx.lock();
        defer self.mtx.unlock();
        const a = self.text.allocator();
        var facts = facts_in;
        facts.sink = try a.dupe(u8, facts.sink);
        p.path = try a.dupe(u8, p.path);
        p.tree = try a.dupe(u8, p.tree);
        for (p.steps.items) |*s| s.tree = p.tree;
        try self.entries.append(self.gpa, .{ .row = row, .seq = self.entries.items.len, .prof = p, .facts = facts, .steps = steps });
    }

    /// Prints the reports grouped by plan, and frees them.
    pub fn finish(self: *LoopReport, arena: std.mem.Allocator) void {
        defer {
            for (self.entries.items) |e| {
                e.prof.deinit();
                self.gpa.destroy(e.prof);
            }
            self.entries.deinit(self.gpa);
            self.text.deinit();
        }
        var aw = std.Io.Writer.Allocating.init(self.gpa);
        defer aw.deinit();
        self.render(arena, &aw.writer) catch return;
        std.fs.File.stderr().writeAll(aw.writer.buffered()) catch {};
    }

    const Group = struct { shape: u64, lead: Entry, facts: RunFacts, rows: std.ArrayListUnmanaged(usize) = .empty, elsewhere: usize = 0 };

    fn render(self: *LoopReport, arena: std.mem.Allocator, w: *std.Io.Writer) !void {
        std.sort.block(Entry, self.entries.items, {}, struct {
            fn lt(_: void, a: Entry, b: Entry) bool {
                return a.row < b.row or (a.row == b.row and a.seq < b.seq);
            }
        }.lt);
        var groups = std.array_list.Managed(Group).init(arena);
        for (self.entries.items) |e| {
            try e.prof.settle();
            const shape = try shapeOf(arena, e.prof, e.steps);
            const g = for (groups.items) |*g| {
                if (g.shape == shape) break g;
            } else blk: {
                try groups.append(.{ .shape = shape, .lead = e, .facts = e.facts });
                break :blk &groups.items[groups.items.len - 1];
            };
            if (g.rows.items.len == 0 or g.rows.items[g.rows.items.len - 1] != e.row) try g.rows.append(arena, e.row);
            if (g.lead.prof == e.prof) continue;
            try g.lead.prof.mergeFrom(e.prof);
            g.facts = sumFacts(g.facts, e.facts);
            if (!std.mem.eql(u8, e.facts.sink, g.lead.facts.sink)) g.elsewhere += 1;
        }
        for (groups.items) |*g| {
            try self.header(w, g.rows.items);
            const color = std.posix.isatty(std.fs.File.stderr().handle) and !std.process.hasEnvVarConstant("NO_COLOR");
            if (g.elsewhere > 0) g.facts.sink = try msgfmt.allocPrint(arena, "{s} (the first row's; {d} of {d} write elsewhere)", .{ g.facts.sink, g.elsewhere, g.lead.prof.rows_merged });
            if (g.lead.steps) {
                try stepTrees(g.lead.prof, try ordered(arena, g.lead.prof), @max(g.lead.prof.laneCount(), 1), color, w);
            } else try renderProfile(arena, g.lead.prof, g.facts, color, w);
        }
    }

    fn header(self: *LoopReport, w: *std.Io.Writer, rows: []const usize) !void {
        const total = self.labels.len;
        if (rows.len == 1) return w.print("FOR EACH row {d} of {d}: {s}\n", .{ rows[0], total, self.label(rows[0]) });
        try w.print("FOR EACH {d} rows of {d} share this plan, its figures summed over them: ", .{ rows.len, total });
        const shown = @min(rows.len, 5);
        for (rows[0..shown], 0..) |r, i| try w.print("{s}{s}", .{ if (i > 0) "; " else "", self.label(r) });
        if (rows.len > shown) try w.print("; +{d} more", .{rows.len - shown});
        try w.writeAll("\n");
    }

    fn label(self: *const LoopReport, row: usize) []const u8 {
        return if (row >= 1 and row <= self.labels.len) self.labels[row - 1] else "?";
    }
};

/// A report's plan as one number: its operators by name and place in the tree.
fn shapeOf(arena: std.mem.Allocator, prof: *const Profile, steps: bool) !u64 {
    var h = std.hash.Wyhash.init(@intFromBool(steps));
    const o = try ordered(arena, prof);
    for (o.ids) |id| {
        hashId(&h, id);
        const n = prof.nodes.getPtr(id) orelse continue;
        h.update(std.mem.asBytes(&n.children.len));
        for (n.children) |c| {
            hashId(&h, c.id);
            h.update(&.{@intFromEnum(c.role)});
        }
        if (n.request) |r| h.update(&.{@intFromEnum(r.purpose)});
    }
    h.update(std.mem.asBytes(&o.ids.len));
    h.update(prof.path);
    return h.final();
}

/// An id field by field: its struct carries padding no write sets.
fn hashId(h: *std.hash.Wyhash, id: profile.Id) void {
    h.update(std.mem.asBytes(&id.at));
    h.update(&.{@intFromEnum(id.kind)});
    h.update(std.mem.asBytes(&id.path));
    h.update(std.mem.asBytes(&id.scope));
}

fn sumFacts(a: RunFacts, b: RunFacts) RunFacts {
    var s = a;
    s.wall_ns += b.wall_ns;
    s.rows_read += b.rows_read;
    s.rows_out += b.rows_out;
    s.lanes = @max(a.lanes, b.lanes);
    s.row_groups += b.row_groups;
    s.row_groups_skipped += b.row_groups_skipped;
    s.columns_read += b.columns_read;
    s.columns_total += b.columns_total;
    s.spill_peak = @max(a.spill_peak, b.spill_peak);
    return s;
}

/// The report, written to stderr in one piece: stdout carries the data, and a
/// writer opened on stderr mid-run overwrote the log when stderr was a file.
pub fn printProfile(env: *Env, prof: *Profile, facts: RunFacts) !void {
    var aw = std.Io.Writer.Allocating.init(env.gpa);
    defer aw.deinit();
    const color = std.posix.isatty(std.fs.File.stderr().handle) and !std.process.hasEnvVarConstant("NO_COLOR");
    try renderProfile(env.arena, prof, facts, color, &aw.writer);
    std.fs.File.stderr().writeAll(aw.writer.buffered()) catch {};
}

const Ordered = struct { ids: []const profile.Id, total_own: u64 };

/// Every node once, the trees before the plan first, root first and inputs after, as
/// the trees print them; a node's number is its place here.
fn ordered(arena: std.mem.Allocator, prof: *const Profile) !Ordered {
    var ids = std.array_list.Managed(profile.Id).init(arena);
    var seen = std.AutoHashMap(profile.Id, void).init(arena);
    for (prof.steps.items) |s| if (s.id) |r| try visit(prof, r, &ids, &seen);
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

/// The trees of what ran besides the plan, each named once, in the order they ran.
fn stepTrees(prof: *const Profile, o: Ordered, lanes: usize, color: bool, w: *std.Io.Writer) !void {
    const steps = prof.steps.items;
    var i: usize = 0;
    var open: ?[]const u8 = null;
    while (i < steps.len) : (i += 1) {
        const id = steps[i].id orelse continue;
        var next = i + 1;
        while (next < steps.len and steps[next].id == null) next += 1;
        const last = next == steps.len or !std.mem.eql(u8, steps[next].tree, steps[i].tree);
        if (open == null or !std.mem.eql(u8, open.?, steps[i].tree)) {
            try w.print("{s} (actuals)\n", .{steps[i].tree});
            open = steps[i].tree;
        }
        try node(prof, o, id, .input, "", last, lanes, color, w);
    }
}

/// `openStep`'s report: its trees alone, as no statement's summary stands over them.
fn renderSteps(arena: std.mem.Allocator, prof: *Profile, w: *std.Io.Writer) !void {
    try prof.settle();
    const color = std.posix.isatty(std.fs.File.stderr().handle) and !std.process.hasEnvVarConstant("NO_COLOR");
    try stepTrees(prof, try ordered(arena, prof), @max(prof.laneCount(), 1), color, w);
}

pub fn renderProfile(arena: std.mem.Allocator, prof: *Profile, facts: RunFacts, color: bool, w: *std.Io.Writer) !void {
    var aw = std.Io.Writer.Allocating.init(arena);
    try renderProfileRaw(arena, prof, facts, color, &aw.writer);
    try @import("../analyze/render.zig").writeShown(w, aw.written());
}

fn renderProfileRaw(arena: std.mem.Allocator, prof: *Profile, facts: RunFacts, color: bool, w: *std.Io.Writer) !void {
    try prof.settle();
    const o = try ordered(arena, prof);
    const lanes = @max(prof.laneCount(), 1);

    try w.writeAll("Summary\n");
    try w.print("  Wall time: {f}   Operator time: {f}", .{ Dur{ .ns = facts.wall_ns }, Dur{ .ns = o.total_own } });
    if (facts.lanes > 1) try w.print(" (summed over {d} lanes)", .{facts.lanes});
    try w.writeAll("\n");
    try w.print("  Rows read: {f}   Rows out: {f}   Path: {s}\n", .{ Count{ .n = facts.rows_read }, Count{ .n = facts.rows_out }, prof.path });
    if (facts.sink.len > 0) try w.print("  Sink: {s}, would write {f} row{s}, {f} before encoding\n", .{ facts.sink, Count{ .n = facts.rows_out }, if (facts.rows_out == 1) "" else "s", Bytes{ .n = prof.sink_bytes.load(.monotonic) } });
    if (facts.row_groups + facts.row_groups_skipped > 0)
        try w.print("  Row groups: {d} read, {d} skipped\n", .{ facts.row_groups, facts.row_groups_skipped });
    if (facts.columns_total > 0) try w.print("  Columns read: {d} of {d}\n", .{ facts.columns_read, facts.columns_total });
    if (facts.spill_peak > 0) try w.print("  Spilled to disk, at most: {f}\n", .{Bytes{ .n = facts.spill_peak }});
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
            try w.print("{d}. {s} (#{d}): {f} ({f})", .{ rank, nodeName(n), numberOf(o, id), Dur{ .ns = own }, Pct{ .v = sh } });
            try paint(w, color, 0, false);
            try w.writeAll("\n");
        }
    }

    try stepTrees(prof, o, lanes, color, w);
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
    try w.print("{s} (#{d})", .{ nodeName(n), numberOf(o, id) });
    const text = if (n.label.len > 0) n.label else prof.detail(id);
    const named = if (text.len > 0) text else if (n.request) |r| r.label else "";
    if (named.len > 0) try w.print("  {s}", .{jsontext.clipUtf8(named, 160)});
    try paint(w, color, 0, false);
    try w.writeAll("\n");

    var kid_prefix_buf: [512]u8 = undefined;
    const kid_prefix = std.fmt.bufPrint(&kid_prefix_buf, "{s}{s}", .{ prefix, if (last) "   " else "│  " }) catch prefix;
    var line_buf: [520]u8 = undefined;
    const line = std.fmt.bufPrint(&line_buf, "{s}{s}", .{ kid_prefix, if (n.children.len > 0) "│  " else "   " }) catch kid_prefix;

    const sent = prof.sentOf(n);
    if (sent.len > 0) {
        var sa = std.heap.ArenaAllocator.init(prof.gpa);
        defer sa.deinit();
        try w.print("{s}sent: {s}\n", .{ line, sentText(sa.allocator(), sent) catch sent });
    }
    if (n.varies > 0) try w.print("{s}the first row's shown: {d} of {d} rows differ here\n", .{ line, n.varies, prof.rows_merged });

    try w.print("{s}time: {f} ({f})", .{ line, Dur{ .ns = own }, Pct{ .v = sh } });
    if (spread(n, prof, lanes, .time)) |s| try w.print(" [min={f}, max={f}]", .{ Dur{ .ns = s.min }, Dur{ .ns = s.max } });
    if (skew(n, prof, lanes, sh)) |x| {
        try paint(w, color, 1, true);
        try w.print("  skew {d:.1}x", .{x});
        try paint(w, color, 0, false);
    }
    try w.writeAll("\n");

    const store: ?op.Request.Purpose = if (n.request) |r| (if (r.purpose.verb() != null) r.purpose else null) else null;
    if (store) |purpose| {
        try w.print("{s}{s}: {f}\n", .{ line, purpose.word(), Count{ .n = n.total.rows } });
    } else {
        try w.print("{s}rows: ", .{line});
        if (n.children.len > 0) {
            const in = n.rowsIn(prof);
            try w.print("{f} -> {f}", .{ Count{ .n = in }, Count{ .n = n.total.rows } });
            if (in > 0 and keepsRows(id.kind)) {
                const ratio = @as(f64, @floatFromInt(n.total.rows)) / @as(f64, @floatFromInt(in));
                if (n.total.rows > in) try w.print(" ({d:.1}x)", .{ratio}) else try w.print(" ({f} kept)", .{Pct{ .v = ratio }});
            }
        } else try w.print("{f}", .{Count{ .n = n.total.rows }});
        try w.print("   batches: {d}", .{n.total.calls});
        if (n.request) |r| if (r.pages > 1) try w.print("   pages: {d}", .{r.pages});
        if (spread(n, prof, lanes, .rows)) |s| try w.print("   rows per lane [min={f}, max={f}]", .{ Count{ .n = s.min }, Count{ .n = s.max } });
        try w.writeAll("\n");
    }

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

fn nodeName(n: *const profile.Node) []const u8 {
    if (n.request) |r| if (r.purpose.verb()) |v| return v;
    return kindName(n.id.kind);
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

test "explain: a request no scan claimed prints before the plan in order, a claimed one on its scan" {
    var p = Profile.init(testing.allocator);
    defer p.deinit();
    const disc = try p.request(.discovery, "db query", "SELECT name\n  FROM tables");
    disc.rows = 2;
    disc.calls = 2;
    _ = try p.request(.probe, "db table orders", "SELECT * FROM orders WHERE 1 = 0");
    var scan = op.Scan{ .src = undefined, .request = try p.request(.read, "db table orders", "SELECT * FROM orders WHERE (a > 1)") };
    scan.stats = .{ .at = 1, .rows = 5, .calls = 1 };
    _ = try p.absorb(.{ .scan = &scan });

    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    var out = std.Io.Writer.Allocating.init(testing.allocator);
    defer out.deinit();
    try renderProfile(ar.allocator(), &p, .{ .wall_ns = 0, .rows_read = 5, .rows_out = 5, .lanes = 1 }, false, &out.writer);
    const printed = out.written();
    try testing.expectEqualStrings(
        \\outside the plan (actuals)
        \\├─ scan (#1)  db query, discovery
        \\│     sent: SELECT name FROM tables
        \\│     time: 0ns (0.0%)
        \\│     rows: 2   batches: 2
        \\└─ scan (#2)  db table orders, schema probe
        \\      sent: SELECT * FROM orders WHERE 1 = 0
        \\      time: 0ns (0.0%)
        \\      rows: 0   batches: 0
        \\plan (actuals)
        \\└─ scan (#3)  db table orders
        \\      sent: SELECT * FROM orders WHERE (a > 1)
        \\      time: 0ns (0.0%)
        \\      rows: 5   batches: 1
        \\
    , printed[std.mem.indexOf(u8, printed, "outside the plan").?..]);
}

test "explain: a span's children hang under it, its own time theirs taken out, and a folder's footers are one node" {
    var p = Profile.init(testing.allocator);
    defer p.deinit();
    trace.current = p.tracer();
    defer trace.current = null;
    {
        var replay = trace.begin(.replay, "s3://bucket/t/");
        defer replay.finish();
        replay.items = 2;
        var list = trace.begin(.list, "s3://bucket/t/_delta_log/");
        list.items = 3;
        list.finish();
        const one = trace.begin(.log_read, "s3://bucket/t/_delta_log/0.json");
        one.finish();
        const two = trace.begin(.log_read, "s3://bucket/t/_delta_log/1.json");
        two.finish();
    }
    trace.record(.footer, "s3://bucket/t/a/1.parquet?sig=secret", 10, 1);
    trace.record(.footer, "s3://bucket/t/b/2.parquet?sig=secret", 10, 1);

    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    var out = std.Io.Writer.Allocating.init(testing.allocator);
    defer out.deinit();
    try renderProfile(ar.allocator(), &p, .{ .wall_ns = 0, .rows_read = 0, .rows_out = 0, .lanes = 1 }, false, &out.writer);
    const printed = out.written();
    for ([_][]const u8{
        "├─ log replay (#1)  s3://bucket/t/\n",
        "live files: 2\n",
        "├─ list (#2)  s3://bucket/t/_delta_log/\n",
        "entries: 3\n",
        "└─ log read (#3)  s3://bucket/t/_delta_log/\n",
        "reads: 2\n",
        "└─ footer (#4)  s3://bucket/t/\n",
    }) |want| {
        testing.expect(std.mem.indexOf(u8, printed, want) != null) catch |e| {
            std.debug.print("{s}\nmissing: {s}\n", .{ printed, want });
            return e;
        };
    }
    try testing.expect(std.mem.indexOf(u8, printed, "secret") == null);
    try testing.expect(p.nodes.count() == 4);
}

test "explain: the sent line cuts a long column list, never the WHERE, and stays on one line" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try testing.expectEqualStrings("SELECT a, b FROM t WHERE x > 1", try sentText(a, "SELECT a,  b\n FROM t\n WHERE x > 1 "));
    const long = "SELECT [column_number_one], [column_number_two], [column_number_three], [column_number_four], [column_number_five], [column_number_six], [column, seven], f(a, b) FROM [t] WHERE ([k] >= 1)";
    try testing.expectEqualStrings("SELECT [column_number_one], [column_number_two], [column_number_three], [column_number_four], [column_number_five], [column_number_six], ... (+2) FROM [t] WHERE ([k] >= 1)", try sentText(a, long));
    try testing.expectEqualStrings("https://host/items?page=1", try sentText(a, "https://host/items?page=1"));
    try testing.expectEqualStrings(
        "SELECT a FROM t WHERE (x IN ('1', '2', '3', '4', '5', '6', '7', '8', ... (+2 values))) AND y IN ('a,b', 'c') AND z = 'IN (1)'",
        try sentText(a, "SELECT a FROM t WHERE (x IN ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10')) AND y IN ('a,b', 'c') AND z = 'IN (1)'"),
    );
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
