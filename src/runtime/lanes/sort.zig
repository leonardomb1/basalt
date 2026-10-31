//! ORDER BY lanes, a `ParallelSink` that `morsels.drive` feeds: each worker holds the
//! rows of the morsels it claims as an `op.SortPart`, written as sorted runs past
//! its share of `--op-memory`; finalize orders what the workers hold, or merges it
//! with every run, and writes the rows through the tail in a serial run's order.

const Env = @import("../env.zig").Env;
const Morsels = @import("morsels.zig").Morsels;
const ParallelSink = @import("morsels.zig").ParallelSink;
const Pipeline = @import("morsels.zig").Pipeline;
const drive = @import("morsels.zig").drive;
const OneBatch = @import("../connect.zig").OneBatch;
const RunOptions = @import("../env.zig").RunOptions;
const Stats = @import("../env.zig").Stats;
const LaneMem = @import("../parallel.zig").LaneMem;
const analyze = @import("../analyze.zig");
const ast = @import("../../lang/ast.zig");
const chainAt = @import("../plan.zig").chainAt;
const op = @import("../../exec/op.zig");
const openTailSink = @import("agg.zig").openTailSink;
const profile = @import("../../exec/profile.zig");
const resolveAt = @import("../plan.zig").resolveAt;
const SinkGuard = @import("../../connect/guard.zig").SinkGuard;
const sinkLabel = @import("../connect.zig").sinkLabel;
const std = @import("std");
const msgfmt = @import("../../format/msgfmt.zig");
const writeTailOver = @import("agg.zig").writeTailOver;

pub const SortShape = struct { prefix: []const ast.Stage, srt: ast.Stage, tail: []const ast.Stage };

/// Recognize `read … | map* | ORDER BY | tail | write`, but not a sort a `LIMIT` the
/// top-N takes follows, which keeps only its rows as they stream.
pub fn classifySortPipeline(stages: []const ast.Stage) ?SortShape {
    const middle = stages[1 .. stages.len - 1];
    for (middle, 0..) |st, i| {
        if (st.node == .sort) {
            if (i + 1 < middle.len and middle[i + 1].node == .limit and op.TopN.fits(middle[i + 1].node.limit)) return null;
            return .{ .prefix = middle[0..i], .srt = st, .tail = middle[i + 1 ..] };
        }
        if (!analyze.isMap(st.node, false)) return null;
    }
    return null;
}

/// The sort's Global: the shared sort, and every worker's part, freed once the rows
/// are written. A Local is the worker's part.
const SortSink = struct {
    env: *Env,
    shared: *op.SortShared,
    parts: std.ArrayListUnmanaged(*op.SortPart) = .empty,
    mtx: std.Thread.Mutex = .{},
    /// The kind of the lanes' top operator, which the tail stands above in a profile.
    feeds: profile.Kind = .scan,

    fn parallelSink(self: *SortSink) ParallelSink {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn deinit(self: *SortSink) void {
        for (self.parts.items) |p| p.destroy();
        self.parts.deinit(self.env.gpa);
    }

    fn initLocal(ptr: *anyopaque, _: *LaneMem) anyerror!*anyopaque {
        const self: *SortSink = @ptrCast(@alignCast(ptr));
        self.mtx.lock();
        defer self.mtx.unlock();
        try self.parts.ensureUnusedCapacity(self.env.gpa, 1);
        const p = try op.SortPart.create(self.env.gpa);
        self.parts.appendAssumeCapacity(p);
        return p;
    }

    fn sink(ptr: *anyopaque, local: *anyopaque, i: usize, chain: op.Op, _: *const usize) anyerror!void {
        const self: *SortSink = @ptrCast(@alignCast(ptr));
        const p: *op.SortPart = @ptrCast(@alignCast(local));
        var row: u64 = 0;
        while (try chain.next(p.held.pull())) |b| {
            try p.add(self.shared, b, (@as(u64, i) << 32) | row);
            row += b.len;
        }
        const prof = self.env.profile orelse return;
        _ = try prof.absorb(chain);
        self.mtx.lock();
        defer self.mtx.unlock();
        self.feeds = chain;
    }

    fn combine(_: *anyopaque, local: *anyopaque) anyerror!void {
        const p: *op.SortPart = @ptrCast(@alignCast(local));
        p.held.donePulling();
    }

    fn finalize(ptr: *anyopaque) anyerror!void {
        const self: *SortSink = @ptrCast(@alignCast(ptr));
        const t0 = std.time.Instant.now() catch unreachable;
        try self.shared.finish(self.parts.items);
        if (self.env.profile) |p| try p.phase("order the lanes' rows", (std.time.Instant.now() catch t0).since(t0));
    }

    const vtable = ParallelSink.VTable{ .initLocal = initLocal, .sink = sink, .combine = combine, .finalize = finalize };
};

pub fn runParallelSort(env: *Env, m: *const Morsels, shape: SortShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    const arena = env.arena;
    const pre = try chainAt(env, m.schema().*, shape.prefix);
    const row_schema = pre.out;
    const keys = (try resolveAt(env, row_schema.*, shape.srt)).plan.sort;

    env.src_name = m.label();
    env.sink_name = sinkLabel(env, w);

    const workers = m.workers(opts.lanes());
    var none = OneBatch{ .b = null, .sch = row_schema.* };
    var scan = op.Scan{ .src = none.source() };
    scan.stats.at = profile.merged_at;
    const shared = try arena.create(op.SortShared);
    shared.* = try .init(arena, .{
        .child = .{ .scan = &scan },
        .in_schema = row_schema,
        .keys = keys,
        .threads = opts.lanes(),
        .space = env.space,
        .spill_at = env.op_memory / @max(workers, 1),
        .gpa = env.gpa,
    });
    const after = try chainAt(env, row_schema.*, shape.tail);
    var g = SortSink{ .env = env, .shared = shared };
    defer g.deinit();
    const p: Pipeline = .{ .m = m, .recs = pre.recs, .errctx = env.errctx, .rows_read = env.rows_read };
    const lanes = drive(arena, p, g.parallelSink(), opts.lanes()) catch |e| {
        shared.sort.release();
        return e;
    };
    lanes_used.* = @max(lanes_used.*, lanes);
    env.log.log(.debug, "parallel {s} sort: {d} {s} over {d} lanes, {d} runs", .{ m.label(), m.count(), m.unitName(), lanes, shared.sort.runs });
    if (env.profile) |pr| pr.path = try msgfmt.allocPrint(env.arena, "{s} parallel sort, {d} {s}", .{ m.label(), m.count(), m.unitName() });

    const start: op.Op = .{ .sort = &shared.sort };
    start.stampNew(profile.keyOf(shape.srt.pos));
    const snk = openTailSink(env, w, after.out.*) catch |e| {
        start.release();
        return e;
    };
    var guard = SinkGuard.init(snk);
    defer guard.abort();
    try writeTailOver(env, snk, start, after.recs, stats, g.feeds);
    try guard.close();
    return true;
}
