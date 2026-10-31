//! ORDER BY and window lanes, a `ParallelSink` that `morsels.drive` feeds: each worker
//! holds the rows of the morsels it claims as an `op.SortPart`, written as sorted
//! runs past its share of `--op-memory`; finalize orders what the workers hold, or
//! merges it with every run, and writes the rows through the tail in a serial run's
//! order. A window sorts by its partition and order keys: the rows held, uncopied,
//! are its input as a serial run's would be, and merged runs it reads a segment of
//! whole partitions at a time (`op.Window.nextSorted`).

const Env = @import("../env.zig").Env;
const Morsels = @import("morsels.zig").Morsels;
const ParallelSink = @import("morsels.zig").ParallelSink;
const Pipeline = @import("morsels.zig").Pipeline;
const drive = @import("morsels.zig").drive;
const OneBatch = @import("../connect.zig").OneBatch;
const RunOptions = @import("../env.zig").RunOptions;
const Stats = @import("../env.zig").Stats;
const LaneMem = @import("../parallel.zig").LaneMem;
const MemSource = @import("../env.zig").MemSource;
const buildOp = @import("../plan.zig").buildOp;
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

/// Recognize `read … | map* | ORDER BY or window | tail | write`, but not a sort a
/// `LIMIT` the top-N takes follows, nor a window a filter follows (`WHERE rn <= k`
/// keeps only `k` rows a partition as they stream), which keep only their rows.
pub fn classifySortPipeline(stages: []const ast.Stage) ?SortShape {
    const middle = stages[1 .. stages.len - 1];
    for (middle, 0..) |st, i| {
        const tail = middle[i + 1 ..];
        switch (st.node) {
            .sort => {
                if (tail.len > 0 and tail[0].node == .limit and op.TopN.fits(tail[0].node.limit)) return null;
                return .{ .prefix = middle[0..i], .srt = st, .tail = tail };
            },
            .window => |wd| {
                if (wd.top_k != null) return null;
                for (tail) |t| if (t.node == .filter) return null;
                return .{ .prefix = middle[0..i], .srt = st, .tail = tail };
            },
            else => if (!analyze.isMap(st.node, false)) return null,
        }
    }
    return null;
}

/// The sort's Global: the shared sort, and every worker's part, freed once the rows
/// are written. A Local is the worker's part.
const SortSink = struct {
    env: *Env,
    shared: *op.SortShared,
    /// Whether rows that fit are ordered here; a window orders them itself.
    order: bool,
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
        _ = try prof.absorbBelow(chain);
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
        try self.shared.finish(self.parts.items, self.order);
        if (self.env.profile) |p| try p.phase("order the lanes' rows", (std.time.Instant.now() catch t0).since(t0));
    }

    const vtable = ParallelSink.VTable{ .initLocal = initLocal, .sink = sink, .combine = combine, .finalize = finalize };
};

pub fn runParallelSort(env: *Env, m: *const Morsels, shape: SortShape, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    const arena = env.arena;
    const pre = try chainAt(env, m.schema().*, shape.prefix);
    const row_schema = pre.out;

    env.src_name = m.label();
    env.sink_name = sinkLabel(env, w);

    var none = OneBatch{ .b = null, .sch = row_schema.* };
    var scan = op.Scan{ .src = none.source() };
    scan.stats.at = profile.merged_at;
    const built = try buildOp(env, try resolveAt(env, row_schema.*, shape.srt), .{ .scan = &scan });
    var base: op.Sort = switch (built.op) {
        .sort => |so| so.*,
        .window => |wo| .{ .child = .{ .scan = &scan }, .in_schema = row_schema, .keys = try std.mem.concat(arena, op.Sort.Key, &.{ wo.part, wo.ord }), .threads = opts.lanes(), .space = env.space, .gpa = env.gpa },
        else => unreachable,
    };
    base.spill_at = env.op_memory / @max(m.workers(opts.lanes()), 1);
    const shared = try arena.create(op.SortShared);
    shared.* = try .init(arena, base);
    defer shared.sort.release();
    const after = try chainAt(env, built.schema, shape.tail);
    var g = SortSink{ .env = env, .shared = shared, .order = built.op == .sort };
    defer g.deinit();
    const p: Pipeline = .{ .m = m, .recs = pre.recs, .errctx = env.errctx, .rows_read = env.rows_read };
    const lanes = try drive(arena, p, g.parallelSink(), opts.lanes());
    lanes_used.* = @max(lanes_used.*, lanes);
    const what = if (built.op == .sort) "sort" else "window";
    env.log.log(.debug, "parallel {s} {s}: {d} {s} over {d} lanes, {d} runs", .{ m.label(), what, m.count(), m.unitName(), lanes, shared.sort.runs });
    if (env.profile) |pr| pr.path = try msgfmt.allocPrint(env.arena, "{s} parallel {s}, {d} {s}", .{ m.label(), what, m.count(), m.unitName() });

    var held: MemSource = undefined;
    var held_scan = op.Scan{ .src = undefined };
    held_scan.stats.at = profile.merged_at;
    const start: op.Op = switch (built.op) {
        .sort => .{ .sort = &shared.sort },
        .window => |wo| blk: {
            wo.done = shared.sort.merge != null;
            if (wo.done) {
                wo.src = &shared.sort;
            } else if (shared.sort.held) |h| {
                held = .{ .m = .{ .schema = row_schema.*, .batches = h.chunks.items } };
                held_scan.src = held.source();
                wo.child = .{ .scan = &held_scan };
                wo.adopt_input = true;
                wo.space = null;
            }
            break :blk built.op;
        },
        else => unreachable,
    };
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
