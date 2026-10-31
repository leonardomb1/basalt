//! Top-N lanes, a `ParallelSink` that `morsels.drive` feeds: each worker keeps its
//! best rows, combined into the final N.

const Env = @import("../env.zig").Env;
const Morsels = @import("morsels.zig").Morsels;
const ParallelSink = @import("morsels.zig").ParallelSink;
const Pipeline = @import("morsels.zig").Pipeline;
const drive = @import("morsels.zig").drive;
const PqPart = @import("agg.zig").PqPart;
const RunOptions = @import("../env.zig").RunOptions;
const Stats = @import("../env.zig").Stats;
const Value = @import("../../exec/value.zig").Value;
const LaneMem = @import("../parallel.zig").LaneMem;
const dispatchWorker = @import("../parallel.zig").dispatchWorker;
const WorkQueue = @import("../parallel.zig").WorkQueue;
const analyze = @import("../analyze.zig");
const ast = @import("../../lang/ast.zig");
const chainAt = @import("../plan.zig").chainAt;
const op = @import("../../exec/op.zig");
const openTailSink = @import("agg.zig").openTailSink;
const parallel = @import("../parallel.zig");
const resolveAt = @import("../plan.zig").resolveAt;
const SinkGuard = @import("../../connect/guard.zig").SinkGuard;
const sinkLabel = @import("../connect.zig").sinkLabel;
const std = @import("std");
const msgfmt = @import("../../format/msgfmt.zig");
const types = @import("../../lang/types.zig");

const TopNTail = struct { keys: []const ast.SortKey, n: usize };

/// A tail of only sorts and limits, so each partition keeps its own best `n` rows. An
/// `OFFSET` disables it: the rows a partition discards could be the ones it lands on.
pub fn topNTail(tail: []const ast.Stage) ?TopNTail {
    var keys: []const ast.SortKey = &.{};
    var n: ?usize = null;
    for (tail) |st| switch (st.node) {
        .sort => |so| keys = so.keys,
        .limit => |l| {
            if (l.offset != 0) return null;
            n = @intCast(l.count);
        },
        else => return null,
    };
    if (keys.len == 0) return null;
    return .{ .keys = keys, .n = n orelse return null };
}

/// The output value a sort key refers to: group keys first, then the aggregates. A sum
/// out of range only orders here; it fails the query when emitted.
fn groupSortValue(set: *const op.Aggregate.GroupSet, gi: usize, col: usize, by_len: usize) Value {
    if (col < by_len) return set.keyValue(gi, col);
    return set.finish(gi, col - by_len) catch .null;
}

const GroupOrder = struct {
    keys: []const op.Sort.Key,
    by_len: usize,
    aggs: []const op.Aggregate.Agg,
    set: *const op.Aggregate.GroupSet = undefined,

    fn less(self: GroupOrder, a: u32, b: u32) bool {
        for (self.keys) |k| {
            const av = groupSortValue(self.set, a, k.idx, self.by_len);
            const bv = groupSortValue(self.set, b, k.idx, self.by_len);
            const o = op.keyOrder(av, bv, k.desc, k.nulls_first);
            if (o != .eq) return o == .lt;
        }
        return false;
    }
};

pub const PqTopNCtx = struct {
    parts: []PqPart,
    order: GroupOrder,
    n: usize,
    queue: WorkQueue,
    sel: [][]const u32,
};

pub const pqTopNWorker = dispatchWorker(PqTopNCtx, pqTopNOne);

fn pqTopNOne(ctx: *PqTopNCtx, p: usize) !void {
    const pp = &ctx.parts[p];
    const m = if (pp.merge) |*x| x else {
        ctx.sel[p] = &.{};
        return;
    };
    const set = try m.result();
    const idx = try pp.arena.allocator().alloc(u32, set.len);
    for (idx, 0..) |*x, i| x.* = @intCast(i);
    if (set.len > ctx.n) {
        const set_ptr = try pp.arena.allocator().create(op.Aggregate.GroupSet);
        set_ptr.* = set;
        var order = ctx.order;
        order.set = set_ptr;
        std.sort.pdq(u32, idx, order, GroupOrder.less);
    }
    ctx.sel[p] = idx[0..@min(idx.len, ctx.n)];
}

pub const TopNShape = struct { prefix: []const ast.Stage, srt: ast.Stage, lim: ast.Limit };

pub fn classifyTopNPipeline(stages: []const ast.Stage) ?TopNShape {
    const middle = stages[1 .. stages.len - 1];
    if (middle.len < 2) return null;
    if (middle[middle.len - 1].node != .limit or middle[middle.len - 2].node != .sort) return null;
    if (!op.TopN.fits(middle[middle.len - 1].node.limit)) return null;
    const prefix = middle[0 .. middle.len - 2];
    for (prefix) |st| if (!analyze.isMap(st.node, false)) return null;
    return .{ .prefix = prefix, .srt = middle[middle.len - 2], .lim = middle[middle.len - 1].node.limit };
}

/// Top-N as a parallel sink: a worker keeps its best rows over every morsel it claims,
/// each row placed by its morsel and position there so ties keep the rows a serial
/// run keeps; finalize combines the workers' rows into the final N.
const TopNSink = struct {
    env: *Env,
    row_schema: *const types.Schema,
    keys: []const op.Sort.Key,
    cap: u64,
    lim: ast.Limit,
    kept: std.array_list.Managed(op.TopN.Kept),
    kept_arena: std.mem.Allocator,
    mtx: std.Thread.Mutex = .{},
    top: ?op.Batch = null,

    fn parallelSink(self: *TopNSink) ParallelSink {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn initLocal(_: *anyopaque, mem: *LaneMem) anyerror!*anyopaque {
        return mem;
    }

    fn sink(ptr: *anyopaque, local: *anyopaque, _: usize, chain: op.Op, at: *const usize) anyerror!void {
        const self: *TopNSink = @ptrCast(@alignCast(ptr));
        const mem: *LaneMem = @ptrCast(@alignCast(local));
        var tn = op.TopN{
            .child = chain,
            .in_schema = self.row_schema,
            .keys = self.keys,
            .count = self.cap,
            .offset = 0,
            .state = mem.batch.allocator(),
            .gpa = mem.gpa.allocator(),
            .item = at,
        };
        const prof = self.env.profile;
        const t0 = if (prof != null) std.time.Instant.now() catch null else null;
        const kept = try tn.nextKept(mem.batch.allocator());
        if (prof) |p| {
            if (t0) |st| tn.stats.ns = if (std.time.Instant.now()) |now| now.since(st) else |_| 0;
            tn.stats.rows = if (kept) |k| k.rows.len else 0;
            tn.stats.calls = 1;
            _ = try p.absorb(.{ .top_n = &tn });
        }
        const best = kept orelse return;
        self.mtx.lock();
        defer self.mtx.unlock();
        try self.kept.append(.{ .rows = try best.rows.deepCopy(self.kept_arena), .seqs = try self.kept_arena.dupe(u64, best.seqs) });
    }

    fn combine(_: *anyopaque, _: *anyopaque) anyerror!void {}

    fn finalize(ptr: *anyopaque) anyerror!void {
        const self: *TopNSink = @ptrCast(@alignCast(ptr));
        const env = self.env;
        const t0 = std.time.Instant.now() catch unreachable;
        self.top = try op.TopN.combine(env.gpa, env.arena, self.row_schema, self.keys, self.lim.count, self.lim.offset, self.kept.items);
        if (env.profile) |p| try p.phase("combine the lanes' best rows", (std.time.Instant.now() catch t0).since(t0));
    }

    const vtable = ParallelSink.VTable{ .initLocal = initLocal, .sink = sink, .combine = combine, .finalize = finalize };
};

pub fn runParallelTopN(
    env: *Env,
    m: *const Morsels,
    prefix: []const ast.Stage,
    srt: ast.Stage,
    lim: ast.Limit,
    w: ast.Write,
    opts: RunOptions,
    stats: *Stats,
    lanes_used: *usize,
) anyerror!bool {
    const arena = env.arena;
    const pre = try chainAt(env, m.schema().*, prefix);
    const row_schema = pre.out;
    const ks = (try resolveAt(env, row_schema.*, srt)).plan.sort;

    env.src_name = m.label();
    env.sink_name = sinkLabel(env, w);

    var kept_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer kept_arena.deinit();
    var g = TopNSink{
        .env = env,
        .row_schema = row_schema,
        .keys = ks,
        .cap = lim.offset + lim.count,
        .lim = lim,
        .kept = .init(env.gpa),
        .kept_arena = kept_arena.allocator(),
    };
    defer g.kept.deinit();
    const p: Pipeline = .{ .m = m, .recs = pre.recs, .errctx = env.errctx, .rows_read = env.rows_read, .per_lane = true };
    const lanes = try drive(arena, p, g.parallelSink(), opts.lanes());
    lanes_used.* = @max(lanes_used.*, lanes);
    env.log.log(.debug, "parallel {s} top-n: {d} {s} over {d} lanes", .{ m.label(), m.count(), m.unitName(), lanes });
    if (env.profile) |pr| pr.path = try msgfmt.allocPrint(env.arena, "{s} parallel top-n, {d} {s}", .{ m.label(), m.count(), m.unitName() });
    const top = g.top;

    const snk = try openTailSink(env, w, row_schema.*);
    var guard = SinkGuard.init(snk);
    defer guard.abort();
    if (top) |b| {
        try snk.writeBatch(arena, b);
        stats.rows_out += b.len;
    }
    try guard.close();
    return true;
}
