//! Top-N lanes: each lane keeps its best rows, merged into the final N.

const Env = @import("../env.zig").Env;
const LaneSplit = @import("sources.zig").LaneSplit;
const MorselSource = @import("agg.zig").MorselSource;
const PqPart = @import("agg.zig").PqPart;
const RunOptions = @import("../env.zig").RunOptions;
const Stats = @import("../env.zig").Stats;
const Value = @import("../../exec/value.zig").Value;
const WorkQueue = @import("../lanes.zig").WorkQueue;
const aErr = @import("../plan.zig").aErr;
const analyze = @import("../analyze.zig");
const ast = @import("../../lang/ast.zig");
const buildMapChain = @import("../plan.zig").buildMapChain;
const csvSplitFile = @import("sources.zig").csvSplitFile;
const dispatchWorker = @import("../lanes.zig").dispatchWorker;
const driver = @import("../../connect/driver.zig");
const eval = @import("../../exec/eval.zig");
const laneRowSource = @import("sources.zig").laneRowSource;
const mapChainSchema = @import("../plan.zig").mapChainSchema;
const obs = @import("../obs.zig");
const op = @import("../../exec/op.zig");
const openSink = @import("../connect.zig").openSink;
const parallel = @import("../parallel.zig");
const parquetSplit = @import("sources.zig").parquetSplit;
const resolveUpsertKeys = @import("../connect.zig").resolveUpsertKeys;
const schemaPtr = @import("../env.zig").schemaPtr;
const sinkLabel = @import("../connect.zig").sinkLabel;
const std = @import("std");
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
fn groupSortValue(set: *const op.Aggregate.GroupSet, gi: usize, col: usize, by_len: usize, aggs: []const op.Aggregate.Agg) Value {
    if (col < by_len) return set.keyValue(gi, col);
    return op.Aggregate.finalizeAcc(set.acc(gi, col - by_len), aggs[col - by_len]) catch .null;
}

const GroupOrder = struct {
    cols: []const usize,
    desc: []const bool,
    by_len: usize,
    aggs: []const op.Aggregate.Agg,
    set: *const op.Aggregate.GroupSet = undefined,

    fn less(self: GroupOrder, a: u32, b: u32) bool {
        for (self.cols, self.desc) |c, d| {
            const av = groupSortValue(self.set, a, c, self.by_len, self.aggs);
            const bv = groupSortValue(self.set, b, c, self.by_len, self.aggs);
            const o = eval.compareValues(av, bv) orelse .eq;
            if (o != .eq) return if (d) o == .gt else o == .lt;
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
    const set = m.result();
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

pub const TopNShape = struct { prefix: []const ast.Stage, srt: ast.Sort, lim: ast.Limit };

pub fn classifyTopNPipeline(stages: []const ast.Stage) ?TopNShape {
    const middle = stages[1 .. stages.len - 1];
    if (middle.len < 2) return null;
    if (middle[middle.len - 1].node != .limit or middle[middle.len - 2].node != .sort) return null;
    if (!op.TopN.fits(middle[middle.len - 1].node.limit)) return null;
    const prefix = middle[0 .. middle.len - 2];
    for (prefix) |st| switch (st.node) {
        .filter, .select => {},
        else => return null,
    };
    return .{ .prefix = prefix, .srt = middle[middle.len - 2].node.sort, .lim = middle[middle.len - 1].node.limit };
}

const TopNCtx = struct {
    split: LaneSplit,
    row_schema: *const types.Schema,
    prefix: []const ast.Stage,
    params: *std.StringHashMap(*const ast.Expr),
    errctx: ?*op.ErrCtx = null,
    keys: []const op.Sort.Key,
    cap: u64,
    queue: WorkQueue,
    kept: std.array_list.Managed(op.TopN.Entry),
    kept_arena: std.mem.Allocator,
    mtx: std.Thread.Mutex = .{},
    rows_read: *obs.RowCounter,
};

const topnWorker = dispatchWorker(TopNCtx, topnWorkOne);

fn topnWorkOne(ctx: *TopNCtx, i: usize) !void {
    var wgpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = false }){};
    defer _ = wgpa.deinit();
    var warena = std.heap.ArenaAllocator.init(wgpa.allocator());
    defer warena.deinit();
    var batch_arena = std.heap.ArenaAllocator.init(wgpa.allocator());
    defer batch_arena.deinit();

    const src_schema = ctx.split.schema();
    var item_ptr: ?*const usize = null;
    const inner: driver.Source = switch (ctx.split) {
        .parquet => |*m| blk: {
            const ms = try warena.allocator().create(MorselSource);
            ms.* = .{ .m = m, .scratch = warena.allocator() };
            item_ptr = &ms.cur_item;
            break :blk .{ .ptr = ms, .vtable = &MorselSource.vtable };
        },
        .csv => (try laneRowSource(ctx.split.unorderedRows(i, ctx.queue.nitems), warena.allocator())) orelse return,
    };
    defer inner.close();
    errdefer ctx.split.abort();
    var cs = obs.CountingSource{ .inner = inner, .count = ctx.rows_read };
    var scan = op.Scan{ .src = cs.source() };
    const child = try buildMapChain(warena.allocator(), ctx.params, ctx.errctx, ctx.prefix, &scan, src_schema);
    var tn = op.TopN{
        .child = child,
        .in_schema = ctx.row_schema,
        .keys = ctx.keys,
        .count = ctx.cap,
        .offset = 0,
        .state = batch_arena.allocator(),
        .gpa = wgpa.allocator(),
        .seq_base = @as(u64, i) << 40,
        .item = item_ptr,
    };
    const local = (try tn.nextEntries(batch_arena.allocator())) orelse return;

    ctx.mtx.lock();
    defer ctx.mtx.unlock();
    for (local) |e| {
        const row = try ctx.kept_arena.alloc(Value, e.len);
        for (row, e) |*o, v| o.* = try op.dupeValue(ctx.kept_arena, v);
        try ctx.kept.append(row);
    }
}

fn runParallelTopN(
    env: *Env,
    split: LaneSplit,
    prefix: []const ast.Stage,
    srt: ast.Sort,
    lim: ast.Limit,
    w: ast.Write,
    opts: RunOptions,
    stats: *Stats,
    lanes_used: *usize,
) anyerror!bool {
    const arena = env.arena;
    const row_schema = try schemaPtr(arena, try mapChainSchema(env, prefix, split.schema().*));

    const qs = try arena.alloc(ast.QualName, srt.keys.len);
    for (srt.keys, qs) |sk, *q| q.* = sk.field;
    var ad = analyze.Diag{};
    const idxs = analyze.fieldIndices(arena, row_schema.*, qs, &ad) catch |e| return aErr(env, &ad, e);
    const ks = try arena.alloc(op.Sort.Key, srt.keys.len);
    for (srt.keys, idxs, ks) |sk, idx, *k| k.* = .{ .idx = idx, .desc = sk.desc };

    env.src_name = split.label();
    env.sink_name = sinkLabel(env, w);

    const nthreads = split.lanes(opts.threads);
    var kept_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer kept_arena.deinit();
    var ctx = TopNCtx{
        .split = split,
        .row_schema = row_schema,
        .prefix = prefix,
        .params = env.params_expr,
        .errctx = env.errctx,
        .keys = ks,
        .cap = lim.offset + lim.count,
        .queue = .{ .nitems = nthreads },
        .kept = .init(env.gpa),
        .kept_arena = kept_arena.allocator(),
        .rows_read = env.rows_read,
    };
    defer ctx.kept.deinit();

    const lanes = try parallel.spawnJoin(arena, nthreads, topnWorker, &ctx);
    lanes_used.* = @max(lanes_used.*, lanes);
    env.log.log(.debug, "parallel {s} top-n: {d} {s} over {d} lanes", .{ split.label(), split.count(nthreads), split.unitName(), lanes });
    if (ctx.queue.failure()) |e| return e;

    std.mem.sort(op.TopN.Entry, ctx.kept.items, ks, struct {
        fn lt(keys: []const op.Sort.Key, x: op.TopN.Entry, y: op.TopN.Entry) bool {
            return op.entryLess(x, y, keys);
        }
    }.lt);
    var global = op.TopN{ .child = undefined, .in_schema = row_schema, .keys = ks, .count = lim.count, .offset = lim.offset, .state = arena, .gpa = env.gpa };
    const start = @min(lim.offset, ctx.kept.items.len);
    const end = @min(lim.offset +| lim.count, ctx.kept.items.len);

    const wr = try resolveUpsertKeys(env, w);
    const snk = try openSink(env, wr, row_schema.*);
    var snk_open = true;
    errdefer if (snk_open) snk.abort();
    if (start < end) {
        const b = try global.emit(arena, ctx.kept.items[start..end]);
        try snk.writeBatch(arena, b);
        stats.rows_out += b.len;
    }
    snk_open = false;
    try snk.close();
    return true;
}

pub fn runParallelParquetTopN(env: *Env, rd: ast.Read, pipeline: []const ast.Stage, prefix: []const ast.Stage, srt: ast.Sort, lim: ast.Limit, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    const split = (try parquetSplit(env, rd, pipeline[1..], w, opts)) orelse return false;
    return runParallelTopN(env, split, prefix, srt, lim, w, opts, stats, lanes_used);
}

pub fn runParallelCsvTopN(env: *Env, rd: ast.Read, prefix: []const ast.Stage, srt: ast.Sort, lim: ast.Limit, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    const mapped = (try csvSplitFile(env, rd, w)) orelse return false;
    defer mapped.close();
    return runParallelTopN(env, .{ .csv = .{ .mapped = mapped, .schema = &mapped.schema } }, prefix, srt, lim, w, opts, stats, lanes_used);
}
