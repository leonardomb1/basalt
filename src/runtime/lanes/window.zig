//! A top-k window on lanes: a lone `ROW_NUMBER()` filtered `rn <= k` (`Window.top_k`)
//! over a file, partitions spread over buckets by the hash of their key, so each
//! bucket's top-k core owns its partitions whole and nothing is merged twice. Lanes
//! keeping their own best `k` a partition were no faster: with many small partitions
//! each lane kept nearly every row, and the merge ran them all again on one thread.
//!
//! 1. Each lane streams the morsels it claims through the map stages and sends every
//!    row to a bucket by its partition key's hash, with its morsel and position there
//!    (`seq`, as `TopN.feed` numbers rows), so ties rank as one serial input ranks them.
//! 2. Each bucket, on a lane, offers its rows from every lane to a top-k core and keeps
//!    the window's rows: its partitions in key order, each one's rows by the order
//!    keys then `seq`.
//! 3. A partition lies in one bucket, so its rows are one run there; the runs are put
//!    in key order across buckets. The output is the serial window's to the row.

const Batch = @import("../../exec/batch.zig").Batch;
const Env = @import("../env.zig").Env;
const KeyArr = @import("../../exec/op/sort.zig").KeyArr;
const LaneMem = @import("../parallel.zig").LaneMem;
const Materialized = @import("../env.zig").Materialized;
const Morsels = @import("morsels.zig").Morsels;
const ParallelSink = @import("morsels.zig").ParallelSink;
const Pipeline = @import("morsels.zig").Pipeline;
const drive = @import("morsels.zig").drive;
const RunOptions = @import("../env.zig").RunOptions;
const TopK = @import("../../exec/op/topk.zig").TopK;
const WorkQueue = @import("../parallel.zig").WorkQueue;
const analyze = @import("../analyze.zig");
const ast = @import("../../lang/ast.zig");
const chainAt = @import("../plan.zig").chainAt;
const column = @import("../../exec/column.zig");
const keyhash = @import("../../exec/keyhash.zig");
const msgfmt = @import("../../format/msgfmt.zig");
const op = @import("../../exec/op.zig");
const parallel = @import("../parallel.zig");
const resolveAt = @import("../plan.zig").resolveAt;
const sortIdx = @import("../../exec/op/sort.zig").sortIdx;
const std = @import("std");
const pagecache = @import("../../exec/pagecache.zig");
const types = @import("../../lang/types.zig");

/// Rows one lane sent to one bucket, with their positions.
const Part = struct { rows: Batch, seqs: []const u64 };

/// The top-k window's Global: every worker's buckets, kept until the window's rows are
/// out. A Local is the worker's `Lane`.
const TopKSink = struct {
    env: *Env,
    in_schema: *const types.Schema,
    out_schema: *const types.Schema,
    part: []const usize,
    ord: []const op.Sort.Key,
    k: usize,
    nb: usize,
    threads: usize,
    mtx: std.Thread.Mutex = .{},
    lanes: std.ArrayListUnmanaged(*Lane) = .empty,
    /// Bucket `b`'s window rows, set by `finalize`.
    outs: []?Batch = &.{},
    out_arenas: []std.heap.ArenaAllocator = &.{},

    /// What one worker sent to each bucket, copied into its own arena.
    const Lane = struct { keep: std.heap.ArenaAllocator, sent: []std.ArrayListUnmanaged(Part), mem: *LaneMem };

    fn parallelSink(self: *TopKSink) ParallelSink {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn deinit(self: *TopKSink) void {
        const gpa = self.env.gpa;
        for (self.lanes.items) |l| {
            l.keep.deinit();
            gpa.destroy(l);
        }
        self.lanes.deinit(gpa);
        for (self.out_arenas) |*oa| oa.deinit();
    }

    fn initLocal(ptr: *anyopaque, mem: *LaneMem) anyerror!*anyopaque {
        const self: *TopKSink = @ptrCast(@alignCast(ptr));
        const gpa = self.env.gpa;
        self.mtx.lock();
        defer self.mtx.unlock();
        try self.lanes.ensureUnusedCapacity(gpa, 1);
        const l = try gpa.create(Lane);
        l.* = .{ .keep = .init(pagecache.process), .sent = &.{}, .mem = mem };
        self.lanes.appendAssumeCapacity(l);
        l.sent = try l.keep.allocator().alloc(std.ArrayListUnmanaged(Part), self.nb);
        for (l.sent) |*x| x.* = .empty;
        return l;
    }

    /// Phase 1: the worker's morsels through the map stages, each batch's rows sent to
    /// their buckets with their positions, `at` the morsel they come from.
    fn sink(ptr: *anyopaque, local: *anyopaque, _: usize, chain: op.Op, at: *const usize) anyerror!void {
        const self: *TopKSink = @ptrCast(@alignCast(ptr));
        const l: *Lane = @ptrCast(@alignCast(local));
        const keep = l.keep.allocator();
        var last: usize = std.math.maxInt(usize);
        var base: u64 = 0;
        var seen: u64 = 0;
        const idx = try l.mem.work.allocator().alloc(std.ArrayListUnmanaged(usize), self.nb);
        while (true) {
            _ = l.mem.batch.reset(.retain_capacity);
            const a = l.mem.batch.allocator();
            const b = (try chain.next(a)) orelse break;
            if (b.len == 0) continue;
            if (at.* != last) {
                last = at.*;
                base = @as(u64, last) << 40;
                seen = 0;
            }
            for (idx) |*x| x.* = .empty;
            for (0..b.len) |r| try idx[bucketOf(b, self.part, r, self.nb)].append(a, r);
            for (idx, l.sent) |x, *bk| {
                if (x.items.len == 0) continue;
                const cols = try keep.alloc(column.Column, b.columns.len);
                for (b.columns, cols) |c, *o| o.* = try column.permute(keep, c, x.items);
                const seqs = try keep.alloc(u64, x.items.len);
                for (seqs, x.items) |*q, r| q.* = base + seen + r;
                try bk.append(keep, .{ .rows = .{ .schema = b.schema, .columns = cols, .len = x.items.len }, .seqs = seqs });
            }
            seen += b.len;
        }
        if (self.env.profile) |p| _ = try p.absorb(chain);
    }

    fn combine(_: *anyopaque, _: *anyopaque) anyerror!void {}

    /// Phase 2: every bucket, on a lane, through its own top-k core.
    fn finalize(ptr: *anyopaque) anyerror!void {
        const self: *TopKSink = @ptrCast(@alignCast(ptr));
        const arena = self.env.arena;
        const t0 = std.time.Instant.now() catch null;
        self.outs = try arena.alloc(?Batch, self.nb);
        @memset(self.outs, null);
        self.out_arenas = try arena.alloc(std.heap.ArenaAllocator, self.threads);
        for (self.out_arenas) |*oa| oa.* = .init(self.env.gpa);
        var ctx = BucketCtx{ .g = self, .queue = .{ .nitems = self.nb } };
        _ = try parallel.spawnJoin(arena, @min(self.threads, self.nb), bucketLane, &ctx);
        if (ctx.queue.failure()) |e| return e;
        if (self.env.profile) |p| if (t0) |st| try p.phase("top-k each bucket's partitions, in key order", (std.time.Instant.now() catch st).since(st));
    }

    const vtable = ParallelSink.VTable{ .initLocal = initLocal, .sink = sink, .combine = combine, .finalize = finalize };
};

/// The bucket of row `r`: its partition key's hash, a null hashed as itself, so
/// rows of one partition share one bucket.
fn bucketOf(b: Batch, part: []const usize, r: usize, nb: usize) usize {
    var h = std.hash.Wyhash.init(0);
    for (part) |ci| {
        const c = &b.columns[ci];
        if (!c.validity.get(r)) {
            keyhash.hashValue(&h, .null);
        } else keyhash.hashCell(&h, c, r);
    }
    return @intCast(h.final() % nb);
}

const BucketCtx = struct { g: *TopKSink, queue: WorkQueue };

fn bucketLane(bc: *BucketCtx, lane_idx: usize) void {
    while (bc.queue.take()) |bi| {
        bucketTask(bc.g, bi, bc.g.out_arenas[lane_idx].allocator()) catch |e| {
            bc.queue.fail(e);
            return;
        };
    }
}

/// Bucket `bi`'s rows from every worker through one top-k core, its window rows into
/// `out_arena`.
fn bucketTask(g: *TopKSink, bi: usize, out_arena: std.mem.Allocator) !void {
    var gpa_state = std.heap.GeneralPurposeAllocator(.{ .thread_safe = false }){};
    defer _ = gpa_state.deinit();
    const gpa = gpa_state.allocator();
    var core = TopK.init(gpa, g.in_schema, g.ord, g.part, g.k);
    defer core.deinit();
    var scratch = std.heap.ArenaAllocator.init(gpa);
    defer scratch.deinit();
    for (g.lanes.items) |l| for (l.sent[bi].items) |p| {
        try core.add(scratch.allocator(), p.rows, 0, p.seqs);
        _ = scratch.reset(.retain_capacity);
    };
    g.outs[bi] = try op.Window.topKRows(&core, out_arena, g.in_schema, g.out_schema);
}

/// The window stage `ws` (its `top_k` set) over `m` through the map stages `prefix`,
/// run on lanes; its output as the serial window gives it, held in memory.
pub fn runWindowTopK(env: *Env, m: *const Morsels, prefix: []const ast.Stage, ws: ast.Stage, opts: RunOptions, lanes_used: *usize) anyerror!Materialized {
    const arena = env.arena;
    const pre = try chainAt(env, m.schema().*, prefix);
    const r = try resolveAt(env, pre.out.*, ws);
    const wp = r.plan.window;
    const wd = ws.node.window;
    const ord = try arena.alloc(op.Sort.Key, wp.ord.len);
    for (wd.order_by, wp.ord, ord) |sk, idx, *key| key.* = .{ .idx = idx, .desc = sk.desc, .nulls_first = sk.nulls_first };

    const nlanes = m.workers(opts.lanes());
    var g = TopKSink{
        .env = env,
        .in_schema = r.in,
        .out_schema = r.out,
        .part = try arena.dupe(usize, wp.part),
        .ord = ord,
        .k = @intCast(wd.top_k.?),
        .nb = if (wp.part.len == 0) 1 else nlanes * 4,
        .threads = nlanes,
    };
    defer g.deinit();
    const p: Pipeline = .{ .m = m, .recs = pre.recs, .errctx = env.errctx, .rows_read = env.rows_read, .per_lane = true };
    lanes_used.* = @max(lanes_used.*, try drive(arena, p, g.parallelSink(), opts.lanes()));
    const out = try keyOrder(env.shared orelse env.arena, &g);
    if (env.profile) |pr| pr.path = try msgfmt.allocPrint(arena, "{s} parallel top-k window, {d} {s}, {d} buckets", .{ m.label(), m.count(), m.unitName(), g.nb });
    return .{ .schema = r.out.*, .batches = out };
}

/// Phase 3: the buckets' runs of rows, one per partition, in partition key order, as
/// one batch in `a`.
fn keyOrder(a: std.mem.Allocator, ctx: *const TopKSink) ![]const Batch {
    var outs = std.array_list.Managed(Batch).init(a);
    for (ctx.outs) |o| if (o) |b| try outs.append(b);
    var total: usize = 0;
    for (outs.items) |b| total += b.len;
    if (total == 0) return &.{};
    const ncols = ctx.out_schema.fields.len;
    const cols = try a.alloc(column.Column, ncols);
    for (cols, 0..) |*c, ci| {
        const chunks = try a.alloc(column.Column, outs.items.len);
        for (chunks, outs.items) |*ch, b| ch.* = b.columns[ci];
        c.* = try column.concat(a, chunks, total);
    }
    const all = Batch{ .schema = ctx.out_schema, .columns = cols, .len = total };
    if (ctx.part.len == 0) return try a.dupe(Batch, &.{all});

    var starts = std.array_list.Managed(usize).init(a);
    const arrs = try a.alloc(KeyArr, ctx.part.len);
    for (ctx.part, arrs) |ci, *k| k.* = try KeyArr.prepare(a, cols[ci], .{ .idx = ci, .desc = false, .nulls_first = false });
    for (0..total) |i| {
        const fresh = i == 0 or for (arrs) |k| {
            if (k.order(i - 1, i) != .eq) break true;
        } else false;
        if (fresh) try starts.append(i);
    }
    const reps = try a.alloc(KeyArr, ctx.part.len);
    for (ctx.part, reps) |ci, *k| k.* = try KeyArr.prepare(a, try column.permute(a, cols[ci], starts.items), .{ .idx = ci, .desc = false, .nulls_first = false });
    const order = try a.alloc(usize, starts.items.len);
    for (order, 0..) |*x, i| x.* = i;
    try sortIdx(a, order, reps);
    const rows = try a.alloc(usize, total);
    var at: usize = 0;
    for (order) |g| {
        const lo = starts.items[g];
        const hi = if (g + 1 < starts.items.len) starts.items[g + 1] else total;
        for (lo..hi) |i| {
            rows[at] = i;
            at += 1;
        }
    }
    const sorted = try a.alloc(column.Column, ncols);
    for (sorted, cols) |*o, c| o.* = try column.permute(a, c, rows);
    return try a.dupe(Batch, &.{.{ .schema = ctx.out_schema, .columns = sorted, .len = total }});
}
