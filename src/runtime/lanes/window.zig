//! A window on lanes, its partitions spread over buckets by the hash of their key, so
//! each bucket owns its partitions whole and nothing is merged twice: a top-k window, a
//! lone `ROW_NUMBER()` filtered `rn <= k` (`Window.top_k`), through a top-k core, and
//! any other window with a PARTITION BY through a window operator of its own. Lanes
//! keeping their own best `k` a partition were no faster: with many small partitions
//! each lane kept nearly every row, and the merge ran them all again on one thread. A
//! whole window sorted on lanes and computed on one thread left that thread the run.
//!
//! 1. Each lane streams the morsels it claims through the map stages and sends every
//!    row to a bucket by its partition key's hash, with its morsel and position there
//!    (`seq`, as `TopN.feed` numbers rows), so ties rank as one serial input ranks them.
//! 2. Each bucket, on a lane, offers its rows from every lane to a top-k core, or reads
//!    them in `seq` order into a window operator, whose sort keeps ties in input
//!    order; either keeps the window's rows, its partitions in key order.
//! 3. A partition lies in one bucket, so its rows are one run there; the runs are put
//!    in key order across buckets. The output is the serial window's to the row.
//!
//! The rows held between the phases count against `--op-memory` when there is a
//! space to spill to; past it the whole window gives up (`error.WindowPastMemory`)
//! and the run sorts it on lanes, spilling, and computes it on one thread.

const Batch = @import("../../exec/batch.zig").Batch;
const Env = @import("../env.zig").Env;
const KeyArr = @import("../../exec/op/sort.zig").KeyArr;
const LaneMem = @import("../parallel.zig").LaneMem;
const Materialized = @import("../env.zig").Materialized;
const Morsels = @import("morsels.zig").Morsels;
const ParallelSink = @import("morsels.zig").ParallelSink;
const Pipeline = @import("morsels.zig").Pipeline;
const drive = @import("morsels.zig").drive;
const MemSource = @import("../env.zig").MemSource;
const RunOptions = @import("../env.zig").RunOptions;
const TopK = @import("../../exec/op/topk.zig").TopK;
const WorkQueue = @import("../parallel.zig").WorkQueue;
const analyze = @import("../analyze.zig");
const ast = @import("../../lang/ast.zig");
const chainAt = @import("../plan.zig").chainAt;
const column = @import("../../exec/column.zig");
const keyhash = @import("../../exec/keyhash.zig");
const msgfmt = @import("../../base/msgfmt.zig");
const op = @import("../../exec/op.zig");
const par = @import("../../exec/par.zig");
const parallel = @import("../parallel.zig");
const resolveAt = @import("../plan.zig").resolveAt;
const sortIdx = @import("../../exec/op/sort.zig").sortIdx;
const std = @import("std");
const pagecache = @import("../../exec/pagecache.zig");
const profile = @import("../../exec/profile.zig");
const types = @import("../../lang/types.zig");

/// Rows one lane sent to one bucket, with their positions.
const Part = struct { rows: Batch, seqs: []const u64 };

/// What a bucket's rows go through: a top-k core keeping `k` rows a partition, or a
/// copy of the window operator `buildOp` made.
const Core = union(enum) { top_k: usize, window: *const op.Window };

/// The window's Global: every worker's buckets, kept until the window's rows are out.
/// A Local is the worker's `Lane`.
const BucketSink = struct {
    env: *Env,
    in_schema: *const types.Schema,
    out_schema: *const types.Schema,
    part: []const usize,
    ord: []const op.Sort.Key,
    core: Core,
    nb: usize,
    threads: usize,
    /// The bytes the lanes may hold in buckets, 0 for no bound.
    budget: usize = 0,
    held: std.atomic.Value(usize) = .init(0),
    mtx: std.Thread.Mutex = .{},
    lanes: std.ArrayListUnmanaged(*Lane) = .empty,
    /// Bucket `b`'s window rows, set by `finalize`.
    outs: []?Batch = &.{},
    /// Where each partition of bucket `b`'s rows starts, for a whole window.
    starts: [][]const usize = &.{},
    /// The kind of the lanes' top operator, which a whole window stands above in a
    /// profile.
    feeds: profile.Kind = .scan,
    /// The time phase 2 took.
    ns: u64 = 0,
    out_arenas: []std.heap.ArenaAllocator = &.{},

    /// What one worker sent to each bucket, copied into its own arena.
    const Lane = struct { keep: std.heap.ArenaAllocator, sent: []std.ArrayListUnmanaged(Part), mem: *LaneMem };

    fn parallelSink(self: *BucketSink) ParallelSink {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn deinit(self: *BucketSink) void {
        const gpa = self.env.gpa;
        for (self.lanes.items) |l| {
            l.keep.deinit();
            gpa.destroy(l);
        }
        self.lanes.deinit(gpa);
        for (self.out_arenas) |*oa| oa.deinit();
    }

    fn initLocal(ptr: *anyopaque, mem: *LaneMem) anyerror!*anyopaque {
        const self: *BucketSink = @ptrCast(@alignCast(ptr));
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
        const self: *BucketSink = @ptrCast(@alignCast(ptr));
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
                const rows = Batch{ .schema = b.schema, .columns = cols, .len = x.items.len };
                if (self.budget > 0) {
                    const n = rows.heapBytes();
                    if (self.held.fetchAdd(n, .monotonic) + n > self.budget) return error.WindowPastMemory;
                }
                try bk.append(keep, .{ .rows = rows, .seqs = seqs });
            }
            seen += b.len;
        }
        const p = self.env.profile orelse return;
        if (self.core == .top_k) {
            _ = try p.absorb(chain);
            return;
        }
        _ = try p.absorbBelow(chain);
        self.mtx.lock();
        defer self.mtx.unlock();
        self.feeds = chain;
    }

    fn combine(_: *anyopaque, _: *anyopaque) anyerror!void {}

    /// Phase 2: every bucket, on a lane, through its own top-k core or window.
    fn finalize(ptr: *anyopaque) anyerror!void {
        const self: *BucketSink = @ptrCast(@alignCast(ptr));
        const arena = self.env.arena;
        const t0 = std.time.Instant.now() catch null;
        self.outs = try arena.alloc(?Batch, self.nb);
        @memset(self.outs, null);
        self.starts = try arena.alloc([]const usize, self.nb);
        @memset(self.starts, &.{});
        self.out_arenas = try arena.alloc(std.heap.ArenaAllocator, self.threads);
        for (self.out_arenas) |*oa| oa.* = .init(self.env.gpa);
        var ctx = BucketCtx{ .g = self, .queue = .{ .nitems = self.nb } };
        _ = try parallel.spawnJoin(arena, @min(self.threads, self.nb), bucketLane, &ctx);
        if (ctx.queue.failure()) |e| return e;
        if (t0) |st| self.ns = (std.time.Instant.now() catch st).since(st);
        if (self.core == .top_k) if (self.env.profile) |p| try p.phase("top-k each bucket's partitions, in key order", self.ns);
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

const BucketCtx = struct { g: *BucketSink, queue: WorkQueue };

fn bucketLane(bc: *BucketCtx, lane_idx: usize) void {
    while (bc.queue.take()) |bi| {
        bucketTask(bc.g, bi, bc.g.out_arenas[lane_idx].allocator()) catch |e| {
            bc.queue.fail(e);
            return;
        };
    }
}

/// Bucket `bi`'s rows from every worker through its top-k core or window, its window
/// rows into `out_arena`.
fn bucketTask(g: *BucketSink, bi: usize, out_arena: std.mem.Allocator) !void {
    switch (g.core) {
        .top_k => |k| return topKBucket(g, bi, k, out_arena),
        .window => |w| return windowBucket(g, bi, w, out_arena),
    }
}

fn topKBucket(g: *BucketSink, bi: usize, k: usize, out_arena: std.mem.Allocator) !void {
    var gpa_state = std.heap.GeneralPurposeAllocator(.{ .thread_safe = false }){};
    defer _ = gpa_state.deinit();
    const gpa = gpa_state.allocator();
    var core = TopK.init(gpa, g.in_schema, g.ord, g.part, k);
    defer core.deinit();
    var scratch = std.heap.ArenaAllocator.init(gpa);
    defer scratch.deinit();
    for (g.lanes.items) |l| for (l.sent[bi].items) |p| {
        try core.add(scratch.allocator(), p.rows, 0, p.seqs);
        _ = scratch.reset(.retain_capacity);
    };
    g.outs[bi] = try op.Window.topKRows(&core, out_arena, g.in_schema, g.out_schema);
}

/// Bucket `bi`'s rows from every worker, in input order, through a copy of `tmpl`: its
/// rows sorted by partition and order keys with every function, as one batch in
/// `out_arena`, and where each partition starts.
fn windowBucket(g: *BucketSink, bi: usize, tmpl: *const op.Window, out_arena: std.mem.Allocator) !void {
    var scratch = std.heap.ArenaAllocator.init(pagecache.process);
    defer scratch.deinit();
    const sa = scratch.allocator();
    var parts = std.ArrayListUnmanaged(Part).empty;
    for (g.lanes.items) |l| try parts.appendSlice(sa, l.sent[bi].items);
    if (parts.items.len == 0) return;
    std.mem.sortUnstable(Part, parts.items, {}, struct {
        fn lt(_: void, a: Part, b: Part) bool {
            return a.seqs[0] < b.seqs[0];
        }
    }.lt);
    const batches = try sa.alloc(Batch, parts.items.len);
    var total: usize = 0;
    for (batches, parts.items) |*b, pt| {
        b.* = pt.rows;
        total += pt.rows.len;
    }
    var src = MemSource{ .m = .{ .schema = g.in_schema.*, .batches = batches } };
    var scan = op.Scan{ .src = src.source() };
    var w = tmpl.*;
    w.child = .{ .scan = &scan };
    w.threads = 1;
    w.space = null;
    w.adopt_input = true;
    w.gpa = pagecache.process;
    w.batch_rows = std.mem.alignForward(usize, total, 8);
    defer w.release();
    const out = (try w.next(out_arena)) orelse return;
    std.debug.assert(out.len == total);
    g.outs[bi] = out;

    var starts = std.ArrayListUnmanaged(usize).empty;
    const arrs = try sa.alloc(KeyArr, w.part.len);
    for (w.part, arrs) |k, *x| x.* = try KeyArr.prepare(sa, out.columns[k.idx], k);
    for (0..out.len) |i| {
        const fresh = i == 0 or for (arrs) |x| {
            if (x.order(i - 1, i) != .eq) break true;
        } else false;
        if (fresh) try starts.append(out_arena, i);
    }
    g.starts[bi] = starts.items;
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
    var g = BucketSink{
        .env = env,
        .in_schema = r.in,
        .out_schema = r.out,
        .part = try arena.dupe(usize, wp.part),
        .ord = ord,
        .core = .{ .top_k = @intCast(wd.top_k.?) },
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
fn keyOrder(a: std.mem.Allocator, ctx: *const BucketSink) ![]const Batch {
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

/// A whole window's rows, its columns each in an arena of its own; `ns` the time its
/// buckets took after the lanes read, and `feeds` the lanes' top operator.
pub const Rows = struct {
    batches: []const Batch = &.{},
    arenas: []std.heap.ArenaAllocator = &.{},
    ns: u64 = 0,
    feeds: profile.Kind = .scan,

    pub fn deinit(self: *Rows) void {
        for (self.arenas) |*a| a.deinit();
    }
};

/// The window `tmpl` (`buildOp`'s, over a PARTITION BY) over `m` through the map
/// records `recs`, run on lanes as buckets of whole partitions; its rows as the serial
/// window gives them, a batch a window's batch of rows at a time.
/// `error.WindowPastMemory` when the rows held passed `--op-memory`.
pub fn runWindowBuckets(env: *Env, m: *const Morsels, recs: []const analyze.Resolved, tmpl: *const op.Window, opts: RunOptions, lanes_used: *usize) anyerror!Rows {
    const arena = env.arena;
    const nlanes = m.workers(opts.lanes());
    const part = try arena.alloc(usize, tmpl.part.len);
    for (tmpl.part, part) |k, *x| x.* = k.idx;
    var g = BucketSink{
        .env = env,
        .in_schema = tmpl.in_schema,
        .out_schema = tmpl.out_schema,
        .part = part,
        .ord = tmpl.ord,
        .core = .{ .window = tmpl },
        .nb = nlanes * 4,
        .threads = nlanes,
        .budget = if (env.space != null) env.op_memory else 0,
    };
    defer g.deinit();
    const p: Pipeline = .{ .m = m, .recs = recs, .errctx = env.errctx, .rows_read = env.rows_read, .per_lane = true };
    lanes_used.* = @max(lanes_used.*, try drive(arena, p, g.parallelSink(), opts.lanes()));
    const t0 = std.time.Instant.now() catch null;
    var rows = try gatherKeyOrder(arena, &g, tmpl.part, tmpl.batch_rows);
    rows.feeds = g.feeds;
    rows.ns = g.ns + if (t0) |a| (std.time.Instant.now() catch a).since(a) else 0;
    if (env.profile) |pr| pr.path = try msgfmt.allocPrint(arena, "{s} parallel window, {d} {s}, {d} buckets", .{ m.label(), m.count(), m.unitName(), g.nb });
    return rows;
}

const Ref = struct { chunk: usize, row: usize };

/// Phase 3 of a whole window: every bucket's partitions in key order, each column
/// gathered from the buckets' rows on its own thread, cut into batches of `batch_rows`.
fn gatherKeyOrder(scratch: std.mem.Allocator, g: *const BucketSink, keys: []const op.Sort.Key, batch_rows: usize) !Rows {
    var srcs = std.ArrayListUnmanaged(Batch).empty;
    var reps = std.ArrayListUnmanaged(Ref).empty;
    var ends = std.ArrayListUnmanaged(usize).empty;
    var total: usize = 0;
    for (g.outs, g.starts) |o, st| {
        const b = o orelse continue;
        const chunk = srcs.items.len;
        try srcs.append(scratch, b);
        for (st, 0..) |s, i| {
            try reps.append(scratch, .{ .chunk = chunk, .row = s });
            try ends.append(scratch, if (i + 1 < st.len) st[i + 1] else b.len);
        }
        total += b.len;
    }
    if (total == 0) return .{};

    const ncols = g.out_schema.fields.len;
    const per = try scratch.alloc(column.Column, srcs.items.len);
    const arrs = try scratch.alloc(KeyArr, keys.len);
    for (keys, arrs) |k, *x| {
        for (per, srcs.items) |*c, b| c.* = b.columns[k.idx];
        x.* = try KeyArr.prepare(scratch, try column.takeAcross(scratch, per, reps.items), k);
    }
    const order = try scratch.alloc(usize, reps.items.len);
    for (order, 0..) |*x, i| x.* = i;
    try sortIdx(scratch, order, arrs);
    const refs = try scratch.alloc(Ref, total);
    var at: usize = 0;
    for (order) |pi| {
        const r = reps.items[pi];
        for (r.row..ends.items[pi]) |row| {
            refs[at] = .{ .chunk = r.chunk, .row = row };
            at += 1;
        }
    }

    var rows = Rows{ .arenas = try scratch.alloc(std.heap.ArenaAllocator, ncols) };
    for (rows.arenas) |*a| a.* = .init(pagecache.process);
    errdefer rows.deinit();
    const cols = try scratch.alloc(column.Column, ncols);
    var ctx = GatherCtx{ .srcs = srcs.items, .refs = refs, .cols = cols, .arenas = rows.arenas };
    par.each(g.threads, ncols, &ctx, GatherCtx.one);
    if (ctx.failed.load(.monotonic)) return error.OutOfMemory;
    const n_batches = (total + batch_rows - 1) / batch_rows;
    const batches = try scratch.alloc(Batch, n_batches);
    for (batches, 0..) |*b, bi| {
        const lo = bi * batch_rows;
        const n = @min(batch_rows, total - lo);
        const bc = try scratch.alloc(column.Column, ncols);
        for (bc, cols) |*o, c| o.* = try column.slice(scratch, c, lo, n);
        b.* = .{ .schema = g.out_schema, .columns = bc, .len = n };
    }
    rows.batches = batches;
    return rows;
}

/// One column of the window's rows gathered in key order, in an arena of its own.
const GatherCtx = struct {
    srcs: []const Batch,
    refs: []const Ref,
    cols: []column.Column,
    arenas: []std.heap.ArenaAllocator,
    failed: std.atomic.Value(bool) = .init(false),

    fn one(ctx: *GatherCtx, ci: usize) void {
        const a = ctx.arenas[ci].allocator();
        const per = a.alloc(column.Column, ctx.srcs.len) catch {
            ctx.failed.store(true, .monotonic);
            return;
        };
        for (per, ctx.srcs) |*c, b| c.* = b.columns[ci];
        ctx.cols[ci] = column.takeAcross(a, per, ctx.refs) catch {
            ctx.failed.store(true, .monotonic);
            return;
        };
    }
};
