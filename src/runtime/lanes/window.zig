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
const Profile = @import("../../exec/profile.zig").Profile;
const Reader = @import("morsels.zig").Reader;
const RunOptions = @import("../env.zig").RunOptions;
const Stream = @import("morsels.zig").Stream;
const TopK = @import("../../exec/op/topk.zig").TopK;
const WorkQueue = @import("../parallel.zig").WorkQueue;
const analyze = @import("../analyze.zig");
const ast = @import("../../lang/ast.zig");
const chainAt = @import("../plan.zig").chainAt;
const chainOps = @import("../plan.zig").chainOps;
const column = @import("../../exec/column.zig");
const keyhash = @import("../../exec/keyhash.zig");
const msgfmt = @import("../../format/msgfmt.zig");
const obs = @import("../obs.zig");
const op = @import("../../exec/op.zig");
const parallel = @import("../parallel.zig");
const resolveAt = @import("../plan.zig").resolveAt;
const sortIdx = @import("../../exec/op/sort.zig").sortIdx;
const std = @import("std");
const types = @import("../../lang/types.zig");

/// Rows one lane sent to one bucket, with their positions.
const Part = struct { rows: Batch, seqs: []const u64 };

const Bucket = struct {
    parts: std.ArrayListUnmanaged(Part) = .empty,
    out: ?Batch = null,
};

const Ctx = struct {
    m: *const Morsels,
    prefix: []const analyze.Resolved,
    in_schema: *const types.Schema,
    out_schema: *const types.Schema,
    part: []const usize,
    ord: []const op.Sort.Key,
    k: usize,
    errctx: ?*op.ErrCtx = null,
    queue: WorkQueue,
    /// `sent[lane * nb + b]` is what lane `lane` sent to bucket `b`; bucket `b`'s
    /// window rows end in `sent[b].out`.
    sent: []Bucket,
    nb: usize,
    lane_arenas: []std.heap.ArenaAllocator,
    rows_read: *obs.RowCounter,
    prof: ?*Profile = null,
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

fn scatterLane(ctx: *Ctx, lane_idx: usize) void {
    scatterRun(ctx, lane_idx) catch |e| ctx.queue.fail(e);
}

/// Phase 1: a lane's morsels through the map stages, each batch's rows sent to their
/// buckets, copied into the lane's own arena with their positions.
fn scatterRun(ctx: *Ctx, me: usize) !void {
    const keep = ctx.lane_arenas[me].allocator();
    const sent = ctx.sent[me * ctx.nb ..][0..ctx.nb];
    var mem: LaneMem = .{};
    mem.init();
    defer mem.deinit();
    var rd = Reader.init(mem.gpa.allocator());
    defer rd.deinit();
    var st = Stream{ .m = ctx.m, .queue = &ctx.queue, .rd = &rd, .scratch = mem.work.allocator() };
    defer st.source().close();
    var cs = obs.CountingSource{ .inner = st.source(), .count = ctx.rows_read };
    var scan = op.Scan{ .src = cs.source() };
    const child = try chainOps(mem.work.allocator(), ctx.errctx, ctx.prefix, .{ .scan = &scan });

    var last: usize = std.math.maxInt(usize);
    var base: u64 = 0;
    var seen: u64 = 0;
    const idx = try mem.work.allocator().alloc(std.ArrayListUnmanaged(usize), ctx.nb);
    for (idx) |*x| x.* = .empty;
    while (true) {
        _ = mem.batch.reset(.retain_capacity);
        const a = mem.batch.allocator();
        const b = (try child.next(a)) orelse break;
        if (b.len == 0) continue;
        if (st.cur_item != last) {
            last = st.cur_item;
            base = @as(u64, last) << 40;
            seen = 0;
        }
        for (idx) |*x| x.* = .empty;
        for (0..b.len) |r| try idx[bucketOf(b, ctx.part, r, ctx.nb)].append(a, r);
        for (idx, sent) |x, *bk| {
            if (x.items.len == 0) continue;
            const cols = try keep.alloc(column.Column, b.columns.len);
            for (b.columns, cols) |c, *o| o.* = try column.permute(keep, c, x.items);
            const seqs = try keep.alloc(u64, x.items.len);
            for (seqs, x.items) |*s, r| s.* = base + seen + r;
            try bk.parts.append(keep, .{ .rows = .{ .schema = b.schema, .columns = cols, .len = x.items.len }, .seqs = seqs });
        }
        seen += b.len;
    }
    if (ctx.prof) |p| _ = try p.absorb(child);
}

/// Phase 2: bucket `bi`'s rows from every lane through one top-k core, its window
/// rows into `out_arena`.
fn bucketTask(ctx: *Ctx, bi: usize, out_arena: std.mem.Allocator) !void {
    var gpa_state = std.heap.GeneralPurposeAllocator(.{ .thread_safe = false }){};
    defer _ = gpa_state.deinit();
    const gpa = gpa_state.allocator();
    var core = TopK.init(gpa, ctx.in_schema, ctx.ord, ctx.part, ctx.k);
    defer core.deinit();
    var scratch = std.heap.ArenaAllocator.init(gpa);
    defer scratch.deinit();
    const nlanes = ctx.sent.len / ctx.nb;
    for (0..nlanes) |l| for (ctx.sent[l * ctx.nb + bi].parts.items) |p| {
        try core.add(scratch.allocator(), p.rows, 0, p.seqs);
        _ = scratch.reset(.retain_capacity);
    };
    ctx.sent[bi].out = try op.Window.topKRows(&core, out_arena, ctx.in_schema, ctx.out_schema);
}

const BucketCtx = struct {
    w: *Ctx,
    queue: WorkQueue,
    arenas: []std.heap.ArenaAllocator,
};

fn bucketLane(bc: *BucketCtx, lane_idx: usize) void {
    while (bc.queue.take()) |bi| {
        bucketTask(bc.w, bi, bc.arenas[lane_idx].allocator()) catch |e| {
            bc.queue.fail(e);
            return;
        };
    }
}

/// The window stage `ws` (its `top_k` set) over `m` through the map stages `prefix`,
/// run on lanes; its output as the serial window gives it, held in memory.
pub fn runWindowTopK(env: *Env, m: *const Morsels, prefix: []const ast.Stage, ws: ast.Stage, opts: RunOptions, lanes_used: *usize) anyerror!Materialized {
    const arena = env.arena;
    const pre = try chainAt(env, m.schema().*, prefix);
    const r = try resolveAt(env, pre.out.*, ws);
    const wp = r.plan.window;
    const wd = ws.node.window;
    const part = try arena.dupe(usize, wp.part);
    const ord = try arena.alloc(op.Sort.Key, wp.ord.len);
    for (wd.order_by, wp.ord, ord) |sk, idx, *key| key.* = .{ .idx = idx, .desc = sk.desc, .nulls_first = sk.nulls_first };

    const nlanes = m.workers(opts.lanes());
    const nb = if (part.len == 0) 1 else nlanes * 4;
    const lane_arenas = try arena.alloc(std.heap.ArenaAllocator, nlanes);
    for (lane_arenas) |*la| la.* = .init(std.heap.page_allocator);
    defer for (lane_arenas) |*la| la.deinit();
    const sent = try arena.alloc(Bucket, nlanes * nb);
    for (sent) |*s| s.* = .{};
    var ctx = Ctx{
        .m = m,
        .prefix = pre.recs,
        .in_schema = r.in,
        .out_schema = r.out,
        .part = part,
        .ord = ord,
        .k = @intCast(wd.top_k.?),
        .errctx = env.errctx,
        .queue = .{ .nitems = m.count() },
        .sent = sent,
        .nb = nb,
        .lane_arenas = lane_arenas,
        .rows_read = env.rows_read,
        .prof = env.profile,
    };
    const used = try parallel.spawnJoin(arena, nlanes, scatterLane, &ctx);
    lanes_used.* = @max(lanes_used.*, used);
    if (ctx.queue.failure()) |e| return e;

    const t0 = std.time.Instant.now() catch null;
    const out_arenas = try arena.alloc(std.heap.ArenaAllocator, nlanes);
    for (out_arenas) |*oa| oa.* = .init(env.gpa);
    defer for (out_arenas) |*oa| oa.deinit();
    var bc = BucketCtx{ .w = &ctx, .queue = .{ .nitems = nb }, .arenas = out_arenas };
    _ = try parallel.spawnJoin(arena, @min(nlanes, nb), bucketLane, &bc);
    if (bc.queue.failure()) |e| return e;

    const out = try keyOrder(env.shared orelse env.arena, &ctx);
    if (env.profile) |p| {
        if (t0) |s| try p.phase("top-k each bucket's partitions, in key order", (std.time.Instant.now() catch s).since(s));
        p.path = try msgfmt.allocPrint(arena, "{s} parallel top-k window, {d} {s}, {d} buckets", .{ m.label(), m.count(), m.unitName(), nb });
    }
    return .{ .schema = r.out.*, .batches = out };
}

/// Phase 3: the buckets' runs of rows, one per partition, in partition key order, as
/// one batch in `a`.
fn keyOrder(a: std.mem.Allocator, ctx: *Ctx) ![]const Batch {
    var outs = std.array_list.Managed(Batch).init(a);
    for (ctx.sent[0..ctx.nb]) |bk| if (bk.out) |b| try outs.append(b);
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
