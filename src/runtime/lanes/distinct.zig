//! DISTINCT lanes: each work item keeps the first row per key it saw; the items' rows
//! are then merged a hash partition at a time on the shared key table, walking items
//! in file order so the first row overall wins, and gathered back in file order.

const Batch = @import("../../exec/batch.zig").Batch;
const Env = @import("../env.zig").Env;
const LaneSplit = @import("sources.zig").LaneSplit;
const Profile = @import("../../exec/profile.zig").Profile;
const RunOptions = @import("../env.zig").RunOptions;
const Stats = @import("../env.zig").Stats;
const Value = @import("../../exec/value.zig").Value;
const LaneMem = @import("../parallel.zig").LaneMem;
const WorkQueue = @import("../parallel.zig").WorkQueue;
const aErr = @import("../env.zig").aErr;
const analyze = @import("../analyze.zig");
const ast = @import("../../lang/ast.zig");
const buildMapChain = @import("../plan.zig").buildMapChain;
const column = @import("../../exec/column.zig");
const keyhash = @import("../../exec/keyhash.zig");
const keytable = @import("../../exec/keytable.zig");
const dispatchWorker = @import("../parallel.zig").dispatchWorker;
const driver = @import("../../connect/driver.zig");
const laneRowSource = @import("sources.zig").laneRowSource;
const mapChainSchema = @import("../plan.zig").mapChainSchema;
const obs = @import("../obs.zig");
const op = @import("../../exec/op.zig");
const openTailSink = @import("agg.zig").openTailSink;
const parallel = @import("../parallel.zig");
const pqdecode = @import("../../format/parquet/read.zig");
const schemaPtr = @import("../env.zig").schemaPtr;
const sinkLabel = @import("../connect.zig").sinkLabel;
const std = @import("std");
const trace = @import("../../exec/trace.zig");
const types = @import("../../lang/types.zig");
const writeTail = @import("agg.zig").writeTail;

pub const DistinctShape = struct { prefix: []const ast.Stage, dist: ast.Distinct, tail: []const ast.Stage };

pub fn classifyDistinctPipeline(stages: []const ast.Stage) ?DistinctShape {
    const middle = stages[1 .. stages.len - 1];
    var di: ?usize = null;
    for (middle, 0..) |st, i| switch (st.node) {
        .filter, .select => {},
        .distinct => {
            if (di != null) return null;
            di = i;
        },
        .sort, .limit, .aggregate, .window => if (di == null) return null,
        else => return null,
    };
    const d = di orelse return null;
    return .{ .prefix = middle[0..d], .dist = middle[d].node.distinct, .tail = middle[d + 1 ..] };
}

/// The key columns of the DISTINCT and how they are keyed: encoded fixed-width words
/// when every key has a fixed encoding, else boxed values.
const Keys = struct {
    idx: []const usize,
    kinds: ?[]const keyhash.KeyKind,

    fn of(a: std.mem.Allocator, schema: *const types.Schema, idx: []const usize) !Keys {
        const kinds = try a.alloc(keyhash.KeyKind, idx.len);
        for (idx, kinds) |ci, *k| k.* = keyhash.keyKindOf(schema.fields[ci].ty.kind) orelse return .{ .idx = idx, .kinds = null };
        return .{ .idx = idx, .kinds = kinds };
    }
};

/// One batch of an item's first rows, keyed for the merge: each row's hash, its
/// encoded key (fixed-width keys), the rows bucketed by hash partition in input order
/// (`order` from `starts[p]` to `starts[p + 1]`), and which rows the merge keeps.
const Piece = struct {
    batch: Batch,
    enc: ?keytable.Encoded,
    hashes: []u64,
    starts: []u32,
    order: []u32,
    keep: []bool,
};

/// A work item's first rows, which live until the merge has gathered them.
const Item = struct {
    arena: std.heap.ArenaAllocator,
    pieces: std.ArrayListUnmanaged(Piece) = .empty,
};

/// What a lane keeps across the items it takes: its string ids, so a string costs the
/// shared table's lock once a lane rather than once an item, and its working memory,
/// reused rather than mapped afresh for every item.
const Lane = struct {
    arena: std.heap.ArenaAllocator,
    cache: std.StringHashMapUnmanaged(keytable.StrId) = .empty,
    mem: LaneMem = .{},
};

const DistinctCtx = struct {
    split: LaneSplit,
    row_schema: *const types.Schema,
    prefix: []const ast.Stage,
    params: *std.StringHashMap(*const ast.Expr),
    errctx: ?*op.ErrCtx = null,
    local_keys: ?[]const usize,
    keys: Keys,
    strs: *?*keytable.StrTable,
    queue: WorkQueue,
    items: []Item,
    lanes: []Lane,
    rows_read: *obs.RowCounter,
    prof: ?*Profile = null,
};

/// A lane: work items taken from the queue until none are left, with the lane's own
/// string cache.
fn distinctWorker(ctx: *DistinctCtx, lane_idx: usize) void {
    const lane = &ctx.lanes[lane_idx];
    const in = keytable.Interner{ .table = ctx.strs, .cache = &lane.cache, .alloc = lane.arena.allocator() };
    lane.mem.init();
    defer lane.mem.deinit();
    while (!ctx.queue.failed.load(.seq_cst)) {
        const i = ctx.queue.next.fetchAdd(1, .seq_cst);
        if (i >= ctx.queue.nitems) return;
        _ = lane.mem.work.reset(.retain_capacity);
        _ = lane.mem.batch.reset(.retain_capacity);
        const span = trace.begin("distinct item", i);
        defer span.end();
        distinctWorkOne(ctx, &lane.mem, in, i) catch |e| return ctx.queue.fail(e);
    }
}

/// Dedups per item rather than per lane, so the item index is a file position and the
/// merge keeps the row a serial run would; then keys the item's first rows for it.
fn distinctWorkOne(ctx: *DistinctCtx, mem: *LaneMem, in: keytable.Interner, i: usize) !void {
    const item = &ctx.items[i];
    const ia = item.arena.allocator();
    const inner = (try laneRowSource(ctx.split.rows(i, ctx.queue.nitems), mem.work.allocator())) orelse return;
    defer inner.close();
    var cs = obs.CountingSource{ .inner = inner, .count = ctx.rows_read };
    var scan = op.Scan{ .src = cs.source() };
    const child = try buildMapChain(mem.work.allocator(), ctx.params, ctx.errctx, ctx.prefix, &scan, ctx.split.schema());
    var d = op.Distinct{ .child = child, .in_schema = ctx.row_schema, .keys = ctx.local_keys, .state = mem.work.allocator(), .gpa = mem.batch.allocator(), .track_ords = true, .partial = true, .shared_strs = in };
    defer d.release();
    const dop: op.Op = .{ .distinct = &d };

    const enc: ?keytable.Encoder = if (ctx.keys.kinds) |k| try keytable.Encoder.init(ia, ctx.keys.idx, k) else null;
    while (try dop.next(ia)) |b| {
        const hashes = try ia.alloc(u64, b.len);
        var e: ?keytable.Encoded = null;
        if (enc) |en| {
            e = try en.encode(ia, b.columns, b.len, in, false);
            for (hashes, 0..) |*h, r| h.* = e.?.hash(r);
        } else {
            const probe = try ia.alloc(Value, ctx.keys.idx.len);
            for (hashes, 0..) |*h, r| {
                for (ctx.keys.idx, probe) |ci, *v| v.* = b.columns[ci].getValue(r);
                h.* = keyhash.MultiKeyCtx.hash(.{}, probe);
            }
        }
        const starts = try ia.alloc(u32, keytable.parts + 1);
        @memset(starts, 0);
        for (hashes) |h| starts[keytable.partOf(h) + 1] += 1;
        for (1..starts.len) |p| starts[p] += starts[p - 1];
        const order = try ia.alloc(u32, b.len);
        const fill = try ia.dupe(u32, starts[0..keytable.parts]);
        for (hashes, 0..) |h, r| {
            const p = keytable.partOf(h);
            order[fill[p]] = @intCast(r);
            fill[p] += 1;
        }
        const keep = try ia.alloc(bool, b.len);
        @memset(keep, false);
        try item.pieces.append(ia, .{ .batch = b, .enc = e, .hashes = hashes, .starts = starts, .order = order, .keep = keep });
    }
    if (ctx.prof) |p| _ = try p.absorb(dop);
}

const MergeCtx = struct {
    gpa: std.mem.Allocator,
    keys: Keys,
    items: []Item,
    queue: WorkQueue,
};

const mergeWorker = dispatchWorker(MergeCtx, mergeOne);

/// Partition `p` across every item in input order: a key's first row is kept.
fn mergeOne(ctx: *MergeCtx, p: usize) !void {
    var arena = std.heap.ArenaAllocator.init(ctx.gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var rows: usize = 0;
    for (ctx.items) |*item| for (item.pieces.items) |pc| {
        rows += pc.starts[p + 1] - pc.starts[p];
    };
    var table = try keytable.GroupTable.init(a, keytable.GroupTable.capFor(rows));
    const nk = ctx.keys.idx.len;
    if (ctx.keys.kinds != null) {
        var st = keytable.FixedStore(keytable.NoPayload).init(a, nk, &no_payload);
        for (ctx.items) |*item| for (item.pieces.items) |pc| for (pc.order[pc.starts[p]..pc.starts[p + 1]]) |r| {
            const key = pc.enc.?.key(r);
            const f = try table.getOrPut(pc.hashes[r], key, &st, @intCast(st.len));
            if (f.found) continue;
            const nr = try st.push(pc.hashes[r]);
            @memcpy(nr.keys, key.vals);
            nr.mask.* = key.mask;
            pc.keep[r] = true;
        };
    } else {
        var st = keytable.GroupStore(keytable.NoPayload).init(a, nk, &no_payload);
        const probe = try a.alloc(Value, nk);
        for (ctx.items) |*item| for (item.pieces.items) |pc| for (pc.order[pc.starts[p]..pc.starts[p + 1]]) |r| {
            for (ctx.keys.idx, probe) |ci, *v| v.* = pc.batch.columns[ci].getValue(r);
            const f = try table.getOrPut(pc.hashes[r], @as([]const Value, probe), &st, @intCast(st.len));
            if (f.found) continue;
            const nr = try st.push(pc.hashes[r]);
            @memcpy(nr.keys, probe);
            pc.keep[r] = true;
        };
    }
}

const no_payload = keytable.NoPayload{};

/// Every item's kept rows, in input order, as one batch.
fn gatherKept(arena: std.mem.Allocator, row_schema: *const types.Schema, items: []const Item) !Batch {
    var parts_ = std.array_list.Managed(Batch).init(arena);
    var total: usize = 0;
    for (items) |*item| for (item.pieces.items) |pc| {
        var kept: usize = 0;
        for (pc.keep) |k| kept += @intFromBool(k);
        if (kept == 0) continue;
        const cols = try arena.alloc(column.Column, pc.batch.columns.len);
        for (pc.batch.columns, cols) |c, *o| o.* = try column.gather(arena, c, pc.keep, kept);
        try parts_.append(.{ .schema = row_schema, .columns = cols, .len = kept });
        total += kept;
    };
    const cols = try arena.alloc(column.Column, row_schema.fields.len);
    if (parts_.items.len == 0) {
        for (row_schema.fields, cols) |f, *c| {
            var bd = column.Builder.init(arena, f.ty);
            c.* = try bd.finish();
        }
        return .{ .schema = row_schema, .columns = cols, .len = 0 };
    }
    const per = try arena.alloc(column.Column, parts_.items.len);
    for (cols, 0..) |*c, ci| {
        for (parts_.items, per) |pb, *o| o.* = pb.columns[ci];
        c.* = try column.concat(arena, per, total);
    }
    return .{ .schema = row_schema, .columns = cols, .len = total };
}

pub fn runParallelDistinct(
    env: *Env,
    split: LaneSplit,
    prefix: []const ast.Stage,
    dist: ast.Distinct,
    tail: []const ast.Stage,
    w: ast.Write,
    opts: RunOptions,
    stats: *Stats,
    lanes_used: *usize,
) anyerror!bool {
    const arena = env.arena;
    const row_schema = try schemaPtr(arena, try mapChainSchema(env, prefix, split.schema().*));

    var local_keys: ?[]const usize = null;
    var key_idx: []const usize = undefined;
    if (dist.on) |fields| {
        var ad = analyze.Diag{};
        const ks = analyze.fieldIndices(arena, row_schema.*, fields, &ad) catch |e| return aErr(env, &ad, e);
        local_keys = ks;
        key_idx = ks;
    } else {
        const all = try arena.alloc(usize, row_schema.fields.len);
        for (all, 0..) |*x, j| x.* = j;
        key_idx = all;
    }

    env.src_name = split.label();
    env.sink_name = sinkLabel(env, w);

    const nthreads = split.lanes(opts);
    const nitems = split.count(nthreads);
    const items = try arena.alloc(Item, nitems);
    for (items) |*it| it.* = .{ .arena = .init(env.gpa) };
    defer for (items) |*it| it.arena.deinit();
    const lane_state = try arena.alloc(Lane, @max(nthreads, 1));
    for (lane_state) |*l| l.* = .{ .arena = .init(env.gpa) };
    defer for (lane_state) |*l| l.arena.deinit();
    var strs_table = keytable.StrTable.init(std.heap.page_allocator);
    defer strs_table.deinit();
    var strs: ?*keytable.StrTable = &strs_table;
    var ctx = DistinctCtx{
        .split = split,
        .row_schema = row_schema,
        .prefix = prefix,
        .params = env.params_expr,
        .errctx = env.errctx,
        .local_keys = local_keys,
        .keys = try Keys.of(arena, row_schema, key_idx),
        .strs = &strs,
        .queue = .{ .nitems = nitems },
        .items = items,
        .lanes = lane_state,
        .rows_read = env.rows_read,
        .prof = env.profile,
    };

    const lanes = try parallel.spawnJoin(arena, nthreads, distinctWorker, &ctx);
    lanes_used.* = @max(lanes_used.*, lanes);
    env.log.log(.debug, "parallel {s} distinct: {d} {s} over {d} lanes", .{ split.label(), nitems, split.unitName(), lanes });
    if (ctx.queue.failure()) |e| return e;

    const t_mrg0 = std.time.Instant.now() catch unreachable;
    var mctx = MergeCtx{ .gpa = env.gpa, .keys = ctx.keys, .items = items, .queue = .{ .nitems = keytable.parts } };
    _ = try parallel.spawnJoin(arena, nthreads, mergeWorker, &mctx);
    if (mctx.queue.failure()) |e| return e;
    const merged = try gatherKept(arena, row_schema, items);
    if (env.profile) |p| {
        try p.phase("merge the lanes' rows", (std.time.Instant.now() catch unreachable).since(t_mrg0));
        p.path = try std.fmt.allocPrint(env.arena, "{s} parallel distinct, {d} {s}", .{ split.label(), nitems, split.unitName() });
    }

    const snk = try openTailSink(env, w, tail, row_schema.*);
    writeTail(env, snk, merged, row_schema.*, tail, stats, .distinct) catch |e| {
        snk.abort();
        return e;
    };
    try snk.close();
    return true;
}
