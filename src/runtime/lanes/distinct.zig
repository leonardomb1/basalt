//! DISTINCT lanes, a `ParallelSink` that `morsels.drive` feeds: each morsel keeps the
//! first row per key it saw; the morsels' rows are then merged a hash partition at a
//! time on the shared key table, walking morsels in file order so the first row
//! overall wins, and gathered back in file order.

const Batch = @import("../../exec/batch.zig").Batch;
const Env = @import("../env.zig").Env;
const Morsels = @import("morsels.zig").Morsels;
const ParallelSink = @import("morsels.zig").ParallelSink;
const Pipeline = @import("morsels.zig").Pipeline;
const drive = @import("morsels.zig").drive;
const RunOptions = @import("../env.zig").RunOptions;
const Stats = @import("../env.zig").Stats;
const Value = @import("../../exec/value.zig").Value;
const LaneMem = @import("../parallel.zig").LaneMem;
const WorkQueue = @import("../parallel.zig").WorkQueue;
const analyze = @import("../analyze.zig");
const ast = @import("../../lang/ast.zig");
const chainAt = @import("../plan.zig").chainAt;
const column = @import("../../exec/column.zig");
const keyhash = @import("../../exec/keyhash.zig");
const keytable = @import("../../exec/keytable.zig");
const dispatchWorker = @import("../parallel.zig").dispatchWorker;
const resolveAt = @import("../plan.zig").resolveAt;
const op = @import("../../exec/op.zig");
const openTailSink = @import("agg.zig").openTailSink;
const parallel = @import("../parallel.zig");
const SinkGuard = @import("../../connect/guard.zig").SinkGuard;
const sinkLabel = @import("../connect.zig").sinkLabel;
const std = @import("std");
const msgfmt = @import("../../format/msgfmt.zig");
const types = @import("../../lang/types.zig");
const writeTail = @import("agg.zig").writeTail;

pub const DistinctShape = struct { prefix: []const ast.Stage, dist: ast.Stage, tail: []const ast.Stage };

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
    return .{ .prefix = middle[0..d], .dist = middle[d], .tail = middle[d + 1 ..] };
}

/// The key columns of the DISTINCT and how they are keyed: encoded fixed-width words
/// when every key has a fixed encoding, else boxed values.
const Keys = struct {
    idx: []const usize,
    kinds: ?[]const keyhash.KeyKind,

    fn of(a: std.mem.Allocator, schema: *const types.Schema, idx: []const usize) !Keys {
        const kinds = try a.alloc(keyhash.KeyKind, idx.len);
        if (idx.len > keyhash.max_fixed_keys) return .{ .idx = idx, .kinds = null };
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

/// A worker's memory, reset by `drive` before each morsel, and its string ids, so a
/// string costs the shared table's lock once a worker rather than once a morsel.
const Local = struct {
    mem: *LaneMem,
    arena: std.heap.ArenaAllocator,
    cache: std.StringHashMapUnmanaged(keytable.StrId) = .empty,
};

/// DISTINCT as a parallel sink: each morsel deduped on its own into its item, so the
/// item index is a file position; finalize keeps each key's first row across items
/// a hash partition at a time, and gathers the kept rows in file order.
const DistinctSink = struct {
    env: *Env,
    row_schema: *const types.Schema,
    local_keys: ?[]const usize,
    keys: Keys,
    strs: *?*keytable.StrTable,
    items: []Item,
    threads: usize,
    merged: Batch = undefined,
    locals: std.ArrayListUnmanaged(*Local) = .empty,
    mtx: std.Thread.Mutex = .{},

    fn parallelSink(self: *DistinctSink) ParallelSink {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn deinit(self: *DistinctSink) void {
        const gpa = self.env.gpa;
        for (self.locals.items) |l| {
            l.arena.deinit();
            gpa.destroy(l);
        }
        self.locals.deinit(gpa);
    }

    fn initLocal(ptr: *anyopaque, mem: *LaneMem) anyerror!*anyopaque {
        const self: *DistinctSink = @ptrCast(@alignCast(ptr));
        const gpa = self.env.gpa;
        const l = try gpa.create(Local);
        errdefer gpa.destroy(l);
        l.* = .{ .mem = mem, .arena = .init(gpa) };
        self.mtx.lock();
        defer self.mtx.unlock();
        try self.locals.append(gpa, l);
        return l;
    }

    /// Morsel `i`'s first row per key, each batch of them keyed for the merge.
    fn sink(ptr: *anyopaque, local: *anyopaque, i: usize, chain: op.Op, _: *const usize) anyerror!void {
        const self: *DistinctSink = @ptrCast(@alignCast(ptr));
        const l: *Local = @ptrCast(@alignCast(local));
        const in = keytable.Interner{ .table = self.strs, .cache = &l.cache, .alloc = l.arena.allocator() };
        const item = &self.items[i];
        const ia = item.arena.allocator();
        var d = op.Distinct{ .child = chain, .in_schema = self.row_schema, .keys = self.local_keys, .state = l.mem.work.allocator(), .gpa = l.mem.batch.allocator(), .track_ords = true, .partial = true, .shared_strs = in };
        defer d.release();
        const dop: op.Op = .{ .distinct = &d };

        const enc: ?keytable.Encoder = if (self.keys.kinds) |k| try keytable.Encoder.init(ia, self.keys.idx, k) else null;
        while (try dop.next(ia)) |b| {
            const hashes = try ia.alloc(u64, b.len);
            var e: ?keytable.Encoded = null;
            if (enc) |en| {
                e = try en.encode(ia, b.columns, b.len, in, false);
                for (hashes, 0..) |*h, r| h.* = e.?.hash(r);
            } else {
                const probe = try ia.alloc(Value, self.keys.idx.len);
                for (hashes, 0..) |*h, r| {
                    for (self.keys.idx, probe) |ci, *v| v.* = b.columns[ci].getValue(r);
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
        if (self.env.profile) |p| _ = try p.absorb(dop);
    }

    fn combine(_: *anyopaque, _: *anyopaque) anyerror!void {}

    fn finalize(ptr: *anyopaque) anyerror!void {
        const self: *DistinctSink = @ptrCast(@alignCast(ptr));
        const t0 = std.time.Instant.now() catch unreachable;
        defer if (self.env.profile) |p| p.phase("merge the lanes' rows", (std.time.Instant.now() catch t0).since(t0)) catch {};
        var mctx = MergeCtx{ .gpa = self.env.gpa, .keys = self.keys, .items = self.items, .queue = .{ .nitems = keytable.parts } };
        _ = try parallel.spawnJoin(self.env.arena, self.threads, mergeWorker, &mctx);
        if (mctx.queue.failure()) |e| return e;
        self.merged = try gatherKept(self.env.arena, self.row_schema, self.items);
    }

    const vtable = ParallelSink.VTable{ .initLocal = initLocal, .sink = sink, .combine = combine, .finalize = finalize };
};

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
        var st = FixedStore.init(a, nk, &no_payload);
        for (ctx.items) |*item| for (item.pieces.items) |pc| {
            const e = pc.enc.?;
            const order = pc.order[pc.starts[p]..pc.starts[p + 1]];
            var v = KeepFirst(FixedStore){ .tb = &table, .st = &st, .keep = pc.keep };
            try keytable.probe(FixedStore, .alias, &keytable.Runs(i64).over(nk, order, pc.hashes, e.keys, e.masks), 0, order.len, &v);
        };
    } else {
        var st = BoxedStore.init(a, nk, &no_payload);
        const probe = try a.alloc(Value, nk);
        for (ctx.items) |*item| for (item.pieces.items) |pc| {
            const order = pc.order[pc.starts[p]..pc.starts[p + 1]];
            var v = KeepFirst(BoxedStore){ .tb = &table, .st = &st, .keep = pc.keep };
            const vals = keytable.ValueRows{ .cols = pc.batch.columns, .idx = ctx.keys.idx, .buf = probe, .hashes = pc.hashes, .rows = order };
            try keytable.probe(BoxedStore, .alias, &vals, 0, order.len, &v);
        };
    }
}

const FixedStore = keytable.FixedStore(keytable.NoPayload);
const BoxedStore = keytable.GroupStore(keytable.NoPayload);

/// `mergeOne`'s side of `keytable.probe`: the first row of each key kept.
fn KeepFirst(comptime Store: type) type {
    return struct {
        tb: *keytable.GroupTable,
        st: *Store,
        keep: []bool,

        pub inline fn hit(c: *@This(), r: usize, _: u64, _: anytype, x: keytable.Hit(Store)) !void {
            if (!x.found) c.keep[r] = true;
        }
    };
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
        try column.gatherColumns(arena, pc.batch.columns, cols, pc.keep, kept);
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
    m: *const Morsels,
    prefix: []const ast.Stage,
    dist: ast.Stage,
    tail: []const ast.Stage,
    w: ast.Write,
    opts: RunOptions,
    stats: *Stats,
    lanes_used: *usize,
) anyerror!bool {
    const arena = env.arena;
    const pre = try chainAt(env, m.schema().*, prefix);
    const row_schema = pre.out;

    const local_keys = (try resolveAt(env, row_schema.*, dist)).plan.distinct;
    var key_idx: []const usize = undefined;
    if (local_keys) |ks| {
        key_idx = ks;
    } else {
        const all = try arena.alloc(usize, row_schema.fields.len);
        for (all, 0..) |*x, j| x.* = j;
        key_idx = all;
    }

    env.src_name = m.label();
    env.sink_name = sinkLabel(env, w);

    const nitems = m.count();
    const items = try arena.alloc(Item, nitems);
    for (items) |*it| it.* = .{ .arena = .init(env.gpa) };
    defer for (items) |*it| it.arena.deinit();
    var strs_table = keytable.StrTable.init(std.heap.page_allocator);
    defer strs_table.deinit();
    var strs: ?*keytable.StrTable = &strs_table;
    var g = DistinctSink{
        .env = env,
        .row_schema = row_schema,
        .local_keys = local_keys,
        .keys = try Keys.of(arena, row_schema, key_idx),
        .strs = &strs,
        .items = items,
        .threads = m.workers(opts.lanes()),
    };
    defer g.deinit();
    const p: Pipeline = .{ .m = m, .recs = pre.recs, .errctx = env.errctx, .rows_read = env.rows_read };
    const lanes = try drive(arena, p, g.parallelSink(), opts.lanes());
    lanes_used.* = @max(lanes_used.*, lanes);
    env.log.log(.debug, "parallel {s} distinct: {d} {s} over {d} lanes", .{ m.label(), nitems, m.unitName(), lanes });
    if (env.profile) |pr| {
        pr.path = try msgfmt.allocPrint(env.arena, "{s} parallel distinct, {d} {s}", .{ m.label(), nitems, m.unitName() });
    }
    const merged = g.merged;

    const after = try chainAt(env, row_schema.*, tail);
    const snk = try openTailSink(env, w, after.out.*);
    var guard = SinkGuard.init(snk);
    defer guard.abort();
    try writeTail(env, snk, merged, row_schema.*, after.recs, stats, .distinct);
    try guard.close();
    return true;
}
