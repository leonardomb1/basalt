//! A grouped parallel aggregate past `--op-memory`, kept parallel by partitioning its
//! rows to disk instead of handing the whole aggregate to one lane. The morsels run
//! through the map stages and joins again, and each lane appends every row to one of
//! `fanout` files of its own, picked by the hash of the row's keys, noting the morsel
//! each batch came from. Every partition is then aggregated on its own by the serial
//! `Aggregate`, its lanes' files read back merged in morsel order, so a group sees its
//! rows in input order, as one lane does: the result is the serial plan's, float sums
//! bit for bit, at every `-j`. A partition's aggregate spills one level deeper when
//! it still outgrows a `fanout`th of `--op-memory`, a share that does not follow `-j`,
//! so neither does where it spills nor the order its groups come out in. Each
//! partition's groups go to a file of their own, and the tail reads them back in
//! partition order.

const std = @import("std");
const Batch = @import("../../exec/batch.zig").Batch;
const Env = @import("../env.zig").Env;
const LaneMem = @import("../parallel.zig").LaneMem;
const ParallelSink = @import("morsels.zig").ParallelSink;
const Pipeline = @import("morsels.zig").Pipeline;
const Stats = @import("../env.zig").Stats;
const WorkQueue = @import("../parallel.zig").WorkQueue;
const analyze = @import("../analyze.zig");
const column = @import("../../exec/column.zig");
const dispatchWorker = @import("../parallel.zig").dispatchWorker;
const driver = @import("../../connect/driver.zig");
const drive = @import("morsels.zig").drive;
const keyhash = @import("../../exec/keyhash.zig");
const obs = @import("../obs.zig");
const op = @import("../../exec/op.zig");
const parallel = @import("../parallel.zig");
const spill = @import("../../exec/spill.zig");
const spill_parts = @import("../../exec/op/spill_parts.zig");
const types = @import("../../lang/types.zig");
const writeTailFrom = @import("agg.zig").writeTailFrom;

const fanout = spill_parts.fanout;

/// A run of consecutive batches in a lane's partition file from one morsel.
const Tag = struct { morsel: u32, batches: u32 };

/// One lane's partition files and, per file, its batches' morsels in order; `arena`
/// holds the files' buffers and paths until the run is done with them.
const Local = struct {
    arena: std.heap.ArenaAllocator,
    writers: [fanout]?spill.Writer = @splat(null),
    tags: [fanout]std.ArrayListUnmanaged(Tag) = @splat(.empty),
    runs: [fanout]?spill.Run = @splat(null),
};

/// The routing pass's Global: every lane's `Local`, and what a row is hashed by.
const RouteSink = struct {
    gpa: std.mem.Allocator,
    space: @import("../../exec/space.zig").Space,
    schema: *const types.Schema,
    by: []const usize,
    locals: std.ArrayListUnmanaged(*Local) = .empty,
    mtx: std.Thread.Mutex = .{},

    const vtable = ParallelSink.VTable{ .initLocal = initLocal, .sink = sink, .combine = combine, .finalize = finalize };

    fn parallelSink(self: *RouteSink) ParallelSink {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn initLocal(ptr: *anyopaque, _: *LaneMem) anyerror!*anyopaque {
        const self: *RouteSink = @ptrCast(@alignCast(ptr));
        const l = try self.gpa.create(Local);
        l.* = .{ .arena = .init(self.gpa) };
        self.mtx.lock();
        defer self.mtx.unlock();
        self.locals.append(self.gpa, l) catch |e| {
            self.gpa.destroy(l);
            return e;
        };
        return l;
    }

    fn sink(ptr: *anyopaque, local: *anyopaque, i: usize, chain: op.Op) anyerror!void {
        const self: *RouteSink = @ptrCast(@alignCast(ptr));
        const l: *Local = @ptrCast(@alignCast(local));
        var scratch = std.heap.ArenaAllocator.init(self.gpa);
        defer scratch.deinit();
        while (try chain.next(scratch.allocator())) |b| {
            try self.route(l, scratch.allocator(), b, @intCast(i));
            _ = scratch.reset(.retain_capacity);
        }
    }

    fn route(self: *RouteSink, l: *Local, scratch: std.mem.Allocator, b: Batch, morsel: u32) !void {
        if (b.len == 0) return;
        const dest = try scratch.alloc(u8, b.len);
        for (dest, 0..) |*d, r| d.* = spill_parts.partOf(rowHash(b, self.by, r), 0);
        const mask = try scratch.alloc(bool, b.len);
        for (0..fanout) |p| {
            var n: usize = 0;
            for (dest, mask) |d, *m| {
                m.* = d == p;
                if (m.*) n += 1;
            }
            if (n == 0) continue;
            const cols = try scratch.alloc(column.Column, b.columns.len);
            try column.gatherColumns(scratch, b.columns, cols, mask, n);
            if (l.writers[p] == null) l.writers[p] = try spill.Writer.init(self.space, l.arena.allocator(), self.schema, "group-by-lanes");
            try l.writers[p].?.write(.{ .schema = self.schema, .columns = cols, .len = n });
            const tags = &l.tags[p];
            if (tags.items.len > 0 and tags.items[tags.items.len - 1].morsel == morsel) {
                tags.items[tags.items.len - 1].batches += 1;
            } else try tags.append(self.gpa, .{ .morsel = morsel, .batches = 1 });
        }
    }

    fn combine(ptr: *anyopaque, local: *anyopaque) anyerror!void {
        _ = ptr;
        const l: *Local = @ptrCast(@alignCast(local));
        for (&l.writers, &l.runs) |*w, *r| if (w.*) |*wr| {
            r.* = try wr.finish();
            w.* = null;
        };
    }

    fn finalize(_: *anyopaque) anyerror!void {}

    fn deinit(self: *RouteSink) void {
        for (self.locals.items) |l| {
            for (&l.writers) |*w| if (w.*) |*wr| wr.abort();
            for (&l.runs) |r| if (r) |held| spill.discard(held);
            for (&l.tags) |*t| t.deinit(self.gpa);
            l.arena.deinit();
            self.gpa.destroy(l);
        }
        self.locals.deinit(self.gpa);
    }
};

/// Row `r`'s keys hashed so that keys the aggregate groups together hash alike:
/// a NULL as `hashValue` hashes it, any other cell as `hashCell` does.
fn rowHash(b: Batch, by: []const usize, r: usize) u64 {
    var h = std.hash.Wyhash.init(0);
    for (by) |k| {
        const c = &b.columns[k];
        if (c.validity.get(r)) keyhash.hashCell(&h, c, r) else keyhash.hashValue(&h, .null);
    }
    return h.final();
}

/// One partition's files from every lane, read as one source in morsel order: the
/// lane holding the lowest next morsel gives that morsel's batches. Lanes claim
/// morsels in increasing order, so each lane's file already is.
const MergeSource = struct {
    schema: *const types.Schema,
    lanes: []Lane,
    closed: bool = false,

    const Lane = struct {
        run: spill.Run,
        tags: []const Tag,
        reader: ?spill.Reader = null,
        at: usize = 0,
        left: u32 = 0,
    };

    const vtable = driver.Source.VTable{ .schema = schemaFn, .next = nextFn, .close = closeFn };

    fn source(self: *MergeSource) driver.Source {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn schemaFn(ptr: *anyopaque) types.Schema {
        const self: *MergeSource = @ptrCast(@alignCast(ptr));
        return self.schema.*;
    }

    fn nextFn(ptr: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
        const self: *MergeSource = @ptrCast(@alignCast(ptr));
        var pick: ?*Lane = null;
        for (self.lanes) |*ln| {
            if (ln.left == 0 and ln.at >= ln.tags.len) continue;
            if (ln.left > 0) {
                pick = ln;
                break;
            }
            if (pick == null or ln.tags[ln.at].morsel < pick.?.tags[pick.?.at].morsel) pick = ln;
        }
        const ln = pick orelse return null;
        if (ln.left == 0) {
            ln.left = ln.tags[ln.at].batches;
            ln.at += 1;
        }
        if (ln.reader == null) ln.reader = try spill.Reader.open(ln.run);
        const b = (try ln.reader.?.next(arena)) orelse return error.CorruptSpill;
        ln.left -= 1;
        return b;
    }

    fn closeFn(ptr: *anyopaque) void {
        const self: *MergeSource = @ptrCast(@alignCast(ptr));
        if (self.closed) return;
        self.closed = true;
        for (self.lanes) |*ln| {
            if (ln.reader) |*r| r.close();
            ln.reader = null;
            spill.discard(ln.run);
        }
    }
};

/// The partition phase: each partition's rows aggregated into a file of groups.
const PartCtx = struct {
    env: *Env,
    route: *RouteSink,
    plan: op.Aggregate,
    spill_at: usize,
    results: []?spill.Run,
    paths: []std.heap.ArenaAllocator,
    queue: WorkQueue,
};

fn aggregatePart(ctx: *PartCtx, p: usize) !void {
    const gpa = ctx.env.gpa;
    var lanes = std.ArrayListUnmanaged(MergeSource.Lane).empty;
    defer lanes.deinit(gpa);
    for (ctx.route.locals.items) |l| if (l.runs[p]) |held| {
        try lanes.append(gpa, .{ .run = held, .tags = l.tags[p].items });
        l.runs[p] = null;
    };
    var ms = MergeSource{ .schema = ctx.route.schema, .lanes = lanes.items };
    defer ms.source().close();
    if (lanes.items.len == 0) return;

    const agg = try gpa.create(op.Aggregate);
    defer gpa.destroy(agg);
    var st = std.heap.ArenaAllocator.init(gpa);
    var scan = op.Scan{ .src = ms.source() };
    agg.* = ctx.plan;
    agg.child = .{ .scan = &scan };
    agg.state = st.allocator();
    agg.own_state = &st;
    agg.spill_at = ctx.spill_at;
    agg.spill_depth = 1;
    const o: op.Op = .{ .aggregate = agg };
    defer o.release();

    var out = try spill.Writer.init(ctx.env.space.?, ctx.paths[p].allocator(), ctx.plan.out_schema, "group-by-groups");
    errdefer out.abort();
    var work = std.heap.ArenaAllocator.init(gpa);
    defer work.deinit();
    while (try o.next(work.allocator())) |b| {
        if (b.len > 0) try out.write(b);
        _ = work.reset(.retain_capacity);
    }
    ctx.results[p] = try out.finish();
}

const partWorker = dispatchWorker(PartCtx, aggregatePart);

/// Every partition's groups, in partition order, as one source.
const GroupsSource = struct {
    schema: *const types.Schema,
    results: []?spill.Run,
    at: usize = 0,
    cur: ?spill_parts.RunSource = null,

    const vtable = driver.Source.VTable{ .schema = schemaFn, .next = nextFn, .close = closeFn };

    fn source(self: *GroupsSource) driver.Source {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn schemaFn(ptr: *anyopaque) types.Schema {
        const self: *GroupsSource = @ptrCast(@alignCast(ptr));
        return self.schema.*;
    }

    fn nextFn(ptr: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
        const self: *GroupsSource = @ptrCast(@alignCast(ptr));
        while (true) {
            if (self.cur) |*c| {
                if (try c.src().next(arena)) |b| return b;
                self.cur = null;
            }
            while (self.at < self.results.len and self.results[self.at] == null) self.at += 1;
            if (self.at >= self.results.len) return null;
            self.cur = .{ .run = self.results[self.at].? };
            self.results[self.at] = null;
            self.at += 1;
        }
    }

    fn closeFn(ptr: *anyopaque) void {
        const self: *GroupsSource = @ptrCast(@alignCast(ptr));
        if (self.cur) |*c| c.end();
        self.cur = null;
        for (self.results) |*r| if (r.*) |held| {
            spill.discard(held);
            r.* = null;
        };
    }
};

/// The grouped aggregate `plan` over `p`, partitioned to disk as the header says, its
/// groups written through `tail` into `snk`. `lanes` bounds both phases' workers.
/// `rows_read` counts the second read of the input, which the caller reconciles.
pub fn run(
    env: *Env,
    p: Pipeline,
    plan: op.Aggregate,
    lanes: usize,
    snk: driver.Sink,
    tail: []const analyze.Resolved,
    stats: *Stats,
) !usize {
    var rs = RouteSink{ .gpa = env.gpa, .space = env.space.?, .schema = plan.in_schema, .by = plan.by };
    defer rs.deinit();
    var pp = p;
    pp.per_lane = false;
    const used = try drive(env.arena, pp, rs.parallelSink(), lanes);

    const results = try env.arena.alloc(?spill.Run, fanout);
    @memset(results, null);
    const paths = try env.arena.alloc(std.heap.ArenaAllocator, fanout);
    for (paths) |*a| a.* = .init(env.gpa);
    defer for (paths) |*a| a.deinit();
    const workers = @min(lanes, fanout);
    var ctx = PartCtx{
        .env = env,
        .route = &rs,
        .plan = plan,
        .spill_at = @max(env.op_memory / fanout, 1),
        .results = results,
        .paths = paths,
        .queue = .{ .nitems = fanout },
    };
    var groups = GroupsSource{ .schema = plan.out_schema, .results = results };
    defer groups.source().close();
    _ = try parallel.spawnJoin(env.arena, workers, partWorker, &ctx);
    if (ctx.queue.failure()) |e| return e;
    try writeTailFrom(env, snk, groups.source(), tail, stats, .aggregate);
    return used;
}
