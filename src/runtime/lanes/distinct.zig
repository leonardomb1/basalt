//! DISTINCT lanes: each lane keeps the first row per key it saw, merged by file order
//! so the first row overall wins.

const Batch = @import("../../exec/batch.zig").Batch;
const Env = @import("../env.zig").Env;
const LaneSplit = @import("sources.zig").LaneSplit;
const RunOptions = @import("../env.zig").RunOptions;
const Stats = @import("../env.zig").Stats;
const Value = @import("../../exec/value.zig").Value;
const WorkQueue = @import("../lanes.zig").WorkQueue;
const aErr = @import("../plan.zig").aErr;
const analyze = @import("../analyze.zig");
const ast = @import("../../lang/ast.zig");
const buildMapChain = @import("../plan.zig").buildMapChain;
const column = @import("../../exec/column.zig");
const csvSplitFile = @import("sources.zig").csvSplitFile;
const dispatchWorker = @import("../lanes.zig").dispatchWorker;
const driver = @import("../../connect/driver.zig");
const laneRowSource = @import("sources.zig").laneRowSource;
const mapChainSchema = @import("../plan.zig").mapChainSchema;
const obs = @import("../obs.zig");
const op = @import("../../exec/op.zig");
const openSink = @import("../connect.zig").openSink;
const parallel = @import("../parallel.zig");
const parquetSplit = @import("sources.zig").parquetSplit;
const pqdecode = @import("../../format/parquet/read.zig");
const resolveUpsertKeys = @import("../connect.zig").resolveUpsertKeys;
const schemaPtr = @import("../env.zig").schemaPtr;
const sinkLabel = @import("../connect.zig").sinkLabel;
const std = @import("std");
const tailSchema = @import("../plan.zig").tailSchema;
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

const DistinctRow = struct {
    chunk: usize,
    ord: u64,
    vals: []Value,

    fn before(_: void, a: DistinctRow, b: DistinctRow) bool {
        if (a.chunk != b.chunk) return a.chunk < b.chunk;
        return a.ord < b.ord;
    }
};

const DistinctMerge = struct {
    seen: op.Aggregate.GroupMap(),
    rows: std.array_list.Managed(DistinctRow),
    key_idx: []const usize,
    arena: std.mem.Allocator,
    mtx: std.Thread.Mutex = .{},

    pub fn init(arena: std.mem.Allocator, key_idx: []const usize) DistinctMerge {
        return .{
            .seen = op.Aggregate.GroupMap().init(arena),
            .rows = std.array_list.Managed(DistinctRow).init(arena),
            .key_idx = key_idx,
            .arena = arena,
        };
    }

    fn dupeRow(self: *DistinctMerge, b: Batch, r: usize) ![]Value {
        const vals = try self.arena.alloc(Value, b.columns.len);
        for (b.columns, vals) |*col, *v| v.* = try op.dupeValue(self.arena, col.getValue(r));
        return vals;
    }

    fn absorb(self: *DistinctMerge, chunk: usize, b: Batch, ords: []const u64, probe: []Value) !void {
        self.mtx.lock();
        defer self.mtx.unlock();
        var r: usize = 0;
        while (r < b.len) : (r += 1) {
            for (self.key_idx, 0..) |ci, j| probe[j] = b.columns[ci].getValue(r);
            const gop = try self.seen.getOrPut(probe);
            if (gop.found_existing) {
                const slot = &self.rows.items[gop.value_ptr.*];
                const cand = DistinctRow{ .chunk = chunk, .ord = ords[r], .vals = &.{} };
                if (!DistinctRow.before({}, cand, slot.*)) continue;
                slot.chunk = chunk;
                slot.ord = ords[r];
                slot.vals = try self.dupeRow(b, r);
                continue;
            }
            const kv = try self.arena.alloc(Value, self.key_idx.len);
            for (self.key_idx, 0..) |ci, j| kv[j] = try op.dupeValue(self.arena, b.columns[ci].getValue(r));
            gop.key_ptr.* = kv;
            gop.value_ptr.* = self.rows.items.len;
            try self.rows.append(.{ .chunk = chunk, .ord = ords[r], .vals = try self.dupeRow(b, r) });
        }
    }

    /// Input order, then one batch: sorting here makes `-j 8` agree with `-j 1` byte for byte.
    fn finish(self: *DistinctMerge, row_schema: *const types.Schema) !Batch {
        std.mem.sort(DistinctRow, self.rows.items, {}, DistinctRow.before);
        const cols = try self.arena.alloc(column.Column, row_schema.fields.len);
        for (row_schema.fields, cols, 0..) |f, *c, ci| {
            var bd = column.Builder.init(self.arena, f.ty);
            for (self.rows.items) |row| try bd.append(row.vals[ci]);
            c.* = try bd.finish();
        }
        return .{ .schema = row_schema, .columns = cols, .len = self.rows.items.len };
    }
};

const DistinctCtx = struct {
    split: LaneSplit,
    row_schema: *const types.Schema,
    prefix: []const ast.Stage,
    params: *std.StringHashMap(*const ast.Expr),
    errctx: ?*op.ErrCtx = null,
    local_keys: ?[]const usize,
    key_idx: []const usize,
    queue: WorkQueue,
    merge: *DistinctMerge,
    rows_read: *obs.RowCounter,
};

const distinctWorker = dispatchWorker(DistinctCtx, distinctWorkOne);

/// Dedups per item rather than per lane, so the item index is a file position and the
/// merge keeps the same row a serial run would.
fn distinctWorkOne(ctx: *DistinctCtx, i: usize) !void {
    var wgpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = false }){};
    defer _ = wgpa.deinit();
    var warena = std.heap.ArenaAllocator.init(wgpa.allocator());
    defer warena.deinit();
    var batch_arena = std.heap.ArenaAllocator.init(wgpa.allocator());
    defer batch_arena.deinit();

    const src_schema = ctx.split.schema();
    const inner = (try laneRowSource(ctx.split.rows(i, ctx.queue.nitems), warena.allocator())) orelse return;
    defer inner.close();
    var cs = obs.CountingSource{ .inner = inner, .count = ctx.rows_read };
    var scan = op.Scan{ .src = cs.source() };
    const child = try buildMapChain(warena.allocator(), ctx.params, ctx.errctx, ctx.prefix, &scan, src_schema);
    var d = op.Distinct{ .child = child, .in_schema = ctx.row_schema, .keys = ctx.local_keys, .state = warena.allocator(), .gpa = wgpa.allocator(), .track_ords = true };

    const probe = try warena.allocator().alloc(Value, ctx.key_idx.len);
    while (try d.next(batch_arena.allocator())) |b| {
        try ctx.merge.absorb(i, b, d.ords, probe);
        _ = batch_arena.reset(.retain_capacity);
    }
}

pub const ReaderSource = struct {
    r: *pqdecode.Reader,
    schema_: *const types.Schema,

    fn schemaFn(ptr: *anyopaque) types.Schema {
        const self: *ReaderSource = @ptrCast(@alignCast(ptr));
        return self.schema_.*;
    }
    fn nextFn(ptr: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
        const self: *ReaderSource = @ptrCast(@alignCast(ptr));
        return self.r.next(arena);
    }
    fn closeFn(ptr: *anyopaque) void {
        const self: *ReaderSource = @ptrCast(@alignCast(ptr));
        self.r.close();
    }
    pub const vtable = driver.Source.VTable{ .schema = schemaFn, .next = nextFn, .close = closeFn };
};

fn runParallelDistinct(
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

    const nthreads = split.lanes(opts.threads);
    const nitems = split.count(nthreads);
    var merge = DistinctMerge.init(arena, key_idx);
    var ctx = DistinctCtx{
        .split = split,
        .row_schema = row_schema,
        .prefix = prefix,
        .params = env.params_expr,
        .errctx = env.errctx,
        .local_keys = local_keys,
        .key_idx = key_idx,
        .queue = .{ .nitems = nitems },
        .merge = &merge,
        .rows_read = env.rows_read,
    };

    const lanes = try parallel.spawnJoin(arena, nthreads, distinctWorker, &ctx);
    lanes_used.* = @max(lanes_used.*, lanes);
    env.log.log(.debug, "parallel {s} distinct: {d} {s} over {d} lanes", .{ split.label(), nitems, split.unitName(), lanes });
    if (ctx.queue.failure()) |e| return e;

    const merged = try merge.finish(row_schema);

    const wr = try resolveUpsertKeys(env, w);
    const snk = try openSink(env, wr, try tailSchema(env, tail, row_schema.*));
    var snk_open = true;
    errdefer if (snk_open) snk.abort();
    try writeTail(env, snk, merged, row_schema.*, tail, stats);
    snk_open = false;
    try snk.close();
    return true;
}

pub fn runParallelParquetDistinct(env: *Env, rd: ast.Read, pipeline: []const ast.Stage, prefix: []const ast.Stage, dist: ast.Distinct, tail: []const ast.Stage, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    const split = (try parquetSplit(env, rd, pipeline[1..], w, opts)) orelse return false;
    return runParallelDistinct(env, split, prefix, dist, tail, w, opts, stats, lanes_used);
}

pub fn runParallelCsvDistinct(env: *Env, rd: ast.Read, prefix: []const ast.Stage, dist: ast.Distinct, tail: []const ast.Stage, w: ast.Write, opts: RunOptions, stats: *Stats, lanes_used: *usize) anyerror!bool {
    const mapped = (try csvSplitFile(env, rd, w)) orelse return false;
    defer mapped.close();
    return runParallelDistinct(env, .{ .csv = .{ .mapped = mapped, .schema = &mapped.schema } }, prefix, dist, tail, w, opts, stats, lanes_used);
}
