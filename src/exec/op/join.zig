//! Hash joins: the build side materialized and indexed once (`JoinIndex`), the probe
//! side streamed through it, for every join kind.
//!
//! Grace mode. Given a `Space`, a join whose build side passes `spill_at` bytes
//! (the join's `max_build`, else the run's `--op-memory`) spills instead of
//! failing: the build rows already pulled and the rest of the build stream are
//! split by key hash into `grace_parts` files, then the whole probe side (read-
//! ahead replay included) the same way, and each partition is joined on its own —
//! its build file indexed in a fresh arena, its probe file streamed through the
//! usual `probeStep`, its unmatched build rows drained for right and full joins —
//! then freed and its files deleted. Peak memory is about one partition's index
//! plus a probe batch, besides the up to `spill_at` bytes held when the switch
//! happens. The partition is the key hash salted per depth (`spill_parts.partOf`;
//! the index buckets on the unsalted hash's low bits), and each side hashes a key
//! that the other side types differently boxed, so equal int, float and decimal
//! keys land together at every depth. A partition whose build file still passes
//! `spill_at` is split again, both its files, one depth deeper, up to
//! `max_spill_depth` levels (16^4 partitions by default); the deepest partitions
//! are joined first and every file is deleted once split or joined, so the disk
//! holds about one copy of the data plus the partition being split. A partition
//! no split can spread — every build row has one key hash — is not split: it is
//! indexed if it fits `max(spill_at, build_cap)`, else fails at once with
//! `JoinBuildTooLarge` naming the single key; one still too large at the depth
//! limit fails with `SpillTooDeep`. A null-key build row matches nothing: dropped,
//! or for right and full joins kept in a file of its own and streamed out last; a
//! null-key probe row follows partition 0 down. Row order is not kept. NOT IN never spills (a null key
//! anywhere on the build side changes every probe row's answer), nor CROSS: both
//! keep the in-memory cap (the planner gives NOT IN the spill threshold as its
//! cap). CROSS emits its pairs `cross_chunk` at a time, and every other kind its
//! output `out_chunk` rows at a time, so a key that multiplies rows streams. In grace mode `push_probe` is never applied, so a late
//! probe source opens without a key predicate. Without a `Space` the join fails
//! with `JoinBuildTooLarge` past `build_cap`, as before.

const Batch = @import("../batch.zig").Batch;
const ErrCtx = @import("../op.zig").ErrCtx;
const Op = @import("../op.zig").Op;
const Stats = @import("../op.zig").Stats;
const ast = @import("../../lang/ast.zig");
const column = @import("../column.zig");
const failLabel = @import("../op.zig").failLabel;
const keyhash = @import("../keyhash.zig");
const keyset = @import("keyset.zig");
const par = @import("../par.zig");
const keytable = @import("../keytable.zig");
const Value = @import("../value.zig").Value;
const spill = @import("../spill.zig");
const SpillDir = spill.TestDir;
const spill_parts = @import("spill_parts.zig");
const Space = @import("../space.zig").Space;
const eval = @import("../eval.zig");
const std = @import("std");
const op_mod = @import("../op.zig");
const types = @import("../../lang/types.zig");
const JoinRows = @import("testing_util.zig").JoinRows;
const Scan = @import("../op.zig").Scan;
const TestSource = @import("testing_util.zig").TestSource;
const expectSameRows = @import("testing_util.zig").expectSameRows;
const lessStr = @import("testing_util.zig").lessStr;
const join_both_schema = @import("testing_util.zig").join_both_schema;
const join_left_schema = @import("testing_util.zig").join_left_schema;
const join_right_schema = @import("testing_util.zig").join_right_schema;
const kvBatch = @import("testing_util.zig").kvBatch;
const testing = std.testing;

/// The build side's batches, pulled into `pull`, and their bytes. Fails with
/// `JoinBuildTooLarge` as soon as they pass `cap`.
fn drainBuild(pull: std.mem.Allocator, child: Op, bytes_out: *usize, cap: usize) anyerror![]const Batch {
    var chunks = std.array_list.Managed(Batch).init(pull);
    var bytes: usize = 0;
    while (try child.next(pull)) |b| {
        bytes += b.heapBytes();
        if (bytes > cap) return error.JoinBuildTooLarge;
        if (b.len == 0) continue;
        try chunks.append(b);
    }
    bytes_out.* = bytes;
    return chunks.items;
}

/// A NOT IN subquery past its cap: it never spills, so the way out is more memory.
pub fn notInTooLarge(ec: *ErrCtx, cap: usize) void {
    ec.set("NOT IN subquery's rows pass the {d}-byte limit — a NOT IN build side never spills; raise --op-memory, or filter the subquery", .{cap});
}

/// `chunks` concatenated into one batch in `state` that is never null, so a join
/// can emit right-side nulls for an empty build.
fn concatBatches(state: std.mem.Allocator, chunks: []const Batch, schema: *const types.Schema, total: usize) anyerror!Batch {
    const cols = try state.alloc(column.Column, schema.fields.len);
    if (total == 0) {
        for (cols, schema.fields) |*c, f| {
            var bd = column.Builder.init(state, f.ty);
            c.* = try bd.finish();
        }
    } else {
        const per = try state.alloc(column.Column, chunks.len);
        for (cols, 0..) |*out, ci| {
            for (chunks, 0..) |b, k| per[k] = b.columns[ci];
            out.* = try column.concat(state, per, total);
        }
    }
    return Batch{ .schema = schema, .columns = cols, .len = total };
}

/// What `JoinIndex` adds over `rows` keyed rows of `nk` keys: the table's slots, a
/// record per key (hash, words, mask, first row) and a chain link per row.
fn indexOverhead(rows: usize, nk: usize) usize {
    var slots: usize = 16;
    while (slots * 7 < (rows + 1) * 10) slots *= 2;
    return slots * @sizeOf(u32) + rows * (3 * @sizeOf(u64) + nk * @sizeOf(i64) + @sizeOf(u32));
}

pub const KeyClass = enum { i64s, bytes, boxed };

/// How a key position compares. Same kind required: `int` and `timestamp` share the
/// i64 store but are never equal. `.i64s` is the kinds `keyhash.keyKindOf` keys by
/// their raw i64.
fn classOf(a: column.Column, b: column.Column) KeyClass {
    if (a.ty.kind != b.ty.kind) return .boxed;
    return switch (a.data) {
        .i64 => if (keyhash.keyKindOf(a.ty.kind) == .i64k and std.meta.activeTag(b.data) == .i64) .i64s else .boxed,
        .bytes => if (std.meta.activeTag(b.data) == .bytes) .bytes else .boxed,
        else => .boxed,
    };
}

fn cellEq(cls: KeyClass, a: *const column.Column, ar: usize, b: *const column.Column, br: usize) bool {
    return switch (cls) {
        .i64s => a.data.i64[ar] == b.data.i64[br],
        .bytes => std.mem.eql(u8, a.data.bytes.at(ar), b.data.bytes.at(br)),
        .boxed => keyhash.valueEq(a.getValue(ar), b.getValue(br)),
    };
}

/// Composite key hash for one row, folded from the typed store exactly as
/// `keyhash.MultiKeyCtx` folds boxed values, so build and probe sides agree.
fn hashRowKeys(cols: []const column.Column, keys: []const usize, classes: []const KeyClass, row: usize) u64 {
    var h = std.hash.Wyhash.init(0);
    for (keys, classes) |k, cls| {
        const c = &cols[k];
        switch (cls) {
            .i64s => switch (c.ty.kind) {
                .int => keyhash.hashInt(&h, c.data.i64[row]),
                .time => keyhash.hashTagged(&h, .time, std.mem.asBytes(&c.data.i64[row])),
                .timestamp => keyhash.hashTagged(&h, .timestamp, std.mem.asBytes(&c.data.i64[row])),
                else => keyhash.hashValue(&h, c.getValue(row)),
            },
            .bytes => keyhash.hashTagged(&h, if (c.ty.kind == .string) .string else .bytes, c.data.bytes.at(row)),
            .boxed => keyhash.hashValue(&h, c.getValue(row)),
        }
    }
    return h.final();
}

fn anyNullKey(cols: []const column.Column, keys: []const usize, row: usize) bool {
    for (keys) |k| {
        if (!cols[k].validity.get(row)) return true;
    }
    return false;
}

pub const grace_parts = spill_parts.fanout;
pub const default_grace_depth: u8 = 4;

/// Which key positions the two sides type differently (int against decimal, say):
/// those hash boxed on both sides, as `classOf` would pair them.
fn mixedKeys(arena: std.mem.Allocator, schema: *const types.Schema, keys: []const usize, other: *const types.Schema, other_keys: []const usize) ![]bool {
    const mixed = try arena.alloc(bool, keys.len);
    for (keys, other_keys, mixed) |k, ok, *m| m.* = schema.fields[k].ty.kind != other.fields[ok].ty.kind;
    return mixed;
}

/// One side's key classes for partitioning `b`.
fn partClasses(arena: std.mem.Allocator, b: Batch, keys: []const usize, mixed: []const bool) ![]KeyClass {
    const classes = try arena.alloc(KeyClass, keys.len);
    for (keys, mixed, classes) |k, m, *c| c.* = if (m) .boxed else classOf(b.columns[k], b.columns[k]);
    return classes;
}

/// A row's partition at `depth`: its key hash salted for that depth by
/// `spill_parts.partOf`. `hashRowKeys` folds equal keys of int, float and decimal
/// alike, so both sides agree at every depth, and the keys of one partition spread
/// over all of the next depth's. A row with a null key goes to `nulls`.
fn partOf(cols: []const column.Column, keys: []const usize, classes: []const KeyClass, row: usize, depth: u8, nulls: u8) u8 {
    if (anyNullKey(cols, keys, row)) return nulls;
    return spill_parts.partOf(hashRowKeys(cols, keys, classes, row), depth);
}

/// A `Split` destination for rows that are written nowhere.
const drop_row: u8 = spill_parts.keep;

/// One side's rows split by key hash into `grace_parts` files at one depth, plus
/// a file at index `grace_parts` for the null-key rows when `nulls` sends them
/// there. A file is made on its first row. The writers' buffers live in the
/// split's own arena, freed by `deinit`, which also deletes the files of a split
/// that never finished; only the paths `finish` copies out outlast it.
const Split = struct {
    arena: std.heap.ArenaAllocator,
    space: Space,
    schema: *const types.Schema,
    keys: []const usize,
    mixed: []const bool,
    depth: u8,
    nulls: u8,
    tag: []const u8,
    ws: [grace_parts + 1]?spill.Writer = @splat(null),

    fn init(space: Space, schema: *const types.Schema, keys: []const usize, mixed: []const bool, depth: u8, nulls: u8, tag: []const u8) Split {
        return .{ .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator), .space = space, .schema = schema, .keys = keys, .mixed = mixed, .depth = depth, .nulls = nulls, .tag = tag };
    }

    /// `b`'s rows written to the files of their partitions, gathered in `scratch`.
    fn add(self: *Split, scratch: std.mem.Allocator, b: Batch) !void {
        if (b.len == 0) return;
        const classes = try partClasses(scratch, b, self.keys, self.mixed);
        const parts = try scratch.alloc(u8, b.len);
        var counts = [_]usize{0} ** (grace_parts + 1);
        for (parts, 0..) |*p, r| {
            p.* = partOf(b.columns, self.keys, classes, r, self.depth, self.nulls);
            if (p.* != drop_row) counts[p.*] += 1;
        }
        var starts: [grace_parts + 1]usize = undefined;
        var at: usize = 0;
        for (&starts, counts) |*s, c| {
            s.* = at;
            at += c;
        }
        const order = try scratch.alloc(usize, at);
        var fill = starts;
        for (parts, 0..) |p, r| {
            if (p == drop_row) continue;
            order[fill[p]] = r;
            fill[p] += 1;
        }
        for (&self.ws, starts, counts) |*w, s, c| {
            if (c == 0) continue;
            const idx = order[s..][0..c];
            const cols = try scratch.alloc(column.Column, b.columns.len);
            for (cols, b.columns) |*out, col| out.* = try column.permute(scratch, col, idx);
            if (w.* == null) w.* = try spill.Writer.init(self.space, self.arena.allocator(), self.schema, self.tag);
            try w.*.?.write(.{ .schema = b.schema, .columns = cols, .len = c });
        }
    }

    fn addRun(self: *Split, run: spill.Run) !void {
        var r = try spill.Reader.open(run);
        defer r.close();
        var scratch = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer scratch.deinit();
        while (try r.next(scratch.allocator())) |b| {
            try self.add(scratch.allocator(), b);
            _ = scratch.reset(.retain_capacity);
        }
    }

    /// Closes the files, their paths copied into `keep`; a partition without rows
    /// has none. On failure every file is deleted.
    fn finish(self: *Split, keep: std.mem.Allocator) ![grace_parts + 1]?spill.Run {
        var runs: [grace_parts + 1]?spill.Run = @splat(null);
        errdefer for (runs) |r| if (r) |run| spill.discard(run);
        for (&self.ws, &runs) |*w, *r| if (w.*) |*wr| {
            r.* = try wr.finish();
            w.* = null;
            r.*.?.path = try keep.dupe(u8, r.*.?.path);
        };
        return runs;
    }

    fn deinit(self: *Split) void {
        for (&self.ws) |*w| if (w.*) |*wr| wr.abort();
        self.arena.deinit();
    }
};

/// A partition: its build file and its probe file, either absent when that side
/// has no rows there, and the depth it was split at.
const Pair = struct { build: ?spill.Run = null, probe: ?spill.Run = null, depth: u8 = 0 };

fn dropPair(p: *Pair) void {
    if (p.build) |r| spill.discard(r);
    if (p.probe) |r| spill.discard(r);
    p.* = .{};
}

/// How a partition's build side came to be indexed: within `spill_at`, or past it
/// because no split can help — every row has one key, or the depth limit is reached.
const Fit = enum { fits, one_key, too_deep };

/// A join spilled to disk: a stack of partitions joined one at a time. A partition
/// whose build side passes `spill_at` is split again one depth deeper and its
/// partitions pushed in its place, so the deepest are joined first and the disk
/// holds about one copy of the data plus the partition being split. `cur` is the
/// partition popped and not yet consumed; its files are deleted as they are read,
/// or by `deinit`. `files` holds the stack and the paths; `part_mem` the current
/// partition's index and match flags; `lone` reads a build file whose rows no
/// probe row can match (the null keys, or a partition without probe rows).
const Grace = struct {
    files: std.heap.ArenaAllocator,
    pending: std.ArrayList(Pair) = .empty,
    build_mixed: []const bool = &.{},
    probe_mixed: []const bool = &.{},
    first: usize = 0,
    probed: bool = false,
    done: bool = false,
    deepest: u8 = 0,
    cur: Pair = .{},
    part_mem: ?std.heap.ArenaAllocator = null,
    index: ?*JoinIndex = null,
    reader: ?spill.Reader = null,
    lone: ?spill.Reader = null,

    fn closePart(self: *Grace) void {
        if (self.reader) |*r| r.close();
        self.reader = null;
        if (self.lone) |*r| r.close();
        self.lone = null;
        if (self.part_mem) |*m| m.deinit();
        self.part_mem = null;
        self.index = null;
        dropPair(&self.cur);
    }

    /// Frees what is left and deletes the files not yet joined; later calls are no-ops.
    fn deinit(self: *Grace) void {
        if (self.done) return;
        self.closePart();
        for (self.pending.items) |*p| dropPair(p);
        self.files.deinit();
        self.done = true;
    }
};

/// The record payload of a key: the first build row holding it, the rest chained
/// through `JoinIndex.next` in build order.
const Head = struct {
    size: usize = @sizeOf(u64),

    pub fn clear(_: *const Head, tail: [*]u8) void {
        @as(*u64, @ptrCast(@alignCast(tail))).* = 0;
    }

    fn of(tail: [*]u8) *u64 {
        return @ptrCast(@alignCast(tail));
    }
};

const head_payload = Head{};

/// A probe batch keyed for `JoinIndex.first`: each row's hash, its key in the build's
/// words (or boxed), and `miss`, a row that cannot match (a NULL key, or a value no
/// build key can equal).
pub const Probe = struct {
    hashes: []u64,
    miss: []bool,
    enc: ?keytable.Encoded = null,
    boxed: []const Value = &.{},
};

/// The build side materialized and its keys in the shared key table: fixed-width
/// words when every build key has a fixed encoding, boxed values otherwise. A key's
/// record holds its first build row; `next` chains the rest in build order. A large
/// build is split into `keytable.parts` partitions by the key hash's top bits, each
/// filled on its own thread. Read-only once built, so the lanes probe one index.
pub const JoinIndex = struct {
    build_batch: Batch,
    keys: []const usize,
    next: []const u32 = &.{},
    has_null_key: bool = false,
    parts: []Part = &.{},
    fixed: bool = true,
    kinds: []const keyhash.KeyKind = &.{},
    want: []const types.TypeKind = &.{},
    strs: ?*keytable.StrTable = null,
    str_cache: std.StringHashMapUnmanaged(keytable.StrId) = .empty,

    const Part = struct {
        table: keytable.GroupTable,
        fixed: *keytable.FixedStore(Head),
        boxed: *keytable.GroupStore(Head),
    };

    /// A build at least this large is partitioned and filled in parallel.
    const parallel_min = 1 << 16;

    /// Drain `build` into `pull`, materialize it in `state` and index `right_keys` on up
    /// to `threads` threads.
    pub fn create(
        state: std.mem.Allocator,
        pull: std.mem.Allocator,
        build: Op,
        right_schema: *const types.Schema,
        right_keys: []const usize,
        cap: usize,
        threads: usize,
    ) anyerror!*JoinIndex {
        var bytes: usize = 0;
        const chunks = try drainBuild(pull, build, &bytes, cap);
        return fromBatches(state, chunks, right_schema, right_keys, bytes, cap, threads);
    }

    /// Index `chunks` (holding `bytes`), copied into `state`; checked against `cap`
    /// before anything is copied. Each partition takes its rows in reverse, so
    /// prepending leaves duplicate chains in build order.
    pub fn fromBatches(
        state: std.mem.Allocator,
        chunks: []const Batch,
        right_schema: *const types.Schema,
        right_keys: []const usize,
        bytes: usize,
        cap: usize,
        threads: usize,
    ) anyerror!*JoinIndex {
        var n: usize = 0;
        for (chunks) |b| n += b.len;
        if (n >= std.math.maxInt(u32)) return error.JoinBuildTooLarge;
        if (bytes + (if (right_keys.len > 0) indexOverhead(n, right_keys.len) else 0) > cap) return error.JoinBuildTooLarge;
        const batch = try concatBatches(state, chunks, right_schema, n);

        const self = try state.create(JoinIndex);
        self.* = .{ .build_batch = batch, .keys = right_keys };
        if (right_keys.len == 0) return self;

        const want = try state.alloc(types.TypeKind, right_keys.len);
        const kinds = try state.alloc(keyhash.KeyKind, right_keys.len);
        for (right_keys, want, kinds) |k, *w, *kk| {
            w.* = right_schema.fields[k].ty.kind;
            kk.* = keyhash.keyKindOf(w.*) orelse blk: {
                self.fixed = false;
                break :blk .i64k;
            };
        }
        self.want = want;
        self.kinds = kinds;
        const chain = try state.alloc(u32, n);
        @memset(chain, 0);
        self.next = chain;

        var b = Build{ .ix = self, .n = n, .hashes = try state.alloc(u64, n), .nulls = try state.alloc(bool, n) };
        if (self.fixed) {
            const enc = try keytable.Encoder.init(state, right_keys, kinds);
            b.enc = try enc.encode(state, batch.columns, n, self.interner(state), false);
        }
        const nparts: usize = if (n >= parallel_min and threads > 1) keytable.parts else 1;
        b.nc = par.chunks(n, 1 << 15);
        if (!self.fixed) b.vals = try state.alloc(Value, (b.nc + keytable.parts) * right_keys.len);
        b.counts = try state.alloc(u32, b.nc * nparts);
        @memset(b.counts, 0);
        b.nparts = nparts;
        par.each(threads, b.nc, &b, Build.hashChunk);
        b.starts = try state.alloc(u32, nparts + 1);
        b.offs = try state.alloc(u32, b.nc * nparts);
        var at: u32 = 0;
        for (0..nparts) |p| {
            b.starts[p] = at;
            for (0..b.nc) |c| {
                b.offs[c * nparts + p] = at;
                at += b.counts[c * nparts + p];
            }
        }
        b.starts[nparts] = at;
        b.order = try state.alloc(u32, at);
        par.each(threads, b.nc, &b, Build.scatterChunk);

        var tsa = std.heap.ThreadSafeAllocator{ .child_allocator = state };
        b.alloc = tsa.allocator();
        self.parts = try state.alloc(Part, nparts);
        par.each(threads, nparts, &b, Build.fillPart);
        if (b.failed) return error.OutOfMemory;
        for (b.nulls) |nl| if (nl) {
            self.has_null_key = true;
            break;
        };
        return self;
    }

    /// The parallel build's shared state: each row's hash and partition order, filled
    /// a chunk at a time, then each partition's table a partition at a time.
    const Build = struct {
        ix: *JoinIndex,
        n: usize,
        hashes: []u64,
        nulls: []bool,
        enc: ?keytable.Encoded = null,
        nc: usize = 0,
        nparts: usize = 1,
        counts: []u32 = &.{},
        offs: []u32 = &.{},
        starts: []u32 = &.{},
        order: []u32 = &.{},
        alloc: std.mem.Allocator = undefined,
        failed: bool = false,
        vals: []Value = &.{},

        fn part(b: *const Build, h: u64) usize {
            return if (b.nparts == 1) 0 else keytable.partOf(h);
        }

        fn hashChunk(b: *Build, c: usize) void {
            const r0, const r1 = par.bounds(b.n, b.nc, c);
            const cols = b.ix.build_batch.columns;
            const nk = b.ix.keys.len;
            for (r0..r1) |r| {
                b.nulls[r] = anyNullKey(cols, b.ix.keys, r);
                if (b.nulls[r]) continue;
                b.hashes[r] = if (b.enc) |e| e.hash(r) else blk: {
                    const v = b.vals[c * nk ..][0..nk];
                    for (b.ix.keys, v) |k, *x| x.* = cols[k].getValue(r);
                    break :blk keyhash.MultiKeyCtx.hash(.{}, v);
                };
                b.counts[c * b.nparts + b.part(b.hashes[r])] += 1;
            }
        }

        fn scatterChunk(b: *Build, c: usize) void {
            const r0, const r1 = par.bounds(b.n, b.nc, c);
            const offs = b.offs[c * b.nparts ..][0..b.nparts];
            for (r0..r1) |r| {
                if (b.nulls[r]) continue;
                const p = b.part(b.hashes[r]);
                b.order[offs[p]] = @intCast(r);
                offs[p] += 1;
            }
        }

        fn fillPart(b: *Build, p: usize) void {
            b.fill(p) catch {
                b.failed = true;
            };
        }

        fn fill(b: *Build, p: usize) !void {
            const ix = b.ix;
            const rows_ = b.order[b.starts[p]..b.starts[p + 1]];
            var slots: usize = 16;
            while (slots * 7 < (rows_.len + 1) * 10) slots *= 2;
            const pt = &ix.parts[p];
            pt.table = try keytable.GroupTable.init(b.alloc, slots);
            pt.fixed = try b.alloc.create(keytable.FixedStore(Head));
            pt.fixed.* = .init(b.alloc, ix.keys.len, &head_payload);
            pt.boxed = try b.alloc.create(keytable.GroupStore(Head));
            pt.boxed.* = .init(b.alloc, ix.keys.len, &head_payload);
            const nk = ix.keys.len;
            var i = rows_.len;
            while (i > 0) {
                i -= 1;
                const r = rows_[i];
                const h = b.hashes[r];
                if (b.enc) |e| {
                    const key = e.key(r);
                    const f = try pt.table.getOrPut(h, key, pt.fixed, @intCast(pt.fixed.len));
                    const rec = if (f.found) pt.fixed.at(f.slot) else blk: {
                        const nr = try pt.fixed.push(h);
                        @memcpy(nr.keys, key.vals);
                        nr.mask.* = key.mask;
                        break :blk nr;
                    };
                    ix.link(Head.of(rec.tail), f.found, r);
                } else {
                    const v = b.vals[(b.nc + p) * nk ..][0..nk];
                    for (ix.keys, v) |k, *x| x.* = ix.build_batch.columns[k].getValue(r);
                    const f = try pt.table.getOrPut(h, @as([]const Value, v), pt.boxed, @intCast(pt.boxed.len));
                    const rec = if (f.found) pt.boxed.at(f.slot) else blk: {
                        const nr = try pt.boxed.push(h);
                        @memcpy(nr.keys, v);
                        break :blk nr;
                    };
                    ix.link(Head.of(rec.tail), f.found, r);
                }
            }
        }
    };

    /// Prepends build row `r` to its key's chain.
    fn link(self: *JoinIndex, head: *u64, found: bool, r: usize) void {
        if (found) @constCast(self.next)[r] = @intCast(head.* + 1);
        head.* = r;
    }

    fn interner(self: *JoinIndex, a: std.mem.Allocator) keytable.Interner {
        return .{ .table = &self.strs, .cache = &self.str_cache, .alloc = a };
    }

    fn partOfHash(self: *const JoinIndex, h: u64) *const Part {
        return &self.parts[if (self.parts.len == 1) 0 else keytable.partOf(h)];
    }

    pub fn rows(self: *const JoinIndex) usize {
        return self.build_batch.len;
    }

    /// The encoder a probing join keys its batches with, the build's key words its
    /// target; in `a`, as one index of a grace join is freed before the next.
    pub fn probeEncoder(self: *const JoinIndex, a: std.mem.Allocator, probe_keys: []const usize) !?keytable.Encoder {
        if (!self.fixed or self.keys.len == 0) return null;
        var enc = try keytable.Encoder.init(a, probe_keys, try a.dupe(keyhash.KeyKind, self.kinds));
        enc.want = try a.dupe(types.TypeKind, self.want);
        return enc;
    }

    /// `lb`'s keys hashed and encoded as the build's, column at a time.
    pub fn probe(self: *const JoinIndex, a: std.mem.Allocator, lb: Batch, probe_keys: []const usize, enc: ?keytable.Encoder) !Probe {
        const miss = try a.alloc(bool, lb.len);
        const hashes = try a.alloc(u64, lb.len);
        const empty = self.keys.len == 0 or self.build_batch.len == 0;
        for (miss, 0..) |*m, r| m.* = empty or anyNullKey(lb.columns, probe_keys, r);
        if (empty) return .{ .hashes = hashes, .miss = miss };
        if (enc) |en| {
            const in = keytable.Interner{ .table = @constCast(&self.strs), .cache = @constCast(&self.str_cache), .alloc = a };
            const e = try en.encode(a, lb.columns, lb.len, in, true);
            for (hashes, miss, 0..) |*h, *m, r| {
                if (e.absent.len > 0 and e.absent[r]) m.* = true;
                h.* = if (m.*) 0 else e.hash(r);
            }
            return .{ .hashes = hashes, .miss = miss, .enc = e };
        }
        const nk = probe_keys.len;
        const vals = try a.alloc(Value, lb.len * nk);
        for (hashes, miss, 0..) |*h, m, r| {
            const v = vals[r * nk ..][0..nk];
            for (probe_keys, v) |k, *x| x.* = lb.columns[k].getValue(r);
            h.* = if (m) 0 else keyhash.MultiKeyCtx.hash(.{}, v);
        }
        return .{ .hashes = hashes, .miss = miss, .boxed = vals };
    }

    pub fn prefetch(self: *const JoinIndex, h: u64) void {
        self.partOfHash(h).table.prefetch(h);
    }

    /// The first build row whose keys equal probe row `r`'s, or null; walk the rest
    /// with `chainNext`.
    pub fn first(self: *const JoinIndex, p: Probe, r: usize) ?usize {
        if (p.miss[r]) return null;
        const h = p.hashes[r];
        const pt = self.partOfHash(h);
        if (self.fixed) {
            const slot = pt.table.find(h, p.enc.?.key(r), pt.fixed) orelse return null;
            return @intCast(Head.of(pt.fixed.at(slot).tail).*);
        }
        const nk = self.keys.len;
        const slot = pt.table.find(h, @as([]const Value, p.boxed[r * nk ..][0..nk]), pt.boxed) orelse return null;
        return @intCast(Head.of(pt.boxed.at(slot).tail).*);
    }

    pub fn chainNext(self: *const JoinIndex, row: usize) ?usize {
        const nx = self.next[row];
        return if (nx == 0) null else nx - 1;
    }
};

/// Key pushdown's limits: the probe rows and bytes read ahead before giving up,
/// and the distinct values sent per key before a range stands in for them.
pub const default_prefetch_rows: usize = 100_000;
pub const default_prefetch_bytes: usize = 64 << 20;
pub const default_push_cap: usize = 1000;

pub const Join = struct {
    stats: Stats = .{},
    probe: Op,
    build: ?Op,
    index: ?*JoinIndex = null,
    left_keys: []const usize,
    right_keys: []const usize,
    probe_enc: ?keytable.Encoder = null,
    probe_keyed: bool = false,
    threads: usize = 1,
    left_schema: *const types.Schema,
    right_schema: *const types.Schema,
    out_schema: *const types.Schema,
    kind: ast.JoinKind,
    null_aware: bool = false,
    state: std.mem.Allocator,
    err: ?*ErrCtx = null,
    build_cap: ?usize = null,
    note: []const u8 = "",

    /// Spilling: with a `space`, a build side past `spill_at` bytes switches the
    /// join to grace mode instead of failing, splitting a partition at most
    /// `max_spill_depth` levels deep.
    space: ?Space = null,
    spill_at: usize = std.math.maxInt(usize),
    max_spill_depth: u8 = default_grace_depth,
    grace: ?*Grace = null,

    /// Key pushdown. `push_build` gets the probe side's keys before the build side
    /// is read: the probe is read ahead into memory up to `prefetch_rows` /
    /// `prefetch_bytes` and replayed, and past either the read-ahead stops and no
    /// keys are sent. `push_probe` gets the build side's keys once it is indexed,
    /// before the first probe row. At most `push_cap` distinct values per key.
    /// The rest of an outer join's ON, checked over `pair_schema` (a left row and a
    /// right row side by side): a key match counts only where it holds.
    residual: ?*const ast.Expr = null,
    pair_schema: ?*const types.Schema = null,

    push_build: ?keyset.KeyPush = null,
    push_probe: ?keyset.KeyPush = null,
    prefetch_rows: usize = default_prefetch_rows,
    prefetch_bytes: usize = default_prefetch_bytes,
    push_cap: usize = default_push_cap,

    matched: ?[]bool = null,
    shared_matched: ?[]std.atomic.Value(u64) = null,
    drain_pos: usize = 0,
    probe_done: bool = false,
    prefetched: bool = false,
    probe_ended: bool = false,
    probe_pushed: bool = false,
    replay: []const Batch = &.{},
    replay_pos: usize = 0,

    probe_mem: ?std.heap.ArenaAllocator = null,
    batch_mem: ?std.heap.ArenaAllocator = null,
    pair_mem: ?std.heap.ArenaAllocator = null,
    pending: ?Pending = null,

    cross_mem: ?std.heap.ArenaAllocator = null,
    cross_batch: ?Batch = null,
    cross_pos: usize = 0,
    cross_chunk: usize = 1 << 16,

    const drain_chunk = 4096;

    pub fn next(self: *Join, arena: std.mem.Allocator) anyerror!?Batch {
        if (self.push_build != null and !self.prefetched) try self.prefetch(arena);
        const ix = (try self.ensureIndex(arena)) orelse return self.nextGrace(arena);
        if (self.push_probe) |kp| if (!self.probe_pushed) {
            self.probe_pushed = true;
            try kp.apply(kp.ctx, try keyset.collect(self.state, &.{ix.build_batch}, self.right_keys, self.push_cap));
        };
        if (self.kind == .cross) return self.nextCross(arena, ix);
        if (!self.probe_done) {
            if (try self.probeStep(arena, ix, ProbeSide{ .join = self })) |b| return b;
            self.probe_done = true;
        }
        return self.drain(arena, ix);
    }

    /// CROSS: each probe batch, held in `cross_mem`, paired with every build row
    /// at most `cross_chunk` pairs per call, so no output batch grows with the
    /// product of the sides. `cross_mem` is backed by `state`, so a consumer that
    /// stops pulling (a LIMIT, an error) leaves nothing behind the plan.
    fn nextCross(self: *Join, arena: std.mem.Allocator, ix: *JoinIndex) anyerror!?Batch {
        const nb = ix.build_batch.len;
        while (true) {
            if (self.cross_batch == null) {
                if (self.probe_done or nb == 0) {
                    if (self.cross_mem) |*m| m.deinit();
                    self.cross_mem = null;
                    return null;
                }
                if (self.cross_mem == null) self.cross_mem = std.heap.ArenaAllocator.init(self.state);
                _ = self.cross_mem.?.reset(.retain_capacity);
                const lb = (try self.nextProbe(self.cross_mem.?.allocator())) orelse {
                    self.probe_done = true;
                    continue;
                };
                if (lb.len == 0) continue;
                self.cross_batch = lb;
                self.cross_pos = 0;
            }
            const lb = self.cross_batch.?;
            const end = @min(lb.len * nb, self.cross_pos + self.cross_chunk);
            const n = end - self.cross_pos;
            const lidx = try arena.alloc(usize, n);
            const ridx = try arena.alloc(usize, n);
            for (lidx, ridx, self.cross_pos..) |*l, *r, p| {
                l.* = p / nb;
                r.* = p % nb;
            }
            self.cross_pos = end;
            if (end == lb.len * nb) self.cross_batch = null;
            return try self.gatherOut(arena, ix, lb, lidx, ridx, &.{}, n);
        }
    }

    /// The in-memory probe side as `probeStep` reads it.
    const ProbeSide = struct {
        join: *Join,
        fn next(ps: ProbeSide, arena: std.mem.Allocator) anyerror!?Batch {
            return ps.join.nextProbe(arena);
        }
    };

    fn nextProbe(self: *Join, arena: std.mem.Allocator) anyerror!?Batch {
        if (self.replay_pos < self.replay.len) {
            self.replay_pos += 1;
            return self.replay[self.replay_pos - 1];
        }
        if (self.probe_ended) return null;
        return self.probe.next(arena);
    }

    /// Reads the probe side ahead, kept in `state`, and sends its keys when it ended
    /// within the limits; past them it sends none and the build side reads in full.
    fn prefetch(self: *Join, arena: std.mem.Allocator) anyerror!void {
        self.prefetched = true;
        var held = std.array_list.Managed(Batch).init(self.state);
        var rows: usize = 0;
        var bytes: usize = 0;
        while (rows <= self.prefetch_rows and bytes <= self.prefetch_bytes) {
            const b = (try self.probe.next(arena)) orelse {
                self.probe_ended = true;
                break;
            };
            if (b.len == 0) continue;
            try held.append(try b.deepCopy(self.state));
            rows += b.len;
            bytes += b.heapBytes();
        }
        self.replay = held.items;
        if (self.probe_ended)
            try self.push_build.?.apply(self.push_build.?.ctx, try keyset.collect(self.state, self.replay, self.left_keys, self.push_cap));
    }

    /// The index, or null once the join has gone to grace mode.
    fn ensureIndex(self: *Join, arena: std.mem.Allocator) anyerror!?*JoinIndex {
        if (self.grace != null) return null;
        const ix = self.index orelse blk: {
            const made = (self.buildIndex(arena) catch |e| {
                if (self.err) |ec| {
                    if (self.null_aware and e == error.JoinBuildTooLarge)
                        notInTooLarge(ec, self.build_cap orelse op_mod.join_build_byte_cap)
                    else
                        ec.set("{s}", .{failLabel(e)});
                }
                return e;
            }) orelse return null;
            self.index = made;
            break :blk made;
        };
        if ((self.kind == .right or self.kind == .full) and self.matched == null and self.shared_matched == null) {
            const m = try self.state.alloc(bool, ix.build_batch.len);
            @memset(m, false);
            self.matched = m;
        }
        return ix;
    }

    /// Frees a spilled join left unfinished by a consumer that stopped pulling: the
    /// partitions not yet joined are deleted, their disk charge given back, and the
    /// current partition's index and mappings freed.
    pub fn release(self: *Join) void {
        if (self.grace) |g| g.deinit();
        self.pending = null;
        for ([_]*?std.heap.ArenaAllocator{ &self.probe_mem, &self.batch_mem, &self.pair_mem }) |m| {
            if (m.*) |*a| a.deinit();
            m.* = null;
        }
    }

    /// Whether this join may spill: never a CROSS join, which has no key to split
    /// on, nor NOT IN, where one null key anywhere on the build side changes every
    /// probe row's answer.
    fn mayGrace(self: *const Join) bool {
        return self.space != null and !self.null_aware and self.kind != .cross and self.right_keys.len > 0;
    }

    fn partCap(self: *const Join) usize {
        return @max(self.spill_at, self.build_cap orelse op_mod.join_build_byte_cap);
    }

    /// The in-memory index, or null after switching to grace mode. A join that may
    /// spill pulls its build side into a scratch arena, so what it held is freed
    /// once indexed or written out.
    fn buildIndex(self: *Join, arena: std.mem.Allocator) anyerror!?*JoinIndex {
        const build = self.build orelse return error.JoinHasNoBuildSide;
        const cap = self.build_cap orelse op_mod.join_build_byte_cap;
        if (!self.mayGrace()) return try JoinIndex.create(self.state, arena, build, self.right_schema, self.right_keys, cap, self.threads);
        var pull = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer pull.deinit();
        var held = std.array_list.Managed(Batch).init(pull.allocator());
        var bytes: usize = 0;
        var rows: usize = 0;
        while (try build.next(pull.allocator())) |b| {
            if (b.len == 0) continue;
            try held.append(b);
            bytes += b.heapBytes();
            rows += b.len;
            if (bytes > self.spill_at) break;
        } else if (bytes + indexOverhead(rows, self.right_keys.len) <= self.spill_at and rows < std.math.maxInt(u32)) {
            return try JoinIndex.fromBatches(self.state, held.items, self.right_schema, self.right_keys, bytes, std.math.maxInt(usize), self.threads);
        }
        try self.startGrace(&pull, held.items, build);
        return null;
    }

    /// Right and full joins emit the build rows no probe row matches.
    fn emitsBuild(self: *const Join) bool {
        return self.kind == .right or self.kind == .full;
    }

    /// Left, full and anti joins emit the probe rows no build row matches.
    fn emitsProbe(self: *const Join) bool {
        return self.kind == .left or self.kind == .full or self.kind == .anti;
    }

    /// Splits the build side, `held` and then the rest of `build`, into the depth-0
    /// partitions, pushed on the stack in reverse so partition 0 is joined first.
    /// A null-key build row matches nothing: dropped, or for a right or full join
    /// kept apart in a file streamed out last.
    fn startGrace(self: *Join, pull: *std.heap.ArenaAllocator, held: []const Batch, build: Op) anyerror!void {
        const g = try self.state.create(Grace);
        g.* = .{ .files = std.heap.ArenaAllocator.init(std.heap.page_allocator) };
        errdefer g.files.deinit();
        const fa = g.files.allocator();
        g.build_mixed = try mixedKeys(fa, self.right_schema, self.right_keys, self.left_schema, self.left_keys);
        g.probe_mixed = try mixedKeys(fa, self.left_schema, self.left_keys, self.right_schema, self.right_keys);
        var sp = Split.init(self.space.?, self.right_schema, self.right_keys, g.build_mixed, 0, if (self.emitsBuild()) grace_parts else drop_row, "join-build");
        defer sp.deinit();
        var scratch = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer scratch.deinit();
        for (held) |b| {
            try sp.add(scratch.allocator(), b);
            _ = scratch.reset(.retain_capacity);
        }
        _ = pull.reset(.free_all);
        while (try build.next(scratch.allocator())) |b| {
            try sp.add(scratch.allocator(), b);
            _ = scratch.reset(.retain_capacity);
        }
        try g.pending.ensureUnusedCapacity(fa, grace_parts + 1);
        const runs = try sp.finish(fa);
        if (runs[grace_parts]) |r| g.pending.appendAssumeCapacity(.{ .build = r });
        g.first = g.pending.items.len;
        var i: usize = grace_parts;
        while (i > 0) {
            i -= 1;
            g.pending.appendAssumeCapacity(.{ .build = runs[i] });
        }
        self.grace = g;
        self.probe_pushed = true;
    }

    /// Grace mode: the probe side split into the depth-0 partitions first, then
    /// each partition joined in turn from its build file's index, right and full
    /// joins draining its unmatched build rows before the next one opens.
    fn nextGrace(self: *Join, arena: std.mem.Allocator) anyerror!?Batch {
        const g = self.grace.?;
        if (g.done) return null;
        errdefer g.deinit();
        if (!g.probed) try self.splitProbe(g);
        while (true) {
            if (g.index) |ix| {
                if (!self.probe_done) {
                    if (g.reader) |*r| if (try self.probeStep(arena, ix, r)) |b| return b;
                    self.probe_done = true;
                }
                if (try self.drain(arena, ix)) |b| return b;
                g.closePart();
                self.matched = null;
                continue;
            }
            if (g.lone) |*r| {
                if (try r.next(arena)) |b| {
                    if (b.len > 0) return try self.loneOut(arena, b);
                    continue;
                }
                g.closePart();
                continue;
            }
            if (!try self.nextPart(g)) break;
        }
        g.deinit();
        return null;
    }

    /// Splits the probe side into the depth-0 partitions, each paired with its
    /// build file. A null-key probe row goes to partition 0 if its join kind emits
    /// it, and is dropped otherwise.
    fn splitProbe(self: *Join, g: *Grace) anyerror!void {
        var sp = Split.init(self.space.?, self.left_schema, self.left_keys, g.probe_mixed, 0, if (self.emitsProbe()) 0 else drop_row, "join-probe");
        defer sp.deinit();
        var scratch = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer scratch.deinit();
        while (try self.nextProbe(scratch.allocator())) |b| {
            try sp.add(scratch.allocator(), b);
            _ = scratch.reset(.retain_capacity);
        }
        const runs = try sp.finish(g.files.allocator());
        for (runs[0..grace_parts], 0..) |r, i| g.pending.items[g.first + grace_parts - 1 - i].probe = r;
        g.probed = true;
    }

    /// Pops the next partition and readies it: indexed from its build file, its
    /// build rows streamed out as they are when it has no probe file and the join
    /// emits them, split one depth deeper when its build side passes `spill_at` and
    /// a split can spread it, or dropped when it cannot add a row. False once the
    /// stack is empty.
    fn nextPart(self: *Join, g: *Grace) anyerror!bool {
        g.cur = g.pending.pop() orelse return false;
        const build = g.cur.build orelse {
            if (g.cur.probe != null and self.emitsProbe()) try self.openPart(g, .fits) else g.closePart();
            return true;
        };
        if (g.cur.probe == null) {
            if (self.emitsBuild()) g.lone = try spill.Reader.open(build) else g.closePart();
            return true;
        }
        var fit: Fit = .fits;
        const rows: usize = @intCast(build.rows);
        const bytes: usize = @intCast(build.bytes);
        if (bytes + indexOverhead(rows, self.right_keys.len) > self.spill_at) {
            if (try self.oneKey(g, build)) {
                fit = .one_key;
            } else if (g.cur.depth + 1 >= self.max_spill_depth) {
                fit = .too_deep;
            } else {
                try self.resplit(g, g.cur.depth + 1);
                return true;
            }
        }
        try self.openPart(g, fit);
        return true;
    }

    /// Whether every row of the build file `run` has the same key hash, which no
    /// split at any depth can spread.
    fn oneKey(self: *Join, g: *Grace, run: spill.Run) anyerror!bool {
        var r = try spill.Reader.open(run);
        defer r.close();
        var scratch = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer scratch.deinit();
        var first: ?u64 = null;
        while (try r.next(scratch.allocator())) |b| {
            const classes = try partClasses(scratch.allocator(), b, self.right_keys, g.build_mixed);
            for (0..b.len) |row| {
                if (anyNullKey(b.columns, self.right_keys, row)) return false;
                const h = hashRowKeys(b.columns, self.right_keys, classes, row);
                if (first) |f| {
                    if (f != h) return false;
                } else first = h;
            }
            _ = scratch.reset(.retain_capacity);
        }
        return true;
    }

    /// Splits the current partition at `depth`, deleting each of its files once
    /// split, and pushes the partitions that hold rows.
    fn resplit(self: *Join, g: *Grace, depth: u8) anyerror!void {
        g.deepest = @max(g.deepest, depth);
        const fa = g.files.allocator();
        try g.pending.ensureUnusedCapacity(fa, grace_parts);
        var bs = Split.init(self.space.?, self.right_schema, self.right_keys, g.build_mixed, depth, drop_row, "join-build");
        defer bs.deinit();
        try bs.addRun(g.cur.build.?);
        const bruns = try bs.finish(fa);
        errdefer for (bruns) |r| if (r) |run| spill.discard(run);
        spill.discard(g.cur.build.?);
        g.cur.build = null;
        var pruns: [grace_parts + 1]?spill.Run = @splat(null);
        if (g.cur.probe) |pr| {
            var ps = Split.init(self.space.?, self.left_schema, self.left_keys, g.probe_mixed, depth, 0, "join-probe");
            defer ps.deinit();
            try ps.addRun(pr);
            pruns = try ps.finish(fa);
            spill.discard(pr);
            g.cur.probe = null;
        }
        var i: usize = grace_parts;
        while (i > 0) {
            i -= 1;
            if (bruns[i] == null and pruns[i] == null) continue;
            g.pending.appendAssumeCapacity(.{ .build = bruns[i], .probe = pruns[i], .depth = depth });
        }
    }

    /// The current partition's index, read from its build file (deleted once read;
    /// none is an empty index), and its probe file opened.
    fn openPart(self: *Join, g: *Grace, fit: Fit) anyerror!void {
        g.part_mem = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        const pa = g.part_mem.?.allocator();
        const ix = try self.indexPart(pa, g.cur.build, fit);
        if (g.cur.build) |run| spill.discard(run);
        g.cur.build = null;
        if (self.emitsBuild()) {
            const m = try pa.alloc(bool, ix.build_batch.len);
            @memset(m, false);
            self.matched = m;
        }
        self.drain_pos = 0;
        self.probe_done = false;
        if (g.cur.probe) |pr| g.reader = try spill.Reader.open(pr);
        g.index = ix;
    }

    /// `run`'s rows indexed in `pa`, under `partCap`; past it the error says why no
    /// split could help.
    fn indexPart(self: *Join, pa: std.mem.Allocator, run_opt: ?spill.Run, fit: Fit) anyerror!*JoinIndex {
        const run = run_opt orelse return JoinIndex.fromBatches(pa, &.{}, self.right_schema, self.right_keys, 0, std.math.maxInt(usize), self.threads);
        var r = try spill.Reader.open(run);
        defer r.close();
        var held = std.array_list.Managed(Batch).init(pa);
        var bytes: usize = 0;
        while (try r.next(pa)) |b| {
            try held.append(b);
            bytes += b.heapBytes();
        }
        const cap = self.partCap();
        return JoinIndex.fromBatches(pa, held.items, self.right_schema, self.right_keys, bytes, cap, self.threads) catch |e| {
            if (e != error.JoinBuildTooLarge) return e;
            const ec = self.err orelse return if (fit == .too_deep) error.SpillTooDeep else e;
            switch (fit) {
                .one_key => ec.set("join build side too large even spilled: a single join key holds {d} rows, more than --op-memory allows (the limit is {d} bytes) — raise --op-memory or the join's max_build with WITH (max_build = '8GB'), or filter that key out", .{ run.rows, cap }),
                .too_deep => ec.set("join build side still too large after {d} levels of spilling: a partition holds {d} rows over the {d}-byte limit — raise --op-memory or the join's max_build with WITH (max_build = '8GB'), or filter the CTE", .{ self.max_spill_depth, run.rows, cap }),
                .fits => ec.set("join build side too large even spilled: a partition holds {d} rows over the {d}-byte limit — raise it with WITH (max_build = '8GB') on the join, filter the CTE, or flip the join", .{ run.rows, cap }),
            }
            return if (fit == .too_deep) error.SpillTooDeep else e;
        };
    }

    /// Build rows no probe row can match, as a right or full join emits them: the
    /// left side null.
    fn loneOut(self: *Join, arena: std.mem.Allocator, b: Batch) anyerror!Batch {
        const nleft = self.left_schema.fields.len;
        const cols = try arena.alloc(column.Column, nleft + b.columns.len);
        for (self.left_schema.fields, 0..) |f, i| cols[i] = try nullColumn(arena, f.ty, b.len);
        const copy = try b.deepCopy(arena);
        @memcpy(cols[nleft..], copy.columns);
        return .{ .schema = self.out_schema, .columns = cols, .len = b.len };
    }

    /// The probe batch being joined and where the join stands in it, kept across
    /// calls so a key that multiplies rows comes out `out_chunk` rows at a time: the
    /// row, whether its chain was looked up, the next build row of that chain and,
    /// with a residual, the rows' matches, the semi/anti window and the pair buffer
    /// (in `pair_mem`). The batch and these live in `probe_mem` until it is done.
    const Pending = struct {
        lb: Batch,
        probe: Probe,
        nulls: []bool,
        empty: bool,
        r: usize = 0,
        started: bool = false,
        cur: ?usize = null,
        hit: bool = false,
        hits: []bool = &.{},
        win: usize = first_win,
        in_buf: usize = 0,
        fin: usize = 0,
        cl: std.ArrayListUnmanaged(usize) = .empty,
        cr: std.ArrayListUnmanaged(usize) = .empty,
    };

    const out_chunk = 1 << 16;
    const pair_chunk = 1 << 16;
    const first_win = 1024;

    /// The next output of the probe side: up to `out_chunk` rows of the pending
    /// batch, a new probe batch read (into `probe_mem`) when none is pending. Null at
    /// the end of the probe side. Index lists live in `batch_mem`, reset per call, and
    /// only the gathered output in `out`. Under NOT IN a null on either side against a
    /// non-empty build is unknown and drops the row.
    fn probeStep(self: *Join, out: std.mem.Allocator, ix: *JoinIndex, read: anytype) anyerror!?Batch {
        while (true) {
            if (self.pending == null) {
                const pa = fresh(self.state, &self.probe_mem);
                const lb = (try read.next(pa)) orelse return null;
                if (lb.len == 0) continue;
                self.pending = try self.startPending(pa, ix, lb);
            }
            const b = if (self.residual != null) try self.stepResidual(out, ix) else try self.stepPlain(out, ix);
            if (b.len > 0) return b;
        }
    }

    fn startPending(self: *Join, pa: std.mem.Allocator, ix: *JoinIndex, lb: Batch) !Pending {
        const empty = ix.keys.len == 0 or ix.build_batch.len == 0;
        if (!self.probe_keyed) {
            self.probe_enc = try ix.probeEncoder(self.state, self.left_keys);
            self.probe_keyed = true;
        }
        const nulls = try pa.alloc(bool, lb.len);
        for (nulls, 0..) |*nl, i| nl.* = empty or anyNullKey(lb.columns, self.left_keys, i);
        const pr = try ix.probe(pa, lb, self.left_keys, self.probe_enc);
        var p = Pending{ .lb = lb, .probe = pr, .nulls = nulls, .empty = empty };
        if (self.residual != null) {
            p.hits = try pa.alloc(bool, lb.len);
            @memset(p.hits, false);
        }
        return p;
    }

    /// Row `r`'s chain looked up, the rows 8 ahead prefetched.
    fn startRow(_: *Join, ix: *JoinIndex, p: *Pending) void {
        const r = p.r;
        const ahead = 8;
        if (r + ahead < p.lb.len and !p.probe.miss[r + ahead]) ix.prefetch(p.probe.hashes[r + ahead]);
        p.cur = ix.first(p.probe, r);
        p.hit = p.cur != null;
        p.started = true;
        p.win = first_win;
        p.in_buf = 0;
    }

    fn nextRow(self: *Join, p: *Pending) void {
        _ = self;
        p.r += 1;
        p.started = false;
    }

    fn stepPlain(self: *Join, out: std.mem.Allocator, ix: *JoinIndex) anyerror!Batch {
        const p = &self.pending.?;
        const arena = fresh(self.state, &self.batch_mem);
        var lidx = std.array_list.Managed(usize).init(arena);
        var ridx = std.array_list.Managed(usize).init(arena);
        var rnull = std.array_list.Managed(bool).init(arena);
        const fill_right = (self.kind == .left or self.kind == .full);
        const lb = p.lb;
        while (p.r < lb.len) {
            if (!p.started) self.startRow(ix, p);
            const r = p.r;
            switch (self.kind) {
                .semi => if (p.hit) try lidx.append(r),
                .anti => {
                    const unknown = self.null_aware and !p.empty and (ix.has_null_key or p.nulls[r]);
                    if (!unknown and !p.hit) try lidx.append(r);
                },
                else => if (!p.hit) {
                    if (fill_right) {
                        try lidx.append(r);
                        try ridx.append(0);
                        try rnull.append(true);
                    }
                } else while (p.cur) |br| {
                    if (lidx.items.len >= out_chunk) return self.gatherOut(out, ix, lb, lidx.items, ridx.items, rnull.items, lidx.items.len);
                    try lidx.append(r);
                    try ridx.append(br);
                    if (fill_right) try rnull.append(false);
                    if (self.matched) |m| m[br] = true;
                    if (self.shared_matched) |sm| markShared(sm, br);
                    p.cur = ix.chainNext(br);
                },
            }
            self.nextRow(p);
        }
        self.pending = null;
        return self.gatherOut(out, ix, lb, lidx.items, ridx.items, rnull.items, lidx.items.len);
    }

    /// The plain step with a residual condition: key matches become candidate pairs,
    /// evaluated in buffers of up to `pair_chunk`, so a key with many build rows never
    /// holds every pair at once. Rows are finished in order as their last pairs are
    /// evaluated: a row whose candidates all fail is unmatched. A semi or anti join
    /// needs only a row's first match, so a row's chain is cut at a window that starts
    /// at `first_win` and doubles, and walked no further once it matched.
    fn stepResidual(self: *Join, out: std.mem.Allocator, ix: *JoinIndex) anyerror!Batch {
        const p = &self.pending.?;
        const arena = fresh(self.state, &self.batch_mem);
        var o = Outs{
            .lidx = std.array_list.Managed(usize).init(arena),
            .ridx = std.array_list.Managed(usize).init(arena),
            .rnull = std.array_list.Managed(bool).init(arena),
        };
        const short = self.kind == .semi or self.kind == .anti;
        const lb = p.lb;
        if (p.cl.capacity == 0) try self.beginPairs(p);
        while (p.r < lb.len) {
            if (!p.started) self.startRow(ix, p);
            const r = p.r;
            while (p.cur) |br| {
                if (p.cl.items.len >= pair_chunk or (short and p.in_buf >= p.win)) {
                    if (short and p.in_buf >= p.win) p.win *= 2;
                    try self.flushPairs(ix, p, &o);
                    p.in_buf = 0;
                    if (short and p.hits[r]) {
                        p.cur = null;
                        break;
                    }
                    if (o.lidx.items.len >= out_chunk) return self.gatherOut(out, ix, lb, o.lidx.items, o.ridx.items, o.rnull.items, o.lidx.items.len);
                }
                p.cl.appendAssumeCapacity(r);
                p.cr.appendAssumeCapacity(br);
                p.in_buf += 1;
                p.cur = ix.chainNext(br);
            }
            self.nextRow(p);
        }
        try self.flushPairs(ix, p, &o);
        while (p.fin < lb.len) : (p.fin += 1) try self.finishRow(p, &o, p.fin);
        self.pending = null;
        return self.gatherOut(out, ix, lb, o.lidx.items, o.ridx.items, o.rnull.items, o.lidx.items.len);
    }

    const Outs = struct {
        lidx: std.array_list.Managed(usize),
        ridx: std.array_list.Managed(usize),
        rnull: std.array_list.Managed(bool),
    };

    /// An empty buffer of `pair_chunk` pairs in a reset `pair_mem`.
    fn beginPairs(self: *Join, p: *Pending) !void {
        const pa = fresh(self.state, &self.pair_mem);
        p.cl = .empty;
        p.cr = .empty;
        try p.cl.ensureTotalCapacity(pa, pair_chunk);
        try p.cr.ensureTotalCapacity(pa, pair_chunk);
    }

    /// The buffered pairs evaluated, their matches recorded, and every row before
    /// the buffer's last finished; the buffer then starts over.
    fn flushPairs(self: *Join, ix: *JoinIndex, p: *Pending, o: *Outs) !void {
        const n = p.cl.items.len;
        if (n > 0) {
            const pa = self.pair_mem.?.allocator();
            const nl = p.lb.columns.len;
            const cols = try pa.alloc(column.Column, nl + ix.build_batch.columns.len);
            for (p.lb.columns, 0..) |c, i| cols[i] = try takeCol(pa, c, p.cl.items, &.{});
            for (ix.build_batch.columns, 0..) |c, k| cols[nl + k] = try takeCol(pa, c, p.cr.items, &.{});
            const pairs = Batch{ .schema = self.pair_schema.?, .columns = cols, .len = n };
            const mask = eval.evalColumn(pa, self.residual.?, pairs, types.Type.init(.bool)) catch |e| {
                if (self.err) |ec| ec.set("{s}: in the join's ON condition", .{failLabel(e)});
                return e;
            };
            const fill_right = (self.kind == .left or self.kind == .full);
            for (p.cl.items, p.cr.items, 0..) |r, br, i| {
                while (p.fin < r) : (p.fin += 1) try self.finishRow(p, o, p.fin);
                if (!(mask.validity.get(i) and mask.data.b[i])) continue;
                p.hits[r] = true;
                if (self.kind == .semi or self.kind == .anti) continue;
                try o.lidx.append(r);
                try o.ridx.append(br);
                if (fill_right) try o.rnull.append(false);
                if (self.matched) |m| m[br] = true;
                if (self.shared_matched) |sm| markShared(sm, br);
            }
        }
        try self.beginPairs(p);
    }

    /// Row `r`, its candidates all evaluated, as the join kind wants it.
    fn finishRow(self: *Join, p: *Pending, o: *Outs, r: usize) !void {
        const hit = p.hits[r];
        switch (self.kind) {
            .semi => if (hit) try o.lidx.append(r),
            .anti => if (!hit) try o.lidx.append(r),
            .left, .full => if (!hit) {
                try o.lidx.append(r);
                try o.ridx.append(0);
                try o.rnull.append(true);
            },
            else => {},
        }
    }

    /// right/full: after the probe ends, every unmatched build row with the left
    /// columns null, in chunks.
    fn drain(self: *Join, arena: std.mem.Allocator, ix: *JoinIndex) anyerror!?Batch {
        if (self.kind != .right and self.kind != .full) return null;
        var ridx = std.array_list.Managed(usize).init(arena);
        const matched = self.matched orelse return null;
        while (self.drain_pos < ix.build_batch.len and ridx.items.len < drain_chunk) : (self.drain_pos += 1) {
            if (!matched[self.drain_pos]) try ridx.append(self.drain_pos);
        }
        if (ridx.items.len == 0) return null;
        return try self.gatherOut(arena, ix, null, &.{}, ridx.items, &.{}, ridx.items.len);
    }

    /// Assemble `n` output rows with one gather per column. `lb == null` marks the
    /// outer drain, where the whole left side is null.
    fn gatherOut(
        self: *Join,
        arena: std.mem.Allocator,
        ix: *JoinIndex,
        lb: ?Batch,
        lidx: []const usize,
        ridx: []const usize,
        rnull: []const bool,
        n: usize,
    ) anyerror!Batch {
        const emit_right = (self.kind != .semi and self.kind != .anti);
        const nleft = if (lb) |b| b.columns.len else self.left_schema.fields.len;
        const nout = nleft + (if (emit_right) ix.build_batch.columns.len else @as(usize, 0));
        const cols = try arena.alloc(column.Column, nout);

        var i: usize = 0;
        while (i < nleft) : (i += 1) {
            cols[i] = if (lb) |b|
                try takeCol(arena, b.columns[i], lidx, &.{})
            else
                try nullColumn(arena, self.left_schema.fields[i].ty, n);
        }
        if (emit_right) {
            var k: usize = 0;
            while (k < ix.build_batch.columns.len) : (k += 1) {
                cols[nleft + k] = try takeCol(arena, ix.build_batch.columns[k], ridx, rnull);
            }
        }
        return Batch{ .schema = self.out_schema, .columns = cols, .len = n };
    }
};

/// `slot`'s arena, made over `state` on first use and reset for this use.
fn fresh(state: std.mem.Allocator, slot: *?std.heap.ArenaAllocator) std.mem.Allocator {
    if (slot.* == null) slot.* = std.heap.ArenaAllocator.init(state);
    _ = slot.*.?.reset(.retain_capacity);
    return slot.*.?.allocator();
}

/// Gather rows `idx` of `c`, then null out the `null_mask` positions (the outer-join
/// fill). Only a source with no rows is built as an all-null column instead.
pub fn takeCol(arena: std.mem.Allocator, c: column.Column, idx: []const usize, null_mask: []const bool) !column.Column {
    if (c.len == 0) return nullColumn(arena, c.ty, idx.len);
    var out = try column.permute(arena, c, idx);
    if (null_mask.len == idx.len) {
        out.ty = out.ty.asNullable();
        for (null_mask, 0..) |is_null, i| {
            if (is_null) out.validity.setValid(i, false);
        }
    }
    return out;
}

pub fn nullColumn(arena: std.mem.Allocator, ty: types.Type, n: usize) !column.Column {
    var b = try column.Builder.initCapacity(arena, ty.asNullable(), n);
    var i: usize = 0;
    while (i < n) : (i += 1) try b.append(.null);
    return b.finish();
}

/// Sets build row `row`'s bit in `shared_matched`, the match bitset of a right or
/// full join whose index several lanes probe: such a join drains nothing itself,
/// its unmatched rows are drained once by whoever ran the lanes. The load first
/// keeps a hot build row's word a shared read once its bit is set.
fn markShared(words: []std.atomic.Value(u64), row: usize) void {
    const w = &words[row >> 6];
    const bit = @as(u64, 1) << @intCast(row & 63);
    if (w.load(.monotonic) & bit == 0) _ = w.fetchOr(bit, .monotonic);
}

test "join: inner/left/semi/anti; null keys never match, duplicate build keys fan out" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const Case = struct { kind: ast.JoinKind, keys: []const ?i64, rvs: []const ?[]const u8 };
    const cases = [_]Case{
        .{ .kind = .inner, .keys = &.{ 1, 1 }, .rvs = &.{ "x", "y" } },
        .{ .kind = .left, .keys = &.{ 1, 1, 2, null, 3 }, .rvs = &.{ "x", "y", null, null, null } },
        .{ .kind = .semi, .keys = &.{1}, .rvs = &.{} },
        .{ .kind = .anti, .keys = &.{ 2, null, 3 }, .rvs = &.{} },
    };
    for (cases) |case| {
        const lb = [_]Batch{try kvBatch(a, &join_left_schema, &.{ 1, 2, null, 3 }, &.{ "a", "b", "n", "c" })};
        const rb = [_]Batch{try kvBatch(a, &join_right_schema, &.{ 1, 1, 4, null }, &.{ "x", "y", "z", "m" })};
        var lts = TestSource{ .schema_ = join_left_schema, .batches = &lb };
        var rts = TestSource{ .schema_ = join_right_schema, .batches = &rb };
        var lscan = Scan{ .src = lts.src() };
        var rscan = Scan{ .src = rts.src() };
        const emit_right = case.kind == .inner or case.kind == .left;
        var jn = Join{
            .probe = .{ .scan = &lscan },
            .build = .{ .scan = &rscan },
            .left_keys = &.{0},
            .right_keys = &.{0},
            .left_schema = &join_left_schema,
            .right_schema = &join_right_schema,
            .out_schema = if (emit_right) &join_both_schema else &join_left_schema,
            .kind = case.kind,
            .state = a,
        };
        const got = try JoinRows.collect(a, .{ .join = &jn }, if (emit_right) @as(?usize, 3) else null);
        try got.expect(case.keys, case.rvs);
    }
}

test "join: key pushdown sends one side's keys first and replays a read-ahead probe unchanged" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const Rec = struct {
        got: ?[]const keyset.KeyValues = null,
        calls: usize = 0,
        fn apply(ctx: *anyopaque, keys: []const keyset.KeyValues) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.got = keys;
            self.calls += 1;
        }
    };
    for ([_]struct { rows: usize, sent: bool }{ .{ .rows = 100, .sent = true }, .{ .rows = 1, .sent = false } }) |case| {
        const lb = [_]Batch{
            try kvBatch(a, &join_left_schema, &.{ 1, 2 }, &.{ "a", "b" }),
            try kvBatch(a, &join_left_schema, &.{ null, 3, 1 }, &.{ "n", "c", "d" }),
        };
        const rb = [_]Batch{try kvBatch(a, &join_right_schema, &.{ 1, 4, null }, &.{ "x", "z", "m" })};
        var lts = TestSource{ .schema_ = join_left_schema, .batches = &lb };
        var rts = TestSource{ .schema_ = join_right_schema, .batches = &rb };
        var lscan = Scan{ .src = lts.src() };
        var rscan = Scan{ .src = rts.src() };
        var to_build = Rec{};
        var to_probe = Rec{};
        var jn = Join{
            .probe = .{ .scan = &lscan },
            .build = .{ .scan = &rscan },
            .left_keys = &.{0},
            .right_keys = &.{0},
            .left_schema = &join_left_schema,
            .right_schema = &join_right_schema,
            .out_schema = &join_both_schema,
            .kind = .left,
            .state = a,
            .push_build = .{ .ctx = &to_build, .apply = Rec.apply },
            .push_probe = .{ .ctx = &to_probe, .apply = Rec.apply },
            .prefetch_rows = case.rows,
        };
        const got = try JoinRows.collect(a, .{ .join = &jn }, 3);
        try got.expect(&.{ 1, 2, null, 3, 1 }, &.{ "x", null, null, null, "x" });
        try testing.expectEqual(@as(usize, if (case.sent) 1 else 0), to_build.calls);
        if (case.sent) try testing.expectEqual(@as(usize, 3), to_build.got.?[0].values.len);
        try testing.expectEqual(@as(usize, 1), to_probe.calls);
        try testing.expectEqual(@as(usize, 2), to_probe.got.?[0].values.len);
    }
}

test "join: a residual over long key chains, past one pair buffer and the semi/anti window, keeps every kind's rows in order" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const rv = try a.create(ast.Expr);
    rv.* = .{ .field = .{ .parts = &.{"rv"} } };
    const x = try a.create(ast.Expr);
    x.* = .{ .str_lit = "x" };
    const residual = try a.create(ast.Expr);
    residual.* = .{ .binary = .{ .op = .ne, .l = rv, .r = x } };

    var rk = std.array_list.Managed(?i64).init(a);
    var rvs = std.array_list.Managed(?[]const u8).init(a);
    for (0..70_000) |i| {
        try rk.append(1);
        try rvs.append(if (i % 9000 == 8999) "y" else "x");
    }
    for (0..3000) |i| {
        try rk.append(2);
        try rvs.append(if (i == 2999) "y" else "x");
    }
    for (0..10) |_| {
        try rk.append(4);
        try rvs.append("x");
    }
    try rk.append(null);
    try rvs.append("y");
    var prng = std.Random.DefaultPrng.init(0x7e5);
    const rnd = prng.random();
    var lk = std.array_list.Managed(?i64).init(a);
    var lv = std.array_list.Managed(?[]const u8).init(a);
    const pool = [_]?i64{ 1, 2, 3, 4, null };
    for (0..60) |i| {
        try lk.append(pool[rnd.uintLessThan(usize, pool.len)]);
        try lv.append(try std.fmt.allocPrint(a, "p{d}", .{i}));
    }
    var hits = [_]usize{0} ** 5;
    for (rk.items, rvs.items) |k, v| {
        if (k == null or std.mem.eql(u8, v.?, "x")) continue;
        hits[@intCast(k.?)] += 1;
    }
    for ([_]ast.JoinKind{ .inner, .left, .semi, .anti }) |kind| {
        var want = std.array_list.Managed(?[]const u8).init(a);
        for (lk.items, lv.items) |k, v| {
            const m: usize = if (k) |kk| hits[@intCast(kk)] else 0;
            const n: usize = switch (kind) {
                .inner => m,
                .left => @max(m, 1),
                .semi => @intFromBool(m > 0),
                else => @intFromBool(m == 0),
            };
            for (0..n) |_| try want.append(v);
        }
        const lb = [_]Batch{ try kvBatch(a, &join_left_schema, lk.items[0..25], lv.items[0..25]), try kvBatch(a, &join_left_schema, lk.items[25..], lv.items[25..]) };
        const rb = [_]Batch{try kvBatch(a, &join_right_schema, rk.items, rvs.items)};
        var lts = TestSource{ .schema_ = join_left_schema, .batches = &lb };
        var rts = TestSource{ .schema_ = join_right_schema, .batches = &rb };
        var lscan = Scan{ .src = lts.src() };
        var rscan = Scan{ .src = rts.src() };
        const emit_right = kind != .semi and kind != .anti;
        var jn = Join{
            .probe = .{ .scan = &lscan },
            .build = .{ .scan = &rscan },
            .left_keys = &.{0},
            .right_keys = &.{0},
            .left_schema = &join_left_schema,
            .right_schema = &join_right_schema,
            .out_schema = if (emit_right) &join_both_schema else &join_left_schema,
            .kind = kind,
            .state = a,
            .residual = residual,
            .pair_schema = &join_both_schema,
        };
        const got = try JoinRows.collect(a, .{ .join = &jn }, 1);
        try testing.expectEqual(want.items.len, got.rvs.items.len);
        for (want.items, got.rvs.items) |w, g| try testing.expectEqualStrings(w.?, g.?);
    }
}

test "join: a key that multiplies rows comes out in bounded batches, in order, with or without a residual" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const rv = try a.create(ast.Expr);
    rv.* = .{ .field = .{ .parts = &.{"rv"} } };
    const x = try a.create(ast.Expr);
    x.* = .{ .str_lit = "x" };
    const residual = try a.create(ast.Expr);
    residual.* = .{ .binary = .{ .op = .ne, .l = rv, .r = x } };
    var rk = std.array_list.Managed(?i64).init(a);
    var rvs = std.array_list.Managed(?[]const u8).init(a);
    for (0..70_000) |_| {
        try rk.append(1);
        try rvs.append("y");
    }
    for (0..10) |_| {
        try rk.append(2);
        try rvs.append("y");
    }
    const lk = [_]?i64{ 1, 2, 3, 1 };
    const lv = [_]?[]const u8{ "p0", "p1", "p2", "p3" };
    for ([_]ast.JoinKind{ .inner, .left, .right, .full }) |kind| for ([_]?*const ast.Expr{ null, residual }) |res| {
        const lb = [_]Batch{try kvBatch(a, &join_left_schema, &lk, &lv)};
        const rb = [_]Batch{try kvBatch(a, &join_right_schema, rk.items, rvs.items)};
        var lts = TestSource{ .schema_ = join_left_schema, .batches = &lb };
        var rts = TestSource{ .schema_ = join_right_schema, .batches = &rb };
        var lscan = Scan{ .src = lts.src() };
        var rscan = Scan{ .src = rts.src() };
        var jn = Join{
            .probe = .{ .scan = &lscan },
            .build = .{ .scan = &rscan },
            .left_keys = &.{0},
            .right_keys = &.{0},
            .left_schema = &join_left_schema,
            .right_schema = &join_right_schema,
            .out_schema = &join_both_schema,
            .kind = kind,
            .state = a,
            .residual = res,
            .pair_schema = if (res != null) &join_both_schema else null,
        };
        var got = std.array_list.Managed([]const u8).init(a);
        var batches: usize = 0;
        while (try jn.next(a)) |b| {
            try testing.expect(b.len <= 2 * Join.out_chunk);
            batches += 1;
            for (0..b.len) |r| {
                const v = b.columns[1].getValue(r);
                if (!v.isNull()) try got.append(v.string);
            }
        }
        try testing.expect(batches >= 3);
        var want = std.array_list.Managed([]const u8).init(a);
        for (0..70_000) |_| try want.append("p0");
        for (0..10) |_| try want.append("p1");
        if (kind == .left or kind == .full) try want.append("p2");
        for (0..70_000) |_| try want.append("p3");
        try testing.expectEqual(want.items.len, got.items.len);
        for (want.items, got.items) |w, g| try testing.expectEqualStrings(w, g);
    };
}

test "join: a residual ON condition decides which key matches count, per join kind" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const rv = try a.create(ast.Expr);
    rv.* = .{ .field = .{ .parts = &.{"rv"} } };
    const x = try a.create(ast.Expr);
    x.* = .{ .str_lit = "x" };
    const residual = try a.create(ast.Expr);
    residual.* = .{ .binary = .{ .op = .ne, .l = rv, .r = x } };

    const Case = struct { kind: ast.JoinKind, keys: []const ?i64, rvs: []const ?[]const u8 };
    const cases = [_]Case{
        .{ .kind = .inner, .keys = &.{1}, .rvs = &.{"y"} },
        .{ .kind = .left, .keys = &.{ 1, 2, null, 3 }, .rvs = &.{ "y", null, null, null } },
        .{ .kind = .semi, .keys = &.{1}, .rvs = &.{} },
        .{ .kind = .anti, .keys = &.{ 2, null, 3 }, .rvs = &.{} },
        .{ .kind = .right, .keys = &.{ 1, null, null, null }, .rvs = &.{ "y", "x", "z", "m" } },
        .{ .kind = .full, .keys = &.{ 1, 2, null, 3, null, null, null }, .rvs = &.{ "y", null, null, null, "x", "z", "m" } },
    };
    for (cases) |case| {
        const lb = [_]Batch{try kvBatch(a, &join_left_schema, &.{ 1, 2, null, 3 }, &.{ "a", "b", "n", "c" })};
        const rb = [_]Batch{try kvBatch(a, &join_right_schema, &.{ 1, 1, 4, null }, &.{ "x", "y", "z", "m" })};
        var lts = TestSource{ .schema_ = join_left_schema, .batches = &lb };
        var rts = TestSource{ .schema_ = join_right_schema, .batches = &rb };
        var lscan = Scan{ .src = lts.src() };
        var rscan = Scan{ .src = rts.src() };
        const emit_right = case.kind != .semi and case.kind != .anti;
        var jn = Join{
            .probe = .{ .scan = &lscan },
            .build = .{ .scan = &rscan },
            .left_keys = &.{0},
            .right_keys = &.{0},
            .left_schema = &join_left_schema,
            .right_schema = &join_right_schema,
            .out_schema = if (emit_right) &join_both_schema else &join_left_schema,
            .kind = case.kind,
            .state = a,
            .residual = residual,
            .pair_schema = &join_both_schema,
        };
        const got = try JoinRows.collect(a, .{ .join = &jn }, if (emit_right) @as(?usize, 3) else null);
        try got.expect(case.keys, case.rvs);
    }
}

test "join: a null-aware anti join is NOT IN — a NULL on either side is unknown" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const Case = struct { build: []const ?i64, keys: []const ?i64 };
    const cases = [_]Case{
        .{ .build = &.{ 1, 4, null }, .keys = &.{} },
        .{ .build = &.{ 1, 4 }, .keys = &.{ 2, 3 } },
        .{ .build = &.{}, .keys = &.{ 1, 2, null, 3 } },
    };
    for (cases) |case| {
        const lb = [_]Batch{try kvBatch(a, &join_left_schema, &.{ 1, 2, null, 3 }, &.{ "a", "b", "n", "c" })};
        const rvs = try a.alloc(?[]const u8, case.build.len);
        @memset(rvs, "r");
        const rb = [_]Batch{try kvBatch(a, &join_right_schema, case.build, rvs)};
        var lts = TestSource{ .schema_ = join_left_schema, .batches = &lb };
        var rts = TestSource{ .schema_ = join_right_schema, .batches = &rb };
        var lscan = Scan{ .src = lts.src() };
        var rscan = Scan{ .src = rts.src() };
        var jn = Join{
            .probe = .{ .scan = &lscan },
            .build = .{ .scan = &rscan },
            .left_keys = &.{0},
            .right_keys = &.{0},
            .left_schema = &join_left_schema,
            .right_schema = &join_right_schema,
            .out_schema = &join_left_schema,
            .kind = .anti,
            .null_aware = true,
            .state = a,
        };
        const got = try JoinRows.collect(a, .{ .join = &jn }, null);
        try got.expect(case.keys, &.{});
    }
}

test "join: right and full drain unmatched build rows with a null left side" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    for ([_]ast.JoinKind{ .right, .full }) |kind| {
        const lb = [_]Batch{try kvBatch(a, &join_left_schema, &.{ 1, 2, null }, &.{ "a", "b", "n" })};
        const rb = [_]Batch{try kvBatch(a, &join_right_schema, &.{ 1, 4, null }, &.{ "x", "z", "m" })};
        var lts = TestSource{ .schema_ = join_left_schema, .batches = &lb };
        var rts = TestSource{ .schema_ = join_right_schema, .batches = &rb };
        var lscan = Scan{ .src = lts.src() };
        var rscan = Scan{ .src = rts.src() };
        var jn = Join{
            .probe = .{ .scan = &lscan },
            .build = .{ .scan = &rscan },
            .left_keys = &.{0},
            .right_keys = &.{0},
            .left_schema = &join_left_schema,
            .right_schema = &join_right_schema,
            .out_schema = &join_both_schema,
            .kind = kind,
            .state = a,
        };
        const got = try JoinRows.collect(a, .{ .join = &jn }, 3);
        if (kind == .right) {
            try got.expect(&.{ 1, null, null }, &.{ "x", "z", "m" });
        } else {
            try got.expect(&.{ 1, 2, null, null, null }, &.{ "x", null, null, "z", "m" });
        }
    }
}

test "join: multi-key ON (int + string) pairs only fully equal keys" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const lb = [_]Batch{try kvBatch(a, &join_left_schema, &.{ 1, 1, 2, 3 }, &.{ "a", "b", "a", null })};
    const rb = [_]Batch{try kvBatch(a, &join_right_schema, &.{ 1, 2, 1 }, &.{ "a", "z", "a" })};
    var lts = TestSource{ .schema_ = join_left_schema, .batches = &lb };
    var rts = TestSource{ .schema_ = join_right_schema, .batches = &rb };
    var lscan = Scan{ .src = lts.src() };
    var rscan = Scan{ .src = rts.src() };
    var jn = Join{
        .probe = .{ .scan = &lscan },
        .build = .{ .scan = &rscan },
        .left_keys = &.{ 0, 1 },
        .right_keys = &.{ 0, 1 },
        .left_schema = &join_left_schema,
        .right_schema = &join_right_schema,
        .out_schema = &join_both_schema,
        .kind = .inner,
        .state = a,
    };
    const got = try JoinRows.collect(a, .{ .join = &jn }, 3);
    try got.expect(&.{ 1, 1 }, &.{ "a", "a" });
}

test "join: cross pairs every probe row with every build row" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const lb = [_]Batch{try kvBatch(a, &join_left_schema, &.{ 1, 2 }, &.{ "a", "b" })};
    const rb = [_]Batch{try kvBatch(a, &join_right_schema, &.{ 7, 8, 9 }, &.{ "x", "y", "z" })};
    var lts = TestSource{ .schema_ = join_left_schema, .batches = &lb };
    var rts = TestSource{ .schema_ = join_right_schema, .batches = &rb };
    var lscan = Scan{ .src = lts.src() };
    var rscan = Scan{ .src = rts.src() };
    var jn = Join{
        .probe = .{ .scan = &lscan },
        .build = .{ .scan = &rscan },
        .left_keys = &.{},
        .right_keys = &.{},
        .left_schema = &join_left_schema,
        .right_schema = &join_right_schema,
        .out_schema = &join_both_schema,
        .kind = .cross,
        .state = a,
    };
    const got = try JoinRows.collect(a, .{ .join = &jn }, 3);
    try got.expect(&.{ 1, 1, 1, 2, 2, 2 }, &.{ "x", "y", "z", "x", "y", "z" });
}

test "join: cross emits bounded batches and keeps its build cap" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const lb = [_]Batch{try kvBatch(a, &join_left_schema, &.{ 1, 2, 3 }, &.{ "a", "b", "c" })};
    const rb = [_]Batch{try kvBatch(a, &join_right_schema, &.{ 7, 8, 9 }, &.{ "x", "y", "z" })};
    var lts = TestSource{ .schema_ = join_left_schema, .batches = &lb };
    var rts = TestSource{ .schema_ = join_right_schema, .batches = &rb };
    var lscan = Scan{ .src = lts.src() };
    var rscan = Scan{ .src = rts.src() };
    var jn = Join{
        .probe = .{ .scan = &lscan },
        .build = .{ .scan = &rscan },
        .left_keys = &.{},
        .right_keys = &.{},
        .left_schema = &join_left_schema,
        .right_schema = &join_right_schema,
        .out_schema = &join_both_schema,
        .kind = .cross,
        .state = a,
        .cross_chunk = 4,
    };
    var keys = std.array_list.Managed(i64).init(a);
    var rvs = std.array_list.Managed(u8).init(a);
    while (try jn.next(a)) |b| {
        try testing.expect(b.len <= 4);
        for (0..b.len) |r| {
            try keys.append(b.columns[0].getValue(r).int);
            try rvs.appendSlice(b.columns[3].getValue(r).string);
        }
    }
    try testing.expectEqualSlices(i64, &.{ 1, 1, 1, 2, 2, 2, 3, 3, 3 }, keys.items);
    try testing.expectEqualStrings("xyzxyzxyz", rvs.items);

    var lts2 = TestSource{ .schema_ = join_left_schema, .batches = &lb };
    var rts2 = TestSource{ .schema_ = join_right_schema, .batches = &rb };
    var lscan2 = Scan{ .src = lts2.src() };
    var rscan2 = Scan{ .src = rts2.src() };
    var capped = Join{
        .probe = .{ .scan = &lscan2 },
        .build = .{ .scan = &rscan2 },
        .left_keys = &.{},
        .right_keys = &.{},
        .left_schema = &join_left_schema,
        .right_schema = &join_right_schema,
        .out_schema = &join_both_schema,
        .kind = .cross,
        .state = a,
        .build_cap = 8,
    };
    try testing.expectError(error.JoinBuildTooLarge, capped.next(a));
}

test "join: a cross join stopped early holds its probe batch in the plan arena, which frees it" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const lb = [_]Batch{try kvBatch(a, &join_left_schema, &.{ 1, 2, 3 }, &.{ "a", "b", "c" })};
    const rb = [_]Batch{try kvBatch(a, &join_right_schema, &.{ 7, 8, 9 }, &.{ "x", "y", "z" })};
    var lts = TestSource{ .schema_ = join_left_schema, .batches = &lb };
    var rts = TestSource{ .schema_ = join_right_schema, .batches = &rb };
    var lscan = Scan{ .src = lts.src() };
    var rscan = Scan{ .src = rts.src() };
    var jn = Join{
        .probe = .{ .scan = &lscan },
        .build = .{ .scan = &rscan },
        .left_keys = &.{},
        .right_keys = &.{},
        .left_schema = &join_left_schema,
        .right_schema = &join_right_schema,
        .out_schema = &join_both_schema,
        .kind = .cross,
        .state = a,
        .cross_chunk = 4,
    };
    try testing.expectEqual(@as(usize, 4), (try jn.next(a)).?.len);
    const child = jn.cross_mem.?.child_allocator;
    try testing.expectEqual(a.ptr, child.ptr);
    try testing.expectEqual(a.vtable, child.vtable);
}

test "join: the build-size guard reports instead of exhausting memory" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const rb = [_]Batch{try kvBatch(a, &join_right_schema, &.{ 1, 2, 3 }, &.{ "x", "y", "z" })};
    const lb = [_]Batch{try kvBatch(a, &join_left_schema, &.{1}, &.{"a"})};
    var lts = TestSource{ .schema_ = join_left_schema, .batches = &lb };
    var rts = TestSource{ .schema_ = join_right_schema, .batches = &rb };
    var lscan = Scan{ .src = lts.src() };
    var rscan = Scan{ .src = rts.src() };
    var ec = ErrCtx{};
    var jn = Join{
        .probe = .{ .scan = &lscan },
        .build = .{ .scan = &rscan },
        .left_keys = &.{0},
        .right_keys = &.{0},
        .left_schema = &join_left_schema,
        .right_schema = &join_right_schema,
        .out_schema = &join_both_schema,
        .kind = .inner,
        .state = a,
        .err = &ec,
    };
    const saved = op_mod.join_build_byte_cap;
    op_mod.join_build_byte_cap = 8;
    defer op_mod.join_build_byte_cap = saved;
    const top = Op{ .join = &jn };
    try testing.expectError(error.JoinBuildTooLarge, top.next(a));
    try testing.expect(std.mem.indexOf(u8, ec.msg, "exceeds its cap") != null);
}

const JoinSetup = struct {
    kind: ast.JoinKind,
    left: []const Batch,
    right: []const Batch,
    left_schema: *const types.Schema = &join_left_schema,
    right_schema: *const types.Schema = &join_right_schema,
    out_schema: ?*const types.Schema = null,
    residual: ?*const ast.Expr = null,
    null_aware: bool = false,
    space: ?Space = null,
    spill_at: usize = std.math.maxInt(usize),
    build_cap: ?usize = null,
    max_spill_depth: u8 = default_grace_depth,
    err: ?*ErrCtx = null,
    deepest: ?*u8 = null,
};

const JoinOutcome = struct { rows: []const []const u8, spilled: bool, deepest: u8 };

fn cellText(a: std.mem.Allocator, v: @import("../value.zig").Value) ![]const u8 {
    return switch (v) {
        .null => "~",
        .int => |x| try std.fmt.allocPrint(a, "{d}", .{x}),
        .string => |s| s,
        .decimal => |d| try std.fmt.allocPrint(a, "{d}e-{d}", .{ d.unscaled, d.scale }),
        else => "?",
    };
}

/// The join's rows as sorted `a|b|c` lines, whether it went to grace mode and the
/// deepest level it split to, also left in `s.deepest` when the join fails.
fn runJoin(a: std.mem.Allocator, s: JoinSetup) !JoinOutcome {
    var lts = TestSource{ .schema_ = s.left_schema.*, .batches = s.left };
    var rts = TestSource{ .schema_ = s.right_schema.*, .batches = s.right };
    var lscan = Scan{ .src = lts.src() };
    var rscan = Scan{ .src = rts.src() };
    const emit_right = s.kind != .semi and s.kind != .anti;
    var jn = Join{
        .probe = .{ .scan = &lscan },
        .build = .{ .scan = &rscan },
        .left_keys = &.{0},
        .right_keys = &.{0},
        .left_schema = s.left_schema,
        .right_schema = s.right_schema,
        .out_schema = s.out_schema orelse if (emit_right) &join_both_schema else s.left_schema,
        .kind = s.kind,
        .null_aware = s.null_aware,
        .state = a,
        .err = s.err,
        .residual = s.residual,
        .pair_schema = if (s.residual != null) &join_both_schema else null,
        .space = s.space,
        .spill_at = s.spill_at,
        .build_cap = s.build_cap,
        .max_spill_depth = s.max_spill_depth,
    };
    defer if (s.deepest) |d| {
        d.* = if (jn.grace) |g| g.deepest else 0;
    };
    var rows = std.array_list.Managed([]const u8).init(a);
    const top = Op{ .join = &jn };
    while (try top.next(a)) |b| {
        for (0..b.len) |r| {
            var line = std.array_list.Managed(u8).init(a);
            for (b.columns, 0..) |c, ci| {
                if (ci > 0) try line.append('|');
                try line.appendSlice(try cellText(a, c.getValue(r)));
            }
            try rows.append(line.items);
        }
    }
    std.mem.sort([]const u8, rows.items, {}, lessStr);
    return .{ .rows = rows.items, .spilled = jn.grace != null, .deepest = if (jn.grace) |g| g.deepest else 0 };
}

/// `n` rows in batches of `per`: key `(i * mul) % mod`, null every `null_every`th
/// row, value `prefix` and `i`, or "x" every third row.
fn genBatches(a: std.mem.Allocator, schema: *const types.Schema, n: usize, per: usize, mul: i64, mod: i64, null_every: usize, prefix: []const u8) ![]const Batch {
    var out = std.array_list.Managed(Batch).init(a);
    var i: usize = 0;
    while (i < n) {
        const m = @min(per, n - i);
        const keys = try a.alloc(?i64, m);
        const vals = try a.alloc(?[]const u8, m);
        for (keys, vals, 0..) |*k, *v, j| {
            const row = i + j;
            const ri: i64 = @intCast(row);
            k.* = if (row % null_every == 0) null else @mod(ri * mul, mod);
            v.* = if (row % 3 == 0) "x" else try std.fmt.allocPrint(a, "{s}{d}", .{ prefix, row });
        }
        try out.append(try kvBatch(a, schema, keys, vals));
        i += m;
    }
    return out.items;
}

test "join spill: every kind, with and without a residual, gives the in-memory rows" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const sd = try SpillDir.init(1 << 30);
    defer sd.deinit();

    const rv = try a.create(ast.Expr);
    rv.* = .{ .field = .{ .parts = &.{"rv"} } };
    const x = try a.create(ast.Expr);
    x.* = .{ .str_lit = "x" };
    const residual = try a.create(ast.Expr);
    residual.* = .{ .binary = .{ .op = .ne, .l = rv, .r = x } };

    const left = try genBatches(a, &join_left_schema, 300, 70, 1, 50, 17, "l");
    const right = try genBatches(a, &join_right_schema, 200, 45, 7, 60, 23, "r");
    const empty_one = [_]Batch{try kvBatch(a, &join_right_schema, &.{}, &.{})};

    const Sides = struct { left: []const Batch, right: []const Batch };
    const sides = [_]Sides{
        .{ .left = left, .right = right },
        .{ .left = left, .right = &.{} },
        .{ .left = left, .right = &empty_one },
        .{ .left = &.{}, .right = right },
    };
    const kinds = [_]ast.JoinKind{ .inner, .left, .right, .full, .semi, .anti };
    for (sides) |sd_case| for (kinds) |kind| for ([_]?*const ast.Expr{ null, residual }) |res| {
        const base = JoinSetup{ .kind = kind, .left = sd_case.left, .right = sd_case.right, .residual = res };
        const want = try runJoin(a, base);
        try testing.expect(!want.spilled);
        var spilled = base;
        spilled.space = sd.ds.space();
        spilled.spill_at = 1;
        const got = try runJoin(a, spilled);
        try testing.expect(got.spilled);
        try expectSameRows(want.rows, got.rows);
        try testing.expectEqual(@as(usize, 0), try sd.count());
    };
}

test "join spill: a build side under the threshold stays in memory" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const sd = try SpillDir.init(1 << 30);
    defer sd.deinit();
    const left = try genBatches(a, &join_left_schema, 50, 20, 1, 10, 7, "l");
    const right = try genBatches(a, &join_right_schema, 30, 10, 1, 10, 7, "r");
    const got = try runJoin(a, .{ .kind = .inner, .left = left, .right = right, .space = sd.ds.space(), .spill_at = 1 << 20 });
    try testing.expect(!got.spilled);
    try testing.expectEqual(@as(usize, 0), try sd.count());
}

test "join spill: a consumer that stops early leaves no file or charge once the tree is released" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const left = try genBatches(a, &join_left_schema, 300, 70, 1, 50, 17, "l");
    const right = try genBatches(a, &join_right_schema, 200, 45, 7, 60, 23, "r");
    for ([_]ast.JoinKind{ .inner, .full }) |kind| for ([_]usize{ 0, 1, 3, std.math.maxInt(usize) }) |pulls| {
        const sd = try SpillDir.init(1 << 30);
        defer sd.deinit();
        var lts = TestSource{ .schema_ = join_left_schema, .batches = left };
        var rts = TestSource{ .schema_ = join_right_schema, .batches = right };
        var lscan = Scan{ .src = lts.src() };
        var rscan = Scan{ .src = rts.src() };
        var jn = Join{
            .probe = .{ .scan = &lscan },
            .build = .{ .scan = &rscan },
            .left_keys = &.{0},
            .right_keys = &.{0},
            .left_schema = &join_left_schema,
            .right_schema = &join_right_schema,
            .out_schema = &join_both_schema,
            .kind = kind,
            .state = a,
            .space = sd.ds.space(),
            .spill_at = 1,
        };
        var lim = op_mod.Limit{ .child = .{ .join = &jn }, .remaining = std.math.maxInt(u64), .to_skip = 0 };
        const top = Op{ .limit = &lim };
        var n: usize = 0;
        while (n < pulls) : (n += 1) {
            if (try top.next(a) == null) break;
        }
        if (pulls == 1) try testing.expect(jn.grace != null and try sd.count() > 0);
        top.release();
        top.release();
        try testing.expectEqual(@as(usize, 0), try sd.count());
        try testing.expectEqual(@as(u64, 0), sd.ds.used.load(.monotonic));
        if (jn.grace != null) try testing.expect(try top.next(a) == null);
    };
}

const dec_right_schema = types.Schema{ .fields = &.{
    .{ .name = "rk", .ty = types.Type.decimal(18, 2).asNullable() },
    .{ .name = "rv", .ty = types.Type.init(.string).asNullable() },
} };

const dec_both_schema = types.Schema{ .fields = &.{
    .{ .name = "lk", .ty = types.Type.init(.int).asNullable() },
    .{ .name = "lv", .ty = types.Type.init(.string).asNullable() },
    .{ .name = "rk", .ty = types.Type.decimal(18, 2).asNullable() },
    .{ .name = "rv", .ty = types.Type.init(.string).asNullable() },
} };

fn decBatch(a: std.mem.Allocator, unscaled: []const ?i128, vals: []const ?[]const u8) !Batch {
    const cols = try a.alloc(column.Column, 2);
    var kb = column.Builder.init(a, dec_right_schema.fields[0].ty);
    for (unscaled) |u| try kb.append(if (u) |x| .{ .decimal = .{ .unscaled = x, .scale = 2 } } else .null);
    cols[0] = try kb.finish();
    var vb = column.Builder.init(a, types.Type.init(.string).asNullable());
    for (vals) |v| try vb.append(if (v) |s| .{ .string = s } else .null);
    cols[1] = try vb.finish();
    return .{ .schema = &dec_right_schema, .columns = cols, .len = unscaled.len };
}

test "join spill: decimal build keys and int probe keys that are equal land in the same partition" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const ints = [_]?i64{ 0, 1, 2, 3, 7, 42, 100, -5, 123456 };
    var unscaled: [ints.len]?i128 = undefined;
    var vals: [ints.len]?[]const u8 = undefined;
    for (ints, &unscaled, &vals) |i, *u, *v| {
        u.* = @as(i128, i.?) * 100;
        v.* = "d";
    }
    const db = try decBatch(a, &unscaled, &vals);
    const ib = try kvBatch(a, &join_left_schema, &ints, &vals);
    const dmixed = try mixedKeys(a, &dec_right_schema, &.{0}, &join_left_schema, &.{0});
    const imixed = try mixedKeys(a, &join_left_schema, &.{0}, &dec_right_schema, &.{0});
    try testing.expect(dmixed[0] and imixed[0]);
    const dcls = try partClasses(a, db, &.{0}, dmixed);
    const icls = try partClasses(a, ib, &.{0}, imixed);
    for (0..default_grace_depth) |depth| {
        var seen = [_]bool{false} ** grace_parts;
        for (0..ints.len) |r| {
            const p = partOf(ib.columns, &.{0}, icls, r, @intCast(depth), 0);
            try testing.expectEqual(p, partOf(db.columns, &.{0}, dcls, r, @intCast(depth), 0));
            seen[p] = true;
        }
        var used: usize = 0;
        for (seen) |s| used += @intFromBool(s);
        try testing.expect(used > 1);
    }

    const sd = try SpillDir.init(1 << 30);
    defer sd.deinit();
    const left = try genBatches(a, &join_left_schema, 200, 64, 1, 40, 11, "l");
    var rights = std.array_list.Managed(Batch).init(a);
    for (0..3) |bi| {
        var us: [20]?i128 = undefined;
        var vs: [20]?[]const u8 = undefined;
        for (&us, &vs, 0..) |*u, *v, j| {
            const k: i128 = @intCast(bi * 20 + j);
            u.* = if (j == 5) null else k * 100 + (if (j % 4 == 0) @as(i128, 50) else 0);
            v.* = try std.fmt.allocPrint(a, "r{d}", .{k});
        }
        try rights.append(try decBatch(a, &us, &vs));
    }
    for ([_]ast.JoinKind{ .inner, .full }) |kind| {
        const base = JoinSetup{ .kind = kind, .left = left, .right = rights.items, .right_schema = &dec_right_schema, .out_schema = &dec_both_schema };
        const want = try runJoin(a, base);
        var spilled = base;
        spilled.space = sd.ds.space();
        spilled.spill_at = 1;
        const got = try runJoin(a, spilled);
        try testing.expect(got.spilled);
        try testing.expect(got.deepest >= 1);
        try testing.expect(want.rows.len > 100);
        try expectSameRows(want.rows, got.rows);
        try testing.expectEqual(@as(usize, 0), try sd.count());
    }
}

test "join spill: NOT IN never spills and still fails past the build cap" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const sd = try SpillDir.init(1 << 30);
    defer sd.deinit();
    const left = try genBatches(a, &join_left_schema, 40, 20, 1, 10, 7, "l");
    const right = try genBatches(a, &join_right_schema, 40, 20, 1, 10, 1000, "r");

    const small = try runJoin(a, .{ .kind = .anti, .null_aware = true, .left = left, .right = right, .space = sd.ds.space(), .spill_at = 1 });
    try testing.expect(!small.spilled);

    var ec = ErrCtx{};
    const saved = op_mod.join_build_byte_cap;
    op_mod.join_build_byte_cap = 8;
    defer op_mod.join_build_byte_cap = saved;
    try testing.expectError(error.JoinBuildTooLarge, runJoin(a, .{ .kind = .anti, .null_aware = true, .left = left, .right = right, .space = sd.ds.space(), .spill_at = 1, .err = &ec }));
    try testing.expect(std.mem.indexOf(u8, ec.msg, "a NOT IN build side never spills") != null);
    try testing.expectEqual(@as(usize, 0), try sd.count());
}

test "join spill: one key past the cap fails at once, naming the single key, and leaves no files" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const sd = try SpillDir.init(1 << 30);
    defer sd.deinit();
    const left = try genBatches(a, &join_left_schema, 40, 20, 1, 10, 7, "l");
    const right = try genBatches(a, &join_right_schema, 400, 50, 0, 1, 1000, "r");
    for ([_]ast.JoinKind{ .inner, .right }) |kind| {
        var ec = ErrCtx{};
        var deepest: u8 = 99;
        try testing.expectError(error.JoinBuildTooLarge, runJoin(a, .{ .kind = kind, .left = left, .right = right, .space = sd.ds.space(), .spill_at = 1, .build_cap = 64, .err = &ec, .deepest = &deepest }));
        try testing.expect(std.mem.indexOf(u8, ec.msg, "a single join key holds 399 rows") != null);
        try testing.expect(std.mem.indexOf(u8, ec.msg, "--op-memory") != null);
        try testing.expectEqual(@as(u8, 0), deepest);
        try testing.expectEqual(@as(usize, 0), try sd.count());
    }

    var mixed = std.array_list.Managed(Batch).init(a);
    try mixed.appendSlice(right);
    try mixed.appendSlice(try genBatches(a, &join_right_schema, 300, 50, 1, 300, 1000, "s"));
    var ec = ErrCtx{};
    var deepest: u8 = 99;
    try testing.expectError(error.JoinBuildTooLarge, runJoin(a, .{ .kind = .full, .left = left, .right = mixed.items, .space = sd.ds.space(), .spill_at = 1, .build_cap = 2048, .max_spill_depth = 8, .err = &ec, .deepest = &deepest }));
    try testing.expect(std.mem.indexOf(u8, ec.msg, "a single join key holds 399 rows") != null);
    try testing.expect(deepest < 4);
    try testing.expectEqual(@as(usize, 0), try sd.count());

    const fits = try runJoin(a, .{ .kind = .inner, .left = left, .right = right, .space = sd.ds.space(), .spill_at = 1 });
    try testing.expectEqual(@as(u8, 0), fits.deepest);
    try expectSameRows((try runJoin(a, .{ .kind = .inner, .left = left, .right = right })).rows, fits.rows);
    try testing.expectEqual(@as(usize, 0), try sd.count());
}

test "join spill: partitions past spill_at split again, two levels and more, for every kind" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const sd = try SpillDir.init(1 << 30);
    defer sd.deinit();

    const rv = try a.create(ast.Expr);
    rv.* = .{ .field = .{ .parts = &.{"rv"} } };
    const x = try a.create(ast.Expr);
    x.* = .{ .str_lit = "x" };
    const residual = try a.create(ast.Expr);
    residual.* = .{ .binary = .{ .op = .ne, .l = rv, .r = x } };

    const left = try genBatches(a, &join_left_schema, 3000, 256, 7, 2500, 31, "l");
    const right = try genBatches(a, &join_right_schema, 4000, 300, 3, 1500, 41, "r");
    const kinds = [_]ast.JoinKind{ .inner, .left, .right, .full, .semi, .anti };
    for (kinds) |kind| for ([_]?*const ast.Expr{ null, residual }) |res| {
        const base = JoinSetup{ .kind = kind, .left = left, .right = right, .residual = res };
        const want = try runJoin(a, base);
        try testing.expect(!want.spilled);
        try testing.expect(want.rows.len > 100);
        for ([_]usize{ 1, 600, 3000 }) |at| {
            var spilled = base;
            spilled.space = sd.ds.space();
            spilled.spill_at = at;
            const got = try runJoin(a, spilled);
            try testing.expect(got.spilled);
            try testing.expect(got.deepest >= if (at == 1) @as(u8, 2) else 1);
            try expectSameRows(want.rows, got.rows);
            try testing.expectEqual(@as(usize, 0), try sd.count());
            try testing.expectEqual(@as(u64, 0), sd.ds.used.load(.monotonic));
        }
    };
}

test "join spill: the disk holds about one copy of the data while partitions split again" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const left = try genBatches(a, &join_left_schema, 3000, 256, 7, 2500, 31, "l");
    const right = try genBatches(a, &join_right_schema, 4000, 300, 3, 1500, 41, "r");

    const flat = try SpillDir.init(1 << 30);
    defer flat.deinit();
    const one = try runJoin(a, .{ .kind = .full, .left = left, .right = right, .space = flat.ds.space(), .spill_at = 1, .max_spill_depth = 1 });
    try testing.expectEqual(@as(u8, 0), one.deepest);

    const deep = try SpillDir.init(1 << 30);
    defer deep.deinit();
    const got = try runJoin(a, .{ .kind = .full, .left = left, .right = right, .space = deep.ds.space(), .spill_at = 1 });
    try testing.expect(got.deepest >= 2);
    try expectSameRows(one.rows, got.rows);
    const once = flat.ds.peak.load(.monotonic);
    try testing.expect(deep.ds.peak.load(.monotonic) < once + once / 2);
}

test "join spill: decimal build keys meet equal int probe keys at every depth" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const sd = try SpillDir.init(1 << 30);
    defer sd.deinit();
    const left = try genBatches(a, &join_left_schema, 3000, 256, 1, 2200, 37, "l");
    var rights = std.array_list.Managed(Batch).init(a);
    var k: usize = 0;
    while (k < 2000) : (k += 100) {
        var us: [100]?i128 = undefined;
        var vs: [100]?[]const u8 = undefined;
        for (&us, &vs, 0..) |*u, *v, j| {
            const key: i128 = @intCast(k + j);
            u.* = if (j == 13) null else key * 100 + (if (j % 5 == 0) @as(i128, 50) else 0);
            v.* = try std.fmt.allocPrint(a, "r{d}", .{key});
        }
        try rights.append(try decBatch(a, &us, &vs));
    }
    for ([_]ast.JoinKind{ .inner, .left, .right, .full, .semi, .anti }) |kind| {
        const emit_right = kind != .semi and kind != .anti;
        const base = JoinSetup{ .kind = kind, .left = left, .right = rights.items, .right_schema = &dec_right_schema, .out_schema = if (emit_right) &dec_both_schema else &join_left_schema };
        const want = try runJoin(a, base);
        var spilled = base;
        spilled.space = sd.ds.space();
        spilled.spill_at = 1;
        const got = try runJoin(a, spilled);
        try testing.expect(got.deepest >= 2);
        try testing.expect(want.rows.len > 100);
        try expectSameRows(want.rows, got.rows);
        try testing.expectEqual(@as(usize, 0), try sd.count());
    }
}

test "join spill: a partition still past the cap at the depth limit fails with SpillTooDeep and leaves no files" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const sd = try SpillDir.init(1 << 30);
    defer sd.deinit();
    const left = try genBatches(a, &join_left_schema, 500, 100, 1, 500, 1000, "l");
    const right = try genBatches(a, &join_right_schema, 2000, 200, 1, 500, 1000, "r");
    for ([_]u8{ 1, 2 }) |depth| {
        var ec = ErrCtx{};
        var deepest: u8 = 99;
        try testing.expectError(error.SpillTooDeep, runJoin(a, .{ .kind = .full, .left = left, .right = right, .space = sd.ds.space(), .spill_at = 1, .build_cap = 1024, .max_spill_depth = depth, .err = &ec, .deepest = &deepest }));
        try testing.expect(std.mem.indexOf(u8, ec.msg, "levels of spilling") != null);
        try testing.expectEqual(depth - 1, deepest);
        try testing.expectEqual(@as(usize, 0), try sd.count());
    }
    const want = try runJoin(a, .{ .kind = .full, .left = left, .right = right });
    const got = try runJoin(a, .{ .kind = .full, .left = left, .right = right, .space = sd.ds.space(), .spill_at = 1, .build_cap = 1024 });
    try expectSameRows(want.rows, got.rows);
    try testing.expectEqual(@as(usize, 0), try sd.count());
}

test "join spill: the disk cap stopping a join mid-split leaves no files" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const left = try genBatches(a, &join_left_schema, 1500, 256, 7, 1200, 31, "l");
    const right = try genBatches(a, &join_right_schema, 2000, 300, 3, 900, 41, "r");
    var deep_fail = false;
    var cap: u64 = 4096;
    while (cap < 1 << 20) : (cap += cap / 8) {
        const sd = try SpillDir.init(cap);
        defer sd.deinit();
        var deepest: u8 = 0;
        if (runJoin(a, .{ .kind = .right, .left = left, .right = right, .space = sd.ds.space(), .spill_at = 1, .deepest = &deepest })) |_| {} else |e| {
            try testing.expectEqual(error.SpillCapExceeded, e);
            if (deepest >= 1) deep_fail = true;
        }
        try testing.expectEqual(@as(usize, 0), try sd.count());
    }
    try testing.expect(deep_fail);
}

test "join spill: the disk cap stops a spilling join" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const sd = try SpillDir.init(4096);
    defer sd.deinit();
    const left = try genBatches(a, &join_left_schema, 40, 20, 1, 10, 7, "l");
    const right = try genBatches(a, &join_right_schema, 2000, 100, 1, 500, 1000, "r");
    try testing.expectError(error.SpillCapExceeded, runJoin(a, .{ .kind = .inner, .left = left, .right = right, .space = sd.ds.space(), .spill_at = 1 }));
    try testing.expectEqual(@as(usize, 0), try sd.count());
}
