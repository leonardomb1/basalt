//! The aggregate's input loop: batches folded into the group tables, partitioned
//! into parts for a parallel combine, and the group sets drained in the end.
//!
//! In spilling mode (`spill.zig`) both folds check the budget before each batch;
//! once frozen they look keys up instead of inserting them, fold the rows they
//! find, and send the rest to `Spill.route`, picked by the hash they already have.

const Aggregate = @import("../aggregate.zig").Aggregate;
const Batch = @import("../../batch.zig").Batch;
const Direct = Aggregate.Direct;
const FixedStore = Aggregate.FixedStore;
const GroupSet = Aggregate.GroupSet;
const GroupStore = Aggregate.GroupStore;
const GroupTable = Aggregate.GroupTable;
const KeyKind = Aggregate.KeyKind;
const Layout = Aggregate.Layout;
const Pairs = @import("pairs.zig").Pairs;
const Rows = @import("kernel.zig").Rows;
const inAggregate = @import("accumulate.zig").inAggregate;
const Value = @import("../../value.zig").Value;
const few_groups = Aggregate.few_groups;
const keyKindOf = Aggregate.keyKindOf;
const keyhash = @import("../../keyhash.zig");
const keytable = @import("../../keytable.zig");
const Encoder = keytable.Encoder;
const spill_parts = @import("../spill_parts.zig");
const std = @import("std");
const types = @import("../../../lang/types.zig");

pub fn next(self: *Aggregate, arena: std.mem.Allocator) anyerror!?Batch {
    if (self.space != null and self.by.len != 0 and self.part_state == null) return self.spillNext(arena);
    if (self.done) return null;
    self.done = true;
    const set = try self.drainSet();
    if (self.by.len != 0 and set.len == 0) return null;
    return try self.emitSets(arena, &.{set}, null);
}

/// Fold the whole child into raw per-group accumulators: the parallelizable half of
/// aggregation, merged across workers by `GroupMerge` and finalized by `emitSets`.
pub fn drainSet(self: *Aggregate) anyerror!GroupSet {
    std.debug.assert(self.part_state == null);
    return (try self.drainImpl())[0];
}

/// As `drainSet`, but one set per radix partition in its own `part_state`
/// allocator, so the merge takes partition `p` from each lane and frees it once folded.
pub fn drainParts(self: *Aggregate) anyerror![]GroupSet {
    std.debug.assert(self.part_state.?.len == fold_parts);
    return self.drainImpl();
}

pub const fold_parts = keytable.parts;

pub const partOf = keytable.partOf;

/// One partition's table and the store it indexes. A serial fold shares one store,
/// so groups keep first-seen order; a lane's partitions each own theirs, indexed by
/// a bounded table of `lane_table` entries (`GroupTable.initBounded`), so a lane
/// probes a table that stays cached and the merge combines the records it repeats.
pub fn FoldPart(comptime Store: type) type {
    return struct {
        table: GroupTable,
        store: *Store,
        alloc: std.mem.Allocator,
    };
}

pub const lane_table = 1 << 10;

/// The tables live in a real allocator, not the arena, so a growing table frees
/// what it outgrew.
pub fn foldParts(self: *Aggregate, comptime Store: type, layout: *const Layout) ![]FoldPart(Store) {
    const parts = try self.state.alloc(FoldPart(Store), fold_parts);
    const tg = self.table_gpa orelse self.gpa;
    var made: usize = 0;
    errdefer for (parts[0..made]) |*p| p.table.deinit();
    if (self.part_state) |ps| {
        for (parts, ps) |*p, a| {
            p.* = .{ .table = try GroupTable.initBounded(tg, lane_table, lane_table), .store = try newIn(a, Store.init(a, self.by.len, layout)), .alloc = a };
            made += 1;
        }
    } else {
        const a = self.state;
        const st = try newIn(a, Store.init(a, self.by.len, layout));
        for (parts) |*p| {
            p.* = .{ .table = try GroupTable.init(tg, 16), .store = st, .alloc = a };
            made += 1;
        }
    }
    return parts;
}

pub fn newIn(a: std.mem.Allocator, v: anytype) !*@TypeOf(v) {
    const p = try a.create(@TypeOf(v));
    p.* = v;
    return p;
}

pub fn foldSets(self: *Aggregate, comptime tag: std.meta.Tag(GroupSet.Store), parts: anytype) ![]GroupSet {
    const kinds = try self.keyKinds();
    const own = self.part_state != null;
    const out = try self.state.alloc(GroupSet, if (own) fold_parts else 1);
    for (out, parts[0..out.len], 0..) |*o, *p, pi| o.* = .{
        .store = @unionInit(GroupSet.Store, @tagName(tag), p.store),
        .len = p.store.len,
        .key_kinds = kinds,
        .table = if (own) &p.table else null,
        .strs = self.strs,
        .pairs = if (own) self.pairs else &.{},
        .part = @intCast(pi),
    };
    if (!own) for (parts) |*p| p.table.deinit();
    if (!own) try applyPairs(self, .{ .groups = &out[0] }, layoutOf(parts));
    return out;
}

fn layoutOf(parts: anytype) *const Layout {
    return parts[0].store.payload;
}

/// One `Pairs` per DISTINCT aggregate, partitioned as the fold's groups are.
fn initPairs(self: *Aggregate, grouped: bool) !void {
    var n: usize = 0;
    for (self.aggs) |a| n += @intFromBool(a.distinct);
    if (n == 0) return;
    const pairs = try self.state.alloc(Pairs, n);
    const allocs: []const std.mem.Allocator = self.part_state orelse &.{self.state};
    var i: usize = 0;
    for (self.aggs, 0..) |a, j| {
        if (!a.distinct) continue;
        pairs[i] = try Pairs.init(self.state, j, a.arg_ty, grouped, fold_parts, allocs, self.table_gpa orelse self.gpa, self.part_state != null, 0);
        i += 1;
    }
    self.pairs = pairs;
}

/// A serial fold's pairs into its states, partition by partition, their indexes
/// then freed; a lane's stay with its sets for the merge.
fn applyPairs(self: *Aggregate, into: Pairs.Into, layout: *const Layout) !void {
    for (self.pairs) |*pp| {
        for (0..pp.parts.len) |p| try pp.apply(p, into, layout, self.state, self.strs);
        pp.deinit();
    }
}

pub fn pairsOf(self: *Aggregate, j: usize) *Pairs {
    for (self.pairs) |*pp| if (pp.j == j) return pp;
    unreachable;
}

pub fn keyKinds(self: *Aggregate) ![]types.TypeKind {
    const kinds = try self.state.alloc(types.TypeKind, self.by.len);
    for (self.by, kinds) |ci, *k| k.* = self.in_schema.fields[ci].ty.kind;
    return kinds;
}

/// Each aggregate's argument over a batch whose rows know their group record
/// (`tails`, null for a row sent to a spill file) and partition (`pidx`), folded
/// one aggregate at a time by its kernel, straight from the operands where it has
/// `foldExprRows`.
fn foldRows(self: *Aggregate, pull: std.mem.Allocator, b: Batch, layout: *const Layout, tails: []const ?[*]u8, parts: anytype, pidx: []const u8) !void {
    var allocs: [fold_parts]std.mem.Allocator = undefined;
    const per_part = self.part_state != null;
    if (per_part) for (parts, 0..) |*p, i| {
        allocs[i] = p.alloc;
    };
    for (self.aggs, 0..) |agg, j| {
        const k = layout.kernels[j];
        if (agg.distinct) {
            try self.pairsOf(j).add(pull, try self.argColumn(pull, agg, agg.arg.?, b), b.len, .{ .tails = tails, .pidx = pidx, .layout = layout }, self.interner());
            continue;
        }
        var cx = layout.ctx(j, self.state);
        cx.scratch = pull;
        const rows = Rows{ .cx = cx, .allocs = if (per_part) &allocs else &.{}, .pidx = pidx };
        if (agg.arg) |e| if (k.foldExprRows) |fx| if (fx(tails, layout.offs[j], e, b, rows) catch |err| return inAggregate(self, err)) continue;
        const col = if (agg.arg) |e| try self.argColumn(pull, agg, e, b) else null;
        try k.foldRows(tails, layout.offs[j], col, rows);
    }
}

/// The fold's side of `keytable.probe`: each row's group record and partition noted,
/// a new group given its ordinal, and once frozen a row of an absent key marked for
/// its spill file.
fn Fold(comptime Store: type) type {
    return struct {
        self: *Aggregate,
        parts: []FoldPart(Store),
        layout: *const Layout,
        tails: []?[*]u8,
        pidx: []u8,
        dest: []u8,
        frozen: bool,
        direct: ?*Direct = null,
        nspill: usize = 0,

        pub inline fn table(c: *@This(), h: u64) *GroupTable {
            return &c.parts[partOf(h)].table;
        }

        pub inline fn store(c: *@This(), h: u64) *Store {
            return c.parts[partOf(h)].store;
        }

        pub inline fn early(c: *@This(), r: usize, key: anytype) keytable.Early {
            if (Store != FixedStore) return .probe;
            if (c.direct) |dr| if (dr.find(key)) |got| {
                c.tails[r] = got.tail;
                c.pidx[r] = got.part;
                return .done;
            };
            return if (c.frozen and c.dest[r] != spill_parts.keep) .miss else .probe;
        }

        pub inline fn hit(c: *@This(), r: usize, h: u64, key: anytype, x: keytable.Hit(Store)) !void {
            const pi: u8 = @intCast(partOf(h));
            if (!x.found) c.layout.setOrd(x.rec.tail, x.slot);
            c.tails[r] = x.rec.tail;
            c.pidx[r] = pi;
            if (Store == FixedStore and !c.frozen) if (c.direct) |dr| dr.note(key, x.rec.tail, pi);
        }

        pub inline fn miss(c: *@This(), r: usize, h: u64) !void {
            c.dest[r] = c.self.destOf(h);
            c.nspill += 1;
        }
    };
}

/// Fold with raw fixed-width keys. Floats group by number, not bits, as everywhere
/// else (-0.0 split from 0.0 before). Below `few_groups` rows go in arrival order;
/// past it a batch is walked a partition at a time so probes share a table.
pub fn drainFixed(self: *Aggregate, kinds: []const KeyKind, comptime counts_only: bool) anyerror![]GroupSet {
    var scratch = std.heap.ArenaAllocator.init(self.gpa);
    defer scratch.deinit();
    const pull = scratch.allocator();

    const layout = try newIn(self.state, try Layout.init(self.state, self.aggs));
    const parts = try self.foldParts(FixedStore, layout);
    errdefer for (parts) |*p| p.table.deinit();
    try initPairs(self, true);
    errdefer for (self.pairs) |*pp| pp.deinit();
    var direct: ?Direct = if (Direct.serves(kinds)) try Direct.init(self.gpa) else null;
    defer if (direct) |d| d.deinit(self.gpa);
    const enc = try Encoder.init(self.state, self.by, kinds);

    while (try self.child.next(pull)) |b| {
        const frozen = self.overBudget(parts);
        const e = try enc.encode(pull, b.columns, b.len, self.interner(), frozen);
        const hashes = try pull.alloc(u64, b.len);
        const tails = try pull.alloc(?[*]u8, b.len);
        const pidx = try pull.alloc(u8, b.len);
        const dest: []u8 = if (frozen) try pull.alloc(u8, b.len) else &.{};
        @memset(tails, null);
        @memset(dest, spill_parts.keep);
        for (e.absent, 0..) |x, r| if (x) {
            dest[r] = 0;
        };
        if (direct) |*d| d.hits(e, tails, pidx);
        for (hashes, tails, 0..) |*h, t, r| h.* = if (t != null) 0 else e.hash(r);

        var groups: usize = 0;
        if (self.part_state == null) groups = parts[0].store.len else for (parts) |*pp| {
            groups += pp.store.len;
        }
        const live = try pull.alloc(u32, b.len);
        var m: usize = 0;
        for (tails, 0..) |t, r| if (t == null) {
            live[m] = @intCast(r);
            m += 1;
        };
        const by_part = groups >= few_groups;
        const pof = try pull.alloc(u8, m);
        for (pof, live[0..m]) |*p, r| p.* = if (by_part) @intCast(partOf(hashes[r])) else 0;
        const runs = try keytable.Runs(i64).init(pull, if (by_part) fold_parts else 1, e.nk, pof, live[0..m], hashes, e.keys, e.masks);

        var fold = Fold(FixedStore){ .self = self, .parts = parts, .layout = layout, .tails = tails, .pidx = pidx, .dest = dest, .frozen = frozen, .direct = if (direct) |*d| d else null };
        try keytable.probe(FixedStore, .alias, &runs, 0, m, &fold);

        if (fold.nspill > 0) try self.spill.?.route(self, pull, b, dest);
        if (counts_only) {
            for (tails[0..b.len]) |t| {
                const cs: [*]i64 = @ptrCast(@alignCast(t orelse continue));
                for (cs[0..self.aggs.len]) |*c| c.* += 1;
            }
        } else try foldRows(self, pull, b, layout, tails[0..b.len], parts, pidx[0..b.len]);
        _ = scratch.reset(.retain_capacity);
    }
    return self.foldSets(.fixed, parts);
}

/// Fold with boxed keys: each row's keys boxed once, the batch hashed then probed
/// behind a prefetch, each argument evaluated once per batch, rows walked by partition.
pub fn drainImpl(self: *Aggregate) anyerror![]GroupSet {
    var scratch = std.heap.ArenaAllocator.init(self.gpa);
    defer scratch.deinit();
    const pull = scratch.allocator();

    if (self.by.len != 0 and self.by.len <= keyhash.max_fixed_keys) fixed: {
        const kinds = try self.state.alloc(KeyKind, self.by.len);
        for (self.by, kinds) |ci, *k| k.* = keyKindOf(self.in_schema.fields[ci].ty.kind) orelse break :fixed;
        var counts_only = true;
        for (self.aggs) |a| {
            if (a.func != .count or a.arg != null or a.distinct) counts_only = false;
        }
        return if (counts_only) self.drainFixed(kinds, true) else self.drainFixed(kinds, false);
    }

    if (self.by.len == 0) {
        const layout = try newIn(self.state, try Layout.init(self.state, self.aggs));
        const tail = try self.state.alignedAlloc(u8, .of(Value), @max(layout.size, 1));
        layout.clear(tail.ptr);
        try initPairs(self, false);
        errdefer for (self.pairs) |*pp| pp.deinit();
        while (try self.child.next(pull)) |b| {
            if (b.len != 0) try self.foldSingle(pull, b, layout, tail.ptr);
            _ = scratch.reset(.retain_capacity);
        }
        const one = try self.state.alloc(GroupSet, 1);
        one[0] = .{ .store = .{ .single = try newIn(self.state, GroupSet.Single{ .layout = layout, .tail = tail.ptr }) }, .len = 1, .key_kinds = &.{}, .strs = self.strs };
        if (self.part_state == null) try applyPairs(self, .{ .one = tail.ptr }, layout) else one[0].pairs = self.pairs;
        return one;
    }

    const layout = try newIn(self.state, try Layout.init(self.state, self.aggs));
    const parts = try self.foldParts(GroupStore, layout);
    errdefer for (parts) |*p| p.table.deinit();
    try initPairs(self, true);
    errdefer for (self.pairs) |*pp| pp.deinit();
    while (try self.child.next(pull)) |b| {
        const frozen = self.overBudget(parts);
        const nk = self.by.len;
        const probes = try pull.alloc(Value, b.len * nk);
        const hashes = try pull.alloc(u64, b.len);
        const dest: []u8 = if (frozen) try pull.alloc(u8, b.len) else &.{};
        @memset(dest, spill_parts.keep);
        keytable.hashBatch(b.columns, self.by, b.len, probes, hashes);

        const tails = try pull.alloc(?[*]u8, b.len);
        const pidx = try pull.alloc(u8, b.len);
        @memset(tails, null);

        const pof = try pull.alloc(u8, b.len);
        for (pof, hashes) |*p, h| p.* = @intCast(partOf(h));
        const all = try pull.alloc(u32, b.len);
        for (all, 0..) |*x, i| x.* = @intCast(i);
        const runs = try keytable.Runs(Value).init(pull, fold_parts, nk, pof, all, hashes, probes, &.{});

        var fold = Fold(GroupStore){ .self = self, .parts = parts, .layout = layout, .tails = tails, .pidx = pidx, .dest = dest, .frozen = frozen };
        try keytable.probe(GroupStore, .canon, &runs, 0, b.len, &fold);
        if (fold.nspill > 0) try self.spill.?.route(self, pull, b, dest);
        try foldRows(self, pull, b, layout, tails, parts, pidx);
        _ = scratch.reset(.retain_capacity);
    }
    return self.foldSets(.boxed, parts);
}
