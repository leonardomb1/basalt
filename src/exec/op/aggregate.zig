//! `GROUP BY` and global aggregates: grouped hash tables, per-lane partials and
//! their merge, and the vectorized reductions of the global path.

const Batch = @import("../batch.zig").Batch;
const Decimal = @import("../value.zig").Decimal;
const ErrCtx = @import("../op.zig").ErrCtx;
const Op = @import("../op.zig").Op;
const Stats = @import("../op.zig").Stats;
const Value = @import("../value.zig").Value;
const aggregates = @import("../../lang/aggregates.zig");
const ast = @import("../../lang/ast.zig");
const column = @import("../column.zig");
const eval = @import("../eval.zig");
const failLabel = @import("../op.zig").failLabel;
const keyhash = @import("../keyhash.zig");
const simd = @import("../simd.zig");
const std = @import("std");
const types = @import("../../lang/types.zig");
const Scan = @import("../op.zig").Scan;
const TestSource = @import("testing_util.zig").TestSource;
const intBatch = @import("testing_util.zig").intBatch;
const int_schema = @import("testing_util.zig").int_schema;
const kvBatch = @import("testing_util.zig").kvBatch;
const strBatch = @import("testing_util.zig").strBatch;
const testing = std.testing;

pub const Aggregate = struct {
    stats: Stats = .{},
    child: Op,
    in_schema: *const types.Schema,
    by: []const usize,
    aggs: []const Agg,
    out_schema: *const types.Schema,
    err: ?*ErrCtx = null,
    state: std.mem.Allocator,
    gpa: std.mem.Allocator,
    part_state: ?[]const std.mem.Allocator = null,
    table_gpa: ?std.mem.Allocator = null,
    strs: ?*StrTable = null,
    str_cache: std.StringHashMapUnmanaged(StrId) = .empty,
    done: bool = false,

    const StrId = struct { id: u32, h: u64 };

    pub const StrTable = struct {
        mtx: std.Thread.Mutex = .{},
        arena: std.heap.ArenaAllocator,
        map: std.StringHashMapUnmanaged(u32) = .empty,
        strs: std.ArrayListUnmanaged([]const u8) = .empty,

        pub fn init(child: std.mem.Allocator) StrTable {
            return .{ .arena = std.heap.ArenaAllocator.init(child) };
        }

        pub fn deinit(self: *StrTable) void {
            self.arena.deinit();
        }

        fn intern(self: *StrTable, s: []const u8) !u32 {
            self.mtx.lock();
            defer self.mtx.unlock();
            const a = self.arena.allocator();
            const gop = try self.map.getOrPut(a, s);
            if (!gop.found_existing) {
                const own = try a.dupe(u8, s);
                gop.key_ptr.* = own;
                gop.value_ptr.* = @intCast(self.strs.items.len);
                try self.strs.append(a, own);
            }
            return gop.value_ptr.*;
        }

        pub fn at(self: *const StrTable, id: u32) []const u8 {
            return self.strs.items[id];
        }
    };

    fn strId(self: *Aggregate, table: *StrTable, s: []const u8) !StrId {
        const gop = try self.str_cache.getOrPut(self.state, s);
        if (!gop.found_existing) {
            gop.key_ptr.* = try self.state.dupe(u8, s);
            gop.value_ptr.* = .{ .id = try table.intern(s), .h = std.hash.Wyhash.hash(0x5bd1e995, s) };
        }
        return gop.value_ptr.*;
    }

    fn strTable(self: *Aggregate) !*StrTable {
        if (self.strs) |t| return t;
        const t = try self.state.create(StrTable);
        t.* = StrTable.init(self.state);
        self.strs = t;
        return t;
    }

    pub const Agg = struct { func: ast.AggFunc, arg: ?*const ast.Expr, ty: types.Type, distinct: bool = false };

    pub const Acc = struct {
        n: i64 = 0,
        sum_i: i128 align(8) = 0,
        sum_f: f64 = 0,
        ext: Value = .null,
        seen: ?*DistinctSet() = null,
        vals: ?*std.array_list.Managed(f64) = null,
    };

    /// A type-returning fn for the same reason as `GroupMap`.
    pub fn DistinctSet() type {
        return std.HashMap([]const Value, void, keyhash.MultiKeyCtx, std.hash_map.default_max_load_percentage);
    }

    fn noteMedian(alloc: std.mem.Allocator, acc: *Acc, x: f64) !void {
        const l = acc.vals orelse blk: {
            const p = try alloc.create(std.array_list.Managed(f64));
            p.* = std.array_list.Managed(f64).init(alloc);
            acc.vals = p;
            break :blk p;
        };
        try l.append(x);
    }

    /// Fold another lane's distinct values into `acc`, sizing the destination first:
    /// inserting a hash-ordered run into a smaller table built ever-longer probe runs
    /// (a global COUNT(DISTINCT) took 292s at -j 8, 8s at -j 1).
    fn mergeDistinct(alloc: std.mem.Allocator, acc: *Acc, src: *const DistinctSet()) !void {
        const set = acc.seen orelse blk: {
            const p = try alloc.create(DistinctSet());
            p.* = DistinctSet().init(alloc);
            acc.seen = p;
            break :blk p;
        };
        try set.ensureTotalCapacity(std.math.cast(u32, set.count() + src.count()) orelse return error.OutOfMemory);
        var it = src.keyIterator();
        while (it.next()) |k| try noteDistinct(alloc, acc, k.*[0]);
    }

    /// Probes with a stack key and copies only on a miss. Copying first grew an arena
    /// per row: COUNT(DISTINCT) over 200M rows held ~7.6 GB for 200 values.
    fn noteDistinct(alloc: std.mem.Allocator, acc: *Acc, v: Value) !void {
        const set = acc.seen orelse blk: {
            const p = try alloc.create(DistinctSet());
            p.* = DistinctSet().init(alloc);
            acc.seen = p;
            break :blk p;
        };
        var probe = [_]Value{v};
        const gop = try set.getOrPut(probe[0..]);
        if (!gop.found_existing) {
            const key = try alloc.alloc(Value, 1);
            key[0] = try dupeValue(alloc, v);
            gop.key_ptr.* = key;
        }
        acc.n = @intCast(set.count());
    }

    pub const GroupSet = struct {
        store: Store,
        len: usize,
        key_kinds: []const types.TypeKind,
        table: ?*GroupTable = null,
        strs: ?*const StrTable = null,

        pub fn freeTable(self: *const GroupSet) void {
            if (self.table) |t| t.deinit();
        }

        pub const Store = union(enum) {
            fixed: *FixedStore,
            boxed: *GroupStore,
            single: []Acc,
        };

        pub fn keyValue(self: *const GroupSet, i: usize, j: usize) Value {
            switch (self.store) {
                .boxed => |st| return st.at(i).keys[j],
                .single => unreachable,
                .fixed => |st| {
                    const rec = st.at(i);
                    if (rec.mask.* & (@as(u64, 1) << @intCast(j)) != 0) return .null;
                    const raw = rec.keys[j];
                    return switch (self.key_kinds[j]) {
                        .int => .{ .int = raw },
                        .time => .{ .time = raw },
                        .timestamp => .{ .timestamp = raw },
                        .date => .{ .date = @intCast(raw) },
                        .bool => .{ .bool = raw != 0 },
                        .float => .{ .float = @bitCast(raw) },
                        .string => .{ .string = self.strs.?.at(@intCast(raw)) },
                        else => unreachable,
                    };
                },
            }
        }

        pub fn acc(self: *const GroupSet, i: usize, j: usize) Acc {
            return switch (self.store) {
                .fixed => |st| st.layout.acc(st.at(i).tail, j),
                .boxed => |st| st.layout.acc(st.at(i).tail, j),
                .single => |accs| accs[j],
            };
        }
    };

    pub const GroupMerge = struct {
        alloc: std.mem.Allocator,
        aggs: []const Agg,
        any_distinct: bool,
        own: bool = false,
        set: GroupSet,
        table: GroupTable,

        fn anyDistinct(aggs: []const Agg) bool {
            for (aggs) |a| {
                if (a.distinct) return true;
            }
            return false;
        }

        /// Merge into `set` itself, its store and index, instead of copying it. The index
        /// moves here (`deinit` frees it); null for a set without one of its own.
        pub fn adopt(set: *const GroupSet, aggs: []const Agg) ?GroupMerge {
            const table = set.table orelse return null;
            const alloc = switch (set.store) {
                .fixed => |st| st.recs.alloc,
                .boxed => |st| st.recs.alloc,
                .single => return null,
            };
            defer table.entries = &.{};
            return .{
                .alloc = alloc,
                .aggs = aggs,
                .any_distinct = anyDistinct(aggs),
                .set = set.*,
                .table = table.*,
            };
        }

        pub fn deinit(self: *GroupMerge) void {
            self.table.deinit();
        }

        pub fn init(alloc: std.mem.Allocator, like: *const GroupSet, aggs: []const Agg) !GroupMerge {
            const any_distinct = anyDistinct(aggs);
            const store: GroupSet.Store = switch (like.store) {
                .fixed => |st| blk: {
                    const n = try alloc.create(FixedStore);
                    n.* = FixedStore.init(alloc, st.nkeys, st.layout);
                    break :blk .{ .fixed = n };
                },
                .boxed => |st| blk: {
                    const n = try alloc.create(GroupStore);
                    n.* = GroupStore.init(alloc, st.nkeys, st.layout);
                    break :blk .{ .boxed = n };
                },
                .single => blk: {
                    const accs = try alloc.alloc(Acc, aggs.len);
                    @memset(accs, .{});
                    break :blk .{ .single = accs };
                },
            };
            return .{
                .alloc = alloc,
                .aggs = aggs,
                .any_distinct = any_distinct,
                .set = .{ .store = store, .len = if (store == .single) 1 else 0, .key_kinds = like.key_kinds, .strs = like.strs },
                .table = try GroupTable.init(alloc, 256),
            };
        }

        pub fn add(self: *GroupMerge, src: *const GroupSet, i: usize) !void {
            switch (self.set.store) {
                .single => |dst| for (dst, self.aggs, 0..) |*d, agg, j| try mergeAcc(self.alloc, d, src.acc(i, j), agg),
                .fixed => |dst| {
                    const sr = src.store.fixed.at(i);
                    const h = src.store.fixed.hashAt(i);
                    const rec = try self.find(dst, h, FixedKey{ .vals = sr.keys, .mask = sr.mask.* }) orelse {
                        const nr = try dst.push(h);
                        @memcpy(nr.keys, sr.keys);
                        nr.mask.* = sr.mask.*;
                        try self.adoptTail(dst.layout, nr.tail, sr.tail);
                        return;
                    };
                    for (self.aggs, 0..) |agg, j| try dst.layout.merge(self.alloc, rec.tail, sr.tail, j, agg);
                },
                .boxed => |dst| {
                    const sr = src.store.boxed.at(i);
                    const h = src.store.boxed.hashAt(i);
                    const rec = try self.find(dst, h, @as([]const Value, sr.keys)) orelse {
                        const nr = try dst.push(h);
                        if (self.own) {
                            for (nr.keys, sr.keys) |*o, v| o.* = try dupeValue(self.alloc, v);
                        } else @memcpy(nr.keys, sr.keys);
                        try self.adoptTail(dst.layout, nr.tail, sr.tail);
                        return;
                    };
                    for (self.aggs, 0..) |agg, j| try dst.layout.merge(self.alloc, rec.tail, sr.tail, j, agg);
                },
            }
        }

        /// The record for `key`, or null after reserving a slot the caller then fills.
        fn find(self: *GroupMerge, dst: anytype, h: u64, key: anytype) !?@TypeOf(dst.at(0)) {
            const f = try self.table.getOrPut(h, key, dst, @intCast(dst.len));
            if (f.found) return dst.at(f.slot);
            self.set.len += 1;
            return null;
        }

        /// A new group's aggregate state, copied, except a DISTINCT set, which belongs to
        /// the producing fold's arena and is rebuilt here, and with `own` anything else
        /// pointing into the source, so the source set can be freed once merged.
        fn adoptTail(self: *GroupMerge, layout: *const Layout, dst: [*]u8, src: [*]u8) !void {
            @memcpy(dst[0..layout.size], src[0..layout.size]);
            if (!self.any_distinct and !self.own) return;
            for (self.aggs, layout.slots, 0..) |agg, sl, j| {
                if (sl != .full) continue;
                const d = layout.ptr(Acc, dst, j);
                if (agg.distinct) {
                    d.seen = null;
                    d.n = 0;
                    if (layout.ptr(Acc, src, j).seen) |ss| try mergeDistinct(self.alloc, d, ss);
                } else if (self.own) switch (agg.func) {
                    .min, .max => d.ext = try dupeValue(self.alloc, d.ext),
                    .median => if (d.vals) |sv| {
                        d.vals = null;
                        const l = try self.alloc.create(std.array_list.Managed(f64));
                        l.* = try .initCapacity(self.alloc, sv.items.len);
                        l.appendSliceAssumeCapacity(sv.items);
                        d.vals = l;
                    },
                    else => {},
                };
            }
        }

        pub fn result(self: *GroupMerge) GroupSet {
            var set = self.set;
            set.table = null;
            return set;
        }
    };

    /// A type-returning fn, not a `const` type: `std.testing.refAllDeclsRecursive` dives
    /// into container-level types, and diving through this `std.HashMap` built a binary
    /// that segfaulted at startup under Zig 0.15.2.
    pub fn GroupMap() type {
        return std.HashMap([]const Value, usize, keyhash.MultiKeyCtx, std.hash_map.default_max_load_percentage);
    }

    const Partial = struct {
        nvalid: usize,
        sum_i: i128 = 0,
        sum_f: f64 = 0,
        ext: ?Value = null,
    };

    pub fn next(self: *Aggregate, arena: std.mem.Allocator) anyerror!?Batch {
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

    pub const fold_parts = 64;
    const part_shift = 58;

    pub fn partOf(h: u64) usize {
        return @intCast(h >> part_shift);
    }

    /// One partition's table and the store it indexes. A serial fold shares one store,
    /// so groups keep first-seen order; a lane's partitions each own theirs.
    fn FoldPart(comptime Store: type) type {
        return struct {
            table: GroupTable,
            store: *Store,
            alloc: std.mem.Allocator,
        };
    }

    /// The tables live in a real allocator, not the arena, so a growing table frees
    /// what it outgrew.
    fn foldParts(self: *Aggregate, comptime Store: type, layout: *const Layout) ![]FoldPart(Store) {
        const parts = try self.state.alloc(FoldPart(Store), fold_parts);
        const tg = self.table_gpa orelse self.gpa;
        var made: usize = 0;
        errdefer for (parts[0..made]) |*p| p.table.deinit();
        if (self.part_state) |ps| {
            for (parts, ps) |*p, a| {
                p.* = .{ .table = try GroupTable.init(tg, 16), .store = try newIn(a, Store.init(a, self.by.len, layout)), .alloc = a };
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

    fn newIn(a: std.mem.Allocator, v: anytype) !*@TypeOf(v) {
        const p = try a.create(@TypeOf(v));
        p.* = v;
        return p;
    }

    fn foldSets(self: *Aggregate, comptime tag: std.meta.Tag(GroupSet.Store), parts: anytype) ![]GroupSet {
        const kinds = try self.keyKinds();
        const own = self.part_state != null;
        const out = try self.state.alloc(GroupSet, if (own) fold_parts else 1);
        for (out, parts[0..out.len]) |*o, *p| o.* = .{
            .store = @unionInit(GroupSet.Store, @tagName(tag), p.store),
            .len = p.store.len,
            .key_kinds = kinds,
            .table = if (own) &p.table else null,
            .strs = self.strs,
        };
        if (!own) for (parts) |*p| p.table.deinit();
        return out;
    }

    fn keyKinds(self: *Aggregate) ![]types.TypeKind {
        const kinds = try self.state.alloc(types.TypeKind, self.by.len);
        for (self.by, kinds) |ci, *k| k.* = self.in_schema.fields[ci].ty.kind;
        return kinds;
    }

    /// Fold with raw fixed-width keys. Floats group by number, not bits, as everywhere
    /// else (-0.0 split from 0.0 before). Below `few_groups` rows go in arrival order;
    /// past it a batch is walked a partition at a time so probes share a table.
    fn drainFixed(self: *Aggregate, kinds: []const KeyKind, comptime counts_only: bool) anyerror![]GroupSet {
        var scratch = std.heap.ArenaAllocator.init(self.gpa);
        defer scratch.deinit();
        const pull = scratch.allocator();

        const nparts = fold_parts;
        const counts = try self.state.alloc(u32, nparts + 1);
        const layout = try newIn(self.state, try Layout.init(self.state, self.aggs));
        const parts = try self.foldParts(FixedStore, layout);
        errdefer for (parts) |*p| p.table.deinit();
        const nk = self.by.len;

        const direct: ?Direct = if (nk == 1 and kinds[0] != .f64k and kinds[0] != .boolk) try Direct.init(self.gpa) else null;
        var has_str = false;
        for (kinds) |k| if (k == .strk) {
            has_str = true;
        };
        defer if (direct) |d| d.deinit(self.gpa);
        var direct_base: ?i64 = null;

        while (try self.child.next(pull)) |b| {
            const keys = try pull.alloc(i64, b.len * nk);
            const hkeys = if (has_str) try pull.alloc(i64, b.len * nk) else keys;
            const masks = try pull.alloc(u64, b.len);
            const hashes = try pull.alloc(u64, b.len);
            const tails = try pull.alloc(?[*]u8, b.len);
            const pidx = try pull.alloc(u8, b.len);
            @memset(masks, 0);
            @memset(tails, null);

            for (self.by, kinds, 0..) |ci, kk, j| {
                const col = b.columns[ci];
                switch (kk) {
                    .i64k => for (0..b.len) |r| {
                        keys[r * nk + j] = col.data.i64[r];
                    },
                    .i32k => for (0..b.len) |r| {
                        keys[r * nk + j] = col.data.i32[r];
                    },
                    .boolk => for (0..b.len) |r| {
                        keys[r * nk + j] = @intFromBool(col.data.b[r]);
                    },
                    .f64k => for (0..b.len) |r| {
                        keys[r * nk + j] = @bitCast(keyhash.canonF64(col.data.f64[r]));
                    },
                    .strk => {
                        const table = try self.strTable();
                        if (col.dict) |d| {
                            const ents = try pull.alloc(StrId, d.values.len);
                            for (ents, d.values) |*e, v| e.* = try self.strId(table, v);
                            for (0..b.len) |r| {
                                if (!col.validity.get(r)) continue;
                                const e = ents[d.codes[r]];
                                keys[r * nk + j] = e.id;
                                hkeys[r * nk + j] = @bitCast(e.h);
                            }
                        } else for (0..b.len) |r| {
                            if (!col.validity.get(r)) continue;
                            const e = try self.strId(table, col.data.bytes.at(r));
                            keys[r * nk + j] = e.id;
                            hkeys[r * nk + j] = @bitCast(e.h);
                        }
                    },
                }
                if (!col.validity.allSet(b.len)) for (0..b.len) |r| {
                    if (col.validity.get(r)) continue;
                    masks[r] |= @as(u64, 1) << @intCast(j);
                    keys[r * nk + j] = 0;
                    if (has_str) hkeys[r * nk + j] = 0;
                };
                if (has_str and kk != .strk) for (0..b.len) |r| {
                    hkeys[r * nk + j] = keys[r * nk + j];
                };
            }
            if (direct) |d| {
                if (direct_base == null) direct_base = Direct.baseFor(keys, masks);
                const base = direct_base.?;
                for (keys, masks, tails, pidx) |k, m, *t, *pi| {
                    if (m != 0) continue;
                    const off = k -% base;
                    if (off < 0 or off >= Direct.len) continue;
                    const e = d.recs[@intCast(off)] orelse continue;
                    t.* = e;
                    pi.* = d.parts[@intCast(off)];
                }
            }
            var r: usize = 0;
            while (r < b.len) : (r += 1) hashes[r] = if (tails[r] != null) 0 else fixedHash(hkeys[r * nk ..][0..nk], masks[r]);

            const argcols = try pull.alloc(?column.Column, self.aggs.len);
            const fast = try pull.alloc(Fast, self.aggs.len);
            for (self.aggs, argcols, fast, layout.slots) |agg, *c, *f, sl| {
                c.* = if (agg.arg) |e| try self.argColumn(pull, agg, e, b) else null;
                f.* = Fast.of(sl, agg, c.*, b.len);
            }

            var groups: usize = 0;
            if (self.part_state == null) groups = parts[0].store.len else for (parts) |*pp| {
                groups += pp.store.len;
            }
            const order = try pull.alloc(u32, b.len);
            if (groups < few_groups) {
                for (order, 0..) |*o, ri| o.* = @intCast(ri);
            } else {
                @memset(counts, 0);
                for (hashes[0..b.len]) |h| counts[(h >> part_shift) + 1] += 1;
                for (1..nparts + 1) |ci| counts[ci] += counts[ci - 1];
                for (hashes[0..b.len], 0..) |h, ri| {
                    const pi = h >> part_shift;
                    order[counts[pi]] = @intCast(ri);
                    counts[pi] += 1;
                }
            }

            for (order, 0..) |ri, oi| {
                if (tails[ri] != null) continue;
                if (oi + prefetch_ahead < order.len) {
                    const pr = order[oi + prefetch_ahead];
                    parts[hashes[pr] >> part_shift].table.prefetch(hashes[pr]);
                }
                const key = FixedKey{ .vals = keys[ri * nk ..][0..nk], .mask = masks[ri] };
                const pi = hashes[ri] >> part_shift;
                const part = &parts[pi];
                const at: u32 = @intCast(part.store.len);
                const f = try part.table.getOrPut(hashes[ri], key, part.store, at);
                const rec = if (f.found) part.store.at(f.slot) else blk: {
                    const nr = try part.store.push(hashes[ri]);
                    @memcpy(nr.keys, key.vals);
                    nr.mask.* = key.mask;
                    break :blk nr;
                };
                tails[ri] = rec.tail;
                pidx[ri] = @intCast(pi);
                if (direct) |d| if (key.mask == 0) {
                    const off = key.vals[0] -% direct_base.?;
                    if (off >= 0 and off < Direct.len) {
                        d.recs[@intCast(off)] = rec.tail;
                        d.parts[@intCast(off)] = @intCast(pi);
                    }
                };
            }

            if (counts_only) {
                for (tails[0..b.len]) |t| {
                    const cs: [*]i64 = @ptrCast(@alignCast(t.?));
                    for (cs[0..self.aggs.len]) |*c| c.* += 1;
                }
            } else for (self.aggs, fast, 0..) |agg, fk, j| switch (fk) {
                .count_star, .count_all_valid => for (tails[0..b.len]) |t| {
                    layout.ptr(i64, t.?, j).* += 1;
                },
                .sum_i64 => {
                    const col = argcols[j].?.data.i64;
                    for (tails[0..b.len], col[0..b.len]) |t, v| {
                        const x = layout.ptr(SumI, t.?, j);
                        x.s += v;
                        x.n += 1;
                    }
                },
                .sum_f64 => {
                    const col = argcols[j].?.data.f64;
                    for (tails[0..b.len], col[0..b.len]) |t, v| {
                        const x = layout.ptr(SumF, t.?, j);
                        x.s += v;
                        x.n += 1;
                    }
                },
                .sum_f_of_i64 => {
                    const col = argcols[j].?.data.i64;
                    for (tails[0..b.len], col[0..b.len]) |t, v| {
                        const x = layout.ptr(SumF, t.?, j);
                        x.s += @floatFromInt(v);
                        x.n += 1;
                    }
                },
                .generic => for (tails[0..b.len], pidx[0..b.len], 0..) |t, pi, ri| {
                    const v = if (argcols[j]) |col| col.getValue(ri) else Value.null;
                    try layout.update(parts[pi].alloc, t.?, j, agg, v);
                },
            };
            _ = scratch.reset(.retain_capacity);
        }
        return self.foldSets(.fixed, parts);
    }

    /// Fold with boxed keys: each row's keys boxed once, the batch hashed then probed
    /// behind a prefetch, each argument evaluated once per batch, rows walked by partition.
    fn drainImpl(self: *Aggregate) anyerror![]GroupSet {
        var scratch = std.heap.ArenaAllocator.init(self.gpa);
        defer scratch.deinit();
        const pull = scratch.allocator();

        if (self.by.len != 0) fixed: {
            const kinds = try self.state.alloc(KeyKind, self.by.len);
            for (self.by, kinds) |ci, *k| k.* = keyKindOf(self.in_schema.fields[ci].ty.kind) orelse break :fixed;
            var counts_only = true;
            for (self.aggs) |a| {
                if (a.func != .count or a.arg != null or a.distinct) counts_only = false;
            }
            return if (counts_only) self.drainFixed(kinds, true) else self.drainFixed(kinds, false);
        }

        if (self.by.len == 0) {
            const accs = try self.state.alloc(Acc, self.aggs.len);
            for (accs) |*a| a.* = .{};
            while (try self.child.next(pull)) |b| {
                if (b.len != 0 and !(try self.foldVectorized(pull, b, accs))) try self.foldRowwise(pull, b, accs);
                _ = scratch.reset(.retain_capacity);
            }
            const one = try self.state.alloc(GroupSet, 1);
            one[0] = .{ .store = .{ .single = accs }, .len = 1, .key_kinds = &.{} };
            return one;
        }

        const nparts = fold_parts;
        const counts = try self.state.alloc(u32, nparts + 1);
        const layout = try newIn(self.state, try Layout.init(self.state, self.aggs));
        const parts = try self.foldParts(GroupStore, layout);
        errdefer for (parts) |*p| p.table.deinit();
        const hctx = keyhash.MultiKeyCtx{};
        while (try self.child.next(pull)) |b| {
            const nk = self.by.len;
            const probes = try pull.alloc(Value, b.len * nk);
            const hashes = try pull.alloc(u64, b.len);

            var r: usize = 0;
            while (r < b.len) : (r += 1) {
                const probe = probes[r * nk ..][0..nk];
                for (self.by, 0..) |ci, j| probe[j] = b.columns[ci].getValue(r);
                hashes[r] = hctx.hash(probe);
            }

            const argcols = try pull.alloc(?column.Column, self.aggs.len);
            for (self.aggs, argcols) |agg, *c| {
                c.* = if (agg.arg) |e| try self.argColumn(pull, agg, e, b) else null;
            }

            @memset(counts, 0);
            for (hashes[0..b.len]) |h| counts[(h >> part_shift) + 1] += 1;
            for (1..nparts + 1) |ci| counts[ci] += counts[ci - 1];
            const order = try pull.alloc(u32, b.len);
            for (hashes[0..b.len], 0..) |h, ri| {
                const pi = h >> part_shift;
                order[counts[pi]] = @intCast(ri);
                counts[pi] += 1;
            }

            for (order, 0..) |ri, oi| {
                if (oi + prefetch_ahead < order.len) {
                    const pr = order[oi + prefetch_ahead];
                    parts[hashes[pr] >> part_shift].table.prefetch(hashes[pr]);
                }
                r = ri;
                const probe = probes[r * nk ..][0..nk];
                const part = &parts[hashes[r] >> part_shift];
                const at: u32 = @intCast(part.store.len);
                const f = try part.table.getOrPut(hashes[r], probe, part.store, at);
                const rec = if (f.found) part.store.at(f.slot) else blk: {
                    const nr = try part.store.push(hashes[r]);
                    for (probe, nr.keys) |v, *o| o.* = try dupeValue(part.alloc, v);
                    break :blk nr;
                };
                for (self.aggs, 0..) |agg, j| {
                    const v = if (argcols[j]) |col| col.getValue(r) else Value.null;
                    try layout.update(part.alloc, rec.tail, j, agg, v);
                }
            }
            _ = scratch.reset(.retain_capacity);
        }
        return self.foldSets(.boxed, parts);
    }

    const prefetch_ahead = 8;

    const Direct = struct {
        recs: []?[*]u8,
        parts: []u8,

        const len = 1 << 14;

        fn init(gpa: std.mem.Allocator) !Direct {
            const recs = try gpa.alloc(?[*]u8, len);
            @memset(recs, null);
            const parts = try gpa.alloc(u8, len);
            return .{ .recs = recs, .parts = parts };
        }

        fn deinit(self: Direct, gpa: std.mem.Allocator) void {
            gpa.free(self.recs);
            gpa.free(self.parts);
        }

        /// The smallest non-null key, so a range starting at 0 or 1 sits wholly inside.
        fn baseFor(keys: []const i64, masks: []const u64) i64 {
            var lo: i64 = std.math.maxInt(i64);
            for (keys, masks) |k, m| if (m == 0 and k < lo) {
                lo = k;
            };
            return if (lo == std.math.maxInt(i64)) 0 else lo;
        }
    };

    const few_groups = 1 << 12;

    /// Each word through murmur3's 64-bit finalizer. Wyhash's streaming form was a fifth
    /// of a 100-group GROUP BY.
    fn fixedHash(vals: []const i64, mask: u64) u64 {
        var h: u64 = 0x9E3779B97F4A7C15 ^ mask;
        for (vals) |v| h = fmix64(h ^ (@as(u64, @bitCast(v)) *% 0xC2B2AE3D27D4EB4F));
        return h;
    }

    fn fmix64(x0: u64) u64 {
        var x = x0;
        x ^= x >> 33;
        x *%= 0xff51afd7ed558ccd;
        x ^= x >> 33;
        x *%= 0xc4ceb9fe1a85ec53;
        x ^= x >> 33;
        return x;
    }

    const Fast = enum {
        count_star,
        count_all_valid,
        sum_i64,
        sum_f64,
        sum_f_of_i64,
        generic,

        fn of(slot: Slot, agg: Agg, col: ?column.Column, n: usize) Fast {
            const c = col orelse return if (slot == .count) .count_star else .generic;
            if (!c.validity.allSet(n)) return .generic;
            return switch (slot) {
                .count => .count_all_valid,
                .sum_i => if (agg.ty.kind != .decimal and c.ty.kind == .int) .sum_i64 else .generic,
                .sum_f => switch (c.ty.kind) {
                    .float => .sum_f64,
                    .int => .sum_f_of_i64,
                    else => .generic,
                },
                .full => .generic,
            };
        }
    };

    const KeyKind = enum { i64k, i32k, boolk, f64k, strk };

    fn keyKindOf(kind: types.TypeKind) ?KeyKind {
        return switch (kind) {
            .int, .time, .timestamp => .i64k,
            .date => .i32k,
            .bool => .boolk,
            .float => .f64k,
            .string => .strk,
            else => null,
        };
    }

    const RecBlocks = struct {
        const first_shift = 4;
        const block_shift = 13;
        const block = 1 << block_shift;
        const small = block_shift - first_shift + 1;

        alloc: std.mem.Allocator,
        rec_size: usize,
        blocks: std.array_list.Managed([]u8),

        fn init(alloc: std.mem.Allocator, rec_size: usize) RecBlocks {
            return .{ .alloc = alloc, .rec_size = rec_size, .blocks = .init(alloc) };
        }

        const Loc = struct { b: usize, off: usize };

        fn locate(i: usize) Loc {
            if (i >= block) return .{ .b = small - 1 + (i >> block_shift), .off = i & (block - 1) };
            if (i < 1 << first_shift) return .{ .b = 0, .off = i };
            const k = std.math.log2_int(usize, i >> first_shift);
            return .{ .b = k + 1, .off = i - (@as(usize, 1) << first_shift << k) };
        }

        fn capOf(b: usize) usize {
            if (b >= small) return block;
            return @as(usize, 1) << @intCast(first_shift + @max(b, 1) - 1);
        }

        fn base(self: RecBlocks, i: usize) [*]u8 {
            const l = locate(i);
            return self.blocks.items[l.b].ptr + l.off * self.rec_size;
        }

        fn reserve(self: *RecBlocks, i: usize) !void {
            const l = locate(i);
            if (l.b < self.blocks.items.len) return;
            try self.blocks.append(try self.alloc.alignedAlloc(u8, .of(Value), capOf(l.b) * self.rec_size));
        }
    };

    const FixedStore = struct {
        nkeys: usize,
        layout: *const Layout,
        recs: RecBlocks,
        len: usize = 0,

        const Rec = struct { keys: []i64, mask: *u64, tail: [*]u8 };

        fn init(alloc: std.mem.Allocator, nkeys: usize, layout: *const Layout) FixedStore {
            return .{ .nkeys = nkeys, .layout = layout, .recs = .init(alloc, @sizeOf(u64) + nkeys * @sizeOf(i64) + @sizeOf(u64) + layout.size) };
        }

        fn hashAt(self: *const FixedStore, i: usize) u64 {
            return @as(*const u64, @ptrCast(@alignCast(self.recs.base(i)))).*;
        }

        fn at(self: FixedStore, i: usize) Rec {
            const base = self.recs.base(i) + @sizeOf(u64);
            const ksz = self.nkeys * @sizeOf(i64);
            return .{
                .keys = @alignCast(std.mem.bytesAsSlice(i64, base[0..ksz])),
                .mask = @ptrCast(@alignCast(base + ksz)),
                .tail = base + ksz + @sizeOf(u64),
            };
        }

        /// One address computation for hash, mask and keys; going through `hashAt` and `at`
        /// located the record twice per probe.
        fn eqlAt(self: *const FixedStore, i: usize, h: u64, key: FixedKey) bool {
            const base = self.recs.base(i);
            const words: [*]const u64 = @ptrCast(@alignCast(base));
            if (words[0] != h) return false;
            const vals: [*]const i64 = @ptrCast(@alignCast(base + @sizeOf(u64)));
            for (key.vals, 0..) |v, j| if (vals[j] != v) return false;
            return words[1 + key.vals.len] == key.mask;
        }

        fn push(self: *FixedStore, h: u64) !Rec {
            try self.recs.reserve(self.len);
            @as(*u64, @ptrCast(@alignCast(self.recs.base(self.len)))).* = h;
            const rec = self.at(self.len);
            self.len += 1;
            self.layout.clear(rec.tail);
            return rec;
        }
    };

    const FixedKey = struct { vals: []const i64, mask: u64 };

    const GroupTable = struct {
        const salt_bits = 6;
        const idx_bits = 32 - salt_bits;
        const max_groups = (1 << idx_bits) - 1;

        entries: []u32,
        len: usize = 0,
        mask: u64,
        alloc: std.mem.Allocator,

        fn init(alloc: std.mem.Allocator, cap_pow2: usize) !GroupTable {
            const e = try alloc.alloc(u32, cap_pow2);
            @memset(e, 0);
            return .{ .entries = e, .mask = cap_pow2 - 1, .alloc = alloc };
        }

        /// Salt comes from bits the bucket index does not use, so the two stay independent.
        fn saltOf(h: u64) u32 {
            return @intCast((h >> 32) & ((1 << salt_bits) - 1));
        }

        fn pack(h: u64, idx: usize) u32 {
            return (saltOf(h) << idx_bits) | @as(u32, @intCast(idx + 1));
        }

        fn prefetch(self: *const GroupTable, h: u64) void {
            @prefetch(&self.entries[h & self.mask], .{ .rw = .read, .locality = 3 });
        }

        /// Free the entries. Safe to repeat: a table handed on by `GroupMerge.adopt` is left
        /// empty and its owner's cleanup still runs.
        fn deinit(self: *GroupTable) void {
            self.alloc.free(self.entries);
            self.entries = &.{};
        }

        /// Rebuild from this table's own entries, taking each hash from its record, and
        /// free the outgrown entries (an arena kept every outgrown table).
        fn grow(self: *GroupTable, store: anytype) !void {
            const cap = self.entries.len * growth;
            const ne = try self.alloc.alloc(u32, cap);
            @memset(ne, 0);
            const nmask = cap - 1;
            for (self.entries) |e| {
                if (e == 0) continue;
                const h = store.hashAt((e & max_groups) - 1);
                var i = h & nmask;
                while (ne[i] != 0) i = (i + 1) & nmask;
                ne[i] = e;
            }
            self.alloc.free(self.entries);
            self.entries = ne;
            self.mask = nmask;
        }

        const growth = 2;

        const Found = struct { slot: u32, found: bool };

        inline fn getOrPut(
            self: *GroupTable,
            h: u64,
            key: anytype,
            store: anytype,
            new_slot: u32,
        ) !Found {
            if (new_slot >= max_groups) return error.TooManyGroups;
            if ((self.len + 1) * 10 >= self.entries.len * 7) try self.grow(store);
            const want = saltOf(h) << idx_bits;
            var i = h & self.mask;
            while (true) : (i = (i + 1) & self.mask) {
                const e = self.entries[i];
                if (e == 0) {
                    self.entries[i] = pack(h, new_slot);
                    self.len += 1;
                    return .{ .slot = new_slot, .found = false };
                }
                if ((e >> idx_bits) << idx_bits != want) continue;
                const idx = (e & max_groups) - 1;
                if (store.eqlAt(idx, h, key)) return .{ .slot = idx, .found = true };
            }
        }
    };

    const GroupStore = struct {
        nkeys: usize,
        layout: *const Layout,
        recs: RecBlocks,
        len: usize = 0,

        const Rec = struct { keys: []Value, tail: [*]u8 };

        fn init(alloc: std.mem.Allocator, nkeys: usize, layout: *const Layout) GroupStore {
            return .{ .nkeys = nkeys, .layout = layout, .recs = .init(alloc, @sizeOf(u64) + nkeys * @sizeOf(Value) + layout.size) };
        }

        fn hashAt(self: *const GroupStore, i: usize) u64 {
            return @as(*const u64, @ptrCast(@alignCast(self.recs.base(i)))).*;
        }

        fn at(self: GroupStore, i: usize) Rec {
            const base = self.recs.base(i) + @sizeOf(u64);
            const ksz = self.nkeys * @sizeOf(Value);
            return .{
                .keys = @alignCast(std.mem.bytesAsSlice(Value, base[0..ksz])),
                .tail = base + ksz,
            };
        }

        fn eqlAt(self: *const GroupStore, i: usize, h: u64, key: []const Value) bool {
            if (self.hashAt(i) != h) return false;
            return keyhash.MultiKeyCtx.eql(.{}, key, self.at(i).keys);
        }

        fn push(self: *GroupStore, h: u64) !Rec {
            try self.recs.reserve(self.len);
            @as(*u64, @ptrCast(@alignCast(self.recs.base(self.len)))).* = h;
            const rec = self.at(self.len);
            self.len += 1;
            self.layout.clear(rec.tail);
            return rec;
        }
    };

    /// Combine partial `src` into `dst` for one agg, the dual of `updateAcc`. `dst_alloc`
    /// owns any MIN/MAX string carried over.
    pub fn mergeAcc(dst_alloc: std.mem.Allocator, dst: *Acc, src: Acc, agg: Agg) !void {
        switch (agg.func) {
            .count => if (agg.distinct) {
                if (src.seen) |s| try mergeDistinct(dst_alloc, dst, s);
            } else {
                dst.n += src.n;
            },
            .sum => {
                if (agg.ty.kind == .float)
                    dst.sum_f += src.sum_f
                else
                    dst.sum_i = std.math.add(i128, dst.sum_i, src.sum_i) catch return error.IntOverflow;
                dst.n += src.n;
            },
            .avg => {
                dst.sum_f += src.sum_f;
                dst.n += src.n;
            },
            .median => if (src.vals) |s| {
                for (s.items) |x| try noteMedian(dst_alloc, dst, x);
            },
            .count_if => dst.n += src.n,
            .bool_and, .bool_or => {
                dst.n += src.n;
                dst.sum_i += src.sum_i;
            },
            .bit_and, .bit_or, .bit_xor => if (src.n > 0) {
                dst.sum_i = if (dst.n == 0) src.sum_i else bitFold(agg.func, dst.sum_i, src.sum_i);
                dst.n += src.n;
            },
            .var_samp, .var_pop, .stddev_samp, .stddev_pop => mergeMoments(dst, src),
            .min => if (!src.ext.isNull() and (dst.ext.isNull() or lessV(src.ext, dst.ext))) {
                dst.ext = try dupeValue(dst_alloc, src.ext);
            },
            .max => if (!src.ext.isNull() and (dst.ext.isNull() or lessV(dst.ext, src.ext))) {
                dst.ext = try dupeValue(dst_alloc, src.ext);
            },
        }
    }

    /// Evaluate every agg's argument as a column once and SIMD-reduce it. False,
    /// touching nothing, if any agg is not covered; that depends only on the schema.
    fn foldVectorized(self: *Aggregate, arena: std.mem.Allocator, b: Batch, accs: []Acc) anyerror!bool {
        for (self.aggs) |agg| if (agg.distinct) return false;
        const partials = try arena.alloc(Partial, self.aggs.len);
        for (self.aggs, partials) |agg, *p| {
            p.* = (try self.reduceBatch(arena, agg, b)) orelse return false;
        }
        for (self.aggs, partials, accs) |agg, p, *acc| try mergePartial(acc, agg, p);
        return true;
    }

    fn foldRowwise(self: *Aggregate, arena: std.mem.Allocator, b: Batch, accs: []Acc) anyerror!void {
        var r: usize = 0;
        while (r < b.len) : (r += 1) {
            for (self.aggs, 0..) |agg, j| {
                const v = try self.argValue(arena, agg, b, r);
                try updateAcc(self.state, &accs[j], agg, v, agg.arg != null);
            }
        }
    }

    /// What a text argument is cast to before it reaches the accumulator, or null for
    /// COUNT, MIN and MAX. Parallel CSV lanes carry raw text, so the cast happens here.
    fn argCast(agg: Agg) ?types.Type {
        return switch (aggregates.spec(agg.func).arg) {
            .star_or_any, .any => null,
            .numeric => agg.ty.asNullable(),
            .int => types.Type.init(.int).asNullable(),
            .bool => types.Type.init(.bool).asNullable(),
        };
    }

    /// One agg's argument across a batch, a text column cast per `argCast`. A stored
    /// column is handed over uncopied when its type feeds the accumulator; others are
    /// built as the type it reads (`count_if` folds BOOLs into an INT).
    fn argColumn(self: *Aggregate, arena: std.mem.Allocator, agg: Agg, e: *const ast.Expr, b: Batch) anyerror!column.Column {
        const want = argCast(agg) orelse agg.ty;
        if (e.* == .field) {
            if (b.schema.resolve(e.field.parts)) |ci| {
                const ck = b.columns[ci].ty.kind;
                const ak = want.kind;
                if (ck == ak or (ck.isNumeric() and ak.isNumeric())) return b.columns[ci];
            }
        }
        const col = eval.evalColumn(arena, e, b, want) catch |err| {
            if (self.err) |ec| ec.set("{s}: in aggregate", .{failLabel(err)});
            return err;
        };
        if (col.ty.kind != .string) return col;
        const to = argCast(agg) orelse return col;
        if (to.kind == .string) return col;
        var out = try column.Builder.initCapacity(arena, to, b.len);
        for (0..b.len) |r| try out.append(try self.castArg(arena, to, col.getValue(r)));
        return out.finish();
    }

    fn argValue(self: *Aggregate, arena: std.mem.Allocator, agg: Agg, b: Batch, r: usize) anyerror!Value {
        const e = agg.arg orelse return .null;
        const v = eval.evalRow(arena, e, b, r) catch |err| {
            if (self.err) |ec| ec.set("{s}: in aggregate", .{failLabel(err)});
            return err;
        };
        if (v != .string) return v;
        const to = argCast(agg) orelse return v;
        if (to.kind == .string) return v;
        return self.castArg(arena, to, v);
    }

    fn castArg(self: *Aggregate, arena: std.mem.Allocator, to: types.Type, v: Value) anyerror!Value {
        if (v.isNull()) return v;
        return eval.castValueTyped(arena, v, to) catch |err| {
            if (self.err) |ec| ec.set("{s}: in aggregate", .{failLabel(err)});
            return err;
        };
    }

    /// Vectorized reduce of one agg over one batch; null means fold row-wise. A null
    /// slot holds whatever its producer left, so only an all-valid batch is summed blind.
    fn reduceBatch(self: *Aggregate, arena: std.mem.Allocator, agg: Agg, b: Batch) anyerror!?Partial {
        if (agg.func == .count and agg.arg == null) return Partial{ .nvalid = b.len };
        if (foldsRowwise(agg.func)) return null;
        const e = agg.arg orelse return null;
        const col = eval.evalColumn(arena, e, b, agg.ty) catch |err| {
            if (self.err) |ec| ec.set("{s}: in aggregate", .{failLabel(err)});
            return err;
        };
        if (col.ty.kind != .int and col.ty.kind != .float) return null;
        const n = b.len;
        const nvalid = simd.popcountValid(col.validity.bits, n);
        var p = Partial{ .nvalid = nvalid };
        if (nvalid == 0) return p;
        switch (agg.func) {
            .count => {},
            .median, .count_if, .bool_and, .bool_or, .bit_and, .bit_or, .bit_xor, .var_samp, .var_pop, .stddev_samp, .stddev_pop => unreachable,
            .sum, .avg => switch (col.ty.kind) {
                .float => p.sum_f = if (nvalid == n) simd.sumF(col.data.f64[0..n]) else validSumF(col, n),
                .int => {
                    p.sum_i = if (nvalid == n) sumIntCol(col.data.i64[0..n]) else validSumI(col, n);
                    p.sum_f = @floatFromInt(p.sum_i);
                },
                else => unreachable,
            },
            .min, .max => p.ext = reduceExtreme(col, agg.func, n),
        }
        return p;
    }

    /// Whether `reduceBatch` leaves an aggregate to the row-wise fold, decided before
    /// evaluating a column it would only discard.
    fn foldsRowwise(func: ast.AggFunc) bool {
        return switch (func) {
            .count, .sum, .avg, .min, .max => false,
            .median, .count_if, .bool_and, .bool_or, .bit_and, .bit_or, .bit_xor, .var_samp, .var_pop, .stddev_samp, .stddev_pop => true,
        };
    }

    /// Fold one batch's `Partial` into the running accumulator, with `updateAcc`'s
    /// semantics.
    fn mergePartial(acc: *Acc, agg: Agg, p: Partial) error{IntOverflow}!void {
        switch (agg.func) {
            .count => acc.n += @intCast(p.nvalid),
            .sum => if (p.nvalid > 0) {
                if (agg.ty.kind == .float)
                    acc.sum_f += p.sum_f
                else
                    acc.sum_i = std.math.add(i128, acc.sum_i, p.sum_i) catch return error.IntOverflow;
                acc.n += @intCast(p.nvalid);
            },
            .avg => if (p.nvalid > 0) {
                acc.sum_f += p.sum_f;
                acc.n += @intCast(p.nvalid);
            },
            .median, .count_if, .bool_and, .bool_or, .bit_and, .bit_or, .bit_xor, .var_samp, .var_pop, .stddev_samp, .stddev_pop => unreachable,
            .min => if (p.ext) |v| {
                if (acc.ext.isNull() or lessV(v, acc.ext)) {
                    acc.ext = v;
                }
            },
            .max => if (p.ext) |v| {
                if (acc.ext.isNull() or lessV(acc.ext, v)) {
                    acc.ext = v;
                }
            },
        }
    }

    /// One output row per group of `sets`, or with `sel` only the groups `sel[i]` lists
    /// for set `i` (a top-N's survivors).
    pub fn emitSets(self: *Aggregate, arena: std.mem.Allocator, sets: []const GroupSet, sel: ?[]const []const u32) anyerror!Batch {
        const nfields = self.out_schema.fields.len;
        var n: usize = 0;
        for (sets, 0..) |*st, si| n += if (sel) |sl| sl[si].len else st.len;
        const builders = try arena.alloc(column.Builder, nfields);
        for (builders, self.out_schema.fields) |*b, f| b.* = try column.Builder.initCapacity(arena, f.ty, @max(n, 1));

        if (n == 0 and self.by.len == 0) {
            for (self.aggs, 0..) |agg, j| try builders[j].append(try finalizeAcc(.{}, agg));
            n = 1;
        } else {
            for (sets, 0..) |*st, si| {
                if (sel) |sl| {
                    for (sl[si]) |gi| try self.emitGroup(builders, st, gi);
                } else {
                    for (0..st.len) |gi| try self.emitGroup(builders, st, gi);
                }
            }
        }

        const cols = try arena.alloc(column.Column, nfields);
        for (builders, 0..) |*b, i| cols[i] = try b.finish();
        return Batch{ .schema = self.out_schema, .columns = cols, .len = n };
    }

    fn emitGroup(self: *Aggregate, builders: []column.Builder, st: *const GroupSet, gi: usize) !void {
        const nk = self.by.len;
        for (builders[0..nk], 0..) |*b, j| try b.append(st.keyValue(gi, j));
        for (self.aggs, builders[nk..], 0..) |agg, *b, j| {
            try b.append(finalizeAcc(st.acc(gi, j), agg) catch |err| {
                if (self.err) |ec| ec.set("{s}: in aggregate", .{failLabel(err)});
                return err;
            });
        }
    }

    /// `state` owns any string extremum copied into the accumulator, since the value
    /// must outlive the per-pull arena.
    pub fn updateAcc(state: std.mem.Allocator, acc: *Acc, agg: Agg, v: Value, has_arg: bool) !void {
        switch (agg.func) {
            .count => {
                if (agg.distinct) {
                    if (!v.isNull()) try noteDistinct(state, acc, v);
                } else if (!has_arg or !v.isNull()) acc.n += 1;
            },
            .sum => if (!v.isNull()) {
                if (agg.ty.kind == .float) acc.sum_f += eval.toF64(v) else try addExact(&acc.sum_i, agg, v);
                acc.n += 1;
            },
            .avg => if (!v.isNull()) {
                acc.sum_f += eval.toF64(v);
                acc.n += 1;
            },
            .median => if (!v.isNull()) try noteMedian(state, acc, eval.toF64(v)),
            .count_if => if (v == .bool and v.bool) {
                acc.n += 1;
            },
            .bool_and, .bool_or => if (!v.isNull()) {
                acc.n += 1;
                if (v == .bool and v.bool) acc.sum_i += 1;
            },
            .bit_and, .bit_or, .bit_xor => if (!v.isNull()) {
                if (v != .int) return error.TypeMismatch;
                acc.sum_i = if (acc.n == 0) v.int else bitFold(agg.func, acc.sum_i, v.int);
                acc.n += 1;
            },
            .var_samp, .var_pop, .stddev_samp, .stddev_pop => if (!v.isNull()) noteMoment(acc, eval.toF64(v)),
            .min => if (!v.isNull()) {
                if (acc.ext.isNull() or lessV(v, acc.ext)) {
                    acc.ext = try dupeValue(state, v);
                }
            },
            .max => if (!v.isNull()) {
                if (acc.ext.isNull() or lessV(acc.ext, v)) {
                    acc.ext = try dupeValue(state, v);
                }
            },
        }
    }

    /// Add a non-null value to an int or DECIMAL SUM, normalized to the output scale.
    /// Adding raw unscaled values multiplied the sum by 10^(declared - actual).
    fn addExact(sum: *align(8) i128, agg: Agg, v: Value) !void {
        if (agg.ty.kind == .decimal) {
            const d: Decimal = if (v == .decimal) v.decimal else .{ .unscaled = v.int, .scale = 0 };
            const r = eval.rescaleTo(d, agg.ty.scale) orelse return error.CastFailed;
            sum.* = std.math.add(i128, sum.*, r.unscaled) catch return error.CastFailed;
        } else sum.* = std.math.add(i128, sum.*, v.int) catch return error.IntOverflow;
    }

    pub const Slot = enum {
        count,
        sum_i,
        sum_f,
        full,

        fn of(agg: Agg) Slot {
            if (agg.distinct) return .full;
            return switch (agg.func) {
                .count => .count,
                .sum => if (agg.ty.kind == .float) .sum_f else .sum_i,
                .avg => .sum_f,
                else => .full,
            };
        }

        fn size(self: Slot) usize {
            return switch (self) {
                .count => @sizeOf(i64),
                .sum_i => @sizeOf(SumI),
                .sum_f => @sizeOf(SumF),
                .full => @sizeOf(Acc),
            };
        }
    };
    const SumI = struct { n: i64, s: i128 align(8) };
    const SumF = struct { n: i64, s: f64 };

    pub const Layout = struct {
        slots: []const Slot,
        offs: []const usize,
        size: usize,
        all_count: bool,

        fn init(a: std.mem.Allocator, aggs: []const Agg) !Layout {
            const slots = try a.alloc(Slot, aggs.len);
            const offs = try a.alloc(usize, aggs.len);
            var at: usize = 0;
            var all_count = true;
            for (aggs, slots, offs) |agg, *sl, *o| {
                sl.* = Slot.of(agg);
                if (sl.* != .count or agg.arg != null) all_count = false;
                o.* = at;
                at += sl.size();
            }
            return .{ .slots = slots, .offs = offs, .size = at, .all_count = all_count };
        }

        fn ptr(self: Layout, comptime T: type, tail: [*]u8, j: usize) *T {
            return @ptrCast(@alignCast(tail + self.offs[j]));
        }

        fn clear(self: Layout, tail: [*]u8) void {
            for (self.slots, 0..) |sl, j| switch (sl) {
                .count => self.ptr(i64, tail, j).* = 0,
                .sum_i => self.ptr(SumI, tail, j).* = .{ .n = 0, .s = 0 },
                .sum_f => self.ptr(SumF, tail, j).* = .{ .n = 0, .s = 0 },
                .full => self.ptr(Acc, tail, j).* = .{},
            };
        }

        fn acc(self: Layout, tail: [*]u8, j: usize) Acc {
            return switch (self.slots[j]) {
                .count => .{ .n = self.ptr(i64, tail, j).* },
                .sum_i => blk: {
                    const x = self.ptr(SumI, tail, j).*;
                    break :blk .{ .n = x.n, .sum_i = x.s };
                },
                .sum_f => blk: {
                    const x = self.ptr(SumF, tail, j).*;
                    break :blk .{ .n = x.n, .sum_f = x.s };
                },
                .full => self.ptr(Acc, tail, j).*,
            };
        }

        fn update(self: Layout, state: std.mem.Allocator, tail: [*]u8, j: usize, agg: Agg, v: Value) !void {
            switch (self.slots[j]) {
                .count => if (agg.arg == null or !v.isNull()) {
                    self.ptr(i64, tail, j).* += 1;
                },
                .sum_i => if (!v.isNull()) {
                    const x = self.ptr(SumI, tail, j);
                    try addExact(&x.s, agg, v);
                    x.n += 1;
                },
                .sum_f => if (!v.isNull()) {
                    const x = self.ptr(SumF, tail, j);
                    x.s += eval.toF64(v);
                    x.n += 1;
                },
                .full => try updateAcc(state, self.ptr(Acc, tail, j), agg, v, agg.arg != null),
            }
        }

        fn merge(self: Layout, alloc: std.mem.Allocator, dst: [*]u8, src: [*]u8, j: usize, agg: Agg) !void {
            switch (self.slots[j]) {
                .count => self.ptr(i64, dst, j).* += self.ptr(i64, src, j).*,
                .sum_i => {
                    const d = self.ptr(SumI, dst, j);
                    const x = self.ptr(SumI, src, j).*;
                    d.s = std.math.add(i128, d.s, x.s) catch return error.IntOverflow;
                    d.n += x.n;
                },
                .sum_f => {
                    const d = self.ptr(SumF, dst, j);
                    const x = self.ptr(SumF, src, j).*;
                    d.s += x.s;
                    d.n += x.n;
                },
                .full => try mergeAcc(alloc, self.ptr(Acc, dst, j), self.ptr(Acc, src, j).*, agg),
            }
        }
    };

    /// The aggregate's final value. MEDIAN uses selection, and with an even count the
    /// mean of the two middle values, as Postgres's percentile_cont(0.5) does.
    pub fn finalizeAcc(acc: Acc, agg: Agg) error{IntOverflow}!Value {
        return switch (agg.func) {
            .count => .{ .int = acc.n },
            .sum => if (acc.n == 0) .null else switch (agg.ty.kind) {
                .float => Value{ .float = acc.sum_f },
                .decimal => Value{ .decimal = .{ .unscaled = acc.sum_i, .scale = agg.ty.scale } },
                else => Value{ .int = std.math.cast(i64, acc.sum_i) orelse return error.IntOverflow },
            },
            .avg => if (acc.n == 0) .null else Value{ .float = acc.sum_f / @as(f64, @floatFromInt(acc.n)) },
            .median => blk: {
                const l = acc.vals orelse break :blk Value.null;
                const xs = l.items;
                if (xs.len == 0) break :blk Value.null;
                const mid = xs.len / 2;
                const hi = selectNth(xs, mid);
                if (xs.len % 2 == 1) break :blk Value{ .float = hi };
                var lo = xs[0];
                for (xs[1..mid]) |x| lo = @max(lo, x);
                break :blk Value{ .float = (lo + hi) / 2 };
            },
            .min, .max => acc.ext,
            .count_if => .{ .int = acc.n },
            .bool_and => if (acc.n == 0) .null else Value{ .bool = acc.sum_i == acc.n },
            .bool_or => if (acc.n == 0) .null else Value{ .bool = acc.sum_i > 0 },
            .bit_and, .bit_or, .bit_xor => if (acc.n == 0) .null else Value{ .int = @intCast(acc.sum_i) },
            .var_samp, .stddev_samp => if (acc.n < 2) .null else spread(agg.func, sqDev(acc) / @as(f64, @floatFromInt(acc.n - 1))),
            .var_pop, .stddev_pop => if (acc.n == 0) .null else spread(agg.func, sqDev(acc) / @as(f64, @floatFromInt(acc.n))),
        };
    }

    fn bitFold(func: ast.AggFunc, a: i128, b: i128) i128 {
        return switch (func) {
            .bit_and => a & b,
            .bit_or => a | b,
            .bit_xor => a ^ b,
            else => unreachable,
        };
    }

    fn sqDev(acc: Acc) f64 {
        return if (acc.ext == .float) acc.ext.float else 0;
    }

    /// Welford's update. `ext` is read before it is assigned: assigning the union may
    /// set its tag first and read back a garbage float.
    fn noteMoment(acc: *Acc, x: f64) void {
        const m2 = sqDev(acc.*);
        acc.n += 1;
        const delta = x - acc.sum_f;
        acc.sum_f += delta / @as(f64, @floatFromInt(acc.n));
        acc.ext = .{ .float = m2 + delta * (x - acc.sum_f) };
    }

    /// Chan et al.'s pairwise combination of two Welford states.
    fn mergeMoments(dst: *Acc, src: Acc) void {
        if (src.n == 0) return;
        if (dst.n == 0) {
            dst.n = src.n;
            dst.sum_f = src.sum_f;
            dst.ext = .{ .float = sqDev(src) };
            return;
        }
        const na: f64 = @floatFromInt(dst.n);
        const nb: f64 = @floatFromInt(src.n);
        const n = na + nb;
        const delta = src.sum_f - dst.sum_f;
        const m2 = sqDev(dst.*) + sqDev(src) + delta * delta * na * nb / n;
        dst.ext = .{ .float = m2 };
        dst.sum_f += delta * nb / n;
        dst.n += src.n;
    }

    fn spread(func: ast.AggFunc, variance: f64) Value {
        return .{ .float = switch (func) {
            .stddev_samp, .stddev_pop => @sqrt(variance),
            else => variance,
        } };
    }
};

pub fn lessV(a: Value, b: Value) bool {
    return (eval.compareValues(a, b) orelse .eq) == .lt;
}

/// Quickselect: `xs[k]` becomes the k-th smallest, smaller before, larger after.
/// Median-of-three pivot, Hoare partition; expected O(n).
fn selectNth(xs: []f64, k: usize) f64 {
    var lo: usize = 0;
    var hi: usize = xs.len - 1;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (xs[mid] < xs[lo]) std.mem.swap(f64, &xs[mid], &xs[lo]);
        if (xs[hi] < xs[lo]) std.mem.swap(f64, &xs[hi], &xs[lo]);
        if (xs[hi] < xs[mid]) std.mem.swap(f64, &xs[hi], &xs[mid]);
        const p = xs[mid];
        var i = lo;
        var j = hi;
        while (i <= j) {
            while (xs[i] < p) i += 1;
            while (p < xs[j]) j -= 1;
            if (i <= j) {
                std.mem.swap(f64, &xs[i], &xs[j]);
                i += 1;
                if (j == 0) break;
                j -= 1;
            }
        }
        if (k <= j) {
            hi = j;
        } else if (k >= i) {
            lo = i;
        } else {
            return xs[k];
        }
    }
    return xs[k];
}

pub fn dupeValue(state: std.mem.Allocator, v: Value) !Value {
    return switch (v) {
        .string => |s| .{ .string = try state.dupe(u8, s) },
        .bytes => |s| .{ .bytes = try state.dupe(u8, s) },
        else => v,
    };
}

/// Sum an i64 column exactly in i128, branch-free, range-checked in `finalizeAcc`.
/// This was `+%`, which returned a plausible wrong total.
pub fn sumIntCol(d: []const i64) i128 {
    var s: i128 = 0;
    for (d) |x| s += x;
    return s;
}

fn validSumI(col: column.Column, n: usize) i128 {
    var s: i128 = 0;
    for (col.data.i64[0..n], 0..) |x, i| {
        if (col.validity.get(i)) s += x;
    }
    return s;
}

fn validSumF(col: column.Column, n: usize) f64 {
    var s: f64 = 0;
    for (col.data.f64[0..n], 0..) |x, i| {
        if (col.validity.get(i)) s += x;
    }
    return s;
}

/// MIN/MAX over an int or float column: SIMD when all valid (a null lane's 0 would
/// corrupt the extreme), else a scalar skip.
fn reduceExtreme(col: column.Column, func: ast.AggFunc, n: usize) Value {
    const is_min = func == .min;
    if (col.ty.kind == .float) {
        const d = col.data.f64[0..n];
        if (col.validity.allSet(n)) {
            return .{ .float = if (is_min) simd.minF(d) else simd.maxF(d) };
        }
        var m: ?f64 = null;
        for (d, 0..) |x, i| {
            if (!col.validity.get(i)) continue;
            m = if (m) |cur| (if (is_min) @min(cur, x) else @max(cur, x)) else x;
        }
        return if (m) |x| Value{ .float = x } else .null;
    }
    var m: ?i64 = null;
    for (col.data.i64[0..n], 0..) |x, i| {
        if (!col.validity.get(i)) continue;
        m = if (m) |cur| (if (is_min) @min(cur, x) else @max(cur, x)) else x;
    }
    return if (m) |x| Value{ .int = x } else .null;
}

test "aggregate: grouped count/sum/avg/min/max skip nulls per group" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const in_schema = types.Schema{ .fields = &.{
        .{ .name = "v", .ty = types.Type.init(.int).asNullable() },
        .{ .name = "k", .ty = types.Type.init(.string).asNullable() },
    } };
    const batches = [_]Batch{
        try kvBatch(a, &in_schema, &.{ 1, 10 }, &.{ "a", "b" }),
        try kvBatch(a, &in_schema, &.{ null, 3, 2 }, &.{ "a", "a", "b" }),
    };
    var ts = TestSource{ .schema_ = in_schema, .batches = &batches };
    var scan = Scan{ .src = ts.src() };

    const fv = ast.Expr{ .field = .{ .parts = &[_][]const u8{"v"} } };
    const aggs = [_]Aggregate.Agg{
        .{ .func = .count, .arg = null, .ty = types.Type.init(.int) },
        .{ .func = .sum, .arg = &fv, .ty = types.Type.init(.int).asNullable() },
        .{ .func = .avg, .arg = &fv, .ty = types.Type.init(.float).asNullable() },
        .{ .func = .min, .arg = &fv, .ty = types.Type.init(.int).asNullable() },
        .{ .func = .max, .arg = &fv, .ty = types.Type.init(.int).asNullable() },
    };
    const out_schema = types.Schema{ .fields = &.{
        .{ .name = "k", .ty = types.Type.init(.string) },
        .{ .name = "c", .ty = types.Type.init(.int) },
        .{ .name = "s", .ty = types.Type.init(.int).asNullable() },
        .{ .name = "av", .ty = types.Type.init(.float).asNullable() },
        .{ .name = "mn", .ty = types.Type.init(.int).asNullable() },
        .{ .name = "mx", .ty = types.Type.init(.int).asNullable() },
    } };
    var agg = Aggregate{
        .child = .{ .scan = &scan },
        .in_schema = &in_schema,
        .by = &.{1},
        .aggs = &aggs,
        .out_schema = &out_schema,
        .state = a,
        .gpa = testing.allocator,
    };
    const b = (try agg.next(a)).?;
    try testing.expectEqual(@as(usize, 2), b.len);
    try testing.expectEqualStrings("a", b.columns[0].getValue(0).string);
    try testing.expectEqual(@as(i64, 3), b.columns[1].getValue(0).int);
    try testing.expectEqual(@as(i64, 4), b.columns[2].getValue(0).int);
    try testing.expectEqual(@as(f64, 2.0), b.columns[3].getValue(0).float);
    try testing.expectEqual(@as(i64, 1), b.columns[4].getValue(0).int);
    try testing.expectEqual(@as(i64, 3), b.columns[5].getValue(0).int);
    try testing.expectEqualStrings("b", b.columns[0].getValue(1).string);
    try testing.expectEqual(@as(i64, 2), b.columns[1].getValue(1).int);
    try testing.expectEqual(@as(i64, 12), b.columns[2].getValue(1).int);
    try testing.expectEqual(@as(f64, 6.0), b.columns[3].getValue(1).float);
    try testing.expectEqual(@as(i64, 2), b.columns[4].getValue(1).int);
    try testing.expectEqual(@as(i64, 10), b.columns[5].getValue(1).int);
}

test "aggregate: sum/avg coerce raw string cells (parallel CSV lane shape); garbage text errors" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const s_schema = types.Schema{ .fields = &.{
        .{ .name = "x", .ty = types.Type.init(.string).asNullable() },
    } };
    const fx = ast.Expr{ .field = .{ .parts = &[_][]const u8{"x"} } };
    const aggs = [_]Aggregate.Agg{
        .{ .func = .sum, .arg = &fx, .ty = types.Type.init(.int).asNullable() },
        .{ .func = .avg, .arg = &fx, .ty = types.Type.init(.float).asNullable() },
    };
    const out_schema = types.Schema{ .fields = &.{
        .{ .name = "s", .ty = types.Type.init(.int).asNullable() },
        .{ .name = "av", .ty = types.Type.init(.float).asNullable() },
    } };

    {
        const batches = [_]Batch{try strBatch(a, &s_schema, &.{ "4", null, "2" })};
        var ts = TestSource{ .schema_ = s_schema, .batches = &batches };
        var scan = Scan{ .src = ts.src() };
        var agg = Aggregate{ .child = .{ .scan = &scan }, .in_schema = &s_schema, .by = &.{}, .aggs = &aggs, .out_schema = &out_schema, .state = a, .gpa = testing.allocator };
        const b = (try agg.next(a)).?;
        try testing.expectEqual(@as(i64, 6), b.columns[0].getValue(0).int);
        try testing.expectEqual(@as(f64, 3.0), b.columns[1].getValue(0).float);
    }
    {
        const batches = [_]Batch{try strBatch(a, &s_schema, &.{ "4", "oops" })};
        var ts = TestSource{ .schema_ = s_schema, .batches = &batches };
        var scan = Scan{ .src = ts.src() };
        var agg = Aggregate{ .child = .{ .scan = &scan }, .in_schema = &s_schema, .by = &.{}, .aggs = &aggs, .out_schema = &out_schema, .state = a, .gpa = testing.allocator };
        try testing.expectError(error.CastFailed, agg.next(a));
    }
}

test "aggregate: variance folded in two halves and merged matches one fold, and holds up far from zero" {
    const A = Aggregate;
    const fx = ast.Expr{ .field = .{ .parts = &[_][]const u8{"x"} } };
    const xs = [_]f64{ 2, 4, 4, 4, 5, 5, 7, 9, 1.5, 12.25 };
    inline for (.{ ast.AggFunc.var_samp, .var_pop, .stddev_samp, .stddev_pop }) |func| {
        const agg = A.Agg{ .func = func, .arg = &fx, .ty = types.Type.init(.float).asNullable() };
        var whole = A.Acc{};
        for (xs) |x| try A.updateAcc(testing.allocator, &whole, agg, .{ .float = x }, true);
        for (0..xs.len + 1) |cut| {
            var lo = A.Acc{};
            var hi = A.Acc{};
            for (xs[0..cut]) |x| try A.updateAcc(testing.allocator, &lo, agg, .{ .float = x }, true);
            for (xs[cut..]) |x| try A.updateAcc(testing.allocator, &hi, agg, .{ .float = x }, true);
            try A.mergeAcc(testing.allocator, &lo, hi, agg);
            try testing.expectApproxEqAbs((try A.finalizeAcc(whole, agg)).float, (try A.finalizeAcc(lo, agg)).float, 1e-9);
        }
    }
    const pop = A.Agg{ .func = .var_pop, .arg = &fx, .ty = types.Type.init(.float).asNullable() };
    const samp = A.Agg{ .func = .var_samp, .arg = &fx, .ty = types.Type.init(.float).asNullable() };
    var p = A.Acc{};
    var q = A.Acc{};
    for (xs[0..8]) |x| {
        try A.updateAcc(testing.allocator, &p, pop, .{ .float = x + 1e9 }, true);
        try A.updateAcc(testing.allocator, &q, samp, .{ .int = @intFromFloat(x) }, true);
    }
    try testing.expectApproxEqAbs(@as(f64, 4), (try A.finalizeAcc(p, pop)).float, 1e-6);
    try testing.expectApproxEqAbs(@as(f64, 32.0 / 7.0), (try A.finalizeAcc(q, samp)).float, 1e-12);
    var one = A.Acc{};
    try A.updateAcc(testing.allocator, &one, samp, .{ .float = 3 }, true);
    try testing.expect((try A.finalizeAcc(one, samp)) == .null);
    try testing.expectEqual(@as(f64, 0), (try A.finalizeAcc(one, pop)).float);
    try testing.expect((try A.finalizeAcc(.{}, pop)) == .null);
}

test "aggregate: bit_and starts from its first value, not zero, through a merge too" {
    const A = Aggregate;
    const fx = ast.Expr{ .field = .{ .parts = &[_][]const u8{"x"} } };
    const agg = A.Agg{ .func = .bit_and, .arg = &fx, .ty = types.Type.init(.int).asNullable() };
    var empty = A.Acc{};
    var some = A.Acc{};
    for ([_]i64{ 0b1110, 0b0111, -1 }) |x| try A.updateAcc(testing.allocator, &some, agg, .{ .int = x }, true);
    try A.updateAcc(testing.allocator, &some, agg, .null, true);
    try A.mergeAcc(testing.allocator, &empty, some, agg);
    try testing.expectEqual(@as(i64, 0b0110), (try A.finalizeAcc(empty, agg)).int);
    var neg = A.Acc{};
    try A.updateAcc(testing.allocator, &neg, agg, .{ .int = std.math.minInt(i64) }, true);
    try testing.expectEqual(@as(i64, std.math.minInt(i64)), (try A.finalizeAcc(neg, agg)).int);
    try testing.expect((try A.finalizeAcc(.{}, agg)) == .null);
}

test "aggregate: a decimal sum normalizes each value's own scale" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const col_ty = types.Type.decimal(38, 6).asNullable();
    const in_schema = types.Schema{ .fields = &.{.{ .name = "n", .ty = col_ty }} };

    var bld = column.Builder.init(a, col_ty);
    try bld.append(.{ .decimal = .{ .unscaled = 15, .scale = 1 } });
    try bld.append(.{ .decimal = .{ .unscaled = 150, .scale = 2 } });
    try bld.append(.{ .decimal = .{ .unscaled = 1, .scale = 3 } });
    try bld.append(.null);
    const cols = try a.alloc(column.Column, 1);
    cols[0] = try bld.finish();
    const batches = [_]Batch{.{ .schema = &in_schema, .columns = cols, .len = 4 }};

    const fx = ast.Expr{ .field = .{ .parts = &[_][]const u8{"n"} } };
    const aggs = [_]Aggregate.Agg{.{ .func = .sum, .arg = &fx, .ty = col_ty }};
    const out_schema = types.Schema{ .fields = &.{.{ .name = "s", .ty = col_ty }} };

    var ts = TestSource{ .schema_ = in_schema, .batches = &batches };
    var scan = Scan{ .src = ts.src() };
    var agg = Aggregate{ .child = .{ .scan = &scan }, .in_schema = &in_schema, .by = &.{}, .aggs = &aggs, .out_schema = &out_schema, .state = a, .gpa = testing.allocator };
    const out = (try agg.next(a)).?;
    const d = out.columns[0].getValue(0).decimal;
    try testing.expectEqual(@as(u8, 6), d.scale);
    try testing.expectEqual(@as(i128, 3_001_000), d.unscaled);
}

test "aggregate: global vectorized reductions honor nulls; empty input edge cases" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const fx = ast.Expr{ .field = .{ .parts = &[_][]const u8{"x"} } };
    const aggs = [_]Aggregate.Agg{
        .{ .func = .count, .arg = null, .ty = types.Type.init(.int) },
        .{ .func = .count, .arg = &fx, .ty = types.Type.init(.int) },
        .{ .func = .sum, .arg = &fx, .ty = types.Type.init(.int).asNullable() },
        .{ .func = .avg, .arg = &fx, .ty = types.Type.init(.float).asNullable() },
        .{ .func = .min, .arg = &fx, .ty = types.Type.init(.int).asNullable() },
        .{ .func = .max, .arg = &fx, .ty = types.Type.init(.int).asNullable() },
    };
    const out_schema = types.Schema{ .fields = &.{
        .{ .name = "c", .ty = types.Type.init(.int) },
        .{ .name = "cv", .ty = types.Type.init(.int) },
        .{ .name = "s", .ty = types.Type.init(.int).asNullable() },
        .{ .name = "av", .ty = types.Type.init(.float).asNullable() },
        .{ .name = "mn", .ty = types.Type.init(.int).asNullable() },
        .{ .name = "mx", .ty = types.Type.init(.int).asNullable() },
    } };

    const batches = [_]Batch{
        try intBatch(a, &int_schema, &.{ 4, null }),
        try intBatch(a, &int_schema, &.{ 2, 9 }),
    };
    var ts = TestSource{ .schema_ = int_schema, .batches = &batches };
    var scan = Scan{ .src = ts.src() };
    var agg = Aggregate{
        .child = .{ .scan = &scan },
        .in_schema = &int_schema,
        .by = &.{},
        .aggs = &aggs,
        .out_schema = &out_schema,
        .state = a,
        .gpa = testing.allocator,
    };
    const b = (try agg.next(a)).?;
    try testing.expectEqual(@as(usize, 1), b.len);
    try testing.expectEqual(@as(i64, 4), b.columns[0].getValue(0).int);
    try testing.expectEqual(@as(i64, 3), b.columns[1].getValue(0).int);
    try testing.expectEqual(@as(i64, 15), b.columns[2].getValue(0).int);
    try testing.expectEqual(@as(f64, 5.0), b.columns[3].getValue(0).float);
    try testing.expectEqual(@as(i64, 2), b.columns[4].getValue(0).int);
    try testing.expectEqual(@as(i64, 9), b.columns[5].getValue(0).int);

    var ets = TestSource{ .schema_ = int_schema, .batches = &.{} };
    var escan = Scan{ .src = ets.src() };
    var eagg = Aggregate{
        .child = .{ .scan = &escan },
        .in_schema = &int_schema,
        .by = &.{},
        .aggs = &aggs,
        .out_schema = &out_schema,
        .state = a,
        .gpa = testing.allocator,
    };
    const eb = (try eagg.next(a)).?;
    try testing.expectEqual(@as(usize, 1), eb.len);
    try testing.expectEqual(@as(i64, 0), eb.columns[0].getValue(0).int);
    try testing.expect(eb.columns[2].getValue(0).isNull());
    try testing.expect(eb.columns[4].getValue(0).isNull());

    var gts = TestSource{ .schema_ = int_schema, .batches = &.{} };
    var gscan = Scan{ .src = gts.src() };
    var gagg = Aggregate{
        .child = .{ .scan = &gscan },
        .in_schema = &int_schema,
        .by = &.{0},
        .aggs = &aggs,
        .out_schema = &out_schema,
        .state = a,
        .gpa = testing.allocator,
    };
    try testing.expect((try gagg.next(a)) == null);
}

test "integer SUM: exact across batches, an error only when the total leaves i64" {
    const big: i64 = 6148914691236517205;
    const agg = Aggregate.Agg{ .func = .sum, .arg = null, .ty = types.Type.init(.int) };
    try testing.expectError(error.IntOverflow, Aggregate.finalizeAcc(.{ .n = 3, .sum_i = sumIntCol(&[_]i64{ big, big, big }) }, agg));
    try testing.expectError(error.IntOverflow, Aggregate.finalizeAcc(.{ .n = 2, .sum_i = sumIntCol(&[_]i64{ std.math.minInt(i64), -1 }) }, agg));
    try testing.expectEqual(@as(i128, 6), sumIntCol(&[_]i64{ 1, 2, 3 }));

    const max = std.math.maxInt(i64);
    var acc = Aggregate.Acc{ .n = 5 };
    for ([_][]const i64{ &.{ max, max }, &.{ -max, -max }, &.{5} }) |part| acc.sum_i += sumIntCol(part);
    try testing.expectEqual(Value{ .int = 5 }, try Aggregate.finalizeAcc(acc, agg));
}
