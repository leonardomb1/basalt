//! The top-k core behind `ORDER BY … LIMIT` (`TopN`) and a lone `ROW_NUMBER()` under
//! `WHERE rn <= k` (`Window.top_k`): per group, a partition key or the one group of a
//! LIMIT, the best `k` rows on the order keys, equal rows ranked by input position
//! (`seq`) as a stable sort ranks them.
//!
//! A candidate is an `Entry`: the first order key as `sort.leadKey` packs it, its
//! `seq`, and where its row is kept. Entries compare on that head, then on the other
//! keys through `KeyArr.orderAcross`, then on `seq`, so no two are equal. A group's
//! buffer grows to `cap` (2k), allocated once at that size when it is small, as one
//! per partition of many is; a quickselect then keeps the best
//! k and the k-th becomes the group's threshold, PostgreSQL's bounded sort pruned at
//! the boundary as DuckDB's and Trino's top-N do. The worst of the first k is the
//! threshold until then. A row that does not rank before its group's threshold is
//! rejected off its batch's key column, before anything is copied; the rest are
//! copied once, column at a time, into a chunk. When the rows held pass twice the
//! live entries, the live ones are gathered into one chunk. The finish sorts each
//! group's survivors.
//!
//! Groups are found through the shared key table (`keytable`) as GROUP BY and
//! DISTINCT find theirs: encoded keys in a `FixedStore`, boxed ones in a `GroupStore`,
//! the store's slot the group's index.

const Batch = @import("../batch.zig").Batch;
const KeyArr = @import("sort.zig").KeyArr;
const Sort = @import("sort.zig").Sort;
const Threshold = @import("../value.zig").Threshold;
const Value = @import("../value.zig").Value;
const column = @import("../column.zig");
const keyhash = @import("../keyhash.zig");
const keytable = @import("../keytable.zig");
const leadExact = @import("sort.zig").leadExact;
const leadKey = @import("sort.zig").leadKey;
const msdSort = @import("sort.zig").msdSort;
const sortIdx = @import("sort.zig").sortIdx;
const std = @import("std");
const types = @import("../../lang/types.zig");
const testing = std.testing;

pub const Entry = struct { head: u128, seq: u64, chunk: u32, row: u32 };

pub const TopK = struct {
    gpa: std.mem.Allocator,
    schema: *const types.Schema,
    keys: []const Sort.Key,
    rest: []const Sort.Key,
    part: []const usize,
    k: usize,
    floor: usize = 0,
    compact_floor: usize = 1 << 16,
    slice: usize = 4096,
    threshold: ?*Threshold = null,
    state: std.heap.ArenaAllocator,
    store: std.heap.ArenaAllocator,
    chunks: std.ArrayListUnmanaged(Chunk) = .empty,
    groups: std.ArrayListUnmanaged(Group) = .empty,
    lookup: ?Lookup = null,
    strs: ?*keytable.StrTable = null,
    str_cache: std.StringHashMapUnmanaged(keytable.StrId) = .empty,
    stored: usize = 0,
    live: usize = 0,

    const Chunk = struct { cols: []column.Column, karr: []KeyArr };
    const Group = struct {
        buf: std.ArrayListUnmanaged(Entry) = .empty,
        thr_at: u32 = no_thr,

        fn bound(g: *const Group) ?Entry {
            return if (g.thr_at == no_thr) null else g.buf.items[g.thr_at];
        }
    };
    const no_thr = std.math.maxInt(u32);
    const exact_cap = 64;
    const FixedStore = keytable.FixedStore(keytable.NoPayload);
    const BoxedStore = keytable.GroupStore(keytable.NoPayload);
    const no_payload = keytable.NoPayload{};
    const Lookup = struct {
        enc: ?keytable.Encoder,
        table: keytable.GroupTable,
        store: union(enum) { fixed: *FixedStore, boxed: *BoxedStore },
    };

    pub fn init(gpa: std.mem.Allocator, schema: *const types.Schema, keys: []const Sort.Key, part: []const usize, k: usize) TopK {
        const exact = keys.len > 0 and leadExact(schema.fields[keys[0].idx].ty.kind);
        return .{
            .gpa = gpa,
            .schema = schema,
            .keys = keys,
            .rest = if (exact) keys[1..] else keys,
            .part = part,
            .k = k,
            .state = .init(gpa),
            .store = .init(gpa),
        };
    }

    pub fn deinit(self: *TopK) void {
        if (self.lookup) |*l| l.table.deinit();
        self.groups.deinit(self.gpa);
        self.store.deinit();
        self.state.deinit();
    }

    /// A group's buffer limit: 2k, as PostgreSQL's bounded sort keeps, or k + 1 for a
    /// k small enough that selecting over k + 1 entries costs no more than a compare
    /// or two, which leaves each of many small partitions holding k entries, not 2k.
    fn cap(self: *const TopK) usize {
        const c = if (self.k <= small_k) self.k + 1 else self.k *| 2;
        return @max(c, self.k +| self.floor);
    }

    const small_k = 8;

    /// Offers `b`'s rows, row `r` at input position `seqs[r]`, or `seq0 + r` without
    /// `seqs`. `scratch` holds what lives only as long as `b`.
    pub fn add(self: *TopK, scratch: std.mem.Allocator, b: Batch, seq0: u64, seqs: ?[]const u64) !void {
        const n = b.len;
        if (n == 0 or self.k == 0) return;
        var p = Pass{
            .b = b,
            .k0 = if (self.keys.len == 0) null else try KeyArr.prepare(scratch, b.columns[self.keys[0].idx], self.keys[0]),
            .gid = try self.groupIds(scratch, b),
            .seq0 = seq0,
            .seqs = seqs,
            .cand = try scratch.alloc(usize, @min(n, self.slice)),
            .heads = try scratch.alloc(u128, @min(n, self.slice)),
        };
        var at: usize = 0;
        while (at < n) : (at += self.slice) {
            const end = @min(n, at + self.slice);
            const nc = if (self.part.len == 0) try self.prune(false, scratch, &p, at, end) else try self.prune(true, scratch, &p, at, end);
            if (nc == 0) continue;
            const ci = try self.addChunk(b, p.cand[0..nc]);
            for (p.cand[0..nc], p.heads[0..nc], 0..) |r, h, i| {
                const seq = if (seqs) |s| s[r] else seq0 + r;
                try self.offer(if (p.gid.len == 0) 0 else p.gid[r], .{ .head = h, .seq = seq, .chunk = ci, .row = @intCast(i) });
            }
        }
        if (self.stored > 2 * self.live + self.compact_floor) try self.compact(scratch);
    }

    const Pass = struct {
        b: Batch,
        k0: ?KeyArr,
        gid: []const u32,
        seq0: u64,
        seqs: ?[]const u64,
        cand: []usize,
        heads: []u128,
        rest: ?[]KeyArr = null,
    };

    /// Lists in `p.cand`, with their heads in `p.heads`, the rows `[from, to)` of `p.b`
    /// that rank before their group's threshold; their count. A batch is offered a
    /// slice at a time, so a threshold set by one slice already prunes the next.
    fn prune(self: *TopK, comptime grouped: bool, scratch: std.mem.Allocator, p: *Pass, from: usize, to: usize) !usize {
        const k0 = p.k0 orelse return self.pruneAs(grouped, null, scratch, p, from, to);
        return switch (k0.data) {
            inline else => |_, tag| self.pruneAs(grouped, tag, scratch, p, from, to),
        };
    }

    /// `prune` with the first key's storage, `tag`, fixed outside the loop.
    fn pruneAs(self: *TopK, comptime grouped: bool, comptime tag: ?std.meta.Tag(@FieldType(KeyArr, "data")), scratch: std.mem.Allocator, p: *Pass, from: usize, to: usize) !usize {
        var nc: usize = 0;
        const groups = self.groups.items;
        for (from..to) |r| {
            const h: u128 = if (tag == null) 0 else leadKey(p.k0.?, r);
            const g = &groups[if (grouped) p.gid[r] else 0];
            if (g.bound()) |thr| {
                if (h > thr.head) continue;
                if (h == thr.head) {
                    if (p.rest == null) p.rest = try self.prepare(scratch, p.b.columns);
                    const seq = if (p.seqs) |s| s[r] else p.seq0 + r;
                    if (self.rowOrder(p.rest.?, r, seq, thr) != .lt) continue;
                }
            }
            p.cand[nc] = r;
            p.heads[nc] = h;
            nc += 1;
        }
        return nc;
    }

    /// Each row's group, made when its key is new; none listed without partition keys,
    /// whose one group is 0.
    fn groupIds(self: *TopK, scratch: std.mem.Allocator, b: Batch) ![]const u32 {
        if (self.part.len == 0) {
            if (self.groups.items.len == 0) try self.groups.append(self.gpa, .{});
            return &.{};
        }
        const gid = try scratch.alloc(u32, b.len);
        const l = try self.lookupOf();
        const hashes = try scratch.alloc(u64, b.len);
        switch (l.store) {
            .fixed => |st| {
                const e = try l.enc.?.encode(scratch, b.columns, b.len, self.interner(), false);
                for (hashes, 0..) |*h, r| h.* = e.hash(r);
                var v = GroupIds(FixedStore){ .self = self, .tb = &l.table, .st = st, .gid = gid };
                try keytable.probe(FixedStore, .dupe, &keytable.Runs(i64).over(e.nk, &.{}, hashes, e.keys, e.masks), 0, b.len, &v);
            },
            .boxed => |st| {
                const nk = self.part.len;
                const probes = try scratch.alloc(Value, b.len * nk);
                keytable.hashBatch(b.columns, self.part, b.len, probes, hashes);
                var v = GroupIds(BoxedStore){ .self = self, .tb = &l.table, .st = st, .gid = gid };
                try keytable.probe(BoxedStore, .dupe, &keytable.Runs(Value).over(nk, &.{}, hashes, probes, &.{}), 0, b.len, &v);
            },
        }
        return gid;
    }

    /// `groupIds`' side of `keytable.probe`: each row's group, a new key's made.
    fn GroupIds(comptime Store: type) type {
        return struct {
            self: *TopK,
            tb: *keytable.GroupTable,
            st: *Store,
            gid: []u32,

            pub inline fn hit(c: *@This(), r: usize, _: u64, _: anytype, x: keytable.Hit(Store)) !void {
                if (!x.found) try c.self.groups.append(c.self.gpa, .{});
                c.gid[r] = x.slot;
            }
        };
    }

    fn lookupOf(self: *TopK) !*Lookup {
        if (self.lookup) |*l| return l;
        const sa = self.state.allocator();
        const kinds = try sa.alloc(keyhash.KeyKind, self.part.len);
        var fixed = self.part.len <= keyhash.max_fixed_keys;
        for (self.part, kinds) |ci, *kk| kk.* = keyhash.keyKindOf(self.schema.fields[ci].ty.kind) orelse blk: {
            fixed = false;
            break :blk .i64k;
        };
        var table = try keytable.GroupTable.init(self.gpa, 16);
        errdefer table.deinit();
        self.lookup = .{
            .enc = if (fixed) try keytable.Encoder.init(sa, self.part, kinds) else null,
            .table = table,
            .store = if (fixed)
                .{ .fixed = try newIn(sa, FixedStore.init(sa, self.part.len, &no_payload)) }
            else
                .{ .boxed = try newIn(sa, BoxedStore.init(sa, self.part.len, &no_payload)) },
        };
        return &self.lookup.?;
    }

    fn interner(self: *TopK) keytable.Interner {
        return .{ .table = &self.strs, .cache = &self.str_cache, .alloc = self.state.allocator() };
    }

    /// The order keys after the head, prepared over `cols`.
    fn prepare(self: *const TopK, a: std.mem.Allocator, cols: []const column.Column) ![]KeyArr {
        const out = try a.alloc(KeyArr, self.rest.len);
        for (self.rest, out) |k, *o| o.* = try KeyArr.prepare(a, cols[k.idx], k);
        return out;
    }

    /// Rows `rows` of `b` copied into a new chunk; its index. Dictionary views are
    /// left behind: a chunk may hold a handful of rows, and a copy of the dictionary
    /// per chunk would outweigh them in a store that lives for the whole input.
    fn addChunk(self: *TopK, b: Batch, rows: []const usize) !u32 {
        const a = self.store.allocator();
        const cols = try a.alloc(column.Column, b.columns.len);
        for (cols, b.columns) |*o, c| {
            var plain = c;
            plain.dict = null;
            o.* = try column.permute(a, plain, rows);
        }
        try self.chunks.append(self.state.allocator(), .{ .cols = cols, .karr = try self.prepare(a, cols) });
        self.stored += rows.len;
        return @intCast(self.chunks.items.len - 1);
    }

    fn offer(self: *TopK, gi: u32, e: Entry) !void {
        const g = &self.groups.items[gi];
        if (g.bound()) |thr| if (self.order(e, thr) != .lt) return;
        if (g.buf.capacity == 0 and self.cap() <= exact_cap) try g.buf.ensureTotalCapacityPrecise(self.state.allocator(), self.cap());
        try g.buf.append(self.state.allocator(), e);
        self.live += 1;
        const len = g.buf.items.len;
        if (g.thr_at == no_thr and len == self.k) {
            var w: usize = 0;
            for (g.buf.items[1..], 1..) |x, i| {
                if (self.order(x, g.buf.items[w]) == .gt) w = i;
            }
            self.setThreshold(g, w);
        } else if (len >= self.cap()) {
            self.select(g.buf.items, self.k - 1);
            g.buf.shrinkRetainingCapacity(self.k);
            self.live -= len - self.k;
            self.setThreshold(g, self.k - 1);
        }
    }

    /// Makes entry `at` the group's bound, published for a source to skip by when the
    /// group is a LIMIT's. Null, string and bytes bounds are not published.
    fn setThreshold(self: *TopK, g: *Group, at: usize) void {
        g.thr_at = @intCast(at);
        const thr = g.buf.items[at];
        const t = self.threshold orelse return;
        if (self.keys.len == 0) return;
        const v = self.chunks.items[thr.chunk].cols[self.keys[0].idx].getValue(thr.row);
        if (v == .null or v == .string or v == .bytes) return;
        t.value = v;
        t.full = true;
    }

    fn order(self: *const TopK, a: Entry, b: Entry) std.math.Order {
        if (a.head != b.head) return std.math.order(a.head, b.head);
        if (self.rest.len > 0) {
            const ka = self.chunks.items[a.chunk].karr;
            const kb = self.chunks.items[b.chunk].karr;
            for (ka, kb) |x, y| {
                const o = x.orderAcross(a.row, y, b.row);
                if (o != .eq) return o;
            }
        }
        return std.math.order(a.seq, b.seq);
    }

    /// `order` of row `r` of a batch, its keys after the head prepared as `bk`, and
    /// entry `t`, their heads equal.
    fn rowOrder(self: *const TopK, bk: []const KeyArr, r: usize, seq: u64, t: Entry) std.math.Order {
        for (bk, self.chunks.items[t.chunk].karr) |x, y| {
            const o = x.orderAcross(r, y, t.row);
            if (o != .eq) return o;
        }
        return std.math.order(seq, t.seq);
    }

    fn lessThan(self: *const TopK, a: Entry, b: Entry) bool {
        return self.order(a, b) == .lt;
    }

    /// Reorders `items` so position `nth` holds the entry a sort would put there, the
    /// better ones before it: quickselect on a median-of-three pivot, a sort past a
    /// depth budget. Entries are all distinct, so a partition never stalls on ties.
    fn select(self: *const TopK, items: []Entry, nth: usize) void {
        var lo: usize = 0;
        var hi: usize = items.len;
        var budget: usize = 2 * std.math.log2_int(usize, @max(items.len, 2)) + 4;
        while (hi - lo > 16) {
            if (budget == 0) break;
            budget -= 1;
            const mid = lo + (hi - lo) / 2;
            if (self.lessThan(items[mid], items[lo])) std.mem.swap(Entry, &items[mid], &items[lo]);
            if (self.lessThan(items[hi - 1], items[lo])) std.mem.swap(Entry, &items[hi - 1], &items[lo]);
            if (self.lessThan(items[mid], items[hi - 1])) std.mem.swap(Entry, &items[mid], &items[hi - 1]);
            const pivot = items[hi - 1];
            var store = lo;
            for (lo..hi - 1) |i| {
                if (self.lessThan(items[i], pivot)) {
                    std.mem.swap(Entry, &items[i], &items[store]);
                    store += 1;
                }
            }
            std.mem.swap(Entry, &items[store], &items[hi - 1]);
            if (store == nth) return;
            if (nth < store) hi = store else lo = store + 1;
        }
        std.sort.pdq(Entry, items[lo..hi], self, lessThan);
    }

    /// Gathers every live entry's row into one chunk, freeing the rows dropped.
    fn compact(self: *TopK, scratch: std.mem.Allocator) !void {
        const refs = try scratch.alloc(Entry, self.live);
        var i: usize = 0;
        for (self.groups.items) |g| for (g.buf.items) |e| {
            refs[i] = e;
            i += 1;
        };
        var fresh = std.heap.ArenaAllocator.init(self.gpa);
        errdefer fresh.deinit();
        const a = fresh.allocator();
        const cols = try a.alloc(column.Column, self.schema.fields.len);
        for (cols, 0..) |*o, ci| o.* = try self.take(a, refs, ci);
        const chunk = Chunk{ .cols = cols, .karr = try self.prepare(a, cols) };
        try self.chunks.ensureTotalCapacity(self.state.allocator(), 1);
        i = 0;
        for (self.groups.items) |*g| {
            for (g.buf.items) |*e| {
                e.chunk = 0;
                e.row = @intCast(i);
                i += 1;
            }
        }
        self.store.deinit();
        self.store = fresh;
        self.chunks.clearRetainingCapacity();
        self.chunks.appendAssumeCapacity(chunk);
        self.stored = refs.len;
    }

    /// Column `ci` of the rows `refs` name, in that order, typed as the chunks hold it.
    fn take(self: *const TopK, a: std.mem.Allocator, refs: []const Entry, ci: usize) !column.Column {
        const srcs = try a.alloc(column.Column, self.chunks.items.len);
        defer a.free(srcs);
        for (srcs, self.chunks.items) |*s, c| s.* = c.cols[ci];
        return column.takeAcross(a, srcs, refs);
    }

    /// Sorts `items`: when the head is the whole key, a large run by `sort.msdSort` on
    /// the head's two words and `seq`, else by `order`.
    fn sort(self: *const TopK, items: []Entry) !void {
        if (self.rest.len > 0 or items.len < 1 << 10) return std.sort.pdq(Entry, items, self, lessThan);
        var tmp = std.heap.ArenaAllocator.init(self.gpa);
        defer tmp.deinit();
        const a = tmp.allocator();
        const recs = try a.alloc(u64, items.len * 4);
        for (items, 0..) |e, i| recs[i * 4 ..][0..4].* = .{ @intCast(e.head >> 64), @truncate(e.head), e.seq, i };
        msdSort(recs, try a.alloc(u64, recs.len), 4, 0);
        const was = try a.dupe(Entry, items);
        for (items, 0..) |*o, i| o.* = was[@intCast(recs[i * 4 + 3])];
    }

    /// Each group's best `k` entries, best first; with partition keys the groups in
    /// ascending order of their key, nulls last. Valid until `deinit`.
    pub fn finish(self: *TopK, arena: std.mem.Allocator) ![]const []const Entry {
        const groups = self.groups.items;
        const out = try arena.alloc([]const Entry, groups.len);
        for (groups, out) |*g, *o| {
            const n = @min(g.buf.items.len, self.k);
            if (n < g.buf.items.len) self.select(g.buf.items, n - 1);
            try self.sort(g.buf.items[0..n]);
            o.* = g.buf.items[0..n];
        }
        if (self.part.len == 0 or groups.len < 2) return out;
        const reps = try arena.alloc(Entry, groups.len);
        for (reps, out) |*r, g| r.* = g[0];
        const arrs = try arena.alloc(KeyArr, self.part.len);
        for (self.part, arrs) |ci, *k| k.* = try KeyArr.prepare(arena, try self.take(arena, reps, ci), .{ .idx = ci, .desc = false, .nulls_first = false });
        const idx = try arena.alloc(usize, groups.len);
        for (idx, 0..) |*x, i| x.* = i;
        try sortIdx(arena, idx, arrs);
        const sorted = try arena.alloc([]const Entry, groups.len);
        for (sorted, idx) |*s, i| s.* = out[i];
        return sorted;
    }

    /// The rows `entries` name as columns of `fields`, in that order.
    pub fn columns(self: *const TopK, arena: std.mem.Allocator, entries: []const Entry, fields: []const types.Schema.Field, out: []column.Column) !void {
        for (fields, out, 0..) |f, *o, ci| {
            const tag = if (entries.len > 0) std.meta.activeTag(self.chunks.items[0].cols[ci].data) else null;
            if (tag != null and (tag == column.storageOf(f.ty.kind) or tag == .d64)) {
                o.* = try self.take(arena, entries, ci);
                o.ty = f.ty;
                continue;
            }
            var bd = try column.Builder.initCapacity(arena, f.ty, entries.len);
            for (entries) |e| try bd.append(self.chunks.items[e.chunk].cols[ci].getValue(e.row));
            o.* = try bd.finish();
        }
    }
};

fn newIn(a: std.mem.Allocator, v: anytype) !*@TypeOf(v) {
    const p = try a.create(@TypeOf(v));
    p.* = v;
    return p;
}

const keyOrder = @import("sort.zig").keyOrder;
const rowOrder = @import("sort.zig").rowOrder;

const test_schema = types.Schema{ .fields = &.{
    .{ .name = "id", .ty = types.Type.init(.int) },
    .{ .name = "p", .ty = types.Type.init(.int).asNullable() },
    .{ .name = "s", .ty = types.Type.init(.string).asNullable() },
    .{ .name = "f", .ty = types.Type.init(.float).asNullable() },
    .{ .name = "d", .ty = types.Type.decimal(10, 2).asNullable() },
    .{ .name = "q", .ty = types.Type.init(.string).asNullable() },
} };

fn randomRow(rnd: std.Random, id: i64, out: []Value) void {
    const strs = [_][]const u8{ "", "a", "ab", "abcdefghij", "abcdefghik", "abcdefgh", "b" };
    const floats = [_]f64{ std.math.nan(f64), -0.0, 0.0, 1.5, -2, std.math.inf(f64) };
    const decs = [_]Value{ .{ .decimal = .{ .unscaled = 10, .scale = 1 } }, .{ .decimal = .{ .unscaled = 100, .scale = 2 } }, .{ .decimal = .{ .unscaled = 5, .scale = 1 } }, .{ .decimal = .{ .unscaled = -3, .scale = 0 } } };
    const parts = [_][]const u8{ "x", "y", "zz" };
    out[0] = .{ .int = id };
    out[1] = if (rnd.uintLessThan(u8, 8) == 0) .null else .{ .int = rnd.intRangeLessThan(i64, 0, 4) };
    out[2] = if (rnd.uintLessThan(u8, 8) == 0) .null else .{ .string = strs[rnd.uintLessThan(usize, strs.len)] };
    out[3] = if (rnd.uintLessThan(u8, 8) == 0) .null else .{ .float = floats[rnd.uintLessThan(usize, floats.len)] };
    out[4] = if (rnd.uintLessThan(u8, 8) == 0) .null else decs[rnd.uintLessThan(usize, decs.len)];
    out[5] = if (rnd.uintLessThan(u8, 8) == 0) .null else .{ .string = parts[rnd.uintLessThan(usize, parts.len)] };
}

fn testBatch(a: std.mem.Allocator, rows: []const []Value) !Batch {
    const cols = try a.alloc(column.Column, test_schema.fields.len);
    for (cols, test_schema.fields, 0..) |*c, f, ci| {
        var bd = column.Builder.init(a, f.ty);
        for (rows) |r| try bd.append(r[ci]);
        c.* = try bd.finish();
    }
    return .{ .schema = &test_schema, .columns = cols, .len = rows.len };
}

const RefCtx = struct {
    keys: []const Sort.Key,
    part: []const usize,
    fn partOrder(c: RefCtx, a: []const Value, b: []const Value) std.math.Order {
        for (c.part) |ci| {
            const o = keyOrder(a[ci], b[ci], false, false);
            if (o != .eq) return o;
        }
        return .eq;
    }
    fn less(c: RefCtx, a: []Value, b: []Value) bool {
        const p = c.partOrder(a, b);
        if (p != .eq) return p == .lt;
        return rowOrder(c.keys, a, b) == .lt;
    }
};

test "top-k core: each group's best k as a stable sort ranks them, over every key kind, ties, nulls, NaN and both directions" {
    var prng = std.Random.DefaultPrng.init(0x7095);
    const rnd = prng.random();
    const K = Sort.Key;
    const key_sets = [_][]const K{
        &.{.{ .idx = 1, .desc = false, .nulls_first = false }},
        &.{.{ .idx = 3, .desc = true, .nulls_first = true }},
        &.{ .{ .idx = 3, .desc = false, .nulls_first = false }, .{ .idx = 2, .desc = true, .nulls_first = false } },
        &.{.{ .idx = 2, .desc = false, .nulls_first = true }},
        &.{ .{ .idx = 4, .desc = true, .nulls_first = false }, .{ .idx = 1, .desc = false, .nulls_first = true } },
        &.{},
    };
    const part_sets = [_][]const usize{ &.{}, &.{1}, &.{5}, &.{ 5, 3 }, &.{4}, &.{0} };
    const ks = [_]usize{ 0, 1, 3, 7, 40, 5000 };
    for (0..72) |round| {
        var ar = std.heap.ArenaAllocator.init(testing.allocator);
        defer ar.deinit();
        const a = ar.allocator();
        const keys = key_sets[round % key_sets.len];
        const part = part_sets[(round / key_sets.len) % part_sets.len];
        const k = ks[rnd.uintLessThan(usize, ks.len)];
        const n = if (round % 4 == 0) 2500 else rnd.uintLessThan(usize, 600) + 1;
        const rows = try a.alloc([]Value, n);
        for (rows, 0..) |*r, i| {
            r.* = try a.alloc(Value, test_schema.fields.len);
            randomRow(rnd, @intCast(i), r.*);
        }

        var core = TopK.init(testing.allocator, &test_schema, keys, part, k);
        defer core.deinit();
        core.floor = rnd.uintLessThan(usize, 4);
        core.compact_floor = rnd.uintLessThan(usize, 8);
        core.slice = rnd.uintLessThan(usize, 16) + 1;
        var at: usize = 0;
        while (at < n) {
            const len = @min(n - at, rnd.uintLessThan(usize, 40) + 1);
            var scratch = std.heap.ArenaAllocator.init(testing.allocator);
            defer scratch.deinit();
            try core.add(scratch.allocator(), try testBatch(scratch.allocator(), rows[at..][0..len]), at, null);
            at += len;
        }
        const groups = try core.finish(a);
        var got = std.array_list.Managed(i64).init(a);
        for (groups) |g| {
            const cols = try a.alloc(column.Column, test_schema.fields.len);
            try core.columns(a, g, test_schema.fields, cols);
            for (0..g.len) |i| try got.append(cols[0].data.i64[i]);
        }

        const ref = RefCtx{ .keys = keys, .part = part };
        std.sort.insertion([]Value, rows, ref, RefCtx.less);
        var want = std.array_list.Managed(i64).init(a);
        var rank: usize = 0;
        for (rows, 0..) |r, i| {
            rank = if (i > 0 and ref.partOrder(rows[i - 1], r) == .eq) rank + 1 else 1;
            if (rank <= k) try want.append(r[0].int);
        }
        try testing.expectEqualSlices(i64, want.items, got.items);
    }
}

test "top-k core: explicit positions rank lanes' rows as one input would" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var prng = std.Random.DefaultPrng.init(42);
    const rnd = prng.random();
    const rows = try a.alloc([]Value, 300);
    for (rows, 0..) |*r, i| {
        r.* = try a.alloc(Value, test_schema.fields.len);
        randomRow(rnd, @intCast(i), r.*);
    }
    const keys = &[_]Sort.Key{.{ .idx = 1, .desc = true, .nulls_first = false }};
    var core = TopK.init(testing.allocator, &test_schema, keys, &.{}, 25);
    defer core.deinit();
    core.floor = 1;
    core.compact_floor = 0;
    var lane: usize = 3;
    while (lane > 0) {
        lane -= 1;
        const part = rows[lane * 100 ..][0..100];
        const seqs = try a.alloc(u64, part.len);
        for (seqs, 0..) |*s, i| s.* = (@as(u64, lane) << 40) + i;
        try core.add(a, try testBatch(a, part), 0, seqs);
    }
    const g = (try core.finish(a))[0];
    const cols = try a.alloc(column.Column, test_schema.fields.len);
    try core.columns(a, g, test_schema.fields, cols);
    const ref = RefCtx{ .keys = keys, .part = &.{} };
    std.sort.insertion([]Value, rows, ref, RefCtx.less);
    for (rows[0..25], cols[0].data.i64[0..25]) |r, id| try testing.expectEqual(r[0].int, id);
}
