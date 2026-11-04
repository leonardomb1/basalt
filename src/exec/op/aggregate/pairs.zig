//! DISTINCT aggregates as a filter in front of the aggregate's own kernel: each one's
//! (group, value) pairs in a key table of its own, under the encodings and `keyhash`
//! rules the group keys use, a NULL never entering. A fold only records pairs; once
//! every row is in, `apply` adds each pair's value to its group's state, so the
//! kernel sees each distinct value of a group once.
//!
//! A group is named by its ordinal in its store (`Layout.ordOf`). Grouped pairs sit
//! in their group's radix partition, so lanes merge a partition at a time and a
//! merge re-keys a source's pairs by the ordinal its group got there (`absorb`). A
//! global aggregate's pairs are partitioned by the value's hash instead, a string's
//! by its text, so each partition unions on its own and the partitions' states fold
//! in partition order.
//!
//! A global aggregate over an INT-like argument whose result the order of its
//! values cannot change (`orderFree`) keeps its values as bits instead
//! (`bits.zig`): one bit set a row, the lanes' sets unioned by OR. Its partitions
//! are value ranges, not hashes, so a set that outgrows its bits is turned into
//! pairs (`toPairs`), and once any lane's set is pairs the merge turns them all
//! before it unions (`GroupMerge.settleGlobal`).

const GroupStore = keytable.GroupStore(keytable.NoPayload);
const FixedStore = keytable.FixedStore(keytable.NoPayload);
const GroupTable = keytable.GroupTable;
const Kernel = @import("kernel.zig").Kernel;
const Layout = @import("kernel.zig").Layout;
const Value = @import("../../value.zig").Value;
const column = @import("../../column.zig");
const eval = @import("../../eval.zig");
const keyhash = @import("../../keyhash.zig");
const keytable = @import("../../keytable.zig");
const ast = @import("../../../lang/ast.zig");
const Bits = @import("bits.zig").Bits;
const std = @import("std");
const types = @import("../../../lang/types.zig");

const no_payload = keytable.NoPayload{};
const lane_table = @import("drain.zig").lane_table;

pub const Pairs = struct {
    j: usize,
    ty: types.Type,
    kind: ?keyhash.KeyKind,
    grouped: bool,
    parts: []Part,
    alloc: std.mem.Allocator,
    seen: Seen = .{},
    enc: ?keytable.Encoder = null,
    bits: ?Bits = null,
    gpa: std.mem.Allocator,

    const Seen = struct { id: u64 = 0, bits: []u64 = &.{} };

    pub const Part = struct {
        table: GroupTable,
        store: Store,
        alloc: std.mem.Allocator,
    };

    pub const Store = union(enum) { fixed: *FixedStore, boxed: *GroupStore };

    pub const Into = union(enum) {
        groups: *const @import("groups.zig").GroupSet,
        one: [*]u8,

        fn tail(self: Into, ord: usize) [*]u8 {
            return switch (self) {
                .groups => |s| s.tail(ord),
                .one => |t| t,
            };
        }
    };

    /// Aggregate `j`'s pairs over `nparts` partitions, partition `p`'s records in
    /// `allocs[p]` (or `allocs[0]` for all) and its index in `table_alloc`: a lane's
    /// bounded (`GroupTable.initBounded`), as the merge dedups what it repeats, or
    /// sized for `expect` pairs. `order_free`: the aggregate's result does not
    /// depend on its values' order, so a global one over an INT-like argument
    /// keeps bits.
    pub fn init(a: std.mem.Allocator, j: usize, arg_ty: ?types.Type, grouped: bool, nparts: usize, allocs: []const std.mem.Allocator, table_alloc: std.mem.Allocator, bounded: bool, expect: usize, order_free: bool) !Pairs {
        var ty = arg_ty orelse types.Type.init(.string);
        if (arg_ty == null) ty.unknown = true;
        const kind = if (ty.unknown) null else keyhash.keyKindOf(ty.kind);
        const parts = try a.alloc(Part, nparts);
        var made: usize = 0;
        errdefer for (parts[0..made]) |*p| p.table.deinit();
        for (parts, 0..) |*p, i| {
            const al = allocs[if (allocs.len == 1) 0 else i];
            const table = if (bounded) try GroupTable.initBounded(table_alloc, lane_table, lane_table) else try GroupTable.init(table_alloc, GroupTable.capFor(expect));
            p.* = .{ .table = table, .store = try newStore(al, kind, grouped), .alloc = al };
            made += 1;
        }
        var enc: ?keytable.Encoder = null;
        if (kind) |k| {
            const kinds = try a.dupe(keyhash.KeyKind, &.{k});
            enc = try keytable.Encoder.init(a, &.{0}, kinds);
        }
        const bits: ?Bits = if (!grouped and order_free and (kind == .i64k or kind == .i32k)) .{} else null;
        return .{ .j = j, .ty = ty, .kind = kind, .grouped = grouped, .parts = parts, .alloc = a, .enc = enc, .bits = bits, .gpa = table_alloc };
    }

    /// An empty one-partition set like `like`, sized for `expect` pairs, for a merge
    /// to absorb into; bits when `like` holds bits.
    pub fn initLike(a: std.mem.Allocator, like: *const Pairs, table_alloc: std.mem.Allocator, expect: usize) !Pairs {
        return init(a, like.j, like.ty, like.grouped, 1, &.{a}, table_alloc, false, expect, like.bits != null);
    }

    /// Whether aggregate `func` over `arg` gives one result for its distinct values
    /// in any order. Only COUNT takes DISTINCT today, and a count is a count.
    pub fn orderFree(func: ast.AggFunc, arg: ?types.Type) bool {
        return arg != null and func == .count;
    }

    /// How many values partition `p` holds.
    pub fn count(self: *const Pairs, p: usize) usize {
        if (self.bits) |*b| return b.count(p);
        return switch (self.parts[p].store) {
            inline else => |st| st.len,
        };
    }

    fn newStore(a: std.mem.Allocator, kind: ?keyhash.KeyKind, grouped: bool) !Store {
        const nk: usize = if (grouped) 2 else 1;
        if (kind != null) {
            const st = try a.create(FixedStore);
            st.* = FixedStore.init(a, nk, &no_payload);
            return .{ .fixed = st };
        }
        const st = try a.create(GroupStore);
        st.* = GroupStore.init(a, nk, &no_payload);
        return .{ .boxed = st };
    }

    pub fn freePart(self: *Pairs, p: usize) void {
        self.parts[p].table.deinit();
    }

    pub fn deinit(self: *Pairs) void {
        for (self.parts) |*p| p.table.deinit();
        if (self.bits) |*b| b.deinit(self.gpa);
        self.bits = null;
    }

    /// The bytes the partitions' indexes hold, for a spilling fold's budget.
    pub fn tableBytes(self: *const Pairs) usize {
        var n: usize = 0;
        for (self.parts) |p| n += p.table.entries.len * @sizeOf(u32);
        if (self.bits) |*b| n += b.bytes();
        return n;
    }

    pub const Rows = struct { tails: []const ?[*]u8, pidx: []const u8, layout: *const Layout };

    /// A batch's pairs: row `r`'s value of `col` with, grouped, the group at
    /// `rows.tails[r]` (none for a row sent to a spill file), filed in `rows.pidx[r]`.
    pub fn add(self: *Pairs, scratch: std.mem.Allocator, col_in: column.Column, n: usize, rows: ?Rows, in: keytable.Interner) !void {
        std.debug.assert(self.grouped == (rows != null));
        const col = try self.typed(scratch, col_in, n);
        const nk: usize = if (self.grouped) 2 else 1;
        const hashes = try scratch.alloc(u64, n);
        const pof = try scratch.alloc(u8, n);
        const keys = try scratch.alloc(i64, n * nk);
        var probes: []Value = &.{};
        var m: usize = 0;
        if (self.kind) |kind| {
            const e = try self.enc.?.encode(scratch, &.{col}, n, in, false);
            if (rows) |rs| {
                for (0..n) |r| {
                    const t = rs.tails[r] orelse continue;
                    if (e.mask(r) != 0) continue;
                    const k = keys[m * 2 ..][0..2];
                    k[0] = @intCast(rs.layout.ordOf(t));
                    k[1] = e.keys[r];
                    hashes[m] = keytable.fixedHash(k, 0);
                    pof[m] = rs.pidx[r];
                    m += 1;
                }
            } else {
                if (self.bits) |*b| {
                    for (0..n) |r| if (e.mask(r) == 0) {
                        keys[m] = e.keys[r];
                        m += 1;
                    };
                    if (try b.addAll(self.gpa, keys[0..m])) return;
                    try self.toPairs();
                    m = 0;
                }
                const d = if (kind == .strk) col.dict else null;
                var none = [_]u64{};
                const seen: []u64 = if (d) |dd| try self.seenOf(dd) else &none;
                for (0..n) |r| {
                    if (e.mask(r) != 0) continue;
                    if (d) |dd| {
                        const c = dd.codes[r];
                        const bit = @as(u64, 1) << @truncate(c);
                        if (seen[c >> 6] & bit != 0) continue;
                        seen[c >> 6] |= bit;
                    }
                    keys[m] = e.keys[r];
                    hashes[m] = keytable.fixedHash(e.hkeys[r..][0..1], 0);
                    pof[m] = @intCast(keytable.partOf(hashes[m]));
                    m += 1;
                }
            }
        } else {
            probes = try scratch.alloc(Value, n * nk);
            for (0..n) |r| {
                if (!col.validity.get(r)) continue;
                const k = probes[m * nk ..][0..nk];
                const v = keyhash.canonValue(col.getValue(r));
                if (rows) |rs| {
                    const t = rs.tails[r] orelse continue;
                    k[0] = .{ .int = @intCast(rs.layout.ordOf(t)) };
                    k[1] = v;
                    pof[m] = rs.pidx[r];
                } else k[0] = v;
                hashes[m] = keyhash.MultiKeyCtx.hash(.{}, k);
                if (rows == null) pof[m] = @intCast(keytable.partOf(hashes[m]));
                m += 1;
            }
        }
        if (self.kind != null) {
            try self.insertFixed(scratch, nk, pof[0..m], hashes, keys);
        } else {
            const runs = try keytable.Runs(Value).init(scratch, self.parts.len, nk, pof[0..m], &.{}, hashes, probes, &.{});
            for (self.parts, 0..) |*part, p| {
                var v = Record(GroupStore){ .tb = &part.table, .st = part.store.boxed };
                try keytable.probe(GroupStore, .dupe, &runs, runs.starts[p], runs.starts[p + 1], &v);
            }
        }
    }

    /// Fixed-width keys filed into the partitions `pof` names and recorded where new.
    fn insertFixed(self: *Pairs, scratch: std.mem.Allocator, nk: usize, pof: []const u8, hashes: []const u64, keys: []const i64) !void {
        const runs = try keytable.Runs(i64).init(scratch, self.parts.len, nk, pof, &.{}, hashes, keys, &.{});
        for (self.parts, 0..) |*part, p| {
            var v = Record(FixedStore){ .tb = &part.table, .st = part.store.fixed };
            try keytable.probe(FixedStore, .dupe, &runs, runs.starts[p], runs.starts[p + 1], &v);
        }
    }

    /// The bits as pairs, each value hashed and filed as `add` files it: for a set
    /// whose values outgrew its bits, or a merge whose other sets hold pairs.
    pub fn toPairs(self: *Pairs) !void {
        var b = self.bits orelse return;
        self.bits = null;
        defer b.deinit(self.gpa);
        var arena = std.heap.ArenaAllocator.init(self.gpa);
        defer arena.deinit();
        const a = arena.allocator();
        var vals: std.ArrayListUnmanaged(i64) = .empty;
        const Collect = struct {
            l: *std.ArrayListUnmanaged(i64),
            a: std.mem.Allocator,
            fn put(c: *const @This(), v: i64) !void {
                try c.l.append(c.a, v);
            }
        };
        const ctx = Collect{ .l = &vals, .a = a };
        for (0..keytable.parts) |p| try b.each(p, &ctx, Collect.put);
        const hashes = try a.alloc(u64, vals.items.len);
        const pof = try a.alloc(u8, vals.items.len);
        for (vals.items, hashes, pof) |k, *h, *pp| {
            h.* = keytable.fixedHash(&[1]i64{k}, 0);
            pp.* = if (self.parts.len == 1) 0 else @intCast(keytable.partOf(h.*));
        }
        try self.insertFixed(a, 1, pof, hashes, vals.items);
    }

    /// `add`'s side of `keytable.probe`: a pair only recorded.
    fn Record(comptime S: type) type {
        return struct {
            tb: *GroupTable,
            st: *S,

            pub inline fn hit(_: *@This(), _: usize, _: u64, _: anytype, _: keytable.Hit(S)) !void {}
        };
    }

    /// Which codes of dictionary `d` a global aggregate has paired already, kept while
    /// batches share it, so a value is hashed once a dictionary instead of once a row.
    fn seenOf(self: *Pairs, d: *const column.Column.Dict) ![]u64 {
        if (d.id != 0 and self.seen.id == d.id) return self.seen.bits;
        const bits = try self.alloc.alloc(u64, (d.values.len + 63) / 64);
        @memset(bits, 0);
        self.seen = .{ .id = d.id, .bits = bits };
        return bits;
    }

    /// `col` as the values' type, a column of another kind (a lane's raw text) cast.
    fn typed(self: *const Pairs, scratch: std.mem.Allocator, col: column.Column, n: usize) !column.Column {
        if (self.kind == null or col.ty.kind == self.ty.kind) return col;
        var bd = try column.Builder.initCapacity(scratch, self.ty.asNullable(), n);
        for (0..n) |r| try bd.append(try eval.castValueTyped(scratch, col.getValue(r), self.ty));
        return bd.finish();
    }

    fn put(part: *Part, h: u64, key: []const i64) !void {
        _ = try keytable.upsert(&part.table, part.store.fixed, h, keytable.FixedKey{ .vals = key, .mask = 0 }, .dupe);
    }

    fn putBoxed(part: *Part, h: u64, key: []const Value) !void {
        _ = try keytable.upsert(&part.table, part.store.boxed, h, key, .dupe);
    }

    /// Source partition `sp` of `src` into partition `dp`, each grouped pair's group
    /// ordinal through `remap`.
    pub fn absorb(self: *Pairs, dp: usize, src: *const Pairs, sp: usize, remap: ?[]const u32) !void {
        const part = &self.parts[dp];
        if (src.bits) |*sb| {
            if (self.bits) |*db| {
                if (try db.unionPart(self.gpa, sb, sp)) return;
                try self.toPairs();
            }
            const Put = struct {
                part: *Part,
                fn one(c: *const @This(), v: i64) !void {
                    const key = [1]i64{v};
                    try put(c.part, keytable.fixedHash(&key, 0), &key);
                }
            };
            return sb.each(sp, &Put{ .part = part }, Put.one);
        }
        try self.toPairs();
        switch (src.parts[sp].store) {
            .fixed => |st| for (0..st.len) |i| {
                const r = st.at(i);
                if (remap) |rm| {
                    const key = [2]i64{ rm[@intCast(r.keys[0])], r.keys[1] };
                    try put(part, keytable.fixedHash(&key, 0), &key);
                } else try put(part, st.hashAt(i), r.keys);
            },
            .boxed => |st| for (0..st.len) |i| {
                const r = st.at(i);
                if (remap) |rm| {
                    const key = [2]Value{ .{ .int = rm[@intCast(r.keys[0].int)] }, r.keys[1] };
                    try putBoxed(part, keyhash.MultiKeyCtx.hash(.{}, &key), &key);
                } else try putBoxed(part, st.hashAt(i), r.keys);
            },
        }
    }

    /// Partition `p`'s pairs into their groups' states, in the order they were met.
    pub fn apply(self: *const Pairs, p: usize, into: Into, layout: *const Layout, alloc: std.mem.Allocator, strs: ?*const keytable.StrTable) !void {
        const k: *const Kernel = layout.kernels[self.j];
        const off = layout.offs[self.j];
        const cx = layout.ctx(self.j, alloc);
        if (self.bits) |*b| {
            const Add = struct {
                k: *const Kernel,
                at: [*]u8,
                cx: @TypeOf(cx),
                kind: types.TypeKind,
                fn one(c: *const @This(), v: i64) !void {
                    try c.k.add(c.at, keytable.wordValue(c.kind, v, null), c.cx);
                }
            };
            const add_one = Add{ .k = k, .at = into.tail(0) + off, .cx = cx, .kind = self.ty.kind };
            if (self.parts.len == 1) {
                for (0..keytable.parts) |q| try b.each(q, &add_one, Add.one);
            } else try b.each(p, &add_one, Add.one);
            return;
        }
        switch (self.parts[p].store) {
            .fixed => |st| for (0..st.len) |i| {
                const r = st.at(i);
                const ord: usize = if (self.grouped) @intCast(r.keys[0]) else 0;
                const v = keytable.wordValue(self.ty.kind, r.keys[r.keys.len - 1], strs);
                try k.add(into.tail(ord) + off, v, cx);
            },
            .boxed => |st| for (0..st.len) |i| {
                const r = st.at(i);
                const ord: usize = if (self.grouped) @intCast(r.keys[0].int) else 0;
                try k.add(into.tail(ord) + off, r.keys[r.keys.len - 1], cx);
            },
        }
    }
};

test "pairs: a dictionary seen again is not re-interned into the state" {
    var state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer state.deinit();
    var scratch = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer scratch.deinit();
    const sa = state.allocator();
    var strs: ?*keytable.StrTable = null;
    var cache: std.StringHashMapUnmanaged(keytable.StrId) = .empty;
    const in = keytable.Interner{ .table = &strs, .cache = &cache, .alloc = sa };

    const vals = try sa.alloc([]const u8, 10_000);
    for (vals, 0..) |*v, i| v.* = try std.fmt.allocPrint(sa, "v{d}", .{i});
    const n = 8192;
    const codes = try sa.alloc(u32, n);
    for (codes, 0..) |*c, i| c.* = @intCast(i * 7 % vals.len);
    var bd = column.Builder.init(sa, types.Type.init(.string));
    for (codes) |c| try bd.append(.{ .string = vals[c] });
    var col = try bd.finish();
    const d = column.Column.Dict{ .values = vals, .codes = codes, .id = 5 };
    col.dict = &d;

    var pairs = try Pairs.init(sa, 0, types.Type.init(.string), false, 1, &.{sa}, std.testing.allocator, false, 0, false);
    defer pairs.deinit();
    try pairs.add(scratch.allocator(), col, n, null, in);
    const after_one = state.queryCapacity();
    for (0..50) |_| {
        _ = scratch.reset(.retain_capacity);
        try pairs.add(scratch.allocator(), col, n, null, in);
    }
    try std.testing.expect(state.queryCapacity() - after_one < 64 * 1024);
    try std.testing.expectEqual(@as(usize, 8192), pairs.count(0));
}
