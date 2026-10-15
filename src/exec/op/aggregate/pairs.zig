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

const GroupStore = keytable.GroupStore(keytable.NoPayload);
const FixedStore = keytable.FixedStore(keytable.NoPayload);
const GroupTable = keytable.GroupTable;
const Kernel = @import("kernel.zig").Kernel;
const Layout = @import("kernel.zig").Layout;
const Value = @import("../../value.zig").Value;
const column = @import("../../column.zig");
const dupeValue = @import("funcs.zig").dupeValue;
const eval = @import("../../eval.zig");
const keyhash = @import("../../keyhash.zig");
const keytable = @import("../../keytable.zig");
const std = @import("std");
const types = @import("../../../lang/types.zig");

const no_payload = keytable.NoPayload{};

pub const Pairs = struct {
    j: usize,
    ty: types.Type,
    kind: ?keyhash.KeyKind,
    grouped: bool,
    parts: []Part,
    alloc: std.mem.Allocator,
    seen: Seen = .{},

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
    /// `allocs[p]` (or `allocs[0]` for all) and its index in `table_alloc`.
    pub fn init(a: std.mem.Allocator, j: usize, arg_ty: ?types.Type, grouped: bool, nparts: usize, allocs: []const std.mem.Allocator, table_alloc: std.mem.Allocator) !Pairs {
        var ty = arg_ty orelse types.Type.init(.string);
        if (arg_ty == null) ty.unknown = true;
        const kind = if (ty.unknown) null else keyhash.keyKindOf(ty.kind);
        const parts = try a.alloc(Part, nparts);
        var made: usize = 0;
        errdefer for (parts[0..made]) |*p| p.table.deinit();
        for (parts, 0..) |*p, i| {
            const al = allocs[if (allocs.len == 1) 0 else i];
            p.* = .{ .table = try GroupTable.init(table_alloc, 16), .store = try newStore(al, kind, grouped), .alloc = al };
            made += 1;
        }
        return .{ .j = j, .ty = ty, .kind = kind, .grouped = grouped, .parts = parts, .alloc = a };
    }

    /// An empty one-partition set like `like`, for a merge to absorb into.
    pub fn initLike(a: std.mem.Allocator, like: *const Pairs, table_alloc: std.mem.Allocator) !Pairs {
        return init(a, like.j, like.ty, like.grouped, 1, &.{a}, table_alloc);
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

    fn lenOf(self: *const Pairs, p: usize) usize {
        return switch (self.parts[p].store) {
            inline else => |st| st.len,
        };
    }

    pub fn freePart(self: *Pairs, p: usize) void {
        self.parts[p].table.deinit();
    }

    pub fn deinit(self: *Pairs) void {
        for (self.parts) |*p| p.table.deinit();
    }

    /// The bytes the partitions' indexes hold, for a spilling fold's budget.
    pub fn tableBytes(self: *const Pairs) usize {
        var n: usize = 0;
        for (self.parts) |p| n += p.table.entries.len * @sizeOf(u32);
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
            const enc = try keytable.Encoder.init(scratch, &.{0}, &.{kind});
            const e = try enc.encode(scratch, &.{col}, n, in, false);
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
        const starts = try scratch.alloc(u32, self.parts.len + 1);
        const sh = try scratch.alloc(u64, m);
        if (self.kind != null) {
            const sk = try scratch.alloc(i64, m * nk);
            scatter(i64, nk, pof[0..m], hashes, keys, sh, sk, starts);
            for (self.parts, 0..) |*part, p| {
                const lo = starts[p];
                const hi = starts[p + 1];
                for (lo..hi) |d| {
                    if (d + keytable.prefetch_ahead < hi) part.table.prefetch(sh[d + keytable.prefetch_ahead]);
                    try put(part, sh[d], sk[d * nk ..][0..nk]);
                }
            }
        } else {
            const sk = try scratch.alloc(Value, m * nk);
            scatter(Value, nk, pof[0..m], hashes, probes, sh, sk, starts);
            for (self.parts, 0..) |*part, p| {
                for (starts[p]..starts[p + 1]) |d| try putBoxed(part, sh[d], sk[d * nk ..][0..nk]);
            }
        }
    }

    /// Rows `i` of `hashes` and `keys` (`nk` a row) moved to `sh` and `sk` grouped by
    /// partition `pof[i]`, in input order within one; partition `p` runs from
    /// `starts[p]` to `starts[p + 1]`. Walking them in place, not through an index of
    /// row numbers, reads both in order.
    fn scatter(comptime K: type, nk: usize, pof: []const u8, hashes: []const u64, keys: []const K, sh: []u64, sk: []K, starts: []u32) void {
        @memset(starts, 0);
        for (pof) |p| starts[p + 1] += 1;
        for (1..starts.len) |ci| starts[ci] += starts[ci - 1];
        var at: [keytable.parts]u32 = undefined;
        @memcpy(at[0 .. starts.len - 1], starts[0 .. starts.len - 1]);
        for (pof, 0..) |p, i| {
            const d = at[p];
            at[p] += 1;
            sh[d] = hashes[i];
            for (0..nk) |w| sk[d * nk + w] = keys[i * nk + w];
        }
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
        const st = part.store.fixed;
        const k = keytable.FixedKey{ .vals = key, .mask = 0 };
        const f = try part.table.getOrPut(h, k, st, @intCast(st.len));
        if (f.found) return;
        const nr = try st.push(h);
        nr.keys[0] = key[0];
        if (key.len > 1) nr.keys[1] = key[1];
        nr.mask.* = 0;
    }

    fn putBoxed(part: *Part, h: u64, key: []const Value) !void {
        const st = part.store.boxed;
        const f = try part.table.getOrPut(h, key, st, @intCast(st.len));
        if (f.found) return;
        const nr = try st.push(h);
        for (nr.keys, key) |*o, v| o.* = try dupeValue(part.alloc, v);
    }

    /// Source partition `sp` of `src` into partition `dp`, each grouped pair's group
    /// ordinal through `remap`.
    pub fn absorb(self: *Pairs, dp: usize, src: *const Pairs, sp: usize, remap: ?[]const u32) !void {
        const part = &self.parts[dp];
        switch (part.store) {
            inline else => |st| try part.table.reserve(part.table.len + src.lenOf(sp), st),
        }
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
