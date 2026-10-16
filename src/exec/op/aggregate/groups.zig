//! A lane's groups: the group set a lane fills, and the merge of several lanes' sets
//! into one. A set from a lane carries its DISTINCT aggregates' pairs, which the
//! merge unions and applies once every set is in (`settle`).

const Aggregate = @import("../aggregate.zig").Aggregate;
const Agg = Aggregate.Agg;
const FixedKey = Aggregate.FixedKey;
const FixedStore = Aggregate.FixedStore;
const GroupStore = Aggregate.GroupStore;
const GroupTable = Aggregate.GroupTable;
const Layout = Aggregate.Layout;
const Pairs = @import("pairs.zig").Pairs;
const Value = @import("../../value.zig").Value;
const dupeValue = @import("../aggregate.zig").dupeValue;
const keyhash = @import("../../keyhash.zig");
const keytable = @import("../../keytable.zig");
const par = @import("../../par.zig");
const std = @import("std");
const types = @import("../../../lang/types.zig");

pub const GroupSet = struct {
    store: Store,
    len: usize,
    key_kinds: []const types.TypeKind,
    table: ?*GroupTable = null,
    strs: ?*const keytable.StrTable = null,
    pairs: []Pairs = &.{},
    part: u8 = 0,

    /// Free the index, and the indexes of the pairs this set holds: its own
    /// partition's when grouped, every partition for a global aggregate.
    pub fn freeTable(self: *const GroupSet) void {
        if (self.table) |t| t.deinit();
        for (self.pairs) |*pp| {
            if (pp.grouped) pp.freePart(self.part) else pp.deinit();
        }
    }

    pub const Store = union(enum) {
        fixed: *FixedStore,
        boxed: *GroupStore,
        single: *Single,
    };

    pub const Single = struct { layout: *const Layout, tail: [*]u8 };

    pub fn layoutOf(self: *const GroupSet) *const Layout {
        return switch (self.store) {
            .fixed => |st| st.payload,
            .boxed => |st| st.payload,
            .single => |st| st.layout,
        };
    }

    /// Group `i`'s state record.
    pub fn tail(self: *const GroupSet, i: usize) [*]u8 {
        return switch (self.store) {
            .fixed => |st| st.at(i).tail,
            .boxed => |st| st.at(i).tail,
            .single => |st| st.tail,
        };
    }

    pub fn keyValue(self: *const GroupSet, i: usize, j: usize) Value {
        switch (self.store) {
            .boxed => |st| return st.at(i).keys[j],
            .single => unreachable,
            .fixed => |st| {
                const rec = st.at(i);
                if (rec.mask.* & (@as(u64, 1) << @intCast(j)) != 0) return .null;
                return keytable.wordValue(self.key_kinds[j], rec.keys[j], self.strs);
            },
        }
    }

    /// Aggregate `j`'s result for group `i`.
    pub fn finish(self: *const GroupSet, i: usize, j: usize) !Value {
        return self.layoutOf().finish(self.tail(i), j);
    }
};

fn capFor(n: usize) usize {
    var cap: usize = 256;
    while ((n + 1) * 10 >= cap * 7) cap *= 2;
    return cap;
}

pub const GroupMerge = struct {
    alloc: std.mem.Allocator,
    aggs: []const Agg,
    own: bool = false,
    set: GroupSet,
    table: GroupTable,
    threads: usize = 1,
    pairs: []Pairs = &.{},
    globals: std.ArrayListUnmanaged(*const GroupSet) = .empty,
    remap: std.ArrayListUnmanaged(u32) = .empty,
    settled: bool = false,

    /// Free the indexes and lists; safe to repeat, as a lane partition's merge is
    /// freed once merged and again with the partitions.
    pub fn deinit(self: *GroupMerge) void {
        self.table.deinit();
        for (self.pairs) |*pp| pp.deinit();
        self.globals.deinit(std.heap.page_allocator);
        self.globals = .empty;
        self.remap.deinit(std.heap.page_allocator);
        self.remap = .empty;
    }

    pub fn init(alloc: std.mem.Allocator, like: *const GroupSet, aggs: []const Agg) !GroupMerge {
        return initFor(alloc, like, aggs, 0);
    }

    /// As `init`, the index sized at once for `expect` groups.
    pub fn initFor(alloc: std.mem.Allocator, like: *const GroupSet, aggs: []const Agg, expect: usize) !GroupMerge {
        const store: GroupSet.Store = switch (like.store) {
            .fixed => |st| blk: {
                const n = try alloc.create(FixedStore);
                n.* = FixedStore.init(alloc, st.nkeys, st.payload);
                break :blk .{ .fixed = n };
            },
            .boxed => |st| blk: {
                const n = try alloc.create(GroupStore);
                n.* = GroupStore.init(alloc, st.nkeys, st.payload);
                break :blk .{ .boxed = n };
            },
            .single => |st| blk: {
                const n = try alloc.create(GroupSet.Single);
                const t = try alloc.alignedAlloc(u8, .of(Value), @max(st.layout.size, 1));
                st.layout.clear(t.ptr);
                n.* = .{ .layout = st.layout, .tail = t.ptr };
                break :blk .{ .single = n };
            },
        };
        const pairs = try alloc.alloc(Pairs, if (like.store == .single) 0 else like.pairs.len);
        for (pairs, like.pairs[0..pairs.len]) |*d, *s| d.* = try Pairs.initLike(alloc, s, alloc);
        return .{
            .alloc = alloc,
            .aggs = aggs,
            .set = .{ .store = store, .len = if (store == .single) 1 else 0, .key_kinds = like.key_kinds, .strs = like.strs },
            .table = try GroupTable.init(alloc, capFor(expect)),
            .pairs = pairs,
        };
    }

    /// Every group of `src`, then its pairs re-keyed by the ordinals its groups got
    /// here; a global set's pairs wait for `settle`, which unions them by partition.
    pub fn addSet(self: *GroupMerge, src: *const GroupSet) !void {
        if (self.set.store == .single) {
            for (0..src.len) |i| _ = try self.add(src, i);
            if (src.pairs.len > 0) try self.globals.append(std.heap.page_allocator, src);
            return;
        }
        if (src.pairs.len == 0) {
            for (0..src.len) |i| _ = try self.add(src, i);
            return;
        }
        try self.remap.resize(std.heap.page_allocator, src.len);
        for (self.remap.items, 0..) |*r, i| r.* = try self.add(src, i);
        for (self.pairs, src.pairs) |*d, *s| try d.absorb(0, s, src.part, self.remap.items);
    }

    /// Adds group `i` of `src`, returning its ordinal here.
    pub fn add(self: *GroupMerge, src: *const GroupSet, i: usize) !u32 {
        switch (self.set.store) {
            .single => |dst| {
                for (0..self.aggs.len) |j| try dst.layout.merge(self.alloc, dst.tail, src.tail(i), j);
                return 0;
            },
            .fixed => |dst| {
                const sr = src.store.fixed.at(i);
                const h = src.store.fixed.hashAt(i);
                const f = try self.find(dst, h, FixedKey{ .vals = sr.keys, .mask = sr.mask.* });
                if (f.found) {
                    for (0..self.aggs.len) |j| try dst.payload.merge(self.alloc, dst.at(f.slot).tail, sr.tail, j);
                    return f.slot;
                }
                const nr = try dst.push(h);
                @memcpy(nr.keys, sr.keys);
                nr.mask.* = sr.mask.*;
                try self.adoptTail(dst.payload, nr.tail, sr.tail, f.slot);
                return f.slot;
            },
            .boxed => |dst| {
                const sr = src.store.boxed.at(i);
                const h = src.store.boxed.hashAt(i);
                const f = try self.find(dst, h, @as([]const Value, sr.keys));
                if (f.found) {
                    for (0..self.aggs.len) |j| try dst.payload.merge(self.alloc, dst.at(f.slot).tail, sr.tail, j);
                    return f.slot;
                }
                const nr = try dst.push(h);
                if (self.own) {
                    for (nr.keys, sr.keys) |*o, v| o.* = try dupeValue(self.alloc, v);
                } else @memcpy(nr.keys, sr.keys);
                try self.adoptTail(dst.payload, nr.tail, sr.tail, f.slot);
                return f.slot;
            },
        }
    }

    /// The slot `key` has or, when new, reserves for the record the caller pushes.
    fn find(self: *GroupMerge, dst: anytype, h: u64, key: anytype) !struct { slot: u32, found: bool } {
        const f = try self.table.getOrPut(h, key, dst, @intCast(dst.len));
        if (!f.found) self.set.len += 1;
        return .{ .slot = f.slot, .found = f.found };
    }

    /// A new group's aggregate state, copied, with `own` rebuilt in this merge's
    /// allocator so the source set can be freed once merged.
    fn adoptTail(self: *GroupMerge, layout: *const Layout, dst: [*]u8, src: [*]u8, ord: u32) !void {
        @memcpy(dst[0..layout.size], src[0..layout.size]);
        layout.setOrd(dst, ord);
        if (!self.own) return;
        for (layout.kernels, layout.offs, 0..) |k, off, j| {
            const own = k.own orelse continue;
            try own(dst + off, layout.ctx(j, self.alloc));
        }
    }

    /// Adds every DISTINCT aggregate's pairs to the merged states, once.
    pub fn settle(self: *GroupMerge) !void {
        if (self.settled) return;
        self.settled = true;
        const layout = self.set.layoutOf();
        switch (self.set.store) {
            .single => |one| if (self.globals.items.len > 0) try self.settleGlobal(layout, one.tail),
            else => for (self.pairs) |*pp| {
                try pp.apply(0, .{ .groups = &self.set }, layout, self.alloc, self.set.strs);
                pp.deinit();
            },
        }
    }

    const GlobalCtx = struct {
        m: *GroupMerge,
        layout: *const Layout,
        partials: [][*]u8,
        keep: []std.heap.ArenaAllocator,
        errs: []?anyerror,

        fn run(c: *const GlobalCtx, p: usize) void {
            c.part(p) catch |e| {
                c.errs[p] = e;
            };
        }

        fn part(c: *const GlobalCtx, p: usize) !void {
            var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
            defer arena.deinit();
            const a = arena.allocator();
            for (c.m.globals.items[0].pairs) |*like| {
                var u = try Pairs.initLike(a, like, std.heap.page_allocator);
                defer u.deinit();
                for (c.m.globals.items) |src| {
                    for (src.pairs) |*s| if (s.j == like.j) try u.absorb(0, s, p, null);
                }
                try u.apply(0, .{ .one = c.partials[p] }, c.layout, c.keep[p].allocator(), c.m.set.strs);
            }
        }
    };

    /// A global aggregate's pairs, each value-hash partition unioned across the sets
    /// into a partial state of its own, the partials then folded in partition order
    /// and what they hold rebuilt in this merge's allocator.
    fn settleGlobal(self: *GroupMerge, layout: *const Layout, dst: [*]u8) !void {
        const keep = try self.alloc.alloc(std.heap.ArenaAllocator, keytable.parts);
        for (keep) |*k| k.* = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer for (keep) |*k| k.deinit();
        const partials = try self.alloc.alloc([*]u8, keytable.parts);
        for (partials) |*pt| {
            pt.* = (try self.alloc.alignedAlloc(u8, .of(Value), @max(layout.size, 1))).ptr;
            layout.clear(pt.*);
        }
        const errs = try self.alloc.alloc(?anyerror, keytable.parts);
        @memset(errs, null);
        const ctx = GlobalCtx{ .m = self, .layout = layout, .partials = partials, .keep = keep, .errs = errs };
        par.each(self.threads, keytable.parts, &ctx, GlobalCtx.run);
        for (errs) |e| if (e) |err| return err;
        for (partials) |pt| {
            for (self.globals.items[0].pairs) |*pp| try layout.merge(self.alloc, dst, pt, pp.j);
        }
        for (self.globals.items[0].pairs) |*pp| {
            const own = layout.kernels[pp.j].own orelse continue;
            try own(dst + layout.offs[pp.j], layout.ctx(pp.j, self.alloc));
        }
    }

    pub fn result(self: *GroupMerge) !GroupSet {
        try self.settle();
        var set = self.set;
        set.table = null;
        set.pairs = &.{};
        return set;
    }
};

/// A type-returning fn, not a `const` type: `std.testing.refAllDeclsRecursive` dives
/// into container-level types, and diving through this `std.HashMap` built a binary
/// that segfaulted at startup under Zig 0.15.2.
pub fn GroupMap() type {
    return std.HashMap([]const Value, usize, keyhash.MultiKeyCtx, std.hash_map.default_max_load_percentage);
}
