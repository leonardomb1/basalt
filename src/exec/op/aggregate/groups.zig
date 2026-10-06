//! A lane's groups: the interned string table, the group set a lane fills, and the
//! merge of several lanes' sets into one.

const Aggregate = @import("../aggregate.zig").Aggregate;
const Acc = Aggregate.Acc;
const Agg = Aggregate.Agg;
const FixedKey = Aggregate.FixedKey;
const FixedStore = Aggregate.FixedStore;
const GroupStore = Aggregate.GroupStore;
const GroupTable = Aggregate.GroupTable;
const Layout = Aggregate.Layout;
const Value = @import("../../value.zig").Value;
const dupeValue = @import("../aggregate.zig").dupeValue;
const keyhash = @import("../../keyhash.zig");
const mergeAcc = Aggregate.mergeAcc;
const mergeDistinct = Aggregate.mergeDistinct;
const std = @import("std");
const types = @import("../../../lang/types.zig");

pub const StrId = struct { id: u32, h: u64 };

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

pub fn strId(self: *Aggregate, table: *StrTable, s: []const u8) !StrId {
    const gop = try self.str_cache.getOrPut(self.state, s);
    if (!gop.found_existing) {
        gop.key_ptr.* = try self.state.dupe(u8, s);
        gop.value_ptr.* = .{ .id = try table.intern(s), .h = std.hash.Wyhash.hash(0x5bd1e995, s) };
    }
    return gop.value_ptr.*;
}

pub fn strTable(self: *Aggregate) !*StrTable {
    if (self.strs) |t| return t;
    const t = try self.state.create(StrTable);
    t.* = StrTable.init(self.state);
    self.strs = t;
    return t;
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

pub const Partial = struct {
    nvalid: usize,
    sum_i: i128 = 0,
    sum_f: f64 = 0,
    ext: ?Value = null,
};
