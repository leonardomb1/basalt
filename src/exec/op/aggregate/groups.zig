//! A lane's groups: the group set a lane fills, and the merge of several lanes' sets
//! into one.

const Aggregate = @import("../aggregate.zig").Aggregate;
const Agg = Aggregate.Agg;
const FixedKey = Aggregate.FixedKey;
const FixedStore = Aggregate.FixedStore;
const GroupStore = Aggregate.GroupStore;
const GroupTable = Aggregate.GroupTable;
const Layout = Aggregate.Layout;
const Value = @import("../../value.zig").Value;
const dupeValue = @import("../aggregate.zig").dupeValue;
const keyhash = @import("../../keyhash.zig");
const std = @import("std");
const types = @import("../../../lang/types.zig");

pub const GroupSet = struct {
    store: Store,
    len: usize,
    key_kinds: []const types.TypeKind,
    table: ?*GroupTable = null,
    strs: ?*const @import("../../keytable.zig").StrTable = null,

    pub fn freeTable(self: *const GroupSet) void {
        if (self.table) |t| t.deinit();
    }

    pub const Store = union(enum) {
        fixed: *FixedStore,
        boxed: *GroupStore,
        single: *Single,
    };

    /// The global aggregate's one record.
    pub const Single = struct { layout: *const Layout, tail: [*]u8 };

    fn layoutOf(self: *const GroupSet) *const Layout {
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

    /// Aggregate `j`'s result for group `i`.
    pub fn finish(self: *const GroupSet, i: usize, j: usize) !Value {
        return self.layoutOf().finish(self.tail(i), j);
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
            .single => |dst| for (0..self.aggs.len) |j| try dst.layout.merge(self.alloc, dst.tail, src.tail(i), j),
            .fixed => |dst| {
                const sr = src.store.fixed.at(i);
                const h = src.store.fixed.hashAt(i);
                const rec = try self.find(dst, h, FixedKey{ .vals = sr.keys, .mask = sr.mask.* }) orelse {
                    const nr = try dst.push(h);
                    @memcpy(nr.keys, sr.keys);
                    nr.mask.* = sr.mask.*;
                    try self.adoptTail(dst.payload, nr.tail, sr.tail);
                    return;
                };
                for (0..self.aggs.len) |j| try dst.payload.merge(self.alloc, rec.tail, sr.tail, j);
            },
            .boxed => |dst| {
                const sr = src.store.boxed.at(i);
                const h = src.store.boxed.hashAt(i);
                const rec = try self.find(dst, h, @as([]const Value, sr.keys)) orelse {
                    const nr = try dst.push(h);
                    if (self.own) {
                        for (nr.keys, sr.keys) |*o, v| o.* = try dupeValue(self.alloc, v);
                    } else @memcpy(nr.keys, sr.keys);
                    try self.adoptTail(dst.payload, nr.tail, sr.tail);
                    return;
                };
                for (0..self.aggs.len) |j| try dst.payload.merge(self.alloc, rec.tail, sr.tail, j);
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
        for (layout.kernels, layout.offs, 0..) |k, off, j| {
            const own = k.own orelse continue;
            if (k.own_always or self.own) try own(dst + off, layout.ctx(j, self.alloc));
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
