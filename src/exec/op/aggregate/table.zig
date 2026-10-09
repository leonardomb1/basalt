//! The group hash tables: a direct table for small integer keys, a fast path for a
//! few groups, fixed-width keys in record blocks, and the general table and store.
//! `GroupTable.find` probes without inserting, for a table frozen by spilling.

const Aggregate = @import("../aggregate.zig").Aggregate;
const Agg = Aggregate.Agg;
const Layout = Aggregate.Layout;
const Slot = Aggregate.Slot;
const Value = @import("../../value.zig").Value;
const column = @import("../../column.zig");
const keyhash = @import("../../keyhash.zig");
const std = @import("std");
const types = @import("../../../lang/types.zig");

pub const prefetch_ahead = 8;

pub const Direct = struct {
    recs: []?[*]u8,
    parts: []u8,

    pub const len = 1 << 14;

    pub fn init(gpa: std.mem.Allocator) !Direct {
        const recs = try gpa.alloc(?[*]u8, len);
        @memset(recs, null);
        const parts = try gpa.alloc(u8, len);
        return .{ .recs = recs, .parts = parts };
    }

    pub fn deinit(self: Direct, gpa: std.mem.Allocator) void {
        gpa.free(self.recs);
        gpa.free(self.parts);
    }

    /// The smallest non-null key, so a range starting at 0 or 1 sits wholly inside.
    pub fn baseFor(keys: []const i64, masks: []const u64) i64 {
        var lo: i64 = std.math.maxInt(i64);
        for (keys, masks) |k, m| if (m == 0 and k < lo) {
            lo = k;
        };
        return if (lo == std.math.maxInt(i64)) 0 else lo;
    }
};

pub const few_groups = 1 << 12;

/// Each word through murmur3's 64-bit finalizer. Wyhash's streaming form was a fifth
/// of a 100-group GROUP BY.
pub fn fixedHash(vals: []const i64, mask: u64) u64 {
    var h: u64 = 0x9E3779B97F4A7C15 ^ mask;
    for (vals) |v| h = fmix64(h ^ (@as(u64, @bitCast(v)) *% 0xC2B2AE3D27D4EB4F));
    return h;
}

pub fn fmix64(x0: u64) u64 {
    var x = x0;
    x ^= x >> 33;
    x *%= 0xff51afd7ed558ccd;
    x ^= x >> 33;
    x *%= 0xc4ceb9fe1a85ec53;
    x ^= x >> 33;
    return x;
}

pub const Fast = enum {
    count_star,
    count_all_valid,
    sum_i64,
    sum_f64,
    sum_f_of_i64,
    generic,

    pub fn of(slot: Slot, agg: Agg, col: ?column.Column, n: usize) Fast {
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

pub const KeyKind = enum { i64k, i32k, boolk, f64k, strk };

pub fn keyKindOf(kind: types.TypeKind) ?KeyKind {
    return switch (kind) {
        .int, .time, .timestamp => .i64k,
        .date => .i32k,
        .bool => .boolk,
        .float => .f64k,
        .string => .strk,
        else => null,
    };
}

pub const RecBlocks = struct {
    const first_shift = 4;
    const block_shift = 13;
    const block = 1 << block_shift;
    const small = block_shift - first_shift + 1;

    alloc: std.mem.Allocator,
    rec_size: usize,
    blocks: std.array_list.Managed([]u8),

    pub fn init(alloc: std.mem.Allocator, rec_size: usize) RecBlocks {
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

pub const FixedStore = struct {
    nkeys: usize,
    layout: *const Layout,
    recs: RecBlocks,
    len: usize = 0,

    const Rec = struct { keys: []i64, mask: *u64, tail: [*]u8 };

    pub fn init(alloc: std.mem.Allocator, nkeys: usize, layout: *const Layout) FixedStore {
        return .{ .nkeys = nkeys, .layout = layout, .recs = .init(alloc, @sizeOf(u64) + nkeys * @sizeOf(i64) + @sizeOf(u64) + layout.size) };
    }

    pub fn hashAt(self: *const FixedStore, i: usize) u64 {
        return @as(*const u64, @ptrCast(@alignCast(self.recs.base(i)))).*;
    }

    pub fn at(self: FixedStore, i: usize) Rec {
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

    pub fn push(self: *FixedStore, h: u64) !Rec {
        try self.recs.reserve(self.len);
        @as(*u64, @ptrCast(@alignCast(self.recs.base(self.len)))).* = h;
        const rec = self.at(self.len);
        self.len += 1;
        self.layout.clear(rec.tail);
        return rec;
    }
};

pub const FixedKey = struct { vals: []const i64, mask: u64 };

pub const GroupTable = struct {
    const salt_bits = 6;
    const idx_bits = 32 - salt_bits;
    const max_groups = (1 << idx_bits) - 1;

    entries: []u32,
    len: usize = 0,
    mask: u64,
    alloc: std.mem.Allocator,

    pub fn init(alloc: std.mem.Allocator, cap_pow2: usize) !GroupTable {
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

    pub fn prefetch(self: *const GroupTable, h: u64) void {
        @prefetch(&self.entries[h & self.mask], .{ .rw = .read, .locality = 3 });
    }

    /// Free the entries. Safe to repeat: a table handed on by `GroupMerge.adopt` is left
    /// empty and its owner's cleanup still runs.
    pub fn deinit(self: *GroupTable) void {
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

    /// The slot holding `key`, or null; never inserts or grows.
    pub fn find(self: *const GroupTable, h: u64, key: anytype, store: anytype) ?u32 {
        const want = saltOf(h) << idx_bits;
        var i = h & self.mask;
        while (true) : (i = (i + 1) & self.mask) {
            const e = self.entries[i];
            if (e == 0) return null;
            if ((e >> idx_bits) << idx_bits != want) continue;
            const idx = (e & max_groups) - 1;
            if (store.eqlAt(idx, h, key)) return idx;
        }
    }

    const Found = struct { slot: u32, found: bool };

    pub inline fn getOrPut(
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

pub const GroupStore = struct {
    nkeys: usize,
    layout: *const Layout,
    recs: RecBlocks,
    len: usize = 0,

    const Rec = struct { keys: []Value, tail: [*]u8 };

    pub fn init(alloc: std.mem.Allocator, nkeys: usize, layout: *const Layout) GroupStore {
        return .{ .nkeys = nkeys, .layout = layout, .recs = .init(alloc, @sizeOf(u64) + nkeys * @sizeOf(Value) + layout.size) };
    }

    pub fn hashAt(self: *const GroupStore, i: usize) u64 {
        return @as(*const u64, @ptrCast(@alignCast(self.recs.base(i)))).*;
    }

    pub fn at(self: GroupStore, i: usize) Rec {
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

    pub fn push(self: *GroupStore, h: u64) !Rec {
        try self.recs.reserve(self.len);
        @as(*u64, @ptrCast(@alignCast(self.recs.base(self.len)))).* = h;
        const rec = self.at(self.len);
        self.len += 1;
        self.layout.clear(rec.tail);
        return rec;
    }
};
