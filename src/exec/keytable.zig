//! The key table every hash operator shares: keys encoded column at a time into
//! fixed words under `keyhash`'s rules (`Encoder`, strings through an `Interner`),
//! one hash over them (`fixedHash`), a salted `u32` slot table (`GroupTable`) over
//! records in blocks (`FixedStore` for fixed-width keys, `GroupStore` for boxed
//! ones), each record's tail a payload `P` with a `size` and a `clear`: an
//! aggregate's states, or nothing for a set of keys. A direct table serves small
//! integer keys. `GroupTable.find` probes without inserting, for a table frozen by
//! spilling.

const Decimal = @import("value.zig").Decimal;
const Value = @import("value.zig").Value;
const pow10_i128 = @import("value.zig").pow10_i128;
const column = @import("column.zig");
const keyhash = @import("keyhash.zig");
const types = @import("../lang/types.zig");
const std = @import("std");
const stats = @import("build_options").stats;

pub const prefetch_ahead = 8;

/// What a `-Dstats` build counts, per table and then in all: lookups, slots probed
/// past the first, salt matches on another key, inserts, rehashes and the entries
/// they moved, and items laid out in partition runs.
pub const Count = enum { lookups, steps, false_hits, inserts, rehashes, moved, scattered };

const ncounts = if (stats) @typeInfo(Count).@"enum".fields.len else 0;
const Counters = [ncounts]std.atomic.Value(u64);
var totals: Counters = [_]std.atomic.Value(u64){.init(0)} ** ncounts;

inline fn tally(counters: *const Counters, comptime c: Count, n: u64) void {
    if (stats) _ = @constCast(&counters[@intFromEnum(c)]).fetchAdd(n, .monotonic);
}

/// The totals of every table freed so far, one line, in a `-Dstats` build.
pub fn report(w: *std.Io.Writer) !void {
    if (!stats) return;
    var v: [ncounts]u64 = undefined;
    for (&v, &totals) |*x, *t| x.* = t.load(.monotonic);
    const lookups: f64 = @floatFromInt(@max(v[@intFromEnum(Count.lookups)], 1));
    try w.print("key tables: {d} lookups, {d:.3} extra slots and {d:.3} false salt hits a lookup, {d} inserts, {d} rehashes moving {d} entries, {d} items laid out in runs\n", .{
        v[@intFromEnum(Count.lookups)],
        @as(f64, @floatFromInt(v[@intFromEnum(Count.steps)])) / lookups,
        @as(f64, @floatFromInt(v[@intFromEnum(Count.false_hits)])) / lookups,
        v[@intFromEnum(Count.inserts)],
        v[@intFromEnum(Count.rehashes)],
        v[@intFromEnum(Count.moved)],
        v[@intFromEnum(Count.scattered)],
    });
}

/// A direct array for one small-range integer key: a key `base + i` finds its record
/// at `recs[i]` with no hashing. `base` is the smallest key of the first batch, so a
/// range starting anywhere sits wholly inside.
pub const Direct = struct {
    recs: []?[*]u8,
    parts: []u8,
    base: ?i64 = null,

    pub const len = 1 << 14;

    pub fn init(gpa: std.mem.Allocator) !Direct {
        const recs = try gpa.alloc(?[*]u8, len);
        @memset(recs, null);
        return .{ .recs = recs, .parts = try gpa.alloc(u8, len) };
    }

    pub fn deinit(self: Direct, gpa: std.mem.Allocator) void {
        gpa.free(self.recs);
        gpa.free(self.parts);
    }

    /// Whether `keys` (one word a row, of kinds this array serves) can use one.
    pub fn serves(kinds: []const keyhash.KeyKind) bool {
        return kinds.len == 1 and kinds[0] != .f64k and kinds[0] != .boolk;
    }

    fn slot(self: *const Direct, k: i64, m: u64) ?usize {
        if (m != 0) return null;
        const off = k -% self.base.?;
        return if (off < 0 or off >= len) null else @intCast(off);
    }

    /// Sets `base` from the first batch seen.
    pub fn prime(self: *Direct, e: Encoded) void {
        if (self.base != null) return;
        var lo: i64 = std.math.maxInt(i64);
        for (e.keys, 0..) |k, r| if (e.mask(r) == 0 and k < lo) {
            lo = k;
        };
        self.base = if (lo == std.math.maxInt(i64)) 0 else lo;
    }

    /// The rows whose key the array does not hold, in order, after one tight pass.
    pub fn misses(self: *Direct, e: Encoded, a: std.mem.Allocator) ![]u32 {
        self.prime(e);
        const out = try a.alloc(u32, e.keys.len);
        const base = self.base.?;
        var n: usize = 0;
        for (e.keys, 0..) |k, r| {
            const off = k -% base;
            const held = e.mask(r) == 0 and off >= 0 and off < len and self.recs[@intCast(off)] != null;
            out[n] = @intCast(r);
            n += @intFromBool(!held);
        }
        return out[0..n];
    }

    /// `key`'s record, when the array holds it.
    pub inline fn lookup(self: *const Direct, key: FixedKey) ?[*]u8 {
        return self.recs[self.slot(key.vals[0], key.mask) orelse return null];
    }

    /// `key`'s record and partition, when the array holds it.
    pub inline fn find(self: *const Direct, key: FixedKey) ?struct { tail: [*]u8, part: u8 } {
        const i = self.slot(key.vals[0], key.mask) orelse return null;
        return .{ .tail = self.recs[i] orelse return null, .part = self.parts[i] };
    }

    /// The record and partition of each row whose key the array holds.
    pub fn hits(self: *Direct, e: Encoded, tails: []?[*]u8, pidx: []u8) void {
        self.prime(e);
        for (e.keys, tails, pidx, 0..) |k, *t, *pi, r| {
            const i = self.slot(k, e.mask(r)) orelse continue;
            t.* = self.recs[i] orelse continue;
            pi.* = self.parts[i];
        }
    }

    /// Notes `key`'s record, when its key falls in the array's range.
    pub fn note(self: *Direct, key: FixedKey, tail: [*]u8, part: u8) void {
        const i = self.slot(key.vals[0], key.mask) orelse return;
        self.recs[i] = tail;
        self.parts[i] = part;
    }
};

pub const few_groups = 1 << 12;

/// The radix partitions every parallel build and merge splits keys into, by the top
/// bits of the key hash (a table's bucket takes the bottom bits, its salt bits 32-37).
pub const parts = 64;
pub const part_shift = 58;

pub inline fn partOf(h: u64) usize {
    return @intCast(h >> part_shift);
}

/// A batch's items laid out by radix partition, input order within one: partition
/// `p` holds `starts[p]` to `starts[p + 1]`, each item's row, hash, key words and null
/// mask copied there, so a walk over a partition reads all of them in order, not
/// through an index of row numbers. Item `i` is row `rows_in[i]` (row `i`, and no row
/// kept, when `rows_in` is empty), in partition `pof[i]`; the hashes, keys and masks
/// given are by row, `nk` key words a row, and no masks means no NULL. With one
/// partition nothing is copied: the accessors read the given arrays through the rows.
pub fn Runs(comptime K: type) type {
    return struct {
        starts: []u32,
        rows: []const u32,
        hashes: []const u64,
        keys: []const K,
        masks: []const u64,
        nk: usize,
        laid: bool,

        pub fn init(a: std.mem.Allocator, nparts: usize, nk: usize, pof: []const u8, rows_in: []const u32, hashes_in: []const u64, keys_in: []const K, masks_in: []const u64) !@This() {
            const m = pof.len;
            const starts = try a.alloc(u32, nparts + 1);
            if (nparts == 1) {
                starts[0] = 0;
                starts[1] = @intCast(m);
                return .{ .starts = starts, .rows = rows_in, .hashes = hashes_in, .keys = keys_in, .masks = masks_in, .nk = nk, .laid = false };
            }
            const rows = try a.alloc(u32, if (rows_in.len == 0) 0 else m);
            const hashes = try a.alloc(u64, m);
            const keys = try a.alloc(K, m * nk);
            const masks = try a.alloc(u64, if (masks_in.len == 0) 0 else m);
            @memset(starts, 0);
            for (pof) |p| starts[p + 1] += 1;
            for (1..starts.len) |ci| starts[ci] += starts[ci - 1];
            tally(&totals, .scattered, m);
            var at: [parts]u32 = undefined;
            @memcpy(at[0..nparts], starts[0..nparts]);
            for (pof, 0..) |p, i| {
                const d = at[p];
                at[p] += 1;
                const r = if (rows_in.len == 0) i else rows_in[i];
                if (rows.len > 0) rows[d] = @intCast(r);
                hashes[d] = hashes_in[r];
                for (0..nk) |w| keys[d * nk + w] = keys_in[r * nk + w];
                if (masks.len > 0) masks[d] = masks_in[r];
            }
            return .{ .starts = starts, .rows = rows, .hashes = hashes, .keys = keys, .masks = masks, .nk = nk, .laid = true };
        }

        pub inline fn row(self: @This(), d: usize) usize {
            return if (self.rows.len == 0) d else self.rows[d];
        }

        inline fn slot(self: @This(), d: usize) usize {
            return if (self.laid) d else self.row(d);
        }

        pub inline fn hash(self: @This(), d: usize) u64 {
            return self.hashes[self.slot(d)];
        }

        pub inline fn mask(self: @This(), d: usize) u64 {
            return if (self.masks.len == 0) 0 else self.masks[self.slot(d)];
        }

        pub inline fn key(self: @This(), d: usize) []const K {
            return self.keys[self.slot(d) * self.nk ..][0..self.nk];
        }
    };
}

/// Each word through murmur3's 64-bit finalizer. Wyhash's streaming form was a fifth
/// of a 100-group GROUP BY.
pub fn fixedHash(vals: []const i64, mask: u64) u64 {
    var h: u64 = 0x9E3779B97F4A7C15 ^ mask;
    for (vals) |v| h = keyhash.fmix64(h ^ (@as(u64, @bitCast(v)) *% 0xC2B2AE3D27D4EB4F));
    return h;
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

    pub fn base(self: RecBlocks, i: usize) [*]u8 {
        const l = locate(i);
        return self.blocks.items[l.b].ptr + l.off * self.rec_size;
    }

    fn reserve(self: *RecBlocks, i: usize) !void {
        const l = locate(i);
        if (l.b < self.blocks.items.len) return;
        try self.blocks.append(try self.alloc.alignedAlloc(u8, .of(Value), capOf(l.b) * self.rec_size));
    }
};

pub fn FixedStore(comptime P: type) type {
    return struct {
        nkeys: usize,
        payload: *const P,
        recs: RecBlocks,
        len: usize = 0,

        const Rec = struct { keys: []i64, mask: *u64, tail: [*]u8 };

        pub fn init(alloc: std.mem.Allocator, nkeys: usize, payload: *const P) @This() {
            return .{ .nkeys = nkeys, .payload = payload, .recs = .init(alloc, @sizeOf(u64) + nkeys * @sizeOf(i64) + @sizeOf(u64) + payload.size) };
        }

        pub fn hashAt(self: *const @This(), i: usize) u64 {
            return @as(*const u64, @ptrCast(@alignCast(self.recs.base(i)))).*;
        }

        pub fn at(self: @This(), i: usize) Rec {
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
        fn eqlAt(self: *const @This(), i: usize, h: u64, key: FixedKey) bool {
            const base = self.recs.base(i);
            const words: [*]const u64 = @ptrCast(@alignCast(base));
            if (words[0] != h) return false;
            const vals: [*]const i64 = @ptrCast(@alignCast(base + @sizeOf(u64)));
            for (key.vals, 0..) |v, j| if (vals[j] != v) return false;
            return words[1 + key.vals.len] == key.mask;
        }

        pub fn push(self: *@This(), h: u64) !Rec {
            try self.recs.reserve(self.len);
            @as(*u64, @ptrCast(@alignCast(self.recs.base(self.len)))).* = h;
            const rec = self.at(self.len);
            self.len += 1;
            self.payload.clear(rec.tail);
            return rec;
        }
    };
}

pub const FixedKey = struct { vals: []const i64, mask: u64 };

/// The value a fixed-width key word of a column of `kind` stands for, a string's
/// through the table that interned it.
pub fn wordValue(kind: types.TypeKind, raw: i64, strs: ?*const StrTable) Value {
    return switch (kind) {
        .int => .{ .int = raw },
        .time => .{ .time = raw },
        .timestamp => .{ .timestamp = raw },
        .date => .{ .date = @intCast(raw) },
        .bool => .{ .bool = raw != 0 },
        .float => .{ .float = @bitCast(raw) },
        .string => .{ .string = strs.?.at(@intCast(raw)) },
        else => unreachable,
    };
}

pub const GroupTable = struct {
    const salt_bits = 6;
    const idx_bits = 32 - salt_bits;
    const max_groups = (1 << idx_bits) - 1;

    entries: []u32,
    len: usize = 0,
    mask: u64,
    alloc: std.mem.Allocator,
    counts: Counters = [_]std.atomic.Value(u64){.init(0)} ** ncounts,

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

    /// Prefetches the record `h`'s first slot points to, when its salt matches: a
    /// second stage behind `prefetch`, once the slot itself is likely in cache.
    pub fn prefetchRecord(self: *const GroupTable, h: u64, store: anytype) void {
        const e = self.entries[h & self.mask];
        if (e == 0 or (e >> idx_bits) != saltOf(h)) return;
        @prefetch(store.recs.base((e & max_groups) - 1), .{ .rw = .read, .locality = 3 });
    }

    /// Free the entries. Safe to repeat: a table handed on by `GroupMerge.adopt` is left
    /// empty and its owner's cleanup still runs.
    pub fn deinit(self: *GroupTable) void {
        if (stats and self.entries.len != 0) for (&totals, &self.counts) |*t, *c| {
            _ = t.fetchAdd(c.load(.monotonic), .monotonic);
        };
        self.alloc.free(self.entries);
        self.entries = &.{};
    }

    /// Rebuild at twice the size from this table's own entries, taking each hash from its
    /// record, the records read a few entries ahead, and free the outgrown entries
    /// (an arena kept every outgrown table). Walking the old entries writes the new
    /// table nearly in order; walking the records wrote it at random and was slower.
    fn grow(self: *GroupTable, store: anytype) !void {
        tally(&self.counts, .rehashes, 1);
        tally(&self.counts, .moved, self.len);
        const cap = self.entries.len * growth;
        const ne = try self.alloc.alloc(u32, cap);
        @memset(ne, 0);
        const nmask = cap - 1;
        for (self.entries, 0..) |e, ei| {
            if (ei + prefetch_ahead < self.entries.len) {
                const ahead = self.entries[ei + prefetch_ahead];
                if (ahead != 0) @prefetch(store.recs.base((ahead & max_groups) - 1), .{ .rw = .read, .locality = 3 });
            }
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
        tally(&self.counts, .lookups, 1);
        const want = saltOf(h) << idx_bits;
        var i = h & self.mask;
        while (true) : (i = (i + 1) & self.mask) {
            const e = self.entries[i];
            if (e == 0) return null;
            if ((e >> idx_bits) << idx_bits != want) {
                tally(&self.counts, .steps, 1);
                continue;
            }
            const idx = (e & max_groups) - 1;
            if (store.eqlAt(idx, h, key)) return idx;
            tally(&self.counts, .false_hits, 1);
            tally(&self.counts, .steps, 1);
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
        tally(&self.counts, .lookups, 1);
        const want = saltOf(h) << idx_bits;
        var i = h & self.mask;
        while (true) : (i = (i + 1) & self.mask) {
            const e = self.entries[i];
            if (e == 0) {
                self.entries[i] = pack(h, new_slot);
                self.len += 1;
                tally(&self.counts, .inserts, 1);
                return .{ .slot = new_slot, .found = false };
            }
            if ((e >> idx_bits) << idx_bits != want) {
                tally(&self.counts, .steps, 1);
                continue;
            }
            const idx = (e & max_groups) - 1;
            if (store.eqlAt(idx, h, key)) return .{ .slot = idx, .found = true };
            tally(&self.counts, .false_hits, 1);
            tally(&self.counts, .steps, 1);
        }
    }
};

pub fn GroupStore(comptime P: type) type {
    return struct {
        nkeys: usize,
        payload: *const P,
        recs: RecBlocks,
        len: usize = 0,

        const Rec = struct { keys: []Value, tail: [*]u8 };

        pub fn init(alloc: std.mem.Allocator, nkeys: usize, payload: *const P) @This() {
            return .{ .nkeys = nkeys, .payload = payload, .recs = .init(alloc, @sizeOf(u64) + nkeys * @sizeOf(Value) + payload.size) };
        }

        pub fn hashAt(self: *const @This(), i: usize) u64 {
            return @as(*const u64, @ptrCast(@alignCast(self.recs.base(i)))).*;
        }

        pub fn at(self: @This(), i: usize) Rec {
            const base = self.recs.base(i) + @sizeOf(u64);
            const ksz = self.nkeys * @sizeOf(Value);
            return .{
                .keys = @alignCast(std.mem.bytesAsSlice(Value, base[0..ksz])),
                .tail = base + ksz,
            };
        }

        fn eqlAt(self: *const @This(), i: usize, h: u64, key: []const Value) bool {
            if (self.hashAt(i) != h) return false;
            return keyhash.MultiKeyCtx.eql(.{}, key, self.at(i).keys);
        }

        pub fn push(self: *@This(), h: u64) !Rec {
            try self.recs.reserve(self.len);
            @as(*u64, @ptrCast(@alignCast(self.recs.base(self.len)))).* = h;
            const rec = self.at(self.len);
            self.len += 1;
            self.payload.clear(rec.tail);
            return rec;
        }
    };
}

/// The payload of a table that only holds keys.
pub const NoPayload = struct {
    size: usize = 0,

    pub fn clear(_: *const NoPayload, _: [*]u8) void {}
};

pub const StrId = struct { id: u32, h: u64 };

/// The id a string key gets, shared by every lane so equal strings meet; ids follow
/// insertion, so placement hashes the string itself (`StrId.h`) and output order does
/// not follow thread timing.
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

pub const absent_id = std.math.maxInt(u32);

/// One operator's view of the shared `StrTable`: a private cache in front of it, so
/// a string already met costs no lock. `lookup` never interns, for a table frozen
/// by spilling: a string never met is `absent_id`, which no held key can contain.
pub const Interner = struct {
    table: *?*StrTable,
    cache: *std.StringHashMapUnmanaged(StrId),
    alloc: std.mem.Allocator,

    fn shared(self: Interner) !*StrTable {
        if (self.table.*) |t| return t;
        const t = try self.alloc.create(StrTable);
        t.* = StrTable.init(self.alloc);
        self.table.* = t;
        return t;
    }

    pub fn id(self: Interner, s: []const u8) !StrId {
        const gop = try self.cache.getOrPut(self.alloc, s);
        if (!gop.found_existing) {
            gop.key_ptr.* = try self.alloc.dupe(u8, s);
            gop.value_ptr.* = .{ .id = try (try self.shared()).intern(s), .h = std.hash.Wyhash.hash(0x5bd1e995, s) };
        }
        return gop.value_ptr.*;
    }

    pub fn lookup(self: Interner, s: []const u8) StrId {
        if (self.cache.get(s)) |e| return e;
        return .{ .id = absent_id, .h = std.hash.Wyhash.hash(0x5bd1e995, s) };
    }
};

/// A batch's keys as `Encoder` writes them: `nk` words a row (`keys`, the column
/// itself for one INT-like key), the words a row hashes by (`hkeys`, a string's hash
/// where `keys` holds its id; `keys` itself when no key is a string), a null mask a
/// row (none when no key is NULL), and `absent` for a row whose string a frozen table
/// cannot hold.
pub const Encoded = struct {
    nk: usize,
    keys: []i64,
    hkeys: []i64,
    masks: []u64,
    absent: []bool,

    pub inline fn mask(self: Encoded, r: usize) u64 {
        return if (self.masks.len == 0) 0 else self.masks[r];
    }

    pub fn key(self: Encoded, r: usize) FixedKey {
        return .{ .vals = self.keys[r * self.nk ..][0..self.nk], .mask = self.mask(r) };
    }

    pub fn hash(self: Encoded, r: usize) u64 {
        return fixedHash(self.hkeys[r * self.nk ..][0..self.nk], self.mask(r));
    }
};

/// Key columns (`cols`, each of `kinds`, from `keyhash.keyKindOf`) encoded a column at
/// a time: ints, dates and booleans as their value, floats by `keyhash.floatWord`,
/// strings by their interned id. A NULL is word 0 with its bit set in the mask. A
/// dictionary column's ids are kept per column while its dictionary stays the same,
/// as batches of one chunk share theirs.
///
/// With `want` (the column kinds another side's words stand for, as a join's probe
/// has its build's), a column of another kind is read into those words where
/// `keyhash.valueEq` lets its values equal them: an integral FLOAT or DECIMAL as an
/// INT, an INT or DECIMAL as its canonical FLOAT, BYTES as text. A value that cannot
/// equal any key there is `absent`.
pub const Encoder = struct {
    cols: []const usize,
    kinds: []const keyhash.KeyKind,
    has_str: bool,
    memo: []DictMemo,
    want: ?[]const types.TypeKind = null,

    const DictMemo = struct { id: u64 = 0, ents: []StrId = &.{} };

    pub fn init(a: std.mem.Allocator, cols: []const usize, kinds: []const keyhash.KeyKind) !Encoder {
        var has_str = false;
        for (kinds) |k| has_str = has_str or k == .strk;
        const memo = try a.alloc(DictMemo, cols.len);
        @memset(memo, .{});
        return .{ .cols = cols, .kinds = kinds, .has_str = has_str, .memo = memo };
    }

    /// `d`'s values as ids, from the memo while the dictionary is the one last seen (by
    /// its id; one without an id is never remembered). Frozen, nothing is interned and
    /// nothing remembered.
    fn dictIds(self: Encoder, a: std.mem.Allocator, j: usize, d: *const column.Column.Dict, in: Interner, frozen: bool) ![]const StrId {
        const m = &self.memo[j];
        if (!frozen and d.id != 0 and m.id == d.id) return m.ents;
        const ents = try (if (frozen) a else in.alloc).alloc(StrId, d.values.len);
        for (ents, d.values) |*x, v| x.* = if (frozen) in.lookup(v) else try in.id(v);
        if (!frozen and d.id != 0) m.* = .{ .id = d.id, .ents = ents };
        return ents;
    }

    /// Whether column `j` arrives as the kind its words stand for.
    fn native(self: Encoder, j: usize, kind: types.TypeKind) bool {
        const w = self.want orelse return true;
        return kind == w[j] or (w[j] == .string and kind == .bytes);
    }

    pub fn encode(self: Encoder, a: std.mem.Allocator, columns: []const column.Column, n: usize, in: Interner, frozen: bool) !Encoded {
        const nk = self.cols.len;
        var nulls = false;
        for (self.cols) |ci| nulls = nulls or !columns[ci].validity.allSet(n);
        if (nk == 1 and self.kinds[0] == .i64k and !nulls and self.native(0, columns[self.cols[0]].ty.kind)) {
            const raw = @constCast(columns[self.cols[0]].data.i64[0..n]);
            return .{ .nk = 1, .keys = raw, .hkeys = raw, .masks = &.{}, .absent = &.{} };
        }
        const keys = try a.alloc(i64, n * nk);
        var e = Encoded{
            .nk = nk,
            .keys = keys,
            .hkeys = if (self.has_str) try a.alloc(i64, n * nk) else keys,
            .masks = if (nulls) try a.alloc(u64, n) else &.{},
            .absent = if (frozen and (self.has_str or self.want != null)) try a.alloc(bool, n) else &.{},
        };
        @memset(e.masks, 0);
        @memset(e.absent, false);
        for (self.cols, self.kinds, 0..) |ci, kk, j| {
            const col = columns[ci];
            if (!self.native(j, col.ty.kind)) {
                cross(&e, j, kk, col, n);
            } else switch (kk) {
                .i64k => for (0..n) |r| {
                    keys[r * nk + j] = col.data.i64[r];
                },
                .i32k => for (0..n) |r| {
                    keys[r * nk + j] = col.data.i32[r];
                },
                .boolk => for (0..n) |r| {
                    keys[r * nk + j] = @intFromBool(col.data.b[r]);
                },
                .f64k => for (0..n) |r| {
                    keys[r * nk + j] = keyhash.floatWord(col.data.f64[r]);
                },
                .strk => if (col.dict) |d| {
                    const ents = try self.dictIds(a, j, d, in, frozen);
                    for (0..n) |r| {
                        if (!col.validity.get(r)) continue;
                        self.putStr(&e, r, j, ents[d.codes[r]]);
                    }
                } else for (0..n) |r| {
                    if (!col.validity.get(r)) continue;
                    const s = col.data.bytes.at(r);
                    self.putStr(&e, r, j, if (frozen) in.lookup(s) else try in.id(s));
                },
            }
            if (!col.validity.allSet(n)) for (0..n) |r| {
                if (col.validity.get(r)) continue;
                e.masks[r] |= @as(u64, 1) << @intCast(j);
                keys[r * nk + j] = 0;
                if (self.has_str) e.hkeys[r * nk + j] = 0;
            };
            if (self.has_str and kk != .strk) for (0..n) |r| {
                e.hkeys[r * nk + j] = keys[r * nk + j];
            };
        }
        return e;
    }

    /// Column `j` of another kind than its words: numbers converted where they can be
    /// equal, every other row absent.
    fn cross(e: *Encoded, j: usize, kk: keyhash.KeyKind, col: column.Column, n: usize) void {
        for (0..n) |r| {
            if (!col.validity.get(r)) continue;
            const w: ?i64 = switch (kk) {
                .i64k => switch (col.ty.kind) {
                    .float => intOf(col.data.f64[r]),
                    .decimal => intOfDec(col.data.dec[r]),
                    else => null,
                },
                .f64k => switch (col.ty.kind) {
                    .int => keyhash.floatWord(@floatFromInt(col.data.i64[r])),
                    .decimal => keyhash.floatWord(col.data.dec[r].keyF64()),
                    else => null,
                },
                else => null,
            };
            if (w) |x| e.keys[r * e.nk + j] = x else e.absent[r] = true;
        }
    }

    fn intOfDec(d: Decimal) ?i64 {
        if (d.scale >= pow10_i128.len) return if (d.unscaled == 0) 0 else null;
        const p = pow10_i128[d.scale];
        if (@rem(d.unscaled, p) != 0) return null;
        return std.math.cast(i64, @divExact(d.unscaled, p));
    }

    fn intOf(x: f64) ?i64 {
        if (!(x >= -9223372036854775808.0 and x < 9223372036854775808.0) or @trunc(x) != x) return null;
        return @intFromFloat(x);
    }

    fn putStr(_: Encoder, e: *Encoded, r: usize, j: usize, x: StrId) void {
        e.keys[r * e.nk + j] = x.id;
        e.hkeys[r * e.nk + j] = @bitCast(x.h);
        if (x.id == absent_id and e.absent.len > 0) e.absent[r] = true;
    }
};
