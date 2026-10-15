//! The key table every hash operator shares: keys encoded column at a time into
//! fixed words under `keyhash`'s rules (`Encoder`, strings through an `Interner`),
//! one hash over them (`fixedHash`), a salted `u32` slot table (`GroupTable`) over
//! records in blocks (`FixedStore` for fixed-width keys, `GroupStore` for boxed
//! ones), each record's tail a payload `P` with a `size` and a `clear`: an
//! aggregate's states, or nothing for a set of keys. A direct table serves small
//! integer keys. `GroupTable.find` probes without inserting, for a table frozen by
//! spilling.

const Value = @import("value.zig").Value;
const column = @import("column.zig");
const keyhash = @import("keyhash.zig");
const std = @import("std");

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

/// A batch's keys as `Encoder` writes them: `nk` words a row (`keys`), the words a row
/// hashes by (`hkeys`, a string's hash where `keys` holds its id; `keys` itself when
/// no key is a string), a null mask a row, and `absent` for a row whose string a
/// frozen table cannot hold.
pub const Encoded = struct {
    nk: usize,
    keys: []i64,
    hkeys: []i64,
    masks: []u64,
    absent: []bool,

    pub fn key(self: Encoded, r: usize) FixedKey {
        return .{ .vals = self.keys[r * self.nk ..][0..self.nk], .mask = self.masks[r] };
    }

    pub fn hash(self: Encoded, r: usize) u64 {
        return fixedHash(self.hkeys[r * self.nk ..][0..self.nk], self.masks[r]);
    }
};

/// Key columns (`cols`, each of `kinds`, from `keyhash.keyKindOf`) encoded a column at
/// a time: ints, dates and booleans as their value, floats by `keyhash.floatWord`,
/// strings by their interned id. A NULL is word 0 with its bit set in the mask.
pub const Encoder = struct {
    cols: []const usize,
    kinds: []const keyhash.KeyKind,
    has_str: bool,

    pub fn init(cols: []const usize, kinds: []const keyhash.KeyKind) Encoder {
        var has_str = false;
        for (kinds) |k| has_str = has_str or k == .strk;
        return .{ .cols = cols, .kinds = kinds, .has_str = has_str };
    }

    pub fn encode(self: Encoder, a: std.mem.Allocator, columns: []const column.Column, n: usize, in: Interner, frozen: bool) !Encoded {
        const nk = self.cols.len;
        const keys = try a.alloc(i64, n * nk);
        var e = Encoded{
            .nk = nk,
            .keys = keys,
            .hkeys = if (self.has_str) try a.alloc(i64, n * nk) else keys,
            .masks = try a.alloc(u64, n),
            .absent = if (frozen and self.has_str) try a.alloc(bool, n) else &.{},
        };
        @memset(e.masks, 0);
        @memset(e.absent, false);
        for (self.cols, self.kinds, 0..) |ci, kk, j| {
            const col = columns[ci];
            switch (kk) {
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
                    const ents = try a.alloc(StrId, d.values.len);
                    for (ents, d.values) |*x, v| x.* = if (frozen) in.lookup(v) else try in.id(v);
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

    fn putStr(_: Encoder, e: *Encoded, r: usize, j: usize, x: StrId) void {
        e.keys[r * e.nk + j] = x.id;
        e.hkeys[r * e.nk + j] = @bitCast(x.h);
        if (x.id == absent_id and e.absent.len > 0) e.absent[r] = true;
    }
};
