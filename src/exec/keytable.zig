//! The key table every hash operator shares: keys encoded column at a time into
//! fixed words under `keyhash`'s rules (`Encoder`, strings through an `Interner`),
//! one hash over them (`fixedHash`), a salted `u32` slot table (`GroupTable`) over
//! records in blocks (`FixedStore` for fixed-width keys, `GroupStore` for boxed
//! ones), each record's tail a payload `P` with a `size` and a `clear`: an
//! aggregate's states, or nothing for a set of keys. A direct table serves small
//! integer keys. `GroupTable.find` probes without inserting, for a table frozen by
//! spilling. `probe` is the one probe-or-insert loop over a batch's keys; an operator
//! supplies only what a row does once its key is found, made or missed.

const Decimal = @import("value.zig").Decimal;
const Value = @import("value.zig").Value;
const decimal = @import("decimal.zig");
const column = @import("column.zig");
const keyhash = @import("keyhash.zig");
const textconv = @import("textconv.zig");
const types = @import("../lang/types.zig");
const std = @import("std");
const stats = @import("build_options").stats;

pub const prefetch_ahead = 8;

/// What a `-Dstats` build counts, per table and then in all: lookups, slots probed
/// past the first, salt matches on another key, inserts, rehashes and the entries
/// they moved, items laid out in partition runs, and a bounded table's clears and
/// the keys it took as new without probing.
pub const Count = enum { lookups, steps, false_hits, inserts, rehashes, moved, scattered, clears, skipped };

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
    try w.print("key tables: {d} lookups, {d:.3} extra slots and {d:.3} false salt hits a lookup, {d} inserts, {d} rehashes moving {d} entries, {d} items laid out in runs, {d} clears, {d} keys taken unprobed\n", .{
        v[@intFromEnum(Count.lookups)],
        @as(f64, @floatFromInt(v[@intFromEnum(Count.steps)])) / lookups,
        @as(f64, @floatFromInt(v[@intFromEnum(Count.false_hits)])) / lookups,
        v[@intFromEnum(Count.inserts)],
        v[@intFromEnum(Count.rehashes)],
        v[@intFromEnum(Count.moved)],
        v[@intFromEnum(Count.scattered)],
        v[@intFromEnum(Count.clears)],
        v[@intFromEnum(Count.skipped)],
    });
}

/// A direct array for up to `max_keys` small-range keys: each key word, less its
/// `base`, takes `bits` bits of the slot (all 14 for one key, 7 each for two), so a
/// row finds its record at `recs[slot]` with no hashing. A key's `base` is its
/// smallest word in the first batch, so a range starting anywhere sits wholly inside;
/// a row past any key's range, or with a NULL key, is probed as before. Two string
/// keys (TPC-H Q1's flags, interned to small ids) skip the hash table this way.
pub const Direct = struct {
    recs: []?[*]u8,
    parts: []u8,
    nk: usize,
    bits: u6,
    base: [max_keys]i64 = undefined,
    primed: bool = false,

    pub const len = 1 << 14;
    pub const max_keys = 4;

    pub fn init(gpa: std.mem.Allocator, nk: usize) !Direct {
        std.debug.assert(nk >= 1 and nk <= max_keys);
        const recs = try gpa.alloc(?[*]u8, len);
        @memset(recs, null);
        return .{ .recs = recs, .parts = try gpa.alloc(u8, len), .nk = nk, .bits = @intCast(14 / nk) };
    }

    pub fn deinit(self: Direct, gpa: std.mem.Allocator) void {
        gpa.free(self.recs);
        gpa.free(self.parts);
    }

    /// Whether `keys` (one word a row a key, of kinds this array serves) can use one.
    /// A float's word is its bits, never a small range; a lone BOOLEAN has two groups.
    pub fn serves(kinds: []const keyhash.KeyKind) bool {
        if (kinds.len == 0 or kinds.len > max_keys) return false;
        for (kinds) |k| if (k == .f64k) return false;
        return kinds.len > 1 or kinds[0] != .boolk;
    }

    inline fn slot(self: *const Direct, keys: []const i64, m: u64) ?usize {
        if (m != 0) return null;
        const span: i64 = @as(i64, 1) << self.bits;
        var at: usize = 0;
        for (keys[0..self.nk], self.base[0..self.nk]) |k, b| {
            const off = k -% b;
            if (off < 0 or off >= span) return null;
            at = (at << self.bits) | @as(usize, @intCast(off));
        }
        return at;
    }

    /// Sets each key's `base` from the first batch seen.
    pub fn prime(self: *Direct, e: Encoded) void {
        if (self.primed) return;
        self.primed = true;
        var lo: [max_keys]i64 = @splat(std.math.maxInt(i64));
        const rows = e.keys.len / self.nk;
        for (0..rows) |r| if (e.mask(r) == 0) {
            for (e.keys[r * self.nk ..][0..self.nk], lo[0..self.nk]) |k, *l| l.* = @min(l.*, k);
        };
        for (lo[0..self.nk], self.base[0..self.nk]) |l, *b| b.* = if (l == std.math.maxInt(i64)) 0 else l;
    }

    /// The rows whose key the array does not hold, in order, after one tight pass.
    pub fn misses(self: *Direct, e: Encoded, a: std.mem.Allocator) ![]u32 {
        self.prime(e);
        const rows = e.keys.len / self.nk;
        const out = try a.alloc(u32, rows);
        var n: usize = 0;
        for (0..rows) |r| {
            const held = if (self.slot(e.keys[r * self.nk ..][0..self.nk], e.mask(r))) |i| self.recs[i] != null else false;
            out[n] = @intCast(r);
            n += @intFromBool(!held);
        }
        return out[0..n];
    }

    /// `key`'s record, when the array holds it.
    pub inline fn lookup(self: *const Direct, key: FixedKey) ?[*]u8 {
        return self.recs[self.slot(key.vals, key.mask) orelse return null];
    }

    /// `key`'s record and partition, when the array holds it.
    pub inline fn find(self: *const Direct, key: FixedKey) ?struct { tail: [*]u8, part: u8 } {
        const i = self.slot(key.vals, key.mask) orelse return null;
        return .{ .tail = self.recs[i] orelse return null, .part = self.parts[i] };
    }

    /// The record and partition of each row whose key the array holds.
    pub fn hits(self: *Direct, e: Encoded, tails: []?[*]u8, pidx: []u8) void {
        self.prime(e);
        for (tails, pidx, 0..) |*t, *pi, r| {
            const i = self.slot(e.keys[r * self.nk ..][0..self.nk], e.mask(r)) orelse continue;
            t.* = self.recs[i] orelse continue;
            pi.* = self.parts[i];
        }
    }

    /// Notes `key`'s record, when its key falls in the array's range.
    pub fn note(self: *Direct, key: FixedKey, tail: [*]u8, part: u8) void {
        const i = self.slot(key.vals, key.mask) orelse return;
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
        starts: []const u32,
        rows: []const u32,
        hashes: []const u64,
        keys: []const K,
        masks: []const u64,
        nk: usize,
        laid: bool,

        /// The given arrays read in place, item `d` being row `rows[d]` (row `d` when
        /// `rows` is empty), with no partitions.
        pub fn over(nk: usize, rows: []const u32, hashes: []const u64, keys: []const K, masks: []const u64) @This() {
            return .{ .starts = &.{}, .rows = rows, .hashes = hashes, .keys = keys, .masks = masks, .nk = nk, .laid = false };
        }

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

        pub inline fn probeKey(self: @This(), d: usize) if (K == i64) FixedKey else []const K {
            return if (K == i64) .{ .vals = self.key(d), .mask = self.mask(d) } else self.key(d);
        }
    };
}

/// Boxed keys read from columns `idx` of `cols` a row at a time into `buf`, for keys
/// whose rows a table may alias: item `d` is row `rows[d]`, its hash `hashes` by row.
pub const ValueRows = struct {
    cols: []const column.Column,
    idx: []const usize,
    buf: []Value,
    hashes: []const u64,
    rows: []const u32,

    pub inline fn row(self: ValueRows, d: usize) usize {
        return self.rows[d];
    }

    pub inline fn hash(self: ValueRows, d: usize) u64 {
        return self.hashes[self.rows[d]];
    }

    pub inline fn probeKey(self: ValueRows, d: usize) []const Value {
        const r = self.rows[d];
        for (self.idx, self.buf) |ci, *v| v.* = self.cols[ci].getValue(r);
        return self.buf;
    }
};

/// How a new record keeps a boxed key: the probe's values themselves (`alias`, for
/// rows that outlive the table), copied into the store's allocator (`dupe`), or
/// canonicalized and copied (`canon`). A fixed key's words are always copied.
pub const KeyCopy = enum { alias, dupe, canon };

/// What a probe hook settles for a row before its lookup: look it up (`probe`), take it
/// as handled (`done`), or treat its key as absent (`miss`).
pub const Early = enum { probe, done, miss };

pub fn Hit(comptime Store: type) type {
    return struct { rec: Store.Rec, slot: u32, found: bool };
}

/// The record holding `key` in `store` under `table`, pushed when new with its key
/// kept as `copy` says. A bounded table that skips its lookup pushes a new record.
pub inline fn upsert(table: *GroupTable, store: anytype, h: u64, key: anytype, comptime copy: KeyCopy) !Hit(@TypeOf(store.*)) {
    const f = try table.getOrPut(h, key, store, @intCast(store.len));
    const rec = if (f.found) store.at(f.slot) else try store.pushKey(h, key, copy);
    return .{ .rec = rec, .slot = f.slot, .found = f.found };
}

/// The probe-or-insert loop every hash operator runs: items `lo..hi` of `items` (a
/// `Runs` or `ValueRows`), each looked up in `v.table(h)` over `v.store(h)`, or the
/// one table `v.tb` over `v.st`, with the slot `prefetch_ahead` items on prefetched.
/// `v.early(r, key)`, when declared, settles row `r` first. Each row looked up then
/// has `v.hit(r, h, key, hit)`, its record made by `upsert`. When `v` has a `frozen`
/// field and it is set, nothing is inserted: a key not held, or a row `early` calls
/// absent, has `v.miss(r, h)` instead.
pub fn probe(comptime Store: type, comptime copy: KeyCopy, items: anytype, lo: usize, hi: usize, v: anytype) !void {
    const V = @typeInfo(@TypeOf(v)).pointer.child;
    const can_freeze = @hasField(V, "frozen");
    for (lo..hi) |d| {
        if (d + prefetch_ahead < hi) {
            const ph = items.hash(d + prefetch_ahead);
            tableOf(v, ph).prefetch(ph);
        }
        const r = items.row(d);
        const h = items.hash(d);
        const key = items.probeKey(d);
        if (@hasDecl(V, "early")) switch (v.early(r, key)) {
            .probe => {},
            .done => continue,
            .miss => {
                if (can_freeze) try v.miss(r, h);
                continue;
            },
        };
        const t = tableOf(v, h);
        const st: *Store = if (@hasDecl(V, "store")) v.store(h) else v.st;
        if (can_freeze and v.frozen) {
            if (t.find(h, key, st)) |slot| {
                try v.hit(r, h, key, Hit(Store){ .rec = st.at(slot), .slot = slot, .found = true });
            } else try v.miss(r, h);
            continue;
        }
        try v.hit(r, h, key, try upsert(t, st, h, key, copy));
    }
}

inline fn tableOf(v: anytype, h: u64) *GroupTable {
    return if (@hasDecl(@typeInfo(@TypeOf(v)).pointer.child, "table")) v.table(h) else v.tb;
}

/// The boxed keys of rows `0..n` of `cols` at `idx` into `probes`, `idx.len` a row,
/// and each row's `keyhash.MultiKeyCtx` hash into `hashes`.
pub fn hashBatch(cols: []const column.Column, idx: []const usize, n: usize, probes: []Value, hashes: []u64) void {
    const nk = idx.len;
    for (0..n) |r| {
        const key = probes[r * nk ..][0..nk];
        for (idx, key) |ci, *v| v.* = cols[ci].getValue(r);
        hashes[r] = keyhash.MultiKeyCtx.hash(.{}, key);
    }
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

        pub fn pushKey(self: *@This(), h: u64, key: FixedKey, comptime _: KeyCopy) !Rec {
            const nr = try self.push(h);
            @memcpy(nr.keys, key.vals);
            nr.mask.* = key.mask;
            return nr;
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
    bounded: bool = false,
    max: usize = 0,
    from: usize = 0,
    probes: u32 = 0,
    skip: u32 = 0,

    const skip_keys = 1 << 16;
    const thin_pct = 95;

    /// A table of `cap` entries that never grows past `max`, for a lane's partial fold
    /// whose records a merge combines. Full, it judges the keys taken since it last
    /// grew or cleared: below `max` and not thin, it grows, so a key set larger than
    /// `cap` but repeated is still folded in the lane; otherwise it forgets its keys
    /// and starts again, so a key met again becomes a second record. When the keys it
    /// judged were reduced by less than 5% (DuckDB's `MaybeSkipLookups`), the next
    /// `skip_keys` keys are taken as new without probing, then probing resumes and the
    /// cycle is judged again.
    pub fn initBounded(alloc: std.mem.Allocator, cap: usize, max: usize) !GroupTable {
        var t = try init(alloc, cap);
        t.bounded = true;
        t.max = @max(cap, max);
        return t;
    }

    /// The smallest power of two, at least 256, holding `n` keys under the load limit.
    pub fn capFor(n: usize) usize {
        var cap: usize = 256;
        while ((n + 1) * 10 >= cap * 7) cap *= growth;
        return cap;
    }

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

    /// Whether one more key would make the table grow.
    pub inline fn full(self: *const GroupTable) bool {
        return (self.len + 1) * 10 >= self.entries.len * 7;
    }

    /// Forget every key, keeping the records and the entries' memory, for a bounded
    /// table whose keys may then be met again as new records.
    pub fn clear(self: *GroupTable) void {
        tally(&self.counts, .clears, 1);
        @memset(self.entries, 0);
        self.len = 0;
        self.from = 0;
        self.probes = 0;
    }

    /// Free the entries. Safe to repeat: a lane's sets and a merge each free theirs
    /// once merged and again at cleanup.
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
        if (self.bounded) {
            if (self.skip > 0) {
                self.skip -= 1;
                tally(&self.counts, .skipped, 1);
                return .{ .slot = new_slot, .found = false };
            }
            if (self.full()) {
                const thin = (self.len - self.from) * 100 >= @as(usize, self.probes) * thin_pct;
                if (!thin and self.entries.len < self.max) {
                    try self.grow(store);
                    self.from = self.len;
                    self.probes = 0;
                } else {
                    if (thin) self.skip = skip_keys;
                    self.clear();
                }
            }
            self.probes += 1;
        } else if (self.full()) try self.grow(store);
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

        pub fn pushKey(self: *@This(), h: u64, key: []const Value, comptime copy: KeyCopy) !Rec {
            const nr = try self.push(h);
            switch (copy) {
                .alias => @memcpy(nr.keys, key),
                .dupe => for (nr.keys, key) |*o, v| {
                    o.* = try keyhash.ownValue(self.recs.alloc, v);
                },
                .canon => for (nr.keys, key) |*o, v| {
                    o.* = try keyhash.ownValue(self.recs.alloc, keyhash.canonValue(v));
                },
            }
            return nr;
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
                    .float => textconv.floatToInt(col.data.f64[r], .exact),
                    .decimal => decimal.exactInt(column.decimals(col).at(r)),
                    else => null,
                },
                .f64k => switch (col.ty.kind) {
                    .int => keyhash.floatWord(@floatFromInt(col.data.i64[r])),
                    .decimal => keyhash.floatWord(column.decimals(col).at(r).keyF64()),
                    else => null,
                },
                else => null,
            };
            if (w) |x| e.keys[r * e.nk + j] = x else e.absent[r] = true;
        }
    }

    fn putStr(_: Encoder, e: *Encoded, r: usize, j: usize, x: StrId) void {
        e.keys[r * e.nk + j] = x.id;
        e.hkeys[r * e.nk + j] = @bitCast(x.h);
        if (x.id == absent_id and e.absent.len > 0) e.absent[r] = true;
    }
};

test "a bounded table clears when full, then skips probing while its cycles barely reduce" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const payload = NoPayload{};
    const Put = struct {
        fn put(tb: *GroupTable, s: *FixedStore(NoPayload), k: i64) !bool {
            const key = FixedKey{ .vals = &.{k}, .mask = 0 };
            const h = fixedHash(key.vals, 0);
            const f = try tb.getOrPut(h, key, s, @intCast(s.len));
            if (!f.found) {
                const nr = try s.push(h);
                nr.keys[0] = k;
                nr.mask.* = 0;
            }
            return f.found;
        }
    };

    var few = try GroupTable.initBounded(std.testing.allocator, 16, 16);
    defer few.deinit();
    var st_few = FixedStore(NoPayload).init(a, 1, &payload);
    for (0..400) |i| _ = try Put.put(&few, &st_few, @intCast(i % 5));
    try std.testing.expectEqual(@as(usize, 5), st_few.len);
    try std.testing.expectEqual(@as(u32, 0), few.skip);

    var twice = try GroupTable.initBounded(std.testing.allocator, 16, 16);
    defer twice.deinit();
    var st_twice = FixedStore(NoPayload).init(a, 1, &payload);
    for (0..24) |i| _ = try Put.put(&twice, &st_twice, @intCast(i / 2));
    try std.testing.expectEqual(@as(u32, 0), twice.skip);
    try std.testing.expect(!try Put.put(&twice, &st_twice, 0));
    try std.testing.expectEqual(@as(usize, 14), st_twice.len);

    var thin = try GroupTable.initBounded(std.testing.allocator, 16, 16);
    defer thin.deinit();
    var st_thin = FixedStore(NoPayload).init(a, 1, &payload);
    for (0..12) |k| try std.testing.expect(!try Put.put(&thin, &st_thin, @intCast(k)));
    try std.testing.expect(thin.skip > 0);
    try std.testing.expect(!try Put.put(&thin, &st_thin, 11));
    try std.testing.expectEqual(@as(usize, 13), st_thin.len);
    try std.testing.expectEqual(@as(usize, 1), thin.len);
}

test "a bounded table grows toward its max while its keys repeat, then clears" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const payload = NoPayload{};
    const Put = struct {
        fn put(tb: *GroupTable, s: *FixedStore(NoPayload), k: i64) !void {
            const key = FixedKey{ .vals = &.{k}, .mask = 0 };
            const h = fixedHash(key.vals, 0);
            const f = try tb.getOrPut(h, key, s, @intCast(s.len));
            if (!f.found) {
                const nr = try s.push(h);
                nr.keys[0] = k;
                nr.mask.* = 0;
            }
        }
    };

    var roomy = try GroupTable.initBounded(std.testing.allocator, 16, 64);
    defer roomy.deinit();
    var st_roomy = FixedStore(NoPayload).init(a, 1, &payload);
    for (0..80) |i| try Put.put(&roomy, &st_roomy, @intCast(i / 2));
    try std.testing.expectEqual(@as(usize, 64), roomy.entries.len);
    try std.testing.expectEqual(@as(usize, 40), st_roomy.len);
    try std.testing.expectEqual(@as(usize, 40), roomy.len);

    var capped = try GroupTable.initBounded(std.testing.allocator, 16, 32);
    defer capped.deinit();
    var st_capped = FixedStore(NoPayload).init(a, 1, &payload);
    for (0..80) |i| try Put.put(&capped, &st_capped, @intCast(i / 2));
    try std.testing.expectEqual(@as(usize, 32), capped.entries.len);
    try std.testing.expect(st_capped.len > 40);
    try std.testing.expect(capped.len < 40);
    try std.testing.expectEqual(@as(u32, 0), capped.skip);
}

/// A `probe` side for tests: each row's slot, a row of a missing key marked.
fn TestSide(comptime Store: type) type {
    return struct {
        tb: *GroupTable,
        st: *Store,
        slots: []u32,
        frozen: bool = false,
        misses: usize = 0,

        pub fn hit(c: *@This(), r: usize, _: u64, key: anytype, x: Hit(Store)) !void {
            if (Store == FixedStore(NoPayload)) {
                try std.testing.expectEqualSlices(i64, key.vals, x.rec.keys);
                try std.testing.expectEqual(key.mask, x.rec.mask.*);
            } else try std.testing.expect(keyhash.MultiKeyCtx.eql(.{}, key, x.rec.keys));
            c.slots[r] = x.slot;
        }

        pub fn miss(c: *@This(), r: usize, _: u64) !void {
            c.slots[r] = std.math.maxInt(u32);
            c.misses += 1;
        }
    };
}

test "Direct: two and three keys each take their own bits, from their own base; a key past its range, a NULL or a float misses" {
    var recs: [3]u8 = undefined;
    for ([_]usize{ 2, 3 }) |nk| {
        var d = try Direct.init(std.testing.allocator, nk);
        defer d.deinit(std.testing.allocator);
        const span: i64 = @as(i64, 1) << d.bits;
        var keys: [6]i64 = undefined;
        for (0..2) |r| for (0..nk) |j| {
            keys[r * nk + j] = 100 * @as(i64, @intCast(j)) + @as(i64, @intCast(r)) - 5;
        };
        d.prime(.{ .nk = nk, .keys = keys[0 .. 2 * nk], .hkeys = &.{}, .masks = &.{}, .absent = &.{} });
        const a = [_]i64{ -5, 95, 195 };
        const b = [_]i64{ -4, 95, 195 };
        const c = [_]i64{ -5, 96, 195 };
        const far = [_]i64{ -5 + span, 95, 195 };
        const below = [_]i64{ -6, 95, 195 };
        d.note(.{ .vals = a[0..nk], .mask = 0 }, recs[0..].ptr, 1);
        d.note(.{ .vals = b[0..nk], .mask = 0 }, recs[1..].ptr, 2);
        d.note(.{ .vals = c[0..nk], .mask = 0 }, recs[2..].ptr, 3);
        try std.testing.expectEqual(@as(?[*]u8, recs[0..].ptr), d.lookup(.{ .vals = a[0..nk], .mask = 0 }));
        try std.testing.expectEqual(@as(u8, 2), d.find(.{ .vals = b[0..nk], .mask = 0 }).?.part);
        try std.testing.expectEqual(@as(?[*]u8, recs[2..].ptr), d.lookup(.{ .vals = c[0..nk], .mask = 0 }));
        try std.testing.expectEqual(@as(?[*]u8, null), d.lookup(.{ .vals = far[0..nk], .mask = 0 }));
        try std.testing.expectEqual(@as(?[*]u8, null), d.lookup(.{ .vals = below[0..nk], .mask = 0 }));
        try std.testing.expectEqual(@as(?[*]u8, null), d.lookup(.{ .vals = a[0..nk], .mask = 1 }));
    }
    try std.testing.expect(Direct.serves(&.{ .i64k, .i64k }));
    try std.testing.expect(!Direct.serves(&.{ .i64k, .f64k }));
    try std.testing.expect(!Direct.serves(&.{.boolk}));
    try std.testing.expect(Direct.serves(&.{ .boolk, .i32k }));
    try std.testing.expect(!Direct.serves(&.{ .i64k, .i64k, .i64k, .i64k, .i64k }));
}

test "probe: two-column keys with NULLs just past each table bound keep one record a key, frozen tables miss new keys" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const payload = NoPayload{};
    for ([_]usize{ 1 << 10, few_groups, Direct.len }) |bound| {
        const nkeys = bound + 1;
        const n = 2 * nkeys + 7;
        const keys = try a.alloc(i64, 2 * n);
        const masks = try a.alloc(u64, n);
        const hashes = try a.alloc(u64, n);
        for (0..n) |r| {
            const k = (r * 7919) % nkeys;
            keys[2 * r] = @intCast(k / 3);
            keys[2 * r + 1] = if (k % 3 == 0) 0 else @intCast(k % 3);
            masks[r] = if (k % 3 == 0) 2 else 0;
            hashes[r] = fixedHash(keys[2 * r ..][0..2], masks[r]);
        }
        const runs = Runs(i64).over(2, &.{}, hashes, keys, masks);
        var tb = try GroupTable.init(std.testing.allocator, 16);
        defer tb.deinit();
        var st = FixedStore(NoPayload).init(a, 2, &payload);
        var side = TestSide(FixedStore(NoPayload)){ .tb = &tb, .st = &st, .slots = try a.alloc(u32, n) };
        try probe(FixedStore(NoPayload), .alias, &runs, 0, n, &side);
        try std.testing.expectEqual(nkeys, st.len);
        for (0..n - nkeys) |r| try std.testing.expectEqual(side.slots[r], side.slots[r + nkeys]);

        var bounded = try GroupTable.initBounded(std.testing.allocator, 1 << 10, 1 << 10);
        defer bounded.deinit();
        var bst = FixedStore(NoPayload).init(a, 2, &payload);
        var bside = TestSide(FixedStore(NoPayload)){ .tb = &bounded, .st = &bst, .slots = try a.alloc(u32, n) };
        try probe(FixedStore(NoPayload), .alias, &runs, 0, n, &bside);
        try std.testing.expect(bst.len >= nkeys);

        const held = try a.dupe(u32, side.slots);
        for (0..n) |r| if (r % 2 == 0) {
            keys[2 * r] += @intCast(bound);
            hashes[r] = fixedHash(keys[2 * r ..][0..2], masks[r]);
        };
        side.frozen = true;
        try probe(FixedStore(NoPayload), .alias, &runs, 0, n, &side);
        try std.testing.expectEqual(nkeys, st.len);
        try std.testing.expectEqual((n + 1) / 2, side.misses);
        for (0..n) |r| if (r % 2 == 1) try std.testing.expectEqual(held[r], side.slots[r]);
    }
}

test "probe: boxed string keys are copied once a key and found again" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const payload = NoPayload{};
    const n = 3000;
    const vals = try a.alloc(Value, 2 * n);
    const hashes = try a.alloc(u64, n);
    for (0..n) |r| {
        vals[2 * r] = .{ .string = try std.fmt.allocPrint(a, "k{d}", .{r % 1025}) };
        vals[2 * r + 1] = if (r % 1025 % 5 == 0) .null else .{ .float = if (r % 2 == 0) -0.0 else 0.0 };
        hashes[r] = keyhash.MultiKeyCtx.hash(.{}, vals[2 * r ..][0..2]);
    }
    var tb = try GroupTable.init(std.testing.allocator, 16);
    defer tb.deinit();
    var st = GroupStore(NoPayload).init(a, 2, &payload);
    var side = TestSide(GroupStore(NoPayload)){ .tb = &tb, .st = &st, .slots = try a.alloc(u32, n) };
    try probe(GroupStore(NoPayload), .canon, &Runs(Value).over(2, &.{}, hashes, vals, &.{}), 0, n, &side);
    try std.testing.expectEqual(@as(usize, 1025), st.len);
    for (0..st.len) |i| {
        const k = st.at(i).keys;
        try std.testing.expect(k[0].string.ptr != vals[2 * i].string.ptr);
        if (k[1] == .float) try std.testing.expectEqual(@as(u64, 0), @as(u64, @bitCast(k[1].float)));
    }
}

test "cross: a FLOAT read as an INT word is textconv's exact float-to-int, as the old intOf" {
    const Old = struct {
        fn intOf(x: f64) ?i64 {
            if (!(x >= -9223372036854775808.0 and x < 9223372036854775808.0) or @trunc(x) != x) return null;
            return @intFromFloat(x);
        }
    };
    const edge = [_]f64{
        0,                     -0.0,                   1,                       -1,                         0.5,                -0.5,
        1.0000000000000002,    0x1p53,                 0x1p53 + 2,              -0x1p53,                    0x1p62,             0x1p63,
        -0x1p63,               0x1p63 - 1024,          -0x1p63 - 2048,          std.math.inf(f64),          -std.math.inf(f64), std.math.nan(f64),
        -std.math.nan(f64),    std.math.floatMax(f64), -std.math.floatMax(f64), std.math.floatMin(f64),     1e300,              123456789.0,
        9223372036854774784.0, -123456789.25,          -9223372036854775808.0,  std.math.floatTrueMin(f64),
    };
    for (edge) |x| try std.testing.expectEqual(Old.intOf(x), textconv.floatToInt(x, .exact));
    var prng = std.Random.DefaultPrng.init(0x1d0f);
    const rand = prng.random();
    for (0..100_000) |_| {
        const x: f64 = @bitCast(rand.int(u64));
        try std.testing.expectEqual(Old.intOf(x), textconv.floatToInt(x, .exact));
        const y = @trunc(@as(f64, @floatFromInt(rand.int(i64))) / @as(f64, @floatFromInt(@as(u64, 1) << rand.int(u6))));
        try std.testing.expectEqual(Old.intOf(y), textconv.floatToInt(y, .exact));
    }
}

test "hashBatch: each row's boxed keys and their MultiKeyCtx hash" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var ib = column.Builder.init(a, types.Type.init(.int));
    var sb = column.Builder.init(a, types.Type.init(.string));
    for ([_]?i64{ 1, null, 3 }, [_]?[]const u8{ "x", "y", null }) |i, s| {
        try ib.append(if (i) |x| .{ .int = x } else .null);
        try sb.append(if (s) |x| .{ .string = x } else .null);
    }
    const cols = [_]column.Column{ try ib.finish(), try sb.finish() };
    var probes: [6]Value = undefined;
    var hashes: [3]u64 = undefined;
    hashBatch(&cols, &.{ 1, 0 }, 3, &probes, &hashes);
    for (0..3) |r| {
        const want = [_]Value{ cols[1].getValue(r), cols[0].getValue(r) };
        try std.testing.expect(keyhash.MultiKeyCtx.eql(.{}, &want, probes[2 * r ..][0..2]));
        try std.testing.expectEqual(keyhash.MultiKeyCtx.hash(.{}, &want), hashes[r]);
    }
}
