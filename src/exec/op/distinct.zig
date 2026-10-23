//! `DISTINCT` and `DISTINCT ON`: the first row per key, in input order. A
//! `partial` one (a lane's work item, whose rows a merge dedups again) keeps its keys
//! in a bounded table (`GroupTable.initBounded`) and may pass a key's later row on.
//! Its one table is not split into partitions the way an aggregate's lane tables
//! are, so it starts at `lane_min` entries and may grow to `lane_max` while it folds.
//!
//! With a `space` (and no `track_ords`, which only the lanes set) it spills past
//! `spill_at` by the scheme in `spill_parts.zig`: once frozen, a row whose key was
//! seen is dropped as before, a row whose key was not goes to the spill file of
//! its key hash, and each file is deduplicated by a fresh `Distinct` after the
//! input ends. A key's first row is then either already out or the first of its
//! file, so DISTINCT ON still keeps the first row per key; only the order of the
//! output changes.

const Batch = @import("../batch.zig").Batch;
const ErrCtx = @import("../op.zig").ErrCtx;
const Op = @import("../op.zig").Op;
const Space = @import("../space.zig").Space;
const spill_parts = @import("spill_parts.zig");
const Stats = @import("../op.zig").Stats;
const Value = @import("../value.zig").Value;
const column = @import("../column.zig");
const keyhash = @import("../keyhash.zig");
const keytable = @import("../keytable.zig");
const lane_min = 1 << 14;
const lane_max = 1 << 18;
const std = @import("std");
const types = @import("../../lang/types.zig");
const Scan = @import("../op.zig").Scan;
const TestSource = @import("testing_util.zig").TestSource;
const expectSameRows = @import("testing_util.zig").expectSameRows;
const lessStr = @import("testing_util.zig").lessStr;
const intBatch = @import("testing_util.zig").intBatch;
const int_schema = @import("testing_util.zig").int_schema;
const strBatch = @import("testing_util.zig").strBatch;
const testing = std.testing;

pub const Distinct = struct {
    stats: Stats = .{},
    child: Op,
    in_schema: *const types.Schema,
    keys: ?[]const usize,
    state: std.mem.Allocator,
    /// The arena `state` comes from when this operator owns it, freed by `release`.
    own_state: ?*std.heap.ArenaAllocator = null,
    gpa: std.mem.Allocator,
    seen: ?Seen = null,
    strs: ?*keytable.StrTable = null,
    str_cache: std.StringHashMapUnmanaged(keytable.StrId) = .empty,
    shared_strs: ?keytable.Interner = null,
    scratch: ?std.heap.ArenaAllocator = null,
    track_ords: bool = false,
    partial: bool = false,
    ords: []const u64 = &.{},
    seen_rows: u64 = 0,
    err: ?*ErrCtx = null,
    space: ?Space = null,
    spill_at: usize = std.math.maxInt(usize),
    max_spill_depth: u8 = spill_parts.default_max_depth,
    spill_depth: u8 = 0,
    spill: ?*Spill = null,
    /// Levels that wrote spill files, this one and its replays' deeper ones, held
    /// by the top `Distinct` (`spilled_to` points a replay at it).
    spilled: u8 = 0,
    spilled_to: ?*u8 = null,

    const Spill = spill_parts.Spill(Distinct, "distinct", "DISTINCT");

    const FixedStore = keytable.FixedStore(keytable.NoPayload);
    const BoxedStore = keytable.GroupStore(keytable.NoPayload);
    const no_payload = keytable.NoPayload{};

    /// The keys seen so far: one shared `GroupTable` over fixed-width encoded keys, or
    /// over boxed ones when a key's kind has no fixed encoding.
    const Seen = struct {
        key_idx: []const usize,
        enc: ?keytable.Encoder,
        direct: ?keytable.Direct,
        table: keytable.GroupTable,
        store: union(enum) { fixed: *FixedStore, boxed: *BoxedStore },

        fn len(self: *const Seen) usize {
            return self.table.len;
        }
    };

    fn seenSet(self: *Distinct) !*Seen {
        if (self.seen) |*s| return s;
        const key_idx = self.keys orelse blk: {
            const all = try self.state.alloc(usize, self.in_schema.fields.len);
            for (all, 0..) |*x, i| x.* = i;
            break :blk all;
        };
        const kinds = try self.state.alloc(keyhash.KeyKind, key_idx.len);
        var fixed = key_idx.len <= keyhash.max_fixed_keys;
        for (key_idx, kinds) |ci, *k| k.* = keyhash.keyKindOf(self.in_schema.fields[ci].ty.kind) orelse blk: {
            fixed = false;
            break :blk .i64k;
        };
        self.seen = .{
            .key_idx = key_idx,
            .enc = if (fixed) try keytable.Encoder.init(self.state, key_idx, kinds) else null,
            .direct = if (fixed and keytable.Direct.serves(kinds)) try keytable.Direct.init(self.tableAlloc()) else null,
            .table = if (self.partial) try keytable.GroupTable.initBounded(self.tableAlloc(), lane_min, lane_max) else try keytable.GroupTable.init(self.tableAlloc(), 16),
            .store = if (fixed)
                .{ .fixed = try newIn(self.state, FixedStore.init(self.state, key_idx.len, &no_payload)) }
            else
                .{ .boxed = try newIn(self.state, BoxedStore.init(self.state, key_idx.len, &no_payload)) },
        };
        return &self.seen.?;
    }

    /// Where the table lives: the general allocator, so an outgrown table is freed,
    /// except on a lane, whose short-lived arena spares many threads mapping and
    /// unmapping tables at once.
    fn tableAlloc(self: *Distinct) std.mem.Allocator {
        return if (self.track_ords) self.state else self.gpa;
    }

    /// String ids through `shared_strs` when a lane shares its own, else a private table.
    fn interner(self: *Distinct) keytable.Interner {
        return self.shared_strs orelse .{ .table = &self.strs, .cache = &self.str_cache, .alloc = self.state };
    }

    fn freeSeen(self: *Distinct) void {
        if (self.scratch) |*a| a.deinit();
        self.scratch = null;
        const s = &(self.seen orelse return);
        s.table.deinit();
        if (s.direct) |d| d.deinit(self.tableAlloc());
        s.direct = null;
    }

    pub fn next(self: *Distinct, arena: std.mem.Allocator) anyerror!?Batch {
        if (self.space == null or self.track_ords) return self.dedup(arena);
        const sp = self.spill orelse try Spill.begin(self);
        if (!sp.drained) {
            const got = self.dedup(arena) catch |e| {
                sp.parts.abort();
                return e;
            };
            if (got) |b| return b;
            try sp.finish(self);
        }
        return sp.replay.?.next(self, self.gpa, arena);
    }

    /// Drops the spill files a consumer that stopped early never read, then the
    /// operator's own arena; safe twice.
    pub fn release(self: *Distinct) void {
        self.freeSeen();
        Spill.release(self);
    }

    /// A fresh `Distinct` over one spill file, one level deeper, on `state`.
    pub fn replayChild(self: *Distinct, child: Op, state: std.mem.Allocator) Distinct {
        return .{
            .child = child,
            .in_schema = self.in_schema,
            .keys = self.keys,
            .state = state,
            .gpa = self.gpa,
            .err = self.err,
            .space = self.space,
            .spill_at = self.spill_at,
            .max_spill_depth = self.max_spill_depth,
            .spill_depth = self.spill_depth + 1,
            .spilled_to = self.spilled_to orelse &self.spilled,
        };
    }

    /// Whether the seen keys are frozen, freezing them once they pass `spill_at`.
    /// Nothing freezes before a key is held, so every level keeps some.
    fn overBudget(self: *Distinct) bool {
        const sp = self.spill orelse return false;
        if (sp.frozen) return true;
        const seen = &(self.seen orelse return false);
        if (seen.len() > 0 and sp.meter.bytes + seen.table.entries.len * @sizeOf(u32) > self.spill_at) sp.frozen = true;
        return sp.frozen;
    }

    /// The next batch of first rows: each row's key looked up in `seen` in input
    /// order, inserted when new. Once frozen, a row whose key is held is dropped and one
    /// whose key is not goes to the spill file of its key hash.
    fn dedup(self: *Distinct, arena: std.mem.Allocator) anyerror!?Batch {
        const seen = try self.seenSet();
        errdefer self.freeSeen();
        if (self.scratch == null) self.scratch = .init(self.gpa);
        const scratch = &self.scratch.?;
        const pull = scratch.allocator();

        while (true) {
            _ = scratch.reset(.retain_capacity);
            const b = (try self.child.next(pull)) orelse break;
            const keep = try pull.alloc(bool, b.len);
            const frozen = self.overBudget();
            const dest: []u8 = if (frozen) try pull.alloc(u8, b.len) else &.{};
            @memset(dest, spill_parts.keep);
            @memset(keep, false);
            var nspill: usize = 0;
            var kept: usize = 0;
            switch (seen.store) {
                .fixed => |st| {
                    const e = try seen.enc.?.encode(pull, b.columns, b.len, self.interner(), frozen);
                    const rows: []const u32 = if (seen.direct) |*d| try d.misses(e, pull) else &.{};
                    const n = if (seen.direct != null) rows.len else b.len;
                    const hashes = try pull.alloc(u64, b.len);
                    for (0..n) |i| {
                        const r = if (seen.direct != null) rows[i] else i;
                        hashes[r] = e.hash(r);
                    }
                    var v = Dedup(FixedStore){ .seen = seen, .tb = &seen.table, .st = st, .keep = keep, .dest = dest, .frozen = frozen, .depth = self.spill_depth, .absent = e.absent };
                    try keytable.probe(FixedStore, .dupe, &keytable.Runs(i64).over(e.nk, rows, hashes, e.keys, e.masks), 0, n, &v);
                    nspill = v.nspill;
                    kept = v.kept;
                },
                .boxed => |st| {
                    const nk = seen.key_idx.len;
                    const hashes = try pull.alloc(u64, b.len);
                    const probes = try pull.alloc(Value, b.len * nk);
                    for (0..b.len) |r| {
                        const probe = probes[r * nk ..][0..nk];
                        for (seen.key_idx, probe) |ci, *v| v.* = b.columns[ci].getValue(r);
                        hashes[r] = keyhash.MultiKeyCtx.hash(.{}, probe);
                    }
                    var v = Dedup(BoxedStore){ .seen = seen, .tb = &seen.table, .st = st, .keep = keep, .dest = dest, .frozen = frozen, .depth = self.spill_depth };
                    try keytable.probe(BoxedStore, .dupe, &keytable.Runs(Value).over(nk, &.{}, hashes, probes, &.{}), 0, b.len, &v);
                    nspill = v.nspill;
                    kept = v.kept;
                },
            }
            if (nspill > 0) try self.spill.?.route(self, pull, b, dest);
            const base = self.seen_rows;
            self.seen_rows += b.len;
            if (kept == 0) {
                _ = scratch.reset(.retain_capacity);
                continue;
            }
            if (self.track_ords) {
                const ords = try arena.alloc(u64, kept);
                var oi: usize = 0;
                for (keep, 0..) |k, r| if (k) {
                    ords[oi] = base + r;
                    oi += 1;
                };
                self.ords = ords;
            }
            return try gatherKept(arena, b, keep, kept, seen.key_idx);
        }
        self.freeSeen();
        return null;
    }
};

/// Dedup's side of `keytable.probe`: a row of a new key kept, and once frozen a row of
/// an absent key marked for its spill file.
fn Dedup(comptime Store: type) type {
    return struct {
        seen: *Distinct.Seen,
        tb: *keytable.GroupTable,
        st: *Store,
        keep: []bool,
        dest: []u8,
        frozen: bool,
        depth: u8,
        absent: []const bool = &.{},
        nspill: usize = 0,
        kept: usize = 0,

        pub inline fn early(c: *@This(), r: usize, key: anytype) keytable.Early {
            if (Store == Distinct.FixedStore) if (c.seen.direct) |*d| if (d.lookup(key) != null) return .done;
            return if (c.absent.len > 0 and c.absent[r]) .miss else .probe;
        }

        pub inline fn hit(c: *@This(), r: usize, _: u64, key: anytype, x: keytable.Hit(Store)) !void {
            if (x.found) return;
            if (Store == Distinct.FixedStore) if (c.seen.direct) |*d| d.note(key, x.rec.tail, 0);
            c.keep[r] = true;
            c.kept += 1;
        }

        pub inline fn miss(c: *@This(), r: usize, h: u64) !void {
            c.dest[r] = spill_parts.partOf(h, c.depth);
            c.nspill += 1;
        }
    };
}

fn newIn(a: std.mem.Allocator, v: anytype) !*@TypeOf(v) {
    const p = try a.create(@TypeOf(v));
    p.* = v;
    return p;
}

/// The `keep`-marked rows of `b` copied into `arena`, string bytes included, for a
/// source batch whose scratch is about to be freed. A float key column's values are
/// `keyhash.canonF64`, as GROUP BY keeps its keys.
fn gatherKept(arena: std.mem.Allocator, b: Batch, keep: []const bool, kept: usize, key_idx: []const usize) anyerror!Batch {
    const outcols = try arena.alloc(column.Column, b.columns.len);
    try column.gatherColumns(arena, b.columns, outcols, keep, kept);
    for (b.columns, outcols, 0..) |col, *o, ci| {
        if (col.ty.kind == .float and std.mem.indexOfScalar(usize, key_idx, ci) != null)
            for (o.data.f64[0..kept]) |*x| {
                x.* = keyhash.canonF64(x.*);
            };
    }
    return Batch{ .schema = b.schema, .columns = outcols, .len = kept };
}

test "distinct reports the input ordinal of every surviving row" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const batches = [_]Batch{
        try intBatch(a, &int_schema, &.{ 7, 7, 4 }),
        try intBatch(a, &int_schema, &.{ 4, 7, 9 }),
    };
    var ts = TestSource{ .schema_ = int_schema, .batches = &batches };
    var scan = Scan{ .src = ts.src() };
    var dst = Distinct{ .child = .{ .scan = &scan }, .in_schema = &int_schema, .keys = null, .state = a, .gpa = testing.allocator, .track_ords = true };

    var ords = std.array_list.Managed(u64).init(a);
    const top = Op{ .distinct = &dst };
    while (try top.next(a)) |b| {
        try testing.expectEqual(b.len, dst.ords.len);
        try ords.appendSlice(dst.ords);
    }
    try testing.expectEqualSlices(u64, &.{ 0, 2, 5 }, ords.items);
}

test "distinct dedups across batches, groups nulls as one key, deep-copies strings" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const schema = types.Schema{ .fields = &.{
        .{ .name = "s", .ty = types.Type.init(.string).asNullable() },
    } };
    const batches = [_]Batch{
        try strBatch(a, &schema, &.{ "a", "b", null }),
        try strBatch(a, &schema, &.{ "b", null, "c", "a" }),
    };
    var ts = TestSource{ .schema_ = schema, .batches = &batches };
    var scan = Scan{ .src = ts.src() };
    var dst = Distinct{ .child = .{ .scan = &scan }, .in_schema = &schema, .keys = null, .state = a, .gpa = testing.allocator };

    var got = std.array_list.Managed(?[]const u8).init(a);
    const top = Op{ .distinct = &dst };
    while (try top.next(a)) |b| {
        var r: usize = 0;
        while (r < b.len) : (r += 1) {
            const v = b.columns[0].getValue(r);
            try got.append(if (v.isNull()) null else v.string);
        }
        for (batches[0..ts.idx]) |consumed| @memset(consumed.columns[0].data.bytes.values, '#');
    }
    const want = [_]?[]const u8{ "a", "b", null, "c" };
    try testing.expectEqual(want.len, got.items.len);
    for (want, got.items) |w, g| {
        if (w) |s| try testing.expectEqualStrings(s, g.?) else try testing.expect(g == null);
    }
}

const SpillDir = @import("../spill.zig").TestDir;

const spill_schema = types.Schema{ .fields = &.{
    .{ .name = "k", .ty = types.Type.init(.int).asNullable() },
    .{ .name = "s", .ty = types.Type.init(.string).asNullable() },
    .{ .name = "v", .ty = types.Type.init(.int) },
} };

/// `rows` rows in batches of `per`: `groups` int keys spread by a multiplier, seven
/// strings, NULLs in both, and the row's own index in `v`.
fn spillInput(a: std.mem.Allocator, rows: usize, per: usize, groups: usize) ![]Batch {
    const out = try a.alloc(Batch, (rows + per - 1) / per);
    for (out, 0..) |*b, bi| {
        const lo = bi * per;
        const hi = @min(rows, lo + per);
        var bs: [3]column.Builder = undefined;
        for (&bs, spill_schema.fields) |*x, f| x.* = column.Builder.init(a, f.ty);
        for (lo..hi) |i| {
            try bs[0].append(if (i % 97 == 5) .null else .{ .int = @intCast((i * 7919) % groups) });
            try bs[1].append(if (i % 13 == 0) .null else .{ .string = try std.fmt.allocPrint(a, "s{d}", .{i % 7}) });
            try bs[2].append(.{ .int = @intCast(i) });
        }
        const cols = try a.alloc(column.Column, 3);
        for (cols, &bs) |*c, *x| c.* = try x.finish();
        b.* = .{ .schema = &spill_schema, .columns = cols, .len = hi - lo };
    }
    return out;
}

/// Every output row as `k|s|v`, sorted.
fn runSpillDistinct(a: std.mem.Allocator, input: []const Batch, keys: ?[]const usize, space: ?Space, spill_at: usize, err: ?*ErrCtx) ![]const []const u8 {
    var ts = TestSource{ .schema_ = spill_schema, .batches = input };
    var scan = Scan{ .src = ts.src() };
    var state = std.heap.ArenaAllocator.init(testing.allocator);
    defer state.deinit();
    var dst = Distinct{ .child = .{ .scan = &scan }, .in_schema = &spill_schema, .keys = keys, .state = state.allocator(), .gpa = testing.allocator, .space = space, .spill_at = spill_at, .err = err };
    var rows = std.array_list.Managed([]const u8).init(a);
    while (try dst.next(a)) |b| for (0..b.len) |r| {
        const k = b.columns[0].getValue(r);
        const s = b.columns[1].getValue(r);
        try rows.append(try std.fmt.allocPrint(a, "{d}|{s}|{d}", .{
            if (k.isNull()) -1 else k.int,
            if (s.isNull()) "NULL" else s.string,
            b.columns[2].getValue(r).int,
        }));
    };
    std.mem.sort([]const u8, rows.items, {}, lessStr);
    return rows.items;
}

test "distinct spill: DISTINCT and DISTINCT ON keep the same rows, first per key, as in memory" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const input = try spillInput(a, 3000, 100, 900);
    const key_sets = [_]?[]const usize{ &.{0}, &.{ 1, 0 }, &.{1}, null };
    for (key_sets) |keys| {
        const want = try runSpillDistinct(a, input, keys, null, std.math.maxInt(usize), null);
        for ([_]usize{ 1, 16 << 10 }) |at| {
            var td = try SpillDir.init(std.math.maxInt(u64));
            defer td.deinit();
            try expectSameRows(want, try runSpillDistinct(a, input, keys, td.ds.space(), at, null));
            if (at == 1 and want.len > 200) try testing.expect(td.ds.seq.load(.monotonic) > 0);
            try testing.expectEqual(@as(usize, 0), try td.count());
        }
    }
    const first = try runSpillDistinct(a, input, &.{0}, null, std.math.maxInt(usize), null);
    try testing.expectEqual(@as(usize, 901), first.len);
}

test "distinct spill: a spill file that overflows again partitions one level deeper" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const input = try spillInput(a, 5000, 100, 4000);
    var td = try SpillDir.init(std.math.maxInt(u64));
    defer td.deinit();
    const want = try runSpillDistinct(a, input, &.{0}, null, std.math.maxInt(usize), null);
    try expectSameRows(want, try runSpillDistinct(a, input, &.{0}, td.ds.space(), 1, null));
    try testing.expect(td.ds.seq.load(.monotonic) > spill_parts.fanout);
}

test "distinct spill: the levels that wrote files are counted, none when the keys fit" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const input = try spillInput(a, 5000, 100, 4000);
    var td = try SpillDir.init(std.math.maxInt(u64));
    defer td.deinit();
    for ([_]struct { usize, bool }{ .{ std.math.maxInt(usize), false }, .{ 1, true } }) |c| {
        var ts = TestSource{ .schema_ = spill_schema, .batches = input };
        var scan = Scan{ .src = ts.src() };
        var state = std.heap.ArenaAllocator.init(testing.allocator);
        defer state.deinit();
        var dst = Distinct{ .child = .{ .scan = &scan }, .in_schema = &spill_schema, .keys = &.{0}, .state = state.allocator(), .gpa = testing.allocator, .space = td.ds.space(), .spill_at = c[0] };
        while (try dst.next(a)) |_| {}
        try testing.expectEqual(c[1], dst.spilled >= 2);
        if (!c[1]) try testing.expectEqual(@as(u8, 0), dst.spilled);
    }
}

test "distinct spill: past the disk cap fails with SpillCapExceeded" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const input = try spillInput(a, 3000, 100, 900);
    var td = try SpillDir.init(2048);
    defer td.deinit();
    var ec = ErrCtx{};
    try testing.expectError(error.SpillCapExceeded, runSpillDistinct(a, input, null, td.ds.space(), 1, &ec));
    try testing.expect(std.mem.indexOf(u8, ec.msg, "--spill-cap") != null);
    try testing.expectEqual(@as(usize, 0), try td.count());
}
