//! Hash joins: the build side materialized and indexed once (`JoinIndex`), the probe
//! side streamed through it, for every join kind.

const Batch = @import("../batch.zig").Batch;
const Decimal = @import("../value.zig").Decimal;
const ErrCtx = @import("../op.zig").ErrCtx;
const Op = @import("../op.zig").Op;
const Stats = @import("../op.zig").Stats;
const ast = @import("../../lang/ast.zig");
const column = @import("../column.zig");
const failLabel = @import("../op.zig").failLabel;
const keyhash = @import("../keyhash.zig");
const keyset = @import("keyset.zig");
const std = @import("std");
const op_mod = @import("../op.zig");
const types = @import("../../lang/types.zig");
const JoinRows = @import("testing_util.zig").JoinRows;
const Scan = @import("../op.zig").Scan;
const TestSource = @import("testing_util.zig").TestSource;
const join_both_schema = @import("testing_util.zig").join_both_schema;
const join_left_schema = @import("testing_util.zig").join_left_schema;
const join_right_schema = @import("testing_util.zig").join_right_schema;
const kvBatch = @import("testing_util.zig").kvBatch;
const testing = std.testing;

/// Drain the build side into a batch in `state` that is never null, so a join can
/// emit right-side nulls for an empty build. Fails with `JoinBuildTooLarge` past `cap`.
fn materializeFull(state: std.mem.Allocator, pull: std.mem.Allocator, child: Op, schema: *const types.Schema, bytes_out: *usize, cap: usize) anyerror!Batch {
    _ = pull;
    const ncols = schema.fields.len;
    var chunks = std.array_list.Managed(Batch).init(state);
    var total: usize = 0;
    var bytes: usize = 0;
    while (try child.next(state)) |b| {
        for (b.columns) |*col| bytes += columnBytes(col);
        if (bytes > cap) return error.JoinBuildTooLarge;
        if (b.len == 0) continue;
        try chunks.append(b);
        total += b.len;
    }
    const cols = try state.alloc(column.Column, ncols);
    if (total == 0) {
        for (cols, schema.fields) |*c, f| {
            var bd = column.Builder.init(state, f.ty);
            c.* = try bd.finish();
        }
    } else {
        const per = try state.alloc(column.Column, chunks.items.len);
        for (cols, 0..) |*out, ci| {
            for (chunks.items, 0..) |b, k| per[k] = b.columns[ci];
            out.* = try column.concat(state, per, total);
        }
    }
    bytes_out.* = bytes;
    return Batch{ .schema = schema, .columns = cols, .len = total };
}

/// A column's heap: typed store plus validity bitmap, close enough to bound a build side.
fn columnBytes(c: *const column.Column) usize {
    const payload: usize = switch (c.data) {
        .b => |s| s.len,
        .i32 => |s| s.len * @sizeOf(i32),
        .i64 => |s| s.len * @sizeOf(i64),
        .f64 => |s| s.len * @sizeOf(f64),
        .dec => |s| s.len * @sizeOf(Decimal),
        .bytes => |b| b.values.len + b.offsets.len * @sizeOf(i32),
    };
    return payload + c.validity.bits.len;
}

pub const KeyClass = enum { i64s, bytes, boxed };

/// How a key position compares. Same kind required: `int` and `timestamp` share the
/// i64 store but are never equal.
fn classOf(a: column.Column, b: column.Column) KeyClass {
    if (a.ty.kind != b.ty.kind) return .boxed;
    return switch (a.data) {
        .i64 => if (std.meta.activeTag(b.data) == .i64) .i64s else .boxed,
        .bytes => if (std.meta.activeTag(b.data) == .bytes) .bytes else .boxed,
        else => .boxed,
    };
}

fn cellEq(cls: KeyClass, a: *const column.Column, ar: usize, b: *const column.Column, br: usize) bool {
    return switch (cls) {
        .i64s => a.data.i64[ar] == b.data.i64[br],
        .bytes => std.mem.eql(u8, a.data.bytes.at(ar), b.data.bytes.at(br)),
        .boxed => keyhash.valueEq(a.getValue(ar), b.getValue(br)),
    };
}

/// Composite key hash for one row, folded from the typed store exactly as
/// `keyhash.MultiKeyCtx` folds boxed values, so build and probe sides agree.
fn hashRowKeys(cols: []const column.Column, keys: []const usize, classes: []const KeyClass, row: usize) u64 {
    var h = std.hash.Wyhash.init(0);
    for (keys, classes) |k, cls| {
        const c = &cols[k];
        switch (cls) {
            .i64s => switch (c.ty.kind) {
                .int => keyhash.hashInt(&h, c.data.i64[row]),
                .time => keyhash.hashTagged(&h, .time, std.mem.asBytes(&c.data.i64[row])),
                .timestamp => keyhash.hashTagged(&h, .timestamp, std.mem.asBytes(&c.data.i64[row])),
                else => keyhash.hashValue(&h, c.getValue(row)),
            },
            .bytes => keyhash.hashTagged(&h, if (c.ty.kind == .string) .string else .bytes, c.data.bytes.at(row)),
            .boxed => keyhash.hashValue(&h, c.getValue(row)),
        }
    }
    return h.final();
}

fn anyNullKey(cols: []const column.Column, keys: []const usize, row: usize) bool {
    for (keys) |k| {
        if (!cols[k].validity.get(row)) return true;
    }
    return false;
}

pub const JoinIndex = struct {
    build_batch: Batch,
    keys: []const usize,
    heads: []const u32,
    next: []const u32,
    hashes: []const u64,
    mask: u64,
    has_null_key: bool = false,

    /// Drain `build`, materialize it and index `right_keys`. Rows are inserted in
    /// reverse so prepending leaves duplicate chains in build order.
    pub fn create(
        state: std.mem.Allocator,
        pull: std.mem.Allocator,
        build: Op,
        right_schema: *const types.Schema,
        right_keys: []const usize,
        cap: usize,
    ) anyerror!*JoinIndex {
        var bytes: usize = 0;
        const batch = try materializeFull(state, pull, build, right_schema, &bytes, cap);
        const n = batch.len;
        if (n >= std.math.maxInt(u32)) return error.JoinBuildTooLarge;

        const self = try state.create(JoinIndex);
        self.* = .{
            .build_batch = batch,
            .keys = right_keys,
            .heads = &.{},
            .next = &.{},
            .hashes = &.{},
            .mask = 0,
        };
        if (right_keys.len == 0) return self;

        var cap_slots: usize = 16;
        while (cap_slots < n * 2) cap_slots *= 2;
        bytes += cap_slots * @sizeOf(u32) + n * (@sizeOf(u32) + @sizeOf(u64));
        if (bytes > cap) return error.JoinBuildTooLarge;

        const heads = try state.alloc(u32, cap_slots);
        @memset(heads, 0);
        const chain = try state.alloc(u32, n);
        const hashes = try state.alloc(u64, n);
        self.heads = heads;
        self.next = chain;
        self.hashes = hashes;
        self.mask = cap_slots - 1;

        const classes = try pull.alloc(KeyClass, right_keys.len);
        for (right_keys, classes) |k, *c| c.* = classOf(batch.columns[k], batch.columns[k]);

        @memset(chain, 0);
        var ri: usize = n;
        while (ri > 0) {
            ri -= 1;
            const r = ri;
            if (anyNullKey(batch.columns, right_keys, r)) {
                self.has_null_key = true;
                continue;
            }
            const h = hashRowKeys(batch.columns, right_keys, classes, r);
            hashes[r] = h;
            var slot = h & self.mask;
            while (heads[slot] != 0) : (slot = (slot + 1) & self.mask) {
                const hr: usize = heads[slot] - 1;
                if (hashes[hr] == h and self.rowsEq(classes, hr, r)) break;
            }
            chain[r] = heads[slot];
            heads[slot] = @intCast(r + 1);
        }
        return self;
    }

    pub fn rows(self: *const JoinIndex) usize {
        return self.build_batch.len;
    }

    fn rowsEq(self: *const JoinIndex, classes: []const KeyClass, a_row: usize, b_row: usize) bool {
        for (self.keys, classes) |k, cls| {
            const c = &self.build_batch.columns[k];
            if (!cellEq(cls, c, a_row, c, b_row)) return false;
        }
        return true;
    }

    fn probeEq(self: *const JoinIndex, probe: Batch, probe_keys: []const usize, classes: []const KeyClass, prow: usize, brow: usize) bool {
        for (self.keys, probe_keys, classes) |bk, pk, cls| {
            if (!cellEq(cls, &self.build_batch.columns[bk], brow, &probe.columns[pk], prow)) return false;
        }
        return true;
    }

    /// First build row whose keys equal probe row `row`'s, or null; walk the rest with
    /// `chainNext`. `classes` comes from `classesFor`.
    pub fn find(self: *const JoinIndex, probe: Batch, probe_keys: []const usize, classes: []const KeyClass, row: usize) ?usize {
        if (self.keys.len == 0 or self.build_batch.len == 0) return null;
        if (anyNullKey(probe.columns, probe_keys, row)) return null;
        return self.findHashed(probe, probe_keys, classes, row, hashRowKeys(probe.columns, probe_keys, classes, row));
    }

    pub fn prefetch(self: *const JoinIndex, h: u64) void {
        @prefetch(&self.heads[h & self.mask], .{ .rw = .read, .locality = 3 });
    }

    pub fn findHashed(self: *const JoinIndex, probe: Batch, probe_keys: []const usize, classes: []const KeyClass, row: usize, h: u64) ?usize {
        var slot = h & self.mask;
        while (self.heads[slot] != 0) : (slot = (slot + 1) & self.mask) {
            const hr: usize = self.heads[slot] - 1;
            if (self.hashes[hr] == h and self.probeEq(probe, probe_keys, classes, row, hr)) return hr;
        }
        return null;
    }

    pub fn chainNext(self: *const JoinIndex, row: usize) ?usize {
        const nx = self.next[row];
        return if (nx == 0) null else nx - 1;
    }

    pub fn classesFor(self: *const JoinIndex, arena: std.mem.Allocator, probe: Batch, probe_keys: []const usize) ![]KeyClass {
        const classes = try arena.alloc(KeyClass, probe_keys.len);
        for (self.keys, probe_keys, classes) |bk, pk, *c|
            c.* = classOf(probe.columns[pk], self.build_batch.columns[bk]);
        return classes;
    }
};

/// Key pushdown's limits: the probe rows and bytes read ahead before giving up,
/// and the distinct values sent per key before a range stands in for them.
pub const default_prefetch_rows: usize = 100_000;
pub const default_prefetch_bytes: usize = 64 << 20;
pub const default_push_cap: usize = 1000;

pub const Join = struct {
    stats: Stats = .{},
    probe: Op,
    build: ?Op,
    index: ?*JoinIndex = null,
    left_keys: []const usize,
    right_keys: []const usize,
    left_schema: *const types.Schema,
    right_schema: *const types.Schema,
    out_schema: *const types.Schema,
    kind: ast.JoinKind,
    null_aware: bool = false,
    state: std.mem.Allocator,
    err: ?*ErrCtx = null,
    build_cap: ?usize = null,

    /// Key pushdown. `push_build` gets the probe side's keys before the build side
    /// is read: the probe is read ahead into memory up to `prefetch_rows` /
    /// `prefetch_bytes` and replayed, and past either the read-ahead stops and no
    /// keys are sent. `push_probe` gets the build side's keys once it is indexed,
    /// before the first probe row. At most `push_cap` distinct values per key.
    push_build: ?keyset.KeyPush = null,
    push_probe: ?keyset.KeyPush = null,
    prefetch_rows: usize = default_prefetch_rows,
    prefetch_bytes: usize = default_prefetch_bytes,
    push_cap: usize = default_push_cap,

    matched: ?[]bool = null,
    drain_pos: usize = 0,
    probe_done: bool = false,
    prefetched: bool = false,
    probe_ended: bool = false,
    probe_pushed: bool = false,
    replay: []const Batch = &.{},
    replay_pos: usize = 0,

    const drain_chunk = 4096;

    pub fn next(self: *Join, arena: std.mem.Allocator) anyerror!?Batch {
        if (self.push_build != null and !self.prefetched) try self.prefetch(arena);
        const ix = try self.ensureIndex(arena);
        if (self.push_probe) |kp| if (!self.probe_pushed) {
            self.probe_pushed = true;
            try kp.apply(kp.ctx, try keyset.collect(self.state, &.{ix.build_batch}, self.right_keys, self.push_cap));
        };
        while (!self.probe_done) {
            if (try self.nextProbe(arena)) |lb| {
                const out = try self.joinBatch(arena, ix, lb);
                if (out.len > 0) return out;
            } else {
                self.probe_done = true;
            }
        }
        return self.drain(arena, ix);
    }

    fn nextProbe(self: *Join, arena: std.mem.Allocator) anyerror!?Batch {
        if (self.replay_pos < self.replay.len) {
            self.replay_pos += 1;
            return self.replay[self.replay_pos - 1];
        }
        if (self.probe_ended) return null;
        return self.probe.next(arena);
    }

    /// Reads the probe side ahead, kept in `state`, and sends its keys when it ended
    /// within the limits; past them it sends none and the build side reads in full.
    fn prefetch(self: *Join, arena: std.mem.Allocator) anyerror!void {
        self.prefetched = true;
        var held = std.array_list.Managed(Batch).init(self.state);
        var rows: usize = 0;
        var bytes: usize = 0;
        while (rows <= self.prefetch_rows and bytes <= self.prefetch_bytes) {
            const b = (try self.probe.next(arena)) orelse {
                self.probe_ended = true;
                break;
            };
            if (b.len == 0) continue;
            try held.append(try b.deepCopy(self.state));
            rows += b.len;
            for (b.columns) |*col| bytes += columnBytes(col);
        }
        self.replay = held.items;
        if (self.probe_ended)
            try self.push_build.?.apply(self.push_build.?.ctx, try keyset.collect(self.state, self.replay, self.left_keys, self.push_cap));
    }

    fn ensureIndex(self: *Join, arena: std.mem.Allocator) anyerror!*JoinIndex {
        const ix = self.index orelse blk: {
            const build = self.build orelse return error.JoinHasNoBuildSide;
            const made = JoinIndex.create(self.state, arena, build, self.right_schema, self.right_keys, self.build_cap orelse op_mod.join_build_byte_cap) catch |e| {
                if (self.err) |ec| ec.set("{s}", .{failLabel(e)});
                return e;
            };
            self.index = made;
            break :blk made;
        };
        if ((self.kind == .right or self.kind == .full) and self.matched == null) {
            const m = try self.state.alloc(bool, ix.build_batch.len);
            @memset(m, false);
            self.matched = m;
        }
        return ix;
    }

    /// One probe batch to one output batch, gathered from index lists. Under NOT IN a
    /// null on either side against a non-empty build is unknown and drops the row.
    fn joinBatch(self: *Join, arena: std.mem.Allocator, ix: *JoinIndex, lb: Batch) anyerror!Batch {
        var lidx = std.array_list.Managed(usize).init(arena);
        var ridx = std.array_list.Managed(usize).init(arena);
        var rnull = std.array_list.Managed(bool).init(arena);
        try lidx.ensureTotalCapacity(lb.len);

        if (self.kind == .cross) {
            var r: usize = 0;
            while (r < lb.len) : (r += 1) {
                var b: usize = 0;
                while (b < ix.build_batch.len) : (b += 1) {
                    try lidx.append(r);
                    try ridx.append(b);
                }
            }
            return self.gatherOut(arena, ix, lb, lidx.items, ridx.items, &.{}, lidx.items.len);
        }

        const fill_right = (self.kind == .left or self.kind == .full);
        const classes = try ix.classesFor(arena, lb, self.left_keys);
        try ridx.ensureTotalCapacity(lb.len);
        if (fill_right) try rnull.ensureTotalCapacity(lb.len);

        const empty = ix.keys.len == 0 or ix.build_batch.len == 0;
        const hs = try arena.alloc(u64, lb.len);
        const nulls = try arena.alloc(bool, lb.len);
        for (hs, nulls, 0..) |*h, *nl, i| {
            nl.* = empty or anyNullKey(lb.columns, self.left_keys, i);
            h.* = if (nl.*) 0 else hashRowKeys(lb.columns, self.left_keys, classes, i);
        }
        const ahead = 8;

        var r: usize = 0;
        while (r < lb.len) : (r += 1) {
            if (r + ahead < lb.len and !nulls[r + ahead]) ix.prefetch(hs[r + ahead]);
            const first = if (nulls[r]) null else ix.findHashed(lb, self.left_keys, classes, r, hs[r]);
            switch (self.kind) {
                .semi => {
                    if (first != null) try lidx.append(r);
                },
                .anti => {
                    if (self.null_aware and !empty and (ix.has_null_key or nulls[r])) continue;
                    if (first == null) try lidx.append(r);
                },
                else => {
                    if (first == null) {
                        if (fill_right) {
                            try lidx.append(r);
                            try ridx.append(0);
                            try rnull.append(true);
                        }
                        continue;
                    }
                    var cur = first;
                    while (cur) |br| {
                        try lidx.append(r);
                        try ridx.append(br);
                        if (fill_right) try rnull.append(false);
                        if (self.matched) |m| m[br] = true;
                        cur = ix.chainNext(br);
                    }
                },
            }
        }
        return self.gatherOut(arena, ix, lb, lidx.items, ridx.items, rnull.items, lidx.items.len);
    }

    /// right/full: after the probe ends, every unmatched build row with the left
    /// columns null, in chunks.
    fn drain(self: *Join, arena: std.mem.Allocator, ix: *JoinIndex) anyerror!?Batch {
        if (self.kind != .right and self.kind != .full) return null;
        var ridx = std.array_list.Managed(usize).init(arena);
        const matched = self.matched orelse return null;
        while (self.drain_pos < ix.build_batch.len and ridx.items.len < drain_chunk) : (self.drain_pos += 1) {
            if (!matched[self.drain_pos]) try ridx.append(self.drain_pos);
        }
        if (ridx.items.len == 0) return null;
        return try self.gatherOut(arena, ix, null, &.{}, ridx.items, &.{}, ridx.items.len);
    }

    /// Assemble `n` output rows with one gather per column. `lb == null` marks the
    /// outer drain, where the whole left side is null.
    fn gatherOut(
        self: *Join,
        arena: std.mem.Allocator,
        ix: *JoinIndex,
        lb: ?Batch,
        lidx: []const usize,
        ridx: []const usize,
        rnull: []const bool,
        n: usize,
    ) anyerror!Batch {
        const emit_right = (self.kind != .semi and self.kind != .anti);
        const nleft = if (lb) |b| b.columns.len else self.left_schema.fields.len;
        const nout = nleft + (if (emit_right) ix.build_batch.columns.len else @as(usize, 0));
        const cols = try arena.alloc(column.Column, nout);

        var i: usize = 0;
        while (i < nleft) : (i += 1) {
            cols[i] = if (lb) |b|
                try takeCol(arena, b.columns[i], lidx, &.{})
            else
                try nullColumn(arena, self.left_schema.fields[i].ty, n);
        }
        if (emit_right) {
            var k: usize = 0;
            while (k < ix.build_batch.columns.len) : (k += 1) {
                cols[nleft + k] = try takeCol(arena, ix.build_batch.columns[k], ridx, rnull);
            }
        }
        return Batch{ .schema = self.out_schema, .columns = cols, .len = n };
    }
};

/// Gather rows `idx` of `c`, then null out the `null_mask` positions (the outer-join
/// fill). Only a source with no rows is built as an all-null column instead.
fn takeCol(arena: std.mem.Allocator, c: column.Column, idx: []const usize, null_mask: []const bool) !column.Column {
    if (c.len == 0) return nullColumn(arena, c.ty, idx.len);
    var out = try column.permute(arena, c, idx);
    if (null_mask.len == idx.len) {
        out.ty = out.ty.asNullable();
        for (null_mask, 0..) |is_null, i| {
            if (is_null) out.validity.setValid(i, false);
        }
    }
    return out;
}

fn nullColumn(arena: std.mem.Allocator, ty: types.Type, n: usize) !column.Column {
    var b = try column.Builder.initCapacity(arena, ty.asNullable(), n);
    var i: usize = 0;
    while (i < n) : (i += 1) try b.append(.null);
    return b.finish();
}

test "join: inner/left/semi/anti; null keys never match, duplicate build keys fan out" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const Case = struct { kind: ast.JoinKind, keys: []const ?i64, rvs: []const ?[]const u8 };
    const cases = [_]Case{
        .{ .kind = .inner, .keys = &.{ 1, 1 }, .rvs = &.{ "x", "y" } },
        .{ .kind = .left, .keys = &.{ 1, 1, 2, null, 3 }, .rvs = &.{ "x", "y", null, null, null } },
        .{ .kind = .semi, .keys = &.{1}, .rvs = &.{} },
        .{ .kind = .anti, .keys = &.{ 2, null, 3 }, .rvs = &.{} },
    };
    for (cases) |case| {
        const lb = [_]Batch{try kvBatch(a, &join_left_schema, &.{ 1, 2, null, 3 }, &.{ "a", "b", "n", "c" })};
        const rb = [_]Batch{try kvBatch(a, &join_right_schema, &.{ 1, 1, 4, null }, &.{ "x", "y", "z", "m" })};
        var lts = TestSource{ .schema_ = join_left_schema, .batches = &lb };
        var rts = TestSource{ .schema_ = join_right_schema, .batches = &rb };
        var lscan = Scan{ .src = lts.src() };
        var rscan = Scan{ .src = rts.src() };
        const emit_right = case.kind == .inner or case.kind == .left;
        var jn = Join{
            .probe = .{ .scan = &lscan },
            .build = .{ .scan = &rscan },
            .left_keys = &.{0},
            .right_keys = &.{0},
            .left_schema = &join_left_schema,
            .right_schema = &join_right_schema,
            .out_schema = if (emit_right) &join_both_schema else &join_left_schema,
            .kind = case.kind,
            .state = a,
        };
        const got = try JoinRows.collect(a, .{ .join = &jn }, if (emit_right) @as(?usize, 3) else null);
        try got.expect(case.keys, case.rvs);
    }
}

test "join: key pushdown sends one side's keys first and replays a read-ahead probe unchanged" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const Rec = struct {
        got: ?[]const keyset.KeyValues = null,
        calls: usize = 0,
        fn apply(ctx: *anyopaque, keys: []const keyset.KeyValues) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.got = keys;
            self.calls += 1;
        }
    };
    for ([_]struct { rows: usize, sent: bool }{ .{ .rows = 100, .sent = true }, .{ .rows = 1, .sent = false } }) |case| {
        const lb = [_]Batch{
            try kvBatch(a, &join_left_schema, &.{ 1, 2 }, &.{ "a", "b" }),
            try kvBatch(a, &join_left_schema, &.{ null, 3, 1 }, &.{ "n", "c", "d" }),
        };
        const rb = [_]Batch{try kvBatch(a, &join_right_schema, &.{ 1, 4, null }, &.{ "x", "z", "m" })};
        var lts = TestSource{ .schema_ = join_left_schema, .batches = &lb };
        var rts = TestSource{ .schema_ = join_right_schema, .batches = &rb };
        var lscan = Scan{ .src = lts.src() };
        var rscan = Scan{ .src = rts.src() };
        var to_build = Rec{};
        var to_probe = Rec{};
        var jn = Join{
            .probe = .{ .scan = &lscan },
            .build = .{ .scan = &rscan },
            .left_keys = &.{0},
            .right_keys = &.{0},
            .left_schema = &join_left_schema,
            .right_schema = &join_right_schema,
            .out_schema = &join_both_schema,
            .kind = .left,
            .state = a,
            .push_build = .{ .ctx = &to_build, .apply = Rec.apply },
            .push_probe = .{ .ctx = &to_probe, .apply = Rec.apply },
            .prefetch_rows = case.rows,
        };
        const got = try JoinRows.collect(a, .{ .join = &jn }, 3);
        try got.expect(&.{ 1, 2, null, 3, 1 }, &.{ "x", null, null, null, "x" });
        try testing.expectEqual(@as(usize, if (case.sent) 1 else 0), to_build.calls);
        if (case.sent) try testing.expectEqual(@as(usize, 3), to_build.got.?[0].values.len);
        try testing.expectEqual(@as(usize, 1), to_probe.calls);
        try testing.expectEqual(@as(usize, 2), to_probe.got.?[0].values.len);
    }
}

test "join: a null-aware anti join is NOT IN — a NULL on either side is unknown" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const Case = struct { build: []const ?i64, keys: []const ?i64 };
    const cases = [_]Case{
        .{ .build = &.{ 1, 4, null }, .keys = &.{} },
        .{ .build = &.{ 1, 4 }, .keys = &.{ 2, 3 } },
        .{ .build = &.{}, .keys = &.{ 1, 2, null, 3 } },
    };
    for (cases) |case| {
        const lb = [_]Batch{try kvBatch(a, &join_left_schema, &.{ 1, 2, null, 3 }, &.{ "a", "b", "n", "c" })};
        const rvs = try a.alloc(?[]const u8, case.build.len);
        @memset(rvs, "r");
        const rb = [_]Batch{try kvBatch(a, &join_right_schema, case.build, rvs)};
        var lts = TestSource{ .schema_ = join_left_schema, .batches = &lb };
        var rts = TestSource{ .schema_ = join_right_schema, .batches = &rb };
        var lscan = Scan{ .src = lts.src() };
        var rscan = Scan{ .src = rts.src() };
        var jn = Join{
            .probe = .{ .scan = &lscan },
            .build = .{ .scan = &rscan },
            .left_keys = &.{0},
            .right_keys = &.{0},
            .left_schema = &join_left_schema,
            .right_schema = &join_right_schema,
            .out_schema = &join_left_schema,
            .kind = .anti,
            .null_aware = true,
            .state = a,
        };
        const got = try JoinRows.collect(a, .{ .join = &jn }, null);
        try got.expect(case.keys, &.{});
    }
}

test "join: right and full drain unmatched build rows with a null left side" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    for ([_]ast.JoinKind{ .right, .full }) |kind| {
        const lb = [_]Batch{try kvBatch(a, &join_left_schema, &.{ 1, 2, null }, &.{ "a", "b", "n" })};
        const rb = [_]Batch{try kvBatch(a, &join_right_schema, &.{ 1, 4, null }, &.{ "x", "z", "m" })};
        var lts = TestSource{ .schema_ = join_left_schema, .batches = &lb };
        var rts = TestSource{ .schema_ = join_right_schema, .batches = &rb };
        var lscan = Scan{ .src = lts.src() };
        var rscan = Scan{ .src = rts.src() };
        var jn = Join{
            .probe = .{ .scan = &lscan },
            .build = .{ .scan = &rscan },
            .left_keys = &.{0},
            .right_keys = &.{0},
            .left_schema = &join_left_schema,
            .right_schema = &join_right_schema,
            .out_schema = &join_both_schema,
            .kind = kind,
            .state = a,
        };
        const got = try JoinRows.collect(a, .{ .join = &jn }, 3);
        if (kind == .right) {
            try got.expect(&.{ 1, null, null }, &.{ "x", "z", "m" });
        } else {
            try got.expect(&.{ 1, 2, null, null, null }, &.{ "x", null, null, "z", "m" });
        }
    }
}

test "join: multi-key ON (int + string) pairs only fully equal keys" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const lb = [_]Batch{try kvBatch(a, &join_left_schema, &.{ 1, 1, 2, 3 }, &.{ "a", "b", "a", null })};
    const rb = [_]Batch{try kvBatch(a, &join_right_schema, &.{ 1, 2, 1 }, &.{ "a", "z", "a" })};
    var lts = TestSource{ .schema_ = join_left_schema, .batches = &lb };
    var rts = TestSource{ .schema_ = join_right_schema, .batches = &rb };
    var lscan = Scan{ .src = lts.src() };
    var rscan = Scan{ .src = rts.src() };
    var jn = Join{
        .probe = .{ .scan = &lscan },
        .build = .{ .scan = &rscan },
        .left_keys = &.{ 0, 1 },
        .right_keys = &.{ 0, 1 },
        .left_schema = &join_left_schema,
        .right_schema = &join_right_schema,
        .out_schema = &join_both_schema,
        .kind = .inner,
        .state = a,
    };
    const got = try JoinRows.collect(a, .{ .join = &jn }, 3);
    try got.expect(&.{ 1, 1 }, &.{ "a", "a" });
}

test "join: cross pairs every probe row with every build row" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const lb = [_]Batch{try kvBatch(a, &join_left_schema, &.{ 1, 2 }, &.{ "a", "b" })};
    const rb = [_]Batch{try kvBatch(a, &join_right_schema, &.{ 7, 8, 9 }, &.{ "x", "y", "z" })};
    var lts = TestSource{ .schema_ = join_left_schema, .batches = &lb };
    var rts = TestSource{ .schema_ = join_right_schema, .batches = &rb };
    var lscan = Scan{ .src = lts.src() };
    var rscan = Scan{ .src = rts.src() };
    var jn = Join{
        .probe = .{ .scan = &lscan },
        .build = .{ .scan = &rscan },
        .left_keys = &.{},
        .right_keys = &.{},
        .left_schema = &join_left_schema,
        .right_schema = &join_right_schema,
        .out_schema = &join_both_schema,
        .kind = .cross,
        .state = a,
    };
    const got = try JoinRows.collect(a, .{ .join = &jn }, 3);
    try got.expect(&.{ 1, 1, 1, 2, 2, 2 }, &.{ "x", "y", "z", "x", "y", "z" });
}

test "join: the build-size guard reports instead of exhausting memory" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const rb = [_]Batch{try kvBatch(a, &join_right_schema, &.{ 1, 2, 3 }, &.{ "x", "y", "z" })};
    const lb = [_]Batch{try kvBatch(a, &join_left_schema, &.{1}, &.{"a"})};
    var lts = TestSource{ .schema_ = join_left_schema, .batches = &lb };
    var rts = TestSource{ .schema_ = join_right_schema, .batches = &rb };
    var lscan = Scan{ .src = lts.src() };
    var rscan = Scan{ .src = rts.src() };
    var ec = ErrCtx{};
    var jn = Join{
        .probe = .{ .scan = &lscan },
        .build = .{ .scan = &rscan },
        .left_keys = &.{0},
        .right_keys = &.{0},
        .left_schema = &join_left_schema,
        .right_schema = &join_right_schema,
        .out_schema = &join_both_schema,
        .kind = .inner,
        .state = a,
        .err = &ec,
    };
    const saved = op_mod.join_build_byte_cap;
    op_mod.join_build_byte_cap = 8;
    defer op_mod.join_build_byte_cap = saved;
    const top = Op{ .join = &jn };
    try testing.expectError(error.JoinBuildTooLarge, top.next(a));
    try testing.expect(std.mem.indexOf(u8, ec.msg, "exceeds its cap") != null);
}
