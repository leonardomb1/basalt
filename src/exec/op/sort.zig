//! `ORDER BY`: a full sort by key words, radix-sorted, parallel past a size, with
//! ties broken by the original row order so the sort is stable.
//!
//! With a `space`, the sort is external once its input passes `spill_at` bytes (the
//! run's `--op-memory`): input is pulled into the sort's own arena and, each time
//! the bytes held pass the limit, sorted in memory and written to a spill file as a
//! sorted run of `batch_rows`-row batches, and the arena is reset. At the end the
//! remainder is sorted and kept in memory as the last run. Past `fan_in` sources,
//! consecutive groups of `fan_in` runs are merged into new runs (their files deleted
//! as each group completes) until few enough remain; the last merge streams out
//! `batch_rows`-row batches across pulls. Runs are cut in input order and the merge
//! prefers the earlier source on equal keys, so the result is the stable in-memory
//! sort's row for row. Heads compare with `KeyArr.orderAcross`, the in-memory
//! comparator across two batches: same null placement, DESC, `orderF64` and byte order;
//! decimals, scaled per batch, compare exactly at the larger scale. Input that never
//! passes the limit is sorted in memory exactly as without a space. The disk cap is
//! booked per byte written, merge passes included, and given back as each file is
//! deleted. A sort abandoned before its last batch (a LIMIT, an error downstream)
//! frees its merge, files and mappings on `release`.

const Batch = @import("../batch.zig").Batch;
const Decimal = @import("../value.zig").Decimal;
const Op = @import("../op.zig").Op;
const Space = @import("../space.zig").Space;
const spill = @import("../spill.zig");
const SpillDir = spill.TestDir;
const Stats = @import("../op.zig").Stats;
const Value = @import("../value.zig").Value;
const column = @import("../column.zig");
const eval = @import("../eval.zig");
const materializeAll = @import("../op.zig").materializeAll;
const std = @import("std");
const types = @import("../../lang/types.zig");
const Filter = @import("../op.zig").Filter;
const Project = @import("../op.zig").Project;
const Scan = @import("../op.zig").Scan;
const TestSource = @import("testing_util.zig").TestSource;
const ast = @import("../../lang/ast.zig");
const drainInts = @import("testing_util.zig").drainInts;
const intBatch = @import("testing_util.zig").intBatch;
const int_schema = @import("testing_util.zig").int_schema;
const linearize = @import("../op.zig").linearize;
const par = @import("../par.zig");
const testing = std.testing;

pub const Sort = struct {
    stats: Stats = .{},
    child: Op,
    in_schema: *const types.Schema,
    keys: []const Key,
    done: bool = false,
    threads: usize = 1,
    space: ?Space = null,
    spill_at: usize = std.math.maxInt(usize),
    fan_in: usize = 64,
    batch_rows: usize = 1 << 16,
    gpa: std.mem.Allocator = std.heap.page_allocator,
    merge: ?*Merge = null,
    /// Sorted runs written to disk, kept past the merge for `EXPLAIN ANALYZE`.
    runs: usize = 0,

    /// `nulls_first` places nulls before every value, otherwise after.
    pub const Key = struct { idx: usize, desc: bool, nulls_first: bool };

    pub fn next(self: *Sort, arena: std.mem.Allocator) anyerror!?Batch {
        errdefer self.dropMerge();
        if (self.merge) |m| {
            if (try m.next(arena, @max(self.batch_rows, 1))) |b| return b;
            self.dropMerge();
            return null;
        }
        if (self.done) return null;
        self.done = true;
        const space = self.space orelse {
            const all = (try materializeAll(arena, self.child, self.in_schema)) orelse return null;
            return try self.sortBatch(arena, all);
        };
        return self.external(arena, space);
    }

    /// Drops a merge left unfinished by a consumer that stopped pulling: its files
    /// are deleted, their disk charge given back and its memory freed.
    pub fn release(self: *Sort) void {
        self.dropMerge();
        self.done = true;
    }

    fn dropMerge(self: *Sort) void {
        const m = self.merge orelse return;
        m.deinit();
        self.merge = null;
    }

    fn sortBatch(self: *Sort, arena: std.mem.Allocator, all: Batch) !Batch {
        const s = try self.sorted(arena, all);
        const outcols = try arena.alloc(column.Column, all.columns.len);
        for (all.columns, 0..) |*col, ci| outcols[ci] = try column.permuteThreads(arena, col.*, s.idx, self.threads);
        return Batch{ .schema = all.schema, .columns = outcols, .len = all.len };
    }

    fn sorted(self: *Sort, arena: std.mem.Allocator, all: Batch) !Sorted {
        const idx = try arena.alloc(usize, all.len);
        for (idx, 0..) |*x, i| x.* = i;
        const arrs = try arena.alloc(KeyArr, self.keys.len);
        for (self.keys, arrs) |k, *a| a.* = try KeyArr.prepareThreads(arena, all.columns[k.idx], k, self.threads);
        try sortIdxThreads(arena, idx, arrs, self.threads);
        return .{ .idx = idx, .arrs = arrs };
    }

    /// Drains the child into `held`, writing a sorted run whenever the bytes held pass
    /// `spill_at`. With no run written the input is sorted in memory as without a
    /// space; otherwise the remainder stays in memory as the last run, groups of runs
    /// are merged into new ones until `fan_in` sources remain, and the final merge is
    /// left in `merge` for the following pulls.
    fn external(self: *Sort, arena: std.mem.Allocator, space: Space) !?Batch {
        const held = try self.gpa.create(std.heap.ArenaAllocator);
        held.* = std.heap.ArenaAllocator.init(self.gpa);
        var held_owned = true;
        defer if (held_owned) {
            held.deinit();
            self.gpa.destroy(held);
        };
        var paths = std.heap.ArenaAllocator.init(self.gpa);
        defer paths.deinit();
        var chunks: std.ArrayListUnmanaged(Batch) = .empty;
        defer chunks.deinit(self.gpa);
        var runs: std.ArrayListUnmanaged(spill.Run) = .empty;
        defer runs.deinit(self.gpa);
        var runs_owned = true;
        errdefer if (runs_owned) deleteRuns(runs.items);

        var rows: usize = 0;
        var bytes: usize = 0;
        while (try self.child.next(held.allocator())) |b| {
            if (b.len == 0) continue;
            try chunks.append(self.gpa, b);
            rows += b.len;
            bytes += b.heapBytes();
            if (bytes <= self.spill_at) continue;
            const all = try concatChunks(held.allocator(), self.in_schema, chunks.items, rows, self.threads);
            const s = try self.sorted(held.allocator(), all);
            try runs.ensureUnusedCapacity(self.gpa, 1);
            runs.appendAssumeCapacity(try self.writeSorted(space, paths.allocator(), all, s.idx));
            self.runs += 1;
            chunks.clearRetainingCapacity();
            _ = held.reset(.retain_capacity);
            rows = 0;
            bytes = 0;
        }

        if (runs.items.len == 0) {
            if (rows == 0) return null;
            return try self.sortBatch(arena, try concatChunks(arena, self.in_schema, chunks.items, rows, self.threads));
        }

        var mem: ?Merge.Mem = null;
        if (rows > 0) {
            const all = try concatChunks(held.allocator(), self.in_schema, chunks.items, rows, self.threads);
            const s = try self.sorted(held.allocator(), all);
            mem = .{ .cols = all.columns, .arrs = s.arrs, .idx = s.idx };
        }
        const fan_in = @max(self.fan_in, 2);
        while (runs.items.len + @intFromBool(mem != null) > fan_in) {
            var kept: usize = 0;
            var i: usize = 0;
            while (i < runs.items.len) : (i += fan_in) {
                const group = runs.items[i..@min(runs.items.len, i + fan_in)];
                runs.items[kept] = if (group.len == 1) group[0] else self.mergeRuns(space, paths.allocator(), group) catch |e| {
                    deleteRuns(runs.items[0..kept]);
                    deleteRuns(runs.items[i + group.len ..]);
                    runs_owned = false;
                    return e;
                };
                kept += 1;
            }
            runs.shrinkRetainingCapacity(kept);
        }

        runs_owned = false;
        const give: ?*std.heap.ArenaAllocator = if (mem != null) held else null;
        if (give != null) held_owned = false;
        const m = try Merge.init(self.gpa, self.in_schema, self.keys, runs.items, mem, give);
        self.merge = m;
        if (try m.next(arena, @max(self.batch_rows, 1))) |b| return b;
        self.dropMerge();
        return null;
    }

    /// Writes `all` in the order of `idx` as a run of `batch_rows`-row batches. The
    /// run's path is copied into `paths`; the writer's buffer is freed on return.
    fn writeSorted(self: *Sort, space: Space, paths: std.mem.Allocator, all: Batch, idx: []const usize) !spill.Run {
        var tmp = std.heap.ArenaAllocator.init(self.gpa);
        defer tmp.deinit();
        var chunk = std.heap.ArenaAllocator.init(self.gpa);
        defer chunk.deinit();
        var w = try spill.Writer.init(space, tmp.allocator(), self.in_schema, "sort");
        errdefer w.abort();
        const path = try paths.dupe(u8, w.path);
        const cols = try tmp.allocator().alloc(column.Column, all.columns.len);
        var lo: usize = 0;
        while (lo < idx.len) {
            const hi = @min(idx.len, lo + @max(self.batch_rows, 1));
            _ = chunk.reset(.retain_capacity);
            for (all.columns, cols) |c, *o| o.* = try column.permute(chunk.allocator(), c, idx[lo..hi]);
            try w.write(.{ .schema = self.in_schema, .columns = cols, .len = hi - lo });
            lo = hi;
        }
        var run = try w.finish();
        run.path = path;
        return run;
    }

    /// Merges `group`, consecutive runs in input order, into one new run and deletes
    /// the group's files whatever happens.
    fn mergeRuns(self: *Sort, space: Space, paths: std.mem.Allocator, group: []const spill.Run) !spill.Run {
        const m = try Merge.init(self.gpa, self.in_schema, self.keys, group, null, null);
        defer m.deinit();
        var tmp = std.heap.ArenaAllocator.init(self.gpa);
        defer tmp.deinit();
        var chunk = std.heap.ArenaAllocator.init(self.gpa);
        defer chunk.deinit();
        var w = try spill.Writer.init(space, tmp.allocator(), self.in_schema, "sort");
        errdefer w.abort();
        const path = try paths.dupe(u8, w.path);
        while (true) {
            _ = chunk.reset(.retain_capacity);
            const b = (try m.next(chunk.allocator(), @max(self.batch_rows, 1))) orelse break;
            try w.write(b);
        }
        var run = try w.finish();
        run.path = path;
        return run;
    }
};

const Sorted = struct { idx: []usize, arrs: []KeyArr };

fn deleteRuns(runs: []const spill.Run) void {
    for (runs) |r| spill.discard(r);
}

fn concatChunks(arena: std.mem.Allocator, schema: *const types.Schema, chunks: []const Batch, total: usize, threads: usize) !Batch {
    const cols = try arena.alloc(column.Column, schema.fields.len);
    const per = try arena.alloc(column.Column, chunks.len);
    for (cols, 0..) |*out, ci| {
        for (chunks, per) |b, *p| p.* = b.columns[ci];
        out.* = try column.concatThreads(arena, per, total, threads);
    }
    return .{ .schema = schema, .columns = cols, .len = total };
}

/// A k-way merge of sorted sources: spilled runs in input order, then optionally the
/// in-memory remainder `mem`, whose rows are `cols` in the order of `idx`. A binary
/// heap of source indices orders heads by `KeyArr.orderAcross` and, on equal keys,
/// by source index, so ties keep input order. Each output batch gathers its rows per
/// source batch with `permute`, then interleaves them; a source is closed, its file
/// deleted, only once the batch reading from its mapping has been copied out.
pub const Merge = struct {
    gpa: std.mem.Allocator,
    schema: *const types.Schema,
    keys: []const Sort.Key,
    cursors: []Cursor,
    heap: []u32,
    live: usize = 0,
    held: ?*std.heap.ArenaAllocator,

    pub const Mem = struct { cols: []const column.Column, arrs: []const KeyArr, idx: []const usize };

    const Cursor = struct {
        reader: ?spill.Reader = null,
        path: []const u8 = "",
        space: ?Space = null,
        bytes: u64 = 0,
        ar: std.heap.ArenaAllocator,
        cols: []const column.Column = &.{},
        arrs: []const KeyArr = &.{},
        idx: ?[]const usize = null,
        pos: usize = 0,
        len: usize = 0,
        seg: ?u32 = null,
        exhausted: bool = false,

        fn row(self: *const Cursor) usize {
            return if (self.idx) |o| o[self.pos] else self.pos;
        }

        fn load(self: *Cursor, keys: []const Sort.Key) !bool {
            self.seg = null;
            if (self.reader) |*r| {
                while (true) {
                    _ = self.ar.reset(.retain_capacity);
                    const a = self.ar.allocator();
                    const b = (try r.next(a)) orelse break;
                    if (b.len == 0) continue;
                    const arrs = try a.alloc(KeyArr, keys.len);
                    for (keys, arrs) |k, *x| x.* = try KeyArr.prepare(a, b.columns[k.idx], k);
                    self.cols = b.columns;
                    self.arrs = arrs;
                    self.pos = 0;
                    self.len = b.len;
                    return true;
                }
            }
            self.exhausted = true;
            return false;
        }

        fn close(self: *Cursor, gpa: std.mem.Allocator) void {
            if (self.reader) |*r| r.close();
            self.reader = null;
            if (self.path.len > 0) {
                std.fs.cwd().deleteFile(self.path) catch {};
                if (self.space) |s| s.release(self.bytes);
                gpa.free(self.path);
            }
            self.path = "";
        }
    };

    const Seg = struct { cols: []const column.Column, picks: std.ArrayListUnmanaged(usize) = .empty };

    /// Takes ownership of `runs`' files and of `held`, which backs `mem`; both are
    /// released by `deinit`, or here on failure.
    pub fn init(gpa: std.mem.Allocator, schema: *const types.Schema, keys: []const Sort.Key, runs: []const spill.Run, mem: ?Mem, held: ?*std.heap.ArenaAllocator) !*Merge {
        const self = gpa.create(Merge) catch |e| {
            deleteRuns(runs);
            freeHeld(gpa, held);
            return e;
        };
        self.* = .{ .gpa = gpa, .schema = schema, .keys = keys, .cursors = &.{}, .heap = &.{}, .held = held };
        const n = runs.len + @intFromBool(mem != null);
        self.cursors = gpa.alloc(Cursor, n) catch |e| {
            deleteRuns(runs);
            self.deinit();
            return e;
        };
        for (self.cursors) |*c| c.* = .{ .ar = .init(gpa) };
        for (self.cursors[0..runs.len], runs) |*c, r| {
            c.path = gpa.dupe(u8, r.path) catch |e| {
                deleteRuns(runs);
                self.deinit();
                return e;
            };
        }
        errdefer self.deinit();
        self.heap = try gpa.alloc(u32, n);
        for (self.cursors[0..runs.len], runs) |*c, r| {
            c.space = r.space;
            c.bytes = r.bytes;
            c.reader = try spill.Reader.open(r);
            if (try c.load(keys)) self.push(c);
        }
        if (mem) |m| {
            const c = &self.cursors[runs.len];
            c.* = .{ .ar = c.ar, .cols = m.cols, .arrs = m.arrs, .idx = m.idx, .len = m.idx.len, .exhausted = m.idx.len == 0 };
            if (!c.exhausted) self.push(c);
        }
        var i = self.live / 2;
        while (i > 0) {
            i -= 1;
            self.siftDown(i);
        }
        return self;
    }

    fn freeHeld(gpa: std.mem.Allocator, held: ?*std.heap.ArenaAllocator) void {
        const h = held orelse return;
        h.deinit();
        gpa.destroy(h);
    }

    pub fn deinit(self: *Merge) void {
        for (self.cursors) |*c| {
            c.close(self.gpa);
            c.ar.deinit();
        }
        self.gpa.free(self.cursors);
        self.gpa.free(self.heap);
        freeHeld(self.gpa, self.held);
        self.gpa.destroy(self);
    }

    fn push(self: *Merge, c: *Cursor) void {
        self.heap[self.live] = @intCast(c - self.cursors.ptr);
        self.live += 1;
    }

    fn less(self: *const Merge, x: u32, y: u32) bool {
        const a = &self.cursors[x];
        const b = &self.cursors[y];
        const ra = a.row();
        const rb = b.row();
        for (a.arrs, b.arrs) |ka, kb| {
            const o = ka.orderAcross(ra, kb, rb);
            if (o != .eq) return o == .lt;
        }
        return x < y;
    }

    fn siftDown(self: *Merge, start: usize) void {
        var i = start;
        while (true) {
            const l = 2 * i + 1;
            if (l >= self.live) return;
            var m = l;
            if (l + 1 < self.live and self.less(self.heap[l + 1], self.heap[l])) m = l + 1;
            if (!self.less(self.heap[m], self.heap[i])) return;
            std.mem.swap(u32, &self.heap[i], &self.heap[m]);
            i = m;
        }
    }

    /// Up to `max_rows` of the merged rows, copied into `arena`; null once every
    /// source is drained.
    pub fn next(self: *Merge, arena: std.mem.Allocator, max_rows: usize) !?Batch {
        if (self.live == 0) return null;
        for (self.cursors) |*c| c.seg = null;
        var segs: std.ArrayListUnmanaged(Seg) = .empty;
        const from = try arena.alloc(u32, max_rows);
        var n: usize = 0;
        while (n < max_rows and self.live > 0) : (n += 1) {
            const c = &self.cursors[self.heap[0]];
            const s = c.seg orelse blk: {
                try segs.append(arena, .{ .cols = try arena.dupe(column.Column, c.cols) });
                const s: u32 = @intCast(segs.items.len - 1);
                c.seg = s;
                break :blk s;
            };
            try segs.items[s].picks.append(arena, c.row());
            from[n] = s;
            c.pos += 1;
            if (c.pos == c.len and !try c.load(self.keys)) {
                self.live -= 1;
                self.heap[0] = self.heap[self.live];
            }
            if (self.live > 0) self.siftDown(0);
        }
        const out = try self.gather(arena, segs.items, from[0..n]);
        for (self.cursors) |*c| if (c.exhausted) c.close(self.gpa);
        return out;
    }

    fn gather(self: *Merge, arena: std.mem.Allocator, segs: []const Seg, from: []const u32) !Batch {
        const ncols = self.schema.fields.len;
        const cols = try arena.alloc(column.Column, ncols);
        if (segs.len == 1) {
            for (cols, 0..) |*o, ci| o.* = try column.permute(arena, segs[0].cols[ci], segs[0].picks.items);
            return .{ .schema = self.schema, .columns = cols, .len = from.len };
        }
        var in_order = true;
        for (from[1..], from[0 .. from.len - 1]) |b, a| {
            if (b < a) in_order = false;
        }
        var map: []usize = &.{};
        if (!in_order) {
            const at = try arena.alloc(usize, segs.len);
            var off: usize = 0;
            for (segs, at) |s, *x| {
                x.* = off;
                off += s.picks.items.len;
            }
            map = try arena.alloc(usize, from.len);
            for (from, map) |s, *m| {
                m.* = at[s];
                at[s] += 1;
            }
        }
        const subs = try arena.alloc(column.Column, segs.len);
        for (cols, 0..) |*o, ci| {
            for (segs, subs) |s, *sub| sub.* = try column.permute(arena, s.cols[ci], s.picks.items);
            const whole = try column.concat(arena, subs, from.len);
            o.* = if (in_order) whole else try column.permute(arena, whole, map);
        }
        return .{ .schema = self.schema, .columns = cols, .len = from.len };
    }
};

pub const KeyArr = struct {
    desc: bool,
    nulls_first: bool,
    valid: column.Bitmap,
    data: Data,
    col: column.Column,
    scale: u8 = 0,

    const Data = union(enum) {
        ints: []i64,
        floats: []f64,
        decs: []i128,
        strs: [][]const u8,
        boxed: column.Column,
    };

    /// `prepare`, a string key's slices filled on up to `threads` threads.
    pub fn prepareThreads(arena: std.mem.Allocator, col: column.Column, key: Sort.Key, threads: usize) !KeyArr {
        const nc = par.chunks(col.len, 1 << 16);
        if (threads < 2 or nc < 2 or (col.ty.kind != .string and col.ty.kind != .bytes)) return prepare(arena, col, key);
        const Fill = struct {
            b: column.Bytes,
            out: [][]const u8,
            nc: usize,
            fn f(x: *const @This(), c: usize) void {
                const r = par.bounds(x.out.len, x.nc, c);
                for (x.out[r[0]..r[1]], r[0]..) |*o, i| o.* = x.b.at(i);
            }
        };
        const out = try arena.alloc([]const u8, col.len);
        par.each(threads, nc, &Fill{ .b = col.data.bytes, .out = out, .nc = nc }, Fill.f);
        return .{ .desc = key.desc, .nulls_first = key.nulls_first, .valid = col.validity, .data = .{ .strs = out }, .col = col, .scale = 0 };
    }

    pub fn prepare(arena: std.mem.Allocator, col: column.Column, key: Sort.Key) !KeyArr {
        const n = col.len;
        var scale: u8 = 0;
        const data: Data = switch (col.ty.kind) {
            .int, .time, .timestamp => .{ .ints = col.data.i64 },
            .date => blk: {
                const out = try arena.alloc(i64, n);
                for (out, col.data.i32[0..n]) |*o, x| o.* = x;
                break :blk .{ .ints = out };
            },
            .bool => blk: {
                const out = try arena.alloc(i64, n);
                for (out, col.data.b[0..n]) |*o, x| o.* = @intFromBool(x);
                break :blk .{ .ints = out };
            },
            .float => .{ .floats = col.data.f64 },
            .decimal => blk: {
                for (col.data.dec[0..n], 0..) |d, i| {
                    if (col.validity.get(i)) scale = @max(scale, d.scale);
                }
                const out = try arena.alloc(i128, n);
                for (out, col.data.dec[0..n], 0..) |*o, d, i| {
                    o.* = if (!col.validity.get(i)) 0 else (eval.rescaleTo(d, scale) orelse break :blk Data{ .boxed = col }).unscaled;
                }
                break :blk .{ .decs = out };
            },
            .string, .bytes => blk: {
                const out = try arena.alloc([]const u8, n);
                for (out, 0..) |*o, i| o.* = col.data.bytes.at(i);
                break :blk .{ .strs = out };
            },
            else => .{ .boxed = col },
        };
        return .{ .desc = key.desc, .nulls_first = key.nulls_first, .valid = col.validity, .data = data, .col = col, .scale = scale };
    }

    /// `order` between row `a` of this batch's key and row `c` of `other`, the same
    /// key prepared over another batch. Decimals were scaled per batch, so they compare
    /// at the larger scale in i256, exactly as one batch holding both would; a kind
    /// either side boxed compares as `.boxed` does.
    pub fn orderAcross(self: KeyArr, a: usize, other: KeyArr, c: usize) std.math.Order {
        const an = !self.valid.get(a);
        const bn = !other.valid.get(c);
        if (an or bn) return nullOrder(an, bn, self.nulls_first);
        const ord: std.math.Order = switch (self.data) {
            .ints => |v| if (other.data == .ints) std.math.order(v[a], other.data.ints[c]) else return self.boxedAcross(a, other, c),
            .floats => |v| if (other.data == .floats) eval.orderF64(v[a], other.data.floats[c]) else return self.boxedAcross(a, other, c),
            .decs => |v| if (other.data == .decs) Decimal.order(.{ .unscaled = v[a], .scale = self.scale }, .{ .unscaled = other.data.decs[c], .scale = other.scale }) else return self.boxedAcross(a, other, c),
            .strs => |v| if (other.data == .strs) std.mem.order(u8, v[a], other.data.strs[c]) else return self.boxedAcross(a, other, c),
            .boxed => return self.boxedAcross(a, other, c),
        };
        return if (self.desc) ord.invert() else ord;
    }

    fn boxedAcross(self: KeyArr, a: usize, other: KeyArr, c: usize) std.math.Order {
        return keyOrder(self.col.getValue(a), other.col.getValue(c), self.desc, self.nulls_first);
    }

    pub fn order(self: KeyArr, a: usize, c: usize) std.math.Order {
        const an = !self.valid.get(a);
        const bn = !self.valid.get(c);
        if (an or bn) return nullOrder(an, bn, self.nulls_first);
        const ord: std.math.Order = switch (self.data) {
            .ints => |v| std.math.order(v[a], v[c]),
            .floats => |v| eval.orderF64(v[a], v[c]),
            .decs => |v| std.math.order(v[a], v[c]),
            .strs => |v| std.mem.order(u8, v[a], v[c]),
            .boxed => |col| return keyOrder(col.getValue(a), col.getValue(c), self.desc, self.nulls_first),
        };
        return if (self.desc) ord.invert() else ord;
    }
};

pub const SortCtx = struct {
    arrs: []const KeyArr,

    pub fn lessThan(self: SortCtx, a: usize, c: usize) bool {
        for (self.arrs) |k| {
            const o = k.order(a, c);
            if (o != .eq) return o == .lt;
        }
        return false;
    }
};

/// Sort `idx` (pre-filled 0..n) by `arrs`, first key most significant, stably,
/// with the radix scheme the module header describes.
pub fn sortIdx(out_arena: std.mem.Allocator, idx: []usize, arrs: []const KeyArr) !void {
    return sortIdxThreads(out_arena, idx, arrs, 1);
}

pub fn sortIdxThreads(out_arena: std.mem.Allocator, idx: []usize, arrs: []const KeyArr, threads: usize) !void {
    _ = out_arena;
    const n = idx.len;
    if (n == 0 or arrs.len == 0) return;
    var scratch = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer scratch.deinit();
    const arena = scratch.allocator();
    const plans = try arena.alloc(KeyPlan, arrs.len);
    var exact = true;
    for (arrs, plans) |k, *p| {
        p.* = KeyPlan.of(k, n) orelse {
            std.mem.sort(usize, idx, SortCtx{ .arrs = arrs }, SortCtx.lessThan);
            return;
        };
        if (!p.exact) exact = false;
    }
    if (n > std.math.maxInt(u32)) {
        std.mem.sort(usize, idx, SortCtx{ .arrs = arrs }, SortCtx.lessThan);
        return;
    }
    if (try sortIdxParallel(arena, idx, arrs, plans, exact, threads)) return;
    try sortRows(arena, idx, arrs, plans, exact);
}

/// Sorts `idx` stably: every row's key words (a null flag word first for a key
/// with nulls) and its row number are packed once, in `idx` order, into records
/// that an MSD byte radix then orders without reading a column again.
fn sortRows(arena: std.mem.Allocator, idx: []usize, arrs: []const KeyArr, plans: []const KeyPlan, exact: bool) !void {
    const n = idx.len;
    var stride: usize = 1;
    for (plans) |p| stride += p.words + @intFromBool(p.nulls);
    const recs = try arena.alloc(u64, n * stride);
    const tmp = try arena.alloc(u64, n * stride);
    for (idx, 0..) |row, r| {
        const rec = recs[r * stride ..][0..stride];
        var o: usize = 0;
        for (arrs, plans) |k, p| {
            if (p.nulls) {
                rec[o] = @intFromBool(k.valid.get(row) == k.nulls_first);
                o += 1;
            }
            for (0..p.words) |w| rec[o + w] = encWord(k, p, w, row);
            o += p.words;
        }
        rec[o] = row;
    }
    msdSort(recs, tmp, stride, 0);
    for (idx, 0..) |*x, r| x.* = @intCast(recs[r * stride + stride - 1]);
    if (!exact) fixTies(idx, arrs, plans);
}

/// MSD radix over `recs`' key words a byte at a time from digit `d0`, the trailing
/// row number left as the tie-break. Bytes every record shares are skipped, found in
/// one pass per word; the scatter is stable, so records equal on every key keep the
/// order they came in, and a small bucket goes to an insertion sort on the words
/// with the row number last.
pub fn msdSort(recs: []u64, tmp: []u64, stride: usize, d0: usize) void {
    const n = recs.len / stride;
    if (n < 2) return;
    const nkey = stride - 1;
    if (n <= 24) return insertionSort(recs, tmp[0..stride], stride, d0 / 8);
    var d = d0;
    while (d < nkey * 8) {
        const wi = d / 8;
        const first = recs[wi];
        var diff: u64 = 0;
        var r: usize = 0;
        while (r < n) : (r += 1) diff |= recs[r * stride + wi] ^ first;
        const done: u6 = @intCast((d % 8) * 8);
        diff &= @as(u64, std.math.maxInt(u64)) >> done;
        if (diff == 0) {
            d = (wi + 1) * 8;
            continue;
        }
        d = wi * 8 + @clz(diff) / 8;
        break;
    } else return;
    const wi = d / 8;
    const sh: u6 = @intCast(56 - (d % 8) * 8);
    var counts = [_]usize{0} ** 257;
    var r: usize = 0;
    while (r < n) : (r += 1) counts[@as(usize, @intCast((recs[r * stride + wi] >> sh) & 0xff)) + 1] += 1;
    for (1..257) |b| counts[b] += counts[b - 1];
    var fill: [256]usize = counts[0..256].*;
    r = 0;
    while (r < n) : (r += 1) {
        const b: usize = @intCast((recs[r * stride + wi] >> sh) & 0xff);
        @memcpy(tmp[fill[b] * stride ..][0..stride], recs[r * stride ..][0..stride]);
        fill[b] += 1;
    }
    @memcpy(recs, tmp);
    for (0..256) |b| {
        const lo = counts[b];
        const hi = counts[b + 1];
        if (hi - lo > 1) msdSort(recs[lo * stride .. hi * stride], tmp[lo * stride .. hi * stride], stride, d + 1);
    }
}

fn insertionSort(recs: []u64, hold: []u64, stride: usize, from: usize) void {
    const n = recs.len / stride;
    var i: usize = 1;
    while (i < n) : (i += 1) {
        @memcpy(hold, recs[i * stride ..][0..stride]);
        var j = i;
        while (j > 0 and recLess(hold, recs[(j - 1) * stride ..][0..stride], from)) : (j -= 1)
            @memcpy(recs[j * stride ..][0..stride], recs[(j - 1) * stride ..][0..stride]);
        @memcpy(recs[j * stride ..][0..stride], hold);
    }
}

fn recLess(a: []const u64, b: []const u64, from: usize) bool {
    for (a[from..], b[from..]) |x, y| if (x != y) return x < y;
    return false;
}

/// The passes that deal rows to the sort's ranges, each run on chunks of the rows:
/// `scan` encodes the first key's leading word once and finds its bounds,
/// `histogram` counts its leading 11 bits, `count` sizes each chunk's share of every
/// range and `scatter` writes the rows there, chunk after chunk, so a range keeps
/// input order. `sortRange` then sorts one range.
const Deal = struct {
    k: KeyArr,
    p: KeyPlan,
    w0: []u64,
    idx: []usize,
    nc: usize,
    lo: []u64,
    hi: []u64,
    hist: [][1 << 11]u32,
    pos: [][max_ranges]usize,
    lo_all: u64 = 0,
    shift: u6 = 0,
    of_digit: [1 << 11]u8 = undefined,
    null_range: u8 = 0,
    arrs: []const KeyArr,
    plans: []const KeyPlan,
    exact: bool,
    start: [max_ranges + 1]usize = undefined,
    errs: [max_ranges]?anyerror = @splat(null),

    const max_ranges = 128;

    fn rows(d: *const Deal, c: usize) [2]usize {
        return par.bounds(d.idx.len, d.nc, c);
    }

    fn isNull(d: *const Deal, row: usize) bool {
        return d.p.nulls and !d.k.valid.get(row);
    }

    fn rangeOf(d: *const Deal, row: usize) u8 {
        if (d.isNull(row)) return d.null_range;
        return d.of_digit[@intCast((d.w0[row] - d.lo_all) >> d.shift)];
    }

    fn scan(d: *Deal, c: usize) void {
        const r = d.rows(c);
        var mn: u64 = std.math.maxInt(u64);
        var mx: u64 = 0;
        for (r[0]..r[1]) |row| {
            if (d.isNull(row)) continue;
            const w = encWord(d.k, d.p, 0, row);
            d.w0[row] = w;
            mn = @min(mn, w);
            mx = @max(mx, w);
        }
        d.lo[c] = mn;
        d.hi[c] = mx;
    }

    fn histogram(d: *Deal, c: usize) void {
        const r = d.rows(c);
        const h = &d.hist[c];
        @memset(h, 0);
        for (r[0]..r[1]) |row| {
            if (d.isNull(row)) continue;
            h[@intCast((d.w0[row] - d.lo_all) >> d.shift)] += 1;
        }
    }

    fn count(d: *Deal, c: usize) void {
        const r = d.rows(c);
        const at = &d.pos[c];
        @memset(at, 0);
        for (r[0]..r[1]) |row| at[d.rangeOf(row)] += 1;
    }

    fn scatter(d: *Deal, c: usize) void {
        const r = d.rows(c);
        const at = &d.pos[c];
        for (r[0]..r[1]) |row| {
            const g = d.rangeOf(row);
            d.idx[at[g]] = row;
            at[g] += 1;
        }
    }

    fn sortRange(d: *Deal, g: usize) void {
        const part = d.idx[d.start[g]..d.start[g + 1]];
        if (part.len < 2) return;
        var ar = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer ar.deinit();
        sortRows(ar.allocator(), part, d.arrs, d.plans, d.exact) catch |e| {
            d.errs[g] = e;
        };
    }
};

/// Deal rows, in input order, into ranges of the first key split where a histogram
/// puts about an even share in each, a few per thread so a slow core takes fewer
/// (nulls in a range of their own, first or last as the key places them), and sort
/// each range. False when not worth it or the first key cannot split.
fn sortIdxParallel(arena: std.mem.Allocator, idx: []usize, arrs: []const KeyArr, plans: []const KeyPlan, exact: bool, threads: usize) !bool {
    const n = idx.len;
    if (threads < 2 or n < 1 << 17) return false;
    const nc = par.chunks(n, 1 << 16);
    var d = Deal{
        .k = arrs[0],
        .p = plans[0],
        .w0 = try arena.alloc(u64, n),
        .idx = idx,
        .nc = nc,
        .lo = try arena.alloc(u64, nc),
        .hi = try arena.alloc(u64, nc),
        .hist = try arena.alloc([1 << 11]u32, nc),
        .pos = try arena.alloc([Deal.max_ranges]usize, nc),
        .arrs = arrs,
        .plans = plans,
        .exact = exact,
    };
    par.each(threads, nc, &d, Deal.scan);
    var lo: u64 = std.math.maxInt(u64);
    var hi: u64 = 0;
    for (d.lo, d.hi) |l, h| {
        lo = @min(lo, l);
        hi = @max(hi, h);
    }
    if (lo >= hi) return false;
    const bits: u32 = 64 - @clz(hi - lo);
    d.lo_all = lo;
    d.shift = @intCast(if (bits > 11) bits - 11 else 0);
    par.each(threads, nc, &d, Deal.histogram);
    var hist = [_]u32{0} ** (1 << 11);
    var nonnull: usize = 0;
    for (d.hist) |h| for (&hist, h) |*t, x| {
        t.* += x;
        nonnull += x;
    };
    const want = @min(threads * 4, Deal.max_ranges - 1);
    var cur: usize = 0;
    var acc: usize = 0;
    for (hist, &d.of_digit) |h, *dg| {
        dg.* = @intCast(cur);
        acc += h;
        if (cur + 1 < want and acc * want >= nonnull * (cur + 1)) cur += 1;
    }
    const nranges = cur + 2;
    d.null_range = if (d.k.nulls_first) 0 else @intCast(cur + 1);
    if (d.k.nulls_first) {
        for (&d.of_digit) |*dg| dg.* += 1;
    }
    par.each(threads, nc, &d, Deal.count);
    var size = [_]usize{0} ** Deal.max_ranges;
    for (d.pos) |pp| for (size[0..nranges], pp[0..nranges]) |*t, x| {
        t.* += x;
    };
    d.start[0] = 0;
    for (0..nranges) |g| d.start[g + 1] = d.start[g] + size[g];
    var at: [Deal.max_ranges]usize = undefined;
    @memcpy(at[0..nranges], d.start[0..nranges]);
    for (d.pos) |*pp| for (pp[0..nranges], at[0..nranges]) |*x, *t| {
        const c = x.*;
        x.* = t.*;
        t.* += c;
    };
    par.each(threads, nc, &d, Deal.scatter);
    par.each(threads, nranges, &d, Deal.sortRange);
    for (d.errs[0..nranges]) |e| if (e) |err| return err;
    return true;
}

const KeyPlan = struct {
    words: u8,
    exact: bool,
    nulls: bool,

    const max_exact_str = 31;
    const prefix_words = 3;

    fn of(k: KeyArr, n: usize) ?KeyPlan {
        const nulls = !k.valid.allSet(n);
        return switch (k.data) {
            .ints, .floats => .{ .words = 1, .exact = true, .nulls = nulls },
            .decs => .{ .words = 2, .exact = true, .nulls = nulls },
            .strs => |v| blk: {
                var longest: usize = 0;
                for (v[0..n], 0..) |x, i| {
                    if (k.valid.get(i)) longest = @max(longest, x.len);
                }
                if (longest > max_exact_str) break :blk .{ .words = prefix_words, .exact = false, .nulls = nulls };
                break :blk .{ .words = @intCast(longest / 8 + 1), .exact = true, .nulls = nulls };
            },
            .boxed => null,
        };
    }
};

/// Word `w` (0 most significant) of row `i`'s key. A null row's words are 0: the
/// flag word places it, and among nulls the key is all equal.
fn encWord(k: KeyArr, p: KeyPlan, w: usize, i: usize) u64 {
    if (p.nulls and !k.valid.get(i)) return 0;
    const word: u64 = switch (k.data) {
        .ints, .floats => return orderedWord(k, i),
        .decs => |v| blk: {
            const u: u128 = @as(u128, @bitCast(v[i])) ^ (@as(u128, 1) << 127);
            break :blk if (w == 0) @intCast(u >> 64) else @truncate(u);
        },
        .strs => |v| blk: {
            const str = v[i];
            var b: [8]u8 = @splat(0);
            const lo = w * 8;
            if (lo < str.len) {
                const take = @min(8, str.len - lo);
                @memcpy(b[0..take], str[lo..][0..take]);
            }
            if (p.exact and w + 1 == p.words) b[7] = @intCast(str.len);
            break :blk std.mem.readInt(u64, &b, .big);
        },
        .boxed => unreachable,
    };
    return if (k.desc) ~word else word;
}

/// Re-sort, with the comparator, each run the string prefixes could not order. A
/// run is equal on every key's words up to the first inexact key only, so a later
/// key cannot decide between rows that still differ in it.
fn fixTies(idx: []usize, arrs: []const KeyArr, plans: []const KeyPlan) void {
    var m: usize = 0;
    while (plans[m].exact) m += 1;
    var start: usize = 0;
    while (start < idx.len) {
        var end = start + 1;
        while (end < idx.len and sameWords(arrs[0 .. m + 1], plans[0 .. m + 1], idx[start], idx[end])) end += 1;
        if (end - start > 1) std.mem.sort(usize, idx[start..end], SortCtx{ .arrs = arrs }, SortCtx.lessThan);
        start = end;
    }
}

fn sameWords(arrs: []const KeyArr, plans: []const KeyPlan, a: usize, b: usize) bool {
    for (arrs, plans) |k, p| {
        if (k.valid.get(a) != k.valid.get(b)) return false;
        var w: usize = 0;
        while (w < p.words) : (w += 1) {
            if (encWord(k, p, w, a) != encWord(k, p, w, b)) return false;
        }
    }
    return true;
}

/// The u64 whose unsigned order is the key's: ints flip the sign bit, floats use the
/// IEEE trick with NaN canonicalized last and -0.0 folded into 0.0; `desc` complements.
fn orderedWord(k: KeyArr, i: usize) u64 {
    const w: u64 = switch (k.data) {
        .ints => |v| @as(u64, @bitCast(v[i])) ^ (1 << 63),
        .floats => |v| blk: {
            const x = if (v[i] == 0) 0.0 else v[i];
            const b: u64 = if (std.math.isNan(x)) 0x7ff8000000000000 else @bitCast(x);
            break :blk if (b >> 63 != 0) ~b else b | (1 << 63);
        },
        else => unreachable,
    };
    return if (k.desc) ~w else w;
}

/// Row `i`'s key as one number whose unsigned order is the key's on its leading word:
/// the null flag `sortRows` writes above the first word `encWord` writes, a null's
/// word 0. The whole key for an integer, date, bool or float (`leadExact`); a
/// string's 8-byte prefix and any other kind's 0, so equal leads still compare.
pub inline fn leadKey(k: KeyArr, i: usize) u128 {
    const valid = k.valid.get(i);
    const flag: u128 = @intFromBool(valid == k.nulls_first);
    if (!valid) return flag << 64;
    const w: u64 = switch (k.data) {
        .ints, .floats => orderedWord(k, i),
        .strs => encWord(k, .{ .words = 1, .exact = false, .nulls = false }, 0, i),
        .decs, .boxed => 0,
    };
    return flag << 64 | w;
}

/// Whether `leadKey` of a column of `kind` is its whole key.
pub fn leadExact(kind: types.TypeKind) bool {
    return switch (kind) {
        .int, .time, .timestamp, .date, .bool, .float => true,
        else => false,
    };
}

fn nullOrder(an: bool, bn: bool, nulls_first: bool) std.math.Order {
    if (an and bn) return .eq;
    return if (an == nulls_first) .lt else .gt;
}

/// Order of two sort-key values: nulls before or after every value as `nulls_first`
/// says, `desc` flips non-null order. Shared by `SortCtx.lessThan` and Top-N.
pub fn keyOrder(va: Value, vb: Value, desc: bool, nulls_first: bool) std.math.Order {
    const an = va.isNull();
    const bn = vb.isNull();
    if (an or bn) return nullOrder(an, bn, nulls_first);
    const ord = eval.compareValues(va, vb) orelse return .eq;
    return if (desc) ord.invert() else ord;
}

/// The order of two rows on `keys`, the first key that tells them apart deciding.
pub fn rowOrder(keys: []const Sort.Key, a: []const Value, b: []const Value) std.math.Order {
    for (keys) |k| {
        const o = keyOrder(a[k.idx], b[k.idx], k.desc, k.nulls_first);
        if (o != .eq) return o;
    }
    return .eq;
}

test "sort: nulls first or last in either direction" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const batches = [_]Batch{
        try intBatch(a, &int_schema, &.{ 3, null }),
        try intBatch(a, &int_schema, &.{ 1, 2 }),
    };
    const cases = [_]struct { key: Sort.Key, want: []const ?i64 }{
        .{ .key = .{ .idx = 0, .desc = false, .nulls_first = false }, .want = &.{ 1, 2, 3, null } },
        .{ .key = .{ .idx = 0, .desc = false, .nulls_first = true }, .want = &.{ null, 1, 2, 3 } },
        .{ .key = .{ .idx = 0, .desc = true, .nulls_first = true }, .want = &.{ null, 3, 2, 1 } },
        .{ .key = .{ .idx = 0, .desc = true, .nulls_first = false }, .want = &.{ 3, 2, 1, null } },
    };
    for (cases) |c| {
        var ts = TestSource{ .schema_ = int_schema, .batches = &batches };
        var scan = Scan{ .src = ts.src() };
        var srt = Sort{ .child = .{ .scan = &scan }, .in_schema = &int_schema, .keys = &[_]Sort.Key{c.key} };
        try testing.expectEqualDeep(c.want, try drainInts(a, .{ .sort = &srt }));
    }
}

test "sortIdxThreads: the parallel split places the null range first or last as the key says" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var prng = std.Random.DefaultPrng.init(0x9a11);
    const rnd = prng.random();
    const n = (1 << 17) + 999;
    var b = column.Builder.init(a, types.Type.init(.int).asNullable());
    for (0..n) |_| try b.append(if (rnd.uintLessThan(u8, 7) == 0) .null else Value{ .int = rnd.intRangeAtMost(i64, -50_000, 50_000) });
    const col = try b.finish();
    for ([_]bool{ false, true }) |desc| for ([_]bool{ false, true }) |nf| {
        const arrs = [_]KeyArr{try KeyArr.prepare(a, col, .{ .idx = 0, .desc = desc, .nulls_first = nf })};
        const want = try a.alloc(usize, n);
        const got = try a.alloc(usize, n);
        for (want, got, 0..) |*x, *y, i| {
            x.* = i;
            y.* = i;
        }
        std.mem.sort(usize, want, SortCtx{ .arrs = &arrs }, SortCtx.lessThan);
        try sortIdxThreads(a, got, &arrs, 4);
        try testing.expectEqualSlices(usize, want, got);
        try testing.expectEqual(nf, !col.validity.get(got[0]));
    };
}

test "linearize decomposes map-only pipelines source-to-sink; breakers refuse" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var ts = TestSource{ .schema_ = int_schema, .batches = &.{} };
    var scan = Scan{ .src = ts.src() };
    var fx = ast.Expr{ .field = .{ .parts = &[_][]const u8{"x"} } };
    var zero = ast.Expr{ .int_lit = 0 };
    var pred = ast.Expr{ .binary = .{ .op = .gt, .l = &fx, .r = &zero } };
    var flt = Filter{ .child = .{ .scan = &scan }, .pred = &pred, .back = a };
    const pcols = [_]Project.Col{.{ .source = .{ .passthrough = 0 }, .ty = types.Type.init(.int).asNullable() }};
    var proj = Project{ .child = .{ .filter = &flt }, .cols = &pcols, .out_schema = &int_schema };

    const lin = (try linearize(a, .{ .project = &proj })).?;
    try testing.expectEqual(@as(usize, 2), lin.stages.len);
    try testing.expect(lin.stages[0] == .filter);
    try testing.expect(lin.stages[1] == .project);
    try testing.expectEqual(@as(*anyopaque, &ts), lin.src.ptr);

    var srt = Sort{ .child = .{ .project = &proj }, .in_schema = &int_schema, .keys = &.{} };
    try testing.expect((try linearize(a, .{ .sort = &srt })) == null);
}

test "sortIdx: the radix words order rows exactly as the comparator does, on one thread or split across four" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var prng = std.Random.DefaultPrng.init(0xba5a17);
    const rnd = prng.random();

    const long_base = "a-long-prefix-shared-by-many-";
    for (0..60) |round| {
        const n = if (round % 15 == 14) (1 << 17) + rnd.uintLessThan(usize, 9000) else 1 + rnd.uintLessThan(usize, 400);
        const nkeys = 1 + rnd.uintLessThan(usize, 3);
        const arrs = try a.alloc(KeyArr, nkeys);
        for (arrs) |*k| {
            const kind = rnd.uintLessThan(u8, 4);
            const ty: types.Type = switch (kind) {
                0 => types.Type.init(.int),
                1 => types.Type.init(.float),
                2 => types.Type.decimal(18, 3),
                else => types.Type.init(.string),
            };
            var b = column.Builder.init(a, ty.asNullable());
            for (0..n) |_| {
                if (rnd.uintLessThan(u8, 8) == 0) {
                    try b.append(.null);
                    continue;
                }
                try b.append(switch (kind) {
                    0 => Value{ .int = rnd.intRangeAtMost(i64, -5, 5) * @as(i64, if (rnd.boolean()) 1 else std.math.maxInt(i64) / 7) },
                    1 => Value{ .float = switch (rnd.uintLessThan(u8, 6)) {
                        0 => std.math.nan(f64),
                        1 => -0.0,
                        2 => 0.0,
                        else => @as(f64, @floatFromInt(rnd.intRangeAtMost(i64, -3, 3))) / 2,
                    } },
                    2 => Value{ .decimal = .{ .unscaled = rnd.intRangeAtMost(i128, -30, 30), .scale = rnd.uintLessThan(u8, 3) } },
                    else => Value{ .string = blk: {
                        const len = rnd.uintLessThan(usize, 4);
                        const tail = try a.alloc(u8, len);
                        for (tail) |*c| c.* = "ab\x00"[rnd.uintLessThan(usize, 3)];
                        break :blk if (rnd.boolean()) try std.mem.concat(a, u8, &.{ long_base, tail }) else tail;
                    } },
                });
            }
            k.* = try KeyArr.prepare(a, try b.finish(), .{ .idx = 0, .desc = rnd.boolean(), .nulls_first = rnd.boolean() });
        }
        const want = try a.alloc(usize, n);
        const got = try a.alloc(usize, n);
        for (want, got, 0..) |*x, *y, i| {
            x.* = i;
            y.* = i;
        }
        std.mem.sort(usize, want, SortCtx{ .arrs = arrs }, SortCtx.lessThan);
        try sortIdxThreads(a, got, arrs, if (round % 2 == 0) 4 else 1);
        testing.expectEqualSlices(usize, want, got) catch |e| {
            std.debug.print("sortIdx mismatch in round {d}\n", .{round});
            return e;
        };
    }
}

const mixed_schema = types.Schema{ .fields = &.{
    .{ .name = "i", .ty = types.Type.init(.int).asNullable() },
    .{ .name = "f", .ty = types.Type.init(.float).asNullable() },
    .{ .name = "d", .ty = types.Type.decimal(38, 4).asNullable() },
    .{ .name = "s", .ty = types.Type.init(.string).asNullable() },
    .{ .name = "b", .ty = types.Type.init(.bool).asNullable() },
    .{ .name = "dt", .ty = types.Type.init(.date).asNullable() },
    .{ .name = "seq", .ty = types.Type.init(.int) },
} };

/// Batches of `mixed_schema` with few distinct keys, so ties are common: nulls, NaN,
/// both zeros, decimals whose widest scale differs from batch to batch, and strings
/// past the radix prefix. `seq` numbers the rows, so stability shows in the output.
fn mixedBatches(a: std.mem.Allocator, rnd: std.Random, nbatches: usize, max_rows: usize) ![]Batch {
    const out = try a.alloc(Batch, nbatches);
    const fields = mixed_schema.fields;
    var seq: i64 = 0;
    for (out, 0..) |*bt, bi| {
        const rows = rnd.uintLessThan(usize, max_rows + 1);
        var bs: [fields.len]column.Builder = undefined;
        for (&bs, fields) |*b, f| b.* = column.Builder.init(a, f.ty);
        const top_scale: u8 = @intCast(bi % 4);
        for (0..rows) |_| {
            const tail = try a.alloc(u8, rnd.uintLessThan(usize, 3));
            for (tail) |*c| c.* = "ab\x00"[rnd.uintLessThan(usize, 3)];
            const row = [fields.len]Value{
                if (rnd.uintLessThan(u8, 6) == 0) .null else .{ .int = rnd.intRangeAtMost(i64, -3, 3) },
                if (rnd.uintLessThan(u8, 6) == 0) .null else .{ .float = switch (rnd.uintLessThan(u8, 5)) {
                    0 => std.math.nan(f64),
                    1 => -0.0,
                    2 => 0.0,
                    else => @as(f64, @floatFromInt(rnd.intRangeAtMost(i64, -2, 2))) / 2,
                } },
                if (rnd.uintLessThan(u8, 6) == 0) .null else .{ .decimal = .{ .unscaled = rnd.intRangeAtMost(i128, -20, 20), .scale = rnd.uintLessThan(u8, top_scale + 1) } },
                if (rnd.uintLessThan(u8, 6) == 0) .null else .{ .string = if (rnd.boolean()) try std.mem.concat(a, u8, &.{ "a-long-prefix-shared-by-many-rows-", tail }) else tail },
                if (rnd.uintLessThan(u8, 6) == 0) .null else .{ .bool = rnd.boolean() },
                if (rnd.uintLessThan(u8, 6) == 0) .null else .{ .date = rnd.intRangeAtMost(i32, -2, 2) },
                .{ .int = seq },
            };
            seq += 1;
            for (&bs, row) |*b, v| try b.append(v);
        }
        const cols = try a.alloc(column.Column, fields.len);
        for (cols, &bs) |*c, *b| c.* = try b.finish();
        bt.* = .{ .schema = &mixed_schema, .columns = cols, .len = rows };
    }
    return out;
}

const SortRun = struct { space: ?Space = null, spill_at: usize = std.math.maxInt(usize), fan_in: usize = 64, batch_rows: usize = 1 << 16, runs: ?*usize = null };

fn drainSorted(a: std.mem.Allocator, batches: []const Batch, keys: []const Sort.Key, cfg: SortRun) ![]const []const Value {
    var ts = TestSource{ .schema_ = mixed_schema, .batches = batches };
    var scan = Scan{ .src = ts.src() };
    var srt = Sort{
        .child = .{ .scan = &scan },
        .in_schema = &mixed_schema,
        .keys = keys,
        .space = cfg.space,
        .spill_at = cfg.spill_at,
        .fan_in = cfg.fan_in,
        .batch_rows = cfg.batch_rows,
        .gpa = testing.allocator,
    };
    errdefer srt.dropMerge();
    defer if (cfg.runs) |r| {
        r.* = srt.runs;
    };
    var rows = std.array_list.Managed([]const Value).init(a);
    while (try srt.next(a)) |b| {
        if (cfg.space != null and b.len > cfg.batch_rows and srt.merge != null) return error.TestUnexpectedResult;
        for (0..b.len) |r| {
            const row = try a.alloc(Value, b.columns.len);
            for (row, b.columns) |*v, c| v.* = c.getValue(r);
            try rows.append(row);
        }
    }
    return rows.toOwnedSlice();
}

fn expectSameRows(want: []const []const Value, got: []const []const Value) !void {
    try testing.expectEqual(want.len, got.len);
    for (want, got, 0..) |w, g, r| {
        for (w, g) |x, y| {
            const same = if (x == .float and y == .float)
                @as(u64, @bitCast(x.float)) == @as(u64, @bitCast(y.float))
            else
                std.meta.activeTag(x) == std.meta.activeTag(y) and switch (x) {
                    .string => std.mem.eql(u8, x.string, y.string),
                    else => std.meta.eql(x, y),
                };
            if (!same) {
                std.debug.print("row {d}: want {any} got {any}\n", .{ r, x, y });
                return error.TestUnexpectedResult;
            }
        }
    }
}

test "sort spill: merged runs give the in-memory order row for row on mixed keys, nulls and ties" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var prng = std.Random.DefaultPrng.init(0x5011);
    const rnd = prng.random();

    for (0..40) |round| {
        const td = try SpillDir.init(std.math.maxInt(u64));
        defer td.deinit();
        const batches = try mixedBatches(a, rnd, 5 + rnd.uintLessThan(usize, 30), 60);
        const nkeys = 1 + rnd.uintLessThan(usize, 3);
        const keys = try a.alloc(Sort.Key, nkeys);
        for (keys) |*k| k.* = .{ .idx = rnd.uintLessThan(usize, 6), .desc = rnd.boolean(), .nulls_first = rnd.boolean() };
        const cfg = SortRun{
            .space = td.ds.space(),
            .spill_at = ([_]usize{ 0, 500, 2000, 6000 })[round % 4],
            .fan_in = ([_]usize{ 2, 3, 64 })[round % 3],
            .batch_rows = ([_]usize{ 1, 7, 1 << 16 })[(round / 3) % 3],
        };
        const want = try drainSorted(a, batches, keys, .{});
        const got = try drainSorted(a, batches, keys, cfg);
        expectSameRows(want, got) catch |e| {
            std.debug.print("spill mismatch in round {d}\n", .{round});
            return e;
        };
        try testing.expectEqual(@as(usize, 0), try td.count());
    }
}

test "sort spill: many runs merge in passes past the fan-in, ascending and descending" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var prng = std.Random.DefaultPrng.init(0xfa41);
    const rnd = prng.random();

    const batches = try mixedBatches(a, rnd, 90, 40);
    var written: usize = 0;
    for (batches) |b| written += @intFromBool(b.len > 0);
    for ([_]bool{ false, true }) |desc| {
        const keys = [_]Sort.Key{ .{ .idx = 0, .desc = desc, .nulls_first = !desc }, .{ .idx = 3, .desc = !desc, .nulls_first = !desc } };
        const want = try drainSorted(a, batches, &keys, .{});
        for ([_]usize{ 2, 3, 5, 64 }) |fan_in| {
            const td = try SpillDir.init(std.math.maxInt(u64));
            defer td.deinit();
            var runs: usize = 0;
            const got = try drainSorted(a, batches, &keys, .{ .space = td.ds.space(), .spill_at = 0, .fan_in = fan_in, .batch_rows = 16, .runs = &runs });
            try expectSameRows(want, got);
            try testing.expectEqual(written, runs);
            try testing.expect(td.ds.peak.load(.monotonic) > 0);
            try testing.expectEqual(@as(u64, 0), td.ds.used.load(.monotonic));
            try testing.expectEqual(@as(usize, 0), try td.count());
        }
    }
}

test "sort spill: duplicate keys keep input order across runs" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const td = try SpillDir.init(std.math.maxInt(u64));
    defer td.deinit();

    const kv_schema = types.Schema{ .fields = &.{
        .{ .name = "k", .ty = types.Type.init(.int).asNullable() },
        .{ .name = "v", .ty = types.Type.init(.string).asNullable() },
    } };
    const kvBatch = @import("testing_util.zig").kvBatch;
    const batches = [_]Batch{
        try kvBatch(a, &kv_schema, &.{ 2, null, 1 }, &.{ "a", "b", "c" }),
        try kvBatch(a, &kv_schema, &.{ 1, 2 }, &.{ "d", "e" }),
        try kvBatch(a, &kv_schema, &.{ null, 2, 1 }, &.{ "f", "g", "h" }),
        try kvBatch(a, &kv_schema, &.{2}, &.{"i"}),
    };
    var ts = TestSource{ .schema_ = kv_schema, .batches = &batches };
    var scan = Scan{ .src = ts.src() };
    var srt = Sort{
        .child = .{ .scan = &scan },
        .in_schema = &kv_schema,
        .keys = &[_]Sort.Key{.{ .idx = 0, .desc = true, .nulls_first = false }},
        .space = td.ds.space(),
        .spill_at = 0,
        .fan_in = 2,
        .batch_rows = 2,
        .gpa = testing.allocator,
    };
    errdefer srt.dropMerge();
    var vs = std.array_list.Managed([]const u8).init(a);
    while (try srt.next(a)) |b| {
        for (0..b.len) |r| try vs.append(b.columns[1].data.bytes.at(r));
    }
    const want = [_][]const u8{ "a", "e", "g", "i", "c", "d", "h", "b", "f" };
    try testing.expectEqual(want.len, vs.items.len);
    for (want, vs.items) |w, g| try testing.expectEqualStrings(w, g);
    try testing.expectEqual(@as(usize, 0), try td.count());
}

test "sort spill: empty input, empty batches and a single batch" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const td = try SpillDir.init(std.math.maxInt(u64));
    defer td.deinit();
    const keys = [_]Sort.Key{.{ .idx = 0, .desc = false, .nulls_first = false }};
    const cfg = SortRun{ .space = td.ds.space(), .spill_at = 0, .batch_rows = 3 };

    try testing.expectEqual(@as(usize, 0), (try drainSorted(a, &.{}, &keys, cfg)).len);
    var prng = std.Random.DefaultPrng.init(7);
    const empties = try mixedBatches(a, prng.random(), 4, 0);
    try testing.expectEqual(@as(usize, 0), (try drainSorted(a, empties, &keys, cfg)).len);

    const one = try mixedBatches(a, prng.random(), 1, 50);
    try expectSameRows(try drainSorted(a, one, &keys, .{}), try drainSorted(a, one, &keys, cfg));
    try testing.expectEqual(@as(usize, 0), try td.count());
}

test "sort spill: a disk cap too small fails with SpillCapExceeded and leaves no files" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const td = try SpillDir.init(2048);
    defer td.deinit();
    var prng = std.Random.DefaultPrng.init(11);
    const batches = try mixedBatches(a, prng.random(), 20, 40);
    const keys = [_]Sort.Key{.{ .idx = 1, .desc = false, .nulls_first = true }};
    try testing.expectError(error.SpillCapExceeded, drainSorted(a, batches, &keys, .{ .space = td.ds.space(), .spill_at = 0 }));
    try testing.expectEqual(@as(usize, 0), try td.count());
}

test "sort spill: input under the limit is sorted in memory without touching the space" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const td = try SpillDir.init(std.math.maxInt(u64));
    defer td.deinit();
    var prng = std.Random.DefaultPrng.init(3);
    const batches = try mixedBatches(a, prng.random(), 10, 30);
    const keys = [_]Sort.Key{ .{ .idx = 3, .desc = false, .nulls_first = false }, .{ .idx = 2, .desc = true, .nulls_first = true } };
    try expectSameRows(try drainSorted(a, batches, &keys, .{}), try drainSorted(a, batches, &keys, .{ .space = td.ds.space(), .spill_at = 1 << 30 }));
    try testing.expectEqual(@as(u64, 0), td.ds.used.load(.monotonic));
}

test "sort spill: a consumer that stops early leaves no file, charge or memory once the tree is released" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var prng = std.Random.DefaultPrng.init(17);
    const batches = try mixedBatches(a, prng.random(), 30, 40);
    const keys = [_]Sort.Key{.{ .idx = 1, .desc = false, .nulls_first = true }};
    for ([_]usize{ 0, 1, 2, std.math.maxInt(usize) }) |pulls| {
        const td = try SpillDir.init(std.math.maxInt(u64));
        defer td.deinit();
        var ts = TestSource{ .schema_ = mixed_schema, .batches = batches };
        var scan = Scan{ .src = ts.src() };
        var srt = Sort{ .child = .{ .scan = &scan }, .in_schema = &mixed_schema, .keys = &keys, .space = td.ds.space(), .spill_at = 200, .fan_in = 4, .batch_rows = 8, .gpa = testing.allocator };
        var lim = @import("../op.zig").Limit{ .child = .{ .sort = &srt }, .remaining = std.math.maxInt(u64), .to_skip = 0 };
        const top: Op = .{ .limit = &lim };
        var n: usize = 0;
        while (n < pulls) : (n += 1) {
            if (try top.next(a) == null) break;
        }
        if (pulls == 1) try testing.expect(srt.merge != null and try td.count() > 0);
        top.release();
        top.release();
        try testing.expectEqual(@as(usize, 0), try td.count());
        try testing.expectEqual(@as(u64, 0), td.ds.used.load(.monotonic));
        try testing.expect(try top.next(a) == null);
    }
}

test "sort spill: a merge pass that fails gives back no more disk than it charged" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var prng = std.Random.DefaultPrng.init(23);
    const batches = try mixedBatches(a, prng.random(), 24, 40);
    const keys = [_]Sort.Key{.{ .idx = 1, .desc = false, .nulls_first = true }};
    const Counting = struct {
        inner: Space,
        charged: u64 = 0,
        released: u64 = 0,

        fn space(self: *@This()) Space {
            return .{ .ctx = self, .createFn = create, .chargeFn = charge, .releaseFn = release };
        }
        fn create(ctx: *anyopaque, arena: std.mem.Allocator, tag: []const u8) anyerror!Space.File {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            return self.inner.create(arena, tag);
        }
        fn charge(ctx: *anyopaque, bytes: u64) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            try self.inner.charge(bytes);
            self.charged += bytes;
        }
        fn release(ctx: *anyopaque, bytes: u64) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.inner.release(bytes);
            self.released += bytes;
        }
    };
    const probe = try SpillDir.init(std.math.maxInt(u64));
    const peak = blk: {
        defer probe.deinit();
        _ = try drainSorted(a, batches, &keys, .{ .space = probe.ds.space(), .spill_at = 0, .fan_in = 1 << 20 });
        break :blk probe.ds.peak.load(.monotonic);
    };
    const td = try SpillDir.init(peak + 1);
    defer td.deinit();
    var cs = Counting{ .inner = td.ds.space() };
    try testing.expectError(error.SpillCapExceeded, drainSorted(a, batches, &keys, .{ .space = cs.space(), .spill_at = 0, .fan_in = 2 }));
    try testing.expectEqual(cs.charged, cs.released);
    try testing.expectEqual(@as(usize, 0), try td.count());
}
