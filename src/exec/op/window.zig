//! Window functions over sorted partitions: ranking, distribution, offsets, value
//! and aggregate functions, each over its own frame. A breaker: a partition is
//! complete before any of its rows is numbered.
//!
//! One sort over partition ++ order keys, then per function one pass. A frame is
//! resolved to `[start, end)` in sorted positions for every row; both edges only move
//! forward as the row does, whatever the bounds, so ROWS bounds are arithmetic,
//! RANGE `CURRENT ROW` is the peer group, and a RANGE offset walks two pointers over
//! the single order key (made ascending, a DESC key negated). Order-key nulls sort
//! first or last as the key places them and are one another's peers: a null row's
//! offset bound is the null group, and a non-null row's offset never reaches it
//! (UNBOUNDED PRECEDING or FOLLOWING does).
//!
//! An aggregate slides over the frames: rows enter at the end edge and leave at the
//! start edge. Exact state takes a row back by subtraction (integer and decimal sums,
//! counts, BIT_XOR, integer moments for the variances); MIN/MAX keep a monotonic
//! deque; COUNT(DISTINCT) per-value counts. A float sum, the float variances, BIT_AND
//! and BIT_OR merge partial results in two stacks (`TwoStack`), so a row that left
//! leaves no rounding behind. MEDIAN keeps the frame's lower half in a max-heap and
//! its upper half in a min-heap (`Halves`), a row that left dropped when it surfaces
//! at a top, or when such rows outnumber a heap's live ones, so the heaps stay
//! O(frame). Every frame thus costs O(1) amortized per row, MEDIAN O(log frame). IGNORE
//! NULLS reads through the positions of the non-null values and their prefix counts.
//! Each aggregate is a state with `add`, `remove` and `finish` (the invertible ones
//! are `aggregate/funcs.zig`'s own), and `Slider` generates the one loop that slides
//! it, per aggregate and lane of argument kinds read alike (`read.Lane`), reading the
//! sorted column as typed values; every kind read as a value shares one loop.
//!
//! The default frame (`Frame{}`) is RANGE from the partition's start to the current
//! row's peers. A RANGE offset (`Off.int` / `Off.float`) is in the order key's units
//! as `RangeConv` reads it: a float key as itself, any other as an integer — an int
//! times `mul` (10^scale of the offsets), a date's days times `mul` (microseconds a
//! day), a timestamp's or time's microseconds, a decimal rescaled to `scale`.

const Batch = @import("../batch.zig").Batch;
const ErrCtx = @import("../op.zig").ErrCtx;
const Held = @import("sort.zig").Held;
const KeyArr = @import("sort.zig").KeyArr;
const Op = @import("../op.zig").Op;
const Sort = @import("sort.zig").Sort;
const TopK = @import("topk.zig").TopK;
const TopKEntry = @import("topk.zig").Entry;
const Stats = @import("../op.zig").Stats;
const Value = @import("../value.zig").Value;
const ast = @import("../../lang/ast.zig");
const column = @import("../column.zig");
const par = @import("../par.zig");
const errLabel = @import("../op.zig").errLabel;
const eval = @import("../eval.zig");
const floatord = @import("../floatord.zig");
const rescaleTo = @import("../decimal.zig").rescaleTo;
const keyhash = @import("../keyhash.zig");
const aggFuncs = @import("aggregate/funcs.zig");
const read = @import("aggregate/read.zig");
const Ctx = aggFuncs.Ctx;
const Moments = aggFuncs.Moments;
const std = @import("std");
const types = @import("../../lang/types.zig");

pub const Window = struct {
    stats: Stats = .{},
    child: Op,
    in_schema: *const types.Schema,
    out_schema: *const types.Schema,
    part: []const Sort.Key,
    ord: []const Sort.Key,
    funcs: []const Func,
    range: RangeConv = .{},
    done: bool = false,
    err: ?*ErrCtx = null,
    top_k: ?u64 = null,
    gpa: ?std.mem.Allocator = null,
    threads: usize = 1,
    /// Rows per output batch, a multiple of 8.
    batch_rows: usize = 1 << 16,
    held: ?*Held = null,
    fcols: []column.Column = &.{},

    pub const max_top_k: u64 = 1 << 19;

    pub const Off = struct { rows: i64 = 0, int: i128 = 0, float: f64 = 0 };
    pub const Bound = union(enum) { unbounded_preceding, preceding: Off, current_row, following: Off, unbounded_following };
    pub const Frame = struct { range: bool = true, start: Bound = .unbounded_preceding, end: Bound = .current_row };
    pub const RangeConv = struct { float: bool = false, mul: i128 = 1, scale: u8 = 0 };

    pub const Func = struct {
        func: ast.WinFn,
        arg: ?usize = null,
        offset: i64 = 1,
        default: Value = .null,
        distinct: bool = false,
        ignore_nulls: bool = false,
        frame: Frame = .{},
    };

    fn sameOn(arrs: []const KeyArr, a: usize, b: usize) bool {
        for (arrs) |k| {
            if (k.order(a, b) != .eq) return false;
        }
        return true;
    }

    /// ROW_NUMBER up to rank `k`: each partition's best `k` rows through the top-k
    /// core, partitions in ascending key order, nulls last, each best first.
    fn topK(self: *Window, arena: std.mem.Allocator, k: u64, gpa: std.mem.Allocator) anyerror!?Batch {
        if (k == 0) return null;
        const part = try arena.alloc(usize, self.part.len);
        for (part, self.part) |*p, pk| p.* = pk.idx;
        var core = TopK.init(gpa, self.in_schema, self.ord, part, @intCast(k));
        defer core.deinit();
        var pull = std.heap.ArenaAllocator.init(gpa);
        defer pull.deinit();
        var seq: u64 = 0;
        while (try self.child.next(pull.allocator())) |b| {
            try core.add(pull.allocator(), b, seq, null);
            seq += b.len;
            _ = pull.reset(.retain_capacity);
        }

        const groups = try core.finish(arena);
        var total: usize = 0;
        for (groups) |g| total += g.len;
        if (total == 0) return null;
        const rows = try arena.alloc(TopKEntry, total);
        const rn = try arena.alloc(i64, total);
        var at: usize = 0;
        for (groups) |g| {
            @memcpy(rows[at..][0..g.len], g);
            for (rn[at..][0..g.len], 1..) |*x, i| x.* = @intCast(i);
            at += g.len;
        }
        const ncols = self.in_schema.fields.len;
        const cols = try arena.alloc(column.Column, self.out_schema.fields.len);
        try core.columns(arena, rows, self.out_schema.fields[0..ncols], cols[0..ncols]);
        var bd = try column.Builder.initCapacity(arena, self.out_schema.fields[ncols].ty, total);
        for (rn) |x| try bd.append(.{ .int = x });
        cols[ncols] = try bd.finish();
        return Batch{ .schema = self.out_schema, .columns = cols, .len = total };
    }

    pub const Layout = struct {
        pstart: []usize,
        pend: []usize,
        gstart: []usize,
        gend: []usize,
    };

    /// Each sorted row's partition and peer group as `[start, end)` positions, the keys
    /// prepared over the sorted columns so neighbours compare in memory order. On up
    /// to `threads` threads, in chunks: each marks where a partition or a peer group
    /// begins, one pass carries every chunk's last and first boundary to its
    /// neighbours, and each fills its starts forward and its ends backward.
    fn layout(arena: std.mem.Allocator, n: usize, parts: []const KeyArr, ords: []const KeyArr, threads: usize) !Layout {
        const nc = par.chunks(n, 1 << 16);
        var lc = LayoutCtx{
            .l = .{
                .pstart = try arena.alloc(usize, n),
                .pend = try arena.alloc(usize, n),
                .gstart = try arena.alloc(usize, n),
                .gend = try arena.alloc(usize, n),
            },
            .parts = parts,
            .ords = ords,
            .n = n,
            .nc = nc,
            .pnew = try arena.alloc(bool, n),
            .gnew = try arena.alloc(bool, n),
            .carry = try arena.alloc(LayoutCtx.Carry, nc),
        };
        par.each(threads, nc, &lc, LayoutCtx.mark);
        var ps: usize = 0;
        var gs: usize = 0;
        for (lc.carry, 0..) |*c, i| {
            const r = par.bounds(n, nc, i);
            c.pstart = ps;
            c.gstart = gs;
            var k = r[1];
            while (k > r[0]) {
                k -= 1;
                if (lc.pnew[k]) {
                    ps = k;
                    break;
                }
            }
            k = r[1];
            while (k > r[0]) {
                k -= 1;
                if (lc.gnew[k]) {
                    gs = k;
                    break;
                }
            }
        }
        var pe: usize = n;
        var ge: usize = n;
        var i = nc;
        while (i > 0) {
            i -= 1;
            const r = par.bounds(n, nc, i);
            lc.carry[i].pend = pe;
            lc.carry[i].gend = ge;
            for (r[0]..r[1]) |k| if (lc.pnew[k]) {
                pe = k;
                break;
            };
            for (r[0]..r[1]) |k| if (lc.gnew[k]) {
                ge = k;
                break;
            };
        }
        par.each(threads, nc, &lc, LayoutCtx.fill);
        return lc.l;
    }

    const LayoutCtx = struct {
        l: Layout,
        parts: []const KeyArr,
        ords: []const KeyArr,
        n: usize,
        nc: usize,
        pnew: []bool,
        gnew: []bool,
        carry: []Carry,

        const Carry = struct { pstart: usize = 0, gstart: usize = 0, pend: usize = 0, gend: usize = 0 };

        fn mark(x: *LayoutCtx, c: usize) void {
            const r = par.bounds(x.n, x.nc, c);
            for (r[0]..r[1]) |k| {
                const p = k == 0 or !sameOn(x.parts, k - 1, k);
                x.pnew[k] = p;
                x.gnew[k] = p or !sameOn(x.ords, k - 1, k);
            }
        }

        fn fill(x: *LayoutCtx, c: usize) void {
            const r = par.bounds(x.n, x.nc, c);
            const cr = x.carry[c];
            var ps = cr.pstart;
            var gs = cr.gstart;
            for (r[0]..r[1]) |k| {
                if (x.pnew[k]) ps = k;
                if (x.gnew[k]) gs = k;
                x.l.pstart[k] = ps;
                x.l.gstart[k] = gs;
            }
            var pe = cr.pend;
            var ge = cr.gend;
            var k = r[1];
            while (k > r[0]) {
                k -= 1;
                x.l.pend[k] = pe;
                x.l.gend[k] = ge;
                if (x.pnew[k]) pe = k;
                if (x.gnew[k]) ge = k;
            }
        }
    };

    const RangeKeys = struct {
        float: bool,
        ints: []i128 = &.{},
        floats: []f64 = &.{},
        null_at: []bool,
    };

    /// The order key as RANGE offsets compare it, ascending (a DESC key negated): one
    /// number per sorted row, `null_at` marking the rows whose key is null.
    fn rangeKeys(self: *Window, arena: std.mem.Allocator, col: column.Column) !RangeKeys {
        const k = self.ord[0];
        const conv = self.range;
        var rk = RangeKeys{ .float = conv.float, .null_at = try arena.alloc(bool, col.len) };
        if (conv.float) rk.floats = try arena.alloc(f64, col.len) else rk.ints = try arena.alloc(i128, col.len);
        for (0..col.len) |p| {
            const v = col.getValue(p);
            rk.null_at[p] = v.isNull();
            if (v.isNull()) continue;
            if (conv.float) {
                const x = eval.toF64(v);
                rk.floats[p] = if (k.desc) -x else x;
            } else {
                const x: i128 = switch (v) {
                    .int => |x| @as(i128, x) * conv.mul,
                    .date => |x| @as(i128, x) * conv.mul,
                    .time, .timestamp => |x| @as(i128, x) * conv.mul,
                    .decimal => |d| (rescaleTo(d, conv.scale) orelse return error.CastFailed).unscaled,
                    else => return error.TypeMismatch,
                };
                rk.ints[p] = if (k.desc) -x else x;
            }
        }
        return rk;
    }

    const Frames = struct { fs: []usize, fe: []usize };

    /// Each row's frame as `[fs, fe)` sorted positions, an empty frame as `fs == fe`.
    fn frames(arena: std.mem.Allocator, l: Layout, fr: Frame, rk: ?RangeKeys) !Frames {
        const n = l.pstart.len;
        const fs = try arena.alloc(usize, n);
        const fe = try arena.alloc(usize, n);
        var ps: usize = 0;
        while (ps < n) {
            const pe = l.pend[ps];
            if (!fr.range) {
                for (ps..pe) |k| {
                    const ki: i64 = @intCast(k);
                    const lo: i64 = @intCast(ps);
                    const hi: i64 = @intCast(pe);
                    const s: i64 = switch (fr.start) {
                        .unbounded_preceding => lo,
                        .preceding => |o| @max(lo, ki -| o.rows),
                        .current_row => ki,
                        .following => |o| @min(hi, ki +| o.rows),
                        .unbounded_following => hi,
                    };
                    const e: i64 = switch (fr.end) {
                        .unbounded_preceding => lo,
                        .preceding => |o| @max(lo, ki -| o.rows +| 1),
                        .current_row => ki + 1,
                        .following => |o| @min(hi, ki +| o.rows +| 1),
                        .unbounded_following => hi,
                    };
                    fs[k] = @intCast(s);
                    fe[k] = @intCast(@max(s, e));
                }
            } else if (rk) |keys| {
                if (keys.float) {
                    rangeOffsets(f64, keys.floats, keys.null_at, l, fr, ps, pe, fs, fe);
                } else {
                    rangeOffsets(i128, keys.ints, keys.null_at, l, fr, ps, pe, fs, fe);
                }
            } else {
                for (ps..pe) |k| {
                    const s = switch (fr.start) {
                        .unbounded_preceding => ps,
                        .current_row => l.gstart[k],
                        else => unreachable,
                    };
                    const e = switch (fr.end) {
                        .current_row => l.gend[k],
                        .unbounded_following => pe,
                        else => unreachable,
                    };
                    fs[k] = s;
                    fe[k] = @max(s, e);
                }
            }
            ps = pe;
        }
        return .{ .fs = fs, .fe = fe };
    }

    fn offOf(comptime T: type, o: Off) T {
        return if (T == f64) o.float else o.int;
    }

    /// RANGE frames over one partition `[ps, pe)` whose non-null keys ascend in one
    /// block `[lo, hi)`, the nulls before or after it: each offset edge is the first
    /// position past a target, found by a pointer that only moves forward as the keys
    /// grow.
    fn rangeOffsets(comptime T: type, keys: []const T, null_at: []const bool, l: Layout, fr: Frame, ps: usize, pe: usize, fs: []usize, fe: []usize) void {
        var lo = ps;
        while (lo < pe and null_at[lo]) lo += 1;
        var hi = lo;
        while (hi < pe and !null_at[hi]) hi += 1;
        const ns, const ne = if (lo > ps) .{ ps, lo } else .{ hi, pe };
        const Ptr = struct {
            at: usize,
            fn lower(p: *@This(), ks: []const T, end: usize, target: T) usize {
                while (p.at < end and ks[p.at] < target) p.at += 1;
                return p.at;
            }
            fn upper(p: *@This(), ks: []const T, end: usize, target: T) usize {
                while (p.at < end and ks[p.at] <= target) p.at += 1;
                return p.at;
            }
        };
        var sp = Ptr{ .at = lo };
        var ep = Ptr{ .at = lo };
        for (ps..pe) |k| {
            const is_null = null_at[k];
            const s: usize = switch (fr.start) {
                .unbounded_preceding => ps,
                .current_row => l.gstart[k],
                .preceding => |o| if (is_null) ns else sp.lower(keys, hi, keys[k] - offOf(T, o)),
                .following => |o| if (is_null) ns else sp.lower(keys, hi, keys[k] + offOf(T, o)),
                .unbounded_following => pe,
            };
            const e: usize = switch (fr.end) {
                .unbounded_preceding => ps,
                .current_row => l.gend[k],
                .preceding => |o| if (is_null) ne else ep.upper(keys, hi, keys[k] - offOf(T, o)),
                .following => |o| if (is_null) ne else ep.upper(keys, hi, keys[k] + offOf(T, o)),
                .unbounded_following => pe,
            };
            fs[k] = s;
            fe[k] = @max(s, e);
        }
    }

    /// A sliding aggregate over a monoid by two stacks: values enter at the back and
    /// leave from the front, whose entries hold suffix totals refilled from the back
    /// when it runs dry. Nothing is ever subtracted, so a float total never keeps the
    /// rounding of a row that left, at O(1) amortized per row.
    fn TwoStack(comptime T: type, comptime identity: T, comptime combine: fn (T, T) T) type {
        return struct {
            back: std.ArrayList(T) = .empty,
            back_agg: T = identity,
            front: std.ArrayList(T) = .empty,

            const Self = @This();

            fn reset(s: *Self) void {
                s.back.clearRetainingCapacity();
                s.front.clearRetainingCapacity();
                s.back_agg = identity;
            }

            fn push(s: *Self, a: std.mem.Allocator, x: T) !void {
                try s.back.append(a, x);
                s.back_agg = combine(s.back_agg, x);
            }

            fn pop(s: *Self, a: std.mem.Allocator) !void {
                if (s.front.items.len == 0) {
                    var acc = identity;
                    var i = s.back.items.len;
                    while (i > 0) {
                        i -= 1;
                        acc = combine(s.back.items[i], acc);
                        try s.front.append(a, acc);
                    }
                    s.back.clearRetainingCapacity();
                    s.back_agg = identity;
                }
                _ = s.front.pop();
            }

            fn get(s: *const Self) T {
                const f = if (s.front.items.len > 0) s.front.items[s.front.items.len - 1] else identity;
                return combine(f, s.back_agg);
            }
        };
    }

    fn addF(a: f64, b: f64) f64 {
        return a + b;
    }

    fn andI(a: i64, b: i64) i64 {
        return a & b;
    }

    fn orI(a: i64, b: i64) i64 {
        return a | b;
    }

    const Halves = struct {
        lo: Heap,
        hi: Heap,
        nlo: usize = 0,
        nhi: usize = 0,
        gone: usize = 0,

        const Item = struct { x: f64, p: usize };
        const Heap = std.PriorityQueue(Item, bool, cmp);

        /// `floatord` value order, ties by position, so every row is distinct.
        fn lt(a: Item, b: Item) bool {
            const o = floatord.orderF64(a.x, b.x);
            return o == .lt or (o == .eq and a.p < b.p);
        }

        fn cmp(max: bool, a: Item, b: Item) std.math.Order {
            if (a.p == b.p) return .eq;
            return if (if (max) lt(b, a) else lt(a, b)) .lt else .gt;
        }

        fn init(a: std.mem.Allocator) Halves {
            return .{ .lo = Heap.init(a, true), .hi = Heap.init(a, false) };
        }

        fn reset(h: *Halves) void {
            h.lo.clearRetainingCapacity();
            h.hi.clearRetainingCapacity();
            h.nlo = 0;
            h.nhi = 0;
            h.gone = 0;
        }

        fn prune(h: *Halves) void {
            while (h.lo.peek()) |t| {
                if (t.p >= h.gone) break;
                _ = h.lo.remove();
            }
            while (h.hi.peek()) |t| {
                if (t.p >= h.gone) break;
                _ = h.hi.remove();
            }
        }

        fn balance(h: *Halves) !void {
            h.prune();
            if (h.nlo > h.nhi + 1) {
                try h.hi.add(h.lo.remove());
                h.nlo -= 1;
                h.nhi += 1;
            } else if (h.nhi > h.nlo) {
                try h.lo.add(h.hi.remove());
                h.nhi -= 1;
                h.nlo += 1;
            }
            h.prune();
        }

        fn add(h: *Halves, it: Item) !void {
            const low = if (h.lo.peek()) |t| !lt(t, it) else false;
            if (low) {
                try h.lo.add(it);
                h.nlo += 1;
            } else {
                try h.hi.add(it);
                h.nhi += 1;
            }
            try h.balance();
        }

        /// Takes back the row `it`; rows leave in position order.
        fn remove(h: *Halves, it: Item) !void {
            h.gone = it.p + 1;
            const low = if (h.lo.peek()) |t| !lt(t, it) else false;
            if (low) h.nlo -= 1 else h.nhi -= 1;
            try h.balance();
            compact(&h.lo, h.nlo, h.gone, true);
            compact(&h.hi, h.nhi, h.gone, false);
        }

        /// Drops the departed rows buried below a heap's top once they outnumber
        /// its `live` rows, so a heap holds O(frame) rows rather than O(partition)
        /// (a monotone order never brings them to the top). A sorted array is a
        /// valid heap; each row is dropped once, so the cost is amortized.
        fn compact(heap: *Heap, live: usize, gone: usize, max: bool) void {
            if (heap.count() <= 2 * live) return;
            var k: usize = 0;
            for (heap.items) |x| if (x.p >= gone) {
                heap.items[k] = x;
                k += 1;
            };
            heap.items.len = k;
            std.mem.sortUnstable(Item, heap.items, max, heapLess);
        }

        fn heapLess(max: bool, a: Item, b: Item) bool {
            return if (max) lt(b, a) else lt(a, b);
        }

        fn median(h: *const Halves) ?f64 {
            if (h.lo.items.len == 0) return null;
            const l = h.lo.items[0];
            if (h.nlo > h.nhi) return l.x;
            return (l.x + h.hi.items[0].x) / 2;
        }
    };

    /// COUNT(DISTINCT x): each value's count in the frame, the key copied on its first
    /// entry and dropped when its count reaches zero.
    const CountDistinct = struct {
        pub const In = Value;
        pub const State = struct { seen: std.HashMapUnmanaged([]const Value, u32, keyhash.MultiKeyCtx, std.hash_map.default_max_load_percentage) = .empty };
        pub fn reset(s: *State) void {
            s.seen.clearRetainingCapacity();
        }
        pub fn add(s: *State, x: Value, cx: Ctx) !void {
            var probe = [_]Value{x};
            const gop = try s.seen.getOrPut(cx.alloc, &probe);
            if (!gop.found_existing) {
                gop.key_ptr.* = try cx.alloc.dupe(Value, &probe);
                gop.value_ptr.* = 0;
            }
            gop.value_ptr.* += 1;
        }
        pub fn remove(s: *State, x: Value, _: Ctx) !void {
            var probe = [_]Value{x};
            const e = s.seen.getEntry(&probe) orelse return;
            e.value_ptr.* -= 1;
            if (e.value_ptr.* == 0) s.seen.removeByPtr(e.key_ptr);
        }
        pub fn finish(s: *const State, _: Ctx) !Value {
            return .{ .int = @intCast(s.seen.count()) };
        }
    };

    /// SUM over ints exactly; the frame total alone is range-checked, as an INT unless
    /// the sum is typed otherwise.
    const SumInt = struct {
        pub const In = i64;
        pub const State = aggFuncs.SumInt.State;
        pub const add = aggFuncs.SumInt.add;
        pub const remove = aggFuncs.SumInt.remove;
        pub fn finish(s: *const State, cx: Ctx) !Value {
            if (s.n > 0 and cx.out.kind != .int) return .{ .float = @floatFromInt(s.s) };
            return aggFuncs.SumInt.finish(s, cx);
        }
    };

    /// SUM or AVG (`agg`) of floats in two stacks, so a row that left leaves no
    /// rounding behind. The total adds zero, as the exact integer part once did, so -0
    /// reads 0.
    const FloatSum = struct {
        pub const In = f64;
        pub const State = struct { n: i64 = 0, avg: bool, t: TwoStack(f64, 0, addF) = .{} };
        pub fn init(_: Ctx, agg: ast.AggFunc) State {
            return .{ .avg = agg == .avg };
        }
        pub fn reset(s: *State) void {
            s.n = 0;
            s.t.reset();
        }
        pub fn add(s: *State, x: f64, cx: Ctx) !void {
            try s.t.push(cx.alloc, x);
            s.n += 1;
        }
        pub fn remove(s: *State, _: f64, cx: Ctx) !void {
            try s.t.pop(cx.alloc);
            s.n -= 1;
        }
        pub fn finish(s: *const State, _: Ctx) !Value {
            if (s.n == 0) return .null;
            const t = s.t.get() + @as(f64, 0);
            return .{ .float = if (s.avg) t / @as(f64, @floatFromInt(s.n)) else t };
        }
    };

    /// MIN (`want` .lt) or MAX by a monotonic deque of the values that can still win,
    /// in `aggregate/funcs.zig`'s `order`, a later equal value displacing an earlier one.
    fn Deque(comptime T: type, comptime want: std.math.Order) type {
        return struct {
            pub const In = T;
            const Item = struct { v: T, p: usize };
            pub const State = struct { q: std.ArrayList(Item) = .empty, head: usize = 0 };
            pub fn reset(s: *State) void {
                s.q.clearRetainingCapacity();
                s.head = 0;
            }
            pub fn add(s: *State, x: T, p: usize, cx: Ctx) !void {
                while (s.q.items.len > s.head) {
                    const o = aggFuncs.order(T, x, s.q.items[s.q.items.len - 1].v);
                    if (o != .eq and o != want) break;
                    s.q.items.len -= 1;
                }
                try s.q.append(cx.alloc, .{ .v = x, .p = p });
            }
            pub fn remove(s: *State, _: T, p: usize, _: Ctx) !void {
                if (s.head < s.q.items.len and s.q.items[s.head].p == p) s.head += 1;
            }
            pub fn finish(s: *const State, cx: Ctx) !Value {
                return if (s.head < s.q.items.len) aggFuncs.toValue(T, s.q.items[s.head].v, cx.out) else .null;
            }
        };
    }

    /// BIT_AND or BIT_OR, which no subtraction undoes, in two stacks.
    fn BitStack(comptime op: aggFuncs.BitOp) type {
        return struct {
            pub const In = i64;
            pub const State = struct { n: i64 = 0, t: if (op == .@"and") TwoStack(i64, -1, andI) else TwoStack(i64, 0, orI) = .{} };
            pub fn reset(s: *State) void {
                s.n = 0;
                s.t.reset();
            }
            pub fn add(s: *State, x: i64, cx: Ctx) !void {
                try s.t.push(cx.alloc, x);
                s.n += 1;
            }
            pub fn remove(s: *State, _: i64, cx: Ctx) !void {
                try s.t.pop(cx.alloc);
                s.n -= 1;
            }
            pub fn finish(s: *const State, _: Ctx) !Value {
                return if (s.n == 0) .null else .{ .int = s.t.get() };
            }
        };
    }

    /// A variance (`func`) from the frame's moments in two stacks; over ints (`exact`)
    /// from `IntSquares` while they hold, as the GROUP BY aggregate has it.
    fn Spread(comptime exact: bool) type {
        return struct {
            pub const In = if (exact) i64 else f64;
            pub const State = struct {
                func: ast.AggFunc,
                n: i64 = 0,
                sq: aggFuncs.IntSquares = .{},
                mom: TwoStack(Moments, .{}, Moments.merge) = .{},
            };
            pub fn init(_: Ctx, agg: ast.AggFunc) State {
                return .{ .func = agg };
            }
            pub fn reset(s: *State) void {
                s.mom.reset();
                s.* = .{ .func = s.func, .mom = s.mom };
            }
            pub fn add(s: *State, x: In, cx: Ctx) !void {
                if (exact) s.sq.add(x, false);
                try s.mom.push(cx.alloc, .{ .n = 1, .mean = if (exact) @floatFromInt(x) else x });
                s.n += 1;
            }
            pub fn remove(s: *State, x: In, cx: Ctx) !void {
                if (exact) s.sq.add(x, true);
                try s.mom.pop(cx.alloc);
                s.n -= 1;
            }
            pub fn finish(s: *const State, _: Ctx) !Value {
                const m2 = (if (exact) s.sq.m2(s.n) else null) orelse s.mom.get().m2;
                return aggFuncs.spreadOf(s.func, s.n, m2);
            }
        };
    }

    const Median = struct {
        pub const In = f64;
        pub const State = Halves;
        pub fn init(cx: Ctx, _: ast.AggFunc) State {
            return Halves.init(cx.alloc);
        }
        pub const reset = Halves.reset;
        pub fn add(s: *State, x: f64, p: usize, _: Ctx) !void {
            try s.add(.{ .x = x, .p = p });
        }
        pub fn remove(s: *State, x: f64, p: usize, _: Ctx) !void {
            try s.remove(.{ .x = x, .p = p });
        }
        pub fn finish(s: *const State, _: Ctx) !Value {
            return if (s.median()) |m| .{ .float = m } else .null;
        }
    };

    const Run = *const fn (?column.Column, Frames, Layout, Ctx, ast.AggFunc, *column.Builder) anyerror!void;

    /// The one loop every window aggregate runs, generated per aggregate `A` (a
    /// `State` with `add`, `remove` and `finish`, each optionally taking the row's
    /// position, and an optional `init` from the call's `agg`) and the column kind its
    /// argument reads as directly, or null for a kind read as a value (`valueAt`): per
    /// partition, rows enter at the frame's end edge and leave at its start edge, as
    /// the file header describes.
    fn Slider(comptime A: type, comptime kind: ?types.TypeKind) type {
        return struct {
            fn run(col: ?column.Column, fr: Frames, l: Layout, given: Ctx, agg: ast.AggFunc, bd: *column.Builder) anyerror!void {
                var cx = given;
                cx.ty = read.inType(A.In, cx.out);
                var s: A.State = if (@hasDecl(A, "init")) A.init(cx, agg) else .{};
                const n = fr.fs.len;
                var k: usize = 0;
                while (k < n) {
                    const pe = l.pend[k];
                    if (@hasDecl(A, "reset")) A.reset(&s) else s = .{};
                    var lo = k;
                    var hi = k;
                    while (k < pe) : (k += 1) {
                        while (hi < fr.fe[k]) : (hi += 1) {
                            if (try at(col, hi, cx)) |x| try step(A.add, &s, x, hi, cx);
                        }
                        while (lo < fr.fs[k]) : (lo += 1) {
                            if (lo < hi) if (try at(col, lo, cx)) |x| try step(A.remove, &s, x, lo, cx);
                        }
                        if (hi < lo) hi = lo;
                        try bd.append(try A.finish(&s, cx));
                    }
                }
            }

            inline fn at(col: ?column.Column, p: usize, cx: Ctx) !?A.In {
                if (A.In == void) {
                    const c = col orelse return {};
                    return read.at(void, .int, c, p, cx);
                }
                if (comptime kind) |k| return read.at(A.In, k, col.?, p, cx);
                return valueAt(A.In, col.?, p, cx);
            }

            inline fn step(comptime f: anytype, s: *A.State, x: A.In, p: usize, cx: Ctx) !void {
                if (@typeInfo(@TypeOf(f)).@"fn".params.len == 4) return f(s, x, p, cx);
                return f(s, x, cx);
            }
        };
    }

    /// Row `p` of a column whose kind `In` is not read from directly, as `read.at`
    /// has it: NULL when null, else its value cast to `In`. Out of line and one per
    /// `In`, as the cast outweighs the call.
    noinline fn valueAt(comptime In: type, c: column.Column, p: usize, cx: Ctx) anyerror!?In {
        if (!c.validity.get(p)) return null;
        return try read.fromValue(In, c.getValue(p), cx);
    }

    /// `A`'s loop over a column of kind `k`: one per lane of kinds `A.In` reads alike,
    /// and one for every kind read as a value.
    fn runAt(comptime A: type, comptime k: types.TypeKind) Run {
        return switch (comptime read.lane(A.In, k)) {
            .slow, .value => Slider(A, null).run,
            else => |l| Slider(A, comptime read.laneKind(A.In, l)).run,
        };
    }

    fn runOf(comptime A: type, kind: types.TypeKind) Run {
        return switch (kind) {
            inline else => |k| runAt(A, k),
        };
    }

    /// The sliding aggregate for `f` over an argument column of `kind`, chosen once.
    fn slider(f: Func, agg: ast.AggFunc, kind: types.TypeKind, out: types.Type) Run {
        if (f.arg == null) return runAt(aggFuncs.Count, .int);
        return switch (agg) {
            .count => if (f.distinct) runOf(CountDistinct, kind) else runOf(aggFuncs.Count, kind),
            .count_if => runOf(aggFuncs.CountIf, kind),
            .sum => if (out.kind == .decimal) runOf(aggFuncs.SumDecimal, kind) else if (kind == .int) runAt(SumInt, .int) else runOf(FloatSum, kind),
            .avg => if (kind == .int) runAt(aggFuncs.AvgInt, .int) else runOf(FloatSum, kind),
            inline .min, .max => |m| switch (kind) {
                inline else => |k| runAt(Deque(read.Phys(k) orelse Value, if (m == .min) .lt else .gt), k),
            },
            .median => runOf(Median, kind),
            .bool_and => runOf(aggFuncs.BoolFold(true), kind),
            .bool_or => runOf(aggFuncs.BoolFold(false), kind),
            .bit_and => runOf(BitStack(.@"and"), kind),
            .bit_or => runOf(BitStack(.@"or"), kind),
            .bit_xor => runOf(aggFuncs.Bits(.xor), kind),
            .var_samp, .var_pop, .stddev_samp, .stddev_pop => if (kind == .int) runAt(Spread(true), .int) else runOf(Spread(false), kind),
        };
    }

    pub fn next(self: *Window, arena: std.mem.Allocator) anyerror!?Batch {
        errdefer self.release();
        if (self.held) |h| return self.emit(arena, h);
        if (self.done) return null;
        self.done = true;
        if (self.top_k) |k| return self.topK(arena, k, self.gpa.?);
        const gpa = self.gpa orelse std.heap.page_allocator;
        const h = try Held.create(gpa);
        self.held = h;
        while (try self.child.next(h.pull())) |b| try h.add(b);
        if (h.rows == 0) {
            self.release();
            return null;
        }
        var work = std.heap.ArenaAllocator.init(gpa);
        defer work.deinit();
        self.fcols = try self.compute(work.allocator(), h);
        return self.emit(arena, h);
    }

    /// Frees the input and function values held for a consumer that stopped pulling.
    pub fn release(self: *Window) void {
        if (self.held) |h| h.destroy();
        self.held = null;
        self.fcols = &.{};
        self.done = true;
    }

    /// The next `batch_rows` input rows in window order with their function values,
    /// gathered into `arena`; what is held is freed with the last.
    fn emit(self: *Window, arena: std.mem.Allocator, h: *Held) !?Batch {
        const ncols = self.in_schema.fields.len;
        const lo = h.pos;
        const cols = try arena.alloc(column.Column, self.out_schema.fields.len);
        const n = try h.gather(arena, self.batch_rows, cols[0..ncols]);
        for (self.fcols, cols[ncols..]) |f, *o| o.* = try column.copy(arena, try column.slice(arena, f, lo, n));
        if (h.pos == h.refs.len) self.release();
        return Batch{ .schema = self.out_schema, .columns = cols, .len = n };
    }

    /// Orders `h`'s rows by partition and order keys and computes every function over
    /// them, into `h`'s arena; the sorted copies of only the key and argument columns
    /// and the frames are scratch in `work`.
    fn compute(self: *Window, work: std.mem.Allocator, h: *Held) ![]column.Column {
        const n = h.rows;
        const keys = try work.alloc(Sort.Key, self.part.len + self.ord.len);
        @memcpy(keys[0..self.part.len], self.part);
        @memcpy(keys[self.part.len..], self.ord);
        try h.order(keys, self.threads);

        const sc = try work.alloc(?column.Column, self.in_schema.fields.len);
        @memset(sc, null);
        for (keys) |k| {
            if (sc[k.idx] == null) sc[k.idx] = try h.sortedColumn(work, k.idx);
        }
        for (self.funcs) |f| if (f.arg) |ai| {
            if (sc[ai] == null) sc[ai] = try h.sortedColumn(work, ai);
        };
        const sorted_keys = try work.alloc(KeyArr, keys.len);
        for (keys, sorted_keys) |k, *x| x.* = try KeyArr.prepareThreads(work, sc[k.idx].?, k, self.threads);
        const l = try layout(work, n, sorted_keys[0..self.part.len], sorted_keys[self.part.len..], self.threads);

        var rk: ?RangeKeys = null;
        for (self.funcs) |f| {
            if (f.frame.range and (f.frame.start == .preceding or f.frame.start == .following or f.frame.end == .preceding or f.frame.end == .following)) {
                rk = try self.rangeKeys(work, sc[self.ord[0].idx].?);
                break;
            }
        }

        const ncols = self.in_schema.fields.len;
        const fcols = try h.allocator().alloc(column.Column, self.funcs.len);
        for (self.funcs, 0..) |f, fi| {
            const out_ty = self.out_schema.fields[ncols + fi].ty;
            var bd = try column.Builder.initCapacity(h.allocator(), out_ty, n);
            const arg: ?column.Column = if (f.arg) |ai| sc[ai].? else null;
            switch (f.func) {
                .win => |kind| switch (kind) {
                    .row_number, .rank, .dense_rank, .percent_rank => {
                        var rn: i64 = 0;
                        var rk_: i64 = 0;
                        var dr: i64 = 0;
                        for (0..n) |k| {
                            if (k == l.pstart[k]) {
                                rn = 1;
                                rk_ = 1;
                                dr = 1;
                            } else {
                                rn += 1;
                                if (k == l.gstart[k]) {
                                    rk_ = rn;
                                    dr += 1;
                                }
                            }
                            switch (kind) {
                                .row_number => try bd.appendInt(rn),
                                .rank => try bd.appendInt(rk_),
                                .dense_rank => try bd.appendInt(dr),
                                else => {
                                    const size = l.pend[k] - l.pstart[k];
                                    try bd.append(.{ .float = if (size <= 1) 0 else @as(f64, @floatFromInt(rk_ - 1)) / @as(f64, @floatFromInt(size - 1)) });
                                },
                            }
                        }
                    },
                    .cume_dist => for (0..n) |k| {
                        const size = l.pend[k] - l.pstart[k];
                        try bd.append(.{ .float = @as(f64, @floatFromInt(l.gend[k] - l.pstart[k])) / @as(f64, @floatFromInt(size)) });
                    },
                    .ntile => for (0..n) |k| {
                        const size: i64 = @intCast(l.pend[k] - l.pstart[k]);
                        const i: i64 = @intCast(k - l.pstart[k]);
                        const q = @divTrunc(size, f.offset);
                        const r = @mod(size, f.offset);
                        const big = r * (q + 1);
                        const bucket = if (i < big) @divTrunc(i, q + 1) + 1 else r + @divTrunc(i - big, q) + 1;
                        try bd.appendInt(bucket);
                    },
                    .lag, .lead, .first_value, .last_value, .nth_value => {
                        const vc = arg.?;
                        var cnt: []usize = &.{};
                        var nnpos: []usize = &.{};
                        if (f.ignore_nulls) {
                            cnt = try work.alloc(usize, n + 1);
                            var c: usize = 0;
                            for (0..n) |p| {
                                cnt[p] = c;
                                if (!vc.getValue(p).isNull()) c += 1;
                            }
                            cnt[n] = c;
                            nnpos = try work.alloc(usize, c);
                            c = 0;
                            for (0..n) |p| if (!vc.getValue(p).isNull()) {
                                nnpos[c] = p;
                                c += 1;
                            };
                        }
                        var fr: ?Frames = null;
                        if (kind != .lag and kind != .lead) fr = try frames(work, l, f.frame, rk);
                        for (0..n) |k| {
                            const ps = l.pstart[k];
                            const pe = l.pend[k];
                            const v: Value = switch (kind) {
                                .lag, .lead => blk: {
                                    if (f.ignore_nulls) {
                                        if (f.offset == 0) break :blk vc.getValue(k);
                                        const j: i64 = if (kind == .lag)
                                            @as(i64, @intCast(cnt[k])) - f.offset
                                        else
                                            @as(i64, @intCast(cnt[k + 1])) + f.offset - 1;
                                        if (j < @as(i64, @intCast(cnt[ps])) or j >= @as(i64, @intCast(cnt[pe]))) break :blk f.default;
                                        break :blk vc.getValue(nnpos[@intCast(j)]);
                                    }
                                    const dir: i64 = if (kind == .lag) -f.offset else f.offset;
                                    const target = @as(i64, @intCast(k)) + dir;
                                    if (target < @as(i64, @intCast(ps)) or target >= @as(i64, @intCast(pe))) break :blk f.default;
                                    break :blk vc.getValue(@intCast(target));
                                },
                                else => blk: {
                                    const s = fr.?.fs[k];
                                    const e = fr.?.fe[k];
                                    if (f.ignore_nulls) {
                                        const a = cnt[s];
                                        const b = cnt[e];
                                        break :blk switch (kind) {
                                            .first_value => if (a < b) vc.getValue(nnpos[a]) else .null,
                                            .last_value => if (a < b) vc.getValue(nnpos[b - 1]) else .null,
                                            else => if (a + @as(usize, @intCast(f.offset)) - 1 < b) vc.getValue(nnpos[a + @as(usize, @intCast(f.offset)) - 1]) else .null,
                                        };
                                    }
                                    break :blk switch (kind) {
                                        .first_value => if (s < e) vc.getValue(s) else .null,
                                        .last_value => if (s < e) vc.getValue(e - 1) else .null,
                                        else => if (s + @as(usize, @intCast(f.offset)) - 1 < e) vc.getValue(s + @as(usize, @intCast(f.offset)) - 1) else .null,
                                    };
                                },
                            };
                            try bd.append(v);
                        }
                    },
                },
                .agg => |agg| {
                    const fr = try frames(work, l, f.frame, rk);
                    const col: ?column.Column = if (arg) |x| try read.castText(work, agg, out_ty, x) else null;
                    const cx = Ctx{ .out = out_ty, .ty = if (col) |x| x.ty else out_ty, .alloc = work, .scratch = work };
                    slider(f, agg, if (col) |x| x.ty.kind else .int, out_ty)(col, fr, l, cx, agg, &bd) catch |e| {
                        if (e == error.IntOverflow and agg == .sum and out_ty.kind == .int) if (self.err) |ec| ec.set("{s}: in window SUM", .{errLabel(e)});
                        return e;
                    };
                },
            }
            fcols[fi] = try bd.finish();
        }
        return fcols;
    }
};

const testing = std.testing;
const Scan = @import("../op.zig").Scan;
const TestSource = @import("testing_util.zig").TestSource;

const BruteRow = struct { p: ?i64, o: ?i64, v: ?i64 };

const Dir = struct { desc: bool, nf: bool };

const dirs = [_]Dir{ .{ .desc = false, .nf = false }, .{ .desc = false, .nf = true }, .{ .desc = true, .nf = false }, .{ .desc = true, .nf = true } };

fn orderNulls(x: ?i64, y: ?i64, d: Dir) std.math.Order {
    if (x == null or y == null) {
        if (x == null and y == null) return .eq;
        return if ((x == null) == d.nf) .lt else .gt;
    }
    const o = std.math.order(x.?, y.?);
    return if (d.desc) o.invert() else o;
}

/// The rows in the order the operator sorts them: partition, then order key (DESC
/// flipping it, nulls placed as `dir` says), ties in input order.
fn bruteSort(a: std.mem.Allocator, rows: []const BruteRow, dir: Dir) ![]BruteRow {
    const out = try a.dupe(BruteRow, rows);
    std.sort.block(BruteRow, out, dir, struct {
        fn lt(d: Dir, l: BruteRow, r: BruteRow) bool {
            const po = orderNulls(l.p, r.p, .{ .desc = false, .nf = false });
            if (po != .eq) return po == .lt;
            return orderNulls(l.o, r.o, d) == .lt;
        }
    }.lt);
    return out;
}

fn sameP(a: ?i64, b: ?i64) bool {
    if (a == null or b == null) return a == null and b == null;
    return a.? == b.?;
}

fn boundN(b: Window.Bound) i64 {
    return switch (b) {
        .preceding, .following => |o| o.rows,
        else => 0,
    };
}

/// Whether sorted row `j` is in row `k`'s frame, from the frame's definition.
fn inFrame(rows: []const BruteRow, k: usize, j: usize, fr: Window.Frame, dir: Dir) bool {
    var yk: i64 = @intCast(k);
    var yj: i64 = @intCast(j);
    if (fr.range) {
        const cur = rows[k].o;
        const x = rows[j].o;
        if (cur == null) return x == null or (if (dir.nf) fr.end == .unbounded_following else fr.start == .unbounded_preceding);
        if (x == null) return if (dir.nf) fr.start == .unbounded_preceding else fr.end == .unbounded_following;
        yk = if (dir.desc) -cur.? else cur.?;
        yj = if (dir.desc) -x.? else x.?;
    }
    const lo_ok = switch (fr.start) {
        .unbounded_preceding => true,
        .preceding => yj >= yk - boundN(fr.start),
        .current_row => yj >= yk,
        .following => yj >= yk + boundN(fr.start),
        .unbounded_following => false,
    };
    const hi_ok = switch (fr.end) {
        .unbounded_preceding => false,
        .preceding => yj <= yk - boundN(fr.end),
        .current_row => yj <= yk,
        .following => yj <= yk + boundN(fr.end),
        .unbounded_following => true,
    };
    return lo_ok and hi_ok;
}

fn partitionOf(rows: []const BruteRow, k: usize) [2]usize {
    var ps = k;
    while (ps > 0 and sameP(rows[ps - 1].p, rows[k].p)) ps -= 1;
    var pe = k + 1;
    while (pe < rows.len and sameP(rows[pe].p, rows[k].p)) pe += 1;
    return .{ ps, pe };
}

fn bruteValue(a: std.mem.Allocator, rows: []const BruteRow, k: usize, f: Window.Func, dir: Dir) !Value {
    const ps, const pe = partitionOf(rows, k);
    var frame = std.array_list.Managed(?i64).init(a);
    for (ps..pe) |j| if (inFrame(rows, k, j, f.frame, dir)) try frame.append(rows[j].v);
    var nn = std.array_list.Managed(?i64).init(a);
    for (frame.items) |x| if (x != null) try nn.append(x);
    var xs = std.array_list.Managed(i64).init(a);
    for (nn.items) |x| try xs.append(x.?);
    const ys = xs.items;
    switch (f.func) {
        .win => |kind| {
            const n: usize = @intCast(f.offset);
            const src = if (f.ignore_nulls) nn.items else frame.items;
            const pick: ?i64 = switch (kind) {
                .first_value => if (src.len > 0) src[0] else null,
                .last_value => if (src.len > 0) src[src.len - 1] else null,
                .nth_value => if (src.len >= n) src[n - 1] else null,
                .lag, .lead => blk: {
                    if (n == 0) break :blk rows[k].v;
                    var seen: usize = 0;
                    var j: i64 = @intCast(k);
                    while (true) {
                        j += if (kind == .lag) -1 else 1;
                        if (j < @as(i64, @intCast(ps)) or j >= @as(i64, @intCast(pe))) break :blk null;
                        const y = rows[@intCast(j)].v;
                        if (f.ignore_nulls and y == null) continue;
                        seen += 1;
                        if (seen == n) break :blk y;
                    }
                },
                else => unreachable,
            };
            return if (pick) |y| .{ .int = y } else .null;
        },
        .agg => |agg| return switch (agg) {
            .count => if (f.arg == null) .{ .int = @intCast(frame.items.len) } else if (f.distinct) blk: {
                var set = std.AutoHashMap(i64, void).init(a);
                for (ys) |y| try set.put(y, {});
                break :blk .{ .int = set.count() };
            } else .{ .int = @intCast(ys.len) },
            .sum => if (ys.len == 0) .null else blk: {
                var s: i64 = 0;
                for (ys) |y| s += y;
                break :blk .{ .int = s };
            },
            .avg => if (ys.len == 0) .null else blk: {
                var s: f64 = 0;
                for (ys) |y| s += @floatFromInt(y);
                break :blk .{ .float = s / @as(f64, @floatFromInt(ys.len)) };
            },
            .min, .max => if (ys.len == 0) .null else blk: {
                var m = ys[0];
                for (ys) |y| m = if (agg == .min) @min(m, y) else @max(m, y);
                break :blk .{ .int = m };
            },
            .median => if (ys.len == 0) .null else blk: {
                const zs = try a.dupe(i64, ys);
                std.mem.sort(i64, zs, {}, std.sort.asc(i64));
                const mid = zs.len / 2;
                if (zs.len % 2 == 1) break :blk .{ .float = @floatFromInt(zs[mid]) };
                break :blk .{ .float = @as(f64, @floatFromInt(zs[mid - 1] + zs[mid])) / 2 };
            },
            .var_samp, .var_pop, .stddev_samp, .stddev_pop => blk: {
                const samp = agg == .var_samp or agg == .stddev_samp;
                if (ys.len < @as(usize, if (samp) 2 else 1)) break :blk .null;
                var mean: f64 = 0;
                for (ys) |y| mean += @floatFromInt(y);
                mean /= @floatFromInt(ys.len);
                var ss: f64 = 0;
                for (ys) |y| ss += (@as(f64, @floatFromInt(y)) - mean) * (@as(f64, @floatFromInt(y)) - mean);
                const vr = ss / @as(f64, @floatFromInt(if (samp) ys.len - 1 else ys.len));
                break :blk .{ .float = if (agg == .stddev_samp or agg == .stddev_pop) @sqrt(vr) else vr };
            },
            .bit_and, .bit_or, .bit_xor => if (ys.len == 0) .null else blk: {
                var m = ys[0];
                for (ys[1..]) |y| m = switch (agg) {
                    .bit_and => m & y,
                    .bit_or => m | y,
                    else => m ^ y,
                };
                break :blk .{ .int = m };
            },
            else => unreachable,
        },
    }
}

fn windowOutType(f: Window.Func) types.Type {
    return switch (f.func) {
        .win => |k| switch (k) {
            .row_number, .rank, .dense_rank, .ntile => types.Type.init(.int),
            .percent_rank, .cume_dist => types.Type.init(.float),
            else => types.Type.init(.int).asNullable(),
        },
        .agg => |agg| switch (agg) {
            .count, .count_if => types.Type.init(.int),
            .avg, .median, .var_samp, .var_pop, .stddev_samp, .stddev_pop => types.Type.init(.float).asNullable(),
            else => types.Type.init(.int).asNullable(),
        },
    };
}

/// The operator's output for `funcs` over `rows` (columns p, o, v), in its order.
fn runWindow(a: std.mem.Allocator, rows: []const BruteRow, funcs: []const Window.Func, dir: Dir, with_order: bool) ![]Batch {
    const in_schema = try a.create(types.Schema);
    in_schema.* = .{ .fields = &.{
        .{ .name = "p", .ty = types.Type.init(.int).asNullable() },
        .{ .name = "o", .ty = types.Type.init(.int).asNullable() },
        .{ .name = "v", .ty = types.Type.init(.int).asNullable() },
    } };
    const fields = try a.alloc(types.Schema.Field, 3 + funcs.len);
    @memcpy(fields[0..3], in_schema.fields);
    for (funcs, fields[3..]) |f, *fd| fd.* = .{ .name = "w", .ty = windowOutType(f) };
    const out_schema = try a.create(types.Schema);
    out_schema.* = .{ .fields = fields };
    var batches = std.array_list.Managed(Batch).init(a);
    if (rows.len > 0) {
        const ps = try a.alloc(?i64, rows.len);
        const os = try a.alloc(?i64, rows.len);
        const vs = try a.alloc(?i64, rows.len);
        for (rows, ps, os, vs) |r, *p, *o, *v| {
            p.* = r.p;
            o.* = r.o;
            v.* = r.v;
        }
        const cols = try a.alloc(column.Column, 3);
        cols[0] = try column.intColumn(a, ps);
        cols[1] = try column.intColumn(a, os);
        cols[2] = try column.intColumn(a, vs);
        try batches.append(.{ .schema = in_schema, .columns = cols, .len = rows.len });
    }
    const ts = try a.create(TestSource);
    ts.* = .{ .schema_ = in_schema.*, .batches = batches.items };
    const scan = try a.create(Scan);
    scan.* = .{ .src = ts.src() };
    const w = try a.create(Window);
    w.* = .{
        .child = .{ .scan = scan },
        .in_schema = in_schema,
        .out_schema = out_schema,
        .part = &[_]Sort.Key{.{ .idx = 0, .desc = false, .nulls_first = false }},
        .ord = if (with_order) try a.dupe(Sort.Key, &[_]Sort.Key{.{ .idx = 1, .desc = dir.desc, .nulls_first = dir.nf }}) else &.{},
        .funcs = funcs,
        .gpa = testing.allocator,
        .batch_rows = 8,
    };
    var out = std.array_list.Managed(Batch).init(a);
    while (try w.next(a)) |b| {
        try testing.expect(b.len <= 8);
        try out.append(b);
    }
    try testing.expect(w.held == null);
    return out.toOwnedSlice();
}

fn expectSameValue(want: Value, got: Value) !void {
    if (want.isNull() or got.isNull()) return testing.expect(want.isNull() and got.isNull());
    switch (want) {
        .float => |x| try testing.expectApproxEqAbs(x, got.float, 1e-9 * @max(1, @abs(x))),
        .int => |x| try testing.expectEqual(x, got.int),
        else => unreachable,
    }
}

fn randomRows(a: std.mem.Allocator, rnd: std.Random, n: usize) ![]BruteRow {
    const rows = try a.alloc(BruteRow, n);
    for (rows) |*r| r.* = .{
        .p = if (rnd.uintLessThan(u8, 10) == 0) null else rnd.intRangeAtMost(i64, 0, 3),
        .o = if (rnd.uintLessThan(u8, 8) == 0) null else rnd.intRangeAtMost(i64, -3, 6),
        .v = if (rnd.uintLessThan(u8, 5) == 0) null else rnd.intRangeAtMost(i64, -20, 20),
    };
    return rows;
}

fn boundRank(b: Window.Bound) u8 {
    return switch (b) {
        .unbounded_preceding => 0,
        .preceding => 1,
        .current_row => 2,
        .following => 3,
        .unbounded_following => 4,
    };
}

test "window frames: every bound pair, ROWS and RANGE, ties, nulls and edges match a brute-force reference" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var prng = std.Random.DefaultPrng.init(0x5eed);
    const rnd = prng.random();
    const offs = [_]i64{ 0, 1, 2, 5 };
    const aggs = [_]ast.AggFunc{ .sum, .count, .avg, .min, .max, .median, .var_samp, .stddev_pop, .bit_and, .bit_or, .bit_xor };
    var bounds = std.array_list.Managed(Window.Bound).init(a);
    try bounds.append(.unbounded_preceding);
    for (offs) |n| try bounds.append(.{ .preceding = .{ .rows = n, .int = n } });
    try bounds.append(.current_row);
    for (offs) |n| try bounds.append(.{ .following = .{ .rows = n, .int = n } });
    try bounds.append(.unbounded_following);
    for ([_]usize{ 0, 1, 2, 7, 40 }) |n| for (dirs) |dir| for ([_]bool{ false, true }) |range| {
        const rows = try randomRows(a, rnd, n);
        const sorted = try bruteSort(a, rows, dir);
        var funcs = std.array_list.Managed(Window.Func).init(a);
        for (bounds.items) |s| for (bounds.items) |e| {
            if (s == .unbounded_following or e == .unbounded_preceding or boundRank(s) > boundRank(e)) continue;
            const fr = Window.Frame{ .range = range, .start = s, .end = e };
            for (aggs) |agg| try funcs.append(.{ .func = .{ .agg = agg }, .arg = 2, .frame = fr });
            try funcs.append(.{ .func = .{ .agg = .count }, .frame = fr });
            try funcs.append(.{ .func = .{ .agg = .count }, .arg = 2, .distinct = true, .frame = fr });
            for ([_]ast.WinKind{ .first_value, .last_value, .nth_value }) |vk| for ([_]bool{ false, true }) |ign|
                try funcs.append(.{ .func = .{ .win = vk }, .arg = 2, .offset = 2, .ignore_nulls = ign, .frame = fr });
        };
        const out = try runWindow(a, rows, funcs.items, dir, true);
        var at: usize = 0;
        for (out) |b| for (0..b.len) |r| {
            for (funcs.items, 0..) |f, fi| try expectSameValue(try bruteValue(a, sorted, at, f, dir), b.columns[3 + fi].getValue(r));
            at += 1;
        };
        try testing.expectEqual(n, at);
    };
}

test "window offsets and ranking: LAG/LEAD (IGNORE NULLS), NTILE, PERCENT_RANK and CUME_DIST match a reference" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var prng = std.Random.DefaultPrng.init(0xfeed);
    const rnd = prng.random();
    for ([_]usize{ 0, 1, 3, 25, 60 }) |n| for (dirs) |dir| {
        const rows = try randomRows(a, rnd, n);
        const sorted = try bruteSort(a, rows, dir);
        var funcs = std.array_list.Managed(Window.Func).init(a);
        for ([_]i64{ 0, 1, 2, 4 }) |off| for ([_]bool{ false, true }) |ign| {
            try funcs.append(.{ .func = .{ .win = .lag }, .arg = 2, .offset = off, .ignore_nulls = ign });
            try funcs.append(.{ .func = .{ .win = .lead }, .arg = 2, .offset = off, .ignore_nulls = ign });
        };
        for ([_]i64{ 1, 2, 3, 7 }) |b| try funcs.append(.{ .func = .{ .win = .ntile }, .offset = b });
        try funcs.append(.{ .func = .{ .win = .rank } });
        try funcs.append(.{ .func = .{ .win = .percent_rank } });
        try funcs.append(.{ .func = .{ .win = .cume_dist } });
        const out = try runWindow(a, rows, funcs.items, dir, true);
        var at: usize = 0;
        for (out) |b| for (0..b.len) |r| {
            const ps, const pe = partitionOf(sorted, at);
            var rk: usize = ps;
            while (rk < at and !sameP(sorted[rk].o, sorted[at].o)) rk += 1;
            var ge = at + 1;
            while (ge < pe and sameP(sorted[ge].o, sorted[at].o)) ge += 1;
            const size = pe - ps;
            for (funcs.items, 0..) |f, fi| {
                const want: Value = switch (f.func.win) {
                    .lag, .lead => try bruteValue(a, sorted, at, f, dir),
                    .ntile => blk: {
                        const bk: usize = @intCast(f.offset);
                        const q = size / bk;
                        const rem = size % bk;
                        const i = at - ps;
                        const big = rem * (q + 1);
                        break :blk .{ .int = @intCast(if (i < big) i / (q + 1) + 1 else rem + (i - big) / q + 1) };
                    },
                    .rank => .{ .int = @intCast(rk - ps + 1) },
                    .percent_rank => .{ .float = if (size <= 1) 0 else @as(f64, @floatFromInt(rk - ps)) / @as(f64, @floatFromInt(size - 1)) },
                    .cume_dist => .{ .float = @as(f64, @floatFromInt(ge - ps)) / @as(f64, @floatFromInt(size)) },
                    else => unreachable,
                };
                try expectSameValue(want, b.columns[3 + fi].getValue(r));
            }
            at += 1;
        };
        try testing.expectEqual(n, at);
    };
}

test "window: output streams in bounded batches gathered from one held copy, freed early or at the end" {
    const util = @import("testing_util.zig");
    const funcs = [_]Window.Func{ .{ .func = .{ .win = .row_number } }, .{ .func = .{ .win = .lag }, .arg = 1 } };
    const fields = [_]types.Schema.Field{
        util.join_right_schema.fields[0],
        util.join_right_schema.fields[1],
        .{ .name = "rn", .ty = types.Type.init(.int) },
        .{ .name = "lg", .ty = types.Type.init(.string).asNullable() },
    };
    const out_schema = types.Schema{ .fields = &fields };
    const ord = [_]Sort.Key{.{ .idx = 0, .desc = false, .nulls_first = false }};
    for ([_]usize{ 3, std.math.maxInt(usize) }) |pulls| {
        var src = util.PullSource{ .schema_ = util.join_right_schema, .count = 64, .rows = 256 };
        var scan = Scan{ .src = src.src() };
        var w = Window{ .child = .{ .scan = &scan }, .in_schema = &util.join_right_schema, .out_schema = &out_schema, .part = &.{}, .ord = &ord, .funcs = &funcs, .gpa = testing.allocator, .batch_rows = 1000 };
        const top: Op = .{ .window = &w };
        errdefer top.release();
        var pull = std.heap.ArenaAllocator.init(testing.allocator);
        defer pull.deinit();
        var want: i64 = 1;
        var n: usize = 0;
        while (n < pulls) : (n += 1) {
            _ = pull.reset(.retain_capacity);
            const b = (try top.next(pull.allocator())) orelse break;
            try testing.expect(b.len <= 1000);
            for (0..b.len) |r| {
                try testing.expectEqual(want, b.columns[0].getValue(r).int);
                try testing.expectEqual(want, b.columns[2].getValue(r).int);
                var buf: [48]u8 = undefined;
                const prev = b.columns[3].getValue(r);
                if (want == 1) try testing.expect(prev.isNull()) else try testing.expectEqualStrings(try std.fmt.bufPrint(&buf, "{d:0>48}", .{@as(u64, @intCast(want - 1))}), prev.string);
                want += 1;
            }
            try testing.expect(pull.queryCapacity() < 256 * 1024);
        }
        if (pulls > 3) try testing.expectEqual(@as(i64, 64 * 256 + 1), want);
        top.release();
        try testing.expect(w.held == null);
    }
}

test "window without ORDER BY: the default frame is the whole partition, a ROWS frame counts in input order" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const rows = [_]BruteRow{
        .{ .p = 1, .o = null, .v = 4 },
        .{ .p = 0, .o = null, .v = 1 },
        .{ .p = 1, .o = null, .v = null },
        .{ .p = 0, .o = null, .v = 2 },
        .{ .p = 1, .o = null, .v = 6 },
    };
    const funcs = [_]Window.Func{
        .{ .func = .{ .agg = .sum }, .arg = 2 },
        .{ .func = .{ .agg = .sum }, .arg = 2, .frame = .{ .range = false, .start = .{ .preceding = .{ .rows = 1 } }, .end = .current_row } },
        .{ .func = .{ .win = .last_value }, .arg = 2 },
    };
    const out = try runWindow(a, &rows, &funcs, dirs[0], false);
    const want = [_][3]?i64{ .{ 3, 1, 2 }, .{ 3, 3, 2 }, .{ 10, 4, 6 }, .{ 10, 4, 6 }, .{ 10, 6, 6 } };
    var at: usize = 0;
    for (out) |b| for (0..b.len) |r| {
        for (want[at], 0..) |w, c| try expectSameValue(if (w) |x| .{ .int = x } else .null, b.columns[3 + c].getValue(r));
        at += 1;
    };
    try testing.expectEqual(@as(usize, 5), at);
}

test "window MEDIAN heaps stay within the frame and match a brute-force median, monotone or random" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const H = Window.Halves;
    var prng = std.Random.DefaultPrng.init(0x3ed1a);
    const rnd = prng.random();
    const n = 20_000;
    const xs = try a.alloc(f64, n);
    const buf = try a.alloc(H.Item, 64);
    for ([_]u8{ 0, 1, 2 }) |shape| for ([_]usize{ 1, 2, 7, 40 }) |w| {
        for (xs, 0..) |*x, i| x.* = switch (shape) {
            0 => @floatFromInt(i),
            1 => -@as(f64, @floatFromInt(i)),
            else => if (rnd.uintLessThan(u8, 20) == 0) std.math.nan(f64) else @floatFromInt(rnd.intRangeAtMost(i32, -50, 50)),
        };
        var h = H.init(a);
        for (0..n) |i| {
            try h.add(.{ .x = xs[i], .p = i });
            if (i >= w) try h.remove(.{ .x = xs[i - w], .p = i - w });
            const lo = i + 1 -| w;
            const frame = buf[0 .. i + 1 - lo];
            for (frame, lo..) |*f, p| f.* = .{ .x = xs[p], .p = p };
            std.mem.sort(H.Item, frame, {}, struct {
                fn less(_: void, l: H.Item, r: H.Item) bool {
                    return H.lt(l, r);
                }
            }.less);
            const mid = frame.len / 2;
            const want = if (frame.len % 2 == 1) frame[mid].x else (frame[mid - 1].x + frame[mid].x) / 2;
            const got = h.median().?;
            try testing.expect((std.math.isNan(want) and std.math.isNan(got)) or want == got);
            try testing.expect(h.lo.count() + h.hi.count() <= 2 * (h.nlo + h.nhi) + 2);
        }
    };
}

test "layout across chunks and threads marks the same partitions and peer groups as a serial walk" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var prng = std.Random.DefaultPrng.init(0x1a7);
    const rnd = prng.random();
    const n = 300_000;
    var pb = column.Builder.init(a, types.Type.init(.int).asNullable());
    var ob = column.Builder.init(a, types.Type.init(.int).asNullable());
    var p: i64 = 0;
    var o: i64 = 0;
    for (0..n) |_| {
        if (rnd.uintLessThan(u32, 20_000) == 0) {
            p += 1;
            o = 0;
        }
        if (rnd.uintLessThan(u8, 3) == 0) o += 1;
        try pb.append(if (@mod(p, 7) == 3) .null else Value{ .int = p });
        try ob.append(Value{ .int = o });
    }
    const key = Sort.Key{ .idx = 0, .desc = false, .nulls_first = false };
    const parts = [_]KeyArr{try KeyArr.prepare(a, try pb.finish(), key)};
    const ords = [_]KeyArr{try KeyArr.prepare(a, try ob.finish(), key)};
    const l = try Window.layout(a, n, &parts, &ords, 4);
    var ps: usize = 0;
    var gs: usize = 0;
    for (0..n) |k| {
        if (k > 0 and !Window.sameOn(&parts, k - 1, k)) ps = k;
        if (k == ps or !Window.sameOn(&ords, k - 1, k)) gs = k;
        try testing.expectEqual(ps, l.pstart[k]);
        try testing.expectEqual(gs, l.gstart[k]);
        try testing.expect(l.pend[k] > k and l.gend[k] > k and l.gend[k] <= l.pend[k]);
        try testing.expect(l.pend[k] == n or !Window.sameOn(&parts, l.pend[k] - 1, l.pend[k]));
        try testing.expect(l.gend[k] == n or l.gend[k] == l.pend[k] or !Window.sameOn(&ords, l.gend[k] - 1, l.gend[k]));
        if (l.pend[k] - 1 > k) try testing.expectEqual(l.pstart[k], l.pstart[l.pend[k] - 1]);
        if (l.gend[k] - 1 > k) try testing.expectEqual(l.gstart[k], l.gstart[l.gend[k] - 1]);
    }
}
