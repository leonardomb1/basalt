//! `ORDER BY`: a full sort by key words, radix-sorted, parallel past a size, with
//! ties broken by the original row order so the sort is stable.

const Batch = @import("../batch.zig").Batch;
const Op = @import("../op.zig").Op;
const Stats = @import("../op.zig").Stats;
const Value = @import("../value.zig").Value;
const column = @import("../column.zig");
const eval = @import("../eval.zig");
const materializeAll = @import("../op.zig").materializeAll;
const std = @import("std");
const types = @import("../../lang/types.zig");

pub const Sort = struct {
    stats: Stats = .{},
    child: Op,
    in_schema: *const types.Schema,
    keys: []const Key,
    done: bool = false,
    threads: usize = 1,

    pub const Key = struct { idx: usize, desc: bool };

    pub fn next(self: *Sort, arena: std.mem.Allocator) anyerror!?Batch {
        if (self.done) return null;
        self.done = true;
        const all = (try materializeAll(arena, self.child, self.in_schema)) orelse return null;

        const idx = try arena.alloc(usize, all.len);
        for (idx, 0..) |*x, i| x.* = i;
        const arrs = try arena.alloc(KeyArr, self.keys.len);
        for (self.keys, arrs) |k, *a| a.* = try KeyArr.prepare(arena, all.columns[k.idx], k.desc);
        try sortIdxThreads(arena, idx, arrs, self.threads);

        const outcols = try arena.alloc(column.Column, all.columns.len);
        for (all.columns, 0..) |*col, ci| outcols[ci] = try column.permute(arena, col.*, idx);
        return Batch{ .schema = all.schema, .columns = outcols, .len = all.len };
    }
};

pub const KeyArr = struct {
    desc: bool,
    valid: column.Bitmap,
    data: Data,

    const Data = union(enum) {
        ints: []i64,
        floats: []f64,
        decs: []i128,
        strs: [][]const u8,
        boxed: column.Column,
    };

    pub fn prepare(arena: std.mem.Allocator, col: column.Column, desc: bool) !KeyArr {
        const n = col.len;
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
                var scale: u8 = 0;
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
        return .{ .desc = desc, .valid = col.validity, .data = data };
    }

    pub fn order(self: KeyArr, a: usize, c: usize) std.math.Order {
        const an = !self.valid.get(a);
        const bn = !self.valid.get(c);
        if (an or bn) {
            if (an and bn) return .eq;
            return if (an) .gt else .lt;
        }
        const ord: std.math.Order = switch (self.data) {
            .ints => |v| std.math.order(v[a], v[c]),
            .floats => |v| eval.orderF64(v[a], v[c]),
            .decs => |v| std.math.order(v[a], v[c]),
            .strs => |v| std.mem.order(u8, v[a], v[c]),
            .boxed => |col| return keyOrder(col.getValue(a), col.getValue(c), self.desc),
        };
        if (ord == .eq) return .eq;
        return if (self.desc) (if (ord == .lt) std.math.Order.gt else std.math.Order.lt) else ord;
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
    try sortRows(arena, idx, arrs, plans, exact, true);
}

/// A stable LSD radix sort of `idx`. With `all` (every row, once) each word is
/// encoded once in row order and gathered; encoding via the permutation read
/// strings at random.
fn sortRows(arena: std.mem.Allocator, idx: []usize, arrs: []const KeyArr, plans: []const KeyPlan, exact: bool, all: bool) !void {
    const n = idx.len;
    const pairs = try arena.alloc(RadixPair, n);
    const tmp = try arena.alloc(RadixPair, n);
    const counts = try arena.alloc(u32, 1 << 11);
    const words: []u64 = if (all) try arena.alloc(u64, n) else &.{};
    var j = arrs.len;
    while (j > 0) {
        j -= 1;
        const k = arrs[j];
        const p = plans[j];
        var w = p.words;
        while (w > 0) {
            w -= 1;
            if (all) {
                for (words, 0..) |*x, i| x.* = encWord(k, p, w, i);
                for (pairs, idx) |*pr, i| pr.* = .{ .k = words[i], .i = @intCast(i) };
            } else {
                for (pairs, idx) |*pr, i| pr.* = .{ .k = encWord(k, p, w, i), .i = @intCast(i) };
            }
            radixSortPairs(pairs, tmp, counts);
            for (pairs, idx) |pr, *x| x.* = pr.i;
        }
        if (p.nulls) {
            for (pairs, idx) |*pr, i| pr.* = .{ .k = @intFromBool(!k.valid.get(i)), .i = @intCast(i) };
            radixSortPairs(pairs, tmp, counts);
            for (pairs, idx) |pr, *x| x.* = pr.i;
        }
    }
    if (!exact) fixTies(idx, arrs, plans);
}

/// Deal rows, in input order, into ranges of the first key split where a histogram
/// puts about one thread's share (nulls in a range of their own), and sort each on
/// its own thread. False when not worth it or the first key cannot split.
fn sortIdxParallel(arena: std.mem.Allocator, idx: []usize, arrs: []const KeyArr, plans: []const KeyPlan, exact: bool, threads: usize) !bool {
    const n = idx.len;
    if (threads < 2 or n < 1 << 17) return false;
    const k0 = arrs[0];
    const p0 = plans[0];
    const isnull = struct {
        fn f(k: KeyArr, p: KeyPlan, row: usize) bool {
            return p.nulls and !k.valid.get(row);
        }
    }.f;
    var lo: u64 = std.math.maxInt(u64);
    var hi: u64 = 0;
    for (0..n) |row| {
        if (isnull(k0, p0, row)) continue;
        const w = encWord(k0, p0, 0, row);
        lo = @min(lo, w);
        hi = @max(hi, w);
    }
    if (lo >= hi) return false;
    const bits: u32 = 64 - @clz(hi - lo);
    const shift: u6 = @intCast(if (bits > 11) bits - 11 else 0);
    var hist = [_]u32{0} ** (1 << 11);
    var nonnull: usize = 0;
    for (0..n) |row| {
        if (isnull(k0, p0, row)) continue;
        hist[@intCast((encWord(k0, p0, 0, row) - lo) >> shift)] += 1;
        nonnull += 1;
    }
    const nthreads = @min(threads, 64);
    var of_digit: [1 << 11]u8 = undefined;
    var cur: usize = 0;
    var acc: usize = 0;
    for (hist, &of_digit) |h, *d| {
        d.* = @intCast(cur);
        acc += h;
        if (cur + 1 < nthreads and acc * nthreads >= nonnull * (cur + 1)) cur += 1;
    }
    const null_range = cur + 1;
    const nranges = null_range + 1;
    var size = [_]usize{0} ** 66;
    var range_of = try arena.alloc(u8, n);
    for (0..n) |row| {
        const r: u8 = if (isnull(k0, p0, row)) @intCast(null_range) else of_digit[@intCast((encWord(k0, p0, 0, row) - lo) >> shift)];
        range_of[row] = r;
        size[r] += 1;
    }
    var start = [_]usize{0} ** 66;
    for (1..nranges) |r| start[r] = start[r - 1] + size[r - 1];
    var fill = start;
    for (0..n) |row| {
        const r = range_of[row];
        idx[fill[r]] = row;
        fill[r] += 1;
    }

    const Job = struct {
        rows: []usize,
        arrs: []const KeyArr,
        plans: []const KeyPlan,
        exact: bool,
        err: ?anyerror = null,
        fn run(job: *@This()) void {
            var ar = std.heap.ArenaAllocator.init(std.heap.page_allocator);
            defer ar.deinit();
            sortRows(ar.allocator(), job.rows, job.arrs, job.plans, job.exact, false) catch |e| {
                job.err = e;
            };
        }
    };
    var jobs: [66]Job = undefined;
    var handles: [66]?std.Thread = @splat(null);
    for (0..nranges) |r| {
        jobs[r] = .{ .rows = idx[start[r]..][0..size[r]], .arrs = arrs, .plans = plans, .exact = exact };
        if (size[r] < 2) continue;
        handles[r] = std.Thread.spawn(.{}, Job.run, .{&jobs[r]}) catch blk: {
            jobs[r].run();
            break :blk null;
        };
    }
    for (handles[0..nranges]) |h| if (h) |t| t.join();
    for (jobs[0..nranges]) |jb| if (jb.err) |e| return e;
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

const RadixPair = struct { k: u64, i: u32 };

/// Stable LSD radix sort on `k` less its minimum, in 11-bit digits over only the
/// digits the range needs. 2048 buckets keep the scatter in cache; 65536 did not.
fn radixSortPairs(pairs: []RadixPair, tmp: []RadixPair, counts: []u32) void {
    if (pairs.len < 2) return;
    var lo: u64 = std.math.maxInt(u64);
    var hi: u64 = 0;
    for (pairs) |p| {
        lo = @min(lo, p.k);
        hi = @max(hi, p.k);
    }
    if (lo == hi) return;
    const bits: u32 = 64 - @clz(hi - lo);
    const digit = 11;
    const nb = 1 << digit;
    const cs = counts[0..nb];
    var src = pairs;
    var dst = tmp;
    var shift: u32 = 0;
    while (shift < bits) : (shift += digit) {
        const sh: u6 = @intCast(shift);
        @memset(cs, 0);
        for (src) |p| cs[@intCast(((p.k - lo) >> sh) & (nb - 1))] += 1;
        var sum: u32 = 0;
        for (cs) |*c| {
            const v = c.*;
            c.* = sum;
            sum += v;
        }
        for (src) |p| {
            const d: usize = @intCast(((p.k - lo) >> sh) & (nb - 1));
            dst[cs[d]] = p;
            cs[d] += 1;
        }
        const t = src;
        src = dst;
        dst = t;
    }
    if (src.ptr != pairs.ptr) @memcpy(pairs, src);
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

/// Order of two sort-key values: nulls always last, `desc` flips non-null order.
/// Shared by `SortCtx.lessThan` and Top-N.
pub fn keyOrder(va: Value, vb: Value, desc: bool) std.math.Order {
    const an = va.isNull();
    const bn = vb.isNull();
    if (an or bn) {
        if (an and bn) return .eq;
        return if (an) .gt else .lt;
    }
    const ord = eval.compareValues(va, vb) orelse return .eq;
    if (ord == .eq) return .eq;
    return if (desc) (if (ord == .lt) std.math.Order.gt else std.math.Order.lt) else ord;
}
