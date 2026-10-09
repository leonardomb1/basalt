//! Window functions over sorted partitions: ranking, distribution, offsets, value
//! and aggregate functions, each over its own frame. A breaker: a partition is
//! complete before any of its rows is numbered.
//!
//! One sort over partition ++ order keys, then per function one pass. A frame is
//! resolved to `[start, end)` in sorted positions for every row; both edges only move
//! forward as the row does, whatever the bounds, so ROWS bounds are arithmetic,
//! RANGE `CURRENT ROW` is the peer group, and a RANGE offset walks two pointers over
//! the single order key (made ascending, a DESC key negated). Order-key nulls sort
//! last and are one another's peers: a null row's offset bound is the null group,
//! and a non-null row's offset never reaches it (UNBOUNDED FOLLOWING does).
//!
//! An aggregate slides over the frames: rows enter at the end edge and leave at the
//! start edge, with a monotonic deque for MIN/MAX, Welford's update and its inverse
//! for the variances, and per-value counts for COUNT(DISTINCT). MEDIAN, BIT_AND and
//! BIT_OR cannot take a row back: a frame that only grows adds to them, any other is
//! recomputed. A float sum over a sliding frame can drift by rounding. IGNORE NULLS
//! reads through the positions of the non-null values and their prefix counts.
//!
//! The default frame (`Frame{}`) is RANGE from the partition's start to the current
//! row's peers. A RANGE offset (`Off.int` / `Off.float`) is in the order key's units
//! as `RangeConv` reads it: a float key as itself, any other as an integer — an int
//! times `mul` (10^scale of the offsets), a date's days times `mul` (microseconds a
//! day), a timestamp's or time's microseconds, a decimal rescaled to `scale`.

const Batch = @import("../batch.zig").Batch;
const ErrCtx = @import("../op.zig").ErrCtx;
const KeyArr = @import("sort.zig").KeyArr;
const Op = @import("../op.zig").Op;
const Sort = @import("sort.zig").Sort;
const Stats = @import("../op.zig").Stats;
const Decimal = @import("../value.zig").Decimal;
const Value = @import("../value.zig").Value;
const ast = @import("../../lang/ast.zig");
const column = @import("../column.zig");
const dupeRowGpa = @import("topn.zig").dupeRowGpa;
const errLabel = @import("../op.zig").errLabel;
const rescaleTo = @import("../eval.zig").rescaleTo;
const freeRowGpa = @import("topn.zig").freeRowGpa;
const keyOrder = @import("sort.zig").keyOrder;
const keyhash = @import("../keyhash.zig");
const lessV = @import("aggregate.zig").lessV;
const materializeAll = @import("../op.zig").materializeAll;
const selectNth = @import("aggregate.zig").selectNth;
const sortIdxThreads = @import("sort.zig").sortIdxThreads;
const std = @import("std");
const types = @import("../../lang/types.zig");

/// Adds (or, leaving a frame, takes away) a value from a DECIMAL sum, exactly, at
/// the sum's scale, as the SUM aggregate does; a float total would read 0.1 + 0.2 as
/// 0.30000000000000004.
fn decStep(acc: *i128, v: Value, scale: u8, sub: bool) !void {
    const d: Decimal = switch (v) {
        .decimal => |x| x,
        .int => |x| .{ .unscaled = x, .scale = 0 },
        else => return error.TypeMismatch,
    };
    const u = (rescaleTo(d, scale) orelse return error.CastFailed).unscaled;
    acc.* = (if (sub) std.math.sub(i128, acc.*, u) else std.math.add(i128, acc.*, u)) catch return error.CastFailed;
}

fn asF64Opt(v: Value) ?f64 {
    return switch (v) {
        .int => |x| @floatFromInt(x),
        .float => |x| x,
        .decimal => |d| blk: {
            var scale: f64 = 1;
            var i: u8 = 0;
            while (i < d.scale) : (i += 1) scale *= 10;
            break :blk @as(f64, @floatFromInt(d.unscaled)) / scale;
        },
        .bool => |b| if (b) 1 else 0,
        else => null,
    };
}

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

    const Kept = std.ArrayListUnmanaged([]Value);
    const Parts = std.HashMap([]const Value, *Kept, keyhash.MultiKeyCtx, std.hash_map.default_max_load_percentage);

    fn ranksBefore(self: *const Window, a: []const Value, b: []const Value) bool {
        for (self.ord) |k| {
            const o = keyOrder(a[k.idx], b[k.idx], k.desc);
            if (o != .eq) return o == .lt;
        }
        return false;
    }

    /// ROW_NUMBER up to rank `k`: per partition, the best `k` rows so far, best first.
    /// A newcomer goes after every row it does not rank before, so ties keep input
    /// order as the stable sort would, and one landing past `k` is never copied.
    fn nextTopK(self: *Window, arena: std.mem.Allocator, k: u64, gpa: std.mem.Allocator) anyerror!?Batch {
        var parts = Parts.init(gpa);
        defer {
            var it = parts.iterator();
            while (it.next()) |e| {
                freeRowGpa(gpa, @constCast(e.key_ptr.*));
                for (e.value_ptr.*.items) |r| freeRowGpa(gpa, r);
                e.value_ptr.*.deinit(gpa);
                gpa.destroy(e.value_ptr.*);
            }
            parts.deinit();
        }
        var pull = std.heap.ArenaAllocator.init(gpa);
        defer pull.deinit();
        const ncols = self.in_schema.fields.len;
        const row = try arena.alloc(Value, ncols);
        const key = try arena.alloc(Value, self.part.len);

        if (k > 0) while (try self.child.next(pull.allocator())) |b| {
            var r: usize = 0;
            while (r < b.len) : (r += 1) {
                for (row, b.columns) |*v, c| v.* = c.getValue(r);
                for (key, self.part) |*v, pk| v.* = row[pk.idx];
                const gop = try parts.getOrPut(key);
                if (!gop.found_existing) {
                    gop.key_ptr.* = try dupeRowGpa(gpa, key);
                    gop.value_ptr.* = try gpa.create(Kept);
                    gop.value_ptr.*.* = .{};
                }
                const kept = gop.value_ptr.*;
                var lo: usize = 0;
                var hi: usize = kept.items.len;
                while (lo < hi) {
                    const mid = (lo + hi) / 2;
                    if (self.ranksBefore(row, kept.items[mid])) hi = mid else lo = mid + 1;
                }
                if (lo >= k) continue;
                try kept.insert(gpa, lo, try dupeRowGpa(gpa, row));
                if (kept.items.len > k) freeRowGpa(gpa, kept.pop().?);
            }
            _ = pull.reset(.retain_capacity);
        };

        const order = try arena.alloc(Parts.Entry, parts.count());
        var it = parts.iterator();
        var n: usize = 0;
        var total: usize = 0;
        while (it.next()) |e| : (n += 1) {
            order[n] = e;
            total += e.value_ptr.*.items.len;
        }
        if (total == 0) return null;
        std.mem.sort(Parts.Entry, order, {}, struct {
            fn lt(_: void, a: Parts.Entry, b: Parts.Entry) bool {
                for (a.key_ptr.*, b.key_ptr.*) |x, y| {
                    const o = keyOrder(x, y, false);
                    if (o != .eq) return o == .lt;
                }
                return false;
            }
        }.lt);

        const builders = try arena.alloc(column.Builder, self.out_schema.fields.len);
        for (builders, self.out_schema.fields) |*bd, f| bd.* = try column.Builder.initCapacity(arena, f.ty, total);
        for (order) |e| {
            for (e.value_ptr.*.items, 1..) |kr, rn| {
                for (kr, builders[0..ncols]) |v, *bd| try bd.append(v);
                try builders[ncols].append(.{ .int = @intCast(rn) });
            }
        }
        const cols = try arena.alloc(column.Column, builders.len);
        for (builders, cols) |*bd, *c| c.* = try bd.finish();
        return Batch{ .schema = self.out_schema, .columns = cols, .len = total };
    }

    fn intSum(self: *Window, acc: i128) error{IntOverflow}!Value {
        const v = std.math.cast(i64, acc) orelse {
            if (self.err) |ec| ec.set("{s}: in window SUM", .{errLabel(error.IntOverflow)});
            return error.IntOverflow;
        };
        return .{ .int = v };
    }

    pub const Layout = struct {
        pstart: []usize,
        pend: []usize,
        gstart: []usize,
        gend: []usize,
    };

    fn layout(arena: std.mem.Allocator, idx: []const usize, parts: []const KeyArr, ords: []const KeyArr) !Layout {
        const n = idx.len;
        const l = Layout{
            .pstart = try arena.alloc(usize, n),
            .pend = try arena.alloc(usize, n),
            .gstart = try arena.alloc(usize, n),
            .gend = try arena.alloc(usize, n),
        };
        var i: usize = 0;
        while (i < n) {
            var j = i + 1;
            while (j < n and sameOn(parts, idx[j - 1], idx[j])) j += 1;
            var g = i;
            while (g < j) {
                var e = g + 1;
                while (e < j and sameOn(ords, idx[e - 1], idx[e])) e += 1;
                for (g..e) |k| {
                    l.pstart[k] = i;
                    l.pend[k] = j;
                    l.gstart[k] = g;
                    l.gend[k] = e;
                }
                g = e;
            }
            i = j;
        }
        return l;
    }

    const RangeKeys = struct {
        float: bool,
        ints: []i128 = &.{},
        floats: []f64 = &.{},
        null_at: []bool,
    };

    /// The order key as RANGE offsets compare it, ascending (a DESC key negated): one
    /// number per sorted row, `null_at` marking the rows whose key is null.
    fn rangeKeys(self: *Window, arena: std.mem.Allocator, all: Batch, idx: []const usize) !RangeKeys {
        const k = self.ord[0];
        const col = all.columns[k.idx];
        const conv = self.range;
        var rk = RangeKeys{ .float = conv.float, .null_at = try arena.alloc(bool, idx.len) };
        if (conv.float) rk.floats = try arena.alloc(f64, idx.len) else rk.ints = try arena.alloc(i128, idx.len);
        for (idx, 0..) |row, p| {
            const v = col.getValue(row);
            rk.null_at[p] = v.isNull();
            if (v.isNull()) continue;
            if (conv.float) {
                const x = asF64Opt(v) orelse 0;
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

    /// RANGE frames over one partition `[ps, pe)` whose non-null keys ascend: each
    /// offset edge is the first position past a target, found by a pointer that only
    /// moves forward as the keys grow.
    fn rangeOffsets(comptime T: type, keys: []const T, null_at: []const bool, l: Layout, fr: Frame, ps: usize, pe: usize, fs: []usize, fe: []usize) void {
        var nb = ps;
        while (nb < pe and !null_at[nb]) nb += 1;
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
        var sp = Ptr{ .at = ps };
        var ep = Ptr{ .at = ps };
        for (ps..pe) |k| {
            const is_null = k >= nb;
            const s: usize = switch (fr.start) {
                .unbounded_preceding => ps,
                .current_row => l.gstart[k],
                .preceding => |o| if (is_null) nb else sp.lower(keys, nb, keys[k] - offOf(T, o)),
                .following => |o| if (is_null) nb else sp.lower(keys, nb, keys[k] + offOf(T, o)),
                .unbounded_following => pe,
            };
            const e: usize = switch (fr.end) {
                .unbounded_preceding => ps,
                .current_row => l.gend[k],
                .preceding => |o| if (is_null) pe else ep.upper(keys, nb, keys[k] - offOf(T, o)),
                .following => |o| if (is_null) pe else ep.upper(keys, nb, keys[k] + offOf(T, o)),
                .unbounded_following => pe,
            };
            fs[k] = s;
            fe[k] = @max(s, e);
        }
    }

    /// Whether the aggregate can take a row back out of its running state.
    fn invertible(f: ast.AggFunc) bool {
        return switch (f) {
            .median, .bit_and, .bit_or => false,
            else => true,
        };
    }

    const Distinct = std.HashMap([]const Value, u32, keyhash.MultiKeyCtx, std.hash_map.default_max_load_percentage);

    const Slide = struct {
        w: *Window,
        f: Func,
        agg: ast.AggFunc,
        vals: []const Value,
        dec: ?u8,
        out_int: bool,
        n: i64 = 0,
        t: i64 = 0,
        acc_i: i128 = 0,
        acc_f: f64 = 0,
        seen_float: bool = false,
        mean: f64 = 0,
        m2: f64 = 0,
        s1: i128 = 0,
        s2: i128 = 0,
        inexact: i64 = 0,
        overflow: bool = false,
        dq: std.array_list.Managed(usize),
        dq_head: usize = 0,
        seen: Distinct,
        xs: std.array_list.Managed(f64),

        fn reset(s: *Slide) void {
            s.n = 0;
            s.t = 0;
            s.acc_i = 0;
            s.acc_f = 0;
            s.seen_float = false;
            s.mean = 0;
            s.m2 = 0;
            s.s1 = 0;
            s.s2 = 0;
            s.inexact = 0;
            s.overflow = false;
            s.dq.clearRetainingCapacity();
            s.dq_head = 0;
            s.seen.clearRetainingCapacity();
            s.xs.clearRetainingCapacity();
        }

        fn add(s: *Slide, p: usize) !void {
            if (s.f.arg == null) {
                s.n += 1;
                return;
            }
            const v = s.vals[p];
            if (v.isNull()) return;
            switch (s.agg) {
                .count => if (s.f.distinct) {
                    const gop = try s.seen.getOrPut(s.vals[p .. p + 1]);
                    if (!gop.found_existing) gop.value_ptr.* = 0;
                    gop.value_ptr.* += 1;
                } else {
                    s.n += 1;
                },
                .count_if => if (v == .bool and v.bool) {
                    s.n += 1;
                },
                .sum, .avg => {
                    if (s.dec) |sc| {
                        try decStep(&s.acc_i, v, sc, false);
                        s.n += 1;
                    } else switch (v) {
                        .int => |x| {
                            s.acc_i += x;
                            s.acc_f += @floatFromInt(x);
                            s.n += 1;
                        },
                        else => if (asF64Opt(v)) |x| {
                            s.acc_f += x;
                            s.seen_float = true;
                            s.n += 1;
                        },
                    }
                },
                .min, .max => {
                    while (s.dq.items.len > s.dq_head) {
                        const back = s.vals[s.dq.items[s.dq.items.len - 1]];
                        const beaten = if (s.agg == .max) !lessV(v, back) else !lessV(back, v);
                        if (!beaten) break;
                        s.dq.items.len -= 1;
                    }
                    try s.dq.append(p);
                },
                .bool_and, .bool_or => if (v == .bool) {
                    s.n += 1;
                    if (v.bool) s.t += 1;
                },
                .bit_and, .bit_or, .bit_xor => if (v == .int) {
                    s.acc_i = if (s.n == 0) v.int else switch (s.agg) {
                        .bit_and => s.acc_i & v.int,
                        .bit_or => s.acc_i | v.int,
                        else => s.acc_i ^ v.int,
                    };
                    s.n += 1;
                },
                .var_samp, .var_pop, .stddev_samp, .stddev_pop => if (asF64Opt(v)) |x| {
                    s.moments(v, false);
                    s.n += 1;
                    const delta = x - s.mean;
                    s.mean += delta / @as(f64, @floatFromInt(s.n));
                    s.m2 += delta * (x - s.mean);
                },
                .median => if (asF64Opt(v)) |x| try s.xs.append(x),
            }
        }

        fn remove(s: *Slide, p: usize) !void {
            if (s.f.arg == null) {
                s.n -= 1;
                return;
            }
            const v = s.vals[p];
            if (v.isNull()) return;
            switch (s.agg) {
                .count => if (s.f.distinct) {
                    const e = s.seen.getEntry(s.vals[p .. p + 1]) orelse return;
                    e.value_ptr.* -= 1;
                    if (e.value_ptr.* == 0) _ = s.seen.remove(s.vals[p .. p + 1]);
                } else {
                    s.n -= 1;
                },
                .count_if => if (v == .bool and v.bool) {
                    s.n -= 1;
                },
                .sum, .avg => {
                    if (s.dec) |sc| {
                        try decStep(&s.acc_i, v, sc, true);
                        s.n -= 1;
                    } else switch (v) {
                        .int => |x| {
                            s.acc_i -= x;
                            s.acc_f -= @floatFromInt(x);
                            s.n -= 1;
                        },
                        else => if (asF64Opt(v)) |x| {
                            s.acc_f -= x;
                            s.n -= 1;
                        },
                    }
                },
                .min, .max => if (s.dq_head < s.dq.items.len and s.dq.items[s.dq_head] == p) {
                    s.dq_head += 1;
                },
                .bool_and, .bool_or => if (v == .bool) {
                    s.n -= 1;
                    if (v.bool) s.t -= 1;
                },
                .bit_xor => if (v == .int) {
                    s.acc_i ^= v.int;
                    s.n -= 1;
                },
                .var_samp, .var_pop, .stddev_samp, .stddev_pop => if (asF64Opt(v)) |x| {
                    s.moments(v, true);
                    if (s.n <= 1) {
                        s.n = 0;
                        s.mean = 0;
                        s.m2 = 0;
                        return;
                    }
                    const nf: f64 = @floatFromInt(s.n);
                    const mean2 = (nf * s.mean - x) / (nf - 1);
                    s.m2 -= (x - s.mean) * (x - mean2);
                    if (s.m2 < 0) s.m2 = 0;
                    s.mean = mean2;
                    s.n -= 1;
                },
                .bit_and, .bit_or, .median => unreachable,
            }
        }

        /// Integer sums of x and x² beside Welford's state: over ints the variance is
        /// then exact, so a frame of equal values reads 0 however many rows passed.
        fn moments(s: *Slide, v: Value, sub: bool) void {
            if (v != .int) {
                s.inexact += if (sub) -1 else 1;
                return;
            }
            const x: i128 = v.int;
            const sq = std.math.mul(i128, x, x) catch {
                s.overflow = true;
                return;
            };
            const d1 = if (sub) -x else x;
            const d2 = if (sub) -sq else sq;
            s.s1 = std.math.add(i128, s.s1, d1) catch blk: {
                s.overflow = true;
                break :blk s.s1;
            };
            s.s2 = std.math.add(i128, s.s2, d2) catch blk: {
                s.overflow = true;
                break :blk s.s2;
            };
        }

        fn sqDev(s: *const Slide) f64 {
            if (s.inexact == 0 and !s.overflow and s.n > 0) {
                const n: i128 = s.n;
                if (std.math.mul(i128, n, s.s2)) |a| if (std.math.mul(i128, s.s1, s.s1)) |b| {
                    return @as(f64, @floatFromInt(a - b)) / @as(f64, @floatFromInt(n));
                } else |_| {} else |_| {}
            }
            return s.m2;
        }

        fn result(s: *Slide) !Value {
            return switch (s.agg) {
                .count => if (s.f.distinct) .{ .int = @intCast(s.seen.count()) } else .{ .int = s.n },
                .count_if => .{ .int = s.n },
                .sum => if (s.n == 0) .null else if (s.dec) |sc| .{ .decimal = .{ .unscaled = s.acc_i, .scale = sc } } else if (s.seen_float or !s.out_int) .{ .float = s.acc_f } else try s.w.intSum(s.acc_i),
                .avg => if (s.n == 0) .null else .{ .float = (if (s.seen_float) s.acc_f else @as(f64, @floatFromInt(s.acc_i))) / @as(f64, @floatFromInt(s.n)) },
                .min, .max => if (s.dq_head < s.dq.items.len) s.vals[s.dq.items[s.dq_head]] else .null,
                .bool_and => if (s.n == 0) .null else .{ .bool = s.t == s.n },
                .bool_or => if (s.n == 0) .null else .{ .bool = s.t > 0 },
                .bit_and, .bit_or, .bit_xor => if (s.n == 0) .null else .{ .int = @intCast(s.acc_i) },
                .var_samp, .stddev_samp => if (s.n < 2) .null else spread(s.agg, s.sqDev() / @as(f64, @floatFromInt(s.n - 1))),
                .var_pop, .stddev_pop => if (s.n < 1) .null else spread(s.agg, s.sqDev() / @as(f64, @floatFromInt(s.n))),
                .median => blk: {
                    const xs = s.xs.items;
                    if (xs.len == 0) break :blk .null;
                    const mid = xs.len / 2;
                    const hi = selectNth(xs, mid);
                    if (xs.len % 2 == 1) break :blk .{ .float = hi };
                    var lo = xs[0];
                    for (xs[1..mid]) |x| lo = @max(lo, x);
                    break :blk .{ .float = (lo + hi) / 2 };
                },
            };
        }
    };

    fn spread(func: ast.AggFunc, variance: f64) Value {
        return .{ .float = switch (func) {
            .stddev_samp, .stddev_pop => @sqrt(variance),
            else => variance,
        } };
    }

    /// An aggregate over each row's frame, as the file header describes.
    fn aggregateOver(self: *Window, arena: std.mem.Allocator, f: Func, agg: ast.AggFunc, vals: []const Value, fs: []const usize, fe: []const usize, l: Layout, out_ty: types.Type, out: []Value) !void {
        var s = Slide{
            .w = self,
            .f = f,
            .agg = agg,
            .vals = vals,
            .dec = if (agg == .sum and out_ty.kind == .decimal) out_ty.scale else null,
            .out_int = out_ty.kind == .int,
            .dq = std.array_list.Managed(usize).init(arena),
            .seen = Distinct.init(arena),
            .xs = std.array_list.Managed(f64).init(arena),
        };
        const inv = invertible(agg);
        var k: usize = 0;
        const n = fs.len;
        while (k < n) {
            const pe = l.pend[k];
            s.reset();
            var lo = k;
            var hi = k;
            while (k < pe) : (k += 1) {
                const st = fs[k];
                const en = fe[k];
                if (inv) {
                    while (hi < en) : (hi += 1) try s.add(hi);
                    while (lo < st) : (lo += 1) {
                        if (lo < hi) try s.remove(lo);
                    }
                    if (hi < lo) hi = lo;
                } else if (st == lo and en >= hi) {
                    while (hi < en) : (hi += 1) try s.add(hi);
                } else {
                    s.reset();
                    lo = st;
                    hi = st;
                    while (hi < en) : (hi += 1) try s.add(hi);
                }
                out[k] = try s.result();
            }
        }
    }

    pub fn next(self: *Window, arena: std.mem.Allocator) anyerror!?Batch {
        if (self.done) return null;
        self.done = true;
        if (self.top_k) |k| return self.nextTopK(arena, k, self.gpa.?);
        const all = (try materializeAll(arena, self.child, self.in_schema)) orelse return null;
        const n = all.len;

        const idx = try arena.alloc(usize, n);
        for (idx, 0..) |*x, i| x.* = i;

        const arrs = try arena.alloc(KeyArr, self.part.len + self.ord.len);
        for (self.part, 0..) |k, i| arrs[i] = try KeyArr.prepare(arena, all.columns[k.idx], k.desc);
        for (self.ord, 0..) |k, i| arrs[self.part.len + i] = try KeyArr.prepare(arena, all.columns[k.idx], k.desc);
        try sortIdxThreads(arena, idx, arrs, self.threads);
        const parts = arrs[0..self.part.len];
        const ords = arrs[self.part.len..];
        const l = try layout(arena, idx, parts, ords);

        var rk: ?RangeKeys = null;
        for (self.funcs) |f| {
            if (f.frame.range and (f.frame.start == .preceding or f.frame.start == .following or f.frame.end == .preceding or f.frame.end == .following)) {
                rk = try self.rangeKeys(arena, all, idx);
                break;
            }
        }

        const ncols = all.columns.len;
        const cols = try arena.alloc(column.Column, ncols + self.funcs.len);
        for (all.columns, 0..) |*col, ci| cols[ci] = try column.permute(arena, col.*, idx);

        for (self.funcs, 0..) |f, fi| {
            const out_ty = self.out_schema.fields[ncols + fi].ty;
            var bd = try column.Builder.initCapacity(arena, out_ty, n);
            const vals: []Value = if (f.arg) |ai| blk: {
                const vs = try arena.alloc(Value, n);
                for (idx, vs) |row, *v| v.* = all.columns[ai].getValue(row);
                break :blk vs;
            } else &.{};
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
                        var cnt: []usize = &.{};
                        var nnpos: []usize = &.{};
                        if (f.ignore_nulls) {
                            cnt = try arena.alloc(usize, n + 1);
                            var c: usize = 0;
                            for (vals, 0..) |v, p| {
                                cnt[p] = c;
                                if (!v.isNull()) c += 1;
                            }
                            cnt[n] = c;
                            nnpos = try arena.alloc(usize, c);
                            c = 0;
                            for (vals, 0..) |v, p| if (!v.isNull()) {
                                nnpos[c] = p;
                                c += 1;
                            };
                        }
                        var fr: ?Frames = null;
                        if (kind != .lag and kind != .lead) fr = try frames(arena, l, f.frame, rk);
                        for (0..n) |k| {
                            const ps = l.pstart[k];
                            const pe = l.pend[k];
                            const v: Value = switch (kind) {
                                .lag, .lead => blk: {
                                    if (f.ignore_nulls) {
                                        if (f.offset == 0) break :blk vals[k];
                                        const j: i64 = if (kind == .lag)
                                            @as(i64, @intCast(cnt[k])) - f.offset
                                        else
                                            @as(i64, @intCast(cnt[k + 1])) + f.offset - 1;
                                        if (j < @as(i64, @intCast(cnt[ps])) or j >= @as(i64, @intCast(cnt[pe]))) break :blk f.default;
                                        break :blk vals[nnpos[@intCast(j)]];
                                    }
                                    const dir: i64 = if (kind == .lag) -f.offset else f.offset;
                                    const target = @as(i64, @intCast(k)) + dir;
                                    if (target < @as(i64, @intCast(ps)) or target >= @as(i64, @intCast(pe))) break :blk f.default;
                                    break :blk vals[@intCast(target)];
                                },
                                else => blk: {
                                    const s = fr.?.fs[k];
                                    const e = fr.?.fe[k];
                                    if (f.ignore_nulls) {
                                        const a = cnt[s];
                                        const b = cnt[e];
                                        break :blk switch (kind) {
                                            .first_value => if (a < b) vals[nnpos[a]] else .null,
                                            .last_value => if (a < b) vals[nnpos[b - 1]] else .null,
                                            else => if (a + @as(usize, @intCast(f.offset)) - 1 < b) vals[nnpos[a + @as(usize, @intCast(f.offset)) - 1]] else .null,
                                        };
                                    }
                                    break :blk switch (kind) {
                                        .first_value => if (s < e) vals[s] else .null,
                                        .last_value => if (s < e) vals[e - 1] else .null,
                                        else => if (s + @as(usize, @intCast(f.offset)) - 1 < e) vals[s + @as(usize, @intCast(f.offset)) - 1] else .null,
                                    };
                                },
                            };
                            try bd.append(v);
                        }
                    },
                },
                .agg => |agg| {
                    const fr = try frames(arena, l, f.frame, rk);
                    const out = try arena.alloc(Value, n);
                    try self.aggregateOver(arena, f, agg, vals, fr.fs, fr.fe, l, out_ty, out);
                    for (out) |v| try bd.append(v);
                },
            }
            cols[ncols + fi] = try bd.finish();
        }
        return Batch{ .schema = self.out_schema, .columns = cols, .len = n };
    }
};

const testing = std.testing;
const Scan = @import("../op.zig").Scan;
const TestSource = @import("testing_util.zig").TestSource;

const BruteRow = struct { p: ?i64, o: ?i64, v: ?i64 };

fn orderNullsLast(x: ?i64, y: ?i64, desc: bool) std.math.Order {
    if (x == null or y == null) {
        if (x == null and y == null) return .eq;
        return if (x == null) .gt else .lt;
    }
    const o = std.math.order(x.?, y.?);
    return if (desc) o.invert() else o;
}

/// The rows in the order the operator sorts them: partition, then order key (DESC
/// flipping it), nulls last either way, ties in input order.
fn bruteSort(a: std.mem.Allocator, rows: []const BruteRow, desc: bool) ![]BruteRow {
    const out = try a.dupe(BruteRow, rows);
    std.sort.block(BruteRow, out, desc, struct {
        fn lt(d: bool, l: BruteRow, r: BruteRow) bool {
            const po = orderNullsLast(l.p, r.p, false);
            if (po != .eq) return po == .lt;
            return orderNullsLast(l.o, r.o, d) == .lt;
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
fn inFrame(rows: []const BruteRow, k: usize, j: usize, fr: Window.Frame, desc: bool) bool {
    var yk: i64 = @intCast(k);
    var yj: i64 = @intCast(j);
    if (fr.range) {
        const cur = rows[k].o;
        const x = rows[j].o;
        if (cur == null) return x == null or fr.start == .unbounded_preceding;
        if (x == null) return fr.end == .unbounded_following;
        yk = if (desc) -cur.? else cur.?;
        yj = if (desc) -x.? else x.?;
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

fn bruteValue(a: std.mem.Allocator, rows: []const BruteRow, k: usize, f: Window.Func, desc: bool) !Value {
    const ps, const pe = partitionOf(rows, k);
    var frame = std.array_list.Managed(?i64).init(a);
    for (ps..pe) |j| if (inFrame(rows, k, j, f.frame, desc)) try frame.append(rows[j].v);
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
fn runWindow(a: std.mem.Allocator, rows: []const BruteRow, funcs: []const Window.Func, desc: bool, with_order: bool) ![]Batch {
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
        .part = &[_]Sort.Key{.{ .idx = 0, .desc = false }},
        .ord = if (with_order) try a.dupe(Sort.Key, &[_]Sort.Key{.{ .idx = 1, .desc = desc }}) else &.{},
        .funcs = funcs,
    };
    var out = std.array_list.Managed(Batch).init(a);
    while (try w.next(a)) |b| try out.append(b);
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
    for ([_]usize{ 0, 1, 2, 7, 40 }) |n| for ([_]bool{ false, true }) |desc| for ([_]bool{ false, true }) |range| {
        const rows = try randomRows(a, rnd, n);
        const sorted = try bruteSort(a, rows, desc);
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
        const out = try runWindow(a, rows, funcs.items, desc, true);
        var at: usize = 0;
        for (out) |b| for (0..b.len) |r| {
            for (funcs.items, 0..) |f, fi| try expectSameValue(try bruteValue(a, sorted, at, f, desc), b.columns[3 + fi].getValue(r));
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
    for ([_]usize{ 0, 1, 3, 25, 60 }) |n| for ([_]bool{ false, true }) |desc| {
        const rows = try randomRows(a, rnd, n);
        const sorted = try bruteSort(a, rows, desc);
        var funcs = std.array_list.Managed(Window.Func).init(a);
        for ([_]i64{ 0, 1, 2, 4 }) |off| for ([_]bool{ false, true }) |ign| {
            try funcs.append(.{ .func = .{ .win = .lag }, .arg = 2, .offset = off, .ignore_nulls = ign });
            try funcs.append(.{ .func = .{ .win = .lead }, .arg = 2, .offset = off, .ignore_nulls = ign });
        };
        for ([_]i64{ 1, 2, 3, 7 }) |b| try funcs.append(.{ .func = .{ .win = .ntile }, .offset = b });
        try funcs.append(.{ .func = .{ .win = .rank } });
        try funcs.append(.{ .func = .{ .win = .percent_rank } });
        try funcs.append(.{ .func = .{ .win = .cume_dist } });
        const out = try runWindow(a, rows, funcs.items, desc, true);
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
                    .lag, .lead => try bruteValue(a, sorted, at, f, desc),
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
    const out = try runWindow(a, &rows, &funcs, false, false);
    const want = [_][3]?i64{ .{ 3, 1, 2 }, .{ 3, 3, 2 }, .{ 10, 4, 6 }, .{ 10, 4, 6 }, .{ 10, 6, 6 } };
    var at: usize = 0;
    for (out) |b| for (0..b.len) |r| {
        for (want[at], 0..) |w, c| try expectSameValue(if (w) |x| .{ .int = x } else .null, b.columns[3 + c].getValue(r));
        at += 1;
    };
    try testing.expectEqual(@as(usize, 5), at);
}
