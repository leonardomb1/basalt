//! The aggregate functions as math only. Each is a `State` and the rules over it:
//! `add` one value of its input type `In`, `merge` another lane's state, `finish`
//! into the result, and, for the few holding memory, `own` it in the group's
//! allocator once a merge has copied the state in. `kernel.zig` builds every loop
//! around these (a batch, a batch's rows into their groups, a lane's states into
//! another's), so adding an aggregate is a struct here and a line in `kernelFor`.
//!
//! `In` is the value the state takes: `void` when only presence counts, `Value` when
//! no physical type fits. Optional hooks: `addN` (k values at once, for `void`),
//! `addAll` (a slice with no nulls, where a vector loop beats one at a time),
//! `needs_alloc` (the state allocates per value), `fold_expr` (a DECIMAL arithmetic
//! argument is added value by value from its operands, as the column would hold
//! them, without the column), `own` (rebuild in the group's
//! allocator when a merge frees its source), and `remove` (the exact inverse of
//! `add`, which a window frame slides with when a value leaves it). A DISTINCT
//! aggregate is its plain math behind `pairs.zig`'s filter.
//!
//! Sums are exact, i128 for ints and i256 for DECIMAL (whose addends reach 10^38),
//! range-checked once in `finish`, so the answer cannot depend on how rows split
//! across batches and lanes or on their order. DECIMAL addends are normalized to
//! the output scale. A DECIMAL AVG sums exactly at its argument's scale, a value of
//! wider scale as a float, and divides once. Variances keep Welford's moments and
//! merge with Chan's formula, in fixed lane order. Float MIN/MAX order NaN above
//! every number. Bitwise aggregates start from their first value, zero not being
//! AND's identity.

const Value = @import("../../value.zig").Value;
const Decimal = @import("../../value.zig").Decimal;
const agg_args = @import("../../agg_args.zig");
const ast = @import("../../../lang/ast.zig");
const column = @import("../../column.zig");
const eval = @import("../../eval.zig");
const fmtnum = @import("../../eval/format.zig");
const floatord = @import("../../floatord.zig");
const decimal = @import("../../../base/decimal.zig");
const decimal_ops = @import("../../decimal_ops.zig");
const simd = @import("../../simd.zig");
const std = @import("std");
const types = @import("../../../lang/types.zig");

/// What the math sees besides its state: the result type, the type values are read
/// as (`ty`, the cast target for a value not already of type `In`), the group's
/// allocator for anything a state keeps, and a scratch one for a cast.
pub const Ctx = struct {
    out: types.Type,
    ty: types.Type,
    alloc: std.mem.Allocator,
    scratch: std.mem.Allocator,
    /// A percentile's fraction, and whether its WITHIN GROUP order is descending.
    frac: f64 = 0.5,
    desc: bool = false,
};

pub const Count = struct {
    pub const In = void;
    pub const State = struct { n: i64 = 0 };
    pub fn add(s: *State, _: void, _: Ctx) !void {
        s.n += 1;
    }
    pub fn addN(s: *State, k: usize) void {
        s.n += @intCast(k);
    }
    pub fn remove(s: *State, _: void, _: Ctx) !void {
        s.n -= 1;
    }
    pub fn merge(d: *State, s: *const State, _: Ctx) !void {
        d.n += s.n;
    }
    pub fn finish(s: *const State, _: Ctx) !Value {
        return .{ .int = s.n };
    }
};

pub const CountIf = struct {
    pub const In = bool;
    pub const State = struct { n: i64 = 0 };
    pub fn add(s: *State, x: bool, _: Ctx) !void {
        s.n += @intFromBool(x);
    }
    pub fn remove(s: *State, x: bool, _: Ctx) !void {
        s.n -= @intFromBool(x);
    }
    pub fn merge(d: *State, s: *const State, _: Ctx) !void {
        d.n += s.n;
    }
    pub fn finish(s: *const State, _: Ctx) !Value {
        return .{ .int = s.n };
    }
};

pub const SumInt = struct {
    pub const In = i64;
    pub const State = struct { n: i64 = 0, s: i128 align(8) = 0 };
    pub fn add(s: *State, x: i64, _: Ctx) !void {
        s.s += x;
        s.n += 1;
    }
    pub fn remove(s: *State, x: i64, _: Ctx) !void {
        s.s -= x;
        s.n -= 1;
    }
    pub fn addAll(s: *State, xs: []const i64, _: Ctx) !void {
        var t: i128 = 0;
        for (xs) |x| t += x;
        s.s += t;
        s.n += @intCast(xs.len);
    }
    pub fn merge(d: *State, s: *const State, _: Ctx) !void {
        d.s = std.math.add(i128, d.s, s.s) catch return error.IntOverflow;
        d.n += s.n;
    }
    pub fn finish(s: *const State, _: Ctx) !Value {
        if (s.n == 0) return .null;
        return .{ .int = std.math.cast(i64, s.s) orelse return error.IntOverflow };
    }
};

pub const SumDecimal = struct {
    pub const In = Decimal;
    pub const fold_expr = true;
    pub const State = struct { n: i64 = 0, s: i256 align(8) = 0 };
    pub fn add(s: *State, x: Decimal, cx: Ctx) !void {
        try decimal_ops.addAtScale(&s.s, x, cx.out.scale);
        s.n += 1;
    }
    pub fn remove(s: *State, x: Decimal, cx: Ctx) !void {
        const u = (decimal.rescaleTo(x, cx.out.scale) orelse return decimal_ops.decimalOverflow()).unscaled;
        s.s = std.math.sub(i256, s.s, u) catch return decimal_ops.decimalOverflow();
        s.n -= 1;
    }
    pub fn merge(d: *State, s: *const State, _: Ctx) !void {
        d.s = std.math.add(i256, d.s, s.s) catch return decimal_ops.decimalOverflow();
        d.n += s.n;
    }
    pub fn finish(s: *const State, cx: Ctx) !Value {
        if (s.n == 0) return .null;
        return .{ .decimal = decimal.wideSumDecimal(s.s, cx.out.scale) orelse return decimal_ops.decimalOverflow() };
    }
};

pub const SumFloat = struct {
    pub const In = f64;
    pub const State = struct { n: i64 = 0, s: f64 = 0 };
    pub fn add(s: *State, x: f64, _: Ctx) !void {
        s.s += x;
        s.n += 1;
    }
    pub fn addAll(s: *State, xs: []const f64, _: Ctx) !void {
        s.s += simd.sumF(xs);
        s.n += @intCast(xs.len);
    }
    pub fn merge(d: *State, s: *const State, _: Ctx) !void {
        d.s += s.s;
        d.n += s.n;
    }
    pub fn finish(s: *const State, _: Ctx) !Value {
        return if (s.n == 0) .null else .{ .float = s.s };
    }
};

pub const AvgInt = struct {
    pub const In = i64;
    pub const State = SumInt.State;
    pub const add = SumInt.add;
    pub const addAll = SumInt.addAll;
    pub const remove = SumInt.remove;
    pub const merge = SumInt.merge;
    pub fn finish(s: *const State, _: Ctx) !Value {
        if (s.n == 0) return .null;
        return .{ .float = @as(f64, @floatFromInt(s.s)) / @as(f64, @floatFromInt(s.n)) };
    }
};

pub const AvgFloat = struct {
    pub const In = f64;
    pub const State = SumFloat.State;
    pub const add = SumFloat.add;
    pub const addAll = SumFloat.addAll;
    pub const merge = SumFloat.merge;
    pub fn finish(s: *const State, _: Ctx) !Value {
        if (s.n == 0) return .null;
        return .{ .float = s.s / @as(f64, @floatFromInt(s.n)) };
    }
};

/// Exact at the argument's scale (`cx.ty.scale`); a value of wider scale, which a
/// per-value scale allows, is added as a float rather than rounded.
pub const AvgDecimal = struct {
    pub const In = Decimal;
    pub const State = struct { n: i64 = 0, s: i256 align(8) = 0, f: f64 = 0 };
    pub fn add(s: *State, x: Decimal, cx: Ctx) !void {
        s.n += 1;
        _ = decimal.addAvg(&s.s, &s.f, x, cx.ty.scale);
    }
    pub fn merge(d: *State, s: *const State, _: Ctx) !void {
        d.s += s.s;
        d.f += s.f;
        d.n += s.n;
    }
    pub fn finish(s: *const State, cx: Ctx) !Value {
        if (s.n == 0) return .null;
        return .{ .float = decimal.avgOf(s.s, s.f, s.n, cx.ty.scale) };
    }
};

/// The order MIN and MAX keep, GROUP BY and window alike: NaN above every number,
/// decimals exactly across scales, text by bytes, and across kinds `compareValues`'s.
pub fn order(comptime T: type, a: T, b: T) std.math.Order {
    return switch (T) {
        Value => eval.compareValues(a, b) orelse .eq,
        f64 => floatord.orderF64(a, b),
        Decimal => a.order(b),
        bool => std.math.order(@intFromBool(a), @intFromBool(b)),
        []const u8 => std.mem.order(u8, a, b),
        else => std.math.order(a, b),
    };
}

/// A physical value as the declared type `out` tags it: a TIMESTAMP and an INT
/// share i64, a BYTES and a STRING their bytes.
pub fn toValue(comptime T: type, x: T, out: types.Type) Value {
    return switch (T) {
        Value => x,
        i64 => switch (out.kind) {
            .time => .{ .time = x },
            .timestamp => .{ .timestamp = x },
            else => .{ .int = x },
        },
        i32 => .{ .date = x },
        f64 => .{ .float = x },
        Decimal => .{ .decimal = x },
        bool => .{ .bool = x },
        []const u8 => if (out.kind == .bytes) .{ .bytes = x } else .{ .string = x },
        else => unreachable,
    };
}

/// MIN (`want` .lt) or MAX (.gt) over one physical type; the result takes its tag
/// from the declared type, so a TIMESTAMP and an INT share the i64 state.
pub fn Extreme(comptime T: type, comptime want: std.math.Order) type {
    return struct {
        pub const In = T;
        pub const needs_alloc = T == []const u8;
        pub const State = struct { has: bool = false, v: T = undefined, cap: if (T == []const u8) usize else void = if (T == []const u8) 0 else {} };
        pub fn add(s: *State, x: T, cx: Ctx) !void {
            if (s.has and order(T, x, s.v) != want) return;
            if (T == []const u8) try keep(s, x, cx) else s.v = x;
            s.has = true;
        }
        /// Copies a new string extreme into the state's buffer, growing it only for a
        /// longer value, so a run of improvements costs the longest one, not their sum.
        fn keep(s: *State, x: []const u8, cx: Ctx) !void {
            if (s.cap == 0 or x.len > s.cap) {
                const buf = try cx.alloc.alloc(u8, @max(x.len, s.cap * 2));
                @memcpy(buf[0..x.len], x);
                s.cap = buf.len;
                s.v = buf[0..x.len];
                return;
            }
            const buf = @constCast(s.v.ptr)[0..s.cap];
            std.mem.copyForwards(u8, buf[0..x.len], x);
            s.v = buf[0..x.len];
        }
        pub fn merge(d: *State, s: *const State, cx: Ctx) !void {
            if (s.has) try add(d, s.v, cx);
        }
        pub fn own(s: *State, cx: Ctx) !void {
            if (T == []const u8 and s.has) {
                s.v = try cx.alloc.dupe(u8, s.v);
                s.cap = s.v.len;
            }
        }
        pub fn finish(s: *const State, cx: Ctx) !Value {
            return if (s.has) toValue(T, s.v, cx.out) else .null;
        }
        pub const addAll = if (T == f64) addAllF else {};
        fn addAllF(s: *State, xs: []const f64, cx: Ctx) !void {
            if (xs.len > 0) try add(s, if (want == .lt) simd.minF(xs) else simd.maxF(xs), cx);
        }
    };
}

/// MIN or MAX over values of no single physical type, in `compareValues` order.
pub fn ExtremeValue(comptime want: std.math.Order) type {
    return struct {
        pub const In = Value;
        pub const needs_alloc = true;
        pub const State = struct { v: Value = .null, buf: []u8 = &.{} };
        pub fn add(s: *State, x: Value, cx: Ctx) !void {
            if (!s.v.isNull() and order(Value, x, s.v) != want) return;
            s.v = switch (x) {
                .string => |b| .{ .string = try keep(s, b, cx) },
                .bytes => |b| .{ .bytes = try keep(s, b, cx) },
                .nested => try x.own(cx.alloc),
                else => x,
            };
        }
        /// Copies a text or bytes extreme into the state's buffer, growing it only for
        /// a longer value, so a run of improvements costs the longest one, not their sum.
        fn keep(s: *State, x: []const u8, cx: Ctx) ![]const u8 {
            if (s.buf.len == 0 or x.len > s.buf.len) s.buf = try cx.alloc.alloc(u8, @max(x.len, s.buf.len * 2));
            std.mem.copyForwards(u8, s.buf[0..x.len], x);
            return s.buf[0..x.len];
        }
        pub fn merge(d: *State, s: *const State, cx: Ctx) !void {
            if (!s.v.isNull()) try add(d, s.v, cx);
        }
        pub fn own(s: *State, cx: Ctx) !void {
            s.v = try s.v.own(cx.alloc);
            s.buf = switch (s.v) {
                .string, .bytes => |b| @constCast(b),
                else => &.{},
            };
        }
        pub fn finish(s: *const State, _: Ctx) !Value {
            return s.v;
        }
    };
}

/// Values a group keeps whole, in blocks that never move: an arena keeps every
/// buffer a growing list outgrows, so a list in one cost up to four times its
/// values. Blocks start small, for a group of few values, and double up to
/// `max_block`; `gather` lays them out in one slice for the selection.
fn Held(comptime T: type) type {
    return struct {
        blocks: std.ArrayListUnmanaged([]T) = .empty,
        last: usize = 0,
        n: usize = 0,

        const Self = @This();
        const max_block = 8192;

        fn append(self: *Self, a: std.mem.Allocator, x: T) !void {
            if (self.blocks.items.len == 0 or self.last == self.blocks.items[self.blocks.items.len - 1].len) {
                try self.blocks.append(a, try a.alloc(T, std.math.clamp(self.n, 16, max_block)));
                self.last = 0;
            }
            self.blocks.items[self.blocks.items.len - 1][self.last] = x;
            self.last += 1;
            self.n += 1;
        }

        fn gather(self: *const Self, a: std.mem.Allocator) ![]T {
            const out = try a.alloc(T, self.n);
            var at: usize = 0;
            for (self.blocks.items, 0..) |b, i| {
                const len = if (i + 1 == self.blocks.items.len) self.last else b.len;
                @memcpy(out[at .. at + len], b[0..len]);
                at += len;
            }
            return out;
        }

        fn each(self: *const Self, comptime f: anytype, ctx: anytype) !void {
            for (self.blocks.items, 0..) |b, i| {
                const len = if (i + 1 == self.blocks.items.len) self.last else b.len;
                for (b[0..len]) |x| try f(ctx, x);
            }
        }
    };
}

/// The state of an aggregate that keeps a group's values whole (`Held`), `keep`
/// copying a value out of its batch.
fn Holding(comptime T: type, comptime keep: fn (T, Ctx) anyerror!T) type {
    return struct {
        pub const State = struct { vals: ?*Held(T) = null };
        pub fn add(s: *State, x: T, cx: Ctx) !void {
            const h = s.vals orelse blk: {
                const p = try cx.alloc.create(Held(T));
                p.* = .{};
                s.vals = p;
                break :blk p;
            };
            try h.append(cx.alloc, try keep(x, cx));
        }
        const Into = struct { s: *State, cx: Ctx };
        fn put(i: Into, x: T) !void {
            try add(i.s, x, i.cx);
        }
        pub fn merge(d: *State, s: *const State, cx: Ctx) !void {
            if (s.vals) |h| try h.each(put, Into{ .s = d, .cx = cx });
        }
        pub fn own(s: *State, cx: Ctx) !void {
            const src = s.vals orelse return;
            s.vals = null;
            try src.each(put, Into{ .s = s, .cx = cx });
        }
    };
}

fn asIs(x: f64, _: Ctx) anyerror!f64 {
    return x;
}

/// MEDIAN and PERCENTILE_CONT (`Ctx.frac`, 0.5 for the median): the values kept
/// whole as numbers and selected from at the end, between two the value
/// interpolated, as Postgres's percentile_cont has it; at 0.5 the mean of the two.
pub const Quantile = struct {
    pub const In = f64;
    pub const needs_alloc = true;
    const H = Holding(f64, asIs);
    pub const State = H.State;
    pub const add = H.add;
    pub const merge = H.merge;
    pub const own = H.own;
    pub fn finish(s: *const State, cx: Ctx) !Value {
        const h = s.vals orelse return .null;
        if (h.n == 0) return .null;
        const xs = try h.gather(cx.scratch);
        const t = if (cx.desc) 1 - cx.frac else cx.frac;
        const at = t * @as(f64, @floatFromInt(xs.len - 1));
        const lo_at: usize = @intFromFloat(@floor(at));
        const hi_at: usize = @intFromFloat(@ceil(at));
        const hi = selectNth(xs, hi_at);
        if (lo_at == hi_at) return .{ .float = hi };
        var lo = xs[0];
        for (xs[1..hi_at]) |x| lo = @max(lo, x);
        const w = at - @floor(at);
        return .{ .float = if (w == 0.5) (lo + hi) / 2 else lo + (hi - lo) * w };
    }
};

/// PERCENTILE_DISC (`Ctx.frac`): the values kept whole in their own type, the
/// first whose place in the WITHIN GROUP order is at or past the fraction.
pub fn Disc(comptime T: type) type {
    return struct {
        pub const In = T;
        pub const needs_alloc = true;
        const H = Holding(T, keepOf(T));
        pub const State = H.State;
        pub const add = H.add;
        pub const merge = H.merge;
        pub const own = H.own;
        pub fn finish(s: *const State, cx: Ctx) !Value {
            const h = s.vals orelse return .null;
            if (h.n == 0) return .null;
            const xs = try h.gather(cx.scratch);
            const place = @ceil(cx.frac * @as(f64, @floatFromInt(xs.len)));
            const i: usize = if (place < 1) 0 else @intFromFloat(place - 1);
            return toValue(T, selectBy(T, xs, if (cx.desc) xs.len - 1 - i else i), cx.out);
        }
    };
}

/// How a percentile keeps a value of `T`: text, bytes and nested values copied out
/// of the batch, the rest as they are.
fn keepOf(comptime T: type) fn (T, Ctx) anyerror!T {
    return struct {
        fn keep(x: T, cx: Ctx) anyerror!T {
            if (T == []const u8) return cx.alloc.dupe(u8, x);
            if (T != Value) return x;
            return x.own(cx.alloc);
        }
    }.keep;
}

/// ANY_VALUE: the first value that is not NULL, in input order.
pub const AnyValue = struct {
    pub const In = Value;
    pub const needs_alloc = true;
    pub const State = struct { has: bool = false, v: Value = .null };
    pub fn add(s: *State, x: Value, cx: Ctx) !void {
        if (s.has) return;
        s.v = try x.own(cx.alloc);
        s.has = true;
    }
    pub fn merge(d: *State, s: *const State, cx: Ctx) !void {
        if (s.has) try add(d, s.v, cx);
    }
    pub fn own(s: *State, cx: Ctx) !void {
        if (s.has) s.v = try s.v.own(cx.alloc);
    }
    pub fn finish(s: *const State, _: Ctx) !Value {
        return s.v;
    }
};

/// ARG_MAX (`want` .gt) or ARG_MIN: the first argument of the row with the best
/// second, the first such row in input order; a row whose second is NULL never wins.
pub fn ArgBest(comptime want: std.math.Order) type {
    return struct {
        pub const In = []const u8;
        pub const needs_alloc = true;
        pub const State = struct { row: ?[]const u8 = null };
        fn key(row: []const u8) Value {
            var buf: [2]Value = undefined;
            return agg_args.decode(row, &buf).vals[1];
        }
        pub fn add(s: *State, x: []const u8, cx: Ctx) !void {
            const k = key(x);
            if (k == .null) return;
            if (s.row) |have| if ((eval.compareValues(k, key(have)) orelse .eq) != want) return;
            s.row = try cx.alloc.dupe(u8, x);
        }
        pub fn merge(d: *State, s: *const State, cx: Ctx) !void {
            if (s.row) |r| try add(d, r, cx);
        }
        pub fn own(s: *State, cx: Ctx) !void {
            if (s.row) |r| s.row = try cx.alloc.dupe(u8, r);
        }
        pub fn finish(s: *const State, cx: Ctx) !Value {
            const r = s.row orelse return .null;
            var buf: [2]Value = undefined;
            return agg_args.decode(r, &buf).firstAs(cx.scratch, cx.out);
        }
    };
}

/// An aggregate that holds its packed rows until the end: `fin` makes its answer of
/// them, in input order, a lane's after the lane before.
pub fn Collect(comptime fin: fn ([]const []const u8, Ctx) anyerror!Value) type {
    return struct {
        pub const In = []const u8;
        pub const needs_alloc = true;
        const List = std.array_list.Managed([]const u8);
        pub const State = struct { rows: ?*List = null };
        pub fn add(s: *State, x: []const u8, cx: Ctx) !void {
            const l = s.rows orelse blk: {
                const p = try cx.alloc.create(List);
                p.* = List.init(cx.alloc);
                s.rows = p;
                break :blk p;
            };
            try l.append(try cx.alloc.dupe(u8, x));
        }
        pub fn merge(d: *State, s: *const State, cx: Ctx) !void {
            if (s.rows) |l| for (l.items) |x| try add(d, x, cx);
        }
        pub fn own(s: *State, cx: Ctx) !void {
            const src = s.rows orelse return;
            s.rows = null;
            for (src.items) |x| try add(s, x, cx);
        }
        pub fn finish(s: *const State, cx: Ctx) !Value {
            const l = s.rows orelse return .null;
            return fin(l.items, cx);
        }
    };
}

/// Packed rows read back and put in their sort keys' order, ties in input order.
fn sortedRows(rows: []const []const u8, a: std.mem.Allocator) ![]agg_args.Row {
    const out = try a.alloc(agg_args.Row, rows.len);
    for (rows, out) |r, *o| o.* = agg_args.decode(r, try a.alloc(Value, agg_args.valueCount(r)));
    if (out.len > 0 and out[0].keys.len > 0) std.sort.block(agg_args.Row, out, {}, struct {
        fn lt(_: void, x: agg_args.Row, y: agg_args.Row) bool {
            for (x.keys, 0..) |flag, i| {
                const code = flag - 'a';
                const desc = code & 1 != 0;
                const nulls_first = code & 2 != 0;
                const p = x.vals[x.leading + i];
                const q = y.vals[y.leading + i];
                if (p == .null or q == .null) {
                    if (p == .null and q == .null) continue;
                    return (p == .null) == nulls_first;
                }
                const o = eval.compareValues(p, q) orelse .eq;
                if (o == .eq) continue;
                return (o == .lt) != desc;
            }
            return false;
        }
    }.lt);
    return out;
}

/// ARRAY_AGG: every value, NULLs too, as a list.
pub fn finishList(rows: []const []const u8, cx: Ctx) anyerror!Value {
    const sorted = try sortedRows(rows, cx.scratch);
    const elem = cx.out.elem.?.*;
    var b = column.Builder.init(cx.scratch, elem);
    for (sorted) |r| try b.append(try r.firstAs(cx.scratch, elem));
    return column.oneList(cx.scratch, cx.out, try b.finish());
}

/// STRING_AGG: the values that are not NULL as text, a value's own separator before it.
pub fn finishString(rows: []const []const u8, cx: Ctx) anyerror!Value {
    const sorted = try sortedRows(rows, cx.scratch);
    var out = std.array_list.Managed(u8).init(cx.scratch);
    var any = false;
    for (sorted) |r| {
        if (r.vals[0] == .null) continue;
        if (any and r.vals[1] != .null) try out.appendSlice(try fmtnum.valueToString(cx.scratch, r.vals[1]));
        try out.appendSlice(try fmtnum.valueToString(cx.scratch, r.vals[0]));
        any = true;
    }
    return if (any) .{ .string = out.items } else .null;
}

/// BOOL_AND (`all`) or BOOL_OR; NULL over no values.
pub fn BoolFold(comptime all: bool) type {
    return struct {
        pub const In = bool;
        pub const State = struct { n: i64 = 0, t: i64 = 0 };
        pub fn add(s: *State, x: bool, _: Ctx) !void {
            s.n += 1;
            s.t += @intFromBool(x);
        }
        pub fn remove(s: *State, x: bool, _: Ctx) !void {
            s.n -= 1;
            s.t -= @intFromBool(x);
        }
        pub fn merge(d: *State, s: *const State, _: Ctx) !void {
            d.n += s.n;
            d.t += s.t;
        }
        pub fn finish(s: *const State, _: Ctx) !Value {
            if (s.n == 0) return .null;
            return .{ .bool = if (all) s.t == s.n else s.t > 0 };
        }
    };
}

pub const BitOp = enum { @"and", @"or", xor };

pub fn Bits(comptime op: BitOp) type {
    return struct {
        pub const In = i64;
        pub const State = struct { n: i64 = 0, v: i64 = 0 };
        fn fold(a: i64, b: i64) i64 {
            return switch (op) {
                .@"and" => a & b,
                .@"or" => a | b,
                .xor => a ^ b,
            };
        }
        pub fn add(s: *State, x: i64, _: Ctx) !void {
            s.v = if (s.n == 0) x else fold(s.v, x);
            s.n += 1;
        }
        pub const remove = if (op == .xor) removeXor else {};
        fn removeXor(s: *State, x: i64, _: Ctx) !void {
            s.v ^= x;
            s.n -= 1;
        }
        pub fn merge(d: *State, s: *const State, _: Ctx) !void {
            if (s.n == 0) return;
            d.v = if (d.n == 0) s.v else fold(d.v, s.v);
            d.n += s.n;
        }
        pub fn finish(s: *const State, _: Ctx) !Value {
            return if (s.n == 0) .null else .{ .int = s.v };
        }
    };
}

/// A group's count, mean and sum of squared deviations.
pub const Moments = MomentsOf(2);

/// The central moments of a group to `order` (2 or 4): its count, mean and sums of
/// the deviations' powers, `m3` and `m4` only at order 4.
pub fn MomentsOf(comptime order_: u8) type {
    const F = if (order_ >= 4) f64 else void;
    return struct {
        const Self = @This();
        n: f64 = 0,
        mean: f64 = 0,
        m2: f64 = 0,
        m3: F = if (order_ >= 4) 0 else {},
        m4: F = if (order_ >= 4) 0 else {},

        pub fn of(x: f64) Self {
            return .{ .n = 1, .mean = x };
        }

        /// The pairwise merge of two groups' moments: Chan et al.'s to the second,
        /// Pebay's to the fourth.
        pub fn merge(a: Self, b: Self) Self {
            if (a.n == 0) return b;
            if (b.n == 0) return a;
            const n = a.n + b.n;
            const d = b.mean - a.mean;
            const ab = a.n * b.n;
            var out = Self{ .n = n, .mean = a.mean + d * b.n / n, .m2 = a.m2 + b.m2 + d * d * ab / n };
            if (order_ >= 4) {
                const d2 = d * d;
                out.m3 = a.m3 + b.m3 + d2 * d * ab * (a.n - b.n) / (n * n) + 3 * d * (a.n * b.m2 - b.n * a.m2) / n;
                out.m4 = a.m4 + b.m4 + d2 * d2 * ab * (a.n * a.n - ab + b.n * b.n) / (n * n * n) +
                    6 * d2 * (a.n * a.n * b.m2 + b.n * b.n * a.m2) / (n * n) + 4 * d * (a.n * b.m3 - b.n * a.m3) / n;
            }
            return out;
        }
    };
}

/// SKEWNESS (adjusted Fisher-Pearson, G1) or KURTOSIS (excess, G2) of a group's
/// moments, the sample statistics DuckDB, Excel and pandas give: NULL below 3 or 4
/// values, or when every value is the same.
pub fn shapeOf(func: ast.AggFunc, m: MomentsOf(4)) Value {
    const n = m.n;
    if (n < (if (func == .skewness) @as(f64, 3) else 4) or !(m.m2 > 0)) return .null;
    if (func == .skewness) {
        const g1 = @sqrt(n) * m.m3 / std.math.pow(f64, m.m2, 1.5);
        return .{ .float = g1 * @sqrt(n * (n - 1)) / (n - 2) };
    }
    const g2 = n * m.m4 / (m.m2 * m.m2) - 3;
    return .{ .float = ((n + 1) * g2 + 6) * (n - 1) / ((n - 2) * (n - 3)) };
}

/// SKEWNESS or KURTOSIS, by the moments to the fourth.
pub fn Shape(comptime func: ast.AggFunc) type {
    return struct {
        pub const In = f64;
        pub const State = MomentsOf(4);
        pub fn add(s: *State, x: f64, _: Ctx) !void {
            s.* = State.merge(s.*, State.of(x));
        }
        pub fn merge(d: *State, s: *const State, _: Ctx) !void {
            d.* = State.merge(d.*, s.*);
        }
        pub fn finish(s: *const State, _: Ctx) !Value {
            return shapeOf(func, s.*);
        }
    };
}

/// PRODUCT: the values multiplied, a FLOAT; NULL over none.
pub const Product = struct {
    pub const In = f64;
    pub const State = struct { n: i64 = 0, p: f64 = 1 };
    pub fn add(s: *State, x: f64, _: Ctx) !void {
        s.n += 1;
        s.p *= x;
    }
    pub fn merge(d: *State, s: *const State, _: Ctx) !void {
        d.n += s.n;
        d.p *= s.p;
    }
    pub fn finish(s: *const State, _: Ctx) !Value {
        return if (s.n == 0) .null else .{ .float = s.p };
    }
};

/// MODE (the most frequent value, the least of those tied) or ENTROPY (`-sum p ln
/// p` over each value's share, in nats): the values held as `percentile_disc` holds
/// them, sorted at the end, so neither answer depends on the order rows came in.
pub fn Tally(comptime T: type, comptime func: ast.AggFunc) type {
    return struct {
        pub const In = T;
        pub const needs_alloc = true;
        const H = Holding(T, keepOf(T));
        pub const State = H.State;
        pub const add = H.add;
        pub const merge = H.merge;
        pub const own = H.own;
        pub fn finish(s: *const State, cx: Ctx) !Value {
            const h = s.vals orelse return .null;
            if (h.n == 0) return .null;
            const xs = try h.gather(cx.scratch);
            std.mem.sort(T, xs, {}, struct {
                fn lt(_: void, a: T, b: T) bool {
                    return order(T, a, b) == .lt;
                }
            }.lt);
            const total: f64 = @floatFromInt(xs.len);
            var best: usize = 0;
            var best_run: usize = 0;
            var ent: f64 = 0;
            var i: usize = 0;
            while (i < xs.len) {
                var j = i + 1;
                while (j < xs.len and order(T, xs[i], xs[j]) == .eq) j += 1;
                const run = j - i;
                if (run > best_run) {
                    best = i;
                    best_run = run;
                }
                const share = @as(f64, @floatFromInt(run)) / total;
                ent -= share * @log(share);
                i = j;
            }
            if (func == .entropy) return .{ .float = ent };
            return toValue(T, xs[best], cx.out);
        }
    };
}

/// A variance or standard deviation (`func`) of `n` values with squared deviations
/// `m2`: NULL below two values for the sample statistics and below one for the
/// population ones. A negative `m2` is rounding and reads 0; a NaN stays NaN.
pub fn spreadOf(func: ast.AggFunc, n: i64, m2: f64) Value {
    const samp = func == .var_samp or func == .stddev_samp;
    if (n < @as(i64, if (samp) 2 else 1)) return .null;
    const variance = (if (m2 < 0) 0 else m2) / @as(f64, @floatFromInt(if (samp) n - 1 else n));
    return .{ .float = switch (func) {
        .stddev_samp, .stddev_pop => @sqrt(variance),
        else => variance,
    } };
}

/// Exact sums of x and x² over INT values, so a variance over ints is exact (a run
/// of equal values reads 0 however many rows passed) until they leave i128, when
/// the float moments stand in.
pub const IntSquares = struct {
    s1: i128 align(8) = 0,
    s2: i128 align(8) = 0,
    overflow: bool = false,

    pub fn add(q: *IntSquares, v: i64, sub: bool) void {
        const x: i128 = v;
        const sq = std.math.mul(i128, x, x) catch {
            q.overflow = true;
            return;
        };
        q.s1 = std.math.add(i128, q.s1, if (sub) -x else x) catch blk: {
            q.overflow = true;
            break :blk q.s1;
        };
        q.s2 = std.math.add(i128, q.s2, if (sub) -sq else sq) catch blk: {
            q.overflow = true;
            break :blk q.s2;
        };
    }

    pub fn merge(d: *IntSquares, s: IntSquares) void {
        d.overflow = d.overflow or s.overflow;
        d.s1 = std.math.add(i128, d.s1, s.s1) catch blk: {
            d.overflow = true;
            break :blk d.s1;
        };
        d.s2 = std.math.add(i128, d.s2, s.s2) catch blk: {
            d.overflow = true;
            break :blk d.s2;
        };
    }

    /// The squared deviations of `n` values, or null once the sums overflowed.
    pub fn m2(q: IntSquares, n: i64) ?f64 {
        if (q.overflow or n <= 0) return null;
        const nn: i128 = n;
        const a = std.math.mul(i128, nn, q.s2) catch return null;
        const b = std.math.mul(i128, q.s1, q.s1) catch return null;
        return @as(f64, @floatFromInt(a - b)) / @as(f64, @floatFromInt(nn));
    }
};

/// VAR_SAMP, VAR_POP, STDDEV_SAMP or STDDEV_POP by Welford's update, and over ints
/// (`exact`) by `IntSquares` while they hold.
pub fn Spread(comptime func: ast.AggFunc, comptime exact: bool) type {
    return struct {
        pub const In = if (exact) i64 else f64;
        pub const State = struct { n: i64 = 0, mean: f64 = 0, m2: f64 = 0, sq: if (exact) IntSquares else void = if (exact) .{} else {} };
        pub fn add(s: *State, v: In, _: Ctx) !void {
            const x: f64 = if (exact) @floatFromInt(v) else v;
            if (exact) s.sq.add(v, false);
            s.n += 1;
            const delta = x - s.mean;
            s.mean += delta / @as(f64, @floatFromInt(s.n));
            s.m2 += delta * (x - s.mean);
        }
        pub fn merge(d: *State, s: *const State, _: Ctx) !void {
            if (s.n == 0) return;
            const m = Moments.merge(
                .{ .n = @floatFromInt(d.n), .mean = d.mean, .m2 = d.m2 },
                .{ .n = @floatFromInt(s.n), .mean = s.mean, .m2 = s.m2 },
            );
            d.mean = m.mean;
            d.m2 = m.m2;
            d.n += s.n;
            if (exact) d.sq.merge(s.sq);
        }
        pub fn finish(s: *const State, _: Ctx) !Value {
            const m2 = if (exact) (s.sq.m2(s.n) orelse s.m2) else s.m2;
            return spreadOf(func, s.n, m2);
        }
    };
}

/// Quickselect: `xs[k]` becomes the k-th smallest, smaller before, larger after.
/// Median-of-three pivot, Hoare partition; expected O(n). NaN orders above every
/// number, as `floatord.orderF64` has it, so the NaNs are moved to the end first.
pub fn selectNth(xs_all: []f64, k: usize) f64 {
    var m: usize = 0;
    for (xs_all) |x| {
        if (std.math.isNan(x)) continue;
        xs_all[m] = x;
        m += 1;
    }
    @memset(xs_all[m..], std.math.nan(f64));
    if (k >= m) return std.math.nan(f64);
    return selectBy(f64, xs_all[0..m], k);
}

/// Quickselect over values of `T` in `order`'s order (no NaN among floats).
pub fn selectBy(comptime T: type, xs: []T, k: usize) T {
    const L = struct {
        inline fn lt(a: T, b: T) bool {
            return if (T == f64) a < b else order(T, a, b) == .lt;
        }
    };
    var lo: usize = 0;
    var hi: usize = xs.len - 1;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (L.lt(xs[mid], xs[lo])) std.mem.swap(T, &xs[mid], &xs[lo]);
        if (L.lt(xs[hi], xs[lo])) std.mem.swap(T, &xs[hi], &xs[lo]);
        if (L.lt(xs[hi], xs[mid])) std.mem.swap(T, &xs[hi], &xs[mid]);
        const p = xs[mid];
        var i = lo;
        var j = hi;
        while (i <= j) {
            while (L.lt(xs[i], p)) i += 1;
            while (L.lt(p, xs[j])) j -= 1;
            if (i <= j) {
                std.mem.swap(T, &xs[i], &xs[j]);
                i += 1;
                if (j == 0) break;
                j -= 1;
            }
        }
        if (k <= j) {
            hi = j;
        } else if (k >= i) {
            lo = i;
        } else {
            return xs[k];
        }
    }
    return xs[k];
}

test "string MAX over ascending input keeps one buffer, not a copy per improvement" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const cx = Ctx{ .out = types.Type.init(.string), .ty = types.Type.init(.string), .alloc = ar.allocator(), .scratch = ar.allocator() };
    const M = Extreme([]const u8, .gt);
    var st: M.State = .{};
    var buf: [64]u8 = undefined;
    for (0..20_000) |i| try M.add(&st, try std.fmt.bufPrint(&buf, "row-{d:0>40}-{d}", .{ i, i % 7 }), cx);
    try std.testing.expectEqualStrings("row-0000000000000000000000000000000000019999-0", st.v);
    try std.testing.expect(ar.queryCapacity() < 16 * 1024);

    const N = Extreme([]const u8, .lt);
    var mn: N.State = .{};
    for ([_][]const u8{ "m", "", "zz", "" }) |x| try N.add(&mn, x, cx);
    try std.testing.expectEqualStrings("", mn.v);
}

test "untyped string MAX over ascending input keeps one buffer; an owned copy stays apart" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const cx = Ctx{ .out = types.Type.init(.string), .ty = types.Type.init(.string), .alloc = ar.allocator(), .scratch = ar.allocator() };
    const M = ExtremeValue(.gt);
    var st: M.State = .{};
    var buf: [64]u8 = undefined;
    for (0..20_000) |i| try M.add(&st, .{ .string = try std.fmt.bufPrint(&buf, "row-{d:0>40}-{d}", .{ i, i % 7 }) }, cx);
    try std.testing.expectEqualStrings("row-0000000000000000000000000000000000019999-0", st.v.string);
    try std.testing.expect(ar.queryCapacity() < 16 * 1024);

    var copy = st;
    try M.own(&copy, cx);
    try M.add(&st, .{ .string = "s" }, cx);
    try std.testing.expectEqualStrings("s", st.v.string);
    try std.testing.expectEqualStrings("row-0000000000000000000000000000000000019999-0", copy.v.string);
    try M.add(&copy, .{ .string = "t" }, cx);
    try std.testing.expectEqualStrings("t", copy.v.string);
    try std.testing.expectEqualStrings("s", st.v.string);
}
