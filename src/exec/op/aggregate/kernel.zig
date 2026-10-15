//! The aggregate factory: `make(Op)` turns an aggregate's math (`funcs.zig`) into a
//! `Kernel` holding every loop the operator runs, chosen once per aggregate by
//! `kernelFor`. A loop is generated for each physical column type an argument can
//! arrive in, with a tight path for a batch with no nulls (the op's `addAll` where
//! it has one) and a validity-checked one otherwise; a column whose type the op
//! cannot take directly is read value by value and cast to the op's type. The
//! grouped loop adds row `r` into the state at `tails[r] + off`, skipping a row with
//! no group (one sent to a spill file).
//!
//! `Layout` places each aggregate's state after a group's keys, so a COUNT pays
//! eight bytes and a SUM its own, and carries the per-aggregate `Ctx`.

const Agg = @import("../aggregate.zig").Aggregate.Agg;
const Decimal = @import("../../value.zig").Decimal;
const Value = @import("../../value.zig").Value;
const column = @import("../../column.zig");
const eval = @import("../../eval.zig");
const funcs = @import("funcs.zig");
const simd = @import("../../simd.zig");
const std = @import("std");
const types = @import("../../../lang/types.zig");

pub const Ctx = funcs.Ctx;

/// A grouped batch's rows: each row's group allocator is `allocs[pidx[r]]`, or
/// the context's own when the fold has one store.
pub const Rows = struct {
    cx: Ctx,
    allocs: []const std.mem.Allocator = &.{},
    pidx: []const u8 = &.{},

    inline fn at(self: Rows, r: usize) Ctx {
        if (self.allocs.len == 0) return self.cx;
        var cx = self.cx;
        cx.alloc = self.allocs[self.pidx[r]];
        return cx;
    }
};

pub const Kernel = struct {
    size: usize,
    init: *const fn ([*]u8) void,
    add: *const fn ([*]u8, Value, Ctx) anyerror!void,
    fold: *const fn ([*]u8, ?column.Column, usize, Ctx) anyerror!void,
    foldRows: *const fn ([]const ?[*]u8, usize, ?column.Column, Rows) anyerror!void,
    merge: *const fn ([*]u8, [*]const u8, Ctx) anyerror!void,
    finish: *const fn ([*]const u8, Ctx) anyerror!Value,
    own: ?*const fn ([*]u8, Ctx) anyerror!void,
    own_always: bool,
};

/// The physical type a column of `kind` holds, or null for one read only as values.
fn Phys(comptime kind: types.TypeKind) ?type {
    return switch (kind) {
        .int, .time, .timestamp => i64,
        .date => i32,
        .float => f64,
        .decimal => Decimal,
        .bool => bool,
        .string, .bytes => []const u8,
        .array, .@"struct" => null,
    };
}

inline fn get(comptime S: type, c: column.Column, i: usize) S {
    return switch (S) {
        i64 => c.data.i64[i],
        i32 => c.data.i32[i],
        f64 => c.data.f64[i],
        Decimal => c.data.dec[i],
        bool => c.data.b[i],
        []const u8 => c.data.bytes.at(i),
        else => unreachable,
    };
}

fn slice(comptime S: type, c: column.Column, n: usize) []const S {
    return switch (S) {
        i64 => c.data.i64[0..n],
        f64 => c.data.f64[0..n],
        Decimal => c.data.dec[0..n],
        else => unreachable,
    };
}

/// Whether a value of physical type `S` converts to `In` without a cast.
fn converts(comptime S: type, comptime In: type) bool {
    if (S == In) return true;
    return (In == f64 and (S == i64 or S == Decimal)) or (In == Decimal and S == i64);
}

inline fn conv(comptime S: type, comptime In: type, x: S) In {
    if (S == In) return x;
    return switch (In) {
        f64 => if (S == i64) @floatFromInt(x) else x.toF64(),
        Decimal => .{ .unscaled = x, .scale = 0 },
        else => unreachable,
    };
}

/// `v` as an `In`, through a cast to the type the op reads when it is not one already.
fn fromValue(comptime In: type, v: Value, cx: Ctx) !In {
    if (In == Value) return v;
    switch (In) {
        i64 => switch (v) {
            .int, .time, .timestamp => |x| return x,
            else => {},
        },
        i32 => if (v == .date) return v.date,
        f64 => switch (v) {
            .float, .int, .decimal => return eval.toF64(v),
            else => {},
        },
        Decimal => if (eval.asDecimal(v)) |d| return d,
        bool => if (v == .bool) return v.bool,
        []const u8 => switch (v) {
            .string, .bytes => |s| return s,
            else => {},
        },
        else => unreachable,
    }
    const c = try eval.castValueTyped(cx.scratch, v, cx.ty);
    if (c.isNull()) return error.CastFailed;
    return fromValue(In, c, cx);
}

fn hasAddAll(comptime Op: type) bool {
    return @hasDecl(Op, "addAll") and @TypeOf(Op.addAll) != void;
}

pub fn make(comptime Op: type) Kernel {
    const K = struct {
        fn st(p: [*]u8) *Op.State {
            return @ptrCast(@alignCast(p));
        }
        fn cst(p: [*]const u8) *const Op.State {
            return @ptrCast(@alignCast(p));
        }
        fn init(p: [*]u8) void {
            st(p).* = .{};
        }

        fn add(p: [*]u8, v: Value, cx: Ctx) anyerror!void {
            if (v.isNull()) return;
            if (Op.In == void) return Op.add(st(p), {}, cx);
            try Op.add(st(p), try fromValue(Op.In, v, cx), cx);
        }

        fn fold(p: [*]u8, col: ?column.Column, n: usize, cx: Ctx) anyerror!void {
            const s = st(p);
            if (Op.In == void) return Op.addN(s, if (col) |c| simd.popcountValid(c.validity.bits, n) else n);
            const c = col.?;
            const all = c.validity.allSet(n);
            if (Op.In != Value) switch (c.ty.kind) {
                inline else => |kind| if (comptime Phys(kind)) |S| if (comptime converts(S, Op.In)) {
                    if (comptime S == Op.In and hasAddAll(Op)) if (all) return Op.addAll(s, slice(S, c, n), cx);
                    for (0..n) |i| {
                        if (!all and !c.validity.get(i)) continue;
                        try Op.add(s, conv(S, Op.In, get(S, c, i)), cx);
                    }
                    return;
                },
            };
            for (0..n) |i| {
                const v = c.getValue(i);
                if (v.isNull()) continue;
                try Op.add(s, try fromValue(Op.In, v, cx), cx);
            }
        }

        fn foldRows(tails: []const ?[*]u8, off: usize, col: ?column.Column, rows: Rows) anyerror!void {
            const needs_alloc = @hasDecl(Op, "needs_alloc") and Op.needs_alloc;
            if (Op.In == void) {
                const c = col orelse {
                    for (tails) |t| if (t) |tp| try Op.add(st(tp + off), {}, rows.cx);
                    return;
                };
                for (tails, 0..) |t, r| if (t) |tp| if (c.validity.get(r)) try Op.add(st(tp + off), {}, rows.cx);
                return;
            }
            const c = col.?;
            const all = c.validity.allSet(tails.len);
            if (Op.In != Value) switch (c.ty.kind) {
                inline else => |kind| if (comptime Phys(kind)) |S| if (comptime converts(S, Op.In)) {
                    for (tails, 0..) |t, r| {
                        const tp = t orelse continue;
                        if (!all and !c.validity.get(r)) continue;
                        try Op.add(st(tp + off), conv(S, Op.In, get(S, c, r)), if (needs_alloc) rows.at(r) else rows.cx);
                    }
                    return;
                },
            };
            for (tails, 0..) |t, r| {
                const tp = t orelse continue;
                const v = c.getValue(r);
                if (v.isNull()) continue;
                const cx = rows.at(r);
                try Op.add(st(tp + off), try fromValue(Op.In, v, cx), cx);
            }
        }

        fn merge(d: [*]u8, s: [*]const u8, cx: Ctx) anyerror!void {
            return Op.merge(st(d), cst(s), cx);
        }
        fn finish(p: [*]const u8, cx: Ctx) anyerror!Value {
            return Op.finish(cst(p), cx);
        }
        fn own(p: [*]u8, cx: Ctx) anyerror!void {
            return Op.own(st(p), cx);
        }
    };
    return .{
        .size = std.mem.alignForward(usize, @sizeOf(Op.State), 8),
        .init = K.init,
        .add = K.add,
        .fold = K.fold,
        .foldRows = K.foldRows,
        .merge = K.merge,
        .finish = K.finish,
        .own = if (@hasDecl(Op, "own")) K.own else null,
        .own_always = @hasDecl(Op, "own_always") and Op.own_always,
    };
}

fn kernelOf(comptime Op: type) *const Kernel {
    const S = struct {
        const k = make(Op);
    };
    return &S.k;
}

fn extreme(comptime want: std.math.Order, kind: types.TypeKind) *const Kernel {
    return switch (kind) {
        inline else => |k| if (comptime Phys(k)) |T| kernelOf(funcs.Extreme(T, want)) else kernelOf(funcs.ExtremeValue(want)),
    };
}

/// An aggregate's kernel and the type its values are read as, from the declared
/// types: the state never changes type when one batch's column does.
pub fn kernelFor(agg: Agg) struct { k: *const Kernel, ty: types.Type } {
    const arg = agg.arg_ty orelse agg.ty;
    const float = types.Type.init(.float);
    return switch (agg.func) {
        .count => .{ .k = if (agg.distinct) kernelOf(funcs.CountDistinct) else kernelOf(funcs.Count), .ty = agg.ty },
        .sum => switch (agg.ty.kind) {
            .decimal => .{ .k = kernelOf(funcs.SumDecimal), .ty = agg.ty },
            .float => .{ .k = kernelOf(funcs.SumFloat), .ty = agg.ty },
            else => .{ .k = kernelOf(funcs.SumInt), .ty = agg.ty },
        },
        .avg => switch (arg.kind) {
            .int => .{ .k = kernelOf(funcs.AvgInt), .ty = arg },
            .decimal => .{ .k = kernelOf(funcs.AvgDecimal), .ty = arg },
            else => .{ .k = kernelOf(funcs.AvgFloat), .ty = float },
        },
        .min => .{ .k = if (agg.ty.unknown) kernelOf(funcs.ExtremeValue(.lt)) else extreme(.lt, agg.ty.kind), .ty = agg.ty },
        .max => .{ .k = if (agg.ty.unknown) kernelOf(funcs.ExtremeValue(.gt)) else extreme(.gt, agg.ty.kind), .ty = agg.ty },
        .median => .{ .k = kernelOf(funcs.Median), .ty = float },
        .count_if => .{ .k = kernelOf(funcs.CountIf), .ty = types.Type.init(.bool) },
        .bool_and => .{ .k = kernelOf(funcs.BoolFold(true)), .ty = types.Type.init(.bool) },
        .bool_or => .{ .k = kernelOf(funcs.BoolFold(false)), .ty = types.Type.init(.bool) },
        .bit_and => .{ .k = kernelOf(funcs.Bits(.@"and")), .ty = types.Type.init(.int) },
        .bit_or => .{ .k = kernelOf(funcs.Bits(.@"or")), .ty = types.Type.init(.int) },
        .bit_xor => .{ .k = kernelOf(funcs.Bits(.xor)), .ty = types.Type.init(.int) },
        .var_samp => .{ .k = kernelOf(funcs.Spread(.var_samp)), .ty = float },
        .var_pop => .{ .k = kernelOf(funcs.Spread(.var_pop)), .ty = float },
        .stddev_samp => .{ .k = kernelOf(funcs.Spread(.stddev_samp)), .ty = float },
        .stddev_pop => .{ .k = kernelOf(funcs.Spread(.stddev_pop)), .ty = float },
    };
}

pub const Layout = struct {
    kernels: []const *const Kernel,
    ctxs: []const Ctx,
    offs: []const usize,
    size: usize,

    pub fn init(a: std.mem.Allocator, aggs: []const Agg) !Layout {
        const kernels = try a.alloc(*const Kernel, aggs.len);
        const ctxs = try a.alloc(Ctx, aggs.len);
        const offs = try a.alloc(usize, aggs.len);
        var at: usize = 0;
        for (aggs, kernels, ctxs, offs) |agg, *k, *cx, *o| {
            const kf = kernelFor(agg);
            k.* = kf.k;
            cx.* = .{ .out = agg.ty, .ty = kf.ty, .alloc = a, .scratch = a };
            o.* = at;
            at += kf.k.size;
        }
        return .{ .kernels = kernels, .ctxs = ctxs, .offs = offs, .size = at };
    }

    pub fn clear(self: Layout, tail: [*]u8) void {
        for (self.kernels, self.offs) |k, o| k.init(tail + o);
    }

    /// The context of aggregate `j` with `alloc` as its group allocator.
    pub fn ctx(self: Layout, j: usize, alloc: std.mem.Allocator) Ctx {
        var cx = self.ctxs[j];
        cx.alloc = alloc;
        return cx;
    }

    pub fn merge(self: Layout, alloc: std.mem.Allocator, dst: [*]u8, src: [*]const u8, j: usize) !void {
        try self.kernels[j].merge(dst + self.offs[j], src + self.offs[j], self.ctx(j, alloc));
    }

    pub fn finish(self: Layout, tail: [*]const u8, j: usize) !Value {
        return self.kernels[j].finish(tail + self.offs[j], self.ctxs[j]);
    }
};
