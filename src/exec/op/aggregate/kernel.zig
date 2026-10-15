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
//! eight bytes and a SUM its own, and carries the per-aggregate `Ctx`. With a
//! DISTINCT aggregate a group's record starts with its ordinal in its store, which
//! names the group in the aggregate's pairs (`pairs.zig`).

const Agg = @import("../aggregate.zig").Aggregate.Agg;
const Decimal = @import("../../value.zig").Decimal;
const Value = @import("../../value.zig").Value;
const column = @import("../../column.zig");
const eval = @import("../../eval.zig");
const funcs = @import("funcs.zig");
const read = @import("read.zig");
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
};

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
            try Op.add(st(p), try read.fromValue(Op.In, v, cx), cx);
        }

        fn fold(p: [*]u8, col: ?column.Column, n: usize, cx: Ctx) anyerror!void {
            const s = st(p);
            if (Op.In == void) return Op.addN(s, if (col) |c| simd.popcountValid(c.validity.bits, n) else n);
            const c = col.?;
            const all = c.validity.allSet(n);
            switch (c.ty.kind) {
                inline else => |kind| {
                    if (comptime read.Phys(kind)) |S| if (comptime S == Op.In and hasAddAll(Op)) if (all) return Op.addAll(s, read.slice(S, c, n), cx);
                    if (all) {
                        for (0..n) |i| if (try read.atValid(Op.In, kind, c, i, cx)) |x| try Op.add(s, x, cx);
                    } else for (0..n) |i| if (try read.at(Op.In, kind, c, i, cx)) |x| try Op.add(s, x, cx);
                },
            }
        }

        fn foldRows(tails: []const ?[*]u8, off: usize, col: ?column.Column, rows: Rows) anyerror!void {
            const needs_alloc = @hasDecl(Op, "needs_alloc") and Op.needs_alloc;
            const c = col orelse {
                if (Op.In != void) unreachable;
                for (tails) |t| if (t) |tp| try Op.add(st(tp + off), {}, rows.cx);
                return;
            };
            const all = c.validity.allSet(tails.len);
            switch (c.ty.kind) {
                inline else => |kind| for (tails, 0..) |t, r| {
                    const tp = t orelse continue;
                    const cx = if (needs_alloc) rows.at(r) else rows.cx;
                    const x = (if (all) try read.atValid(Op.In, kind, c, r, cx) else try read.at(Op.In, kind, c, r, cx)) orelse continue;
                    try Op.add(st(tp + off), x, cx);
                },
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
    comptime std.debug.assert(@alignOf(Op.State) <= 8);
    return .{
        .size = std.mem.alignForward(usize, @sizeOf(Op.State), 8),
        .init = K.init,
        .add = K.add,
        .fold = K.fold,
        .foldRows = K.foldRows,
        .merge = K.merge,
        .finish = K.finish,
        .own = if (@hasDecl(Op, "own")) K.own else null,
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
        inline else => |k| if (comptime read.Phys(k)) |T| kernelOf(funcs.Extreme(T, want)) else kernelOf(funcs.ExtremeValue(want)),
    };
}

/// An aggregate's kernel and the type its values are read as, from the declared
/// types: the state never changes type when one batch's column does.
pub fn kernelFor(agg: Agg) struct { k: *const Kernel, ty: types.Type } {
    const arg = agg.arg_ty orelse agg.ty;
    const float = types.Type.init(.float);
    return switch (agg.func) {
        .count => .{ .k = kernelOf(funcs.Count), .ty = agg.ty },
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
        .var_samp => if (arg.kind == .int) .{ .k = kernelOf(funcs.Spread(.var_samp, true)), .ty = arg } else .{ .k = kernelOf(funcs.Spread(.var_samp, false)), .ty = float },
        .var_pop => if (arg.kind == .int) .{ .k = kernelOf(funcs.Spread(.var_pop, true)), .ty = arg } else .{ .k = kernelOf(funcs.Spread(.var_pop, false)), .ty = float },
        .stddev_samp => if (arg.kind == .int) .{ .k = kernelOf(funcs.Spread(.stddev_samp, true)), .ty = arg } else .{ .k = kernelOf(funcs.Spread(.stddev_samp, false)), .ty = float },
        .stddev_pop => if (arg.kind == .int) .{ .k = kernelOf(funcs.Spread(.stddev_pop, true)), .ty = arg } else .{ .k = kernelOf(funcs.Spread(.stddev_pop, false)), .ty = float },
    };
}

pub const Layout = struct {
    kernels: []const *const Kernel,
    ctxs: []const Ctx,
    offs: []const usize,
    size: usize,
    ord: bool,

    pub fn init(a: std.mem.Allocator, aggs: []const Agg) !Layout {
        const kernels = try a.alloc(*const Kernel, aggs.len);
        const ctxs = try a.alloc(Ctx, aggs.len);
        const offs = try a.alloc(usize, aggs.len);
        var ord = false;
        for (aggs) |agg| ord = ord or agg.distinct;
        var at: usize = if (ord) @sizeOf(u64) else 0;
        for (aggs, kernels, ctxs, offs) |agg, *k, *cx, *o| {
            const kf = kernelFor(agg);
            k.* = kf.k;
            cx.* = .{ .out = agg.ty, .ty = kf.ty, .alloc = a, .scratch = a };
            o.* = at;
            at += kf.k.size;
        }
        return .{ .kernels = kernels, .ctxs = ctxs, .offs = offs, .size = at, .ord = ord };
    }

    pub fn setOrd(self: Layout, tail: [*]u8, i: usize) void {
        if (self.ord) @as(*u64, @ptrCast(@alignCast(tail))).* = i;
    }

    pub fn ordOf(_: Layout, tail: [*]const u8) usize {
        return @intCast(@as(*const u64, @ptrCast(@alignCast(tail))).*);
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
