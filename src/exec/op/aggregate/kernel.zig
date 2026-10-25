//! The aggregate factory: `make(Op)` turns an aggregate's math (`funcs.zig`) into a
//! `Kernel` holding every loop the operator runs, chosen once per aggregate by
//! `kernelFor`. A loop is generated once per lane of column storages the op reads
//! alike (`read.Lane`), with a tight path for a batch with no nulls (the op's
//! `addAll` where it has one) and a validity-checked one otherwise; every kind the
//! op cannot take directly shares one lane, read value by value and cast. The
//! grouped loop adds row `r` into the state at `tails[r] + off`, skipping a row with
//! no group (one sent to a spill file). An op taking DECIMALs that sets `fold_expr`
//! also gets `foldExpr`/`foldExprRows`, which take a decimal arithmetic argument's
//! values straight from its operands (`eval.decFold`), no column built between.
//!
//! `Layout` places each aggregate's state after a group's keys, so a COUNT pays
//! eight bytes and a SUM its own, and carries the per-aggregate `Ctx`. With a
//! DISTINCT aggregate a group's record starts with its ordinal in its store, which
//! names the group in the aggregate's pairs (`pairs.zig`).

const Agg = @import("../aggregate.zig").Aggregate.Agg;
const Batch = @import("../../batch.zig").Batch;
const Decimal = @import("../../value.zig").Decimal;
const Decimal64 = @import("../../value.zig").Dec64;
const Value = @import("../../value.zig").Value;
const ast = @import("../../../lang/ast.zig");
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
    finishAll: *const fn ([]const [*]const u8, usize, Ctx, *column.Builder) anyerror!void,
    own: ?*const fn ([*]u8, Ctx) anyerror!void,
    foldExpr: ?*const fn ([*]u8, *const ast.Expr, Batch, Ctx) anyerror!bool = null,
    foldExprRows: ?*const fn ([]const ?[*]u8, usize, *const ast.Expr, Batch, Rows) anyerror!bool = null,
};

fn hasAddAll(comptime Op: type) bool {
    return @hasDecl(Op, "addAll") and @TypeOf(Op.addAll) != void;
}

fn foldsExpr(comptime Op: type) bool {
    if (!@hasDecl(Op, "fold_expr") or !Op.fold_expr) return false;
    comptime std.debug.assert(Op.In == Decimal);
    return true;
}

const keys = std.enums.values(read.Key);

/// Each key's lane when read as `In`, indexed by the key.
fn lanes(comptime In: type) *const [keys.len]read.Lane {
    const L = struct {
        const of = blk: {
            var t: [keys.len]read.Lane = undefined;
            for (keys, &t) |k, *l| l.* = read.lane(In, k);
            break :blk t;
        };
    };
    return &L.of;
}

/// The key a lane's loop is generated for, its first; null for a lane no key takes
/// when read as `In`, whose loop is never generated.
fn laneKey(comptime In: type, comptime l: read.Lane) ?read.Key {
    for (keys) |k| if (read.lane(In, k) == l) return k;
    return null;
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
            switch (lanes(Op.In)[@intFromEnum(read.keyOf(c))]) {
                inline else => |l| if (comptime laneKey(Op.In, l)) |key| {
                    if (comptime read.Phys(key)) |S| if (comptime S == Op.In and hasAddAll(Op)) if (all and (S != Decimal64 or c.ty.scale == cx.ty.scale)) return Op.addAll(s, read.slice(S, c, n), cx);
                    if (all) {
                        for (0..n) |i| if (try read.atValid(Op.In, key, c, i, cx)) |x| try Op.add(s, x, cx);
                    } else for (0..n) |i| if (try read.at(Op.In, key, c, i, cx)) |x| try Op.add(s, x, cx);
                } else unreachable,
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
            switch (lanes(Op.In)[@intFromEnum(read.keyOf(c))]) {
                inline else => |l| if (comptime laneKey(Op.In, l)) |key| {
                    for (tails, 0..) |t, r| {
                        const tp = t orelse continue;
                        const cx = if (needs_alloc) rows.at(r) else rows.cx;
                        const x = (if (all) try read.atValid(Op.In, key, c, r, cx) else try read.at(Op.In, key, c, r, cx)) orelse continue;
                        try Op.add(st(tp + off), x, cx);
                    }
                } else unreachable,
            }
        }

        fn foldExpr(p: [*]u8, e: *const ast.Expr, b: Batch, cx: Ctx) anyerror!bool {
            const Sink = struct {
                s: *Op.State,
                cx: Ctx,
                pub inline fn put(self: @This(), _: usize, d: Decimal) eval.EvalError!void {
                    try @call(.always_inline, Op.add, .{ self.s, d, self.cx });
                }
            };
            return eval.decFold(cx.scratch, e, b, Sink{ .s = st(p), .cx = cx });
        }

        fn foldExprRows(tails: []const ?[*]u8, off: usize, e: *const ast.Expr, b: Batch, rows: Rows) anyerror!bool {
            const needs_alloc = @hasDecl(Op, "needs_alloc") and Op.needs_alloc;
            const Sink = struct {
                tails: []const ?[*]u8,
                off: usize,
                rows: Rows,
                pub inline fn put(self: @This(), r: usize, d: Decimal) eval.EvalError!void {
                    const tp = self.tails[r] orelse return;
                    try @call(.always_inline, Op.add, .{ st(tp + self.off), d, if (needs_alloc) self.rows.at(r) else self.rows.cx });
                }
            };
            return eval.decFold(rows.cx.scratch, e, b, Sink{ .tails = tails, .off = off, .rows = rows });
        }

        fn merge(d: [*]u8, s: [*]const u8, cx: Ctx) anyerror!void {
            return Op.merge(st(d), cst(s), cx);
        }
        fn finish(p: [*]const u8, cx: Ctx) anyerror!Value {
            return Op.finish(cst(p), cx);
        }
        fn finishAll(tails: []const [*]const u8, off: usize, cx: Ctx, out: *column.Builder) anyerror!void {
            for (tails) |t| try out.append(try Op.finish(cst(t + off), cx));
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
        .finishAll = K.finishAll,
        .own = if (@hasDecl(Op, "own")) K.own else null,
        .foldExpr = if (foldsExpr(Op)) K.foldExpr else null,
        .foldExprRows = if (foldsExpr(Op)) K.foldExprRows else null,
    };
}

fn kernelOf(comptime Op: type) *const Kernel {
    const S = struct {
        const k = make(Op);
    };
    return &S.k;
}

fn extreme(comptime want: std.math.Order, ty: types.Type) *const Kernel {
    return switch (read.keyOfType(ty)) {
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
            .decimal => if (narrowArg(arg, agg.ty)) .{ .k = kernelOf(funcs.SumDec64), .ty = arg } else .{ .k = kernelOf(funcs.SumDecimal), .ty = agg.ty },
            .float => .{ .k = kernelOf(funcs.SumFloat), .ty = agg.ty },
            else => .{ .k = kernelOf(funcs.SumInt), .ty = agg.ty },
        },
        .avg => switch (arg.kind) {
            .int => .{ .k = kernelOf(funcs.AvgInt), .ty = arg },
            .decimal => if (column.isDec64(arg)) .{ .k = kernelOf(funcs.AvgDec64), .ty = arg } else .{ .k = kernelOf(funcs.AvgDecimal), .ty = arg },
            else => .{ .k = kernelOf(funcs.AvgFloat), .ty = float },
        },
        .min => .{ .k = if (agg.ty.unknown) kernelOf(funcs.ExtremeValue(.lt)) else extreme(.lt, agg.ty), .ty = agg.ty },
        .max => .{ .k = if (agg.ty.unknown) kernelOf(funcs.ExtremeValue(.gt)) else extreme(.gt, agg.ty), .ty = agg.ty },
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

/// Whether a DECIMAL SUM can add its argument as `dec64` values: the argument is
/// declared one, at the result's scale, so no addend needs rescaling.
fn narrowArg(arg: types.Type, out: types.Type) bool {
    return column.isDec64(arg) and arg.scale == out.scale;
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

    /// Aggregate `j`'s result for every record of `tails`, in order, into `out`.
    pub fn finishAll(self: Layout, tails: []const [*]const u8, j: usize, out: *column.Builder) !void {
        return self.kernels[j].finishAll(tails, self.offs[j], self.ctxs[j], out);
    }
};
