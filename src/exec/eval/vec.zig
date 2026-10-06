//! The vectorized evaluator: an expression over a whole batch at once, as typed
//! column kernels. What a kernel cannot take (an error in a branch not taken, a
//! function with no vector form) falls back to the row evaluator for that node.

const Batch = @import("../batch.zig").Batch;
const Decimal = @import("../value.zig").Decimal;
const EvalError = @import("../eval.zig").EvalError;
const Type = @import("../../lang/types.zig").Type;
const Value = @import("../value.zig").Value;
const asDecimal = @import("row.zig").asDecimal;
const ast = @import("../../lang/ast.zig");
const castValue = @import("cast.zig").castValue;
const cmpResult = @import("row.zig").cmpResult;
const column = @import("../column.zig");
const decimalArithType = @import("row.zig").decimalArithType;
const decimalOp = @import("row.zig").decimalOp;
const evalRow = @import("row.zig").evalRow;
const fieldIndex = @import("support.zig").fieldIndex;
const forgetFailure = @import("support.zig").forgetFailure;
const intDiv = @import("row.zig").intDiv;
const intRem = @import("row.zig").intRem;
const lookupBuiltin = @import("functions.zig").lookupBuiltin;
const orderF64 = @import("support.zig").orderF64;
const parseIsoDate = @import("time.zig").parseIsoDate;
const parseIsoTimestamp = @import("time.zig").parseIsoTimestamp;
const std = @import("std");
const toF64 = @import("support.zig").toF64;
const trim = @import("support.zig").trim;
const types = @import("../../lang/types.zig");

pub fn evalColumn(arena: std.mem.Allocator, expr: *const ast.Expr, batch: Batch, out_ty: Type) EvalError!column.Column {
    forgetFailure();
    const v = evalVec(arena, expr, batch) catch |e| switch (e) {
        error.Unsupported => return evalColumnRowwise(arena, expr, batch, out_ty),
        error.CastFailed => return error.CastFailed,
        error.DivByZero => return error.DivByZero,
        error.TypeMismatch => return error.TypeMismatch,
        error.IntOverflow => return error.IntOverflow,
        error.PatternTooComplex => return error.PatternTooComplex,
        error.InvalidJson => return error.InvalidJson,
        error.OutOfMemory => return error.OutOfMemory,
    };
    return switch (v) {
        .col => |c| c,
        .scalar => |s| broadcastScalar(arena, s, out_ty, batch.len),
    };
}

pub fn evalColumnRowwise(arena: std.mem.Allocator, expr: *const ast.Expr, batch: Batch, out_ty: Type) EvalError!column.Column {
    var ty = out_ty;
    if (ty.unknown) ty = Type.init(.string).asNullable();
    var b = column.Builder.init(arena, ty);
    var i: usize = 0;
    while (i < batch.len) : (i += 1) {
        try b.append(try evalRow(arena, expr, batch, i));
    }
    return b.finish();
}

const Column = column.Column;

pub const Bitmap = column.Bitmap;

pub const VecError = error{ Unsupported, CastFailed, DivByZero, TypeMismatch, IntOverflow, PatternTooComplex, InvalidJson, OutOfMemory };

pub const Vec = union(enum) {
    col: Column,
    scalar: Value,
};

pub const Num = union(enum) {
    icol: struct { d: []const i64, v: Bitmap },
    fcol: struct { d: []const f64, v: Bitmap },
    iscalar: i64,
    fscalar: f64,
};

pub const Str = union(enum) {
    col: struct { d: column.Bytes, v: Bitmap },
    scalar: []const u8,
};

const BoolOp = union(enum) {
    col: struct { d: []const bool, v: Bitmap },
    scalar: ?bool,
};

pub fn evalVec(arena: std.mem.Allocator, expr: *const ast.Expr, batch: Batch) VecError!Vec {
    return evalVecNode(arena, expr, batch) catch |e| switch (e) {
        error.Unsupported => rowwiseVec(arena, expr, batch),
        else => e,
    };
}

pub fn evalVecNode(arena: std.mem.Allocator, expr: *const ast.Expr, batch: Batch) VecError!Vec {
    switch (expr.*) {
        .null_lit => return .{ .scalar = .null },
        .bool_lit => |b| return .{ .scalar = .{ .bool = b } },
        .int_lit => |i| return .{ .scalar = .{ .int = i } },
        .float_lit => |f| return .{ .scalar = .{ .float = f } },
        .str_lit => |s| return .{ .scalar = .{ .string = s } },
        .field => |q| {
            const idx = fieldIndex(batch.schema.*, q) orelse return error.TypeMismatch;
            return .{ .col = batch.columns[idx] };
        },
        .unary => |u| return unaryVec(arena, u, batch),
        .is_null => |n| return isNullVec(arena, n, batch),
        .binary => |b| return binaryVec(arena, b, batch),
        .cast => |c| return castVec(arena, c, batch),
        .cond => |c| return condVec(arena, c, batch),
        .call => |c| return callVec(arena, c, batch),
        .match => |m| return matchVec(arena, m, batch),
        .let_in, .lambda, .lambda_var => return error.Unsupported,
    }
}

/// Row-wise evaluation of one node as a column, typed by widening every non-null
/// value's type (the first value's alone made the answer depend on where a batch
/// began). Kinds with no common type decline.
fn rowwiseVec(arena: std.mem.Allocator, expr: *const ast.Expr, batch: Batch) VecError!Vec {
    const n = batch.len;
    const vals = try arena.alloc(Value, n);
    var ty: ?Type = null;
    for (vals, 0..) |*v, i| {
        v.* = evalRow(arena, expr, batch, i) catch return error.Unsupported;
        const vt: Type = switch (v.*) {
            .null => continue,
            .bool => Type.init(.bool),
            .int => Type.init(.int),
            .float => Type.init(.float),
            .string => Type.init(.string),
            .bytes => Type.init(.bytes),
            .date => Type.init(.date),
            .time => Type.init(.time),
            .timestamp => Type.init(.timestamp),
            .decimal => |d| Type.decimal(0, d.scale),
        };
        ty = if (ty) |t| Type.unify(t, vt) orelse return error.Unsupported else vt;
    }
    const t = ty orelse return .{ .scalar = .null };
    var b = column.Builder.init(arena, t.asNullable());
    for (vals) |v| try b.append(v);
    return .{ .col = try b.finish() };
}

/// CASE, both forms: each arm's condition becomes a row mask, folded back to
/// front over the default so an earlier arm's rows override a later one's.
fn matchVec(arena: std.mem.Allocator, m: ast.Match, batch: Batch) VecError!Vec {
    const n = batch.len;
    const takes = try arena.alloc([]bool, m.arms.len);
    var narms: usize = 0;
    var acc: Vec = .{ .scalar = .null };
    for (m.arms) |arm| {
        if (arm.is_default) {
            acc = try evalVecLazy(arena, arm.value, batch);
            break;
        }
        const take = try arena.alloc(bool, n);
        @memset(take, false);
        if (m.subject) |se| {
            for (arm.pats) |p| {
                var same = ast.Expr{ .binary = .{ .op = .eq, .l = se, .r = p } };
                try orInto(take, try evalVec(arena, &same, batch));
            }
        } else {
            try orInto(take, try evalVec(arena, arm.guard.?, batch));
        }
        takes[narms] = take;
        narms += 1;
    }
    var i = narms;
    while (i > 0) {
        i -= 1;
        acc = try pickVec(arena, takes[i], try evalVecLazy(arena, m.arms[i].value, batch), acc);
    }
    return acc;
}

fn orInto(take: []bool, c: Vec) VecError!void {
    switch (c) {
        .scalar => |s| if (s == .bool and s.bool) {
            @memset(take, true);
        } else if (s != .bool and s != .null) return error.Unsupported,
        .col => |cc| {
            if (cc.ty.kind != .bool) return error.Unsupported;
            for (take, 0..) |*x, i| x.* = x.* or (cc.validity.get(i) and cc.data.b[i]);
        },
    }
}

/// Per row, `t` where `take[i]` else `e`. A null scalar on either side is an
/// all-null column of the other's type, so `IF(c, x, NULL)` stays vectorized.
fn pickVec(arena: std.mem.Allocator, take: []const bool, t: Vec, e: Vec) VecError!Vec {
    const n = take.len;
    const tn = t == .scalar and t.scalar.isNull();
    const en = e == .scalar and e.scalar.isNull();
    if (tn and en) return .{ .scalar = .null };
    const tc = if (tn) null else (try realize(arena, t, n)) orelse return error.Unsupported;
    const ec = if (en) null else (try realize(arena, e, n)) orelse return error.Unsupported;
    const tcol = tc orelse try broadcastScalar(arena, .null, ec.?.ty.asNullable(), n);
    const ecol = ec orelse try broadcastScalar(arena, .null, tc.?.ty.asNullable(), n);
    if (tcol.ty.kind != ecol.ty.kind) return error.Unsupported;
    return mergeCols(arena, take, tcol, ecol);
}

pub fn strArg(arena: std.mem.Allocator, e: *const ast.Expr, batch: Batch) VecError!Str {
    const v = try evalVec(arena, e, batch);
    return asStr(v) orelse error.Unsupported;
}

fn callVec(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
    const b = lookupBuiltin(c.name) orelse return error.Unsupported;
    const f = b.vec_fn orelse return error.Unsupported;
    return f(arena, c, batch);
}

fn unaryVec(arena: std.mem.Allocator, u: ast.Expr.Unary, batch: Batch) VecError!Vec {
    const v = try evalVec(arena, u.e, batch);
    switch (u.op) {
        .neg => switch (v) {
            .scalar => |s| return .{ .scalar = switch (s) {
                .null => .null,
                .int => |x| .{ .int = std.math.negate(x) catch return error.IntOverflow },
                .float => |x| .{ .float = -x },
                .decimal => |d| .{ .decimal = .{ .unscaled = -d.unscaled, .scale = d.scale } },
                else => return error.Unsupported,
            } },
            .col => |c| {
                const n = c.len;
                switch (c.ty.kind) {
                    .int => {
                        const out = try arena.alloc(i64, n);
                        for (c.data.i64, 0..) |x, i| {
                            out[i] = if (c.validity.get(i)) std.math.negate(x) catch return error.IntOverflow else 0;
                        }
                        return mkCol(c.ty, n, c.validity, .{ .i64 = out });
                    },
                    .float => {
                        const out = try arena.alloc(f64, n);
                        for (c.data.f64, 0..) |x, i| out[i] = -x;
                        return mkCol(c.ty, n, c.validity, .{ .f64 = out });
                    },
                    else => return error.Unsupported,
                }
            },
        },
        .not => switch (v) {
            .scalar => |s| return .{ .scalar = if (s.isNull()) .null else .{ .bool = !s.bool } },
            .col => |c| {
                if (c.ty.kind != .bool) return error.Unsupported;
                const n = c.len;
                const out = try arena.alloc(bool, n);
                for (c.data.b, 0..) |x, i| out[i] = !x;
                return mkCol(c.ty, n, c.validity, .{ .b = out });
            },
        },
        .bit_not => return error.Unsupported,
    }
}

fn isNullVec(arena: std.mem.Allocator, n: ast.Expr.IsNull, batch: Batch) VecError!Vec {
    if (n.kind == .is_empty) return error.Unsupported;
    const v = try evalVec(arena, n.e, batch);
    switch (v) {
        .scalar => |s| {
            const r = s.isNull();
            return .{ .scalar = .{ .bool = if (n.negated) !r else r } };
        },
        .col => |c| {
            const rows = c.len;
            const out = try arena.alloc(bool, rows);
            var i: usize = 0;
            while (i < rows) : (i += 1) {
                const isn = !c.validity.get(i);
                out[i] = if (n.negated) !isn else isn;
            }
            const bm = try Bitmap.initFull(arena, rows);
            return mkCol(Type.init(.bool), rows, bm, .{ .b = out });
        },
    }
}

/// Bitwise ops are refused here and run row-wise. A date column against an ISO
/// literal is tried as a temporal comparison before the string path.
fn binaryVec(arena: std.mem.Allocator, b: ast.Expr.Binary, batch: Batch) VecError!Vec {
    switch (b.op) {
        .@"and", .@"or" => return boolOpVec(arena, b.op, b.l, b.r, batch),
        .bit_and, .bit_or, .bit_xor, .shl, .shr => return error.Unsupported,
        .add, .sub, .mul, .div, .mod => {
            const l = try evalVec(arena, b.l, batch);
            const r = try evalVec(arena, b.r, batch);
            if (scalarNull(l) or scalarNull(r)) return .{ .scalar = .null };
            if (try decOpVec(arena, b.op, l, r, batch.len)) |v| return v;
            const ln = (try asNum(arena, l, batch.len)) orelse return error.Unsupported;
            const rn = (try asNum(arena, r, batch.len)) orelse return error.Unsupported;
            return numOpVec(arena, b.op, ln, rn, batch.len);
        },
        .eq, .ne, .lt, .le, .gt, .ge => {
            const l = try evalVec(arena, b.l, batch);
            const r = try evalVec(arena, b.r, batch);
            if (scalarNull(l) or scalarNull(r)) return .{ .scalar = .null };
            if (try asNum(arena, l, batch.len)) |ln| {
                if (try asNum(arena, r, batch.len)) |rn| return numOpVec(arena, b.op, ln, rn, batch.len);
            }
            if (try temporalPair(arena, l, r, batch.len)) |tp| {
                return numOpVec(arena, b.op, tp[0], tp[1], batch.len);
            }
            if (try dictCmpVec(arena, b.op, l, r, batch.len)) |v| return v;
            if (asStr(l)) |ls| {
                if (asStr(r)) |rs| return cmpStrVec(arena, b.op, ls, rs, batch.len);
            }
            return error.Unsupported;
        },
    }
}

fn numOpVec(arena: std.mem.Allocator, op: ast.BinOp, l: Num, r: Num, n: usize) VecError!Vec {
    const int_lane = isIntNum(l) and isIntNum(r);
    switch (op) {
        inline .add, .sub, .mul, .div, .mod, .eq, .ne, .lt, .le, .gt, .ge => |cop| {
            return if (int_lane)
                numOpVecT(i64, cop, arena, l, r, n)
            else
                numOpVecT(f64, cop, arena, l, r, n);
        },
        else => unreachable,
    }
}

/// Applies `op` elementwise over operands widened to comptime `T`. All-valid
/// inputs skip the per-row validity checks; a null slot is zero-filled.
fn numOpVecT(comptime T: type, comptime op: ast.BinOp, arena: std.mem.Allocator, l: Num, r: Num, n: usize) VecError!Vec {
    const Out = OpOut(T, op);
    const ty = Type.init(if (Out == bool) .bool else if (T == i64) .int else .float);
    const out = try arena.alloc(Out, n);
    if (allValidNum(l, n) and allValidNum(r, n)) {
        for (0..n) |i| out[i] = try applyOp(T, op, numAt(T, l, i), numAt(T, r, i));
        return mkCol(ty, n, try Bitmap.initFull(arena, n), outData(Out, out));
    }
    var bm = try Bitmap.initFull(arena, n);
    var any: bool = false;
    var i: usize = 0;
    while (i < n) : (i += 1) {
        if (!numValid(l, i) or !numValid(r, i)) {
            out[i] = if (Out == bool) false else 0;
            bm.setValid(i, false);
            any = true;
            continue;
        }
        out[i] = try applyOp(T, op, numAt(T, l, i), numAt(T, r, i));
    }
    return mkCol(ty.withNull(any), n, bm, outData(Out, out));
}

fn OpOut(comptime T: type, comptime op: ast.BinOp) type {
    return switch (op) {
        .eq, .ne, .lt, .le, .gt, .ge => bool,
        else => T,
    };
}

/// Int div/mod raise on a zero divisor and int add/sub/mul on overflow; f64
/// compares through `orderF64`, since `std.math.order` is `unreachable` on NaN.
inline fn applyOp(comptime T: type, comptime op: ast.BinOp, a: T, d: T) VecError!OpOut(T, op) {
    return switch (op) {
        .add => if (T == i64) (std.math.add(i64, a, d) catch return error.IntOverflow) else a + d,
        .sub => if (T == i64) (std.math.sub(i64, a, d) catch return error.IntOverflow) else a - d,
        .mul => if (T == i64) (std.math.mul(i64, a, d) catch return error.IntOverflow) else a * d,
        .div => if (T == i64) intDiv(a, d) else a / d,
        .mod => if (T == i64) intRem(a, d) else @rem(a, d),
        .eq, .ne, .lt, .le, .gt, .ge => cmpResult(op, if (T == f64) orderF64(a, d) else std.math.order(a, d)),
        else => unreachable,
    };
}

inline fn outData(comptime Out: type, out: []Out) Column.Data {
    return if (Out == bool) .{ .b = out } else if (Out == i64) .{ .i64 = out } else .{ .f64 = out };
}

/// A dictionary column against a string literal: each entry compared once and the
/// answer gathered through the codes. Null when the operands are not that shape.
fn dictCmpVec(arena: std.mem.Allocator, op: ast.BinOp, l: Vec, r: Vec, n: usize) VecError!?Vec {
    const col, const lit, const flip = blk: {
        if (l == .col and r == .scalar) if (l.col.dict != null and r.scalar == .string) break :blk .{ l.col, r.scalar.string, false };
        if (r == .col and l == .scalar) if (r.col.dict != null and l.scalar == .string) break :blk .{ r.col, l.scalar.string, true };
        return null;
    };
    const d = col.dict.?;
    const per = try arena.alloc(bool, d.values.len);
    for (d.values, per) |v, *o| o.* = cmpResult(op, if (flip) std.mem.order(u8, lit, v) else std.mem.order(u8, v, lit));
    const out = try arena.alloc(bool, n);
    for (out, d.codes[0..n]) |*o, c| o.* = per[c];
    if (col.validity.allSet(n)) return mkCol(Type.init(.bool), n, try Bitmap.initFull(arena, n), .{ .b = out });
    var bm = try Bitmap.initFull(arena, n);
    for (0..n) |i| if (!col.validity.get(i)) {
        out[i] = false;
        bm.setValid(i, false);
    };
    return mkCol(Type.init(.bool).withNull(true), n, bm, .{ .b = out });
}

fn cmpStrVec(arena: std.mem.Allocator, op: ast.BinOp, l: Str, r: Str, n: usize) VecError!Vec {
    const out = try arena.alloc(bool, n);
    var bm = try Bitmap.initFull(arena, n);
    var any: bool = false;
    var i: usize = 0;
    while (i < n) : (i += 1) {
        const ls = strAt(l, i);
        const rs = strAt(r, i);
        if (ls == null or rs == null) {
            out[i] = false;
            bm.setValid(i, false);
            any = true;
            continue;
        }
        out[i] = cmpResult(op, std.mem.order(u8, ls.?, rs.?));
    }
    return mkCol(Type.init(.bool).withNull(any), n, bm, .{ .b = out });
}

/// Evaluates a subexpression the row evaluator might never need (an untaken
/// branch, a short-circuited side), demoting a value-dependent error to Unsupported.
fn evalVecLazy(arena: std.mem.Allocator, e: *const ast.Expr, batch: Batch) VecError!Vec {
    return evalVec(arena, e, batch) catch |err| switch (err) {
        error.DivByZero, error.CastFailed => error.Unsupported,
        else => err,
    };
}

fn boolOpVec(arena: std.mem.Allocator, op: ast.BinOp, le: *const ast.Expr, re: *const ast.Expr, batch: Batch) VecError!Vec {
    const lv = try evalVecLazy(arena, le, batch);
    const rv = try evalVecLazy(arena, re, batch);
    const l = asBool(lv) orelse return error.Unsupported;
    const r = asBool(rv) orelse return error.Unsupported;
    const n = batch.len;
    const out = try arena.alloc(bool, n);
    var bm = try Bitmap.initFull(arena, n);
    var any: bool = false;
    var i: usize = 0;
    while (i < n) : (i += 1) {
        const lk = boolKnown(l, i);
        const lval = boolVal(l, i);
        const rk = boolKnown(r, i);
        const rval = boolVal(r, i);
        var res: ?bool = null;
        if (op == .@"and") {
            if ((lk and !lval) or (rk and !rval)) {
                res = false;
            } else if (lk and lval and rk and rval) {
                res = true;
            }
        } else {
            if ((lk and lval) or (rk and rval)) {
                res = true;
            } else if (lk and !lval and rk and !rval) {
                res = false;
            }
        }
        if (res) |b| {
            out[i] = b;
        } else {
            out[i] = false;
            bm.setValid(i, false);
            any = true;
        }
    }
    return mkCol(Type.init(.bool).withNull(any), n, bm, .{ .b = out });
}

/// `try_cast` and any value that does not convert go to the row-wise path, which
/// yields per-row nulls or stops at the failing row and names it.
fn castVec(arena: std.mem.Allocator, c: ast.Expr.Cast, batch: Batch) VecError!Vec {
    if (c.safe) return error.Unsupported;
    const v = try evalVec(arena, c.e, batch);
    const target = c.ty.kind;
    if (target == .decimal) return error.Unsupported;
    switch (v) {
        .scalar => |s| {
            if (s.isNull()) return .{ .scalar = .null };
            return .{ .scalar = castValue(arena, s, target) catch |e| switch (e) {
                error.CastFailed => return error.Unsupported,
                else => return e,
            } };
        },
        .col => |col| return castColVec(arena, col, target, batch.len) catch |e| switch (e) {
            error.CastFailed => return error.Unsupported,
            else => return e,
        },
    }
}

/// Float to int guarding the i64 range and NaN/inf, which `@intFromFloat` treats
/// as illegal behavior; out of range is CastFailed, as for a string.
pub fn floatToInt(x: f64) error{CastFailed}!i64 {
    if (!(x >= -9223372036854775808.0 and x < 9223372036854775808.0)) return error.CastFailed;
    return @intFromFloat(x);
}

fn castColVec(arena: std.mem.Allocator, col: Column, target: types.TypeKind, n: usize) VecError!Vec {
    const src = col.ty.kind;
    if (src == target) return .{ .col = col };
    const out_ty = c: {
        var t = Type.init(target);
        t.nullable = col.ty.nullable;
        break :c t;
    };
    switch (target) {
        .int => {
            const out = try arena.alloc(i64, n);
            switch (src) {
                .float => for (col.data.f64, 0..) |x, i| {
                    out[i] = try floatToInt(x);
                },
                .bool => for (col.data.b, 0..) |x, i| {
                    out[i] = if (x) 1 else 0;
                },
                .string => for (0..n) |i| {
                    if (!col.validity.get(i)) {
                        out[i] = 0;
                        continue;
                    }
                    out[i] = std.fmt.parseInt(i64, trim(col.data.bytes.at(i)), 10) catch return error.CastFailed;
                },
                else => return error.Unsupported,
            }
            return mkCol(out_ty, n, col.validity, .{ .i64 = out });
        },
        .float => {
            const out = try arena.alloc(f64, n);
            switch (src) {
                .int => for (col.data.i64, 0..) |x, i| {
                    out[i] = @floatFromInt(x);
                },
                .string => for (0..n) |i| {
                    if (!col.validity.get(i)) {
                        out[i] = 0;
                        continue;
                    }
                    out[i] = std.fmt.parseFloat(f64, trim(col.data.bytes.at(i))) catch return error.CastFailed;
                },
                else => return error.Unsupported,
            }
            return mkCol(out_ty, n, col.validity, .{ .f64 = out });
        },
        .bool => {
            const out = try arena.alloc(bool, n);
            switch (src) {
                .int => for (col.data.i64, 0..) |x, i| {
                    out[i] = x != 0;
                },
                else => return error.Unsupported,
            }
            return mkCol(out_ty, n, col.validity, .{ .b = out });
        },
        else => return error.Unsupported,
    }
}

fn condVec(arena: std.mem.Allocator, c: ast.Expr.Cond, batch: Batch) VecError!Vec {
    const cond = try evalVec(arena, c.cond, batch);
    const tv = try evalVecLazy(arena, c.then, batch);
    const ev = try evalVecLazy(arena, c.els, batch);
    const take = try arena.alloc(bool, batch.len);
    @memset(take, false);
    try orInto(take, cond);
    return pickVec(arena, take, tv, ev);
}

fn mergeCols(arena: std.mem.Allocator, take: []const bool, t: Column, e: Column) VecError!Vec {
    const n = take.len;
    var bm = try Bitmap.initFull(arena, n);
    const data: Column.Data = switch (t.data) {
        .b => |ts| blk: {
            const o = try arena.alloc(bool, n);
            mergePick(bool, o, &bm, take, ts, e.data.b, t.validity, e.validity);
            break :blk .{ .b = o };
        },
        .i32 => |ts| blk: {
            const o = try arena.alloc(i32, n);
            mergePick(i32, o, &bm, take, ts, e.data.i32, t.validity, e.validity);
            break :blk .{ .i32 = o };
        },
        .i64 => |ts| blk: {
            const o = try arena.alloc(i64, n);
            mergePick(i64, o, &bm, take, ts, e.data.i64, t.validity, e.validity);
            break :blk .{ .i64 = o };
        },
        .f64 => |ts| blk: {
            const o = try arena.alloc(f64, n);
            mergePick(f64, o, &bm, take, ts, e.data.f64, t.validity, e.validity);
            break :blk .{ .f64 = o };
        },
        .dec => |ts| blk: {
            const o = try arena.alloc(Decimal, n);
            mergePick(Decimal, o, &bm, take, ts, e.data.dec, t.validity, e.validity);
            break :blk .{ .dec = o };
        },
        .bytes => |ts| blk: {
            break :blk .{ .bytes = try mergeBytes(arena, &bm, take, ts, e.data.bytes, t.validity, e.validity) };
        },
    };
    return .{ .col = .{ .ty = t.ty.withNull(true), .len = n, .validity = bm, .data = data } };
}

fn mergeBytes(arena: std.mem.Allocator, bm: *Bitmap, take: []const bool, ts: column.Bytes, es: column.Bytes, tv: Bitmap, ev: Bitmap) !column.Bytes {
    const n = take.len;
    var span: usize = 0;
    for (0..n) |i| span += (if (take[i]) ts.at(i) else es.at(i)).len;

    const values = try arena.alloc(u8, span);
    const offsets = try arena.alloc(i32, n + 1);
    offsets[0] = 0;
    var off: usize = 0;
    for (0..n) |i| {
        const s = if (take[i]) ts.at(i) else es.at(i);
        @memcpy(values[off..][0..s.len], s);
        off += s.len;
        offsets[i + 1] = @intCast(off);
        if (!(if (take[i]) tv.get(i) else ev.get(i))) bm.setValid(i, false);
    }
    return .{ .offsets = offsets, .values = values };
}

fn mergePick(comptime T: type, out: []T, bm: *Bitmap, take: []const bool, ts: []const T, es: []const T, tv: Bitmap, ev: Bitmap) void {
    for (0..out.len) |i| {
        if (take[i]) {
            out[i] = ts[i];
            if (!tv.get(i)) bm.setValid(i, false);
        } else {
            out[i] = es[i];
            if (!ev.get(i)) bm.setValid(i, false);
        }
    }
}

fn scalarNull(v: Vec) bool {
    return v == .scalar and v.scalar.isNull();
}

pub fn asNum(arena: std.mem.Allocator, v: Vec, n: usize) VecError!?Num {
    switch (v) {
        .scalar => |s| return switch (s) {
            .int => |x| Num{ .iscalar = x },
            .float => |x| Num{ .fscalar = x },
            .decimal => |d| Num{ .fscalar = toF64(.{ .decimal = d }) },
            else => null,
        },
        .col => |c| return switch (c.ty.kind) {
            .int => Num{ .icol = .{ .d = c.data.i64, .v = c.validity } },
            .float => Num{ .fcol = .{ .d = c.data.f64, .v = c.validity } },
            .decimal => blk: {
                const out = try arena.alloc(f64, n);
                for (c.data.dec, 0..) |d, i| out[i] = d.toF64();
                break :blk Num{ .fcol = .{ .d = out, .v = c.validity } };
            },
            else => null,
        },
    }
}

/// The exact lane for `decimalArithType`; null when the operands or the op are
/// not its case, and the float kernels take over.
fn decOpVec(arena: std.mem.Allocator, op: ast.BinOp, l: Vec, r: Vec, n: usize) VecError!?Vec {
    const ty = decimalArithType(op, vecType(l) orelse return null, vecType(r) orelse return null) orelse return null;
    const out = try arena.alloc(Decimal, n);
    var bm = try Bitmap.initFull(arena, n);
    var any = false;
    for (0..n) |i| {
        if (!vecValid(l, i) or !vecValid(r, i)) {
            out[i] = .{ .unscaled = 0, .scale = ty.scale };
            bm.setValid(i, false);
            any = true;
            continue;
        }
        out[i] = try (decimalOp(op, decAt(l, i), decAt(r, i)) orelse unreachable);
    }
    return mkCol(ty.withNull(any), n, bm, .{ .dec = out });
}

fn vecType(v: Vec) ?Type {
    return switch (v) {
        .scalar => |s| switch (s) {
            .int => Type.init(.int),
            .decimal => |d| Type.decimal(0, d.scale),
            else => null,
        },
        .col => |c| if (c.ty.kind == .int or c.ty.kind == .decimal) c.ty else null,
    };
}

inline fn vecValid(v: Vec, i: usize) bool {
    return switch (v) {
        .scalar => true,
        .col => |c| c.validity.get(i),
    };
}

inline fn decAt(v: Vec, i: usize) Decimal {
    return switch (v) {
        .scalar => |s| asDecimal(s).?,
        .col => |c| if (c.ty.kind == .decimal) c.data.dec[i] else Decimal{ .unscaled = c.data.i64[i], .scale = 0 },
    };
}

fn temporalKind(v: Vec) ?types.TypeKind {
    return switch (v) {
        .col => |c| switch (c.ty.kind) {
            .date, .time, .timestamp => c.ty.kind,
            else => null,
        },
        .scalar => |x| switch (x) {
            .date => .date,
            .time => .time,
            .timestamp => .timestamp,
            else => null,
        },
    };
}

fn temporalNum(arena: std.mem.Allocator, v: Vec, want: types.TypeKind, n: usize) VecError!?Num {
    switch (v) {
        .col => |c| switch (c.ty.kind) {
            .date => {
                const out = try arena.alloc(i64, n);
                for (c.data.i32[0..n], out) |d, *o| o.* = d;
                return Num{ .icol = .{ .d = out, .v = c.validity } };
            },
            .time, .timestamp => return Num{ .icol = .{ .d = c.data.i64, .v = c.validity } },
            else => return null,
        },
        .scalar => |x| switch (x) {
            .date => |d| return Num{ .iscalar = d },
            .time => |t| return Num{ .iscalar = t },
            .timestamp => |t| return Num{ .iscalar = t },
            .string, .bytes => |str| {
                const parsed = switch (want) {
                    .date => parseIsoDate(str),
                    .timestamp => parseIsoTimestamp(str),
                    else => null,
                } orelse return null;
                return Num{ .iscalar = parsed };
            },
            else => return null,
        },
    }
}

/// Both sides of a temporal comparison as i64 lanes (dates widened once per batch),
/// or null when they do not line up, including a date against a timestamp.
fn temporalPair(arena: std.mem.Allocator, l: Vec, r: Vec, n: usize) VecError!?[2]Num {
    const lk = temporalKind(l);
    const rk = temporalKind(r);
    const kind = lk orelse rk orelse return null;
    if (lk != null and rk != null and lk.? != rk.?) return null;
    const ln = (try temporalNum(arena, l, kind, n)) orelse return null;
    const rn = (try temporalNum(arena, r, kind, n)) orelse return null;
    return [2]Num{ ln, rn };
}

pub fn asStr(v: Vec) ?Str {
    switch (v) {
        .scalar => |s| return switch (s) {
            .string => |x| Str{ .scalar = x },
            .bytes => |x| Str{ .scalar = x },
            else => null,
        },
        .col => |c| return switch (c.ty.kind) {
            .string, .bytes => Str{ .col = .{ .d = c.data.bytes, .v = c.validity } },
            else => null,
        },
    }
}

fn asBool(v: Vec) ?BoolOp {
    switch (v) {
        .scalar => |s| return switch (s) {
            .bool => |x| BoolOp{ .scalar = x },
            .null => BoolOp{ .scalar = null },
            else => null,
        },
        .col => |c| return if (c.ty.kind == .bool) BoolOp{ .col = .{ .d = c.data.b, .v = c.validity } } else null,
    }
}

pub inline fn isIntNum(x: Num) bool {
    return x == .icol or x == .iscalar;
}

pub inline fn numI(x: Num, i: usize) i64 {
    return switch (x) {
        .icol => |c| c.d[i],
        .iscalar => |s| s,
        else => unreachable,
    };
}

inline fn numF(x: Num, i: usize) f64 {
    return switch (x) {
        .icol => |c| @floatFromInt(c.d[i]),
        .fcol => |c| c.d[i],
        .iscalar => |s| @floatFromInt(s),
        .fscalar => |s| s,
    };
}

inline fn numAt(comptime T: type, x: Num, i: usize) T {
    return if (T == i64) numI(x, i) else numF(x, i);
}

pub inline fn numValid(x: Num, i: usize) bool {
    return switch (x) {
        .icol => |c| c.v.get(i),
        .fcol => |c| c.v.get(i),
        else => true,
    };
}

inline fn allValidNum(x: Num, n: usize) bool {
    return switch (x) {
        .icol => |c| c.v.allSet(n),
        .fcol => |c| c.v.allSet(n),
        else => true,
    };
}

pub inline fn strAt(x: Str, i: usize) ?[]const u8 {
    return switch (x) {
        .col => |c| if (c.v.get(i)) c.d.at(i) else null,
        .scalar => |s| s,
    };
}

inline fn boolKnown(x: BoolOp, i: usize) bool {
    return switch (x) {
        .col => |c| c.v.get(i),
        .scalar => |s| s != null,
    };
}

inline fn boolVal(x: BoolOp, i: usize) bool {
    return switch (x) {
        .col => |c| c.d[i],
        .scalar => |s| s orelse false,
    };
}

pub fn mkCol(ty: Type, n: usize, validity: Bitmap, data: Column.Data) Vec {
    return .{ .col = .{ .ty = ty, .len = n, .validity = validity, .data = data } };
}

fn realize(arena: std.mem.Allocator, v: Vec, n: usize) VecError!?Column {
    switch (v) {
        .col => |c| return c,
        .scalar => |s| {
            const ty: Type = switch (s) {
                .bool => Type.init(.bool),
                .int => Type.init(.int),
                .float => Type.init(.float),
                .string => Type.init(.string),
                .bytes => Type.init(.bytes),
                .decimal => Type.init(.decimal),
                else => return null,
            };
            return try broadcastScalar(arena, s, ty, n);
        },
    }
}

fn broadcastScalar(arena: std.mem.Allocator, s: Value, out_ty: Type, n: usize) EvalError!Column {
    var ty = out_ty;
    if (ty.unknown) ty = Type.init(.string).asNullable();
    var b = column.Builder.init(arena, ty);
    var i: usize = 0;
    while (i < n) : (i += 1) try b.append(s);
    return b.finish();
}

/// Evaluates an expression at plan time against named scalar bindings (params,
/// loop variables) by materializing them as a one-row batch for `evalRow`.
pub fn constEval(arena: std.mem.Allocator, expr: *const ast.Expr, names: []const []const u8, values: []const Value) EvalError!Value {
    const fields = try arena.alloc(types.Schema.Field, names.len);
    const cols = try arena.alloc(column.Column, names.len);
    for (names, values, 0..) |nm, v, i| {
        const ty = scalarType(v);
        fields[i] = .{ .name = nm, .ty = ty };
        var b = column.Builder.init(arena, ty);
        try b.append(v);
        cols[i] = try b.finish();
    }
    const schema = types.Schema{ .fields = fields };
    const batch = Batch{ .schema = &schema, .columns = cols, .len = 1 };
    return evalRow(arena, expr, batch, 0);
}

fn scalarType(v: Value) Type {
    return switch (v) {
        .null => Type.init(.string).asNullable(),
        .bool => Type.init(.bool),
        .int => Type.init(.int),
        .float => Type.init(.float),
        .decimal => Type.init(.decimal),
        .string => Type.init(.string),
        .bytes => Type.init(.bytes),
        .date => Type.init(.date),
        .time => Type.init(.time),
        .timestamp => Type.init(.timestamp),
    };
}

pub fn isEmptyVal(v: Value) bool {
    return switch (v) {
        .string, .bytes => |s| s.len == 0,
        else => false,
    };
}
