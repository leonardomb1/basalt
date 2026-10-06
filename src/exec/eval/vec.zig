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
const TypeCtx = @import("../eval.zig").TypeCtx;
const compareValues = @import("support.zig").compareValues;
const evalLit = @import("testing_util.zig").evalLit;
const parser = @import("../../lang/sql_parser.zig");
const vectorized = @import("fn_vec.zig").vectorized;

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

test "constEval folds an expression over plan-time bindings" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var tbl = ast.Expr{ .field = .{ .parts = &[_][]const u8{"tbl"} } };
    var prefix = ast.Expr{ .str_lit = "SD1" };
    var sw_args = [_]*ast.Expr{ &tbl, &prefix };
    var sw = ast.Expr{ .call = .{ .name = "starts_with", .args = &sw_args } };
    const r = try constEval(a, &sw, &[_][]const u8{"tbl"}, &[_]Value{.{ .string = "SD1010" }});
    try std.testing.expect(r.bool);

    var four = ast.Expr{ .int_lit = 4 };
    var two = ast.Expr{ .int_lit = 2 };
    var ss_args = [_]*ast.Expr{ &tbl, &four, &two };
    var ss = ast.Expr{ .call = .{ .name = "substr", .args = &ss_args } };
    const e = try constEval(a, &ss, &[_][]const u8{"tbl"}, &[_]Value{.{ .string = "SD1010" }});
    try std.testing.expectEqualStrings("01", e.string);
}

test "type-check and evaluate an if-expression with 3VL" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const pred = try parser.parseExprStr(a, "amount > 100", &diag);
    const sel = try parser.parseExprStr(a, "if(amount >= 100, 'yes', 'no')", &diag);

    const schema = types.Schema{ .fields = &.{.{ .name = "amount", .ty = Type.init(.int).asNullable() }} };
    var ctx = TypeCtx{ .schema = schema, .arena = a };
    try std.testing.expectEqual(types.TypeKind.bool, (try ctx.typeOf(pred)).kind);
    const sel_ty = try ctx.typeOf(sel);
    try std.testing.expectEqual(types.TypeKind.string, sel_ty.kind);

    const amt = try column.intColumn(a, &.{ 50, 150, null });
    var cols = [_]column.Column{amt};
    const batch = Batch{ .schema = &schema, .columns = &cols, .len = 3 };

    const out = try evalColumn(a, sel, batch, sel_ty);
    try std.testing.expectEqualStrings("no", out.getValue(0).string);
    try std.testing.expectEqualStrings("yes", out.getValue(1).string);
    try std.testing.expectEqualStrings("no", out.getValue(2).string);
}

test "a date column compares on the vectorized path, and matches rowwise" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const days = [_]?i32{ 9130, 9131, 9132, null };
    var validity = try column.Bitmap.initFull(a, days.len);
    const store = try a.alloc(i32, days.len);
    for (days, 0..) |d, i| {
        if (d) |x| store[i] = x else {
            store[i] = 0;
            validity.setValid(i, false);
        }
    }
    const dcol = column.Column{
        .ty = Type.init(.date).asNullable(),
        .len = days.len,
        .validity = validity,
        .data = .{ .i32 = store },
    };
    const schema = types.Schema{ .fields = &.{.{ .name = "d", .ty = Type.init(.date).asNullable() }} };
    var cols = [_]column.Column{dcol};
    const batch = Batch{ .schema = &schema, .columns = &cols, .len = days.len };

    const sqlp = @import("../../lang/sql_parser.zig");
    const cases = [_]struct { src: []const u8, want: [4]?bool }{
        .{ .src = "d < '1995-01-01'", .want = .{ true, false, false, null } },
        .{ .src = "d > '1995-01-01'", .want = .{ false, false, true, null } },
        .{ .src = "d <= '1995-01-01'", .want = .{ true, true, false, null } },
        .{ .src = "d >= '1995-01-01'", .want = .{ false, true, true, null } },
        .{ .src = "d = '1995-01-01'", .want = .{ false, true, false, null } },
        .{ .src = "d <> '1995-01-01'", .want = .{ true, false, true, null } },
    };
    for (cases) |tc| {
        var diag: sqlp.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        const e = try sqlp.parseExprStr(a, tc.src, &diag);
        _ = evalVecNode(a, e, batch) catch |err| {
            std.debug.print("expr de-vectorized: {s}: {s}\n", .{ tc.src, @errorName(err) });
            return err;
        };
        const out = try evalColumn(a, e, batch, Type.init(.bool).asNullable());
        for (tc.want, 0..) |w, i| {
            const got = out.getValue(i);
            if (w) |b| {
                try std.testing.expectEqual(b, got.bool);
            } else {
                try std.testing.expect(got.isNull());
            }
            const rw = try evalRow(a, e, batch, i);
            if (w) |b| try std.testing.expectEqual(b, rw.bool) else try std.testing.expect(rw.isNull());
        }
    }

    var d2: sqlp.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const bad = try sqlp.parseExprStr(a, "d < 'not-a-date'", &d2);
    try std.testing.expectError(error.TypeMismatch, evalColumn(a, bad, batch, Type.init(.bool).asNullable()));
}

test "vectorized kernels match the rowwise evaluator" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const x = try column.intColumn(a, &.{ 10, 20, null, 40, 0 });
    const y = try column.intColumn(a, &.{ 3, null, 7, 8, 5 });
    const schema = types.Schema{ .fields = &.{
        .{ .name = "x", .ty = Type.init(.int).asNullable() },
        .{ .name = "y", .ty = Type.init(.int).asNullable() },
    } };
    var cols = [_]column.Column{ x, y };
    const batch = Batch{ .schema = &schema, .columns = &cols, .len = 5 };

    const exprs = [_]struct { src: []const u8, vectorized: bool }{
        .{ .src = "x + y", .vectorized = true },
        .{ .src = "x * y - 1", .vectorized = true },
        .{ .src = "x / y", .vectorized = true },
        .{ .src = "x > y", .vectorized = true },
        .{ .src = "x >= 10 and y < 8", .vectorized = true },
        .{ .src = "x == 40 or y == 5", .vectorized = true },
        .{ .src = "if(x > y, x, y)", .vectorized = true },
        .{ .src = "-x", .vectorized = true },
        .{ .src = "x is null", .vectorized = true },
        .{ .src = "if(x != 0, y / x, 0)", .vectorized = false },
        .{ .src = "x != 0 and y / x > 1", .vectorized = false },
        .{ .src = "x == 0 or y / x > 1", .vectorized = false },
    };
    for (exprs) |tc| {
        const body = tc.src;
        var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        const e = try parser.parseExprStr(a, body, &diag);
        var ctx = TypeCtx{ .schema = schema, .arena = a };
        const ty = try ctx.typeOf(e);

        if (tc.vectorized) {
            _ = evalVecNode(a, e, batch) catch |err| {
                std.debug.print("expr de-vectorized: {s}: {s}\n", .{ body, @errorName(err) });
                return err;
            };
        }

        const vec = try evalColumn(a, e, batch, ty);
        const rowwise = try evalColumnRowwise(a, e, batch, ty);
        try std.testing.expectEqual(rowwise.len, vec.len);
        var i: usize = 0;
        while (i < vec.len) : (i += 1) {
            const want = rowwise.getValue(i);
            const got = vec.getValue(i);
            try std.testing.expectEqual(want.isNull(), got.isNull());
            if (!want.isNull()) {
                if (compareValues(want, got)) |ord| {
                    try std.testing.expect(ord == .eq);
                } else try std.testing.expect(false);
            }
        }
    }
}

test "vectorized string kernels match the rowwise evaluator" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var sb = column.Builder.init(a, Type.init(.string).asNullable());
    try sb.append(.{ .string = "  Apple " });
    try sb.append(.null);
    try sb.append(.{ .string = "banana" });
    try sb.append(.{ .string = "" });
    try sb.append(.{ .string = "Cherry pie" });
    const s = try sb.finish();
    const x = try column.intColumn(a, &.{ 1, 2, null, 4, 5 });
    const schema = types.Schema{ .fields = &.{
        .{ .name = "s", .ty = Type.init(.string).asNullable() },
        .{ .name = "x", .ty = Type.init(.int).asNullable() },
    } };
    var cols = [_]column.Column{ s, x };
    const batch = Batch{ .schema = &schema, .columns = &cols, .len = 5 };

    const exprs = [_][]const u8{
        "upper(s)",
        "lower(s)",
        "trim(s)",
        "length(s)",
        "concat(s, '-', s)",
        "starts_with(s, 'b')",
        "ends_with(s, 'e')",
        "contains(s, 'an')",
        "like(s, '%an%')",
        "substr(s, 2, 3)",
        "replace(s, 'an', 'AN')",
        "coalesce(s, 'fallback')",
        "if(contains(s, 'p'), upper(s), s)",
        "length(trim(s)) > 5 and contains(s, 'e')",
    };
    for (exprs) |body| {
        var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        const e = try parser.parseExprStr(a, body, &diag);
        var ctx = TypeCtx{ .schema = schema, .arena = a };
        const ty = try ctx.typeOf(e);

        _ = evalVecNode(a, e, batch) catch |err| {
            std.debug.print("expr de-vectorized: {s}: {s}\n", .{ body, @errorName(err) });
            return err;
        };

        const vec = try evalColumn(a, e, batch, ty);
        const rowwise = try evalColumnRowwise(a, e, batch, ty);
        try std.testing.expectEqual(rowwise.len, vec.len);
        var i: usize = 0;
        while (i < vec.len) : (i += 1) {
            const want = rowwise.getValue(i);
            const got = vec.getValue(i);
            try std.testing.expectEqual(want.isNull(), got.isNull());
            if (!want.isNull()) {
                if (compareValues(want, got)) |ord| {
                    try std.testing.expect(ord == .eq);
                } else try std.testing.expect(false);
            }
        }
    }
}

test "bitwise operators and hex builtins" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const schema = types.Schema{ .fields = &.{
        .{ .name = "x", .ty = Type.init(.int).asNullable() },
        .{ .name = "s", .ty = Type.init(.string) },
    } };
    const x = try column.intColumn(a, &.{ -8, null });
    var sb = column.Builder.init(a, Type.init(.string));
    try sb.append(.{ .string = "0xFF" });
    try sb.append(.{ .string = "ff" });
    var cols = [_]column.Column{ x, try sb.finish() };
    const batch = Batch{ .schema = &schema, .columns = &cols, .len = 2 };

    const S = struct {
        fn checked(al: std.mem.Allocator, sch: types.Schema, src: []const u8) !struct { *ast.Expr, Type } {
            var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
            const e = try parser.parseExprStr(al, src, &diag);
            var ctx = TypeCtx{ .schema = sch, .arena = al };
            return .{ e, try ctx.typeOf(e) };
        }
    };

    const ints = [_]struct { src: []const u8, want: i64 }{
        .{ .src = "1 | 2 & 3", .want = 3 },
        .{ .src = "6 & 3", .want = 2 },
        .{ .src = "6 ^ 3", .want = 5 },
        .{ .src = "1 + 1 << 2", .want = 8 },
        .{ .src = "~0", .want = -1 },
        .{ .src = "~x", .want = 7 },
        .{ .src = "x >> 1", .want = -4 },
        .{ .src = "1 << 63 >> 63", .want = -1 },
        .{ .src = "1 << 64", .want = 0 },
        .{ .src = "8 << -1", .want = 0 },
        .{ .src = "8 >> 100", .want = 0 },
        .{ .src = "x >> 100", .want = -1 },
        .{ .src = "x >> -1", .want = 0 },
        .{ .src = "bit_count(255)", .want = 8 },
        .{ .src = "bit_count(~0)", .want = 64 },
        .{ .src = "bit_count(0)", .want = 0 },
        .{ .src = "from_hex('ff')", .want = 255 },
        .{ .src = "from_hex('0xFF')", .want = 255 },
        .{ .src = "from_hex(s)", .want = 255 },
        .{ .src = "from_hex(to_hex(x))", .want = -8 },
        .{ .src = "from_hex(to_hex(0))", .want = 0 },
    };
    for (ints) |c| {
        const e, const t = try S.checked(a, schema, c.src);
        try std.testing.expectEqual(types.TypeKind.int, t.kind);
        const col = try evalColumn(a, e, batch, t);
        try std.testing.expectEqual(c.want, col.getValue(0).int);
        try std.testing.expectEqual(c.want, (try evalRow(a, e, batch, 0)).int);
    }

    const hex = [_]struct { src: []const u8, want: []const u8 }{
        .{ .src = "to_hex(255)", .want = "ff" },
        .{ .src = "to_hex(0)", .want = "0" },
        .{ .src = "to_hex(-1)", .want = "ffffffffffffffff" },
        .{ .src = "to_hex(x)", .want = "fffffffffffffff8" },
    };
    for (hex) |c| {
        const e, const t = try S.checked(a, schema, c.src);
        try std.testing.expectEqual(types.TypeKind.string, t.kind);
        const col = try evalColumn(a, e, batch, t);
        try std.testing.expectEqualStrings(c.want, col.getValue(0).string);
    }

    const nulls = [_][]const u8{ "x & 1", "x | 1", "x ^ 1", "x << 1", "x >> 1", "~x", "bit_count(x)", "to_hex(x)", "from_hex(to_hex(x))" };
    for (nulls) |src| {
        const e, const t = try S.checked(a, schema, src);
        try std.testing.expect(t.nullable);
        try std.testing.expect((try evalColumn(a, e, batch, t)).getValue(1).isNull());
        try std.testing.expect((try evalRow(a, e, batch, 1)).isNull());
    }

    {
        const pair = try S.checked(a, schema, "x & 1");
        try std.testing.expectError(error.Unsupported, evalVecNode(a, pair[0], batch));
        const v = try evalVec(a, pair[0], batch);
        try std.testing.expect(v == .col and v.col.ty.kind == .int);
        try std.testing.expectEqual(@as(i64, 0), v.col.getValue(0).int);
        try std.testing.expect(v.col.getValue(1).isNull());
    }

    for ([_][]const u8{ "from_hex('zz')", "from_hex('')", "from_hex('0x')", "from_hex('1ffffffffffffffff')" }) |src| {
        const e, const t = try S.checked(a, schema, src);
        try std.testing.expectError(error.CastFailed, evalRow(a, e, batch, 0));
        try std.testing.expectError(error.CastFailed, evalColumn(a, e, batch, t));
    }

    for ([_][]const u8{ "s & 1", "1.5 & 1", "1 << 1.5", "~s", "bit_count(s)", "to_hex(s)", "from_hex(1)" }) |src| {
        var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        const e = try parser.parseExprStr(a, src, &diag);
        var ctx = TypeCtx{ .schema = schema, .arena = a };
        try std.testing.expectError(error.TypeError, ctx.typeOf(e));
    }
}

test "int division/modulo by zero raise DivByZero; float division yields inf" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const schema = types.Schema{ .fields = &.{.{ .name = "x", .ty = Type.init(.int).asNullable() }} };
    const x = try column.intColumn(a, &.{ 6, null });
    var cols = [_]column.Column{x};
    const batch = Batch{ .schema = &schema, .columns = &cols, .len = 2 };

    var fx = ast.Expr{ .field = .{ .parts = &[_][]const u8{"x"} } };
    var zero = ast.Expr{ .int_lit = 0 };
    var div = ast.Expr{ .binary = .{ .op = .div, .l = &fx, .r = &zero } };
    var mod = ast.Expr{ .binary = .{ .op = .mod, .l = &fx, .r = &zero } };
    try std.testing.expectError(error.DivByZero, evalColumn(a, &div, batch, Type.init(.int).asNullable()));
    try std.testing.expectError(error.DivByZero, evalRow(a, &div, batch, 0));
    try std.testing.expectError(error.DivByZero, evalRow(a, &mod, batch, 0));

    var fzero = ast.Expr{ .float_lit = 0.0 };
    var fdiv = ast.Expr{ .binary = .{ .op = .div, .l = &fx, .r = &fzero } };
    const out = try evalColumn(a, &fdiv, batch, Type.init(.float).asNullable());
    try std.testing.expect(std.math.isInf(out.getValue(0).float));
    try std.testing.expect(out.getValue(1).isNull());
}

test "evalColumn over an empty batch yields an empty column" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const schema = types.Schema{ .fields = &.{.{ .name = "x", .ty = Type.init(.int) }} };
    const x = try column.intColumn(a, &.{});
    var cols = [_]column.Column{x};
    const batch = Batch{ .schema = &schema, .columns = &cols, .len = 0 };

    var fx = ast.Expr{ .field = .{ .parts = &[_][]const u8{"x"} } };
    var one = ast.Expr{ .int_lit = 1 };
    var plus = ast.Expr{ .binary = .{ .op = .add, .l = &fx, .r = &one } };
    const out = try evalColumn(a, &plus, batch, Type.init(.int));
    try std.testing.expectEqual(@as(usize, 0), out.len);
}

test "try_cast yields null exactly where cast raises" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    try std.testing.expectEqual(@as(i64, 3), (try evalLit(a, "try_cast('3' as int)")).int);
    try std.testing.expect((try evalLit(a, "try_cast('x' as int)")).isNull());
    try std.testing.expectError(error.CastFailed, evalLit(a, "cast('x' as int)"));

    var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const e = try parser.parseExprStr(a, "try_cast(s as int)", &diag);
    const schema = types.Schema{ .fields = &.{.{ .name = "s", .ty = Type.init(.string) }} };
    var ctx = TypeCtx{ .schema = schema, .arena = a };
    const ty = try ctx.typeOf(e);
    try std.testing.expectEqual(types.TypeKind.int, ty.kind);
    try std.testing.expect(ty.nullable);

    var sb = column.Builder.init(a, Type.init(.string));
    try sb.append(.{ .string = "3" });
    try sb.append(.{ .string = "x" });
    var cols = [_]column.Column{try sb.finish()};
    const batch = Batch{ .schema = &schema, .columns = &cols, .len = 2 };
    const out = try evalColumn(a, e, batch, ty);
    try std.testing.expectEqual(@as(i64, 3), out.getValue(0).int);
    try std.testing.expect(out.getValue(1).isNull());
}
