//! Typed expression evaluation. `TypeCtx` resolves and type-checks an expression
//! against an input schema (filling `msg` and `span` on failure); `evalColumn`
//! evaluates an expression over a whole batch into a new column. Null handling
//! follows SQL three-valued logic: any null operand in a comparison/arithmetic
//! yields null; `and`/`or` use the 3VL truth tables; `is null` is total.
//!
//! Every scalar builtin is one `Builtin` entry (plan-time typing, row-wise
//! evaluator, optional whole-batch kernel) found by `lookupBuiltin`; the table is
//! re-exported by `exec/builtins.zig`. Check time catches what it can so a bad
//! input fails `check`, not the run: a literal regex pattern or `strftime` format
//! is compiled, and a string literal compared to a date/timestamp must be strict
//! ISO, bending to the column's type (never the reverse, so `'01/07/2013'` is an
//! error instead of a text comparison). A `$name` still present at typing time
//! names nothing and is never read as a column.
//!
//! Two evaluators. `evalVec` works on whole typed column slices with no per-row
//! `Value` boxing; a node it does not cover (bitwise ops, `try_cast`, most string
//! functions) is evaluated row-wise by itself and handed back as a column, so its
//! parent stays vectorized. The vector path is eager, so a value-dependent error
//! (zero divisor, failed cast) there is demoted to `Unsupported` and the
//! expression re-runs on the lazy row-wise `evalRow`, which raises it only for a
//! row that really takes the branch, and names that row. The row-wise evaluator
//! is the reference both paths must agree with. Temporal comparisons vectorize as
//! i64 lanes with the ISO literal parsed once per batch (row-wise they were 43x
//! slower than the same comparison on an int column).
//!
//! Numeric rules, each fixed after a wrong answer: i64 arithmetic is checked,
//! since the shipped ReleaseFast build makes overflow undefined (and `minInt / -1`
//! traps with SIGFPE); `%` takes the dividend's sign for ints, floats and
//! decimals alike; DECIMAL `+ - * %` is exact on i128 unscaled integers (`/` and
//! any float operand stay float), so `1.1 + 0.3` is not 1.4000000000000001.
//! Every decimal rounding (casts, sinks, aggregates, `round`) is half away from
//! zero, as PostgreSQL does, and a float becomes a decimal through its 15
//! significant digits, so 12.345 rounds to 12.35. Floats order totally, NaN equal
//! to itself and above everything (PostgreSQL's rule), because `std.math.order`
//! reaches `unreachable` on NaN. Text functions count characters, not bytes; a
//! byte that does not start valid UTF-8 counts as one character, so non-UTF-8
//! text degrades to byte semantics. Generated strings are capped at
//! `max_str_bytes` (1 MiB).
//!
//! Per-thread state, since parallel lanes evaluate concurrently: `field_memo`
//! (column index per name, verified on every hit so a stale entry costs one
//! compare, never a wrong column), `RegexCache` (keyed by pattern bytes, not
//! address, because batch-arena patterns reuse addresses), `reduce_memo`, and
//! `fail_note`, the human reason for the last builtin failure. A note is tied to
//! the error code it explains and dropped when evaluation restarts, so an error
//! swallowed by TRY_CAST or a fallback never lends its note to a later one.
//!
//! Rendering: `writeValue` writes text straight into a sink with no allocation
//! and is byte-identical to `valueToString`; years before 0 print with a sign.
//! Dates are day counts and timestamps microseconds since 1970-01-01.

const std = @import("std");
const regex = @import("regex.zig");
const sql = @import("../db/sql.zig");
const ast = @import("../lang/ast.zig");
const types = @import("../lang/types.zig");
const column = @import("column.zig");
const Decimal = @import("value.zig").Decimal;
const Value = @import("value.zig").Value;
const json = @import("json.zig");
const pow10f = @import("value.zig").pow10f;
const Batch = @import("batch.zig").Batch;

const Type = types.Type;

pub const TypeError = error{ TypeError, OutOfMemory };
pub const EvalError = error{ CastFailed, DivByZero, TypeMismatch, IntOverflow, PatternTooComplex, InvalidJson, OutOfMemory };

pub const TypeCtx = struct {
    schema: types.Schema,
    arena: std.mem.Allocator,
    msg: []const u8 = "",
    span: ?ast.Span = null,

    pub fn typeOf(self: *TypeCtx, expr: *const ast.Expr) TypeError!Type {
        switch (expr.*) {
            .null_lit => return Type.unknownNull(),
            .bool_lit => return Type.init(.bool),
            .int_lit => return Type.init(.int),
            .float_lit => return Type.init(.float),
            .str_lit => return Type.init(.string),
            .field => |q| {
                self.span = q.span;
                if (q.dollar) return self.err("unknown `${s}`: no PARAM, LET or loop variable of that name", .{q.parts[0]});
                if (q.safe.len > 0) return self.err("`?.` (safe navigation) only applies to JSON-param paths, not column `{s}`", .{lastPart(q)});
                const idx = fieldIndex(self.schema, q) orelse
                    return self.err("unknown field `{s}`", .{lastPart(q)});
                self.span = null;
                return self.schema.fields[idx].ty;
            },
            .unary => |u| {
                const t = try self.typeOf(u.e);
                return switch (u.op) {
                    .neg => if (numericish(t)) t else self.err("`-` needs a numeric operand", .{}),
                    .not => if (boolish(t)) Type.init(.bool).withNull(t.nullable) else self.err("`not` needs a bool operand", .{}),
                    .bit_not => if (intish(t)) Type.init(.int).withNull(t.nullable or t.unknown) else self.err("`~` needs an INT operand", .{}),
                };
            },
            .binary => |b| return self.typeOfBinary(b),
            .call => |c| return self.typeOfCall(c),
            .cond => |c| {
                const ct = try self.typeOf(c.cond);
                if (!boolish(ct)) return self.err("`if` condition must be bool", .{});
                const a = try self.typeOf(c.then);
                const d = try self.typeOf(c.els);
                const u = Type.unify(a, d) orelse return self.err("`if` branches have incompatible types", .{});
                return u.withNull(u.nullable or ct.nullable);
            },
            .match => |m| return self.typeOfMatch(m),
            .cast => |c| {
                const s = try self.typeOf(c.e);
                return c.ty.withNull(s.nullable or c.safe);
            },
            .is_null => |n| {
                if (n.kind == .is_empty) {
                    const t = try self.typeOf(n.e);
                    if (!(t.kind == .string or t.kind == .bytes or t.unknown))
                        return self.err("`is empty` needs a string operand (got {s}); use `is null`", .{@tagName(t.kind)});
                }
                return Type.init(.bool);
            },
            .let_in => return self.err("internal: `let … in` should have been expanded before type-checking", .{}),
            .lambda => return self.err("a lambda (`x -> …`) is only an argument of json_filter, json_transform, json_any, json_all or json_reduce", .{}),
            .lambda_var => |n| return self.err("internal: lambda parameter `{s}` typed outside its function", .{n}),
        }
    }

    fn typeOfBinary(self: *TypeCtx, b: ast.Expr.Binary) TypeError!Type {
        const lt = try self.typeOf(b.l);
        const rt = try self.typeOf(b.r);
        const nn = lt.nullable or rt.nullable or lt.unknown or rt.unknown;
        switch (b.op) {
            .add, .sub, .mul, .div, .mod => {
                if (!(numericish(lt) and numericish(rt))) return self.err("arithmetic needs numeric operands", .{});
                if (decimalArithType(b.op, lt, rt)) |dt| return dt.withNull(nn);
                const k: types.TypeKind = if (lt.kind == .float or rt.kind == .float or lt.kind == .decimal or rt.kind == .decimal) .float else .int;
                return Type{ .kind = k, .nullable = nn };
            },
            .bit_and, .bit_or, .bit_xor, .shl, .shr => {
                if (!(intish(lt) and intish(rt))) return self.err("bitwise operators need INT operands", .{});
                return Type{ .kind = .int, .nullable = nn };
            },
            .eq, .ne, .lt, .le, .gt, .ge => {
                if (!comparable(lt, rt) and
                    !try self.temporalLit(b.r, lt) and
                    !try self.temporalLit(b.l, rt)) return self.err("incomparable operands", .{});
                return Type{ .kind = .bool, .nullable = nn };
            },
            .@"and", .@"or" => {
                if (!(boolish(lt) and boolish(rt))) return self.err("`and`/`or` need bool operands", .{});
                return Type{ .kind = .bool, .nullable = nn };
            },
        }
    }

    /// A date/timestamp compared against a string literal: the literal must parse as
    /// the column's type, validated here so a bad one fails `check` rather than the run.
    fn temporalLit(self: *TypeCtx, e: *const ast.Expr, other: Type) TypeError!bool {
        if (e.* != .str_lit) return false;
        if (other.kind != .date and other.kind != .timestamp) return false;
        const ok = if (other.kind == .date)
            parseIsoDate(e.str_lit) != null
        else
            parseIsoTimestamp(e.str_lit) != null;
        if (!ok) return self.err("`{s}` is not a valid {s} literal", .{ e.str_lit, @tagName(other.kind) });
        return true;
    }

    /// An argument's own error names its own span; one about the call as a whole
    /// (arity, argument types) underlines the function name.
    fn typeOfCall(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        const name = c.name;
        self.span = c.span;
        inline for (.{ "count", "sum", "avg", "min", "max" }) |agg| {
            if (std.mem.eql(u8, name, agg)) return self.err("aggregate `{s}` is only valid inside `aggregate`", .{name});
        }
        const b = lookupBuiltin(name) orelse return self.err("unknown function `{s}`", .{name});
        self.span = null;
        return b.type_fn(self, c) catch |e| {
            if (self.span == null) self.span = c.span;
            return e;
        };
    }

    fn typeOfMatch(self: *TypeCtx, m: ast.Match) TypeError!Type {
        var subj: ?Type = null;
        if (m.subject) |s| subj = try self.typeOf(s);
        var result: ?Type = null;
        var has_default = false;
        for (m.arms) |arm| {
            if (arm.is_default) {
                has_default = true;
            } else if (arm.guard) |g| {
                if (!boolish(try self.typeOf(g))) return self.err("match guard must be bool", .{});
            } else {
                for (arm.pats) |p| {
                    const pt = try self.typeOf(p);
                    if (subj) |st| if (!comparable(st, pt)) return self.err("match pattern type does not match subject", .{});
                }
            }
            const vt = try self.typeOf(arm.value);
            result = if (result) |r| (Type.unify(r, vt) orelse return self.err("match arms have incompatible types", .{})) else vt;
        }
        var r = result orelse return self.err("match has no arms", .{});
        if (!has_default) r.nullable = true;
        return r;
    }

    fn argType(self: *TypeCtx, c: ast.Expr.Call, i: usize) TypeError!Type {
        if (i >= c.args.len) return self.err("`{s}` is missing an argument", .{c.name});
        return self.typeOf(c.args[i]);
    }

    fn err(self: *TypeCtx, comptime fmt: []const u8, args: anytype) TypeError {
        self.msg = std.fmt.allocPrint(self.arena, fmt, args) catch "out of memory";
        return error.TypeError;
    }

    /// A text parameter: strings, bytes and every scalar the runtime renders as text,
    /// since `concat(id, '-', name)` is too common to refuse. Only nested values fail.
    fn wantText(self: *TypeCtx, c: ast.Expr.Call, i: usize) TypeError!Type {
        const t = try self.argType(c, i);
        if (t.kind == .array or t.kind == .@"struct")
            return self.err("`{s}` argument {d} must be text, got {s}", .{ c.name, i + 1, @tagName(t.kind) });
        return t;
    }

    fn wantInt(self: *TypeCtx, c: ast.Expr.Call, i: usize, what: []const u8) TypeError!Type {
        const t = try self.argType(c, i);
        if (!intish(t)) return self.err("`{s}` {s} must be an INT, got {s}", .{ c.name, what, @tagName(t.kind) });
        return t;
    }
};

fn numericish(t: Type) bool {
    return t.kind.isNumeric() or t.unknown;
}
fn boolish(t: Type) bool {
    return t.kind == .bool or t.unknown;
}
fn temporalish(t: Type) bool {
    return t.kind == .date or t.kind == .timestamp or t.unknown;
}
fn intish(t: Type) bool {
    return t.kind == .int or t.unknown;
}
fn comparable(a: Type, b: Type) bool {
    if (a.unknown or b.unknown) return true;
    if (a.kind.isNumeric() and b.kind.isNumeric()) return true;
    return a.kind == b.kind;
}

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

fn evalColumnRowwise(arena: std.mem.Allocator, expr: *const ast.Expr, batch: Batch, out_ty: Type) EvalError!column.Column {
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
const Bitmap = column.Bitmap;

const VecError = error{ Unsupported, CastFailed, DivByZero, TypeMismatch, IntOverflow, PatternTooComplex, InvalidJson, OutOfMemory };

const Vec = union(enum) {
    col: Column,
    scalar: Value,
};

const Num = union(enum) {
    icol: struct { d: []const i64, v: Bitmap },
    fcol: struct { d: []const f64, v: Bitmap },
    iscalar: i64,
    fscalar: f64,
};

const Str = union(enum) {
    col: struct { d: column.Bytes, v: Bitmap },
    scalar: []const u8,
};

const BoolOp = union(enum) {
    col: struct { d: []const bool, v: Bitmap },
    scalar: ?bool,
};

fn evalVec(arena: std.mem.Allocator, expr: *const ast.Expr, batch: Batch) VecError!Vec {
    return evalVecNode(arena, expr, batch) catch |e| switch (e) {
        error.Unsupported => rowwiseVec(arena, expr, batch),
        else => e,
    };
}

fn evalVecNode(arena: std.mem.Allocator, expr: *const ast.Expr, batch: Batch) VecError!Vec {
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

fn strArg(arena: std.mem.Allocator, e: *const ast.Expr, batch: Batch) VecError!Str {
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
fn floatToInt(x: f64) error{CastFailed}!i64 {
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

fn asNum(arena: std.mem.Allocator, v: Vec, n: usize) VecError!?Num {
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

fn asStr(v: Vec) ?Str {
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

inline fn isIntNum(x: Num) bool {
    return x == .icol or x == .iscalar;
}
inline fn numI(x: Num, i: usize) i64 {
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
inline fn numValid(x: Num, i: usize) bool {
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
inline fn strAt(x: Str, i: usize) ?[]const u8 {
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

fn mkCol(ty: Type, n: usize, validity: Bitmap, data: Column.Data) Vec {
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

fn isEmptyVal(v: Value) bool {
    return switch (v) {
        .string, .bytes => |s| s.len == 0,
        else => false,
    };
}

pub fn evalRow(arena: std.mem.Allocator, expr: *const ast.Expr, batch: Batch, row: usize) EvalError!Value {
    forgetFailure();
    switch (expr.*) {
        .null_lit => return .null,
        .bool_lit => |b| return .{ .bool = b },
        .int_lit => |i| return .{ .int = i },
        .float_lit => |f| return .{ .float = f },
        .str_lit => |s| return .{ .string = s },
        .field => |q| {
            const idx = fieldIndex(batch.schema.*, q) orelse return error.TypeMismatch;
            return batch.columns[idx].getValue(row);
        },
        .unary => |u| {
            const v = try evalRow(arena, u.e, batch, row);
            if (v.isNull()) return .null;
            return switch (u.op) {
                .neg => switch (v) {
                    .int => |x| .{ .int = std.math.negate(x) catch return error.IntOverflow },
                    .float => |x| .{ .float = -x },
                    .decimal => |d| .{ .decimal = .{ .unscaled = -d.unscaled, .scale = d.scale } },
                    else => error.TypeMismatch,
                },
                .not => .{ .bool = !v.bool },
                .bit_not => switch (v) {
                    .int => |x| .{ .int = ~x },
                    else => error.TypeMismatch,
                },
            };
        },
        .binary => |b| return evalBinary(arena, b, batch, row),
        .is_null => |n| {
            const v = try evalRow(arena, n.e, batch, row);
            const hit = switch (n.kind) {
                .is_null => v.isNull(),
                .is_empty => v.isNull() or isEmptyVal(v),
            };
            return .{ .bool = if (n.negated) !hit else hit };
        },
        .cond => |c| {
            const cv = try evalRow(arena, c.cond, batch, row);
            if (cv == .bool and cv.bool) return evalRow(arena, c.then, batch, row);
            return evalRow(arena, c.els, batch, row);
        },
        .cast => |c| {
            const v = try evalRow(arena, c.e, batch, row);
            if (v.isNull()) return .null;
            if (!c.safe) return castValueTyped(arena, v, c.ty) catch |e| switch (e) {
                error.CastFailed => return castFailure(arena, v, c.ty),
                else => return e,
            };
            const out: Value = castValueTyped(arena, v, c.ty) catch |e| {
                if (e == error.CastFailed) return .null;
                return e;
            };
            return out;
        },
        .match => |m| return evalMatch(arena, m, batch, row),
        .call => |c| return evalCall(arena, c, batch, row),
        .let_in, .lambda, .lambda_var => return error.TypeMismatch,
    }
}

fn evalBinary(arena: std.mem.Allocator, b: ast.Expr.Binary, batch: Batch, row: usize) EvalError!Value {
    switch (b.op) {
        .@"and" => {
            const l = try evalRow(arena, b.l, batch, row);
            if (l == .bool and l.bool == false) return .{ .bool = false };
            const r = try evalRow(arena, b.r, batch, row);
            if (r == .bool and r.bool == false) return .{ .bool = false };
            if (l.isNull() or r.isNull()) return .null;
            return .{ .bool = true };
        },
        .@"or" => {
            const l = try evalRow(arena, b.l, batch, row);
            if (l == .bool and l.bool == true) return .{ .bool = true };
            const r = try evalRow(arena, b.r, batch, row);
            if (r == .bool and r.bool == true) return .{ .bool = true };
            if (l.isNull() or r.isNull()) return .null;
            return .{ .bool = false };
        },
        else => {
            const l = try evalRow(arena, b.l, batch, row);
            const r = try evalRow(arena, b.r, batch, row);
            if (l.isNull() or r.isNull()) return .null;
            return switch (b.op) {
                .add, .sub, .mul, .div, .mod => arith(b.op, l, r),
                .bit_and, .bit_or, .bit_xor, .shl, .shr => bitwise(b.op, l, r),
                .eq, .ne, .lt, .le, .gt, .ge => blk: {
                    const ord = compareValues(l, r) orelse break :blk error.TypeMismatch;
                    break :blk Value{ .bool = cmpResult(b.op, ord) };
                },
                else => unreachable,
            };
        },
    }
}

/// `minInt(i64) / -1` is the one quotient i64 cannot hold and the hardware traps
/// on it (SIGFPE); it is IntOverflow here, and its remainder is 0.
fn intDiv(a: i64, b: i64) error{ DivByZero, IntOverflow }!i64 {
    if (b == 0) return error.DivByZero;
    if (b == -1) return std.math.negate(a) catch error.IntOverflow;
    return @divTrunc(a, b);
}

fn intRem(a: i64, b: i64) error{DivByZero}!i64 {
    if (b == 0) return error.DivByZero;
    if (b == -1) return 0;
    return @rem(a, b);
}

fn arith(op: ast.BinOp, l: Value, r: Value) EvalError!Value {
    if (l == .int and r == .int) {
        const a = l.int;
        const b = r.int;
        return switch (op) {
            .add => .{ .int = std.math.add(i64, a, b) catch return error.IntOverflow },
            .sub => .{ .int = std.math.sub(i64, a, b) catch return error.IntOverflow },
            .mul => .{ .int = std.math.mul(i64, a, b) catch return error.IntOverflow },
            .div => .{ .int = try intDiv(a, b) },
            .mod => .{ .int = try intRem(a, b) },
            else => unreachable,
        };
    }
    if (l == .decimal or r == .decimal) {
        if (asDecimal(l)) |a| if (asDecimal(r)) |b| if (decimalOp(op, a, b)) |d| return .{ .decimal = try d };
    }
    const a = toF64(l);
    const b = toF64(r);
    return switch (op) {
        .add => .{ .float = a + b },
        .sub => .{ .float = a - b },
        .mul => .{ .float = a * b },
        .div => .{ .float = a / b },
        .mod => .{ .float = @rem(a, b) },
        else => unreachable,
    };
}

/// The exact DECIMAL type of `op`, or null when it stays float. `+ - %` take the
/// wider scale, `*` the summed scale; an INT operand is a DECIMAL(19,0).
fn decimalArithType(op: ast.BinOp, lt: Type, rt: Type) ?Type {
    if (op != .add and op != .sub and op != .mul and op != .mod) return null;
    if (lt.kind == .float or rt.kind == .float) return null;
    if (lt.kind != .decimal and rt.kind != .decimal) return null;
    const lp: u16 = if (lt.kind != .decimal) 19 else if (lt.precision == 0) 38 else lt.precision;
    const rp: u16 = if (rt.kind != .decimal) 19 else if (rt.precision == 0) 38 else rt.precision;
    const ls: u16 = if (lt.kind == .decimal) lt.scale else 0;
    const rs: u16 = if (rt.kind == .decimal) rt.scale else 0;
    const scale: u16 = if (op == .mul) ls + rs else @max(ls, rs);
    const prec: u16 = switch (op) {
        .mul => lp + rp,
        .mod => @max(1, @min(lp -| ls, rp -| rs) + scale),
        else => @max(lp -| ls, rp -| rs) + scale + 1,
    };
    return Type.decimal(@intCast(@min(38, prec)), @intCast(@min(38, scale)));
}

fn asDecimal(v: Value) ?Decimal {
    return switch (v) {
        .decimal => |d| d,
        .int => |x| .{ .unscaled = x, .scale = 0 },
        else => null,
    };
}

fn decimalOp(op: ast.BinOp, a: Decimal, b: Decimal) ?error{ IntOverflow, DivByZero }!Decimal {
    switch (op) {
        .mul => return .{
            .unscaled = std.math.mul(i128, a.unscaled, b.unscaled) catch return error.IntOverflow,
            .scale = a.scale + b.scale,
        },
        .add, .sub => {
            const s = @max(a.scale, b.scale);
            const x = rescaleTo(a, s) orelse return error.IntOverflow;
            const y = rescaleTo(b, s) orelse return error.IntOverflow;
            const u = if (op == .add) std.math.add(i128, x.unscaled, y.unscaled) else std.math.sub(i128, x.unscaled, y.unscaled);
            return .{ .unscaled = u catch return error.IntOverflow, .scale = s };
        },
        .mod => {
            const s = @max(a.scale, b.scale);
            const x = rescaleTo(a, s) orelse return error.IntOverflow;
            const y = rescaleTo(b, s) orelse return error.IntOverflow;
            if (y.unscaled == 0) return error.DivByZero;
            return .{ .unscaled = @rem(x.unscaled, y.unscaled), .scale = s };
        },
        else => return null,
    }
}

fn bitwise(op: ast.BinOp, l: Value, r: Value) EvalError!Value {
    if (l != .int or r != .int) return error.TypeMismatch;
    const a = l.int;
    const b = r.int;
    return switch (op) {
        .bit_and => .{ .int = a & b },
        .bit_or => .{ .int = a | b },
        .bit_xor => .{ .int = a ^ b },
        .shl => .{ .int = shiftLeft(a, b) },
        .shr => .{ .int = shiftRight(a, b) },
        else => unreachable,
    };
}

/// Shift counts outside 0..63 are defined, never UB: an over-wide `<<` is 0 and
/// `>>` is 0 or -1 by sign; a negative count is 0 either way.
fn shiftLeft(a: i64, n: i64) i64 {
    const s = std.math.cast(u6, n) orelse return 0;
    return @bitCast(@as(u64, @bitCast(a)) << s);
}

fn shiftRight(a: i64, n: i64) i64 {
    const s = std.math.cast(u6, n) orelse return if (n > 0 and a < 0) -1 else 0;
    return a >> s;
}

fn cmpResult(op: ast.BinOp, ord: std.math.Order) bool {
    return switch (op) {
        .eq => ord == .eq,
        .ne => ord != .eq,
        .lt => ord == .lt,
        .le => ord != .gt,
        .gt => ord == .gt,
        .ge => ord != .lt,
        else => false,
    };
}

fn evalMatch(arena: std.mem.Allocator, m: ast.Match, batch: Batch, row: usize) EvalError!Value {
    if (m.subject) |se| {
        const s = try evalRow(arena, se, batch, row);
        for (m.arms) |arm| {
            if (arm.is_default) return evalRow(arena, arm.value, batch, row);
            for (arm.pats) |p| {
                const pv = try evalRow(arena, p, batch, row);
                if (!s.isNull() and !pv.isNull()) {
                    if (compareValues(s, pv)) |ord| {
                        if (ord == .eq) return evalRow(arena, arm.value, batch, row);
                    }
                }
            }
        }
        return .null;
    }
    for (m.arms) |arm| {
        if (arm.is_default) return evalRow(arena, arm.value, batch, row);
        const g = try evalRow(arena, arm.guard.?, batch, row);
        if (g == .bool and g.bool) return evalRow(arena, arm.value, batch, row);
    }
    return .null;
}

fn evalCall(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
    const b = lookupBuiltin(c.name) orelse return error.TypeMismatch;
    return b.eval_fn(arena, c, batch, row);
}

pub fn parseJson(arena: std.mem.Allocator, text: []const u8) EvalError!std.json.Value {
    return std.json.parseFromSliceLeaky(std.json.Value, arena, text, .{}) catch |e|
        return if (e == error.OutOfMemory) error.OutOfMemory else error.InvalidJson;
}

/// Walks `path` (`a.b`, `a[0].b` or `a.0.b`, optionally led by `$.`) through `v`;
/// null for a missing key, an index out of range, or a step onto a scalar.
pub fn jsonPath(v: std.json.Value, path: []const u8) ?std.json.Value {
    var p = path;
    if (std.mem.startsWith(u8, p, "$")) p = p[1..];
    var cur = v;
    var it = std.mem.tokenizeAny(u8, p, ".[]");
    while (it.next()) |step| {
        cur = switch (cur) {
            .object => |o| o.get(step) orelse return null,
            .array => |a| blk: {
                const i = std.fmt.parseInt(usize, step, 10) catch return null;
                break :blk if (i < a.items.len) a.items[i] else return null;
            },
            else => return null,
        };
    }
    return cur;
}

pub fn jsonToValue(arena: std.mem.Allocator, v: std.json.Value) EvalError!Value {
    return switch (v) {
        .null => .null,
        .string, .number_string => |s| .{ .string = s },
        .integer => |i| .{ .string = try std.fmt.allocPrint(arena, "{d}", .{i}) },
        .float => |f| .{ .string = try std.fmt.allocPrint(arena, "{d}", .{f}) },
        .bool => |b| .{ .string = if (b) "true" else "false" },
        .object, .array => .{ .string = std.json.Stringify.valueAlloc(arena, v, .{}) catch return error.OutOfMemory },
    };
}

/// A JSON array element as a lambda's parameter: numbers, booleans and strings as
/// themselves, so `x > 5` compares numbers, and objects or arrays as their JSON text.
fn jsonElementValue(arena: std.mem.Allocator, raw: []const u8) EvalError!Value {
    return switch (raw[0]) {
        '"' => .{ .string = try json.decodeString(arena, raw[1 .. raw.len - 1]) },
        't' => .{ .bool = true },
        'f' => .{ .bool = false },
        'n' => .null,
        '{', '[' => .{ .string = try json.compact(arena, raw) },
        else => switch (json.number(raw)) {
            .int => |i| .{ .int = i },
            .float => |f| .{ .float = f },
            .text => |t| .{ .string = t },
        },
    };
}

/// Text that is a JSON object or array goes in as that value, not a string.
/// NaN and infinity are written as `null`, as JSON.stringify does.
fn writeJsonValue(arena: std.mem.Allocator, v: Value, w: *std.Io.Writer) EvalError!void {
    switch (v) {
        .null => w.writeAll("null") catch return error.OutOfMemory,
        .bool => |b| w.writeAll(if (b) "true" else "false") catch return error.OutOfMemory,
        .int => |i| w.print("{d}", .{i}) catch return error.OutOfMemory,
        .float => |f| if (std.math.isFinite(f))
            w.print("{}", .{f}) catch return error.OutOfMemory
        else
            w.writeAll("null") catch return error.OutOfMemory,
        .decimal => w.writeAll(try valueToString(arena, v)) catch return error.OutOfMemory,
        .string => |s| {
            const t = std.mem.trim(u8, s, " \t\r\n");
            if (t.len > 0 and (t[0] == '{' or t[0] == '[')) {
                if (json.validate(arena, t)) |_| return json.compactInto(arena, t, w) else |_| {}
            }
            std.json.Stringify.encodeJsonString(s, .{}, w) catch return error.OutOfMemory;
        },
        else => std.json.Stringify.encodeJsonString(try valueToString(arena, v), .{}, w) catch return error.OutOfMemory,
    }
}

pub fn bindLambda(arena: std.mem.Allocator, body: *const ast.Expr, name: []const u8, v: Value) error{OutOfMemory}!*ast.Expr {
    const lit = try arena.create(ast.Expr);
    lit.* = try literalOf(arena, v);
    return bindLambdaTo(arena, body, name, lit);
}

fn literalOf(arena: std.mem.Allocator, v: Value) error{OutOfMemory}!ast.Expr {
    return switch (v) {
        .null => .null_lit,
        .bool => |x| .{ .bool_lit = x },
        .int => |x| .{ .int_lit = x },
        .float => |x| .{ .float_lit = x },
        .string => |x| .{ .str_lit = x },
        else => .{ .str_lit = try valueToString(arena, v) },
    };
}

/// `body` with parameter `name` replaced by the node `lit`, one node for every
/// mention, so a caller rebinds it by overwriting `lit`.
fn bindLambdaTo(arena: std.mem.Allocator, body: *const ast.Expr, name: []const u8, lit: *ast.Expr) error{OutOfMemory}!*ast.Expr {
    const Bind = struct {
        arena: std.mem.Allocator,
        name: []const u8,
        lit: *ast.Expr,
        fn recur(b: @This(), e: *const ast.Expr) error{OutOfMemory}!*ast.Expr {
            switch (e.*) {
                .lambda_var => |n| if (std.mem.eql(u8, n, b.name)) return b.lit,
                .lambda => |l| for (l.params) |pp| {
                    if (std.mem.eql(u8, pp, b.name)) return @constCast(e);
                },
                else => {},
            }
            return ast.rebuildExpr(b.arena, e, b, recur);
        }
    };
    return Bind.recur(.{ .arena = arena, .name = name, .lit = lit }, body);
}

fn jsonArrayArg(arena: std.mem.Allocator, e: *const ast.Expr, batch: Batch, row: usize) EvalError!?[]const u8 {
    const v = try evalRow(arena, e, batch, row);
    if (v.isNull()) return null;
    const doc = try valueToString(arena, v);
    try json.validate(arena, doc);
    if (json.rootKind(doc) != .array) return error.InvalidJson;
    return doc;
}

fn bindParams(arena: std.mem.Allocator, l: ast.Expr.Lambda, slots: []const *ast.Expr) error{OutOfMemory}!*ast.Expr {
    var body = l.body;
    for (l.params, slots[0..l.params.len]) |pp, slot| body = try bindLambdaTo(arena, body, pp, slot);
    return body;
}

fn lambdaSlots(arena: std.mem.Allocator, vals: []const ast.Expr) error{OutOfMemory}![]*ast.Expr {
    const slots = try arena.alloc(*ast.Expr, vals.len);
    for (slots, vals) |*sl, v| {
        sl.* = try arena.create(ast.Expr);
        sl.*.* = v;
    }
    return slots;
}

/// The node `json_reduce` binds `acc` to. A value with no literal of its own (a
/// DECIMAL, a date) is its text cast back, so a running DECIMAL total stays one.
fn accNode(arena: std.mem.Allocator, acc: Value, ty: Type) error{OutOfMemory}!ast.Expr {
    switch (acc) {
        .null, .bool, .int, .float, .string => return literalOf(arena, acc),
        else => {
            if (ty.unknown) return literalOf(arena, acc);
            const text = try arena.create(ast.Expr);
            text.* = .{ .str_lit = try valueToString(arena, acc) };
            return .{ .cast = .{ .e = text, .ty = ty } };
        },
    }
}

threadlocal var reduce_memo: struct { args: ?[*]const *ast.Expr = null, schema: ?*const types.Schema = null, ty: Type = undefined } = .{};

fn reduceTypeAt(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) EvalError!Type {
    const m = &reduce_memo;
    if (m.args == c.args.ptr and m.schema == batch.schema) return m.ty;
    var tc = TypeCtx{ .schema = batch.schema.*, .arena = arena };
    const ty = typing.reduceAcc(&tc, c) catch |e| switch (e) {
        error.OutOfMemory => return error.OutOfMemory,
        error.TypeError => return error.TypeMismatch,
    };
    m.* = .{ .args = c.args.ptr, .schema = batch.schema, .ty = ty };
    return ty;
}

const max_str_bytes = 1 << 20;

pub const Builtin = struct {
    name: []const u8,
    type_fn: *const fn (*TypeCtx, ast.Expr.Call) TypeError!Type,
    eval_fn: *const fn (std.mem.Allocator, ast.Expr.Call, Batch, usize) EvalError!Value,
    vec_fn: ?*const fn (std.mem.Allocator, ast.Expr.Call, Batch) VecError!Vec = null,
};

pub const builtins = [_]Builtin{
    .{ .name = "now", .type_fn = typing.now, .eval_fn = per_row.now, .vec_fn = vectorized.now },
    .{ .name = "today", .type_fn = typing.today, .eval_fn = per_row.today, .vec_fn = vectorized.today },
    .{ .name = "regexp_replace", .type_fn = typing.regexpReplace, .eval_fn = per_row.regexpReplace },
    .{ .name = "regexp_matches", .type_fn = typing.regexpMatches, .eval_fn = per_row.regexpMatches },
    .{ .name = "regexp_extract", .type_fn = typing.regexpExtract, .eval_fn = per_row.regexpExtract },
    .{ .name = "md5", .type_fn = typing.digest, .eval_fn = per_row.digest },
    .{ .name = "sha256", .type_fn = typing.digest, .eval_fn = per_row.digest },
    .{ .name = "xxhash64", .type_fn = typing.digest, .eval_fn = per_row.digest },
    .{ .name = "concat_ws", .type_fn = typing.concatWs, .eval_fn = per_row.concatWs },
    .{ .name = "date_trunc", .type_fn = typing.dateTruncExtract, .eval_fn = per_row.dateTruncExtract },
    .{ .name = "extract", .type_fn = typing.dateTruncExtract, .eval_fn = per_row.dateTruncExtract },
    .{ .name = "upper", .type_fn = typing.unaryString, .eval_fn = per_row.upperLower, .vec_fn = vectorized.upperLower },
    .{ .name = "lower", .type_fn = typing.unaryString, .eval_fn = per_row.upperLower, .vec_fn = vectorized.upperLower },
    .{ .name = "length", .type_fn = typing.strlen, .eval_fn = per_row.strlen, .vec_fn = vectorized.strlen },
    .{ .name = "strlen", .type_fn = typing.strlen, .eval_fn = per_row.strlen, .vec_fn = vectorized.strlen },
    .{ .name = "bit_count", .type_fn = typing.bitCountToHex, .eval_fn = per_row.bitCount },
    .{ .name = "to_hex", .type_fn = typing.bitCountToHex, .eval_fn = per_row.toHex },
    .{ .name = "from_hex", .type_fn = typing.fromHex, .eval_fn = per_row.fromHex },
    .{ .name = "concat", .type_fn = typing.concat, .eval_fn = per_row.concat, .vec_fn = vectorized.concat },
    .{ .name = "coalesce", .type_fn = typing.coalesce, .eval_fn = per_row.coalesce, .vec_fn = vectorized.coalesce },
    .{ .name = "starts_with", .type_fn = typing.strPredicate, .eval_fn = per_row.affix, .vec_fn = vectorized.strPredicate },
    .{ .name = "ends_with", .type_fn = typing.strPredicate, .eval_fn = per_row.affix, .vec_fn = vectorized.strPredicate },
    .{ .name = "contains", .type_fn = typing.strPredicate, .eval_fn = per_row.affix, .vec_fn = vectorized.strPredicate },
    .{ .name = "like", .type_fn = typing.strPredicate, .eval_fn = per_row.like, .vec_fn = vectorized.strPredicate },
    .{ .name = "trim", .type_fn = typing.unaryString, .eval_fn = per_row.trimSpace, .vec_fn = vectorized.trimSpace },
    .{ .name = "substr", .type_fn = typing.substr, .eval_fn = per_row.substr, .vec_fn = vectorized.substr },
    .{ .name = "replace", .type_fn = typing.replace, .eval_fn = per_row.replace, .vec_fn = vectorized.replace },
    .{ .name = "abs", .type_fn = typing.abs, .eval_fn = per_row.abs },
    .{ .name = "floor", .type_fn = typing.floorCeil, .eval_fn = per_row.floorCeil },
    .{ .name = "ceil", .type_fn = typing.floorCeil, .eval_fn = per_row.floorCeil },
    .{ .name = "round", .type_fn = typing.round, .eval_fn = per_row.round },
    .{ .name = "mod", .type_fn = typing.mod, .eval_fn = per_row.mod },
    .{ .name = "power", .type_fn = typing.power, .eval_fn = per_row.power },
    .{ .name = "sqrt", .type_fn = typing.sqrt, .eval_fn = per_row.sqrt },
    .{ .name = "sign", .type_fn = typing.sign, .eval_fn = per_row.sign },
    .{ .name = "nullif", .type_fn = typing.nullif, .eval_fn = per_row.nullif },
    .{ .name = "greatest", .type_fn = typing.greatestLeast, .eval_fn = per_row.greatestLeast },
    .{ .name = "least", .type_fn = typing.greatestLeast, .eval_fn = per_row.greatestLeast },
    .{ .name = "lpad", .type_fn = typing.pad, .eval_fn = per_row.pad },
    .{ .name = "rpad", .type_fn = typing.pad, .eval_fn = per_row.pad },
    .{ .name = "left", .type_fn = typing.leftRight, .eval_fn = per_row.leftRight },
    .{ .name = "right", .type_fn = typing.leftRight, .eval_fn = per_row.leftRight },
    .{ .name = "split_part", .type_fn = typing.splitPart, .eval_fn = per_row.splitPart },
    .{ .name = "strpos", .type_fn = typing.strpos, .eval_fn = per_row.strpos },
    .{ .name = "repeat", .type_fn = typing.repeat, .eval_fn = per_row.repeat },
    .{ .name = "reverse", .type_fn = typing.reverse, .eval_fn = per_row.reverse },
    .{ .name = "date_add", .type_fn = typing.dateAdd, .eval_fn = per_row.dateAddDiff },
    .{ .name = "date_diff", .type_fn = typing.dateDiff, .eval_fn = per_row.dateAddDiff },
    .{ .name = "make_date", .type_fn = typing.makeDate, .eval_fn = per_row.makeDate },
    .{ .name = "epoch", .type_fn = typing.epoch, .eval_fn = per_row.epoch },
    .{ .name = "to_timestamp", .type_fn = typing.toTimestamp, .eval_fn = per_row.toTimestamp },
    .{ .name = "strftime", .type_fn = typing.strftime, .eval_fn = per_row.strftime },
    .{ .name = "strptime", .type_fn = typing.strptime, .eval_fn = per_row.strptime },
    .{ .name = "try_strptime", .type_fn = typing.strptime, .eval_fn = per_row.strptime },
    .{ .name = "unaccent", .type_fn = typing.unaryString, .eval_fn = per_row.unaccent },
    .{ .name = "strip_accents", .type_fn = typing.unaryString, .eval_fn = per_row.unaccent },
    .{ .name = "translate", .type_fn = typing.translate, .eval_fn = per_row.translate },
    .{ .name = "initcap", .type_fn = typing.unaryString, .eval_fn = per_row.initcap },
    .{ .name = "ascii", .type_fn = typing.strlen, .eval_fn = per_row.ascii },
    .{ .name = "chr", .type_fn = typing.chr, .eval_fn = per_row.chr },
    .{ .name = "json_get", .type_fn = typing.jsonGet, .eval_fn = per_row.jsonGet },
    .{ .name = "json_filter", .type_fn = typing.jsonLambda, .eval_fn = per_row.jsonLambda },
    .{ .name = "json_transform", .type_fn = typing.jsonLambda, .eval_fn = per_row.jsonLambda },
    .{ .name = "json_any", .type_fn = typing.jsonLambda, .eval_fn = per_row.jsonLambda },
    .{ .name = "json_all", .type_fn = typing.jsonLambda, .eval_fn = per_row.jsonLambda },
    .{ .name = "json_reduce", .type_fn = typing.jsonReduce, .eval_fn = per_row.jsonReduce },
    .{ .name = "chars", .type_fn = typing.chars, .eval_fn = per_row.chars },
    .{ .name = "json_range", .type_fn = typing.jsonRange, .eval_fn = per_row.jsonRange },
    .{ .name = "json_length", .type_fn = typing.jsonLength, .eval_fn = per_row.jsonLength },
    .{ .name = "json_slice", .type_fn = typing.jsonSlice, .eval_fn = per_row.jsonSlice },
    .{ .name = "json_concat", .type_fn = typing.jsonConcat, .eval_fn = per_row.jsonConcat },
    .{ .name = "json_object", .type_fn = typing.jsonBuild, .eval_fn = per_row.jsonObject },
    .{ .name = "json_array", .type_fn = typing.jsonBuild, .eval_fn = per_row.jsonArray },
    .{ .name = "to_base64", .type_fn = typing.unaryString, .eval_fn = per_row.toBase64 },
    .{ .name = "from_base64", .type_fn = typing.fromBase64, .eval_fn = per_row.fromBase64 },
    .{ .name = "url_encode", .type_fn = typing.unaryString, .eval_fn = per_row.urlCode },
    .{ .name = "url_decode", .type_fn = typing.unaryString, .eval_fn = per_row.urlCode },
};

pub fn lookupBuiltin(name: []const u8) ?*const Builtin {
    const map = comptime blk: {
        var kvs: [builtins.len]struct { []const u8, usize } = undefined;
        for (builtins, 0..) |b, i| kvs[i] = .{ b.name, i };
        break :blk std.StaticStringMap(usize).initComptime(kvs);
    };
    const i = map.get(name) orelse return null;
    return &builtins[i];
}

const typing = struct {
    fn now(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 0) return self.err("`now` takes no arguments", .{});
        return Type.init(.timestamp);
    }

    fn today(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 0) return self.err("`today` takes no arguments", .{});
        return Type.init(.date);
    }

    fn regexpReplace(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 3) return self.err("`regexp_replace` takes (string, pattern, replacement)", .{});
        _ = try literalPattern(self, c);
        const a = try self.wantText(c, 0);
        _ = try self.wantText(c, 1);
        _ = try self.wantText(c, 2);
        return Type.init(.string).withNull(a.nullable);
    }

    fn literalPattern(self: *TypeCtx, c: ast.Expr.Call) TypeError!?u8 {
        if (c.args[1].* != .str_lit) return null;
        var pbuf: [16 * 1024]u8 = undefined;
        var pfba = std.heap.FixedBufferAllocator.init(&pbuf);
        const re = regex.Regex.compile(pfba.allocator(), c.args[1].str_lit) catch
            return self.err("invalid regular expression `{s}`", .{c.args[1].str_lit});
        return re.ngroups;
    }

    fn regexpMatches(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 2) return self.err("`regexp_matches` takes (string, pattern)", .{});
        _ = try literalPattern(self, c);
        const a = try self.wantText(c, 0);
        const p = try self.wantText(c, 1);
        return Type.init(.bool).withNull(a.nullable or p.nullable);
    }

    fn regexpExtract(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 2 and c.args.len != 3) return self.err("`regexp_extract` takes (string, pattern[, group])", .{});
        const groups = try literalPattern(self, c);
        _ = try self.wantText(c, 0);
        _ = try self.wantText(c, 1);
        if (c.args.len == 3) {
            if (c.args[2].* != .int_lit) return self.err("`regexp_extract` needs a literal group number", .{});
            const g = c.args[2].int_lit;
            if (g < 0 or g >= regex.max_groups or (groups != null and g >= groups.?))
                return self.err("`regexp_extract` group {d} is not in the pattern", .{g});
        }
        return Type.init(.string).withNull(true);
    }

    fn digest(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 1) return self.err("`{s}` takes one argument", .{c.name});
        const a = try self.wantText(c, 0);
        return Type.init(if (eq(c.name, "xxhash64")) .int else .string).withNull(a.nullable);
    }

    fn jsonBuild(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (eq(c.name, "json_object") and c.args.len % 2 != 0)
            return self.err("`json_object` takes key, value pairs", .{});
        for (c.args, 0..) |_, i| _ = try self.wantText(c, i);
        return Type.init(.string);
    }

    fn fromBase64(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 1) return self.err("`from_base64` takes one argument", .{});
        const a = try self.wantText(c, 0);
        return Type.init(.bytes).withNull(a.nullable);
    }

    fn concatWs(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len < 2) return self.err("`concat_ws` takes (separator, value, ...)", .{});
        for (c.args, 0..) |_, i| _ = try self.wantText(c, i);
        return Type.init(.string).withNull((try self.argType(c, 0)).nullable);
    }

    fn dateTruncExtract(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        const name = c.name;
        if (c.args.len != 2) return self.err("`{s}` takes (unit, timestamp)", .{name});
        if (c.args[0].* != .str_lit) return self.err("`{s}` needs a literal unit", .{name});
        if (timeUnit(c.args[0].str_lit) == null)
            return self.err("unknown time unit `{s}` (units: year, month, week, day, hour, minute, second)", .{c.args[0].str_lit});
        const a = try self.argType(c, 1);
        if (a.kind != .date and a.kind != .timestamp and !a.unknown)
            return self.err("`{s}` needs a date or timestamp", .{name});
        const out: types.TypeKind = if (eq(name, "extract")) .int else .timestamp;
        return Type.init(out).withNull(a.nullable);
    }

    fn unaryString(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 1) return self.err("`{s}` takes one argument", .{c.name});
        const a = try self.wantText(c, 0);
        return Type.init(.string).withNull(a.nullable);
    }

    fn strlen(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 1) return self.err("`{s}` takes one argument", .{c.name});
        const a = try self.wantText(c, 0);
        return Type.init(.int).withNull(a.nullable);
    }

    fn bitCountToHex(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        const name = c.name;
        const a = try self.argType(c, 0);
        if (c.args.len != 1 or !intish(a)) return self.err("`{s}` takes one INT argument", .{name});
        const out: types.TypeKind = if (eq(name, "to_hex")) .string else .int;
        return Type.init(out).withNull(a.nullable);
    }

    fn fromHex(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        const a = try self.argType(c, 0);
        if (c.args.len != 1 or !(a.kind == .string or a.kind == .bytes or a.unknown))
            return self.err("`from_hex` takes one STRING argument", .{});
        return Type.init(.int).withNull(a.nullable);
    }

    fn concat(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len == 0) return self.err("`concat` needs at least one argument", .{});
        var nn = false;
        for (c.args, 0..) |_, i| nn = nn or (try self.wantText(c, i)).nullable;
        return Type.init(.string).withNull(nn);
    }

    fn coalesce(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len == 0) return self.err("`coalesce` needs at least one argument", .{});
        var result: ?Type = null;
        var all_null = true;
        for (c.args) |a| {
            const t = try self.typeOf(a);
            all_null = all_null and t.nullable;
            result = if (result) |r| (Type.unify(r, t) orelse return self.err("`coalesce` args have incompatible types", .{})) else t;
        }
        return result.?.withNull(all_null);
    }

    fn strPredicate(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 2) return self.err("`{s}` takes (string, string)", .{c.name});
        const a = try self.wantText(c, 0);
        const b = try self.wantText(c, 1);
        return Type.init(.bool).withNull(a.nullable or b.nullable);
    }

    fn substr(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 2 and c.args.len != 3) return self.err("`substr` takes (string, start[, length])", .{});
        const a = try self.wantText(c, 0);
        _ = try self.wantInt(c, 1, "start");
        if (c.args.len > 2) _ = try self.wantInt(c, 2, "length");
        return Type.init(.string).withNull(a.nullable);
    }

    fn replace(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 3) return self.err("`replace` takes (string, from, to)", .{});
        const a = try self.wantText(c, 0);
        _ = try self.wantText(c, 1);
        _ = try self.wantText(c, 2);
        return Type.init(.string).withNull(a.nullable);
    }

    fn abs(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 1) return self.err("`abs` takes one argument", .{});
        const a = try self.argType(c, 0);
        if (!numericish(a)) return self.err("`abs` needs a numeric argument", .{});
        return a;
    }

    fn floorCeil(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        const name = c.name;
        if (c.args.len != 1) return self.err("`{s}` takes one argument", .{name});
        const a = try self.argType(c, 0);
        if (!numericish(a)) return self.err("`{s}` needs a numeric argument", .{name});
        if (a.unknown or a.kind == .int) return a;
        return Type.init(.float).withNull(a.nullable);
    }

    fn round(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 1 and c.args.len != 2) return self.err("`round` takes (x) or (x, digits)", .{});
        const a = try self.argType(c, 0);
        if (!numericish(a)) return self.err("`round` needs a numeric argument", .{});
        if (c.args.len == 2) {
            const d = try self.argType(c, 1);
            if (!numericish(d)) return self.err("`round` digits must be an integer", .{});
        }
        if (a.unknown or (a.kind == .int and c.args.len == 1)) return a;
        if (a.kind == .decimal) return Type.decimal(a.precision, roundOutScale(c, a.scale)).withNull(a.nullable);
        return Type.init(.float).withNull(a.nullable);
    }

    fn mod(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 2) return self.err("`mod` takes (a, b)", .{});
        const a = try self.argType(c, 0);
        const b = try self.argType(c, 1);
        if (!(a.kind == .int or a.unknown) or !(b.kind == .int or b.unknown))
            return self.err("`mod` needs integer arguments", .{});
        return Type.init(.int).asNullable();
    }

    fn power(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 2) return self.err("`power` takes (base, exponent)", .{});
        const a = try self.argType(c, 0);
        const b = try self.argType(c, 1);
        if (!numericish(a) or !numericish(b)) return self.err("`power` needs numeric arguments", .{});
        return Type.init(.float).withNull(a.nullable or b.nullable or a.unknown or b.unknown);
    }

    fn sqrt(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 1) return self.err("`sqrt` takes one argument", .{});
        const a = try self.argType(c, 0);
        if (!numericish(a)) return self.err("`sqrt` needs a numeric argument", .{});
        return Type.init(.float).asNullable();
    }

    fn sign(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 1) return self.err("`sign` takes one argument", .{});
        const a = try self.argType(c, 0);
        if (!numericish(a)) return self.err("`sign` needs a numeric argument", .{});
        return Type.init(.int).withNull(a.nullable or a.unknown);
    }

    fn nullif(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 2) return self.err("`nullif` takes (a, b)", .{});
        const a = try self.argType(c, 0);
        const b = try self.argType(c, 1);
        if (!comparable(a, b)) return self.err("`nullif` arguments are not comparable", .{});
        return a.asNullable();
    }

    fn greatestLeast(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        const name = c.name;
        if (c.args.len < 2) return self.err("`{s}` needs at least two arguments", .{name});
        var result: ?Type = null;
        for (c.args) |a| {
            const t = try self.typeOf(a);
            result = if (result) |r|
                (Type.unify(r, t) orelse return self.err("`{s}` arguments have incompatible types", .{name}))
            else
                t;
        }
        return result.?.asNullable();
    }

    fn pad(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        const name = c.name;
        if (c.args.len != 2 and c.args.len != 3) return self.err("`{s}` takes (string, length[, fill])", .{name});
        const a = try self.wantText(c, 0);
        _ = try self.wantInt(c, 1, "length");
        if (c.args.len > 2) _ = try self.wantText(c, 2);
        return Type.init(.string).withNull(a.nullable);
    }

    fn leftRight(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        const name = c.name;
        if (c.args.len != 2) return self.err("`{s}` takes (string, n)", .{name});
        const a = try self.wantText(c, 0);
        _ = try self.wantInt(c, 1, "n");
        return Type.init(.string).withNull(a.nullable);
    }

    fn splitPart(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 3) return self.err("`split_part` takes (string, delimiter, n)", .{});
        _ = try self.wantText(c, 0);
        _ = try self.wantText(c, 1);
        _ = try self.wantInt(c, 2, "n");
        return Type.init(.string).asNullable();
    }

    fn jsonLambda(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 2 or c.args[1].* != .lambda)
            return self.err("`{s}` takes (json array, x -> {s})", .{ c.name, if (eq(c.name, "json_transform")) "value" else "condition" });
        const a = try self.wantText(c, 0);
        const l = c.args[1].lambda;
        if (l.params.len > 2) return self.err("`{s}`'s lambda takes (x) or (x, i), not {d} parameters", .{ c.name, l.params.len });
        const bt = try self.typeOf(try bindParams(self.arena, l, try lambdaSlots(self.arena, &.{ .null_lit, .{ .int_lit = 0 } })));
        if (eq(c.name, "json_transform")) return Type.init(.string).asNullable();
        if (!boolish(bt)) return self.err("`{s}`: the lambda must be a condition (BOOL), not {s}", .{ c.name, @tagName(bt.kind) });
        if (eq(c.name, "json_filter")) return Type.init(.string).asNullable();
        return Type.init(.bool).withNull(a.nullable or a.unknown);
    }

    fn jsonReduce(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 3 or c.args[2].* != .lambda)
            return self.err("`json_reduce` takes (json array, initial, (acc, x) -> value)", .{});
        _ = try self.wantText(c, 0);
        const l = c.args[2].lambda;
        if (l.params.len < 2 or l.params.len > 3)
            return self.err("`json_reduce`'s lambda takes (acc, x) or (acc, x, i), not {d} parameter{s}", .{ l.params.len, if (l.params.len == 1) "" else "s" });
        return (try reduceAcc(self, c)).asNullable();
    }

    /// The accumulator's type: the initial value's, widened to hold what the lambda
    /// returns (an INT start summing floats is FLOAT) and settled before the run.
    fn reduceAcc(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        const l = c.args[2].lambda;
        var acc = try self.typeOf(c.args[1]);
        var pass: usize = 0;
        while (pass < 3) : (pass += 1) {
            const null_node = try self.arena.create(ast.Expr);
            null_node.* = .null_lit;
            const acc_node: ast.Expr = if (acc.unknown) .null_lit else .{ .cast = .{ .e = null_node, .ty = acc } };
            const body = try bindParams(self.arena, l, try lambdaSlots(self.arena, &.{ acc_node, .null_lit, .{ .int_lit = 0 } }));
            const bt = try self.typeOf(body);
            var u = Type.unify(acc, bt) orelse
                return self.err("`json_reduce`: the lambda returns {s}, which an accumulator of {s} cannot hold", .{ @tagName(bt.kind), @tagName(acc.kind) });
            if (u.kind == .decimal) u.precision = 38;
            if (u.kind == acc.kind and u.unknown == acc.unknown and u.scale == acc.scale and u.precision == acc.precision) return u;
            acc = u;
        }
        return self.err("`json_reduce`: the accumulator's type keeps changing — give the initial value the type the lambda returns", .{});
    }

    fn chars(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 1) return self.err("`chars` takes one argument", .{});
        const a = try self.wantText(c, 0);
        return Type.init(.string).withNull(a.nullable);
    }

    fn jsonRange(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 1 and c.args.len != 2) return self.err("`json_range` takes (n) or (start, stop)", .{});
        var nn = false;
        for (0..c.args.len) |i| nn = nn or (try self.wantInt(c, i, if (i + 1 == c.args.len) "stop" else "start")).nullable;
        return Type.init(.string).withNull(nn);
    }

    fn jsonLength(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 1) return self.err("`json_length` takes one argument", .{});
        const a = try self.wantText(c, 0);
        return Type.init(.int).withNull(a.nullable or a.unknown);
    }

    fn jsonSlice(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 2 and c.args.len != 3) return self.err("`json_slice` takes (json array, start[, stop])", .{});
        var nn = (try self.wantText(c, 0)).nullable;
        for (1..c.args.len) |i| nn = nn or (try self.wantInt(c, i, if (i == 1) "start" else "stop")).nullable;
        return Type.init(.string).withNull(nn);
    }

    fn jsonConcat(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len < 2) return self.err("`json_concat` takes two or more JSON arrays", .{});
        var nn = false;
        for (0..c.args.len) |i| nn = nn or (try self.wantText(c, i)).nullable;
        return Type.init(.string).withNull(nn);
    }

    fn jsonGet(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 2) return self.err("`json_get` takes (json, path)", .{});
        _ = try self.wantText(c, 0);
        _ = try self.wantText(c, 1);
        return Type.init(.string).asNullable();
    }

    fn strpos(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 2) return self.err("`strpos` takes (string, substring)", .{});
        const a = try self.wantText(c, 0);
        const b = try self.wantText(c, 1);
        return Type.init(.int).withNull(a.nullable or b.nullable);
    }

    fn repeat(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 2) return self.err("`repeat` takes (string, n)", .{});
        const a = try self.wantText(c, 0);
        _ = try self.wantInt(c, 1, "n");
        return Type.init(.string).withNull(a.nullable);
    }

    fn reverse(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 1) return self.err("`reverse` takes one argument", .{});
        const a = try self.wantText(c, 0);
        return Type.init(.string).withNull(a.nullable);
    }

    fn dateAdd(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 3) return self.err("`date_add` takes (unit, n, timestamp)", .{});
        if (c.args[0].* != .str_lit) return self.err("`date_add` needs a literal unit", .{});
        const u = timeUnit(c.args[0].str_lit) orelse
            return self.err("unknown time unit `{s}` (units: year, month, week, day, hour, minute, second)", .{c.args[0].str_lit});
        const nt = try self.argType(c, 1);
        if (!numericish(nt)) return self.err("`date_add` needs an integer amount", .{});
        const a = try self.argType(c, 2);
        if (a.unknown) return a;
        const nn = a.nullable or nt.nullable or nt.unknown;
        if (a.kind == .date) {
            if (u == .hour or u == .minute or u == .second)
                return self.err("`date_add` cannot add `{s}` to a date; cast it to a timestamp first", .{c.args[0].str_lit});
            return Type.init(.date).withNull(nn);
        }
        if (a.kind != .timestamp) return self.err("`date_add` needs a date or timestamp", .{});
        return Type.init(.timestamp).withNull(nn);
    }

    fn dateDiff(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 3) return self.err("`date_diff` takes (unit, start, end)", .{});
        if (c.args[0].* != .str_lit) return self.err("`date_diff` needs a literal unit", .{});
        if (timeUnit(c.args[0].str_lit) == null)
            return self.err("unknown time unit `{s}` (units: year, month, week, day, hour, minute, second)", .{c.args[0].str_lit});
        const a = try self.argType(c, 1);
        const b = try self.argType(c, 2);
        if (!temporalish(a) or !temporalish(b))
            return self.err("`date_diff` needs date or timestamp arguments", .{});
        return Type.init(.int).withNull(a.nullable or b.nullable or a.unknown or b.unknown);
    }

    fn makeDate(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 3) return self.err("`make_date` takes (year, month, day)", .{});
        var nn = false;
        for (c.args) |a| {
            const t = try self.typeOf(a);
            if (!numericish(t)) return self.err("`make_date` needs integer arguments", .{});
            nn = nn or t.nullable or t.unknown;
        }
        return Type.init(.date).withNull(nn);
    }

    fn epoch(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 1) return self.err("`epoch` takes one argument", .{});
        const a = try self.argType(c, 0);
        if (!temporalish(a)) return self.err("`epoch` needs a date or timestamp", .{});
        return Type.init(.int).withNull(a.nullable);
    }

    fn toTimestamp(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 1) return self.err("`to_timestamp` takes one argument", .{});
        const a = try self.argType(c, 0);
        if (!numericish(a)) return self.err("`to_timestamp` needs a numeric argument", .{});
        return Type.init(.timestamp).withNull(a.nullable);
    }

    /// A literal format is validated here so an unsupported directive fails `check`.
    fn strftime(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 2) return self.err("`strftime` takes (timestamp, format)", .{});
        const a = try self.argType(c, 0);
        if (!temporalish(a)) return self.err("`strftime` needs a date or timestamp", .{});
        const f = try self.argType(c, 1);
        if (c.args[1].* == .str_lit) {
            if (badStrftime(c.args[1].str_lit)) |bad|
                return self.err("`strftime` does not support `%{s}` (supported: %Y %m %d %H %M %S %y %%)", .{bad});
        }
        return Type.init(.string).withNull(a.nullable or f.nullable);
    }

    fn strptime(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 2) return self.err("`{s}` takes (text, format)", .{c.name});
        const a = try self.wantText(c, 0);
        const f = try self.wantText(c, 1);
        if (c.args[1].* == .str_lit) {
            if (badStrftime(c.args[1].str_lit)) |bad|
                return self.err("`{s}` does not support `%{s}` (supported: %Y %m %d %H %M %S %y %%)", .{ c.name, bad });
        }
        return Type.init(.timestamp).withNull(eq(c.name, "try_strptime") or a.nullable or f.nullable);
    }

    fn translate(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        if (c.args.len != 3) return self.err("`translate` takes (string, from, to)", .{});
        var nn = false;
        for (0..3) |i| nn = nn or (try self.wantText(c, i)).nullable;
        return Type.init(.string).withNull(nn);
    }

    fn chr(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        const a = try self.argType(c, 0);
        if (c.args.len != 1 or !intish(a)) return self.err("`chr` takes one INT argument", .{});
        return Type.init(.string).withNull(a.nullable);
    }
};

const per_row = struct {
    fn now(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        _ = arena;
        _ = c;
        _ = batch;
        _ = row;
        return .{ .timestamp = std.time.microTimestamp() };
    }

    fn today(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        _ = arena;
        _ = c;
        _ = batch;
        _ = row;
        const days = @divFloor(std.time.microTimestamp(), 86_400_000_000);
        return .{ .date = @intCast(days) };
    }

    fn regexpReplace(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        const pat = try evalRow(arena, c.args[1], batch, row);
        const rep = try evalRow(arena, c.args[2], batch, row);
        if (pat.isNull() or rep.isNull()) return .null;
        const re = cachedRegex(try valueToString(arena, pat)) catch |e| switch (e) {
            error.OutOfMemory => return error.OutOfMemory,
            error.BadPattern => return error.CastFailed,
            error.PatternTooComplex => return error.PatternTooComplex,
        };
        const out = regex.replaceFirstRe(
            arena,
            re,
            try valueToString(arena, v),
            try valueToString(arena, rep),
        ) catch |e| switch (e) {
            error.OutOfMemory => return error.OutOfMemory,
            error.BadPattern => return error.CastFailed,
            error.PatternTooComplex => return error.PatternTooComplex,
        };
        return .{ .string = out };
    }

    fn regexpMatches(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const m = try regexpFind(arena, c, batch, row) orelse return .null;
        return .{ .bool = m.span != null };
    }

    /// Null where the pattern does not match, unlike DuckDB's '', which a load
    /// cannot tell from an empty field.
    fn regexpExtract(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const m = try regexpFind(arena, c, batch, row) orelse return .null;
        if (m.span == null) return .null;
        const g: usize = if (c.args.len == 3) @intCast(c.args[2].int_lit) else 0;
        const span = m.caps[g] orelse return .null;
        return .{ .string = m.s[span[0]..span[1]] };
    }

    fn digest(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        const data = try valueToString(arena, v);
        if (eq(c.name, "xxhash64")) return .{ .int = @bitCast(std.hash.XxHash64.hash(0, data)) };
        if (eq(c.name, "md5")) {
            var d: [std.crypto.hash.Md5.digest_length]u8 = undefined;
            std.crypto.hash.Md5.hash(data, &d, .{});
            return .{ .string = try std.fmt.allocPrint(arena, "{x}", .{&d}) };
        }
        var d: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(data, &d, .{});
        return .{ .string = try std.fmt.allocPrint(arena, "{x}", .{&d}) };
    }

    /// Values are written as `json_transform` writes elements, so a nested object or
    /// array goes in as JSON. A null key is an error.
    fn jsonObject(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        var out = std.Io.Writer.Allocating.init(arena);
        const w = &out.writer;
        w.writeByte('{') catch return error.OutOfMemory;
        var i: usize = 0;
        while (i < c.args.len) : (i += 2) {
            const k = try evalRow(arena, c.args[i], batch, row);
            if (k.isNull()) return failWith(error.CastFailed, "json_object: key {d} is null", .{i / 2 + 1});
            if (i > 0) w.writeByte(',') catch return error.OutOfMemory;
            std.json.Stringify.encodeJsonString(try valueToString(arena, k), .{}, w) catch return error.OutOfMemory;
            w.writeByte(':') catch return error.OutOfMemory;
            try writeJsonValue(arena, try evalRow(arena, c.args[i + 1], batch, row), w);
        }
        w.writeByte('}') catch return error.OutOfMemory;
        return .{ .string = out.written() };
    }

    fn jsonArray(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        var out = std.Io.Writer.Allocating.init(arena);
        const w = &out.writer;
        w.writeByte('[') catch return error.OutOfMemory;
        for (c.args, 0..) |e, i| {
            if (i > 0) w.writeByte(',') catch return error.OutOfMemory;
            try writeJsonValue(arena, try evalRow(arena, e, batch, row), w);
        }
        w.writeByte(']') catch return error.OutOfMemory;
        return .{ .string = out.written() };
    }

    fn toBase64(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        const data = try valueToString(arena, v);
        const enc = std.base64.standard.Encoder;
        const out = try arena.alloc(u8, enc.calcSize(data.len));
        return .{ .string = enc.encode(out, data) };
    }

    fn fromBase64(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        const text = std.mem.trim(u8, try valueToString(arena, v), " \t\r\n");
        const dec = std.base64.standard.Decoder;
        const bad = "from_base64: '{s}' is not base64";
        const out = try arena.alloc(u8, dec.calcSizeForSlice(text) catch return failWith(error.CastFailed, bad, .{clip(text)}));
        dec.decode(out, text) catch return failWith(error.CastFailed, bad, .{clip(text)});
        return .{ .bytes = out };
    }

    /// RFC 3986 percent-encoding: all but letters, digits and `-._~` become `%XX`.
    /// Decoding leaves `+` alone and passes a stray `%` through, as DuckDB does.
    fn urlCode(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        const str = try valueToString(arena, v);
        var out = try std.array_list.Managed(u8).initCapacity(arena, str.len);
        if (eq(c.name, "url_encode")) {
            for (str) |b| {
                if (std.ascii.isAlphanumeric(b) or b == '-' or b == '.' or b == '_' or b == '~') {
                    try out.append(b);
                } else try out.writer().print("%{X:0>2}", .{b});
            }
        } else {
            var i: usize = 0;
            while (i < str.len) : (i += 1) {
                if (str[i] == '%' and i + 2 < str.len) {
                    if (std.fmt.parseInt(u8, str[i + 1 .. i + 3], 16)) |b| {
                        try out.append(b);
                        i += 2;
                        continue;
                    } else |_| {}
                }
                try out.append(str[i]);
            }
        }
        return .{ .string = out.items };
    }

    /// Postgres' `concat_ws`: nulls are skipped, where `concat` is null when any
    /// value is (which hashed a row with one empty column to null).
    fn concatWs(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const sv = try evalRow(arena, c.args[0], batch, row);
        if (sv.isNull()) return .null;
        const sep = try valueToString(arena, sv);
        var buf = std.array_list.Managed(u8).init(arena);
        var first = true;
        for (c.args[1..]) |e| {
            const v = try evalRow(arena, e, batch, row);
            if (v.isNull()) continue;
            if (!first) try buf.appendSlice(sep);
            first = false;
            try buf.appendSlice(try valueToString(arena, v));
        }
        return .{ .string = buf.items };
    }

    fn dateTruncExtract(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[1], batch, row);
        if (v.isNull()) return .null;
        const us = temporalMicros(v) orelse return error.TypeMismatch;
        const u = timeUnit(c.args[0].str_lit) orelse return error.TypeMismatch;
        return if (eq(c.name, "extract"))
            Value{ .int = extractField(us, u) }
        else
            Value{ .timestamp = truncMicros(us, u) };
    }

    fn coalesce(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        for (c.args) |a| {
            const v = try evalRow(arena, a, batch, row);
            if (!v.isNull()) return v;
        }
        return .null;
    }

    fn upperLower(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        var out = std.array_list.Managed(u8).init(arena);
        try caseMapInto(&out, try valueToString(arena, v), eq(c.name, "upper"));
        return .{ .string = out.items };
    }

    /// `length` counts characters, `strlen` bytes (DuckDB's split); a BYTES value
    /// is bytes either way.
    fn strlen(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        const s = try valueToString(arena, v);
        return .{ .int = @intCast(if (eq(c.name, "length") and v != .bytes) charCount(s) else s.len) };
    }

    fn bitCount(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        if (v != .int) return error.TypeMismatch;
        return .{ .int = @intCast(@popCount(v.int)) };
    }

    fn toHex(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        if (v != .int) return error.TypeMismatch;
        return .{ .string = try std.fmt.allocPrint(arena, "{x}", .{@as(u64, @bitCast(v.int))}) };
    }

    fn fromHex(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        return .{ .int = try parseHexI64(try valueToString(arena, v)) };
    }

    fn concat(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        var buf = std.array_list.Managed(u8).init(arena);
        for (c.args) |a| {
            const v = try evalRow(arena, a, batch, row);
            if (v.isNull()) return .null;
            try buf.appendSlice(try valueToString(arena, v));
        }
        return .{ .string = try buf.toOwnedSlice() };
    }

    fn affix(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const name = c.name;
        const sv = try evalRow(arena, c.args[0], batch, row);
        const pv = try evalRow(arena, c.args[1], batch, row);
        if (sv.isNull() or pv.isNull()) return .null;
        const s = try valueToString(arena, sv);
        const p = try valueToString(arena, pv);
        const r = if (eq(name, "starts_with")) std.mem.startsWith(u8, s, p) else if (eq(name, "ends_with")) std.mem.endsWith(u8, s, p) else (std.mem.indexOf(u8, s, p) != null);
        return .{ .bool = r };
    }

    fn like(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const sv = try evalRow(arena, c.args[0], batch, row);
        const pv = try evalRow(arena, c.args[1], batch, row);
        if (sv.isNull() or pv.isNull()) return .null;
        return .{ .bool = likeMatch(try valueToString(arena, sv), try valueToString(arena, pv)) };
    }

    fn trimSpace(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        return .{ .string = try arena.dupe(u8, trim(try valueToString(arena, v))) };
    }

    fn substr(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const sv = try evalRow(arena, c.args[0], batch, row);
        if (sv.isNull()) return .null;
        const startv = try evalRow(arena, c.args[1], batch, row);
        if (startv.isNull()) return .null;
        var len_opt: ?i64 = null;
        if (c.args.len > 2) {
            const lv = try evalRow(arena, c.args[2], batch, row);
            if (lv.isNull()) return .null;
            len_opt = toI64(lv);
        }
        return .{ .string = try substrChars(arena, try valueToString(arena, sv), toI64(startv), len_opt) };
    }

    fn replace(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const sv = try evalRow(arena, c.args[0], batch, row);
        const fv = try evalRow(arena, c.args[1], batch, row);
        const tv = try evalRow(arena, c.args[2], batch, row);
        if (sv.isNull() or fv.isNull() or tv.isNull()) return .null;
        const s = try valueToString(arena, sv);
        const from = try valueToString(arena, fv);
        const to = try valueToString(arena, tv);
        if (from.len == 0) return .{ .string = try arena.dupe(u8, s) };
        const out = try arena.alloc(u8, std.mem.replacementSize(u8, s, from, to));
        _ = std.mem.replace(u8, s, from, to, out);
        return .{ .string = out };
    }

    fn abs(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        switch (v) {
            .int => |x| {
                if (x == std.math.minInt(i64)) return error.IntOverflow;
                return Value{ .int = if (x < 0) -x else x };
            },
            .float => |x| return Value{ .float = @abs(x) },
            .decimal => |d| return Value{ .decimal = .{
                .unscaled = if (d.unscaled < 0) -d.unscaled else d.unscaled,
                .scale = d.scale,
            } },
            else => return error.TypeMismatch,
        }
    }

    fn floorCeil(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        if (v == .int) return v;
        if (!isNum(v)) return error.TypeMismatch;
        const x = toF64(v);
        return Value{ .float = if (eq(c.name, "floor")) @floor(x) else @ceil(x) };
    }

    fn round(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        if (!isNum(v)) return error.TypeMismatch;
        var digits: i64 = 0;
        if (c.args.len > 1) {
            const dv = try evalRow(arena, c.args[1], batch, row);
            if (dv.isNull()) return .null;
            digits = toI64(dv);
        }
        if (v == .int and c.args.len == 1) return v;
        if (v == .decimal) return Value{ .decimal = roundDecimal(v.decimal, digits, roundOutScale(c, v.decimal.scale)) orelse return error.IntOverflow };
        return Value{ .float = roundHalfAway(toF64(v), digits) };
    }

    fn mod(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const a = try evalRow(arena, c.args[0], batch, row);
        const b = try evalRow(arena, c.args[1], batch, row);
        if (a.isNull() or b.isNull()) return .null;
        const d = toI64(b);
        if (d == 0) return .null;
        if (d == -1) return Value{ .int = 0 };
        return Value{ .int = @rem(toI64(a), d) };
    }

    fn power(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const a = try evalRow(arena, c.args[0], batch, row);
        const b = try evalRow(arena, c.args[1], batch, row);
        if (a.isNull() or b.isNull()) return .null;
        return Value{ .float = std.math.pow(f64, toF64(a), toF64(b)) };
    }

    fn sqrt(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        const x = toF64(v);
        if (x < 0) return .null;
        return Value{ .float = @sqrt(x) };
    }

    fn sign(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        const x = toF64(v);
        return Value{ .int = if (x > 0) @as(i64, 1) else if (x < 0) @as(i64, -1) else @as(i64, 0) };
    }

    fn nullif(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const a = try evalRow(arena, c.args[0], batch, row);
        if (a.isNull()) return .null;
        const b = try evalRow(arena, c.args[1], batch, row);
        if (b.isNull()) return a;
        if (compareValues(a, b)) |ord| {
            if (ord == .eq) return .null;
        }
        return a;
    }

    /// Null arguments are ignored (Postgres); all-null yields null.
    fn greatestLeast(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const want_gt = eq(c.name, "greatest");
        var best: Value = .null;
        for (c.args) |ae| {
            const v = try evalRow(arena, ae, batch, row);
            if (v.isNull()) continue;
            if (best.isNull()) {
                best = v;
                continue;
            }
            const ord = compareValues(best, v) orelse return error.TypeMismatch;
            if (if (want_gt) ord == .lt else ord == .gt) best = v;
        }
        return best;
    }

    fn pad(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const sv = try evalRow(arena, c.args[0], batch, row);
        if (sv.isNull()) return .null;
        const nv = try evalRow(arena, c.args[1], batch, row);
        if (nv.isNull()) return .null;
        var fill: []const u8 = " ";
        if (c.args.len > 2) {
            const fv = try evalRow(arena, c.args[2], batch, row);
            if (fv.isNull()) return .null;
            fill = try valueToString(arena, fv);
        }
        const s = try valueToString(arena, sv);
        return Value{ .string = try padChars(arena, s, toI64(nv), fill, eq(c.name, "lpad")) };
    }

    fn leftRight(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const sv = try evalRow(arena, c.args[0], batch, row);
        if (sv.isNull()) return .null;
        const nv = try evalRow(arena, c.args[1], batch, row);
        if (nv.isNull()) return .null;
        const s = try valueToString(arena, sv);
        return Value{ .string = try arena.dupe(u8, endSlice(s, toI64(nv), eq(c.name, "left"))) };
    }

    fn splitPart(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const sv = try evalRow(arena, c.args[0], batch, row);
        const dv = try evalRow(arena, c.args[1], batch, row);
        const nv = try evalRow(arena, c.args[2], batch, row);
        if (sv.isNull() or dv.isNull() or nv.isNull()) return .null;
        const delim = try valueToString(arena, dv);
        if (delim.len == 0) return .null;
        const want = toI64(nv);
        if (want < 1) return Value{ .string = "" };
        var it = std.mem.splitSequence(u8, try valueToString(arena, sv), delim);
        var k: i64 = 0;
        while (it.next()) |part| {
            k += 1;
            if (k == want) return Value{ .string = try arena.dupe(u8, part) };
        }
        return Value{ .string = "" };
    }

    /// The JSON array functions: the body is bound once to slots each element then
    /// overwrites. An element the body cannot compare counts as null; a failed CAST
    /// still fails. A JSON cell that is not an array is an error.
    fn jsonLambda(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        if (c.args.len != 2 or c.args[1].* != .lambda) return error.TypeMismatch;
        const dv = try evalRow(arena, c.args[0], batch, row);
        if (dv.isNull()) return .null;
        const doc = try valueToString(arena, dv);
        try json.validate(arena, doc);
        if (json.rootKind(doc) != .array) return error.InvalidJson;
        const l = c.args[1].lambda;
        const Kind = enum { filter, transform, any, all };
        const kind: Kind = if (eq(c.name, "json_filter")) .filter else if (eq(c.name, "json_transform")) .transform else if (eq(c.name, "json_any")) .any else .all;
        var out = std.Io.Writer.Allocating.init(arena);
        const w = &out.writer;
        w.writeByte('[') catch return error.OutOfMemory;
        var n: usize = 0;
        const slots = try lambdaSlots(arena, &.{ .null_lit, .{ .int_lit = 0 } });
        const body = try bindParams(arena, l, slots);
        var items = json.Elements.root(doc);
        var idx: i64 = 0;
        while (items.next()) |el| : (idx += 1) {
            slots[0].* = try literalOf(arena, try jsonElementValue(arena, el));
            slots[1].* = .{ .int_lit = idx };
            const r = evalRow(arena, body, batch, row) catch |e| switch (e) {
                error.TypeMismatch => Value.null,
                else => return e,
            };
            const holds = r == .bool and r.bool;
            switch (kind) {
                .filter, .transform => if (kind == .transform or holds) {
                    if (n > 0) w.writeByte(',') catch return error.OutOfMemory;
                    n += 1;
                    if (kind == .filter) try json.compactInto(arena, el, w) else try writeJsonValue(arena, r, w);
                },
                .any => if (holds) return Value{ .bool = true },
                .all => if (!holds) return Value{ .bool = false },
            }
        }
        return switch (kind) {
            .any => Value{ .bool = false },
            .all => Value{ .bool = true },
            .filter, .transform => blk: {
                w.writeByte(']') catch return error.OutOfMemory;
                break :blk Value{ .string = out.written() };
            },
        };
    }

    /// A fold from `initial`. Unlike `json_transform`, an element the body cannot
    /// compare fails the statement, and a float into an INT total is a CastFailed.
    fn jsonReduce(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        if (c.args.len != 3 or c.args[2].* != .lambda) return error.TypeMismatch;
        const dv = try evalRow(arena, c.args[0], batch, row);
        if (dv.isNull()) return .null;
        const doc = try valueToString(arena, dv);
        try json.validate(arena, doc);
        if (json.rootKind(doc) != .array) return error.InvalidJson;
        const ty = try reduceTypeAt(arena, c, batch);
        var acc = try evalRow(arena, c.args[1], batch, row);
        if (!ty.unknown and !acc.isNull()) acc = try castValueTyped(arena, acc, ty);
        const slots = try lambdaSlots(arena, &.{ .null_lit, .null_lit, .{ .int_lit = 0 } });
        const body = try bindParams(arena, c.args[2].lambda, slots);
        var items = json.Elements.root(doc);
        var idx: i64 = 0;
        while (items.next()) |el| : (idx += 1) {
            slots[0].* = try accNode(arena, acc, ty);
            slots[1].* = try literalOf(arena, try jsonElementValue(arena, el));
            slots[2].* = .{ .int_lit = idx };
            const r = try evalRow(arena, body, batch, row);
            if (r == .float and (ty.kind == .int or ty.kind == .decimal) and !ty.unknown)
                return failWith(error.CastFailed, "json_reduce: the lambda returned {d} into {s} accumulator — start from a FLOAT (0.0)", .{ r.float, if (ty.kind == .int) "an INT" else "a DECIMAL" });
            acc = if (r.isNull() or ty.unknown) r else try castValueTyped(arena, r, ty);
        }
        return acc;
    }

    fn chars(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        const str = try valueToString(arena, v);
        var out = std.Io.Writer.Allocating.init(arena);
        const w = &out.writer;
        w.writeByte('[') catch return error.OutOfMemory;
        var i: usize = 0;
        while (i < str.len) {
            const cw = charWidth(str, i);
            if (i > 0) w.writeByte(',') catch return error.OutOfMemory;
            std.json.Stringify.encodeJsonString(str[i..][0..cw], .{}, w) catch return error.OutOfMemory;
            i += cw;
            if (out.written().len > max_str_bytes) return failWith(error.CastFailed, "chars: the array passes {d} bytes", .{max_str_bytes});
        }
        w.writeByte(']') catch return error.OutOfMemory;
        return .{ .string = out.written() };
    }

    fn jsonRange(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        var bounds = [2]i64{ 0, 0 };
        for (c.args, bounds[2 - c.args.len ..]) |e, *b| {
            const v = try evalRow(arena, e, batch, row);
            if (v.isNull()) return .null;
            if (v != .int) return error.TypeMismatch;
            b.* = v.int;
        }
        var out = std.Io.Writer.Allocating.init(arena);
        const w = &out.writer;
        w.writeByte('[') catch return error.OutOfMemory;
        var k = bounds[0];
        while (k < bounds[1]) : (k += 1) {
            if (k > bounds[0]) w.writeByte(',') catch return error.OutOfMemory;
            w.print("{d}", .{k}) catch return error.OutOfMemory;
            if (out.written().len > max_str_bytes) return failWith(error.CastFailed, "json_range: the array passes {d} bytes", .{max_str_bytes});
        }
        w.writeByte(']') catch return error.OutOfMemory;
        return .{ .string = out.written() };
    }

    fn jsonLength(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const doc = try jsonArrayArg(arena, c.args[0], batch, row) orelse return .null;
        var items = json.Elements.root(doc);
        var n: i64 = 0;
        while (items.next()) |_| n += 1;
        return .{ .int = n };
    }

    /// Elements `start` up to (not including) `stop`, from 0; negative bounds count
    /// from the end, as Python's slices do, and bounds past either end are clamped.
    fn jsonSlice(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const doc = try jsonArrayArg(arena, c.args[0], batch, row) orelse return .null;
        var len: i64 = 0;
        var count = json.Elements.root(doc);
        while (count.next()) |_| len += 1;
        var bounds = [2]i64{ 0, len };
        for (c.args[1..], bounds[0 .. c.args.len - 1]) |e, *b| {
            const v = try evalRow(arena, e, batch, row);
            if (v.isNull()) return .null;
            if (v != .int) return error.TypeMismatch;
            b.* = std.math.clamp(if (v.int < 0) len + v.int else v.int, 0, len);
        }
        var out = std.Io.Writer.Allocating.init(arena);
        const w = &out.writer;
        w.writeByte('[') catch return error.OutOfMemory;
        var items = json.Elements.root(doc);
        var k: i64 = 0;
        var n: usize = 0;
        while (items.next()) |el| : (k += 1) {
            if (k < bounds[0] or k >= bounds[1]) continue;
            if (n > 0) w.writeByte(',') catch return error.OutOfMemory;
            n += 1;
            try json.compactInto(arena, el, w);
        }
        w.writeByte(']') catch return error.OutOfMemory;
        return .{ .string = out.written() };
    }

    fn jsonConcat(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        var out = std.Io.Writer.Allocating.init(arena);
        const w = &out.writer;
        w.writeByte('[') catch return error.OutOfMemory;
        var n: usize = 0;
        for (c.args) |e| {
            const doc = try jsonArrayArg(arena, e, batch, row) orelse return .null;
            var items = json.Elements.root(doc);
            while (items.next()) |el| {
                if (n > 0) w.writeByte(',') catch return error.OutOfMemory;
                n += 1;
                try json.compactInto(arena, el, w);
            }
            if (out.written().len > max_str_bytes) return failWith(error.CastFailed, "json_concat: the array passes {d} bytes", .{max_str_bytes});
        }
        w.writeByte(']') catch return error.OutOfMemory;
        return .{ .string = out.written() };
    }

    fn jsonGet(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const dv = try evalRow(arena, c.args[0], batch, row);
        const pv = try evalRow(arena, c.args[1], batch, row);
        if (dv.isNull() or pv.isNull()) return .null;
        const doc = try valueToString(arena, dv);
        try json.validate(arena, doc);
        const leaf = (try json.path(arena, doc, try valueToString(arena, pv))) orelse return .null;
        return switch (try json.cell(arena, leaf)) {
            .null => .null,
            .text => |t| .{ .string = t },
        };
    }

    fn strpos(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const sv = try evalRow(arena, c.args[0], batch, row);
        const pv = try evalRow(arena, c.args[1], batch, row);
        if (sv.isNull() or pv.isNull()) return .null;
        const s = try valueToString(arena, sv);
        const sub = try valueToString(arena, pv);
        if (sub.len == 0) return Value{ .int = 1 };
        const at = std.mem.indexOf(u8, s, sub) orelse return Value{ .int = 0 };
        return Value{ .int = @as(i64, @intCast(charCount(s[0..at]))) + 1 };
    }

    fn repeat(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const sv = try evalRow(arena, c.args[0], batch, row);
        if (sv.isNull()) return .null;
        const nv = try evalRow(arena, c.args[1], batch, row);
        if (nv.isNull()) return .null;
        const n = toI64(nv);
        if (n <= 0) return Value{ .string = "" };
        const s = try valueToString(arena, sv);
        const total = @as(u128, @intCast(n)) * @as(u128, s.len);
        if (total > max_str_bytes) return error.CastFailed;
        const out = try arena.alloc(u8, @intCast(total));
        var i: usize = 0;
        while (i < out.len) : (i += s.len) @memcpy(out[i..][0..s.len], s);
        return Value{ .string = out };
    }

    fn reverse(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const sv = try evalRow(arena, c.args[0], batch, row);
        if (sv.isNull()) return .null;
        return Value{ .string = try reverseChars(arena, try valueToString(arena, sv)) };
    }

    fn dateAddDiff(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        if (c.args[0].* != .str_lit) return error.TypeMismatch;
        const u = timeUnit(c.args[0].str_lit) orelse return error.TypeMismatch;
        const a = try evalRow(arena, c.args[1], batch, row);
        const b = try evalRow(arena, c.args[2], batch, row);
        if (a.isNull() or b.isNull()) return .null;
        if (eq(c.name, "date_add")) return try addUnits(b, u, toI64(a));
        const a_us = temporalMicros(a) orelse return error.TypeMismatch;
        const b_us = temporalMicros(b) orelse return error.TypeMismatch;
        return Value{ .int = dateDiff(a_us, b_us, u) };
    }

    fn makeDate(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const yv = try evalRow(arena, c.args[0], batch, row);
        const mv = try evalRow(arena, c.args[1], batch, row);
        const dv = try evalRow(arena, c.args[2], batch, row);
        if (yv.isNull() or mv.isNull() or dv.isNull()) return .null;
        const y = toI64(yv);
        const m = toI64(mv);
        const d = toI64(dv);
        if (m < 1 or m > 12) return error.CastFailed;
        if (d < 1 or d > daysInMonth(y, @intCast(m))) return error.CastFailed;
        const days = daysFromCivil(y, @intCast(m), @intCast(d));
        return Value{ .date = std.math.cast(i32, days) orelse return error.CastFailed };
    }

    fn epoch(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        const us = temporalMicros(v) orelse return error.TypeMismatch;
        return Value{ .int = @divFloor(us, 1_000_000) };
    }

    fn toTimestamp(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        return Value{ .timestamp = try mulI64(toI64(v), 1_000_000) };
    }

    fn strftime(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        const fv = try evalRow(arena, c.args[1], batch, row);
        if (fv.isNull()) return .null;
        const us = temporalMicros(v) orelse return error.TypeMismatch;
        return Value{ .string = try strftimeFmt(arena, us, try valueToString(arena, fv)) };
    }

    fn strptime(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        const fv = try evalRow(arena, c.args[1], batch, row);
        if (fv.isNull()) return .null;
        const text = try valueToString(arena, v);
        const fmt = try valueToString(arena, fv);
        const us = strptimeFmt(text, fmt) orelse {
            if (eq(c.name, "try_strptime")) return .null;
            return failWith(error.CastFailed, "strptime: '{s}' is not a date in '{s}' (try_strptime gives null)", .{ clip(text), clip(fmt) });
        };
        return .{ .timestamp = us };
    }

    fn unaccent(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        const str = try valueToString(arena, v);
        if (isAscii(str)) return .{ .string = str };
        var out = try std.array_list.Managed(u8).initCapacity(arena, str.len);
        var i: usize = 0;
        while (i < str.len) {
            const w = charWidth(str, i);
            const base = if (w == 1) null else unaccentCp(std.unicode.utf8Decode(str[i..][0..w]) catch unreachable);
            try out.appendSlice(base orelse str[i..][0..w]);
            i += w;
        }
        return .{ .string = out.items };
    }

    /// Postgres' `translate`, by characters: each character of `from` becomes the one
    /// at the same place in `to`, or is deleted when `to` is shorter.
    fn translate(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        var args: [3][]const u8 = undefined;
        for (&args, c.args) |*o, e| {
            const v = try evalRow(arena, e, batch, row);
            if (v.isNull()) return .null;
            o.* = try valueToString(arena, v);
        }
        const str, const from, const to = args;
        var out = try std.array_list.Managed(u8).initCapacity(arena, str.len);
        var i: usize = 0;
        while (i < str.len) {
            const w = charWidth(str, i);
            const ch = str[i..][0..w];
            i += w;
            var at: usize = 0;
            var k: usize = 0;
            const hit = while (k < from.len) {
                const fw = charWidth(from, k);
                if (std.mem.eql(u8, from[k..][0..fw], ch)) break at;
                k += fw;
                at += 1;
            } else null;
            const idx = hit orelse {
                try out.appendSlice(ch);
                continue;
            };
            const off = charOffset(to, idx);
            if (off < to.len) try out.appendSlice(to[off..][0..charWidth(to, off)]);
        }
        return .{ .string = out.items };
    }

    fn initcap(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        const str = try valueToString(arena, v);
        var out = try std.array_list.Managed(u8).initCapacity(arena, str.len);
        var in_word = false;
        var i: usize = 0;
        while (i < str.len) {
            const w = charWidth(str, i);
            const ch = str[i..][0..w];
            i += w;
            const cp: u21 = if (w == 1) ch[0] else std.unicode.utf8Decode(ch) catch unreachable;
            const word = isWordChar(cp, w);
            if (!word) {
                try out.appendSlice(ch);
            } else {
                var buf: [4]u8 = undefined;
                const n = std.unicode.utf8Encode(caseMap(cp, !in_word), &buf) catch unreachable;
                try out.appendSlice(buf[0..n]);
            }
            in_word = word;
        }
        return .{ .string = out.items };
    }

    fn ascii(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        const str = try valueToString(arena, v);
        if (str.len == 0) return .{ .int = 0 };
        const w = charWidth(str, 0);
        return .{ .int = if (w == 1) str[0] else std.unicode.utf8Decode(str[0..w]) catch unreachable };
    }

    fn chr(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        if (v != .int) return error.TypeMismatch;
        const bad = "chr: {d} is not a character";
        const cp = std.math.cast(u21, v.int) orelse return failWith(error.CastFailed, bad, .{v.int});
        var buf: [4]u8 = undefined;
        if (cp == 0) return failWith(error.CastFailed, bad, .{v.int});
        const n = std.unicode.utf8Encode(cp, &buf) catch return failWith(error.CastFailed, bad, .{v.int});
        return .{ .string = try arena.dupe(u8, buf[0..n]) };
    }
};

const vectorized = struct {
    fn now(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
        _ = arena;
        _ = c;
        _ = batch;
        return .{ .scalar = .{ .timestamp = std.time.microTimestamp() } };
    }

    fn today(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
        _ = arena;
        _ = c;
        _ = batch;
        return .{ .scalar = .{ .date = @intCast(@divFloor(std.time.microTimestamp(), 86_400_000_000)) } };
    }

    fn upperLower(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
        const n = batch.len;
        if (c.args.len < 1) return error.Unsupported;
        const s = try strArg(arena, c.args[0], batch);
        const up = eq(c.name, "upper");
        var scratch = std.array_list.Managed(u8).init(arena);
        var out = try column.BytesAppender.init(arena, n);
        var bm = try Bitmap.initFull(arena, n);
        var any = false;
        var i: usize = 0;
        while (i < n) : (i += 1) {
            const sv = strAt(s, i) orelse {
                try out.pushNull();
                bm.setValid(i, false);
                any = true;
                continue;
            };
            if (isAscii(sv)) {
                const o = try out.pushMutable(sv);
                for (o) |*ch| ch.* = if (up) std.ascii.toUpper(ch.*) else std.ascii.toLower(ch.*);
            } else {
                scratch.clearRetainingCapacity();
                try caseMapInto(&scratch, sv, up);
                try out.push(scratch.items);
            }
        }
        return mkCol(Type.init(.string).withNull(any), n, bm, .{ .bytes = try out.finish() });
    }

    fn trimSpace(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
        const n = batch.len;
        if (c.args.len < 1) return error.Unsupported;
        const s = try strArg(arena, c.args[0], batch);
        var out = try column.BytesAppender.init(arena, n);
        var bm = try Bitmap.initFull(arena, n);
        var any = false;
        var i: usize = 0;
        while (i < n) : (i += 1) {
            const sv = strAt(s, i) orelse {
                try out.pushNull();
                bm.setValid(i, false);
                any = true;
                continue;
            };
            try out.push(trim(sv));
        }
        return mkCol(Type.init(.string).withNull(any), n, bm, .{ .bytes = try out.finish() });
    }

    fn strlen(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
        const n = batch.len;
        if (c.args.len < 1) return error.Unsupported;
        const v = try evalVec(arena, c.args[0], batch);
        const s = asStr(v) orelse return error.Unsupported;
        const is_bytes = switch (v) {
            .col => |col| col.ty.kind == .bytes,
            .scalar => |sc| sc == .bytes,
        };
        const chars = eq(c.name, "length") and !is_bytes;
        const out = try arena.alloc(i64, n);
        var bm = try Bitmap.initFull(arena, n);
        var any = false;
        var i: usize = 0;
        while (i < n) : (i += 1) {
            if (strAt(s, i)) |sv| {
                out[i] = @intCast(if (chars) charCount(sv) else sv.len);
            } else {
                out[i] = 0;
                bm.setValid(i, false);
                any = true;
            }
        }
        return mkCol(Type.init(.int).withNull(any), n, bm, .{ .i64 = out });
    }

    fn strPredicate(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
        const name = c.name;
        const n = batch.len;
        if (c.args.len < 2) return error.Unsupported;
        const s = try strArg(arena, c.args[0], batch);
        const p = try strArg(arena, c.args[1], batch);
        const out = try arena.alloc(bool, n);
        var bm = try Bitmap.initFull(arena, n);
        var any = false;
        var i: usize = 0;
        while (i < n) : (i += 1) {
            const sv = strAt(s, i);
            const pv = strAt(p, i);
            if (sv == null or pv == null) {
                out[i] = false;
                bm.setValid(i, false);
                any = true;
                continue;
            }
            out[i] = if (eq(name, "starts_with"))
                std.mem.startsWith(u8, sv.?, pv.?)
            else if (eq(name, "ends_with"))
                std.mem.endsWith(u8, sv.?, pv.?)
            else if (eq(name, "contains"))
                std.mem.indexOf(u8, sv.?, pv.?) != null
            else
                likeMatch(sv.?, pv.?);
        }
        return mkCol(Type.init(.bool).withNull(any), n, bm, .{ .b = out });
    }

    fn concat(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
        const n = batch.len;
        if (c.args.len == 0) return error.Unsupported;
        const parts = try arena.alloc(Str, c.args.len);
        for (c.args, parts) |a, *sp| sp.* = try strArg(arena, a, batch);
        var out = try column.BytesAppender.init(arena, n);
        var bm = try Bitmap.initFull(arena, n);
        var any = false;
        var i: usize = 0;
        rows: while (i < n) : (i += 1) {
            for (parts) |sp| {
                if (strAt(sp, i) == null) {
                    try out.pushNull();
                    bm.setValid(i, false);
                    any = true;
                    continue :rows;
                }
            }
            for (parts) |sp| try out.append(strAt(sp, i).?);
            try out.endRow();
        }
        return mkCol(Type.init(.string).withNull(any), n, bm, .{ .bytes = try out.finish() });
    }

    fn coalesce(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
        const n = batch.len;
        if (c.args.len == 0) return error.Unsupported;
        const parts = try arena.alloc(Str, c.args.len);
        for (c.args, parts) |a, *sp| sp.* = try strArg(arena, a, batch);
        var out = try column.BytesAppender.init(arena, n);
        var bm = try Bitmap.initFull(arena, n);
        var any = false;
        var i: usize = 0;
        rows: while (i < n) : (i += 1) {
            for (parts) |sp| {
                if (strAt(sp, i)) |sv| {
                    try out.push(sv);
                    continue :rows;
                }
            }
            try out.pushNull();
            bm.setValid(i, false);
            any = true;
        }
        return mkCol(Type.init(.string).withNull(any), n, bm, .{ .bytes = try out.finish() });
    }

    fn substr(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
        const n = batch.len;
        if (c.args.len < 2) return error.Unsupported;
        const s = try strArg(arena, c.args[0], batch);
        const start = (try asNum(arena, try evalVec(arena, c.args[1], batch), n)) orelse return error.Unsupported;
        if (!isIntNum(start)) return error.Unsupported;
        var len_num: ?Num = null;
        if (c.args.len > 2) {
            len_num = (try asNum(arena, try evalVec(arena, c.args[2], batch), n)) orelse return error.Unsupported;
            if (!isIntNum(len_num.?)) return error.Unsupported;
        }
        var out = try column.BytesAppender.init(arena, n);
        var bm = try Bitmap.initFull(arena, n);
        var any = false;
        var i: usize = 0;
        while (i < n) : (i += 1) {
            const sv = strAt(s, i);
            const start_ok = numValid(start, i);
            const len_ok = if (len_num) |l| numValid(l, i) else true;
            if (sv == null or !start_ok or !len_ok) {
                try out.pushNull();
                bm.setValid(i, false);
                any = true;
                continue;
            }
            try out.push(try substrChars(arena, sv.?, numI(start, i), if (len_num) |l| numI(l, i) else null));
        }
        return mkCol(Type.init(.string).withNull(any), n, bm, .{ .bytes = try out.finish() });
    }

    fn replace(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
        const n = batch.len;
        if (c.args.len < 3) return error.Unsupported;
        const s = try strArg(arena, c.args[0], batch);
        const f = try strArg(arena, c.args[1], batch);
        const t = try strArg(arena, c.args[2], batch);
        var out = try column.BytesAppender.init(arena, n);
        var bm = try Bitmap.initFull(arena, n);
        var any = false;
        var i: usize = 0;
        while (i < n) : (i += 1) {
            const sv = strAt(s, i);
            const fv = strAt(f, i);
            const tv = strAt(t, i);
            if (sv == null or fv == null or tv == null) {
                try out.pushNull();
                bm.setValid(i, false);
                any = true;
                continue;
            }
            if (fv.?.len == 0) {
                try out.push(sv.?);
                continue;
            }
            const o = try arena.alloc(u8, std.mem.replacementSize(u8, sv.?, fv.?, tv.?));
            _ = std.mem.replace(u8, sv.?, fv.?, tv.?, o);
            try out.push(o);
        }
        return mkCol(Type.init(.string).withNull(any), n, bm, .{ .bytes = try out.finish() });
    }
};

/// Cast honouring the target's scale; `castValue` only sees a `TypeKind`, which
/// for `DECIMAL(p, s)` left the conversion undefined.
pub fn castValueTyped(arena: std.mem.Allocator, v: Value, ty: types.Type) EvalError!Value {
    if (ty.kind != .decimal) return castValue(arena, v, ty.kind);
    const d: Decimal = switch (v) {
        .decimal => |x| x,
        .int => |x| .{ .unscaled = x, .scale = 0 },
        .bool => |x| .{ .unscaled = if (x) 1 else 0, .scale = 0 },
        .float => |x| floatToDecimal(x) orelse return error.CastFailed,
        .string, .bytes => |str| switch (sql.parseDecimalText(trim(str)) orelse return error.CastFailed) {
            .decimal => |x| x,
            .int => |x| Decimal{ .unscaled = x, .scale = 0 },
            else => return error.CastFailed,
        },
        else => return error.CastFailed,
    };
    return .{ .decimal = rescaleTo(d, ty.scale) orelse return error.CastFailed };
}

fn powTen(n: u8) i128 {
    var r: i128 = 1;
    var i: u8 = 0;
    while (i < n) : (i += 1) r *= 10;
    return r;
}

/// A float as the decimal its 15 significant digits spell, as PostgreSQL casts
/// float8 to numeric. Null for a non-finite value or one past 38 digits.
pub fn floatToDecimal(x: f64) ?Decimal {
    if (!std.math.isFinite(x)) return null;
    if (x == 0) return .{ .unscaled = 0, .scale = 0 };
    var buf: [48]u8 = undefined;
    const s = std.fmt.bufPrint(&buf, "{e:.14}", .{@abs(x)}) catch return null;
    const e_at = std.mem.indexOfScalar(u8, s, 'e') orelse return null;
    var m: i128 = 0;
    for (s[0..e_at]) |c| {
        if (c == '.') continue;
        m = m * 10 + (c - '0');
    }
    const exp = std.fmt.parseInt(i32, s[e_at + 1 ..], 10) catch return null;
    if (x < 0) m = -m;
    const shift = exp - 14;
    if (shift >= 0) {
        if (shift > 23) return null;
        return .{ .unscaled = std.math.mul(i128, m, powTen(@intCast(shift))) catch return null, .scale = 0 };
    }
    const sc = -shift;
    if (sc <= 38) return .{ .unscaled = m, .scale = @intCast(sc) };
    return .{ .unscaled = roundScaleDown(m, @intCast(sc - 38)), .scale = 38 };
}

/// The scale `round(decimal, digits)` answers in: the digits when they are a
/// literal (as DuckDB types it), else the input's own.
fn roundOutScale(c: ast.Expr.Call, in_scale: u8) u8 {
    if (c.args.len < 2) return 0;
    if (c.args[1].* != .int_lit) return in_scale;
    return @intCast(std.math.clamp(c.args[1].int_lit, 0, in_scale));
}

fn roundDecimal(d: Decimal, digits: i64, out_scale: u8) ?Decimal {
    var r = d;
    if (digits < d.scale) {
        const drop = @as(i64, d.scale) - digits;
        if (drop > 38) return .{ .unscaled = 0, .scale = out_scale };
        var q = roundScaleDown(d.unscaled, @intCast(drop));
        if (digits < 0) {
            q = std.math.mul(i128, q, powTen(@intCast(@min(-digits, 38)))) catch return null;
            r = .{ .unscaled = q, .scale = 0 };
        } else r = .{ .unscaled = q, .scale = @intCast(digits) };
    }
    return rescaleTo(r, out_scale);
}

/// `u / 10^drop`, rounded half away from zero.
pub fn roundScaleDown(u: i128, drop: u32) i128 {
    if (drop == 0) return u;
    if (drop > 38) return 0;
    const p = powTen(@intCast(drop));
    const q = @divTrunc(u, p);
    const r = @rem(u, p);
    const twice = @abs(r) * 2;
    if (twice >= @abs(p)) return if (u < 0) q - 1 else q + 1;
    return q;
}

/// Shifts a decimal to `want`, rounding half away from zero; null on overflow.
/// Public because a value's scale need not match its column's (Postgres sends a
/// per-value `dscale`), so anything combining decimals across rows normalizes first.
pub fn rescaleTo(d: Decimal, want: u8) ?Decimal {
    var unscaled = d.unscaled;
    var have: i32 = d.scale;
    while (have < want) : (have += 1) {
        unscaled = std.math.mul(i128, unscaled, 10) catch return null;
    }
    if (have > want) unscaled = roundScaleDown(unscaled, @intCast(have - want));
    return .{ .unscaled = unscaled, .scale = want };
}

pub fn castValue(arena: std.mem.Allocator, v: Value, kind: types.TypeKind) EvalError!Value {
    return switch (kind) {
        .int => switch (v) {
            .int => v,
            .float => |x| .{ .int = try floatToInt(x) },
            .decimal => |d| .{ .int = std.math.cast(i64, (rescaleTo(d, 0) orelse return error.IntOverflow).unscaled) orelse return error.IntOverflow },
            .bool => |x| .{ .int = if (x) 1 else 0 },
            .string => |s| .{ .int = std.fmt.parseInt(i64, trim(s), 10) catch return error.CastFailed },
            else => error.CastFailed,
        },
        .float => switch (v) {
            .float => v,
            .int => |x| .{ .float = @floatFromInt(x) },
            .decimal => |d| .{ .float = d.toF64() },
            .string => |s| .{ .float = std.fmt.parseFloat(f64, trim(s)) catch return error.CastFailed },
            else => error.CastFailed,
        },
        .date => switch (v) {
            .date => v,
            .timestamp => |x| .{ .date = @intCast(@divFloor(x, 86_400_000_000)) },
            .string => |str| .{ .date = @intCast(parseIsoDate(str) orelse return error.CastFailed) },
            else => error.CastFailed,
        },
        .timestamp => switch (v) {
            .timestamp => v,
            .date => |x| .{ .timestamp = @as(i64, x) * 86_400_000_000 },
            .string => |str| .{ .timestamp = parseIsoTimestamp(str) orelse return error.CastFailed },
            else => error.CastFailed,
        },
        .time => switch (v) {
            .time => v,
            .timestamp => |x| .{ .time = @mod(x, 86_400_000_000) },
            .string => |str| .{ .time = parseIsoTime(str) orelse return error.CastFailed },
            else => error.CastFailed,
        },
        .string => .{ .string = try valueToString(arena, v) },
        .bool => switch (v) {
            .bool => v,
            .int => |x| .{ .bool = x != 0 },
            .string => |s| if (std.ascii.eqlIgnoreCase(trim(s), "true"))
                Value{ .bool = true }
            else if (std.ascii.eqlIgnoreCase(trim(s), "false"))
                Value{ .bool = false }
            else
                error.CastFailed,
            else => error.CastFailed,
        },
        else => error.CastFailed,
    };
}

pub fn writeValue(w: anytype, v: Value) !void {
    switch (v) {
        .null => {},
        .string, .bytes => |x| try w.writeAll(x),
        .bool => |b| try w.writeAll(if (b) "true" else "false"),
        .int => |x| try w.print("{d}", .{x}),
        .float => |x| try w.print("{d}", .{x}),
        .decimal => |d| try writeDecimal(w, d.unscaled, d.scale),
        .date => |x| try writeDate(w, x),
        .time => |x| try writeTime(w, x),
        .timestamp => |x| try writeTimestamp(w, x),
    }
}

pub fn writeDate(w: anytype, days: i64) !void {
    const c = civilFromDays(days);
    try writeYear(w, c.y);
    try w.print("-{d:0>2}-{d:0>2}", .{ c.m, c.d });
}

/// Four digits, zero-padded; a year before 0 carries a leading `-`, as ISO
/// 8601's expanded form does (printing one used to trap on the cast).
fn writeYear(w: anytype, y: i64) !void {
    if (y < 0) try w.writeByte('-');
    try w.print("{d:0>4}", .{@abs(y)});
}

pub fn writeTime(w: anytype, t: i64) !void {
    const us: u64 = @intCast(@mod(t, 86_400_000_000));
    const secs = us / 1_000_000;
    const frac = us % 1_000_000;
    if (frac != 0) {
        try w.print("{d:0>2}:{d:0>2}:{d:0>2}.{d:0>6}", .{ secs / 3600, (secs % 3600) / 60, secs % 60, frac });
    } else {
        try w.print("{d:0>2}:{d:0>2}:{d:0>2}", .{ secs / 3600, (secs % 3600) / 60, secs % 60 });
    }
}

pub fn writeTimestamp(w: anytype, micros: i64) !void {
    const days = @divFloor(micros, 86_400_000_000);
    const us: u64 = @intCast(micros - days * 86_400_000_000);
    const secs = us / 1_000_000;
    const frac = us % 1_000_000;
    const c = civilFromDays(days);
    try writeYear(w, c.y);
    if (frac != 0) {
        try w.print("-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}.{d:0>6}", .{
            c.m, c.d, secs / 3600, (secs % 3600) / 60, secs % 60, frac,
        });
    } else {
        try w.print("-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}", .{
            c.m, c.d, secs / 3600, (secs % 3600) / 60, secs % 60,
        });
    }
}

pub fn writeDecimal(w: anytype, unscaled: i128, scale: u8) !void {
    const neg = unscaled < 0;
    var mag: u128 = if (neg) @intCast(-unscaled) else @intCast(unscaled);

    var digits: [48]u8 = undefined;
    var n: usize = 0;
    if (mag == 0) {
        digits[0] = '0';
        n = 1;
    }
    while (mag > 0) : (mag /= 10) {
        digits[n] = @intCast('0' + mag % 10);
        n += 1;
    }
    while (n <= scale) : (n += 1) digits[n] = '0';

    if (neg) try w.writeByte('-');
    var k: usize = n;
    while (k > 0) {
        k -= 1;
        try w.writeByte(digits[k]);
        if (scale > 0 and k == scale) try w.writeByte('.');
    }
}

pub fn valueToString(arena: std.mem.Allocator, v: Value) ![]const u8 {
    return switch (v) {
        .null => "",
        .string => |s| s,
        .bytes => |s| s,
        .bool => |b| if (b) "true" else "false",
        .int => |x| try std.fmt.allocPrint(arena, "{d}", .{x}),
        .float => |x| try std.fmt.allocPrint(arena, "{d}", .{x}),
        .decimal => |d| try formatDecimal(arena, d.unscaled, d.scale),
        .date => |x| try formatDate(arena, x),
        .time => |x| try formatTime(arena, x),
        .timestamp => |x| try formatTimestamp(arena, x),
    };
}

const fmt_bound = 128;

/// Renders through `writeDate` into a fixed `fmt_bound` buffer, which cannot
/// overflow (the widest output is 17 bytes), so the catch is `unreachable`.
pub fn formatDate(arena: std.mem.Allocator, days: i64) ![]const u8 {
    var buf: [fmt_bound]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    writeDate(&w, days) catch unreachable;
    return arena.dupe(u8, w.buffered());
}

pub fn formatTime(arena: std.mem.Allocator, t: i64) ![]const u8 {
    var buf: [fmt_bound]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    writeTime(&w, t) catch unreachable;
    return arena.dupe(u8, w.buffered());
}

pub fn formatTimestamp(arena: std.mem.Allocator, micros: i64) ![]const u8 {
    var buf: [fmt_bound]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    writeTimestamp(&w, micros) catch unreachable;
    return arena.dupe(u8, w.buffered());
}

pub const TimeUnit = enum { year, month, week, day, hour, minute, second };

fn isoWeekStart(day: i64) i64 {
    return day - @mod(day + 3, 7);
}

pub fn timeUnit(name: []const u8) ?TimeUnit {
    var buf: [16]u8 = undefined;
    if (name.len == 0 or name.len >= buf.len) return null;
    return std.meta.stringToEnum(TimeUnit, std.ascii.lowerString(buf[0..name.len], name));
}

fn temporalMicros(v: Value) ?i64 {
    return switch (v) {
        .timestamp => |x| x,
        .date => |x| @as(i64, x) * 86_400_000_000,
        else => null,
    };
}

fn truncMicros(us: i64, u: TimeUnit) i64 {
    const day = @divFloor(us, 86_400_000_000);
    const rem = us - day * 86_400_000_000;
    return switch (u) {
        .second => us - @mod(rem, 1_000_000),
        .minute => us - @mod(rem, 60_000_000),
        .hour => us - @mod(rem, 3_600_000_000),
        .day => day * 86_400_000_000,
        .week => isoWeekStart(day) * 86_400_000_000,
        .month => blk: {
            const c = civilFromDays(day);
            break :blk daysFromCivil(c.y, c.m, 1) * 86_400_000_000;
        },
        .year => blk: {
            const c = civilFromDays(day);
            break :blk daysFromCivil(c.y, 1, 1) * 86_400_000_000;
        },
    };
}

fn extractField(us: i64, u: TimeUnit) i64 {
    const day = @divFloor(us, 86_400_000_000);
    const rem = us - day * 86_400_000_000;
    const c = civilFromDays(day);
    return switch (u) {
        .year => c.y,
        .month => @intCast(c.m),
        .week => blk: {
            const thu = isoWeekStart(day) + 3;
            const ty = civilFromDays(thu).y;
            break :blk @divFloor(thu - daysFromCivil(ty, 1, 1), 7) + 1;
        },
        .day => @intCast(c.d),
        .hour => @divFloor(rem, 3_600_000_000),
        .minute => @mod(@divFloor(rem, 60_000_000), 60),
        .second => @mod(@divFloor(rem, 1_000_000), 60),
    };
}

fn mulI64(a: i64, b: i64) EvalError!i64 {
    return std.math.mul(i64, a, b) catch return error.CastFailed;
}
fn addI64(a: i64, b: i64) EvalError!i64 {
    return std.math.add(i64, a, b) catch return error.CastFailed;
}

fn isLeapYear(y: i64) bool {
    return @mod(y, 4) == 0 and (@mod(y, 100) != 0 or @mod(y, 400) == 0);
}

fn daysInMonth(y: i64, m: u32) u32 {
    const lens = [_]u32{ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    if (m == 2 and isLeapYear(y)) return 29;
    return lens[m - 1];
}

/// Adds `n` calendar months, clamping the day to the target month's length;
/// the clamp is not remembered (Jan 31 +1 is Feb 29, +2 is Mar 29), as Postgres does.
fn addMonthsToDays(days: i64, n: i64) i64 {
    const c = civilFromDays(days);
    const total = c.y * 12 + @as(i64, c.m) - 1 + n;
    const y = @divFloor(total, 12);
    const m: u32 = @intCast(@mod(total, 12) + 1);
    return daysFromCivil(y, m, @min(c.d, daysInMonth(y, m)));
}

fn addUnits(v: Value, u: TimeUnit, n: i64) EvalError!Value {
    switch (v) {
        .date => |d0| {
            const days: i64 = d0;
            const nd = switch (u) {
                .year => addMonthsToDays(days, try mulI64(n, 12)),
                .month => addMonthsToDays(days, n),
                .week => try addI64(days, try mulI64(n, 7)),
                .day => try addI64(days, n),
                .hour, .minute, .second => return error.TypeMismatch,
            };
            return .{ .date = std.math.cast(i32, nd) orelse return error.CastFailed };
        },
        .timestamp => |us| {
            const out: i64 = switch (u) {
                .year, .month => blk: {
                    const day = @divFloor(us, 86_400_000_000);
                    const rem = us - day * 86_400_000_000;
                    const months = if (u == .year) try mulI64(n, 12) else n;
                    const shifted = try mulI64(addMonthsToDays(day, months), 86_400_000_000);
                    break :blk try addI64(shifted, rem);
                },
                .week => try addI64(us, try mulI64(n, 7 * 86_400_000_000)),
                .day => try addI64(us, try mulI64(n, 86_400_000_000)),
                .hour => try addI64(us, try mulI64(n, 3_600_000_000)),
                .minute => try addI64(us, try mulI64(n, 60_000_000)),
                .second => try addI64(us, try mulI64(n, 1_000_000)),
            };
            return .{ .timestamp = out };
        },
        else => return error.TypeMismatch,
    }
}

/// DuckDB semantics: `year`/`month`/`week` count unit boundaries crossed, so
/// 2023-12-31 to 2024-01-01 is one year; `day` and finer divide the elapsed time and truncate.
fn dateDiff(a_us: i64, b_us: i64, u: TimeUnit) i64 {
    switch (u) {
        .year, .month => {
            const ca = civilFromDays(@divFloor(a_us, 86_400_000_000));
            const cb = civilFromDays(@divFloor(b_us, 86_400_000_000));
            if (u == .year) return cb.y - ca.y;
            return (cb.y * 12 + @as(i64, cb.m)) - (ca.y * 12 + @as(i64, ca.m));
        },
        .week => {
            const wa = isoWeekStart(@divFloor(a_us, 86_400_000_000));
            const wb = isoWeekStart(@divFloor(b_us, 86_400_000_000));
            return @divExact(wb - wa, 7);
        },
        .day => return @divTrunc(b_us - a_us, 86_400_000_000),
        .hour => return @divTrunc(b_us - a_us, 3_600_000_000),
        .minute => return @divTrunc(b_us - a_us, 60_000_000),
        .second => return @divTrunc(b_us - a_us, 1_000_000),
    }
}

pub fn badStrftime(fmt: []const u8) ?[]const u8 {
    var i: usize = 0;
    while (i < fmt.len) : (i += 1) {
        if (fmt[i] != '%') continue;
        i += 1;
        if (i >= fmt.len) return fmt[fmt.len - 1 ..];
        switch (fmt[i]) {
            'Y', 'y', 'm', 'd', 'H', 'M', 'S', '%' => {},
            else => return fmt[i .. i + 1],
        }
    }
    return null;
}

/// `strftime` over exactly `%Y %m %d %H %M %S %y %%`; any other directive is an
/// error, never a silent passthrough.
fn strftimeFmt(arena: std.mem.Allocator, us: i64, fmt: []const u8) EvalError![]const u8 {
    const day = @divFloor(us, 86_400_000_000);
    const rem: u64 = @intCast(us - day * 86_400_000_000);
    const secs = rem / 1_000_000;
    const c = civilFromDays(day);
    const year: u32 = if (c.y < 0) 0 else @intCast(c.y);
    var out = std.array_list.Managed(u8).init(arena);
    const w = out.writer();
    var i: usize = 0;
    while (i < fmt.len) : (i += 1) {
        if (fmt[i] != '%') {
            try out.append(fmt[i]);
            continue;
        }
        i += 1;
        if (i >= fmt.len) return error.CastFailed;
        switch (fmt[i]) {
            'Y' => try w.print("{d:0>4}", .{year}),
            'y' => try w.print("{d:0>2}", .{year % 100}),
            'm' => try w.print("{d:0>2}", .{c.m}),
            'd' => try w.print("{d:0>2}", .{c.d}),
            'H' => try w.print("{d:0>2}", .{secs / 3600}),
            'M' => try w.print("{d:0>2}", .{(secs % 3600) / 60}),
            'S' => try w.print("{d:0>2}", .{secs % 60}),
            '%' => try out.append('%'),
            else => return error.CastFailed,
        }
    }
    return try out.toOwnedSlice();
}

/// Numbers take up to their width in digits, `%y` pivots as POSIX does (69-99 are
/// the 1900s), and the whole text must be consumed. Null for a date that does not exist.
fn strptimeFmt(text: []const u8, fmt: []const u8) ?i64 {
    var y: i64 = 1970;
    var mo: u32 = 1;
    var d: u32 = 1;
    var h: i64 = 0;
    var mi: i64 = 0;
    var sec: i64 = 0;
    var t: usize = 0;
    var i: usize = 0;
    while (i < fmt.len) : (i += 1) {
        if (fmt[i] != '%' or (i + 1 < fmt.len and fmt[i + 1] == '%')) {
            if (fmt[i] == '%') i += 1;
            if (t >= text.len or text[t] != fmt[i]) return null;
            t += 1;
            continue;
        }
        i += 1;
        if (i >= fmt.len) return null;
        const width: usize = if (fmt[i] == 'Y') 4 else 2;
        const start = t;
        var n: i64 = 0;
        while (t < text.len and t - start < width and std.ascii.isDigit(text[t])) : (t += 1) n = n * 10 + (text[t] - '0');
        if (t == start) return null;
        switch (fmt[i]) {
            'Y' => y = n,
            'y' => y = if (n >= 69) 1900 + n else 2000 + n,
            'm' => mo = std.math.cast(u32, n) orelse return null,
            'd' => d = std.math.cast(u32, n) orelse return null,
            'H' => h = n,
            'M' => mi = n,
            'S' => sec = n,
            else => return null,
        }
    }
    if (t != text.len) return null;
    if (mo < 1 or mo > 12 or d < 1 or d > daysInMonth(y, mo) or h > 23 or mi > 59 or sec > 59) return null;
    return (daysFromCivil(y, mo, d) * 86_400 + h * 3600 + mi * 60 + sec) * 1_000_000;
}

pub fn daysFromCivil(y0: i64, m: u32, d: u32) i64 {
    const y = if (m <= 2) y0 - 1 else y0;
    const era = @divFloor(if (y >= 0) y else y - 399, 400);
    const yoe = y - era * 400;
    const mp: i64 = @intCast((m + 9) % 12);
    const doy = @divFloor(153 * mp + 2, 5) + @as(i64, @intCast(d)) - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146097 + doe - 719468;
}

fn isoNum(s: []const u8) ?i64 {
    var v: i64 = 0;
    for (s) |c| {
        if (c < '0' or c > '9') return null;
        v = v * 10 + @as(i64, c - '0');
    }
    return v;
}

/// Strict on purpose: a literal that is not an ISO date must fail, so
/// `date_col = '01/07/2013'` is an error, not a silently wrong comparison.
pub fn parseIsoDate(s0: []const u8) ?i64 {
    const s = trim(s0);
    if (s.len != 10 or s[4] != '-' or s[7] != '-') return null;
    const y = isoNum(s[0..4]) orelse return null;
    const m = isoNum(s[5..7]) orelse return null;
    const d = isoNum(s[8..10]) orelse return null;
    if (m < 1 or m > 12 or d < 1 or d > 31) return null;
    return daysFromCivil(y, @intCast(m), @intCast(d));
}

/// `HH:MM:SS[.ffffff]` or `HH:MM` as microseconds since midnight, the text a
/// `time` prints as, using the timestamp parser's rules on a fixed date.
pub fn parseIsoTime(s0: []const u8) ?i64 {
    const s = trim(s0);
    if (s.len == 5 and s[2] == ':') {
        const hh = isoNum(s[0..2]) orelse return null;
        const mm = isoNum(s[3..5]) orelse return null;
        if (hh > 23 or mm > 59) return null;
        return (hh * 3600 + mm * 60) * 1_000_000;
    }
    if (s.len < 8 or s.len > 8 + 7) return null;
    var buf: [32]u8 = undefined;
    const ts = std.fmt.bufPrint(&buf, "1970-01-01 {s}", .{s}) catch return null;
    return parseIsoTimestamp(ts);
}

/// `YYYY-MM-DD[ HH:MM:SS[.ffffff]]` as microseconds since the epoch. The fraction
/// is kept: dropping it once made sub-second rows identical under DISTINCT.
pub fn parseIsoTimestamp(s0: []const u8) ?i64 {
    const s = trim(s0);
    if (s.len == 10) return (parseIsoDate(s) orelse return null) * 86_400_000_000;
    if (s.len < 19 or s[13] != ':' or s[16] != ':') return null;
    const days = parseIsoDate(s[0..10]) orelse return null;
    const hh = isoNum(s[11..13]) orelse return null;
    const mm = isoNum(s[14..16]) orelse return null;
    const ss = isoNum(s[17..19]) orelse return null;
    if (hh > 23 or mm > 59 or ss > 59) return null;
    var frac: i64 = 0;
    if (s.len > 20 and s[19] == '.') {
        var i: usize = 20;
        var scale: i64 = 100_000;
        while (i < s.len and scale > 0 and s[i] >= '0' and s[i] <= '9') : (i += 1) {
            frac += @as(i64, s[i] - '0') * scale;
            scale = @divTrunc(scale, 10);
        }
        if (i != s.len) return null;
    } else if (s.len != 19) return null;
    return days * 86_400_000_000 + (hh * 3600 + mm * 60 + ss) * 1_000_000 + frac;
}

pub fn civilFromDays(z0: i64) struct { y: i64, m: u32, d: u32 } {
    const z = z0 + 719468;
    const era = @divFloor(if (z >= 0) z else z - 146096, 146097);
    const doe = z - era * 146097;
    const yoe = @divFloor(doe - @divFloor(doe, 1460) + @divFloor(doe, 36524) - @divFloor(doe, 146096), 365);
    const y = yoe + era * 400;
    const doy = doe - (365 * yoe + @divFloor(yoe, 4) - @divFloor(yoe, 100));
    const mp = @divFloor(5 * doy + 2, 153);
    const d: u32 = @intCast(doy - @divFloor(153 * mp + 2, 5) + 1);
    const m: u32 = @intCast(if (mp < 10) mp + 3 else mp - 9);
    return .{ .y = y + (if (m <= 2) @as(i64, 1) else 0), .m = m, .d = d };
}

test "parseIsoTime: the text a time prints as, and nothing out of range" {
    try std.testing.expectEqual(@as(?i64, 3_723_000_000), parseIsoTime("01:02:03"));
    try std.testing.expectEqual(@as(?i64, 86_399_999_999), parseIsoTime("23:59:59.999999"));
    try std.testing.expectEqual(@as(?i64, 45_000_000_000), parseIsoTime(" 12:30 "));
    try std.testing.expectEqual(@as(?i64, null), parseIsoTime("24:00:00"));
    try std.testing.expectEqual(@as(?i64, null), parseIsoTime("1:02:03"));
    try std.testing.expectEqual(@as(?i64, null), parseIsoTime("01:02:03 extra"));
}

test "decimals lose digits by rounding half away from zero, on every path" {
    const cases = [_]struct { u: i128, s: u8, to: u8, want: i128 }{
        .{ .u = 12345, .s = 3, .to = 2, .want = 1235 },
        .{ .u = -12345, .s = 3, .to = 2, .want = -1235 },
        .{ .u = 12344, .s = 3, .to = 2, .want = 1234 },
        .{ .u = 5, .s = 3, .to = 2, .want = 1 },
        .{ .u = -4, .s = 3, .to = 2, .want = 0 },
        .{ .u = 999, .s = 3, .to = 0, .want = 1 },
        .{ .u = std.math.maxInt(i128), .s = 38, .to = 0, .want = 2 },
    };
    for (cases) |c| try std.testing.expectEqual(c.want, rescaleTo(.{ .unscaled = c.u, .scale = c.s }, c.to).?.unscaled);

    const floats = [_]struct { x: f64, to: u8, want: i128 }{
        .{ .x = 12.345, .to = 2, .want = 1235 },
        .{ .x = -12.345, .to = 2, .want = -1235 },
        .{ .x = 1.005, .to = 2, .want = 101 },
        .{ .x = 2.675, .to = 2, .want = 268 },
        .{ .x = 0.1 + 0.2, .to = 17, .want = 30000000000000000 },
        .{ .x = 1e-300, .to = 2, .want = 0 },
        .{ .x = 123456789012345678.0, .to = 0, .want = 123456789012346000 },
    };
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    for (floats) |c| {
        const got = try castValueTyped(ar.allocator(), .{ .float = c.x }, types.Type.decimal(38, c.to));
        try std.testing.expectEqual(c.want, got.decimal.unscaled);
    }
    try std.testing.expectError(error.CastFailed, castValueTyped(ar.allocator(), .{ .float = 1e300 }, types.Type.decimal(10, 2)));
    try std.testing.expectError(error.CastFailed, castValueTyped(ar.allocator(), .{ .float = std.math.nan(f64) }, types.Type.decimal(10, 2)));
    try std.testing.expectEqual(@as(i128, -1235), (try castValueTyped(ar.allocator(), .{ .string = "-12.345" }, types.Type.decimal(10, 2))).decimal.unscaled);
}

test "dates and timestamps before year 0 print with a sign instead of trapping" {
    var buf: [96]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try writeDate(&w, -1_000_000);
    try w.writeByte(' ');
    try writeTimestamp(&w, -1_000_000 * 86_400_000_000 + 1);
    const got = w.buffered();
    try std.testing.expect(got[0] == '-');
    try std.testing.expect(std.mem.endsWith(u8, got, " 00:00:00.000001"));
}

test "format temporal values for text sinks" {
    const alloc = std.testing.allocator;
    const cases = .{
        .{ try formatDate(alloc, 0), "1970-01-01" },
        .{ try formatDate(alloc, -1), "1969-12-31" },
        .{ try formatTimestamp(alloc, 0), "1970-01-01 00:00:00" },
        .{ try formatTimestamp(alloc, 86_400_000_000 + (1 * 3600 + 2 * 60 + 3) * 1_000_000), "1970-01-02 01:02:03" },
    };
    inline for (cases) |c| {
        defer alloc.free(c[0]);
        try std.testing.expectEqualStrings(c[1], c[0]);
    }
}

pub fn formatDecimal(arena: std.mem.Allocator, unscaled: i128, scale: u8) ![]const u8 {
    var buf: [fmt_bound]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    writeDecimal(&w, unscaled, scale) catch unreachable;
    return arena.dupe(u8, w.buffered());
}

threadlocal var field_memo: [8]usize = @splat(0);

/// Hashes length, first and last byte (names like `col36`..`col39` share the
/// first two) into the memo. A qualified name skips the memo, which is by name alone.
fn fieldIndex(schema: types.Schema, q: ast.QualName) ?usize {
    if (q.parts.len > 1) return schema.resolve(q.parts);
    const name = lastPart(q);
    if (name.len == 0) return schema.indexOf(name);
    const slot = (name.len *% 31 +% name[0] *% 7 +% name[name.len - 1]) & (field_memo.len - 1);
    const cached = field_memo[slot];
    if (cached < schema.fields.len and std.mem.eql(u8, schema.fields[cached].name, name)) return cached;
    const idx = schema.indexOf(name) orelse return null;
    field_memo[slot] = idx;
    return idx;
}
fn lastPart(q: ast.QualName) []const u8 {
    return q.parts[q.parts.len - 1];
}
fn isNum(v: Value) bool {
    return v == .int or v == .float or v == .decimal;
}
pub fn toF64(v: Value) f64 {
    return switch (v) {
        .int => |x| @floatFromInt(x),
        .float => |x| x,
        .decimal => |d| d.toF64(),
        else => 0,
    };
}

const RegexCache = struct {
    buf: [16 * 1024]u8 = undefined,
    src: []const u8 = &.{},
    re: regex.Regex = undefined,
    valid: bool = false,
};
threadlocal var regex_cache: RegexCache = .{};

threadlocal var fail_note: struct { err: ?anyerror = null, buf: [480]u8 = undefined, len: usize = 0 } = .{};

pub fn explain(e: anyerror, msg: []const u8) anyerror {
    const n = &fail_note;
    const k = @min(msg.len, n.buf.len);
    @memcpy(n.buf[0..k], msg[0..k]);
    n.len = k;
    n.err = e;
    return e;
}

fn failWith(e: EvalError, comptime fmt: []const u8, args: anytype) EvalError {
    const n = &fail_note;
    const msg = std.fmt.bufPrint(&n.buf, fmt, args) catch blk: {
        @memcpy(n.buf[n.buf.len - 3 ..], "...");
        break :blk n.buf[0..];
    };
    n.len = msg.len;
    n.err = e;
    return e;
}

/// A CAST failure naming the value and target, with the format a date or time is
/// read in, since that is what a file in another convention trips over.
fn castFailure(arena: std.mem.Allocator, v: Value, ty: Type) EvalError {
    const text = clip(valueToString(arena, v) catch "?");
    const want: []const u8 = switch (ty.kind) {
        .int => "an INT",
        .float => "a FLOAT",
        .bool => "a BOOL",
        .decimal => "a DECIMAL",
        .date => "a DATE (YYYY-MM-DD; strptime reads other formats)",
        .timestamp => "a TIMESTAMP (YYYY-MM-DD HH:MM:SS; strptime reads other formats)",
        .time => "a TIME (HH:MM[:SS])",
        else => @tagName(ty.kind),
    };
    if (ty.kind == .decimal)
        return failWith(error.CastFailed, "CAST: '{s}' is not a DECIMAL({d},{d})", .{ text, ty.precision, ty.scale });
    return failWith(error.CastFailed, "CAST: '{s}' is not {s}", .{ text, want });
}

inline fn forgetFailure() void {
    if (fail_note.err != null) fail_note.err = null;
}

pub fn takeFailure(e: anyerror) ?[]const u8 {
    const n = &fail_note;
    if (n.err == null or n.err.? != e) return null;
    n.err = null;
    return n.buf[0..n.len];
}

fn clip(s: []const u8) []const u8 {
    const max = 80;
    return if (s.len <= max) s else s[0..max];
}

const RegexMatch = struct { s: []const u8, span: ?[2]usize, caps: regex.Captures };

fn regexpFind(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!?RegexMatch {
    const v = try evalRow(arena, c.args[0], batch, row);
    if (v.isNull()) return null;
    const pat = try evalRow(arena, c.args[1], batch, row);
    if (pat.isNull()) return null;
    const re = cachedRegex(try valueToString(arena, pat)) catch |e| switch (e) {
        error.OutOfMemory => return error.OutOfMemory,
        error.BadPattern => return error.CastFailed,
        error.PatternTooComplex => return error.PatternTooComplex,
    };
    var m = RegexMatch{ .s = try valueToString(arena, v), .span = null, .caps = undefined };
    m.span = re.find(m.s, 0, &m.caps) catch |e| switch (e) {
        error.OutOfMemory => return error.OutOfMemory,
        error.BadPattern => return error.CastFailed,
        error.PatternTooComplex => return error.PatternTooComplex,
    };
    return m;
}

/// The entry is invalidated before compiling, so a failed compile never leaves the
/// old pattern under the new key, and `src` is copied into the entry's own buffer.
fn cachedRegex(pattern: []const u8) regex.Error!regex.Regex {
    const c = &regex_cache;
    if (c.valid and std.mem.eql(u8, c.src, pattern)) return c.re;
    var fba = std.heap.FixedBufferAllocator.init(&c.buf);
    c.valid = false;
    c.re = try regex.Regex.compile(fba.allocator(), pattern);
    c.src = fba.allocator().dupe(u8, pattern) catch return error.OutOfMemory;
    c.valid = true;
    return c.re;
}
pub fn orderF64(x: f64, y: f64) std.math.Order {
    const xn = std.math.isNan(x);
    const yn = std.math.isNan(y);
    if (xn or yn) {
        if (xn and yn) return .eq;
        return if (xn) .gt else .lt;
    }
    return std.math.order(x, y);
}

/// Bytes compare by content: a null answer here once made MIN/MAX over a bytes
/// column keep the first value forever.
pub fn compareValues(a: Value, b: Value) ?std.math.Order {
    if (isNum(a) and isNum(b)) {
        if (a == .int and b == .int) return std.math.order(a.int, b.int);
        return orderF64(toF64(a), toF64(b));
    }
    if (a == .string and b == .string) return std.mem.order(u8, a.string, b.string);
    if (a == .bytes and b == .bytes) return std.mem.order(u8, a.bytes, b.bytes);
    if (a == .bool and b == .bool) return std.math.order(@intFromBool(a.bool), @intFromBool(b.bool));
    if (a == .timestamp and b == .timestamp) return std.math.order(a.timestamp, b.timestamp);
    if (a == .date and b == .date) return std.math.order(a.date, b.date);
    if (a == .date and b == .string) return std.math.order(@as(i64, a.date), parseIsoDate(b.string) orelse return null);
    if (a == .string and b == .date) return std.math.order(parseIsoDate(a.string) orelse return null, @as(i64, b.date));
    if (a == .timestamp and b == .string) return std.math.order(a.timestamp, parseIsoTimestamp(b.string) orelse return null);
    if (a == .string and b == .timestamp) return std.math.order(parseIsoTimestamp(a.string) orelse return null, b.timestamp);
    if (a == .time and b == .time) return std.math.order(a.time, b.time);
    return null;
}
fn eq(a: []const u8, b: []const u8) bool {
    return std.mem.eql(u8, a, b);
}
fn trim(s: []const u8) []const u8 {
    return std.mem.trim(u8, s, " \t\r\n");
}

/// `from_hex`: optional `0x`, either case, 16 digits of range so `to_hex` of a
/// negative round-trips. Junk or overflow is an error, never a null.
fn parseHexI64(s: []const u8) EvalError!i64 {
    var t = trim(s);
    if (t.len >= 2 and t[0] == '0' and (t[1] == 'x' or t[1] == 'X')) t = t[2..];
    if (t.len == 0) return error.CastFailed;
    for (t) |ch| if (!std.ascii.isHex(ch)) return error.CastFailed;
    const u = std.fmt.parseUnsigned(u64, t, 16) catch return error.CastFailed;
    return @bitCast(u);
}

fn toI64(v: Value) i64 {
    return switch (v) {
        .int => |x| x,
        .float => |x| @intFromFloat(x),
        .string => |s| std.fmt.parseInt(i64, std.mem.trim(u8, s, " "), 10) catch 0,
        else => 0,
    };
}

inline fn charWidth(s: []const u8, i: usize) usize {
    const b = s[i];
    if (b < 0x80) return 1;
    const n = std.unicode.utf8ByteSequenceLength(b) catch return 1;
    if (i + n > s.len) return 1;
    _ = std.unicode.utf8Decode(s[i..][0..n]) catch return 1;
    return n;
}

fn isAscii(s: []const u8) bool {
    var i: usize = 0;
    while (i + 8 <= s.len) : (i += 8) {
        if (std.mem.readInt(u64, s[i..][0..8], .little) & 0x8080808080808080 != 0) return false;
    }
    while (i < s.len) : (i += 1) {
        if (s[i] >= 0x80) return false;
    }
    return true;
}

fn charCount(s: []const u8) usize {
    if (isAscii(s)) return s.len;
    var i: usize = 0;
    var n: usize = 0;
    while (i < s.len) : (n += 1) i += charWidth(s, i);
    return n;
}

fn charOffset(s: []const u8, k: usize) usize {
    var i: usize = 0;
    var c: usize = 0;
    while (c < k and i < s.len) : (c += 1) i += charWidth(s, i);
    return i;
}

fn substrChars(arena: std.mem.Allocator, s: []const u8, start1: i64, len_opt: ?i64) ![]const u8 {
    var start: usize = 0;
    if (start1 > 1) start = charOffset(s, @intCast(start1 - 1));
    var end: usize = s.len;
    if (len_opt) |l| {
        if (l <= 0) return "";
        end = start + charOffset(s[start..], @intCast(l));
    }
    return arena.dupe(u8, s[start..end]);
}

/// Half away from zero (2.5 to 3, -2.5 to -3), not banker's rounding. Engines
/// disagree, so `round` stays out of the pushdown whitelist in runtime/pushdown.zig.
fn roundHalfAway(x: f64, digits: i64) f64 {
    if (digits == 0) return @round(x);
    const s = pow10f(@intCast(@min(@abs(digits), 22)));
    return if (digits > 0) @round(x * s) / s else @round(x / s) * s;
}

/// Postgres `lpad`/`rpad`: pads to exactly `n` characters and truncates a longer
/// `s` to its first `n`; an empty `fill` leaves a short `s` unchanged.
fn padChars(arena: std.mem.Allocator, s: []const u8, n: i64, fill: []const u8, left: bool) ![]const u8 {
    if (n <= 0) return "";
    const want: usize = @intCast(n);
    if (want > max_str_bytes) return error.CastFailed;
    const have = charCount(s);
    if (have >= want) return arena.dupe(u8, s[0..charOffset(s, want)]);
    if (fill.len == 0) return arena.dupe(u8, s);
    var pad = std.array_list.Managed(u8).init(arena);
    var fi: usize = 0;
    var k: usize = 0;
    while (k < want - have) : (k += 1) {
        if (fi == fill.len) fi = 0;
        const w = charWidth(fill, fi);
        try pad.appendSlice(fill[fi..][0..w]);
        fi += w;
    }
    if (pad.items.len + s.len > max_str_bytes) return error.CastFailed;
    return std.mem.concat(arena, u8, if (left) &.{ pad.items, s } else &.{ s, pad.items });
}

/// Postgres `left`/`right`: a negative `n` means all but the last/first |n|
/// characters, rather than clamping to empty.
fn endSlice(s: []const u8, n: i64, left: bool) []const u8 {
    const slen: i64 = @intCast(charCount(s));
    var take: i64 = if (n >= 0) n else slen + n;
    if (take < 0) take = 0;
    if (take > slen) take = slen;
    const k: usize = @intCast(take);
    return if (left) s[0..charOffset(s, k)] else s[charOffset(s, @intCast(slen - take))..];
}

fn reverseChars(arena: std.mem.Allocator, s: []const u8) ![]const u8 {
    const out = try arena.alloc(u8, s.len);
    var i: usize = 0;
    while (i < s.len) {
        const w = charWidth(s, i);
        @memcpy(out[s.len - i - w ..][0..w], s[i..][0..w]);
        i += w;
    }
    return out;
}

/// One-to-one case mapping for ASCII, Latin-1, Latin Extended-A, Greek and
/// Cyrillic; anything else, and length-changing mappings like `ß`, stay as is.
fn caseMap(cp: u21, up: bool) u21 {
    if (cp < 0x80) return if (up) std.ascii.toUpper(@intCast(cp)) else std.ascii.toLower(@intCast(cp));
    if (up) {
        if ((cp >= 0xE0 and cp <= 0xFE and cp != 0xF7)) return cp - 0x20;
        if (cp == 0xFF) return 0x178;
        if (cp >= 0x100 and cp <= 0x17F) return latinExtA(cp, true);
        if (cp >= 0x3B1 and cp <= 0x3C9 and cp != 0x3C2) return cp - 0x20;
        if (cp == 0x3C2) return 0x3A3;
        if (cp == 0x3AC) return 0x386;
        if (cp >= 0x3AD and cp <= 0x3AF) return cp - 0x25;
        if (cp == 0x3CC) return 0x38C;
        if (cp == 0x3CD or cp == 0x3CE) return cp - 0x3F;
        if (cp >= 0x430 and cp <= 0x44F) return cp - 0x20;
        if (cp >= 0x450 and cp <= 0x45F) return cp - 0x50;
    } else {
        if ((cp >= 0xC0 and cp <= 0xDE and cp != 0xD7)) return cp + 0x20;
        if (cp == 0x178) return 0xFF;
        if (cp >= 0x100 and cp <= 0x17F) return latinExtA(cp, false);
        if (cp >= 0x391 and cp <= 0x3A9 and cp != 0x3A2) return cp + 0x20;
        if (cp == 0x386) return 0x3AC;
        if (cp >= 0x388 and cp <= 0x38A) return cp + 0x25;
        if (cp == 0x38C) return 0x3CC;
        if (cp == 0x38E or cp == 0x38F) return cp + 0x3F;
        if (cp >= 0x410 and cp <= 0x42F) return cp + 0x20;
        if (cp >= 0x400 and cp <= 0x40F) return cp + 0x50;
    }
    return cp;
}

/// Latin Extended-A pairs on adjacent code points: even/odd through U+0137 and from
/// U+014A, odd/even across U+0139-U+0148 and U+0179-U+017E; the rest have no partner.
fn latinExtA(cp: u21, up: bool) u21 {
    if (cp == 0x130 or cp == 0x131 or cp == 0x138 or cp == 0x149 or cp == 0x17F or cp == 0x178) return cp;
    const odd_upper = (cp >= 0x139 and cp <= 0x148) or (cp >= 0x179 and cp <= 0x17E);
    const is_upper = if (odd_upper) cp % 2 == 1 else cp % 2 == 0;
    if (up and !is_upper) return if (odd_upper) cp - 1 else cp - 1;
    if (!up and is_upper) return cp + 1;
    return cp;
}

const unaccent_base = "AAAAAA*CEEEEIIIIDNOOOOO-OUUUUY**" ++ "aaaaaa*ceeeeiiiidnooooo-ouuuuy*y" ++
    "AaAaAaCcCcCcCcDdDdEeEeEeEeEeGgGgGgGgHhHhIiIiIiIiIi**JjKkkLlLlLlLlLlNnNnNnnNnOoOoOo**RrRrRrSsSsSsSsTtTtTtUuUuUuUuUuUuWwYyYZzZzZzs";

comptime {
    std.debug.assert(unaccent_base.len == 0x180 - 0xC0);
}

/// What `unaccent` writes for `cp`, or null to keep it. In `unaccent_base`, `*`
/// marks a ligature spelled here and `-` a non-letter; combining accents are dropped.
fn unaccentCp(cp: u21) ?[]const u8 {
    if (cp >= 0x300 and cp <= 0x36F) return "";
    if (cp < 0xC0 or cp >= 0x180) return null;
    const b = unaccent_base[cp - 0xC0];
    if (b == '-') return null;
    if (b != '*') return unaccent_base[cp - 0xC0 ..][0..1];
    return switch (cp) {
        0xC6 => "AE",
        0xE6 => "ae",
        0xDE => "TH",
        0xFE => "th",
        0xDF => "ss",
        0x132 => "IJ",
        0x133 => "ij",
        0x152 => "OE",
        0x153 => "oe",
        else => unreachable,
    };
}

fn isWordChar(cp: u21, w: usize) bool {
    if (cp < 0x80) return std.ascii.isAlphanumeric(@intCast(cp));
    if (w == 1) return false;
    return cp == 0xDF or caseMap(cp, true) != cp or caseMap(cp, false) != cp;
}

fn caseMapInto(out: *std.array_list.Managed(u8), s: []const u8, up: bool) !void {
    var i: usize = 0;
    while (i < s.len) {
        const w = charWidth(s, i);
        if (w == 1) {
            try out.append(if (s[i] < 0x80) (if (up) std.ascii.toUpper(s[i]) else std.ascii.toLower(s[i])) else s[i]);
        } else {
            const cp = std.unicode.utf8Decode(s[i..][0..w]) catch unreachable;
            var buf: [4]u8 = undefined;
            const n = std.unicode.utf8Encode(caseMap(cp, up), &buf) catch unreachable;
            try out.appendSlice(buf[0..n]);
        }
        i += w;
    }
}

/// SQL `LIKE`. `%` is tested before the literal compare: otherwise a `%` in the
/// text matched a `%` in the pattern literally, and `'50% off' LIKE '50%'` was false.
fn likeMatch(s: []const u8, pat: []const u8) bool {
    var si: usize = 0;
    var pi: usize = 0;
    var star: ?usize = null;
    var smark: usize = 0;
    while (si < s.len) {
        if (pi < pat.len and pat[pi] == '%') {
            star = pi;
            smark = si;
            pi += 1;
        } else if (pi < pat.len and pat[pi] == '_') {
            si += charWidth(s, si);
            pi += 1;
        } else if (pi < pat.len and pat[pi] == s[si]) {
            si += 1;
            pi += 1;
        } else if (star) |st| {
            pi = st + 1;
            smark += charWidth(s, smark);
            si = smark;
        } else return false;
    }
    while (pi < pat.len and pat[pi] == '%') pi += 1;
    return pi == pat.len;
}

test "substr (1-based, in characters) and like wildcard matcher" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try std.testing.expectEqualStrings("01", try substrChars(a, "SD1010", 4, 2));
    try std.testing.expectEqualStrings("SD1", try substrChars(a, "SD1010", 1, 3));
    try std.testing.expectEqualStrings("010", try substrChars(a, "SD1010", 4, null));
    try std.testing.expectEqualStrings("", try substrChars(a, "SD1010", 99, 2));

    try std.testing.expect(likeMatch("hello, world", "hello%"));
    try std.testing.expect(likeMatch("hello", "h_llo"));
    try std.testing.expect(likeMatch("anything", "%"));
    try std.testing.expect(!likeMatch("hello", "h_l"));
    try std.testing.expect(!likeMatch("paid", "pending%"));

    try std.testing.expect(likeMatch("%%", "%"));
    try std.testing.expect(likeMatch("50% off", "50%"));
    try std.testing.expect(likeMatch("ab%c", "ab%"));
    try std.testing.expect(likeMatch("a%b", "a%b"));
    try std.testing.expect(!likeMatch("a%b", "a%c"));

    try std.testing.expectEqualStrings("ïv", try substrChars(a, "naïve", 3, 2));
    try std.testing.expectEqual(@as(usize, 5), charCount("naïve"));
    try std.testing.expectEqual(@as(usize, 3), charCount("a\xe9b"));
    try std.testing.expectEqualStrings("本日", try reverseChars(a, "日本"));
    try std.testing.expectEqualStrings("aç", endSlice("ação", -2, true));
    try std.testing.expectEqualStrings("ão", endSlice("ação", 2, false));
    try std.testing.expectEqualStrings("çã", endSlice("çãoo", 2, true));
    try std.testing.expectEqualStrings("ñ-ñ-a", try padChars(a, "a", 5, "ñ-", true));
    try std.testing.expectEqualStrings("日", try padChars(a, "日本", 1, " ", false));
    try std.testing.expect(likeMatch("ünï", "_n_"));
    try std.testing.expect(!likeMatch("ü", "__"));
    var out = std.array_list.Managed(u8).init(a);
    try caseMapInto(&out, "café ação ÿ łódź πσς ελληνικά ώ жё", true);
    try std.testing.expectEqualStrings("CAFÉ AÇÃO Ÿ ŁÓDŹ ΠΣΣ ΕΛΛΗΝΙΚΆ Ώ ЖЁ", out.items);
    out.clearRetainingCapacity();
    try caseMapInto(&out, "CAFÉ AÇÃO Ÿ ŁÓDŹ ΠΣ ЖЁ ß", false);
    try std.testing.expectEqualStrings("café ação ÿ łódź πσ жё ß", out.items);
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

const parser = @import("../lang/sql_parser.zig");

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

    const sqlp = @import("../lang/sql_parser.zig");
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

    const exprs = [_][]const u8{
        "x + y",
        "x * y - 1",
        "x / y",
        "x > y",
        "x >= 10 and y < 8",
        "x == 40 or y == 5",
        "if(x > y, x, y)",
        "-x",
        "x is null",
        "if(x != 0, y / x, 0)",
        "x != 0 and y / x > 1",
        "x == 0 or y / x > 1",
    };
    for (exprs) |body| {
        var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        const e = try parser.parseExprStr(a, body, &diag);
        var ctx = TypeCtx{ .schema = schema, .arena = a };
        const ty = try ctx.typeOf(e);

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

        _ = evalVec(a, e, batch) catch |err| {
            std.debug.print("expr de-vectorized: {s}\n", .{body});
            try std.testing.expect(err != error.Unsupported);
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

test "type errors: unknown field and non-bool not" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const schema = types.Schema{ .fields = &.{.{ .name = "x", .ty = Type.init(.int) }} };

    var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const pred = try parser.parseExprStr(a, "missing > 1", &diag);
    var ctx = TypeCtx{ .schema = schema, .arena = a };
    try std.testing.expectError(error.TypeError, ctx.typeOf(pred));
    try std.testing.expect(std.mem.indexOf(u8, ctx.msg, "unknown field") != null);

    var fx = ast.Expr{ .field = .{ .parts = &[_][]const u8{"x"} } };
    var notx = ast.Expr{ .unary = .{ .op = .not, .e = &fx } };
    try std.testing.expectError(error.TypeError, ctx.typeOf(&notx));
    try std.testing.expect(std.mem.indexOf(u8, ctx.msg, "bool operand") != null);
}

test "castValue: conversions succeed and failures are CastFailed specifically" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    try std.testing.expectEqual(@as(i64, 42), (try castValue(a, .{ .string = " 42 " }, .int)).int);
    try std.testing.expectEqual(@as(i64, 1), (try castValue(a, .{ .bool = true }, .int)).int);
    try std.testing.expectEqual(@as(i64, -3), (try castValue(a, .{ .float = -3.9 }, .int)).int);
    try std.testing.expectEqual(@as(f64, 2.5), (try castValue(a, .{ .string = "2.5" }, .float)).float);
    try std.testing.expect((try castValue(a, .{ .string = " TRUE " }, .bool)).bool);
    try std.testing.expect(!(try castValue(a, .{ .int = 0 }, .bool)).bool);
    try std.testing.expectEqualStrings("123.45", (try castValue(a, .{ .decimal = .{ .unscaled = 12345, .scale = 2 } }, .string)).string);

    try std.testing.expectError(error.CastFailed, castValue(a, .{ .string = "abc" }, .int));
    try std.testing.expectError(error.CastFailed, castValue(a, .{ .float = std.math.nan(f64) }, .int));
    try std.testing.expectError(error.CastFailed, castValue(a, .{ .float = 1e19 }, .int));
    try std.testing.expectError(error.CastFailed, castValue(a, .{ .string = "yes" }, .bool));
    try std.testing.expectError(error.CastFailed, castValue(a, .{ .bool = true }, .float));
}

test "formatDecimal pads sub-unit magnitudes, zero, and negatives" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings("-0.005", try formatDecimal(a, -5, 3));
    try std.testing.expectEqualStrings("0", try formatDecimal(a, 0, 0));
    try std.testing.expectEqualStrings("0.00", try formatDecimal(a, 0, 2));
    try std.testing.expectEqualStrings("7", try formatDecimal(a, 7, 0));
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

test "compareValues orders across numeric kinds and rejects mixed kinds" {
    try std.testing.expectEqual(std.math.Order.lt, compareValues(.{ .int = 1 }, .{ .float = 1.5 }).?);
    try std.testing.expectEqual(std.math.Order.eq, compareValues(.{ .float = 2.0 }, .{ .int = 2 }).?);
    try std.testing.expectEqual(std.math.Order.gt, compareValues(.{ .decimal = .{ .unscaled = 250, .scale = 2 } }, .{ .int = 2 }).?);
    try std.testing.expectEqual(std.math.Order.lt, compareValues(.{ .string = "a" }, .{ .string = "b" }).?);
    try std.testing.expectEqual(std.math.Order.lt, compareValues(.{ .bool = false }, .{ .bool = true }).?);
    try std.testing.expect(compareValues(.{ .string = "1" }, .{ .int = 1 }) == null);
    try std.testing.expect(compareValues(.{ .bool = true }, .{ .int = 1 }) == null);
    try std.testing.expect(compareValues(.{ .date = 1 }, .{ .timestamp = 1 }) == null);
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

fn evalLit(a: std.mem.Allocator, src: []const u8) !Value {
    var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const e = try parser.parseExprStr(a, src, &diag);
    return constEval(a, e, &[_][]const u8{}, &[_]Value{});
}

test "math builtins: rounding direction, guarded mod, domain edges" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    try std.testing.expectEqual(@as(i64, 7), (try evalLit(a, "abs(-7)")).int);
    try std.testing.expectEqual(@as(f64, 2.5), (try evalLit(a, "abs(-2.5)")).float);
    try std.testing.expect((try evalLit(a, "abs(null)")).isNull());

    try std.testing.expectEqual(@as(i64, 5), (try evalLit(a, "floor(5)")).int);
    try std.testing.expectEqual(@as(i64, 5), (try evalLit(a, "round(5)")).int);
    try std.testing.expectEqual(@as(f64, 2.0), (try evalLit(a, "floor(2.7)")).float);
    try std.testing.expectEqual(@as(f64, 3.0), (try evalLit(a, "ceil(2.1)")).float);

    try std.testing.expectEqual(@as(f64, 3.0), (try evalLit(a, "round(2.5)")).float);
    try std.testing.expectEqual(@as(f64, -3.0), (try evalLit(a, "round(-2.5)")).float);
    try std.testing.expectEqual(@as(f64, 2.13), (try evalLit(a, "round(2.125, 2)")).float);

    try std.testing.expectEqual(@as(i64, 1), (try evalLit(a, "mod(7, 3)")).int);
    try std.testing.expectEqual(@as(i64, -1), (try evalLit(a, "mod(-7, 3)")).int);
    try std.testing.expect((try evalLit(a, "mod(7, 0)")).isNull());

    try std.testing.expectEqual(@as(f64, 8.0), (try evalLit(a, "power(2, 3)")).float);
    try std.testing.expectEqual(@as(f64, 3.0), (try evalLit(a, "sqrt(9)")).float);
    try std.testing.expect((try evalLit(a, "sqrt(-1)")).isNull());

    try std.testing.expectEqual(@as(i64, -1), (try evalLit(a, "sign(-0.5)")).int);
    try std.testing.expectEqual(@as(i64, 0), (try evalLit(a, "sign(0)")).int);
    try std.testing.expectEqual(@as(i64, 1), (try evalLit(a, "sign(42)")).int);
}

test "nullif propagates nulls; greatest/least ignore them" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    try std.testing.expect((try evalLit(a, "nullif(3, 3)")).isNull());
    try std.testing.expectEqual(@as(i64, 3), (try evalLit(a, "nullif(3, 4)")).int);
    try std.testing.expectEqual(@as(i64, 3), (try evalLit(a, "nullif(3, null)")).int);
    try std.testing.expect((try evalLit(a, "nullif(null, 3)")).isNull());

    try std.testing.expectEqual(@as(i64, 9), (try evalLit(a, "greatest(1, 9, 4)")).int);
    try std.testing.expectEqual(@as(i64, 1), (try evalLit(a, "least(1, 9, 4)")).int);
    try std.testing.expectEqual(@as(i64, 9), (try evalLit(a, "greatest(null, 9)")).int);
    try std.testing.expectEqual(@as(i64, 9), (try evalLit(a, "least(null, 9)")).int);
    try std.testing.expect((try evalLit(a, "least(null, null)")).isNull());
    try std.testing.expectEqualStrings("pear", (try evalLit(a, "greatest('apple', 'pear')")).string);
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

test "string builtins: padding truncates, ends take negatives, split_part clamps" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    try std.testing.expectEqualStrings("00042", (try evalLit(a, "lpad('42', 5, '0')")).string);
    try std.testing.expectEqualStrings("42   ", (try evalLit(a, "rpad('42', 5)")).string);
    try std.testing.expectEqualStrings("abc", (try evalLit(a, "lpad('abcdef', 3)")).string);
    try std.testing.expectEqualStrings("abc", (try evalLit(a, "rpad('abcdef', 3)")).string);

    try std.testing.expectEqualStrings("ab", (try evalLit(a, "left('abcde', 2)")).string);
    try std.testing.expectEqualStrings("de", (try evalLit(a, "right('abcde', 2)")).string);
    try std.testing.expectEqualStrings("abc", (try evalLit(a, "left('abcde', -2)")).string);
    try std.testing.expectEqualStrings("cde", (try evalLit(a, "right('abcde', -2)")).string);

    try std.testing.expectEqualStrings("b", (try evalLit(a, "split_part('a,b,c', ',', 2)")).string);
    try std.testing.expectEqualStrings("", (try evalLit(a, "split_part('a,b,c', ',', 9)")).string);
    try std.testing.expectEqualStrings("", (try evalLit(a, "split_part('a,b,c', ',', 0)")).string);
    try std.testing.expect((try evalLit(a, "split_part('a,b,c', '', 1)")).isNull());

    try std.testing.expectEqual(@as(i64, 3), (try evalLit(a, "strpos('abcd', 'cd')")).int);
    try std.testing.expectEqual(@as(i64, 0), (try evalLit(a, "strpos('abcd', 'z')")).int);

    try std.testing.expectEqualStrings("abab", (try evalLit(a, "repeat('ab', 2)")).string);
    try std.testing.expectEqualStrings("", (try evalLit(a, "repeat('ab', 0)")).string);
    try std.testing.expectError(error.CastFailed, evalLit(a, "repeat('ab', 1000000)"));

    try std.testing.expectEqualStrings("cba", (try evalLit(a, "reverse('abc')")).string);
    try std.testing.expect((try evalLit(a, "reverse(null)")).isNull());
}

test "date builtins: month clamp, boundary diffs, epoch round trip, strftime padding" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    try std.testing.expectEqualStrings("2024-02-29", try formatDate(a, (try evalLit(a, "date_add('month', 1, cast('2024-01-31' as date))")).date));
    try std.testing.expectEqualStrings("2023-02-28", try formatDate(a, (try evalLit(a, "date_add('month', 1, cast('2023-01-31' as date))")).date));
    try std.testing.expectEqualStrings("2023-12-31", try formatDate(a, (try evalLit(a, "date_add('day', -1, cast('2024-01-01' as date))")).date));
    try std.testing.expectEqualStrings("2025-03-15", try formatDate(a, (try evalLit(a, "date_add('year', 1, cast('2024-03-15' as date))")).date));
    try std.testing.expectEqualStrings("2024-02-29 06:30:00", try formatTimestamp(a, (try evalLit(a, "date_add('month', 1, cast('2024-01-31 06:30:00' as timestamp))")).timestamp));

    try std.testing.expectEqual(@as(i64, 1), (try evalLit(a, "date_diff('year', cast('2023-12-31' as date), cast('2024-01-01' as date))")).int);
    try std.testing.expectEqual(@as(i64, 1), (try evalLit(a, "date_diff('month', cast('2023-12-31' as date), cast('2024-01-01' as date))")).int);
    try std.testing.expectEqual(@as(i64, 60), (try evalLit(a, "date_diff('day', cast('2024-01-01' as date), cast('2024-03-01' as date))")).int);
    try std.testing.expectEqual(@as(i64, -1), (try evalLit(a, "date_diff('day', cast('2024-01-02' as date), cast('2024-01-01' as date))")).int);

    try std.testing.expectEqualStrings("2024-02-29", try formatDate(a, (try evalLit(a, "make_date(2024, 2, 29)")).date));
    try std.testing.expectError(error.CastFailed, evalLit(a, "make_date(2023, 2, 29)"));
    try std.testing.expectError(error.CastFailed, evalLit(a, "make_date(2023, 13, 1)"));

    try std.testing.expectEqual(@as(i64, 1700000000), (try evalLit(a, "epoch(to_timestamp(1700000000))")).int);
    try std.testing.expectEqual(@as(i64, 0), (try evalLit(a, "epoch(cast('1970-01-01' as date))")).int);

    try std.testing.expectEqualStrings("1970-01-01 00:00:00", (try evalLit(a, "strftime(to_timestamp(0), '%Y-%m-%d %H:%M:%S')")).string);
    try std.testing.expectEqualStrings("70 01:01:01 %", (try evalLit(a, "strftime(to_timestamp(3661), '%y %H:%M:%S %%')")).string);
}

test "strptime: widths, %y pivot, impossible days, try_ form" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const ts = struct {
        fn f(al: std.mem.Allocator, src: []const u8) ![]const u8 {
            return formatTimestamp(al, (try evalLit(al, src)).timestamp);
        }
    }.f;

    try std.testing.expectEqualStrings("2026-10-03 00:00:00", try ts(a, "strptime('03/10/2026', '%d/%m/%Y')"));
    try std.testing.expectEqualStrings("2026-01-03 00:00:00", try ts(a, "strptime('3/1/2026', '%d/%m/%Y')"));
    try std.testing.expectEqualStrings("2024-02-29 13:05:09", try ts(a, "strptime('2024-02-29 13:05:09', '%Y-%m-%d %H:%M:%S')"));
    try std.testing.expectEqualStrings("1969-10-03 00:00:00", try ts(a, "strptime('03/10/69', '%d/%m/%y')"));
    try std.testing.expectEqualStrings("2068-10-03 00:00:00", try ts(a, "strptime('03/10/68', '%d/%m/%y')"));
    try std.testing.expectEqualStrings("2026-10-03 00:00:00", try ts(a, "strptime('100% 03/10/2026', '100%% %d/%m/%Y')"));

    try std.testing.expectError(error.CastFailed, evalLit(a, "strptime('31/02/2026', '%d/%m/%Y')"));
    try std.testing.expectError(error.CastFailed, evalLit(a, "strptime('03/10/2026 x', '%d/%m/%Y')"));
    try std.testing.expectError(error.CastFailed, evalLit(a, "strptime('2026-10-03', '%d/%m/%Y')"));
    try std.testing.expectError(error.CastFailed, evalLit(a, "strptime('03/10/2026 24:00:00', '%d/%m/%Y %H:%M:%S')"));
    try std.testing.expect((try evalLit(a, "try_strptime('31/02/2026', '%d/%m/%Y')")) == .null);
    _ = evalLit(a, "strptime('31/02/2026', '%d/%m/%Y')") catch {};
    try std.testing.expect(takeFailure(error.DivByZero) == null);
    try std.testing.expectEqualStrings("strptime: '31/02/2026' is not a date in '%d/%m/%Y' (try_strptime gives null)", takeFailure(error.CastFailed).?);
    try std.testing.expect(takeFailure(error.CastFailed) == null);
    try std.testing.expect((try evalLit(a, "try_strptime('', '%d/%m/%Y')")) == .null);
}

test "text cleanup: translate, initcap, unaccent, ascii, chr" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const str = struct {
        fn f(al: std.mem.Allocator, src: []const u8) ![]const u8 {
            return (try evalLit(al, src)).string;
        }
    }.f;

    try std.testing.expectEqualStrings("12345678000190", try str(a, "translate('12.345.678/0001-90', './-', '')"));
    try std.testing.expectEqualStrings("xc", try str(a, "translate('abc', 'ab', 'x')"));
    try std.testing.expectEqualStrings("acao", try str(a, "translate('ação', 'ãç', 'ac')"));

    try std.testing.expectEqualStrings("Hello World-Foo Bar_Baz 2nd", try str(a, "initcap('hello wORLD-foo bar_baz 2ND')"));
    try std.testing.expectEqualStrings("São Paulo Élan", try str(a, "initcap('SÃO PAULO élan')"));

    try std.testing.expectEqualStrings("Sao Paulo Acao U n", try str(a, "unaccent('São Paulo Ação Ü ñ')"));
    try std.testing.expectEqualStrings("AEther strasse Lodz OEuvre ×", try str(a, "strip_accents('Æther straße Łódź Œuvre ×')"));
    try std.testing.expectEqualStrings("Sao", try str(a, "unaccent('Sa\u{0303}o')"));

    try std.testing.expectEqual(@as(i64, 65), (try evalLit(a, "ascii('ABC')")).int);
    try std.testing.expectEqual(@as(i64, 0xE3), (try evalLit(a, "ascii('ã')")).int);
    try std.testing.expectEqual(@as(i64, 0), (try evalLit(a, "ascii('')")).int);
    try std.testing.expectEqualStrings("ã", try str(a, "chr(227)"));
    try std.testing.expectError(error.CastFailed, evalLit(a, "chr(0)"));
    try std.testing.expectError(error.CastFailed, evalLit(a, "chr(1114112)"));
}

test "regex and hashes: regexp_matches, regexp_extract, md5, sha256, xxhash64, concat_ws" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    try std.testing.expect((try evalLit(a, "regexp_matches('abc123', '[0-9]+')")).bool);
    try std.testing.expect(!(try evalLit(a, "regexp_matches('abc', '^[0-9]+$')")).bool);
    try std.testing.expectEqualStrings("123", (try evalLit(a, "regexp_extract('abc123', '[0-9]+')")).string);
    try std.testing.expectEqualStrings("12", (try evalLit(a, "regexp_extract('ab-12', '([a-z]+)-([0-9]+)', 2)")).string);
    try std.testing.expect((try evalLit(a, "regexp_extract('abc', '[0-9]+')")) == .null);
    try std.testing.expect((try evalLit(a, "regexp_extract('b', '(a)?b', 1)")) == .null);

    try std.testing.expectEqualStrings("900150983cd24fb0d6963f7d28e17f72", (try evalLit(a, "md5('abc')")).string);
    try std.testing.expectEqualStrings("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad", (try evalLit(a, "sha256('abc')")).string);
    try std.testing.expectEqual(@as(i64, 4952883123889572249), (try evalLit(a, "xxhash64('abc')")).int);
    try std.testing.expect((try evalLit(a, "md5(NULL)")) == .null);

    try std.testing.expectEqualStrings("a|c", (try evalLit(a, "concat_ws('|', 'a', NULL, 'c')")).string);
    try std.testing.expectEqualStrings("", (try evalLit(a, "concat_ws('|', NULL, NULL)")).string);
    try std.testing.expect((try evalLit(a, "concat_ws(NULL, 'a')")) == .null);
}

test "check-time errors: a regexp_extract group past the pattern's" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    var ctx = TypeCtx{ .schema = .{ .fields = &.{} }, .arena = a };
    try std.testing.expectError(error.TypeError, ctx.typeOf(try parser.parseExprStr(a, "regexp_extract('x', '(a)', 2)", &diag)));
    try std.testing.expectError(error.TypeError, ctx.typeOf(try parser.parseExprStr(a, "regexp_matches('x', '(')", &diag)));
    _ = try ctx.typeOf(try parser.parseExprStr(a, "regexp_extract('x', '(a)', 1)", &diag));
}

test "json builders and encodings: json_object, json_array, base64, url" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const str = struct {
        fn f(al: std.mem.Allocator, src: []const u8) ![]const u8 {
            return (try evalLit(al, src)).string;
        }
    }.f;

    try std.testing.expectEqualStrings(
        \\{"a":1,"b":"x \"q\"","c":null,"d":[1,"two"],"e":true}
    , try str(a, "json_object('a', 1, 'b', 'x \"q\"', 'c', NULL, 'd', json_array(1, 'two'), 'e', 1 = 1)"));
    try std.testing.expectEqualStrings("{}", try str(a, "json_object()"));
    try std.testing.expectEqualStrings("[]", try str(a, "json_array()"));
    try std.testing.expectEqualStrings(
        \\{"k":{"y":1}}
    , try str(a, "json_object('k', json_get('{\"x\": {\"y\": 1}}', 'x'))"));
    try std.testing.expectEqualStrings(
        \\["[not json"]
    , try str(a, "json_array('[not json')"));
    try std.testing.expectError(error.CastFailed, evalLit(a, "json_object(NULL, 1)"));
    try std.testing.expectEqualStrings(
        \\[1.5,null,null,12.30,"2026-10-03"]
    , try str(a, "json_array(1.5, CAST('nan' AS DOUBLE), -CAST('inf' AS DOUBLE), CAST('12.30' AS DECIMAL(10,2)), CAST('2026-10-03' AS DATE))"));

    try std.testing.expectEqualStrings("aGVsbG8=", try str(a, "to_base64('hello')"));
    try std.testing.expectEqualStrings("hello", (try evalLit(a, "from_base64('aGVsbG8=')")).bytes);
    try std.testing.expectError(error.CastFailed, evalLit(a, "from_base64('!!')"));

    try std.testing.expectEqualStrings("a%20b%26c%3Dd%2F%C3%A9", try str(a, "url_encode('a b&c=d/é')"));
    try std.testing.expectEqualStrings("a+b cé", try str(a, "url_decode('a+b%20c%C3%A9')"));
    try std.testing.expectEqualStrings("%zz%4", try str(a, "url_decode('%zz%4')"));
}

test "json_reduce: folds, typed accumulators, positions, shadowing" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    try std.testing.expectEqual(@as(i64, 6), (try evalLit(a, "json_reduce('[1,2,3]', 0, (acc, x) -> acc + x)")).int);
    try std.testing.expectEqual(@as(i64, 0), (try evalLit(a, "json_reduce('[]', 0, (acc, x) -> acc + x)")).int);
    try std.testing.expect((try evalLit(a, "json_reduce(NULL, 0, (acc, x) -> acc + x)")) == .null);
    try std.testing.expectEqual(@as(f64, 3.5), (try evalLit(a, "json_reduce('[1.5,2]', 0.0, (acc, x) -> acc + x)")).float);
    try std.testing.expectError(error.CastFailed, evalLit(a, "json_reduce('[1.5,2]', 0, (acc, x) -> acc + x)"));

    const d = try evalLit(a, "json_reduce('[\"0.10\",\"0.25\"]', CAST(0 AS DECIMAL(10,2)), (acc, x) -> acc + CAST(x AS DECIMAL(10,2)))");
    try std.testing.expectEqualStrings("0.35", try valueToString(a, d));
    try std.testing.expectEqualStrings("2026-01-04", try formatDate(a, (try evalLit(a, "json_reduce('[1,2]', CAST('2026-01-01' AS DATE), (dt, x) -> date_add('day', x, dt))")).date));

    try std.testing.expectEqual(@as(i64, 22), (try evalLit(a, "json_reduce('[1,2,3]', 0, (acc, x, i) -> acc + x * CAST(json_get('[5,4,3]', CAST(i AS STRING)) AS INT))")).int);
    try std.testing.expectEqualStrings("[10,21]", (try evalLit(a, "json_transform('[10,20]', (x, i) -> x + i)")).string);

    try std.testing.expect((try evalLit(a, "json_any('[[1,2],[3]]', x -> json_reduce(x, 0, (acc, x) -> acc + x) = 3)")).bool);
}

test "array helpers: chars, json_range, json_length, json_slice, json_concat" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const str = struct {
        fn f(al: std.mem.Allocator, src: []const u8) ![]const u8 {
            return (try evalLit(al, src)).string;
        }
    }.f;

    try std.testing.expectEqualStrings("[\"a\",\"ã\",\"\\\"\"]", try str(a, "chars('aã\"')"));
    try std.testing.expectEqualStrings("[]", try str(a, "chars('')"));
    try std.testing.expectEqualStrings("[0,1,2]", try str(a, "json_range(3)"));
    try std.testing.expectEqualStrings("[2,3,4]", try str(a, "json_range(2, 5)"));
    try std.testing.expectEqualStrings("[]", try str(a, "json_range(3, 1)"));
    try std.testing.expectEqual(@as(i64, 3), (try evalLit(a, "json_length('[1, [2, 3], {}]')")).int);
    try std.testing.expectError(error.InvalidJson, evalLit(a, "json_length('{}')"));
    try std.testing.expectEqualStrings("[2,3]", try str(a, "json_slice('[1,2,3,4]', 1, 3)"));
    try std.testing.expectEqualStrings("[3,4]", try str(a, "json_slice('[1,2,3,4]', -2)"));
    try std.testing.expectEqualStrings("[]", try str(a, "json_slice('[1,2,3,4]', 9)"));
    try std.testing.expectEqualStrings("[1,{\"k\":2},3]", try str(a, "json_concat('[1]', '[{\"k\": 2}, 3]')"));
    try std.testing.expect((try evalLit(a, "json_concat('[1]', NULL)")) == .null);
    try std.testing.expectEqual(@as(i64, 8), (try evalLit(a, "11 - json_reduce(chars('112223330001'), 0, (acc, c, i) -> acc + (ascii(c) - 48) * CAST(json_get('[5,4,3,2,9,8,7,6,5,4,3,2]', CAST(i AS STRING)) AS INT)) % 11")).int);
}

test "a failed CAST names its value and type, and TRY_CAST leaves no note" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectError(error.CastFailed, evalLit(a, "CAST('03/10/2026' AS DATE)"));
    try std.testing.expectEqualStrings("CAST: '03/10/2026' is not a DATE (YYYY-MM-DD; strptime reads other formats)", takeFailure(error.CastFailed).?);
    try std.testing.expectError(error.CastFailed, evalLit(a, "CAST('1,5' AS DECIMAL(10,2))"));
    try std.testing.expectEqualStrings("CAST: '1,5' is not a DECIMAL(10,2)", takeFailure(error.CastFailed).?);
    try std.testing.expect((try evalLit(a, "TRY_CAST('x' AS INT)")) == .null);
    try std.testing.expect(takeFailure(error.CastFailed) == null);
}

test "check-time errors: bad strftime directive, sub-day date_add on a date" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const schema = types.Schema{ .fields = &.{
        .{ .name = "ts", .ty = Type.init(.timestamp) },
        .{ .name = "d", .ty = Type.init(.date) },
    } };
    var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    var ctx = TypeCtx{ .schema = schema, .arena = a };

    const bad_fmt = try parser.parseExprStr(a, "strftime(ts, '%Q')", &diag);
    try std.testing.expectError(error.TypeError, ctx.typeOf(bad_fmt));
    try std.testing.expect(std.mem.indexOf(u8, ctx.msg, "strftime") != null);

    ctx.msg = "";
    const bad_unit = try parser.parseExprStr(a, "date_add('hour', 1, d)", &diag);
    try std.testing.expectError(error.TypeError, ctx.typeOf(bad_unit));
    try std.testing.expect(std.mem.indexOf(u8, ctx.msg, "date_add") != null);

    ctx.msg = "";
    const ok = try parser.parseExprStr(a, "date_add('day', 7, d)", &diag);
    try std.testing.expectEqual(types.TypeKind.date, (try ctx.typeOf(ok)).kind);
}

test "orderF64: a total order over NaN, so comparisons never hit unreachable" {
    const nan = std.math.nan(f64);
    try std.testing.expectEqual(std.math.Order.eq, orderF64(nan, nan));
    try std.testing.expectEqual(std.math.Order.gt, orderF64(nan, 1.0));
    try std.testing.expectEqual(std.math.Order.lt, orderF64(1.0, nan));
    try std.testing.expectEqual(std.math.Order.gt, orderF64(nan, std.math.inf(f64)));
    try std.testing.expectEqual(std.math.Order.eq, orderF64(0.0, -0.0));
    try std.testing.expectEqual(std.math.Order.lt, orderF64(-1.0, 1.0));

    const v_nan = Value{ .float = nan };
    try std.testing.expectEqual(std.math.Order.eq, compareValues(v_nan, v_nan).?);
    try std.testing.expectEqual(std.math.Order.gt, compareValues(v_nan, .{ .int = 9 }).?);
}

test "integer arithmetic overflow is an error, not a silent wrap" {
    const big = Value{ .int = std.math.maxInt(i64) };
    const one = Value{ .int = 1 };
    try std.testing.expectError(error.IntOverflow, arith(.add, big, one));
    try std.testing.expectError(error.IntOverflow, arith(.mul, big, .{ .int = 2 }));
    try std.testing.expectError(error.IntOverflow, arith(.sub, .{ .int = std.math.minInt(i64) }, one));
    try std.testing.expectEqual(@as(i64, 5), (try arith(.add, .{ .int = 2 }, .{ .int = 3 })).int);

    const min = Value{ .int = std.math.minInt(i64) };
    try std.testing.expectError(error.IntOverflow, arith(.div, min, .{ .int = -1 }));
    try std.testing.expectEqual(@as(i64, 0), (try arith(.mod, min, .{ .int = -1 })).int);
    try std.testing.expectEqual(@as(i64, -7), (try arith(.div, .{ .int = 7 }, .{ .int = -1 })).int);
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    try std.testing.expectError(error.IntOverflow, evalLit(ar.allocator(), "-(-9223372036854775807 - 1)"));
    try std.testing.expectError(error.IntOverflow, evalLit(ar.allocator(), "abs(-9223372036854775807 - 1)"));
}

test "numeric semantics: % truncates for every kind; decimals round, negate and cast exactly" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqual(@as(f64, -2), (try evalLit(a, "-5.0 % 3")).float);
    try std.testing.expectEqual(@as(f64, -1.5), (try evalLit(a, "-5.5 % 2")).float);
    const m = (try evalLit(a, "CAST(-1.005 AS DECIMAL(10,3)) % 2")).decimal;
    try std.testing.expectEqual(@as(i128, -1005), m.unscaled);
    try std.testing.expectEqual(@as(u8, 3), m.scale);

    const r = (try evalLit(a, "round(CAST(-1.005 AS DECIMAL(10,3)), 2)")).decimal;
    try std.testing.expectEqual(@as(i128, -101), r.unscaled);
    try std.testing.expectEqual(@as(u8, 2), r.scale);
    try std.testing.expectEqual(@as(i128, 3), (try evalLit(a, "round(CAST(2.5 AS DECIMAL(4,1)))")).decimal.unscaled);
    try std.testing.expectEqual(@as(i128, 1000), (try evalLit(a, "round(CAST(149.9 AS DECIMAL(5,1)), -2)")).decimal.unscaled);

    try std.testing.expectEqual(@as(i128, -25), (try evalLit(a, "-CAST(2.5 AS DECIMAL(4,1))")).decimal.unscaled);
    try std.testing.expectEqual(@as(i64, 3), (try evalLit(a, "CAST(CAST(2.5 AS DECIMAL(4,1)) AS INT)")).int);
    try std.testing.expectEqual(@as(i64, -3), (try evalLit(a, "CAST(CAST(-2.5 AS DECIMAL(4,1)) AS INT)")).int);
    try std.testing.expectEqual(@as(f64, 2.5), (try evalLit(a, "CAST(CAST(2.5 AS DECIMAL(4,1)) AS DOUBLE)")).float);
}

test "timestamps keep sub-second precision through parse and format" {
    const us = parseIsoTimestamp("2026-08-08 12:34:56.123456").?;
    try std.testing.expectEqual(@as(i64, 123456), @mod(us, 1_000_000));
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    try std.testing.expectEqualStrings(
        "2026-08-08 12:34:56.123456",
        try formatTimestamp(ar.allocator(), us),
    );
    try std.testing.expectEqual(@as(i64, 100000), @mod(parseIsoTimestamp("2026-08-08 12:34:56.1").?, 1_000_000));
    const w = parseIsoTimestamp("2026-08-08 12:34:56").?;
    try std.testing.expectEqualStrings("2026-08-08 12:34:56", try formatTimestamp(ar.allocator(), w));
    try std.testing.expect(parseIsoTimestamp("2026-08-08 12:34:56.12x") == null);
}

test "writeValue renders exactly what valueToString does, for every kind" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const cases = [_]Value{
        .null,
        .{ .bool = true },
        .{ .bool = false },
        .{ .int = 0 },
        .{ .int = -1 },
        .{ .int = std.math.maxInt(i64) },
        .{ .int = std.math.minInt(i64) },
        .{ .float = 0 },
        .{ .float = -0.0 },
        .{ .float = 0.1 },
        .{ .float = 1.0 / 3.0 },
        .{ .float = -2.5e-8 },
        .{ .float = 1.7976931348623157e308 },
        .{ .float = 5e-324 },
        .{ .float = std.math.inf(f64) },
        .{ .float = -std.math.inf(f64) },
        .{ .float = std.math.nan(f64) },
        .{ .decimal = .{ .unscaled = 0, .scale = 0 } },
        .{ .decimal = .{ .unscaled = 1700, .scale = 2 } },
        .{ .decimal = .{ .unscaled = -1700, .scale = 2 } },
        .{ .decimal = .{ .unscaled = 5, .scale = 6 } },
        .{ .decimal = .{ .unscaled = std.math.maxInt(i128), .scale = 0 } },
        .{ .decimal = .{ .unscaled = std.math.minInt(i128) + 1, .scale = 10 } },
        .{ .string = "" },
        .{ .string = "plain" },
        .{ .bytes = "raw" },
        .{ .date = 0 },
        .{ .date = 20000 },
        .{ .date = 2932896 },
        .{ .time = 0 },
        .{ .time = 1 },
        .{ .time = 86_400_000_000 - 1 },
        .{ .timestamp = 0 },
        .{ .timestamp = -1 },
        .{ .timestamp = 1_754_000_000_000_000 },
        .{ .timestamp = 1_754_000_000_123_456 },
    };

    for (cases) |v| {
        var buf: [512]u8 = undefined;
        var w = std.Io.Writer.fixed(&buf);
        try writeValue(&w, v);
        const want = try valueToString(a, v);
        std.testing.expectEqualStrings(want, w.buffered()) catch |e| {
            std.debug.print("mismatch on {s}\n", .{@tagName(v)});
            return e;
        };
    }
}

test "formatTime suppresses an all-zero fraction, like formatTimestamp" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings("12:00:00", try formatTime(a, 12 * 3600 * 1_000_000));
    try std.testing.expectEqualStrings("00:00:00", try formatTime(a, 0));
    try std.testing.expectEqualStrings("23:59:59.999999", try formatTime(a, 86_400_000_000 - 1));
    try std.testing.expectEqualStrings("00:00:00.000001", try formatTime(a, 1));
}

test "regexp_replace: the compiled-pattern cache keys on bytes, not on identity" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    try std.testing.expectEqualStrings("X-b-c", (try evalLit(a, "regexp_replace('a-b-c', 'a', 'X')")).string);
    try std.testing.expectEqualStrings("a-b-X", (try evalLit(a, "regexp_replace('a-b-c', 'c', 'X')")).string);
    try std.testing.expectEqualStrings("X-b-c", (try evalLit(a, "regexp_replace('a-b-c', 'a', 'X')")).string);

    try std.testing.expectEqualStrings("Xbc", (try evalLit(a, "regexp_replace('abc', '^a', 'X')")).string);
    try std.testing.expectEqualStrings("abX", (try evalLit(a, "regexp_replace('abc', 'c$', 'X')")).string);

    try std.testing.expectEqualStrings("b-a", (try evalLit(a, "regexp_replace('a-b', '(a)-(b)', '\\2-\\1')")).string);
    try std.testing.expectEqualStrings("b-a", (try evalLit(a, "regexp_replace('a-b', '(a)-(b)', '\\2-\\1')")).string);

    try std.testing.expectError(error.CastFailed, evalLit(a, "regexp_replace('abc', '(', 'X')"));
    try std.testing.expectEqualStrings("Xbc", (try evalLit(a, "regexp_replace('abc', '^a', 'X')")).string);

    try std.testing.expectEqualStrings("abc", (try evalLit(a, "regexp_replace('abc', 'zzz', 'X')")).string);
}

test "field resolution: the memo verifies its entry instead of trusting it" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const int = types.Type.init(.int);
    const wide = types.Schema{ .fields = &.{
        .{ .name = "col36", .ty = int },
        .{ .name = "col37", .ty = int },
        .{ .name = "col38", .ty = int },
        .{ .name = "col39", .ty = int },
    } };
    const flipped = types.Schema{ .fields = &.{
        .{ .name = "col39", .ty = int },
        .{ .name = "col38", .ty = int },
        .{ .name = "col37", .ty = int },
        .{ .name = "col36", .ty = int },
    } };

    const q = struct {
        fn of(alloc: std.mem.Allocator, name: []const u8) ast.QualName {
            const parts = alloc.alloc([]const u8, 1) catch unreachable;
            parts[0] = name;
            return .{ .parts = parts };
        }
    };

    for (0..3) |_| {
        for ([_][]const u8{ "col36", "col37", "col38", "col39" }, 0..) |name, i| {
            try std.testing.expectEqual(i, fieldIndex(wide, q.of(a, name)).?);
            try std.testing.expectEqual(3 - i, fieldIndex(flipped, q.of(a, name)).?);
        }
    }

    try std.testing.expect(fieldIndex(wide, q.of(a, "nope")) == null);
    try std.testing.expectEqual(@as(usize, 0), fieldIndex(wide, q.of(a, "col36")).?);
    try std.testing.expect(fieldIndex(wide, q.of(a, "nope")) == null);

    const tiny = types.Schema{ .fields = &.{.{ .name = "z", .ty = int }} };
    try std.testing.expectEqual(@as(usize, 3), fieldIndex(wide, q.of(a, "col39")).?);
    try std.testing.expect(fieldIndex(tiny, q.of(a, "col39")) == null);
    try std.testing.expectEqual(@as(usize, 0), fieldIndex(tiny, q.of(a, "z")).?);
}
