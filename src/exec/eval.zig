//! Typed expression evaluation. `TypeCtx` resolves and type-checks an expression
//! against an input schema (filling `msg` on failure); `evalColumn` evaluates an
//! expression over a whole batch into a new column. Null handling follows SQL
//! three-valued logic: any null operand in a comparison/arithmetic yields null;
//! `and`/`or` use the 3VL truth tables; `is null` is total (never null).

const std = @import("std");
const ast = @import("../lang/ast.zig");
const types = @import("../lang/types.zig");
const column = @import("column.zig");
const valuemod = @import("value.zig");
const batchmod = @import("batch.zig");

const Type = types.Type;
const Value = valuemod.Value;
const Batch = batchmod.Batch;

pub const TypeError = error{ TypeError, OutOfMemory };
pub const EvalError = error{ CastFailed, DivByZero, TypeMismatch, OutOfMemory };

// ---------------------------------------------------------------------------
// Type checking
// ---------------------------------------------------------------------------

pub const TypeCtx = struct {
    schema: types.Schema,
    arena: std.mem.Allocator,
    msg: []const u8 = "",

    pub fn typeOf(self: *TypeCtx, expr: *const ast.Expr) TypeError!Type {
        switch (expr.*) {
            .null_lit => return Type.unknownNull(),
            .bool_lit => return Type.init(.bool),
            .int_lit => return Type.init(.int),
            .float_lit => return Type.init(.float),
            .str_lit => return Type.init(.string),
            .field => |q| {
                // `?.` is meaningful only on JSON-param paths, which expansion has
                // already turned into literals; a `?.` still on a column reference is
                // a mistake, not a silent no-op.
                if (q.safe.len > 0) return self.err("`?.` (safe navigation) only applies to JSON-param paths, not column `{s}`", .{lastPart(q)});
                const idx = fieldIndex(self.schema, q) orelse
                    return self.err("unknown field `{s}`", .{lastPart(q)});
                return self.schema.fields[idx].ty;
            },
            .unary => |u| {
                const t = try self.typeOf(u.e);
                return switch (u.op) {
                    .neg => if (numericish(t)) t else self.err("`-` needs a numeric operand", .{}),
                    .not => if (boolish(t)) Type.init(.bool).withNull(t.nullable) else self.err("`not` needs a bool operand", .{}),
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
                return c.ty.withNull(s.nullable);
            },
            .is_null => |n| {
                // `is empty` (null-or-empty-string) is only meaningful for strings;
                // on a non-string it would silently collapse to `is null`.
                if (n.kind == .is_empty) {
                    const t = try self.typeOf(n.e);
                    if (!(t.kind == .string or t.kind == .bytes or t.unknown))
                        return self.err("`is empty` needs a string operand (got {s}); use `is null`", .{@tagName(t.kind)});
                }
                return Type.init(.bool);
            },
            .let_in => return self.err("internal: `let … in` should have been expanded before type-checking", .{}),
        }
    }

    fn typeOfBinary(self: *TypeCtx, b: ast.Expr.Binary) TypeError!Type {
        const lt = try self.typeOf(b.l);
        const rt = try self.typeOf(b.r);
        const nn = lt.nullable or rt.nullable or lt.unknown or rt.unknown;
        switch (b.op) {
            .add, .sub, .mul, .div, .mod => {
                if (!(numericish(lt) and numericish(rt))) return self.err("arithmetic needs numeric operands", .{});
                const k: types.TypeKind = if (lt.kind == .float or rt.kind == .float or lt.kind == .decimal or rt.kind == .decimal) .float else .int;
                return Type{ .kind = k, .nullable = nn };
            },
            .eq, .ne, .lt, .le, .gt, .ge => {
                if (!comparable(lt, rt)) return self.err("incomparable operands", .{});
                return Type{ .kind = .bool, .nullable = nn };
            },
            .@"and", .@"or" => {
                if (!(boolish(lt) and boolish(rt))) return self.err("`and`/`or` need bool operands", .{});
                return Type{ .kind = .bool, .nullable = nn };
            },
        }
    }

    fn typeOfCall(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        const name = c.name;
        inline for (.{ "count", "sum", "avg", "min", "max" }) |agg| {
            if (std.mem.eql(u8, name, agg)) return self.err("aggregate `{s}` is only valid inside `aggregate`", .{name});
        }
        if (eq(name, "now")) {
            if (c.args.len != 0) return self.err("`now` takes no arguments", .{});
            return Type.init(.timestamp); // current wall-clock, non-null
        }
        if (eq(name, "today")) {
            if (c.args.len != 0) return self.err("`today` takes no arguments", .{});
            return Type.init(.date); // current UTC date, non-null
        }
        if (eq(name, "upper") or eq(name, "lower")) {
            const a = try self.argType(c, 0);
            return Type.init(.string).withNull(a.nullable);
        }
        if (eq(name, "length")) {
            const a = try self.argType(c, 0);
            return Type.init(.int).withNull(a.nullable);
        }
        if (eq(name, "concat")) {
            if (c.args.len == 0) return self.err("`concat` needs at least one argument", .{});
            var nn = false;
            for (c.args) |a| nn = nn or (try self.typeOf(a)).nullable;
            return Type.init(.string).withNull(nn);
        }
        if (eq(name, "coalesce")) {
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
        if (eq(name, "starts_with") or eq(name, "ends_with") or eq(name, "contains") or eq(name, "like")) {
            const a = try self.argType(c, 0);
            const b = try self.argType(c, 1);
            return Type.init(.bool).withNull(a.nullable or b.nullable);
        }
        if (eq(name, "trim")) {
            const a = try self.argType(c, 0);
            return Type.init(.string).withNull(a.nullable);
        }
        if (eq(name, "substr")) {
            const a = try self.argType(c, 0);
            _ = try self.argType(c, 1); // start (1-based)
            if (c.args.len > 2) _ = try self.argType(c, 2); // optional length
            return Type.init(.string).withNull(a.nullable);
        }
        if (eq(name, "replace")) {
            const a = try self.argType(c, 0);
            _ = try self.argType(c, 1);
            _ = try self.argType(c, 2);
            return Type.init(.string).withNull(a.nullable);
        }
        return self.err("unknown function `{s}`", .{name});
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
};

fn numericish(t: Type) bool {
    return t.kind.isNumeric() or t.unknown;
}
fn boolish(t: Type) bool {
    return t.kind == .bool or t.unknown;
}
fn comparable(a: Type, b: Type) bool {
    if (a.unknown or b.unknown) return true;
    if (a.kind.isNumeric() and b.kind.isNumeric()) return true;
    return a.kind == b.kind;
}

// ---------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------

/// Evaluate `expr` over every row of `batch` into a new column of type `out_ty`.
///
/// Fast path: a vectorized kernel that works on whole typed column slices (i64 /
/// f64 / bool / bytes) with no per-row `Value` boxing — the inner loops are tight
/// and autovectorize when a column has no nulls. Expressions containing nodes the
/// vectorizer does not cover (string functions, `match`) transparently fall back
/// to the row-at-a-time evaluator below.
pub fn evalColumn(arena: std.mem.Allocator, expr: *const ast.Expr, batch: Batch, out_ty: Type) EvalError!column.Column {
    const v = evalVec(arena, expr, batch) catch |e| switch (e) {
        error.Unsupported => return evalColumnRowwise(arena, expr, batch, out_ty),
        error.CastFailed => return error.CastFailed,
        error.DivByZero => return error.DivByZero,
        error.TypeMismatch => return error.TypeMismatch,
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

// ---------------------------------------------------------------------------
// Vectorized kernels
//
// `evalVec` evaluates an expression to a `Vec` — either a full column or a
// broadcast scalar constant (the DuckDB constant-vector trick: `amount > 100`
// keeps `100` as a scalar instead of materializing 4096 copies). Binary kernels
// handle the col×col, col×scalar and scalar×col shapes. Any node the vectorizer
// does not implement raises `error.Unsupported`, which the caller turns into a
// row-at-a-time fallback for that whole expression.
// ---------------------------------------------------------------------------

const Column = column.Column;
const Bitmap = column.Bitmap;
const Decimal = valuemod.Decimal;

const VecError = error{ Unsupported, CastFailed, DivByZero, TypeMismatch, OutOfMemory };

const Vec = union(enum) {
    col: Column,
    scalar: Value,
};

/// A numeric operand normalized to one of four shapes. Decimal columns are
/// widened to an f64 column up front (rare in the hot path).
const Num = union(enum) {
    icol: struct { d: []const i64, v: Bitmap },
    fcol: struct { d: []const f64, v: Bitmap },
    iscalar: i64,
    fscalar: f64,
};

const Str = union(enum) {
    col: struct { d: []const []const u8, v: Bitmap },
    scalar: []const u8,
};

const BoolOp = union(enum) {
    col: struct { d: []const bool, v: Bitmap },
    scalar: ?bool,
};

fn evalVec(arena: std.mem.Allocator, expr: *const ast.Expr, batch: Batch) VecError!Vec {
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
        .match => return error.Unsupported,
        .let_in => return error.Unsupported, // expanded away at plan time; never vectorized
    }
}

// --- vectorized string/function kernels ---
//
// Args are evaluated once per batch (one dispatch per node), then a tight loop
// runs over the `[]const u8` slices — no per-row tree walk, no `Value` boxing.
// Functions keep the rowwise null semantics exactly: any null input → null row
// (except coalesce). Anything not covered (non-string args that rowwise would
// stringify, `match`) raises Unsupported and the whole expression falls back —
// results are identical either way, only the path differs.

/// Evaluate an argument to a string operand, or null → Unsupported fallback.
fn strArg(arena: std.mem.Allocator, e: *const ast.Expr, batch: Batch) VecError!Str {
    const v = try evalVec(arena, e, batch);
    return asStr(v) orelse error.Unsupported;
}

fn callVec(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
    const name = c.name;
    const n = batch.len;

    // Per-batch timestamps: rowwise re-reads the clock per row; one consistent
    // instant per batch is the intended (SQL-like) semantics.
    if (eq(name, "now")) return .{ .scalar = .{ .timestamp = std.time.microTimestamp() } };
    if (eq(name, "today")) return .{ .scalar = .{ .date = @intCast(@divFloor(std.time.microTimestamp(), 86_400_000_000)) } };

    if (eq(name, "upper") or eq(name, "lower")) {
        if (c.args.len < 1) return error.Unsupported;
        const s = try strArg(arena, c.args[0], batch);
        const up = eq(name, "upper");
        const out = try arena.alloc([]const u8, n);
        var bm = try Bitmap.initFull(arena, n);
        var any = false;
        var i: usize = 0;
        while (i < n) : (i += 1) {
            const sv = strAt(s, i) orelse {
                out[i] = "";
                bm.setValid(i, false);
                any = true;
                continue;
            };
            const o = try arena.dupe(u8, sv);
            for (o) |*ch| ch.* = if (up) std.ascii.toUpper(ch.*) else std.ascii.toLower(ch.*);
            out[i] = o;
        }
        return mkCol(Type.init(.string).withNull(any), n, bm, .{ .bytes = out });
    }

    if (eq(name, "trim")) {
        if (c.args.len < 1) return error.Unsupported;
        const s = try strArg(arena, c.args[0], batch);
        const out = try arena.alloc([]const u8, n);
        var bm = try Bitmap.initFull(arena, n);
        var any = false;
        var i: usize = 0;
        while (i < n) : (i += 1) {
            const sv = strAt(s, i) orelse {
                out[i] = "";
                bm.setValid(i, false);
                any = true;
                continue;
            };
            out[i] = trim(sv); // subslice of the source bytes: no copy
        }
        return mkCol(Type.init(.string).withNull(any), n, bm, .{ .bytes = out });
    }

    if (eq(name, "length")) {
        if (c.args.len < 1) return error.Unsupported;
        const s = try strArg(arena, c.args[0], batch);
        const out = try arena.alloc(i64, n);
        var bm = try Bitmap.initFull(arena, n);
        var any = false;
        var i: usize = 0;
        while (i < n) : (i += 1) {
            if (strAt(s, i)) |sv| {
                out[i] = @intCast(sv.len);
            } else {
                out[i] = 0;
                bm.setValid(i, false);
                any = true;
            }
        }
        return mkCol(Type.init(.int).withNull(any), n, bm, .{ .i64 = out });
    }

    if (eq(name, "starts_with") or eq(name, "ends_with") or eq(name, "contains") or eq(name, "like")) {
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

    if (eq(name, "concat")) {
        if (c.args.len == 0) return error.Unsupported;
        const parts = try arena.alloc(Str, c.args.len);
        for (c.args, parts) |a, *sp| sp.* = try strArg(arena, a, batch);
        const out = try arena.alloc([]const u8, n);
        var bm = try Bitmap.initFull(arena, n);
        var any = false;
        var i: usize = 0;
        rows: while (i < n) : (i += 1) {
            var total: usize = 0;
            for (parts) |sp| {
                total += (strAt(sp, i) orelse {
                    out[i] = "";
                    bm.setValid(i, false);
                    any = true;
                    continue :rows;
                }).len;
            }
            const o = try arena.alloc(u8, total);
            var off: usize = 0;
            for (parts) |sp| {
                const sv = strAt(sp, i).?;
                @memcpy(o[off..][0..sv.len], sv);
                off += sv.len;
            }
            out[i] = o;
        }
        return mkCol(Type.init(.string).withNull(any), n, bm, .{ .bytes = out });
    }

    if (eq(name, "coalesce")) {
        if (c.args.len == 0) return error.Unsupported;
        const parts = try arena.alloc(Str, c.args.len);
        for (c.args, parts) |a, *sp| sp.* = try strArg(arena, a, batch);
        const out = try arena.alloc([]const u8, n);
        var bm = try Bitmap.initFull(arena, n);
        var any = false;
        var i: usize = 0;
        rows: while (i < n) : (i += 1) {
            for (parts) |sp| {
                if (strAt(sp, i)) |sv| {
                    out[i] = sv; // alias the winning slice: no copy
                    continue :rows;
                }
            }
            out[i] = "";
            bm.setValid(i, false);
            any = true;
        }
        return mkCol(Type.init(.string).withNull(any), n, bm, .{ .bytes = out });
    }

    if (eq(name, "substr")) {
        if (c.args.len < 2) return error.Unsupported;
        const s = try strArg(arena, c.args[0], batch);
        const start = (try asNum(arena, try evalVec(arena, c.args[1], batch), n)) orelse return error.Unsupported;
        if (!isIntNum(start)) return error.Unsupported;
        var len_num: ?Num = null;
        if (c.args.len > 2) {
            len_num = (try asNum(arena, try evalVec(arena, c.args[2], batch), n)) orelse return error.Unsupported;
            if (!isIntNum(len_num.?)) return error.Unsupported;
        }
        const out = try arena.alloc([]const u8, n);
        var bm = try Bitmap.initFull(arena, n);
        var any = false;
        var i: usize = 0;
        while (i < n) : (i += 1) {
            const sv = strAt(s, i);
            const start_ok = numValid(start, i);
            const len_ok = if (len_num) |l| numValid(l, i) else true;
            if (sv == null or !start_ok or !len_ok) {
                out[i] = "";
                bm.setValid(i, false);
                any = true;
                continue;
            }
            out[i] = try substrBytes(arena, sv.?, numI(start, i), if (len_num) |l| numI(l, i) else null);
        }
        return mkCol(Type.init(.string).withNull(any), n, bm, .{ .bytes = out });
    }

    if (eq(name, "replace")) {
        if (c.args.len < 3) return error.Unsupported;
        const s = try strArg(arena, c.args[0], batch);
        const f = try strArg(arena, c.args[1], batch);
        const t = try strArg(arena, c.args[2], batch);
        const out = try arena.alloc([]const u8, n);
        var bm = try Bitmap.initFull(arena, n);
        var any = false;
        var i: usize = 0;
        while (i < n) : (i += 1) {
            const sv = strAt(s, i);
            const fv = strAt(f, i);
            const tv = strAt(t, i);
            if (sv == null or fv == null or tv == null) {
                out[i] = "";
                bm.setValid(i, false);
                any = true;
                continue;
            }
            if (fv.?.len == 0) {
                out[i] = sv.?;
                continue;
            }
            const o = try arena.alloc(u8, std.mem.replacementSize(u8, sv.?, fv.?, tv.?));
            _ = std.mem.replace(u8, sv.?, fv.?, tv.?, o);
            out[i] = o;
        }
        return mkCol(Type.init(.string).withNull(any), n, bm, .{ .bytes = out });
    }

    return error.Unsupported;
}

fn unaryVec(arena: std.mem.Allocator, u: ast.Expr.Unary, batch: Batch) VecError!Vec {
    const v = try evalVec(arena, u.e, batch);
    switch (u.op) {
        .neg => switch (v) {
            .scalar => |s| return .{ .scalar = switch (s) {
                .null => .null,
                .int => |x| .{ .int = -x },
                .float => |x| .{ .float = -x },
                else => return error.Unsupported,
            } },
            .col => |c| {
                const n = c.len;
                switch (c.ty.kind) {
                    .int => {
                        const out = try arena.alloc(i64, n);
                        for (c.data.i64, 0..) |x, i| out[i] = -x;
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
    }
}

fn isNullVec(arena: std.mem.Allocator, n: ast.Expr.IsNull, batch: Batch) VecError!Vec {
    // `is empty` (null-or-empty-string) has no vectorized kernel — fall the whole
    // expression back to the rowwise evaluator, which implements it.
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
            // `is null` is total — the result is never itself null.
            const bm = try Bitmap.initFull(arena, rows);
            return mkCol(Type.init(.bool), rows, bm, .{ .b = out });
        },
    }
}

fn binaryVec(arena: std.mem.Allocator, b: ast.Expr.Binary, batch: Batch) VecError!Vec {
    switch (b.op) {
        .@"and", .@"or" => return boolOpVec(arena, b.op, b.l, b.r, batch),
        .add, .sub, .mul, .div, .mod => {
            const l = try evalVec(arena, b.l, batch);
            const r = try evalVec(arena, b.r, batch);
            if (scalarNull(l) or scalarNull(r)) return .{ .scalar = .null };
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
            if (asStr(l)) |ls| {
                if (asStr(r)) |rs| return cmpStrVec(arena, b.op, ls, rs, batch.len);
            }
            return error.Unsupported;
        },
    }
}

/// Vectorized arithmetic/comparison over two numeric operands: dispatches the
/// runtime op and int-vs-float lane type to a comptime-specialized kernel
/// (`numOpVecT`), keeping each op's inner loop free of per-row branching —
/// the same codegen shape as the previous hand-unrolled per-op loops.
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

/// Shared valid/nullable template behind `numOpVec`: apply `op` elementwise
/// over two numeric operands widened to comptime `T`. All-valid inputs skip
/// the per-row validity checks; otherwise null-in → null-out with the evicted
/// slot zero-filled (builder convention).
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

/// Result element type of `applyOp`: comparisons yield `bool`, arithmetic the
/// operand type.
fn OpOut(comptime T: type, comptime op: ast.BinOp) type {
    return switch (op) {
        .eq, .ne, .lt, .le, .gt, .ge => bool,
        else => T,
    };
}

/// One elementwise binary op over already-valid operands widened to `T` (i64
/// or f64). Int div/mod raise on a zero divisor; float div/mod follow float
/// semantics (`/`, `@mod`) — both matching the rowwise evaluator.
inline fn applyOp(comptime T: type, comptime op: ast.BinOp, a: T, d: T) VecError!OpOut(T, op) {
    return switch (op) {
        .add => a + d,
        .sub => a - d,
        .mul => a * d,
        .div => if (T == i64)
            (if (d == 0) error.DivByZero else @divTrunc(a, d))
        else
            a / d,
        .mod => if (T == i64)
            (if (d == 0) error.DivByZero else @rem(a, d))
        else
            @mod(a, d),
        .eq, .ne, .lt, .le, .gt, .ge => cmpResult(op, std.math.order(a, d)),
        else => unreachable,
    };
}

inline fn outData(comptime Out: type, out: []Out) Column.Data {
    return if (Out == bool) .{ .b = out } else if (Out == i64) .{ .i64 = out } else .{ .f64 = out };
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

/// Evaluate a subexpression whose value the rowwise evaluator might never need
/// (an untaken `if` branch, the short-circuited side of and/or). The vectorized
/// path is eager — it computes every row of every branch — so a value-dependent
/// error (div-by-zero, failed cast) here must not escape: rowwise semantics only
/// raise it on rows that actually take the branch. Demote it to Unsupported,
/// which falls the whole expression back to the lazy rowwise evaluator: that
/// either succeeds (the error was on an untaken row) or raises it for real.
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
        // SQL three-valued logic.
        var res: ?bool = null;
        if (op == .@"and") {
            if ((lk and !lval) or (rk and !rval)) {
                res = false;
            } else if (lk and lval and rk and rval) {
                res = true;
            }
        } else { // or
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

fn castVec(arena: std.mem.Allocator, c: ast.Expr.Cast, batch: Batch) VecError!Vec {
    const v = try evalVec(arena, c.e, batch);
    const target = c.ty.kind;
    switch (v) {
        .scalar => |s| {
            if (s.isNull()) return .{ .scalar = .null };
            return .{ .scalar = try castValue(arena, s, target) };
        },
        .col => |col| return castColVec(arena, col, target, batch.len),
    }
}

/// Float→int cast guarding the i64 range and NaN/inf (all of which `@intFromFloat`
/// treats as illegal behavior — a safety-check panic in safe builds, UB otherwise).
/// Out-of-range/NaN → CastFailed, matching the string→int arm's error contract.
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
                .string => for (col.data.bytes, 0..) |s, i| {
                    if (!col.validity.get(i)) {
                        out[i] = 0;
                        continue;
                    }
                    out[i] = std.fmt.parseInt(i64, trim(s), 10) catch return error.CastFailed;
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
                .string => for (col.data.bytes, 0..) |s, i| {
                    if (!col.validity.get(i)) {
                        out[i] = 0;
                        continue;
                    }
                    out[i] = std.fmt.parseFloat(f64, trim(s)) catch return error.CastFailed;
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
        else => return error.Unsupported, // to-string / to-decimal fall back to rowwise
    }
}

fn condVec(arena: std.mem.Allocator, c: ast.Expr.Cond, batch: Batch) VecError!Vec {
    // The condition is evaluated on every row in both paths, so its errors are
    // genuine; the branches are lazy rowwise and must go through evalVecLazy.
    const cond = try evalVec(arena, c.cond, batch);
    const tv = try evalVecLazy(arena, c.then, batch);
    const ev = try evalVecLazy(arena, c.els, batch);
    const n = batch.len;
    const t = (try realize(arena, tv, n)) orelse return error.Unsupported;
    const e = (try realize(arena, ev, n)) orelse return error.Unsupported;
    if (t.ty.kind != e.ty.kind) return error.Unsupported;

    const take = try arena.alloc(bool, n);
    switch (cond) {
        .scalar => |s| {
            const all = (s == .bool and s.bool);
            for (take) |*x| x.* = all;
        },
        .col => |cc| {
            if (cc.ty.kind != .bool) return error.Unsupported;
            var i: usize = 0;
            while (i < n) : (i += 1) take[i] = cc.validity.get(i) and cc.data.b[i];
        },
    }
    return mergeCols(arena, take, t, e);
}

/// Pick, per row, the matching element from `t` (where `take[i]`) or `e`.
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
            const o = try arena.alloc([]const u8, n);
            mergePick([]const u8, o, &bm, take, ts, e.data.bytes, t.validity, e.validity);
            break :blk .{ .bytes = o };
        },
    };
    return .{ .col = .{ .ty = t.ty.withNull(true), .len = n, .validity = bm, .data = data } };
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

// --- operand normalization & element accessors ---

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
                for (c.data.dec, 0..) |d, i| out[i] = @as(f64, @floatFromInt(d.unscaled)) / pow10f(d.scale);
                break :blk Num{ .fcol = .{ .d = out, .v = c.validity } };
            },
            else => null,
        },
    }
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
/// `numI`/`numF` selected by comptime lane type (folds to a direct call).
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
        .col => |c| if (c.v.get(i)) c.d[i] else null,
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

/// Turn a `Vec` into a concrete column of `n` rows, broadcasting a scalar across
/// all rows (used when the whole expression collapses to a constant).
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
                else => return null, // null/temporal scalar: unknown target kind -> fall back
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

/// Evaluate an expression at PLAN TIME against named scalar bindings (params,
/// for-each loop variables) — no columns exist yet. Implemented by materializing
/// the bindings as a one-row batch and reusing `evalRow`, so the full expression
/// language (the C primitives, `match`, `cond`, `cast`) is available for `match`
/// subjects/guards and `fn` folding. Errors if a referenced name isn't bound.
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

/// True for an empty string/bytes value — the non-null half of `is empty`.
fn isEmptyVal(v: Value) bool {
    return switch (v) {
        .string, .bytes => |s| s.len == 0,
        else => false,
    };
}

pub fn evalRow(arena: std.mem.Allocator, expr: *const ast.Expr, batch: Batch, row: usize) EvalError!Value {
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
                    .int => |x| .{ .int = -x },
                    .float => |x| .{ .float = -x },
                    else => error.TypeMismatch,
                },
                .not => .{ .bool = !v.bool },
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
            return castValue(arena, v, c.ty.kind);
        },
        .match => |m| return evalMatch(arena, m, batch, row),
        .call => |c| return evalCall(arena, c, batch, row),
        // `let … in` is inlined away during expansion. It can only reach here in a
        // raw `${ ... }` interpolation expression (which is not expanded); that is
        // unsupported — use the function/`coalesce` forms there instead.
        .let_in => return error.TypeMismatch,
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
                .eq, .ne, .lt, .le, .gt, .ge => blk: {
                    const ord = compareValues(l, r) orelse break :blk error.TypeMismatch;
                    break :blk Value{ .bool = cmpResult(b.op, ord) };
                },
                else => unreachable,
            };
        },
    }
}

fn arith(op: ast.BinOp, l: Value, r: Value) EvalError!Value {
    if (l == .int and r == .int) {
        const a = l.int;
        const b = r.int;
        return switch (op) {
            .add => .{ .int = a + b },
            .sub => .{ .int = a - b },
            .mul => .{ .int = a * b },
            .div => if (b == 0) error.DivByZero else .{ .int = @divTrunc(a, b) },
            .mod => if (b == 0) error.DivByZero else .{ .int = @rem(a, b) },
            else => unreachable,
        };
    }
    const a = toF64(l);
    const b = toF64(r);
    return switch (op) {
        .add => .{ .float = a + b },
        .sub => .{ .float = a - b },
        .mul => .{ .float = a * b },
        .div => .{ .float = a / b },
        .mod => .{ .float = @mod(a, b) },
        else => unreachable,
    };
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
    const name = c.name;
    if (eq(name, "now")) {
        return .{ .timestamp = std.time.microTimestamp() };
    }
    if (eq(name, "today")) {
        const days = @divFloor(std.time.microTimestamp(), 86_400_000_000);
        return .{ .date = @intCast(days) };
    }
    if (eq(name, "coalesce")) {
        for (c.args) |a| {
            const v = try evalRow(arena, a, batch, row);
            if (!v.isNull()) return v;
        }
        return .null;
    }
    if (eq(name, "upper") or eq(name, "lower")) {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        const out = try arena.dupe(u8, try valueToString(arena, v));
        for (out) |*ch| ch.* = if (eq(name, "upper")) std.ascii.toUpper(ch.*) else std.ascii.toLower(ch.*);
        return .{ .string = out };
    }
    if (eq(name, "length")) {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        return .{ .int = @intCast((try valueToString(arena, v)).len) };
    }
    if (eq(name, "concat")) {
        var buf = std.array_list.Managed(u8).init(arena);
        for (c.args) |a| {
            const v = try evalRow(arena, a, batch, row);
            if (v.isNull()) return .null;
            try buf.appendSlice(try valueToString(arena, v));
        }
        return .{ .string = try buf.toOwnedSlice() };
    }
    if (eq(name, "starts_with") or eq(name, "ends_with") or eq(name, "contains")) {
        const sv = try evalRow(arena, c.args[0], batch, row);
        const pv = try evalRow(arena, c.args[1], batch, row);
        if (sv.isNull() or pv.isNull()) return .null;
        const s = try valueToString(arena, sv);
        const p = try valueToString(arena, pv);
        const r = if (eq(name, "starts_with")) std.mem.startsWith(u8, s, p) else if (eq(name, "ends_with")) std.mem.endsWith(u8, s, p) else (std.mem.indexOf(u8, s, p) != null);
        return .{ .bool = r };
    }
    if (eq(name, "like")) {
        const sv = try evalRow(arena, c.args[0], batch, row);
        const pv = try evalRow(arena, c.args[1], batch, row);
        if (sv.isNull() or pv.isNull()) return .null;
        return .{ .bool = likeMatch(try valueToString(arena, sv), try valueToString(arena, pv)) };
    }
    if (eq(name, "trim")) {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        return .{ .string = try arena.dupe(u8, trim(try valueToString(arena, v))) };
    }
    if (eq(name, "substr")) {
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
        return .{ .string = try substrBytes(arena, try valueToString(arena, sv), toI64(startv), len_opt) };
    }
    if (eq(name, "replace")) {
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
    return error.TypeMismatch;
}

pub fn castValue(arena: std.mem.Allocator, v: Value, kind: types.TypeKind) EvalError!Value {
    return switch (kind) {
        .int => switch (v) {
            .int => v,
            .float => |x| .{ .int = try floatToInt(x) },
            .bool => |x| .{ .int = if (x) 1 else 0 },
            .string => |s| .{ .int = std.fmt.parseInt(i64, trim(s), 10) catch return error.CastFailed },
            else => error.CastFailed,
        },
        .float => switch (v) {
            .float => v,
            .int => |x| .{ .float = @floatFromInt(x) },
            .string => |s| .{ .float = std.fmt.parseFloat(f64, trim(s)) catch return error.CastFailed },
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

/// `YYYY-MM-DD` from a day count since the 1970 epoch.
pub fn formatDate(arena: std.mem.Allocator, days: i64) ![]const u8 {
    const c = civilFromDays(days);
    return std.fmt.allocPrint(arena, "{d:0>4}-{d:0>2}-{d:0>2}", .{ @as(u32, @intCast(c.y)), c.m, c.d });
}

/// `HH:MM:SS.ffffff` from microseconds since midnight. (Time parts are unsigned so
/// `{d:0>2}` zero-pads instead of printing a sign.)
pub fn formatTime(arena: std.mem.Allocator, t: i64) ![]const u8 {
    const us: u64 = @intCast(@mod(t, 86_400_000_000));
    const secs = us / 1_000_000;
    return std.fmt.allocPrint(arena, "{d:0>2}:{d:0>2}:{d:0>2}.{d:0>6}", .{ secs / 3600, (secs % 3600) / 60, secs % 60, us % 1_000_000 });
}

/// `YYYY-MM-DD HH:MM:SS` from microseconds since the 1970 epoch (floor-divides so
/// pre-epoch instants format correctly).
pub fn formatTimestamp(arena: std.mem.Allocator, micros: i64) ![]const u8 {
    const days = @divFloor(micros, 86_400_000_000);
    const us: u64 = @intCast(micros - days * 86_400_000_000); // intraday remainder, ≥ 0
    const secs = us / 1_000_000;
    const c = civilFromDays(days);
    return std.fmt.allocPrint(arena, "{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}", .{
        @as(u32, @intCast(c.y)), c.m, c.d, secs / 3600, (secs % 3600) / 60, secs % 60,
    });
}

/// Civil (Gregorian) date from a day count since the 1970 epoch (Howard Hinnant's
/// algorithm). Shared by the text-sink serializer and the SQL INSERT serializer.
pub fn civilFromDays(z0: i64) struct { y: i64, m: u32, d: u32 } {
    const z = z0 + 719468;
    const era = @divFloor(if (z >= 0) z else z - 146096, 146097);
    const doe = z - era * 146097; // [0, 146096]
    const yoe = @divFloor(doe - @divFloor(doe, 1460) + @divFloor(doe, 36524) - @divFloor(doe, 146096), 365);
    const y = yoe + era * 400;
    const doy = doe - (365 * yoe + @divFloor(yoe, 4) - @divFloor(yoe, 100));
    const mp = @divFloor(5 * doy + 2, 153);
    const d: u32 = @intCast(doy - @divFloor(153 * mp + 2, 5) + 1);
    const m: u32 = @intCast(if (mp < 10) mp + 3 else mp - 9);
    return .{ .y = y + (if (m <= 2) @as(i64, 1) else 0), .m = m, .d = d };
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

/// Render an exact decimal `unscaled * 10^-scale`, e.g. (12345, 2) -> "123.45".
pub fn formatDecimal(arena: std.mem.Allocator, unscaled: i128, scale: u8) ![]const u8 {
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
    while (n <= scale) : (n += 1) digits[n] = '0'; // pad so there's an integer digit

    var out = std.array_list.Managed(u8).init(arena);
    if (neg) try out.append('-');
    var k: usize = n;
    while (k > 0) {
        k -= 1;
        try out.append(digits[k]);
        if (scale > 0 and k == scale) try out.append('.');
    }
    return out.toOwnedSlice();
}

// --- small helpers ---

fn fieldIndex(schema: types.Schema, q: ast.QualName) ?usize {
    return schema.indexOf(lastPart(q));
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
        .decimal => |d| @as(f64, @floatFromInt(d.unscaled)) / pow10f(d.scale),
        else => 0,
    };
}
fn pow10f(n: u8) f64 {
    var r: f64 = 1;
    var k: u8 = 0;
    while (k < n) : (k += 1) r *= 10;
    return r;
}
pub fn compareValues(a: Value, b: Value) ?std.math.Order {
    if (isNum(a) and isNum(b)) {
        if (a == .int and b == .int) return std.math.order(a.int, b.int);
        return std.math.order(toF64(a), toF64(b));
    }
    if (a == .string and b == .string) return std.mem.order(u8, a.string, b.string);
    if (a == .bool and b == .bool) return std.math.order(@intFromBool(a.bool), @intFromBool(b.bool));
    if (a == .timestamp and b == .timestamp) return std.math.order(a.timestamp, b.timestamp);
    if (a == .date and b == .date) return std.math.order(a.date, b.date);
    if (a == .time and b == .time) return std.math.order(a.time, b.time);
    return null;
}
fn eq(a: []const u8, b: []const u8) bool {
    return std.mem.eql(u8, a, b);
}
fn trim(s: []const u8) []const u8 {
    return std.mem.trim(u8, s, " \t\r\n");
}

fn toI64(v: Value) i64 {
    return switch (v) {
        .int => |x| x,
        .float => |x| @intFromFloat(x),
        .string => |s| std.fmt.parseInt(i64, std.mem.trim(u8, s, " "), 10) catch 0,
        else => 0,
    };
}

/// Byte-based substring with a 1-based start (SQL `substr`); `len` null = to end.
fn substrBytes(arena: std.mem.Allocator, s: []const u8, start1: i64, len_opt: ?i64) ![]const u8 {
    const slen: i64 = @intCast(s.len);
    var start: usize = 0;
    if (start1 > 1) start = @intCast(@min(start1 - 1, slen));
    var end: usize = s.len;
    if (len_opt) |l| {
        if (l <= 0) return "";
        end = @min(start + @as(usize, @intCast(l)), s.len);
    }
    return arena.dupe(u8, s[start..end]);
}

/// SQL `LIKE`: `%` matches any run (including empty), `_` matches one byte.
fn likeMatch(s: []const u8, pat: []const u8) bool {
    var si: usize = 0;
    var pi: usize = 0;
    var star: ?usize = null;
    var smark: usize = 0;
    while (si < s.len) {
        if (pi < pat.len and (pat[pi] == '_' or pat[pi] == s[si])) {
            si += 1;
            pi += 1;
        } else if (pi < pat.len and pat[pi] == '%') {
            star = pi;
            smark = si;
            pi += 1;
        } else if (star) |st| {
            pi = st + 1;
            smark += 1;
            si = smark;
        } else return false;
    }
    while (pi < pat.len and pat[pi] == '%') pi += 1;
    return pi == pat.len;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "substr (1-based, byte) and like wildcard matcher" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try std.testing.expectEqualStrings("01", try substrBytes(a, "SD1010", 4, 2)); // empresa code
    try std.testing.expectEqualStrings("SD1", try substrBytes(a, "SD1010", 1, 3)); // prefix
    try std.testing.expectEqualStrings("010", try substrBytes(a, "SD1010", 4, null)); // to end
    try std.testing.expectEqualStrings("", try substrBytes(a, "SD1010", 99, 2)); // past end

    try std.testing.expect(likeMatch("hello, world", "hello%"));
    try std.testing.expect(likeMatch("hello", "h_llo"));
    try std.testing.expect(likeMatch("anything", "%"));
    try std.testing.expect(!likeMatch("hello", "h_l"));
    try std.testing.expect(!likeMatch("paid", "pending%"));
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

const parser = @import("../lang/parser.zig");

test "type-check and evaluate an if-expression with 3VL" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(a,
        \\@batch
        \\read x query "q"
        \\  | filter amount > 100
        \\  | select big = if(amount >= 100, "yes", "no")
    , &diag);
    const stages = prog.stmts[1].output.stages;
    const pred = stages[1].node.filter;
    const sel = stages[2].node.select[0].computed.expr;

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
    try std.testing.expectEqualStrings("no", out.getValue(2).string); // null >= 100 -> null -> else
}

test "vectorized kernels match the rowwise evaluator" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    // Two nullable int columns with nulls in different rows.
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
        // Fallible ops on lazily-evaluated branches: x is 0 on the last row, so
        // the eager vectorized path hits a div-by-zero the rowwise path never
        // evaluates — it must fall back instead of raising.
        "if(x != 0, y / x, 0)",
        "x != 0 and y / x > 1",
        "x == 0 or y / x > 1",
    };
    for (exprs) |body| {
        const src = try std.fmt.allocPrint(a, "@batch\nread t query \"q\" | select r = {s}", .{body});
        var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        const prog = try parser.parseSource(a, src, &diag);
        const e = prog.stmts[1].output.stages[1].node.select[0].computed.expr;
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
        "concat(s, \"-\", s)",
        "starts_with(s, \"b\")",
        "ends_with(s, \"e\")",
        "contains(s, \"an\")",
        "like(s, \"%an%\")",
        "substr(s, 2, 3)",
        "replace(s, \"an\", \"AN\")",
        "coalesce(s, \"fallback\")",
        "if(contains(s, \"p\"), upper(s), s)",
        "length(trim(s)) > 5 and contains(s, \"e\")",
    };
    for (exprs) |body| {
        const src = try std.fmt.allocPrint(a, "@batch\nread t query \"q\" | select r = {s}", .{body});
        var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        const prog = try parser.parseSource(a, src, &diag);
        const e = prog.stmts[1].output.stages[1].node.select[0].computed.expr;
        var ctx = TypeCtx{ .schema = schema, .arena = a };
        const ty = try ctx.typeOf(e);

        // Every expression here must take the vectorized path for real — a
        // silent rowwise fallback would make this test vacuous.
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

test "type errors: unknown field and non-bool not" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const schema = types.Schema{ .fields = &.{.{ .name = "x", .ty = Type.init(.int) }} };

    var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(a, "@batch\nread t query \"q\" | filter missing > 1", &diag);
    const pred = prog.stmts[1].output.stages[1].node.filter;
    var ctx = TypeCtx{ .schema = schema, .arena = a };
    try std.testing.expectError(error.TypeError, ctx.typeOf(pred));
    try std.testing.expect(std.mem.indexOf(u8, ctx.msg, "unknown field") != null);

    // `not` over an int column must be rejected too (with its own message).
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
    try std.testing.expectEqual(@as(i64, -3), (try castValue(a, .{ .float = -3.9 }, .int)).int); // truncates toward zero
    try std.testing.expectEqual(@as(f64, 2.5), (try castValue(a, .{ .string = "2.5" }, .float)).float);
    try std.testing.expect((try castValue(a, .{ .string = " TRUE " }, .bool)).bool);
    try std.testing.expect(!(try castValue(a, .{ .int = 0 }, .bool)).bool);
    try std.testing.expectEqualStrings("123.45", (try castValue(a, .{ .decimal = .{ .unscaled = 12345, .scale = 2 } }, .string)).string);

    // Which error matters: all of these are CastFailed, never TypeMismatch/panic.
    try std.testing.expectError(error.CastFailed, castValue(a, .{ .string = "abc" }, .int));
    try std.testing.expectError(error.CastFailed, castValue(a, .{ .float = std.math.nan(f64) }, .int));
    try std.testing.expectError(error.CastFailed, castValue(a, .{ .float = 1e19 }, .int)); // beyond i64 range
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
    // Both the vectorized (whole-column) and rowwise paths must raise, not fall back.
    try std.testing.expectError(error.DivByZero, evalColumn(a, &div, batch, Type.init(.int).asNullable()));
    try std.testing.expectError(error.DivByZero, evalRow(a, &div, batch, 0));
    try std.testing.expectError(error.DivByZero, evalRow(a, &mod, batch, 0));

    // Float lanes follow IEEE: 6 / 0.0 is inf, and null still propagates.
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
