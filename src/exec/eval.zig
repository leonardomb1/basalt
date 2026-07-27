//! Typed expression evaluation. `TypeCtx` resolves and type-checks an expression
//! against an input schema (filling `msg` on failure); `evalColumn` evaluates an
//! expression over a whole batch into a new column. Null handling follows SQL
//! three-valued logic: any null operand in a comparison/arithmetic yields null;
//! `and`/`or` use the 3VL truth tables; `is null` is total (never null).

const std = @import("std");
const regex = @import("regex.zig");
const sqlmod = @import("../connect/sql.zig");
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
                // `try_cast` turns a failed conversion into null, so its result
                // is nullable even when the input can never be null.
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
            // Bitwise ops are INT-only: no float coercion, no string coercion.
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

    /// A date/timestamp compared against a string *literal*. The literal bends
    /// to the column's type, never the reverse, so a mistyped `'01/07/2013'`
    /// stays an error instead of degrading into a text comparison. Validated
    /// here so a bad literal fails `check` rather than the run.
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

    fn typeOfCall(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        const name = c.name;
        inline for (.{ "count", "sum", "avg", "min", "max" }) |agg| {
            if (std.mem.eql(u8, name, agg)) return self.err("aggregate `{s}` is only valid inside `aggregate`", .{name});
        }
        if (eq(name, "now")) {
            if (c.args.len != 0) return self.err("`now` takes no arguments", .{});
            return Type.init(.timestamp);
        }
        if (eq(name, "today")) {
            if (c.args.len != 0) return self.err("`today` takes no arguments", .{});
            return Type.init(.date);
        }
        if (eq(name, "regexp_replace")) {
            if (c.args.len != 3) return self.err("`regexp_replace` takes (string, pattern, replacement)", .{});
            // A literal pattern is compiled here so a bad one fails `check`
            // rather than partway through a run.
            if (c.args[1].* == .str_lit) {
                var pbuf: [16 * 1024]u8 = undefined;
                var pfba = std.heap.FixedBufferAllocator.init(&pbuf);
                _ = regex.Regex.compile(pfba.allocator(), c.args[1].str_lit) catch
                    return self.err("invalid regular expression `{s}`", .{c.args[1].str_lit});
            }
            const a = try self.argType(c, 0);
            return Type.init(.string).withNull(a.nullable);
        }
        if (eq(name, "date_trunc") or eq(name, "extract")) {
            if (c.args.len != 2) return self.err("`{s}` takes (unit, timestamp)", .{name});
            if (c.args[0].* != .str_lit) return self.err("`{s}` needs a literal unit", .{name});
            if (timeUnit(c.args[0].str_lit) == null)
                return self.err("unknown time unit `{s}`", .{c.args[0].str_lit});
            const a = try self.argType(c, 1);
            if (a.kind != .date and a.kind != .timestamp and !a.unknown)
                return self.err("`{s}` needs a date or timestamp", .{name});
            const out: types.TypeKind = if (eq(name, "extract")) .int else .timestamp;
            return Type.init(out).withNull(a.nullable);
        }
        if (eq(name, "upper") or eq(name, "lower")) {
            const a = try self.argType(c, 0);
            return Type.init(.string).withNull(a.nullable);
        }
        if (eq(name, "length") or eq(name, "strlen")) {
            const a = try self.argType(c, 0);
            return Type.init(.int).withNull(a.nullable);
        }
        if (eq(name, "bit_count") or eq(name, "to_hex")) {
            const a = try self.argType(c, 0);
            if (c.args.len != 1 or !intish(a)) return self.err("`{s}` takes one INT argument", .{name});
            const out: types.TypeKind = if (eq(name, "to_hex")) .string else .int;
            return Type.init(out).withNull(a.nullable);
        }
        if (eq(name, "from_hex")) {
            const a = try self.argType(c, 0);
            if (c.args.len != 1 or !(a.kind == .string or a.kind == .bytes or a.unknown))
                return self.err("`from_hex` takes one STRING argument", .{});
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
            _ = try self.argType(c, 1);
            if (c.args.len > 2) _ = try self.argType(c, 2);
            return Type.init(.string).withNull(a.nullable);
        }
        if (eq(name, "replace")) {
            const a = try self.argType(c, 0);
            _ = try self.argType(c, 1);
            _ = try self.argType(c, 2);
            return Type.init(.string).withNull(a.nullable);
        }
        if (try self.typeOfMathCall(c)) |t| return t;
        if (try self.typeOfStringCall(c)) |t| return t;
        if (try self.typeOfDateCall(c)) |t| return t;
        return self.err("unknown function `{s}`", .{name});
    }

    /// Numeric builtins plus the null-selecting ones (`nullif`, `greatest`,
    /// `least`). Returns null when `c` names none of them, so `typeOfCall` can
    /// keep walking its chain. Split out only to keep `typeOfCall` readable.
    fn typeOfMathCall(self: *TypeCtx, c: ast.Expr.Call) TypeError!?Type {
        const name = c.name;
        if (eq(name, "abs")) {
            if (c.args.len != 1) return self.err("`abs` takes one argument", .{});
            const a = try self.argType(c, 0);
            if (!numericish(a)) return self.err("`abs` needs a numeric argument", .{});
            return a;
        }
        if (eq(name, "floor") or eq(name, "ceil")) {
            if (c.args.len != 1) return self.err("`{s}` takes one argument", .{name});
            const a = try self.argType(c, 0);
            if (!numericish(a)) return self.err("`{s}` needs a numeric argument", .{name});
            // An int is already whole, so it passes through with its type.
            if (a.unknown or a.kind == .int) return a;
            return Type.init(.float).withNull(a.nullable);
        }
        if (eq(name, "round")) {
            if (c.args.len != 1 and c.args.len != 2) return self.err("`round` takes (x) or (x, digits)", .{});
            const a = try self.argType(c, 0);
            if (!numericish(a)) return self.err("`round` needs a numeric argument", .{});
            if (c.args.len == 2) {
                const d = try self.argType(c, 1);
                if (!numericish(d)) return self.err("`round` digits must be an integer", .{});
            }
            if (a.unknown or (a.kind == .int and c.args.len == 1)) return a;
            return Type.init(.float).withNull(a.nullable);
        }
        if (eq(name, "mod")) {
            if (c.args.len != 2) return self.err("`mod` takes (a, b)", .{});
            const a = try self.argType(c, 0);
            const b = try self.argType(c, 1);
            if (!(a.kind == .int or a.unknown) or !(b.kind == .int or b.unknown))
                return self.err("`mod` needs integer arguments", .{});
            // A zero divisor yields null, so the result is always nullable.
            return Type.init(.int).asNullable();
        }
        if (eq(name, "power")) {
            if (c.args.len != 2) return self.err("`power` takes (base, exponent)", .{});
            const a = try self.argType(c, 0);
            const b = try self.argType(c, 1);
            if (!numericish(a) or !numericish(b)) return self.err("`power` needs numeric arguments", .{});
            return Type.init(.float).withNull(a.nullable or b.nullable or a.unknown or b.unknown);
        }
        if (eq(name, "sqrt")) {
            if (c.args.len != 1) return self.err("`sqrt` takes one argument", .{});
            const a = try self.argType(c, 0);
            if (!numericish(a)) return self.err("`sqrt` needs a numeric argument", .{});
            // A negative operand is outside the domain and yields null.
            return Type.init(.float).asNullable();
        }
        if (eq(name, "sign")) {
            if (c.args.len != 1) return self.err("`sign` takes one argument", .{});
            const a = try self.argType(c, 0);
            if (!numericish(a)) return self.err("`sign` needs a numeric argument", .{});
            return Type.init(.int).withNull(a.nullable or a.unknown);
        }
        if (eq(name, "nullif")) {
            if (c.args.len != 2) return self.err("`nullif` takes (a, b)", .{});
            const a = try self.argType(c, 0);
            const b = try self.argType(c, 1);
            if (!comparable(a, b)) return self.err("`nullif` arguments are not comparable", .{});
            return a.asNullable();
        }
        if (eq(name, "greatest") or eq(name, "least")) {
            if (c.args.len < 2) return self.err("`{s}` needs at least two arguments", .{name});
            var result: ?Type = null;
            for (c.args) |a| {
                const t = try self.typeOf(a);
                result = if (result) |r|
                    (Type.unify(r, t) orelse return self.err("`{s}` arguments have incompatible types", .{name}))
                else
                    t;
            }
            // Null arguments are ignored (Postgres), so the result is null only
            // when every argument is — hence nullable regardless of the inputs.
            return result.?.asNullable();
        }
        return null;
    }

    /// Byte-wise string builtins. Like the existing `substr`/`replace` rules
    /// these do not insist the operand is already a string — `valueToString`
    /// renders whatever arrives — they only fix arity and the result type.
    fn typeOfStringCall(self: *TypeCtx, c: ast.Expr.Call) TypeError!?Type {
        const name = c.name;
        if (eq(name, "lpad") or eq(name, "rpad")) {
            if (c.args.len != 2 and c.args.len != 3) return self.err("`{s}` takes (string, length[, fill])", .{name});
            const a = try self.argType(c, 0);
            _ = try self.argType(c, 1);
            if (c.args.len > 2) _ = try self.argType(c, 2);
            return Type.init(.string).withNull(a.nullable);
        }
        if (eq(name, "left") or eq(name, "right")) {
            if (c.args.len != 2) return self.err("`{s}` takes (string, n)", .{name});
            const a = try self.argType(c, 0);
            _ = try self.argType(c, 1);
            return Type.init(.string).withNull(a.nullable);
        }
        if (eq(name, "split_part")) {
            if (c.args.len != 3) return self.err("`split_part` takes (string, delimiter, n)", .{});
            _ = try self.argType(c, 0);
            _ = try self.argType(c, 1);
            _ = try self.argType(c, 2);
            // An empty delimiter yields null, so this is nullable either way.
            return Type.init(.string).asNullable();
        }
        if (eq(name, "strpos")) {
            if (c.args.len != 2) return self.err("`strpos` takes (string, substring)", .{});
            const a = try self.argType(c, 0);
            const b = try self.argType(c, 1);
            return Type.init(.int).withNull(a.nullable or b.nullable);
        }
        if (eq(name, "repeat")) {
            if (c.args.len != 2) return self.err("`repeat` takes (string, n)", .{});
            const a = try self.argType(c, 0);
            _ = try self.argType(c, 1);
            return Type.init(.string).withNull(a.nullable);
        }
        if (eq(name, "reverse")) {
            if (c.args.len != 1) return self.err("`reverse` takes one argument", .{});
            const a = try self.argType(c, 0);
            return Type.init(.string).withNull(a.nullable);
        }
        return null;
    }

    fn typeOfDateCall(self: *TypeCtx, c: ast.Expr.Call) TypeError!?Type {
        const name = c.name;
        if (eq(name, "date_add")) {
            if (c.args.len != 3) return self.err("`date_add` takes (unit, n, timestamp)", .{});
            if (c.args[0].* != .str_lit) return self.err("`date_add` needs a literal unit", .{});
            const u = timeUnit(c.args[0].str_lit) orelse
                return self.err("unknown time unit `{s}`", .{c.args[0].str_lit});
            const nt = try self.argType(c, 1);
            if (!numericish(nt)) return self.err("`date_add` needs an integer amount", .{});
            const a = try self.argType(c, 2);
            if (a.unknown) return a;
            const nn = a.nullable or nt.nullable or nt.unknown;
            if (a.kind == .date) {
                // A DATE has no time of day, so sub-day units have nowhere to go.
                if (u == .hour or u == .minute or u == .second)
                    return self.err("`date_add` cannot add `{s}` to a date; cast it to a timestamp first", .{c.args[0].str_lit});
                return Type.init(.date).withNull(nn);
            }
            if (a.kind != .timestamp) return self.err("`date_add` needs a date or timestamp", .{});
            return Type.init(.timestamp).withNull(nn);
        }
        if (eq(name, "date_diff")) {
            if (c.args.len != 3) return self.err("`date_diff` takes (unit, start, end)", .{});
            if (c.args[0].* != .str_lit) return self.err("`date_diff` needs a literal unit", .{});
            if (timeUnit(c.args[0].str_lit) == null)
                return self.err("unknown time unit `{s}`", .{c.args[0].str_lit});
            const a = try self.argType(c, 1);
            const b = try self.argType(c, 2);
            if (!temporalish(a) or !temporalish(b))
                return self.err("`date_diff` needs date or timestamp arguments", .{});
            return Type.init(.int).withNull(a.nullable or b.nullable or a.unknown or b.unknown);
        }
        if (eq(name, "make_date")) {
            if (c.args.len != 3) return self.err("`make_date` takes (year, month, day)", .{});
            var nn = false;
            for (c.args) |a| {
                const t = try self.typeOf(a);
                if (!numericish(t)) return self.err("`make_date` needs integer arguments", .{});
                nn = nn or t.nullable or t.unknown;
            }
            return Type.init(.date).withNull(nn);
        }
        if (eq(name, "epoch")) {
            if (c.args.len != 1) return self.err("`epoch` takes one argument", .{});
            const a = try self.argType(c, 0);
            if (!temporalish(a)) return self.err("`epoch` needs a date or timestamp", .{});
            return Type.init(.int).withNull(a.nullable);
        }
        if (eq(name, "to_timestamp")) {
            if (c.args.len != 1) return self.err("`to_timestamp` takes one argument", .{});
            const a = try self.argType(c, 0);
            if (!numericish(a)) return self.err("`to_timestamp` needs a numeric argument", .{});
            return Type.init(.timestamp).withNull(a.nullable);
        }
        if (eq(name, "strftime")) {
            if (c.args.len != 2) return self.err("`strftime` takes (timestamp, format)", .{});
            const a = try self.argType(c, 0);
            if (!temporalish(a)) return self.err("`strftime` needs a date or timestamp", .{});
            const f = try self.argType(c, 1);
            // A literal format is validated here so an unsupported directive
            // fails `check` rather than partway through a run — the same
            // treatment `regexp_replace` gives a literal pattern.
            if (c.args[1].* == .str_lit) {
                if (badStrftime(c.args[1].str_lit)) |bad|
                    return self.err("`strftime` does not support `%{s}` (supported: %Y %m %d %H %M %S %y %%)", .{bad});
            }
            return Type.init(.string).withNull(a.nullable or f.nullable);
        }
        return null;
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
    col: struct { d: column.Bytes, v: Bitmap },
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
        .let_in => return error.Unsupported,
    }
}

/// Evaluate an argument to a string operand, or null → Unsupported fallback.
fn strArg(arena: std.mem.Allocator, e: *const ast.Expr, batch: Batch) VecError!Str {
    const v = try evalVec(arena, e, batch);
    return asStr(v) orelse error.Unsupported;
}

fn callVec(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
    const name = c.name;
    const n = batch.len;

    if (eq(name, "now")) return .{ .scalar = .{ .timestamp = std.time.microTimestamp() } };
    if (eq(name, "today")) return .{ .scalar = .{ .date = @intCast(@divFloor(std.time.microTimestamp(), 86_400_000_000)) } };

    if (eq(name, "upper") or eq(name, "lower")) {
        if (c.args.len < 1) return error.Unsupported;
        const s = try strArg(arena, c.args[0], batch);
        const up = eq(name, "upper");
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
            const o = try out.pushMutable(sv);
            for (o) |*ch| ch.* = if (up) std.ascii.toUpper(ch.*) else std.ascii.toLower(ch.*);
        }
        return mkCol(Type.init(.string).withNull(any), n, bm, .{ .bytes = try out.finish() });
    }

    if (eq(name, "trim")) {
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

    if (eq(name, "length") or eq(name, "strlen")) {
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

    if (eq(name, "coalesce")) {
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
            try out.push(try substrBytes(arena, sv.?, numI(start, i), if (len_num) |l| numI(l, i) else null));
        }
        return mkCol(Type.init(.string).withNull(any), n, bm, .{ .bytes = try out.finish() });
    }

    if (eq(name, "replace")) {
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
        // Handled rowwise, like the binary bitwise ops.
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

fn binaryVec(arena: std.mem.Allocator, b: ast.Expr.Binary, batch: Batch) VecError!Vec {
    switch (b.op) {
        .@"and", .@"or" => return boolOpVec(arena, b.op, b.l, b.r, batch),
        // No vectorized kernel yet — the rowwise evaluator owns bitwise ops.
        .bit_and, .bit_or, .bit_xor, .shl, .shr => return error.Unsupported,
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

fn castVec(arena: std.mem.Allocator, c: ast.Expr.Cast, batch: Batch) VecError!Vec {
    // `try_cast` needs per-row null-on-failure, but the vectorized cast fails
    // the whole column at the first bad value — hand it to the rowwise path.
    if (c.safe) return error.Unsupported;
    const v = try evalVec(arena, c.e, batch);
    const target = c.ty.kind;
    if (target == .decimal) return error.Unsupported;
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
            break :blk .{ .bytes = try mergeBytes(arena, &bm, take, ts, e.data.bytes, t.validity, e.validity) };
        },
    };
    return .{ .col = .{ .ty = t.ty.withNull(true), .len = n, .validity = bm, .data = data } };
}

/// `mergePick` for the Arrow bytes layout: the picked payloads have to be copied
/// into a fresh values buffer, so sizes are summed first and the buffer is
/// allocated exactly once.
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
            if (!c.safe) return castValueTyped(arena, v, c.ty);
            // `try_cast`: a failed conversion is a null, not an error.
            const out: Value = castValueTyped(arena, v, c.ty) catch |e| {
                if (e == error.CastFailed) return .null;
                return e;
            };
            return out;
        },
        .match => |m| return evalMatch(arena, m, batch, row),
        .call => |c| return evalCall(arena, c, batch, row),
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

/// INT-only bitwise ops. Null propagation happens in the caller.
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

/// Shift counts outside 0..63 are defined, never UB: they shift every bit out.
/// `<<` therefore yields 0, and the arithmetic `>>` yields 0 or -1 depending on
/// the sign bit — but only for an over-wide count; a negative count is 0 either
/// way (there is no implied direction flip).
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
    const name = c.name;
    if (eq(name, "now")) {
        return .{ .timestamp = std.time.microTimestamp() };
    }
    if (eq(name, "today")) {
        const days = @divFloor(std.time.microTimestamp(), 86_400_000_000);
        return .{ .date = @intCast(days) };
    }
    if (eq(name, "regexp_replace")) {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        const pat = try evalRow(arena, c.args[1], batch, row);
        const rep = try evalRow(arena, c.args[2], batch, row);
        if (pat.isNull() or rep.isNull()) return .null;
        var rbuf: [16 * 1024]u8 = undefined;
        var rfba = std.heap.FixedBufferAllocator.init(&rbuf);
        const out = regex.replaceFirst(
            arena,
            rfba.allocator(),
            try valueToString(arena, v),
            try valueToString(arena, pat),
            try valueToString(arena, rep),
        ) catch |e| switch (e) {
            error.OutOfMemory => return error.OutOfMemory,
            error.BadPattern => return error.CastFailed,
        };
        return .{ .string = out };
    }
    if (eq(name, "date_trunc") or eq(name, "extract")) {
        const v = try evalRow(arena, c.args[1], batch, row);
        if (v.isNull()) return .null;
        const us = temporalMicros(v) orelse return error.TypeMismatch;
        const u = timeUnit(c.args[0].str_lit) orelse return error.TypeMismatch;
        return if (eq(name, "extract"))
            Value{ .int = extractField(us, u) }
        else
            Value{ .timestamp = truncMicros(us, u) };
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
    if (eq(name, "length") or eq(name, "strlen")) {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        return .{ .int = @intCast((try valueToString(arena, v)).len) };
    }
    if (eq(name, "bit_count")) {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        if (v != .int) return error.TypeMismatch;
        return .{ .int = @intCast(@popCount(v.int)) };
    }
    if (eq(name, "to_hex")) {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        if (v != .int) return error.TypeMismatch;
        return .{ .string = try std.fmt.allocPrint(arena, "{x}", .{@as(u64, @bitCast(v.int))}) };
    }
    if (eq(name, "from_hex")) {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        return .{ .int = try parseHexI64(try valueToString(arena, v)) };
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
    if (try evalMathCall(arena, c, batch, row)) |v| return v;
    if (try evalStringCall(arena, c, batch, row)) |v| return v;
    if (try evalDateCall(arena, c, batch, row)) |v| return v;
    return error.TypeMismatch;
}

/// SQL null as a `Value`. The builtin helpers below return `?Value`, where a
/// bare `null` means "not my function, keep dispatching" — this names the other
/// null so the two can't be confused at a glance.
const sql_null: Value = .null;

/// 1 MiB ceiling on a single generated string (`repeat`, `lpad`/`rpad`), so
/// `repeat(x, 1000000000)` is a clean error instead of an OOM or a stall.
const max_str_bytes = 1 << 20;

/// Numeric and null-selecting builtins. Null in → null out throughout; a
/// non-match returns null so `evalCall` can keep dispatching.
fn evalMathCall(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!?Value {
    const name = c.name;
    if (eq(name, "abs")) {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return sql_null;
        switch (v) {
            // `-minInt(i64)` has no i64 representation; refuse rather than wrap.
            .int => |x| {
                if (x == std.math.minInt(i64)) return error.CastFailed;
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
    if (eq(name, "floor") or eq(name, "ceil")) {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return sql_null;
        if (v == .int) return v;
        if (!isNum(v)) return error.TypeMismatch;
        const x = toF64(v);
        return Value{ .float = if (eq(name, "floor")) @floor(x) else @ceil(x) };
    }
    if (eq(name, "round")) {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return sql_null;
        if (!isNum(v)) return error.TypeMismatch;
        var digits: i64 = 0;
        if (c.args.len > 1) {
            const dv = try evalRow(arena, c.args[1], batch, row);
            if (dv.isNull()) return sql_null;
            digits = toI64(dv);
        }
        if (v == .int and c.args.len == 1) return v;
        return Value{ .float = roundHalfAway(toF64(v), digits) };
    }
    if (eq(name, "mod")) {
        const a = try evalRow(arena, c.args[0], batch, row);
        const b = try evalRow(arena, c.args[1], batch, row);
        if (a.isNull() or b.isNull()) return sql_null;
        const d = toI64(b);
        // `mod` is the guarded spelling of `%`: a zero divisor is null here,
        // where the operator raises DivByZero. Never a crash either way.
        if (d == 0) return sql_null;
        // `@rem(minInt, -1)` overflows; the answer is 0 by definition.
        if (d == -1) return Value{ .int = 0 };
        // @rem (not @mod) so the result takes the sign of the dividend, as SQL wants.
        return Value{ .int = @rem(toI64(a), d) };
    }
    if (eq(name, "power")) {
        const a = try evalRow(arena, c.args[0], batch, row);
        const b = try evalRow(arena, c.args[1], batch, row);
        if (a.isNull() or b.isNull()) return sql_null;
        return Value{ .float = std.math.pow(f64, toF64(a), toF64(b)) };
    }
    if (eq(name, "sqrt")) {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return sql_null;
        const x = toF64(v);
        if (x < 0) return sql_null;
        return Value{ .float = @sqrt(x) };
    }
    if (eq(name, "sign")) {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return sql_null;
        const x = toF64(v);
        return Value{ .int = if (x > 0) @as(i64, 1) else if (x < 0) @as(i64, -1) else @as(i64, 0) };
    }
    if (eq(name, "nullif")) {
        const a = try evalRow(arena, c.args[0], batch, row);
        if (a.isNull()) return sql_null;
        const b = try evalRow(arena, c.args[1], batch, row);
        // `a = b` is unknown against a null `b`, so `a` comes back (Postgres).
        if (b.isNull()) return a;
        if (compareValues(a, b)) |ord| {
            if (ord == .eq) return sql_null;
        }
        return a;
    }
    if (eq(name, "greatest") or eq(name, "least")) {
        // Postgres semantics: null arguments are IGNORED (unlike the
        // null-propagating arithmetic above); all-null yields null.
        const want_gt = eq(name, "greatest");
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
    return null;
}

fn evalStringCall(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!?Value {
    const name = c.name;
    if (eq(name, "lpad") or eq(name, "rpad")) {
        const sv = try evalRow(arena, c.args[0], batch, row);
        if (sv.isNull()) return sql_null;
        const nv = try evalRow(arena, c.args[1], batch, row);
        if (nv.isNull()) return sql_null;
        var fill: []const u8 = " ";
        if (c.args.len > 2) {
            const fv = try evalRow(arena, c.args[2], batch, row);
            if (fv.isNull()) return sql_null;
            fill = try valueToString(arena, fv);
        }
        const s = try valueToString(arena, sv);
        return Value{ .string = try padBytes(arena, s, toI64(nv), fill, eq(name, "lpad")) };
    }
    if (eq(name, "left") or eq(name, "right")) {
        const sv = try evalRow(arena, c.args[0], batch, row);
        if (sv.isNull()) return sql_null;
        const nv = try evalRow(arena, c.args[1], batch, row);
        if (nv.isNull()) return sql_null;
        const s = try valueToString(arena, sv);
        return Value{ .string = try arena.dupe(u8, endSlice(s, toI64(nv), eq(name, "left"))) };
    }
    if (eq(name, "split_part")) {
        const sv = try evalRow(arena, c.args[0], batch, row);
        const dv = try evalRow(arena, c.args[1], batch, row);
        const nv = try evalRow(arena, c.args[2], batch, row);
        if (sv.isNull() or dv.isNull() or nv.isNull()) return sql_null;
        const delim = try valueToString(arena, dv);
        // An empty delimiter splits into nothing meaningful — null, not a guess.
        if (delim.len == 0) return sql_null;
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
    if (eq(name, "strpos")) {
        const sv = try evalRow(arena, c.args[0], batch, row);
        const pv = try evalRow(arena, c.args[1], batch, row);
        if (sv.isNull() or pv.isNull()) return sql_null;
        const s = try valueToString(arena, sv);
        const sub = try valueToString(arena, pv);
        if (sub.len == 0) return Value{ .int = 1 };
        const at = std.mem.indexOf(u8, s, sub) orelse return Value{ .int = 0 };
        return Value{ .int = @as(i64, @intCast(at)) + 1 };
    }
    if (eq(name, "repeat")) {
        const sv = try evalRow(arena, c.args[0], batch, row);
        if (sv.isNull()) return sql_null;
        const nv = try evalRow(arena, c.args[1], batch, row);
        if (nv.isNull()) return sql_null;
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
    if (eq(name, "reverse")) {
        const sv = try evalRow(arena, c.args[0], batch, row);
        if (sv.isNull()) return sql_null;
        const s = try valueToString(arena, sv);
        const out = try arena.alloc(u8, s.len);
        for (s, 0..) |ch, i| out[s.len - 1 - i] = ch;
        return Value{ .string = out };
    }
    return null;
}

fn evalDateCall(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!?Value {
    const name = c.name;
    if (eq(name, "date_add") or eq(name, "date_diff")) {
        if (c.args[0].* != .str_lit) return error.TypeMismatch;
        const u = timeUnit(c.args[0].str_lit) orelse return error.TypeMismatch;
        const a = try evalRow(arena, c.args[1], batch, row);
        const b = try evalRow(arena, c.args[2], batch, row);
        if (a.isNull() or b.isNull()) return sql_null;
        if (eq(name, "date_add")) return try addUnits(b, u, toI64(a));
        const a_us = temporalMicros(a) orelse return error.TypeMismatch;
        const b_us = temporalMicros(b) orelse return error.TypeMismatch;
        return Value{ .int = dateDiff(a_us, b_us, u) };
    }
    if (eq(name, "make_date")) {
        const yv = try evalRow(arena, c.args[0], batch, row);
        const mv = try evalRow(arena, c.args[1], batch, row);
        const dv = try evalRow(arena, c.args[2], batch, row);
        if (yv.isNull() or mv.isNull() or dv.isNull()) return sql_null;
        const y = toI64(yv);
        const m = toI64(mv);
        const d = toI64(dv);
        // Fail loud on an impossible date, matching CAST's posture. Wrap the
        // call in `try_cast`-style validity checks upstream if null is wanted.
        if (m < 1 or m > 12) return error.CastFailed;
        if (d < 1 or d > daysInMonth(y, @intCast(m))) return error.CastFailed;
        const days = daysFromCivil(y, @intCast(m), @intCast(d));
        return Value{ .date = std.math.cast(i32, days) orelse return error.CastFailed };
    }
    if (eq(name, "epoch")) {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return sql_null;
        const us = temporalMicros(v) orelse return error.TypeMismatch;
        return Value{ .int = @divFloor(us, 1_000_000) };
    }
    if (eq(name, "to_timestamp")) {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return sql_null;
        return Value{ .timestamp = try mulI64(toI64(v), 1_000_000) };
    }
    if (eq(name, "strftime")) {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return sql_null;
        const fv = try evalRow(arena, c.args[1], batch, row);
        if (fv.isNull()) return sql_null;
        const us = temporalMicros(v) orelse return error.TypeMismatch;
        return Value{ .string = try strftimeFmt(arena, us, try valueToString(arena, fv)) };
    }
    return null;
}

/// Cast honouring the target's scale. `castValue` only sees a `TypeKind`, which
/// is enough for every type except `DECIMAL(p, s)` — where dropping the scale
/// left the conversion undefined, so it failed outright.
pub fn castValueTyped(arena: std.mem.Allocator, v: Value, ty: types.Type) EvalError!Value {
    if (ty.kind != .decimal) return castValue(arena, v, ty.kind);
    const d: valuemod.Decimal = switch (v) {
        .decimal => |x| x,
        .int => |x| .{ .unscaled = x, .scale = 0 },
        .bool => |x| .{ .unscaled = if (x) 1 else 0, .scale = 0 },
        .float => |x| blk: {
            const p = powTen(ty.scale);
            const scaled = x * @as(f64, @floatFromInt(p));
            if (!std.math.isFinite(scaled)) return error.CastFailed;
            break :blk .{ .unscaled = @intFromFloat(@round(scaled)), .scale = ty.scale };
        },
        .string, .bytes => |str| switch (sqlmod.parseDecimalText(trim(str))) {
            .decimal => |x| x,
            .int => |x| valuemod.Decimal{ .unscaled = x, .scale = 0 },
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

/// Shift a decimal to `want`, truncating toward zero when it loses digits —
/// the same rule the parquet writer applies.
fn rescaleTo(d: valuemod.Decimal, want: u8) ?valuemod.Decimal {
    var unscaled = d.unscaled;
    var have: i32 = d.scale;
    while (have < want) : (have += 1) {
        unscaled = std.math.mul(i128, unscaled, 10) catch return null;
    }
    while (have > want) : (have -= 1) unscaled = @divTrunc(unscaled, 10);
    return .{ .unscaled = unscaled, .scale = want };
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
    const us: u64 = @intCast(micros - days * 86_400_000_000);
    const secs = us / 1_000_000;
    const c = civilFromDays(days);
    return std.fmt.allocPrint(arena, "{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}", .{
        @as(u32, @intCast(c.y)), c.m, c.d, secs / 3600, (secs % 3600) / 60, secs % 60,
    });
}

/// Field selector shared by `extract` and `date_trunc`.
pub const TimeUnit = enum { year, month, day, hour, minute, second };

pub fn timeUnit(name: []const u8) ?TimeUnit {
    var buf: [16]u8 = undefined;
    if (name.len == 0 or name.len >= buf.len) return null;
    return std.meta.stringToEnum(TimeUnit, std.ascii.lowerString(buf[0..name.len], name));
}

/// Microseconds since the epoch for a temporal value, so both `date` and
/// `timestamp` feed the same field arithmetic.
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
        .day => @intCast(c.d),
        .hour => @divFloor(rem, 3_600_000_000),
        .minute => @mod(@divFloor(rem, 60_000_000), 60),
        .second => @mod(@divFloor(rem, 1_000_000), 60),
    };
}

/// Checked i64 arithmetic for the date builtins — an absurd `n` in
/// `date_add`/`to_timestamp` becomes a clean error instead of a wrap panic.
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

/// Add `n` CALENDAR months to a day count, clamping the day-of-month to the
/// target month's length: 2024-01-31 plus one month is 2024-02-29, and plus
/// one more is 2024-03-29 (the clamp is not remembered, matching Postgres).
fn addMonthsToDays(days: i64, n: i64) i64 {
    const c = civilFromDays(days);
    const total = c.y * 12 + @as(i64, c.m) - 1 + n;
    const y = @divFloor(total, 12);
    const m: u32 = @intCast(@mod(total, 12) + 1);
    return daysFromCivil(y, m, @min(c.d, daysInMonth(y, m)));
}

/// `date_add(unit, n, ts)` for one temporal value. A DATE only accepts day and
/// coarser units (the type-checker rejects the rest); a TIMESTAMP accepts all
/// six, and month/year steps preserve the time of day.
fn addUnits(v: Value, u: TimeUnit, n: i64) EvalError!Value {
    switch (v) {
        .date => |d0| {
            const days: i64 = d0;
            const nd = switch (u) {
                .year => addMonthsToDays(days, try mulI64(n, 12)),
                .month => addMonthsToDays(days, n),
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

/// `date_diff(unit, a, b)` with DuckDB's semantics, chosen because it is the
/// one definition that does not depend on the time of day for calendar units:
/// `year`/`month` count the unit BOUNDARIES crossed, computed from the civil
/// components — so 2023-12-31 → 2024-01-01 is one year, and one month — while
/// `day` and finer are the exact elapsed difference divided by the unit and
/// truncated toward zero. (Postgres' `age`-style "complete units" would make
/// that same pair zero years; we deliberately do not use it.)
fn dateDiff(a_us: i64, b_us: i64, u: TimeUnit) i64 {
    switch (u) {
        .year, .month => {
            const ca = civilFromDays(@divFloor(a_us, 86_400_000_000));
            const cb = civilFromDays(@divFloor(b_us, 86_400_000_000));
            if (u == .year) return cb.y - ca.y;
            return (cb.y * 12 + @as(i64, cb.m)) - (ca.y * 12 + @as(i64, ca.m));
        },
        .day => return @divTrunc(b_us - a_us, 86_400_000_000),
        .hour => return @divTrunc(b_us - a_us, 3_600_000_000),
        .minute => return @divTrunc(b_us - a_us, 60_000_000),
        .second => return @divTrunc(b_us - a_us, 1_000_000),
    }
}

/// The first unsupported `%` directive in `fmt` (as a one-byte slice), or null
/// when every directive is one `strftimeFmt` understands. Used at check time on
/// a literal format so a typo fails the plan, not the run.
fn badStrftime(fmt: []const u8) ?[]const u8 {
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

/// `strftime` over exactly `%Y %m %d %H %M %S %y %%`. Any other directive is an
/// error, never a silent passthrough — a literal format is already rejected at
/// check time, so this only fires for a format computed at run time.
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

/// Day count since the 1970 epoch for a civil date: the inverse of
/// `civilFromDays` (Howard Hinnant's algorithm).
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

/// Parse `YYYY-MM-DD` into a day count. Strict on purpose: a literal that is
/// not an ISO date must fail rather than coerce, so `date_col = '01/07/2013'`
/// is an error instead of a silently wrong comparison.
pub fn parseIsoDate(s0: []const u8) ?i64 {
    const s = trim(s0);
    if (s.len != 10 or s[4] != '-' or s[7] != '-') return null;
    const y = isoNum(s[0..4]) orelse return null;
    const m = isoNum(s[5..7]) orelse return null;
    const d = isoNum(s[8..10]) orelse return null;
    if (m < 1 or m > 12 or d < 1 or d > 31) return null;
    return daysFromCivil(y, @intCast(m), @intCast(d));
}

/// Parse `YYYY-MM-DD[ HH:MM:SS]` into microseconds since the epoch.
pub fn parseIsoTimestamp(s0: []const u8) ?i64 {
    const s = trim(s0);
    if (s.len == 10) return (parseIsoDate(s) orelse return null) * 86_400_000_000;
    if (s.len < 19 or s[13] != ':' or s[16] != ':') return null;
    const days = parseIsoDate(s[0..10]) orelse return null;
    const hh = isoNum(s[11..13]) orelse return null;
    const mm = isoNum(s[14..16]) orelse return null;
    const ss = isoNum(s[17..19]) orelse return null;
    if (hh > 23 or mm > 59 or ss > 59) return null;
    return days * 86_400_000_000 + (hh * 3600 + mm * 60 + ss) * 1_000_000;
}

/// Civil (Gregorian) date from a day count since the 1970 epoch (Howard Hinnant's
/// algorithm). Shared by the text-sink serializer and the SQL INSERT serializer.
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
    while (n <= scale) : (n += 1) digits[n] = '0';

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

/// `from_hex`: optional `0x` prefix, either case, and 16 digits' worth of range
/// (so `to_hex` of a negative round-trips). Fail-loud on junk or overflow —
/// nullability is the caller's to compose, e.g. `if(like(s, '%'), …)`.
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

/// `round` rounds HALF AWAY FROM ZERO (`@round`) — 2.5 → 3, -2.5 → -3 — which
/// is the SQL-standard / Postgres / SQL Server rule. It is deliberately NOT the
/// banker's rounding some engines (DuckDB, and IEEE `rint`) use for floats.
/// Because engines disagree, `round` is kept OUT of the pushdown whitelist in
/// `runtime/pushdown.zig`: the engine must compute it locally so the answer
/// cannot change depending on where the query happened to run.
fn roundHalfAway(x: f64, digits: i64) f64 {
    if (digits == 0) return @round(x);
    // 10^22 is the last power of ten f64 holds exactly; past it the scaling
    // step is meaningless anyway, so clamp rather than drift.
    const s = pow10f(@intCast(@min(@abs(digits), 22)));
    return if (digits > 0) @round(x * s) / s else @round(x / s) * s;
}

/// Postgres `lpad`/`rpad`: pad `s` with repetitions of `fill` out to exactly
/// `n` bytes, TRUNCATING to the first `n` bytes when `s` is already longer. An
/// empty `fill` cannot pad, so a short `s` comes back unchanged.
fn padBytes(arena: std.mem.Allocator, s: []const u8, n: i64, fill: []const u8, left: bool) ![]const u8 {
    if (n <= 0) return "";
    const want: usize = @intCast(n);
    if (want > max_str_bytes) return error.CastFailed;
    if (s.len >= want) return arena.dupe(u8, s[0..want]);
    if (fill.len == 0) return arena.dupe(u8, s);
    const out = try arena.alloc(u8, want);
    const pad = want - s.len;
    var i: usize = 0;
    while (i < pad) : (i += 1) out[if (left) i else s.len + i] = fill[i % fill.len];
    @memcpy(if (left) out[pad..] else out[0..s.len], s);
    return out;
}

/// Postgres `left`/`right`: a NEGATIVE `n` means "all but the last/first |n|
/// bytes" rather than clamping to empty, so `left(s, -2)` drops the last two.
fn endSlice(s: []const u8, n: i64, left: bool) []const u8 {
    const slen: i64 = @intCast(s.len);
    var take: i64 = if (n >= 0) n else slen + n;
    if (take < 0) take = 0;
    if (take > slen) take = slen;
    const k: usize = @intCast(take);
    return if (left) s[0..k] else s[s.len - k ..];
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

test "substr (1-based, byte) and like wildcard matcher" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try std.testing.expectEqualStrings("01", try substrBytes(a, "SD1010", 4, 2));
    try std.testing.expectEqualStrings("SD1", try substrBytes(a, "SD1010", 1, 3));
    try std.testing.expectEqualStrings("010", try substrBytes(a, "SD1010", 4, null));
    try std.testing.expectEqualStrings("", try substrBytes(a, "SD1010", 99, 2));

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

    // Row 0 has x = -8. Every case is INT-typed and evaluated on the rowwise
    // path (the vectorizer has no bitwise kernel).
    const ints = [_]struct { src: []const u8, want: i64 }{
        .{ .src = "1 | 2 & 3", .want = 3 },
        .{ .src = "6 & 3", .want = 2 },
        .{ .src = "6 ^ 3", .want = 5 },
        .{ .src = "1 + 1 << 2", .want = 8 },
        .{ .src = "~0", .want = -1 },
        .{ .src = "~x", .want = 7 },
        .{ .src = "x >> 1", .want = -4 },
        .{ .src = "1 << 63 >> 63", .want = -1 },
        // shift edges: never UB, never a panic
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

    // Row 1 has x = null: it propagates through every new op.
    const nulls = [_][]const u8{ "x & 1", "x | 1", "x ^ 1", "x << 1", "x >> 1", "~x", "bit_count(x)", "to_hex(x)", "from_hex(to_hex(x))" };
    for (nulls) |src| {
        const e, const t = try S.checked(a, schema, src);
        try std.testing.expect(t.nullable);
        try std.testing.expect((try evalColumn(a, e, batch, t)).getValue(1).isNull());
        try std.testing.expect((try evalRow(a, e, batch, 1)).isNull());
    }

    // Bitwise ops de-vectorize on purpose, so the rowwise path always runs.
    {
        const pair = try S.checked(a, schema, "x & 1");
        try std.testing.expectError(error.Unsupported, evalVec(a, pair[0], batch));
    }

    // `from_hex` is fail-loud: junk and overflow raise instead of nulling.
    for ([_][]const u8{ "from_hex('zz')", "from_hex('')", "from_hex('0x')", "from_hex('1ffffffffffffffff')" }) |src| {
        const e, const t = try S.checked(a, schema, src);
        try std.testing.expectError(error.CastFailed, evalRow(a, e, batch, 0));
        try std.testing.expectError(error.CastFailed, evalColumn(a, e, batch, t));
    }

    // Anything but INT is a check-time type error.
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

/// Parse and fold a constant expression — the shortest path to a builtin's
/// row-wise semantics, since `constEval` runs the same `evalRow` the batch
/// evaluator falls back to.
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

    // An int is already whole: floor/ceil/round hand it straight back.
    try std.testing.expectEqual(@as(i64, 5), (try evalLit(a, "floor(5)")).int);
    try std.testing.expectEqual(@as(i64, 5), (try evalLit(a, "round(5)")).int);
    try std.testing.expectEqual(@as(f64, 2.0), (try evalLit(a, "floor(2.7)")).float);
    try std.testing.expectEqual(@as(f64, 3.0), (try evalLit(a, "ceil(2.1)")).float);

    // Half away from zero, both signs — not banker's rounding.
    try std.testing.expectEqual(@as(f64, 3.0), (try evalLit(a, "round(2.5)")).float);
    try std.testing.expectEqual(@as(f64, -3.0), (try evalLit(a, "round(-2.5)")).float);
    try std.testing.expectEqual(@as(f64, 2.13), (try evalLit(a, "round(2.125, 2)")).float);

    try std.testing.expectEqual(@as(i64, 1), (try evalLit(a, "mod(7, 3)")).int);
    // @rem semantics: the remainder takes the sign of the dividend.
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
    // `3 = null` is unknown, not true, so `a` survives.
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

    // A safe cast is nullable even over a non-nullable input, and the column
    // path must agree with it (the vectorized cast bails out to rowwise).
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
    // n shorter than the input truncates rather than padding (Postgres).
    try std.testing.expectEqualStrings("abc", (try evalLit(a, "lpad('abcdef', 3)")).string);
    try std.testing.expectEqualStrings("abc", (try evalLit(a, "rpad('abcdef', 3)")).string);

    try std.testing.expectEqualStrings("ab", (try evalLit(a, "left('abcde', 2)")).string);
    try std.testing.expectEqualStrings("de", (try evalLit(a, "right('abcde', 2)")).string);
    // A negative n drops that many from the OTHER end.
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
    // Past the 1 MiB ceiling this is an error, never an unbounded allocation.
    try std.testing.expectError(error.CastFailed, evalLit(a, "repeat('ab', 1000000)"));

    try std.testing.expectEqualStrings("cba", (try evalLit(a, "reverse('abc')")).string);
    try std.testing.expect((try evalLit(a, "reverse(null)")).isNull());
}

test "date builtins: month clamp, boundary diffs, epoch round trip, strftime padding" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    // Jan 31 + 1 month clamps to the end of February, leap year or not.
    try std.testing.expectEqualStrings("2024-02-29", try formatDate(a, (try evalLit(a, "date_add('month', 1, cast('2024-01-31' as date))")).date));
    try std.testing.expectEqualStrings("2023-02-28", try formatDate(a, (try evalLit(a, "date_add('month', 1, cast('2023-01-31' as date))")).date));
    try std.testing.expectEqualStrings("2023-12-31", try formatDate(a, (try evalLit(a, "date_add('day', -1, cast('2024-01-01' as date))")).date));
    try std.testing.expectEqualStrings("2025-03-15", try formatDate(a, (try evalLit(a, "date_add('year', 1, cast('2024-03-15' as date))")).date));
    // On a timestamp a month step keeps the time of day.
    try std.testing.expectEqualStrings("2024-02-29 06:30:00", try formatTimestamp(a, (try evalLit(a, "date_add('month', 1, cast('2024-01-31 06:30:00' as timestamp))")).timestamp));

    // Boundary crossings for year/month: one day apart, but a year apart.
    try std.testing.expectEqual(@as(i64, 1), (try evalLit(a, "date_diff('year', cast('2023-12-31' as date), cast('2024-01-01' as date))")).int);
    try std.testing.expectEqual(@as(i64, 1), (try evalLit(a, "date_diff('month', cast('2023-12-31' as date), cast('2024-01-01' as date))")).int);
    // Exact division for day and finer.
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

    // A good one still type-checks, and picks up its operand's kind.
    ctx.msg = "";
    const ok = try parser.parseExprStr(a, "date_add('day', 7, d)", &diag);
    try std.testing.expectEqual(types.TypeKind.date, (try ctx.typeOf(ok)).kind);
}
