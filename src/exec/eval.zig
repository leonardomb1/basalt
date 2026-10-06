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
//!
//! The evaluator's parts are in eval/: `vec.zig` and `row.zig` are the two
//! evaluators, `functions.zig` the built-in table with each function's typing,
//! row and vector forms in `fn_typing.zig`, `fn_row.zig` and `fn_vec.zig`;
//! `cast.zig`, `format.zig`, `time.zig` and `strings.zig` the value helpers, and
//! `support.zig` the shared caches, failure notes and value ordering. This file
//! keeps the types and `TypeCtx`, and re-exports the public API. Each part carries
//! the tests of its own code; `testing_util.zig` holds the helpers they share.

const std = @import("std");
const regex = @import("regex.zig");
const sql = @import("../db/sql.zig");
const ast = @import("../lang/ast.zig");
const types = @import("../lang/types.zig");
const column = @import("column.zig");
const Decimal = @import("value.zig").Decimal;
pub const Value = @import("value.zig").Value;
const json = @import("json.zig");
const pow10f = @import("value.zig").pow10f;
pub const Batch = @import("batch.zig").Batch;

const Type = types.Type;

pub const TypeError = error{ TypeError, OutOfMemory };
pub const EvalError = error{ CastFailed, DivByZero, TypeMismatch, IntOverflow, PatternTooComplex, InvalidJson, OutOfMemory };
const evalLit = @import("eval/testing_util.zig").evalLit;

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

    pub fn argType(self: *TypeCtx, c: ast.Expr.Call, i: usize) TypeError!Type {
        if (i >= c.args.len) return self.err("`{s}` is missing an argument", .{c.name});
        return self.typeOf(c.args[i]);
    }

    pub fn err(self: *TypeCtx, comptime fmt: []const u8, args: anytype) TypeError {
        self.msg = std.fmt.allocPrint(self.arena, fmt, args) catch "out of memory";
        return error.TypeError;
    }

    /// A text parameter: strings, bytes and every scalar the runtime renders as text,
    /// since `concat(id, '-', name)` is too common to refuse. Only nested values fail.
    pub fn wantText(self: *TypeCtx, c: ast.Expr.Call, i: usize) TypeError!Type {
        const t = try self.argType(c, i);
        if (t.kind == .array or t.kind == .@"struct")
            return self.err("`{s}` argument {d} must be text, got {s}", .{ c.name, i + 1, @tagName(t.kind) });
        return t;
    }

    pub fn wantInt(self: *TypeCtx, c: ast.Expr.Call, i: usize, what: []const u8) TypeError!Type {
        const t = try self.argType(c, i);
        if (!intish(t)) return self.err("`{s}` {s} must be an INT, got {s}", .{ c.name, what, @tagName(t.kind) });
        return t;
    }
};

pub fn numericish(t: Type) bool {
    return t.kind.isNumeric() or t.unknown;
}
pub fn boolish(t: Type) bool {
    return t.kind == .bool or t.unknown;
}
pub fn temporalish(t: Type) bool {
    return t.kind == .date or t.kind == .timestamp or t.unknown;
}
pub fn intish(t: Type) bool {
    return t.kind == .int or t.unknown;
}
pub fn comparable(a: Type, b: Type) bool {
    if (a.unknown or b.unknown) return true;
    if (a.kind.isNumeric() and b.kind.isNumeric()) return true;
    return a.kind == b.kind;
}

pub const evalColumn = @import("eval/vec.zig").evalColumn;
const evalColumnRowwise = @import("eval/vec.zig").evalColumnRowwise;
const evalVec = @import("eval/vec.zig").evalVec;
const evalVecNode = @import("eval/vec.zig").evalVecNode;
pub const constEval = @import("eval/vec.zig").constEval;
pub const evalRow = @import("eval/row.zig").evalRow;
const arith = @import("eval/row.zig").arith;
const decimalArithType = @import("eval/row.zig").decimalArithType;
pub const parseJson = @import("eval/row.zig").parseJson;
pub const jsonPath = @import("eval/row.zig").jsonPath;
pub const jsonToValue = @import("eval/row.zig").jsonToValue;
pub const bindLambda = @import("eval/row.zig").bindLambda;
pub const Builtin = @import("eval/functions.zig").Builtin;
pub const builtins = @import("eval/functions.zig").builtins;
pub const lookupBuiltin = @import("eval/functions.zig").lookupBuiltin;
const vectorized = @import("eval/fn_vec.zig").vectorized;
pub const castValueTyped = @import("eval/cast.zig").castValueTyped;
pub const floatToDecimal = @import("eval/cast.zig").floatToDecimal;
pub const roundScaleDown = @import("eval/cast.zig").roundScaleDown;
pub const rescaleTo = @import("eval/cast.zig").rescaleTo;
pub const castValue = @import("eval/cast.zig").castValue;
pub const writeValue = @import("eval/format.zig").writeValue;
pub const writeDate = @import("eval/format.zig").writeDate;
pub const writeTime = @import("eval/format.zig").writeTime;
pub const writeTimestamp = @import("eval/format.zig").writeTimestamp;
pub const writeDecimal = @import("eval/format.zig").writeDecimal;
pub const valueToString = @import("eval/format.zig").valueToString;
pub const formatDate = @import("eval/format.zig").formatDate;
pub const formatTime = @import("eval/format.zig").formatTime;
pub const formatTimestamp = @import("eval/format.zig").formatTimestamp;
pub const TimeUnit = @import("eval/time.zig").TimeUnit;
pub const timeUnit = @import("eval/time.zig").timeUnit;
pub const badStrftime = @import("eval/time.zig").badStrftime;
pub const daysFromCivil = @import("eval/time.zig").daysFromCivil;
pub const parseIsoDate = @import("eval/time.zig").parseIsoDate;
pub const parseIsoTime = @import("eval/time.zig").parseIsoTime;
pub const parseIsoTimestamp = @import("eval/time.zig").parseIsoTimestamp;
pub const civilFromDays = @import("eval/time.zig").civilFromDays;
pub const formatDecimal = @import("eval/support.zig").formatDecimal;
const fieldIndex = @import("eval/support.zig").fieldIndex;
const lastPart = @import("eval/support.zig").lastPart;
pub const toF64 = @import("eval/support.zig").toF64;
pub const explain = @import("eval/support.zig").explain;
pub const takeFailure = @import("eval/support.zig").takeFailure;
const cachedRegex = @import("eval/support.zig").cachedRegex;
pub const orderF64 = @import("eval/support.zig").orderF64;
pub const compareValues = @import("eval/support.zig").compareValues;
const charCount = @import("eval/strings.zig").charCount;
const substrChars = @import("eval/strings.zig").substrChars;
const padChars = @import("eval/strings.zig").padChars;
const endSlice = @import("eval/strings.zig").endSlice;
const reverseChars = @import("eval/strings.zig").reverseChars;
const caseMapInto = @import("eval/strings.zig").caseMapInto;
const likeMatch = @import("eval/strings.zig").likeMatch;

const parser = @import("../lang/sql_parser.zig");

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

test "check-time errors: regexp group past the pattern's count, malformed pattern" {
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

test {
    _ = @import("eval/cast.zig");
    _ = @import("eval/fn_row.zig");
    _ = @import("eval/fn_typing.zig");
    _ = @import("eval/fn_vec.zig");
    _ = @import("eval/format.zig");
    _ = @import("eval/functions.zig");
    _ = @import("eval/row.zig");
    _ = @import("eval/strings.zig");
    _ = @import("eval/support.zig");
    _ = @import("eval/time.zig");
    _ = @import("eval/vec.zig");
    _ = @import("eval/testing_util.zig");
}
