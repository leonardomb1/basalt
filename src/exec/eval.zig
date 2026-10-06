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
//! keeps the types, `TypeCtx` and the tests, and re-exports the public API.

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
    var buf: [128]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try writeDate(&w, -1_000_000);
    try w.writeByte(' ');
    try writeTimestamp(&w, -1_000_000 * 86_400_000_000 + 1);
    try w.writeByte('|');
    try writeDate(&w, -719_529);
    try w.writeByte('|');
    try writeDate(&w, -719_530);
    try std.testing.expectEqualStrings("-0768-02-05 -0768-02-05 00:00:00.000001|0000-01-01|-0001-12-31", w.buffered());
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

test "UTF-8 string helpers: substr, like, reverse, pad, case-map" {
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

    var caps: regex.Captures = undefined;
    var pat = "(a)-(b)".*;
    var re = try cachedRegex(&pat);
    try std.testing.expectEqual(@as(?[2]usize, .{ 0, 3 }), try re.find("a-b", 0, &caps));
    pat[1] = 'b';
    pat[5] = 'a';
    re = try cachedRegex(&pat);
    try std.testing.expectEqual(@as(?[2]usize, null), try re.find("a-b", 0, &caps));
    try std.testing.expectEqual(@as(?[2]usize, .{ 0, 3 }), try re.find("b-a", 0, &caps));

    try std.testing.expectError(error.BadPattern, cachedRegex("("));
    re = try cachedRegex("(a)-(b)");
    try std.testing.expectEqual(@as(?[2]usize, .{ 0, 3 }), try re.find("a-b", 0, &caps));
    try std.testing.expectEqual(@as(?[2]usize, .{ 2, 3 }), caps[2]);
    try std.testing.expectEqual(@as(?[2]usize, null), try re.find("b-a", 0, &caps));
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
