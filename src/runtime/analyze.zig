//! Static analysis: parsed program to validated `Plan` IR, without executing or
//! connecting. Shared groundwork for `EXPLAIN` (render the IR) and `basalt check`
//! (validate and report). It does full structural and reference validation and
//! resolves what it can read locally, a CSV header (split on the script's
//! delimiter), a Parquet footer, a workbook's first pass. A schema only the source
//! can describe (a database table, a remote object) stays null, renders as
//! `schema: unresolved` once at its scan, and nothing past it is typed; so does a
//! name computed per row (`IDENTIFIER(...)`). A `$name` that nothing binds is still
//! refused there, whatever the columns turn out to be.
//!
//! Analysis mirrors the runtime so `check` refuses exactly what a run would and
//! `EXPLAIN` prints the plan that runs: the same param substitution into filters
//! (a `$param` reaches pushdown as its value, never as a column), the same binding
//! inlining and pushdown rewrite, the same `joinPlan`, the same `THROW` guards and
//! body-statement rule, and the same physical labels. `splittable` is the SQL
//! key-range fan-out (map-only, an aggregate with a sort/limit tail, map+join);
//! `morsel_parallel` is a file read cut into byte ranges or row groups. Both were
//! once mislabelled `serial` while the run fanned out, so the predicates here are
//! the ones the runtime asks.
//!
//! Binding order: PARAMs (with `-p` overrides typed as the run binds them), then
//! statement-level LETs in declaration order as expressions; a query LET is an
//! unknown-typed null placeholder. Guards are checked before body-scoped variables
//! (loop vars, statement-fn params) bind to typed placeholders.
//!
//! `Diag.pos` is cleared by `fail` and stamped as the error unwinds, innermost
//! stage first; `end` is set when the error names a span. With `Options.issues`
//! every statement is checked and each failure recorded; without it the first
//! fails. `ParamMap` is deliberately not a `pub` alias: re-exporting a
//! StringHashMap type makes `refAllDeclsRecursive` recurse its decl tree and crash.
//!
//! File targets: an unrecognised extension used to fall through to the CSV reader,
//! and a 12 MB zip answered `COUNT(*)` with the newlines in its deflate stream, so
//! an extension basalt does not read is a plan-time error. Parquet cannot be read
//! through a codec (it must seek), a compressed CSV or archive member is read
//! serially, and archives are judged by their member's name.
//!
//! This file walks a program (`Ctx`) and holds the entry points and parameter
//! substitution; `analyze/schema.zig` types each stage, `analyze/targets.zig` judges
//! what a read or write reaches without connecting, and `analyze/render.zig` prints
//! EXPLAIN. Whole-program tests are in `analyze/tests.zig`.

const std = @import("std");
const ast = @import("../lang/ast.zig");
const expand = @import("../lang/expand.zig");
const formats = @import("../format/formats.zig");
const types = @import("../lang/types.zig");
const pushdown = @import("pushdown.zig");
const Dialect = @import("../db/sql.zig").Dialect;
const decimal = @import("../exec/decimal.zig");
const eval = @import("../exec/eval.zig");
const textconv = @import("../exec/textconv.zig");
const pqdecode = @import("../format/parquet/read.zig");
const s3 = @import("../store/s3.zig");
const ftp = @import("../store/ftp.zig");
const registry = @import("../connect/registry.zig");
const table_builtins = @import("../connect/table_builtins.zig");
const keypush = @import("plan/keypush.zig");
const joinorder = @import("plan/joinorder.zig");
const op = @import("../exec/op.zig");
const profile = @import("../exec/profile.zig");
const body_stmt_rule = @import("env.zig").body_stmt_rule;
const env_mod = @import("env.zig");
const Value = @import("../exec/value.zig").Value;
const sql = @import("../db/sql.zig");
const holes = @import("analyze/holes.zig");
const msgfmt = @import("../format/msgfmt.zig");

pub const Diag = struct {
    buf: [512]u8 = undefined,
    msg: []const u8 = "",
    pos: ?ast.Pos = null,
    end: ?ast.Pos = null,

    pub fn stamp(self: *Diag, pos: ast.Pos) void {
        if (self.pos == null) self.pos = pos;
    }
};

pub const Error = error{ AnalyzeFailed, OutOfMemory };

pub fn fail(diag: *Diag, comptime fmt: []const u8, args: anytype) error{AnalyzeFailed} {
    @branchHint(.cold);
    return failSpan(diag, null, null, msgfmt.bufPrint(&diag.buf, fmt, args) catch "analysis error");
}

/// `fail`, underlining `span` when the offending text has one.
pub fn failAt(diag: *Diag, span: ?ast.Span, comptime fmt: []const u8, args: anytype) error{AnalyzeFailed} {
    @branchHint(.cold);
    const msg = msgfmt.bufPrint(&diag.buf, fmt, args) catch "analysis error";
    const s = span orelse return failSpan(diag, null, null, msg);
    return failSpan(diag, s.start, s.end, msg);
}

fn failSpan(diag: *Diag, pos: ?ast.Pos, end: ?ast.Pos, msg: []const u8) error{AnalyzeFailed} {
    diag.msg = msg;
    diag.pos = pos;
    diag.end = end;
    return error.AnalyzeFailed;
}

const ParamMap = std.StringHashMap(*const ast.Expr);

const SubstCtx = struct { arena: std.mem.Allocator, params: *const ParamMap };

fn substRecur(ctx: SubstCtx, e: *const ast.Expr) Error!*ast.Expr {
    return @constCast(try substExpr(ctx.arena, e, ctx.params));
}

/// Deep-copy `expr`, replacing each `$name` that names a param or LET with its literal;
/// no params returns the original. A bare name is a column, never a same-named PARAM.
pub fn substExpr(arena: std.mem.Allocator, expr: *const ast.Expr, params: *const ParamMap) Error!*const ast.Expr {
    if (params.count() == 0) return expr;
    if (expr.* == .field) {
        const q = expr.field;
        if (q.dollar and q.parts.len == 1) if (params.get(q.parts[0])) |lit| return lit;
        return expr;
    }
    return ast.rebuildExpr(arena, expr, SubstCtx{ .arena = arena, .params = params }, substRecur);
}

/// Every `$param` / `$let` in a filter replaced by its value: pushdown translates
/// before evaluation and once sent `[since]` to the source as a column. Returns
/// `stages` itself when no filter names one.
pub fn substFilterParams(arena: std.mem.Allocator, stages: []const ast.Stage, params: *const ParamMap) Error![]const ast.Stage {
    if (params.count() == 0) return stages;
    var out: ?[]ast.Stage = null;
    for (stages, 0..) |st, i| {
        if (st.node != .filter) continue;
        const e = try substExpr(arena, st.node.filter, params);
        if (e == st.node.filter) continue;
        const o = out orelse blk: {
            const copy = try arena.dupe(ast.Stage, stages);
            out = copy;
            break :blk copy;
        };
        o[i].node = .{ .filter = @constCast(e) };
    }
    return out orelse stages;
}

/// A LOAD target takes each column by its name, so two of one name (a file whose
/// header repeats one, `*` beside a same-named item) are refused, naming it.
pub fn uniqueColumns(schema: types.Schema, diag: *Diag) Error!void {
    for (schema.fields, 0..) |f, i| for (schema.fields[0..i]) |g| {
        if (std.mem.eql(u8, f.name, g.name))
            return fail(diag, "LOAD INTO writes each column under its own name, and two columns are named `{s}` — alias one apart in the SELECT list", .{f.name});
    };
}

/// A records file writes each row's one value verbatim, so its query yields exactly
/// one STRING or BYTES column.
pub fn recordsColumn(schema: types.Schema, diag: *Diag) Error!void {
    if (schema.fields.len != 1)
        return fail(diag, "a `records` file writes one STRING or BYTES column a row, verbatim; this query yields {d} columns — compose the record into one with `||`", .{schema.fields.len});
    const f = schema.fields[0];
    if (f.ty.kind != .string and f.ty.kind != .bytes)
        return fail(diag, "a `records` file writes one STRING or BYTES column a row, verbatim; `{s}` is {s} — CAST it to STRING or format it into the record", .{ f.name, if (f.ty.unknown) "an untyped NULL" else @tagName(f.ty.kind) });
}

fn recordsSink(w: ast.Write, hints: []const ast.Hint) bool {
    if (!std.mem.eql(u8, w.connector, "csv") or w.target.len == 0) return false;
    var d = Diag{};
    return (formatFromHints(hints, &d) catch null) == .records;
}

/// A LIMIT or OFFSET expression's rows: its `$name`s replaced and the whole evaluated
/// once into a whole number at least 0; null for NULL, which SQL reads as no bound.
/// Text that spells one is taken too, as a loop variable is text. A fraction rounds
/// as PostgreSQL casts it to an integer: a decimal half away from zero, a float to
/// the nearest even.
pub fn limitValue(arena: std.mem.Allocator, e: *const ast.Expr, params: *const ParamMap, what: []const u8, diag: *Diag) Error!?u64 {
    const x = try substExpr(arena, e, params);
    const v = eval.constEval(arena, x, &.{}, &.{}) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => {
            if (try firstDollar(arena, x)) |q|
                return failAt(diag, q.span, "unknown `${s}`: no PARAM, LET or loop variable of that name", .{q.parts[0]});
            return fail(diag, "{s} must be a constant: an integer, a `$param` or LET, or an expression of them", .{what});
        },
    };
    return switch (v) {
        .null => null,
        .int => |n| if (n < 0) fail(diag, "{s} must not be negative, got {d}", .{ what, n }) else @intCast(n),
        .string => |t| std.fmt.parseInt(u64, std.mem.trim(u8, t, " "), 10) catch fail(diag, "{s} must be a whole number of rows, got '{s}'", .{ what, t }),
        .float => |f| roundedRows(diag, what, textconv.floatToInt(f, .half_even) orelse return fail(diag, "{s} {d} is out of range", .{ what, f })),
        .decimal => |d| blk: {
            const q = decimal.roundWhole(d) orelse break :blk fail(diag, "{s} is out of range", .{what});
            break :blk roundedRows(diag, what, q);
        },
        else => fail(diag, "{s} must be an integer, got {s}", .{ what, @tagName(v) }),
    };
}

fn roundedRows(diag: *Diag, what: []const u8, n: i128) Error!?u64 {
    if (n < 0) return fail(diag, "{s} must not be negative, got {d}", .{ what, n });
    return std.math.cast(u64, n) orelse fail(diag, "{s} {d} is out of range", .{ what, n });
}

/// The first `$name` in `e`, after substitution one nothing binds.
fn firstDollar(arena: std.mem.Allocator, e: *const ast.Expr) Error!?ast.QualName {
    const refs = try dollarRefs(arena, e);
    return if (refs.len > 0) refs[0] else null;
}

/// Every LIMIT and OFFSET written as an expression, evaluated against `params`, so
/// top-N, pushdown and the limit itself read plain numbers. Returns `stages` itself
/// when none is.
pub fn bindLimits(arena: std.mem.Allocator, stages: []const ast.Stage, params: *const ParamMap, diag: *Diag) Error![]const ast.Stage {
    var out: ?[]ast.Stage = null;
    for (stages, 0..) |st, i| {
        if (st.node != .limit) continue;
        const l = st.node.limit;
        if (l.count_expr == null and l.offset_expr == null) continue;
        errdefer diag.stamp(st.pos);
        var bound: ast.Limit = .{ .count = l.count, .offset = l.offset };
        if (l.count_expr) |e| bound.count = (try limitValue(arena, e, params, "LIMIT", diag)) orelse std.math.maxInt(u64);
        if (l.offset_expr) |e| bound.offset = (try limitValue(arena, e, params, "OFFSET", diag)) orelse 0;
        const o = out orelse blk: {
            const copy = try arena.dupe(ast.Stage, stages);
            out = copy;
            break :blk copy;
        };
        o[i].node = .{ .limit = bound };
    }
    return out orelse stages;
}

pub fn mk(arena: std.mem.Allocator, e: ast.Expr) Error!*const ast.Expr {
    const p = try arena.create(ast.Expr);
    p.* = e;
    return p;
}

pub fn exprType(arena: std.mem.Allocator, in: types.Schema, e: *const ast.Expr, diag: *Diag) Error!types.Type {
    var ctx = eval.TypeCtx{ .schema = in, .arena = arena };
    return ctx.typeOf(e) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.TypeError => return failAt(diag, ctx.span, "{s}", .{ctx.msg}),
    };
}

const CoerceCtx = struct { arena: std.mem.Allocator, schema: types.Schema };

/// `e` with each quoted literal compared against a number or a bool made that
/// type's literal (`eval.litAs`), as SQL types an untyped literal by the other
/// operand: run after type checking, which refuses one that does not read so.
pub fn coerceLits(arena: std.mem.Allocator, in: types.Schema, e: *const ast.Expr) Error!*const ast.Expr {
    return coerceRecur(.{ .arena = arena, .schema = in }, e);
}

fn coerceRecur(cx: CoerceCtx, e: *const ast.Expr) Error!*ast.Expr {
    const out = try ast.rebuildExpr(cx.arena, e, cx, coerceRecur);
    if (out.* != .binary) return out;
    switch (out.binary.op) {
        .eq, .ne, .lt, .le, .gt, .ge, .distinct, .not_distinct => {},
        else => return out,
    }
    const b = out.binary;
    if (b.l.* == .str_lit and b.r.* != .str_lit) {
        if (try litFor(cx, b.l.str_lit, b.r)) |lit| out.binary.l = lit;
    } else if (b.r.* == .str_lit and b.l.* != .str_lit) {
        if (try litFor(cx, b.r.str_lit, b.l)) |lit| out.binary.r = lit;
    }
    return out;
}

fn litFor(cx: CoerceCtx, text: []const u8, other: *const ast.Expr) Error!?*ast.Expr {
    var tc = eval.TypeCtx{ .schema = cx.schema, .arena = cx.arena };
    const t = tc.typeOf(other) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.TypeError => return null,
    };
    if (t.unknown or !(t.kind.isNumeric() or t.kind == .bool)) return null;
    const lit = (try eval.litAs(cx.arena, text, t)) orelse return null;
    const p = try cx.arena.create(ast.Expr);
    p.* = lit;
    return p;
}

pub const connectionProblem = @import("analyze/options.zig").connectionProblem;
pub const sqlOptionProblem = @import("analyze/options.zig").sqlOptionProblem;
pub const checkSinkConnection = @import("analyze/options.zig").checkSinkConnection;
pub const unknownConnectionMsg = @import("analyze/options.zig").unknownConnectionMsg;
pub const checkBareUpsert = @import("analyze/options.zig").checkBareUpsert;
const options = @import("analyze/options.zig");
pub const Col = @import("analyze/schema.zig").Col;
pub const selectCols = @import("analyze/schema.zig").selectCols;
pub const schemaOfCols = @import("analyze/schema.zig").schemaOfCols;
pub const checkFilter = @import("analyze/schema.zig").checkFilter;
pub const fieldIndices = @import("analyze/schema.zig").fieldIndices;
pub const Agg = @import("analyze/schema.zig").Agg;
pub const AggregatePlan = @import("analyze/schema.zig").AggregatePlan;
pub const aggregatePlan = @import("analyze/schema.zig").aggregatePlan;
pub const windowFuncType = @import("analyze/schema.zig").windowFuncType;
pub const windowSchema = @import("analyze/schema.zig").windowSchema;
pub const windowPlan = @import("analyze/schema.zig").windowPlan;
pub const windowOffset = @import("analyze/schema.zig").windowOffset;
pub const lagDefault = @import("analyze/schema.zig").lagDefault;
pub const ExplodePlan = @import("analyze/schema.zig").ExplodePlan;
pub const explodePlan = @import("analyze/schema.zig").explodePlan;
pub const JoinPlan = @import("analyze/schema.zig").JoinPlan;
pub const joinPlan = @import("analyze/schema.zig").joinPlan;
pub const orientKeys = @import("analyze/schema.zig").orientKeys;
pub const residualPlan = @import("analyze/schema.zig").residualPlan;
pub const prepSchema = @import("analyze/schema.zig").prepSchema;
pub const KeyPrep = @import("analyze/schema.zig").KeyPrep;
const bindBodyVars = @import("analyze/schema.zig").bindBodyVars;
const typedZero = @import("analyze/schema.zig").typedZero;
pub const render = @import("analyze/render.zig").render;
pub const appendUnsupported = @import("analyze/render.zig").appendUnsupported;
const sinkKind = @import("analyze/render.zig").sinkKind;
pub const FileFormat = @import("analyze/targets.zig").FileFormat;
pub const readFormat = @import("analyze/targets.zig").readFormat;
pub const stdinProblem = @import("analyze/targets.zig").stdinProblem;
pub const hintText = @import("analyze/targets.zig").hintText;
pub const dialectFromHints = @import("analyze/targets.zig").dialectFromHints;
pub const formatFromHints = @import("analyze/targets.zig").formatFromHints;
pub const jsonOptions = @import("analyze/targets.zig").jsonOptions;
pub const parquetCodecFromHints = @import("analyze/targets.zig").parquetCodecFromHints;
pub const cutoverProblem = @import("analyze/targets.zig").cutoverProblem;
pub const xlsxOptions = @import("analyze/targets.zig").xlsxOptions;
pub const xmlOptions = @import("analyze/targets.zig").xmlOptions;
pub const jsonShapeFromHints = @import("analyze/targets.zig").jsonShapeFromHints;
pub const xmlNamesFromHints = @import("analyze/targets.zig").xmlNamesFromHints;
pub const json_array_append_fmt = @import("analyze/targets.zig").json_array_append_fmt;
pub const formatOfPath = @import("analyze/targets.zig").formatOfPath;
pub const xlsxSheetFromHints = @import("analyze/targets.zig").xlsxSheetFromHints;
pub const WriteOptions = @import("analyze/targets.zig").WriteOptions;
pub const writeOptions = @import("analyze/targets.zig").writeOptions;
pub const writeFormat = @import("analyze/targets.zig").writeFormat;
pub const deltaTarget = @import("analyze/targets.zig").deltaTarget;
pub const partitionBy = @import("analyze/targets.zig").partitionBy;
const delta_write = @import("../format/lake/delta_write.zig");
const lake = @import("../format/lake/table.zig");
pub const formatLabel = @import("analyze/targets.zig").formatLabel;
pub const unwritableTarget = @import("analyze/targets.zig").unwritableTarget;
pub const unreadableTarget = @import("analyze/targets.zig").unreadableTarget;
pub const archiveProblem = @import("analyze/targets.zig").archiveProblem;
pub const laneHints = @import("analyze/targets.zig").laneHints;
const morselParallelRead = @import("analyze/targets.zig").morselParallelRead;
const offlineSchema = @import("analyze/targets.zig").offlineSchema;
const offlineTable = @import("analyze/targets.zig").offlineTable;
pub const lakeAt = @import("analyze/targets.zig").lakeAt;
const folder = @import("../connect/folder.zig");

pub const Source = struct {
    connector: []const u8,
    detail: []const u8,
    schema: ?types.Schema = null,
    pushdown: []const u8 = "",
    /// The format a scan names when its path does not say, as a lake table's.
    format: ?[]const u8 = null,
};

pub const Sink = struct {
    connector: []const u8,
    target: []const u8,
    mode: []const u8,
    /// The format a sink writes when its path does not say, as a Delta table's.
    format: ?[]const u8 = null,
};

pub const Stage = struct {
    kind: []const u8,
    detail: []const u8,
    breaker: bool,
    /// The stage in SQL, as `EXPLAIN ANALYZE` prints it beside the operator.
    sql: []const u8 = "",
    /// How a join with no key runs, `range` or `nested-loop`; empty for a hash join.
    how: []const u8 = "",
    right_scan: ?[]const u8 = null,
    right_pushdown: []const u8 = "",
    /// Which side's SQL read a join narrows by the other side's keys, if either.
    key_pushdown: []const u8 = "",
    join_order: []const u8 = "",
    out_schema: ?types.Schema = null,
};

pub const Physical = struct {
    has_breaker: bool,
    splittable: bool,
    sink_parallel: bool,
    morsel_parallel: bool,
    top_n: ?u64 = null,
};

pub const Output = struct {
    source: Source,
    stages: []const Stage,
    sink: Sink,
    physical: Physical,
};

pub const Plan = struct {
    outputs: []const Output,
};

pub const Scope = struct { names: []const []const u8, values: []const Value };

pub const Folded = union(enum) { value: Value, failed: []const u8 };

/// Whether `e` calls a function that reads a file (`read_bytes`, `file_size`), which
/// `check` does not fold, as it reads nothing.
fn readsFiles(e: *const ast.Expr) bool {
    const S = struct {
        fn hit(_: void, n: *const ast.Expr) ?bool {
            if (n.* != .call) return null;
            const b = eval.lookupBuiltin(n.call.name) orelse return null;
            return if (b.io) true else null;
        }
    };
    return ast.anyExpr(e, {}, S.hit);
}

/// What an expression folds to over `sc`, or the label of why it does not.
pub fn fold(arena: std.mem.Allocator, e: *const ast.Expr, sc: Scope) Error!Folded {
    if (readsFiles(e)) return .{ .failed = "reads a file, which only a run does" };
    const v = eval.constEval(arena, e, sc.names, sc.values) catch |err| {
        if (err == error.OutOfMemory) return error.OutOfMemory;
        return .{ .failed = env_mod.errLabel(err) };
    };
    return .{ .value = v };
}

/// A THROW's stop message over `sc`, or null when it does not fire: only a `true`
/// condition fires (null is not a failure, as in SQL), an absent one always does.
pub fn throwVerdict(arena: std.mem.Allocator, t: ast.Throw, sc: Scope) Error!?[]const u8 {
    if (t.when) |w| switch (try fold(arena, w, sc)) {
        .failed => |why| return try msgfmt.allocPrint(arena, "THROW condition: {s}", .{why}),
        .value => |c| {
            if (c == .null) return null;
            if (c != .bool) return try msgfmt.allocPrint(arena, "THROW condition must be a boolean, got {s}", .{@tagName(std.meta.activeTag(c))});
            if (!c.bool) return null;
        },
    };
    return switch (try fold(arena, t.message, sc)) {
        .failed => |why| try msgfmt.allocPrint(arena, "THROW message: {s}", .{why}),
        .value => |m| try eval.valueToString(arena, m),
    };
}

pub const Printed = union(enum) { text: []const u8, problem: []const u8 };

/// A `PRINT`'s text, rendered as any cell is, or why it cannot be.
pub fn printed(arena: std.mem.Allocator, e: *const ast.Expr, sc: Scope) Error!Printed {
    return switch (try fold(arena, e, sc)) {
        .failed => |why| .{ .problem = try msgfmt.allocPrint(arena, "PRINT: {s}", .{why}) },
        .value => |v| .{ .text = try eval.valueToString(arena, v) },
    };
}

pub const CallArgs = union(enum) { cells: []const []const u8, problem: []const u8 };

/// A `CALL`'s arguments as the text cells its body's row binds, or why one cannot be.
pub fn callArgs(arena: std.mem.Allocator, c: ast.CallStmt, params: []const ast.FnParam, sc: Scope) Error!CallArgs {
    const cells = try arena.alloc([]const u8, params.len);
    for (params, c.args, cells) |p, a, *cell| switch (try fold(arena, a, sc)) {
        .failed => |why| return .{ .problem = try msgfmt.allocPrint(arena, "CALL {s}: argument `{s}`: {s}", .{ c.name, p.name, why }) },
        .value => |v| cell.* = try eval.valueToString(arena, v),
    };
    return .{ .cells = cells };
}

pub const ArmPick = union(enum) { arm: ?usize, problem: []const u8 };

/// The statement `CASE` arm that runs (`_` matches), null for none, or why the CASE
/// cannot be decided; `ctx` names it in the refusal.
pub fn matchArm(arena: std.mem.Allocator, m: ast.StmtMatch, sc: Scope, ctx: []const u8) Error!ArmPick {
    var subj: ?Value = null;
    if (m.subject) |s| switch (try fold(arena, s, sc)) {
        .failed => |why| return .{ .problem = try msgfmt.allocPrint(arena, "{s} subject: {s}", .{ ctx, why }) },
        .value => |v| subj = v,
    };
    if (subj) |sv| for (m.arms) |arm| for (arm.pats) |p| if (try patternTypeProblem(arena, sv, p)) |why|
        return .{ .problem = why };
    for (m.arms, 0..) |arm, i| {
        if (arm.is_default) return .{ .arm = i };
        if (arm.guard) |g| {
            switch (try fold(arena, g, sc)) {
                .failed => |why| return .{ .problem = try msgfmt.allocPrint(arena, "{s} guard: {s}", .{ ctx, why }) },
                .value => |gv| if (gv == .bool and gv.bool) return .{ .arm = i },
            }
            continue;
        }
        const sv = subj orelse continue;
        for (arm.pats) |p| switch (try fold(arena, p, sc)) {
            .failed => |why| return .{ .problem = try msgfmt.allocPrint(arena, "{s} pattern: {s}", .{ ctx, why }) },
            .value => |pv| if (valuesEqualLoose(arena, sv, pv)) return .{ .arm = i },
        };
    }
    return .{ .arm = null };
}

/// `subject = pattern` as a CASE arm judges it: NULL equals nothing, NULL included,
/// as in a CASE expression.
fn valuesEqualLoose(arena: std.mem.Allocator, a: Value, b: Value) bool {
    if (a.isNull() or b.isNull()) return false;
    if (eval.compareValues(a, b)) |ord| return ord == .eq;
    const as = eval.valueToString(arena, a) catch return false;
    const bs = eval.valueToString(arena, b) catch return false;
    return std.mem.eql(u8, as, bs);
}

/// Every `$name` the expressions read is in `sc`: what the run decides from PARAMs
/// and expression LETs alone, which `check` can decide too.
fn foldsHere(arena: std.mem.Allocator, exprs: []const ?*const ast.Expr, sc: Scope) Error!bool {
    for (exprs) |maybe| {
        const e = maybe orelse continue;
        for (try dollarRefs(arena, e)) |q| if (!ast.nameIn(sc.names, q.parts[0])) return false;
    }
    return true;
}

/// A top-level THROW, PRINT, CALL or CASE that reads only what is decided before
/// anything runs, judged as the run judges it.
fn checkScriptStmt(arena: std.mem.Allocator, s: ast.Stmt, program: ast.Program, params: *const ParamMap, sc: Scope, diag: *Diag) Error!void {
    switch (s) {
        .throw => |t| if (try foldsHere(arena, &.{ t.when, t.message }, sc)) if (try throwVerdict(arena, t, sc)) |why|
            return failPos(diag, t.pos, "{s}", .{why}),
        .print => |p| if (try foldsHere(arena, &.{p.expr}, sc)) switch (try printed(arena, p.expr, sc)) {
            .problem => |why| return failPos(diag, p.pos, "{s}", .{why}),
            .text => {},
        },
        .call => |c| {
            const fd = for (program.stmts) |o| {
                if (o == .func and std.mem.eql(u8, o.func.name, c.name)) break o.func;
            } else return;
            if (fd.body != .stmts or fd.params.len != c.args.len) return;
            const args = try arena.alloc(?*const ast.Expr, c.args.len);
            for (c.args, args) |a, *o| o.* = a;
            if (try foldsHere(arena, args, sc)) switch (try callArgs(arena, c, fd.params, sc)) {
                .problem => |why| return failPos(diag, c.pos, "{s}", .{why}),
                .cells => {},
            };
        },
        .match => |m| {
            var exprs = std.array_list.Managed(?*const ast.Expr).init(arena);
            try exprs.append(m.subject);
            for (m.arms) |arm| {
                for (arm.pats) |p| try exprs.append(p);
                try exprs.append(arm.guard);
            }
            if (!try foldsHere(arena, exprs.items, sc)) return checkMatchPatterns(arena, m, params, diag);
            switch (try matchArm(arena, m, sc, "match")) {
                .problem => |why| return failPos(diag, m.pos, "{s}", .{why}),
                .arm => {},
            }
        },
        else => {},
    }
}

pub fn failPos(diag: *Diag, pos: ?ast.Pos, comptime fmt: []const u8, args: anytype) error{AnalyzeFailed} {
    @branchHint(.cold);
    return failSpan(diag, pos, null, msgfmt.bufPrint(&diag.buf, fmt, args) catch "analysis error");
}

/// A statement `CASE` arm's string-literal pattern that cannot read as its typed
/// subject's type, so the arm could never match: `CASE $n WHEN 'x'` with an INT `$n`.
pub fn patternTypeProblem(arena: std.mem.Allocator, subj: Value, pat: *const ast.Expr) Error!?[]const u8 {
    if (pat.* != .str_lit) return null;
    const ty: types.Type = switch (subj) {
        .int => types.Type.init(.int),
        .float => types.Type.init(.float),
        .bool => types.Type.init(.bool),
        .date => types.Type.init(.date),
        .time => types.Type.init(.time),
        .timestamp => types.Type.init(.timestamp),
        .decimal => |d| types.Type.decimal(38, d.scale),
        else => return null,
    };
    if (eval.castValueTyped(arena, .{ .string = pat.str_lit }, ty)) |_| return null else |_| {}
    return try msgfmt.allocPrint(arena, "CASE pattern '{s}' is not {s} like its subject, so it can never match", .{ pat.str_lit, @tagName(ty.kind) });
}

/// `matchArm`'s pattern refusal, for a top-level `CASE` whose subject folds here
/// though not all of it does.
fn checkMatchPatterns(arena: std.mem.Allocator, m: ast.StmtMatch, params: *const ParamMap, diag: *Diag) Error!void {
    const s = m.subject orelse return;
    const v = eval.constEval(arena, try substExpr(arena, s, params), &.{}, &.{}) catch return;
    for (m.arms) |arm| for (arm.pats) |p| if (try patternTypeProblem(arena, v, p)) |why|
        return failPos(diag, m.pos, "{s}", .{why});
}

pub const ParamOverride = struct { name: []const u8, value: []const u8 };

pub const param_default_msg = "param default must be a literal";

/// A PARAM's DEFAULT as the value it stands for: a literal, or a negated number.
pub fn paramDefaultValue(e: *const ast.Expr) ?Value {
    return switch (e.*) {
        .int_lit => |i| .{ .int = i },
        .float_lit => |f| .{ .float = f },
        .dec_lit => |d| .{ .decimal = .{ .unscaled = d.unscaled, .scale = d.scale } },
        .str_lit => |s| .{ .string = s },
        .bool_lit => |b| .{ .bool = b },
        .null_lit => .null,
        .unary => |u| if (u.op != .neg) null else switch (paramDefaultValue(u.e) orelse return null) {
            .int => |i| if (i == std.math.minInt(i64)) null else .{ .int = -i },
            .float => |f| .{ .float = -f },
            .decimal => |d| .{ .decimal = decimal.negate(d) },
            else => null,
        },
        else => null,
    };
}

/// How a query LET is named in its errors; a desugared WHERE subquery is a
/// "scalar subquery".
pub fn queryLetLabel(arena: std.mem.Allocator, name: []const u8) Error![]const u8 {
    if (std.mem.startsWith(u8, name, "__scalar")) return "scalar subquery";
    return msgfmt.allocPrint(arena, "LET `{s}`", .{name});
}

/// A query LET binds one cell, so a result of other than one column is refused.
pub fn queryLetColumnsProblem(arena: std.mem.Allocator, name: []const u8, ncols: usize) Error!?[]const u8 {
    if (ncols == 1) return null;
    return try msgfmt.allocPrint(arena, "{s}: must produce exactly one column, got {d}", .{ try queryLetLabel(arena, name), ncols });
}

/// A FOR EACH binds one discovery column per loop variable, so a narrower result is refused.
pub fn loopColumnsProblem(arena: std.mem.Allocator, ncols: usize, nvars: usize) Error!?[]const u8 {
    if (ncols >= nvars) return null;
    return try msgfmt.allocPrint(arena, "for-each: the discovery query returns fewer columns than loop variables ({d} for {d})", .{ ncols, nvars });
}

/// The name `ANCHOR SCHEMA` matches a union branch by: a table's last name part,
/// `query` for a subquery, nothing for a file.
pub fn unionBranchName(b: ast.UnionBranch) []const u8 {
    if (b.pipeline != null) return "query";
    return switch (b.read.form) {
        .table => |q| q.last(),
        else => "",
    };
}

/// `ANCHOR SCHEMA name` must name one of the union's tables (`first` names the first).
pub fn unionCanonProblem(arena: std.mem.Allocator, canon: []const u8, names: []const []const u8) Error!?[]const u8 {
    if (std.mem.eql(u8, canon, "first") or ast.nameIn(names, canon)) return null;
    return try msgfmt.allocPrint(arena, "union canon `{s}` is not one of the source tables", .{canon});
}

pub fn notJsonParamMsg(arena: std.mem.Allocator, name: []const u8) Error![]const u8 {
    return msgfmt.allocPrint(arena, "for-each: `{s}` is not a JSON param", .{name});
}

/// The literal a `-p` string stands for, typed by the PARAM's declared type. Anything
/// not scalar keeps the declared default: `check` is offline.
/// The `$name`s an expression reads, in order, repeats included.
pub fn dollarRefs(arena: std.mem.Allocator, e: *const ast.Expr) Error![]const ast.QualName {
    const Collect = struct {
        arena: std.mem.Allocator,
        names: *std.array_list.Managed(ast.QualName),
        fn recur(c: @This(), x: *const ast.Expr) Error!*ast.Expr {
            if (x.* == .field) {
                if (x.field.dollar) try c.names.append(x.field);
                return @constCast(x);
            }
            return ast.rebuildExpr(c.arena, x, c, recur);
        }
    };
    var names = std.array_list.Managed(ast.QualName).init(arena);
    _ = try Collect.recur(.{ .arena = arena, .names = &names }, e);
    return names.items;
}

pub const LetRefProblem = struct { msg: []const u8, span: ?ast.Span };

/// Why an expression LET cannot be folded, or null: it reads a query LET, which is
/// decided only once queries run, or a `$name` no PARAM or earlier LET declares.
pub fn letRefProblem(arena: std.mem.Allocator, program: ast.Program, l: ast.LetConst) Error!?LetRefProblem {
    const le = l.expr orelse return null;
    for (try dollarRefs(arena, le)) |q| {
        const n = q.parts[0];
        var declared = false;
        for (program.stmts) |s| switch (s) {
            .param => |p| if (std.mem.eql(u8, p.name, n)) {
                declared = true;
            },
            .let_const => |other| {
                if (std.mem.eql(u8, other.name, l.name)) break;
                if (!std.mem.eql(u8, other.name, n)) continue;
                if (other.expr == null)
                    return .{ .span = q.span, .msg = try msgfmt.allocPrint(arena, "LET `{s}` reads `${s}`, a query LET: an expression LET is folded before any query runs, so compute it in the query instead", .{ l.name, n }) };
                declared = true;
            },
            else => {},
        };
        if (!declared) return .{ .span = q.span, .msg = try msgfmt.allocPrint(arena, "LET `{s}`: unknown `${s}`: no PARAM or earlier LET of that name", .{ l.name, n }) };
    }
    return null;
}

/// What a PARAM's text should have been when it does not read as the declared type,
/// or null when it does; `check` and the run both word the error from it.
pub fn paramTextProblem(arena: std.mem.Allocator, ty: types.Type, text: []const u8) ?[]const u8 {
    const ok = switch (ty.kind) {
        .int, .float, .bool, .date, .time, .timestamp, .decimal => paramFromText(arena, ty, text) != null,
        else => true,
    };
    if (ok) return null;
    return switch (ty.kind) {
        .int => "an integer",
        .float, .decimal => "a number",
        .bool => "a boolean (true/false, t/f, yes/no, y/n or 1/0)",
        .date => "a date (YYYY-MM-DD)",
        .time => "a time (HH:MM[:SS[.ffffff]])",
        .timestamp => "a timestamp (YYYY-MM-DD[ HH:MM:SS[.ffffff]])",
        else => unreachable,
    };
}

/// A PARAM's text as its declared type through `eval.paramFromText`, null when it
/// does not read as one. Null for the kinds bound as text or not from text at all.
pub fn paramFromText(arena: std.mem.Allocator, ty: types.Type, text: []const u8) ?Value {
    return switch (ty.kind) {
        .bool, .int, .float, .date, .time, .timestamp, .decimal => eval.paramFromText(arena, ty, text) catch null,
        else => null,
    };
}

fn overrideExpr(arena: std.mem.Allocator, ty: types.Type, raw: []const u8) Error!?*const ast.Expr {
    return switch (ty.kind) {
        .int => mk(arena, .{ .int_lit = (paramFromText(arena, ty, raw) orelse return null).int }),
        .float => mk(arena, .{ .float_lit = (paramFromText(arena, ty, raw) orelse return null).float }),
        .bool => mk(arena, .{ .bool_lit = (paramFromText(arena, ty, raw) orelse return null).bool }),
        .string, .bytes => mk(arena, .{ .str_lit = raw }),
        .date, .time, .timestamp, .decimal => typedParam(arena, ty, try mk(arena, .{ .str_lit = raw })),
        else => null,
    };
}

/// A DATE, TIME, TIMESTAMP or DECIMAL param is its text CAST to the declared type
/// (`env.mkLit`), so `date_add('day', 1, $d)` types as the run does; a FLOAT one
/// defaulting to `0.5` is that float.
fn typedParam(arena: std.mem.Allocator, ty: types.Type, e: *const ast.Expr) Error!*const ast.Expr {
    return switch (ty.kind) {
        .date, .time, .timestamp, .decimal => mk(arena, .{ .cast = .{ .e = @constCast(e), .ty = ty } }),
        .float => switch (e.*) {
            .int_lit => |i| mk(arena, .{ .float_lit = @floatFromInt(i) }),
            .dec_lit => |d| mk(arena, .{ .float_lit = decimal.toF64ByPow(d.unscaled, d.scale) }),
            else => e,
        },
        else => e,
    };
}

pub fn analyze(arena: std.mem.Allocator, raw_program: ast.Program, diag: *Diag) error{ AnalyzeFailed, OutOfMemory }!Plan {
    return analyzeWith(arena, raw_program, &.{}, diag);
}

pub fn analyzeWith(arena: std.mem.Allocator, raw_program: ast.Program, cli: []const ParamOverride, diag: *Diag) error{ AnalyzeFailed, OutOfMemory }!Plan {
    return analyzeOpts(arena, raw_program, .{ .overrides = cli }, diag);
}

/// Syntax-like arguments (a date unit, a `strftime` format) checked wherever a literal
/// one appears, since typing only sees them where column types are known:
/// `date_trunc('fortnight', ts)` over a SQL table once failed only at run time.
fn checkLiteralArgs(diag: *Diag, e: *const ast.Expr) Error!void {
    switch (e.*) {
        .call => |c| {
            const unit_fn = std.mem.eql(u8, c.name, "date_trunc") or std.mem.eql(u8, c.name, "extract") or
                std.mem.eql(u8, c.name, "date_add") or std.mem.eql(u8, c.name, "date_diff");
            if (unit_fn and c.args.len > 0 and c.args[0].* == .str_lit and eval.timeUnit(c.args[0].str_lit) == null)
                return fail(diag, "unknown time unit `{s}` (units: year, month, week, day, hour, minute, second)", .{c.args[0].str_lit});
            if (std.mem.eql(u8, c.name, "strftime") and c.args.len == 2 and c.args[1].* == .str_lit)
                if (eval.badStrftime(c.args[1].str_lit)) |bad|
                    return fail(diag, "`strftime` does not support `%{s}` (supported: %Y %m %d %H %M %S %y %%)", .{bad});
            for (c.args) |a| try checkLiteralArgs(diag, a);
        },
        .unary => |u| try checkLiteralArgs(diag, u.e),
        .binary => |b| {
            try checkLiteralArgs(diag, b.l);
            try checkLiteralArgs(diag, b.r);
        },
        .cond => |c| {
            try checkLiteralArgs(diag, c.cond);
            try checkLiteralArgs(diag, c.then);
            try checkLiteralArgs(diag, c.els);
        },
        .cast => |c| try checkLiteralArgs(diag, c.e),
        .is_null => |n| try checkLiteralArgs(diag, n.e),
        .let_in => |l| {
            try checkLiteralArgs(diag, l.value);
            try checkLiteralArgs(diag, l.body);
        },
        .lambda => |l| try checkLiteralArgs(diag, l.body),
        .match => |m| {
            if (m.subject) |s| try checkLiteralArgs(diag, s);
            for (m.arms) |arm| {
                for (arm.pats) |p| try checkLiteralArgs(diag, p);
                if (arm.guard) |g| try checkLiteralArgs(diag, g);
                try checkLiteralArgs(diag, arm.value);
            }
        },
        else => {},
    }
}

fn checkStageLiterals(diag: *Diag, st: ast.Stage) Error!void {
    errdefer diag.stamp(st.pos);
    switch (st.node) {
        .filter => |p| try checkLiteralArgs(diag, p),
        .select => |items| for (items) |it| if (it == .computed) try checkLiteralArgs(diag, it.computed.expr),
        .aggregate => |ag| for (ag.aggs) |a| if (a.arg) |x| try checkLiteralArgs(diag, x),
        else => {},
    }
}

/// The `${...}` holes in a stage's paths, target, query text and option values, each
/// of which the run renders: one whose text is no expression a hole may hold.
fn checkStageHoles(arena: std.mem.Allocator, st: ast.Stage, diag: *Diag) Error!void {
    var texts = std.array_list.Managed([]const u8).init(arena);
    switch (st.node) {
        .read => |rd| {
            try texts.append(rd.where);
            switch (rd.form) {
                .path, .query => |s| try texts.append(s),
                else => {},
            }
        },
        .write => |w| try texts.append(w.target),
        else => {},
    }
    for (st.hints) |h| switch (h.value) {
        .str, .ident => |s| try texts.append(s),
        else => {},
    };
    for (texts.items) |s| if (try holes.problem(arena, s)) |why| return failPos(diag, st.pos, "{s}", .{why});
}

pub const KnownTable = struct { name: []const u8, schema: ?types.Schema = null };

pub const Issue = struct { msg: []const u8, pos: ?ast.Pos = null, end: ?ast.Pos = null };

pub const Options = struct {
    overrides: []const ParamOverride = &.{},
    known_tables: []const KnownTable = &.{},
    issues: ?*std.array_list.Managed(Issue) = null,
    declarations_only: bool = false,
};

pub fn analyzeOpts(arena: std.mem.Allocator, raw_program: ast.Program, opts: Options, diag: *Diag) error{ AnalyzeFailed, OutOfMemory }!Plan {
    var p = analyzeInner(arena, raw_program, opts, diag);
    if (opts.issues) |list| if (p) |_| {} else |e| {
        if (e == error.OutOfMemory) return e;
        try list.append(.{ .msg = try arena.dupe(u8, diag.msg), .pos = diag.pos, .end = diag.end });
        p = .{ .outputs = &.{} };
    };
    return p;
}

/// A LET over what the run binds for sure, a PARAM given a value or a DEFAULT and the
/// LETs folded from them, is folded here as the run folds it.
fn analyzeInner(arena: std.mem.Allocator, raw_program: ast.Program, opts: Options, diag: *Diag) error{ AnalyzeFailed, OutOfMemory }!Plan {
    const cli = opts.overrides;
    var expand_msg: []const u8 = "";
    const json_args = try arena.alloc(expand.JsonArg, cli.len);
    for (cli, json_args) |o, *ja| ja.* = .{ .name = o.name, .text = o.value };
    var expand_at: ?ast.Pos = null;
    const program = expand.expandProgramAt(arena, raw_program, json_args, &expand_msg, &expand_at) catch |e| switch (e) {
        error.OutOfMemory => return error.OutOfMemory,
        error.ExpandFailed => return failPos(diag, expand_at, "{s}", .{expand_msg}),
    };
    if (program.stmts.len == 0 or program.stmts[0] != .kind)
        return fail(diag, "internal error: the parsed program has no kind statement", .{});

    var bindings = std.StringHashMap(ast.Pipeline).init(arena);
    var connections = std.StringHashMap(ast.Connection).init(arena);
    for (program.stmts[1..]) |s| if (s == .connection) {
        if (try connectionProblem(arena, s.connection)) |why| {
            const e = fail(diag, "{s}", .{why});
            diag.pos = s.connection.pos;
            return e;
        }
        try connections.put(s.connection.name, s.connection);
    };
    if (!opts.declarations_only and !hasRunnable(program.stmts[1..]))
        return fail(diag, "nothing to run: the script needs a `LOAD INTO` or a terminal `SELECT`", .{});

    var params_map = ParamMap.init(arena);
    var known_names = std.array_list.Managed([]const u8).init(arena);
    var known_values = std.array_list.Managed(Value).init(arena);
    for (cli) |o| for (program.stmts) |s| if (s == .let_const and std.mem.eql(u8, s.let_const.name, o.name))
        return fail(diag, "`{s}` is a LET, not a PARAM — it cannot be bound externally", .{o.name});
    for (program.stmts) |s| if (s == .param) {
        const p = s.param;
        if (params_map.contains(p.name)) {
            const e = fail(diag, "duplicate PARAM `{s}`", .{p.name});
            diag.pos = p.pos;
            return e;
        }
        var bound: ?*const ast.Expr = null;
        var text: ?[]const u8 = null;
        for (cli) |o| if (std.mem.eql(u8, o.name, p.name)) {
            bound = try overrideExpr(arena, p.ty, o.value);
            text = o.value;
        };
        if (text == null) if (p.default) |d| if (d.* == .str_lit) {
            text = d.str_lit;
        };
        if (bound == null and !p.is_json) if (p.default) |d| if (paramDefaultValue(d) == null)
            return fail(diag, "{s}", .{param_default_msg});
        if (text) |t| if (paramTextProblem(arena, p.ty, t)) |want|
            return fail(diag, "PARAM `{s}`: `{s}` is not {s}", .{ p.name, t, want });
        if (p.ty.kind == .int) if (p.default) |d| if (paramDefaultValue(d)) |raw| if (raw == .float or raw == .decimal)
            return fail(diag, "PARAM `{s}`: its DEFAULT is not an integer", .{p.name});
        const pe = bound orelse try typedParam(arena, p.ty, if (p.default) |d| d else try typedZero(arena, p.ty));
        try params_map.put(p.name, pe);
        if (bound != null or p.default != null) if (eval.constEval(arena, pe, &.{}, &.{})) |v| {
            try known_names.append(p.name);
            try known_values.append(v);
        } else |_| {};
    };
    for (program.stmts) |s| if (s == .let_const) {
        const l = s.let_const;
        if (params_map.contains(l.name)) {
            const clash = for (program.stmts) |o| {
                if (o == .param and std.mem.eql(u8, o.param.name, l.name)) break true;
            } else false;
            const e = if (clash)
                fail(diag, "`{s}` is declared twice: LET and PARAM share one name space", .{l.name})
            else
                fail(diag, "duplicate LET `{s}`", .{l.name});
            diag.pos = l.pos;
            return e;
        }
        if (try letRefProblem(arena, program, l)) |why| {
            const e = failAt(diag, why.span, "{s}", .{why.msg});
            if (diag.pos == null) diag.pos = l.pos;
            return e;
        }
        if (l.expr) |le| {
            const refs = try dollarRefs(arena, le);
            const all_known = for (refs) |q| {
                if (!ast.nameIn(known_names.items, q.parts[0])) break false;
            } else true;
            if (all_known and !readsFiles(le)) {
                const v = eval.constEval(arena, le, known_names.items, known_values.items) catch |err| {
                    if (err == error.OutOfMemory) return error.OutOfMemory;
                    const e = fail(diag, "LET `{s}`: {s}", .{ l.name, env_mod.errLabel(err) });
                    diag.pos = l.pos;
                    return e;
                };
                try known_names.append(l.name);
                try known_values.append(v);
                try params_map.put(l.name, env_mod.mkLit(arena, v) catch return error.OutOfMemory);
            } else try params_map.put(l.name, try substExpr(arena, le, &params_map));
        } else {
            const ph = try arena.create(ast.Expr);
            ph.* = .null_lit;
            try params_map.put(l.name, ph);
        }
    };
    for (program.stmts) |s| switch (s) {
        .throw => try checkScriptStmt(arena, s, program, &params_map, .{ .names = known_names.items, .values = known_values.items }, diag),
        else => {},
    };
    for (program.stmts) |s| switch (s) {
        .print, .call, .match => try checkScriptStmt(arena, s, program, &params_map, .{ .names = known_names.items, .values = known_values.items }, diag),
        else => {},
    };
    try bindBodyVars(arena, program.stmts, &params_map);

    var written = std.array_list.Managed([]const u8).init(arena);
    try writtenFiles(arena, program.stmts[1..], &written);
    var json_params = std.array_list.Managed([]const u8).init(arena);
    for (program.stmts) |s| if (s == .param and s.param.is_json) try json_params.append(s.param.name);
    var ctx = Ctx{ .arena = arena, .bindings = &bindings, .connections = &connections, .params = &params_map, .lets = &params_map, .diag = diag, .known = opts.known_tables, .issues = opts.issues, .written = written.items, .json_params = json_params.items };

    var out_plans = std.array_list.Managed(Output).init(arena);
    try ctx.checkStmts(program.stmts[1..], &out_plans, true);
    for (raw_program.stmts) |s| if (s == .func) if (s.func.table_check) |tc| {
        var tc_at: ?ast.Pos = null;
        const etc = expand.expandTableCheck(arena, raw_program, tc, &expand_msg, &tc_at) catch |e| switch (e) {
            error.OutOfMemory => return error.OutOfMemory,
            error.ExpandFailed => return failPos(diag, tc_at orelse s.func.pos, "{s}", .{expand_msg}),
        };
        try ctx.checkTableFn(s.func, etc);
    };

    return .{ .outputs = try out_plans.toOwnedSlice() };
}

/// Does a stage name a column through `IDENTIFIER(<expr>)`, which the parser lowers to
/// a field whose name is a `${...}` template?
fn hasDynamicName(node: ast.Stage.Node) bool {
    return switch (node) {
        .filter => |e| exprHasDynamicName(e),
        .select => |items| for (items) |it| {
            if (switch (it) {
                .field => |q| isDynamicName(q),
                .computed => |c| exprHasDynamicName(c.expr),
                .star_except => |names| for (names) |n| {
                    if (std.mem.indexOf(u8, n, "${") != null) break true;
                } else false,
                else => false,
            }) break true;
        } else false,
        .distinct => |d| if (d.on) |on| anyDynamicName(on) else false,
        .sort => |st| for (st.keys) |k| {
            if (isDynamicName(k.field)) break true;
        } else false,
        .aggregate => |ag| anyDynamicName(ag.by) or for (ag.aggs) |a| {
            if (a.arg) |e| if (exprHasDynamicName(e)) break true;
        } else false,
        else => false,
    };
}

fn anyDynamicName(names: []const ast.QualName) bool {
    for (names) |q| if (isDynamicName(q)) return true;
    return false;
}

fn exprHasDynamicName(e: *const ast.Expr) bool {
    return switch (e.*) {
        .null_lit, .bool_lit, .int_lit, .float_lit, .dec_lit, .str_lit, .lambda_var => false,
        .lambda => |l| exprHasDynamicName(l.body),
        .field => |q| isDynamicName(q),
        .unary => |u| exprHasDynamicName(u.e),
        .binary => |b| exprHasDynamicName(b.l) or exprHasDynamicName(b.r),
        .cond => |c| exprHasDynamicName(c.cond) or exprHasDynamicName(c.then) or exprHasDynamicName(c.els),
        .cast => |c| exprHasDynamicName(c.e),
        .is_null => |n| exprHasDynamicName(n.e),
        .let_in => |l| exprHasDynamicName(l.value) or exprHasDynamicName(l.body),
        .call => |c| for (c.args) |a| {
            if (exprHasDynamicName(a)) break true;
        } else false,
        .match => |m| blk: {
            if (m.subject) |sub| if (exprHasDynamicName(sub)) break :blk true;
            for (m.arms) |arm| {
                for (arm.pats) |p| if (exprHasDynamicName(p)) break :blk true;
                if (arm.guard) |g| if (exprHasDynamicName(g)) break :blk true;
                if (exprHasDynamicName(arm.value)) break :blk true;
            }
            break :blk false;
        },
    };
}

pub fn isDynamicName(q: ast.QualName) bool {
    return std.mem.indexOf(u8, lastPart(q), "${") != null;
}

/// Whether the top level does any work: a query, or a FOR EACH, CASE or CALL,
/// which may only PRINT. One rule for `check` and `run`.
pub fn hasRunnable(stmts: []const ast.Stmt) bool {
    for (stmts) |s| switch (s) {
        .output, .for_each, .match, .call, .explain => return true,
        else => {},
    };
    return false;
}

pub const stmtPos = ast.stmtPos;

/// A join's `join_order` hint, which only names 'auto' or 'written', and its
/// `max_build`, a size unless a loop row renders it.
pub fn checkJoinHints(arena: std.mem.Allocator, stages: []const ast.Stage, diag: *Diag) Error!void {
    for (stages) |st| if (st.node == .join) {
        if (joinorder.mode(st.hints) == .invalid) return failPos(diag, st.pos, "{s}", .{joinorder.invalid_msg});
        for (st.hints) |h| if (std.mem.eql(u8, h.key, "max_build")) {
            const text: []const u8 = switch (h.value) {
                .str, .ident => |v| v,
                else => "",
            };
            if (std.mem.indexOf(u8, text, "${") != null) continue;
            switch (try options.maxBuild(arena, h)) {
                .problem => |why| return failPos(diag, h.pos, "{s}", .{why}),
                .bytes => {},
            }
        };
    };
}

/// Every file the script writes, in any statement or body. A read of one is typed
/// by the run, not from what is on disk now: a copy left by an earlier run may have
/// other columns than the one this script writes before it reads.
fn writtenFiles(arena: std.mem.Allocator, stmts: []const ast.Stmt, out: *std.array_list.Managed([]const u8)) Error!void {
    for (stmts) |s| switch (s) {
        .output => |p| try writtenFile(p, out),
        .explain => |e| try writtenFile(e.pipeline, out),
        .for_each => |fe| try writtenFiles(arena, fe.body, out),
        .match => |m| for (m.arms) |arm| try writtenFiles(arena, arm.body, out),
        .func => |fd| if (fd.body == .stmts) try writtenFiles(arena, fd.body.stmts, out),
        else => {},
    };
}

fn writtenFile(p: ast.Pipeline, out: *std.array_list.Managed([]const u8)) Error!void {
    if (p.stages.len == 0) return;
    const last = p.stages[p.stages.len - 1];
    if (last.node != .write or !std.mem.eql(u8, last.node.write.connector, "csv")) return;
    try out.append(last.node.write.target);
}

/// One pipeline against declarations already in scope, for the executor's
/// `EXPLAIN <query>;`, which holds these maps (params folded) and hands them over.
pub fn analyzeOne(
    arena: std.mem.Allocator,
    pipe: ast.Pipeline,
    bindings: *std.StringHashMap(ast.Pipeline),
    connections: *std.StringHashMap(ast.Connection),
    params: *const std.StringHashMap(*const ast.Expr),
    diag: *Diag,
) Error!Plan {
    var ctx = Ctx{ .arena = arena, .bindings = bindings, .connections = connections, .params = params, .diag = diag };
    const outs = try arena.alloc(Output, 1);
    outs[0] = try ctx.analyzeOutput(pipe);
    return .{ .outputs = outs };
}

const Ctx = struct {
    arena: std.mem.Allocator,
    bindings: *std.StringHashMap(ast.Pipeline),
    connections: *std.StringHashMap(ast.Connection),
    params: *const ParamMap,
    lets: ?*ParamMap = null,
    diag: *Diag,
    known: []const KnownTable = &.{},
    issues: ?*std.array_list.Managed(Issue) = null,
    written: []const []const u8 = &.{},
    json_params: ?[]const []const u8 = null,

    /// A table function's body, typed where it is declared with each parameter a null
    /// of its declared type, so one nothing calls is checked too. A problem a call
    /// already reported at the same place is not reported twice.
    fn checkTableFn(self: *Ctx, fd: ast.FnDecl, tc: ast.TableCheck) Error!void {
        var scoped = ParamMap.init(self.arena);
        var it = self.params.iterator();
        while (it.next()) |kv| try scoped.put(kv.key_ptr.*, kv.value_ptr.*);
        for (fd.params) |p| {
            const nul = try mk(self.arena, .null_lit);
            try scoped.put(p.name, if (p.ty) |ty| try mk(self.arena, .{ .cast = .{ .e = @constCast(nul), .ty = ty.asNullable() } }) else nul);
        }
        const outer = self.params;
        self.params = &scoped;
        defer self.params = outer;
        var saved = std.array_list.Managed(struct { name: []const u8, prev: ?ast.Pipeline }).init(self.arena);
        defer {
            var i = saved.items.len;
            while (i > 0) {
                i -= 1;
                const sv = saved.items[i];
                if (sv.prev) |p| self.bindings.put(sv.name, p) catch {} else _ = self.bindings.remove(sv.name);
            }
        }
        for (tc.bindings) |b| switch (b) {
            .binding => |bd| {
                try saved.append(.{ .name = bd.name, .prev = self.bindings.get(bd.name) });
                try self.bindings.put(bd.name, bd.pipeline);
            },
            .let_const => |l| try scoped.put(l.name, try mk(self.arena, .null_lit)),
            else => {},
        };
        const stages = try self.arena.alloc(ast.Stage, tc.pipeline.stages.len + 1);
        @memcpy(stages[0..tc.pipeline.stages.len], tc.pipeline.stages);
        stages[tc.pipeline.stages.len] = .{ .node = .{ .write = .{ .connector = "stdout", .form = null, .target = "", .mode = .default } }, .hints = &.{}, .pos = tc.pipeline.pos };
        _ = self.analyzeOutput(.{ .stages = stages, .pos = tc.pipeline.pos }) catch |e| {
            if (e == error.OutOfMemory) return e;
            if (self.issues) |list| for (list.items) |is| {
                if (std.mem.eql(u8, is.msg, self.diag.msg) and std.meta.eql(is.pos, self.diag.pos)) {
                    self.diag.* = .{};
                    return;
                }
            };
            return self.note(e);
        };
    }

    /// A query LET is checked where it stands, as the run queries it there, and
    /// `$name` then reads as a null of its one column's type when that is known.
    fn checkQueryLet(self: *Ctx, l: ast.LetConst) Error!void {
        const q = l.query orelse return;
        const stages = try self.arena.alloc(ast.Stage, q.stages.len + 1);
        @memcpy(stages[0..q.stages.len], q.stages);
        stages[q.stages.len] = .{ .node = .{ .write = .{ .connector = "stdout", .form = null, .target = "", .mode = .default } }, .hints = &.{}, .pos = q.pos };
        const o = try self.attempt(self.analyzeOutput(.{ .stages = stages, .pos = q.pos })) orelse return;
        if (o.stages.len == 0) return;
        const schema = o.stages[o.stages.len - 1].out_schema orelse return;
        if (try queryLetColumnsProblem(self.arena, l.name, schema.fields.len)) |why|
            return self.note(self.failNear(l.pos, "{s}", .{why}));
        const lets = self.lets orelse return;
        const nul = try mk(self.arena, .null_lit);
        try lets.put(l.name, try mk(self.arena, .{ .cast = .{ .e = @constCast(nul), .ty = schema.fields[0].ty.asNullable() } }));
    }

    /// A statement's failure: recorded and the caller goes on, or, without an issue
    /// list, returned as the analysis's error.
    fn note(self: *Ctx, e: Error) Error!void {
        if (e == error.OutOfMemory) return e;
        const list = self.issues orelse return e;
        try list.append(.{ .msg = try self.arena.dupe(u8, self.diag.msg), .pos = self.diag.pos, .end = self.diag.end });
        self.diag.* = .{};
    }

    fn attempt(self: *Ctx, r: Error!Output) Error!?Output {
        return r catch |e| {
            try self.note(e);
            return null;
        };
    }

    /// A CTE's literal arguments, checked where it is declared, since it is only typed
    /// where it is read and its source's columns are known.
    fn checkBindingLiterals(self: *Ctx, p: ast.Pipeline) Error!void {
        for (p.stages) |st| checkStageLiterals(self.diag, st) catch |e| return self.note(e);
    }

    fn knownTable(self: *Ctx, name: []const u8) ?KnownTable {
        for (self.known) |k| if (std.mem.eql(u8, k.name, name)) return k;
        return null;
    }

    /// The statements in run order: a `WITH` is visible to what follows, a body's `WITH`
    /// is scoped to it, and `runForStmt`'s per-row rule is applied once, with a position.
    fn checkStmts(self: *Ctx, stmts: []const ast.Stmt, outs: *std.array_list.Managed(Output), top: bool) Error!void {
        for (stmts) |s| switch (s) {
            .binding => |b| {
                try self.bindings.put(b.name, try self.limitsBound(b.pipeline));
                try self.checkBindingLiterals(b.pipeline);
                try self.checkLeadForm(b.pipeline);
            },
            .output => |p| if (try self.attempt(self.analyzeOutput(p))) |o| try outs.append(o),
            .explain => |e| if (try self.attempt(self.analyzeOutput(e.pipeline))) |o| try outs.append(o),
            .for_each => |fe| {
                try self.checkLoopSource(fe);
                try self.checkBody(fe.body, outs);
            },
            .match => |m| for (m.arms) |arm| try self.checkStmts(arm.body, outs, false),
            .func => |fd| if (!top)
                try self.note(self.failNear(fd.pos, "function `{s}` must be declared at the top level of the script", .{fd.name}))
            else if (fd.body == .stmts) try self.checkBody(fd.body.stmts, outs),
            .let_const => |l| if (!top)
                try self.note(self.failNear(l.pos, "LET `{s}` must be declared at the top level of the script", .{l.name}))
            else
                try self.checkQueryLet(l),
            .param => |pd| if (!top) try self.note(self.failNear(pd.pos, "PARAM `{s}` must be declared at the top level of the script", .{pd.name})),
            .kind, .connection, .call, .throw, .print => {},
        };
    }

    fn checkBody(self: *Ctx, stmts: []const ast.Stmt, outs: *std.array_list.Managed(Output)) Error!void {
        const Saved = struct { name: []const u8, prev: ?ast.Pipeline };
        var saved = std.array_list.Managed(Saved).init(self.arena);
        defer {
            var i = saved.items.len;
            while (i > 0) {
                i -= 1;
                const sv = saved.items[i];
                if (sv.prev) |p| self.bindings.put(sv.name, p) catch {} else _ = self.bindings.remove(sv.name);
            }
        }
        for (stmts) |s| switch (s) {
            .binding => |b| {
                try saved.append(.{ .name = b.name, .prev = self.bindings.get(b.name) });
                try self.bindings.put(b.name, try self.limitsBound(b.pipeline));
                try self.checkBindingLiterals(b.pipeline);
                try self.checkLeadForm(b.pipeline);
            },
            .output => |p| if (try self.attempt(self.analyzeOutput(p))) |o| try outs.append(o),
            .explain => |e| if (try self.attempt(self.analyzeOutput(e.pipeline))) |o| try outs.append(o),
            .for_each => |fe| {
                try self.checkLoopSource(fe);
                try self.checkBody(fe.body, outs);
            },
            .match => |m| for (m.arms) |arm| try self.checkBody(arm.body, outs),
            .call, .throw, .print => {},
            .let_const => |l| try self.note(self.failNear(l.pos, "LET `{s}` must be declared at the top level of the script", .{l.name})),
            .param, .kind, .connection, .func => try self.note(self.failNear(stmtPos(s).?, "{s}", .{body_stmt_rule})),
        };
    }

    /// A FOR EACH's rows as the run discovers them: `$name` must be a JSON PARAM, and
    /// a query whose columns are known must give each loop variable one.
    fn checkLoopSource(self: *Ctx, fe: ast.ForEach) Error!void {
        switch (fe.source) {
            .json_path => |q| {
                const declared = self.json_params orelse return;
                if (!ast.nameIn(declared, q.parts[0]))
                    return self.note(failAt(self.diag, q.span, "{s}", .{try notJsonParamMsg(self.arena, q.parts[0])}));
            },
            .pipeline => |p| {
                if (p.stages.len == 0) return;
                const stages = try self.arena.alloc(ast.Stage, p.stages.len + 1);
                @memcpy(stages[0..p.stages.len], p.stages);
                stages[p.stages.len] = .{ .node = .{ .write = .{ .connector = "stdout", .form = null, .target = "", .mode = .default } }, .hints = &.{}, .pos = p.pos };
                const o = try self.attempt(self.analyzeOutput(.{ .stages = stages, .pos = p.pos })) orelse return;
                const schema = (if (o.stages.len > 0) o.stages[o.stages.len - 1].out_schema else o.source.schema) orelse return;
                if (try loopColumnsProblem(self.arena, schema.fields.len, fe.var_names.len)) |why|
                    return self.note(failPos(self.diag, p.pos, "{s}", .{why}));
            },
            .read => {},
        }
    }

    inline fn failNear(self: *Ctx, pos: ast.Pos, comptime fmt: []const u8, args: anytype) error{AnalyzeFailed} {
        return failPos(self.diag, pos, fmt, args);
    }

    /// `p` with its LIMITs and OFFSETs evaluated as the run does; a problem with one is
    /// noted and the pipeline kept as written.
    fn limitsBound(self: *Ctx, p: ast.Pipeline) Error!ast.Pipeline {
        const stages = bindLimits(self.arena, p.stages, self.params, self.diag) catch |e| {
            try self.note(e);
            return p;
        };
        return .{ .stages = stages, .pos = p.pos, .show = p.show };
    }

    fn analyzeOutput(self: *Ctx, pipe: ast.Pipeline) !Output {
        errdefer self.diag.stamp(pipe.pos);
        if (pipe.stages.len == 0) return fail(self.diag, "empty pipeline", .{});
        const head = try self.inlineHead(try bindLimits(self.arena, pipe.stages, self.params, self.diag));
        const via = head.via;
        const stages = (pushdown.hoistFilters(self.arena, self.arena, head.stages, self.bindings) catch null) orelse head.stages;
        if (stages[stages.len - 1].node != .write)
            return fail(self.diag, "a top-level query must be a `LOAD INTO` or a terminal `SELECT`", .{});
        for (stages) |st| {
            try checkStageLiterals(self.diag, st);
            try checkStageHoles(self.arena, st, self.diag);
        }
        try checkJoinHints(self.arena, stages, self.diag);

        var source = self.resolveSource(stages[0]) catch |e| {
            self.diag.stamp(stages[0].pos);
            return e;
        };
        if (via) |name| source.detail = try msgfmt.allocPrint(self.arena, "{s} (via binding {s})", .{ source.detail, name });

        var top_n: ?pushdown.ExplainedTopN = null;
        if (try self.previewPushdown(stages)) |pv| {
            source.pushdown = pv.where;
            if (try pushdown.explainTopN(self.arena, pv.dialect, pv.bound[0 .. pv.bound.len - 1])) |t| {
                top_n = t;
                source.pushdown = if (source.pushdown.len > 0)
                    try msgfmt.allocPrint(self.arena, "{s}; {s}", .{ source.pushdown, t.text })
                else
                    t.text;
            }
        }

        var stage_infos = std.array_list.Managed(Stage).init(self.arena);
        var has_breaker = false;
        var breakers: usize = 0;
        var map_only = true;
        var sql_fanout = true;
        var seen_breaker = false;
        var cur: ?types.Schema = source.schema;
        for (stages[1 .. stages.len - 1], 1..) |st, si_at| {
            errdefer self.diag.stamp(st.pos);
            var si = try self.stageInfo(st);
            si.sql = profile.stageText(self.arena, st) catch return error.OutOfMemory;
            if (st.node == .join) si.key_pushdown = try self.keyPushNote(stages[0..si_at], st);
            if (st.node == .join) si.join_order = joinorder.staticNote(st, stages[si_at + 1 ..]);
            if (si.breaker) {
                has_breaker = true;
                breakers += 1;
            }
            if (!isMapStage(st.node)) map_only = false;
            switch (st.node) {
                .aggregate, .join => seen_breaker = true,
                .sort, .limit => if (!seen_breaker) {
                    sql_fanout = false;
                },
                else => if (si.breaker) {
                    sql_fanout = false;
                },
            }
            var how: ?[]const u8 = null;
            if (cur) |c| {
                cur = try self.propagate(c, st.node);
                si.out_schema = cur;
                if (cur != null and st.node == .join) how = try self.joinStrategy(c, st.node.join);
            } else try self.checkUnbound(st.node);
            if (how == null and st.node == .join and st.node.join.keyless()) how = "no key: range or nested-loop, decided at run time";
            if (how) |h| {
                si.detail = try msgfmt.allocPrint(self.arena, "{s} ({s})", .{ si.detail, h });
                si.how = h;
            }
            try stage_infos.append(si);
        }

        const w = stages[stages.len - 1].node.write;
        const sink = self.resolveSink(w, stages[stages.len - 1].hints) catch |e| {
            self.diag.stamp(stages[stages.len - 1].pos);
            return e;
        };
        checkBareUpsert(stages, self.connections, self.diag) catch |e| {
            self.diag.stamp(stages[stages.len - 1].pos);
            return e;
        };
        if (w.split) |sp| if (!isSqlSource(source.connector) and !std.mem.eql(u8, source.connector, "union"))
            return failPos(self.diag, sp.pos, "`SPLIT BY` cuts a SQL table read into key ranges; this load reads {s}, which fans out across lanes on its own (`-j`)", .{source.detail});
        if (cur) |c| if (!std.mem.eql(u8, w.connector, "stdout")) uniqueColumns(c, self.diag) catch |e| {
            self.diag.stamp(stages[stages.len - 1].pos);
            return e;
        };
        if (cur) |c| if (recordsSink(w, stages[stages.len - 1].hints)) recordsColumn(c, self.diag) catch |e| {
            self.diag.stamp(stages[stages.len - 1].pos);
            return e;
        };
        var sink_out = sink;
        if (try self.offlineDelta(w, stages[stages.len - 1].hints, cur)) |label| sink_out.mode = try msgfmt.allocPrint(self.arena, "{s}; {s}", .{ sink.mode, label });

        const src_is_sql = isSqlSource(source.connector);
        const sink_is_parallel = isSqlConnector(sink.connector) or
            (if (registry.Connector.parse(sink.connector)) |c| c.streamLoad() else false);
        const splittable = src_is_sql and sql_fanout and splittableRead(stages[0].node);

        return .{
            .source = source,
            .stages = try stage_infos.toOwnedSlice(),
            .sink = sink_out,
            .physical = .{
                .has_breaker = has_breaker,
                .splittable = splittable,
                .sink_parallel = sink_is_parallel,
                .morsel_parallel = !splittable and laneHints(stages[0]) and morselParallelRead(source.connector, stages[0].node),
                .top_n = if (top_n) |t| (if (!t.sorted or breakers == 1) t.rows else null) else null,
            },
        };
    }

    /// A local Delta target planned as a run would, so `check` refuses a table it
    /// cannot write or rows that do not fit it; its label, or null when the target
    /// is remote, templated, or no Delta table.
    fn offlineDelta(self: *Ctx, w: ast.Write, hints: []const ast.Hint, schema: ?types.Schema) !?[]const u8 {
        if (!deltaTarget(w, hints) or std.mem.indexOf(u8, w.target, "://") != null or std.mem.indexOf(u8, w.target, "${") != null) return null;
        const opts = try writeOptions(self.arena, w, hints, self.diag);
        const create = (formatFromHints(hints, self.diag) catch null) == .delta;
        const p = delta_write.plan(self.arena, w.target, delta_write.modeOf(w.mode), create, opts.delta.partition_by, opts.delta.codec, schema) catch |e| {
            if (e == lake.Error.LakeTableRefused) return fail(self.diag, "{s}", .{eval.takeFailure(e) orelse @errorName(e)});
            _ = eval.takeFailure(e);
            return null;
        };
        return try p.label(self.arena);
    }

    /// Every binding's read, a UNION branch's among them, is checked where it is
    /// declared, since a branch is never resolved as a pipeline's lead.
    fn checkLeadForm(self: *Ctx, p: ast.Pipeline) !void {
        if (p.stages.len == 0) return;
        const lead = p.stages[0];
        const connector = switch (lead.node) {
            .read => |rd| if (self.connections.get(rd.connector)) |c| c.connector else rd.connector,
            .union_ => "union",
            else => "",
        };
        checkSourceForm(lead, connector, self.diag) catch |e| {
            self.diag.stamp(lead.pos);
            return e;
        };
    }

    fn resolveSource(self: *Ctx, lead: ast.Stage) !Source {
        switch (lead.node) {
            .read => |rd| {
                const conn: ?ast.Connection = self.connections.get(rd.connector);
                if (rd.form != .builtin and !isBuiltinSource(rd.connector) and conn == null)
                    return fail(self.diag, "{s}", .{try unknownConnectionMsg(self.arena, rd.connector, "read")});
                const connector = if (conn) |c| c.connector else rd.connector;
                if (conn != null and !isSqlSource(connector) and !std.mem.eql(u8, connector, "http"))
                    return fail(self.diag, "{s}", .{try options.unsupportedSourceMsg(self.arena, connector)});
                try checkSourceForm(lead, connector, self.diag);
                const detail = switch (rd.form) {
                    .table => |t| try msgfmt.allocPrint(self.arena, "table {s}", .{lastPart(t)}),
                    .query => "query",
                    .path => |p| p,
                    .builtin => |b| table_builtins.of(b).label,
                    .unit => if (std.mem.eql(u8, connector, "stdin")) "stdin" else "unit",
                };
                if (std.mem.eql(u8, connector, "stdin")) if (try formatFromHints(lead.hints, self.diag)) |f| if (stdinProblem(f)) |why|
                    return fail(self.diag, "{s}", .{why});
                if (rd.form == .builtin) {
                    const tb = table_builtins.of(rd.form.builtin);
                    for (rd.form.builtin.args, tb.args[0..rd.form.builtin.args.len]) |e, kind| {
                        if ((try dollarRefs(self.arena, e)).len == 0 and try table_builtins.argValue(self.arena, kind, e) == null)
                            return fail(self.diag, "{s}", .{tb.arg_msg});
                    }
                }
                if (rd.form == .path and std.mem.eql(u8, connector, "csv") and std.mem.indexOf(u8, rd.form.path, "${") == null) {
                    if (s3.bucketNameError(rd.form.path)) |why|
                        return fail(self.diag, "`{s}` is not a valid S3 source: {s}", .{ rd.form.path, why });
                    const fmt = try formatFromHints(lead.hints, self.diag);
                    if (unreadableTarget(rd.form.path, fmt)) |why|
                        return fail(self.diag, "cannot read `{s}`: {s}", .{ rd.form.path, why });
                    if (archiveProblem(self.arena, rd.form.path, fmt, false)) |why|
                        return fail(self.diag, "cannot read `{s}`: {s}", .{ rd.form.path, why });
                    if (readFormat(rd.form.path, fmt) == .xlsx) _ = try xlsxOptions(lead.hints, self.diag);
                    if (readFormat(rd.form.path, fmt) == .xml) _ = try xmlOptions(lead.hints, &.{}, self.diag);
                    _ = try dialectFromHints(lead.hints, self.diag);
                    _ = try lakeAt(lead.hints, self.diag);
                }
                const rewritten = rd.form == .path and ast.nameIn(self.written, rd.form.path);
                if (!rewritten and rd.form == .path and std.mem.eql(u8, connector, "csv")) {
                    const t = offlineTable(self.arena, rd.form.path, lead.hints) catch |e|
                        return fail(self.diag, "{s}", .{eval.takeFailure(e) orelse @errorName(e)});
                    if (t) |tb| return .{
                        .connector = connector,
                        .detail = try msgfmt.allocPrint(self.arena, "{s} ({s})", .{ detail, tb.label }),
                        .schema = tb.layout.schema,
                        .format = @tagName(tb.kind),
                    };
                }
                const schema = if (rewritten) null else offlineSchema(self.arena, rd, lead.hints);
                return .{ .connector = connector, .detail = detail, .schema = schema };
            },
            .ref => |name| {
                try checkSourceForm(lead, "", self.diag);
                const b = self.bindings.get(name) orelse {
                    if (self.knownTable(name)) |k|
                        return .{ .connector = "session", .detail = try msgfmt.allocPrint(self.arena, "session table {s}", .{name}), .schema = k.schema };
                    return fail(self.diag, "unknown binding `{s}`", .{name});
                };
                var src = try self.resolveSource(b.stages[0]);
                src.schema = try self.bindingSchema(b);
                src.detail = try msgfmt.allocPrint(self.arena, "{s} (via binding {s})", .{ src.detail, name });
                return src;
            },
            .union_ => |un| {
                try checkSourceForm(lead, "union", self.diag);
                const detail = if (un.discover_query.len > 0 or un.discover_pipeline != null)
                    try msgfmt.allocPrint(self.arena, "union (tables discovered from {s})", .{un.discover_conn})
                else
                    try msgfmt.allocPrint(self.arena, "{s} of {d} sources", .{ switch (un.set) {
                        .union_all => "union",
                        .intersect => "intersect",
                        .except => "except",
                    }, un.branches.len });
                const listed = un.discover_query.len == 0 and un.discover_pipeline == null and un.discover_json.len == 0;
                if (listed) if (env_mod.forHintIdent(lead.hints, "canon")) |c| if (std.mem.indexOf(u8, c, "${") == null) {
                    const names = try self.arena.alloc([]const u8, un.branches.len);
                    for (un.branches, names) |b, *n| n.* = unionBranchName(b);
                    const rendered = for (names) |n| {
                        if (std.mem.indexOf(u8, n, "${") != null) break true;
                    } else false;
                    const at = for (lead.hints) |h| {
                        if (std.mem.eql(u8, h.key, "canon")) break h.pos;
                    } else lead.pos;
                    if (!rendered) if (try unionCanonProblem(self.arena, c, names)) |why| return failPos(self.diag, at, "{s}", .{why});
                };
                return .{ .connector = "union", .detail = detail, .schema = null };
            },
            else => return fail(self.diag, "a pipeline must start with `read`, `union`, or a binding reference", .{}),
        }
    }

    fn resolveSink(self: *Ctx, w: ast.Write, hints: []const ast.Hint) !Sink {
        if (std.mem.eql(u8, w.connector, "csv") or std.mem.eql(u8, w.connector, "stdout")) {
            try checkFileSink(w, hints, self.diag);
            try checkSinkForm(w, hints, w.connector, self.diag);
            if (deltaTarget(w, hints))
                return .{ .connector = w.connector, .target = w.target, .mode = try delta_write.modeOf(w.mode).label(self.arena), .format = "delta" };
            return .{ .connector = w.connector, .target = w.target, .mode = @tagName(w.mode) };
        }
        const conn = self.connections.get(w.connector) orelse
            return fail(self.diag, "{s}", .{try unknownConnectionMsg(self.arena, w.connector, "write")});
        try checkSinkForm(w, hints, conn.connector, self.diag);
        try checkSinkConnection(self.arena, conn, self.diag);
        return .{ .connector = conn.connector, .target = w.target, .mode = @tagName(w.mode) };
    }

    /// The schema a binding's pipeline produces; null past anything only the source can
    /// describe.
    fn bindingSchema(self: *Ctx, b: ast.Pipeline) Error!?types.Schema {
        const src = try self.resolveSource(b.stages[0]);
        var cur: ?types.Schema = src.schema orelse return null;
        for (b.stages[1..]) |st| {
            errdefer self.diag.stamp(st.pos);
            if (cur) |c| {
                cur = try self.propagate(c, st.node);
            } else try self.checkUnbound(st.node);
        }
        return cur;
    }

    fn checkUnbound(self: *Ctx, node: ast.Stage.Node) Error!void {
        switch (node) {
            .filter => |p| try self.unboundIn(p),
            .select => |items| for (items) |it| if (it == .computed) try self.unboundIn(it.computed.expr),
            .aggregate => |ag| for (ag.aggs) |a| if (a.arg) |e| try self.unboundIn(e),
            else => {},
        }
    }

    fn unboundIn(self: *Ctx, e: *const ast.Expr) Error!void {
        const q = (try firstDollar(self.arena, try substExpr(self.arena, e, self.params))) orelse return;
        return failAt(self.diag, q.span, "unknown `${s}`: no PARAM, LET or loop variable of that name", .{q.parts[0]});
    }

    /// Output schema after a stage, type-checking along the way; null where the flow
    /// becomes unresolvable. A join is planned with `joinPlan` so later stages are checked
    /// (it once stopped there, and `check` passed a missing column).
    fn propagate(self: *Ctx, in: types.Schema, node: ast.Stage.Node) Error!?types.Schema {
        if (hasDynamicName(node)) return null;
        switch (node) {
            .filter => |p| {
                _ = try checkFilter(self.arena, in, p, self.params, self.diag);
                return in;
            },
            .select => |items| return try schemaOfCols(self.arena, try selectCols(self.arena, in, items, self.params, self.diag)),
            .limit => return in,
            .distinct => |d| {
                if (d.on) |f| _ = try fieldIndices(self.arena, in, f, self.diag);
                return in;
            },
            .sort => |s| {
                const qs = try self.arena.alloc(ast.QualName, s.keys.len);
                for (s.keys, qs) |sk, *q| q.* = sk.field;
                _ = try fieldIndices(self.arena, in, qs, self.diag);
                return in;
            },
            .explode => |ex| return (try explodePlan(self.arena, in, ex, self.diag)).schema,
            .aggregate => |ag| return (try aggregatePlan(self.arena, in, ag, self.params, self.diag)).schema,
            .window => |wd| return try windowSchema(self.arena, in, wd, self.diag),
            .join => |j| {
                const s = (try self.joinSides(in, j)) orelse return null;
                _ = try residualPlan(self.arena, s.left, s.right, s.join, self.params, self.diag);
                return (try joinPlan(self.arena, s.left, s.right, s.join, self.diag)).schema;
            },
            else => return null,
        }
    }

    /// A join's two sides as the plan joins them, its keys placed; null where the
    /// right side's columns are not known before the run.
    fn joinSides(self: *Ctx, in: types.Schema, j: ast.Join) Error!?struct { left: types.Schema, right: types.Schema, join: ast.Join } {
        const right = if (self.bindings.get(j.binding)) |b|
            (try self.bindingSchema(b)) orelse return null
        else if (self.knownTable(j.binding)) |k|
            k.schema orelse return null
        else
            return fail(self.diag, "unknown binding `{s}` in join", .{j.binding});
        const prep = try orientKeys(self.arena, in, right, j, self.diag);
        const l2 = try prepSchema(self.arena, in, prep.left, self.params, self.diag);
        const r2 = try prepSchema(self.arena, right, prep.right, self.params, self.diag);
        return .{ .left = l2, .right = r2, .join = prep.join };
    }

    /// `nested-loop` or `range` for a join with no key, as `plan.buildNLJoin` runs
    /// it; null for a hash join.
    fn joinStrategy(self: *Ctx, in: types.Schema, j: ast.Join) Error!?[]const u8 {
        const s = (try self.joinSides(in, j)) orelse return null;
        if (!s.join.keyless()) return null;
        const rp = (try residualPlan(self.arena, s.left, s.right, s.join, self.params, self.diag)) orelse return "nested-loop";
        return if (try op.nlRangeOf(self.arena, rp.pred, rp.schema, s.left.fields.len) != null) "range" else "nested-loop";
    }

    fn stageInfo(self: *Ctx, st: ast.Stage) !Stage {
        return switch (st.node) {
            .filter => .{ .kind = "filter", .detail = "", .breaker = false },
            .select => |items| .{ .kind = "select", .detail = try self.selectDetail(items), .breaker = false },
            .limit => |l| .{ .kind = "limit", .detail = if (l.count == std.math.maxInt(u64))
                (if (l.offset > 0) "all (offset)" else "all")
            else
                try msgfmt.allocPrint(self.arena, "{d}{s}", .{ l.count, if (l.offset > 0) " (offset)" else "" }), .breaker = false },
            .explode => |e| .{ .kind = "explode", .detail = e.field, .breaker = false },
            .distinct => .{ .kind = "distinct", .detail = "", .breaker = true },
            .sort => |s| .{ .kind = "sort", .detail = try msgfmt.allocPrint(self.arena, "{d} key(s)", .{s.keys.len}), .breaker = true },
            .aggregate => |ag| .{ .kind = "aggregate", .detail = try msgfmt.allocPrint(self.arena, "{d} agg(s), {d} group(s)", .{ ag.aggs.len, ag.by.len }), .breaker = true },
            .join => |j| try self.joinInfo(j),
            .window => |wd| .{ .kind = "window", .detail = try ast.windowDetail(self.arena, wd), .breaker = true },
            .read, .ref, .write, .union_ => fail(self.diag, "unexpected operator in the middle of a pipeline", .{}),
        };
    }

    fn joinInfo(self: *Ctx, j: ast.Join) !Stage {
        if (self.bindings.get(j.binding) == null and self.knownTable(j.binding) == null)
            return fail(self.diag, "unknown binding `{s}` in join", .{j.binding});
        const d = try msgfmt.allocPrint(self.arena, "{s} {s}", .{ @tagName(j.kind), j.binding });
        var st = Stage{ .kind = "join", .detail = d, .breaker = true };
        const b = self.bindings.get(j.binding) orelse return st;
        if (b.stages.len == 0) return st;
        const head = try self.inlineHead(try j.rightStages(self.arena, b.stages));
        const stages = (pushdown.hoistFilters(self.arena, self.arena, head.stages, self.bindings) catch null) orelse head.stages;
        if (stages[0].node != .read) return st;
        var src = self.resolveSource(stages[0]) catch return st;
        st.right_scan = try msgfmt.allocPrint(self.arena, "{s} {s} (via binding {s})", .{ sinkKind(src), src.detail, head.via orelse j.binding });
        if (try self.previewPushdown(stages)) |pv| src.pushdown = pv.where;
        st.right_pushdown = src.pushdown;
        return st;
    }

    /// What `plan.buildJoin` will do for key pushdown, as `EXPLAIN` tells it: a SQL
    /// right side takes the left side's keys, else a SQL left read right before
    /// the first join takes the right side's, else a Parquet file on the right,
    /// else on the left, skips row groups by them.
    fn keyPushNote(self: *Ctx, before: []const ast.Stage, st: ast.Stage) ![]const u8 {
        const j = st.node.join;
        if (keypush.disabled(st.hints)) return "";
        const b = self.bindings.get(j.binding) orelse return "";
        if (b.stages.len == 0) return "";
        const right = (try self.inlineHead(try j.rightStages(self.arena, b.stages))).stages;
        if (keypush.rightMayNarrow(j) and self.sqlSide(right))
            return try msgfmt.allocPrint(self.arena, "the right read takes the left side's keys (the left read ahead up to {d} rows / {d} MB, else none)", .{ op.default_prefetch_rows, op.default_prefetch_bytes >> 20 });
        if (keypush.leftMayNarrow(j) and self.sqlSide(before))
            return "the left read takes the right side's keys";
        if (keypush.rightMayNarrow(j) and parquetFileSide(right))
            return try msgfmt.allocPrint(self.arena, "the right Parquet read skips row groups by the left side's keys (the left read ahead up to {d} rows / {d} MB, else none)", .{ op.default_prefetch_rows, op.default_prefetch_bytes >> 20 });
        if (keypush.leftMayNarrow(j) and parquetFileSide(before))
            return "the left Parquet read skips row groups by the right side's keys";
        return "";
    }

    /// A `.parquet` file, local or read by range, followed by only filters and selects.
    fn parquetFileSide(stages: []const ast.Stage) bool {
        if (stages.len == 0 or stages[0].node != .read) return false;
        const rd = stages[0].node.read;
        if (!std.mem.eql(u8, rd.connector, "csv") or rd.form != .path) return false;
        if (!pqdecode.Reader.isPath(rd.form.path) or ftp.isUrl(rd.form.path)) return false;
        for (stages[1..]) |x| switch (x.node) {
            .filter, .select => {},
            else => return false,
        };
        return true;
    }

    /// A SQL table or query read followed by only filters and selects.
    fn sqlSide(self: *Ctx, stages: []const ast.Stage) bool {
        if (stages.len == 0 or stages[0].node != .read) return false;
        const rd = stages[0].node.read;
        if (rd.form != .table and rd.form != .query) return false;
        const conn = self.connections.get(rd.connector) orelse return false;
        const c = registry.Connector.parse(conn.connector) orelse return false;
        if (c.sqlRead() == null) return false;
        for (stages[1..]) |x| switch (x.node) {
            .filter, .select => {},
            else => return false,
        };
        return true;
    }

    /// The binding chain at the head of `stages` laid out in front of the rest, as
    /// `plan.inlineHeadBindings` does. `via` names the bindings laid out, outermost
    /// first, if any.
    fn inlineHead(self: *Ctx, stages_in: []const ast.Stage) !struct { stages: []const ast.Stage, via: ?[]const u8 } {
        var via: ?[]const u8 = null;
        var head = stages_in;
        var n: usize = 0;
        while (head[0].node == .ref and n < 16) : (n += 1) {
            const b = self.bindings.get(head[0].node.ref) orelse break;
            if (b.stages.len == 0 or (b.stages[0].node != .read and b.stages[0].node != .ref)) break;
            if (for (b.stages) |st| {
                if (st.node == .window) break true;
            } else false) break;
            via = if (via) |v| try msgfmt.allocPrint(self.arena, "{s}, {s}", .{ v, head[0].node.ref }) else head[0].node.ref;
            const joined = try self.arena.alloc(ast.Stage, b.stages.len + head.len - 1);
            @memcpy(joined[0..b.stages.len], b.stages);
            @memcpy(joined[b.stages.len..], head[1..]);
            head = joined;
        }
        return .{ .stages = head, .via = via };
    }

    /// The WHERE the leading SQL read would be sent (raw `PUSHDOWN` AND the contiguous
    /// filters), params in; text comparisons are left to the run, which knows collation.
    fn previewPushdown(self: *Ctx, stages: []const ast.Stage) !?struct { where: []const u8, dialect: Dialect, bound: []const ast.Stage } {
        if (stages[0].node != .read) return null;
        const rd = stages[0].node.read;
        if (rd.form != .table and rd.form != .query) return null;
        const conn = self.connections.get(rd.connector) orelse return null;
        const d = dialectOf(conn.connector) orelse return null;
        var raw: []const u8 = rd.where;
        for (stages[0].hints) |h| {
            if (std.mem.eql(u8, h.key, "where") and h.value == .str) raw = h.value.str;
        }
        const bound = try substFilterParams(self.arena, stages, self.params);
        var wants = false;
        const implicit = pushdown.serialWhereWith(self.arena, d, bound, null, &wants) catch null;
        var where = try composePushdown(self.arena, raw, implicit);
        if (wants) where = if (where.len > 0)
            try msgfmt.allocPrint(self.arena, "{s}; text comparisons decided by the collation at run time", .{where})
        else
            "text comparisons decided by the collation at run time";
        return .{ .where = where, .dialect = d, .bound = bound };
    }

    fn selectDetail(self: *Ctx, items: []const ast.SelectItem) ![]const u8 {
        var buf = std.array_list.Managed(u8).init(self.arena);
        for (items, 0..) |item, i| {
            if (i > 0) try buf.appendSlice(", ");
            switch (item) {
                .star => try buf.appendSlice("*"),
                .star_except => try buf.appendSlice("* except (…)"),
                .star_rename => try buf.appendSlice("* rename (…)"),
                .star_of => |so| try buf.appendSlice(if (so.rel.len > 0) try msgfmt.allocPrint(self.arena, "{s}.*", .{so.rel}) else "*"),
                .field => |q| try buf.appendSlice(lastPart(q)),
                .computed => |c| try buf.appendSlice(c.name),
            }
        }
        return buf.toOwnedSlice();
    }
};

pub const append_unsupported_fmt = "`APPEND` into `{s}` is not supported: {s}. Use `REPLACE`, or write each run to its own path";

/// What a file or stdout sink is refused for, shared by `check` and the run's
/// planner so the two can never disagree about a target.
pub fn checkFileSink(w: ast.Write, hints: []const ast.Hint, diag: *Diag) Error!void {
    if (std.mem.indexOf(u8, w.target, "${") == null) if (s3.bucketNameError(w.target)) |why|
        return fail(diag, "`{s}` is not a valid S3 target: {s}", .{ w.target, why });
    if (ftp.isUrl(w.target))
        return fail(diag, "cannot write `{s}`: basalt reads from FTP but does not write to it; write to `sftp://` or a local file", .{w.target});
    const to_file = std.mem.eql(u8, w.connector, "csv") and w.target.len > 0;
    if (to_file) {
        const fmt = try formatFromHints(hints, diag);
        if (unwritableTarget(w.target, fmt) orelse unreadableTarget(w.target, fmt)) |why|
            return fail(diag, "cannot write `{s}`: {s}", .{ w.target, why });
    }
    _ = try dialectFromHints(hints, diag);
    const encodes = to_file and ast.nameIn(formats.of((try formatFromHints(hints, diag)) orelse formatOfPath(w.target) orelse .csv).write_keys, "encoding");
    if (hintText(hints, "encoding") != null and !encodes)
        return fail(diag, "`encoding` applies to a read; a CSV sink always writes UTF-8", .{});
    if (to_file) if (try writeFormat(w, hints, diag)) |f| if (folder.isFolder(w.target) and f != .delta)
        return fail(diag, "cannot write `{s}`: a folder is written as a Delta table, not as {s} files; drop `format`, or name a file", .{ w.target, formats.of(f).label });
    if (deltaTarget(w, hints)) {
        if (delta_write.storeProblem(w.target)) |why| return fail(diag, "cannot write Delta table `{s}`: {s}", .{ w.target, why });
        return;
    }
    if (w.mode == .upsert)
        return fail(diag, "`UPSERT` needs a table to merge into; a file is written whole — use `REPLACE` (the default) or `APPEND`", .{});
    if (w.mode == .append) {
        const fmt = if (std.mem.eql(u8, w.connector, "csv")) try formatFromHints(hints, diag) else null;
        if (appendUnsupported(w.target, fmt)) |why|
            return fail(diag, append_unsupported_fmt, .{ w.target, why });
    }
}

const file_sink_keys: []const []const u8 = formats.all_write_keys ++ .{"encoding"};
const stream_sink_keys = [_][]const u8{ "label_prefix", "compression" };
const table_sink_keys = [_][]const u8{ "compression", "cutover" };

/// Each file format's write keys, `encoding` beside them for `checkFileSink` to refuse
/// with its own reason, and how a message names the target and its keys.
const file_writes = blk: {
    var out: [formats.table.len]struct { keys: []const []const u8, what: []const u8, takes: []const u8 } = undefined;
    for (formats.table, &out) |f, *o| o.* = .{
        .keys = f.write_keys ++ .{"encoding"},
        .what = f.read_what[0 .. f.read_what.len - " read".len] ++ (if (std.mem.endsWith(u8, f.read_what, "table read")) "" else " file"),
        .takes = formats.takes(f.write_keys),
    };
    break :blk out;
};

/// The file a write option belongs to, for a message refusing it on another; null
/// for one no file write takes.
fn ownerOf(key: []const u8) ?[]const u8 {
    if (std.mem.eql(u8, key, "format")) return null;
    for (formats.table, file_writes) |f, fw| if (ast.nameIn(f.write_keys, key)) return fw.what;
    if (ast.nameIn(&stream_sink_keys, key)) return "a starrocks or doris table";
    if (ast.nameIn(&table_sink_keys, key)) return "a database table";
    return null;
}

/// `USING` names a load path, and only StarRocks and Doris have one to name; a
/// `WITH` key the target's kind does not read is refused rather than ignored.
pub fn checkSinkForm(w: ast.Write, hints: []const ast.Hint, connector: []const u8, diag: *Diag) Error!void {
    const file = std.mem.eql(u8, connector, "csv") or std.mem.eql(u8, connector, "stdout");
    const stream = if (registry.Connector.parse(connector)) |c| c.streamLoad() else false;
    const to_file = file and w.target.len > 0;
    const fw = if (to_file) &file_writes[@intFromEnum((writeFormat(w, hints, diag) catch null) orelse .csv)] else null;
    const keys: []const []const u8 = if (fw) |f| f.keys else if (file) file_sink_keys else if (stream) &stream_sink_keys else &table_sink_keys;
    for (hints) |h| {
        if (ast.nameIn(keys, h.key)) continue;
        const e = if (fw) |f| (if (ownerOf(h.key)) |owner|
            fail(diag, "`LOAD INTO` option `{s}` does not apply to {s}, which takes {s}; `{s}` is an option of {s}", .{ h.key, f.what, f.takes, h.key, owner })
        else
            fail(diag, "unknown `LOAD INTO` option `{s}` ({s} takes {s})", .{ h.key, f.what, f.takes })) else if (ast.nameIn(file_sink_keys, h.key) or ast.nameIn(&stream_sink_keys, h.key))
            fail(diag, "`LOAD INTO` option `{s}` does not apply to a {s} target (a file takes `format` and its format's options; a starrocks or doris table `label_prefix`)", .{ h.key, if (file) "file" else connector })
        else
            fail(diag, "unknown `LOAD INTO` option `{s}` (a file takes `format` and its format's options; a starrocks or doris table `label_prefix`)", .{h.key});
        diag.pos = h.pos;
        return e;
    }
    var scratch = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer scratch.deinit();
    _ = try writeOptions(scratch.allocator(), w, hints, diag);
    _ = try cutoverProblem(w, hints, diag);
    const form = w.form orelse return;
    if (stream and std.mem.eql(u8, form, "stream_load")) return;
    if (stream) return fail(diag, "unknown load path `USING {s}`; a {s} target loads by `stream_load`", .{ form, connector });
    return fail(diag, "`USING {s}` does not apply here: only a starrocks or doris target takes `USING stream_load`", .{form});
}

const any_read_keys = [_][]const u8{ "where", "canon", "buffer" };

const rest_read_keys = [_][]const u8{
    "items",          "method",        "body",         "body_type",   "header",    "bearer",
    "bearer_env",     "auth",          "auth_env",     "user_env",    "pass_env",  "paginate",
    "page_param",     "start_page",    "start_offset", "size_param",  "page_size", "cursor_param",
    "cursor_field",   "total_field",   "max_pages",    "stop_short",  "prefetch",  "retries",
    "retry_statuses", "retry_base_ms", "timeout_ms",   "progress_ms",
};
const union_read_keys = [_][]const u8{ "tag", "table_field", "tag_field", "tag_substr" };

const ReadKind = struct { what: []const u8, keys: []const []const u8, takes: []const u8 };

/// Each file format's read, its keys from `formats.zig`.
const file_reads = blk: {
    var out: [formats.table.len]ReadKind = undefined;
    for (formats.table, &out) |f, *r| r.* = .{ .what = f.read_what, .keys = f.read_keys, .takes = formats.takes(f.read_keys) };
    break :blk out;
};
const stdin_read = ReadKind{ .what = "a STDIN read", .keys = formats.stdin_read_keys, .takes = formats.takes(formats.stdin_read_keys) };
const rest_read = ReadKind{ .what = "a REST read", .keys = &rest_read_keys, .takes = "items, paginate, bearer_env and the other REST options in docs/language/sources.md" };
const sql_read = ReadKind{ .what = "a SQL table read", .keys = &[_][]const u8{ "split", "splits", "split_kind" }, .takes = "split, splits, split_kind" };
const union_read = ReadKind{ .what = "a UNION", .keys = &union_read_keys, .takes = "table_field, tag_field, tag_substr" };
const other_read = ReadKind{ .what = "this source", .keys = &.{}, .takes = "no options" };

/// A read's `WITH` key that its kind of source does not read is refused rather
/// than ignored, so a misspelled `delimeter` fails instead of reading commas.
/// `connector` is the resolved source type: a file is `csv` whatever its format.
pub fn checkSourceForm(lead: ast.Stage, connector: []const u8, diag: *Diag) Error!void {
    if (lead.node == .read and (std.mem.eql(u8, connector, "csv") or std.mem.eql(u8, connector, "stdin")))
        if (formatFromHints(lead.hints, diag) catch null) |f| if (formats.readRefused(f)) |why| {
            if (lead.node.read.form == .path) return fail(diag, "cannot read `{s}`: {s}", .{ lead.node.read.form.path, why });
            return fail(diag, "{s}", .{why});
        };
    const kind: ReadKind = switch (lead.node) {
        .read => |rd| if (std.mem.eql(u8, connector, "csv")) blk: {
            const fmt = (formatFromHints(lead.hints, diag) catch null) orelse
                (if (rd.form == .path) formatOfPath(rd.form.path) else null);
            break :blk file_reads[@intFromEnum(fmt orelse .csv)];
        } else if (std.mem.eql(u8, connector, "stdin"))
            stdin_read
        else if (std.mem.eql(u8, connector, "http"))
            rest_read
        else if (isSqlSource(connector))
            sql_read
        else
            other_read,
        .union_ => union_read,
        else => other_read,
    };
    const plain_folder = lead.node == .read and lead.node.read.form == .path and folder.isFolder(lead.node.read.form.path) and
        std.mem.eql(u8, connector, "csv") and (formatFromHints(lead.hints, diag) catch null) == null;
    for (lead.hints) |h| {
        if (ast.nameIn(kind.keys, h.key) or ast.nameIn(&any_read_keys, h.key)) continue;
        if (plain_folder and std.mem.eql(u8, h.key, "version")) continue;
        if (std.mem.eql(u8, connector, "http") and std.mem.startsWith(u8, h.key, "query:")) continue;
        const e = if (ast.nameIn(&join_hint_keys, h.key))
            fail(diag, "`{s}` is an option of a JOIN clause: write it in the `WITH (...)` after the joined source", .{h.key})
        else if (ast.nameIn(formats.all_read_keys, h.key) or ast.nameIn(&rest_read_keys, h.key) or ast.nameIn(&union_read_keys, h.key) or ast.nameIn(sql_read.keys, h.key))
            fail(diag, "read option `{s}` does not apply to {s}, which takes {s}", .{ h.key, kind.what, kind.takes })
        else
            fail(diag, "unknown read option `{s}`: {s} takes {s}", .{ h.key, kind.what, kind.takes });
        diag.pos = h.pos;
        return e;
    }
}

const join_hint_keys = [_][]const u8{ "max_build", "join_order", "key_pushdown" };

pub const connection_types = "http, postgres, mysql, sqlserver, starrocks, doris, sftp, smb, ftp";

/// A `CREATE CONNECTION ... TYPE` that names a connector; the built-in sources
/// (files, stdin) and the table builtins are never declared.
pub fn isConnectionType(connector: []const u8) bool {
    const c = registry.Connector.parse(connector) orelse return false;
    return !c.isBuiltinSource() or c == .http;
}

fn isBuiltinSource(connector: []const u8) bool {
    const c = registry.Connector.parse(connector) orelse return false;
    return c.isBuiltinSource();
}

fn isSqlConnector(connector: []const u8) bool {
    return registry.SqlKind.parse(connector) != null;
}

fn isSqlSource(connector: []const u8) bool {
    return dialectOf(connector) != null;
}

fn dialectOf(connector: []const u8) ?Dialect {
    const c = registry.Connector.parse(connector) orelse return null;
    return (c.sqlRead() orelse return null).dialect;
}

fn composePushdown(arena: std.mem.Allocator, raw: []const u8, implicit: ?[]const u8) ![]const u8 {
    if (raw.len > 0 and implicit != null)
        return msgfmt.allocPrint(arena, "({s}) AND ({s})", .{ raw, implicit.? });
    if (raw.len > 0) return raw;
    return implicit orelse "";
}

fn isMapStage(node: ast.Stage.Node) bool {
    return switch (node) {
        .filter, .select, .explode => true,
        else => false,
    };
}

/// A `table` (PK introspection) or a `query` with `@[split]`; key and size are
/// checked at run time.
fn splittableRead(node: ast.Stage.Node) bool {
    return switch (node) {
        .read => |rd| switch (rd.form) {
            .table => true,
            .query => false,
            else => false,
        },
        else => false,
    };
}

pub fn lastPart(q: ast.QualName) []const u8 {
    return q.parts[q.parts.len - 1];
}

const parser = @import("../lang/sql_parser.zig");

pub fn parse(a: std.mem.Allocator, src: []const u8) !ast.Program {
    var pd: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    return parser.parseSource(a, src, &pd);
}

test "a float LIMIT rounds half to even through textconv.floatToInt as the old formula did" {
    const old = struct {
        fn rows(f: f64) ?i64 {
            if (!std.math.isFinite(f) or @abs(f) >= 0x1p63) return null;
            const r = if (@abs(f - @trunc(f)) == 0.5) 2 * @round(f / 2) else @round(f);
            return @intFromFloat(r);
        }
    };
    const specials = [_]f64{ 0, -0.0, 0.5, 1.5, 2.5, -0.5, -1.5, -2.5, 0.49999999999999994, 4503599627370495.5, 4503599627370497, 9007199254740994, 0x1p62, 0x1p63, -0x1p63 + 1024, 1e300, std.math.inf(f64), -std.math.inf(f64), std.math.nan(f64), 9.223372036854775e18 };
    for (specials) |f| try std.testing.expectEqual(old.rows(f), textconv.floatToInt(f, .half_even));
    try std.testing.expectEqual(@as(?i64, null), old.rows(-0x1p63));
    try std.testing.expectEqual(@as(?i64, std.math.minInt(i64)), textconv.floatToInt(-0x1p63, .half_even));
    var prng = std.Random.DefaultPrng.init(0x6c696d6974);
    const rand = prng.random();
    for (0..20000) |_| {
        const f = switch (rand.uintLessThan(u8, 3)) {
            0 => (rand.float(f64) - 0.5) * 1000,
            1 => @as(f64, @floatFromInt(rand.intRangeAtMost(i32, -100000, 100000))) / 2,
            else => @as(f64, @bitCast(rand.int(u64))),
        };
        if (f == -0x1p63) continue;
        try std.testing.expectEqual(old.rows(f), textconv.floatToInt(f, .half_even));
    }
}

test {
    _ = @import("analyze/render.zig");
    _ = @import("analyze/schema.zig");
    _ = @import("analyze/targets.zig");
    _ = @import("analyze/holes.zig");
    _ = @import("analyze/tests.zig");
    _ = @import("analyze/testing_util.zig");
}
