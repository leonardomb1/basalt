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
const aggregates = @import("../lang/aggregates.zig");
const expand = @import("../lang/expand.zig");
const types = @import("../lang/types.zig");
const pushdown = @import("pushdown.zig");
const Dialect = @import("../db/sql.zig").Dialect;
const eval = @import("../exec/eval.zig");
const csv = @import("../format/csv.zig");
const pqdecode = @import("../format/parquet/read.zig");
const pqwrite = @import("../format/parquet/write.zig");
const arrowread = @import("../format/arrowread.zig");
const azure = @import("../store/azure.zig");
const s3 = @import("../store/s3.zig");
const zipsrc = @import("../format/zipsrc.zig");
const xlsx = @import("../format/xlsx.zig");
const folder = @import("../connect/folder.zig");
const registry = @import("../connect/registry.zig");
const body_stmt_rule = @import("env.zig").body_stmt_rule;
const analyzeCsv = @import("analyze/testing_util.zig").analyzeCsv;
const expectAnalyzeErr = @import("analyze/testing_util.zig").expectAnalyzeErr;

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
    diag.msg = std.fmt.bufPrint(&diag.buf, fmt, args) catch "analysis error";
    diag.pos = null;
    diag.end = null;
    return error.AnalyzeFailed;
}

/// `fail`, underlining `span` when the offending text has one.
pub fn failAt(diag: *Diag, span: ?ast.Span, comptime fmt: []const u8, args: anytype) error{AnalyzeFailed} {
    const e = fail(diag, fmt, args);
    if (span) |s| {
        diag.pos = s.start;
        diag.end = s.end;
    }
    return e;
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

pub const Col = @import("analyze/schema.zig").Col;
pub const selectCols = @import("analyze/schema.zig").selectCols;
pub const schemaOfCols = @import("analyze/schema.zig").schemaOfCols;
pub const checkFilter = @import("analyze/schema.zig").checkFilter;
pub const fieldIndices = @import("analyze/schema.zig").fieldIndices;
pub const Agg = @import("analyze/schema.zig").Agg;
pub const AggregatePlan = @import("analyze/schema.zig").AggregatePlan;
pub const aggregatePlan = @import("analyze/schema.zig").aggregatePlan;
const aggResultType = @import("analyze/schema.zig").aggResultType;
pub const windowFuncType = @import("analyze/schema.zig").windowFuncType;
pub const windowSchema = @import("analyze/schema.zig").windowSchema;
pub const ExplodePlan = @import("analyze/schema.zig").ExplodePlan;
pub const explodePlan = @import("analyze/schema.zig").explodePlan;
pub const JoinPlan = @import("analyze/schema.zig").JoinPlan;
pub const joinPlan = @import("analyze/schema.zig").joinPlan;
const bindBodyVars = @import("analyze/schema.zig").bindBodyVars;
const typedZero = @import("analyze/schema.zig").typedZero;
pub const render = @import("analyze/render.zig").render;
pub const appendUnsupported = @import("analyze/render.zig").appendUnsupported;
const sinkKind = @import("analyze/render.zig").sinkKind;
pub const FileFormat = @import("analyze/targets.zig").FileFormat;
pub const readFormat = @import("analyze/targets.zig").readFormat;
pub const hintText = @import("analyze/targets.zig").hintText;
pub const dialectFromHints = @import("analyze/targets.zig").dialectFromHints;
pub const formatFromHints = @import("analyze/targets.zig").formatFromHints;
pub const xlsxOptions = @import("analyze/targets.zig").xlsxOptions;
const formatOfPath = @import("analyze/targets.zig").formatOfPath;
pub const formatLabel = @import("analyze/targets.zig").formatLabel;
pub const unwritableTarget = @import("analyze/targets.zig").unwritableTarget;
pub const unreadableTarget = @import("analyze/targets.zig").unreadableTarget;
pub const archiveProblem = @import("analyze/targets.zig").archiveProblem;
pub const laneHints = @import("analyze/targets.zig").laneHints;
const morselParallelRead = @import("analyze/targets.zig").morselParallelRead;
const offlineSchema = @import("analyze/targets.zig").offlineSchema;

pub const Source = struct {
    connector: []const u8,
    detail: []const u8,
    schema: ?types.Schema = null,
    pushdown: []const u8 = "",
};

pub const Sink = struct {
    connector: []const u8,
    target: []const u8,
    mode: []const u8,
};

pub const Stage = struct {
    kind: []const u8,
    detail: []const u8,
    breaker: bool,
    right_scan: ?[]const u8 = null,
    right_pushdown: []const u8 = "",
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
    kind: []const u8,
    outputs: []const Output,
};

/// Only a literal `true` fires (null is not a failure, as in SQL), and then the
/// script's own message is the diagnostic, so `check` rejects exactly what a run would.
fn checkThrow(arena: std.mem.Allocator, t: ast.Throw, params: *const ParamMap, diag: *Diag) Error!void {
    if (t.when) |w| {
        const c = eval.constEval(arena, try substExpr(arena, w, params), &.{}, &.{}) catch
            return fail(diag, "THROW condition is not decidable at plan time", .{});
        if (!(c == .bool and c.bool)) return;
    }
    const m = eval.constEval(arena, try substExpr(arena, t.message, params), &.{}, &.{}) catch
        return fail(diag, "THROW message is not decidable at plan time", .{});
    return fail(diag, "{s}", .{try eval.valueToString(arena, m)});
}

pub const ParamOverride = struct { name: []const u8, value: []const u8 };

/// The literal a `-p` string stands for, typed by the PARAM's declared type. Anything
/// not scalar keeps the declared default: `check` is offline.
fn overrideExpr(arena: std.mem.Allocator, ty: types.Type, raw: []const u8) Error!?*const ast.Expr {
    return switch (ty.kind) {
        .int => mk(arena, .{ .int_lit = std.fmt.parseInt(i64, raw, 10) catch return null }),
        .float => mk(arena, .{ .float_lit = std.fmt.parseFloat(f64, raw) catch return null }),
        .bool => mk(arena, .{ .bool_lit = std.mem.eql(u8, raw, "true") }),
        .string, .bytes => mk(arena, .{ .str_lit = raw }),
        .date, .time, .timestamp, .decimal => typedParam(arena, ty, try mk(arena, .{ .str_lit = raw })),
        else => null,
    };
}

/// A DATE, TIME, TIMESTAMP or DECIMAL param is its text CAST to the declared type
/// (`env.mkLit`), so `date_add('day', 1, $d)` types as the run does.
fn typedParam(arena: std.mem.Allocator, ty: types.Type, e: *const ast.Expr) Error!*const ast.Expr {
    return switch (ty.kind) {
        .date, .time, .timestamp, .decimal => mk(arena, .{ .cast = .{ .e = @constCast(e), .ty = ty } }),
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
        p = .{ .kind = "batch", .outputs = &.{} };
    };
    return p;
}

fn analyzeInner(arena: std.mem.Allocator, raw_program: ast.Program, opts: Options, diag: *Diag) error{ AnalyzeFailed, OutOfMemory }!Plan {
    const cli = opts.overrides;
    var expand_msg: []const u8 = "";
    const json_args = try arena.alloc(expand.JsonArg, cli.len);
    for (cli, json_args) |o, *ja| ja.* = .{ .name = o.name, .text = o.value };
    const program = expand.expandProgramWith(arena, raw_program, null, json_args, &expand_msg) catch |e| switch (e) {
        error.OutOfMemory => return error.OutOfMemory,
        error.ExpandFailed => return fail(diag, "{s}", .{expand_msg}),
    };
    if (program.stmts.len == 0 or program.stmts[0] != .kind)
        return fail(diag, "script must begin with a @kind tag", .{});
    const kind_name = @tagName(program.stmts[0].kind.kind);

    var bindings = std.StringHashMap(ast.Pipeline).init(arena);
    var connections = std.StringHashMap(ast.Connection).init(arena);
    for (program.stmts[1..]) |s| if (s == .connection) {
        if (!isConnectionType(s.connection.connector)) {
            const e = fail(diag, "unknown connection type `{s}` (one of {s})", .{ s.connection.connector, connection_types });
            diag.pos = s.connection.pos;
            return e;
        }
        try connections.put(s.connection.name, s.connection);
    };
    if (!opts.declarations_only and countOutputs(program.stmts[1..]) == 0)
        return fail(diag, "no output pipeline (a pipeline ending in `write`)", .{});

    var params_map = ParamMap.init(arena);
    for (program.stmts) |s| if (s == .param) {
        const p = s.param;
        var bound: ?*const ast.Expr = null;
        var text: ?[]const u8 = null;
        for (cli) |o| if (std.mem.eql(u8, o.name, p.name)) {
            bound = try overrideExpr(arena, p.ty, o.value);
            text = o.value;
        };
        if (text == null) if (p.default) |d| if (d.* == .str_lit) {
            text = d.str_lit;
        };
        switch (p.ty.kind) {
            .date, .time, .timestamp, .decimal => if (text) |t| {
                _ = eval.castValueTyped(arena, .{ .string = t }, p.ty) catch
                    return fail(diag, "PARAM `{s}`: `{s}` is not a {s}", .{ p.name, t, try p.ty.name(arena) });
            },
            else => {},
        }
        try params_map.put(p.name, bound orelse try typedParam(arena, p.ty, if (p.default) |d| d else try typedZero(arena, p.ty)));
    };
    for (program.stmts) |s| if (s == .let_const) {
        const l = s.let_const;
        if (params_map.contains(l.name))
            return fail(diag, "`{s}` is declared twice: LET and PARAM share one name space", .{l.name});
        if (l.expr) |le| {
            try params_map.put(l.name, try substExpr(arena, le, &params_map));
        } else {
            const ph = try arena.create(ast.Expr);
            ph.* = .null_lit;
            try params_map.put(l.name, ph);
        }
    };
    for (program.stmts) |s| if (s == .throw) try checkThrow(arena, s.throw, &params_map, diag);
    try bindBodyVars(arena, program.stmts, &params_map);

    var ctx = Ctx{ .arena = arena, .bindings = &bindings, .connections = &connections, .params = &params_map, .diag = diag, .known = opts.known_tables, .issues = opts.issues };

    var out_plans = std.array_list.Managed(Output).init(arena);
    try ctx.checkStmts(program.stmts[1..], &out_plans, true);

    return .{ .kind = kind_name, .outputs = try out_plans.toOwnedSlice() };
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
        .null_lit, .bool_lit, .int_lit, .float_lit, .str_lit, .lambda_var => false,
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

fn countOutputs(stmts: []const ast.Stmt) usize {
    var n: usize = 0;
    for (stmts) |s| n += switch (s) {
        .output, .explain => 1,
        .for_each => |fe| countOutputs(fe.body),
        .match => |m| blk: {
            var k: usize = 0;
            for (m.arms) |arm| k += countOutputs(arm.body);
            break :blk k;
        },
        .func => |fd| if (fd.body == .stmts) countOutputs(fd.body.stmts) else 0,
        else => 0,
    };
    return n;
}

pub fn stmtPos(s: ast.Stmt) ?ast.Pos {
    return switch (s) {
        .kind => |k| k.pos,
        .param => |p| p.pos,
        .connection => |c| c.pos,
        .binding => |b| b.pos,
        .output => |p| p.pos,
        .for_each => |fe| fe.pos,
        .match => |m| m.pos,
        .func => |fd| fd.pos,
        .let_const => |l| l.pos,
        .print => |p| p.pos,
        .call => |c| c.pos,
        .throw => |t| t.pos,
        .explain => |e| e.pos,
    };
}

/// One pipeline against declarations already in scope, for the executor's
/// `EXPLAIN <query>;`, which holds these maps (params folded) and hands them over.
pub fn analyzeOne(
    arena: std.mem.Allocator,
    kind_name: []const u8,
    pipe: ast.Pipeline,
    bindings: *std.StringHashMap(ast.Pipeline),
    connections: *std.StringHashMap(ast.Connection),
    params: *const std.StringHashMap(*const ast.Expr),
    diag: *Diag,
) Error!Plan {
    var ctx = Ctx{ .arena = arena, .bindings = bindings, .connections = connections, .params = params, .diag = diag };
    const outs = try arena.alloc(Output, 1);
    outs[0] = try ctx.analyzeOutput(pipe);
    return .{ .kind = kind_name, .outputs = outs };
}

const Ctx = struct {
    arena: std.mem.Allocator,
    bindings: *std.StringHashMap(ast.Pipeline),
    connections: *std.StringHashMap(ast.Connection),
    params: *const ParamMap,
    diag: *Diag,
    known: []const KnownTable = &.{},
    issues: ?*std.array_list.Managed(Issue) = null,

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
                try self.bindings.put(b.name, b.pipeline);
                try self.checkBindingLiterals(b.pipeline);
            },
            .output => |p| if (try self.attempt(self.analyzeOutput(p))) |o| try outs.append(o),
            .explain => |e| if (try self.attempt(self.analyzeOutput(e.pipeline))) |o| try outs.append(o),
            .for_each => |fe| try self.checkBody(fe.body, outs),
            .match => |m| for (m.arms) |arm| try self.checkStmts(arm.body, outs, false),
            .func => |fd| if (!top)
                try self.note(self.failAt(fd.pos, "function `{s}` must be declared at the top level of the script", .{fd.name}))
            else if (fd.body == .stmts) try self.checkBody(fd.body.stmts, outs),
            .let_const => |l| if (!top) try self.note(self.failAt(l.pos, "LET `{s}` must be declared at the top level of the script", .{l.name})),
            .param => |pd| if (!top) try self.note(self.failAt(pd.pos, "PARAM `{s}` must be declared at the top level of the script", .{pd.name})),
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
                try self.bindings.put(b.name, b.pipeline);
                try self.checkBindingLiterals(b.pipeline);
            },
            .output => |p| if (try self.attempt(self.analyzeOutput(p))) |o| try outs.append(o),
            .explain => |e| if (try self.attempt(self.analyzeOutput(e.pipeline))) |o| try outs.append(o),
            .for_each => |fe| try self.checkBody(fe.body, outs),
            .match => |m| for (m.arms) |arm| try self.checkBody(arm.body, outs),
            .call, .throw, .print => {},
            .let_const => |l| try self.note(self.failAt(l.pos, "LET `{s}` must be declared at the top level of the script", .{l.name})),
            .param, .kind, .connection, .func => try self.note(self.failAt(stmtPos(s).?, "{s}", .{body_stmt_rule})),
        };
    }

    fn failAt(self: *Ctx, pos: ast.Pos, comptime fmt: []const u8, args: anytype) error{AnalyzeFailed} {
        const e = fail(self.diag, fmt, args);
        self.diag.stamp(pos);
        return e;
    }

    fn analyzeOutput(self: *Ctx, pipe: ast.Pipeline) !Output {
        errdefer self.diag.stamp(pipe.pos);
        if (pipe.stages.len == 0) return fail(self.diag, "empty pipeline", .{});
        const head = try self.inlineHead(pipe.stages);
        const via = head.via;
        const stages = (pushdown.hoistFilters(self.arena, self.arena, head.stages, self.bindings) catch null) orelse head.stages;
        if (stages[stages.len - 1].node != .write)
            return fail(self.diag, "a top-level pipeline must end in `write`", .{});
        for (stages) |st| try checkStageLiterals(self.diag, st);

        var source = self.resolveSource(stages[0]) catch |e| {
            self.diag.stamp(stages[0].pos);
            return e;
        };
        if (via) |name| source.detail = try std.fmt.allocPrint(self.arena, "{s} (via binding {s})", .{ source.detail, name });

        var top_n: ?pushdown.ExplainedTopN = null;
        if (try self.previewPushdown(stages)) |pv| {
            source.pushdown = pv.where;
            if (try pushdown.explainTopN(self.arena, pv.dialect, pv.bound[0 .. pv.bound.len - 1])) |t| {
                top_n = t;
                source.pushdown = if (source.pushdown.len > 0)
                    try std.fmt.allocPrint(self.arena, "{s}; {s}", .{ source.pushdown, t.text })
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
        for (stages[1 .. stages.len - 1]) |st| {
            errdefer self.diag.stamp(st.pos);
            var si = try self.stageInfo(st);
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
            if (cur) |c| {
                cur = try self.propagate(c, st.node);
                si.out_schema = cur;
            } else try self.checkUnbound(st.node);
            try stage_infos.append(si);
        }

        const w = stages[stages.len - 1].node.write;
        const sink = self.resolveSink(w, stages[stages.len - 1].hints) catch |e| {
            self.diag.stamp(stages[stages.len - 1].pos);
            return e;
        };

        const src_is_sql = isSqlSource(source.connector);
        const sink_is_parallel = isSqlConnector(sink.connector) or
            (if (registry.Connector.parse(sink.connector)) |c| c.streamLoad() else false);
        const splittable = src_is_sql and sql_fanout and splittableRead(stages[0].node);

        return .{
            .source = source,
            .stages = try stage_infos.toOwnedSlice(),
            .sink = sink,
            .physical = .{
                .has_breaker = has_breaker,
                .splittable = splittable,
                .sink_parallel = sink_is_parallel,
                .morsel_parallel = !splittable and laneHints(stages[0]) and morselParallelRead(source.connector, stages[0].node),
                .top_n = if (top_n) |t| (if (!t.sorted or breakers == 1) t.rows else null) else null,
            },
        };
    }

    fn resolveSource(self: *Ctx, lead: ast.Stage) !Source {
        switch (lead.node) {
            .read => |rd| {
                const conn: ?ast.Connection = self.connections.get(rd.connector);
                if (!isBuiltinSource(rd.connector) and conn == null)
                    return fail(self.diag, "unknown connection `{s}` in read", .{rd.connector});
                const connector = if (conn) |c| c.connector else rd.connector;
                const detail = switch (rd.form) {
                    .table => |t| try std.fmt.allocPrint(self.arena, "table {s}", .{lastPart(t)}),
                    .query => "query",
                    .path => |p| p,
                    .request => "request",
                    .buffer => |b| try std.fmt.allocPrint(self.arena, "buffer {s}", .{b.name}),
                    .range => "range",
                    .unit => "unit",
                };
                if (rd.form == .path and std.mem.eql(u8, connector, "csv")) {
                    if (s3.bucketNameError(rd.form.path)) |why|
                        return fail(self.diag, "`{s}` is not a valid S3 source: {s}", .{ rd.form.path, why });
                    const fmt = try formatFromHints(lead.hints, self.diag);
                    if (unreadableTarget(rd.form.path, fmt)) |why|
                        return fail(self.diag, "cannot read `{s}`: {s}", .{ rd.form.path, why });
                    if (archiveProblem(self.arena, rd.form.path, fmt, false)) |why|
                        return fail(self.diag, "cannot read `{s}`: {s}", .{ rd.form.path, why });
                    if (readFormat(rd.form.path, fmt) == .xlsx) _ = try xlsxOptions(lead.hints, self.diag);
                    _ = try dialectFromHints(lead.hints, self.diag);
                }
                const schema = offlineSchema(self.arena, rd, lead.hints);
                return .{ .connector = connector, .detail = detail, .schema = schema };
            },
            .ref => |name| {
                const b = self.bindings.get(name) orelse {
                    if (self.knownTable(name)) |k|
                        return .{ .connector = "session", .detail = try std.fmt.allocPrint(self.arena, "session table {s}", .{name}), .schema = k.schema };
                    return fail(self.diag, "unknown binding `{s}`", .{name});
                };
                var src = try self.resolveSource(b.stages[0]);
                src.schema = try self.bindingSchema(b);
                src.detail = try std.fmt.allocPrint(self.arena, "{s} (via binding {s})", .{ src.detail, name });
                return src;
            },
            .union_ => |un| {
                const detail = if (un.discover_query.len > 0 or un.discover_pipeline != null)
                    try std.fmt.allocPrint(self.arena, "union (tables discovered from {s})", .{un.discover_conn})
                else
                    try std.fmt.allocPrint(self.arena, "{s} of {d} sources", .{ switch (un.set) {
                        .union_all => "union",
                        .intersect => "intersect",
                        .except => "except",
                    }, un.branches.len });
                return .{ .connector = "union", .detail = detail, .schema = null };
            },
            else => return fail(self.diag, "a pipeline must start with `read`, `union`, or a binding reference", .{}),
        }
    }

    fn resolveSink(self: *Ctx, w: ast.Write, hints: []const ast.Hint) !Sink {
        if (std.mem.eql(u8, w.connector, "csv") or std.mem.eql(u8, w.connector, "stdout")) {
            try checkFileSink(w, hints, self.diag);
            try checkSinkForm(w, w.connector, self.diag);
            return .{ .connector = w.connector, .target = w.target, .mode = @tagName(w.mode) };
        }
        const conn = self.connections.get(w.connector) orelse
            return fail(self.diag, "unknown connection `{s}` in write", .{w.connector});
        try checkSinkForm(w, conn.connector, self.diag);
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
        const Find = struct {
            arena: std.mem.Allocator,
            found: *?ast.QualName,
            fn recur(f: @This(), x: *const ast.Expr) Error!*ast.Expr {
                if (x.* == .field) {
                    if (x.field.dollar and f.found.* == null) f.found.* = x.field;
                    return @constCast(x);
                }
                return ast.rebuildExpr(f.arena, x, f, recur);
            }
        };
        var found: ?ast.QualName = null;
        _ = try Find.recur(.{ .arena = self.arena, .found = &found }, try substExpr(self.arena, e, self.params));
        const q = found orelse return;
        const err = fail(self.diag, "unknown `${s}`: no PARAM, LET or loop variable of that name", .{q.parts[0]});
        if (q.span) |sp| {
            self.diag.pos = sp.start;
            self.diag.end = sp.end;
        }
        return err;
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
                const right = if (self.bindings.get(j.binding)) |b|
                    (try self.bindingSchema(b)) orelse return null
                else if (self.knownTable(j.binding)) |k|
                    k.schema orelse return null
                else
                    return fail(self.diag, "unknown binding `{s}` in join", .{j.binding});
                return (try joinPlan(self.arena, in, right, j, self.diag)).schema;
            },
            else => return null,
        }
    }

    fn stageInfo(self: *Ctx, st: ast.Stage) !Stage {
        return switch (st.node) {
            .filter => .{ .kind = "filter", .detail = "", .breaker = false },
            .select => |items| .{ .kind = "select", .detail = try self.selectDetail(items), .breaker = false },
            .limit => |l| .{ .kind = "limit", .detail = try std.fmt.allocPrint(self.arena, "{d}{s}", .{ l.count, if (l.offset > 0) " (offset)" else "" }), .breaker = false },
            .explode => |e| .{ .kind = "explode", .detail = e.field, .breaker = false },
            .distinct => .{ .kind = "distinct", .detail = "", .breaker = true },
            .sort => |s| .{ .kind = "sort", .detail = try std.fmt.allocPrint(self.arena, "{d} key(s)", .{s.keys.len}), .breaker = true },
            .aggregate => |ag| .{ .kind = "aggregate", .detail = try std.fmt.allocPrint(self.arena, "{d} agg(s), {d} group(s)", .{ ag.aggs.len, ag.by.len }), .breaker = true },
            .join => |j| try self.joinInfo(j),
            .window => |wd| .{ .kind = "window", .detail = try std.fmt.allocPrint(self.arena, "{d} fn(s), {d} partition key(s)", .{ wd.funcs.len, wd.partition_by.len }), .breaker = true },
            .read, .ref, .write, .union_ => fail(self.diag, "unexpected operator in the middle of a pipeline", .{}),
        };
    }

    fn joinInfo(self: *Ctx, j: ast.Join) !Stage {
        if (self.bindings.get(j.binding) == null and self.knownTable(j.binding) == null)
            return fail(self.diag, "unknown binding `{s}` in join", .{j.binding});
        const d = try std.fmt.allocPrint(self.arena, "{s} {s}", .{ @tagName(j.kind), j.binding });
        var st = Stage{ .kind = "join", .detail = d, .breaker = true };
        const b = self.bindings.get(j.binding) orelse return st;
        if (b.stages.len == 0) return st;
        const head = try self.inlineHead(try j.rightStages(self.arena, b.stages));
        const stages = (pushdown.hoistFilters(self.arena, self.arena, head.stages, self.bindings) catch null) orelse head.stages;
        if (stages[0].node != .read) return st;
        var src = self.resolveSource(stages[0]) catch return st;
        st.right_scan = try std.fmt.allocPrint(self.arena, "{s}  {s} (via binding {s})", .{ sinkKind(src), src.detail, head.via orelse j.binding });
        if (try self.previewPushdown(stages)) |pv| src.pushdown = pv.where;
        st.right_pushdown = src.pushdown;
        return st;
    }

    /// The binding chain at the head of `stages` laid out in front of the rest, as
    /// `plan.inlineHeadBindings` does. `via` is the first binding laid out, if any.
    fn inlineHead(self: *Ctx, stages_in: []const ast.Stage) !struct { stages: []const ast.Stage, via: ?[]const u8 } {
        var via: ?[]const u8 = null;
        var head = stages_in;
        var n: usize = 0;
        while (head[0].node == .ref and n < 16) : (n += 1) {
            const b = self.bindings.get(head[0].node.ref) orelse break;
            if (b.stages.len == 0 or b.stages[0].node != .read) break;
            if (for (b.stages) |st| {
                if (st.node == .window) break true;
            } else false) break;
            via = via orelse head[0].node.ref;
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
            try std.fmt.allocPrint(self.arena, "{s}; text comparisons decided by the collation at run time", .{where})
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
                .field => |q| try buf.appendSlice(lastPart(q)),
                .computed => |c| try buf.appendSlice(c.name),
            }
        }
        return buf.toOwnedSlice();
    }
};

/// What a file or stdout sink is refused for, shared by `check` and the run's
/// planner so the two can never disagree about a target.
pub fn checkFileSink(w: ast.Write, hints: []const ast.Hint, diag: *Diag) Error!void {
    if (s3.bucketNameError(w.target)) |why|
        return fail(diag, "`{s}` is not a valid S3 target: {s}", .{ w.target, why });
    if (std.mem.eql(u8, w.connector, "csv") and w.target.len > 0) {
        const fmt = try formatFromHints(hints, diag);
        if (unwritableTarget(w.target, fmt) orelse unreadableTarget(w.target, fmt)) |why|
            return fail(diag, "cannot write `{s}`: {s}", .{ w.target, why });
        if ((fmt orelse formatOfPath(w.target)) == .xlsx)
            return fail(diag, "cannot write `{s}`: basalt reads Excel workbooks but does not write them; write a `.csv` or `.parquet`", .{w.target});
    }
    _ = try dialectFromHints(hints, diag);
    if (hintText(hints, "encoding") != null)
        return fail(diag, "`encoding` applies to a read; a CSV sink always writes UTF-8", .{});
    if (w.mode == .append) {
        if (appendUnsupported(w.target)) |why|
            return fail(diag, "`APPEND` into `{s}` is not supported: {s}", .{ w.target, why });
    }
}

/// `USING` names a load path, and only StarRocks and Doris have one to name.
pub fn checkSinkForm(w: ast.Write, connector: []const u8, diag: *Diag) Error!void {
    const form = w.form orelse return;
    const stream = if (registry.Connector.parse(connector)) |c| c.streamLoad() else false;
    if (stream and std.mem.eql(u8, form, "stream_load")) return;
    if (stream) return fail(diag, "unknown load path `USING {s}`; a {s} target loads by `stream_load`", .{ form, connector });
    return fail(diag, "`USING {s}` does not apply here: only a starrocks or doris target takes `USING stream_load`", .{form});
}

pub const connection_types = "http, postgres, mysql, sqlserver, starrocks, doris, sftp, smb";

/// A `CREATE CONNECTION ... TYPE` that names a connector; the built-in sources
/// (files, `range`, a request body) are never declared.
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
        return std.fmt.allocPrint(arena, "({s}) AND ({s})", .{ raw, implicit.? });
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

test {
    _ = @import("analyze/render.zig");
    _ = @import("analyze/schema.zig");
    _ = @import("analyze/targets.zig");
    _ = @import("analyze/tests.zig");
    _ = @import("analyze/testing_util.zig");
}
