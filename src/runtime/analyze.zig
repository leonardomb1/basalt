//! Static analysis: parsed program → validated `Plan` IR, without executing or
//! connecting. Shared groundwork for `EXPLAIN` (render the IR) and `basalt
//! check` (validate and report). It does full structural + reference validation
//! and resolves what it can read locally — a CSV header, a Parquet footer. A
//! schema only the source can describe (a database table, a remote object)
//! stays null and renders as `schema: unresolved`.

const std = @import("std");
const ast = @import("../lang/ast.zig");
const expand = @import("../lang/expand.zig");
const types = @import("../lang/types.zig");
const pushdown = @import("pushdown.zig");
const Dialect = @import("../connect/sql.zig").Dialect;
const eval = @import("../exec/eval.zig");
const csv = @import("../connect/csv.zig");
const pqdecode = @import("../connect/pqdecode.zig");
const pqwrite = @import("../connect/pqwrite.zig");
const azure = @import("../connect/azure.zig");
const s3 = @import("../connect/s3.zig");
const zipsrc = @import("../connect/zipsrc.zig");

pub const Diag = struct {
    buf: [512]u8 = undefined,
    msg: []const u8 = "",
};

pub const Error = error{ AnalyzeFailed, OutOfMemory };

fn fail(diag: *Diag, comptime fmt: []const u8, args: anytype) error{AnalyzeFailed} {
    diag.msg = std.fmt.bufPrint(&diag.buf, fmt, args) catch "analysis error";
    return error.AnalyzeFailed;
}

/// Param name → the literal expression it substitutes to (CLI values for the
/// executor; declared defaults for offline analysis). Deliberately NOT a `pub`
/// named alias — re-exporting a StringHashMap type makes `refAllDeclsRecursive`
/// (the test harness) recurse the whole hashmap decl tree and crash. Callers spell
/// `std.StringHashMap(*const ast.Expr)` directly; it's the same type.
const ParamMap = std.StringHashMap(*const ast.Expr);

/// Deep-copy `expr`, replacing single-name field refs that name a param with its
/// literal. No params ⇒ returns the original (no copy).
const SubstCtx = struct { arena: std.mem.Allocator, params: *const ParamMap };

fn substRecur(ctx: SubstCtx, e: *const ast.Expr) Error!*ast.Expr {
    return @constCast(try substExpr(ctx.arena, e, ctx.params));
}

pub fn substExpr(arena: std.mem.Allocator, expr: *const ast.Expr, params: *const ParamMap) Error!*const ast.Expr {
    if (params.count() == 0) return expr;
    if (expr.* == .field) {
        const q = expr.field;
        if (q.parts.len == 1) if (params.get(q.parts[0])) |lit| return lit;
        return expr;
    }
    return ast.rebuildExpr(arena, expr, SubstCtx{ .arena = arena, .params = params }, substRecur);
}

fn mk(arena: std.mem.Allocator, e: ast.Expr) Error!*const ast.Expr {
    const p = try arena.create(ast.Expr);
    p.* = e;
    return p;
}

fn exprType(arena: std.mem.Allocator, in: types.Schema, e: *const ast.Expr, diag: *Diag) Error!types.Type {
    var ctx = eval.TypeCtx{ .schema = in, .arena = arena };
    return ctx.typeOf(e) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.TypeError => return fail(diag, "{s}", .{ctx.msg}),
    };
}

/// One resolved output column of `select`: either a passthrough of an input index
/// or a computed (already param-substituted) expression, plus its name and type.
pub const Col = struct {
    name: []const u8,
    ty: types.Type,
    source: union(enum) { passthrough: usize, expr: *const ast.Expr },
};

pub fn selectCols(arena: std.mem.Allocator, in: types.Schema, items: []const ast.SelectItem, params: *const ParamMap, diag: *Diag) Error![]Col {
    var cols = std.array_list.Managed(Col).init(arena);
    for (items) |item| switch (item) {
        .star => for (in.fields, 0..) |f, idx| try cols.append(.{ .name = f.name, .ty = f.ty, .source = .{ .passthrough = idx } }),
        .star_except => |names| for (in.fields, 0..) |f, idx| {
            if (nameIn(names, f.name)) continue;
            try cols.append(.{ .name = f.name, .ty = f.ty, .source = .{ .passthrough = idx } });
        },
        .star_rename => |renames| {
            for (renames) |r| if (in.indexOf(r.from) == null)
                return fail(diag, "unknown rename field `{s}`", .{r.from});
            for (in.fields, 0..) |f, idx| {
                const nm = renameTo(renames, f.name) orelse f.name;
                for (in.fields[0..idx]) |g|
                    if (std.mem.eql(u8, nm, renameTo(renames, g.name) orelse g.name))
                        return fail(diag, "`* rename` produces duplicate column `{s}`", .{nm});
                try cols.append(.{ .name = nm, .ty = f.ty, .source = .{ .passthrough = idx } });
            }
        },
        .field => |q| {
            const nm = lastPart(q);
            const idx = in.indexOf(nm) orelse return fail(diag, "unknown field `{s}`", .{nm});
            try cols.append(.{ .name = nm, .ty = in.fields[idx].ty, .source = .{ .passthrough = idx } });
        },
        .computed => |c| {
            const e = try substExpr(arena, c.expr, params);
            const ty = try exprType(arena, in, e, diag);
            try cols.append(.{ .name = c.name, .ty = ty, .source = .{ .expr = e } });
        },
    };
    return cols.toOwnedSlice();
}

pub fn schemaOfCols(arena: std.mem.Allocator, cols: []const Col) Error!types.Schema {
    const fields = try arena.alloc(types.Schema.Field, cols.len);
    for (cols, fields) |c, *f| f.* = .{ .name = c.name, .ty = c.ty };
    return .{ .fields = fields };
}

pub fn checkFilter(arena: std.mem.Allocator, in: types.Schema, pred0: *const ast.Expr, params: *const ParamMap, diag: *Diag) Error!*const ast.Expr {
    const pred = try substExpr(arena, pred0, params);
    const t = try exprType(arena, in, pred, diag);
    if (!(t.kind == .bool or t.unknown)) return fail(diag, "filter predicate must be bool", .{});
    return pred;
}

/// Validate field references (sort keys / distinct keys / group-by) and return
/// their column indices.
pub fn fieldIndices(arena: std.mem.Allocator, in: types.Schema, names: []const ast.QualName, diag: *Diag) Error![]usize {
    const idxs = try arena.alloc(usize, names.len);
    for (names, 0..) |q, i| idxs[i] = in.indexOf(lastPart(q)) orelse return fail(diag, "unknown field `{s}`", .{lastPart(q)});
    return idxs;
}

pub const Agg = struct { func: ast.AggFunc, arg: ?*const ast.Expr, ty: types.Type, name: []const u8, distinct: bool = false };
pub const AggregatePlan = struct { by: []usize, aggs: []Agg, schema: types.Schema };

pub fn aggregatePlan(arena: std.mem.Allocator, in: types.Schema, ag: ast.Aggregate, params: *const ParamMap, diag: *Diag) Error!AggregatePlan {
    var fields = std.array_list.Managed(types.Schema.Field).init(arena);
    const by = try arena.alloc(usize, ag.by.len);
    for (ag.by, 0..) |q, i| {
        const idx = in.indexOf(lastPart(q)) orelse return fail(diag, "unknown group field `{s}`", .{lastPart(q)});
        by[i] = idx;
        try fields.append(.{ .name = lastPart(q), .ty = in.fields[idx].ty });
    }
    const aggs = try arena.alloc(Agg, ag.aggs.len);
    for (ag.aggs, 0..) |item, i| {
        const arg: ?*const ast.Expr = if (item.arg) |a| try substExpr(arena, a, params) else null;
        const ty = try aggResultType(arena, item.func, arg, in, diag);
        aggs[i] = .{ .func = item.func, .arg = arg, .ty = ty, .name = item.name, .distinct = item.distinct };
        try fields.append(.{ .name = item.name, .ty = ty });
    }
    return .{ .by = by, .aggs = aggs, .schema = .{ .fields = try fields.toOwnedSlice() } };
}

fn aggResultType(arena: std.mem.Allocator, func: ast.AggFunc, arg: ?*const ast.Expr, in: types.Schema, diag: *Diag) Error!types.Type {
    switch (func) {
        .count => return types.Type.init(.int),
        else => {
            const a = arg orelse return fail(diag, "this aggregate requires an argument", .{});
            const at = try exprType(arena, in, a, diag);
            return switch (func) {
                .sum => switch (at.kind) {
                    .float => types.Type.init(.float).withNull(true),
                    // A decimal sum stays a decimal: typing it as an int reported
                    // the accumulated *unscaled* integer, so 1.5+2.25+3.125 came
                    // back as 68750.
                    .decimal => at.withNull(true),
                    else => types.Type.init(.int).withNull(true),
                },
                .avg => types.Type.init(.float).withNull(true),
                .min, .max => at.withNull(true),
                .count => unreachable,
            };
        },
    }
}

pub const ExplodePlan = struct { idx: usize, schema: types.Schema };

pub fn explodePlan(arena: std.mem.Allocator, in: types.Schema, ex: ast.Explode, diag: *Diag) Error!ExplodePlan {
    const idx = in.indexOf(ex.field) orelse return fail(diag, "unknown field `{s}`", .{ex.field});
    const fty = in.fields[idx].ty;
    if (!(fty.kind == .string or fty.kind == .bytes))
        return fail(diag, "explode needs a string column (it splits a delimited value)", .{});
    const fields = try arena.alloc(types.Schema.Field, in.fields.len);
    for (in.fields, fields, 0..) |f, *out, i| {
        out.* = if (i == idx) .{ .name = ex.as_name orelse f.name, .ty = types.Type.init(.string) } else f;
    }
    return .{ .idx = idx, .schema = .{ .fields = fields } };
}

pub const JoinPlan = struct {
    lks: []const usize,
    rks: []const usize,
    schema: types.Schema,
    emit_right: bool,
    right_nullable: bool,
    left_nullable: bool,
};

/// Resolve one `a = b` pair against both schemas. The parser orients by alias
/// prefix, which unqualified names don't carry — so a pair may still arrive
/// written right-side-first, and the side each name belongs to is decided here
/// by where it actually resolves. A name living in both schemas is ambiguous.
fn joinPair(left: types.Schema, right: types.Schema, lq: ast.QualName, rq: ast.QualName, diag: *Diag) Error![2]usize {
    const ln = lastPart(lq);
    const rn = lastPart(rq);
    const l_in_l = left.indexOf(ln);
    const l_in_r = right.indexOf(ln);
    const r_in_l = left.indexOf(rn);
    const r_in_r = right.indexOf(rn);

    const as_written = l_in_l != null and r_in_r != null;
    // Written the other way round (`right.k = left.k`).
    const flipped = r_in_l != null and l_in_r != null;
    // Both readings resolve and they name different columns: only the writer
    // knows which side each belongs to. Equal names are the benign case —
    // either reading pairs the same two columns.
    if (as_written and flipped and !std.mem.eql(u8, ln, rn))
        return fail(diag, "join key `{s}` is ambiguous — `{s}` and `{s}` both exist on both sides; qualify them", .{ ln, ln, rn });
    if (as_written) return .{ l_in_l.?, r_in_r.? };
    if (flipped) return .{ r_in_l.?, l_in_r.? };
    if (l_in_l == null and l_in_r == null) return fail(diag, "unknown left join key `{s}`", .{ln});
    if (r_in_r == null and r_in_l == null) return fail(diag, "unknown right join key `{s}`", .{rn});
    if (l_in_l != null and r_in_l != null) return fail(diag, "join key `{s}` is not a column of the joined side", .{rn});
    return fail(diag, "join key `{s}` is not a column of the joined side", .{ln});
}

pub fn joinPlan(arena: std.mem.Allocator, left: types.Schema, right: types.Schema, j: ast.Join, diag: *Diag) Error!JoinPlan {
    if (j.left_keys.len != j.right_keys.len) return fail(diag, "join has mismatched key lists", .{});
    if (j.kind != .cross and j.left_keys.len == 0) return fail(diag, "join needs at least one `ON <column> = <column>` pair", .{});

    const lks = try arena.alloc(usize, j.left_keys.len);
    const rks = try arena.alloc(usize, j.right_keys.len);
    for (j.left_keys, j.right_keys, lks, rks) |lq, rq, *lo, *ro| {
        const pair = try joinPair(left, right, lq, rq, diag);
        lo.* = pair[0];
        ro.* = pair[1];
        const lt = left.fields[pair[0]].ty;
        const rt = right.fields[pair[1]].ty;
        if (types.Type.unify(lt, rt) == null)
            return fail(diag, "join keys `{s}` and `{s}` are not comparable", .{ left.fields[pair[0]].name, right.fields[pair[1]].name });
    }

    const emit_right = (j.kind != .semi and j.kind != .anti);
    const right_nullable = (j.kind == .left or j.kind == .full);
    const left_nullable = (j.kind == .right or j.kind == .full);

    var fields = std.array_list.Managed(types.Schema.Field).init(arena);
    for (left.fields) |f| try fields.append(.{ .name = f.name, .ty = if (left_nullable) f.ty.asNullable() else f.ty });
    if (emit_right) for (right.fields) |f| {
        // The `_r` suffix can collide in turn — a left column literally named
        // `x_r` beside a right `x`, or two right columns that disambiguate onto
        // the same name. Two output fields with one name make the second
        // unreachable, since every lookup goes through `Schema.indexOf`.
        var name = f.name;
        var n: usize = 0;
        while ((types.Schema{ .fields = fields.items }).indexOf(name) != null) : (n += 1) {
            name = if (n == 0)
                try std.fmt.allocPrint(arena, "{s}_r", .{f.name})
            else
                try std.fmt.allocPrint(arena, "{s}_r{d}", .{ f.name, n + 1 });
        }
        try fields.append(.{ .name = name, .ty = if (right_nullable) f.ty.asNullable() else f.ty });
    };
    return .{
        .lks = lks,
        .rks = rks,
        .schema = .{ .fields = try fields.toOwnedSlice() },
        .emit_right = emit_right,
        .right_nullable = right_nullable,
        .left_nullable = left_nullable,
    };
}

fn nameIn(names: []const []const u8, n: []const u8) bool {
    for (names) |x| if (std.mem.eql(u8, x, n)) return true;
    return false;
}

/// The new name for field `n` under a `* rename (...)` list, or null if unrenamed.
fn renameTo(renames: []const ast.SelectItem.Rename, n: []const u8) ?[]const u8 {
    for (renames) |r| if (std.mem.eql(u8, r.from, n)) return r.to;
    return null;
}

/// A literal of the right type (value irrelevant) to stand in for a param during
/// type-flow when it has no declared default.
fn bindBodyVars(arena: std.mem.Allocator, stmts: []const ast.Stmt, map: *ParamMap) Error!void {
    for (stmts) |s| switch (s) {
        .for_each => |fe| {
            for (fe.var_names, 0..) |vn, i| {
                if (map.contains(vn)) continue;
                const ty: ?types.Type = if (i < fe.var_types.len) fe.var_types[i] else null;
                try map.put(vn, if (ty) |t| try typedZero(arena, t) else try mk(arena, .null_lit));
            }
            try bindBodyVars(arena, fe.body, map);
        },
        .func => |fd| if (fd.body == .stmts) {
            for (fd.params) |p| {
                if (map.contains(p.name)) continue;
                try map.put(p.name, if (p.ty) |t| try typedZero(arena, t) else try mk(arena, .null_lit));
            }
            try bindBodyVars(arena, fd.body.stmts, map);
        },
        .match => |m| for (m.arms) |arm| try bindBodyVars(arena, arm.body, map),
        else => {},
    };
}

fn typedZero(arena: std.mem.Allocator, ty: types.Type) Error!*const ast.Expr {
    const e = try arena.create(ast.Expr);
    e.* = switch (ty.kind) {
        .int => .{ .int_lit = 0 },
        .float => .{ .float_lit = 0 },
        .string, .bytes => .{ .str_lit = "" },
        .bool => .{ .bool_lit = false },
        else => .null_lit,
    };
    return e;
}

pub const Source = struct {
    connector: []const u8,
    detail: []const u8,
    /// Resolved column schema, or null when it needs a live connection.
    schema: ?types.Schema = null,
    /// The predicate translated down into the source query (§7 implicit
    /// pushdown + any raw `PUSHDOWN` fragment), or "" when none — so `check -s`
    /// shows the cut line between "descended to the source" and "runs here".
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
    /// Output schema after this stage — filled by the type-flow layer (later).
    out_schema: ?types.Schema = null,
};

pub const Physical = struct {
    has_breaker: bool,
    splittable: bool,
    sink_parallel: bool,
    /// The source divides into per-lane morsels at `-j > 1` — a local CSV into
    /// byte-range chunks, a parquet into row groups. Distinct from `splittable`,
    /// which is the SQL key-range fan-out; a file read reported as neither used to
    /// print `physical: serial` while the run fanned out over 16 lanes.
    morsel_parallel: bool,
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

/// Collect every output pipeline reachable in a statement block (a `for` or
/// `match` arm body), descending through nested `for`/`match` so all branches
/// are type-checked offline.
fn collectStmtOutputs(outputs: *std.array_list.Managed(ast.Pipeline), stmts: []const ast.Stmt) error{OutOfMemory}!void {
    for (stmts) |st| switch (st) {
        .output => |p| try outputs.append(p),
        // An EXPLAIN'd query is checked like any other: a plan-time error in it must
        // fail `check` and `run` whether or not its rows are ever asked for.
        .explain => |e| try outputs.append(e.pipeline),
        .for_each => |fe| try collectStmtOutputs(outputs, fe.body),
        .match => |m| for (m.arms) |arm| try collectStmtOutputs(outputs, arm.body),
        // A statement function's block is checked where it is *declared*, not per
        // `CALL`: the pipelines are the same either way, and their `${param}`
        // placeholders are only resolvable at run time — exactly the deal a `for`
        // body already gets. So the declaration is descended into and `CALL` (whose
        // name/arity/types expansion has already validated) is a no-op here.
        .func => |fd| if (fd.body == .stmts) try collectStmtOutputs(outputs, fd.body.stmts),
        else => {},
    };
}

/// Decide one top-level `THROW` against the folded PARAM/LET values: substitute the
/// bindings into both operands and const-fold them. Only a literal `true` fires (a
/// null condition is not a failure, as in SQL), and when it does the script's own
/// message becomes the diagnostic verbatim — so `basalt check` rejects exactly what
/// a run would, before anything connects.
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

/// A `-p key=value` binding, so `check` decides a `THROW` guard against the same
/// inputs the run would use instead of always against the declared defaults.
pub const ParamOverride = struct { name: []const u8, value: []const u8 };

/// The literal a CLI string stands for, typed by the PARAM's declared type.
/// Anything not scalar keeps the declared default: `check` is offline, and a
/// half-parsed JSON document would be a worse answer than the default.
fn overrideExpr(arena: std.mem.Allocator, ty: types.Type, raw: []const u8) Error!?*const ast.Expr {
    return switch (ty.kind) {
        .int => mk(arena, .{ .int_lit = std.fmt.parseInt(i64, raw, 10) catch return null }),
        .float => mk(arena, .{ .float_lit = std.fmt.parseFloat(f64, raw) catch return null }),
        .bool => mk(arena, .{ .bool_lit = std.mem.eql(u8, raw, "true") }),
        .string, .date, .time, .timestamp, .decimal, .bytes => mk(arena, .{ .str_lit = raw }),
        else => null,
    };
}

pub fn analyze(arena: std.mem.Allocator, raw_program: ast.Program, diag: *Diag) error{ AnalyzeFailed, OutOfMemory }!Plan {
    return analyzeWith(arena, raw_program, &.{}, diag);
}

pub fn analyzeWith(arena: std.mem.Allocator, raw_program: ast.Program, cli: []const ParamOverride, diag: *Diag) error{ AnalyzeFailed, OutOfMemory }!Plan {
    var expand_msg: []const u8 = "";
    const program = expand.expandProgram(arena, raw_program, null, &expand_msg) catch |e| switch (e) {
        error.OutOfMemory => return error.OutOfMemory,
        error.ExpandFailed => return fail(diag, "{s}", .{expand_msg}),
    };
    if (program.stmts.len == 0 or program.stmts[0] != .kind)
        return fail(diag, "script must begin with a @kind tag", .{});
    const kind_name = @tagName(program.stmts[0].kind.kind);

    var bindings = std.StringHashMap(ast.Pipeline).init(arena);
    var connections = std.StringHashMap(ast.Connection).init(arena);
    var outputs = std.array_list.Managed(ast.Pipeline).init(arena);
    for (program.stmts[1..]) |s| switch (s) {
        .binding => |b| try bindings.put(b.name, b.pipeline),
        .connection => |c| try connections.put(c.name, c),
        .output => |p| try outputs.append(p),
        .explain => |e| try outputs.append(e.pipeline),
        .for_each => |fe| try collectStmtOutputs(&outputs, fe.body),
        .match => |m| for (m.arms) |arm| try collectStmtOutputs(&outputs, arm.body),
        .func => |fd| if (fd.body == .stmts) try collectStmtOutputs(&outputs, fd.body.stmts),
        .param, .kind, .call, .let_const, .throw, .print => {},
    };
    if (outputs.items.len == 0)
        return fail(diag, "no output pipeline (a pipeline ending in `write`)", .{});

    var params_map = ParamMap.init(arena);
    for (program.stmts) |s| if (s == .param) {
        const p = s.param;
        var bound: ?*const ast.Expr = null;
        for (cli) |o| if (std.mem.eql(u8, o.name, p.name)) {
            bound = try overrideExpr(arena, p.ty, o.value);
        };
        try params_map.put(p.name, bound orelse if (p.default) |d| d else try typedZero(arena, p.ty));
    };
    // Statement-level `LET`s bind exactly like params — a `$name` ref substitutes
    // the bound expression — but after every PARAM and in declaration order, so a
    // LET body sees all params and only the LETs ahead of it. The executor folds
    // these to a literal; here they stay expressions, which is all the checker
    // needs to type a filter that mentions `$let`.
    for (program.stmts) |s| if (s == .let_const) {
        const l = s.let_const;
        if (params_map.contains(l.name))
            return fail(diag, "`{s}` is declared twice: LET and PARAM share one name space", .{l.name});
        try params_map.put(l.name, try substExpr(arena, l.expr, &params_map));
    };
    // Guards run against params and LETs only, before the body-var placeholders
    // below can make a `$name` resolve to a stand-in the script never sees.
    for (program.stmts) |s| if (s == .throw) try checkThrow(arena, s.throw, &params_map, diag);
    // Body-scoped variables (for-each loop vars, statement-fn params) bind per
    // row/call at run time; for checking, a typed placeholder (or unknown-typed
    // null) keeps `$var`-as-value expressions lenient instead of "unknown field".
    try bindBodyVars(arena, program.stmts, &params_map);

    var ctx = Ctx{ .arena = arena, .bindings = &bindings, .connections = &connections, .params = &params_map, .diag = diag };

    var out_plans = std.array_list.Managed(Output).init(arena);
    for (outputs.items) |pipe| try out_plans.append(try ctx.analyzeOutput(pipe));

    return .{ .kind = kind_name, .outputs = try out_plans.toOwnedSlice() };
}

/// Analyze a single pipeline against declarations already in scope — what the
/// executor's `EXPLAIN <query>;` statement renders. `analyzeWith` derives the same
/// context by walking a whole program; the executor is already holding these maps
/// (with params folded to their bound values), so it hands them over directly.
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

    fn analyzeOutput(self: *Ctx, pipe: ast.Pipeline) !Output {
        // The same rewrite the runtime applies, so the plan `EXPLAIN` prints is the
        // plan that runs — a filter shown below a join really did descend, and one
        // shown above it really did not.
        const stages = (pushdown.hoistThroughJoins(self.arena, self.arena, pipe.stages, self.bindings) catch null) orelse pipe.stages;
        if (stages.len == 0) return fail(self.diag, "empty pipeline", .{});
        if (stages[stages.len - 1].node != .write)
            return fail(self.diag, "a top-level pipeline must end in `write`", .{});

        var source = try self.resolveSource(stages[0]);

        if (stages[0].node == .read) {
            const rd = stages[0].node.read;
            if ((rd.form == .table or rd.form == .query))
                if (self.connections.get(rd.connector)) |conn| {
                    if (dialectOf(conn.connector)) |d| {
                        var raw: []const u8 = rd.where;
                        for (stages[0].hints) |h| {
                            if (std.mem.eql(u8, h.key, "where") and h.value == .str) raw = h.value.str;
                        }
                        const implicit = pushdown.serialWhere(self.arena, d, stages) catch null;
                        source.pushdown = try composePushdown(self.arena, raw, implicit);
                    }
                };
        }

        var stage_infos = std.array_list.Managed(Stage).init(self.arena);
        var has_breaker = false;
        var map_only = true;
        // Tracks whether the shape is one the runtime fans out over key ranges. It
        // dispatches three of them for a SQL source: map-only, an aggregate with a
        // sort/limit tail, and a map+join. Anything else — a DISTINCT, or a sort
        // with no aggregate under it — falls to the serial driver.
        var sql_fanout = true;
        var seen_breaker = false;
        var cur: ?types.Schema = source.schema;
        for (stages[1 .. stages.len - 1]) |st| {
            var si = try self.stageInfo(st);
            if (si.breaker) has_breaker = true;
            if (!isMapStage(st.node)) map_only = false;
            switch (st.node) {
                .aggregate, .join => seen_breaker = true,
                // A sort or a limit is a tail, which both fan-out paths carry; on its
                // own in front of one it is a top-N, and that runs serially.
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
            }
            try stage_infos.append(si);
        }

        const w = stages[stages.len - 1].node.write;
        const sink = try self.resolveSink(w, stages[stages.len - 1].hints);

        const src_is_sql = isSqlConnector(source.connector);
        const sink_is_parallel = isSqlConnector(sink.connector) or std.mem.eql(u8, sink.connector, "starrocks");
        // Not `map_only`: that gate said `serial` for every aggregate over a
        // splittable table, while `runParallelSqlAgg` fans exactly that shape into
        // key-range lanes. It was the SQL half of the same mislabelling fixed for
        // file sources — reported as serial, run in parallel.
        const splittable = src_is_sql and sql_fanout and splittableRead(stages[0].node);

        return .{
            .source = source,
            .stages = try stage_infos.toOwnedSlice(),
            .sink = sink,
            .physical = .{
                .has_breaker = has_breaker,
                .splittable = splittable,
                .sink_parallel = sink_is_parallel,
                .morsel_parallel = !splittable and morselParallelRead(source.connector, stages[0].node),
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
                    if (archiveProblem(self.arena, rd.form.path, fmt)) |why|
                        return fail(self.diag, "cannot read `{s}`: {s}", .{ rd.form.path, why });
                    _ = try dialectFromHints(lead.hints, self.diag);
                }
                const schema = offlineSchema(self.arena, rd, lead.hints);
                return .{ .connector = connector, .detail = detail, .schema = schema };
            },
            .ref => |name| {
                const b = self.bindings.get(name) orelse
                    return fail(self.diag, "unknown binding `{s}`", .{name});
                var src = try self.resolveSource(b.stages[0]);
                if (src.schema) |s0| {
                    var cur: ?types.Schema = s0;
                    for (b.stages[1..]) |st| {
                        if (cur) |c| cur = try self.propagate(c, st.node);
                    }
                    src.schema = cur;
                }
                src.detail = try std.fmt.allocPrint(self.arena, "{s} (via binding {s})", .{ src.detail, name });
                return src;
            },
            .union_ => |un| {
                const detail = if (un.discover_query.len > 0 or un.discover_pipeline != null)
                    try std.fmt.allocPrint(self.arena, "union (tables discovered from {s})", .{un.discover_conn})
                else
                    try std.fmt.allocPrint(self.arena, "union of {d} sources", .{un.branches.len});
                return .{ .connector = "union", .detail = detail, .schema = null };
            },
            else => return fail(self.diag, "a pipeline must start with `read`, `union`, or a binding reference", .{}),
        }
    }

    fn resolveSink(self: *Ctx, w: ast.Write, hints: []const ast.Hint) !Sink {
        if (std.mem.eql(u8, w.connector, "csv") or std.mem.eql(u8, w.connector, "stdout")) {
            if (s3.bucketNameError(w.target)) |why|
                return fail(self.diag, "`{s}` is not a valid S3 target: {s}", .{ w.target, why });
            // A `stdout` sink has no target path to name a format for.
            if (std.mem.eql(u8, w.connector, "csv") and w.target.len > 0) {
                const fmt = try formatFromHints(hints, self.diag);
                if (unreadableTarget(w.target, fmt)) |why|
                    return fail(self.diag, "cannot write `{s}`: {s}", .{ w.target, why });
            }
            _ = try dialectFromHints(hints, self.diag);
            // Accepting it and writing UTF-8 anyway would be the silent kind of
            // wrong; transcoding on the way out is a separate feature.
            if (hintText(hints, "encoding") != null)
                return fail(self.diag, "`encoding` applies to a read; a CSV sink always writes UTF-8", .{});
            if (w.mode == .append) {
                if (appendUnsupported(w.target)) |why|
                    return fail(self.diag, "`APPEND` into `{s}` is not supported: {s}", .{ w.target, why });
            }
            return .{ .connector = w.connector, .target = w.target, .mode = @tagName(w.mode) };
        }
        const conn = self.connections.get(w.connector) orelse
            return fail(self.diag, "unknown connection `{s}` in write", .{w.connector});
        return .{ .connector = conn.connector, .target = w.target, .mode = @tagName(w.mode) };
    }

    /// Output schema after a stage (type-checking expressions along the way).
    /// Returns null where the flow becomes unresolvable (join's right side).
    fn propagate(self: *Ctx, in: types.Schema, node: ast.Stage.Node) Error!?types.Schema {
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
            .join => return null,
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
        if (self.bindings.get(j.binding) == null)
            return fail(self.diag, "unknown binding `{s}` in join", .{j.binding});
        const d = try std.fmt.allocPrint(self.arena, "{s} {s}", .{ @tagName(j.kind), j.binding });
        return .{ .kind = "join", .detail = d, .breaker = true };
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

/// Prints the plan as a tree, root first and source deepest — the nesting a
/// pull pipeline actually has, and the one `EXPLAIN ANALYZE` prints, so the two
/// read as the same picture with different annotations. Node names are the
/// operator names `ANALYZE` reports, `scan` included.
pub fn render(plan: Plan, w: anytype) !void {
    for (plan.outputs) |o| {
        if (std.mem.eql(u8, plan.kind, "batch")) {
            try w.writeAll("plan\n");
        } else {
            try w.print("plan ({s})\n", .{plan.kind});
        }

        var depth: usize = 1;
        try indent(w, depth);
        if (o.sink.target.len > 0) {
            try w.print("write  {s}  {s} ({s})\n", .{ sinkKind(o.sink), o.sink.target, o.sink.mode });
        } else {
            try w.print("write  {s}  ({s})\n", .{ sinkKind(o.sink), o.sink.mode });
        }

        // Stages are held in dataflow order; the tree reads the other way.
        var i = o.stages.len;
        while (i > 0) {
            i -= 1;
            depth += 1;
            const st = o.stages[i];
            try indent(w, depth);
            if (st.detail.len > 0) {
                try w.print("{s}  {s}\n", .{ st.kind, st.detail });
            } else {
                try w.print("{s}\n", .{st.kind});
            }
            try printSchema(w, depth + 1, st.out_schema);
        }

        depth += 1;
        try indent(w, depth);
        try w.print("scan  {s}  {s}\n", .{ sinkKind(o.source), o.source.detail });
        if (o.source.pushdown.len > 0) {
            try indent(w, depth + 1);
            try w.print("pushdown: {s}\n", .{o.source.pushdown});
        }
        if (o.source.schema == null) {
            try indent(w, depth + 1);
            try w.writeAll("schema: unresolved\n");
        }
        try printSchema(w, depth + 1, o.source.schema);

        try w.writeAll("  physical: ");
        if (o.physical.splittable) {
            // Whether it *will* split depends on the table's key and size, which
            // only the source can answer — and analysis does not connect.
            try w.writeAll("split-parallel candidate");
            if (o.physical.sink_parallel) try w.writeAll(", per-lane sink");
        } else if (o.physical.morsel_parallel) {
            // Same hedge as above, for the reasons only the file can settle: a CSV
            // that quotes a newline cannot be cut on byte boundaries, and a parquet
            // with a single row group has nothing to divide. A breaker still runs
            // per lane here — the lanes fold partials and the combine merges them —
            // so unlike the serial branch it does not mean the fan-out is off.
            try w.writeAll("morsel-parallel candidate");
            if (o.physical.has_breaker) try w.writeAll(" (per-lane partials, combined)");
        } else {
            try w.writeAll("serial");
            if (o.physical.has_breaker) try w.writeAll(" (has breaker, materializes)");
        }
        try w.writeAll("\n");
    }
}

/// Why an explicit `APPEND` cannot be honoured for a file target, or null when
/// it can. Both refusals are about rewriting what is already there: a parquet
/// footer indexes every row group and is written last, and a block blob is
/// committed whole rather than extended. One source of truth, so `check` and the
/// runtime planner cannot drift apart on which targets accumulate.
pub fn appendUnsupported(target: []const u8) ?[]const u8 {
    if (azure.isUrl(target) or s3.isUrl(target)) return "an object-store blob is replaced on write, never extended";
    if (pqwrite.Writer.isPath(target)) return "a parquet file's footer indexes every row group and is written last, so appending means rewriting the file";
    return null;
}

/// The `csv` connector backs every file sink, so the plan has to name the
/// format from the target — otherwise a parquet write reads as `write csv`.
fn sinkKind(node: anytype) []const u8 {
    const path = if (@hasField(@TypeOf(node), "target")) node.target else node.detail;
    if (std.mem.eql(u8, node.connector, "csv") and pqwrite.Writer.isPath(path)) return "parquet";
    return node.connector;
}

fn indent(w: anytype, depth: usize) !void {
    var n: usize = 0;
    while (n < depth) : (n += 1) try w.writeAll("  ");
}

/// An unresolved schema is only worth saying once, at the scan that could not
/// resolve it: nothing downstream of an unknown source is knowable either, and
/// repeating the note on every stage buried the plan in it.
///
/// Labelled, because an annotation and a child node land at the same depth and
/// a bare list of columns reads like another operator otherwise.
fn printSchema(w: anytype, depth: usize, schema: ?types.Schema) !void {
    const s = schema orelse return;
    try indent(w, depth);
    try w.writeAll("schema: ");
    for (s.fields, 0..) |f, i| {
        if (i > 0) try w.writeAll("  ");
        try w.print("{s}:{s}{s}", .{ f.name, @tagName(f.ty.kind), if (f.ty.nullable) "?" else "" });
    }
    try w.writeAll("\n");
}

fn isBuiltinSource(connector: []const u8) bool {
    return std.mem.eql(u8, connector, "csv") or std.mem.eql(u8, connector, "request") or
        std.mem.eql(u8, connector, "http") or std.mem.eql(u8, connector, "buffer") or
        std.mem.eql(u8, connector, "range") or std.mem.eql(u8, connector, "unit");
}

fn isSqlConnector(connector: []const u8) bool {
    return std.mem.eql(u8, connector, "postgres") or std.mem.eql(u8, connector, "mysql") or std.mem.eql(u8, connector, "sqlserver");
}

/// The pushdown dialect for a connector, or null if it's not a SQL source.
fn dialectOf(connector: []const u8) ?Dialect {
    if (std.mem.eql(u8, connector, "postgres")) return .postgres;
    if (std.mem.eql(u8, connector, "mysql")) return .mysql;
    if (std.mem.eql(u8, connector, "sqlserver")) return .sqlserver;
    return null;
}

/// AND a raw `PUSHDOWN`/@[where] fragment with the translated implicit
/// predicate for the plan preview.
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

/// A read is split-eligible if it's a `table` (PK introspection) or a `query`
/// with an explicit `@[split]`. (The actual key/size check happens at run time.)
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

/// The file format a path is read or written as. `format` in a `WITH (...)` names
/// it outright; otherwise the extension does.
pub const FileFormat = enum { csv, parquet };

fn hintText(hints: []const ast.Hint, key: []const u8) ?[]const u8 {
    for (hints) |h| {
        if (!std.mem.eql(u8, h.key, key)) continue;
        return switch (h.value) {
            .str => |s| s,
            .ident => |s| s,
            else => null,
        };
    }
    return null;
}

/// `WITH (delimiter = ';', encoding = 'latin1')` for a file read or write.
///
/// Both are validated here rather than at the reader, so `basalt check` rejects a
/// typo before anything opens a file — an unknown encoding name is exactly the
/// kind of mistake that would otherwise be discovered halfway through a load.
pub fn dialectFromHints(hints: []const ast.Hint, diag: *Diag) Error!csv.Dialect {
    var d = csv.Dialect{};
    if (hintText(hints, "delimiter") orelse hintText(hints, "delim")) |s| {
        // One byte, because the reader compares bytes and the parallel reader cuts
        // the file on them. A tab is worth spelling out; `'\t'` in a SQL string
        // literal has no escape processing.
        const one: ?u8 = if (s.len == 1)
            s[0]
        else if (std.mem.eql(u8, s, "\\t") or std.mem.eql(u8, s, "tab"))
            '\t'
        else
            null;
        d.delim = one orelse return fail(diag, "delimiter must be a single character (or `tab`), got `{s}`", .{s});
        if (d.delim == '"' or d.delim == '\n' or d.delim == '\r')
            return fail(diag, "delimiter cannot be a quote or a newline", .{});
    }
    if (hintText(hints, "encoding")) |s| {
        d.encoding = csv.Encoding.parse(s) orelse
            return fail(diag, "unknown encoding `{s}` (utf8, latin1 / iso-8859-1, cp1252 / windows-1252)", .{s});
    }
    return d;
}

/// The format named by `WITH (format = ...)`, validated. Null when unset.
pub fn formatFromHints(hints: []const ast.Hint, diag: *Diag) Error!?FileFormat {
    const s = hintText(hints, "format") orelse return null;
    if (std.ascii.eqlIgnoreCase(s, "csv")) return .csv;
    if (std.ascii.eqlIgnoreCase(s, "parquet")) return .parquet;
    return fail(diag, "unknown format `{s}` (csv, parquet)", .{s});
}

/// The extension basalt reads a path as, or null when it carries none it knows.
///
/// `csv.dataName` walks the chain first, so `orders.csv.gz` and
/// `inf.zip :: inf_diario.csv` both answer `.csv` — the name that matters is the
/// innermost one, not the container's.
fn formatOfPath(path: []const u8) ?FileFormat {
    const bare = csv.dataName(path);
    if (pqwrite.Writer.isPath(bare)) return .parquet;
    if (std.ascii.endsWithIgnoreCase(bare, ".csv")) return .csv;
    return null;
}

/// The label the run summary shows for a file source or sink: the format actually
/// resolved, not the connector name. A bare path lowers to the `csv` connector
/// whatever its extension, so reporting the connector announced every serial
/// parquet scan as `csv` — the summary is the main feedback channel, and it was
/// naming the wrong reader.
///
/// A malformed `format` hint is `analyzeOne`'s error to raise, not a label's, so an
/// unresolvable format falls back to the extension and then to `csv`.
pub fn formatLabel(path: []const u8, hints: []const ast.Hint) []const u8 {
    var d = Diag{};
    const explicit = formatFromHints(hints, &d) catch null;
    return @tagName(explicit orelse formatOfPath(path) orelse .csv);
}

/// Why this path cannot be read or written as a table, or null when it can.
///
/// Every unrecognised extension used to fall through to the CSV reader, silently.
/// A 12 MB zip holding 583k rows answered `SELECT COUNT(*)` with 46204 — the
/// newlines that happen to occur in deflate output — and `check` said the script
/// was fine. A wrong number that looks right is the one outcome this engine is
/// built to avoid, so an extension it does not read is a plan-time error.
pub fn unreadableTarget(path: []const u8, explicit: ?FileFormat) ?[]const u8 {
    // A trailing `/` is a prefix read: the objects under it carry the extensions,
    // and `parquetPrefix`/the CSV lister decide per object.
    if (std.mem.endsWith(u8, path, "/")) return null;

    // Everything about an archive is `archiveProblem`'s to judge: the member's own
    // name is what carries the format, and only opening the archive reveals it.
    if (csv.splitArchive(path) != null) return null;

    // Parquet is random-access — footer first, then the chunks a query needs. A
    // compressed stream is sequential, so the reader has nothing to seek in.
    // Refusing beats decompressing gigabytes into a temp file that nothing in the
    // plan mentions.
    const fmt = explicit orelse formatOfPath(path);
    if (csv.splitCodec(path).codec != .none and fmt == .parquet)
        return "parquet needs random access, so it cannot be read through compression; decompress it first";

    if (explicit != null) return null;
    if (fmt != null) return null;
    return "basalt handles `.csv` and `.parquet`, optionally `.gz`/`.zst` compressed or inside a `.zip`; name the format with `WITH (format = 'csv')` if the extension differs";
}

/// Why this archive reference cannot be read as one table, or null when it can.
///
/// Opens the archive to answer, and stays quiet when it cannot be opened — a script
/// may legitimately be checked before its data has been fetched, which is what
/// `offlineSchema` already assumes. Everything archive-shaped is decided here so the
/// guarantee `unreadableTarget` gives for a loose file also holds inside a
/// container: a `.json` member is refused exactly like a `.json` file.
pub fn archiveProblem(arena: std.mem.Allocator, path: []const u8, explicit: ?FileFormat) ?[]const u8 {
    const ar = csv.splitArchive(path) orelse return null;
    if (csv.CsvReader.isUrl(ar.archive))
        return "an archive has its index at the end, so reading one over HTTP needs a ranged fetch that is not wired up yet; download it first";

    const members = zipsrc.names(arena, ar.archive) catch return null;
    if (members.len == 0) return "the archive holds no files";

    const chosen = if (ar.member) |want| blk: {
        for (members) |m| if (std.mem.eql(u8, m, want)) break :blk m;
        return std.fmt.allocPrint(arena, "no file `{s}` in the archive ({s})", .{ want, joinNames(arena, members) }) catch null;
    } else if (members.len > 1)
        return std.fmt.allocPrint(arena, "the archive holds {d} files; name one with `:: <name>` ({s})", .{ members.len, joinNames(arena, members) }) catch null
    else
        members[0];

    if (explicit == null and formatOfPath(chosen) == null)
        return std.fmt.allocPrint(arena, "`{s}` inside it is not a `.csv` or `.parquet`; name the format with `WITH (format = 'csv')`", .{chosen}) catch null;
    if ((explicit orelse formatOfPath(chosen)) == .parquet)
        return "parquet needs random access, so it cannot be read out of an archive; extract it first";
    return null;
}

/// The first few names, for an error that has to name the choices.
fn joinNames(arena: std.mem.Allocator, items: []const []const u8) []const u8 {
    var out: []const u8 = "";
    for (items, 0..) |m, i| {
        if (i == 3) return std.fmt.allocPrint(arena, "{s}, …", .{out}) catch out;
        out = std.fmt.allocPrint(arena, "{s}{s}{s}", .{ out, if (i == 0) "" else ", ", m }) catch return out;
    }
    return out;
}

/// Whether a read divides into per-lane morsels at `-j > 1`.
///
/// A parquet is cut into row groups wherever it lives, since every lane range-reads
/// its own chunks. A CSV is cut into byte ranges, which needs the bytes locally —
/// the runtime memory-maps the file, so a CSV over HTTP or object storage is
/// fetched whole and parsed serially.
fn morselParallelRead(connector: []const u8, node: ast.Stage.Node) bool {
    // Every file read arrives on the `csv` connector; the path decides the format.
    if (!std.mem.eql(u8, connector, "csv")) return false;
    const path = switch (node) {
        .read => |rd| switch (rd.form) {
            .path => |p| p,
            else => return false,
        },
        else => return false,
    };
    // Splittability, in Hadoop's sense: there is no mapping from a byte offset in a
    // compressed stream to a row, so a `.csv.gz` is read start to finish however
    // many lanes are free. An archive member is sequential for the same reason. Both
    // are `MappedCsv.open`'s `NotMappable`, and the label has to agree with the
    // runtime or EXPLAIN goes back to overstating what it is about to do.
    if (csv.splitCodec(path).codec != .none or csv.splitArchive(path) != null) return false;
    if (pqwrite.Writer.isPath(path)) return true;
    return std.mem.indexOf(u8, path, "://") == null;
}

/// Offline schema resolution: a local CSV header or parquet footer is readable
/// without connecting to anything; everything else stays unresolved.
fn offlineSchema(arena: std.mem.Allocator, rd: ast.Read, hints: []const ast.Hint) ?types.Schema {
    if (std.mem.eql(u8, rd.connector, "unit")) return .{ .fields = &.{} };
    if (std.mem.eql(u8, rd.connector, "range")) {
        const fields = arena.alloc(types.Schema.Field, 1) catch return null;
        fields[0] = .{ .name = "range", .ty = .{ .kind = .int } };
        return .{ .fields = fields };
    }
    if (std.mem.eql(u8, rd.connector, "csv") and rd.form == .path) {
        if (csv.CsvReader.isUrl(rd.form.path)) return null;
        // A compressed or archived parquet is refused above, so only a plain path
        // reaches the parquet reader here.
        if (csv.splitCodec(rd.form.path).codec == .none and csv.splitArchive(rd.form.path) == null and
            pqdecode.Reader.isPath(rd.form.path))
        {
            const pr = pqdecode.Reader.open(arena, rd.form.path) catch return null;
            return pr.schema;
        }
        // The header is split on the script's delimiter, or `check` would report
        // one column named after the whole header line for a `;` file.
        var hdiag = Diag{};
        const d = dialectFromHints(hints, &hdiag) catch return null;
        const reader = csv.CsvReader.open(arena, rd.form.path, d) catch return null;
        const schema = reader.schema;
        reader.close();
        return schema;
    }
    return null;
}

fn lastPart(q: ast.QualName) []const u8 {
    return q.parts[q.parts.len - 1];
}

const parser = @import("../lang/sql_parser.zig");

fn parse(a: std.mem.Allocator, src: []const u8) !ast.Program {
    var pd: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    return parser.parseSource(a, src, &pd);
}

test "analyze a CSV map pipeline: structure, offline schema, physical" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,amount\n1,100\n" });
    const base = try tmp.dir.realpathAlloc(a, ".");
    const in = try std.fs.path.join(a, &.{ base, "in.csv" });

    const src = try std.fmt.allocPrint(a,
        "LOAD INTO '/tmp/x.csv' AS SELECT id FROM '{s}' WHERE CAST(amount AS INT) >= 50;",
        .{in},
    );
    const prog = try parse(a, src);
    var diag = Diag{};
    const plan = try analyze(a, prog, &diag);

    try std.testing.expectEqualStrings("batch", plan.kind);
    try std.testing.expectEqual(@as(usize, 1), plan.outputs.len);
    const o = plan.outputs[0];
    try std.testing.expectEqualStrings("csv", o.source.connector);
    try std.testing.expect(o.source.schema != null);
    try std.testing.expectEqual(@as(usize, 2), o.source.schema.?.fields.len);
    try std.testing.expectEqual(@as(usize, 2), o.stages.len);
    try std.testing.expectEqualStrings("filter", o.stages[0].kind);
    try std.testing.expectEqualStrings("select", o.stages[1].kind);
    try std.testing.expect(!o.physical.has_breaker);
    try std.testing.expect(!o.physical.splittable);
    // Not `splittable` (that is the SQL key-range fan-out) but still parallel:
    // the runtime cuts a local CSV into byte-range chunks.
    try std.testing.expect(o.physical.morsel_parallel);
}

test "unreadableTarget: an extension basalt does not read is refused" {
    // The reason this exists: a 12MB zip of 583k rows answered COUNT(*) with 46204
    // — newlines in its deflate stream — and `check` approved the script.
    try std.testing.expect(unreadableTarget("/data/x.csv", null) == null);
    try std.testing.expect(unreadableTarget("/data/X.CSV", null) == null);
    try std.testing.expect(unreadableTarget("/data/x.parquet", null) == null);
    // A query string is not part of the name.
    try std.testing.expect(unreadableTarget("https://h/d.csv?token=abc", null) == null);
    // A trailing slash is a prefix read; the objects under it carry extensions.
    try std.testing.expect(unreadableTarget("s3://bkt/bronze/", null) == null);

    // Compressed and archived names resolve through the chain to their inner name.
    try std.testing.expect(unreadableTarget("/data/x.csv.gz", null) == null);
    try std.testing.expect(unreadableTarget("/data/x.csv.zst", null) == null);
    // An archive is `archiveProblem`'s to judge, since only its members name a
    // format; `unreadableTarget` deliberately passes it through.
    try std.testing.expect(unreadableTarget("/data/inf.zip", null) == null);
    try std.testing.expect(unreadableTarget("/data/inf.zip :: a.csv", null) == null);
    // Parquet cannot be read through a codec: it needs to seek.
    try std.testing.expect(unreadableTarget("/data/x.parquet.gz", null) != null);

    try std.testing.expect(unreadableTarget("/data/rows.json", null) != null);
    try std.testing.expect(unreadableTarget("/data/book.xlsx", null) != null);
    try std.testing.expect(unreadableTarget("/data/noext", null) != null);
    // Naming the format is the escape hatch for an oddly-named file.
    try std.testing.expect(unreadableTarget("/data/weird.dat", .csv) == null);
}

test "dialectFromHints: parses, and rejects what cannot work" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const mkh = struct {
        fn hint(al: std.mem.Allocator, k: []const u8, v: []const u8) ![]const ast.Hint {
            const h = try al.alloc(ast.Hint, 1);
            h[0] = .{ .key = k, .value = .{ .str = v }, .pos = .{ .line = 1, .col = 1 } };
            return h;
        }
    };

    var d = Diag{};
    try std.testing.expectEqual(@as(u8, ','), (try dialectFromHints(&.{}, &d)).delim);
    try std.testing.expectEqual(@as(u8, ';'), (try dialectFromHints(try mkh.hint(a, "delimiter", ";"), &d)).delim);
    try std.testing.expectEqual(@as(u8, '\t'), (try dialectFromHints(try mkh.hint(a, "delimiter", "tab"), &d)).delim);
    try std.testing.expectEqual(@as(u8, '|'), (try dialectFromHints(try mkh.hint(a, "delim", "|"), &d)).delim);
    try std.testing.expectEqual(csv.Encoding.latin1, (try dialectFromHints(try mkh.hint(a, "encoding", "iso-8859-1"), &d)).encoding);

    try std.testing.expectError(error.AnalyzeFailed, dialectFromHints(try mkh.hint(a, "delimiter", ";;"), &d));
    try std.testing.expectError(error.AnalyzeFailed, dialectFromHints(try mkh.hint(a, "delimiter", "\""), &d));
    try std.testing.expectError(error.AnalyzeFailed, dialectFromHints(try mkh.hint(a, "encoding", "latin9"), &d));
}

test "physical plan: which SQL shapes report a key-range split" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const conn = "CREATE CONNECTION pg TYPE postgres OPTIONS (host = 'h', database = 'd');\n";

    const cases = [_]struct { q: []const u8, split: bool }{
        // map-only: the original case
        .{ .q = "SELECT a FROM pg.t WHERE b > 0", .split = true },
        // an aggregate fans into key-range lanes via runParallelSqlAgg; this is the
        // shape that used to print `serial` while running in parallel
        .{ .q = "SELECT g, COUNT(*) AS n FROM pg.t GROUP BY g", .split = true },
        // ... including with the sort/limit tail both fan-out paths carry
        .{ .q = "SELECT g, COUNT(*) AS n FROM pg.t GROUP BY g ORDER BY n DESC LIMIT 5", .split = true },
        // a top-N with nothing to fan out under it stays serial
        .{ .q = "SELECT a FROM pg.t ORDER BY a DESC LIMIT 10", .split = false },
        // DISTINCT is a breaker neither path handles
        .{ .q = "SELECT DISTINCT a FROM pg.t", .split = false },
        // a raw query read is not divisible by key range whatever its shape
        .{ .q = "SELECT g, COUNT(*) AS n FROM pg.QUERY($$SELECT * FROM t$$) GROUP BY g", .split = false },
    };

    for (cases) |c| {
        const src = try std.fmt.allocPrint(a, "{s}LOAD INTO '/tmp/o.csv' AS {s};", .{ conn, c.q });
        const prog = try parse(a, src);
        var diag = Diag{};
        const plan = try analyze(a, prog, &diag);
        try std.testing.expectEqual(c.split, plan.outputs[0].physical.splittable);
    }
}

test "physical plan: which file reads divide into morsels" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const cases = [_]struct { from: []const u8, morsel: bool }{
        // A parquet is cut into row groups wherever it lives — each lane
        // range-reads its own chunks.
        .{ .from = "'/data/x.parquet'", .morsel = true },
        .{ .from = "'https://h/x.parquet'", .morsel = true },
        .{ .from = "'s3://bkt/x.parquet'", .morsel = true },
        // A CSV is cut on byte offsets, which needs the bytes on disk to mmap.
        .{ .from = "'/data/x.csv'", .morsel = true },
        .{ .from = "'https://h/x.csv'", .morsel = false },
        .{ .from = "'az://acct/c/x.csv'", .morsel = false },
        // Not a file read at all.
        .{ .from = "RANGE(10)", .morsel = false },
    };

    for (cases) |c| {
        const src = try std.fmt.allocPrint(a, "LOAD INTO '/tmp/o.csv' AS SELECT * FROM {s};", .{c.from});
        const prog = try parse(a, src);
        var diag = Diag{};
        const plan = try analyze(a, prog, &diag);
        try std.testing.expectEqual(c.morsel, plan.outputs[0].physical.morsel_parallel);
    }
}

test "analyze a SQL table pipeline: unresolved schema offline, split candidate" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const prog = try parse(a,
        \\CREATE CONNECTION pg TYPE postgres OPTIONS (
        \\  host = 'h', user = 'u', password = 'p', database = 'd'
        \\);
        \\LOAD INTO '/tmp/x.csv' AS SELECT * FROM pg.orders WHERE amount > 0;
    );
    var diag = Diag{};
    const plan = try analyze(a, prog, &diag);
    const o = plan.outputs[0];
    try std.testing.expectEqualStrings("postgres", o.source.connector);
    try std.testing.expect(o.source.schema == null);
    try std.testing.expect(o.physical.splittable);
    try std.testing.expectEqualStrings("(\"amount\" > 0)", o.source.pushdown);
}

test "analyze pushdown preview: raw PUSHDOWN AND-ed with the translated filter" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const prog = try parse(a,
        \\CREATE CONNECTION erp TYPE sqlserver OPTIONS (host = 'h', database = 'd');
        \\LOAD INTO '/tmp/x.csv' AS
        \\SELECT filial FROM erp.dbo.T PUSHDOWN($$D_E_L_E_T_ <> '*'$$)
        \\WHERE valor > 0 AND status = 'ok';
    );
    var diag = Diag{};
    const plan = try analyze(a, prog, &diag);
    try std.testing.expectEqualStrings(
        "(D_E_L_E_T_ <> '*') AND ((([valor] > 0) AND ([status] = 'ok')))",
        plan.outputs[0].source.pushdown,
    );
}

test "analyze pushdown preview: an untranslatable filter is not pushed (stays engine-side)" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const prog = try parse(a,
        \\CREATE CONNECTION pg TYPE postgres OPTIONS (host = 'h', database = 'd');
        \\LOAD INTO '/tmp/x.csv' AS SELECT * FROM pg.t WHERE amount + 1 > 5;
    );
    var diag = Diag{};
    const plan = try analyze(a, prog, &diag);
    try std.testing.expectEqualStrings("", plan.outputs[0].source.pushdown);
}

test "type flow fills out_schema for resolved sources" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,amount\n1,100\n" });
    const base = try tmp.dir.realpathAlloc(a, ".");
    const in = try std.fs.path.join(a, &.{ base, "in.csv" });
    const src = try std.fmt.allocPrint(a, "LOAD INTO '/tmp/x.csv' AS SELECT id, CAST(amount AS INT) * 2 AS d FROM '{s}';", .{in});
    var diag = Diag{};
    const plan = try analyze(a, try parse(a, src), &diag);
    const sel = plan.outputs[0].stages[0];
    try std.testing.expect(sel.out_schema != null);
    try std.testing.expectEqual(@as(usize, 2), sel.out_schema.?.fields.len);
    try std.testing.expectEqual(types.TypeKind.int, sel.out_schema.?.fields[1].ty.kind);
}

test "type flow catches a type error in an expression" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,name\n1,x\n" });
    const base = try tmp.dir.realpathAlloc(a, ".");
    const in = try std.fs.path.join(a, &.{ base, "in.csv" });
    const src = try std.fmt.allocPrint(a, "LOAD INTO '/tmp/x.csv' AS SELECT * FROM '{s}' WHERE NOT name;", .{in});
    var diag = Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyze(a, try parse(a, src), &diag));
}

test "analyze rejects unknown connection" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const prog = try parse(a, "LOAD INTO '/tmp/x.csv' AS SELECT * FROM nope.t;");
    var diag = Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyze(a, prog, &diag));
    try std.testing.expect(std.mem.indexOf(u8, diag.msg, "unknown connection") != null);
}

/// Analyze `LOAD INTO ... AS <query over a 2-col CSV>` offline and expect a
/// type/plan error. `$IN` in the query is the input CSV's path.
fn expectAnalyzeErr(a: std.mem.Allocator, csv_data: []const u8, query: []const u8) !void {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = csv_data });
    const base = try tmp.dir.realpathAlloc(a, ".");
    const in = try std.fs.path.join(a, &.{ base, "in.csv" });
    const q = try std.mem.replaceOwned(u8, a, query, "$IN", in);
    const src = try std.fmt.allocPrint(a, "LOAD INTO '/tmp/x.csv' AS {s};", .{q});
    var diag = Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyze(a, try parse(a, src), &diag));
}

test "analyze rejects a program with no output pipeline" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const prog = try parse(a, "PARAM x INT DEFAULT 1;");
    var diag = Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyze(a, prog, &diag));
    try std.testing.expect(std.mem.indexOf(u8, diag.msg, "no output pipeline") != null);
}

test "physical plan: a breaker keeps SQL serial; a query read is not split-eligible" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag = Diag{};

    const p1 = try analyze(a, try parse(a,
        \\CREATE CONNECTION pg TYPE postgres OPTIONS (host = 'h', database = 'd');
        \\LOAD INTO '/tmp/x.csv' AS SELECT * FROM pg.orders ORDER BY id;
    ), &diag);
    try std.testing.expect(p1.outputs[0].physical.has_breaker);
    try std.testing.expect(!p1.outputs[0].physical.splittable);

    const p2 = try analyze(a, try parse(a,
        \\CREATE CONNECTION pg TYPE postgres OPTIONS (host = 'h', database = 'd');
        \\LOAD INTO '/tmp/x.csv' AS SELECT * FROM pg.QUERY($$SELECT 1 AS x$$);
    ), &diag);
    try std.testing.expect(!p2.outputs[0].physical.has_breaker);
    try std.testing.expect(!p2.outputs[0].physical.splittable);
}

fn tfld(a: std.mem.Allocator, name: []const u8) !*ast.Expr {
    const parts = try a.alloc([]const u8, 1);
    parts[0] = name;
    const e = try a.create(ast.Expr);
    e.* = .{ .field = .{ .parts = parts } };
    return e;
}

test "joinPlan: collision suffix `_r`, left-nullability, semi/anti drop the right side" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const I = types.Type.init(.int);
    const S = types.Type.init(.string);
    const left = types.Schema{ .fields = &.{ .{ .name = "id", .ty = I }, .{ .name = "code", .ty = S } } };
    const right = types.Schema{ .fields = &.{ .{ .name = "code", .ty = S }, .{ .name = "label", .ty = S } } };
    const key = ast.QualName{ .parts = &.{"code"} };
    var diag = Diag{};

    const keys: []const ast.QualName = &.{key};

    const inner = try joinPlan(a, left, right, .{ .kind = .inner, .binding = "r", .left_keys = keys, .right_keys = keys }, &diag);
    try std.testing.expectEqual(@as(usize, 1), inner.lks[0]);
    try std.testing.expectEqual(@as(usize, 0), inner.rks[0]);
    try std.testing.expectEqual(@as(usize, 4), inner.schema.fields.len);
    try std.testing.expectEqualStrings("code_r", inner.schema.fields[2].name);
    try std.testing.expectEqualStrings("label", inner.schema.fields[3].name);
    try std.testing.expect(!inner.schema.fields[3].ty.nullable);

    const lj = try joinPlan(a, left, right, .{ .kind = .left, .binding = "r", .left_keys = keys, .right_keys = keys }, &diag);
    try std.testing.expect(lj.right_nullable);
    try std.testing.expect(lj.schema.fields[2].ty.nullable and lj.schema.fields[3].ty.nullable);
    try std.testing.expect(!lj.schema.fields[0].ty.nullable);

    const semi = try joinPlan(a, left, right, .{ .kind = .semi, .binding = "r", .left_keys = keys, .right_keys = keys }, &diag);
    try std.testing.expect(!semi.emit_right);
    try std.testing.expectEqual(@as(usize, 2), semi.schema.fields.len);

    // right/full null the left side; full nulls both. cross needs no keys.
    const rj = try joinPlan(a, left, right, .{ .kind = .right, .binding = "r", .left_keys = keys, .right_keys = keys }, &diag);
    try std.testing.expect(rj.left_nullable and !rj.right_nullable);
    try std.testing.expect(rj.schema.fields[0].ty.nullable);
    const fj = try joinPlan(a, left, right, .{ .kind = .full, .binding = "r", .left_keys = keys, .right_keys = keys }, &diag);
    try std.testing.expect(fj.left_nullable and fj.right_nullable);
    const cj = try joinPlan(a, left, right, .{ .kind = .cross, .binding = "r", .left_keys = &.{}, .right_keys = &.{} }, &diag);
    try std.testing.expectEqual(@as(usize, 0), cj.lks.len);
    try std.testing.expectEqual(@as(usize, 4), cj.schema.fields.len);

    try std.testing.expectError(error.AnalyzeFailed, joinPlan(a, left, right, .{ .kind = .inner, .binding = "r", .left_keys = &.{.{ .parts = &.{"nope"} }}, .right_keys = keys }, &diag));
    try std.testing.expect(std.mem.indexOf(u8, diag.msg, "unknown left join key") != null);
}

test "joinPlan: the `_r` suffix keeps bumping until the name is actually free" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const I = types.Type.init(.int);
    const S = types.Type.init(.string);
    // `x_r` on the left and `x` on the right used to produce two `x_r` columns,
    // the second unreachable; `x_r` on the right collides with its own suffix.
    const left = types.Schema{ .fields = &.{
        .{ .name = "id", .ty = I },
        .{ .name = "x", .ty = S },
        .{ .name = "x_r", .ty = S },
    } };
    const right = types.Schema{ .fields = &.{
        .{ .name = "id", .ty = I },
        .{ .name = "x", .ty = S },
        .{ .name = "x_r", .ty = S },
    } };
    var diag = Diag{};
    const keys: []const ast.QualName = &.{.{ .parts = &.{"id"} }};
    const p = try joinPlan(a, left, right, .{ .kind = .inner, .binding = "r", .left_keys = keys, .right_keys = keys }, &diag);
    try std.testing.expectEqual(@as(usize, 6), p.schema.fields.len);
    try std.testing.expectEqualStrings("id_r", p.schema.fields[3].name);
    try std.testing.expectEqualStrings("x_r2", p.schema.fields[4].name);
    try std.testing.expectEqualStrings("x_r_r", p.schema.fields[5].name);
    // Every output name resolves to its own column.
    for (p.schema.fields, 0..) |f, i| try std.testing.expectEqual(i, p.schema.indexOf(f.name).?);
}

test "joinPlan: pair orientation, ambiguity, per-pair comparability" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const I = types.Type.init(.int);
    const S = types.Type.init(.string);
    const left = types.Schema{ .fields = &.{ .{ .name = "id", .ty = I }, .{ .name = "day", .ty = S }, .{ .name = "amount", .ty = I } } };
    const right = types.Schema{ .fields = &.{ .{ .name = "d", .ty = S }, .{ .name = "ref", .ty = I }, .{ .name = "note", .ty = S } } };
    const k_ref = ast.QualName{ .parts = &.{"ref"} };
    const k_id = ast.QualName{ .parts = &.{"id"} };
    const k_day = ast.QualName{ .parts = &.{"day"} };
    const k_d = ast.QualName{ .parts = &.{"d"} };
    const k_amount = ast.QualName{ .parts = &.{"amount"} };
    const k_a = ast.QualName{ .parts = &.{"a"} };
    const k_b = ast.QualName{ .parts = &.{"b"} };
    var diag = Diag{};

    // `ref = id AND day = d`: the first pair is written right-side-first.
    const p = try joinPlan(a, left, right, .{
        .kind = .inner,
        .binding = "r",
        .left_keys = &.{ k_ref, k_day },
        .right_keys = &.{ k_id, k_d },
    }, &diag);
    try std.testing.expectEqualSlices(usize, &.{ 0, 1 }, p.lks);
    try std.testing.expectEqualSlices(usize, &.{ 1, 0 }, p.rks);

    // `amount = d` compares an int against a string.
    try std.testing.expectError(error.AnalyzeFailed, joinPlan(a, left, right, .{
        .kind = .inner,
        .binding = "r",
        .left_keys = &.{k_amount},
        .right_keys = &.{k_d},
    }, &diag));
    try std.testing.expect(std.mem.indexOf(u8, diag.msg, "not comparable") != null);

    // Both names live in both schemas, so neither reading wins.
    const both_l = types.Schema{ .fields = &.{ .{ .name = "a", .ty = I }, .{ .name = "b", .ty = I } } };
    const both_r = types.Schema{ .fields = &.{ .{ .name = "b", .ty = I }, .{ .name = "a", .ty = I } } };
    try std.testing.expectError(error.AnalyzeFailed, joinPlan(a, both_l, both_r, .{
        .kind = .inner,
        .binding = "r",
        .left_keys = &.{k_a},
        .right_keys = &.{k_b},
    }, &diag));
    try std.testing.expect(std.mem.indexOf(u8, diag.msg, "ambiguous") != null);

    // Same name on both sides is not ambiguous — both readings agree.
    const same = try joinPlan(a, both_l, both_r, .{
        .kind = .inner,
        .binding = "r",
        .left_keys = &.{k_a},
        .right_keys = &.{k_a},
    }, &diag);
    try std.testing.expectEqualSlices(usize, &.{0}, same.lks);
    try std.testing.expectEqualSlices(usize, &.{1}, same.rks);
}

test "aggregatePlan: result types per function and group-key passthrough" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const in = types.Schema{ .fields = &.{
        .{ .name = "g", .ty = types.Type.init(.string) },
        .{ .name = "v", .ty = types.Type.init(.int) },
    } };
    const by = try a.alloc(ast.QualName, 1);
    by[0] = .{ .parts = &.{"g"} };
    const aggs = try a.alloc(ast.AggItem, 4);
    aggs[0] = .{ .name = "n", .func = .count, .arg = null };
    aggs[1] = .{ .name = "s", .func = .sum, .arg = try tfld(a, "v") };
    aggs[2] = .{ .name = "m", .func = .avg, .arg = try tfld(a, "v") };
    aggs[3] = .{ .name = "lo", .func = .min, .arg = try tfld(a, "g") };
    var pm = std.StringHashMap(*const ast.Expr).init(a);
    var diag = Diag{};
    const plan = try aggregatePlan(a, in, .{ .aggs = aggs, .by = by }, &pm, &diag);

    try std.testing.expectEqual(@as(usize, 1), plan.by.len);
    try std.testing.expectEqual(@as(usize, 0), plan.by[0]);
    const f = plan.schema.fields;
    try std.testing.expectEqual(@as(usize, 5), f.len);
    try std.testing.expectEqual(types.TypeKind.string, f[0].ty.kind);
    try std.testing.expect(f[1].ty.kind == .int and !f[1].ty.nullable);
    try std.testing.expect(f[2].ty.kind == .int and f[2].ty.nullable);
    try std.testing.expect(f[3].ty.kind == .float and f[3].ty.nullable);
    try std.testing.expect(f[4].ty.kind == .string and f[4].ty.nullable);

    const bad = try a.alloc(ast.QualName, 1);
    bad[0] = .{ .parts = &.{"zzz"} };
    try std.testing.expectError(error.AnalyzeFailed, aggregatePlan(a, in, .{ .aggs = aggs, .by = bad }, &pm, &diag));
    try std.testing.expect(std.mem.indexOf(u8, diag.msg, "zzz") != null);
}

test "selectCols: `* except` drops the named columns and keeps source order" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const I = types.Type.init(.int);
    const in = types.Schema{ .fields = &.{
        .{ .name = "a", .ty = I },
        .{ .name = "b", .ty = I },
        .{ .name = "c", .ty = I },
    } };
    var pm = std.StringHashMap(*const ast.Expr).init(a);
    var diag = Diag{};
    const items = [_]ast.SelectItem{.{ .star_except = &.{"b"} }};
    const cols = try selectCols(a, in, &items, &pm, &diag);
    try std.testing.expectEqual(@as(usize, 2), cols.len);
    try std.testing.expectEqualStrings("a", cols[0].name);
    try std.testing.expectEqualStrings("c", cols[1].name);
    try std.testing.expectEqual(@as(usize, 0), cols[0].source.passthrough);
    try std.testing.expectEqual(@as(usize, 2), cols[1].source.passthrough);
}

test "analyze rejects `* rename` onto a duplicate column name" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    try expectAnalyzeErr(ar.allocator(), "id,name\n1,x\n", "SELECT * RENAME (id AS name) FROM '$IN'");
}

test "analyze rejects `is empty` on a non-string operand" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    try expectAnalyzeErr(ar.allocator(), "id,amount\n1,100\n", "SELECT * FROM '$IN' WHERE CAST(amount AS INT) IS EMPTY");
}

test "formatLabel names the reader, not the connector" {
    const no_hints: []const ast.Hint = &.{};
    // The bug: a bare path lowers to the `csv` connector, so a serial parquet scan
    // announced itself as csv in the run summary.
    try std.testing.expectEqualStrings("parquet", formatLabel("t.parquet", no_hints));
    try std.testing.expectEqualStrings("csv", formatLabel("t.csv", no_hints));
    // The innermost name wins, so a compressed or archived CSV is still csv.
    try std.testing.expectEqualStrings("csv", formatLabel("t.csv.gz", no_hints));
    try std.testing.expectEqualStrings("csv", formatLabel("a.zip :: t.csv", no_hints));
    // An explicit hint outranks the extension.
    const as_parquet: []const ast.Hint = &.{.{ .key = "format", .value = .{ .str = "parquet" }, .pos = .{ .line = 1, .col = 1 } }};
    try std.testing.expectEqualStrings("parquet", formatLabel("t.dat", as_parquet));
    // An unknown extension is `unreadableTarget`'s error to raise; the label just
    // must not crash or claim parquet.
    try std.testing.expectEqualStrings("csv", formatLabel("t.dat", no_hints));
}

test "analyze rejects `?.` safe navigation on a plain column reference" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    try expectAnalyzeErr(ar.allocator(), "id,name\n1,x\n", "SELECT name?.foo AS v FROM '$IN'");
}

test "check accepts a filter over a statement-level LET (and rejects a LET/PARAM clash)" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,amount\n1,100\n" });
    const base = try tmp.dir.realpathAlloc(a, ".");
    const in = try std.fs.path.join(a, &.{ base, "in.csv" });

    const src = try std.fmt.allocPrint(a,
        \\PARAM floor INT DEFAULT 10;
        \\LET cutoff = $floor * 5;
        \\LOAD INTO '/tmp/x.csv' AS SELECT id FROM '{s}' WHERE CAST(amount AS INT) >= $cutoff;
    , .{in});
    const prog = try parse(a, src);
    var diag = Diag{};
    const plan = try analyze(a, prog, &diag);
    try std.testing.expectEqual(@as(usize, 1), plan.outputs.len);
    try std.testing.expectEqualStrings("filter", plan.outputs[0].stages[0].kind);

    const clash = try std.fmt.allocPrint(a,
        \\PARAM cutoff INT DEFAULT 10;
        \\LET cutoff = 5;
        \\LOAD INTO '/tmp/x.csv' AS SELECT id FROM '{s}';
    , .{in});
    const cprog = try parse(a, clash);
    var cdiag = Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyze(a, cprog, &cdiag));
    try std.testing.expect(std.mem.indexOf(u8, cdiag.msg, "declared twice") != null);
}

test "check rejects a script whose THROW guard fires, and passes one whose WHEN is false" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,amount\n1,100\n" });
    const base = try tmp.dir.realpathAlloc(a, ".");
    const in = try std.fs.path.join(a, &.{ base, "in.csv" });

    const fires = try std.fmt.allocPrint(a,
        \\PARAM tbl STRING DEFAULT '';
        \\THROW 'tbl is required (e.g. -p tbl=SC5)' WHEN $tbl IS EMPTY;
        \\LOAD INTO '/tmp/x.csv' AS SELECT id FROM '{s}';
    , .{in});
    var fdiag = Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyze(a, try parse(a, fires), &fdiag));
    try std.testing.expectEqualStrings("tbl is required (e.g. -p tbl=SC5)", fdiag.msg);

    const holds = try std.fmt.allocPrint(a,
        \\PARAM tbl STRING DEFAULT 'SC5';
        \\THROW 'tbl is required (e.g. -p tbl=SC5)' WHEN $tbl IS EMPTY;
        \\LOAD INTO '/tmp/x.csv' AS SELECT id FROM '{s}';
    , .{in});
    var hdiag = Diag{};
    const plan = try analyze(a, try parse(a, holds), &hdiag);
    try std.testing.expectEqual(@as(usize, 1), plan.outputs.len);

    const bare = try std.fmt.allocPrint(a,
        \\LET tag = 'zz';
        \\THROW 'unreachable branch: ' || $tag;
        \\LOAD INTO '/tmp/x.csv' AS SELECT id FROM '{s}';
    , .{in});
    var bdiag = Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyze(a, try parse(a, bare), &bdiag));
    try std.testing.expectEqualStrings("unreachable branch: zz", bdiag.msg);
}
