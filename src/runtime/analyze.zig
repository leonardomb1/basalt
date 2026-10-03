//! Static analysis: parsed program → validated `Plan` IR, without executing or
//! connecting. Shared groundwork for `EXPLAIN` (render the IR) and `basalt
//! check` (validate and report). It does full structural + reference validation
//! and resolves what it can read locally — a CSV header, a Parquet footer. A
//! schema only the source can describe (a database table, a remote object)
//! stays null and renders as `schema: unresolved`.

const std = @import("std");
const ast = @import("../lang/ast.zig");
const aggregates = @import("../lang/aggregates.zig");
const expand = @import("../lang/expand.zig");
const types = @import("../lang/types.zig");
const pushdown = @import("pushdown.zig");
const Dialect = @import("../connect/sql.zig").Dialect;
const eval = @import("../exec/eval.zig");
const csv = @import("../connect/csv.zig");
const pqdecode = @import("../connect/pqdecode.zig");
const pqwrite = @import("../connect/pqwrite.zig");
const arrowread = @import("../connect/arrowread.zig");
const azure = @import("../connect/azure.zig");
const s3 = @import("../connect/s3.zig");
const zipsrc = @import("../connect/zipsrc.zig");
const xlsx = @import("../connect/xlsx.zig");
const registry = @import("../connect/registry.zig");
const body_stmt_rule = @import("env.zig").body_stmt_rule;

pub const Diag = struct {
    buf: [512]u8 = undefined,
    msg: []const u8 = "",
    /// Where the failing stage/pipeline sits in the script. Cleared by `fail` and
    /// stamped as the error unwinds through the analyzer, innermost stage first.
    pos: ?ast.Pos = null,
    /// End of the offending text, when the error names a span rather than a stage.
    end: ?ast.Pos = null,

    pub fn stamp(self: *Diag, pos: ast.Pos) void {
        if (self.pos == null) self.pos = pos;
    }
};

pub const Error = error{ AnalyzeFailed, OutOfMemory };

fn fail(diag: *Diag, comptime fmt: []const u8, args: anytype) error{AnalyzeFailed} {
    diag.msg = std.fmt.bufPrint(&diag.buf, fmt, args) catch "analysis error";
    diag.pos = null;
    diag.end = null;
    return error.AnalyzeFailed;
}

/// `fail`, underlining `span` when the offending text has one.
fn failAt(diag: *Diag, span: ?ast.Span, comptime fmt: []const u8, args: anytype) error{AnalyzeFailed} {
    const e = fail(diag, fmt, args);
    if (span) |s| {
        diag.pos = s.start;
        diag.end = s.end;
    }
    return e;
}

/// Param name → the literal expression it substitutes to (CLI values for the
/// executor; declared defaults for offline analysis). Deliberately NOT a `pub`
/// named alias — re-exporting a StringHashMap type makes `refAllDeclsRecursive`
/// (the test harness) recurse the whole hashmap decl tree and crash. Callers spell
/// `std.StringHashMap(*const ast.Expr)` directly; it's the same type.
const ParamMap = std.StringHashMap(*const ast.Expr);

/// Deep-copy `expr`, replacing each `$name` that names a param or LET with its
/// literal. A bare name is a column — a PARAM of the same name never stands in
/// for it. No params ⇒ returns the original (no copy).
const SubstCtx = struct { arena: std.mem.Allocator, params: *const ParamMap };

fn substRecur(ctx: SubstCtx, e: *const ast.Expr) Error!*ast.Expr {
    return @constCast(try substExpr(ctx.arena, e, ctx.params));
}

pub fn substExpr(arena: std.mem.Allocator, expr: *const ast.Expr, params: *const ParamMap) Error!*const ast.Expr {
    if (params.count() == 0) return expr;
    if (expr.* == .field) {
        const q = expr.field;
        if (q.dollar and q.parts.len == 1) if (params.get(q.parts[0])) |lit| return lit;
        return expr;
    }
    return ast.rebuildExpr(arena, expr, SubstCtx{ .arena = arena, .params = params }, substRecur);
}

/// `stages` with every `$param` / `$let` in a filter replaced by its value. The
/// parser reads `$since` as a name, which the engine resolves when it evaluates;
/// pushdown translates the predicate before that, and sent `[since]` to the
/// source as a column. Applied ahead of every descent, it hands them literals.
/// Returns `stages` itself when no filter names a param.
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

fn mk(arena: std.mem.Allocator, e: ast.Expr) Error!*const ast.Expr {
    const p = try arena.create(ast.Expr);
    p.* = e;
    return p;
}

fn exprType(arena: std.mem.Allocator, in: types.Schema, e: *const ast.Expr, diag: *Diag) Error!types.Type {
    var ctx = eval.TypeCtx{ .schema = in, .arena = arena };
    return ctx.typeOf(e) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.TypeError => return failAt(diag, ctx.span, "{s}", .{ctx.msg}),
    };
}

/// One resolved output column of `select`: either a passthrough of an input index
/// or a computed (already param-substituted) expression, plus its name and type.
pub const Col = struct {
    name: []const u8,
    ty: types.Type,
    source: union(enum) { passthrough: usize, expr: *const ast.Expr },
    /// A passed-through column keeps the join side it came from (`Schema.Field`).
    rel: []const u8 = "",
    base: []const u8 = "",
};

pub fn selectCols(arena: std.mem.Allocator, in: types.Schema, items: []const ast.SelectItem, params: *const ParamMap, diag: *Diag) Error![]Col {
    var cols = std.array_list.Managed(Col).init(arena);
    for (items) |item| switch (item) {
        .star => for (in.fields, 0..) |f, idx| try cols.append(.{ .name = f.name, .ty = f.ty, .source = .{ .passthrough = idx }, .rel = f.rel, .base = f.base }),
        .star_except => |names| for (in.fields, 0..) |f, idx| {
            if (nameIn(names, f.name)) continue;
            try cols.append(.{ .name = f.name, .ty = f.ty, .source = .{ .passthrough = idx }, .rel = f.rel, .base = f.base });
        },
        .star_rename => |renames| {
            for (renames) |r| if (in.indexOf(r.from) == null)
                return fail(diag, "unknown rename field `{s}`", .{r.from});
            for (in.fields, 0..) |f, idx| {
                const nm = renameTo(renames, f.name) orelse f.name;
                for (in.fields[0..idx]) |g|
                    if (std.mem.eql(u8, nm, renameTo(renames, g.name) orelse g.name))
                        return fail(diag, "`* rename` produces duplicate column `{s}`", .{nm});
                try cols.append(.{ .name = nm, .ty = f.ty, .source = .{ .passthrough = idx }, .rel = f.rel, .base = f.base });
            }
        },
        .field => |q| {
            const idx = in.resolve(q.parts) orelse return failAt(diag, q.span, "unknown field `{s}`", .{lastPart(q)});
            // `b.x` is called `x` in the output, as SQL has it — unless `a.x` is
            // already there, when it keeps the name the join gave it.
            var nm = lastPart(q);
            for (cols.items) |c| if (std.mem.eql(u8, c.name, nm)) {
                nm = in.fields[idx].name;
                break;
            };
            try cols.append(.{ .name = nm, .ty = in.fields[idx].ty, .source = .{ .passthrough = idx }, .rel = in.fields[idx].rel, .base = in.fields[idx].base });
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
    for (cols, fields) |c, *f| f.* = .{ .name = c.name, .ty = c.ty, .rel = c.rel, .base = c.base };
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
    for (names, 0..) |q, i| idxs[i] = in.resolve(q.parts) orelse return failAt(diag, q.span, "unknown field `{s}`", .{lastPart(q)});
    return idxs;
}

pub const Agg = struct { func: ast.AggFunc, arg: ?*const ast.Expr, ty: types.Type, name: []const u8, distinct: bool = false };
pub const AggregatePlan = struct { by: []usize, aggs: []Agg, schema: types.Schema };

pub fn aggregatePlan(arena: std.mem.Allocator, in: types.Schema, ag: ast.Aggregate, params: *const ParamMap, diag: *Diag) Error!AggregatePlan {
    var fields = std.array_list.Managed(types.Schema.Field).init(arena);
    const by = try arena.alloc(usize, ag.by.len);
    for (ag.by, 0..) |q, i| {
        const idx = in.resolve(q.parts) orelse return fail(diag, "unknown group field `{s}`", .{lastPart(q)});
        by[i] = idx;
        try fields.append(.{ .name = lastPart(q), .ty = in.fields[idx].ty, .rel = in.fields[idx].rel, .base = in.fields[idx].base });
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
    const sp = aggregates.spec(func);
    if (sp.arg == .star_or_any) return types.Type.init(.int);
    const a = arg orelse return fail(diag, "this aggregate requires an argument", .{});
    const at = try exprType(arena, in, a, diag);
    if (!sp.arg.accepts(at))
        return fail(diag, "`{s}` needs a {s} argument, got {s}", .{ sp.names[0], sp.arg.word(), try at.name(arena) });
    return switch (sp.result) {
        .count => types.Type.init(.int),
        .sum => switch (at.kind) {
            .float => types.Type.init(.float).withNull(true),
            // A decimal sum stays a decimal: typing it as an int reported
            // the accumulated *unscaled* integer, so 1.5+2.25+3.125 came
            // back as 68750.
            .decimal => at.withNull(true),
            else => types.Type.init(.int).withNull(true),
        },
        .float => types.Type.init(.float).withNull(true),
        .same => at.withNull(true),
        .bool => types.Type.init(.bool).withNull(true),
        .int => types.Type.init(.int).withNull(true),
    };
}

/// Result type of one window function over its source column (`int` for the
/// argument-less ones). Ranking counts rows, so it is a non-null int; MIN/MAX,
/// LAG and LEAD keep the column's type; AVG is always a float; SUM keeps the
/// column's family. Everything with an argument is nullable — a peer group of
/// nothing but nulls has no answer, and a partition's first row has no LAG.
pub fn windowFuncType(kind: ast.WinKind, src: types.Type) types.Type {
    return switch (kind) {
        .row_number, .rank, .dense_rank, .count => types.Type.init(.int),
        .min, .max, .lag, .lead => src.asNullable(),
        .avg => types.Type.init(.float).asNullable(),
        .sum => (if (src.kind == .int) types.Type.init(.int) else types.Type.init(.float)).asNullable(),
    };
}

/// Output schema of a window stage: the input, then one appended column per
/// function. The same rule the planner applies, so `EXPLAIN` can show the
/// schema past a window instead of `unresolved`.
pub fn windowSchema(arena: std.mem.Allocator, in: types.Schema, wd: ast.Window, diag: *Diag) Error!types.Schema {
    _ = try fieldIndices(arena, in, wd.partition_by, diag);
    const oqs = try arena.alloc(ast.QualName, wd.order_by.len);
    for (wd.order_by, oqs) |sk, *q| q.* = sk.field;
    _ = try fieldIndices(arena, in, oqs, diag);
    const fields = try arena.alloc(types.Schema.Field, in.fields.len + wd.funcs.len);
    @memcpy(fields[0..in.fields.len], in.fields);
    for (wd.funcs, 0..) |f, i| {
        var src = types.Type.init(.int);
        if (f.arg) |q| src = in.fields[(try fieldIndices(arena, in, &[_]ast.QualName{q}, diag))[0]].ty;
        fields[in.fields.len + i] = .{ .name = f.out, .ty = windowFuncType(f.kind, src) };
    }
    return .{ .fields = fields };
}

pub const ExplodePlan = struct { idx: usize, schema: types.Schema };

pub fn explodePlan(arena: std.mem.Allocator, in: types.Schema, ex: ast.Explode, diag: *Diag) Error!ExplodePlan {
    const idx = in.indexOf(ex.field) orelse return fail(diag, "unknown field `{s}`", .{ex.field});
    const fty = in.fields[idx].ty;
    if (!(fty.kind == .string or fty.kind == .bytes))
        return fail(diag, "explode needs a string column (it splits a delimited value or a JSON array)", .{});
    const fields = try arena.alloc(types.Schema.Field, in.fields.len);
    for (in.fields, fields, 0..) |f, *out, i| {
        // A JSON array may hold nulls; a split string never yields one.
        const ty = if (ex.json) types.Type.init(.string).asNullable() else types.Type.init(.string);
        out.* = if (i == idx) .{ .name = ex.as_name orelse f.name, .ty = ty } else f;
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
    // A left key may be qualified by an earlier join's alias (`b.k = c.k`).
    const l_in_l = left.resolve(lq.parts);
    const l_in_r = right.indexOf(ln);
    const r_in_l = left.resolve(rq.parts);
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
    for (left.fields) |f| try fields.append(.{ .name = f.name, .ty = if (left_nullable) f.ty.asNullable() else f.ty, .rel = f.rel, .base = f.base });
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
        try fields.append(.{
            .name = name,
            .ty = if (right_nullable) f.ty.asNullable() else f.ty,
            .rel = if (j.alias.len != 0) j.alias else j.binding,
            .base = f.name,
        });
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
    /// A join's right side, when it is a SQL read: what it scans and the WHERE it
    /// sends (`plan.prepareJoinSide`), shown under the join.
    right_scan: ?[]const u8 = null,
    right_pushdown: []const u8 = "",
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
    /// A LIMIT descended into the SQL read, and it caps everything the pipeline
    /// holds at once: at most this many rows arrive to be sorted and cut.
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
        .string, .bytes => mk(arena, .{ .str_lit = raw }),
        .date, .time, .timestamp, .decimal => typedParam(arena, ty, try mk(arena, .{ .str_lit = raw })),
        else => null,
    };
}

/// A PARAM's value as the runtime binds it: a DATE, TIME, TIMESTAMP or DECIMAL
/// is its text CAST to the declared type (`env.mkLit`), so a check types `$d` as
/// the run does — `date_add('day', 1, $d)` is fine for a DATE param.
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

/// Arguments a builtin takes as syntax rather than data — a date unit, a
/// `strftime` format — checked wherever a literal one appears. Typing checks
/// them too, but only where the columns' types are known, which a SQL table's
/// are not until it is read: `date_trunc('fortnight', ts)` over one checked out
/// and failed at run time.
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

/// A table that exists where the script runs but that the script does not
/// declare — a notebook's other cells. `FROM name` reads it; `schema`, when the
/// caller knows it, types what follows, and null leaves it unresolved (as a live
/// SQL table is to an analysis that does not connect).
pub const KnownTable = struct { name: []const u8, schema: ?types.Schema = null };

/// One problem the analysis found.
pub const Issue = struct { msg: []const u8, pos: ?ast.Pos = null, end: ?ast.Pos = null };

pub const Options = struct {
    overrides: []const ParamOverride = &.{},
    known_tables: []const KnownTable = &.{},
    /// Report every problem: each statement is checked on its own, a failure is
    /// recorded here, and the rest are still checked. Null: the first one fails.
    issues: ?*std.array_list.Managed(Issue) = null,
    /// A script of declarations alone is whole — a notebook cell that only
    /// declares — rather than "no output pipeline".
    declarations_only: bool = false,
};

pub fn analyzeOpts(arena: std.mem.Allocator, raw_program: ast.Program, opts: Options, diag: *Diag) error{ AnalyzeFailed, OutOfMemory }!Plan {
    var p = analyzeInner(arena, raw_program, opts, diag);
    // With an issue list, a failure before the statements (an expansion, a
    // parameter) is one more issue — there is nothing after it to go on with.
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
    const program = expand.expandProgram(arena, raw_program, null, &expand_msg) catch |e| switch (e) {
        error.OutOfMemory => return error.OutOfMemory,
        error.ExpandFailed => return fail(diag, "{s}", .{expand_msg}),
    };
    if (program.stmts.len == 0 or program.stmts[0] != .kind)
        return fail(diag, "script must begin with a @kind tag", .{});
    const kind_name = @tagName(program.stmts[0].kind.kind);

    var bindings = std.StringHashMap(ast.Pipeline).init(arena);
    var connections = std.StringHashMap(ast.Connection).init(arena);
    for (program.stmts[1..]) |s| if (s == .connection) try connections.put(s.connection.name, s.connection);
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
        // what the run's CAST would refuse, refused here with its words
        switch (p.ty.kind) {
            .date, .time, .timestamp, .decimal => if (text) |t| {
                _ = eval.castValueTyped(arena, .{ .string = t }, p.ty) catch
                    return fail(diag, "PARAM `{s}`: `{s}` is not a {s}", .{ p.name, t, try p.ty.name(arena) });
            },
            else => {},
        }
        try params_map.put(p.name, bound orelse try typedParam(arena, p.ty, if (p.default) |d| d else try typedZero(arena, p.ty)));
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
        if (l.expr) |le| {
            try params_map.put(l.name, try substExpr(arena, le, &params_map));
        } else {
            // A query LET's value only exists at run time. For checking, an
            // unknown-typed null stands in — it unifies with any comparison,
            // which is all the checker needs from it.
            const ph = try arena.create(ast.Expr);
            ph.* = .null_lit;
            try params_map.put(l.name, ph);
        }
    };
    // Guards run against params and LETs only, before the body-var placeholders
    // below can make a `$name` resolve to a stand-in the script never sees.
    for (program.stmts) |s| if (s == .throw) try checkThrow(arena, s.throw, &params_map, diag);
    // Body-scoped variables (for-each loop vars, statement-fn params) bind per
    // row/call at run time; for checking, a typed placeholder (or unknown-typed
    // null) keeps `$var`-as-value expressions lenient instead of "unknown field".
    try bindBodyVars(arena, program.stmts, &params_map);

    var ctx = Ctx{ .arena = arena, .bindings = &bindings, .connections = &connections, .params = &params_map, .diag = diag, .known = opts.known_tables, .issues = opts.issues };

    var out_plans = std.array_list.Managed(Output).init(arena);
    try ctx.checkStmts(program.stmts[1..], &out_plans, true);

    return .{ .kind = kind_name, .outputs = try out_plans.toOwnedSlice() };
}

/// Does a stage name a column through a `${...}` template — `IDENTIFIER(<expr>)`,
/// which the parser lowers to a field whose name is the template?
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
    known: []const KnownTable = &.{},
    issues: ?*std.array_list.Managed(Issue) = null,

    /// A statement's failure: recorded, and the caller goes on to the next
    /// statement — or, without an issue list, returned as the analysis's error.
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

    /// A CTE's literal arguments, checked where it is declared: it is only typed
    /// where it is read, and then only when its source's columns are known.
    fn checkBindingLiterals(self: *Ctx, p: ast.Pipeline) Error!void {
        for (p.stages) |st| checkStageLiterals(self.diag, st) catch |e| return self.note(e);
    }

    fn knownTable(self: *Ctx, name: []const u8) ?KnownTable {
        for (self.known) |k| if (std.mem.eql(u8, k.name, name)) return k;
        return null;
    }

    /// The statements in the order the executor runs them: a `WITH` is visible to
    /// what follows it, a body's `WITH` is scoped to that body, and the one rule
    /// `runForStmt` applies per row is applied here once, with a position — so
    /// `check` and `run` name the same constraint.
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
            .func => |fd| if (fd.body == .stmts) try self.checkBody(fd.body.stmts, outs),
            .let_const => |l| if (!top) try self.note(self.failAt(l.pos, "LET `{s}` must be declared at the top level of the script", .{l.name})),
            .param, .kind, .connection, .call, .throw, .print => {},
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
        // The same rewrite the runtime applies, so the plan `EXPLAIN` prints is the
        // plan that runs — a filter shown below a join really did descend, and one
        // shown above it really did not.
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
        // Tracks whether the shape is one the runtime fans out over key ranges. It
        // dispatches three of them for a SQL source: map-only, an aggregate with a
        // sort/limit tail, and a map+join. Anything else — a DISTINCT, or a sort
        // with no aggregate under it — falls to the serial driver.
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
                .morsel_parallel = !splittable and laneHints(stages[0]) and morselParallelRead(source.connector, stages[0].node),
                // the pushed sort is then the only breaker, over the capped rows
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
                    // a script's own CTE shadows a table the session knows
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
            if (s3.bucketNameError(w.target)) |why|
                return fail(self.diag, "`{s}` is not a valid S3 target: {s}", .{ w.target, why });
            // A `stdout` sink has no target path to name a format for.
            if (std.mem.eql(u8, w.connector, "csv") and w.target.len > 0) {
                const fmt = try formatFromHints(hints, self.diag);
                if (unreadableTarget(w.target, fmt)) |why|
                    return fail(self.diag, "cannot write `{s}`: {s}", .{ w.target, why });
                if ((fmt orelse formatOfPath(w.target)) == .xlsx)
                    return fail(self.diag, "cannot write `{s}`: basalt reads Excel workbooks but does not write them; write a `.csv` or `.parquet`", .{w.target});
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

    /// The schema a binding's pipeline produces, propagated stage by stage from
    /// its source; null past anything only the source can describe.
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

    /// A stage past the point the schema is known is not typed, but a `$name`
    /// that no PARAM, LET or loop variable binds is wrong whatever the columns
    /// turn out to be — so `check` says so offline too, not only the run.
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

    /// Output schema after a stage (type-checking expressions along the way).
    /// Returns null where the flow becomes unresolvable — a source only a
    /// connection can describe, or a join whose right side is one.
    fn propagate(self: *Ctx, in: types.Schema, node: ast.Stage.Node) Error!?types.Schema {
        // A name computed per row (`IDENTIFIER(...)` in a SELECT list, GROUP BY,
        // ORDER BY or DISTINCT ON) has no column to type until the row renders it;
        // from here on the schema is unresolved, as it is behind a live source.
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
            // The same joinPlan the runtime builds from, over the binding's offline
            // schema, so an aggregate or filter after the join is checked like any
            // other stage. It used to stop here, and `check` said ok to a column
            // that did not exist as long as a join sat in front of it.
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
        // The right side's read, readied as the runtime readies it
        // (`plan.prepareJoinSide`), so the WHERE it sends is on the plan too.
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
    /// the runtime does (`plan.inlineHeadBindings`) — so a binding's WHERE is shown
    /// descending. `via` is the first binding laid out, if any.
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

    /// What the SQL read leading `stages` would be sent as its WHERE — a raw
    /// `PUSHDOWN`, AND the contiguous filters after it — with the params' values
    /// in, as the run sends them. Null when the lead is no SQL read.
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
        // analysis does not connect, and a text comparison's descent is the
        // column's collation's to decide
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
            if (st.right_scan) |rs| {
                try indent(w, depth + 1);
                try w.print("right  scan  {s}\n", .{rs});
                if (st.right_pushdown.len > 0) {
                    try indent(w, depth + 2);
                    try w.print("pushdown: {s}\n", .{st.right_pushdown});
                }
            }
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
            if (o.physical.top_n) |n| {
                if (o.physical.has_breaker) try w.print(" (top-N pushed, sorts at most {d} rows)", .{n}) else try w.print(" (limit pushed, at most {d} rows arrive)", .{n});
            } else if (o.physical.has_breaker) try w.writeAll(" (has breaker, materializes)");
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
    if (arrowread.isPath(target)) return "an Arrow IPC file's footer indexes every batch and is written last, so appending means rewriting the file";
    return null;
}

/// The `csv` connector backs every file sink, so the plan has to name the
/// format from the target — otherwise a parquet write reads as `write csv`.
fn sinkKind(node: anytype) []const u8 {
    const path = if (@hasField(@TypeOf(node), "target")) node.target else node.detail;
    if (std.mem.eql(u8, node.connector, "csv")) {
        if (pqwrite.Writer.isPath(path)) return "parquet";
        if (arrowread.isPath(path)) return "arrow";
        if (xlsx.isPath(path)) return "xlsx";
    }
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
    const c = registry.Connector.parse(connector) orelse return false;
    return c.isBuiltinSource();
}

fn isSqlConnector(connector: []const u8) bool {
    return registry.SqlKind.parse(connector) != null;
}

fn isSqlSource(connector: []const u8) bool {
    return dialectOf(connector) != null;
}

/// The pushdown dialect for a connector, or null if it's not a SQL source.
fn dialectOf(connector: []const u8) ?Dialect {
    const c = registry.Connector.parse(connector) orelse return null;
    return (c.sqlRead() orelse return null).dialect;
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
pub const FileFormat = enum {
    csv,
    parquet,
    /// Arrow IPC: `.arrow` / `.feather` / `.ipc` (file) or `.arrows` (stream).
    arrow,
    /// An Excel workbook, `.xlsx` / `.xlsm` — read only.
    xlsx,
};

/// The format a file read resolves to: the named one, else the extension's,
/// else CSV. The one place the CSV fast paths ask, so a binary format is never
/// memory-mapped and parsed as text.
pub fn readFormat(path: []const u8, explicit: ?FileFormat) FileFormat {
    return explicit orelse formatOfPath(path) orelse .csv;
}

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
    inline for (.{ "arrow", "ipc", "feather" }) |n| if (std.ascii.eqlIgnoreCase(s, n)) return .arrow;
    inline for (.{ "xlsx", "excel" }) |n| if (std.ascii.eqlIgnoreCase(s, n)) return .xlsx;
    return fail(diag, "unknown format `{s}` (csv, parquet, arrow, xlsx)", .{s});
}

/// `WITH (sheet = 'Vendas', header = false, range = 'B3:F200')` for a workbook
/// read, validated here so `check` turns away a malformed range before a run.
pub fn xlsxOptions(hints: []const ast.Hint, diag: *Diag) Error!xlsx.Options {
    var o = xlsx.Options{};
    o.sheet = hintText(hints, "sheet");
    if (hintText(hints, "range")) |r| o.range = xlsx.parseRange(r) orelse
        return fail(diag, "`range = '{s}'` is not a cell range like `A1:F100`, `B3` or `B3:F`", .{r});
    for (hints) |h| {
        if (!std.mem.eql(u8, h.key, "header")) continue;
        o.header = switch (h.value) {
            .flag => true,
            .int => |n| n != 0,
            .str, .ident => |s| if (std.ascii.eqlIgnoreCase(s, "true")) true else if (std.ascii.eqlIgnoreCase(s, "false")) false else return fail(diag, "`header` is true or false, not `{s}`", .{s}),
        };
    }
    return o;
}

/// The extension basalt reads a path as, or null when it carries none it knows.
///
/// `csv.dataName` walks the chain first, so `orders.csv.gz` and
/// `inf.zip :: inf_diario.csv` both answer `.csv` — the name that matters is the
/// innermost one, not the container's.
fn formatOfPath(path: []const u8) ?FileFormat {
    const bare = csv.dataName(path);
    if (pqwrite.Writer.isPath(bare)) return .parquet;
    if (arrowread.isPath(bare)) return .arrow;
    if (xlsx.isPath(bare)) return .xlsx;
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
    if (fmt == .xlsx and csv.splitCodec(path).codec != .none)
        return "an Excel workbook is a zip archive already, so it is read uncompressed; decompress it first";
    if (fmt == .arrow) {
        if (csv.splitCodec(path).codec != .none)
            return "an Arrow IPC file is read memory-mapped, so it cannot be read through compression; decompress it first (IPC compresses its own buffers)";
        if (std.mem.indexOf(u8, path, "://") != null)
            return "Arrow IPC is read from a local file; fetch it first";
    }

    if (explicit != null) return null;
    if (fmt != null) return null;
    return "basalt handles `.csv`, `.parquet`, Arrow IPC (`.arrow`, `.feather`, `.ipc`, `.arrows`) and Excel (`.xlsx`, read only), a CSV optionally `.gz`/`.zst` compressed or inside a `.zip`; name the format with `WITH (format = 'csv')` if the extension differs";
}

/// Why this archive reference cannot be read as one table, or null when it can.
///
/// Opens the archive to answer, and stays quiet when it cannot be opened — a script
/// may legitimately be checked before its data has been fetched, which is what
/// `offlineSchema` already assumes. Everything archive-shaped is decided here so the
/// guarantee `unreadableTarget` gives for a loose file also holds inside a
/// container: a `.json` member is refused exactly like a `.json` file.
///
/// A remote archive is opened only when `online`: `check` stays off the network,
/// and judges just the member name the script wrote.
pub fn archiveProblem(arena: std.mem.Allocator, path: []const u8, explicit: ?FileFormat, online: bool) ?[]const u8 {
    const ar = csv.splitArchive(path) orelse return null;
    if (!online and csv.CsvReader.isUrl(ar.archive)) {
        const m = ar.member orelse return null;
        return memberProblem(arena, m, explicit);
    }

    const members = zipsrc.names(arena, ar.archive) catch return null;
    if (members.len == 0) return "the archive holds no files";

    const chosen = if (ar.member) |want| blk: {
        for (members) |m| if (std.mem.eql(u8, m, want)) break :blk m;
        return std.fmt.allocPrint(arena, "no file `{s}` in the archive ({s})", .{ want, joinNames(arena, members) }) catch null;
    } else if (members.len > 1)
        return std.fmt.allocPrint(arena, "the archive holds {d} files; name one with `:: <name>` ({s})", .{ members.len, joinNames(arena, members) }) catch null
    else
        members[0];

    return memberProblem(arena, chosen, explicit);
}

/// Why the chosen member cannot be streamed as a table, or null when it can.
fn memberProblem(arena: std.mem.Allocator, chosen: []const u8, explicit: ?FileFormat) ?[]const u8 {
    if (explicit == null and formatOfPath(chosen) == null)
        return std.fmt.allocPrint(arena, "`{s}` inside it is not a `.csv` or `.parquet`; name the format with `WITH (format = 'csv')`", .{chosen}) catch null;
    if ((explicit orelse formatOfPath(chosen)) == .parquet)
        return "parquet needs random access, so it cannot be read out of an archive; extract it first";
    if ((explicit orelse formatOfPath(chosen)) == .arrow)
        return "an Arrow IPC file is read memory-mapped, so it cannot be read out of an archive; extract it first";
    if ((explicit orelse formatOfPath(chosen)) == .xlsx)
        return "an Excel workbook is itself a zip archive, so it cannot be read out of another; extract it first";
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
/// Whether a file read's hints still let it fan out over lanes: its CSV dialect
/// (`delimiter`, `encoding`), which every lane reads its chunk with, and a
/// `format` naming what the path's extension already says. Any other hint keeps
/// the read on the serial reader. The runtime (`lanes.laneEligible`) and EXPLAIN
/// both ask this, so the plan cannot claim a fan-out the run does not take.
pub fn laneHints(st: ast.Stage) bool {
    for (st.hints) |h| {
        if (std.mem.eql(u8, h.key, "delimiter") or std.mem.eql(u8, h.key, "delim") or std.mem.eql(u8, h.key, "encoding")) continue;
        if (std.mem.eql(u8, h.key, "format")) {
            if (st.node != .read or st.node.read.form != .path) return false;
            var d = Diag{};
            const f = (formatFromHints(st.hints, &d) catch return false) orelse return false;
            if (f != readFormat(st.node.read.form.path, null)) return false;
            continue;
        }
        return false;
    }
    return true;
}

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
    // an Arrow file reads serially: its batches are not independent morsels yet
    if (arrowread.isPath(path)) return false;
    // nor does a workbook: a sheet is one stream of XML
    if (xlsx.isPath(path)) return false;
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
        var fdiag = Diag{};
        const explicit = formatFromHints(hints, &fdiag) catch return null;
        if (readFormat(rd.form.path, explicit) == .arrow) {
            const ar = arrowread.Reader.open(arena, rd.form.path) catch return null;
            defer ar.close();
            return ar.schema;
        }
        if (readFormat(rd.form.path, explicit) == .xlsx) {
            // the same first pass the run makes, so `check` and the run agree
            var odiag = Diag{};
            const opts = xlsxOptions(hints, &odiag) catch return null;
            const xr = xlsx.Reader.open(arena, arena, rd.form.path, opts) catch return null;
            defer xr.close();
            return xr.schema;
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

    const src = try std.fmt.allocPrint(
        a,
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
    // a workbook is read; the old binary `.xls` and a compressed one are not
    try std.testing.expect(unreadableTarget("/data/book.xlsx", null) == null);
    try std.testing.expect(unreadableTarget("/data/book.xlsx.gz", null) != null);
    try std.testing.expect(unreadableTarget("/data/book.xls", null) != null);
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

test "analyze pushdown preview: a CTE, derived table or table function at the head sends its WHERE to the source" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const conn = "CREATE CONNECTION pg TYPE postgres OPTIONS (host = 'h', user = 'u', password = 'p', database = 'd');\n";
    // Through a binding, the table used to be read whole and filtered in the engine.
    const queries = [_][]const u8{
        "LOAD INTO '/tmp/x.csv' AS WITH o AS (SELECT id, amount FROM pg.orders WHERE amount > 0) SELECT id FROM o;",
        "LOAD INTO '/tmp/x.csv' AS SELECT id FROM (SELECT id, amount FROM pg.orders WHERE amount > 0) d;",
        "CREATE FUNCTION pos(lo INT) RETURNS TABLE AS SELECT id, amount FROM pg.orders WHERE amount > $lo;\nLOAD INTO '/tmp/x.csv' AS SELECT id FROM pos(0);",
        // the query's own WHERE, over a renamed column, crosses the binding's SELECT
        "LOAD INTO '/tmp/x.csv' AS SELECT id FROM (SELECT id, amount AS amt FROM pg.orders) d WHERE amt > 0;",
    };
    for (queries) |q| {
        var diag = Diag{};
        const plan = try analyze(a, try parse(a, try std.mem.concat(a, u8, &.{ conn, q })), &diag);
        const src = plan.outputs[0].source;
        try std.testing.expectEqualStrings("postgres", src.connector);
        try std.testing.expectEqualStrings("(\"amount\" > 0)", src.pushdown);
        try std.testing.expect(std.mem.indexOf(u8, src.detail, "(via binding ") != null);
    }
    // A window in the binding keeps it apart: the top-N over `rn` needs to see it so.
    var diag = Diag{};
    const w = try analyze(a, try parse(a, conn ++
        "LOAD INTO '/tmp/x.csv' AS WITH r AS (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM pg.orders WHERE amount > 0) SELECT id FROM r WHERE rn = 1;"), &diag);
    try std.testing.expectEqualStrings("", w.outputs[0].source.pushdown);
}

test "analyze: EXPLAIN shows the WHERE a join's right side sends, under the join" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag = Diag{};
    const plan = try analyze(a, try parse(a,
        \\CREATE CONNECTION pg TYPE postgres OPTIONS (host = 'h', user = 'u', password = 'p', database = 'd');
        \\CREATE FUNCTION active(flag INT) RETURNS TABLE AS SELECT cid, name AS nm FROM pg.customers WHERE active = $flag;
        \\LOAD INTO '/tmp/x.csv' AS SELECT o.id, c.nm FROM pg.orders o JOIN active(1) c ON o.cid = c.cid;
    ), &diag);
    var join: ?Stage = null;
    for (plan.outputs[0].stages) |st| {
        if (std.mem.eql(u8, st.kind, "join")) join = st;
    }
    try std.testing.expectEqualStrings("(\"active\" = 1)", join.?.right_pushdown);
    var out = std.Io.Writer.Allocating.init(a);
    try render(plan, &out.writer);
    try std.testing.expect(std.mem.indexOf(u8, out.written(), "right  scan  postgres  table customers (via binding __tvf") != null);
    try std.testing.expect(std.mem.indexOf(u8, out.written(), "pushdown: (\"active\" = 1)") != null);
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

/// Analyze one CSV-backed query and hand back the plan (or the analyzer's error).
fn analyzeCsv(a: std.mem.Allocator, csv_data: []const u8, query: []const u8, diag: *Diag) !Plan {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = csv_data });
    const base = try tmp.dir.realpathAlloc(a, ".");
    const in = try std.fs.path.join(a, &.{ base, "in.csv" });
    const q = try std.mem.replaceOwned(u8, a, query, "$IN", in);
    const src = try std.fmt.allocPrint(a, "LOAD INTO '/tmp/x.csv' AS {s};", .{q});
    return analyze(a, try parse(a, src), diag);
}

test "analyze checks the stages after a join against the joined schema" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const csv_data = "id,name,amount\n1,x,10\n";

    // Used to pass: the schema went unresolved at the join, so nothing after it
    // was checked and `check` said ok to a column that does not exist.
    var diag = Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyzeCsv(a, csv_data, "WITH r AS (SELECT id AS rid, name AS rname FROM '$IN') SELECT SUM(CAST(nope AS INT)) AS x FROM '$IN' JOIN r ON id = rid", &diag));
    try std.testing.expect(std.mem.indexOf(u8, diag.msg, "nope") != null);

    // A filter after the join is checked the same way.
    diag = Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyzeCsv(a, csv_data, "WITH r AS (SELECT id AS rid, name AS rname FROM '$IN') SELECT id FROM '$IN' JOIN r ON id = rid WHERE missing = 'x'", &diag));
    try std.testing.expect(std.mem.indexOf(u8, diag.msg, "missing") != null);

    // The good query plans, and the join stage carries the joined schema: the
    // left columns plus the binding's, so a right-side column resolves after it.
    diag = Diag{};
    const plan = try analyzeCsv(a, csv_data, "WITH r AS (SELECT id AS rid, name AS rname FROM '$IN') SELECT rname, SUM(CAST(amount AS INT)) AS total FROM '$IN' JOIN r ON id = rid WHERE rname <> '' GROUP BY rname", &diag);
    const stages = plan.outputs[0].stages;
    var join_schema: ?types.Schema = null;
    for (stages) |st| {
        if (std.mem.eql(u8, st.kind, "join")) join_schema = st.out_schema;
    }
    const js = join_schema orelse return error.TestUnexpectedResult;
    try std.testing.expect(js.indexOf("rname") != null);
    try std.testing.expect(js.indexOf("amount") != null);
    try std.testing.expect(stages[stages.len - 1].out_schema != null);
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

test "analyze: an undeclared `$name` is refused by name, never read as the column it spells" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    // The file has a `tag` column; `$tag` used to read it.
    var diag = Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyzeCsv(a, "id,tag\n1,x\n", "SELECT $tag AS t FROM '$IN'", &diag));
    try std.testing.expectEqualStrings("unknown `$tag`: no PARAM, LET or loop variable of that name", diag.msg);
    // Beside an aggregate the parser lifts it as a constant; the name is still refused.
    var grouped = Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyzeCsv(a, "id,g\n1,x\n", "SELECT $tag AS t, g, COUNT(*) AS n FROM '$IN' GROUP BY g", &grouped));
    try std.testing.expectEqualStrings("unknown `$tag`: no PARAM, LET or loop variable of that name", grouped.msg);
    // Over a SQL table `check` cannot type the stages, but an unbound name is
    // wrong whatever the columns are — while a PARAM, a query LET and a loop
    // variable all stay fine.
    const conn = "CREATE CONNECTION pg TYPE postgres OPTIONS (host = 'h', user = 'u', password = 'p', database = 'd');\n";
    var sql_diag = Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyze(a, try parse(a, conn ++ "LOAD INTO '/tmp/x.csv' AS SELECT id FROM pg.orders WHERE day >= $since;"), &sql_diag));
    try std.testing.expectEqualStrings("unknown `$since`: no PARAM, LET or loop variable of that name", sql_diag.msg);
    var ok = Diag{};
    _ = try analyze(a, try parse(a, conn ++
        \\PARAM since DATE DEFAULT '2026-01-01';
        \\LET hi = (SELECT max(id) AS m FROM pg.orders);
        \\LOAD INTO '/tmp/x.csv' AS SELECT id, $since AS s FROM pg.orders WHERE day >= $since AND id <= $hi;
        \\FOR EACH ROW OF (SELECT 'a' AS r) AS (r)
        \\  LOAD INTO '/tmp/y.csv' AS SELECT id FROM pg.orders WHERE region = $r;
        \\END FOR;
    ), &ok);
}

test "analyze: a numeric aggregate refuses a non-numeric argument at plan time" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    // `SUM(date)` used to reach the accumulator and panic on the union access.
    var diag = Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyzeCsv(a, "id,d\n1,2024-01-01\n", "SELECT SUM(d) AS s FROM '$IN'", &diag));
    try std.testing.expectEqualStrings("`sum` needs a numeric argument, got date", diag.msg);
    // Text still passes: it is coerced per row, as the parallel CSV lanes need.
    var ok = Diag{};
    _ = try analyzeCsv(a, "id,s\n1,x\n", "SELECT SUM(s) AS n, MIN(s) AS lo FROM '$IN'", &ok);
}

test "laneHints: a CSV dialect and an agreeing format fan out; anything else stays serial" {
    const rd = ast.Stage.Node{ .read = .{ .connector = "csv", .form = .{ .path = "x.csv" } } };
    const pos = ast.Pos{ .line = 1, .col = 1 };
    const Case = struct { hints: []const ast.Hint, ok: bool };
    const cases = [_]Case{
        .{ .hints = &.{}, .ok = true },
        .{ .hints = &.{ .{ .key = "delimiter", .value = .{ .str = ";" }, .pos = pos }, .{ .key = "encoding", .value = .{ .str = "latin1" }, .pos = pos } }, .ok = true },
        .{ .hints = &.{.{ .key = "format", .value = .{ .str = "csv" }, .pos = pos }}, .ok = true },
        // a format the extension does not say is read by another reader than the lanes'
        .{ .hints = &.{.{ .key = "format", .value = .{ .str = "parquet" }, .pos = pos }}, .ok = false },
        .{ .hints = &.{.{ .key = "split", .value = .{ .str = "id" }, .pos = pos }}, .ok = false },
    };
    for (cases) |c| try std.testing.expectEqual(c.ok, laneHints(.{ .node = rd, .hints = c.hints, .pos = pos }));
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

test "analyze: a failing stage reports its line and column" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,amount\n1,100\n" });
    const base = try tmp.dir.realpathAlloc(a, ".");
    const in = try std.fs.path.join(a, &.{ base, "in.csv" });

    const src = try std.fmt.allocPrint(a, "LOAD INTO '/tmp/x.csv' AS\nSELECT id\nFROM '{s}'\nWHERE nosuch > 1;", .{in});
    const prog = try parse(a, src);
    var diag = Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyze(a, prog, &diag));
    try std.testing.expectEqualStrings("unknown field `nosuch`", diag.msg);
    try std.testing.expectEqual(@as(u32, 4), diag.pos.?.line);
    try std.testing.expectEqual(@as(u32, 7), diag.pos.?.col);
    try std.testing.expectEqual(@as(u32, 13), diag.end.?.col);
}

test "analyze: an unknown name is underlined where it is written, not at its statement" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const Case = struct { src: []const u8, line: u32, col: u32, end_col: u32 };
    const cases = [_]Case{
        // a bare select-list column
        .{ .src = "SELECT nope FROM RANGE(3);", .line = 1, .col = 8, .end_col = 12 },
        // inside a call, on the second line of the statement
        .{ .src = "SELECT range,\n       upper(nope) AS u\nFROM RANGE(3);", .line = 2, .col = 14, .end_col = 18 },
        // an unknown function: its name
        .{ .src = "SELECT frobnicate(range) AS f FROM RANGE(3);", .line = 1, .col = 8, .end_col = 18 },
        // a call with the wrong arguments: the call's name, not its first argument
        .{ .src = "SELECT substr(range) AS s FROM RANGE(3);", .line = 1, .col = 8, .end_col = 14 },
        // a sort key
        .{ .src = "SELECT range FROM RANGE(3) ORDER BY zz;", .line = 1, .col = 37, .end_col = 39 },
        // a quoted name spans its quotes
        .{ .src = "SELECT \"no such\" FROM RANGE(3);", .line = 1, .col = 8, .end_col = 17 },
    };
    for (cases) |c| {
        var diag = Diag{};
        try std.testing.expectError(error.AnalyzeFailed, analyze(a, try parse(a, c.src), &diag));
        errdefer std.debug.print("case: {s} -> {s} at {?}..{?}\n", .{ c.src, diag.msg, diag.pos, diag.end });
        try std.testing.expectEqual(c.line, diag.pos.?.line);
        try std.testing.expectEqual(c.col, diag.pos.?.col);
        try std.testing.expectEqual(c.line, diag.end.?.line);
        try std.testing.expectEqual(c.end_col, diag.end.?.col);
    }
}

test "analyze: string builtins refuse a non-INT position but coerce scalars to text" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,name\n1,ann\n" });
    const base = try tmp.dir.realpathAlloc(a, ".");
    const in = try std.fs.path.join(a, &.{ base, "in.csv" });

    const bad = try std.fmt.allocPrint(a, "LOAD INTO '/tmp/x.csv' AS SELECT substr(id, 'a', 'b') AS s FROM '{s}';", .{in});
    var diag = Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyze(a, try parse(a, bad), &diag));
    try std.testing.expectEqualStrings("`substr` start must be an INT, got string", diag.msg);

    const ok = try std.fmt.allocPrint(a, "LOAD INTO '/tmp/x.csv' AS SELECT concat(id, '-', name) AS c, lpad(id, 5, '0') AS p, length(id) AS n FROM '{s}';", .{in});
    var diag2 = Diag{};
    _ = try analyze(a, try parse(a, ok), &diag2);
}

test "analyze: CREATE FUNCTION cannot shadow a builtin or an aggregate" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    inline for (.{ "upper", "sum" }) |name| {
        const src = try std.fmt.allocPrint(a, "CREATE FUNCTION {s}(x) AS 999; SELECT 1 AS a;", .{name});
        var diag = Diag{};
        try std.testing.expectError(error.AnalyzeFailed, analyze(a, try parse(a, src), &diag));
        try std.testing.expectEqualStrings("`" ++ name ++ "` is a built-in function and cannot be redefined", diag.msg);
    }
}

test "analyze: a window stage resolves its schema — the input plus one typed column per function" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const prog = try parse(a,
        \\LOAD INTO '/tmp/x.csv' AS
        \\SELECT y, LAG(rev) OVER (ORDER BY y) AS prev, SUM(rev) OVER (ORDER BY y) AS run
        \\FROM (SELECT 'a' AS y, CAST(1 AS DECIMAL(10,2)) AS rev) m;
    );
    var diag = Diag{};
    const plan = try analyze(a, prog, &diag);
    var found = false;
    for (plan.outputs[0].stages) |st| {
        if (!std.mem.eql(u8, st.kind, "window")) continue;
        found = true;
        const s = st.out_schema orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(@as(usize, 4), s.fields.len);
        try std.testing.expectEqualStrings("prev", s.fields[2].name);
        try std.testing.expect(s.fields[2].ty.kind == .decimal and s.fields[2].ty.nullable);
        try std.testing.expectEqualStrings("run", s.fields[3].name);
        try std.testing.expect(s.fields[3].ty.kind == .float and s.fields[3].ty.nullable);
    }
    try std.testing.expect(found);
}

test "a $param or LET in a filter reaches the pushdown as its value, never as a column" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(a,
        \\CREATE CONNECTION db TYPE sqlserver OPTIONS (host = 'h');
        \\LET c = 1;
        \\PARAM n INT DEFAULT 5;
        \\EXPLAIN SELECT * FROM db.dbo.t WHERE b >= $c AND k < $n;
    , &pdiag);
    var diag = Diag{};
    const plan = try analyze(a, prog, &diag);
    try std.testing.expectEqualStrings("(([b] >= 1) AND ([k] < 5))", plan.outputs[0].source.pushdown);

    // the rewrite itself: values in, and the stages untouched when none is named
    var params = ParamMap.init(a);
    const five = try a.create(ast.Expr);
    five.* = .{ .int_lit = 5 };
    try params.put("n", five);
    const stages = prog.stmts[prog.stmts.len - 1].explain.pipeline.stages;
    const bound = try substFilterParams(a, stages, &params);
    try std.testing.expect(bound.ptr != stages.ptr);
    const sql = (try pushdown.serialWhere(a, .postgres, bound)).?;
    try std.testing.expect(std.mem.indexOf(u8, sql, "\"k\" < 5") != null);
    var none = ParamMap.init(a);
    try std.testing.expect((try substFilterParams(a, stages, &none)).ptr == stages.ptr);
}
