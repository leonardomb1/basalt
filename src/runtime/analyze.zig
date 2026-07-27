//! Static analysis: parsed program → validated `Plan` IR, without executing or
//! (by default) connecting. Shared groundwork for `pipeline plan` (render the IR)
//! and `pipeline check` (validate and report). Offline it does full structural +
//! reference validation and resolves what it can locally (CSV schema from the
//! header); DB source schemas + per-stage type flow are filled in only when a
//! connecting resolver is supplied (`--connect`).

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

pub const JoinPlan = struct { lk: usize, rk: usize, schema: types.Schema, emit_right: bool, right_nullable: bool };

pub fn joinPlan(arena: std.mem.Allocator, left: types.Schema, right: types.Schema, j: ast.Join, diag: *Diag) Error!JoinPlan {
    const lk = left.indexOf(lastPart(j.left_key)) orelse return fail(diag, "unknown left join key `{s}`", .{lastPart(j.left_key)});
    const rk = right.indexOf(lastPart(j.right_key)) orelse return fail(diag, "unknown right join key `{s}`", .{lastPart(j.right_key)});
    const emit_right = (j.kind == .inner or j.kind == .left);
    const right_nullable = (j.kind == .left);

    var fields = std.array_list.Managed(types.Schema.Field).init(arena);
    for (left.fields) |f| try fields.append(f);
    if (emit_right) for (right.fields) |f| {
        var name = f.name;
        if (left.indexOf(name) != null) name = try std.fmt.allocPrint(arena, "{s}_r", .{name});
        try fields.append(.{ .name = name, .ty = if (right_nullable) f.ty.asNullable() else f.ty });
    };
    return .{ .lk = lk, .rk = rk, .schema = .{ .fields = try fields.toOwnedSlice() }, .emit_right = emit_right, .right_nullable = right_nullable };
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

/// Result of probing a splittable table for its key + estimated size (`--connect`).
pub const SplitProbe = struct {
    key: []const u8,
    est_rows: i64,
    will_split: bool,
};

pub const Physical = struct {
    has_breaker: bool,
    splittable: bool,
    sink_parallel: bool,
    split_probe: ?SplitProbe = null,
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

/// Supplies a source's schema. Offline (null) resolves CSV locally and leaves DB
/// sources unresolved; `--connect` injects one that connects.
pub const Resolver = struct {
    ctx: *anyopaque,
    resolveFn: *const fn (ctx: *anyopaque, arena: std.mem.Allocator, read: ast.Read, conn: ?ast.Connection) anyerror!?types.Schema,
    /// Optional: probe a splittable table for its real key + size (the physical plan).
    splitFn: ?*const fn (ctx: *anyopaque, arena: std.mem.Allocator, read: ast.Read, conn: ?ast.Connection) anyerror!?SplitProbe = null,
};

/// Collect every output pipeline reachable in a statement block (a `for` or
/// `match` arm body), descending through nested `for`/`match` so all branches
/// are type-checked offline.
fn collectStmtOutputs(outputs: *std.array_list.Managed(ast.Pipeline), stmts: []const ast.Stmt) error{OutOfMemory}!void {
    for (stmts) |st| switch (st) {
        .output => |p| try outputs.append(p),
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

pub fn analyze(arena: std.mem.Allocator, raw_program: ast.Program, resolver: ?Resolver, diag: *Diag) error{ AnalyzeFailed, OutOfMemory }!Plan {
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
        .for_each => |fe| try collectStmtOutputs(&outputs, fe.body),
        .match => |m| for (m.arms) |arm| try collectStmtOutputs(&outputs, arm.body),
        .func => |fd| if (fd.body == .stmts) try collectStmtOutputs(&outputs, fd.body.stmts),
        .param, .kind, .call, .let_const => {},
    };
    if (outputs.items.len == 0)
        return fail(diag, "no output pipeline (a pipeline ending in `write`)", .{});

    var params_map = ParamMap.init(arena);
    for (program.stmts) |s| if (s == .param) {
        const p = s.param;
        try params_map.put(p.name, if (p.default) |d| d else try typedZero(arena, p.ty));
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

    var ctx = Ctx{ .arena = arena, .bindings = &bindings, .connections = &connections, .resolver = resolver, .params = &params_map, .diag = diag };

    var out_plans = std.array_list.Managed(Output).init(arena);
    for (outputs.items) |pipe| try out_plans.append(try ctx.analyzeOutput(pipe));

    return .{ .kind = kind_name, .outputs = try out_plans.toOwnedSlice() };
}

const Ctx = struct {
    arena: std.mem.Allocator,
    bindings: *std.StringHashMap(ast.Pipeline),
    connections: *std.StringHashMap(ast.Connection),
    resolver: ?Resolver,
    params: *const ParamMap,
    diag: *Diag,

    fn analyzeOutput(self: *Ctx, pipe: ast.Pipeline) !Output {
        const stages = pipe.stages;
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
        var cur: ?types.Schema = source.schema;
        for (stages[1 .. stages.len - 1]) |st| {
            var si = try self.stageInfo(st);
            if (si.breaker) has_breaker = true;
            if (!isMapStage(st.node)) map_only = false;
            if (cur) |c| {
                cur = try self.propagate(c, st.node);
                si.out_schema = cur;
            }
            try stage_infos.append(si);
        }

        const w = stages[stages.len - 1].node.write;
        const sink = try self.resolveSink(w);

        const src_is_sql = isSqlConnector(source.connector);
        const sink_is_parallel = isSqlConnector(sink.connector) or std.mem.eql(u8, sink.connector, "starrocks");
        const splittable = src_is_sql and map_only and splittableRead(stages[0].node);

        var probe: ?SplitProbe = null;
        if (splittable) {
            if (self.resolver) |r| if (r.splitFn) |f| {
                const lead = stages[0].node.read;
                probe = f(r.ctx, self.arena, lead, self.connections.get(lead.connector)) catch null;
            };
        }

        return .{
            .source = source,
            .stages = try stage_infos.toOwnedSlice(),
            .sink = sink,
            .physical = .{
                .has_breaker = has_breaker,
                .splittable = splittable,
                .sink_parallel = sink_is_parallel,
                .split_probe = probe,
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
                var schema = offlineSchema(self.arena, rd);
                if (schema == null) {
                    if (self.resolver) |r| schema = r.resolveFn(r.ctx, self.arena, rd, conn) catch null;
                }
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

    fn resolveSink(self: *Ctx, w: ast.Write) !Sink {
        if (std.mem.eql(u8, w.connector, "csv") or std.mem.eql(u8, w.connector, "stdout")) {
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

pub fn render(plan: Plan, w: anytype) !void {
    try w.print("@{s}\n", .{plan.kind});
    for (plan.outputs) |o| {
        try w.print("  read  {s}  {s}\n", .{ sinkKind(o.source), o.source.detail });
        if (o.source.pushdown.len > 0)
            try w.print("        pushdown: {s}\n", .{o.source.pushdown});
        try printSchema(w, "        ", o.source.schema);
        for (o.stages) |st| {
            if (st.detail.len > 0) {
                try w.print("  → {s}  {s}\n", .{ st.kind, st.detail });
            } else {
                try w.print("  → {s}\n", .{st.kind});
            }
            try printSchema(w, "        ", st.out_schema);
        }
        try w.print("  write {s}  {s} ({s})\n", .{ sinkKind(o.sink), o.sink.target, o.sink.mode });

        try w.writeAll("  physical: ");
        if (o.physical.splittable) {
            if (o.physical.split_probe) |sp| {
                if (sp.will_split) {
                    try w.print("split-parallel on `{s}` (~{d} rows)", .{ sp.key, sp.est_rows });
                    if (o.physical.sink_parallel) try w.writeAll(" · per-lane sink");
                } else if (sp.key.len > 0) {
                    try w.print("serial — key `{s}` but ~{d} rows is below the split gate", .{ sp.key, sp.est_rows });
                } else {
                    try w.writeAll("serial — no single-column int/uuid primary key to split on");
                }
            } else {
                try w.writeAll("split-parallel candidate");
                if (o.physical.sink_parallel) try w.writeAll(" · per-lane sink");
            }
        } else {
            try w.writeAll("serial");
            if (o.physical.has_breaker) try w.writeAll(" (has breaker — materializes)");
        }
        try w.writeAll("\n");
    }
}

/// The `csv` connector backs every file sink, so the plan has to name the
/// format from the target — otherwise a parquet write reads as `write csv`.
fn sinkKind(node: anytype) []const u8 {
    const path = if (@hasField(@TypeOf(node), "target")) node.target else node.detail;
    if (std.mem.eql(u8, node.connector, "csv") and pqwrite.Writer.isPath(path)) return "parquet";
    return node.connector;
}

fn printSchema(w: anytype, indent: []const u8, schema: ?types.Schema) !void {
    const s = schema orelse {
        try w.print("{s}(schema: connect to resolve)\n", .{indent});
        return;
    };
    try w.writeAll(indent);
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

/// Offline schema resolution: a local CSV header or parquet footer is readable
/// without connecting to anything; everything else stays unresolved.
fn offlineSchema(arena: std.mem.Allocator, rd: ast.Read) ?types.Schema {
    if (std.mem.eql(u8, rd.connector, "unit")) return .{ .fields = &.{} };
    if (std.mem.eql(u8, rd.connector, "range")) {
        const fields = arena.alloc(types.Schema.Field, 1) catch return null;
        fields[0] = .{ .name = "range", .ty = .{ .kind = .int } };
        return .{ .fields = fields };
    }
    if (std.mem.eql(u8, rd.connector, "csv") and rd.form == .path) {
        if (csv.CsvReader.isUrl(rd.form.path)) return null;
        if (pqdecode.Reader.isPath(rd.form.path)) {
            const pr = pqdecode.Reader.open(arena, rd.form.path) catch return null;
            return pr.schema;
        }
        const reader = csv.CsvReader.open(arena, rd.form.path) catch return null;
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
    const plan = try analyze(a, prog, null, &diag);

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
    const plan = try analyze(a, prog, null, &diag);
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
    const plan = try analyze(a, prog, null, &diag);
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
    const plan = try analyze(a, prog, null, &diag);
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
    const plan = try analyze(a, try parse(a, src), null, &diag);
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
    try std.testing.expectError(error.AnalyzeFailed, analyze(a, try parse(a, src), null, &diag));
}

test "analyze rejects unknown connection" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const prog = try parse(a, "LOAD INTO '/tmp/x.csv' AS SELECT * FROM nope.t;");
    var diag = Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyze(a, prog, null, &diag));
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
    try std.testing.expectError(error.AnalyzeFailed, analyze(a, try parse(a, src), null, &diag));
}

test "analyze rejects a program with no output pipeline" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const prog = try parse(a, "PARAM x INT DEFAULT 1;");
    var diag = Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyze(a, prog, null, &diag));
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
    ), null, &diag);
    try std.testing.expect(p1.outputs[0].physical.has_breaker);
    try std.testing.expect(!p1.outputs[0].physical.splittable);

    const p2 = try analyze(a, try parse(a,
        \\CREATE CONNECTION pg TYPE postgres OPTIONS (host = 'h', database = 'd');
        \\LOAD INTO '/tmp/x.csv' AS SELECT * FROM pg.QUERY($$SELECT 1 AS x$$);
    ), null, &diag);
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

    const inner = try joinPlan(a, left, right, .{ .kind = .inner, .binding = "r", .left_key = key, .right_key = key }, &diag);
    try std.testing.expectEqual(@as(usize, 1), inner.lk);
    try std.testing.expectEqual(@as(usize, 0), inner.rk);
    try std.testing.expectEqual(@as(usize, 4), inner.schema.fields.len);
    try std.testing.expectEqualStrings("code_r", inner.schema.fields[2].name);
    try std.testing.expectEqualStrings("label", inner.schema.fields[3].name);
    try std.testing.expect(!inner.schema.fields[3].ty.nullable);

    const lj = try joinPlan(a, left, right, .{ .kind = .left, .binding = "r", .left_key = key, .right_key = key }, &diag);
    try std.testing.expect(lj.right_nullable);
    try std.testing.expect(lj.schema.fields[2].ty.nullable and lj.schema.fields[3].ty.nullable);
    try std.testing.expect(!lj.schema.fields[0].ty.nullable);

    const semi = try joinPlan(a, left, right, .{ .kind = .semi, .binding = "r", .left_key = key, .right_key = key }, &diag);
    try std.testing.expect(!semi.emit_right);
    try std.testing.expectEqual(@as(usize, 2), semi.schema.fields.len);

    try std.testing.expectError(error.AnalyzeFailed, joinPlan(a, left, right, .{ .kind = .inner, .binding = "r", .left_key = .{ .parts = &.{"nope"} }, .right_key = key }, &diag));
    try std.testing.expect(std.mem.indexOf(u8, diag.msg, "unknown left join key") != null);
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
    const plan = try analyze(a, prog, null, &diag);
    try std.testing.expectEqual(@as(usize, 1), plan.outputs.len);
    try std.testing.expectEqualStrings("filter", plan.outputs[0].stages[0].kind);

    const clash = try std.fmt.allocPrint(a,
        \\PARAM cutoff INT DEFAULT 10;
        \\LET cutoff = 5;
        \\LOAD INTO '/tmp/x.csv' AS SELECT id FROM '{s}';
    , .{in});
    const cprog = try parse(a, clash);
    var cdiag = Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyze(a, cprog, null, &cdiag));
    try std.testing.expect(std.mem.indexOf(u8, cdiag.msg, "declared twice") != null);
}
