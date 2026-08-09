//! Predicate + projection pushdown for split-parallel SQL aggregates.
//!
//! A `read <sqltable> | (filter)* | aggregate … by …` over a splittable source reads
//! one key range per lane (see `connect/split.zig`). Without pushdown each lane does
//! `SELECT * FROM (base) WHERE <key range>` and ships every column of every row across
//! the wire, only to drop most of it in the engine. This module narrows each lane's
//! query to what the aggregate actually consumes:
//!
//!   - **projection** — `SELECT` only the source columns the group keys, the aggregate
//!     arguments, and the surviving filters reference (an ERP fact table is often 20+
//!     columns; an aggregate touches 2–3).
//!   - **predicate** — translate the prefix `filter`s into a SQL `WHERE` AND-ed onto the
//!     key range, so the server filters rows before they're sent.
//!
//! Correctness rests on two things: basalt's filter is 3-valued *exactly* like SQL
//! ("only a known-true keeps the row; a null result drops it" — see `op.applyFilter`),
//! so eq/ne/comparisons/and/or/not/is-null map 1:1; and the caller KEEPS the filter
//! ops, so a pushed predicate only has to be a superset (never drop a kept row) — which
//! holds, since untranslatable parts are simply not pushed. Anything ambiguous (a
//! `select` in the prefix, a non-source field, arithmetic, a function call) disables
//! the relevant half and the lane falls back to the safe `SELECT * … WHERE <key range>`.

const std = @import("std");
const ast = @import("../lang/ast.zig");
const types = @import("../lang/types.zig");
const splitmod = @import("../connect/split.zig");
const Dialect = @import("../connect/sql.zig").Dialect;

/// The result of planning pushdown for one aggregate pipeline. Empty fields mean
/// "don't push that half" — the lane query then uses `*` / the bare key range.
pub const Plan = struct {
    /// Comma-joined, dialect-quoted column list, or null for `SELECT *`.
    proj_select: ?[]const u8 = null,
    /// Schema matching `proj_select` column order, or null when not projecting. The
    /// aggregate's input schema becomes this, so its column indices line up with the
    /// narrowed result set.
    proj_schema: ?types.Schema = null,
    /// AND-combined translated filter predicates, or null when none were pushable.
    where_extra: ?[]const u8 = null,
};

fn inSchema(schema: types.Schema, name: []const u8) bool {
    for (schema.fields) |f| if (std.mem.eql(u8, f.name, name)) return true;
    return false;
}

/// Plan projection + predicate pushdown for `read … | prefix | aggregate ag`. `prefix`
/// is the map-only stages between read and aggregate (filter/select only, per the
/// caller's classifier). A `select` in the prefix disables projection (its renames make
/// source-column attribution ambiguous); filters are still translated for the WHERE.
pub fn planAgg(arena: std.mem.Allocator, dialect: Dialect, src_schema: types.Schema, prefix: []const ast.Stage, ag: ast.Aggregate) !Plan {
    var plan = Plan{};

    const has_select = for (prefix) |st| {
        if (st.node == .select) break true;
    } else false;
    if (!has_select) {
        var need = std.StringHashMap(void).init(arena);
        defer need.deinit();
        for (prefix) |st| if (st.node == .filter) try collectFields(st.node.filter, &need);
        for (ag.by) |q| try need.put(q.parts[0], {});
        for (ag.aggs) |a| if (a.arg) |arg| try collectFields(arg, &need);
        const p = try buildProjection(arena, dialect, src_schema, &need);
        plan.proj_select = p.sel;
        plan.proj_schema = p.schema;
    }

    var where = std.array_list.Managed(u8).init(arena);
    for (prefix) |st| {
        if (st.node != .filter) continue;
        const frag = (try translateExpr(arena, st.node.filter, dialect, src_schema, true)) orelse continue;
        if (where.items.len > 0) try where.appendSlice(" AND ");
        try where.appendSlice(frag);
    }
    if (where.items.len > 0) plan.where_extra = try where.toOwnedSlice();

    return plan;
}

/// A whole aggregate descended into ONE source query. Unlike `Plan`/`MapPlan` this is
/// AUTHORITATIVE: the source's GROUP BY result is what the pipeline emits, so there is
/// no engine-side re-aggregation to correct a rendering that isn't exactly equivalent.
pub const WholeAgg = struct {
    /// The complete grouped statement, ready for `sql.Source.open`.
    sql: []const u8,
    /// The AND-ed translated prefix filters, or null when the prefix was empty.
    where_sql: ?[]const u8 = null,
};

/// Render `read <base> | filter* | aggregate ag` as a single grouped query, or null to
/// leave the aggregate engine-side. `plan_schema` is what the engine's `aggregatePlan`
/// decided the output is — group keys first, then one field per `ag.aggs` entry — and
/// every aggregate is wrapped in a CAST to the dialect spelling of *that* type, so the
/// source cannot hand back a wider/narrower result than the plan promises.
///
/// Null (fall back to scanning + aggregating here) on anything not provably identical
/// on postgres, mysql/StarRocks and sqlserver alike:
///   - a non-`filter` stage in the prefix, or a filter `translateExpr` won't take;
///   - a group key that isn't a bare source column, or whose type doesn't pass through;
///   - `AVG` (int-avg diverges: engine float vs mysql decimal vs sqlserver int division);
///   - `SUM` of anything but an int column into an int result — a decimal sum can
///     overflow the CAST target the engine would have carried in an i64, and postgres
///     `SUM(real)` accumulates in float4 where the engine uses f64;
///   - `MIN`/`MAX` outside int/float/decimal/date — string extremes follow the source's
///     COLLATION (case-insensitive by default on mysql and sqlserver) while the engine
///     compares bytes, and a timestamp CAST can shift under a session timezone;
///   - `DISTINCT` on anything but `COUNT` (the engine's `updateAcc` only honours it there);
///   - a planned output type `sqlTypeName` has no cast target for.
///
/// Null handling matches by construction and is verified on the engine side: NULL group
/// keys collapse into one group (`op.Aggregate.drainFixed`'s null mask / `keyhash.valueEq`),
/// `COUNT(col)`/`SUM`/`MIN`/`MAX` skip nulls (`op.Aggregate.updateAcc`), and an ungrouped
/// aggregate over zero rows still emits one row of COUNT 0 / NULL extremes
/// (`op.Aggregate.drainImpl`'s `by.len == 0` branch always returns one group) — which is
/// exactly what all three dialects return for an ungrouped aggregate over no rows.
pub fn planWholeAgg(
    arena: std.mem.Allocator,
    dialect: Dialect,
    base_sql: []const u8,
    src_schema: types.Schema,
    prefix: []const ast.Stage,
    ag: ast.Aggregate,
    plan_schema: types.Schema,
) !?WholeAgg {
    if (ag.by.len == 0 and ag.aggs.len == 0) return null;
    if (plan_schema.fields.len != ag.by.len + ag.aggs.len) return null;

    // Gate 2: EVERY prefix stage is a filter, and every one translates whole. A
    // partially pushed predicate would be a superset — fine for advisory pushdown,
    // wrong here, because nothing re-applies the missing half.
    var where = std.array_list.Managed(u8).init(arena);
    for (prefix) |st| {
        if (st.node != .filter) return null;
        const frag = (try translateExpr(arena, st.node.filter, dialect, src_schema, true)) orelse return null;
        if (where.items.len > 0) try where.appendSlice(" AND ");
        try where.appendSlice(frag);
    }

    // Gate 3: bare source columns only, carried through with their own type.
    var sel = std.array_list.Managed(u8).init(arena);
    var keys = std.array_list.Managed(u8).init(arena);
    for (ag.by, 0..) |q, i| {
        if (q.parts.len != 1) return null;
        const col = q.parts[0];
        const idx = src_schema.indexOf(col) orelse return null;
        const out = plan_schema.fields[i];
        if (!std.mem.eql(u8, out.name, col)) return null;
        if (out.ty.kind != src_schema.fields[idx].ty.kind) return null;
        const qc = try splitmod.quoteIdent(arena, dialect, col);
        if (keys.items.len > 0) try keys.appendSlice(", ");
        try keys.appendSlice(qc);
        if (sel.items.len > 0) try sel.appendSlice(", ");
        try sel.appendSlice(try std.fmt.allocPrint(arena, "{s} AS {s}", .{ qc, qc }));
    }

    // Gates 4 + 5: an allowed aggregate over a bare column, CAST to the engine's own
    // planned output type, and aliased to the engine's own column name.
    for (ag.aggs, 0..) |item, i| {
        const out = plan_schema.fields[ag.by.len + i];
        const inner = (try aggExpr(arena, dialect, src_schema, item, out.ty)) orelse return null;
        const cast_to = (try sqlTypeName(arena, dialect, out.ty)) orelse return null;
        if (sel.items.len > 0) try sel.appendSlice(", ");
        try sel.appendSlice(try std.fmt.allocPrint(arena, "CAST({s} AS {s}) AS {s}", .{
            inner, cast_to, try splitmod.quoteIdent(arena, dialect, out.name),
        }));
    }

    // `base_sql` is the read's own statement, raw `PUSHDOWN(...)` predicate included, so
    // wrapping it as a subquery composes with whatever it already filters — the shape
    // `sqlWithWhere` and `split.zig` both use for a QUERY-form read.
    var q = std.array_list.Managed(u8).init(arena);
    try q.appendSlice(try std.fmt.allocPrint(arena, "SELECT {s} FROM ({s}) _g", .{ sel.items, base_sql }));
    if (where.items.len > 0) try q.appendSlice(try std.fmt.allocPrint(arena, " WHERE {s}", .{where.items}));
    if (keys.items.len > 0) try q.appendSlice(try std.fmt.allocPrint(arena, " GROUP BY {s}", .{keys.items}));

    const where_sql: ?[]const u8 = if (where.items.len > 0) where.items else null;
    return WholeAgg{ .sql = try q.toOwnedSlice(), .where_sql = where_sql };
}

/// One aggregate's SQL (before the outer result-type CAST), or null when it isn't
/// provably the engine's own answer. See `planWholeAgg`'s doc comment for the why of
/// each exclusion.
fn aggExpr(arena: std.mem.Allocator, dialect: Dialect, src_schema: types.Schema, item: ast.AggItem, out_ty: types.Type) !?[]const u8 {
    // T-SQL's COUNT accumulates in a 32-bit int and RAISES past 2^31 rows, where
    // postgres/mysql already return a 64-bit count; COUNT_BIG is the matching spelling.
    const count_fn: []const u8 = if (dialect == .sqlserver) "COUNT_BIG" else "COUNT";

    if (item.func == .count and item.arg == null) {
        if (item.distinct) return null;
        return try std.fmt.allocPrint(arena, "{s}(*)", .{count_fn});
    }

    const arg = item.arg orelse return null;
    if (arg.* != .field or arg.field.parts.len != 1) return null;
    const name = arg.field.parts[0];
    const idx = src_schema.indexOf(name) orelse return null;
    const src_kind = src_schema.fields[idx].ty.kind;
    const col = try splitmod.quoteIdent(arena, dialect, name);

    switch (item.func) {
        .count => {
            if (item.distinct) return try std.fmt.allocPrint(arena, "{s}(DISTINCT {s})", .{ count_fn, col });
            return try std.fmt.allocPrint(arena, "{s}({s})", .{ count_fn, col });
        },
        .sum => {
            if (item.distinct) return null;
            if (src_kind != .int or out_ty.kind != .int) return null;
            // sqlserver sums `int` in `int` and raises on overflow; widening the
            // ADDEND (not the result) makes it accumulate in bigint like the engine.
            // postgres (int4→int8, int8→numeric) and mysql (→decimal) already do.
            if (dialect != .sqlserver) return try std.fmt.allocPrint(arena, "SUM({s})", .{col});
            return try std.fmt.allocPrint(arena, "SUM(CAST({s} AS BIGINT))", .{col});
        },
        .min, .max => {
            if (item.distinct) return null;
            if (out_ty.kind != src_kind) return null;
            switch (src_kind) {
                .int, .float, .date => {},
                // A `DECIMAL(0,0)` cast target is not valid SQL anywhere; an
                // unresolved precision means fall back rather than guess one.
                .decimal => if (out_ty.precision == 0 or out_ty.scale > out_ty.precision) return null,
                else => return null,
            }
            return try std.fmt.allocPrint(arena, "{s}({s})", .{ if (item.func == .min) "MIN" else "MAX", col });
        },
        .avg => return null,
    }
}

/// The result of map-only pushdown planning (`read … | (filter|select|…) | write`).
pub const MapPlan = struct {
    proj_select: ?[]const u8 = null,
    proj_schema: ?types.Schema = null,
    where_extra: ?[]const u8 = null,
    /// The middle stages with dead `select` items removed, so the rebuilt chain doesn't
    /// reference a projected-away column. Set only alongside `proj_schema`; the caller
    /// rebuilds from these instead of the originals. Null → projection not applied.
    stages: ?[]const ast.Stage = null,
};

/// Plan pushdown for a map-only split read. `out_cols` is the pipeline's final output
/// column set (what the sink receives). Only the FILTERS before the first non-filter stage
/// are source-attributable (a later filter sees a select's renamed output), so only those
/// become a WHERE. Projection is computed by a backward liveness pass: start from the
/// output columns, and walk the stages in reverse — a `select` maps each live output back
/// to the source columns its item reads, a `filter` adds its predicate's columns. What
/// survives to the source is the minimal column set to fetch. This traces a union branch's
/// `select(reconcile) | … | select id, recno` all the way back, so only the columns that
/// reach the sink cross the wire. A `*`/`* rename`/explode stage makes liveness imprecise,
/// so projection is dropped (the caller keeps the full `SELECT *` chain). The caller
/// rebuilds its stage chain against `proj_schema` so the narrower indices line up.
pub fn planMap(arena: std.mem.Allocator, dialect: Dialect, src_schema: types.Schema, middle: []const ast.Stage, out_cols: []const []const u8) !MapPlan {
    var plan = MapPlan{};

    var nf: usize = middle.len;
    for (middle, 0..) |st, i| if (st.node != .filter) {
        nf = i;
        break;
    };
    const leading = middle[0..nf];

    var where = std.array_list.Managed(u8).init(arena);
    for (leading) |st| {
        const frag = (try translateExpr(arena, st.node.filter, dialect, src_schema, true)) orelse continue;
        if (where.items.len > 0) try where.appendSlice(" AND ");
        try where.appendSlice(frag);
    }
    if (where.items.len > 0) plan.where_extra = try where.toOwnedSlice();

    var live = std.StringHashMap(void).init(arena);
    for (out_cols) |c| try live.put(c, {});
    var pruned_rev = std.array_list.Managed(ast.Stage).init(arena);
    var proj_ok = true;
    var i = middle.len;
    while (i > 0 and proj_ok) {
        i -= 1;
        const st = middle[i];
        switch (st.node) {
            .filter => |pred| {
                try collectFields(pred, &live);
                try pruned_rev.append(st);
            },
            .select => |items| {
                var kept = std.array_list.Managed(ast.SelectItem).init(arena);
                var nl = std.StringHashMap(void).init(arena);
                for (items) |item| switch (item) {
                    .field => |q| if (live.contains(q.last())) {
                        try kept.append(item);
                        try nl.put(q.parts[0], {});
                    },
                    .computed => |c| if (live.contains(c.name)) {
                        try kept.append(item);
                        try collectFields(c.expr, &nl);
                    },
                    else => proj_ok = false,
                };
                try pruned_rev.append(.{ .node = .{ .select = try kept.toOwnedSlice() }, .hints = st.hints, .pos = st.pos });
                live = nl;
            },
            else => proj_ok = false,
        }
    }
    if (proj_ok) {
        const p = try buildProjection(arena, dialect, src_schema, &live);
        if (p.schema != null) {
            plan.proj_select = p.sel;
            plan.proj_schema = p.schema;
            const pruned = try arena.alloc(ast.Stage, pruned_rev.items.len);
            for (pruned_rev.items, 0..) |st, k| pruned[pruned_rev.items.len - 1 - k] = st;
            plan.stages = pruned;
        }
    }

    return plan;
}

const Projection = struct { sel: ?[]const u8 = null, schema: ?types.Schema = null };

/// Build a `SELECT` list + matching schema for the source columns in `need`, in
/// source-schema order (deterministic, and the order the engine expects). Null when it
/// wouldn't drop anything (all or no columns) — the caller then scans `SELECT *`.
fn buildProjection(arena: std.mem.Allocator, dialect: Dialect, src_schema: types.Schema, need: *std.StringHashMap(void)) !Projection {
    var sel = std.array_list.Managed(u8).init(arena);
    var fields = std.array_list.Managed(types.Schema.Field).init(arena);
    for (src_schema.fields) |f| {
        if (!need.contains(f.name)) continue;
        if (sel.items.len > 0) try sel.appendSlice(", ");
        try sel.appendSlice(try splitmod.quoteIdent(arena, dialect, f.name));
        try fields.append(f);
    }
    if (fields.items.len > 0 and fields.items.len < src_schema.fields.len)
        return .{ .sel = try sel.toOwnedSlice(), .schema = .{ .fields = try fields.toOwnedSlice() } };
    return .{};
}

/// Translate a filter predicate to an equivalent SQL boolean expression, or null if
/// any part isn't faithfully translatable (the caller keeps the filter op regardless,
/// so a null here just forgoes the wire saving). Only single-part fields that exist in
/// `schema` are emitted — a param, a nested/JSON path, or a typo yields null, never a
/// guess.
/// ast.Expr -> source-dialect SQL, or null for anything whose semantics
/// aren't guaranteed identical at the source (the caller then keeps that
/// part engine-side — pushing is always optional). `schema` null means
/// "trust bare field names" — safe for filters DIRECTLY after a read, whose
/// fields the type-checker already resolved against the source schema.
pub fn translateExpr(arena: std.mem.Allocator, e: *const ast.Expr, dialect: Dialect, schema: types.Schema, check_fields: bool) error{OutOfMemory}!?[]const u8 {
    switch (e.*) {
        .bool_lit => |b| return try arena.dupe(u8, if (b) "(1=1)" else "(1=0)"),
        .int_lit => |v| return try std.fmt.allocPrint(arena, "{d}", .{v}),
        .float_lit => |v| return try std.fmt.allocPrint(arena, "{d}", .{v}),
        .null_lit => return try arena.dupe(u8, "NULL"),
        .str_lit => |s| return try sqlStr(arena, s),
        .field => |q| {
            if (q.parts.len != 1) return null;
            if (check_fields and !inSchema(schema, q.parts[0])) return null;
            return try splitmod.quoteIdent(arena, dialect, q.parts[0]);
        },
        .unary => |u| {
            if (u.op != .not) return null;
            const inner = (try translateExpr(arena, u.e, dialect, schema, check_fields)) orelse return null;
            return try std.fmt.allocPrint(arena, "(NOT ({s}))", .{inner});
        },
        .is_null => |n| {
            const inner = (try translateExpr(arena, n.e, dialect, schema, check_fields)) orelse return null;
            if (n.kind == .is_null)
                return try std.fmt.allocPrint(arena, "({s} IS {s}NULL)", .{ inner, if (n.negated) "NOT " else "" });
            const test_sql = try std.fmt.allocPrint(arena, "({s} IS NULL OR {s} = '')", .{ inner, inner });
            return if (n.negated) try std.fmt.allocPrint(arena, "(NOT {s})", .{test_sql}) else test_sql;
        },
        .binary => |b| {
            const op = switch (b.op) {
                .eq => "=",
                .ne => "<>",
                .lt => "<",
                .le => "<=",
                .gt => ">",
                .ge => ">=",
                .@"and" => "AND",
                .@"or" => "OR",
                // Arithmetic and bitwise stay engine-side: `^` is POWER on
                // Postgres and SQL Server has no shift operators at all.
                else => return null,
            };
            const l = (try translateExpr(arena, b.l, dialect, schema, check_fields)) orelse return null;
            const r = (try translateExpr(arena, b.r, dialect, schema, check_fields)) orelse return null;
            return try std.fmt.allocPrint(arena, "({s} {s} {s})", .{ l, op, r });
        },
        .cond => |c| {
            const cnd = (try translateExpr(arena, c.cond, dialect, schema, check_fields)) orelse return null;
            const t = (try translateExpr(arena, c.then, dialect, schema, check_fields)) orelse return null;
            const f = (try translateExpr(arena, c.els, dialect, schema, check_fields)) orelse return null;
            return try std.fmt.allocPrint(arena, "(CASE WHEN {s} THEN {s} ELSE {s} END)", .{ cnd, t, f });
        },
        .match => |m| return translateMatch(arena, m, dialect, schema, check_fields),
        .cast => |c| {
            // TRY_CAST is engine-only: the dialects' support for a null-on-failure cast is
            // uneven (no portable spelling on mysql/starrocks), and a plain CAST would raise
            // on the rows TRY_CAST is there to turn into nulls. Never push it.
            if (c.safe) return null;
            const inner = (try translateExpr(arena, c.e, dialect, schema, check_fields)) orelse return null;
            const ty = sqlTypeName(arena, dialect, c.ty) catch return error.OutOfMemory;
            return try std.fmt.allocPrint(arena, "CAST({s} AS {s})", .{ inner, (ty orelse return null) });
        },
        .call => |c| return translateCall(arena, c, dialect, schema, check_fields),
        else => return null,
    }
}

fn translateMatch(arena: std.mem.Allocator, m: ast.Match, dialect: Dialect, schema: types.Schema, check_fields: bool) error{OutOfMemory}!?[]const u8 {
    var out = std.array_list.Managed(u8).init(arena);
    const w = out.writer();
    if (m.subject) |subj| {
        const s = (try translateExpr(arena, subj, dialect, schema, check_fields)) orelse return null;
        w.print("(CASE {s}", .{s}) catch return error.OutOfMemory;
    } else {
        w.writeAll("(CASE") catch return error.OutOfMemory;
    }
    for (m.arms) |arm| {
        const v = (try translateExpr(arena, arm.value, dialect, schema, check_fields)) orelse return null;
        if (arm.is_default) {
            w.print(" ELSE {s}", .{v}) catch return error.OutOfMemory;
        } else if (m.subject != null) {
            for (arm.pats) |p| {
                const ps = (try translateExpr(arena, p, dialect, schema, check_fields)) orelse return null;
                w.print(" WHEN {s} THEN {s}", .{ ps, v }) catch return error.OutOfMemory;
            }
        } else {
            const g = (try translateExpr(arena, arm.guard orelse return null, dialect, schema, check_fields)) orelse return null;
            w.print(" WHEN {s} THEN {s}", .{ g, v }) catch return error.OutOfMemory;
        }
    }
    w.writeAll(" END)") catch return error.OutOfMemory;
    return try out.toOwnedSlice();
}

/// Dialect spelling of a CAST target type (null = don't push this cast).
fn sqlTypeName(arena: std.mem.Allocator, dialect: Dialect, ty: types.Type) !?[]const u8 {
    return switch (ty.kind) {
        .int => switch (dialect) {
            .mysql => "SIGNED",
            else => "BIGINT",
        },
        .float => switch (dialect) {
            .sqlserver => "FLOAT",
            .mysql => "DOUBLE",
            .postgres => "DOUBLE PRECISION",
        },
        .string => switch (dialect) {
            .sqlserver => "VARCHAR(MAX)",
            .mysql => "CHAR",
            .postgres => "TEXT",
        },
        .decimal => try std.fmt.allocPrint(arena, "DECIMAL({d},{d})", .{ ty.precision, ty.scale }),
        .date => "DATE",
        .time => "TIME",
        .timestamp => switch (dialect) {
            .sqlserver => "DATETIME2",
            .mysql => "DATETIME",
            .postgres => "TIMESTAMP",
        },
        else => null,
    };
}

/// Scalar-function translation — only names whose semantics are identical in
/// all three dialects (or have an exact per-dialect spelling). StarRocks rides
/// the `.mysql` dialect, so every mysql rendering here must hold there too.
///
/// DELIBERATELY NOT TRANSLATED (don't "helpfully" add these — each one can drop a
/// row the engine's kept filter would keep, which is the one thing pushdown may
/// never do):
///   - `round`         — half-even (postgres numeric) vs half-away-from-zero (mysql,
///                       sqlserver), so .5 cases land on different values.
///   - `greatest`/`least` — mysql returns NULL if ANY arg is null; postgres (and the
///                       engine) ignore nulls and return the extreme of the rest.
///   - `lpad`/`rpad`   — no sqlserver equivalent (and the hand-rolled REPLICATE form
///                       truncates differently when the input is already too long).
///   - `split_part`    — postgres-only; no mysql/sqlserver equivalent.
///   - date functions (`date_add`, `date_diff`, `strftime`, `date_trunc`, …) — divergent
///                       unit syntax, and month arithmetic clamps end-of-month differently.
///   - `try_cast` / safe cast — see the `.cast` arm; null-on-failure must stay engine-side.
fn translateCall(arena: std.mem.Allocator, c: ast.Expr.Call, dialect: Dialect, schema: types.Schema, check_fields: bool) error{OutOfMemory}!?[]const u8 {
    const args = try arena.alloc([]const u8, c.args.len);
    for (c.args, args) |a, *out| out.* = (try translateExpr(arena, a, dialect, schema, check_fields)) orelse return null;

    const n = c.name;
    if (std.mem.eql(u8, n, "lower") and args.len == 1)
        return try std.fmt.allocPrint(arena, "LOWER({s})", .{args[0]});
    if (std.mem.eql(u8, n, "upper") and args.len == 1)
        return try std.fmt.allocPrint(arena, "UPPER({s})", .{args[0]});
    if (std.mem.eql(u8, n, "length") and args.len == 1) {
        const f = switch (dialect) {
            .sqlserver => "LEN",
            .mysql => "CHAR_LENGTH",
            .postgres => "LENGTH",
        };
        return try std.fmt.allocPrint(arena, "{s}({s})", .{ f, args[0] });
    }
    if (std.mem.eql(u8, n, "trim") and args.len == 1) {
        return switch (dialect) {
            .sqlserver => try std.fmt.allocPrint(arena, "LTRIM(RTRIM({s}))", .{args[0]}),
            else => try std.fmt.allocPrint(arena, "TRIM({s})", .{args[0]}),
        };
    }
    if (std.mem.eql(u8, n, "substr") and args.len == 3)
        return try std.fmt.allocPrint(arena, "SUBSTRING({s}, {s}, {s})", .{ args[0], args[1], args[2] });
    if (std.mem.eql(u8, n, "replace") and args.len == 3)
        return try std.fmt.allocPrint(arena, "REPLACE({s}, {s}, {s})", .{ args[0], args[1], args[2] });
    if ((std.mem.eql(u8, n, "concat") or std.mem.eql(u8, n, "coalesce")) and args.len >= 2) {
        const f = if (n[0] == 'c' and n[1] == 'o' and n[2] == 'n') "CONCAT" else "COALESCE";
        const joined = try std.mem.join(arena, ", ", args);
        return try std.fmt.allocPrint(arena, "{s}({s})", .{ f, joined });
    }
    if (std.mem.eql(u8, n, "like") and args.len == 2)
        return try std.fmt.allocPrint(arena, "({s} LIKE {s})", .{ args[0], args[1] });
    if ((std.mem.eql(u8, n, "starts_with") or std.mem.eql(u8, n, "ends_with") or
        std.mem.eql(u8, n, "contains")) and c.args.len == 2)
    {
        if (c.args[1].* != .str_lit) return null;
        const pat = c.args[1].str_lit;
        for (pat) |ch| {
            if (ch == '%' or ch == '_' or ch == '\\') return null;
        }
        const shaped = if (std.mem.eql(u8, n, "starts_with"))
            try std.fmt.allocPrint(arena, "{s}%", .{pat})
        else if (std.mem.eql(u8, n, "ends_with"))
            try std.fmt.allocPrint(arena, "%{s}", .{pat})
        else
            try std.fmt.allocPrint(arena, "%{s}%", .{pat});
        return try std.fmt.allocPrint(arena, "({s} LIKE {s})", .{ args[0], try sqlStr(arena, shaped) });
    }

    // Builtins spelled and evaluated identically on postgres, mysql/starrocks and
    // sqlserver. (Domain edges — SQRT of a negative, POWER(0, -n), MOD by zero — raise
    // on some engines and yield NULL on others; that's a loud query failure rather than
    // a silently dropped row, and the same input is a degenerate case engine-side too.)
    const uniform = [_]struct { name: []const u8, sql: []const u8, arity: usize }{
        .{ .name = "abs", .sql = "ABS", .arity = 1 },
        .{ .name = "floor", .sql = "FLOOR", .arity = 1 },
        .{ .name = "sqrt", .sql = "SQRT", .arity = 1 },
        .{ .name = "sign", .sql = "SIGN", .arity = 1 },
        .{ .name = "reverse", .sql = "REVERSE", .arity = 1 },
        .{ .name = "power", .sql = "POWER", .arity = 2 },
        .{ .name = "nullif", .sql = "NULLIF", .arity = 2 },
    };
    for (uniform) |u| {
        if (std.mem.eql(u8, n, u.name) and args.len == u.arity) {
            const joined = try std.mem.join(arena, ", ", args);
            return try std.fmt.allocPrint(arena, "{s}({s})", .{ u.sql, joined });
        }
    }

    if (std.mem.eql(u8, n, "ceil") and args.len == 1) {
        const f = switch (dialect) {
            .sqlserver => "CEILING", // T-SQL has no CEIL
            else => "CEIL",
        };
        return try std.fmt.allocPrint(arena, "{s}({s})", .{ f, args[0] });
    }
    if (std.mem.eql(u8, n, "mod") and args.len == 2) {
        // sqlserver has no MOD function, only the `%` operator. Both take the sign of
        // the DIVIDEND (`-7 % 3` = `MOD(-7, 3)` = -1) on all four engines, matching the
        // engine's own mod — so the two spellings agree on negatives.
        return switch (dialect) {
            .sqlserver => try std.fmt.allocPrint(arena, "({s} % {s})", .{ args[0], args[1] }),
            else => try std.fmt.allocPrint(arena, "MOD({s}, {s})", .{ args[0], args[1] }),
        };
    }
    if ((std.mem.eql(u8, n, "left") or std.mem.eql(u8, n, "right") or
        std.mem.eql(u8, n, "repeat")) and c.args.len == 2)
    {
        // A negative count diverges: postgres LEFT/RIGHT count back from the far end
        // while mysql/starrocks return '', and sqlserver REPLICATE returns NULL where
        // the others return ''. Push only a literal count we can see is >= 0.
        if (c.args[1].* != .int_lit or c.args[1].int_lit < 0) return null;
        const f: []const u8 = if (std.mem.eql(u8, n, "left"))
            "LEFT"
        else if (std.mem.eql(u8, n, "right"))
            "RIGHT"
        else if (dialect == .sqlserver)
            "REPLICATE" // T-SQL's spelling of REPEAT
        else
            "REPEAT";
        return try std.fmt.allocPrint(arena, "{s}({s}, {s})", .{ f, args[0], args[1] });
    }
    if (std.mem.eql(u8, n, "strpos") and c.args.len == 2) {
        // All three are 1-based with 0 for "not found", like the engine — but mysql's
        // LOCATE and sqlserver's CHARINDEX take (needle, haystack), the reverse of
        // postgres' STRPOS. An empty needle is the one divergence (postgres 1,
        // sqlserver 0), so a literal '' isn't pushed.
        if (c.args[1].* == .str_lit and c.args[1].str_lit.len == 0) return null;
        return switch (dialect) {
            .postgres => try std.fmt.allocPrint(arena, "STRPOS({s}, {s})", .{ args[0], args[1] }),
            .mysql => try std.fmt.allocPrint(arena, "LOCATE({s}, {s})", .{ args[1], args[0] }),
            .sqlserver => try std.fmt.allocPrint(arena, "CHARINDEX({s}, {s})", .{ args[1], args[0] }),
        };
    }
    return null;
}

/// §7 implicit pushdown for a serial pipeline: translate the `filter` stages
/// that immediately follow a SQL read into one AND-ed WHERE fragment. The
/// engine KEEPS the filter stages (superset rule) — the fragment only lets
/// the source pre-narrow, so an untranslatable piece just isn't pushed.
pub fn serialWhere(arena: std.mem.Allocator, dialect: Dialect, stages: []const ast.Stage) !?[]const u8 {
    if (stages.len < 2 or stages[0].node != .read) return null;
    var parts = std.array_list.Managed([]const u8).init(arena);
    for (stages[1..]) |st| {
        if (st.node != .filter) break;
        if (try translateExpr(arena, st.node.filter, dialect, .{ .fields = &.{} }, false)) |sql_frag| {
            try parts.append(sql_frag);
        }
    }
    if (parts.items.len == 0) return null;
    return try std.mem.join(arena, " AND ", parts.items);
}

/// Single-quoted SQL string literal with `'` doubled — ANSI, accepted by all three
/// dialects (and StarRocks).
fn sqlStr(arena: std.mem.Allocator, s: []const u8) ![]const u8 {
    var out = std.array_list.Managed(u8).init(arena);
    try out.append('\'');
    for (s) |c| {
        if (c == '\'') try out.append('\'');
        try out.append(c);
    }
    try out.append('\'');
    return out.toOwnedSlice();
}

/// Collect every source column an expression references (by `parts[0]`, the base
/// column even for a nested path) into `set`.
pub fn collectFields(e: *const ast.Expr, set: *std.StringHashMap(void)) !void {
    switch (e.*) {
        .field => |q| try set.put(q.parts[0], {}),
        .unary => |u| try collectFields(u.e, set),
        .binary => |b| {
            try collectFields(b.l, set);
            try collectFields(b.r, set);
        },
        .cond => |c| {
            try collectFields(c.cond, set);
            try collectFields(c.then, set);
            try collectFields(c.els, set);
        },
        .cast => |c| try collectFields(c.e, set),
        .is_null => |n| try collectFields(n.e, set),
        .let_in => |l| {
            try collectFields(l.value, set);
            try collectFields(l.body, set);
        },
        .call => |c| for (c.args) |a| try collectFields(a, set),
        .match => |m| {
            if (m.subject) |s| try collectFields(s, set);
            for (m.arms) |arm| {
                for (arm.pats) |p| try collectFields(p, set);
                if (arm.guard) |g| try collectFields(g, set);
                try collectFields(arm.value, set);
            }
        },
        else => {},
    }
}

const testing = std.testing;

fn testSchema() types.Schema {
    const I = types.Type.init(.int);
    const S = types.Type.init(.string);
    return .{ .fields = &.{
        .{ .name = "a", .ty = I },
        .{ .name = "b", .ty = S },
        .{ .name = "c", .ty = I },
    } };
}

fn fld(arena: std.mem.Allocator, name: []const u8) !*ast.Expr {
    const q = try arena.create(ast.Expr);
    const parts = try arena.alloc([]const u8, 1);
    parts[0] = name;
    q.* = .{ .field = .{ .parts = parts } };
    return q;
}

fn bin(arena: std.mem.Allocator, op: ast.BinOp, l: *ast.Expr, r: *ast.Expr) !*ast.Expr {
    const e = try arena.create(ast.Expr);
    e.* = .{ .binary = .{ .op = op, .l = l, .r = r } };
    return e;
}

test "translatePred: equality with a string literal escapes quotes" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const lit = try a.create(ast.Expr);
    lit.* = .{ .str_lit = "O'Brien" };
    const e = try bin(a, .eq, try fld(a, "b"), lit);
    const sql = (try translateExpr(a, e, .mysql, testSchema(), true)).?;
    try testing.expectEqualStrings("(`b` = 'O''Brien')", sql);
}

test "translatePred: AND of comparisons, per-dialect quoting" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const lit5 = try a.create(ast.Expr);
    lit5.* = .{ .int_lit = 5 };
    const lit9 = try a.create(ast.Expr);
    lit9.* = .{ .int_lit = 9 };
    const e = try bin(a, .@"and", try bin(a, .ge, try fld(a, "a"), lit5), try bin(a, .lt, try fld(a, "c"), lit9));
    try testing.expectEqualStrings("((\"a\" >= 5) AND (\"c\" < 9))", (try translateExpr(a, e, .postgres, testSchema(), true)).?);
    try testing.expectEqualStrings("(([a] >= 5) AND ([c] < 9))", (try translateExpr(a, e, .sqlserver, testSchema(), true)).?);
}

test "translatePred: is not null" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const e = try a.create(ast.Expr);
    e.* = .{ .is_null = .{ .e = try fld(a, "a"), .negated = true } };
    try testing.expectEqualStrings("(`a` IS NOT NULL)", (try translateExpr(a, e, .mysql, testSchema(), true)).?);
}

test "translatePred: unknown field and unsupported nodes are not pushed" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const lit = try a.create(ast.Expr);
    lit.* = .{ .int_lit = 1 };
    try testing.expect((try translateExpr(a, try bin(a, .eq, try fld(a, "zzz"), lit), .mysql, testSchema(), true)) == null);
    try testing.expect((try translateExpr(a, try bin(a, .add, try fld(a, "a"), lit), .mysql, testSchema(), true)) == null);
    const call = try a.create(ast.Expr);
    call.* = .{ .call = .{ .name = "now", .args = &.{} } };
    try testing.expect((try translateExpr(a, call, .mysql, testSchema(), true)) == null);
}

test "translatePred: bitwise operators are never pushed down" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const four = try a.create(ast.Expr);
    four.* = .{ .int_lit = 4 };

    // `a & 4 = 4` — the AND is untranslatable, so the whole filter stays engine-side.
    const masked = try bin(a, .bit_and, try fld(a, "a"), four);
    const pred = try bin(a, .eq, masked, four);
    for ([_]Dialect{ .mysql, .postgres, .sqlserver }) |d| {
        try testing.expect((try translateExpr(a, pred, d, testSchema(), true)) == null);
        try testing.expect((try translateExpr(a, masked, d, testSchema(), true)) == null);
    }
    for ([_]ast.BinOp{ .bit_or, .bit_xor, .shl, .shr }) |op| {
        try testing.expect((try translateExpr(a, try bin(a, op, try fld(a, "a"), four), .mysql, testSchema(), true)) == null);
    }
    const notx = try a.create(ast.Expr);
    notx.* = .{ .unary = .{ .op = .bit_not, .e = try fld(a, "a") } };
    try testing.expect((try translateExpr(a, notx, .mysql, testSchema(), true)) == null);
}

test "planAgg: projects only referenced columns and pushes the filter" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const I = types.Type.init(.int);
    const S = types.Type.init(.string);
    const schema = types.Schema{ .fields = &.{
        .{ .name = "a", .ty = I }, .{ .name = "b", .ty = S }, .{ .name = "c", .ty = I }, .{ .name = "d", .ty = I },
    } };
    const lit5 = try a.create(ast.Expr);
    lit5.* = .{ .int_lit = 5 };
    const filt = ast.Stage{ .node = .{ .filter = try bin(a, .ge, try fld(a, "a"), lit5) }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
    const by = try a.alloc(ast.QualName, 1);
    by[0] = .{ .parts = &.{"b"} };
    const aggs = try a.alloc(ast.AggItem, 1);
    aggs[0] = .{ .name = "total", .func = .sum, .arg = try fld(a, "c") };
    const ag = ast.Aggregate{ .aggs = aggs, .by = by };

    const plan = try planAgg(a, .sqlserver, schema, &.{filt}, ag);
    try testing.expectEqualStrings("[a], [b], [c]", plan.proj_select.?);
    try testing.expectEqual(@as(usize, 3), plan.proj_schema.?.fields.len);
    try testing.expectEqualStrings("([a] >= 5)", plan.where_extra.?);
}

fn fieldItem(arena: std.mem.Allocator, name: []const u8) !ast.SelectItem {
    const parts = try arena.alloc([]const u8, 1);
    parts[0] = name;
    return .{ .field = .{ .parts = parts } };
}

fn selectStage(arena: std.mem.Allocator, items: []const ast.SelectItem) ast.Stage {
    _ = arena;
    return .{ .node = .{ .select = items }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
}

fn schema4() types.Schema {
    const I = types.Type.init(.int);
    const S = types.Type.init(.string);
    return .{ .fields = &.{
        .{ .name = "a", .ty = I }, .{ .name = "b", .ty = S }, .{ .name = "c", .ty = I }, .{ .name = "d", .ty = I },
    } };
}

test "planMap: a downstream select narrows a wide reconcile (dead items pruned)" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const recon = try a.alloc(ast.SelectItem, 4);
    recon[0] = try fieldItem(a, "a");
    recon[1] = try fieldItem(a, "b");
    recon[2] = try fieldItem(a, "c");
    recon[3] = try fieldItem(a, "d");
    const down = try a.alloc(ast.SelectItem, 2);
    down[0] = try fieldItem(a, "c");
    down[1] = try fieldItem(a, "a");
    const middle = [_]ast.Stage{ selectStage(a, recon), selectStage(a, down) };
    const out_cols = [_][]const u8{ "c", "a" };

    const plan = try planMap(a, .postgres, schema4(), &middle, &out_cols);
    try testing.expectEqualStrings("\"a\", \"c\"", plan.proj_select.?);
    try testing.expectEqual(@as(usize, 2), plan.proj_schema.?.fields.len);
    try testing.expectEqual(@as(usize, 2), plan.stages.?[0].node.select.len);
}

test "planMap: a downstream filter through the reconcile keeps its column live" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const recon = try a.alloc(ast.SelectItem, 4);
    recon[0] = try fieldItem(a, "a");
    recon[1] = try fieldItem(a, "b");
    recon[2] = try fieldItem(a, "c");
    recon[3] = try fieldItem(a, "d");
    const litc = try a.create(ast.Expr);
    litc.* = .{ .int_lit = 1 };
    const fc = ast.Stage{ .node = .{ .filter = try bin(a, .gt, try fld(a, "c"), litc) }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
    const down = try a.alloc(ast.SelectItem, 1);
    down[0] = try fieldItem(a, "b");
    const middle = [_]ast.Stage{ selectStage(a, recon), fc, selectStage(a, down) };
    const out_cols = [_][]const u8{"b"};

    const plan = try planMap(a, .mysql, schema4(), &middle, &out_cols);
    try testing.expect(plan.where_extra == null);
    try testing.expectEqualStrings("`b`, `c`", plan.proj_select.?);
}

test "planMap: a leading filter is pushed as a predicate" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const lit0 = try a.create(ast.Expr);
    lit0.* = .{ .int_lit = 0 };
    const filt = ast.Stage{ .node = .{ .filter = try bin(a, .gt, try fld(a, "a"), lit0) }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
    const down = try a.alloc(ast.SelectItem, 1);
    down[0] = try fieldItem(a, "a");
    const middle = [_]ast.Stage{ filt, selectStage(a, down) };
    const out_cols = [_][]const u8{"a"};

    const plan = try planMap(a, .mysql, schema4(), &middle, &out_cols);
    try testing.expectEqualStrings("(`a` > 0)", plan.where_extra.?);
    try testing.expectEqualStrings("`a`", plan.proj_select.?);
}

test "planMap: a star select disables projection" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const items = try a.alloc(ast.SelectItem, 1);
    items[0] = .star;
    const middle = [_]ast.Stage{selectStage(a, items)};
    const out_cols = [_][]const u8{ "a", "b" };
    const plan = try planMap(a, .postgres, testSchema(), &middle, &out_cols);
    try testing.expect(plan.proj_select == null);
}

test "translatePred: NOT over an OR with a bool literal" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const lit1 = try a.create(ast.Expr);
    lit1.* = .{ .int_lit = 1 };
    const f = try a.create(ast.Expr);
    f.* = .{ .bool_lit = false };
    const or_e = try bin(a, .@"or", try bin(a, .gt, try fld(a, "a"), lit1), f);
    const not_e = try a.create(ast.Expr);
    not_e.* = .{ .unary = .{ .op = .not, .e = or_e } };
    try testing.expectEqualStrings("(NOT (((`a` > 1) OR (1=0))))", (try translateExpr(a, not_e, .mysql, testSchema(), true)).?);
}

test "translatePred: `is empty` translates to null-or-'', plain `is null` to IS NULL" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const e = try a.create(ast.Expr);
    e.* = .{ .is_null = .{ .e = try fld(a, "b"), .negated = false, .kind = .is_empty } };
    try testing.expectEqualStrings("(`b` IS NULL OR `b` = '')", (try translateExpr(a, e, .mysql, testSchema(), true)).?);
    const n = try a.create(ast.Expr);
    n.* = .{ .is_null = .{ .e = try fld(a, "b"), .negated = false, .kind = .is_null } };
    try testing.expectEqualStrings("(`b` IS NULL)", (try translateExpr(a, n, .mysql, testSchema(), true)).?);
}

test "planAgg: no projection when the aggregate consumes every source column" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const by = try a.alloc(ast.QualName, 2);
    by[0] = .{ .parts = &.{"a"} };
    by[1] = .{ .parts = &.{"b"} };
    const aggs = try a.alloc(ast.AggItem, 1);
    aggs[0] = .{ .name = "total", .func = .sum, .arg = try fld(a, "c") };
    const plan = try planAgg(a, .mysql, testSchema(), &.{}, .{ .aggs = aggs, .by = by });
    try testing.expect(plan.proj_select == null);
    try testing.expect(plan.proj_schema == null);
    try testing.expect(plan.where_extra == null);
}

test "planAgg: an untranslatable filter is not pushed but its columns stay projected" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const nowc = try a.create(ast.Expr);
    nowc.* = .{ .call = .{ .name = "now", .args = &.{} } };
    const filt = ast.Stage{ .node = .{ .filter = try bin(a, .gt, try fld(a, "b"), nowc) }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
    const by = try a.alloc(ast.QualName, 1);
    by[0] = .{ .parts = &.{"a"} };
    const aggs = try a.alloc(ast.AggItem, 1);
    aggs[0] = .{ .name = "total", .func = .sum, .arg = try fld(a, "c") };
    const plan = try planAgg(a, .mysql, schema4(), &.{filt}, .{ .aggs = aggs, .by = by });
    try testing.expect(plan.where_extra == null);
    try testing.expectEqualStrings("`a`, `b`, `c`", plan.proj_select.?);
}

test "planMap: no projection when every source column reaches the sink" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const items = try a.alloc(ast.SelectItem, 3);
    items[0] = try fieldItem(a, "a");
    items[1] = try fieldItem(a, "b");
    items[2] = try fieldItem(a, "c");
    const middle = [_]ast.Stage{selectStage(a, items)};
    const out_cols = [_][]const u8{ "a", "b", "c" };
    const plan = try planMap(a, .postgres, testSchema(), &middle, &out_cols);
    try testing.expect(plan.proj_select == null);
    try testing.expect(plan.stages == null);
    try testing.expect(plan.where_extra == null);
}

test "planAgg: a select in the prefix disables projection (filter still pushed)" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const lit5 = try a.create(ast.Expr);
    lit5.* = .{ .int_lit = 5 };
    const filt = ast.Stage{ .node = .{ .filter = try bin(a, .ge, try fld(a, "a"), lit5) }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
    const items = try a.alloc(ast.SelectItem, 1);
    items[0] = .star;
    const sel = ast.Stage{ .node = .{ .select = items }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
    const by = try a.alloc(ast.QualName, 1);
    by[0] = .{ .parts = &.{"b"} };
    const aggs = try a.alloc(ast.AggItem, 1);
    aggs[0] = .{ .name = "n", .func = .count, .arg = null };
    const ag = ast.Aggregate{ .aggs = aggs, .by = by };

    const plan = try planAgg(a, .postgres, testSchema(), &.{ filt, sel }, ag);
    try testing.expect(plan.proj_select == null);
    try testing.expectEqualStrings("(\"a\" >= 5)", plan.where_extra.?);
}

test "translateExpr: extended constructs (is empty, CASE, CAST, functions)" {
    var arn = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arn.deinit();
    const a = arn.allocator();
    const sqlp = @import("../lang/sql_parser.zig");

    const cases = [_]struct { src: []const u8, want: ?[]const u8, d: Dialect = .sqlserver }{
        .{ .src = "status IS EMPTY", .want = "([status] IS NULL OR [status] = '')" },
        .{ .src = "status IS NOT EMPTY", .want = "(NOT ([status] IS NULL OR [status] = ''))" },
        .{ .src = "IF(v > 1, 'a', 'b')", .want = "(CASE WHEN ([v] > 1) THEN 'a' ELSE 'b' END)" },
        .{ .src = "CASE status WHEN 'x', 'y' THEN 1 ELSE 0 END", .want = "(CASE [status] WHEN 'x' THEN 1 WHEN 'y' THEN 1 ELSE 0 END)" },
        .{ .src = "CAST(v AS INT) > 5", .want = "(CAST([v] AS BIGINT) > 5)" },
        .{ .src = "CAST(v AS INT) > 5", .want = "(CAST(`v` AS SIGNED) > 5)", .d = .mysql },
        .{ .src = "lower(status) = 'ok'", .want = "(LOWER([status]) = 'ok')" },
        .{ .src = "length(status) > 2", .want = "(LEN([status]) > 2)" },
        .{ .src = "length(status) > 2", .want = "(CHAR_LENGTH(`status`) > 2)", .d = .mysql },
        .{ .src = "trim(status) = 'x'", .want = "(LTRIM(RTRIM([status])) = 'x')" },
        .{ .src = "substr(status, 1, 2) = 'AB'", .want = "(SUBSTRING([status], 1, 2) = 'AB')" },
        .{ .src = "coalesce(status, 'n') = 'n'", .want = "(COALESCE([status], 'n') = 'n')" },
        .{ .src = "contains(status, 'ab')", .want = "([status] LIKE '%ab%')" },
        .{ .src = "starts_with(status, 'CT2')", .want = "([status] LIKE 'CT2%')" },
        .{ .src = "status LIKE 'a%'", .want = "([status] LIKE 'a%')" },
        .{ .src = "contains(status, '10%')", .want = null },
        .{ .src = "now() > v", .want = null },
        .{ .src = "v + 1 > 2", .want = null },
    };
    for (cases) |tc| {
        var diag: sqlp.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        const e = try sqlp.parseExprStr(a, tc.src, &diag);
        const got = try translateExpr(a, e, tc.d, .{ .fields = &.{} }, false);
        if (tc.want) |w| {
            try std.testing.expectEqualStrings(w, got orelse return error.TestUnexpectedResult);
        } else {
            try std.testing.expect(got == null);
        }
    }
}

test "serialWhere: contiguous filter prefix, partial translation, stops at non-filter" {
    var arn = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arn.deinit();
    const a = arn.allocator();
    const sqlp = @import("../lang/sql_parser.zig");

    var diag: sqlp.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const f1 = try sqlp.parseExprStr(a, "v > 0", &diag);
    const f2 = try sqlp.parseExprStr(a, "v + 1 > 2", &diag);
    const f3 = try sqlp.parseExprStr(a, "status = 'ok'", &diag);
    const rd = ast.Stage{ .node = .{ .read = .{ .connector = "erp", .form = .{ .table = .{ .parts = &.{"T"} } } } }, .hints = &.{}, .pos = .{ .line = 1, .col = 1 } };
    const mk = struct {
        fn f(e: *ast.Expr) ast.Stage {
            return .{ .node = .{ .filter = e }, .hints = &.{}, .pos = .{ .line = 1, .col = 1 } };
        }
    }.f;

    const stages = [_]ast.Stage{ rd, mk(f1), mk(f2), mk(f3) };
    const w = (try serialWhere(a, .sqlserver, &stages)).?;
    try std.testing.expectEqualStrings("([v] > 0) AND ([status] = 'ok')", w);

    const sel = ast.Stage{ .node = .{ .select = &.{.star} }, .hints = &.{}, .pos = .{ .line = 1, .col = 1 } };
    const stages2 = [_]ast.Stage{ rd, sel, mk(f1) };
    try std.testing.expect((try serialWhere(a, .sqlserver, &stages2)) == null);
}

fn intLit(arena: std.mem.Allocator, v: i64) !*ast.Expr {
    const e = try arena.create(ast.Expr);
    e.* = .{ .int_lit = v };
    return e;
}

fn strLit(arena: std.mem.Allocator, s: []const u8) !*ast.Expr {
    const e = try arena.create(ast.Expr);
    e.* = .{ .str_lit = s };
    return e;
}

fn callExpr(arena: std.mem.Allocator, name: []const u8, args: []const *ast.Expr) !*ast.Expr {
    const owned = try arena.alloc(*ast.Expr, args.len);
    @memcpy(owned, args);
    const e = try arena.create(ast.Expr);
    e.* = .{ .call = .{ .name = name, .args = owned } };
    return e;
}

test "translateCall: same-name numeric builtins render identically everywhere" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const abs_e = try callExpr(a, "abs", &[_]*ast.Expr{try fld(a, "a")});
    try testing.expectEqualStrings("ABS([a])", (try translateExpr(a, abs_e, .sqlserver, testSchema(), true)).?);
    try testing.expectEqualStrings("ABS(\"a\")", (try translateExpr(a, abs_e, .postgres, testSchema(), true)).?);
    try testing.expectEqualStrings("ABS(`a`)", (try translateExpr(a, abs_e, .mysql, testSchema(), true)).?);

    const floor_e = try callExpr(a, "floor", &[_]*ast.Expr{try fld(a, "a")});
    try testing.expectEqualStrings("FLOOR([a])", (try translateExpr(a, floor_e, .sqlserver, testSchema(), true)).?);
    const sqrt_e = try callExpr(a, "sqrt", &[_]*ast.Expr{try fld(a, "a")});
    try testing.expectEqualStrings("SQRT(`a`)", (try translateExpr(a, sqrt_e, .mysql, testSchema(), true)).?);
    const sign_e = try callExpr(a, "sign", &[_]*ast.Expr{try fld(a, "a")});
    try testing.expectEqualStrings("SIGN(\"a\")", (try translateExpr(a, sign_e, .postgres, testSchema(), true)).?);
    const rev_e = try callExpr(a, "reverse", &[_]*ast.Expr{try fld(a, "b")});
    try testing.expectEqualStrings("REVERSE([b])", (try translateExpr(a, rev_e, .sqlserver, testSchema(), true)).?);
    const pow_e = try callExpr(a, "power", &[_]*ast.Expr{ try fld(a, "a"), try intLit(a, 2) });
    try testing.expectEqualStrings("POWER([a], 2)", (try translateExpr(a, pow_e, .sqlserver, testSchema(), true)).?);
    const nif_e = try callExpr(a, "nullif", &[_]*ast.Expr{ try fld(a, "b"), try strLit(a, "x") });
    try testing.expectEqualStrings("NULLIF(`b`, 'x')", (try translateExpr(a, nif_e, .mysql, testSchema(), true)).?);

    // Wrong arity is not pushed.
    const bad = try callExpr(a, "abs", &[_]*ast.Expr{ try fld(a, "a"), try intLit(a, 1) });
    try testing.expect((try translateExpr(a, bad, .mysql, testSchema(), true)) == null);
}

test "translateCall: ceil is CEILING on sqlserver, CEIL elsewhere" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const e = try callExpr(a, "ceil", &[_]*ast.Expr{try fld(a, "a")});
    try testing.expectEqualStrings("CEILING([a])", (try translateExpr(a, e, .sqlserver, testSchema(), true)).?);
    try testing.expectEqualStrings("CEIL(\"a\")", (try translateExpr(a, e, .postgres, testSchema(), true)).?);
    try testing.expectEqualStrings("CEIL(`a`)", (try translateExpr(a, e, .mysql, testSchema(), true)).?);
}

test "translateCall: mod is the % operator on sqlserver, MOD elsewhere" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const e = try callExpr(a, "mod", &[_]*ast.Expr{ try fld(a, "a"), try intLit(a, 3) });
    try testing.expectEqualStrings("([a] % 3)", (try translateExpr(a, e, .sqlserver, testSchema(), true)).?);
    try testing.expectEqualStrings("MOD(\"a\", 3)", (try translateExpr(a, e, .postgres, testSchema(), true)).?);
    try testing.expectEqualStrings("MOD(`a`, 3)", (try translateExpr(a, e, .mysql, testSchema(), true)).?);

    // Comparison context: the fragment stays a well-formed operand.
    const cmp = try bin(a, .eq, e, try intLit(a, 0));
    try testing.expectEqualStrings("(([a] % 3) = 0)", (try translateExpr(a, cmp, .sqlserver, testSchema(), true)).?);
}

test "translateCall: strpos swaps its arguments on mysql and sqlserver" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const e = try callExpr(a, "strpos", &[_]*ast.Expr{ try fld(a, "b"), try strLit(a, "xy") });
    try testing.expectEqualStrings("STRPOS(\"b\", 'xy')", (try translateExpr(a, e, .postgres, testSchema(), true)).?);
    try testing.expectEqualStrings("LOCATE('xy', `b`)", (try translateExpr(a, e, .mysql, testSchema(), true)).?);
    try testing.expectEqualStrings("CHARINDEX('xy', [b])", (try translateExpr(a, e, .sqlserver, testSchema(), true)).?);

    // An empty needle is 1 on postgres and 0 on sqlserver — not pushable.
    const empty = try callExpr(a, "strpos", &[_]*ast.Expr{ try fld(a, "b"), try strLit(a, "") });
    try testing.expect((try translateExpr(a, empty, .postgres, testSchema(), true)) == null);
}

test "translateCall: repeat is REPLICATE on sqlserver; left/right are portable" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const rep = try callExpr(a, "repeat", &[_]*ast.Expr{ try fld(a, "b"), try intLit(a, 3) });
    try testing.expectEqualStrings("REPLICATE([b], 3)", (try translateExpr(a, rep, .sqlserver, testSchema(), true)).?);
    try testing.expectEqualStrings("REPEAT(\"b\", 3)", (try translateExpr(a, rep, .postgres, testSchema(), true)).?);
    try testing.expectEqualStrings("REPEAT(`b`, 3)", (try translateExpr(a, rep, .mysql, testSchema(), true)).?);

    const lf = try callExpr(a, "left", &[_]*ast.Expr{ try fld(a, "b"), try intLit(a, 2) });
    try testing.expectEqualStrings("LEFT([b], 2)", (try translateExpr(a, lf, .sqlserver, testSchema(), true)).?);
    const rt = try callExpr(a, "right", &[_]*ast.Expr{ try fld(a, "b"), try intLit(a, 2) });
    try testing.expectEqualStrings("RIGHT(`b`, 2)", (try translateExpr(a, rt, .mysql, testSchema(), true)).?);

    // A negative or non-literal count diverges across dialects — left engine-side.
    const neg = try callExpr(a, "left", &[_]*ast.Expr{ try fld(a, "b"), try intLit(a, -2) });
    try testing.expect((try translateExpr(a, neg, .postgres, testSchema(), true)) == null);
    const dyn = try callExpr(a, "repeat", &[_]*ast.Expr{ try fld(a, "b"), try fld(a, "a") });
    try testing.expect((try translateExpr(a, dyn, .postgres, testSchema(), true)) == null);
}

test "translateCall: excluded builtins fall back to the engine" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const round_e = try callExpr(a, "round", &[_]*ast.Expr{try fld(a, "a")});
    const greatest_e = try callExpr(a, "greatest", &[_]*ast.Expr{ try fld(a, "a"), try fld(a, "c") });
    const lpad_e = try callExpr(a, "lpad", &[_]*ast.Expr{ try fld(a, "b"), try intLit(a, 4), try strLit(a, "0") });
    const split_e = try callExpr(a, "split_part", &[_]*ast.Expr{ try fld(a, "b"), try strLit(a, ","), try intLit(a, 1) });
    const excluded = [_]*ast.Expr{ round_e, greatest_e, lpad_e, split_e };
    for (excluded) |e| {
        try testing.expect((try translateExpr(a, e, .postgres, testSchema(), true)) == null);
        try testing.expect((try translateExpr(a, e, .mysql, testSchema(), true)) == null);
        try testing.expect((try translateExpr(a, e, .sqlserver, testSchema(), true)) == null);
    }

    // …and an excluded function inside a comparison sinks the whole predicate.
    const cmp = try bin(a, .gt, round_e, try intLit(a, 1));
    try testing.expect((try translateExpr(a, cmp, .postgres, testSchema(), true)) == null);
}

test "translateExpr: a safe (TRY_) cast is never pushed" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const plain = try a.create(ast.Expr);
    plain.* = .{ .cast = .{ .e = try fld(a, "b"), .ty = types.Type.init(.int) } };
    try testing.expectEqualStrings("CAST(`b` AS SIGNED)", (try translateExpr(a, plain, .mysql, testSchema(), true)).?);

    const safe = try a.create(ast.Expr);
    safe.* = .{ .cast = .{ .e = try fld(a, "b"), .ty = types.Type.init(.int), .safe = true } };
    try testing.expect((try translateExpr(a, safe, .mysql, testSchema(), true)) == null);
    try testing.expect((try translateExpr(a, safe, .postgres, testSchema(), true)) == null);
    try testing.expect((try translateExpr(a, safe, .sqlserver, testSchema(), true)) == null);
}

// --- planWholeAgg: AUTHORITATIVE whole-aggregate descent -------------------

/// a int, b string, c int, f float, m decimal(12,2), t timestamp.
fn wholeSchema() types.Schema {
    return .{ .fields = &.{
        .{ .name = "a", .ty = types.Type.init(.int) },
        .{ .name = "b", .ty = types.Type.init(.string) },
        .{ .name = "c", .ty = types.Type.init(.int) },
        .{ .name = "f", .ty = types.Type.init(.float) },
        .{ .name = "m", .ty = types.Type.decimal(12, 2) },
        .{ .name = "t", .ty = types.Type.init(.timestamp) },
    } };
}

fn geFilter(arena: std.mem.Allocator, col: []const u8, v: i64) !ast.Stage {
    const lit = try arena.create(ast.Expr);
    lit.* = .{ .int_lit = v };
    return .{ .node = .{ .filter = try bin(arena, .ge, try fld(arena, col), lit) }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
}

fn byList(arena: std.mem.Allocator, names: []const []const u8) ![]ast.QualName {
    const by = try arena.alloc(ast.QualName, names.len);
    for (names, by) |n, *q| {
        const parts = try arena.alloc([]const u8, 1);
        parts[0] = n;
        q.* = .{ .parts = parts };
    }
    return by;
}

const base_t = "SELECT * FROM t";

test "planWholeAgg: grouped multi-aggregate renders with a CAST per dialect" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const filt = try geFilter(a, "a", 5);
    const aggs = try a.alloc(ast.AggItem, 2);
    aggs[0] = .{ .name = "n", .func = .count, .arg = null };
    aggs[1] = .{ .name = "total", .func = .sum, .arg = try fld(a, "c") };
    const ag = ast.Aggregate{ .aggs = aggs, .by = try byList(a, &.{"b"}) };
    const plan_schema = types.Schema{ .fields = &.{
        .{ .name = "b", .ty = types.Type.init(.string) },
        .{ .name = "n", .ty = types.Type.init(.int) },
        .{ .name = "total", .ty = types.Type.init(.int).withNull(true) },
    } };

    const pg = (try planWholeAgg(a, .postgres, base_t, wholeSchema(), &.{filt}, ag, plan_schema)).?;
    try testing.expectEqualStrings(
        "SELECT \"b\" AS \"b\", CAST(COUNT(*) AS BIGINT) AS \"n\", CAST(SUM(\"c\") AS BIGINT) AS \"total\"" ++
            " FROM (SELECT * FROM t) _g WHERE (\"a\" >= 5) GROUP BY \"b\"",
        pg.sql,
    );
    try testing.expectEqualStrings("(\"a\" >= 5)", pg.where_sql.?);

    const my = (try planWholeAgg(a, .mysql, base_t, wholeSchema(), &.{filt}, ag, plan_schema)).?;
    try testing.expectEqualStrings(
        "SELECT `b` AS `b`, CAST(COUNT(*) AS SIGNED) AS `n`, CAST(SUM(`c`) AS SIGNED) AS `total`" ++
            " FROM (SELECT * FROM t) _g WHERE (`a` >= 5) GROUP BY `b`",
        my.sql,
    );

    // T-SQL: COUNT_BIG (COUNT raises past 2^31) and a widened addend (SUM of `int`
    // otherwise accumulates in `int` and overflows where the engine's i64 would not).
    const ms = (try planWholeAgg(a, .sqlserver, base_t, wholeSchema(), &.{filt}, ag, plan_schema)).?;
    try testing.expectEqualStrings(
        "SELECT [b] AS [b], CAST(COUNT_BIG(*) AS BIGINT) AS [n], CAST(SUM(CAST([c] AS BIGINT)) AS BIGINT) AS [total]" ++
            " FROM (SELECT * FROM t) _g WHERE ([a] >= 5) GROUP BY [b]",
        ms.sql,
    );
}

test "planWholeAgg: ungrouped COUNT DISTINCT has no GROUP BY and no WHERE" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const aggs = try a.alloc(ast.AggItem, 1);
    aggs[0] = .{ .name = "u", .func = .count, .arg = try fld(a, "b"), .distinct = true };
    const ag = ast.Aggregate{ .aggs = aggs, .by = &.{} };
    const plan_schema = types.Schema{ .fields = &.{.{ .name = "u", .ty = types.Type.init(.int) }} };

    const pg = (try planWholeAgg(a, .postgres, base_t, wholeSchema(), &.{}, ag, plan_schema)).?;
    try testing.expectEqualStrings(
        "SELECT CAST(COUNT(DISTINCT \"b\") AS BIGINT) AS \"u\" FROM (SELECT * FROM t) _g",
        pg.sql,
    );
    try testing.expect(pg.where_sql == null);

    const ms = (try planWholeAgg(a, .sqlserver, base_t, wholeSchema(), &.{}, ag, plan_schema)).?;
    try testing.expectEqualStrings(
        "SELECT CAST(COUNT_BIG(DISTINCT [b]) AS BIGINT) AS [u] FROM (SELECT * FROM t) _g",
        ms.sql,
    );
}

test "planWholeAgg: a QUERY-form read is wrapped as the subquery, its own WHERE intact" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const aggs = try a.alloc(ast.AggItem, 2);
    aggs[0] = .{ .name = "lo", .func = .min, .arg = try fld(a, "m") };
    aggs[1] = .{ .name = "hi", .func = .max, .arg = try fld(a, "f") };
    const ag = ast.Aggregate{ .aggs = aggs, .by = try byList(a, &.{ "a", "b" }) };
    const plan_schema = types.Schema{ .fields = &.{
        .{ .name = "a", .ty = types.Type.init(.int) },
        .{ .name = "b", .ty = types.Type.init(.string) },
        .{ .name = "lo", .ty = types.Type.decimal(12, 2).withNull(true) },
        .{ .name = "hi", .ty = types.Type.init(.float).withNull(true) },
    } };

    const q = "SELECT * FROM v WHERE D_E_L_E_T_ <> '*'";
    const got = (try planWholeAgg(a, .mysql, q, wholeSchema(), &.{}, ag, plan_schema)).?;
    try testing.expectEqualStrings(
        "SELECT `a` AS `a`, `b` AS `b`, CAST(MIN(`m`) AS DECIMAL(12,2)) AS `lo`, CAST(MAX(`f`) AS DOUBLE) AS `hi`" ++
            " FROM (SELECT * FROM v WHERE D_E_L_E_T_ <> '*') _g GROUP BY `a`, `b`",
        got.sql,
    );
}

test "planWholeAgg: AVG always falls back (int-avg result types diverge)" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const aggs = try a.alloc(ast.AggItem, 1);
    aggs[0] = .{ .name = "m", .func = .avg, .arg = try fld(a, "c") };
    const ag = ast.Aggregate{ .aggs = aggs, .by = try byList(a, &.{"b"}) };
    const plan_schema = types.Schema{ .fields = &.{
        .{ .name = "b", .ty = types.Type.init(.string) },
        .{ .name = "m", .ty = types.Type.init(.float).withNull(true) },
    } };
    for ([_]Dialect{ .postgres, .mysql, .sqlserver }) |d|
        try testing.expect((try planWholeAgg(a, d, base_t, wholeSchema(), &.{}, ag, plan_schema)) == null);
}

test "planWholeAgg: a qualified or unknown group key falls back" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const aggs = try a.alloc(ast.AggItem, 1);
    aggs[0] = .{ .name = "n", .func = .count, .arg = null };

    // `t.b` — two parts; the engine groups by `b` but the subquery alias is `_g`,
    // so the qualified spelling would not resolve there.
    const by = try a.alloc(ast.QualName, 1);
    by[0] = .{ .parts = &.{ "t", "b" } };
    const plan_schema = types.Schema{ .fields = &.{
        .{ .name = "b", .ty = types.Type.init(.string) },
        .{ .name = "n", .ty = types.Type.init(.int) },
    } };
    try testing.expect((try planWholeAgg(a, .postgres, base_t, wholeSchema(), &.{}, .{ .aggs = aggs, .by = by }, plan_schema)) == null);

    // A key the source schema doesn't have at all.
    const missing = try byList(a, &.{"zzz"});
    const ms_schema = types.Schema{ .fields = &.{
        .{ .name = "zzz", .ty = types.Type.init(.string) },
        .{ .name = "n", .ty = types.Type.init(.int) },
    } };
    try testing.expect((try planWholeAgg(a, .postgres, base_t, wholeSchema(), &.{}, .{ .aggs = aggs, .by = missing }, ms_schema)) == null);
}

test "planWholeAgg: an untranslatable filter falls back instead of pushing a superset" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const nowc = try a.create(ast.Expr);
    nowc.* = .{ .call = .{ .name = "now", .args = &.{} } };
    const bad = ast.Stage{ .node = .{ .filter = try bin(a, .gt, try fld(a, "t"), nowc) }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
    const ok = try geFilter(a, "a", 5);

    const aggs = try a.alloc(ast.AggItem, 1);
    aggs[0] = .{ .name = "n", .func = .count, .arg = null };
    const ag = ast.Aggregate{ .aggs = aggs, .by = try byList(a, &.{"b"}) };
    const plan_schema = types.Schema{ .fields = &.{
        .{ .name = "b", .ty = types.Type.init(.string) },
        .{ .name = "n", .ty = types.Type.init(.int) },
    } };

    // One translatable filter alone is fine…
    try testing.expect((try planWholeAgg(a, .mysql, base_t, wholeSchema(), &.{ok}, ag, plan_schema)) != null);
    // …but one untranslatable filter anywhere in the prefix sinks the whole descent.
    try testing.expect((try planWholeAgg(a, .mysql, base_t, wholeSchema(), &.{ ok, bad }, ag, plan_schema)) == null);

    // A `select` in the prefix does too: it renames columns out from under the keys.
    const items = try a.alloc(ast.SelectItem, 1);
    items[0] = .star;
    const sel = ast.Stage{ .node = .{ .select = items }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
    try testing.expect((try planWholeAgg(a, .mysql, base_t, wholeSchema(), &.{sel}, ag, plan_schema)) == null);
}

test "planWholeAgg: SUM only descends for an int column into an int result" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const by = try byList(a, &.{"b"});
    const key = types.Schema.Field{ .name = "b", .ty = types.Type.init(.string) };

    // float: postgres SUM(real) accumulates in float4, the engine in f64.
    const fa = try a.alloc(ast.AggItem, 1);
    fa[0] = .{ .name = "s", .func = .sum, .arg = try fld(a, "f") };
    const fs = types.Schema{ .fields = &.{ key, .{ .name = "s", .ty = types.Type.init(.float).withNull(true) } } };
    try testing.expect((try planWholeAgg(a, .postgres, base_t, wholeSchema(), &.{}, .{ .aggs = fa, .by = by }, fs)) == null);

    // decimal: the sum can outgrow the column's own precision, which the CAST would
    // raise on where the engine's i64 unscaled accumulator would not.
    const da = try a.alloc(ast.AggItem, 1);
    da[0] = .{ .name = "s", .func = .sum, .arg = try fld(a, "m") };
    const ds = types.Schema{ .fields = &.{ key, .{ .name = "s", .ty = types.Type.decimal(12, 2).withNull(true) } } };
    try testing.expect((try planWholeAgg(a, .mysql, base_t, wholeSchema(), &.{}, .{ .aggs = da, .by = by }, ds)) == null);

    // DISTINCT is honoured by the engine only on COUNT, so never render it elsewhere.
    const dd = try a.alloc(ast.AggItem, 1);
    dd[0] = .{ .name = "s", .func = .sum, .arg = try fld(a, "c"), .distinct = true };
    const is = types.Schema{ .fields = &.{ key, .{ .name = "s", .ty = types.Type.init(.int).withNull(true) } } };
    try testing.expect((try planWholeAgg(a, .mysql, base_t, wholeSchema(), &.{}, .{ .aggs = dd, .by = by }, is)) == null);
}

test "planWholeAgg: MIN/MAX falls back on collation- and timezone-sensitive types" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    // A string extreme follows the source COLLATION (case-insensitive by default on
    // mysql and sqlserver); the engine compares bytes.
    const sa = try a.alloc(ast.AggItem, 1);
    sa[0] = .{ .name = "lo", .func = .min, .arg = try fld(a, "b") };
    const ss = types.Schema{ .fields = &.{.{ .name = "lo", .ty = types.Type.init(.string).withNull(true) }} };
    for ([_]Dialect{ .postgres, .mysql, .sqlserver }) |d|
        try testing.expect((try planWholeAgg(a, d, base_t, wholeSchema(), &.{}, .{ .aggs = sa, .by = &.{} }, ss)) == null);

    // A timestamp CAST can shift the value under a session timezone.
    const ta = try a.alloc(ast.AggItem, 1);
    ta[0] = .{ .name = "hi", .func = .max, .arg = try fld(a, "t") };
    const ts = types.Schema{ .fields = &.{.{ .name = "hi", .ty = types.Type.init(.timestamp).withNull(true) }} };
    try testing.expect((try planWholeAgg(a, .postgres, base_t, wholeSchema(), &.{}, .{ .aggs = ta, .by = &.{} }, ts)) == null);

    // An unresolved decimal precision has no valid cast target either.
    const ma = try a.alloc(ast.AggItem, 1);
    ma[0] = .{ .name = "lo", .func = .min, .arg = try fld(a, "m") };
    const bad = types.Schema{ .fields = &.{.{ .name = "lo", .ty = types.Type.decimal(0, 0).withNull(true) }} };
    try testing.expect((try planWholeAgg(a, .mysql, base_t, wholeSchema(), &.{}, .{ .aggs = ma, .by = &.{} }, bad)) == null);
}

test "planWholeAgg: a non-bare aggregate argument falls back" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const two = try a.create(ast.Expr);
    two.* = .{ .int_lit = 2 };
    const aggs = try a.alloc(ast.AggItem, 1);
    aggs[0] = .{ .name = "s", .func = .sum, .arg = try bin(a, .add, try fld(a, "c"), two) };
    const plan_schema = types.Schema{ .fields = &.{.{ .name = "s", .ty = types.Type.init(.int).withNull(true) }} };
    try testing.expect((try planWholeAgg(a, .postgres, base_t, wholeSchema(), &.{}, .{ .aggs = aggs, .by = &.{} }, plan_schema)) == null);
}


// ---------------------------------------------------------------------------
// Moving a filter below a join
//
// `serialWhere` above only reads the *contiguous* filter prefix after a read, so
// a join between the read and the filter stopped every descent: `FROM fact JOIN
// dim ON … WHERE fact.d = '…'` pulled the whole fact table over the wire and
// filtered it here. On an 80M-row table that measured 18ms against 15.7s.
//
// Every comparable engine solves this the same way — filter pushdown as a rewrite
// on a plan tree (DuckDB's `filter_pushdown.cpp`, DataFusion's `push_down_filter`,
// Polars' `predicate_pushdown`, Trino's `PredicatePushDown`) — and basalt has no
// tree to rewrite. What it does have is a stage list where a join's right side is
// always a named binding, so the two rewrites below work locally on that list and
// need no optimizer framework:
//
//   hoist:  read | join | filter(probe cols)  ->  read | filter | join
//   derive: read | join(l.k = r.k) | filter(r.k = 'x')
//             ->  read | filter(l.k = 'x') | join | filter(r.k = 'x')
//
// The second is DuckDB's equivalence-set trick: a predicate on one side of an
// equijoin also constrains the other, so a filter naming the dimension's key can
// prune the fact table at the source.
//
// Pushdown is where these engines have shipped *wrong answers* rather than slow
// ones — ClickHouse generating bad queries against distributed tables, Polars
// returning wrong rows after join/select/filter — so both rewrites here refuse
// unless they can prove the move is safe, and refusing only costs the old speed.

/// Join kinds a probe-only filter may move below.
///
/// `inner` and `cross` are products, so filtering the probe before or after is the
/// same rows. `left` keeps every probe row (the right side is null-extended, which
/// a probe-only predicate cannot see), and `semi`/`anti` emit a subset of probe
/// rows and no right columns at all.
///
/// `right` and `full` are excluded because there the *probe* side is the one that
/// gets null-extended: a filter above the join sees rows that do not exist below
/// it, and moving it changes the answer.
fn hoistableKind(k: ast.JoinKind) bool {
    return switch (k) {
        .inner, .cross, .left, .semi, .anti => true,
        .right, .full => false,
    };
}

/// Output column names of a binding, or null when they cannot be known statically —
/// a `SELECT *` over a source whose schema only the source can describe. Null means
/// "cannot prove", and every caller treats that as "do not move".
fn bindingNames(
    arena: std.mem.Allocator,
    bindings: *const std.StringHashMap(ast.Pipeline),
    name: []const u8,
) !?[]const []const u8 {
    const pipe = bindings.get(name) orelse return null;
    var i = pipe.stages.len;
    while (i > 0) {
        i -= 1;
        switch (pipe.stages[i].node) {
            .select => |items| {
                const out = try arena.alloc([]const u8, items.len);
                for (items, out) |it, *o| o.* = switch (it) {
                    .field => |q| q.last(),
                    .computed => |c| c.name,
                    // A star of any kind leaves the name set open.
                    else => return null,
                };
                return out;
            },
            // Anything else between the read and here neither adds nor renames a
            // column, so keep looking for the projection that names them.
            .filter, .limit, .sort, .distinct => {},
            else => return null,
        }
    }
    return null;
}

/// Whether `name` could be a column the join's right side contributed — including
/// the `_r`, `_r2`, … suffixes a colliding right-side name comes back under.
fn isRightName(name: []const u8, right: []const []const u8) bool {
    for (right) |r| {
        if (std.mem.eql(u8, name, r)) return true;
        if (!std.mem.startsWith(u8, name, r)) continue;
        const tail = name[r.len..];
        if (std.mem.eql(u8, tail, "_r")) return true;
        if (tail.len > 2 and std.mem.startsWith(u8, tail, "_r")) {
            var all_digits = true;
            for (tail[2..]) |c| if (!std.ascii.isDigit(c)) {
                all_digits = false;
            };
            if (all_digits) return true;
        }
    }
    return false;
}

/// True when every column the predicate names is one the probe side already had.
/// Aliases are stripped at parse time, so a bare name cannot say which side it came
/// from — but the only other supplier is the right side, and that one is
/// enumerable, so "not the right side's" is a proof of "the probe's".
fn refsOnlyProbe(
    arena: std.mem.Allocator,
    e: *const ast.Expr,
    right: []const []const u8,
    binding: []const u8,
) !bool {
    var list = std.array_list.Managed(ast.QualName).init(arena);
    try collectQuals(arena, e, &list);
    for (list.items) |q| {
        // `r.name` where `r` is the join's binding is the right side, said outright.
        if (q.parts.len > 1 and std.mem.eql(u8, q.parts[0], binding)) return false;
        if (isRightName(q.last(), right)) return false;
    }
    return true;
}


/// Collect the `QualName` of every column reference, qualifier included.
///
/// `collectFields` above keys on `parts[0]`, which for `r.name` is the *qualifier*
/// `r` and not the column — checking eligibility against that hoisted right-side
/// filters as if they named probe columns. Both halves are needed here: the
/// qualifier can name the join's binding outright, and the last part is the column.
const QualWalk = struct { arena: std.mem.Allocator, list: *std.array_list.Managed(ast.QualName) };

fn collectQualsRecur(cx: QualWalk, e: *const ast.Expr) error{OutOfMemory}!*ast.Expr {
    if (e.* == .field) {
        try cx.list.append(e.field);
        return @constCast(e);
    }
    return ast.rebuildExpr(cx.arena, e, cx, collectQualsRecur);
}

fn collectQuals(arena: std.mem.Allocator, e: *const ast.Expr, list: *std.array_list.Managed(ast.QualName)) !void {
    _ = try collectQualsRecur(.{ .arena = arena, .list = list }, e);
}

const KeySwap = struct {
    arena: std.mem.Allocator,
    /// right key name -> left key name
    map: *const std.StringHashMap([]const u8),
};

fn swapKeysRecur(cx: KeySwap, e: *const ast.Expr) error{OutOfMemory}!*ast.Expr {
    if (e.* == .field) {
        {
            const nm = e.field.last();
            if (cx.map.get(nm)) |left| {
                const parts = try cx.arena.alloc([]const u8, 1);
                parts[0] = left;
                return mkExpr(cx.arena, .{ .field = .{ .parts = parts } });
            }
        }
        return @constCast(e);
    }
    return ast.rebuildExpr(cx.arena, e, cx, swapKeysRecur);
}

fn mkExpr(arena: std.mem.Allocator, e: ast.Expr) !*ast.Expr {
    const p = try arena.create(ast.Expr);
    p.* = e;
    return p;
}

/// The probe-side twin of a predicate that names only right-side join keys, or null
/// when there is none to derive.
///
/// Sound because an equijoin never matches a null key: if the surviving rows must
/// have `r.k = 'x'` and they are paired by `l.k = r.k`, then their `l.k` is `'x'`
/// too. Restricted to `inner`: under `left`/`anti` a probe row that matches nothing
/// still reaches the output, so constraining it by the right side's predicate would
/// drop rows the query asked for.
fn deriveProbePredicate(
    arena: std.mem.Allocator,
    gpa: std.mem.Allocator,
    e: *const ast.Expr,
    j: ast.Join,
) !?*ast.Expr {
    if (j.kind != .inner) return null;
    if (j.left_keys.len == 0 or j.left_keys.len != j.right_keys.len) return null;

    var map = std.StringHashMap([]const u8).init(gpa);
    defer map.deinit();
    for (j.right_keys, j.left_keys) |r, l| try map.put(r.last(), l.last());

    var list = std.array_list.Managed(ast.QualName).init(arena);
    try collectQuals(arena, e, &list);
    if (list.items.len == 0) return null;
    for (list.items) |q| {
        // Every reference has to be a right key with a left twin; a predicate
        // mentioning any other column has no probe-side equivalent.
        if (map.get(q.last()) == null) return null;
    }
    return try swapKeysRecur(.{ .arena = arena, .map = &map }, e);
}

/// Rewrite `stages` so filters sit as early as the join structure allows. Returns
/// null when nothing moved, so the caller keeps its original slice.
pub fn hoistThroughJoins(
    arena: std.mem.Allocator,
    gpa: std.mem.Allocator,
    stages: []const ast.Stage,
    bindings: *const std.StringHashMap(ast.Pipeline),
) !?[]const ast.Stage {
    if (stages.len < 3) return null;
    var list = std.array_list.Managed(ast.Stage).init(arena);
    try list.appendSlice(stages);
    var changed = false;

    // Hoist: swap a probe-only filter with the join in front of it, repeatedly, so
    // a filter can cross several joins. Bounded by the stage count squared, which is
    // a handful — a pipeline is tens of stages, not thousands.
    var rounds: usize = 0;
    while (rounds < list.items.len) : (rounds += 1) {
        var moved_this_round = false;
        var i: usize = 1;
        while (i < list.items.len) : (i += 1) {
            if (list.items[i].node != .filter) continue;
            if (list.items[i - 1].node != .join) continue;
            const j = list.items[i - 1].node.join;
            if (!hoistableKind(j.kind)) continue;
            const right = (try bindingNames(arena, bindings, j.binding)) orelse continue;
            if (!try refsOnlyProbe(arena, list.items[i].node.filter, right, j.binding)) continue;
            const tmp = list.items[i - 1];
            list.items[i - 1] = list.items[i];
            list.items[i] = tmp;
            moved_this_round = true;
            changed = true;
        }
        if (!moved_this_round) break;
    }

    // Derive: once per join, and only for the filters directly above it, so there is
    // no chance of deriving the same predicate twice.
    var k: usize = 0;
    while (k + 1 < list.items.len) : (k += 1) {
        if (list.items[k].node != .join) continue;
        const j = list.items[k].node.join;
        var m = k + 1;
        var inserted: usize = 0;
        while (m < list.items.len and list.items[m].node == .filter) : (m += 1) {
            const derived = (try deriveProbePredicate(arena, gpa, list.items[m].node.filter, j)) orelse continue;
            try list.insert(k, .{ .node = .{ .filter = derived }, .hints = &.{}, .pos = list.items[m].pos });
            inserted += 1;
            m += 1;
            changed = true;
        }
        k += inserted;
    }

    if (!changed) return null;
    return try list.toOwnedSlice();
}

// --- the join rewrites -----------------------------------------------------

fn qual(arena: std.mem.Allocator, parts: []const []const u8) !ast.QualName {
    const p = try arena.alloc([]const u8, parts.len);
    for (parts, p) |src, *dst| dst.* = src;
    return .{ .parts = p };
}

fn qfld(arena: std.mem.Allocator, parts: []const []const u8) !*ast.Expr {
    const e = try arena.create(ast.Expr);
    e.* = .{ .field = try qual(arena, parts) };
    return e;
}

/// `<parts> = <int>` as a filter stage.
fn eqFilter(arena: std.mem.Allocator, parts: []const []const u8, v: i64) !ast.Stage {
    const lit = try arena.create(ast.Expr);
    lit.* = .{ .int_lit = v };
    return .{ .node = .{ .filter = try bin(arena, .eq, try qfld(arena, parts), lit) }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
}

fn joinStage(arena: std.mem.Allocator, kind: ast.JoinKind, binding: []const u8, lk: []const u8, rk: []const u8) !ast.Stage {
    const lefts = try arena.alloc(ast.QualName, 1);
    lefts[0] = try qual(arena, &.{lk});
    const rights = try arena.alloc(ast.QualName, 1);
    rights[0] = try qual(arena, &.{rk});
    return .{ .node = .{ .join = .{ .kind = kind, .binding = binding, .left_keys = lefts, .right_keys = rights } }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
}

/// A binding whose projection names `rk` and `name`, so its output columns are
/// statically knowable — which is what the rewrites require before moving anything.
fn dimBindings(arena: std.mem.Allocator) !std.StringHashMap(ast.Pipeline) {
    var m = std.StringHashMap(ast.Pipeline).init(arena);
    const items = try arena.alloc(ast.SelectItem, 2);
    items[0] = .{ .field = try qual(arena, &.{"rk"}) };
    items[1] = .{ .field = try qual(arena, &.{"name"}) };
    const stages = try arena.alloc(ast.Stage, 2);
    stages[0] = .{ .node = .{ .read = .{ .connector = "csv", .form = .{ .path = "dim.csv" } } }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
    stages[1] = .{ .node = .{ .select = items }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
    try m.put("r", .{ .stages = stages, .pos = .{ .line = 0, .col = 0 } });
    return m;
}

fn readStage() ast.Stage {
    return .{ .node = .{ .read = .{ .connector = "mysql", .form = .{ .table = .{ .parts = &.{"t"} } } } }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
}

fn writeStage() ast.Stage {
    return .{ .node = .{ .write = .{ .connector = "stdout", .form = null, .target = "", .mode = .default } }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
}

test "hoist: a probe-only filter moves below an inner join" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var binds = try dimBindings(a);
    defer binds.deinit();

    // read | join | filter(v = 1) | write
    const stages = try a.alloc(ast.Stage, 4);
    stages[0] = readStage();
    stages[1] = try joinStage(a, .inner, "r", "k", "rk");
    stages[2] = try eqFilter(a, &.{"v"}, 1);
    stages[3] = writeStage();

    const out = (try hoistThroughJoins(a, a, stages, &binds)).?;
    try std.testing.expectEqual(@as(usize, 4), out.len);
    // The filter now sits directly after the read, which is the prefix `serialWhere`
    // reads — so the predicate can descend into the source query.
    try std.testing.expect(out[1].node == .filter);
    try std.testing.expect(out[2].node == .join);

    const d: Dialect = .postgres;
    const where = (try serialWhere(a, d, out[0 .. out.len - 1])).?;
    try std.testing.expectEqualStrings("(\"v\" = 1)", where);
}

test "hoist: a filter naming a right-side column stays put" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var binds = try dimBindings(a);
    defer binds.deinit();

    for ([_][]const []const u8{
        &.{"name"}, // bare right-side column
        &.{ "r", "name" }, // qualified by the binding
        &.{"name_r"}, // the suffix a colliding right name comes back under
    }) |parts| {
        const stages = try a.alloc(ast.Stage, 4);
        stages[0] = readStage();
        stages[1] = try joinStage(a, .inner, "r", "k", "rk");
        stages[2] = try eqFilter(a, parts, 1);
        stages[3] = writeStage();
        // Nothing to move: the predicate cannot be evaluated before the join.
        try std.testing.expect((try hoistThroughJoins(a, a, stages, &binds)) == null);
    }
}

test "hoist: refused for the join kinds that null-extend the probe side" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var binds = try dimBindings(a);
    defer binds.deinit();

    // A probe row that only exists null-extended cannot be filtered beforehand.
    for ([_]ast.JoinKind{ .right, .full }) |kind| {
        const stages = try a.alloc(ast.Stage, 4);
        stages[0] = readStage();
        stages[1] = try joinStage(a, kind, "r", "k", "rk");
        stages[2] = try eqFilter(a, &.{"v"}, 1);
        stages[3] = writeStage();
        try std.testing.expect((try hoistThroughJoins(a, a, stages, &binds)) == null);
    }
    // Whereas these preserve or subset the probe rows, so it is safe.
    for ([_]ast.JoinKind{ .inner, .left, .semi, .anti, .cross }) |kind| {
        const stages = try a.alloc(ast.Stage, 4);
        stages[0] = readStage();
        stages[1] = try joinStage(a, kind, "r", "k", "rk");
        stages[2] = try eqFilter(a, &.{"v"}, 1);
        stages[3] = writeStage();
        try std.testing.expect((try hoistThroughJoins(a, a, stages, &binds)) != null);
    }
}

test "hoist: a binding with an open name set is left alone" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    // `SELECT *` over a source whose columns only the source knows: the right-side
    // name set cannot be enumerated, so "not the right side's" is unprovable.
    var binds = std.StringHashMap(ast.Pipeline).init(a);
    defer binds.deinit();
    const items = try a.alloc(ast.SelectItem, 1);
    items[0] = .star;
    const bs = try a.alloc(ast.Stage, 2);
    bs[0] = readStage();
    bs[1] = .{ .node = .{ .select = items }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
    try binds.put("r", .{ .stages = bs, .pos = .{ .line = 0, .col = 0 } });

    const stages = try a.alloc(ast.Stage, 4);
    stages[0] = readStage();
    stages[1] = try joinStage(a, .inner, "r", "k", "rk");
    stages[2] = try eqFilter(a, &.{"v"}, 1);
    stages[3] = writeStage();
    try std.testing.expect((try hoistThroughJoins(a, a, stages, &binds)) == null);
}

test "derive: a predicate on the join key gains a probe-side twin" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var binds = try dimBindings(a);
    defer binds.deinit();

    // read | join(k = rk) | filter(rk = 7) | write
    const stages = try a.alloc(ast.Stage, 4);
    stages[0] = readStage();
    stages[1] = try joinStage(a, .inner, "r", "k", "rk");
    stages[2] = try eqFilter(a, &.{ "r", "rk" }, 7);
    stages[3] = writeStage();

    const out = (try hoistThroughJoins(a, a, stages, &binds)).?;
    // The original stays; a twin on the probe key appears ahead of the join, which
    // is what lets a filter written against the dimension prune the fact table.
    try std.testing.expectEqual(@as(usize, 5), out.len);
    try std.testing.expect(out[1].node == .filter);
    try std.testing.expect(out[2].node == .join);
    try std.testing.expect(out[3].node == .filter);

    const where = (try serialWhere(a, .postgres, out[0..2])).?;
    try std.testing.expectEqualStrings("(\"k\" = 7)", where);
}

test "derive: only for an inner join, and only when every ref is a key" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var binds = try dimBindings(a);
    defer binds.deinit();

    // Under a left/anti join a probe row that matches nothing still reaches the
    // output, so constraining the probe by the right side's predicate drops rows the
    // query asked for.
    for ([_]ast.JoinKind{ .left, .anti, .semi, .right, .full, .cross }) |kind| {
        const stages = try a.alloc(ast.Stage, 4);
        stages[0] = readStage();
        stages[1] = try joinStage(a, kind, "r", "k", "rk");
        stages[2] = try eqFilter(a, &.{ "r", "rk" }, 7);
        stages[3] = writeStage();
        try std.testing.expect((try hoistThroughJoins(a, a, stages, &binds)) == null);
    }

    // A predicate touching a non-key right column has no probe-side equivalent.
    const stages = try a.alloc(ast.Stage, 4);
    stages[0] = readStage();
    stages[1] = try joinStage(a, .inner, "r", "k", "rk");
    stages[2] = try eqFilter(a, &.{ "r", "name" }, 7);
    stages[3] = writeStage();
    try std.testing.expect((try hoistThroughJoins(a, a, stages, &binds)) == null);
}
