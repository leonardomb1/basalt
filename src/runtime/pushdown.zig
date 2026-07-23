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

    // --- projection: which source columns does the aggregate actually need? ---
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

    // --- predicate: translate each prefix filter; AND the pushable ones ---
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

    var nf: usize = middle.len; // index of the first non-filter stage
    for (middle, 0..) |st, i| if (st.node != .filter) {
        nf = i;
        break;
    };
    const leading = middle[0..nf];

    // predicate: the leading filters reference source columns directly
    var where = std.array_list.Managed(u8).init(arena);
    for (leading) |st| {
        const frag = (try translateExpr(arena, st.node.filter, dialect, src_schema, true)) orelse continue;
        if (where.items.len > 0) try where.appendSlice(" AND ");
        try where.appendSlice(frag);
    }
    if (where.items.len > 0) plan.where_extra = try where.toOwnedSlice();

    // projection: backward liveness from the output columns to the source columns, with
    // dead-item elimination — each select keeps only the items still live downstream, so a
    // wide reconcile narrows to what the final `select` actually emits. (`arena`-backed
    // maps; intermediates are reclaimed with the arena, not deinit'd.)
    var live = std.StringHashMap(void).init(arena);
    for (out_cols) |c| try live.put(c, {});
    var pruned_rev = std.array_list.Managed(ast.Stage).init(arena);
    var proj_ok = true;
    var i = middle.len;
    while (i > 0 and proj_ok) {
        i -= 1;
        const st = middle[i];
        switch (st.node) {
            // A filter keeps the schema and reads its predicate's columns.
            .filter => |pred| {
                try collectFields(pred, &live);
                try pruned_rev.append(st);
            },
            // A select redefines the schema: keep each item whose output is still live,
            // make that item's source reads live, and drop the rest.
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
                    else => proj_ok = false, // star family: can't attribute precisely
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
            for (pruned_rev.items, 0..) |st, k| pruned[pruned_rev.items.len - 1 - k] = st; // reverse to source→sink
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
            // `is empty` == null OR '' — exact, portable
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
                else => return null, // arithmetic: coercion/division semantics differ — skip
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
            const inner = (try translateExpr(arena, c.e, dialect, schema, check_fields)) orelse return null;
            const ty = sqlTypeName(arena, dialect, c.ty) catch return error.OutOfMemory;
            return try std.fmt.allocPrint(arena, "CAST({s} AS {s})", .{ inner, (ty orelse return null) });
        },
        .call => |c| return translateCall(arena, c, dialect, schema, check_fields),
        else => return null, // let_in (inlined before planning anyway)
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
    // no ELSE -> SQL CASE yields NULL, matching the engine's match semantics
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
        else => null, // bool/bytes: cast semantics diverge per dialect
    };
}

/// Scalar-function translation — only names whose semantics are identical in
/// all three dialects (or have an exact per-dialect spelling).
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
            .sqlserver => try std.fmt.allocPrint(arena, "LTRIM(RTRIM({s}))", .{args[0]}), // TRIM is 2017+
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
    // starts_with/ends_with/contains -> LIKE, only for literal patterns free
    // of wildcard chars (no ESCAPE portability games).
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
    return null; // now()/today() (clock skew!), unknown/user fns: keep engine-side
}

/// §7 implicit pushdown for a serial pipeline: translate the `filter` stages
/// that immediately follow a SQL read into one AND-ed WHERE fragment. The
/// engine KEEPS the filter stages (superset rule) — the fragment only lets
/// the source pre-narrow, so an untranslatable piece just isn't pushed.
pub fn serialWhere(arena: std.mem.Allocator, dialect: Dialect, stages: []const ast.Stage) !?[]const u8 {
    if (stages.len < 2 or stages[0].node != .read) return null;
    var parts = std.array_list.Managed([]const u8).init(arena);
    for (stages[1..]) |st| {
        if (st.node != .filter) break; // only the contiguous filter prefix
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
fn collectFields(e: *const ast.Expr, set: *std.StringHashMap(void)) !void {
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
        else => {}, // literals reference nothing
    }
}

// ---------------------------------------------------------------------------
// Tests (pure: SQL fragment generation — no DB)
// ---------------------------------------------------------------------------

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
    // unknown column → null
    const lit = try a.create(ast.Expr);
    lit.* = .{ .int_lit = 1 };
    try testing.expect((try translateExpr(a, try bin(a, .eq, try fld(a, "zzz"), lit), .mysql, testSchema(), true)) == null);
    // arithmetic → null
    try testing.expect((try translateExpr(a, try bin(a, .add, try fld(a, "a"), lit), .mysql, testSchema(), true)) == null);
    // an untranslatable function (clock skew) → null
    const call = try a.create(ast.Expr);
    call.* = .{ .call = .{ .name = "now", .args = &.{} } };
    try testing.expect((try translateExpr(a, call, .mysql, testSchema(), true)) == null);
}

test "planAgg: projects only referenced columns and pushes the filter" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    // prefix: filter a >= 5 ; aggregate sum(c) by b  → needs a, b, c → here all 3, so
    // use a 4-col schema so projection actually drops one.
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
    try testing.expectEqualStrings("[a], [b], [c]", plan.proj_select.?); // d dropped
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
    // reconcile passes a,b,c,d; a downstream select narrows to c,a (the output cols).
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
    try testing.expectEqualStrings("\"a\", \"c\"", plan.proj_select.?); // b, d dropped (never reach the sink)
    try testing.expectEqual(@as(usize, 2), plan.proj_schema.?.fields.len);
    // the reconcile stage was pruned to its two live items.
    try testing.expectEqual(@as(usize, 2), plan.stages.?[0].node.select.len);
}

test "planMap: a downstream filter through the reconcile keeps its column live" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    // reconcile(a,b,c,d) | filter c > 1 | select b  → output b, but c stays live for the filter.
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
    try testing.expect(plan.where_extra == null); // the filter is after the reconcile → not a leading filter
    try testing.expectEqualStrings("`b`, `c`", plan.proj_select.?); // b (output) + c (filter); a, d dropped
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
    try testing.expectEqualStrings("`a`", plan.proj_select.?); // only a reaches the sink / filter
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
    try testing.expect(plan.proj_select == null); // select present → no projection
    try testing.expectEqualStrings("(\"a\" >= 5)", plan.where_extra.?); // filter still pushed
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
        // refusals: wildcards in the pattern, clock functions, arithmetic
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

    // read | filter (translatable) | filter (not) | filter (translatable)
    var diag: sqlp.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const f1 = try sqlp.parseExprStr(a, "v > 0", &diag);
    const f2 = try sqlp.parseExprStr(a, "v + 1 > 2", &diag); // arithmetic: refused
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

    // filter after a select is NOT part of the prefix
    const sel = ast.Stage{ .node = .{ .select = &.{.star} }, .hints = &.{}, .pos = .{ .line = 1, .col = 1 } };
    const stages2 = [_]ast.Stage{ rd, sel, mk(f1) };
    try std.testing.expect((try serialWhere(a, .sqlserver, &stages2)) == null);
}

