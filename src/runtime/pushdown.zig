//! Predicate, projection, aggregate and top-N pushdown into SQL sources.
//!
//! A split-parallel `read <sqltable> | (filter)* | aggregate … by …` reads one key
//! range per lane (see `connect/split.zig`). Without pushdown each lane runs
//! `SELECT * FROM (base) WHERE <key range>` and ships every column of every row.
//! `planAgg` and `planMap` narrow each lane's query to the source columns that are
//! actually consumed and translate the leading filters into a WHERE AND-ed onto
//! the key range. `serialWhere` does the same for a serial pipeline (§7 implicit
//! pushdown).
//!
//! Correctness rests on two things: basalt's filter is 3-valued exactly like SQL
//! (only a known-true keeps the row, see `op.applyFilter`), so comparisons and
//! and/or/not/is-null map 1:1; and the engine keeps its filter ops, so an advisory
//! pushed predicate only has to be a superset. `Need` states what a rendering must
//! guarantee: `superset` where the engine re-applies the filter, `exact` where the
//! source's answer is taken as is, `subset` for what sits under a NOT. Anything
//! unprovable is not pushed, which can cost speed but never changes an answer.
//!
//! Text is where dialects diverge. `ColFacts` records what the catalog says of a
//! column: byte-order collation, trailing-space padding (every SQL Server string,
//! PAD SPACE on mysql, `char(n)` on postgres), and wide characters whose byte
//! length is not their character count. Literals must be printable ASCII: a
//! non-ASCII byte sorts above ASCII in every encoding and binary collation, a
//! control character sorts below the pad space, and SQL Server reads a literal
//! without `N` in the column's code page. TRY_CAST never descends (no portable
//! null-on-failure cast), nor does a text-to-number cast of input not provably
//! numeric: sources disagree on `CAST('1000,00' AS DECIMAL)` (NULL on mysql, an
//! error on postgres and here). Builtins in `pushable` must also be engine builtins.
//!
//! `planWholeAgg` is the exception to advisory pushdown: the source's GROUP BY
//! result is what the pipeline emits, so every gate must prove the rendering
//! identical on postgres, mysql and sqlserver, and each aggregate is CAST to the
//! engine's planned output type. Null semantics match by construction: NULL keys
//! form one group, COUNT(col)/SUM/MIN/MAX skip nulls, and an ungrouped aggregate
//! over no rows yields one row. A top-N (`TopN`) is never authoritative: the
//! engine re-sorts and re-limits, so the source need only send a sufficient set,
//! which holds when every earlier filter runs at the source and the keys order
//! alike, nulls last.
//!
//! The test schema arrays are file-scope on purpose: as function locals,
//! `&.{…}` pointed at a stack temporary that dangled in release builds.
//!
//! This file plans what an aggregate or a map pipeline sends to a SQL source and
//! builds the WHERE it sends; `pushdown/translate.zig` renders expressions in each
//! dialect, `pushdown/topn.zig` sends `ORDER BY … LIMIT`, and `pushdown/hoist.zig`
//! moves filters through joins and selects toward the sources.

const std = @import("std");
const ast = @import("../lang/ast.zig");
const types = @import("../lang/types.zig");
const split = @import("../connect/split.zig");
pub const Dialect = @import("../db/sql.zig").Dialect;
const builtins = @import("../exec/builtins.zig");
const byList = @import("pushdown/testing_util.zig").byList;
const expectWholeAggRefused = @import("pushdown/testing_util.zig").expectWholeAggRefused;
const fieldItem = @import("pushdown/testing_util.zig").fieldItem;
const geFilter = @import("pushdown/testing_util.zig").geFilter;
const schema4 = @import("pushdown/testing_util.zig").schema4;
const selectStage = @import("pushdown/testing_util.zig").selectStage;
const testSchema = @import("pushdown/testing_util.zig").testSchema;
const topNOf = @import("pushdown/testing_util.zig").topNOf;

pub const Plan = struct {
    proj_select: ?[]const u8 = null,
    proj_schema: ?types.Schema = null,
    where_extra: ?[]const u8 = null,
};

pub fn inSchema(schema: types.Schema, name: []const u8) bool {
    for (schema.fields) |f| if (std.mem.eql(u8, f.name, name)) return true;
    return false;
}

/// Projection and predicate pushdown for `read … | prefix | aggregate ag`. A
/// `select` in the prefix disables projection (renames blur attribution); filters still go.
pub fn planAgg(arena: std.mem.Allocator, dialect: Dialect, src_schema: types.Schema, prefix: []const ast.Stage, ag: ast.Aggregate) !Plan {
    return planAggWith(arena, dialect, src_schema, prefix, ag, null);
}

pub fn planAggWith(arena: std.mem.Allocator, dialect: Dialect, src_schema: types.Schema, prefix: []const ast.Stage, ag: ast.Aggregate, facts: ?*const Facts) !Plan {
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
        const frag = (try translatePred(arena, st.node.filter, dialect, .{ .schema = src_schema, .check_fields = true, .facts = facts })) orelse continue;
        if (where.items.len > 0) try where.appendSlice(" AND ");
        try where.appendSlice(frag);
    }
    if (where.items.len > 0) plan.where_extra = try where.toOwnedSlice();

    return plan;
}

pub const WholeAgg = struct {
    sql: []const u8,
    where_sql: ?[]const u8 = null,
};

/// The aggregate as one grouped source query, or null to keep it engine-side.
/// Refuses AVG, non-int SUM, MIN/MAX over text or timestamps, and non-COUNT DISTINCT.
pub fn planWholeAgg(
    arena: std.mem.Allocator,
    dialect: Dialect,
    base_sql: []const u8,
    src_schema: types.Schema,
    prefix: []const ast.Stage,
    ag: ast.Aggregate,
    plan_schema: types.Schema,
) !?WholeAgg {
    var why: []const u8 = "";
    return planWholeAggWhy(arena, dialect, base_sql, src_schema, prefix, ag, plan_schema, null, &why);
}

/// `planWholeAgg` that also says which gate refused, in one line for the run log.
pub fn planWholeAggWhy(
    arena: std.mem.Allocator,
    dialect: Dialect,
    base_sql: []const u8,
    src_schema: types.Schema,
    prefix: []const ast.Stage,
    ag: ast.Aggregate,
    plan_schema: types.Schema,
    facts: ?*const Facts,
    why: *[]const u8,
) !?WholeAgg {
    if (ag.by.len == 0 and ag.aggs.len == 0) return refuse(why, "nothing to aggregate");
    if (plan_schema.fields.len != ag.by.len + ag.aggs.len) return refuse(why, "the planned output does not match the aggregate");

    var where = std.array_list.Managed(u8).init(arena);
    for (prefix) |st| {
        if (st.node != .filter) return refuse(why, "a stage other than WHERE sits between the read and the aggregate");
        const frag = (try translatePred(arena, st.node.filter, dialect, .{ .schema = src_schema, .check_fields = true, .facts = facts, .need = .exact })) orelse
            return refuse(why, try std.fmt.allocPrint(arena, "the WHERE predicate does not translate exactly to {s} SQL (a text comparison the column's collation decides differently, or an untranslatable piece)", .{@tagName(dialect)}));
        if (where.items.len > 0) try where.appendSlice(" AND ");
        try where.appendSlice(frag);
    }

    var sel = std.array_list.Managed(u8).init(arena);
    var keys = std.array_list.Managed(u8).init(arena);
    for (ag.by, 0..) |q, i| {
        if (q.parts.len != 1) return refuse(why, "a group key is not a bare source column");
        const col = q.parts[0];
        const idx = src_schema.indexOf(col) orelse
            return refuse(why, try std.fmt.allocPrint(arena, "group key `{s}` is not a source column", .{col}));
        const out = plan_schema.fields[i];
        if (!std.mem.eql(u8, out.name, col)) return refuse(why, try std.fmt.allocPrint(arena, "group key `{s}` is renamed by the aggregate", .{col}));
        if (out.ty.kind != src_schema.fields[idx].ty.kind) return refuse(why, try std.fmt.allocPrint(arena, "group key `{s}` changes type through the aggregate", .{col}));
        const qc = try split.quoteIdent(arena, dialect, col);
        if (keys.items.len > 0) try keys.appendSlice(", ");
        try keys.appendSlice(qc);
        if (sel.items.len > 0) try sel.appendSlice(", ");
        try sel.appendSlice(try std.fmt.allocPrint(arena, "{s} AS {s}", .{ qc, qc }));
    }

    for (ag.aggs, 0..) |item, i| {
        const out = plan_schema.fields[ag.by.len + i];
        const inner = (try aggExpr(arena, dialect, src_schema, item, out.ty)) orelse
            return refuse(why, try std.fmt.allocPrint(arena, "`{s}` is not pushed down for this argument and result type (see the pushdown rules in docs/language.md)", .{out.name}));
        const cast_to = (try dialect.castType(arena, out.ty)) orelse
            return refuse(why, try std.fmt.allocPrint(arena, "{s} has no cast for the result type of `{s}`", .{ @tagName(dialect), out.name }));
        if (sel.items.len > 0) try sel.appendSlice(", ");
        try sel.appendSlice(try std.fmt.allocPrint(arena, "CAST({s} AS {s}) AS {s}", .{
            inner, cast_to, try split.quoteIdent(arena, dialect, out.name),
        }));
    }

    var q = std.array_list.Managed(u8).init(arena);
    try q.appendSlice(try std.fmt.allocPrint(arena, "SELECT {s} FROM ({s}) _g", .{ sel.items, base_sql }));
    if (where.items.len > 0) try q.appendSlice(try std.fmt.allocPrint(arena, " WHERE {s}", .{where.items}));
    if (keys.items.len > 0) try q.appendSlice(try std.fmt.allocPrint(arena, " GROUP BY {s}", .{keys.items}));

    const where_sql: ?[]const u8 = if (where.items.len > 0) where.items else null;
    return WholeAgg{ .sql = try q.toOwnedSlice(), .where_sql = where_sql };
}

fn refuse(why: *[]const u8, reason: []const u8) ?WholeAgg {
    why.* = reason;
    return null;
}

/// One aggregate's SQL before the outer CAST. SQL Server takes COUNT_BIG and a
/// bigint SUM addend, as its 32-bit COUNT and int SUM raise on overflow.
fn aggExpr(arena: std.mem.Allocator, dialect: Dialect, src_schema: types.Schema, item: ast.AggItem, out_ty: types.Type) !?[]const u8 {
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
    const col = try split.quoteIdent(arena, dialect, name);

    switch (item.func) {
        .count => {
            if (item.distinct) return try std.fmt.allocPrint(arena, "{s}(DISTINCT {s})", .{ count_fn, col });
            return try std.fmt.allocPrint(arena, "{s}({s})", .{ count_fn, col });
        },
        .sum => {
            if (item.distinct) return null;
            if (src_kind != .int or out_ty.kind != .int) return null;
            if (dialect != .sqlserver) return try std.fmt.allocPrint(arena, "SUM({s})", .{col});
            return try std.fmt.allocPrint(arena, "SUM(CAST({s} AS BIGINT))", .{col});
        },
        .min, .max => {
            if (item.distinct) return null;
            if (out_ty.kind != src_kind) return null;
            switch (src_kind) {
                .int, .float, .date => {},
                .decimal => if (out_ty.precision == 0 or out_ty.scale > out_ty.precision) return null,
                else => return null,
            }
            return try std.fmt.allocPrint(arena, "{s}({s})", .{ if (item.func == .min) "MIN" else "MAX", col });
        },
        .avg, .median, .count_if, .bool_and, .bool_or, .bit_and, .bit_or, .bit_xor, .var_samp, .var_pop, .stddev_samp, .stddev_pop => return null,
    }
}

pub const MapPlan = struct {
    proj_select: ?[]const u8 = null,
    proj_schema: ?types.Schema = null,
    where_extra: ?[]const u8 = null,
    stages: ?[]const ast.Stage = null,
};

/// Pushdown for a map-only split read. Only filters before the first non-filter
/// stage become a WHERE; projection is a backward liveness pass from `out_cols`.
pub fn planMap(arena: std.mem.Allocator, dialect: Dialect, src_schema: types.Schema, middle: []const ast.Stage, out_cols: []const []const u8) !MapPlan {
    return planMapWith(arena, dialect, src_schema, middle, out_cols, null);
}

pub fn planMapWith(arena: std.mem.Allocator, dialect: Dialect, src_schema: types.Schema, middle: []const ast.Stage, out_cols: []const []const u8, facts: ?*const Facts) !MapPlan {
    var plan = MapPlan{};

    var nf: usize = middle.len;
    for (middle, 0..) |st, i| if (st.node != .filter) {
        nf = i;
        break;
    };
    const leading = middle[0..nf];

    var where = std.array_list.Managed(u8).init(arena);
    for (leading) |st| {
        const frag = (try translatePred(arena, st.node.filter, dialect, .{ .schema = src_schema, .check_fields = true, .facts = facts })) orelse continue;
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

/// A `SELECT` list and schema for the columns in `need`, in source order; null
/// when it would drop nothing (all or no columns).
fn buildProjection(arena: std.mem.Allocator, dialect: Dialect, src_schema: types.Schema, need: *std.StringHashMap(void)) !Projection {
    var sel = std.array_list.Managed(u8).init(arena);
    var fields = std.array_list.Managed(types.Schema.Field).init(arena);
    for (src_schema.fields) |f| {
        if (!need.contains(f.name)) continue;
        if (sel.items.len > 0) try sel.appendSlice(", ");
        try sel.appendSlice(try split.quoteIdent(arena, dialect, f.name));
        try fields.append(f);
    }
    if (fields.items.len > 0 and fields.items.len < src_schema.fields.len)
        return .{ .sel = try sel.toOwnedSlice(), .schema = .{ .fields = try fields.toOwnedSlice() } };
    return .{};
}

pub const Need = @import("pushdown/translate.zig").Need;
pub const ColFacts = @import("pushdown/translate.zig").ColFacts;
pub const Facts = @import("pushdown/translate.zig").Facts;
pub const Opts = @import("pushdown/translate.zig").Opts;
pub const translatePred = @import("pushdown/translate.zig").translatePred;
pub const wantsFacts = @import("pushdown/translate.zig").wantsFacts;
pub const translateExpr = @import("pushdown/translate.zig").translateExpr;
const pushable = @import("pushdown/translate.zig").pushable;
pub const TopN = @import("pushdown/topn.zig").TopN;
pub const classifyTopN = @import("pushdown/topn.zig").classifyTopN;
pub const ExplainedTopN = @import("pushdown/topn.zig").ExplainedTopN;
pub const explainTopN = @import("pushdown/topn.zig").explainTopN;
pub const planTopN = @import("pushdown/topn.zig").planTopN;
pub const renderTopN = @import("pushdown/topn.zig").renderTopN;
pub const collectQuals = @import("pushdown/hoist.zig").collectQuals;
const mkExpr = @import("pushdown/hoist.zig").mkExpr;
pub const hoistThroughJoins = @import("pushdown/hoist.zig").hoistThroughJoins;
pub const hoistFilters = @import("pushdown/hoist.zig").hoistFilters;
pub const pushIntoJoinSides = @import("pushdown/hoist.zig").pushIntoJoinSides;
pub const hoistThroughSelects = @import("pushdown/hoist.zig").hoistThroughSelects;

pub fn serialWhere(arena: std.mem.Allocator, dialect: Dialect, stages: []const ast.Stage) !?[]const u8 {
    return serialWhereWith(arena, dialect, stages, null, null);
}

pub fn serialWhereWith(arena: std.mem.Allocator, dialect: Dialect, stages: []const ast.Stage, facts: ?*const Facts, wants: ?*bool) !?[]const u8 {
    if (stages.len < 2 or stages[0].node != .read) return null;
    var parts = std.array_list.Managed([]const u8).init(arena);
    for (stages[1..]) |st| {
        if (st.node != .filter) break;
        if (try translatePred(arena, st.node.filter, dialect, .{ .facts = facts, .wants_facts = wants })) |sql_frag| {
            try parts.append(sql_frag);
        }
    }
    if (parts.items.len == 0) return null;
    return try std.mem.join(arena, " AND ", parts.items);
}

pub fn sqlStr(arena: std.mem.Allocator, s: []const u8) ![]const u8 {
    var out = std.array_list.Managed(u8).init(arena);
    try out.append('\'');
    for (s) |c| {
        if (c == '\'') try out.append('\'');
        try out.append(c);
    }
    try out.append('\'');
    return out.toOwnedSlice();
}

/// Every source column an expression references, by `parts[0]`; a lambda's
/// parameter is not one.
pub fn collectFields(e: *const ast.Expr, set: *std.StringHashMap(void)) !void {
    switch (e.*) {
        .field => |q| try set.put(q.parts[0], {}),
        .lambda => |l| try collectFields(l.body, set),
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

pub const test_schema_fields = [_]types.Schema.Field{
    .{ .name = "a", .ty = types.Type.init(.int) },
    .{ .name = "b", .ty = types.Type.init(.string) },
    .{ .name = "c", .ty = types.Type.init(.int) },
};

pub fn fld(arena: std.mem.Allocator, name: []const u8) !*ast.Expr {
    const q = try arena.create(ast.Expr);
    const parts = try arena.alloc([]const u8, 1);
    parts[0] = name;
    q.* = .{ .field = .{ .parts = parts } };
    return q;
}

pub fn bin(arena: std.mem.Allocator, op: ast.BinOp, l: *ast.Expr, r: *ast.Expr) !*ast.Expr {
    const e = try arena.create(ast.Expr);
    e.* = .{ .binary = .{ .op = op, .l = l, .r = r } };
    return e;
}

pub const schema4_fields = [_]types.Schema.Field{
    .{ .name = "a", .ty = types.Type.init(.int) },
    .{ .name = "b", .ty = types.Type.init(.string) },
    .{ .name = "c", .ty = types.Type.init(.int) },
    .{ .name = "d", .ty = types.Type.init(.int) },
};

const whole_schema_fields = [_]types.Schema.Field{
    .{ .name = "a", .ty = types.Type.init(.int) },
    .{ .name = "b", .ty = types.Type.init(.string) },
    .{ .name = "c", .ty = types.Type.init(.int) },
    .{ .name = "f", .ty = types.Type.init(.float) },
    .{ .name = "m", .ty = types.Type.decimal(12, 2) },
    .{ .name = "t", .ty = types.Type.init(.timestamp) },
};

pub fn wholeSchema() types.Schema {
    return .{ .fields = &whole_schema_fields };
}

pub const base_t = "SELECT * FROM t";

pub fn qual(arena: std.mem.Allocator, parts: []const []const u8) !ast.QualName {
    const p = try arena.alloc([]const u8, parts.len);
    for (parts, p) |src, *dst| dst.* = src;
    return .{ .parts = p };
}

pub fn qfld(arena: std.mem.Allocator, parts: []const []const u8) !*ast.Expr {
    const e = try arena.create(ast.Expr);
    e.* = .{ .field = try qual(arena, parts) };
    return e;
}

pub fn topNStagesOf(arena: std.mem.Allocator, sql: []const u8) ![]const ast.Stage {
    const parser = @import("../lang/sql_parser.zig");
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const src = try std.fmt.allocPrint(arena, "CREATE CONNECTION db TYPE postgres OPTIONS (host = 'h');\n{s};", .{sql});
    const prog = try parser.parseSource(arena, src, &pdiag);
    for (prog.stmts) |s| if (s == .output) return s.output.stages[0 .. s.output.stages.len - 1];
    return error.TestUnexpectedResult;
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

test "planWholeAgg: only COUNT, SUM, MIN and MAX descend — every other aggregate stays engine-side" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const plan_schema = types.Schema{ .fields = &.{
        .{ .name = "b", .ty = types.Type.init(.string) },
        .{ .name = "m", .ty = types.Type.init(.float).withNull(true) },
    } };
    inline for (@typeInfo(ast.AggFunc).@"enum".fields) |f| {
        const func: ast.AggFunc = @enumFromInt(f.value);
        switch (func) {
            .count, .sum, .min, .max => {},
            else => {
                const aggs = try a.alloc(ast.AggItem, 1);
                aggs[0] = .{ .name = "m", .func = func, .arg = try fld(a, "c") };
                const ag = ast.Aggregate{ .aggs = aggs, .by = try byList(a, &.{"b"}) };
                for ([_]Dialect{ .postgres, .mysql, .sqlserver }) |d|
                    try expectWholeAggRefused(a, d, ag, plan_schema, "`m` is not pushed down");
            },
        }
    }

    const cnt = try a.alloc(ast.AggItem, 1);
    cnt[0] = .{ .name = "m", .func = .count, .arg = try fld(a, "c") };
    const cnt_schema = types.Schema{ .fields = &.{
        .{ .name = "b", .ty = types.Type.init(.string) },
        .{ .name = "m", .ty = types.Type.init(.int) },
    } };
    for ([_]Dialect{ .postgres, .mysql, .sqlserver }) |d|
        try testing.expect((try planWholeAgg(a, d, base_t, wholeSchema(), &.{}, .{ .aggs = cnt, .by = try byList(a, &.{"b"}) }, cnt_schema)) != null);
}

test "planWholeAgg: a qualified group key falls back" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const aggs = try a.alloc(ast.AggItem, 1);
    aggs[0] = .{ .name = "n", .func = .count, .arg = null };

    const by = try a.alloc(ast.QualName, 1);
    by[0] = .{ .parts = &.{ "t", "b" } };
    const plan_schema = types.Schema{ .fields = &.{
        .{ .name = "b", .ty = types.Type.init(.string) },
        .{ .name = "n", .ty = types.Type.init(.int) },
    } };
    var why: []const u8 = "";
    try testing.expect((try planWholeAggWhy(a, .postgres, base_t, wholeSchema(), &.{}, .{ .aggs = aggs, .by = by }, plan_schema, null, &why)) == null);
    try testing.expectEqualStrings("a group key is not a bare source column", why);
}

test "planWholeAggWhy: a refusal names the gate that refused" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const aggs = try a.alloc(ast.AggItem, 1);
    aggs[0] = .{ .name = "n", .func = .count, .arg = null };
    const plan_schema = types.Schema{ .fields = &.{
        .{ .name = "b", .ty = types.Type.init(.string) },
        .{ .name = "n", .ty = types.Type.init(.int) },
    } };
    var why: []const u8 = "";

    const nowc = try a.create(ast.Expr);
    nowc.* = .{ .call = .{ .name = "now", .args = &.{} } };
    const bad = ast.Stage{ .node = .{ .filter = try bin(a, .gt, try fld(a, "t"), nowc) }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
    const ag = ast.Aggregate{ .aggs = aggs, .by = try byList(a, &.{"b"}) };
    try testing.expect((try planWholeAggWhy(a, .mysql, base_t, wholeSchema(), &.{bad}, ag, plan_schema, null, &why)) == null);
    try testing.expectEqualStrings("the WHERE predicate does not translate exactly to mysql SQL (a text comparison the column's collation decides differently, or an untranslatable piece)", why);

    const missing = ast.Aggregate{ .aggs = aggs, .by = try byList(a, &.{"zzz"}) };
    try testing.expect((try planWholeAggWhy(a, .postgres, base_t, wholeSchema(), &.{}, missing, plan_schema, null, &why)) == null);
    try testing.expectEqualStrings("group key `zzz` is not a source column", why);

    const avg = try a.alloc(ast.AggItem, 1);
    avg[0] = .{ .name = "av", .func = .avg, .arg = try fld(a, "a") };
    const avg_schema = types.Schema{ .fields = &.{.{ .name = "av", .ty = types.Type.init(.float) }} };
    try testing.expect((try planWholeAggWhy(a, .postgres, base_t, wholeSchema(), &.{}, .{ .aggs = avg, .by = &.{} }, avg_schema, null, &why)) == null);
    try testing.expect(std.mem.startsWith(u8, why, "`av` is not pushed down"));
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

    try testing.expect((try planWholeAgg(a, .mysql, base_t, wholeSchema(), &.{ok}, ag, plan_schema)) != null);
    try testing.expect((try planWholeAgg(a, .mysql, base_t, wholeSchema(), &.{ ok, bad }, ag, plan_schema)) == null);

    const items = try a.alloc(ast.SelectItem, 1);
    items[0] = .star;
    const sel = ast.Stage{ .node = .{ .select = items }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
    try testing.expect((try planWholeAgg(a, .mysql, base_t, wholeSchema(), &.{sel}, ag, plan_schema)) == null);
}

test "planWholeAgg: SUM only descends, without DISTINCT, for an int column into an int result" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const by = try byList(a, &.{"b"});
    const key = types.Schema.Field{ .name = "b", .ty = types.Type.init(.string) };
    const is = types.Schema{ .fields = &.{ key, .{ .name = "s", .ty = types.Type.init(.int).withNull(true) } } };

    const fa = try a.alloc(ast.AggItem, 1);
    fa[0] = .{ .name = "s", .func = .sum, .arg = try fld(a, "f") };
    const fs = types.Schema{ .fields = &.{ key, .{ .name = "s", .ty = types.Type.init(.float).withNull(true) } } };
    try expectWholeAggRefused(a, .postgres, .{ .aggs = fa, .by = by }, fs, "`s` is not pushed down");

    const da = try a.alloc(ast.AggItem, 1);
    da[0] = .{ .name = "s", .func = .sum, .arg = try fld(a, "m") };
    const ds = types.Schema{ .fields = &.{ key, .{ .name = "s", .ty = types.Type.decimal(12, 2).withNull(true) } } };
    try expectWholeAggRefused(a, .mysql, .{ .aggs = da, .by = by }, ds, "`s` is not pushed down");

    const dd = try a.alloc(ast.AggItem, 1);
    dd[0] = .{ .name = "s", .func = .sum, .arg = try fld(a, "c"), .distinct = true };
    try expectWholeAggRefused(a, .mysql, .{ .aggs = dd, .by = by }, is, "`s` is not pushed down");

    const ia = try a.alloc(ast.AggItem, 1);
    ia[0] = .{ .name = "s", .func = .sum, .arg = try fld(a, "c") };
    try testing.expect((try planWholeAgg(a, .mysql, base_t, wholeSchema(), &.{}, .{ .aggs = ia, .by = by }, is)) != null);
}

test "planWholeAgg: MIN/MAX falls back on collation- and timezone-sensitive types and on a decimal result without a precision" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const sa = try a.alloc(ast.AggItem, 1);
    sa[0] = .{ .name = "lo", .func = .min, .arg = try fld(a, "b") };
    const ss = types.Schema{ .fields = &.{.{ .name = "lo", .ty = types.Type.init(.string).withNull(true) }} };
    for ([_]Dialect{ .postgres, .mysql, .sqlserver }) |d|
        try expectWholeAggRefused(a, d, .{ .aggs = sa, .by = &.{} }, ss, "`lo` is not pushed down");

    const ta = try a.alloc(ast.AggItem, 1);
    ta[0] = .{ .name = "hi", .func = .max, .arg = try fld(a, "t") };
    const ts = types.Schema{ .fields = &.{.{ .name = "hi", .ty = types.Type.init(.timestamp).withNull(true) }} };
    try expectWholeAggRefused(a, .postgres, .{ .aggs = ta, .by = &.{} }, ts, "`hi` is not pushed down");

    const ma = try a.alloc(ast.AggItem, 1);
    ma[0] = .{ .name = "lo", .func = .min, .arg = try fld(a, "m") };
    const bad = types.Schema{ .fields = &.{.{ .name = "lo", .ty = types.Type.decimal(0, 0).withNull(true) }} };
    try expectWholeAggRefused(a, .mysql, .{ .aggs = ma, .by = &.{} }, bad, "`lo` is not pushed down");

    const good = types.Schema{ .fields = &.{.{ .name = "lo", .ty = types.Type.decimal(12, 2).withNull(true) }} };
    try testing.expect((try planWholeAgg(a, .mysql, base_t, wholeSchema(), &.{}, .{ .aggs = ma, .by = &.{} }, good)) != null);
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
    try expectWholeAggRefused(a, .postgres, .{ .aggs = aggs, .by = &.{} }, plan_schema, "`s` is not pushed down");

    const bare = try a.alloc(ast.AggItem, 1);
    bare[0] = .{ .name = "s", .func = .sum, .arg = try fld(a, "c") };
    try testing.expect((try planWholeAgg(a, .postgres, base_t, wholeSchema(), &.{}, .{ .aggs = bare, .by = &.{} }, plan_schema)) != null);
}

test "top-N: which pipelines carry a limit to the source, and with which keys" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var why: []const u8 = "";

    const plain = (try topNOf(a, "SELECT * FROM db.t LIMIT 50 OFFSET 10", &why)).?;
    try std.testing.expectEqual(@as(u64, 60), plain.rows);
    try std.testing.expectEqual(@as(usize, 0), plain.keys.len);

    const t = (try topNOf(a, "SELECT id AS k, name FROM db.t WHERE id > 5 ORDER BY k DESC, ts LIMIT 3", &why)).?;
    try std.testing.expectEqual(@as(u64, 3), t.rows);
    try std.testing.expectEqual(@as(usize, 2), t.keys.len);
    try std.testing.expectEqualStrings("id", t.keys[0].col);
    try std.testing.expect(t.keys[0].desc);
    try std.testing.expectEqualStrings("ts", t.keys[1].col);
    try std.testing.expectEqualStrings("order by id desc, ts limit 3", try t.describe(a));

    why = "";
    try std.testing.expect((try topNOf(a, "SELECT * FROM db.t ORDER BY id", &why)) == null);
    try std.testing.expect((try topNOf(a, "SELECT DISTINCT name FROM db.t LIMIT 3", &why)) == null);
    try std.testing.expect((try topNOf(a, "SELECT name, count(*) AS n FROM db.t GROUP BY name ORDER BY n LIMIT 3", &why)) == null);
    try std.testing.expectEqualStrings("", why);

    try std.testing.expect((try topNOf(a, "SELECT id * 2 AS d FROM db.t ORDER BY d LIMIT 3", &why)) == null);
    try std.testing.expect(std.mem.indexOf(u8, why, "not a plain source column") != null);
}

test {
    _ = @import("pushdown/hoist.zig");
    _ = @import("pushdown/topn.zig");
    _ = @import("pushdown/translate.zig");
    _ = @import("pushdown/testing_util.zig");
}
