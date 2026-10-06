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

const std = @import("std");
const ast = @import("../lang/ast.zig");
const types = @import("../lang/types.zig");
const split = @import("../connect/split.zig");
const Dialect = @import("../db/sql.zig").Dialect;
const builtins = @import("../exec/builtins.zig");

pub const Plan = struct {
    proj_select: ?[]const u8 = null,
    proj_schema: ?types.Schema = null,
    where_extra: ?[]const u8 = null,
};

fn inSchema(schema: types.Schema, name: []const u8) bool {
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

pub const Need = enum {
    superset,
    subset,
    exact,

    fn flip(n: Need) Need {
        return switch (n) {
            .superset => .subset,
            .subset => .superset,
            .exact => .exact,
        };
    }
};

pub const ColFacts = struct {
    text: bool,
    byte_order: bool = false,
    pads: bool = true,
    wide: bool = false,
};

pub const Facts = std.StringHashMap(ColFacts);

pub const Opts = struct {
    schema: types.Schema = .{ .fields = &.{} },
    check_fields: bool = false,
    facts: ?*const Facts = null,
    need: Need = .superset,
    wants_facts: ?*bool = null,
};

/// A predicate as SQL for `dialect` that keeps the rows `opts.need` asks for, or
/// null when no rendering is sure to.
pub fn translatePred(arena: std.mem.Allocator, e: *const ast.Expr, dialect: Dialect, opts: Opts) error{OutOfMemory}!?[]const u8 {
    const tx = Tx{ .arena = arena, .dialect = dialect, .o = opts };
    return tx.pred(e, opts.need);
}

/// Whether knowing the columns' collations could change how any filter in
/// `stages` descends at `need`, asked before paying a catalog round trip.
pub fn wantsFacts(arena: std.mem.Allocator, dialect: Dialect, stages: []const ast.Stage, schema: types.Schema, check_fields: bool, need: Need) !bool {
    var w = false;
    for (stages) |st| {
        if (st.node != .filter) continue;
        _ = try translatePred(arena, st.node.filter, dialect, .{ .schema = schema, .check_fields = check_fields, .need = need, .wants_facts = &w });
        if (w) return true;
    }
    return false;
}

/// `translatePred` at `need = superset` with no catalog facts, as EXPLAIN and the
/// tests use.
pub fn translateExpr(arena: std.mem.Allocator, e: *const ast.Expr, dialect: Dialect, schema: types.Schema, check_fields: bool) error{OutOfMemory}!?[]const u8 {
    return translatePred(arena, e, dialect, .{ .schema = schema, .check_fields = check_fields });
}

fn numericKind(k: types.TypeKind) bool {
    return k == .int or k == .float or k == .decimal;
}

/// Whether the operand is certainly a number already: a literal, or a column the
/// schema at hand says is numeric. A nested numeric cast is checked on its own.
fn provablyNumeric(e: *const ast.Expr, schema: types.Schema) bool {
    return switch (e.*) {
        .int_lit, .float_lit => true,
        .str_lit => |s| plainNumber(s),
        .field => |q| blk: {
            if (schema.fields.len == 0) break :blk false;
            const idx = schema.indexOf(q.last()) orelse break :blk false;
            break :blk numericKind(schema.fields[idx].ty.kind);
        },
        else => false,
    };
}

fn plainNumber(s: []const u8) bool {
    var i: usize = 0;
    if (i < s.len and s[i] == '-') i += 1;
    var digits: usize = 0;
    var dot = false;
    while (i < s.len) : (i += 1) switch (s[i]) {
        '0'...'9' => digits += 1,
        '.' => {
            if (dot or digits == 0 or i + 1 >= s.len) return false;
            dot = true;
        },
        else => return false,
    };
    return digits > 0;
}

fn asciiPrintable(s: []const u8) bool {
    for (s) |c| if (c < 0x20 or c > 0x7e) return false;
    return true;
}

const text_fns = [_][]const u8{ "lower", "upper", "trim", "substr", "replace", "concat", "coalesce", "left", "right", "repeat", "reverse" };

fn isTextFn(name: []const u8) bool {
    for (text_fns) |n| if (std.mem.eql(u8, n, name)) return true;
    return false;
}

fn isLikeFn(name: []const u8) bool {
    return std.mem.eql(u8, name, "like") or std.mem.eql(u8, name, "starts_with") or
        std.mem.eql(u8, name, "ends_with") or std.mem.eql(u8, name, "contains");
}

const Tx = struct {
    arena: std.mem.Allocator,
    dialect: Dialect,
    o: Opts,

    const Err = error{OutOfMemory};

    const Kind = union(enum) {
        other,
        unknown,
        text: struct { facts: ?ColFacts, fn_: bool = false },
    };

    fn fmt(self: Tx, comptime f: []const u8, args: anytype) Err![]const u8 {
        return std.fmt.allocPrint(self.arena, f, args);
    }

    fn want(self: Tx) void {
        if (self.o.wants_facts) |w| w.* = true;
    }

    fn factsOf(self: Tx, name: []const u8) ?ColFacts {
        const f = self.o.facts orelse return null;
        if (f.get(name)) |c| return c;
        var it = f.iterator();
        while (it.next()) |kv| if (std.ascii.eqlIgnoreCase(kv.key_ptr.*, name)) return kv.value_ptr.*;
        return null;
    }

    fn kind(self: Tx, e: *const ast.Expr) Kind {
        return switch (e.*) {
            .str_lit => .{ .text = .{ .facts = null } },
            .int_lit, .float_lit, .bool_lit, .null_lit => .other,
            .field => |q| blk: {
                if (q.parts.len != 1) break :blk .unknown;
                if (self.factsOf(q.parts[0])) |f| break :blk if (f.text) Kind{ .text = .{ .facts = f } } else .other;
                if (self.o.schema.indexOf(q.parts[0])) |i| {
                    const k = self.o.schema.fields[i].ty.kind;
                    break :blk if (k == .string) Kind{ .text = .{ .facts = null } } else .other;
                }
                break :blk .unknown;
            },
            .cast => |c| if (c.ty.kind == .string) Kind{ .text = .{ .facts = null } } else .other,
            .call => |c| blk: {
                if (!isTextFn(c.name)) break :blk .other;
                break :blk .{ .text = .{ .facts = self.textFnFacts(c), .fn_ = true } };
            },
            else => .unknown,
        };
    }

    fn textFnFacts(self: Tx, c: ast.Expr.Call) ?ColFacts {
        for (c.args) |a| if (a.* == .field and a.field.parts.len == 1) return self.factsOf(a.field.parts[0]);
        return null;
    }

    fn pred(self: Tx, e: *const ast.Expr, need: Need) Err!?[]const u8 {
        switch (e.*) {
            .binary => |b| switch (b.op) {
                .@"and", .@"or" => {
                    const l = try self.pred(b.l, need);
                    const r = try self.pred(b.r, need);
                    if (b.op == .@"and" and need == .superset) {
                        if (l == null) return r;
                        if (r == null) return l;
                    }
                    return try self.fmt("({s} {s} {s})", .{ l orelse return null, if (b.op == .@"and") "AND" else "OR", r orelse return null });
                },
                .eq, .ne, .lt, .le, .gt, .ge => return self.compare(b.op, b.l, b.r, need),
                else => return null,
            },
            .unary => |u| {
                if (u.op != .not) return null;
                const inner = (try self.pred(u.e, need.flip())) orelse return null;
                return try self.fmt("(NOT ({s}))", .{inner});
            },
            .is_null => |n| {
                const v = (try self.value(n.e)) orelse return null;
                if (n.kind == .is_null)
                    return try self.fmt("({s} IS {s}NULL)", .{ v, if (n.negated) "NOT " else "" });
                const blank = (try self.textCompare(.eq, n.e, "", if (n.negated) need.flip() else need)) orelse return null;
                const t = try self.fmt("({s} IS NULL OR {s})", .{ v, blank });
                return if (n.negated) try self.fmt("(NOT {s})", .{t}) else t;
            },
            .call => |c| {
                if (isLikeFn(c.name)) return self.likePred(c, need);
                return self.value(e);
            },
            .cond => |c| {
                const cnd = (try self.pred(c.cond, .exact)) orelse return null;
                const t = (try self.value(c.then)) orelse return null;
                const f = (try self.value(c.els)) orelse return null;
                return try self.fmt("(CASE WHEN {s} THEN {s} ELSE {s} END)", .{ cnd, t, f });
            },
            .match => |m| return self.match(m),
            else => return self.value(e),
        }
    }

    /// TRY_CAST and text-to-number casts of unprovable input stay engine-side.
    fn value(self: Tx, e: *const ast.Expr) Err!?[]const u8 {
        switch (e.*) {
            .bool_lit => |b| return try self.arena.dupe(u8, if (b) "(1=1)" else "(1=0)"),
            .int_lit => |v| return try self.fmt("{d}", .{v}),
            .float_lit => |v| return try self.fmt("{d}", .{v}),
            .null_lit => return try self.arena.dupe(u8, "NULL"),
            .str_lit => |s| {
                if (self.dialect == .sqlserver and !asciiPrintable(s)) return null;
                if (self.dialect.mysqlWire() and std.mem.indexOfScalar(u8, s, '\\') != null) return null;
                return try sqlStr(self.arena, s);
            },
            .field => |q| {
                if (q.parts.len != 1) return null;
                if (self.o.check_fields and !inSchema(self.o.schema, q.parts[0])) return null;
                return try split.quoteIdent(self.arena, self.dialect, q.parts[0]);
            },
            .cast => |c| {
                if (c.safe) return null;
                if (numericKind(c.ty.kind) and !provablyNumeric(c.e, self.o.schema)) return null;
                const inner = (try self.value(c.e)) orelse return null;
                const ty = (try self.dialect.castType(self.arena, c.ty)) orelse return null;
                return try self.fmt("CAST({s} AS {s})", .{ inner, ty });
            },
            .call => |c| return self.call(c),
            .cond => |c| {
                const cnd = (try self.pred(c.cond, .exact)) orelse return null;
                const t = (try self.value(c.then)) orelse return null;
                const f = (try self.value(c.els)) orelse return null;
                return try self.fmt("(CASE WHEN {s} THEN {s} ELSE {s} END)", .{ cnd, t, f });
            },
            .match => |m| return self.match(m),
            .unary, .binary, .is_null => return self.pred(e, .exact),
            else => return null,
        }
    }

    /// `substr` descends only from position 1 (a UTF-8 SQL Server varchar counts bytes),
    /// `replace` only under a byte-order collation (others replace case-insensitively).
    fn call(self: Tx, c: ast.Expr.Call) Err!?[]const u8 {
        if (isLikeFn(c.name)) return self.likePred(c, .exact);
        const p = lookupPushable(c.name) orelse return null;
        if (c.args.len < p.min_args or c.args.len > p.max_args) return null;
        if (std.mem.eql(u8, c.name, "substr")) {
            if (c.args[1].* != .int_lit or c.args[1].int_lit != 1) return null;
        }
        if (std.mem.eql(u8, c.name, "replace")) {
            const f = self.textFnFacts(c) orelse {
                self.want();
                return null;
            };
            if (!f.byte_order) return null;
        }
        const args = try self.arena.alloc([]const u8, c.args.len);
        for (c.args, args) |a, *out| out.* = (try self.value(a)) orelse return null;
        return p.render(self.arena, p, c, args, self.dialect);
    }

    /// `CASE x WHEN p …` compares by equality, which is exact only off text; the guard
    /// form's conditions must hold exactly.
    fn match(self: Tx, m: ast.Match) Err!?[]const u8 {
        var out = std.array_list.Managed(u8).init(self.arena);
        const w = out.writer();
        if (m.subject) |subj| {
            const sk = self.kind(subj);
            if (sk == .text) return null;
            if (sk == .unknown) for (m.arms) |arm| for (arm.pats) |p| switch (p.*) {
                .int_lit, .float_lit => {},
                else => return null,
            };
            const s = (try self.value(subj)) orelse return null;
            try w.print("(CASE {s}", .{s});
        } else {
            try w.writeAll("(CASE");
        }
        for (m.arms) |arm| {
            const v = (try self.value(arm.value)) orelse return null;
            if (arm.is_default) {
                try w.print(" ELSE {s}", .{v});
            } else if (m.subject != null) {
                for (arm.pats) |p| {
                    if (self.kind(p) == .text) return null;
                    const ps = (try self.value(p)) orelse return null;
                    try w.print(" WHEN {s} THEN {s}", .{ ps, v });
                }
            } else {
                const g = (try self.pred(arm.guard orelse return null, .exact)) orelse return null;
                try w.print(" WHEN {s} THEN {s}", .{ g, v });
            }
        }
        try w.writeAll(" END)");
        return try out.toOwnedSlice();
    }

    fn mirror(op: ast.BinOp) ast.BinOp {
        return switch (op) {
            .lt => .gt,
            .le => .ge,
            .gt => .lt,
            .ge => .le,
            else => op,
        };
    }

    fn opSql(op: ast.BinOp) []const u8 {
        return switch (op) {
            .eq => "=",
            .ne => "<>",
            .lt => "<",
            .le => "<=",
            .gt => ">",
            .ge => ">=",
            else => unreachable,
        };
    }

    fn isLit(e: *const ast.Expr) bool {
        return switch (e.*) {
            .str_lit, .int_lit, .float_lit, .bool_lit, .null_lit => true,
            else => false,
        };
    }

    /// Numbers, dates and times compare alike everywhere. On text, equality only widens
    /// under a folding collation, and order is shared only by byte-ordered unpadded columns.
    fn compare(self: Tx, op_in: ast.BinOp, l_in: *const ast.Expr, r_in: *const ast.Expr, need: Need) Err!?[]const u8 {
        var l = l_in;
        var r = r_in;
        var op = op_in;
        if (isLit(l) and !isLit(r)) {
            l = r_in;
            r = l_in;
            op = mirror(op_in);
        }
        const lk = self.kind(l);
        const rk = self.kind(r);
        if (r.* == .str_lit and !isLit(l)) return self.textCompare(op, l, r.str_lit, need);
        const texty = lk == .text or rk == .text;
        const unknown = lk == .unknown or rk == .unknown;
        if (!texty and !(unknown and !isLit(r))) {
            const ls = (try self.value(l)) orelse return null;
            const rs = (try self.value(r)) orelse return null;
            return try self.fmt("({s} {s} {s})", .{ ls, opSql(op), rs });
        }
        const lf: ?ColFacts = if (lk == .text) lk.text.facts else null;
        const rf: ?ColFacts = if (rk == .text) rk.text.facts else null;
        const settled = (op == .eq and need == .superset) or (op == .ne and need == .subset);
        if ((lf == null or rf == null) and !settled) self.want();
        const bytes = if (lf) |a| (if (rf) |b| a.byte_order and b.byte_order and !a.pads and !b.pads and !(lk == .text and lk.text.fn_) and !(rk == .text and rk.text.fn_) else false) else false;
        const ok = bytes or switch (op) {
            .eq => need == .superset,
            .ne => need == .subset,
            else => false,
        };
        if (!ok) return null;
        const ls = (try self.value(l)) orelse return null;
        const rs = (try self.value(r)) orelse return null;
        return try self.fmt("({s} {s} {s})", .{ ls, opSql(op), rs });
    }

    /// `col op 'L'` over text. Under padding the shorter side compares as if
    /// space-filled, so ranges widen to a prefix test.
    fn textCompare(self: Tx, op: ast.BinOp, col: *const ast.Expr, lit: []const u8, need: Need) Err!?[]const u8 {
        const k = self.kind(col);
        if (k == .other) {
            const cs = (try self.value(col)) orelse return null;
            const ls = (try self.value(&.{ .str_lit = lit })) orelse return null;
            return try self.fmt("({s} {s} {s})", .{ cs, opSql(op), ls });
        }
        const known: ?ColFacts = if (k == .text) k.text.facts else null;
        const f = known orelse ColFacts{ .text = true };
        const is_fn = k == .text and k.text.fn_;
        const settled = (op == .eq and need == .superset) or (op == .ne and need == .subset);
        if (known == null and !is_fn and !settled) self.want();
        const cs = (try self.value(col)) orelse return null;
        const ls = (try self.value(&.{ .str_lit = lit })) orelse return null;
        const plain = try self.fmt("({s} {s} {s})", .{ cs, opSql(op), ls });
        const ascii = asciiPrintable(lit);

        if (is_fn) {
            if (!ascii) return null;
            return switch (op) {
                .eq => if (need == .superset) plain else null,
                .ne => if (need == .subset) plain else null,
                else => null,
            };
        }
        if (f.byte_order and !f.pads and ascii) return plain;
        switch (op) {
            .eq => {
                if (need == .superset) return plain;
                return self.padExact(.eq, cs, ls, lit, f);
            },
            .ne => {
                if (need == .subset) return plain;
                return self.padExact(.ne, cs, ls, lit, f);
            },
            .lt, .le, .gt, .ge => {
                if (!f.byte_order or !ascii) return null;
                if (!f.pads) return plain;
                if (need == .superset) {
                    if (op == .lt or op == .le) return try self.fmt("({s} <= {s})", .{ cs, ls });
                    const pre = (try self.prefixLike(lit)) orelse return null;
                    return try self.fmt("({s} >= {s} OR {s} LIKE {s})", .{ cs, ls, cs, pre });
                }
                return self.padExact(op, cs, ls, lit, f);
            },
            else => return null,
        }
    }

    /// Exact text comparison under a binary, padding collation, via byte length;
    /// printable ASCII literals with no trailing space only.
    fn padExact(self: Tx, op: ast.BinOp, cs: []const u8, ls: []const u8, lit: []const u8, f: ColFacts) Err!?[]const u8 {
        if (!f.byte_order or f.wide or !asciiPrintable(lit)) return null;
        if (lit.len > 0 and lit[lit.len - 1] == ' ') return null;
        const len_fn: []const u8 = switch (self.dialect) {
            .sqlserver => "DATALENGTH",
            .mysql, .starrocks, .doris => "LENGTH",
            .postgres => return null,
        };
        const n = lit.len;
        const eq = try self.fmt("({s} = {s} AND {s}({s}) = {d})", .{ cs, ls, len_fn, cs, n });
        if (op == .eq) return eq;
        if (op == .ne) return try self.fmt("(NOT {s})", .{eq});
        const pre = (try self.prefixLike(lit)) orelse return null;
        const ge = try self.fmt("({s} >= {s} OR {s} LIKE {s})", .{ cs, ls, cs, pre });
        const gt = try self.fmt("({s} > {s} OR ({s} LIKE {s} AND {s}({s}) > {d}))", .{ cs, ls, cs, pre, len_fn, cs, n });
        return switch (op) {
            .ge => ge,
            .gt => gt,
            .lt => try self.fmt("(NOT {s})", .{ge}),
            .le => try self.fmt("(NOT {s})", .{gt}),
            else => null,
        };
    }

    fn prefixLike(self: Tx, lit: []const u8) Err!?[]const u8 {
        var out = std.array_list.Managed(u8).init(self.arena);
        for (lit) |c| switch (self.dialect) {
            .sqlserver => switch (c) {
                '%', '_', '[' => try out.writer().print("[{c}]", .{c}),
                else => try out.append(c),
            },
            else => switch (c) {
                '%', '_', '\\' => return null,
                else => try out.append(c),
            },
        };
        try out.append('%');
        return try sqlStr(self.arena, out.items);
    }

    /// `like`/`starts_with`/`ends_with`/`contains` on a literal pattern. The source's `_`
    /// is a character, the engine's a byte; SQL Server reads `[` as a class, mysql `\` as an escape.
    fn likePred(self: Tx, c: ast.Expr.Call, need: Need) Err!?[]const u8 {
        if (c.args.len != 2 or c.args[1].* != .str_lit) return null;
        const raw = c.args[1].str_lit;
        if (!asciiPrintable(raw) and self.dialect == .sqlserver) return null;
        for (raw) |ch| {
            if (ch == '_' or ch == '\\') return null;
            if (!std.mem.eql(u8, c.name, "like") and ch == '%') return null;
        }
        var body = std.array_list.Managed(u8).init(self.arena);
        for (raw) |ch| {
            if (self.dialect == .sqlserver and ch == '[') try body.appendSlice("[[]") else try body.append(ch);
        }
        const pat = if (std.mem.eql(u8, c.name, "starts_with"))
            try self.fmt("{s}%", .{body.items})
        else if (std.mem.eql(u8, c.name, "ends_with"))
            try self.fmt("%{s}", .{body.items})
        else if (std.mem.eql(u8, c.name, "contains"))
            try self.fmt("%{s}%", .{body.items})
        else
            body.items;
        const k = self.kind(c.args[0]);
        if (k != .text and k != .unknown) return null;
        const known: ?ColFacts = if (k == .text) k.text.facts else null;
        if (need != .superset) {
            const f = known orelse {
                self.want();
                return null;
            };
            if (!f.byte_order or f.pads or (k == .text and k.text.fn_)) return null;
        }
        const target = (try self.value(c.args[0])) orelse return null;
        return try self.fmt("({s} LIKE {s})", .{ target, try sqlStr(self.arena, pat) });
    }
};

const Pushable = struct {
    name: []const u8,
    sql: []const u8 = "",
    min_args: usize,
    max_args: usize,
    render: *const fn (std.mem.Allocator, *const Pushable, ast.Expr.Call, []const []const u8, Dialect) error{OutOfMemory}!?[]const u8,
};

const variadic = std.math.maxInt(usize);

const pushable = [_]Pushable{
    .{ .name = "lower", .sql = "LOWER", .min_args = 1, .max_args = 1, .render = render.plain },
    .{ .name = "upper", .sql = "UPPER", .min_args = 1, .max_args = 1, .render = render.plain },
    .{ .name = "trim", .min_args = 1, .max_args = 1, .render = render.trim },
    .{ .name = "substr", .sql = "SUBSTRING", .min_args = 3, .max_args = 3, .render = render.plain },
    .{ .name = "replace", .sql = "REPLACE", .min_args = 3, .max_args = 3, .render = render.plain },
    .{ .name = "concat", .sql = "CONCAT", .min_args = 2, .max_args = variadic, .render = render.plain },
    .{ .name = "coalesce", .sql = "COALESCE", .min_args = 2, .max_args = variadic, .render = render.plain },
    .{ .name = "like", .min_args = 2, .max_args = 2, .render = render.none },
    .{ .name = "starts_with", .min_args = 2, .max_args = 2, .render = render.none },
    .{ .name = "ends_with", .min_args = 2, .max_args = 2, .render = render.none },
    .{ .name = "contains", .min_args = 2, .max_args = 2, .render = render.none },
    .{ .name = "abs", .sql = "ABS", .min_args = 1, .max_args = 1, .render = render.plain },
    .{ .name = "floor", .sql = "FLOOR", .min_args = 1, .max_args = 1, .render = render.plain },
    .{ .name = "sqrt", .sql = "SQRT", .min_args = 1, .max_args = 1, .render = render.plain },
    .{ .name = "sign", .sql = "SIGN", .min_args = 1, .max_args = 1, .render = render.plain },
    .{ .name = "reverse", .sql = "REVERSE", .min_args = 1, .max_args = 1, .render = render.plain },
    .{ .name = "power", .sql = "POWER", .min_args = 2, .max_args = 2, .render = render.plain },
    .{ .name = "nullif", .sql = "NULLIF", .min_args = 2, .max_args = 2, .render = render.plain },
    .{ .name = "ceil", .min_args = 1, .max_args = 1, .render = render.ceil },
    .{ .name = "mod", .min_args = 2, .max_args = 2, .render = render.mod },
    .{ .name = "left", .min_args = 2, .max_args = 2, .render = render.counted },
    .{ .name = "right", .min_args = 2, .max_args = 2, .render = render.counted },
    .{ .name = "repeat", .min_args = 2, .max_args = 2, .render = render.counted },
};

fn lookupPushable(name: []const u8) ?*const Pushable {
    const map = comptime blk: {
        var kvs: [pushable.len]struct { []const u8, usize } = undefined;
        for (pushable, 0..) |p, i| kvs[i] = .{ p.name, i };
        break :blk std.StaticStringMap(usize).initComptime(kvs);
    };
    const i = map.get(name) orelse return null;
    return &pushable[i];
}

const render = struct {
    fn none(arena: std.mem.Allocator, p: *const Pushable, c: ast.Expr.Call, args: []const []const u8, dialect: Dialect) error{OutOfMemory}!?[]const u8 {
        _ = .{ arena, p, c, args, dialect };
        return null;
    }

    fn plain(arena: std.mem.Allocator, p: *const Pushable, c: ast.Expr.Call, args: []const []const u8, dialect: Dialect) error{OutOfMemory}!?[]const u8 {
        _ = c;
        _ = dialect;
        const joined = try std.mem.join(arena, ", ", args);
        return try std.fmt.allocPrint(arena, "{s}({s})", .{ p.sql, joined });
    }

    fn trim(arena: std.mem.Allocator, p: *const Pushable, c: ast.Expr.Call, args: []const []const u8, dialect: Dialect) error{OutOfMemory}!?[]const u8 {
        _ = p;
        _ = c;
        return switch (dialect) {
            .sqlserver => try std.fmt.allocPrint(arena, "LTRIM(RTRIM({s}))", .{args[0]}),
            else => try std.fmt.allocPrint(arena, "TRIM({s})", .{args[0]}),
        };
    }

    fn ceil(arena: std.mem.Allocator, p: *const Pushable, c: ast.Expr.Call, args: []const []const u8, dialect: Dialect) error{OutOfMemory}!?[]const u8 {
        _ = p;
        _ = c;
        const f = switch (dialect) {
            .sqlserver => "CEILING",
            else => "CEIL",
        };
        return try std.fmt.allocPrint(arena, "{s}({s})", .{ f, args[0] });
    }

    /// SQL Server has only `%`; both spellings take the dividend's sign everywhere,
    /// matching the engine.
    fn mod(arena: std.mem.Allocator, p: *const Pushable, c: ast.Expr.Call, args: []const []const u8, dialect: Dialect) error{OutOfMemory}!?[]const u8 {
        _ = p;
        _ = c;
        return switch (dialect) {
            .sqlserver => try std.fmt.allocPrint(arena, "({s} % {s})", .{ args[0], args[1] }),
            else => try std.fmt.allocPrint(arena, "MOD({s}, {s})", .{ args[0], args[1] }),
        };
    }

    /// Only a literal count >= 0 is pushed: negative counts diverge across dialects.
    fn counted(arena: std.mem.Allocator, p: *const Pushable, c: ast.Expr.Call, args: []const []const u8, dialect: Dialect) error{OutOfMemory}!?[]const u8 {
        if (c.args[1].* != .int_lit or c.args[1].int_lit < 0) return null;
        const f: []const u8 = if (std.mem.eql(u8, p.name, "left"))
            "LEFT"
        else if (std.mem.eql(u8, p.name, "right"))
            "RIGHT"
        else if (dialect == .sqlserver)
            "REPLICATE"
        else
            "REPEAT";
        return try std.fmt.allocPrint(arena, "{s}({s}, {s})", .{ f, args[0], args[1] });
    }
};

pub const TopN = struct {
    rows: u64,
    keys: []const Key = &.{},

    pub const Key = struct { col: []const u8, desc: bool };

    pub fn describe(self: TopN, arena: std.mem.Allocator) ![]const u8 {
        var out = std.array_list.Managed(u8).init(arena);
        for (self.keys, 0..) |k, i| {
            try out.appendSlice(if (i == 0) "order by " else ", ");
            try out.appendSlice(k.col);
            if (k.desc) try out.appendSlice(" desc");
        }
        if (out.items.len > 0) try out.append(' ');
        try out.writer().print("limit {d}", .{self.rows});
        return out.toOwnedSlice();
    }
};

/// The top-N a pipeline asks of its read (`read | filter* | select* | [sort] | limit`),
/// sort keys traced to source columns. Null, with `why` when there is a limit, otherwise.
pub fn classifyTopN(arena: std.mem.Allocator, stages: []const ast.Stage, why: *[]const u8) !?TopN {
    if (stages.len < 2 or stages[0].node != .read) return null;
    const mid = stages[1..];
    const at = for (mid, 0..) |st, i| {
        if (st.node == .limit) break i;
    } else return null;
    const lim = mid[at].node.limit;
    var sort: ?ast.Sort = null;
    var selects_end: usize = at;
    if (at > 0 and mid[at - 1].node == .sort) {
        sort = mid[at - 1].node.sort;
        selects_end = at - 1;
    }
    var i: usize = 0;
    while (i < selects_end and mid[i].node == .filter) i += 1;
    const sel_start = i;
    while (i < selects_end) : (i += 1) switch (mid[i].node) {
        .select => {},
        .filter => {
            why.* = "a WHERE follows the projection, so it filters computed columns";
            return null;
        },
        else => return null,
    };
    const rows = std.math.add(u64, lim.count, lim.offset) catch {
        why.* = "LIMIT plus OFFSET overflows";
        return null;
    };
    if (rows > std.math.maxInt(i64)) {
        why.* = "LIMIT plus OFFSET is past what a SQL LIMIT takes";
        return null;
    }
    const s = sort orelse return .{ .rows = rows };
    const keys = try arena.alloc(TopN.Key, s.keys.len);
    for (s.keys, keys) |k, *out| {
        const col = sourceName(mid[sel_start..selects_end], k.field.last()) orelse {
            why.* = try std.fmt.allocPrint(arena, "ORDER BY `{s}` is not a plain source column", .{k.field.last()});
            return null;
        };
        out.* = .{ .col = col, .desc = k.desc };
    }
    return .{ .rows = rows, .keys = keys };
}

/// The source column an output `name` carries through `selects` unchanged or
/// renamed, or null when some select computes or drops it.
fn sourceName(selects: []const ast.Stage, name_in: []const u8) ?[]const u8 {
    var name = name_in;
    var i = selects.len;
    while (i > 0) {
        i -= 1;
        name = selectSource(selects[i].node.select, name) orelse return null;
    }
    return name;
}

fn selectSource(items: []const ast.SelectItem, name: []const u8) ?[]const u8 {
    for (items) |item| switch (item) {
        .field => |q| if (std.mem.eql(u8, q.last(), name)) return q.last(),
        .computed => |c| if (std.mem.eql(u8, c.name, name)) return switch (c.expr.*) {
            .field => |q| q.last(),
            else => null,
        },
        else => {},
    };
    for (items) |item| switch (item) {
        .star => return name,
        .star_except => |ex| {
            for (ex) |x| if (std.mem.eql(u8, x, name)) return null;
            return name;
        },
        .star_rename => |rs| {
            for (rs) |r| if (std.mem.eql(u8, r.to, name)) return r.from;
            for (rs) |r| if (std.mem.eql(u8, r.from, name)) return null;
            return name;
        },
        else => {},
    };
    return null;
}

/// Whether every filter before the limit runs exactly at the source; otherwise
/// the pushed cap would count rows the engine then drops.
fn filtersAllTranslate(arena: std.mem.Allocator, dialect: Dialect, stages: []const ast.Stage, schema: types.Schema, check_fields: bool, facts: ?*const Facts) !bool {
    for (stages[1..]) |st| {
        if (st.node != .filter) break;
        if ((try translatePred(arena, st.node.filter, dialect, .{ .schema = schema, .check_fields = check_fields, .facts = facts, .need = .exact })) == null) return false;
    }
    return true;
}

/// Key kinds every dialect orders exactly as the engine: numbers (NaN greatest)
/// and temporals. Strings follow collations, so they are refused, as in MIN/MAX.
fn orderedAlike(kind: types.TypeKind) bool {
    return switch (kind) {
        .int, .float, .decimal, .date, .time, .timestamp => true,
        else => false,
    };
}

pub const ExplainedTopN = struct { text: []const u8, rows: u64, sorted: bool };

/// EXPLAIN's view of the top-N without a connection; a sort key's type is still
/// checked when the statement runs.
pub fn explainTopN(arena: std.mem.Allocator, dialect: Dialect, stages: []const ast.Stage) !?ExplainedTopN {
    var why: []const u8 = "";
    const t = (try classifyTopN(arena, stages, &why)) orelse return null;
    if (!try filtersAllTranslate(arena, dialect, stages, .{ .fields = &.{} }, false, null)) return null;
    const d = try t.describe(arena);
    return .{
        .text = if (t.keys.len == 0) d else try std.fmt.allocPrint(arena, "{s} (if the keys are numeric or temporal)", .{d}),
        .rows = t.rows,
        .sorted = t.keys.len > 0,
    };
}

pub fn planTopN(arena: std.mem.Allocator, dialect: Dialect, base_sql: []const u8, src_schema: types.Schema, stages: []const ast.Stage, t: TopN, facts: ?*const Facts, why: *[]const u8) !?[]const u8 {
    if (!try filtersAllTranslate(arena, dialect, stages, src_schema, true, facts)) {
        why.* = try std.fmt.allocPrint(arena, "a WHERE predicate does not translate exactly to {s} SQL (a text comparison the column's collation decides differently, or an untranslatable piece)", .{@tagName(dialect)});
        return null;
    }
    for (t.keys) |k| {
        const idx = src_schema.indexOf(k.col) orelse {
            why.* = try std.fmt.allocPrint(arena, "ORDER BY `{s}` is not a source column", .{k.col});
            return null;
        };
        const kind = src_schema.fields[idx].ty.kind;
        if (!orderedAlike(kind)) {
            why.* = try std.fmt.allocPrint(arena, "ORDER BY `{s}` is a {s}, which {s} orders by its collation rather than byte by byte", .{ k.col, @tagName(kind), @tagName(dialect) });
            return null;
        }
    }
    return try renderTopN(arena, dialect, base_sql, t);
}

/// `base_sql` ordered nulls-last and capped: `NULLS LAST` on postgres, a leading
/// `k IS NULL` key on mysql, a `CASE` key and `TOP` on sqlserver.
pub fn renderTopN(arena: std.mem.Allocator, dialect: Dialect, base_sql: []const u8, t: TopN) ![]const u8 {
    var ob = std.array_list.Managed(u8).init(arena);
    for (t.keys, 0..) |k, i| {
        if (i > 0) try ob.appendSlice(", ");
        const q = try split.quoteIdent(arena, dialect, k.col);
        const dir = if (k.desc) " DESC" else "";
        switch (dialect) {
            .postgres => try ob.writer().print("{s}{s} NULLS LAST", .{ q, dir }),
            .mysql, .starrocks, .doris => try ob.writer().print("({s} IS NULL), {s}{s}", .{ q, q, dir }),
            .sqlserver => try ob.writer().print("CASE WHEN {s} IS NULL THEN 1 ELSE 0 END, {s}{s}", .{ q, q, dir }),
        }
    }
    const order: []const u8 = if (ob.items.len > 0) try std.fmt.allocPrint(arena, " ORDER BY {s}", .{ob.items}) else "";
    return switch (dialect) {
        .sqlserver => std.fmt.allocPrint(arena, "SELECT TOP ({d}) * FROM ({s}) _t{s}", .{ t.rows, base_sql, order }),
        else => std.fmt.allocPrint(arena, "SELECT * FROM ({s}) _t{s} LIMIT {d}", .{ base_sql, order, t.rows }),
    };
}

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

const test_schema_fields = [_]types.Schema.Field{
    .{ .name = "a", .ty = types.Type.init(.int) },
    .{ .name = "b", .ty = types.Type.init(.string) },
    .{ .name = "c", .ty = types.Type.init(.int) },
};

fn testSchema() types.Schema {
    return .{ .fields = &test_schema_fields };
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

const schema4_fields = [_]types.Schema.Field{
    .{ .name = "a", .ty = types.Type.init(.int) },
    .{ .name = "b", .ty = types.Type.init(.string) },
    .{ .name = "c", .ty = types.Type.init(.int) },
    .{ .name = "d", .ty = types.Type.init(.int) },
};

fn schema4() types.Schema {
    return .{ .fields = &schema4_fields };
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
    try testing.expectEqualStrings("(`b` IS NULL OR (`b` = ''))", (try translateExpr(a, e, .mysql, testSchema(), true)).?);
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
        .{ .src = "status IS EMPTY", .want = "([status] IS NULL OR ([status] = ''))" },
        .{ .src = "status IS NOT EMPTY", .want = null },
        .{ .src = "IF(v > 1, 'a', 'b')", .want = "(CASE WHEN ([v] > 1) THEN 'a' ELSE 'b' END)" },
        .{ .src = "CASE status WHEN 'x', 'y' THEN 1 ELSE 0 END", .want = null },
        .{ .src = "CASE v WHEN 1 THEN 1 ELSE 0 END > 0", .want = "((CASE [v] WHEN 1 THEN 1 ELSE 0 END) > 0)" },
        .{ .src = "CAST(v AS INT) > 5", .want = null },
        .{ .src = "CAST(v AS INT) > 5", .want = null, .d = .mysql },
        .{ .src = "CAST(5 AS INT) > 1", .want = "(CAST(5 AS BIGINT) > 1)" },
        .{ .src = "CAST(v AS STRING) = 'x'", .want = "(CAST([v] AS VARCHAR(MAX)) = 'x')" },
        .{ .src = "lower(status) = 'ok'", .want = "(LOWER([status]) = 'ok')" },
        .{ .src = "length(status) > 2", .want = null },
        .{ .src = "length(status) > 2", .want = null, .d = .mysql },
        .{ .src = "strpos(status, 'a') = 2", .want = null },
        .{ .src = "trim(status) = 'x'", .want = "(LTRIM(RTRIM([status])) = 'x')" },
        .{ .src = "substr(status, 1, 2) = 'AB'", .want = "(SUBSTRING([status], 1, 2) = 'AB')" },
        .{ .src = "coalesce(status, 'n') = 'n'", .want = "(COALESCE([status], 'n') = 'n')" },
        .{ .src = "contains(status, 'ab')", .want = "([status] LIKE '%ab%')" },
        .{ .src = "starts_with(status, 'CT2')", .want = "([status] LIKE 'CT2%')" },
        .{ .src = "status LIKE 'a%'", .want = "([status] LIKE 'a%')" },
        .{ .src = "contains(status, '10%')", .want = null },
        .{ .src = "status LIKE 'a_b'", .want = null },
        .{ .src = "contains(status, '[x]')", .want = "([status] LIKE '%[[]x]%')" },
        .{ .src = "contains(status, '[x]')", .want = "(`status` LIKE '%[x]%')", .d = .mysql },
        .{ .src = "substr(status, 2, 2) = 'AB'", .want = null },
        .{ .src = "status >= 'B'", .want = null },
        .{ .src = "status <> 'x'", .want = null },
        .{ .src = "NOT (status = 'x')", .want = null },
        .{ .src = "status >= 'B' AND v > 1", .want = "([v] > 1)" },
        .{ .src = "status = '\u{e9}'", .want = null },
        .{ .src = "status = 'a\\b'", .want = null, .d = .mysql },
        .{ .src = "now() > v", .want = null },
        .{ .src = "v + 1 > 2", .want = null },
    };
    for (cases) |tc| {
        var diag: sqlp.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        const e = try sqlp.parseExprStr(a, tc.src, &diag);
        const got = try translateExpr(a, e, tc.d, .{ .fields = &.{} }, false);
        if (tc.want) |w| {
            std.testing.expectEqualStrings(w, got orelse "<null>") catch |err| {
                std.debug.print("case: {s}\n", .{tc.src});
                return err;
            };
        } else if (got) |g| {
            std.debug.print("case {s}: want null, got {s}\n", .{ tc.src, g });
            return error.TestUnexpectedResult;
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

    const cmp = try bin(a, .eq, e, try intLit(a, 0));
    try testing.expectEqualStrings("(([a] % 3) = 0)", (try translateExpr(a, cmp, .sqlserver, testSchema(), true)).?);
}

test "translateCall: strpos and length stay in the engine, where the sources disagree on what they count" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    for ([_]Dialect{ .postgres, .mysql, .sqlserver }) |d| {
        const sp = try callExpr(a, "strpos", &[_]*ast.Expr{ try fld(a, "b"), try strLit(a, "x") });
        try testing.expect((try translateExpr(a, sp, d, testSchema(), true)) == null);
        const ln = try callExpr(a, "length", &[_]*ast.Expr{try fld(a, "b")});
        try testing.expect((try translateExpr(a, ln, d, testSchema(), true)) == null);
    }
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

    const cmp = try bin(a, .gt, round_e, try intLit(a, 1));
    try testing.expect((try translateExpr(a, cmp, .postgres, testSchema(), true)) == null);
}

test "translateCall: every pushable name is an engine builtin" {
    for (pushable) |p| {
        if (builtins.lookup(p.name) == null) {
            std.debug.print("pushdown table names `{s}`, which is not a builtin\n", .{p.name});
            return error.TestUnexpectedResult;
        }
    }
}

test "translateExpr: a safe (TRY_) cast is never pushed" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const from_text = try a.create(ast.Expr);
    from_text.* = .{ .cast = .{ .e = try fld(a, "b"), .ty = types.Type.init(.int) } };
    try testing.expect((try translateExpr(a, from_text, .mysql, testSchema(), true)) == null);

    const plain = try a.create(ast.Expr);
    plain.* = .{ .cast = .{ .e = try fld(a, "a"), .ty = types.Type.init(.int) } };
    try testing.expectEqualStrings("CAST(`a` AS SIGNED)", (try translateExpr(a, plain, .mysql, testSchema(), true)).?);

    const safe = try a.create(ast.Expr);
    safe.* = .{ .cast = .{ .e = try fld(a, "a"), .ty = types.Type.init(.int), .safe = true } };
    try testing.expect((try translateExpr(a, safe, .mysql, testSchema(), true)) == null);
    try testing.expect((try translateExpr(a, safe, .postgres, testSchema(), true)) == null);
    try testing.expect((try translateExpr(a, safe, .sqlserver, testSchema(), true)) == null);
}

const whole_schema_fields = [_]types.Schema.Field{
    .{ .name = "a", .ty = types.Type.init(.int) },
    .{ .name = "b", .ty = types.Type.init(.string) },
    .{ .name = "c", .ty = types.Type.init(.int) },
    .{ .name = "f", .ty = types.Type.init(.float) },
    .{ .name = "m", .ty = types.Type.decimal(12, 2) },
    .{ .name = "t", .ty = types.Type.init(.timestamp) },
};

fn wholeSchema() types.Schema {
    return .{ .fields = &whole_schema_fields };
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
                    try testing.expect((try planWholeAgg(a, d, base_t, wholeSchema(), &.{}, ag, plan_schema)) == null);
            },
        }
    }
}

test "planWholeAgg: a qualified or unknown group key falls back" {
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
    try testing.expect((try planWholeAgg(a, .postgres, base_t, wholeSchema(), &.{}, .{ .aggs = aggs, .by = by }, plan_schema)) == null);

    const missing = try byList(a, &.{"zzz"});
    const ms_schema = types.Schema{ .fields = &.{
        .{ .name = "zzz", .ty = types.Type.init(.string) },
        .{ .name = "n", .ty = types.Type.init(.int) },
    } };
    try testing.expect((try planWholeAgg(a, .postgres, base_t, wholeSchema(), &.{}, .{ .aggs = aggs, .by = missing }, ms_schema)) == null);
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

test "planWholeAgg: SUM only descends for an int column into an int result" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const by = try byList(a, &.{"b"});
    const key = types.Schema.Field{ .name = "b", .ty = types.Type.init(.string) };

    const fa = try a.alloc(ast.AggItem, 1);
    fa[0] = .{ .name = "s", .func = .sum, .arg = try fld(a, "f") };
    const fs = types.Schema{ .fields = &.{ key, .{ .name = "s", .ty = types.Type.init(.float).withNull(true) } } };
    try testing.expect((try planWholeAgg(a, .postgres, base_t, wholeSchema(), &.{}, .{ .aggs = fa, .by = by }, fs)) == null);

    const da = try a.alloc(ast.AggItem, 1);
    da[0] = .{ .name = "s", .func = .sum, .arg = try fld(a, "m") };
    const ds = types.Schema{ .fields = &.{ key, .{ .name = "s", .ty = types.Type.decimal(12, 2).withNull(true) } } };
    try testing.expect((try planWholeAgg(a, .mysql, base_t, wholeSchema(), &.{}, .{ .aggs = da, .by = by }, ds)) == null);

    const dd = try a.alloc(ast.AggItem, 1);
    dd[0] = .{ .name = "s", .func = .sum, .arg = try fld(a, "c"), .distinct = true };
    const is = types.Schema{ .fields = &.{ key, .{ .name = "s", .ty = types.Type.init(.int).withNull(true) } } };
    try testing.expect((try planWholeAgg(a, .mysql, base_t, wholeSchema(), &.{}, .{ .aggs = dd, .by = by }, is)) == null);
}

test "planWholeAgg: MIN/MAX falls back on collation- and timezone-sensitive types" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const sa = try a.alloc(ast.AggItem, 1);
    sa[0] = .{ .name = "lo", .func = .min, .arg = try fld(a, "b") };
    const ss = types.Schema{ .fields = &.{.{ .name = "lo", .ty = types.Type.init(.string).withNull(true) }} };
    for ([_]Dialect{ .postgres, .mysql, .sqlserver }) |d|
        try testing.expect((try planWholeAgg(a, d, base_t, wholeSchema(), &.{}, .{ .aggs = sa, .by = &.{} }, ss)) == null);

    const ta = try a.alloc(ast.AggItem, 1);
    ta[0] = .{ .name = "hi", .func = .max, .arg = try fld(a, "t") };
    const ts = types.Schema{ .fields = &.{.{ .name = "hi", .ty = types.Type.init(.timestamp).withNull(true) }} };
    try testing.expect((try planWholeAgg(a, .postgres, base_t, wholeSchema(), &.{}, .{ .aggs = ta, .by = &.{} }, ts)) == null);

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

fn hoistableKind(k: ast.JoinKind) bool {
    return switch (k) {
        .inner, .cross, .left, .semi, .anti => true,
        .right, .full => false,
    };
}

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
                    else => return null,
                };
                return out;
            },
            .filter, .limit, .sort, .distinct => {},
            else => return null,
        }
    }
    return null;
}

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

fn refsOnlyProbe(
    arena: std.mem.Allocator,
    e: *const ast.Expr,
    right: []const []const u8,
    j: ast.Join,
) !bool {
    var list = std.array_list.Managed(ast.QualName).init(arena);
    try collectQuals(arena, e, &list);
    for (list.items) |q| {
        if (q.parts.len > 1 and (std.mem.eql(u8, q.parts[0], j.binding) or std.mem.eql(u8, q.parts[0], j.alias))) return false;
        if (isRightName(q.last(), right)) return false;
    }
    return true;
}

const QualWalk = struct { arena: std.mem.Allocator, list: *std.array_list.Managed(ast.QualName) };

fn collectQualsRecur(cx: QualWalk, e: *const ast.Expr) error{OutOfMemory}!*ast.Expr {
    if (e.* == .field) {
        try cx.list.append(e.field);
        return @constCast(e);
    }
    return ast.rebuildExpr(cx.arena, e, cx, collectQualsRecur);
}

pub fn collectQuals(arena: std.mem.Allocator, e: *const ast.Expr, list: *std.array_list.Managed(ast.QualName)) !void {
    _ = try collectQualsRecur(.{ .arena = arena, .list = list }, e);
}

const KeySwap = struct {
    arena: std.mem.Allocator,
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
        if (map.get(q.last()) == null) return null;
    }
    return try swapKeysRecur(.{ .arena = arena, .map = &map }, e);
}

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
            if (!try refsOnlyProbe(arena, list.items[i].node.filter, right, j)) continue;
            const tmp = list.items[i - 1];
            list.items[i - 1] = list.items[i];
            list.items[i] = tmp;
            moved_this_round = true;
            changed = true;
        }
        if (!moved_this_round) break;
    }

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

pub fn hoistFilters(
    arena: std.mem.Allocator,
    gpa: std.mem.Allocator,
    stages: []const ast.Stage,
    bindings: *const std.StringHashMap(ast.Pipeline),
) !?[]const ast.Stage {
    var cur = stages;
    var changed = false;
    if (try hoistThroughSelects(arena, cur)) |s| {
        cur = s;
        changed = true;
    }
    if (try hoistThroughJoins(arena, gpa, cur, bindings)) |s| {
        cur = s;
        changed = true;
        if (try hoistThroughSelects(arena, cur)) |t| cur = t;
    }
    if (try pushIntoJoinSides(arena, cur)) |s| {
        cur = s;
        changed = true;
    }
    return if (changed) cur else null;
}

pub fn pushIntoJoinSides(arena: std.mem.Allocator, stages: []const ast.Stage) !?[]const ast.Stage {
    var list = std.array_list.Managed(ast.Stage).init(arena);
    try list.appendSlice(stages);
    var changed = false;
    var i: usize = 1;
    while (i < list.items.len) {
        if (list.items[i].node != .filter) {
            i += 1;
            continue;
        }
        var parts = std.array_list.Managed(*ast.Expr).init(arena);
        try splitAnd(list.items[i].node.filter, &parts);
        var stay: ?*ast.Expr = null;
        var moved = false;
        for (parts.items) |c| {
            const target = (try rightAliasOf(arena, c)) orelse {
                stay = try andWith(arena, stay, c);
                continue;
            };
            var k = i;
            const at: ?usize = while (k > 0) {
                k -= 1;
                switch (list.items[k].node) {
                    .filter => {},
                    .join => |j| {
                        if (j.kind != .inner and j.kind != .cross) break null;
                        if (std.mem.eql(u8, j.alias, target) or std.mem.eql(u8, j.binding, target)) break k;
                    },
                    else => break null,
                }
            } else null;
            const jk = at orelse {
                stay = try andWith(arena, stay, c);
                continue;
            };
            var j = list.items[jk].node.join;
            j.right_filter = try andWith(arena, j.right_filter, try unqualify(arena, c, target));
            list.items[jk].node = .{ .join = j };
            moved = true;
        }
        if (!moved) {
            i += 1;
            continue;
        }
        changed = true;
        if (stay) |rest| {
            list.items[i].node = .{ .filter = rest };
            i += 1;
        } else _ = list.orderedRemove(i);
    }
    if (!changed) return null;
    return try list.toOwnedSlice();
}

fn rightAliasOf(arena: std.mem.Allocator, e: *const ast.Expr) !?[]const u8 {
    var refs = std.array_list.Managed(ast.QualName).init(arena);
    try collectQuals(arena, e, &refs);
    var alias: ?[]const u8 = null;
    for (refs.items) |q| {
        if (q.dollar) continue;
        if (q.parts.len != 2) return null;
        if (alias) |a| {
            if (!std.mem.eql(u8, a, q.parts[0])) return null;
        } else alias = q.parts[0];
    }
    return alias;
}

const Unqualify = struct { arena: std.mem.Allocator, alias: []const u8 };

fn unqualifyRecur(cx: Unqualify, e: *const ast.Expr) error{OutOfMemory}!*ast.Expr {
    if (e.* == .field) {
        const q = e.field;
        if (!q.dollar and q.parts.len == 2 and std.mem.eql(u8, q.parts[0], cx.alias))
            return mkExpr(cx.arena, .{ .field = try qual(cx.arena, q.parts[1..]) });
        return @constCast(e);
    }
    return ast.rebuildExpr(cx.arena, e, cx, unqualifyRecur);
}

fn unqualify(arena: std.mem.Allocator, e: *ast.Expr, alias: []const u8) !*ast.Expr {
    return unqualifyRecur(.{ .arena = arena, .alias = alias }, e);
}

pub fn hoistThroughSelects(arena: std.mem.Allocator, stages: []const ast.Stage) !?[]const ast.Stage {
    if (stages.len < 3) return null;
    var list = std.array_list.Managed(ast.Stage).init(arena);
    try list.appendSlice(stages);
    var changed = false;
    var rounds: usize = 0;
    while (rounds < list.items.len) : (rounds += 1) {
        var moved_this_round = false;
        var i: usize = 1;
        while (i < list.items.len) : (i += 1) {
            if (list.items[i].node != .filter or list.items[i - 1].node != .select) continue;
            const f = list.items[i];
            const items = list.items[i - 1].node.select;
            var parts = std.array_list.Managed(*ast.Expr).init(arena);
            try splitAnd(f.node.filter, &parts);
            var below: ?*ast.Expr = null;
            var above: ?*ast.Expr = null;
            for (parts.items) |c| {
                if (try filterBelowSelect(arena, c, items)) |m| {
                    below = try andWith(arena, below, m);
                } else above = try andWith(arena, above, c);
            }
            const moved = below orelse continue;
            const sel = list.items[i - 1];
            list.items[i - 1] = .{ .node = .{ .filter = moved }, .hints = f.hints, .pos = f.pos };
            list.items[i] = sel;
            if (above) |rest| try list.insert(i + 1, .{ .node = .{ .filter = rest }, .hints = f.hints, .pos = f.pos });
            moved_this_round = true;
            changed = true;
        }
        if (!moved_this_round) break;
    }
    if (!changed) return null;
    return try list.toOwnedSlice();
}

fn splitAnd(e: *ast.Expr, out: *std.array_list.Managed(*ast.Expr)) !void {
    if (e.* == .binary and e.binary.op == .@"and") {
        try splitAnd(e.binary.l, out);
        try splitAnd(e.binary.r, out);
    } else try out.append(e);
}

fn andWith(arena: std.mem.Allocator, acc: ?*ast.Expr, e: *ast.Expr) !*ast.Expr {
    const l = acc orelse return e;
    return mkExpr(arena, .{ .binary = .{ .op = .@"and", .l = l, .r = e } });
}

fn projectedFrom(arena: std.mem.Allocator, items: []const ast.SelectItem, name: []const u8) !?ast.QualName {
    var star = false;
    for (items) |it| switch (it) {
        .star => star = true,
        .star_except => |names| {
            for (names) |x| if (std.mem.eql(u8, x, name)) return null;
            star = true;
        },
        .star_rename => |rs| {
            for (rs) |r| {
                if (std.mem.eql(u8, r.to, name)) return try qual(arena, &.{r.from});
                if (std.mem.eql(u8, r.from, name)) return null;
            }
            star = true;
        },
        .field => |q| if (std.mem.eql(u8, q.last(), name)) return q,
        .computed => |c| if (std.mem.eql(u8, c.name, name)) {
            if (c.expr.* == .field and !c.expr.field.dollar) return c.expr.field;
            return null;
        },
    };
    return if (star) try qual(arena, &.{name}) else null;
}

const ColSwap = struct {
    arena: std.mem.Allocator,
    map: *const std.StringHashMap(ast.QualName),
};

fn swapColsRecur(cx: ColSwap, e: *const ast.Expr) error{OutOfMemory}!*ast.Expr {
    if (e.* == .field) {
        if (!e.field.dollar and e.field.parts.len == 1)
            if (cx.map.get(e.field.parts[0])) |src| return mkExpr(cx.arena, .{ .field = src });
        return @constCast(e);
    }
    return ast.rebuildExpr(cx.arena, e, cx, swapColsRecur);
}

fn filterBelowSelect(arena: std.mem.Allocator, pred: *const ast.Expr, items: []const ast.SelectItem) !?*ast.Expr {
    for (items) |it| switch (it) {
        .field => |q| if (std.mem.indexOf(u8, q.last(), "${") != null) return null,
        .computed => |c| if (std.mem.indexOf(u8, c.name, "${") != null) return null,
        .star_except => |names| for (names) |n| {
            if (std.mem.indexOf(u8, n, "${") != null) return null;
        },
        else => {},
    };
    var refs = std.array_list.Managed(ast.QualName).init(arena);
    try collectQuals(arena, pred, &refs);
    var map = std.StringHashMap(ast.QualName).init(arena);
    for (refs.items) |q| {
        if (q.dollar) continue;
        if (q.parts.len != 1 or std.mem.indexOf(u8, q.parts[0], "${") != null) return null;
        const src = (try projectedFrom(arena, items, q.parts[0])) orelse return null;
        try map.put(q.parts[0], src);
    }
    return try swapColsRecur(.{ .arena = arena, .map = &map }, pred);
}

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

fn projStage(items: []const ast.SelectItem) ast.Stage {
    return .{ .node = .{ .select = items }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
}

fn filterStage(e: *ast.Expr) ast.Stage {
    return .{ .node = .{ .filter = e }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
}

test "hoist: a filter on a renamed column moves below the projection, in the source's names" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const items = [_]ast.SelectItem{
        .{ .computed = .{ .name = "num", .expr = try fld(a, "C5_NUM") } },
        .{ .computed = .{ .name = "dbl", .expr = try bin(a, .mul, try fld(a, "C5_VALOR"), try intLit(a, 2)) } },
    };
    const pred = try bin(a, .@"and", try bin(a, .gt, try fld(a, "dbl"), try intLit(a, 10)), try bin(a, .gt, try fld(a, "num"), try intLit(a, 5)));
    const stages = [_]ast.Stage{ readStage(), projStage(&items), filterStage(pred), writeStage() };

    const out = (try hoistThroughSelects(a, &stages)).?;
    try std.testing.expectEqual(@as(usize, 5), out.len);
    try std.testing.expect(out[1].node == .filter and out[2].node == .select and out[3].node == .filter);
    const where = (try serialWhere(a, .postgres, out[0 .. out.len - 1])).?;
    try std.testing.expectEqualStrings("(\"C5_NUM\" > 5)", where);
    try std.testing.expectEqualStrings("dbl", out[3].node.filter.binary.l.field.parts[0]);
}

test "hoist: a projection keeps a filter it computes, or names per row, and a qualifier it passes" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const computed = [_]ast.SelectItem{.{ .computed = .{ .name = "y", .expr = try bin(a, .add, try fld(a, "x"), try intLit(a, 1)) } }};
    const s1 = [_]ast.Stage{ readStage(), projStage(&computed), filterStage(try bin(a, .eq, try fld(a, "y"), try intLit(a, 1))), writeStage() };
    try std.testing.expect((try hoistThroughSelects(a, &s1)) == null);
    const s2 = [_]ast.Stage{ readStage(), projStage(&computed), filterStage(try bin(a, .@"or", try bin(a, .eq, try fld(a, "y"), try intLit(a, 1)), try bin(a, .eq, try fld(a, "x"), try intLit(a, 2)))), writeStage() };
    try std.testing.expect((try hoistThroughSelects(a, &s2)) == null);
    const dynamic = [_]ast.SelectItem{.{ .field = try qual(a, &.{"${col}"}) }};
    const s3 = [_]ast.Stage{ readStage(), projStage(&dynamic), filterStage(try bin(a, .eq, try fld(a, "k"), try intLit(a, 1))), writeStage() };
    try std.testing.expect((try hoistThroughSelects(a, &s3)) == null);
    const qualified = [_]ast.SelectItem{.{ .computed = .{ .name = "bx", .expr = try mkExpr(a, .{ .field = try qual(a, &.{ "b", "x" }) }) } }};
    const s4 = [_]ast.Stage{ readStage(), projStage(&qualified), filterStage(try bin(a, .eq, try fld(a, "bx"), try intLit(a, 1))), writeStage() };
    const out = (try hoistThroughSelects(a, &s4)).?;
    try std.testing.expectEqual(@as(usize, 2), out[1].node.filter.binary.l.field.parts.len);
    const star = [_]ast.SelectItem{ .star, .{ .computed = .{ .name = "y", .expr = try bin(a, .add, try fld(a, "x"), try intLit(a, 1)) } } };
    const s5 = [_]ast.Stage{ readStage(), projStage(&star), filterStage(try bin(a, .eq, try fld(a, "k"), try intLit(a, 1))), writeStage() };
    try std.testing.expect((try hoistThroughSelects(a, &s5)).?[1].node == .filter);
}

test "push: a filter on an inner join's right side by its alias becomes the join's, in the right side's names" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const stages = [_]ast.Stage{ readStage(), try joinStage(a, .inner, "r", "k", "rk"), filterStage(try bin(a, .@"and", try bin(a, .gt, try qfld(a, &.{ "r", "v" }), try intLit(a, 0)), try bin(a, .gt, try fld(a, "x"), try intLit(a, 1)))), writeStage() };
    const out = (try pushIntoJoinSides(a, &stages)).?;
    const rf = out[1].node.join.right_filter.?;
    try std.testing.expectEqual(@as(usize, 1), rf.binary.l.field.parts.len);
    try std.testing.expectEqualStrings("v", rf.binary.l.field.parts[0]);
    try std.testing.expectEqualStrings("x", out[2].node.filter.binary.l.field.parts[0]);
    const whole = [_]ast.Stage{ readStage(), try joinStage(a, .inner, "r", "k", "rk"), filterStage(try bin(a, .gt, try qfld(a, &.{ "r", "v" }), try intLit(a, 0))), writeStage() };
    try std.testing.expectEqual(@as(usize, 3), (try pushIntoJoinSides(a, &whole)).?.len);
    const two = [_]ast.Stage{ readStage(), try joinStage(a, .inner, "r", "k", "rk"), try joinStage(a, .inner, "s", "k", "sk"), filterStage(try bin(a, .gt, try qfld(a, &.{ "r", "v" }), try intLit(a, 0))), writeStage() };
    const o2 = (try pushIntoJoinSides(a, &two)).?;
    try std.testing.expect(o2[1].node.join.right_filter != null and o2[2].node.join.right_filter == null);
}

test "push: a filter stays after a LEFT join, across sides, on a bare name, or past a LEFT join" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const rv = try bin(a, .gt, try qfld(a, &.{ "r", "v" }), try intLit(a, 0));
    const left = [_]ast.Stage{ readStage(), try joinStage(a, .left, "r", "k", "rk"), filterStage(rv), writeStage() };
    try std.testing.expect((try pushIntoJoinSides(a, &left)) == null);
    const across = [_]ast.Stage{ readStage(), try joinStage(a, .inner, "r", "k", "rk"), filterStage(try bin(a, .@"or", rv, try bin(a, .eq, try fld(a, "x"), try intLit(a, 1)))), writeStage() };
    try std.testing.expect((try pushIntoJoinSides(a, &across)) == null);
    const bare = [_]ast.Stage{ readStage(), try joinStage(a, .inner, "r", "k", "rk"), filterStage(try bin(a, .gt, try fld(a, "v"), try intLit(a, 0))), writeStage() };
    try std.testing.expect((try pushIntoJoinSides(a, &bare)) == null);
    const past = [_]ast.Stage{ readStage(), try joinStage(a, .inner, "r", "k", "rk"), try joinStage(a, .left, "s", "k", "sk"), filterStage(rv), writeStage() };
    try std.testing.expect((try pushIntoJoinSides(a, &past)) == null);
}

test "hoist: a probe-only filter moves below an inner join" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var binds = try dimBindings(a);
    defer binds.deinit();

    const stages = try a.alloc(ast.Stage, 4);
    stages[0] = readStage();
    stages[1] = try joinStage(a, .inner, "r", "k", "rk");
    stages[2] = try eqFilter(a, &.{"v"}, 1);
    stages[3] = writeStage();

    const out = (try hoistThroughJoins(a, a, stages, &binds)).?;
    try std.testing.expectEqual(@as(usize, 4), out.len);
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
        &.{"name"},
        &.{ "r", "name" },
        &.{"name_r"},
    }) |parts| {
        const stages = try a.alloc(ast.Stage, 4);
        stages[0] = readStage();
        stages[1] = try joinStage(a, .inner, "r", "k", "rk");
        stages[2] = try eqFilter(a, parts, 1);
        stages[3] = writeStage();
        try std.testing.expect((try hoistThroughJoins(a, a, stages, &binds)) == null);
    }
}

test "hoist: refused for the join kinds that null-extend the probe side" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var binds = try dimBindings(a);
    defer binds.deinit();

    for ([_]ast.JoinKind{ .right, .full }) |kind| {
        const stages = try a.alloc(ast.Stage, 4);
        stages[0] = readStage();
        stages[1] = try joinStage(a, kind, "r", "k", "rk");
        stages[2] = try eqFilter(a, &.{"v"}, 1);
        stages[3] = writeStage();
        try std.testing.expect((try hoistThroughJoins(a, a, stages, &binds)) == null);
    }
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

    const stages = try a.alloc(ast.Stage, 4);
    stages[0] = readStage();
    stages[1] = try joinStage(a, .inner, "r", "k", "rk");
    stages[2] = try eqFilter(a, &.{ "r", "rk" }, 7);
    stages[3] = writeStage();

    const out = (try hoistThroughJoins(a, a, stages, &binds)).?;
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

    for ([_]ast.JoinKind{ .left, .anti, .semi, .right, .full, .cross }) |kind| {
        const stages = try a.alloc(ast.Stage, 4);
        stages[0] = readStage();
        stages[1] = try joinStage(a, kind, "r", "k", "rk");
        stages[2] = try eqFilter(a, &.{ "r", "rk" }, 7);
        stages[3] = writeStage();
        try std.testing.expect((try hoistThroughJoins(a, a, stages, &binds)) == null);
    }

    const stages = try a.alloc(ast.Stage, 4);
    stages[0] = readStage();
    stages[1] = try joinStage(a, .inner, "r", "k", "rk");
    stages[2] = try eqFilter(a, &.{ "r", "name" }, 7);
    stages[3] = writeStage();
    try std.testing.expect((try hoistThroughJoins(a, a, stages, &binds)) == null);
}

fn topNStagesOf(arena: std.mem.Allocator, sql: []const u8) ![]const ast.Stage {
    const parser = @import("../lang/sql_parser.zig");
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const src = try std.fmt.allocPrint(arena, "CREATE CONNECTION db TYPE postgres OPTIONS (host = 'h');\n{s};", .{sql});
    const prog = try parser.parseSource(arena, src, &pdiag);
    for (prog.stmts) |s| if (s == .output) return s.output.stages[0 .. s.output.stages.len - 1];
    return error.TestUnexpectedResult;
}

fn topNOf(arena: std.mem.Allocator, sql: []const u8, why: *[]const u8) !?TopN {
    return classifyTopN(arena, try topNStagesOf(arena, sql), why);
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

test "top-N: key types the source orders as the engine does, and filters that must all descend" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const fields = [_]types.Schema.Field{
        .{ .name = "id", .ty = types.Type.init(.int) },
        .{ .name = "ts", .ty = types.Type.init(.timestamp).asNullable() },
        .{ .name = "amt", .ty = types.Type.decimal(10, 2) },
        .{ .name = "name", .ty = types.Type.init(.string) },
    };
    const schema = types.Schema{ .fields = &fields };
    var why: []const u8 = "";

    for ([_][]const u8{ "id", "ts", "amt" }) |k| {
        const sql = try std.fmt.allocPrint(a, "SELECT * FROM db.t ORDER BY {s} LIMIT 5", .{k});
        const stages = try topNStagesOf(a, sql);
        const t = (try classifyTopN(a, stages, &why)).?;
        try std.testing.expect((try planTopN(a, .postgres, "SELECT * FROM t", schema, stages, t, null, &why)) != null);
    }
    const s_st = try topNStagesOf(a, "SELECT * FROM db.t ORDER BY name LIMIT 5");
    const s_t = (try classifyTopN(a, s_st, &why)).?;
    try std.testing.expect((try planTopN(a, .postgres, "SELECT * FROM t", schema, s_st, s_t, null, &why)) == null);
    try std.testing.expect(std.mem.indexOf(u8, why, "collation") != null);
    const f_st = try topNStagesOf(a, "SELECT * FROM db.t WHERE to_hex(id) = '5' ORDER BY id LIMIT 5");
    const f_t = (try classifyTopN(a, f_st, &why)).?;
    try std.testing.expect((try planTopN(a, .postgres, "SELECT * FROM t", schema, f_st, f_t, null, &why)) == null);
    try std.testing.expect(std.mem.indexOf(u8, why, "does not translate") != null);
}

test "top-N: each dialect's spelling of nulls-last ordering and the row cap" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const t = TopN{ .rows = 10, .keys = &.{ .{ .col = "v", .desc = true }, .{ .col = "id", .desc = false } } };
    const base = "SELECT * FROM t";
    try std.testing.expectEqualStrings(
        "SELECT * FROM (SELECT * FROM t) _t ORDER BY \"v\" DESC NULLS LAST, \"id\" NULLS LAST LIMIT 10",
        try renderTopN(a, .postgres, base, t),
    );
    try std.testing.expectEqualStrings(
        "SELECT * FROM (SELECT * FROM t) _t ORDER BY (`v` IS NULL), `v` DESC, (`id` IS NULL), `id` LIMIT 10",
        try renderTopN(a, .mysql, base, t),
    );
    try std.testing.expectEqualStrings(
        "SELECT * FROM (SELECT * FROM t) _t ORDER BY (`v` IS NULL), `v` DESC, (`id` IS NULL), `id` LIMIT 10",
        try renderTopN(a, .starrocks, base, t),
    );
    try std.testing.expectEqualStrings(
        "SELECT TOP (10) * FROM (SELECT * FROM t) _t ORDER BY CASE WHEN [v] IS NULL THEN 1 ELSE 0 END, [v] DESC, CASE WHEN [id] IS NULL THEN 1 ELSE 0 END, [id]",
        try renderTopN(a, .sqlserver, base, t),
    );
    try std.testing.expectEqualStrings("SELECT TOP (5) * FROM (SELECT * FROM t) _t", try renderTopN(a, .sqlserver, base, .{ .rows = 5 }));
    try std.testing.expectEqualStrings("SELECT * FROM (SELECT * FROM t) _t LIMIT 5", try renderTopN(a, .postgres, base, .{ .rows = 5 }));
}

test "text comparisons against the column's collation: exact where it compares bytes, widened where it pads, refused where it folds" {
    var arn = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arn.deinit();
    const a = arn.allocator();
    const sqlp = @import("../lang/sql_parser.zig");

    var facts = Facts.init(a);
    try facts.put("d", .{ .text = true, .byte_order = true, .pads = true });
    try facts.put("ci", .{ .text = true, .byte_order = false, .pads = true });
    try facts.put("c", .{ .text = true, .byte_order = true, .pads = false });
    try facts.put("n", .{ .text = false });

    const cases = [_]struct { src: []const u8, need: Need, want: ?[]const u8, d: Dialect = .sqlserver }{
        .{ .src = "d >= '20240105'", .need = .superset, .want = "([d] >= '20240105' OR [d] LIKE '20240105%')" },
        .{ .src = "d > '000123'", .need = .superset, .want = "([d] >= '000123' OR [d] LIKE '000123%')" },
        .{ .src = "d < 'Z'", .need = .superset, .want = "([d] <= 'Z')" },
        .{ .src = "d = '01'", .need = .superset, .want = "([d] = '01')" },
        .{ .src = "d = '01'", .need = .exact, .want = "([d] = '01' AND DATALENGTH([d]) = 2)" },
        .{ .src = "d > '000123'", .need = .exact, .want = "([d] > '000123' OR ([d] LIKE '000123%' AND DATALENGTH([d]) > 6))" },
        .{ .src = "d < '2024'", .need = .exact, .want = "(NOT ([d] >= '2024' OR [d] LIKE '2024%'))" },
        .{ .src = "NOT (d = 'x')", .need = .superset, .want = "(NOT (([d] = 'x' AND DATALENGTH([d]) = 1)))" },
        .{ .src = "d >= 'a b'", .need = .superset, .want = "([d] >= 'a b' OR [d] LIKE 'a b%')" },
        .{ .src = "d >= '10%'", .need = .superset, .want = "([d] >= '10%' OR [d] LIKE '10[%]%')" },
        .{ .src = "d = 'x '", .need = .exact, .want = null },
        .{ .src = "ci = 'x'", .need = .superset, .want = "([ci] = 'x')" },
        .{ .src = "ci = 'x'", .need = .exact, .want = null },
        .{ .src = "ci >= 'B'", .need = .superset, .want = null },
        .{ .src = "ci <> 'x'", .need = .superset, .want = null },
        .{ .src = "ci LIKE 'a%'", .need = .superset, .want = "([ci] LIKE 'a%')" },
        .{ .src = "ci LIKE 'a%'", .need = .exact, .want = null },
        .{ .src = "c >= 'b'", .need = .exact, .want = "(\"c\" >= 'b')", .d = .postgres },
        .{ .src = "c <> 'b'", .need = .exact, .want = "(\"c\" <> 'b')", .d = .postgres },
        .{ .src = "d = 'x'", .need = .exact, .want = null, .d = .postgres },
        .{ .src = "n >= '2024-01-01'", .need = .exact, .want = "([n] >= '2024-01-01')" },
        .{ .src = "d = '01'", .need = .exact, .want = "(`d` = '01' AND LENGTH(`d`) = 2)", .d = .mysql },
    };
    for (cases) |tc| {
        var diag: sqlp.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        const e = try sqlp.parseExprStr(a, tc.src, &diag);
        const got = try translatePred(a, e, tc.d, .{ .facts = &facts, .need = tc.need });
        if (tc.want) |w| {
            std.testing.expectEqualStrings(w, got orelse "<null>") catch |err| {
                std.debug.print("case: {s}\n", .{tc.src});
                return err;
            };
        } else if (got) |g| {
            std.debug.print("case {s}: want null, got {s}\n", .{ tc.src, g });
            return error.TestUnexpectedResult;
        }
    }

    var wants = false;
    var diag: sqlp.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const e = try sqlp.parseExprStr(a, "x >= 'B'", &diag);
    try std.testing.expect((try translatePred(a, e, .sqlserver, .{ .wants_facts = &wants })) == null);
    try std.testing.expect(wants);
}
