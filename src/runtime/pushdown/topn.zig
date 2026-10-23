//! `ORDER BY … LIMIT n` sent to a SQL source, when the order and filters before it
//! translate exactly.

pub const Dialect = @import("../../db/sql.zig").Dialect;
const Facts = @import("translate.zig").Facts;
const ast = @import("../../lang/ast.zig");
const quoteName = @import("../../db/sql.zig").quoteName;
const std = @import("std");
const translatePred = @import("translate.zig").translatePred;
const types = @import("../../lang/types.zig");
pub const topNStagesOf = @import("testing_util.zig").topNStagesOf;

pub const TopN = struct {
    rows: u64,
    keys: []const Key = &.{},

    pub const Key = struct { col: []const u8, desc: bool, nulls_first: bool };

    pub fn describe(self: TopN, arena: std.mem.Allocator) ![]const u8 {
        var out = std.array_list.Managed(u8).init(arena);
        for (self.keys, 0..) |k, i| {
            try out.appendSlice(if (i == 0) "order by " else ", ");
            try out.appendSlice(k.col);
            if (k.desc) try out.appendSlice(" desc");
            if (k.nulls_first != k.desc) try out.appendSlice(if (k.nulls_first) " nulls first" else " nulls last");
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
        out.* = .{ .col = col, .desc = k.desc, .nulls_first = k.nulls_first };
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

/// The filters before the limit as one exact source predicate ("" with none), or
/// null when one does not run exactly there: the cap must count only the rows the
/// engine keeps, so this predicate, not a looser one, goes under it.
fn filtersAllTranslate(arena: std.mem.Allocator, dialect: Dialect, stages: []const ast.Stage, schema: types.Schema, check_fields: bool, facts: ?*const Facts) !?[]const u8 {
    var w = std.array_list.Managed(u8).init(arena);
    for (stages[1..]) |st| {
        if (st.node != .filter) break;
        const sql = (try translatePred(arena, st.node.filter, dialect, .{ .schema = schema, .check_fields = check_fields, .facts = facts, .need = .exact })) orelse return null;
        if (w.items.len > 0) try w.appendSlice(" AND ");
        try w.writer().print("({s})", .{sql});
    }
    return w.items;
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
    _ = (try filtersAllTranslate(arena, dialect, stages, .{ .fields = &.{} }, false, null)) orelse return null;
    const d = try t.describe(arena);
    return .{
        .text = if (t.keys.len == 0) d else try std.fmt.allocPrint(arena, "{s} (if the keys are numeric or temporal)", .{d}),
        .rows = t.rows,
        .sorted = t.keys.len > 0,
    };
}

pub fn planTopN(arena: std.mem.Allocator, dialect: Dialect, base_sql: []const u8, src_schema: types.Schema, stages: []const ast.Stage, t: TopN, facts: ?*const Facts, why: *[]const u8) !?[]const u8 {
    const where = (try filtersAllTranslate(arena, dialect, stages, src_schema, true, facts)) orelse {
        why.* = try std.fmt.allocPrint(arena, "a WHERE predicate does not translate exactly to {s} SQL (a text comparison the column's collation decides differently, or an untranslatable piece)", .{@tagName(dialect)});
        return null;
    };
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
    const base = if (where.len == 0) base_sql else try std.fmt.allocPrint(arena, "SELECT * FROM ({s}) _w WHERE {s}", .{ base_sql, where });
    return try renderTopN(arena, dialect, base, t);
}

/// `base_sql` ordered with each key's nulls placed as the engine places them, and
/// capped: `NULLS FIRST`/`NULLS LAST` on postgres, a leading `k IS NULL` key on
/// mysql, a `CASE` key and `TOP` on sqlserver.
pub fn renderTopN(arena: std.mem.Allocator, dialect: Dialect, base_sql: []const u8, t: TopN) ![]const u8 {
    var ob = std.array_list.Managed(u8).init(arena);
    for (t.keys, 0..) |k, i| {
        if (i > 0) try ob.appendSlice(", ");
        const q = try quoteName(arena, dialect, k.col);
        try dialect.orderKey(ob.writer(), q, k.desc, k.nulls_first);
    }
    const order: []const u8 = if (ob.items.len > 0) try std.fmt.allocPrint(arena, " ORDER BY {s}", .{ob.items}) else "";
    return dialect.firstRows(arena, base_sql, order, t.rows);
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

    const w_st = try topNStagesOf(a, "SELECT * FROM db.t WHERE ts >= '2024-01-05T00:00:00Z' ORDER BY id LIMIT 5");
    const w_t = (try classifyTopN(a, w_st, &why)).?;
    const q = (try planTopN(a, .postgres, "SELECT * FROM \"t\"", schema, w_st, w_t, null, &why)).?;
    try std.testing.expect(std.mem.startsWith(u8, q, "SELECT * FROM (SELECT * FROM (SELECT * FROM \"t\") _w WHERE ((\"ts\" >= '2024-01-05 00:00:00'))) _t"));
    try std.testing.expect(std.mem.endsWith(u8, q, ") _t ORDER BY \"id\" NULLS LAST LIMIT 5"));
}

test "top-N: each dialect's spelling of the keys' null placement and the row cap" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const t = TopN{ .rows = 10, .keys = &.{ .{ .col = "v", .desc = true, .nulls_first = true }, .{ .col = "id", .desc = false, .nulls_first = false }, .{ .col = "w", .desc = false, .nulls_first = true } } };
    const base = "SELECT * FROM t";
    try std.testing.expectEqualStrings(
        "SELECT * FROM (SELECT * FROM t) _t ORDER BY \"v\" DESC NULLS FIRST, \"id\" NULLS LAST, \"w\" NULLS FIRST LIMIT 10",
        try renderTopN(a, .postgres, base, t),
    );
    try std.testing.expectEqualStrings(
        "SELECT * FROM (SELECT * FROM t) _t ORDER BY (`v` IS NULL) DESC, `v` DESC, (`id` IS NULL), `id`, (`w` IS NULL) DESC, `w` LIMIT 10",
        try renderTopN(a, .mysql, base, t),
    );
    try std.testing.expectEqualStrings(
        "SELECT * FROM (SELECT * FROM t) _t ORDER BY (`v` IS NULL) DESC, `v` DESC, (`id` IS NULL), `id`, (`w` IS NULL) DESC, `w` LIMIT 10",
        try renderTopN(a, .starrocks, base, t),
    );
    try std.testing.expectEqualStrings(
        "SELECT TOP (10) * FROM (SELECT * FROM t) _t ORDER BY CASE WHEN [v] IS NULL THEN 0 ELSE 1 END, [v] DESC, CASE WHEN [id] IS NULL THEN 1 ELSE 0 END, [id], CASE WHEN [w] IS NULL THEN 0 ELSE 1 END, [w]",
        try renderTopN(a, .sqlserver, base, t),
    );
    try std.testing.expectEqualStrings("SELECT TOP (5) * FROM (SELECT * FROM t) _t", try renderTopN(a, .sqlserver, base, .{ .rows = 5 }));
    try std.testing.expectEqualStrings("SELECT * FROM (SELECT * FROM t) _t LIMIT 5", try renderTopN(a, .postgres, base, .{ .rows = 5 }));
}
