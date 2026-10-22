//! What a SQL read sends: its query, the columns it asks for, `* EXCEPT`, the WHERE it
//! carries, and how it splits across lanes.

const DbConfig = @import("../env.zig").DbConfig;
const Env = @import("../env.zig").Env;
const SplitCtx = @import("../connect.zig").SplitCtx;
const SqlDesc = @import("../env.zig").SqlDesc;
pub const SqlKind = @import("../env.zig").SqlKind;
const ast = @import("../../lang/ast.zig");
const connectSql = @import("../connect.zig").connectSql;
const connectorType = @import("../connect.zig").connectorType;
const forHintName = @import("../env.zig").forHintName;
const openSourceProjected = @import("source.zig").openSourceProjected;
const planErr = @import("../env.zig").planErr;
const projectedColumns = @import("../plan.zig").projectedColumns;
const qualStr = @import("facts.zig").qualStr;
const readReport = @import("../connect.zig").readReport;
const resolveDbConfig = @import("dbconfig.zig").resolveDbConfig;
const split = @import("../../connect/split.zig");
const sql = @import("../../db/sql.zig");
const sqlConnInfo = @import("../connect.zig").sqlConnInfo;
const std = @import("std");

pub fn readSql(env: *Env, rd: ast.Read) ![]const u8 {
    const base = switch (rd.form) {
        .query => |q| queryText(q),
        .table => |t| try std.fmt.allocPrint(env.arena, "SELECT {s} FROM {s}", .{ try selectList(env, rd), try fromName(env, rd.connector, t) }),
        else => return planErr(env.diag, "a DB read needs `table <name>` or `query \"...\"`"),
    };
    return sqlWithWhere(env.arena, base, rd.form == .query, rd.where);
}

/// Whether pushdowns may wrap read `rd` as a subquery: a table always; a query unless
/// SQL Server would refuse it in a derived table, as it does a leading `WITH` or a
/// top-level `ORDER BY`. Such a query is then read as written, filtered here.
pub fn pushableRead(dialect: sql.Dialect, rd: ast.Read) bool {
    return switch (rd.form) {
        .table => true,
        .query => |q| dialect != .sqlserver or wrappableTsql(queryText(q)),
        else => false,
    };
}

fn wrappableTsql(q: []const u8) bool {
    var depth: usize = 0;
    var i: usize = 0;
    var first = true;
    while (i < q.len) {
        const c = q[i];
        if (c == '\'' or c == '"' or c == '[') {
            const close: u8 = if (c == '[') ']' else c;
            i += 1;
            while (i < q.len and q[i] != close) i += 1;
            i += 1;
            continue;
        }
        if (c == '-' and i + 1 < q.len and q[i + 1] == '-') {
            i = std.mem.indexOfScalarPos(u8, q, i, '\n') orelse q.len;
            continue;
        }
        if (c == '/' and i + 1 < q.len and q[i + 1] == '*') {
            i = if (std.mem.indexOfPos(u8, q, i + 2, "*/")) |e| e + 2 else q.len;
            continue;
        }
        if (c == '(') depth += 1;
        if (c == ')' and depth > 0) depth -= 1;
        if (std.ascii.isAlphabetic(c) and (i == 0 or !isWordByte(q[i - 1]))) {
            var e = i;
            while (e < q.len and isWordByte(q[e])) e += 1;
            const word = q[i..e];
            if (first and std.ascii.eqlIgnoreCase(word, "with")) return false;
            first = false;
            if (depth == 0 and std.ascii.eqlIgnoreCase(word, "order")) {
                var k = e;
                while (k < q.len and std.ascii.isWhitespace(q[k])) k += 1;
                if (k + 2 <= q.len and std.ascii.eqlIgnoreCase(q[k .. k + 2], "by") and (k + 2 == q.len or !isWordByte(q[k + 2]))) return false;
            }
            i = e;
            continue;
        }
        if (!std.ascii.isWhitespace(c)) first = false;
        i += 1;
    }
    return true;
}

fn isWordByte(c: u8) bool {
    return std.ascii.isAlphanumeric(c) or c == '_';
}

/// A `query "..."` read's text without its trailing `;`: pushdowns wrap it as a
/// subquery, where the terminator is a syntax error.
fn queryText(q: []const u8) []const u8 {
    return std.mem.trimRight(u8, q, " \t\r\n;");
}

/// Each part quoted for the source's dialect, as a write names the table, so a
/// mixed-case or reserved name reads back what a LOAD created.
fn fromName(env: *Env, connector: []const u8, t: ast.QualName) ![]const u8 {
    const conn = env.connections.get(connector) orelse return qualStr(env.arena, t);
    const info = sqlConnInfo(conn) orelse return qualStr(env.arena, t);
    var out = std.array_list.Managed(u8).init(env.arena);
    for (t.parts, 0..) |p, i| {
        if (i > 0) try out.append('.');
        try out.appendSlice(try sql.quoteName(env.arena, info.dialect, p));
    }
    return out.toOwnedSlice();
}

fn selectList(env: *Env, rd: ast.Read) ![]const u8 {
    if (rd.cols.len == 0) return "*";
    const conn = env.connections.get(rd.connector) orelse return "*";
    const info = sqlConnInfo(conn) orelse return "*";
    return selectListFor(env.arena, info.dialect, rd.cols);
}

pub fn selectListFor(arena: std.mem.Allocator, dialect: sql.Dialect, cols: []const []const u8) ![]const u8 {
    if (cols.len == 0) return "*";
    var out = std.array_list.Managed(u8).init(arena);
    for (cols, 0..) |c, i| {
        if (i > 0) try out.appendSlice(", ");
        try out.appendSlice(try sql.quoteName(arena, dialect, c));
    }
    return out.toOwnedSlice();
}

/// After a join an unqualified name may be the other side's, so only the table's
/// own columns are kept, learnt from a zero-row probe; a failed probe reads all.
pub fn projectSqlRead(env: *Env, stages: []const ast.Stage) ![]const ast.Stage {
    if (stages.len == 0 or stages[0].node != .read) return stages;
    const rd = stages[0].node.read;
    if (rd.form != .table or rd.cols.len > 0) return stages;
    const conn = env.connections.get(rd.connector) orelse return stages;
    if (sqlConnInfo(conn) == null) return stages;
    const cols = (try projectedColumns(env, stages[1..])) orelse blk: {
        const except = starExcept(stages[1..]) orelse return stages;
        break :blk (try exceptColumns(env, rd, stages[0].hints, except)) orelse return stages;
    };
    if (cols.len == 0) return stages;
    for (cols) |c| if (std.mem.indexOfScalar(u8, c, '.') != null) return stages;
    var own = cols;
    for (stages[1..]) |st| if (st.node == .join) {
        own = (try tableColumnsAmong(env, rd, stages[0].hints, cols)) orelse return stages;
        break;
    };
    if (own.len == 0) return stages;
    const out = try env.arena.dupe(ast.Stage, stages);
    var nrd = rd;
    nrd.cols = own;
    out[0].node = .{ .read = nrd };
    return out;
}

/// A joined side's SQL table read narrowed to what the join and the stages after it
/// may take from it (`needs`, names after the side's own stages, traced back through
/// them), kept to the table's own columns, as `needs` holds the other side's names
/// too. Unchanged when the side already asks for columns or anything is unprovable.
pub fn projectJoinedSqlRead(env: *Env, stages: []const ast.Stage, needs: []const []const u8) ![]const ast.Stage {
    if (stages.len == 0 or stages[0].node != .read) return stages;
    const rd = stages[0].node.read;
    if (rd.form != .table or rd.cols.len > 0) return stages;
    const conn = env.connections.get(rd.connector) orelse return stages;
    if (sqlConnInfo(conn) == null) return stages;
    const items = try env.arena.alloc(ast.SelectItem, needs.len);
    for (needs, items) |n, *it| {
        const parts = try env.arena.alloc([]const u8, 1);
        parts[0] = n;
        it.* = .{ .field = .{ .parts = parts } };
    }
    const tail = [_]ast.Stage{.{ .node = .{ .select = items }, .hints = &.{}, .pos = stages[0].pos }};
    const cols = (try projectedColumns(env, try std.mem.concat(env.arena, ast.Stage, &.{ stages[1..], &tail }))) orelse return stages;
    if (cols.len == 0) return stages;
    for (cols) |c| if (std.mem.indexOfScalar(u8, c, '.') != null) return stages;
    const own = (try tableColumnsAmong(env, rd, stages[0].hints, cols)) orelse return stages;
    if (own.len == 0) return stages;
    const out = try env.arena.dupe(ast.Stage, stages);
    var nrd = rd;
    nrd.cols = own;
    out[0].node = .{ .read = nrd };
    return out;
}

fn tableColumnsAmong(env: *Env, rd: ast.Read, hints: []const ast.Hint, names: []const []const u8) !?[]const []const u8 {
    var probe = rd;
    probe.where = "1 = 0";
    probe.cols = &.{};
    const src = openSourceProjected(env, probe, hints, null, &.{}) catch return null;
    defer src.close();
    var out = std.array_list.Managed([]const u8).init(env.arena);
    for (src.schema().fields) |f| {
        for (names) |n| if (std.ascii.eqlIgnoreCase(n, f.name)) {
            try out.append(try env.arena.dupe(u8, f.name));
            break;
        };
    }
    return try out.toOwnedSlice();
}

/// The names a `SELECT * EXCEPT (...)` right after the read leaves out, when that
/// select has no other star.
fn starExcept(after: []const ast.Stage) ?[]const []const u8 {
    if (after.len == 0 or after[0].node != .select) return null;
    var found: ?[]const []const u8 = null;
    for (after[0].node.select) |it| switch (it) {
        .star_except => |names| found = names,
        .star, .star_rename, .star_of => return null,
        else => {},
    };
    return found;
}

pub fn exceptColumns(env: *Env, rd: ast.Read, hints: []const ast.Hint, except: []const []const u8) !?[]const []const u8 {
    var probe = rd;
    probe.where = "1 = 0";
    probe.cols = &.{};
    const src = openSourceProjected(env, probe, hints, null, &.{}) catch return null;
    defer src.close();
    const schema = src.schema();
    var out = std.array_list.Managed([]const u8).init(env.arena);
    for (schema.fields) |f| {
        var drop = false;
        for (except) |x| if (std.ascii.eqlIgnoreCase(x, f.name)) {
            drop = true;
        };
        if (!drop) try out.append(try env.arena.dupe(u8, f.name));
    }
    if (out.items.len == 0 or out.items.len == schema.fields.len) return null;
    return try out.toOwnedSlice();
}

/// Table reads get a plain `WHERE`; query reads are wrapped as a subquery. An
/// empty predicate (a `${var}` that rendered empty) means a full scan.
pub fn sqlWithWhere(arena: std.mem.Allocator, base: []const u8, is_query: bool, where: []const u8) ![]const u8 {
    if (where.len == 0) return base;
    if (is_query) return std.fmt.allocPrint(arena, "SELECT * FROM ({s}) _w WHERE {s}", .{ base, where });
    return std.fmt.allocPrint(arena, "{s} WHERE {s}", .{ base, where });
}

pub fn sqlDescFor(env: *Env, kind: SqlKind, dialect: sql.Dialect, cfg: DbConfig, base_sql: []const u8, rd: ast.Read) !SqlDesc {
    const table: ?[]const u8 = switch (rd.form) {
        .table => |t| try qualStr(env.arena, t),
        else => null,
    };
    return .{ .kind = kind, .dialect = dialect, .cfg = cfg, .base_sql = base_sql, .table = table, .read = rd };
}

/// Recomputed rather than read from `env.sql_desc`, which is last-writer-wins: a
/// join plans its build side after the probe read. Null for a non-splittable read.
pub fn sqlDescForStage(env: *Env, stage: ast.Stage) !?SqlDesc {
    if (stage.node != .read) return null;
    var rd = stage.node.read;
    const conn = env.connections.get(rd.connector) orelse return null;
    const info = sqlConnInfo(conn) orelse return null;
    if (!pushableRead(info.dialect, rd)) return null;
    if (forHintName(stage.hints, "where")) |wh| {
        if (wh.len > 0 and !std.mem.eql(u8, wh, rd.where)) {
            rd.where = if (rd.where.len > 0)
                try std.fmt.allocPrint(env.arena, "({s}) AND ({s})", .{ wh, rd.where })
            else
                wh;
        }
    }
    const cfg = try resolveDbConfig(env, conn, info.port);
    return try sqlDescFor(env, info.kind, info.dialect, cfg, try readSql(env, rd), rd);
}

const SplitHints = struct { col: ?[]const u8 = null, count: ?usize = null, kind: ?split.KeyKind = null };

fn splitHints(stage: ast.Stage) SplitHints {
    var h = SplitHints{};
    for (stage.hints) |hint| {
        if (std.mem.eql(u8, hint.key, "split")) {
            if (hint.value == .ident) h.col = hint.value.ident;
        } else if (std.mem.eql(u8, hint.key, "splits")) {
            if (hint.value == .int and hint.value.int > 0) h.count = @intCast(hint.value.int);
        } else if (std.mem.eql(u8, hint.key, "split_kind")) {
            if (hint.value == .ident) h.kind = std.meta.stringToEnum(split.KeyKind, hint.value.ident);
        }
    }
    return h;
}

fn isPostgresCopySink(env: *Env, w: ast.Write) bool {
    return w.mode != .upsert and std.mem.eql(u8, connectorType(env, w.connector), "postgres");
}

/// Null (run serial) when the read is not splittable, no key is usable, or the
/// table is too small. A projection that dropped the split key takes it back.
pub fn planSplit(env: *Env, desc: SqlDesc, lead: ast.Stage, threads: usize, w: ast.Write) !?split.Plan {
    const hints = splitHints(lead);
    const forced = hints.col != null or hints.count != null;
    const m: usize = hints.count orelse @min(@as(usize, 64), threads * 4);
    if (m < 2) return null;
    if (!forced and isPostgresCopySink(env, w)) return null;

    var pctx = SplitCtx{ .gpa = env.gpa, .kind = desc.kind, .cfg = desc.cfg, .base_sql = desc.base_sql, .report = try readReport(env, @tagName(desc.kind)) };
    const prober = split.Prober{ .ctx = &pctx, .openFn = proberOpen };

    var key: split.Key = undefined;
    if (hints.col) |col| {
        key = .{ .col = col, .kind = hints.kind orelse .auto };
    } else if (desc.table) |table| {
        const info = (try split.introspectKey(env.arena, prober, desc.dialect, table)) orelse return null;
        if (!forced and info.est_rows < split.min_rows_to_split) return null;
        key = info.key;
    } else {
        return null;
    }
    var base = desc.base_sql;
    if (desc.read.cols.len > 0) {
        var has = false;
        for (desc.read.cols) |c| if (std.ascii.eqlIgnoreCase(c, key.col)) {
            has = true;
        };
        if (!has) {
            var rd = desc.read;
            const cols = try env.arena.alloc([]const u8, rd.cols.len + 1);
            @memcpy(cols[0..rd.cols.len], rd.cols);
            cols[rd.cols.len] = key.col;
            rd.cols = cols;
            base = try readSql(env, rd);
            pctx.base_sql = base;
        }
    }
    const p = try split.plan(env.arena, prober, desc.dialect, base, key, m);
    if (p == null) if (hints.col) |col| env.log.log(.warn, "SPLIT BY ({s}): no key ranges to cut, as the column is not an integer, date or timestamp with more than one value; reading on one lane", .{col});
    return p;
}

pub fn proberOpen(ctx_ptr: *anyopaque) anyerror!sql.Conn {
    const ctx: *SplitCtx = @ptrCast(@alignCast(ctx_ptr));
    return connectSql(ctx.gpa, ctx.kind, ctx.cfg);
}

test "sqlWithWhere: table appends WHERE, query wraps, empty is a no-op" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings(
        "SELECT * FROM SC1010 WHERE S_T_A_M_P_ >= '2026-05-09'",
        try sqlWithWhere(a, "SELECT * FROM SC1010", false, "S_T_A_M_P_ >= '2026-05-09'"),
    );
    try std.testing.expectEqualStrings(
        "SELECT * FROM (SELECT id FROM t WHERE x = 1) _w WHERE id > 5",
        try sqlWithWhere(a, "SELECT id FROM t WHERE x = 1", true, "id > 5"),
    );
    try std.testing.expectEqualStrings(
        "SELECT * FROM SC1010",
        try sqlWithWhere(a, "SELECT * FROM SC1010", false, ""),
    );
}

test "a query read's trailing semicolons are dropped before any pushdown wraps it" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    try std.testing.expectEqualStrings("SELECT * FROM (SELECT id FROM t) _w WHERE id > 5", try sqlWithWhere(ar.allocator(), queryText("SELECT id FROM t;  \n"), true, "id > 5"));
    try std.testing.expectEqualStrings("SELECT ';'", queryText("SELECT ';' ;;"));
}

test "a projected SQL read asks for its columns, quoted per dialect; none means *" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings("*", try selectListFor(a, .sqlserver, &.{}));
    try std.testing.expectEqualStrings("[E1_NUM], [E1_VALOR]", try selectListFor(a, .sqlserver, &.{ "E1_NUM", "E1_VALOR" }));
    try std.testing.expectEqualStrings("\"id\", \"amount\"", try selectListFor(a, .postgres, &.{ "id", "amount" }));
    try std.testing.expectEqualStrings("`id`", try selectListFor(a, .mysql, &.{"id"}));
}

test "a dotted or quote-bearing column name stays one quoted name in a read" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings("\"a.b\"", try selectListFor(a, .postgres, &.{"a.b"}));
    try std.testing.expectEqualStrings("[a.b], [x]]y]", try selectListFor(a, .sqlserver, &.{ "a.b", "x]y" }));
}

test "a SQL Server query with a leading WITH or a top-level ORDER BY is never wrapped" {
    const q = struct {
        fn of(text: []const u8) ast.Read {
            return .{ .connector = "db", .form = .{ .query = text } };
        }
    }.of;
    try std.testing.expect(pushableRead(.sqlserver, q("SELECT id FROM t WHERE x = 1")));
    try std.testing.expect(!pushableRead(.sqlserver, q("  WITH c AS (SELECT 1 AS id) SELECT id FROM c")));
    try std.testing.expect(!pushableRead(.sqlserver, q("-- note\nwith c AS (SELECT 1 AS id) SELECT id FROM c;")));
    try std.testing.expect(!pushableRead(.sqlserver, q("SELECT id FROM t ORDER BY id")));
    try std.testing.expect(pushableRead(.sqlserver, q("SELECT id FROM (SELECT TOP 5 id FROM t ORDER BY id) s")));
    try std.testing.expect(pushableRead(.sqlserver, q("SELECT 'ORDER BY x' AS s, [order by] FROM t")));
    try std.testing.expect(pushableRead(.postgres, q("WITH c AS (SELECT 1) SELECT * FROM c ORDER BY 1")));
}
