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
        .query => |q| q,
        .table => |t| try std.fmt.allocPrint(env.arena, "SELECT {s} FROM {s}", .{ try selectList(env, rd), try qualStr(env.arena, t) }),
        else => return planErr(env.diag, "a DB read needs `table <name>` or `query \"...\"`"),
    };
    return sqlWithWhere(env.arena, base, rd.form == .query, rd.where);
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
        try out.appendSlice(try sql.quoteIdent(arena, dialect, c));
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
        .star, .star_rename => return null,
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
    if (rd.form != .table and rd.form != .query) return null;
    const conn = env.connections.get(rd.connector) orelse return null;
    const info = sqlConnInfo(conn) orelse return null;
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
        key = .{ .col = col, .kind = hints.kind orelse .int };
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
    return split.plan(env.arena, prober, desc.dialect, base, key, m);
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

test "a projected SQL read asks for its columns, quoted per dialect; none means *" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings("*", try selectListFor(a, .sqlserver, &.{}));
    try std.testing.expectEqualStrings("[E1_NUM], [E1_VALOR]", try selectListFor(a, .sqlserver, &.{ "E1_NUM", "E1_VALOR" }));
    try std.testing.expectEqualStrings("\"id\", \"amount\"", try selectListFor(a, .postgres, &.{ "id", "amount" }));
    try std.testing.expectEqualStrings("`id`", try selectListFor(a, .mysql, &.{"id"}));
}
