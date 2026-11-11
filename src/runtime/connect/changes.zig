//! A SQL table read `WITH (changes_from = <position>)` as its change feed: the rows
//! committed after the position, each with the table's columns and the change's
//! `_change_type`, `_commit_version`, `_commit_timestamp` and `_change_seq`, as a
//! Delta table's change feed reads. The read becomes a query the server runs; the
//! rest of the plan (its WHERE, its projection, EXPLAIN) treats it as any query.
//!
//! SQL Server (CDC): the table's capture instance from `cdc.change_tables`, the
//! newest when it has two; its LSN range from `sys.fn_cdc_get_min_lsn` and
//! `sys.fn_cdc_get_max_lsn`, the end fixed as the run starts (or `changes_to`), so a
//! rerun reads the same rows. A position is the LSN as 20 uppercase hex digits,
//! which order as the LSNs do. The changes are read from the instance's change
//! table, its update before-images left out; `_change_seq` numbers them in commit
//! order, then order within the commit, which is all `sequence_by` compares. A
//! position the cleanup job has already passed fails the read rather than skip
//! changes (`USING changes` asks `cleanedPast` first and reads the table whole
//! instead), and so does a database or table without CDC, naming the statements
//! that enable it.
//!
//! With no position (`changes_from` NULL or empty, a feed's first run) the whole
//! table is read as inserts at the current end: a change made during that read is
//! after the end, read again by the next run, and applied by key it leaves the same
//! rows.

const std = @import("std");
const ast = @import("../../lang/ast.zig");
const sql = @import("../../db/sql.zig");
const split = @import("../../connect/split.zig");
const msgfmt = @import("../../format/msgfmt.zig");
const fmtnum = @import("../../exec/eval/format.zig");
const Env = @import("../env.zig").Env;
const SqlConnInfo = @import("../connect.zig").SqlConnInfo;
const catalogQuery = @import("../plan/estimate.zig").catalogQuery;
const planErr = @import("../env.zig").planErr;
const changes_pg = @import("changes_pg.zig");

/// Whether read options `hints` ask for the change feed.
pub fn asked(hints: []const ast.Hint) bool {
    for (hints) |h| if (std.mem.eql(u8, h.key, "changes_from")) return true;
    return false;
}

/// What the run learnt of a table's change feed, asked once per statement.
pub const Meta = struct { instance: []const u8, cols: []const []const u8, min: []const u8, max: []const u8 };

/// Read `rd` as the query that reads its changes over the position range its
/// options give.
pub fn read(env: *Env, rd: ast.Read, hints: []const ast.Hint, conn: ast.Connection, info: SqlConnInfo) !ast.Read {
    if (rd.form != .table) return planErr(env.diag, "`changes_from` reads a table's change feed; give the table, not a query");
    if (info.dialect == .postgres) return changes_pg.read(env, rd, hints, conn, info);
    if (info.dialect != .sqlserver) return planErr(env.diag, try msgfmt.allocPrint(env.arena, "`changes_from` on a {s} table is not read yet; a SQL Server table with CDC or a PostgreSQL table with logical decoding is", .{@tagName(info.dialect)}));
    const from = try position(env, hints, "changes_from");
    const to_given = try position(env, hints, "changes_to");
    const m = try metaOf(env, rd, hints, conn, info);
    if (from) |f| if (m.min.len > 0 and std.mem.order(u8, f, m.min) == .lt and !zeroes(m.min))
        return planErr(env.diag, try msgfmt.allocPrint(env.arena, "the change feed of {s} starts at {s}, after the position {s} this read continues from: the cleanup job has removed the changes between, so read the table whole again (no position)", .{ try label(env, rd), m.min, f }));
    const to = to_given orelse m.max;
    const a = env.arena;
    var cols = std.array_list.Managed(u8).init(a);
    for (m.cols, 0..) |c, i| {
        if (i > 0) try cols.appendSlice(", ");
        try cols.appendSlice(try sql.quoteName(a, .sqlserver, c));
    }
    var out = rd;
    out.cols = &.{};
    const q = if (from == null)
        try msgfmt.allocPrint(a, "SELECT {s}, CAST('insert' AS VARCHAR(16)) AS _change_type, CAST('{s}' AS CHAR(20)) AS _commit_version, sys.fn_cdc_map_lsn_to_time(0x{s}) AS _commit_timestamp, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS _change_seq FROM {s}", .{ cols.items, to, to, try sql.quoteParts(a, .sqlserver, rd.form.table.parts) })
    else
        try msgfmt.allocPrint(a, "SELECT {s}, CAST(CASE __$operation WHEN 1 THEN 'delete' WHEN 2 THEN 'insert' ELSE 'update_postimage' END AS VARCHAR(16)) AS _change_type, CONVERT(CHAR(20), __$start_lsn, 2) AS _commit_version, sys.fn_cdc_map_lsn_to_time(__$start_lsn) AS _commit_timestamp, ROW_NUMBER() OVER (ORDER BY __$start_lsn, __$seqval, __$operation) AS _change_seq FROM cdc.{s} WHERE __$start_lsn > 0x{s} AND __$start_lsn <= 0x{s} AND __$operation <> 3", .{ cols.items, try sql.quoteName(a, .sqlserver, try std.mem.concat(a, u8, &.{ m.instance, "_CT" })), from.?, to });
    out.form = .{ .query = q };
    return out;
}

/// Whether the change feed `rd` reads starts after position `since`: the cleanup
/// job has removed changes past it, so reading on from it could skip some. False
/// for a read that is no SQL Server table's.
pub fn cleanedPast(env: *Env, rd: ast.Read, hints: []const ast.Hint, conn: ast.Connection, info: SqlConnInfo, since: []const u8) !bool {
    if (rd.form == .table and info.dialect == .postgres) return changes_pg.cleanedPast(env, rd, hints, conn, info, since);
    if (rd.form != .table or info.dialect != .sqlserver) return false;
    const f = (try lsn(env, "changes_from", since)) orelse return false;
    const m = try metaOf(env, rd, hints, conn, info);
    return m.min.len > 0 and !zeroes(m.min) and std.mem.order(u8, f, m.min) == .lt;
}

fn zeroes(s: []const u8) bool {
    for (s) |c| if (c != '0') return false;
    return true;
}

fn label(env: *Env, rd: ast.Read) ![]const u8 {
    return std.mem.join(env.arena, ".", rd.form.table.parts);
}

/// A position option's value as 20 uppercase hex digits (a leading `0x` dropped);
/// null when the option is absent, NULL or empty.
fn position(env: *Env, hints: []const ast.Hint, key: []const u8) !?[]const u8 {
    for (hints) |h| if (std.mem.eql(u8, h.key, key)) {
        const text = switch (h.value) {
            .str, .ident => |s| s,
            .flag => return null,
            .int => return planErr(env.diag, try msgfmt.allocPrint(env.arena, "`{s}` on a SQL Server table is an LSN as text, 20 hex digits (`0000002B000004280003`), as `_commit_version` reads it", .{key})),
        };
        return lsn(env, key, text);
    };
    return null;
}

/// `text` as 20 uppercase hex digits (a leading `0x` dropped); null when empty.
fn lsn(env: *Env, key: []const u8, text: []const u8) !?[]const u8 {
    const t = if (std.ascii.startsWithIgnoreCase(text, "0x")) text[2..] else text;
    if (t.len == 0) return null;
    if (t.len != 20) return planErr(env.diag, try msgfmt.allocPrint(env.arena, "`{s} = '{s}'` is not an LSN: it takes 20 hex digits, as `_commit_version` reads it", .{ key, text }));
    const up = try env.arena.alloc(u8, 20);
    for (t, up) |c, *o| {
        if (!std.ascii.isHex(c)) return planErr(env.diag, try msgfmt.allocPrint(env.arena, "`{s} = '{s}'` is not an LSN: it takes 20 hex digits, as `_commit_version` reads it", .{ key, text }));
        o.* = std.ascii.toUpper(c);
    }
    return up;
}

/// The table's capture instance, its captured columns and its LSN range, asked of
/// the server once per statement.
fn metaOf(env: *Env, rd: ast.Read, hints: []const ast.Hint, conn: ast.Connection, info: SqlConnInfo) !Meta {
    const a = env.arena;
    const key = try std.mem.concat(a, u8, &.{ rd.connector, "\x00", try label(env, rd) });
    for (env.change_meta.items) |m| if (std.mem.eql(u8, m.key, key)) return m.meta;
    const obj = try split.catalogObjectLit(a, .sqlserver, rd.form.table.parts);
    var picked: ?[]const u8 = null;
    for (hints) |h| if (std.mem.eql(u8, h.key, "capture_instance")) {
        picked = switch (h.value) {
            .str, .ident => |s| s,
            else => null,
        };
    };
    const where_instance = if (picked) |p| try msgfmt.allocPrint(a, " AND ct.capture_instance = N'{s}'", .{p}) else "";
    const q = try msgfmt.allocPrint(a, "SELECT ct.capture_instance, CONVERT(CHAR(20), sys.fn_cdc_get_min_lsn(ct.capture_instance), 2), CONVERT(CHAR(20), sys.fn_cdc_get_max_lsn(), 2) FROM cdc.change_tables ct WHERE ct.source_object_id = OBJECT_ID({s}){s} ORDER BY ct.create_date DESC", .{ obj, where_instance });
    const rows = askRows(env, conn, info, q) catch |e| {
        if (e == error.OutOfMemory) return e;
        return planErr(env.diag, try msgfmt.allocPrint(a, "{s} has no change data capture to read (CDC is not enabled on its database); enable it with `EXEC sys.sp_cdc_enable_db;` and then `EXEC sys.sp_cdc_enable_table @source_schema = N'<schema>', @source_name = N'<table>', @role_name = NULL;`, with SQL Server Agent running", .{try label(env, rd)}));
    };
    if (rows.len == 0) {
        if (picked) |p| return planErr(env.diag, try msgfmt.allocPrint(a, "{s} has no capture instance `{s}`", .{ try label(env, rd), p }));
        return planErr(env.diag, try msgfmt.allocPrint(a, "{s} is not tracked by change data capture; enable it with `EXEC sys.sp_cdc_enable_table @source_schema = N'<schema>', @source_name = N'<table>', @role_name = NULL;`", .{try label(env, rd)}));
    }
    const instance = rows[0][0];
    const cq = try msgfmt.allocPrint(a, "SELECT cc.column_name FROM cdc.captured_columns cc JOIN cdc.change_tables ct ON cc.object_id = ct.object_id WHERE ct.capture_instance = N'{s}' ORDER BY cc.column_ordinal", .{instance});
    const crows = try askRows(env, conn, info, cq);
    const cols = try a.alloc([]const u8, crows.len);
    for (crows, cols) |r, *c| c.* = r[0];
    const meta = Meta{ .instance = instance, .cols = cols, .min = rows[0][1], .max = if (rows[0][2].len > 0) rows[0][2] else "00000000000000000000" };
    try env.change_meta.append(a, .{ .key = key, .meta = meta });
    return meta;
}

/// The rows of catalog query `q`, each cell as text, NULL as empty.
pub fn askRows(env: *Env, conn: ast.Connection, info: SqlConnInfo, q: []const u8) ![]const []const []const u8 {
    const Ctx = struct { a: std.mem.Allocator, out: *std.array_list.Managed([]const []const u8) };
    var out = std.array_list.Managed([]const []const u8).init(env.arena);
    try catalogQuery(env, conn, info, q, Ctx{ .a = env.arena, .out = &out }, struct {
        fn rows(cx: Ctx, b: anytype) !void {
            for (0..b.len) |r| {
                const row = try cx.a.alloc([]const u8, b.columns.len);
                for (b.columns, row) |c, *cell| {
                    const v = c.getValue(r);
                    cell.* = if (v.isNull()) "" else try cx.a.dupe(u8, std.mem.trim(u8, try fmtnum.valueToString(cx.a, v), " "));
                }
                try cx.out.append(row);
            }
        }
    }.rows);
    return out.items;
}
