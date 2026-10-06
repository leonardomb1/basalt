//! What a SQL table's catalog says of its columns (types, collation, nullability),
//! asked once per run and cached, for the pushdown rules.

const Env = @import("../env.zig").Env;
const SqlConnInfo = @import("../connect.zig").SqlConnInfo;
const Value = @import("../../exec/value.zig").Value;
const ast = @import("../../lang/ast.zig");
const connectSql = @import("../connect.zig").connectSql;
const pushdown = @import("../pushdown.zig");
const resolveDbConfig = @import("dbconfig.zig").resolveDbConfig;
const sql = @import("../../db/sql.zig");
const sqlConnInfo = @import("../connect.zig").sqlConnInfo;
const std = @import("std");
const types = @import("../../lang/types.zig");

/// Per-column text and collation facts that decide which text comparisons may
/// descend. A catalog that cannot be asked answers nothing, keeping them in the engine.
pub fn columnFacts(env: *Env, rd: ast.Read) !?*const pushdown.Facts {
    if (rd.form != .table) return null;
    const conn = env.connections.get(rd.connector) orelse return null;
    const info = sqlConnInfo(conn) orelse return null;
    const cache = env.facts_cache orelse return null;
    cache.mu.lock();
    defer cache.mu.unlock();
    const ca = cache.arena.allocator();
    const key = try std.fmt.allocPrint(ca, "{s}\x00{s}", .{ rd.connector, try qualStr(ca, rd.form.table) });
    if (cache.map.get(key)) |f| return f;

    const facts = try ca.create(pushdown.Facts);
    facts.* = pushdown.Facts.init(ca);
    try cache.map.put(key, facts);
    probeFacts(env, ca, conn, info, rd.form.table.parts, facts) catch |e| {
        if (e == error.OutOfMemory) return e;
        facts.clearRetainingCapacity();
        env.log.log(.debug, "catalog facts for {s}: not available ({s}); text comparisons stay in the engine", .{ key[rd.connector.len + 1 ..], @errorName(e) });
    };
    return facts;
}

pub fn factsIfWanted(env: *Env, rd: ast.Read, dialect: sql.Dialect, stages: []const ast.Stage, schema: types.Schema, check_fields: bool, need: pushdown.Need) !?*const pushdown.Facts {
    if (rd.form != .table) return null;
    if (!try pushdown.wantsFacts(env.arena, dialect, stages, schema, check_fields, need)) return null;
    return columnFacts(env, rd);
}

/// Restores the run's diag, so a failed probe leaves no message behind.
fn probeFacts(env: *Env, ca: std.mem.Allocator, conn: ast.Connection, info: SqlConnInfo, parts: []const []const u8, out: *pushdown.Facts) !void {
    const saved = env.diag.*;
    defer env.diag.* = saved;
    const cfg = try resolveDbConfig(env, conn, info.port);
    const q = try factsQuery(env.arena, info.dialect, parts);
    const c = try connectSql(env.gpa, info.kind, cfg);
    var cur = c.queryCursor(q) catch |e| {
        c.close();
        return e;
    };
    defer cur.close();
    var scratch = std.heap.ArenaAllocator.init(env.gpa);
    defer scratch.deinit();
    while (try cur.nextBatch(scratch.allocator())) |b| {
        var r: usize = 0;
        while (r < b.len) : (r += 1) {
            const name = b.columns[0].getValue(r);
            if (name.isNull()) continue;
            try out.put(try ca.dupe(u8, name.string), .{
                .text = flag(b.columns[1].getValue(r)),
                .byte_order = flag(b.columns[2].getValue(r)),
                .pads = flag(b.columns[3].getValue(r)),
                .wide = flag(b.columns[4].getValue(r)),
            });
        }
    }
}

fn flag(v: Value) bool {
    return switch (v) {
        .int => |i| i != 0,
        .bool => |x| x,
        .string => |s| std.mem.eql(u8, std.mem.trim(u8, s, " "), "1"),
        else => false,
    };
}

/// One row per column: name, is text, compares by byte, ignores trailing spaces,
/// multi-byte. Byte order is judged from the server's own collation data (C/POSIX,
/// builtin, `_bin`/`_BIN2`), never a locale's name; musl libc locales compare bytes.
fn factsQuery(arena: std.mem.Allocator, dialect: sql.Dialect, parts: []const []const u8) ![]const u8 {
    const table = parts[parts.len - 1];
    switch (dialect) {
        .sqlserver => {
            var obj = std.array_list.Managed(u8).init(arena);
            for (parts, 0..) |p, i| {
                if (i > 0) try obj.append('.');
                try obj.append('[');
                for (p) |ch| if (ch == ']') try obj.appendSlice("]]") else try obj.append(ch);
                try obj.append(']');
            }
            return std.fmt.allocPrint(arena,
                \\SELECT c.name,
                \\ CASE WHEN c.collation_name IS NULL THEN 0 ELSE 1 END,
                \\ CASE WHEN c.collation_name LIKE '%[_]BIN2%' THEN 1
                \\      WHEN c.collation_name LIKE '%[_]BIN' AND TYPE_NAME(c.system_type_id) IN ('char', 'varchar', 'text') THEN 1 ELSE 0 END,
                \\ 1,
                \\ CASE WHEN TYPE_NAME(c.system_type_id) IN ('nchar', 'nvarchar', 'ntext') THEN 1 ELSE 0 END
                \\FROM sys.columns c WHERE c.object_id = OBJECT_ID({s})
            , .{try sqlLit(arena, obj.items)});
        },
        .postgres => {
            var obj = std.array_list.Managed(u8).init(arena);
            for (parts, 0..) |p, i| {
                if (i > 0) try obj.append('.');
                try obj.append('"');
                for (p) |ch| if (ch == '"') try obj.appendSlice("\"\"") else try obj.append(ch);
                try obj.append('"');
            }
            return std.fmt.allocPrint(arena,
                \\SELECT a.attname,
                \\ CASE WHEN t.typcategory = 'S' THEN 1 ELSE 0 END,
                \\ CASE WHEN t.typcategory <> 'S' OR t.typname = 'citext' THEN 0
                \\      WHEN co.collname IN ('C', 'POSIX', 'ucs_basic') THEN 1
                \\      WHEN prov = 'b' THEN 1
                \\      WHEN prov <> 'c' THEN 0
                \\      WHEN version() LIKE '%-musl%' THEN 1
                \\      WHEN locale IN ('C', 'POSIX', 'C.UTF-8', 'C.utf8') THEN 1
                \\      ELSE 0 END,
                \\ CASE WHEN t.typname = 'bpchar' THEN 1 ELSE 0 END,
                \\ 0
                \\FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid
                \\LEFT JOIN pg_collation co ON co.oid = a.attcollation
                \\CROSS JOIN pg_database d
                \\CROSS JOIN LATERAL (SELECT
                \\   CASE WHEN co.collname = 'default' THEN coalesce(to_jsonb(d) ->> 'datlocprovider', 'c')
                \\        ELSE coalesce(to_jsonb(co) ->> 'collprovider', 'c') END AS prov,
                \\   CASE WHEN co.collname = 'default' THEN d.datcollate ELSE to_jsonb(co) ->> 'collcollate' END AS locale) x
                \\WHERE a.attrelid = {s}::regclass AND a.attnum > 0 AND NOT a.attisdropped AND d.datname = current_database()
            , .{try sqlLit(arena, obj.items)});
        },
        .mysql => {
            const schema = if (parts.len >= 2) try sqlLit(arena, parts[parts.len - 2]) else "DATABASE()";
            return std.fmt.allocPrint(arena,
                \\SELECT c.COLUMN_NAME,
                \\ CASE WHEN c.COLLATION_NAME IS NULL THEN 0 ELSE 1 END,
                \\ CASE WHEN c.COLLATION_NAME = 'binary' OR RIGHT(c.COLLATION_NAME, 4) = '_bin' THEN 1 ELSE 0 END,
                \\ CASE WHEN co.PAD_ATTRIBUTE = 'NO PAD' THEN 0 ELSE 1 END,
                \\ CASE WHEN c.CHARACTER_SET_NAME IN ('ucs2', 'utf16', 'utf16le', 'utf32') THEN 1 ELSE 0 END
                \\FROM information_schema.COLUMNS c LEFT JOIN information_schema.COLLATIONS co ON co.COLLATION_NAME = c.COLLATION_NAME
                \\WHERE c.TABLE_SCHEMA = {s} AND c.TABLE_NAME = {s}
            , .{ schema, try sqlLit(arena, table) });
        },
        .starrocks, .doris => {
            const schema = if (parts.len >= 2) try sqlLit(arena, parts[parts.len - 2]) else "DATABASE()";
            return std.fmt.allocPrint(arena,
                \\SELECT COLUMN_NAME,
                \\ CASE WHEN LOWER(DATA_TYPE) IN ('varchar', 'char', 'string') THEN 1 ELSE 0 END,
                \\ 1,
                \\ CASE WHEN LOWER(DATA_TYPE) = 'char' THEN 1 ELSE 0 END,
                \\ 0
                \\FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = {s} AND TABLE_NAME = {s}
            , .{ schema, try sqlLit(arena, table) });
        },
    }
}

fn sqlLit(arena: std.mem.Allocator, s: []const u8) ![]const u8 {
    var out = std.array_list.Managed(u8).init(arena);
    try out.append('\'');
    for (s) |c| {
        if (c == '\'') try out.append('\'');
        try out.append(c);
    }
    try out.append('\'');
    return out.toOwnedSlice();
}

pub fn qualStr(arena: std.mem.Allocator, q: ast.QualName) ![]const u8 {
    if (q.parts.len == 1) return q.parts[0];
    return std.mem.join(arena, ".", q.parts);
}
