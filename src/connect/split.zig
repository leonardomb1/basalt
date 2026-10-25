//! Split planning for parallel source reads. A split is a SQL boolean predicate
//! over a key column; the set of splits is disjoint and covering over the key's
//! `[min,max]` captured at plan time, so each lane reads one key range on its own
//! connection, wrapping `Plan.base_sql` (which may carry a key column the read's
//! projection left out) as `SELECT * FROM (<base>) _split WHERE <pred>`.
//!
//! This is an unsynchronized partitioned read: a row present and unchanged for the
//! whole read appears exactly once, but concurrent writes are fuzzy and re-runs
//! repeat rows; downstream dedup (a primary-key table or MERGE) owns exactly-once.
//! See `runtime/parallel.zig`.
//!
//! Ranges are half-open and the last one is open-ended, so rows inserted past `max`
//! after the probe still land in a lane. Slice 0 also claims NULL keys (`OR col IS
//! NULL`): every range test is UNKNOWN for NULL, so a nullable split column once
//! made those rows match no lane and vanish from a `-j > 1` read. Date keys (DATE and
//! DATETIME/TIMESTAMP alike) are sliced by day with date literals, which every dialect
//! compares against either type. UUID keys are sliced evenly over the 128-bit space,
//! balanced for random v4 ids with no bounds probe.
//!
//! Below `min_rows_to_split` estimated rows the planner stays serial: each lane
//! reconnects, and that setup costs more than it saves. An explicit @[split] or
//! @[splits] overrides. Each probe consumes the connection `Prober` opens for it.

const std = @import("std");
const sql = @import("../db/sql.zig");
const civil = @import("../exec/civil.zig");
const uuid = @import("../format/uuid.zig");
const eval = @import("../exec/eval.zig");
const Value = @import("../exec/value.zig").Value;

const Conn = sql.Conn;
const Dialect = sql.Dialect;

/// `auto` is a `SPLIT BY (col)` that names no kind: one MIN/MAX probe tells an
/// integer key from a date or timestamp one.
pub const KeyKind = enum { int, uuid, date, auto };
pub const Key = struct { col: []const u8, kind: KeyKind };
pub const KeyInfo = struct { key: Key, est_rows: i64 };

pub const min_rows_to_split: i64 = 2_000_000;

pub const Prober = struct {
    ctx: *anyopaque,
    openFn: *const fn (ctx: *anyopaque) anyerror!Conn,

    fn open(self: Prober) !Conn {
        return self.openFn(self.ctx);
    }
};

pub const Plan = struct {
    key: Key,
    predicates: []const []const u8,
    base_sql: []const u8,
};

pub fn wrap(arena: std.mem.Allocator, base: []const u8, pred: []const u8) ![]const u8 {
    return wrapProjected(arena, base, null, pred, null);
}

/// `wrap` with pushdown: `proj` (null → `*`) is a comma-joined column list and
/// `extra` a filter AND-ed onto `pred`. Both see every base column inside the
/// subquery, so a pushed filter may test a column the projection drops.
pub fn wrapProjected(arena: std.mem.Allocator, base: []const u8, proj: ?[]const u8, pred: []const u8, extra: ?[]const u8) ![]const u8 {
    const sel = proj orelse "*";
    if (extra) |x|
        return std.fmt.allocPrint(arena, "SELECT {s} FROM ({s}) _split WHERE {s} AND ({s})", .{ sel, base, pred, x });
    return std.fmt.allocPrint(arena, "SELECT {s} FROM ({s}) _split WHERE {s}", .{ sel, base, pred });
}

/// Every primary-key column name for the table `parts` names, in key order, or
/// empty when there is none. Used to infer upsert keys from the source.
pub fn introspectPkCols(arena: std.mem.Allocator, prober: Prober, dialect: Dialect, parts: []const []const u8) ![]const []const u8 {
    const lit = try catalogLit(arena, dialect, parts);
    const query = switch (dialect) {
        .postgres => try std.fmt.allocPrint(arena,
            \\SELECT a.attname
            \\FROM pg_index i
            \\JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            \\WHERE i.indrelid = {s}::regclass AND i.indisprimary
            \\ORDER BY (SELECT k FROM generate_subscripts(i.indkey, 1) k WHERE i.indkey[k] = a.attnum)
        , .{lit}),
        .sqlserver => try std.fmt.allocPrint(arena,
            \\SELECT c.name
            \\FROM sys.indexes i
            \\JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            \\JOIN sys.columns c ON c.object_id = i.object_id AND c.column_id = ic.column_id
            \\WHERE i.object_id = OBJECT_ID({s}) AND i.is_primary_key = 1
            \\ORDER BY ic.key_ordinal
        , .{lit}),
        .mysql, .starrocks, .doris => try std.fmt.allocPrint(arena,
            \\SELECT k.COLUMN_NAME
            \\FROM information_schema.KEY_COLUMN_USAGE k
            \\WHERE k.CONSTRAINT_NAME = 'PRIMARY' AND {s}
            \\ORDER BY k.ORDINAL_POSITION
        , .{lit}),
    };
    const conn = prober.open() catch return &.{};
    var cur = conn.queryCursor(query) catch {
        conn.close();
        return &.{};
    };
    defer cur.close();
    var cols = std.array_list.Managed([]const u8).init(arena);
    while (try cur.nextBatch(arena)) |b| {
        var r: usize = 0;
        while (r < b.len) : (r += 1) {
            const v = b.columns[0].getValue(r);
            if (!v.isNull()) try cols.append(try arena.dupe(u8, v.string));
        }
    }
    return cols.toOwnedSlice();
}

pub fn introspectKey(arena: std.mem.Allocator, prober: Prober, dialect: Dialect, parts: []const []const u8) !?KeyInfo {
    const lit = try catalogLit(arena, dialect, parts);
    const query = switch (dialect) {
        .postgres => try std.fmt.allocPrint(arena,
            \\SELECT a.attname, t.typname, c.reltuples::bigint
            \\FROM pg_index i
            \\JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            \\JOIN pg_type t ON t.oid = a.atttypid
            \\JOIN pg_class c ON c.oid = i.indrelid
            \\WHERE i.indrelid = {s}::regclass AND i.indisprimary
        , .{lit}),
        .sqlserver => try std.fmt.allocPrint(arena,
            \\SELECT c.name, ty.name, p.rows
            \\FROM sys.indexes i
            \\JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            \\JOIN sys.columns c ON c.object_id = i.object_id AND c.column_id = ic.column_id
            \\JOIN sys.types ty ON ty.user_type_id = c.user_type_id
            \\JOIN (SELECT object_id, SUM(rows) AS rows FROM sys.partitions WHERE index_id IN (0,1) GROUP BY object_id) p ON p.object_id = i.object_id
            \\WHERE i.object_id = OBJECT_ID({s}) AND i.is_primary_key = 1
            \\ORDER BY ic.key_ordinal
        , .{lit}),
        .mysql, .starrocks, .doris => try std.fmt.allocPrint(arena,
            \\SELECT k.COLUMN_NAME, c.DATA_TYPE, t.TABLE_ROWS
            \\FROM information_schema.KEY_COLUMN_USAGE k
            \\JOIN information_schema.COLUMNS c ON c.TABLE_SCHEMA = k.TABLE_SCHEMA AND c.TABLE_NAME = k.TABLE_NAME AND c.COLUMN_NAME = k.COLUMN_NAME
            \\JOIN information_schema.TABLES t ON t.TABLE_SCHEMA = k.TABLE_SCHEMA AND t.TABLE_NAME = k.TABLE_NAME
            \\WHERE k.CONSTRAINT_NAME = 'PRIMARY' AND {s}
            \\ORDER BY k.ORDINAL_POSITION
        , .{lit}),
    };
    const conn = prober.open() catch return null;
    var cur = conn.queryCursor(query) catch {
        conn.close();
        return null;
    };
    defer cur.close();
    const b = (try cur.nextBatch(arena)) orelse return null;
    if (b.len != 1) return null;
    const name = b.columns[0].getValue(0);
    const typ = b.columns[1].getValue(0);
    if (name.isNull() or typ.isNull()) return null;
    const kind = keyKindFor(typ.string) orelse return null;
    const est = b.columns[2].getValue(0);
    const rows: i64 = if (est == .int) est.int else 0;
    return KeyInfo{ .key = .{ .col = try arena.dupe(u8, name.string), .kind = kind }, .est_rows = rows };
}

fn keyKindFor(typname: []const u8) ?KeyKind {
    const ints = [_][]const u8{
        "int2", "int4",   "int8",     "serial",  "bigserial", "smallserial",
        "int",  "bigint", "smallint", "tinyint", "mediumint",
    };
    for (ints) |t| if (std.mem.eql(u8, typname, t)) return .int;
    const dates = [_][]const u8{
        "date",     "timestamp", "timestamptz",
        "datetime", "datetime2", "smalldatetime",
    };
    for (dates) |t| if (std.mem.eql(u8, typname, t)) return .date;
    if (std.mem.eql(u8, typname, "uuid") or std.mem.eql(u8, typname, "uniqueidentifier")) return .uuid;
    return null;
}

/// Up to `m` split predicates for `key` over the unsplit `base`, or null when the
/// source is empty or the key has no usable bounds, so the caller reads serially.
pub fn plan(arena: std.mem.Allocator, prober: Prober, dialect: Dialect, base: []const u8, key: Key, m: usize) !?Plan {
    if (m <= 1) return null;
    switch (key.kind) {
        .int => {
            const b = (try intBounds(arena, prober, dialect, base, key.col)) orelse return null;
            const preds = try intRangePreds(arena, dialect, key.col, b.min, b.max, m);
            if (preds.len <= 1) return null;
            return Plan{ .key = key, .predicates = preds, .base_sql = base };
        },
        .uuid => {
            if (dialect != .postgres) return null;
            if (!(try hasAnyRow(arena, prober, base))) return null;
            const preds = try uuidSpacePreds(arena, dialect, key.col, m);
            return Plan{ .key = key, .predicates = preds, .base_sql = base };
        },
        .date => {
            const b = (try dateBounds(arena, prober, dialect, base, key.col)) orelse return null;
            const preds = try dateRangePreds(arena, dialect, key.col, b.min, b.max, m);
            if (preds.len <= 1) return null;
            return Plan{ .key = key, .predicates = preds, .base_sql = base };
        },
        .auto => {
            const lh = (try minMax(arena, prober, dialect, base, key.col)) orelse return null;
            if (lh[0] == .int and lh[1] == .int) {
                if (lh[1].int <= lh[0].int) return null;
                const preds = try intRangePreds(arena, dialect, key.col, lh[0].int, lh[1].int, m);
                if (preds.len <= 1) return null;
                return Plan{ .key = .{ .col = key.col, .kind = .int }, .predicates = preds, .base_sql = base };
            }
            const lo = dayOf(lh[0]) orelse return null;
            const hi = dayOf(lh[1]) orelse return null;
            if (hi <= lo) return null;
            const preds = try dateRangePreds(arena, dialect, key.col, lo, hi, m);
            if (preds.len <= 1) return null;
            return Plan{ .key = .{ .col = key.col, .kind = .date }, .predicates = preds, .base_sql = base };
        },
    }
}

/// The key's MIN and MAX over `base`, as the driver types them; null when the
/// probe fails or the table is empty.
fn minMax(arena: std.mem.Allocator, prober: Prober, dialect: Dialect, base: []const u8, col: []const u8) !?[2]Value {
    const q = try std.fmt.allocPrint(arena, "SELECT MIN({0s}) AS lo, MAX({0s}) AS hi FROM ({1s}) _b", .{ try sql.quoteName(arena, dialect, col), base });
    const conn = prober.open() catch return null;
    var cur = conn.queryCursor(q) catch {
        conn.close();
        return null;
    };
    defer cur.close();
    const b = (try cur.nextBatch(arena)) orelse return null;
    if (b.len == 0) return null;
    const lo = b.columns[0].getValue(0);
    const hi = b.columns[1].getValue(0);
    if (lo.isNull() or hi.isNull()) return null;
    return .{ lo, hi };
}

/// A `LIMIT 1` probe so a forced uuid split does not fan out over an empty table.
/// A failed probe returns true; only a confirmed empty result skips the split.
fn hasAnyRow(arena: std.mem.Allocator, prober: Prober, base: []const u8) !bool {
    const q = try std.fmt.allocPrint(arena, "SELECT 1 FROM ({s}) _e LIMIT 1", .{base});
    const conn = prober.open() catch return true;
    var cur = conn.queryCursor(q) catch {
        conn.close();
        return true;
    };
    defer cur.close();
    const b = (try cur.nextBatch(arena)) orelse return false;
    return b.len > 0;
}

const Bounds = struct { min: i64, max: i64 };

fn intBounds(arena: std.mem.Allocator, prober: Prober, dialect: Dialect, base: []const u8, col: []const u8) !?Bounds {
    const q = try std.fmt.allocPrint(arena, "SELECT MIN({0s}) AS lo, MAX({0s}) AS hi FROM ({1s}) _b", .{ try sql.quoteName(arena, dialect, col), base });
    const conn = prober.open() catch return null;
    var cur = conn.queryCursor(q) catch {
        conn.close();
        return null;
    };
    defer cur.close();
    const b = (try cur.nextBatch(arena)) orelse return null;
    if (b.len == 0) return null;
    const lo = b.columns[0].getValue(0);
    const hi = b.columns[1].getValue(0);
    if (lo.isNull() or hi.isNull() or lo != .int or hi != .int) return null;
    if (hi.int <= lo.int) return null;
    return Bounds{ .min = lo.int, .max = hi.int };
}

fn intRangePreds(arena: std.mem.Allocator, dialect: Dialect, col: []const u8, min: i64, max: i64, m_in: usize) ![]const []const u8 {
    const qcol = try sql.quoteName(arena, dialect, col);
    const span: i128 = @as(i128, max) - @as(i128, min) + 1;
    var m: usize = m_in;
    if (@as(i128, @intCast(m)) > span) m = @intCast(span);
    if (m <= 1) {
        const one = try std.fmt.allocPrint(arena, "({s} >= {d} OR {s} IS NULL)", .{ qcol, min, qcol });
        return try dupeOne(arena, one);
    }
    const width: i128 = @divTrunc(span + @as(i128, @intCast(m)) - 1, @as(i128, @intCast(m)));
    var list = std.array_list.Managed([]const u8).init(arena);
    var k: usize = 0;
    while (k < m) : (k += 1) {
        const lo: i128 = @as(i128, min) + @as(i128, @intCast(k)) * width;
        if (k == m - 1) {
            try list.append(try std.fmt.allocPrint(arena, "{s} >= {d}", .{ qcol, lo }));
        } else if (k == 0) {
            const hi: i128 = lo + width;
            try list.append(try std.fmt.allocPrint(arena, "(({s} >= {d} AND {s} < {d}) OR {s} IS NULL)", .{ qcol, lo, qcol, hi, qcol }));
        } else {
            const hi: i128 = lo + width;
            try list.append(try std.fmt.allocPrint(arena, "{s} >= {d} AND {s} < {d}", .{ qcol, lo, qcol, hi }));
        }
    }
    return list.toOwnedSlice();
}

/// MIN/MAX of a date/timestamp key as days since 1970, or null when the cursor
/// did not yield `.date` or `.timestamp` (e.g. a driver left it as text).
fn dateBounds(arena: std.mem.Allocator, prober: Prober, dialect: Dialect, base: []const u8, col: []const u8) !?Bounds {
    const q = try std.fmt.allocPrint(arena, "SELECT MIN({0s}) AS lo, MAX({0s}) AS hi FROM ({1s}) _b", .{ try sql.quoteName(arena, dialect, col), base });
    const conn = prober.open() catch return null;
    var cur = conn.queryCursor(q) catch {
        conn.close();
        return null;
    };
    defer cur.close();
    const b = (try cur.nextBatch(arena)) orelse return null;
    if (b.len == 0) return null;
    const lo = dayOf(b.columns[0].getValue(0)) orelse return null;
    const hi = dayOf(b.columns[1].getValue(0)) orelse return null;
    if (hi <= lo) return null;
    return Bounds{ .min = lo, .max = hi };
}

fn dayOf(v: Value) ?i64 {
    return switch (v) {
        .date => |d| d,
        .timestamp => |us| civil.dayOf(us),
        else => null,
    };
}

fn dateRangePreds(arena: std.mem.Allocator, dialect: Dialect, col: []const u8, min_day: i64, max_day: i64, m_in: usize) ![]const []const u8 {
    const qcol = try sql.quoteName(arena, dialect, col);
    const span: i128 = @as(i128, max_day) - @as(i128, min_day) + 1;
    var m: usize = m_in;
    if (@as(i128, @intCast(m)) > span) m = @intCast(span);
    if (m <= 1) {
        const one = try std.fmt.allocPrint(arena, "({s} >= '{s}' OR {s} IS NULL)", .{ qcol, try eval.formatDate(arena, min_day), qcol });
        return try dupeOne(arena, one);
    }
    const width: i128 = @divTrunc(span + @as(i128, @intCast(m)) - 1, @as(i128, @intCast(m)));
    var list = std.array_list.Managed([]const u8).init(arena);
    var k: usize = 0;
    while (k < m) : (k += 1) {
        const lo: i64 = @intCast(@as(i128, min_day) + @as(i128, @intCast(k)) * width);
        if (k == m - 1) {
            try list.append(try std.fmt.allocPrint(arena, "{s} >= '{s}'", .{ qcol, try eval.formatDate(arena, lo) }));
        } else {
            const hi: i64 = @intCast(@as(i128, lo) + width);
            if (k == 0) {
                try list.append(try std.fmt.allocPrint(arena, "(({s} >= '{s}' AND {s} < '{s}') OR {s} IS NULL)", .{ qcol, try eval.formatDate(arena, lo), qcol, try eval.formatDate(arena, hi), qcol }));
            } else {
                try list.append(try std.fmt.allocPrint(arena, "{s} >= '{s}' AND {s} < '{s}'", .{ qcol, try eval.formatDate(arena, lo), qcol, try eval.formatDate(arena, hi) }));
            }
        }
    }
    return list.toOwnedSlice();
}

fn uuidSpacePreds(arena: std.mem.Allocator, dialect: Dialect, col: []const u8, m: usize) ![]const []const u8 {
    const qcol = try sql.quoteName(arena, dialect, col);
    var list = std.array_list.Managed([]const u8).init(arena);
    var k: usize = 0;
    while (k < m) : (k += 1) {
        const lo = if (k == 0) null else try uuidAt(arena, k, m);
        const hi = if (k == m - 1) null else try uuidAt(arena, k + 1, m);
        if (lo == null) {
            try list.append(try std.fmt.allocPrint(arena, "({s} < '{s}' OR {s} IS NULL)", .{ qcol, hi.?, qcol }));
        } else if (hi == null) {
            try list.append(try std.fmt.allocPrint(arena, "{s} >= '{s}'", .{ qcol, lo.? }));
        } else {
            try list.append(try std.fmt.allocPrint(arena, "{s} >= '{s}' AND {s} < '{s}'", .{ qcol, lo.?, qcol, hi.? }));
        }
    }
    return list.toOwnedSlice();
}

/// The k/m boundary of the UUID space as a canonical UUID string, computing
/// `floor(k * 2^128 / m)` in u256 to avoid overflow.
fn uuidAt(arena: std.mem.Allocator, k: usize, m: usize) ![]const u8 {
    const val: u128 = @intCast((@as(u256, k) << 128) / @as(u256, m));
    var bytes: [16]u8 = undefined;
    std.mem.writeInt(u128, &bytes, val, .big);
    return arena.dupe(u8, &uuid.text(&bytes, .canonical));
}

/// A name as a catalog lookup's string literal: `N'…'` on SQL Server, so a name
/// outside the database's code page still resolves. Stands in for
/// `sql.writeStrLit(.catalog_name)` until sql.zig has it.
pub fn catalogNameLit(arena: std.mem.Allocator, dialect: Dialect, name: []const u8) ![]const u8 {
    const lit = try dialect.strLit(arena, name);
    return if (dialect == .sqlserver) std.mem.concat(arena, u8, &.{ "N", lit }) else lit;
}

/// A table as a catalog lookup names it: its quoted parts as one literal, the way a
/// read names it (`'"s"."T"'::regclass` keeps the case PostgreSQL would fold).
pub fn catalogObjectLit(arena: std.mem.Allocator, dialect: Dialect, parts: []const []const u8) ![]const u8 {
    return catalogNameLit(arena, dialect, try sql.quoteParts(arena, dialect, parts));
}

/// What a catalog probe matches the table by: its quoted name as a literal on
/// postgres and SQL Server, its schema and name columns on the MySQL family.
fn catalogLit(arena: std.mem.Allocator, dialect: Dialect, parts: []const []const u8) ![]const u8 {
    return switch (dialect) {
        .postgres, .sqlserver => catalogObjectLit(arena, dialect, parts),
        .mysql, .starrocks, .doris => std.fmt.allocPrint(arena, "k.TABLE_SCHEMA = {s} AND k.TABLE_NAME = {s}", .{ try sql.schemaLit(arena, dialect, parts), try catalogNameLit(arena, dialect, parts[parts.len - 1]) }),
    };
}

test "catalog probes name the table by its quoted parts, as the read does" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings("'\"s\".\"Orders\"'", try catalogLit(a, .postgres, &.{ "s", "Orders" }));
    try std.testing.expectEqualStrings("N'[dbo].[o''x]'", try catalogLit(a, .sqlserver, &.{ "dbo", "o'x" }));
    try std.testing.expectEqualStrings("N'[dbo].[Ação]'", try catalogLit(a, .sqlserver, &.{ "dbo", "Ação" }));
    try std.testing.expectEqualStrings("k.TABLE_SCHEMA = 'db' AND k.TABLE_NAME = 't\\\\'", try catalogLit(a, .mysql, &.{ "db", "t\\" }));
    try std.testing.expectEqualStrings("k.TABLE_SCHEMA = DATABASE() AND k.TABLE_NAME = 't'", try catalogLit(a, .doris, &.{"t"}));
}

fn dupeOne(arena: std.mem.Allocator, s: []const u8) ![]const []const u8 {
    const out = try arena.alloc([]const u8, 1);
    out[0] = s;
    return out;
}

test "int range splits are covering and disjoint" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const preds = try intRangePreds(a, .postgres, "id", 1, 1000, 7);
    try std.testing.expect(preds.len == 7);
    var id: i64 = 1;
    while (id <= 1000) : (id += 1) {
        var hits: usize = 0;
        for (preds) |p| if (intPredHolds(p, id)) {
            hits += 1;
        };
        try std.testing.expectEqual(@as(usize, 1), hits);
    }
    var hits_over: usize = 0;
    for (preds) |p| if (intPredHolds(p, 5000)) {
        hits_over += 1;
    };
    try std.testing.expectEqual(@as(usize, 1), hits_over);
    var null_preds: usize = 0;
    for (preds) |p| if (std.mem.indexOf(u8, p, "IS NULL") != null) {
        null_preds += 1;
    };
    try std.testing.expectEqual(@as(usize, 1), null_preds);
}

test "int range clamps slice count to the value span" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const preds = try intRangePreds(a, .postgres, "id", 1, 3, 8);
    try std.testing.expectEqual(@as(usize, 3), preds.len);
    var id: i64 = 1;
    while (id <= 3) : (id += 1) {
        var hits: usize = 0;
        for (preds) |p| if (intPredHolds(p, id)) {
            hits += 1;
        };
        try std.testing.expectEqual(@as(usize, 1), hits);
    }
}

test "uuid space splits are ordered and cover the endpoints" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const preds = try uuidSpacePreds(a, .postgres, "id", 4);
    try std.testing.expect(preds.len == 4);
    try std.testing.expect(std.mem.indexOf(u8, preds[0], ">=") == null);
    try std.testing.expect(std.mem.startsWith(u8, preds[3], "\"id\" >= "));
    try std.testing.expect(std.mem.indexOf(u8, preds[3], "<") == null);
    var k: usize = 0;
    while (k + 1 < preds.len) : (k += 1) {
        const hi_pos = std.mem.lastIndexOf(u8, preds[k], "< '").?;
        const hi = preds[k][hi_pos + 3 ..][0..36];
        const lo_pos = std.mem.indexOf(u8, preds[k + 1], ">= '").?;
        const lo = preds[k + 1][lo_pos + 4 ..][0..36];
        try std.testing.expectEqualStrings(hi, lo);
        try std.testing.expectEqualStrings(try uuidAt(a, k + 1, 4), hi);
        if (k > 0) {
            const prev_lo_pos = std.mem.indexOf(u8, preds[k], ">= '").?;
            const prev_lo = preds[k][prev_lo_pos + 4 ..][0..36];
            try std.testing.expect(std.mem.order(u8, prev_lo, hi) == .lt);
        }
    }
    var null_preds: usize = 0;
    for (preds) |p| if (std.mem.indexOf(u8, p, "IS NULL") != null) {
        null_preds += 1;
    };
    try std.testing.expectEqual(@as(usize, 1), null_preds);
}

test "date range splits are covering, disjoint, and day-aligned" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const preds = try dateRangePreds(a, .mysql, "updated_at", 19723, 20088, 4);
    try std.testing.expectEqual(@as(usize, 4), preds.len);
    try std.testing.expectEqualStrings(
        "((`updated_at` >= '2024-01-01' AND `updated_at` < '2024-04-02') OR `updated_at` IS NULL)",
        preds[0],
    );
    try std.testing.expect(std.mem.endsWith(u8, preds[3], ">= '2024-10-03'"));
    var k: usize = 0;
    while (k + 1 < preds.len) : (k += 1) {
        const hi_pos = std.mem.lastIndexOf(u8, preds[k], "< '").?;
        const hi = preds[k][hi_pos + 3 ..][0..10];
        const lo_pos = std.mem.indexOf(u8, preds[k + 1], ">= '").?;
        const lo = preds[k + 1][lo_pos + 4 ..][0..10];
        try std.testing.expectEqualStrings(hi, lo);
    }
}

test "date range clamps slice count to the day span" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const preds = try dateRangePreds(a, .postgres, "d", 100, 102, 8);
    try std.testing.expectEqual(@as(usize, 3), preds.len);
    try std.testing.expect(std.mem.indexOf(u8, preds[0], ">= '1970-04-11'") != null);
    try std.testing.expect(std.mem.endsWith(u8, preds[2], ">= '1970-04-13'"));
    var k: usize = 0;
    while (k + 1 < preds.len) : (k += 1) {
        const hi_pos = std.mem.lastIndexOf(u8, preds[k], "< '").?;
        const hi = preds[k][hi_pos + 3 ..][0..10];
        const lo_pos = std.mem.indexOf(u8, preds[k + 1], ">= '").?;
        const lo = preds[k + 1][lo_pos + 4 ..][0..10];
        try std.testing.expectEqualStrings(hi, lo);
    }
}

test "dayOf converts date and timestamp values" {
    try std.testing.expectEqual(@as(?i64, 19723), dayOf(.{ .date = 19723 }));
    try std.testing.expectEqual(@as(?i64, 19723), dayOf(.{ .timestamp = 19723 * 86_400_000_000 + 3_600_000_000 }));
    try std.testing.expectEqual(@as(?i64, null), dayOf(.{ .string = "2024-01-01" }));
}

test "int range splits handle negative bounds" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const preds = try intRangePreds(a, .mysql, "id", -100, 50, 4);
    try std.testing.expectEqual(@as(usize, 4), preds.len);
    var id: i64 = -100;
    while (id <= 50) : (id += 1) {
        var hits: usize = 0;
        for (preds) |p| if (intPredHolds(p, id)) {
            hits += 1;
        };
        try std.testing.expectEqual(@as(usize, 1), hits);
    }
    for (preds) |p| try std.testing.expect(!intPredHolds(p, -101));
}

test "uuid boundary values are exact space fractions" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings("80000000-0000-0000-0000-000000000000", try uuidAt(a, 1, 2));
    try std.testing.expectEqualStrings("40000000-0000-0000-0000-000000000000", try uuidAt(a, 1, 4));
    try std.testing.expectEqualStrings("00000000-0000-0000-0000-000000000000", try uuidAt(a, 0, 3));
}

test "uuid boundaries: the shared uuid text gives the old allocPrint text for every k of several m" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    for ([_]usize{ 1, 2, 3, 4, 7, 16, 255, 1000, 65537 }) |m| {
        var k: usize = 0;
        while (k < m) : (k += @max(1, m / 97)) {
            const val: u128 = @intCast((@as(u256, k) << 128) / @as(u256, m));
            var b: [16]u8 = undefined;
            std.mem.writeInt(u128, &b, val, .big);
            const want = try std.fmt.allocPrint(a, "{x:0>2}{x:0>2}{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}", .{
                b[0], b[1], b[2],  b[3],  b[4],  b[5],  b[6],  b[7],
                b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15],
            });
            try std.testing.expectEqualStrings(want, try uuidAt(a, k, m));
        }
    }
}

test "dayOf floors a timestamp to its day, before the epoch too" {
    for ([_]i64{ 0, 1, -1, 86_399_999_999, -86_400_000_000, -86_400_000_001, std.math.minInt(i64), std.math.maxInt(i64) }) |us|
        try std.testing.expectEqual(@as(?i64, @divFloor(us, 86_400_000_000)), dayOf(.{ .timestamp = us }));
}

test "plan-level guards: m<=1 and non-postgres uuid never split" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const failing = Prober{ .ctx = undefined, .openFn = failOpen };
    try std.testing.expectEqual(@as(?Plan, null), try plan(a, failing, .postgres, "SELECT * FROM t", .{ .col = "id", .kind = .int }, 1));
    try std.testing.expectEqual(@as(?Plan, null), try plan(a, failing, .mysql, "SELECT * FROM t", .{ .col = "id", .kind = .uuid }, 4));
}

fn failOpen(_: *anyopaque) anyerror!Conn {
    return error.ConnectionRefused;
}

/// Test-only evaluator of an emitted int predicate for a non-null id; the
/// `OR ... IS NULL` tail of slice 0 is stripped first.
fn intPredHolds(pred_in: []const u8, id: i64) bool {
    var pred = pred_in;
    if (std.mem.indexOf(u8, pred, " OR ")) |o| pred = pred[0..o];
    pred = std.mem.trim(u8, pred, "()");
    var lo: i64 = std.math.minInt(i64);
    var hi: ?i64 = null;
    var it = std.mem.splitSequence(u8, pred, " AND ");
    while (it.next()) |part_in| {
        const part = std.mem.trim(u8, part_in, "()");
        const ge = std.mem.indexOf(u8, part, ">= ");
        const lt = std.mem.indexOf(u8, part, "< ");
        if (ge) |i| {
            lo = std.fmt.parseInt(i64, std.mem.trim(u8, part[i + 3 ..], "() "), 10) catch unreachable;
        } else if (lt) |i| {
            hi = std.fmt.parseInt(i64, std.mem.trim(u8, part[i + 2 ..], "() "), 10) catch unreachable;
        }
    }
    return id >= lo and (hi == null or id < hi.?);
}

/// A test connection answering every query with one batch of `rows`, or none,
/// recording each query's text and every open and close.
const FakeConn = struct {
    arena: std.mem.Allocator,
    rows: []const []const Value = &.{},
    no_batch: bool = false,
    fail_open: bool = false,
    fail_query: bool = false,
    served: bool = false,
    queries: std.ArrayListUnmanaged([]const u8) = .empty,
    opens: usize = 0,
    conn_closes: usize = 0,
    cursor_closes: usize = 0,

    const conn_vt = sql.Conn.VTable{ .queryCursor = queryCursor, .exec = exec, .close = closeConn };
    const cursor_vt = sql.Cursor.VTable{ .schema = schema, .nextBatch = nextBatch, .close = closeCursor };

    fn prober(self: *FakeConn) Prober {
        return .{ .ctx = self, .openFn = open };
    }

    fn open(ctx: *anyopaque) anyerror!Conn {
        const self: *FakeConn = @ptrCast(@alignCast(ctx));
        if (self.fail_open) return error.ConnectionRefused;
        self.opens += 1;
        self.served = false;
        return .{ .ptr = self, .vtable = &conn_vt };
    }

    fn queryCursor(p: *anyopaque, q: []const u8) anyerror!sql.Cursor {
        const self: *FakeConn = @ptrCast(@alignCast(p));
        try self.queries.append(self.arena, try self.arena.dupe(u8, q));
        if (self.fail_query) return error.QueryFailed;
        return .{ .ptr = self, .vtable = &cursor_vt };
    }

    fn exec(_: *anyopaque, _: []const u8) anyerror!void {}

    fn closeConn(p: *anyopaque) void {
        const self: *FakeConn = @ptrCast(@alignCast(p));
        self.conn_closes += 1;
    }

    fn schema(_: *anyopaque) @import("../lang/types.zig").Schema {
        return .{ .fields = &.{} };
    }

    fn closeCursor(p: *anyopaque) void {
        const self: *FakeConn = @ptrCast(@alignCast(p));
        self.cursor_closes += 1;
    }

    fn nextBatch(p: *anyopaque, arena: std.mem.Allocator) anyerror!?@import("../exec/batch.zig").Batch {
        const self: *FakeConn = @ptrCast(@alignCast(p));
        if (self.no_batch or self.served) return null;
        self.served = true;
        const types = @import("../lang/types.zig");
        const column = @import("../exec/column.zig");
        const ncols: usize = if (self.rows.len == 0) 3 else self.rows[0].len;
        const fields = try arena.alloc(types.Schema.Field, ncols);
        const cols = try arena.alloc(column.Column, ncols);
        for (0..ncols) |c| {
            var kind: types.TypeKind = .int;
            for (self.rows) |r| if (!r[c].isNull()) {
                kind = switch (r[c]) {
                    .string => .string,
                    .date => .date,
                    .timestamp => .timestamp,
                    else => .int,
                };
                break;
            };
            const ty = types.Type.init(kind).asNullable();
            fields[c] = .{ .name = "c", .ty = ty };
            var b = column.Builder.init(arena, ty);
            for (self.rows) |r| try b.append(r[c]);
            cols[c] = try b.finish();
        }
        const sch = try arena.create(types.Schema);
        sch.* = .{ .fields = fields };
        return .{ .schema = sch, .columns = cols, .len = self.rows.len };
    }
};

test "plan probes the key's bounds and slices them per kind, the NULL keys in slice 0" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const base = "SELECT * FROM t";

    var f = FakeConn{ .arena = a, .rows = &.{&.{ .{ .int = 1 }, .{ .int = 1000 } }} };
    const p = (try plan(a, f.prober(), .postgres, base, .{ .col = "id", .kind = .int }, 4)).?;
    try std.testing.expectEqualStrings("SELECT MIN(\"id\") AS lo, MAX(\"id\") AS hi FROM (SELECT * FROM t) _b", f.queries.items[0]);
    try std.testing.expectEqual(KeyKind.int, p.key.kind);
    try std.testing.expectEqualStrings(base, p.base_sql);
    const want_int = [_][]const u8{
        "((\"id\" >= 1 AND \"id\" < 251) OR \"id\" IS NULL)",
        "\"id\" >= 251 AND \"id\" < 501",
        "\"id\" >= 501 AND \"id\" < 751",
        "\"id\" >= 751",
    };
    try std.testing.expectEqual(want_int.len, p.predicates.len);
    for (want_int, p.predicates) |w, g| try std.testing.expectEqualStrings(w, g);
    try std.testing.expectEqual(@as(usize, 1), f.cursor_closes);

    f = FakeConn{ .arena = a, .rows = &.{&.{ .{ .date = 19723 }, .{ .date = 19725 } }} };
    const pd = (try plan(a, f.prober(), .sqlserver, base, .{ .col = "d", .kind = .date }, 8)).?;
    try std.testing.expectEqualStrings("SELECT MIN([d]) AS lo, MAX([d]) AS hi FROM (SELECT * FROM t) _b", f.queries.items[0]);
    const want_date = [_][]const u8{
        "(([d] >= '2024-01-01' AND [d] < '2024-01-02') OR [d] IS NULL)",
        "[d] >= '2024-01-02' AND [d] < '2024-01-03'",
        "[d] >= '2024-01-03'",
    };
    try std.testing.expectEqual(want_date.len, pd.predicates.len);
    for (want_date, pd.predicates) |w, g| try std.testing.expectEqualStrings(w, g);

    f = FakeConn{ .arena = a, .rows = &.{&.{ .{ .int = -5 }, .{ .int = 5 } }} };
    const pa = (try plan(a, f.prober(), .mysql, base, .{ .col = "k", .kind = .auto }, 2)).?;
    try std.testing.expectEqual(KeyKind.int, pa.key.kind);
    try std.testing.expectEqualStrings("((`k` >= -5 AND `k` < 1) OR `k` IS NULL)", pa.predicates[0]);
    try std.testing.expectEqualStrings("`k` >= 1", pa.predicates[1]);

    f = FakeConn{ .arena = a, .rows = &.{&.{ .{ .timestamp = 19723 * 86_400_000_000 + 5 }, .{ .timestamp = 19724 * 86_400_000_000 + 7 } }} };
    const pt = (try plan(a, f.prober(), .postgres, base, .{ .col = "ts", .kind = .auto }, 4)).?;
    try std.testing.expectEqual(KeyKind.date, pt.key.kind);
    try std.testing.expectEqual(@as(usize, 2), pt.predicates.len);
    try std.testing.expectEqualStrings("((\"ts\" >= '2024-01-01' AND \"ts\" < '2024-01-02') OR \"ts\" IS NULL)", pt.predicates[0]);
    try std.testing.expectEqualStrings("\"ts\" >= '2024-01-02'", pt.predicates[1]);

    f = FakeConn{ .arena = a, .rows = &.{&.{.{ .int = 1 }}} };
    const pu = (try plan(a, f.prober(), .postgres, base, .{ .col = "u", .kind = .uuid }, 2)).?;
    try std.testing.expectEqualStrings("SELECT 1 FROM (SELECT * FROM t) _e LIMIT 1", f.queries.items[0]);
    try std.testing.expectEqualStrings("(\"u\" < '80000000-0000-0000-0000-000000000000' OR \"u\" IS NULL)", pu.predicates[0]);
    try std.testing.expectEqualStrings("\"u\" >= '80000000-0000-0000-0000-000000000000'", pu.predicates[1]);
}

test "plan stays serial when the bounds probe finds nothing to slice" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const base = "SELECT * FROM t";
    const Case = struct { rows: []const []const Value = &.{}, kind: KeyKind, no_batch: bool = false, fail_open: bool = false, fail_query: bool = false };
    const cases = [_]Case{
        .{ .kind = .int, .rows = &.{&.{ .{ .int = 7 }, .{ .int = 7 } }} },
        .{ .kind = .int, .rows = &.{&.{ .null, .null }} },
        .{ .kind = .int, .rows = &.{&.{ .{ .string = "a" }, .{ .string = "z" } }} },
        .{ .kind = .int, .rows = &.{} },
        .{ .kind = .int, .no_batch = true },
        .{ .kind = .int, .fail_open = true },
        .{ .kind = .int, .fail_query = true },
        .{ .kind = .date, .rows = &.{&.{ .{ .date = 9 }, .{ .date = 9 } }} },
        .{ .kind = .date, .rows = &.{&.{ .{ .string = "2024-01-01" }, .{ .date = 9 } }} },
        .{ .kind = .date, .rows = &.{} },
        .{ .kind = .date, .no_batch = true },
        .{ .kind = .date, .fail_open = true },
        .{ .kind = .date, .fail_query = true },
        .{ .kind = .auto, .rows = &.{&.{ .{ .int = 3 }, .{ .int = 2 } }} },
        .{ .kind = .auto, .rows = &.{&.{ .{ .date = 10 }, .{ .date = 10 } }} },
        .{ .kind = .auto, .rows = &.{&.{ .{ .string = "a" }, .{ .string = "b" } }} },
        .{ .kind = .auto, .rows = &.{&.{ .null, .{ .int = 1 } }} },
        .{ .kind = .auto, .rows = &.{} },
        .{ .kind = .auto, .no_batch = true },
        .{ .kind = .auto, .fail_open = true },
        .{ .kind = .auto, .fail_query = true },
        .{ .kind = .uuid, .no_batch = true },
        .{ .kind = .uuid, .rows = &.{} },
    };
    for (cases, 0..) |cs, i| {
        var f = FakeConn{ .arena = a, .rows = cs.rows, .no_batch = cs.no_batch, .fail_open = cs.fail_open, .fail_query = cs.fail_query };
        const got = try plan(a, f.prober(), .postgres, base, .{ .col = "k", .kind = cs.kind }, 4);
        if (got != null) {
            std.debug.print("case {d} split into {d}\n", .{ i, got.?.predicates.len });
            return error.TestUnexpectedResult;
        }
        if (cs.fail_query) try std.testing.expectEqual(@as(usize, 1), f.conn_closes);
    }
}

test "a uuid split whose emptiness probe fails still fans out" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    for ([_]bool{ true, false }) |fail_open| {
        var f = FakeConn{ .arena = a, .fail_open = fail_open, .fail_query = !fail_open };
        const p = (try plan(a, f.prober(), .postgres, "SELECT * FROM t", .{ .col = "u", .kind = .uuid }, 3)).?;
        try std.testing.expectEqual(@as(usize, 3), p.predicates.len);
        try std.testing.expectEqualStrings("\"u\" >= '55555555-5555-5555-5555-555555555555' AND \"u\" < 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'", p.predicates[1]);
    }
}

test "range slicing: the full i64 span is computed in i128, a one-value span is one slice" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const wide = try intRangePreds(a, .postgres, "id", std.math.minInt(i64), std.math.maxInt(i64), 4);
    const want = [_][]const u8{
        "((\"id\" >= -9223372036854775808 AND \"id\" < -4611686018427387904) OR \"id\" IS NULL)",
        "\"id\" >= -4611686018427387904 AND \"id\" < 0",
        "\"id\" >= 0 AND \"id\" < 4611686018427387904",
        "\"id\" >= 4611686018427387904",
    };
    try std.testing.expectEqual(want.len, wide.len);
    for (want, wide) |w, g| try std.testing.expectEqualStrings(w, g);
    for ([_]i64{ std.math.minInt(i64), -1, 0, std.math.maxInt(i64) }) |id| {
        var hits: usize = 0;
        for (wide) |p| if (intPredHolds(p, id)) {
            hits += 1;
        };
        try std.testing.expectEqual(@as(usize, 1), hits);
    }

    const one = try intRangePreds(a, .sqlserver, "id", 5, 5, 4);
    try std.testing.expectEqual(@as(usize, 1), one.len);
    try std.testing.expectEqualStrings("([id] >= 5 OR [id] IS NULL)", one[0]);
    const one_day = try dateRangePreds(a, .mysql, "d", 19723, 19723, 4);
    try std.testing.expectEqual(@as(usize, 1), one_day.len);
    try std.testing.expectEqualStrings("(`d` >= '2024-01-01' OR `d` IS NULL)", one_day[0]);
}

test "wrap and wrapProjected put the split predicate and a pushed filter around the base" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings("SELECT * FROM (SELECT * FROM t) _split WHERE id >= 5", try wrap(a, "SELECT * FROM t", "id >= 5"));
    try std.testing.expectEqualStrings(
        "SELECT \"a\", \"b\" FROM (SELECT * FROM t) _split WHERE id >= 5 AND (c > 1 OR c IS NULL)",
        try wrapProjected(a, "SELECT * FROM t", "\"a\", \"b\"", "id >= 5", "c > 1 OR c IS NULL"),
    );
}

test "introspectKey reads a single-column primary key's name, kind and row estimate" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const kinds = [_]struct { typ: []const u8, kind: ?KeyKind }{
        .{ .typ = "int8", .kind = .int },
        .{ .typ = "bigserial", .kind = .int },
        .{ .typ = "mediumint", .kind = .int },
        .{ .typ = "timestamptz", .kind = .date },
        .{ .typ = "datetime2", .kind = .date },
        .{ .typ = "smalldatetime", .kind = .date },
        .{ .typ = "uuid", .kind = .uuid },
        .{ .typ = "uniqueidentifier", .kind = .uuid },
        .{ .typ = "varchar", .kind = null },
        .{ .typ = "numeric", .kind = null },
    };
    for (kinds) |k| {
        var f = FakeConn{ .arena = a, .rows = &.{&.{ .{ .string = "id" }, .{ .string = k.typ }, .{ .int = 3_000_000 } }} };
        const got = try introspectKey(a, f.prober(), .postgres, &.{ "public", "t" });
        if (k.kind) |want| {
            try std.testing.expectEqualStrings("id", got.?.key.col);
            try std.testing.expectEqual(want, got.?.key.kind);
            try std.testing.expectEqual(@as(i64, 3_000_000), got.?.est_rows);
        } else try std.testing.expectEqual(@as(?KeyInfo, null), got);
    }

    var f = FakeConn{ .arena = a, .rows = &.{&.{ .{ .string = "id" }, .{ .string = "int" }, .null }} };
    try std.testing.expectEqual(@as(i64, 0), (try introspectKey(a, f.prober(), .mysql, &.{ "db", "t" })).?.est_rows);
    try std.testing.expect(std.mem.indexOf(u8, f.queries.items[0], "WHERE k.CONSTRAINT_NAME = 'PRIMARY' AND k.TABLE_SCHEMA = 'db' AND k.TABLE_NAME = 't'") != null);

    f = FakeConn{ .arena = a, .rows = &.{ &.{ .{ .string = "a" }, .{ .string = "int" }, .{ .int = 9 } }, &.{ .{ .string = "b" }, .{ .string = "int" }, .{ .int = 9 } } } };
    try std.testing.expectEqual(@as(?KeyInfo, null), try introspectKey(a, f.prober(), .sqlserver, &.{ "dbo", "t" }));
    try std.testing.expect(std.mem.indexOf(u8, f.queries.items[0], "WHERE i.object_id = OBJECT_ID(N'[dbo].[t]') AND i.is_primary_key = 1") != null);

    f = FakeConn{ .arena = a, .rows = &.{&.{ .null, .{ .string = "int" }, .{ .int = 9 } }} };
    try std.testing.expectEqual(@as(?KeyInfo, null), try introspectKey(a, f.prober(), .postgres, &.{"t"}));
    try std.testing.expect(std.mem.indexOf(u8, f.queries.items[0], "WHERE i.indrelid = '\"t\"'::regclass AND i.indisprimary") != null);

    for ([_]FakeConn{
        .{ .arena = a, .no_batch = true },
        .{ .arena = a, .fail_open = true },
        .{ .arena = a, .fail_query = true },
    }) |base| {
        f = base;
        try std.testing.expectEqual(@as(?KeyInfo, null), try introspectKey(a, f.prober(), .starrocks, &.{ "db", "t" }));
    }
    try std.testing.expectEqual(@as(usize, 1), f.conn_closes);
}

test "introspectPkCols lists every key column in order, empty when the probe cannot run" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var f = FakeConn{ .arena = a, .rows = &.{ &.{.{ .string = "tenant" }}, &.{.null}, &.{.{ .string = "id" }} } };
    const cols = try introspectPkCols(a, f.prober(), .sqlserver, &.{ "dbo", "t" });
    try std.testing.expectEqual(@as(usize, 2), cols.len);
    try std.testing.expectEqualStrings("tenant", cols[0]);
    try std.testing.expectEqualStrings("id", cols[1]);
    try std.testing.expect(std.mem.endsWith(u8, f.queries.items[0], "WHERE i.object_id = OBJECT_ID(N'[dbo].[t]') AND i.is_primary_key = 1\nORDER BY ic.key_ordinal"));

    f = FakeConn{ .arena = a, .rows = &.{&.{.{ .string = "id" }}} };
    _ = try introspectPkCols(a, f.prober(), .postgres, &.{ "s", "t" });
    try std.testing.expect(std.mem.indexOf(u8, f.queries.items[0], "WHERE i.indrelid = '\"s\".\"t\"'::regclass AND i.indisprimary") != null);
    f = FakeConn{ .arena = a, .rows = &.{&.{.{ .string = "id" }}} };
    _ = try introspectPkCols(a, f.prober(), .doris, &.{"t"});
    try std.testing.expect(std.mem.indexOf(u8, f.queries.items[0], "WHERE k.CONSTRAINT_NAME = 'PRIMARY' AND k.TABLE_SCHEMA = DATABASE() AND k.TABLE_NAME = 't'") != null);

    f = FakeConn{ .arena = a, .fail_open = true };
    try std.testing.expectEqual(@as(usize, 0), (try introspectPkCols(a, f.prober(), .mysql, &.{ "db", "t" })).len);
    f = FakeConn{ .arena = a, .fail_query = true };
    try std.testing.expectEqual(@as(usize, 0), (try introspectPkCols(a, f.prober(), .mysql, &.{ "db", "t" })).len);
    try std.testing.expectEqual(@as(usize, 1), f.conn_closes);
}
