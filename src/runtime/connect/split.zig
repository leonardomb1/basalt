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
//! DATETIME/TIMESTAMP alike) are sliced by day with date literals (`sql.dateLit`,
//! `0044-03-15 BC` on PostgreSQL), which every dialect compares against either
//! type; bounds past what the dialect can name read serially. UUID keys are sliced evenly over the 128-bit space,
//! balanced for random v4 ids with no bounds probe.
//!
//! Below `min_rows_to_split` estimated rows the planner stays serial: each lane
//! reconnects, and that setup costs more than it saves. An explicit @[split] or
//! @[splits] overrides. Each probe consumes the connection `Prober` opens for it.
//!
//! A PostgreSQL table is cut by pages instead (`pagePlan`), where no `SPLIT BY` names
//! a column: each lane reads a range of the table's physical pages, `ctid` between
//! two block numbers, which PostgreSQL 14 and later answer with a TID range scan
//! reading only those pages. No key or index is needed, so a table of a quarter
//! million rows splits; a key range over a column without an index read the whole
//! table once per range. The range goes into the read's own WHERE (`Reach.inner`),
//! where the scan sees it, and every lane reads under one exported snapshot
//! (`Reach.snapshot`), so a row an UPDATE moves to another page during the read is
//! read once, from the table as it stood when the plan was made.

const std = @import("std");
const msgfmt = @import("../../base/msgfmt.zig");
const sql = @import("../../db/sql.zig");
const civil = @import("../../base/civil.zig");
const uuid = @import("../../format/uuid.zig");
const Value = @import("../../exec/value.zig").Value;

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

/// How a lane's range reaches the source: outside the read as a wrapping WHERE (a key
/// range), or with `inner` in the read's own WHERE, `own_where` being the read's own
/// filter and `base_sql` the read without it (a page range, which must reach the table
/// scan); `snapshot` names a PostgreSQL snapshot each lane imports before it reads.
pub const Reach = struct { inner: bool = false, own_where: []const u8 = "", snapshot: ?[]const u8 = null };

pub const Plan = struct {
    key: Key,
    predicates: []const []const u8,
    base_sql: []const u8,
    reach: Reach = .{},
};

/// The query one lane sends for range `pred`: `wrapProjected`'s for a key range; for a
/// page range, the read with `pred` AND-ed into its own WHERE, under the projection
/// and filter that pushdown sends.
pub fn rangeQuery(arena: std.mem.Allocator, base: []const u8, reach: Reach, proj: ?[]const u8, pred: []const u8, extra: ?[]const u8) ![]const u8 {
    if (!reach.inner) return wrapProjected(arena, base, proj, pred, extra);
    const inner = if (reach.own_where.len > 0)
        try msgfmt.allocPrint(arena, "{s} WHERE ({s}) AND {s}", .{ base, reach.own_where, pred })
    else
        try msgfmt.allocPrint(arena, "{s} WHERE {s}", .{ base, pred });
    const sel = proj orelse "*";
    if (extra) |x| return msgfmt.allocPrint(arena, "SELECT {s} FROM ({s}) _split WHERE {s}", .{ sel, inner, x });
    return msgfmt.allocPrint(arena, "SELECT {s} FROM ({s}) _split", .{ sel, inner });
}

/// The rows from which a PostgreSQL table is cut by pages: below it the lanes'
/// logins cost more than the parallel read saves.
pub const min_rows_page_split: i64 = 200_000;

/// The pages from which a table never analyzed (no row estimate) is cut by pages.
pub const min_pages_page_split: u64 = 2048;

/// PostgreSQL's first version with a TID range scan; before it every page range read
/// the whole table.
pub const min_page_split_version: i64 = 140000;

/// `m` page ranges over a table of `pages` pages, half-open, the last open-ended so a
/// page added after the probe still lands in a lane.
pub fn pageRangePreds(arena: std.mem.Allocator, pages: u64, m: usize) ![]const []const u8 {
    const n: u64 = @min(@as(u64, @intCast(m)), pages);
    const out = try arena.alloc([]const u8, @intCast(n));
    for (out, 0..) |*p, i| {
        const lo = pages * i / n;
        const hi = pages * (i + 1) / n;
        p.* = if (i == 0)
            try msgfmt.allocPrint(arena, "ctid < '({d},0)'::tid", .{hi})
        else if (i + 1 == n)
            try msgfmt.allocPrint(arena, "ctid >= '({d},0)'::tid", .{lo})
        else
            try msgfmt.allocPrint(arena, "ctid >= '({d},0)'::tid AND ctid < '({d},0)'::tid", .{ lo, hi });
    }
    return out;
}

/// A PostgreSQL table's size in pages and its estimated rows, read when it is an
/// ordinary table or materialized view (a view or a partitioned table has no pages of
/// its own) on a server with a TID range scan; null otherwise, or when the catalog
/// cannot be read.
pub const Pages = struct { pages: u64, est_rows: i64 };

pub fn tablePages(arena: std.mem.Allocator, prober: Prober, parts: []const []const u8) !?Pages {
    const q = try msgfmt.allocPrint(arena,
        \\SELECT CAST(pg_relation_size(c.oid) / current_setting('block_size')::int AS bigint),
        \\CAST(c.reltuples AS bigint), CAST(current_setting('server_version_num') AS int), CAST(c.relkind AS text)
        \\FROM pg_class c WHERE c.oid = to_regclass({s})
    , .{try catalogObjectLit(arena, .postgres, parts)});
    const conn = prober.open() catch return null;
    var cur = conn.queryCursor(q) catch {
        conn.close();
        return null;
    };
    defer cur.close();
    const b = (cur.nextBatch(arena) catch return null) orelse return null;
    if (b.len == 0) return null;
    const pages = intOf(b.columns[0].getValue(0)) orelse return null;
    const est = intOf(b.columns[1].getValue(0)) orelse return null;
    const ver = intOf(b.columns[2].getValue(0)) orelse return null;
    const kind = b.columns[3].getValue(0);
    if (ver < min_page_split_version or pages <= 0) return null;
    if (kind != .string or !(std.mem.eql(u8, kind.string, "r") or std.mem.eql(u8, kind.string, "m"))) return null;
    return .{ .pages = @intCast(pages), .est_rows = est };
}

fn intOf(v: Value) ?i64 {
    return switch (v) {
        .int => |i| i,
        .string => |t| std.fmt.parseInt(i64, std.mem.trim(u8, t, " "), 10) catch null,
        else => null,
    };
}

pub fn wrap(arena: std.mem.Allocator, base: []const u8, pred: []const u8) ![]const u8 {
    return wrapProjected(arena, base, null, pred, null);
}

/// `wrap` with pushdown: `proj` (null → `*`) is a comma-joined column list and
/// `extra` a filter AND-ed onto `pred`. Both see every base column inside the
/// subquery, so a pushed filter may test a column the projection drops.
pub fn wrapProjected(arena: std.mem.Allocator, base: []const u8, proj: ?[]const u8, pred: []const u8, extra: ?[]const u8) ![]const u8 {
    const sel = proj orelse "*";
    if (extra) |x|
        return msgfmt.allocPrint(arena, "SELECT {s} FROM ({s}) _split WHERE {s} AND ({s})", .{ sel, base, pred, x });
    return msgfmt.allocPrint(arena, "SELECT {s} FROM ({s}) _split WHERE {s}", .{ sel, base, pred });
}

/// Every primary-key column name for the table `parts` names, in key order, or
/// empty when there is none. Used to infer upsert keys from the source.
pub fn introspectPkCols(arena: std.mem.Allocator, prober: Prober, dialect: Dialect, parts: []const []const u8) ![]const []const u8 {
    const lit = try catalogLit(arena, dialect, parts);
    const query = switch (dialect) {
        .postgres => try msgfmt.allocPrint(arena,
            \\SELECT a.attname
            \\FROM pg_index i
            \\JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            \\WHERE i.indrelid = {s}::regclass AND i.indisprimary
            \\ORDER BY (SELECT k FROM generate_subscripts(i.indkey, 1) k WHERE i.indkey[k] = a.attnum)
        , .{lit}),
        .sqlserver => try msgfmt.allocPrint(arena,
            \\SELECT c.name
            \\FROM sys.indexes i
            \\JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            \\JOIN sys.columns c ON c.object_id = i.object_id AND c.column_id = ic.column_id
            \\WHERE i.object_id = OBJECT_ID({s}) AND i.is_primary_key = 1
            \\ORDER BY ic.key_ordinal
        , .{lit}),
        .mysql, .starrocks, .doris => try msgfmt.allocPrint(arena,
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
        .postgres => try msgfmt.allocPrint(arena,
            \\SELECT a.attname, t.typname, c.reltuples::bigint
            \\FROM pg_index i
            \\JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            \\JOIN pg_type t ON t.oid = a.atttypid
            \\JOIN pg_class c ON c.oid = i.indrelid
            \\WHERE i.indrelid = {s}::regclass AND i.indisprimary
        , .{lit}),
        .sqlserver => try msgfmt.allocPrint(arena,
            \\SELECT c.name, ty.name, p.rows
            \\FROM sys.indexes i
            \\JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            \\JOIN sys.columns c ON c.object_id = i.object_id AND c.column_id = ic.column_id
            \\JOIN sys.types ty ON ty.user_type_id = c.user_type_id
            \\JOIN (SELECT object_id, SUM(rows) AS rows FROM sys.partitions WHERE index_id IN (0,1) GROUP BY object_id) p ON p.object_id = i.object_id
            \\WHERE i.object_id = OBJECT_ID({s}) AND i.is_primary_key = 1
            \\ORDER BY ic.key_ordinal
        , .{lit}),
        .mysql, .starrocks, .doris => try msgfmt.allocPrint(arena,
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

/// Why `plan` cut no key ranges: the read has no key values (no rows, or all NULL),
/// the key holds one value, it is no integer, date or timestamp (or its span
/// overflows), its bounds could not be read, or one lane was asked for.
pub const NoSplit = enum { no_rows, one_value, not_rangeable, unprobed, one_lane };

/// Up to `m` split predicates for `key` over the unsplit `base`, or null, with
/// `why`, when the key gives no ranges to cut, so the caller reads serially.
pub fn plan(arena: std.mem.Allocator, prober: Prober, dialect: Dialect, base: []const u8, key: Key, m: usize, why: *NoSplit) !?Plan {
    if (m <= 1) return noSplit(why, .one_lane);
    switch (key.kind) {
        .int => {
            const b = boundsOf(i128, try minMax(arena, prober, dialect, base, key.col), intKey, why) orelse return null;
            const preds = try intRangePreds(arena, dialect, key.col, b[0], b[1], m);
            if (preds.len <= 1) return noSplit(why, .not_rangeable);
            return Plan{ .key = key, .predicates = preds, .base_sql = base };
        },
        .uuid => {
            if (dialect != .postgres) return noSplit(why, .not_rangeable);
            if (!(try hasAnyRow(arena, prober, base))) return noSplit(why, .no_rows);
            const preds = try uuidSpacePreds(arena, dialect, key.col, m);
            return Plan{ .key = key, .predicates = preds, .base_sql = base };
        },
        .date => {
            const b = boundsOf(i64, try minMax(arena, prober, dialect, base, key.col), dayOf, why) orelse return null;
            const preds = try dateRangePreds(arena, dialect, key.col, b[0], b[1], m);
            if (preds.len <= 1) return noSplit(why, .not_rangeable);
            return Plan{ .key = key, .predicates = preds, .base_sql = base };
        },
        .auto => {
            const lh = try minMax(arena, prober, dialect, base, key.col);
            if (lh) |v| if (intKey(v[0]) != null or intKey(v[1]) != null) {
                const b = boundsOf(i128, lh, intKey, why) orelse return null;
                const preds = try intRangePreds(arena, dialect, key.col, b[0], b[1], m);
                if (preds.len <= 1) return noSplit(why, .not_rangeable);
                return Plan{ .key = .{ .col = key.col, .kind = .int }, .predicates = preds, .base_sql = base };
            };
            const b = boundsOf(i64, lh, dayOf, why) orelse return null;
            const preds = try dateRangePreds(arena, dialect, key.col, b[0], b[1], m);
            if (preds.len <= 1) return noSplit(why, .not_rangeable);
            return Plan{ .key = .{ .col = key.col, .kind = .date }, .predicates = preds, .base_sql = base };
        },
    }
}

fn noSplit(why: *NoSplit, w: NoSplit) ?Plan {
    why.* = w;
    return null;
}

/// The key's bounds `lh` as `T`, low below high, or null with `why`: no bounds read,
/// a NULL one (no rows, or every key NULL), one `conv` cannot read, or one value.
fn boundsOf(comptime T: type, lh: ?[2]Value, conv: fn (Value) ?T, why: *NoSplit) ?[2]T {
    const v = lh orelse {
        why.* = .unprobed;
        return null;
    };
    if (v[0].isNull() or v[1].isNull()) {
        why.* = .no_rows;
        return null;
    }
    const lo = conv(v[0]) orelse {
        why.* = .not_rangeable;
        return null;
    };
    const hi = conv(v[1]) orelse {
        why.* = .not_rangeable;
        return null;
    };
    if (hi <= lo) {
        why.* = .one_value;
        return null;
    }
    return .{ lo, hi };
}

/// The key's MIN and MAX over `base`, as the driver types them, NULL over no rows;
/// null when the probe fails.
fn minMax(arena: std.mem.Allocator, prober: Prober, dialect: Dialect, base: []const u8, col: []const u8) !?[2]Value {
    const q = try msgfmt.allocPrint(arena, "SELECT MIN({0s}) AS lo, MAX({0s}) AS hi FROM ({1s}) _b", .{ try sql.quoteName(arena, dialect, col), base });
    const conn = prober.open() catch return null;
    var cur = conn.queryCursor(q) catch {
        conn.close();
        return null;
    };
    defer cur.close();
    const b = (try cur.nextBatch(arena)) orelse return .{ .null, .null };
    if (b.len == 0) return .{ .null, .null };
    return .{ b.columns[0].getValue(0), b.columns[1].getValue(0) };
}

/// A `LIMIT 1` probe so a forced uuid split does not fan out over an empty table.
/// A failed probe returns true; only a confirmed empty result skips the split.
fn hasAnyRow(arena: std.mem.Allocator, prober: Prober, base: []const u8) !bool {
    const q = try msgfmt.allocPrint(arena, "SELECT 1 FROM ({s}) _e LIMIT 1", .{base});
    const conn = prober.open() catch return true;
    var cur = conn.queryCursor(q) catch {
        conn.close();
        return true;
    };
    defer cur.close();
    const b = (try cur.nextBatch(arena)) orelse return false;
    return b.len > 0;
}

/// An integer key's value: an INT, or a scale-0 DECIMAL such as the DECIMAL(20,0)
/// a MySQL BIGINT UNSIGNED reads as (`types.unsignedType`).
fn intKey(v: Value) ?i128 {
    return switch (v) {
        .int => |x| x,
        .decimal => |d| if (d.scale == 0) d.unscaled else null,
        else => null,
    };
}

/// Up to `m_in` covering, disjoint range predicates over [min, max]; a single
/// whole-range predicate (a serial read) when the slicing arithmetic would
/// overflow i128, as bounds near a DECIMAL(38,0)'s limits can.
fn intRangePreds(arena: std.mem.Allocator, dialect: Dialect, col: []const u8, min: i128, max: i128, m_in: usize) ![]const []const u8 {
    const qcol = try sql.quoteName(arena, dialect, col);
    const one = try msgfmt.allocPrint(arena, "({s} >= {d} OR {s} IS NULL)", .{ qcol, min, qcol });
    const span: i128 = std.math.add(i128, std.math.sub(i128, max, min) catch return dupeOne(arena, one), 1) catch return dupeOne(arena, one);
    var m: usize = m_in;
    if (@as(i128, @intCast(m)) > span) m = @intCast(span);
    if (m <= 1) return try dupeOne(arena, one);
    const width: i128 = @divTrunc(span - 1, @as(i128, @intCast(m))) + 1;
    var list = std.array_list.Managed([]const u8).init(arena);
    var k: usize = 0;
    while (k < m) : (k += 1) {
        const off: i128 = std.math.mul(i128, @intCast(k), width) catch return dupeOne(arena, one);
        const lo: i128 = std.math.add(i128, min, off) catch return dupeOne(arena, one);
        if (k == m - 1) {
            try list.append(try msgfmt.allocPrint(arena, "{s} >= {d}", .{ qcol, lo }));
        } else {
            const hi: i128 = std.math.add(i128, lo, width) catch return dupeOne(arena, one);
            if (k == 0) {
                try list.append(try msgfmt.allocPrint(arena, "(({s} >= {d} AND {s} < {d}) OR {s} IS NULL)", .{ qcol, lo, qcol, hi, qcol }));
            } else {
                try list.append(try msgfmt.allocPrint(arena, "{s} >= {d} AND {s} < {d}", .{ qcol, lo, qcol, hi }));
            }
        }
    }
    return list.toOwnedSlice();
}

fn dayOf(v: Value) ?i64 {
    return switch (v) {
        .date => |d| d,
        .timestamp => |us| civil.dayOf(us),
        else => null,
    };
}

/// Up to `m_in` covering, disjoint day-range predicates over [min_day, max_day],
/// each bound a `sql.dateLit`; a single whole-range predicate (a serial read)
/// when a bound is a day the dialect cannot name, rather than one it misreads.
/// Every bound is a day in [min_day, max_day], days the key column holds, so its
/// type can name each literal: a SMALLDATETIME ending 2079-06-06 never meets a
/// `'2079-06-07'` it cannot convert; the lane that would overshoot runs open-ended.
fn dateRangePreds(arena: std.mem.Allocator, dialect: Dialect, col: []const u8, min_day: i64, max_day: i64, m_in: usize) ![]const []const u8 {
    const qcol = try sql.quoteName(arena, dialect, col);
    const whole = try msgfmt.allocPrint(arena, "({s} IS NULL OR {s} IS NOT NULL)", .{ qcol, qcol });
    const span: i128 = @as(i128, max_day) - @as(i128, min_day) + 1;
    var m: usize = m_in;
    if (@as(i128, @intCast(m)) > span) m = @intCast(span);
    if (m <= 1) {
        const one = try msgfmt.allocPrint(arena, "({s} >= {s} OR {s} IS NULL)", .{ qcol, (try sql.dateLit(arena, dialect, min_day)) orelse return dupeOne(arena, whole), qcol });
        return try dupeOne(arena, one);
    }
    const width: i128 = @divTrunc(span + @as(i128, @intCast(m)) - 1, @as(i128, @intCast(m)));
    var list = std.array_list.Managed([]const u8).init(arena);
    var k: usize = 0;
    while (k < m) : (k += 1) {
        const lo: i64 = @intCast(@as(i128, min_day) + @as(i128, @intCast(k)) * width);
        const lo_lit = (try sql.dateLit(arena, dialect, lo)) orelse return dupeOne(arena, whole);
        const hi: i64 = @intCast(@as(i128, lo) + width);
        if (k == m - 1 or hi > max_day) {
            try list.append(try msgfmt.allocPrint(arena, "{s} >= {s}", .{ qcol, lo_lit }));
            break;
        } else {
            const hi_lit = (try sql.dateLit(arena, dialect, hi)) orelse return dupeOne(arena, whole);
            if (k == 0) {
                try list.append(try msgfmt.allocPrint(arena, "(({s} >= {s} AND {s} < {s}) OR {s} IS NULL)", .{ qcol, lo_lit, qcol, hi_lit, qcol }));
            } else {
                try list.append(try msgfmt.allocPrint(arena, "{s} >= {s} AND {s} < {s}", .{ qcol, lo_lit, qcol, hi_lit }));
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
            try list.append(try msgfmt.allocPrint(arena, "({s} < '{s}' OR {s} IS NULL)", .{ qcol, hi.?, qcol }));
        } else if (hi == null) {
            try list.append(try msgfmt.allocPrint(arena, "{s} >= '{s}'", .{ qcol, lo.? }));
        } else {
            try list.append(try msgfmt.allocPrint(arena, "{s} >= '{s}' AND {s} < '{s}'", .{ qcol, lo.?, qcol, hi.? }));
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

/// A name as a catalog lookup's string literal, `writeStrLit(.catalog_name)`
/// allocated: `N'…'` on SQL Server, so a name outside the database's code page
/// still resolves.
pub fn catalogNameLit(arena: std.mem.Allocator, dialect: Dialect, name: []const u8) ![]const u8 {
    var out = std.array_list.Managed(u8).init(arena);
    try dialect.writeStrLit(out.writer(), name, .catalog_name);
    return out.toOwnedSlice();
}

/// A table as a catalog lookup names it: its quoted parts as one literal, the way a
/// read names it (`'"s"."T"'::regclass` keeps the case PostgreSQL would fold).
pub fn catalogObjectLit(arena: std.mem.Allocator, dialect: Dialect, parts: []const []const u8) ![]const u8 {
    return catalogNameLit(arena, dialect, try sql.quoteParts(arena, dialect, parts));
}

/// What a catalog probe matches the table by: its quoted name as a literal on
/// postgres and SQL Server, its schema and name columns on the MySQL family.
/// A table's unique key, primary or not: its columns, and whether it holds whole
/// column values (not partial, on no expression or column prefix, not deferred), as
/// an upsert's match needs.
pub const UniqueKey = struct { cols: []const []const u8, whole: bool };

/// The unique keys of table `parts`, read on a connection of the prober's own (a
/// cursor owns the connection it reads); null when no such table exists, or on SQL
/// Server, whose MERGE matches by the keys it names. A StarRocks or Doris table has
/// one, the PRIMARY KEY or UNIQUE KEY its upserts merge by, and none in a duplicate
/// or aggregate model. A column of an expression key part reads as "".
pub fn uniqueKeys(arena: std.mem.Allocator, prober: Prober, dialect: Dialect, parts: []const []const u8) !?[]const UniqueKey {
    const lit = try catalogLit(arena, dialect, parts);
    const query = switch (dialect) {
        .postgres => try msgfmt.allocPrint(arena,
            \\SELECT '' AS i, '' AS c, 1 AS w WHERE to_regclass({s}) IS NOT NULL
            \\UNION ALL
            \\SELECT i.indexrelid::text, a.attname,
            \\ CASE WHEN i.indpred IS NULL AND i.indexprs IS NULL AND i.indimmediate THEN 1 ELSE 0 END
            \\FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            \\WHERE i.indrelid = to_regclass({s}) AND i.indisunique
        , .{ lit, lit }),
        .mysql => try msgfmt.allocPrint(arena,
            \\SELECT '' AS i, '' AS c, 1 AS w FROM information_schema.TABLES k WHERE {s}
            \\UNION ALL
            \\SELECT k.INDEX_NAME, COALESCE(k.COLUMN_NAME, ''),
            \\ CASE WHEN k.SUB_PART IS NULL AND k.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END
            \\FROM information_schema.STATISTICS k WHERE k.NON_UNIQUE = 0 AND {s}
        , .{ lit, lit }),
        .starrocks, .doris => try msgfmt.allocPrint(arena,
            \\SELECT '' AS i, '' AS c, 1 AS w FROM information_schema.TABLES k WHERE {s}
            \\UNION ALL
            \\SELECT 'key', k.COLUMN_NAME, 1 FROM information_schema.COLUMNS k WHERE k.COLUMN_KEY IN ('PRI', 'UNI') AND {s}
        , .{ lit, lit }),
        .sqlserver => return null,
    };
    const conn = try prober.open();
    var cur = conn.queryCursor(query) catch |e| {
        conn.close();
        return e;
    };
    defer cur.close();
    var exists = false;
    var names = std.array_list.Managed([]const u8).init(arena);
    var keys = std.array_list.Managed(struct { cols: std.array_list.Managed([]const u8), whole: bool }).init(arena);
    while (try cur.nextBatch(arena)) |b| for (0..b.len) |r| {
        const name = b.columns[0].getValue(r).string;
        if (name.len == 0) {
            exists = true;
            continue;
        }
        const col = try arena.dupe(u8, b.columns[1].getValue(r).string);
        const whole = b.columns[2].getValue(r).int == 1;
        const at = for (names.items, 0..) |n, i| {
            if (std.mem.eql(u8, n, name)) break i;
        } else blk: {
            try names.append(try arena.dupe(u8, name));
            try keys.append(.{ .cols = std.array_list.Managed([]const u8).init(arena), .whole = true });
            break :blk keys.items.len - 1;
        };
        try keys.items[at].cols.append(col);
        keys.items[at].whole = keys.items[at].whole and whole;
    };
    if (!exists) return null;
    const out = try arena.alloc(UniqueKey, keys.items.len);
    for (keys.items, out) |k, *o| o.* = .{ .cols = k.cols.items, .whole = k.whole };
    return out;
}

fn catalogLit(arena: std.mem.Allocator, dialect: Dialect, parts: []const []const u8) ![]const u8 {
    return switch (dialect) {
        .postgres, .sqlserver => catalogObjectLit(arena, dialect, parts),
        .mysql, .starrocks, .doris => msgfmt.allocPrint(arena, "k.TABLE_SCHEMA = {s} AND k.TABLE_NAME = {s}", .{ try sql.schemaLit(arena, dialect, parts), try catalogNameLit(arena, dialect, parts[parts.len - 1]) }),
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

test "date range bounds stay within the probed days, so a SMALLDATETIME key ending at its last day gets no literal it cannot convert" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const max = civil.daysFromCivil(2079, 6, 6);
    const preds = try dateRangePreds(a, .sqlserver, "d", max - 9, max, 6);
    try std.testing.expectEqual(@as(usize, 5), preds.len);
    try std.testing.expectEqualStrings("[d] >= '2079-06-05'", preds[4]);
    for (preds) |p| try std.testing.expect(std.mem.indexOf(u8, p, "2079-06-07") == null);
    var k: usize = 0;
    while (k + 1 < preds.len) : (k += 1) {
        const hi_pos = std.mem.lastIndexOf(u8, preds[k], "< '").?;
        const lo_pos = std.mem.indexOf(u8, preds[k + 1], ">= '").?;
        try std.testing.expectEqualStrings(preds[k][hi_pos + 3 ..][0..10], preds[k + 1][lo_pos + 4 ..][0..10]);
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
            const want = try msgfmt.allocPrint(a, "{x:0>2}{x:0>2}{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}", .{
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
    try std.testing.expectEqual(@as(?Plan, null), try planned(a, failing, .postgres, "SELECT * FROM t", .{ .col = "id", .kind = .int }, 1));
    try std.testing.expectEqual(@as(?Plan, null), try planned(a, failing, .mysql, "SELECT * FROM t", .{ .col = "id", .kind = .uuid }, 4));
}

fn planned(a: std.mem.Allocator, prober: Prober, dialect: Dialect, base: []const u8, key: Key, m: usize) !?Plan {
    var why: NoSplit = .one_lane;
    return plan(a, prober, dialect, base, key, m, &why);
}

test "a key with no ranges to cut says why: no rows, one value, no integer or date, no bounds read, one lane" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const base = "SELECT * FROM t";
    const Case = struct { rows: []const []const Value, kind: KeyKind, m: usize = 4, why: NoSplit };
    const cases = [_]Case{
        .{ .rows = &.{&.{ .null, .null }}, .kind = .int, .why = .no_rows },
        .{ .rows = &.{&.{ .null, .null }}, .kind = .auto, .why = .no_rows },
        .{ .rows = &.{&.{ .null, .null }}, .kind = .date, .why = .no_rows },
        .{ .rows = &.{&.{ .{ .int = 7 }, .{ .int = 7 } }}, .kind = .auto, .why = .one_value },
        .{ .rows = &.{&.{ .{ .date = 19000 }, .{ .date = 19000 } }}, .kind = .date, .why = .one_value },
        .{ .rows = &.{&.{ .{ .string = "a" }, .{ .string = "z" } }}, .kind = .auto, .why = .not_rangeable },
        .{ .rows = &.{&.{ .{ .int = 1 }, .{ .int = 9 } }}, .kind = .int, .m = 1, .why = .one_lane },
    };
    for (cases) |c| {
        var f = FakeConn{ .arena = a, .rows = c.rows };
        var why: NoSplit = .one_lane;
        try std.testing.expectEqual(@as(?Plan, null), try plan(a, f.prober(), .postgres, base, .{ .col = "id", .kind = c.kind }, c.m, &why));
        try std.testing.expectEqual(c.why, why);
    }
    var why: NoSplit = .one_lane;
    const failing = Prober{ .ctx = undefined, .openFn = failOpen };
    try std.testing.expectEqual(@as(?Plan, null), try plan(a, failing, .postgres, base, .{ .col = "id", .kind = .int }, 4, &why));
    try std.testing.expectEqual(NoSplit.unprobed, why);
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

    fn schema(_: *anyopaque) @import("../../lang/types.zig").Schema {
        return .{ .fields = &.{} };
    }

    fn closeCursor(p: *anyopaque) void {
        const self: *FakeConn = @ptrCast(@alignCast(p));
        self.cursor_closes += 1;
    }

    fn nextBatch(p: *anyopaque, arena: std.mem.Allocator) anyerror!?@import("../../exec/batch.zig").Batch {
        const self: *FakeConn = @ptrCast(@alignCast(p));
        if (self.no_batch or self.served) return null;
        self.served = true;
        const types = @import("../../lang/types.zig");
        const column = @import("../../exec/column.zig");
        const ncols: usize = if (self.rows.len == 0) 3 else self.rows[0].len;
        const fields = try arena.alloc(types.Schema.Field, ncols);
        const cols = try arena.alloc(column.Column, ncols);
        for (0..ncols) |c| {
            var kind: types.TypeKind = .int;
            var scale: u8 = 0;
            for (self.rows) |r| if (!r[c].isNull()) {
                if (r[c] == .decimal) scale = r[c].decimal.scale;
                kind = switch (r[c]) {
                    .string => .string,
                    .date => .date,
                    .timestamp => .timestamp,
                    .decimal => .decimal,
                    else => .int,
                };
                break;
            };
            const ty = (if (kind == .decimal) types.Type.decimal(20, scale) else types.Type.init(kind)).asNullable();
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
    const p = (try planned(a, f.prober(), .postgres, base, .{ .col = "id", .kind = .int }, 4)).?;
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
    const pd = (try planned(a, f.prober(), .sqlserver, base, .{ .col = "d", .kind = .date }, 8)).?;
    try std.testing.expectEqualStrings("SELECT MIN([d]) AS lo, MAX([d]) AS hi FROM (SELECT * FROM t) _b", f.queries.items[0]);
    const want_date = [_][]const u8{
        "(([d] >= '2024-01-01' AND [d] < '2024-01-02') OR [d] IS NULL)",
        "[d] >= '2024-01-02' AND [d] < '2024-01-03'",
        "[d] >= '2024-01-03'",
    };
    try std.testing.expectEqual(want_date.len, pd.predicates.len);
    for (want_date, pd.predicates) |w, g| try std.testing.expectEqualStrings(w, g);

    f = FakeConn{ .arena = a, .rows = &.{&.{ .{ .int = -5 }, .{ .int = 5 } }} };
    const pa = (try planned(a, f.prober(), .mysql, base, .{ .col = "k", .kind = .auto }, 2)).?;
    try std.testing.expectEqual(KeyKind.int, pa.key.kind);
    try std.testing.expectEqualStrings("((`k` >= -5 AND `k` < 1) OR `k` IS NULL)", pa.predicates[0]);
    try std.testing.expectEqualStrings("`k` >= 1", pa.predicates[1]);

    f = FakeConn{ .arena = a, .rows = &.{&.{ .{ .timestamp = 19723 * 86_400_000_000 + 5 }, .{ .timestamp = 19724 * 86_400_000_000 + 7 } }} };
    const pt = (try planned(a, f.prober(), .postgres, base, .{ .col = "ts", .kind = .auto }, 4)).?;
    try std.testing.expectEqual(KeyKind.date, pt.key.kind);
    try std.testing.expectEqual(@as(usize, 2), pt.predicates.len);
    try std.testing.expectEqualStrings("((\"ts\" >= '2024-01-01' AND \"ts\" < '2024-01-02') OR \"ts\" IS NULL)", pt.predicates[0]);
    try std.testing.expectEqualStrings("\"ts\" >= '2024-01-02'", pt.predicates[1]);

    f = FakeConn{ .arena = a, .rows = &.{&.{.{ .int = 1 }}} };
    const pu = (try planned(a, f.prober(), .postgres, base, .{ .col = "u", .kind = .uuid }, 2)).?;
    try std.testing.expectEqualStrings("SELECT 1 FROM (SELECT * FROM t) _e LIMIT 1", f.queries.items[0]);
    try std.testing.expectEqualStrings("(\"u\" < '80000000-0000-0000-0000-000000000000' OR \"u\" IS NULL)", pu.predicates[0]);
    try std.testing.expectEqualStrings("\"u\" >= '80000000-0000-0000-0000-000000000000'", pu.predicates[1]);
}

test "a BIGINT UNSIGNED key, read as DECIMAL(20,0), splits over its whole unsigned range" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const base = "SELECT * FROM t";
    const top: i128 = std.math.maxInt(u64);
    const lo = Value{ .decimal = .{ .unscaled = 0, .scale = 0 } };
    const hi = Value{ .decimal = .{ .unscaled = top, .scale = 0 } };
    for ([_]KeyKind{ .int, .auto }) |kind| {
        var f = FakeConn{ .arena = a, .rows = &.{&.{ lo, hi }} };
        const p = (try planned(a, f.prober(), .mysql, base, .{ .col = "id", .kind = kind }, 4)).?;
        try std.testing.expectEqual(KeyKind.int, p.key.kind);
        const want = [_][]const u8{
            "((`id` >= 0 AND `id` < 4611686018427387904) OR `id` IS NULL)",
            "`id` >= 4611686018427387904 AND `id` < 9223372036854775808",
            "`id` >= 9223372036854775808 AND `id` < 13835058055282163712",
            "`id` >= 13835058055282163712",
        };
        try std.testing.expectEqual(want.len, p.predicates.len);
        for (want, p.predicates) |w, g| try std.testing.expectEqualStrings(w, g);
    }
    var frac = FakeConn{ .arena = a, .rows = &.{&.{ Value{ .decimal = .{ .unscaled = 15, .scale = 1 } }, Value{ .decimal = .{ .unscaled = 95, .scale = 1 } } }} };
    try std.testing.expectEqual(@as(?Plan, null), try planned(a, frac.prober(), .mysql, base, .{ .col = "id", .kind = .int }, 4));
}

test "a DECIMAL(38,0) key whose span overflows i128 reads serially; one that fits still splits" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const base = "SELECT * FROM t";
    const top: i128 = std.math.pow(i128, 10, 38) - 1;
    const neg = Value{ .decimal = .{ .unscaled = -top, .scale = 0 } };
    const zero = Value{ .decimal = .{ .unscaled = 0, .scale = 0 } };
    const pos = Value{ .decimal = .{ .unscaled = top, .scale = 0 } };
    for ([_]KeyKind{ .int, .auto }) |kind| {
        var f = FakeConn{ .arena = a, .rows = &.{&.{ neg, pos }} };
        try std.testing.expectEqual(@as(?Plan, null), try planned(a, f.prober(), .postgres, base, .{ .col = "id", .kind = kind }, 4));
        var g = FakeConn{ .arena = a, .rows = &.{&.{ zero, pos }} };
        const p = (try planned(a, g.prober(), .postgres, base, .{ .col = "id", .kind = kind }, 4)).?;
        try std.testing.expectEqual(KeyKind.int, p.key.kind);
        try std.testing.expectEqual(@as(usize, 4), p.predicates.len);
        try std.testing.expectEqualStrings("\"id\" >= 75000000000000000000000000000000000000", p.predicates[3]);
    }
    try std.testing.expectEqual(@as(usize, 1), (try intRangePreds(a, .postgres, "id", std.math.minInt(i128), std.math.maxInt(i128), 4)).len);
    try std.testing.expectEqual(@as(usize, 1), (try intRangePreds(a, .postgres, "id", std.math.maxInt(i128) - 5, std.math.maxInt(i128), 4)).len);
}

test "a date or timestamp key with a bound before year 1 splits with BC on PostgreSQL, serially elsewhere (D3)" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const base = "SELECT * FROM t";
    const ides = civil.daysFromCivil(-43, 3, 15);
    const end = civil.daysFromCivil(2024, 1, 1);
    const bounds = [_][2]Value{
        .{ .{ .date = @intCast(ides) }, .{ .date = @intCast(end) } },
        .{ .{ .timestamp = civil.dayMicrosChecked(ides).? + 5 }, .{ .timestamp = civil.dayMicrosChecked(end).? + 7 } },
    };
    for (bounds) |b| for ([_]KeyKind{ .date, .auto }) |kind| {
        var f = FakeConn{ .arena = a, .rows = &.{&.{ b[0], b[1] }} };
        const p = (try planned(a, f.prober(), .postgres, base, .{ .col = "d", .kind = kind }, 2)).?;
        try std.testing.expectEqual(@as(usize, 2), p.predicates.len);
        try std.testing.expectEqualStrings("((\"d\" >= '0044-03-15 BC' AND \"d\" < '0990-08-08') OR \"d\" IS NULL)", p.predicates[0]);
        try std.testing.expectEqualStrings("\"d\" >= '0990-08-08'", p.predicates[1]);
        for ([_]Dialect{ .mysql, .sqlserver, .starrocks, .doris }) |d| {
            var g = FakeConn{ .arena = a, .rows = &.{&.{ b[0], b[1] }} };
            try std.testing.expectEqual(@as(?Plan, null), try planned(a, g.prober(), d, base, .{ .col = "d", .kind = kind }, 2));
        }
    };
    var far = FakeConn{ .arena = a, .rows = &.{&.{ .{ .date = @intCast(end) }, .{ .date = @intCast(civil.daysFromCivil(10000, 1, 1)) } }} };
    const pf = (try planned(a, far.prober(), .mysql, base, .{ .col = "d", .kind = .date }, 2)).?;
    try std.testing.expectEqualStrings("`d` >= '6012-01-02'", pf.predicates[1]);
    var past = FakeConn{ .arena = a, .rows = &.{&.{ .{ .date = @intCast(civil.daysFromCivil(-5000, 1, 1)) }, .{ .date = @intCast(end) } }} };
    try std.testing.expectEqual(@as(?Plan, null), try planned(a, past.prober(), .postgres, base, .{ .col = "d", .kind = .date }, 2));
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
        const got = try planned(a, f.prober(), .postgres, base, .{ .col = "k", .kind = cs.kind }, 4);
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
        const p = (try planned(a, f.prober(), .postgres, "SELECT * FROM t", .{ .col = "u", .kind = .uuid }, 3)).?;
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

test "page ranges cover every page, half-open, the last open-ended, at most one per page" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const four = try pageRangePreds(a, 10, 4);
    try std.testing.expectEqual(@as(usize, 4), four.len);
    try std.testing.expectEqualStrings("ctid < '(2,0)'::tid", four[0]);
    try std.testing.expectEqualStrings("ctid >= '(2,0)'::tid AND ctid < '(5,0)'::tid", four[1]);
    try std.testing.expectEqualStrings("ctid >= '(5,0)'::tid AND ctid < '(7,0)'::tid", four[2]);
    try std.testing.expectEqualStrings("ctid >= '(7,0)'::tid", four[3]);
    try std.testing.expectEqual(@as(usize, 3), (try pageRangePreds(a, 3, 8)).len);
}

test "a page range goes inside the read, beside its own filter, the pushed projection and filter outside" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const pred = "ctid < '(2,0)'::tid";
    try std.testing.expectEqualStrings(
        "SELECT * FROM (SELECT * FROM t WHERE ctid < '(2,0)'::tid) _split",
        try rangeQuery(a, "SELECT * FROM t", .{ .inner = true }, null, pred, null),
    );
    try std.testing.expectEqualStrings(
        "SELECT \"a\" FROM (SELECT * FROM t WHERE (b = 1 OR c = 2) AND ctid < '(2,0)'::tid) _split WHERE c > 1",
        try rangeQuery(a, "SELECT * FROM t", .{ .inner = true, .own_where = "b = 1 OR c = 2" }, "\"a\"", pred, "c > 1"),
    );
    try std.testing.expectEqualStrings(
        "SELECT * FROM (SELECT * FROM t) _split WHERE id >= 5 AND (c > 1)",
        try rangeQuery(a, "SELECT * FROM t", .{}, null, "id >= 5", "c > 1"),
    );
}

test "tablePages reads an ordinary table's pages on PostgreSQL 14 or later, null for a view, an older server or no pages" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var f = FakeConn{ .arena = a, .rows = &.{&.{ .{ .int = 27696 }, .{ .int = 1_500_000 }, .{ .int = 160004 }, .{ .string = "r" } }} };
    const got = (try tablePages(a, f.prober(), &.{ "s", "Orders" })).?;
    try std.testing.expectEqual(@as(u64, 27696), got.pages);
    try std.testing.expectEqual(@as(i64, 1_500_000), got.est_rows);
    try std.testing.expect(std.mem.indexOf(u8, f.queries.items[0], "WHERE c.oid = to_regclass('\"s\".\"Orders\"')") != null);

    f = FakeConn{ .arena = a, .rows = &.{&.{ .{ .int = 9 }, .{ .int = -1 }, .{ .int = 140000 }, .{ .string = "m" } }} };
    try std.testing.expectEqual(@as(i64, -1), (try tablePages(a, f.prober(), &.{"t"})).?.est_rows);

    const refused = [_][]const Value{
        &.{ .{ .int = 9 }, .{ .int = 9 }, .{ .int = 160004 }, .{ .string = "v" } },
        &.{ .{ .int = 9 }, .{ .int = 9 }, .{ .int = 160004 }, .{ .string = "p" } },
        &.{ .{ .int = 9 }, .{ .int = 9 }, .{ .int = 130010 }, .{ .string = "r" } },
        &.{ .{ .int = 0 }, .{ .int = 0 }, .{ .int = 160004 }, .{ .string = "r" } },
        &.{ .null, .{ .int = 9 }, .{ .int = 160004 }, .{ .string = "r" } },
    };
    for (refused) |row| {
        f = FakeConn{ .arena = a, .rows = &.{row} };
        try std.testing.expectEqual(@as(?Pages, null), try tablePages(a, f.prober(), &.{"t"}));
    }
    for ([_]FakeConn{
        .{ .arena = a, .no_batch = true },
        .{ .arena = a, .fail_open = true },
        .{ .arena = a, .fail_query = true },
    }) |base| {
        f = base;
        try std.testing.expectEqual(@as(?Pages, null), try tablePages(a, f.prober(), &.{"t"}));
    }
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
