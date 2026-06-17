//! Split planning for parallel source reads. A "split" is a SQL boolean predicate
//! over a key column; the set of splits is **disjoint and covering** over the
//! key's `[min,max]` (captured at plan time), so each lane reads one key range on
//! its own connection. This is an *unsynchronized* partitioned read: a row present
//! and unchanged for the whole read appears exactly once, but concurrent writes are
//! fuzzy and re-runs repeat rows — downstream dedup (StarRocks PK / ClickHouse
//! Replacing / Snowflake MERGE) owns exactly-once. See `runtime/parallel.zig`.

const std = @import("std");
const sqlmod = @import("sql.zig");
const types = @import("../lang/types.zig");
const eval = @import("../exec/eval.zig");
const valuemod = @import("../exec/value.zig");

const Conn = sqlmod.Conn;
const Dialect = sqlmod.Dialect;
const Value = valuemod.Value;

/// `.date` covers DATE and DATETIME/TIMESTAMP keys alike: ranges are sliced at
/// day granularity with date literals, which all three dialects compare against
/// either type (a date literal coerces to that day's midnight).
pub const KeyKind = enum { int, uuid, date };
pub const Key = struct { col: []const u8, kind: KeyKind };
pub const KeyInfo = struct { key: Key, est_rows: i64 };

/// Below this estimated row count, auto-splitting a table costs more in
/// per-connection setup (each lane re-connects; SCRAM/handshake is not free) than
/// it saves, so the planner stays serial. An explicit @[split]/@[splits] overrides.
pub const min_rows_to_split: i64 = 2_000_000;

/// Opens a fresh connection for one probe query. Each probe consumes its
/// connection (a `Cursor` owns and closes its `Conn`), so probes never share one.
pub const Prober = struct {
    ctx: *anyopaque,
    openFn: *const fn (ctx: *anyopaque) anyerror!Conn,

    fn open(self: Prober) !Conn {
        return self.openFn(self.ctx);
    }
};

pub const Plan = struct {
    key: Key,
    /// Each entry is a SQL boolean expression over the key column. Wrap the base
    /// query with `wrap(base, predicates[i])` to get one lane's query.
    predicates: []const []const u8,
};

/// `SELECT * FROM (<base>) _split WHERE <pred>` — uniform whether `base` came from
/// a `table T` (`SELECT * FROM T`) or an explicit `query`.
pub fn wrap(arena: std.mem.Allocator, base: []const u8, pred: []const u8) ![]const u8 {
    return std.fmt.allocPrint(arena, "SELECT * FROM ({s}) _split WHERE {s}", .{ base, pred });
}

// ---------------------------------------------------------------------------
// Key discovery (table reads only — arbitrary queries name the key via @[split])
// ---------------------------------------------------------------------------

/// Discover a single-column int/uuid primary key for `table`, or null (no PK, a
/// composite PK, or an unsupported key type → caller stays serial). Each dialect's
/// catalog query returns the same shape: (pk_column_name, type_name, est_rows) —
/// one row per PK column, so a composite PK yields >1 row and is rejected below.
/// Every primary-key column name for `table`, in key order — composite-safe and
/// type-agnostic, unlike `introspectKey` (which gates to a single int/uuid split
/// key). Returns an empty slice when the table has no declared primary key.
/// Used to infer upsert keys from the source.
pub fn introspectPkCols(arena: std.mem.Allocator, prober: Prober, dialect: Dialect, table: []const u8) ![]const []const u8 {
    const sql = switch (dialect) {
        .postgres => try std.fmt.allocPrint(arena,
            \\SELECT a.attname
            \\FROM pg_index i
            \\JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            \\WHERE i.indrelid = '{s}'::regclass AND i.indisprimary
            \\ORDER BY (SELECT k FROM generate_subscripts(i.indkey, 1) k WHERE i.indkey[k] = a.attnum)
        , .{table}),
        .sqlserver => try std.fmt.allocPrint(arena,
            \\SELECT c.name
            \\FROM sys.indexes i
            \\JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            \\JOIN sys.columns c ON c.object_id = i.object_id AND c.column_id = ic.column_id
            \\WHERE i.object_id = OBJECT_ID('{s}') AND i.is_primary_key = 1
            \\ORDER BY ic.key_ordinal
        , .{table}),
        .mysql => try std.fmt.allocPrint(arena,
            \\SELECT k.COLUMN_NAME
            \\FROM information_schema.KEY_COLUMN_USAGE k
            \\WHERE k.CONSTRAINT_NAME = 'PRIMARY' AND k.TABLE_SCHEMA = DATABASE() AND k.TABLE_NAME = '{s}'
            \\ORDER BY k.ORDINAL_POSITION
        , .{table}),
    };
    const conn = prober.open() catch return &.{};
    var cur = conn.queryCursor(sql) catch {
        conn.close();
        return &.{};
    };
    defer cur.close(); // closes the connection
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

pub fn introspectKey(arena: std.mem.Allocator, prober: Prober, dialect: Dialect, table: []const u8) !?KeyInfo {
    // One probe returns the single-column PK (name + type) and the engine's row
    // estimate, so the size gate costs no extra round-trip.
    const sql = switch (dialect) {
        .postgres => try std.fmt.allocPrint(arena,
            \\SELECT a.attname, t.typname, c.reltuples::bigint
            \\FROM pg_index i
            \\JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            \\JOIN pg_type t ON t.oid = a.atttypid
            \\JOIN pg_class c ON c.oid = i.indrelid
            \\WHERE i.indrelid = '{s}'::regclass AND i.indisprimary
        , .{table}),
        .sqlserver => try std.fmt.allocPrint(arena,
            \\SELECT c.name, ty.name, p.rows
            \\FROM sys.indexes i
            \\JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            \\JOIN sys.columns c ON c.object_id = i.object_id AND c.column_id = ic.column_id
            \\JOIN sys.types ty ON ty.user_type_id = c.user_type_id
            \\JOIN (SELECT object_id, SUM(rows) AS rows FROM sys.partitions WHERE index_id IN (0,1) GROUP BY object_id) p ON p.object_id = i.object_id
            \\WHERE i.object_id = OBJECT_ID('{s}') AND i.is_primary_key = 1
            \\ORDER BY ic.key_ordinal
        , .{table}),
        .mysql => try std.fmt.allocPrint(arena,
            \\SELECT k.COLUMN_NAME, c.DATA_TYPE, t.TABLE_ROWS
            \\FROM information_schema.KEY_COLUMN_USAGE k
            \\JOIN information_schema.COLUMNS c ON c.TABLE_SCHEMA = k.TABLE_SCHEMA AND c.TABLE_NAME = k.TABLE_NAME AND c.COLUMN_NAME = k.COLUMN_NAME
            \\JOIN information_schema.TABLES t ON t.TABLE_SCHEMA = k.TABLE_SCHEMA AND t.TABLE_NAME = k.TABLE_NAME
            \\WHERE k.CONSTRAINT_NAME = 'PRIMARY' AND k.TABLE_SCHEMA = DATABASE() AND k.TABLE_NAME = '{s}'
            \\ORDER BY k.ORDINAL_POSITION
        , .{table}),
    };
    const conn = prober.open() catch return null;
    var cur = conn.queryCursor(sql) catch {
        conn.close();
        return null;
    };
    defer cur.close(); // closes the connection
    const b = (try cur.nextBatch(arena)) orelse return null;
    if (b.len != 1) return null; // no PK, or composite PK (>1 key column)
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
        "int2",   "int4",      "int8",   "serial", "bigserial", "smallserial", // postgres
        "int",    "bigint",    "smallint", "tinyint", "mediumint", // sql server / mysql
    };
    for (ints) |t| if (std.mem.eql(u8, typname, t)) return .int;
    const dates = [_][]const u8{
        "date",     "timestamp", "timestamptz", // postgres
        "datetime", // mysql / sql server (mysql `timestamp` matches above)
        "datetime2", "smalldatetime", // sql server
    };
    for (dates) |t| if (std.mem.eql(u8, typname, t)) return .date;
    // uuid: postgres `uuid`, sql server `uniqueidentifier` (only postgres splits it —
    // plan() bails on uuid for other dialects since their sort order isn't lexical).
    if (std.mem.eql(u8, typname, "uuid") or std.mem.eql(u8, typname, "uniqueidentifier")) return .uuid;
    return null;
}

// ---------------------------------------------------------------------------
// Plan construction
// ---------------------------------------------------------------------------

/// Build up to `m` split predicates for `key` over `base` (the unsplit query).
/// Returns null when the source isn't worth/possible to split (empty, or the key
/// has no usable bounds), so the caller falls back to a single serial read.
pub fn plan(arena: std.mem.Allocator, prober: Prober, dialect: Dialect, base: []const u8, key: Key, m: usize) !?Plan {
    if (m <= 1) return null;
    switch (key.kind) {
        .int => {
            const b = (try intBounds(arena, prober, dialect, base, key.col)) orelse return null;
            const preds = try intRangePreds(arena, dialect, key.col, b.min, b.max, m);
            if (preds.len <= 1) return null;
            return Plan{ .key = key, .predicates = preds };
        },
        .uuid => {
            if (dialect != .postgres) return null; // others order uuids non-lexically
            if (!(try hasAnyRow(arena, prober, base))) return null; // empty table: don't fan out lanes
            const preds = try uuidSpacePreds(arena, dialect, key.col, m);
            return Plan{ .key = key, .predicates = preds };
        },
        .date => {
            const b = (try dateBounds(arena, prober, dialect, base, key.col)) orelse return null;
            const preds = try dateRangePreds(arena, dialect, key.col, b.min, b.max, m);
            if (preds.len <= 1) return null;
            return Plan{ .key = key, .predicates = preds };
        },
    }
}

/// Cheap non-empty probe (`LIMIT 1`) so a forced uuid split doesn't fan out lanes
/// over an empty table. A failed probe returns true (uuid splitting doesn't depend
/// on it, so don't block on a transient probe error) — we only skip on a confirmed
/// empty result.
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
    const q = try std.fmt.allocPrint(arena, "SELECT MIN({0s}) AS lo, MAX({0s}) AS hi FROM ({1s}) _b", .{ quoteIdent(arena, dialect, col) catch col, base });
    const conn = prober.open() catch return null;
    var cur = conn.queryCursor(q) catch {
        conn.close();
        return null;
    };
    defer cur.close(); // closes the connection
    const b = (try cur.nextBatch(arena)) orelse return null;
    if (b.len == 0) return null;
    const lo = b.columns[0].getValue(0);
    const hi = b.columns[1].getValue(0);
    if (lo.isNull() or hi.isNull() or lo != .int or hi != .int) return null; // empty table or non-int key
    if (hi.int <= lo.int) return null; // single value: nothing to split
    return Bounds{ .min = lo.int, .max = hi.int };
}

/// Equal-width half-open ranges over `[min, max]`. The last range has no upper
/// bound (`>= lo`), so it also captures rows inserted past `max` after the probe.
fn intRangePreds(arena: std.mem.Allocator, dialect: Dialect, col: []const u8, min: i64, max: i64, m_in: usize) ![]const []const u8 {
    const qcol = quoteIdent(arena, dialect, col) catch col;
    const span: i128 = @as(i128, max) - @as(i128, min) + 1;
    var m: usize = m_in;
    if (@as(i128, @intCast(m)) > span) m = @intCast(span); // don't make empty slices
    if (m <= 1) {
        const one = try std.fmt.allocPrint(arena, "{s} >= {d}", .{ qcol, min });
        return try dupeOne(arena, one);
    }
    const width: i128 = @divTrunc(span + @as(i128, @intCast(m)) - 1, @as(i128, @intCast(m))); // ceil
    var list = std.array_list.Managed([]const u8).init(arena);
    var k: usize = 0;
    while (k < m) : (k += 1) {
        const lo: i128 = @as(i128, min) + @as(i128, @intCast(k)) * width;
        if (k == m - 1) {
            try list.append(try std.fmt.allocPrint(arena, "{s} >= {d}", .{ qcol, lo }));
        } else {
            const hi: i128 = lo + width;
            try list.append(try std.fmt.allocPrint(arena, "{s} >= {d} AND {s} < {d}", .{ qcol, lo, qcol, hi }));
        }
    }
    return list.toOwnedSlice();
}

/// MIN/MAX of a date/timestamp key as day counts since the 1970 epoch. The
/// cursor's text coercion yields `.date` (days) or `.timestamp` (micros);
/// anything else (e.g. a driver that left the column as text) → no split.
fn dateBounds(arena: std.mem.Allocator, prober: Prober, dialect: Dialect, base: []const u8, col: []const u8) !?Bounds {
    const q = try std.fmt.allocPrint(arena, "SELECT MIN({0s}) AS lo, MAX({0s}) AS hi FROM ({1s}) _b", .{ quoteIdent(arena, dialect, col) catch col, base });
    const conn = prober.open() catch return null;
    var cur = conn.queryCursor(q) catch {
        conn.close();
        return null;
    };
    defer cur.close(); // closes the connection
    const b = (try cur.nextBatch(arena)) orelse return null;
    if (b.len == 0) return null;
    const lo = dayOf(b.columns[0].getValue(0)) orelse return null;
    const hi = dayOf(b.columns[1].getValue(0)) orelse return null;
    if (hi <= lo) return null; // empty table or single-day key: nothing to split
    return Bounds{ .min = lo, .max = hi };
}

fn dayOf(v: Value) ?i64 {
    return switch (v) {
        .date => |d| d,
        .timestamp => |us| @divFloor(us, 86_400_000_000),
        else => null,
    };
}

/// Equal-width day ranges over `[min_day, max_day]` rendered as date literals:
/// `col >= 'YYYY-MM-DD' AND col < 'YYYY-MM-DD'`, last slice open-ended. Works
/// for both DATE and DATETIME/TIMESTAMP keys: a timestamp inside the boundary
/// day falls in the slice whose half-open range contains its midnight-floored
/// day, so slices stay disjoint and covering.
fn dateRangePreds(arena: std.mem.Allocator, dialect: Dialect, col: []const u8, min_day: i64, max_day: i64, m_in: usize) ![]const []const u8 {
    const qcol = quoteIdent(arena, dialect, col) catch col;
    const span: i128 = @as(i128, max_day) - @as(i128, min_day) + 1;
    var m: usize = m_in;
    if (@as(i128, @intCast(m)) > span) m = @intCast(span); // at least one day per slice
    if (m <= 1) {
        const one = try std.fmt.allocPrint(arena, "{s} >= '{s}'", .{ qcol, try eval.formatDate(arena, min_day) });
        return try dupeOne(arena, one);
    }
    const width: i128 = @divTrunc(span + @as(i128, @intCast(m)) - 1, @as(i128, @intCast(m))); // ceil
    var list = std.array_list.Managed([]const u8).init(arena);
    var k: usize = 0;
    while (k < m) : (k += 1) {
        const lo: i64 = @intCast(@as(i128, min_day) + @as(i128, @intCast(k)) * width);
        if (k == m - 1) {
            try list.append(try std.fmt.allocPrint(arena, "{s} >= '{s}'", .{ qcol, try eval.formatDate(arena, lo) }));
        } else {
            const hi: i64 = @intCast(@as(i128, lo) + width);
            try list.append(try std.fmt.allocPrint(arena, "{s} >= '{s}' AND {s} < '{s}'", .{ qcol, try eval.formatDate(arena, lo), qcol, try eval.formatDate(arena, hi) }));
        }
    }
    return list.toOwnedSlice();
}

/// Equal lexicographic slices of the whole 128-bit UUID space. Random (v4) UUIDs
/// are uniform over this space, so the slices are balanced with no bounds probe.
fn uuidSpacePreds(arena: std.mem.Allocator, dialect: Dialect, col: []const u8, m: usize) ![]const []const u8 {
    const qcol = quoteIdent(arena, dialect, col) catch col;
    var list = std.array_list.Managed([]const u8).init(arena);
    var k: usize = 0;
    while (k < m) : (k += 1) {
        const lo = if (k == 0) null else try uuidAt(arena, k, m);
        const hi = if (k == m - 1) null else try uuidAt(arena, k + 1, m);
        if (lo == null) {
            try list.append(try std.fmt.allocPrint(arena, "{s} < '{s}'", .{ qcol, hi.? }));
        } else if (hi == null) {
            try list.append(try std.fmt.allocPrint(arena, "{s} >= '{s}'", .{ qcol, lo.? }));
        } else {
            try list.append(try std.fmt.allocPrint(arena, "{s} >= '{s}' AND {s} < '{s}'", .{ qcol, lo.?, qcol, hi.? }));
        }
    }
    return list.toOwnedSlice();
}

/// The k/m boundary of the UUID space as a canonical UUID string. Uses u256 to
/// compute `floor(k * 2^128 / m)` without overflow.
fn uuidAt(arena: std.mem.Allocator, k: usize, m: usize) ![]const u8 {
    const val: u128 = @intCast((@as(u256, k) << 128) / @as(u256, m));
    var bytes: [16]u8 = undefined;
    std.mem.writeInt(u128, &bytes, val, .big);
    return std.fmt.allocPrint(arena, "{x:0>2}{x:0>2}{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}", .{
        bytes[0],  bytes[1],  bytes[2],  bytes[3],
        bytes[4],  bytes[5],  bytes[6],  bytes[7],
        bytes[8],  bytes[9],  bytes[10], bytes[11],
        bytes[12], bytes[13], bytes[14], bytes[15],
    });
}

fn quoteIdent(arena: std.mem.Allocator, dialect: Dialect, name: []const u8) ![]const u8 {
    return switch (dialect) {
        .postgres => std.fmt.allocPrint(arena, "\"{s}\"", .{name}),
        .mysql => std.fmt.allocPrint(arena, "`{s}`", .{name}),
        .sqlserver => std.fmt.allocPrint(arena, "[{s}]", .{name}),
    };
}

fn dupeOne(arena: std.mem.Allocator, s: []const u8) ![]const []const u8 {
    const out = try arena.alloc([]const u8, 1);
    out[0] = s;
    return out;
}

// ---------------------------------------------------------------------------
// Tests (pure: predicate math — coverage & disjointness)
// ---------------------------------------------------------------------------

test "int range splits are covering and disjoint" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    // [1, 1000] into 7 slices: every id in range hits exactly one predicate.
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
    // A row inserted past max is still captured by the open-ended last slice.
    var hits_over: usize = 0;
    for (preds) |p| if (intPredHolds(p, 5000)) {
        hits_over += 1;
    };
    try std.testing.expectEqual(@as(usize, 1), hits_over);
}

test "int range clamps slice count to the value span" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const preds = try intRangePreds(a, .postgres, "id", 1, 3, 8);
    try std.testing.expect(preds.len <= 3);
}

test "uuid space splits are ordered and cover the endpoints" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const preds = try uuidSpacePreds(a, .postgres, "id", 4);
    try std.testing.expect(preds.len == 4);
    // First slice is open-below, last is open-above; boundaries are monotonic.
    try std.testing.expect(std.mem.indexOf(u8, preds[0], ">=") == null);
    try std.testing.expect(std.mem.startsWith(u8, preds[3], "\"id\" >= "));
    const b1 = try uuidAt(a, 1, 4);
    const b2 = try uuidAt(a, 2, 4);
    try std.testing.expect(std.mem.order(u8, b1, b2) == .lt);
}

test "date range splits are covering, disjoint, and day-aligned" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    // 2024-01-01 (19723) .. 2024-12-31 (20088) into 4 slices.
    const preds = try dateRangePreds(a, .mysql, "updated_at", 19723, 20088, 4);
    try std.testing.expectEqual(@as(usize, 4), preds.len);
    try std.testing.expectEqualStrings("`updated_at` >= '2024-01-01' AND `updated_at` < '2024-04-02'", preds[0]);
    try std.testing.expect(std.mem.endsWith(u8, preds[3], ">= '2024-10-03'")); // 2024-01-01 + 3*ceil(366/4) days
    // Boundaries chain: each slice's upper bound is the next slice's lower bound.
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
    try std.testing.expect(preds.len <= 3);
}

test "dayOf converts date and timestamp values" {
    try std.testing.expectEqual(@as(?i64, 19723), dayOf(.{ .date = 19723 }));
    try std.testing.expectEqual(@as(?i64, 19723), dayOf(.{ .timestamp = 19723 * 86_400_000_000 + 3_600_000_000 }));
    try std.testing.expectEqual(@as(?i64, null), dayOf(.{ .string = "2024-01-01" }));
}

/// Minimal evaluator for the int predicates this module emits, for tests only:
/// `"id" >= LO` or `"id" >= LO AND "id" < HI`.
fn intPredHolds(pred: []const u8, id: i64) bool {
    var lo: i64 = std.math.minInt(i64);
    var hi: ?i64 = null;
    var it = std.mem.splitSequence(u8, pred, " AND ");
    while (it.next()) |part| {
        const ge = std.mem.indexOf(u8, part, ">= ");
        const lt = std.mem.indexOf(u8, part, "< ");
        if (ge) |i| {
            lo = std.fmt.parseInt(i64, part[i + 3 ..], 10) catch unreachable;
        } else if (lt) |i| {
            hi = std.fmt.parseInt(i64, part[i + 2 ..], 10) catch unreachable;
        }
    }
    return id >= lo and (hi == null or id < hi.?);
}
