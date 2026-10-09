//! Size estimates of a join side, for choosing which side to hold in memory and
//! in which order to run a chain of joins. Best effort and never failing the run:
//! anything not known comes back as unknown, and an unknown side keeps the join
//! as written.
//!
//! A side is estimated from its head read once its bindings are inlined: a local
//! Parquet file by its footer's row count, a local line-per-row text file (CSV,
//! TSV, JSON lines) by its size and the newlines in its first 64 KiB, any other
//! local file by its size alone, a SQL table by its catalog's statistics
//! (`pg_class.reltuples`, `information_schema.TABLES.TABLE_ROWS`, `sys.partitions`),
//! and a materialized binding by its rows. Between the read and the join only
//! filters, selects and a limit may stand: a filter keeps the raw estimate (its
//! selectivity is not modelled), a limit caps it, and anything else (a join, an
//! aggregate, a union) makes the side unknown. A remote file, a SQL query, a REST
//! read or a folder is unknown too.
//!
//! A catalog answer is a statistic, not a count: a table never analyzed reports
//! nothing or a negative or zero count, all read as unknown. Each table is asked
//! once per run (cached with the catalog facts), on its own connection, and any
//! failure, from connecting to an unexpected value, is unknown and leaves the
//! run's diagnostics as they were.

const Env = @import("env.zig").Env;
const SqlConnInfo = @import("connect.zig").SqlConnInfo;
const Value = @import("../exec/value.zig").Value;
const ast = @import("../lang/ast.zig");
const connectSql = @import("connect.zig").connectSql;
const footerOf = @import("../format/parquet/folder.zig").parseFooterOf;
const inlineHeadBindings = @import("plan.zig").inlineHeadBindings;
const pqread = @import("../format/parquet/read.zig");
const resolveDbConfig = @import("connect/dbconfig.zig").resolveDbConfig;
const sql = @import("../db/sql.zig");
const sqlConnInfo = @import("connect.zig").sqlConnInfo;
const std = @import("std");

pub const Estimate = struct {
    rows: ?u64 = null,
    bytes: ?u64 = null,

    pub fn known(self: Estimate) bool {
        return self.rows != null or self.bytes != null;
    }
};

pub const Unit = enum { rows, bytes };

/// Two estimates in a unit both carry, rows before bytes; null when they share none.
pub const Pair = struct { left: u64, right: u64, unit: Unit };

pub fn pair(left: Estimate, right: Estimate) ?Pair {
    if (left.rows != null and right.rows != null) return .{ .left = left.rows.?, .right = right.rows.?, .unit = .rows };
    if (left.bytes != null and right.bytes != null) return .{ .left = left.bytes.?, .right = right.bytes.?, .unit = .bytes };
    return null;
}

const sample_len = 64 << 10;

/// The estimate of a side's stages, head first.
pub fn ofSide(env: *Env, stages_in: []const ast.Stage) Estimate {
    if (stages_in.len == 0) return .{};
    const stages = inlineHeadBindings(env, stages_in) catch return .{};
    var est: Estimate = switch (stages[0].node) {
        .read => |rd| ofRead(env, rd),
        .ref => |name| if (env.materialized.get(name)) |m| blk: {
            var n: u64 = 0;
            for (m.batches) |b| n += b.len;
            break :blk .{ .rows = n };
        } else .{},
        else => .{},
    };
    for (stages[1..]) |st| switch (st.node) {
        .filter, .select => {},
        .limit => |l| if (est.rows) |r| {
            est.rows = @min(r, l.count);
        },
        else => return .{},
    };
    return est;
}

pub fn ofRead(env: *Env, rd: ast.Read) Estimate {
    return switch (rd.form) {
        .path => |p| if (std.mem.eql(u8, rd.connector, "csv")) ofFile(p) else .{},
        .table => .{ .rows = sqlRows(env, rd) },
        else => .{},
    };
}

/// A local file's size, and its rows where the format tells them cheaply.
pub fn ofFile(path: []const u8) Estimate {
    if (std.mem.indexOf(u8, path, "://") != null) return .{};
    const st = std.fs.cwd().statFile(path) catch return .{};
    if (st.kind != .file) return .{};
    var est = Estimate{ .bytes = st.size };
    if (pqread.Reader.isPath(path)) {
        est.rows = parquetRows(path);
    } else if (lineFormat(path)) {
        est.rows = sampledRows(path, st.size);
    }
    return est;
}

fn lineFormat(path: []const u8) bool {
    const exts = [_][]const u8{ ".csv", ".tsv", ".txt", ".psv", ".jsonl", ".ndjson" };
    for (exts) |e| {
        if (path.len >= e.len and std.ascii.eqlIgnoreCase(path[path.len - e.len ..], e)) return true;
    }
    return false;
}

fn parquetRows(path: []const u8) ?u64 {
    var ar = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer ar.deinit();
    const src = pqread.Bytes.open(ar.allocator(), path) catch return null;
    defer src.close();
    var footer_start: u64 = 0;
    const md = footerOf(ar.allocator(), src, &footer_start) catch return null;
    return std.math.cast(u64, md.num_rows);
}

/// Every row of a small file, else the newlines of the first `sample_len` bytes
/// scaled to the file's size.
fn sampledRows(path: []const u8, size: u64) ?u64 {
    const f = std.fs.cwd().openFile(path, .{}) catch return null;
    defer f.close();
    const buf = std.heap.page_allocator.alloc(u8, sample_len) catch return null;
    defer std.heap.page_allocator.free(buf);
    const n = f.readAll(buf) catch return null;
    if (n == 0) return 0;
    const lines: u64 = std.mem.count(u8, buf[0..n], "\n");
    if (n >= size) return lines + @intFromBool(buf[n - 1] != '\n');
    if (lines == 0) return 1;
    return std.math.cast(u64, std.math.mulWide(u64, size, lines) / n);
}

/// A SQL table's row count from its catalog, asked once per run; null when the
/// catalog has none or cannot be asked.
pub fn sqlRows(env: *Env, rd: ast.Read) ?u64 {
    if (rd.form != .table) return null;
    const conn = env.connections.get(rd.connector) orelse return null;
    const info = sqlConnInfo(conn) orelse return null;
    const cache = env.facts_cache orelse return null;
    cache.mu.lock();
    defer cache.mu.unlock();
    const ca = cache.arena.allocator();
    const key = std.mem.join(ca, "\x00", &.{ rd.connector, std.mem.join(ca, ".", rd.form.table.parts) catch return null }) catch return null;
    if (cache.rows.get(key)) |r| return r;
    const got = askCatalog(env, conn, info, rd.form.table.parts) catch |e| blk: {
        env.log.log(.debug, "row estimate for {s}: not available ({s})", .{ key[rd.connector.len + 1 ..], @errorName(e) });
        break :blk null;
    };
    cache.rows.put(ca, key, got) catch {};
    return got;
}

fn askCatalog(env: *Env, conn: ast.Connection, info: SqlConnInfo, parts: []const []const u8) !?u64 {
    const saved = env.diag.*;
    defer env.diag.* = saved;
    const cfg = try resolveDbConfig(env, conn, info.port);
    const q = try statsQuery(env.arena, info.dialect, parts);
    const c = try connectSql(env.gpa, info.kind, cfg);
    var cur = c.queryCursor(q) catch |e| {
        c.close();
        return e;
    };
    defer cur.close();
    var scratch = std.heap.ArenaAllocator.init(env.gpa);
    defer scratch.deinit();
    var out: ?u64 = null;
    while (try cur.nextBatch(scratch.allocator())) |b| {
        if (out == null and b.len > 0 and b.columns.len > 0) out = valueRows(b.columns[0].getValue(0));
    }
    return out;
}

/// A catalog's count as rows: positive numbers only, as zero and below mean
/// "never analyzed" as often as "empty".
pub fn valueRows(v: Value) ?u64 {
    const f: f64 = switch (v) {
        .int => |i| @floatFromInt(i),
        .float => |x| x,
        .decimal => |d| d.toF64(),
        .string => |s| std.fmt.parseFloat(f64, std.mem.trim(u8, s, " ")) catch return null,
        else => return null,
    };
    if (!(f >= 1) or f > 1e18) return null;
    return @intFromFloat(f);
}

/// The one-row, one-column catalog query giving a table's row statistic.
pub fn statsQuery(arena: std.mem.Allocator, dialect: sql.Dialect, parts: []const []const u8) ![]const u8 {
    const table = parts[parts.len - 1];
    switch (dialect) {
        .postgres => {
            const obj = try quoted(arena, parts, '"', '"');
            return std.fmt.allocPrint(arena, "SELECT CAST(c.reltuples AS BIGINT) FROM pg_class c WHERE c.oid = to_regclass({s})", .{try sqlLit(arena, obj)});
        },
        .sqlserver => {
            const obj = try quoted(arena, parts, '[', ']');
            return std.fmt.allocPrint(arena, "SELECT SUM(p.rows) FROM sys.partitions p WHERE p.object_id = OBJECT_ID({s}) AND p.index_id IN (0, 1)", .{try sqlLit(arena, obj)});
        },
        .mysql, .starrocks, .doris => {
            const schema = if (parts.len >= 2) try sqlLit(arena, parts[parts.len - 2]) else "DATABASE()";
            return std.fmt.allocPrint(arena, "SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA = {s} AND TABLE_NAME = {s}", .{ schema, try sqlLit(arena, table) });
        },
    }
}

fn quoted(arena: std.mem.Allocator, parts: []const []const u8, open: u8, close: u8) ![]const u8 {
    var out = std.array_list.Managed(u8).init(arena);
    for (parts, 0..) |p, i| {
        if (i > 0) try out.append('.');
        try out.append(open);
        for (p) |ch| {
            if (ch == close) try out.append(close);
            try out.append(ch);
        }
        try out.append(close);
    }
    return out.toOwnedSlice();
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

/// `950`, `1.2k`, `3.4M`, `5.6G`, with ` rows` or ` bytes` after.
pub fn describe(arena: std.mem.Allocator, n: u64, unit: Unit) ![]const u8 {
    const f: f64 = @floatFromInt(n);
    const u = @tagName(unit);
    if (n < 1000) return std.fmt.allocPrint(arena, "{d} {s}", .{ n, u });
    if (n < 1_000_000) return std.fmt.allocPrint(arena, "{d:.1}k {s}", .{ f / 1e3, u });
    if (n < 1_000_000_000) return std.fmt.allocPrint(arena, "{d:.1}M {s}", .{ f / 1e6, u });
    return std.fmt.allocPrint(arena, "{d:.1}G {s}", .{ f / 1e9, u });
}

const testing = std.testing;

test "estimate: a parquet file by its footer's rows, a CSV by size and sampled lines, a missing file unknown" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const column = @import("../exec/column.zig");
    const types = @import("../lang/types.zig");
    const pqwrite = @import("../format/parquet/write.zig");

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realpathAlloc(a, ".");

    const pq = try std.fs.path.join(a, &.{ dir, "t.parquet" });
    const schema = types.Schema{ .fields = &.{.{ .name = "id", .ty = types.Type.init(.int) }} };
    var w = try pqwrite.Writer.open(a, pq, schema, .snappy, .truncate);
    var ids = try column.Builder.initCapacity(a, schema.fields[0].ty, 5);
    for (0..5) |i| try ids.append(.{ .int = @intCast(i) });
    var cols = [_]column.Column{try ids.finish()};
    try w.writeBatch(a, .{ .schema = &schema, .columns = &cols, .len = 5 });
    try w.close();
    const pe = ofFile(pq);
    try testing.expectEqual(@as(?u64, 5), pe.rows);
    try testing.expect(pe.bytes.? > 0);

    try tmp.dir.writeFile(.{ .sub_path = "small.csv", .data = "k,v\n1,a\n2,b" });
    const ce = ofFile(try std.fs.path.join(a, &.{ dir, "small.csv" }));
    try testing.expectEqual(@as(?u64, 3), ce.rows);
    try testing.expectEqual(@as(?u64, 11), ce.bytes);

    var big = std.array_list.Managed(u8).init(a);
    for (0..20_000) |i| try big.writer().print("{d},row\n", .{i % 10});
    try tmp.dir.writeFile(.{ .sub_path = "big.csv", .data = big.items });
    const be = ofFile(try std.fs.path.join(a, &.{ dir, "big.csv" }));
    try testing.expect(be.rows.? > 19_800 and be.rows.? < 20_200);

    try tmp.dir.writeFile(.{ .sub_path = "book.xlsx", .data = "not really" });
    const xe = ofFile(try std.fs.path.join(a, &.{ dir, "book.xlsx" }));
    try testing.expectEqual(@as(?u64, null), xe.rows);
    try testing.expectEqual(@as(?u64, 10), xe.bytes);

    try testing.expect(!ofFile(try std.fs.path.join(a, &.{ dir, "missing.csv" })).known());
    try testing.expect(!ofFile(dir).known());
    try testing.expect(!ofFile("s3://bucket/x.parquet").known());
}

test "estimate: rows compare with rows, bytes with bytes, and nothing across units" {
    const p = pair(.{ .rows = 10, .bytes = 400 }, .{ .rows = 90, .bytes = 100 }).?;
    try testing.expectEqual(Unit.rows, p.unit);
    try testing.expectEqual(@as(u64, 10), p.left);
    const b = pair(.{ .bytes = 400 }, .{ .rows = 90, .bytes = 100 }).?;
    try testing.expectEqual(Unit.bytes, b.unit);
    try testing.expect(pair(.{ .rows = 4 }, .{ .bytes = 100 }) == null);
    try testing.expect(pair(.{}, .{ .rows = 1 }) == null);
}

test "estimate: the catalog query per dialect, quoting each name" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try testing.expectEqualStrings(
        "SELECT CAST(c.reltuples AS BIGINT) FROM pg_class c WHERE c.oid = to_regclass('\"public\".\"o''k\"\"s\"')",
        try statsQuery(a, .postgres, &.{ "public", "o'k\"s" }),
    );
    try testing.expectEqualStrings(
        "SELECT SUM(p.rows) FROM sys.partitions p WHERE p.object_id = OBJECT_ID('[dbo].[a]]b]') AND p.index_id IN (0, 1)",
        try statsQuery(a, .sqlserver, &.{ "dbo", "a]b" }),
    );
    try testing.expectEqualStrings(
        "SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'erp' AND TABLE_NAME = 't'",
        try statsQuery(a, .mysql, &.{ "erp", "t" }),
    );
    try testing.expectEqualStrings(
        "SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 't'",
        try statsQuery(a, .starrocks, &.{"t"}),
    );
    try testing.expectEqualStrings(
        "SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'd' AND TABLE_NAME = 't'",
        try statsQuery(a, .doris, &.{ "d", "t" }),
    );
}

test "estimate: a catalog value counts only as a positive number" {
    try testing.expectEqual(@as(?u64, 42), valueRows(.{ .int = 42 }));
    try testing.expectEqual(@as(?u64, 1200), valueRows(.{ .float = 1200.4 }));
    try testing.expectEqual(@as(?u64, 7), valueRows(.{ .string = " 7 " }));
    try testing.expectEqual(@as(?u64, null), valueRows(.{ .int = -1 }));
    try testing.expectEqual(@as(?u64, null), valueRows(.{ .int = 0 }));
    try testing.expectEqual(@as(?u64, null), valueRows(.null));
    try testing.expectEqual(@as(?u64, null), valueRows(.{ .string = "n/a" }));
}

test "estimate: a SQL table whose catalog cannot be asked is unknown, asked once, the diagnostics untouched" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const parser = @import("../lang/sql_parser.zig");
    const env_mod = @import("env.zig");
    const obs = @import("obs.zig");
    const op = @import("../exec/op.zig");

    var pd: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(a,
        \\CREATE CONNECTION pg TYPE postgres OPTIONS (host = '127.0.0.1', port = 1, user = 'u', password = 'p', database = 'd');
        \\SELECT 1 AS x;
    , &pd);
    var params = std.StringHashMap(Value).init(a);
    var bindings = std.StringHashMap(ast.Pipeline).init(a);
    var connections = std.StringHashMap(ast.Connection).init(a);
    for (prog.stmts) |st| switch (st) {
        .connection => |c| try connections.put(c.name, c),
        else => {},
    };
    var sources = std.array_list.Managed(@import("../connect/driver.zig").Source).init(a);
    var diag = env_mod.Diag{};
    var log = obs.Logger.init(0, .text, .err);
    var params_expr = std.StringHashMap(*const ast.Expr).init(a);
    var errctx = op.ErrCtx{};
    var rows = obs.RowCounter.init(0);
    var json = std.StringHashMap(std.json.Value).init(a);
    const fns = std.StringHashMap(ast.FnDecl).init(a);
    var cache = env_mod.FactsCache.init(testing.allocator);
    defer cache.deinit();
    var env = Env{ .arena = a, .gpa = testing.allocator, .params = &params, .bindings = &bindings, .connections = &connections, .sources = &sources, .request_body = null, .diag = &diag, .log = &log, .params_expr = &params_expr, .errctx = &errctx, .rows_read = &rows, .json_params = &json, .fns = &fns, .facts_cache = &cache };

    const rd = ast.Read{ .connector = "pg", .form = .{ .table = .{ .parts = &.{ "public", "t" } } } };
    try testing.expectEqual(@as(?u64, null), sqlRows(&env, rd));
    try testing.expectEqual(@as(usize, 1), cache.rows.count());
    try testing.expectEqual(@as(?u64, null), sqlRows(&env, rd));
    try testing.expectEqual(@as(usize, 0), diag.msg.len);
    try testing.expect(!ofRead(&env, .{ .connector = "pg", .form = .{ .query = "SELECT 1" } }).known());
    try testing.expect(!ofRead(&env, .{ .connector = "nope", .form = .{ .table = .{ .parts = &.{"t"} } } }).known());
}

test "estimate: describe rounds to k, M and G" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try testing.expectEqualStrings("950 rows", try describe(a, 950, .rows));
    try testing.expectEqualStrings("1.2k rows", try describe(a, 1234, .rows));
    try testing.expectEqualStrings("3.4M bytes", try describe(a, 3_400_000, .bytes));
    try testing.expectEqualStrings("5.6G rows", try describe(a, 5_600_000_000, .rows));
}
