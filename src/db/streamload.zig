//! Stream Load sink, for the databases that load this way — StarRocks, and the
//! project it forked from. Two channels:
//!   - MySQL protocol -> FE (9030): DDL (CREATE TABLE IF NOT EXISTS, TRUNCATE).
//!   - HTTP Stream Load -> BE/FE: the actual data load.
//! The write mode selects the table model: append/overwrite -> Duplicate Key,
//! `upsert on k` -> Primary Key (keys reordered first + NOT NULL). What differs
//! between the databases — DDL, a header's spelling — is the `Flavor`. Doris
//! splits rows on newlines unless told `line_delimiter` (it ignores
//! `row_delimiter`), and loads non-strict by default, turning a bad value into a
//! silent NULL; both are set explicitly.
//!
//! The body is fields separated by 0x01 and rows by 0x02, control bytes Stream
//! Load does no quoting for; `\n` would let an embedded newline split a row.
//! Stray 0x01/0x02 bytes in source text become spaces. NULL is `\N`, which has no
//! escape in StarRocks CSV, so a value that is exactly `\N` is refused rather than
//! silently loaded as NULL; loads run at max_filter_ratio=0, never partial.
//!
//! Each flush has a label, which makes it at-most-once within a run. Labels do not
//! give cross-run idempotency (split ranges are reassigned to lanes
//! non-deterministically); exactly-once across runs is owned by downstream dedup
//! on a primary-key table, per the split.zig contract.
//!
//! `logger` and `errctx` are set by the runtime after `open`, so a refused load or
//! DDL carries the server's own message (e.g. the missing privilege) into the
//! run's error instead of only the log. Auto-create creates only what is missing:
//! StarRocks checks the CREATE privilege before `IF NOT EXISTS` can no-op, so a
//! role allowed only to load would otherwise be refused.
//!
//! The pure logic (type mapping, DDL, body, labels, auth) is unit-tested; the
//! network paths need a live server.

const std = @import("std");
const types = @import("../lang/types.zig");
const ast = @import("../lang/ast.zig");
const Batch = @import("../exec/batch.zig").Batch;
const driver = @import("../connect/driver.zig");
const mysql = @import("mysql.zig");
const sql = @import("sql.zig");
const obs = @import("../runtime/obs.zig");
const op = @import("../exec/op.zig");

const FLUSH_BYTES = 8 * 1024 * 1024;

pub const Flavor = enum {
    starrocks,
    doris,

    pub fn of(connector: []const u8) ?Flavor {
        return std.meta.stringToEnum(Flavor, connector);
    }

    pub fn name(self: Flavor) []const u8 {
        return @tagName(self);
    }

    pub fn dialect(self: Flavor) sql.Dialect {
        return switch (self) {
            .starrocks => .starrocks,
            .doris => .doris,
        };
    }

    fn rowDelimiterHeader(self: Flavor) []const u8 {
        return switch (self) {
            .starrocks => "row_delimiter",
            .doris => "line_delimiter",
        };
    }

    fn partialHeader(self: Flavor) []const u8 {
        return switch (self) {
            .starrocks => "partial_update",
            .doris => "partial_columns",
        };
    }

    fn strict(self: Flavor) bool {
        return self == .doris;
    }
};

pub const Config = struct {
    flavor: Flavor = .starrocks,
    fe_host: []const u8 = "127.0.0.1",
    fe_port: u16 = 9030,
    load_url: []const u8 = "http://127.0.0.1:8040",
    database: []const u8,
    user: []const u8 = "root",
    password: []const u8 = "",
    buckets: u32 = 4,
    replication_num: u32 = 1,
    auto_create: bool = true,
    label_prefix: []const u8 = "basalt",
    run_id: u64 = 0,
    errctx: ?*op.ErrCtx = null,
};

/// A single-quoted literal for the FE's MySQL protocol: `'` and `\` doubled.
fn appendStrLit(out: *std.array_list.Managed(u8), s: []const u8) !void {
    try out.append('\'');
    for (s) |c| switch (c) {
        '\'' => try out.appendSlice("''"),
        '\\' => try out.appendSlice("\\\\"),
        else => try out.append(c),
    };
    try out.append('\'');
}

pub fn srType(arena: std.mem.Allocator, t: types.Type) ![]const u8 {
    return sql.Dialect.starrocks.ddlType(arena, t, false);
}

pub fn genCreateTable(
    arena: std.mem.Allocator,
    flavor: Flavor,
    db: []const u8,
    table: []const u8,
    schema: types.Schema,
    mode: ast.WriteMode,
    buckets: u32,
    replication_num: u32,
) ![]const u8 {
    const qtable = try std.fmt.allocPrint(arena, "`{s}`.`{s}`", .{ db, table });
    return sql.createTableSqlWith(arena, flavor.dialect(), qtable, schema, mode, .{ .buckets = buckets, .replication_num = replication_num });
}

/// `<prefix>_<table>_<run_id>_<seq>`, with every byte outside `[-\w]` folded to `_`:
/// a qualified target's dot once got the load rejected with "Label format error".
/// Over 128 bytes the head is cut, keeping the run id and sequence that make it unique.
pub fn genLabel(arena: std.mem.Allocator, prefix: []const u8, table: []const u8, run_id: u64, seq: u64) ![]const u8 {
    const raw = try std.fmt.allocPrint(arena, "{s}_{s}_{d}_{d}", .{ prefix, table, run_id, seq });
    for (raw) |*c| {
        const ok = std.ascii.isAlphanumeric(c.*) or c.* == '_' or c.* == '-';
        if (!ok) c.* = '_';
    }
    if (raw.len > 128) return raw[raw.len - 128 ..];
    return raw;
}

/// Backtick-quoted: source field names can hold spaces or symbols the `columns`
/// header's parser would otherwise reject.
pub fn columnList(arena: std.mem.Allocator, schema: types.Schema) ![]const u8 {
    return sql.colList(arena, .starrocks, schema);
}

const NULL_MARKER = "\\N";

pub fn appendBatchTsv(w: anytype, arena: std.mem.Allocator, batch: Batch) !void {
    var r: usize = 0;
    while (r < batch.len) : (r += 1) {
        for (batch.columns, 0..) |*c, i| {
            if (i > 0) try w.writeByte(0x01);
            const v = c.getValue(r);
            if (v.isNull()) {
                try w.writeAll(NULL_MARKER);
            } else {
                const s = try sql.valueText(arena, v, .{ .bool_true = "true", .bool_false = "false" });
                if (std.mem.eql(u8, s, NULL_MARKER)) return error.StreamLoadNullMarkerInData;
                try writeSanitized(w, s);
            }
        }
        try w.writeByte(0x02);
    }
}

fn writeSanitized(w: anytype, s: []const u8) !void {
    var start: usize = 0;
    for (s, 0..) |b, i| {
        if (b == 0x01 or b == 0x02) {
            try w.writeAll(s[start..i]);
            try w.writeByte(' ');
            start = i + 1;
        }
    }
    try w.writeAll(s[start..]);
}

pub const StreamLoadSink = struct {
    gpa: std.mem.Allocator,
    cfg: Config,
    db: []const u8,
    table: []const u8,
    columns: []const u8,
    mode: ast.WriteMode,
    buffer: std.array_list.Managed(u8),
    seq: u64 = 0,
    run_id: u64 = 0,
    client: std.http.Client,
    logger: ?*obs.Logger = null,
    errctx: ?*op.ErrCtx = null,

    pub fn open(gpa: std.mem.Allocator, cfg: Config, table: []const u8, schema: types.Schema, mode: ast.WriteMode) !*StreamLoadSink {
        const self = try gpa.create(StreamLoadSink);
        errdefer gpa.destroy(self);
        const columns = try columnList(gpa, schema);
        errdefer gpa.free(columns);
        var cfg_owned = cfg;
        cfg_owned.label_prefix = try gpa.dupe(u8, cfg.label_prefix);
        errdefer gpa.free(cfg_owned.label_prefix);
        self.* = .{
            .gpa = gpa,
            .cfg = cfg_owned,
            .db = cfg.database,
            .table = table,
            .columns = columns,
            .mode = mode,
            .buffer = std.array_list.Managed(u8).init(gpa),
            .run_id = if (cfg.run_id != 0) cfg.run_id else @intCast(std.time.milliTimestamp()),
            .client = std.http.Client{ .allocator = gpa },
        };
        errdefer self.buffer.deinit();
        errdefer self.client.deinit();
        if (cfg.auto_create) {
            if (!try self.exists("information_schema.tables", "TABLE_SCHEMA", cfg.database, table)) {
                if (!try self.exists("information_schema.schemata", "SCHEMA_NAME", cfg.database, null)) {
                    const cdb = try std.fmt.allocPrint(gpa, "CREATE DATABASE IF NOT EXISTS `{s}`", .{cfg.database});
                    defer gpa.free(cdb);
                    try self.runDDL(cdb);
                }
                const ddl = try genCreateTable(gpa, cfg.flavor, cfg.database, table, schema, mode, cfg.buckets, cfg.replication_num);
                defer gpa.free(ddl);
                try self.runDDL(ddl);
            }

            if (mode == .overwrite) {
                const trunc = try std.fmt.allocPrint(gpa, "TRUNCATE TABLE `{s}`.`{s}`", .{ cfg.database, table });
                defer gpa.free(trunc);
                try self.runDDL(trunc);
            }
        }
        return self;
    }

    pub fn sink(self: *StreamLoadSink) driver.Sink {
        return .{ .ptr = self, .vtable = &sink_vtable };
    }

    fn runDDL(self: *StreamLoadSink, stmt: []const u8) !void {
        const conn = try mysql.Conn.connect(self.gpa, self.cfg.fe_host, self.cfg.fe_port, self.cfg.user, self.cfg.password, "", .off);
        defer conn.close();
        conn.exec(stmt) catch |e| {
            obs.logOr(self.logger, .err, "{s} DDL error: {s} (sql: {s})", .{ self.cfg.flavor.name(), conn.last_error, stmt });
            if (self.cfg.errctx) |ec| ec.set("{s} refused `{s}`: {s}", .{ self.cfg.flavor.name(), stmt, conn.last_error });
            return e;
        };
    }

    /// Whether `information_schema` lists the database (`table` null) or the table,
    /// matched by equality since `_` is a LIKE wildcard. A catalog that cannot be
    /// asked answers "no", leaving the decision to the `IF NOT EXISTS` DDL.
    fn exists(self: *StreamLoadSink, view: []const u8, schema_col: []const u8, db: []const u8, table: ?[]const u8) !bool {
        var q = std.array_list.Managed(u8).init(self.gpa);
        defer q.deinit();
        try q.writer().print("SELECT COUNT(*) FROM {s} WHERE {s} = ", .{ view, schema_col });
        try appendStrLit(&q, db);
        if (table) |t| {
            try q.appendSlice(" AND TABLE_NAME = ");
            try appendStrLit(&q, t);
        }
        const conn = mysql.Conn.connect(self.gpa, self.cfg.fe_host, self.cfg.fe_port, self.cfg.user, self.cfg.password, "", .off) catch return false;
        var cur = conn.sqlConn().queryCursor(q.items) catch {
            conn.close();
            return false;
        };
        defer cur.close();
        var arena = std.heap.ArenaAllocator.init(self.gpa);
        defer arena.deinit();
        const b = (cur.nextBatch(arena.allocator()) catch return false) orelse return false;
        if (b.len == 0) return false;
        return switch (b.columns[0].getValue(0)) {
            .int => |n| n > 0,
            .string => |s| !std.mem.eql(u8, std.mem.trim(u8, s, " "), "0"),
            else => false,
        };
    }

    pub fn writeBatch(self: *StreamLoadSink, arena: std.mem.Allocator, batch: Batch) !void {
        try appendBatchTsv(self.buffer.writer(), arena, batch);
        if (self.buffer.items.len >= FLUSH_BYTES) try self.flush();
    }

    pub fn close(self: *StreamLoadSink) !void {
        defer self.teardown();
        try self.flush();
    }

    /// Drops the buffered body; loads already sent are committed and stay.
    pub fn abort(self: *StreamLoadSink) void {
        self.teardown();
    }

    fn teardown(self: *StreamLoadSink) void {
        self.client.deinit();
        self.buffer.deinit();
        self.gpa.free(self.columns);
        self.gpa.free(self.cfg.label_prefix);
        self.gpa.destroy(self);
    }

    fn flush(self: *StreamLoadSink) !void {
        if (self.buffer.items.len == 0) return;
        self.seq += 1;
        var attempt: usize = 0;
        while (true) {
            self.streamLoad() catch |e| {
                attempt += 1;
                if (attempt >= 3 or !driver.transientNet(e)) return e;
                std.Thread.sleep(attempt * 500 * std.time.ns_per_ms);
                continue;
            };
            break;
        }
        self.buffer.clearRetainingCapacity();
    }

    fn streamLoad(self: *StreamLoadSink) !void {
        const url = try std.fmt.allocPrint(self.gpa, "{s}/api/{s}/{s}/_stream_load", .{ self.cfg.load_url, self.db, self.table });
        defer self.gpa.free(url);
        const label = try genLabel(self.gpa, self.cfg.label_prefix, self.table, self.run_id, self.seq);
        defer self.gpa.free(label);

        const cred = try std.fmt.allocPrint(self.gpa, "{s}:{s}", .{ self.cfg.user, self.cfg.password });
        defer self.gpa.free(cred);
        var enc: [512]u8 = undefined;
        const b64 = std.base64.standard.Encoder.encode(&enc, cred);
        const auth = try std.fmt.allocPrint(self.gpa, "Basic {s}", .{b64});
        defer self.gpa.free(auth);

        var hdrs = std.array_list.Managed(std.http.Header).init(self.gpa);
        defer hdrs.deinit();
        try hdrs.append(.{ .name = "Authorization", .value = auth });
        try hdrs.append(.{ .name = "label", .value = label });
        try hdrs.append(.{ .name = "format", .value = "CSV" });
        try hdrs.append(.{ .name = "column_separator", .value = "\\x01" });
        try hdrs.append(.{ .name = self.cfg.flavor.rowDelimiterHeader(), .value = "\\x02" });
        try hdrs.append(.{ .name = "columns", .value = self.columns });
        try hdrs.append(.{ .name = "max_filter_ratio", .value = "0" });
        if (self.cfg.flavor.strict()) try hdrs.append(.{ .name = "strict_mode", .value = "true" });
        if (self.mode == .upsert and self.mode.upsert.partial != null) {
            try hdrs.append(.{ .name = self.cfg.flavor.partialHeader(), .value = "true" });
        }

        var body_aw = std.Io.Writer.Allocating.init(self.gpa);
        defer body_aw.deinit();
        const res = self.client.fetch(.{
            .method = .PUT,
            .location = .{ .url = url },
            .extra_headers = hdrs.items,
            .payload = self.buffer.items,
            .response_writer = &body_aw.writer,
        }) catch |e| {
            obs.logOr(self.logger, .err, "stream load PUT failed ({s}): {s} ({d} bytes)", .{ @errorName(e), url, self.buffer.items.len });
            return e;
        };
        const body = body_aw.writer.buffered();
        if (!loadSucceeded(body)) {
            obs.logOr(self.logger, .err, "stream load failed (http {d}): {s}", .{ @intFromEnum(res.status), body });
            if (self.errctx) |ec| {
                const why = jsonField(body, "Message") orelse jsonField(body, "Status") orelse "no reason given";
                if (jsonField(body, "ErrorURL")) |u|
                    ec.set("stream load failed: {s} (rejected rows: {s})", .{ why, u })
                else
                    ec.set("stream load failed: {s}", .{why});
            }
            return error.StreamLoadFailed;
        }
    }
};

/// One string field of a flat JSON object, unescaped only as far as the server's
/// own responses need; not a general JSON parser.
fn jsonField(body: []const u8, name: []const u8) ?[]const u8 {
    var needle_buf: [64]u8 = undefined;
    const needle = std.fmt.bufPrint(&needle_buf, "\"{s}\"", .{name}) catch return null;
    const at = std.mem.indexOf(u8, body, needle) orelse return null;
    var i = at + needle.len;
    while (i < body.len and (body[i] == ' ' or body[i] == ':')) i += 1;
    if (i >= body.len or body[i] != '"') return null;
    i += 1;
    const start = i;
    while (i < body.len and body[i] != '"') {
        if (body[i] == '\\') i += 1;
        i += 1;
    }
    if (i > body.len) return null;
    return body[start..@min(i, body.len)];
}

test "jsonField pulls the reason out of a StarRocks failure body" {
    const body =
        \\{"Status": "Fail", "Message": "Access denied; you need (at least one of) the INSERT privilege(s)", "ErrorURL": "http://cn:8040/api/_load_error_log?file=x"}
    ;
    try std.testing.expectEqualStrings("Fail", jsonField(body, "Status").?);
    try std.testing.expectEqualStrings("Access denied; you need (at least one of) the INSERT privilege(s)", jsonField(body, "Message").?);
    try std.testing.expectEqualStrings("http://cn:8040/api/_load_error_log?file=x", jsonField(body, "ErrorURL").?);
    try std.testing.expect(jsonField(body, "Absent") == null);
    try std.testing.expect(jsonField("{\"Status\": 7}", "Status") == null);
}

fn loadSucceeded(body: []const u8) bool {
    return std.mem.indexOf(u8, body, "Success") != null or
        std.mem.indexOf(u8, body, "Publish Timeout") != null or
        std.mem.indexOf(u8, body, "Label Already Exists") != null;
}

test "loadSucceeded accepts success, publish timeout, and duplicate label" {
    try std.testing.expect(loadSucceeded("{\"Status\": \"Success\"}"));
    try std.testing.expect(loadSucceeded("{\"Status\": \"Publish Timeout\"}"));
    try std.testing.expect(loadSucceeded("{\"Status\": \"Label Already Exists\", \"ExistingJobStatus\": \"FINISHED\"}"));
    try std.testing.expect(!loadSucceeded("{\"Status\": \"Fail\", \"Message\": \"too many filtered rows\"}"));
}

const sink_vtable = driver.sinkVTable(StreamLoadSink);

test "type mapping" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings("BIGINT", try srType(a, types.Type.init(.int)));
    try std.testing.expectEqualStrings("VARCHAR(65533)", try srType(a, types.Type.init(.string)));
    try std.testing.expectEqualStrings("DOUBLE", try srType(a, types.Type.init(.float)));
    try std.testing.expectEqualStrings("DECIMAL(10,2)", try srType(a, types.Type.decimal(10, 2)));
    try std.testing.expectEqualStrings("DATETIME", try srType(a, types.Type.init(.timestamp)));
}

test "create table: append -> Duplicate Key" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const schema = types.Schema{ .fields = &.{
        .{ .name = "id", .ty = types.Type.init(.int) },
        .{ .name = "name", .ty = types.Type.init(.string) },
    } };
    const stmt = try genCreateTable(a, .starrocks, "warehouse", "orders", schema, .append, 4, 1);
    try std.testing.expect(std.mem.indexOf(u8, stmt, "CREATE TABLE IF NOT EXISTS `warehouse`.`orders`") != null);
    try std.testing.expect(std.mem.indexOf(u8, stmt, "`id` BIGINT") != null);
    try std.testing.expect(std.mem.indexOf(u8, stmt, "`name` VARCHAR(65533)") != null);
    try std.testing.expect(std.mem.indexOf(u8, stmt, "DUPLICATE KEY(`id`)") != null);
    try std.testing.expect(std.mem.indexOf(u8, stmt, "BUCKETS 4") != null);
}

test "create table: inferred upsert with unresolved (empty) keys errors" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const schema = types.Schema{ .fields = &.{
        .{ .name = "id", .ty = types.Type.init(.int) },
    } };
    const mode = ast.WriteMode{ .upsert = .{ .keys = &.{}, .partial = null } };
    try std.testing.expectError(error.UpsertKeysUnresolved, genCreateTable(ar.allocator(), .starrocks, "db", "t", schema, mode, 4, 1));
}

test "create table: composite inferred upsert -> multi-col PRIMARY KEY, ordered first" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const schema = types.Schema{ .fields = &.{
        .{ .name = "payload", .ty = types.Type.init(.string) },
        .{ .name = "emp", .ty = types.Type.init(.string) },
        .{ .name = "recno", .ty = types.Type.init(.int) },
    } };
    const mode = ast.WriteMode{ .upsert = .{ .keys = &.{ "emp", "recno" }, .partial = null } };
    const stmt = try genCreateTable(ar.allocator(), .starrocks, "bronze", "t", schema, mode, 4, 1);
    try std.testing.expect(std.mem.indexOf(u8, stmt, "PRIMARY KEY(`emp`,`recno`)") != null);
    try std.testing.expect(std.mem.indexOf(u8, stmt, "`emp` VARCHAR(65533) NOT NULL") != null);
    try std.testing.expect(std.mem.indexOf(u8, stmt, "`recno` BIGINT NOT NULL") != null);
    const epos = std.mem.indexOf(u8, stmt, "`emp`").?;
    const ppos = std.mem.indexOf(u8, stmt, "`payload`").?;
    try std.testing.expect(epos < ppos);
}

test "create table: upsert -> Primary Key, keys first + NOT NULL" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const schema = types.Schema{ .fields = &.{
        .{ .name = "name", .ty = types.Type.init(.string) },
        .{ .name = "id", .ty = types.Type.init(.int) },
    } };
    const mode = ast.WriteMode{ .upsert = .{ .keys = &.{"id"} } };
    const stmt = try genCreateTable(a, .starrocks, "warehouse", "orders", schema, mode, 4, 1);
    try std.testing.expect(std.mem.indexOf(u8, stmt, "PRIMARY KEY(`id`)") != null);
    try std.testing.expect(std.mem.indexOf(u8, stmt, "`id` BIGINT NOT NULL") != null);
    const ipos = std.mem.indexOf(u8, stmt, "`id`").?;
    const npos = std.mem.indexOf(u8, stmt, "`name`").?;
    try std.testing.expect(ipos < npos);
}

test "create table: Doris appends to a keyless duplicate table and upserts into a merge-on-write unique key" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const schema = types.Schema{ .fields = &.{
        .{ .name = "score", .ty = types.Type.init(.float) },
        .{ .name = "name", .ty = types.Type.init(.string) },
        .{ .name = "at", .ty = types.Type.init(.timestamp) },
    } };
    const app = try genCreateTable(a, .doris, "it", "t", schema, .append, 4, 1);
    try std.testing.expect(std.mem.indexOf(u8, app, "KEY(") == null);
    try std.testing.expect(std.mem.indexOf(u8, app, "DISTRIBUTED BY RANDOM BUCKETS 4") != null);
    try std.testing.expect(std.mem.indexOf(u8, app, "\"enable_duplicate_without_keys_by_default\"=\"true\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, app, "`name` STRING") != null);
    try std.testing.expect(std.mem.indexOf(u8, app, "`at` DATETIME(6)") != null);

    const mode = ast.WriteMode{ .upsert = .{ .keys = &.{"name"} } };
    const up = try genCreateTable(a, .doris, "it", "t", schema, mode, 4, 1);
    try std.testing.expect(std.mem.indexOf(u8, up, "UNIQUE KEY(`name`)") != null);
    try std.testing.expect(std.mem.indexOf(u8, up, "`name` VARCHAR(65533) NOT NULL") != null);
    try std.testing.expect(std.mem.indexOf(u8, up, "DISTRIBUTED BY HASH(`name`) BUCKETS 4") != null);
    try std.testing.expect(std.mem.indexOf(u8, up, "\"enable_unique_key_merge_on_write\"=\"true\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, up, "`name`").? < std.mem.indexOf(u8, up, "`score`").?);
}

test "label and column list" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings("basalt_orders_99_3", try genLabel(a, "basalt", "orders", 99, 3));
    try std.testing.expectEqualStrings("basalt_scratch_cvm_cad_fi_99_3", try genLabel(a, "basalt", "scratch.cvm_cad_fi", 99, 3));
    const long = try genLabel(a, "basalt", "x" ** 200, 99, 3);
    try std.testing.expectEqual(@as(usize, 128), long.len);
    try std.testing.expect(std.mem.endsWith(u8, long, "_99_3"));

    const l0 = try genLabel(a, "pipeline_l0", "orders", 99, 1);
    const l1 = try genLabel(a, "pipeline_l1", "orders", 99, 1);
    try std.testing.expect(!std.mem.eql(u8, l0, l1));
    try std.testing.expectEqualStrings("pipeline_l0_orders_99_1", l0);
    const schema = types.Schema{ .fields = &.{
        .{ .name = "id", .ty = types.Type.init(.int) },
        .{ .name = "amount", .ty = types.Type.init(.int) },
    } };
    try std.testing.expectEqualStrings("`id`,`amount`", try columnList(a, schema));
}

test "writeSanitized replaces separator bytes embedded in data" {
    var buf = std.array_list.Managed(u8).init(std.testing.allocator);
    defer buf.deinit();
    try writeSanitized(buf.writer(), "memo\x01with\x02stray bytes\x02");
    try std.testing.expectEqualStrings("memo with stray bytes ", buf.items);

    buf.clearRetainingCapacity();
    try writeSanitized(buf.writer(), "clean value");
    try std.testing.expectEqualStrings("clean value", buf.items);
}

test "stream-load TSV body: control-byte framing, nulls, sanitized values" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const columnmod = @import("../exec/column.zig");
    const int_ty = types.Type.init(.int).asNullable();
    const str_ty = types.Type.init(.string).asNullable();
    var b0 = columnmod.Builder.init(a, int_ty);
    try b0.append(.{ .int = 1 });
    try b0.append(.null);
    var b1 = columnmod.Builder.init(a, str_ty);
    try b1.append(.{ .string = "memo\x01with\x02bytes" });
    try b1.append(.{ .string = "line\nbreak" });
    const cols = try a.alloc(columnmod.Column, 2);
    cols[0] = try b0.finish();
    cols[1] = try b1.finish();
    var schema = types.Schema{ .fields = &.{
        .{ .name = "id", .ty = int_ty },
        .{ .name = "memo", .ty = str_ty },
    } };
    const batch = Batch{ .schema = &schema, .columns = cols, .len = 2 };

    var out = std.array_list.Managed(u8).init(a);
    try appendBatchTsv(out.writer(), a, batch);
    try std.testing.expectEqualStrings("1\x01memo with bytes\x02\\N\x01line\nbreak\x02", out.items);
}

test "a value that is literally the null marker is refused, not written as null" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const columnmod = @import("../exec/column.zig");
    const str_ty = types.Type.init(.string).asNullable();
    var b0 = columnmod.Builder.init(a, str_ty);
    try b0.append(.{ .string = "\\N" });
    const cols = try a.alloc(columnmod.Column, 1);
    cols[0] = try b0.finish();
    var schema = types.Schema{ .fields = &.{.{ .name = "s", .ty = str_ty }} };
    const batch = Batch{ .schema = &schema, .columns = cols, .len = 1 };

    var out = std.array_list.Managed(u8).init(a);
    try std.testing.expectError(error.StreamLoadNullMarkerInData, appendBatchTsv(out.writer(), a, batch));

    var b1 = columnmod.Builder.init(a, str_ty);
    try b1.append(.{ .string = "a\\Nb" });
    const cols2 = try a.alloc(columnmod.Column, 1);
    cols2[0] = try b1.finish();
    const batch2 = Batch{ .schema = &schema, .columns = cols2, .len = 1 };
    out.clearRetainingCapacity();
    try appendBatchTsv(out.writer(), a, batch2);
    try std.testing.expectEqualStrings("a\\Nb\x02", out.items);
}
