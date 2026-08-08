//! StarRocks sink. Two channels:
//!   - MySQL protocol -> FE (9030): DDL (CREATE TABLE IF NOT EXISTS, TRUNCATE).
//!   - HTTP Stream Load -> BE/FE: the actual data load.
//! The write mode selects the table model: append/overwrite -> Duplicate Key,
//! `upsert on k` -> Primary Key (keys reordered first + NOT NULL).
//!
//! This file's pure logic (type mapping, DDL generation, TSV body, labels, auth)
//! is unit-tested. The network paths require a live StarRocks to exercise.

const std = @import("std");
const types = @import("../lang/types.zig");
const ast = @import("../lang/ast.zig");
const batchmod = @import("../exec/batch.zig");
const eval = @import("../exec/eval.zig");
const driver = @import("driver.zig");
const mysql = @import("mysql.zig");
const sqlmod = @import("sql.zig");
const obs = @import("../runtime/obs.zig");

const Batch = batchmod.Batch;

const FLUSH_BYTES = 8 * 1024 * 1024;

pub const Config = struct {
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
};

pub fn srType(arena: std.mem.Allocator, t: types.Type) ![]const u8 {
    return switch (t.kind) {
        .bool => "BOOLEAN",
        .int => "BIGINT",
        .float => "DOUBLE",
        .decimal => try std.fmt.allocPrint(arena, "DECIMAL({d},{d})", .{ t.precision, t.scale }),
        .string => "VARCHAR(65533)",
        .bytes => "STRING",
        .date => "DATE",
        .time => "VARCHAR(32)",
        .timestamp => "DATETIME",
        .array => "STRING",
        .@"struct" => "JSON",
    };
}

pub fn genCreateTable(
    arena: std.mem.Allocator,
    db: []const u8,
    table: []const u8,
    schema: types.Schema,
    mode: ast.WriteMode,
    buckets: u32,
    replication_num: u32,
) ![]const u8 {
    const is_pk = (mode == .upsert);
    const keys: []const []const u8 = switch (mode) {
        .upsert => |u| u.keys,
        else => &.{schema.fields[0].name},
    };
    if (is_pk and keys.len == 0) return error.UpsertKeysUnresolved;

    var ordered = std.array_list.Managed(types.Schema.Field).init(arena);
    if (is_pk) {
        for (keys) |k| {
            const f = findField(schema, k) orelse return error.UnknownKeyColumn;
            try ordered.append(f);
        }
        for (schema.fields) |f| {
            if (!nameIn(keys, f.name)) try ordered.append(f);
        }
    } else {
        for (schema.fields) |f| try ordered.append(f);
    }

    var buf = std.array_list.Managed(u8).init(arena);
    const w = buf.writer();
    try w.print("CREATE TABLE IF NOT EXISTS `{s}`.`{s}` (\n", .{ db, table });
    for (ordered.items, 0..) |f, i| {
        const not_null = is_pk and nameIn(keys, f.name);
        try w.print("  `{s}` {s}{s}", .{ f.name, try srType(arena, f.ty), if (not_null) " NOT NULL" else "" });
        if (i + 1 < ordered.items.len) try w.writeByte(',');
        try w.writeByte('\n');
    }
    try w.writeAll(") ENGINE=OLAP\n");
    if (is_pk) {
        try w.writeAll("PRIMARY KEY(");
        for (keys, 0..) |k, i| {
            if (i > 0) try w.writeByte(',');
            try w.print("`{s}`", .{k});
        }
        try w.writeAll(")\n");
    } else {
        try w.print("DUPLICATE KEY(`{s}`)\n", .{keys[0]});
    }
    try w.writeAll("DISTRIBUTED BY HASH(");
    if (is_pk) {
        for (keys, 0..) |k, i| {
            if (i > 0) try w.writeByte(',');
            try w.print("`{s}`", .{k});
        }
    } else try w.print("`{s}`", .{keys[0]});
    try w.print(") BUCKETS {d}\n", .{buckets});
    try w.print("PROPERTIES(\"replication_num\"=\"{d}\");", .{replication_num});
    return buf.toOwnedSlice();
}

fn findField(schema: types.Schema, name: []const u8) ?types.Schema.Field {
    for (schema.fields) |f| {
        if (std.mem.eql(u8, f.name, name)) return f;
    }
    return null;
}

const nameIn = sqlmod.nameIn;

/// Stream Load label: `<prefix>_<table>_<run_id>_<seq>`. The label makes each flush
/// at-most-once within a run (StarRocks rejects a duplicate label). It does NOT give
/// cross-run idempotency: split key-ranges are re-probed each run and work-stealing
/// assigns them to lanes non-deterministically, so the same (prefix, run_id, seq) can
/// cover different rows on a re-run. Exactly-once across re-runs is owned by downstream
/// dedup (a StarRocks primary-key table), per the split.zig contract — not by run_id.
pub fn genLabel(arena: std.mem.Allocator, prefix: []const u8, table: []const u8, run_id: u64, seq: u64) ![]const u8 {
    return std.fmt.allocPrint(arena, "{s}_{s}_{d}_{d}", .{ prefix, table, run_id, seq });
}

/// The comma-joined column list for the Stream Load `columns` header. Names are
/// backtick-quoted: source field names can contain spaces or symbols (e.g.
/// payroll APIs emitting keys like "extra noturna 110"), which the header's
/// SQL-ish parser would otherwise reject.
pub fn columnList(arena: std.mem.Allocator, schema: types.Schema) ![]const u8 {
    var buf = std.array_list.Managed(u8).init(arena);
    for (schema.fields, 0..) |f, i| {
        if (i > 0) try buf.append(',');
        try buf.append('`');
        for (f.name) |c| {
            if (c != '`') try buf.append(c);
        }
        try buf.append('`');
    }
    return buf.toOwnedSlice();
}

/// Append a batch to the load buffer. Fields are separated by 0x01 (`\x01`, set
/// as the Stream Load `column_separator`) and rows by 0x02 (`\x02`, set as the
/// `row_delimiter`) — control bytes StarRocks CSV does no quoting for. A literal
/// `\n` is NOT used as the row delimiter because text columns routinely contain
/// embedded newlines, which would otherwise split one row into several and break
/// the column count. ERP text/memo columns do carry stray 0x01/0x02 bytes in
/// practice (e.g. Protheus memo fields), so values are sanitized: those two
/// bytes are replaced with a space. Nulls are `\N`, and a non-null value that
/// is itself `\N` is an error rather than a silent null.
/// What Stream Load reads as NULL. Unescapable — see `appendBatchTsv`.
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
                const s = try eval.valueToString(arena, v);
                // StarRocks CSV has no escape for the null marker: a field whose
                // bytes are exactly `\N` is NULL whatever we do — `enclose` does
                // not exempt it, and backslashes are never unescaped, so `\\N`
                // arrives as three characters. Writing the value through would
                // silently turn it into NULL, so refuse. The load already runs
                // at max_filter_ratio=0; this sink does not do partial data.
                if (std.mem.eql(u8, s, NULL_MARKER)) return error.StarRocksNullMarkerInData;
                try writeSanitized(w, s);
            }
        }
        try w.writeByte(0x02);
    }
}

/// Write `s` with any separator/delimiter bytes (0x01, 0x02) replaced by a space,
/// so data can never shift the Stream Load column or row framing.
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

/// mysql_native_password auth token:
///   SHA1(pw) XOR SHA1( salt ++ SHA1(SHA1(pw)) )
pub fn mysqlAuthToken(password: []const u8, salt: []const u8) [20]u8 {
    const Sha1 = std.crypto.hash.Sha1;
    var h1: [20]u8 = undefined;
    Sha1.hash(password, &h1, .{});
    var h2: [20]u8 = undefined;
    Sha1.hash(&h1, &h2, .{});

    var ctx = Sha1.init(.{});
    ctx.update(salt);
    ctx.update(&h2);
    var h3: [20]u8 = undefined;
    ctx.final(&h3);

    var out: [20]u8 = undefined;
    for (&out, 0..) |*b, i| b.* = h1[i] ^ h3[i];
    return out;
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
    /// Set by the runtime after open(): error diagnostics go through the
    /// structured logger; null (tests/embedded) falls back to raw stderr.
    logger: ?*obs.Logger = null,

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
            const cdb = try std.fmt.allocPrint(gpa, "CREATE DATABASE IF NOT EXISTS `{s}`", .{cfg.database});
            defer gpa.free(cdb);
            try self.runDDL(cdb);

            const ddl = try genCreateTable(gpa, cfg.database, table, schema, mode, cfg.buckets, cfg.replication_num);
            defer gpa.free(ddl);
            try self.runDDL(ddl);

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

    fn runDDL(self: *StreamLoadSink, sql: []const u8) !void {
        const conn = try mysql.Conn.connect(self.gpa, self.cfg.fe_host, self.cfg.fe_port, self.cfg.user, self.cfg.password, "", .off);
        defer conn.close();
        conn.exec(sql) catch |e| {
            obs.logOr(self.logger, .err, "starrocks DDL error: {s} (sql: {s})", .{ conn.last_error, sql });
            return e;
        };
    }

    fn writeBatch(self: *StreamLoadSink, arena: std.mem.Allocator, batch: Batch) !void {
        try appendBatchTsv(self.buffer.writer(), arena, batch);
        if (self.buffer.items.len >= FLUSH_BYTES) try self.flush();
    }

    fn closeImpl(self: *StreamLoadSink) !void {
        defer self.teardown();
        try self.flush();
    }

    /// Failure path: drop the buffered TSV without a final Stream Load. Loads
    /// that already went out are committed server-side and stay (downstream
    /// dedup owns exactly-once, per the label scheme above).
    fn abortImpl(self: *StreamLoadSink) void {
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
        try hdrs.append(.{ .name = "row_delimiter", .value = "\\x02" });
        try hdrs.append(.{ .name = "columns", .value = self.columns });
        try hdrs.append(.{ .name = "max_filter_ratio", .value = "0" });
        if (self.mode == .upsert and self.mode.upsert.partial != null) {
            try hdrs.append(.{ .name = "partial_update", .value = "true" });
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
            return error.StreamLoadFailed;
        }
    }
};

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

const sink_vtable = driver.Sink.VTable{ .writeBatch = slWrite, .close = slClose, .abort = slAbort };

fn slWrite(ptr: *anyopaque, arena: std.mem.Allocator, b: Batch) anyerror!void {
    const self: *StreamLoadSink = @ptrCast(@alignCast(ptr));
    return self.writeBatch(arena, b);
}
fn slClose(ptr: *anyopaque) anyerror!void {
    const self: *StreamLoadSink = @ptrCast(@alignCast(ptr));
    return self.closeImpl();
}
fn slAbort(ptr: *anyopaque) void {
    const self: *StreamLoadSink = @ptrCast(@alignCast(ptr));
    self.abortImpl();
}

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
    const sql = try genCreateTable(a, "warehouse", "orders", schema, .append, 4, 1);
    try std.testing.expect(std.mem.indexOf(u8, sql, "CREATE TABLE IF NOT EXISTS `warehouse`.`orders`") != null);
    try std.testing.expect(std.mem.indexOf(u8, sql, "`id` BIGINT") != null);
    try std.testing.expect(std.mem.indexOf(u8, sql, "`name` VARCHAR(65533)") != null);
    try std.testing.expect(std.mem.indexOf(u8, sql, "DUPLICATE KEY(`id`)") != null);
    try std.testing.expect(std.mem.indexOf(u8, sql, "BUCKETS 4") != null);
}

test "create table: inferred upsert with unresolved (empty) keys errors" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const schema = types.Schema{ .fields = &.{
        .{ .name = "id", .ty = types.Type.init(.int) },
    } };
    const mode = ast.WriteMode{ .upsert = .{ .keys = &.{}, .partial = null } };
    try std.testing.expectError(error.UpsertKeysUnresolved, genCreateTable(ar.allocator(), "db", "t", schema, mode, 4, 1));
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
    const sql = try genCreateTable(ar.allocator(), "bronze", "t", schema, mode, 4, 1);
    try std.testing.expect(std.mem.indexOf(u8, sql, "PRIMARY KEY(`emp`,`recno`)") != null);
    try std.testing.expect(std.mem.indexOf(u8, sql, "`emp` VARCHAR(65533) NOT NULL") != null);
    try std.testing.expect(std.mem.indexOf(u8, sql, "`recno` BIGINT NOT NULL") != null);
    const epos = std.mem.indexOf(u8, sql, "`emp`").?;
    const ppos = std.mem.indexOf(u8, sql, "`payload`").?;
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
    const sql = try genCreateTable(a, "warehouse", "orders", schema, mode, 4, 1);
    try std.testing.expect(std.mem.indexOf(u8, sql, "PRIMARY KEY(`id`)") != null);
    try std.testing.expect(std.mem.indexOf(u8, sql, "`id` BIGINT NOT NULL") != null);
    const ipos = std.mem.indexOf(u8, sql, "`id`").?;
    const npos = std.mem.indexOf(u8, sql, "`name`").?;
    try std.testing.expect(ipos < npos);
}

test "label and column list" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings("basalt_orders_99_3", try genLabel(a, "basalt", "orders", 99, 3));

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

test "mysql_native_password token matches a known vector" {
    var salt: [20]u8 = undefined;
    for (&salt, 0..) |*b, i| b.* = @intCast(i + 1);
    const tok = mysqlAuthToken("foobar", &salt);
    var expect: [20]u8 = undefined;
    _ = try std.fmt.hexToBytes(&expect, "e419caeec63ade5aeb8e0f8bbb2ac2d86b183350");
    try std.testing.expectEqualSlices(u8, &expect, &tok);
    var salt2 = salt;
    salt2[0] ^= 0xFF;
    try std.testing.expect(!std.mem.eql(u8, &tok, &mysqlAuthToken("foobar", &salt2)));
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
    try std.testing.expectError(error.StarRocksNullMarkerInData, appendBatchTsv(out.writer(), a, batch));

    // Only the whole field is ambiguous — `\N` inside a longer value is data,
    // and StarRocks reads it back verbatim.
    var b1 = columnmod.Builder.init(a, str_ty);
    try b1.append(.{ .string = "a\\Nb" });
    const cols2 = try a.alloc(columnmod.Column, 1);
    cols2[0] = try b1.finish();
    const batch2 = Batch{ .schema = &schema, .columns = cols2, .len = 1 };
    out.clearRetainingCapacity();
    try appendBatchTsv(out.writer(), a, batch2);
    try std.testing.expectEqualStrings("a\\Nb\x02", out.items);
}
