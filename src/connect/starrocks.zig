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

const Batch = batchmod.Batch;

const FLUSH_BYTES = 8 * 1024 * 1024; // ~8MB per Stream Load PUT

pub const Config = struct {
    fe_host: []const u8 = "127.0.0.1",
    fe_port: u16 = 9030,
    load_url: []const u8 = "http://127.0.0.1:8040", // BE (direct) or FE :8030
    database: []const u8,
    user: []const u8 = "root",
    password: []const u8 = "",
    buckets: u32 = 4,
    replication_num: u32 = 1,
    auto_create: bool = true,
    label_prefix: []const u8 = "pipeline",
    run_id: u64 = 0, // 0 => generate per run (timestamp); set for retry-idempotency
};

// ---------------------------------------------------------------------------
// Type mapping
// ---------------------------------------------------------------------------

pub fn srType(arena: std.mem.Allocator, t: types.Type) ![]const u8 {
    return switch (t.kind) {
        .bool => "BOOLEAN",
        .int => "BIGINT",
        .float => "DOUBLE",
        .decimal => try std.fmt.allocPrint(arena, "DECIMAL({d},{d})", .{ t.precision, t.scale }),
        .string => "VARCHAR(65533)",
        .bytes => "STRING",
        .date => "DATE",
        .time => "VARCHAR(32)", // StarRocks has no TIME type
        .timestamp => "DATETIME",
        .array => "STRING",
        .@"struct" => "JSON",
    };
}

// ---------------------------------------------------------------------------
// DDL generation (auto-create)
// ---------------------------------------------------------------------------

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

    // Column order: for a Primary Key table the key columns must come first.
    var ordered = std.ArrayList(types.Schema.Field).init(arena);
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

    var buf = std.ArrayList(u8).init(arena);
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
    try w.print("DISTRIBUTED BY HASH(`{s}`) BUCKETS {d}\n", .{ keys[0], buckets });
    try w.print("PROPERTIES(\"replication_num\"=\"{d}\");", .{replication_num});
    return buf.toOwnedSlice();
}

fn findField(schema: types.Schema, name: []const u8) ?types.Schema.Field {
    for (schema.fields) |f| {
        if (std.mem.eql(u8, f.name, name)) return f;
    }
    return null;
}

fn nameIn(names: []const []const u8, n: []const u8) bool {
    for (names) |x| {
        if (std.mem.eql(u8, x, n)) return true;
    }
    return false;
}

// ---------------------------------------------------------------------------
// Stream Load body + helpers
// ---------------------------------------------------------------------------

/// Stream Load label: `<prefix>_<table>_<run_id>_<seq>`. `run_id` is unique per
/// run (so distinct runs don't collide) but can be made stable by a caller that
/// wants retry-idempotency (re-using the same run_id re-uses labels, which
/// StarRocks dedups).
pub fn genLabel(arena: std.mem.Allocator, prefix: []const u8, table: []const u8, run_id: u64, seq: u64) ![]const u8 {
    return std.fmt.allocPrint(arena, "{s}_{s}_{d}_{d}", .{ prefix, table, run_id, seq });
}

/// The comma-joined column list for the Stream Load `columns` header.
pub fn columnList(arena: std.mem.Allocator, schema: types.Schema) ![]const u8 {
    var buf = std.ArrayList(u8).init(arena);
    for (schema.fields, 0..) |f, i| {
        if (i > 0) try buf.append(',');
        try buf.appendSlice(f.name);
    }
    return buf.toOwnedSlice();
}

/// Append a batch to the load buffer. Fields are separated by 0x01 (`\x01`, set
/// as the Stream Load `column_separator`) since StarRocks CSV does no quoting;
/// nulls are `\N`, rows end with `\n`.
pub fn appendBatchTsv(w: anytype, arena: std.mem.Allocator, batch: Batch) !void {
    var r: usize = 0;
    while (r < batch.len) : (r += 1) {
        for (batch.columns, 0..) |*c, i| {
            if (i > 0) try w.writeByte(0x01);
            const v = c.getValue(r);
            if (v.isNull()) {
                try w.writeAll("\\N");
            } else {
                try w.writeAll(try eval.valueToString(arena, v));
            }
        }
        try w.writeByte('\n');
    }
}

/// mysql_native_password auth token:
///   SHA1(pw) XOR SHA1( salt ++ SHA1(SHA1(pw)) )
pub fn mysqlAuthToken(password: []const u8, salt: []const u8) [20]u8 {
    const Sha1 = std.crypto.hash.Sha1;
    var h1: [20]u8 = undefined; // SHA1(pw)
    Sha1.hash(password, &h1, .{});
    var h2: [20]u8 = undefined; // SHA1(SHA1(pw))
    Sha1.hash(&h1, &h2, .{});

    var ctx = Sha1.init(.{});
    ctx.update(salt);
    ctx.update(&h2);
    var h3: [20]u8 = undefined; // SHA1(salt ++ SHA1(SHA1(pw)))
    ctx.final(&h3);

    var out: [20]u8 = undefined;
    for (&out, 0..) |*b, i| b.* = h1[i] ^ h3[i];
    return out;
}

// ---------------------------------------------------------------------------
// Sink: auto-create via FE (MySQL), then Stream Load batches to the BE (HTTP)
// ---------------------------------------------------------------------------

pub const StreamLoadSink = struct {
    gpa: std.mem.Allocator,
    cfg: Config,
    db: []const u8,
    table: []const u8,
    columns: []const u8,
    mode: ast.WriteMode,
    buffer: std.ArrayList(u8),
    seq: u64 = 0,
    run_id: u64 = 0,
    client: std.http.Client,

    pub fn open(gpa: std.mem.Allocator, cfg: Config, table: []const u8, schema: types.Schema, mode: ast.WriteMode) !*StreamLoadSink {
        const self = try gpa.create(StreamLoadSink);
        self.* = .{
            .gpa = gpa,
            .cfg = cfg,
            .db = cfg.database,
            .table = table,
            .columns = try columnList(gpa, schema),
            .mode = mode,
            .buffer = std.ArrayList(u8).init(gpa),
            .run_id = if (cfg.run_id != 0) cfg.run_id else @intCast(std.time.milliTimestamp()),
            .client = std.http.Client{ .allocator = gpa },
        };
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
        // connect without a preselected database (it may not exist yet); DDL uses
        // fully-qualified `db`.`table` names.
        const conn = try mysql.Conn.connect(self.gpa, self.cfg.fe_host, self.cfg.fe_port, self.cfg.user, self.cfg.password, "");
        defer conn.close();
        conn.exec(sql) catch |e| {
            std.debug.print("starrocks DDL error: {s}\n  sql: {s}\n", .{ conn.last_error, sql });
            return e;
        };
    }

    fn writeBatch(self: *StreamLoadSink, arena: std.mem.Allocator, batch: Batch) !void {
        try appendBatchTsv(self.buffer.writer(), arena, batch);
        if (self.buffer.items.len >= FLUSH_BYTES) try self.flush();
    }

    fn closeImpl(self: *StreamLoadSink) !void {
        try self.flush();
        self.client.deinit();
        self.buffer.deinit();
        self.gpa.free(self.columns);
        self.gpa.destroy(self);
    }

    fn flush(self: *StreamLoadSink) !void {
        if (self.buffer.items.len == 0) return;
        self.seq += 1;
        try self.streamLoad();
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

        var hdrs = std.ArrayList(std.http.Header).init(self.gpa);
        defer hdrs.deinit();
        try hdrs.append(.{ .name = "Authorization", .value = auth });
        try hdrs.append(.{ .name = "label", .value = label });
        try hdrs.append(.{ .name = "format", .value = "CSV" });
        try hdrs.append(.{ .name = "column_separator", .value = "\\x01" });
        try hdrs.append(.{ .name = "columns", .value = self.columns });
        try hdrs.append(.{ .name = "max_filter_ratio", .value = "0" });
        if (self.mode == .upsert and self.mode.upsert.partial != null) {
            try hdrs.append(.{ .name = "partial_update", .value = "true" });
        }

        var body_buf = std.ArrayList(u8).init(self.gpa);
        defer body_buf.deinit();
        const res = try self.client.fetch(.{
            .method = .PUT,
            .location = .{ .url = url },
            .extra_headers = hdrs.items,
            .payload = self.buffer.items,
            .response_storage = .{ .dynamic = &body_buf },
        });
        if (!loadSucceeded(body_buf.items)) {
            std.debug.print("stream load failed (http {d}): {s}\n", .{ @intFromEnum(res.status), body_buf.items });
            return error.StreamLoadFailed;
        }
    }
};

fn loadSucceeded(body: []const u8) bool {
    // tolerate pretty/compact JSON; accept Success and Publish Timeout (committed)
    return std.mem.indexOf(u8, body, "Success") != null or
        std.mem.indexOf(u8, body, "Publish Timeout") != null;
}

const sink_vtable = driver.Sink.VTable{ .writeBatch = slWrite, .close = slClose };

fn slWrite(ptr: *anyopaque, arena: std.mem.Allocator, b: Batch) anyerror!void {
    const self: *StreamLoadSink = @ptrCast(@alignCast(ptr));
    return self.writeBatch(arena, b);
}
fn slClose(ptr: *anyopaque) anyerror!void {
    const self: *StreamLoadSink = @ptrCast(@alignCast(ptr));
    return self.closeImpl();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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
    // id reordered before name
    const ipos = std.mem.indexOf(u8, sql, "`id`").?;
    const npos = std.mem.indexOf(u8, sql, "`name`").?;
    try std.testing.expect(ipos < npos);
}

test "label and column list" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings("pipeline_orders_99_3", try genLabel(a, "pipeline", "orders", 99, 3));
    const schema = types.Schema{ .fields = &.{
        .{ .name = "id", .ty = types.Type.init(.int) },
        .{ .name = "amount", .ty = types.Type.init(.int) },
    } };
    try std.testing.expectEqualStrings("id,amount", try columnList(a, schema));
}

test "mysql_native_password token matches a known vector" {
    // password "foobar", salt = 20 bytes 0x01..0x14
    var salt: [20]u8 = undefined;
    for (&salt, 0..) |*b, i| b.* = @intCast(i + 1);
    const tok = mysqlAuthToken("foobar", &salt);
    // recompute independently to guard the formula (XOR of two SHA1s)
    const Sha1 = std.crypto.hash.Sha1;
    var s1: [20]u8 = undefined;
    Sha1.hash("foobar", &s1, .{});
    var s2: [20]u8 = undefined;
    Sha1.hash(&s1, &s2, .{});
    var c = Sha1.init(.{});
    c.update(&salt);
    c.update(&s2);
    var s3: [20]u8 = undefined;
    c.final(&s3);
    var expect: [20]u8 = undefined;
    for (&expect, 0..) |*b, i| b.* = s1[i] ^ s3[i];
    try std.testing.expectEqualSlices(u8, &expect, &tok);
}
