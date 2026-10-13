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
//! The body streams with chunked transfer encoding: each batch is one chunk of a
//! transaction that stays open until it reaches `load_size` (or half its timeout),
//! so the server ingests while the lane reads on and its fixed per-transaction
//! cost is paid rarely. Each transaction is teed to a spool file in the run's
//! scratch space and truncated after its commit; a transient failure resends the
//! spool under the same label, which keeps memory constant and the transaction
//! at-most-once: the server's answer for a reused label tells committed from
//! aborted. Labels do not give cross-run idempotency (split ranges are reassigned
//! to lanes non-deterministically); exactly-once across runs is owned by
//! downstream dedup on a primary-key table, per the split.zig contract.
//!
//! `logger` and `errctx` are set by the runtime after `open`, so a refused load or
//! DDL carries the server's own message (e.g. the missing privilege) into the
//! run's error instead of only the log. Auto-create creates only what is missing:
//! StarRocks checks the CREATE privilege before `IF NOT EXISTS` can no-op, so a
//! role allowed only to load would otherwise be refused.
//!
//! The pure logic (type mapping, DDL, body, labels, response outcomes) is
//! unit-tested; the network paths need a live server.

const std = @import("std");
const types = @import("../lang/types.zig");
const ast = @import("../lang/ast.zig");
const Batch = @import("../exec/batch.zig").Batch;
const driver = @import("../connect/driver.zig");
const mysql = @import("mysql.zig");
const sql = @import("sql.zig");
const obs = @import("../runtime/obs.zig");
const op = @import("../exec/op.zig");
const Space = @import("../exec/space.zig").Space;

const TIMEOUT_S = 600;
const MAX_AGE_NS = TIMEOUT_S / 2 * std.time.ns_per_s;
const RETRY_BUDGET_NS = (TIMEOUT_S + 120) * std.time.ns_per_s;
const REPLAY_CHUNK = 1 << 20;

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
    load_size: u64 = 512 << 20,
    space: ?Space = null,
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
    url: []const u8 = "",
    auth: []const u8 = "",
    label: []const u8 = "",
    headers: std.array_list.Managed(std.http.Header),
    txn: ?Txn = null,
    txn_bytes: u64 = 0,
    txn_started: std.time.Instant,
    broken: bool = false,
    spool: ?Space.File = null,
    opened: std.time.Instant,
    setup_ns: u64 = 0,
    encode_ns: u64 = 0,
    spool_ns: u64 = 0,
    send_ns: u64 = 0,
    wait_ns: u64 = 0,
    sent_bytes: u64 = 0,

    const Txn = struct {
        req: std.http.Client.Request,
        body: std.http.BodyWriter,
    };

    pub fn open(gpa: std.mem.Allocator, cfg: Config, table: []const u8, schema: types.Schema, mode: ast.WriteMode) !*StreamLoadSink {
        const self = try gpa.create(StreamLoadSink);
        errdefer gpa.destroy(self);
        const columns = try columnList(gpa, schema);
        errdefer gpa.free(columns);
        var cfg_owned = cfg;
        cfg_owned.label_prefix = try gpa.dupe(u8, cfg.label_prefix);
        errdefer gpa.free(cfg_owned.label_prefix);
        const now = try std.time.Instant.now();
        self.* = .{
            .gpa = gpa,
            .cfg = cfg_owned,
            .db = cfg.database,
            .table = table,
            .columns = columns,
            .mode = mode,
            .buffer = std.array_list.Managed(u8).init(gpa),
            .headers = std.array_list.Managed(std.http.Header).init(gpa),
            .run_id = if (cfg.run_id != 0) cfg.run_id else @intCast(std.time.milliTimestamp()),
            .client = std.http.Client{ .allocator = gpa },
            .opened = now,
            .txn_started = now,
        };
        errdefer self.buffer.deinit();
        errdefer self.client.deinit();
        errdefer self.freeRequestParts();
        try self.buildRequestParts();
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
        self.setup_ns = (try std.time.Instant.now()).since(self.opened);
        return self;
    }

    pub fn sink(self: *StreamLoadSink) driver.Sink {
        return .{ .ptr = self, .vtable = &sink_vtable };
    }

    /// The URL and headers every transaction sends; the label is filled per transaction.
    fn buildRequestParts(self: *StreamLoadSink) !void {
        self.url = try std.fmt.allocPrint(self.gpa, "{s}/api/{s}/{s}/_stream_load", .{ self.cfg.load_url, self.db, self.table });
        const cred = try std.fmt.allocPrint(self.gpa, "{s}:{s}", .{ self.cfg.user, self.cfg.password });
        defer self.gpa.free(cred);
        const b64 = try self.gpa.alloc(u8, std.base64.standard.Encoder.calcSize(cred.len));
        defer self.gpa.free(b64);
        self.auth = try std.fmt.allocPrint(self.gpa, "Basic {s}", .{std.base64.standard.Encoder.encode(b64, cred)});
        const h = &self.headers;
        try h.append(.{ .name = "Authorization", .value = self.auth });
        try h.append(.{ .name = "label", .value = "" });
        try h.append(.{ .name = "format", .value = "CSV" });
        try h.append(.{ .name = "column_separator", .value = "\\x01" });
        try h.append(.{ .name = self.cfg.flavor.rowDelimiterHeader(), .value = "\\x02" });
        try h.append(.{ .name = "columns", .value = self.columns });
        try h.append(.{ .name = "max_filter_ratio", .value = "0" });
        try h.append(.{ .name = "timeout", .value = std.fmt.comptimePrint("{d}", .{TIMEOUT_S}) });
        if (self.cfg.flavor.strict()) try h.append(.{ .name = "strict_mode", .value = "true" });
        if (self.mode == .upsert and self.mode.upsert.partial != null) {
            try h.append(.{ .name = self.cfg.flavor.partialHeader(), .value = "true" });
        }
    }

    fn freeRequestParts(self: *StreamLoadSink) void {
        self.headers.deinit();
        self.gpa.free(self.url);
        self.gpa.free(self.auth);
        self.gpa.free(self.label);
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

    /// Encodes the batch once, tees it to the spool and streams it into the open
    /// transaction as one chunk, then commits when the transaction reaches
    /// `load_size` or has been open half its timeout. A connection lost mid-stream
    /// only marks the transaction broken: later batches go to the spool alone and
    /// the commit replays it.
    pub fn writeBatch(self: *StreamLoadSink, arena: std.mem.Allocator, batch: Batch) !void {
        const t0 = try std.time.Instant.now();
        self.buffer.clearRetainingCapacity();
        try appendBatchTsv(self.buffer.writer(), arena, batch);
        self.encode_ns += (try std.time.Instant.now()).since(t0);
        const bytes = self.buffer.items;
        if (bytes.len == 0) return;
        if (self.txn_bytes == 0) {
            self.seq += 1;
            self.txn_started = try std.time.Instant.now();
            self.gpa.free(self.label);
            self.label = "";
            self.label = try genLabel(self.gpa, self.cfg.label_prefix, self.table, self.run_id, self.seq);
            self.headers.items[1].value = self.label;
        }
        try self.spoolWrite(bytes);
        self.txn_bytes += bytes.len;
        if (!self.broken) self.send(bytes) catch |e| try self.sendFailed(e);
        const age = (try std.time.Instant.now()).since(self.txn_started);
        if (self.txn_bytes >= self.cfg.load_size or age >= MAX_AGE_NS) try self.commit();
    }

    /// At debug level, a closing line splits the sink's lifetime into body
    /// encoding, spool writes, time blocked writing the body to the socket, time
    /// waiting for the commit's answer, and the rest (reading upstream).
    pub fn close(self: *StreamLoadSink) !void {
        defer self.teardown();
        try self.commit();
        if (self.logger) |lg| {
            const life = (std.time.Instant.now() catch self.opened).since(self.opened);
            const ms = std.time.ns_per_ms;
            lg.log(.debug, "stream load {s} closed: {d} loads, {d} bytes, life {d} ms, setup {d} ms, encode {d} ms, spool {d} ms, send {d} ms, wait {d} ms", .{
                self.table, self.seq, self.sent_bytes, life / ms, self.setup_ns / ms, self.encode_ns / ms, self.spool_ns / ms, self.send_ns / ms, self.wait_ns / ms,
            });
        }
    }

    /// Drops the open transaction without its terminating chunk, so the server
    /// aborts it; transactions already committed stay.
    pub fn abort(self: *StreamLoadSink) void {
        self.teardown();
    }

    fn teardown(self: *StreamLoadSink) void {
        self.dropTxn();
        self.client.deinit();
        if (self.spool) |sp| {
            sp.file.close();
            std.fs.cwd().deleteFile(sp.path) catch {};
            self.gpa.free(sp.path);
            if (self.cfg.space) |space| space.release(self.txn_bytes);
        }
        self.buffer.deinit();
        self.freeRequestParts();
        self.gpa.free(self.columns);
        self.gpa.free(self.cfg.label_prefix);
        self.gpa.destroy(self);
    }

    fn spoolWrite(self: *StreamLoadSink, bytes: []const u8) !void {
        const space = self.cfg.space orelse return;
        const t0 = try std.time.Instant.now();
        defer self.spool_ns += (std.time.Instant.now() catch t0).since(t0);
        if (self.spool == null) self.spool = try space.create(self.gpa, "streamload");
        try space.charge(bytes.len);
        try self.spool.?.file.writeAll(bytes);
    }

    fn spoolReset(self: *StreamLoadSink) !void {
        const sp = self.spool orelse return;
        try sp.file.setEndPos(0);
        try sp.file.seekTo(0);
        if (self.cfg.space) |space| space.release(self.txn_bytes);
    }

    /// Opens the transaction's request: chunked, one connection of its own, with
    /// send and receive timeouts so a server that stops reading cannot hang the lane.
    fn begin(self: *StreamLoadSink) !void {
        const uri = try std.Uri.parse(self.url);
        self.txn = .{ .req = try self.client.request(.PUT, uri, .{
            .redirect_behavior = .unhandled,
            .keep_alive = false,
            .headers = .{ .accept_encoding = .omit },
            .extra_headers = self.headers.items,
        }), .body = undefined };
        errdefer self.dropTxn();
        const t = &self.txn.?;
        const fd = t.req.connection.?.stream_writer.getStream().handle;
        driver.tuneSocket(fd);
        const tv = std.posix.timeval{ .sec = TIMEOUT_S, .usec = 0 };
        std.posix.setsockopt(fd, std.posix.SOL.SOCKET, std.posix.SO.SNDTIMEO, std.mem.asBytes(&tv)) catch {};
        std.posix.setsockopt(fd, std.posix.SOL.SOCKET, std.posix.SO.RCVTIMEO, std.mem.asBytes(&tv)) catch {};
        t.req.transfer_encoding = .chunked;
        t.body = t.req.sendBodyUnflushed(&.{}) catch return self.writeErr();
    }

    fn dropTxn(self: *StreamLoadSink) void {
        if (self.txn) |*t| {
            if (t.req.connection) |c| c.closing = true;
            t.req.deinit();
            self.txn = null;
        }
    }

    fn send(self: *StreamLoadSink, bytes: []const u8) !void {
        const t0 = try std.time.Instant.now();
        defer self.send_ns += (std.time.Instant.now() catch t0).since(t0);
        if (self.txn == null) try self.begin();
        const body = &self.txn.?.body;
        body.writer.writeAll(bytes) catch return self.writeErr();
        body.flush() catch return self.writeErr();
    }

    /// The socket error behind a failed body write.
    fn writeErr(self: *StreamLoadSink) anyerror {
        const t = self.txn orelse return error.WriteFailed;
        const c = t.req.connection orelse return error.WriteFailed;
        return c.stream_writer.err orelse error.WriteFailed;
    }

    /// A body write failed: when the server already answered (it closes the
    /// socket on a refusal it can make early, like a reused label), that answer
    /// decides; otherwise the transaction is replayed at commit.
    fn sendFailed(self: *StreamLoadSink, e: anyerror) !void {
        if (!transient(e)) {
            self.dropTxn();
            return e;
        }
        obs.logOr(self.logger, .warn, "stream load {s} #{d}: connection lost mid-body ({s}), replaying at commit", .{ self.table, self.seq, @errorName(e) });
        if (self.txn != null and e != error.WouldBlock) {
            const out = self.answer() catch Outcome.resend;
            if (out == .fail) {
                self.dropTxn();
                return error.StreamLoadFailed;
            }
        }
        self.dropTxn();
        self.broken = true;
        if (self.spool == null) return e;
    }

    /// Ends the open transaction and waits for its outcome. A transient failure
    /// replays the spool under the same label, with backoff, until the server
    /// gives an answer that settles it or the retry budget is spent.
    fn commit(self: *StreamLoadSink) !void {
        if (self.txn_bytes == 0) return;
        const t0 = try std.time.Instant.now();
        defer self.wait_ns += (std.time.Instant.now() catch t0).since(t0);
        var delay: u64 = std.time.ns_per_s;
        while (true) {
            const out = self.attempt() catch |e| blk: {
                if (!transient(e)) return e;
                obs.logOr(self.logger, .warn, "stream load {s} #{d}: {s}, retrying", .{ self.table, self.seq, @errorName(e) });
                break :blk Outcome.resend;
            };
            switch (out) {
                .ok => break,
                .fail => return error.StreamLoadFailed,
                .resend, .wait => {
                    self.broken = true;
                    if (self.spool == null) return error.StreamLoadUnreplayable;
                    if (driver.aborting()) return error.Aborted;
                    if ((try std.time.Instant.now()).since(t0) + delay > RETRY_BUDGET_NS) {
                        if (self.errctx) |ec| ec.set("stream load {s}: label {s} still unsettled after {d} s of retries", .{ self.table, self.label, RETRY_BUDGET_NS / std.time.ns_per_s });
                        return error.StreamLoadRetriesExhausted;
                    }
                    std.Thread.sleep(delay);
                    delay = @min(delay * 2, 10 * std.time.ns_per_s);
                },
            }
        }
        self.sent_bytes += self.txn_bytes;
        try self.spoolReset();
        self.txn_bytes = 0;
        self.broken = false;
    }

    fn attempt(self: *StreamLoadSink) !Outcome {
        defer self.dropTxn();
        if (self.broken or self.txn == null) {
            self.dropTxn();
            try self.begin();
            self.broken = false;
            self.replay() catch |e| return self.answerAfter(e);
        }
        self.txn.?.body.end() catch return self.answerAfter(self.writeErr());
        return self.answer();
    }

    /// The server's answer when a write failed; the write's error when there is none.
    fn answerAfter(self: *StreamLoadSink, e: anyerror) !Outcome {
        if (!transient(e) or e == error.WouldBlock) return e;
        return self.answer() catch e;
    }

    fn replay(self: *StreamLoadSink) !void {
        const sp = self.spool orelse return error.StreamLoadUnreplayable;
        try sp.file.seekTo(0);
        try self.buffer.resize(REPLAY_CHUNK);
        defer self.buffer.clearRetainingCapacity();
        const body = &self.txn.?.body;
        var left = self.txn_bytes;
        while (left > 0) {
            const n = try sp.file.read(self.buffer.items[0..@min(left, REPLAY_CHUNK)]);
            if (n == 0) return error.StreamLoadSpoolShort;
            body.writer.writeAll(self.buffer.items[0..n]) catch return self.writeErr();
            left -= n;
        }
    }

    /// Reads the response to the open request and classifies it, logging the
    /// server's timings on success and its reason on failure.
    fn answer(self: *StreamLoadSink) !Outcome {
        const req = &self.txn.?.req;
        var res = req.receiveHead(&.{}) catch |e| return if (e == error.ReadFailed) (req.connection.?.getReadError() orelse e) else e;
        var body_aw = std.Io.Writer.Allocating.init(self.gpa);
        defer body_aw.deinit();
        var transfer: [64]u8 = undefined;
        var decompress: std.http.Decompress = undefined;
        _ = res.readerDecompressing(&transfer, &decompress, &.{}).streamRemaining(&body_aw.writer) catch |e| switch (e) {
            error.ReadFailed => return res.bodyErr() orelse e,
            else => return e,
        };
        const body = body_aw.writer.buffered();
        const out = classify(body);
        switch (out) {
            .ok => if (self.logger) |lg| lg.log(.debug, "stream load {s} #{d}: {d} rows, {d} bytes, total {d} ms, begin {d}, plan {d}, read {d}, write {d}, commit+publish {d} ms", .{
                self.table,                            self.seq,
                jsonInt(body, "NumberLoadedRows"),     jsonInt(body, "LoadBytes"),
                jsonInt(body, "LoadTimeMs"),           jsonInt(body, "BeginTxnTimeMs"),
                jsonInt(body, "StreamLoadPlanTimeMs"), jsonInt(body, "ReadDataTimeMs"),
                jsonInt(body, "WriteDataTimeMs"),      jsonInt(body, "CommitAndPublishTimeMs"),
            }),
            .fail => {
                obs.logOr(self.logger, .err, "stream load failed (http {d}): {s}", .{ @intFromEnum(res.head.status), body });
                if (self.errctx) |ec| {
                    const why = jsonField(body, "Message") orelse jsonField(body, "Status") orelse "no reason given";
                    if (jsonField(body, "ErrorURL")) |u|
                        ec.set("stream load failed: {s} (rejected rows: {s})", .{ why, u })
                    else
                        ec.set("stream load failed: {s}", .{why});
                }
            },
            .resend, .wait => obs.logOr(self.logger, .warn, "stream load {s} #{d} not settled (http {d}): {s}", .{ self.table, self.seq, @intFromEnum(res.head.status), body }),
        }
        return out;
    }
};

fn transient(e: anyerror) bool {
    return driver.transientNet(e) or e == error.WouldBlock or e == error.HttpConnectionClosing;
}

/// Where the value of key `name` starts in a flat JSON object.
fn jsonValueAt(body: []const u8, name: []const u8) ?usize {
    var needle_buf: [64]u8 = undefined;
    const needle = std.fmt.bufPrint(&needle_buf, "\"{s}\"", .{name}) catch return null;
    const at = std.mem.indexOf(u8, body, needle) orelse return null;
    var i = at + needle.len;
    while (i < body.len and (body[i] == ' ' or body[i] == ':')) i += 1;
    return i;
}

/// One integer field of a flat JSON object, -1 when absent or not a number.
fn jsonInt(body: []const u8, name: []const u8) i64 {
    var i = jsonValueAt(body, name) orelse return -1;
    const start = i;
    while (i < body.len and (std.ascii.isDigit(body[i]) or (i == start and body[i] == '-'))) i += 1;
    return std.fmt.parseInt(i64, body[start..i], 10) catch -1;
}

test "jsonInt reads the timing fields of a stream load response" {
    const body =
        \\{"NumberLoadedRows": 1200, "LoadBytes": 8388700, "CommitAndPublishTimeMs": 41, "Status": "Success"}
    ;
    try std.testing.expectEqual(@as(i64, 1200), jsonInt(body, "NumberLoadedRows"));
    try std.testing.expectEqual(@as(i64, 8388700), jsonInt(body, "LoadBytes"));
    try std.testing.expectEqual(@as(i64, 41), jsonInt(body, "CommitAndPublishTimeMs"));
    try std.testing.expectEqual(@as(i64, -1), jsonInt(body, "Status"));
    try std.testing.expectEqual(@as(i64, -1), jsonInt(body, "Absent"));
}

/// One string field of a flat JSON object, unescaped only as far as the server's
/// own responses need; not a general JSON parser.
fn jsonField(body: []const u8, name: []const u8) ?[]const u8 {
    var i = jsonValueAt(body, name) orelse return null;
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

const Outcome = enum { ok, fail, resend, wait };

/// What a stream load response settles, from its `Status` (and, for a reused
/// label, `ExistingJobStatus`): `ok` once the data is committed — a publish
/// timeout included, which must never be resent — `fail` for a definite
/// refusal, `wait` while the label's earlier transaction is still open,
/// `resend` when it was aborted (a failed label may be reused) or the answer is
/// not a stream load response at all, as from a server still starting.
fn classify(body: []const u8) Outcome {
    const status = jsonField(body, "Status") orelse return .resend;
    if (eqlAnyIgnoreCase(status, &.{ "Success", "Publish Timeout" })) return .ok;
    if (!std.ascii.eqlIgnoreCase(status, "Label Already Exists")) return .fail;
    const existing = jsonField(body, "ExistingJobStatus") orelse return .wait;
    if (eqlAnyIgnoreCase(existing, &.{ "FINISHED", "VISIBLE", "COMMITTED" })) return .ok;
    if (eqlAnyIgnoreCase(existing, &.{ "CANCELLED", "ABORTED" })) return .resend;
    return .wait;
}

fn eqlAnyIgnoreCase(s: []const u8, options: []const []const u8) bool {
    for (options) |o| if (std.ascii.eqlIgnoreCase(s, o)) return true;
    return false;
}

test "classify settles on the parsed Status, not on words anywhere in the body" {
    try std.testing.expectEqual(Outcome.ok, classify("{\"Status\": \"Success\", \"Message\": \"OK\"}"));
    try std.testing.expectEqual(Outcome.ok, classify("{\"Status\": \"Publish Timeout\"}"));
    try std.testing.expectEqual(Outcome.fail, classify("{\"Status\": \"Fail\", \"Message\": \"too many filtered rows\", \"ErrorURL\": \"http://x/Success\"}"));
    try std.testing.expectEqual(Outcome.fail, classify("{\"Status\": \"Fail\", \"Message\": \"Label Already Exists elsewhere: Success\"}"));
    try std.testing.expectEqual(Outcome.resend, classify(""));
    try std.testing.expectEqual(Outcome.resend, classify("<html>503 Service Unavailable</html>"));
}

test "classify: a reused label is ok only once its earlier load committed" {
    const cases = [_]struct { []const u8, Outcome }{
        .{ "{\"Status\": \"Label Already Exists\", \"ExistingJobStatus\": \"FINISHED\"}", .ok },
        .{ "{\"Status\": \"Label Already Exists\", \"ExistingJobStatus\": \"VISIBLE\"}", .ok },
        .{ "{\"Status\": \"Label Already Exists\", \"ExistingJobStatus\": \"COMMITTED\"}", .ok },
        .{ "{\"Status\": \"Label Already Exists\", \"ExistingJobStatus\": \"RUNNING\"}", .wait },
        .{ "{\"Status\": \"Label Already Exists\", \"ExistingJobStatus\": \"PREPARE\"}", .wait },
        .{ "{\"Status\": \"Label Already Exists\", \"ExistingJobStatus\": \"PREPARED\"}", .wait },
        .{ "{\"Status\": \"Label Already Exists\", \"ExistingJobStatus\": \"CANCELLED\"}", .resend },
        .{ "{\"Status\": \"Label Already Exists\", \"ExistingJobStatus\": \"ABORTED\"}", .resend },
        .{ "{\"Status\": \"Label Already Exists\"}", .wait },
        .{ "{\"Status\": \"Label Already Exists\", \"ExistingJobStatus\": \"SOMETHING_NEW\"}", .wait },
    };
    for (cases) |c| try std.testing.expectEqual(c[1], classify(c[0]));
}

test "classify reads the pretty-printed StarRocks response" {
    const body =
        \\{
        \\    "TxnId": -1,
        \\    "Label": "probe_a1",
        \\    "Status": "Label Already Exists",
        \\    "ExistingJobStatus": "FINISHED",
        \\    "Message": "Label [probe_a1] has already been used."
        \\}
    ;
    try std.testing.expectEqual(Outcome.ok, classify(body));
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
    try b0.append(.{ .int = 2 });
    var b1 = columnmod.Builder.init(a, str_ty);
    try b1.append(.{ .string = "memo\x01with\x02bytes" });
    try b1.append(.{ .string = "line\nbreak" });
    try b1.append(.{ .string = "clean value" });
    const cols = try a.alloc(columnmod.Column, 2);
    cols[0] = try b0.finish();
    cols[1] = try b1.finish();
    var schema = types.Schema{ .fields = &.{
        .{ .name = "id", .ty = int_ty },
        .{ .name = "memo", .ty = str_ty },
    } };
    const batch = Batch{ .schema = &schema, .columns = cols, .len = 3 };

    var out = std.array_list.Managed(u8).init(a);
    try appendBatchTsv(out.writer(), a, batch);
    try std.testing.expectEqualStrings("1\x01memo with bytes\x02\\N\x01line\nbreak\x022\x01clean value\x02", out.items);
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
