//! A PostgreSQL server for tests, each connection on a thread of its own: a trust
//! login (AuthenticationOk, ReadyForQuery; an SSLRequest is refused with 'N'), and
//! simple Query messages over tables held in memory, answered with RowDescription,
//! DataRows in text format, CommandComplete and ReadyForQuery. Every query's text,
//! and every StartupMessage's parameters, are recorded, so a test can assert what
//! the engine sent.
//!
//! It reads only the queries basalt itself writes for a table: `SELECT * | "a", "b"
//! FROM <table> [WHERE …]`. A WHERE holding `1 = 0` returns no rows, and each
//! `"col" IN (…)` in it keeps the rows whose text is listed; anything else in a
//! WHERE is ignored, so it may return more rows than match, never fewer. A column
//! or table it does not hold is an ErrorResponse, as is any query holding `fail_on`,
//! which then carries `fail_msg`; a query holding `drop_on` has its connection
//! closed unanswered, as a server that went away mid-query; one holding `delay_on`
//! is answered `delay_ms` late, as a slow query; one holding `retype_on` describes
//! every column as `retype_oid`, as a server typing one query otherwise.
//!
//! For a key-range split it also reads a read wrapped as `(<read>) _alias` (the
//! outer WHERE ignored but for `1 = 0`), an `ORDER BY …`/`LIMIT n` tail (ignored),
//! `SELECT MIN("k") AS lo, MAX("k") AS hi` over an integer column, and the
//! `pg_index` primary-key question for a table given a `key` and `est_rows`.

const std = @import("std");

pub const Col = struct { name: []const u8, oid: i32 = 25 };

pub const Table = struct {
    name: []const u8,
    cols: []const Col,
    rows: []const []const ?[]const u8,
    key: ?[]const u8 = null,
    est_rows: i64 = 0,
};

pub const FakePg = struct {
    gpa: std.mem.Allocator,
    listener: std.net.Server,
    tables: []const Table,
    fail_on: ?[]const u8 = null,
    fail_msg: []const u8 = "",
    drop_on: ?[]const u8 = null,
    delay_on: ?[]const u8 = null,
    delay_ms: u64 = 0,
    retype_on: ?[]const u8 = null,
    retype_oid: i32 = 25,
    stop: std.atomic.Value(bool) = .init(false),
    mutex: std.Thread.Mutex = .{},
    queries: std.array_list.Managed([]u8),
    startups: std.array_list.Managed([]u8),
    sessions: std.array_list.Managed(std.Thread),
    acceptor: ?std.Thread = null,

    pub fn start(gpa: std.mem.Allocator, tables: []const Table) !*FakePg {
        const self = try gpa.create(FakePg);
        errdefer gpa.destroy(self);
        const addr = try std.net.Address.parseIp("127.0.0.1", 0);
        self.* = .{
            .gpa = gpa,
            .listener = try addr.listen(.{ .reuse_address = true }),
            .tables = tables,
            .queries = std.array_list.Managed([]u8).init(gpa),
            .startups = std.array_list.Managed([]u8).init(gpa),
            .sessions = std.array_list.Managed(std.Thread).init(gpa),
        };
        self.acceptor = try std.Thread.spawn(.{}, acceptLoop, .{self});
        return self;
    }

    pub fn port(self: *const FakePg) u16 {
        return self.listener.listen_address.getPort();
    }

    /// Stops accepting, waits for every session to end, and frees the server.
    pub fn finish(self: *FakePg) void {
        self.stop.store(true, .seq_cst);
        if (std.net.tcpConnectToAddress(self.listener.listen_address)) |c| c.close() else |_| {}
        if (self.acceptor) |t| t.join();
        for (self.sessions.items) |t| t.join();
        self.sessions.deinit();
        for (self.queries.items) |q| self.gpa.free(q);
        self.queries.deinit();
        for (self.startups.items) |q| self.gpa.free(q);
        self.startups.deinit();
        self.listener.deinit();
        self.gpa.destroy(self);
    }

    /// The queries received so far, in order; the caller frees the list, not the texts.
    pub fn received(self: *FakePg, gpa: std.mem.Allocator) ![]const []const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();
        const out = try gpa.alloc([]const u8, self.queries.items.len);
        for (self.queries.items, out) |q, *o| o.* = q;
        return out;
    }

    fn acceptLoop(self: *FakePg) void {
        while (!self.stop.load(.seq_cst)) {
            const conn = self.listener.accept() catch return;
            if (self.stop.load(.seq_cst)) {
                conn.stream.close();
                return;
            }
            self.mutex.lock();
            defer self.mutex.unlock();
            const t = std.Thread.spawn(.{}, session, .{ self, conn.stream }) catch {
                conn.stream.close();
                continue;
            };
            self.sessions.append(t) catch {
                t.detach();
            };
        }
    }

    fn session(self: *FakePg, s: std.net.Stream) void {
        defer s.close();
        self.serve(s) catch {};
    }

    fn serve(self: *FakePg, s: std.net.Stream) !void {
        var rb: [4096]u8 = undefined;
        var wb: [4096]u8 = undefined;
        var sr = std.net.Stream.Reader.init(s, &rb);
        var sw = std.net.Stream.Writer.init(s, &wb);
        const r = sr.interface();
        const w = &sw.interface;
        var arena = std.heap.ArenaAllocator.init(self.gpa);
        defer arena.deinit();
        const a = arena.allocator();

        while (true) {
            const len = try r.takeInt(u32, .big);
            if (len < 8) return error.BadStartup;
            const body = try a.alloc(u8, len - 4);
            try r.readSliceAll(body);
            if (std.mem.readInt(u32, body[0..4], .big) == 80877103) {
                try w.writeAll("N");
                try w.flush();
                continue;
            }
            self.mutex.lock();
            defer self.mutex.unlock();
            try self.startups.append(try self.gpa.dupe(u8, body[4..]));
            break;
        }
        try message(w, 'R', &.{ 0, 0, 0, 0 });
        try message(w, 'Z', "I");
        try w.flush();

        while (true) {
            const t = r.takeByte() catch return;
            const len = try r.takeInt(u32, .big);
            if (len < 4) return error.BadMessage;
            const body = try a.alloc(u8, len - 4);
            try r.readSliceAll(body);
            switch (t) {
                'X' => return,
                'Q' => {
                    const text = std.mem.sliceTo(body, 0);
                    {
                        self.mutex.lock();
                        defer self.mutex.unlock();
                        try self.queries.append(try self.gpa.dupe(u8, text));
                    }
                    if (self.drop_on) |d| if (std.mem.indexOf(u8, text, d) != null) return;
                    if (self.delay_on) |d| if (std.mem.indexOf(u8, text, d) != null) std.Thread.sleep(self.delay_ms * std.time.ns_per_ms);
                    try self.answer(a, w, text);
                    try message(w, 'Z', "I");
                    try w.flush();
                },
                else => {},
            }
        }
    }

    const Src = struct { tbl: ?Table, where: []const u8, name: []const u8, list: []const u8 = "*" };

    /// The table, WHERE and columns a query's text past ` FROM ` reads: a table, or
    /// a read wrapped as `(<read>) _alias`, whose own WHERE and columns apply.
    fn source(self: *FakePg, a: std.mem.Allocator, rest_in: []const u8) !Src {
        var rest = rest_in;
        if (rest.len > 0 and rest[0] == '(') {
            var depth: usize = 0;
            const close = for (rest, 0..) |c, i| {
                if (c == '(') depth += 1;
                if (c == ')') {
                    depth -= 1;
                    if (depth == 0) break i;
                }
            } else rest.len;
            const inner = rest[1..@min(close, rest.len)];
            const from = std.mem.indexOf(u8, inner, " FROM ") orelse return .{ .tbl = null, .where = "", .name = inner };
            var in_src = try self.source(a, inner[from + " FROM ".len ..]);
            if (std.mem.startsWith(u8, inner, "SELECT ")) in_src.list = inner["SELECT ".len..from];
            const outer = if (close < rest.len) rest[close + 1 ..] else "";
            const ow = std.mem.indexOf(u8, outer, " WHERE ") orelse return in_src;
            if (std.mem.indexOf(u8, outer[ow..], "1 = 0") == null) return in_src;
            in_src.where = try std.fmt.allocPrint(a, "{s} AND 1 = 0", .{in_src.where});
            return in_src;
        }
        for ([_][]const u8{ " ORDER BY ", " LIMIT " }) |tail| {
            if (std.mem.indexOf(u8, rest, tail)) |i| rest = rest[0..i];
        }
        const where_at = std.mem.indexOf(u8, rest, " WHERE ");
        const tname = unquote(lastPart(rest[0 .. where_at orelse rest.len]));
        const where = if (where_at) |i| rest[i + " WHERE ".len ..] else "";
        for (self.tables) |tb| {
            if (std.mem.eql(u8, tb.name, tname)) return .{ .tbl = tb, .where = where, .name = tname };
        }
        return .{ .tbl = null, .where = where, .name = tname };
    }

    /// The primary-key question a split asks: the key's name and type and the
    /// table's estimated rows, for the first keyed table the query names.
    fn answerKey(self: *FakePg, a: std.mem.Allocator, w: *std.Io.Writer, text: []const u8) !void {
        const cols = [_]Col{ .{ .name = "attname" }, .{ .name = "typname" }, .{ .name = "reltuples", .oid = 20 } };
        try describe(a, w, &cols);
        var sent: usize = 0;
        for (self.tables) |tb| {
            const k = tb.key orelse continue;
            if (std.mem.indexOf(u8, text, try std.fmt.allocPrint(a, "\"{s}\"", .{tb.name})) == null) continue;
            try dataRow(a, w, &.{ k, "int8", try std.fmt.allocPrint(a, "{d}", .{tb.est_rows}) });
            sent = 1;
            break;
        }
        try message(w, 'C', try std.fmt.allocPrint(a, "SELECT {d}\x00", .{sent}));
    }

    fn answer(self: *FakePg, a: std.mem.Allocator, w: *std.Io.Writer, text: []const u8) !void {
        if (self.fail_on) |f| if (std.mem.indexOf(u8, text, f) != null) return failure(a, w, self.fail_msg);
        if (std.mem.indexOf(u8, text, "pg_index") != null) return self.answerKey(a, w, text);
        if (!std.mem.startsWith(u8, text, "SELECT ")) return failure(a, w, "syntax error");
        const from = std.mem.indexOf(u8, text, " FROM ") orelse return failure(a, w, "syntax error");
        const src = try self.source(a, text[from + " FROM ".len ..]);
        const outer_list = text["SELECT ".len..from];
        const list = if (std.mem.eql(u8, outer_list, "*")) src.list else outer_list;
        const tbl = src.tbl orelse return failure(a, w, try std.fmt.allocPrint(a, "relation \"{s}\" does not exist", .{src.name}));
        const where = src.where;
        if (std.mem.startsWith(u8, list, "MIN(")) return answerMinMax(a, w, tbl, list, where);

        var picks = std.array_list.Managed(usize).init(a);
        if (std.mem.eql(u8, list, "*")) {
            for (0..tbl.cols.len) |i| try picks.append(i);
        } else {
            var it = std.mem.splitSequence(u8, list, ", ");
            while (it.next()) |raw| {
                const name = unquote(raw);
                const i = colIndex(tbl, name) orelse
                    return failure(a, w, try std.fmt.allocPrint(a, "column \"{s}\" does not exist", .{name}));
                try picks.append(i);
            }
        }

        const cols = try a.alloc(Col, picks.items.len);
        for (picks.items, cols) |i, *c| c.* = tbl.cols[i];
        if (self.retype_on) |r| if (std.mem.indexOf(u8, text, r) != null) {
            for (cols) |*c| c.oid = self.retype_oid;
        };
        try describe(a, w, cols);
        const vals = try a.alloc(?[]const u8, picks.items.len);
        var sent: usize = 0;
        for (tbl.rows) |row| {
            if (!try keeps(a, tbl, row, where)) continue;
            for (picks.items, vals) |i, *v| v.* = row[i];
            try dataRow(a, w, vals);
            sent += 1;
        }
        try message(w, 'C', try std.fmt.allocPrint(a, "SELECT {d}\x00", .{sent}));
    }
};

/// `SELECT MIN("k") AS lo, MAX("k") AS hi` over the rows `where` keeps, `k` an
/// integer column; nulls when none is.
fn answerMinMax(a: std.mem.Allocator, w: *std.Io.Writer, tbl: Table, list: []const u8, where: []const u8) !void {
    const close = std.mem.indexOfScalar(u8, list, ')') orelse return failure(a, w, "syntax error");
    const ci = colIndex(tbl, unquote(list["MIN(".len..close])) orelse return failure(a, w, "column does not exist");
    var lo: ?i64 = null;
    var hi: ?i64 = null;
    for (tbl.rows) |row| {
        if (!try keeps(a, tbl, row, where)) continue;
        const v = std.fmt.parseInt(i64, row[ci] orelse continue, 10) catch continue;
        lo = if (lo) |x| @min(x, v) else v;
        hi = if (hi) |x| @max(x, v) else v;
    }
    const cols = [_]Col{ .{ .name = "lo", .oid = 20 }, .{ .name = "hi", .oid = 20 } };
    try describe(a, w, &cols);
    try dataRow(a, w, &.{
        if (lo) |x| try std.fmt.allocPrint(a, "{d}", .{x}) else null,
        if (hi) |x| try std.fmt.allocPrint(a, "{d}", .{x}) else null,
    });
    try message(w, 'C', "SELECT 1\x00");
}

fn describe(a: std.mem.Allocator, w: *std.Io.Writer, cols: []const Col) !void {
    var desc = std.array_list.Managed(u8).init(a);
    try desc.writer().writeInt(i16, @intCast(cols.len), .big);
    for (cols) |c| {
        try desc.appendSlice(c.name);
        try desc.append(0);
        try desc.writer().writeInt(i32, 0, .big);
        try desc.writer().writeInt(i16, 0, .big);
        try desc.writer().writeInt(i32, c.oid, .big);
        try desc.writer().writeInt(i16, -1, .big);
        try desc.writer().writeInt(i32, -1, .big);
        try desc.writer().writeInt(i16, 0, .big);
    }
    try message(w, 'T', desc.items);
}

fn dataRow(a: std.mem.Allocator, w: *std.Io.Writer, vals: []const ?[]const u8) !void {
    var d = std.array_list.Managed(u8).init(a);
    try d.writer().writeInt(i16, @intCast(vals.len), .big);
    for (vals) |v| {
        if (v) |x| {
            try d.writer().writeInt(i32, @intCast(x.len), .big);
            try d.appendSlice(x);
        } else try d.writer().writeInt(i32, -1, .big);
    }
    try message(w, 'D', d.items);
}

/// Whether `row` passes the parts of `where` this server reads: `1 = 0`, and each
/// `"col" IN (…)` of literals.
fn keeps(a: std.mem.Allocator, tbl: Table, row: []const ?[]const u8, where: []const u8) !bool {
    if (std.mem.indexOf(u8, where, "1 = 0") != null) return false;
    var at: usize = 0;
    while (std.mem.indexOfPos(u8, where, at, " IN (")) |i| {
        at = i + " IN (".len;
        const close = std.mem.indexOfScalarPos(u8, where, at, ')') orelse return true;
        const lhs_start = (std.mem.lastIndexOfScalar(u8, where[0..i], ' ') orelse std.math.maxInt(usize)) +% 1;
        const lhs = where[lhs_start..i];
        if (lhs.len < 2 or lhs[0] != '"' or lhs[lhs.len - 1] != '"') continue;
        const ci = colIndex(tbl, unquote(lhs)) orelse continue;
        const v = row[ci] orelse return false;
        var hit = false;
        var it = std.mem.splitSequence(u8, where[at..close], ", ");
        while (it.next()) |lit| {
            const text = if (lit.len >= 2 and lit[0] == '\'') try std.mem.replaceOwned(u8, a, lit[1 .. lit.len - 1], "''", "'") else lit;
            if (std.mem.eql(u8, text, v)) hit = true;
        }
        if (!hit) return false;
    }
    return true;
}

fn colIndex(tbl: Table, name: []const u8) ?usize {
    for (tbl.cols, 0..) |c, i| if (std.mem.eql(u8, c.name, name)) return i;
    return null;
}

fn lastPart(q: []const u8) []const u8 {
    const t = std.mem.trim(u8, q, " ");
    const dot = std.mem.lastIndexOfScalar(u8, t, '.') orelse return t;
    return t[dot + 1 ..];
}

fn unquote(s: []const u8) []const u8 {
    const t = std.mem.trim(u8, s, " ");
    if (t.len >= 2 and t[0] == '"' and t[t.len - 1] == '"') return t[1 .. t.len - 1];
    return t;
}

fn message(w: *std.Io.Writer, t: u8, body: []const u8) !void {
    try w.writeByte(t);
    try w.writeInt(u32, @intCast(body.len + 4), .big);
    try w.writeAll(body);
}

fn failure(a: std.mem.Allocator, w: *std.Io.Writer, msg: []const u8) !void {
    try message(w, 'E', try std.fmt.allocPrint(a, "SERROR\x00C57014\x00M{s}\x00\x00", .{msg}));
}

test "the fake server answers a table read, narrows by IN, refuses an unknown column" {
    const gpa = std.testing.allocator;
    const rows = [_][]const ?[]const u8{ &.{ "1", "a" }, &.{ "2", null }, &.{ "3", "c" } };
    const tables = [_]Table{.{ .name = "t", .cols = &.{ .{ .name = "id", .oid = 20 }, .{ .name = "v" } }, .rows = &rows }};
    const srv = try FakePg.start(gpa, &tables);
    defer srv.finish();
    const pg = @import("../db/postgres.zig");
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();

    const c1 = try pg.Conn.connect(gpa, "127.0.0.1", srv.port(), "u", "", "d", .off, .{});
    var cur = try c1.sqlConn().queryCursor("SELECT \"v\", \"id\" FROM \"public\".\"t\" WHERE \"id\" IN (1, 3)");
    var n: usize = 0;
    while (try cur.nextBatch(ar.allocator())) |b| {
        try std.testing.expectEqualStrings("v", b.schema.fields[0].name);
        try std.testing.expectEqual(@as(i64, 3), b.columns[1].getValue(b.len - 1).int);
        n += b.len;
    }
    cur.close();
    try std.testing.expectEqual(@as(usize, 2), n);

    const c2 = try pg.Conn.connect(gpa, "127.0.0.1", srv.port(), "u", "", "d", .off, .{});
    defer c2.close();
    try std.testing.expectError(error.PgQueryFailed, c2.sqlConn().queryCursor("SELECT \"nope\" FROM t"));
    try std.testing.expectEqualStrings("column \"nope\" does not exist", c2.wire.last_error);

    const got = try srv.received(gpa);
    defer gpa.free(got);
    try std.testing.expectEqual(@as(usize, 2), got.len);
    try std.testing.expectEqualStrings("SELECT \"nope\" FROM t", got[1]);
}

test "the login deadline is lifted once the session is up: a query slower than connect_timeout succeeds unless query_timeout is set" {
    const gpa = std.testing.allocator;
    const rows = [_][]const ?[]const u8{&.{"1"}};
    const tables = [_]Table{.{ .name = "t", .cols = &.{.{ .name = "id", .oid = 20 }}, .rows = &rows }};
    const srv = try FakePg.start(gpa, &tables);
    defer srv.finish();
    srv.delay_on = "'slow'";
    srv.delay_ms = 1500;
    const pg = @import("../db/postgres.zig");
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const q = "SELECT * FROM t WHERE 'slow' <> ''";

    const live = try pg.Conn.connect(gpa, "127.0.0.1", srv.port(), "u", "", "d", .off, .{ .connect_s = 1 });
    var cur = try live.sqlConn().queryCursor(q);
    defer cur.close();
    try std.testing.expectEqual(@as(usize, 1), (try cur.nextBatch(ar.allocator())).?.len);

    const capped = try pg.Conn.connect(gpa, "127.0.0.1", srv.port(), "u", "", "d", .off, .{ .connect_s = 1, .query_s = 1 });
    defer capped.close();
    try std.testing.expectError(error.ConnectionIoFailed, capped.sqlConn().queryCursor(q));
}
