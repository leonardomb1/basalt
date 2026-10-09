//! A PostgreSQL server for tests, each connection on a thread of its own: a trust
//! login (AuthenticationOk, ReadyForQuery; an SSLRequest is refused with 'N'), and
//! simple Query messages over tables held in memory, answered with RowDescription,
//! DataRows in text format, CommandComplete and ReadyForQuery. Every query's text
//! is recorded, so a test can assert what the engine sent.
//!
//! It reads only the queries basalt itself writes for a table: `SELECT * | "a", "b"
//! FROM <table> [WHERE …]`. A WHERE holding `1 = 0` returns no rows, and each
//! `"col" IN (…)` in it keeps the rows whose text is listed; anything else in a
//! WHERE is ignored, so it may return more rows than match, never fewer. A column
//! or table it does not hold is an ErrorResponse, as is any query holding `fail_on`,
//! which then carries `fail_msg`; a query holding `drop_on` has its connection
//! closed unanswered, as a server that went away mid-query.

const std = @import("std");

pub const Col = struct { name: []const u8, oid: i32 = 25 };

pub const Table = struct {
    name: []const u8,
    cols: []const Col,
    rows: []const []const ?[]const u8,
};

pub const FakePg = struct {
    gpa: std.mem.Allocator,
    listener: std.net.Server,
    tables: []const Table,
    fail_on: ?[]const u8 = null,
    fail_msg: []const u8 = "",
    drop_on: ?[]const u8 = null,
    stop: std.atomic.Value(bool) = .init(false),
    mutex: std.Thread.Mutex = .{},
    queries: std.array_list.Managed([]u8),
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
                    try self.answer(a, w, text);
                    try message(w, 'Z', "I");
                    try w.flush();
                },
                else => {},
            }
        }
    }

    fn answer(self: *FakePg, a: std.mem.Allocator, w: *std.Io.Writer, text: []const u8) !void {
        if (self.fail_on) |f| if (std.mem.indexOf(u8, text, f) != null) return failure(a, w, self.fail_msg);
        if (!std.mem.startsWith(u8, text, "SELECT ")) return failure(a, w, "syntax error");
        const from = std.mem.indexOf(u8, text, " FROM ") orelse return failure(a, w, "syntax error");
        const list = text["SELECT ".len..from];
        const rest = text[from + " FROM ".len ..];
        const where_at = std.mem.indexOf(u8, rest, " WHERE ");
        const tname = unquote(lastPart(rest[0 .. where_at orelse rest.len]));
        const where = if (where_at) |i| rest[i + " WHERE ".len ..] else "";
        const tbl = for (self.tables) |tb| {
            if (std.mem.eql(u8, tb.name, tname)) break tb;
        } else return failure(a, w, try std.fmt.allocPrint(a, "relation \"{s}\" does not exist", .{tname}));

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

        var desc = std.array_list.Managed(u8).init(a);
        try desc.writer().writeInt(i16, @intCast(picks.items.len), .big);
        for (picks.items) |i| {
            try desc.appendSlice(tbl.cols[i].name);
            try desc.append(0);
            try desc.writer().writeInt(i32, 0, .big);
            try desc.writer().writeInt(i16, 0, .big);
            try desc.writer().writeInt(i32, tbl.cols[i].oid, .big);
            try desc.writer().writeInt(i16, -1, .big);
            try desc.writer().writeInt(i32, -1, .big);
            try desc.writer().writeInt(i16, 0, .big);
        }
        try message(w, 'T', desc.items);

        var sent: usize = 0;
        for (tbl.rows) |row| {
            if (!try keeps(a, tbl, row, where)) continue;
            var d = std.array_list.Managed(u8).init(a);
            try d.writer().writeInt(i16, @intCast(picks.items.len), .big);
            for (picks.items) |i| {
                if (row[i]) |v| {
                    try d.writer().writeInt(i32, @intCast(v.len), .big);
                    try d.appendSlice(v);
                } else try d.writer().writeInt(i32, -1, .big);
            }
            try message(w, 'D', d.items);
            sent += 1;
        }
        try message(w, 'C', try std.fmt.allocPrint(a, "SELECT {d}\x00", .{sent}));
    }
};

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
    const pg = @import("postgres.zig");
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();

    const c1 = try pg.Conn.connect(gpa, "127.0.0.1", srv.port(), "u", "", "d", .off);
    var cur = try c1.sqlConn().queryCursor("SELECT \"v\", \"id\" FROM \"public\".\"t\" WHERE \"id\" IN (1, 3)");
    var n: usize = 0;
    while (try cur.nextBatch(ar.allocator())) |b| {
        try std.testing.expectEqualStrings("v", b.schema.fields[0].name);
        try std.testing.expectEqual(@as(i64, 3), b.columns[1].getValue(b.len - 1).int);
        n += b.len;
    }
    cur.close();
    try std.testing.expectEqual(@as(usize, 2), n);

    const c2 = try pg.Conn.connect(gpa, "127.0.0.1", srv.port(), "u", "", "d", .off);
    defer c2.close();
    try std.testing.expectError(error.PgQueryFailed, c2.sqlConn().queryCursor("SELECT \"nope\" FROM t"));
    try std.testing.expectEqualStrings("column \"nope\" does not exist", c2.last_error);

    const got = try srv.received(gpa);
    defer gpa.free(got);
    try std.testing.expectEqual(@as(usize, 2), got.len);
    try std.testing.expectEqualStrings("SELECT \"nope\" FROM t", got[1]);
}
