//! End-to-end failure and load reporting: transient database errors and the
//! on_load callback.

const std = @import("std");
const basalt = @import("basalt");
const parser = basalt.sql_parser;
const Diag = basalt.env.Diag;
const isTransient = basalt.env.isTransient;
const OutcomeSink = basalt.env.OutcomeSink;
const run = basalt.runtime.run;

const SlamServer = struct {
    server: std.net.Server,
    done: std.atomic.Value(bool) = .init(false),

    fn loop(self: *SlamServer) void {
        while (true) {
            const c = self.server.accept() catch return;
            c.stream.close();
            if (self.done.load(.acquire)) return;
        }
    }
};

test "a database that closes the connection is a transient failure (exit 75), at top level and per for-each item" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "names.csv", .data = "name\nalpha\nbeta\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    var slam = SlamServer{ .server = try (try std.net.Address.parseIp("127.0.0.1", 0)).listen(.{ .reuse_address = true }) };
    defer slam.server.deinit();
    const port = slam.server.listen_address.getPort();
    const th = try std.Thread.spawn(.{}, SlamServer.loop, .{&slam});
    defer {
        slam.done.store(true, .release);
        if (std.net.tcpConnectToAddress(slam.server.listen_address)) |s| s.close() else |_| {}
        th.join();
    }

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };

    for ([_][]const u8{ "postgres", "mysql", "sqlserver" }) |kind| {
        const top = try std.fmt.allocPrint(parena.allocator(),
            \\CREATE CONNECTION db TYPE {s} OPTIONS (host = '127.0.0.1', port = {d}, user = 'u', password = 'p', database = 'd');
            \\LOAD INTO '{s}/out.csv' AS SELECT * FROM db.t;
        , .{ kind, port, base });
        const tprog = try parser.parseSource(parena.allocator(), top, &pdiag);
        var tdiag: Diag = .{};
        const te = if (run(alloc, tprog, .{}, &tdiag)) |_| return error.TestUnexpectedResult else |e| e;
        if (!(tdiag.retryable or isTransient(te))) {
            std.debug.print("{s}: {s} ({s}) not transient\n", .{ kind, @errorName(te), tdiag.msg });
            return error.TestUnexpectedResult;
        }

        const each = try std.fmt.allocPrint(parena.allocator(),
            \\CREATE CONNECTION db TYPE {s} OPTIONS (host = '127.0.0.1', port = {d}, user = 'u', password = 'p', database = 'd');
            \\FOR EACH ROW OF ('{s}/names.csv') AS (name) SEQUENTIAL ON ERROR CONTINUE
            \\  LOAD INTO '{s}/out_${{name}}.csv' AS SELECT * FROM db.t;
            \\END FOR;
        , .{ kind, port, base, base });
        const eprog = try parser.parseSource(parena.allocator(), each, &pdiag);
        var sink = OutcomeSink.init(parena.allocator());
        var ediag: Diag = .{};
        _ = run(alloc, eprog, .{ .outcomes = &sink }, &ediag) catch {};
        try std.testing.expectEqual(@as(usize, 2), sink.failures());
        try std.testing.expectEqual(@as(usize, 2), sink.list.items.len);
        for (sink.list.items) |o| {
            if (!o.ok and !o.retryable) {
                std.debug.print("{s}: a for-each item failed as permanent\n", .{kind});
                return error.TestUnexpectedResult;
            }
        }
    }
}

test "onWire names a closed or failed database socket as transient, and leaves the rest alone" {
    const sql = basalt.sql;
    try std.testing.expectEqual(error.ServerClosedConnection, sql.onWire(error.EndOfStream));
    try std.testing.expectEqual(error.ServerClosedConnection, sql.onWire(error.TlsConnectionTruncated));
    try std.testing.expectEqual(error.ConnectionIoFailed, sql.onWire(error.ReadFailed));
    try std.testing.expectEqual(error.ConnectionIoFailed, sql.onWire(error.WriteFailed));
    try std.testing.expectEqual(error.TdsProtocol, sql.onWire(error.TdsProtocol));
    try std.testing.expect(isTransient(sql.onWire(error.EndOfStream)));
    try std.testing.expect(isTransient(sql.onWire(error.ReadFailed)));
    try std.testing.expect(!isTransient(error.EndOfStream));
    try std.testing.expect(!isTransient(error.WriteFailed));
}

const LoadLog = struct {
    mu: std.Thread.Mutex = .{},
    arena: std.mem.Allocator,
    list: std.array_list.Managed(basalt.env.LoadDone),

    fn f(ctx: *anyopaque, done: basalt.env.LoadDone) void {
        const self: *LoadLog = @ptrCast(@alignCast(ctx));
        self.mu.lock();
        defer self.mu.unlock();
        var d = done;
        d.target = self.arena.dupe(u8, done.target) catch "";
        d.reason = self.arena.dupe(u8, done.reason) catch "";
        self.list.append(d) catch {};
    }
};

test "on_load: every LOAD reports as it finishes — written, failed before writing, per for-each row — and the summary survives a failed run" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "names.csv", .data = "n\na\nb\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    var ar = std.heap.ArenaAllocator.init(alloc);
    defer ar.deinit();
    const a = ar.allocator();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };

    {
        var log = LoadLog{ .arena = a, .list = .init(a) };
        const src = try std.fmt.allocPrint(a,
            \\LOAD INTO '{s}/one.csv' AS SELECT range AS v FROM RANGE(5);
            \\LOAD INTO '/nonexistent/x.csv' AS SELECT 1 AS v;
        , .{base});
        const prog = try parser.parseSource(a, src, &pdiag);
        var summary: basalt.obs.Summary = .{ .run_id = 0 };
        var rdiag: Diag = .{};
        try std.testing.expectError(error.PlanFailed, run(alloc, prog, .{ .on_load = .{ .ctx = &log, .f = LoadLog.f }, .summary_out = &summary }, &rdiag));
        try std.testing.expectEqual(@as(usize, 2), log.list.items.len);
        const ok = log.list.items[0];
        try std.testing.expect(ok.ok);
        try std.testing.expectEqual(@as(u64, 0), ok.ordinal);
        try std.testing.expectEqual(@as(u64, 5), ok.rows_written);
        try std.testing.expectEqual(@as(u64, 5), ok.rows_read);
        try std.testing.expectEqual(@as(u32, 1), ok.line);
        const bad = log.list.items[1];
        try std.testing.expect(!bad.ok);
        try std.testing.expectEqual(@as(u32, 2), bad.line);
        try std.testing.expectEqualStrings("/nonexistent/x.csv", bad.target);
        try std.testing.expect(std.mem.indexOf(u8, bad.reason, "FileNotFound") != null);
        try std.testing.expect(!bad.transient);
        try std.testing.expectEqual(@as(u64, 1), summary.loads);
        try std.testing.expectEqual(@as(u64, 1), summary.loads_failed);
        try std.testing.expectEqual(@as(?u64, 5), summary.rows_loaded);
    }

    {
        var log = LoadLog{ .arena = a, .list = .init(a) };
        const src = try std.fmt.allocPrint(a,
            \\FOR EACH ROW OF ('{s}/names.csv') AS (n)
            \\  LOAD INTO IDENTIFIER('{s}/out_' || $n || '.csv') AS SELECT range AS v FROM RANGE(3);
            \\END FOR;
        , .{ base, base });
        const prog = try parser.parseSource(a, src, &pdiag);
        var rdiag: Diag = .{};
        _ = try run(alloc, prog, .{ .on_load = .{ .ctx = &log, .f = LoadLog.f } }, &rdiag);
        try std.testing.expectEqual(@as(usize, 2), log.list.items.len);
        for (log.list.items, 1..) |l, k| {
            try std.testing.expect(l.ok);
            try std.testing.expectEqual(k, l.loop_row);
            try std.testing.expectEqual(@as(usize, 2), l.loop_rows);
            try std.testing.expect(std.mem.endsWith(u8, l.target, if (k == 1) "out_a.csv" else "out_b.csv"));
        }
    }

    {
        try tmp.dir.writeFile(.{ .sub_path = "sizes.csv", .data = "n\n1\n2\n3\n4\n5\n6\n" });
        for (1..7) |n| {
            var body = std.array_list.Managed(u8).init(a);
            try body.appendSlice("v\n");
            for (0..n * 1000) |v| try body.writer().print("{d}\n", .{v});
            try tmp.dir.writeFile(.{ .sub_path = try std.fmt.allocPrint(a, "in_{d}.csv", .{n}), .data = body.items });
        }
        var log = LoadLog{ .arena = a, .list = .init(a) };
        const src = try std.fmt.allocPrint(a,
            \\FOR EACH ROW OF ('{s}/sizes.csv') AS (n:INT) PARALLEL
            \\  LOAD INTO IDENTIFIER('{s}/par_' || $n || '.csv') AS SELECT v FROM IDENTIFIER('{s}/in_' || $n || '.csv');
            \\END FOR;
        , .{ base, base, base });
        const prog = try parser.parseSource(a, src, &pdiag);
        var summary: basalt.obs.Summary = .{ .run_id = 0 };
        var rdiag: Diag = .{};
        _ = run(alloc, prog, .{ .threads = 4, .on_load = .{ .ctx = &log, .f = LoadLog.f }, .summary_out = &summary }, &rdiag) catch |e| {
            std.debug.print("parallel on_load run: {s} ({s})\n", .{ @errorName(e), rdiag.msg });
            return e;
        };
        try std.testing.expectEqual(@as(usize, 6), log.list.items.len);
        for (log.list.items) |l| {
            try std.testing.expect(l.ok);
            try std.testing.expectEqual(@as(u64, l.loop_row * 1000), l.rows_read);
            try std.testing.expectEqual(l.rows_read, l.rows_written);
        }
        try std.testing.expectEqual(@as(u64, 21_000), summary.rows_read);
    }
}
