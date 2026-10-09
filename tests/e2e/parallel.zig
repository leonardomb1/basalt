//! End-to-end parallel lanes: threaded maps, aggregates, joins, DISTINCT and top-N
//! over CSV chunks and parquet row groups, each checked against the serial answer.

const std = @import("std");
const basalt = @import("basalt");
const parser = basalt.sql_parser;
const fixtures = basalt.fixtures;
const Diag = basalt.env.Diag;
const agg_combine_parallel_min = basalt.lanes.agg_combine_parallel_min;
const run = basalt.runtime.run;
const runToString = @import("harness.zig").runToString;
const runCsvThreaded = @import("harness.zig").runCsvThreaded;

test "parallel map keeps file order: a threaded load writes the rows a serial one does" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var input = std.array_list.Managed(u8).init(alloc);
    defer input.deinit();
    var want = std.array_list.Managed(u8).init(alloc);
    defer want.deinit();
    try input.appendSlice("id,v\n");
    try want.appendSlice("id\n");
    for (0..4000) |i| {
        try input.writer().print("{d},{d}\n", .{ i, i % 7 });
        if (i % 7 != 3) try want.writer().print("{d}\n", .{i});
    }
    for (0..5) |_| {
        const out = try runCsvThreaded(alloc, &tmp, input.items, "SELECT id FROM '$IN' WHERE v <> 3", 4);
        defer alloc.free(out);
        try std.testing.expectEqualStrings(want.items, out);
    }
}

test "parallel load into parquet: lanes encode row groups, the file keeps every row in order" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var input = std.array_list.Managed(u8).init(alloc);
    defer input.deinit();
    try input.appendSlice("id,s\n");
    for (0..240_000) |i| try input.writer().print("{d},s{d}\n", .{ i, i % 1000 });
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = input.items });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(alloc,
        \\LOAD INTO '{s}/out.parquet' AS SELECT id, s FROM '{s}/in.csv';
        \\LOAD INTO '{s}/back.csv' AS SELECT id, s FROM '{s}/out.parquet';
    , .{ base, base, base, base });
    defer alloc.free(script);
    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    _ = try run(alloc, prog, .{ .threads = 4 }, &rdiag);
    const got = try tmp.dir.readFileAlloc(alloc, "back.csv", 64 << 20);
    defer alloc.free(got);
    try std.testing.expectEqualStrings(input.items, got);
}

const fx_rg2 = fixtures.rg2_parquet;

fn runParquetThreaded(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir, query: []const u8, threads: usize) ![]u8 {
    try tmp.dir.writeFile(.{ .sub_path = "in.parquet", .data = fx_rg2 });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.parquet" });
    defer alloc.free(in_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);
    const q = try std.mem.replaceOwned(u8, alloc, query, "$IN", in_path);
    defer alloc.free(q);
    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}' AS {s};", .{ out_path, q });
    defer alloc.free(script);
    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    _ = run(alloc, prog, .{ .threads = threads }, &rdiag) catch |e| {
        return e;
    };
    return tmp.dir.readFileAlloc(alloc, "out.csv", 1 << 20);
}

test "parallel parquet aggregate: an ungrouped agg (threads>1) matches serial" {
    const alloc = std.testing.allocator;
    var t1 = std.testing.tmpDir(.{});
    defer t1.cleanup();
    const serial = try runParquetThreaded(alloc, &t1, "SELECT COUNT(*) AS n, SUM(id) AS sid, SUM(f) AS sf FROM '$IN'", 1);
    defer alloc.free(serial);
    var t4 = std.testing.tmpDir(.{});
    defer t4.cleanup();
    const par = try runParquetThreaded(alloc, &t4, "SELECT COUNT(*) AS n, SUM(id) AS sid, SUM(f) AS sf FROM '$IN'", 4);
    defer alloc.free(par);

    try std.testing.expectEqualStrings("n,sid,sf\n5000,12502500,6251250\n", serial);
    try std.testing.expectEqualStrings(serial, par);
}

test "parallel parquet aggregate: an ungrouped agg under a filter (threads>1)" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const par = try runParquetThreaded(alloc, &tmp, "SELECT COUNT(*) AS n, SUM(id) AS sid FROM '$IN' WHERE id <= 100", 4);
    defer alloc.free(par);
    try std.testing.expectEqualStrings("n,sid\n100,5050\n", par);
}

test "parallel parquet aggregate: a join then an ungrouped agg (threads>1)" {
    const alloc = std.testing.allocator;
    const q = "WITH d AS (SELECT id AS did FROM '$IN' WHERE id <= 50) " ++
        "SELECT COUNT(*) AS n, SUM(f) AS sf FROM '$IN' JOIN d ON id = did";
    var t1 = std.testing.tmpDir(.{});
    defer t1.cleanup();
    const serial = try runParquetThreaded(alloc, &t1, q, 1);
    defer alloc.free(serial);
    var t4 = std.testing.tmpDir(.{});
    defer t4.cleanup();
    const par = try runParquetThreaded(alloc, &t4, q, 4);
    defer alloc.free(par);

    try std.testing.expectEqualStrings("n,sf\n50,637.5\n", serial);
    try std.testing.expectEqualStrings(serial, par);
}

test "parallel parquet aggregate: a chain of two joins then an agg (threads>1)" {
    const alloc = std.testing.allocator;
    const q = "WITH d1 AS (SELECT id AS did FROM '$IN' WHERE id <= 100), " ++
        "d2 AS (SELECT id AS eid FROM '$IN' WHERE id <= 50) " ++
        "SELECT COUNT(*) AS n, SUM(f) AS sf FROM '$IN' JOIN d1 ON id = did JOIN d2 ON id = eid";
    var t1 = std.testing.tmpDir(.{});
    defer t1.cleanup();
    const serial = try runParquetThreaded(alloc, &t1, q, 1);
    defer alloc.free(serial);
    var t4 = std.testing.tmpDir(.{});
    defer t4.cleanup();
    const par = try runParquetThreaded(alloc, &t4, q, 4);
    defer alloc.free(par);

    try std.testing.expectEqualStrings("n,sf\n50,637.5\n", serial);
    try std.testing.expectEqualStrings(serial, par);
}

test "parallel parquet aggregate: a right join under an aggregate gives the serial answer at -j4" {
    const alloc = std.testing.allocator;
    const q = "WITH d AS (SELECT (id + 4995) AS did FROM '$IN' WHERE id <= 10) " ++
        "SELECT COUNT(*) AS n, SUM(id) AS sum_left FROM '$IN' RIGHT JOIN d ON id = did";
    var t1 = std.testing.tmpDir(.{});
    defer t1.cleanup();
    const serial = try runParquetThreaded(alloc, &t1, q, 1);
    defer alloc.free(serial);
    var t4 = std.testing.tmpDir(.{});
    defer t4.cleanup();
    const par = try runParquetThreaded(alloc, &t4, q, 4);
    defer alloc.free(par);

    try std.testing.expectEqualStrings("n,sum_left\n10,24990\n", serial);
    try std.testing.expectEqualStrings(serial, par);
}

test "parallel parquet aggregate: a full join under an aggregate gives the serial answer at -j4" {
    const alloc = std.testing.allocator;
    const q = "WITH d AS (SELECT (id + 4995) AS did FROM '$IN' WHERE id <= 10) " ++
        "SELECT COUNT(*) AS n FROM '$IN' FULL JOIN d ON id = did";
    var t1 = std.testing.tmpDir(.{});
    defer t1.cleanup();
    const serial = try runParquetThreaded(alloc, &t1, q, 1);
    defer alloc.free(serial);
    var t4 = std.testing.tmpDir(.{});
    defer t4.cleanup();
    const par = try runParquetThreaded(alloc, &t4, q, 4);
    defer alloc.free(par);

    try std.testing.expectEqualStrings("n\n5005\n", serial);
    try std.testing.expectEqualStrings(serial, par);
}

test "parallel parquet aggregate: the lane-safe join kinds under an aggregate give the serial answer at -j4" {
    const alloc = std.testing.allocator;
    inline for (.{
        .{ "INNER", "n\n5\n" },
        .{ "LEFT", "n\n5000\n" },
        .{ "SEMI", "n\n5\n" },
        .{ "ANTI", "n\n4995\n" },
    }) |c| {
        const q = "WITH d AS (SELECT (id + 4995) AS did FROM '$IN' WHERE id <= 10) " ++
            "SELECT COUNT(*) AS n FROM '$IN' " ++ c[0] ++ " JOIN d ON id = did";
        var t1 = std.testing.tmpDir(.{});
        defer t1.cleanup();
        const serial = try runParquetThreaded(alloc, &t1, q, 1);
        defer alloc.free(serial);
        var t4 = std.testing.tmpDir(.{});
        defer t4.cleanup();
        const par = try runParquetThreaded(alloc, &t4, q, 4);
        defer alloc.free(par);
        try std.testing.expectEqualStrings(c[1], serial);
        try std.testing.expectEqualStrings(serial, par);
    }
}

test "parallel parquet aggregate: an interleaved SELECT list still fans out" {
    const alloc = std.testing.allocator;
    const q = "SELECT g, COUNT(*) AS n, id FROM '$IN' WHERE id <= 4 GROUP BY g, id ORDER BY id";
    var t1 = std.testing.tmpDir(.{});
    defer t1.cleanup();
    const serial = try runParquetThreaded(alloc, &t1, q, 1);
    defer alloc.free(serial);
    var t4 = std.testing.tmpDir(.{});
    defer t4.cleanup();
    const par = try runParquetThreaded(alloc, &t4, q, 4);
    defer alloc.free(par);

    try std.testing.expectEqualStrings("g,n,id\n1,1,1\n2,1,2\n3,1,3\n0,1,4\n", serial);
    try std.testing.expectEqualStrings(serial, par);
}

test "parallel parquet aggregate: a join then a grouped agg (threads>1)" {
    const alloc = std.testing.allocator;
    const q = "WITH d AS (SELECT id AS did FROM '$IN' WHERE id <= 8) " ++
        "SELECT g, COUNT(*) AS n, SUM(f) AS sf FROM '$IN' JOIN d ON id = did GROUP BY g ORDER BY g";
    var t1 = std.testing.tmpDir(.{});
    defer t1.cleanup();
    const serial = try runParquetThreaded(alloc, &t1, q, 1);
    defer alloc.free(serial);
    var t4 = std.testing.tmpDir(.{});
    defer t4.cleanup();
    const par = try runParquetThreaded(alloc, &t4, q, 4);
    defer alloc.free(par);

    try std.testing.expectEqualStrings("g,n,sf\n0,2,6\n1,2,3\n2,2,4\n3,2,5\n", serial);
    try std.testing.expectEqualStrings(serial, par);
}

test "parallel parquet aggregate: a join, a grouped agg and a HAVING (threads>1)" {
    const alloc = std.testing.allocator;
    const q = "WITH d AS (SELECT id AS did FROM '$IN' WHERE id <= 8) " ++
        "SELECT g, COUNT(*) AS n, SUM(f) AS sf FROM '$IN' JOIN d ON id = did GROUP BY g HAVING SUM(f) > 3 ORDER BY g";
    var t1 = std.testing.tmpDir(.{});
    defer t1.cleanup();
    const serial = try runParquetThreaded(alloc, &t1, q, 1);
    defer alloc.free(serial);
    var t4 = std.testing.tmpDir(.{});
    defer t4.cleanup();
    const par = try runParquetThreaded(alloc, &t4, q, 4);
    defer alloc.free(par);

    try std.testing.expectEqualStrings("g,n,sf\n0,2,6\n2,2,4\n3,2,5\n", serial);
    try std.testing.expectEqualStrings(serial, par);
}

test "parallel CSV aggregate: global agg (threads>1)" {
    const alloc = std.testing.allocator;
    const input = "id,v\n1,10\n2,20\n3,30\n4,40\n5,50\n";
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const par = try runCsvThreaded(alloc, &tmp, input, "SELECT COUNT(*) AS n, SUM(CAST(v AS INT)) AS s FROM '$IN'", 4);
    defer alloc.free(par);
    try std.testing.expectEqualStrings("n,s\n5,150\n", par);
}

test "parallel CSV aggregate: a join then an agg (threads>1) matches serial" {
    const alloc = std.testing.allocator;
    const input = "id,v\n1,10\n2,20\n3,30\n4,40\n5,50\n";
    const q = "WITH d AS (SELECT id AS did FROM '$IN' WHERE CAST(id AS INT) <= 3) " ++
        "SELECT COUNT(*) AS n, SUM(CAST(v AS INT)) AS s FROM '$IN' JOIN d ON id = did";
    var t1 = std.testing.tmpDir(.{});
    defer t1.cleanup();
    const serial = try runCsvThreaded(alloc, &t1, input, q, 1);
    defer alloc.free(serial);
    var t4 = std.testing.tmpDir(.{});
    defer t4.cleanup();
    const par = try runCsvThreaded(alloc, &t4, input, q, 4);
    defer alloc.free(par);

    try std.testing.expectEqualStrings("n,s\n3,60\n", serial);
    try std.testing.expectEqualStrings(serial, par);
}

test "parallel CSV aggregate: filter/select prefix + sort/limit tail (threads>1)" {
    const alloc = std.testing.allocator;
    const input = "id,g,v\n1,a,10\n2,b,20\n3,a,30\n4,b,5\n5,a,50\n";
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runCsvThreaded(alloc, &tmp, input, "SELECT g, SUM(CAST(v AS INT)) AS s FROM '$IN' WHERE CAST(v AS INT) > 6 GROUP BY g ORDER BY s DESC LIMIT 1", 4);
    defer alloc.free(out);
    try std.testing.expectEqualStrings("g,s\na,90\n", out);
}

test "parallel CSV distinct (threads>1): dedups across chunks" {
    const alloc = std.testing.allocator;
    const input = "id,g\n1,a\n2,b\n3,a\n4,c\n5,b\n6,a\n";
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runCsvThreaded(alloc, &tmp, input, "SELECT DISTINCT g FROM '$IN' ORDER BY g ASC", 4);
    defer alloc.free(out);
    try std.testing.expectEqualStrings("g\na\nb\nc\n", out);
}

test "parallel CSV Top-N: sort | limit (threads>1)" {
    const alloc = std.testing.allocator;
    const input = "id,v\n1,10\n2,40\n3,20\n4,50\n5,30\n";
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runCsvThreaded(alloc, &tmp, input, "SELECT id, CAST(v AS INT) AS v FROM '$IN' ORDER BY v DESC, id ASC LIMIT 3", 4);
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,v\n4,50\n2,40\n5,30\n", out);
}

test "parallel CSV aggregate: grouped agg (threads>1) merges partials by key" {
    const alloc = std.testing.allocator;
    const input = "id,g,v\n1,a,10\n2,b,20\n3,a,30\n4,b,40\n5,a,50\n";
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const par = try runCsvThreaded(alloc, &tmp, input, "SELECT g, SUM(CAST(v AS INT)) AS s FROM '$IN' GROUP BY g", 4);
    defer alloc.free(par);
    try std.testing.expect(std.mem.startsWith(u8, par, "g,s\n"));
    try std.testing.expect(std.mem.indexOf(u8, par, "a,90\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, par, "b,60\n") != null);
    try std.testing.expectEqual(@as(usize, 3), std.mem.count(u8, par, "\n"));
}

const float_order_csv =
    "g,v\n" ++
    "a,1\n" ++ "a,1\n" ++ "a,1\n" ++ "a,1\n" ++
    "a,1\n" ++ "a,1\n" ++ "a,1\n" ++ "a,1\n" ++
    "a,10000000000000000\n";

test "parallel CSV aggregate: float SUM combines partials in chunk order" {
    const alloc = std.testing.allocator;
    for ([_]usize{ 4, 8, 8, 8, 8 }) |threads| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const out = try runCsvThreaded(alloc, &tmp, float_order_csv, "SELECT g, SUM(CAST(v AS FLOAT)) AS s FROM '$IN' GROUP BY g", threads);
        defer alloc.free(out);
        try std.testing.expectEqualStrings("g,s\na,10000000000000008\n", out);
    }
}

test "parallel CSV aggregate: high-cardinality combine is partitioned and exact" {
    const alloc = std.testing.allocator;
    const ngroups = (agg_combine_parallel_min * 3) / 2;
    var input = std.array_list.Managed(u8).init(alloc);
    defer input.deinit();
    try input.appendSlice("k,v\n");
    for (0..ngroups) |k| {
        try input.writer().print("{d},1\n{d},2\n", .{ k, k });
    }

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runCsvThreaded(alloc, &tmp, input.items, "SELECT k, COUNT(*) AS c, SUM(CAST(v AS INT)) AS s FROM '$IN' GROUP BY k", 4);
    defer alloc.free(out);

    var seen: usize = 0;
    var it = std.mem.tokenizeScalar(u8, out, '\n');
    _ = it.next();
    while (it.next()) |line| {
        var f = std.mem.tokenizeScalar(u8, line, ',');
        const k = f.next().?;
        try std.testing.expectEqualStrings("2", f.next().?);
        try std.testing.expectEqualStrings("3", f.next().?);
        const kv = try std.fmt.parseInt(usize, k, 10);
        try std.testing.expect(kv < ngroups);
        seen += 1;
    }
    try std.testing.expectEqual(ngroups, seen);
}

test "parallel CSV aggregate: the partitioned combine is reproducible" {
    const alloc = std.testing.allocator;
    const ngroups = (agg_combine_parallel_min * 3) / 2;
    var input = std.array_list.Managed(u8).init(alloc);
    defer input.deinit();
    try input.appendSlice("k,v\n");
    for (0..ngroups) |k| {
        try input.writer().print("{d},1\n{d},1\n{d},10000000000000000\n", .{ k, k, k });
    }

    var first: ?[]u8 = null;
    defer if (first) |f| alloc.free(f);
    for (0..4) |_| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const out = try runCsvThreaded(alloc, &tmp, input.items, "SELECT k, SUM(CAST(v AS FLOAT)) AS s FROM '$IN' GROUP BY k ORDER BY k", 8);
        if (first) |f| {
            defer alloc.free(out);
            try std.testing.expectEqualStrings(f, out);
        } else {
            first = out;
        }
    }

    var rows: usize = 0;
    var it = std.mem.tokenizeScalar(u8, first.?, '\n');
    try std.testing.expectEqualStrings("k,s", it.next().?);
    while (it.next()) |line| : (rows += 1) {
        var f = std.mem.tokenizeScalar(u8, line, ',');
        try std.testing.expectEqual(rows, try std.fmt.parseInt(usize, f.next().?, 10));
        try std.testing.expect(f.next() != null);
        try std.testing.expect(f.next() == null);
    }
    try std.testing.expectEqual(ngroups, rows);
}

/// Run a join script over `in.csv` (probe) + `lookup.csv` (build) at an explicit
/// thread count. `$IN`/`$LOOKUP` in `body` are replaced with the two paths; the
/// result comes back as written, since lanes keep file order.
fn runJoinThreaded(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir, body: []const u8, threads: usize) ![]u8 {
    var lanes: usize = 0;
    return runJoinLanes(alloc, tmp, body, threads, &lanes);
}

/// `runJoinThreaded`, also reporting how many lanes the run used.
fn runJoinLanes(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir, body: []const u8, threads: usize, lanes: *usize) ![]u8 {
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const out_name = try std.fmt.allocPrint(alloc, "out{d}.csv", .{threads});
    defer alloc.free(out_name);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);
    const lookup_path = try std.fs.path.join(alloc, &.{ base, "lookup.csv" });
    defer alloc.free(lookup_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, out_name });
    defer alloc.free(out_path);

    const q1 = try std.mem.replaceOwned(u8, alloc, body, "$IN", in_path);
    defer alloc.free(q1);
    const q2 = try std.mem.replaceOwned(u8, alloc, q1, "$LOOKUP", lookup_path);
    defer alloc.free(q2);
    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}' AS {s};", .{ out_path, q2 });
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    var summary: basalt.obs.Summary = .{ .run_id = 0 };
    _ = run(alloc, prog, .{ .threads = threads, .summary_out = &summary }, &rdiag) catch |e| {
        return e;
    };
    lanes.* = summary.threads;
    return tmp.dir.readFileAlloc(alloc, out_name, 1 << 20);
}

fn writeJoinFixtures(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir) !void {
    var in = std.array_list.Managed(u8).init(alloc);
    defer in.deinit();
    try in.appendSlice("id,code\n");
    var i: usize = 0;
    while (i < 2000) : (i += 1) try in.writer().print("{d},{c}\n", .{ i, "ABCDE"[i % 5] });
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = in.items });
    try tmp.dir.writeFile(.{ .sub_path = "lookup.csv", .data = "code,label\nA,Apple\nB,Banana\nC,Cherry\n" });
}

test "parallel join: inner join over CSV chunks matches the serial driver" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeJoinFixtures(alloc, &tmp);

    const body =
        "WITH labels AS (SELECT * FROM '$LOOKUP') " ++
        "SELECT t.id, l.label FROM '$IN' t JOIN labels l ON t.code = l.code";

    const serial = try runJoinThreaded(alloc, &tmp, body, 1);
    defer alloc.free(serial);
    const par = try runJoinThreaded(alloc, &tmp, body, 4);
    defer alloc.free(par);

    try std.testing.expectEqualStrings(serial, par);
    try std.testing.expectEqual(@as(usize, 1201), std.mem.count(u8, par, "\n"));
    try std.testing.expect(std.mem.startsWith(u8, par, "id,label\n0,Apple\n1,Banana\n2,Cherry\n5,Apple\n"));
    try std.testing.expect(std.mem.endsWith(u8, par, "\n1995,Apple\n1996,Banana\n1997,Cherry\n"));
}

test "parallel join: left join keeps unmatched probe rows on every lane" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeJoinFixtures(alloc, &tmp);

    const body =
        "WITH labels AS (SELECT * FROM '$LOOKUP') " ++
        "SELECT t.id, l.label FROM '$IN' t LEFT JOIN labels l ON t.code = l.code";

    const serial = try runJoinThreaded(alloc, &tmp, body, 1);
    defer alloc.free(serial);
    const par = try runJoinThreaded(alloc, &tmp, body, 4);
    defer alloc.free(par);

    try std.testing.expectEqualStrings(serial, par);
    try std.testing.expectEqual(@as(usize, 2001), std.mem.count(u8, par, "\n"));
    try std.testing.expect(std.mem.startsWith(u8, par, "id,label\n0,Apple\n1,Banana\n2,Cherry\n3,\n4,\n5,Apple\n"));
    try std.testing.expect(std.mem.endsWith(u8, par, "\n1997,Cherry\n1998,\n1999,\n"));
}

test "parallel join: a suffix filter on a right-side column runs after the join" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeJoinFixtures(alloc, &tmp);

    const body =
        "WITH labels AS (SELECT * FROM '$LOOKUP') " ++
        "SELECT t.id, l.label FROM '$IN' t JOIN labels l ON t.code = l.code WHERE l.label = 'Cherry'";

    const serial = try runJoinThreaded(alloc, &tmp, body, 1);
    defer alloc.free(serial);
    const par = try runJoinThreaded(alloc, &tmp, body, 4);
    defer alloc.free(par);

    try std.testing.expectEqualStrings(serial, par);
    try std.testing.expectEqual(@as(usize, 401), std.mem.count(u8, par, "\n"));
    try std.testing.expect(std.mem.startsWith(u8, par, "id,label\n2,Cherry\n7,Cherry\n12,Cherry\n"));
    try std.testing.expect(std.mem.endsWith(u8, par, "\n1992,Cherry\n1997,Cherry\n"));
    try std.testing.expect(std.mem.indexOf(u8, par, "Apple") == null);
}

test "parallel join then GROUP BY matches serial" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeJoinFixtures(alloc, &tmp);

    const body =
        "WITH labels AS (SELECT * FROM '$LOOKUP') " ++
        "SELECT l.label, COUNT(*) AS n FROM '$IN' t JOIN labels l ON t.code = l.code GROUP BY l.label ORDER BY l.label";

    const serial = try runJoinThreaded(alloc, &tmp, body, 1);
    defer alloc.free(serial);
    const par = try runJoinThreaded(alloc, &tmp, body, 4);
    defer alloc.free(par);

    try std.testing.expectEqualStrings("label,n\nApple,400\nBanana,400\nCherry,400\n", serial);
    try std.testing.expectEqualStrings(serial, par);
}

/// A probe side with null codes and a code nothing on the right has, and a build
/// side with a duplicate key, a null key and keys nothing on the left has.
fn writeOuterJoinFixtures(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir) !void {
    var in = std.array_list.Managed(u8).init(alloc);
    defer in.deinit();
    try in.appendSlice("id,code\n");
    for (0..3000) |i| {
        if (i % 7 == 0) {
            try in.writer().print("{d},\n", .{i});
        } else try in.writer().print("{d},{c}\n", .{ i, "ABCDE"[i % 5] });
    }
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = in.items });
    try tmp.dir.writeFile(.{ .sub_path = "lookup.csv", .data = "code,label\nA,Apple\nX,Xigua\nB,Banana\n,Nul\nA,Avocado\nZ,Zucchini\nC,Cherry\n" });
}

/// The rows of `body` at -j 1 and at -j 4, which must be identical and the -j 4 run
/// must have used lanes; returns the -j 4 output.
fn outerJoinSerialVsLanes(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir, body: []const u8) ![]u8 {
    var serial_lanes: usize = 0;
    const serial = try runJoinLanes(alloc, tmp, body, 1, &serial_lanes);
    defer alloc.free(serial);
    var par_lanes: usize = 0;
    const par = try runJoinLanes(alloc, tmp, body, 4, &par_lanes);
    errdefer alloc.free(par);
    try std.testing.expectEqualStrings(serial, par);
    try std.testing.expect(par_lanes > 1);
    return par;
}

test "parallel join: RIGHT and FULL joins run on lanes and drain the unmatched build rows once, last, in build order" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeOuterJoinFixtures(alloc, &tmp);

    const with = "WITH labels AS (SELECT * FROM '$LOOKUP') SELECT t.id, l.code, l.label FROM '$IN' t ";
    const Case = struct { body: []const u8, lines: usize, head: []const u8, tail: []const u8 };
    const cases = [_]Case{
        .{
            .body = with ++ "RIGHT JOIN labels l ON t.code = l.code",
            .lines = 2060,
            .head = "id,code,label\n1,B,Banana\n2,C,Cherry\n5,A,Apple\n5,A,Avocado\n6,B,Banana\n",
            .tail = "\n2995,A,Apple\n2995,A,Avocado\n2997,C,Cherry\n,X,Xigua\n,,Nul\n,Z,Zucchini\n",
        },
        .{
            .body = with ++ "FULL JOIN labels l ON t.code = l.code",
            .lines = 3518,
            .head = "id,code,label\n0,,\n1,B,Banana\n2,C,Cherry\n3,,\n4,,\n5,A,Apple\n5,A,Avocado\n",
            .tail = "\n2997,C,Cherry\n2998,,\n2999,,\n,X,Xigua\n,,Nul\n,Z,Zucchini\n",
        },
        .{
            .body = with ++ "RIGHT JOIN labels l ON t.code = l.code AND t.id < 3",
            .lines = 8,
            .head = "id,code,label\n1,B,Banana\n2,C,Cherry\n",
            .tail = "\n2,C,Cherry\n,A,Apple\n,X,Xigua\n,,Nul\n,A,Avocado\n,Z,Zucchini\n",
        },
        .{
            .body = with ++ "FULL JOIN labels l ON t.code = l.code AND t.id < 3",
            .lines = 3006,
            .head = "id,code,label\n0,,\n1,B,Banana\n2,C,Cherry\n3,,\n",
            .tail = "\n2999,,\n,A,Apple\n,X,Xigua\n,,Nul\n,A,Avocado\n,Z,Zucchini\n",
        },
        .{
            .body = with ++ "FULL JOIN labels l ON t.code = l.code WHERE t.id IS NULL OR t.id < 6",
            .lines = 11,
            .head = "id,code,label\n0,,\n1,B,Banana\n2,C,Cherry\n3,,\n4,,\n5,A,Apple\n5,A,Avocado\n",
            .tail = "\n5,A,Avocado\n,X,Xigua\n,,Nul\n,Z,Zucchini\n",
        },
        .{
            .body = with ++ "RIGHT JOIN labels l ON t.code = l.code WHERE l.label <> 'Banana'",
            .lines = 1546,
            .head = "id,code,label\n2,C,Cherry\n5,A,Apple\n5,A,Avocado\n",
            .tail = "\n2995,A,Avocado\n2997,C,Cherry\n,X,Xigua\n,,Nul\n,Z,Zucchini\n",
        },
        .{
            .body = with ++ "RIGHT JOIN labels l ON t.code = l.code WHERE t.id IS NULL",
            .lines = 4,
            .head = "id,code,label\n,X,Xigua\n",
            .tail = "\n,X,Xigua\n,,Nul\n,Z,Zucchini\n",
        },
    };
    for (cases) |case| {
        const out = try outerJoinSerialVsLanes(alloc, &tmp, case.body);
        defer alloc.free(out);
        try std.testing.expectEqual(case.lines, std.mem.count(u8, out, "\n"));
        try std.testing.expect(std.mem.startsWith(u8, out, case.head));
        try std.testing.expect(std.mem.endsWith(u8, out, case.tail));
    }
}

test "parallel join: a FULL join's drained rows land after the lanes' row groups in a Parquet sink" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeOuterJoinFixtures(alloc, &tmp);
    const body =
        "WITH labels AS (SELECT * FROM '$LOOKUP') " ++
        "SELECT t.id, l.code, l.label FROM '$IN' t FULL JOIN labels l ON t.code = l.code";

    var outs: [2][]u8 = undefined;
    for ([_]usize{ 1, 4 }, &outs) |threads, *out| {
        const base = try tmp.dir.realpathAlloc(alloc, ".");
        defer alloc.free(base);
        const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
        defer alloc.free(in_path);
        const lookup_path = try std.fs.path.join(alloc, &.{ base, "lookup.csv" });
        defer alloc.free(lookup_path);
        const pq_path = try std.fs.path.join(alloc, &.{ base, "out.parquet" });
        defer alloc.free(pq_path);
        const back_path = try std.fs.path.join(alloc, &.{ base, "back.csv" });
        defer alloc.free(back_path);
        const q1 = try std.mem.replaceOwned(u8, alloc, body, "$IN", in_path);
        defer alloc.free(q1);
        const q2 = try std.mem.replaceOwned(u8, alloc, q1, "$LOOKUP", lookup_path);
        defer alloc.free(q2);
        const load = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}' AS {s};", .{ pq_path, q2 });
        defer alloc.free(load);
        const back = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}' AS SELECT * FROM '{s}';", .{ back_path, pq_path });
        defer alloc.free(back);

        var parena = std.heap.ArenaAllocator.init(alloc);
        defer parena.deinit();
        var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        var rdiag: Diag = .{};
        var summary: basalt.obs.Summary = .{ .run_id = 0 };
        _ = try run(alloc, try parser.parseSource(parena.allocator(), load, &pdiag), .{ .threads = threads, .summary_out = &summary }, &rdiag);
        if (threads > 1) try std.testing.expect(summary.threads > 1);
        _ = try run(alloc, try parser.parseSource(parena.allocator(), back, &pdiag), .{ .threads = 1 }, &rdiag);
        out.* = try tmp.dir.readFileAlloc(alloc, "back.csv", 1 << 20);
    }
    defer for (outs) |o| alloc.free(o);
    try std.testing.expectEqualStrings(outs[0], outs[1]);
    try std.testing.expectEqual(@as(usize, 3518), std.mem.count(u8, outs[1], "\n"));
    try std.testing.expect(std.mem.endsWith(u8, outs[1], "\n2999,,\n,X,Xigua\n,,Nul\n,Z,Zucchini\n"));
}

test "a CSV read with a delimiter and an encoding fans out over lanes and reads each chunk in that dialect" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var input = std.array_list.Managed(u8).init(alloc);
    defer input.deinit();
    try input.appendSlice("cidade;valor\n");
    for (0..3000) |i| try input.writer().print("{s};{d}\n", .{ if (i % 3 == 0) "Bel\xe9m" else if (i % 3 == 1) "Goi\xe2nia" else "Macei\xf3", i % 7 });
    const q = "SELECT cidade, COUNT(*) AS n, SUM(valor) AS s FROM '$IN' WITH (delimiter = ';', encoding = 'latin1') GROUP BY cidade ORDER BY cidade";
    const want = "cidade,n,s\nBelém,1000,2999\nGoiânia,1000,2998\nMaceió,1000,2997\n";
    const serial = try runToString(alloc, &tmp, input.items, q);
    defer alloc.free(serial);
    try std.testing.expectEqualStrings(want, serial);
    const lanes = try runCsvThreaded(alloc, &tmp, input.items, q, 4);
    defer alloc.free(lanes);
    try std.testing.expectEqualStrings(want, lanes);
}

test "parallel parquet: an aggregate after DISTINCT and a HAVING both fan out and the sink gets the tail's columns" {
    const alloc = std.testing.allocator;
    var t1 = std.testing.tmpDir(.{});
    defer t1.cleanup();
    const dist = try runParquetThreaded(alloc, &t1, "SELECT COUNT(*) AS n, MIN(g) AS mg FROM (SELECT DISTINCT g FROM '$IN') t", 4);
    defer alloc.free(dist);
    try std.testing.expectEqualStrings("n,mg\n4,0\n", dist);
    var t2 = std.testing.tmpDir(.{});
    defer t2.cleanup();
    const having = try runParquetThreaded(alloc, &t2, "SELECT g, COUNT(*) AS c FROM '$IN' GROUP BY g HAVING SUM(id) > 3125000 ORDER BY g", 4);
    defer alloc.free(having);
    try std.testing.expectEqualStrings("g,c\n0,1250\n3,1250\n", having);
}
