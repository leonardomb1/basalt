//! End-to-end writes of Delta tables: a LOAD creating a table, appending to it,
//! replacing it and upserting into it, each version read back by basalt;
//! partitioned tables, the log's actions and statistics as written, and what a
//! write refuses, in `check` and `run` alike where `check` can see it.

const std = @import("std");
const basalt = @import("basalt");
const harness = @import("harness.zig");
const parser = basalt.sql_parser;
const Diag = basalt.env.Diag;
const run = basalt.runtime.run;

const testing = std.testing;
const gpa = testing.allocator;

/// `script` (with `$B` the tmp dir) checked and run on `threads` lanes.
fn load(tmp: *testing.TmpDir, script: []const u8, threads: usize) !void {
    try harness.checkAndRun(gpa, tmp, script, threads, &.{});
}

/// The rows `query` returns, as CSV.
fn rows(a: std.mem.Allocator, tmp: *testing.TmpDir, query: []const u8) ![]const u8 {
    try load(tmp, try std.fmt.allocPrint(a, "LOAD INTO '$B/out.csv' AS {s};", .{query}), 1);
    return tmp.dir.readFileAlloc(a, "out.csv", 1 << 20);
}

/// `run` (not `check`, which cannot see it) refuses `script`, naming `want`.
fn runRefuses(a: std.mem.Allocator, tmp: *testing.TmpDir, script: []const u8, want: []const u8) !void {
    const base = try tmp.dir.realpathAlloc(a, ".");
    const text = try std.mem.replaceOwned(u8, a, script, "$B", base);
    var pd: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(a, text, &pd);
    var rdiag: Diag = .{};
    if (run(gpa, prog, .{ .log = .{ .quiet = true } }, &rdiag)) |_| {
        std.debug.print("run accepted: {s}\n", .{text});
        return error.TestUnexpectedResult;
    } else |e| {
        const msg = if (rdiag.msg.len > 0) rdiag.msg else basalt.eval.peekFailure(e) orelse @errorName(e);
        if (std.mem.indexOf(u8, msg, want) == null) {
            std.debug.print("want `{s}` in `{s}`\n", .{ want, msg });
            return error.TestUnexpectedResult;
        }
    }
}

fn logFile(a: std.mem.Allocator, tmp: *testing.TmpDir, root: []const u8, v: u64) ![]const u8 {
    return tmp.dir.readFileAlloc(a, try std.fmt.allocPrint(a, "{s}/_delta_log/{d:0>20}.json", .{ root, v }), 1 << 20);
}

fn exists(tmp: *testing.TmpDir, path: []const u8) bool {
    tmp.dir.access(path, .{}) catch return false;
    return true;
}

test "Delta write: create, append (the default), REPLACE; every version reads back" {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    try load(&tmp, "LOAD INTO '$B/t/' WITH (format = 'delta') AS SELECT id, grp, amt FROM '$B/a.csv';", 1);
    const v0 = try logFile(a, &tmp, "t", 0);
    try testing.expect(std.mem.startsWith(u8, v0, "{\"commitInfo\":{"));
    try testing.expect(std.mem.indexOf(u8, v0, "\"txnId\":\"") != null);
    try testing.expect(std.mem.indexOf(u8, v0, "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}") != null);
    try testing.expect(std.mem.indexOf(u8, v0, "\\\"type\\\":\\\"long\\\"") != null);
    try testing.expect(std.mem.indexOf(u8, v0, "\\\"numRecords\\\":3,\\\"minValues\\\":{\\\"id\\\":1,\\\"grp\\\":\\\"a\\\",\\\"amt\\\":5},\\\"maxValues\\\":{\\\"id\\\":3,\\\"grp\\\":\\\"b\\\",\\\"amt\\\":20},\\\"nullCount\\\":{\\\"id\\\":0,\\\"grp\\\":0,\\\"amt\\\":0}") != null);

    try load(&tmp, "LOAD INTO '$B/t/' AS SELECT id + 10 AS id, grp, amt FROM '$B/a.csv' WHERE grp = 'a';", 1);
    try testing.expect(std.mem.indexOf(u8, try logFile(a, &tmp, "t", 1), "\"isBlindAppend\":true") != null);
    try load(&tmp, "LOAD INTO '$B/t/' APPEND AS SELECT id + 20 AS id, grp, amt FROM '$B/a.csv' WHERE grp = 'b';", 1);
    try testing.expectEqualStrings("id,grp,amt\n1,a,10\n2,a,20\n3,b,5\n11,a,10\n12,a,20\n23,b,5\n", try rows(a, &tmp, "SELECT * FROM '$B/t/' ORDER BY id"));

    try load(&tmp, "LOAD INTO '$B/t/' REPLACE AS SELECT id, grp, amt * 2 AS amt FROM '$B/a.csv' WHERE id = 2;", 1);
    const v3 = try logFile(a, &tmp, "t", 3);
    try testing.expectEqual(@as(usize, 3), std.mem.count(u8, v3, "{\"remove\":"));
    try testing.expectEqual(@as(usize, 1), std.mem.count(u8, v3, "{\"add\":"));
    try testing.expectEqualStrings("id,grp,amt\n2,a,40\n", try rows(a, &tmp, "SELECT * FROM '$B/t/'"));
    try testing.expectEqualStrings("n\n3\n", try rows(a, &tmp, "SELECT COUNT(*) AS n FROM '$B/t/' WITH (version = 0)"));
    try testing.expectEqualStrings("n\n5\n", try rows(a, &tmp, "SELECT COUNT(*) AS n FROM '$B/t/' WITH (version = 1)"));
    try testing.expectEqualStrings("n\n6\n", try rows(a, &tmp, "SELECT COUNT(*) AS n FROM '$B/t/' WITH (version = 2)"));
    try testing.expect(!exists(&tmp, "t/_delta_log/00000000000000000004.json"));
}

test "Delta write: empty create, an empty append commits nothing, an empty REPLACE removes every file" {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    try load(&tmp, "LOAD INTO '$B/e/' WITH (format = 'delta') AS SELECT id, grp FROM '$B/a.csv' WHERE id > 99;", 1);
    try testing.expect(std.mem.indexOf(u8, try logFile(a, &tmp, "e", 0), "{\"add\":") == null);
    try testing.expectEqualStrings("n\n0\n", try rows(a, &tmp, "SELECT COUNT(*) AS n FROM '$B/e/'"));
    try load(&tmp, "LOAD INTO '$B/e/' AS SELECT id, grp FROM '$B/a.csv' WHERE id > 99;", 1);
    try testing.expect(!exists(&tmp, "e/_delta_log/00000000000000000001.json"));
    try load(&tmp, "LOAD INTO '$B/e/' AS SELECT id, grp FROM '$B/a.csv';", 1);
    try load(&tmp, "LOAD INTO '$B/e/' REPLACE AS SELECT id, grp FROM '$B/a.csv' WHERE id > 99;", 1);
    const v2 = try logFile(a, &tmp, "e", 2);
    try testing.expect(std.mem.indexOf(u8, v2, "{\"remove\":") != null and std.mem.indexOf(u8, v2, "{\"add\":") == null);
    try testing.expectEqualStrings("n\n0\n", try rows(a, &tmp, "SELECT COUNT(*) AS n FROM '$B/e/'"));
}

test "Delta write: partitions as Hive folders, escaped, NULL as the default partition, not inside the files" {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "p.csv", .data = "id,region,day,ts,amt\n1,eu,2026-01-01,2026-01-01 01:02:03.5,1.50\n2,us/east,2026-01-02,2026-01-02 00:00:00,2.25\n3,a:b=c%d,2026-01-01,,0.00\n4,,,2026-03-04 05:06:07,9.99\n" });
    const sel = "SELECT id, region, CAST(day AS DATE) AS day, CAST(ts AS TIMESTAMP) AS ts, CAST(amt AS DECIMAL(10,2)) AS amt FROM '$B/p.csv'";
    try load(&tmp, try std.fmt.allocPrint(a, "LOAD INTO '$B/p/' WITH (format = 'delta', partition_by = 'region, day') AS {s};", .{sel}), 1);
    try testing.expect(exists(&tmp, "p/region=eu/day=2026-01-01"));
    try testing.expect(exists(&tmp, "p/region=us%2Feast/day=2026-01-02"));
    try testing.expect(exists(&tmp, "p/region=a%3Ab%3Dc%25d/day=2026-01-01"));
    try testing.expect(exists(&tmp, "p/region=__HIVE_DEFAULT_PARTITION__/day=__HIVE_DEFAULT_PARTITION__"));
    const v0 = try logFile(a, &tmp, "p", 0);
    try testing.expect(std.mem.indexOf(u8, v0, "\"partitionColumns\":[\"region\",\"day\"]") != null);
    try testing.expect(std.mem.indexOf(u8, v0, "\"path\":\"region=a%253Ab%253Dc%2525d/day=2026-01-01/part-") != null);
    try testing.expect(std.mem.indexOf(u8, v0, "\"partitionValues\":{\"region\":\"a:b=c%d\",\"day\":\"2026-01-01\"}") != null);
    try testing.expect(std.mem.indexOf(u8, v0, "\"partitionValues\":{\"region\":null,\"day\":null}") != null);
    try testing.expect(std.mem.indexOf(u8, v0, "\\\"ts\\\":\\\"2026-01-01T01:02:03.500Z\\\"") != null);
    try testing.expect(std.mem.indexOf(u8, v0, "\\\"nullCount\\\":{\\\"id\\\":0,\\\"ts\\\":0,\\\"amt\\\":0}") != null);

    const want = "id,region,day,ts,amt\n1,eu,2026-01-01,2026-01-01 01:02:03.500000,1.50\n2,us/east,2026-01-02,2026-01-02 00:00:00,2.25\n3,a:b=c%d,2026-01-01,,0.00\n4,,,2026-03-04 05:06:07,9.99\n";
    try testing.expectEqualStrings(want, try rows(a, &tmp, "SELECT * FROM '$B/p/' ORDER BY id"));
    try testing.expectEqualStrings("id,ts,amt\n1,2026-01-01 01:02:03.500000,1.50\n", try rows(a, &tmp, "SELECT * FROM '$B/p/region=eu/day=2026-01-01/' WITH (format = 'parquet')"));

    try load(&tmp, "LOAD INTO '$B/p/' AS SELECT 5 AS id, 'eu' AS region, CAST('2026-01-01' AS DATE) AS day, CAST(NULL AS TIMESTAMP) AS ts, CAST('7.00' AS DECIMAL(10,2)) AS amt;", 1);
    try testing.expectEqualStrings("n\n2\n", try rows(a, &tmp, "SELECT COUNT(*) AS n FROM '$B/p/' WHERE region = 'eu'"));
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/p/' WITH (partition_by = 'day') AS SELECT * FROM '$B/p/';", "is partitioned by (region, day), not (day)");
}

test "Delta write: past the open-file cap, partitions close and reopen into further files; lanes write what one does" {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var csv = std.array_list.Managed(u8).init(a);
    try csv.appendSlice("id,k\n");
    for (0..20000) |i| try csv.writer().print("{d},{d}\n", .{ i, i % 40 });
    try tmp.dir.writeFile(.{ .sub_path = "many.csv", .data = csv.items });

    try load(&tmp, "LOAD INTO '$B/m/' WITH (format = 'delta', partition_by = 'k') AS SELECT id, k FROM '$B/many.csv';", 1);
    const v0 = try logFile(a, &tmp, "m", 0);
    try testing.expect(std.mem.count(u8, v0, "{\"add\":") > 40);
    try testing.expectEqualStrings("n,s,ks\n20000,199990000,40\n", try rows(a, &tmp, "SELECT COUNT(*) AS n, SUM(id) AS s, COUNT(DISTINCT k) AS ks FROM '$B/m/'"));
    try testing.expectEqualStrings("n\n500\n", try rows(a, &tmp, "SELECT COUNT(*) AS n FROM '$B/m/' WHERE k = 7 AND id % 40 = 7"));

    try load(&tmp, "LOAD INTO '$B/m/' AS SELECT id + 20000 AS id, k FROM '$B/many.csv';", 4);
    try testing.expectEqualStrings("n,s\n40000,799980000\n", try rows(a, &tmp, "SELECT COUNT(*) AS n, SUM(id) AS s FROM '$B/m/'"));
}

test "Delta write: an existing table's types, narrower than basalt's, are written as the table has them" {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const fields = "{\\\"name\\\":\\\"i\\\",\\\"type\\\":\\\"integer\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"b\\\",\\\"type\\\":\\\"byte\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"f\\\",\\\"type\\\":\\\"float\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"d\\\",\\\"type\\\":\\\"decimal(10,3)\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"n\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":false,\\\"metadata\\\":{}}";
    try tmp.dir.makePath("n/_delta_log");
    try tmp.dir.writeFile(.{ .sub_path = "n/_delta_log/00000000000000000000.json", .data = try std.fmt.allocPrint(a,
        \\{{"protocol":{{"minReaderVersion":1,"minWriterVersion":2}}}}
        \\{{"metaData":{{"id":"n","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{{\"type\":\"struct\",\"fields\":[{s}]}}","partitionColumns":[],"configuration":{{}},"createdTime":0}}}}
        \\
    , .{fields}) });
    try load(&tmp, "LOAD INTO '$B/n/' AS SELECT 7 AS i, -8 AS b, CAST(1.5 AS DOUBLE) AS f, CAST('2.5' AS DECIMAL(4,1)) AS d, 'x' AS n;", 1);
    try testing.expectEqualStrings("i,b,f,d,n\n7,-8,1.5,2.500,x\n", try rows(a, &tmp, "SELECT * FROM '$B/n/'"));
    try runRefuses(a, &tmp, "LOAD INTO '$B/n/' AS SELECT 7 AS i, 200 AS b, CAST(1.5 AS DOUBLE) AS f, CAST('2.5' AS DECIMAL(4,1)) AS d, 'x' AS n;", "column `b` is `byte` in Delta table");
    try runRefuses(a, &tmp, "LOAD INTO '$B/n/' AS SELECT 7 AS i, 1 AS b, CAST(1.5 AS DOUBLE) AS f, CAST('2.5' AS DECIMAL(4,1)) AS d, CAST(NULL AS VARCHAR) AS n;", "column `n` is NOT NULL");
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/n/' AS SELECT 7 AS i, 1 AS b, CAST(1.5 AS DOUBLE) AS f, CAST('2.5' AS DECIMAL(12,4)) AS d, 'x' AS n;", "cannot hold every value");
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/n/' AS SELECT 'x' AS i, 1 AS b, CAST(1.5 AS DOUBLE) AS f, CAST('2.5' AS DECIMAL(4,1)) AS d, 'x' AS n;", "column `i` is string in the rows but `integer`");
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/n/' AS SELECT 7 AS i, 1 AS b, CAST(1.5 AS DOUBLE) AS f, 'x' AS n;", "has column `d`, which the rows lack");
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/n/' AS SELECT 7 AS i, 1 AS b, CAST(1.5 AS DOUBLE) AS f, CAST('2.5' AS DECIMAL(4,1)) AS d, 'x' AS n, 1 AS extra;", "the rows have column `extra`");
    try testing.expect(!exists(&tmp, "n/_delta_log/00000000000000000002.json"));

    try load(&tmp, "LOAD INTO '$B/n/' AS SELECT 8 AS i, 1 AS b, CAST(2.5 AS DOUBLE) AS f, CAST('3.25' AS DECIMAL(4,2)) AS d, 'y' AS n;", 1);
    try load(&tmp, "LOAD INTO '$B/n/' UPSERT ON (i) PARTIAL COLS (n) AS SELECT 7 AS i, 'x2' AS n;", 1);
    try testing.expectEqual(@as(usize, 1), try actions(a, &tmp, "n", 3, "remove"));
    try load(&tmp, "LOAD INTO '$B/n/' UPSERT ON (d) PARTIAL COLS (n) AS SELECT CAST('3.25' AS DECIMAL(4,2)) AS d, 'y2' AS n;", 1);
    try testing.expectEqual(@as(usize, 1), try actions(a, &tmp, "n", 4, "remove"));
    try testing.expectEqualStrings("i,b,f,d,n\n7,-8,1.5,2.500,x2\n8,1,2.5,3.250,y2\n", try rows(a, &tmp, "SELECT * FROM '$B/n/' ORDER BY i"));
}

test "Delta write: lists, structs and maps create, append and UPSERT, a struct's fields with nested statistics, a table's narrow leaves kept" {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const nest = "CAST(json_object('k', id, 'addr', json_object('city', grp), 'amt', amt) AS STRUCT(k INT, addr STRUCT(city STRING), amt DECIMAL(10,2))) AS s, CAST(json_array(id, amt) AS LIST(INT)) AS l, CAST(json_object(grp, id) AS MAP(STRING, INT)) AS m";
    try load(&tmp, try std.fmt.allocPrint(a, "LOAD INTO '$B/t/' WITH (format = 'delta') AS SELECT id, {s} FROM '$B/a.csv';", .{nest}), 1);
    const v0 = try logFile(a, &tmp, "t", 0);
    try testing.expect(std.mem.indexOf(u8, v0, "\\\"type\\\":{\\\"type\\\":\\\"array\\\",\\\"elementType\\\":\\\"long\\\",\\\"containsNull\\\":true}") != null);
    try testing.expect(std.mem.indexOf(u8, v0, "\\\"type\\\":{\\\"type\\\":\\\"map\\\",\\\"keyType\\\":\\\"string\\\",\\\"valueType\\\":\\\"long\\\",\\\"valueContainsNull\\\":true}") != null);
    try testing.expect(std.mem.indexOf(u8, v0, "\\\"minValues\\\":{\\\"id\\\":1,\\\"s\\\":{\\\"k\\\":1,\\\"addr\\\":{\\\"city\\\":\\\"a\\\"},\\\"amt\\\":5.00}}") != null);
    try testing.expect(std.mem.indexOf(u8, v0, "\\\"nullCount\\\":{\\\"id\\\":0,\\\"s\\\":{\\\"k\\\":0,\\\"addr\\\":{\\\"city\\\":0},\\\"amt\\\":0}}") != null);

    try load(&tmp, "LOAD INTO '$B/t/' AS SELECT id + 10 AS id, s, l, m FROM '$B/t/' WHERE id = 1;", 1);
    try load(&tmp, "LOAD INTO '$B/t/' UPSERT ON (id) AS SELECT id, CAST('{\"k\":99}' AS STRUCT(k INT, addr STRUCT(city STRING), amt DECIMAL(10,2))) AS s, l, m FROM '$B/t/' WHERE id = 2;", 4);
    try testing.expectEqualStrings(
        "id,k,city,amt,l,ma\n1,1,a,10.00,\"[1,10]\",1\n2,99,,,\"[2,20]\",2\n3,3,b,5.00,\"[3,5]\",\n11,1,a,10.00,\"[1,10]\",1\n",
        try rows(a, &tmp, "SELECT id, s.k AS k, s.addr.city AS city, s.amt AS amt, l, m['a'] AS ma FROM '$B/t/' ORDER BY id"),
    );
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/t/' AS SELECT id, CAST('{\"k\":1}' AS STRUCT(k INT)) AS s, CAST(NULL AS LIST(INT)) AS l, CAST(NULL AS MAP(STRING, INT)) AS m FROM '$B/a.csv';", "column `s` is struct(k int) in the rows but struct(k int, addr struct(city string), amt decimal(10, 2)) in Delta table");
    try harness.expectRefusedAlike(gpa, &tmp, try std.fmt.allocPrint(a, "LOAD INTO '$B/p/' WITH (format = 'delta', partition_by = 's') AS SELECT id, {s} FROM '$B/a.csv';", .{nest}), "cannot be partitioned by `s`, a list, map or struct");
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/x/' WITH (format = 'delta') AS SELECT CAST('{\"t\":\"01:02:03\"}' AS STRUCT(t TIME)) AS s;", "holds a time of day");

    const fields = "{\\\"name\\\":\\\"s\\\",\\\"type\\\":{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{\\\"name\\\":\\\"i\\\",\\\"type\\\":\\\"integer\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"ts\\\",\\\"type\\\":\\\"timestamp\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}]},\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"l\\\",\\\"type\\\":{\\\"type\\\":\\\"array\\\",\\\"elementType\\\":\\\"short\\\",\\\"containsNull\\\":true},\\\"nullable\\\":true,\\\"metadata\\\":{}}";
    try tmp.dir.makePath("n/_delta_log");
    try tmp.dir.writeFile(.{ .sub_path = "n/_delta_log/00000000000000000000.json", .data = try std.fmt.allocPrint(a,
        \\{{"protocol":{{"minReaderVersion":1,"minWriterVersion":2}}}}
        \\{{"metaData":{{"id":"n","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{{\"type\":\"struct\",\"fields\":[{s}]}}","partitionColumns":[],"configuration":{{}},"createdTime":0}}}}
        \\
    , .{fields}) });
    try load(&tmp, "LOAD INTO '$B/n/' AS SELECT CAST('{\"i\":7,\"ts\":\"2026-01-02 03:04:05\"}' AS STRUCT(i INT, ts TIMESTAMP)) AS s, CAST('[1,-2]' AS LIST(INT)) AS l;", 1);
    try testing.expectEqualStrings("i,ts,l2\n7,2026-01-02 03:04:05,-2\n", try rows(a, &tmp, "SELECT s.i AS i, s.ts AS ts, l[2] AS l2 FROM '$B/n/'"));
    try testing.expect(std.mem.indexOf(u8, try logFile(a, &tmp, "n", 1), "\\\"maxValues\\\":{\\\"s\\\":{\\\"i\\\":7,\\\"ts\\\":\\\"2026-01-02T03:04:05.000Z\\\"}}") != null);
    try runRefuses(a, &tmp, "LOAD INTO '$B/n/' AS SELECT CAST('{\"i\":3000000000}' AS STRUCT(i INT, ts TIMESTAMP)) AS s, CAST('[1]' AS LIST(INT)) AS l;", "a number inside a list, map or struct past its `integer`");
    try runRefuses(a, &tmp, "LOAD INTO '$B/n/' AS SELECT CAST('{\"i\":3}' AS STRUCT(i INT, ts TIMESTAMP)) AS s, CAST('[40000]' AS LIST(INT)) AS l;", "past its `integer`, `short` or `byte`");
    try testing.expect(!exists(&tmp, "n/_delta_log/00000000000000000002.json"));
}

test "Delta checkpoint: a commit at the table's interval writes one, carrying another writer's txn, domain and tags and the live tombstones, and reads start there" {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try load(&tmp, "LOAD INTO '$B/t/' WITH (format = 'delta') AS SELECT id, grp, amt FROM '$B/a.csv';", 1);
    const v0 = try logFile(a, &tmp, "t", 0);
    var lines = std.mem.splitScalar(u8, v0, '\n');
    var meta: []const u8 = "";
    var add: []const u8 = "";
    while (lines.next()) |l| {
        if (std.mem.startsWith(u8, l, "{\"metaData\":")) meta = l;
        if (std.mem.startsWith(u8, l, "{\"add\":")) add = l;
    }
    const now = std.time.milliTimestamp();
    const v1 = try std.mem.join(a, "\n", &.{
        try std.mem.replaceOwned(u8, a, meta, "\"configuration\":{}", "\"configuration\":{\"delta.checkpointInterval\":\"2\"}"),
        "{\"txn\":{\"appId\":\"app\",\"version\":5,\"lastUpdated\":1}}",
        "{\"domainMetadata\":{\"domain\":\"d\",\"configuration\":\"{}\",\"removed\":false}}",
        try std.mem.replaceOwned(u8, a, add, "{\"add\":{", "{\"add\":{\"tags\":{\"t\":\"1\"},"),
        "{\"remove\":{\"path\":\"old.parquet\",\"deletionTimestamp\":1,\"dataChange\":true}}",
        try std.fmt.allocPrint(a, "{{\"remove\":{{\"path\":\"new.parquet\",\"deletionTimestamp\":{d},\"dataChange\":true}}}}", .{now}),
        "",
    });
    try tmp.dir.writeFile(.{ .sub_path = "t/_delta_log/00000000000000000001.json", .data = v1 });
    try load(&tmp, "LOAD INTO '$B/t/' AS SELECT id + 10 AS id, grp, amt FROM '$B/a.csv' WHERE id = 1;", 1);
    try testing.expect(exists(&tmp, "t/_delta_log/00000000000000000002.checkpoint.parquet"));
    try testing.expect(std.mem.startsWith(u8, try tmp.dir.readFileAlloc(a, "t/_delta_log/_last_checkpoint", 1 << 10), "{\"version\":2,\"size\":7,"));
    const cp = "'$B/t/_delta_log/00000000000000000002.checkpoint.parquet'";
    try testing.expectEqualStrings("a,v\napp,5\n", try rows(a, &tmp, try std.fmt.allocPrint(a, "SELECT txn.appId AS a, txn.version AS v FROM {s} WHERE txn.appId IS NOT NULL", .{cp})));
    try testing.expectEqualStrings("d\nd\n", try rows(a, &tmp, try std.fmt.allocPrint(a, "SELECT domainMetadata.domain AS d FROM {s} WHERE domainMetadata.domain IS NOT NULL", .{cp})));
    try testing.expectEqualStrings("t,n\n1,2\n", try rows(a, &tmp, try std.fmt.allocPrint(a, "SELECT max(add.tags['t']) AS t, count(add.path) AS n FROM {s}", .{cp})));
    try testing.expectEqualStrings("p,c\nnew.parquet,false\n", try rows(a, &tmp, try std.fmt.allocPrint(a, "SELECT remove.path AS p, remove.dataChange AS c FROM {s} WHERE remove.path IS NOT NULL", .{cp})));
    try testing.expectEqualStrings("r,w\n1,2\n", try rows(a, &tmp, try std.fmt.allocPrint(a, "SELECT protocol.minReaderVersion AS r, protocol.minWriterVersion AS w FROM {s} WHERE protocol.minReaderVersion IS NOT NULL", .{cp})));
    try tmp.dir.deleteFile("t/_delta_log/00000000000000000000.json");
    try tmp.dir.deleteFile("t/_delta_log/00000000000000000001.json");
    try testing.expectEqualStrings("id,grp,amt\n1,a,10\n2,a,20\n3,b,5\n11,a,10\n", try rows(a, &tmp, "SELECT * FROM '$B/t/' ORDER BY id"));
    try load(&tmp, "LOAD INTO '$B/t/' AS SELECT id + 20 AS id, grp, amt FROM '$B/a.csv' WHERE id = 1;", 1);
    try testing.expect(!exists(&tmp, "t/_delta_log/00000000000000000003.checkpoint.parquet"));
    try load(&tmp, "LOAD INTO '$B/t/' AS SELECT id + 30 AS id, grp, amt FROM '$B/a.csv' WHERE id = 1;", 1);
    try testing.expect(std.mem.startsWith(u8, try tmp.dir.readFileAlloc(a, "t/_delta_log/_last_checkpoint", 1 << 10), "{\"version\":4,"));
    try testing.expectEqualStrings("n\n6\n", try rows(a, &tmp, "SELECT COUNT(*) AS n FROM '$B/t/'"));
    const cp4 = "'$B/t/_delta_log/00000000000000000004.checkpoint.parquet'";
    try testing.expectEqualStrings("a,t,n,d\napp,1,4,d\n", try rows(a, &tmp, try std.fmt.allocPrint(a, "SELECT max(txn.appId) AS a, max(add.tags['t']) AS t, count(add.path) AS n, max(domainMetadata.domain) AS d FROM {s}", .{cp4})));
}

test "Delta file skipping: a filter on a column or a struct's field leaves out the files whose statistics rule it out, unopened, on one lane or four" {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try load(&tmp, "LOAD INTO '$B/t/' WITH (format = 'delta') AS SELECT 1 AS id, CAST('{\"k\":1}' AS STRUCT(k INT)) AS s, 'p' AS tag;", 1);
    for ([_]u8{ 2, 3, 4 }) |i| try load(&tmp, try std.fmt.allocPrint(a, "LOAD INTO '$B/t/' AS SELECT {d}0 AS id, CAST('{{\"k\":{d}0}}' AS STRUCT(k INT)) AS s, CAST(NULL AS STRING) AS tag;", .{ i, i }), 1);
    const v1 = try logFile(a, &tmp, "t", 1);
    const at = std.mem.indexOf(u8, v1, "part-").?;
    const kept = v1[at .. at + std.mem.indexOfScalar(u8, v1[at..], '"').?];
    var dir = try tmp.dir.openDir("t", .{ .iterate = true });
    defer dir.close();
    var gone = std.array_list.Managed([]const u8).init(a);
    var it = dir.iterate();
    while (try it.next()) |e| if (e.kind == .file and !std.mem.eql(u8, e.name, kept)) try gone.append(try a.dupe(u8, e.name));
    for (gone.items) |n| try dir.deleteFile(n);
    try testing.expectEqual(@as(usize, 3), gone.items.len);
    for ([_]usize{ 1, 4 }) |threads| {
        for ([_][]const u8{ "s.k = 20", "id BETWEEN 15 AND 25", "id > 10 AND id < 30", "s.k >= 11 AND s.k <= 29" }) |w| {
            try load(&tmp, try std.fmt.allocPrint(a, "LOAD INTO '$B/out.csv' AS SELECT id FROM '$B/t/' WHERE {s};", .{w}), threads);
            try testing.expectEqualStrings("id\n20\n", try tmp.dir.readFileAlloc(a, "out.csv", 1 << 10));
        }
    }
    try load(&tmp, "LOAD INTO '$B/out.csv' AS SELECT COUNT(*) AS n FROM '$B/t/' WHERE tag = 'p' AND id = 20;", 1);
    try testing.expectEqualStrings("n\n0\n", try tmp.dir.readFileAlloc(a, "out.csv", 1 << 10));
    try runRefuses(a, &tmp, "LOAD INTO '$B/out.csv' AS SELECT id FROM '$B/t/' WHERE id >= 20;", "could not read its file");
}

test "Delta USING changes: each run applies the changes past the table's position, the latest per key, deletes too, once" {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const apply =
        \\LOAD INTO '$B/t/' WITH (format = 'delta', partition_by = 'region') UPSERT ON (id)
        \\  USING changes (sequence_by = seq, delete_when = op = 'd', position = lsn, name = 'orders_cdc')
        \\AS SELECT * FROM '$B/c.csv' WHERE lsn > coalesce($since, 0);
    ;
    const view = "SELECT * FROM '$B/t/' ORDER BY id";
    const batches = [_][]const u8{
        "id,name,region,op,seq,lsn\n1,a,eu,i,1,10\n2,b,us,i,2,11\n1,a2,eu,u,3,12\n",
        "2,b,us,d,4,13\n3,c,eu,i,5,14\n9,z,us,d,6,15\n",
        "1,a3,us,u,8,16\n3,c,eu,d,7,16\n4,d,eu,i,9,17\n",
    };
    const want = [_][]const u8{
        "id,name,region\n1,a2,eu\n2,b,us\n",
        "id,name,region\n1,a2,eu\n3,c,eu\n",
        "id,name,region\n1,a3,us\n4,d,eu\n",
    };
    var text = std.array_list.Managed(u8).init(a);
    for (batches, want, 0..) |b, w, i| {
        try text.appendSlice(b);
        try tmp.dir.writeFile(.{ .sub_path = "c.csv", .data = text.items });
        try load(&tmp, apply, if (i == 1) 4 else 1);
        try testing.expectEqualStrings(w, try rows(a, &tmp, view));
        try load(&tmp, apply, 4);
        try testing.expectEqualStrings(w, try rows(a, &tmp, view));
        try testing.expect(!exists(&tmp, try std.fmt.allocPrint(a, "t/_delta_log/{d:0>20}.json", .{i + 1})));
    }
    try testing.expect(std.mem.indexOf(u8, try logFile(a, &tmp, "t", 2), "{\"txn\":{\"appId\":\"orders_cdc\",\"version\":17,") != null);
    try testing.expectEqual(@as(usize, 2), try actions(a, &tmp, "t", 2, "remove"));

    try tmp.dir.writeFile(.{ .sub_path = "c.csv", .data = "id,name,region,op,seq,lsn\n4,d2,eu,u,10,18\n" });
    try load(&tmp, "LOAD INTO '$B/t/' UPSERT ON (id) PARTIAL COLS (name) USING changes (sequence_by = seq, delete_when = op = 'd', position = lsn, name = 'orders_cdc') AS SELECT * FROM '$B/c.csv' WHERE lsn > $since;", 1);
    try testing.expectEqualStrings("id,name,region\n1,a3,us\n4,d2,eu\n", try rows(a, &tmp, view));

    try tmp.dir.writeFile(.{ .sub_path = "c.csv", .data = "id,name,region,op,seq,lsn\n5,e,eu,i,1,30\n5,f,eu,i,1,31\n" });
    try runRefuses(a, &tmp, apply, "twice at sequence 1");
    try tmp.dir.writeFile(.{ .sub_path = "c.csv", .data = "id,name,region,op,seq,lsn\n5,e,eu,i,1,7\n" });
    try runRefuses(a, &tmp, "LOAD INTO '$B/t/' UPSERT ON (id) USING changes (sequence_by = seq, delete_when = op = 'd', position = lsn, name = 'orders_cdc') AS SELECT * FROM '$B/c.csv';", "behind the 18 it holds for `orders_cdc`");
    try testing.expect(!exists(&tmp, "t/_delta_log/00000000000000000005.json"));
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/t/' APPEND USING changes (sequence_by = seq, position = lsn) AS SELECT * FROM '$B/c.csv';", "write it with `UPSERT ON (<key>)`");
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/x.csv' USING changes (sequence_by = seq, position = lsn) AS SELECT * FROM '$B/c.csv';", "applies a change feed to a Delta table");
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/t/' UPSERT ON (id) USING changes (sequence_by = seq, delete_when = op = 'd', position = name) AS SELECT id, name, region, op, seq FROM '$B/c.csv';", "takes an INT, DATE or TIMESTAMP");
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/t/' UPSERT ON (id) USING changes (sequence_by = seq, delete_when = seq, position = lsn) AS SELECT id, name, region, seq, lsn FROM '$B/c.csv';", "not a condition");

    try tmp.dir.writeFile(.{ .sub_path = "c.csv", .data = "id,name,region,op,seq,lsn\n6,g,eu,i,1,40\n" });
    for (5..11) |_| try load(&tmp, "LOAD INTO '$B/t/' UPSERT ON (id) USING changes (sequence_by = seq, delete_when = op = 'd', position = lsn, name = 'other') AS SELECT * FROM '$B/c.csv' WHERE lsn > coalesce($since, 0) OR TRUE;", 1);
    try load(&tmp, "LOAD INTO '$B/t/' APPEND AS SELECT 7 AS id, 'h' AS name, 'eu' AS region;", 1);
    try testing.expect(exists(&tmp, "t/_delta_log/00000000000000000010.checkpoint.parquet"));
    try tmp.dir.writeFile(.{ .sub_path = "c.csv", .data = "id,name,region,op,seq,lsn\n8,i,eu,i,1,18\n8,j,eu,i,2,19\n" });
    try load(&tmp, apply, 1);
    try testing.expectEqualStrings("id,name\n8,j\n", try rows(a, &tmp, "SELECT id, name FROM '$B/t/' WHERE id = 8"));
}

test "Delta schema evolution: merge_schema adds the rows' new columns and struct fields and writes NULL for missing ones; overwrite_schema replaces it" {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try load(&tmp, "LOAD INTO '$B/t/' WITH (format = 'delta', partition_by = 'grp') AS SELECT id, grp, CAST(json_object('x', id) AS STRUCT(x INT)) AS s FROM '$B/a.csv';", 1);
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/t/' AS SELECT id, grp, CAST(json_object('x', id) AS STRUCT(x INT)) AS s, amt FROM '$B/a.csv';", "`WITH (merge_schema = true)` adds it");
    try load(&tmp, "LOAD INTO '$B/t/' WITH (merge_schema = true) AS SELECT id + 10 AS id, grp, CAST(json_object('x', id) AS STRUCT(x INT)) AS s, amt FROM '$B/a.csv' WHERE id = 1;", 1);
    try load(&tmp, "LOAD INTO '$B/t/' WITH (merge_schema = true) AS SELECT id + 20 AS id, grp, CAST(json_object('y', grp, 'x', id) AS STRUCT(y STRING, x INT)) AS s FROM '$B/a.csv' WHERE id = 2;", 4);
    try load(&tmp, "LOAD INTO '$B/t/' UPSERT ON (id) PARTIAL COLS (tag) WITH (merge_schema = true) AS SELECT 3 AS id, 'b' AS grp, 'hot' AS tag;", 1);
    try testing.expectEqualStrings(
        "id,grp,s,amt,tag\n1,a,\"{\"\"x\"\":1,\"\"y\"\":null}\",,\n2,a,\"{\"\"x\"\":2,\"\"y\"\":null}\",,\n3,b,\"{\"\"x\"\":3,\"\"y\"\":null}\",,hot\n11,a,\"{\"\"x\"\":1,\"\"y\"\":null}\",10,\n22,a,\"{\"\"x\"\":2,\"\"y\"\":\"\"a\"\"}\",,\n",
        try rows(a, &tmp, "SELECT * FROM '$B/t/' ORDER BY id"),
    );
    const v1 = try logFile(a, &tmp, "t", 1);
    const v0 = try logFile(a, &tmp, "t", 0);
    const id_at = std.mem.indexOf(u8, v0, "{\"metaData\":{\"id\":\"").? + 19;
    try testing.expect(std.mem.indexOf(u8, v1, v0[id_at .. id_at + 36]) != null);
    try testing.expect(std.mem.indexOf(u8, v1, "\\\"name\\\":\\\"amt\\\",\\\"type\\\":\\\"long\\\",\\\"nullable\\\":true") != null);
    try testing.expect(std.mem.indexOf(u8, try logFile(a, &tmp, "t", 2), "\\\"name\\\":\\\"y\\\",\\\"type\\\":\\\"string\\\"") != null);
    try testing.expect(std.mem.indexOf(u8, try logFile(a, &tmp, "t", 3), "\\\"name\\\":\\\"tag\\\"") != null);
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/t/' WITH (merge_schema = true) AS SELECT 5 AS id;", "partitioned by `grp`, which the rows lack");
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/t/' WITH (merge_schema = true) AS SELECT 'x' AS id, grp FROM '$B/a.csv';", "column `id` is string in the rows but `long`");
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/t/' WITH (overwrite_schema = true) AS SELECT 1 AS k;", "which only a REPLACE may");
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/t/' REPLACE WITH (merge_schema = true, overwrite_schema = true) AS SELECT 1 AS k;", "give one");

    try load(&tmp, "LOAD INTO '$B/t/' REPLACE WITH (overwrite_schema = true) AS SELECT 'k' AS key, 1.5 AS v, grp FROM '$B/a.csv' WHERE id = 3;", 1);
    try testing.expectEqualStrings("key,v,grp\nk,1.5,b\n", try rows(a, &tmp, "SELECT * FROM '$B/t/'"));
    try testing.expect(exists(&tmp, "t/grp=b"));
    try load(&tmp, "LOAD INTO '$B/t/' REPLACE WITH (overwrite_schema = true, partition_by = 'key') AS SELECT 'k2' AS key, 7 AS n;", 1);
    try testing.expectEqualStrings("key,n\nk2,7\n", try rows(a, &tmp, "SELECT * FROM '$B/t/'"));
    try testing.expect(std.mem.indexOf(u8, try logFile(a, &tmp, "t", 5), "\"partitionColumns\":[\"key\"]") != null);
    try testing.expectEqualStrings("n\n5\n", try rows(a, &tmp, "SELECT COUNT(*) AS n FROM '$B/t/' WITH (version = 3)"));

    try load(&tmp, "LOAD INTO '$B/l/' WITH (format = 'delta') AS SELECT 1 AS id, CAST('[{\"a\":1}]' AS LIST(STRUCT(a INT))) AS l;", 1);
    try load(&tmp, "LOAD INTO '$B/l/' WITH (merge_schema = true) AS SELECT 2 AS id, CAST('[{\"b\":\"z\",\"a\":2},{\"a\":3}]' AS LIST(STRUCT(b STRING, a INT))) AS l;", 1);
    try load(&tmp, "LOAD INTO '$B/out.csv' AS SELECT id, l FROM '$B/l/' ORDER BY id;", 4);
    try testing.expectEqualStrings("id,l\n1,\"[{\"\"a\"\":1,\"\"b\"\":null}]\"\n2,\"[{\"\"a\"\":2,\"\"b\"\":\"\"z\"\"},{\"\"a\"\":3,\"\"b\"\":null}]\"\n", try tmp.dir.readFileAlloc(a, "out.csv", 1 << 10));
}

test "Delta log cleanup: a checkpoint past the log's retention deletes the commits and checkpoints below it; off, or within retention, keeps them" {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_][]const u8{ "keep", "zero", "off" }) |root| {
        try load(&tmp, try std.fmt.allocPrint(a, "LOAD INTO '$B/{s}/' WITH (format = 'delta') AS SELECT 0 AS id;", .{root}), 1);
        const v0 = try logFile(a, &tmp, root, 0);
        var meta: []const u8 = "";
        var lines = std.mem.splitScalar(u8, v0, '\n');
        while (lines.next()) |l| if (std.mem.startsWith(u8, l, "{\"metaData\":")) {
            meta = l;
        };
        const conf = if (std.mem.eql(u8, root, "zero"))
            "{\"delta.logRetentionDuration\":\"interval 0 seconds\"}"
        else if (std.mem.eql(u8, root, "off"))
            "{\"delta.logRetentionDuration\":\"interval 0 seconds\",\"delta.enableExpiredLogCleanup\":\"false\"}"
        else
            "{}";
        const v1 = try std.mem.concat(a, u8, &.{ "{\"commitInfo\":{\"timestamp\":1}}\n", try std.mem.replaceOwned(u8, a, meta, "\"configuration\":{}", try std.fmt.allocPrint(a, "\"configuration\":{s}", .{conf})), "\n" });
        try tmp.dir.writeFile(.{ .sub_path = try std.fmt.allocPrint(a, "{s}/_delta_log/00000000000000000001.json", .{root}), .data = v1 });
        for (2..21) |i| try load(&tmp, try std.fmt.allocPrint(a, "LOAD INTO '$B/{s}/' AS SELECT {d} AS id;", .{ root, i }), 1);
        try testing.expectEqualStrings("n,s\n20,209\n", try rows(a, &tmp, try std.fmt.allocPrint(a, "SELECT COUNT(*) AS n, SUM(id) AS s FROM '$B/{s}/'", .{root})));
    }
    try testing.expect(exists(&tmp, "keep/_delta_log/00000000000000000000.json"));
    try testing.expect(exists(&tmp, "keep/_delta_log/00000000000000000010.checkpoint.parquet"));
    try testing.expect(exists(&tmp, "off/_delta_log/00000000000000000000.json"));
    for ([_]u8{ 0, 1, 9, 10, 19 }) |v| try testing.expect(!exists(&tmp, try std.fmt.allocPrint(a, "zero/_delta_log/{d:0>20}.json", .{v})));
    try testing.expect(!exists(&tmp, "zero/_delta_log/00000000000000000010.checkpoint.parquet"));
    try testing.expect(exists(&tmp, "zero/_delta_log/00000000000000000020.checkpoint.parquet"));
    try testing.expect(exists(&tmp, "zero/_delta_log/00000000000000000020.json"));
    try testing.expectEqualStrings("n\n20\n", try rows(a, &tmp, "SELECT COUNT(*) AS n FROM '$B/zero/' WITH (version = 20)"));
    try runRefuses(a, &tmp, "LOAD INTO '$B/out.csv' AS SELECT COUNT(*) AS n FROM '$B/zero/' WITH (version = 12);", "versions 20 to 20 can be read");
}

test "delta_vacuum deletes the files no version within the retention reads; delta_optimize compacts a partition's small files" {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try load(&tmp, "LOAD INTO '$B/t/' WITH (format = 'delta', partition_by = 'grp') AS SELECT id, grp, amt FROM '$B/a.csv';", 1);
    for (0..3) |i| try load(&tmp, try std.fmt.allocPrint(a, "LOAD INTO '$B/t/' AS SELECT id + {d} AS id, grp, amt FROM '$B/a.csv';", .{(i + 1) * 10}), 1);
    try load(&tmp, "CALL delta_optimize('$B/t/', 'grp = ''a''');", 1);
    const v4 = try logFile(a, &tmp, "t", 4);
    try testing.expect(std.mem.indexOf(u8, v4, "\"operation\":\"OPTIMIZE\"") != null);
    try testing.expectEqual(@as(usize, 4), std.mem.count(u8, v4, "{\"remove\":"));
    try testing.expectEqual(@as(usize, 1), std.mem.count(u8, v4, "{\"add\":"));
    try load(&tmp, "CALL delta_optimize('$B/t');", 4);
    try testing.expectEqual(@as(usize, 4), try actions(a, &tmp, "t", 5, "remove"));
    try load(&tmp, "CALL delta_optimize('$B/t/');", 1);
    try testing.expect(!exists(&tmp, "t/_delta_log/00000000000000000006.json"));
    const all = "SELECT grp, COUNT(*) AS n, SUM(id) AS s, SUM(amt) AS m FROM '$B/t/' GROUP BY grp ORDER BY grp";
    const want = "grp,n,s,m\na,8,132,120\nb,4,72,20\n";
    try testing.expectEqualStrings(want, try rows(a, &tmp, all));
    try runRefuses(a, &tmp, "CALL delta_optimize('$B/t/', 'id > 2');", "may name only");
    try harness.expectRefusedAlike(gpa, &tmp, "CALL delta_vacuum('$B/t/', 1, 2);", "takes 1 to 2 arguments");
    try harness.expectRefusedAlike(gpa, &tmp, "CALL delta_nope('$B/t/');", "no function or procedure named `delta_nope`");

    try runRefuses(a, &tmp, "CALL delta_vacuum('$B/t/', 0);", "less than the table's `delta.deletedFileRetentionDuration` of 168 hours");
    try load(&tmp, "CALL delta_vacuum('$B/t/');", 1);
    try testing.expectEqual(@as(usize, 10), try dataFiles(a, &tmp));

    const v0 = try logFile(a, &tmp, "t", 0);
    var meta: []const u8 = "";
    var lines = std.mem.splitScalar(u8, v0, '\n');
    while (lines.next()) |l| if (std.mem.startsWith(u8, l, "{\"metaData\":")) {
        meta = l;
    };
    try tmp.dir.writeFile(.{ .sub_path = "t/_delta_log/00000000000000000006.json", .data = try std.mem.concat(a, u8, &.{ "{\"commitInfo\":{\"timestamp\":1}}\n", try std.mem.replaceOwned(u8, a, meta, "\"configuration\":{}", "\"configuration\":{\"delta.deletedFileRetentionDuration\":\"interval 0 hours\"}"), "\n" }) });
    try tmp.dir.writeFile(.{ .sub_path = "t/stray.parquet", .data = "x" });
    try tmp.dir.makePath("t/_scratch");
    try tmp.dir.writeFile(.{ .sub_path = "t/_scratch/x.parquet", .data = "x" });
    try load(&tmp, "CALL delta_vacuum('$B/t/');", 1);
    try testing.expectEqual(@as(usize, 2), try dataFiles(a, &tmp));
    try testing.expect(exists(&tmp, "t/_scratch/x.parquet"));
    try testing.expect(!exists(&tmp, "t/stray.parquet"));
    try testing.expectEqualStrings(want, try rows(a, &tmp, all));
}

/// The Parquet files of table `t` outside its hidden folders.
fn dataFiles(a: std.mem.Allocator, tmp: *testing.TmpDir) !usize {
    var dir = try tmp.dir.openDir("t", .{ .iterate = true });
    defer dir.close();
    var w = try dir.walk(a);
    var n: usize = 0;
    while (try w.next()) |e| if (e.kind == .file and std.mem.endsWith(u8, e.path, ".parquet") and !std.mem.startsWith(u8, e.path, "_")) {
        n += 1;
    };
    return n;
}

test "Delta time travel by timestamp: the newest commit at or before the time, by its commitInfo; outside the log's times refused" {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try load(&tmp, "LOAD INTO '$B/t/' WITH (format = 'delta') AS SELECT 1 AS id;", 1);
    try load(&tmp, "LOAD INTO '$B/t/' AS SELECT 2 AS id;", 1);
    try load(&tmp, "LOAD INTO '$B/t/' AS SELECT 3 AS id;", 1);
    for (0..3) |v| {
        const path = try std.fmt.allocPrint(a, "t/_delta_log/{d:0>20}.json", .{v});
        const text = try tmp.dir.readFileAlloc(a, path, 1 << 16);
        const at = std.mem.indexOf(u8, text, "{\"commitInfo\":{\"timestamp\":").? + 27;
        const end = at + std.mem.indexOfScalar(u8, text[at..], ',').?;
        try tmp.dir.writeFile(.{ .sub_path = path, .data = try std.mem.concat(a, u8, &.{ text[0..at], try std.fmt.allocPrint(a, "{d}", .{(v + 1) * 1000}), text[end..] }) });
    }
    const q = "SELECT COUNT(*) AS n FROM '$B/t/' WITH (timestamp = '{s}')";
    for ([_][2][]const u8{ .{ "1970-01-01 00:00:01", "n\n1\n" }, .{ "1970-01-01 00:00:01.999", "n\n1\n" }, .{ "1970-01-01 00:00:02", "n\n2\n" }, .{ "1970-01-01 00:00:03", "n\n3\n" } }) |c|
        try testing.expectEqualStrings(c[1], try rows(a, &tmp, try std.fmt.allocPrint(a, q, .{c[0]})));
    try runRefuses(a, &tmp, try std.fmt.allocPrint(a, "LOAD INTO '$B/out.csv' AS {s};", .{try std.fmt.allocPrint(a, q, .{"1970-01-01 00:00:00.5"})}), "its oldest readable commit, version 0, is from 1970-01-01 00:00:01.000 UTC");
    try runRefuses(a, &tmp, try std.fmt.allocPrint(a, "LOAD INTO '$B/out.csv' AS {s};", .{try std.fmt.allocPrint(a, q, .{"1970-01-01 00:00:04"})}), "its newest commit, version 2, is from 1970-01-01 00:00:03.000 UTC");
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/out.csv' AS SELECT id FROM '$B/t/' WITH (timestamp = 'soon');", "is not a date or timestamp");
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/out.csv' AS SELECT id FROM '$B/t/' WITH (timestamp = '1970-01-01', version = 1);", "give one");
}

test "Delta write: what is refused, at check where it can be seen" {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try load(&tmp, "LOAD INTO '$B/t/' WITH (format = 'delta') AS SELECT * FROM '$B/a.csv';", 1);
    try tmp.dir.makePath("plain");
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/plain/' AS SELECT id FROM '$B/a.csv';", "holds no _delta_log/, so it is no Delta table; `WITH (format = 'delta')` creates one");
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/plain/' WITH (format = 'parquet') AS SELECT id FROM '$B/a.csv';", "a folder is written as a Delta table");
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/x/' WITH (format = 'delta') AS SELECT CAST('01:02:03' AS TIME) AS t, id FROM '$B/a.csv';", "a time of day, which Delta has no type for");
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/x/' WITH (format = 'delta', partition_by = 'id, grp, amt') AS SELECT * FROM '$B/a.csv';", "partitioned by every column");
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/x/' WITH (format = 'delta', partition_by = 'nope') AS SELECT * FROM '$B/a.csv';", "has no column `nope` to partition by");
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO 'sftp://host/t/' WITH (format = 'delta') AS SELECT * FROM '$B/a.csv';", "SFTP and SMB have no create-if-absent write");
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/x/' WITH (format = 'delta', delimiter = ';') AS SELECT * FROM '$B/a.csv';", "does not apply to a Delta table, which takes format, partition_by, compression");
    try testing.expect(!exists(&tmp, "x"));
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/t/' UPSERT ON (nope) AS SELECT * FROM '$B/a.csv';", "has no column `nope` to match an UPSERT's rows on");
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/t/' UPSERT ON (id) PARTIAL COLS (id) AS SELECT * FROM '$B/a.csv';", "`PARTIAL COLS` names key column `id`");
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/t/' UPSERT ON (id) PARTIAL COLS (amt) AS SELECT id, grp FROM '$B/a.csv';", "`PARTIAL COLS` names `amt`, which the rows lack");
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/t/' UPSERT ON (id) AS SELECT id, grp FROM '$B/a.csv';", "has column `amt`, which the rows lack");
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/t/' UPSERT ON (grp) PARTIAL COLS (amt) AS SELECT id, amt FROM '$B/a.csv';", "the rows lack key column `grp`");
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/t/' UPSERT AS SELECT * FROM '$B/a.csv';", "infers the primary key from the source");

    const v0 = try logFile(a, &tmp, "t", 0);
    const featured = try std.mem.replaceOwned(u8, a, v0, "{\"minReaderVersion\":1,\"minWriterVersion\":2}", "{\"minReaderVersion\":1,\"minWriterVersion\":7,\"writerFeatures\":[\"appendOnly\",\"rowTracking\"]}");
    try tmp.dir.makePath("f/_delta_log");
    try tmp.dir.writeFile(.{ .sub_path = "f/_delta_log/00000000000000000000.json", .data = featured });
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/f/' AS SELECT * FROM '$B/a.csv';", "uses writer feature `rowTracking`, which basalt does not honour");

    const append_only = try std.mem.replaceOwned(u8, a, v0, "\"configuration\":{}", "\"configuration\":{\"delta.appendOnly\":\"true\"}");
    try tmp.dir.makePath("ao/_delta_log");
    try tmp.dir.writeFile(.{ .sub_path = "ao/_delta_log/00000000000000000000.json", .data = append_only });
    try load(&tmp, "LOAD INTO '$B/ao/' AS SELECT * FROM '$B/a.csv';", 1);
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/ao/' REPLACE AS SELECT * FROM '$B/a.csv';", "is append-only");
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/ao/' UPSERT ON (id) AS SELECT * FROM '$B/a.csv';", "so an UPSERT cannot remove its files");

    const cdf = try std.mem.replaceOwned(u8, a, v0, "\"configuration\":{}", "\"configuration\":{\"delta.enableChangeDataFeed\":\"true\"}");
    try tmp.dir.makePath("cdf/_delta_log");
    try tmp.dir.writeFile(.{ .sub_path = "cdf/_delta_log/00000000000000000000.json", .data = cdf });
    try load(&tmp, "LOAD INTO '$B/cdf/' AS SELECT * FROM '$B/a.csv';", 1);
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/cdf/' UPSERT ON (id) AS SELECT * FROM '$B/a.csv';", "records a change data feed");
}

/// The number of `action` lines in version `v` of the table at `root`.
fn actions(a: std.mem.Allocator, tmp: *testing.TmpDir, root: []const u8, v: u64, action: []const u8) !usize {
    return std.mem.count(u8, try logFile(a, tmp, root, v), try std.fmt.allocPrint(a, "{{\"{s}\":", .{action}));
}

test "Delta UPSERT: insert-only, update-only and mixed, each a MERGE commit removing only the files it changed" {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try load(&tmp, "LOAD INTO '$B/t/' WITH (format = 'delta') AS SELECT * FROM '$B/a.csv';", 1);
    try load(&tmp, "LOAD INTO '$B/t/' AS SELECT * FROM '$B/b.csv';", 1);

    try load(&tmp, "LOAD INTO '$B/t/' UPSERT ON (id) AS SELECT id + 100 AS id, grp, amt FROM '$B/a.csv';", 1);
    try testing.expectEqual(@as(usize, 0), try actions(a, &tmp, "t", 2, "remove"));
    try testing.expect(std.mem.indexOf(u8, try logFile(a, &tmp, "t", 2), "\"operation\":\"MERGE\"") != null);

    try load(&tmp, "LOAD INTO '$B/t/' UPSERT ON (id) AS SELECT id, 'y' AS grp, amt * 10 AS amt FROM '$B/b.csv';", 1);
    try testing.expectEqual(@as(usize, 1), try actions(a, &tmp, "t", 3, "remove"));
    try testing.expectEqual(@as(usize, 1), try actions(a, &tmp, "t", 3, "add"));

    try load(&tmp, "LOAD INTO '$B/t/' UPSERT ON (id) AS SELECT id * 2 AS id, 'z' AS grp, amt FROM '$B/a.csv';", 1);
    try testing.expectEqual(@as(usize, 1), try actions(a, &tmp, "t", 4, "remove"));

    const want = [_][]const u8{
        "id,grp,amt\n1,a,10\n2,a,20\n3,b,5\n",
        "id,grp,amt\n1,a,10\n2,a,20\n3,b,5\n7,x,1\n8,x,2\n",
        "id,grp,amt\n1,a,10\n2,a,20\n3,b,5\n7,x,1\n8,x,2\n101,a,10\n102,a,20\n103,b,5\n",
        "id,grp,amt\n1,a,10\n2,a,20\n3,b,5\n7,y,10\n8,y,20\n101,a,10\n102,a,20\n103,b,5\n",
        "id,grp,amt\n1,a,10\n2,z,10\n3,b,5\n4,z,20\n6,z,5\n7,y,10\n8,y,20\n101,a,10\n102,a,20\n103,b,5\n",
    };
    for (want, 0..) |w, v| try testing.expectEqualStrings(w, try rows(a, &tmp, try std.fmt.allocPrint(a, "SELECT * FROM '$B/t/' WITH (version = {d}) ORDER BY id", .{v})));
    try testing.expect(!exists(&tmp, "t/_delta_log/00000000000000000005.json"));
}

test "Delta UPSERT: PARTIAL COLS changes only the listed columns; an insert leaves the rest NULL, refused where NOT NULL" {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try load(&tmp, "LOAD INTO '$B/t/' WITH (format = 'delta') AS SELECT * FROM '$B/a.csv';", 1);
    try load(&tmp, "LOAD INTO '$B/t/' UPSERT ON (id) PARTIAL COLS (amt) AS SELECT id, 'ignored' AS grp, amt + 1 AS amt FROM '$B/a.csv' WHERE id <> 3 UNION ALL SELECT 9 AS id, 'new' AS grp, 90 AS amt;", 1);
    try testing.expectEqualStrings("id,grp,amt\n1,a,11\n2,a,21\n3,b,5\n9,new,90\n", try rows(a, &tmp, "SELECT * FROM '$B/t/' ORDER BY id"));
    try load(&tmp, "LOAD INTO '$B/t/' UPSERT ON (id) PARTIAL COLS (grp) AS SELECT id, 'g' || id AS grp FROM '$B/b.csv' UNION ALL SELECT 1 AS id, 'one' AS grp;", 1);
    try testing.expectEqualStrings("id,grp,amt\n1,one,11\n2,a,21\n3,b,5\n7,g7,\n8,g8,\n9,new,90\n", try rows(a, &tmp, "SELECT * FROM '$B/t/' ORDER BY id"));

    try load(&tmp, "LOAD INTO '$B/nn/' WITH (format = 'delta') AS SELECT * FROM '$B/a.csv';", 1);
    const v0 = try logFile(a, &tmp, "nn", 0);
    const nn = try std.mem.replaceOwned(u8, a, v0, "\\\"name\\\":\\\"amt\\\",\\\"type\\\":\\\"long\\\",\\\"nullable\\\":true", "\\\"name\\\":\\\"amt\\\",\\\"type\\\":\\\"long\\\",\\\"nullable\\\":false");
    try testing.expect(!std.mem.eql(u8, nn, v0));
    try tmp.dir.writeFile(.{ .sub_path = "nn/_delta_log/00000000000000000000.json", .data = nn });
    try load(&tmp, "LOAD INTO '$B/nn/' UPSERT ON (id) PARTIAL COLS (grp) AS SELECT id, 'u' AS grp FROM '$B/a.csv';", 1);
    try runRefuses(a, &tmp, "LOAD INTO '$B/nn/' UPSERT ON (id) PARTIAL COLS (grp) AS SELECT 5 AS id, 'u' AS grp;", "column `amt` is NOT NULL");
    try testing.expectEqualStrings("id,grp,amt\n1,u,10\n2,u,20\n3,u,5\n", try rows(a, &tmp, "SELECT * FROM '$B/nn/' ORDER BY id"));
    try testing.expect(!exists(&tmp, "nn/_delta_log/00000000000000000002.json"));
}

test "Delta UPSERT: a repeated or NULL key fails naming it, commits nothing and leaves no data file" {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try load(&tmp, "LOAD INTO '$B/t/' WITH (format = 'delta') AS SELECT * FROM '$B/a.csv';", 1);
    try runRefuses(a, &tmp, "LOAD INTO '$B/t/' UPSERT ON (id) AS SELECT * FROM '$B/b.csv' UNION ALL SELECT * FROM '$B/b.csv';", "the rows hold key (id) = (7) more than once");
    try runRefuses(a, &tmp, "LOAD INTO '$B/t/' UPSERT ON (id, grp) AS SELECT 1 AS id, 'a' AS grp, 0 AS amt UNION ALL SELECT 1 AS id, 'a' AS grp, 1 AS amt;", "the rows hold key (id, grp) = (1, a) more than once");
    try runRefuses(a, &tmp, "LOAD INTO '$B/t/' UPSERT ON (id) AS SELECT CASE WHEN id = 2 THEN NULL ELSE id END AS id, grp, amt FROM '$B/a.csv';", "column `id` is a key of this UPSERT");
    try load(&tmp, "LOAD INTO '$B/t/' UPSERT ON (id, grp) AS SELECT 1 AS id, 'b' AS grp, 7 AS amt;", 1);
    try testing.expectEqualStrings("id,grp,amt\n1,a,10\n1,b,7\n2,a,20\n3,b,5\n", try rows(a, &tmp, "SELECT * FROM '$B/t/' ORDER BY id, grp"));
    var dir = try tmp.dir.openDir("t", .{ .iterate = true });
    defer dir.close();
    var it = dir.iterate();
    var files: usize = 0;
    while (try it.next()) |e| files += @intFromBool(std.mem.endsWith(u8, e.name, ".parquet"));
    try testing.expectEqual(@as(usize, 2), files);
}

test "Delta UPSERT: a live row with a NULL key stays; a table the UPSERT creates takes every row" {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try load(&tmp, "LOAD INTO '$B/c/' WITH (format = 'delta') UPSERT ON (id) AS SELECT * FROM '$B/a.csv';", 1);
    try testing.expect(std.mem.indexOf(u8, try logFile(a, &tmp, "c", 0), "{\"metaData\":") != null);
    try load(&tmp, "LOAD INTO '$B/c/' AS SELECT CAST(NULL AS BIGINT) AS id, 'n' AS grp, 0 AS amt;", 1);
    try load(&tmp, "LOAD INTO '$B/c/' UPSERT ON (id) AS SELECT id, 'u' AS grp, amt FROM '$B/a.csv' WHERE id = 1;", 1);
    try testing.expectEqualStrings("id,grp,amt\n1,u,10\n2,a,20\n3,b,5\n,n,0\n", try rows(a, &tmp, "SELECT * FROM '$B/c/' ORDER BY id NULLS LAST"));
}

test "Delta UPSERT: a partitioned table rewrites only the partitions holding a matched key; a row may move partition" {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try load(&tmp, "LOAD INTO '$B/p/' WITH (format = 'delta', partition_by = 'grp') AS SELECT * FROM '$B/a.csv' UNION ALL SELECT * FROM '$B/b.csv';", 1);
    try testing.expectEqual(@as(usize, 3), try actions(a, &tmp, "p", 0, "add"));
    try load(&tmp, "LOAD INTO '$B/p/' UPSERT ON (grp, id) AS SELECT 2 AS id, 'a' AS grp, 99 AS amt UNION ALL SELECT 4 AS id, 'a' AS grp, 4 AS amt;", 1);
    const v1 = try logFile(a, &tmp, "p", 1);
    try testing.expectEqual(@as(usize, 1), std.mem.count(u8, v1, "{\"remove\":"));
    try testing.expect(std.mem.indexOf(u8, v1, "{\"remove\":{\"path\":\"grp=a/") != null);
    try testing.expectEqualStrings("id,grp,amt\n1,a,10\n2,a,99\n3,b,5\n4,a,4\n7,x,1\n8,x,2\n", try rows(a, &tmp, "SELECT * FROM '$B/p/' ORDER BY id"));

    try load(&tmp, "LOAD INTO '$B/p/' UPSERT ON (id) AS SELECT 7 AS id, 'b' AS grp, 70 AS amt;", 1);
    const v2 = try logFile(a, &tmp, "p", 2);
    try testing.expectEqual(@as(usize, 1), std.mem.count(u8, v2, "{\"remove\":"));
    try testing.expect(std.mem.indexOf(u8, v2, "{\"remove\":{\"path\":\"grp=x/") != null);
    try testing.expectEqualStrings("id,grp,amt\n1,a,10\n2,a,99\n3,b,5\n4,a,4\n7,b,70\n8,x,2\n", try rows(a, &tmp, "SELECT * FROM '$B/p/' ORDER BY id"));
    try testing.expectEqualStrings("grp,n\na,3\nb,2\nx,1\n", try rows(a, &tmp, "SELECT grp, COUNT(*) AS n FROM '$B/p/' GROUP BY grp ORDER BY grp"));
    try testing.expectEqualStrings("id,grp,amt\n1,a,10\n2,a,20\n3,b,5\n7,x,1\n8,x,2\n", try rows(a, &tmp, "SELECT * FROM '$B/p/' WITH (version = 0) ORDER BY id"));
    try testing.expectEqualStrings("id,grp,amt\n1,a,10\n2,a,99\n3,b,5\n4,a,4\n7,x,1\n8,x,2\n", try rows(a, &tmp, "SELECT * FROM '$B/p/' WITH (version = 1) ORDER BY id"));
}

test "Delta UPSERT: past --op-memory the spool and the joins spill, with the rows a roomy run writes; lanes alike" {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var csv = std.array_list.Managed(u8).init(a);
    try csv.appendSlice("id,k,v\n");
    for (0..20000) |i| try csv.writer().print("{d},{d},v{d}\n", .{ i, i % 40, i });
    try tmp.dir.writeFile(.{ .sub_path = "many.csv", .data = csv.items });
    const base = try tmp.dir.realpathAlloc(a, ".");
    const script = try std.mem.replaceOwned(u8, a,
        \\LOAD INTO '$B/s/' UPSERT ON (id) AS SELECT id, k, 'u' || v AS v FROM '$B/many.csv' WHERE id % 3 = 0;
        \\LOAD INTO '$B/out.csv' AS SELECT COUNT(*) AS n, SUM(id) AS s, SUM(CASE WHEN v LIKE 'u%' THEN 1 ELSE 0 END) AS u FROM '$B/s/';
    , "$B", base);
    for ([_]usize{ 1, 4 }) |threads| {
        tmp.dir.deleteTree("s") catch {};
        try load(&tmp, "LOAD INTO '$B/s/' WITH (format = 'delta', partition_by = 'k') AS SELECT id, k, v FROM '$B/many.csv' WHERE id % 2 = 0;", 1);
        const out = try harness.runScriptOpts(gpa, &tmp, script, .{ .threads = threads, .op_memory = 64 << 10, .log = .{ .quiet = true } });
        defer gpa.free(out);
        try testing.expectEqualStrings("n,s,u\n13333,133316667,6667\n", out);
        try testing.expectEqual(@as(usize, 40), try actions(a, &tmp, "s", 1, "add"));
    }
}

/// A table at `root` made from a log of `fields` (the schemaString's, escaped).
fn handMade(a: std.mem.Allocator, tmp: *testing.TmpDir, root: []const u8, fields: []const u8) !void {
    try tmp.dir.makePath(try std.fmt.allocPrint(a, "{s}/_delta_log", .{root}));
    try tmp.dir.writeFile(.{ .sub_path = try std.fmt.allocPrint(a, "{s}/_delta_log/00000000000000000000.json", .{root}), .data = try std.fmt.allocPrint(a,
        \\{{"protocol":{{"minReaderVersion":1,"minWriterVersion":2}}}}
        \\{{"metaData":{{"id":"h","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{{\"type\":\"struct\",\"fields\":[{s}]}}","partitionColumns":[],"configuration":{{}},"createdTime":0}}}}
        \\
    , .{fields}) });
}

test "Delta UPSERT: a key of a 4-byte float column matches as the table stores it, and keys equal as floats repeat" {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try handMade(a, &tmp, "ft", "{\\\"name\\\":\\\"k\\\",\\\"type\\\":\\\"float\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"v\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}");
    try load(&tmp, "LOAD INTO '$B/ft/' AS SELECT CAST(0.1 AS DOUBLE) AS k, 'old' AS v UNION ALL SELECT CAST(2.5 AS DOUBLE) AS k, 'old2' AS v;", 1);
    try load(&tmp, "LOAD INTO '$B/ft/' UPSERT ON (k) AS SELECT CAST(0.1 AS DOUBLE) AS k, 'new' AS v UNION ALL SELECT CAST(2.5 AS DOUBLE) AS k, 'new2' AS v;", 1);
    try testing.expectEqualStrings("v\nnew\nnew2\n", try rows(a, &tmp, "SELECT v FROM '$B/ft/' ORDER BY v"));
    try load(&tmp, "LOAD INTO '$B/ft/' UPSERT ON (k) AS SELECT CAST(0.10000000149011612 AS DOUBLE) AS k, 'newer' AS v;", 1);
    try testing.expectEqualStrings("v\nnew2\nnewer\n", try rows(a, &tmp, "SELECT v FROM '$B/ft/' ORDER BY v"));
    try runRefuses(a, &tmp, "LOAD INTO '$B/ft/' UPSERT ON (k) AS SELECT CAST(0.1 AS DOUBLE) AS k, 'a' AS v UNION ALL SELECT CAST(0.100000001 AS DOUBLE) AS k, 'b' AS v;", "more than once");
}

test "Delta UPSERT: PARTIAL COLS leaving out a column with a default is refused, as basalt does not apply it" {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try handMade(a, &tmp, "dt", "{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"long\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"v\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"d\\\",\\\"type\\\":\\\"long\\\",\\\"nullable\\\":true,\\\"metadata\\\":{\\\"CURRENT_DEFAULT\\\":\\\"42\\\"}}");
    try load(&tmp, "LOAD INTO '$B/dt/' AS SELECT 1 AS id, 'a' AS v, 7 AS d;", 1);
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/dt/' UPSERT ON (id) PARTIAL COLS (v) AS SELECT 2 AS id, 'ins' AS v;", "column `d` has a default value (`CURRENT_DEFAULT`), which basalt does not apply");
    try load(&tmp, "LOAD INTO '$B/dt/' UPSERT ON (id) PARTIAL COLS (v) AS SELECT 2 AS id, 'ins' AS v, 9 AS d;", 1);
    try testing.expectEqualStrings("id,v,d\n1,a,7\n2,ins,9\n", try rows(a, &tmp, "SELECT * FROM '$B/dt/' ORDER BY id"));
}

test "Delta UPSERT: no rows into a folder with format = 'delta' still creates the table" {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try load(&tmp, "LOAD INTO '$B/eu/' WITH (format = 'delta') UPSERT ON (id) AS SELECT id, grp FROM '$B/a.csv' WHERE id > 99;", 1);
    try testing.expect(std.mem.indexOf(u8, try logFile(a, &tmp, "eu", 0), "{\"metaData\":") != null);
    try load(&tmp, "LOAD INTO '$B/eu/' AS SELECT id, grp FROM '$B/a.csv';", 1);
    try testing.expectEqualStrings("n\n3\n", try rows(a, &tmp, "SELECT COUNT(*) AS n FROM '$B/eu/'"));
}

test "Delta write: thousands of partitions in one sorted batch, one file each; doubles as compact partition text" {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var csv = std.array_list.Managed(u8).init(a);
    try csv.appendSlice("id\n");
    for (0..20000) |i| try csv.writer().print("{d}\n", .{i});
    try tmp.dir.writeFile(.{ .sub_path = "ids.csv", .data = csv.items });
    try load(&tmp, "LOAD INTO '$B/hp/' WITH (format = 'delta', partition_by = 'p') AS SELECT id, id % 2000 AS p FROM '$B/ids.csv' ORDER BY p;", 1);
    try testing.expectEqual(@as(usize, 2000), try actions(a, &tmp, "hp", 0, "add"));
    try testing.expectEqualStrings("n,s,ps\n20000,199990000,2000\n", try rows(a, &tmp, "SELECT COUNT(*) AS n, SUM(id) AS s, COUNT(DISTINCT p) AS ps FROM '$B/hp/'"));

    try load(&tmp, "LOAD INTO '$B/pf/' WITH (format = 'delta', partition_by = 'p') AS SELECT 1 AS id, CAST(1e300 AS DOUBLE) * 10 AS p UNION ALL SELECT 2 AS id, CAST(0.0000001 AS DOUBLE) AS p;", 1);
    try testing.expect(exists(&tmp, "pf/p=1e301"));
    try testing.expect(exists(&tmp, "pf/p=1e-7"));
    try testing.expectEqualStrings("id\n1\n", try rows(a, &tmp, "SELECT id FROM '$B/pf/' WHERE p > 1e300"));
}

test "Delta write: a failed run deletes its data files and the folders it made; a failed commit names the table and deletes them" {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var csv = std.array_list.Managed(u8).init(a);
    try csv.appendSlice("id,v\n");
    for (0..200000) |i| try csv.writer().print("{d},{d}\n", .{ i, i });
    try csv.appendSlice("200000,x\n");
    try tmp.dir.writeFile(.{ .sub_path = "bad.csv", .data = csv.items });
    try runRefuses(a, &tmp, "LOAD INTO '$B/fl/' WITH (format = 'delta', partition_by = 'p') AS SELECT id, id % 20 AS p, CAST(v AS INT) AS v FROM '$B/bad.csv';", "CAST");
    try testing.expect(!exists(&tmp, "fl"));

    if (@import("builtin").os.tag != .linux or std.os.linux.geteuid() == 0) return;
    try load(&tmp, "LOAD INTO '$B/ro/' WITH (format = 'delta') AS SELECT * FROM '$B/a.csv';", 1);
    var log = try tmp.dir.openDir("ro/_delta_log", .{ .iterate = true });
    defer log.close();
    try log.chmod(0o555);
    defer log.chmod(0o755) catch {};
    try runRefuses(a, &tmp, "LOAD INTO '$B/ro/' AS SELECT * FROM '$B/a.csv';", "could not commit version 1 to Delta table");
    try runRefuses(a, &tmp, "LOAD INTO '$B/ro/' AS SELECT * FROM '$B/a.csv';", "nothing was committed, and the 1 data file it wrote was deleted");
    var dir = try tmp.dir.openDir("ro", .{ .iterate = true });
    defer dir.close();
    var it = dir.iterate();
    var files: usize = 0;
    while (try it.next()) |e| files += @intFromBool(std.mem.endsWith(u8, e.name, ".parquet"));
    try testing.expectEqual(@as(usize, 1), files);
}

test "Delta write: a key or column NULL through a whole JSON or CSV file creates a string column; a bare NULL is refused" {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "n.json", .data = "[{\"id\":1,\"x\":null},{\"id\":2}]" });
    try tmp.dir.writeFile(.{ .sub_path = "n.csv", .data = "id,x\n1,\n2,\n" });
    for ([_][]const u8{ "json", "csv" }) |ext| {
        try load(&tmp, try std.fmt.allocPrint(a, "LOAD INTO '$B/{s}/' WITH (format = 'delta') AS SELECT * FROM '$B/n.{s}';", .{ ext, ext }), 1);
        const v0 = try logFile(a, &tmp, ext, 0);
        try testing.expect(std.mem.indexOf(u8, v0, "{\\\"name\\\":\\\"x\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true") != null);
        try testing.expectEqualStrings("id,x\n1,\n2,\n", try rows(a, &tmp, try std.fmt.allocPrint(a, "SELECT id, x FROM '$B/{s}/' ORDER BY id", .{ext})));
    }
    try runRefuses(a, &tmp, "LOAD INTO '$B/b/' WITH (format = 'delta') AS SELECT id, NULL AS n FROM '$B/a.csv';", "column `n` has no type (a bare NULL)");
}

test "Delta write: a column name that is not UTF-8 is refused, at create and at append" {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try load(&tmp, "LOAD INTO '$B/t/' WITH (format = 'delta') AS SELECT * FROM '$B/a.csv';", 1);
    try runRefuses(a, &tmp, "LOAD INTO '$B/n/' WITH (format = 'delta') AS SELECT id, grp AS \"s\xff\" FROM '$B/a.csv';", "column \"s\\xff\" has a name that is not UTF-8");
    try testing.expect(!exists(&tmp, "n/_delta_log"));
    try runRefuses(a, &tmp, "LOAD INTO '$B/t/' AS SELECT id, grp AS \"s\xff\", amt FROM '$B/a.csv';", "has a name that is not UTF-8");
}
