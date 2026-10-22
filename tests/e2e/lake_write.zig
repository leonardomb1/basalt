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
        const out = try harness.runScriptOpts(a, &tmp, script, .{ .threads = threads, .op_memory = 64 << 10, .log = .{ .quiet = true } });
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
