//! End-to-end reads of Delta Lake and Iceberg tables: data files written by basalt,
//! a Delta log written as text (a checkpoint as Parquet, flat as basalt writes it or
//! nested as delta-rs does), Iceberg metadata as text and its manifests through the
//! test Avro writer, plus tables delta-rs and pyiceberg wrote; each read checked by
//! `check` and `run` alike.

const std = @import("std");
const basalt = @import("basalt");
const analyze = basalt.analyze;
const parser = basalt.sql_parser;
const Diag = basalt.env.Diag;
const run = basalt.runtime.run;

const testing = std.testing;

const Ran = struct {
    err: ?anyerror = null,
    msg: []const u8 = "",
    check_msg: ?[]const u8 = null,
    out: []const u8 = "",
    summary: basalt.obs.Summary = .{ .run_id = 0 },
};

/// `script` (its `$B` the tmp dir) checked, then run on `threads` lanes; `out.csv` read back.
fn runIn(a: std.mem.Allocator, tmp: *std.testing.TmpDir, tmpl: []const u8, threads: usize) !Ran {
    const base = try tmp.dir.realpathAlloc(a, ".");
    const script = try std.mem.replaceOwned(u8, a, tmpl, "$B", base);
    var pd: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(a, script, &pd);
    var ran = Ran{};
    var adiag = analyze.Diag{};
    if (analyze.analyze(a, prog, &adiag)) |_| {} else |_| ran.check_msg = try a.dupe(u8, adiag.msg);
    var rdiag: Diag = .{};
    tmp.dir.deleteFile("out.csv") catch {};
    _ = run(testing.allocator, prog, .{ .threads = threads, .log = .{ .quiet = true }, .summary_out = &ran.summary }, &rdiag) catch |e| {
        ran.err = e;
        ran.msg = try a.dupe(u8, if (rdiag.msg.len > 0) rdiag.msg else basalt.eval.peekFailure(e) orelse @errorName(e));
        return ran;
    };
    ran.out = tmp.dir.readFileAlloc(a, "out.csv", 1 << 20) catch "";
    return ran;
}

/// The rows `query` returns, as CSV, after `check` accepted it.
fn rows(a: std.mem.Allocator, tmp: *std.testing.TmpDir, query: []const u8, threads: usize) ![]const u8 {
    const r = try runIn(a, tmp, try std.fmt.allocPrint(a, "LOAD INTO '$B/out.csv' AS {s};", .{query}), threads);
    if (r.check_msg) |m| {
        std.debug.print("check refused: {s}\n", .{m});
        return error.TestUnexpectedResult;
    }
    if (r.err) |e| {
        std.debug.print("run failed: {s}\n", .{r.msg});
        return e;
    }
    return r.out;
}

/// Both `check` and `run` refuse `query`, each naming `want`; `run_only` when the
/// refusal needs the data, which `check` reads only for a local table.
fn refused(a: std.mem.Allocator, tmp: *std.testing.TmpDir, query: []const u8, want: []const u8, run_only: bool) !void {
    const r = try runIn(a, tmp, try std.fmt.allocPrint(a, "LOAD INTO '$B/out.csv' AS {s};", .{query}), 1);
    if (r.err == null) {
        std.debug.print("run accepted: {s}\n", .{query});
        return error.TestUnexpectedResult;
    }
    if (std.mem.indexOf(u8, r.msg, want) == null) {
        std.debug.print("want `{s}` in run's `{s}`\n", .{ want, r.msg });
        return error.TestUnexpectedResult;
    }
    if (run_only) return;
    const m = r.check_msg orelse {
        std.debug.print("check accepted: {s}\n", .{query});
        return error.TestUnexpectedResult;
    };
    if (std.mem.indexOf(u8, m, want) == null) {
        std.debug.print("want `{s}` in check's `{s}`\n", .{ want, m });
        return error.TestUnexpectedResult;
    }
}

fn put(tmp: *std.testing.TmpDir, path: []const u8, data: []const u8) !void {
    if (std.fs.path.dirname(path)) |d| try tmp.dir.makePath(d);
    try tmp.dir.writeFile(.{ .sub_path = path, .data = data });
}

/// A Parquet data file at `path` holding `select` over the CSV `csv`.
fn dataFile(a: std.mem.Allocator, tmp: *std.testing.TmpDir, path: []const u8, csv: []const u8, select: []const u8) !void {
    if (std.fs.path.dirname(path)) |d| try tmp.dir.makePath(d);
    try tmp.dir.writeFile(.{ .sub_path = "src.csv", .data = csv });
    const r = try runIn(a, tmp, try std.fmt.allocPrint(a, "LOAD INTO '$B/{s}' AS SELECT {s} FROM '$B/src.csv';", .{ path, select }), 1);
    if (r.err) |e| {
        std.debug.print("writing {s}: {s}\n", .{ path, r.msg });
        return e;
    }
}

fn commit(a: std.mem.Allocator, tmp: *std.testing.TmpDir, root: []const u8, v: u64, lines: []const []const u8) !void {
    const path = try std.fmt.allocPrint(a, "{s}/_delta_log/{d:0>20}.json", .{ root, v });
    try put(tmp, path, try std.mem.join(a, "\n", lines));
}

/// A metaData action over `fields` (schemaString's field list, unescaped) and the
/// partition columns `parts` (a JSON array), with `config` its configuration.
fn meta(a: std.mem.Allocator, fields: []const u8, parts: []const u8, config: []const u8) ![]const u8 {
    const schema = try std.fmt.allocPrint(a, "{{\"type\":\"struct\",\"fields\":[{s}]}}", .{fields});
    const esc = try std.mem.replaceOwned(u8, a, schema, "\"", "\\\"");
    return std.fmt.allocPrint(a, "{{\"metaData\":{{\"id\":\"t\",\"format\":{{\"provider\":\"parquet\",\"options\":{{}}}},\"schemaString\":\"{s}\",\"partitionColumns\":{s},\"configuration\":{s},\"createdTime\":0}}}}", .{ esc, parts, config });
}

fn field(a: std.mem.Allocator, name: []const u8, ty: []const u8) ![]const u8 {
    return std.fmt.allocPrint(a, "{{\"name\":\"{s}\",\"type\":\"{s}\",\"nullable\":true,\"metadata\":{{}}}}", .{ name, ty });
}

fn add(a: std.mem.Allocator, path: []const u8, parts: []const u8) ![]const u8 {
    return std.fmt.allocPrint(a, "{{\"add\":{{\"path\":\"{s}\",\"partitionValues\":{s},\"modificationTime\":0,\"dataChange\":true}}}}", .{ path, parts });
}

fn remove(a: std.mem.Allocator, path: []const u8) ![]const u8 {
    return std.fmt.allocPrint(a, "{{\"remove\":{{\"path\":\"{s}\",\"deletionTimestamp\":0,\"dataChange\":true}}}}", .{path});
}

const protocol1 = "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}";
const commit_info = "{\"commitInfo\":{\"operation\":\"WRITE\",\"engineInfo\":\"basalt-test\"}}";

/// A table `t/` of two commits: three files added, then one removed and one added.
fn basicTable(a: std.mem.Allocator, tmp: *std.testing.TmpDir) !void {
    const sel = "CAST(id AS INT) AS id, name";
    try dataFile(a, tmp, "t/a.parquet", "id,name\n1,ann\n2,bob\n", sel);
    try dataFile(a, tmp, "t/b.parquet", "id,name\n3,cy\n4,dee\n", sel);
    try dataFile(a, tmp, "t/c.parquet", "id,name\n5,eve\n", sel);
    const fields = try std.mem.join(a, ",", &.{ try field(a, "id", "long"), try field(a, "name", "string") });
    try commit(a, tmp, "t", 0, &.{ commit_info, protocol1, try meta(a, fields, "[]", "{}"), try add(a, "a.parquet", "{}"), try add(a, "b.parquet", "{}") });
    try commit(a, tmp, "t", 1, &.{ commit_info, try remove(a, "a.parquet"), try add(a, "c.parquet", "{}"), "{\"txn\":{\"appId\":\"x\",\"version\":1}}", "{\"someFutureAction\":{\"k\":1}}" });
}

test "a Delta folder reads its live files: the removed file's rows are gone, format = 'parquet' still reads them all" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try basicTable(a, &tmp);

    try testing.expectEqualStrings("id,name\n3,cy\n4,dee\n5,eve\n", try rows(a, &tmp, "SELECT * FROM '$B/t/' ORDER BY id", 1));
    try testing.expectEqualStrings("id,name\n3,cy\n4,dee\n5,eve\n", try rows(a, &tmp, "SELECT * FROM '$B/t/' WITH (format = 'delta') ORDER BY id", 1));
    try testing.expectEqualStrings("n\n5\n", try rows(a, &tmp, "SELECT COUNT(*) AS n FROM '$B/t/' WITH (format = 'parquet')", 1));
    try testing.expectEqualStrings("id,name\n1,ann\n2,bob\n3,cy\n4,dee\n", try rows(a, &tmp, "SELECT * FROM '$B/t/' WITH (version = 0) ORDER BY id", 1));
    try testing.expectEqualStrings("n\n3\n", try rows(a, &tmp, "SELECT COUNT(*) AS n FROM '$B/t/'", 1));
    try testing.expectEqualStrings("name\ndee\n", try rows(a, &tmp, "SELECT name FROM '$B/t/' WHERE id = 4", 1));
}

test "a Delta table's lanes return what one lane does, and lanes run" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try basicTable(a, &tmp);
    for ([_][]const u8{
        "SELECT name, SUM(id) AS s FROM '$B/t/' GROUP BY name ORDER BY name",
        "SELECT id, name FROM '$B/t/' WHERE id > 3",
        "SELECT DISTINCT name FROM '$B/t/' WITH (format = 'delta', version = 1) ORDER BY name",
        "WITH old AS (SELECT * FROM '$B/t/' WITH (version = 0)) SELECT name, COUNT(*) AS c FROM old GROUP BY name ORDER BY name",
        "WITH raw AS (SELECT * FROM '$B/t/' WITH (format = 'parquet')) SELECT name, COUNT(*) AS c FROM raw GROUP BY name ORDER BY name",
    }, [_]usize{ 3, 2, 3, 4, 5 }) |q, n| {
        const one = try rows(a, &tmp, q, 1);
        const r = try runIn(a, &tmp, try std.fmt.allocPrint(a, "LOAD INTO '$B/out.csv' AS {s};", .{q}), 4);
        try testing.expect(r.err == null);
        try testing.expectEqualStrings(one, r.out);
        try testing.expectEqual(n + 1, std.mem.count(u8, one, "\n"));
        try testing.expect(r.summary.threads > 1);
    }
}

test "Delta partitions: constants from the log, NULL partitions, a log value over the file's, added columns null in old files" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try dataFile(a, &tmp, "p/day=2026-01-01/n=1/a.parquet", "id,amt\n1,1.50\n2,2.25\n", "CAST(id AS INT) AS id, CAST(amt AS DECIMAL(9,2)) AS amt");
    try dataFile(a, &tmp, "p/day=2026-01-02/n=__HIVE_DEFAULT_PARTITION__/b.parquet", "id,amt,day\n3,3.00,1999-09-09\n", "CAST(id AS INT) AS id, CAST(amt AS DECIMAL(10,2)) AS amt, CAST(day AS DATE) AS day");
    try dataFile(a, &tmp, "p/day=2026-01-03/n=7/c.parquet", "id,amt,extra\n4,4.75,new\n", "CAST(id AS INT) AS id, CAST(amt AS DECIMAL(10,2)) AS amt, extra");
    const f4 = try std.mem.join(a, ",", &.{ try field(a, "id", "long"), try field(a, "amt", "decimal(10,2)"), try field(a, "day", "date"), try field(a, "n", "integer") });
    const f5 = try std.mem.join(a, ",", &.{ f4, try field(a, "extra", "string") });
    try commit(a, &tmp, "p", 0, &.{
        protocol1,
        try meta(a, f4, "[\"day\",\"n\"]", "{}"),
        try add(a, "day=2026-01-01/n=1/a.parquet", "{\"day\":\"2026-01-01\",\"n\":\"1\"}"),
        try add(a, "day=2026-01-02/n=__HIVE_DEFAULT_PARTITION__/b.parquet", "{\"day\":\"2026-01-02\",\"n\":null}"),
    });
    try commit(a, &tmp, "p", 1, &.{
        try meta(a, f5, "[\"day\",\"n\"]", "{\"delta.columnMapping.mode\":\"none\"}"),
        try add(a, "day=2026-01-03/n=7/c.parquet", "{\"day\":\"2026-01-03\",\"n\":\"\"}"),
    });

    try testing.expectEqualStrings(
        \\id,amt,day,n,extra
        \\1,1.50,2026-01-01,1,
        \\2,2.25,2026-01-01,1,
        \\3,3.00,2026-01-02,,
        \\4,4.75,2026-01-03,,new
        \\
    , try rows(a, &tmp, "SELECT * FROM '$B/p/' ORDER BY id", 1));
    try testing.expectEqualStrings("day,c\n2026-01-01,2\n2026-01-02,1\n2026-01-03,1\n", try rows(a, &tmp, "SELECT day, COUNT(*) AS c FROM '$B/p/' GROUP BY day ORDER BY day", 1));
    try testing.expectEqualStrings("n\n4\n", try rows(a, &tmp, "SELECT COUNT(*) AS n FROM '$B/p/'", 1));

    const pruned = try runIn(a, &tmp, "LOAD INTO '$B/out.csv' AS SELECT id FROM '$B/p/' WHERE n = 1 ORDER BY id;", 1);
    try testing.expect(pruned.err == null);
    try testing.expectEqualStrings("id\n1\n2\n", pruned.out);
    try testing.expectEqual(@as(u64, 1), pruned.summary.pushdown.row_groups);

    try testing.expectEqualStrings("id,amt,day,n\n1,1.50,2026-01-01,1\n2,2.25,2026-01-01,1\n3,3.00,2026-01-02,\n", try rows(a, &tmp, "SELECT * FROM '$B/p/' WITH (version = 0) ORDER BY id", 1));
}

test "a Delta table with every file removed reads no rows, typed by its schema" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try basicTable(a, &tmp);
    try commit(a, &tmp, "t", 2, &.{ try remove(a, "b.parquet"), try remove(a, "c.parquet") });
    try testing.expectEqualStrings("n\n0\n", try rows(a, &tmp, "SELECT COUNT(*) AS n FROM '$B/t/'", 1));
    try testing.expectEqualStrings("id,name\n", try rows(a, &tmp, "SELECT id, name FROM '$B/t/' WHERE id > 0", 4));
}

/// A flat checkpoint as basalt writes one: each action's fields as dotted columns,
/// maps and lists as JSON text, which the reader takes as a nested writer's.
fn flatCheckpoint(a: std.mem.Allocator, tmp: *std.testing.TmpDir, path: []const u8, actions: []const []const u8) !void {
    const head = "{\"add\":null,\"metaData\":null,\"protocol\":null,\"checkpointMetadata\":null}";
    try put(tmp, "cp.ndjson", try std.mem.join(a, "\n", try std.mem.concat(a, []const u8, &.{ &.{head}, actions })));
    const r = try runIn(a, tmp, try std.fmt.allocPrint(a,
        \\LOAD INTO '$B/{s}' AS SELECT json_get(add, 'path') AS "add.path", json_get(add, 'partitionValues') AS "add.partitionValues",
        \\  json_get(metaData, 'schemaString') AS "metaData.schemaString", json_get(metaData, 'partitionColumns') AS "metaData.partitionColumns",
        \\  CAST(json_get(protocol, 'minReaderVersion') AS INT) AS "protocol.minReaderVersion", json_get(protocol, 'readerFeatures') AS "protocol.readerFeatures",
        \\  json_get(checkpointMetadata, 'version') AS "checkpointMetadata.version"
        \\FROM '$B/cp.ndjson' WITH (format = 'ndjson');
    , .{path}), 1);
    if (r.err) |e| {
        std.debug.print("writing {s}: {s}\n", .{ path, r.msg });
        return e;
    }
}

test "Delta checkpoints: single and multi-part, commits after them replayed, an incomplete one passed over" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try basicTable(a, &tmp);
    try dataFile(a, &tmp, "t/d.parquet", "id,name\n6,fay\n", "CAST(id AS INT) AS id, name");
    const fields = try std.mem.join(a, ",", &.{ try field(a, "id", "long"), try field(a, "name", "string") });
    const m = try meta(a, fields, "[]", "{}");
    try flatCheckpoint(a, &tmp, "t/_delta_log/00000000000000000001.checkpoint.parquet", &.{ protocol1, m, try add(a, "b.parquet", "{}"), try add(a, "c.parquet", "{}") });
    try commit(a, &tmp, "t", 2, &.{try add(a, "d.parquet", "{}")});
    try tmp.dir.deleteFile("t/_delta_log/00000000000000000000.json");
    try tmp.dir.deleteFile("t/_delta_log/00000000000000000001.json");
    try testing.expectEqualStrings("id\n3\n4\n5\n6\n", try rows(a, &tmp, "SELECT id FROM '$B/t/' ORDER BY id", 1));
    try testing.expectEqualStrings("id\n3\n4\n5\n", try rows(a, &tmp, "SELECT id FROM '$B/t/' WITH (version = 1) ORDER BY id", 1));
    try refused(a, &tmp, "SELECT id FROM '$B/t/' WITH (version = 0)", "has no version 0 (versions 1 to 2 can be read)", false);

    try flatCheckpoint(a, &tmp, "t/_delta_log/00000000000000000002.checkpoint.0000000001.0000000002.parquet", &.{ protocol1, m, try add(a, "c.parquet", "{}") });
    try flatCheckpoint(a, &tmp, "t/_delta_log/00000000000000000002.checkpoint.0000000002.0000000002.parquet", &.{try add(a, "d.parquet", "{}")});
    try commit(a, &tmp, "t", 3, &.{try remove(a, "d.parquet")});
    try tmp.dir.deleteFile("t/_delta_log/00000000000000000002.json");
    try testing.expectEqualStrings("id\n5\n", try rows(a, &tmp, "SELECT id FROM '$B/t/' ORDER BY id", 1));
    try testing.expectEqualStrings("id\n5\n6\n", try rows(a, &tmp, "SELECT id FROM '$B/t/' WITH (version = 2) ORDER BY id", 1));

    try tmp.dir.deleteFile("t/_delta_log/00000000000000000002.checkpoint.0000000002.0000000002.parquet");
    try refused(a, &tmp, "SELECT id FROM '$B/t/'", "checkpoint 2 lists 2 parts but only 1 are in _delta_log/", false);
}

test "a checkpoint written by delta-rs: nested add, map partition values, a NULL partition, its tombstone skipped" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const sel = "CAST(id AS INT) AS id, CAST(amt AS DECIMAL(10,2)) AS amt";
    try dataFile(a, &tmp, "r/region=eu/day=2026-01-01/part-00000-fecb9d60-ca52-4091-aa6d-9c8772e5448c-c000.snappy.parquet", "id,amt\n1,1.50\n", sel);
    try dataFile(a, &tmp, "r/region=us/day=2026-01-01/part-00000-3cbbebc7-ecd9-44cb-8dd4-2341016c494b-c000.snappy.parquet", "id,amt\n2,2.00\n", sel);
    try dataFile(a, &tmp, "r/region=__HIVE_DEFAULT_PARTITION__/day=2026-01-02/part-00000-aecb4f61-0c41-48fa-bc08-3f5a595b12f0-c000.snappy.parquet", "id,amt\n3,3.25\n", sel);
    try dataFile(a, &tmp, "r/region=us/day=2026-01-05/new.parquet", "id,amt\n9,9.99\n", sel);
    try put(&tmp, "r/_delta_log/00000000000000000002.checkpoint.parquet", basalt.fixtures.delta_rs_checkpoint_parquet);
    try put(&tmp, "r/_delta_log/_last_checkpoint", "{\"version\":2,\"size\":6}");
    try commit(a, &tmp, "r", 3, &.{try add(a, "region=us/day=2026-01-05/new.parquet", "{\"region\":\"us\",\"day\":\"2026-01-05\"}")});
    try testing.expectEqualStrings(
        \\id,amt,region,day
        \\1,1.50,eu,2026-01-01
        \\3,3.25,,2026-01-02
        \\9,9.99,us,2026-01-05
        \\
    , try rows(a, &tmp, "SELECT * FROM '$B/r/' ORDER BY id", 1));
    try testing.expectEqualStrings("n\n2\n", try rows(a, &tmp, "SELECT COUNT(*) AS n FROM '$B/r/' WITH (version = 2)", 1));
}

test "Delta refusals name the feature, version or file a read cannot honour" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try basicTable(a, &tmp);

    const dv = "{\"add\":{\"path\":\"c.parquet\",\"partitionValues\":{},\"dataChange\":true,\"deletionVector\":{\"storageType\":\"u\",\"pathOrInlineDv\":\"ab^-aqEH.-t@S}K{vb[*k^\",\"offset\":4,\"sizeInBytes\":40,\"cardinality\":1}}}";
    const p3 = "{\"protocol\":{\"minReaderVersion\":3,\"minWriterVersion\":7,\"readerFeatures\":[\"deletionVectors\"],\"writerFeatures\":[\"deletionVectors\"]}}";
    try commit(a, &tmp, "t", 2, &.{ p3, try remove(a, "c.parquet"), dv });
    try refused(a, &tmp, "SELECT * FROM '$B/t/'", "has file `c.parquet` with a deletion vector (reader feature `deletionVectors`)", false);
    try testing.expectEqualStrings("n\n3\n", try rows(a, &tmp, "SELECT COUNT(*) AS n FROM '$B/t/' WITH (version = 1)", 1));

    for ([_][2][]const u8{
        .{ "{\"protocol\":{\"minReaderVersion\":3,\"minWriterVersion\":7,\"readerFeatures\":[\"v2Checkpoint\"]}}", "uses V2 checkpoints (reader feature `v2Checkpoint`)" },
        .{ "{\"protocol\":{\"minReaderVersion\":3,\"minWriterVersion\":7,\"readerFeatures\":[\"catalogManaged\"]}}", "is catalog-managed (reader feature `catalogManaged`)" },
        .{ "{\"protocol\":{\"minReaderVersion\":3,\"minWriterVersion\":7,\"readerFeatures\":[\"typeWidening\"]}}", "uses reader feature `typeWidening`, which basalt does not read" },
        .{ "{\"protocol\":{\"minReaderVersion\":3,\"minWriterVersion\":7,\"readerFeatures\":[\"timestampNtz\",\"futureThing\"]}}", "needs reader feature `futureThing`, which basalt does not know" },
        .{ "{\"protocol\":{\"minReaderVersion\":4,\"minWriterVersion\":7}}", "needs Delta reader version 4; basalt reads reader versions 1 to 3" },
    }) |c| {
        try commit(a, &tmp, "t", 2, &.{c[0]});
        try refused(a, &tmp, "SELECT * FROM '$B/t/'", c[1], false);
    }

    const fields = try std.mem.join(a, ",", &.{ try field(a, "id", "long"), try field(a, "name", "string") });
    try commit(a, &tmp, "t", 2, &.{ "{\"protocol\":{\"minReaderVersion\":2,\"minWriterVersion\":5}}", try meta(a, fields, "[]", "{\"delta.columnMapping.mode\":\"name\"}") });
    try refused(a, &tmp, "SELECT * FROM '$B/t/'", "uses column mapping (mode `name`)", false);
    try commit(a, &tmp, "t", 2, &.{try meta(a, try field(a, "v", "variant"), "[]", "{}")});
    try refused(a, &tmp, "SELECT * FROM '$B/t/'", "has a column `v` of type `variant`, which basalt does not read", false);
    try commit(a, &tmp, "t", 2, &.{try meta(a, try std.mem.join(a, ",", &.{ try field(a, "id", "string"), try field(a, "name", "string") }), "[]", "{}")});
    try refused(a, &tmp, "SELECT * FROM '$B/t/'", "has file `b.parquet` whose column `id` is int, not string as in the table's schema", true);
    try tmp.dir.deleteFile("t/_delta_log/00000000000000000002.json");

    try commit(a, &tmp, "t", 3, &.{try remove(a, "b.parquet")});
    try refused(a, &tmp, "SELECT * FROM '$B/t/'", "is missing commit 2 from _delta_log/ (commits 0 to 3 are needed to reach version 3)", false);
    try refused(a, &tmp, "SELECT * FROM '$B/t/' WITH (version = 9)", "has no version 9 (versions 0 to 3 can be read)", false);
    try tmp.dir.deleteFile("t/_delta_log/00000000000000000003.json");
    try refused(a, &tmp, "SELECT * FROM '$B/t/' WITH (version = 'latest')", "`version` is a Delta table's commit number", false);

    try dataFile(a, &tmp, "plain/x.parquet", "id\n1\n", "CAST(id AS INT) AS id");
    try refused(a, &tmp, "SELECT * FROM '$B/plain/' WITH (format = 'delta')", "has no _delta_log/: it is not a Delta table", false);
    try refused(a, &tmp, "SELECT * FROM '$B/plain/' WITH (version = 1)", "`version` pins a Delta table to a commit", false);
    try refused(a, &tmp, "SELECT * FROM '$B/t' WITH (format = 'delta')", "a Delta table is read from its root folder: end the path with `/`", false);
    try refused(a, &tmp, "SELECT * FROM '$B/t/' WITH (format = 'parquet', version = 1)", "read option `version` does not apply to a Parquet read", false);
    try refused(a, &tmp, "SELECT * FROM 'http://127.0.0.1:1/t/' WITH (format = 'delta')", "a Delta table is found by listing _delta_log/, which a URL cannot do", true);
    const w = try runIn(a, &tmp, "LOAD INTO '$B/w/' WITH (format = 'delta') AS SELECT 1 AS a;", 1);
    try testing.expect(w.err != null and w.check_msg != null);
    try testing.expect(std.mem.indexOf(u8, w.msg, "basalt reads Delta tables; it does not write them") != null);
}

test "EXPLAIN names a Delta scan with its version and file count" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try basicTable(a, &tmp);
    const base = try tmp.dir.realpathAlloc(a, ".");
    const script = try std.fmt.allocPrint(a, "EXPLAIN LOAD INTO '{s}/out.csv' AS SELECT id FROM '{s}/t/' WITH (version = 0);", .{ base, base });
    var pd: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(a, script, &pd);
    var adiag = analyze.Diag{};
    const plan = try analyze.analyze(a, prog, &adiag);
    var aw = std.Io.Writer.Allocating.init(a);
    try analyze.render(plan, &aw.writer);
    const want = try std.fmt.allocPrint(a, "delta {s}/t/ (version 0, 2 files)", .{base});
    if (std.mem.indexOf(u8, aw.written(), want) == null) {
        std.debug.print("want `{s}` in:\n{s}\n", .{ want, aw.written() });
        return error.TestUnexpectedResult;
    }
}

test "a join's keys skip a Delta table's row groups, partition constants pass through the join" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try basicTable(a, &tmp);
    try put(&tmp, "k.csv", "k,q\n5,x\n");
    for ([_]usize{ 1, 4 }) |threads| {
        const r = try runIn(a, &tmp, "LOAD INTO '$B/out.csv' AS SELECT t.name, k.q FROM '$B/t/' t JOIN '$B/k.csv' k ON t.id = k.k;", threads);
        try testing.expect(r.err == null);
        try testing.expectEqualStrings("name,q\neve,x\n", r.out);
        try testing.expectEqual(@as(u64, 1), r.summary.pushdown.row_groups_skipped);
    }
}

const avrow = basalt.avrowrite;

const ice_loc = "s3://bucket/wh/";

const list_schema =
    \\{"type":"record","name":"manifest_file","fields":[
    \\{"name":"manifest_path","field-id":500,"type":"string"},{"name":"manifest_length","field-id":501,"type":"long"},
    \\{"name":"partition_spec_id","field-id":502,"type":"int"},{"name":"content","field-id":517,"type":"int"},
    \\{"name":"added_snapshot_id","field-id":503,"type":"long"},{"name":"key_metadata","field-id":519,"type":["null","bytes"]}]}
;

const entry_schema =
    \\{"type":"record","name":"manifest_entry","fields":[
    \\{"name":"status","field-id":0,"type":"int"},{"name":"snapshot_id","field-id":1,"type":["null","long"]},
    \\{"name":"data_file","field-id":2,"type":{"type":"record","name":"r2","fields":[
    \\ {"name":"content","field-id":134,"type":"int"},{"name":"file_path","field-id":100,"type":"string"},
    \\ {"name":"file_format","field-id":101,"type":"string"},
    \\ {"name":"partition","field-id":102,"type":{"type":"record","name":"r102","fields":[{"name":"region","field-id":1000,"type":["null","string"]}]}},
    \\ {"name":"record_count","field-id":103,"type":"long"},{"name":"file_size_in_bytes","field-id":104,"type":"long"},
    \\ {"name":"key_metadata","field-id":131,"type":["null","bytes"]}]}}]}
;

const region_spec = "[{\"source-id\":3,\"field-id\":1000,\"transform\":\"identity\",\"name\":\"region\"}]";

/// One manifest entry: `file` the data file's path under the tmp dir, named in the
/// manifest under the table's location unless `uri` names it otherwise.
const Entry = struct {
    status: i64 = 1,
    file: []const u8,
    uri: ?[]const u8 = null,
    region: ?[]const u8 = null,
    content: i64 = 0,
    format: []const u8 = "PARQUET",
    key: bool = false,
};

fn manifest(a: std.mem.Allocator, tmp: *std.testing.TmpDir, rel: []const u8, codec: avrow.Codec, entries: []const Entry, deletes: bool) !void {
    var recs = std.array_list.Managed([]const u8).init(a);
    for (entries) |en| {
        var e = avrow.Encoder.init(a);
        try e.long(en.status);
        try e.long(1);
        try e.long(1);
        try e.long(en.content);
        try e.string(en.uri orelse try std.mem.concat(a, u8, &.{ ice_loc, en.file }));
        try e.string(en.format);
        if (en.region) |r| {
            try e.long(1);
            try e.string(r);
        } else try e.long(0);
        try e.long(1);
        const st = tmp.dir.statFile(en.file) catch null;
        try e.long(if (st) |x| @intCast(x.size) else 10);
        if (en.key) {
            try e.long(1);
            try e.string("k");
        } else try e.long(0);
        try recs.append(e.out.items);
    }
    const header = [_][2][]const u8{ .{ "partition-spec", region_spec }, .{ "partition-spec-id", "0" }, .{ "format-version", "2" }, .{ "content", if (deletes) "deletes" else "data" } };
    try put(tmp, rel, try avrow.container(a, entry_schema, codec, recs.items, 2, &header));
}

const Listed = struct { rel: []const u8, content: i64 = 0, key: bool = false };

fn manifestList(a: std.mem.Allocator, tmp: *std.testing.TmpDir, rel: []const u8, codec: avrow.Codec, ms: []const Listed) !void {
    var recs = std.array_list.Managed([]const u8).init(a);
    for (ms) |m| {
        var e = avrow.Encoder.init(a);
        try e.string(try std.mem.concat(a, u8, &.{ ice_loc, m.rel }));
        try e.long(100);
        try e.long(0);
        try e.long(m.content);
        try e.long(1);
        if (m.key) {
            try e.long(1);
            try e.string("k");
        } else try e.long(0);
        try recs.append(e.out.items);
    }
    try put(tmp, rel, try avrow.container(a, list_schema, codec, recs.items, 1, &.{}));
}

const ice_schema = "{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"long\"},{\"id\":2,\"name\":\"name\",\"required\":false,\"type\":\"string\"},{\"id\":3,\"name\":\"region\",\"required\":false,\"type\":\"string\"}]}";

fn snapshot(a: std.mem.Allocator, id: i64, list: []const u8) ![]const u8 {
    return std.fmt.allocPrint(a, "{{\"snapshot-id\":{d},\"sequence-number\":1,\"timestamp-ms\":0,\"manifest-list\":\"{s}{s}\",\"summary\":{{\"operation\":\"append\"}},\"schema-id\":0}}", .{ id, ice_loc, list });
}

/// Metadata for table `t` (located at `s3://bucket/wh/t`) of format version `fv`,
/// its `schemas` current at `schema_id`, `snaps` current at `cur`.
fn metadata(a: std.mem.Allocator, tmp: *std.testing.TmpDir, t: []const u8, rel: []const u8, fv: u8, schemas: []const u8, schema_id: i64, snaps: []const []const u8, cur: i64) !void {
    try put(tmp, try std.fmt.allocPrint(a, "{s}/metadata/{s}", .{ t, rel }), try std.fmt.allocPrint(a,
        \\{{"format-version":{d},"table-uuid":"u","location":"{s}{s}","last-updated-ms":0,"last-column-id":3,
        \\"schemas":[{s}],"current-schema-id":{d},
        \\"partition-specs":[{{"spec-id":0,"fields":{s}}}],"default-spec-id":0,"last-partition-id":1000,
        \\"properties":{{}},"current-snapshot-id":{d},"snapshots":[{s}]}}
    , .{ fv, ice_loc, t, schemas, schema_id, region_spec, cur, try std.mem.join(a, ",", snaps) }));
}

/// Table `ice/` of two snapshots: 101 adds `a` (eu) and `b` (us); 202 deletes `a`,
/// keeps `b` and adds `c` (us) under an `s3a://` path. A stale version hint names 1.
fn iceTable(a: std.mem.Allocator, tmp: *std.testing.TmpDir) !void {
    const sel = "CAST(id AS INT) AS id, name, region";
    try dataFile(a, tmp, "ice/data/a.parquet", "id,name,region\n1,ann,eu\n2,bob,eu\n", sel);
    try dataFile(a, tmp, "ice/data/b.parquet", "id,name,region\n3,cy,us\n", sel);
    try dataFile(a, tmp, "ice/data/c.parquet", "id,name,region\n4,dee,us\n", sel);
    try manifest(a, tmp, "ice/metadata/m1.avro", .deflate, &.{ .{ .file = "ice/data/a.parquet", .region = "eu" }, .{ .file = "ice/data/b.parquet", .region = "us" } }, false);
    try manifest(a, tmp, "ice/metadata/m2a.avro", .null, &.{ .{ .status = 2, .file = "ice/data/a.parquet", .region = "eu" }, .{ .status = 0, .file = "ice/data/b.parquet", .region = "us" } }, false);
    try manifest(a, tmp, "ice/metadata/m2b.avro", .zstandard, &.{.{ .file = "ice/data/c.parquet", .uri = "s3a://bucket/wh/ice/data/c.parquet", .region = "us" }}, false);
    try manifestList(a, tmp, "ice/metadata/snap-101.avro", .snappy, &.{.{ .rel = "ice/metadata/m1.avro" }});
    try manifestList(a, tmp, "ice/metadata/snap-202.avro", .null, &.{ .{ .rel = "ice/metadata/m2a.avro" }, .{ .rel = "ice/metadata/m2b.avro" } });
    const s1 = try snapshot(a, 101, "ice/metadata/snap-101.avro");
    const s2 = try snapshot(a, 202, "ice/metadata/snap-202.avro");
    try metadata(a, tmp, "ice", "v1.metadata.json", 2, ice_schema, 0, &.{s1}, 101);
    try metadata(a, tmp, "ice", "v2.metadata.json", 2, ice_schema, 0, &.{ s1, s2 }, 202);
    try put(tmp, "ice/metadata/version-hint.text", "1\n");
}

test "an Iceberg folder reads its current snapshot's live files; snapshot_id and a metadata path read others" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try iceTable(a, &tmp);

    try testing.expectEqualStrings("id,name,region\n3,cy,us\n4,dee,us\n", try rows(a, &tmp, "SELECT * FROM '$B/ice/' ORDER BY id", 1));
    try testing.expectEqualStrings("id,name,region\n3,cy,us\n4,dee,us\n", try rows(a, &tmp, "SELECT * FROM '$B/ice/' WITH (format = 'iceberg') ORDER BY id", 1));
    try testing.expectEqualStrings("n\n4\n", try rows(a, &tmp, "SELECT COUNT(*) AS n FROM '$B/ice/' WITH (format = 'parquet')", 1));
    try testing.expectEqualStrings("id\n1\n2\n3\n", try rows(a, &tmp, "SELECT id FROM '$B/ice/' WITH (snapshot_id = 101) ORDER BY id", 1));
    try testing.expectEqualStrings("id\n1\n2\n3\n", try rows(a, &tmp, "SELECT id FROM '$B/ice/' WITH (snapshot_id = '101') ORDER BY id", 1));
    try testing.expectEqualStrings("id\n1\n2\n3\n", try rows(a, &tmp, "SELECT id FROM '$B/ice/metadata/v1.metadata.json' WITH (format = 'iceberg') ORDER BY id", 1));
    try testing.expectEqualStrings("id\n3\n4\n", try rows(a, &tmp, "SELECT id FROM '$B/ice/metadata/v2.metadata.json' WITH (format = 'iceberg') ORDER BY id", 1));
    try testing.expectEqualStrings("n\n2\n", try rows(a, &tmp, "SELECT COUNT(*) AS n FROM '$B/ice/'", 1));
    try testing.expectEqualStrings("fv\n2\n", try rows(a, &tmp, "SELECT \"format-version\" AS fv FROM '$B/ice/metadata/v1.metadata.json'", 1));
    try testing.expectEqualStrings("name\ndee\n", try rows(a, &tmp, "SELECT name FROM '$B/ice/' WHERE id = 4", 1));
}

test "an Iceberg table's lanes return what one lane does, and lanes run" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try iceTable(a, &tmp);
    for ([_][]const u8{
        "SELECT region, SUM(id) AS s FROM '$B/ice/' GROUP BY region ORDER BY region",
        "SELECT DISTINCT name FROM '$B/ice/' WITH (snapshot_id = 101) ORDER BY name",
        "WITH old AS (SELECT * FROM '$B/ice/' WITH (snapshot_id = 101)) SELECT region, COUNT(*) AS c FROM old GROUP BY region ORDER BY region",
        "SELECT region, COUNT(*) AS c FROM '$B/ice/metadata/v1.metadata.json' WITH (format = 'iceberg') GROUP BY region ORDER BY region",
    }, [_]usize{ 1, 3, 2, 2 }) |q, n| {
        const one = try rows(a, &tmp, q, 1);
        const r = try runIn(a, &tmp, try std.fmt.allocPrint(a, "LOAD INTO '$B/out.csv' AS {s};", .{q}), 4);
        if (r.err != null) std.debug.print("{s}: {s}\n", .{ q, r.msg });
        try testing.expect(r.err == null);
        try testing.expectEqualStrings(one, r.out);
        try testing.expectEqual(n + 1, std.mem.count(u8, one, "\n"));
        try testing.expect(r.summary.threads > 1);
    }
}

test "Iceberg identity partitions: a file lacking the column reads the manifest's value, a filter skips files unopened" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try dataFile(a, &tmp, "hv/data/region=eu/a.parquet", "id,name\n1,ann\n2,bob\n", "CAST(id AS INT) AS id, name");
    try dataFile(a, &tmp, "hv/data/region=us/b.parquet", "id,name\n3,cy\n", "CAST(id AS INT) AS id, name");
    try dataFile(a, &tmp, "hv/data/region=null/c.parquet", "id,name\n4,dee\n", "CAST(id AS INT) AS id, name");
    try manifest(a, &tmp, "hv/metadata/m1.avro", .snappy, &.{
        .{ .file = "hv/data/region=eu/a.parquet", .region = "eu" },
        .{ .file = "hv/data/region=us/b.parquet", .region = "us" },
        .{ .file = "hv/data/region=null/c.parquet" },
    }, false);
    try manifestList(a, &tmp, "hv/metadata/snap-7.avro", .deflate, &.{.{ .rel = "hv/metadata/m1.avro" }});
    try metadata(a, &tmp, "hv", "v1.metadata.json", 1, ice_schema, 0, &.{try snapshot(a, 7, "hv/metadata/snap-7.avro")}, 7);

    try testing.expectEqualStrings("id,name,region\n1,ann,eu\n2,bob,eu\n3,cy,us\n4,dee,\n", try rows(a, &tmp, "SELECT * FROM '$B/hv/' ORDER BY id", 1));
    try testing.expectEqualStrings("region,c\n,1\neu,2\nus,1\n", try rows(a, &tmp, "SELECT region, COUNT(*) AS c FROM '$B/hv/' GROUP BY region ORDER BY region NULLS FIRST", 1));
    const pruned = try runIn(a, &tmp, "LOAD INTO '$B/out.csv' AS SELECT id FROM '$B/hv/' WHERE region = 'us';", 1);
    try testing.expect(pruned.err == null);
    try testing.expectEqualStrings("id\n3\n", pruned.out);
    try testing.expectEqual(@as(u64, 1), pruned.summary.pushdown.row_groups);
    try testing.expectEqualStrings("id\n4\n", try rows(a, &tmp, "SELECT id FROM '$B/hv/' WHERE region IS NULL", 1));
}

test "the Iceberg metadata read: the highest version listed, gzip forms, a tie refused, an empty table" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try iceTable(a, &tmp);
    const v2 = try tmp.dir.readFileAlloc(a, "ice/metadata/v2.metadata.json", 1 << 20);
    const v1 = try tmp.dir.readFileAlloc(a, "ice/metadata/v1.metadata.json", 1 << 20);
    try put(&tmp, "ice/metadata/v3.gz.metadata.json", try basalt.codec.compress(a, .gzip, v1));
    try testing.expectEqualStrings("id\n1\n2\n3\n", try rows(a, &tmp, "SELECT id FROM '$B/ice/' ORDER BY id", 1));
    try put(&tmp, "ice/metadata/00004-6f1e0c2a-1b3d-4e5f-8a9b-0c1d2e3f4a5b.metadata.json.gz", try basalt.codec.compress(a, .gzip, v2));
    try testing.expectEqualStrings("id\n3\n4\n", try rows(a, &tmp, "SELECT id FROM '$B/ice/' ORDER BY id", 1));
    try put(&tmp, "ice/metadata/00004-9a2b7c6d-5e4f-4a3b-9c2d-1e0f9a8b7c6d.metadata.json", v1);
    try refused(a, &tmp, "SELECT id FROM '$B/ice/'", "has two metadata files of version 4", false);
    try testing.expectEqualStrings("id\n1\n2\n3\n", try rows(a, &tmp, "SELECT id FROM '$B/ice/metadata/00004-9a2b7c6d-5e4f-4a3b-9c2d-1e0f9a8b7c6d.metadata.json' WITH (format = 'iceberg') ORDER BY id", 1));

    try metadata(a, &tmp, "empty", "v1.metadata.json", 2, ice_schema, 0, &.{}, -1);
    try testing.expectEqualStrings("n\n0\n", try rows(a, &tmp, "SELECT COUNT(*) AS n FROM '$B/empty/'", 1));
    try testing.expectEqualStrings("id,region\n", try rows(a, &tmp, "SELECT id, region FROM '$B/empty/' WHERE id > 0", 4));
}

test "a table pyiceberg wrote: its Avro shapes, a delete of one partition, a NULL partition, time travel, read from a copy" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    for (basalt.fixtures.iceberg_py) |f| try put(&tmp, try std.mem.concat(a, u8, &.{ "py/", f[0] }), f[1]);
    try testing.expectEqualStrings(
        \\id,name,day,amt,geo.lat,geo.city,tags
        \\3,cy,2026-01-02,,,sp,[]
        \\4,dee,2026-01-02,4.00,,,"[""x""]"
        \\5,eve,,5.55,,,
        \\
    , try rows(a, &tmp, "SELECT * FROM '$B/py/' ORDER BY id", 1));
    try testing.expectEqualStrings("n\n5\n", try rows(a, &tmp, "SELECT COUNT(*) AS n FROM '$B/py/' WITH (snapshot_id = 4854212885010183066)", 1));
    try testing.expectEqualStrings("id,day\n1,2026-01-01\n2,2026-01-01\n3,2026-01-02\n", try rows(a, &tmp, "SELECT id, day FROM '$B/py/' WITH (snapshot_id = 2072416895968069595) ORDER BY id", 1));
    try testing.expectEqualStrings("s\n5.55\n", try rows(a, &tmp, "SELECT SUM(amt) AS s FROM '$B/py/' WHERE day IS NULL", 1));
    const pruned = try runIn(a, &tmp, "LOAD INTO '$B/out.csv' AS SELECT id FROM '$B/py/' WHERE day = CAST('2026-01-02' AS DATE) ORDER BY id;", 1);
    try testing.expect(pruned.err == null);
    try testing.expectEqualStrings("id\n3\n4\n", pruned.out);
    const q = "SELECT day, COUNT(*) AS n, SUM(amt) AS s FROM '$B/py/' WITH (snapshot_id = 4854212885010183066) GROUP BY day ORDER BY day";
    const one = try rows(a, &tmp, q, 1);
    try testing.expectEqualStrings("day,n,s\n2026-01-01,2,3.75\n2026-01-02,2,4.00\n,1,5.55\n", one);
    const four = try runIn(a, &tmp, try std.fmt.allocPrint(a, "LOAD INTO '$B/out.csv' AS {s};", .{q}), 4);
    try testing.expectEqualStrings(one, four.out);
}

test "Iceberg refusals name the delete files, rename, format or snapshot a read cannot honour" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try iceTable(a, &tmp);
    const s1 = try snapshot(a, 101, "ice/metadata/snap-101.avro");
    const s2 = try snapshot(a, 202, "ice/metadata/snap-202.avro");
    const s3 = try snapshot(a, 303, "ice/metadata/snap-303.avro");

    try refused(a, &tmp, "SELECT * FROM '$B/ice/' WITH (snapshot_id = 999)", "has no snapshot 999 (snapshots: 101, 202)", false);
    try refused(a, &tmp, "SELECT * FROM '$B/ice/' WITH (version = 1)", "`version` pins a Delta table to a commit; `", false);
    try refused(a, &tmp, "SELECT * FROM '$B/ice/' WITH (snapshot_id = 'latest')", "`snapshot_id` is an Iceberg snapshot's id", false);

    try manifest(a, &tmp, "ice/metadata/d1.avro", .null, &.{.{ .file = "ice/data/del.parquet", .content = 1 }}, true);
    try manifestList(a, &tmp, "ice/metadata/snap-303.avro", .null, &.{ .{ .rel = "ice/metadata/m2a.avro" }, .{ .rel = "ice/metadata/d1.avro", .content = 1 } });
    try metadata(a, &tmp, "ice", "v3.metadata.json", 2, ice_schema, 0, &.{ s1, s2, s3 }, 303);
    try refused(a, &tmp, "SELECT * FROM '$B/ice/'", "has delete files in its snapshot (`data/del.parquet`, position deletes); basalt does not apply row-level deletes", false);
    try testing.expectEqualStrings("n\n2\n", try rows(a, &tmp, "SELECT COUNT(*) AS n FROM '$B/ice/' WITH (snapshot_id = 202)", 1));
    try manifest(a, &tmp, "ice/metadata/d1.avro", .null, &.{.{ .status = 2, .file = "ice/data/del.parquet", .content = 1 }}, true);
    try testing.expectEqualStrings("n\n1\n", try rows(a, &tmp, "SELECT COUNT(*) AS n FROM '$B/ice/'", 1));

    for ([_]struct { Entry, []const u8 }{
        .{ .{ .file = "ice/data/x.orc", .format = "ORC" }, "has data file `data/x.orc` in ORC; basalt reads Iceberg data files in Parquet" },
        .{ .{ .file = "ice/data/e.parquet", .content = 2 }, "(`data/e.parquet`, equality deletes)" },
        .{ .{ .file = "ice/data/b.parquet", .key = true }, "is encrypted (key_metadata on `data/b.parquet`)" },
    }) |c| {
        try manifest(a, &tmp, "ice/metadata/d1.avro", .null, &.{c[0]}, false);
        try manifestList(a, &tmp, "ice/metadata/snap-303.avro", .null, &.{.{ .rel = "ice/metadata/d1.avro" }});
        try refused(a, &tmp, "SELECT * FROM '$B/ice/'", c[1], false);
    }
    try put(&tmp, "ice/metadata/d1.avro", try avrow.container(a, entry_schema, .null, &.{}, 1, &.{.{ "avro.codec", "bzip2" }}));
    try refused(a, &tmp, "SELECT * FROM '$B/ice/'", "has manifest `metadata/d1.avro` written with the Avro codec `bzip2`; basalt reads null, deflate, snappy and zstandard", false);
    try put(&tmp, "ice/metadata/d1.avro", "Obj\x01\x02garbage");
    try refused(a, &tmp, "SELECT * FROM '$B/ice/'", "has corrupt Avro in `metadata/d1.avro`", false);

    try metadata(a, &tmp, "ice", "v3.metadata.json", 4, ice_schema, 0, &.{s2}, 202);
    try refused(a, &tmp, "SELECT * FROM '$B/ice/'", "is format version 4; basalt reads Iceberg format versions 1 to 3", false);
    const renamed = "{\"type\":\"struct\",\"schema-id\":1,\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"long\"},{\"id\":2,\"name\":\"who\",\"required\":false,\"type\":\"string\"},{\"id\":3,\"name\":\"region\",\"required\":false,\"type\":\"string\"}]}";
    try metadata(a, &tmp, "ice", "v3.metadata.json", 2, try std.mem.join(a, ",", &.{ ice_schema, renamed }), 1, &.{s2}, 202);
    try refused(a, &tmp, "SELECT * FROM '$B/ice/'", "has a column `who` renamed from `name`; basalt reads Iceberg data files by column name", false);
    const wide = "{\"type\":\"struct\",\"schema-id\":1,\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"string\"},{\"id\":2,\"name\":\"name\",\"required\":false,\"type\":\"string\"},{\"id\":3,\"name\":\"region\",\"required\":false,\"type\":\"string\"},{\"id\":4,\"name\":\"extra\",\"required\":false,\"type\":\"date\"}]}";
    try metadata(a, &tmp, "ice", "v3.metadata.json", 2, try std.mem.join(a, ",", &.{ ice_schema, wide }), 1, &.{s2}, 202);
    try refused(a, &tmp, "SELECT * FROM '$B/ice/'", "has file `data/b.parquet` whose column `id` is int, not string as in the table's schema", true);
    try tmp.dir.deleteFile("ice/metadata/v3.metadata.json");

    try dataFile(a, &tmp, "plain/x.parquet", "id\n1\n", "CAST(id AS INT) AS id");
    try refused(a, &tmp, "SELECT * FROM '$B/plain/' WITH (format = 'iceberg')", "has no metadata/*.metadata.json: it is not an Iceberg table", false);
    try refused(a, &tmp, "SELECT * FROM '$B/plain/' WITH (snapshot_id = 1)", "`snapshot_id` pins an Iceberg table to a snapshot", false);
    try refused(a, &tmp, "SELECT * FROM '$B/plain/x.parquet' WITH (format = 'iceberg')", "an Iceberg table is read from its root folder", false);
    try refused(a, &tmp, "SELECT * FROM '$B/ice/' WITH (format = 'delta')", "has no _delta_log/: it is not a Delta table", false);
    try refused(a, &tmp, "SELECT * FROM '$B/ice/' WITH (format = 'parquet', snapshot_id = 1)", "read option `snapshot_id` does not apply to a Parquet read", false);
    const w = try runIn(a, &tmp, "LOAD INTO '$B/w/' WITH (format = 'iceberg') AS SELECT 1 AS a;", 1);
    try testing.expect(w.err != null and w.check_msg != null);
    try testing.expect(std.mem.indexOf(u8, w.msg, "basalt reads Iceberg tables; it does not write them") != null);
}

test "EXPLAIN names an Iceberg scan with its snapshot and file count" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try iceTable(a, &tmp);
    const base = try tmp.dir.realpathAlloc(a, ".");
    const script = try std.fmt.allocPrint(a, "EXPLAIN LOAD INTO '{s}/out.csv' AS SELECT id FROM '{s}/ice/' WITH (snapshot_id = 101);", .{ base, base });
    var pd: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(a, script, &pd);
    var adiag = analyze.Diag{};
    const plan = try analyze.analyze(a, prog, &adiag);
    var aw = std.Io.Writer.Allocating.init(a);
    try analyze.render(plan, &aw.writer);
    const want = try std.fmt.allocPrint(a, "iceberg {s}/ice/ (snapshot 101, 2 files)", .{base});
    if (std.mem.indexOf(u8, aw.written(), want) == null) {
        std.debug.print("want `{s}` in:\n{s}\n", .{ want, aw.written() });
        return error.TestUnexpectedResult;
    }
}

/// Every file under `dir` in the tmp dir, as (path, bytes) served at `/<dir>/...`.
fn served(a: std.mem.Allocator, tmp: *std.testing.TmpDir, dir: []const u8) ![]const [2][]const u8 {
    var out = std.array_list.Managed([2][]const u8).init(a);
    var d = try tmp.dir.openDir(dir, .{ .iterate = true });
    defer d.close();
    var w = try d.walk(a);
    defer w.deinit();
    while (try w.next()) |en| {
        if (en.kind != .file) continue;
        const rel = try std.mem.concat(a, u8, &.{ dir, "/", en.path });
        try out.append(.{ try std.mem.concat(a, u8, &.{ "/", rel }), try tmp.dir.readFileAlloc(a, rel, 1 << 24) });
    }
    return out.items;
}

test "an Iceberg table over http: its version hint, its paths rebased onto the URL, lanes and ranged reads" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try iceTable(a, &tmp);
    try put(&tmp, "ice/metadata/version-hint.text", "2\n");
    const local = try rows(a, &tmp, "SELECT region, name, id FROM '$B/ice/' ORDER BY id", 1);
    var srv: basalt.objstore.RangeServer = undefined;
    try srv.startFiles(try served(a, &tmp, "ice"));
    defer srv.stop();
    const root = try srv.url(a, "/ice/");
    for ([_]usize{ 1, 4 }) |threads| {
        const r = try runIn(a, &tmp, try std.fmt.allocPrint(a, "LOAD INTO '$B/out.csv' AS SELECT region, name, id FROM '{s}' ORDER BY id;", .{root}), threads);
        if (r.err != null) std.debug.print("{s}\n", .{r.msg});
        try testing.expect(r.err == null);
        try testing.expectEqualStrings(local, r.out);
    }
    const agg = try runIn(a, &tmp, try std.fmt.allocPrint(a, "LOAD INTO '$B/out.csv' AS SELECT region, COUNT(*) AS c FROM '{s}' WITH (snapshot_id = 101) GROUP BY region ORDER BY region;", .{root}), 4);
    try testing.expect(agg.err == null);
    try testing.expectEqualStrings("region,c\neu,2\nus,1\n", agg.out);
    try testing.expect(agg.summary.threads > 1);
    try testing.expect(srv.partial.load(.seq_cst) > 0);
    const direct = try runIn(a, &tmp, try std.fmt.allocPrint(a, "LOAD INTO '$B/out.csv' AS SELECT id FROM '{s}metadata/v1.metadata.json' WITH (format = 'iceberg') ORDER BY id;", .{root}), 1);
    try testing.expect(direct.err == null);
    try testing.expectEqualStrings("id\n1\n2\n3\n", direct.out);

    var bare: basalt.objstore.RangeServer = undefined;
    const files = try served(a, &tmp, "ice");
    var kept = std.array_list.Managed([2][]const u8).init(a);
    for (files) |f| if (!std.mem.endsWith(u8, f[0], "version-hint.text")) try kept.append(f);
    try bare.startFiles(kept.items);
    defer bare.stop();
    const r = try runIn(a, &tmp, try std.fmt.allocPrint(a, "LOAD INTO '$B/out.csv' AS SELECT id FROM '{s}' WITH (format = 'iceberg');", .{try bare.url(a, "/ice/")}), 1);
    try testing.expect(r.err != null);
    try testing.expect(std.mem.indexOf(u8, r.msg, "has no metadata/version-hint.text, which an http(s) URL needs") != null);
}

test "Delta replay keys a file by its resolved path and deletion vector; a missing file and an empty schema are named" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try basicTable(a, &tmp);
    const base = try tmp.dir.realpathAlloc(a, ".");

    const p3 = "{\"protocol\":{\"minReaderVersion\":3,\"minWriterVersion\":7,\"readerFeatures\":[\"deletionVectors\"],\"writerFeatures\":[\"deletionVectors\"]}}";
    const dv = "{\"add\":{\"path\":\"c.parquet\",\"partitionValues\":{},\"dataChange\":true,\"deletionVector\":{\"storageType\":\"u\",\"pathOrInlineDv\":\"ab^-aqEH.-t@S}K{vb[*k^\",\"offset\":4,\"sizeInBytes\":40,\"cardinality\":1}}}";
    try commit(a, &tmp, "t", 2, &.{ p3, dv, try remove(a, "c.parquet") });
    try refused(a, &tmp, "SELECT * FROM '$B/t/'", "has file `c.parquet` with a deletion vector (reader feature `deletionVectors`)", false);
    try commit(a, &tmp, "t", 2, &.{ p3, try remove(a, "c.parquet"), dv });
    try refused(a, &tmp, "SELECT * FROM '$B/t/'", "has file `c.parquet` with a deletion vector (reader feature `deletionVectors`)", false);

    try commit(a, &tmp, "t", 2, &.{try remove(a, try std.fmt.allocPrint(a, "file://{s}/t/b.parquet", .{base}))});
    try testing.expectEqualStrings("id\n5\n", try rows(a, &tmp, "SELECT id FROM '$B/t/' ORDER BY id", 1));

    try commit(a, &tmp, "t", 3, &.{try add(a, "gone.parquet", "{}")});
    for ([_]usize{ 1, 4 }) |threads| {
        const r = try runIn(a, &tmp, "LOAD INTO '$B/out.csv' AS SELECT id FROM '$B/t/';", threads);
        try testing.expect(r.err != null);
        if (std.mem.indexOf(u8, r.msg, "could not read its file `gone.parquet` (") == null) {
            std.debug.print("threads {d}: {s}\n", .{ threads, r.msg });
            return error.TestUnexpectedResult;
        }
    }

    try commit(a, &tmp, "e", 0, &.{ protocol1, try meta(a, "", "[]", "{}") });
    try refused(a, &tmp, "SELECT * FROM '$B/e/'", "has a schema with no columns", false);
}

test "Iceberg data files are checked by field id: a column renamed or re-added after its old schema is gone is refused, not read as NULL" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    for (basalt.fixtures.iceberg_py) |f| try put(&tmp, try std.mem.concat(a, u8, &.{ "py/", f[0] }), f[1]);
    const cur = "py/metadata/00003-91ece667-5252-4853-b887-31591380dc23.metadata.json";
    const md = try tmp.dir.readFileAlloc(a, cur, 1 << 20);

    try put(&tmp, "py/metadata/12345678-aaaa-4bbb-8ccc-dddddddddddd.metadata.json", try tmp.dir.readFileAlloc(a, "py/metadata/00002-83198d25-577f-4aee-9c4a-9734541e16d5.metadata.json", 1 << 20));
    try testing.expectEqualStrings("n\n3\n", try rows(a, &tmp, "SELECT COUNT(*) AS n FROM '$B/py/'", 1));

    try put(&tmp, cur, try std.mem.replaceOwned(u8, a, md, "{\"id\":2,\"name\":\"name\"", "{\"id\":2,\"name\":\"label\""));
    try refused(a, &tmp, "SELECT id, label FROM '$B/py/'", "holding column `label` (field id 2) as `name`: the column was renamed", true);
    try put(&tmp, cur, try std.mem.replaceOwned(u8, a, md, "{\"id\":2,\"name\":\"name\"", "{\"id\":12,\"name\":\"name\""));
    try refused(a, &tmp, "SELECT id, name FROM '$B/py/'", "whose column `name` has field id 2, not 12 as in the table's schema", true);
    try put(&tmp, cur, md);
    try testing.expectEqualStrings("id,name\n3,cy\n4,dee\n5,eve\n", try rows(a, &tmp, "SELECT id, name FROM '$B/py/' ORDER BY id", 1));
}
