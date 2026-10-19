//! End-to-end reads of Delta Lake tables: data files written by basalt, a Delta log
//! written as text (a checkpoint as Parquet, flat as basalt writes it or nested as
//! delta-rs does), plus a table delta-rs wrote; each read checked by `check` and
//! `run` alike. An Iceberg folder is refused.

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

test "a local Delta table of many files splits at files: rows as one lane reads them, column counts kept, a missing file named from its lane" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var lines = std.array_list.Managed([]const u8).init(a);
    try lines.appendSlice(&.{ protocol1, try meta(a, try std.mem.join(a, ",", &.{ try field(a, "id", "long"), try field(a, "name", "string") }), "[]", "{}") });
    for (0..20) |i| {
        const f = try std.fmt.allocPrint(a, "m/f{d:0>2}.parquet", .{i});
        try dataFile(a, &tmp, f, try std.fmt.allocPrint(a, "id,name\n{d},n{d}\n{d},n{d}\n", .{ 2 * i, i % 3, 2 * i + 1, i % 3 }), "CAST(id AS INT) AS id, name");
        try lines.append(try add(a, f["m/".len..], "{}"));
    }
    try commit(a, &tmp, "m", 0, lines.items);
    const q = "SELECT name, COUNT(*) AS c, SUM(id) AS s FROM '$B/m/' GROUP BY name ORDER BY name";
    const one = try rows(a, &tmp, q, 1);
    try testing.expectEqualStrings("name,c,s\nn0,14,259\nn1,14,287\nn2,12,234\n", one);
    const two = try runIn(a, &tmp, try std.fmt.allocPrint(a, "LOAD INTO '$B/out.csv' AS {s};", .{q}), 2);
    try testing.expect(two.err == null);
    try testing.expectEqualStrings(one, two.out);
    try testing.expect(two.summary.threads > 1);
    try testing.expectEqual(@as(u64, 2), two.summary.pushdown.columns_total);

    try commit(a, &tmp, "m", 1, &.{try add(a, "gone.parquet", "{}")});
    const r = try runIn(a, &tmp, "LOAD INTO '$B/out.csv' AS SELECT name, COUNT(*) AS c FROM '$B/m/' GROUP BY name;", 2);
    try testing.expect(r.err != null);
    if (std.mem.indexOf(u8, r.msg, "could not read its file `gone.parquet` (") == null) {
        std.debug.print("lanes: {s}\n", .{r.msg});
        return error.TestUnexpectedResult;
    }
}

test "an Iceberg folder is refused rather than read as a plain folder; format = 'parquet' reads its files" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try dataFile(a, &tmp, "ice/data/a.parquet", "id\n1\n2\n", "id");
    try put(&tmp, "ice/metadata/v1.metadata.json", "{}");
    try refused(a, &tmp, "SELECT id FROM '$B/ice/'", "holds an Iceberg table (metadata/*.metadata.json), which basalt does not read yet", false);
    try testing.expectEqualStrings("id\n1\n2\n", try rows(a, &tmp, "SELECT id FROM '$B/ice/' WITH (format = 'parquet') ORDER BY id", 1));
}
