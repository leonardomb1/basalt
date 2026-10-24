//! End-to-end Avro: a LOAD writing a container file under each codec and lane count,
//! read back typed by basalt; a folder of files read as one table; and what a write
//! refuses, in `check` and `run` alike.

const std = @import("std");
const harness = @import("harness.zig");

const testing = std.testing;
const gpa = testing.allocator;

fn many(a: std.mem.Allocator, tmp: *testing.TmpDir) !void {
    var csv = std.array_list.Managed(u8).init(a);
    try csv.appendSlice("id,grp,amt,day,note\n");
    for (0..20_000) |i| try csv.writer().print("{d},{s},{d}.{d:0>2},2026-01-{d:0>2},{s}\n", .{ i, if (i % 3 == 0) "a" else "b", i, i % 100, i % 28 + 1, if (i % 7 == 0) "" else "n\"ote" });
    try tmp.dir.writeFile(.{ .sub_path = "many.csv", .data = csv.items });
}

fn rows(a: std.mem.Allocator, tmp: *testing.TmpDir, query: []const u8) ![]const u8 {
    try harness.checkAndRun(gpa, tmp, try std.fmt.allocPrint(a, "LOAD INTO '$B/out.csv' AS {s};", .{query}), 1, &.{});
    return tmp.dir.readFileAlloc(a, "out.csv", 1 << 20);
}

test "an Avro file a LOAD writes reads back typed, under each codec and on 1 and 4 lanes" {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try many(a, &tmp);
    const load =
        \\LOAD INTO '$B/t.avro' WITH (compression = '{s}') AS
        \\SELECT id, grp, CAST(amt AS DECIMAL(12,2)) AS amt, CAST(day AS DATE) AS day,
        \\       CAST(day AS TIMESTAMP) AS ts, NULLIF(note, '') AS note, id % 2 = 0 AS even, amt * 1.5 AS f
        \\FROM '$B/many.csv';
    ;
    for ([_][]const u8{ "deflate", "snappy", "none" }) |codec| for ([_]usize{ 1, 4 }) |threads| {
        try harness.checkAndRun(gpa, &tmp, try std.fmt.allocPrint(a, load, .{codec}), threads, &.{});
        const got = try rows(a, &tmp,
            \\SELECT count(*) AS n, sum(id) AS s, sum(amt) AS total, max(day) AS last, min(ts) AS first,
            \\       count(note) AS notes, count_if(even) AS evens, min(f) AS f, count(DISTINCT grp) AS g
            \\FROM '$B/t.avro'
        );
        try testing.expectEqualStrings("n,s,total,last,first,notes,evens,f,g\n20000,199990000,199999900.00,2026-01-28,2026-01-01 00:00:00,17142,10000,0,2\n", got);
    };
}

test "a folder of Avro files is one table, a later file's columns matched by name" {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.makePath("d");
    try harness.checkAndRun(gpa, &tmp, "LOAD INTO '$B/d/1.avro' AS SELECT id, grp FROM '$B/a.csv';", 1, &.{});
    try harness.checkAndRun(gpa, &tmp, "LOAD INTO '$B/d/2.avro' AS SELECT amt, id FROM '$B/b.csv';", 1, &.{});
    const got = try rows(a, &tmp, "SELECT id, grp FROM '$B/d/' ORDER BY id");
    try testing.expectEqualStrings("id,grp\n1,a\n2,a\n3,b\n7,\n8,\n", got);
    const named = try rows(a, &tmp, "SELECT count(*) AS n FROM '$B/d/1.avro' WITH (format = 'avro')");
    try testing.expectEqualStrings("n\n3\n", named);
}

test "an Avro write refuses a gzip suffix, APPEND and zstandard in check and run alike" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "a.csv", .data = "id\n1\n" });
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/t.avro.gz' AS SELECT id FROM '$B/a.csv';", "Avro compresses its own blocks");
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/t.avro' APPEND AS SELECT id FROM '$B/a.csv';", "an Avro file's header fixes its schema");
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/t.avro' WITH (compression = 'zstandard') AS SELECT id FROM '$B/a.csv';", "does not write them");
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/t.avro' WITH (compression = 'lz4') AS SELECT id FROM '$B/a.csv';", "unknown Avro compression `lz4`");
    try harness.expectRefusedAlike(gpa, &tmp, "LOAD INTO '$B/t.csv' WITH (compression = 'snappy') AS SELECT id FROM '$B/a.csv';", "`compression` does not apply to a CSV file");
}
