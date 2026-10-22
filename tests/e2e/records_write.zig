//! End-to-end records sinks: one value a row written verbatim, gzipped, the same
//! bytes on one lane or four, and what is refused at plan time.

const std = @import("std");
const basalt = @import("basalt");
const Diag = basalt.env.Diag;
const checkAndRun = @import("harness.zig").checkAndRun;
const exec = @import("harness.zig").exec;
const expectRefusedAlike = @import("harness.zig").expectRefusedAlike;

fn bigCsv(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir, null_at: ?usize) !void {
    var text = std.array_list.Managed(u8).init(alloc);
    defer text.deinit();
    try text.appendSlice("id,name\n");
    for (0..20000) |i| {
        if (null_at == i) try text.writer().print("{d},\n", .{i}) else try text.writer().print("{d},n{d}\n", .{ i, (i * 31) % 997 });
    }
    try tmp.dir.writeFile(.{ .sub_path = "big.csv", .data = text.items });
}

test "a records file is each row's value and the newline, gzipped by a .gz name" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try checkAndRun(alloc, &tmp, "LOAD INTO '$B/o.ret.gz' WITH (format = 'records', newline = '\\r\\n') AS SELECT lpad(CAST(id AS STRING), 3, '0') || grp || lpad(CAST(amt AS STRING), 4, ' ') AS rec FROM '$B/a.csv' ORDER BY id;", 1, &.{});
    const gz = try tmp.dir.readFileAlloc(alloc, "o.ret.gz", 1 << 16);
    defer alloc.free(gz);
    var in: std.Io.Reader = .fixed(gz);
    var window: [std.compress.flate.max_window_len]u8 = undefined;
    var dec: std.compress.flate.Decompress = .init(&in, .gzip, &window);
    const got = try dec.reader.allocRemaining(alloc, .unlimited);
    defer alloc.free(got);
    try std.testing.expectEqualStrings("001a  10\r\n002a  20\r\n003b   5\r\n", got);
    try checkAndRun(alloc, &tmp, "LOAD INTO '$B/o.ret' WITH (format = 'records', newline = 'none') AS SELECT from_base64('QUI=') AS rec FROM '$B/a.csv';", 1, &.{});
    const raw = try tmp.dir.readFileAlloc(alloc, "o.ret", 1 << 16);
    defer alloc.free(raw);
    try std.testing.expectEqualStrings("ABABAB", raw);
}

test "a records file is byte for byte the same on one lane or four, sorted or in file order" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try bigCsv(alloc, &tmp, null);
    for ([_][]const u8{
        "LOAD INTO '$B/o.ret' WITH (format = 'records', newline = 'crlf') AS SELECT name || '|' || CAST(id AS STRING) AS rec FROM '$B/big.csv' ORDER BY name, id;",
        "LOAD INTO '$B/o.ret' WITH (format = 'records') AS SELECT name || CAST(id AS STRING) AS rec FROM '$B/big.csv' WHERE id % 3 <> 0;",
    }) |script| {
        var got: [2][]u8 = undefined;
        for ([_]usize{ 1, 4 }, 0..) |threads, k| {
            try checkAndRun(alloc, &tmp, script, threads, &.{});
            got[k] = try tmp.dir.readFileAlloc(alloc, "o.ret", 1 << 22);
        }
        defer for (got) |g| alloc.free(g);
        try std.testing.expectEqualStrings(got[0], got[1]);
        try std.testing.expect(std.mem.count(u8, got[0], "\n") > 13000);
    }
}

test "a NULL record is an error naming its row in the output" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try bigCsv(alloc, &tmp, 15000);
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}/o.ret' WITH (format = 'records') AS SELECT name FROM '{s}/big.csv';", .{ base, base });
    defer alloc.free(script);
    for ([_]usize{ 1, 4 }) |threads| {
        var rdiag: Diag = .{};
        try std.testing.expectError(error.RecordNull, exec(alloc, script, .{ .threads = threads, .log = .{ .quiet = true } }, &rdiag));
        try std.testing.expect(std.mem.indexOf(u8, basalt.eval.takeFailure(error.RecordNull).?, "row 15001 of the records output is NULL") != null);
    }
    try std.testing.expectError(error.FileNotFound, tmp.dir.access("o.ret", .{}));
}

test "a records sink takes one STRING or BYTES column, and a records file is not read" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "a.csv", .data = "id,grp\n1,a\n" });
    try expectRefusedAlike(alloc, &tmp, "LOAD INTO '$B/o.ret' WITH (format = 'records') AS SELECT id, grp FROM '$B/a.csv';", "this query yields 2 columns");
    try expectRefusedAlike(alloc, &tmp, "LOAD INTO '$B/o.ret' WITH (format = 'records') AS SELECT id FROM '$B/a.csv';", "`id` is int");
    try expectRefusedAlike(alloc, &tmp, "LOAD INTO '$B/o.ret' WITH (format = 'records', newline = 'tab') AS SELECT grp FROM '$B/a.csv';", "unknown `newline = 'tab'`");
    try expectRefusedAlike(alloc, &tmp, "SELECT * FROM '$B/a.csv' WITH (format = 'records');", "the `records` format is written, not read; read raw records with lines(), fixed(), split() or chunks()");
    try expectRefusedAlike(alloc, &tmp, "SELECT * FROM '$B/a.csv' WITH (format = 'records', encoding = 'latin1');", "the `records` format is written, not read");
    try expectRefusedAlike(alloc, &tmp, "SELECT * FROM '$B/' WITH (format = 'records');", "the `records` format is written, not read");
}
