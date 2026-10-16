//! End-to-end JSON and NDJSON reads: the three shapes, a folder, `items`, lanes against
//! a serial read, and what is refused.

const std = @import("std");
const basalt = @import("basalt");
const parser = basalt.sql_parser;
const Diag = basalt.env.Diag;
const run = basalt.runtime.run;
const checkAndRun = @import("harness.zig").checkAndRun;
const expectRefusedAlike = @import("harness.zig").expectRefusedAlike;

fn out(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir) ![]u8 {
    return tmp.dir.readFileAlloc(alloc, "out.csv", 1 << 20);
}

test "NDJSON, a JSON array and a lone object load the same rows, columns resolved before the run" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "a.ndjson", .data = "{\"id\":1,\"name\":\"x\",\"tags\":[1,2]}\n{\"id\":2,\"name\":\"y\"}\n" });
    try tmp.dir.writeFile(.{ .sub_path = "a.json", .data = "[{\"id\":1,\"name\":\"x\",\"tags\":[1,2]},\n {\"id\":2,\"name\":\"y\"}]" });
    try tmp.dir.writeFile(.{ .sub_path = "one.json", .data = "{\"id\":1,\"name\":\"x\",\"tags\":[1,2]}" });
    const want = "id,name,tags\n1,x,\"[1,2]\"\n2,y,\n";
    for ([_][]const u8{ "a.ndjson", "a.json" }) |f| {
        const script = try std.fmt.allocPrint(alloc, "LOAD INTO '$B/out.csv' AS SELECT id, name, tags FROM '$B/{s}' ORDER BY id;", .{f});
        defer alloc.free(script);
        try checkAndRun(alloc, &tmp, script, 1, &.{});
        const got = try out(alloc, &tmp);
        defer alloc.free(got);
        try std.testing.expectEqualStrings(want, got);
    }
    try checkAndRun(alloc, &tmp, "LOAD INTO '$B/out.csv' AS SELECT id + 1 AS n, json_get(tags, '1') AS t FROM '$B/one.json';", 1, &.{});
    const got = try out(alloc, &tmp);
    defer alloc.free(got);
    try std.testing.expectEqualStrings("n,t\n2,2\n", got);
}

test "a folder of JSON files is one table; items picks a nested array" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.makePath("d");
    try tmp.dir.writeFile(.{ .sub_path = "d/1.jsonl", .data = "{\"k\":1,\"v\":\"a\"}\n" });
    try tmp.dir.writeFile(.{ .sub_path = "d/2.jsonl", .data = "{\"v\":\"b\",\"k\":2,\"new\":true}\n{\"k\":3}\n" });
    try tmp.dir.writeFile(.{ .sub_path = "doc.json", .data = "{\"meta\":{\"n\":2},\"data\":{\"rows\":[{\"k\":10},{\"k\":20}]}}" });
    try checkAndRun(alloc, &tmp, "LOAD INTO '$B/out.csv' AS SELECT k, v FROM '$B/d/' ORDER BY k;", 1, &.{});
    const got = try out(alloc, &tmp);
    defer alloc.free(got);
    try std.testing.expectEqualStrings("k,v\n1,a\n2,b\n3,\n", got);
    try checkAndRun(alloc, &tmp, "LOAD INTO '$B/out.csv' AS SELECT sum(k) AS s FROM '$B/doc.json' WITH (items = 'data.rows');", 1, &.{});
    const s = try out(alloc, &tmp);
    defer alloc.free(s);
    try std.testing.expectEqualStrings("s\n30\n", s);
}

test "an NDJSON file read on four lanes gives what one lane does, in file order" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var text = std.array_list.Managed(u8).init(alloc);
    defer text.deinit();
    for (0..20000) |i| try text.writer().print("{{\"id\":{d},\"g\":\"g{d}\",\"x\":{d}}}\n", .{ i, i % 7, (i * 31) % 997 });
    try tmp.dir.writeFile(.{ .sub_path = "big.ndjson", .data = text.items });
    var got: [2][]u8 = undefined;
    for ([_]usize{ 1, 4 }, 0..) |threads, k| {
        try checkAndRun(alloc, &tmp, "LOAD INTO '$B/out.csv' AS SELECT id, g FROM '$B/big.ndjson' WHERE x > 500;", threads, &.{});
        got[k] = try out(alloc, &tmp);
    }
    defer for (got) |g| alloc.free(g);
    try std.testing.expectEqualStrings(got[0], got[1]);
    try std.testing.expect(std.mem.count(u8, got[0], "\n") > 9000);
}

test "a JSON file is not written, and STDIN is refused when stdin carries something else" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try expectRefusedAlike(alloc, &tmp, "LOAD INTO '$B/o.json' AS SELECT 1 AS a;", "basalt reads JSON but does not write it");

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), "SELECT * FROM STDIN WITH (format = 'ndjson');", &pdiag);
    var rdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, run(alloc, prog, .{ .log = .{ .quiet = true } }, &rdiag));
    try std.testing.expect(std.mem.indexOf(u8, rdiag.msg, "FROM STDIN needs stdin free") != null);
}
