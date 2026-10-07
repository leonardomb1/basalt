//! End-to-end window functions: ranking, offsets, aggregate frames and ROWS frames.

const std = @import("std");
const basalt = @import("basalt");
const parser = basalt.sql_parser;
const ParamArg = basalt.env.ParamArg;
const runToString = @import("harness.zig").runToString;
const runScript = @import("harness.zig").runScript;

test "windows: each function keeps its own frame; DISTINCT and * see the window's output once" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "f.csv", .data = "id,v\n1,10\n2,20\n3,30\n4,40\n" });
    try tmp.dir.writeFile(.{ .sub_path = "m.csv", .data = "k,v\na,1\na,2\nb,3\n,4\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const cases = [_]struct { q: []const u8, want: []const u8 }{
        .{
            .q = "SELECT id, SUM(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s, SUM(v) OVER (ORDER BY id) AS r FROM '$B/f.csv' ORDER BY id",
            .want = "id,s,r\n1,10,10\n2,30,30\n3,50,60\n4,70,100\n",
        },
        .{ .q = "SELECT DISTINCT k, SUM(v) OVER (PARTITION BY k) AS s FROM '$B/m.csv' ORDER BY k", .want = "k,s\na,3\nb,3\n,4\n" },
        .{ .q = "SELECT *, ROW_NUMBER() OVER (ORDER BY v) AS rn FROM '$B/m.csv' ORDER BY rn", .want = "k,v,rn\na,1,1\na,2,2\nb,3,3\n,4,4\n" },
        .{ .q = "SELECT * EXCEPT (v), SUM(v) OVER (ORDER BY v) AS rs FROM '$B/m.csv' ORDER BY rs", .want = "k,rs\na,1\na,3\nb,6\n,10\n" },
    };
    for (cases) |c| {
        const q = try std.mem.replaceOwned(u8, alloc, c.q, "$B", base);
        defer alloc.free(q);
        const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}/out.csv' AS {s};", .{ base, q });
        defer alloc.free(script);
        const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
        defer alloc.free(out);
        try std.testing.expectEqualStrings(c.want, out);
    }

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    try std.testing.expectError(error.ParseFailed, parser.parseSource(parena.allocator(), "SELECT k, SUM(v * 2) OVER (PARTITION BY k) AS a FROM 'm.csv';", &pdiag));
    try std.testing.expect(std.mem.indexOf(u8, pdiag.msg, "a window function takes a plain column") != null);
}

test "window: a column only named inside OVER survives the projection" {
    const alloc = std.testing.allocator;
    var t1 = std.testing.tmpDir(.{});
    defer t1.cleanup();
    const out = try runToString(
        alloc,
        &t1,
        "id,categoria,valor\n1,a,10\n2,a,20\n3,b,5\n",
        "SELECT LAG(valor) OVER (PARTITION BY categoria ORDER BY id) AS ant FROM '$IN'",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("ant\n\n10\n\n", out);

    var t2 = std.testing.tmpDir(.{});
    defer t2.cleanup();
    const sub = try runToString(
        alloc,
        &t2,
        "id,categoria,valor\n1,a,10\n2,a,20\n3,b,5\n",
        "SELECT SUM(ant) AS s FROM (SELECT LAG(valor) OVER (PARTITION BY categoria ORDER BY id) AS ant FROM '$IN') x",
    );
    defer alloc.free(sub);
    try std.testing.expectEqualStrings("s\n10\n", sub);
}

test "window: row_number, rank and dense_rank number within a partition" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "k,v\na,10\na,20\na,20\nb,5\nb,7\n",
        "SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v) AS rn FROM '$IN'",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("k,v,rn\na,10,1\na,20,2\na,20,3\nb,5,1\nb,7,2\n", out);
}

test "window: a tie holds rank and skips, dense_rank leaves no gap" {
    const alloc = std.testing.allocator;
    var t1 = std.testing.tmpDir(.{});
    defer t1.cleanup();
    const rk = try runToString(
        alloc,
        &t1,
        "v\n10\n20\n20\n30\n30\n40\n",
        "SELECT v, RANK() OVER (ORDER BY v) AS rk FROM '$IN'",
    );
    defer alloc.free(rk);
    try std.testing.expectEqualStrings("v,rk\n10,1\n20,2\n20,2\n30,4\n30,4\n40,6\n", rk);

    var t2 = std.testing.tmpDir(.{});
    defer t2.cleanup();
    const dr = try runToString(
        alloc,
        &t2,
        "v\n10\n20\n20\n30\n30\n40\n",
        "SELECT v, DENSE_RANK() OVER (ORDER BY v) AS dr FROM '$IN'",
    );
    defer alloc.free(dr);
    try std.testing.expectEqualStrings("v,dr\n10,1\n20,2\n20,2\n30,3\n30,3\n40,4\n", dr);
}

test "window: lag and lead stop at the partition edge" {
    const alloc = std.testing.allocator;
    var t1 = std.testing.tmpDir(.{});
    defer t1.cleanup();
    const lag = try runToString(
        alloc,
        &t1,
        "k,v\na,10\na,20\na,30\nb,5\nb,7\n",
        "SELECT k, v, LAG(v) OVER (PARTITION BY k ORDER BY v) AS prev FROM '$IN'",
    );
    defer alloc.free(lag);
    try std.testing.expectEqualStrings("k,v,prev\na,10,\na,20,10\na,30,20\nb,5,\nb,7,5\n", lag);

    var t2 = std.testing.tmpDir(.{});
    defer t2.cleanup();
    const lead = try runToString(
        alloc,
        &t2,
        "k,v\na,10\na,20\nb,5\n",
        "SELECT k, v, LEAD(v) OVER (PARTITION BY k ORDER BY v) AS nxt FROM '$IN'",
    );
    defer alloc.free(lead);
    try std.testing.expectEqualStrings("k,v,nxt\na,10,20\na,20,\nb,5,\n", lead);
}

test "window: an explicit lag offset skips that many rows" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "v\n10\n20\n30\n40\n",
        "SELECT v, LAG(v, 2) OVER (ORDER BY v) AS prev2 FROM '$IN'",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("v,prev2\n10,\n20,\n30,10\n40,20\n", out);
}

test "window: a lag result feeds an expression through a derived table" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "k,v\na,10\na,25\nb,5\nb,7\n",
        "SELECT k, v - prev AS delta FROM (SELECT k, v, LAG(v) OVER (PARTITION BY k ORDER BY v) AS prev FROM '$IN') x WHERE prev IS NOT NULL",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("k,delta\na,15\nb,2\n", out);
}

test "window: an aggregate frame is the partition, or the peers so far" {
    const alloc = std.testing.allocator;
    var t1 = std.testing.tmpDir(.{});
    defer t1.cleanup();
    const tot = try runToString(
        alloc,
        &t1,
        "k,v\na,10\na,20\na,20\nb,5\nb,7\n",
        "SELECT k, v, SUM(v) OVER (PARTITION BY k) AS tot FROM '$IN'",
    );
    defer alloc.free(tot);
    try std.testing.expectEqualStrings("k,v,tot\na,10,50\na,20,50\na,20,50\nb,5,12\nb,7,12\n", tot);

    var t2 = std.testing.tmpDir(.{});
    defer t2.cleanup();
    const running = try runToString(
        alloc,
        &t2,
        "k,v\na,10\na,20\na,20\nb,5\nb,7\n",
        "SELECT k, v, SUM(v) OVER (PARTITION BY k ORDER BY v) AS run FROM '$IN'",
    );
    defer alloc.free(running);
    try std.testing.expectEqualStrings("k,v,run\na,10,10\na,20,50\na,20,50\nb,5,5\nb,7,12\n", running);
}

test "window: COUNT(*) over a partition counts the partition's rows" {
    const alloc = std.testing.allocator;
    var t1 = std.testing.tmpDir(.{});
    defer t1.cleanup();
    const n = try runToString(
        alloc,
        &t1,
        "k,v\na,10\na,20\nb,5\n",
        "SELECT k, COUNT(*) OVER (PARTITION BY k) AS n FROM '$IN'",
    );
    defer alloc.free(n);
    try std.testing.expectEqualStrings("k,n\na,2\na,2\nb,1\n", n);
}

test "window: min, max and avg share one window in a single SELECT" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "k,v\na,10\na,20\nb,5\n",
        "SELECT k, v, MIN(v) OVER (PARTITION BY k) AS lo, MAX(v) OVER (PARTITION BY k) AS hi, AVG(v) OVER (PARTITION BY k) AS mean FROM '$IN'",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("k,v,lo,hi,mean\na,10,10,20,15\na,20,10,20,15\nb,5,5,5,5\n", out);
}

test "window: two different windows in one SELECT are refused" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "k,v\na,1\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);
    const script = try std.fmt.allocPrint(
        alloc,
        "LOAD INTO '{s}' AS SELECT k, MIN(v) OVER (PARTITION BY k) AS lo, MAX(v) OVER (ORDER BY v) AS hi FROM '{s}';",
        .{ out_path, in_path },
    );
    defer alloc.free(script);
    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    try std.testing.expectError(error.ParseFailed, parser.parseSource(parena.allocator(), script, &pdiag));
    try std.testing.expectEqualStrings("two window functions in one SELECT must share the same OVER (...) window", pdiag.msg);
}

test "window: a ROWS frame counts rows where the default counts peers" {
    const alloc = std.testing.allocator;
    var t1 = std.testing.tmpDir(.{});
    defer t1.cleanup();
    const rows = try runToString(
        alloc,
        &t1,
        "v\n10\n20\n20\n30\n",
        "SELECT v, SUM(v) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS run FROM '$IN'",
    );
    defer alloc.free(rows);
    try std.testing.expectEqualStrings("v,run\n10,10\n20,30\n20,50\n30,80\n", rows);

    var t2 = std.testing.tmpDir(.{});
    defer t2.cleanup();
    const range = try runToString(
        alloc,
        &t2,
        "v\n10\n20\n20\n30\n",
        "SELECT v, SUM(v) OVER (ORDER BY v) AS run FROM '$IN'",
    );
    defer alloc.free(range);
    try std.testing.expectEqualStrings("v,run\n10,10\n20,50\n20,50\n30,80\n", range);
}

test "window: a bounded ROWS frame is a moving window" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "v\n10\n20\n20\n30\n",
        "SELECT v, AVG(v) OVER (ORDER BY v ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS ma FROM '$IN'",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("v,ma\n10,10\n20,15\n20,20\n30,25\n", out);
}

test "window: a column named `rank` is still a column" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "rank,v\n7,1\n",
        "SELECT rank, v FROM '$IN'",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("rank,v\n7,1\n", out);
}

test "window ROWS frame slides: bounded sum/min/max/count per partition, nulls skipped" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const got = try runToString(alloc, &tmp, "k,i,v\n0,0,0\n0,2,4\n0,4,8\n0,6,2\n0,8,6\n1,1,7\n1,3,1\n1,5,\n1,7,9\n1,9,3\n",
        \\SELECT k, i, v,
        \\       SUM(v) OVER (PARTITION BY k ORDER BY i ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS s3,
        \\       MIN(v) OVER (PARTITION BY k ORDER BY i ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS mn3,
        \\       MAX(v) OVER (PARTITION BY k ORDER BY i ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS mx3,
        \\       COUNT(v) OVER (PARTITION BY k ORDER BY i ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS c3
        \\FROM '$IN' ORDER BY k, i
    );
    defer alloc.free(got);
    try std.testing.expectEqualStrings("k,i,v,s3,mn3,mx3,c3\n0,0,0,0,0,0,1\n0,2,4,4,0,4,2\n0,4,8,12,0,8,3\n0,6,2,14,2,8,3\n0,8,6,16,2,8,3\n1,1,7,7,7,7,1\n1,3,1,8,1,7,2\n1,5,,8,1,7,2\n1,7,9,10,1,9,2\n1,9,3,12,3,9,2\n", got);
}

test "window SUM over a DECIMAL is an exact DECIMAL in every frame, as the SUM aggregate is" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const input = "g,v\na,0.10\na,0.20\na,0.30\nb,1.05\n";
    const cases = [_][2][]const u8{
        .{ "SUM(d) OVER (PARTITION BY g)", "g,d,s\na,0.10,0.60\na,0.20,0.60\na,0.30,0.60\nb,1.05,1.05\n" },
        .{ "SUM(d) OVER (PARTITION BY g ORDER BY d)", "g,d,s\na,0.10,0.10\na,0.20,0.30\na,0.30,0.60\nb,1.05,1.05\n" },
        .{ "SUM(d) OVER (PARTITION BY g ORDER BY d ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)", "g,d,s\na,0.10,0.10\na,0.20,0.30\na,0.30,0.50\nb,1.05,1.05\n" },
    };
    for (cases) |c| {
        const q = try std.fmt.allocPrint(alloc, "SELECT g, d, {s} AS s FROM (SELECT g, CAST(v AS DECIMAL(10,2)) AS d FROM '$IN') x ORDER BY g, d", .{c[0]});
        defer alloc.free(q);
        const got = try runToString(alloc, &tmp, input, q);
        defer alloc.free(got);
        try std.testing.expectEqualStrings(c[1], got);
    }
}
