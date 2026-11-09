//! End-to-end QUALIFY, the collecting and ordered aggregates, grouping sets, PIVOT and
//! UNPIVOT, each answer checked at one lane and at four, as `-j` must not change it.

const std = @import("std");
const runCsvThreaded = @import("harness.zig").runCsvThreaded;
const basalt = @import("basalt");

const Case = struct { q: []const u8, want: []const u8 };

const sales =
    \\id,g,r,v
    \\1,a,x,10
    \\2,a,y,30
    \\3,a,x,20
    \\4,b,y,5
    \\5,b,y,
    \\6,c,x,7
    \\
;

fn expectCases(input: []const u8, cases: []const Case) !void {
    const alloc = std.testing.allocator;
    var failed = false;
    for (cases) |c| for ([_]usize{ 1, 4 }) |threads| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const out = runCsvThreaded(alloc, &tmp, input, c.q, threads) catch |e| {
            std.debug.print("-j {d}: {s}\n  failed: {s}\n", .{ threads, c.q, @errorName(e) });
            failed = true;
            continue;
        };
        defer alloc.free(out);
        if (!std.mem.eql(u8, c.want, out)) {
            std.debug.print("-j {d}: {s}\n  want:\n{s}\n  got:\n{s}\n", .{ threads, c.q, c.want, out });
            failed = true;
        }
    };
    try std.testing.expect(!failed);
}

test "QUALIFY filters on a window's result: inline, by alias, through an expression, beside WHERE and GROUP BY" {
    try expectCases(sales, &.{
        .{ .q = "SELECT id, g FROM '$IN' QUALIFY ROW_NUMBER() OVER (PARTITION BY g ORDER BY id DESC) = 1 ORDER BY g", .want = "id,g\n3,a\n5,b\n6,c\n" },
        .{ .q = "SELECT id, g, ROW_NUMBER() OVER (PARTITION BY g ORDER BY id) AS rn FROM '$IN' QUALIFY rn <= 1 ORDER BY id", .want = "id,g,rn\n1,a,1\n4,b,1\n6,c,1\n" },
        .{ .q = "SELECT id, ROW_NUMBER() OVER (ORDER BY id) * 10 AS t FROM '$IN' QUALIFY t > 40 ORDER BY id", .want = "id,t\n5,50\n6,60\n" },
        .{ .q = "SELECT id FROM '$IN' WHERE g <> 'c' QUALIFY RANK() OVER (ORDER BY g) = 1 ORDER BY id", .want = "id\n1\n2\n3\n" },
        .{ .q = "SELECT id FROM '$IN' QUALIFY ROW_NUMBER() OVER w = 2 WINDOW w AS (PARTITION BY g ORDER BY id) ORDER BY id", .want = "id\n2\n5\n" },
        .{ .q = "SELECT id FROM '$IN' WINDOW w AS (PARTITION BY g ORDER BY id) QUALIFY ROW_NUMBER() OVER w = 2 ORDER BY id", .want = "id\n2\n5\n" },
        .{ .q = "SELECT g, COUNT(*) AS n FROM '$IN' GROUP BY g QUALIFY RANK() OVER (ORDER BY COUNT(*) DESC) = 1", .want = "g,n\na,3\n" },
        .{ .q = "SELECT DISTINCT g FROM '$IN' QUALIFY ROW_NUMBER() OVER (PARTITION BY g ORDER BY id) = 1 ORDER BY g", .want = "g\na\nb\nc\n" },
        .{ .q = "SELECT *, ROW_NUMBER() OVER (PARTITION BY g ORDER BY id) AS rn FROM '$IN' QUALIFY rn = 2 ORDER BY id", .want = "id,g,r,v,rn\n2,a,y,30,2\n5,b,y,,2\n" },
    });
}

/// That `q` is refused while parsing, in words holding `says`.
fn expectParseRefused(q: []const u8, says: []const u8) !void {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const script = try std.fmt.allocPrint(ar.allocator(), "LOAD INTO 'o.csv' AS {s};", .{q});
    var pd: basalt.sql_parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    try std.testing.expectError(error.ParseFailed, basalt.sql_parser.parseSource(ar.allocator(), script, &pd));
    std.testing.expect(std.mem.indexOf(u8, pd.msg, says) != null) catch |e| {
        std.debug.print("{s}: {s}\n", .{ q, pd.msg });
        return e;
    };
}

test "QUALIFY with no window function in it is refused" {
    try expectParseRefused("SELECT id FROM 'a.csv' QUALIFY id = 1", "this one names none");
}

test "string_agg and array_agg: input order or their ORDER BY, FILTER, DISTINCT, NULLs" {
    try expectCases(sales, &.{
        .{ .q = "SELECT g, string_agg(id, '-') AS s FROM '$IN' GROUP BY g ORDER BY g", .want = "g,s\na,1-2-3\nb,4-5\nc,6\n" },
        .{ .q = "SELECT g, string_agg(id, '-' ORDER BY id DESC) AS s FROM '$IN' GROUP BY g ORDER BY g", .want = "g,s\na,3-2-1\nb,5-4\nc,6\n" },
        .{ .q = "SELECT g, string_agg(id, '-') WITHIN GROUP (ORDER BY id DESC) AS s FROM '$IN' GROUP BY g ORDER BY g", .want = "g,s\na,3-2-1\nb,5-4\nc,6\n" },
        .{ .q = "SELECT g, string_agg(v, ',') AS s FROM '$IN' GROUP BY g ORDER BY g", .want = "g,s\na,\"10,30,20\"\nb,5\nc,7\n" },
        .{ .q = "SELECT string_agg(DISTINCT r, ',' ORDER BY r) AS s FROM '$IN'", .want = "s\n\"x,y\"\n" },
        .{ .q = "SELECT g, string_agg(id, '-') FILTER (WHERE r = 'x') AS s FROM '$IN' GROUP BY g ORDER BY g", .want = "g,s\na,1-3\nb,\nc,6\n" },
        .{ .q = "SELECT g, cardinality(array_agg(id)) AS n FROM '$IN' GROUP BY g ORDER BY g", .want = "g,n\na,3\nb,2\nc,1\n" },
        .{ .q = "SELECT g, (array_agg(id ORDER BY id DESC))[1] AS top FROM '$IN' GROUP BY g ORDER BY g", .want = "g,top\na,3\nb,5\nc,6\n" },
        .{ .q = "SELECT g, cardinality(array_agg(v)) AS n FROM '$IN' WHERE g = 'b' GROUP BY g", .want = "g,n\nb,2\n" },
        .{ .q = "SELECT g, cardinality(array_agg(id) FILTER (WHERE r = 'y')) AS n FROM '$IN' GROUP BY g ORDER BY g", .want = "g,n\na,1\nb,2\nc,\n" },
        .{ .q = "SELECT x FROM (SELECT array_agg(id ORDER BY id) AS l FROM '$IN' WHERE g = 'a') t CROSS JOIN UNNEST(l) AS x", .want = "x\n1\n2\n3\n" },
    });
}

test "any_value, arg_max and arg_min: the first winner in input order, a NULL key never winning" {
    try expectCases(sales, &.{
        .{ .q = "SELECT g, any_value(r) AS a FROM '$IN' GROUP BY g ORDER BY g", .want = "g,a\na,x\nb,y\nc,x\n" },
        .{ .q = "SELECT g, arg_max(id, CAST(v AS INT)) AS a, arg_min(id, CAST(v AS INT)) AS b FROM '$IN' GROUP BY g ORDER BY g", .want = "g,a,b\na,2,1\nb,4,4\nc,6,6\n" },
        .{ .q = "SELECT g, max_by(r, id) AS a, min_by(r, id) AS b FROM '$IN' GROUP BY g ORDER BY g", .want = "g,a,b\na,x,x\nb,y,y\nc,x,x\n" },
        .{ .q = "SELECT arg_max(id, r) AS a FROM '$IN'", .want = "a\n2\n" },
    });
}

test "percentile_cont interpolates, percentile_disc picks a row, either way round" {
    try expectCases(sales, &.{
        .{ .q = "SELECT g, percentile_cont(0.5) WITHIN GROUP (ORDER BY CAST(v AS INT)) AS m FROM '$IN' GROUP BY g ORDER BY g", .want = "g,m\na,20\nb,5\nc,7\n" },
        .{ .q = "SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY CAST(id AS INT)) AS m FROM '$IN'", .want = "m\n2.25\n" },
        .{ .q = "SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY CAST(id AS INT) DESC) AS m FROM '$IN'", .want = "m\n4.75\n" },
        .{ .q = "SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY CAST(id AS INT)) AS m FROM '$IN'", .want = "m\n3\n" },
        .{ .q = "SELECT percentile_disc(0) WITHIN GROUP (ORDER BY r) AS lo, percentile_disc(1) WITHIN GROUP (ORDER BY r) AS hi FROM '$IN'", .want = "lo,hi\nx,y\n" },
        .{ .q = "SELECT g, percentile_cont(0.5) WITHIN GROUP (ORDER BY CAST(v AS INT)) FILTER (WHERE r = 'x') AS m FROM '$IN' GROUP BY g ORDER BY g", .want = "g,m\na,15\nb,\nc,7\n" },
    });
}

test "the new aggregates' arguments and window use are refused where they are written" {
    const cases = [_]struct { q: []const u8, says: []const u8 }{
        .{ .q = "SELECT string_agg(id) AS s FROM 'a.csv'", .says = "takes 2 arguments, not 1" },
        .{ .q = "SELECT arg_max(id) AS s FROM 'a.csv'", .says = "takes 2 arguments, not 1" },
        .{ .q = "SELECT arg_max(id, v ORDER BY v) AS s FROM 'a.csv'", .says = "takes no ORDER BY" },
        .{ .q = "SELECT percentile_cont(0.5) AS s FROM 'a.csv'", .says = "needs WITHIN GROUP" },
        .{ .q = "SELECT string_agg(id, ',') OVER (PARTITION BY g) AS s FROM 'a.csv'", .says = "not available over a window" },
        .{ .q = "SELECT any_value(id) OVER (PARTITION BY g) AS s FROM 'a.csv'", .says = "not available over a window" },
    };
    for (cases) |c| try expectParseRefused(c.q, c.says);
}

test "ROLLUP, CUBE and GROUPING SETS: a row per set, a key own NULL apart, GROUPING, keys inside aggregates, a plain key beside" {
    try expectCases(sales, &.{
        .{ .q = "SELECT g, r, COUNT(*) AS n FROM \x27$IN\x27 GROUP BY ROLLUP(g, r) ORDER BY g NULLS LAST, r NULLS LAST", .want = "g,r,n\na,x,2\na,y,1\na,,3\nb,y,2\nb,,2\nc,x,1\nc,,1\n,,6\n" },
        .{ .q = "SELECT g, r, COUNT(*) AS n FROM \x27$IN\x27 WHERE g <> \x27c\x27 GROUP BY CUBE(g, r) ORDER BY g NULLS LAST, r NULLS LAST", .want = "g,r,n\na,x,2\na,y,1\na,,3\nb,y,2\nb,,2\n,x,2\n,y,3\n,,5\n" },
        .{ .q = "SELECT g, r, COUNT(*) AS n FROM \x27$IN\x27 GROUP BY GROUPING SETS ((g), (r), ()) ORDER BY g NULLS LAST, r NULLS LAST", .want = "g,r,n\na,,3\nb,,2\nc,,1\n,x,3\n,y,3\n,,6\n" },
        .{ .q = "SELECT g, r, GROUPING(g) AS gg, GROUPING(g, r) AS gr, COUNT(*) AS n FROM \x27$IN\x27 GROUP BY ROLLUP(g, r) ORDER BY gr, g, r", .want = "g,r,gg,gr,n\na,x,0,0,2\na,y,0,0,1\nb,y,0,0,2\nc,x,0,0,1\na,,0,1,3\nb,,0,1,2\nc,,0,1,1\n,,1,3,6\n" },
        .{ .q = "SELECT g, r, COUNT(*) AS n FROM \x27$IN\x27 GROUP BY g, ROLLUP(r) ORDER BY g, r NULLS LAST", .want = "g,r,n\na,x,2\na,y,1\na,,3\nb,y,2\nb,,2\nc,x,1\nc,,1\n" },
        .{ .q = "SELECT r, MAX(r) AS m, COUNT(*) AS n FROM \x27$IN\x27 GROUP BY ROLLUP(r) ORDER BY r NULLS LAST", .want = "r,m,n\nx,x,3\ny,y,3\n,y,6\n" },
        .{ .q = "SELECT upper(g) AS u, COUNT(*) AS n FROM \x27$IN\x27 GROUP BY ROLLUP(upper(g)) ORDER BY u NULLS LAST", .want = "u,n\nA,3\nB,2\nC,1\n,6\n" },
        .{ .q = "SELECT g, COUNT(*) AS n FROM \x27$IN\x27 GROUP BY ROLLUP(g) HAVING COUNT(*) > 2 ORDER BY n", .want = "g,n\na,3\n,6\n" },
        .{ .q = "SELECT g, r, COUNT(*) AS n FROM \x27$IN\x27 GROUP BY ROLLUP((g, r)) ORDER BY g NULLS LAST, r", .want = "g,r,n\na,x,2\na,y,1\nb,y,2\nc,x,1\n,,6\n" },
        .{ .q = "SELECT g, string_agg(id, \x27-\x27 ORDER BY id) AS s FROM \x27$IN\x27 GROUP BY ROLLUP(g) ORDER BY g NULLS LAST", .want = "g,s\na,1-2-3\nb,4-5\nc,6\n,1-2-3-4-5-6\n" },
    });
    try expectCases("g,v\n,1\n,2\na,3\n", &.{
        .{ .q = "SELECT g, GROUPING(g) AS b, COUNT(*) AS n FROM \x27$IN\x27 GROUP BY ROLLUP(g) ORDER BY b, n", .want = "g,b,n\na,0,1\n,0,2\n,1,3\n" },
    });
}

test "GROUPING names only keys, and only under a grouping set" {
    try expectParseRefused("SELECT g, GROUPING(r) AS b, COUNT(*) AS n FROM \x27a.csv\x27 GROUP BY ROLLUP(g)", "not a grouping key");
}

test "PIVOT: one column per value, grouped on the columns it does not use, with AS, several aggregates, WHERE after" {
    const input = "g,r,v\na,x,1\na,y,2\na,x,3\nb,y,4\n";
    try expectCases(input, &.{
        .{ .q = "SELECT * FROM \x27$IN\x27 PIVOT (SUM(v) FOR r IN (\x27x\x27, \x27y\x27)) ORDER BY g", .want = "g,x,y\na,4,2\nb,,4\n" },
        .{ .q = "SELECT * FROM \x27$IN\x27 PIVOT (SUM(v) FOR r IN (\x27x\x27 AS ex, \x27y\x27)) AS p WHERE p.g = \x27a\x27", .want = "g,ex,y\na,4,2\n" },
        .{ .q = "SELECT g, x_s, y_n FROM \x27$IN\x27 PIVOT (SUM(v) AS s, COUNT(*) AS n FOR r IN (\x27x\x27, \x27y\x27)) ORDER BY g", .want = "g,x_s,y_n\na,4,1\nb,,1\n" },
        .{ .q = "SELECT * FROM (SELECT g, r FROM \x27$IN\x27) t PIVOT (COUNT(*) FOR r IN (\x27x\x27, \x27y\x27, \x27z\x27)) ORDER BY g", .want = "g,x,y,z\na,2,1,0\nb,0,1,0\n" },
        .{ .q = "SELECT * FROM \x27$IN\x27 PIVOT (string_agg(v, \x27-\x27 ORDER BY v DESC) FOR r IN (\x27x\x27)) ORDER BY g", .want = "g,x\na,3-1\nb,\n" },
        .{ .q = "SELECT * FROM (SELECT g, r, v FROM \x27$IN\x27 WHERE g = \x27a\x27) t PIVOT (MAX(v) FOR r IN (\x27x\x27, \x27y\x27))", .want = "g,x,y\na,3,2\n" },
    });
}

test "UNPIVOT: a row per column, NULLs out unless asked for, labels, the rest kept" {
    const input = "id,q1,q2\n1,10,20\n2,,40\n";
    try expectCases(input, &.{
        .{ .q = "SELECT * FROM \x27$IN\x27 UNPIVOT (amt FOR q IN (q1, q2)) ORDER BY id, q", .want = "id,q,amt\n1,q1,10\n1,q2,20\n2,q2,40\n" },
        .{ .q = "SELECT * FROM \x27$IN\x27 UNPIVOT INCLUDE NULLS (amt FOR q IN (q1, q2)) ORDER BY id, q", .want = "id,q,amt\n1,q1,10\n1,q2,20\n2,q1,\n2,q2,40\n" },
        .{ .q = "SELECT id, q, amt FROM \x27$IN\x27 UNPIVOT (amt FOR q IN (q1 AS \x27first\x27, q2 AS \x27second\x27)) AS u WHERE u.q = \x27second\x27 ORDER BY id", .want = "id,q,amt\n1,second,20\n2,second,40\n" },
        .{ .q = "SELECT q, SUM(amt) AS s FROM \x27$IN\x27 UNPIVOT (amt FOR q IN (q1, q2)) GROUP BY q ORDER BY q", .want = "q,s\nq1,10\nq2,60\n" },
    });
}

test "PIVOT takes aggregates, and names a column its input has" {
    try expectParseRefused("SELECT * FROM \x27a.csv\x27 PIVOT (v FOR r IN (\x27x\x27))", "takes aggregates");
    try expectParseRefused("SELECT * FROM \x27a.csv\x27 PIVOT (SUM(v) FOR r IN (1.5))", "name this PIVOT value");
}

test "WITH TIES keeps the rows tied with the last, by LIMIT or FETCH, over a kept or a hidden key" {
    const input = "id,s\n1,5\n2,9\n3,9\n4,7\n5,9\n6,7\n";
    try expectCases(input, &.{
        .{ .q = "SELECT id, CAST(s AS INT) AS s FROM \x27$IN\x27 ORDER BY s DESC, id LIMIT 2", .want = "id,s\n2,9\n3,9\n" },
        .{ .q = "SELECT id, CAST(s AS INT) AS n FROM \x27$IN\x27 ORDER BY n DESC LIMIT 1 WITH TIES", .want = "id,n\n2,9\n3,9\n5,9\n" },
        .{ .q = "SELECT id, CAST(s AS INT) AS n FROM \x27$IN\x27 ORDER BY n DESC FETCH FIRST 4 ROWS WITH TIES", .want = "id,n\n2,9\n3,9\n5,9\n4,7\n6,7\n" },
        .{ .q = "SELECT id FROM \x27$IN\x27 ORDER BY s DESC FETCH FIRST 1 ROW WITH TIES", .want = "id\n2\n3\n5\n" },
    });
    try expectParseRefused("SELECT id FROM \x27a.csv\x27 ORDER BY id LIMIT 1 WITH TIES OFFSET 1", "takes no OFFSET");
}

test "INTERSECT ALL and EXCEPT ALL count duplicates" {
    const input = "k,side\na,l\na,l\na,l\nb,l\nb,r\na,r\nc,r\na,r\n";
    const left = "SELECT k FROM \x27$IN\x27 WHERE side = \x27l\x27";
    const right = "SELECT k FROM \x27$IN\x27 WHERE side = \x27r\x27";
    const alloc = std.testing.allocator;
    const cases = [_]struct { op: []const u8, want: []const u8 }{
        .{ .op = "INTERSECT ALL", .want = "k\na\na\nb\n" },
        .{ .op = "EXCEPT ALL", .want = "k\na\n" },
        .{ .op = "INTERSECT", .want = "k\na\nb\n" },
        .{ .op = "EXCEPT", .want = "k\n" },
    };
    for (cases) |c| {
        const q = try std.fmt.allocPrint(alloc, "SELECT k FROM ({s} {s} {s}) t ORDER BY k", .{ left, c.op, right });
        defer alloc.free(q);
        try expectCases(input, &.{.{ .q = q, .want = c.want }});
    }
}

test "EXCLUDE takes rows out of a frame: the current row, its peers, or the peers but itself" {
    const input = "id,g,o,v\n1,a,1,10\n2,a,2,20\n3,a,2,30\n4,a,3,40\n5,b,1,5\n";
    const frame = "PARTITION BY g ORDER BY o ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING";
    try expectCases(input, &.{
        .{ .q = "SELECT id, SUM(CAST(v AS INT)) OVER (" ++ frame ++ " EXCLUDE NO OTHERS) AS s FROM \x27$IN\x27 ORDER BY id", .want = "id,s\n1,100\n2,100\n3,100\n4,100\n5,5\n" },
        .{ .q = "SELECT id, SUM(CAST(v AS INT)) OVER (" ++ frame ++ " EXCLUDE CURRENT ROW) AS s FROM \x27$IN\x27 ORDER BY id", .want = "id,s\n1,90\n2,80\n3,70\n4,60\n5,\n" },
        .{ .q = "SELECT id, SUM(CAST(v AS INT)) OVER (" ++ frame ++ " EXCLUDE GROUP) AS s FROM \x27$IN\x27 ORDER BY id", .want = "id,s\n1,90\n2,50\n3,50\n4,60\n5,\n" },
        .{ .q = "SELECT id, SUM(CAST(v AS INT)) OVER (" ++ frame ++ " EXCLUDE TIES) AS s FROM \x27$IN\x27 ORDER BY id", .want = "id,s\n1,100\n2,70\n3,80\n4,100\n5,5\n" },
        .{ .q = "SELECT id, COUNT(*) OVER (" ++ frame ++ " EXCLUDE GROUP) AS n FROM \x27$IN\x27 ORDER BY id", .want = "id,n\n1,3\n2,2\n3,2\n4,3\n5,0\n" },
        .{ .q = "SELECT id, MAX(v) OVER (" ++ frame ++ " EXCLUDE GROUP) AS m FROM \x27$IN\x27 ORDER BY id", .want = "id,m\n1,40\n2,40\n3,40\n4,30\n5,\n" },
        .{ .q = "SELECT id, FIRST_VALUE(id) OVER (" ++ frame ++ " EXCLUDE CURRENT ROW) AS a, LAST_VALUE(id) OVER (" ++ frame ++ " EXCLUDE GROUP) AS b, NTH_VALUE(id, 2) OVER (" ++ frame ++ " EXCLUDE TIES) AS c FROM \x27$IN\x27 ORDER BY id", .want = "id,a,b,c\n1,2,4,2\n2,1,4,2\n3,1,4,3\n4,1,3,2\n5,,,\n" },
    });
}

test "a grouping set of no keys answers over no rows, as a plain aggregate does; one with keys does not" {
    try expectCases(sales, &.{
        .{ .q = "SELECT g, COUNT(*) AS n, MAX(v) AS m FROM '$IN' WHERE g = 'none' GROUP BY ROLLUP(g)", .want = "g,n,m\n,0,\n" },
        .{ .q = "SELECT g, r, GROUPING(g) AS b, COUNT(*) AS n FROM '$IN' WHERE g = 'none' GROUP BY CUBE(g, r)", .want = "g,r,b,n\n,,1,0\n" },
        .{ .q = "SELECT g, COUNT(*) AS n FROM '$IN' WHERE g = 'none' GROUP BY GROUPING SETS ((g))", .want = "g,n\n" },
        .{ .q = "SELECT g, r, COUNT(*) AS n FROM '$IN' WHERE g = 'none' GROUP BY g, ROLLUP(r)", .want = "g,r,n\n" },
    });
}

test "the new forms answer the same at one lane, at four, and spilled to disk, over more rows than a chunk" {
    const alloc = std.testing.allocator;
    var input = std.array_list.Managed(u8).init(alloc);
    defer input.deinit();
    try input.appendSlice("id,g,r,v\n");
    for (0..20000) |i| try input.writer().print("{d},{c},{c},{d}\n", .{ i, "abcde"[(i * 7) % 5], "xyz"[(i * 11) % 3], (i * 37) % 201 });
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const spill = try std.fs.path.join(alloc, &.{ base, "spill" });
    defer alloc.free(spill);
    try tmp.dir.makeDir("spill");
    const queries = [_][]const u8{
        "SELECT g, string_agg(id, ',') AS s FROM '$IN' GROUP BY g ORDER BY g",
        "SELECT g, string_agg(id, ',' ORDER BY CAST(v AS INT), id) AS s FROM '$IN' GROUP BY g ORDER BY g",
        "SELECT g, r, cardinality(array_agg(id)) AS n, (array_agg(id ORDER BY id DESC))[1] AS top FROM '$IN' GROUP BY g, r ORDER BY g, r",
        "SELECT g, any_value(id) AS a, arg_max(id, CAST(v AS INT)) AS b, arg_min(id, CAST(v AS INT)) AS c FROM '$IN' GROUP BY g ORDER BY g",
        "SELECT g, percentile_cont(0.9) WITHIN GROUP (ORDER BY CAST(v AS INT)) AS p, percentile_disc(0.5) WITHIN GROUP (ORDER BY CAST(v AS INT) DESC) AS d FROM '$IN' GROUP BY g ORDER BY g",
        "SELECT g, r, GROUPING(g, r) AS b, COUNT(*) AS n, SUM(CAST(v AS INT)) AS s FROM '$IN' GROUP BY CUBE(g, r) ORDER BY b, g, r",
        "SELECT * FROM (SELECT g, r, CAST(v AS INT) AS v FROM '$IN') t PIVOT (SUM(v) FOR r IN ('x', 'y', 'z')) ORDER BY g",
        "SELECT id, g FROM '$IN' QUALIFY ROW_NUMBER() OVER (PARTITION BY g ORDER BY CAST(v AS INT) DESC, id) <= 2 ORDER BY g, id",
    };
    var failed = false;
    for (queries) |q| {
        var tmp_a = std.testing.tmpDir(.{});
        defer tmp_a.cleanup();
        const one = try runCsvThreaded(alloc, &tmp_a, input.items, q, 1);
        defer alloc.free(one);
        const four = try runCsvThreaded(alloc, &tmp_a, input.items, q, 4);
        defer alloc.free(four);
        const in_path = try tmp_a.dir.realpathAlloc(alloc, "in.csv");
        defer alloc.free(in_path);
        const pointed = try std.mem.replaceOwned(u8, alloc, q, "$IN", in_path);
        defer alloc.free(pointed);
        const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}/s.csv' AS {s};", .{ base, pointed });
        defer alloc.free(script);
        var diag: basalt.env.Diag = .{};
        _ = try @import("harness.zig").exec(alloc, script, .{ .op_memory = 1, .spill_dir = spill }, &diag);
        const spilled = try tmp.dir.readFileAlloc(alloc, "s.csv", 1 << 24);
        defer alloc.free(spilled);
        if (!std.mem.eql(u8, one, four) or !std.mem.eql(u8, one, spilled)) {
            std.debug.print("differs: {s}\n", .{q});
            failed = true;
        }
    }
    try std.testing.expect(!failed);
}

test "array_agg holds nested values and indexes without parentheses; a PIVOT over a column its input lacks is refused" {
    try expectCases(sales, &.{
        .{ .q = "SELECT g, array_agg(id ORDER BY id DESC)[1] AS top FROM '$IN' GROUP BY g ORDER BY g", .want = "g,top\na,3\nb,5\nc,6\n" },
        .{ .q = "SELECT (array_agg(CAST('{\"a\":1}' AS STRUCT(a INT))))[1].a AS x FROM '$IN' WHERE g = 'c'", .want = "x\n1\n" },
        .{ .q = "SELECT cardinality((array_agg(CAST('[1,2]' AS LIST(INT))))[1]) AS x FROM '$IN' WHERE g = 'c'", .want = "x\n2\n" },
    });
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try std.testing.expectError(error.PlanFailed, runCsvThreaded(alloc, &tmp, sales, "SELECT * FROM '$IN' PIVOT (SUM(v) FOR nope IN ('a'))", 1));
}
