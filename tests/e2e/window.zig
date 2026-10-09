//! End-to-end window functions: ranking, offsets, value and distribution functions,
//! ROWS and RANGE frames, IGNORE NULLS, FILTER, windows inside expressions, several
//! OVER clauses, windows over a GROUP BY, and the parallel paths before a window.

const std = @import("std");
const basalt = @import("basalt");
const ParamArg = basalt.env.ParamArg;
const expectRefusedAlike = @import("harness.zig").expectRefusedAlike;
const runCsvThreaded = @import("harness.zig").runCsvThreaded;
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
        .{ .q = "SELECT k, SUM(v * 2) OVER (PARTITION BY k) AS a FROM '$B/m.csv' ORDER BY k, a", .want = "k,a\na,6\na,6\nb,6\n,8\n" },
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

test "window: two different windows in one SELECT each sort their own input; columns keep the SELECT order" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "k,v\na,1\nb,5\na,3\nb,2\n",
        "SELECT k, v, MAX(v) OVER (ORDER BY v) AS hi, MIN(v) OVER (PARTITION BY k) AS lo, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v DESC) AS rn FROM '$IN' ORDER BY k, v",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("k,v,hi,lo,rn\na,1,1,1,2\na,3,3,1,1\nb,2,2,2,2\nb,5,5,2,1\n", out);
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

test "window columns come out where the SELECT list puts them, beside *, t.* or other items" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const input = "v,t\n1,a\n2,b\n";
    const cases = [_][2][]const u8{
        .{ "SELECT v, LAG(v) OVER (ORDER BY v) AS prev, t FROM '$IN' ORDER BY v", "v,prev,t\n1,,a\n2,1,b\n" },
        .{ "SELECT ROW_NUMBER() OVER (ORDER BY v) AS rn, v * 2 AS dbl FROM '$IN' ORDER BY rn", "rn,dbl\n1,2\n2,4\n" },
        .{ "SELECT ROW_NUMBER() OVER (ORDER BY v) AS rn, * FROM '$IN' ORDER BY rn", "rn,v,t\n1,1,a\n2,2,b\n" },
        .{ "SELECT x.*, ROW_NUMBER() OVER (ORDER BY v) AS rn FROM '$IN' x ORDER BY rn", "v,t,rn\n1,a,1\n2,b,2\n" },
    };
    for (cases) |c| {
        const got = try runToString(alloc, &tmp, input, c[0]);
        defer alloc.free(got);
        try std.testing.expectEqualStrings(c[1], got);
    }
}

test "LAG and LEAD take a default for the rows past the partition's edge, cast to the column's type" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const got = try runToString(alloc, &tmp, "g,v\na,1.50\na,2.25\nb,4.00\n",
        \\SELECT g, d, LAG(d, 1, 0) OVER (PARTITION BY g ORDER BY d) AS prev,
        \\       LEAD(d, 1, -1) OVER (PARTITION BY g ORDER BY d) AS nxt
        \\FROM (SELECT g, CAST(v AS DECIMAL(10,2)) AS d FROM '$IN') x ORDER BY g, d
    );
    defer alloc.free(got);
    try std.testing.expectEqualStrings("g,d,prev,nxt\na,1.50,0.00,2.25\na,2.25,1.50,-1.00\nb,4.00,0.00,-1.00\n", got);
}

const Case = struct { q: []const u8, want: []const u8 };

const sales_csv = "id,k,t,v\n1,a,1,10\n2,a,2,\n3,a,2,30\n4,a,5,40\n5,b,1,5\n6,b,3,\n7,b,4,7\n";
const days_csv = "day,amt\n2024-01-01,1\n2024-01-02,2\n2024-01-02,3\n2024-01-05,4\n2024-01-06,5\n";

/// Each case's query over `$B/s.csv` (`sales_csv`) and `$B/d.csv` (`days_csv`),
/// loaded into a CSV and compared whole.
fn expectCases(cases: []const Case) !void {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "s.csv", .data = sales_csv });
    try tmp.dir.writeFile(.{ .sub_path = "d.csv", .data = days_csv });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    for (cases) |c| {
        const q = try std.mem.replaceOwned(u8, alloc, c.q, "$B", base);
        defer alloc.free(q);
        const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}/out.csv' AS {s};", .{ base, q });
        defer alloc.free(script);
        const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
        defer alloc.free(out);
        std.testing.expectEqualStrings(c.want, out) catch |e| {
            std.debug.print("query: {s}\n", .{c.q});
            return e;
        };
    }
}

test "window frames: ROWS and RANGE, every bound, offsets past the partition's edges" {
    try expectCases(&.{
        .{
            .q = "SELECT id, SUM(v) OVER (PARTITION BY k ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS c3, SUM(v) OVER (PARTITION BY k ORDER BY id ROWS 2 PRECEDING) AS p2, COUNT(v) OVER (PARTITION BY k ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS rest, SUM(v) OVER (PARTITION BY k ORDER BY id ROWS BETWEEN 2 FOLLOWING AND 5 FOLLOWING) AS ahead FROM '$B/s.csv' ORDER BY id",
            .want = "id,c3,p2,rest,ahead\n1,10,10,3,70\n2,40,10,2,40\n3,70,40,2,\n4,70,70,1,\n5,5,5,2,7\n6,12,5,1,\n7,7,12,1,\n",
        },
        .{
            .q = "SELECT id, t, SUM(v) OVER (PARTITION BY k ORDER BY t RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS r1, COUNT(*) OVER (PARTITION BY k ORDER BY t RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS f2, SUM(v) OVER (PARTITION BY k ORDER BY t DESC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS above FROM '$B/s.csv' ORDER BY id",
            .want = "id,t,r1,f2,above\n1,1,10,3,70\n2,2,40,2,40\n3,2,40,2,40\n4,5,40,1,\n5,1,5,2,7\n6,3,,2,7\n7,4,7,1,\n",
        },
        .{
            .q = "SELECT day, SUM(amt) OVER (ORDER BY day RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS two_days, SUM(amt) OVER (ORDER BY day RANGE BETWEEN INTERVAL '3' DAY PRECEDING AND INTERVAL '1' DAY FOLLOWING) AS wide FROM '$B/d.csv' ORDER BY day, amt",
            .want = "day,two_days,wide\n2024-01-01,1,6\n2024-01-02,6,6\n2024-01-02,6,6\n2024-01-05,4,14\n2024-01-06,9,9\n",
        },
    });
}

test "window functions: value functions, distribution, and every aggregate over a window" {
    try expectCases(&.{
        .{
            .q = "SELECT id, FIRST_VALUE(v) OVER w AS fv, LAST_VALUE(v) OVER w AS lv_default, LAST_VALUE(v) OVER (w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lv_all, NTH_VALUE(v, 2) OVER (w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second FROM '$B/s.csv' WINDOW w AS (PARTITION BY k ORDER BY t) ORDER BY id",
            .want = "id,fv,lv_default,lv_all,second\n1,10,10,40,\n2,10,30,40,\n3,10,30,40,\n4,10,40,40,\n5,5,5,7,\n6,5,,7,\n7,5,7,7,\n",
        },
        .{
            .q = "SELECT id, NTILE(3) OVER (ORDER BY id) AS nt, PERCENT_RANK() OVER (PARTITION BY k ORDER BY t) AS pr, CUME_DIST() OVER (PARTITION BY k ORDER BY t) AS cd, DENSE_RANK() OVER (PARTITION BY k ORDER BY t) AS dr FROM '$B/s.csv' ORDER BY id",
            .want = "id,nt,pr,cd,dr\n1,1,0,0.25,1\n2,1,0.3333333333333333,0.75,2\n3,1,0.3333333333333333,0.75,2\n4,2,1,1,3\n5,2,0,0.3333333333333333,1\n6,3,0.5,0.6666666666666666,2\n7,3,1,1,3\n",
        },
        .{
            .q = "SELECT k, id, MEDIAN(v) OVER (PARTITION BY k) AS med, STDDEV(v) OVER (PARTITION BY k) AS sd, VAR_POP(v) OVER (PARTITION BY k) AS vp, COUNT(DISTINCT t) OVER (PARTITION BY k) AS dt, BOOL_AND(v > 6) OVER (PARTITION BY k) AS all_big, BOOL_OR(v > 30) OVER (PARTITION BY k ORDER BY id) AS any_big_yet FROM '$B/s.csv' ORDER BY id",
            .want = "k,id,med,sd,vp,dt,all_big,any_big_yet\na,1,30,15.275252316519467,155.55555555555557,3,true,false\na,2,30,15.275252316519467,155.55555555555557,3,true,false\na,3,30,15.275252316519467,155.55555555555557,3,true,false\na,4,30,15.275252316519467,155.55555555555557,3,true,true\nb,5,6,1.4142135623730951,1,3,false,false\nb,6,6,1.4142135623730951,1,3,false,false\nb,7,6,1.4142135623730951,1,3,false,false\n",
        },
    });
}

test "window IGNORE NULLS: LAG, LEAD, FIRST_VALUE and LAST_VALUE skip nulls, after the call or inside it" {
    try expectCases(&.{.{
        .q = "SELECT id, v, LAG(v) OVER (PARTITION BY k ORDER BY id) AS lag_r, LAG(v) IGNORE NULLS OVER (PARTITION BY k ORDER BY id) AS lag_i, LEAD(v IGNORE NULLS) OVER (PARTITION BY k ORDER BY id) AS lead_i, FIRST_VALUE(v) IGNORE NULLS OVER (PARTITION BY k ORDER BY id DESC) AS fv_i, LAST_VALUE(v) IGNORE NULLS OVER (PARTITION BY k ORDER BY id) AS carried FROM '$B/s.csv' ORDER BY id",
        .want = "id,v,lag_r,lag_i,lead_i,fv_i,carried\n1,10,,,30,40,10\n2,,10,10,30,40,10\n3,30,,10,40,40,30\n4,40,30,30,,40,40\n5,5,,,7,7,5\n6,,5,5,7,7,5\n7,7,,5,,7,7\n",
    }});
}

test "FILTER (WHERE …): under GROUP BY and HAVING, over the whole table, and over a window" {
    try expectCases(&.{
        .{
            .q = "SELECT k, SUM(v) FILTER (WHERE t > 1) AS late, COUNT(*) FILTER (WHERE v IS NULL) AS missing, COUNT(DISTINCT t) FILTER (WHERE v > 6) AS dt, AVG(v) FILTER (WHERE id <> 4) AS a FROM '$B/s.csv' GROUP BY k HAVING SUM(v) FILTER (WHERE t > 1) > 10 ORDER BY k",
            .want = "k,late,missing,dt,a\na,70,1,3,20\n",
        },
        .{
            .q = "SELECT COUNT(*) FILTER (WHERE v > 6) AS big, MAX(id) FILTER (WHERE v IS NULL) AS last_missing FROM '$B/s.csv'",
            .want = "big,last_missing\n4,6\n",
        },
        .{
            .q = "SELECT id, SUM(v) FILTER (WHERE t <= 2) OVER (PARTITION BY k ORDER BY id) AS early_run, COUNT(*) FILTER (WHERE v IS NOT NULL) OVER (PARTITION BY k) AS n FROM '$B/s.csv' ORDER BY id",
            .want = "id,early_run,n\n1,10,3\n2,10,3\n3,40,3\n4,40,3\n5,5,2\n6,5,2\n7,5,2\n",
        },
    });
}

test "windows in expressions, ORDER BY, expression keys and arguments, and over a GROUP BY" {
    try expectCases(&.{
        .{
            .q = "SELECT id, ROW_NUMBER() OVER (PARTITION BY k ORDER BY id) + 100 AS rn, v - LAG(v) OVER (PARTITION BY k ORDER BY id) AS delta, CASE WHEN RANK() OVER (PARTITION BY k ORDER BY v DESC) = 1 THEN 'top' ELSE '-' END AS best, ROUND(AVG(v) OVER (PARTITION BY k), 2) AS mean FROM '$B/s.csv' ORDER BY ROW_NUMBER() OVER (PARTITION BY k ORDER BY id) + 100, id",
            .want = "id,rn,delta,best,mean\n1,101,,-,26.67\n5,101,,-,6\n2,102,,-,26.67\n6,102,,-,6\n3,103,,-,26.67\n7,103,,top,6\n4,104,10,top,26.67\n",
        },
        .{
            .q = "SELECT id, SUM(v * t) OVER (PARTITION BY substr(k, 1, 1) ORDER BY t + id) AS weighted, MAX(v) OVER (ORDER BY id % 3, id) AS m, MIN(t) OVER (PARTITION BY v IS NULL) AS mt FROM '$B/s.csv' ORDER BY id",
            .want = "id,weighted,m,mt\n1,10,30,1\n2,10,40,2\n3,70,30,1\n4,270,40,1\n5,5,40,1\n6,5,30,2\n7,33,40,1\n",
        },
        .{
            .q = "SELECT k, SUM(v) AS s, SUM(SUM(v)) OVER () AS total, ROUND(100.0 * SUM(v) / SUM(SUM(v)) OVER (), 1) AS pct, RANK() OVER (ORDER BY SUM(v) DESC) AS rk FROM '$B/s.csv' GROUP BY k ORDER BY rk",
            .want = "k,s,total,pct,rk\na,80,92,87,1\nb,12,92,13,2\n",
        },
        .{
            .q = "SELECT * FROM (SELECT id, k, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v DESC) AS rn FROM '$B/s.csv') r WHERE rn <= 2 ORDER BY id",
            .want = "id,k,rn\n3,a,2\n4,a,1\n5,b,2\n7,b,1\n",
        },
    });
}

test "windows check like they run: a RANGE offset's key type, an INTERVAL's, a window's argument type" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "s.csv", .data = sales_csv });
    try expectRefusedAlike(alloc, &tmp, "SELECT SUM(v) OVER (ORDER BY k RANGE 1 PRECEDING) AS s FROM '$B/s.csv';", "RANGE with an offset needs a numeric, date or timestamp ORDER BY key; `k` is string");
    try expectRefusedAlike(alloc, &tmp, "SELECT SUM(v) OVER (ORDER BY t RANGE INTERVAL '1' DAY PRECEDING) AS s FROM '$B/s.csv';", "an INTERVAL frame offset needs a date or timestamp ORDER BY key");
    try expectRefusedAlike(alloc, &tmp, "SELECT BOOL_AND(v) OVER (ORDER BY t) AS s FROM '$B/s.csv';", "`bool_and` needs a BOOL argument");
    try expectRefusedAlike(alloc, &tmp, "SELECT SUM(nope * 2) OVER (ORDER BY t) AS s FROM '$B/s.csv';", "unknown field `nope`");
    try expectRefusedAlike(alloc, &tmp, "SELECT SUM(v) OVER (PARTITION BY nope) AS s FROM '$B/s.csv';", "unknown field `nope`");
}

/// A CSV large enough to split across lanes: an id, a group, and a value that is
/// sometimes missing.
fn laneInput(alloc: std.mem.Allocator) ![]u8 {
    var out = std.array_list.Managed(u8).init(alloc);
    try out.appendSlice("id,g,v\n");
    var prng = std.Random.DefaultPrng.init(42);
    const rnd = prng.random();
    for (0..6000) |i| {
        const g = "abcde"[rnd.uintLessThan(usize, 5)];
        if (rnd.uintLessThan(u8, 10) == 0) {
            try out.writer().print("{d},{c},\n", .{ i, g });
        } else {
            try out.writer().print("{d},{c},{d}\n", .{ i, g, rnd.intRangeAtMost(i64, -100, 100) });
        }
    }
    return out.toOwnedSlice();
}

test "windows after parallel stages: -j 4 writes what -j 1 does" {
    const alloc = std.testing.allocator;
    const input = try laneInput(alloc);
    defer alloc.free(input);
    const queries = [_][]const u8{
        "SELECT g, SUM(v) AS s, SUM(SUM(v)) OVER () AS tot, RANK() OVER (ORDER BY SUM(v) DESC, g) AS rk, COUNT(*) FILTER (WHERE v > 50) AS big FROM '$IN' GROUP BY g ORDER BY g",
        "SELECT id, SUM(v) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS s, v - LAG(v) IGNORE NULLS OVER (PARTITION BY g ORDER BY id) AS d, MAX(v) OVER (ORDER BY id % 7, id) AS m, COUNT(DISTINCT v) FILTER (WHERE v > 0) OVER (PARTITION BY g) AS pos FROM '$IN' WHERE id % 5 <> 0 ORDER BY id",
    };
    for (queries) |q| {
        var t1 = std.testing.tmpDir(.{});
        defer t1.cleanup();
        const serial = try runCsvThreaded(alloc, &t1, input, q, 1);
        defer alloc.free(serial);
        var t4 = std.testing.tmpDir(.{});
        defer t4.cleanup();
        const lanes = try runCsvThreaded(alloc, &t4, input, q, 4);
        defer alloc.free(lanes);
        try std.testing.expect(std.mem.count(u8, serial, "\n") > 5);
        try std.testing.expectEqualStrings(serial, lanes);
    }
}
