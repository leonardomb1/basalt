//! End-to-end aggregates: grouped and global, across batches, the numeric and
//! statistical functions, and GROUP BY and DISTINCT spilling past a tiny
//! `op_memory` with the same result as in memory.

const std = @import("std");
const basalt = @import("basalt");
const parser = basalt.sql_parser;
const Diag = basalt.env.Diag;
const ParamArg = basalt.env.ParamArg;
const runToString = @import("harness.zig").runToString;
const exec = @import("harness.zig").exec;
const runCsvThreaded = @import("harness.zig").runCsvThreaded;
const runScript = @import("harness.zig").runScript;
const runScriptOpts = @import("harness.zig").runScriptOpts;

test "SUM/AVG over an outer join's null fill, and integer sums that leave i64" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "l.csv", .data = "k,v\n1,a\n2,b\n3,c\n4,d\n" });
    try tmp.dir.writeFile(.{ .sub_path = "r.csv", .data = "k,n\n1,100\n2,7\n" });
    try tmp.dir.writeFile(.{ .sub_path = "big.csv", .data = "g,x\n1,9223372036854775807\n1,9223372036854775807\n1,-9223372036854775807\n1,-9223372036854775807\n2,5\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const cases = [_]struct { q: []const u8, want: []const u8 }{
        .{
            .q = "SELECT SUM(r.n) AS s, AVG(r.n) AS a FROM '$B/l.csv' l LEFT JOIN (SELECT * FROM '$B/r.csv') r ON l.k = r.k",
            .want = "s,a\n107,53.5\n",
        },
        .{ .q = "SELECT SUM(x) AS s FROM '$B/big.csv'", .want = "s\n5\n" },
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

    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}/out.csv' AS SELECT g, x, SUM(x) OVER (ORDER BY g, x) AS s FROM '{s}/big.csv';", .{ base, base });
    defer alloc.free(script);
    var rdiag: Diag = .{};
    try std.testing.expectError(error.IntOverflow, exec(alloc, script, .{}, &rdiag));
}

test "aggregate: two aggregates over different expressions stay separate" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "v\n10\n20\n30\n",
        "SELECT SUM(v * 2) + SUM(v + 100) AS chk, MIN(v * 2) + MIN(v + 100) AS m FROM '$IN'",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("chk,m\n480,130\n", out);
}

test "aggregate: HAVING reads an aggregate inside CASE, and a constant CASE sits beside one" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cases = [_]struct { q: []const u8, want: []const u8 }{
        .{ .q = "SELECT g, COUNT(*) AS n FROM '$IN' GROUP BY g HAVING CASE SUM(x) WHEN 3 THEN true ELSE false END", .want = "g,n\na,2\n" },
        .{ .q = "SELECT g FROM '$IN' GROUP BY g HAVING CASE WHEN SUM(x) > 4 THEN true ELSE false END", .want = "g\nb\n" },
        .{ .q = "SELECT CASE WHEN 1 = 1 THEN 'k' END AS k, COUNT(*) AS n FROM '$IN'", .want = "k,n\nk,3\n" },
    };
    for (cases) |c| {
        const out = try runToString(alloc, &tmp, "g,x\na,1\na,2\nb,5\n", c.q);
        defer alloc.free(out);
        try std.testing.expectEqualStrings(c.want, out);
    }
}

test "aggregate: the same expression twice still shares one accumulator" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "v\n10\n20\n30\n",
        "SELECT SUM(v * 2) + SUM(v * 2) AS d, SUM(v) + COUNT(*) AS c FROM '$IN'",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("d,c\n240,63\n", out);
}

test "aggregate: count and sum by group (nulls skipped)" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "status,amount\npaid,100\npending,50\npaid,200\npaid,\n",
        "SELECT status, COUNT(*) AS n, SUM(CAST(amount AS INT)) AS total FROM '$IN' GROUP BY status ORDER BY status ASC",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("status,n,total\npaid,3,300\npending,1,50\n", out);
}

test "aggregate: an interleaved SELECT list keeps its column order" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "status,region,amount\npaid,west,100\npaid,west,50\n",
        "SELECT status, COUNT(*) AS n, region FROM '$IN' GROUP BY status, region",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("status,n,region\npaid,2,west\n", out);

    var t2 = std.testing.tmpDir(.{});
    defer t2.cleanup();
    const keys_first = try runToString(
        alloc,
        &t2,
        "status,region,amount\npaid,west,100\npaid,west,50\n",
        "SELECT status, region, COUNT(*) AS n FROM '$IN' GROUP BY status, region",
    );
    defer alloc.free(keys_first);
    try std.testing.expectEqualStrings("status,region,n\npaid,west,2\n", keys_first);
}

test "aggregate: group by a numeric (int) key (value-keyed hashing)" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "id,n\n1,5\n2,5\n3,7\n4,5\n",
        "SELECT CAST(n AS INT) AS g, COUNT(*) AS c FROM '$IN' GROUP BY g ORDER BY g ASC",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("g,c\n5,3\n7,1\n", out);
}

test "aggregate: a literal tag sits beside an aggregate" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "id,g\n1,a\n2,b\n3,a\n",
        "SELECT 'nightly' AS run, COUNT(*) AS c FROM '$IN'",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("run,c\nnightly,3\n", out);
}

test "aggregate: a PARAM tag sits beside a grouped aggregate, in select order" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,g\n1,a\n2,b\n3,a\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const script = try std.fmt.allocPrint(
        alloc,
        "PARAM tag STRING DEFAULT 'x';\nLOAD INTO '{s}' AS " ++
            "SELECT $tag AS run, g, COUNT(*) AS c FROM '{s}' GROUP BY g ORDER BY g;",
        .{ out_path, in_path },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{.{ .key = "tag", .val = "nightly" }});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("run,g,c\nnightly,a,2\nnightly,b,1\n", out);
}

test "aggregate: a loop variable and a function parameter count as constants" {
    const alloc = std.testing.allocator;
    var ar = std.heap.ArenaAllocator.init(alloc);
    defer ar.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };

    _ = try parser.parseSource(ar.allocator(), "FOR EACH ROW OF (SELECT 'z' AS x) AS (x)\n" ++
        "  LOAD INTO '/tmp/o.csv' AS SELECT $x AS a, COUNT(*) AS n FROM 'in.csv';\n" ++
        "END FOR;", &pdiag);

    _ = try parser.parseSource(ar.allocator(), "CREATE FUNCTION f(t) AS\n" ++
        "  LOAD INTO '/tmp/o.csv' AS SELECT $t AS a, COUNT(*) AS n FROM 'in.csv';\n" ++
        "END;\nCALL f('x');", &pdiag);
}

test "aggregate: a bare column beside an aggregate is still refused" {
    const alloc = std.testing.allocator;
    var ar = std.heap.ArenaAllocator.init(alloc);
    defer ar.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const r = parser.parseSource(ar.allocator(), "LOAD INTO '/tmp/o.csv' AS SELECT g, COUNT(*) AS c FROM 'in.csv';", &pdiag);
    try std.testing.expectError(error.ParseFailed, r);
    try std.testing.expect(std.mem.indexOf(u8, pdiag.msg, "neither an aggregate nor a grouping key") != null);
}

test "aggregate folds groups across multiple batches" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var in_buf = std.array_list.Managed(u8).init(alloc);
    defer in_buf.deinit();
    try in_buf.appendSlice("code,amount,name\n");
    var i: usize = 0;
    while (i < 3000) : (i += 1) {
        try in_buf.writer().print("{c},{d},n{d:0>4}\n", .{ "XY"[i % 2], i, i });
    }
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = in_buf.items });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(
        alloc,
        "LOAD INTO '{s}/out.csv' AS SELECT code, COUNT(*) AS n, SUM(CAST(amount AS INT)) AS total, MIN(name) AS first_name FROM '{s}/in.csv' GROUP BY code ORDER BY code;",
        .{ base, base },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("code,n,total,first_name\nX,1500,2248500,n0000\nY,1500,2250000,n0001\n", out);
}

test "global aggregate streams vectorized partials across batches" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var in_buf = std.array_list.Managed(u8).init(alloc);
    defer in_buf.deinit();
    try in_buf.appendSlice("amount\n");
    var i: usize = 0;
    while (i < 3000) : (i += 1) {
        try in_buf.writer().print("{d}\n", .{i});
    }
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = in_buf.items });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(
        alloc,
        "LOAD INTO '{s}/out.csv' AS SELECT COUNT(*) AS n, SUM(CAST(amount AS INT)) AS total, MIN(CAST(amount AS INT)) AS lo, MAX(CAST(amount AS INT)) AS hi FROM '{s}/in.csv';",
        .{ base, base },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("n,total,lo,hi\n3000,4498500,0,2999\n", out);
}

test "csv aggregate: min/max on inferred numeric columns compare numerically, not lexically" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const got = try runToString(alloc, &tmp, "id\n9\n10\n2\n", "SELECT MIN(id) AS mn, MAX(id) AS mx, SUM(id) AS s FROM '$IN'");
    defer alloc.free(got);
    try std.testing.expectEqualStrings("mn,mx,s\n2,10,21\n", got);
}

test "DECIMAL MIN/MAX/AVG and INT MIN/MAX: exact order across scales, nulls skipped, grouped or not, on one lane or several" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const in = "g,v,n\na,1.25,4\na,-3.5,-2\na,,\nb,10,7\nb,2.125,9\nc,,\n";
    const d = "CASE WHEN v > 5 THEN CAST(v AS DECIMAL(12,1)) ELSE CAST(v AS DECIMAL(12,3)) END";
    const cases = [_]struct { q: []const u8, want: []const u8 }{
        .{
            .q = "SELECT g, MIN(d) AS mn, MAX(d) AS mx, AVG(d) AS av, MIN(n) AS ni, MAX(n) AS nx FROM (SELECT g, n, " ++ d ++ " AS d FROM '$IN') x GROUP BY g ORDER BY g",
            .want = "g,mn,mx,av,ni,nx\na,-3.500,1.250,-1.125,-2,4\nb,2.125,10.0,6.0625,7,9\nc,,,,,\n",
        },
        .{ .q = "SELECT MIN(d) AS mn, MAX(d) AS mx, AVG(d) AS av FROM (SELECT " ++ d ++ " AS d FROM '$IN') x", .want = "mn,mx,av\n-3.500,10.0,2.46875\n" },
        .{ .q = "SELECT MIN(d) AS mn, AVG(d) AS av FROM (SELECT CAST(v AS DECIMAL(12,3)) AS d FROM '$IN' WHERE v IS NULL) x", .want = "mn,av\n,\n" },
    };
    for (cases) |c| {
        const one = try runToString(alloc, &tmp, in, c.q);
        defer alloc.free(one);
        try std.testing.expectEqualStrings(c.want, one);
        const lanes = try runCsvThreaded(alloc, &tmp, in, c.q, 4);
        defer alloc.free(lanes);
        try std.testing.expectEqualStrings(c.want, lanes);
    }
}

test "MEDIAN: odd count takes the middle, even count the mean of the two, nulls ignored, empty is null" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const got = try runToString(alloc, &tmp, "k,v\na,3\na,1\na,\na,2\nb,20\nb,10\nc,\n", "SELECT k, MEDIAN(v) AS m FROM '$IN' GROUP BY k ORDER BY k");
    defer alloc.free(got);
    try std.testing.expectEqualStrings("k,m\na,2\nb,15\nc,\n", got);
}

test "a numeric aggregate casts a text argument, grouped or not, and refuses text that is no number" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const grouped = try runToString(alloc, &tmp, "k,v\na,1\na,2\nb,3\n", "SELECT k, SUM(CAST(v AS STRING)) AS s, AVG(CAST(v AS STRING)) AS a, MEDIAN(CAST(v AS STRING)) AS m FROM '$IN' GROUP BY k ORDER BY k");
    defer alloc.free(grouped);
    try std.testing.expectEqualStrings("k,s,a,m\na,3,1.5,1.5\nb,3,3,3\n", grouped);
    const whole = try runToString(alloc, &tmp, "k,v\na,1\na,2\nb,3\n", "SELECT SUM(CAST(v AS STRING)) AS s, MEDIAN(CAST(v AS STRING)) AS m FROM '$IN'");
    defer alloc.free(whole);
    try std.testing.expectEqualStrings("s,m\n6,2\n", whole);
    try std.testing.expectError(error.CastFailed, runToString(alloc, &tmp, "k,v\na,1\nb,x\n", "SELECT k, SUM(v) AS s FROM '$IN' GROUP BY k"));
}

test "count_if, bool_and/or, bit_and/or/xor: grouped and not, nulls skipped, empty groups as the engines answer" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const input = "k,ok,v\na,true,6\na,false,3\na,,\nb,true,5\nc,,\n";
    const grouped = try runToString(alloc, &tmp, input,
        \\SELECT k, count_if(ok) AS ci, count_if(v > 4) AS big, bool_and(ok) AS ba, bool_or(ok) AS bo,
        \\       bit_and(v) AS an, bit_or(v) AS o, bit_xor(v) AS x
        \\FROM '$IN' GROUP BY k ORDER BY k
    );
    defer alloc.free(grouped);
    try std.testing.expectEqualStrings("k,ci,big,ba,bo,an,o,x\na,1,1,false,true,2,7,5\nb,1,1,true,true,5,5,5\nc,0,0,,,,,\n", grouped);
    const whole = try runToString(alloc, &tmp, input, "SELECT count_if(ok) AS ci, bool_and(ok) AS ba, bit_or(v) AS o FROM '$IN'");
    defer alloc.free(whole);
    try std.testing.expectEqualStrings("ci,ba,o\n2,false,7\n", whole);
    const none = try runToString(alloc, &tmp, input, "SELECT count_if(ok) AS ci, bool_or(ok) AS bo, bit_and(v) AS an FROM '$IN' WHERE k = 'zz'");
    defer alloc.free(none);
    try std.testing.expectEqualStrings("ci,bo,an\n0,,\n", none);
    const built = try runToString(alloc, &tmp, input, "SELECT k, count_if(true) AS t, count_if(abs(v) > 4) AS a FROM '$IN' GROUP BY k ORDER BY k");
    defer alloc.free(built);
    try std.testing.expectEqualStrings("k,t,a\na,3,1\nb,1,1\nc,1,0\n", built);
    const lanes = try runCsvThreaded(alloc, &tmp, input,
        \\SELECT k, count_if(ok) AS ci, count_if(v > 4) AS big, bool_and(ok) AS ba, bool_or(ok) AS bo,
        \\       bit_and(v) AS an, bit_or(v) AS o, bit_xor(v) AS x
        \\FROM '$IN' GROUP BY k ORDER BY k
    , 4);
    defer alloc.free(lanes);
    try std.testing.expectEqualStrings(grouped, lanes);
}

test "variance and standard deviation: sample by default, population on request, the same across lanes" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const input = "k,v\na,2\na,4\na,4\na,4\na,5\na,5\na,7\na,9\nb,3\nb,\nc,\n";
    const q =
        \\SELECT k, var_pop(v) AS vp, stddev_pop(v) AS sp, round(variance(v), 6) AS vs, round(stddev(v), 6) AS ss,
        \\       round(var_samp(v), 6) AS vs2, round(stddev_samp(v), 6) AS ss2
        \\FROM '$IN' GROUP BY k ORDER BY k
    ;
    const want = "k,vp,sp,vs,ss,vs2,ss2\na,4,2,4.571429,2.13809,4.571429,2.13809\nb,0,0,,,,\nc,,,,,,\n";
    const serial = try runToString(alloc, &tmp, input, q);
    defer alloc.free(serial);
    try std.testing.expectEqualStrings(want, serial);
    const threaded = try runCsvThreaded(alloc, &tmp, input,
        \\SELECT k, round(var_pop(v), 9) AS vp, round(stddev_pop(v), 9) AS sp, round(variance(v), 6) AS vs, round(stddev(v), 6) AS ss,
        \\       round(var_samp(v), 6) AS vs2, round(stddev_samp(v), 6) AS ss2
        \\FROM '$IN' GROUP BY k ORDER BY k
    , 4);
    defer alloc.free(threaded);
    try std.testing.expectEqualStrings(want, threaded);
}

test "GROUP BY and DISTINCT past --op-memory spill to disk and return what they return in memory" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var csv = std.array_list.Managed(u8).init(alloc);
    defer csv.deinit();
    try csv.appendSlice("g,s,v,f\n");
    for (0..6000) |i| {
        if (i % 41 == 3) {
            try csv.writer().print(",n{d},{d},{d}.25\n", .{ i % 7, i, i % 13 });
        } else try csv.writer().print("{d},n{d},{d},{d}.25\n", .{ (i * 7919) % 2200, i % 7, i, i % 13 });
    }
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = csv.items });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const spill_dir = try std.fs.path.join(alloc, &.{ base, "spill" });
    defer alloc.free(spill_dir);

    const queries = [_][]const u8{
        "SELECT g, s, COUNT(*) AS c, SUM(v) AS sv, SUM(f) AS sf, AVG(f) AS af, MIN(v) AS mn, MAX(s) AS mx, COUNT(DISTINCT s) AS ds, median(v) AS md, stddev_pop(f) AS sd FROM '$B/in.csv' GROUP BY g, s ORDER BY g, s",
        "SELECT g, COUNT(*) AS c, SUM(v * 2) AS sv, bit_xor(v) AS bx FROM '$B/in.csv' GROUP BY g ORDER BY g",
        "WITH t AS (SELECT g, v, CAST(f AS DECIMAL(10,2)) AS d FROM '$B/in.csv') SELECT g, COUNT(*) AS c, SUM(d * v) AS sd, SUM(d - 1.255) AS se FROM t GROUP BY g ORDER BY g",
        "SELECT DISTINCT g, s FROM '$B/in.csv' ORDER BY g, s",
        "WITH d AS (SELECT DISTINCT ON (g) g, v FROM '$B/in.csv') SELECT * FROM d ORDER BY g",
    };
    for (queries) |tmpl| {
        const q = try std.mem.replaceOwned(u8, alloc, tmpl, "$B", base);
        defer alloc.free(q);
        const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}/out.csv' AS {s};", .{ base, q });
        defer alloc.free(script);
        const want = try runScriptOpts(alloc, &tmp, script, .{});
        defer alloc.free(want);
        const got = try runScriptOpts(alloc, &tmp, script, .{ .op_memory = 1, .spill_dir = spill_dir });
        defer alloc.free(got);
        try std.testing.expect(std.mem.count(u8, want, "\n") > 1000);
        try std.testing.expectEqualStrings(want, got);
        try std.testing.expectError(error.SpillCapExceeded, runScriptOpts(alloc, &tmp, script, .{ .op_memory = 1, .spill_dir = spill_dir, .spill_cap = 64 }));
    }
    for ([_][]const u8{ "SELECT g, COUNT(*) AS c FROM '$B/in.csv' GROUP BY g LIMIT 1500", "SELECT DISTINCT g FROM '$B/in.csv' LIMIT 1500" }) |tmpl| {
        const q = try std.mem.replaceOwned(u8, alloc, tmpl, "$B", base);
        defer alloc.free(q);
        const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}/out.csv' AS {s};", .{ base, q });
        defer alloc.free(script);
        const got = try runScriptOpts(alloc, &tmp, script, .{ .op_memory = 1, .spill_dir = spill_dir });
        defer alloc.free(got);
        try std.testing.expectEqual(@as(usize, 1501), std.mem.count(u8, got, "\n"));
    }
    var dir = try tmp.dir.openDir("spill", .{ .iterate = true });
    defer dir.close();
    var it = dir.iterate();
    try std.testing.expect((try it.next()) == null);
}

test "GROUP BY, HAVING and ORDER BY bind only the same expression; unselected keys stay out; ORDER BY takes expressions and positions" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const q_csv = "\"Valor Total\",cat\n10,x\n20,y\n30,x\n-5,\n";
    const r_csv = "k,s\n2,x\n2,y\n3,z\n,nn\n5,w\n1,u\n";
    const cases = [_]struct { in: []const u8, q: []const u8, want: []const u8 }{
        .{ .in = q_csv, .q = "SELECT upper('cat') AS u, COUNT(*) AS n FROM '$IN' GROUP BY upper(cat) ORDER BY n", .want = "u,n\nCAT,1\nCAT,1\nCAT,2\n" },
        .{ .in = q_csv, .q = "SELECT cat, count_if(cat = 'x') AS a FROM '$IN' GROUP BY cat HAVING count_if(cat = 'X') > 0", .want = "cat,a\n" },
        .{ .in = q_csv, .q = "SELECT replace(cat, 'y', 'A') AS r FROM '$IN' WHERE cat <> '' ORDER BY replace(cat, 'Y', 'A')", .want = "r\nx\nx\nA\n" },
        .{ .in = q_csv, .q = "SELECT cat FROM '$IN' GROUP BY cat HAVING COUNT(*) > 1", .want = "cat\nx\n" },
        .{ .in = q_csv, .q = "SELECT count(*) AS n FROM '$IN' GROUP BY replace(cat, 'x', 'A'), replace(cat, 'x', 'a') ORDER BY n", .want = "n\n1\n1\n2\n" },
        .{ .in = q_csv, .q = "SELECT upper(cat) AS u, count(*) AS n FROM '$IN' GROUP BY upper(cat) HAVING sum(\"Valor Total\") > 15 ORDER BY u", .want = "u,n\nX,2\nY,1\n" },
        .{ .in = q_csv, .q = "SELECT \"Valor Total\" AS v FROM '$IN' ORDER BY \"Valor Total\" - 1", .want = "v\n-5\n10\n20\n30\n" },
        .{ .in = q_csv, .q = "SELECT cat, \"Valor Total\" AS v FROM '$IN' ORDER BY 2 DESC", .want = "cat,v\nx,30\ny,20\nx,10\n,-5\n" },
        .{ .in = q_csv, .q = "SELECT cat FROM '$IN' WHERE cat <> '' ORDER BY cat || 'a' DESC", .want = "cat\ny\nx\nx\n" },
        .{ .in = q_csv, .q = "SELECT DISTINCT cat FROM '$IN' ORDER BY coalesce(cat, 'a')", .want = "cat\n\nx\ny\n" },
        .{ .in = q_csv, .q = "SELECT cat, SUM(\"Valor Total\") / COUNT(*) AS m FROM '$IN' WHERE cat <> '' GROUP BY cat ORDER BY SUM(\"Valor Total\") / COUNT(*) DESC, 1", .want = "cat,m\nx,20\ny,20\n" },
        .{ .in = r_csv, .q = "SELECT count(*) AS c FROM '$IN' GROUP BY k ORDER BY k", .want = "c\n1\n2\n1\n1\n1\n" },
        .{ .in = r_csv, .q = "SELECT k FROM '$IN' WHERE k > (SELECT count(*) FROM '$IN' GROUP BY k HAVING count(*) > 1) ORDER BY k", .want = "k\n3\n5\n" },
        .{ .in = r_csv, .q = "SELECT k FROM '$IN' WHERE k IN (SELECT max(k) FROM '$IN' GROUP BY s) ORDER BY k", .want = "k\n1\n2\n2\n3\n5\n" },
    };
    for (cases) |c| {
        const got = try runToString(alloc, &tmp, c.in, c.q);
        defer alloc.free(got);
        try std.testing.expectEqualStrings(c.want, got);
    }
}

test "a bare GROUP BY name is the input column before a SELECT alias of it; ORDER BY takes the output name" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const t_csv = "g,v\nab,1\nAb,2\ncd,3\n";
    const cases = [_]struct { q: []const u8, want: []const u8 }{
        .{ .q = "SELECT upper(g) AS g, SUM(v) AS s FROM '$IN' GROUP BY g HAVING g <> 'ab' ORDER BY s", .want = "g,s\nAB,2\nCD,3\n" },
        .{ .q = "SELECT g AS h, upper(g) AS g, COUNT(*) AS n FROM '$IN' GROUP BY g ORDER BY 1", .want = "h,g,n\nAb,AB,1\nab,AB,1\ncd,CD,1\n" },
        .{ .q = "SELECT upper(g) AS g, COUNT(*) AS n FROM '$IN' GROUP BY upper(g) ORDER BY g", .want = "g,n\nAB,2\nCD,1\n" },
        .{ .q = "SELECT upper(g) AS u, COUNT(*) AS n FROM '$IN' GROUP BY u ORDER BY u", .want = "u,n\nAB,2\nCD,1\n" },
        .{ .q = "SELECT -v AS v FROM '$IN' ORDER BY v", .want = "v\n-3\n-2\n-1\n" },
    };
    for (cases) |c| {
        const got = try runToString(alloc, &tmp, t_csv, c.q);
        defer alloc.free(got);
        try std.testing.expectEqualStrings(c.want, got);
    }
}

test "a grouping expression matches however its columns are written: with the table's alias, quoted or bare" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const t_csv = "g,x\nab,1\nAb,5\ncd,2\ncd,10\n";
    const cases = [_]struct { q: []const u8, want: []const u8 }{
        .{ .q = "SELECT upper(g) AS u, COUNT(*) AS n FROM '$IN' t GROUP BY upper(t.g) ORDER BY upper(t.g)", .want = "u,n\nAB,2\nCD,2\n" },
        .{ .q = "SELECT upper(t.g) AS u, SUM(t.x) AS s FROM '$IN' t GROUP BY upper(\"g\") HAVING SUM(x) > 6 ORDER BY SUM(x)", .want = "u,s\nCD,12\n" },
        .{ .q = "SELECT t.x + 1 AS y FROM '$IN' t GROUP BY x + 1 HAVING COUNT(t.x) > 0 ORDER BY 1", .want = "y\n2\n3\n6\n11\n" },
    };
    for (cases) |c| {
        const got = try runToString(alloc, &tmp, t_csv, c.q);
        defer alloc.free(got);
        try std.testing.expectEqualStrings(c.want, got);
    }
}

test "an expression over grouping expressions sits in SELECT, HAVING and ORDER BY; ORDER BY may add aggregates of its own" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const t_csv = "g,x\nab,1\nAb,5\ncd,2\ncd,10\nef,3\n";
    const cases = [_]struct { q: []const u8, want: []const u8 }{
        .{ .q = "SELECT upper(g), COUNT(*) AS n, lower(upper(g)) || 'x' AS z FROM '$IN' GROUP BY upper(g) ORDER BY 1", .want = "upper(g),n,z\nAB,2,abx\nCD,2,cdx\nEF,1,efx\n" },
        .{ .q = "SELECT COUNT(*) AS n, lower(upper(g)) AS z FROM '$IN' GROUP BY upper(g) HAVING lower(upper(g)) <> 'ef' ORDER BY lower(upper(g)) DESC", .want = "n,z\n2,cd\n2,ab\n" },
        .{ .q = "SELECT g || '!' AS z, upper(g) || COUNT(*) AS w FROM '$IN' GROUP BY g ORDER BY g", .want = "z,w\nAb!,AB1\nab!,AB1\ncd!,CD2\nef!,EF1\n" },
        .{ .q = "SELECT g, SUM(x) AS s FROM '$IN' GROUP BY g ORDER BY SUM(x) / COUNT(*) DESC, MAX(x)", .want = "g,s\ncd,12\nAb,5\nef,3\nab,1\n" },
        .{ .q = "SELECT g FROM '$IN' GROUP BY g HAVING COUNT(*) > 0 ORDER BY MIN(x) DESC LIMIT 2", .want = "g\nAb\nef\n" },
        .{ .q = "SELECT COUNT(*) AS n FROM '$IN' GROUP BY upper(g) ORDER BY MAX(x) + 0", .want = "n\n1\n2\n2\n" },
        .{ .q = "SELECT SUM(x) AS s FROM '$IN' ORDER BY MAX(x)", .want = "s\n21\n" },
    };
    for (cases) |c| {
        const got = try runToString(alloc, &tmp, t_csv, c.q);
        defer alloc.free(got);
        try std.testing.expectEqualStrings(c.want, got);
    }
    var ar = std.heap.ArenaAllocator.init(alloc);
    defer ar.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const refused = [_][2][]const u8{
        .{ "SELECT upper(g), lower(g) FROM 'in.csv' GROUP BY upper(g);", "`g` is neither an aggregate nor a grouping key — wrap it in an aggregate, or name it in GROUP BY" },
        .{ "SELECT count(*) FROM 'in.csv' GROUP BY upper(g) HAVING g > 'a';", "`g` is neither an aggregate nor a grouping key — wrap it in an aggregate, or name it in GROUP BY" },
        .{ "SELECT g, count(*) FROM 'in.csv' GROUP BY g ORDER BY max(x) + x;", "`x` is neither an aggregate nor a grouping key — wrap it in an aggregate, or name it in GROUP BY" },
        .{ "SELECT DISTINCT g, count(*) FROM 'in.csv' GROUP BY g ORDER BY max(x);", "with SELECT DISTINCT, an aggregate in ORDER BY must be one the SELECT list computes" },
    };
    for (refused) |r| {
        try std.testing.expectError(error.ParseFailed, parser.parseSource(ar.allocator(), r[0], &pdiag));
        try std.testing.expectEqualStrings(r[1], pdiag.msg);
    }
}
