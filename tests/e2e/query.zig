//! End-to-end SQL: set operations, subqueries and derived tables, joins, DISTINCT,
//! sorts and limits, generated sources and table functions.

const std = @import("std");
const basalt = @import("basalt");
const parser = basalt.sql_parser;
const Diag = basalt.env.Diag;
const ParamArg = basalt.env.ParamArg;
const run = basalt.runtime.run;
const runToString = @import("harness.zig").runToString;
const runScript = @import("harness.zig").runScript;
const runScriptOpts = @import("harness.zig").runScriptOpts;
const checkAndRun = @import("harness.zig").checkAndRun;
const expectFile = @import("harness.zig").expectFile;
const expectRefusedAlike = @import("harness.zig").expectRefusedAlike;

test "union all by name: a branch may be any query, not just a table" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "a.csv", .data = "k,v\na,1\nb,2\n" });
    try tmp.dir.writeFile(.{ .sub_path = "b.csv", .data = "k,v,extra\nc,3,x\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const pa = try std.fs.path.join(alloc, &.{ base, "a.csv" });
    defer alloc.free(pa);
    const pb = try std.fs.path.join(alloc, &.{ base, "b.csv" });
    defer alloc.free(pb);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const script = try std.fmt.allocPrint(
        alloc,
        "LOAD INTO '{s}' AS SELECT k, v FROM '{s}' WHERE v = 1" ++
            " UNION ALL BY NAME SELECT k, v, extra FROM '{s}';",
        .{ out_path, pa, pb },
    );
    defer alloc.free(script);
    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("k,v\na,1\nc,3\n", out);
}

test "derived tables: an alias names its table only inside its own query" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "t1.csv", .data = "k\n1\n2\n3\n" });
    try tmp.dir.writeFile(.{ .sub_path = "t2.csv", .data = "k\n2\n3\n4\n5\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const cases = [_]struct { q: []const u8, want: []const u8 }{
        .{
            .q = "WITH a AS (SELECT k FROM (SELECT k FROM '$B/t1.csv' WHERE k = 1) r)," ++
                " b AS (SELECT k FROM (SELECT k FROM '$B/t2.csv' WHERE k = 5) r)" ++
                " SELECT 'a' AS src, k FROM a UNION ALL BY NAME SELECT 'b' AS src, k FROM b",
            .want = "src,k\na,1\nb,5\n",
        },
        .{
            .q = "SELECT COUNT(*) AS n FROM (SELECT k2 FROM (SELECT k AS k2 FROM '$B/t1.csv') r) r",
            .want = "n\n3\n",
        },
        .{
            .q = "WITH b AS (SELECT k AS k2 FROM '$B/t2.csv')," ++
                " a AS (SELECT k1 FROM (SELECT k AS k1 FROM '$B/t1.csv') b)" ++
                " SELECT COUNT(*) AS n FROM a JOIN b ON a.k1 = b.k2",
            .want = "n\n2\n",
        },
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

test "union: plain UNION [ALL] lines branches up by position" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "t1.csv", .data = "k\n1\n2\n3\n" });
    try tmp.dir.writeFile(.{ .sub_path = "t2.csv", .data = "k,v\n2,x\n3,y\n4,z\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const cases = [_]struct { q: []const u8, want: []const u8 }{
        .{ .q = "SELECT k AS a FROM '$B/t1.csv' UNION ALL SELECT k AS b FROM '$B/t2.csv'", .want = "a\n1\n2\n3\n2\n3\n4\n" },
        .{ .q = "SELECT k FROM '$B/t1.csv' UNION SELECT k FROM '$B/t2.csv' ORDER BY k", .want = "k\n1\n2\n3\n4\n" },
        .{ .q = "SELECT k FROM '$B/t1.csv' WHERE k = 1 UNION ALL SELECT 2.5 AS x FROM '$B/t2.csv' WHERE k = 2", .want = "k\n1.0\n2.5\n" },
        .{
            .q = "SELECT k FROM '$B/t1.csv' UNION SELECT k FROM '$B/t2.csv' UNION ALL SELECT k FROM '$B/t1.csv' WHERE k = 1 ORDER BY k",
            .want = "k\n1\n1\n2\n3\n4\n",
        },
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

    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}/out.csv' AS SELECT k FROM '{s}/t1.csv' UNION ALL SELECT k, v FROM '{s}/t2.csv';", .{ base, base, base });
    defer alloc.free(script);
    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, run(alloc, prog, .{}, &rdiag));
    try std.testing.expect(std.mem.indexOf(u8, rdiag.msg, "branch 2 has 2 columns") != null);
}

test "NOT IN (SELECT ...): a NULL in the subquery keeps no row, a NULL probe survives only an empty one" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "l.csv", .data = "k,v\n1,a\n,b\n2,c\n" });
    try tmp.dir.writeFile(.{ .sub_path = "r.csv", .data = "k,v\n2,x\n,y\n3,z\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const cases = [_]struct { q: []const u8, want: []const u8 }{
        .{ .q = "SELECT k FROM '$B/l.csv' WHERE k NOT IN (SELECT k FROM '$B/r.csv')", .want = "k\n" },
        .{ .q = "SELECT k FROM '$B/l.csv' WHERE k NOT IN (SELECT k FROM '$B/r.csv' WHERE k IS NOT NULL)", .want = "k\n1\n" },
        .{ .q = "SELECT k FROM '$B/l.csv' WHERE k NOT IN (SELECT k FROM '$B/r.csv' WHERE k > 100)", .want = "k\n1\n\n2\n" },
        .{ .q = "SELECT k FROM '$B/l.csv' WHERE k IN (SELECT k FROM '$B/r.csv')", .want = "k\n2\n" },
        .{ .q = "SELECT k FROM '$B/l.csv' WHERE k IN (SELECT k FROM '$B/r.csv' UNION SELECT 1 AS k)", .want = "k\n1\n2\n" },
        .{ .q = "SELECT k FROM '$B/l.csv' WHERE k IN (SELECT k FROM '$B/r.csv' UNION ALL SELECT 1 AS k)", .want = "k\n1\n2\n" },
        .{ .q = "SELECT k FROM '$B/l.csv' WHERE k IN (SELECT k FROM '$B/r.csv' INTERSECT SELECT 2 AS k)", .want = "k\n2\n" },
        .{ .q = "SELECT k FROM '$B/l.csv' WHERE k IN (SELECT k FROM '$B/r.csv' EXCEPT SELECT 2 AS k)", .want = "k\n" },
        .{ .q = "SELECT k FROM '$B/l.csv' WHERE k NOT IN (SELECT k FROM '$B/r.csv' WHERE k > 2 UNION SELECT 1 AS k UNION ALL SELECT 1 AS k)", .want = "k\n2\n" },
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

test "correlated subqueries: EXISTS, IN and scalar subqueries over the outer row, as PostgreSQL answers them, at one lane and four" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "c.csv", .data = "id,name,region,tier\n1,alpha,north,1\n2,beta,south,2\n3,gamma,north,\n4,delta,,1\n5,eps,south,3\n,nokey,east,2\n" });
    try tmp.dir.writeFile(.{ .sub_path = "o.csv", .data = "oid,cust,amount,region,day\n10,1,100,north,1\n11,1,50,north,2\n12,2,75,south,3\n13,3,,north,4\n14,3,20,south,5\n15,,40,east,6\n16,9,10,west,7\n17,2,75,south,8\n18,4,5,,9\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const cases = [_]struct { q: []const u8, want: []const u8 }{
        .{ .q = "SELECT c.id, c.name FROM '$B/c.csv' c WHERE EXISTS (SELECT 1 FROM '$B/o.csv' o WHERE o.cust = c.id) ORDER BY c.id", .want = "id,name\n1,alpha\n2,beta\n3,gamma\n4,delta\n" },
        .{ .q = "SELECT c.id, c.name FROM '$B/c.csv' c WHERE NOT EXISTS (SELECT 1 FROM '$B/o.csv' o WHERE o.cust = c.id) ORDER BY c.id", .want = "id,name\n5,eps\n,nokey\n" },
        .{ .q = "SELECT c.id FROM '$B/c.csv' c WHERE EXISTS (SELECT 1 FROM '$B/o.csv' o WHERE o.cust = c.id AND o.day > c.id * 2) ORDER BY c.id", .want = "id\n2\n4\n" },
        .{ .q = "SELECT c.id FROM '$B/c.csv' c WHERE NOT EXISTS (SELECT 1 FROM '$B/o.csv' o WHERE o.day <= c.tier) ORDER BY c.id", .want = "id\n3\n" },
        .{ .q = "SELECT c.id FROM '$B/c.csv' c WHERE EXISTS (SELECT 1 FROM '$B/o.csv' WHERE amount > 90) AND NOT EXISTS (SELECT 1 FROM '$B/o.csv' WHERE amount > 900) ORDER BY c.id", .want = "id\n1\n2\n3\n4\n5\n\n" },
        .{ .q = "SELECT c.id, c.tier FROM '$B/c.csv' c WHERE c.tier IN (SELECT o.cust FROM '$B/o.csv' o WHERE o.region = c.region) ORDER BY c.id", .want = "id,tier\n1,1\n2,2\n5,3\n" },
        .{ .q = "SELECT c.id, c.tier FROM '$B/c.csv' c WHERE c.tier NOT IN (SELECT o.cust FROM '$B/o.csv' o WHERE o.region = c.region) ORDER BY c.id", .want = "id,tier\n4,1\n" },
        .{ .q = "SELECT c.id, (SELECT count(*) FROM '$B/o.csv' o WHERE o.cust = c.id) AS n, (SELECT sum(o.amount) FROM '$B/o.csv' o WHERE o.cust = c.id) AS s, (SELECT coalesce(max(o.amount), 0) FROM '$B/o.csv' o WHERE o.cust = c.id) AS m FROM '$B/c.csv' c ORDER BY c.id", .want = "id,n,s,m\n1,2,150,100\n2,2,150,75\n3,2,20,20\n4,1,5,5\n5,0,,0\n,0,,0\n" },
        .{ .q = "SELECT * FROM '$B/c.csv' c WHERE (SELECT count(*) FROM '$B/o.csv' o WHERE o.cust = c.id) = 0 ORDER BY id", .want = "id,name,region,tier\n5,eps,south,3\n,nokey,east,2\n" },
        .{ .q = "SELECT c.id, (SELECT count(*) FROM '$B/o.csv' o WHERE o.cust = c.id AND o.region = c.region) AS n FROM '$B/c.csv' c ORDER BY c.id", .want = "id,n\n1,2\n2,2\n3,1\n4,0\n5,0\n,0\n" },
        .{ .q = "SELECT c.id, (SELECT o.oid FROM '$B/o.csv' o WHERE o.cust = c.id AND o.day < 5 AND o.amount > 60) AS one FROM '$B/c.csv' c ORDER BY c.id", .want = "id,one\n1,10\n2,12\n3,\n4,\n5,\n,\n" },
        .{ .q = "WITH big AS (SELECT * FROM '$B/o.csv' WHERE amount >= 20) SELECT c.id, (SELECT count(*) FROM big b WHERE b.cust = c.id) AS n FROM '$B/c.csv' c ORDER BY n DESC, c.id LIMIT 3", .want = "id,n\n1,2\n2,2\n3,1\n" },
        .{ .q = "SELECT c.id, (SELECT count(*) FROM '$B/o.csv' o WHERE o.cust = c.id AND c.tier = 1) AS n FROM '$B/c.csv' c ORDER BY c.id", .want = "id,n\n1,2\n2,0\n3,0\n4,1\n5,0\n,0\n" },
        .{ .q = "SELECT DISTINCT * FROM '$B/c.csv' c WHERE (SELECT coalesce(sum(o.amount), 0) FROM '$B/o.csv' o WHERE o.cust = c.id) < 100 ORDER BY id", .want = "id,name,region,tier\n3,gamma,north,\n4,delta,,1\n5,eps,south,3\n,nokey,east,2\n" },
    };
    for ([_]usize{ 1, 4 }) |threads| for (cases) |c| {
        const q = try std.mem.replaceOwned(u8, alloc, c.q, "$B", base);
        defer alloc.free(q);
        const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}/out.csv' AS {s};", .{ base, q });
        defer alloc.free(script);
        const out = try runScriptOpts(alloc, &tmp, script, .{ .threads = threads });
        defer alloc.free(out);
        try std.testing.expectEqualStrings(c.want, out);
    };

    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}/out.csv' AS SELECT c.id, (SELECT o.oid FROM '{s}/o.csv' o WHERE o.cust = c.id) AS one FROM '{s}/c.csv' c;", .{ base, base, base });
    defer alloc.free(script);
    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    try std.testing.expectError(error.CastFailed, run(alloc, prog, .{}, &rdiag));
    try std.testing.expect(std.mem.indexOf(u8, rdiag.msg, "more than one row returned by a subquery used as an expression") != null);
}

test "INTERSECT / EXCEPT: NULLs compare equal, results are deduplicated, INTERSECT binds tighter" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "s1.csv", .data = "k,v\n1,a\n,b\n,b\n2,\n2,\n4,d\n" });
    try tmp.dir.writeFile(.{ .sub_path = "s2.csv", .data = "k,v\n,b\n2,\n3,c\n" });
    try tmp.dir.writeFile(.{ .sub_path = "s3.csv", .data = "k,v\n4,d\n9,z\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const cases = [_]struct { q: []const u8, want: []const u8 }{
        .{ .q = "SELECT k, v FROM '$B/s1.csv' INTERSECT SELECT k, v FROM '$B/s2.csv' ORDER BY k", .want = "k,v\n2,\n,b\n" },
        .{ .q = "SELECT k, v FROM '$B/s1.csv' EXCEPT SELECT k, v FROM '$B/s2.csv' ORDER BY k", .want = "k,v\n1,a\n4,d\n" },
        .{
            .q = "SELECT k, v FROM '$B/s2.csv' UNION SELECT k, v FROM '$B/s1.csv' INTERSECT SELECT k, v FROM '$B/s3.csv' ORDER BY k",
            .want = "k,v\n2,\n3,c\n4,d\n,b\n",
        },
        .{
            .q = "SELECT k, v FROM '$B/s1.csv' EXCEPT SELECT k, v FROM '$B/s2.csv' EXCEPT SELECT k, v FROM '$B/s3.csv'",
            .want = "k,v\n1,a\n",
        },
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

test "CASE / COALESCE / greatest mixing int and float are float on every row, not the first row's kind" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "m.csv", .data = "id,k,f\n1,3,3.75\n2,5,1.5\n3,,2.5\n4,7,\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const cases = [_]struct { q: []const u8, want: []const u8 }{
        .{ .q = "SELECT id, CASE WHEN id < 3 THEN 1 ELSE 2.5 END AS c FROM '$B/m.csv'", .want = "id,c\n1,1\n2,1\n3,2.5\n4,2.5\n" },
        .{ .q = "SELECT id FROM '$B/m.csv' WHERE CASE WHEN id < 3 THEN 1 ELSE 2.5 END > 2.2", .want = "id\n3\n4\n" },
        .{ .q = "SELECT greatest(id, 1.5) AS g FROM '$B/m.csv' WHERE id < 3", .want = "g\n1.5\n2\n" },
        .{ .q = "SELECT SUM(COALESCE(k, f)) AS s FROM '$B/m.csv'", .want = "s\n17.5\n" },
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

test "DISTINCT ON with ORDER BY keeps the first row per key in ORDER BY order" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "d.csv", .data = "k,v,t\n1,10,3\n1,20,1\n1,30,2\n2,5,9\n2,6,8\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const cases = [_]struct { q: []const u8, want: []const u8 }{
        .{ .q = "SELECT DISTINCT ON (k) k, v, t FROM '$B/d.csv' ORDER BY k, t", .want = "k,v,t\n1,20,1\n2,6,8\n" },
        .{ .q = "SELECT DISTINCT ON (k) k, v FROM '$B/d.csv' ORDER BY k, t DESC", .want = "k,v\n1,10\n2,5\n" },
        .{ .q = "SELECT DISTINCT ON (k) k, v, t FROM '$B/d.csv'", .want = "k,v,t\n1,10,3\n2,5,9\n" },
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

test "GROUP BY on any key path and DISTINCT print one key for -0.0 and 0.0" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "z.csv", .data = "f,d\n-0.0,1\n0.0,1\nnan,2\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const t = "(SELECT CAST(f AS FLOAT) AS g, CAST(d AS DECIMAL(5,1)) AS x FROM '$B/z.csv') t";
    const cases = [_]struct { q: []const u8, want: []const u8 }{
        .{ .q = "SELECT g, count(*) AS n FROM " ++ t ++ " GROUP BY g ORDER BY g", .want = "g,n\n0,2\nnan,1\n" },
        .{ .q = "SELECT g, x, count(*) AS n FROM " ++ t ++ " GROUP BY g, x ORDER BY g", .want = "g,x,n\n0,1.0,2\nnan,2.0,1\n" },
        .{ .q = "SELECT DISTINCT g FROM " ++ t ++ " ORDER BY g", .want = "g\n0\nnan\n" },
        .{ .q = "SELECT DISTINCT ON (g) g, x FROM " ++ t ++ " ORDER BY g", .want = "g,x\n0,1.0\nnan,2.0\n" },
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

test "several IN (SELECT ...) conditions, each subquery with its own WHERE" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "m.csv", .data = "k,x\n1,a\n2,b\n3,c\n4,d\n" });
    try tmp.dir.writeFile(.{ .sub_path = "pa.csv", .data = "p,q\n1,0\n2,1\n3,1\n9,1\n" });
    try tmp.dir.writeFile(.{ .sub_path = "pb.csv", .data = "p,q\n2,0\n3,0\n4,0\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const cases = [_]struct { q: []const u8, want: []const u8 }{
        .{ .q = "SELECT k FROM '$B/m.csv' WHERE k IN (SELECT p FROM '$B/pa.csv' WHERE q > 0) AND k IN (SELECT p FROM '$B/pb.csv') ORDER BY k", .want = "k\n2\n3\n" },
        .{ .q = "SELECT k FROM '$B/m.csv' WHERE k IN (SELECT p FROM '$B/pa.csv') AND k IN (SELECT p FROM '$B/pb.csv' WHERE q = 0) ORDER BY k", .want = "k\n2\n3\n" },
        .{ .q = "SELECT k FROM '$B/m.csv' WHERE k IN (SELECT p FROM '$B/pa.csv' WHERE p NOT IN (SELECT p FROM '$B/pb.csv')) AND k IN (SELECT p FROM '$B/pa.csv')", .want = "k\n1\n" },
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

test "derived table: a subquery in FROM is an anonymous CTE" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "id,status,amount\n1,paid,100\n2,pending,50\n3,paid,200\n",
        "SELECT id, amount FROM (SELECT id, amount FROM '$IN' WHERE status = 'paid') x",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,amount\n1,100\n3,200\n", out);
}

test "derived table: nested, and beside a WITH binding" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "id,amount\n1,100\n2,50\n",
        "SELECT id FROM (WITH w AS (SELECT id FROM '$IN') SELECT id FROM (SELECT id FROM w) a) b ORDER BY id",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id\n1\n2\n", out);
}

test "derived table: a join right side may be a subquery" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "id,status,amount\n1,paid,100\n2,pending,50\n3,paid,200\n",
        "SELECT x.id, x.amount FROM (SELECT id, amount FROM '$IN') x " ++
            "JOIN (SELECT id AS pid FROM '$IN' WHERE status = 'paid') p ON x.id = p.pid ORDER BY x.id",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,amount\n1,100\n3,200\n", out);
}

test "CSV -> filter/select -> CSV round-trips" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "id,status,amount\n1,paid,100\n2,pending,50\n3,paid,200\n",
        "SELECT id, amount FROM '$IN' WHERE status = 'paid'",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,amount\n1,100\n3,200\n", out);
}

test "sort: nulls sort as the largest value unless NULLS FIRST or NULLS LAST says" {
    const alloc = std.testing.allocator;
    const cases = [_]struct { order: []const u8, want: []const u8 }{
        .{ .order = "amt DESC", .want = "id,amt\n2,\n3,200\n1,100\n" },
        .{ .order = "amt", .want = "id,amt\n1,100\n3,200\n2,\n" },
        .{ .order = "amt DESC NULLS LAST", .want = "id,amt\n3,200\n1,100\n2,\n" },
        .{ .order = "amt ASC NULLS FIRST", .want = "id,amt\n2,\n1,100\n3,200\n" },
        .{ .order = "amt NULLS FIRST LIMIT 2", .want = "id,amt\n2,\n1,100\n" },
        .{ .order = "amt DESC NULLS LAST LIMIT 1", .want = "id,amt\n3,200\n" },
    };
    for (cases) |c| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const sql = try std.fmt.allocPrint(alloc, "SELECT id, CAST(amount AS INT) AS amt FROM '$IN' ORDER BY {s}", .{c.order});
        defer alloc.free(sql);
        const out = try runToString(alloc, &tmp, "id,amount\n1,100\n2,\n3,200\n", sql);
        defer alloc.free(out);
        try std.testing.expectEqualStrings(c.want, out);
    }
}

test "window: NULLS FIRST / LAST inside OVER and in a named window order the rows numbered" {
    const alloc = std.testing.allocator;
    const cases = [_]struct { sql: []const u8, want: []const u8 }{
        .{ .sql = "SELECT id, ROW_NUMBER() OVER (ORDER BY a DESC) AS rn FROM '$IN' ORDER BY id", .want = "id,rn\n1,3\n2,1\n3,2\n" },
        .{ .sql = "SELECT id, ROW_NUMBER() OVER (ORDER BY a NULLS FIRST) AS rn FROM '$IN' ORDER BY id", .want = "id,rn\n1,2\n2,1\n3,3\n" },
        .{ .sql = "SELECT id, ROW_NUMBER() OVER w AS rn FROM '$IN' WINDOW w AS (ORDER BY a DESC NULLS LAST) ORDER BY id", .want = "id,rn\n1,2\n2,3\n3,1\n" },
        .{ .sql = "SELECT id, ROW_NUMBER() OVER (ORDER BY a) AS x, ROW_NUMBER() OVER (ORDER BY a NULLS FIRST) AS y FROM '$IN' ORDER BY id", .want = "id,x,y\n1,1,2\n2,3,1\n3,2,3\n" },
    };
    for (cases) |c| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const out = try runToString(alloc, &tmp, "id,a\n1,1\n2,\n3,5\n", c.sql);
        defer alloc.free(out);
        try std.testing.expectEqualStrings(c.want, out);
    }
}

test "distinct: multi-column key (value-keyed)" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "a,b\nx,1\nx,1\nx,2\ny,1\n",
        "SELECT DISTINCT ON (a, b) * FROM '$IN'",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("a,b\nx,1\nx,2\ny,1\n", out);
}

test "top-N: sort | limit fuses to the K largest, in order" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "id,amount\n1,100\n2,50\n3,200\n4,\n5,150\n",
        "SELECT id, CAST(amount AS INT) AS amt FROM '$IN' ORDER BY amt DESC NULLS LAST LIMIT 2",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,amt\n3,200\n5,150\n", out);
}

test "top-N: offset skips before taking" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "id,amount\n1,100\n2,50\n3,200\n4,\n5,150\n",
        "SELECT id, CAST(amount AS INT) AS amt FROM '$IN' ORDER BY amt DESC LIMIT 2 OFFSET 1",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,amt\n3,200\n5,150\n", out);
}

test "top-N: nulls sort first under DESC, matching a full sort | limit-all" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "id,amount\n1,100\n2,50\n3,200\n4,\n5,150\n",
        "SELECT id, CAST(amount AS INT) AS amt FROM '$IN' ORDER BY amt DESC LIMIT 99",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,amt\n4,\n3,200\n5,150\n1,100\n2,50\n", out);
}

test "join: an inner join drops the unmatched probe row" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,code\n1,A\n2,B\n3,Z\n" });
    try tmp.dir.writeFile(.{ .sub_path = "lookup.csv", .data = "code,label\nA,Apple\nB,Banana\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);
    const lookup_path = try std.fs.path.join(alloc, &.{ base, "lookup.csv" });
    defer alloc.free(lookup_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const script = try std.fmt.allocPrint(
        alloc,
        "LOAD INTO '{s}' AS\nWITH labels AS (SELECT * FROM '{s}')\nSELECT t.id, l.label FROM '{s}' t JOIN labels l ON t.code = l.code;",
        .{ out_path, lookup_path, in_path },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,label\n1,Apple\n2,Banana\n", out);
}

test "distinct dedups across multiple batches" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var in_buf = std.array_list.Managed(u8).init(alloc);
    defer in_buf.deinit();
    try in_buf.appendSlice("code\n");
    var i: usize = 0;
    while (i < 3000) : (i += 1) {
        try in_buf.writer().print("{c}\n", .{"XYZ"[i % 3]});
    }
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = in_buf.items });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(
        alloc,
        "LOAD INTO '{s}/out.csv' AS SELECT DISTINCT * FROM '{s}/in.csv';",
        .{ base, base },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("code\nX\nY\nZ\n", out);
}

test "join probe side spanning multiple batches" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var in_buf = std.array_list.Managed(u8).init(alloc);
    defer in_buf.deinit();
    try in_buf.appendSlice("id,code\n");
    var i: usize = 0;
    while (i < 2500) : (i += 1) {
        try in_buf.writer().print("{d},{s}\n", .{ i, if (i % 2 == 0) "A" else "B" });
    }
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = in_buf.items });
    try tmp.dir.writeFile(.{ .sub_path = "lookup.csv", .data = "code,label\nA,Apple\nB,Banana\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);
    const lookup_path = try std.fs.path.join(alloc, &.{ base, "lookup.csv" });
    defer alloc.free(lookup_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const script = try std.fmt.allocPrint(
        alloc,
        "LOAD INTO '{s}' AS\nWITH labels AS (SELECT * FROM '{s}')\nSELECT t.id, l.label FROM '{s}' t JOIN labels l ON t.code = l.code;",
        .{ out_path, lookup_path, in_path },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqual(@as(usize, 2501), std.mem.count(u8, out, "\n"));
    try std.testing.expect(std.mem.startsWith(u8, out, "id,label\n0,Apple\n1,Banana\n"));
    try std.testing.expect(std.mem.indexOf(u8, out, "\n2499,Banana\n") != null);
}

fn sortedLines(alloc: std.mem.Allocator, text: []const u8) ![][]const u8 {
    var lines = std.array_list.Managed([]const u8).init(alloc);
    var it = std.mem.splitScalar(u8, std.mem.trimRight(u8, text, "\n"), '\n');
    while (it.next()) |l| try lines.append(l);
    std.mem.sort([]const u8, lines.items, {}, struct {
        fn lt(_: void, x: []const u8, y: []const u8) bool {
            return std.mem.lessThan(u8, x, y);
        }
    }.lt);
    return lines.toOwnedSlice();
}

test "join spill: a build side past --op-memory spills to disk, serially and under -j, and gives every row" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    var orders = std.array_list.Managed(u8).init(alloc);
    defer orders.deinit();
    try orders.appendSlice("id,cust\n");
    var custs = std.array_list.Managed(u8).init(alloc);
    defer custs.deinit();
    try custs.appendSlice("cust,name\n");
    var want = std.array_list.Managed(u8).init(alloc);
    defer want.deinit();
    try want.appendSlice("id,name\n");
    for (0..3000) |i| {
        const c = (i * 7) % 900;
        try orders.writer().print("{d},{d}\n", .{ i, c });
        if (c % 3 == 0) try want.writer().print("{d},\n", .{i}) else try want.writer().print("{d},cust-{d}\n", .{ i, c });
    }
    for (0..1200) |c| if (c % 3 != 0) try custs.writer().print("{d},cust-{d}\n", .{ c, c });
    try tmp.dir.writeFile(.{ .sub_path = "orders.csv", .data = orders.items });
    try tmp.dir.writeFile(.{ .sub_path = "custs.csv", .data = custs.items });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    for ([_]struct { threads: usize, order: []const u8 }{ .{ .threads = 1, .order = " ORDER BY o.id" }, .{ .threads = 4, .order = "" } }) |case| {
        const script = try std.fmt.allocPrint(
            alloc,
            "LOAD INTO '{s}/out.csv' AS SELECT o.id, c.name FROM '{s}/orders.csv' o LEFT JOIN '{s}/custs.csv' c ON o.cust = c.cust{s};",
            .{ base, base, base, case.order },
        );
        defer alloc.free(script);
        var parena = std.heap.ArenaAllocator.init(alloc);
        defer parena.deinit();
        var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
        var rdiag: Diag = .{};
        _ = try run(alloc, prog, .{ .threads = case.threads, .op_memory = 1024, .spill_dir = base, .log = .{ .quiet = true } }, &rdiag);

        const out = try tmp.dir.readFileAlloc(alloc, "out.csv", 1 << 20);
        defer alloc.free(out);
        if (case.order.len > 0) {
            try std.testing.expectEqualStrings(want.items, out);
        } else {
            const got_lines = try sortedLines(alloc, out);
            defer alloc.free(got_lines);
            const want_lines = try sortedLines(alloc, want.items);
            defer alloc.free(want_lines);
            try std.testing.expectEqual(want_lines.len, got_lines.len);
            for (want_lines, got_lines) |w, g| try std.testing.expectEqualStrings(w, g);
        }

        var it = tmp.dir.iterate();
        while (try it.next()) |e| try std.testing.expect(!std.mem.startsWith(u8, e.name, "basalt-spill-"));

        var capped: Diag = .{};
        try std.testing.expectError(error.SpillCapExceeded, run(alloc, prog, .{ .threads = case.threads, .op_memory = 1024, .spill_dir = base, .spill_cap = 64, .log = .{ .quiet = true } }, &capped));
    }
}

test "join spill: partitions still past max_build split again, and one key past it fails naming the key" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    var orders = std.array_list.Managed(u8).init(alloc);
    defer orders.deinit();
    try orders.appendSlice("id,cust\n");
    var custs = std.array_list.Managed(u8).init(alloc);
    defer custs.deinit();
    try custs.appendSlice("cust,name\n");
    var want = std.array_list.Managed(u8).init(alloc);
    defer want.deinit();
    try want.appendSlice("id,name\n");
    for (0..60000) |i| {
        const c = (i * 7) % 70000;
        try orders.writer().print("{d},{d}\n", .{ i, c });
        if (c >= 50000 or c % 5 == 0) try want.writer().print("{d},\n", .{i}) else try want.writer().print("{d},n{d}\n", .{ i, c });
    }
    for (0..50000) |c| if (c % 5 != 0) try custs.writer().print("{d},n{d}\n", .{ c, c });
    try tmp.dir.writeFile(.{ .sub_path = "orders.csv", .data = orders.items });
    try tmp.dir.writeFile(.{ .sub_path = "custs.csv", .data = custs.items });
    var skew = std.array_list.Managed(u8).init(alloc);
    defer skew.deinit();
    try skew.appendSlice("cust,name\n");
    for (0..5000) |i| try skew.writer().print("7,s{d}\n", .{i});
    try tmp.dir.writeFile(.{ .sub_path = "skew.csv", .data = skew.items });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    for ([_]usize{ 1, 4 }) |threads| {
        const script = try std.fmt.allocPrint(
            alloc,
            "LOAD INTO '{s}/out.csv' AS SELECT o.id, c.name FROM '{s}/orders.csv' o LEFT JOIN '{s}/custs.csv' c ON o.cust = c.cust WITH (max_build = '4KB') ORDER BY o.id;",
            .{ base, base, base },
        );
        defer alloc.free(script);
        const out = try runScriptOpts(alloc, &tmp, script, .{ .threads = threads, .spill_dir = base, .log = .{ .quiet = true } });
        defer alloc.free(out);
        try std.testing.expectEqualStrings(want.items, out);
        var it = tmp.dir.iterate();
        while (try it.next()) |e| try std.testing.expect(!std.mem.startsWith(u8, e.name, "basalt-spill-"));
    }

    const script = try std.fmt.allocPrint(
        alloc,
        "LOAD INTO '{s}/out.csv' AS SELECT o.id, c.name FROM '{s}/orders.csv' o JOIN '{s}/skew.csv' c ON o.cust = c.cust WITH (max_build = '4KB');",
        .{ base, base, base },
    );
    defer alloc.free(script);
    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    try std.testing.expectError(error.JoinBuildTooLarge, run(alloc, prog, .{ .spill_dir = base, .log = .{ .quiet = true } }, &rdiag));
    try std.testing.expect(std.mem.indexOf(u8, rdiag.msg, "a single join key holds 5000 rows") != null);
    var it = tmp.dir.iterate();
    while (try it.next()) |e| try std.testing.expect(!std.mem.startsWith(u8, e.name, "basalt-spill-"));
}

test "cross join and NOT IN keep their in-memory caps" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var big = std.array_list.Managed(u8).init(alloc);
    defer big.deinit();
    try big.appendSlice("t\n");
    for (0..2000) |i| try big.writer().print("some longer text value {d}\n", .{i});
    try tmp.dir.writeFile(.{ .sub_path = "big.csv", .data = big.items });
    try tmp.dir.writeFile(.{ .sub_path = "l.csv", .data = "s\na\nb\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const cases = [_]struct { sql: []const u8, op_memory: ?usize, msg: []const u8 }{
        .{ .sql = "LOAD INTO '{s}/out.csv' AS SELECT count(*) AS n FROM '{s}/l.csv' s CROSS JOIN '{s}/big.csv' p WITH (max_build = '1KB');", .op_memory = null, .msg = "exceeds its cap" },
        .{ .sql = "LOAD INTO '{s}/out.csv' AS SELECT count(*) AS n FROM '{s}/l.csv' WHERE s NOT IN (SELECT t FROM '{s}/big.csv');", .op_memory = 1024, .msg = "NOT IN subquery's rows pass the 1024-byte limit" },
    };
    inline for (cases) |c| for ([_]usize{ 1, 4 }) |threads| {
        const script = try std.fmt.allocPrint(alloc, c.sql, .{ base, base, base });
        defer alloc.free(script);
        var parena = std.heap.ArenaAllocator.init(alloc);
        defer parena.deinit();
        var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
        var rdiag: Diag = .{};
        var opts: basalt.env.RunOptions = .{ .threads = threads, .log = .{ .quiet = true } };
        if (c.op_memory) |m| opts.op_memory = m;
        try std.testing.expectError(error.JoinBuildTooLarge, run(alloc, prog, opts, &rdiag));
        try std.testing.expect(std.mem.indexOf(u8, rdiag.msg, c.msg) != null);
    };

    const ok = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}/out.csv' AS SELECT count(*) AS n FROM '{s}/l.csv' s CROSS JOIN '{s}/big.csv' p;", .{ base, base, base });
    defer alloc.free(ok);
    const out = try runScriptOpts(alloc, &tmp, ok, .{ .log = .{ .quiet = true } });
    defer alloc.free(out);
    try std.testing.expectEqualStrings("n\n4000\n", out);
}

test "union reconciles branches to a canon schema (tag, null-fill, drop-extra)" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "a.csv", .data = "id,v\n1,10\n2,20\n" });
    try tmp.dir.writeFile(.{ .sub_path = "b.csv", .data = "id,w\n3,99\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(
        alloc,
        "LOAD INTO '{s}/out.csv' AS\nSELECT '01' AS src, t.* FROM '{s}/a.csv' t\nUNION ALL BY NAME\nSELECT '02' AS src, t.* FROM '{s}/b.csv' t\nANCHOR SCHEMA first;",
        .{ base, base, base },
    );
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    _ = run(alloc, prog, .{}, &rdiag) catch |e| {
        return e;
    };
    const out = try tmp.dir.readFileAlloc(alloc, "out.csv", 1 << 20);
    defer alloc.free(out);
    try std.testing.expectEqualStrings("src,id,v\n01,1,10\n01,2,20\n02,3,\n", out);
}

test "empty source and an all-dropping filter still write just the header" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const empty = try runToString(alloc, &tmp, "id,v\n", "SELECT id FROM '$IN'");
    defer alloc.free(empty);
    try std.testing.expectEqualStrings("id\n", empty);
    const dropped = try runToString(alloc, &tmp, "id,v\n1,10\n2,20\n", "SELECT * FROM '$IN' WHERE v = 999");
    defer alloc.free(dropped);
    try std.testing.expectEqualStrings("id,v\n", dropped);
}

test "limit: LIMIT and OFFSET take PARAMs, LETs, loop variables and constant expressions; FETCH FIRST is LIMIT" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "n.csv", .data = "id\n5\n3\n1\n4\n2\n" });
    try checkAndRun(alloc, &tmp,
        \\PARAM n INT DEFAULT 2;
        \\LET m = $n + 1;
        \\LOAD INTO '$B/param.csv' AS SELECT id FROM '$B/n.csv' ORDER BY id LIMIT $n OFFSET $n - 1;
        \\LOAD INTO '$B/let.csv' AS SELECT id FROM '$B/n.csv' ORDER BY id DESC LIMIT $m;
        \\LOAD INTO '$B/fetch.csv' AS SELECT id FROM '$B/n.csv' ORDER BY id OFFSET 1 ROWS FETCH FIRST 2 * 1 ROWS ONLY;
        \\LOAD INTO '$B/offset.csv' AS SELECT id FROM '$B/n.csv' ORDER BY id OFFSET 3;
        \\LOAD INTO '$B/cte.csv' AS WITH c AS (SELECT id FROM '$B/n.csv' ORDER BY id LIMIT $n + 0) SELECT * FROM c;
        \\FOR EACH ROW OF (SELECT id FROM '$B/n.csv' WHERE id = 3) AS (k)
        \\  LOAD INTO '$B/loop.csv' AS SELECT id FROM '$B/n.csv' ORDER BY id LIMIT $k;
        \\END FOR;
        \\LET top = (SELECT id FROM '$B/n.csv' ORDER BY id DESC LIMIT $n - 1);
        \\LOAD INTO '$B/scalar.csv' AS SELECT $top AS t;
        \\FOR EACH ROW OF (SELECT id FROM '$B/n.csv' ORDER BY id LIMIT $n) AS (k)
        \\  LOAD INTO IDENTIFIER('$B/d_' || $k || '.csv') AS SELECT $k AS k;
        \\END FOR;
    , 1, &.{});
    try expectFile(&tmp, "scalar.csv", "t\n5\n");
    try expectFile(&tmp, "d_1.csv", "k\n1\n");
    try expectFile(&tmp, "d_2.csv", "k\n2\n");
    try std.testing.expectError(error.FileNotFound, tmp.dir.access("d_3.csv", .{}));
    try expectFile(&tmp, "param.csv", "id\n2\n3\n");
    try expectFile(&tmp, "let.csv", "id\n5\n4\n3\n");
    try expectFile(&tmp, "fetch.csv", "id\n2\n3\n");
    try expectFile(&tmp, "offset.csv", "id\n4\n5\n");
    try expectFile(&tmp, "cte.csv", "id\n1\n2\n");
    try expectFile(&tmp, "loop.csv", "id\n1\n2\n3\n");

    try expectRefusedAlike(alloc, &tmp, "SELECT id FROM '$B/n.csv' LIMIT 1 - 2;", "LIMIT must not be negative, got -1");
}

test "limit: plain (unfused) limit takes the first N; offset past the end empties" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const first2 = try runToString(alloc, &tmp, "id\n1\n2\n3\n", "SELECT * FROM '$IN' LIMIT 2");
    defer alloc.free(first2);
    try std.testing.expectEqualStrings("id\n1\n2\n", first2);
    const none = try runToString(alloc, &tmp, "id\n1\n2\n3\n", "SELECT * FROM '$IN' LIMIT 5 OFFSET 100");
    defer alloc.free(none);
    try std.testing.expectEqualStrings("id\n", none);
}

test "limit: a fraction rounds as PostgreSQL casts it; a negative one is refused" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cases = [_]struct { q: []const u8, want: []const u8 }{
        .{ .q = "SELECT * FROM '$IN' LIMIT 2.5", .want = "id\n1\n2\n3\n" },
        .{ .q = "SELECT * FROM '$IN' LIMIT 1.4999 OFFSET 1.5", .want = "id\n3\n" },
        .{ .q = "SELECT * FROM '$IN' LIMIT -0.4", .want = "id\n" },
        .{ .q = "SELECT * FROM '$IN' LIMIT CAST(2.5 AS FLOAT)", .want = "id\n1\n2\n" },
        .{ .q = "SELECT * FROM '$IN' LIMIT CAST(1.5 AS DECIMAL(3,1))", .want = "id\n1\n2\n" },
    };
    for (cases) |c| {
        const got = try runToString(alloc, &tmp, "id\n1\n2\n3\n4\n", c.q);
        defer alloc.free(got);
        try std.testing.expectEqualStrings(c.want, got);
    }
    try expectRefusedAlike(alloc, &tmp, "SELECT 1 AS id LIMIT CAST(-1.5 AS DECIMAL(3,1));", "LIMIT must not be negative, got -2");
}

test "csv: a column with no value in the whole file compares and unifies with any type, as a NULL does" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "empty.csv", .data = "k\n" });
    try tmp.dir.writeFile(.{ .sub_path = "nulls.csv", .data = "k,n\n1,\n2,\n" });
    const cases = [_]struct { q: []const u8, want: []const u8 }{
        .{ .q = "SELECT id FROM '$IN' WHERE id NOT IN (SELECT k FROM '$B/empty.csv')", .want = "id\n1\n2\n" },
        .{ .q = "SELECT id FROM '$IN' WHERE id NOT IN (SELECT n FROM '$B/nulls.csv')", .want = "id\n" },
        .{ .q = "SELECT t.id, n.k FROM '$IN' t LEFT JOIN '$B/nulls.csv' n ON t.id = n.n", .want = "id,k\n1,\n2,\n" },
        .{ .q = "SELECT id FROM '$IN' UNION ALL SELECT n FROM '$B/nulls.csv'", .want = "id\n1\n2\n\n\n" },
        .{ .q = "SELECT k, coalesce(n, 0) + 1 AS c FROM '$B/nulls.csv'", .want = "k,c\n1,1\n2,1\n" },
    };
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    for (cases) |c| {
        const q = try std.mem.replaceOwned(u8, alloc, c.q, "$B", base);
        defer alloc.free(q);
        const got = try runToString(alloc, &tmp, "id\n1\n2\n", q);
        defer alloc.free(got);
        try std.testing.expectEqualStrings(c.want, got);
    }
}

test "join: an empty build side drops all rows (inner) and null-fills (left)" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,code\n1,A\n2,B\n" });
    try tmp.dir.writeFile(.{ .sub_path = "lookup.csv", .data = "code,label\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}/inner.csv' AS WITH labels AS (SELECT * FROM '{s}/lookup.csv') " ++
        "SELECT t.id, l.label FROM '{s}/in.csv' t JOIN labels l ON t.code = l.code;\n" ++
        "LOAD INTO '{s}/left.csv' AS WITH labels AS (SELECT * FROM '{s}/lookup.csv') " ++
        "SELECT t.id, l.label FROM '{s}/in.csv' t LEFT JOIN labels l ON t.code = l.code;", .{ base, base, base, base, base, base });
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    _ = run(alloc, prog, .{}, &rdiag) catch |e| {
        return e;
    };
    const inner = try tmp.dir.readFileAlloc(alloc, "inner.csv", 1 << 20);
    defer alloc.free(inner);
    try std.testing.expectEqualStrings("id,label\n", inner);
    const left = try tmp.dir.readFileAlloc(alloc, "left.csv", 1 << 20);
    defer alloc.free(left);
    try std.testing.expectEqualStrings("id,label\n1,\n2,\n", left);
}

test "join: two-key ON matches on both columns" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,day,code\n1,mon,A\n2,tue,A\n3,mon,B\n" });
    try tmp.dir.writeFile(.{ .sub_path = "lookup.csv", .data = "day,code,label\nmon,A,first\ntue,A,second\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);
    const lookup_path = try std.fs.path.join(alloc, &.{ base, "lookup.csv" });
    defer alloc.free(lookup_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const script = try std.fmt.allocPrint(
        alloc,
        "LOAD INTO '{s}' AS\nWITH labels AS (SELECT * FROM '{s}')\n" ++
            "SELECT t.id, l.label FROM '{s}' t JOIN labels l ON t.code = l.code AND l.day = t.day;",
        .{ out_path, lookup_path, in_path },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,label\n1,first\n2,second\n", out);
}

test "join: computed keys of bare columns take their side from where the columns are, in either order" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "sheet.csv", .data = "cr,flag\n1005,yes\n1048,no\n" });
    try tmp.dir.writeFile(.{ .sub_path = "centers.csv", .data = "cc_code,cc_name\n1005  ,North\n1048  ,South\n2000  ,West\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const sheet = try std.fs.path.join(alloc, &.{ base, "sheet.csv" });
    defer alloc.free(sheet);
    const centers = try std.fs.path.join(alloc, &.{ base, "centers.csv" });
    defer alloc.free(centers);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    for ([_][]const u8{ "trim(cc_code) = CAST(cr AS varchar)", "CAST(cr AS varchar) = trim(cc_code)" }) |on| {
        const script = try std.fmt.allocPrint(
            alloc,
            "LOAD INTO '{s}' AS\nWITH base AS (SELECT * FROM '{s}' WHERE trim(cc_code) <> '2000')\n" ++
                "SELECT * FROM '{s}' INNER JOIN base ON {s};",
            .{ out_path, centers, sheet, on },
        );
        defer alloc.free(script);
        const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
        defer alloc.free(out);
        try std.testing.expectEqualStrings("cr,flag,cc_code,cc_name\n1005,yes,1005  ,North\n1048,no,1048  ,South\n", out);
    }
}

test "aggregate: SUM over text is exact, typed by the sampled scale; AVG stays FLOAT" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const input = "g,s,i,u\na,\"1.5\",\"9007199254740993\",1_000\na,\"2.25\",\"1\",2\nb,\"3\",\"2\",3\n";
    const out = try runToString(alloc, &tmp, input, "SELECT g, SUM(s) AS s, SUM(i) AS i, AVG(s) AS a, MAX(u) AS u FROM '$IN' GROUP BY g ORDER BY g");
    defer alloc.free(out);
    try std.testing.expectEqualStrings("g,s,i,a,u\na,3.75,9007199254740994,1.875,2\nb,3.00,2,3,3\n", out);
    const win = try runToString(alloc, &tmp, input, "SELECT g, SUM(s) OVER (PARTITION BY g) AS w, SUM(i) OVER (PARTITION BY g) AS wi FROM '$IN' ORDER BY g, w");
    defer alloc.free(win);
    try std.testing.expectEqualStrings("g,w,wi\na,3.75,9007199254740994\na,3.75,9007199254740994\nb,3.00,2\n", win);
}

test "aggregate: SUM over text refuses what it cannot add exactly, naming the CAST that can" {
    const alloc = std.testing.allocator;
    var late: std.Io.Writer.Allocating = .init(alloc);
    defer late.deinit();
    try late.writer.writeAll("s\n");
    for (0..1100) |_| try late.writer.writeAll("\"1.5\"\n");
    try late.writer.writeAll("\"1.255\"\n");
    const cases = [_]struct { input: []const u8, want: []const u8 }{
        .{ .input = late.written(), .want = "'1.255' has 3 decimal places, more than the 1" },
        .{ .input = "s\n\"1\"\n\"1e3\"\n", .want = "sum CAST(x AS FLOAT)" },
        .{ .input = "s\n\"1\"\n\"nan\"\n", .want = "sum CAST(x AS FLOAT)" },
        .{ .input = "s\n\"1\"\n\"abc\"\n", .want = "'abc' is not a number" },
    };
    for (cases) |c| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = c.input });
        const base = try tmp.dir.realpathAlloc(alloc, ".");
        defer alloc.free(base);
        const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}/out.csv' AS SELECT SUM(s) AS s FROM '{s}/in.csv';", .{ base, base });
        defer alloc.free(script);
        var parena = std.heap.ArenaAllocator.init(alloc);
        defer parena.deinit();
        var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
        var rdiag: Diag = .{};
        if (run(alloc, prog, .{ .log = .{ .quiet = true } }, &rdiag)) |_| return error.TestUnexpectedResult else |_| {}
        if (std.mem.indexOf(u8, rdiag.msg, c.want) == null) {
            std.debug.print("want `{s}` in `{s}`\n", .{ c.want, rdiag.msg });
            return error.TestUnexpectedResult;
        }
    }
}

test "join: an outer join's ON beyond its keys picks matching pairs and keeps the rest unmatched" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "sales.csv", .data = "id,product,sold_on\n1,A,2026-01-10\n2,A,2026-02-15\n3,B,2026-03-05\n4,C,2026-01-01\n" });
    try tmp.dir.writeFile(.{ .sub_path = "prices.csv", .data = "product,valid_from,valid_to,price\nA,2026-01-01,2026-01-31,10\nA,2026-02-01,2026-12-31,12\nB,2026-01-01,2026-02-28,7\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const sales = try std.fs.path.join(alloc, &.{ base, "sales.csv" });
    defer alloc.free(sales);
    const prices = try std.fs.path.join(alloc, &.{ base, "prices.csv" });
    defer alloc.free(prices);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const cases = .{
        .{
            "SELECT s.id, p.price FROM '{s}' s LEFT JOIN '{s}' p ON p.product = s.product AND CAST(s.sold_on AS DATE) BETWEEN CAST(p.valid_from AS DATE) AND CAST(p.valid_to AS DATE) ORDER BY s.id",
            "id,price\n1,10\n2,12\n3,\n4,\n",
        },
        .{
            "SELECT p.price, s.id FROM '{s}' s RIGHT JOIN '{s}' p ON p.product = s.product AND p.price > 9 ORDER BY p.price, s.id",
            "price,id\n7,\n10,1\n10,2\n12,1\n12,2\n",
        },
    };
    inline for (cases) |c| {
        const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}' AS " ++ c[0] ++ ";", .{ out_path, sales, prices });
        defer alloc.free(script);
        const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
        defer alloc.free(out);
        try std.testing.expectEqualStrings(c[1], out);
    }
}

test "join: an ON with no `=` key runs as a nested-loop or range join, of any kind" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "sales.csv", .data = "id,sold_on\n1,2026-01-10\n2,2026-02-15\n3,2025-12-31\n4,2026-03-01\n" });
    try tmp.dir.writeFile(.{ .sub_path = "periods.csv", .data = "name,starts,ends\nJan,2026-01-01,2026-01-31\nFeb,2026-02-01,2026-02-28\nMar,2026-03-01,2026-03-31\n" });
    try tmp.dir.writeFile(.{ .sub_path = "a.csv", .data = "v\n1\n5\n9\n" });
    try tmp.dir.writeFile(.{ .sub_path = "b.csv", .data = "w\n3\n7\n" });
    try tmp.dir.writeFile(.{ .sub_path = "c.csv", .data = "lo,hi\n0,2\n4,6\n20,30\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const cases = .{
        .{
            "SELECT s.id, p.name FROM '{s}/sales.csv' s LEFT JOIN '{s}/periods.csv' p ON CAST(s.sold_on AS DATE) BETWEEN CAST(p.starts AS DATE) AND CAST(p.ends AS DATE) ORDER BY s.id",
            "id,name\n1,Jan\n2,Feb\n3,\n4,Mar\n",
        },
        .{
            "SELECT a.v, b.w FROM '{s}/a.csv' a JOIN '{s}/b.csv' b ON a.v < b.w ORDER BY a.v, b.w",
            "v,w\n1,3\n1,7\n5,7\n",
        },
        .{
            "SELECT a.v, c.lo FROM '{s}/a.csv' a FULL JOIN '{s}/c.csv' c ON a.v BETWEEN c.lo AND c.hi ORDER BY a.v, c.lo",
            "v,lo\n1,0\n5,4\n9,\n,20\n",
        },
        .{
            "SELECT a.v, b.w FROM '{s}/a.csv' a RIGHT JOIN '{s}/b.csv' b ON a.v = b.w OR a.v + 2 = b.w ORDER BY b.w",
            "v,w\n1,3\n5,7\n",
        },
    };
    inline for (cases) |c| {
        const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}' AS " ++ c[0] ++ ";", .{ out_path, base, base });
        defer alloc.free(script);
        const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
        defer alloc.free(out);
        try std.testing.expectEqualStrings(c[1], out);
    }
}

test "join: USING lists each shared column once, first, and a full join takes it from either side" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "a.csv", .data = "v,k\n10,1\n20,2\n" });
    try tmp.dir.writeFile(.{ .sub_path = "b.csv", .data = "k,w\n2,x\n3,y\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const a = try std.fs.path.join(alloc, &.{ base, "a.csv" });
    defer alloc.free(a);
    const b = try std.fs.path.join(alloc, &.{ base, "b.csv" });
    defer alloc.free(b);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const cases = .{
        .{ "SELECT * FROM '{s}' a JOIN '{s}' b USING (k)", "k,v,w\n2,20,x\n" },
        .{ "SELECT * FROM '{s}' a FULL JOIN '{s}' b USING (k) ORDER BY k", "k,v,w\n1,10,\n2,20,x\n3,,y\n" },
    };
    inline for (cases) |c| {
        const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}' AS " ++ c[0] ++ ";", .{ out_path, a, b });
        defer alloc.free(script);
        const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
        defer alloc.free(out);
        try std.testing.expectEqualStrings(c[1], out);
    }
}

test "search: words across the chosen columns, after a select and a join, in ON too" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "orders.csv", .data = "id,cust,note\n1,10,urgent north\n2,11,\n3,10,north pallet\n4,12,south\n" });
    try tmp.dir.writeFile(.{ .sub_path = "custs.csv", .data = "cust,name,region\n10,Acme North,north\n11,Beta,south\n12,Gamma North,east\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const orders = try std.fs.path.join(alloc, &.{ base, "orders.csv" });
    defer alloc.free(orders);
    const custs = try std.fs.path.join(alloc, &.{ base, "custs.csv" });
    defer alloc.free(custs);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const cases = .{
        .{ "SELECT id FROM '{s}' o WHERE search(*, 'NORTH -urgent') ORDER BY id", "id\n3\n", false },
        .{ "SELECT id FROM (SELECT id, note AS memo FROM '{s}') WHERE search(*, 'pallet')", "id\n3\n", false },
        .{ "SELECT id, search(* EXCEPT (note), 'north') AS a, search((note), 'north') AS b FROM '{s}' o ORDER BY id", "id,a,b\n1,false,true\n2,false,false\n3,false,true\n4,false,false\n", false },
        .{ "SELECT o.id FROM '{s}' o JOIN '{s}' c ON c.cust = o.cust WHERE search(c.*, 'north') ORDER BY o.id", "id\n1\n3\n4\n", true },
        .{ "SELECT o.id FROM '{s}' o JOIN '{s}' c ON c.cust = o.cust WHERE search(o.*, 'north') ORDER BY o.id", "id\n1\n3\n", true },
        .{ "SELECT o.id, c.name FROM '{s}' o LEFT JOIN '{s}' c ON c.cust = o.cust AND search(c.*, 'north') ORDER BY o.id", "id,name\n1,Acme North\n2,\n3,Acme North\n4,Gamma North\n", true },
    };
    inline for (cases) |c| {
        const script = if (c[2])
            try std.fmt.allocPrint(alloc, "LOAD INTO '{s}' AS " ++ c[0] ++ ";", .{ out_path, orders, custs })
        else
            try std.fmt.allocPrint(alloc, "LOAD INTO '{s}' AS " ++ c[0] ++ ";", .{ out_path, orders });
        defer alloc.free(script);
        const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
        defer alloc.free(out);
        try std.testing.expectEqualStrings(c[1], out);
    }
}

test "join: duplicate build keys fan out (inner); semi/anti reduce to existence" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,code\n1,A\n2,Z\n" });
    try tmp.dir.writeFile(.{ .sub_path = "lookup.csv", .data = "code,label\nA,x1\nA,x2\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}/inner.csv' AS WITH labels AS (SELECT * FROM '{s}/lookup.csv') " ++
        "SELECT t.id, l.label FROM '{s}/in.csv' t JOIN labels l ON t.code = l.code;\n" ++
        "LOAD INTO '{s}/semi.csv' AS WITH labels AS (SELECT * FROM '{s}/lookup.csv') " ++
        "SELECT * FROM '{s}/in.csv' t SEMI JOIN labels l ON t.code = l.code;\n" ++
        "LOAD INTO '{s}/anti.csv' AS WITH labels AS (SELECT * FROM '{s}/lookup.csv') " ++
        "SELECT * FROM '{s}/in.csv' t ANTI JOIN labels l ON t.code = l.code;", .{ base, base, base, base, base, base, base, base, base });
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    _ = run(alloc, prog, .{}, &rdiag) catch |e| {
        return e;
    };
    const inner = try tmp.dir.readFileAlloc(alloc, "inner.csv", 1 << 20);
    defer alloc.free(inner);
    try std.testing.expectEqualStrings("id,label\n1,x1\n1,x2\n", inner);
    const semi = try tmp.dir.readFileAlloc(alloc, "semi.csv", 1 << 20);
    defer alloc.free(semi);
    try std.testing.expectEqualStrings("id,code\n1,A\n", semi);
    const anti = try tmp.dir.readFileAlloc(alloc, "anti.csv", 1 << 20);
    defer alloc.free(anti);
    try std.testing.expectEqualStrings("id,code\n2,Z\n", anti);
}

test "generated sources: SELECT with no FROM and RANGE(lo, hi)" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);

    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}/one.csv' AS SELECT 1 AS x, 'a' AS s;\n" ++
        "LOAD INTO '{s}/r.csv' AS SELECT range AS i FROM RANGE(2, 5) WHERE range != 3;\n", .{ base, base });
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    _ = try run(alloc, prog, .{}, &rdiag);

    const one = try tmp.dir.readFileAlloc(alloc, "one.csv", 1 << 20);
    defer alloc.free(one);
    try std.testing.expectEqualStrings("x,s\n1,a\n", one);
    const r = try tmp.dir.readFileAlloc(alloc, "r.csv", 1 << 20);
    defer alloc.free(r);
    try std.testing.expectEqualStrings("i\n2\n4\n", r);
}

test "decimal arithmetic is exact: + - * / stay DECIMAL, / at PostgreSQL's numeric scale" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const got = try runToString(alloc, &tmp, "a,b\n1.1,0.3\n28875360.09,21199414.08\n",
        \\SELECT CAST(a AS DECIMAL(18,2)) + CAST(b AS DECIMAL(18,2)) AS s,
        \\       CAST(a AS DECIMAL(18,2)) - CAST(b AS DECIMAL(18,2)) AS d,
        \\       CAST(a AS DECIMAL(18,2)) * CAST(b AS DECIMAL(18,2)) AS p,
        \\       CAST(a AS DECIMAL(18,2)) * 3 AS p3,
        \\       CAST(a AS DECIMAL(18,2)) / 2 AS q
        \\FROM '$IN'
    );
    defer alloc.free(got);
    try std.testing.expectEqualStrings("s,d,p,p3,q\n1.40,0.80,0.3300,3.30,0.55000000000000000000\n50074774.17,7675946.01,612140715257016.0672,86626080.27,14437680.045000000000\n", got);
}

test "numeric literals and division over columns answer as PostgreSQL does" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const got = try runToString(alloc, &tmp, "i,f,d,n\n7,0.1,1.25,\n-7,2.5,2.50,1.5\n",
        \\SELECT i / 2 AS h, i * 0.5 AS m, CAST(d AS DECIMAL(10,2)) / 3 AS q, f > 0.1 AS g,
        \\       i < 2.5 AS lt, coalesce(n, 0.0) AS c, i / 2e0 AS fl
        \\FROM '$IN'
    );
    defer alloc.free(got);
    try std.testing.expectEqualStrings("h,m,q,g,lt,c,fl\n3,3.5,0.41666666666666666667,false,false,0,3.5\n-3,-3.5,0.83333333333333333333,true,true,1.5,-3.5\n", got);
}

test "table functions: defaults, a join side, two calls with their own CTEs, one calling another" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const input = "id,g,x,status\n1,a,10,paid\n2,a,20,open\n3,b,30,paid\n4,b,5,paid\n5,c,7,open\n";
    const decls =
        \\CREATE FUNCTION paid(minx INT DEFAULT 0) RETURNS TABLE AS
        \\  SELECT id, g, x FROM '$IN' WHERE status = 'paid' AND x >= $minx;
        \\CREATE FUNCTION byg(k STRING) RETURNS TABLE AS
        \\  WITH s AS (SELECT g, SUM(x) AS t FROM '$IN' GROUP BY g) SELECT t FROM s WHERE g = $k;
        \\CREATE FUNCTION per_g(n INT) RETURNS TABLE AS
        \\  SELECT g, COUNT(*) AS c FROM paid($n) GROUP BY g;
        \\
    ;
    const cases = [_]struct { q: []const u8, want: []const u8 }{
        .{ .q = "SELECT id, x FROM paid(8) ORDER BY id", .want = "id,x\n1,10\n3,30\n" },
        .{ .q = "SELECT id FROM paid() p WHERE p.g = 'b' ORDER BY id", .want = "id\n3\n4\n" },
        .{ .q = "SELECT a.id, b.id AS bid FROM paid(8) a JOIN paid(25) b ON a.g = b.g", .want = "id,bid\n3,3\n" },
        .{ .q = "SELECT * FROM byg('a') x CROSS JOIN byg('b') y", .want = "t,t_r\n30,35\n" },
        .{ .q = "SELECT * FROM per_g(6) ORDER BY g", .want = "g,c\na,1\nb,1\n" },
    };
    for (cases) |c| {
        const base = try tmp.dir.realpathAlloc(alloc, ".");
        defer alloc.free(base);
        try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = input });
        const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
        defer alloc.free(in_path);
        const d = try std.mem.replaceOwned(u8, alloc, decls, "$IN", in_path);
        defer alloc.free(d);
        const script = try std.fmt.allocPrint(alloc, "{s}LOAD INTO '{s}/out.csv' AS {s};", .{ d, base, c.q });
        defer alloc.free(script);
        var parena = std.heap.ArenaAllocator.init(alloc);
        defer parena.deinit();
        var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
        var rdiag: Diag = .{};
        _ = run(alloc, prog, .{}, &rdiag) catch |e| {
            return e;
        };
        const got = try tmp.dir.readFileAlloc(alloc, "out.csv", 1 << 20);
        defer alloc.free(got);
        try std.testing.expectEqualStrings(c.want, got);
    }
}

test "JSON array lambdas: filter, transform, any, all — elements as themselves, mixed kinds null, nested" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const input =
        \\id|tags|items
        \\1|["vip-gold","new","vip-silver"]|[{"sku":"A1","qty":2,"dims":{"w":1}},{"sku":"B2","qty":0}]
        \\2|["new"]|[]
        \\3||[{"sku":"C3","qty":5}]
        \\4|[1,7,12]|[{"sku":"D4","qty":1}]
        \\
    ;
    const got = try runToString(alloc, &tmp, input,
        \\SELECT id,
        \\       json_filter(tags, t -> t LIKE 'vip%') AS vip,
        \\       json_transform(items, i -> json_get(i, 'sku')) AS skus,
        \\       json_transform(items, i -> json_get(i, 'dims')) AS dims,
        \\       json_any(items, i -> CAST(json_get(i, 'qty') AS INT) = 0) AS zero,
        \\       json_all(tags, t -> t = 'new') AS only_new,
        \\       json_filter(tags, t -> t > id + 5) AS big,
        \\       json_transform(items, i -> json_any(tags, t -> t LIKE 'vip%')) AS nested
        \\FROM '$IN' WITH (delimiter = '|') ORDER BY id
    );
    defer alloc.free(got);
    try std.testing.expectEqualStrings(
        \\id,vip,skus,dims,zero,only_new,big,nested
        \\1,"[""vip-gold"",""vip-silver""]","[""A1"",""B2""]","[{""w"":1},null]",true,false,[],"[true,true]"
        \\2,[],[],[],false,true,[],[]
        \\3,,"[""C3""]",[null],false,,,[null]
        \\4,[],"[""D4""]",[null],false,false,[12],[false]
        \\
    , got);
}

test "JOIN LATERAL passes a row's column to a table function as a join on it" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const input = "kind,num,prod,qty\no,1,,\no,2,,\no,,,\no,4,,\ni,1,X,5\ni,1,Y,2\ni,2,Z,1\ni,,N,9\n";
    const fns =
        \\CREATE FUNCTION itens(pedido, minq INT DEFAULT 0) RETURNS TABLE AS
        \\  SELECT num AS item_num, prod, qty, $pedido AS eco FROM '$IN' WHERE kind = 'i' AND qty > $minq AND num = $pedido;
        \\CREATE FUNCTION nomes(pedido) RETURNS TABLE AS
        \\  SELECT prod FROM '$IN' WHERE num = $pedido AND kind = 'i';
        \\
    ;
    const cases = [_]struct { q: []const u8, want: []const u8 }{
        .{ .q = "SELECT o.num, i.prod, i.eco FROM (SELECT num FROM '$IN' WHERE kind = 'o') o CROSS JOIN LATERAL itens(o.num, 1) i ORDER BY o.num, i.prod", .want = "num,prod,eco\n1,X,1\n1,Y,1\n" },
        .{ .q = "SELECT o.num, i.prod FROM (SELECT num FROM '$IN' WHERE kind = 'o') o LEFT JOIN LATERAL itens(o.num) i ON TRUE ORDER BY o.num, i.prod", .want = "num,prod\n1,X\n1,Y\n2,Z\n4,\n,\n" },
        .{ .q = "SELECT o.num, n.prod, n.pedido FROM (SELECT num FROM '$IN' WHERE kind = 'o') o JOIN LATERAL nomes(o.num) n ORDER BY n.prod", .want = "num,prod,pedido\n1,X,1\n1,Y,1\n2,Z,2\n" },
    };
    for (cases) |c| {
        const q = try std.fmt.allocPrint(alloc, "{s}", .{c.q});
        defer alloc.free(q);
        try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = input });
        const base = try tmp.dir.realpathAlloc(alloc, ".");
        defer alloc.free(base);
        const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
        defer alloc.free(in_path);
        const raw = try std.fmt.allocPrint(alloc, "{s}LOAD INTO '{s}/out.csv' AS {s};", .{ fns, base, c.q });
        defer alloc.free(raw);
        const script = try std.mem.replaceOwned(u8, alloc, raw, "$IN", in_path);
        defer alloc.free(script);
        var parena = std.heap.ArenaAllocator.init(alloc);
        defer parena.deinit();
        var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        const prog = parser.parseSource(parena.allocator(), script, &pdiag) catch |e| {
            std.debug.print("parse error: {s}\n", .{pdiag.msg});
            return e;
        };
        var rdiag: Diag = .{};
        _ = run(alloc, prog, .{}, &rdiag) catch |e| {
            return e;
        };
        const got = try tmp.dir.readFileAlloc(alloc, "out.csv", 1 << 20);
        defer alloc.free(got);
        try std.testing.expectEqualStrings(c.want, got);
    }
}

test "sort: the radix fast path keeps input order on ties, honors DESC and multi-key; nulls sort first under DESC" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const desc = try runToString(alloc, &tmp, "i,k\n0,1\n1,0\n2,1\n4,0\n", "SELECT i FROM '$IN' ORDER BY k DESC");
    defer alloc.free(desc);
    try std.testing.expectEqualStrings("i\n0\n2\n1\n4\n", desc);
    const multi = try runToString(alloc, &tmp, "i,k\n0,1\n1,0\n2,1\n4,0\n", "SELECT i FROM '$IN' ORDER BY k, i DESC");
    defer alloc.free(multi);
    try std.testing.expectEqualStrings("i\n4\n1\n2\n0\n", multi);
    const nulls = try runToString(alloc, &tmp, "i,k\n0,1\n1,0\n2,1\n3,\n4,0\n", "SELECT i FROM '$IN' ORDER BY k DESC");
    defer alloc.free(nulls);
    try std.testing.expectEqualStrings("i\n3\n0\n2\n1\n4\n", nulls);
}

test "sort: an ORDER BY past --op-memory spills sorted runs and merges them to the in-memory result" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const Row = struct { id: usize, k: ?u32, s: u32 };
    const n = 5000;
    const rows = try alloc.alloc(Row, n);
    defer alloc.free(rows);
    var in = std.array_list.Managed(u8).init(alloc);
    defer in.deinit();
    try in.appendSlice("id,k,s\n");
    for (rows, 0..) |*r, i| {
        r.* = .{ .id = i, .k = if (i % 13 == 0) null else @intCast((i * 7919) % 97), .s = @intCast((i * 31) % 50) };
        if (r.k) |k| try in.print("{d},{d},s{d}\n", .{ i, k, r.s }) else try in.print("{d},,s{d}\n", .{ i, r.s });
    }
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = in.items });
    try tmp.dir.makeDir("spill");

    const Ctx = struct {
        fn less(_: void, a: Row, b: Row) bool {
            if (a.k == null or b.k == null) {
                if (a.k == null and b.k == null) return sLess(a.s, b.s);
                return a.k == null;
            }
            if (a.k.? != b.k.?) return a.k.? > b.k.?;
            return sLess(a.s, b.s);
        }
        fn sLess(x: u32, y: u32) bool {
            var bx: [16]u8 = undefined;
            var by: [16]u8 = undefined;
            const sx = std.fmt.bufPrint(&bx, "s{d}", .{x}) catch unreachable;
            const sy = std.fmt.bufPrint(&by, "s{d}", .{y}) catch unreachable;
            return std.mem.order(u8, sx, sy) == .lt;
        }
    };
    std.mem.sort(Row, rows, {}, Ctx.less);
    var want = std.array_list.Managed(u8).init(alloc);
    defer want.deinit();
    try want.appendSlice("id,k,s\n");
    for (rows) |r| {
        if (r.k) |k| try want.print("{d},{d},s{d}\n", .{ r.id, k, r.s }) else try want.print("{d},,s{d}\n", .{ r.id, r.s });
    }

    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const spill_dir = try std.fs.path.join(alloc, &.{ base, "spill" });
    defer alloc.free(spill_dir);
    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}/out.csv' AS SELECT id, k, s FROM '{s}/in.csv' ORDER BY k DESC, s;", .{ base, base });
    defer alloc.free(script);
    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    for ([_]usize{ 2 << 30, 1, 20_000 }) |op_memory| {
        var rdiag: Diag = .{};
        _ = try run(alloc, prog, .{ .op_memory = op_memory, .spill_dir = spill_dir, .log = .{ .quiet = true } }, &rdiag);
        const out = try tmp.dir.readFileAlloc(alloc, "out.csv", 1 << 20);
        defer alloc.free(out);
        try std.testing.expectEqualStrings(want.items, out);
        var sd = try tmp.dir.openDir("spill", .{ .iterate = true });
        defer sd.close();
        var it = sd.iterate();
        try std.testing.expect((try it.next()) == null);
    }

    var rdiag: Diag = .{};
    try std.testing.expectError(error.SpillCapExceeded, run(alloc, prog, .{ .op_memory = 1, .spill_dir = spill_dir, .spill_cap = 1024, .log = .{ .quiet = true } }, &rdiag));
}

test "spill: a LIMIT over a spilled sort, join, GROUP BY or DISTINCT gives its disk back before the next statement" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var in = std.array_list.Managed(u8).init(alloc);
    defer in.deinit();
    try in.appendSlice("id,k,s\n");
    for (0..4000) |i| try in.print("{d},{d},s{d}\n", .{ i, (i * 7919) % 4001, i % 50 });
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = in.items });
    try tmp.dir.makeDir("spill");
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const spill_dir = try std.fs.path.join(alloc, &.{ base, "spill" });
    defer alloc.free(spill_dir);

    const cases = [_]struct { early: []const u8, full: []const u8 }{
        .{ .early = "SELECT id FROM (SELECT id, k FROM '{0s}/in.csv' ORDER BY k) t WHERE id >= 0 LIMIT 1", .full = "SELECT count(*) AS n FROM (SELECT id FROM '{0s}/in.csv' ORDER BY k)" },
        .{ .early = "SELECT a.id FROM '{0s}/in.csv' a JOIN '{0s}/in.csv' b ON a.k = b.k LIMIT 1", .full = "SELECT count(*) AS n FROM '{0s}/in.csv' a JOIN '{0s}/in.csv' b ON a.k = b.k" },
        .{ .early = "SELECT id FROM (SELECT id, max(k) AS m FROM '{0s}/in.csv' GROUP BY id) t WHERE id >= 0 LIMIT 1", .full = "SELECT count(*) AS n FROM (SELECT id, max(k) AS m FROM '{0s}/in.csv' GROUP BY id)" },
        .{ .early = "SELECT id FROM (SELECT DISTINCT id, k FROM '{0s}/in.csv') t WHERE id >= 0 LIMIT 1", .full = "SELECT count(*) AS n FROM (SELECT DISTINCT id, k FROM '{0s}/in.csv')" },
    };
    inline for (cases) |c| {
        const early = try std.fmt.allocPrint(alloc, "LOAD INTO '{0s}/early.csv' AS " ++ c.early ++ ";\n", .{base});
        defer alloc.free(early);
        const full = try std.fmt.allocPrint(alloc, "LOAD INTO '{0s}/out.csv' AS " ++ c.full ++ ";\n", .{base});
        defer alloc.free(full);
        var cap: u64 = 1024;
        while (true) : (cap *= 2) {
            if (runScriptOpts(alloc, &tmp, full, .{ .threads = 1, .op_memory = 1, .spill_dir = spill_dir, .spill_cap = cap, .log = .{ .quiet = true } })) |out| {
                alloc.free(out);
                break;
            } else |e| try std.testing.expectEqual(error.SpillCapExceeded, e);
        }
        const script = try std.mem.concat(alloc, u8, &.{ early, early, early, full });
        defer alloc.free(script);
        const out = try runScriptOpts(alloc, &tmp, script, .{ .threads = 1, .op_memory = 1, .spill_dir = spill_dir, .spill_cap = cap, .log = .{ .quiet = true } });
        defer alloc.free(out);
        try std.testing.expectEqualStrings("n\n4000\n", out);
    }
}

test "distinct into parquet on one lane: the writer thread and the operator do not share an arena" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var in = std.array_list.Managed(u8).init(alloc);
    defer in.deinit();
    try in.appendSlice("id,s\n");
    for (0..200_000) |i| try in.print("{d},value_{d}_{d}\n", .{ i, i, (i * 7919) % 100_003 });
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = in.items });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{0s}/d.parquet' AS SELECT DISTINCT s FROM '{0s}/in.csv';\n" ++
        "LOAD INTO '{0s}/out.csv' AS SELECT count(*) AS n FROM '{0s}/d.parquet';", .{base});
    defer alloc.free(script);
    const out = try runScriptOpts(alloc, &tmp, script, .{ .threads = 1, .log = .{ .quiet = true } });
    defer alloc.free(out);
    try std.testing.expectEqualStrings("n\n200000\n", out);
}

test "csv: a quote left open is an error, not every later line read as one field" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try std.testing.expectError(error.PlanFailed, runToString(alloc, &tmp, "id,name\n1,\"open\n2,b\n3,c\n", "SELECT * FROM '$IN'"));
}

test "csv: a leading-zero code past the sample is a mismatch, not a silent int" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var in = std.array_list.Managed(u8).init(alloc);
    defer in.deinit();
    try in.appendSlice("id,code\n");
    for (0..2000) |i| try in.print("{d},{d}\n", .{ i, 1000 + i });
    try in.appendSlice("2000,01310100\n");
    try std.testing.expectError(error.CsvTypeMismatch, runToString(alloc, &tmp, in.items, "SELECT * FROM '$IN'"));
}

test "count: a float column past the int range is counted, not converted" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const got = try runToString(alloc, &tmp, "k,f\n1,1e300\n2,\n3,-1e300\n", "SELECT COUNT(f) AS n, COUNT(*) AS a FROM '$IN'");
    defer alloc.free(got);
    try std.testing.expectEqualStrings("n,a\n2,3\n", got);
}

test "distinct: the single int-key path keeps first occurrences, null included once" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const got = try runToString(alloc, &tmp, "k,v\n1,2\n2,\n3,1\n4,2\n5,\n6,1\n", "SELECT DISTINCT v FROM '$IN'");
    defer alloc.free(got);
    try std.testing.expectEqualStrings("v\n2\n\n1\n", got);
}

test "join: `b.col` names the right side's column even when the join renamed it `col_r`" {
    const alloc = std.testing.allocator;
    for ([_]usize{ 1, 4 }) |threads| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        try tmp.dir.writeFile(.{ .sub_path = "r.csv", .data = "id,grp,amt\n1,p,100\n2,q,200\n9,z,900\n" });
        try checkAndRun(alloc, &tmp,
            \\LOAD INTO '$B/both.csv' AS
            \\WITH r AS (SELECT id, grp, amt FROM '$B/r.csv')
            \\SELECT a.id, a.amt, b.amt FROM '$B/a.csv' a JOIN r b ON a.id = b.id ORDER BY a.id;
            \\LOAD INTO '$B/filt.csv' AS
            \\WITH r AS (SELECT id, grp, amt FROM '$B/r.csv')
            \\SELECT a.id, b.amt AS bamt, a.amt + b.amt AS tot FROM '$B/a.csv' a LEFT JOIN r b ON a.id = b.id
            \\WHERE b.amt > 150 OR b.amt IS NULL ORDER BY b.amt DESC;
            \\LOAD INTO '$B/agg.csv' AS
            \\WITH r AS (SELECT id, grp, amt FROM '$B/r.csv')
            \\SELECT b.grp, SUM(b.amt) AS sb, SUM(a.amt) AS sa FROM '$B/a.csv' a JOIN r b ON a.id = b.id GROUP BY b.grp ORDER BY b.grp;
            \\LOAD INTO '$B/chain.csv' AS
            \\WITH r AS (SELECT id, amt FROM '$B/r.csv'), s AS (SELECT id, amt FROM '$B/r.csv' WHERE amt >= 200)
            \\SELECT a.id, b.amt, c.amt AS camt FROM '$B/a.csv' a JOIN r b ON a.id = b.id JOIN s c ON b.id = c.id;
            \\LOAD INTO '$B/unaliased.csv' AS
            \\WITH r AS (SELECT id, amt FROM '$B/r.csv')
            \\SELECT a.id, r.amt FROM '$B/a.csv' a JOIN r ON a.id = r.id ORDER BY a.id;
        , threads, &.{});
        try expectFile(&tmp, "both.csv", "id,amt,amt_r\n1,10,100\n2,20,200\n");
        try expectFile(&tmp, "filt.csv", "id,bamt,tot\n3,,\n2,200,220\n");
        try expectFile(&tmp, "agg.csv", "grp,sb,sa\np,100,10\nq,200,20\n");
        try expectFile(&tmp, "chain.csv", "id,amt,camt\n2,200,200\n");
        try expectFile(&tmp, "unaliased.csv", "id,amt\n1,100\n2,200\n");
    }
}

test "a qualified ORDER BY or DISTINCT ON key sorts by that table's column, not a same-named one before it" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "l.csv", .data = "k,s\n1,b\n2,a\n3,c\n" });
    try tmp.dir.writeFile(.{ .sub_path = "r.csv", .data = "k,s\n1,z\n2,y\n3,x\n" });
    try checkAndRun(alloc, &tmp,
        \\LOAD INTO '$B/both.csv' AS SELECT b.s, a.s FROM '$B/l.csv' a JOIN '$B/r.csv' b ON a.k = b.k ORDER BY a.s;
        \\LOAD INTO '$B/right.csv' AS SELECT b.s FROM '$B/l.csv' a JOIN '$B/r.csv' b ON a.k = b.k ORDER BY a.s DESC;
        \\LOAD INTO '$B/on.csv' AS SELECT DISTINCT ON (a.s) b.s FROM '$B/l.csv' a JOIN '$B/r.csv' b ON a.k = b.k ORDER BY a.s;
    , 1, &.{});
    try expectFile(&tmp, "both.csv", "s,s_r\ny,a\nz,b\nx,c\n");
    try expectFile(&tmp, "right.csv", "s\nx\nz\ny\n");
    try expectFile(&tmp, "on.csv", "s\ny\nz\nx\n");
}

test "DISTINCT ON keys are input columns: renamed, unprojected, and beside an ORDER BY on another hidden column" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "d.csv", .data = "id,grp,amt\n1,a,10\n2,a,20\n3,b,5\n4,b,50\n" });
    try checkAndRun(alloc, &tmp,
        \\LOAD INTO '$B/renamed.csv' AS SELECT DISTINCT ON (grp) grp AS k, upper(grp) AS u FROM '$B/d.csv' ORDER BY grp DESC;
        \\LOAD INTO '$B/hidden.csv' AS SELECT DISTINCT ON (grp) id, amt FROM '$B/d.csv';
        \\LOAD INTO '$B/sorted.csv' AS SELECT DISTINCT ON (grp) id FROM '$B/d.csv' ORDER BY amt DESC;
        \\LOAD INTO '$B/output.csv' AS SELECT DISTINCT ON (k) grp AS k, id FROM '$B/d.csv';
        \\LOAD INTO '$B/pair.csv' AS SELECT DISTINCT ON (grp, amt) id FROM '$B/d.csv';
    , 1, &.{});
    try expectFile(&tmp, "renamed.csv", "k,u\nb,B\na,A\n");
    try expectFile(&tmp, "hidden.csv", "id,amt\n1,10\n3,5\n");
    try expectFile(&tmp, "sorted.csv", "id\n4\n2\n");
    try expectFile(&tmp, "output.csv", "k,id\na,1\nb,3\n");
    try expectFile(&tmp, "pair.csv", "id\n1\n2\n3\n4\n");
}

test "union: SELECT * EXCEPT drops a column before the branches are reconciled, so its type never has to agree" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "ua.csv", .data = "id,x\n1,2024-01-01\n" });
    try tmp.dir.writeFile(.{ .sub_path = "ub.csv", .data = "id,x\n2,7\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const with_u =
        \\WITH u AS (SELECT id, CAST(x AS DATE) AS x FROM '$B/ua.csv'
        \\           UNION ALL BY NAME SELECT id, CAST(x AS INT) AS x FROM '$B/ub.csv' ANCHOR SCHEMA first)
    ;
    const bad = try std.mem.replaceOwned(u8, alloc, "LOAD INTO '$B/bad.csv' AS " ++ with_u ++ "\nSELECT * FROM u;", "$B", base);
    defer alloc.free(bad);
    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const bad_prog = try parser.parseSource(parena.allocator(), bad, &pdiag);
    var rdiag: Diag = .{};
    try std.testing.expect(std.meta.isError(run(alloc, bad_prog, .{ .log = .{ .quiet = true } }, &rdiag)));
    try std.testing.expect(std.mem.indexOf(u8, rdiag.msg, "no common type") != null);

    try checkAndRun(alloc, &tmp, "LOAD INTO '$B/ok.csv' AS " ++ with_u ++ "\nSELECT * EXCEPT (x) FROM u ORDER BY id;", 1, &.{});
    try expectFile(&tmp, "ok.csv", "id\n1\n2\n");
}

test "week: ISO weeks from Monday, in date_trunc, extract, date_add and date_diff" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try checkAndRun(alloc, &tmp,
        \\LOAD INTO '$B/w.csv' AS SELECT
        \\  date_trunc('week', CAST('2026-09-30' AS DATE)) AS wed,
        \\  date_trunc('week', CAST('2026-10-04 23:59:59' AS TIMESTAMP)) AS sun,
        \\  extract('week', CAST('2021-01-01' AS DATE)) AS w53,
        \\  extract('week', CAST('2024-12-30' AS DATE)) AS w1,
        \\  date_diff('week', CAST('2024-01-07' AS DATE), CAST('2024-01-08' AS DATE)) AS crossed,
        \\  date_add('week', 2, CAST('2026-09-30' AS DATE)) AS plus2;
    , 1, &.{});
    try expectFile(&tmp, "w.csv", "wed,sun,w53,w1,crossed,plus2\n2026-09-28 00:00:00,2026-09-28 00:00:00,53,1,1,2026-10-14\n");
}

test "a chain of CTEs, each reading the one before, gives the same rows inlined at any thread count" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_]usize{ 1, 4 }) |threads| {
        try checkAndRun(alloc, &tmp,
            \\LOAD INTO '$B/filters.csv' AS
            \\WITH x AS (SELECT id, grp, amt FROM '$B/a.csv' WHERE amt > 5),
            \\     y AS (SELECT id, grp FROM x WHERE id < 3)
            \\SELECT id FROM y WHERE grp = 'a' ORDER BY id;
            \\LOAD INTO '$B/computed.csv' AS
            \\WITH x AS (SELECT id, amt * 2 AS dbl FROM '$B/a.csv'),
            \\     y AS (SELECT id, dbl FROM x WHERE dbl > 15)
            \\SELECT id, dbl FROM y ORDER BY id;
            \\LOAD INTO '$B/window.csv' AS
            \\WITH x AS (SELECT id, grp, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY id) AS rn FROM '$B/a.csv'),
            \\     y AS (SELECT id, grp FROM x WHERE rn = 1)
            \\SELECT id, grp FROM y ORDER BY id;
            \\LOAD INTO '$B/agg.csv' AS
            \\WITH x AS (SELECT grp, SUM(amt) AS total FROM '$B/a.csv' GROUP BY grp),
            \\     y AS (SELECT grp, total FROM x WHERE total > 10)
            \\SELECT grp, total FROM y;
            \\LOAD INTO '$B/joined.csv' AS
            \\WITH x AS (SELECT id, amt FROM '$B/a.csv' WHERE amt > 5),
            \\     y AS (SELECT id FROM x WHERE id > 1)
            \\SELECT l.id, l.grp FROM '$B/a.csv' l JOIN y ON l.id = y.id ORDER BY l.id;
        , threads, &.{});
        try expectFile(&tmp, "filters.csv", "id\n1\n2\n");
        try expectFile(&tmp, "computed.csv", "id,dbl\n1,20\n2,40\n");
        try expectFile(&tmp, "window.csv", "id,grp\n1,a\n3,b\n");
        try expectFile(&tmp, "agg.csv", "grp,total\na,30\n");
        try expectFile(&tmp, "joined.csv", "id,grp\n2,a\n");
    }
}

fn readOut(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir, name: []const u8) ![]u8 {
    return tmp.dir.readFileAlloc(alloc, name, 1 << 22);
}

fn expectSameFiles(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir, a: []const u8, b: []const u8) !void {
    const x = try readOut(alloc, tmp, a);
    defer alloc.free(x);
    const y = try readOut(alloc, tmp, b);
    defer alloc.free(y);
    try std.testing.expectEqualStrings(x, y);
}

fn expectDifferentFiles(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir, a: []const u8, b: []const u8) !void {
    const x = try readOut(alloc, tmp, a);
    defer alloc.free(x);
    const y = try readOut(alloc, tmp, b);
    defer alloc.free(y);
    try std.testing.expect(!std.mem.eql(u8, x, y));
}

fn writeJoinOrderInputs(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir) !void {
    try tmp.dir.writeFile(.{ .sub_path = "small.csv", .data = "k,v\n1,a\n1,b\n2,c\n,n\n7,z\n" });
    var big = std.array_list.Managed(u8).init(alloc);
    defer big.deinit();
    try big.appendSlice("k,v,w\n");
    for (0..400) |i| {
        if (i % 50 == 0) try big.writer().print(",x{d},{d}\n", .{ i, i }) else try big.writer().print("{d},x{d},{d}\n", .{ i % 5, i, i });
    }
    try tmp.dir.writeFile(.{ .sub_path = "big.csv", .data = big.items });
}

test "join order: a left side four times smaller is held in memory, with the written columns and rows" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeJoinOrderInputs(alloc, &tmp);
    try checkAndRun(alloc, &tmp,
        \\LOAD INTO '$B/auto.csv' AS
        \\SELECT * FROM '$B/small.csv' s JOIN '$B/big.csv' b ON s.k = b.k ORDER BY s.k, s.v, b.w;
        \\LOAD INTO '$B/written.csv' AS
        \\SELECT * FROM '$B/small.csv' s JOIN '$B/big.csv' b ON s.k = b.k WITH (join_order = 'written') ORDER BY s.k, s.v, b.w;
        \\LOAD INTO '$B/auto_agg.csv' AS
        \\SELECT s.v, COUNT(*) AS n, SUM(b.w) AS t FROM '$B/small.csv' s JOIN '$B/big.csv' b ON s.k = b.k GROUP BY s.v ORDER BY s.v;
        \\LOAD INTO '$B/written_agg.csv' AS
        \\SELECT s.v, COUNT(*) AS n, SUM(b.w) AS t FROM '$B/small.csv' s JOIN '$B/big.csv' b ON s.k = b.k WITH (join_order = 'written') GROUP BY s.v ORDER BY s.v;
        \\LOAD INTO '$B/auto_ties.csv' AS
        \\SELECT s.v, b.w FROM '$B/small.csv' s JOIN '$B/big.csv' b ON s.k = b.k ORDER BY s.k;
        \\LOAD INTO '$B/written_ties.csv' AS
        \\SELECT s.v, b.w FROM '$B/small.csv' s JOIN '$B/big.csv' b ON s.k = b.k WITH (join_order = 'written') ORDER BY s.k;
        \\LOAD INTO '$B/unsorted.csv' AS
        \\SELECT s.v, b.w FROM '$B/small.csv' s JOIN '$B/big.csv' b ON s.k = b.k;
        \\LOAD INTO '$B/unsorted_written.csv' AS
        \\SELECT s.v, b.w FROM '$B/small.csv' s JOIN '$B/big.csv' b ON s.k = b.k WITH (join_order = 'written');
    , 1, &.{});
    const auto = try readOut(alloc, &tmp, "auto.csv");
    defer alloc.free(auto);
    try std.testing.expect(std.mem.startsWith(u8, auto, "k,v,k_r,v_r,w\n1,a,1,x1,1\n1,a,1,x6,6\n"));
    try std.testing.expectEqual(@as(usize, 1 + 2 * 80 + 80), std.mem.count(u8, auto, "\n"));
    try expectSameFiles(alloc, &tmp, "auto.csv", "written.csv");
    try expectSameFiles(alloc, &tmp, "auto_agg.csv", "written_agg.csv");
    try expectDifferentFiles(alloc, &tmp, "auto_ties.csv", "written_ties.csv");
    try expectSameFiles(alloc, &tmp, "unsorted.csv", "unsorted_written.csv");
}

test "join order: a turned-around join that spills gives the written rows" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeJoinOrderInputs(alloc, &tmp);
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const spill_dir = try std.fs.path.join(alloc, &.{ base, "spill" });
    defer alloc.free(spill_dir);
    const script = try std.mem.replaceOwned(u8, alloc,
        \\LOAD INTO '$B/auto.csv' AS
        \\SELECT * FROM '$B/small.csv' s JOIN '$B/big.csv' b ON s.k = b.k ORDER BY s.k, s.v, b.w;
        \\LOAD INTO '$B/written.csv' AS
        \\SELECT * FROM '$B/small.csv' s JOIN '$B/big.csv' b ON s.k = b.k WITH (join_order = 'written') ORDER BY s.k, s.v, b.w;
    , "$B", base);
    defer alloc.free(script);
    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var want: ?[]u8 = null;
    defer if (want) |w| alloc.free(w);
    for ([_]usize{ 2 << 30, 64 }) |op_memory| {
        var rdiag: Diag = .{};
        _ = try run(alloc, prog, .{ .op_memory = op_memory, .spill_dir = spill_dir, .log = .{ .quiet = true } }, &rdiag);
        try expectSameFiles(alloc, &tmp, "auto.csv", "written.csv");
        const got = try readOut(alloc, &tmp, "auto.csv");
        if (want) |w| {
            defer alloc.free(got);
            try std.testing.expectEqualStrings(w, got);
        } else want = got;
    }
}

test "join order: a left side larger than the right stays the probe side" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeJoinOrderInputs(alloc, &tmp);
    try checkAndRun(alloc, &tmp,
        \\LOAD INTO '$B/auto.csv' AS
        \\SELECT * FROM '$B/big.csv' b JOIN '$B/small.csv' s ON b.k = s.k ORDER BY b.k;
        \\LOAD INTO '$B/written.csv' AS
        \\SELECT * FROM '$B/big.csv' b JOIN '$B/small.csv' s ON b.k = s.k WITH (join_order = 'written') ORDER BY b.k;
    , 1, &.{});
    const auto = try readOut(alloc, &tmp, "auto.csv");
    defer alloc.free(auto);
    try std.testing.expect(std.mem.startsWith(u8, auto, "k,v,w,k_r,v_r\n1,x1,1,1,a\n1,x1,1,1,b\n"));
    try expectSameFiles(alloc, &tmp, "auto.csv", "written.csv");
}

test "join order: a star chain of inner joins runs smallest side first, the written columns kept" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf = std.array_list.Managed(u8).init(alloc);
    defer buf.deinit();
    try buf.appendSlice("id,a,b,c\n");
    for (0..60) |i| {
        if (i % 13 == 0) try buf.writer().print("{d},,{d},{d}\n", .{ i, i % 3, i % 5 }) else try buf.writer().print("{d},{d},{d},{d}\n", .{ i, i % 4, i % 3, i % 5 });
    }
    try tmp.dir.writeFile(.{ .sub_path = "f.csv", .data = buf.items });
    buf.clearRetainingCapacity();
    try buf.appendSlice("a,name\n");
    for (0..40) |i| try buf.writer().print("{d},da{d}\n", .{ i % 4, i });
    try tmp.dir.writeFile(.{ .sub_path = "da.csv", .data = buf.items });
    try tmp.dir.writeFile(.{ .sub_path = "db.csv", .data = "b,name\n0,b0\n1,b1\n1,b1x\n" });
    buf.clearRetainingCapacity();
    try buf.appendSlice("c,name\n");
    for (0..20) |i| try buf.writer().print("{d},dc{d}\n", .{ i % 5, i });
    try tmp.dir.writeFile(.{ .sub_path = "dc.csv", .data = buf.items });

    const q = "SELECT * FROM '$B/f.csv' f JOIN '$B/da.csv' da ON f.a = da.a JOIN '$B/db.csv' db ON f.b = db.b JOIN '$B/dc.csv' dc ON f.c = dc.c";
    const qw = "SELECT * FROM '$B/f.csv' f JOIN '$B/da.csv' da ON f.a = da.a WITH (join_order = 'written') JOIN '$B/db.csv' db ON f.b = db.b WITH (join_order = 'written') JOIN '$B/dc.csv' dc ON f.c = dc.c WITH (join_order = 'written')";
    const sorted = " ORDER BY id, name, name_r, name_r2;\n";
    const ties = " ORDER BY id;\n";
    const script = try std.mem.concat(alloc, u8, &.{
        "LOAD INTO '$B/auto.csv' AS ",                                                                                                                                                                                                                  q,                                                                                                                                                                                                                                                                                                                                         sorted,
        "LOAD INTO '$B/written.csv' AS ",                                                                                                                                                                                                               qw,                                                                                                                                                                                                                                                                                                                                        sorted,
        "LOAD INTO '$B/auto_ties.csv' AS ",                                                                                                                                                                                                             q,                                                                                                                                                                                                                                                                                                                                         ties,
        "LOAD INTO '$B/written_ties.csv' AS ",                                                                                                                                                                                                          qw,                                                                                                                                                                                                                                                                                                                                        ties,
        "LOAD INTO '$B/auto_agg.csv' AS SELECT db.name AS bn, COUNT(*) AS n, SUM(f.id) AS s FROM '$B/f.csv' f JOIN '$B/da.csv' da ON f.a = da.a JOIN '$B/db.csv' db ON f.b = db.b JOIN '$B/dc.csv' dc ON f.c = dc.c GROUP BY db.name ORDER BY bn;\n",   "LOAD INTO '$B/written_agg.csv' AS SELECT db.name AS bn, COUNT(*) AS n, SUM(f.id) AS s FROM '$B/f.csv' f JOIN '$B/da.csv' da ON f.a = da.a WITH (join_order = 'written') JOIN '$B/db.csv' db ON f.b = db.b WITH (join_order = 'written') JOIN '$B/dc.csv' dc ON f.c = dc.c WITH (join_order = 'written') GROUP BY db.name ORDER BY bn;\n", "LOAD INTO '$B/linked.csv' AS SELECT f.id, da.name, db.name AS bn FROM '$B/f.csv' f JOIN '$B/da.csv' da ON f.a = da.a JOIN '$B/db.csv' db ON da.a = db.b ORDER BY f.id;\n",
        "LOAD INTO '$B/linked_written.csv' AS SELECT f.id, da.name, db.name AS bn FROM '$B/f.csv' f JOIN '$B/da.csv' da ON f.a = da.a WITH (join_order = 'written') JOIN '$B/db.csv' db ON da.a = db.b WITH (join_order = 'written') ORDER BY f.id;\n",
    });
    defer alloc.free(script);
    try checkAndRun(alloc, &tmp, script, 1, &.{});

    const auto = try readOut(alloc, &tmp, "auto.csv");
    defer alloc.free(auto);
    try std.testing.expect(std.mem.startsWith(u8, auto, "id,a,b,c,a_r,name,b_r,name_r,c_r,name_r2\n1,1,1,1,1,da1,1,b1,1,dc1\n"));
    try expectSameFiles(alloc, &tmp, "auto.csv", "written.csv");
    try expectSameFiles(alloc, &tmp, "auto_agg.csv", "written_agg.csv");
    try expectDifferentFiles(alloc, &tmp, "auto_ties.csv", "written_ties.csv");
    try expectSameFiles(alloc, &tmp, "linked.csv", "linked_written.csv");
}

test "join order: an unknown join_order is refused" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeJoinOrderInputs(alloc, &tmp);
    try @import("harness.zig").expectRefusedAlike(alloc, &tmp,
        \\LOAD INTO '$B/x.csv' AS
        \\SELECT * FROM '$B/small.csv' s JOIN '$B/big.csv' b ON s.k = b.k WITH (join_order = 'best') ORDER BY s.k;
    , "join_order is 'auto' or 'written'");
}

const QualCase = struct { q: []const u8, want: []const u8 };

/// Each query of `cases` over the qualifier fixtures (`$B` the tmp dir), its CSV output
/// against `want`.
fn runQualCases(cases: []const QualCase) !void {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "l.csv", .data = "k,s,v\n1,a,10.5\n2,b,\n2,b2,3.0\n,n,1.0\n3,c,2.0\n4,e,4.0\n" });
    try tmp.dir.writeFile(.{ .sub_path = "r.csv", .data = "k,s\n2,x\n2,y\n3,z\n,nn\n5,w\n1,u\n" });
    try tmp.dir.writeFile(.{ .sub_path = "emp.csv", .data = "id,mgr,name\n1,,boss\n2,1,ann\n3,1,bob\n4,2,cy\n" });
    try tmp.dir.writeFile(.{ .sub_path = "t.csv", .data = "id,name,amt\n1,ana,10.5\n2,bob,20.25\n3,cid,\n" });
    try tmp.dir.writeFile(.{ .sub_path = "items.csv", .data = "name,item,q\nana,x,1\nana,y,2\nbob,z,3\neve,w,4\n" });
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

test "join: qualified keys bind to their side, a self-join too; an unqualified name on both sides is refused" {
    try runQualCases(&.{
        .{ .q = "SELECT e.name, m.name AS mname FROM '$B/emp.csv' e JOIN '$B/emp.csv' m ON e.mgr = m.id ORDER BY e.id", .want = "name,mname\nann,boss\nbob,boss\ncy,ann\n" },
        .{ .q = "SELECT e.name, m.name AS mname FROM '$B/emp.csv' e LEFT JOIN '$B/emp.csv' m ON m.id = e.mgr ORDER BY e.id", .want = "name,mname\nboss,\nann,boss\nbob,boss\ncy,ann\n" },
        .{ .q = "SELECT e.name, m.name AS mname FROM '$B/emp.csv' e JOIN (SELECT * FROM '$B/emp.csv') m ON m.id = e.mgr ORDER BY e.id", .want = "name,mname\nann,boss\nbob,boss\ncy,ann\n" },
        .{ .q = "WITH c AS (SELECT * FROM '$B/emp.csv') SELECT e.name, m.name AS mname FROM '$B/emp.csv' e LEFT JOIN c m ON e.mgr = m.id ORDER BY e.id", .want = "name,mname\nboss,\nann,boss\nbob,boss\ncy,ann\n" },
        .{ .q = "SELECT e.name, g.name AS gname FROM '$B/emp.csv' e JOIN '$B/emp.csv' m ON e.mgr = m.id JOIN '$B/emp.csv' g ON m.mgr = g.id", .want = "name,gname\ncy,boss\n" },
        .{ .q = "SELECT e.name, m.name AS mname FROM '$B/emp.csv' e JOIN '$B/emp.csv' m ON CAST(id AS string) = trim(CAST(e.mgr AS string)) ORDER BY e.id", .want = "name,mname\nann,boss\nbob,boss\ncy,ann\n" },
    });
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "c1.csv", .data = "code,a\nx,1\n" });
    try tmp.dir.writeFile(.{ .sub_path = "c2.csv", .data = "code,b\nx,2\n" });
    try @import("harness.zig").expectRefusedAlike(alloc, &tmp, "SELECT * FROM '$B/c1.csv' a JOIN '$B/c2.csv' b ON code = code;", "join key `code` is ambiguous");
}

test "join: `t.*` is that table's columns alone, by their own names unless taken; `b.x` is only `b`'s" {
    try runQualCases(&.{
        .{ .q = "SELECT i.* FROM '$B/t.csv' t JOIN '$B/items.csv' i ON t.name = i.name ORDER BY i.item", .want = "name,item,q\nana,x,1\nana,y,2\nbob,z,3\n" },
        .{ .q = "SELECT t.* FROM '$B/t.csv' t JOIN '$B/items.csv' i ON t.name = i.name WHERE i.item = 'z'", .want = "id,name,amt\n2,bob,20.25\n" },
        .{ .q = "SELECT t.*, i.* FROM '$B/t.csv' t JOIN '$B/items.csv' i ON t.name = i.name WHERE i.item = 'z'", .want = "id,name,amt,name_r,item,q\n2,bob,20.25,bob,z,3\n" },
        .{ .q = "SELECT i.*, row_number() OVER (ORDER BY t.id, i.item) AS rn FROM '$B/t.csv' t JOIN '$B/items.csv' i ON t.name = i.name", .want = "name,item,q,rn\nana,x,1,1\nana,y,2,2\nbob,z,3,3\n" },
        .{ .q = "SELECT g.* FROM '$B/emp.csv' e JOIN '$B/emp.csv' m ON e.mgr = m.id JOIN '$B/emp.csv' g ON m.mgr = g.id", .want = "id,mgr,name\n1,,boss\n" },
        .{ .q = "SELECT i.name, i.item FROM '$B/t.csv' t JOIN '$B/items.csv' i USING (name) WHERE i.name = 'bob'", .want = "name,item\nbob,z\n" },
        .{ .q = "SELECT l.k FROM '$B/l.csv' WHERE l.k > 3", .want = "k\n4\n" },
        .{ .q = "SELECT r.* FROM '$B/l.csv' JOIN '$B/r.csv' ON l.k = r.k WHERE l.s = 'c'", .want = "k,s\n3,z\n" },
        .{ .q = "WITH c AS (SELECT a.k, b.s FROM '$B/l.csv' a JOIN '$B/r.csv' b ON a.k = b.k) SELECT c.*, b.* FROM c JOIN '$B/items.csv' b ON c.s = b.item WHERE c.s = 'z'", .want = "k,s,name,item,q\n3,z,bob,z,3\n" },
    });
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "l.csv", .data = "k,v\n1,2\n" });
    try tmp.dir.writeFile(.{ .sub_path = "r.csv", .data = "k,s\n1,x\n" });
    try @import("harness.zig").expectRefusedAlike(alloc, &tmp, "SELECT b.v FROM '$B/l.csv' a JOIN '$B/r.csv' b ON a.k = b.k;", "unknown column `b.v`");
}

test "join: under USING `t.k` and `t.*` are that side's own, the key in its place; `*` lists the merged key once, first" {
    try runQualCases(&.{
        .{ .q = "SELECT t.* FROM '$B/t.csv' t JOIN '$B/items.csv' i USING (name) WHERE i.item = 'z'", .want = "id,name,amt\n2,bob,20.25\n" },
        .{ .q = "SELECT i.* FROM '$B/t.csv' t RIGHT JOIN '$B/items.csv' i USING (name) WHERE i.item = 'w'", .want = "name,item,q\neve,w,4\n" },
        .{ .q = "SELECT * FROM '$B/t.csv' t FULL JOIN '$B/items.csv' i USING (name) WHERE i.q IS NULL OR i.q > 3", .want = "name,id,amt,item,q\ncid,3,,,\neve,,,w,4\n" },
        .{ .q = "SELECT t.name AS tn, i.name AS iname, name FROM '$B/t.csv' t FULL JOIN '$B/items.csv' i USING (name) WHERE t.name IS NULL OR i.name IS NULL", .want = "tn,iname,name\ncid,,cid\n,eve,eve\n" },
        .{ .q = "SELECT i.name, count(*) AS n FROM '$B/t.csv' t FULL JOIN '$B/items.csv' i USING (name) WHERE i.name IS NULL OR i.name = 'ana' GROUP BY i.name ORDER BY n", .want = "name,n\n,1\nana,2\n" },
        .{ .q = "SELECT * FROM '$B/t.csv' t FULL JOIN '$B/items.csv' i USING (name) ORDER BY i.name DESC, q", .want = "name,id,amt,item,q\ncid,3,,,\neve,,,w,4\nbob,2,20.25,z,3\nana,1,10.5,x,1\nana,1,10.5,y,2\n" },
        .{ .q = "SELECT * RENAME (id AS tid) FROM '$B/t.csv' t JOIN '$B/items.csv' i USING (name) WHERE i.item = 'z'", .want = "name,tid,amt,item,q\nbob,2,20.25,z,3\n" },
        .{ .q = "SELECT name, item FROM '$B/t.csv' t FULL JOIN '$B/items.csv' i USING (name) ORDER BY t.name, item", .want = "name,item\nana,x\nana,y\nbob,z\ncid,\neve,w\n" },
        .{ .q = "SELECT DISTINCT ON (i.name) * FROM '$B/t.csv' t JOIN '$B/items.csv' i USING (name) ORDER BY i.name, q DESC", .want = "name,id,amt,item,q\nana,1,10.5,y,2\nbob,2,20.25,z,3\n" },
        .{ .q = "SELECT DISTINCT i.name FROM '$B/t.csv' t JOIN '$B/items.csv' i USING (name) ORDER BY i.name DESC", .want = "name\nbob\nana\n" },
    });
}

test "join: a key or column the parser renamed is named in a message as the query wrote it" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "l.csv", .data = "k,w\n1,x\n" });
    try tmp.dir.writeFile(.{ .sub_path = "r.csv", .data = "k,v\n1,10\n" });
    try tmp.dir.writeFile(.{ .sub_path = "s.csv", .data = "k,v\na,10\n" });
    const refuse = @import("harness.zig").expectRefusedAlike;
    try refuse(alloc, &tmp, "SELECT * FROM '$B/l.csv' l JOIN '$B/r.csv' r ON upper(l.w) = r.k;", "join keys an ON expression (string) and `k` (int) are not comparable");
    try refuse(alloc, &tmp, "SELECT * FROM '$B/l.csv' l JOIN '$B/s.csv' s USING (k);", "join keys `k` (int) and `k` (string) are not comparable");
    try refuse(alloc, &tmp, "SELECT count(*) FROM '$B/l.csv' GROUP BY upper(w) HAVING upper(w) > 1;", "cannot compare an expression (string) with 1 (int)");
}

test "join: a qualified column is that table's alone, the FROM table's and a later join's too" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "t.csv", .data = "id,name\n1,ana\n" });
    try tmp.dir.writeFile(.{ .sub_path = "items.csv", .data = "name,item,q\nana,x,1\n" });
    try tmp.dir.writeFile(.{ .sub_path = "emp.csv", .data = "id,mgr\n1,2\n" });
    const refuse = @import("harness.zig").expectRefusedAlike;
    try refuse(alloc, &tmp, "SELECT a.item FROM '$B/t.csv' a JOIN '$B/items.csv' b ON a.name = b.name;", "unknown column `a.item`");
    try refuse(alloc, &tmp, "SELECT * FROM '$B/t.csv' a JOIN '$B/items.csv' b ON a.name = b.name WHERE a.q > 0;", "unknown column `a.q`");
    try refuse(alloc, &tmp, "SELECT * FROM '$B/t.csv' a JOIN '$B/items.csv' b ON a.name = b.name JOIN '$B/emp.csv' c ON c.id = a.q;", "unknown column `a.q`");
    try refuse(alloc, &tmp, "SELECT count(*) FROM '$B/t.csv' a JOIN '$B/items.csv' b ON a.name = b.name GROUP BY a.item;", "unknown column `a.item`");
    try refuse(alloc, &tmp, "SELECT * FROM '$B/t.csv' a JOIN '$B/items.csv' b ON a.name = b.name ORDER BY a.item;", "unknown column `a.item`");
    try refuse(alloc, &tmp, "SELECT * FROM '$B/t.csv' a JOIN '$B/items.csv' b ON c.id = a.id JOIN '$B/emp.csv' c ON c.id = a.id;", "unknown column `c.id`");
}

test "join: a second column of one name is named apart, hidden sort and DISTINCT ON keys too" {
    try runQualCases(&.{
        .{ .q = "SELECT b.s, a.s FROM '$B/l.csv' a JOIN '$B/r.csv' b ON a.k = b.k WHERE a.k = 1", .want = "s,s_r\nu,a\n" },
        .{ .q = "SELECT a.s, b.s FROM '$B/l.csv' a JOIN '$B/r.csv' b ON a.k = b.k WHERE a.k = 1", .want = "s,s_r\na,u\n" },
        .{ .q = "SELECT b.s, a.s FROM '$B/l.csv' a JOIN '$B/r.csv' b ON a.k = b.k WHERE a.k <> 2 ORDER BY b.k", .want = "s,s_r\nu,a\nz,c\n" },
        .{ .q = "SELECT DISTINCT ON (b.k) b.s, a.s FROM '$B/l.csv' a JOIN '$B/r.csv' b ON a.k = b.k WHERE a.k <> 2 ORDER BY b.k", .want = "s,s_r\nu,a\nz,c\n" },
    });
}

test "where: a quoted literal against a number or bool is read as one, exactly; one that does not read so is refused" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const in = "id,amt,f,ok\n1,99999.50,1.5,true\n2,100000.25,2.5,false\n3,,,\n";
    const Case = struct { q: []const u8, want: []const u8 };
    const cases = [_]Case{
        .{ .q = "SELECT id FROM '$IN' WHERE CAST(amt AS DECIMAL(10,2)) < '100000'", .want = "id\n1\n" },
        .{ .q = "SELECT id FROM '$IN' WHERE CAST(amt AS DECIMAL(10,2)) <= ' 99999.505 '", .want = "id\n1\n" },
        .{ .q = "SELECT id FROM '$IN' WHERE '2' > id", .want = "id\n1\n" },
        .{ .q = "SELECT id FROM '$IN' WHERE id IN ('1', '3')", .want = "id\n1\n3\n" },
        .{ .q = "SELECT id FROM '$IN' WHERE f >= '2.5'", .want = "id\n2\n" },
        .{ .q = "SELECT id FROM '$IN' WHERE CAST(ok AS BOOL) = 'yes'", .want = "id\n1\n" },
        .{ .q = "SELECT id, id = '2' AS two FROM '$IN' WHERE id < 3", .want = "id,two\n1,false\n2,true\n" },
    };
    for (cases) |c| {
        const out = try runToString(alloc, &tmp, in, c.q);
        defer alloc.free(out);
        std.testing.expectEqualStrings(c.want, out) catch |e| {
            std.debug.print("query: {s}\n", .{c.q});
            return e;
        };
    }
    try tmp.dir.writeFile(.{ .sub_path = "d.csv", .data = in });
    const refuse = @import("harness.zig").expectRefusedAlike;
    try refuse(alloc, &tmp, "SELECT * FROM '$B/d.csv' WHERE CAST(amt AS DECIMAL(10,2)) = '1,5';", "`1,5` is not a valid decimal literal");
    try refuse(alloc, &tmp, "SELECT * FROM '$B/d.csv' WHERE id = '1.5';", "`1.5` is not a valid int literal");
    try refuse(alloc, &tmp, "SELECT * FROM '$B/d.csv' WHERE CAST(id AS STRING) = id;", "cannot compare");
}

test "where: IS [NOT] DISTINCT FROM compares with nulls as values, never null itself; NULL keys never match a join" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "r.csv", .data = "k,b\n1,p\n3,q\n,r\n" });
    const in = "a,k\nx,1\ny,2\nz,\n";
    const Case = struct { q: []const u8, want: []const u8 };
    const cases = [_]Case{
        .{ .q = "SELECT a FROM '$IN' WHERE k IS DISTINCT FROM 1", .want = "a\ny\nz\n" },
        .{ .q = "SELECT a FROM '$IN' WHERE k IS NOT DISTINCT FROM NULL", .want = "a\nz\n" },
        .{ .q = "SELECT a, k IS DISTINCT FROM 2 AS d, k = 2 AS e FROM '$IN'", .want = "a,d,e\nx,true,false\ny,false,true\nz,true,\n" },
        .{ .q = "SELECT a FROM '$IN' WHERE NOT (k IS DISTINCT FROM '2')", .want = "a\ny\n" },
        .{ .q = "SELECT l.a, r.b FROM '$IN' l LEFT JOIN '$B/r.csv' r ON l.k IS NOT DISTINCT FROM r.k", .want = "a,b\nx,p\ny,\nz,r\n" },
        .{ .q = "SELECT l.a, r.b FROM '$IN' l LEFT JOIN '$B/r.csv' r ON l.k = r.k", .want = "a,b\nx,p\ny,\nz,\n" },
        .{ .q = "SELECT l.a FROM '$IN' l ANTI JOIN '$B/r.csv' r ON l.k = r.k", .want = "a\ny\nz\n" },
        .{ .q = "SELECT l.a FROM '$IN' l WHERE l.k NOT IN (SELECT k FROM '$B/r.csv')", .want = "a\n" },
        .{ .q = "SELECT l.a FROM '$IN' l WHERE l.k NOT IN (SELECT k FROM '$B/r.csv' WHERE k > 5)", .want = "a\nx\ny\nz\n" },
    };
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    for (cases) |c| {
        const q = try std.mem.replaceOwned(u8, alloc, c.q, "$B", base);
        defer alloc.free(q);
        const out = try runToString(alloc, &tmp, in, q);
        defer alloc.free(out);
        std.testing.expectEqualStrings(c.want, out) catch |e| {
            std.debug.print("query: {s}\n", .{c.q});
            return e;
        };
    }
}
